//! Pipeline steps. [`run_step`] runs one claimed step and returns what to record on it plus the
//! follow-up steps; the worker hands both to `steps::complete_and_enqueue` (or the error to
//! `steps::fail`). Acquire → Scan → Detect → Convert (documents, images, audio/video) →
//! OcrPages / Caption / TranscribeSegment → Bundle → Deliver.
//!
//! Originals and binary artefacts (figures, audio segments) live in storage_service, which
//! virus-scans every upload; textual artefacts (convert, ocr, transcript, bundle) live in
//! `ingest_artifacts` (rulings R25, R26).
//!
//! Step writes made while a step runs (asset rows, artefacts, job meta, rejecting the job) are
//! fenced on the claim; a `LeaseLost` among them becomes a `lease_lost` [`StepError`], whose `fail` is then
//! refused as `LeaseLost` too and dropped by the worker (rulings R12).

use std::time::{Duration, Instant};

use bytes::BytesMut;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::audio;
use super::bundle::{
    BundleInput, MAX_SOURCE_CHARS, Segment, build_bundle, figure_refs, has_content,
};
use super::detect::{self, Detected};
use super::figures::{self, extract_figures, figure_id, placeholder};
use super::page_images::inline_page_images;
use super::triage::{
    OCR_PAGES_PER_STEP, OcrMode, batch_key, low_text_pages, ocr_batches, split_pages,
};
use super::{
    BUNDLE_ARTIFACT, CONVERT_ARTIFACT, Ctx, audio_key, figure_key, ocr_artifact, original_key,
    transcript_artifact,
};
use crate::db::artifacts;
use crate::db::assets::{self, AssetKind, NewAsset};
use crate::db::jobs::{self, Job};
use crate::db::steps::{self, ClaimedStep, StepKind, StepOutput};
use crate::error::{IngestError, StepError};
use crate::infra::docling::DoclingOpts;
use crate::infra::storage::{ScanVerdict, StorageError};
use crate::infra::strata::Origin;
use crate::infra::webscraper::ScrapeOpts;
use crate::limits::MAX_BUNDLE_BYTES;
use crate::metering::Meter;
use crate::providers::ocr::OcrInput;
use crate::providers::registry::Resolved;
use crate::providers::vision;
use crate::providers::{Capability, ErrorClass, Usage};

/// A step's result: what to record on it, and the follow-up steps to enqueue.
pub type StepResult = Result<(StepOutput, Vec<(StepKind, String, Value)>), StepError>;

/// Bytes Detect reads before deciding; only a zip container needs the whole body.
const DETECT_HEAD: usize = 64 * 1024;

/// `ingest_jobs.source`.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Source {
    Upload {
        #[serde(default)]
        filename: Option<String>,
        #[serde(default)]
        mime: Option<String>,
        #[serde(default)]
        bytes: Option<u64>,
        /// The storage_service verdict the API got when it stored the original: `clean` or
        /// `unverified` (an infected upload never becomes a queued job).
        #[serde(default)]
        scan: Option<String>,
    },
    Url {
        url: String,
        /// A crawl child whose page the parent's Discover step already scraped and stored at
        /// the original key (`pipeline::discover`): Acquire records it instead of scraping.
        #[serde(default)]
        prefetched: Option<Prefetched>,
    },
    /// A crawl / sitemap parent: its only step is Discover (`pipeline::discover`).
    Crawl {
        url: String,
    },
    Sitemap {
        url: String,
    },
    /// A structured source: the API fetched and split it into records and created one Deliver
    /// step per record (`api::sources`); it never has an Acquire step.
    Api {
        url: String,
    },
    Datatable {
        url: String,
    },
}

/// A crawl child's `source.prefetched`: what the parent's Discover step stored for it.
#[derive(Debug, Deserialize)]
struct Prefetched {
    bytes: u64,
    /// storage_service's verdict: `clean`, `unverified` or `infected` (nothing stored).
    scan: String,
    #[serde(default)]
    signature: Option<String>,
    final_url: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    images: Option<Value>,
}

/// What a crawl child's Acquire found of its prefetched original.
enum Reuse {
    /// Recorded; the Acquire output.
    Stored(Value),
    /// The page was refused as infected (signature).
    Infected(String),
    /// Not (fully) there: scrape the page.
    Missing,
}

/// Runs one claimed step.
pub async fn run_step(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    match step.kind {
        StepKind::Discover => super::discover::discover(ctx, step).await,
        StepKind::Acquire => acquire(ctx, step).await,
        StepKind::Scan => scan(ctx, step).await,
        StepKind::Detect => detect(ctx, step).await,
        StepKind::Convert => convert(ctx, step).await,
        StepKind::OcrPages => ocr_pages(ctx, step).await,
        StepKind::Caption => caption(ctx, step).await,
        StepKind::TranscribeSegment => transcribe_segment(ctx, step).await,
        StepKind::Bundle => bundle(ctx, step).await,
        StepKind::Deliver => deliver(ctx, step).await,
    }
}

// ---------------------------------------------------------------------------------------------
// Acquire

/// Upload: the API already stored the file at the original key (storage_service scanned it);
/// check it is all there, record it and carry the upload's scan verdict into `meta.scan`. URL:
/// scrape the page (with its images, `download_images`), inline the usable images as data URIs
/// (`page_images`: Convert turns them into figures) and store that markdown as the original; the
/// upload's scan decides: infected → the job is `rejected` (`infected`, nothing stored) and
/// nothing follows.
async fn acquire(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let job = load_job(ctx, step).await?;
    let key = original_key(step.tenant, step.job_id);
    let max = ctx.cfg.limits.max_upload_bytes;
    let output = match source(&job)? {
        Source::Upload {
            mime,
            bytes: declared,
            scan,
            ..
        } => {
            let size = ctx.storage.size(&key).await.map_err(|e| match e {
                StorageError::NotFound => err(
                    ErrorClass::InvalidRequest,
                    "upload_missing",
                    format!("the uploaded original {key} does not exist"),
                ),
                e => e.step_error(&key),
            })?;
            if let Some(declared) = declared
                && declared != size
            {
                return Err(err(
                    ErrorClass::InvalidRequest,
                    "upload_incomplete",
                    format!("the original holds {size} bytes; the upload declared {declared}"),
                ));
            }
            if size > max {
                return Err(too_large(size, max));
            }
            let mime = mime
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_else(|| "application/octet-stream".to_string());
            record_original(ctx, step, &key, size, &mime).await?;
            // Only an explicit `clean` is clean; anything else is unverified (rulings R25).
            let scan = match scan.as_deref() {
                Some("clean") => ScanVerdict::Clean,
                _ => ScanVerdict::Unverified,
            };
            steps::merge_job_meta(&ctx.pool, step, &json!({ "scan": scan.as_str() }))
                .await
                .map_err(internal)?;
            json!({ "key": key, "bytes": size, "mime": mime, "scan": scan.as_str() })
        }
        Source::Url { url, prefetched } => {
            let reuse = match &prefetched {
                Some(p) => reuse_prefetched(ctx, step, &key, p).await?,
                None => Reuse::Missing,
            };
            match reuse {
                Reuse::Stored(output) => {
                    return Ok((
                        done(output),
                        vec![(StepKind::Scan, String::new(), json!({}))],
                    ));
                }
                Reuse::Infected(signature) => return reject_infected(ctx, step, signature).await,
                Reuse::Missing => {}
            }
            let page = ctx
                .webscraper
                .scrape_with(
                    &url,
                    &ScrapeOpts {
                        download_images: true,
                        link_query: None,
                    },
                )
                .await?;
            let images = inline_page_images(
                &page.markdown,
                &page.final_url,
                &page.images,
                ctx.cfg.limits.figure_min_bytes,
                max,
            );
            let images_report = images.report();
            let markdown = images.markdown;
            let size = markdown.len() as u64;
            if size > max {
                return Err(too_large(size, max));
            }
            let mime = Detected::Markdown.mime();
            let scan = match ctx.storage.upload_bytes(&key, markdown, mime).await {
                Ok(up) => up.verdict(),
                Err(StorageError::Infected(signature)) => {
                    return reject_infected(ctx, step, signature).await;
                }
                Err(e) => return Err(e.step_error(&key)),
            };
            record_original(ctx, step, &key, size, mime).await?;
            let mut meta = Map::new();
            if let Some(title) = &page.title {
                meta.insert("title".into(), json!(title));
            }
            meta.insert("final_url".into(), json!(page.final_url));
            meta.insert("mime".into(), json!(mime));
            meta.insert("scan".into(), json!(scan.as_str()));
            steps::merge_job_meta(&ctx.pool, step, &Value::Object(meta))
                .await
                .map_err(internal)?;
            json!({
                "key": key, "bytes": size, "mime": mime, "scan": scan.as_str(),
                "final_url": page.final_url, "title": page.title, "images": images_report,
            })
        }
        Source::Api { .. } | Source::Datatable { .. } => {
            return Err(err(
                ErrorClass::InvalidRequest,
                "invalid_source",
                "a structured source is fetched by the API; it has no Acquire step",
            ));
        }
        Source::Crawl { .. } | Source::Sitemap { .. } => {
            return Err(err(
                ErrorClass::InvalidRequest,
                "invalid_source",
                "a crawl / sitemap parent has only a Discover step",
            ));
        }
    };
    Ok((
        done(output),
        vec![(StepKind::Scan, String::new(), json!({}))],
    ))
}

/// A crawl child's prefetched original (module `discover`): infected → the job is rejected like
/// a URL job's; stored with the size Discover recorded → recorded as the original with the
/// page's title, final URL and scan verdict (no scrape); missing or of another size → `Missing`
/// (Acquire scrapes the page itself).
async fn reuse_prefetched(
    ctx: &Ctx,
    step: &ClaimedStep,
    key: &str,
    p: &Prefetched,
) -> Result<Reuse, StepError> {
    if p.scan == "infected" {
        return Ok(Reuse::Infected(
            p.signature.clone().unwrap_or_else(|| "unknown".to_string()),
        ));
    }
    let size = match ctx.storage.size(key).await {
        Ok(size) => size,
        Err(StorageError::NotFound) => return Ok(Reuse::Missing),
        Err(e) => return Err(e.step_error(key)),
    };
    if size != p.bytes {
        tracing::warn!(job = %step.job_id, size, recorded = p.bytes, "prefetched original changed; scraping again");
        return Ok(Reuse::Missing);
    }
    let max = ctx.cfg.limits.max_upload_bytes;
    if size > max {
        return Err(too_large(size, max));
    }
    let mime = Detected::Markdown.mime();
    record_original(ctx, step, key, size, mime).await?;
    // Only an explicit `clean` is clean (rulings R25).
    let scan = if p.scan == "clean" {
        ScanVerdict::Clean
    } else {
        ScanVerdict::Unverified
    };
    let mut meta = Map::new();
    if let Some(title) = p.title.as_ref().filter(|t| !t.trim().is_empty()) {
        meta.insert("title".into(), json!(title));
    }
    meta.insert("final_url".into(), json!(p.final_url));
    meta.insert("mime".into(), json!(mime));
    meta.insert("scan".into(), json!(scan.as_str()));
    steps::merge_job_meta(&ctx.pool, step, &Value::Object(meta))
        .await
        .map_err(internal)?;
    Ok(Reuse::Stored(json!({
        "key": key, "bytes": size, "mime": mime, "scan": scan.as_str(),
        "final_url": p.final_url, "title": p.title, "images": p.images, "prefetched": true,
    })))
}

/// storage_service refused an original as infected (nothing was stored): the verdict goes into
/// `meta` first, then the job is `rejected` with code `infected`; the step completes with no
/// successors (rulings R2).
async fn reject_infected(ctx: &Ctx, step: &ClaimedStep, signature: String) -> StepResult {
    steps::merge_job_meta(
        &ctx.pool,
        step,
        &json!({ "scan": "infected", "signature": signature }),
    )
    .await
    .map_err(internal)?;
    let reason = StepError {
        class: ErrorClass::InvalidRequest,
        code: "infected".into(),
        message: signature.clone(),
        provider: Some("storage".into()),
        retry_after_s: None,
    };
    steps::reject_job(&ctx.pool, step, &reason)
        .await
        .map_err(internal)?;
    tracing::warn!(job = %step.job_id, signature, "original is infected; job rejected");
    Ok((
        done(json!({ "result": "infected", "signature": signature })),
        vec![],
    ))
}

async fn record_original(
    ctx: &Ctx,
    step: &ClaimedStep,
    key: &str,
    bytes: u64,
    mime: &str,
) -> Result<(), StepError> {
    assets::record(
        &ctx.pool,
        step,
        NewAsset {
            id: None,
            kind: AssetKind::Original,
            object_key: key,
            bytes,
            mime,
            page: None,
        },
    )
    .await
    .map(drop)
    .map_err(internal)
}

// ---------------------------------------------------------------------------------------------
// Scan

/// The original was virus-scanned by storage_service when it was stored (the API's upload, or
/// Acquire's for a URL job); this step only enforces the verdict recorded in `meta.scan`
/// (rulings R25): `clean` → Detect; `infected` → the job is `rejected`; anything else (stored
/// with `virus_scanned=false`, or no verdict at all) is Retryable `scan_unavailable`, never clean.
/// A retry cannot change a recorded verdict, so such a job fails once its attempts run out.
async fn scan(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let job = load_job(ctx, step).await?;
    match job.meta.get("scan").and_then(Value::as_str) {
        Some("clean") => Ok((
            done(json!({ "result": "clean" })),
            vec![(StepKind::Detect, String::new(), json!({}))],
        )),
        Some("infected") => {
            let signature = job
                .meta
                .get("signature")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            reject_infected(ctx, step, signature).await
        }
        other => Err(StepError {
            class: ErrorClass::Retryable,
            code: "scan_unavailable".into(),
            message: format!(
                "the original has no clean virus-scan verdict (meta.scan = {other:?}); \
                 storage_service stored it without scanning"
            ),
            provider: Some("storage".into()),
            retry_after_s: None,
        }),
    }
}

// ---------------------------------------------------------------------------------------------
// Detect

/// Classifies the original by its bytes (a URL job's markdown is `Markdown` by construction) and
/// enqueues Convert with `{"detected": ...}`.
async fn detect(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let job = load_job(ctx, step).await?;
    let detected = match source(&job)? {
        Source::Url { .. } => Detected::Markdown,
        Source::Upload { filename, .. } => {
            let key = original_key(step.tenant, step.job_id);
            classify_original(ctx, &key, filename.as_deref()).await?
        }
        Source::Api { .. }
        | Source::Datatable { .. }
        | Source::Crawl { .. }
        | Source::Sitemap { .. } => {
            return Err(err(
                ErrorClass::InvalidRequest,
                "invalid_source",
                "a structured, crawl or sitemap source has no Detect step",
            ));
        }
    };
    if let Detected::Audio(mime) | Detected::Video(mime) = &detected
        && ctx
            .providers
            .transcribe(step.tenant, Some(step.job_id))
            .await
            .map_err(resolve_err)?
            .is_none()
    {
        return Err(err(
            ErrorClass::InvalidRequest,
            "unsupported_no_provider",
            format!("{mime} needs a transcribe provider and none is configured"),
        ));
    }
    let detected_json = serde_json::to_value(&detected)
        .map_err(|e| internal(IngestError::InvalidInput(e.to_string())))?;
    steps::merge_job_meta(&ctx.pool, step, &json!({ "mime": detected.mime() }))
        .await
        .map_err(internal)?;
    Ok((
        done(json!({ "detected": detected_json, "mime": detected.mime() })),
        vec![(
            StepKind::Convert,
            String::new(),
            json!({ "detected": detected_json }),
        )],
    ))
}

/// Classifies the original. Reads the first 64 KiB; a body that ends there, or that is not a
/// zip, is sniffed from those bytes. A larger zip container needs its central directory (at the
/// END) and storage_service has no Range reads (rulings R27): the rest of the same download goes
/// to an anonymous scratch file, bounded by `max_upload_bytes`, and `detect::sniff_zip_file`
/// classifies it from there (never buffered whole in memory).
async fn classify_original(
    ctx: &Ctx,
    key: &str,
    filename: Option<&str>,
) -> Result<Detected, StepError> {
    let unsupported = |e: IngestError| match e {
        IngestError::UnsupportedType(mime) => err(
            ErrorClass::InvalidRequest,
            "unsupported_type",
            format!("unsupported type: {mime}"),
        ),
        e => internal(e),
    };
    let mut stream = ctx
        .storage
        .get_stream(key)
        .await
        .map_err(|e| e.step_error(key))?;
    let mut head = BytesMut::new();
    let mut complete = true;
    while let Some(chunk) = stream.next().await {
        head.extend_from_slice(&chunk.map_err(|e| e.step_error(key))?);
        if head.len() >= DETECT_HEAD {
            complete = false;
            break;
        }
    }
    if complete || !detect::is_zip(&head) {
        drop(stream); // stops the download
        return detect::sniff(&head, filename).map_err(unsupported);
    }
    let max = ctx.cfg.limits.max_upload_bytes;
    let io = |e: std::io::Error| internal(IngestError::Io(e));
    // Unlinked at creation: removed by the OS however this step ends.
    let std_file = tempfile::tempfile().map_err(io)?;
    let mut file = tokio::fs::File::from_std(std_file.try_clone().map_err(io)?);
    let mut total = head.len() as u64;
    {
        use tokio::io::AsyncWriteExt;
        file.write_all(&head).await.map_err(io)?;
        drop(head);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| e.step_error(key))?;
            total += chunk.len() as u64;
            if total > max {
                return Err(too_large(total, max));
            }
            file.write_all(&chunk).await.map_err(io)?;
        }
        file.flush().await.map_err(io)?;
    }
    drop(file);
    tokio::task::spawn_blocking(move || detect::sniff_zip_file(std_file))
        .await
        .map_err(|e| internal(IngestError::Io(std::io::Error::other(e))))?
        .map_err(unsupported)
}

// ---------------------------------------------------------------------------------------------
// Convert

/// `ingest_jobs.options` as Convert reads it; missing fields take their defaults.
#[derive(Debug, Default, Deserialize)]
struct Options {
    #[serde(default)]
    ocr: OcrMode,
    #[serde(default)]
    captions: bool,
}

/// Turns the original into pages of markdown (artefact `convert.json`: `{"pages": [...]}`),
/// stores the figures in storage_service, and enqueues the enrich steps (OcrPages, Caption) plus Bundle:
/// - Text / Markdown: passed through as one page (no docling, no OCR).
/// - Html / Docx / Pptx / Xlsx / Epub / Odt / Pdf: docling with `do_ocr=false` (the text layer),
///   split on its page breaks. A Pdf over `max_pdf_pages` pages fails `too_many_pages` before
///   anything is stored or enqueued. Only a Pdf gets OCR pages.
/// - Image: one page, the placeholder of the original itself; OCR of page 0 and a caption.
///
/// OCR pages by `options.ocr`: `off` none, `force` all, `auto` the low-text ones. With an `ocr`
/// provider: one OcrPages step per contiguous run of at most 8 pages (key `p{first}-{last}`);
/// without one: the same batches as OcrPages steps `cpu-p{first}-{last}` on docling
/// (`do_ocr=true, force_ocr=true`, `page_range` = the batch). A PDF is converted with
/// `page_range` 1..=max_pdf_pages+1, so a huge PDF costs docling at most one page past the limit
/// before `too_many_pages`. The whole step holds the pod's heavy-work slot (`Ctx::heavy_slot`).
/// A Caption step per figure (key = asset id); with captions off or no vision provider it carries
/// `skip` = `disabled` / `no_provider` and ends `skipped`.
async fn convert(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let _slot = ctx.heavy_slot().await;
    let detected: Detected =
        serde_json::from_value(step.input.get("detected").cloned().unwrap_or(Value::Null))
            .map_err(|e| {
                err(
                    ErrorClass::InvalidRequest,
                    "invalid_input",
                    format!("convert input has no valid `detected`: {e}"),
                )
            })?;
    if let Detected::Audio(_) | Detected::Video(_) = &detected {
        return convert_audio(ctx, step, &detected).await;
    }
    let job = load_job(ctx, step).await?;
    let options: Options = serde_json::from_value(job.options.clone()).map_err(|e| {
        err(
            ErrorClass::InvalidRequest,
            "invalid_options",
            format!("job options: {e}"),
        )
    })?;
    let limits = &ctx.cfg.limits;
    let (tenant, job_id) = (step.tenant, step.job_id);
    let key = original_key(tenant, job_id);
    let original = ctx
        .storage
        .get_bytes(&key, limits.max_upload_bytes)
        .await
        .map_err(|e| e.step_error(&key))?;
    let original_len = original.len() as u64;

    let mut docling_report = Value::Null;
    // (asset id, blob key, mime) of every image to caption.
    let mut to_caption: Vec<(uuid::Uuid, String, String)> = Vec::new();
    let mut dropped = figures::Dropped::default();
    let (pages, low_text, ocr_eligible) = match &detected {
        Detected::Image(mime) => {
            let id = assets::record(
                &ctx.pool,
                step,
                NewAsset {
                    id: None,
                    kind: AssetKind::Original,
                    object_key: &key,
                    bytes: original.len() as u64,
                    mime,
                    page: None,
                },
            )
            .await
            .map_err(internal)?;
            to_caption.push((id, key.clone(), mime.clone()));
            (vec![placeholder(id)], vec![0], true)
        }
        _ => {
            let (md, split) = match &detected {
                Detected::Text | Detected::Markdown => (text_of(&original), false),
                d => {
                    let r = ctx
                        .docling
                        .convert(
                            original.clone(),
                            docling_filename(d),
                            DoclingOpts {
                                do_ocr: false,
                                force_ocr: false,
                                ocr_engine: limits.docling_ocr_engine.clone(),
                                // One page past the limit is enough to know it is exceeded:
                                // docling never converts the rest of a huge PDF.
                                page_range: (*d == Detected::Pdf)
                                    .then(|| (1, limits.max_pdf_pages.saturating_add(1))),
                                embed_images: true,
                            },
                        )
                        .await?;
                    docling_report = json!({
                        "status": r.status, "errors": r.errors, "missing_pages": r.missing_pages,
                    });
                    (r.markdown, true)
                }
            };
            drop(original);
            let raw = if split {
                split_pages(&md)
            } else {
                vec![md.clone()]
            };
            if detected == Detected::Pdf && raw.len() > limits.max_pdf_pages as usize {
                return Err(err(
                    ErrorClass::InvalidRequest,
                    "too_many_pages",
                    // docling converted at most one page past the limit (its page_range).
                    format!(
                        "the PDF has more than {} pages (the limit)",
                        limits.max_pdf_pages
                    ),
                ));
            }
            let low_text = low_text_pages(&raw, limits.ocr_min_chars_per_page);
            drop(raw);
            let x = extract_figures(
                &md,
                limits.figure_min_bytes,
                limits.max_figures_per_job,
                |i| figure_id(job_id, i),
            );
            dropped = x.dropped;
            let pages = if split {
                split_pages(&x.markdown)
            } else {
                vec![x.markdown]
            };
            let page_of = figure_pages(&pages);
            // One figure decoded and uploaded at a time, from the docling markdown `md`.
            for fig in x.figures {
                let page = page_of.get(&fig.id).copied().unwrap_or(0);
                let bytes = fig.decode(&md).ok_or_else(|| {
                    internal(IngestError::InvalidInput(format!(
                        "figure {} no longer decodes",
                        fig.idx
                    )))
                })?;
                let fkey = figure_key(tenant, job_id, fig.id);
                let len = bytes.len() as u64;
                put_blob(ctx, &fkey, bytes, fig.mime).await?;
                assets::record(
                    &ctx.pool,
                    step,
                    NewAsset {
                        id: Some(fig.id),
                        kind: AssetKind::Figure,
                        object_key: &fkey,
                        bytes: len,
                        mime: fig.mime,
                        page: Some(page as i32),
                    },
                )
                .await
                .map_err(internal)?;
                to_caption.push((fig.id, fkey, fig.mime.to_string()));
            }
            (pages, low_text, detected == Detected::Pdf)
        }
    };
    let page_count = pages.len();

    let mut next: Vec<(StepKind, String, Value)> = Vec::new();
    let selected = if ocr_eligible {
        options.ocr.select(page_count, &low_text)
    } else {
        vec![]
    };
    // An original over the provider input cap goes to the docling-cpu path instead.
    let over_cap = original_len > limits.max_ocr_input_bytes;
    // The job's provider (its store's, an ancestor's or its tenant's registration, else the dev
    // default) decides the path.
    let has_ocr = !selected.is_empty()
        && ctx
            .providers
            .ocr(step.tenant, Some(step.job_id))
            .await
            .map_err(resolve_err)?
            .is_some();
    let ocr_reason = (has_ocr && over_cap).then_some("ocr_input_too_large");
    let ocr_engine = if selected.is_empty() {
        None
    } else if has_ocr && !over_cap {
        for batch in ocr_batches(&selected, OCR_PAGES_PER_STEP) {
            next.push((
                StepKind::OcrPages,
                batch_key(&batch),
                json!({ "pages": batch, "engine": "provider", "mime": detected.mime() }),
            ));
        }
        Some("provider")
    } else {
        // Batched like the provider path (rulings R37): each step OCRs one contiguous run of
        // at most 8 pages through docling's `page_range`, so one step's CPU time is bounded
        // whatever the document's length.
        for batch in ocr_batches(&selected, OCR_PAGES_PER_STEP) {
            next.push((
                StepKind::OcrPages,
                format!("cpu-{}", batch_key(&batch)),
                json!({
                    "pages": batch, "engine": "docling-cpu", "mime": detected.mime(),
                    "do_ocr": true, "force_ocr": true, "ocr_engine": limits.docling_ocr_engine,
                }),
            ));
        }
        Some("docling-cpu")
    };
    let skip = if !options.captions {
        Some("disabled")
    } else if !to_caption.is_empty()
        && ctx
            .providers
            .vision(step.tenant, Some(step.job_id))
            .await
            .map_err(resolve_err)?
            .is_none()
    {
        Some("no_provider")
    } else {
        None
    };
    let kept = to_caption.len();
    for (id, object_key, mime) in to_caption {
        let mut input = json!({ "asset_id": id, "object_key": object_key, "mime": mime });
        if let Some(reason) = skip {
            input["skip"] = json!(reason);
        }
        next.push((StepKind::Caption, id.to_string(), input));
    }
    let ckey = CONVERT_ARTIFACT;
    next.push((
        StepKind::Bundle,
        String::new(),
        json!({ "convert_key": ckey }),
    ));

    put_json_artifact(ctx, step, ckey, &json!({ "pages": pages })).await?;
    steps::merge_job_meta(
        &ctx.pool,
        step,
        &json!({ "mime": detected.mime(), "pages": page_count }),
    )
    .await
    .map_err(internal)?;
    Ok((
        done(json!({
            "convert_key": ckey,
            "page_count": page_count,
            "low_text": low_text,
            "ocr": {
                "mode": options.ocr.as_str(), "engine": ocr_engine, "pages": selected,
                "reason": ocr_reason,
            },
            "figures": {
                "kept": if matches!(detected, Detected::Image(_)) { 0 } else { kept },
                "dropped": dropped.total(),
                "dropped_small": dropped.small,
                "dropped_unsupported": dropped.unsupported,
                "dropped_over_cap": dropped.over_cap,
            },
            "docling": docling_report,
        })),
        next,
    ))
}

/// Page index of every figure placeholder in `pages`, in one pass over the text.
fn figure_pages(pages: &[String]) -> std::collections::HashMap<uuid::Uuid, usize> {
    const MARK: &str = "<!-- figure:";
    let mut out = std::collections::HashMap::new();
    for (i, page) in pages.iter().enumerate() {
        for (at, _) in page.match_indices(MARK) {
            let rest = &page[at + MARK.len()..];
            if let Some(id) = rest.get(..36).and_then(|s| uuid::Uuid::parse_str(s).ok()) {
                out.entry(id).or_insert(i);
            }
        }
    }
    out
}

/// The filename docling sees: docling picks its input format from the extension.
fn docling_filename(d: &Detected) -> &'static str {
    match d {
        Detected::Pdf => "document.pdf",
        Detected::Html => "document.html",
        Detected::Docx => "document.docx",
        Detected::Pptx => "document.pptx",
        Detected::Xlsx => "document.xlsx",
        Detected::Epub => "document.epub",
        Detected::Odt => "document.odt",
        Detected::Markdown => "document.md",
        Detected::Text => "document.txt",
        Detected::Image(_) | Detected::Audio(_) | Detected::Video(_) => "document.bin",
    }
}

/// A text original as a string: UTF-8 (BOM stripped) or UTF-16 with BOM, else lossy UTF-8.
fn text_of(bytes: &[u8]) -> String {
    detect::decode_text(bytes).unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
}

/// Stores a binary artefact in storage_service (scanned there like every upload).
async fn put_blob(ctx: &Ctx, key: &str, body: Vec<u8>, mime: &str) -> Result<(), StepError> {
    ctx.storage
        .upload_bytes(key, body, mime)
        .await
        .map(drop)
        .map_err(|e| e.step_error(key))
}

/// Writes a JSON artefact of the step's job to `ingest_artifacts` (fenced).
async fn put_json_artifact(
    ctx: &Ctx,
    step: &ClaimedStep,
    key: &str,
    v: &Value,
) -> Result<(), StepError> {
    let body =
        serde_json::to_string(v).map_err(|e| internal(IngestError::InvalidInput(e.to_string())))?;
    artifacts::put(&ctx.pool, step, key, &body)
        .await
        .map_err(internal)
}

// ---------------------------------------------------------------------------------------------
// Convert: audio / video

/// Audio/video: the original goes to a scratch dir (removed when this returns), ffprobe measures
/// it (over `max_audio_seconds` → `too_long`, nothing stored), ffmpeg cuts it into mono 16 kHz
/// mp3 segments of `audio_segment_s`; each is stored (storage_service) at `audio/{n:04}.mp3` with
/// an `audio_segment` asset row and gets a TranscribeSegment step (key `s{n:04}`, input
/// `{offset_s, end_s, segment_key}`). Then Bundle (`{"transcript_keys": [artefact names]}`). No
/// convert.json.
async fn convert_audio(ctx: &Ctx, step: &ClaimedStep, detected: &Detected) -> StepResult {
    let limits = &ctx.cfg.limits;
    let (tenant, job_id) = (step.tenant, step.job_id);
    let key = original_key(tenant, job_id);
    let dir = tempfile::Builder::new()
        .prefix(audio::SCRATCH_PREFIX)
        .tempdir()
        .map_err(|e| internal(IngestError::Io(e)))?;
    let input = dir.path().join("original");
    ctx.storage
        .get_to_file(&key, &input, limits.max_upload_bytes)
        .await
        .map_err(|e| e.step_error(&key))?;

    let duration = audio::probe_duration(&input).await?;
    if duration > limits.max_audio_seconds as f64 {
        return Err(err(
            ErrorClass::InvalidRequest,
            "too_long",
            format!(
                "the media runs {duration:.1} s; the limit is {} s",
                limits.max_audio_seconds
            ),
        ));
    }
    let out_dir = dir.path().join("segments");
    tokio::fs::create_dir(&out_dir)
        .await
        .map_err(|e| internal(IngestError::Io(e)))?;
    // The probe above is the container's claim; the decode is bounded and measured again here,
    // before anything is stored or enqueued.
    let segments = audio::segment_audio(
        &input,
        &out_dir,
        limits.audio_segment_s,
        limits.max_audio_seconds,
    )
    .await?;

    let mut next = Vec::with_capacity(segments.len() + 1);
    let mut transcript_keys = Vec::with_capacity(segments.len());
    for (n, seg) in segments.iter().enumerate() {
        let akey = audio_key(tenant, job_id, n);
        // Streamed from the scratch dir.
        let len = ctx
            .storage
            .upload_file(&akey, &seg.path, "audio/mpeg")
            .await
            .map_err(|e| e.step_error(&akey))?
            .size;
        assets::record(
            &ctx.pool,
            step,
            NewAsset {
                id: None,
                kind: AssetKind::AudioSegment,
                object_key: &akey,
                bytes: len,
                mime: "audio/mpeg",
                page: None,
            },
        )
        .await
        .map_err(internal)?;
        let skey = format!("s{n:04}");
        transcript_keys.push(transcript_artifact(&skey));
        next.push((
            StepKind::TranscribeSegment,
            skey,
            json!({ "offset_s": seg.start_s, "end_s": seg.end_s, "segment_key": akey }),
        ));
    }
    next.push((
        StepKind::Bundle,
        String::new(),
        json!({ "transcript_keys": transcript_keys }),
    ));
    steps::merge_job_meta(
        &ctx.pool,
        step,
        &json!({ "mime": detected.mime(), "duration_s": duration }),
    )
    .await
    .map_err(internal)?;
    Ok((
        done(json!({
            "duration_s": duration,
            "segment_s": limits.audio_segment_s,
            "segments": segments.len(),
            "media": if matches!(detected, Detected::Video(_)) { "video" } else { "audio" },
        })),
        next,
    ))
}

// ---------------------------------------------------------------------------------------------
// OcrPages

/// An OcrPages step's input, as Convert enqueues it.
#[derive(Debug, Deserialize)]
struct OcrStep {
    pages: Vec<u32>,
    engine: String,
    mime: String,
    #[serde(default)]
    ocr_engine: Option<String>,
}

/// OCRs the step's pages of the original and writes `{"<page>": markdown}` to the artefact
/// `ocr/{step key}.json` (only the requested pages).
/// - `engine: provider`: the OCR provider, sent the whole original (a PDF with `pages`, an image
///   without). An original over `max_ocr_input_bytes` → `ocr_input_too_large` (no page subset can
///   be cut from a PDF here); a requested page missing from the answer → `ocr_missing_page`.
///   Records provider, model, usage (pages) and cost; usage the provider did not report is null.
/// - `engine: docling-cpu`: docling with `do_ocr=true, force_ocr=true`, split on its page
///   breaks; its page count must equal Convert's (`meta.pages`) or the step fails
///   `ocr_page_mismatch` rather than mis-assign pages. Embedded images are dropped from the OCR
///   text. Provider `docling-cpu`, no usage, no cost.
async fn ocr_pages(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let input: OcrStep = step_input(step)?;
    if input.pages.is_empty() {
        return Err(err(
            ErrorClass::InvalidRequest,
            "invalid_input",
            "an OcrPages step needs at least one page",
        ));
    }
    let key = original_key(step.tenant, step.job_id);
    let (pages, mut out) = match input.engine.as_str() {
        "provider" => ocr_with_provider(ctx, step, &input, &key).await?,
        "docling-cpu" => ocr_with_docling(ctx, step, &input, &key).await?,
        other => {
            return Err(err(
                ErrorClass::InvalidRequest,
                "invalid_input",
                format!("unknown OCR engine {other:?}"),
            ));
        }
    };
    let okey = ocr_artifact(&step.key);
    let written = pages.len();
    let doc: Map<String, Value> = pages
        .into_iter()
        .map(|(i, md)| (i.to_string(), Value::String(md)))
        .collect();
    put_json_artifact(ctx, step, &okey, &Value::Object(doc)).await?;
    out.output = json!({
        "ocr_key": okey, "engine": input.engine, "pages": input.pages, "written": written,
    });
    Ok((out, vec![]))
}

async fn ocr_with_provider(
    ctx: &Ctx,
    step: &ClaimedStep,
    input: &OcrStep,
    key: &str,
) -> Result<(std::collections::BTreeMap<u32, String>, StepOutput), StepError> {
    let Some(pcfg) = ctx
        .providers
        .ocr(step.tenant, Some(step.job_id))
        .await
        .map_err(resolve_err)?
    else {
        return Err(not_configured(Capability::Ocr, "OcrPages"));
    };
    let max = ctx.cfg.limits.max_ocr_input_bytes;
    let size = ctx.storage.size(key).await.map_err(|e| e.step_error(key))?;
    if size > max {
        return Err(err(
            ErrorClass::InvalidRequest,
            "ocr_input_too_large",
            format!("the original holds {size} bytes; an OCR provider is sent at most {max}"),
        ));
    }
    let bytes = ctx
        .storage
        .get_bytes(key, max)
        .await
        .map_err(|e| e.step_error(key))?;
    // An image is one page: sent whole (`pages` is for documents), answered as index 0.
    let pages = (!input.mime.starts_with("image/")).then(|| input.pages.clone());
    let (result, latency) = {
        let (_permit, client) = pcfg.client.acquire().await;
        let t0 = Instant::now();
        let r = client
            .ocr(OcrInput {
                bytes,
                mime: input.mime.clone(),
                pages,
            })
            .await?;
        (r, t0.elapsed())
    };
    // Paid: recorded now, whatever happens to the step after this.
    record_paid(
        ctx,
        step,
        meter(&pcfg, Capability::Ocr, result.usage, latency),
    )
    .await;
    let mut got: std::collections::BTreeMap<u32, String> = result
        .pages
        .into_iter()
        .map(|p| (p.index, p.markdown))
        .collect();
    let missing: Vec<u32> = input
        .pages
        .iter()
        .copied()
        .filter(|p| !got.contains_key(p))
        .collect();
    if !missing.is_empty() {
        return Err(StepError {
            class: ErrorClass::InvalidRequest,
            code: "ocr_missing_page".into(),
            message: format!(
                "the OCR provider returned no text for requested pages {missing:?} (of {:?})",
                input.pages
            ),
            provider: Some(pcfg.dialect.clone()),
            retry_after_s: None,
        });
    }
    got.retain(|i, _| input.pages.contains(i));
    Ok((got, metered(&pcfg, result.usage)))
}

async fn ocr_with_docling(
    ctx: &Ctx,
    step: &ClaimedStep,
    input: &OcrStep,
    key: &str,
) -> Result<(std::collections::BTreeMap<u32, String>, StepOutput), StepError> {
    let job = load_job(ctx, step).await?;
    let expected = job
        .meta
        .get("pages")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            err(
                ErrorClass::InvalidRequest,
                "invalid_input",
                "the job has no page count from Convert (meta.pages)",
            )
        })?;
    // The step's pages as one window (Convert enqueues contiguous batches); an image is a
    // single page, converted whole.
    let first = *input.pages.iter().min().expect("checked non-empty");
    let last = *input.pages.iter().max().expect("checked non-empty");
    let window = (!input.mime.starts_with("image/")).then_some((first + 1, last + 1));
    let _slot = ctx.heavy_slot().await;
    let bytes = ctx
        .storage
        .get_bytes(key, ctx.cfg.limits.max_upload_bytes)
        .await
        .map_err(|e| e.step_error(key))?;
    let opts = DoclingOpts {
        do_ocr: true,
        force_ocr: true,
        ocr_engine: input
            .ocr_engine
            .clone()
            .unwrap_or_else(|| ctx.cfg.limits.docling_ocr_engine.clone()),
        page_range: window,
        // Only the text is wanted: no page scans base64'd into the answer.
        embed_images: false,
    };
    let out = StepOutput {
        provider: Some("docling-cpu".into()),
        ..StepOutput::default()
    };
    let r = match ctx
        .docling
        .convert(bytes, ocr_filename(&input.mime), opts)
        .await
    {
        Ok(r) => r,
        // OCR found no text anywhere: nothing overrides Convert's pages.
        Err(e) if e.code == "empty_conversion" => {
            tracing::info!(job = %step.job_id, "docling OCR produced no text");
            return Ok((Default::default(), out));
        }
        Err(e) => return Err(e),
    };
    let raw = split_pages(&r.markdown);
    // Pages of the window, or of the whole input (an image).
    let (offset, want) = match window {
        Some((a, b)) => (first, u64::from(b - a + 1)),
        None => (0, expected),
    };
    let in_doc = u64::from(last) < expected;
    let bad = input
        .pages
        .iter()
        .find(|&&p| (p - offset) as usize >= raw.len());
    if raw.len() as u64 != want || bad.is_some() || !in_doc {
        return Err(err(
            ErrorClass::InvalidRequest,
            "ocr_page_mismatch",
            format!(
                "docling OCR returned {} pages for a window of {want}; Convert saw {expected} \
                 (requested {:?})",
                raw.len(),
                input.pages
            ),
        ));
    }
    let pages = input
        .pages
        .iter()
        .map(|&p| {
            // A page scan left in the text (placeholder or image) is not OCR text.
            let text = extract_figures(&raw[(p - offset) as usize], u64::MAX, 0, |_| {
                uuid::Uuid::nil()
            });
            (p, strip_image_placeholders(&text.markdown))
        })
        .collect();
    Ok((pages, out))
}

/// `md` without docling's `<!-- image -->` picture placeholder lines (OCR text only).
fn strip_image_placeholders(md: &str) -> String {
    md.lines()
        .filter(|l| l.trim() != "<!-- image -->")
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

/// The filename docling sees for an OCR input (docling picks its format from the extension).
fn ocr_filename(mime: &str) -> &'static str {
    match mime {
        "image/png" => "document.png",
        "image/jpeg" => "document.jpg",
        "image/tiff" => "document.tiff",
        "image/bmp" => "document.bmp",
        "image/webp" => "document.webp",
        "image/gif" => "document.gif",
        _ => "document.pdf",
    }
}

// ---------------------------------------------------------------------------------------------
// Caption

/// A Caption that Convert marked `skip` (captions disabled, or no vision provider) ends
/// `skipped` with `{"skipped": reason}`. A vision call refused with a non-retryable class also
/// ends `skipped`, with `{"skipped": "caption_failed", "asset_id", "error"}`: the job goes on and
/// the figure keeps its bare link (rulings R40). A Retryable failure is returned; once the step's
/// budget is used up `steps::fail` skips it the same way (as it does a step/slot-wait timeout or
/// an expired lease on the last attempt). Otherwise: the asset's bytes (key and mime from its
/// `ingest_assets` row; no row → `asset_missing`) go to the vision provider and the caption is
/// written to `ingest_assets.caption` (fenced). Records provider, model, usage (tokens), cost.
/// An answer that is a refusal ("I'm sorry, but I cannot assist with that.", see
/// [`vision::is_refusal`]) is never stored as the caption: the step ends `skipped` with
/// `{"skipped": "caption_refused", "asset_id"}` (provider, model and usage kept; the call was paid,
/// so it is metered like any other) and
/// the figure keeps its bare link (R40).
async fn caption(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    if let Some(reason) = step.input.get("skip").and_then(Value::as_str) {
        return Ok((
            StepOutput {
                output: json!({ "skipped": reason }),
                skipped: true,
                ..StepOutput::default()
            },
            vec![],
        ));
    }
    #[derive(Deserialize)]
    struct Input {
        asset_id: uuid::Uuid,
    }
    let Input { asset_id } = step_input(step)?;
    let Some(pcfg) = ctx
        .providers
        .vision(step.tenant, Some(step.job_id))
        .await
        .map_err(resolve_err)?
    else {
        return Err(not_configured(Capability::Vision, "Caption"));
    };
    let asset_missing = || {
        err(
            ErrorClass::InvalidRequest,
            "asset_missing",
            format!("the job has no asset {asset_id}"),
        )
    };
    let (key, mime) = assets::key_and_mime(&ctx.pool, step.job_id, asset_id)
        .await
        .map_err(internal)?
        .ok_or_else(asset_missing)?;
    let bytes = ctx
        .storage
        .get_bytes(&key, ctx.cfg.limits.max_upload_bytes)
        .await
        .map_err(|e| e.step_error(&key))?;
    let (called, latency) = {
        let (_permit, client) = pcfg.client.acquire().await;
        let t0 = Instant::now();
        let r = client.caption(&bytes, &mime).await;
        (r, t0.elapsed())
    };
    let (text, usage) = match called {
        Ok(r) => {
            // Paid (a refusal too): recorded now, whatever happens to the step after this.
            record_paid(ctx, step, meter(&pcfg, Capability::Vision, r.1, latency)).await;
            r
        }
        Err(e) => {
            let e = StepError::from(e);
            // A caption is optional enrichment (rulings R40): a provider refusal skips this
            // figure, which keeps its bare `![](asset:…)` link, instead of failing the whole
            // document. A Retryable failure is skipped by `steps::fail` once the budget is used.
            if e.class != ErrorClass::Retryable {
                tracing::warn!(
                    job = %step.job_id,
                    %asset_id,
                    code = %e.code,
                    class = e.class.as_str(),
                    attempt = step.attempt,
                    error = %e.message,
                    "caption failed; figure left uncaptioned"
                );
                return Ok((
                    StepOutput {
                        output: json!({
                            "skipped": "caption_failed", "asset_id": asset_id, "error": e,
                        }),
                        provider: Some(pcfg.dialect.clone()),
                        model: Some(pcfg.model.clone()),
                        skipped: true,
                        ..StepOutput::default()
                    },
                    vec![],
                ));
            }
            return Err(e);
        }
    };
    if vision::is_refusal(&text) {
        tracing::warn!(
            job = %step.job_id,
            %asset_id,
            attempt = step.attempt,
            chars = text.chars().count(),
            "vision provider refused to caption; figure left uncaptioned"
        );
        return Ok((
            StepOutput {
                output: json!({ "skipped": "caption_refused", "asset_id": asset_id }),
                provider: Some(pcfg.dialect.clone()),
                model: Some(pcfg.model.clone()),
                usage: usage
                    .units
                    .is_some()
                    .then(|| serde_json::to_value(usage).expect("Usage serializes")),
                skipped: true,
                ..StepOutput::default()
            },
            vec![],
        ));
    }
    assets::set_caption(&ctx.pool, step, asset_id, &text)
        .await
        .map_err(|e| match e {
            IngestError::NotFound => asset_missing(),
            e => internal(e),
        })?;
    let mut out = metered(&pcfg, usage);
    out.output = json!({ "asset_id": asset_id, "chars": text.chars().count() });
    Ok((out, vec![]))
}

// ---------------------------------------------------------------------------------------------
// TranscribeSegment

/// Transcribes one audio segment (`segment_key`, under the job's `audio/` prefix) with the job's
/// `options.language` hint, shifts every segment by `offset_s` and writes
/// `{"segments": [{start_s, end_s, text}], "duration_s", "offset_s"}` to the artefact
/// `transcript/{step key}.json`. Records provider, model, usage (seconds), cost.
async fn transcribe_segment(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    #[derive(Deserialize)]
    struct Input {
        offset_s: f64,
        segment_key: String,
    }
    let Input {
        offset_s,
        segment_key,
    } = step_input(step)?;
    let prefix = format!("{}/{}/audio/", step.tenant, step.job_id);
    if !segment_key.starts_with(&prefix) || !(offset_s.is_finite() && offset_s >= 0.0) {
        return Err(err(
            ErrorClass::InvalidRequest,
            "invalid_input",
            format!("segment {segment_key:?} at {offset_s} s is not this job's audio"),
        ));
    }
    let Some(pcfg) = ctx
        .providers
        .transcribe(step.tenant, Some(step.job_id))
        .await
        .map_err(resolve_err)?
    else {
        return Err(not_configured(Capability::Transcribe, "TranscribeSegment"));
    };
    let job = load_job(ctx, step).await?;
    let language = job
        .options
        .get("language")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string);
    let bytes = ctx
        .storage
        .get_bytes(&segment_key, ctx.cfg.limits.max_upload_bytes)
        .await
        .map_err(|e| e.step_error(&segment_key))?;
    let filename = segment_key.rsplit('/').next().unwrap_or("segment.mp3");
    let (t, latency) = {
        let (_permit, client) = pcfg.client.acquire().await;
        let t0 = Instant::now();
        let r = client
            .transcribe(bytes, filename, language.as_deref())
            .await?;
        (r, t0.elapsed())
    };
    // Paid: recorded now, whatever happens to the step after this.
    record_paid(
        ctx,
        step,
        meter(&pcfg, Capability::Transcribe, t.usage, latency),
    )
    .await;
    let segments: Vec<Value> = t
        .segments
        .iter()
        .map(|s| {
            json!({
                "start_s": s.start_s + offset_s,
                "end_s": s.end_s + offset_s,
                "text": s.text,
            })
        })
        .collect();
    let tkey = transcript_artifact(&step.key);
    put_json_artifact(
        ctx,
        step,
        &tkey,
        &json!({ "segments": segments, "duration_s": t.duration_s, "offset_s": offset_s }),
    )
    .await?;
    let mut out = metered(&pcfg, t.usage);
    out.output = json!({
        "transcript_key": tkey, "segments": segments.len(), "offset_s": offset_s,
        "language": language,
    });
    Ok((out, vec![]))
}

// ---------------------------------------------------------------------------------------------
// Bundle

/// Builds the markdown bundle (`pipeline::bundle`) from the job's artefacts (`ingest_artifacts`)
/// and writes it to the artefact `bundle.md`, then enqueues Deliver with `{bundle_key, title}`:
/// - `{"convert_key"}` (documents, images): `convert.json`'s pages; the OCR text of every
///   completed OcrPages step (`ocr/{key}.json`); the captions of the figures the assembled pages
///   actually reference (`ingest_assets`; a placeholder without a row is dropped as an orphan).
/// - `{"transcript_keys"}` (audio/video): the transcript segments, in key order.
///
/// Title: the scraped page's `meta.title`, else the upload's filename without its extension. A
/// bundle with no content besides anchors/headers/title fails `empty_bundle`, one over
/// `MAX_BUNDLE_BYTES` fails `bundle_too_large` (both terminal; nothing stored or delivered).
async fn bundle(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    #[derive(Deserialize)]
    struct Input {
        #[serde(default)]
        convert_key: Option<String>,
        #[serde(default)]
        transcript_keys: Option<Vec<String>>,
    }
    let input: Input = step_input(step)?;
    let job = load_job(ctx, step).await?;
    let job_id = step.job_id;
    let title = job_title(&job);
    let mut b = BundleInput {
        title: title.clone(),
        ..BundleInput::default()
    };
    let mut ocr_pages: Vec<u32> = Vec::new();
    let mut segments = 0usize;
    match (input.convert_key, input.transcript_keys) {
        (Some(ckey), None) => {
            #[derive(Deserialize)]
            struct Converted {
                pages: Vec<String>,
            }
            let c: Converted = read_json(ctx, step, &ckey).await?;
            b.pages = c.pages;
            for key in steps::completed_keys(&ctx.pool, job_id, StepKind::OcrPages)
                .await
                .map_err(internal)?
            {
                let okey = ocr_artifact(&key);
                let pages: std::collections::BTreeMap<String, String> =
                    read_json(ctx, step, &okey).await?;
                for (page, md) in pages {
                    let page: u32 = page.parse().map_err(|_| {
                        err(
                            ErrorClass::InvalidRequest,
                            "invalid_input",
                            format!("{okey}: page key {page:?} is not a page index"),
                        )
                    })?;
                    b.ocr.insert(page, md);
                }
            }
            ocr_pages = b.ocr.keys().copied().collect();
        }
        (None, Some(keys)) => {
            #[derive(Deserialize)]
            struct Transcribed {
                segments: Vec<Segment>,
            }
            let mut all = Vec::new();
            for key in &keys {
                let t: Transcribed = read_json(ctx, step, key).await?;
                all.extend(t.segments);
            }
            segments = all.len();
            b.transcript = Some(all);
        }
        _ => {
            return Err(err(
                ErrorClass::InvalidRequest,
                "invalid_input",
                "bundle input needs exactly one of `convert_key` and `transcript_keys`",
            ));
        }
    }
    let refs = figure_refs(&b);
    b.captions = assets::captions(&ctx.pool, job_id, &refs)
        .await
        .map_err(internal)?;
    let orphans = refs.len() - b.captions.len();
    if !has_content(&b) {
        return Err(err(
            ErrorClass::InvalidRequest,
            "empty_bundle",
            "the job produced no text and no figures: nothing to deliver",
        ));
    }
    let md = build_bundle(&b);
    let bytes = md.len();
    // This bounds the markdown. Deliver's JSON body (`{text, source, title}`, escaped) can still
    // exceed strata's 16 MiB body cap for a bundle just under it; `StrataClient::deliver`
    // re-checks the serialised body and fails that terminally as `payload_too_large`.
    if bytes as u64 > MAX_BUNDLE_BYTES {
        return Err(err(
            ErrorClass::InvalidRequest,
            "bundle_too_large",
            format!("the bundle is {bytes} bytes; strata accepts at most {MAX_BUNDLE_BYTES}"),
        ));
    }
    let bkey = BUNDLE_ARTIFACT;
    artifacts::put(&ctx.pool, step, bkey, &md)
        .await
        .map_err(internal)?;
    Ok((
        done(json!({
            "bundle_key": bkey,
            "bytes": bytes,
            "pages": b.pages.len(),
            "ocr_pages": ocr_pages,
            "segments": segments,
            "figures": b.captions.len(),
            "orphans": orphans,
        })),
        vec![(
            StepKind::Deliver,
            String::new(),
            json!({ "bundle_key": bkey, "title": title }),
        )],
    ))
}

/// The bundle's title: `meta.title` (a URL job's page title), else the upload's filename without
/// its extension; single-line. `None` when neither gives one.
fn job_title(job: &Job) -> Option<String> {
    let clean = |s: &str| {
        let t = s.split_whitespace().collect::<Vec<_>>().join(" ");
        (!t.is_empty()).then_some(t)
    };
    if let Some(t) = job
        .meta
        .get("title")
        .and_then(Value::as_str)
        .and_then(clean)
    {
        return Some(t);
    }
    match source(job) {
        Ok(Source::Upload {
            filename: Some(f), ..
        }) => {
            let name = f.rsplit(['/', '\\']).next().unwrap_or(&f);
            let stem = match name.rsplit_once('.') {
                Some((stem, _)) if !stem.trim().is_empty() => stem,
                _ => name,
            };
            clean(stem)
        }
        _ => None,
    }
}

/// Artefact `key` of the step's job; never written → terminal `artifact_missing`. Artefacts are
/// read by `(job, key)`, so a step input can never name another job's artefact.
async fn read_artifact(ctx: &Ctx, step: &ClaimedStep, key: &str) -> Result<String, StepError> {
    artifacts::get(&ctx.pool, step.job_id, key)
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            err(
                ErrorClass::InvalidRequest,
                "artifact_missing",
                format!("the job has no artefact {key:?}"),
            )
        })
}

async fn read_json<T: serde::de::DeserializeOwned>(
    ctx: &Ctx,
    step: &ClaimedStep,
    key: &str,
) -> Result<T, StepError> {
    let body = read_artifact(ctx, step, key).await?;
    serde_json::from_str(&body).map_err(|e| {
        err(
            ErrorClass::InvalidRequest,
            "invalid_artefact",
            format!("{key}: {e}"),
        )
    })
}

// ---------------------------------------------------------------------------------------------
// Deliver

/// Sends the input's artefact (`bundle.md`, or a structured-source record) to strata (`POST
/// /api/stores/{store}/documents`, one attempt per run) with `source` = the input's own label (a
/// record), else the upload's filename or the URL job's final URL (cut to `MAX_SOURCE_CHARS`), the
/// input's title, the job's `meta.source_id` / `meta.source_run` (a copy's own
/// `knowledge.source_run` first) and the input's metadata (a re-ingest copy's source-document
/// fields — input `knowledge`, else `meta.knowledge` — carried as well), the job's `meta`
/// `topic_keys` / `supersedes` / `provenance` (`crate::doc_fields`; a copy's own win), then appends the returned
/// `document_id` to the job (fenced, once). strata answering `existing` is success: the output says
/// `skipped_duplicate: true` unless this job had already recorded that document (a retry after a
/// delivery that went through). The job then completes through `complete_and_enqueue` (R2).
async fn deliver(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    #[derive(Deserialize)]
    struct Input {
        bundle_key: String,
        #[serde(default)]
        title: Option<String>,
        /// A structured-source record's own `source` label (else [`delivery_source`]).
        #[serde(default)]
        source: Option<String>,
        /// A structured-source record's strata metadata.
        #[serde(default)]
        metadata: Option<Value>,
        /// A re-ingest copy's record: its source document's fields (`reingest`).
        #[serde(default)]
        knowledge: Option<Value>,
    }
    let Input {
        bundle_key,
        title,
        source,
        metadata,
        knowledge,
    } = step_input(step)?;
    let job = load_job(ctx, step).await?;
    let text = read_artifact(ctx, step, &bundle_key).await?;
    let source = match source {
        Some(s) => super::bundle::truncate_chars(&s, MAX_SOURCE_CHARS).to_string(),
        None => delivery_source(&job)?,
    };
    // A re-ingest copy's file job carries its source document's fields in `meta.knowledge`.
    let knowledge = knowledge.or_else(|| job.meta.get(crate::reingest::KNOWLEDGE_META).cloned());
    // The caller's topic_keys / supersedes / provenance (job `meta`); a copy's own win.
    let knowledge = crate::doc_fields::with_job_fields(&job.meta, knowledge);
    // A copy keeps its source document's run (`knowledge.source_run`), else the job's.
    let source_run = knowledge
        .as_ref()
        .and_then(|k| k.get("source_run"))
        .and_then(Value::as_str)
        .or_else(|| job.meta.get("source_run").and_then(Value::as_str));
    let origin = Origin {
        source_id: job.meta.get("source_id").and_then(Value::as_str),
        source_run,
        metadata: metadata.as_ref(),
        knowledge: knowledge.as_ref(),
    };
    let d = ctx
        .strata
        .deliver(
            job.tenant,
            job.store_id,
            &text,
            &source,
            title.as_deref(),
            &origin,
        )
        .await?;
    // Known gap: when an earlier attempt's delivery went through but its lease was lost before
    // `append_document_id` committed, strata now answers `existing` for this job's own document
    // and it is labelled `skipped_duplicate`. The id is still recorded exactly once.
    let ours = job.document_ids.contains(&d.document_id);
    steps::append_document_id(&ctx.pool, step, d.document_id)
        .await
        .map_err(internal)?;
    Ok((
        done(json!({
            "document_id": d.document_id,
            "duplicate": d.duplicate,
            "skipped_duplicate": d.duplicate && !ours,
            "source": source,
            "bytes": text.len(),
        })),
        vec![],
    ))
}

/// strata's `source`: the upload's filename (`upload:{job id}` without one) or the URL job's final
/// URL (its requested URL before Acquire recorded one), at most `MAX_SOURCE_CHARS` chars.
fn delivery_source(job: &Job) -> Result<String, StepError> {
    let nonempty = |s: &str| (!s.trim().is_empty()).then(|| s.trim().to_string());
    let s = match source(job)? {
        Source::Upload { filename, .. } => filename
            .as_deref()
            .and_then(nonempty)
            .unwrap_or_else(|| format!("upload:{}", job.id)),
        Source::Api { url }
        | Source::Datatable { url }
        | Source::Crawl { url }
        | Source::Sitemap { url } => url,
        Source::Url { url, .. } => job
            .meta
            .get("final_url")
            .and_then(Value::as_str)
            .and_then(nonempty)
            .unwrap_or(url),
    };
    Ok(super::bundle::truncate_chars(&s, MAX_SOURCE_CHARS).to_string())
}

/// A provider step's stored metering: provider (dialect) and model always; usage and cost only
/// when the provider reported units (unknown is null, never 0).
fn metered<T>(pcfg: &Resolved<T>, usage: Usage) -> StepOutput {
    StepOutput {
        provider: Some(pcfg.dialect.clone()),
        model: Some(pcfg.model.clone()),
        usage: usage
            .units
            .is_some()
            .then(|| serde_json::to_value(usage).expect("Usage serializes")),
        cost_micros: usage.cost_micros(pcfg.price_micros_per_unit),
        ..StepOutput::default()
    }
}

/// The billing view of a provider call that returned after `latency` (`metering::Meter`).
fn meter<T>(pcfg: &Resolved<T>, cap: Capability, usage: Usage, latency: Duration) -> Meter {
    Meter {
        capability: cap,
        provider: pcfg.billing_provider.clone(),
        model: pcfg.model.clone(),
        funding: pcfg.funding,
        usage,
        cost_micros: usage.cost_micros(pcfg.price_micros_per_unit),
        latency_ms: u64::try_from(latency.as_millis()).unwrap_or(u64::MAX),
        at: chrono::Utc::now(),
    }
}

/// Records a paid provider call's cost event (`metering::record_with_retry`: its own
/// transaction, a few tries over about a second). Never fails the step: the provider was already
/// paid, and a retry would call (and pay) it again. The meter is parked in `ctx.unrecorded`
/// BEFORE the first try and taken back only once a try succeeded, so a run aborted mid-retry
/// (shutdown grace, step deadline, lost lease) is still settled by the worker. When every try
/// failed it stays parked: the worker writes it in the completion transaction, or tries once more
/// and logs it as lost when the step does not complete.
async fn record_paid(ctx: &Ctx, step: &ClaimedStep, m: Meter) {
    ctx.unrecorded.put(step, m.clone());
    match crate::metering::record_with_retry(&ctx.pool, step, &m).await {
        Ok(()) => {
            ctx.unrecorded.take(step);
        }
        Err(e) => tracing::error!(
            job = %step.job_id,
            step = step.id,
            attempt = step.attempt,
            error = %e,
            "recording a paid provider call failed; it is written with the step's result instead"
        ),
    }
}

fn step_input<T: serde::de::DeserializeOwned>(step: &ClaimedStep) -> Result<T, StepError> {
    serde_json::from_value(step.input.clone()).map_err(|e| {
        err(
            ErrorClass::InvalidRequest,
            "invalid_input",
            format!("{} input: {e}", step.kind.as_str()),
        )
    })
}

// ---------------------------------------------------------------------------------------------
// helpers

async fn load_job(ctx: &Ctx, step: &ClaimedStep) -> Result<Job, StepError> {
    jobs::job(&ctx.pool, step.tenant, step.job_id)
        .await
        .map_err(internal)
}

fn source(job: &Job) -> Result<Source, StepError> {
    serde_json::from_value(job.source.clone()).map_err(|e| {
        err(
            ErrorClass::InvalidRequest,
            "invalid_source",
            format!(
                "job source is not an upload, url, api, datatable, crawl or sitemap source: {e}"
            ),
        )
    })
}

fn done(output: Value) -> StepOutput {
    StepOutput {
        output,
        ..StepOutput::default()
    }
}

fn err(class: ErrorClass, code: &str, message: impl Into<String>) -> StepError {
    StepError {
        class,
        code: code.to_string(),
        message: message.into(),
        provider: None,
        retry_after_s: None,
    }
}

fn too_large(size: u64, max: u64) -> StepError {
    err(
        ErrorClass::InvalidRequest,
        "too_large",
        format!("the original holds {size}+ bytes; the limit is {max}"),
    )
}

/// A DB (or other internal) failure: retryable. `LeaseLost` keeps its own code so the logs say
/// why; the worker's `fail` is then refused and the result dropped.
/// The step needs `cap` and the job's tenant has none (no registration and no dev default):
/// terminal, the job fails with a code the tenant can act on.
fn not_configured(cap: Capability, step: &str) -> StepError {
    err(
        ErrorClass::InvalidRequest,
        "provider_not_configured",
        format!(
            "the {step} step needs a {cap} provider and none is configured for this tenant; \
             register one with PUT /api/tenant/providers"
        ),
    )
}

/// A provider resolution failure: a stored registration this deployment refuses (its host, or a
/// missing STRATA_INGEST_PROVIDER_KEY) is terminal with its own code; anything else (the DB) is
/// retried.
fn resolve_err(e: IngestError) -> StepError {
    match e {
        IngestError::ProviderRejected { code, message } => {
            err(ErrorClass::InvalidRequest, code, message)
        }
        e @ IngestError::ProviderKeyUnavailable(_) => {
            err(ErrorClass::InvalidRequest, e.code(), e.to_string())
        }
        e => internal(e),
    }
}

fn internal(e: IngestError) -> StepError {
    let code = match e {
        IngestError::LeaseLost { .. } => "lease_lost",
        _ => "internal",
    };
    err(ErrorClass::Retryable, code, e.to_string())
}
