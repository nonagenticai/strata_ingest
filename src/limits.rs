//! Working defaults from the plan's "Defaults" table. Each `Limits` field is overridable by the
//! env var `STRATA_INGEST_` + its upper-snake name (e.g. `STRATA_INGEST_MAX_PDF_PAGES`).
//! Task 11 re-checks the thresholds against the corpus; change them here if measurements say so.

use std::collections::HashMap;

use crate::config::{get, parse};
use crate::error::{IngestError, Result};

/// The largest body storage_service can store: ClamAV scans every upload inside the request and
/// its `StreamMaxLength` is 100 MiB; a bigger body is refused with the same 503 as "scanner down"
/// (rulings R24). `max_upload_bytes` may be lowered, never raised past this.
pub const STORAGE_SCAN_LIMIT_BYTES: u64 = 100 * 1024 * 1024;
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = STORAGE_SCAN_LIMIT_BYTES;
/// Only small PDFs were measured against docling (a 3-page forced OCR: 54 s, 1.30 GiB); 500
/// pages keeps a born-digital conversion inside docling's 1800 s timeout (rulings R37). Convert
/// asks docling for at most one page past this, so a bigger PDF fails fast `too_many_pages`.
pub const DEFAULT_MAX_PDF_PAGES: u32 = 500;
pub const DEFAULT_MAX_AUDIO_SECONDS: u64 = 4 * 3600;
/// A page with fewer non-whitespace chars (image tags ignored) in docling's output is "low text".
pub const DEFAULT_OCR_MIN_CHARS_PER_PAGE: usize = 100;
/// Figures smaller than this are dropped (icons, bullets).
pub const DEFAULT_FIGURE_MIN_BYTES: u64 = 8 * 1024;
/// Figures kept per job (assets, blobs, Caption steps); later ones are dropped and counted.
pub const DEFAULT_MAX_FIGURES_PER_JOB: usize = 200;
/// ffmpeg segment length: mono 16 kHz 64 kbps mp3 ≈ 4.8 MB, under OpenAI's 25 MB upload cap.
pub const DEFAULT_AUDIO_SEGMENT_S: u64 = 600;
/// Attempts per step: with the backoff below the retries span 2+4+…+256+300 s ≈ 13.5 min, so a
/// dependency restart (storage-api roll, ClamAV loading signatures, docling/mlx-api restart) or a
/// provider 429 storm is survived instead of failing every in-flight job (final review I1, R34).
pub const DEFAULT_MAX_ATTEMPTS: i32 = 10;
/// Largest original an OCR provider is sent: the provider contract carries the whole file
/// base64-encoded in one JSON body (+33 %), and a page subset cannot be cut out of a PDF without
/// a PDF splitter. Convert sends a larger original to the docling-cpu OCR path instead; an
/// OcrPages provider step over it fails `ocr_input_too_large`.
pub const DEFAULT_MAX_OCR_INPUT_BYTES: u64 = 50 * 1024 * 1024;
/// docling CPU OCR engine; Task 0 Step 4 switches this to `tesseract` if easyocr is absent.
pub const DEFAULT_DOCLING_OCR_ENGINE: &str = "easyocr";

/// Page images of a URL job (webscraper `download_images`, `pipeline::page_images`): at most
/// this many are kept per page; the rest keep their original markdown link.
pub const MAX_PAGE_IMAGES: usize = 20;
/// Largest page image kept (decoded bytes); the smallest is `Limits::figure_min_bytes`.
pub const MAX_PAGE_IMAGE_BYTES: u64 = 10 * 1024 * 1024;
/// Page image types kept (webscraper's `mime_type`, parameters stripped, `image/jpg` = jpeg).
pub const PAGE_IMAGE_MIMES: [&str; 4] = ["image/png", "image/jpeg", "image/webp", "image/gif"];
/// Largest webscraper answer read. With images it can be large (webscraper downloads up to 100
/// images of ≤ 5 MB, base64'd); past this the page is scraped again without images.
pub const MAX_SCRAPE_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;

/// strata's request body limit: a bigger bundle fails with `bundle_too_large`.
pub const MAX_BUNDLE_BYTES: u64 = 16 * 1024 * 1024;
/// Step retry backoff: `min(2^(n-1) · BACKOFF_BASE_S, BACKOFF_MAX_S)` seconds.
pub const BACKOFF_BASE_S: u64 = 2;
pub const BACKOFF_MAX_S: u64 = 300;
/// A claimed step's lease; a worker heartbeats it every `HEARTBEAT_S`.
pub const LEASE_S: u64 = 600;
pub const HEARTBEAT_S: u64 = 60;

/// Margin of [`SLOT_WAIT_MAX_S`] over the longest run of a heavy-slot holder.
pub const SLOT_WAIT_MARGIN_S: u64 = 5 * 60;

/// Longest single wait of a step for a shared slot (provider permit / rpm, the Convert slot, a
/// storage upload slot). Waits do not count against the step deadline (see `step_clock`); past
/// this the run is abandoned with `slot_wait_timeout` and re-queued after a backoff WITHOUT using
/// up an attempt (rulings R35; at most [`MAX_SLOT_WAIT_REQUEUES`] times). Derived from the
/// longest holder of the heavy slot (a Convert, [`STEP_DEADLINE_CONVERT_S`]) plus a margin, so a
/// step queued behind one maximal Convert is served instead of timing out.
pub const SLOT_WAIT_MAX_S: u64 = STEP_DEADLINE_CONVERT_S + SLOT_WAIT_MARGIN_S;
const _: () = assert!(SLOT_WAIT_MAX_S > STEP_DEADLINE_CONVERT_S);
const _: () = assert!(SLOT_WAIT_MAX_S > STEP_DEADLINE_OCR_PAGES_S);

/// Slot-wait timeouts of one step that are re-queued without using an attempt; past it a
/// `slot_wait_timeout` counts like any Retryable failure (so a step cannot wait for ever).
pub const MAX_SLOT_WAIT_REQUEUES: i32 = 24;

/// Shutdown releases (`steps::release`) of one step refunded to its attempt budget. `attempt`
/// itself never decreases (it fences every step write); a step whose run keeps getting its pod
/// terminated uses up `max_attempts + MAX_RELEASE_REFUNDS` runs and then fails.
pub const MAX_RELEASE_REFUNDS: i32 = 3;

/// Working ceiling of one step run (time spent waiting for a shared slot excluded); past it the run is abandoned as Retryable `step_timeout`
/// (a hung docling/provider call must not hold a worker for ever). Per kind, see
/// [`step_deadline`].
/// Above docling's 1800 s client timeout (`infra::docling::TIMEOUT`), so a slow conversion
/// fails as a classified docling timeout, not as `step_timeout`.
pub const STEP_DEADLINE_CONVERT_S: u64 = 1900;
pub const STEP_DEADLINE_OCR_PAGES_S: u64 = 600;
pub const STEP_DEADLINE_CAPTION_S: u64 = 120;
pub const STEP_DEADLINE_TRANSCRIBE_SEGMENT_S: u64 = 600;
pub const STEP_DEADLINE_DELIVER_S: u64 = 300;
pub const STEP_DEADLINE_DEFAULT_S: u64 = 300;
/// A Discover step scrapes up to `CRAWL_MAX_PAGES` pages one after another (or fetches up to
/// `SITEMAP_MAX_FETCHES` sitemaps); a crawl records every page as it goes, so a retry resumes.
pub const STEP_DEADLINE_DISCOVER_S: u64 = 3600;

/// Crawl sources (`source.kind = "crawl"`): link depth from the start page (0) and pages.
pub const CRAWL_MAX_DEPTH: u32 = 5;
pub const CRAWL_DEFAULT_DEPTH: u32 = 2;
pub const CRAWL_MAX_PAGES: usize = 200;
pub const CRAWL_DEFAULT_PAGES: usize = 50;
/// In-scope links kept per crawled page (the frontier's growth bound).
pub const CRAWL_MAX_LINKS_PER_PAGE: usize = 1000;
/// Sitemap sources (`source.kind = "sitemap"`): page URLs taken, index nesting followed,
/// sitemap documents fetched, the largest (inflated) sitemap and the `<loc>`s read from one.
/// Parsing streams: a document stops being read at `SITEMAP_MAX_ENTRIES` `<loc>`s or once
/// `max_urls` URLs are accepted, and only accepted URLs are kept, so a Discover step holds at most
/// `max_urls` page URLs plus `SITEMAP_MAX_CHILD_SITEMAPS` queued sitemap URLs.
pub const SITEMAP_MAX_URLS: usize = 1000;
pub const SITEMAP_DEFAULT_URLS: usize = 200;
pub const SITEMAP_MAX_LEVELS: u32 = 2;
pub const SITEMAP_MAX_FETCHES: usize = 50;
/// Nested sitemaps (index entries) queued per Discover step, over all index documents: every one
/// of them can still be fetched (`SITEMAP_MAX_FETCHES` includes the root). Further index entries
/// are counted as skipped and never held.
pub const SITEMAP_MAX_CHILD_SITEMAPS: usize = SITEMAP_MAX_FETCHES - 1;
pub const SITEMAP_MAX_BYTES: u64 = 50 * 1024 * 1024;
pub const SITEMAP_MAX_ENTRIES: usize = 50_000;
/// Child job ids a parent's job view lists (`child_job_ids`).
pub const MAX_CHILD_IDS: i64 = 1000;

/// The run ceiling of a step of `kind`.
pub fn step_deadline(kind: crate::db::steps::StepKind) -> std::time::Duration {
    use crate::db::steps::StepKind;
    let s = match kind {
        StepKind::Convert => STEP_DEADLINE_CONVERT_S,
        StepKind::OcrPages => STEP_DEADLINE_OCR_PAGES_S,
        StepKind::Caption => STEP_DEADLINE_CAPTION_S,
        StepKind::TranscribeSegment => STEP_DEADLINE_TRANSCRIBE_SEGMENT_S,
        StepKind::Deliver => STEP_DEADLINE_DELIVER_S,
        StepKind::Discover => STEP_DEADLINE_DISCOVER_S,
        StepKind::Acquire | StepKind::Scan | StepKind::Detect | StepKind::Bundle => {
            STEP_DEADLINE_DEFAULT_S
        }
    };
    std::time::Duration::from_secs(s)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_upload_bytes: u64,
    pub max_pdf_pages: u32,
    pub max_audio_seconds: u64,
    pub ocr_min_chars_per_page: usize,
    pub figure_min_bytes: u64,
    pub max_figures_per_job: usize,
    pub audio_segment_s: u64,
    pub max_attempts: i32,
    pub max_ocr_input_bytes: u64,
    pub docling_ocr_engine: String,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_upload_bytes: DEFAULT_MAX_UPLOAD_BYTES,
            max_pdf_pages: DEFAULT_MAX_PDF_PAGES,
            max_audio_seconds: DEFAULT_MAX_AUDIO_SECONDS,
            ocr_min_chars_per_page: DEFAULT_OCR_MIN_CHARS_PER_PAGE,
            figure_min_bytes: DEFAULT_FIGURE_MIN_BYTES,
            max_figures_per_job: DEFAULT_MAX_FIGURES_PER_JOB,
            audio_segment_s: DEFAULT_AUDIO_SEGMENT_S,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_ocr_input_bytes: DEFAULT_MAX_OCR_INPUT_BYTES,
            docling_ocr_engine: DEFAULT_DOCLING_OCR_ENGINE.to_string(),
        }
    }
}

impl Limits {
    /// Defaults overridden by `STRATA_INGEST_*` entries of `env`. Every number must be >= 1.
    pub fn from_map(env: &HashMap<String, String>) -> Result<Self> {
        let d = Limits::default();
        let limits = Limits {
            max_upload_bytes: parse(env, "STRATA_INGEST_MAX_UPLOAD_BYTES", d.max_upload_bytes)?,
            max_pdf_pages: parse(env, "STRATA_INGEST_MAX_PDF_PAGES", d.max_pdf_pages)?,
            max_audio_seconds: parse(env, "STRATA_INGEST_MAX_AUDIO_SECONDS", d.max_audio_seconds)?,
            ocr_min_chars_per_page: parse(
                env,
                "STRATA_INGEST_OCR_MIN_CHARS_PER_PAGE",
                d.ocr_min_chars_per_page,
            )?,
            figure_min_bytes: parse(env, "STRATA_INGEST_FIGURE_MIN_BYTES", d.figure_min_bytes)?,
            max_figures_per_job: parse(
                env,
                "STRATA_INGEST_MAX_FIGURES_PER_JOB",
                d.max_figures_per_job,
            )?,
            audio_segment_s: parse(env, "STRATA_INGEST_AUDIO_SEGMENT_S", d.audio_segment_s)?,
            max_attempts: parse(env, "STRATA_INGEST_MAX_ATTEMPTS", d.max_attempts)?,
            max_ocr_input_bytes: parse(
                env,
                "STRATA_INGEST_MAX_OCR_INPUT_BYTES",
                d.max_ocr_input_bytes,
            )?,
            docling_ocr_engine: get(env, "STRATA_INGEST_DOCLING_OCR_ENGINE")
                .unwrap_or(DEFAULT_DOCLING_OCR_ENGINE)
                .to_string(),
        };
        let positive = [
            ("STRATA_INGEST_MAX_UPLOAD_BYTES", limits.max_upload_bytes),
            (
                "STRATA_INGEST_MAX_PDF_PAGES",
                u64::from(limits.max_pdf_pages),
            ),
            ("STRATA_INGEST_MAX_AUDIO_SECONDS", limits.max_audio_seconds),
            (
                "STRATA_INGEST_OCR_MIN_CHARS_PER_PAGE",
                limits.ocr_min_chars_per_page as u64,
            ),
            ("STRATA_INGEST_FIGURE_MIN_BYTES", limits.figure_min_bytes),
            (
                "STRATA_INGEST_MAX_FIGURES_PER_JOB",
                limits.max_figures_per_job as u64,
            ),
            ("STRATA_INGEST_AUDIO_SEGMENT_S", limits.audio_segment_s),
            (
                "STRATA_INGEST_MAX_OCR_INPUT_BYTES",
                limits.max_ocr_input_bytes,
            ),
        ];
        for (key, v) in positive {
            if v == 0 {
                return Err(IngestError::Config(format!("{key} must be >= 1")));
            }
        }
        if limits.max_upload_bytes > STORAGE_SCAN_LIMIT_BYTES {
            return Err(IngestError::Config(format!(
                "STRATA_INGEST_MAX_UPLOAD_BYTES must be <= {STORAGE_SCAN_LIMIT_BYTES} \
                 (storage_service's virus-scan limit)"
            )));
        }
        if limits.max_attempts < 1 {
            return Err(IngestError::Config(
                "STRATA_INGEST_MAX_ATTEMPTS must be >= 1".to_string(),
            ));
        }
        Ok(limits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::steps::StepKind;

    /// A step queued behind one maximal heavy-slot holder must get the slot before its wait
    /// bound: the bound exceeds every holder's deadline (a Convert runs up to 1900 s).
    #[test]
    fn slot_wait_bound_exceeds_every_heavy_slot_holder() {
        for kind in [StepKind::Convert, StepKind::OcrPages] {
            assert!(
                SLOT_WAIT_MAX_S > step_deadline(kind).as_secs(),
                "{kind:?}: {SLOT_WAIT_MAX_S} s"
            );
        }
    }
}
