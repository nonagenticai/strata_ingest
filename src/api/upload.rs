//! Multipart uploads for `POST /api/ingest/jobs`: field order, sniffing, spooling each file and
//! storing it in storage_service, and cancellation-safe cleanup.
//!
//! Each file part is first spooled to an anonymous scratch file (unlinked at creation), bounded
//! by `max_upload_bytes`: an over-limit file is a 413 before storage_service is called (rulings
//! R24), and the upload then goes out with its known length, streamed from disk. storage_service
//! scans the body inside the upload request and stores nothing unless the request completes and
//! the body is clean (rulings R25): an infected file becomes a `rejected` job (`infected`) and is
//! listed in the 202 like any other job; a 503 (scanner down / over its scan limit) fails the
//! request as upstream-unavailable.
//!
//! The storage upload runs in its own spawned task, so its fate does not depend on the handler
//! future being polled to completion. Each original that is (or may be) stored but whose job does
//! not exist yet is tracked in [`Pending`]; its cleanup (wait for the upload task, then delete the
//! original unless its job exists) runs in a spawned task, both on an error and from `Drop`, so it
//! survives the handler being cancelled (client disconnect, shutdown, pod kill). A handler
//! dropped while a file is still being spooled has sent storage nothing for it.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::Multipart;
use axum::extract::multipart::{Field, MultipartError};
use axum::http::StatusCode;
use bytes::{Bytes, BytesMut};
use serde_json::{Value, json};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::handlers::{
    bad, initial_meta, parse_options, parse_source_id, parse_source_run, validate_store,
};
use super::{ApiOpts, REFUSED_DRAIN_TIMEOUT};
use crate::db::jobs::{self, NewJob};
use crate::doc_fields::{self, DocFields, ProvenanceIn};
use crate::error::{IngestError, Result, StepError};
use crate::infra::storage::{StorageError, StorageResult, Uploaded};
use crate::pipeline::{Ctx, detect, original_key};
use crate::providers::ErrorClass;

/// Bytes of each upload sniffed before anything is stored (rulings R4).
pub const SNIFF_HEAD: usize = 64 * 1024;
/// Default for [`ApiOpts::upload_idle_timeout`].
pub const UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Default for [`ApiOpts::max_concurrent_uploads`] (rulings R38).
pub const MAX_CONCURRENT_UPLOADS: usize = 8;
/// `Retry-After` of a 503 `busy` upload refusal.
pub const UPLOAD_BUSY_RETRY_AFTER_S: u64 = 30;

fn multipart_err(e: MultipartError) -> IngestError {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        IngestError::PayloadTooLarge
    } else {
        bad(format!("multipart body: {}", e.body_text()))
    }
}

/// One original in storage_service (or being uploaded there) whose job does not exist yet.
struct PendingItem {
    job: Uuid,
    key: String,
    /// The upload task, until the handler has seen it finish.
    upload: Option<JoinHandle<StorageResult<Uploaded>>>,
}

/// Originals stored by this request and not yet committed to a job. Whatever is still here when
/// the request fails, or when this value is dropped, is cleaned up by a spawned task.
struct Pending {
    ctx: Arc<Ctx>,
    tenant: Uuid,
    items: Vec<PendingItem>,
}

impl Pending {
    /// Starts cleanup of everything still pending (nothing pending → `None`).
    fn spawn_cleanup(&mut self) -> Option<JoinHandle<()>> {
        if self.items.is_empty() {
            return None;
        }
        let items = std::mem::take(&mut self.items);
        let (ctx, tenant) = (self.ctx.clone(), self.tenant);
        let rt = tokio::runtime::Handle::try_current().ok()?;
        Some(rt.spawn(cleanup(ctx, tenant, items)))
    }

    /// Cleans up now and waits for it (the cleanup itself still runs if this future is dropped).
    async fn cleanup_now(&mut self) {
        if let Some(h) = self.spawn_cleanup() {
            let _ = h.await;
        }
    }

    /// The job of `job` exists: its original is no longer ours to remove.
    fn commit(&mut self, job: Uuid) {
        self.items.retain(|i| i.job != job);
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if self.spawn_cleanup().is_some() {
            tracing::info!("upload request ended early; removing its stored originals");
        }
    }
}

/// Waits for each upload task (so an upload that is going to land has landed), then deletes the
/// original unless its job exists (deleting a key that was never stored succeeds). A DB error
/// keeps the blob: an original without a job is waste, a job without its original is a failed
/// job.
async fn cleanup(ctx: Arc<Ctx>, tenant: Uuid, items: Vec<PendingItem>) {
    for item in items {
        if let Some(h) = item.upload {
            let _ = h.await;
        }
        match jobs::job(&ctx.pool, tenant, item.job).await {
            Err(IngestError::NotFound) => match ctx.storage.delete(&item.key).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(key = %item.key, error = %e, "removing an orphaned upload failed");
                }
            },
            Ok(_) => {}
            Err(e) => tracing::warn!(
                key = %item.key, error = %e,
                "cannot tell whether the upload's job exists; keeping the original"
            ),
        }
    }
}

/// One file part read, its job not yet created.
struct Stored {
    id: Uuid,
    source: Value,
    /// storage_service refused it as infected (nothing stored): the job is created `rejected`.
    infected: Option<String>,
}

/// A created job and its initial status (`queued`, or `rejected` for an infected upload).
pub(super) type CreatedJob = (Uuid, &'static str);

/// Reads the whole multipart request, stores every file, then creates one job per file. Any
/// failure (including this future being dropped) leaves no original without a job.
pub(super) async fn upload_jobs(
    ctx: &Arc<Ctx>,
    tenant: Uuid,
    mut mp: Multipart,
    opts: &ApiOpts,
) -> Result<Vec<CreatedJob>> {
    let mut pending = Pending {
        ctx: ctx.clone(),
        tenant,
        items: Vec::new(),
    };
    let mut parts = Parts::default();
    let read = read_parts(ctx, &mut mp, &mut pending, &mut parts, opts).await;
    if let Err(e) = &read
        && !matches!(e, IngestError::RequestTimeout(_))
    {
        // Read (and discard) what the client is still sending, so it sees our error instead of
        // a connection reset. Bounded by the body limit layer and by REFUSED_DRAIN_TIMEOUT.
        let drain = async { while let Ok(Some(_)) = mp.next_field().await {} };
        let _ = tokio::time::timeout(REFUSED_DRAIN_TIMEOUT, drain).await;
    }
    let created = match read {
        Ok(()) => create_all(ctx, tenant, parts, &mut pending).await,
        Err(e) => Err(e),
    };
    if created.is_err() {
        pending.cleanup_now().await;
    }
    created
}

#[derive(Default)]
struct Parts {
    store_id: Option<Uuid>,
    source_id: Option<String>,
    source_run: Option<String>,
    options: Option<Value>,
    /// `topic_keys` / `supersedes` / `provenance` (JSON text fields), validated as read; every
    /// file of the request gets them.
    fields: DocFields,
    stored: Vec<Stored>,
}

async fn read_parts(
    ctx: &Arc<Ctx>,
    mp: &mut Multipart,
    pending: &mut Pending,
    parts: &mut Parts,
    opts: &ApiOpts,
) -> Result<()> {
    let mut validated = false;
    while let Some(field) = mp.next_field().await.map_err(multipart_err)? {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "store_id" | "source_id" | "source_run" | "options" | "topic_keys" | "supersedes"
            | "provenance"
                if validated =>
            {
                return Err(bad(format!(
                    "`{name}` must come before the first `file` part"
                )));
            }
            "store_id" => {
                if parts.store_id.is_some() {
                    return Err(bad("duplicate `store_id` field"));
                }
                let text = field.text().await.map_err(multipart_err)?;
                parts.store_id = Some(
                    Uuid::parse_str(text.trim()).map_err(|_| bad("`store_id` is not a uuid"))?,
                );
            }
            "source_id" => {
                if parts.source_id.is_some() {
                    return Err(bad("duplicate `source_id` field"));
                }
                let text = field.text().await.map_err(multipart_err)?;
                parts.source_id = Some(parse_source_id(Some(text))?.unwrap_or_default());
            }
            "source_run" => {
                if parts.source_run.is_some() {
                    return Err(bad("duplicate `source_run` field"));
                }
                let text = field.text().await.map_err(multipart_err)?;
                // Shape only here; "needs a `source_id`" once all fields before the file are in.
                parts.source_run =
                    Some(parse_source_run(Some(text), Some(""))?.unwrap_or_default());
            }
            "options" => {
                if parts.options.is_some() {
                    return Err(bad("duplicate `options` field"));
                }
                let text = field.text().await.map_err(multipart_err)?;
                let v: Value = serde_json::from_str(&text)
                    .map_err(|e| bad(format!("`options` is not JSON: {e}")))?;
                // Validated now so a bad option fails before any file is read.
                parse_options(Some(v.clone()))?;
                parts.options = Some(v);
            }
            "topic_keys" | "supersedes" => {
                let seen = if name == "topic_keys" {
                    parts.fields.topic_keys.is_some()
                } else {
                    parts.fields.supersedes.is_some()
                };
                if seen {
                    return Err(bad(format!("duplicate `{name}` field")));
                }
                let text = field.text().await.map_err(multipart_err)?;
                let keys = doc_fields::topic_keys(
                    &name,
                    doc_fields::from_json_text::<Vec<String>>(&name, &text)?,
                )?;
                if name == "topic_keys" {
                    parts.fields.topic_keys = Some(keys);
                } else {
                    parts.fields.supersedes = Some(keys);
                }
            }
            "provenance" => {
                if parts.fields.provenance.is_some() {
                    return Err(bad("duplicate `provenance` field"));
                }
                let text = field.text().await.map_err(multipart_err)?;
                let p: ProvenanceIn = doc_fields::from_json_text("provenance", &text)?;
                parts.fields.provenance = Some(p.validate()?);
            }
            "file" => {
                let store = parts
                    .store_id
                    .ok_or_else(|| bad("`store_id` must come before the first `file` part"))?;
                if !validated {
                    if parts.source_run.is_some() && parts.source_id.is_none() {
                        return Err(bad(
                            "`source_run` is only allowed together with `source_id`",
                        ));
                    }
                    validate_store(ctx, pending.tenant, store).await?;
                    validated = true;
                }
                let id = Uuid::now_v7();
                let filename = field.file_name().map(str::to_string);
                let up = store_upload(ctx, pending, id, field, filename.as_deref(), opts).await?;
                let (scan, infected) = match up.verdict {
                    Ok(v) => (v.as_str(), None),
                    Err(sig) => ("infected", Some(sig)),
                };
                let mut source = json!({
                    "kind": "upload", "filename": filename, "mime": up.mime, "bytes": up.bytes,
                    "scan": scan,
                });
                if let Some(sig) = &infected {
                    source["signature"] = json!(sig);
                }
                parts.stored.push(Stored {
                    id,
                    source,
                    infected,
                });
            }
            other => return Err(bad(format!("unexpected multipart field `{other}`"))),
        }
    }
    if parts.stored.is_empty() {
        return Err(bad("no `file` part"));
    }
    Ok(())
}

/// Why the handler stopped feeding an upload.
#[derive(Debug)]
enum Stop {
    TooLarge,
    Idle(Duration),
    Body(String),
}

impl From<Stop> for IngestError {
    fn from(s: Stop) -> Self {
        match s {
            Stop::TooLarge => IngestError::PayloadTooLarge,
            Stop::Idle(d) => IngestError::RequestTimeout(format!(
                "no upload bytes received for {} s",
                d.as_secs_f64()
            )),
            Stop::Body(m) => bad(format!("upload interrupted: {m}")),
        }
    }
}

/// The next chunk of `field`, bounded by the idle timeout.
async fn next_chunk(
    field: &mut Field<'_>,
    idle: Duration,
) -> std::result::Result<Option<Bytes>, Stop> {
    match tokio::time::timeout(idle, field.chunk()).await {
        Err(_) => Err(Stop::Idle(idle)),
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) if e.status() == StatusCode::PAYLOAD_TOO_LARGE => Err(Stop::TooLarge),
        Ok(Err(e)) => Err(Stop::Body(e.body_text())),
    }
}

/// One file part as stored: its size, sniffed mime and storage_service's verdict (`Err` = the
/// signature of an infected file, which was not stored).
struct Upload {
    bytes: u64,
    mime: String,
    verdict: std::result::Result<crate::infra::storage::ScanVerdict, String>,
}

/// Sniffs the head (nothing is written when it is refused), spools the part to a scratch file
/// (over `max_upload_bytes` → 413, storage never called), then uploads it from there through a
/// spawned task registered in `pending`.
async fn store_upload(
    ctx: &Arc<Ctx>,
    pending: &mut Pending,
    job: Uuid,
    mut field: Field<'_>,
    filename: Option<&str>,
    opts: &ApiOpts,
) -> Result<Upload> {
    let max = ctx.cfg.limits.max_upload_bytes;
    let idle = opts.upload_idle_timeout;
    let mut head = BytesMut::new();
    let mut eof = false;
    while head.len() < SNIFF_HEAD {
        match next_chunk(&mut field, idle).await? {
            Some(c) => head.extend_from_slice(&c),
            None => {
                eof = true;
                break;
            }
        }
    }
    if head.len() as u64 > max {
        return Err(IngestError::PayloadTooLarge);
    }
    let mime = match detect::sniff_head(&head[..head.len().min(SNIFF_HEAD)], filename)? {
        Some(d) => d.mime().to_string(),
        // A zip container: the worker's Detect step decides what it is.
        None => "application/zip".to_string(),
    };

    // Spool: an anonymous file (unlinked at creation), removed however this request ends.
    let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
    let mut total = head.len() as u64;
    file.write_all(&head).await?;
    drop(head);
    if !eof {
        while let Some(c) = next_chunk(&mut field, idle).await? {
            total += c.len() as u64;
            if total > max {
                return Err(Stop::TooLarge.into());
            }
            file.write_all(&c).await?;
        }
    }
    file.flush().await?;
    file.seek(std::io::SeekFrom::Start(0)).await?;

    let key = original_key(pending.tenant, job);
    let storage = ctx.storage.clone();
    let (task_key, task_mime) = (key.clone(), mime.clone());
    let upload = tokio::spawn(async move {
        storage
            .upload_stream(&task_key, reqwest::Body::from(file), total, &task_mime)
            .await
    });
    pending.items.push(PendingItem {
        job,
        key,
        upload: Some(upload),
    });
    let item = pending.items.last_mut().expect("pushed above");
    let joined = match item.upload.as_mut() {
        Some(h) => h.await,
        None => unreachable!("the upload handle is only taken here"),
    };
    item.upload = None;
    let verdict = match joined.map_err(|e| IngestError::Upstream {
        service: "storage",
        message: format!("upload task: {e}"),
    })? {
        Ok(up) => Ok(up.verdict()),
        Err(StorageError::Infected(sig)) => {
            // Nothing was stored: no original to clean up for this job.
            pending.commit(job);
            Err(sig)
        }
        Err(e) => return Err(e.into()),
    };
    Ok(Upload {
        bytes: total,
        mime,
        verdict,
    })
}

/// Creates one job per stored file (an infected one `rejected`, rulings R25); each committed
/// original leaves `pending`.
async fn create_all(
    ctx: &Ctx,
    tenant: Uuid,
    parts: Parts,
    pending: &mut Pending,
) -> Result<Vec<CreatedJob>> {
    let store_id = parts
        .store_id
        .ok_or_else(|| bad("missing `store_id` field"))?;
    let options = parse_options(parts.options)?;
    let mut meta = initial_meta(parts.source_id.as_deref(), parts.source_run.as_deref());
    parts.fields.add_to(&mut meta);
    // after the uploads: a DB failure here keeps the originals (`cleanup`), like a failed insert
    super::store_providers::record_lineage(ctx, tenant, store_id).await?;
    let mut ids = Vec::with_capacity(parts.stored.len());
    for s in parts.stored {
        let new = NewJob {
            tenant,
            store_id,
            source: s.source,
            options: options.clone(),
        };
        match s.infected {
            None => {
                ids.push((
                    jobs::create_with_meta(&ctx.pool, s.id, new, &meta).await?,
                    "queued",
                ));
            }
            Some(signature) => {
                let reason = StepError {
                    class: ErrorClass::InvalidRequest,
                    code: "infected".into(),
                    message: signature,
                    provider: Some("storage".into()),
                    retry_after_s: None,
                };
                tracing::warn!(job = %s.id, reason = %reason, "upload is infected; job rejected");
                ids.push((
                    jobs::create_ended(&ctx.pool, s.id, new, &meta, "rejected", &reason).await?,
                    "rejected",
                ));
            }
        }
        pending.commit(s.id);
    }
    Ok(ids)
}
