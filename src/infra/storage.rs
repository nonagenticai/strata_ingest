//! storage_service client (default `http://127.0.0.1:8391`, `STRATA_INGEST_STORAGE_URL`): the only
//! place job blobs live.
//!
//! Contract (storage_service origin/main `22062db8`, `src/routes/files.py`):
//! - `POST /api/files/upload`, multipart `bucket`, `object_key`, `content_type`, `file` →
//!   `{bucket, object_key, size, etag, content_type, virus_scanned, scan_result}`. ClamAV runs
//!   inside the request on the whole body: infected → 400 `Virus detected: <name>`, scanner down or
//!   over clamd's 100 MiB `StreamMaxLength` → 503; either way NOTHING is stored. Last write wins.
//!   `virus_scanned=false` (scanning disabled) is a store without a verdict: never "clean".
//! - `GET /api/files/{bucket}/{key}` whole body (no Range), 404 when missing;
//!   `…/exists` → `{exists}`; `…/metadata` → `{size, …}` (404 when missing);
//!   `DELETE` → 204, 404 when missing (treated as success here).
//! - Errors `{"error": {"code", "message", …}}`. Optional auth: `X-Tenant-Id: Bearer <licence
//!   token>`; without it an in-cluster caller is the platform tenant. storage_service does no
//!   tenant scoping at all, so every key carries our own `{tenant}/{job}/` prefix.
//! - Keys: non-empty, ≤ 1024 bytes, no leading `/`, no control chars, no `.`/`..` segment, and
//!   not ending in `/exists` or `/metadata` (those suffixes route to the sub-endpoints).
//!
//! Keys: `{tenant}/{job}/original`, `{tenant}/{job}/figures/{asset_id}`,
//! `{tenant}/{job}/audio/{n:04}.mp3`. Textual intermediates live in Postgres (`ingest_artifacts`).
//!
//! Like the other infra clients each call makes one attempt: retrying a Retryable step error is
//! the step queue's job. The token only travels in a sensitive header and never in a message.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt};
use reqwest::StatusCode;
use reqwest::header::HeaderValue;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use super::{snippet, transport_message};
use crate::config::{Secret, redact_url};
use crate::error::{IngestError, StepError};
use crate::providers::ErrorClass;

const SERVICE: &str = "storage";
/// The auth header storage_service reads (not `Authorization`).
const AUTH_HEADER: &str = "x-tenant-id";
/// Longest object key storage_service accepts (UTF-8 bytes).
pub const MAX_KEY_BYTES: usize = 1024;
/// Uploads in flight per pod (all clones of one client share them; rulings R38). storage-api
/// holds a whole upload body in RAM under a 256 Mi limit, so a burst of figure/segment uploads
/// from every worker plus API uploads must not reach it at once.
pub const UPLOAD_SLOTS: usize = 2;

/// HTTP timeouts. An upload waits for ClamAV inside the request (up to ~93 s against a hung
/// clamd, contract §3) with no bytes flowing, so `idle` must exceed that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageTimeouts {
    /// TCP connect.
    pub connect: Duration,
    /// One whole exchange, body included: 100 MiB at a ~0.2 MB/s floor plus the scan.
    pub request: Duration,
    /// Longest gap between two reads of a response; a stalled connection fails after this.
    pub idle: Duration,
}

impl Default for StorageTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            request: Duration::from_secs(600),
            idle: Duration::from_secs(120),
        }
    }
}

/// Why a storage call failed.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The upload was refused by the virus scan; nothing was stored. Carries the signature.
    #[error("infected: {0}")]
    Infected(String),
    #[error("object not found")]
    NotFound,
    /// 503 (scanner down / over the scan limit), another 5xx, a transport error or a timeout.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// Any other 4xx (bad key/bucket, auth, too large).
    #[error("storage refused the request ({status}): {message}")]
    Refused { status: u16, message: String },
    /// Refused locally: the key breaks storage_service's key rules.
    #[error("invalid object key {0:?}")]
    InvalidKey(String),
    /// An object bigger than the caller's read bound.
    #[error("object exceeds {0} bytes")]
    TooLarge(u64),
    /// A local file error (reading an upload, writing a download).
    #[error("local i/o: {0}")]
    Io(#[from] std::io::Error),
}

impl StorageError {
    /// The step error for a failed call on `key`: unavailable/transport → Retryable
    /// `storage_unavailable`; infected → `infected`; anything else terminal.
    pub fn step_error(&self, key: &str) -> StepError {
        let (class, code) = match self {
            StorageError::Infected(_) => (ErrorClass::InvalidRequest, "infected"),
            StorageError::NotFound => (ErrorClass::InvalidRequest, "object_missing"),
            StorageError::Unavailable(_) => (ErrorClass::Retryable, "storage_unavailable"),
            // storage_service answers 401 on ANY licence-introspection failure (a licences
            // restart, a token rotation) and caches it 60 s: a blip, retried (R41).
            StorageError::Refused {
                status: 401 | 403, ..
            } => (ErrorClass::Retryable, "storage_auth"),
            StorageError::Refused {
                status: 408 | 429, ..
            } => (ErrorClass::Retryable, "storage_unavailable"),
            StorageError::Refused { status: 413, .. } | StorageError::TooLarge(_) => {
                (ErrorClass::InvalidRequest, "too_large")
            }
            StorageError::Refused { .. } => (ErrorClass::InvalidRequest, "storage_refused"),
            StorageError::InvalidKey(_) => (ErrorClass::InvalidRequest, "invalid_key"),
            StorageError::Io(_) => (ErrorClass::Retryable, "internal"),
        };
        StepError {
            class,
            code: code.to_string(),
            message: format!("{key}: {self}"),
            provider: Some(SERVICE.to_string()),
            retry_after_s: None,
        }
    }
}

impl From<StorageError> for IngestError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound => IngestError::NotFound,
            StorageError::Io(e) => IngestError::Io(e),
            StorageError::InvalidKey(k) => IngestError::InvalidInput(format!("object key {k:?}")),
            StorageError::TooLarge(_) | StorageError::Refused { status: 413, .. } => {
                IngestError::PayloadTooLarge
            }
            e => IngestError::Upstream {
                service: SERVICE,
                message: e.to_string(),
            },
        }
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// What storage_service answered to an upload.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Uploaded {
    pub size: u64,
    #[serde(default)]
    pub virus_scanned: bool,
    #[serde(default)]
    pub scan_result: Option<String>,
}

/// The scan verdict of a stored object (an infected one is never stored: [`StorageError::Infected`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanVerdict {
    /// Scanned by ClamAV and clean.
    Clean,
    /// Stored without a verdict (`virus_scanned=false`): must never be treated as clean.
    Unverified,
}

impl ScanVerdict {
    /// The value recorded in a job's `meta.scan` / an upload source's `scan`.
    pub fn as_str(self) -> &'static str {
        match self {
            ScanVerdict::Clean => "clean",
            ScanVerdict::Unverified => "unverified",
        }
    }
}

impl Uploaded {
    pub fn verdict(&self) -> ScanVerdict {
        if self.virus_scanned && self.scan_result.as_deref() == Some("clean") {
            ScanVerdict::Clean
        } else {
            ScanVerdict::Unverified
        }
    }
}

#[derive(Deserialize)]
struct Metadata {
    size: u64,
}

#[derive(Deserialize)]
struct Exists {
    exists: bool,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    #[serde(default)]
    message: String,
}

/// storage_service on one bucket. Cheap to clone.
#[derive(Clone)]
pub struct StorageClient {
    http: reqwest::Client,
    base: url::Url,
    bucket: String,
    auth: Option<HeaderValue>,
    /// Object bytes received by this client and its clones (observability; tests bound reads).
    read: Arc<AtomicU64>,
    /// [`UPLOAD_SLOTS`] permits, shared by this client and its clones.
    uploads: Arc<tokio::sync::Semaphore>,
}

impl fmt::Debug for StorageClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageClient")
            .field("base", &redact_url(self.base.as_str()))
            .field("bucket", &self.bucket)
            .field("auth", &self.auth.as_ref().map(|_| "***"))
            .finish()
    }
}

impl StorageClient {
    /// A client for `bucket` at `base_url` (`http(s)://host:port`), with the optional licence
    /// token sent as `X-Tenant-Id: Bearer <token>`.
    pub fn new(base_url: &str, bucket: &str, token: Option<&Secret>) -> crate::Result<Self> {
        Self::with_timeouts(base_url, bucket, token, StorageTimeouts::default())
    }

    pub fn with_timeouts(
        base_url: &str,
        bucket: &str,
        token: Option<&Secret>,
        t: StorageTimeouts,
    ) -> crate::Result<Self> {
        let cfg = |m: String| IngestError::Config(m);
        let base = url::Url::parse(base_url.trim().trim_end_matches('/'))
            .map_err(|e| cfg(format!("storage url {}: {e}", redact_url(base_url))))?;
        if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
            return Err(cfg(format!(
                "storage url {} must be http(s)://host[:port]",
                redact_url(base_url)
            )));
        }
        if bucket.trim().is_empty() {
            return Err(cfg("storage bucket is empty".into()));
        }
        let auth = token
            .map(|t| {
                let mut v =
                    HeaderValue::from_str(&format!("Bearer {}", t.expose())).map_err(|_| {
                        cfg("STRATA_INGEST_STORAGE_TOKEN is not a valid header value".into())
                    })?;
                v.set_sensitive(true);
                Ok::<_, IngestError>(v)
            })
            .transpose()?;
        let http = reqwest::Client::builder()
            .connect_timeout(t.connect)
            .timeout(t.request)
            .read_timeout(t.idle)
            .build()
            .map_err(|e| cfg(format!("storage http client: {e}")))?;
        Ok(Self {
            http,
            base,
            bucket: bucket.to_string(),
            auth,
            read: Arc::new(AtomicU64::new(0)),
            uploads: Arc::new(tokio::sync::Semaphore::new(UPLOAD_SLOTS)),
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Object bytes received so far by this client and its clones.
    pub fn bytes_read(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    fn with_auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(v) => rb.header(AUTH_HEADER, v.clone()),
            None => rb,
        }
    }

    /// `{base}/api/files/{bucket}/{key…}{suffix}`, every key segment percent-encoded.
    fn object_url(&self, key: &str, suffix: Option<&str>) -> StorageResult<url::Url> {
        check_key(key)?;
        let mut u = self.base.clone();
        {
            let mut seg = u
                .path_segments_mut()
                .map_err(|_| StorageError::InvalidKey(key.to_string()))?;
            seg.pop_if_empty().extend(["api", "files", &self.bucket]);
            seg.extend(key.split('/'));
            if let Some(s) = suffix {
                seg.push(s);
            }
        }
        Ok(u)
    }

    async fn send(&self, rb: reqwest::RequestBuilder) -> StorageResult<reqwest::Response> {
        let r = self
            .with_auth(rb)
            .send()
            .await
            .map_err(|e| StorageError::Unavailable(transport_message(e)))?;
        if r.status().is_success() {
            return Ok(r);
        }
        Err(error_of(r).await)
    }

    /// Uploads `len` bytes streamed from `body` (never buffered here) to `key`. The object exists
    /// only once storage_service has the whole body and a verdict: an aborted body stores nothing.
    /// At most [`UPLOAD_SLOTS`] uploads of this client and its clones run at once; the rest wait.
    pub async fn upload_stream(
        &self,
        key: &str,
        body: reqwest::Body,
        len: u64,
        content_type: &str,
    ) -> StorageResult<Uploaded> {
        check_key(key)?;
        let file_name = key.rsplit('/').next().unwrap_or("object").to_string();
        let part = Part::stream_with_length(body, len)
            .file_name(file_name)
            .mime_str(content_type)
            .map_err(|e| StorageError::Refused {
                status: 400,
                message: format!("content type {content_type:?}: {e}"),
            })?;
        let form = Form::new()
            .text("bucket", self.bucket.clone())
            .text("object_key", key.to_string())
            .text("content_type", content_type.to_string())
            .part("file", part);
        let url = self.base.join("api/files/upload").expect("static path");
        // At most UPLOAD_SLOTS bodies in flight per pod; the wait does not count against a
        // step's deadline.
        let _slot = crate::step_clock::waiting(self.uploads.acquire())
            .await
            .expect("the upload semaphore is never closed");
        let r = self.send(self.http.post(url).multipart(form)).await?;
        let up: Uploaded = r.json().await.map_err(|e| {
            StorageError::Unavailable(format!("upload answer: {}", transport_message(e)))
        })?;
        if up.size != len {
            return Err(StorageError::Unavailable(format!(
                "storage recorded {} bytes of {len}",
                up.size
            )));
        }
        Ok(up)
    }

    /// Uploads an in-memory body (artefacts, scraped markdown).
    pub async fn upload_bytes(
        &self,
        key: &str,
        body: impl Into<Bytes>,
        content_type: &str,
    ) -> StorageResult<Uploaded> {
        let body: Bytes = body.into();
        let len = body.len() as u64;
        self.upload_stream(key, reqwest::Body::from(body), len, content_type)
            .await
    }

    /// Uploads the file at `path` (its current length), streamed from disk.
    pub async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        content_type: &str,
    ) -> StorageResult<Uploaded> {
        let file = tokio::fs::File::open(path).await?;
        let len = file.metadata().await?.len();
        self.upload_stream(key, reqwest::Body::from(file), len, content_type)
            .await
    }

    /// The object as a stream of chunks as they arrive.
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> StorageResult<impl Stream<Item = StorageResult<Bytes>> + Send + Unpin + 'static> {
        let url = self.object_url(key, None)?;
        let r = self.send(self.http.get(url)).await?;
        let read = self.read.clone();
        Ok(r.bytes_stream()
            .map_err(|e| StorageError::Unavailable(transport_message(e)))
            .inspect_ok(move |b| {
                read.fetch_add(b.len() as u64, Ordering::Relaxed);
            })
            .boxed())
    }

    /// The whole object in memory; more than `max` bytes → [`StorageError::TooLarge`].
    pub async fn get_bytes(&self, key: &str, max: u64) -> StorageResult<Bytes> {
        let mut s = self.get_stream(key).await?;
        let mut buf = BytesMut::new();
        while let Some(chunk) = s.next().await {
            let chunk = chunk?;
            if (buf.len() + chunk.len()) as u64 > max {
                return Err(StorageError::TooLarge(max));
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }

    /// Streams the object into a new file at `path`; more than `max` bytes →
    /// [`StorageError::TooLarge`] (the partial file is left for the caller's scratch dir).
    /// Returns the bytes written.
    pub async fn get_to_file(&self, key: &str, path: &Path, max: u64) -> StorageResult<u64> {
        let mut s = self.get_stream(key).await?;
        let mut file = tokio::fs::File::create(path).await?;
        let mut n: u64 = 0;
        while let Some(chunk) = s.next().await {
            let chunk = chunk?;
            n += chunk.len() as u64;
            if n > max {
                return Err(StorageError::TooLarge(max));
            }
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(n)
    }

    /// The object's size (`GET …/metadata`); missing → [`StorageError::NotFound`].
    pub async fn size(&self, key: &str) -> StorageResult<u64> {
        let url = self.object_url(key, Some("metadata"))?;
        let r = self.send(self.http.get(url)).await?;
        let m: Metadata = r.json().await.map_err(|e| {
            StorageError::Unavailable(format!("metadata: {}", transport_message(e)))
        })?;
        Ok(m.size)
    }

    /// Whether the object exists (`GET …/exists`).
    pub async fn exists(&self, key: &str) -> StorageResult<bool> {
        let url = self.object_url(key, Some("exists"))?;
        let r = self.send(self.http.get(url)).await?;
        let e: Exists = r
            .json()
            .await
            .map_err(|e| StorageError::Unavailable(format!("exists: {}", transport_message(e))))?;
        Ok(e.exists)
    }

    /// Deletes the object; a missing one is success (storage_service answers 404).
    pub async fn delete(&self, key: &str) -> StorageResult<()> {
        let url = self.object_url(key, None)?;
        match self.send(self.http.delete(url)).await {
            Ok(_) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// `GET /health/ready` (MinIO reachable; it deliberately ignores clamd) answers 200 within
    /// `timeout`.
    pub async fn health(&self, timeout: Duration) -> bool {
        let url = self.base.join("health/ready").expect("static path");
        matches!(
            self.with_auth(self.http.get(url).timeout(timeout)).send().await,
            Ok(r) if r.status().is_success()
        )
    }
}

/// Maps a non-2xx answer. 400 `Virus detected: <name>` is [`StorageError::Infected`].
async fn error_of(r: reqwest::Response) -> StorageError {
    let status = r.status();
    let body = r.text().await.unwrap_or_default();
    let message = serde_json::from_str::<ErrorBody>(&body)
        .map(|b| b.error.message)
        .unwrap_or_else(|_| snippet(&body));
    if status == StatusCode::BAD_REQUEST
        && let Some(sig) = message.strip_prefix("Virus detected:")
    {
        let sig = sig.trim();
        return StorageError::Infected(if sig.is_empty() { "unknown" } else { sig }.to_string());
    }
    if status == StatusCode::NOT_FOUND {
        return StorageError::NotFound;
    }
    if status.is_server_error() {
        return StorageError::Unavailable(format!("{status}: {}", snippet(&message)));
    }
    StorageError::Refused {
        status: status.as_u16(),
        message: snippet(&message),
    }
}

/// storage_service's key rules (`object_keys.py`), plus the route-suffix trap.
pub fn check_key(key: &str) -> StorageResult<()> {
    let bad = key.is_empty()
        || key.len() > MAX_KEY_BYTES
        || key.starts_with('/')
        || key.chars().any(|c| c.is_ascii_control())
        || key.split('/').any(|s| s == "." || s == "..")
        || key.ends_with("/exists")
        || key.ends_with("/metadata");
    if bad {
        Err(StorageError::InvalidKey(key.chars().take(80).collect()))
    } else {
        Ok(())
    }
}
