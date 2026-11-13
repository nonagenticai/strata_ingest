//! strata (strata_rag) client. Routes (strata_rag `src/api/mod.rs`): `GET /api/stores/{id}` →
//! `StoreView` with `kind` = `"leaf"` | `"router"` and `parent_id` (404 unknown); `POST /api/stores/{id}/documents`
//! with JSON `{text, source, title?, source_id?, metadata?}` (strata_rag 6c6cf8d: `source_id`
//! 1..=200 chars, `metadata` an object of at most 64 KiB) → 201 `{status: "created", id, …}` or 200
//! `{status: "existing", id, …}` for a duplicate (same content hash in that leaf). Errors are
//! `{"error": {"code", "message"}}`: 409 `leaf_full` (also `conflict`, `embed_model_mismatch`),
//! 422 `invalid_target` (router) / `invalid_input`, 413 `payload_too_large`, 404 `not_found`.
//! Bearer auth; request bodies are capped at 16 MiB. Every call carries `X-Tenant-Id: <tenant uuid>`
//! (a bare UUID: the job's / caller's tenant) for strata's tenant scoping (spec §8.6).

use std::fmt;
use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use super::{http_client, snippet, step_err, transport_message};
use crate::config::Secret;
use crate::error::{IngestError, StepError};
use crate::providers::ErrorClass;

/// strata's `MAX_BODY_BYTES`.
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Ingest embeds every chunk before answering.
pub const TIMEOUT: Duration = Duration::from_secs(300);
/// A store lookup is one small GET.
pub const STORE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const SERVICE: &str = "strata";
/// strata's tenant-scoping header; the value is a bare UUID.
pub const TENANT_HEADER: &str = "x-tenant-id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivered {
    pub document_id: Uuid,
    /// strata already held identical content in this store (`status: "existing"`).
    pub duplicate: bool,
}

/// A document's provenance as strata_rag stores it: the caller's `source_id`, `source_run` and
/// metadata.
#[derive(Debug, Clone, Copy, Default)]
pub struct Origin<'a> {
    pub source_id: Option<&'a str>,
    /// The sync run (`prune?except_run=`); sent only with a `source_id` (strata refuses it alone).
    pub source_run: Option<&'a str>,
    pub metadata: Option<&'a serde_json::Value>,
    /// A re-ingest copy's source document (`reingest::knowledge_of`): its `metadata`
    /// (replacing `metadata`), `topic_keys`, `supersedes` and `provenance`, carried onto the copy.
    pub knowledge: Option<&'a serde_json::Value>,
}

pub struct StrataClient {
    http: reqwest::Client,
    base: String,
    auth: HeaderValue,
}

impl fmt::Debug for StrataClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StrataClient")
            .field("base", &crate::config::redact_url(&self.base))
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct StoreView {
    kind: String,
    #[serde(default)]
    parent_id: Option<Uuid>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    chunk_count: i64,
    #[serde(default)]
    children: Vec<Uuid>,
}

/// What a store lookup tells: its kind and its parent (`None` for a root), and what a mirror of
/// it needs (name, description, children) and whether it holds anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreInfo {
    /// `"leaf"` / `"router"`.
    pub kind: String,
    pub parent_id: Option<Uuid>,
    pub name: String,
    pub description: String,
    /// Chunks in the store's subtree (strata's rollup).
    pub chunk_count: i64,
    pub children: Vec<Uuid>,
}

/// One document of `GET /api/stores/{id}/documents` (strata_rag `DocumentListItem`): what a
/// re-ingest copy carries onto its copy. `topic_keys` / `supersedes` are read when strata lists
/// them (empty otherwise).
#[derive(Debug, Clone, Deserialize)]
pub struct StoreDocument {
    pub id: Uuid,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_run: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub topic_keys: Vec<String>,
    #[serde(default)]
    pub supersedes: Vec<String>,
}

/// One page of a store's documents; `next` is the cursor of the next page.
#[derive(Debug, Clone, Deserialize)]
pub struct DocumentPage {
    pub items: Vec<StoreDocument>,
    #[serde(default)]
    pub next: Option<String>,
}

/// `POST /api/stores` outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    Store(Uuid),
    /// 409 `duplicate_name`: the parent already has a child of that name.
    DuplicateName,
}

#[derive(Deserialize)]
struct CreatedStore {
    id: Uuid,
}

#[derive(Deserialize)]
struct PackList {
    #[serde(default)]
    packs: Vec<PackRow>,
}

#[derive(Deserialize)]
struct PackRow {
    root_id: Uuid,
    #[serde(default)]
    published_at: Option<String>,
}

/// Page size of a re-ingest copy's document listing (strata's maximum).
pub const DOCUMENTS_PAGE: usize = 500;

#[derive(Deserialize)]
struct IngestOutcome {
    status: String,
    id: Uuid,
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    code: String,
    #[serde(default)]
    message: String,
}

impl StrataClient {
    pub fn new(base_url: &str, token: Secret) -> crate::Result<Self> {
        let mut auth =
            HeaderValue::from_str(&format!("Bearer {}", token.expose())).map_err(|_| {
                IngestError::Config("STRATA_INGEST_STRATA_TOKEN is not a valid header value".into())
            })?;
        auth.set_sensitive(true);
        Ok(Self {
            http: http_client(TIMEOUT)?,
            base: base_url.trim().trim_end_matches('/').to_string(),
            auth,
        })
    }

    /// strata's public `GET /health` answers 2xx within `timeout`.
    pub async fn health(&self, timeout: Duration) -> bool {
        self.http
            .get(format!("{}/health", self.base))
            .timeout(timeout)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    /// The store's kind (`"leaf"` / `"router"`); `None` when strata does not know the id.
    /// Bounded by [`STORE_LOOKUP_TIMEOUT`]: the API calls it while a client waits.
    pub async fn store_kind(&self, tenant: Uuid, store_id: Uuid) -> crate::Result<Option<String>> {
        Ok(self.store(tenant, store_id).await?.map(|s| s.kind))
    }

    /// The store's kind and parent (`GET /api/stores/{id}` within `tenant`); `None` when strata
    /// does not know the id for this tenant. Bounded by [`STORE_LOOKUP_TIMEOUT`].
    pub async fn store(&self, tenant: Uuid, store_id: Uuid) -> crate::Result<Option<StoreInfo>> {
        let upstream = |message: String| IngestError::Upstream {
            service: SERVICE,
            message,
        };
        let resp = self
            .http
            .get(format!("{}/api/stores/{store_id}", self.base))
            .header(AUTHORIZATION, self.auth.clone())
            .header(TENANT_HEADER, tenant.to_string())
            .timeout(STORE_LOOKUP_TIMEOUT)
            .send()
            .await
            .map_err(|e| upstream(transport_message(e)))?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = resp
            .text()
            .await
            .map_err(|e| upstream(transport_message(e)))?;
        if !status.is_success() {
            return Err(upstream(format!(
                "GET store: status {}: {}",
                status.as_u16(),
                snippet(&body)
            )));
        }
        let view: StoreView = serde_json::from_str(&body)
            .map_err(|e| upstream(format!("GET store: unexpected body: {e}")))?;
        Ok(Some(StoreInfo {
            kind: view.kind,
            parent_id: view.parent_id,
            name: view.name,
            description: view.description,
            chunk_count: view.chunk_count,
            children: view.children,
        }))
    }

    /// One request of the re-ingest copy (API side, not a step): sends `req` with auth and the
    /// tenant header, bounded by [`STORE_LOOKUP_TIMEOUT`]; answers the status and the body.
    async fn call(
        &self,
        tenant: Uuid,
        req: reqwest::RequestBuilder,
    ) -> crate::Result<(StatusCode, String)> {
        let resp = req
            .header(AUTHORIZATION, self.auth.clone())
            .header(TENANT_HEADER, tenant.to_string())
            .timeout(STORE_LOOKUP_TIMEOUT)
            .send()
            .await
            .map_err(|e| upstream(transport_message(e)))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| upstream(transport_message(e)))?;
        Ok((status, body))
    }

    /// Creates a store (`POST /api/stores {parent_id, kind, name, description}`) for `tenant`.
    /// 409 `duplicate_name` → [`Created::DuplicateName`]; other failures as `api_error` maps them.
    pub async fn create_store(
        &self,
        tenant: Uuid,
        parent_id: Uuid,
        kind: &str,
        name: &str,
        description: &str,
    ) -> crate::Result<Created> {
        let body = json!({
            "parent_id": parent_id, "kind": kind, "name": name, "description": description,
        });
        let (status, text) = self
            .call(
                tenant,
                self.http
                    .post(format!("{}/api/stores", self.base))
                    .header(CONTENT_TYPE, "application/json")
                    .body(body.to_string()),
            )
            .await?;
        if status.is_success() {
            let c: CreatedStore = serde_json::from_str(&text)
                .map_err(|e| upstream(format!("POST store: unexpected body: {e}")))?;
            return Ok(Created::Store(c.id));
        }
        if status == StatusCode::CONFLICT && error_code(&text).as_deref() == Some("duplicate_name")
        {
            return Ok(Created::DuplicateName);
        }
        Err(api_error("POST store", status, &text))
    }

    /// One page (at most [`DOCUMENTS_PAGE`]) of `store`'s own documents, newest first.
    pub async fn list_documents(
        &self,
        tenant: Uuid,
        store: Uuid,
        cursor: Option<&str>,
    ) -> crate::Result<DocumentPage> {
        let mut url = url::Url::parse(&format!("{}/api/stores/{store}/documents", self.base))
            .map_err(|e| IngestError::Config(format!("strata url: {e}")))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("limit", &DOCUMENTS_PAGE.to_string());
            if let Some(c) = cursor {
                q.append_pair("cursor", c);
            }
        }
        let (status, text) = self.call(tenant, self.http.get(url.as_str())).await?;
        if !status.is_success() {
            return Err(api_error("GET documents", status, &text));
        }
        serde_json::from_str(&text)
            .map_err(|e| upstream(format!("GET documents: unexpected body: {e}")))
    }

    /// Deletes one document (`DELETE /api/documents/{id}`); already gone (404) is success.
    pub async fn delete_document(&self, tenant: Uuid, id: Uuid) -> crate::Result<()> {
        let (status, text) = self
            .call(
                tenant,
                self.http
                    .delete(format!("{}/api/documents/{id}", self.base)),
            )
            .await?;
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        Err(api_error("DELETE document", status, &text))
    }

    /// Whether `root` is the root of a PUBLISHED platform pack (`GET /api/platform/packs`, the
    /// packs `tenant` may read). Platform knowledge off (404) → no packs.
    pub async fn is_published_pack_root(&self, tenant: Uuid, root: Uuid) -> crate::Result<bool> {
        let (status, text) = self
            .call(
                tenant,
                self.http.get(format!("{}/api/platform/packs", self.base)),
            )
            .await?;
        if status == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(api_error("GET packs", status, &text));
        }
        let list: PackList = serde_json::from_str(&text)
            .map_err(|e| upstream(format!("GET packs: unexpected body: {e}")))?;
        Ok(list
            .packs
            .iter()
            .any(|p| p.root_id == root && p.published_at.is_some()))
    }

    /// Adds a document to leaf `store_id` for `tenant`, tagged with `origin`. One attempt. A duplicate is
    /// success.
    pub async fn deliver(
        &self,
        tenant: Uuid,
        store_id: Uuid,
        text: &str,
        source: &str,
        title: Option<&str>,
        origin: &Origin<'_>,
    ) -> Result<Delivered, StepError> {
        let mut body = json!({ "text": text, "source": source });
        if let Some(t) = title {
            body["title"] = json!(t);
        }
        if let Some(id) = origin.source_id {
            body["source_id"] = json!(id);
            if let Some(run) = origin.source_run {
                body["source_run"] = json!(run);
            }
        }
        if let Some(m) = origin.metadata {
            body["metadata"] = m.clone();
        }
        if let Some(k) = origin.knowledge {
            for field in ["metadata", "topic_keys", "supersedes", "provenance"] {
                if let Some(v) = k.get(field).filter(|v| !v.is_null()) {
                    body[field] = v.clone();
                }
            }
        }
        let body = serde_json::to_vec(&body).map_err(|e| {
            step_err(
                ErrorClass::InvalidRequest,
                "invalid_document",
                SERVICE,
                e.to_string(),
            )
        })?;
        if body.len() > MAX_BODY_BYTES {
            return Err(step_err(
                ErrorClass::InvalidRequest,
                "payload_too_large",
                SERVICE,
                format!(
                    "document body is {} bytes; strata accepts at most {MAX_BODY_BYTES}",
                    body.len()
                ),
            ));
        }
        let resp = self
            .http
            .post(format!("{}/api/stores/{store_id}/documents", self.base))
            .header(AUTHORIZATION, self.auth.clone())
            .header(TENANT_HEADER, tenant.to_string())
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| retryable("delivery_unavailable", transport_message(e)))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| retryable("delivery_unavailable", transport_message(e)))?;
        if status.is_success() {
            let o: IngestOutcome = serde_json::from_str(&text).map_err(|e| {
                retryable(
                    "delivery_unavailable",
                    format!(
                        "strata answered {} with an unexpected body: {e}",
                        status.as_u16()
                    ),
                )
            })?;
            return match o.status.as_str() {
                "created" | "existing" => Ok(Delivered {
                    document_id: o.id,
                    duplicate: o.status == "existing",
                }),
                other => Err(retryable(
                    "delivery_unavailable",
                    format!("strata answered unknown status {other:?}"),
                )),
            };
        }
        Err(classify_failure(status.as_u16(), &text))
    }
}

fn upstream(message: String) -> IngestError {
    IngestError::Upstream {
        service: SERVICE,
        message,
    }
}

/// The `error.code` of a strata error body.
fn error_code(body: &str) -> Option<String> {
    serde_json::from_str::<ErrorBody>(body)
        .ok()
        .map(|b| b.error.code)
}

/// A failed re-ingest-copy call: 404 → `UnknownStore`; 409 `pack_published` → `PackPublished`
/// (the target is frozen; never retried); 401/403, 408/429 and 5xx → `Upstream` (retried by the
/// driver); any other answer → `InvalidInput` (terminal, the copy fails with it).
fn api_error(what: &str, status: StatusCode, body: &str) -> IngestError {
    let detail = serde_json::from_str::<ErrorBody>(body)
        .ok()
        .map(|b| b.error);
    let message = match &detail {
        Some(d) => format!(
            "{what}: strata {} {}: {}",
            status.as_u16(),
            d.code,
            snippet(&d.message)
        ),
        None => format!("{what}: strata {}: {}", status.as_u16(), snippet(body)),
    };
    match (status.as_u16(), detail.as_ref().map(|d| d.code.as_str())) {
        (404, _) => IngestError::UnknownStore,
        (409, Some("pack_published")) => IngestError::PackPublished(message),
        (401 | 403 | 408 | 429 | 500..=599, _) => upstream(message),
        _ => IngestError::InvalidInput(message),
    }
}

/// 422 naming platform knowledge (strata's licence check) is `provenance_rejected`.
/// 404/413/422 and 409 `leaf_full` / `embed_model_mismatch` / `pack_published` are terminal; other 409s and 5xx (and 408/429) retry;
/// 401/403 are `Auth`; any other 4xx is terminal.
fn classify_failure(status: u16, body: &str) -> StepError {
    let detail = serde_json::from_str::<ErrorBody>(body)
        .ok()
        .map(|b| b.error);
    let code = detail.as_ref().map(|d| d.code.as_str());
    let message = match &detail {
        Some(d) => format!("strata {status} {}: {}", d.code, snippet(&d.message)),
        None => format!("strata {status}: {}", snippet(body)),
    };
    let (class, step_code) = match (status, code) {
        (409, Some("leaf_full")) => (ErrorClass::InvalidRequest, "leaf_full"),
        // Rulings R13: the leaf's lineage embeds with another model; retrying cannot change it.
        (409, Some("embed_model_mismatch")) => (ErrorClass::InvalidRequest, "embed_model_mismatch"),
        // The target store is in a published platform pack (frozen): the job's error, no retry.
        (409, Some("pack_published")) => (ErrorClass::InvalidRequest, "pack_published"),
        (409, _) => (ErrorClass::Retryable, "delivery_conflict"),
        (404, _) => (ErrorClass::InvalidRequest, "unknown_store"),
        (413, _) => (ErrorClass::InvalidRequest, "payload_too_large"),
        (422, Some("invalid_target")) => (ErrorClass::InvalidRequest, "invalid_target"),
        // A platform-owned store refused the document's provenance (missing, or a licence not on
        // STRATA_PLATFORM_LICENSES): named for what the caller must fix, never retried.
        (422, _)
            if detail
                .as_ref()
                .is_some_and(|d| d.message.contains("platform knowledge")) =>
        {
            (ErrorClass::InvalidRequest, "provenance_rejected")
        }
        (422, _) => (ErrorClass::InvalidRequest, "delivery_rejected"),
        (401 | 403, _) => (ErrorClass::Auth, "strata_auth"),
        (408 | 429 | 500..=599, _) => (ErrorClass::Retryable, "delivery_unavailable"),
        _ => (ErrorClass::InvalidRequest, "delivery_rejected"),
    };
    step_err(class, step_code, SERVICE, message)
}

fn retryable(code: &str, message: String) -> StepError {
    step_err(ErrorClass::Retryable, code, SERVICE, message)
}
