//! `/api/ingest/jobs` handlers and `/health`. Every job route is scoped by the caller's
//! [`Tenant`]; another tenant's job is `404`.

use axum::Json;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Extension, FromRequest, Multipart, Path, Query, Request, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use super::AppState;
use super::auth::Tenant;
use crate::db::jobs::{self, JOB_STATUSES, MAX_LIST_LIMIT, NewJob};
use crate::doc_fields::{DocFields, ProvenanceIn};
use crate::error::{IngestError, Result};
use crate::netguard::validate_url;
use crate::pipeline::Ctx;
use crate::pipeline::triage::OcrMode;
use crate::providers::registry::{ProbeOutcome, ProvidersView, PutProviders, TestProviderBody};

/// Most URLs one request may submit.
pub const MAX_URLS: usize = 50;
pub const LIST_DEFAULT_LIMIT: i64 = 50;

pub(super) fn bad(msg: impl Into<String>) -> IngestError {
    IngestError::BadRequest(msg.into())
}

fn path_id(p: std::result::Result<Path<Uuid>, PathRejection>) -> Result<Uuid> {
    p.map(|Path(id)| id)
        .map_err(|e| bad(format!("invalid id in path: {}", e.body_text())))
}

fn media_type(headers: &HeaderMap) -> String {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------------------------
// Options

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionsIn {
    #[serde(default)]
    ocr: OcrMode,
    #[serde(default = "yes")]
    captions: bool,
    #[serde(default)]
    language: Option<String>,
}

fn yes() -> bool {
    true
}

/// Validates job options and fills the defaults: `{"ocr":"auto","captions":true,
/// "language":null}`. Unknown fields or values are a 400. `language` is a BCP-47-like tag
/// (1–35 chars of ASCII letters, digits and `-`).
pub(super) fn parse_options(v: Option<Value>) -> Result<Value> {
    let v = match v {
        None | Some(Value::Null) => Value::Object(Default::default()),
        Some(v) => v,
    };
    let o: OptionsIn =
        serde_json::from_value(v).map_err(|e| bad(format!("invalid options: {e}")))?;
    if let Some(lang) = &o.language
        && (lang.is_empty()
            || lang.len() > 35
            || !lang.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
    {
        return Err(bad("invalid options: `language` must be a language tag"));
    }
    Ok(json!({ "ocr": o.ocr.as_str(), "captions": o.captions, "language": o.language }))
}

// ---------------------------------------------------------------------------------------------
// POST /api/ingest/jobs

/// `POST /api/ingest/jobs` → `202 {"jobs":[{"job_id", "status"}]}` (`status` = `queued`;
/// `rejected` for an upload storage_service's virus scan refused: that job carries error
/// `infected` and nothing was stored; `failed` for a structured source that could not be fetched
/// or parsed). Every job kind takes an optional `source_id`, echoed in `meta.source_id` and
/// forwarded to strata as each document's `source_id`, and an optional `source_run` (only with a
/// `source_id`), echoed in `meta.source_run` and forwarded as each document's `source_run`, and
/// optional `topic_keys` / `supersedes` / `provenance` ([`crate::doc_fields`]: validated with
/// strata's limits, 400 otherwise; kept in `meta` under those names and forwarded on every
/// delivery).
///
/// - `multipart/form-data`: a `store_id` field and optional `source_id` / `source_run` (text),
///   `options`, `topic_keys`, `supersedes`, `provenance` (JSON) fields, ALL before the first
///   `file` part (the document fields apply to every file), then one or more `file` parts, one
///   job each. Any other field, or one of those after a file, is a 400. The store is validated at the first file
///   part, before anything is written. Each file's first 64 KiB are sniffed
///   (`detect::sniff_head`): unsupported → 422 before it is stored; a zip container is accepted
///   and classified by the worker. Each file is spooled to a scratch file and stored in
///   storage_service (which virus-scans it); a file over `max_upload_bytes` (413, storage never
///   called), a client disconnect, storage unavailable (502) or any later failure in the request
///   removes the files already stored, and no job is created. Jobs are created only once the
///   whole request has been read.
/// - `application/json`: `{store_id, url | urls (1..=50), source_id?, source_run?, options?,
///   topic_keys?, supersedes?, provenance?}`, unknown fields refused; a `urls` item is a URL or
///   `{url, topic_keys?, supersedes?, provenance?}` (its fields replace the request's for it); or, with a `source` object, a structured source (`api` |
///   `datatable`, see [`super::sources`]): one job, `status` `queued` or `failed` (fetch/parse failure); or a
///   discovery source (`crawl` | `sitemap`): one queued parent job whose Discover step creates
///   url child jobs. A source body's job id is also answered as top-level `job_id`.
///   Only http(s); literal private/loopback/link-local/CGNAT/etc. IPs and internal hostnames
///   (dotless, `.internal`, `.local`, `.svc`, …; see [`crate::netguard`]) are refused.
///   Resolving hostnames (and redirects) is the webscraper's job; see the Task 10 report.
pub async fn create_jobs(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    req: Request,
) -> Result<(StatusCode, Json<Value>)> {
    // A source body creates ONE job; its id is also answered as `job_id`.
    let mut single: Option<Uuid> = None;
    let created = match media_type(req.headers()).as_str() {
        "multipart/form-data" => {
            // Refused before a byte is spooled when every upload slot is taken (R38). The body
            // is still read and discarded (bounded), so the client gets the 503 instead of a
            // connection reset while it is still sending.
            let Ok(_slot) = s.uploads.clone().try_acquire_owned() else {
                super::drain(req.into_body()).await;
                return Err(IngestError::Busy {
                    message: format!(
                        "{} uploads are already in progress; retry later",
                        s.opts.max_concurrent_uploads
                    ),
                    retry_after_s: super::upload::UPLOAD_BUSY_RETRY_AFTER_S,
                });
            };
            let mp = Multipart::from_request(req, &())
                .await
                .map_err(|e| bad(e.body_text()))?;
            super::upload::upload_jobs(&s.ctx, tenant, mp, &s.opts).await?
        }
        "application/json" => {
            let body = Bytes::from_request(req, &()).await.map_err(|e| {
                if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
                    IngestError::PayloadTooLarge
                } else {
                    bad(e.body_text())
                }
            })?;
            // Parsed once as a Value: a body with `source` is a structured source. Syntax errors
            // carry only a position, never the input.
            let v: Value =
                serde_json::from_slice(&body).map_err(|e| bad(format!("invalid body: {e}")))?;
            if super::sources::is_source_body(&v) {
                let job = super::sources::source_job(&s.ctx, tenant, v).await?;
                single = Some(job.0);
                vec![job]
            } else {
                url_jobs(&s.ctx, tenant, v)
                    .await?
                    .into_iter()
                    .map(|id| (id, "queued"))
                    .collect()
            }
        }
        _ => {
            return Err(bad(
                "Content-Type must be multipart/form-data or application/json",
            ));
        }
    };
    let jobs: Vec<Value> = created
        .iter()
        .map(|(id, status)| json!({ "job_id": id, "status": status }))
        .collect();
    let mut body = json!({ "jobs": jobs });
    if let Some(id) = single {
        body["job_id"] = json!(id);
    }
    Ok((StatusCode::ACCEPTED, Json(body)))
}

/// A job's target store must be a leaf of the caller's tenant in strata (one lookup, no DB). The
/// job paths then call [`super::store_providers::record_lineage`] right before creating the job.
pub(super) async fn validate_store(ctx: &Ctx, tenant: Uuid, store: Uuid) -> Result<()> {
    match ctx.strata.store_kind(tenant, store).await? {
        None => Err(IngestError::UnknownStore),
        Some(kind) if kind == "leaf" => Ok(()),
        Some(_) => Err(IngestError::InvalidTarget),
    }
}

/// Longest `source_id` / `source_run` (strata_rag's limit).
pub const MAX_SOURCE_ID_CHARS: usize = 200;

/// The caller's optional `source_id`: trimmed, 1..=200 characters, no control characters. Kept
/// in the job's `meta.source_id` and sent to strata as the document's `source_id`.
pub(super) fn parse_source_id(v: Option<String>) -> Result<Option<String>> {
    parse_source_field("source_id", v)
}

/// The caller's optional `source_run` (the sync run the documents belong to, which strata's
/// `DELETE /api/sources/{source_id}/documents?except_run=` keeps): the same rules as
/// `source_id`, and only together with one (strata refuses a run without an id). Kept in the
/// job's `meta.source_run` and sent to strata as the document's `source_run`.
pub(super) fn parse_source_run(
    v: Option<String>,
    source_id: Option<&str>,
) -> Result<Option<String>> {
    let run = parse_source_field("source_run", v)?;
    if run.is_some() && source_id.is_none() {
        return Err(bad(
            "`source_run` is only allowed together with `source_id`",
        ));
    }
    Ok(run)
}

fn parse_source_field(name: &str, v: Option<String>) -> Result<Option<String>> {
    let Some(v) = v else { return Ok(None) };
    let t = v.trim();
    if t.is_empty() || t.chars().count() > MAX_SOURCE_ID_CHARS || t.chars().any(char::is_control) {
        return Err(bad(format!(
            "`{name}` must be 1 to {MAX_SOURCE_ID_CHARS} characters without control characters"
        )));
    }
    Ok(Some(t.to_string()))
}

/// `meta` of a new job: `{"source_id", "source_run"}`, each when given.
pub(super) fn initial_meta(source_id: Option<&str>, source_run: Option<&str>) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(s) = source_id {
        m.insert("source_id".into(), json!(s));
    }
    if let Some(r) = source_run {
        m.insert("source_run".into(), json!(r));
    }
    Value::Object(m)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UrlJobsIn {
    store_id: Uuid,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    source_run: Option<String>,
    #[serde(default)]
    url: Option<String>,
    /// Each a URL string, or a [`UrlItemIn`] object overriding the request's document fields.
    #[serde(default)]
    urls: Option<Vec<Value>>,
    #[serde(default)]
    options: Option<Value>,
    #[serde(default)]
    topic_keys: Option<Vec<String>>,
    #[serde(default)]
    supersedes: Option<Vec<String>>,
    #[serde(default)]
    provenance: Option<ProvenanceIn>,
}

/// A `urls` item as an object: the URL and this document's own `topic_keys` / `supersedes` /
/// `provenance` (each, when given, replacing the request-level one for this URL).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UrlItemIn {
    url: String,
    #[serde(default)]
    topic_keys: Option<Vec<String>>,
    #[serde(default)]
    supersedes: Option<Vec<String>>,
    #[serde(default)]
    provenance: Option<ProvenanceIn>,
}

/// One `urls` item: a string (the request's document fields) or a [`UrlItemIn`] object.
fn url_item(i: usize, v: Value) -> Result<(String, DocFields)> {
    match v {
        Value::String(u) => Ok((u, DocFields::default())),
        Value::Object(_) => {
            let it: UrlItemIn =
                serde_json::from_value(v).map_err(|e| bad(format!("invalid `urls[{i}]`: {e}")))?;
            let fields = DocFields::parse(it.topic_keys, it.supersedes, it.provenance)?;
            Ok((it.url, fields))
        }
        _ => Err(bad(format!(
            "`urls[{i}]` must be a URL string or an object {{url, topic_keys?, supersedes?, \
             provenance?}}"
        ))),
    }
}

async fn url_jobs(ctx: &Ctx, tenant: Uuid, body: Value) -> Result<Vec<Uuid>> {
    let b: UrlJobsIn =
        serde_json::from_value(body).map_err(|e| bad(format!("invalid body: {e}")))?;
    let items = match (b.url, b.urls) {
        (Some(u), None) => vec![(u, DocFields::default())],
        (None, Some(us)) if !us.is_empty() && us.len() <= MAX_URLS => us
            .into_iter()
            .enumerate()
            .map(|(i, v)| url_item(i, v))
            .collect::<Result<Vec<_>>>()?,
        (None, Some(_)) => return Err(bad(format!("`urls` must hold 1 to {MAX_URLS} URLs"))),
        _ => return Err(bad("exactly one of `url` or `urls` is required")),
    };
    let items = items
        .into_iter()
        .map(|(u, f)| validate_url(&u).map(|u| (u, f)))
        .collect::<Result<Vec<_>>>()?;
    let options = parse_options(b.options)?;
    let source_id = parse_source_id(b.source_id)?;
    let source_run = parse_source_run(b.source_run, source_id.as_deref())?;
    let fields = DocFields::parse(b.topic_keys, b.supersedes, b.provenance)?;
    let meta = initial_meta(source_id.as_deref(), source_run.as_deref());
    validate_store(ctx, tenant, b.store_id).await?;
    super::store_providers::record_lineage(ctx, tenant, b.store_id).await?;
    let mut ids = Vec::with_capacity(items.len());
    for (url, own) in items {
        let new = NewJob {
            tenant,
            store_id: b.store_id,
            source: json!({ "kind": "url", "url": url }),
            options: options.clone(),
        };
        let mut meta = meta.clone();
        fields.overlay(&own).add_to(&mut meta);
        ids.push(jobs::create_with_meta(&ctx.pool, Uuid::now_v7(), new, &meta).await?);
    }
    Ok(ids)
}

// ---------------------------------------------------------------------------------------------
// Reads, cancel, retry, delete

/// `GET /api/ingest/jobs/{id}`: the job, its `meta` and its steps.
pub async fn get_job(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<Json<jobs::JobView>> {
    let id = path_id(id)?;
    Ok(Json(jobs::get(&s.ctx.pool, tenant, id).await?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    #[serde(default)]
    store_id: Option<Uuid>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    before: Option<Uuid>,
}

/// `GET /api/ingest/jobs?store_id=&status=&limit=&before=` → `{"jobs": [...], "next": <job id> |
/// null}`, newest first. `limit` defaults to 50; above 200 it is capped at 200; below 1 it is a
/// 400. Unknown status → 400. `next` is set when more jobs follow: pass it as `before` for the
/// next page (same filters). `before` must be one of the caller's job ids (400 otherwise).
pub async fn list_jobs(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    q: std::result::Result<Query<ListParams>, QueryRejection>,
) -> Result<Json<Value>> {
    let Query(p) = q.map_err(|e| bad(e.body_text()))?;
    if let Some(st) = &p.status
        && !JOB_STATUSES.contains(&st.as_str())
    {
        return Err(bad(format!(
            "unknown status `{st}`; one of {}",
            JOB_STATUSES.join(", ")
        )));
    }
    let limit = p.limit.unwrap_or(LIST_DEFAULT_LIMIT);
    if limit < 1 {
        return Err(bad("`limit` must be >= 1"));
    }
    let (list, next) = jobs::page(
        &s.ctx.pool,
        tenant,
        p.store_id,
        p.status.as_deref(),
        p.before,
        limit.min(MAX_LIST_LIMIT),
    )
    .await
    .map_err(|e| match e {
        IngestError::NotFound => bad("`before` is not one of your jobs"),
        e => e,
    })?;
    Ok(Json(json!({ "jobs": list, "next": next })))
}

/// `POST /api/ingest/jobs/{id}/retry`: a failed job's failed steps run again → the job view.
/// 409 unless the job is `failed` (a `deleting` job included).
pub async fn retry_job(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<Json<jobs::JobView>> {
    let id = path_id(id)?;
    jobs::retry(&s.ctx.pool, tenant, id).await?;
    Ok(Json(jobs::get(&s.ctx.pool, tenant, id).await?))
}

/// `POST /api/ingest/jobs/{id}/cancel` → 200 with the job view. A queued job (or one with no step
/// running) is `cancelled` at once; a running job is `cancelling` until its running step(s) end,
/// then `cancelled` (cooperative: nothing new is started). Its pending/failed steps are
/// `cancelled` (a crawl / sitemap parent's queued and running children as well). Idempotent on a
/// `cancelling` / `cancelled` job. 409 when the job is `deleting` or already ended (completed,
/// failed, rejected),
/// except an ended crawl / sitemap parent with open children: those are cancelled (200).
pub async fn cancel_job(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<Json<jobs::JobView>> {
    let id = path_id(id)?;
    jobs::cancel(&s.ctx.pool, tenant, id).await?;
    Ok(Json(jobs::get(&s.ctx.pool, tenant, id).await?))
}

/// How long `DELETE /api/ingest/jobs/{id}` waits for the delete to finish before answering
/// `202` (below care-bus's 8 s client timeout).
pub const DELETE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// `DELETE /api/ingest/jobs/{id}` → 200 `{"job_id", "status", "deleted_jobs", "deleted_objects",
/// "document_ids", "documents": "retained"}`: an ended job (completed, failed, rejected,
/// cancelled), its staged blobs in storage_service and, for a crawl / sitemap parent, its child
/// jobs. Documents already delivered to strata stay indexed there (`document_ids`). 409 while
/// the job is queued / running / cancelling or a step or child of it still runs (cancel first).
/// 409 `child_of_parent_job` (with `parent_job_id` in the error) for a crawl / sitemap child:
/// delete the parent, which removes its children with it.
///
/// The job (and its children) is marked `deleting` in the request (`jobs::begin_delete`); the
/// blob and row deletes then run in a task of their own, so a dropped request (client timeout,
/// disconnect) does not stop them. The request waits up to [`ApiOpts::delete_wait`] for that
/// task: done → the 200 above (or 502 `upstream_unavailable` when a blob cannot be deleted: the
/// job stays `deleting`, DELETE again or the sweeper finishes it); still working → `202
/// {"job_id", "status": "deleting"}` (poll `GET`; `404` once it is gone).
///
/// [`ApiOpts::delete_wait`]: super::ApiOpts::delete_wait
pub async fn delete_job(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<axum::response::Response> {
    use axum::response::IntoResponse;
    let id = path_id(id)?;
    let plan = jobs::begin_delete(&s.ctx.pool, tenant, id).await?;
    let ctx = s.ctx.clone();
    let mut task = tokio::spawn(async move {
        let r = jobs::finish_delete(&ctx.pool, &ctx.storage, tenant, id, plan).await;
        if let Err(e) = &r {
            tracing::warn!(job = %id, error = %e, "delete did not finish; the job stays `deleting`");
        }
        r
    });
    let d = match tokio::time::timeout(s.opts.delete_wait, &mut task).await {
        Ok(Ok(r)) => r?,
        Ok(Err(e)) => {
            // A panic in the delete task: the job stays `deleting` (DELETE again or the sweeper).
            return Err(IngestError::Io(std::io::Error::other(format!(
                "delete task failed: {e}"
            ))));
        }
        // The task keeps running (dropping a JoinHandle does not abort it).
        Err(_) => {
            return Ok((
                axum::http::StatusCode::ACCEPTED,
                Json(json!({ "job_id": id, "status": "deleting" })),
            )
                .into_response());
        }
    };
    Ok(Json(json!({
        "job_id": d.job_id,
        "status": d.status,
        "deleted_jobs": d.deleted_jobs,
        "deleted_objects": d.deleted_objects,
        "document_ids": d.document_ids,
        "documents": "retained",
    }))
    .into_response())
}

/// Largest `PUT /api/tenant/providers` body.
pub const MAX_PROVIDERS_BODY: usize = 64 * 1024;

/// `GET /api/tenant/providers`: the tenant's ocr / vision / transcribe providers and where each
/// comes from (`tenant` | `default` | `unconfigured`). Never a key: only `has_api_key`.
pub async fn get_tenant_providers(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
) -> Result<Json<ProvidersView>> {
    Ok(Json(s.ctx.providers.view(tenant).await?))
}

/// `PUT /api/tenant/providers`: replaces the tenant's provider set (a capability left out or
/// `null` is removed). Validated against the deployment (region, zdr, dialects, SSRF host rules)
/// and probed live; 422 with a specific `error.code` otherwise. Answers the stored view.
pub async fn put_tenant_providers(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    body: Bytes,
) -> Result<Json<ProvidersView>> {
    let put = parse_providers_body(&body)?;
    Ok(Json(s.ctx.providers.replace(tenant, put).await?))
}

/// A `PUT …/providers` body (tenant or store): at most [`MAX_PROVIDERS_BODY`] bytes, the
/// [`PutProviders`] shape.
pub(super) fn parse_providers_body(body: &Bytes) -> Result<PutProviders> {
    parse_secret_body(body, "providers body")
}

/// A JSON body that may carry an `api_key`: at most [`MAX_PROVIDERS_BODY`] bytes; a parse error
/// never echoes a value (only the position, or a field name).
fn parse_secret_body<T: serde::de::DeserializeOwned>(body: &Bytes, what: &str) -> Result<T> {
    if body.len() > MAX_PROVIDERS_BODY {
        return Err(IngestError::PayloadTooLarge);
    }
    serde_json::from_slice(body).map_err(|e| {
        // "invalid type: string \"sk-…\"" would echo a key sent in the wrong field: name the
        // position only. Missing / unknown field messages carry field names, never values.
        let m = e.to_string();
        if m.starts_with("invalid type") || m.starts_with("invalid value") {
            bad(format!(
                "invalid {what}: a field has the wrong type or value (line {} column {})",
                e.line(),
                e.column()
            ))
        } else {
            bad(format!("invalid {what}: {m}"))
        }
    })
}

/// `POST /api/tenant/providers/test` `{capability, dialect, base_url, model, api_key?, model_id?,
/// regions?, zdr?, timeout_ms?}` → 200 `{ok, detail}`: the PUT's live probe of one capability,
/// storing nothing and recording no metering event. A probe failure (401 from the provider,
/// unreachable, timeout) is `ok: false` naming the error class; an invalid body is 400 / 422
/// (the PUT's codes, plus `unknown_capability` and `secret_required_for_new_host`). Never a key.
pub async fn test_tenant_provider(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    body: Bytes,
) -> Result<Json<ProbeOutcome>> {
    let b: TestProviderBody = parse_secret_body(&body, "provider test body")?;
    Ok(Json(s.ctx.providers.test(tenant, b).await?))
}

/// `GET /health` (unauthenticated): always 200.
/// `GET /health`; also `"ready"`: whether the database is migrated and the job routes and
/// workers run. Not ready → `db` is `down` and `status` `degraded` whatever the probe says.
pub async fn health(State(s): State<AppState>) -> Json<Value> {
    let mut body = super::health::report(&s.ctx, &s.health).await;
    let ready = s.ready.load(std::sync::atomic::Ordering::Acquire);
    body["ready"] = json!(ready);
    if !ready {
        body["db"] = json!("down");
        body["status"] = json!("degraded");
    }
    Json(body)
}
