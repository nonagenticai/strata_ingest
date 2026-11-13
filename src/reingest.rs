//! `POST /api/stores/{id}/reingest-copy {target_store}` (agent D93, B3 T0i): re-run extraction on
//! the retained originals of every document of a strata store's subtree, with the TARGET store's
//! ocr / vision / transcribe bindings, into an empty new root mirroring the source tree. The
//! counterpart of strata_rag's `reembed-copy` for the ingest capabilities.
//!
//! What is copyable. strata-ingest keeps a document's original only while the ingest job that
//! delivered it exists (`DELETE /api/ingest/jobs/{id}` removes it):
//! - a document delivered by an upload or URL job (crawl / sitemap pages included) whose job still
//!   exists with its original → `file`: the original is copied to a new upload / url job on the
//!   mirrored target leaf, which runs the whole pipeline again (Acquire records the copy; a URL page
//!   is never scraped again) and resolves its providers by the target store;
//! - a record of an `api` / `datatable` source whose job still exists → `record`: its stored record
//!   text is delivered again by a new structured-source job (no extraction runs for a record);
//! - anything else — its job was deleted, it never came through strata-ingest (written to strata
//!   directly), or its original cannot be read — → `missing` with a reason
//!   (`originals_not_retained`, `original_too_large`, `original_infected`), listed in
//!   `missing_originals`; it is not copied.
//!
//! Each copy carries its source document's `metadata` (with `metadata.provenance`), `topic_keys`
//! and `supersedes` as strata lists them, plus its `source_id`.
//!
//! The target must be an empty ROOT store of the caller's tenant, of the source's kind, never a
//! store of a published platform pack (409 `pack_published`, checked before anything runs; strata
//! refuses every later write into a pack published meanwhile with the same code, which fails the
//! copy). The source subtree's stores are mirrored under it (kind, name, description).
//!
//! The copy is driven by [`run_driver`] (every pod; one pod at a time per copy, `lease_until`),
//! never by a schedule: only a request creates one. Each drive does one of: create the next
//! children (at most [`CHILD_BATCH`]); wait while children run; or a LISTING of the source (stores
//! mirrored, documents read). A listing that finds documents not seen before is a pass (D92 delta
//! pass); when a listing finds nothing new and every child has ended, the copy finishes: target
//! copies of documents deleted from the source meanwhile are deleted (mirrored deletion), then it
//! is `done`, or `failed` when a child did not complete. More than [`MAX_PASSES`] passes → `failed`
//! `conflict` (did not converge).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::db::jobs::{self, NewJob, RecordDelivery};
use crate::db::store_providers as srows;
use crate::error::{IngestError, Result};
use crate::infra::storage::StorageError;
use crate::infra::strata::{Created, StoreDocument, StoreInfo};
use crate::pipeline::{Ctx, original_key};

/// `meta` key of a copy's file job holding its source document's fields (the Deliver step sends
/// them to strata).
pub const KNOWLEDGE_META: &str = "knowledge";
/// `meta` key of a copy's child job naming the copy.
pub const COPY_META: &str = "reingest_copy";
/// Passes (listings that found new source documents) before the copy fails as not converged.
pub const MAX_PASSES: i32 = 8;
/// A claimed copy is another pod's after this long without a heartbeat.
pub const LEASE: Duration = Duration::from_secs(120);
/// How often [`run_driver`] looks for copies to drive.
pub const DRIVE_INTERVAL: Duration = Duration::from_secs(5);
/// Children created per drive (each copies one original through storage_service).
pub const CHILD_BATCH: usize = 20;
/// Most stores in a source subtree.
pub const MAX_STORES: usize = 10_000;
/// Deepest target lineage walked (as `api::store_providers::MAX_STORE_DEPTH`).
pub const MAX_DEPTH: usize = 32;
/// Most `missing_originals` entries a job view lists (`documents_missing` counts them all).
pub const MAX_MISSING_LISTED: i64 = 1000;
/// Document ids per producer lookup.
const LOOKUP_CHUNK: usize = 500;

/// A copy as `POST` (202) and `GET /api/stores/{id}/reingest-copy/{job}` answer it.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ReingestCopyJob {
    pub job: Uuid,
    pub source_store: Uuid,
    pub target_store: Uuid,
    /// `running`, `done` or `failed`.
    pub state: String,
    /// Source documents copied (their new ingest job completed).
    pub documents_done: i64,
    /// Source documents seen (copyable or not; documents deleted from the source meanwhile are
    /// not counted).
    pub documents_total: i64,
    /// Copies whose ingest job failed, was rejected, cancelled or deleted.
    pub documents_failed: i64,
    pub documents_missing: i64,
    /// Documents that cannot be copied (at most [`MAX_MISSING_LISTED`]).
    pub missing_originals: Vec<MissingOriginal>,
    /// `{code, message}` of a failed copy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    pub passes: i32,
    /// RFC 3339 (UTC, microseconds).
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MissingOriginal {
    pub document_id: Uuid,
    /// `originals_not_retained` | `original_too_large` | `original_infected`.
    pub reason: String,
    pub title: Option<String>,
    pub source_id: Option<String>,
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn invalid_target(message: impl Into<String>) -> IngestError {
    IngestError::InvalidCopyTarget(message.into())
}

/// The root of `store` (whose parent is `parent`), walking strata within `tenant`.
async fn root_of(ctx: &Ctx, tenant: Uuid, store: Uuid, parent: Option<Uuid>) -> Result<Uuid> {
    let (mut at, mut next) = (store, parent);
    for _ in 0..=MAX_DEPTH {
        let Some(p) = next else { return Ok(at) };
        at = p;
        next = match ctx.strata.store(tenant, p).await? {
            Some(info) => info.parent_id,
            None => return Err(IngestError::UnknownStore),
        };
    }
    Err(IngestError::Upstream {
        service: "strata",
        message: format!("store {store}: more than {MAX_DEPTH} ancestors"),
    })
}

/// Validates and records a copy of `source`'s subtree into `target` (both `tenant`'s); the
/// driver then runs it. 404 `unknown_store` (either unknown in the tenant); 409 `pack_published`
/// (the target is in a published platform pack); 422 `invalid_target` (the source itself, not a
/// root, another kind); 409 `target_not_empty`; 409 `conflict` (a copy into the target is
/// already running).
pub async fn start(ctx: &Ctx, tenant: Uuid, source: Uuid, target: Uuid) -> Result<ReingestCopyJob> {
    let src = ctx
        .strata
        .store(tenant, source)
        .await?
        .ok_or(IngestError::UnknownStore)?;
    let tgt = ctx
        .strata
        .store(tenant, target)
        .await?
        .ok_or(IngestError::UnknownStore)?;
    if source == target {
        return Err(invalid_target(
            "the target must be another store than the source",
        ));
    }
    // A frozen target answers its own code first, whatever else is wrong with it.
    let root = root_of(ctx, tenant, target, tgt.parent_id).await?;
    if ctx.strata.is_published_pack_root(tenant, root).await? {
        return Err(IngestError::PackPublished(format!(
            "store {target} is in a published platform pack; copy into a new root (a new pack \
             version's root or a new generation root)"
        )));
    }
    if tgt.parent_id.is_some() {
        return Err(invalid_target(
            "the target must be a new root store (it has a parent)",
        ));
    }
    if tgt.kind != src.kind {
        return Err(invalid_target(format!(
            "the target is a {} and the source a {}; they must be of the same kind",
            tgt.kind, src.kind
        )));
    }
    if !tgt.children.is_empty() || tgt.chunk_count > 0 {
        return Err(IngestError::TargetNotEmpty(format!(
            "store {target} holds {} child stores and {} chunks",
            tgt.children.len(),
            tgt.chunk_count
        )));
    }
    let id = Uuid::now_v7();
    let mut tx = ctx.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO reingest_copies (id, tenant, source_store, target_store, state)
         VALUES ($1, $2, $3, $4, 'running')",
    )
    .bind(id)
    .bind(tenant)
    .bind(source)
    .bind(target)
    .execute(&mut *tx)
    .await;
    if let Err(e) = inserted {
        let unique = e
            .as_database_error()
            .and_then(|d| d.code())
            .is_some_and(|c| c == "23505");
        if unique {
            return Err(IngestError::Conflict(format!(
                "a re-ingest copy into store {target} is already running"
            )));
        }
        return Err(e.into());
    }
    sqlx::query(
        "INSERT INTO reingest_copy_stores (copy_id, source_store, target_store) VALUES ($1, $2, $3)",
    )
    .bind(id)
    .bind(source)
    .bind(target)
    .execute(&mut *tx)
    .await?;
    // The target is a root: its lineage is empty (its leaves inherit its bindings through theirs).
    srows::put_lineage(&mut *tx, tenant, target, &[]).await?;
    tx.commit().await?;
    tracing::info!(%tenant, copy = %id, %source, %target, "re-ingest copy started");
    get(&ctx.pool, tenant, source, id).await
}

/// The tenant's copy `job` of `source` (another tenant's, or of another source → 404).
pub async fn get(pool: &PgPool, tenant: Uuid, source: Uuid, job: Uuid) -> Result<ReingestCopyJob> {
    let row = sqlx::query(
        "SELECT id, source_store, target_store, state, passes, error, created_at, finished_at
         FROM reingest_copies WHERE id = $1 AND tenant = $2 AND source_store = $3",
    )
    .bind(job)
    .bind(tenant)
    .bind(source)
    .fetch_optional(pool)
    .await?
    .ok_or(IngestError::NotFound)?;
    let counts = sqlx::query(
        "SELECT
           count(*) AS total,
           count(*) FILTER (WHERE i.kind <> 'missing' AND j.status = 'completed') AS done,
           count(*) FILTER (WHERE i.kind <> 'missing' AND i.child_job IS NOT NULL
                              AND (j.id IS NULL
                                   OR j.status IN ('failed','rejected','cancelled','deleting')))
             AS failed,
           count(*) FILTER (WHERE i.kind = 'missing') AS missing
         FROM reingest_copy_items i LEFT JOIN ingest_jobs j ON j.id = i.child_job
         WHERE i.copy_id = $1 AND NOT i.removed",
    )
    .bind(job)
    .fetch_one(pool)
    .await?;
    let missing = sqlx::query(
        "SELECT source_document_id, reason, title, source_id FROM reingest_copy_items
         WHERE copy_id = $1 AND kind = 'missing' AND NOT removed
         ORDER BY created_at, source_document_id LIMIT $2",
    )
    .bind(job)
    .bind(MAX_MISSING_LISTED)
    .fetch_all(pool)
    .await?
    .iter()
    .map(|r| {
        Ok(MissingOriginal {
            document_id: r.try_get("source_document_id")?,
            reason: r
                .try_get::<Option<String>, _>("reason")?
                .unwrap_or_else(|| "originals_not_retained".to_string()),
            title: r.try_get("title")?,
            source_id: r.try_get("source_id")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    let created: DateTime<Utc> = row.try_get("created_at")?;
    let finished: Option<DateTime<Utc>> = row.try_get("finished_at")?;
    Ok(ReingestCopyJob {
        job: row.try_get("id")?,
        source_store: row.try_get("source_store")?,
        target_store: row.try_get("target_store")?,
        state: row.try_get("state")?,
        documents_done: counts.try_get("done")?,
        documents_total: counts.try_get("total")?,
        documents_failed: counts.try_get("failed")?,
        documents_missing: counts.try_get("missing")?,
        missing_originals: missing,
        error: row.try_get("error")?,
        passes: row.try_get("passes")?,
        started_at: rfc3339(created),
        finished_at: finished.map(rfc3339),
    })
}

// ---------------------------------------------------------------------------------------------
// Driver

/// A claimed copy.
#[derive(Debug, Clone, Copy)]
struct CopyRun {
    id: Uuid,
    tenant: Uuid,
    source: Uuid,
    passes: i32,
}

/// What one drive did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drive {
    /// Children were created, or a listing found new documents.
    Progress,
    /// Children are still running.
    Waiting,
    /// The copy ended (`done` or `failed`).
    Ended,
}

/// Drives copies at start, then every `interval`, until `shutdown`: each tick drives every
/// copy whose lease is free once. Safe with several pods.
pub async fn run_driver(ctx: Arc<Ctx>, interval: Duration, shutdown: CancellationToken) {
    loop {
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match drive_once(&ctx, interval).await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "re-ingest copy driver: claim failed");
                    break;
                }
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// Claims one running copy whose lease is free, drives it one step and frees its lease for
/// `rest` (a copy waiting on its children is looked at again after `rest`). `None` when no copy
/// is free. A transient failure (strata / storage_service unavailable, the database) leaves the
/// copy running for the next drive; any other fails it with that error's code.
pub async fn drive_once(ctx: &Ctx, rest: Duration) -> Result<Option<(Uuid, Drive)>> {
    let Some(copy) = claim(&ctx.pool).await? else {
        return Ok(None);
    };
    let outcome = drive(ctx, copy).await;
    let drive = match outcome {
        Ok(d) => d,
        Err(e) if transient(&e) => {
            tracing::warn!(copy = %copy.id, error = %e, "re-ingest copy: transient failure, retried");
            Drive::Waiting
        }
        Err(e) => {
            tracing::warn!(copy = %copy.id, error = %e, code = e.code(), "re-ingest copy failed");
            finish(
                &ctx.pool,
                copy.id,
                "failed",
                Some(json!({"code": e.code(), "message": e.to_string()})),
            )
            .await?;
            Drive::Ended
        }
    };
    if drive != Drive::Ended {
        sqlx::query(
            "UPDATE reingest_copies
             SET lease_until = now() + make_interval(secs => $2::float8), updated_at = now()
             WHERE id = $1 AND state = 'running'",
        )
        .bind(copy.id)
        .bind(rest.as_secs_f64())
        .execute(&ctx.pool)
        .await?;
    }
    Ok(Some((copy.id, drive)))
}

fn transient(e: &IngestError) -> bool {
    matches!(
        e,
        IngestError::Upstream { .. }
            | IngestError::Db(_)
            | IngestError::Io(_)
            | IngestError::Busy { .. }
            | IngestError::NotReady
    )
}

async fn claim(pool: &PgPool) -> Result<Option<CopyRun>> {
    let row = sqlx::query(
        "UPDATE reingest_copies
         SET lease_until = now() + make_interval(secs => $1::float8), updated_at = now()
         WHERE id = (SELECT id FROM reingest_copies
                     WHERE state = 'running' AND (lease_until IS NULL OR lease_until <= now())
                     ORDER BY lease_until NULLS FIRST, created_at
                     LIMIT 1 FOR UPDATE SKIP LOCKED)
         RETURNING id, tenant, source_store, target_store, passes",
    )
    .bind(LEASE.as_secs_f64())
    .fetch_optional(pool)
    .await?;
    row.map(|r| -> Result<CopyRun> {
        Ok(CopyRun {
            id: r.try_get("id")?,
            tenant: r.try_get("tenant")?,
            source: r.try_get("source_store")?,
            passes: r.try_get("passes")?,
        })
    })
    .transpose()
}

async fn heartbeat(pool: &PgPool, copy: Uuid) -> Result<()> {
    sqlx::query(
        "UPDATE reingest_copies SET lease_until = now() + make_interval(secs => $2::float8)
         WHERE id = $1 AND state = 'running'",
    )
    .bind(copy)
    .bind(LEASE.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

async fn finish(pool: &PgPool, copy: Uuid, state: &str, error: Option<Value>) -> Result<()> {
    sqlx::query(
        "UPDATE reingest_copies
         SET state = $2, error = $3, lease_until = NULL, finished_at = now(), updated_at = now()
         WHERE id = $1 AND state = 'running'",
    )
    .bind(copy)
    .bind(state)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

async fn drive(ctx: &Ctx, copy: CopyRun) -> Result<Drive> {
    if create_children(ctx, copy).await? > 0 {
        return Ok(Drive::Progress);
    }
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM reingest_copy_items i JOIN ingest_jobs j ON j.id = i.child_job
         WHERE i.copy_id = $1 AND j.status IN ('queued','running','cancelling')",
    )
    .bind(copy.id)
    .fetch_one(&ctx.pool)
    .await?;
    if open > 0 {
        return Ok(Drive::Waiting);
    }
    let new = listing(ctx, copy).await?;
    if new > 0 {
        let passes = copy.passes + 1;
        sqlx::query("UPDATE reingest_copies SET passes = $2 WHERE id = $1")
            .bind(copy.id)
            .bind(passes)
            .execute(&ctx.pool)
            .await?;
        if passes > MAX_PASSES {
            return Err(IngestError::Conflict(format!(
                "re-ingest copy did not converge: the source kept gaining documents for \
                 {MAX_PASSES} passes; run it again"
            )));
        }
        return Ok(Drive::Progress);
    }
    finalize(ctx, copy).await?;
    Ok(Drive::Ended)
}

// ---------------------------------------------------------------------------------------------
// Listing: mirror the source tree, record the documents not seen before

/// The source subtree (breadth first, the source first) as strata reports it now.
async fn source_tree(ctx: &Ctx, copy: CopyRun) -> Result<Vec<(Uuid, StoreInfo)>> {
    let root = ctx
        .strata
        .store(copy.tenant, copy.source)
        .await?
        .ok_or(IngestError::UnknownStore)?;
    let mut tree = vec![(copy.source, root)];
    let mut at = 0;
    while at < tree.len() {
        let children = tree[at].1.children.clone();
        for c in children {
            if tree.len() >= MAX_STORES {
                return Err(IngestError::InvalidInput(format!(
                    "the source subtree holds more than {MAX_STORES} stores"
                )));
            }
            // a child deleted since its parent was read is skipped
            if let Some(info) = ctx.strata.store(copy.tenant, c).await? {
                tree.push((c, info));
            }
        }
        at += 1;
    }
    Ok(tree)
}

/// Creates the target stores mirroring `tree` that do not exist yet (parents first) and records
/// each one's lineage. Returns source store → target store.
async fn mirror(
    ctx: &Ctx,
    copy: CopyRun,
    tree: &[(Uuid, StoreInfo)],
) -> Result<HashMap<Uuid, Uuid>> {
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT source_store, target_store FROM reingest_copy_stores WHERE copy_id = $1",
    )
    .bind(copy.id)
    .fetch_all(&ctx.pool)
    .await?;
    let mut map: HashMap<Uuid, Uuid> = rows.into_iter().collect();
    for (store, info) in tree.iter().skip(1) {
        if map.contains_key(store) {
            continue;
        }
        let Some(parent) = info.parent_id.and_then(|p| map.get(&p).copied()) else {
            return Err(IngestError::Upstream {
                service: "strata",
                message: format!("store {store}: its parent is not in the source subtree"),
            });
        };
        let mirrored = match ctx
            .strata
            .create_store(
                copy.tenant,
                parent,
                &info.kind,
                &info.name,
                &info.description,
            )
            .await?
        {
            Created::Store(id) => id,
            // created by a drive that died before recording it
            Created::DuplicateName => existing_child(ctx, copy, parent, &info.name, &map).await?,
        };
        let mut lineage = vec![parent];
        lineage.extend(
            srows::lineage(&ctx.pool, copy.tenant, parent)
                .await?
                .unwrap_or_default(),
        );
        let mut tx = ctx.pool.begin().await?;
        sqlx::query(
            "INSERT INTO reingest_copy_stores (copy_id, source_store, target_store)
             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        )
        .bind(copy.id)
        .bind(store)
        .bind(mirrored)
        .execute(&mut *tx)
        .await?;
        srows::put_lineage(&mut *tx, copy.tenant, mirrored, &lineage).await?;
        tx.commit().await?;
        map.insert(*store, mirrored);
        heartbeat(&ctx.pool, copy.id).await?;
    }
    Ok(map)
}

/// The child of target store `parent` named `name` that no source store maps to yet.
async fn existing_child(
    ctx: &Ctx,
    copy: CopyRun,
    parent: Uuid,
    name: &str,
    map: &HashMap<Uuid, Uuid>,
) -> Result<Uuid> {
    let mapped: HashSet<Uuid> = map.values().copied().collect();
    let children = ctx
        .strata
        .store(copy.tenant, parent)
        .await?
        .map(|p| p.children)
        .unwrap_or_default();
    for c in children {
        if mapped.contains(&c) {
            continue;
        }
        if ctx
            .strata
            .store(copy.tenant, c)
            .await?
            .is_some_and(|i| i.name == name)
        {
            return Ok(c);
        }
    }
    Err(IngestError::Conflict(format!(
        "target store {parent} refused a child named {name:?} as a duplicate, but holds none"
    )))
}

/// The fields of `d` a copy carries: `metadata` (with `provenance`), `topic_keys`, `supersedes`,
/// `provenance` (strata's ingest body takes provenance as its own field) and `source_run` (the
/// document's sync run as strata lists it, so `prune?except_run=` treats the copy alike).
pub fn knowledge_of(d: &StoreDocument) -> Value {
    let mut k = Map::new();
    if let Some(run) = &d.source_run {
        k.insert("source_run".into(), json!(run));
    }
    k.insert("metadata".into(), Value::Object(d.metadata.clone()));
    if !d.topic_keys.is_empty() {
        k.insert("topic_keys".into(), json!(d.topic_keys));
    }
    if !d.supersedes.is_empty() {
        k.insert("supersedes".into(), json!(d.supersedes));
    }
    if let Some(p) = d.metadata.get("provenance").filter(|p| p.is_object()) {
        k.insert("provenance".into(), p.clone());
    }
    Value::Object(k)
}

/// One listing: mirror the tree, read every source leaf's documents, record the new ones (how
/// each is copied) and mark the recorded ones no longer in the source `removed`. Returns how many
/// were new.
async fn listing(ctx: &Ctx, copy: CopyRun) -> Result<usize> {
    let tree = source_tree(ctx, copy).await?;
    let map = mirror(ctx, copy, &tree).await?;
    let mut docs: Vec<(Uuid, StoreDocument)> = Vec::new();
    for (store, info) in &tree {
        if info.kind != "leaf" {
            continue;
        }
        let mut cursor: Option<String> = None;
        loop {
            let page = ctx
                .strata
                .list_documents(copy.tenant, *store, cursor.as_deref())
                .await?;
            docs.extend(page.items.into_iter().map(|d| (*store, d)));
            match page.next {
                Some(n) => cursor = Some(n),
                None => break,
            }
            heartbeat(&ctx.pool, copy.id).await?;
        }
    }
    let seen: HashSet<Uuid> = sqlx::query_scalar::<_, Uuid>(
        "SELECT source_document_id FROM reingest_copy_items WHERE copy_id = $1",
    )
    .bind(copy.id)
    .fetch_all(&ctx.pool)
    .await?
    .into_iter()
    .collect();
    let listed: Vec<Uuid> = docs.iter().map(|(_, d)| d.id).collect();
    let fresh: Vec<&(Uuid, StoreDocument)> =
        docs.iter().filter(|(_, d)| !seen.contains(&d.id)).collect();
    for chunk in fresh.chunks(LOOKUP_CHUNK) {
        let ids: Vec<Uuid> = chunk.iter().map(|(_, d)| d.id).collect();
        let producers = producers(&ctx.pool, copy.tenant, &ids).await?;
        let mut tx = ctx.pool.begin().await?;
        for (store, d) in chunk {
            let Some(target) = map.get(store).copied() else {
                return Err(IngestError::Upstream {
                    service: "strata",
                    message: format!("store {store} is not mirrored"),
                });
            };
            let (kind, producer, reason) = match producers.get(&d.id) {
                Some(p) if p.kind == "record" => ("record", Some(p.job), None),
                Some(p) if p.has_original => ("file", Some(p.job), None),
                _ => ("missing", None, Some("originals_not_retained")),
            };
            sqlx::query(
                "INSERT INTO reingest_copy_items
                   (copy_id, source_document_id, source_store, target_store, kind, producer_job,
                    reason, title, source_id, knowledge)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (copy_id, source_document_id) DO NOTHING",
            )
            .bind(copy.id)
            .bind(d.id)
            .bind(*store)
            .bind(target)
            .bind(kind)
            .bind(producer)
            .bind(reason)
            .bind(d.title.as_deref())
            .bind(d.source_id.as_deref())
            .bind(knowledge_of(d))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        heartbeat(&ctx.pool, copy.id).await?;
    }
    sqlx::query(
        "UPDATE reingest_copy_items SET removed = true
         WHERE copy_id = $1 AND NOT removed AND NOT (source_document_id = ANY($2))",
    )
    .bind(copy.id)
    .bind(listed.as_slice())
    .execute(&ctx.pool)
    .await?;
    Ok(fresh.len())
}

/// The ingest job that delivered a document and whether it can be copied.
struct Producer {
    job: Uuid,
    /// `file` (upload / url) or `record` (api / datatable).
    kind: &'static str,
    has_original: bool,
}

/// For each of `docs`, the newest live job of `tenant` that delivered it, preferring one that
/// still holds its original (a record job holds its texts as long as it exists).
async fn producers(pool: &PgPool, tenant: Uuid, docs: &[Uuid]) -> Result<HashMap<Uuid, Producer>> {
    let rows = sqlx::query(
        "SELECT d.doc, p.id, p.kind, p.has_original
         FROM unnest($2::uuid[]) AS d(doc)
         JOIN LATERAL (
           SELECT j.id, j.source->>'kind' AS kind,
                  EXISTS (SELECT 1 FROM ingest_assets a
                          WHERE a.job_id = j.id AND a.kind = 'original') AS has_original
           FROM ingest_jobs j
           WHERE j.tenant = $1 AND j.document_ids @> ARRAY[d.doc] AND j.status <> 'deleting'
           ORDER BY (EXISTS (SELECT 1 FROM ingest_assets a
                             WHERE a.job_id = j.id AND a.kind = 'original')
                     OR j.source->>'kind' IN ('api','datatable')) DESC,
                    j.created_at DESC
           LIMIT 1) p ON true",
    )
    .bind(tenant)
    .bind(docs)
    .fetch_all(pool)
    .await?;
    let mut out = HashMap::new();
    for r in rows {
        let kind: Option<String> = r.try_get("kind")?;
        let kind = match kind.as_deref() {
            Some("upload" | "url") => "file",
            Some("api" | "datatable") => "record",
            _ => continue,
        };
        out.insert(
            r.try_get::<Uuid, _>("doc")?,
            Producer {
                job: r.try_get("id")?,
                kind,
                has_original: r.try_get("has_original")?,
            },
        );
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Children

struct Pending {
    doc: Uuid,
    kind: String,
    producer: Uuid,
    target: Uuid,
    knowledge: Value,
}

/// Creates up to [`CHILD_BATCH`] children for recorded items without one; returns how many items
/// it settled (a child created, or the item turned `missing`).
async fn create_children(ctx: &Ctx, copy: CopyRun) -> Result<usize> {
    let rows = sqlx::query(
        "SELECT source_document_id, kind, producer_job, target_store, knowledge
         FROM reingest_copy_items
         WHERE copy_id = $1 AND kind IN ('file','record') AND child_job IS NULL AND NOT removed
         ORDER BY created_at, source_document_id LIMIT $2",
    )
    .bind(copy.id)
    .bind(i64::try_from(CHILD_BATCH * 10).unwrap_or(i64::MAX))
    .fetch_all(&ctx.pool)
    .await?;
    let mut pending = Vec::with_capacity(rows.len());
    for r in rows {
        pending.push(Pending {
            doc: r.try_get("source_document_id")?,
            kind: r.try_get("kind")?,
            producer: r
                .try_get::<Option<Uuid>, _>("producer_job")?
                .unwrap_or_default(),
            target: r.try_get("target_store")?,
            knowledge: r.try_get("knowledge")?,
        });
    }
    let mut settled = 0usize;
    let mut children = 0usize;
    // records: one child per (producer, target) group
    let mut groups: Vec<((Uuid, Uuid), Vec<&Pending>)> = Vec::new();
    for p in &pending {
        if children >= CHILD_BATCH {
            break;
        }
        if p.kind == "file" {
            settled += file_child(ctx, copy, p).await?;
            children += 1;
            heartbeat(&ctx.pool, copy.id).await?;
        } else {
            match groups
                .iter_mut()
                .find(|(k, _)| *k == (p.producer, p.target))
            {
                Some((_, v)) => v.push(p),
                None => {
                    groups.push(((p.producer, p.target), vec![p]));
                    children += 1;
                }
            }
        }
    }
    for ((producer, target), items) in groups {
        settled += records_child(ctx, copy, producer, target, &items).await?;
        heartbeat(&ctx.pool, copy.id).await?;
    }
    Ok(settled)
}

async fn mark_missing(pool: &PgPool, copy: Uuid, doc: Uuid, reason: &str) -> Result<usize> {
    sqlx::query(
        "UPDATE reingest_copy_items SET kind = 'missing', reason = $3
         WHERE copy_id = $1 AND source_document_id = $2 AND child_job IS NULL",
    )
    .bind(copy)
    .bind(doc)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(1)
}

/// The child `meta`: the producer's `source_id` / `source_run`, the copy and its producer, plus
/// `extra`. (A copied document's run as strata lists it travels in its `knowledge` and wins at
/// Deliver.)
fn child_meta(copy: CopyRun, producer: Uuid, producer_meta: &Value, extra: Option<Value>) -> Value {
    let mut m = Map::new();
    for key in ["source_id", "source_run"] {
        if let Some(v) = producer_meta.get(key).filter(|v| v.is_string()) {
            m.insert(key.into(), v.clone());
        }
    }
    m.insert(COPY_META.into(), json!(copy.id));
    m.insert("reingest_of".into(), json!(producer));
    if let Some(k) = extra {
        m.insert(KNOWLEDGE_META.into(), k);
    }
    Value::Object(m)
}

/// One upload / url child: the producer's original copied to the child's original key, then the
/// child job (Acquire records the copy) created on the mirrored target store and recorded on the
/// item in one transaction.
async fn file_child(ctx: &Ctx, copy: CopyRun, p: &Pending) -> Result<usize> {
    let row = sqlx::query(
        "SELECT j.source, j.options, j.meta, a.object_key, a.mime
         FROM ingest_jobs j
         JOIN ingest_assets a ON a.job_id = j.id AND a.kind = 'original'
         WHERE j.id = $1 AND j.tenant = $2 AND j.status <> 'deleting'",
    )
    .bind(p.producer)
    .bind(copy.tenant)
    .fetch_optional(&ctx.pool)
    .await?;
    let Some(row) = row else {
        return mark_missing(&ctx.pool, copy.id, p.doc, "originals_not_retained").await;
    };
    let source: Value = row.try_get("source")?;
    let options: Value = row.try_get("options")?;
    let meta: Value = row.try_get("meta")?;
    let key: String = row.try_get("object_key")?;
    let mime: String = row.try_get("mime")?;
    let max = ctx.cfg.limits.max_upload_bytes;
    let body = match ctx.storage.get_bytes(&key, max).await {
        Ok(b) => b,
        Err(StorageError::NotFound) => {
            return mark_missing(&ctx.pool, copy.id, p.doc, "originals_not_retained").await;
        }
        Err(StorageError::TooLarge(_)) => {
            return mark_missing(&ctx.pool, copy.id, p.doc, "original_too_large").await;
        }
        Err(e) => return Err(e.into()),
    };
    let size = body.len() as u64;
    let child = Uuid::now_v7();
    let child_key = original_key(copy.tenant, child);
    let verdict = match ctx.storage.upload_bytes(&child_key, body, &mime).await {
        Ok(up) => up.verdict(),
        Err(StorageError::Infected(_)) => {
            return mark_missing(&ctx.pool, copy.id, p.doc, "original_infected").await;
        }
        Err(e) => return Err(e.into()),
    };
    let str_of = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    let child_source = match source.get("kind").and_then(Value::as_str) {
        Some("url") => {
            let url = str_of(&source, "url").unwrap_or_default();
            json!({
                "kind": "url",
                "url": url,
                "prefetched": {
                    "bytes": size,
                    "scan": verdict.as_str(),
                    "final_url": str_of(&meta, "final_url").unwrap_or_else(|| url.clone()),
                    "title": str_of(&meta, "title"),
                },
            })
        }
        _ => json!({
            "kind": "upload",
            "filename": str_of(&source, "filename"),
            "mime": str_of(&source, "mime").unwrap_or_else(|| mime.clone()),
            "bytes": size,
            "scan": verdict.as_str(),
        }),
    };
    let new = NewJob {
        tenant: copy.tenant,
        store_id: p.target,
        source: child_source,
        options,
    };
    let child_meta = child_meta(copy, p.producer, &meta, Some(p.knowledge.clone()));
    // `false`: another drive (this copy's lease expired meanwhile) recorded a child first.
    let recorded = async {
        let mut tx = ctx.pool.begin().await?;
        jobs::create_in(&mut tx, child, &new, &child_meta).await?;
        let n = sqlx::query(
            "UPDATE reingest_copy_items SET child_job = $3
             WHERE copy_id = $1 AND source_document_id = $2 AND child_job IS NULL",
        )
        .bind(copy.id)
        .bind(p.doc)
        .bind(child)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Ok(false);
        }
        tx.commit().await?;
        Ok::<bool, IngestError>(true)
    }
    .await;
    if !matches!(recorded, Ok(true)) {
        // nothing references the copied original: remove it (best effort)
        if let Err(d) = ctx.storage.delete(&child_key).await {
            tracing::warn!(key = %child_key, error = %d, "re-ingest copy: orphaned original not removed");
        }
    }
    recorded.map(|_| 1)
}

/// One structured-source child for `items` (records of `producer` going to `target`): each
/// record's stored text and Deliver input, with the source document's fields, delivered again.
async fn records_child(
    ctx: &Ctx,
    copy: CopyRun,
    producer: Uuid,
    target: Uuid,
    items: &[&Pending],
) -> Result<usize> {
    let job = sqlx::query(
        "SELECT source, options, meta FROM ingest_jobs
         WHERE id = $1 AND tenant = $2 AND status <> 'deleting'",
    )
    .bind(producer)
    .bind(copy.tenant)
    .fetch_optional(&ctx.pool)
    .await?;
    let Some(job) = job else {
        for p in items {
            mark_missing(&ctx.pool, copy.id, p.doc, "originals_not_retained").await?;
        }
        return Ok(items.len());
    };
    let mut settled = 0usize;
    let mut records: Vec<RecordDelivery> = Vec::new();
    let mut docs: Vec<(Uuid, String)> = Vec::new();
    for p in items {
        let row = sqlx::query(
            "SELECT s.input, a.body FROM ingest_steps s
             JOIN ingest_artifacts a ON a.job_id = s.job_id AND a.key = s.input->>'bundle_key'
             WHERE s.job_id = $1 AND s.kind = 'deliver' AND s.output->>'document_id' = $2
             LIMIT 1",
        )
        .bind(producer)
        .bind(p.doc.to_string())
        .fetch_optional(&ctx.pool)
        .await?;
        let Some(row) = row else {
            settled += mark_missing(&ctx.pool, copy.id, p.doc, "originals_not_retained").await?;
            continue;
        };
        let input: Value = row.try_get("input")?;
        let text: String = row.try_get("body")?;
        let key = format!("{:06}", records.len() + 1);
        let artifact = format!("record/{key}.md");
        let mut input_out = Map::new();
        input_out.insert("bundle_key".into(), json!(artifact));
        for field in ["title", "source", "metadata"] {
            if let Some(v) = input.get(field) {
                input_out.insert(field.into(), v.clone());
            }
        }
        input_out.insert("knowledge".into(), p.knowledge.clone());
        records.push(RecordDelivery {
            key: key.clone(),
            artifact,
            text,
            input: Value::Object(input_out),
        });
        docs.push((p.doc, key));
    }
    if records.is_empty() {
        return Ok(settled);
    }
    let source: Value = job.try_get("source")?;
    let options: Value = job.try_get("options")?;
    let meta: Value = job.try_get("meta")?;
    let child = Uuid::now_v7();
    let new = NewJob {
        tenant: copy.tenant,
        store_id: target,
        source,
        options,
    };
    let child_meta = child_meta(copy, producer, &meta, None);
    let mut tx = ctx.pool.begin().await?;
    jobs::create_records_in(&mut tx, child, &new, &child_meta, &records).await?;
    for (doc, key) in &docs {
        let n = sqlx::query(
            "UPDATE reingest_copy_items SET child_job = $3, child_key = $4
             WHERE copy_id = $1 AND source_document_id = $2 AND child_job IS NULL",
        )
        .bind(copy.id)
        .bind(*doc)
        .bind(child)
        .bind(key.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            // another drive (this copy's lease expired meanwhile) got there first: roll back
            return Ok(settled);
        }
    }
    tx.commit().await?;
    Ok(settled + docs.len())
}

// ---------------------------------------------------------------------------------------------
// Finish

/// Every child has ended and the last listing found nothing new: copies of documents deleted from
/// the source meanwhile are deleted from the target, then the copy is `done` — or `failed` when a
/// child did not complete (the children's own error code when they all share one, e.g.
/// `pack_published`, else `documents_failed`).
async fn finalize(ctx: &Ctx, copy: CopyRun) -> Result<()> {
    let gone: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT (s.output->>'document_id')::uuid
         FROM reingest_copy_items i
         JOIN ingest_steps s ON s.job_id = i.child_job AND s.kind = 'deliver'
              AND (i.child_key IS NULL OR s.key = i.child_key)
         WHERE i.copy_id = $1 AND i.removed AND s.output ? 'document_id'",
    )
    .bind(copy.id)
    .fetch_all(&ctx.pool)
    .await?;
    for doc in gone {
        ctx.strata.delete_document(copy.tenant, doc).await?;
        heartbeat(&ctx.pool, copy.id).await?;
    }
    let failed = sqlx::query(
        "SELECT i.source_document_id, j.error->>'code' AS code
         FROM reingest_copy_items i LEFT JOIN ingest_jobs j ON j.id = i.child_job
         WHERE i.copy_id = $1 AND NOT i.removed AND i.kind <> 'missing'
           AND (j.id IS NULL OR j.status <> 'completed')
         ORDER BY i.created_at, i.source_document_id",
    )
    .bind(copy.id)
    .fetch_all(&ctx.pool)
    .await?;
    if failed.is_empty() {
        finish(&ctx.pool, copy.id, "done", None).await?;
        tracing::info!(copy = %copy.id, "re-ingest copy done");
        return Ok(());
    }
    let mut codes: Vec<Option<String>> = Vec::with_capacity(failed.len());
    let mut first: Vec<String> = Vec::new();
    for r in &failed {
        codes.push(r.try_get("code")?);
        if first.len() < 10 {
            first.push(r.try_get::<Uuid, _>("source_document_id")?.to_string());
        }
    }
    let shared = codes
        .first()
        .cloned()
        .flatten()
        .filter(|c| codes.iter().all(|x| x.as_deref() == Some(c.as_str())));
    let code = shared.unwrap_or_else(|| "documents_failed".to_string());
    let error = json!({
        "code": code,
        "message": format!(
            "{} source documents were not copied (their ingest job did not complete), e.g. {}",
            failed.len(),
            first.join(", ")
        ),
    });
    finish(&ctx.pool, copy.id, "failed", Some(error)).await?;
    tracing::warn!(copy = %copy.id, failed = failed.len(), "re-ingest copy failed");
    Ok(())
}
