//! Ingest jobs: create (with its first step), read, list, cancel, retry, delete. Every read and
//! write is scoped by tenant; another tenant's job is `NotFound`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::db::steps::{self, ClaimedStep, StepKind};
use crate::error::{IngestError, Result};
use crate::limits::MAX_CHILD_IDS;

/// `list` returns at most this many jobs, whatever the caller asks for.
pub const MAX_LIST_LIMIT: i64 = 200;

/// Input for `create`. `source` / `options` follow the shapes documented in the migration.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub tenant: Uuid,
    pub store_id: Uuid,
    pub source: Value,
    pub options: Value,
}

/// An `ingest_jobs` row.
#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub tenant: Uuid,
    pub store_id: Uuid,
    pub source: Value,
    pub options: Value,
    pub meta: Value,
    pub status: String,
    pub error: Option<Value>,
    pub document_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A step as the jobs API shows it.
#[derive(Debug, Clone, Serialize)]
pub struct StepView {
    pub kind: String,
    pub key: String,
    pub status: String,
    pub attempt: i32,
    /// What the step recorded (e.g. Deliver's `document_id` and `skipped_duplicate`).
    pub output: Option<Value>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub usage: Option<Value>,
    pub cost_micros: Option<i64>,
    pub error: Option<Value>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// A job and its steps (stage order). Serialises flat: the job's fields plus `steps`; a crawl /
/// sitemap parent also `children` and `child_job_ids`.
#[derive(Debug, Clone, Serialize)]
pub struct JobView {
    #[serde(flatten)]
    pub job: Job,
    pub steps: Vec<StepView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<ChildrenView>,
    /// The first [`MAX_CHILD_IDS`] child job ids, oldest first.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_job_ids: Option<Vec<Uuid>>,
}

/// A parent job's children: how many, and how many per job status (every status, 0 included).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChildrenView {
    pub total: i64,
    pub by_status: BTreeMap<String, i64>,
}

/// Every `ingest_jobs.status`. `cancelling`: cancelled while a step was running; nothing new
/// runs, and it becomes `cancelled` once the running step(s) have ended. `deleting`: a
/// [`delete`] started and has not finished (its blobs may be partly gone); nothing runs, it
/// cannot be retried or cancelled, and only `DELETE` again (which finishes it) changes it.
pub const JOB_STATUSES: [&str; 8] = [
    "queued",
    "running",
    "cancelling",
    "completed",
    "failed",
    "rejected",
    "cancelled",
    "deleting",
];

/// Source kinds whose job is a parent: its Discover step creates url child jobs.
pub const PARENT_KINDS: [&str; 2] = ["crawl", "sitemap"];

/// The `ingest_jobs` columns `job_from_row` reads (a macro so queries stay `&'static str`).
macro_rules! job_columns {
    () => {
        "id, tenant, store_id, source, options, meta, status, error, document_ids, created_at, \
         updated_at"
    };
}

fn job_from_row(r: &PgRow) -> Result<Job> {
    Ok(Job {
        id: r.try_get("id")?,
        tenant: r.try_get("tenant")?,
        store_id: r.try_get("store_id")?,
        source: r.try_get("source")?,
        options: r.try_get("options")?,
        meta: r.try_get("meta")?,
        status: r.try_get("status")?,
        error: r.try_get("error")?,
        document_ids: r.try_get("document_ids")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

/// Creates a `queued` job and its `Acquire` step in one transaction.
pub async fn create(pool: &PgPool, new: NewJob) -> Result<Uuid> {
    create_with_id(pool, Uuid::now_v7(), new).await
}

/// [`create`] with a caller-chosen id. An upload is stored at its object key
/// (`{tenant}/{id}/original`, see `pipeline::original_key`) BEFORE its job exists, so the
/// Acquire step never sees a half-written original; the API therefore picks the id first.
pub async fn create_with_id(pool: &PgPool, id: Uuid, new: NewJob) -> Result<Uuid> {
    create_with_meta(pool, id, new, &Value::Object(Default::default())).await
}

fn check_ids(id: Uuid, new: &NewJob) -> Result<()> {
    if new.tenant.is_nil() {
        return Err(IngestError::InvalidInput("tenant must not be nil".into()));
    }
    if id.is_nil() {
        return Err(IngestError::InvalidInput("job id must not be nil".into()));
    }
    Ok(())
}

async fn insert_job(
    tx: &mut PgConnection,
    id: Uuid,
    new: &NewJob,
    meta: &Value,
    status: &str,
    error: Option<&Value>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingest_jobs (id, tenant, store_id, source, options, meta, status, error)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(id)
    .bind(new.tenant)
    .bind(new.store_id)
    .bind(&new.source)
    .bind(&new.options)
    .bind(meta)
    .bind(status)
    .bind(error)
    .execute(tx)
    .await?;
    Ok(())
}

/// [`create_with_id`] with an initial `meta` (e.g. `{"source_id"}`).
pub async fn create_with_meta(pool: &PgPool, id: Uuid, new: NewJob, meta: &Value) -> Result<Uuid> {
    create_first(pool, id, new, meta, StepKind::Acquire).await
}

/// Creates a `queued` crawl / sitemap parent job and its `Discover` step in one transaction
/// (`pipeline::discover` then creates the url children).
pub async fn create_discovery(pool: &PgPool, id: Uuid, new: NewJob, meta: &Value) -> Result<Uuid> {
    create_first(pool, id, new, meta, StepKind::Discover).await
}

async fn create_first(
    pool: &PgPool,
    id: Uuid,
    new: NewJob,
    meta: &Value,
    first: StepKind,
) -> Result<Uuid> {
    check_ids(id, &new)?;
    let mut tx = pool.begin().await?;
    insert_job(&mut tx, id, &new, meta, "queued", None).await?;
    steps::enqueue(&mut tx, id, first, "", Value::Object(Default::default())).await?;
    tx.commit().await?;
    Ok(id)
}

/// A url child job of a crawl / sitemap parent; `id` is derived from the parent and the page
/// URL (`pipeline::discover::child_id`), so a retried Discover finds the children it made.
#[derive(Debug, Clone)]
pub struct NewChild {
    pub id: Uuid,
    /// `{"kind": "url", "url", "parent_job_id", "prefetched"?}`.
    pub source: Value,
}

/// Creates url children of the step's job (`parent`), in one transaction fenced on the claim
/// (`LeaseLost`, nothing written, when `step` is no longer current): each a `queued` job of the
/// parent's tenant and store with its options and `meta.source_id` / `meta.source_run` (and
/// `topic_keys` / `supersedes` / `provenance`, `crate::doc_fields`),
/// `parent_job_id` set, and an Acquire step, the same shape [`create_with_meta`] gives a url job. A
/// child id that already exists is left as it is (a retried Discover). `record` (`(key, body)`) is
/// written as an artefact of the parent in the same transaction (a crawl's per-page progress). A
/// parent that is no longer queued/running gets no more children (`Conflict`). Returns how many
/// were created.
pub async fn add_children(
    pool: &PgPool,
    step: &ClaimedStep,
    parent: &Job,
    children: &[NewChild],
    record: Option<(&str, &str)>,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR SHARE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    if current.is_none() {
        return Err(IngestError::LeaseLost {
            step: step.id,
            attempt: step.attempt,
        });
    }
    let status: String = sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1")
        .bind(parent.id)
        .fetch_one(&mut *tx)
        .await?;
    if !matches!(status.as_str(), "queued" | "running") {
        return Err(IngestError::Conflict(format!(
            "the parent job is {status}; no more children"
        )));
    }
    let mut meta = serde_json::Map::new();
    let inherited = ["source_id", "source_run"]
        .into_iter()
        .chain(crate::doc_fields::DOC_FIELD_KEYS);
    for key in inherited {
        if let Some(v) = parent.meta.get(key) {
            meta.insert(key.into(), v.clone());
        }
    }
    let meta = Value::Object(meta);
    let mut created = 0u64;
    for c in children {
        if c.id.is_nil() {
            return Err(IngestError::InvalidInput("job id must not be nil".into()));
        }
        // clock_timestamp(): children of one transaction keep their order in `created_at`.
        let n = sqlx::query(
            "INSERT INTO ingest_jobs
               (id, tenant, store_id, source, options, meta, status, parent_job_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, clock_timestamp())
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(c.id)
        .bind(parent.tenant)
        .bind(parent.store_id)
        .bind(&c.source)
        .bind(&parent.options)
        .bind(&meta)
        .bind(parent.id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 1 {
            steps::enqueue(
                &mut tx,
                c.id,
                StepKind::Acquire,
                "",
                Value::Object(Default::default()),
            )
            .await?;
            created += 1;
        }
    }
    if let Some((key, body)) = record {
        sqlx::query(
            "INSERT INTO ingest_artifacts (job_id, key, body) VALUES ($1, $2, $3)
             ON CONFLICT (job_id, key) DO UPDATE SET body = EXCLUDED.body, created_at = now()",
        )
        .bind(parent.id)
        .bind(key)
        .bind(body)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(created)
}

/// The children of the tenant's job `id`: the per-status summary and the first
/// [`MAX_CHILD_IDS`] ids, oldest first.
pub async fn children_of(
    pool: &PgPool,
    tenant: Uuid,
    id: Uuid,
) -> Result<(ChildrenView, Vec<Uuid>)> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, count(*) FROM ingest_jobs
         WHERE parent_job_id = $1 AND tenant = $2 GROUP BY status",
    )
    .bind(id)
    .bind(tenant)
    .fetch_all(pool)
    .await?;
    let mut by_status: BTreeMap<String, i64> =
        JOB_STATUSES.iter().map(|s| (s.to_string(), 0)).collect();
    for (status, n) in rows {
        by_status.insert(status, n);
    }
    let total = by_status.values().sum();
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM ingest_jobs WHERE parent_job_id = $1 AND tenant = $2
         ORDER BY created_at, id LIMIT $3",
    )
    .bind(id)
    .bind(tenant)
    .bind(MAX_CHILD_IDS)
    .fetch_all(pool)
    .await?;
    Ok((ChildrenView { total, by_status }, ids))
}

/// Records a job that ended before it started: `rejected` with `error`, no steps (an upload the
/// storage_service virus scan refused, rulings R25: nothing was stored). Same id rules as
/// [`create_with_id`].
pub async fn create_rejected(
    pool: &PgPool,
    id: Uuid,
    new: NewJob,
    error: &crate::error::StepError,
) -> Result<Uuid> {
    create_ended(
        pool,
        id,
        new,
        &Value::Object(Default::default()),
        "rejected",
        error,
    )
    .await
}

/// A job that ended before any step: `status` (`rejected` | `failed`) with `error` and no steps.
/// A source whose fetch failed is recorded `failed` this way (its credentials are gone, so there
/// is nothing to retry; `retry` answers 409).
pub async fn create_ended(
    pool: &PgPool,
    id: Uuid,
    new: NewJob,
    meta: &Value,
    status: &str,
    error: &crate::error::StepError,
) -> Result<Uuid> {
    check_ids(id, &new)?;
    if !matches!(status, "rejected" | "failed") {
        return Err(IngestError::InvalidInput(format!(
            "a job ends before it starts only as rejected or failed, not {status}"
        )));
    }
    let error =
        serde_json::to_value(error).map_err(|e| IngestError::InvalidInput(e.to_string()))?;
    let mut conn = pool.acquire().await?;
    insert_job(&mut conn, id, &new, meta, status, Some(&error)).await?;
    Ok(id)
}

/// One record of a structured source, ready for its Deliver step.
#[derive(Debug, Clone)]
pub struct RecordDelivery {
    /// The Deliver step's key (and its order).
    pub key: String,
    /// The artefact holding the record's text.
    pub artifact: String,
    pub text: String,
    /// The Deliver step's input (`bundle_key` = `artifact`, `title`, `source`, `metadata`).
    pub input: Value,
}

/// Creates a `queued` structured-source job in one transaction: the job, each record's text as an
/// artefact and one Deliver step per record (no Acquire: the API fetched and split the source
/// while the credentials existed). The job completes when every record is delivered.
pub async fn create_records(
    pool: &PgPool,
    id: Uuid,
    new: NewJob,
    meta: &Value,
    records: &[RecordDelivery],
) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    create_records_in(&mut tx, id, &new, meta, records).await?;
    tx.commit().await?;
    Ok(id)
}

/// [`create_with_meta`] inside the caller's transaction: a `queued` job and its `Acquire` step
/// (a re-ingest copy records the job it creates in the same transaction).
pub(crate) async fn create_in(
    tx: &mut PgConnection,
    id: Uuid,
    new: &NewJob,
    meta: &Value,
) -> Result<()> {
    check_ids(id, new)?;
    insert_job(tx, id, new, meta, "queued", None).await?;
    steps::enqueue(
        tx,
        id,
        StepKind::Acquire,
        "",
        Value::Object(Default::default()),
    )
    .await
}

/// [`create_records`] inside the caller's transaction.
pub(crate) async fn create_records_in(
    tx: &mut PgConnection,
    id: Uuid,
    new: &NewJob,
    meta: &Value,
    records: &[RecordDelivery],
) -> Result<()> {
    check_ids(id, new)?;
    if records.is_empty() {
        return Err(IngestError::InvalidInput(
            "a records job needs at least one record".into(),
        ));
    }
    let keys: Vec<&str> = records.iter().map(|r| r.key.as_str()).collect();
    let artifacts: Vec<&str> = records.iter().map(|r| r.artifact.as_str()).collect();
    let texts: Vec<&str> = records.iter().map(|r| r.text.as_str()).collect();
    let inputs: Vec<String> = records.iter().map(|r| r.input.to_string()).collect();
    insert_job(&mut *tx, id, new, meta, "queued", None).await?;
    sqlx::query(
        "INSERT INTO ingest_artifacts (job_id, key, body)
         SELECT $1, k, b FROM UNNEST($2::text[], $3::text[]) AS t(k, b)",
    )
    .bind(id)
    .bind(&artifacts)
    .bind(&texts)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_steps (job_id, kind, key, stage, status, input)
         SELECT $1, $2, k, $3, 'pending', i::jsonb FROM UNNEST($4::text[], $5::text[]) AS t(k, i)",
    )
    .bind(id)
    .bind(StepKind::Deliver.as_str())
    .bind(StepKind::Deliver.stage())
    .bind(&keys)
    .bind(&inputs)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// The tenant's job, without its steps.
pub async fn job(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<Job> {
    let row = sqlx::query(concat!(
        "SELECT ",
        job_columns!(),
        " FROM ingest_jobs WHERE id = $1 AND tenant = $2"
    ))
    .bind(id)
    .bind(tenant)
    .fetch_optional(pool)
    .await?
    .ok_or(IngestError::NotFound)?;
    job_from_row(&row)
}

/// The tenant's job with its steps.
pub async fn get(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<JobView> {
    let job = job(pool, tenant, id).await?;
    let steps = sqlx::query(
        "SELECT kind, key, status, attempt, output, provider, model, usage, cost_micros, error,
                started_at, finished_at
         FROM ingest_steps WHERE job_id = $1 ORDER BY stage, id",
    )
    .bind(id)
    .fetch_all(pool)
    .await?
    .iter()
    .map(|r| {
        Ok(StepView {
            kind: r.try_get("kind")?,
            key: r.try_get("key")?,
            status: r.try_get("status")?,
            attempt: r.try_get("attempt")?,
            output: r.try_get("output")?,
            provider: r.try_get("provider")?,
            model: r.try_get("model")?,
            usage: r.try_get("usage")?,
            cost_micros: r.try_get("cost_micros")?,
            error: r.try_get("error")?,
            started_at: r.try_get("started_at")?,
            finished_at: r.try_get("finished_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    let is_parent = job
        .source
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|k| PARENT_KINDS.contains(&k));
    let (children, child_job_ids) = if is_parent {
        let (c, ids) = children_of(pool, tenant, id).await?;
        (Some(c), Some(ids))
    } else {
        (None, None)
    };
    Ok(JobView {
        job,
        steps,
        children,
        child_job_ids,
    })
}

/// The tenant's jobs, newest first, optionally filtered by store and status. `limit` is clamped
/// to `1..=MAX_LIST_LIMIT`.
pub async fn list(
    pool: &PgPool,
    tenant: Uuid,
    store_id: Option<Uuid>,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<Job>> {
    Ok(page(pool, tenant, store_id, status, None, limit).await?.0)
}

/// One page of [`list`]: the jobs strictly older than job `before` (by `created_at`, then id,
/// the list order), and the cursor of the next page (the last job's id) when there are more.
/// `before` must be one of the tenant's jobs (`NotFound` otherwise).
pub async fn page(
    pool: &PgPool,
    tenant: Uuid,
    store_id: Option<Uuid>,
    status: Option<&str>,
    before: Option<Uuid>,
    limit: i64,
) -> Result<(Vec<Job>, Option<Uuid>)> {
    let limit = limit.clamp(1, MAX_LIST_LIMIT);
    let cursor: Option<(DateTime<Utc>, Uuid)> = match before {
        None => None,
        Some(b) => Some(
            sqlx::query_as("SELECT created_at, id FROM ingest_jobs WHERE id = $1 AND tenant = $2")
                .bind(b)
                .bind(tenant)
                .fetch_optional(pool)
                .await?
                .ok_or(IngestError::NotFound)?,
        ),
    };
    let mut jobs = sqlx::query(concat!(
        "SELECT ",
        job_columns!(),
        " FROM ingest_jobs
         WHERE tenant = $1 AND ($2::uuid IS NULL OR store_id = $2)
           AND ($3::text IS NULL OR status = $3)
           AND ($5::timestamptz IS NULL OR (created_at, id) < ($5, $6))
         ORDER BY created_at DESC, id DESC
         LIMIT $4"
    ))
    .bind(tenant)
    .bind(store_id)
    .bind(status)
    .bind(limit + 1)
    .bind(cursor.map(|c| c.0))
    .bind(cursor.map(|c| c.1))
    .fetch_all(pool)
    .await?
    .iter()
    .map(job_from_row)
    .collect::<Result<Vec<_>>>()?;
    let more = jobs.len() as i64 > limit;
    jobs.truncate(limit as usize);
    let next = more.then(|| jobs.last().map(|j| j.id)).flatten();
    Ok((jobs, next))
}

/// Locks the job's steps, then the job (the queue's lock order), and returns the job's status.
/// `NotFound` when the job does not exist for this tenant.
async fn lock_job(tx: &mut sqlx::PgConnection, tenant: Uuid, id: Uuid) -> Result<String> {
    let exists: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM ingest_jobs WHERE id = $1 AND tenant = $2")
            .bind(id)
            .bind(tenant)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(IngestError::NotFound);
    }
    sqlx::query("SELECT id FROM ingest_steps WHERE job_id = $1 ORDER BY id FOR UPDATE")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    // A concurrent delete may have removed it while we waited for the step locks.
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    status.ok_or(IngestError::NotFound)
}

/// The status a queued / running job gets when cancelled: `cancelling` while one of its steps is
/// still running (the worker finishes that step and enqueues nothing more; the last one to end
/// turns the job `cancelled`, `steps::settle_cancelling`), else `cancelled` at once.
/// (A macro so queries stay `&'static str`.)
macro_rules! cancelled_or_cancelling {
    () => {
        "CASE WHEN EXISTS (SELECT 1 FROM ingest_steps s
                           WHERE s.job_id = ingest_jobs.id AND s.status = 'running')
              THEN 'cancelling' ELSE 'cancelled' END"
    };
}

/// Cancels a queued or running job. Its pending/failed steps become `cancelled` at once; the job
/// becomes `cancelled`, or `cancelling` while a step is still running (cooperative: the worker
/// finishes that step, records its result and enqueues nothing more; when the last running step
/// has ended the job is `cancelled`). Idempotent: a `cancelling` or `cancelled` job is left as
/// it is (Ok). `Conflict` when the job already ended otherwise (completed, failed, rejected).
///
/// A crawl / sitemap parent's queued and running children (of the same tenant) are cancelled the
/// same way in the same transaction; children that already ended keep their status. A parent
/// that is no longer queued/running gets no new children ([`add_children`]). A parent that has
/// already ended (it completes when its Discover step does, while its children still run) is
/// left as it is, but its open children are cancelled: Ok when there were any, else `Conflict`.
pub async fn cancel(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    let status = lock_job(&mut tx, tenant, id).await?;
    match status.as_str() {
        "queued" | "running" => {}
        "cancelling" | "cancelled" => {
            // Idempotent; children the first cancel left open cannot exist (it cancelled them
            // and a cancelled parent gets no new ones), but a re-cancel is cheap insurance.
            cancel_children(&mut tx, tenant, id).await?;
            tx.commit().await?;
            return Ok(());
        }
        "deleting" => {
            return Err(IngestError::Conflict(
                "job is being deleted; DELETE it again to finish".into(),
            ));
        }
        _ => {
            if cancel_children(&mut tx, tenant, id).await? > 0 {
                tx.commit().await?;
                return Ok(());
            }
            return Err(IngestError::Conflict(format!(
                "job is already {status}; only a queued or running job can be cancelled"
            )));
        }
    }
    sqlx::query(
        "UPDATE ingest_steps SET status = 'cancelled', lease_until = NULL, finished_at = now()
         WHERE job_id = $1 AND status IN ('pending','failed')",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(concat!(
        "UPDATE ingest_jobs SET status = ",
        cancelled_or_cancelling!(),
        ", updated_at = now() WHERE id = $1"
    ))
    .bind(id)
    .execute(&mut *tx)
    .await?;
    cancel_children(&mut tx, tenant, id).await?;
    tx.commit().await?;
    Ok(())
}

/// Cancels the tenant's queued / running children of `parent` (part of [`cancel`]), each
/// `cancelled` or `cancelling` like the parent. Lock order as everywhere: the children's step
/// rows first (in id order), then their job rows (in id order). Returns how many it cancelled.
async fn cancel_children(tx: &mut PgConnection, tenant: Uuid, parent: Uuid) -> Result<u64> {
    sqlx::query(
        "SELECT s.id FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id
         WHERE j.parent_job_id = $1 AND j.tenant = $2 AND j.status IN ('queued','running')
         ORDER BY s.id FOR UPDATE OF s",
    )
    .bind(parent)
    .bind(tenant)
    .execute(&mut *tx)
    .await?;
    let children: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM ingest_jobs
         WHERE parent_job_id = $1 AND tenant = $2 AND status IN ('queued','running')
         ORDER BY id FOR UPDATE",
    )
    .bind(parent)
    .bind(tenant)
    .fetch_all(&mut *tx)
    .await?;
    if children.is_empty() {
        return Ok(0);
    }
    sqlx::query(
        "UPDATE ingest_steps SET status = 'cancelled', lease_until = NULL, finished_at = now()
         WHERE job_id = ANY($1) AND status IN ('pending','failed')",
    )
    .bind(children.as_slice())
    .execute(&mut *tx)
    .await?;
    let n = sqlx::query(concat!(
        "UPDATE ingest_jobs SET status = ",
        cancelled_or_cancelling!(),
        ", updated_at = now() WHERE id = ANY($1) AND status IN ('queued','running')"
    ))
    .bind(children.as_slice())
    .execute(&mut *tx)
    .await?
    .rows_affected();
    Ok(n)
}

/// How many blobs [`delete`] removes from storage_service at once.
pub const DELETE_CONCURRENCY: usize = 8;
/// Longest a single blob delete may take inside [`delete`] (no database transaction is open
/// meanwhile; the job is marked `deleting`).
pub const DELETE_OBJECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// What [`delete`] removed.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Deleted {
    pub job_id: Uuid,
    /// The status the job had before the delete started (never `deleting`, unless the row was
    /// marked by hand without `deleting_from`).
    pub status: String,
    /// Job rows removed: the job and, for a crawl / sitemap parent, its child jobs.
    pub deleted_jobs: u64,
    /// Blobs removed from storage_service (the originals and every recorded asset of those jobs;
    /// an object already gone counts too).
    pub deleted_objects: u64,
    /// The strata documents those jobs delivered. strata-ingest does NOT delete them: they stay
    /// indexed in strata_rag; remove them there if wanted.
    pub document_ids: Vec<Uuid>,
}

/// What phase 1 of [`delete`] ([`begin_delete`]) marked and found; [`finish_delete`] runs
/// phases 2 and 3 on it.
#[derive(Debug, Clone)]
pub struct DeletePlan {
    /// The status the job had before it was marked `deleting`.
    status: String,
    /// The job and its children.
    ids: Vec<Uuid>,
    /// Every staged blob of those jobs.
    keys: Vec<String>,
    document_ids: Vec<Uuid>,
}

/// While [`finish_delete`] works, it refreshes the job's `deleting_since` this often, so the
/// sweeper ([`sweep_deleting`], threshold [`DELETING_STALE_AFTER`]) leaves a live delete alone.
pub const DELETE_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(30);
/// A `deleting` job whose `deleting_since` is older than this has nobody working on it (the pod
/// died, the blob deletes failed): the sweeper resumes it.
pub const DELETING_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(120);
/// Most stale `deleting` jobs one sweeper pass takes on.
pub const SWEEP_BATCH: i64 = 20;

/// Deletes the tenant's ended job (`completed`, `failed`, `rejected` or `cancelled`): its
/// staged blobs in storage_service (the original at `{tenant}/{job}/original` and every
/// `ingest_assets` object: figures, page images, audio segments), then its rows (steps, assets,
/// textual artefacts) and, for a crawl / sitemap parent, its child jobs with theirs. Documents
/// already delivered to strata are left alone (listed in [`Deleted::document_ids`]); billing
/// rows (`metering_outbox`) are kept.
///
/// Three phases, so no row lock is held while storage_service is called:
/// 1. [`begin_delete`]: one short transaction locks and validates, then marks the job and its
///    children `deleting` (the status they had is kept in `deleting_from`, the time in
///    `deleting_since`). `Conflict`, nothing changed, when the job is `queued` / `running`
///    (cancel it first), still `cancelling`, has a step still running (a failed job's parallel
///    step), or has a child that is not ended or still runs a step. A crawl / sitemap child job
///    is never deleted alone: `ChildOfParentJob` (delete its parent, which cascades). A
///    `deleting` job is never claimed, retried or cancelled (`Conflict`).
/// 2. [`finish_delete`]: the blobs are deleted, outside any transaction (`deleting_since` is
///    refreshed every [`DELETE_HEARTBEAT`] meanwhile). A blob that cannot be deleted →
///    `Upstream`; the job stays `deleting` (some blobs may already be gone, so it can no longer
///    be retried); deleting it again, or the sweeper, resumes here (a missing blob counts as
///    deleted).
/// 3. a second transaction deletes the rows (children cascade). `NotFound` if a concurrent
///    delete of the same job (another request, the sweeper) finished first.
///
/// The API runs phases 2 and 3 in a task of their own, so a dropped request does not stop them.
pub async fn delete(
    pool: &PgPool,
    storage: &crate::infra::storage::StorageClient,
    tenant: Uuid,
    id: Uuid,
) -> Result<Deleted> {
    let plan = begin_delete(pool, tenant, id).await?;
    finish_delete(pool, storage, tenant, id, plan).await
}

/// Phase 1 of [`delete`]: lock, validate, mark `deleting`, collect the blob keys. Once this
/// returns, the job is `deleting` until [`finish_delete`] (or the sweeper) removes it.
pub async fn begin_delete(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<DeletePlan> {
    mark_deleting(pool, tenant, id).await
}

/// Phases 2 and 3 of [`delete`] for a `plan` from [`begin_delete`].
pub async fn finish_delete(
    pool: &PgPool,
    storage: &crate::infra::storage::StorageClient,
    tenant: Uuid,
    id: Uuid,
    plan: DeletePlan,
) -> Result<Deleted> {
    let objects = {
        let blobs = delete_blobs(storage, id, &plan.keys);
        tokio::pin!(blobs);
        let start = tokio::time::Instant::now() + DELETE_HEARTBEAT;
        let mut beat = tokio::time::interval_at(start, DELETE_HEARTBEAT);
        loop {
            tokio::select! {
                r = &mut blobs => break r?,
                _ = beat.tick() => {
                    // Still working: keep the sweeper away. Best effort; a failed refresh at
                    // worst lets the sweeper run the same (idempotent) delete alongside.
                    if let Err(e) = sqlx::query(
                        "UPDATE ingest_jobs SET deleting_since = now()
                         WHERE id = ANY($1) AND status = 'deleting'",
                    )
                    .bind(plan.ids.as_slice())
                    .execute(pool)
                    .await
                    {
                        tracing::warn!(job = %id, error = %e, "refreshing deleting_since failed");
                    }
                }
            }
        }
    };
    let deleted_jobs = remove_rows(pool, tenant, id, &plan.ids).await?;
    tracing::info!(job = %id, deleted_jobs, objects, "job deleted");
    Ok(Deleted {
        job_id: id,
        status: plan.status,
        deleted_jobs,
        deleted_objects: objects,
        document_ids: plan.document_ids,
    })
}

/// One sweeper pass: resumes (to the end) every top-level job that has been `deleting` for
/// longer than `stale_after` (nobody is working on it: the pod died mid-delete, or its blob
/// deletes failed), at most [`SWEEP_BATCH`]. Safe with several pods and a concurrent `DELETE`:
/// a job is claimed by moving its `deleting_since` to now in one `UPDATE … FOR UPDATE SKIP
/// LOCKED`, so another sweeper skips it for `stale_after`; a job a concurrent delete finished
/// first (`NotFound`) is not counted. Returns how many jobs this pass deleted.
pub async fn sweep_deleting(
    pool: &PgPool,
    storage: &crate::infra::storage::StorageClient,
    stale_after: std::time::Duration,
) -> Result<u64> {
    let claimed: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "UPDATE ingest_jobs SET deleting_since = now()
         WHERE id IN (
           SELECT id FROM ingest_jobs
           WHERE status = 'deleting' AND parent_job_id IS NULL
             AND coalesce(deleting_since, '-infinity') < now() - make_interval(secs => $1)
           ORDER BY deleting_since NULLS FIRST
           LIMIT $2
           FOR UPDATE SKIP LOCKED)
         RETURNING id, tenant",
    )
    .bind(stale_after.as_secs_f64())
    .bind(SWEEP_BATCH)
    .fetch_all(pool)
    .await?;
    let mut done = 0;
    for (id, tenant) in claimed {
        match delete(pool, storage, tenant, id).await {
            Ok(d) => {
                tracing::info!(job = %id, deleted_jobs = d.deleted_jobs, "sweeper finished a stale delete");
                done += 1;
            }
            // A concurrent DELETE (or another sweeper) finished it first.
            Err(IngestError::NotFound) => {}
            Err(e) => {
                tracing::warn!(job = %id, error = %e, "sweeper could not finish a stale delete; it stays `deleting`");
            }
        }
    }
    Ok(done)
}

/// Phase 1 of [`delete`]: lock, validate, mark `deleting`, collect the blob keys.
async fn mark_deleting(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<DeletePlan> {
    let mut tx = pool.begin().await?;
    let status = lock_job(&mut tx, tenant, id).await?;
    // A crawl / sitemap child goes only with its parent: deleting it alone would drop it from
    // the parent's `child_job_ids`, and a source run would count fewer documents than it ingested.
    let parent: Option<Uuid> =
        sqlx::query_scalar("SELECT parent_job_id FROM ingest_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    if let Some(parent_job_id) = parent {
        return Err(IngestError::ChildOfParentJob { parent_job_id });
    }
    match status.as_str() {
        // `deleting`: an earlier delete stopped after phase 1 (or 2); this one resumes it.
        "completed" | "failed" | "rejected" | "cancelled" | "deleting" => {}
        "cancelling" => {
            return Err(IngestError::Conflict(
                "job is cancelling: a step is still running; delete it once it is cancelled".into(),
            ));
        }
        _ => {
            return Err(IngestError::Conflict(format!(
                "job is {status}; cancel it first (POST /api/ingest/jobs/{id}/cancel)"
            )));
        }
    }
    // Children: step rows first, then job rows (the queue's lock order).
    sqlx::query(
        "SELECT s.id FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id
         WHERE j.parent_job_id = $1 AND j.tenant = $2 ORDER BY s.id FOR UPDATE OF s",
    )
    .bind(id)
    .bind(tenant)
    .execute(&mut *tx)
    .await?;
    let children: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, status FROM ingest_jobs WHERE parent_job_id = $1 AND tenant = $2
         ORDER BY id FOR UPDATE",
    )
    .bind(id)
    .bind(tenant)
    .fetch_all(&mut *tx)
    .await?;
    refuse_foreign_children(&mut tx, tenant, id).await?;
    let open = children
        .iter()
        .filter(|(_, st)| matches!(st.as_str(), "queued" | "running" | "cancelling"))
        .count();
    if open > 0 {
        return Err(IngestError::Conflict(format!(
            "{open} child job(s) are still queued, running or cancelling; cancel them first \
             (POST /api/ingest/jobs/{id}/cancel cancels a parent's open children)"
        )));
    }
    let mut ids: Vec<Uuid> = vec![id];
    ids.extend(children.iter().map(|(c, _)| *c));
    let running: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_steps WHERE job_id = ANY($1) AND status = 'running'",
    )
    .bind(ids.as_slice())
    .fetch_one(&mut *tx)
    .await?;
    if running > 0 {
        return Err(IngestError::Conflict(format!(
            "{running} step(s) of the job are still running; delete it once they have ended"
        )));
    }
    // Not claimable (the queue claims only queued/running jobs' steps), not retryable, not
    // cancellable. `deleting_from` keeps the status it had (a resumed delete keeps the first).
    sqlx::query(
        "UPDATE ingest_jobs
         SET deleting_from = CASE WHEN status = 'deleting' THEN deleting_from ELSE status END,
             status = 'deleting', deleting_since = now(), updated_at = now()
         WHERE id = ANY($1)",
    )
    .bind(ids.as_slice())
    .execute(&mut *tx)
    .await?;
    let from: Option<String> =
        sqlx::query_scalar("SELECT deleting_from FROM ingest_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let mut keys: std::collections::BTreeSet<String> =
        sqlx::query_scalar("SELECT object_key FROM ingest_assets WHERE job_id = ANY($1)")
            .bind(ids.as_slice())
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
    keys.extend(
        ids.iter()
            .map(|j| crate::pipeline::original_key(tenant, *j)),
    );
    let document_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT d FROM ingest_jobs, unnest(document_ids) AS d
         WHERE id = ANY($1) ORDER BY d",
    )
    .bind(ids.as_slice())
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(DeletePlan {
        status: from.unwrap_or(status),
        ids,
        keys: keys.into_iter().collect(),
        document_ids,
    })
}

/// Children are always of the parent's tenant (`add_children`); the cascade must never reach
/// another tenant's rows, so a violated invariant refuses instead.
async fn refuse_foreign_children(tx: &mut PgConnection, tenant: Uuid, id: Uuid) -> Result<()> {
    let foreign: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_jobs WHERE parent_job_id = $1 AND tenant <> $2",
    )
    .bind(id)
    .bind(tenant)
    .fetch_one(&mut *tx)
    .await?;
    if foreign > 0 {
        return Err(IngestError::Conflict(
            "job has child jobs that cannot be deleted with it".into(),
        ));
    }
    Ok(())
}

/// Phase 2 of [`delete`]: removes `keys` from storage_service, [`DELETE_CONCURRENCY`] at once,
/// no transaction open. Returns how many were removed (all of them), or `Upstream`.
async fn delete_blobs(
    storage: &crate::infra::storage::StorageClient,
    id: Uuid,
    keys: &[String],
) -> Result<u64> {
    use futures::StreamExt;

    let results: Vec<(String, std::result::Result<(), String>)> =
        futures::stream::iter(keys.iter().cloned())
            .map(|key| async move {
                let r =
                    match tokio::time::timeout(DELETE_OBJECT_TIMEOUT, storage.delete(&key)).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(_) => Err(format!(
                            "timed out after {} s",
                            DELETE_OBJECT_TIMEOUT.as_secs()
                        )),
                    };
                (key, r)
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .collect()
            .await;
    let failed: Vec<&(String, std::result::Result<(), String>)> =
        results.iter().filter(|(_, r)| r.is_err()).collect();
    if let Some((key, Err(e))) = failed.first() {
        tracing::warn!(job = %id, failed = failed.len(), key = %key, error = %e, "deleting job blobs failed");
        return Err(IngestError::Upstream {
            service: "storage",
            message: format!(
                "{} of {} staged objects could not be deleted (first: {key}: {e}); the job \
                 stays `deleting` (it can no longer be retried): DELETE it again",
                failed.len(),
                results.len()
            ),
        });
    }
    Ok(results.len() as u64)
}

/// Phase 3 of [`delete`]: deletes the job row (children, steps, assets and artefacts go with it,
/// ON DELETE CASCADE). Returns how many job rows went.
async fn remove_rows(pool: &PgPool, tenant: Uuid, id: Uuid, ids: &[Uuid]) -> Result<u64> {
    let mut tx = pool.begin().await?;
    // `NotFound` when a concurrent delete of the same job removed it first.
    let status = lock_job(&mut tx, tenant, id).await?;
    if status != "deleting" {
        return Err(IngestError::Conflict(format!(
            "job is {status}, not deleting; delete it again"
        )));
    }
    refuse_foreign_children(&mut tx, tenant, id).await?;
    let deleted_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_jobs WHERE id = ANY($1) OR parent_job_id = $2",
    )
    .bind(ids)
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(deleted_jobs as u64)
}

/// Retries a failed job: its failed steps go back to `pending` with a fresh attempt budget
/// (`attempt_base = attempt`, refund counters cleared; `attempt` itself is the fence token and
/// keeps growing), the job to
/// `running` with its error cleared. Completed and skipped steps are not re-run. `Conflict`
/// unless the job is `failed` (and has a failed step to retry).
pub async fn retry(pool: &PgPool, tenant: Uuid, id: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    let status = lock_job(&mut tx, tenant, id).await?;
    if status == "deleting" {
        return Err(IngestError::Conflict(
            "job is being deleted (its staged originals may be gone) and cannot be retried; \
             DELETE it again to finish"
                .into(),
        ));
    }
    if status != "failed" {
        return Err(IngestError::Conflict(format!(
            "only a failed job can be retried; job is {status}"
        )));
    }
    let reset = sqlx::query(
        "UPDATE ingest_steps
         SET status = 'pending', attempt_base = attempt, releases = 0, slot_waits = 0,
             run_after = now(), lease_until = NULL,
             error = NULL, started_at = NULL, finished_at = NULL
         WHERE job_id = $1 AND status = 'failed'",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if reset == 0 {
        return Err(IngestError::Conflict(
            "job has no failed step to retry".into(),
        ));
    }
    sqlx::query(
        "UPDATE ingest_jobs SET status = 'running', error = NULL, updated_at = now() WHERE id = $1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
