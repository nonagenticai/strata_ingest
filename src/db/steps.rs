//! The persisted step queue. Workers `claim_next` a pending step (SKIP LOCKED, stage-gated per
//! job), hold a lease they `heartbeat`, then `complete_and_enqueue` or `fail` it. An expired lease
//! (a crashed worker) is reaped on the next claim: back to `pending` after a backoff, or `failed`
//! once its attempt budget is used up ([`budget_exhausted`]). Every write by a worker is fenced
//! on its claim (`id` + `attempt`), so a worker that lost its lease cannot overwrite the current
//! run; `attempt` therefore only ever grows (never reset or decremented), and runs that should
//! not count are refunded through `releases` / `slot_waits` / `attempt_base` instead.
//!
//! A Caption step is optional enrichment (rulings R40): when its budget runs out on a Retryable
//! failure (a step / slot-wait timeout, a panic) or an expired lease, it ends `skipped` with
//! `{"skipped": "caption_failed", "asset_id", "error"}` and the job goes on.
//!
//! Lock order, everywhere: step rows first, then the job row. That keeps claim, complete, fail,
//! cancel, retry and delete free of deadlocks against each other.
//!
//! Cooperative cancellation: a job cancelled while a step runs is `cancelling`. Nothing is
//! claimed or enqueued for it; every path that ends a running step (complete, fail, release,
//! slot-wait requeue, the expired-lease reaper) ends it `cancelled` or `completed` as usual and
//! then [`settle_cancelling`]: the last running step to end turns the job `cancelled`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::{IngestError, Result, StepError};
use crate::limits::{
    BACKOFF_BASE_S, BACKOFF_MAX_S, LEASE_S, MAX_RELEASE_REFUNDS, MAX_SLOT_WAIT_REQUEUES,
};
use crate::providers::ErrorClass;

/// A pipeline step. Steps of a job run in `stage` order; steps sharing a stage run in parallel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StepKind {
    /// A crawl / sitemap parent job's only step: finds the pages and creates one url child job
    /// per page (`pipeline::discover`).
    Discover,
    Acquire,
    Scan,
    Detect,
    Convert,
    OcrPages,
    Caption,
    TranscribeSegment,
    Bundle,
    Deliver,
}

impl StepKind {
    pub const ALL: [StepKind; 10] = [
        StepKind::Discover,
        StepKind::Acquire,
        StepKind::Scan,
        StepKind::Detect,
        StepKind::Convert,
        StepKind::OcrPages,
        StepKind::Caption,
        StepKind::TranscribeSegment,
        StepKind::Bundle,
        StepKind::Deliver,
    ];

    /// The value stored in `ingest_steps.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            StepKind::Discover => "discover",
            StepKind::Acquire => "acquire",
            StepKind::Scan => "scan",
            StepKind::Detect => "detect",
            StepKind::Convert => "convert",
            StepKind::OcrPages => "ocr_pages",
            StepKind::Caption => "caption",
            StepKind::TranscribeSegment => "transcribe_segment",
            StepKind::Bundle => "bundle",
            StepKind::Deliver => "deliver",
        }
    }

    pub fn parse(s: &str) -> Option<StepKind> {
        StepKind::ALL.into_iter().find(|k| k.as_str() == s)
    }

    /// Stage gate: a step is claimable only once every lower-stage step of its job is done.
    pub fn stage(self) -> i32 {
        match self {
            StepKind::Discover => 5,
            StepKind::Acquire => 10,
            StepKind::Scan => 20,
            StepKind::Detect => 30,
            StepKind::Convert => 40,
            StepKind::OcrPages | StepKind::Caption | StepKind::TranscribeSegment => 50,
            StepKind::Bundle => 60,
            StepKind::Deliver => 70,
        }
    }
}

/// A step a worker now holds a lease on. `attempt` already counts this run (first run = 1).
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedStep {
    pub id: i64,
    pub job_id: Uuid,
    pub tenant: Uuid,
    pub kind: StepKind,
    pub key: String,
    pub attempt: i32,
    pub input: Value,
}

/// What a finished step records. `skipped` stores the step as `skipped` instead of `completed`
/// (both open the stage gate).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StepOutput {
    pub output: Value,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub usage: Option<Value>,
    pub cost_micros: Option<i64>,
    pub skipped: bool,
    /// A paid provider call of this attempt whose own record failed (`metering::record_with_retry`):
    /// written to `metering_outbox` inside the completion transaction (idempotent on `event_key`).
    pub unrecorded: Option<crate::metering::Meter>,
}

/// The attempt counters of a step row (`ingest_steps`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Budget {
    /// The run number; the fence token. Never decreases.
    pub attempt: i32,
    /// Runs before the last manual retry (`jobs::retry`); they no longer count.
    pub attempt_base: i32,
    /// Runs given back on shutdown; at most [`MAX_RELEASE_REFUNDS`] of them are refunded.
    pub releases: i32,
    /// Runs abandoned with `slot_wait_timeout` and re-queued (never counted).
    pub slot_waits: i32,
}

impl Budget {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Budget> {
        Ok(Budget {
            attempt: row.try_get("attempt")?,
            attempt_base: row.try_get("attempt_base")?,
            releases: row.try_get("releases")?,
            slot_waits: row.try_get("slot_waits")?,
        })
    }

    /// Runs that count against `max_attempts`.
    pub fn used(&self) -> i32 {
        self.attempt
            - self.attempt_base
            - self.releases.clamp(0, MAX_RELEASE_REFUNDS)
            - self.slot_waits.max(0)
    }
}

/// Whether a step whose latest run had `budget` may not run again under `max_attempts`.
pub fn budget_exhausted(budget: Budget, max_attempts: i32) -> bool {
    budget.used() >= max_attempts
}

/// Retry delay after failed attempt `attempt` (1-based): `min(2^(attempt-1) · 2 s, 300 s)`.
/// Saturates instead of overflowing; `attempt < 1` is treated as 1.
pub fn backoff(attempt: i32) -> Duration {
    let exp = u32::try_from(attempt.max(1) - 1).unwrap_or(0);
    let secs = 1u64
        .checked_shl(exp)
        .and_then(|m| m.checked_mul(BACKOFF_BASE_S))
        .map_or(BACKOFF_MAX_S, |s| s.min(BACKOFF_MAX_S));
    Duration::from_secs(secs)
}

/// Delay before retrying after failed attempt `attempt`: [`backoff`], or the server's
/// `Retry-After` when that is longer (capped at `BACKOFF_MAX_S`, so a hostile or broken header
/// cannot park a step for hours). Never shorter than the backoff: a 429 storm that answers
/// `Retry-After: 1` must not burn the attempt budget in seconds.
pub fn retry_delay(attempt: i32, retry_after_s: Option<u64>) -> Duration {
    let b = backoff(attempt);
    match retry_after_s {
        Some(s) => b.max(Duration::from_secs(s.min(BACKOFF_MAX_S))),
        None => b,
    }
}

/// Adds a pending step to a job, inside the caller's transaction. Idempotent: a step with the
/// same `(job, kind, key)` already present is left as it is.
pub async fn enqueue(
    tx: &mut PgConnection,
    job_id: Uuid,
    kind: StepKind,
    key: &str,
    input: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingest_steps (job_id, kind, key, stage, status, input)
         VALUES ($1, $2, $3, $4, 'pending', $5)
         ON CONFLICT (job_id, kind, key) DO NOTHING",
    )
    .bind(job_id)
    .bind(kind.as_str())
    .bind(key)
    .bind(kind.stage())
    .bind(input)
    .execute(tx)
    .await?;
    Ok(())
}

/// Handles expired leases (crashed, OOM-killed or stalled workers). Each expired step:
/// - of a cancelling/cancelled/rejected/completed job → `cancelled` (a cancelling job then
///   settles, [`settle_cancelling`]);
/// - that has used `max_attempts` attempts → `failed`, and its queued/running job `failed` with
///   code `lease_expired` (a document that kills its worker every time stops here);
/// - otherwise → `pending` after `backoff(attempt)`, like a retryable failure.
///
/// Safe to run from many workers at once: steps another reaper or a claimer holds are skipped,
/// and job rows are locked in id order after the step rows (the queue's lock order).
async fn reap_expired(pool: &PgPool, max_attempts: i32) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT s.id, s.job_id, s.kind, s.attempt, s.attempt_base, s.releases, s.slot_waits,
                j.status AS job_status
         FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id
         WHERE s.status = 'running' AND s.lease_until < now()
         ORDER BY s.id
         FOR UPDATE OF s SKIP LOCKED",
    )
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }
    let mut failed_jobs: BTreeMap<Uuid, Value> = BTreeMap::new();
    let mut skipped_jobs: std::collections::BTreeSet<Uuid> = Default::default();
    let mut cancelling_jobs: std::collections::BTreeSet<Uuid> = Default::default();
    for row in &rows {
        let id: i64 = row.try_get("id")?;
        let job_id: Uuid = row.try_get("job_id")?;
        let kind: String = row.try_get("kind")?;
        let budget = Budget::from_row(row)?;
        let attempt = budget.attempt;
        let job_status: String = row.try_get("job_status")?;
        if matches!(
            job_status.as_str(),
            "cancelling" | "cancelled" | "rejected" | "completed"
        ) {
            sqlx::query(
                "UPDATE ingest_steps SET status = 'cancelled', lease_until = NULL,
                        finished_at = now()
                 WHERE id = $1",
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
            if job_status == "cancelling" {
                cancelling_jobs.insert(job_id);
            }
        } else if budget_exhausted(budget, max_attempts) {
            let err = StepError {
                class: ErrorClass::Retryable,
                code: "lease_expired".to_string(),
                message: format!(
                    "step {kind} lost its lease on all {attempt} attempts \
                     (worker crashed or stalled); attempts exhausted"
                ),
                provider: None,
                retry_after_s: None,
            };
            let error = to_json(&err)?;
            if kind == StepKind::Caption.as_str() {
                skip_caption(&mut tx, id, &error).await?;
                skipped_jobs.insert(job_id);
                tracing::warn!(
                    step = id,
                    attempt,
                    "caption lease expired on the last attempt; skipped"
                );
                continue;
            }
            sqlx::query(
                "UPDATE ingest_steps SET status = 'failed', lease_until = NULL, error = $2,
                        finished_at = now()
                 WHERE id = $1",
            )
            .bind(id)
            .bind(&error)
            .execute(&mut *tx)
            .await?;
            failed_jobs.entry(job_id).or_insert(error);
            tracing::warn!(
                step = id,
                kind,
                attempt,
                "lease expired on the last attempt; failing"
            );
        } else {
            sqlx::query(
                "UPDATE ingest_steps SET status = 'pending', lease_until = NULL,
                        run_after = now() + make_interval(secs => $2)
                 WHERE id = $1",
            )
            .bind(id)
            .bind(backoff(attempt).as_secs_f64())
            .execute(&mut *tx)
            .await?;
            tracing::warn!(
                step = id,
                kind,
                attempt,
                "reclaimed step with an expired lease"
            );
        }
    }
    for (job_id, error) in failed_jobs {
        sqlx::query(
            "UPDATE ingest_jobs SET status = 'failed', error = $2, updated_at = now()
             WHERE id = $1 AND status IN ('queued','running')",
        )
        .bind(job_id)
        .bind(&error)
        .execute(&mut *tx)
        .await?;
    }
    for job_id in skipped_jobs {
        settle_job(&mut tx, job_id).await?;
    }
    for job_id in cancelling_jobs {
        settle_cancelling(&mut tx, job_id).await?;
    }
    tx.commit().await?;
    Ok(rows.len() as u64)
}

/// Ends a Caption step `skipped` because its budget ran out on `error` (rulings R40): the figure
/// keeps its bare link and the job goes on. The caller holds the step row and settles the job.
async fn skip_caption(tx: &mut PgConnection, id: i64, error: &Value) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_steps
         SET status = 'skipped', lease_until = NULL, error = NULL, finished_at = now(),
             output = jsonb_build_object('skipped', 'caption_failed',
                                         'asset_id', input->'asset_id', 'error', $2::jsonb)
         WHERE id = $1",
    )
    .bind(id)
    .bind(error)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// A queued/running job with no step left pending or running becomes `completed` (locks the job
/// row; call after the step rows).
async fn settle_job(tx: &mut PgConnection, job_id: Uuid) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_jobs j SET status = 'completed', updated_at = now()
         WHERE j.id = $1 AND j.status IN ('queued','running')
           AND NOT EXISTS (SELECT 1 FROM ingest_steps s
                           WHERE s.job_id = j.id AND s.status IN ('pending','running'))",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// A `cancelling` job with no step left running becomes `cancelled` (locks the job row; call
/// after the step rows). A no-op for any other status.
pub(crate) async fn settle_cancelling(tx: &mut PgConnection, job_id: Uuid) -> Result<()> {
    sqlx::query(
        "UPDATE ingest_jobs j SET status = 'cancelled', updated_at = now()
         WHERE j.id = $1 AND j.status = 'cancelling'
           AND NOT EXISTS (SELECT 1 FROM ingest_steps s
                           WHERE s.job_id = j.id AND s.status = 'running')",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// Claims a runnable step: pending, due, of a queued/running job, and with every lower-stage
/// step of its job completed or skipped. Among those, a step of the job with the fewest running
/// steps wins, then the oldest (`run_after`, `id`): jobs take turns instead of FIFO across all
/// jobs. Leases it for `LEASE_S`, bumps `attempt`,
/// and marks the job `running`. `None` when nothing is runnable. Expired leases are reaped first
/// (see `reap_expired`); `max_attempts` is `Limits.max_attempts`.
pub async fn claim_next(pool: &PgPool, max_attempts: i32) -> Result<Option<ClaimedStep>> {
    reap_expired(pool, max_attempts).await?;
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "WITH next AS (
           SELECT s.id FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id
           WHERE s.status = 'pending' AND s.run_after <= now() AND j.status IN ('queued','running')
             AND NOT EXISTS (SELECT 1 FROM ingest_steps p
                             WHERE p.job_id = s.job_id AND p.stage < s.stage
                               AND p.status NOT IN ('completed','skipped'))
           -- Fairness (rulings R36): the job with the fewest running steps first, so one big
           -- document's backlog (250 OcrPages steps) cannot starve every other tenant's job.
           ORDER BY (SELECT count(*) FROM ingest_steps r
                     WHERE r.job_id = s.job_id AND r.status = 'running'),
                    s.run_after, s.id
           FOR UPDATE OF s SKIP LOCKED
           LIMIT 1)
         UPDATE ingest_steps s
         SET status = 'running', attempt = s.attempt + 1, started_at = now(),
             finished_at = NULL, lease_until = now() + make_interval(secs => $1)
         FROM next, ingest_jobs j
         WHERE s.id = next.id AND j.id = s.job_id AND s.status = 'pending'
         RETURNING s.id, s.job_id, j.tenant, s.kind, s.key, s.attempt, s.input",
    )
    .bind(LEASE_S as f64)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        tx.rollback().await?;
        return Ok(None);
    };
    let kind_text: String = row.try_get("kind")?;
    let step = ClaimedStep {
        id: row.try_get("id")?,
        job_id: row.try_get("job_id")?,
        tenant: row.try_get("tenant")?,
        kind: parse_kind(&kind_text)?,
        key: row.try_get("key")?,
        attempt: row.try_get("attempt")?,
        input: row.try_get("input")?,
    };
    sqlx::query(
        "UPDATE ingest_jobs SET status = 'running', updated_at = now()
         WHERE id = $1 AND status = 'queued'",
    )
    .bind(step.job_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(step))
}

/// Extends the lease of a step this worker still holds by `LEASE_S`. `LeaseLost` when the claim
/// is no longer current (the step finished, or was reaped and re-claimed): the worker must stop.
pub async fn heartbeat(pool: &PgPool, step: &ClaimedStep) -> Result<()> {
    let n = sqlx::query(
        "UPDATE ingest_steps SET lease_until = now() + make_interval(secs => $3)
         WHERE id = $1 AND attempt = $2 AND status = 'running'",
    )
    .bind(step.id)
    .bind(step.attempt)
    .bind(LEASE_S as f64)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 0 {
        return Err(lease_lost(step));
    }
    Ok(())
}

/// Records a running step's result and enqueues its follow-up steps, in one transaction. When no
/// step of the job is left pending or running, a `queued`/`running` job becomes `completed`
/// (rulings R2: a job already ended otherwise keeps its status). Follow-ups are not enqueued for
/// a cancelling, cancelled, rejected or completed job; a failed job keeps them so `retry` can
/// resume. The last running step of a `cancelling` job turns it `cancelled`.
/// Fenced on the claim: `LeaseLost` (and nothing written) when `step` is no longer current.
///
/// Billing: a provider call's cost event was recorded when the call returned (`metering::record`),
/// whatever happens to this write. Only a call whose record failed (`out.unrecorded`) is written
/// here, in this transaction, after the fence check.
pub async fn complete_and_enqueue(
    pool: &PgPool,
    step: &ClaimedStep,
    out: StepOutput,
    next: &[(StepKind, String, Value)],
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let job_id: Option<Uuid> = sqlx::query_scalar(
        "UPDATE ingest_steps
         SET status = CASE WHEN $2 THEN 'skipped' ELSE 'completed' END,
             output = $3, provider = $4, model = $5, usage = $6, cost_micros = $7,
             error = NULL, lease_until = NULL, finished_at = now()
         WHERE id = $1 AND attempt = $8 AND status = 'running'
         RETURNING job_id",
    )
    .bind(step.id)
    .bind(out.skipped)
    .bind(&out.output)
    .bind(&out.provider)
    .bind(&out.model)
    .bind(&out.usage)
    .bind(out.cost_micros)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(job_id) = job_id else {
        return Err(lease_lost(step));
    };
    if let Some(m) = &out.unrecorded {
        crate::metering::record_in(&mut tx, step, m).await?;
    }
    // Serialises the completers of one job, so the last of two parallel steps sees the other's
    // committed status and completes the job (READ COMMITTED: the count below is a new snapshot).
    let job_status: String =
        sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await?;
    if matches!(job_status.as_str(), "queued" | "running" | "failed") {
        for (kind, key, input) in next {
            enqueue(&mut tx, job_id, *kind, key, input.clone()).await?;
        }
    }
    if matches!(job_status.as_str(), "queued" | "running") {
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ingest_steps
             WHERE job_id = $1 AND status IN ('pending','running')",
        )
        .bind(job_id)
        .fetch_one(&mut *tx)
        .await?;
        let status = if open == 0 { "completed" } else { "running" };
        sqlx::query("UPDATE ingest_jobs SET status = $2, updated_at = now() WHERE id = $1")
            .bind(job_id)
            .bind(status)
            .execute(&mut *tx)
            .await?;
    }
    if job_status == "cancelling" {
        settle_cancelling(&mut tx, job_id).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Gives back a step this worker holds without a result, because the process is shutting down
/// (rulings R39): `pending` and due now, lease cleared, `releases` + 1, so another pod claims it
/// at once instead of after a 10-minute lease. `attempt` is NOT decremented (it is the fence
/// token: the next claim gets a new one); up to [`MAX_RELEASE_REFUNDS`] releases are refunded to
/// the budget instead. A step released with its budget used up (its run keeps getting the pod
/// terminated) is failed like an exhausted Retryable failure (`shutdown_released`), so it cannot
/// cycle for ever. A step of a job that ended meanwhile (cancelled, rejected, completed) or is
/// `cancelling` is `cancelled` (a cancelling job then settles). Fenced on the claim: `LeaseLost` (nothing written) when `step` is no longer
/// current.
pub async fn release(pool: &PgPool, step: &ClaimedStep, max_attempts: i32) -> Result<()> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT job_id, attempt, attempt_base, releases, slot_waits FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR UPDATE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Err(lease_lost(step));
    };
    let job_id: Uuid = row.try_get("job_id")?;
    let mut budget = Budget::from_row(&row)?;
    budget.releases += 1;
    let job_status: String =
        sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await?;
    if matches!(
        job_status.as_str(),
        "cancelling" | "cancelled" | "rejected" | "completed"
    ) {
        sqlx::query(
            "UPDATE ingest_steps SET status = 'cancelled', lease_until = NULL, finished_at = now()
             WHERE id = $1",
        )
        .bind(step.id)
        .execute(&mut *tx)
        .await?;
        settle_cancelling(&mut tx, job_id).await?;
    } else if budget_exhausted(budget, max_attempts) {
        tx.commit().await?;
        let err = StepError {
            class: ErrorClass::Retryable,
            code: "shutdown_released".into(),
            message: format!(
                "step {} was interrupted by a shutdown on its last attempt ({} runs, {} releases)",
                step.kind.as_str(),
                budget.attempt,
                budget.releases
            ),
            provider: None,
            retry_after_s: None,
        };
        return fail(pool, step, &err, max_attempts).await;
    } else {
        sqlx::query(
            "UPDATE ingest_steps
             SET status = 'pending', releases = releases + 1, lease_until = NULL,
                 run_after = now(), started_at = NULL
             WHERE id = $1",
        )
        .bind(step.id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Re-queues a step whose run was abandoned with `slot_wait_timeout` (it never got its shared
/// slot): `pending` after `backoff(slot_waits)`, `slot_waits` + 1, no attempt used up (rulings
/// R35). Past [`MAX_SLOT_WAIT_REQUEUES`] it is an ordinary Retryable failure ([`fail`]). Fenced
/// on the claim like every step write.
pub async fn requeue_slot_wait(
    pool: &PgPool,
    step: &ClaimedStep,
    err: &StepError,
    max_attempts: i32,
) -> Result<()> {
    let error = to_json(err)?;
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT s.slot_waits, j.status AS job_status
         FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id
         WHERE s.id = $1 AND s.attempt = $2 AND s.status = 'running' FOR UPDATE OF s",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Err(lease_lost(step));
    };
    let slot_waits: i32 = row.try_get("slot_waits")?;
    let job_status: String = row.try_get("job_status")?;
    if slot_waits >= MAX_SLOT_WAIT_REQUEUES
        || matches!(
            job_status.as_str(),
            "cancelling" | "cancelled" | "rejected" | "completed"
        )
    {
        tx.rollback().await?;
        return fail(pool, step, err, max_attempts).await;
    }
    let delay = backoff(slot_waits + 1);
    sqlx::query(
        "UPDATE ingest_steps
         SET status = 'pending', slot_waits = slot_waits + 1, lease_until = NULL, error = $2,
             run_after = now() + make_interval(secs => $3)
         WHERE id = $1",
    )
    .bind(step.id)
    .bind(&error)
    .bind(delay.as_secs_f64())
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::info!(
        step = step.id,
        slot_waits = slot_waits + 1,
        delay_s = delay.as_secs(),
        "slot wait timed out; re-queued without using an attempt"
    );
    Ok(())
}

/// Records the failure of a step this worker still holds (`LeaseLost`, nothing written, when the
/// claim is no longer current):
/// - its job is cancelling/cancelled/rejected → the step is `cancelled` (no more work for that
///   job; a cancelling job then settles);
/// - `Retryable` with budget left ([`budget_exhausted`]) → `pending` after `backoff(attempt)`;
/// - a Caption whose Retryable failure used up its budget → `skipped` (rulings R40);
/// - otherwise → step `failed`, and a queued/running job `failed` with the step's error copied.
pub async fn fail(
    pool: &PgPool,
    step: &ClaimedStep,
    err: &StepError,
    max_attempts: i32,
) -> Result<()> {
    let error = to_json(err)?;
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT job_id, attempt, attempt_base, releases, slot_waits FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR UPDATE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Err(lease_lost(step));
    };
    let job_id: Uuid = row.try_get("job_id")?;
    let budget = Budget::from_row(&row)?;
    let job_status: String =
        sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1 FOR UPDATE")
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await?;
    let (id, attempt) = (step.id, step.attempt);
    if matches!(job_status.as_str(), "cancelling" | "cancelled" | "rejected") {
        sqlx::query(
            "UPDATE ingest_steps
             SET status = 'cancelled', lease_until = NULL, error = $2, finished_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(&error)
        .execute(&mut *tx)
        .await?;
        settle_cancelling(&mut tx, job_id).await?;
    } else if err.class == ErrorClass::Retryable && !budget_exhausted(budget, max_attempts) {
        let delay = retry_delay(attempt, err.retry_after_s);
        sqlx::query(
            "UPDATE ingest_steps
             SET status = 'pending', lease_until = NULL, error = $2,
                 run_after = now() + make_interval(secs => $3)
             WHERE id = $1",
        )
        .bind(id)
        .bind(&error)
        .bind(delay.as_secs_f64())
        .execute(&mut *tx)
        .await?;
        tracing::info!(step = id, attempt, code = %err.code, delay_s = delay.as_secs(), "step will retry");
    } else if err.class == ErrorClass::Retryable && step.kind == StepKind::Caption {
        skip_caption(&mut tx, id, &error).await?;
        settle_job(&mut tx, job_id).await?;
        tracing::warn!(step = id, attempt, code = %err.code, "caption failed on its last attempt; skipped");
    } else {
        sqlx::query(
            "UPDATE ingest_steps
             SET status = 'failed', lease_until = NULL, error = $2, finished_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(&error)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE ingest_jobs SET status = 'failed', error = $2, updated_at = now()
             WHERE id = $1 AND status IN ('queued','running')",
        )
        .bind(job_id)
        .bind(&error)
        .execute(&mut *tx)
        .await?;
        tracing::warn!(step = id, attempt, code = %err.code, class = ?err.class, "step failed");
    }
    tx.commit().await?;
    Ok(())
}

/// Merges `patch` (a JSON object) into the job's `meta`, top-level keys replacing. Fenced on the
/// claim like every step write: `LeaseLost` (nothing written) when `step` is no longer current.
pub async fn merge_job_meta(pool: &PgPool, step: &ClaimedStep, patch: &Value) -> Result<()> {
    let n = sqlx::query(
        "UPDATE ingest_jobs j SET meta = j.meta || $3, updated_at = now()
         FROM ingest_steps s
         WHERE s.id = $1 AND s.attempt = $2 AND s.status = 'running' AND j.id = s.job_id",
    )
    .bind(step.id)
    .bind(step.attempt)
    .bind(patch)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 0 {
        return Err(lease_lost(step));
    }
    Ok(())
}

/// Keys of the job's `kind` steps that completed, in queue order.
pub async fn completed_keys(pool: &PgPool, job_id: Uuid, kind: StepKind) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT key FROM ingest_steps
         WHERE job_id = $1 AND kind = $2 AND status = 'completed' ORDER BY id",
    )
    .bind(job_id)
    .bind(kind.as_str())
    .fetch_all(pool)
    .await?)
}

/// Appends strata's `document_id` to the step's job's `document_ids`, once: an id already there
/// (a retried Deliver that strata answered with the same document) is not added again; returns
/// whether it was added. Fenced on the claim: `LeaseLost` (nothing written) when `step` is no
/// longer current. Lock order: the step row, then the job row.
pub async fn append_document_id(pool: &PgPool, step: &ClaimedStep, id: Uuid) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let job_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR SHARE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(job_id) = job_id else {
        return Err(lease_lost(step));
    };
    let added = sqlx::query(
        "UPDATE ingest_jobs SET document_ids = array_append(document_ids, $2), updated_at = now()
         WHERE id = $1 AND NOT ($2 = ANY(document_ids))",
    )
    .bind(job_id)
    .bind(id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(added > 0)
}

/// Ends a queued/running job as `rejected` with `err` (e.g. the Scan step found malware). The
/// step itself is then completed with no successors; `complete_and_enqueue` leaves a rejected
/// job's status alone (rulings R2). A job that already ended (cancelled, failed) is left as it
/// is. Fenced on the claim: `LeaseLost` when `step` is no longer current.
pub async fn reject_job(pool: &PgPool, step: &ClaimedStep, err: &StepError) -> Result<()> {
    let error = to_json(err)?;
    let mut tx = pool.begin().await?;
    // Lock order: the step row, then the job row.
    let job_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT job_id FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR UPDATE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(job_id) = job_id else {
        return Err(lease_lost(step));
    };
    sqlx::query(
        "UPDATE ingest_jobs SET status = 'rejected', error = $2, updated_at = now()
         WHERE id = $1 AND status IN ('queued','running')",
    )
    .bind(job_id)
    .bind(&error)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

fn lease_lost(step: &ClaimedStep) -> IngestError {
    IngestError::LeaseLost {
        step: step.id,
        attempt: step.attempt,
    }
}

fn to_json(err: &StepError) -> Result<Value> {
    serde_json::to_value(err).map_err(|e| IngestError::InvalidInput(e.to_string()))
}

pub(crate) fn parse_kind(s: &str) -> Result<StepKind> {
    StepKind::parse(s).ok_or_else(|| {
        IngestError::Db(sqlx::Error::Decode(
            format!("unknown step kind {s:?}").into(),
        ))
    })
}
