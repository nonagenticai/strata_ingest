//! `ingest_artifacts`: a job's textual intermediates (rulings R26), by name relative to the job:
//! `convert.json`, `ocr/{step}.json`, `transcript/{step}.json`, `bundle.md`. They used to be
//! blobs; in Postgres they need no virus scan, no storage round trip and go with the job (ON
//! DELETE CASCADE). Writes from a worker are fenced on its step claim (rulings R12).

use sqlx::PgPool;
use uuid::Uuid;

use crate::db::steps::ClaimedStep;
use crate::error::{IngestError, Result};

/// Writes artefact `key` of the step's job; a retried step overwrites its own artefact. Fenced on
/// the claim: `LeaseLost` (nothing written) when `step` is no longer current.
pub async fn put(pool: &PgPool, step: &ClaimedStep, key: &str, body: &str) -> Result<()> {
    if key.is_empty() {
        return Err(IngestError::InvalidInput("artifact key is empty".into()));
    }
    // One statement: the claim check and the upsert see the same snapshot, and the step row is
    // locked against a concurrent reclaim until the upsert commits.
    let n = sqlx::query(
        "WITH current AS (
           SELECT job_id FROM ingest_steps
           WHERE id = $1 AND attempt = $2 AND status = 'running' FOR SHARE
         )
         INSERT INTO ingest_artifacts (job_id, key, body)
         SELECT job_id, $3, $4 FROM current
         ON CONFLICT (job_id, key) DO UPDATE SET body = EXCLUDED.body, created_at = now()",
    )
    .bind(step.id)
    .bind(step.attempt)
    .bind(key)
    .bind(body)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 0 {
        return Err(IngestError::LeaseLost {
            step: step.id,
            attempt: step.attempt,
        });
    }
    Ok(())
}

/// Artefact `key` of job `job_id`; `None` when it was never written.
pub async fn get(pool: &PgPool, job_id: Uuid, key: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT body FROM ingest_artifacts WHERE job_id = $1 AND key = $2")
            .bind(job_id)
            .bind(key)
            .fetch_optional(pool)
            .await?,
    )
}

/// The names of job `job_id`'s artefacts, sorted.
pub async fn keys(pool: &PgPool, job_id: Uuid) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar("SELECT key FROM ingest_artifacts WHERE job_id = $1 ORDER BY key")
            .bind(job_id)
            .fetch_all(pool)
            .await?,
    )
}
