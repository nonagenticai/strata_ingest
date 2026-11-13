//! `metering_outbox`: one billing cost event per paid provider call, delivered to `billing:costs`
//! by the drainer (`metering::run_drainer`). Migration 0005.
//!
//! - [`insert`] is one statement in its own (implicit) transaction, run right after the provider
//!   call returned (`metering::record`): independent of the step's completion and its fence, so a
//!   paid call keeps its row even when the step's result is then fenced out or fails. Only when
//!   that record failed does `steps::complete_and_enqueue` insert it in the completion
//!   transaction instead. Idempotent on `event_key`.
//! - [`claim`] leases due pending rows (`FOR UPDATE SKIP LOCKED`, then `next_attempt_at` pushed
//!   past the lease) and COMMITS: no transaction is open while the drainer talks to Redis. Several
//!   drainers (one per replica) never claim the same row while its lease runs; a claimed row whose
//!   drainer died before marking it is due again once the lease has passed (re-sent; billing
//!   dedups on `event_key`).
//! - [`mark_sent`] / [`mark_failed`] / [`release`] settle one claimed row each, short statements.
//! - [`prune_sent`] deletes delivered rows past the retention window.

use std::collections::BTreeMap;
use std::time::Duration;

use sqlx::types::Json;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::Result;

/// A pending row claimed for delivery.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub id: i64,
    pub event_key: String,
    pub fields: BTreeMap<String, String>,
    pub attempts: i32,
}

/// Inserts one event (`fields` carries `event_key`) on `conn`: a pool connection (its own
/// transaction) or the step-completion transaction. A second insert of the same `event_key` is
/// a no-op; returns whether a row was inserted.
pub async fn insert(
    conn: &mut PgConnection,
    tenant: Uuid,
    event_key: &str,
    fields: &BTreeMap<&'static str, String>,
) -> Result<bool> {
    let n = sqlx::query(
        "INSERT INTO metering_outbox (tenant, event_key, fields) VALUES ($1, $2, $3)
         ON CONFLICT (event_key) DO NOTHING",
    )
    .bind(tenant)
    .bind(event_key)
    .bind(Json(fields))
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok(n == 1)
}

/// The stored field map of `event_key`, if there is a row.
pub async fn fields_of(
    conn: &mut PgConnection,
    event_key: &str,
) -> Result<Option<BTreeMap<String, String>>> {
    let row: Option<Json<BTreeMap<String, String>>> =
        sqlx::query_scalar("SELECT fields FROM metering_outbox WHERE event_key = $1")
            .bind(event_key)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(row.map(|j| j.0))
}

/// Leases up to `limit` due pending rows for `lease` (oldest first, skipping rows another
/// transaction holds) and commits. Returns them in id order.
pub async fn claim(pool: &PgPool, limit: i64, lease: Duration) -> Result<Vec<OutboxRow>> {
    let rows = sqlx::query(
        "WITH due AS (
           SELECT id FROM metering_outbox
           WHERE status = 'pending' AND next_attempt_at <= now()
           ORDER BY next_attempt_at, id
           LIMIT $1
           FOR UPDATE SKIP LOCKED)
         UPDATE metering_outbox m SET next_attempt_at = now() + make_interval(secs => $2)
         FROM due WHERE m.id = due.id
         RETURNING m.id, m.event_key, m.fields, m.attempts",
    )
    .bind(limit)
    .bind(lease.as_secs_f64())
    .fetch_all(pool)
    .await?;
    let mut out = rows
        .into_iter()
        .map(|r| {
            let Json(fields): Json<BTreeMap<String, String>> = r.try_get("fields")?;
            Ok(OutboxRow {
                id: r.try_get("id")?,
                event_key: r.try_get("event_key")?,
                fields,
                attempts: r.try_get("attempts")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    out.sort_by_key(|r| r.id);
    Ok(out)
}

/// Marks a claimed row delivered.
pub async fn mark_sent(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE metering_outbox SET status = 'sent', sent_at = now(), last_error = NULL
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records a failed delivery of a claimed row: `attempts` + 1 and the next try after
/// `min(max, base · 2^attempts)` (the row's attempts before this failure).
pub async fn mark_failed(
    pool: &PgPool,
    id: i64,
    error: &str,
    base: Duration,
    max: Duration,
) -> Result<()> {
    sqlx::query(
        "UPDATE metering_outbox
         SET attempts = attempts + 1, last_error = $2,
             next_attempt_at = now() + make_interval(
               secs => LEAST($4, $3 * power(2.0::float8, LEAST(attempts, 30))))
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .bind(error)
    .bind(base.as_secs_f64())
    .bind(max.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// Gives claimed rows that were not tried back, due after `after` (no attempt counted).
pub async fn release(pool: &PgPool, ids: &[i64], after: Duration) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE metering_outbox SET next_attempt_at = now() + make_interval(secs => $2)
         WHERE id = ANY($1) AND status = 'pending'",
    )
    .bind(ids)
    .bind(after.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes at most `limit` rows sent longer than `older_than` ago; returns how many.
pub async fn prune_sent(pool: &PgPool, older_than: Duration, limit: i64) -> Result<u64> {
    // ARRAY(subquery) is evaluated once, so the LIMIT bounds the delete (an `id IN (… LIMIT n)`
    // semi-join may be planned without honouring it).
    let n = sqlx::query(
        "DELETE FROM metering_outbox WHERE id = ANY(ARRAY(
           SELECT id FROM metering_outbox
           WHERE status = 'sent' AND sent_at < now() - make_interval(secs => $1)
           ORDER BY sent_at LIMIT $2))",
    )
    .bind(older_than.as_secs_f64())
    .bind(limit)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n)
}

/// Undelivered spend: pending rows and the age in seconds of the oldest (0 when none).
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Backlog {
    pub pending: i64,
    pub oldest_pending_age_s: f64,
}

pub async fn backlog(pool: &PgPool) -> Result<Backlog> {
    let row = sqlx::query(
        "SELECT count(*) AS pending,
                COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8 AS oldest
         FROM metering_outbox WHERE status = 'pending'",
    )
    .fetch_one(pool)
    .await?;
    Ok(Backlog {
        pending: row.try_get("pending")?,
        oldest_pending_age_s: row.try_get("oldest")?,
    })
}
