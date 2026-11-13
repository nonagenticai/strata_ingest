//! Postgres access: jobs (`jobs`), the persisted step queue (`steps`), stored blobs (`assets`)
//! and textual artefacts (`artifacts`), provider registrations per tenant (`providers`) and per
//! store (`store_providers`).

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::error::Result;

pub mod artifacts;
pub mod assets;
pub mod jobs;
pub mod outbox;
pub mod providers;
pub mod steps;
pub mod store_providers;

/// `serve`: how long startup keeps retrying the DB connect + migrations before it gives up and
/// the process exits non-zero (rulings: a Postgres blip during a rollout must not crash-loop
/// the pod; the API answers 503 `not_ready` meanwhile).
pub const STARTUP_DB_DEADLINE: Duration = Duration::from_secs(120);
/// First and largest pause between two startup attempts (doubling in between).
pub const STARTUP_RETRY_MIN: Duration = Duration::from_secs(1);
pub const STARTUP_RETRY_MAX: Duration = Duration::from_secs(10);

/// A pool that connects on first use (no I/O here). Each acquire gives up after 5 s.
pub fn connect_lazy(url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy(url)?)
}

/// Applies the embedded migrations. sqlx serialises concurrent migrators with an advisory lock.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

/// Open a pool and apply the embedded migrations. If the DB is unreachable this returns
/// `IngestError::Db` (no retry: `migrate` and tests).
pub async fn connect(url: &str) -> Result<PgPool> {
    let pool = connect_lazy(url)?;
    migrate(&pool).await?;
    Ok(pool)
}

/// [`migrate`], retried with a doubling pause (`STARTUP_RETRY_MIN`..`STARTUP_RETRY_MAX`) until it
/// succeeds or `deadline` has passed (then the last error). Every failed attempt is logged.
pub async fn migrate_with_retry(pool: &PgPool, deadline: Duration) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut pause = STARTUP_RETRY_MIN;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match migrate(pool).await {
            Ok(()) => {
                if attempt > 1 {
                    tracing::info!(attempt, "database reachable; migrations applied");
                }
                return Ok(());
            }
            Err(e) => {
                let elapsed = start.elapsed();
                if elapsed + pause > deadline {
                    tracing::error!(
                        attempt,
                        error = %e,
                        elapsed_s = elapsed.as_secs(),
                        "database still unreachable at startup; giving up"
                    );
                    return Err(e);
                }
                tracing::warn!(
                    attempt,
                    error = %e,
                    retry_in_s = pause.as_secs_f64(),
                    "database not ready at startup (connect/migrate failed); retrying"
                );
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(STARTUP_RETRY_MAX);
            }
        }
    }
}
