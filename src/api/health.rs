//! `GET /health`: liveness (always 200) plus the state of every dependency and provider.
//!
//! Body: `{"status": "ok"|"degraded", "db", "storage", "docling", "strata"}` each
//! `"ok"|"down"`, and `{"ocr", "vision", "transcribe"}` each `"unconfigured"` or
//! `{"last_success", "last_error_class", "last_error_at"}` from [`ProviderHealth`]: the
//! deployment's default providers only (`unconfigured` in production), never a tenant's
//! registration. `metering`: `{"transport": "on"|"off", "pending", "oldest_pending_age_s"}`, the
//! billing outbox backlog (undelivered cost events; `pending`/`oldest_pending_age_s` null when
//! the DB could not be read). Dependency
//! probes run concurrently, each bounded by [`CHECK_TIMEOUT`], and their result is cached for
//! [`CACHE_TTL`] (one probe round at a time; concurrent callers wait for it). Provider status is
//! read live (it is in memory). Nothing here carries a URL, key or error message.
//!
//! [`ProviderHealth`]: crate::providers::ProviderHealth

use std::future::Future;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::pipeline::Ctx;
use crate::providers::Capability;

/// How long one probe round is reused.
pub const CACHE_TTL: Duration = Duration::from_secs(10);
/// Ceiling of each dependency probe.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Down,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Deps {
    pub db: Status,
    /// storage_service `GET /health/ready` (it reports clamd only on `/health`; a scanner
    /// outage shows up as Retryable `storage_unavailable` on uploads).
    pub storage: Status,
    pub docling: Status,
    pub strata: Status,
    /// The billing outbox backlog; `None` when it could not be read in time.
    pub metering: Option<crate::db::outbox::Backlog>,
}

impl Deps {
    fn all_ok(&self) -> bool {
        [self.db, self.storage, self.docling, self.strata]
            .iter()
            .all(|s| *s == Status::Ok)
    }
}

/// The last probe round and when it ran.
#[derive(Debug, Default)]
pub struct HealthCache {
    last: Mutex<Option<(Instant, Deps)>>,
}

impl HealthCache {
    async fn deps(&self, ctx: &Ctx) -> Deps {
        let mut last = self.last.lock().await;
        if let Some((at, deps)) = *last
            && at.elapsed() < CACHE_TTL
        {
            return deps;
        }
        let deps = probe(ctx).await;
        *last = Some((Instant::now(), deps));
        deps
    }
}

async fn check(f: impl Future<Output = bool>) -> Status {
    match tokio::time::timeout(CHECK_TIMEOUT, f).await {
        Ok(true) => Status::Ok,
        _ => Status::Down,
    }
}

async fn probe(ctx: &Ctx) -> Deps {
    let (db, storage, docling, strata, metering) = tokio::join!(
        check(async { sqlx::query("SELECT 1").execute(&ctx.pool).await.is_ok() }),
        check(ctx.storage.health(CHECK_TIMEOUT)),
        check(ctx.docling.health(CHECK_TIMEOUT)),
        check(ctx.strata.health(CHECK_TIMEOUT)),
        tokio::time::timeout(CHECK_TIMEOUT, crate::db::outbox::backlog(&ctx.pool)),
    );
    Deps {
        db,
        storage,
        docling,
        strata,
        metering: metering.ok().and_then(|r| r.ok()),
    }
}

/// The `/health` body.
pub async fn report(ctx: &Ctx, cache: &HealthCache) -> Value {
    let deps = cache.deps(ctx).await;
    let configured = [
        (Capability::Ocr, ctx.providers.has_default(Capability::Ocr)),
        (
            Capability::Vision,
            ctx.providers.has_default(Capability::Vision),
        ),
        (
            Capability::Transcribe,
            ctx.providers.has_default(Capability::Transcribe),
        ),
    ];
    let mut body = json!({
        "status": if deps.all_ok() { "ok" } else { "degraded" },
        "db": deps.db,
        "storage": deps.storage,
        "docling": deps.docling,
        "strata": deps.strata,
        "metering": {
            "transport": if ctx.cfg.metering.is_some() { "on" } else { "off" },
            "pending": deps.metering.map(|b| b.pending),
            "oldest_pending_age_s": deps.metering.map(|b| b.oldest_pending_age_s.round() as i64),
        },
    });
    for (cap, on) in configured {
        body[cap.as_str()] = if on {
            json!(ctx.health.get(cap))
        } else {
            json!("unconfigured")
        };
    }
    body
}
