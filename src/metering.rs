//! Cost events to billing-service (`billing:costs`), one per paid provider call, through an outbox
//! (`metering_outbox`, migration 0005; `db::outbox`).
//!
//! Contract: billing-service `docs/EMITTER_GUIDE.md` (flat string→string map on the stream,
//! `cost_source` = `strata-ingest`) and `src/tenancy.py` `tenant_level_project_id`.
//!
//! - Recorded when the provider call returns: the step calls [`record`] right after a successful
//!   OCR / vision / transcribe call, one INSERT in its own transaction, independent of the step's
//!   completion and its fence. The provider was already paid, so the row must exist even when the
//!   step's result is then fenced out, fails, or is a refusal (commonpractice `llm-broker.ts`
//!   semantics). A step that made no successful provider call has no row.
//! - Delivered by [`run_drainer`] (started by `serve` only when `METERING_REDIS_URL` is set;
//!   stream `METERING_STREAM`, default `billing:costs`; the deployment sets both explicitly, the
//!   guide's rule). Rows are written regardless, so a deployment without Redis (or a Redis outage)
//!   only delays delivery: the rows wait in the outbox and go out once the transport is there.
//! - At-least-once: a crash between the XADD and the `sent` update re-sends the row. `event_key` =
//!   `<job_id>:<kind>/<key>:<attempt>` (one provider call per step attempt) is billing's dedup key
//!   (billing condenses a key over 64 characters to `p1:<sha256 prefix>` on its side), so a
//!   re-send is stored once and a retried attempt bills its own call once.
//! - Attribution: the API knows only the tenant (the bearer token's), so `project_id` is always
//!   the tenant-level project id, and `license_id` / `subject_id` / `app_id` are sent empty
//!   (service-to-service, billing derives what it can); `actor_id` is omitted.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::json;
use sqlx::{PgConnection, PgPool};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::{Secret, get, redact_url};
use crate::db::outbox;
use crate::db::steps::ClaimedStep;
use crate::error::{IngestError, Result};
use crate::providers::{Capability, Usage, UsageUnit};

pub const COST_SOURCE: &str = "strata-ingest";
pub const DEFAULT_STREAM: &str = "billing:costs";
pub const REDIS_URL_ENV: &str = "METERING_REDIS_URL";
pub const STREAM_ENV: &str = "METERING_STREAM";
const MAXLEN: usize = 100_000;

/// One cost event: the flat field map XADDed to the stream.
pub type CostEvent = BTreeMap<&'static str, String>;

/// Who pays for a provider call (billing `services/funding.py`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Funding {
    /// Self-hosted platform compute (mlx-api and other in-cluster / private hosts).
    LocalGpu,
    /// An external vendor billed the platform.
    PlatformExternal,
    /// The tenant's own registered provider and key.
    TenantByo,
}

impl Funding {
    pub fn as_str(self) -> &'static str {
        match self {
            Funding::LocalGpu => "local_gpu",
            Funding::PlatformExternal => "platform_external",
            Funding::TenantByo => "tenant_byo",
        }
    }
}

/// How a deployment-default provider at `base_url` (dialect `dialect`) is billed: a private or
/// in-cluster host (`*.internal`, `*.svc…`, `localhost`, single-label names, non-global IPs) is
/// `local_gpu` and named after its host (`mlx-api*` → `mlx-api`), so it never joins a commercial
/// vendor's price row; any other host is `platform_external`, named by its dialect.
pub fn default_billing(base_url: &str, dialect: &str) -> (Funding, String) {
    let host = url::Url::parse(base_url).ok().and_then(|u| {
        u.host_str()
            .map(|h| h.trim_matches(['[', ']']).to_ascii_lowercase())
    });
    let Some(host) = host.filter(|h| !h.is_empty()) else {
        return (Funding::PlatformExternal, dialect.to_string());
    };
    let local = match host.parse::<IpAddr>() {
        Ok(ip) => !is_global(ip),
        Err(_) => {
            host == "localhost"
                || !host.contains('.')
                || host.ends_with(".internal")
                || host.ends_with(".local")
                || host.ends_with(".localhost")
                || host.ends_with(".svc")
                || host.contains(".svc.")
        }
    };
    if !local {
        return (Funding::PlatformExternal, dialect.to_string());
    }
    let label = host.split('.').next().unwrap_or(&host);
    let name = if label.starts_with("mlx-api") {
        "mlx-api".to_string()
    } else if host.parse::<IpAddr>().is_ok() {
        "local".to_string()
    } else {
        label.to_string()
    };
    (Funding::LocalGpu, name)
}

fn is_global(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                // CGNAT 100.64/10
                || (o[0] == 100 && (o[1] & 0xc0) == 64))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_global(IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || (s[0] & 0xfe00) == 0xfc00 // unique local
                || (s[0] & 0xffc0) == 0xfe80) // link local
        }
    }
}

/// What a provider step hands the worker to meter once its result is committed.
#[derive(Debug, Clone, PartialEq)]
pub struct Meter {
    pub capability: Capability,
    /// billing `provider`: the dialect for an external or tenant provider, the host for a local one.
    pub provider: String,
    pub model: String,
    pub funding: Funding,
    pub usage: Usage,
    pub cost_micros: Option<i64>,
    pub latency_ms: u64,
    /// When the provider call returned.
    pub at: DateTime<Utc>,
}

/// billing `tenancy.tenant_level_project_id`: the 32 hex digits of `tenant`, reversed, as a
/// uuid. The project id a project-less caller sends for a tenant's own spend.
pub fn tenant_level_project_id(tenant: Uuid) -> Uuid {
    let hex: String = tenant.simple().to_string().chars().rev().collect();
    Uuid::parse_str(&hex).expect("32 hex digits are a uuid")
}

/// The step's key within its job (`ingest_steps` is unique on `(job_id, kind, key)`).
pub fn step_key(step: &ClaimedStep) -> String {
    format!("{}/{}", step.kind.as_str(), step.key)
}

/// Unique per step attempt, so a re-sent event dedups in billing.
pub fn event_key(step: &ClaimedStep) -> String {
    format!("{}:{}:{}", step.job_id, step_key(step), step.attempt)
}

/// The event for `step`'s committed attempt.
///
/// The natural unit goes in `input_tokens` (EMITTER_GUIDE): tokens, seconds, or OCR pages. Pages
/// have no value in billing's bounded `quantity_unit` set (`tokens|characters|seconds|megapixels|
/// passages|requests`), so a page-metered event omits `quantity_unit` and names the unit in
/// `usage_extra` (`unit: "pages"`, `pages`). Usage the provider did not report is sent as
/// `input_tokens` 0 with `usage_extra.usage_unknown: true`, so a $0 row explains itself.
pub fn cost_event(step: &ClaimedStep, m: &Meter) -> CostEvent {
    let cap = m.capability.as_str();
    let quantity_unit = match m.usage.unit {
        UsageUnit::Pages => None,
        UsageUnit::Tokens => Some("tokens"),
        UsageUnit::Seconds => Some("seconds"),
    };
    let input_tokens = m.usage.units.unwrap_or(0);
    let unit = serde_json::to_value(m.usage.unit).expect("UsageUnit serializes");
    let mut extra = json!({
        "capability": cap,
        "unit": unit,
        "units": m.usage.units,
        "job_id": step.job_id.to_string(),
        "step_key": step_key(step),
    });
    if m.usage.unit == UsageUnit::Pages {
        extra["pages"] = json!(m.usage.units);
    }
    if m.usage.units.is_none() {
        extra["usage_unknown"] = json!(true);
    }
    if m.funding == Funding::TenantByo {
        extra["byo"] = json!(true);
    }
    let mut ev = CostEvent::new();
    let mut put = |k: &'static str, v: String| {
        ev.insert(k, v);
    };
    put("tenant_id", step.tenant.to_string());
    put(
        "project_id",
        tenant_level_project_id(step.tenant).to_string(),
    );
    put("license_id", String::new());
    put("subject_id", String::new());
    put("app_id", String::new());
    put("cost_source", COST_SOURCE.to_string());
    put("service_key", format!("{COST_SOURCE}-{cap}"));
    put("provider", m.provider.clone());
    put("model", m.model.clone());
    put("served_model", m.model.clone());
    put("input_tokens", input_tokens.to_string());
    put("output_tokens", "0".to_string());
    if let Some(q) = quantity_unit {
        put("quantity_unit", q.to_string());
    }
    put("funding", m.funding.as_str().to_string());
    if let Some(c) = m.cost_micros {
        put("cost_usd", format!("{:.6}", c as f64 / 1e6));
    }
    put("latency_ms", m.latency_ms.to_string());
    put("provider_request_id", String::new());
    put("usage_extra", extra.to_string());
    put("event_key", event_key(step));
    put(
        "timestamp",
        m.at.to_rfc3339_opts(SecondsFormat::Micros, true),
    );
    ev
}

/// `METERING_REDIS_URL` + `METERING_STREAM`, and the drainer's pace.
#[derive(Clone)]
pub struct MeteringCfg {
    pub redis_url: Secret,
    pub stream: String,
    /// Pause between drain passes when the last one left nothing due ([`DRAIN_INTERVAL`]; not
    /// an env knob).
    pub interval: Duration,
}

impl fmt::Debug for MeteringCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeteringCfg")
            .field("redis_url", &redact_url(self.redis_url.expose()))
            .field("stream", &self.stream)
            .field("interval", &self.interval)
            .finish()
    }
}

impl MeteringCfg {
    /// `None` (no drainer) when `METERING_REDIS_URL` is unset or empty; an unparseable URL is a
    /// config error.
    pub fn from_map(env: &HashMap<String, String>) -> Result<Option<Self>> {
        let Some(url) = get(env, REDIS_URL_ENV) else {
            return Ok(None);
        };
        redis::Client::open(url)
            .map_err(|_| IngestError::Config(format!("{REDIS_URL_ENV} is not a redis URL")))?;
        Ok(Some(MeteringCfg {
            redis_url: Secret::new(url),
            stream: get(env, STREAM_ENV).unwrap_or(DEFAULT_STREAM).to_string(),
            interval: DRAIN_INTERVAL,
        }))
    }
}

/// Records the cost event of `step`'s provider call, which has just returned (paid): one INSERT
/// in its own transaction, so it stands whatever then happens to the step's result. Idempotent on
/// `event_key` (one call per attempt). Rows are written whether or not `METERING_REDIS_URL` is
/// set: nothing is lost while the transport is off.
pub async fn record(pool: &PgPool, step: &ClaimedStep, meter: &Meter) -> Result<()> {
    if meter.usage.units.is_none() {
        tracing::warn!(
            job = %step.job_id,
            step = %step_key(step),
            attempt = step.attempt,
            capability = meter.capability.as_str(),
            provider = %meter.provider,
            "provider reported no usage; metering input_tokens=0 with usage_unknown"
        );
    }
    let mut conn = pool.acquire().await?;
    record_in(&mut conn, step, meter).await
}

/// [`record`] on `conn` (the step-completion transaction carries a call whose own record failed).
pub async fn record_in(conn: &mut PgConnection, step: &ClaimedStep, meter: &Meter) -> Result<()> {
    let ev = cost_event(step, meter);
    let key = &ev["event_key"];
    if !outbox::insert(conn, step.tenant, key, &ev).await? {
        // One paid call per step attempt is what makes `event_key` unique per call. A second,
        // DIFFERENT call recorded under the same key would be dropped silently (billed once
        // instead of twice): loud in debug builds, an error log in release. The same call recorded
        // twice (a retried insert that had in fact committed) is fine.
        let stored = outbox::fields_of(conn, key).await?;
        let same = stored
            .as_ref()
            .is_some_and(|f| f.len() == ev.len() && ev.iter().all(|(k, v)| f.get(*k) == Some(v)));
        if !same {
            tracing::error!(
                event_key = %key,
                fields = %serde_json::to_string(&ev).unwrap_or_default(),
                "a second, different provider call was recorded under one event_key; it is NOT \
                 billed (a step attempt must make at most one paid call)"
            );
            debug_assert!(
                same,
                "two different paid calls under event_key {key}: a step attempt must make at most \
                 one paid call (or event_key needs a call index)"
            );
        }
    }
    Ok(())
}

/// Pauses between the tries of [`record_with_retry`] (4 tries, about 1.3 s in all).
pub const RECORD_RETRY_PAUSES: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(300),
    Duration::from_millis(900),
];

/// [`record`], tried again after each of [`RECORD_RETRY_PAUSES`]; the last error if every try
/// failed.
pub async fn record_with_retry(pool: &PgPool, step: &ClaimedStep, meter: &Meter) -> Result<()> {
    let mut pauses = RECORD_RETRY_PAUSES.iter();
    loop {
        match record(pool, step, meter).await {
            Ok(()) => return Ok(()),
            Err(e) => match pauses.next() {
                Some(p) => {
                    tracing::warn!(
                        job = %step.job_id,
                        step = %step_key(step),
                        attempt = step.attempt,
                        error = %e,
                        "recording a paid provider call failed; retrying"
                    );
                    tokio::time::sleep(*p).await;
                }
                None => return Err(e),
            },
        }
    }
}

/// Paid calls whose record failed, waiting for the worker to settle them: the worker hands one
/// to `complete_and_enqueue` (inserted in the completion transaction) or, when the step does not
/// complete, tries [`record`] once more. Keyed by (step id, attempt).
#[derive(Debug, Default)]
pub struct Unrecorded {
    calls: std::sync::Mutex<HashMap<(i64, i32), Meter>>,
}

impl Unrecorded {
    pub fn put(&self, step: &ClaimedStep, meter: Meter) {
        self.calls
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((step.id, step.attempt), meter);
    }

    pub fn take(&self, step: &ClaimedStep) -> Option<Meter> {
        self.calls
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(step.id, step.attempt))
    }
}

/// The last chance for a paid call whose record failed and whose step did not commit it: one more
/// [`record`]; if that fails too the event is lost and logged at error level with its full field
/// map (commonpractice `broker_settle_failed_after_paid_call`), so it can be re-entered by hand.
pub async fn record_or_log_lost(pool: &PgPool, step: &ClaimedStep, meter: &Meter, why: &str) {
    if let Err(e) = record(pool, step, meter).await {
        let ev = cost_event(step, meter);
        tracing::error!(
            event_key = %ev["event_key"],
            fields = %serde_json::to_string(&ev).unwrap_or_default(),
            reason = why,
            error = %e,
            "metering_record_failed_after_paid_call: the provider was paid but the cost event \
             could not be recorded"
        );
    }
}

/// Pause between drain passes when the last one left nothing due.
pub const DRAIN_INTERVAL: Duration = Duration::from_secs(5);
/// Rows claimed per pass.
pub const DRAIN_BATCH: i64 = 100;
/// How long a claimed row is reserved for the drainer that claimed it. A drainer that dies (or
/// stalls) before marking it leaves it due again after this; it is then re-sent.
pub const DRAIN_LEASE: Duration = Duration::from_secs(60);
/// A pass stops sending once this much of the lease is used and gives the rest of its rows back,
/// so it never sends a row whose lease another drainer may already have taken over.
const PASS_BUDGET: Duration = Duration::from_secs(30);
/// A failed row is retried after `min(BACKOFF_MAX, BACKOFF_BASE · 2^(attempts-1))`.
pub const BACKOFF_BASE: Duration = Duration::from_secs(5);
pub const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Delivered rows are kept this long (for a look-back), then pruned.
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
const PRUNE_BATCH: i64 = 5_000;
/// One XADD (connect included) may take at most this long.
const XADD_TIMEOUT: Duration = Duration::from_secs(5);
/// At most one drain-failure warning per this period; the rest are counted into the next one.
const FAIL_LOG_EVERY: Duration = Duration::from_secs(60);

/// Why an XADD was not acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError {
    /// The reason (no secrets).
    pub message: String,
    /// The connection itself failed (refused, dropped, timed out): the next row would fail the
    /// same way. `false`: Redis answered this one command with an error.
    pub transport: bool,
}

/// The Redis side of the drainer: XADD `MAXLEN ~ 100000` to `stream`. The connection is opened
/// on the first send and re-opened after a transport failure.
pub struct Sender {
    client: redis::Client,
    conn: Option<redis::aio::ConnectionManager>,
    stream: String,
}

impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("stream", &self.stream)
            .finish()
    }
}

impl Sender {
    /// No I/O.
    pub fn new(cfg: &MeteringCfg) -> Result<Self> {
        let client = redis::Client::open(cfg.redis_url.expose())
            .map_err(|_| IngestError::Config(format!("{REDIS_URL_ENV} is not a redis URL")))?;
        Ok(Sender {
            client,
            conn: None,
            stream: cfg.stream.clone(),
        })
    }

    /// XADDs one event; `Err` when Redis did not acknowledge it.
    pub async fn send(
        &mut self,
        fields: &BTreeMap<String, String>,
    ) -> std::result::Result<(), SendError> {
        let err = match tokio::time::timeout(XADD_TIMEOUT, self.xadd(fields)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => SendError {
                transport: e.is_io_error()
                    || e.is_timeout()
                    || e.is_connection_refusal()
                    || e.is_connection_dropped()
                    || self.conn.is_none(),
                message: e.to_string(),
            },
            Err(_) => SendError {
                message: "timed out".to_string(),
                transport: true,
            },
        };
        if err.transport {
            self.conn = None;
        }
        Err(err)
    }

    async fn xadd(&mut self, fields: &BTreeMap<String, String>) -> redis::RedisResult<()> {
        if self.conn.is_none() {
            let cfg = redis::aio::ConnectionManagerConfig::new()
                .set_number_of_retries(1)
                .set_connection_timeout(Some(Duration::from_secs(2)))
                .set_response_timeout(Some(Duration::from_secs(2)));
            self.conn = Some(
                redis::aio::ConnectionManager::new_with_config(self.client.clone(), cfg).await?,
            );
        }
        let conn = self.conn.as_mut().expect("connected above");
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAXLEN)
            .arg("*");
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        let _id: String = cmd.query_async(conn).await?;
        Ok(())
    }
}

/// What one [`drain_once`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Due rows this pass claimed.
    pub claimed: usize,
    pub sent: usize,
    /// Rows whose XADD failed (backed off, `attempts` + 1).
    pub failed: usize,
    /// Rows given back untried (after a transport failure, or past the pass budget).
    pub released: usize,
    /// The first send failure of the pass.
    pub error: Option<String>,
}

/// One pass: claims (leases for `lease`, then commits) up to `limit` due rows, then — with no
/// transaction open — XADDs them in order and settles each row on its own: `sent`, or a backoff
/// on failure. A row Redis refused does not stop the pass; a transport failure (the connection
/// is down, so every next row would fail the same way) or running past [`PASS_BUDGET`] gives the
/// untried rest back. A crash leaves claimed rows pending; they are re-sent after the lease and
/// billing dedups them on `event_key` (at-least-once by design).
pub async fn drain_once(
    pool: &PgPool,
    sender: &mut Sender,
    limit: i64,
    lease: Duration,
) -> Result<DrainReport> {
    let rows = outbox::claim(pool, limit, lease).await?;
    let started = tokio::time::Instant::now();
    let budget = PASS_BUDGET.min(lease / 2);
    let mut report = DrainReport {
        claimed: rows.len(),
        ..DrainReport::default()
    };
    let mut rest = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        if started.elapsed() > budget {
            rest.extend(rows[i..].iter().map(|r| r.id));
            break;
        }
        match sender.send(&row.fields).await {
            Ok(()) => {
                outbox::mark_sent(pool, row.id).await?;
                report.sent += 1;
            }
            Err(e) => {
                outbox::mark_failed(pool, row.id, &e.message, BACKOFF_BASE, BACKOFF_MAX).await?;
                report.failed += 1;
                if report.error.is_none() {
                    report.error = Some(e.message.clone());
                }
                if e.transport {
                    rest.extend(rows[i + 1..].iter().map(|r| r.id));
                    break;
                }
            }
        }
    }
    outbox::release(pool, &rest, BACKOFF_BASE).await?;
    report.released = rest.len();
    Ok(report)
}

/// Rate-limits a repeated warning: `ready` is `Some` at most once per `every`, with how many
/// calls it suppressed since the last time it was.
#[derive(Debug)]
struct Throttle {
    every: Duration,
    last: Option<tokio::time::Instant>,
    suppressed: u64,
}

impl Throttle {
    fn new(every: Duration) -> Self {
        Throttle {
            every,
            last: None,
            suppressed: 0,
        }
    }

    fn ready(&mut self) -> Option<u64> {
        let now = tokio::time::Instant::now();
        if self
            .last
            .is_some_and(|t| now.duration_since(t) < self.every)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// The background drainer (`serve`, only when `METERING_REDIS_URL` is set): a pass at start,
/// then every `cfg.interval` (at once again while batches come back full), prunes sent rows
/// older than [`RETENTION`] once an hour, and logs failures at most once a minute with the
/// backlog. Runs on every replica; claims keep them apart. After `shutdown` it makes one last
/// pass, then returns.
pub async fn run_drainer(pool: PgPool, cfg: MeteringCfg, shutdown: CancellationToken) {
    let mut sender = match Sender::new(&cfg) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "metering drainer not started");
            return;
        }
    };
    tracing::info!(stream = %cfg.stream, "metering outbox drainer started");
    let mut warn = Throttle::new(FAIL_LOG_EVERY);
    let mut failing = false;
    let mut next_prune = tokio::time::Instant::now();
    loop {
        let stopping = shutdown.is_cancelled();
        let mut again = false;
        match drain_once(&pool, &mut sender, DRAIN_BATCH, DRAIN_LEASE).await {
            Ok(r) => {
                if let Some(err) = &r.error {
                    failing = true;
                    if let Some(suppressed) = warn.ready() {
                        let b = outbox::backlog(&pool).await.ok();
                        tracing::warn!(
                            error = %err,
                            failed = r.failed,
                            pending = b.map(|b| b.pending),
                            oldest_pending_age_s = b.map(|b| b.oldest_pending_age_s as i64),
                            suppressed,
                            "cost events not delivered; kept in metering_outbox, retrying with backoff"
                        );
                    }
                } else if failing && r.sent > 0 {
                    failing = false;
                    tracing::info!(sent = r.sent, "cost event delivery recovered");
                }
                again = r.error.is_none() && r.claimed as i64 >= DRAIN_BATCH;
            }
            Err(e) => {
                if let Some(suppressed) = warn.ready() {
                    tracing::warn!(error = %e, suppressed, "metering outbox drain failed");
                }
            }
        }
        if stopping {
            tracing::info!("metering outbox drainer stopped");
            return;
        }
        if tokio::time::Instant::now() >= next_prune {
            next_prune = tokio::time::Instant::now() + PRUNE_EVERY;
            match outbox::prune_sent(&pool, RETENTION, PRUNE_BATCH).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(deleted = n, "pruned delivered cost events"),
                Err(e) => tracing::warn!(error = %e, "pruning delivered cost events failed"),
            }
        }
        if !again {
            tokio::select! {
                () = tokio::time::sleep(cfg.interval) => {}
                () = shutdown.cancelled() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::steps::StepKind;
    use serde_json::Value;

    fn step(kind: StepKind, key: &str, attempt: i32) -> ClaimedStep {
        ClaimedStep {
            id: 7,
            job_id: Uuid::parse_str("0199e1a0-1111-7000-8000-00000000000a").unwrap(),
            tenant: Uuid::parse_str("0195f48b-d000-7000-8000-000000000100").unwrap(),
            kind,
            key: key.to_string(),
            attempt,
            input: json!({}),
        }
    }

    fn meter(cap: Capability, unit: UsageUnit, units: Option<i64>, funding: Funding) -> Meter {
        Meter {
            capability: cap,
            provider: "mistral".into(),
            model: "mistral-ocr-latest".into(),
            funding,
            usage: Usage { units, unit },
            cost_micros: units.map(|u| u * 1_500),
            latency_ms: 812,
            at: DateTime::parse_from_rfc3339("2026-10-16T19:00:00.123456Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn extra(ev: &CostEvent) -> Value {
        serde_json::from_str(&ev["usage_extra"]).unwrap()
    }

    #[test]
    fn tenant_level_project_id_matches_billing_vector() {
        let t = Uuid::parse_str("0195f48b-d000-7000-8000-000000000100").unwrap();
        assert_eq!(
            tenant_level_project_id(t).to_string(),
            "00100000-0000-0008-0007-000db84f5910"
        );
        // an involution, and never the tenant itself
        assert_eq!(tenant_level_project_id(tenant_level_project_id(t)), t);
        assert_ne!(tenant_level_project_id(t), t);
    }

    #[test]
    fn event_key_is_job_step_attempt() {
        let s = step(StepKind::OcrPages, "p0-1", 3);
        assert_eq!(
            event_key(&s),
            "0199e1a0-1111-7000-8000-00000000000a:ocr_pages/p0-1:3"
        );
        // a retry is a new attempt, so a new key; the same attempt is the same key
        assert_ne!(
            event_key(&s),
            event_key(&step(StepKind::OcrPages, "p0-1", 4))
        );
        assert_eq!(
            event_key(&s),
            event_key(&step(StepKind::OcrPages, "p0-1", 3))
        );
        // kind is part of the key: two kinds may share a step key
        assert_ne!(
            event_key(&step(StepKind::Caption, "x", 1)),
            event_key(&step(StepKind::TranscribeSegment, "x", 1))
        );
    }

    #[test]
    fn pages_are_the_quantity_without_a_quantity_unit() {
        let s = step(StepKind::OcrPages, "p0-1", 3);
        let ev = cost_event(
            &s,
            &meter(
                Capability::Ocr,
                UsageUnit::Pages,
                Some(2),
                Funding::LocalGpu,
            ),
        );
        let want: [(&str, &str); 18] = [
            ("app_id", ""),
            ("cost_source", "strata-ingest"),
            ("cost_usd", "0.003000"),
            (
                "event_key",
                "0199e1a0-1111-7000-8000-00000000000a:ocr_pages/p0-1:3",
            ),
            ("funding", "local_gpu"),
            ("input_tokens", "2"),
            ("latency_ms", "812"),
            ("license_id", ""),
            ("model", "mistral-ocr-latest"),
            ("output_tokens", "0"),
            ("project_id", "00100000-0000-0008-0007-000db84f5910"),
            ("provider", "mistral"),
            ("provider_request_id", ""),
            ("served_model", "mistral-ocr-latest"),
            ("service_key", "strata-ingest-ocr"),
            ("subject_id", ""),
            ("tenant_id", "0195f48b-d000-7000-8000-000000000100"),
            ("timestamp", "2026-10-16T19:00:00.123456Z"),
        ];
        let got: Vec<(&str, &str)> = ev
            .iter()
            .filter(|(k, _)| **k != "usage_extra")
            .map(|(k, v)| (*k, v.as_str()))
            .collect();
        assert_eq!(got, want);
        assert_eq!(
            extra(&ev),
            json!({
                "capability": "ocr", "unit": "pages", "units": 2, "pages": 2,
                "job_id": "0199e1a0-1111-7000-8000-00000000000a", "step_key": "ocr_pages/p0-1",
            })
        );
        assert!(!ev.contains_key("actor_id"));
        assert!(
            !ev.contains_key("quantity_unit"),
            "pages are not in billing's QUANTITY_UNITS"
        );
    }

    #[test]
    fn tokens_and_seconds_are_the_quantity() {
        let ev = cost_event(
            &step(StepKind::Caption, "a", 1),
            &meter(
                Capability::Vision,
                UsageUnit::Tokens,
                Some(431),
                Funding::PlatformExternal,
            ),
        );
        assert_eq!(
            (
                ev["quantity_unit"].as_str(),
                ev["input_tokens"].as_str(),
                ev["service_key"].as_str(),
                ev["funding"].as_str()
            ),
            ("tokens", "431", "strata-ingest-vision", "platform_external")
        );
        assert_eq!(extra(&ev).get("pages"), None);
        assert_eq!(extra(&ev).get("byo"), None);
        assert_eq!(extra(&ev).get("usage_unknown"), None);

        let ev = cost_event(
            &step(StepKind::TranscribeSegment, "s0", 2),
            &meter(
                Capability::Transcribe,
                UsageUnit::Seconds,
                Some(30),
                Funding::LocalGpu,
            ),
        );
        assert_eq!(
            (ev["quantity_unit"].as_str(), ev["input_tokens"].as_str()),
            ("seconds", "30")
        );
        assert_eq!(ev["service_key"], "strata-ingest-transcribe");
    }

    #[test]
    fn tenant_provider_is_byo_and_unknown_usage_stays_null() {
        let ev = cost_event(
            &step(StepKind::Caption, "a", 1),
            &meter(
                Capability::Vision,
                UsageUnit::Tokens,
                None,
                Funding::TenantByo,
            ),
        );
        assert_eq!(ev["funding"], "tenant_byo");
        let x = extra(&ev);
        assert_eq!(x["byo"], true);
        assert_eq!(x["units"], Value::Null, "unknown usage is null, not 0");
        assert_eq!(x["usage_unknown"], true, "{x}");
        assert_eq!(ev["input_tokens"], "0");
        assert!(!ev.contains_key("cost_usd"), "no cost when unknown");
    }

    #[test]
    fn default_provider_funding_by_host() {
        let cases = [
            ("http://provider.example.internal:9131", "local_gpu", "provider"),
            ("http://provider.example.internal:9131/v1", "local_gpu", "provider"),
            ("http://127.0.0.1:9130/v1", "local_gpu", "local"),
            ("http://10.0.0.4/v1", "local_gpu", "local"),
            ("http://100.64.0.9:9131", "local_gpu", "local"),
            ("http://[::1]:9130", "local_gpu", "local"),
            ("http://fake:9130", "local_gpu", "fake"),
            (
                "http://vllm.example.svc.cluster.local:8001",
                "local_gpu",
                "vllm",
            ),
            ("https://api.mistral.ai/v1", "platform_external", "mistral"),
            ("https://8.8.8.8/v1", "platform_external", "mistral"),
            ("not a url", "platform_external", "mistral"),
        ];
        for (url, funding, provider) in cases {
            let (f, p) = default_billing(url, "mistral");
            assert_eq!((f.as_str(), p.as_str()), (funding, provider), "{url}");
        }
    }

    #[test]
    fn cfg_off_without_url_and_defaults_the_stream() {
        let env = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(MeteringCfg::from_map(&env(&[])).unwrap().is_none());
        assert!(
            MeteringCfg::from_map(&env(&[(REDIS_URL_ENV, "  ")]))
                .unwrap()
                .is_none()
        );
        let c = MeteringCfg::from_map(&env(&[(REDIS_URL_ENV, "redis://:pw@h:6379/0")]))
            .unwrap()
            .unwrap();
        assert_eq!(c.stream, "billing:costs");
        assert!(!format!("{c:?}").contains("pw@"), "{c:?}");
        let c = MeteringCfg::from_map(&env(&[
            (REDIS_URL_ENV, "redis://h:6379"),
            (STREAM_ENV, "s:x"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(c.stream, "s:x");
        assert!(MeteringCfg::from_map(&env(&[(REDIS_URL_ENV, "http://h")])).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_allows_one_warning_per_period_and_counts_the_rest() {
        let mut t = Throttle::new(Duration::from_secs(60));
        assert_eq!(t.ready(), Some(0));
        assert_eq!(t.ready(), None);
        assert_eq!(t.ready(), None);
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            t.ready(),
            Some(2),
            "the suppressed count is reported, then reset"
        );
        assert_eq!(t.ready(), None);
    }

    #[tokio::test]
    async fn sender_reports_an_unreachable_redis_as_an_error() {
        let cfg = MeteringCfg {
            redis_url: Secret::new("redis://127.0.0.1:1/0"),
            stream: DEFAULT_STREAM.into(),
            interval: DRAIN_INTERVAL,
        };
        let mut s = Sender::new(&cfg).unwrap();
        let fields: BTreeMap<String, String> = [("event_key".to_string(), "k".to_string())].into();
        let e = s.send(&fields).await.expect_err("refused");
        assert!(
            e.transport,
            "a refused connection is a transport failure: {e:?}"
        );
        assert!(s.conn.is_none(), "a transport failure drops the connection");
    }
}
