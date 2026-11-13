//! The billing cost-event outbox (`metering_outbox`, migration 0005): one row per paid provider
//! call, recorded independently of the step's completion; delivered by the drainer to a live
//! Redis stream, with no transaction open across Redis calls. Real Postgres 18.
//!
//! Redis: `REDIS_URL` when set (CI: the archetype's `redis_image`, started in dind), otherwise
//! one throwaway container per test binary from Docker Hub. Never skipped.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use common::{STORAGE_BUCKET, TestDb};
use serde_json::{Value, json};
use sqlx::PgPool;
use strata_ingest::IngestError;
use strata_ingest::config::{Config, Mode, ProvidersFile, Region, Secret};
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::outbox;
use strata_ingest::db::steps::{self, ClaimedStep, StepKind, StepOutput};
use strata_ingest::limits::{DEFAULT_MAX_ATTEMPTS, Limits};
use strata_ingest::metering::{
    self, DRAIN_LEASE, DrainReport, Funding, Meter, MeteringCfg, RETENTION, Sender, cost_event,
};
use strata_ingest::pipeline::Ctx;
use strata_ingest::providers::{Capability, Usage, UsageUnit};
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::OnceCell;
use uuid::Uuid;

type Ev = BTreeMap<String, String>;

// ---------------------------------------------------------------------------------------------
// Redis

const REDIS_IMAGE: &str = "redis";
const REDIS_TAG: &str = "8.10.1-alpine";

struct RedisServer {
    url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

static REDIS: OnceCell<RedisServer> = OnceCell::const_new();
static REDIS_CONTAINER_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();

extern "C" fn remove_redis_container() {
    if let Some(id) = REDIS_CONTAINER_ID.get() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", "-v", id])
            .output();
    }
}

async fn redis_url() -> &'static str {
    &REDIS
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("REDIS_URL")
                && !url.trim().is_empty()
            {
                return RedisServer {
                    url: url.trim().to_string(),
                    _container: None,
                };
            }
            let c = GenericImage::new(REDIS_IMAGE, REDIS_TAG)
                .with_exposed_port(6379.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
                .start()
                .await
                .expect("start a redis container (is Docker running?)");
            if REDIS_CONTAINER_ID.set(c.id().to_string()).is_ok() {
                // SAFETY: registering a plain `extern "C" fn()` with atexit.
                unsafe { libc::atexit(remove_redis_container) };
            }
            let port = c.get_host_port_ipv4(6379.tcp()).await.expect("redis port");
            RedisServer {
                url: format!("redis://127.0.0.1:{port}/"),
                _container: Some(c),
            }
        })
        .await
        .url
}

/// A fresh stream per test: tests share the server.
fn stream() -> String {
    format!("billing:costs:test:{}", Uuid::now_v7())
}

async fn sender(stream: &str) -> Sender {
    Sender::new(&cfg(redis_url().await, stream)).unwrap()
}

fn cfg(url: &str, stream: &str) -> MeteringCfg {
    MeteringCfg {
        redis_url: Secret::new(url),
        stream: stream.to_string(),
        interval: metering::DRAIN_INTERVAL,
    }
}

/// A sender whose Redis refuses connections: an outage.
fn down_sender(stream: &str) -> Sender {
    Sender::new(&cfg("redis://127.0.0.1:1/0", stream)).unwrap()
}

/// Every entry of `stream`, oldest first.
async fn entries(stream: &str) -> Vec<Ev> {
    let client = redis::Client::open(redis_url().await).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let rows: Vec<(String, HashMap<String, String>)> = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    rows.into_iter()
        .map(|(_, f)| f.into_iter().collect())
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Postgres

fn tenant() -> Uuid {
    Uuid::parse_str("0199f0a0-1111-7000-8000-000000000002").unwrap()
}

/// A new job, its Acquire step claimed.
async fn claimed(pool: &PgPool) -> ClaimedStep {
    jobs::create(
        pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "url", "url": "https://example.com/a.pdf"}),
            options: json!({"ocr": "auto", "captions": false, "language": null}),
        },
    )
    .await
    .unwrap();
    steps::claim_next(pool, DEFAULT_MAX_ATTEMPTS)
        .await
        .unwrap()
        .expect("the job's step is claimable")
}

fn a_meter() -> Meter {
    Meter {
        capability: Capability::Ocr,
        provider: "mlx-api".into(),
        model: "ocr-m".into(),
        funding: Funding::LocalGpu,
        usage: Usage {
            units: Some(3),
            unit: UsageUnit::Pages,
        },
        cost_micros: Some(4_500),
        latency_ms: 20,
        at: chrono::Utc::now(),
    }
}

fn done() -> StepOutput {
    StepOutput {
        output: json!({}),
        ..StepOutput::default()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Row {
    event_key: String,
    tenant: Uuid,
    fields: Ev,
    status: String,
    attempts: i32,
    last_error: Option<String>,
    sent: bool,
}

async fn rows(pool: &PgPool) -> Vec<Row> {
    sqlx::query_as::<
        _,
        (
            String,
            Uuid,
            sqlx::types::Json<Ev>,
            String,
            i32,
            Option<String>,
            bool,
        ),
    >(
        "SELECT event_key, tenant, fields, status, attempts, last_error, sent_at IS NOT NULL
         FROM metering_outbox ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(
        |(event_key, tenant, f, status, attempts, last_error, sent)| Row {
            event_key,
            tenant,
            fields: f.0,
            status,
            attempts,
            last_error,
            sent,
        },
    )
    .collect()
}

/// `n` pending events with distinct keys, recorded the way a paid provider call is.
async fn seed(pool: &PgPool, n: usize) -> Vec<String> {
    let step = claimed(pool).await;
    let mut keys = Vec::new();
    for i in 0..n {
        let s = ClaimedStep {
            key: format!("p{i}"),
            ..step.clone()
        };
        metering::record(pool, &s, &a_meter()).await.unwrap();
        keys.push(metering::event_key(&s));
    }
    keys
}

fn stringly(ev: &BTreeMap<&'static str, String>) -> Ev {
    ev.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

async fn set_due_now(pool: &PgPool) {
    sqlx::query("UPDATE metering_outbox SET next_attempt_at = now()")
        .execute(pool)
        .await
        .unwrap();
}

async fn seconds_until_due(pool: &PgPool, key: &str) -> f64 {
    sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM next_attempt_at - now())::float8 FROM metering_outbox
         WHERE event_key = $1",
    )
    .bind(key)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------------------------
// Recording (the production path through a real provider step is in tests/enrich.rs)

#[tokio::test]
async fn record_writes_the_frozen_event_under_the_unchanged_key() {
    let db = TestDb::start().await;
    let step = claimed(&db.pool).await;
    let m = a_meter();
    metering::record(&db.pool, &step, &m).await.unwrap();
    let got = rows(&db.pool).await;
    assert_eq!(got.len(), 1, "{got:#?}");
    let r = &got[0];
    // The key billing dedups on is unchanged by the switch to the outbox.
    assert_eq!(r.event_key, metering::event_key(&step));
    assert_eq!(
        r.event_key,
        format!(
            "{}:{}/{}:{}",
            step.job_id,
            step.kind.as_str(),
            step.key,
            step.attempt
        )
    );
    assert_eq!(r.tenant, tenant());
    assert_eq!(
        r.fields,
        stringly(&cost_event(&step, &m)),
        "frozen field map"
    );
    assert_eq!(
        (r.status.as_str(), r.attempts, r.sent),
        ("pending", 0, false)
    );
}

#[tokio::test]
async fn an_unrecorded_call_is_written_by_the_completion_transaction() {
    let db = TestDb::start().await;
    let step = claimed(&db.pool).await;
    let out = StepOutput {
        unrecorded: Some(a_meter()),
        ..done()
    };
    steps::complete_and_enqueue(&db.pool, &step, out, &[])
        .await
        .unwrap();
    let got = rows(&db.pool).await;
    assert_eq!(
        got.iter().map(|r| r.event_key.clone()).collect::<Vec<_>>(),
        [metering::event_key(&step)]
    );
    assert_eq!(
        got[0].fields,
        stringly(&cost_event(&step, &a_meter_at(&got[0])))
    );
}

/// `a_meter()` with the `timestamp` of a stored row (the meter's `at` is `now()`).
fn a_meter_at(r: &Row) -> Meter {
    Meter {
        at: chrono::DateTime::parse_from_rfc3339(&r.fields["timestamp"])
            .unwrap()
            .with_timezone(&chrono::Utc),
        ..a_meter()
    }
}

#[tokio::test]
async fn an_unrecorded_call_on_a_fenced_out_completion_is_left_to_the_caller() {
    let db = TestDb::start().await;
    let step = claimed(&db.pool).await;
    sqlx::query("UPDATE ingest_steps SET attempt = attempt + 1 WHERE id = $1")
        .bind(step.id)
        .execute(&db.pool)
        .await
        .unwrap();
    let out = StepOutput {
        unrecorded: Some(a_meter()),
        ..done()
    };
    let r = steps::complete_and_enqueue(&db.pool, &step, out, &[]).await;
    assert!(matches!(r, Err(IngestError::LeaseLost { .. })), "{r:?}");
    // Nothing written: the worker's `record_or_log_lost` takes over (tests/enrich.rs).
    assert_eq!(rows(&db.pool).await, vec![]);
}

#[tokio::test]
async fn recording_the_same_call_twice_is_idempotent() {
    let db = TestDb::start().await;
    let step = claimed(&db.pool).await;
    let m = a_meter();
    metering::record(&db.pool, &step, &m).await.unwrap();
    // A retried insert that had in fact committed: same call, same key, no second row.
    metering::record(&db.pool, &step, &m).await.unwrap();
    let got = rows(&db.pool).await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].fields, stringly(&cost_event(&step, &m)));
}

/// A step attempt makes at most one paid call: a second, different call under the same
/// `event_key` would be dropped by the dedup, so it fails loudly (debug builds).
#[tokio::test]
#[should_panic(expected = "two different paid calls under event_key")]
async fn a_second_different_call_under_one_event_key_is_loud() {
    let db = TestDb::start().await;
    let step = claimed(&db.pool).await;
    metering::record(&db.pool, &step, &a_meter()).await.unwrap();
    let mut other = a_meter();
    other.model = "a-second-call".into();
    let _ = metering::record(&db.pool, &step, &other).await;
}

// ---------------------------------------------------------------------------------------------
// The drainer

#[tokio::test]
async fn the_drainer_delivers_to_the_stream_and_marks_rows_sent() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 3).await;
    let want: Vec<Ev> = rows(&db.pool).await.into_iter().map(|r| r.fields).collect();
    let s = stream();
    let mut tx = sender(&s).await;
    let r = metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(
        r,
        DrainReport {
            claimed: 3,
            sent: 3,
            ..DrainReport::default()
        }
    );
    assert_eq!(entries(&s).await, want, "the frozen maps, in order");
    let got = rows(&db.pool).await;
    assert!(
        got.iter()
            .all(|r| r.status == "sent" && r.sent && r.last_error.is_none()),
        "{got:#?}"
    );
    assert_eq!(
        got.iter().map(|r| r.event_key.clone()).collect::<Vec<_>>(),
        keys
    );
    let r = metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(r.claimed, 0, "nothing left to send");
    assert_eq!(entries(&s).await.len(), 3);
}

#[tokio::test]
async fn a_row_redis_refuses_does_not_stop_the_batch() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 3).await;
    // The middle row's map is empty: `XADD <stream> MAXLEN ~ n *` with no fields is a Redis
    // command error for that row only.
    sqlx::query("UPDATE metering_outbox SET fields = '{}' WHERE event_key = $1")
        .bind(&keys[1])
        .execute(&db.pool)
        .await
        .unwrap();
    let s = stream();
    let r = metering::drain_once(&db.pool, &mut sender(&s).await, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(
        (r.claimed, r.sent, r.failed, r.released),
        (3, 2, 1, 0),
        "{r:?}"
    );
    let got: HashSet<String> = entries(&s)
        .await
        .iter()
        .map(|e| e["event_key"].clone())
        .collect();
    assert_eq!(got, HashSet::from([keys[0].clone(), keys[2].clone()]));
    let bad = rows(&db.pool).await.remove(1);
    assert_eq!((bad.status.as_str(), bad.attempts), ("pending", 1));
    assert!(bad.last_error.is_some());
}

#[tokio::test]
async fn a_redis_outage_keeps_the_rows_and_they_go_out_after_recovery() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 2).await;
    let s = stream();
    let r = metering::drain_once(&db.pool, &mut down_sender(&s), 100, DRAIN_LEASE)
        .await
        .unwrap();
    // The connection is down: the first row fails, the untried second is given back.
    assert_eq!(
        (r.claimed, r.sent, r.failed, r.released),
        (2, 0, 1, 1),
        "{r:?}"
    );
    assert!(r.error.is_some());
    let got = rows(&db.pool).await;
    assert!(
        got.iter().all(|r| r.status == "pending" && !r.sent),
        "{got:#?}"
    );
    assert_eq!((got[0].attempts, got[1].attempts), (1, 0), "{got:#?}");
    assert!(got[0].last_error.is_some());
    // Both backed off by BACKOFF_BASE (5 s): not due yet, not retried early.
    for k in &keys {
        let w = seconds_until_due(&db.pool, k).await;
        assert!((3.0..=5.5).contains(&w), "{k}: due in {w}s");
    }
    let mut up = sender(&s).await;
    assert_eq!(
        metering::drain_once(&db.pool, &mut up, 100, DRAIN_LEASE)
            .await
            .unwrap()
            .claimed,
        0
    );
    // A second failure of the same row doubles its backoff.
    set_due_now(&db.pool).await;
    metering::drain_once(&db.pool, &mut down_sender(&s), 100, DRAIN_LEASE)
        .await
        .unwrap();
    let w = seconds_until_due(&db.pool, &keys[0]).await;
    assert!((8.0..=10.5).contains(&w), "second backoff {w}s");
    // Redis is back and the backoff has passed: everything is delivered, nothing was lost.
    set_due_now(&db.pool).await;
    let r = metering::drain_once(&db.pool, &mut up, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!((r.sent, r.failed), (2, 0), "{r:?}");
    assert_eq!(entries(&s).await.len(), 2);
    assert!(
        rows(&db.pool)
            .await
            .iter()
            .all(|r| r.status == "sent" && r.last_error.is_none())
    );
}

/// A TCP server that accepts connections and never answers: a hung Redis.
async fn black_hole() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = l.accept().await {
            held.push(sock);
        }
    });
    format!("redis://127.0.0.1:{port}/0")
}

#[tokio::test]
async fn no_transaction_is_open_while_the_drainer_waits_on_redis() {
    let db = TestDb::start().await;
    seed(&db.pool, 2).await;
    let mut hung = Sender::new(&cfg(&black_hole().await, &stream())).unwrap();
    let pool = db.pool.clone();
    let pass = tokio::spawn(async move {
        metering::drain_once(&pool, &mut hung, 100, DRAIN_LEASE)
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !pass.is_finished(),
        "the hung Redis is still being waited on"
    );
    let open: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity
         WHERE datname = current_database() AND state LIKE 'idle in transaction%'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(open, 0, "the claim committed before the XADD");
    let r = pass.await.unwrap();
    assert_eq!((r.sent, r.failed, r.released), (0, 1, 1), "{r:?}");
}

#[tokio::test]
async fn a_claimed_row_is_not_claimed_again_until_its_lease_passes_then_it_is_resent() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 2).await;
    let s = stream();
    let mut tx = sender(&s).await;
    // A drainer claims, XADDs, and dies before marking the rows sent.
    let claimed = outbox::claim(&db.pool, 100, DRAIN_LEASE).await.unwrap();
    assert_eq!(claimed.len(), 2);
    for row in &claimed {
        tx.send(&row.fields).await.unwrap();
    }
    // While the lease runs, no other drainer takes them.
    let r = metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(r.claimed, 0, "{r:?}");
    for k in &keys {
        let w = seconds_until_due(&db.pool, k).await;
        assert!(w > 50.0, "leased for DRAIN_LEASE: due in {w}s");
    }
    // The lease has passed: they are due again and re-sent.
    set_due_now(&db.pool).await;
    let r = metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(r.sent, 2, "{r:?}");
    assert!(rows(&db.pool).await.iter().all(|r| r.status == "sent"));
    // At-least-once: each event is on the stream twice, under the SAME event_key, which is what
    // billing dedups on, so each call is stored once.
    let got = entries(&s).await;
    assert_eq!(got.len(), 4);
    let mut per_key: HashMap<String, usize> = HashMap::new();
    for e in &got {
        *per_key.entry(e["event_key"].clone()).or_default() += 1;
    }
    assert_eq!(
        per_key,
        keys.iter()
            .map(|k| (k.clone(), 2))
            .collect::<HashMap<_, _>>()
    );
}

#[tokio::test]
async fn a_drainer_skips_rows_another_transaction_holds_without_waiting() {
    let db = TestDb::start().await;
    seed(&db.pool, 4).await;
    let s = stream();
    let mut holder = db.pool.begin().await.unwrap();
    let held: Vec<i64> = sqlx::query_scalar("SELECT id FROM metering_outbox FOR UPDATE")
        .fetch_all(&mut *holder)
        .await
        .unwrap();
    assert_eq!(held.len(), 4);
    let mut tx = sender(&s).await;
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE),
    )
    .await
    .expect("SKIP LOCKED: the claim does not block on held rows")
    .unwrap();
    assert_eq!(r.claimed, 0, "{r:?}");
    assert!(entries(&s).await.is_empty());
    holder.rollback().await.unwrap();
    let r = metering::drain_once(&db.pool, &mut tx, 100, DRAIN_LEASE)
        .await
        .unwrap();
    assert_eq!(r.sent, 4);
}

#[tokio::test]
async fn concurrent_drainers_never_send_a_row_twice() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 60).await;
    let s = Arc::new(stream());
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let (pool, s) = (db.pool.clone(), s.clone());
        tasks.push(tokio::spawn(async move {
            let mut tx = sender(&s).await;
            let mut sent = 0;
            loop {
                let r = metering::drain_once(&pool, &mut tx, 5, DRAIN_LEASE)
                    .await
                    .unwrap();
                assert_eq!(r.error, None);
                if r.claimed == 0 {
                    return sent;
                }
                sent += r.sent;
            }
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await.unwrap();
    }
    assert_eq!(total, 60);
    let got = entries(&s).await;
    assert_eq!(got.len(), 60, "every row sent exactly once");
    let distinct: HashSet<String> = got.iter().map(|e| e["event_key"].clone()).collect();
    assert_eq!(distinct, keys.into_iter().collect::<HashSet<_>>());
}

#[tokio::test]
async fn prune_deletes_only_sent_rows_past_retention() {
    let db = TestDb::start().await;
    let keys = seed(&db.pool, 3).await;
    // keys[0]: sent 8 days ago; keys[1]: sent just now; keys[2]: pending and old.
    sqlx::query(
        "UPDATE metering_outbox SET status = 'sent', sent_at = now() - interval '8 days'
         WHERE event_key = $1",
    )
    .bind(&keys[0])
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE metering_outbox SET status = 'sent', sent_at = now() WHERE event_key = $1")
        .bind(&keys[1])
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE metering_outbox SET created_at = now() - interval '30 days' WHERE event_key = $1",
    )
    .bind(&keys[2])
    .execute(&db.pool)
    .await
    .unwrap();
    assert_eq!(RETENTION, Duration::from_secs(7 * 24 * 3600));
    assert_eq!(
        outbox::prune_sent(&db.pool, RETENTION, 100).await.unwrap(),
        1
    );
    let left: Vec<String> = rows(&db.pool)
        .await
        .into_iter()
        .map(|r| r.event_key)
        .collect();
    assert_eq!(left, vec![keys[1].clone(), keys[2].clone()]);
}

// ---------------------------------------------------------------------------------------------
// Production wiring: `serve` starts the drainer, /health shows the backlog, shutdown drains

/// Nothing listens on port 9 (discard).
const UNUSED_URL: &str = "http://127.0.0.1:9";

fn serve_config(db: &TestDb, metering: Option<MeteringCfg>) -> Config {
    Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![(tenant(), Secret::new("api-token"))],
        storage_url: UNUSED_URL.to_string(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: UNUSED_URL.to_string(),
        webscraper_url: UNUSED_URL.to_string(),
        webscraper_token: None,
        strata_url: UNUSED_URL.to_string(),
        strata_token: Secret::new("strata-token"),
        workers: 1,
        limits: Limits::default(),
        providers: ProvidersFile {
            region: Region::Eu,
            mode: Mode::Dev,
            allow_internal_provider_hosts: false,
            ocr: None,
            vision: None,
            transcribe: None,
        },
        provider_key: None,
        metering,
    }
}

async fn wait_for(what: &str, secs: u64, mut ok: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !ok().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn serve_starts_the_drainer_reports_the_backlog_and_drains_on_shutdown() {
    let db = TestDb::start().await;
    let s = stream();
    // A pass only at start and at shutdown: the interval never elapses during the test.
    let m = MeteringCfg {
        interval: Duration::from_secs(3600),
        ..cfg(redis_url().await, &s)
    };
    let before = seed(&db.pool, 1).await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), serve_config(&db, Some(m))).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(strata_ingest::api::serve_starting(
        ctx,
        listener,
        async move {
            let _ = stop_rx.await;
        },
        async { Ok(()) },
    ));
    // The drainer runs: its start pass delivers the row that was waiting.
    wait_for("the start pass delivers", 30, async || {
        entries(&s).await.len() == 1
    })
    .await;
    assert_eq!(entries(&s).await[0]["event_key"], before[0]);
    // A call paid while serving: no pass is due for an hour, so it waits in the outbox...
    // (a step of its own, so the running workers have nothing to claim from this test)
    let step = ClaimedStep {
        id: 0,
        job_id: Uuid::now_v7(),
        tenant: tenant(),
        kind: StepKind::OcrPages,
        key: "late".into(),
        attempt: 1,
        input: json!({}),
    };
    metering::record(&db.pool, &step, &a_meter()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(entries(&s).await.len(), 1);
    // ...and /health shows it as backlog.
    let h: Value = reqwest::get(format!("{url}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h["metering"]["transport"], "on", "{h}");
    assert_eq!(h["metering"]["pending"], 1, "{h}");
    assert!(
        h["metering"]["oldest_pending_age_s"].as_i64().unwrap() >= 0,
        "{h}"
    );
    // Shutdown: the drainer's last pass delivers it before serve returns.
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(60), server)
        .await
        .expect("serve stops")
        .unwrap()
        .expect("serve ok");
    let got = entries(&s).await;
    assert_eq!(got.len(), 2, "{got:#?}");
    assert_eq!(got[1]["event_key"], metering::event_key(&step));
    assert!(rows(&db.pool).await.iter().all(|r| r.status == "sent"));
}

#[tokio::test]
async fn serve_without_a_transport_keeps_events_and_reports_them() {
    let db = TestDb::start().await;
    seed(&db.pool, 2).await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), serve_config(&db, None)).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(strata_ingest::api::serve_starting(
        ctx,
        listener,
        async move {
            let _ = stop_rx.await;
        },
        async { Ok(()) },
    ));
    let h: Value = reqwest::get(format!("{url}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h["metering"]["transport"], "off", "{h}");
    assert_eq!(h["metering"]["pending"], 2, "{h}");
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(60), server)
        .await
        .expect("serve stops")
        .unwrap()
        .expect("serve ok");
    assert!(
        rows(&db.pool).await.iter().all(|r| r.status == "pending"),
        "kept for when a transport is configured"
    );
}
