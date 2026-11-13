//! Job/step schema and the SKIP LOCKED stage-gated step queue (Task 2). Real Postgres 18.

mod common;

use std::collections::HashSet;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use common::TestDb;
use serde_json::{Value, json};
use sqlx::PgPool;
use strata_ingest::IngestError;
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::steps::{self, StepKind, StepOutput, backoff, retry_delay};
use strata_ingest::error::StepError;
use strata_ingest::limits::DEFAULT_MAX_ATTEMPTS;
use strata_ingest::providers::ErrorClass;
use tokio::sync::Barrier;
use uuid::Uuid;

/// The real default (`Limits.max_attempts`), so tests pin production retry behaviour.
const MAX: i32 = DEFAULT_MAX_ATTEMPTS;

fn tenant() -> Uuid {
    Uuid::parse_str("0199f0a0-1111-7000-8000-000000000001").unwrap()
}

fn new_job() -> NewJob {
    NewJob {
        tenant: tenant(),
        store_id: Uuid::now_v7(),
        source: json!({"kind": "url", "url": "https://example.com/a.pdf"}),
        options: json!({"ocr": "auto", "captions": false, "language": null}),
    }
}

/// A job whose only steps are `steps` (the `Acquire` step `create` adds is removed).
async fn job_with(pool: &PgPool, steps_: &[(StepKind, &str)]) -> Uuid {
    let id = jobs::create(pool, new_job()).await.expect("create job");
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM ingest_steps WHERE job_id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    for (kind, key) in steps_ {
        steps::enqueue(&mut tx, id, *kind, key, json!({"k": key}))
            .await
            .expect("enqueue");
    }
    tx.commit().await.unwrap();
    id
}

async fn step_status(pool: &PgPool, id: i64) -> String {
    sqlx::query_scalar("SELECT status FROM ingest_steps WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn job_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

fn err(class: ErrorClass, code: &str) -> StepError {
    StepError {
        class,
        code: code.to_string(),
        message: format!("{code} happened"),
        provider: Some("ocr".to_string()),
        retry_after_s: None,
    }
}

fn out() -> StepOutput {
    StepOutput {
        output: json!({"ok": true}),
        provider: None,
        model: None,
        usage: None,
        cost_micros: None,
        skipped: false,
        unrecorded: None,
    }
}

#[test]
fn step_kinds_round_trip_with_stages() {
    let all = [
        (StepKind::Discover, "discover", 5),
        (StepKind::Acquire, "acquire", 10),
        (StepKind::Scan, "scan", 20),
        (StepKind::Detect, "detect", 30),
        (StepKind::Convert, "convert", 40),
        (StepKind::OcrPages, "ocr_pages", 50),
        (StepKind::Caption, "caption", 50),
        (StepKind::TranscribeSegment, "transcribe_segment", 50),
        (StepKind::Bundle, "bundle", 60),
        (StepKind::Deliver, "deliver", 70),
    ];
    for (kind, s, stage) in all {
        assert_eq!(kind.as_str(), s);
        assert_eq!(StepKind::parse(s), Some(kind));
        assert_eq!(kind.stage(), stage);
    }
    assert_eq!(StepKind::parse("nope"), None);
}

#[tokio::test]
async fn create_makes_a_queued_job_with_an_acquire_step() {
    let db = TestDb::start().await;
    let id = jobs::create(&db.pool, new_job()).await.unwrap();
    let view = jobs::get(&db.pool, tenant(), id).await.unwrap();
    assert_eq!(view.job.status, "queued");
    assert_eq!(view.steps.len(), 1);
    assert_eq!(view.steps[0].kind, "acquire");
    assert_eq!(view.steps[0].status, "pending");
    // Another tenant cannot see it.
    let other = Uuid::now_v7();
    assert!(matches!(
        jobs::get(&db.pool, other, id).await,
        Err(IngestError::NotFound)
    ));
    // The nil tenant is never a tenant.
    let mut nil = new_job();
    nil.tenant = Uuid::nil();
    assert!(matches!(
        jobs::create(&db.pool, nil).await,
        Err(IngestError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn later_stage_waits_for_earlier() {
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::Scan, ""), (StepKind::Convert, "")]).await;
    let first = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("a claim");
    assert_eq!(first.kind, StepKind::Scan);
    assert_eq!(first.job_id, job);
    assert_eq!(first.tenant, tenant());
    assert_eq!(first.attempt, 1);
    assert_eq!(first.input, json!({"k": ""}));
    assert_eq!(job_status(&db.pool, job).await, "running");
    assert!(
        steps::claim_next(&db.pool, MAX).await.unwrap().is_none(),
        "Convert (40) must wait for Scan (20)"
    );
    // Completing Scan opens the gate.
    steps::complete_and_enqueue(&db.pool, &first, out(), &[])
        .await
        .unwrap();
    let second = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("convert");
    assert_eq!(second.kind, StepKind::Convert);
}

#[tokio::test]
async fn parallel_stage_steps_are_all_claimable() {
    let db = TestDb::start().await;
    job_with(
        &db.pool,
        &[
            (StepKind::OcrPages, "1-10"),
            (StepKind::OcrPages, "11-20"),
            (StepKind::OcrPages, "21-30"),
            (StepKind::Bundle, ""),
        ],
    )
    .await;
    let mut ids = HashSet::new();
    let mut keys = HashSet::new();
    for _ in 0..3 {
        let c = steps::claim_next(&db.pool, MAX)
            .await
            .unwrap()
            .expect("claim");
        assert_eq!(c.kind, StepKind::OcrPages);
        ids.insert(c.id);
        keys.insert(c.key);
    }
    assert_eq!(ids.len(), 3);
    assert_eq!(keys.len(), 3);
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
}

#[tokio::test]
async fn enqueue_is_idempotent() {
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::Scan, ""), (StepKind::Scan, "")]).await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_steps WHERE job_id = $1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_claims_never_duplicate() {
    let db = TestDb::start().await;
    // Several rounds, each: 20 pending steps over 5 jobs (all stage 50), 8 claimers released
    // together by a barrier on a multi-threaded runtime so their claims really overlap.
    for round in 0..5 {
        for j in 0..5 {
            let keys: Vec<String> = (0..4).map(|k| format!("{round}-{j}-{k}")).collect();
            let s: Vec<(StepKind, &str)> = keys
                .iter()
                .map(|k| (StepKind::OcrPages, k.as_str()))
                .collect();
            job_with(&db.pool, &s).await;
        }
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = db.pool.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let mut mine = Vec::new();
                while let Some(c) = steps::claim_next(&pool, MAX).await.expect("claim") {
                    mine.push((c.id, c.attempt));
                }
                mine
            }));
        }
        let mut all = Vec::new();
        for t in tasks {
            all.extend(t.await.unwrap());
        }
        let unique: HashSet<i64> = all.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            all.len(),
            20,
            "round {round}: every step claimed once: {all:?}"
        );
        assert_eq!(unique.len(), 20, "round {round}: no duplicate claims");
        assert!(all.iter().all(|(_, a)| *a == 1), "no step claimed twice");
    }
}

async fn expire_lease(pool: &PgPool, id: i64) {
    sqlx::query("UPDATE ingest_steps SET lease_until = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

/// Makes a backed-off pending step due now.
async fn make_due(pool: &PgPool, id: i64) {
    sqlx::query("UPDATE ingest_steps SET run_after = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn seconds_until_run_after(pool: &PgPool, id: i64) -> f64 {
    sqlx::query_scalar(
        "SELECT extract(epoch FROM run_after - now())::float8 FROM ingest_steps WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn expired_lease_is_reclaimed() {
    let db = TestDb::start().await;
    job_with(&db.pool, &[(StepKind::Convert, "")]).await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert!(
        steps::claim_next(&db.pool, MAX).await.unwrap().is_none(),
        "leased"
    );
    // A heartbeat on a live lease succeeds.
    steps::heartbeat(&db.pool, &c).await.unwrap();
    expire_lease(&db.pool, c.id).await;
    // The reap puts it back to pending with the attempt's backoff, not claimable at once.
    assert!(
        steps::claim_next(&db.pool, MAX).await.unwrap().is_none(),
        "reaped step waits out backoff(1)"
    );
    assert_eq!(step_status(&db.pool, c.id).await, "pending");
    let delay = seconds_until_run_after(&db.pool, c.id).await;
    assert!(
        (1.0..=2.1).contains(&delay),
        "run_after ≈ now+2s, got {delay}"
    );
    make_due(&db.pool, c.id).await;
    let again = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("reclaimed");
    assert_eq!(again.id, c.id);
    assert_eq!(again.attempt, 2);
}

#[tokio::test]
async fn expired_lease_after_max_attempts_fails_job() {
    // A document that kills its worker on every attempt must stop after max_attempts (10).
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::Convert, "")]).await;
    for attempt in 1..=MAX {
        let c = steps::claim_next(&db.pool, MAX)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("attempt {attempt} claimable"));
        assert_eq!(c.attempt, attempt);
        expire_lease(&db.pool, c.id).await;
        if attempt < MAX {
            assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
            assert_eq!(step_status(&db.pool, c.id).await, "pending");
            make_due(&db.pool, c.id).await;
        }
    }
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    let view = jobs::get(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "failed");
    assert_eq!(view.steps[0].status, "failed");
    assert_eq!(view.steps[0].attempt, MAX);
    let e = view.job.error.expect("job error");
    assert_eq!(e["code"], "lease_expired");
    assert_eq!(e["class"], "retryable");
    assert!(
        e["message"].as_str().unwrap().contains("convert"),
        "names the step kind: {e}"
    );
}

#[tokio::test]
async fn stale_worker_writes_are_fenced() {
    // Worker A stalls past its lease; the step is reaped and re-claimed by worker B. Every write
    // A then attempts is refused with LeaseLost and changes nothing; B's claim stays live.
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::OcrPages, "1-10")]).await;
    let a = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    expire_lease(&db.pool, a.id).await;
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    make_due(&db.pool, a.id).await;
    let b = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!((b.id, b.attempt), (a.id, 2));
    let snapshot = || async {
        sqlx::query_as::<_, (String, i32, Option<Value>, Option<Value>, String)>(
            "SELECT s.status, s.attempt, s.output, s.error, j.status
             FROM ingest_steps s JOIN ingest_jobs j ON j.id = s.job_id WHERE s.id = $1",
        )
        .bind(a.id)
        .fetch_one(&db.pool)
        .await
        .unwrap()
    };
    let before = snapshot().await;
    let lost = |r: strata_ingest::Result<()>| {
        assert!(
            matches!(r, Err(IngestError::LeaseLost { step, attempt: 1 }) if step == a.id),
            "expected LeaseLost, got {r:?}"
        )
    };
    lost(steps::heartbeat(&db.pool, &a).await);
    lost(
        steps::complete_and_enqueue(
            &db.pool,
            &a,
            out(),
            &[(StepKind::Bundle, String::new(), json!({}))],
        )
        .await,
    );
    lost(
        steps::fail(
            &db.pool,
            &a,
            &err(ErrorClass::Retryable, "rate_limited"),
            MAX,
        )
        .await,
    );
    lost(steps::fail(&db.pool, &a, &err(ErrorClass::InvalidRequest, "bad"), MAX).await);
    assert_eq!(snapshot().await, before, "stale writes changed nothing");
    assert_eq!(before.0, "running");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_steps WHERE job_id = $1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(n, 1, "no follow-up enqueued by the stale worker");
    // B still owns the step.
    steps::heartbeat(&db.pool, &b).await.unwrap();
    steps::complete_and_enqueue(&db.pool, &b, out(), &[])
        .await
        .unwrap();
    assert_eq!(job_status(&db.pool, job).await, "completed");
    lost(steps::heartbeat(&db.pool, &a).await);
}

#[tokio::test]
async fn heartbeat_extends_lease() {
    let db = TestDb::start().await;
    job_with(&db.pool, &[(StepKind::Convert, "")]).await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    sqlx::query("UPDATE ingest_steps SET lease_until = now() + interval '5 seconds' WHERE id = $1")
        .bind(c.id)
        .execute(&db.pool)
        .await
        .unwrap();
    steps::heartbeat(&db.pool, &c).await.unwrap();
    let left: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM lease_until - now())::float8 FROM ingest_steps WHERE id = $1",
    )
    .bind(c.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!((590.0..=601.0).contains(&left), "lease ~10 min, got {left}");
    // A step that is not running has no lease to extend.
    steps::complete_and_enqueue(&db.pool, &c, out(), &[])
        .await
        .unwrap();
    assert!(matches!(
        steps::heartbeat(&db.pool, &c).await,
        Err(IngestError::LeaseLost { .. })
    ));
}

#[tokio::test]
async fn retryable_failure_backs_off() {
    assert_eq!(backoff(1), Duration::from_secs(2));
    assert_eq!(backoff(2), Duration::from_secs(4));
    assert_eq!(backoff(4), Duration::from_secs(16));
    assert_eq!(backoff(9), Duration::from_secs(300));
    assert_eq!(backoff(20), Duration::from_secs(300));
    assert_eq!(backoff(i32::MAX), Duration::from_secs(300));
    assert_eq!(backoff(0), Duration::from_secs(2));

    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::OcrPages, "1-10")]).await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(c.attempt, 1);
    steps::fail(&db.pool, &c, &err(ErrorClass::Retryable, "rate_limited"), 5)
        .await
        .unwrap();
    assert_eq!(step_status(&db.pool, c.id).await, "pending");
    let delay: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM run_after - now())::float8 FROM ingest_steps WHERE id = $1",
    )
    .bind(c.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(
        (1.0..=2.1).contains(&delay),
        "run_after ≈ now+2s, got {delay}"
    );
    let lease: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT lease_until FROM ingest_steps WHERE id = $1")
            .bind(c.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(lease.is_none());
    assert_eq!(job_status(&db.pool, job).await, "running");
    // Not claimable until run_after passes.
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let again = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("after backoff");
    assert_eq!(again.id, c.id);
    assert_eq!(again.attempt, 2);
}

#[tokio::test]
async fn claims_take_turns_between_jobs() {
    // A big document's OCR backlog was enqueued first; a small job arrives later. FIFO would
    // serve all 6 big steps first; fair claiming (R36) serves the small job second.
    let db = TestDb::start().await;
    let big = job_with(
        &db.pool,
        &[
            (StepKind::OcrPages, "p0-7"),
            (StepKind::OcrPages, "p8-15"),
            (StepKind::OcrPages, "p16-23"),
            (StepKind::OcrPages, "p24-31"),
            (StepKind::OcrPages, "p32-39"),
            (StepKind::OcrPages, "p40-47"),
        ],
    )
    .await;
    let small = job_with(&db.pool, &[(StepKind::Acquire, ""), (StepKind::Scan, "")]).await;
    let first = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(first.job_id, big, "oldest first while no job runs anything");
    let second = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(
        second.job_id, small,
        "the job with nothing running goes next"
    );
    let third = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(third.job_id, big, "1 running each: oldest again");
    // The small job completes long before the big job's backlog is through.
    steps::complete_and_enqueue(&db.pool, &second, out(), &[])
        .await
        .unwrap();
    let fourth = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(
        (fourth.job_id, fourth.kind),
        (small, StepKind::Scan),
        "small job (0 running) beats big job (2 running)"
    );
    steps::complete_and_enqueue(&db.pool, &fourth, out(), &[])
        .await
        .unwrap();
    assert_eq!(job_status(&db.pool, small).await, "completed");
    assert_eq!(job_status(&db.pool, big).await, "running");
}

#[test]
fn default_retry_schedule_outlasts_a_dependency_restart() {
    // The retries of one step span >= 10 min (R34): a storage/ClamAV/docling restart or a
    // provider 429 storm of several minutes is survived.
    let total: u64 = (1..MAX).map(|a| backoff(a).as_secs()).sum();
    assert!(total >= 600, "retry window {total} s");
    assert_eq!(MAX, 10);
    // Retry-After lengthens a wait, capped at 300 s, never shortens the backoff.
    assert_eq!(retry_delay(1, Some(120)), Duration::from_secs(120));
    assert_eq!(retry_delay(1, Some(86_400)), Duration::from_secs(300));
    assert_eq!(retry_delay(6, Some(1)), backoff(6));
    assert_eq!(retry_delay(3, None), backoff(3));
}

#[tokio::test]
async fn retry_after_on_a_failure_sets_run_after() {
    let db = TestDb::start().await;
    job_with(&db.pool, &[(StepKind::OcrPages, "1-10")]).await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    let mut e = err(ErrorClass::Retryable, "provider_retryable");
    e.retry_after_s = Some(120);
    steps::fail(&db.pool, &c, &e, MAX).await.unwrap();
    let delay = seconds_until_run_after(&db.pool, c.id).await;
    assert!(
        (119.0..=120.1).contains(&delay),
        "run_after ≈ now+120s, got {delay}"
    );
    let stored: Value = sqlx::query_scalar("SELECT error FROM ingest_steps WHERE id = $1")
        .bind(c.id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(stored["retry_after_s"], 120, "{stored}");
}

#[tokio::test]
async fn fail_after_max_attempts_marks_job_failed() {
    // A 429 storm: the default max_attempts (10) retryable failures, then the job fails.
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::OcrPages, "1-10")]).await;
    let mut last = None;
    for attempt in 1..=MAX {
        let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
        assert_eq!(c.attempt, attempt);
        steps::fail(
            &db.pool,
            &c,
            &err(ErrorClass::Retryable, "rate_limited"),
            MAX,
        )
        .await
        .unwrap();
        if attempt < MAX {
            assert_eq!(
                step_status(&db.pool, c.id).await,
                "pending",
                "attempt {attempt}"
            );
            let expect = backoff(attempt).as_secs_f64();
            let delay = seconds_until_run_after(&db.pool, c.id).await;
            assert!(
                (expect - 1.0..=expect + 0.1).contains(&delay),
                "attempt {attempt}: run_after ≈ now+{expect}s, got {delay}"
            );
            assert_eq!(job_status(&db.pool, job).await, "running");
            make_due(&db.pool, c.id).await;
        }
        last = Some(c);
    }
    let c = last.unwrap();
    assert_eq!(step_status(&db.pool, c.id).await, "failed");
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    let view = jobs::get(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "failed");
    let e = view.job.error.expect("job error");
    assert_eq!(e["code"], "rate_limited");
    assert_eq!(e["class"], "retryable");
    assert_eq!(e["provider"], "ocr");
    assert_eq!(
        view.steps[0].error.as_ref().unwrap()["code"],
        "rate_limited"
    );
    assert_eq!(view.steps[0].attempt, MAX);
    assert!(view.steps[0].finished_at.is_some());
}

#[tokio::test]
async fn failures_on_a_cancelled_job_cancel_the_step() {
    let db = TestDb::start().await;
    let job = job_with(
        &db.pool,
        &[(StepKind::OcrPages, "a"), (StepKind::OcrPages, "b")],
    )
    .await;
    let a = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    let b = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    jobs::cancel(&db.pool, tenant(), job).await.unwrap();
    // A retryable failure must not put the step back to pending.
    steps::fail(
        &db.pool,
        &a,
        &err(ErrorClass::Retryable, "rate_limited"),
        MAX,
    )
    .await
    .unwrap();
    assert_eq!(step_status(&db.pool, a.id).await, "cancelled");
    // An expired lease on a cancelled job is cancelled too, not reclaimed.
    expire_lease(&db.pool, b.id).await;
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    assert_eq!(step_status(&db.pool, b.id).await, "cancelled");
    assert_eq!(job_status(&db.pool, job).await, "cancelled");
}

#[tokio::test]
async fn invalid_request_is_not_retried() {
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::Convert, "")]).await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    steps::fail(
        &db.pool,
        &c,
        &err(ErrorClass::InvalidRequest, "convert_failed"),
        5,
    )
    .await
    .unwrap();
    assert_eq!(step_status(&db.pool, c.id).await, "failed");
    assert_eq!(job_status(&db.pool, job).await, "failed");
    for class in [ErrorClass::Auth, ErrorClass::NotFound] {
        let job = job_with(&db.pool, &[(StepKind::Convert, "")]).await;
        let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
        steps::fail(&db.pool, &c, &err(class, "nope"), 5)
            .await
            .unwrap();
        assert_eq!(job_status(&db.pool, job).await, "failed", "{class:?}");
    }
}

#[tokio::test]
async fn cancelled_job_steps_are_not_claimed() {
    let db = TestDb::start().await;
    let job = job_with(
        &db.pool,
        &[(StepKind::OcrPages, "a"), (StepKind::OcrPages, "b")],
    )
    .await;
    let running = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    jobs::cancel(&db.pool, tenant(), job).await.unwrap();
    // A step is still running: cooperative cancellation.
    assert_eq!(job_status(&db.pool, job).await, "cancelling");
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    // Idempotent while cancelling.
    jobs::cancel(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(job_status(&db.pool, job).await, "cancelling");
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM ingest_steps WHERE job_id = $1 ORDER BY id")
            .bind(job)
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert!(statuses.contains(&"cancelled".to_string()));
    // The in-flight step finishes, enqueues no more work, and the job is cancelled.
    steps::complete_and_enqueue(
        &db.pool,
        &running,
        out(),
        &[(StepKind::Bundle, String::new(), json!({}))],
    )
    .await
    .unwrap();
    assert_eq!(job_status(&db.pool, job).await, "cancelled");
    let bundles: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_steps WHERE job_id = $1 AND kind = 'bundle'",
    )
    .bind(job)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(bundles, 0);
    // Already cancelled → Ok (idempotent); ended otherwise → Conflict (409); other tenant →
    // NotFound.
    jobs::cancel(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(job_status(&db.pool, job).await, "cancelled");
    let done = job_with(&db.pool, &[(StepKind::Bundle, "")]).await;
    let b = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    steps::complete_and_enqueue(&db.pool, &b, out(), &[])
        .await
        .unwrap();
    assert_eq!(job_status(&db.pool, done).await, "completed");
    assert!(matches!(
        jobs::cancel(&db.pool, tenant(), done).await,
        Err(IngestError::Conflict(_))
    ));
    assert!(matches!(
        jobs::cancel(&db.pool, Uuid::now_v7(), job).await,
        Err(IngestError::NotFound)
    ));
}

#[tokio::test]
async fn last_step_completes_job() {
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::Bundle, "")]).await;
    let b = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    steps::complete_and_enqueue(
        &db.pool,
        &b,
        out(),
        &[(StepKind::Deliver, String::new(), json!({"x": 1}))],
    )
    .await
    .unwrap();
    assert_eq!(
        job_status(&db.pool, job).await,
        "running",
        "deliver pending"
    );
    let d = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(d.kind, StepKind::Deliver);
    assert_eq!(d.input, json!({"x": 1}));
    let usage = json!({"units": 3, "unit": "pages"});
    steps::complete_and_enqueue(
        &db.pool,
        &d,
        StepOutput {
            output: json!({"document_id": "d"}),
            provider: Some("ocr".into()),
            model: Some("m".into()),
            usage: Some(usage.clone()),
            cost_micros: Some(42),
            skipped: false,
            unrecorded: None,
        },
        &[],
    )
    .await
    .unwrap();
    let view = jobs::get(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "completed");
    let s = view.steps.iter().find(|s| s.kind == "deliver").unwrap();
    assert_eq!(s.status, "completed");
    assert_eq!(s.provider.as_deref(), Some("ocr"));
    assert_eq!(s.model.as_deref(), Some("m"));
    assert_eq!(s.usage.as_ref(), Some(&usage));
    assert_eq!(s.cost_micros, Some(42));
    assert!(s.finished_at.is_some());
    // The JSON view carries the API's step fields.
    let v: Value = serde_json::to_value(&view).unwrap();
    for f in [
        "kind",
        "key",
        "status",
        "attempt",
        "provider",
        "model",
        "usage",
        "cost_micros",
        "error",
        "started_at",
        "finished_at",
    ] {
        assert!(v["steps"][0].get(f).is_some(), "step field {f}");
    }
    assert_eq!(v["id"], json!(job));
    assert_eq!(v["status"], "completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_last_steps_complete_job() {
    // Two same-stage steps finishing concurrently must not each see the other as still running
    // (which would leave the job `running` forever). 30 jobs × 2 steps, all completed at once.
    let db = TestDb::start().await;
    let mut jobs_ = Vec::new();
    for _ in 0..30 {
        jobs_.push(
            job_with(
                &db.pool,
                &[(StepKind::Caption, "a"), (StepKind::Caption, "b")],
            )
            .await,
        );
    }
    let mut claimed = Vec::new();
    while let Some(c) = steps::claim_next(&db.pool, MAX).await.unwrap() {
        claimed.push(c);
    }
    assert_eq!(claimed.len(), 60);
    let barrier = Arc::new(Barrier::new(claimed.len()));
    let mut tasks = Vec::new();
    for c in claimed {
        let pool = db.pool.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            steps::complete_and_enqueue(&pool, &c, out(), &[]).await
        }));
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    for job in jobs_ {
        assert_eq!(job_status(&db.pool, job).await, "completed", "job {job}");
    }
}

#[tokio::test]
async fn skipped_output_marks_step_skipped_and_opens_the_gate() {
    let db = TestDb::start().await;
    let job = job_with(
        &db.pool,
        &[(StepKind::Caption, "a"), (StepKind::Bundle, "")],
    )
    .await;
    let c = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    let mut o = out();
    o.skipped = true;
    steps::complete_and_enqueue(&db.pool, &c, o, &[])
        .await
        .unwrap();
    assert_eq!(step_status(&db.pool, c.id).await, "skipped");
    let b = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("bundle");
    assert_eq!(b.kind, StepKind::Bundle);
    assert_eq!(job_status(&db.pool, job).await, "running");
}

#[tokio::test]
async fn retry_resets_failed_steps_only() {
    let db = TestDb::start().await;
    let job = job_with(
        &db.pool,
        &[(StepKind::OcrPages, "a"), (StepKind::OcrPages, "b")],
    )
    .await;
    // Retry is only for failed jobs.
    assert!(matches!(
        jobs::retry(&db.pool, tenant(), job).await,
        Err(IngestError::Conflict(_))
    ));
    let a = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    let b = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    steps::complete_and_enqueue(&db.pool, &a, out(), &[])
        .await
        .unwrap();
    steps::fail(
        &db.pool,
        &b,
        &err(ErrorClass::InvalidRequest, "bad_page"),
        5,
    )
    .await
    .unwrap();
    assert_eq!(job_status(&db.pool, job).await, "failed");
    assert!(matches!(
        jobs::retry(&db.pool, Uuid::now_v7(), job).await,
        Err(IngestError::NotFound)
    ));
    jobs::retry(&db.pool, tenant(), job).await.unwrap();
    let view = jobs::get(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "running");
    assert!(view.job.error.is_none());
    assert_eq!(step_status(&db.pool, a.id).await, "completed");
    assert_eq!(step_status(&db.pool, b.id).await, "pending");
    let again = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("retried step");
    assert_eq!(again.id, b.id);
    // `attempt` is the fence token: it keeps growing, so a worker still holding run 1 can
    // never write over the retried run. The attempt BUDGET restarts instead.
    assert_eq!(again.attempt, 2, "attempt never reused");
    assert!(steps::claim_next(&db.pool, MAX).await.unwrap().is_none());
    lost_on(steps::heartbeat(&db.pool, &b).await, b.id, 1);
    // A fresh budget of 2: run 2 is the first of it, so a Retryable failure is retried.
    steps::fail(&db.pool, &again, &err(ErrorClass::Retryable, "x"), 2)
        .await
        .unwrap();
    assert_eq!(step_status(&db.pool, b.id).await, "pending");
}

fn lost_on(r: strata_ingest::Result<()>, id: i64, attempt: i32) {
    assert!(
        matches!(r, Err(IngestError::LeaseLost { step, attempt: a }) if step == id && a == attempt),
        "expected LeaseLost for attempt {attempt}, got {r:?}"
    );
}

async fn counters(pool: &PgPool, id: i64) -> (String, i32, i32, i32) {
    sqlx::query_as("SELECT status, attempt, releases, slot_waits FROM ingest_steps WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A shutdown release never decrements `attempt` (the fence token): the next claim gets a NEW
/// attempt, and the released run's stale worker is fenced out. The release is refunded to the
/// budget instead (`releases`).
#[tokio::test]
async fn release_keeps_attempt_monotonic_and_fences_the_released_run() {
    let db = TestDb::start().await;
    let job = job_with(&db.pool, &[(StepKind::OcrPages, "p1-1")]).await;
    let a = steps::claim_next(&db.pool, MAX).await.unwrap().unwrap();
    assert_eq!(a.attempt, 1);
    steps::release(&db.pool, &a, MAX).await.unwrap();
    assert_eq!(counters(&db.pool, a.id).await, ("pending".into(), 1, 1, 0));
    let b = steps::claim_next(&db.pool, MAX)
        .await
        .unwrap()
        .expect("due at once");
    assert_eq!((b.id, b.attempt), (a.id, 2), "a new fence token");
    // The released run (attempt 1) cannot write over run 2.
    lost_on(steps::heartbeat(&db.pool, &a).await, a.id, 1);
    lost_on(
        steps::complete_and_enqueue(&db.pool, &a, out(), &[]).await,
        a.id,
        1,
    );
    lost_on(steps::release(&db.pool, &a, MAX).await, a.id, 1);
    steps::complete_and_enqueue(&db.pool, &b, out(), &[])
        .await
        .unwrap();
    assert_eq!(job_status(&db.pool, job).await, "completed");
}

/// Releases are refunded at most MAX_RELEASE_REFUNDS (3) times: a step whose run keeps being
/// interrupted by shutdowns (its own work triggers the SIGTERM) stops after max + 3 runs.
#[tokio::test]
async fn a_step_released_on_every_run_fails_after_max_plus_refunds() {
    use strata_ingest::limits::MAX_RELEASE_REFUNDS;
    let db = TestDb::start().await;
    let max = 2;
    let job = job_with(&db.pool, &[(StepKind::Convert, "")]).await;
    let mut runs = 0;
    while let Some(c) = steps::claim_next(&db.pool, max).await.unwrap() {
        runs += 1;
        assert!(runs <= 10, "release loop never ends");
        steps::release(&db.pool, &c, max).await.unwrap();
    }
    assert_eq!(runs, max + MAX_RELEASE_REFUNDS);
    let view = jobs::get(&db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "failed");
    assert_eq!(view.steps[0].status, "failed");
    assert_eq!(view.job.error.unwrap()["code"], "shutdown_released");
}

/// A run abandoned with `slot_wait_timeout` (it never got its slot) is re-queued after a
/// backoff without using an attempt; `attempt` still grows.
#[tokio::test]
async fn slot_wait_timeout_requeues_without_using_an_attempt() {
    let db = TestDb::start().await;
    job_with(&db.pool, &[(StepKind::OcrPages, "p1-1")]).await;
    let max = 1;
    let a = steps::claim_next(&db.pool, max).await.unwrap().unwrap();
    let wait = err(ErrorClass::Retryable, "slot_wait_timeout");
    steps::requeue_slot_wait(&db.pool, &a, &wait, max)
        .await
        .unwrap();
    assert_eq!(counters(&db.pool, a.id).await, ("pending".into(), 1, 0, 1));
    let delay = seconds_until_run_after(&db.pool, a.id).await;
    assert!(delay > 0.5, "backed off: {delay}");
    make_due(&db.pool, a.id).await;
    let b = steps::claim_next(&db.pool, max).await.unwrap().unwrap();
    assert_eq!(b.attempt, 2);
    // Run 2 under max_attempts = 2: the abandoned wait was not counted, so it is retried
    // (counted, the budget would be used up and the step failed).
    steps::fail(&db.pool, &b, &err(ErrorClass::Retryable, "x"), 2)
        .await
        .unwrap();
    assert_eq!(step_status(&db.pool, a.id).await, "pending");
    lost_on(
        steps::requeue_slot_wait(&db.pool, &a, &wait, max).await,
        a.id,
        1,
    );
}

async fn caption_step(pool: &PgPool) -> (Uuid, i64) {
    let job = job_with(pool, &[(StepKind::Caption, "fig")]).await;
    sqlx::query("UPDATE ingest_steps SET input = '{\"asset_id\": \"a1\"}' WHERE job_id = $1")
        .bind(job)
        .execute(pool)
        .await
        .unwrap();
    let id: i64 = sqlx::query_scalar("SELECT id FROM ingest_steps WHERE job_id = $1")
        .bind(job)
        .fetch_one(pool)
        .await
        .unwrap();
    (job, id)
}

async fn caption_outcome(pool: &PgPool, id: i64) -> (String, Option<Value>) {
    sqlx::query_as("SELECT status, output FROM ingest_steps WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// R40: a Caption whose last attempt ends in a step / slot-wait timeout is `skipped`, never a
/// failed job; a non-caption step with the same error fails its job.
#[tokio::test]
async fn caption_timeout_on_the_last_attempt_is_skipped() {
    let db = TestDb::start().await;
    for code in ["step_timeout", "slot_wait_timeout"] {
        let (job, id) = caption_step(&db.pool).await;
        let c = steps::claim_next(&db.pool, 1).await.unwrap().unwrap();
        assert_eq!(c.id, id);
        steps::fail(&db.pool, &c, &err(ErrorClass::Retryable, code), 1)
            .await
            .unwrap();
        let (status, output) = caption_outcome(&db.pool, id).await;
        assert_eq!(status, "skipped", "{code}");
        let output = output.unwrap();
        assert_eq!(output["skipped"], "caption_failed", "{output}");
        assert_eq!(output["asset_id"], "a1", "{output}");
        assert_eq!(output["error"]["code"], code, "{output}");
        assert_eq!(job_status(&db.pool, job).await, "completed", "{code}");
    }
    // The same on a non-optional step fails the job.
    let job = job_with(&db.pool, &[(StepKind::OcrPages, "p1-1")]).await;
    let c = steps::claim_next(&db.pool, 1).await.unwrap().unwrap();
    steps::fail(&db.pool, &c, &err(ErrorClass::Retryable, "step_timeout"), 1)
        .await
        .unwrap();
    assert_eq!(job_status(&db.pool, job).await, "failed");
}

/// R40: a Caption whose lease expires on its last attempt is reaped as `skipped`, and a
/// Caption looping on shutdown releases ends `skipped` too.
#[tokio::test]
async fn caption_lease_expiry_or_release_loop_on_the_last_attempt_is_skipped() {
    let db = TestDb::start().await;
    let (job, id) = caption_step(&db.pool).await;
    let c = steps::claim_next(&db.pool, 1).await.unwrap().unwrap();
    expire_lease(&db.pool, c.id).await;
    assert!(steps::claim_next(&db.pool, 1).await.unwrap().is_none());
    let (status, output) = caption_outcome(&db.pool, id).await;
    assert_eq!(status, "skipped");
    assert_eq!(output.unwrap()["error"]["code"], "lease_expired");
    assert_eq!(job_status(&db.pool, job).await, "completed");

    let (job, id) = caption_step(&db.pool).await;
    while let Some(c) = steps::claim_next(&db.pool, 1).await.unwrap() {
        steps::release(&db.pool, &c, 1).await.unwrap();
    }
    let (status, output) = caption_outcome(&db.pool, id).await;
    assert_eq!(status, "skipped");
    assert_eq!(output.unwrap()["error"]["code"], "shutdown_released");
    assert_eq!(job_status(&db.pool, job).await, "completed");
}

#[tokio::test]
async fn list_filters_by_tenant_store_and_status() {
    let db = TestDb::start().await;
    let a = jobs::create(&db.pool, new_job()).await.unwrap();
    let nb = new_job();
    let store = nb.store_id;
    let b = jobs::create(&db.pool, nb).await.unwrap();
    let mut other = new_job();
    other.tenant = Uuid::now_v7();
    jobs::create(&db.pool, other).await.unwrap();
    jobs::cancel(&db.pool, tenant(), a).await.unwrap();

    let all = jobs::list(&db.pool, tenant(), None, None, 50)
        .await
        .unwrap();
    let ids: Vec<Uuid> = all.iter().map(|j| j.id).collect();
    assert_eq!(ids, vec![b, a], "newest first, own tenant only");
    let by_store = jobs::list(&db.pool, tenant(), Some(store), None, 50)
        .await
        .unwrap();
    assert_eq!(by_store.iter().map(|j| j.id).collect::<Vec<_>>(), vec![b]);
    let cancelled = jobs::list(&db.pool, tenant(), None, Some("cancelled"), 50)
        .await
        .unwrap();
    assert_eq!(cancelled.iter().map(|j| j.id).collect::<Vec<_>>(), vec![a]);
    let one = jobs::list(&db.pool, tenant(), None, None, 1).await.unwrap();
    assert_eq!(one.len(), 1);
}

/// `migrate` needs only the database URL: no tokens, S3 keys or providers file.
#[tokio::test]
async fn migrate_subcommand_needs_only_database_url() {
    let db = TestDb::start().await;
    let url = db.url.clone();
    let out = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_strata-ingest"))
            .arg("migrate")
            .env_clear()
            .env("STRATA_INGEST_DATABASE_URL", url)
            .output()
            .expect("run binary")
    })
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "migrate exits 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let bad = Command::new(env!("CARGO_BIN_EXE_strata-ingest"))
        .arg("migrate")
        .env_clear()
        .env(
            "STRATA_INGEST_DATABASE_URL",
            "postgres://nobody:x@127.0.0.1:1/none",
        )
        .output()
        .expect("run binary");
    assert!(!bad.status.success(), "unreachable DB exits non-zero");
    // Required: no fallback to the dev default URL.
    let unset = Command::new(env!("CARGO_BIN_EXE_strata-ingest"))
        .arg("migrate")
        .env_clear()
        .output()
        .expect("run binary");
    assert!(!unset.status.success(), "missing URL exits non-zero");
    assert!(
        String::from_utf8_lossy(&unset.stderr).contains("STRATA_INGEST_DATABASE_URL"),
        "names the missing variable: {}",
        String::from_utf8_lossy(&unset.stderr)
    );
}
