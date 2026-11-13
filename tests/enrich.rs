//! Task 8: the enrich steps — OcrPages (provider and docling-cpu paths), Caption,
//! TranscribeSegment — and Convert's audio/video branch (ffprobe + ffmpeg segmenting).
//! Real Postgres + in-process storage_service (`FakeStorage`); providers are the `pcontract fake`
//! or in-process mocks.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::{FakeStorage, STORAGE_BUCKET, TestDb};
use strata_ingest::config::{Config, Mode, ProviderCfg, ProvidersFile, Region, Secret};
use strata_ingest::db::artifacts;
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::steps::{self, ClaimedStep, StepKind, StepOutput};
use strata_ingest::infra::docling::PAGE_BREAK;
use strata_ingest::limits::Limits;
use strata_ingest::pipeline::{Ctx, original_key};
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{StepRunner, WorkerOpts, run_workers_with};

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-000000000008").unwrap()
}

/// Nothing listens on port 9 (discard).
const UNUSED_URL: &str = "http://127.0.0.1:9";
const PRICE: i64 = 1_500;

fn three_page_pdf() -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/providers/three_page.pdf",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

fn config(db: &TestDb, storage: &FakeStorage) -> Config {
    Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![(tenant(), Secret::new("api-token"))],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: UNUSED_URL.to_string(),
        webscraper_url: UNUSED_URL.to_string(),
        webscraper_token: None,
        strata_url: UNUSED_URL.to_string(),
        strata_token: Secret::new("strata-token"),
        workers: 2,
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
        metering: None,
    }
}

/// A provider at `base` (`…/v1`). The fake runs emulated on arm64 Macs: generous timeout.
fn provider(base: &str, dialect: &str, model: &str, key: &str) -> ProviderCfg {
    ProviderCfg {
        base_url: base.to_string(),
        dialect: dialect.to_string(),
        model: model.to_string(),
        api_key: Secret::new(key),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 120_000,
        concurrency: 2,
        rpm: None,
        price_micros_per_unit: Some(PRICE),
    }
}

struct Env {
    db: TestDb,
    _storage: FakeStorage,
    ctx: Arc<Ctx>,
}

/// A cost event as the outbox stores it.
type Ev = std::collections::BTreeMap<String, String>;

/// The cost events in the outbox (`metering_outbox.fields`), oldest first.
async fn outbox_events(pool: &sqlx::PgPool) -> Vec<Ev> {
    sqlx::query_scalar::<_, sqlx::types::Json<Ev>>("SELECT fields FROM metering_outbox ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|j| j.0)
        .collect()
}

impl Env {
    /// The cost events committed so far (call after `run_job`, which waits for the workers).
    async fn events(&self) -> Vec<Ev> {
        outbox_events(&self.db.pool).await
    }
}

async fn env(edit: impl FnOnce(&mut Config)) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let mut cfg = config(&db, &storage);
    edit(&mut cfg);
    let ctx = Ctx::with_pool(db.pool.clone(), cfg).unwrap();
    Env {
        db,
        _storage: storage,
        ctx: Arc::new(ctx),
    }
}

fn usage_extra(ev: &Ev) -> Value {
    serde_json::from_str(&ev["usage_extra"]).unwrap()
}

fn fast() -> WorkerOpts {
    WorkerOpts {
        poll: Duration::from_millis(50),
        heartbeat: Duration::from_secs(60),
        deadline: None,
        ..WorkerOpts::default()
    }
}

/// The real steps; Bundle / Deliver complete with a stub output (covered in tests/bundle.rs).
fn runner() -> StepRunner {
    Arc::new(|ctx: Arc<Ctx>, step: ClaimedStep| {
        Box::pin(async move {
            if matches!(step.kind, StepKind::Bundle | StepKind::Deliver) {
                return Ok((
                    StepOutput {
                        output: json!({"stub": true}),
                        ..StepOutput::default()
                    },
                    vec![],
                ));
            }
            strata_ingest::pipeline::run_step(&ctx, &step).await
        })
    })
}

/// Runs the real worker pool until `job` is completed or failed (up to `secs`).
async fn run_job(ctx: &Arc<Ctx>, job: Uuid, secs: u64) -> (String, Option<Value>, Value) {
    run_job_with(ctx, job, secs, 2, runner()).await
}

/// [`run_job`] with `workers` workers and a custom step runner.
async fn run_job_with(
    ctx: &Arc<Ctx>,
    job: Uuid,
    secs: u64,
    workers: usize,
    runner: StepRunner,
) -> (String, Option<Value>, Value) {
    let shutdown = CancellationToken::new();
    let h = tokio::spawn(run_workers_with(
        ctx.clone(),
        workers,
        shutdown.clone(),
        fast(),
        runner,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let row = loop {
        let row = job_row(&ctx.pool, job).await;
        if matches!(row.0.as_str(), "completed" | "failed" | "rejected") {
            break row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job} still {} after {secs} s: {:#?}",
            row.0,
            steps_full(&ctx.pool, job).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), h)
        .await
        .expect("workers stop")
        .expect("pool task");
    row
}

async fn job_row(pool: &PgPool, id: Uuid) -> (String, Option<Value>, Value) {
    let r = sqlx::query("SELECT status, error, meta FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get("status"), r.get("error"), r.get("meta"))
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // fields are read through Debug in assertion messages
struct StepRow {
    kind: String,
    key: String,
    status: String,
    attempt: i32,
    input: Value,
    output: Option<Value>,
    provider: Option<String>,
    model: Option<String>,
    usage: Option<Value>,
    cost_micros: Option<i64>,
    error: Option<Value>,
}

async fn steps_full(pool: &PgPool, job: Uuid) -> Vec<StepRow> {
    sqlx::query(
        "SELECT kind, key, status, attempt, input, output, provider, model, usage, cost_micros,
                error
         FROM ingest_steps WHERE job_id = $1 ORDER BY stage, key, id",
    )
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| StepRow {
        kind: r.get("kind"),
        key: r.get("key"),
        status: r.get("status"),
        attempt: r.get("attempt"),
        input: r.get("input"),
        output: r.get("output"),
        provider: r.get("provider"),
        model: r.get("model"),
        usage: r.get("usage"),
        cost_micros: r.get("cost_micros"),
        error: r.get("error"),
    })
    .collect()
}

async fn step_of(pool: &PgPool, job: Uuid, kind: &str, key: &str) -> StepRow {
    let all = steps_full(pool, job).await;
    all.iter()
        .find(|s| s.kind == kind && s.key == key)
        .cloned()
        .unwrap_or_else(|| panic!("no {kind}/{key} step: {all:#?}"))
}

async fn put(ctx: &Ctx, key: &str, body: Vec<u8>) {
    ctx.storage
        .upload_bytes(key, Bytes::from(body), "application/octet-stream")
        .await
        .expect("put object");
}

/// A JSON artefact of `job` (`ingest_artifacts`).
async fn get_json(ctx: &Ctx, job: Uuid, key: &str) -> Value {
    let body = artifacts::get(&ctx.pool, job, key)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("job {job} has no artefact {key}"));
    serde_json::from_str(&body).unwrap()
}

/// As the API does it: the original first, then the job (which enqueues Acquire).
async fn upload(ctx: &Ctx, filename: &str, body: Vec<u8>, options: Value) -> Uuid {
    let id = Uuid::now_v7();
    let len = body.len();
    put(ctx, &original_key(tenant(), id), body).await;
    jobs::create_with_id(
        &ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": filename,
                           "mime": "application/octet-stream", "bytes": len,
                           "scan": "clean"}),
            options,
        },
    )
    .await
    .expect("create job")
}

/// A job whose Acquire..Convert already ran (the original stored, `meta` set): only the given
/// stage-50 step is pending, so each test drives exactly one enrich step through the real pool.
async fn job_at(
    ctx: &Ctx,
    original: Vec<u8>,
    meta: Value,
    kind: StepKind,
    key: &str,
    input: Value,
) -> Uuid {
    let job = upload(
        ctx,
        "doc.pdf",
        original,
        json!({"ocr": "auto", "captions": true}),
    )
    .await;
    sqlx::query("UPDATE ingest_steps SET status = 'completed', attempt = 1 WHERE job_id = $1")
        .bind(job)
        .execute(&ctx.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE ingest_jobs SET status = 'running', meta = $2 WHERE id = $1")
        .bind(job)
        .bind(meta)
        .execute(&ctx.pool)
        .await
        .unwrap();
    let mut tx = ctx.pool.begin().await.unwrap();
    steps::enqueue(&mut tx, job, kind, key, input)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    job
}

fn pages_doc(pages: &[String]) -> String {
    pages.join(&format!("\n\n{PAGE_BREAK}\n\n"))
}

/// A page with no text layer worth the name (docling's view of a scan).
fn low(n: usize) -> String {
    format!("Scan {n}\n\n![Image](data:image/png;base64,iVBORw0KGgo=)")
}

fn rich(n: usize) -> String {
    format!(
        "## Section {n}\n\n{}",
        "Born-digital text that docling extracted without OCR. ".repeat(4)
    )
}

type Captured = Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>;

/// docling-serve stand-in: every convert answers `md`; request bodies are captured.
async fn docling_fake(md: String) -> (String, Captured) {
    let captured: Captured = Arc::default();
    let reply = json!({
        "document": {"filename": "document.pdf", "md_content": md, "json_content": null},
        "status": "success", "errors": [], "processing_time": 0.5, "timings": {}
    });
    let app = Router::new()
        .route(
            "/v1/convert/file",
            post(
                |State((cap, reply)): State<(Captured, Value)>, body: Bytes| async move {
                    cap.lock().await.push(body.to_vec());
                    axum::Json(reply)
                },
            ),
        )
        .with_state((captured.clone(), reply));
    (spawn(app).await, captured)
}

/// Call counter + the replies of a [`scripted`] endpoint.
type Script = (Arc<AtomicUsize>, Arc<Vec<(u16, Value)>>);

/// A provider endpoint answering the i-th call with `replies[i]` (the last one repeats).
async fn scripted(path: &'static str, replies: Vec<(u16, Value)>) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = (calls.clone(), Arc::new(replies));
    let app = Router::new()
        .route(
            path,
            post(|State((calls, replies)): State<Script>| async move {
                let i = calls.fetch_add(1, Ordering::SeqCst).min(replies.len() - 1);
                let (status, body) = replies[i].clone();
                (StatusCode::from_u16(status).unwrap(), axum::Json(body))
            }),
        )
        .with_state(state);
    (format!("{}/v1", spawn(app).await), calls)
}

fn ocr_reply(pages: &[u32]) -> Value {
    json!({
        "pages": pages.iter().map(|i| json!({
            "index": i, "markdown": format!("ocr text of page {i}"), "images": [],
            "dimensions": {"dpi": 72, "height": 792, "width": 612}
        })).collect::<Vec<_>>(),
        "model": "mistral-ocr-test",
        "usage_info": {"pages_processed": pages.len(), "doc_size_bytes": 100}
    })
}

fn rate_limited() -> Value {
    json!({"error": {"message": "slow down", "type": "rate_limit", "code": null}})
}

/// 16 kHz mono 16-bit PCM WAV of a 440 Hz tone.
fn wav(seconds: u32) -> Vec<u8> {
    let rate = 16_000u32;
    let samples = rate * seconds;
    let data_len = samples * 2;
    let mut out = Vec::with_capacity(44 + data_len as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM
    out.extend_from_slice(&1u16.to_le_bytes()); // mono
    out.extend_from_slice(&rate.to_le_bytes());
    out.extend_from_slice(&(rate * 2).to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for n in 0..samples {
        let t = n as f64 / rate as f64;
        let v = ((t * 440.0 * std::f64::consts::TAU).sin() * 8_000.0) as i16;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------------------------
// OcrPages, provider path

#[tokio::test]
async fn ocr_missing_page_is_invalid_request() {
    let (base, calls) = scripted("/v1/ocr", vec![(200, ocr_reply(&[0]))]).await;
    let e = env(|c| c.providers.ocr = Some(provider(&base, "mistral", "m", "k"))).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-1",
        json!({"pages": [0, 1], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "ocr_missing_page", "{error}");
    assert_eq!(error["class"], "invalid_request", "{error}");
    assert!(error["message"].as_str().unwrap().contains('1'), "{error}");
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!(
        (s.status.as_str(), s.attempt),
        ("failed", 1),
        "terminal, not retried"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        artifacts::get(&e.db.pool, job, "ocr/p0-1.json")
            .await
            .unwrap()
            .is_none(),
        "no partial OCR artefact"
    );
    // The provider answered (and was paid) before the step failed: its call is metered.
    let keys: Vec<String> = e
        .events()
        .await
        .iter()
        .map(|ev| ev["event_key"].clone())
        .collect();
    assert_eq!(
        keys,
        [format!("{job}:ocr_pages/p0-1:1")],
        "a paid call is metered even though its step failed"
    );
}

#[tokio::test]
async fn ocr_429_retries_then_succeeds() {
    let (base, calls) = scripted(
        "/v1/ocr",
        vec![
            (429, rate_limited()),
            (429, rate_limited()),
            (200, ocr_reply(&[0, 1])),
        ],
    )
    .await;
    let e = env(|c| c.providers.ocr = Some(provider(&base, "mistral", "m", "k"))).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-1",
        json!({"pages": [0, 1], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    // Backoff 2 s then 4 s: the queue's own retry, no shortcut.
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!((s.status.as_str(), s.attempt), ("completed", 3), "{s:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(s.cost_micros, Some(2 * PRICE));
    let ocr = get_json(&e.ctx, job, "ocr/p0-1.json").await;
    assert_eq!(
        ocr,
        json!({"0": "ocr text of page 0", "1": "ocr text of page 1"})
    );
    // One cost event, for the attempt that committed (the two 429s are not metered).
    let evs = e.events().await;
    assert_eq!(evs.len(), 1, "{evs:#?}");
    let ev = &evs[0];
    assert_eq!(ev["event_key"], format!("{job}:ocr_pages/p0-1:3"));
    assert_eq!(ev["tenant_id"], tenant().to_string());
    assert_eq!(
        ev["project_id"],
        strata_ingest::metering::tenant_level_project_id(tenant()).to_string()
    );
    assert_eq!(ev["cost_source"], "strata-ingest");
    assert_eq!(ev["service_key"], "strata-ingest-ocr");
    assert_eq!(
        ev["provider"], "local",
        "a 127.0.0.1 default is self-hosted"
    );
    assert_eq!(ev["funding"], "local_gpu");
    assert_eq!(ev["model"], "m");
    assert!(!ev.contains_key("quantity_unit"), "{ev:?}");
    assert_eq!(ev["input_tokens"], "2", "billed per page, not per call");
    assert_eq!(ev["cost_usd"], format!("{:.6}", (2 * PRICE) as f64 / 1e6));
    let x = usage_extra(ev);
    assert_eq!(x["pages"], 2, "{x}");
    assert_eq!(x["step_key"], "ocr_pages/p0-1", "{x}");
    assert_eq!(x["job_id"], job.to_string(), "{x}");
}

#[tokio::test]
async fn ocr_without_reported_usage_records_null_not_zero() {
    let mut reply = ocr_reply(&[2]);
    reply.as_object_mut().unwrap().remove("usage_info");
    let (base, _) = scripted("/v1/ocr", vec![(200, reply)]).await;
    let e = env(|c| c.providers.ocr = Some(provider(&base, "mistral", "m", "k"))).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p2-2",
        json!({"pages": [2], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    let s = step_of(&e.db.pool, job, "ocr_pages", "p2-2").await;
    assert_eq!(s.usage, None, "{s:?}");
    assert_eq!(s.cost_micros, None, "{s:?}");
    assert_eq!(s.provider.as_deref(), Some("mistral"));
    // Still metered (the call happened), flagged so a $0 row explains itself.
    let evs = e.events().await;
    assert_eq!(evs.len(), 1, "{evs:#?}");
    assert_eq!(evs[0]["input_tokens"], "0");
    assert!(!evs[0].contains_key("cost_usd"), "{:?}", evs[0]);
    let x = usage_extra(&evs[0]);
    assert_eq!(x["usage_unknown"], true, "{x}");
    assert_eq!(x["units"], Value::Null, "{x}");
}

#[tokio::test]
async fn ocr_input_over_the_cap_is_rejected_without_a_provider_call() {
    let (base, calls) = scripted("/v1/ocr", vec![(200, ocr_reply(&[0]))]).await;
    let e = env(|c| {
        c.providers.ocr = Some(provider(&base, "mistral", "m", "k"));
        c.limits.max_ocr_input_bytes = 100;
    })
    .await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-0",
        json!({"pages": [0], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "ocr_input_too_large", "{error}");
    assert_eq!(error["class"], "invalid_request", "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn convert_routes_an_original_over_the_ocr_cap_to_docling_cpu() {
    let (docling_url, _) = docling_fake(pages_doc(&[low(0), rich(1)])).await;
    let e = env(|c| {
        c.docling_url = docling_url;
        c.providers.ocr = Some(provider(&format!("{UNUSED_URL}/v1"), "mistral", "m", "k"));
        c.limits.max_ocr_input_bytes = 100;
    })
    .await;
    let job = upload(
        &e.ctx,
        "scan.pdf",
        three_page_pdf(),
        json!({"ocr": "auto", "captions": false}),
    )
    .await;
    // Stop after Convert: the OcrPages it enqueued is what this test is about.
    let shutdown = CancellationToken::new();
    let only_to_convert: StepRunner = Arc::new(|ctx: Arc<Ctx>, step: ClaimedStep| {
        Box::pin(async move {
            if step.kind.stage() > StepKind::Convert.stage() {
                return Err(strata_ingest::error::StepError {
                    class: strata_ingest::providers::ErrorClass::Auth,
                    code: "stop".into(),
                    message: "test stops after convert".into(),
                    provider: None,
                    retry_after_s: None,
                });
            }
            strata_ingest::pipeline::run_step(&ctx, &step).await
        })
    });
    let h = tokio::spawn(run_workers_with(
        e.ctx.clone(),
        1,
        shutdown.clone(),
        fast(),
        only_to_convert,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let convert = loop {
        let s = steps_full(&e.db.pool, job).await;
        if let Some(c) = s
            .iter()
            .find(|s| s.kind == "convert" && s.status != "pending" && s.status != "running")
        {
            break c.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "convert did not run: {s:#?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    shutdown.cancel();
    h.await.unwrap();
    assert_eq!(convert.status, "completed", "{convert:?}");
    let out = convert.output.unwrap();
    assert_eq!(out["ocr"]["engine"], "docling-cpu", "{out}");
    assert_eq!(out["ocr"]["reason"], "ocr_input_too_large", "{out}");
    let ocr: Vec<StepRow> = steps_full(&e.db.pool, job)
        .await
        .into_iter()
        .filter(|s| s.kind == "ocr_pages")
        .collect();
    assert_eq!(ocr.len(), 1, "{ocr:#?}");
    assert_eq!(ocr[0].key, "cpu-p0-0");
    assert_eq!(ocr[0].input["pages"], json!([0]));
}

// ---------------------------------------------------------------------------------------------
// OcrPages, docling-cpu path

#[tokio::test]
async fn docling_cpu_ocr_takes_only_the_selected_pages() {
    let figure = format!(
        "![Image](data:image/png;base64,{})",
        base64_of(&[7u8; 20_000])
    );
    let md = pages_doc(&[
        "ocr page zero".to_string(),
        format!("ocr page one\n\n{figure}"),
        format!("ocr page two\n\n{figure}\n\nafter the figure"),
    ]);
    let (docling_url, captured) = docling_fake(md).await;
    let e = env(|c| c.docling_url = docling_url).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "docling-cpu",
        json!({"pages": [0, 2], "engine": "docling-cpu", "mime": "application/pdf",
               "do_ocr": true, "force_ocr": true, "ocr_engine": "easyocr"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    let s = step_of(&e.db.pool, job, "ocr_pages", "docling-cpu").await;
    assert_eq!(s.provider.as_deref(), Some("docling-cpu"));
    assert_eq!(
        (s.model.as_ref(), s.usage.as_ref(), s.cost_micros),
        (None, None, None)
    );
    let ocr = get_json(&e.ctx, job, "ocr/docling-cpu.json").await;
    // Only the selected pages; embedded images (the page scan itself) are not OCR text.
    assert_eq!(
        ocr,
        json!({"0": "ocr page zero", "2": "ocr page two\n\n\n\nafter the figure"})
    );

    let reqs = captured.lock().await;
    assert_eq!(reqs.len(), 1);
    let body = String::from_utf8_lossy(&reqs[0]);
    for (field, want) in [
        ("do_ocr", "true"),
        ("force_ocr", "true"),
        ("ocr_engine", "easyocr"),
    ] {
        assert!(
            body.contains(&format!("name=\"{field}\"\r\n\r\n{want}\r\n")),
            "{field}={want} not sent"
        );
    }
}

#[tokio::test]
async fn docling_cpu_page_count_mismatch_fails() {
    let (docling_url, _) = docling_fake(pages_doc(&["a".into(), "b".into(), "c".into()])).await;
    let e = env(|c| c.docling_url = docling_url).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 4}),
        StepKind::OcrPages,
        "docling-cpu",
        json!({"pages": [3], "engine": "docling-cpu", "mime": "application/pdf",
               "do_ocr": true, "force_ocr": true, "ocr_engine": "easyocr"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "ocr_page_mismatch", "{error}");
    assert_eq!(error["class"], "invalid_request", "{error}");
}

/// A docling-cpu batch is a window of the document (R37): docling gets `page_range` for the
/// batch (1-based), placeholder pictures, and its pages map back onto the batch's page numbers.
#[tokio::test]
async fn docling_cpu_batch_ocrs_only_its_window() {
    let md = pages_doc(&[
        "ocr page four\n\n<!-- image -->".to_string(),
        "ocr page five".to_string(),
    ]);
    let (docling_url, captured) = docling_fake(md).await;
    let e = env(|c| c.docling_url = docling_url).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 8}),
        StepKind::OcrPages,
        "cpu-p4-5",
        json!({"pages": [4, 5], "engine": "docling-cpu", "mime": "application/pdf",
               "do_ocr": true, "force_ocr": true, "ocr_engine": "easyocr"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    let ocr = get_json(&e.ctx, job, "ocr/cpu-p4-5.json").await;
    assert_eq!(ocr, json!({"4": "ocr page four", "5": "ocr page five"}));
    let reqs = captured.lock().await;
    let body = String::from_utf8_lossy(&reqs[0]);
    for (field, want) in [
        ("page_range", "5"),
        ("page_range", "6"),
        ("image_export_mode", "placeholder"),
    ] {
        assert!(
            body.contains(&format!("name=\"{field}\"\r\n\r\n{want}\r\n")),
            "{field}={want} not sent"
        );
    }
}

fn base64_of(b: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(b)
}

// ---------------------------------------------------------------------------------------------
// Caption

const PNG_1X1: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGD4DwABBAEAwS2OUAAAAABJRU5ErkJggg==";

#[tokio::test]
async fn caption_of_a_missing_asset_is_invalid_request() {
    let (base, calls) = scripted("/v1/chat/completions", vec![(500, json!({}))]).await;
    let e = env(|c| c.providers.vision = Some(provider(&base, "openai", "m", "k"))).await;
    let asset = Uuid::now_v7();
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({}),
        StepKind::Caption,
        &asset.to_string(),
        json!({"asset_id": asset, "object_key": "x", "mime": "image/png"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "failed");
    assert_eq!(error.unwrap()["code"], "asset_missing");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

/// A job with one stored figure (asset row + blob) and a pending Caption step for it.
async fn caption_job(e: &Env) -> (Uuid, Uuid) {
    use base64::Engine as _;
    let job = upload(
        &e.ctx,
        "doc.pdf",
        three_page_pdf(),
        json!({"captions": true}),
    )
    .await;
    let asset = Uuid::now_v7();
    let fkey = format!("{}/{job}/figures/{asset}", tenant());
    let png = base64::engine::general_purpose::STANDARD
        .decode(PNG_1X1)
        .unwrap();
    put(&e.ctx, &fkey, png.clone()).await;
    sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, page, bytes, mime)
         VALUES ($1, $2, 'figure', $3, 0, $4, 'image/png')",
    )
    .bind(asset)
    .bind(job)
    .bind(&fkey)
    .bind(png.len() as i64)
    .execute(&e.db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE ingest_steps SET status = 'completed', attempt = 1 WHERE job_id = $1")
        .bind(job)
        .execute(&e.db.pool)
        .await
        .unwrap();
    let mut tx = e.db.pool.begin().await.unwrap();
    steps::enqueue(
        &mut tx,
        job,
        StepKind::Caption,
        &asset.to_string(),
        json!({"asset_id": asset, "object_key": fkey, "mime": "image/png"}),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    (job, asset)
}

/// One figure the vision provider refuses (422) must not fail the document (R40): the Caption
/// step ends `skipped` with the error kept, no caption is written, the job completes.
#[tokio::test]
async fn caption_refused_by_the_provider_is_skipped_not_fatal() {
    let refusal = json!({"error": {"message": "unsupported image", "type": "invalid_request_error", "code": null}});
    let (base, calls) = scripted("/v1/chat/completions", vec![(422, refusal)]).await;
    let e = env(|c| c.providers.vision = Some(provider(&base, "openai", "m", "k"))).await;
    let (job, asset) = caption_job(&e).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "not retried");
    let s = step_of(&e.db.pool, job, "caption", &asset.to_string()).await;
    assert_eq!(s.status, "skipped");
    let out = s.output.expect("output");
    assert_eq!(out["skipped"], "caption_failed", "{out}");
    assert_eq!(out["error"]["code"], "provider_invalid_request", "{out}");
    assert_eq!(s.provider.as_deref(), Some("openai"));
    let caption: Option<String> =
        sqlx::query_scalar("SELECT caption FROM ingest_assets WHERE id = $1")
            .bind(asset)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    assert_eq!(caption, None);
    assert_eq!(
        e.events().await,
        Vec::<Ev>::new(),
        "a provider call that failed was not paid, so it is not metered"
    );
}

/// A retryable caption failure is retried; on the last attempt it is skipped, not fatal (R40).
#[tokio::test]
async fn caption_failing_on_its_last_attempt_is_skipped() {
    let (base, calls) = scripted("/v1/chat/completions", vec![(500, json!({}))]).await;
    let e = env(|c| {
        c.providers.vision = Some(provider(&base, "openai", "m", "k"));
        c.limits.max_attempts = 1;
    })
    .await;
    let (job, asset) = caption_job(&e).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let s = step_of(&e.db.pool, job, "caption", &asset.to_string()).await;
    assert_eq!(s.status, "skipped");
    assert_eq!(s.output.unwrap()["error"]["class"], "retryable");
}

/// A 200 whose text is a refusal ("I'm sorry, but I cannot assist with that.") is not a caption:
/// the step ends `skipped` (`caption_refused`), nothing is written to the asset row and the job
/// completes. The call was answered (and paid), so it is metered.
#[tokio::test]
async fn caption_answer_that_is_a_refusal_is_skipped_not_stored() {
    let reply = json!({
        "choices": [{"message": {"role": "assistant",
                                 "content": "I'm sorry, but I cannot assist with that."}}],
        "usage": {"total_tokens": 42},
    });
    let (base, calls) = scripted("/v1/chat/completions", vec![(200, reply)]).await;
    let e = env(|c| c.providers.vision = Some(provider(&base, "openai", "m", "k"))).await;
    let (job, asset) = caption_job(&e).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "not retried");
    let s = step_of(&e.db.pool, job, "caption", &asset.to_string()).await;
    assert_eq!(s.status, "skipped");
    let out = s.output.expect("output");
    assert_eq!(out["skipped"], "caption_refused", "{out}");
    assert_eq!(out["asset_id"], asset.to_string(), "{out}");
    assert_eq!(s.provider.as_deref(), Some("openai"));
    assert_eq!(s.model.as_deref(), Some("m"));
    assert_eq!(s.usage.as_ref().unwrap()["units"], 42);
    let caption: Option<String> =
        sqlx::query_scalar("SELECT caption FROM ingest_assets WHERE id = $1")
            .bind(asset)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    assert_eq!(caption, None);
    let evs = e.events().await;
    assert_eq!(evs.len(), 1, "a refused but paid call is metered: {evs:#?}");
    assert_eq!(evs[0]["event_key"], format!("{job}:caption/{asset}:1"));
    assert_eq!(evs[0]["input_tokens"], "42");
    assert_eq!(evs[0]["service_key"], "strata-ingest-vision");
}

// ---------------------------------------------------------------------------------------------
// Audio / video

#[tokio::test]
async fn audio_longer_than_the_limit_fails_too_long() {
    let e = env(|c| {
        c.providers.transcribe = Some(provider(&format!("{UNUSED_URL}/v1"), "openai", "m", "k"));
        c.limits.max_audio_seconds = 60;
    })
    .await;
    let job = upload(&e.ctx, "talk.wav", wav(90), json!({"captions": false})).await;
    let (status, error, _) = run_job(&e.ctx, job, 120).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "too_long", "{error}");
    assert_eq!(error["class"], "invalid_request", "{error}");
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_assets WHERE job_id = $1 AND kind = 'audio_segment'",
    )
    .bind(job)
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(n, 0, "nothing segmented");
}

#[tokio::test]
async fn ffmpeg_helpers_probe_and_segment() {
    use strata_ingest::pipeline::audio::{probe_duration, segment_audio};
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.wav");
    std::fs::write(&input, wav(25)).unwrap();
    let d = probe_duration(&input).await.expect("ffprobe");
    assert!((d - 25.0).abs() < 0.1, "{d}");
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    let segs = segment_audio(&input, &out, 10, 3600).await.expect("ffmpeg");
    assert_eq!(segs.len(), 3, "{segs:?}");
    for (n, s) in segs.iter().enumerate() {
        assert_eq!(s.path, out.join(format!("out_{n:04}.mp3")));
        assert!((s.start_s - 10.0 * n as f64).abs() < 0.2, "{segs:?}");
    }
    // Garbage in: ffprobe's own stderr explains why.
    let bad = dir.path().join("bad.wav");
    std::fs::write(&bad, b"not audio at all").unwrap();
    let err = probe_duration(&bad).await.expect_err("garbage");
    assert_eq!(
        err.class,
        strata_ingest::providers::ErrorClass::InvalidRequest,
        "{err:?}"
    );
    assert!(!err.message.is_empty());
}

/// Runs ffmpeg to make a test input in `dir`; panics (fails the test) if ffmpeg is missing.
fn ffmpeg_make(dir: &std::path::Path, args: &[&str]) {
    let out = std::process::Command::new("ffmpeg")
        .current_dir(dir)
        .args(["-nostdin", "-loglevel", "error", "-y"])
        .args(args)
        .output()
        .expect("ffmpeg is required for these tests");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// An mp3 whose container claims ~12 s but decodes to ~202 s: 2 s at 160 kbps, then 200 s at
/// 8 kbps, no Xing header, so ffprobe estimates the length from the first frame's bitrate.
fn lying_mp3(dir: &std::path::Path) -> Vec<u8> {
    ffmpeg_make(
        dir,
        &[
            "-f",
            "lavfi",
            "-i",
            "sine=f=440:d=2",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-b:a",
            "160k",
            "-write_xing",
            "0",
            "hi.mp3",
        ],
    );
    ffmpeg_make(
        dir,
        &[
            "-f",
            "lavfi",
            "-i",
            "sine=f=440:d=200",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-b:a",
            "8k",
            "-write_xing",
            "0",
            "-id3v2_version",
            "0",
            "lo.mp3",
        ],
    );
    let mut body = std::fs::read(dir.join("hi.mp3")).unwrap();
    body.extend(std::fs::read(dir.join("lo.mp3")).unwrap());
    body
}

#[tokio::test]
async fn audio_whose_header_understates_its_length_fails_too_long() {
    use strata_ingest::pipeline::audio::probe_duration;
    let dir = tempfile::tempdir().unwrap();
    let body = lying_mp3(dir.path());
    let lie = dir.path().join("lie.mp3");
    std::fs::write(&lie, &body).unwrap();
    let claimed = probe_duration(&lie).await.unwrap();
    assert!(
        claimed < 60.0,
        "precondition: the container claims {claimed} s"
    );

    let e = env(|c| {
        c.providers.transcribe = Some(provider(&format!("{UNUSED_URL}/v1"), "openai", "m", "k"));
        c.limits.max_audio_seconds = 60;
        c.limits.audio_segment_s = 30;
    })
    .await;
    let job = upload(&e.ctx, "talk.mp3", body, json!({"captions": false})).await;
    let (status, error, _) = run_job(&e.ctx, job, 120).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "too_long", "{error}");
    assert_eq!(error["class"], "invalid_request", "{error}");
    let steps = steps_full(&e.db.pool, job).await;
    assert!(
        !steps.iter().any(|s| s.kind == "transcribe_segment"),
        "{steps:#?}"
    );
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_assets WHERE job_id = $1 AND kind = 'audio_segment'",
    )
    .bind(job)
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(n, 0, "nothing stored");
}

#[tokio::test]
async fn segmenting_bounds_the_decode_and_rebases_a_late_start() {
    use strata_ingest::pipeline::audio::{probe_duration, segment_audio};
    let dir = tempfile::tempdir().unwrap();
    // Decoded length over the limit: too_long, whatever the probe said.
    let lie = dir.path().join("lie.mp3");
    std::fs::write(&lie, lying_mp3(dir.path())).unwrap();
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    let err = segment_audio(&lie, &out, 30, 60)
        .await
        .expect_err("over the limit");
    assert_eq!(err.code, "too_long", "{err:?}");
    // The decode stopped near max+1 s instead of running the whole 202 s.
    let written: Vec<_> = std::fs::read_dir(&out).unwrap().flatten().collect();
    assert!(written.len() <= 4, "{written:?}");

    // A stream whose timestamps start at 5 s: segment times still start at 0.
    ffmpeg_make(
        dir.path(),
        &[
            "-f",
            "lavfi",
            "-i",
            "sine=f=440:d=10",
            "-c:a",
            "aac",
            "-output_ts_offset",
            "5",
            "-f",
            "mpegts",
            "late.ts",
        ],
    );
    let out2 = dir.path().join("out2");
    std::fs::create_dir(&out2).unwrap();
    let segs = segment_audio(&dir.path().join("late.ts"), &out2, 4, 3600)
        .await
        .unwrap();
    assert_eq!(segs[0].start_s, 0.0, "{segs:?}");
    assert!((segs[1].start_s - 4.0).abs() < 0.2, "{segs:?}");
    assert!(segs.last().unwrap().end_s < 11.0, "{segs:?}");

    // A concat playlist named like media is refused, not followed.
    let wav_path = dir.path().join("a.wav");
    std::fs::write(&wav_path, wav(2)).unwrap();
    let playlist = dir.path().join("evil.wav");
    std::fs::write(&playlist, "ffconcat version 1.0\nfile a.wav\n").unwrap();
    let err = probe_duration(&playlist)
        .await
        .expect_err("playlist refused");
    assert_eq!(err.code, "audio_decode_failed", "{err:?}");
    let out3 = dir.path().join("out3");
    std::fs::create_dir(&out3).unwrap();
    let err = segment_audio(&playlist, &out3, 4, 3600)
        .await
        .expect_err("playlist refused");
    assert_eq!(err.code, "audio_decode_failed", "{err:?}");
}

// ---------------------------------------------------------------------------------------------
// Metering through the real provider step: a paid call is recorded when it returns
// (`record_paid` in the OCR step), independent of the step's completion and fence; a failing
// record never fails the step (no second paid call); no call, no row.

async fn event_keys(e: &Env) -> Vec<String> {
    e.events()
        .await
        .iter()
        .map(|ev| ev["event_key"].clone())
        .collect()
}

/// An OcrPages step on a provider that answers both pages (every call, counted).
async fn ocr_env() -> (Env, Arc<AtomicUsize>, Uuid) {
    let (base, calls) = scripted("/v1/ocr", vec![(200, ocr_reply(&[0, 1]))]).await;
    let e = env(|c| c.providers.ocr = Some(provider(&base, "mistral", "m", "k"))).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-1",
        json!({"pages": [0, 1], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    (e, calls, job)
}

/// Claims the job's OcrPages step and runs the real step code (the provider call included).
async fn run_ocr(e: &Env) -> (ClaimedStep, StepOutput) {
    let step = steps::claim_next(&e.db.pool, strata_ingest::limits::DEFAULT_MAX_ATTEMPTS)
        .await
        .unwrap()
        .expect("the OcrPages step is claimable");
    assert_eq!(step.kind, StepKind::OcrPages);
    let (out, _) = strata_ingest::pipeline::run_step(&e.ctx, &step)
        .await
        .expect("the step ran");
    (step, out)
}

#[tokio::test]
async fn a_paid_call_whose_result_commits_has_exactly_one_row() {
    let (e, calls, job) = ocr_env().await;
    let (step, out) = run_ocr(&e).await;
    assert_eq!(out.unrecorded, None, "recorded when the call returned");
    steps::complete_and_enqueue(&e.db.pool, &step, out, &[])
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        event_keys(&e).await,
        [format!("{job}:ocr_pages/p0-1:1")],
        "one row, none added by the completion"
    );
}

#[tokio::test]
async fn a_paid_call_whose_result_is_fenced_out_keeps_its_row() {
    let (e, calls, job) = ocr_env().await;
    let (step, out) = run_ocr(&e).await;
    // Another run now owns the step (its claim moved the fence).
    sqlx::query("UPDATE ingest_steps SET attempt = attempt + 1 WHERE id = $1")
        .bind(step.id)
        .execute(&e.db.pool)
        .await
        .unwrap();
    let r = steps::complete_and_enqueue(&e.db.pool, &step, out, &[]).await;
    assert!(
        matches!(r, Err(strata_ingest::IngestError::LeaseLost { .. })),
        "{r:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        event_keys(&e).await,
        [format!("{job}:ocr_pages/p0-1:1")],
        "the provider was paid although the result never committed"
    );
}

#[tokio::test]
async fn a_paid_call_whose_completion_rolls_back_keeps_its_row() {
    let (e, _, job) = ocr_env().await;
    let (step, out) = run_ocr(&e).await;
    // The completion's last statement (the job status update) fails: it rolls back.
    sqlx::query(
        "CREATE FUNCTION boom() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$",
    )
    .execute(&e.db.pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER boom BEFORE UPDATE ON ingest_jobs FOR EACH ROW EXECUTE FUNCTION boom()",
    )
    .execute(&e.db.pool)
    .await
    .unwrap();
    let r = steps::complete_and_enqueue(&e.db.pool, &step, out, &[]).await;
    assert!(r.is_err(), "{r:?}");
    assert_eq!(
        step_of(&e.db.pool, job, "ocr_pages", "p0-1").await.status,
        "running",
        "the completion rolled back"
    );
    assert_eq!(event_keys(&e).await, [format!("{job}:ocr_pages/p0-1:1")]);
}

/// The next `n` INSERTs into `metering_outbox` fail (a sequence counts them: it is not rolled
/// back with the failed statement).
async fn fail_outbox_inserts(pool: &PgPool, n: i64) {
    for sql in [
        "CREATE SEQUENCE outbox_fail_seq",
        "CREATE TABLE outbox_fail_n (n bigint NOT NULL)",
        "CREATE FUNCTION outbox_fail() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF nextval('outbox_fail_seq') <= (SELECT n FROM outbox_fail_n) THEN
             RAISE EXCEPTION 'injected metering_outbox failure';
           END IF;
           RETURN NEW;
         END $$",
        "CREATE TRIGGER outbox_fail BEFORE INSERT ON metering_outbox
         FOR EACH ROW EXECUTE FUNCTION outbox_fail()",
    ] {
        sqlx::query(sql).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO outbox_fail_n VALUES ($1)")
        .bind(n)
        .execute(pool)
        .await
        .unwrap();
}

async fn outbox_insert_tries(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT last_value FROM outbox_fail_seq")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_record_that_fails_briefly_is_retried_and_the_provider_is_called_once() {
    let (e, calls, job) = ocr_env().await;
    fail_outbox_inserts(&e.db.pool, 2).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the paid call is not repeated"
    );
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!((s.status.as_str(), s.attempt), ("completed", 1), "{s:?}");
    assert_eq!(event_keys(&e).await, [format!("{job}:ocr_pages/p0-1:1")]);
    assert_eq!(
        outbox_insert_tries(&e.db.pool).await,
        3,
        "2 failed, the 3rd stood"
    );
}

#[tokio::test]
async fn a_record_that_keeps_failing_does_not_fail_the_step_and_rides_the_completion() {
    let (e, calls, job) = ocr_env().await;
    // Every try of record_with_retry fails; the completion's insert is the next one.
    let tries = 1 + strata_ingest::metering::RECORD_RETRY_PAUSES.len() as i64;
    fail_outbox_inserts(&e.db.pool, tries).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the paid call is not repeated"
    );
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!((s.status.as_str(), s.attempt), ("completed", 1), "{s:?}");
    assert_eq!(
        event_keys(&e).await,
        [format!("{job}:ocr_pages/p0-1:1")],
        "written in the completion transaction"
    );
    assert_eq!(outbox_insert_tries(&e.db.pool).await, tries + 1);
}

#[tokio::test]
async fn a_failed_record_on_a_step_that_then_fails_gets_one_more_try() {
    // The provider answers page 0 only: the call is paid, then the step fails ocr_missing_page.
    let (base, calls) = scripted("/v1/ocr", vec![(200, ocr_reply(&[0]))]).await;
    let e = env(|c| c.providers.ocr = Some(provider(&base, "mistral", "m", "k"))).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-1",
        json!({"pages": [0, 1], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    let tries = 1 + strata_ingest::metering::RECORD_RETRY_PAUSES.len() as i64;
    fail_outbox_inserts(&e.db.pool, tries).await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "failed");
    assert_eq!(error.unwrap()["code"], "ocr_missing_page");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        event_keys(&e).await,
        [format!("{job}:ocr_pages/p0-1:1")],
        "the worker's last try recorded the paid call"
    );
}

#[tokio::test]
async fn a_step_that_makes_no_provider_call_has_no_row() {
    let e = env(|_| {}).await;
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::Caption,
        "x",
        json!({"skip": "too_small"}),
    )
    .await;
    let (status, error, _) = run_job(&e.ctx, job, 60).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(
        step_of(&e.db.pool, job, "caption", "x").await.status,
        "skipped"
    );
    assert_eq!(event_keys(&e).await, Vec::<String>::new());
}

/// While `fail_until` (a timestamp in `outbox_fail_window`) is in the future, every INSERT into
/// `metering_outbox` waits `sleep_s` and then fails: a slow, failing outbox.
async fn fail_outbox_inserts_until(pool: &PgPool, sleep_s: f64) {
    for sql in [
        "CREATE TABLE outbox_fail_window (fail_until timestamptz NOT NULL, sleep_s float8 NOT NULL)",
        "CREATE FUNCTION outbox_fail_window() RETURNS trigger LANGUAGE plpgsql AS $$
         DECLARE w record;
         BEGIN
           SELECT * INTO w FROM outbox_fail_window;
           IF clock_timestamp() < w.fail_until THEN
             PERFORM pg_sleep(w.sleep_s);
             RAISE EXCEPTION 'injected slow metering_outbox failure';
           END IF;
           RETURN NEW;
         END $$",
        "CREATE TRIGGER outbox_fail_window BEFORE INSERT ON metering_outbox
         FOR EACH ROW EXECUTE FUNCTION outbox_fail_window()",
    ] {
        sqlx::query(sql).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO outbox_fail_window VALUES ('infinity', $1)")
        .bind(sleep_s)
        .execute(pool)
        .await
        .unwrap();
}

/// A run aborted while its paid call's record is still being retried (here: shutdown grace
/// ran out) leaves the meter parked; the worker records it after the abort. The provider is
/// called once and the step is released, not completed.
#[tokio::test]
async fn a_run_aborted_mid_record_retry_is_settled_by_the_worker() {
    let (e, calls, job) = ocr_env().await;
    // Each failing insert takes 3 s: the first try is still running when the run is aborted.
    fail_outbox_inserts_until(&e.db.pool, 3.0).await;
    let shutdown = CancellationToken::new();
    let opts = WorkerOpts {
        shutdown_grace: Duration::from_millis(4500),
        ..fast()
    };
    let workers = tokio::spawn(run_workers_with(
        e.ctx.clone(),
        1,
        shutdown.clone(),
        opts,
        runner(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "provider never called"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Inserts that START from now + 4 s succeed. Tries 1 (t≈0..3 s) and 2 (t≈3.1..6.1 s) start
    // inside the window and fail; the run is aborted at t≈0.5 + 4.5 = 5 s, mid try 2; the
    // worker's own insert after the abort starts past the window.
    sqlx::query("UPDATE outbox_fail_window SET fail_until = clock_timestamp() + interval '4 s'")
        .execute(&e.db.pool)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), workers)
        .await
        .expect("workers stop")
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the paid call is not repeated"
    );
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!(
        s.status, "pending",
        "aborted and released, not completed: {s:?}"
    );
    assert_eq!(
        event_keys(&e).await,
        [format!("{job}:ocr_pages/p0-1:1")],
        "the worker recorded the parked meter after the abort"
    );
}

/// A provider that bumps the step's attempt (another run "took" the step) before answering: the
/// call is paid, then the worker's completion is fenced out.
async fn fencing_ocr_provider(
    pool: PgPool,
    job: Arc<std::sync::Mutex<Option<Uuid>>>,
) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = (calls.clone(), pool, job);
    type S = (
        Arc<AtomicUsize>,
        PgPool,
        Arc<std::sync::Mutex<Option<Uuid>>>,
    );
    let app = Router::new()
        .route(
            "/v1/ocr",
            post(|State((calls, pool, job)): State<S>| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let job = { *job.lock().unwrap() }.expect("job set");
                sqlx::query(
                    "UPDATE ingest_steps SET attempt = attempt + 1
                     WHERE job_id = $1 AND kind = 'ocr_pages'",
                )
                .bind(job)
                .execute(&pool)
                .await
                .unwrap();
                (StatusCode::OK, axum::Json(ocr_reply(&[0, 1])))
            }),
        )
        .with_state(state);
    (format!("{}/v1", spawn(app).await), calls)
}

/// The record fails on every try (meter carried), then the completion is fenced out: the worker
/// records the carried meter on its own connection. The provider is called once.
#[tokio::test]
async fn a_carried_meter_whose_completion_is_fenced_out_is_recorded_by_the_worker() {
    let db_job = Arc::new(std::sync::Mutex::new(None));
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (base, calls) = fencing_ocr_provider(db.pool.clone(), db_job.clone()).await;
    let mut cfg = config(&db, &storage);
    cfg.providers.ocr = Some(provider(&base, "mistral", "m", "k"));
    let e = Env {
        ctx: Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap()),
        db,
        _storage: storage,
    };
    let job = job_at(
        &e.ctx,
        three_page_pdf(),
        json!({"pages": 3}),
        StepKind::OcrPages,
        "p0-1",
        json!({"pages": [0, 1], "engine": "provider", "mime": "application/pdf"}),
    )
    .await;
    *db_job.lock().unwrap() = Some(job);
    let tries = 1 + strata_ingest::metering::RECORD_RETRY_PAUSES.len() as i64;
    fail_outbox_inserts(&e.db.pool, tries).await;
    let shutdown = CancellationToken::new();
    let workers = tokio::spawn(run_workers_with(
        e.ctx.clone(),
        1,
        shutdown.clone(),
        fast(),
        runner(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while event_keys(&e).await.is_empty() {
        assert!(tokio::time::Instant::now() < deadline, "never recorded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(60), workers)
        .await
        .expect("workers stop")
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the paid call is not repeated"
    );
    let s = step_of(&e.db.pool, job, "ocr_pages", "p0-1").await;
    assert_eq!(
        (s.status.as_str(), s.attempt),
        ("running", 2),
        "the completion was fenced out: {s:?}"
    );
    assert_eq!(event_keys(&e).await, [format!("{job}:ocr_pages/p0-1:1")]);
    assert_eq!(
        outbox_insert_tries(&e.db.pool).await,
        tries + 1,
        "every try failed, then the worker's own insert stood"
    );
}
