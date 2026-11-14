//! End to end over the corpus (tests/fixtures/corpus, provenance in its `make-fixtures.sh`): real
//! Postgres and docling-serve-cpu, an in-process storage_service (`FakeStorage`, clean verdicts),
//! the `pcontract fake` for OCR / vision /
//! transcription (or a counting mock OCR), and a strata mock that captures every delivery. The
//! real worker pool runs every job from Acquire to Deliver.
//!
//! Slow by nature (docling on CPU, the fake emulated on arm64): one docling container and one
//! fake are shared by the whole binary, and docling-bound tests run one at a time.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::{FAKE_KEY, FakeStorage, STORAGE_BUCKET, TestDb, docling_url, shared_fake_url};
use strata_ingest::config::{Config, Mode, ProviderCfg, ProvidersFile, Region, Secret};
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::steps::{ClaimedStep, StepKind};
use strata_ingest::limits::Limits;
use strata_ingest::pipeline::{Ctx, original_key};
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{StepRunner, WorkerOpts, default_runner, run_workers_with};

const CORPUS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/corpus");
/// Nothing listens on port 9 (discard).
const UNUSED_URL: &str = "http://127.0.0.1:9";
/// A job's ceiling: docling CPU OCR of three pages on a loaded box takes minutes.
const JOB_DEADLINE: Duration = Duration::from_secs(900);

/// docling-serve converts one document at a time here: concurrent CPU conversions only slow each
/// other down past the step deadlines. Held by every test that reaches docling.
static DOCLING_LANE: Mutex<()> = Mutex::const_new(());

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000e2").unwrap()
}

// ---------------------------------------------------------------------------------------------
// expected.toml

#[derive(serde::Deserialize)]
struct Expected {
    pages: HashMap<String, Vec<String>>,
    table: Table,
    born_digital: BornDigital,
    scanned: Scanned,
}

#[derive(serde::Deserialize)]
struct Table {
    rows: Vec<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct BornDigital {
    files: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Scanned {
    min_snippet_rate: f64,
}

fn expected() -> Expected {
    let raw = std::fs::read_to_string(format!("{CORPUS}/expected.toml")).expect("expected.toml");
    toml::from_str(&raw).expect("expected.toml parses")
}

impl Expected {
    /// The snippets of page `n` (1-based).
    fn page(&self, n: usize) -> &[String] {
        &self.pages[&format!("page{n}")]
    }

    fn all_snippets(&self) -> Vec<&String> {
        (1..=3).flat_map(|n| self.page(n)).collect()
    }
}

/// Lower-cased, whitespace runs collapsed to one space, markdown escapes (`\_`) undone.
fn norm(s: &str) -> String {
    s.replace('\\', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn contains(haystack: &str, snippet: &str) -> bool {
    norm(haystack).contains(&norm(snippet))
}

fn missing<'a>(text: &str, snippets: impl IntoIterator<Item = &'a String>) -> Vec<&'a String> {
    snippets
        .into_iter()
        .filter(|s| !contains(text, s))
        .collect()
}

/// Rows of `rows` with no markdown table row (`| … |`) carrying all their cells.
fn missing_rows<'a>(text: &str, rows: &'a [Vec<String>]) -> Vec<&'a Vec<String>> {
    let table_lines: Vec<String> = text
        .lines()
        .filter(|l| l.trim_start().starts_with('|'))
        .map(norm)
        .collect();
    rows.iter()
        .filter(|cells| {
            !table_lines
                .iter()
                .any(|l| cells.iter().all(|c| l.contains(&norm(c))))
        })
        .collect()
}

/// The bundle text under each `<!-- page:N -->` anchor, in order.
fn bundle_pages(text: &str) -> Vec<String> {
    let mut pages = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("<!-- page:") {
        let after = &rest[at..];
        let body_start = after.find("-->").map(|i| i + 3).unwrap_or(after.len());
        let body = &after[body_start..];
        let end = body.find("<!-- page:").unwrap_or(body.len());
        pages.push(body[..end].to_string());
        rest = &body[end..];
    }
    pages
}

// ---------------------------------------------------------------------------------------------
// harness

type Received = Arc<Mutex<Vec<(String, Value)>>>;

/// A strata stand-in that accepts every delivery (`201 created`) and keeps its body.
async fn strata_mock() -> (String, Received) {
    let received: Received = Arc::default();
    let app = Router::new()
        .route(
            "/api/stores/{id}/documents",
            post(
                |State(rec): State<Received>,
                 Path(id): Path<String>,
                 axum::Json(body): axum::Json<Value>| async move {
                    rec.lock().await.push((id, body));
                    (
                        StatusCode::CREATED,
                        axum::Json(json!({"status": "created", "id": Uuid::now_v7(), "chunks": 1})),
                    )
                },
            ),
        )
        .with_state(received.clone());
    (spawn(app).await, received)
}

/// Every OCR request a [`MockOcr`] saw, keyed by its `pages` (JSON), and whether page 2 fails.
#[derive(Default)]
struct MockOcrState {
    calls: Mutex<HashMap<String, usize>>,
    fail_page_2: AtomicBool,
}

/// A Mistral-contract OCR stand-in: answers `mock ocr page {i}` per requested page, or a 400 for
/// a request carrying page 2 while `fail_page_2` is set.
async fn mock_ocr() -> (String, Arc<MockOcrState>) {
    let state = Arc::new(MockOcrState::default());
    let app = Router::new()
        .route(
            "/v1/ocr",
            post(
                |State(st): State<Arc<MockOcrState>>, axum::Json(body): axum::Json<Value>| async move {
                    let pages: Vec<u32> = serde_json::from_value(body["pages"].clone())
                        .unwrap_or_default();
                    *st.calls
                        .lock()
                        .await
                        .entry(json!(pages).to_string())
                        .or_default() += 1;
                    if st.fail_page_2.load(Ordering::SeqCst) && pages.contains(&2) {
                        return (
                            StatusCode::BAD_REQUEST,
                            axum::Json(json!({"error": {"message": "mock refuses page 2",
                                                         "type": "invalid_request_error"}})),
                        );
                    }
                    let out: Vec<Value> = pages
                        .iter()
                        .map(|i| json!({"index": i, "markdown": format!("mock ocr page {i}")}))
                        .collect();
                    (
                        StatusCode::OK,
                        axum::Json(json!({"pages": out,
                                          "usage_info": {"pages_processed": pages.len()}})),
                    )
                },
            ),
        )
        .with_state(state.clone());
    (format!("{}/v1", spawn(app).await), state)
}

/// A provider at `base` (`…/v1`). The fake runs emulated on arm64 Macs: generous timeout.
fn provider(base: &str, dialect: &str) -> ProviderCfg {
    ProviderCfg {
        base_url: base.to_string(),
        dialect: dialect.to_string(),
        model: "fake-model".to_string(),
        api_key: Secret::new(FAKE_KEY),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 300_000,
        concurrency: 2,
        rpm: None,
        price_micros_per_unit: Some(10),
    }
}

/// Which providers a test configures.
#[derive(Default, Clone, Copy)]
struct Providers {
    ocr: bool,
    vision: bool,
    transcribe: bool,
}

struct Env {
    db: TestDb,
    _storage: FakeStorage,
    ctx: Arc<Ctx>,
    received: Received,
}

async fn env(p: Providers, ocr_override: Option<&str>) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (strata, received) = strata_mock().await;
    let fake = if p.ocr || p.vision || p.transcribe {
        format!("{}/v1", shared_fake_url().await)
    } else {
        String::new()
    };
    let cfg = Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        tokens: vec![(tenant(), Secret::new("api-token"))],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: docling_url().await,
        webscraper_url: UNUSED_URL.to_string(),
        webscraper_token: None,
        strata_url: strata,
        strata_token: Secret::new("strata-token"),
        provider_key: None,
        metering: None,
        licence: None,
        workers: 2,
        // The production defaults: this suite is where they are checked (plan Task 11 Step 5).
        limits: Limits::default(),
        providers: ProvidersFile {
            region: Region::Eu,
            // `dev`: the sections below are operator-set defaults for every tenant, which
            // `production` forbids. They are never SSRF-guarded (only tenant registrations are),
            // so the loopback fake needs no `allow_internal_provider_hosts`.
            mode: Mode::Dev,
            allow_internal_provider_hosts: false,
            ocr: match ocr_override {
                Some(base) => Some(provider(base, "mistral")),
                None => p.ocr.then(|| provider(&fake, "mistral")),
            },
            vision: p.vision.then(|| provider(&fake, "openai")),
            transcribe: p.transcribe.then(|| provider(&fake, "openai")),
        },
    };
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    Env {
        db,
        _storage: storage,
        ctx,
        received,
    }
}

fn opts() -> WorkerOpts {
    WorkerOpts {
        poll: Duration::from_millis(100),
        heartbeat: Duration::from_secs(30),
        deadline: None,
        ..WorkerOpts::default()
    }
}

fn mime_of(file: &str) -> &'static str {
    match file.rsplit('.').next().unwrap_or_default() {
        "pdf" => "application/pdf",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "epub" => "application/epub+zip",
        "html" => "text/html",
        "odt" => "application/vnd.oasis.opendocument.text",
        "png" => "image/png",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    }
}

/// As the API does it: the fixture stored (scanned clean) at the original key, then the job
/// created.
async fn upload(ctx: &Ctx, file: &str, options: Value) -> Uuid {
    let body = std::fs::read(format!("{CORPUS}/{file}")).expect("fixture");
    let id = Uuid::now_v7();
    let len = body.len();
    ctx.storage
        .upload_bytes(
            &original_key(tenant(), id),
            Bytes::from(body),
            mime_of(file),
        )
        .await
        .expect("put original");
    jobs::create_with_id(
        &ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": file, "mime": mime_of(file),
                           "bytes": len, "scan": "clean"}),
            options,
        },
    )
    .await
    .expect("create job")
}

fn options(ocr: &str, captions: bool) -> Value {
    json!({"ocr": ocr, "captions": captions, "language": null})
}

async fn job_status(pool: &PgPool, id: Uuid) -> (String, Option<Value>) {
    let r = sqlx::query("SELECT status, error FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get("status"), r.get("error"))
}

/// Polls until every job in `jobs` is completed / failed / rejected; panics past the deadline.
async fn wait_jobs(pool: &PgPool, jobs: &[Uuid]) -> Vec<(String, Option<Value>)> {
    let deadline = tokio::time::Instant::now() + JOB_DEADLINE * jobs.len().max(1) as u32;
    loop {
        let mut out = Vec::new();
        for &job in jobs {
            out.push(job_status(pool, job).await);
        }
        if out
            .iter()
            .all(|(s, _)| matches!(s.as_str(), "completed" | "failed" | "rejected"))
        {
            return out;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "jobs still running: {out:?}\nsteps: {:?}",
            steps_dump(pool, jobs).await
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn steps_dump(pool: &PgPool, jobs: &[Uuid]) -> Vec<(String, String, String, i32)> {
    let mut out = Vec::new();
    for &job in jobs {
        out.extend(
            steps_of(pool, job)
                .await
                .into_iter()
                .map(|s| (s.0, s.1, s.2, s.3)),
        );
    }
    out
}

/// A worker pool over `runner` until `shutdown`.
fn start_pool(
    ctx: &Arc<Ctx>,
    shutdown: &CancellationToken,
    opts: WorkerOpts,
    runner: StepRunner,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_workers_with(
        ctx.clone(),
        2,
        shutdown.clone(),
        opts,
        runner,
    ))
}

/// Runs the real worker pool until every one of `jobs` has ended; returns their statuses.
async fn run_jobs(
    ctx: &Arc<Ctx>,
    jobs: &[Uuid],
    runner: StepRunner,
) -> Vec<(String, Option<Value>)> {
    let shutdown = CancellationToken::new();
    let h = start_pool(ctx, &shutdown, opts(), runner);
    let out = wait_jobs(&ctx.pool, jobs).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(120), h)
        .await
        .expect("workers stop")
        .expect("worker pool");
    out
}

/// `(kind, key, status, attempt, output)` per step, in queue order.
async fn steps_of(pool: &PgPool, job: Uuid) -> Vec<(String, String, String, i32, Option<Value>)> {
    sqlx::query(
        "SELECT kind, key, status, attempt, output FROM ingest_steps WHERE job_id = $1
         ORDER BY stage, id",
    )
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("kind"),
            r.get("key"),
            r.get("status"),
            r.get("attempt"),
            r.get("output"),
        )
    })
    .collect()
}

async fn step_output(pool: &PgPool, job: Uuid, kind: StepKind) -> Value {
    steps_of(pool, job)
        .await
        .into_iter()
        .find(|s| s.0 == kind.as_str())
        .and_then(|s| s.4)
        .unwrap_or(Value::Null)
}

/// The text strata received for `job` (by its source filename and store; one delivery each).
async fn delivered(e: &Env, file: &str) -> String {
    let rec = e.received.lock().await;
    let bodies: Vec<&Value> = rec
        .iter()
        .map(|(_, b)| b)
        .filter(|b| b["source"] == json!(file))
        .collect();
    assert_eq!(bodies.len(), 1, "{file}: one delivery");
    bodies[0]["text"].as_str().expect("text").to_string()
}

fn ocr_steps(steps: &[(String, String, String, i32, Option<Value>)]) -> Vec<String> {
    steps
        .iter()
        .filter(|s| s.0 == StepKind::OcrPages.as_str())
        .map(|s| s.1.clone())
        .collect()
}

// ---------------------------------------------------------------------------------------------
// corpus

#[tokio::test]
async fn corpus_born_digital_formats() {
    let _lane = DOCLING_LANE.lock().await;
    let x = expected();
    // A counting OCR provider is configured: born-digital text must never reach it.
    let (ocr, ocr_state) = mock_ocr().await;
    let e = env(Providers::default(), Some(&ocr)).await;
    let mut jobs = Vec::new();
    for file in &x.born_digital.files {
        jobs.push(upload(&e.ctx, file, options("auto", false)).await);
    }
    let statuses = run_jobs(&e.ctx, &jobs, default_runner()).await;

    let mut problems = Vec::new();
    for ((file, &job), (status, error)) in x.born_digital.files.iter().zip(&jobs).zip(&statuses) {
        if status != "completed" {
            problems.push(format!("{file}: {status} {error:?}"));
            continue;
        }
        let text = delivered(&e, file).await;
        let gone = missing(&text, x.all_snippets());
        let rows = missing_rows(&text, &x.table.rows);
        let steps = steps_of(&e.db.pool, job).await;
        if !gone.is_empty() || !rows.is_empty() || !ocr_steps(&steps).is_empty() {
            problems.push(format!(
                "{file}: missing snippets {gone:?}, table rows {rows:?}, ocr steps {:?}\n{text}",
                ocr_steps(&steps)
            ));
        }
        let convert = step_output(&e.db.pool, job, StepKind::Convert).await;
        eprintln!(
            "{file}: pages {} low_text {} figures {}",
            convert["page_count"], convert["low_text"], convert["figures"]
        );
        // Plan Task 11 Step 5, figure threshold (8 KiB): the ~19 KB chart is kept, the 138-byte
        // icon dropped. Measured 2026-10-16: docling v1.37 exports both from pdf/docx/odt; it
        // exports no images from html/epub at all, and pandoc's pptx carries only the icon.
        let figures = &convert["figures"];
        let links = text.matches("](asset:").count();
        let want = match file.rsplit('.').next() {
            Some("pdf" | "docx" | "odt") => (json!(1), json!(1), 1),
            Some("pptx") => (json!(0), json!(1), 0),
            _ => (json!(0), json!(0), 0),
        };
        assert_eq!(
            (&figures["kept"], &figures["dropped_small"], links),
            (&want.0, &want.1, want.2),
            "{file}: {figures}\n{text}"
        );
        if file.ends_with(".pdf") {
            assert_eq!(convert["page_count"], json!(3), "{file}: {convert}");
            assert_eq!(convert["low_text"], json!([]), "{file}: {convert}");
            let pages = bundle_pages(&text);
            for n in 1..=3 {
                let gone = missing(&pages[n - 1], x.page(n));
                assert!(gone.is_empty(), "{file} page {n}: missing {gone:?}");
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n\n"));
    assert!(
        ocr_state.calls.lock().await.is_empty(),
        "born-digital documents reached the OCR provider"
    );
}

#[tokio::test]
#[ignore = "provider path needs a pcontract fake: set PCONTRACT_FAKE_URL, or PCONTRACT_FAKE_IMAGE=name:tag (estate registry image)"]
async fn corpus_scanned_pdf_with_provider() {
    let _lane = DOCLING_LANE.lock().await;
    let e = env(
        Providers {
            ocr: true,
            ..Providers::default()
        },
        None,
    )
    .await;
    let job = upload(&e.ctx, "scanned.pdf", options("auto", false)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let steps = steps_of(&e.db.pool, job).await;
    assert_eq!(ocr_steps(&steps), ["p0-2"]);
    let convert = step_output(&e.db.pool, job, StepKind::Convert).await;
    assert_eq!(convert["low_text"], json!([0, 1, 2]), "{convert}");
    // docling's text pass does not export a scanned page as a page-sized picture (measured on
    // v1.37): the one figure it finds is the chart region of page 2 (640x400 drawn, landscape),
    // not a portrait page, so a scan is not OCR'd and captioned twice.
    assert_eq!(convert["figures"]["kept"], json!(1), "{convert}");
    let (key, page): (String, i32) = sqlx::query_as(
        "SELECT object_key, page FROM ingest_assets WHERE job_id = $1 AND kind = 'figure'",
    )
    .bind(job)
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(page, 1);
    let png = e
        .ctx
        .storage
        .get_bytes(&key, u64::MAX)
        .await
        .expect("figure object");
    assert_eq!(&png[..8], b"\x89PNG\r\n\x1a\n", "a PNG figure");
    let (w, h) = (
        u32::from_be_bytes(png[16..20].try_into().unwrap()),
        u32::from_be_bytes(png[20..24].try_into().unwrap()),
    );
    eprintln!(
        "scanned.pdf figure on page 2: {w}x{h} px, {} bytes",
        png.len()
    );
    assert!(
        w as f64 / h as f64 > 1.2,
        "a landscape chart, not a page: {w}x{h}"
    );
    assert_eq!(convert["page_count"], json!(3), "{convert}");
    let text = delivered(&e, "scanned.pdf").await;
    let pages = bundle_pages(&text);
    assert_eq!(pages.len(), 3, "{text}");
    // The fake's no-text-layer marker, once per page, each under its own anchor.
    assert_eq!(text.matches("has no text layer").count(), 3, "{text}");
    // The fake numbers pages by the 0-based index it was asked for (provider_contracts
    // `fake/ocr.py`): bundle page i+1 must carry exactly the marker of index i.
    for (i, page) in pages.iter().enumerate() {
        assert!(
            page.contains(&format!("[fake-ocr: page {i} has no text layer]")),
            "page {}: {page}",
            i + 1
        );
    }
}

/// scan, text, scan: docling v1.37 returns ONE markdown segment (page 2) for this PDF; the pages
/// are placed by provenance (rulings R30), so each OCR batch lands on its own page.
#[tokio::test]
#[ignore = "provider path needs a pcontract fake: set PCONTRACT_FAKE_URL, or PCONTRACT_FAKE_IMAGE=name:tag (estate registry image)"]
async fn corpus_alternating_pdf_places_ocr_on_the_right_pages() {
    let _lane = DOCLING_LANE.lock().await;
    let x = expected();
    let e = env(
        Providers {
            ocr: true,
            ..Providers::default()
        },
        None,
    )
    .await;
    let job = upload(&e.ctx, "alternating.pdf", options("auto", false)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let convert = step_output(&e.db.pool, job, StepKind::Convert).await;
    assert_eq!(convert["page_count"], json!(3), "{convert}");
    assert_eq!(convert["low_text"], json!([0, 2]), "{convert}");
    let steps = steps_of(&e.db.pool, job).await;
    assert_eq!(ocr_steps(&steps), ["p0-0", "p2-2"], "{steps:?}");

    let text = delivered(&e, "alternating.pdf").await;
    let pages = bundle_pages(&text);
    assert_eq!(pages.len(), 3, "{text}");
    assert!(
        pages[0].contains("[fake-ocr: page 0 has no text layer]"),
        "{text}"
    );
    assert!(
        pages[2].contains("[fake-ocr: page 2 has no text layer]"),
        "{text}"
    );
    assert!(!pages[1].contains("fake-ocr"), "{text}");
    let gone = missing(&pages[1], x.page(2));
    assert!(gone.is_empty(), "page 2: missing {gone:?}\n{text}");
}

#[tokio::test]
async fn corpus_scanned_pdf_without_provider() {
    let _lane = DOCLING_LANE.lock().await;
    let x = expected();
    let e = env(Providers::default(), None).await;
    let job = upload(&e.ctx, "scanned.pdf", options("auto", false)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let steps = steps_of(&e.db.pool, job).await;
    let ocr = ocr_steps(&steps);
    assert!(
        !ocr.is_empty() && ocr.iter().all(|k| k.starts_with("cpu-p")),
        "batched docling-cpu OCR steps: {ocr:?}"
    );
    let text = delivered(&e, "scanned.pdf").await;
    let all = x.all_snippets();
    let found = all.len() - missing(&text, all.iter().copied()).len();
    let rate = found as f64 / all.len() as f64;
    // Measured 2026-10-16 (docling-serve-cpu v1.37.0, easyocr, 150 dpi grey scans): see the
    // RATE line printed below and the task-11 report.
    eprintln!(
        "RATE scanned.pdf docling-cpu snippets {found}/{} = {rate:.2}; missing {:?}",
        all.len(),
        missing(&text, all.iter().copied())
    );
    assert!(
        rate >= x.scanned.min_snippet_rate,
        "docling CPU OCR found {found}/{} snippets\n{text}",
        all.len()
    );
    assert_eq!(bundle_pages(&text).len(), 3, "{text}");
}

#[tokio::test]
#[ignore = "provider path needs a pcontract fake: set PCONTRACT_FAKE_URL, or PCONTRACT_FAKE_IMAGE=name:tag (estate registry image)"]
async fn corpus_mixed_pdf_ocr_only_page_2() {
    let _lane = DOCLING_LANE.lock().await;
    let x = expected();
    let e = env(
        Providers {
            ocr: true,
            ..Providers::default()
        },
        None,
    )
    .await;
    let job = upload(&e.ctx, "mixed.pdf", options("auto", false)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    // Plan Task 11 Step 5: the low-text threshold puts exactly page 2 in OCR.
    let convert = step_output(&e.db.pool, job, StepKind::Convert).await;
    eprintln!("mixed.pdf convert: {convert}");
    assert_eq!(convert["page_count"], json!(3), "{convert}");
    assert_eq!(convert["low_text"], json!([1]), "{convert}");
    let steps = steps_of(&e.db.pool, job).await;
    assert_eq!(ocr_steps(&steps), ["p1-1"]);

    let text = delivered(&e, "mixed.pdf").await;
    let pages = bundle_pages(&text);
    assert_eq!(pages.len(), 3, "{text}");
    assert_eq!(text.matches("has no text layer").count(), 1, "{text}");
    assert!(
        pages[1].contains("[fake-ocr: page 1 has no text layer]"),
        "{text}"
    );
    for n in [1, 3] {
        let gone = missing(&pages[n - 1], x.page(n));
        assert!(gone.is_empty(), "page {n}: missing {gone:?}\n{text}");
    }
}

#[tokio::test]
#[ignore = "provider path needs a pcontract fake: set PCONTRACT_FAKE_URL, or PCONTRACT_FAKE_IMAGE=name:tag (estate registry image)"]
async fn corpus_image_ocr_and_caption() {
    let e = env(
        Providers {
            ocr: true,
            vision: true,
            ..Providers::default()
        },
        None,
    )
    .await;
    let job = upload(&e.ctx, "text.png", options("auto", true)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let steps = steps_of(&e.db.pool, job).await;
    assert_eq!(ocr_steps(&steps), ["p0-0"]);
    let captions: Vec<_> = steps
        .iter()
        .filter(|s| s.0 == StepKind::Caption.as_str())
        .collect();
    assert_eq!(captions.len(), 1, "{steps:?}");
    assert_eq!(captions[0].2, "completed", "{steps:?}");

    let text = delivered(&e, "text.png").await;
    assert_eq!(bundle_pages(&text).len(), 1, "{text}");
    assert!(text.contains("[fake-ocr: image sha256:"), "{text}");
    // The caption (the fake's chat answer describing the image) links the original.
    let original: Uuid =
        sqlx::query_scalar("SELECT id FROM ingest_assets WHERE job_id = $1 AND kind = 'original'")
            .bind(job)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    let link_end = format!("](asset:{original})");
    let line = text
        .lines()
        .find(|l| l.contains(&link_end))
        .unwrap_or_else(|| panic!("no caption link to {original}\n{text}"));
    assert!(line.starts_with("![fake"), "{line}");
    assert!(line.contains("image sha256:"), "{line}");
}

#[tokio::test]
#[ignore = "provider path needs a pcontract fake: set PCONTRACT_FAKE_URL, or PCONTRACT_FAKE_IMAGE=name:tag (estate registry image)"]
async fn corpus_audio_transcribed() {
    let e = env(
        Providers {
            transcribe: true,
            ..Providers::default()
        },
        None,
    )
    .await;
    let job = upload(&e.ctx, "speech.wav", options("auto", false)).await;
    let statuses = run_jobs(&e.ctx, &[job], default_runner()).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let steps = steps_of(&e.db.pool, job).await;
    let segments: Vec<_> = steps
        .iter()
        .filter(|s| s.0 == StepKind::TranscribeSegment.as_str())
        .map(|s| (s.1.as_str(), s.2.as_str()))
        .collect();
    assert_eq!(segments, [("s0000", "completed")], "{steps:?}");
    let text = delivered(&e, "speech.wav").await;
    assert!(text.contains("## 00:00–"), "{text}");
    assert!(text.contains("fake transcript "), "{text}");
    assert!(!text.contains("<!-- page:"), "{text}");
    // Every transcript line sits under a section header.
    let first_header = text.find("## 00:00–").unwrap();
    assert!(
        text.find("fake transcript").unwrap() > first_header,
        "{text}"
    );
}

// ---------------------------------------------------------------------------------------------
// retry and crash

/// Counts runs per `(kind, key)` and hands each to `inner`.
type Runs = Arc<std::sync::Mutex<HashMap<(String, String), usize>>>;

fn counting(runs: Runs, inner: StepRunner) -> StepRunner {
    Arc::new(move |ctx: Arc<Ctx>, step: ClaimedStep| {
        *runs
            .lock()
            .unwrap()
            .entry((step.kind.as_str().to_string(), step.key.clone()))
            .or_default() += 1;
        inner(ctx, step)
    })
}

fn runs_of(runs: &Runs, kind: StepKind) -> HashMap<String, usize> {
    runs.lock()
        .unwrap()
        .iter()
        .filter(|((k, _), _)| k == kind.as_str())
        .map(|((_, key), n)| (key.clone(), *n))
        .collect()
}

#[tokio::test]
async fn retry_does_not_rerun_completed_steps() {
    let _lane = DOCLING_LANE.lock().await;
    let (ocr, state) = mock_ocr().await;
    state.fail_page_2.store(true, Ordering::SeqCst);
    let e = env(Providers::default(), Some(&ocr)).await;
    // Pages 1 and 3 are scans: two OCR batches, p0-0 and p2-2.
    let job = upload(&e.ctx, "alternating.pdf", options("auto", false)).await;
    let runs: Runs = Arc::default();
    let statuses = run_jobs(&e.ctx, &[job], counting(runs.clone(), default_runner())).await;
    assert_eq!(statuses[0].0, "failed", "{:?}", statuses[0].1);
    let steps = steps_of(&e.db.pool, job).await;
    assert_eq!(ocr_steps(&steps), ["p0-0", "p2-2"], "{steps:?}");
    let failed = steps.iter().find(|s| s.1 == "p2-2").unwrap();
    assert_eq!(failed.2, "failed", "{steps:?}");

    // The provider is fixed; the job is retried.
    state.fail_page_2.store(false, Ordering::SeqCst);
    jobs::retry(&e.db.pool, tenant(), job).await.expect("retry");
    let statuses = run_jobs(&e.ctx, &[job], counting(runs.clone(), default_runner())).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    let calls = state.calls.lock().await.clone();
    assert_eq!(calls.get("[0]"), Some(&1), "batch 1 called once: {calls:?}");
    assert_eq!(calls.get("[2]"), Some(&2), "{calls:?}");
    for kind in [
        StepKind::Acquire,
        StepKind::Scan,
        StepKind::Detect,
        StepKind::Convert,
    ] {
        assert_eq!(
            runs_of(&runs, kind).values().sum::<usize>(),
            1,
            "{} re-ran",
            kind.as_str()
        );
    }
    let ocr_runs = runs_of(&runs, StepKind::OcrPages);
    assert_eq!(ocr_runs.get("p0-0"), Some(&1), "{ocr_runs:?}");
    assert_eq!(ocr_runs.get("p2-2"), Some(&2), "{ocr_runs:?}");
    let text = delivered(&e, "alternating.pdf").await;
    let pages = bundle_pages(&text);
    assert!(pages[0].contains("mock ocr page 0"), "{text}");
    assert!(pages[2].contains("mock ocr page 2"), "{text}");
}

#[tokio::test]
async fn crash_mid_step_resumes() {
    let _lane = DOCLING_LANE.lock().await;
    let (ocr, state) = mock_ocr().await;
    let e = env(Providers::default(), Some(&ocr)).await;
    let job = upload(&e.ctx, "mixed.pdf", options("auto", false)).await;

    // First pool: the OcrPages step hangs mid-run, as in a worker that died there.
    let runs: Runs = Arc::default();
    let entered = Arc::new(tokio::sync::Notify::new());
    let hang: StepRunner = {
        let entered = entered.clone();
        Arc::new(move |ctx: Arc<Ctx>, step: ClaimedStep| {
            let entered = entered.clone();
            Box::pin(async move {
                if step.kind == StepKind::OcrPages {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                strata_ingest::pipeline::run_step(&ctx, &step).await
            })
        })
    };
    let shutdown = CancellationToken::new();
    // No heartbeat within the test: a dead worker does not extend its lease.
    let dead = WorkerOpts {
        heartbeat: Duration::from_secs(3600),
        ..opts()
    };
    let h = start_pool(&e.ctx, &shutdown, dead, counting(runs.clone(), hang));
    tokio::time::timeout(JOB_DEADLINE, entered.notified())
        .await
        .expect("OcrPages started");
    h.abort();
    let _ = h.await;

    // The lease runs out; a new pool reclaims the step and finishes the job.
    let expired = sqlx::query(
        "UPDATE ingest_steps SET lease_until = now() - interval '1 second'
         WHERE job_id = $1 AND kind = 'ocr_pages' AND status = 'running'",
    )
    .bind(job)
    .execute(&e.db.pool)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(expired, 1);
    let statuses = run_jobs(&e.ctx, &[job], counting(runs.clone(), default_runner())).await;
    assert_eq!(statuses[0].0, "completed", "{:?}", statuses[0].1);

    for kind in [
        StepKind::Acquire,
        StepKind::Scan,
        StepKind::Detect,
        StepKind::Convert,
    ] {
        assert_eq!(
            runs_of(&runs, kind).values().sum::<usize>(),
            1,
            "{} re-ran",
            kind.as_str()
        );
    }
    assert_eq!(
        runs_of(&runs, StepKind::OcrPages).get("p1-1"),
        Some(&2),
        "{runs:?}"
    );
    let steps = steps_of(&e.db.pool, job).await;
    for s in &steps {
        let want = if s.0 == StepKind::OcrPages.as_str() {
            2
        } else {
            1
        };
        assert_eq!((s.0.as_str(), s.3), (s.0.as_str(), want), "{steps:?}");
    }
    // The hung first run never reached the provider; the resumed one did, once.
    assert_eq!(
        state.calls.lock().await.get("[1]"),
        Some(&1),
        "{:?}",
        state.calls.lock().await
    );
    let text = delivered(&e, "mixed.pdf").await;
    assert!(bundle_pages(&text)[1].contains("mock ocr page 1"), "{text}");
}
