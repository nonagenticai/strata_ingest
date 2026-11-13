//! Pipeline steps Acquire → Scan → Detect (→ Convert) driven through the real worker loop, on a
//! real Postgres with an in-process storage_service (`FakeStorage`: verdicts, request capture) and
//! a fake webscraper / docling.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::{FakeStorage, STORAGE_BUCKET, TestDb, Verdict};
use strata_ingest::IngestError;
use strata_ingest::config::{Config, Mode, ProvidersFile, Region, Secret};
use strata_ingest::db::artifacts;
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::steps::{ClaimedStep, StepOutput};
use strata_ingest::limits::Limits;
use strata_ingest::pipeline::detect::Detected;
use strata_ingest::pipeline::{Ctx, original_key};
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{StepRunner, WorkerOpts, run_workers_with};

const EICAR: &[u8] = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*";

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-000000000001").unwrap()
}

/// Nothing listens on port 9 (discard); docling and strata are not reached by these steps.
const UNUSED_URL: &str = "http://127.0.0.1:9";

fn config(db: &TestDb, storage: &FakeStorage, webscraper: &str) -> Config {
    Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![(tenant(), Secret::new("api-token"))],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: UNUSED_URL.to_string(),
        webscraper_url: webscraper.to_string(),
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

fn fast() -> WorkerOpts {
    WorkerOpts {
        poll: Duration::from_millis(50),
        heartbeat: Duration::from_secs(60),
        deadline: None,
        ..WorkerOpts::default()
    }
}

/// Runs the real worker pool in the background; cancel + await the handle to stop it.
fn start_workers(
    ctx: Arc<Ctx>,
    n: usize,
    opts: WorkerOpts,
    runner: StepRunner,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let shutdown = CancellationToken::new();
    let h = tokio::spawn(run_workers_with(ctx, n, shutdown.clone(), opts, runner));
    (shutdown, h)
}

async fn stop(shutdown: CancellationToken, h: tokio::task::JoinHandle<()>) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), h)
        .await
        .expect("workers stop after shutdown")
        .expect("worker pool task");
}

/// Polls `check` until it returns `Some`, for up to 30 s.
async fn wait_for<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn job_row(pool: &PgPool, id: Uuid) -> (String, Option<Value>, Value) {
    let r = sqlx::query("SELECT status, error, meta FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get("status"), r.get("error"), r.get("meta"))
}

async fn wait_job_status(pool: &PgPool, id: Uuid, want: &str) -> (Option<Value>, Value) {
    wait_for(&format!("job {id} to become {want}"), || async {
        let (status, error, meta) = job_row(pool, id).await;
        (status == want).then_some((error, meta))
    })
    .await
}

/// `(kind, status, input, output)` per step, in queue order.
async fn steps_of(pool: &PgPool, job: Uuid) -> Vec<(String, String, Value, Option<Value>)> {
    sqlx::query(
        "SELECT kind, status, input, output FROM ingest_steps WHERE job_id = $1 ORDER BY stage, id",
    )
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("kind"),
            r.get("status"),
            r.get("input"),
            r.get("output"),
        )
    })
    .collect()
}

async fn assets_of(pool: &PgPool, job: Uuid) -> Vec<(String, String, i64, String)> {
    sqlx::query("SELECT kind, object_key, bytes, mime FROM ingest_assets WHERE job_id = $1")
        .bind(job)
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get("kind"),
                r.get("object_key"),
                r.get("bytes"),
                r.get("mime"),
            )
        })
        .collect()
}

/// As the API does it (Task 10): store the original at its key (storage_service scans it) and
/// create the job with the verdict it got.
async fn upload_job(ctx: &Ctx, filename: &str, mime: &str, body: Vec<u8>) -> Uuid {
    let id = Uuid::now_v7();
    let len = body.len();
    let up = ctx
        .storage
        .upload_bytes(&original_key(tenant(), id), Bytes::from(body), mime)
        .await
        .expect("put original");
    jobs::create_with_id(
        &ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": filename, "mime": mime, "bytes": len,
                           "scan": up.verdict().as_str()}),
            options: json!({"ocr": "auto", "captions": false, "language": null}),
        },
    )
    .await
    .expect("create job")
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/detect/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

#[test]
fn detected_round_trips_as_tagged_json() {
    for (d, j) in [
        (Detected::Markdown, json!({"type": "markdown"})),
        (Detected::Pdf, json!({"type": "pdf"})),
        (
            Detected::Image("image/png".into()),
            json!({"type": "image", "mime": "image/png"}),
        ),
        (
            Detected::Audio("audio/mpeg".into()),
            json!({"type": "audio", "mime": "audio/mpeg"}),
        ),
    ] {
        assert_eq!(serde_json::to_value(&d).unwrap(), j);
        assert_eq!(serde_json::from_value::<Detected>(j).unwrap(), d);
    }
    assert_eq!(Detected::Markdown.mime(), "text/markdown");
    assert_eq!(Detected::Audio("audio/mpeg".into()).mime(), "audio/mpeg");
}

/// A scraper answering every scrape with `markdown`.
async fn scraper_serving(markdown: &'static str) -> String {
    spawn(Router::new().route(
        "/api/scrape/url",
        post(move || async move {
            axum::Json(json!({
                "url": "https://example.com/a",
                "final_url": "https://example.com/a/",
                "title": "Example Domain",
                "markdown_content": markdown,
                "status": "completed"
            }))
        }),
    ))
    .await
}

async fn url_job(pool: &PgPool) -> Uuid {
    jobs::create(
        pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "url", "url": "https://example.com/a"}),
            options: json!({"ocr": "auto", "captions": false, "language": null}),
        },
    )
    .await
    .unwrap()
}

/// Rulings R25: Acquire stores the scraped page through storage_service, which refuses it as
/// infected: the job is `rejected` (`infected`), nothing is stored and nothing follows.
#[tokio::test]
async fn url_job_with_infected_page_is_rejected_and_stores_nothing() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let page: &'static str = Box::leak(String::from_utf8(EICAR.to_vec()).unwrap().into_boxed_str());
    let scraper = scraper_serving(page).await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, &scraper)).unwrap());
    let job = url_job(&db.pool).await;

    let (shutdown, h) = start_workers(
        ctx.clone(),
        2,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let (error, meta) = wait_job_status(&db.pool, job, "rejected").await;
    // Let a worker try to claim more; nothing is left for this job.
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop(shutdown, h).await;

    let error = error.expect("job error");
    assert_eq!(error["code"], "infected");
    assert_eq!(error["message"], common::EICAR_SIGNATURE);
    assert_eq!(error["provider"], "storage");
    assert_eq!(meta["scan"], "infected");
    // storage_service saw the page once and stored nothing; no asset row.
    assert_eq!(storage.uploads().len(), 1);
    assert!(storage.keys().is_empty());
    assert!(assets_of(&db.pool, job).await.is_empty());
    let steps = steps_of(&db.pool, job).await;
    let kinds: Vec<(&str, &str)> = steps.iter().map(|s| (s.0.as_str(), s.1.as_str())).collect();
    assert_eq!(kinds, [("acquire", "completed")]);
    assert_eq!(job_row(&db.pool, job).await.0, "rejected");
}

/// Rulings R25: an original storage_service stored with `virus_scanned=false` (scanning off) is
/// never clean: Scan fails Retryable `scan_unavailable` until its attempts run out, and Detect is
/// never reached. Both entry points: an upload (verdict recorded by the API) and a URL job.
#[tokio::test]
async fn scan_unverified_is_not_clean() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    storage.set_verdict(Verdict::Unscanned);
    let scraper = scraper_serving("# Page\n\nSome text.\n").await;
    let mut cfg = config(&db, &storage, &scraper);
    cfg.limits.max_attempts = 2;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let upload = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let url = url_job(&db.pool).await;

    let (shutdown, h) = start_workers(
        ctx.clone(),
        2,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    for job in [upload, url] {
        let (error, meta) = wait_job_status(&db.pool, job, "failed").await;
        let error = error.expect("job error");
        assert_eq!(error["code"], "scan_unavailable", "{error}");
        assert_eq!(error["class"], "retryable", "{error}");
        assert_eq!(meta["scan"], "unverified");
    }
    stop(shutdown, h).await;
    for job in [upload, url] {
        let (status, attempt, _) = step_state(&db.pool, job, "scan").await.unwrap();
        assert_eq!(
            (status.as_str(), attempt),
            ("failed", 2),
            "retried, never passed"
        );
        assert!(step_state(&db.pool, job, "detect").await.is_none());
    }
}

#[tokio::test]
async fn url_job_is_scanned_then_converted() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let markdown = "# Example Domain\n\nThis domain is for use in examples.\n";
    let scraper = scraper_serving(markdown).await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, &scraper)).unwrap());
    let job = url_job(&db.pool).await;

    let (shutdown, h) = start_workers(
        ctx.clone(),
        2,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let convert_input = wait_for("a convert step", || async {
        steps_of(&db.pool, job)
            .await
            .into_iter()
            .find(|s| s.0 == "convert")
            .map(|s| s.2)
    })
    .await;
    stop(shutdown, h).await;

    assert_eq!(convert_input, json!({"detected": {"type": "markdown"}}));
    let steps = steps_of(&db.pool, job).await;
    // Convert (and what it enqueues) may run before the pool stops; only the first three matter.
    let done: Vec<(&str, &str)> = steps
        .iter()
        .filter(|s| matches!(s.0.as_str(), "acquire" | "scan" | "detect"))
        .map(|s| (s.0.as_str(), s.1.as_str()))
        .collect();
    assert_eq!(
        done,
        [
            ("acquire", "completed"),
            ("scan", "completed"),
            ("detect", "completed")
        ]
    );
    // The scraped markdown is the original, scanned by storage_service as it was stored.
    let key = original_key(tenant(), job);
    assert_eq!(storage.get(&key).unwrap(), markdown.as_bytes());
    let ups = storage.uploads();
    assert_eq!(ups.len(), 1);
    assert_eq!(ups[0].object_key.as_deref(), Some(key.as_str()));
    assert_eq!(ups[0].content_type.as_deref(), Some("text/markdown"));
    assert_eq!(
        assets_of(&db.pool, job).await,
        [(
            "original".to_string(),
            key,
            markdown.len() as i64,
            "text/markdown".to_string()
        )]
    );
    let (_, _, meta) = job_row(&db.pool, job).await;
    assert_eq!(meta["title"], "Example Domain");
    assert_eq!(meta["final_url"], "https://example.com/a/");
    assert_eq!(meta["mime"], "text/markdown");
    assert_eq!(meta["scan"], "clean");
}

#[tokio::test]
async fn audio_without_transcribe_provider_fails_unsupported() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    assert!(
        !ctx.providers
            .has_default(strata_ingest::providers::Capability::Transcribe)
    );
    let job = upload_job(&ctx, "talk.mp3", "audio/mpeg", fixture("sample.mp3")).await;

    let (shutdown, h) = start_workers(
        ctx.clone(),
        1,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let (error, meta) = wait_job_status(&db.pool, job, "failed").await;
    stop(shutdown, h).await;

    let error = error.expect("job error");
    assert_eq!(error["code"], "unsupported_no_provider", "{error}");
    assert_eq!(error["class"], "invalid_request");
    assert!(
        error["message"].as_str().unwrap().contains("audio/mpeg"),
        "{error}"
    );
    let steps = steps_of(&db.pool, job).await;
    let kinds: Vec<(&str, &str)> = steps.iter().map(|s| (s.0.as_str(), s.1.as_str())).collect();
    // Terminal on the first attempt: no retry for a missing provider.
    assert_eq!(
        kinds,
        [
            ("acquire", "completed"),
            ("scan", "completed"),
            ("detect", "failed")
        ]
    );
    assert!(meta.get("title").is_none());
    let asset = &assets_of(&db.pool, job).await[0];
    assert_eq!(
        (asset.0.as_str(), asset.3.as_str()),
        ("original", "audio/mpeg")
    );
}

#[tokio::test]
async fn worker_survives_panicking_step() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    // The first run of any step panics; later runs succeed with no successors.
    let runner: StepRunner = Arc::new(move |_ctx, _step: ClaimedStep| {
        let n = c.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            assert!(n != 0, "boom on the first run");
            Ok((StepOutput::default(), vec![]))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;

    // One worker: if the panic killed it, nothing would run the retry.
    let (shutdown, h) = start_workers(ctx.clone(), 1, fast(), runner);
    wait_job_status(&db.pool, job, "completed").await;
    stop(shutdown, h).await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let r = sqlx::query("SELECT attempt, status FROM ingest_steps WHERE job_id = $1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        (
            r.get::<i32, _>("attempt"),
            r.get::<String, _>("status").as_str()
        ),
        (2, "completed")
    );
}

#[tokio::test]
async fn lost_lease_aborts_the_step_and_drops_its_result() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    struct SetOnDrop(Arc<AtomicBool>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (s, d) = (started.clone(), dropped.clone());
    let runner: StepRunner = Arc::new(move |_ctx, _step: ClaimedStep| {
        let (s, d) = (s.clone(), d.clone());
        Box::pin(async move {
            let _guard = SetOnDrop(d);
            s.store(true, Ordering::SeqCst);
            // Longer than `wait_for`'s 30 s: only an abort (not the run finishing) drops the guard in time.
            tokio::time::sleep(Duration::from_secs(120)).await;
            Ok((
                StepOutput {
                    output: json!({"stale": true}),
                    ..StepOutput::default()
                },
                vec![],
            ))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let opts = WorkerOpts {
        poll: Duration::from_millis(50),
        heartbeat: Duration::from_millis(100),
        deadline: None,
        ..WorkerOpts::default()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 1, opts, runner);
    wait_for("the step to start", || async {
        started.load(Ordering::SeqCst).then_some(())
    })
    .await;
    // Another worker reclaimed the step (as the reaper + a new claim would): attempt moves on.
    sqlx::query("UPDATE ingest_steps SET attempt = attempt + 1 WHERE job_id = $1")
        .bind(job)
        .execute(&db.pool)
        .await
        .unwrap();
    wait_for("the stale run to be aborted", || async {
        dropped.load(Ordering::SeqCst).then_some(())
    })
    .await;
    stop(shutdown, h).await;

    let r = sqlx::query("SELECT attempt, status, output FROM ingest_steps WHERE job_id = $1")
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(r.get::<i32, _>("attempt"), 2);
    assert_eq!(
        r.get::<String, _>("status"),
        "running",
        "the new owner's run is untouched"
    );
    assert_eq!(
        r.get::<Option<Value>, _>("output"),
        None,
        "the stale result was dropped"
    );
    assert_eq!(job_row(&db.pool, job).await.0, "running");
}

// ---------------------------------------------------------------------------------------------
// Fix round 1: zip detection over a full download, step deadlines, unique assets, retry/cancel

use std::io::{Cursor, Write};
use std::sync::atomic::AtomicU64;

use strata_ingest::db::assets::{self, AssetKind, NewAsset};
use strata_ingest::db::steps;
use strata_ingest::pipeline::detect;
use zip::write::SimpleFileOptions;

/// The worker's zip path (rulings R27): `body` written to a scratch file, then
/// `detect::sniff_zip_file`.
fn file_sniff(body: &[u8]) -> Result<Detected, IngestError> {
    let mut f = tempfile::tempfile().unwrap();
    f.write_all(body).unwrap();
    detect::sniff_zip_file(f)
}

/// Incompressible filler.
fn noise(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// `src` (a zip) re-written with a stored `pad` entry of `pad` bytes after its first entry, so
/// the central directory and later entries sit `pad` bytes further in.
fn padded(src: &[u8], pad: usize, large: bool) -> Vec<u8> {
    let mut input = zip::ZipArchive::new(Cursor::new(src)).unwrap();
    let mut out = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for i in 0..input.len() {
        out.raw_copy_file(input.by_index_raw(i).unwrap()).unwrap();
        if i == 0 {
            let opts = SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored)
                .large_file(large);
            out.start_file("zz-padding/pad.bin", opts).unwrap();
            out.write_all(&noise(pad)).unwrap();
        }
    }
    out.finish().unwrap().into_inner()
}

fn verdict(r: Result<Detected, IngestError>) -> String {
    match r {
        Ok(d) => format!("{d:?}"),
        Err(e) => format!("err {e}"),
    }
}

#[test]
fn file_zip_sniff_matches_full_sniff_on_every_zip_fixture() {
    let dir = format!("{}/tests/fixtures/detect", env!("CARGO_MANIFEST_DIR"));
    let mut seen = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let body = std::fs::read(&path).unwrap();
        if !detect::is_zip(&body) {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        seen += 1;
        let mut variants = vec![
            ("as is", body.clone()),
            ("truncated", body[..body.len() / 2].to_vec()),
        ];
        if zip::ZipArchive::new(Cursor::new(&body[..])).is_ok() {
            variants.push(("padded 1 MiB", padded(&body, 1 << 20, false)));
            variants.push(("padded zip64", padded(&body, 300 * 1024, true)));
        }
        for (what, v) in variants {
            let full = verdict(detect::sniff(&v, Some(&name)));
            let from_file = verdict(file_sniff(&v));
            assert_eq!(from_file, full, "{name} ({what})");
        }
    }
    assert!(seen >= 6, "zip fixtures found: {seen}");
}

/// Rulings R27: storage_service has no Range reads, so Detect downloads a zip container whole
/// (once: the head and the rest come from the same GET) into a bounded scratch file and
/// classifies it from there.
#[tokio::test]
async fn zip_container_detected_from_full_download() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let docx = padded(&fixture("sample.docx"), 20 << 20, false);
    let size = docx.len() as u64;
    let job = upload_job(&ctx, "report.docx", "application/octet-stream", docx).await;
    // The real steps, measuring what Detect reads from storage (one worker: nothing overlaps).
    let detect_read = Arc::new(AtomicU64::new(u64::MAX));
    let dr = detect_read.clone();
    let runner: StepRunner = Arc::new(move |ctx: Arc<Ctx>, step: ClaimedStep| {
        let dr = dr.clone();
        Box::pin(async move {
            let before = ctx.storage.bytes_read();
            let r = strata_ingest::pipeline::run_step(&ctx, &step).await;
            if step.kind == steps::StepKind::Detect {
                dr.store(ctx.storage.bytes_read() - before, Ordering::SeqCst);
            }
            r
        })
    });
    let (shutdown, h) = start_workers(ctx.clone(), 1, fast(), runner);
    let input = wait_for("a convert step", || async {
        steps_of(&db.pool, job)
            .await
            .into_iter()
            .find(|s| s.0 == "convert")
            .map(|s| s.2)
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!(input, json!({"detected": {"type": "docx"}}));
    let read = detect_read.load(Ordering::SeqCst);
    // Head and rest from one GET: exactly the object, once (Convert downloads it again later).
    assert_eq!(read, size, "Detect read the {size}-byte docx exactly once");

    // A zip over max_upload_bytes is cut off while downloading: terminal `too_large`.
    let mut cfg = config(&db, &storage, UNUSED_URL);
    cfg.limits.max_upload_bytes = 1 << 20;
    let small = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let big = padded(&fixture("sample.docx"), 2 << 20, false);
    let job = upload_job(&ctx, "big.docx", "application/octet-stream", big).await;
    sqlx::query("UPDATE ingest_steps SET status = 'completed' WHERE job_id = $1")
        .bind(job)
        .execute(&db.pool)
        .await
        .unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    steps::enqueue(&mut tx, job, steps::StepKind::Detect, "", json!({}))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let step = steps::claim_next(&db.pool, 5).await.unwrap().unwrap();
    assert_eq!(step.kind, steps::StepKind::Detect);
    let e = strata_ingest::pipeline::run_step(&small, &step)
        .await
        .unwrap_err();
    assert_eq!(e.code, "too_large", "{e:?}");
}

/// `raw` (a [`raw_zip`] archive) with its end-of-central-directory record replaced by a zip64
/// record declaring `entries` entries, its locator and an EOCD pointing at them.
fn as_zip64(raw: &[u8], entries: u64) -> Vec<u8> {
    let eocd = raw.len() - 22;
    assert_eq!(&raw[eocd..eocd + 4], b"PK\x05\x06");
    let cd_size = u64::from(u32::from_le_bytes(
        raw[eocd + 12..eocd + 16].try_into().unwrap(),
    ));
    let cd_off = u64::from(u32::from_le_bytes(
        raw[eocd + 16..eocd + 20].try_into().unwrap(),
    ));
    let mut out = raw[..eocd].to_vec();
    let rec_off = out.len() as u64;
    out.extend_from_slice(b"PK\x06\x06");
    out.extend_from_slice(&44u64.to_le_bytes());
    out.extend_from_slice(&45u16.to_le_bytes()); // made by
    out.extend_from_slice(&45u16.to_le_bytes()); // needed
    out.extend_from_slice(&[0; 8]); // this disk, cd disk
    out.extend_from_slice(&entries.to_le_bytes()); // on this disk
    out.extend_from_slice(&entries.to_le_bytes()); // total
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_off.to_le_bytes());
    out.extend_from_slice(b"PK\x06\x07");
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&rec_off.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(b"PK\x05\x06");
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&0xFFFFu16.to_le_bytes());
    out.extend_from_slice(&0xFFFFu16.to_le_bytes());
    out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

/// The central-directory caps hold on the file path: a directory over `MAX_CENTRAL_DIR`, or one
/// declaring more than `MAX_ZIP_ENTRIES` entries (zip64), is `unsupported_type` before the zip
/// crate parses it.
#[test]
fn oversized_central_directory_is_unsupported() {
    let unsupported = |r: Result<Detected, IngestError>| matches!(r, Err(IngestError::UnsupportedType(m)) if m == "application/zip");
    let epub = raw_zip(&[("mimetype", b"application/epub+zip")], &[("mimetype", 0)]);
    // The zip64 record is followed: an honest one classifies as the plain archive does.
    assert_eq!(file_sniff(&epub).unwrap(), Detected::Epub);
    assert_eq!(file_sniff(&as_zip64(&epub, 1)).unwrap(), Detected::Epub);
    assert!(unsupported(file_sniff(&as_zip64(
        &epub,
        detect::MAX_ZIP_ENTRIES + 1
    ))));
    // A central directory just over 8 MiB of (duplicate) entries.
    let long: &'static str = Box::leak("n".repeat(250).into_boxed_str());
    let entries = (detect::MAX_CENTRAL_DIR as usize / (46 + 250)) + 10;
    assert!(entries < 65_535);
    let big_cd = vec![(long, 0usize); entries];
    let zip = raw_zip(&[(long, b"x")], &big_cd);
    assert!(unsupported(file_sniff(&zip)));
}

/// `(status, attempt, error)` of the job's step of `kind`.
async fn step_state(pool: &PgPool, job: Uuid, kind: &str) -> Option<(String, i32, Option<Value>)> {
    sqlx::query("SELECT status, attempt, error FROM ingest_steps WHERE job_id = $1 AND kind = $2")
        .bind(job)
        .bind(kind)
        .fetch_optional(pool)
        .await
        .unwrap()
        .map(|r| (r.get("status"), r.get("attempt"), r.get("error")))
}

#[tokio::test]
async fn step_past_its_deadline_is_retried_as_step_timeout() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let runner: StepRunner = Arc::new(|_ctx, _step: ClaimedStep| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(120)).await;
            Ok((StepOutput::default(), vec![]))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let opts = WorkerOpts {
        deadline: Some(Duration::from_millis(200)),
        ..fast()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 1, opts, runner);
    let (attempt, error) = wait_for("the step to time out", || async {
        match step_state(&db.pool, job, "acquire").await {
            Some((s, a, Some(e))) if s == "pending" => Some((a, e)),
            _ => None,
        }
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!(attempt, 1);
    assert_eq!(error["code"], "step_timeout");
    assert_eq!(error["class"], "retryable");
    assert_eq!(job_row(&db.pool, job).await.0, "running");
    // Production ceilings, per kind.
    use strata_ingest::db::steps::StepKind;
    use strata_ingest::limits::step_deadline;
    assert_eq!(step_deadline(StepKind::Convert), Duration::from_secs(1900));
    assert_eq!(step_deadline(StepKind::Caption), Duration::from_secs(120));
    assert_eq!(step_deadline(StepKind::Scan), Duration::from_secs(300));
}

/// A step queued behind a busy one-permit provider is served: the time it waits for the permit
/// does not count against its deadline (R35). Without the pause it would die `step_timeout`
/// 200 ms into a 600 ms wait.
#[tokio::test]
async fn deadline_does_not_count_the_wait_for_a_provider_permit() {
    use strata_ingest::providers::Limited;
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let gpu = Arc::new(Limited::new((), 1, None));
    // Another call holds the only permit for 600 ms.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let other = gpu.clone();
    tokio::spawn(async move {
        let (_p, _) = other.acquire().await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(600)).await;
    });
    rx.await.unwrap();
    let g = gpu.clone();
    let runner: StepRunner = Arc::new(move |_ctx, _step: ClaimedStep| {
        let g = g.clone();
        Box::pin(async move {
            let (_p, _) = g.acquire().await;
            // 100 ms of work: well inside the 200 ms deadline once the wait is excluded.
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok((
                StepOutput {
                    output: json!({"served": true}),
                    ..StepOutput::default()
                },
                vec![],
            ))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let opts = WorkerOpts {
        deadline: Some(Duration::from_millis(200)),
        ..fast()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 1, opts, runner);
    let (status, attempt, error) = wait_for("the step to end", || async {
        match step_state(&db.pool, job, "acquire").await {
            Some((s, a, e)) if s != "running" && (s != "pending" || e.is_some()) => Some((s, a, e)),
            _ => None,
        }
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!((status.as_str(), attempt, error), ("completed", 1, None));
}

/// A wait for a slot is bounded on its own: past `slot_wait_max` the run is abandoned as
/// Retryable `slot_wait_timeout` (R35), even though the step never started working.
#[tokio::test]
async fn a_slot_wait_past_its_bound_is_retried_as_slot_wait_timeout() {
    use strata_ingest::providers::Limited;
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let gpu = Arc::new(Limited::new((), 1, None));
    let held = gpu.clone();
    let (_permit, _) = held.acquire().await; // never released during the test
    let g = gpu.clone();
    let runner: StepRunner = Arc::new(move |_ctx, _step: ClaimedStep| {
        let g = g.clone();
        Box::pin(async move {
            let (_p, _) = g.acquire().await;
            Ok((StepOutput::default(), vec![]))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let opts = WorkerOpts {
        deadline: Some(Duration::from_secs(60)),
        slot_wait_max: Duration::from_millis(300),
        ..fast()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 1, opts, runner);
    let (attempt, error) = wait_for("the wait to time out", || async {
        match step_state(&db.pool, job, "acquire").await {
            Some((s, a, Some(e))) if s == "pending" => Some((a, e)),
            _ => None,
        }
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!(attempt, 1);
    assert_eq!(error["code"], "slot_wait_timeout", "{error}");
    assert_eq!(error["class"], "retryable");
    // Re-queued without using an attempt (counted in slot_waits instead).
    let slot_waits: i32 = sqlx::query_scalar(
        "SELECT slot_waits FROM ingest_steps WHERE job_id = $1 AND kind = 'acquire'",
    )
    .bind(job)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(slot_waits >= 1, "{slot_waits}");
}

/// Shutdown (a deploy) aborts a long step after the grace and RELEASES it at once — pending,
/// due now, attempt not used up — instead of leaving it `running` for a 10-minute lease (R39).
#[tokio::test]
async fn shutdown_releases_a_running_step_for_another_worker() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let started = Arc::new(AtomicBool::new(false));
    let s = started.clone();
    let runner: StepRunner = Arc::new(move |_ctx, _step: ClaimedStep| {
        let s = s.clone();
        Box::pin(async move {
            s.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(600)).await;
            Ok((StepOutput::default(), vec![]))
        })
    });
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    let opts = WorkerOpts {
        shutdown_grace: Duration::from_millis(200),
        ..fast()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 1, opts, runner);
    wait_for("the step to start", || async {
        started.load(Ordering::SeqCst).then_some(())
    })
    .await;
    let t0 = std::time::Instant::now();
    stop(shutdown, h).await;
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "the pool stops after the grace, not after the step: {:?}",
        t0.elapsed()
    );
    let r = sqlx::query(
        "SELECT status, attempt, releases, lease_until IS NULL AS free, run_after <= now() AS due,
                error
         FROM ingest_steps WHERE job_id = $1",
    )
    .bind(job)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(r.get::<String, _>("status"), "pending");
    assert_eq!(
        r.get::<i32, _>("attempt"),
        1,
        "attempt is the fence token: never decremented"
    );
    assert_eq!(
        r.get::<i32, _>("releases"),
        1,
        "the run is refunded instead"
    );
    assert!(r.get::<bool, _>("free") && r.get::<bool, _>("due"));
    assert_eq!(r.get::<Option<Value>, _>("error"), None);
    // Another worker (pod) claims it at once.
    let c = strata_ingest::db::steps::claim_next(&db.pool, 10)
        .await
        .unwrap()
        .expect("claimable now");
    assert_eq!(c.attempt, 2);
}

/// storage_service answers 503 (scanner down) to Acquire's store of a URL job's page: Acquire is
/// re-queued as Retryable `storage_unavailable`, nothing is stored, no Scan is enqueued; an
/// unreadable original (500 on GET) makes Detect retryable too, never a verdict.
#[tokio::test]
async fn storage_503_retries_acquire_and_enqueues_no_scan() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    storage.set_verdict(Verdict::Unavailable);
    let scraper = scraper_serving("# Page\n\nSome text.\n").await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, &scraper)).unwrap());
    let job = url_job(&db.pool).await;
    let (shutdown, h) = start_workers(
        ctx.clone(),
        2,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let (attempt, error) = wait_for("acquire to be re-queued", || async {
        match step_state(&db.pool, job, "acquire").await {
            Some((s, a, Some(e))) if s == "pending" => Some((a, e)),
            _ => None,
        }
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!(attempt, 1);
    assert_eq!(error["code"], "storage_unavailable");
    assert_eq!(error["class"], "retryable");
    assert_eq!(error["provider"], "storage");
    assert!(step_state(&db.pool, job, "scan").await.is_none());
    assert!(storage.keys().is_empty());
    assert_eq!(job_row(&db.pool, job).await.0, "running");

    // A stored, clean upload whose original cannot be read back: Detect retries.
    storage.set_verdict(Verdict::Clean);
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    storage.set_reads_unavailable(true);
    let (shutdown, h) = start_workers(
        ctx.clone(),
        2,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let (attempt, error) = wait_for("detect to be re-queued", || async {
        match step_state(&db.pool, job, "detect").await {
            Some((s, a, Some(e))) if s == "pending" => Some((a, e)),
            _ => None,
        }
    })
    .await;
    stop(shutdown, h).await;
    assert_eq!(attempt, 1);
    assert_eq!(error["code"], "storage_unavailable");
    assert_eq!(error["class"], "retryable");
    assert!(step_state(&db.pool, job, "convert").await.is_none());
    // The original is untouched.
    assert_eq!(
        storage.get(&original_key(tenant(), job)).unwrap(),
        &b"hello"[..]
    );
}

#[tokio::test]
async fn cancelled_job_gets_no_more_steps_from_the_running_pool() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    let job = upload_job(&ctx, "a.txt", "text/plain", b"hello".to_vec()).await;
    // A second pending step, gated behind Acquire.
    let mut tx = db.pool.begin().await.unwrap();
    steps::enqueue(&mut tx, job, steps::StepKind::Detect, "extra", json!({}))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let (c, r) = (calls.clone(), release.clone());
    // Acquire blocks until released, then asks for a Scan.
    let runner: StepRunner = Arc::new(move |_ctx, step: ClaimedStep| {
        c.fetch_add(1, Ordering::SeqCst);
        let r = r.clone();
        Box::pin(async move {
            if step.kind == steps::StepKind::Acquire {
                r.notified().await;
            }
            Ok((
                StepOutput::default(),
                vec![(steps::StepKind::Scan, String::new(), json!({}))],
            ))
        })
    });
    let (shutdown, h) = start_workers(ctx.clone(), 2, fast(), runner);
    wait_for("acquire to run", || async {
        (calls.load(Ordering::SeqCst) == 1).then_some(())
    })
    .await;
    jobs::cancel(&db.pool, tenant(), job).await.unwrap();
    release.notify_one();
    // Give the pool time to (wrongly) pick anything up.
    tokio::time::sleep(Duration::from_millis(800)).await;
    stop(shutdown, h).await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "only Acquire ever ran");
    assert_eq!(job_row(&db.pool, job).await.0, "cancelled");
    assert!(
        step_state(&db.pool, job, "scan").await.is_none(),
        "no successor for a cancelled job"
    );
    assert_eq!(
        step_state(&db.pool, job, "detect").await.unwrap().0,
        "cancelled"
    );
}

#[tokio::test]
async fn recorded_infected_verdict_rejects_without_rescanning() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    // The verdict is only read from meta: storage_service is never asked again.
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, UNUSED_URL)).unwrap());
    // A Scan run whose job recorded an infected verdict (e.g. a retry after Acquire's reject was
    // interrupted): verdict in meta, no original.
    let job = jobs::create(
        &db.pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": "x.txt", "mime": "text/plain", "bytes": 5}),
            options: json!({}),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE ingest_steps SET status = 'completed', attempt = 1 WHERE job_id = $1")
        .bind(job)
        .execute(&db.pool)
        .await
        .unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    steps::enqueue(&mut tx, job, steps::StepKind::Scan, "", json!({}))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query(
        "UPDATE ingest_jobs SET status = 'running',
                meta = '{\"scan\":\"infected\",\"signature\":\"Eicar-Test-Signature\"}'
         WHERE id = $1",
    )
    .bind(job)
    .execute(&db.pool)
    .await
    .unwrap();

    let (shutdown, h) = start_workers(
        ctx.clone(),
        1,
        fast(),
        strata_ingest::worker::default_runner(),
    );
    let (error, _) = wait_job_status(&db.pool, job, "rejected").await;
    stop(shutdown, h).await;
    let error = error.unwrap();
    assert_eq!(error["code"], "infected");
    assert_eq!(error["message"], "Eicar-Test-Signature");
    assert_eq!(
        step_state(&db.pool, job, "scan").await.unwrap().0,
        "completed"
    );
}

#[tokio::test]
async fn asset_record_is_unique_per_job_and_key_and_fenced() {
    let db = TestDb::start().await;
    let job = jobs::create(
        &db.pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "url", "url": "https://example.com"}),
            options: json!({}),
        },
    )
    .await
    .unwrap();
    let step = steps::claim_next(&db.pool, 5).await.unwrap().unwrap();
    let asset = |bytes, mime| NewAsset {
        id: None,
        kind: AssetKind::Original,
        object_key: "t/j/original",
        bytes,
        mime,
        page: None,
    };
    let a = assets::record(&db.pool, &step, asset(10, "text/plain"))
        .await
        .unwrap();
    let b = assets::record(&db.pool, &step, asset(20, "text/markdown"))
        .await
        .unwrap();
    assert_eq!(a, b, "a retried record keeps the row");
    let rows = assets_of(&db.pool, job).await;
    assert_eq!(
        rows,
        [(
            "original".into(),
            "t/j/original".into(),
            20,
            "text/markdown".into()
        )]
    );
    // The constraint itself: a raw duplicate insert fails.
    let dup = sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, bytes, mime)
         VALUES ($1, $2, 'original', 't/j/original', 1, 'x')",
    )
    .bind(Uuid::now_v7())
    .bind(job)
    .execute(&db.pool)
    .await;
    assert!(dup.is_err(), "UNIQUE (job_id, object_key)");
    // A stale claim writes nothing.
    let stale = ClaimedStep {
        attempt: step.attempt + 1,
        ..step.clone()
    };
    assert!(matches!(
        assets::record(&db.pool, &stale, asset(30, "x")).await,
        Err(IngestError::LeaseLost { .. })
    ));
}

// ---------------------------------------------------------------------------------------------
// Task 7: Convert — docling path, page triage, figure extraction

use axum::extract::State;
use axum::http::HeaderMap;
use base64::Engine as _;
use strata_ingest::config::ProviderCfg;
use strata_ingest::infra::docling::PAGE_BREAK;
use strata_ingest::pipeline::figures::extract_figures;
use strata_ingest::pipeline::triage::{low_text_pages, split_pages};

type Captured = Arc<tokio::sync::Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

/// A docling-serve stand-in answering every convert with `md`; captures each request.
async fn docling_fake(md: String) -> (String, Captured) {
    let captured: Captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let reply = json!({
        "document": {"filename": "document.pdf", "md_content": md, "json_content": null},
        "status": "success",
        "errors": [],
        "processing_time": 0.5,
        "timings": {}
    });
    let app =
        Router::new()
            .route(
                "/v1/convert/file",
                post(
                    |State((cap, reply)): State<(Captured, Value)>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        cap.lock().await.push((headers, body.to_vec()));
                        axum::Json(reply)
                    },
                ),
            )
            .with_state((captured.clone(), reply));
    (spawn(app).await, captured)
}

/// `(name, filename, value)` per multipart part (enough of RFC 7578 for these assertions).
fn form_parts(headers: &HeaderMap, body: &[u8]) -> Vec<(String, Option<String>, Vec<u8>)> {
    fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
        hay.windows(needle.len()).position(|w| w == needle)
    }
    fn attr(head: &str, key: &str) -> Option<String> {
        let pat = format!("; {key}=\"");
        let i = head.find(&pat)? + pat.len();
        let j = head[i..].find('"')? + i;
        Some(head[i..j].to_string())
    }
    let ct = headers["content-type"].to_str().unwrap();
    let boundary = ct.split("boundary=").nth(1).unwrap().trim_matches('"');
    let delim = format!("--{boundary}");
    let mut parts = Vec::new();
    let mut rest = body;
    while let Some(i) = find(rest, delim.as_bytes()) {
        rest = &rest[i + delim.len()..];
        if rest.starts_with(b"--") {
            break;
        }
        let after = &rest[2..];
        let head_end = find(after, b"\r\n\r\n").expect("part headers");
        let head = String::from_utf8_lossy(&after[..head_end]).to_string();
        let content = &after[head_end + 4..];
        let end = find(content, format!("\r\n{delim}").as_bytes()).expect("part end");
        parts.push((
            attr(&head, "name").expect("name"),
            attr(&head, "filename"),
            content[..end].to_vec(),
        ));
        rest = &content[end + 2..];
    }
    parts
}

/// The text fields of one captured docling request, by name.
fn form_field(req: &(HeaderMap, Vec<u8>), name: &str) -> Vec<String> {
    form_parts(&req.0, &req.1)
        .into_iter()
        .filter(|(n, _, _)| n == name)
        .map(|(_, _, v)| String::from_utf8(v).unwrap())
        .collect()
}

fn provider(dialect: &str) -> ProviderCfg {
    ProviderCfg {
        base_url: format!("{UNUSED_URL}/v1"),
        dialect: dialect.to_string(),
        model: "test-model".to_string(),
        api_key: Secret::new("provider-key"),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 1_000,
        concurrency: 2,
        rpm: None,
        price_micros_per_unit: None,
    }
}

struct Setup {
    db: TestDb,
    storage: FakeStorage,
    ctx: Arc<Ctx>,
    docling: Captured,
}

async fn ctx_with(
    db: &TestDb,
    storage: &FakeStorage,
    md: String,
    ocr: bool,
    vision: bool,
    limits: Limits,
) -> (Arc<Ctx>, Captured) {
    let (docling_url, docling) = docling_fake(md).await;
    let mut cfg = config(db, storage, UNUSED_URL);
    cfg.docling_url = docling_url;
    cfg.limits = limits;
    cfg.providers.ocr = ocr.then(|| provider("mistral"));
    cfg.providers.vision = vision.then(|| provider("openai"));
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    (ctx, docling)
}

async fn setup(md: String, ocr: bool, vision: bool, limits: Limits) -> Setup {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (ctx, docling) = ctx_with(&db, &storage, md, ocr, vision, limits).await;
    Setup {
        db,
        storage,
        ctx,
        docling,
    }
}

/// A page with plenty of born-digital text (well over 100 non-whitespace chars).
fn rich(n: usize) -> String {
    format!(
        "## Section {n}\n\n{}",
        "Born-digital text that docling extracted without OCR. ".repeat(4)
    )
}

/// A scanned-looking page: a caption-ish line and an embedded page image only.
fn low(n: usize) -> String {
    format!("Scan {n}\n\n![Image](data:image/png;base64,iVBORw0KGgo=)")
}

fn doc(pages: &[String]) -> String {
    pages.join(&format!("\n\n{PAGE_BREAK}\n\n"))
}

/// Real Acquire → Scan → Detect → Convert, and the real Caption skip path. OcrPages / Caption /
/// Bundle / Deliver complete with a stub output (covered in tests/enrich.rs and tests/bundle.rs).
fn convert_runner() -> StepRunner {
    Arc::new(|ctx: Arc<Ctx>, step: ClaimedStep| {
        Box::pin(async move {
            let stub = match step.kind {
                steps::StepKind::OcrPages
                | steps::StepKind::Bundle
                | steps::StepKind::TranscribeSegment
                | steps::StepKind::Deliver => true,
                steps::StepKind::Caption => step.input.get("skip").is_none(),
                _ => false,
            };
            if stub {
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

async fn upload_with(ctx: &Ctx, filename: &str, body: Vec<u8>, options: Value) -> Uuid {
    let id = Uuid::now_v7();
    let len = body.len();
    ctx.storage
        .upload_bytes(
            &original_key(tenant(), id),
            Bytes::from(body),
            "application/octet-stream",
        )
        .await
        .expect("put original");
    jobs::create_with_id(
        &ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": filename,
                           "mime": "application/octet-stream", "bytes": len, "scan": "clean"}),
            options,
        },
    )
    .await
    .expect("create job")
}

fn opts(ocr: &str, captions: bool) -> Value {
    json!({"ocr": ocr, "captions": captions, "language": null})
}

/// Runs the pool until every job in `jobs` is completed or failed.
async fn run_jobs(ctx: &Arc<Ctx>, pool: &PgPool, jobs: &[Uuid]) {
    let (shutdown, h) = start_workers(ctx.clone(), 2, fast(), convert_runner());
    for &job in jobs {
        wait_for(&format!("job {job} to finish"), || async {
            let (status, _, _) = job_row(pool, job).await;
            matches!(status.as_str(), "completed" | "failed").then_some(())
        })
        .await;
    }
    stop(shutdown, h).await;
}

/// `(kind, key, status, input, output)` per step, in queue order.
async fn steps_full(
    pool: &PgPool,
    job: Uuid,
) -> Vec<(String, String, String, Value, Option<Value>)> {
    sqlx::query(
        "SELECT kind, key, status, input, output FROM ingest_steps
         WHERE job_id = $1 ORDER BY stage, key, id",
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
            r.get("input"),
            r.get("output"),
        )
    })
    .collect()
}

/// `(key, input)` of the job's OcrPages steps.
async fn ocr_steps(pool: &PgPool, job: Uuid) -> Vec<(String, Value)> {
    steps_full(pool, job)
        .await
        .into_iter()
        .filter(|s| s.0 == "ocr_pages")
        .map(|s| (s.1, s.3))
        .collect()
}

async fn convert_output(pool: &PgPool, job: Uuid) -> Value {
    let s = steps_full(pool, job).await;
    let c = s.iter().find(|s| s.0 == "convert").expect("convert step");
    assert_eq!(c.2, "completed", "convert: {c:?}");
    c.4.clone().expect("convert output")
}

/// The job's `convert.json` artefact (`ingest_artifacts`).
async fn convert_json(ctx: &Ctx, job: Uuid) -> Value {
    let body = artifacts::get(&ctx.pool, job, "convert.json")
        .await
        .unwrap()
        .expect("convert.json");
    serde_json::from_str(&body).unwrap()
}

fn as_u32s(v: &Value) -> Vec<u32> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_u64().unwrap() as u32)
        .collect()
}

/// The docling request was the first (text-layer) pass: no OCR.
async fn assert_one_text_pass(docling: &Captured, ext: &str) {
    let reqs = docling.lock().await;
    assert_eq!(reqs.len(), 1, "one docling call");
    assert_eq!(form_field(&reqs[0], "do_ocr"), ["false"]);
    assert_eq!(form_field(&reqs[0], "force_ocr"), ["false"]);
    assert_eq!(form_field(&reqs[0], "ocr_engine"), ["easyocr"]);
    let parts = form_parts(&reqs[0].0, &reqs[0].1);
    let file = parts.iter().find(|p| p.0 == "files").expect("files part");
    assert!(
        file.1.as_deref().unwrap().ends_with(ext),
        "docling infers the format from the extension: {:?}",
        file.1
    );
}

#[test]
fn split_and_low_text() {
    let md = format!(
        "{}\n\n{PAGE_BREAK}\n\nshort\n\n![Image](data:image/png;base64,iVBORw0KGgo=)\n\n{PAGE_BREAK}\n\n  \n",
        rich(0)
    );
    let pages = split_pages(&md);
    assert_eq!(pages.len(), 3, "{pages:?}");
    assert_eq!(
        pages[0],
        rich(0),
        "page breaks and their blank lines are not page text"
    );
    assert!(pages.iter().all(|p| !p.contains("page-break")));
    assert_eq!(low_text_pages(&pages, 100), vec![1, 2]);
    // Image tags do not count as text, however long the data URI.
    let one_big_tag = format!("![{}](data:image/png;base64,{})", "alt", "A".repeat(5000));
    assert_eq!(low_text_pages(&[one_big_tag], 100), vec![0]);
    // Threshold: `< min` non-whitespace chars is low; whitespace never counts.
    let at = "ab ".repeat(50); // 100 non-whitespace chars
    let below = format!("{}\n\n\t", "x".repeat(99));
    assert_eq!(low_text_pages(&[at, below], 100), vec![1]);
    // No page break: the whole document is one page.
    assert_eq!(split_pages("just text"), vec!["just text".to_string()]);
}

#[tokio::test]
async fn born_digital_pdf_enqueues_no_ocr() {
    let pages: Vec<String> = (0..3).map(rich).collect();
    let s = setup(doc(&pages), true, false, Limits::default()).await;
    let job = upload_with(
        &s.ctx,
        "report.pdf",
        fixture("sample.pdf"),
        opts("auto", false),
    )
    .await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    let (status, error, meta) = job_row(&s.db.pool, job).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_one_text_pass(&s.docling, ".pdf").await;
    assert!(ocr_steps(&s.db.pool, job).await.is_empty());
    let out = convert_output(&s.db.pool, job).await;
    assert_eq!(out["convert_key"], "convert.json");
    assert_eq!(out["page_count"], 3);
    assert_eq!(out["low_text"], json!([]));
    assert_eq!(convert_json(&s.ctx, job).await, json!({ "pages": pages }));
    assert_eq!(meta["pages"], 3);
    assert_eq!(meta["mime"], "application/pdf");
    // Bundle follows the enrich stage.
    let kinds: Vec<(String, String)> = steps_full(&s.db.pool, job)
        .await
        .into_iter()
        .map(|s| (s.0, s.2))
        .collect();
    assert_eq!(
        kinds.last().unwrap(),
        &("bundle".to_string(), "completed".to_string())
    );
}

#[tokio::test]
async fn mixed_pdf_ocr_only_low_pages() {
    // 0 rich, 1-2 low, 3 rich, 4-13 low (a run of 10: split into 8 + 2).
    let mut pages = vec![rich(0), low(1), low(2), rich(3)];
    pages.extend((4..14).map(low));
    let s = setup(doc(&pages), true, false, Limits::default()).await;
    let job = upload_with(
        &s.ctx,
        "mixed.pdf",
        fixture("sample.pdf"),
        opts("auto", false),
    )
    .await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    assert_one_text_pass(&s.docling, ".pdf").await;
    let out = convert_output(&s.db.pool, job).await;
    let mut low_pages = vec![1, 2];
    low_pages.extend(4..14);
    assert_eq!(as_u32s(&out["low_text"]), low_pages);
    assert_eq!(out["page_count"], 14);
    let ocr = ocr_steps(&s.db.pool, job).await;
    let keys: Vec<&str> = ocr.iter().map(|o| o.0.as_str()).collect();
    assert_eq!(keys, ["p1-2", "p12-13", "p4-11"]); // ORDER BY key
    let by_key = |k: &str| ocr.iter().find(|o| o.0 == k).unwrap().1.clone();
    assert_eq!(as_u32s(&by_key("p1-2")["pages"]), [1, 2]);
    assert_eq!(
        as_u32s(&by_key("p4-11")["pages"]),
        (4..12).collect::<Vec<_>>()
    );
    assert_eq!(as_u32s(&by_key("p12-13")["pages"]), [12, 13]);
    assert_eq!(by_key("p1-2")["engine"], "provider");
    // Rich pages are kept as docling extracted them.
    let cj = convert_json(&s.ctx, job).await;
    assert_eq!(cj["pages"][0], rich(0));
    assert_eq!(cj["pages"][3], rich(3));
}

#[tokio::test]
async fn ocr_force_sends_every_page() {
    let pages: Vec<String> = (0..3).map(rich).collect();
    let s = setup(doc(&pages), true, false, Limits::default()).await;
    let job = upload_with(&s.ctx, "a.pdf", fixture("sample.pdf"), opts("force", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    assert_one_text_pass(&s.docling, ".pdf").await;
    let ocr = ocr_steps(&s.db.pool, job).await;
    assert_eq!(ocr.len(), 1, "{ocr:?}");
    assert_eq!(ocr[0].0, "p0-2");
    assert_eq!(as_u32s(&ocr[0].1["pages"]), [0, 1, 2]);
    assert_eq!(convert_output(&s.db.pool, job).await["low_text"], json!([]));
}

#[tokio::test]
async fn ocr_off_sends_none() {
    let pages = vec![rich(0), low(1), low(2)];
    let s = setup(doc(&pages), true, false, Limits::default()).await;
    let job = upload_with(&s.ctx, "a.pdf", fixture("sample.pdf"), opts("off", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    assert_one_text_pass(&s.docling, ".pdf").await;
    assert!(ocr_steps(&s.db.pool, job).await.is_empty());
    // Low-text pages are still reported.
    assert_eq!(
        as_u32s(&convert_output(&s.db.pool, job).await["low_text"]),
        [1, 2]
    );
}

#[tokio::test]
async fn no_ocr_provider_uses_docling_cpu() {
    let pages = vec![rich(0), low(1), rich(2), low(3)];
    let s = setup(doc(&pages), false, false, Limits::default()).await;
    assert!(
        !s.ctx
            .providers
            .has_default(strata_ingest::providers::Capability::Ocr)
    );
    let job = upload_with(&s.ctx, "a.pdf", fixture("sample.pdf"), opts("auto", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    // Convert's own docling call is the text pass; the CPU OCR pass is the OcrPages step's.
    assert_one_text_pass(&s.docling, ".pdf").await;
    // Batched like the provider path (R37): one step per contiguous run of <= 8 pages.
    let ocr = ocr_steps(&s.db.pool, job).await;
    let keys: Vec<&str> = ocr.iter().map(|o| o.0.as_str()).collect();
    assert_eq!(keys, ["cpu-p1-1", "cpu-p3-3"], "{ocr:?}");
    for (input, page) in [(&ocr[0].1, 1), (&ocr[1].1, 3)] {
        assert_eq!(as_u32s(&input["pages"]), [page]);
        assert_eq!(input["engine"], "docling-cpu");
        assert_eq!(input["do_ocr"], true);
        assert_eq!(input["force_ocr"], true);
        assert_eq!(input["ocr_engine"], "easyocr");
    }
}

/// At most one Convert per pod at a time (R37): three uploads on three workers reach docling
/// one after the other; the waits do not time the steps out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converts_run_one_at_a_time_per_pod() {
    #[derive(Clone, Default)]
    struct Load {
        now: Arc<AtomicUsize>,
        max: Arc<AtomicUsize>,
    }
    let load = Load::default();
    let reply = json!({
        "document": {"filename": "document.pdf", "md_content": rich(0), "json_content": null},
        "status": "success", "errors": [], "processing_time": 0.5, "timings": {}
    });
    let app = Router::new()
        .route(
            "/v1/convert/file",
            post(|State((l, reply)): State<(Load, Value)>| async move {
                let n = l.now.fetch_add(1, Ordering::SeqCst) + 1;
                l.max.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                l.now.fetch_sub(1, Ordering::SeqCst);
                axum::Json(reply)
            }),
        )
        .with_state((load.clone(), reply));
    let docling_url = spawn(app).await;
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let mut cfg = config(&db, &storage, UNUSED_URL);
    cfg.docling_url = docling_url;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let mut jobs = Vec::new();
    for i in 0..3 {
        let name = format!("d{i}.pdf");
        jobs.push(upload_with(&ctx, &name, fixture("sample.pdf"), opts("off", false)).await);
    }
    let opts_ = WorkerOpts {
        deadline: Some(Duration::from_millis(500)),
        ..fast()
    };
    let (shutdown, h) = start_workers(ctx.clone(), 3, opts_, convert_runner());
    for &job in &jobs {
        wait_for("the job to finish", || async {
            let (status, _, _) = job_row(&db.pool, job).await;
            matches!(status.as_str(), "completed" | "failed").then_some(status)
        })
        .await;
    }
    stop(shutdown, h).await;
    for &job in &jobs {
        assert_eq!(job_row(&db.pool, job).await.0, "completed");
    }
    assert_eq!(load.max.load(Ordering::SeqCst), 1, "one Convert in flight");
}

#[tokio::test]
async fn too_many_pages_fails_before_ocr() {
    let pages: Vec<String> = (0..4).map(low).collect();
    let limits = Limits {
        max_pdf_pages: 3,
        ..Limits::default()
    };
    let s = setup(doc(&pages), true, false, limits).await;
    let job = upload_with(
        &s.ctx,
        "big.pdf",
        fixture("sample.pdf"),
        opts("force", false),
    )
    .await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    let (status, error, meta) = job_row(&s.db.pool, job).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "too_many_pages", "{error}");
    assert_eq!(error["class"], "invalid_request");
    assert!(
        error["message"].as_str().unwrap().contains("more than 3"),
        "{error}"
    );
    // docling was asked for one page past the limit, never the whole PDF (R37).
    assert_eq!(
        form_field(&s.docling.lock().await[0], "page_range"),
        ["1", "4"]
    );
    let st = step_state(&s.db.pool, job, "convert").await.unwrap();
    assert_eq!(
        (st.0.as_str(), st.1),
        ("failed", 1),
        "terminal, not retried"
    );
    assert_eq!(s.docling.lock().await.len(), 1);
    let kinds: Vec<String> = steps_full(&s.db.pool, job)
        .await
        .into_iter()
        .map(|s| s.0)
        .collect();
    assert_eq!(kinds, ["acquire", "scan", "detect", "convert"]);
    assert!(
        artifacts::get(&s.db.pool, job, "convert.json")
            .await
            .unwrap()
            .is_none()
    );
    assert!(meta.get("pages").is_none());
}

fn data_uri(mime: &str, bytes: &[u8]) -> String {
    format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

#[tokio::test]
async fn small_figures_dropped_large_figures_captioned() {
    let small = vec![7u8; 100]; // an icon
    let large_png = noise(9000);
    let large_jpg = noise(12_000);
    let pages = vec![
        format!(
            "{}\n\n![Image]({})\n\n![Image]({})",
            rich(0),
            data_uri("image/png", &small),
            data_uri("image/png", &large_png)
        ),
        format!(
            "{}\n\nInline ![Image]({}) figure.\n\n![svg]({})",
            rich(1),
            data_uri("image/jpeg", &large_jpg),
            data_uri("image/svg+xml", &large_png) // not an allowed figure type: removed
        ),
    ];
    let s = setup(doc(&pages), false, true, Limits::default()).await;
    let pdf = fixture("sample.pdf");
    let with = upload_with(&s.ctx, "a.pdf", pdf.clone(), opts("auto", true)).await;
    let without = upload_with(&s.ctx, "b.pdf", pdf.clone(), opts("auto", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[with, without]).await;

    for (job, caption_status, caption_out) in [
        (with, "completed", json!({"stub": true})),
        (without, "skipped", json!({"skipped": "disabled"})),
    ] {
        assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
        let figures: Vec<(Uuid, String, i32, i64, String)> = sqlx::query(
            "SELECT id, object_key, page, bytes, mime FROM ingest_assets
             WHERE job_id = $1 AND kind = 'figure' ORDER BY page",
        )
        .bind(job)
        .fetch_all(&s.db.pool)
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get("id"),
                r.get("object_key"),
                r.get("page"),
                r.get("bytes"),
                r.get("mime"),
            )
        })
        .collect();
        assert_eq!(
            figures.len(),
            2,
            "the 100-byte icon is dropped: {figures:?}"
        );
        let cj = convert_json(&s.ctx, job).await;
        let text = cj.to_string();
        assert!(!text.contains("data:image"), "no data URI survives: {text}");
        for ((id, key, page, bytes, mime), (want_page, want_bytes, want_mime)) in figures
            .iter()
            .zip([(0, &large_png, "image/png"), (1, &large_jpg, "image/jpeg")])
        {
            assert_eq!(key, &format!("{}/{job}/figures/{id}", tenant()));
            assert_eq!(
                (*page, *bytes, mime.as_str()),
                (want_page, want_bytes.len() as i64, want_mime)
            );
            assert_eq!(s.storage.get(key).unwrap(), want_bytes.as_slice());
            let page_md = cj["pages"][*page as usize].as_str().unwrap();
            assert!(
                page_md
                    .lines()
                    .any(|l| l == format!("<!-- figure:{id} -->")),
                "placeholder on its own line: {page_md:?}"
            );
        }
        // The icon left nothing behind; the rich text is intact.
        assert!(cj["pages"][0].as_str().unwrap().starts_with(&rich(0)));
        assert_eq!(
            cj["pages"][0]
                .as_str()
                .unwrap()
                .matches("<!-- figure:")
                .count(),
            1
        );
        let captions: Vec<_> = steps_full(&s.db.pool, job)
            .await
            .into_iter()
            .filter(|s| s.0 == "caption")
            .collect();
        assert_eq!(captions.len(), 2, "{captions:?}");
        for c in &captions {
            let id: Uuid = c.1.parse().expect("caption key = asset id");
            assert!(figures.iter().any(|f| f.0 == id));
            assert_eq!(c.3["asset_id"], id.to_string());
            assert_eq!(c.2, caption_status);
            assert_eq!(c.4.as_ref().unwrap(), &caption_out);
        }
        let out = convert_output(&s.db.pool, job).await;
        assert_eq!(
            out["figures"],
            json!({"kept": 2, "dropped": 2, "dropped_small": 1,
                   "dropped_unsupported": 1, "dropped_over_cap": 0})
        );
    }

    // Captions requested but no vision provider: skipped as `no_provider`.
    let (ctx2, _) = ctx_with(
        &s.db,
        &s.storage,
        doc(&pages),
        false,
        false,
        Limits::default(),
    )
    .await;
    let job = upload_with(&ctx2, "c.pdf", pdf, opts("auto", true)).await;
    run_jobs(&ctx2, &s.db.pool, &[job]).await;
    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    let captions: Vec<_> = steps_full(&s.db.pool, job)
        .await
        .into_iter()
        .filter(|s| s.0 == "caption")
        .collect();
    assert_eq!(captions.len(), 2);
    for c in captions {
        assert_eq!(c.2, "skipped");
        assert_eq!(c.4.unwrap(), json!({"skipped": "no_provider"}));
    }
}

#[tokio::test]
async fn figures_past_the_job_cap_are_dropped() {
    let fig = |n: u8| data_uri("image/png", &vec![n; 9000]);
    let pages = vec![
        format!("{}\n\n![a]({})\n\n![b]({})", rich(0), fig(1), fig(2)),
        format!("{}\n\n![c]({})", rich(1), fig(3)),
    ];
    let limits = Limits {
        max_figures_per_job: 2,
        ..Limits::default()
    };
    let s = setup(doc(&pages), false, false, limits).await;
    let job = upload_with(&s.ctx, "a.pdf", fixture("sample.pdf"), opts("auto", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    let out = convert_output(&s.db.pool, job).await;
    assert_eq!(out["figures"]["kept"], 2);
    assert_eq!(out["figures"]["dropped_over_cap"], 1);
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_assets WHERE job_id = $1 AND kind = 'figure'",
    )
    .bind(job)
    .fetch_one(&s.db.pool)
    .await
    .unwrap();
    assert_eq!(n, 2);
    let cj = convert_json(&s.ctx, job).await.to_string();
    assert_eq!(cj.matches("<!-- figure:").count(), 2);
    assert!(!cj.contains("data:image"));
    let captions = steps_full(&s.db.pool, job)
        .await
        .into_iter()
        .filter(|s| s.0 == "caption")
        .count();
    assert_eq!(captions, 2);
}

#[test]
fn figure_ids_come_from_the_caller_and_tags_without_data_uris_stay() {
    let big = data_uri("image/png", &noise(9000));
    let md = format!(
        "a ![x]({big}) b\n\n![remote](https://example.com/a.png)\n\n![bad](data:image/png;base64,***)"
    );
    let id = Uuid::now_v7();
    let x = extract_figures(&md, 8192, 200, |idx| {
        assert_eq!(idx, 0);
        id
    });
    let figs = &x.figures;
    assert_eq!(figs.len(), 1);
    assert_eq!(
        (figs[0].idx, figs[0].id, figs[0].mime),
        (0, id, "image/png")
    );
    assert_eq!(figs[0].decode(&md).unwrap(), noise(9000));
    assert_eq!(x.dropped.unsupported, 1, "the undecodable one");
    assert_eq!(
        x.markdown,
        format!("a \n<!-- figure:{id} -->\n b\n\n![remote](https://example.com/a.png)\n\n")
    );
}

#[tokio::test]
async fn image_gets_one_page_ocr_and_a_caption_without_docling() {
    let s = setup("unused".into(), true, true, Limits::default()).await;
    let job = upload_with(
        &s.ctx,
        "photo.png",
        fixture("sample.png"),
        opts("auto", true),
    )
    .await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    let (status, error, meta) = job_row(&s.db.pool, job).await;
    assert_eq!(status, "completed", "{error:?}");
    assert!(
        s.docling.lock().await.is_empty(),
        "an image is not sent to docling"
    );
    let original: Uuid =
        sqlx::query_scalar("SELECT id FROM ingest_assets WHERE job_id = $1 AND kind = 'original'")
            .bind(job)
            .fetch_one(&s.db.pool)
            .await
            .unwrap();
    assert_eq!(
        convert_json(&s.ctx, job).await,
        json!({"pages": [format!("<!-- figure:{original} -->")]})
    );
    let ocr = ocr_steps(&s.db.pool, job).await;
    assert_eq!(ocr.len(), 1);
    assert_eq!(ocr[0].0, "p0-0");
    assert_eq!(as_u32s(&ocr[0].1["pages"]), [0]);
    let steps = steps_full(&s.db.pool, job).await;
    let caption = steps
        .iter()
        .find(|s| s.0 == "caption")
        .expect("caption step");
    assert_eq!(caption.1, original.to_string());
    assert_eq!(meta["pages"], 1);
    assert_eq!(meta["mime"], "image/png");
}

#[tokio::test]
async fn text_is_passed_through_as_one_page() {
    let s = setup("unused".into(), true, false, Limits::default()).await;
    let body = fixture("sample.txt");
    let job = upload_with(&s.ctx, "notes.txt", body.clone(), opts("force", false)).await;
    run_jobs(&s.ctx, &s.db.pool, &[job]).await;

    assert_eq!(job_row(&s.db.pool, job).await.0, "completed");
    assert!(s.docling.lock().await.is_empty());
    assert!(
        ocr_steps(&s.db.pool, job).await.is_empty(),
        "no OCR for text, even forced"
    );
    let text = String::from_utf8(body).unwrap();
    assert_eq!(convert_json(&s.ctx, job).await, json!({"pages": [text]}));
    assert_eq!(convert_output(&s.db.pool, job).await["page_count"], 1);
}

// ---------------------------------------------------------------------------------------------
// Fix round 2: hostile central directories

fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = if c & 1 == 1 {
                (c >> 1) ^ 0xEDB8_8320
            } else {
                c >> 1
            };
        }
    }
    !c
}

/// A stored zip built by hand: `locals` are written in order; the central directory lists
/// `(name, index into locals)` exactly as given (duplicates allowed, unlike `ZipWriter`).
fn raw_zip(locals: &[(&str, &[u8])], cd: &[(&str, usize)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut offsets = Vec::new();
    for (name, data) in locals {
        offsets.push(out.len() as u32);
        out.extend_from_slice(b"PK\x03\x04");
        out.extend_from_slice(&20u16.to_le_bytes()); // version needed
        out.extend_from_slice(&[0; 2 + 2 + 2 + 2]); // flags, method (stored), time, date
        out.extend_from_slice(&crc32(data).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(data);
    }
    let cd_off = out.len() as u32;
    for (name, i) in cd {
        let data = locals[*i].1;
        out.extend_from_slice(b"PK\x01\x02");
        out.extend_from_slice(&20u16.to_le_bytes()); // made by
        out.extend_from_slice(&20u16.to_le_bytes()); // needed
        out.extend_from_slice(&[0; 2 + 2 + 2 + 2]);
        out.extend_from_slice(&crc32(data).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(&[0; 2 + 2 + 2 + 2 + 4]); // extra, comment, disk, int, ext attrs
        out.extend_from_slice(&offsets[*i].to_le_bytes());
        out.extend_from_slice(name.as_bytes());
    }
    let cd_size = out.len() as u32 - cd_off;
    out.extend_from_slice(b"PK\x05\x06");
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(cd.len() as u16).to_le_bytes());
    out.extend_from_slice(&(cd.len() as u16).to_le_bytes());
    out.extend_from_slice(&cd_size.to_le_bytes());
    out.extend_from_slice(&cd_off.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

#[test]
fn hostile_duplicate_entries_match_the_full_sniff() {
    const DUPES: usize = 30_000;
    let locals: [(&str, &[u8]); 2] = [
        ("mimetype", b"application/epub+zip"),
        ("mimetype", b"application/vnd.oasis.opendocument.text"),
    ];
    // 30 000 `mimetype` entries pointing at the epub one, the last at the odt one.
    let mut cd = vec![("mimetype", 0usize); DUPES - 1];
    cd.push(("mimetype", 1));
    let zip = raw_zip(&locals, &cd);

    // The last duplicate wins in the full-body sniff (the zip crate's name index)...
    assert_eq!(detect::sniff(&zip, None).unwrap(), Detected::Odt);
    // ...and on the worker's file path.
    assert_eq!(file_sniff(&zip).unwrap(), Detected::Odt);

    // Duplicates of both needed names: the same verdict both ways.
    let mut cd = vec![("mimetype", 0usize); 10_000];
    cd.extend(vec![("[Content_Types].xml", 1usize); 10_000]);
    let zip = raw_zip(
        &[
            ("mimetype", b"application/epub+zip"),
            ("[Content_Types].xml", b"<Types/>"),
        ],
        &cd,
    );
    assert_eq!(
        verdict(file_sniff(&zip)),
        verdict(detect::sniff(&zip, None))
    );
}

#[test]
fn lying_central_directory_is_unsupported_not_a_panic() {
    let base = padded(&fixture("sample.docx"), 200 * 1024, false);
    let size = base.len() as u64;
    let eocd = base.len() - 22; // no comment
    assert_eq!(&base[eocd..eocd + 4], b"PK\x05\x06");
    let patch = |field: usize, v: u32| {
        let mut z = base.clone();
        z[eocd + field..eocd + field + 4].copy_from_slice(&v.to_le_bytes());
        z
    };
    for (what, z) in [
        ("cd_size beyond the file", patch(12, (size * 2) as u32)),
        ("cd_size near u32::MAX", patch(12, 0xFFFF_FFF0)),
        ("cd_offset beyond EOF", patch(16, (size + 10) as u32)),
        (
            "cd_offset + cd_size beyond EOF",
            patch(16, (size - 30) as u32),
        ),
        ("cd_offset into the padding", patch(16, 1000)),
    ] {
        let r = file_sniff(&z);
        assert!(
            matches!(&r, Err(IngestError::UnsupportedType(m)) if m == "application/zip"),
            "{what}: {r:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// URL page images (spec §6): webscraper downloads them, Convert makes them figures

/// A scraper answering every scrape with `reply`; captures each request body.
async fn scraper_replying(reply: Value) -> (String, Arc<std::sync::Mutex<Vec<Value>>>) {
    let bodies: Arc<std::sync::Mutex<Vec<Value>>> = Arc::default();
    let app = Router::new()
        .route(
            "/api/scrape/url",
            post(
                |State((bodies, reply)): State<(Arc<std::sync::Mutex<Vec<Value>>>, Value)>,
                 axum::Json(body): axum::Json<Value>| async move {
                    bodies.lock().unwrap().push(body);
                    axum::Json(reply)
                },
            ),
        )
        .with_state((bodies.clone(), reply));
    (spawn(app).await, bodies)
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn page_reply(markdown: &str, images: Vec<Value>) -> Value {
    json!({
        "url": "https://example.com/a", "final_url": "https://example.com/a/",
        "title": "Pictures", "markdown_content": markdown, "status": "completed",
        "links": [], "images": images,
    })
}

async fn url_job_with(pool: &PgPool, captions: bool) -> Uuid {
    jobs::create(
        pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "url", "url": "https://example.com/a"}),
            options: opts("auto", captions),
        },
    )
    .await
    .unwrap()
}

/// `(id, object_key, bytes, mime)` of the job's figure assets.
async fn figure_assets(pool: &PgPool, job: Uuid) -> Vec<(Uuid, String, i64, String)> {
    sqlx::query(
        "SELECT id, object_key, bytes, mime FROM ingest_assets
         WHERE job_id = $1 AND kind = 'figure' ORDER BY object_key",
    )
    .bind(job)
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.get("id"),
            r.get("object_key"),
            r.get("bytes"),
            r.get("mime"),
        )
    })
    .collect()
}

#[tokio::test]
async fn url_page_images_become_figures_and_others_keep_their_link() {
    let chart = noise(9000);
    let md = "# Pictures\n\n![icon](/icon.png)\n\n![report](https://example.com/report.pdf)\n\n\
              ![chart](chart.png)\n\nSome text about the chart.\n";
    let reply = page_reply(
        md,
        vec![
            // Too small (an icon), not an image, and one valid PNG.
            json!({"src": "https://example.com/icon.png", "alt": "icon",
                   "mime_type": "image/png", "content_base64": b64(&[1u8; 100])}),
            json!({"src": "https://example.com/report.pdf", "alt": "report",
                   "mime_type": "application/pdf", "content_base64": b64(&noise(9000))}),
            json!({"src": "https://example.com/a/chart.png", "alt": "chart",
                   "mime_type": "image/png", "content_base64": b64(&chart)}),
        ],
    );
    for vision in [true, false] {
        let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
        let (scraper, bodies) = scraper_replying(reply.clone()).await;
        let mut cfg = config(&db, &storage, &scraper);
        cfg.providers.vision = vision.then(|| provider("openai"));
        let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
        let job = url_job_with(&db.pool, true).await;
        run_jobs(&ctx, &db.pool, &[job]).await;

        let (status, error, _) = job_row(&db.pool, job).await;
        assert_eq!(status, "completed", "{error:?}");
        // webscraper was asked for the images; nothing else fetched them.
        assert_eq!(
            bodies.lock().unwrap()[0],
            json!({"url": "https://example.com/a", "scraper_type": "auto",
                   "download_images": true})
        );
        let figures = figure_assets(&db.pool, job).await;
        assert_eq!(figures.len(), 1, "{figures:?}");
        let (id, key, bytes, mime) = &figures[0];
        assert_eq!(key, &format!("{}/{job}/figures/{id}", tenant()));
        assert_eq!((*bytes, mime.as_str()), (chart.len() as i64, "image/png"));
        assert_eq!(storage.get(key).unwrap(), chart.as_slice());
        let page = convert_json(&ctx, job).await["pages"][0]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            page.lines().any(|l| l == format!("<!-- figure:{id} -->")),
            "{page}"
        );
        // The images that were not kept keep their original reference.
        assert!(page.contains("![icon](/icon.png)"), "{page}");
        assert!(
            page.contains("![report](https://example.com/report.pdf)"),
            "{page}"
        );
        assert!(!page.contains("data:"), "{page}");
        assert!(!page.contains("## Images"), "{page}");
        let acquire = steps_full(&db.pool, job)
            .await
            .into_iter()
            .find(|s| s.0 == "acquire")
            .unwrap();
        assert_eq!(
            acquire.4.unwrap()["images"],
            json!({"kept": 1, "referenced": 1, "appended": 0, "dropped": 2})
        );
        let captions: Vec<_> = steps_full(&db.pool, job)
            .await
            .into_iter()
            .filter(|s| s.0 == "caption")
            .collect();
        assert_eq!(captions.len(), 1, "{captions:?}");
        assert_eq!(captions[0].1, id.to_string());
        if vision {
            assert_eq!(captions[0].2, "completed");
            assert_eq!(captions[0].4.as_ref().unwrap(), &json!({"stub": true}));
        } else {
            assert_eq!(captions[0].2, "skipped");
            assert_eq!(
                captions[0].4.as_ref().unwrap(),
                &json!({"skipped": "no_provider"})
            );
        }
    }
}

#[tokio::test]
async fn unreferenced_page_images_are_appended_and_capped() {
    use strata_ingest::limits::MAX_PAGE_IMAGES;
    let images: Vec<Value> = (0..MAX_PAGE_IMAGES + 5)
        .map(|i| {
            let mut bytes = noise(9000);
            bytes[0] = i as u8;
            json!({"src": format!("https://cdn.example.com/{i}.webp"), "alt": format!("pic [{i}]"),
                   "mime_type": "image/webp", "content_base64": b64(&bytes)})
        })
        .collect();
    let reply = page_reply("# Gallery\n\nNo image tags in this markdown.\n", images);
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (scraper, _) = scraper_replying(reply).await;
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), config(&db, &storage, &scraper)).unwrap());
    let job = url_job_with(&db.pool, false).await;
    run_jobs(&ctx, &db.pool, &[job]).await;

    let (status, error, _) = job_row(&db.pool, job).await;
    assert_eq!(status, "completed", "{error:?}");
    let figures = figure_assets(&db.pool, job).await;
    assert_eq!(figures.len(), MAX_PAGE_IMAGES, "at most 20 per page");
    assert!(figures.iter().all(|f| f.3 == "image/webp"));
    let page = convert_json(&ctx, job).await["pages"][0]
        .as_str()
        .unwrap()
        .to_string();
    let (text, gallery) = page.split_once("\n## Images\n").expect("an Images section");
    assert!(text.starts_with("# Gallery\n\nNo image tags"), "{page}");
    assert_eq!(gallery.matches("<!-- figure:").count(), MAX_PAGE_IMAGES);
    let out = convert_output(&db.pool, job).await;
    assert_eq!(out["figures"]["kept"], MAX_PAGE_IMAGES);
    let acquire = steps_full(&db.pool, job)
        .await
        .into_iter()
        .find(|s| s.0 == "acquire")
        .unwrap();
    assert_eq!(
        acquire.4.unwrap()["images"],
        json!({"kept": MAX_PAGE_IMAGES, "referenced": 0, "appended": MAX_PAGE_IMAGES,
               "dropped": 5})
    );
    // Captions off: every Caption step is skipped as disabled.
    let captions: Vec<_> = steps_full(&db.pool, job)
        .await
        .into_iter()
        .filter(|s| s.0 == "caption")
        .collect();
    assert_eq!(captions.len(), MAX_PAGE_IMAGES);
    assert!(captions.iter().all(|c| c.2 == "skipped"));
}
