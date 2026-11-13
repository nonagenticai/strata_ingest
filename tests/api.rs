//! The HTTP API (`/api/ingest/...`, `/health`) on a real Postgres, with in-process mocks for
//! storage_service (`FakeStorage`: verdicts, request capture), strata and docling.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::get;
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};
use sqlx::Row;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use common::{FakeStorage, STORAGE_BUCKET, TestDb};
use strata_ingest::api::ApiOpts;
use strata_ingest::config::{Config, Mode, ProviderCfg, ProvidersFile, Region, Secret};
use strata_ingest::limits::Limits;
use strata_ingest::pipeline::{Ctx, original_key};
use strata_ingest::providers::{Capability, ErrorClass};
use strata_ingest::testutil::spawn;

const TOKEN_A: &str = "token-tenant-a";
const TOKEN_B: &str = "token-tenant-b";
/// Nothing listens on port 9 (discard).
const UNUSED_URL: &str = "http://127.0.0.1:9";

fn tenant_a() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-00000000000a").unwrap()
}

fn tenant_b() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-00000000000b").unwrap()
}

/// A leaf store the strata mock knows.
fn leaf() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000f1").unwrap()
}

/// A router store the strata mock knows.
fn router_store() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000f2").unwrap()
}

/// strata stand-in: `GET /api/stores/{id}` (leaf / router / 404) and `GET /health`.
async fn strata_mock() -> String {
    let app = Router::new()
        .route(
            "/api/stores/{id}",
            get(|Path(id): Path<Uuid>| async move {
                if id == leaf() {
                    (
                        StatusCode::OK,
                        axum::Json(json!({"id": id, "kind": "leaf"})),
                    )
                } else if id == router_store() {
                    (
                        StatusCode::OK,
                        axum::Json(json!({"id": id, "kind": "router"})),
                    )
                } else {
                    (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": {"code": "not_found", "message": "no"}})),
                    )
                }
            }),
        )
        .route(
            "/health",
            get(|| async { axum::Json(json!({"status": "ok"})) }),
        );
    spawn(app).await
}

/// docling-serve stand-in: `GET /health` only.
async fn docling_mock() -> String {
    let app = Router::new().route(
        "/health",
        get(|| async { axum::Json(json!({"status": "ok"})) }),
    );
    spawn(app).await
}

fn provider(dialect: &str) -> ProviderCfg {
    ProviderCfg {
        base_url: format!("{UNUSED_URL}/v1"),
        dialect: dialect.to_string(),
        model: "m".to_string(),
        api_key: Secret::new("provider-secret-key"),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 1000,
        concurrency: 1,
        rpm: None,
        price_micros_per_unit: None,
    }
}

fn config(db: &TestDb, storage: &FakeStorage, docling: &str, strata: &str) -> Config {
    Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![
            (tenant_a(), Secret::new(TOKEN_A)),
            (tenant_b(), Secret::new(TOKEN_B)),
        ],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: docling.to_string(),
        webscraper_url: UNUSED_URL.to_string(),
        webscraper_token: None,
        strata_url: strata.to_string(),
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
        metering: None,
    }
}

struct Env {
    db: TestDb,
    storage: FakeStorage,
    ctx: Arc<Ctx>,
    /// `http://127.0.0.1:<port>` of the API under test.
    url: String,
    http: reqwest::Client,
}

async fn env_with(tweak: impl FnOnce(&mut Config)) -> Env {
    env_opts(tweak, ApiOpts::default()).await
}

async fn env_opts(tweak: impl FnOnce(&mut Config), opts: ApiOpts) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (strata, docling) = tokio::join!(strata_mock(), docling_mock());
    let mut cfg = config(&db, &storage, &docling, &strata);
    tweak(&mut cfg);
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let url = spawn(strata_ingest::api::router_with(ctx.clone(), opts)).await;
    Env {
        db,
        storage,
        ctx,
        url,
        http: reqwest::Client::new(),
    }
}

async fn env() -> Env {
    env_with(|_| {}).await
}

impl Env {
    fn req(&self, method: reqwest::Method, path: &str, token: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(token)
    }

    async fn upload(&self, token: &str, form: Form) -> (StatusCode, Value) {
        let r = self
            .req(reqwest::Method::POST, "/api/ingest/jobs", token)
            .multipart(form)
            .send()
            .await
            .expect("send");
        status_json(r).await
    }

    async fn call(&self, method: reqwest::Method, path: &str, token: &str) -> (StatusCode, Value) {
        status_json(self.req(method, path, token).send().await.expect("send")).await
    }

    async fn job_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM ingest_jobs")
            .fetch_one(&self.db.pool)
            .await
            .unwrap()
    }

    /// Every object in the bucket.
    async fn keys(&self) -> Vec<String> {
        self.storage.keys()
    }

    /// Creates one text upload job for `token` and returns its id.
    async fn text_job(&self, token: &str) -> Uuid {
        let (s, v) = self
            .upload(token, form(leaf(), &[("a.txt", b"hello world".to_vec())]))
            .await;
        assert_eq!(s, StatusCode::ACCEPTED, "{v}");
        job_id(&v["jobs"][0])
    }
}

async fn status_json(r: reqwest::Response) -> (StatusCode, Value) {
    let status = StatusCode::from_u16(r.status().as_u16()).unwrap();
    let text = r.text().await.expect("body");
    let v = serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text }));
    (status, v)
}

fn job_id(v: &Value) -> Uuid {
    Uuid::parse_str(v["job_id"].as_str().expect("job_id")).expect("uuid")
}

fn form(store: Uuid, files: &[(&str, Vec<u8>)]) -> Form {
    let mut f = Form::new().text("store_id", store.to_string());
    for (name, body) in files {
        f = f.part(
            "file",
            Part::bytes(body.clone())
                .file_name(name.to_string())
                .mime_str("application/octet-stream")
                .unwrap(),
        );
    }
    f
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/detect/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("fixture")
}

fn assert_error(v: &Value, code: &str) {
    assert_eq!(v["error"]["code"], code, "{v}");
    assert!(v["error"]["message"].is_string(), "{v}");
}

// ---------------------------------------------------------------------------------------------
// Auth

#[tokio::test]
async fn no_token_is_401() {
    let e = env().await;
    for path in ["/api/ingest/jobs", "/api/ingest/nope", "/health/"] {
        let r = e.http.get(format!("{}{path}", e.url)).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 401, "{path}");
        assert_eq!(r.headers()["www-authenticate"], "Bearer");
        let (_, v) = status_json(r).await;
        assert_error(&v, "unauthorized");
    }
    let (s, v) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs", "wrong-token")
        .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_error(&v, "unauthorized");
    // An unauthenticated upload is refused before anything is stored.
    let r = e
        .http
        .post(format!("{}/api/ingest/jobs", e.url))
        .multipart(form(leaf(), &[("a.txt", b"hello".to_vec())]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 401);
    assert_eq!(e.job_count().await, 0);
    assert!(e.keys().await.is_empty());
}

#[tokio::test]
async fn health_is_public() {
    let e = env().await;
    let r = e
        .http
        .get(format!("{}/health", e.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let (_, v) = status_json(r).await;
    assert_eq!(v["db"], "ok", "{v}");
}

#[tokio::test]
async fn tenant_isolation() {
    let e = env().await;
    let a = e.text_job(TOKEN_A).await;
    let path = format!("/api/ingest/jobs/{a}");
    let (s, _) = e.call(reqwest::Method::GET, &path, TOKEN_B).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, v) = e
        .call(reqwest::Method::POST, &format!("{path}/retry"), TOKEN_B)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
    let (s, v) = e
        .call(reqwest::Method::POST, &format!("{path}/cancel"), TOKEN_B)
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_B).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
    let (s, v) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs", TOKEN_B)
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["jobs"], json!([]), "{v}");
    // B's attempts changed nothing; A sees its job, still queued.
    let (s, v) = e.call(reqwest::Method::GET, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "queued");
    assert_eq!(v["tenant"], tenant_a().to_string());
    let (_, v) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs", TOKEN_A)
        .await;
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(v["jobs"][0]["id"], a.to_string());
}

// ---------------------------------------------------------------------------------------------
// Upload validation

#[tokio::test]
async fn unknown_store_is_404_router_store_is_422_and_nothing_written() {
    let e = env().await;
    let file = [("a.txt", b"hello world".to_vec())];
    let (s, v) = e.upload(TOKEN_A, form(Uuid::now_v7(), &file)).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_error(&v, "unknown_store");
    let (s, v) = e.upload(TOKEN_A, form(router_store(), &file)).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&v, "invalid_target");
    // URL jobs validate the store the same way.
    let (s, v) = status_json(
        e.req(reqwest::Method::POST, "/api/ingest/jobs", TOKEN_A)
            .json(&json!({"store_id": router_store(), "url": "https://example.com/a"}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&v, "invalid_target");
    assert_eq!(e.job_count().await, 0);
    assert!(e.keys().await.is_empty(), "{:?}", e.keys().await);
}

#[tokio::test]
async fn unsupported_upload_is_422_and_nothing_written() {
    let e = env().await;
    let (s, v) = e
        .upload(
            TOKEN_A,
            form(leaf(), &[("tool.exe", fixture("sample.exe"))]),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&v, "unsupported_type");
    // A good file before a bad one: the whole request is refused and the good one removed.
    let (s, v) = e
        .upload(
            TOKEN_A,
            form(
                leaf(),
                &[
                    ("a.txt", b"hello".to_vec()),
                    ("tool.exe", fixture("sample.exe")),
                ],
            ),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert_eq!(e.job_count().await, 0);
    assert!(e.keys().await.is_empty(), "{:?}", e.keys().await);
}

#[tokio::test]
async fn zip_container_head_is_accepted() {
    let e = env().await;
    let docx = fixture("sample.docx");
    let (s, v) = e
        .upload(TOKEN_A, form(leaf(), &[("report.docx", docx.clone())]))
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let id = job_id(&v["jobs"][0]);
    let stored = e.storage.get(&original_key(tenant_a(), id)).unwrap();
    assert_eq!(stored.as_ref(), docx.as_slice());
    // Stored under its sniffed type, scanned clean.
    let up = &e.storage.uploads()[0];
    assert_eq!(up.content_type.as_deref(), Some("application/zip"));
    // The worker classifies the container; the declared part type is not trusted.
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{id}"),
            TOKEN_A,
        )
        .await;
    assert_eq!(v["source"]["mime"], "application/zip", "{v}");
    assert_eq!(v["source"]["scan"], "clean", "{v}");
}

#[tokio::test]
async fn upload_over_limit_is_413_json() {
    // Over the 64 KiB sniff head, so the limit is hit while streaming, not while sniffing.
    let max = 128 * 1024;
    let e = env_with(|c| c.limits.max_upload_bytes = max).await;
    // One file part over max_upload_bytes, the request itself under the body limit
    // (max + 1 MiB): the upload is aborted mid-stream.
    let big = vec![b'a'; (max as usize) * 4];
    let (s, v) = e.upload(TOKEN_A, form(leaf(), &[("big.txt", big)])).await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error(&v, "payload_too_large");
    // The whole request over the body limit (by Content-Length), multipart and JSON: the
    // tower-http layer's 413, rewritten to the JSON body. Sent over a raw socket that declares
    // the length but never sends the body: the server answers without reading it, so a client
    // still writing would race the connection close (EPIPE / RST under load).
    let over = max + 2 * 1024 * 1024;
    for ct in [
        format!("multipart/form-data; boundary={BOUNDARY}"),
        "application/json".to_string(),
    ] {
        let text = early_answer(&e, &ct, over).await;
        assert!(text.starts_with("HTTP/1.1 413"), "{ct}: {text}");
        assert!(
            text.contains("\"code\":\"payload_too_large\""),
            "{ct}: {text}"
        );
    }
    assert_eq!(e.job_count().await, 0);
    assert!(e.keys().await.is_empty(), "{:?}", e.keys().await);
    // Refused before storage_service was called at all.
    assert!(e.storage.uploads().is_empty(), "{:?}", e.storage.uploads());
    // Exactly max is accepted.
    let ok = vec![b'a'; max as usize];
    let (s, v) = e.upload(TOKEN_A, form(leaf(), &[("ok.txt", ok)])).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
}

const MIB: usize = 1024 * 1024;
const BOUNDARY: &str = "XBOUNDARYX";

/// Sends only the head of a `POST /api/ingest/jobs` declaring `len` body bytes and returns what
/// the server answers (up to the end of its JSON error body).
async fn early_answer(e: &Env, content_type: &str, len: u64) -> String {
    let addr = e.url.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /api/ingest/jobs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN_A}\r\n\
         Content-Type: {content_type}\r\nContent-Length: {len}\r\n\r\n"
    );
    sock.write_all(head.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => resp.extend_from_slice(&buf[..n]),
            }
            if String::from_utf8_lossy(&resp).contains("}}") {
                break;
            }
        }
    })
    .await;
    String::from_utf8_lossy(&resp).into_owned()
}

/// Sends a whole multipart `POST /api/ingest/jobs` of `len` body bytes (every byte written
/// before anything is read, as a simple client does) and returns the answer. A write error (the
/// server reset the connection instead of reading the body) fails the test.
async fn full_body_answer(e: &Env, len: usize) -> String {
    let addr = e.url.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /api/ingest/jobs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN_A}\r\n\
         Content-Type: multipart/form-data; boundary={BOUNDARY}\r\nContent-Length: {len}\r\n\r\n"
    );
    let mut body = preamble().into_bytes();
    body.resize(len, b'a');
    let wrote = tokio::time::timeout(Duration::from_secs(20), async {
        sock.write_all(head.as_bytes()).await?;
        sock.write_all(&body).await?;
        sock.flush().await
    })
    .await
    .expect("the server reads the refused body");
    assert!(
        wrote.is_ok(),
        "connection reset while sending the body: {wrote:?}"
    );
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => resp.extend_from_slice(&buf[..n]),
            }
            if String::from_utf8_lossy(&resp).contains("}}") {
                break;
            }
        }
    })
    .await;
    String::from_utf8_lossy(&resp).into_owned()
}

/// Multipart framing up to the first file's bytes: `store_id`, then the `file` part header.
fn preamble() -> String {
    format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"store_id\"\r\n\r\n{}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n\
         Content-Type: text/plain\r\n\r\n",
        leaf()
    )
}

/// Ends the current file part and opens a second one.
fn next_file() -> String {
    format!(
        "\r\n--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"b.txt\"\r\n\
         Content-Type: text/plain\r\n\r\n"
    )
}

/// Polls `cond` for up to 20 s.
async fn eventually<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Env {
    /// No originals, no storage upload still in flight, no jobs.
    async fn nothing_left(&self) -> bool {
        self.keys().await.is_empty() && self.storage.in_flight() == 0 && self.job_count().await == 0
    }

    /// File 1 is stored (its upload completed) while file 2 is still arriving and being spooled:
    /// what a cleanup must remove.
    async fn first_stored(&self) -> bool {
        self.keys().await.len() == 1
    }
}

/// A client that disconnects mid-way through the second file leaves no object and no job.
#[tokio::test]
async fn interrupted_upload_stores_nothing() {
    let e = env().await;
    let addr = e.url.trim_start_matches("http://").to_string();
    let declared = 64 * MIB;
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "POST /api/ingest/jobs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN_A}\r\n\
         Content-Type: multipart/form-data; boundary={BOUNDARY}\r\nContent-Length: {declared}\r\n\r\n"
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    sock.write_all(preamble().as_bytes()).await.unwrap();
    sock.write_all(&vec![b'a'; 6 * MIB]).await.unwrap();
    sock.write_all(next_file().as_bytes()).await.unwrap();
    sock.write_all(&vec![b'b'; 6 * MIB]).await.unwrap();
    sock.flush().await.unwrap();
    eventually("file 1 stored, file 2 arriving", || e.first_stored()).await;
    drop(sock);
    eventually("cleanup after disconnect", || e.nothing_left()).await;
}

/// The handler future is DROPPED mid-request (as on shutdown or a hyper abort), not failed:
/// the cleanup still runs.
#[tokio::test]
async fn dropped_upload_request_cleans_up() {
    use tower::ServiceExt;
    let e = env().await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let body = axum::body::Body::from_stream(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let req = axum::http::Request::post("/api/ingest/jobs")
        .header("authorization", format!("Bearer {TOKEN_A}"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .body(body)
        .unwrap();
    let app = strata_ingest::api::router(e.ctx.clone());
    let task = tokio::spawn(app.oneshot(req));
    let send = |b: Vec<u8>| {
        let tx = tx.clone();
        async move {
            for c in b.chunks(64 * 1024) {
                tx.send(Ok(bytes::Bytes::copy_from_slice(c))).await.unwrap();
            }
        }
    };
    send(preamble().into_bytes()).await;
    send(vec![b'a'; 6 * MIB]).await;
    send(next_file().into_bytes()).await;
    send(vec![b'b'; 6 * MIB]).await;
    eventually("file 1 stored, file 2 arriving", || e.first_stored()).await;
    task.abort();
    let _ = task.await;
    eventually("cleanup after the request future was dropped", || {
        e.nothing_left()
    })
    .await;
}

/// A client that stops sending gets 408 `request_timeout` after the idle timeout; nothing stays.
#[tokio::test]
async fn stalled_upload_is_408() {
    let e = env_opts(
        |_| {},
        ApiOpts {
            upload_idle_timeout: Duration::from_millis(500),
            ..ApiOpts::default()
        },
    )
    .await;
    let addr = e.url.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "POST /api/ingest/jobs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN_A}\r\n\
         Content-Type: multipart/form-data; boundary={BOUNDARY}\r\nContent-Length: {}\r\n\r\n",
        16 * MIB
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    sock.write_all(preamble().as_bytes()).await.unwrap();
    sock.write_all(&vec![b'a'; 200 * 1024]).await.unwrap();
    sock.flush().await.unwrap();
    // Stall (connection open, no more bytes) and read the answer.
    let mut resp = Vec::new();
    let mut buf = [0u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf)
                .await
                .unwrap();
            if n == 0 {
                break;
            }
            resp.extend_from_slice(&buf[..n]);
            if String::from_utf8_lossy(&resp).contains("}}") {
                break;
            }
        }
    })
    .await;
    assert!(read.is_ok(), "no answer to a stalled upload");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 408"), "{text}");
    assert!(text.contains("\"code\":\"request_timeout\""), "{text}");
    eventually("nothing stored", || e.nothing_left()).await;
}

/// Every upload slot taken (here 1): another multipart upload is refused 503 `busy` with
/// `Retry-After` before anything is spooled; once the slot frees, uploads are accepted (R38).
#[tokio::test]
async fn upload_over_the_concurrency_cap_is_503_with_retry_after() {
    let e = env_opts(
        |_| {},
        ApiOpts {
            max_concurrent_uploads: 1,
            ..ApiOpts::default()
        },
    )
    .await;
    // Upload 1 holds the only slot: its body keeps arriving slowly.
    let addr = e.url.trim_start_matches("http://").to_string();
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let req = format!(
        "POST /api/ingest/jobs HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN_A}\r\n\
         Content-Type: multipart/form-data; boundary={BOUNDARY}\r\nContent-Length: {}\r\n\r\n",
        MIB
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    sock.write_all(preamble().as_bytes()).await.unwrap();
    sock.write_all(&vec![b'a'; 1024]).await.unwrap();
    sock.flush().await.unwrap();
    eventually("upload 1 holds the slot", || async {
        let r = e
            .req(reqwest::Method::POST, "/api/ingest/jobs", TOKEN_A)
            .multipart(form(leaf(), &[("b.txt", b"hello".to_vec())]))
            .send()
            .await
            .expect("send");
        r.status() == StatusCode::SERVICE_UNAVAILABLE
    })
    .await;
    let stored = e.storage.uploads().len();
    let r = e
        .req(reqwest::Method::POST, "/api/ingest/jobs", TOKEN_A)
        .multipart(form(leaf(), &[("b.txt", b"hello".to_vec())]))
        .send()
        .await
        .expect("send");
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers()["retry-after"], "30");
    let (_, v) = status_json(r).await;
    assert_error(&v, "busy");
    assert_eq!(
        e.storage.uploads().len(),
        stored,
        "nothing spooled or stored"
    );
    // A client that sends its whole (large) body before reading gets the 503, not a reset:
    // the refused body is drained (bounded), like the other refusal paths.
    let busy = full_body_answer(&e, 8 * MIB).await;
    assert!(busy.starts_with("HTTP/1.1 503"), "{busy}");
    assert!(busy.contains("\"code\":\"busy\""), "{busy}");
    // Upload 1 goes away: the slot is free again.
    drop(sock);
    eventually("the slot frees", || async {
        e.upload(TOKEN_A, form(leaf(), &[("c.txt", b"hello".to_vec())]))
            .await
            .0
            == StatusCode::ACCEPTED
    })
    .await;
}

/// When the DB cannot say whether a job exists, its original is kept (never deleted on an
/// error, only on a definite "not found").
#[tokio::test]
async fn db_error_keeps_the_original() {
    let e = env().await;
    e.db.pool.close().await;
    let (s, v) = e
        .upload(TOKEN_A, form(leaf(), &[("a.txt", b"hello".to_vec())]))
        .await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
    let keys = e.keys().await;
    assert_eq!(keys.len(), 1, "{keys:?}");
}

/// The request future is dropped while storage_service is still answering file 1's upload (its
/// in-request scan is slow): the spawned upload completes, then the cleanup removes it.
#[tokio::test]
async fn dropped_while_storage_upload_in_flight_cleans_up() {
    use tower::ServiceExt;
    let e = env().await;
    e.storage.set_upload_delay(Duration::from_millis(1500));
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let body = axum::body::Body::from_stream(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let req = axum::http::Request::post("/api/ingest/jobs")
        .header("authorization", format!("Bearer {TOKEN_A}"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .body(body)
        .unwrap();
    let app = strata_ingest::api::router(e.ctx.clone());
    let task = tokio::spawn(app.oneshot(req));
    tx.send(Ok(preamble().into_bytes().into())).await.unwrap();
    tx.send(Ok(vec![b'a'; MIB].into())).await.unwrap();
    // The end of file 1: it is spooled and its upload starts.
    tx.send(Ok(next_file().into_bytes().into())).await.unwrap();
    eventually("file 1's upload in flight", || async {
        e.storage.in_flight() == 1
    })
    .await;
    task.abort();
    let _ = task.await;
    // The upload lands (storage finishes its request) and is then deleted: never a job.
    eventually("upload landed and was cleaned up", || async {
        e.storage.uploads().len() == 1 && e.nothing_left().await
    })
    .await;
    let deletes = e
        .storage
        .requests()
        .into_iter()
        .filter(|r| r.method == "DELETE")
        .count();
    assert_eq!(deletes, 1);
}

/// storage_service refuses an infected file (nothing stored): its job is created `rejected` with
/// error `infected` and returned in the 202 list next to the clean file's queued job.
#[tokio::test]
async fn infected_upload_creates_rejected_job_and_stores_nothing() {
    let e = env().await;
    let mut eicar = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$".to_vec();
    eicar.extend_from_slice(common::EICAR);
    eicar.extend_from_slice(b"!$H+H*");
    let (s, v) = e
        .upload(
            TOKEN_A,
            form(
                leaf(),
                &[("clean.txt", b"hello".to_vec()), ("bad.txt", eicar)],
            ),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let jobs = v["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2, "{v}");
    assert_eq!(jobs[0]["status"], "queued", "{v}");
    assert_eq!(jobs[1]["status"], "rejected", "{v}");
    let (clean, bad) = (job_id(&jobs[0]), job_id(&jobs[1]));
    // Only the clean original is stored.
    assert_eq!(e.keys().await, vec![original_key(tenant_a(), clean)]);
    let (s, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{bad}"),
            TOKEN_A,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "rejected", "{v}");
    assert_eq!(v["error"]["code"], "infected", "{v}");
    assert_eq!(v["error"]["class"], "invalid_request", "{v}");
    assert_eq!(v["error"]["message"], common::EICAR_SIGNATURE, "{v}");
    assert_eq!(v["source"]["scan"], "infected", "{v}");
    assert_eq!(v["source"]["filename"], "bad.txt", "{v}");
    assert_eq!(v["steps"], json!([]), "a rejected job never runs: {v}");
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{clean}"),
            TOKEN_A,
        )
        .await;
    assert_eq!(v["status"], "queued", "{v}");
    assert_eq!(v["source"]["scan"], "clean", "{v}");
}

/// storage_service answers 503 (scanner down, or a body over clamd's limit): the request fails
/// as upstream-unavailable, never as stored-and-clean; the earlier file of the same request is
/// removed and no job exists.
#[tokio::test]
async fn storage_503_is_retryable_not_clean() {
    let e = env().await;
    e.storage.set_unavailable_after(1);
    let (s, v) = e
        .upload(
            TOKEN_A,
            form(
                leaf(),
                &[
                    ("one.txt", b"first".to_vec()),
                    ("two.txt", b"second".to_vec()),
                ],
            ),
        )
        .await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{v}");
    assert_error(&v, "upstream_unavailable");
    eventually("file 1 removed", || e.nothing_left()).await;
    // Both files reached storage; only the 503'd one was never stored.
    assert_eq!(e.storage.uploads().len(), 2);
}

/// Rulings R24: the default limit is storage_service's scan ceiling (100 MiB). One byte more is a
/// 413 from strata-ingest itself, before storage_service is called.
#[tokio::test]
async fn upload_over_100mib_is_413_before_storage() {
    let e = env().await;
    assert_eq!(e.ctx.cfg.limits.max_upload_bytes, 100 * MIB as u64);
    let over = vec![b'a'; 100 * MIB + 1];
    let (s, v) = e.upload(TOKEN_A, form(leaf(), &[("big.txt", over)])).await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_error(&v, "payload_too_large");
    assert!(
        e.storage.requests().is_empty(),
        "{:?}",
        e.storage.requests()
    );
    assert_eq!(e.job_count().await, 0);
}

#[tokio::test]
async fn multipart_two_files_two_jobs() {
    let e = env().await;
    let f = form(
        leaf(),
        &[
            ("one.txt", b"first file".to_vec()),
            ("two.md", b"# second\n\nfile".to_vec()),
        ],
    )
    .text("unused", "x");
    // A field after the files that is not store_id/options/file is refused.
    let (s, v) = e.upload(TOKEN_A, f).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(e.job_count().await, 0);
    assert!(e.keys().await.is_empty());

    let f = Form::new()
        .text("store_id", leaf().to_string())
        .text("options", r#"{"ocr":"off"}"#)
        .part("file", f_part("one.txt", b"first file"))
        .part("file", f_part("two.md", b"# second\n\nfile"));
    let (s, v) = e.upload(TOKEN_A, f).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let jobs = v["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 2);
    let (one, two) = (job_id(&jobs[0]), job_id(&jobs[1]));
    assert_ne!(one, two);
    for (id, name, body) in [
        (one, "one.txt", &b"first file"[..]),
        (two, "two.md", &b"# second\n\nfile"[..]),
    ] {
        let stored = e.storage.get(&original_key(tenant_a(), id)).unwrap();
        assert_eq!(stored.as_ref(), body);
        let (s, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs/{id}"),
                TOKEN_A,
            )
            .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v["status"], "queued");
        assert_eq!(v["store_id"], leaf().to_string());
        assert_eq!(v["source"]["kind"], "upload");
        assert_eq!(v["source"]["filename"], name);
        assert_eq!(v["source"]["bytes"], body.len());
        assert_eq!(
            v["options"],
            json!({"ocr": "off", "captions": true, "language": null})
        );
        assert!(v["meta"].is_object());
        let steps = v["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 1);
        for k in [
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
            "output",
        ] {
            assert!(steps[0].get(k).is_some(), "step field {k}: {v}");
        }
        assert_eq!(steps[0]["kind"], "acquire");
        assert_eq!(steps[0]["status"], "pending");
    }
    // store_id after a file part is refused.
    let f = Form::new()
        .part("file", f_part("one.txt", b"first"))
        .text("store_id", leaf().to_string());
    let (s, v) = e.upload(TOKEN_A, f).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    // options after a file part too.
    let f = Form::new()
        .text("store_id", leaf().to_string())
        .part("file", f_part("one.txt", b"first"))
        .text("options", "{}");
    let (s, _) = e.upload(TOKEN_A, f).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    // Unknown option fields / values.
    for bad in [r#"{"ocr":"always"}"#, r#"{"colour":true}"#, "not json"] {
        let f = Form::new()
            .text("store_id", leaf().to_string())
            .text("options", bad)
            .part("file", f_part("one.txt", b"first"));
        let (s, v) = e.upload(TOKEN_A, f).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    // No file part at all.
    let (s, _) = e
        .upload(TOKEN_A, Form::new().text("store_id", leaf().to_string()))
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(e.job_count().await, 2);
}

fn f_part(name: &str, body: &[u8]) -> Part {
    Part::bytes(body.to_vec()).file_name(name.to_string())
}

// ---------------------------------------------------------------------------------------------
// URL jobs

async fn post_json(e: &Env, body: Value) -> (StatusCode, Value) {
    status_json(
        e.req(reqwest::Method::POST, "/api/ingest/jobs", TOKEN_A)
            .json(&body)
            .send()
            .await
            .unwrap(),
    )
    .await
}

#[tokio::test]
async fn url_jobs_are_created() {
    let e = env().await;
    let (s, v) = post_json(
        &e,
        json!({"store_id": leaf(), "url": "https://example.com/a", "options": {"captions": false}}),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let id = job_id(&v["jobs"][0]);
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{id}"),
            TOKEN_A,
        )
        .await;
    assert_eq!(
        v["source"],
        json!({"kind": "url", "url": "https://example.com/a"})
    );
    assert_eq!(
        v["options"],
        json!({"ocr": "auto", "captions": false, "language": null})
    );
    let urls: Vec<String> = (0..3).map(|i| format!("http://example.org/{i}")).collect();
    let (s, v) = post_json(&e, json!({"store_id": leaf(), "urls": urls})).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["jobs"].as_array().unwrap().len(), 3);
    let too_many: Vec<String> = (0..51).map(|i| format!("http://example.org/{i}")).collect();
    let (s, _) = post_json(&e, json!({"store_id": leaf(), "urls": too_many})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    for bad in [
        json!({"store_id": leaf()}),
        json!({"store_id": leaf(), "urls": []}),
        json!({"store_id": leaf(), "url": "https://a.example", "urls": ["https://b.example"]}),
        json!({"store_id": leaf(), "url": "https://a.example", "extra": 1}),
        json!({"store_id": leaf(), "url": "ftp://a.example/x"}),
        json!({"store_id": leaf(), "url": "file:///etc/passwd"}),
        json!({"store_id": leaf(), "url": "not a url"}),
        json!({"store_id": leaf(), "url": "https://user:pw@a.example/"}),
        json!({"store_id": "nope", "url": "https://a.example"}),
    ] {
        let (s, v) = post_json(&e, bad.clone()).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    assert_eq!(e.job_count().await, 4);
}

#[tokio::test]
async fn url_job_private_ip_rejected() {
    let e = env().await;
    for url in [
        "http://127.0.0.1/",
        "http://localhost:9135/api",
        "http://10.0.0.5/",
        "http://172.16.1.1/",
        "http://192.168.1.1/",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.64.0.2/",
        "http://0.0.0.0/",
        "http://2130706433/",
        "http://0x7f.1/",
        "http://[::1]/",
        "http://[fd00::1]/",
        "http://[fe80::1]/",
        "http://[::ffff:127.0.0.1]/",
        "http://[::ffff:10.0.0.1]/",
        // Internal hostnames.
        "http://storage.example.internal:8389/",
        "http://db.example.internal/",
        "http://MINIO.INTERNAL./",
        "http://db.example.svc.cluster.local:6432/",
        "http://pgbouncer.pgbouncer.svc/",
        "http://printer.local/",
        "http://router.lan/",
        "http://nas.home.arpa/",
        "http://app.localhost/",
        "http://localhost./",
        "http://LOCALHOST/",
        "http://minio/",
        "http://minio.:9000/",
        // IPv4 inside 6to4, Teredo (client = NOT of the last 32 bits) and NAT64 local-use.
        "http://[2002:a00:1::1]/",
        "http://[2002:7f00:1::]/",
        "http://[2001:0:4136:e378:8000:63bf:f5ff:fffe]/",
        "http://[64:ff9b::a00:1]/",
        "http://[64:ff9b:1::5db8:d70e]/",
    ] {
        let (s, v) = post_json(&e, json!({"store_id": leaf(), "url": url})).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{url}: {v}");
    }
    assert_eq!(e.job_count().await, 0);
    for ok in [
        "https://93.184.215.14/page",
        "https://www.example.com/a",
        "http://[2002:5db8:d70e::1]/",
        "http://[2001:0:4136:e378:8000:63bf:a247:28f1]/",
        "http://[64:ff9b::5db8:d70e]/",
    ] {
        let (s, v) = post_json(&e, json!({"store_id": leaf(), "url": ok})).await;
        assert_eq!(s, StatusCode::ACCEPTED, "{ok}: {v}");
    }
}

// ---------------------------------------------------------------------------------------------
// List / cancel / retry

#[tokio::test]
async fn list_filters_and_validates() {
    let e = env().await;
    let a = e.text_job(TOKEN_A).await;
    let b = e.text_job(TOKEN_A).await;
    let (s, v) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs?limit=1", TOKEN_A)
        .await;
    assert_eq!(s, StatusCode::OK);
    let jobs = v["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["id"], b.to_string(), "newest first");
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs?store_id={}&status=queued", leaf()),
            TOKEN_A,
        )
        .await;
    assert_eq!(v["jobs"].as_array().unwrap().len(), 2);
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs?store_id={}", Uuid::now_v7()),
            TOKEN_A,
        )
        .await;
    assert_eq!(v["jobs"], json!([]));
    e.call(
        reqwest::Method::POST,
        &format!("/api/ingest/jobs/{a}/cancel"),
        TOKEN_A,
    )
    .await;
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            "/api/ingest/jobs?status=cancelled",
            TOKEN_A,
        )
        .await;
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(v["jobs"][0]["id"], a.to_string());
    let (s, _) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs?limit=1000", TOKEN_A)
        .await;
    assert_eq!(s, StatusCode::OK, "limit is capped, not refused");
    for bad in [
        "status=done",
        "limit=0",
        "limit=abc",
        "store_id=nope",
        "colour=red",
    ] {
        let (s, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs?{bad}"),
                TOKEN_A,
            )
            .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    let (s, _) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs/not-a-uuid", TOKEN_A)
        .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{}", Uuid::now_v7()),
            TOKEN_A,
        )
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_pages_with_a_before_cursor() {
    let e = env().await;
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(e.text_job(TOKEN_A).await);
    }
    ids.reverse(); // newest first, the list order
    let other_tenant = e.text_job(TOKEN_B).await;
    let page = |q: String| {
        let e = &e;
        async move {
            let (s, v) = e
                .call(
                    reqwest::Method::GET,
                    &format!("/api/ingest/jobs?{q}"),
                    TOKEN_A,
                )
                .await;
            assert_eq!(s, StatusCode::OK, "{q}: {v}");
            let got: Vec<String> = v["jobs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|j| j["id"].as_str().unwrap().to_string())
                .collect();
            (got, v["next"].clone())
        }
    };
    let s = |i: usize| ids[i].to_string();
    let (p1, n1) = page("limit=2".into()).await;
    assert_eq!((p1, n1.clone()), (vec![s(0), s(1)], json!(s(1))));
    let (p2, n2) = page(format!("limit=2&before={}", s(1))).await;
    assert_eq!((p2, n2), (vec![s(2), s(3)], json!(s(3))));
    let (p3, n3) = page(format!("limit=2&before={}", s(3))).await;
    assert_eq!((p3, n3), (vec![s(4)], Value::Null), "last page: no next");
    // Without a cursor: today's behaviour, plus `next: null` when everything fits.
    let (all, next) = page(String::new()).await;
    assert_eq!((all.len(), next), (5, Value::Null));
    // Filters hold across pages.
    let (f, _) = page(format!("limit=10&status=queued&before={}", s(2))).await;
    assert_eq!(f, vec![s(3), s(4)]);
    // A cursor that is not one of the caller's jobs is a 400.
    for bad in [
        other_tenant.to_string(),
        Uuid::now_v7().to_string(),
        "x".into(),
    ] {
        let (st, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs?before={bad}"),
                TOKEN_A,
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
}

#[tokio::test]
async fn source_id_is_kept_in_meta_for_url_and_upload_jobs() {
    let e = env().await;
    let (s, v) = post_json(
        &e,
        json!({"store_id": leaf(), "url": "https://example.com/a", "source_id": " web-1 ",
               "source_run": " run-1 "}),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let url_job = job_id(&v["jobs"][0]);
    let form = Form::new()
        .text("store_id", leaf().to_string())
        .text("source_run", "run-2")
        .text("source_id", "file-1")
        .part("file", f_part("a.txt", b"hello"));
    let (s, v) = e.upload(TOKEN_A, form).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let upload_job = job_id(&v["jobs"][0]);
    for (job, sid, run) in [(url_job, "web-1", "run-1"), (upload_job, "file-1", "run-2")] {
        let (_, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs/{job}"),
                TOKEN_A,
            )
            .await;
        assert_eq!(v["meta"]["source_id"], sid, "{v}");
        assert_eq!(v["meta"]["source_run"], run, "{v}");
    }
    // Without one, meta stays as before.
    let plain = e.text_job(TOKEN_A).await;
    let (_, v) = e
        .call(
            reqwest::Method::GET,
            &format!("/api/ingest/jobs/{plain}"),
            TOKEN_A,
        )
        .await;
    assert_eq!(v["meta"], json!({}), "{v}");
    // Blank, too long, after a file, duplicated: 400.
    for bad in [json!(""), json!("x".repeat(201)), json!(7)] {
        let (s, v) = post_json(
            &e,
            json!({"store_id": leaf(), "url": "https://example.com/b", "source_id": bad}),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    }
    let late = Form::new()
        .text("store_id", leaf().to_string())
        .part("file", f_part("a.txt", b"hello"))
        .text("source_id", "file-1");
    let (s, v) = e.upload(TOKEN_A, late).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let twice = Form::new()
        .text("store_id", leaf().to_string())
        .text("source_id", "a")
        .text("source_id", "b")
        .part("file", f_part("a.txt", b"hello"));
    let (s, v) = e.upload(TOKEN_A, twice).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    // source_run: malformed, without a source_id, after a file, duplicated: 400.
    for bad in [json!(""), json!("x".repeat(201)), json!("a\tb"), json!(7)] {
        let (s, v) = post_json(
            &e,
            json!({"store_id": leaf(), "url": "https://example.com/b", "source_id": "s",
                   "source_run": bad}),
        )
        .await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    }
    let (s, v) = post_json(
        &e,
        json!({"store_id": leaf(), "url": "https://example.com/b", "source_run": "r"}),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let forms = [
        Form::new()
            .text("store_id", leaf().to_string())
            .text("source_run", "r")
            .part("file", f_part("a.txt", b"hello")),
        Form::new()
            .text("store_id", leaf().to_string())
            .text("source_id", "s")
            .part("file", f_part("a.txt", b"hello"))
            .text("source_run", "r"),
        Form::new()
            .text("store_id", leaf().to_string())
            .text("source_id", "s")
            .text("source_run", "r")
            .text("source_run", "r")
            .part("file", f_part("a.txt", b"hello")),
        Form::new()
            .text("store_id", leaf().to_string())
            .text("source_id", "s")
            .text("source_run", " ")
            .part("file", f_part("a.txt", b"hello")),
    ];
    for form in forms {
        let (s, v) = e.upload(TOKEN_A, form).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    }
    assert_eq!(e.job_count().await, 3);
}

#[tokio::test]
async fn document_fields_are_kept_in_meta_for_every_uploaded_file() {
    let e = env().await;
    let prov = json!({"source_url": "https://x.eu/a.pdf", "license": "CC-BY-4.0",
                      "retrieved_at": "2026-10-21T08:00:00Z"});
    let form = Form::new()
        .text("store_id", leaf().to_string())
        .text("topic_keys", r#"["hr/leave", "hr/leave", "hr/a"]"#)
        .text("supersedes", "[]")
        .text("provenance", prov.to_string())
        .part("file", f_part("a.txt", b"hello"))
        .part("file", f_part("b.txt", b"world"));
    let (s, v) = e.upload(TOKEN_A, form).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let jobs = v["jobs"].as_array().unwrap().clone();
    assert_eq!(jobs.len(), 2, "{v}");
    for j in &jobs {
        let (_, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs/{}", job_id(j)),
                TOKEN_A,
            )
            .await;
        assert_eq!(v["meta"]["topic_keys"], json!(["hr/a", "hr/leave"]), "{v}");
        assert_eq!(v["meta"]["supersedes"], json!([]), "{v}");
        assert_eq!(v["meta"]["provenance"], prov, "{v}");
    }
    // Not JSON, a bad key, a bad / incomplete provenance, duplicated, after a file: 400, no job.
    let bad_fields: [(&'static str, String); 7] = [
        ("topic_keys", "hr/leave".into()),
        ("topic_keys", r#"["HR"]"#.into()),
        ("supersedes", r#"{"a": 1}"#.into()),
        (
            "provenance",
            r#"{"source_url": "u", "license": "MIT"}"#.into(),
        ),
        (
            "provenance",
            r#"{"source_url": "u", "license": "MIT", "retrieved_at": "now"}"#.into(),
        ),
        (
            "provenance",
            r#"{"source_url": "u", "license": " ", "retrieved_at": "2026-10-21T08:00:00Z"}"#.into(),
        ),
        ("supersedes", format!("{:?}", ["k"; 33])),
    ];
    for (name, value) in bad_fields {
        let form = Form::new()
            .text("store_id", leaf().to_string())
            .text(name, value.clone())
            .part("file", f_part("a.txt", b"hello"));
        let (s, v) = e.upload(TOKEN_A, form).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{name}={value}: {v}");
    }
    let twice = Form::new()
        .text("store_id", leaf().to_string())
        .text("topic_keys", "[]")
        .text("topic_keys", "[]")
        .part("file", f_part("a.txt", b"hello"));
    let (s, v) = e.upload(TOKEN_A, twice).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let late = Form::new()
        .text("store_id", leaf().to_string())
        .part("file", f_part("a.txt", b"hello"))
        .text("provenance", prov.to_string());
    let (s, v) = e.upload(TOKEN_A, late).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(e.job_count().await, 2);
}

async fn step_statuses(e: &Env, job: Uuid) -> Vec<(String, String)> {
    sqlx::query("SELECT kind, status FROM ingest_steps WHERE job_id = $1 ORDER BY id")
        .bind(job)
        .fetch_all(&e.db.pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get("kind"), r.get("status")))
        .collect()
}

#[tokio::test]
async fn cancel_stops_pending_steps() {
    let e = env().await;
    let id = e.text_job(TOKEN_A).await;
    let path = format!("/api/ingest/jobs/{id}");
    let cancel = format!("{path}/cancel");
    let (s, v) = e.call(reqwest::Method::POST, &cancel, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "cancelled");
    assert_eq!(v["steps"][0]["status"], "cancelled");
    assert_eq!(
        step_statuses(&e, id).await,
        vec![("acquire".to_string(), "cancelled".to_string())]
    );
    // A worker pool finds nothing to run.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let h = tokio::spawn(strata_ingest::worker::run_workers(
        e.ctx.clone(),
        1,
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    shutdown.cancel();
    h.await.unwrap();
    assert_eq!(
        step_statuses(&e, id).await,
        vec![("acquire".to_string(), "cancelled".to_string())]
    );
    // Idempotent: cancelling a cancelled job answers its view again.
    let (s, v) = e.call(reqwest::Method::POST, &cancel, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "cancelled");
}

/// Sets a job's status (and optionally its document ids) directly: a job that ended without
/// running the real pipeline.
async fn set_status(e: &Env, id: Uuid, status: &str, docs: &[Uuid]) {
    sqlx::query("UPDATE ingest_jobs SET status = $2, document_ids = $3 WHERE id = $1")
        .bind(id)
        .bind(status)
        .bind(docs)
        .execute(&e.db.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancel_of_an_ended_job_is_409() {
    let e = env().await;
    for status in ["completed", "failed", "rejected"] {
        let id = e.text_job(TOKEN_A).await;
        set_status(&e, id, status, &[]).await;
        let (s, v) = e
            .call(
                reqwest::Method::POST,
                &format!("/api/ingest/jobs/{id}/cancel"),
                TOKEN_A,
            )
            .await;
        assert_eq!(s, StatusCode::CONFLICT, "{status}: {v}");
        assert_error(&v, "conflict");
        let (_, v) = e
            .call(
                reqwest::Method::GET,
                &format!("/api/ingest/jobs/{id}"),
                TOKEN_A,
            )
            .await;
        assert_eq!(v["status"], status, "unchanged");
    }
}

/// A runner whose Acquire blocks until `release` is notified, then asks for a Scan; every other
/// step completes at once. Counts runs in `calls`.
fn blocking_acquire(
    calls: Arc<std::sync::atomic::AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
) -> strata_ingest::worker::StepRunner {
    use strata_ingest::db::steps::{ClaimedStep, StepKind, StepOutput};
    Arc::new(move |_ctx, step: ClaimedStep| {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let r = release.clone();
        Box::pin(async move {
            if step.kind == StepKind::Acquire {
                r.notified().await;
            }
            Ok((
                StepOutput::default(),
                vec![(StepKind::Scan, String::new(), json!({}))],
            ))
        })
    })
}

async fn job_status_of(e: &Env, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(&e.db.pool)
        .await
        .unwrap()
}

/// Cancel while a worker runs the job's step: the job is `cancelling` (delete refused), the
/// worker finishes that step, enqueues nothing more, and the job ends `cancelled`; then delete
/// removes the job, its steps and its staged original.
#[tokio::test]
async fn cancel_while_running_is_cooperative_then_delete_cleans_up() {
    let e = env().await;
    let id = e.text_job(TOKEN_A).await;
    let original = original_key(tenant_a(), id);
    assert!(e.storage.get(&original).is_some(), "the upload is staged");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let shutdown = tokio_util::sync::CancellationToken::new();
    let opts = strata_ingest::worker::WorkerOpts {
        poll: Duration::from_millis(50),
        ..Default::default()
    };
    let h = tokio::spawn(strata_ingest::worker::run_workers_with(
        e.ctx.clone(),
        2,
        shutdown.clone(),
        opts,
        blocking_acquire(calls.clone(), release.clone()),
    ));
    eventually("acquire to run", || async {
        step_statuses(&e, id).await == vec![("acquire".to_string(), "running".to_string())]
    })
    .await;

    let path = format!("/api/ingest/jobs/{id}");
    let cancel = format!("{path}/cancel");
    let (s, v) = e.call(reqwest::Method::POST, &cancel, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "cancelling", "{v}");
    assert_eq!(v["steps"][0]["status"], "running", "{v}");
    // Idempotent while cancelling.
    let (s, v) = e.call(reqwest::Method::POST, &cancel, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "cancelling", "{v}");
    // Delete waits for the running step.
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_error(&v, "conflict");

    release.notify_one();
    eventually("the job to be cancelled", || async {
        job_status_of(&e, id).await.as_deref() == Some("cancelled")
    })
    .await;
    // Give the pool time to (wrongly) pick anything up.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    h.await.unwrap();
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "only Acquire ever ran"
    );
    assert_eq!(
        step_statuses(&e, id).await,
        vec![("acquire".to_string(), "completed".to_string())],
        "the running step finished; no Scan was enqueued"
    );

    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_B).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "another tenant: {v}");
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v,
        json!({"job_id": id, "status": "cancelled", "deleted_jobs": 1, "deleted_objects": 1,
               "document_ids": [], "documents": "retained"})
    );
    assert!(
        e.storage.get(&original).is_none(),
        "the staged original is gone"
    );
    assert_eq!(e.job_count().await, 0);
    let steps_left: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_steps")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(steps_left, 0);
    let (s, _) = e.call(reqwest::Method::GET, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "a second delete finds nothing");
}

/// Race: a cancel and the completion of the job's running step (which asks for a successor)
/// commit concurrently, in either order. Whatever wins, the job ends `cancelled` with nothing
/// left to run, never `cancelling` for ever and never with a successor enqueued.
#[tokio::test]
async fn cancel_racing_step_completion_always_ends_cancelled() {
    use strata_ingest::db::{jobs, steps};
    let e = env().await;
    for round in 0..20 {
        let id = e.text_job(TOKEN_A).await;
        let step = steps::claim_next(&e.db.pool, 5)
            .await
            .unwrap()
            .expect("the acquire step");
        assert_eq!(step.job_id, id);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let (b1, b2) = (barrier.clone(), barrier.clone());
        let (pool1, pool2) = (e.db.pool.clone(), e.db.pool.clone());
        let cancel = tokio::spawn(async move {
            b1.wait().await;
            jobs::cancel(&pool1, tenant_a(), id).await
        });
        let complete = tokio::spawn(async move {
            b2.wait().await;
            steps::complete_and_enqueue(
                &pool2,
                &step,
                steps::StepOutput::default(),
                &[(steps::StepKind::Scan, String::new(), json!({}))],
            )
            .await
        });
        let (c, d) = (cancel.await.unwrap(), complete.await.unwrap());
        c.unwrap_or_else(|err| panic!("round {round}: cancel: {err}"));
        d.unwrap_or_else(|err| panic!("round {round}: complete: {err}"));
        assert_eq!(
            job_status_of(&e, id).await.as_deref(),
            Some("cancelled"),
            "round {round}"
        );
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ingest_steps
             WHERE job_id = $1 AND status IN ('pending','running')",
        )
        .bind(id)
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
        assert_eq!(open, 0, "round {round}: nothing left to run");
    }
    assert!(
        steps::claim_next(&e.db.pool, 5).await.unwrap().is_none(),
        "no cancelled job has a claimable step"
    );
}

#[tokio::test]
async fn delete_only_ended_jobs_and_leaves_strata_documents() {
    let e = env().await;
    // queued / running → 409, nothing removed.
    let id = e.text_job(TOKEN_A).await;
    let path = format!("/api/ingest/jobs/{id}");
    for status in ["queued", "running"] {
        set_status(&e, id, status, &[]).await;
        let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
        assert_eq!(s, StatusCode::CONFLICT, "{status}: {v}");
        assert_error(&v, "conflict");
        assert!(
            v["error"]["message"].as_str().unwrap().contains("cancel"),
            "{v}"
        );
    }
    assert!(e.storage.get(&original_key(tenant_a(), id)).is_some());

    // A completed job with a recorded figure and a delivered document.
    let doc = Uuid::now_v7();
    set_status(&e, id, "completed", &[doc]).await;
    let figure = format!("{}/{id}/figures/{}", tenant_a(), Uuid::now_v7());
    e.storage.put(&figure, &b"png"[..]);
    sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, bytes, mime)
         VALUES ($1, $2, 'figure', $3, 3, 'image/png')",
    )
    .bind(Uuid::now_v7())
    .bind(id)
    .bind(&figure)
    .execute(&e.db.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO ingest_artifacts (job_id, key, body) VALUES ($1, 'bundle.md', 'x')")
        .bind(id)
        .execute(&e.db.pool)
        .await
        .unwrap();
    // storage_service down: 502; the job stays `deleting` (no row lock held, its rows kept),
    // which nothing claims, retries or cancels; deleting again finishes it.
    e.storage.set_deletes_unavailable(true);
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{v}");
    assert_error(&v, "upstream_unavailable");
    let (s, v) = e.call(reqwest::Method::GET, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "deleting", "GET shows it: {v}");
    let (s, v) = e
        .call(
            reqwest::Method::GET,
            "/api/ingest/jobs?status=deleting",
            TOKEN_A,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v["jobs"][0]["id"], json!(id), "{v}");
    for action in ["retry", "cancel"] {
        let (s, v) = e
            .call(reqwest::Method::POST, &format!("{path}/{action}"), TOKEN_A)
            .await;
        assert_eq!(s, StatusCode::CONFLICT, "{action}: {v}");
        assert_error(&v, "conflict");
    }
    // Its pending Acquire step is never claimed.
    assert!(
        strata_ingest::db::steps::claim_next(&e.db.pool, 5)
            .await
            .unwrap()
            .is_none(),
        "a deleting job's step is not claimable"
    );
    assert_eq!(job_status_of(&e, id).await.as_deref(), Some("deleting"));
    e.storage.set_deletes_unavailable(false);
    let other = e.text_job(TOKEN_A).await;

    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "completed");
    assert_eq!(v["deleted_jobs"], 1);
    assert_eq!(v["deleted_objects"], 2, "original + figure: {v}");
    assert_eq!(v["document_ids"], json!([doc]));
    assert_eq!(v["documents"], "retained");
    assert!(e.storage.get(&figure).is_none());
    assert!(e.storage.get(&original_key(tenant_a(), id)).is_none());
    for table in ["ingest_steps", "ingest_assets", "ingest_artifacts"] {
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {table} WHERE job_id = $1"
        )))
        .bind(id)
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    // The other job and its staged original are untouched.
    assert_eq!(job_status_of(&e, other).await.as_deref(), Some("queued"));
    assert!(e.storage.get(&original_key(tenant_a(), other)).is_some());
}

/// A failed job whose parallel step is still running cannot be deleted until that step ends.
#[tokio::test]
async fn delete_waits_for_a_running_step_of_a_failed_job() {
    let e = env().await;
    let id = e.text_job(TOKEN_A).await;
    let step = strata_ingest::db::steps::claim_next(&e.db.pool, 5)
        .await
        .unwrap()
        .expect("the acquire step");
    set_status(&e, id, "failed", &[]).await;
    let path = format!("/api/ingest/jobs/{id}");
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert!(
        v["error"]["message"].as_str().unwrap().contains("running"),
        "{v}"
    );
    strata_ingest::db::steps::complete_and_enqueue(
        &e.db.pool,
        &step,
        strata_ingest::db::steps::StepOutput::default(),
        &[],
    )
    .await
    .unwrap();
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "failed");
}

#[tokio::test]
async fn retry_only_when_failed() {
    let e = env().await;
    let id = e.text_job(TOKEN_A).await;
    let retry = format!("/api/ingest/jobs/{id}/retry");
    let (s, v) = e.call(reqwest::Method::POST, &retry, TOKEN_A).await;
    assert_eq!(s, StatusCode::CONFLICT, "queued: {v}");
    assert_error(&v, "conflict");
    sqlx::query(
        "UPDATE ingest_steps SET status = 'failed', attempt = 3,
                error = '{\"code\":\"x\"}', finished_at = now() WHERE job_id = $1",
    )
    .bind(id)
    .execute(&e.db.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE ingest_jobs SET status = 'failed', error = '{\"code\":\"x\"}' WHERE id = $1",
    )
    .bind(id)
    .execute(&e.db.pool)
    .await
    .unwrap();
    let (s, v) = e.call(reqwest::Method::POST, &retry, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "running");
    assert_eq!(v["error"], Value::Null);
    assert_eq!(v["steps"][0]["status"], "pending");
    // `attempt` is the fence token and never goes back; the retry gives a fresh budget instead
    // (`attempt_base = attempt`).
    assert_eq!(v["steps"][0]["attempt"], 3);
    let base: i32 = sqlx::query_scalar(
        "SELECT attempt_base FROM ingest_steps WHERE job_id = $1 AND kind = 'acquire'",
    )
    .bind(id)
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(base, 3, "retry refunds the earlier runs");
    let (s, _) = e.call(reqwest::Method::POST, &retry, TOKEN_A).await;
    assert_eq!(s, StatusCode::CONFLICT, "running");
}

// ---------------------------------------------------------------------------------------------
// Health

#[tokio::test]
async fn health_reports_provider_status() {
    let e = env_with(|c| {
        c.providers.ocr = Some(provider("mistral"));
        c.providers.transcribe = Some(provider("openai"));
        c.storage_token = Some(Secret::new("storage-licence-SECRET"));
    })
    .await;
    e.ctx.health.record_success(Capability::Ocr);
    e.ctx.health.record_error(Capability::Ocr, ErrorClass::Auth);
    let r = e
        .http
        .get(format!("{}/health", e.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let text = r.text().await.unwrap();
    let v: Value = serde_json::from_str(&text).unwrap();
    for dep in ["db", "storage", "docling", "strata"] {
        assert_eq!(v[dep], "ok", "{dep}: {v}");
    }
    for gone in ["minio", "clamav"] {
        assert!(v.get(gone).is_none(), "{gone}: {v}");
    }
    assert_eq!(v["status"], "ok");
    assert_eq!(v["vision"], "unconfigured");
    assert!(v["ocr"]["last_success"].is_string(), "{v}");
    assert_eq!(v["ocr"]["last_error_class"], "auth");
    assert_eq!(v["transcribe"]["last_success"], Value::Null);
    assert_eq!(v["transcribe"]["last_error_class"], Value::Null);
    for secret in [
        "127.0.0.1",
        "provider-secret-key",
        "strata-token",
        "storage-licence-SECRET",
    ] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }

    // Dependencies down: still 200, each `down`, answered within the per-check timeout.
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let mut cfg = config(&db, &storage, UNUSED_URL, UNUSED_URL);
    cfg.storage_url = UNUSED_URL.to_string();
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let url = spawn(strata_ingest::api::router(ctx)).await;
    let t = Instant::now();
    let r = e.http.get(format!("{url}/health")).send().await.unwrap();
    assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
    assert_eq!(r.status().as_u16(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["db"], "ok");
    for dep in ["storage", "docling", "strata"] {
        assert_eq!(v[dep], "down", "{dep}: {v}");
    }
    assert_eq!(v["status"], "degraded");
    assert_eq!(v["ocr"], "unconfigured");
    // Cached: a second call is immediate.
    let t = Instant::now();
    e.http.get(format!("{url}/health")).send().await.unwrap();
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "{:?}",
        t.elapsed()
    );
}

// ---------------------------------------------------------------------------------------------
// serve

#[tokio::test]
async fn serve_runs_api_and_workers_and_stops_on_signal() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (strata, docling) = tokio::join!(strata_mock(), docling_mock());
    let cfg = config(&db, &storage, &docling, &strata);
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(strata_ingest::api::serve(ctx, listener, async move {
        let _ = rx.await;
    }));
    let http = reqwest::Client::new();
    let r = http.get(format!("{url}/health")).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    // The workers run: an upload's Acquire step is claimed and completed.
    let r = http
        .post(format!("{url}/api/ingest/jobs"))
        .bearer_auth(TOKEN_A)
        .multipart(form(leaf(), &[("a.txt", b"hello world".to_vec())]))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM ingest_steps WHERE kind = 'acquire' AND status = 'completed'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        if done == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "acquire never completed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(40), server)
        .await
        .expect("serve stops")
        .unwrap()
        .expect("serve ok");
    assert!(http.get(format!("{url}/health")).send().await.is_err());
}

/// Startup (rulings: Postgres unreachable at boot must not crash the pod): the API listens at
/// once, `/health` answers 200 with `ready: false` and db `down`, every job route is 503
/// `not_ready` (after auth), and no step runs; once startup succeeds the routes and the workers
/// run.
#[tokio::test]
async fn serve_answers_503_not_ready_and_runs_no_workers_until_startup_succeeds() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (strata, docling) = tokio::join!(strata_mock(), docling_mock());
    let cfg = config(&db, &storage, &docling, &strata);
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    // A job already queued: no worker may claim its Acquire step before startup succeeded.
    let job = strata_ingest::db::jobs::create(
        &db.pool,
        strata_ingest::db::jobs::NewJob {
            tenant: tenant_a(),
            store_id: leaf(),
            source: json!({"kind": "url", "url": "https://example.com/"}),
            options: json!({"ocr": "auto", "captions": false, "language": null}),
        },
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(strata_ingest::api::serve_starting(
        ctx,
        listener,
        async move {
            let _ = stop_rx.await;
        },
        async move {
            let _ = ready_rx.await;
            Ok(())
        },
    ));
    let http = reqwest::Client::new();
    let h: Value = http
        .get(format!("{url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (h["ready"].clone(), h["db"].clone()),
        (json!(false), json!("down")),
        "{h}"
    );
    assert_eq!(h["status"], "degraded");
    for r in [
        http.get(format!("{url}/api/ingest/jobs"))
            .bearer_auth(TOKEN_A),
        http.post(format!("{url}/api/ingest/jobs"))
            .bearer_auth(TOKEN_A)
            .json(&json!({"store_id": leaf(), "url": "https://example.com/"})),
    ] {
        let r = r.send().await.unwrap();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.headers()["retry-after"], "5");
        let (_, v) = status_json(r).await;
        assert_error(&v, "not_ready");
    }
    let r = http
        .get(format!("{url}/api/ingest/jobs"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "auth still first");
    tokio::time::sleep(Duration::from_millis(800)).await;
    // `attempt` counts claims (it only grows): a step that ran and went back to `pending` (e.g. a
    // retryable failure) still shows the claim, where `status` alone would not.
    let acquire = || async {
        sqlx::query_as::<_, (String, i32)>(
            "SELECT status, attempt FROM ingest_steps WHERE job_id = $1 AND kind = 'acquire'",
        )
        .bind(job)
        .fetch_one(&db.pool)
        .await
        .unwrap()
    };
    assert_eq!(
        acquire().await,
        ("pending".to_owned(), 0),
        "no worker before startup"
    );
    ready_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while acquire().await.1 == 0 {
        assert!(Instant::now() < deadline, "workers never started");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let r = http
        .get(format!("{url}/api/ingest/jobs"))
        .bearer_auth(TOKEN_A)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let h: Value = http
        .get(format!("{url}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (h["ready"].clone(), h["db"].clone()),
        (json!(true), json!("ok")),
        "{h}"
    );
    stop_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(40), server)
        .await
        .expect("serve stops")
        .unwrap()
        .expect("serve ok");
}

/// A startup that gives up (DB unreachable past the deadline) stops the server with its error
/// (exit non-zero); a signal during startup stops cleanly.
#[tokio::test]
async fn serve_returns_the_startup_error_and_stops_cleanly_on_a_signal_during_startup() {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let (strata, docling) = tokio::join!(strata_mock(), docling_mock());
    let cfg = config(&db, &storage, &docling, &strata);
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let r = tokio::time::timeout(
        Duration::from_secs(40),
        strata_ingest::api::serve_starting(ctx.clone(), listener, std::future::pending(), async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Err(strata_ingest::IngestError::Config("db down".into()))
        }),
    )
    .await
    .expect("serve stops after a failed startup");
    assert!(
        matches!(r, Err(strata_ingest::IngestError::Config(_))),
        "{r:?}"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(40),
        strata_ingest::api::serve_starting(
            ctx,
            listener,
            tokio::time::sleep(Duration::from_millis(200)),
            std::future::pending(),
        ),
    )
    .await
    .expect("serve stops on a signal during startup")
    .expect("clean stop");
}

/// The startup retry: Postgres not listening at first (connection refused), up after ~2.5 s
/// (a TCP forwarder to the test DB appears on the port): migrations succeed on a later attempt.
/// Never reachable: the last error after the deadline.
#[tokio::test]
async fn startup_retries_the_db_until_it_comes_up_then_gives_up_after_the_deadline() {
    let db = TestDb::start().await;
    let target = url::Url::parse(&db.url).unwrap();
    let upstream = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(5432)
    );
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mut late = target.clone();
    late.set_host(Some("127.0.0.1")).unwrap();
    late.set_port(Some(port)).unwrap();
    let pool = strata_ingest::db::connect_lazy(late.as_str()).unwrap();
    let forwarder = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        loop {
            let (mut inbound, _) = l.accept().await.unwrap();
            let upstream = upstream.clone();
            tokio::spawn(async move {
                let mut out = tokio::net::TcpStream::connect(upstream).await.unwrap();
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut out).await;
            });
        }
    });
    let t0 = Instant::now();
    strata_ingest::db::migrate_with_retry(&pool, Duration::from_secs(60))
        .await
        .expect("migrated once the DB came up");
    assert!(t0.elapsed() >= Duration::from_secs(2), "{:?}", t0.elapsed());
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(one, 1);
    forwarder.abort();
    pool.close().await;

    let never = strata_ingest::db::connect_lazy("postgres://x:y@127.0.0.1:1/none").unwrap();
    let t0 = Instant::now();
    let r = strata_ingest::db::migrate_with_retry(&never, Duration::from_secs(3)).await;
    assert!(r.is_err());
    assert!(t0.elapsed() < Duration::from_secs(20), "{:?}", t0.elapsed());
}

/// A completed job with its staged original: what the delete tests remove.
async fn completed_job(e: &Env) -> (Uuid, String) {
    let id = e.text_job(TOKEN_A).await;
    set_status(e, id, "completed", &[]).await;
    let original = original_key(tenant_a(), id);
    assert!(e.storage.get(&original).is_some(), "the upload is staged");
    (id, original)
}

/// Moves a `deleting` job's `deleting_since` 10 minutes back: nobody has worked on it since.
async fn age_deleting(e: &Env, id: Uuid) {
    let n = sqlx::query(
        "UPDATE ingest_jobs SET deleting_since = now() - interval '10 minutes'
         WHERE id = $1 AND status = 'deleting'",
    )
    .bind(id)
    .execute(&e.db.pool)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(n, 1, "the job is deleting");
}

/// A slow storage_service: DELETE answers 202 `{job_id, status: "deleting"}` once
/// `delete_wait` has passed, and the delete still finishes on its own task. A request dropped
/// mid-delete (client gone) does not stop it either.
#[tokio::test]
async fn slow_delete_answers_202_and_finishes_without_the_request() {
    let e = env_opts(
        |_| {},
        ApiOpts {
            delete_wait: Duration::from_millis(200),
            ..ApiOpts::default()
        },
    )
    .await;
    e.storage.set_delete_delay(Duration::from_millis(1500));

    let (id, original) = completed_job(&e).await;
    let path = format!("/api/ingest/jobs/{id}");
    let (s, v) = e.call(reqwest::Method::DELETE, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v, json!({"job_id": id, "status": "deleting"}));
    let (s, v) = e.call(reqwest::Method::GET, &path, TOKEN_A).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "deleting", "{v}");
    eventually("the 202 delete to finish", || async {
        e.call(reqwest::Method::GET, &path, TOKEN_A).await.0 == StatusCode::NOT_FOUND
    })
    .await;
    assert!(e.storage.get(&original).is_none(), "its original is gone");

    // The caller goes away (client timeout, disconnect) while the blobs are being deleted:
    // the request's handler future is dropped once phase 1 has committed. The delete still
    // finishes (it runs on its own task).
    let (id, original) = completed_job(&e).await;
    let app = strata_ingest::api::router_with(e.ctx.clone(), ApiOpts::default());
    let req = axum::http::Request::builder()
        .method("DELETE")
        .uri(format!("/api/ingest/jobs/{id}"))
        .header("authorization", format!("Bearer {TOKEN_A}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let request = tokio::spawn(tower::ServiceExt::oneshot(app, req));
    eventually("phase 1 to mark the job", || async {
        job_status_of(&e, id).await.as_deref() == Some("deleting")
    })
    .await;
    request.abort();
    assert!(
        request.await.unwrap_err().is_cancelled(),
        "the handler was dropped before it answered"
    );
    eventually("the abandoned delete to finish", || async {
        job_status_of(&e, id).await.is_none()
    })
    .await;
    assert!(e.storage.get(&original).is_none(), "its original is gone");
    assert_eq!(e.job_count().await, 0);
}

/// The delete's task dies mid-way (the pod crashed): the job stays `deleting`. The sweeper
/// leaves a fresh one alone (a delete may still be working on it) and finishes a stale one.
#[tokio::test]
async fn sweeper_finishes_a_delete_whose_task_died() {
    use strata_ingest::db::jobs;
    let e = env().await;
    let (id, original) = completed_job(&e).await;
    let doc_key = format!("{}/{id}/figures/{}", tenant_a(), Uuid::now_v7());
    e.storage.put(&doc_key, &b"png"[..]);
    sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, bytes, mime)
         VALUES ($1, $2, 'figure', $3, 3, 'image/png')",
    )
    .bind(Uuid::now_v7())
    .bind(id)
    .bind(&doc_key)
    .execute(&e.db.pool)
    .await
    .unwrap();
    e.storage.set_delete_delay(Duration::from_secs(30));
    let task = tokio::spawn({
        let ctx = e.ctx.clone();
        async move { jobs::delete(&ctx.pool, &ctx.storage, tenant_a(), id).await }
    });
    eventually("phase 1 to mark the job", || async {
        job_status_of(&e, id).await.as_deref() == Some("deleting")
    })
    .await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    e.storage.set_delete_delay(Duration::ZERO);
    assert_eq!(job_status_of(&e, id).await.as_deref(), Some("deleting"));

    let swept = jobs::sweep_deleting(&e.db.pool, &e.ctx.storage, jobs::DELETING_STALE_AFTER)
        .await
        .unwrap();
    assert_eq!(swept, 0, "a fresh `deleting` job is left alone");
    assert_eq!(job_status_of(&e, id).await.as_deref(), Some("deleting"));

    age_deleting(&e, id).await;
    let swept = jobs::sweep_deleting(&e.db.pool, &e.ctx.storage, jobs::DELETING_STALE_AFTER)
        .await
        .unwrap();
    assert_eq!(swept, 1);
    assert_eq!(job_status_of(&e, id).await, None, "the sweeper finished it");
    assert!(e.storage.get(&original).is_none());
    assert!(e.storage.get(&doc_key).is_none());
    let steps_left: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_steps")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(steps_left, 0);
    let swept = jobs::sweep_deleting(&e.db.pool, &e.ctx.storage, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(swept, 0, "nothing left to sweep");

    // The periodic loop does the same on its own (first pass at start).
    let (id, original) = completed_job(&e).await;
    jobs::begin_delete(&e.db.pool, tenant_a(), id)
        .await
        .unwrap();
    age_deleting(&e, id).await;
    let stop = tokio_util::sync::CancellationToken::new();
    let sweeper = tokio::spawn(strata_ingest::worker::run_delete_sweeper(
        e.ctx.clone(),
        Duration::from_millis(100),
        jobs::DELETING_STALE_AFTER,
        stop.clone(),
    ));
    eventually("the sweeper loop to finish it", || async {
        job_status_of(&e, id).await.is_none()
    })
    .await;
    stop.cancel();
    sweeper.await.unwrap();
    assert!(e.storage.get(&original).is_none());
}

/// A sweeper pass and a DELETE of the same stale `deleting` job, at once: the job is deleted
/// exactly once (the other side finds it gone: sweeper counts 0 / DELETE `NotFound`), and
/// nothing is left.
#[tokio::test]
async fn sweeper_racing_delete_deletes_once() {
    use strata_ingest::db::jobs;
    let e = env().await;
    e.storage.set_delete_delay(Duration::from_millis(20));
    for round in 0..10 {
        let (id, original) = completed_job(&e).await;
        jobs::begin_delete(&e.db.pool, tenant_a(), id)
            .await
            .unwrap();
        age_deleting(&e, id).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let sweep = tokio::spawn({
            let (ctx, b) = (e.ctx.clone(), barrier.clone());
            async move {
                b.wait().await;
                jobs::sweep_deleting(&ctx.pool, &ctx.storage, Duration::from_secs(60)).await
            }
        });
        let delete = tokio::spawn({
            let (ctx, b) = (e.ctx.clone(), barrier.clone());
            async move {
                b.wait().await;
                jobs::delete(&ctx.pool, &ctx.storage, tenant_a(), id).await
            }
        });
        let swept = sweep.await.unwrap().unwrap();
        let deleted = delete.await.unwrap();
        match &deleted {
            Ok(d) => {
                assert_eq!(swept, 0, "round {round}: both deleted it");
                assert_eq!(d.status, "completed", "round {round}");
                assert_eq!(d.deleted_jobs, 1, "round {round}");
            }
            Err(strata_ingest::error::IngestError::NotFound) => {
                assert_eq!(swept, 1, "round {round}: nobody deleted it");
            }
            Err(err) => panic!("round {round}: delete: {err}"),
        }
        assert_eq!(job_status_of(&e, id).await, None, "round {round}");
        assert!(e.storage.get(&original).is_none(), "round {round}");
    }
    assert_eq!(e.job_count().await, 0);
}

/// Migrations 0006 / 0007 replace the job status CHECK whatever it is called: on a database
/// whose check drifted to another name (or was duplicated), re-applying them leaves exactly the
/// new check, so `deleting` is accepted. Other checks are untouched; re-running is a no-op.
#[tokio::test]
async fn status_check_migrations_tolerate_a_renamed_check() {
    let e = env().await;
    let pool = &e.db.pool;
    // Drift: the 0001 check under two other names, as an old environment might have it.
    sqlx::raw_sql(
        "ALTER TABLE ingest_jobs DROP CONSTRAINT ingest_jobs_status_check;
         ALTER TABLE ingest_jobs ADD CONSTRAINT jobs_status_drifted
           CHECK (status IN ('queued','running','completed','failed','rejected','cancelled'));
         ALTER TABLE ingest_jobs ADD CONSTRAINT ingest_jobs_status_check1
           CHECK (status <> 'deleting');
         ALTER TABLE ingest_jobs ADD CONSTRAINT ingest_jobs_kind_unrelated
           CHECK (tenant IS NOT NULL);",
    )
    .execute(pool)
    .await
    .unwrap();
    let id = e.text_job(TOKEN_A).await;
    let refused = sqlx::query("UPDATE ingest_jobs SET status = 'deleting' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
    assert!(refused.is_err(), "the drifted check rejects `deleting`");

    for sql in [
        include_str!("../migrations/0006_job_cancelling.sql"),
        include_str!("../migrations/0007_job_deleting.sql"),
        include_str!("../migrations/0007_job_deleting.sql"),
    ] {
        sqlx::raw_sql(sql).execute(pool).await.unwrap();
    }
    let checks: Vec<String> = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint
         WHERE conrelid = 'ingest_jobs'::regclass AND contype = 'c' ORDER BY conname",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(
        checks,
        vec!["ingest_jobs_kind_unrelated", "ingest_jobs_status_check"],
        "only the new status check (and the unrelated one) remain"
    );
    sqlx::query("UPDATE ingest_jobs SET status = 'deleting' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    let bogus = sqlx::query("UPDATE ingest_jobs SET status = 'bogus' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
    assert!(bogus.is_err(), "the new check is in force");
}
