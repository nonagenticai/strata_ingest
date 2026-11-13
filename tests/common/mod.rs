//! Shared integration-test harness: one Postgres 18 per test binary with a fresh, migrated
//! database per test, an in-process storage_service stand-in ([`FakeStorage`]) and a
//! shared docling-serve.
//! Each test binary uses a subset of these helpers.
#![allow(dead_code)]

use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

const IMAGE: &str = "postgres";
const TAG: &str = "18-alpine";

/// A fresh database with migrations applied, on the Postgres server shared by every test of this
/// binary. The queue claims across all jobs, so tests must not share a database: each `start`
/// creates a new one. A container per test meant dozens of concurrent containers per run, which
/// took down the shared Docker VM; now there is one server per test binary.
pub struct TestDb {
    pub pool: PgPool,
    pub url: String,
}

/// An OS-assigned free port. These are ephemeral (49152+), so never 8000. A port docker has
/// published is bound on the host, so the OS will not hand it out here either.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Starting a shared container (or connecting to it under load) is retried on a fresh container.
const START_ATTEMPTS: usize = 3;

/// The shared server: `TEST_DATABASE_URL` (a superuser URL; each test creates its own database)
/// when set, else one throwaway Postgres 18 container started on first use. The container lives
/// in a static (never dropped), so `remove_pg_container` (atexit) and a process watchdog remove it.
struct PgServer {
    /// Admin URL (`.../strata_ingest`) on the server.
    base: String,
    _container: Option<ContainerAsync<Postgres>>,
}

static PG_SERVER: tokio::sync::OnceCell<PgServer> = tokio::sync::OnceCell::const_new();
static PG_CONTAINER_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Removes the shared Postgres container (and its volume) when the test process exits.
extern "C" fn remove_pg_container() {
    if let Some(id) = PG_CONTAINER_ID.get() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", "-v", id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// Swaps the database name (path segment) in a postgres URL, keeping any query string.
fn with_db_name(url: &str, name: &str) -> String {
    let (head, query) = match url.split_once('?') {
        Some((h, q)) => (h, Some(q)),
        None => (url, None),
    };
    let scheme_end = head.find("://").map(|i| i + 3).unwrap_or(0);
    let base = match head[scheme_end..].find('/') {
        Some(i) => &head[..scheme_end + i],
        None => head,
    };
    match query {
        Some(q) => format!("{base}/{name}?{q}"),
        None => format!("{base}/{name}"),
    }
}

/// Connects to `url` and runs `SELECT 1`, retrying for up to `within`.
async fn wait_pg_ready(url: &str, within: std::time::Duration) -> Result<(), String> {
    let deadline = std::time::Instant::now() + within;
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        match PgPool::connect(url).await {
            Ok(pool) => {
                let r = sqlx::query("SELECT 1").execute(&pool).await;
                pool.close().await;
                match r {
                    Ok(_) => return Ok(()),
                    Err(e) => last = e.to_string(),
                }
            }
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    Err(format!(
        "postgres not ready within {} s: {last}",
        within.as_secs()
    ))
}

async fn start_pg_container() -> (String, ContainerAsync<Postgres>) {
    let mut failures = Vec::new();
    for attempt in 1..=START_ATTEMPTS {
        // A unique name, so a container docker created but could not start can be removed.
        let name = format!("strata-ingest-test-pg-{}", uuid::Uuid::now_v7());
        let started = Postgres::default()
            .with_db_name("strata_ingest")
            .with_user("strata_ingest")
            .with_password("strata_ingest")
            .with_name(IMAGE)
            .with_tag(TAG)
            // Every test of the binary (each with its own pools) shares this one server.
            .with_cmd(["postgres", "-c", "max_connections=500", "-c", "fsync=off"])
            .with_container_name(&name)
            .start()
            .await;
        let container = match started {
            Ok(c) => c,
            Err(e) => {
                eprintln!("postgres container failed (attempt {attempt}/{START_ATTEMPTS}): {e}");
                failures.push(format!("start: {e}"));
                remove_container(&name);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        if PG_CONTAINER_ID.set(container.id().to_string()).is_ok() {
            // SAFETY: registering a plain `extern "C" fn()` with atexit.
            unsafe { libc::atexit(remove_pg_container) };
        }
        // atexit does not run when the process is killed; the watchdog does.
        remove_when_this_process_exits(&name);
        // Docker can report the container started before the host port mapping is published
        // (`PortNotExposed` under load): poll for it instead of failing.
        let mut port = None;
        let mut last = String::new();
        for _ in 0..40 {
            match container.get_host_port_ipv4(5432).await {
                Ok(p) => {
                    port = Some(p);
                    break;
                }
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let Some(port) = port else {
            eprintln!("postgres port never published (attempt {attempt}/{START_ATTEMPTS}): {last}");
            failures.push(format!("port: {last}"));
            remove_container(&name);
            continue;
        };
        let base = format!("postgres://strata_ingest:strata_ingest@127.0.0.1:{port}/strata_ingest");
        match wait_pg_ready(&base, std::time::Duration::from_secs(60)).await {
            Ok(()) => return (base, container),
            Err(e) => {
                eprintln!("postgres on port {port} (attempt {attempt}/{START_ATTEMPTS}): {e}");
                failures.push(format!("port {port}: {e}"));
                remove_container(&name);
            }
        }
    }
    panic!(
        "start postgres container (is Docker running?) failed {START_ATTEMPTS} times: {failures:?}"
    );
}

async fn pg_server() -> &'static PgServer {
    PG_SERVER
        .get_or_init(|| async {
            if let Ok(base) = std::env::var("TEST_DATABASE_URL")
                && !base.trim().is_empty()
            {
                let base = base.trim().to_string();
                wait_pg_ready(&base, std::time::Duration::from_secs(60))
                    .await
                    .expect("TEST_DATABASE_URL is not reachable");
                return PgServer {
                    base,
                    _container: None,
                };
            }
            let (base, container) = start_pg_container().await;
            PgServer {
                base,
                _container: Some(container),
            }
        })
        .await
}

impl TestDb {
    pub async fn start() -> TestDb {
        let srv = pg_server().await;
        let mut failures = Vec::new();
        for attempt in 1..=START_ATTEMPTS {
            let n = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
            let name = format!(
                "si_t_{}_{n}_{}",
                std::process::id(),
                uuid::Uuid::now_v7().simple()
            );
            let created = async {
                let admin = PgPool::connect(&srv.base).await?;
                let r = sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                    .execute(&admin)
                    .await;
                admin.close().await;
                r.map(|_| ())
            }
            .await;
            if let Err(e) = created {
                eprintln!(
                    "create database {name} failed (attempt {attempt}/{START_ATTEMPTS}): {e}"
                );
                failures.push(format!("create {name}: {e}"));
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            let url = with_db_name(&srv.base, &name);
            // Migrations run per test database, exactly as the service runs them.
            match strata_ingest::db::connect(&url).await {
                Ok(pool) => return TestDb { pool, url },
                Err(e) => {
                    eprintln!(
                        "connect + migrate {name} failed (attempt {attempt}/{START_ATTEMPTS}): {e}"
                    );
                    failures.push(format!("{name}: connect: {e}"));
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        panic!("create test database + connect failed {START_ATTEMPTS} times: {failures:?}");
    }
}

/// Best effort: remove a container this harness named.
fn remove_container(name: &str) {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", "-v", name])
        .output();
}

// ---------------------------------------------------------------------------------------------
// storage_service

/// The bucket every test uses (strata-ingest's default).
pub const STORAGE_BUCKET: &str = "strata-ingest";
/// The EICAR test signature's marker: an upload containing it is infected whatever the verdict.
pub const EICAR: &[u8] = b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE";
/// The signature [`FakeStorage`] reports for an [`EICAR`] upload.
pub const EICAR_SIGNATURE: &str = "Eicar-Test-Signature";

/// What [`FakeStorage`] answers to an upload (an [`EICAR`] body is infected regardless).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// 200, `virus_scanned=true, scan_result="clean"`.
    Clean,
    /// 400 `Virus detected: <sig>`; nothing stored.
    Infected(String),
    /// 503 `Virus scanning is unavailable; upload refused`; nothing stored.
    Unavailable,
    /// 200, `virus_scanned=false, scan_result=null` (scanning disabled): stored, no verdict.
    Unscanned,
}

/// One request [`FakeStorage`] received.
#[derive(Debug, Clone)]
pub struct Captured {
    pub method: String,
    /// The path as sent (still percent-encoded).
    pub path: String,
    /// The `X-Tenant-Id` header, if any.
    pub auth: Option<String>,
    /// Upload: the form's `object_key`, `content_type`, the file part's length and whether the
    /// request completed (an aborted body never completes).
    pub object_key: Option<String>,
    pub content_type: Option<String>,
    pub size: Option<usize>,
}

#[derive(Default)]
struct FakeStorageState {
    objects: std::sync::Mutex<std::collections::BTreeMap<String, (bytes::Bytes, String)>>,
    verdict: std::sync::Mutex<Option<Verdict>>,
    requests: std::sync::Mutex<Vec<Captured>>,
    /// Held after the whole upload body arrived, before answering (a slow scan).
    upload_delay: std::sync::Mutex<std::time::Duration>,
    /// Uploads that have started and not yet answered (or been dropped).
    in_flight: std::sync::atomic::AtomicUsize,
    /// The most uploads ever in flight at once.
    max_in_flight: std::sync::atomic::AtomicUsize,
    /// When set, every GET of an object answers 500 (MinIO behind storage_service failing).
    reads_unavailable: std::sync::atomic::AtomicBool,
    /// When set, every DELETE answers 503 (storage_service unavailable).
    deletes_unavailable: std::sync::atomic::AtomicBool,
    /// Held before answering a DELETE (a slow storage_service).
    delete_delay: std::sync::Mutex<std::time::Duration>,
    /// After this many stored uploads, every upload answers 503.
    unavailable_after: std::sync::Mutex<Option<usize>>,
    stored_uploads: std::sync::atomic::AtomicUsize,
}

/// An in-process storage_service (contract: storage-service-contract.md; storage_service
/// `src/routes/files.py`) over memory: upload (multipart, verdicts), download, exists, metadata,
/// delete (404 when missing), `/health/ready`. Objects only land once the whole upload request was
/// read, as in the real service (`await file.read()` before anything is stored).
#[derive(Clone)]
pub struct FakeStorage {
    pub url: String,
    state: std::sync::Arc<FakeStorageState>,
}

impl FakeStorage {
    pub async fn start() -> FakeStorage {
        use axum::routing::{get, post};
        let state = std::sync::Arc::new(FakeStorageState::default());
        let app = axum::Router::new()
            .route("/api/files/upload", post(fs_upload))
            .route("/api/files/{bucket}/{*key}", get(fs_get).delete(fs_delete))
            .route("/health/ready", get(|| async { "{\"status\":\"ready\"}" }))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(state.clone());
        let url = strata_ingest::testutil::spawn(app).await;
        FakeStorage { url, state }
    }

    /// A client on the test bucket (no token).
    pub fn client(&self) -> strata_ingest::infra::storage::StorageClient {
        strata_ingest::infra::storage::StorageClient::new(&self.url, STORAGE_BUCKET, None)
            .expect("storage client")
    }

    /// Sets the verdict for every following upload.
    pub fn set_verdict(&self, v: Verdict) {
        *self.state.verdict.lock().unwrap() = Some(v);
    }

    pub fn set_upload_delay(&self, d: std::time::Duration) {
        *self.state.upload_delay.lock().unwrap() = d;
    }

    /// The next `n` uploads are stored as usual; every later one answers 503.
    pub fn set_unavailable_after(&self, n: usize) {
        *self.state.unavailable_after.lock().unwrap() = Some(n);
    }

    pub fn set_reads_unavailable(&self, on: bool) {
        self.state
            .reads_unavailable
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_deletes_unavailable(&self, on: bool) {
        self.state
            .deletes_unavailable
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_delete_delay(&self, d: std::time::Duration) {
        *self.state.delete_delay.lock().unwrap() = d;
    }

    /// Every stored key, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.state.objects.lock().unwrap().keys().cloned().collect()
    }

    pub fn get(&self, key: &str) -> Option<bytes::Bytes> {
        self.state
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(b, _)| b.clone())
    }

    /// Stores `body` at `key` directly (no request, no scan).
    pub fn put(&self, key: &str, body: impl Into<bytes::Bytes>) {
        self.state.objects.lock().unwrap().insert(
            key.to_string(),
            (body.into(), "application/octet-stream".to_string()),
        );
    }

    pub fn requests(&self) -> Vec<Captured> {
        self.state.requests.lock().unwrap().clone()
    }

    /// The uploads received (complete requests only; an aborted one is never captured).
    pub fn uploads(&self) -> Vec<Captured> {
        self.requests()
            .into_iter()
            .filter(|c| c.method == "POST")
            .collect()
    }

    /// The most uploads that were ever in flight at once.
    pub fn max_in_flight(&self) -> usize {
        self.state
            .max_in_flight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Uploads started and not yet answered.
    pub fn in_flight(&self) -> usize {
        self.state
            .in_flight
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

type FsState = axum::extract::State<std::sync::Arc<FakeStorageState>>;
type FsReply = (axum::http::StatusCode, axum::Json<serde_json::Value>);

fn fs_error(status: u16, message: &str) -> FsReply {
    (
        axum::http::StatusCode::from_u16(status).unwrap(),
        axum::Json(serde_json::json!({"error": {
            "code": format!("HTTP_{status}"), "message": message, "status": status,
            "request_id": "fake", "timestamp": "2026-10-16T00:00:00Z",
        }})),
    )
}

/// storage_service's `object_keys.py` rules.
fn fs_key_ok(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 1024
        && !key.starts_with('/')
        && !key.chars().any(|c| c.is_ascii_control())
        && !key.split('/').any(|s| s == "." || s == "..")
}

/// Decrements `in_flight` however the handler ends (answered or dropped).
struct InFlight(std::sync::Arc<FakeStorageState>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

async fn fs_upload(
    axum::extract::State(st): FsState,
    headers: axum::http::HeaderMap,
    mut mp: axum::extract::Multipart,
) -> FsReply {
    let now = st
        .in_flight
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        + 1;
    st.max_in_flight
        .fetch_max(now, std::sync::atomic::Ordering::SeqCst);
    let _guard = InFlight(st.clone());
    let (mut bucket, mut key, mut ct, mut file) = (None, None, None, None);
    loop {
        match mp.next_field().await {
            Ok(Some(f)) => {
                let name = f.name().unwrap_or_default().to_string();
                let part_ct = f.content_type().map(str::to_string);
                let Ok(body) = f.bytes().await else {
                    return fs_error(400, "multipart body");
                };
                match name.as_str() {
                    "bucket" => bucket = Some(String::from_utf8_lossy(&body).into_owned()),
                    "object_key" => key = Some(String::from_utf8_lossy(&body).into_owned()),
                    "content_type" => ct = Some(String::from_utf8_lossy(&body).into_owned()),
                    "file" => file = Some((body, part_ct)),
                    _ => {}
                }
            }
            Ok(None) => break,
            Err(_) => return fs_error(400, "multipart body"),
        }
    }
    let (Some(bucket), Some(key), Some((body, part_ct))) = (bucket, key, file) else {
        return fs_error(422, "missing form field");
    };
    st.requests.lock().unwrap().push(Captured {
        method: "POST".into(),
        path: "/api/files/upload".into(),
        auth: headers
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        object_key: Some(key.clone()),
        content_type: ct.clone(),
        size: Some(body.len()),
    });
    if bucket != STORAGE_BUCKET {
        return fs_error(400, &format!("Bucket '{bucket}' not found"));
    }
    if !fs_key_ok(&key) {
        return fs_error(400, "Invalid object key");
    }
    let delay = *st.upload_delay.lock().unwrap();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    let infected = body.windows(EICAR.len()).any(|w| w == EICAR);
    let exhausted = st
        .unavailable_after
        .lock()
        .unwrap()
        .is_some_and(|n| st.stored_uploads.load(std::sync::atomic::Ordering::SeqCst) >= n);
    let verdict = if exhausted {
        Verdict::Unavailable
    } else if infected {
        Verdict::Infected(EICAR_SIGNATURE.to_string())
    } else {
        st.verdict.lock().unwrap().clone().unwrap_or(Verdict::Clean)
    };
    let (scanned, result) = match verdict {
        Verdict::Infected(sig) => return fs_error(400, &format!("Virus detected: {sig}")),
        Verdict::Unavailable => {
            return fs_error(503, "Virus scanning is unavailable; upload refused");
        }
        Verdict::Clean => (true, Some("clean")),
        Verdict::Unscanned => (false, None),
    };
    let content_type = ct
        .or(part_ct)
        .unwrap_or_else(|| "application/octet-stream".into());
    let size = body.len();
    st.stored_uploads
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    st.objects
        .lock()
        .unwrap()
        .insert(key.clone(), (body, content_type.clone()));
    (
        axum::http::StatusCode::OK,
        axum::Json(serde_json::json!({
            "bucket": bucket, "object_key": key, "size": size, "etag": "fake",
            "content_type": content_type, "virus_scanned": scanned, "scan_result": result,
        })),
    )
}

fn fs_capture(
    st: &FakeStorageState,
    method: &str,
    uri: &axum::http::Uri,
    h: &axum::http::HeaderMap,
) {
    st.requests.lock().unwrap().push(Captured {
        method: method.into(),
        path: uri.path().to_string(),
        auth: h
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        object_key: None,
        content_type: None,
        size: None,
    });
}

async fn fs_get(
    axum::extract::State(st): FsState,
    axum::extract::Path((bucket, key)): axum::extract::Path<(String, String)>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    fs_capture(&st, "GET", &uri, &headers);
    if bucket != STORAGE_BUCKET {
        return fs_error(400, &format!("Bucket '{bucket}' not found")).into_response();
    }
    // Route order as in storage_service: `…/exists` and `…/metadata` before the catch-all.
    if let Some(k) = key.strip_suffix("/exists") {
        let exists = st.objects.lock().unwrap().contains_key(k);
        return axum::Json(
            serde_json::json!({"bucket": bucket, "object_key": k, "exists": exists}),
        )
        .into_response();
    }
    if let Some(k) = key.strip_suffix("/metadata") {
        return match st.objects.lock().unwrap().get(k) {
            Some((b, ct)) => axum::Json(serde_json::json!({
                "bucket": bucket, "object_key": k, "size": b.len(), "etag": "fake",
                "content_type": ct, "last_modified": "2026-10-16T00:00:00Z", "metadata": {},
            }))
            .into_response(),
            None => fs_error(404, "File not found").into_response(),
        };
    }
    if !fs_key_ok(&key) {
        return fs_error(400, "Invalid object key").into_response();
    }
    if st
        .reads_unavailable
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return fs_error(500, "Download failed: connection refused").into_response();
    }
    match st.objects.lock().unwrap().get(&key) {
        Some((b, ct)) => {
            ([(axum::http::header::CONTENT_TYPE, ct.clone())], b.clone()).into_response()
        }
        None => fs_error(404, "File not found").into_response(),
    }
}

async fn fs_delete(
    axum::extract::State(st): FsState,
    axum::extract::Path((bucket, key)): axum::extract::Path<(String, String)>,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    fs_capture(&st, "DELETE", &uri, &headers);
    if bucket != STORAGE_BUCKET {
        return fs_error(400, &format!("Bucket '{bucket}' not found")).into_response();
    }
    let delay = *st.delete_delay.lock().unwrap();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    if st
        .deletes_unavailable
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return fs_error(503, "Storage unavailable").into_response();
    }
    match st.objects.lock().unwrap().remove(&key) {
        Some(_) => axum::http::StatusCode::NO_CONTENT.into_response(),
        None => fs_error(404, "File not found").into_response(),
    }
}

// ---------------------------------------------------------------------------------------------
// docling-serve (CPU)

/// Rulings R9: the CPU image, started as `docling-serve run --host 0.0.0.0 --port P`
/// (`DOCLING_SERVE_PORT` is ignored). Multi-arch, several GB: pulled once.
const DOCLING_IMAGE: &str = "docling-project/docling-serve-cpu:v1.37.0";

static DOCLING: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();

/// The base URL of a docling-serve shared by every test of this binary: `DOCLING_URL` when set
/// (a CI service container), else one container started on first use. The container outlives
/// any one test's runtime, so it is not a testcontainers value: a detached watchdog removes it
/// once this test process has exited (also on a panic or abort).
pub async fn docling_url() -> String {
    DOCLING
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("DOCLING_URL")
                && !url.trim().is_empty()
            {
                let url = url.trim().trim_end_matches('/').to_string();
                wait_http_healthy(&url, std::time::Duration::from_secs(120))
                    .await
                    .expect("DOCLING_URL is not healthy");
                return url;
            }
            let mut failures = Vec::new();
            for attempt in 1..=START_ATTEMPTS {
                // Same port inside and outside: `--port P`, published as P.
                let port = free_port();
                let name = format!("strata-ingest-docling-{}", uuid::Uuid::now_v7());
                let out = std::process::Command::new("docker")
                    .args([
                        "run",
                        "-d",
                        "--rm",
                        "--name",
                        &name,
                        "-p",
                        &format!("127.0.0.1:{port}:{port}"),
                        DOCLING_IMAGE,
                        "docling-serve",
                        "run",
                        "--host",
                        "0.0.0.0",
                        "--port",
                        &port.to_string(),
                    ])
                    .output()
                    .expect("run docker");
                if !out.status.success() {
                    let e = String::from_utf8_lossy(&out.stderr).trim().to_string();
                    eprintln!(
                        "docling on port {port} failed (attempt {attempt}/{START_ATTEMPTS}): {e}"
                    );
                    failures.push(format!("port {port}: {e}"));
                    remove_container(&name);
                    continue;
                }
                remove_when_this_process_exits(&name);
                let url = format!("http://127.0.0.1:{port}");
                match wait_http_healthy(&url, std::time::Duration::from_secs(300)).await {
                    Ok(()) => return url,
                    Err(e) => {
                        failures.push(format!("port {port}: {e}"));
                        remove_container(&name);
                    }
                }
            }
            panic!(
                "start docling-serve (is Docker running?) failed {START_ATTEMPTS} times: {failures:?}"
            );
        })
        .await
        .clone()
}

/// A detached `sh` that polls this process and removes container `name` once it is gone. The
/// outer shell backgrounds the watcher and exits at once (waited for here), so the watcher is
/// re-parented and outlives this process without leaving a zombie.
fn remove_when_this_process_exits(name: &str) {
    let script = format!(
        "(while kill -0 {pid} 2>/dev/null; do sleep 2; done; docker rm -f {name} >/dev/null 2>&1) \
         </dev/null >/dev/null 2>&1 &",
        pid = std::process::id()
    );
    let status = std::process::Command::new("sh")
        .args(["-c", &script])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("spawn the container watchdog");
    assert!(status.success(), "container watchdog: {status}");
}

/// Polls `GET {url}/health` until 200.
async fn wait_http_healthy(url: &str, within: std::time::Duration) -> Result<(), String> {
    let http = reqwest::Client::new();
    let deadline = std::time::Instant::now() + within;
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        match http.get(format!("{url}/health")).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => last = format!("status {}", r.status()),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Err(format!(
        "{url} not healthy within {} s: {last}",
        within.as_secs()
    ))
}
