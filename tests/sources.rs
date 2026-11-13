//! Structured sources (`source.kind` = `api` | `datatable`) through `POST /api/ingest/jobs`:
//! the guarded fetch against an in-process source server (names pinned to it through
//! `FetchOpts::resolution`), records → one Deliver step each → a strata mock, on a real Postgres.
//! The credential must never reach a stored row, a response or an error.

mod common;

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::{FakeStorage, STORAGE_BUCKET, TestDb};
use strata_ingest::config::{Config, Mode, ProvidersFile, Region, Secret};
use strata_ingest::limits::Limits;
use strata_ingest::netguard::Resolution;
use strata_ingest::pipeline::Ctx;
use strata_ingest::sources::fetch::FetchOpts;
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{WorkerOpts, default_runner, run_workers_with};

const TOKEN: &str = "token-tenant-a";
const SECRET: &str = "s3cr3t-Value-That-Must-Never-Leak-9f2c";
const UNUSED_URL: &str = "http://127.0.0.1:9";
/// Pinned to the source server.
const SRC: &str = "src.example.test";
/// Pinned to a private address: anything reaching for it must be refused before connecting.
const EVIL: &str = "evil.example.test";
/// Another ALLOWED name for the same server: a different origin than [`SRC`].
const OTHER: &str = "other.example.test";

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-00000000000a").unwrap()
}

fn leaf() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000f1").unwrap()
}

// ---------------------------------------------------------------------------------------------
// The source server

#[derive(Default)]
struct SourceState {
    port: AtomicUsize,
    hits: std::sync::Mutex<HashMap<String, usize>>,
    /// The auth headers each request presented: (authorization, x-api-key).
    auth: std::sync::Mutex<Vec<(Option<String>, Option<String>)>>,
}

type Src = State<Arc<SourceState>>;

fn customers() -> Value {
    json!([
        {"customer_id": "C-1", "name": "Acme BV", "address": {"city": "Utrecht"}},
        {"customer_id": "C-2", "name": "Beta", "address": {"city": "Delft"}, "vip": true},
    ])
}

fn header(h: &HeaderMap, name: &str) -> Option<String> {
    h.get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// A table workbook: header row + two rows on sheet `Customers` (after a decoy first sheet).
fn workbook() -> Vec<u8> {
    let mut buf = std::io::Cursor::new(Vec::new());
    let mut z = zip::ZipWriter::new(&mut buf);
    let o = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let ns = r#"xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main""#;
    let files = [
        (
            "xl/workbook.xml".to_string(),
            format!(
                r#"<?xml version="1.0"?><workbook {ns} xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Notes" sheetId="1" r:id="rId1"/><sheet name="Customers" sheetId="2" r:id="rId2"/></sheets></workbook>"#
            ),
        ),
        (
            "xl/_rels/workbook.xml.rels".to_string(),
            r#"<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="/xl/worksheets/sheet2.xml"/></Relationships>"#.to_string(),
        ),
        (
            "xl/sharedStrings.xml".to_string(),
            format!(
                r#"<?xml version="1.0"?><sst {ns}><si><t>customer_id</t></si><si><t>name</t></si><si><t>Acme &amp; Zn</t></si></sst>"#
            ),
        ),
        (
            "xl/worksheets/sheet1.xml".to_string(),
            format!(r#"<?xml version="1.0"?><worksheet {ns}><sheetData><row r="1"><c r="A1"><v>1</v></c></row></sheetData></worksheet>"#),
        ),
        (
            "xl/worksheets/sheet2.xml".to_string(),
            format!(
                r#"<?xml version="1.0"?><worksheet {ns}><sheetData>
                <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
                <row r="2"><c r="A2" t="inlineStr"><is><t>C-1</t></is></c><c r="B2" t="s"><v>2</v></c></row>
                <row r="3"><c r="A3" t="inlineStr"><is><t>C-2</t></is></c><c r="B3"><v>42</v></c></row>
                </sheetData></worksheet>"#
            ),
        ),
    ];
    for (name, body) in files {
        z.start_file(name, o).unwrap();
        z.write_all(body.as_bytes()).unwrap();
    }
    z.finish().unwrap();
    buf.into_inner()
}

async fn source_handler(State(st): Src, Path(path): Path<String>, h: HeaderMap) -> Response {
    *st.hits.lock().unwrap().entry(path.clone()).or_default() += 1;
    let (authz, key) = (header(&h, "authorization"), header(&h, "x-api-key"));
    st.auth.lock().unwrap().push((authz.clone(), key.clone()));
    let port = st.port.load(Ordering::SeqCst);
    let redirect =
        |to: String| (StatusCode::FOUND, [(axum::http::header::LOCATION, to)]).into_response();
    match path.as_str() {
        "customers" | "echo-401" => {
            if key.as_deref() == Some(SECRET) && path == "customers" {
                axum::Json(customers()).into_response()
            } else {
                // A hostile or careless server that reflects the credential it was sent.
                (
                    StatusCode::UNAUTHORIZED,
                    format!("bad key {key:?} {authz:?}"),
                )
                    .into_response()
            }
        }
        "wrapped" => {
            if authz.as_deref() == Some(&format!("Bearer {SECRET}")) {
                axum::Json(json!({"total": 2, "data": customers(), "links": []})).into_response()
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }
        "customers.csv" => (
            [(axum::http::header::CONTENT_TYPE, "text/csv")],
            "\u{feff}customer_id;name;city\nC-1;Acme BV;Utrecht\nC-2;Beta;Delft\nC-1;Acme BV;Utrecht\n",
        )
            .into_response(),
        "book.xlsx" => workbook().into_response(),
        "empty" => axum::Json(json!([])).into_response(),
        "scalar" => axum::Json(json!(42)).into_response(),
        "big" => vec![b'['; 4096].into_response(),
        "slow" => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            axum::Json(customers()).into_response()
        }
        "redirect-same" => redirect("/customers".into()),
        "redirect-cross" => redirect(format!("http://{OTHER}:{port}/open")),
        "open" => axum::Json(customers()).into_response(),
        "redirect-evil" => redirect(format!("http://{EVIL}:{port}/customers")),
        "redirect-literal" => redirect(format!("http://127.0.0.1:{port}/customers")),
        "redirect-loop" => redirect("/redirect-loop".into()),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn source_server() -> (u16, Arc<SourceState>) {
    let st = Arc::new(SourceState::default());
    let app = Router::new()
        .route("/{*path}", get(source_handler))
        .with_state(st.clone());
    let url = spawn(app).await;
    let port: u16 = url.rsplit(':').next().unwrap().parse().unwrap();
    st.port.store(port as usize, Ordering::SeqCst);
    (port, st)
}

// ---------------------------------------------------------------------------------------------
// strata

/// Every document body strata received.
type Received = Arc<Mutex<Vec<Value>>>;

/// `GET /api/stores/{id}` (the leaf) and `POST /api/stores/{id}/documents`: identical text in
/// the store answers 200 `existing` with the first id, like strata's content-hash dedup.
async fn strata_mock() -> (String, Received) {
    let received: Received = Arc::default();
    let out = received.clone();
    let seen: Arc<Mutex<HashMap<String, Uuid>>> = Arc::default();
    let app = Router::new()
        .route(
            "/api/stores/{id}",
            get(|Path(id): Path<Uuid>| async move {
                if id == leaf() {
                    (
                        StatusCode::OK,
                        axum::Json(json!({"id": id, "kind": "leaf"})),
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
            "/api/stores/{id}/documents",
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let (received, seen) = (received.clone(), seen.clone());
                async move {
                    received.lock().await.push(body.clone());
                    let text = body["text"].as_str().unwrap_or_default().to_string();
                    let mut seen = seen.lock().await;
                    match seen.get(&text) {
                        Some(id) => (
                            StatusCode::OK,
                            axum::Json(json!({"status": "existing", "id": id})),
                        ),
                        None => {
                            let id = Uuid::now_v7();
                            seen.insert(text, id);
                            (
                                StatusCode::CREATED,
                                axum::Json(json!({"status": "created", "id": id})),
                            )
                        }
                    }
                }
            }),
        );
    (spawn(app).await, out)
}

// ---------------------------------------------------------------------------------------------
// Harness

struct Env {
    db: TestDb,
    _storage: FakeStorage,
    ctx: Arc<Ctx>,
    url: String,
    http: reqwest::Client,
    port: u16,
    src: Arc<SourceState>,
    received: Received,
}

async fn env() -> Env {
    env_with(|_| {}).await
}

async fn env_with(tweak: impl FnOnce(&mut FetchOpts)) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let ((strata, received), (port, src)) = tokio::join!(strata_mock(), source_server());
    let cfg = Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![(tenant(), Secret::new(TOKEN))],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: UNUSED_URL.to_string(),
        webscraper_url: UNUSED_URL.to_string(),
        webscraper_token: None,
        strata_url: strata,
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
    };
    let mut ctx = Ctx::with_pool(db.pool.clone(), cfg).unwrap();
    ctx.source_fetch = FetchOpts {
        timeout: Duration::from_secs(10),
        max_bytes: 1024 * 1024,
        resolution: Resolution {
            hosts: HashMap::from([
                (SRC.to_string(), vec!["127.0.0.1".parse().unwrap()]),
                (EVIL.to_string(), vec!["10.0.0.7".parse().unwrap()]),
                (OTHER.to_string(), vec!["127.0.0.1".parse().unwrap()]),
                // A public-looking name resolving to loopback on a port nobody allowed.
                (
                    "rebind.example.test".to_string(),
                    vec!["127.0.0.1".parse().unwrap()],
                ),
            ]),
            allow: vec![SocketAddr::from(([127, 0, 0, 1], port))],
        },
    };
    tweak(&mut ctx.source_fetch);
    let ctx = Arc::new(ctx);
    let url = spawn(strata_ingest::api::router(ctx.clone())).await;
    Env {
        db,
        _storage: storage,
        ctx,
        url,
        http: reqwest::Client::new(),
        port,
        src,
        received,
    }
}

impl Env {
    fn src(&self, path: &str) -> String {
        format!("http://{SRC}:{}/{path}", self.port)
    }

    async fn post(&self, body: &Value) -> (StatusCode, String) {
        let r = self
            .http
            .post(format!("{}/api/ingest/jobs", self.url))
            .bearer_auth(TOKEN)
            .json(body)
            .send()
            .await
            .unwrap();
        let s = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (s, r.text().await.unwrap())
    }

    async fn get(&self, path: &str) -> String {
        self.http
            .get(format!("{}{path}", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// Posts a body expected to be accepted; returns the job id and its reported status.
    async fn accepted(&self, body: &Value) -> (Uuid, String) {
        let (s, text) = self.post(body).await;
        assert_eq!(s, StatusCode::ACCEPTED, "{text}");
        assert!(!text.contains(SECRET), "secret in the 202: {text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["jobs"].as_array().unwrap().len(), 1, "{v}");
        let id = Uuid::parse_str(v["jobs"][0]["job_id"].as_str().unwrap()).unwrap();
        (id, v["jobs"][0]["status"].as_str().unwrap().to_string())
    }

    async fn job(&self, id: Uuid) -> Value {
        let text = self.get(&format!("/api/ingest/jobs/{id}")).await;
        assert!(!text.contains(SECRET), "secret in the job view: {text}");
        serde_json::from_str(&text).unwrap()
    }

    async fn run_to_end(&self, id: Uuid) -> Value {
        let shutdown = CancellationToken::new();
        let opts = WorkerOpts {
            poll: Duration::from_millis(50),
            heartbeat: Duration::from_secs(60),
            deadline: None,
            ..WorkerOpts::default()
        };
        let h = tokio::spawn(run_workers_with(
            self.ctx.clone(),
            2,
            shutdown.clone(),
            opts,
            default_runner(),
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let v = loop {
            let v = self.job(id).await;
            if matches!(v["status"].as_str(), Some("completed" | "failed")) {
                break v;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "job still {}",
                v["status"]
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(30), h)
            .await
            .expect("workers stop")
            .expect("worker pool");
        v
    }

    fn hits(&self, path: &str) -> usize {
        self.src
            .hits
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or(0)
    }

    /// Everything stored about every job (rows, steps, artefacts, assets), as text.
    async fn stored(&self) -> String {
        let mut out = String::new();
        for q in [
            "SELECT coalesce(json_agg(j)::text, '') FROM ingest_jobs j",
            "SELECT coalesce(json_agg(s)::text, '') FROM ingest_steps s",
            "SELECT coalesce(json_agg(a)::text, '') FROM ingest_artifacts a",
            "SELECT coalesce(json_agg(a)::text, '') FROM ingest_assets a",
        ] {
            let t: String = sqlx::query_scalar(q)
                .fetch_one(&self.db.pool)
                .await
                .unwrap();
            out.push_str(&t);
        }
        out
    }

    async fn assert_secret_nowhere(&self) {
        let stored = self.stored().await;
        assert!(!stored.is_empty());
        assert!(!stored.contains(SECRET), "secret stored: {stored}");
        let list = self.get("/api/ingest/jobs?limit=200").await;
        assert!(!list.contains(SECRET), "secret in the list: {list}");
        for body in self.received.lock().await.iter() {
            assert!(!body.to_string().contains(SECRET), "secret sent to strata");
        }
    }
}

fn api_body(e: &Env, path: &str, auth: Value, secret: Value) -> Value {
    json!({
        "store_id": leaf(),
        "source_id": "care-bus-src-1",
        "source_run": "run-a",
        "topic_keys": ["crm/customers"],
        "supersedes": ["crm/legacy"],
        "provenance": {"source_url": "https://crm.example.com/api", "license": "CC0-1.0",
                       "retrieved_at": "2026-10-21T08:00:00Z"},
        "source": {"kind": "api", "config": {"url": e.src(path), "auth": auth},
                   "credentials": secret},
        "mapping": [
            {"source_field": "customer_id", "entity": "Customer", "attribute": "customer_id"},
            {"source_field": "address.city", "entity": "Customer", "attribute": "city"},
        ],
        "options": {"language": "nl"},
    })
}

fn key_auth() -> Value {
    json!({"type": "header", "header_name": "X-Api-Key"})
}

fn secret() -> Value {
    json!({"secret": SECRET})
}

fn doc_ids(v: &Value) -> Vec<String> {
    v["document_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d.as_str().unwrap().to_string())
        .collect()
}

// ---------------------------------------------------------------------------------------------
// api

#[tokio::test]
async fn api_source_delivers_one_document_per_item() {
    let e = env().await;
    let (id, status) = e
        .accepted(&api_body(&e, "customers", key_auth(), secret()))
        .await;
    assert_eq!(status, "queued");
    // The credential was presented as `X-Api-Key: <secret>` and only so.
    assert_eq!(
        e.src.auth.lock().unwrap().clone(),
        vec![(None, Some(SECRET.to_string()))]
    );
    let job = e.job(id).await;
    assert_eq!(job["meta"]["source_id"], "care-bus-src-1");
    assert_eq!(job["meta"]["source_run"], "run-a");
    assert_eq!(job["meta"]["topic_keys"], json!(["crm/customers"]));
    assert_eq!(job["meta"]["records"], 2);
    assert_eq!(job["source"]["kind"], "api");
    assert_eq!(
        job["source"]["auth"],
        json!({"type": "header", "header_name": "X-Api-Key"})
    );
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(doc_ids(&done).len(), 2);

    let received = e.received.lock().await.clone();
    assert_eq!(received.len(), 2);
    let first = received
        .iter()
        .find(|b| b["text"].as_str().unwrap().contains("C-1"))
        .unwrap();
    assert_eq!(
        first["text"],
        "Customer.customer_id: C-1\nCustomer.city: Utrecht\n\n\
         address.city: Utrecht\ncustomer_id: C-1\nname: Acme BV"
    );
    assert_eq!(first["title"], "Customer C-1");
    assert_eq!(first["source_id"], "care-bus-src-1");
    // Every item carries the sync run (strata's `prune?except_run=`).
    // Every item carries the sync run (strata's `prune?except_run=`) and the source's
    // platform-knowledge fields.
    for body in &received {
        assert_eq!(body["source_run"], "run-a", "{body}");
        assert_eq!(body["topic_keys"], json!(["crm/customers"]), "{body}");
        assert_eq!(body["supersedes"], json!(["crm/legacy"]), "{body}");
        assert_eq!(
            body["provenance"],
            json!({"source_url": "https://crm.example.com/api", "license": "CC0-1.0",
                   "retrieved_at": "2026-10-21T08:00:00Z"}),
            "{body}"
        );
    }
    assert_eq!(
        first["metadata"],
        json!({"source_id": "care-bus-src-1",
               "attributes": {"Customer.customer_id": "C-1", "Customer.city": "Utrecht"}})
    );
    assert!(
        first["source"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{}#item-", e.src("customers"))),
        "{first}"
    );
    e.assert_secret_nowhere().await;
}

#[tokio::test]
async fn api_object_holding_an_array_with_bearer_auth() {
    let e = env().await;
    let body = api_body(&e, "wrapped", json!({"type": "bearer"}), secret());
    let (id, _) = e.accepted(&body).await;
    assert_eq!(
        e.src.auth.lock().unwrap()[0],
        (Some(format!("Bearer {SECRET}")), None)
    );
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(doc_ids(&done).len(), 2);
    e.assert_secret_nowhere().await;
}

#[tokio::test]
async fn upstream_401_that_echoes_the_secret_fails_the_job_without_it() {
    let e = env().await;
    let (id, status) = e
        .accepted(&api_body(&e, "echo-401", key_auth(), secret()))
        .await;
    assert_eq!(status, "failed");
    let job = e.job(id).await;
    assert_eq!(job["status"], "failed");
    assert_eq!(job["error"]["code"], "source_auth", "{job}");
    assert_eq!(job["error"]["message"], "the source answered HTTP 401");
    assert_eq!(job["steps"], json!([]));
    assert_eq!(job["meta"]["source_id"], "care-bus-src-1");
    // Nothing to retry: the credential is gone.
    let r = e
        .http
        .post(format!("{}/api/ingest/jobs/{id}/retry", e.url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 409);
    e.assert_secret_nowhere().await;
}

#[tokio::test]
async fn redirects_same_origin_followed_others_guarded() {
    let e = env().await;
    // Same origin: followed, credential still sent.
    let (id, status) = e
        .accepted(&api_body(&e, "redirect-same", key_auth(), secret()))
        .await;
    assert_eq!(status, "queued");
    assert_eq!(e.hits("customers"), 1);
    assert_eq!(e.job(id).await["meta"]["records"], 2);
    // To a name resolving to a private address, or to a literal loopback IP: refused, the
    // target never contacted.
    for path in ["redirect-evil", "redirect-literal"] {
        let (id, status) = e.accepted(&api_body(&e, path, key_auth(), secret())).await;
        assert_eq!(status, "failed", "{path}");
        assert_eq!(e.job(id).await["error"]["code"], "fetch_blocked", "{path}");
    }
    assert_eq!(
        e.hits("customers"),
        1,
        "no blocked redirect reached its target"
    );
    let (id, _) = e
        .accepted(&api_body(&e, "redirect-loop", key_auth(), secret()))
        .await;
    assert_eq!(e.job(id).await["error"]["code"], "too_many_redirects");
    e.assert_secret_nowhere().await;
}

#[tokio::test]
async fn the_credential_never_follows_a_redirect_to_another_allowed_origin() {
    let e = env().await;
    let (id, status) = e
        .accepted(&api_body(&e, "redirect-cross", key_auth(), secret()))
        .await;
    assert_eq!(status, "queued", "the allowed target is fetched");
    assert_eq!(e.hits("open"), 1);
    assert_eq!(e.job(id).await["meta"]["records"], 2);
    let auth = e.src.auth.lock().unwrap().clone();
    assert_eq!(
        auth,
        vec![(None, Some(SECRET.to_string())), (None, None)],
        "key sent to the source's origin only"
    );
    let (id, _) = e
        .accepted(&api_body(
            &e,
            "redirect-cross",
            json!({"type": "bearer"}),
            secret(),
        ))
        .await;
    assert_eq!(e.job(id).await["status"], "queued");
    assert_eq!(
        e.src.auth.lock().unwrap()[3],
        (None, None),
        "no bearer either"
    );
}

#[tokio::test]
async fn a_query_in_the_source_url_is_not_persisted_or_echoed() {
    let e = env().await;
    let mut body = api_body(&e, "customers", key_auth(), secret());
    body["source"]["config"]["url"] = json!(format!(
        "{}?api_key=QUERY-TOKEN-77&page=1#frag-x",
        e.src("customers")
    ));
    let (id, _) = e.accepted(&body).await;
    let job = e.job(id).await;
    assert_eq!(job["source"]["url"], e.src("customers"), "{job}");
    // The fetch used the full URL (the query reached the source).
    assert_eq!(e.hits("customers"), 1);
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    let stored = e.stored().await;
    let list = e.get("/api/ingest/jobs").await;
    let sent = format!("{:?}", e.received.lock().await);
    for (what, text) in [("stored", &stored), ("list", &list), ("strata", &sent)] {
        assert!(!text.contains("QUERY-TOKEN-77"), "query in {what}");
        assert!(!text.contains("frag-x"), "fragment in {what}");
    }
}

#[tokio::test]
async fn private_hosts_are_refused_before_and_after_dns() {
    let e = env().await;
    // Literal private / internal hosts: 400 at the door, like `url` jobs; nothing stored.
    for url in [
        format!("http://127.0.0.1:{}/customers", e.port),
        "http://169.254.169.254/latest/meta-data".to_string(),
        "http://storage.example.internal:8389/x".to_string(),
        "http://localhost/x".to_string(),
    ] {
        let mut body = api_body(&e, "customers", key_auth(), secret());
        body["source"]["config"]["url"] = json!(url);
        let (s, text) = e.post(&body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{url}: {text}");
        assert!(!text.contains(SECRET));
    }
    assert_eq!(e.stored().await, "");
    // Public-looking names that resolve to a private address, or to loopback on a port nobody
    // allowed: refused after resolution, nothing contacted.
    for host in [EVIL, "rebind.example.test"] {
        let mut body = api_body(&e, "customers", key_auth(), secret());
        body["source"]["config"]["url"] = json!(format!("http://{host}:{}/customers", e.port + 1));
        let (id, status) = e.accepted(&body).await;
        assert_eq!(status, "failed", "{host}");
        assert_eq!(e.job(id).await["error"]["code"], "fetch_blocked", "{host}");
    }
    assert!(e.src.hits.lock().unwrap().is_empty());
    e.assert_secret_nowhere().await;
}

#[tokio::test]
async fn size_time_shape_and_empty_bounds() {
    let e = env_with(|o| {
        o.max_bytes = 1024;
        o.timeout = Duration::from_secs(1);
    })
    .await;
    for (path, code) in [
        ("big", "too_large"),
        ("slow", "fetch_timeout"),
        ("scalar", "unrecognised_shape"),
        ("empty", "no_records"),
        ("nope", "source_rejected"),
    ] {
        let (id, status) = e.accepted(&api_body(&e, path, key_auth(), secret())).await;
        assert_eq!(status, "failed", "{path}");
        let job = e.job(id).await;
        assert_eq!(job["error"]["code"], code, "{path}: {job}");
        assert_eq!(job["document_ids"], json!([]));
    }
    e.assert_secret_nowhere().await;
}

// ---------------------------------------------------------------------------------------------
// datatable

fn table_body(url: String, format: Option<&str>, sheet: Option<&str>) -> Value {
    let mut config = json!({"url": url});
    if let Some(f) = format {
        config["format"] = json!(f);
    }
    if let Some(s) = sheet {
        config["sheet"] = json!(s);
    }
    json!({
        "store_id": leaf(),
        "source_id": "care-bus-src-2",
        "source_run": "run-t",
        "source": {"kind": "datatable", "config": config, "credentials": null},
        "mapping": [{"source_field": "customer_id", "entity": "Customer", "attribute": "id"}],
    })
}

#[tokio::test]
async fn csv_rows_become_documents_and_dedup_hits_are_listed() {
    let e = env().await;
    let (id, _) = e
        .accepted(&table_body(e.src("customers.csv"), Some("csv"), None))
        .await;
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    // Three rows, the third identical to the first: strata answers `existing` for it, and its
    // id is still in the job's list (once).
    let received = e.received.lock().await.clone();
    assert_eq!(received.len(), 3);
    assert_eq!(doc_ids(&done).len(), 2);
    let deliver: Vec<&Value> = done["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "deliver")
        .collect();
    assert_eq!(deliver.len(), 3);
    let dup = deliver
        .iter()
        .find(|s| s["output"]["duplicate"] == true)
        .expect("one duplicate");
    assert!(doc_ids(&done).contains(&dup["output"]["document_id"].as_str().unwrap().to_string()));
    let row1 = received
        .iter()
        .find(|b| b["source"].as_str().unwrap().ends_with("#row-1"))
        .unwrap();
    assert_eq!(
        row1["text"],
        "Customer.id: C-1\n\ncustomer_id: C-1\nname: Acme BV\ncity: Utrecht"
    );
    assert_eq!(row1["source_id"], "care-bus-src-2");
    for body in &received {
        assert_eq!(body["source_run"], "run-t", "{body}");
    }
}

#[tokio::test]
async fn xlsx_named_sheet_and_format_from_the_url() {
    let e = env().await;
    let (id, _) = e
        .accepted(&table_body(e.src("book.xlsx"), None, Some("Customers")))
        .await;
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    let mut texts: Vec<String> = e
        .received
        .lock()
        .await
        .iter()
        .map(|b| b["text"].as_str().unwrap().to_string())
        .collect();
    texts.sort();
    assert_eq!(
        texts,
        [
            "Customer.id: C-1\n\ncustomer_id: C-1\nname: Acme & Zn",
            "Customer.id: C-2\n\ncustomer_id: C-2\nname: 42",
        ]
    );
    let (id, status) = e
        .accepted(&table_body(
            e.src("book.xlsx"),
            Some("xlsx"),
            Some("Missing"),
        ))
        .await;
    assert_eq!(status, "failed");
    let job = e.job(id).await;
    assert_eq!(job["error"]["code"], "sheet_not_found");
    assert!(
        job["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Customers")
    );
}

#[tokio::test]
async fn csv_without_a_header_row_names_columns_by_position() {
    let e = env().await;
    let mut body = table_body(e.src("customers.csv"), Some("csv"), None);
    body["source"]["config"]["header_row"] = json!(false);
    body["mapping"] =
        json!([{"source_field": "column_1", "entity": "Customer", "attribute": "id"}]);
    let (id, _) = e.accepted(&body).await;
    let done = e.run_to_end(id).await;
    assert_eq!(done["status"], "completed", "{done}");
    let received = e.received.lock().await.clone();
    // The header line is now a record of its own: four rows, not three.
    assert_eq!(received.len(), 4, "{received:?}");
    let row1 = received
        .iter()
        .find(|b| b["source"].as_str().unwrap().ends_with("#row-1"))
        .unwrap();
    assert_eq!(
        row1["text"],
        "Customer.id: customer_id\n\ncolumn_1: customer_id\ncolumn_2: name\ncolumn_3: city"
    );
    let job = e.job(id).await;
    assert_eq!(job["source"]["header_row"], false, "{job}");
}

#[tokio::test]
async fn header_row_on_an_api_source_is_400() {
    let e = env().await;
    for v in [json!(true), json!(false)] {
        let mut body = api_body(&e, "customers", key_auth(), secret());
        body["source"]["config"]["header_row"] = v.clone();
        let (s, text) = e.post(&body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{v}: {text}");
        assert!(text.contains("header_row"), "{text}");
        assert!(!text.contains(SECRET));
    }
    assert_eq!(e.stored().await, "");
    assert!(e.src.hits.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------------------------
// Request validation and the secret in errors

#[tokio::test]
async fn unknown_kind_is_422_unsupported_source_kind() {
    let e = env().await;
    for kind in [json!("sftp"), json!("url"), json!(null)] {
        let mut body = api_body(&e, "customers", key_auth(), secret());
        body["source"]["kind"] = kind.clone();
        let (s, text) = e.post(&body).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{kind}: {text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["error"]["code"], "unsupported_source_kind", "{v}");
        assert!(v["error"]["message"].is_string());
        assert!(!text.contains(SECRET));
    }
    assert_eq!(e.stored().await, "");
    assert!(e.src.hits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bad_requests_never_echo_the_secret() {
    let e = env().await;
    let base = || api_body(&e, "customers", key_auth(), secret());
    let mut cases: Vec<Value> = Vec::new();
    // Malformed credentials (the secret inside them).
    for creds in [
        json!(SECRET),
        json!([SECRET]),
        json!({"secret": SECRET, "user": "x"}),
        json!({"token": SECRET}),
        json!({"secret": 7}),
        json!({"secret": format!("{SECRET}\n")}),
    ] {
        let mut b = base();
        b["source"]["credentials"] = creds;
        cases.push(b);
    }
    // Shape errors elsewhere, with the secret present in credentials.
    let edits: Vec<fn(&mut Value)> = vec![
        |b| b["extra"] = json!(1),
        |b| b["source"]["config"]["method"] = json!("POST"),
        |b| b["source"]["config"]["format"] = json!("csv"),
        |b| b["source"]["config"]["auth"] = json!({"type": "basic"}),
        |b| b["source"]["config"]["auth"] = json!({"type": "header", "header_name": "Host"}),
        |b| b["source"]["config"]["auth"] = json!({"type": "header"}),
        |b| b["source"]["config"]["url"] = json!(7),
        |b| b["mapping"] = json!([{"source_field": "", "entity": "E", "attribute": "a"}]),
        |b| b["mapping"] = json!([{"source_field": "x", "entity": "E"}]),
        |b| b["source_id"] = json!(""),
        |b| b["source_run"] = json!(""),
        |b| b["source_run"] = json!("r\u{7}"),
        |b| {
            b.as_object_mut().unwrap().remove("source_id");
        },
        |b| b["options"] = json!({"ocr": "sometimes"}),
        |b| b["topic_keys"] = json!(["Not A Key"]),
        |b| b["supersedes"] = json!("crm/legacy"),
        |b| b["provenance"]["retrieved_at"] = json!("21 Oct 2026"),
        |b| b["provenance"]["extra"] = json!(1),
        |b| b["source"] = json!("api"),
    ];
    for edit in &edits {
        let mut b = base();
        edit(&mut b);
        cases.push(b);
    }
    // bearer/header without a secret.
    let mut b = base();
    b["source"]["credentials"] = Value::Null;
    cases.push(b);
    for body in cases {
        let (s, text) = e.post(&body).await;
        assert!(s.is_client_error(), "{s} for {body}: {text}");
        assert!(!text.contains(SECRET), "secret echoed for {body}: {text}");
        let v: Value = serde_json::from_str(&text).unwrap();
        assert!(v["error"]["code"].is_string(), "{v}");
    }
    assert_eq!(e.stored().await, "", "no job for a refused request");
    assert!(e.src.hits.lock().unwrap().is_empty(), "nothing fetched");
    // An unknown store is refused before the fetch, too.
    let mut b = base();
    b["store_id"] = json!(Uuid::now_v7());
    let (s, text) = e.post(&b).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{text}");
    assert!(e.src.hits.lock().unwrap().is_empty());
}
