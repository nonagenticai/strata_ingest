//! Discovery sources (`source.kind` = `crawl` | `sitemap`) through `POST /api/ingest/jobs`: the
//! parent's Discover step against a fake webscraper (crawl) or an in-process sitemap server
//! (names pinned to it through `FetchOpts::resolution`), its url children through the real
//! pipeline to a strata mock, on a real Postgres.

mod common;

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use sqlx::Row;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use common::{FakeStorage, STORAGE_BUCKET, TestDb};
use strata_ingest::config::{Config, Mode, ProvidersFile, Region, Secret};
use strata_ingest::db::steps;
use strata_ingest::limits::{Limits, SITEMAP_MAX_CHILD_SITEMAPS};
use strata_ingest::netguard::Resolution;
use strata_ingest::pipeline::{Ctx, discover};
use strata_ingest::sources::fetch::FetchOpts;
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{WorkerOpts, default_runner, run_workers_with};

const TOKEN: &str = "token-tenant-crawl";
const UNUSED_URL: &str = "http://127.0.0.1:9";
/// Pinned to the sitemap server.
const MAPS: &str = "maps.example.test";
const SITE: &str = "https://site.example.com";

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000c1").unwrap()
}

fn leaf() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-0000000000c2").unwrap()
}

fn page(path: &str) -> String {
    format!("{SITE}{path}")
}

/// A public address the crawled test hosts resolve to (never connected to: webscraper is faked).
const PUBLIC_IP: &str = "93.184.215.14";

/// Crawl hosts pinned in the test resolution: the site (public) and subdomains of it that
/// resolve to loopback, the cloud metadata address and a private address.
fn crawl_hosts() -> Vec<(&'static str, &'static str)> {
    vec![
        ("site.example.com", PUBLIC_IP),
        ("docs.site.example.com", PUBLIC_IP),
        ("evil.site.example.com", "127.0.0.1"),
        ("meta.site.example.com", "169.254.169.254"),
        ("ten.site.example.com", "10.0.0.7"),
    ]
}

// ---------------------------------------------------------------------------------------------
// The fake webscraper: pages by URL, a hit counter, URLs that answer 503

#[derive(Default)]
struct Scraper {
    /// url → `links` of the page (absolute, as webscraper returns them).
    links: HashMap<String, Vec<String>>,
    /// URLs answered 503.
    down: HashSet<String>,
    /// URLs answered 503 on their first n requests.
    fail_first: HashMap<String, usize>,
    hits: std::sync::Mutex<HashMap<String, usize>>,
    bodies: std::sync::Mutex<Vec<Value>>,
}

async fn scrape(State(s): State<Arc<Scraper>>, axum::Json(body): axum::Json<Value>) -> Response {
    let url = body["url"].as_str().unwrap_or_default().to_string();
    let hit = {
        let mut hits = s.hits.lock().unwrap();
        let n = hits.entry(url.clone()).or_default();
        *n += 1;
        *n
    };
    s.bodies.lock().unwrap().push(body);
    if s.down.contains(&url) || s.fail_first.get(&url).is_some_and(|n| hit <= *n) {
        return (StatusCode::SERVICE_UNAVAILABLE, "busy").into_response();
    }
    let links: Vec<Value> = s
        .links
        .get(&url)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|l| json!({"text": "link", "url": l, "rel": null, "type": "internal", "score": 0.5}))
        .collect();
    axum::Json(json!({
        "url": url, "final_url": url, "title": format!("Title of {url}"),
        "markdown_content": format!("# Page\n\nThe text of {url}.\n"),
        "status": "completed", "links": links, "images": [],
    }))
    .into_response()
}

// ---------------------------------------------------------------------------------------------
// The sitemap server

fn urlset(locs: &[String]) -> String {
    let urls: String = locs
        .iter()
        .map(|l| format!("<url><loc>{l}</loc><lastmod>2026-10-01</lastmod></url>"))
        .collect();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>"#
    )
}

fn index(port: u16, paths: &[&str]) -> String {
    let maps: String = paths
        .iter()
        .map(|p| format!("<sitemap><loc>http://{MAPS}:{port}/{p}</loc></sitemap>"))
        .collect();
    format!(
        r#"<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{maps}</sitemapindex>"#
    )
}

fn gzip(s: &str) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(s.as_bytes()).unwrap();
    gz.finish().unwrap()
}

/// The sitemap server's state: its own port (index entries name it) and requests per path.
#[derive(Default)]
struct Maps {
    port: std::sync::atomic::AtomicU16,
    hits: std::sync::Mutex<HashMap<String, usize>>,
}

/// Child sitemaps `many.xml` names: far more than `SITEMAP_MAX_CHILD_SITEMAPS`.
const MANY: usize = 120;

async fn sitemap_handler(State(maps): State<Arc<Maps>>, Path(path): Path<String>) -> Response {
    let port = maps.port.load(std::sync::atomic::Ordering::SeqCst);
    *maps.hits.lock().unwrap().entry(path.clone()).or_default() += 1;
    let xml =
        |s: String| ([(axum::http::header::CONTENT_TYPE, "application/xml")], s).into_response();
    match path.as_str() {
        "sitemap.xml" => xml(urlset(&[
            page("/docs/a"),
            page("/docs/b"),
            page("/old/docs/c"),
            page("/blog/d"),
            page("/docs/a#again"),
        ])),
        "index.xml" => xml(index(port, &["s1.xml.gz", "nested.xml", "missing.xml"])),
        "s1.xml.gz" => gzip(&urlset(&[page("/g/1"), page("/g/2")])).into_response(),
        "nested.xml" => xml(index(port, &["s2.xml"])),
        "s2.xml" => xml(urlset(&[page("/n/1"), page("/n/2")])),
        "empty.xml" => xml(urlset(&[])),
        "many.xml" => {
            let paths: Vec<String> = (0..MANY).map(|i| format!("m/{i}.xml")).collect();
            let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
            xml(index(port, &paths))
        }
        // Every child sitemap lists the same page: one child job, however many are fetched.
        p if p.starts_with("m/") => xml(urlset(&[page("/m/same")])),
        "big.xml" => xml(urlset(
            &(0..2000)
                .map(|i| page(&format!("/big/{i}")))
                .collect::<Vec<_>>(),
        )),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------------------------------------------------------------------------------------------
// strata

type Received = Arc<std::sync::Mutex<Vec<Value>>>;

async fn strata_mock() -> (String, Received) {
    let received: Received = Arc::default();
    let out = received.clone();
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
            post(move |axum::Json(body): axum::Json<Value>| {
                let received = received.clone();
                async move {
                    // strata's platform-store licence check (strata_rag `check_platform_license`).
                    let refused = body["provenance"]["license"] == "all-rights-reserved";
                    received.lock().unwrap().push(body);
                    if refused {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            axum::Json(json!({"error": {"code": "invalid_input", "message":
                                "licence `all-rights-reserved` is not allowed for platform \
                                 knowledge; allowed (STRATA_PLATFORM_LICENSES): CC-BY-4.0"}})),
                        );
                    }
                    (
                        StatusCode::CREATED,
                        axum::Json(json!({"status": "created", "id": Uuid::now_v7()})),
                    )
                }
            }),
        );
    (spawn(app).await, out)
}

// ---------------------------------------------------------------------------------------------
// Harness

struct Env {
    db: TestDb,
    storage: FakeStorage,
    ctx: Arc<Ctx>,
    url: String,
    http: reqwest::Client,
    scraper: Arc<Scraper>,
    maps: Arc<Maps>,
    maps_port: u16,
    received: Received,
}

async fn env(scraper: Scraper, max_attempts: i32) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let scraper = Arc::new(scraper);
    let scraper_url = spawn(
        Router::new()
            .route("/api/scrape/url", post(scrape))
            .with_state(scraper.clone()),
    )
    .await;
    let maps = Arc::new(Maps::default());
    let maps_url = spawn(
        Router::new()
            .route("/{*path}", get(sitemap_handler))
            .with_state(maps.clone()),
    )
    .await;
    let maps_port: u16 = maps_url.rsplit(':').next().unwrap().parse().unwrap();
    maps.port
        .store(maps_port, std::sync::atomic::Ordering::SeqCst);
    let (strata, received) = strata_mock().await;
    let cfg = Config {
        database_url: db.url.clone(),
        bind: "127.0.0.1:0".parse().unwrap(),
        licence: None,
        tokens: vec![(tenant(), Secret::new(TOKEN))],
        storage_url: storage.url.clone(),
        storage_bucket: STORAGE_BUCKET.to_string(),
        storage_token: None,
        docling_url: UNUSED_URL.to_string(),
        webscraper_url: scraper_url,
        webscraper_token: None,
        strata_url: strata,
        strata_token: Secret::new("strata-token"),
        workers: 2,
        limits: Limits {
            max_attempts,
            ..Limits::default()
        },
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
            hosts: crawl_hosts()
                .into_iter()
                .map(|(h, ip)| (h.to_string(), vec![ip.parse::<IpAddr>().unwrap()]))
                .chain([(MAPS.to_string(), vec![IpAddr::from([127, 0, 0, 1])])])
                .collect(),
            allow: vec![SocketAddr::from(([127, 0, 0, 1], maps_port))],
        },
    };
    let ctx = Arc::new(ctx);
    let url = spawn(strata_ingest::api::router(ctx.clone())).await;
    Env {
        db,
        storage,
        ctx,
        url,
        http: reqwest::Client::new(),
        scraper,
        maps,
        maps_port,
        received,
    }
}

impl Env {
    fn map(&self, path: &str) -> String {
        format!("http://{MAPS}:{}/{path}", self.maps_port)
    }

    async fn post(&self, body: &Value) -> (StatusCode, Value) {
        let r = self
            .http
            .post(format!("{}/api/ingest/jobs", self.url))
            .bearer_auth(TOKEN)
            .json(body)
            .send()
            .await
            .unwrap();
        let s = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (s, r.json().await.unwrap())
    }

    /// Posts a discovery body expected to be accepted; returns the parent job id.
    async fn accepted(&self, body: &Value) -> Uuid {
        let (s, v) = self.post(body).await;
        assert_eq!(s, StatusCode::ACCEPTED, "{v}");
        assert_eq!(v["jobs"].as_array().unwrap().len(), 1, "{v}");
        assert_eq!(v["jobs"][0]["status"], "queued", "{v}");
        assert_eq!(v["job_id"], v["jobs"][0]["job_id"], "{v}");
        Uuid::parse_str(v["job_id"].as_str().unwrap()).unwrap()
    }

    async fn job(&self, id: Uuid) -> Value {
        self.http
            .get(format!("{}/api/ingest/jobs/{id}", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// Runs the worker pool until the parent and every child have ended (up to 90 s); returns
    /// the parent's view.
    async fn run_to_end(&self, parent: Uuid) -> Value {
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            let open: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM ingest_jobs
                 WHERE (id = $1 OR parent_job_id = $1) AND status IN ('queued','running')",
            )
            .bind(parent)
            .fetch_one(&self.db.pool)
            .await
            .unwrap();
            if open == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{open} jobs still open"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(30), h)
            .await
            .expect("workers stop")
            .expect("worker pool");
        self.job(parent).await
    }

    /// `(url, status, source)` of the parent's children, oldest first.
    async fn children(&self, parent: Uuid) -> Vec<(String, String, Value)> {
        sqlx::query(
            "SELECT source, status FROM ingest_jobs WHERE parent_job_id = $1
             ORDER BY created_at, id",
        )
        .bind(parent)
        .fetch_all(&self.db.pool)
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let source: Value = r.get("source");
            (
                source["url"].as_str().unwrap().to_string(),
                r.get("status"),
                source,
            )
        })
        .collect()
    }

    fn hits(&self, url: &str) -> usize {
        self.scraper
            .hits
            .lock()
            .unwrap()
            .get(url)
            .copied()
            .unwrap_or(0)
    }

    /// Requests the sitemap server got for paths starting with `prefix`.
    fn map_hits(&self, prefix: &str) -> usize {
        self.maps
            .hits
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.starts_with(prefix))
            .map(|(_, n)| n)
            .sum()
    }

    async fn cancel(&self, id: Uuid) -> (StatusCode, Value) {
        let r = self
            .http
            .post(format!("{}/api/ingest/jobs/{id}/cancel", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        let s = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (s, r.json().await.unwrap())
    }

    async fn call(&self, method: reqwest::Method, path: &str) -> (StatusCode, Value) {
        let r = self
            .http
            .request(method, format!("{}{path}", self.url))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        let s = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (s, r.json().await.unwrap())
    }

    async fn job_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM ingest_jobs")
            .fetch_one(&self.db.pool)
            .await
            .unwrap()
    }
}

/// The test site: `/` links to /a, /b, /a again (fragment), another site, a subdomain and a
/// PDF; /a links back and to /c; /b to /e; /c (depth 2) to /deep (depth 3).
fn site() -> Scraper {
    let mut links = HashMap::new();
    links.insert(
        page("/"),
        vec![
            page("/a"),
            page("/b"),
            page("/a#section"),
            "https://other.org/x".to_string(),
            "https://docs.site.example.com/d".to_string(),
            page("/file.pdf"),
            "mailto:team@site.example.com".to_string(),
        ],
    );
    links.insert(page("/a"), vec![page("/"), page("/c")]);
    links.insert(page("/b"), vec![page("/e")]);
    links.insert(page("/c"), vec![page("/deep")]);
    Scraper {
        links,
        ..Scraper::default()
    }
}

fn crawl_body(config: Value) -> Value {
    json!({
        "store_id": leaf(), "source_id": "care-bus-crawl-1", "source_run": "run-3",
        "topic_keys": ["site/docs"], "provenance": provenance("CC-BY-4.0"),
        "source": {"kind": "crawl", "config": config},
        "options": {"captions": false},
    })
}

fn provenance(license: &str) -> Value {
    json!({"source_url": page("/"), "license": license, "retrieved_at": "2026-10-21T08:00:00Z"})
}

fn sitemap_body(config: Value) -> Value {
    json!({
        "store_id": leaf(),
        "source": {"kind": "sitemap", "config": config},
    })
}

// ---------------------------------------------------------------------------------------------
// crawl

#[tokio::test]
async fn crawl_creates_one_child_per_page_within_depth_and_domains() {
    let e = env(site(), 10).await;
    let parent = e.accepted(&crawl_body(json!({"url": page("/")}))).await;
    // Nothing is fetched inside the request.
    assert_eq!(e.scraper.hits.lock().unwrap().len(), 0);
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");

    let children = e.children(parent).await;
    let urls: Vec<String> = children.iter().map(|c| c.0.clone()).collect();
    // Breadth first; default depth 2 (/deep is depth 3); the fragment link, another site, the
    // PDF and mailto are not followed; the subdomain is.
    assert_eq!(
        urls,
        vec![
            page("/"),
            page("/a"),
            page("/b"),
            "https://docs.site.example.com/d".to_string(),
            page("/c"),
            page("/e"),
        ]
    );
    for (url, status, source) in &children {
        assert_eq!(status, "completed", "{url}: {source}");
        assert_eq!(source["parent_job_id"], parent.to_string());
        assert_eq!(source["prefetched"]["scan"], "clean", "{source}");
        // Scraped once, by the parent: the child reused the stored page.
        assert_eq!(e.hits(url), 1, "{url}");
    }
    assert_eq!(e.hits(&page("/deep")), 0);
    assert_eq!(e.hits("https://other.org/x"), 0);
    // The crawl asked webscraper for the images too.
    assert!(
        e.scraper
            .bodies
            .lock()
            .unwrap()
            .iter()
            .all(|b| b["download_images"] == true)
    );

    // Children inherit the parent's store, options, source_id and source_run; each delivered
    // its page.
    let received = e.received.lock().unwrap().clone();
    assert_eq!(received.len(), 6);
    for body in &received {
        assert_eq!(body["source_id"], "care-bus-crawl-1", "{body}");
        assert_eq!(body["source_run"], "run-3", "{body}");
        assert_eq!(body["topic_keys"], json!(["site/docs"]), "{body}");
        assert_eq!(body["provenance"], provenance("CC-BY-4.0"), "{body}");
        assert!(body.get("supersedes").is_none(), "{body}");
    }
    let child_view = e
        .job(Uuid::parse_str(view["child_job_ids"][0].as_str().unwrap()).unwrap())
        .await;
    assert_eq!(child_view["options"]["captions"], false);
    assert_eq!(
        child_view["meta"]["title"],
        format!("Title of {}", page("/"))
    );
    let acquire = child_view["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "acquire")
        .unwrap();
    assert_eq!(acquire["output"]["prefetched"], true, "{acquire}");

    // The parent's view summarises its children; its only step is discover.
    assert_eq!(view["children"]["total"], 6, "{view}");
    assert_eq!(view["children"]["by_status"]["completed"], 6);
    assert_eq!(view["children"]["by_status"]["failed"], 0);
    assert_eq!(view["child_job_ids"].as_array().unwrap().len(), 6);
    let steps = view["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["kind"], "discover");
    assert_eq!(steps[0]["output"]["pages"], 6);
    assert_eq!(steps[0]["output"]["children_created"], 6);
    // A plain url job's view has no children fields.
    assert!(child_view.get("children").is_none());
    assert!(child_view.get("child_job_ids").is_none());
}

#[tokio::test]
async fn crawl_respects_max_pages_depth_and_allowed_domains() {
    let e = env(site(), 10).await;
    let parent = e
        .accepted(&crawl_body(json!({
            "url": page("/"), "max_pages": 3, "link_query": "pricing"
        })))
        .await;
    e.run_to_end(parent).await;
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    assert_eq!(urls, vec![page("/"), page("/a"), page("/b")]);
    let bodies = e.scraper.bodies.lock().unwrap().clone();
    assert_eq!(bodies[0]["link_query"], "pricing");
    assert_eq!(bodies[0]["sort_links_by_score"], true);

    // Depth 1: the start page and its in-scope links only.
    let parent = e
        .accepted(&crawl_body(json!({"url": page("/"), "max_depth": 1})))
        .await;
    e.run_to_end(parent).await;
    assert_eq!(e.children(parent).await.len(), 4);

    // allowed_domains given: the start host is crawled, only listed hosts are followed.
    let parent = e
        .accepted(&crawl_body(json!({
            "url": page("/"), "allowed_domains": ["docs.site.example.com"]
        })))
        .await;
    e.run_to_end(parent).await;
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    assert_eq!(
        urls,
        vec![page("/"), "https://docs.site.example.com/d".to_string()]
    );
}

#[tokio::test]
async fn crawl_discover_failure_is_retried_then_terminal() {
    let mut s = site();
    s.down.insert(page("/"));
    let e = env(s, 2).await;
    let parent = e.accepted(&crawl_body(json!({"url": page("/")}))).await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "failed", "{view}");
    assert_eq!(view["error"]["code"], "fetch_unavailable", "{view}");
    assert_eq!(view["error"]["class"], "retryable");
    let step = &view["steps"][0];
    assert_eq!(step["kind"], "discover");
    assert_eq!(step["status"], "failed");
    assert_eq!(step["attempt"], 2, "retried before failing: {step}");
    assert_eq!(e.hits(&page("/")), 2);
    assert_eq!(view["children"]["total"], 0);
    assert!(e.children(parent).await.is_empty());
}

#[tokio::test]
async fn crawl_retry_replays_recorded_pages_without_scraping_them_again() {
    // BFS order: /, /a, /b, docs/d, /c, /e. The last page answers 503 once: the first attempt
    // records five pages (and their children), then fails retryable; the retry replays them.
    let mut s = site();
    s.fail_first.insert(page("/e"), 1);
    let e = env(s, 3).await;
    let parent = e.accepted(&crawl_body(json!({"url": page("/")}))).await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");

    let before = [
        page("/"),
        page("/a"),
        page("/b"),
        "https://docs.site.example.com/d".to_string(),
        page("/c"),
    ];
    // Each recorded page was scraped exactly once: not again by the retry, not by its child.
    for url in &before {
        assert_eq!(e.hits(url), 1, "{url}");
    }
    assert_eq!(
        e.hits(&page("/e")),
        2,
        "503 once, then scraped by the retry"
    );

    // No duplicate children: one per page, the retry created only the remaining one.
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    let mut expected = before.to_vec();
    expected.push(page("/e"));
    assert_eq!(urls, expected);
    assert_eq!(view["children"]["total"], 6, "{view}");
    assert_eq!(view["children"]["by_status"]["completed"], 6, "{view}");
    let step = &view["steps"][0];
    assert_eq!(step["kind"], "discover");
    assert_eq!(step["attempt"], 2, "{step}");
    assert_eq!(step["output"]["replayed"], before.len(), "{step}");
    assert_eq!(step["output"]["children_created"], 1, "{step}");
    assert_eq!(step["output"]["pages"], 6, "{step}");
}

#[tokio::test]
async fn crawl_skips_links_whose_host_is_not_public() {
    let mut links = HashMap::new();
    links.insert(
        page("/"),
        vec![
            page("/a"),
            "https://evil.site.example.com/x".to_string(),
            "https://meta.site.example.com/latest/meta-data/".to_string(),
            "https://ten.site.example.com/y".to_string(),
            // Literal non-public IPs never get this far (the url guard drops them).
            "http://127.0.0.1/z".to_string(),
            "http://169.254.169.254/latest/meta-data/".to_string(),
        ],
    );
    let e = env(
        Scraper {
            links,
            ..Scraper::default()
        },
        10,
    )
    .await;
    let parent = e.accepted(&crawl_body(json!({"url": page("/")}))).await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    assert_eq!(urls, vec![page("/"), page("/a")]);
    for unsafe_url in [
        "https://evil.site.example.com/x",
        "https://meta.site.example.com/latest/meta-data/",
        "https://ten.site.example.com/y",
        "http://127.0.0.1/z",
        "http://169.254.169.254/latest/meta-data/",
    ] {
        assert_eq!(e.hits(unsafe_url), 0, "{unsafe_url}");
    }
    let out = &view["steps"][0]["output"];
    assert_eq!(out["skipped_unsafe"], 3, "{out}");
    assert_eq!(out["skipped_unresolvable"], 0, "{out}");
    assert_eq!(out["pages"], 2, "{out}");
}

#[tokio::test]
async fn crawl_whose_start_page_is_not_public_fails_with_no_pages() {
    let e = env(site(), 10).await;
    let parent = e
        .accepted(&crawl_body(
            json!({"url": "https://evil.site.example.com/"}),
        ))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "failed", "{view}");
    assert_eq!(view["error"]["code"], "no_pages_discovered", "{view}");
    assert_eq!(view["steps"][0]["attempt"], 1, "not retried: {view}");
    assert_eq!(view["children"]["total"], 0);
    assert!(e.scraper.hits.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------------------------
// sitemap

#[tokio::test]
async fn sitemap_urlset_filters_and_dedupes() {
    let e = env(Scraper::default(), 10).await;
    let parent = e
        .accepted(&sitemap_body(json!({
            "url": e.map("sitemap.xml"), "include": ["/docs/"], "exclude": ["/old/"]
        })))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");
    let children = e.children(parent).await;
    let urls: Vec<String> = children.iter().map(|c| c.0.clone()).collect();
    assert_eq!(urls, vec![page("/docs/a"), page("/docs/b")]);
    // Sitemap children scrape their page themselves.
    for (url, status, source) in &children {
        assert_eq!(status, "completed", "{url}");
        assert!(source.get("prefetched").is_none());
        assert_eq!(e.hits(url), 1);
    }
    assert_eq!(view["children"]["by_status"]["completed"], 2);
    // A parent without source_id / source_run: its children send neither.
    let received = e.received.lock().unwrap().clone();
    assert_eq!(received.len(), 2);
    for body in &received {
        assert!(body.get("source_id").is_none(), "{body}");
        assert!(body.get("source_run").is_none(), "{body}");
        for field in ["topic_keys", "supersedes", "provenance"] {
            assert!(body.get(field).is_none(), "{body}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// url jobs

#[tokio::test]
async fn url_job_forwards_source_id_and_source_run_to_strata() {
    let e = env(site(), 10).await;
    let (s, v) = e
        .post(&json!({
            "store_id": leaf(), "url": page("/a"),
            "source_id": "care-bus-web-1", "source_run": " run-9 ",
        }))
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let job = Uuid::parse_str(v["jobs"][0]["job_id"].as_str().unwrap()).unwrap();
    let view = e.run_to_end(job).await;
    assert_eq!(view["status"], "completed", "{view}");
    assert_eq!(
        view["meta"]["source_run"], "run-9",
        "trimmed and kept: {view}"
    );
    let received = e.received.lock().unwrap().clone();
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0]["source_id"], "care-bus-web-1",
        "{}",
        received[0]
    );
    assert_eq!(received[0]["source_run"], "run-9", "{}", received[0]);
}

#[tokio::test]
async fn url_jobs_forward_topic_keys_supersedes_and_provenance_per_item() {
    let e = env(site(), 10).await;
    let (s, v) = e
        .post(&json!({
            "store_id": leaf(),
            "topic_keys": ["hr/leave", "hr/leave", "hr/a"], "supersedes": ["hr/old"],
            "provenance": provenance(" CC-BY-4.0 "),
            "urls": [
                page("/a"),
                {"url": page("/b"), "topic_keys": ["hr/b"], "provenance": provenance("MIT")},
            ],
        }))
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let ids: Vec<Uuid> = v["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| Uuid::parse_str(j["job_id"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(ids.len(), 2, "{v}");
    for id in &ids {
        assert_eq!(e.run_to_end(*id).await["status"], "completed");
    }
    // Kept in meta (validated: sorted, deduplicated, trimmed), the item's own replacing the
    // request's field by field.
    let a = e.job(ids[0]).await;
    assert_eq!(a["meta"]["topic_keys"], json!(["hr/a", "hr/leave"]), "{a}");
    assert_eq!(a["meta"]["supersedes"], json!(["hr/old"]), "{a}");
    assert_eq!(a["meta"]["provenance"], provenance("CC-BY-4.0"), "{a}");
    let received = e.received.lock().unwrap().clone();
    assert_eq!(received.len(), 2);
    let sent = |path: &str| {
        received
            .iter()
            .find(|b| b["source"] == json!(page(path)))
            .cloned()
            .unwrap_or_else(|| panic!("{path} delivered: {received:?}"))
    };
    let (ba, bb) = (sent("/a"), sent("/b"));
    assert_eq!(ba["topic_keys"], json!(["hr/a", "hr/leave"]), "{ba}");
    assert_eq!(ba["supersedes"], json!(["hr/old"]), "{ba}");
    assert_eq!(ba["provenance"], provenance("CC-BY-4.0"), "{ba}");
    assert_eq!(bb["topic_keys"], json!(["hr/b"]), "{bb}");
    assert_eq!(bb["supersedes"], json!(["hr/old"]), "{bb}");
    assert_eq!(bb["provenance"], provenance("MIT"), "{bb}");
}

#[tokio::test]
async fn strata_licence_refusal_fails_the_job_as_provenance_rejected() {
    let e = env(site(), 10).await;
    let (s, v) = e
        .post(&json!({"store_id": leaf(), "url": page("/a"),
                      "provenance": provenance("all-rights-reserved")}))
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let job = Uuid::parse_str(v["jobs"][0]["job_id"].as_str().unwrap()).unwrap();
    let view = e.run_to_end(job).await;
    assert_eq!(view["status"], "failed", "{view}");
    assert_eq!(view["error"]["code"], "provenance_rejected", "{view}");
    let msg = view["error"]["message"].as_str().unwrap();
    assert!(msg.contains("all-rights-reserved"), "{view}");
    // Terminal: delivered once, not retried.
    assert_eq!(e.received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn bad_document_fields_are_refused_before_any_job() {
    let e = env(site(), 10).await;
    let url = page("/a");
    let mut no_licence = provenance("CC-BY-4.0");
    no_licence.as_object_mut().unwrap().remove("license");
    let many: Vec<String> = (0..33).map(|i| format!("k{i}")).collect();
    let bodies = [
        json!({"store_id": leaf(), "url": url, "topic_keys": ["Upper"]}),
        json!({"store_id": leaf(), "url": url, "topic_keys": many}),
        json!({"store_id": leaf(), "url": url, "supersedes": ["x".repeat(129)]}),
        json!({"store_id": leaf(), "url": url, "topic_keys": "hr/leave"}),
        json!({"store_id": leaf(), "url": url, "provenance": no_licence}),
        json!({"store_id": leaf(), "url": url,
               "provenance": {"source_url": url, "license": "MIT", "retrieved_at": "today"}}),
        json!({"store_id": leaf(), "url": url,
               "provenance": {"source_url": " ", "license": "MIT",
                              "retrieved_at": "2026-10-21T08:00:00Z"}}),
        json!({"store_id": leaf(), "url": url, "provenance": {
               "source_url": url, "license": "MIT", "retrieved_at": "2026-10-21T08:00:00Z",
               "extra": 1}}),
        json!({"store_id": leaf(), "urls": [{"url": url, "topic_keys": ["a b"]}]}),
        json!({"store_id": leaf(), "urls": [{"url": url, "title": "x"}]}),
        json!({"store_id": leaf(), "urls": [7]}),
        json!({"store_id": leaf(), "topic_keys": ["Bad"],
               "source": {"kind": "crawl", "config": {"url": page("/")}}}),
        json!({"store_id": leaf(), "provenance": {"license": "MIT"},
               "source": {"kind": "sitemap", "config": {"url": page("/sitemap.xml")}}}),
    ];
    for body in &bodies {
        let (s, v) = e.post(body).await;
        assert!(
            s == StatusCode::BAD_REQUEST || s == StatusCode::UNPROCESSABLE_ENTITY,
            "{body}: {s} {v}"
        );
    }
    assert_eq!(e.job_count().await, 0);
}

#[tokio::test]
async fn source_run_without_source_id_or_malformed_is_refused() {
    let e = env(site(), 10).await;
    let bodies = [
        json!({"store_id": leaf(), "url": page("/a"), "source_run": "run-1"}),
        json!({"store_id": leaf(), "url": page("/a"), "source_id": "s", "source_run": ""}),
        json!({"store_id": leaf(), "url": page("/a"), "source_id": "s",
               "source_run": "x".repeat(201)}),
        json!({"store_id": leaf(), "url": page("/a"), "source_id": "s", "source_run": "a\nb"}),
        json!({"store_id": leaf(), "source_run": "run-1",
               "source": {"kind": "crawl", "config": {"url": page("/")}}}),
    ];
    for body in &bodies {
        let (s, v) = e.post(body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{body}: {v}");
    }
    assert_eq!(e.job_count().await, 0);
}

#[tokio::test]
async fn sitemap_index_recurses_gunzips_and_caps_max_urls() {
    let e = env(Scraper::default(), 10).await;
    let parent = e
        .accepted(&sitemap_body(json!({"url": e.map("index.xml")})))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    // s1.xml.gz (gzip), nested.xml → s2.xml (level 2); missing.xml (404) is skipped.
    assert_eq!(
        urls,
        vec![page("/g/1"), page("/g/2"), page("/n/1"), page("/n/2")]
    );
    assert_eq!(view["steps"][0]["output"]["sitemaps_skipped"], 1, "{view}");

    let parent = e
        .accepted(&sitemap_body(
            json!({"url": e.map("index.xml"), "max_urls": 3}),
        ))
        .await;
    e.run_to_end(parent).await;
    assert_eq!(e.children(parent).await.len(), 3);
}

#[tokio::test]
async fn sitemap_root_failure_fails_the_parent() {
    let e = env(Scraper::default(), 10).await;
    let parent = e
        .accepted(&sitemap_body(json!({"url": e.map("nope.xml")})))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "failed", "{view}");
    assert_eq!(view["error"]["code"], "source_rejected", "{view}");
    assert_eq!(view["steps"][0]["attempt"], 1, "a 404 is not retried");
    assert_eq!(view["children"]["total"], 0);
}

#[tokio::test]
async fn sitemap_that_yields_no_page_fails_instead_of_completing_empty() {
    let e = env(Scraper::default(), 10).await;
    // An empty urlset, and a urlset the filters reduce to nothing.
    for config in [
        json!({"url": e.map("empty.xml")}),
        json!({"url": e.map("sitemap.xml"), "include": ["/nothing-matches/"]}),
    ] {
        let parent = e.accepted(&sitemap_body(config.clone())).await;
        let view = e.run_to_end(parent).await;
        assert_eq!(view["status"], "failed", "{config}: {view}");
        assert_eq!(
            view["error"]["code"], "no_pages_discovered",
            "{config}: {view}"
        );
        assert_eq!(view["steps"][0]["attempt"], 1, "not retried: {view}");
        assert_eq!(view["children"]["total"], 0, "{view}");
        assert!(e.children(parent).await.is_empty());
    }
    assert!(e.scraper.hits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sitemap_index_and_urlset_reads_are_bounded() {
    let e = env(Scraper::default(), 10).await;
    // An index naming MANY sitemaps: at most SITEMAP_MAX_CHILD_SITEMAPS are queued (and
    // fetched); the rest are counted as skipped, never held or fetched.
    let parent = e
        .accepted(&sitemap_body(json!({"url": e.map("many.xml")})))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");
    let out = &view["steps"][0]["output"];
    assert_eq!(out["sitemaps_queued"], SITEMAP_MAX_CHILD_SITEMAPS, "{out}");
    assert_eq!(
        out["sitemaps_fetched"],
        SITEMAP_MAX_CHILD_SITEMAPS + 1,
        "{out}"
    );
    assert_eq!(
        out["sitemaps_skipped"],
        MANY - SITEMAP_MAX_CHILD_SITEMAPS,
        "{out}"
    );
    assert_eq!(e.map_hits("m/"), SITEMAP_MAX_CHILD_SITEMAPS);
    assert_eq!(out["urls"], 1, "{out}");
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    assert_eq!(urls, vec![page("/m/same")]);

    // A urlset far larger than max_urls: reading stops at the max_urls-th accepted URL.
    let parent = e
        .accepted(&sitemap_body(
            json!({"url": e.map("big.xml"), "max_urls": 5}),
        ))
        .await;
    let view = e.run_to_end(parent).await;
    assert_eq!(view["status"], "completed", "{view}");
    let out = &view["steps"][0]["output"];
    assert_eq!(out["locs_read"], 5, "{out}");
    assert_eq!(out["sitemaps_truncated"], 1, "{out}");
    assert_eq!(out["urls"], 5, "{out}");
    let urls: Vec<String> = e.children(parent).await.into_iter().map(|c| c.0).collect();
    assert_eq!(
        urls,
        (0..5)
            .map(|i| page(&format!("/big/{i}")))
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------------------------
// validation

#[tokio::test]
async fn invalid_discovery_configs_are_422_and_store_nothing() {
    let e = env(Scraper::default(), 10).await;
    let url = page("/");
    let mut bodies = vec![
        crawl_body(json!({"url": url, "max_depth": 0})),
        crawl_body(json!({"url": url, "max_depth": 6})),
        crawl_body(json!({"url": url, "max_pages": 0})),
        crawl_body(json!({"url": url, "max_pages": 201})),
        crawl_body(json!({"url": url, "allowed_domains": []})),
        crawl_body(json!({"url": url, "allowed_domains": ["https://site.example.com/"]})),
        crawl_body(json!({"url": url, "link_query": " "})),
        crawl_body(json!({"url": url, "follow": true})),
        crawl_body(json!({"url": "http://10.0.0.1/"})),
        crawl_body(json!({"url": "http://intranet.example.internal/"})),
        crawl_body(json!({"url": "ftp://site.example.com/"})),
        crawl_body(json!({})),
        sitemap_body(json!({"url": url, "max_urls": 0})),
        sitemap_body(json!({"url": url, "max_urls": 1001})),
        sitemap_body(json!({"url": url, "include": []})),
        sitemap_body(json!({"url": url, "exclude": [""]})),
        sitemap_body(json!({"url": url, "max_depth": 2})),
    ];
    let mut with_credentials = crawl_body(json!({"url": url}));
    with_credentials["source"]["credentials"] = json!({"secret": "never-echoed-7c1"});
    bodies.push(with_credentials);
    let mut with_mapping = sitemap_body(json!({"url": url}));
    with_mapping["mapping"] = json!([{"source_field": "a", "entity": "E", "attribute": "a"}]);
    bodies.push(with_mapping);
    for body in &bodies {
        let (s, v) = e.post(body).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}: {v}");
        assert_eq!(v["error"]["code"], "invalid_input", "{body}: {v}");
        assert!(v["error"]["message"].is_string());
        assert!(!v.to_string().contains("never-echoed-7c1"));
    }
    assert_eq!(e.job_count().await, 0);
    assert!(e.scraper.hits.lock().unwrap().is_empty());

    // Unknown store: 404 as for every job.
    let mut body = crawl_body(json!({"url": url}));
    body["store_id"] = json!(Uuid::now_v7());
    let (s, _) = e.post(&body).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(e.job_count().await, 0);
}

// ---------------------------------------------------------------------------------------------
// cancel

#[tokio::test]
async fn cancelling_a_parent_cancels_its_open_children_of_the_same_tenant() {
    let e = env(site(), 10).await;
    let parent = e
        .accepted(&crawl_body(json!({"url": page("/"), "max_depth": 1})))
        .await;
    // Run the parent's Discover by hand (no workers): its children are created and queued, the
    // parent stays running.
    let step = steps::claim_next(&e.db.pool, 10)
        .await
        .unwrap()
        .expect("the discover step");
    assert_eq!(step.job_id, parent);
    discover::discover(&e.ctx, &step)
        .await
        .expect("discover runs");
    let children: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM ingest_jobs WHERE parent_job_id = $1 ORDER BY created_at, id",
    )
    .bind(parent)
    .fetch_all(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(children.len(), 4);
    // One child already ended; another tenant's job claims the same parent.
    sqlx::query("UPDATE ingest_jobs SET status = 'completed' WHERE id = $1")
        .bind(children[0])
        .execute(&e.db.pool)
        .await
        .unwrap();
    let foreign = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO ingest_jobs (id, tenant, store_id, source, options, status, parent_job_id)
         VALUES ($1, $2, $3, $4, '{}', 'queued', $5)",
    )
    .bind(foreign)
    .bind(Uuid::now_v7())
    .bind(leaf())
    .bind(json!({"kind": "url", "url": page("/foreign")}))
    .bind(parent)
    .execute(&e.db.pool)
    .await
    .unwrap();

    let (s, view) = e.cancel(parent).await;
    assert_eq!(s, StatusCode::OK, "{view}");
    // Its Discover step is still running (claimed by hand above): cooperative cancellation.
    assert_eq!(view["status"], "cancelling", "{view}");
    assert_eq!(view["children"]["by_status"]["cancelled"], 3, "{view}");
    assert_eq!(view["children"]["by_status"]["completed"], 1, "{view}");
    assert_eq!(view["children"]["by_status"]["queued"], 0, "{view}");
    let status = |id: Uuid| {
        let pool = e.db.pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT status FROM ingest_jobs WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(status(children[0]).await, "completed");
    for id in &children[1..] {
        assert_eq!(status(*id).await, "cancelled", "{id}");
    }
    assert_eq!(
        status(foreign).await,
        "queued",
        "another tenant's job is untouched"
    );
    // The cancelled children's steps can no longer be claimed.
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_steps WHERE job_id = ANY($1) AND status = 'pending'",
    )
    .bind(&children[1..])
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(pending, 0);
    let cancelled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingest_steps WHERE job_id = ANY($1) AND status = 'cancelled'",
    )
    .bind(&children[1..])
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(cancelled, 3);
    // The running Discover ends: nothing more is enqueued and the parent is cancelled.
    steps::complete_and_enqueue(&e.db.pool, &step, steps::StepOutput::default(), &[])
        .await
        .unwrap();
    assert_eq!(status(parent).await, "cancelled");
    let (s, view) = e.cancel(parent).await;
    assert_eq!(s, StatusCode::OK, "cancel is idempotent: {view}");
    assert_eq!(view["status"], "cancelled", "{view}");
}

// ---------------------------------------------------------------------------------------------
// delete

/// Deleting a crawl parent: refused while a child is open or another tenant's job claims the
/// parent; a storage failure leaves parent and children `deleting` (shown by GET and the list,
/// not retryable or cancellable); deleting again removes them all with every child's original
/// and asset.
#[tokio::test]
async fn deleting_a_parent_cascades_to_its_children_and_their_blobs() {
    use strata_ingest::pipeline::original_key;
    let e = env(site(), 10).await;
    let parent = e
        .accepted(&crawl_body(json!({"url": page("/"), "max_depth": 1})))
        .await;
    let step = steps::claim_next(&e.db.pool, 10)
        .await
        .unwrap()
        .expect("the discover step");
    assert_eq!(step.job_id, parent);
    discover::discover(&e.ctx, &step)
        .await
        .expect("discover runs");
    steps::complete_and_enqueue(&e.db.pool, &step, steps::StepOutput::default(), &[])
        .await
        .unwrap();
    let children: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id FROM ingest_jobs WHERE parent_job_id = $1 ORDER BY created_at, id",
    )
    .bind(parent)
    .fetch_all(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(children.len(), 4);
    let path = format!("/api/ingest/jobs/{parent}");
    let status = |id: Uuid| {
        let pool = e.db.pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT status FROM ingest_jobs WHERE id = $1")
                .bind(id)
                .fetch_optional(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(status(parent).await.as_deref(), Some("completed"));
    // Staged blobs of the children: each original, and a figure of the first.
    for c in &children {
        e.storage.put(&original_key(tenant(), *c), &b"page"[..]);
    }
    let figure = format!("{}/{}/figures/{}", tenant(), children[0], Uuid::now_v7());
    e.storage.put(&figure, &b"png"[..]);
    sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, bytes, mime)
         VALUES ($1, $2, 'figure', $3, 3, 'image/png')",
    )
    .bind(Uuid::now_v7())
    .bind(children[0])
    .bind(&figure)
    .execute(&e.db.pool)
    .await
    .unwrap();

    // The parent ended, its children are still queued: 409, nothing changed.
    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert!(
        v["error"]["message"].as_str().unwrap().contains("child"),
        "{v}"
    );
    assert_eq!(status(parent).await.as_deref(), Some("completed"));
    for c in &children {
        assert_eq!(status(*c).await.as_deref(), Some("queued"), "{c}");
        assert!(e.storage.get(&original_key(tenant(), *c)).is_some());
    }

    // Cancel the open children (an ended parent: its children are cancelled).
    let (s, v) = e.cancel(parent).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["children"]["by_status"]["cancelled"], 4, "{v}");

    // A child (ended now) cannot be deleted alone: 409 `child_of_parent_job` naming the parent;
    // parent and child untouched (the child stays in the parent's `child_job_ids`).
    let (s, v) = e
        .call(
            reqwest::Method::DELETE,
            &format!("/api/ingest/jobs/{}", children[0]),
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["code"], "child_of_parent_job", "{v}");
    assert_eq!(v["error"]["parent_job_id"], parent.to_string(), "{v}");
    assert_eq!(status(children[0]).await.as_deref(), Some("cancelled"));
    assert_eq!(status(parent).await.as_deref(), Some("completed"));
    assert!(
        e.storage
            .get(&original_key(tenant(), children[0]))
            .is_some()
    );
    assert!(e.storage.get(&figure).is_some());
    let view = e.job(parent).await;
    assert_eq!(view["child_job_ids"].as_array().unwrap().len(), 4, "{view}");

    // Another tenant's job claims the parent: the cascade would reach it, so 409.
    let foreign = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO ingest_jobs (id, tenant, store_id, source, options, status, parent_job_id)
         VALUES ($1, $2, $3, $4, '{}', 'completed', $5)",
    )
    .bind(foreign)
    .bind(Uuid::now_v7())
    .bind(leaf())
    .bind(json!({"kind": "url", "url": page("/foreign")}))
    .bind(parent)
    .execute(&e.db.pool)
    .await
    .unwrap();
    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_eq!(status(foreign).await.as_deref(), Some("completed"));
    assert_eq!(status(parent).await.as_deref(), Some("completed"));
    sqlx::query("DELETE FROM ingest_jobs WHERE id = $1")
        .bind(foreign)
        .execute(&e.db.pool)
        .await
        .unwrap();

    // storage_service down: 502; parent and children stay `deleting`.
    e.storage.set_deletes_unavailable(true);
    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::BAD_GATEWAY, "{v}");
    assert_eq!(v["error"]["code"], "upstream_unavailable", "{v}");
    let view = e.job(parent).await;
    assert_eq!(view["status"], "deleting", "{view}");
    assert_eq!(view["children"]["by_status"]["deleting"], 4, "{view}");
    let (s, v) = e
        .call(reqwest::Method::GET, "/api/ingest/jobs?status=deleting")
        .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["jobs"].as_array().unwrap().len(), 5, "{v}");
    for id in [parent, children[1]] {
        for action in ["retry", "cancel"] {
            let (s, v) = e
                .call(
                    reqwest::Method::POST,
                    &format!("/api/ingest/jobs/{id}/{action}"),
                )
                .await;
            assert_eq!(s, StatusCode::CONFLICT, "{action} {id}: {v}");
        }
    }
    e.storage.set_deletes_unavailable(false);

    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "completed", "the status it had: {v}");
    assert_eq!(v["deleted_jobs"], 5, "{v}");
    assert_eq!(v["deleted_objects"], 6, "5 originals + 1 figure: {v}");
    assert_eq!(e.job_count().await, 0);
    for c in &children {
        assert!(e.storage.get(&original_key(tenant(), *c)).is_none(), "{c}");
    }
    assert!(e.storage.get(&figure).is_none());
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM ingest_steps")
        .fetch_one(&e.db.pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "children's steps cascade");
    let (s, _) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

/// A child still running a step blocks the parent's delete (it is `cancelling` after a cancel).
#[tokio::test]
async fn deleting_a_parent_waits_for_a_running_child() {
    let e = env(site(), 10).await;
    let parent = e
        .accepted(&crawl_body(json!({"url": page("/"), "max_depth": 1})))
        .await;
    let step = steps::claim_next(&e.db.pool, 10)
        .await
        .unwrap()
        .expect("the discover step");
    discover::discover(&e.ctx, &step)
        .await
        .expect("discover runs");
    steps::complete_and_enqueue(&e.db.pool, &step, steps::StepOutput::default(), &[])
        .await
        .unwrap();
    // A worker holds one child's Acquire.
    let held = steps::claim_next(&e.db.pool, 10)
        .await
        .unwrap()
        .expect("a child's acquire step");
    let (s, v) = e.cancel(parent).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["children"]["by_status"]["cancelling"], 1, "{v}");
    let path = format!("/api/ingest/jobs/{parent}");
    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_eq!(e.job(parent).await["status"], "completed");
    // The step ends: the child is cancelled and the delete goes through.
    steps::complete_and_enqueue(&e.db.pool, &held, steps::StepOutput::default(), &[])
        .await
        .unwrap();
    let (s, v) = e.call(reqwest::Method::DELETE, &path).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["deleted_jobs"], 5, "{v}");
    assert_eq!(e.job_count().await, 0);
}
