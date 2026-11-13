//! Bundle (stage 60) and Deliver (stage 70): the pure bundle builder, the Bundle step reading the
//! artefacts (`ingest_artifacts`), and delivery to a mock strata, on a real Postgres and an
//! in-process storage_service (`FakeStorage`).

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
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

use common::{FakeStorage, STORAGE_BUCKET, TestDb};
use strata_ingest::config::{Config, Mode, ProviderCfg, ProvidersFile, Region, Secret};
use strata_ingest::db::jobs::{self, NewJob};
use strata_ingest::db::steps::{self, ClaimedStep, StepKind, StepOutput};
use strata_ingest::db::{artifacts, assets};
use strata_ingest::limits::{Limits, MAX_BUNDLE_BYTES};
use strata_ingest::pipeline::bundle::{
    BundleInput, MAX_SOURCE_CHARS, Segment, build_bundle, figure_refs, has_content, truncate_chars,
};
use strata_ingest::pipeline::{
    BUNDLE_ARTIFACT, CONVERT_ARTIFACT, Ctx, ocr_artifact, original_key, run_step,
    transcript_artifact,
};
use strata_ingest::providers::ErrorClass;
use strata_ingest::testutil::spawn;
use strata_ingest::worker::{StepRunner, WorkerOpts, default_runner, run_workers_with};

fn uid(n: u128) -> Uuid {
    Uuid::from_u128(0x0199e1a0_0000_7000_8000_000000000000 + n)
}

fn ph(id: Uuid) -> String {
    format!("<!-- figure:{id} -->")
}

fn pages(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| s.to_string()).collect()
}

fn seg(start_s: f64, end_s: f64, text: &str) -> Segment {
    Segment {
        start_s,
        end_s,
        text: text.to_string(),
    }
}

// ---------------------------------------------------------------------------------------------
// build_bundle (pure)

#[test]
fn bundle_anchors_and_ocr_override() {
    let table = "| a | b |\n|---|---|\n| 1 | 2 |";
    let input = BundleInput {
        title: Some("Ignored: the document has its own heading".into()),
        pages: pages(&["# Doc\n\nfirst page text", "scanned junk", table, "low"]),
        ocr: BTreeMap::from([
            (1, "Real OCR text".to_string()),
            // Whitespace-only OCR (nothing found) does not blank the page.
            (3, "  \n".to_string()),
            // An OCR page past the last page has nowhere to go.
            (9, "stray".to_string()),
        ]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        format!(
            "<!-- page:1 -->\n# Doc\n\nfirst page text\n\n<!-- page:2 -->\nReal OCR text\n\n\
             <!-- page:3 -->\n{table}\n\n<!-- page:4 -->\nlow"
        )
    );
}

#[test]
fn ocr_text_loses_provider_image_refs() {
    let input = BundleInput {
        pages: pages(&["junk"]),
        ocr: BTreeMap::from([(
            0,
            "Text\n![img-0.jpeg](img-0.jpeg)\nmore ![x](y.png) end".to_string(),
        )]),
        ..BundleInput::default()
    };
    assert_eq!(build_bundle(&input), "<!-- page:1 -->\nText\nmore  end");
}

#[test]
fn title_only_without_top_level_heading() {
    let with = |p: &str| BundleInput {
        title: Some("My\n  Doc".into()),
        pages: pages(&[p]),
        ..BundleInput::default()
    };
    // No heading: the title (single-lined) leads.
    assert_eq!(
        build_bundle(&with("plain")),
        "# My Doc\n\n<!-- page:1 -->\nplain"
    );
    // A level-2 heading or a `#` inside a code fence is not a top-level heading.
    assert_eq!(
        build_bundle(&with("## Sub\n\ntext")),
        "# My Doc\n\n<!-- page:1 -->\n## Sub\n\ntext"
    );
    assert_eq!(
        build_bundle(&with("```\n# comment\n```")),
        "# My Doc\n\n<!-- page:1 -->\n```\n# comment\n```"
    );
    // A real top-level heading (up to 3 spaces of indent) wins.
    assert_eq!(
        build_bundle(&with("intro\n   # Own")),
        "<!-- page:1 -->\nintro\n   # Own"
    );
    // No title: nothing is prepended.
    let none = BundleInput {
        pages: pages(&["plain"]),
        ..BundleInput::default()
    };
    assert_eq!(build_bundle(&none), "<!-- page:1 -->\nplain");
}

#[test]
fn bundle_inlines_captions() {
    let (a, b) = (uid(1), uid(2));
    let input = BundleInput {
        pages: pages(&[&format!("Intro\n{}\nmid {} tail", ph(a), ph(b))]),
        captions: HashMap::from([(a, Some("A chart".to_string())), (b, None)]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        format!("<!-- page:1 -->\nIntro\n![A chart](asset:{a})\nmid ![](asset:{b}) tail")
    );
    assert_eq!(figure_refs(&input), vec![a, b]);
}

#[test]
fn caption_is_single_line_and_escaped() {
    let (a, b) = (uid(1), uid(2));
    let input = BundleInput {
        pages: pages(&[&format!("{}\n{}", ph(a), ph(b))]),
        captions: HashMap::from([
            (
                a,
                Some("Line one\n  line\ttwo ] back\\slash [x]\r\n".to_string()),
            ),
            // A blank caption is no caption.
            (b, Some("  \n ".to_string())),
        ]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        format!(
            "<!-- page:1 -->\n![Line one line two \\] back\\\\slash \\[x\\]](asset:{a})\n![](asset:{b})"
        )
    );
}

#[test]
fn caption_survives_ocr_override() {
    let (a, b) = (uid(1), uid(2));
    let input = BundleInput {
        pages: pages(&[
            &format!("docling junk\n\n{}\n\nmore junk {}", ph(a), ph(b)),
            // An image job: the page is the placeholder of the original alone.
            &ph(uid(3)),
            "untouched",
        ]),
        ocr: BTreeMap::from([
            (0, "OCR TEXT".to_string()),
            (1, "Receipt total 12".to_string()),
        ]),
        captions: HashMap::from([
            (a, Some("Cap A".to_string())),
            (b, Some("Cap B".to_string())),
            (uid(3), Some("A receipt".to_string())),
        ]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        format!(
            "<!-- page:1 -->\nOCR TEXT\n\n![Cap A](asset:{a})\n\n![Cap B](asset:{b})\n\n\
             <!-- page:2 -->\nReceipt total 12\n\n![A receipt](asset:{})\n\n\
             <!-- page:3 -->\nuntouched",
            uid(3)
        )
    );
    assert_eq!(figure_refs(&input), vec![a, b, uid(3)]);
}

#[test]
fn orphan_placeholder_is_removed() {
    let (a, orphan) = (uid(1), uid(9));
    let input = BundleInput {
        pages: pages(&[&format!(
            "before\n{}\n{}\nafter {} x\n<!-- figure:not-a-uuid -->",
            ph(orphan),
            ph(a),
            ph(orphan)
        )]),
        captions: HashMap::from([(a, None)]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        // A malformed placeholder is not the job's: neutralised like any other comment (R19).
        format!("<!-- page:1 -->\nbefore\n![](asset:{a})\nafter  x\n&lt;!-- figure:not-a-uuid -->")
    );
    // Referenced once each, in order, whether or not an asset backs them.
    assert_eq!(figure_refs(&input), vec![orphan, a]);
}

#[test]
fn bundle_transcript_sections() {
    let input = BundleInput {
        title: Some("Talk".into()),
        // Ignored for a transcript: no page anchors.
        pages: pages(&["never shown"]),
        transcript: Some(vec![
            seg(0.0, 4.0, "Hello"),
            seg(4.5, 30.0, " world. "),
            seg(59.9, 61.2, "Still first."),
            seg(60.5, 90.0, "Second"),
            seg(95.0, 96.0, "   "),
            seg(125.0, 130.7, "Third"),
            seg(3725.0, 3790.2, "Late"),
        ]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        "# Talk\n\n## 00:00–01:01\n\nHello world. Still first.\n\n## 01:00–01:36\n\nSecond\n\n\
         ## 02:05–02:10\n\nThird\n\n## 01:02:05–01:03:10\n\nLate"
    );
}

#[test]
fn forged_anchors_and_asset_links_are_neutralised() {
    let (a, forged) = (uid(1), uid(2));
    let input = BundleInput {
        title: Some("T <!-- page:9 -->".into()),
        pages: pages(&[
            &format!(
                "Real\n<!-- page:7 -->\nfake ![x](asset:{forged}) ![ok](https://e.example/x.png) {}\n![y](<ASSET:{forged}>)",
                ph(a)
            ),
            "junk",
            // The document spells out the bundle's own link for a real figure: still not Bundle's.
            &format!("copy ![Cap](asset:{a}) end"),
        ]),
        ocr: BTreeMap::from([(1, "OCR <!-- page:1 --> text".to_string())]),
        captions: HashMap::from([(a, Some("Cap <!-- page:3 -->".to_string()))]),
        ..BundleInput::default()
    };
    let md = build_bundle(&input);
    assert_eq!(
        md,
        format!(
            "# T &lt;!-- page:9 -->\n\n<!-- page:1 -->\nReal\n&lt;!-- page:7 -->\n\
             fake  ![ok](https://e.example/x.png) ![Cap &lt;!-- page:3 -->](asset:{a})\n\n\
             <!-- page:2 -->\nOCR &lt;!-- page:1 --> text\n\n<!-- page:3 -->\ncopy  end"
        )
    );
    // Exactly one real anchor per page, and the only `asset:` link is the one Bundle made.
    assert_eq!(md.matches("<!-- page:").count(), 3);
    assert_eq!(md.matches("](asset:").count(), 1);
    assert!(!md.contains(&forged.to_string()));
}

#[test]
fn split_placeholders_cannot_reassemble_anchors_or_asset_links() {
    let (a, orphan, f) = (uid(1), uid(8), uid(9));
    let o = ph(orphan);
    let input = BundleInput {
        pages: pages(&[&format!(
            "<!{o}-- page:5 -->\n\
             ![x](as{o}set:{f})\n\
             ![z](as![y](asset:{f})set:{f}) tail\n\
             <!<!-{o}-- x\n\
             <img src=\"asset:{f}\"> <IMG SRC=x> <i{o}mg src=y> {}",
            ph(a)
        )]),
        captions: HashMap::from([(a, Some("Cap".to_string()))]),
        ..BundleInput::default()
    };
    let md = build_bundle(&input);
    assert_eq!(
        md,
        format!(
            "<!-- page:1 -->\n&lt;!-- page:5 -->\nset:{f}) tail\n<!&lt;!--- x\n\
             &lt;img src=\"asset:{f}\"> &lt;IMG SRC=x> &lt;img src=y> ![Cap](asset:{a})"
        )
    );
    assert_eq!(md.matches("<!--").count(), 1, "only the real anchor");
    assert_eq!(md.matches("](asset:").count(), 1, "only Bundle's link");
}

#[test]
fn a_caption_cannot_carry_html_either() {
    let a = uid(1);
    let input = BundleInput {
        pages: pages(&[&ph(a)]),
        captions: HashMap::from([(a, Some("<img src=x> <!-- page:2 -->".to_string()))]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        format!("<!-- page:1 -->\n![&lt;img src=x> &lt;!-- page:2 -->](asset:{a})")
    );
}

#[test]
fn forged_anchor_in_a_transcript_is_neutralised() {
    let input = BundleInput {
        transcript: Some(vec![seg(
            0.0,
            5.0,
            "say <!-- page:2 --> ![x](asset:00000000-0000-0000-0000-000000000001)",
        )]),
        ..BundleInput::default()
    };
    assert_eq!(
        build_bundle(&input),
        "## 00:00–00:05\n\nsay &lt;!-- page:2 --> "
    );
}

#[test]
fn content_is_more_than_anchors_headers_and_title() {
    let blank = |pages_: &[&str], ocr: &[(u32, &str)]| BundleInput {
        title: Some("Title".into()),
        pages: pages(pages_),
        ocr: ocr.iter().map(|(i, t)| (*i, t.to_string())).collect(),
        ..BundleInput::default()
    };
    assert!(!has_content(&blank(&[], &[])));
    assert!(!has_content(&blank(&[" \n", "\n\n"], &[(0, "  ")])));
    // An orphan placeholder or a forged asset link alone is nothing.
    assert!(!has_content(&blank(
        &[&ph(uid(5)), &format!("![x](asset:{})", uid(6))],
        &[]
    )));
    assert!(has_content(&blank(&["", "text"], &[])));
    // A figure the job owns is content, caption or not.
    let mut fig = blank(&[&ph(uid(1))], &[]);
    fig.captions.insert(uid(1), None);
    assert!(has_content(&fig));
    // Transcripts: no segments, or only blank text, is empty.
    let t = |segs: Vec<Segment>| BundleInput {
        title: Some("Talk".into()),
        transcript: Some(segs),
        ..BundleInput::default()
    };
    assert!(!has_content(&t(vec![])));
    assert!(!has_content(&t(vec![
        seg(0.0, 1.0, " "),
        seg(70.0, 71.0, "\n")
    ])));
    assert!(has_content(&t(vec![seg(0.0, 1.0, "hi")])));
}

#[test]
fn source_is_truncated_on_a_char_boundary() {
    assert_eq!(truncate_chars("a.pdf", MAX_SOURCE_CHARS), "a.pdf");
    let long = "é".repeat(2000);
    let t = truncate_chars(&long, MAX_SOURCE_CHARS);
    assert_eq!(t.chars().count(), MAX_SOURCE_CHARS);
    assert!(long.starts_with(t));
}

// ---------------------------------------------------------------------------------------------
// Harness

fn tenant() -> Uuid {
    Uuid::parse_str("0199e1a0-0000-7000-8000-000000000001").unwrap()
}

/// Nothing listens on port 9 (discard).
const UNUSED_URL: &str = "http://127.0.0.1:9";

fn config(db: &TestDb, storage: &FakeStorage, webscraper: &str, strata: &str) -> Config {
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
        strata_url: strata.to_string(),
        strata_token: Secret::new("strata-token"),
        workers: 2,
        limits: Limits {
            figure_min_bytes: 1,
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
    }
}

/// `(store id, JSON body, Authorization)` of every delivery the mock received.
type Received = Arc<Mutex<Vec<(String, Value, String)>>>;

/// A strata stand-in: `POST /api/stores/{id}/documents` answers `status` + `reply`.
async fn strata_mock(status: StatusCode, reply: Value) -> (String, Received) {
    let received: Received = Arc::default();
    let app = Router::new()
        .route(
            "/api/stores/{id}/documents",
            post(
                move |State(rec): State<Received>,
                      Path(id): Path<String>,
                      headers: axum::http::HeaderMap,
                      axum::Json(body): axum::Json<Value>| {
                    let reply = reply.clone();
                    async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        rec.lock().await.push((id, body, auth));
                        (status, axum::Json(reply))
                    }
                },
            ),
        )
        .with_state(received.clone());
    (spawn(app).await, received)
}

struct Env {
    db: TestDb,
    storage: FakeStorage,
    ctx: Arc<Ctx>,
}

async fn env(webscraper: &str, strata: &str) -> Env {
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let cfg = config(&db, &storage, webscraper, strata);
    let ctx = Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap());
    Env { db, storage, ctx }
}

fn fast() -> WorkerOpts {
    WorkerOpts {
        poll: Duration::from_millis(50),
        heartbeat: Duration::from_secs(60),
        deadline: None,
        ..WorkerOpts::default()
    }
}

async fn job_row(pool: &PgPool, id: Uuid) -> (String, Option<Value>, Vec<Uuid>) {
    let r = sqlx::query("SELECT status, error, document_ids FROM ingest_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (r.get("status"), r.get("error"), r.get("document_ids"))
}

/// Runs the real worker pool (`runner`) until `job` is completed or failed (up to 60 s).
async fn run_job(ctx: &Arc<Ctx>, job: Uuid, runner: StepRunner) -> (String, Option<Value>) {
    let shutdown = CancellationToken::new();
    let h = tokio::spawn(run_workers_with(
        ctx.clone(),
        2,
        shutdown.clone(),
        fast(),
        runner,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let out = loop {
        let (status, error, _) = job_row(&ctx.pool, job).await;
        if matches!(status.as_str(), "completed" | "failed") {
            break (status, error);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job} still {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), h)
        .await
        .expect("workers stop")
        .expect("worker pool");
    out
}

async fn put(ctx: &Ctx, key: &str, body: Vec<u8>) {
    ctx.storage
        .upload_bytes(key, Bytes::from(body), "application/octet-stream")
        .await
        .expect("put object");
}

/// Seeds artefact `key` of `job` as an earlier step would have written it.
async fn put_artifact(pool: &PgPool, job: Uuid, key: &str, v: &Value) {
    sqlx::query("INSERT INTO ingest_artifacts (job_id, key, body) VALUES ($1, $2, $3)")
        .bind(job)
        .bind(key)
        .bind(serde_json::to_string(v).unwrap())
        .execute(pool)
        .await
        .unwrap();
}

/// The job's `bundle.md` artefact, if written.
async fn bundle_md(pool: &PgPool, job: Uuid) -> Option<String> {
    artifacts::get(pool, job, BUNDLE_ARTIFACT).await.unwrap()
}

/// As the API does it: store the original (scanned clean) at its key, then create the job.
async fn upload(ctx: &Ctx, store: Uuid, filename: &str, body: Vec<u8>, options: Value) -> Uuid {
    let id = Uuid::now_v7();
    let len = body.len();
    put(ctx, &original_key(tenant(), id), body).await;
    jobs::create_with_id(
        &ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: store,
            source: json!({"kind": "upload", "filename": filename, "mime": "text/plain",
                           "bytes": len, "scan": "clean"}),
            options,
        },
    )
    .await
    .expect("create job")
}

fn no_captions() -> Value {
    json!({"ocr": "auto", "captions": false, "language": null})
}

/// `(kind, key, status, output)` per step, in queue order.
async fn steps_of(pool: &PgPool, job: Uuid) -> Vec<(String, String, String, Option<Value>)> {
    sqlx::query(
        "SELECT kind, key, status, output FROM ingest_steps WHERE job_id = $1 ORDER BY stage, id",
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
            r.get("output"),
        )
    })
    .collect()
}

/// A job whose earlier stages are done, with `kind` (+ `input`) the one claimable step: Acquire is
/// marked completed, `done` steps are inserted completed, and the claimed step is returned.
async fn claim_single(
    ctx: &Ctx,
    source: Value,
    done: &[(StepKind, &str)],
    kind: StepKind,
    input: Value,
) -> ClaimedStep {
    let job = jobs::create(
        &ctx.pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source,
            options: no_captions(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE ingest_steps SET status = 'completed' WHERE job_id = $1")
        .bind(job)
        .execute(&ctx.pool)
        .await
        .unwrap();
    for (k, key) in done {
        sqlx::query(
            "INSERT INTO ingest_steps (job_id, kind, key, stage, status)
             VALUES ($1, $2, $3, $4, 'completed')",
        )
        .bind(job)
        .bind(k.as_str())
        .bind(key)
        .bind(k.stage())
        .execute(&ctx.pool)
        .await
        .unwrap();
    }
    let mut tx = ctx.pool.begin().await.unwrap();
    steps::enqueue(&mut tx, job, kind, "", input).await.unwrap();
    tx.commit().await.unwrap();
    let step = steps::claim_next(&ctx.pool, 5)
        .await
        .unwrap()
        .expect("claimable step");
    assert_eq!((step.job_id, step.kind), (job, kind));
    step
}

async fn insert_asset(pool: &PgPool, job: Uuid, id: Uuid, caption: Option<&str>) {
    sqlx::query(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, page, caption, bytes, mime)
         VALUES ($1, $2, 'figure', $3, 0, $4, 1, 'image/png')",
    )
    .bind(id)
    .bind(job)
    .bind(format!("{}/{job}/figures/{id}", tenant()))
    .bind(caption)
    .execute(pool)
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------------------------
// Bundle step

#[tokio::test]
async fn bundle_step_assembles_pages_ocr_and_captions_from_the_artefacts() {
    let e = env(UNUSED_URL, UNUSED_URL).await;
    let (a, b, orphan) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    // The job id is only known after creation.
    let step = claim_single(
        &e.ctx,
        json!({"kind": "upload", "filename": "scan.pdf"}),
        &[(StepKind::OcrPages, "p0-0")],
        StepKind::Bundle,
        json!({}),
    )
    .await;
    let job = step.job_id;
    sqlx::query("UPDATE ingest_steps SET input = $2 WHERE id = $1")
        .bind(step.id)
        .bind(json!({"convert_key": CONVERT_ARTIFACT}))
        .execute(&e.db.pool)
        .await
        .unwrap();
    let step = ClaimedStep {
        input: json!({"convert_key": CONVERT_ARTIFACT}),
        ..step
    };
    insert_asset(&e.db.pool, job, a, Some("Bar\nchart")).await;
    insert_asset(&e.db.pool, job, b, None).await;
    let pages = json!({"pages": [
        format!("scanned junk\n\n{}", ph(a)),
        format!("Text layer\n\n{}\n\n{}", ph(b), ph(orphan)),
    ]});
    put_artifact(&e.db.pool, job, CONVERT_ARTIFACT, &pages).await;
    put_artifact(
        &e.db.pool,
        job,
        &ocr_artifact("p0-0"),
        &json!({"0": "OCR words\n![img-0.jpeg](img-0.jpeg)"}),
    )
    .await;

    let (out, next) = run_step(&e.ctx, &step).await.expect("bundle");
    let want = format!(
        "# scan\n\n<!-- page:1 -->\nOCR words\n\n![Bar chart](asset:{a})\n\n\
         <!-- page:2 -->\nText layer\n\n![](asset:{b})\n"
    );
    assert_eq!(
        bundle_md(&e.db.pool, job).await.as_deref(),
        Some(want.as_str())
    );
    assert_eq!(out.output["bundle_key"], BUNDLE_ARTIFACT);
    assert_eq!(out.output["bytes"], want.len());
    assert_eq!(out.output["figures"], 2);
    assert_eq!(out.output["orphans"], 1);
    assert_eq!(out.output["ocr_pages"], json!([0]));
    assert_eq!(
        next,
        vec![(
            StepKind::Deliver,
            String::new(),
            json!({"bundle_key": BUNDLE_ARTIFACT, "title": "scan"})
        )]
    );
}

#[tokio::test]
async fn bundle_step_reads_transcripts_in_order() {
    let e = env(UNUSED_URL, UNUSED_URL).await;
    let step = claim_single(
        &e.ctx,
        json!({"kind": "upload", "filename": "talk.mp3"}),
        &[
            (StepKind::TranscribeSegment, "s0000"),
            (StepKind::TranscribeSegment, "s0001"),
        ],
        StepKind::Bundle,
        json!({}),
    )
    .await;
    let job = step.job_id;
    let keys = [transcript_artifact("s0000"), transcript_artifact("s0001")];
    put_artifact(
        &e.db.pool,
        job,
        &keys[0],
        &json!({"segments": [
            {"start_s": 0.0, "end_s": 30.0, "text": "One."},
            {"start_s": 30.0, "end_s": 60.0, "text": "Two."}
        ], "duration_s": 60.0, "offset_s": 0.0}),
    )
    .await;
    put_artifact(
        &e.db.pool,
        job,
        &keys[1],
        &json!({"segments": [
            {"start_s": 60.012, "end_s": 75.5, "text": "Three."}
        ], "duration_s": 15.5, "offset_s": 60.012}),
    )
    .await;
    let step = ClaimedStep {
        input: json!({"transcript_keys": keys}),
        ..step
    };
    let (out, next) = run_step(&e.ctx, &step).await.expect("bundle");
    let want = "# talk\n\n## 00:00–01:00\n\nOne. Two.\n\n## 01:00–01:15\n\nThree.";
    assert_eq!(bundle_md(&e.db.pool, job).await.as_deref(), Some(want));
    assert_eq!(out.output["segments"], 3);
    assert_eq!(next[0].0, StepKind::Deliver);

    // Artefacts are read by (job, key): another job's transcript is not visible to this step.
    let other = jobs::create(
        &e.db.pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": "other.mp3"}),
            options: no_captions(),
        },
    )
    .await
    .unwrap();
    put_artifact(
        &e.db.pool,
        other,
        "transcript/s9999.json",
        &json!({"segments": [{"start_s": 0.0, "end_s": 1.0, "text": "Foreign."}]}),
    )
    .await;
    let foreign = ClaimedStep {
        input: json!({"transcript_keys": ["transcript/s9999.json"]}),
        ..step
    };
    let err = run_step(&e.ctx, &foreign).await.unwrap_err();
    assert_eq!(
        (err.class, err.code.as_str()),
        (ErrorClass::InvalidRequest, "artifact_missing")
    );
}

#[tokio::test]
async fn blank_pages_fail_empty_bundle_without_delivery() {
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": Uuid::now_v7()}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "blank.txt",
        b"  \n\n\t \n".to_vec(),
        no_captions(),
    )
    .await;
    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "empty_bundle");
    assert_eq!(error["class"], "invalid_request");
    let steps = steps_of(&e.db.pool, job).await;
    assert!(!steps.iter().any(|s| s.0 == "deliver"), "{steps:?}");
    assert!(received.lock().await.is_empty(), "strata was not called");
    assert!(
        bundle_md(&e.db.pool, job).await.is_none(),
        "no bundle.md stored"
    );
}

#[tokio::test]
async fn blank_transcript_fails_empty_bundle() {
    let e = env(UNUSED_URL, UNUSED_URL).await;
    let step = claim_single(
        &e.ctx,
        json!({"kind": "upload", "filename": "silence.mp3"}),
        &[(StepKind::TranscribeSegment, "s0000")],
        StepKind::Bundle,
        json!({}),
    )
    .await;
    let key = transcript_artifact("s0000");
    put_artifact(
        &e.db.pool,
        step.job_id,
        &key,
        &json!({"segments": [
            {"start_s": 0.0, "end_s": 3.0, "text": "  "}
        ], "duration_s": 3.0, "offset_s": 0.0}),
    )
    .await;
    let step = ClaimedStep {
        input: json!({"transcript_keys": [key]}),
        ..step
    };
    let err = run_step(&e.ctx, &step).await.unwrap_err();
    assert_eq!(
        (err.class, err.code.as_str()),
        (ErrorClass::InvalidRequest, "empty_bundle")
    );
}

#[tokio::test]
async fn bundle_too_large_fails_without_delivery() {
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": Uuid::now_v7()}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let body = "word "
        .repeat(MAX_BUNDLE_BYTES as usize / 5 + 1)
        .into_bytes();
    let job = upload(&e.ctx, Uuid::now_v7(), "big.txt", body, no_captions()).await;

    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "failed");
    let error = error.expect("job error");
    assert_eq!(error["code"], "bundle_too_large");
    assert_eq!(error["class"], "invalid_request");
    let steps = steps_of(&e.db.pool, job).await;
    assert!(
        !steps.iter().any(|s| s.0 == "deliver"),
        "no deliver step: {steps:?}"
    );
    let bundle = steps.iter().find(|s| s.0 == "bundle").unwrap();
    assert_eq!(bundle.2, "failed");
    assert!(received.lock().await.is_empty(), "strata was not called");
    assert!(job_row(&e.db.pool, job).await.2.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Deliver + the whole chain

/// A small but valid figure for a markdown upload (`figure_min_bytes` = 1 in these tests).
const PNG_DATA_URI: &str = "data:image/png;base64,iVBORw0KGgo=";

#[tokio::test]
async fn text_upload_runs_end_to_end_to_completed() {
    let doc_id = Uuid::now_v7();
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": doc_id, "chunks": 1}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let store = Uuid::now_v7();
    let md = format!("Some notes.\n\n![a figure]({PNG_DATA_URI})\n\nThe end.\n");
    let job = upload(&e.ctx, store, "notes.md", md.into_bytes(), no_captions()).await;

    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    let (_, _, document_ids) = job_row(&e.db.pool, job).await;
    assert_eq!(document_ids, vec![doc_id]);
    // The original was stored (and virus-scanned by storage_service) exactly once.
    let originals = e
        .storage
        .uploads()
        .into_iter()
        .filter(|u| u.object_key.as_deref() == Some(original_key(tenant(), job).as_str()))
        .count();
    assert_eq!(originals, 1);

    // Every stage ran for real: acquire → … → caption (skipped: captions off) → bundle → deliver.
    let kinds: Vec<(String, String)> = steps_of(&e.db.pool, job)
        .await
        .into_iter()
        .map(|s| (s.0, s.2))
        .collect();
    let kinds: Vec<(&str, &str)> = kinds
        .iter()
        .map(|(k, s)| (k.as_str(), s.as_str()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("acquire", "completed"),
            ("scan", "completed"),
            ("detect", "completed"),
            ("convert", "completed"),
            ("caption", "skipped"),
            ("bundle", "completed"),
            ("deliver", "completed"),
        ]
    );
    let figure: Uuid =
        sqlx::query_scalar("SELECT id FROM ingest_assets WHERE job_id = $1 AND kind = 'figure'")
            .bind(job)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();

    let received = received.lock().await;
    assert_eq!(received.len(), 1);
    let (store_seen, body, auth) = &received[0];
    assert_eq!(store_seen, &store.to_string());
    assert_eq!(auth, "Bearer strata-token");
    assert_eq!(
        body,
        &json!({
            "text": format!(
                "# notes\n\n<!-- page:1 -->\nSome notes.\n\n![](asset:{figure})\n\nThe end.\n"
            ),
            "source": "notes.md",
            "title": "notes",
        })
    );

    let view = jobs::get(&e.db.pool, tenant(), job).await.unwrap();
    let deliver = view.steps.iter().find(|s| s.kind == "deliver").unwrap();
    let out = deliver.output.as_ref().expect("deliver output");
    assert_eq!(out["document_id"], json!(doc_id));
    assert_eq!(out["skipped_duplicate"], false);
}

#[tokio::test]
async fn duplicate_delivery_completes_job_as_skipped_duplicate() {
    let doc_id = Uuid::now_v7();
    let (strata, received) =
        strata_mock(StatusCode::OK, json!({"status": "existing", "id": doc_id})).await;
    let e = env(UNUSED_URL, &strata).await;
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "a.txt",
        b"# Same\n\nalready there".to_vec(),
        no_captions(),
    )
    .await;

    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(job_row(&e.db.pool, job).await.2, vec![doc_id]);
    assert_eq!(received.lock().await.len(), 1);
    // The document already had its own heading: no title heading added.
    assert_eq!(
        received.lock().await[0].1["text"],
        "<!-- page:1 -->\n# Same\n\nalready there"
    );

    let view = jobs::get(&e.db.pool, tenant(), job).await.unwrap();
    assert_eq!(view.job.status, "completed");
    let deliver = view.steps.iter().find(|s| s.kind == "deliver").unwrap();
    assert_eq!(deliver.status, "completed");
    let out = deliver.output.as_ref().expect("deliver output");
    assert_eq!(out["skipped_duplicate"], true);
    assert_eq!(out["duplicate"], true);
    assert_eq!(out["document_id"], json!(doc_id));
}

#[tokio::test]
async fn meta_source_id_is_forwarded_as_the_document_source_id() {
    let doc_id = Uuid::now_v7();
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": doc_id}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let id = Uuid::now_v7();
    let body = b"# Notes\n\nfrom a file source".to_vec();
    put(&e.ctx, &original_key(tenant(), id), body.clone()).await;
    jobs::create_with_meta(
        &e.ctx.pool,
        id,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "upload", "filename": "notes.md", "mime": "text/plain",
                           "bytes": body.len(), "scan": "clean"}),
            options: no_captions(),
        },
        &json!({"source_id": "care-bus-source-7"}),
    )
    .await
    .unwrap();
    let (status, error) = run_job(&e.ctx, id, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    let received = received.lock().await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].1["source_id"], "care-bus-source-7");
    assert_eq!(
        received[0].1.get("metadata"),
        None,
        "no metadata for a file"
    );
    // The pipeline's own meta writes keep the caller's source_id.
    let view = jobs::get(&e.db.pool, tenant(), id).await.unwrap();
    assert_eq!(view.job.meta["source_id"], "care-bus-source-7");
}

#[tokio::test]
async fn url_job_delivers_its_final_url_as_source() {
    let long_url = format!("https://example.com/{}", "ü".repeat(1500));
    let final_url = long_url.clone();
    let scraper = spawn(Router::new().route(
        "/api/scrape/url",
        post(move || {
            let final_url = final_url.clone();
            async move {
                axum::Json(json!({
                    "url": "https://example.com/a",
                    "final_url": final_url,
                    "title": "Example Domain",
                    "markdown_content": "Example text without a heading.\n",
                    "status": "completed"
                }))
            }
        }),
    ))
    .await;
    let doc_id = Uuid::now_v7();
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": doc_id}),
    )
    .await;
    let e = env(&scraper, &strata).await;
    let job = jobs::create(
        &e.db.pool,
        NewJob {
            tenant: tenant(),
            store_id: Uuid::now_v7(),
            source: json!({"kind": "url", "url": "https://example.com/a"}),
            options: no_captions(),
        },
    )
    .await
    .unwrap();

    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    let received = received.lock().await;
    let body = &received[0].1;
    let source = body["source"].as_str().unwrap();
    assert_eq!(source.chars().count(), MAX_SOURCE_CHARS);
    assert!(long_url.starts_with(source));
    assert_eq!(body["title"], "Example Domain");
    assert_eq!(
        body["text"],
        "# Example Domain\n\n<!-- page:1 -->\nExample text without a heading.\n"
    );
}

#[tokio::test]
async fn captions_from_the_asset_rows_reach_strata() {
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": Uuid::now_v7()}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let md = format!("# Fig\n\n![x]({PNG_DATA_URI})\n");
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "fig.md",
        md.into_bytes(),
        json!({"ocr": "auto", "captions": true, "language": null}),
    )
    .await;
    // No vision provider in these tests: Caption is stubbed to write a caption as the real step
    // does (fenced, on the asset row); everything else is the real pipeline.
    let runner: StepRunner = Arc::new(|ctx: Arc<Ctx>, step: ClaimedStep| {
        Box::pin(async move {
            if step.kind == StepKind::Caption {
                let id: Uuid = step.input["asset_id"].as_str().unwrap().parse().unwrap();
                assets::set_caption(&ctx.pool, &step, id, "A tiny\n[png] image")
                    .await
                    .unwrap();
                return Ok((StepOutput::default(), vec![]));
            }
            run_step(&ctx, &step).await
        })
    });
    let (status, error) = run_job(&e.ctx, job, runner).await;
    assert_eq!(status, "completed", "{error:?}");
    let figure: Uuid =
        sqlx::query_scalar("SELECT id FROM ingest_assets WHERE job_id = $1 AND kind = 'figure'")
            .bind(job)
            .fetch_one(&e.db.pool)
            .await
            .unwrap();
    assert_eq!(
        received.lock().await[0].1["text"],
        format!("<!-- page:1 -->\n# Fig\n\n![A tiny \\[png\\] image](asset:{figure})\n")
    );
}

/// The real Caption step against a vision provider that answers with a refusal: the refusal is
/// not stored as the caption, the step ends `skipped` (`caption_refused`) and the bundle keeps
/// the figure's bare link (R40).
#[tokio::test]
async fn a_refused_caption_leaves_the_figure_link_bare() {
    let reply = json!({
        "choices": [{"message": {"role": "assistant",
                                 "content": "I'm sorry, but I cannot assist with that."}}],
        "usage": {"total_tokens": 17},
    });
    let vision = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let reply = reply.clone();
            async move { axum::Json(reply) }
        }),
    );
    let vision = format!("{}/v1", spawn(vision).await);
    let (strata, received) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": Uuid::now_v7()}),
    )
    .await;
    let (db, storage) = tokio::join!(TestDb::start(), FakeStorage::start());
    let mut cfg = config(&db, &storage, UNUSED_URL, &strata);
    cfg.providers.vision = Some(ProviderCfg {
        base_url: vision,
        dialect: "openai".to_string(),
        model: "vlm-test".to_string(),
        api_key: Secret::new("k"),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 30_000,
        concurrency: 2,
        rpm: None,
        price_micros_per_unit: None,
    });
    let e = Env {
        ctx: Arc::new(Ctx::with_pool(db.pool.clone(), cfg).unwrap()),
        db,
        storage,
    };
    let md = format!("# Fig\n\n![x]({PNG_DATA_URI})\n");
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "fig.md",
        md.into_bytes(),
        json!({"ocr": "auto", "captions": true, "language": null}),
    )
    .await;
    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    let (figure, caption): (Uuid, Option<String>) = sqlx::query_as(
        "SELECT id, caption FROM ingest_assets WHERE job_id = $1 AND kind = 'figure'",
    )
    .bind(job)
    .fetch_one(&e.db.pool)
    .await
    .unwrap();
    assert_eq!(caption, None, "a refusal is never stored as the caption");
    let caption_step = steps_of(&e.db.pool, job)
        .await
        .into_iter()
        .find(|(kind, ..)| kind == "caption")
        .expect("a caption step");
    assert_eq!(caption_step.2, "skipped");
    assert_eq!(
        caption_step.3.as_ref().unwrap()["skipped"],
        "caption_refused",
        "{caption_step:?}"
    );
    let text = received.lock().await[0].1["text"].clone();
    assert_eq!(
        text,
        format!("<!-- page:1 -->\n# Fig\n\n![](asset:{figure})\n")
    );
    assert!(!text.as_str().unwrap().to_lowercase().contains("sorry"));
}

#[tokio::test]
async fn strata_rejection_fails_the_job_without_a_document_id() {
    let (strata, received) = strata_mock(
        StatusCode::CONFLICT,
        json!({"error": {"code": "leaf_full", "message": "leaf is full"}}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "a.txt",
        b"text".to_vec(),
        no_captions(),
    )
    .await;
    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "failed");
    assert_eq!(error.unwrap()["code"], "leaf_full");
    assert_eq!(received.lock().await.len(), 1, "terminal: one attempt");
    assert!(job_row(&e.db.pool, job).await.2.is_empty());
}

#[tokio::test]
async fn document_id_append_is_fenced_and_idempotent() {
    let e = env(UNUSED_URL, UNUSED_URL).await;
    let step = claim_single(
        &e.ctx,
        json!({"kind": "upload", "filename": "a.txt"}),
        &[],
        StepKind::Deliver,
        json!({}),
    )
    .await;
    let (d1, d2) = (Uuid::now_v7(), Uuid::now_v7());
    assert!(
        steps::append_document_id(&e.db.pool, &step, d1)
            .await
            .unwrap()
    );
    // A retry that gets the same id back adds nothing.
    assert!(
        !steps::append_document_id(&e.db.pool, &step, d1)
            .await
            .unwrap()
    );
    assert!(
        steps::append_document_id(&e.db.pool, &step, d2)
            .await
            .unwrap()
    );
    assert_eq!(job_row(&e.db.pool, step.job_id).await.2, vec![d1, d2]);
    // A stale claim writes nothing.
    let stale = ClaimedStep {
        attempt: step.attempt + 1,
        ..step.clone()
    };
    let r = steps::append_document_id(&e.db.pool, &stale, Uuid::now_v7()).await;
    assert!(
        matches!(r, Err(strata_ingest::IngestError::LeaseLost { .. })),
        "{r:?}"
    );
    assert_eq!(job_row(&e.db.pool, step.job_id).await.2, vec![d1, d2]);
}

/// Rulings R26: textual intermediates live in `ingest_artifacts` (fenced, upserted, cascaded with
/// the job); storage_service only holds the original and binary artefacts.
#[tokio::test]
async fn artifacts_live_in_postgres() {
    let (strata, _) = strata_mock(
        StatusCode::CREATED,
        json!({"status": "created", "id": Uuid::now_v7()}),
    )
    .await;
    let e = env(UNUSED_URL, &strata).await;
    let md = format!("Notes.\n\n![f]({PNG_DATA_URI})\n");
    let job = upload(
        &e.ctx,
        Uuid::now_v7(),
        "notes.md",
        md.into_bytes(),
        no_captions(),
    )
    .await;
    let (status, error) = run_job(&e.ctx, job, default_runner()).await;
    assert_eq!(status, "completed", "{error:?}");
    assert_eq!(
        artifacts::keys(&e.db.pool, job).await.unwrap(),
        ["bundle.md", "convert.json"]
    );
    let bundle = bundle_md(&e.db.pool, job).await.unwrap();
    assert!(bundle.contains("Notes."), "{bundle}");
    // storage_service: the original and the figure, nothing textual.
    let prefix = format!("{}/{job}/", tenant());
    let keys = e.storage.keys();
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert!(keys.contains(&original_key(tenant(), job)), "{keys:?}");
    assert!(
        keys.iter()
            .all(|k| k == &original_key(tenant(), job)
                || k.starts_with(&format!("{prefix}figures/"))),
        "{keys:?}"
    );

    // Writes are fenced on the claim and upserted by a retry of the same step.
    let step = claim_single(
        &e.ctx,
        json!({"kind": "upload", "filename": "a.txt"}),
        &[],
        StepKind::Bundle,
        json!({}),
    )
    .await;
    artifacts::put(&e.db.pool, &step, "bundle.md", "first")
        .await
        .unwrap();
    artifacts::put(&e.db.pool, &step, "bundle.md", "second")
        .await
        .unwrap();
    assert_eq!(
        bundle_md(&e.db.pool, step.job_id).await.as_deref(),
        Some("second")
    );
    let stale = ClaimedStep {
        attempt: step.attempt + 1,
        ..step.clone()
    };
    let r = artifacts::put(&e.db.pool, &stale, "bundle.md", "stale").await;
    assert!(
        matches!(r, Err(strata_ingest::IngestError::LeaseLost { .. })),
        "{r:?}"
    );
    let r = artifacts::put(&e.db.pool, &stale, "other.json", "{}").await;
    assert!(matches!(
        r,
        Err(strata_ingest::IngestError::LeaseLost { .. })
    ));
    assert_eq!(
        artifacts::keys(&e.db.pool, step.job_id).await.unwrap(),
        ["bundle.md"]
    );
    assert_eq!(
        bundle_md(&e.db.pool, step.job_id).await.as_deref(),
        Some("second")
    );
    // They go with their job.
    sqlx::query("DELETE FROM ingest_jobs WHERE id = $1")
        .bind(job)
        .execute(&e.db.pool)
        .await
        .unwrap();
    assert!(artifacts::keys(&e.db.pool, job).await.unwrap().is_empty());
}
