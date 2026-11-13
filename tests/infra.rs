//! Infra clients: storage_service (in-process `FakeStorage` implementing its contract, verdicts
//! included), docling-serve, webscraper and strata (in-process axum fakes capturing requests).

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes as AxumBytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use uuid::Uuid;

use common::{FakeStorage, Verdict};
use strata_ingest::config::Secret;
use strata_ingest::infra::docling::{DoclingClient, DoclingOpts};
use strata_ingest::infra::storage::{
    ScanVerdict, StorageClient, StorageError, StorageTimeouts, Uploaded,
};
use strata_ingest::infra::strata::{Origin, StrataClient};
use strata_ingest::infra::webscraper::WebscraperClient;
use strata_ingest::providers::ErrorClass;
use strata_ingest::testutil::spawn;

// ---------------------------------------------------------------------------------------------
// storage_service (in-process FakeStorage implementing the contract)

/// Deterministic, non-repeating-at-chunk-size content so a mis-ordered chunk would be caught.
fn pattern(len: usize) -> Vec<u8> {
    let mut x: u32 = 0x9e37_79b9;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn job_key(leaf: &str) -> String {
    format!("{}/{}/{leaf}", Uuid::now_v7(), Uuid::now_v7())
}

#[tokio::test]
async fn storage_roundtrip_streamed_upload() {
    let fake = FakeStorage::start().await;
    let s = fake.client();
    let data = pattern(20 * 1024 * 1024);
    let key = job_key("original");
    // Streamed from a file with its known length (the API's spool path).
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("upload");
    std::fs::write(&src, &data).unwrap();
    let up = s
        .upload_file(&key, &src, "application/pdf")
        .await
        .expect("upload");
    assert_eq!(up.size, data.len() as u64);
    assert_eq!(up.verdict(), ScanVerdict::Clean);
    let sent = fake.uploads();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].object_key.as_deref(), Some(key.as_str()));
    assert_eq!(sent[0].content_type.as_deref(), Some("application/pdf"));
    assert_eq!(sent[0].size, Some(data.len()));
    assert_eq!(
        sent[0].auth, None,
        "no token configured: tokenless in-cluster caller"
    );
    assert!(fake.get(&key).unwrap() == data, "stored bytes differ");

    assert_eq!(s.size(&key).await.unwrap(), data.len() as u64);
    assert!(s.exists(&key).await.unwrap());
    let got = s.get_bytes(&key, data.len() as u64).await.unwrap();
    assert!(got == data, "roundtrip bytes differ");
    let out = dir.path().join("download");
    assert_eq!(
        s.get_to_file(&key, &out, data.len() as u64).await.unwrap(),
        data.len() as u64
    );
    assert!(std::fs::read(&out).unwrap() == data);
    // Reads are bounded by the caller.
    assert!(matches!(
        s.get_bytes(&key, 1024).await.unwrap_err(),
        StorageError::TooLarge(1024)
    ));
    assert!(matches!(
        s.get_to_file(&key, &dir.path().join("cut"), 1024)
            .await
            .unwrap_err(),
        StorageError::TooLarge(1024)
    ));

    // A small in-memory body (artefact / scraped page) round-trips too.
    let small = job_key("figures/x");
    s.upload_bytes(&small, Bytes::from_static(b"hello"), "image/png")
        .await
        .unwrap();
    assert_eq!(&s.get_bytes(&small, 5).await.unwrap()[..], b"hello");

    s.delete(&key).await.expect("delete");
    // storage_service answers 404 for a missing key: still success.
    s.delete(&key).await.expect("delete is idempotent");
    assert!(!s.exists(&key).await.unwrap());
    assert!(matches!(
        s.get_bytes(&key, 10).await.unwrap_err(),
        StorageError::NotFound
    ));
    assert!(matches!(
        s.size(&key).await.unwrap_err(),
        StorageError::NotFound
    ));
    let e = StorageError::NotFound.step_error(&key);
    assert_eq!(
        (e.class, e.code.as_str()),
        (ErrorClass::InvalidRequest, "object_missing")
    );
}

#[tokio::test]
async fn storage_infected_upload_stores_nothing() {
    let fake = FakeStorage::start().await;
    let s = fake.client();
    let key = job_key("original");
    let mut body = b"some text then ".to_vec();
    body.extend_from_slice(common::EICAR);
    let e = s.upload_bytes(&key, body, "text/plain").await.unwrap_err();
    match &e {
        StorageError::Infected(sig) => assert_eq!(sig, common::EICAR_SIGNATURE),
        other => panic!("expected Infected, got {other:?}"),
    }
    assert!(fake.keys().is_empty());
    let se = e.step_error(&key);
    assert_eq!(
        (se.class, se.code.as_str()),
        (ErrorClass::InvalidRequest, "infected")
    );

    // A verdict-driven infection (any signature) maps the same way.
    fake.set_verdict(Verdict::Infected("Win.Test.Foo".into()));
    let e = s
        .upload_bytes(&key, Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap_err();
    assert!(
        matches!(&e, StorageError::Infected(s) if s == "Win.Test.Foo"),
        "{e:?}"
    );
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn storage_503_is_retryable_not_clean() {
    let fake = FakeStorage::start().await;
    fake.set_verdict(Verdict::Unavailable);
    let s = fake.client();
    let key = job_key("original");
    let e = s
        .upload_bytes(&key, Bytes::from_static(b"data"), "text/plain")
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unavailable(_)), "{e:?}");
    let se = e.step_error(&key);
    assert_eq!(
        (se.class, se.code.as_str()),
        (ErrorClass::Retryable, "storage_unavailable")
    );
    assert!(fake.keys().is_empty(), "a 503 stores nothing");
    // The API maps it to an upstream failure (502), never to a stored-and-clean result.
    let api: strata_ingest::IngestError = e.into();
    assert_eq!(api.code(), "upstream_unavailable");

    // A 500 on read (MinIO behind storage_service failing) is retryable too.
    fake.set_verdict(Verdict::Clean);
    s.upload_bytes(&key, Bytes::from_static(b"data"), "text/plain")
        .await
        .unwrap();
    fake.set_reads_unavailable(true);
    let e = s.get_bytes(&key, 100).await.unwrap_err();
    assert_eq!(e.step_error(&key).class, ErrorClass::Retryable, "{e:?}");

    // Nothing listening: connect refused → retryable.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let down =
        StorageClient::new(&format!("http://127.0.0.1:{port}"), "strata-ingest", None).unwrap();
    let e = down
        .upload_bytes(&key, Bytes::from_static(b"data"), "text/plain")
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unavailable(_)), "{e:?}");
    assert!(!down.health(Duration::from_secs(1)).await);
    assert!(fake.client().health(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn storage_unscanned_upload_is_unverified_not_clean() {
    let fake = FakeStorage::start().await;
    fake.set_verdict(Verdict::Unscanned);
    let up = fake
        .client()
        .upload_bytes(&job_key("original"), Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap();
    assert!(!up.virus_scanned);
    assert_eq!(up.verdict(), ScanVerdict::Unverified);
    // Only `virus_scanned=true` AND `scan_result="clean"` is clean.
    for (scanned, result) in [(true, None), (false, Some("clean")), (true, Some("other"))] {
        let u = Uploaded {
            size: 1,
            virus_scanned: scanned,
            scan_result: result.map(str::to_string),
        };
        assert_eq!(u.verdict(), ScanVerdict::Unverified, "{u:?}");
    }
}

#[tokio::test]
async fn storage_key_rules_are_checked_before_any_request() {
    let fake = FakeStorage::start().await;
    let s = fake.client();
    let long = "a".repeat(1025);
    for bad in [
        "",
        "/abs/key",
        "t/j/../x",
        "t/./x",
        "t/j/exists",
        "t/j/metadata",
        "t/j/a\nb",
        long.as_str(),
    ] {
        let e = s
            .upload_bytes(bad, Bytes::from_static(b"x"), "text/plain")
            .await
            .unwrap_err();
        assert!(matches!(e, StorageError::InvalidKey(_)), "{bad:?}: {e:?}");
        assert!(matches!(
            s.get_bytes(bad, 1).await.unwrap_err(),
            StorageError::InvalidKey(_)
        ));
        assert!(matches!(
            s.delete(bad).await.unwrap_err(),
            StorageError::InvalidKey(_)
        ));
    }
    assert!(fake.requests().is_empty(), "{:?}", fake.requests());
    // 1024 bytes is allowed; segments with spaces, `%`, `#` and `?` are percent-encoded on
    // the path and round-trip.
    let edge = format!("{}/{}", "b".repeat(511), "c".repeat(512));
    s.upload_bytes(&edge, Bytes::from_static(b"1"), "text/plain")
        .await
        .unwrap();
    assert_eq!(&s.get_bytes(&edge, 1).await.unwrap()[..], b"1");
    let odd = "t/j/a b%20c#d?e";
    s.upload_bytes(odd, Bytes::from_static(b"2"), "text/plain")
        .await
        .unwrap();
    assert_eq!(&s.get_bytes(odd, 1).await.unwrap()[..], b"2");
    assert_eq!(fake.keys(), vec![edge.clone(), odd.to_string()]);
    s.delete(odd).await.unwrap();
    assert_eq!(fake.keys(), vec![edge]);
}

#[tokio::test]
async fn storage_token_travels_as_a_sensitive_x_tenant_id_header() {
    let fake = FakeStorage::start().await;
    let token = Secret::new("licence-SECRET");
    let s = StorageClient::new(&fake.url, common::STORAGE_BUCKET, Some(&token)).unwrap();
    s.upload_bytes(&job_key("o"), Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap();
    assert_eq!(
        fake.uploads()[0].auth.as_deref(),
        Some("Bearer licence-SECRET")
    );
    assert!(!format!("{s:?}").contains("SECRET"), "{s:?}");
    // A wrong bucket is the service's 400, terminal (not an infection, not retryable).
    let other = StorageClient::new(&fake.url, "nope", None).unwrap();
    let e = other
        .upload_bytes(&job_key("o"), Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap_err();
    assert!(
        matches!(e, StorageError::Refused { status: 400, .. }),
        "{e:?}"
    );
    assert_eq!(e.step_error("k").class, ErrorClass::InvalidRequest);
}

#[tokio::test]
async fn storage_aborted_upload_body_stores_nothing() {
    let fake = FakeStorage::start().await;
    let s = fake.client();
    let key = job_key("original");
    let mut pieces: Vec<Result<Bytes, std::io::Error>> = (0..64)
        .map(|_| Ok(Bytes::from(vec![7u8; 64 * 1024])))
        .collect();
    pieces.push(Err(std::io::Error::other("client went away")));
    let body = reqwest::Body::wrap_stream(futures::stream::iter(pieces));
    let e = s
        .upload_stream(&key, body, 8 * 1024 * 1024, "text/plain")
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unavailable(_)), "{e:?}");
    // The service never saw a complete request: nothing stored, no upload recorded.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(fake.keys().is_empty());
    assert!(fake.uploads().is_empty());
    assert_eq!(fake.in_flight(), 0);
}

/// A storage_service download of `len` bytes sent as 1 KiB pieces, `gap` apart, with one
/// `stall` before the second piece.
async fn trickling_storage(len: usize, gap: Duration, stall: Duration) -> String {
    let app = Router::new().route(
        "/api/files/{bucket}/{*key}",
        get(move || async move {
            let pieces = len.div_ceil(1024);
            let body = futures::stream::unfold(0usize, move |i| async move {
                if i == pieces {
                    return None;
                }
                if i > 0 {
                    tokio::time::sleep(if i == 1 { stall.max(gap) } else { gap }).await;
                }
                let n = (len - i * 1024).min(1024);
                Some((Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; n])), i + 1))
            });
            (
                [("content-length", len.to_string())],
                axum::body::Body::from_stream(body),
            )
        }),
    );
    spawn(app).await
}

fn storage_at(url: &str, t: StorageTimeouts) -> StorageClient {
    StorageClient::with_timeouts(url, "strata-ingest", None, t).unwrap()
}

#[tokio::test]
async fn storage_timeouts_are_explicit_and_applied() {
    // The production defaults: a whole-request budget for a 100 MiB body on a slow link, and an
    // idle limit above storage_service's ~93 s worst-case in-request scan.
    assert_eq!(
        StorageTimeouts::default(),
        StorageTimeouts {
            connect: Duration::from_secs(10),
            request: Duration::from_secs(600),
            idle: Duration::from_secs(120),
        }
    );
    // A slow but moving body (3 s, a piece every 300 ms) is not cut by the idle limit.
    let url = trickling_storage(10 * 1024, Duration::from_millis(300), Duration::ZERO).await;
    let t = |request: u64, idle: u64| StorageTimeouts {
        connect: Duration::from_secs(5),
        request: Duration::from_secs(request),
        idle: Duration::from_secs(idle),
    };
    let got = storage_at(&url, t(30, 1))
        .get_bytes("k/original", u64::MAX)
        .await
        .expect("slow body within the request budget");
    assert_eq!(got.len(), 10 * 1024);
    // The request budget covers the body: the same 3 s body fails against a 1 s budget.
    let e = storage_at(&url, t(1, 30))
        .get_bytes("k/original", u64::MAX)
        .await
        .unwrap_err();
    assert_eq!(e.step_error("k").class, ErrorClass::Retryable, "{e:?}");
    // A stall longer than the idle limit fails, well inside the request budget.
    let url = trickling_storage(2 * 1024, Duration::ZERO, Duration::from_secs(3)).await;
    let e = storage_at(&url, t(60, 1))
        .get_bytes("k/original", u64::MAX)
        .await
        .unwrap_err();
    assert!(matches!(e, StorageError::Unavailable(_)), "{e:?}");
}

// ---------------------------------------------------------------------------------------------
// docling

/// `(name, filename, value)` per multipart part; enough of RFC 7578 for these assertions.
fn multipart_parts(headers: &HeaderMap, body: &[u8]) -> Vec<(String, Option<String>, Vec<u8>)> {
    let ct = headers["content-type"].to_str().unwrap();
    let boundary = ct
        .split("boundary=")
        .nth(1)
        .expect("boundary")
        .trim_matches('"');
    let delim = format!("--{boundary}");
    let text = body;
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(i) = find(rest, delim.as_bytes()) {
        rest = &rest[i + delim.len()..];
        if rest.starts_with(b"--") {
            break;
        }
        let rest_after_crlf = &rest[2..];
        let head_end = find(rest_after_crlf, b"\r\n\r\n").expect("part headers");
        let head = String::from_utf8_lossy(&rest_after_crlf[..head_end]).to_string();
        let content = &rest_after_crlf[head_end + 4..];
        let end = find(content, format!("\r\n{delim}").as_bytes()).expect("part end");
        let name = attr(&head, "name").expect("part name");
        parts.push((name, attr(&head, "filename"), content[..end].to_vec()));
        rest = &content[end + 2..];
    }
    parts
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn attr(head: &str, key: &str) -> Option<String> {
    let pat = format!("; {key}=\"");
    let i = head.find(&pat)? + pat.len();
    let j = head[i..].find('"')? + i;
    Some(head[i..j].to_string())
}

type Captured = Arc<Mutex<Vec<(HeaderMap, Vec<u8>)>>>;

async fn docling_fake(status: StatusCode, reply: Value) -> (String, Captured) {
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/v1/convert/file",
            post(
                |State((cap, status, reply)): State<(Captured, StatusCode, Value)>,
                 headers: HeaderMap,
                 body: AxumBytes| async move {
                    cap.lock().await.push((headers, body.to_vec()));
                    (status, axum::Json(reply))
                },
            ),
        )
        .with_state((captured.clone(), status, reply));
    (spawn(app).await, captured)
}

fn docling_ok(status: &str, md: &str) -> Value {
    json!({
        "document": {"filename": "a.pdf", "md_content": md, "json_content": null},
        "status": status,
        "errors": [],
        "processing_time": 1.25,
        "timings": {}
    })
}

#[tokio::test]
async fn docling_sends_required_form_fields() {
    let (url, cap) = docling_fake(StatusCode::OK, docling_ok("success", "# Title\n\nbody")).await;
    let client = DoclingClient::new(&url).unwrap();
    let opts = DoclingOpts {
        do_ocr: true,
        force_ocr: false,
        ocr_engine: "easyocr".into(),
        ..DoclingOpts::default()
    };
    let r = client
        .convert(Bytes::from_static(b"%PDF-1.7 fake"), "report.pdf", opts)
        .await
        .expect("convert");
    assert_eq!(r.markdown, "# Title\n\nbody");
    assert_eq!(r.status, "success");

    let cap = cap.lock().await;
    assert_eq!(cap.len(), 1, "exactly one attempt");
    let (headers, body) = &cap[0];
    let parts = multipart_parts(headers, body);
    let field = |n: &str| -> Vec<String> {
        parts
            .iter()
            .filter(|(name, _, _)| name == n)
            .map(|(_, _, v)| String::from_utf8(v.clone()).unwrap())
            .collect()
    };
    let file: Vec<_> = parts.iter().filter(|(n, _, _)| n == "files").collect();
    assert_eq!(file.len(), 1);
    assert_eq!(file[0].1.as_deref(), Some("report.pdf"));
    assert_eq!(file[0].2, b"%PDF-1.7 fake");
    // json: the page count and content pages that place the page breaks.
    assert_eq!(field("to_formats"), ["md", "json"]);
    assert_eq!(field("image_export_mode"), ["embedded"]);
    assert_eq!(field("md_page_break_placeholder"), ["<!-- page-break -->"]);
    assert_eq!(field("abort_on_error"), ["false"]);
    assert_eq!(field("table_mode"), ["accurate"]);
    assert_eq!(field("do_ocr"), ["true"]);
    assert_eq!(field("force_ocr"), ["false"]);
    assert_eq!(field("ocr_engine"), ["easyocr"]);
    assert!(field("page_range").is_empty(), "whole document by default");
}

#[tokio::test]
async fn docling_sends_a_page_range_and_placeholder_images_when_asked() {
    // The window 9..=10 of a longer document: docling reports pages 9 and 10 (whole-document
    // numbers, measured on v1.37) with an item on page 10 only.
    let mut reply = docling_ok("success", "ten");
    reply["document"]["json_content"] = json!({
        "pages": {"9": {}, "10": {}},
        "texts": [{"prov": [{"page_no": 10}]}]
    });
    let (url, cap) = docling_fake(StatusCode::OK, reply).await;
    let opts = DoclingOpts {
        page_range: Some((9, 10)),
        embed_images: false,
        ..DoclingOpts::default()
    };
    let r = DoclingClient::new(&url)
        .unwrap()
        .convert(Bytes::from_static(b"%PDF-1.7 fake"), "a.pdf", opts)
        .await
        .expect("convert");
    assert_eq!(r.page_count, 2);
    let pages: Vec<&str> = r
        .markdown
        .split("<!-- page-break -->")
        .map(str::trim)
        .collect();
    assert_eq!(pages, ["", "ten"], "padded to the window");
    let cap = cap.lock().await;
    let parts = multipart_parts(&cap[0].0, &cap[0].1);
    let field = |n: &str| -> Vec<String> {
        parts
            .iter()
            .filter(|(name, _, _)| name == n)
            .map(|(_, _, v)| String::from_utf8(v.clone()).unwrap())
            .collect()
    };
    assert_eq!(field("page_range"), ["9", "10"]);
    assert_eq!(field("image_export_mode"), ["placeholder"]);
}

#[tokio::test]
async fn docling_answer_over_the_cap_is_conversion_too_large() {
    let (url, _) = docling_fake(StatusCode::OK, docling_ok("success", &"x".repeat(10_000))).await;
    let e = DoclingClient::new(&url)
        .unwrap()
        .with_max_response(4096)
        .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
        .await
        .expect_err("over the cap");
    assert_eq!(
        (e.class, e.code.as_str()),
        (ErrorClass::InvalidRequest, "conversion_too_large")
    );
    // Under the cap: unchanged behaviour.
    let (url, _) = docling_fake(StatusCode::OK, docling_ok("success", "small")).await;
    let r = DoclingClient::new(&url)
        .unwrap()
        .with_max_response(4096)
        .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
        .await
        .expect("under the cap");
    assert_eq!(r.markdown, "small");
}

#[tokio::test]
async fn docling_partial_success_is_ok() {
    let mut reply = docling_ok("partial_success", "page one");
    reply["errors"] = json!([{"component_type": "pipeline", "module_name": "x", "error_message": "page 2 failed"}]);
    let (url, _) = docling_fake(StatusCode::OK, reply).await;
    let r = DoclingClient::new(&url)
        .unwrap()
        .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
        .await
        .expect("partial_success is usable");
    assert_eq!(r.status, "partial_success");
    assert_eq!(r.markdown, "page one");
    assert_eq!(r.errors.len(), 1);
}

/// A `partial_success` whose failed page docling left out of `pages` (non-contiguous keys):
/// the gap becomes an empty page (low text → OCR'd) and is reported, instead of failing the
/// whole document; the same gap on `success` still contradicts and fails.
#[tokio::test]
async fn docling_partial_success_fills_missing_pages_as_empty() {
    let text_on = |p: usize| json!({"prov": [{"page_no": p}], "label": "text", "text": "t"});
    let json_content = json!({
        "pages": {"1": {}, "2": {}, "4": {}},
        "texts": [text_on(1), text_on(2), text_on(4)]
    });
    let md = "one\n\n<!-- page-break -->\n\ntwo\n\n<!-- page-break -->\n\nfour";
    let mut reply = docling_ok("partial_success", md);
    reply["document"]["json_content"] = json_content.clone();
    reply["errors"] = json!([{"component_type": "pipeline", "module_name": "x", "error_message": "page 3 failed"}]);
    let (url, _) = docling_fake(StatusCode::OK, reply).await;
    let r = DoclingClient::new(&url)
        .unwrap()
        .convert(
            Bytes::from_static(b"%PDF-"),
            "a.pdf",
            DoclingOpts::default(),
        )
        .await
        .expect("partial_success with a missing page is usable");
    assert_eq!(r.page_count, 4);
    assert_eq!(r.missing_pages, [3]);
    let pages: Vec<&str> = r
        .markdown
        .split("<!-- page-break -->")
        .map(str::trim)
        .collect();
    assert_eq!(pages, ["one", "two", "", "four"]);

    let mut reply = docling_ok("success", md);
    reply["document"]["json_content"] = json_content;
    let (url, _) = docling_fake(StatusCode::OK, reply).await;
    let e = DoclingClient::new(&url)
        .unwrap()
        .convert(
            Bytes::from_static(b"%PDF-"),
            "a.pdf",
            DoclingOpts::default(),
        )
        .await
        .expect_err("a gap on success contradicts");
    assert_eq!(e.code, "convert_failed");
}

#[tokio::test]
async fn docling_empty_markdown_is_terminal() {
    let mut no_document = docling_ok("success", "");
    no_document["document"] = Value::Null;
    let mut no_md = docling_ok("success", "");
    no_md["document"] = json!({"filename": "a.pdf"});
    for reply in [
        docling_ok("success", ""),
        docling_ok("success", "  \n\t \n"),
        docling_ok("partial_success", "\n"),
        no_document,
        no_md,
    ] {
        let (url, cap) = docling_fake(StatusCode::OK, reply.clone()).await;
        let e = DoclingClient::new(&url)
            .unwrap()
            .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
            .await
            .unwrap_err();
        assert_eq!(
            (e.class, e.code.as_str()),
            (ErrorClass::InvalidRequest, "empty_conversion"),
            "{reply}"
        );
        assert_eq!(cap.lock().await.len(), 1);
    }
}

/// docling-serve v1.37 leaves out the page breaks of content-less leading/trailing pages and
/// answers an all-scanned PDF with empty markdown; the JSON's page count restores them.
#[tokio::test]
async fn docling_pads_pages_docling_left_out() {
    let pages = json!({"1": {"page_no": 1}, "2": {"page_no": 2}, "3": {"page_no": 3}});
    let text_on = |p: usize| json!({"prov": [{"page_no": p}], "label": "text", "text": "t"});
    let cases = [
        // scan, text, scan (measured: one segment)
        (
            "only page two",
            json!([text_on(2)]),
            vec!["", "only page two", ""],
        ),
        // all scanned (measured: empty markdown, an empty table on page 2)
        ("", json!([]), vec!["", "", ""]),
    ];
    for (md, texts, want) in cases {
        let mut reply = docling_ok("success", md);
        reply["document"]["json_content"] = json!({"pages": pages, "texts": texts,
            "tables": [{"prov": [{"page_no": 2}], "data": {"table_cells": []}}]});
        let (url, _) = docling_fake(StatusCode::OK, reply).await;
        let r = DoclingClient::new(&url)
            .unwrap()
            .convert(
                Bytes::from_static(b"%PDF-"),
                "a.pdf",
                DoclingOpts::default(),
            )
            .await
            .expect("convert");
        assert_eq!(r.page_count, 3);
        let got: Vec<&str> = r
            .markdown
            .split("<!-- page-break -->")
            .map(str::trim)
            .collect();
        assert_eq!(got, want, "{md:?}");
    }
}

#[tokio::test]
async fn docling_failure_is_not_retried() {
    for status in ["failure", "skipped"] {
        let (url, cap) = docling_fake(StatusCode::OK, docling_ok(status, "")).await;
        let e = DoclingClient::new(&url)
            .unwrap()
            .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
            .await
            .unwrap_err();
        assert_eq!(e.class, ErrorClass::InvalidRequest, "{status}: {e:?}");
        assert_eq!(e.code, "convert_failed");
        assert_eq!(cap.lock().await.len(), 1, "the client makes one attempt");
    }

    // 5xx is the queue's to retry.
    let (url, _) = docling_fake(StatusCode::SERVICE_UNAVAILABLE, json!({"detail": "busy"})).await;
    let e = DoclingClient::new(&url)
        .unwrap()
        .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Retryable, "{e:?}");

    // Transport failure (nothing listening) is retryable too.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e = DoclingClient::new(&format!("http://127.0.0.1:{port}"))
        .unwrap()
        .convert(Bytes::from_static(b"x"), "a.pdf", DoclingOpts::default())
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Retryable, "{e:?}");
}

// ---------------------------------------------------------------------------------------------
// webscraper

async fn webscraper_fake(status: StatusCode, reply: Value) -> (String, Captured) {
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/api/scrape/url",
            post(
                |State((cap, status, reply)): State<(Captured, StatusCode, Value)>,
                 headers: HeaderMap,
                 body: AxumBytes| async move {
                    cap.lock().await.push((headers, body.to_vec()));
                    (status, axum::Json(reply))
                },
            ),
        )
        .with_state((captured.clone(), status, reply));
    (spawn(app).await, captured)
}

fn scraped(md: Option<&str>) -> Value {
    json!({
        "url": "https://example.com/a",
        "final_url": "https://example.com/a/",
        "title": "Example Domain",
        "markdown_content": md,
        "status": "completed",
        "scraper_used": "HttpxScraper",
        "processing_time_seconds": 0.4,
        "metadata": {}, "structured_data": {}, "links": [], "images": []
    })
}

#[tokio::test]
async fn webscraper_sends_tenant_header_and_maps_markdown() {
    let (url, cap) = webscraper_fake(StatusCode::OK, scraped(Some("# Example\n\ntext"))).await;
    let client = WebscraperClient::new(&url, Some(Secret::new("lic-token"))).unwrap();
    let s = client
        .scrape("https://example.com/a")
        .await
        .expect("scrape");
    assert_eq!(s.markdown, "# Example\n\ntext");
    assert_eq!(s.final_url, "https://example.com/a/");
    assert_eq!(s.title.as_deref(), Some("Example Domain"));

    let cap = cap.lock().await;
    let (headers, body) = &cap[0];
    assert_eq!(headers["x-tenant-id"], "Bearer lic-token");
    let body: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(
        body,
        json!({"url": "https://example.com/a", "scraper_type": "auto"})
    );
    drop(cap);

    // Without a token: no header at all.
    let (url, cap) = webscraper_fake(StatusCode::OK, scraped(Some("x"))).await;
    WebscraperClient::new(&url, None)
        .unwrap()
        .scrape("https://example.com/a")
        .await
        .unwrap();
    assert!(cap.lock().await[0].0.get("x-tenant-id").is_none());

    // Empty markdown → empty_page; 4xx → fetch_rejected; 5xx → retryable.
    for md in [None, Some(""), Some("  \n ")] {
        let (url, _) = webscraper_fake(StatusCode::OK, scraped(md)).await;
        let e = WebscraperClient::new(&url, None)
            .unwrap()
            .scrape("https://example.com/a")
            .await
            .unwrap_err();
        assert_eq!(
            (e.class, e.code.as_str()),
            (ErrorClass::InvalidRequest, "empty_page")
        );
    }
    let (url, _) = webscraper_fake(StatusCode::BAD_REQUEST, json!({"detail": "blocked url"})).await;
    let e = WebscraperClient::new(&url, None)
        .unwrap()
        .scrape("http://10.0.0.1/")
        .await
        .unwrap_err();
    assert_eq!(
        (e.class, e.code.as_str()),
        (ErrorClass::InvalidRequest, "fetch_rejected")
    );
    let (url, _) = webscraper_fake(StatusCode::BAD_GATEWAY, json!({})).await;
    let e = WebscraperClient::new(&url, None)
        .unwrap()
        .scrape("https://example.com/a")
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::Retryable);
    // A refused licence token is a licences-service blip (or a rotation), not this URL's
    // fault: retried, never terminal (R41).
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        let (url, _) = webscraper_fake(status, json!({"detail": "Invalid or expired"})).await;
        let e = WebscraperClient::new(&url, Some(Secret::new("t")))
            .unwrap()
            .scrape("https://example.com/a")
            .await
            .unwrap_err();
        assert_eq!(
            (e.class, e.code.as_str()),
            (ErrorClass::Retryable, "webscraper_auth"),
            "{status}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_uploads_in_flight_are_capped_per_pod() {
    // storage-api buffers whole bodies in a 256 Mi pod: this pod sends at most UPLOAD_SLOTS
    // (2) at once, across every clone of the client (R38).
    use strata_ingest::infra::storage::UPLOAD_SLOTS;
    let fake = FakeStorage::start().await;
    fake.set_upload_delay(Duration::from_millis(200));
    let client = fake.client();
    let mut set = tokio::task::JoinSet::new();
    for i in 0..6 {
        let c = client.clone();
        set.spawn(async move {
            c.upload_bytes(&job_key(&format!("f{i}")), vec![1u8; 1024], "image/png")
                .await
        });
    }
    while let Some(r) = set.join_next().await {
        r.unwrap().expect("upload");
    }
    assert_eq!(fake.uploads().len(), 6);
    assert_eq!(fake.max_in_flight(), UPLOAD_SLOTS);
    assert_eq!(UPLOAD_SLOTS, 2);
}

#[test]
fn storage_auth_refusal_is_retryable() {
    // storage_service answers 401 on any licence-introspection failure (cached 60 s): R41.
    for status in [401u16, 403] {
        let e = StorageError::Refused {
            status,
            message: "Invalid or expired license token".into(),
        }
        .step_error("k");
        assert_eq!(
            (e.class, e.code.as_str()),
            (ErrorClass::Retryable, "storage_auth"),
            "{status}"
        );
    }
    let e = StorageError::Refused {
        status: 429,
        message: "busy".into(),
    }
    .step_error("k");
    assert_eq!(e.class, ErrorClass::Retryable, "429 is retried");
}

// ---------------------------------------------------------------------------------------------
// strata

#[derive(Clone)]
struct StrataFake {
    captured: Captured,
    reply: (StatusCode, Value),
}

async fn strata_fake(reply: (StatusCode, Value)) -> (String, Captured) {
    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let state = StrataFake {
        captured: captured.clone(),
        reply,
    };
    let leaf = Uuid::from_u128(1);
    let router = Uuid::from_u128(2);
    let app = Router::new()
        .route(
            "/api/stores/{id}",
            get(move |Path(id): Path<Uuid>, headers: HeaderMap| async move {
                // strata scopes stores by tenant: the lookup must name the caller's tenant.
                if headers.get("x-tenant-id").and_then(|v| v.to_str().ok())
                    != Some(Uuid::from_u128(0xA).to_string().as_str())
                {
                    return (
                        StatusCode::BAD_REQUEST,
                        axum::Json(
                            json!({"error": {"code": "invalid_input", "message": "tenant"}}),
                        ),
                    )
                        .into_response();
                }
                let kind = if id == leaf {
                    "leaf"
                } else if id == router {
                    "router"
                } else {
                    return (
                        StatusCode::NOT_FOUND,
                        axum::Json(json!({"error": {"code": "not_found", "message": "no store"}})),
                    )
                        .into_response();
                };
                axum::Json(json!({
                    "id": id, "parent_id": null, "kind": kind, "name": "n", "description": "d"
                }))
                .into_response()
            }),
        )
        .route(
            "/api/stores/{id}/documents",
            post(
                |State(s): State<StrataFake>, headers: HeaderMap, body: AxumBytes| async move {
                    s.captured.lock().await.push((headers, body.to_vec()));
                    (s.reply.0, axum::Json(s.reply.1))
                },
            ),
        )
        .with_state(state);
    (spawn(app).await, captured)
}

fn doc_row(status: &str, id: Uuid) -> Value {
    json!({
        "status": status, "id": id, "store_id": Uuid::from_u128(1), "source": "upload:a.pdf",
        "title": "A", "token_count": 12, "chunk_count": 1
    })
}

fn strata(url: &str) -> StrataClient {
    StrataClient::new(url, Secret::new("strata-token")).unwrap()
}

#[tokio::test]
async fn strata_duplicate_is_not_an_error() {
    let id = Uuid::now_v7();
    let (url, cap) = strata_fake((StatusCode::CREATED, doc_row("created", id))).await;
    let d = strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            "# A\n\ntext",
            "upload:a.pdf",
            Some("A"),
            &Origin::default(),
        )
        .await
        .expect("created");
    assert_eq!(d.document_id, id);
    assert!(!d.duplicate);
    {
        let cap = cap.lock().await;
        let (headers, body) = &cap[0];
        assert_eq!(headers["authorization"], "Bearer strata-token");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(
            headers["x-tenant-id"],
            Uuid::from_u128(0xA).to_string(),
            "the job's tenant, as a bare uuid"
        );
        let body: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            body,
            json!({"text": "# A\n\ntext", "source": "upload:a.pdf", "title": "A"})
        );
    }

    let (url, _) = strata_fake((StatusCode::OK, doc_row("existing", id))).await;
    let d = strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            "# A\n\ntext",
            "upload:a.pdf",
            None,
            &Origin::default(),
        )
        .await
        .expect("duplicate is success");
    assert_eq!(d.document_id, id);
    assert!(d.duplicate);
}

#[tokio::test]
async fn strata_delivery_carries_source_id_and_metadata() {
    let id = Uuid::now_v7();
    let (url, cap) = strata_fake((StatusCode::CREATED, doc_row("created", id))).await;
    let meta = json!({"source_id": "src-1", "attributes": {"Customer.id": "C-1"}});
    strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            "t",
            "s",
            None,
            &Origin {
                source_id: Some("src-1"),
                source_run: Some("run-7"),
                metadata: Some(&meta),
                knowledge: None,
            },
        )
        .await
        .expect("created");
    // A run without a source id is not sent (strata refuses `source_run` alone).
    strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            "t",
            "s",
            None,
            &Origin {
                source_run: Some("run-7"),
                ..Origin::default()
            },
        )
        .await
        .expect("created");
    let cap = cap.lock().await;
    let body: Value = serde_json::from_slice(&cap[0].1).unwrap();
    assert_eq!(
        body,
        json!({"text": "t", "source": "s", "source_id": "src-1", "source_run": "run-7",
               "metadata": meta})
    );
    let alone: Value = serde_json::from_slice(&cap[1].1).unwrap();
    assert_eq!(alone, json!({"text": "t", "source": "s"}));
}

#[tokio::test]
async fn strata_delivery_carries_a_reingest_copys_knowledge_fields() {
    let id = Uuid::now_v7();
    let (url, cap) = strata_fake((StatusCode::CREATED, doc_row("created", id))).await;
    let record_meta = json!({"row": 1});
    let knowledge = json!({
        "metadata": {"provenance": {"source_url": "https://x.eu/a", "license": "cc-by-4.0",
                                    "retrieved_at": "2026-10-01T00:00:00Z"}, "k": "v"},
        "topic_keys": ["hr/leave"],
        "supersedes": ["hr/old"],
        "provenance": {"source_url": "https://x.eu/a", "license": "cc-by-4.0",
                       "retrieved_at": "2026-10-01T00:00:00Z"},
    });
    strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            "t",
            "s",
            None,
            &Origin {
                source_id: Some("src-1"),
                source_run: None,
                metadata: Some(&record_meta),
                knowledge: Some(&knowledge),
            },
        )
        .await
        .expect("created");
    let cap = cap.lock().await;
    let body: Value = serde_json::from_slice(&cap[0].1).unwrap();
    assert_eq!(
        body,
        json!({
            "text": "t", "source": "s", "source_id": "src-1",
            "metadata": knowledge["metadata"], "topic_keys": ["hr/leave"],
            "supersedes": ["hr/old"], "provenance": knowledge["provenance"],
        }),
        "the source document's metadata wins; keys and provenance are carried"
    );
}

/// strata's platform-store licence check (422 `invalid_input` naming platform knowledge) is the
/// job's `provenance_rejected`, terminal; any other 422 stays `delivery_rejected`.
#[tokio::test]
async fn strata_licence_refusal_is_provenance_rejected() {
    let cases = [
        (
            "licence `proprietary` is not allowed for platform knowledge; allowed \
             (STRATA_PLATFORM_LICENSES): CC-BY-4.0",
            "provenance_rejected",
        ),
        (
            "store 0199e1a0 holds platform knowledge: provenance {source_url, license, \
             retrieved_at} is required",
            "provenance_rejected",
        ),
        ("supersedes: `X` is not a topic key", "delivery_rejected"),
    ];
    for (message, code) in cases {
        let (url, _) = strata_fake((
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": {"code": "invalid_input", "message": message}}),
        ))
        .await;
        let e = strata(&url)
            .deliver(
                Uuid::from_u128(0xA),
                Uuid::from_u128(1),
                "t",
                "s",
                None,
                &Origin::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(e.class, ErrorClass::InvalidRequest, "{message}");
        assert_eq!(e.code, code, "{message}");
        assert!(e.message.contains("invalid_input"), "{}", e.message);
    }
}

fn strata_err(code: &str) -> Value {
    json!({"error": {"code": code, "message": format!("{code} happened")}})
}

#[tokio::test]
async fn strata_leaf_full_is_terminal() {
    let cases = [
        (
            StatusCode::CONFLICT,
            "leaf_full",
            ErrorClass::InvalidRequest,
        ),
        (
            StatusCode::NOT_FOUND,
            "not_found",
            ErrorClass::InvalidRequest,
        ),
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            ErrorClass::InvalidRequest,
        ),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_input",
            ErrorClass::InvalidRequest,
        ),
        // Rulings R13: a lineage embed-model mismatch is terminal, like leaf_full.
        (
            StatusCode::CONFLICT,
            "embed_model_mismatch",
            ErrorClass::InvalidRequest,
        ),
        // a store of a published platform pack is frozen: the job's error, never retried
        (
            StatusCode::CONFLICT,
            "pack_published",
            ErrorClass::InvalidRequest,
        ),
        (StatusCode::CONFLICT, "conflict", ErrorClass::Retryable),
        (
            StatusCode::BAD_GATEWAY,
            "upstream_unavailable",
            ErrorClass::Retryable,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            ErrorClass::Retryable,
        ),
    ];
    for (status, code, class) in cases {
        let (url, _) = strata_fake((status, strata_err(code))).await;
        let e = strata(&url)
            .deliver(
                Uuid::from_u128(0xA),
                Uuid::from_u128(1),
                "t",
                "s",
                None,
                &Origin::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(e.class, class, "{status} {code}: {e:?}");
        if matches!(
            code,
            "leaf_full" | "embed_model_mismatch" | "pack_published"
        ) {
            assert_eq!(e.code, code);
        }
        assert!(!e.message.contains("strata-token"));
    }

    // Over strata's 16 MiB body limit: rejected without a request.
    let (url, cap) = strata_fake((StatusCode::CREATED, doc_row("created", Uuid::now_v7()))).await;
    let big = "a".repeat(16 * 1024 * 1024);
    let e = strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(1),
            &big,
            "s",
            None,
            &Origin::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.class, ErrorClass::InvalidRequest);
    assert!(cap.lock().await.is_empty());
}

#[tokio::test]
async fn strata_router_target_detected() {
    let (url, _) = strata_fake((StatusCode::CREATED, json!({}))).await;
    let c = strata(&url);
    assert_eq!(
        c.store_kind(Uuid::from_u128(0xA), Uuid::from_u128(1))
            .await
            .unwrap()
            .as_deref(),
        Some("leaf")
    );
    assert_eq!(
        c.store_kind(Uuid::from_u128(0xA), Uuid::from_u128(2))
            .await
            .unwrap()
            .as_deref(),
        Some("router")
    );
    assert_eq!(
        c.store_kind(Uuid::from_u128(0xA), Uuid::from_u128(3))
            .await
            .unwrap(),
        None
    );

    // A delivery to a router is answered 422 invalid_target: terminal.
    let (url, _) = strata_fake((
        StatusCode::UNPROCESSABLE_ENTITY,
        strata_err("invalid_target"),
    ))
    .await;
    let e = strata(&url)
        .deliver(
            Uuid::from_u128(0xA),
            Uuid::from_u128(2),
            "t",
            "s",
            None,
            &Origin::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (e.class, e.code.as_str()),
        (ErrorClass::InvalidRequest, "invalid_target")
    );
}

#[tokio::test]
async fn strata_store_kind_upstream_failure_is_an_error() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let e = strata(&format!("http://127.0.0.1:{port}"))
        .store_kind(Uuid::from_u128(0xA), Uuid::from_u128(1))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "upstream_unavailable");
}
