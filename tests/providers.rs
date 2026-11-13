//! Provider clients (ocr, vision, transcribe) against the canonical contracts
//! (provider_contracts CONTRACTS.md): the `pcontract fake` for behaviour, in-process axum mocks
//! for request capture and in-flight counting, recorded-shape fixtures for real-provider bodies.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes as AxumBytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::Mutex;

use strata_ingest::config::{ProviderCfg, Region, Secret};
use strata_ingest::error::StepError;
use strata_ingest::providers::ocr::{OcrClient, OcrInput};
use strata_ingest::providers::transcribe::TranscribeClient;
use strata_ingest::providers::vision::{CAPTION_PROMPT, VisionClient};
use strata_ingest::providers::{
    Capability, ErrorClass, Limited, ProviderError, ProviderHealth, Usage, UsageUnit, classify,
};
use strata_ingest::testutil::spawn;

const PDF: &[u8] = include_bytes!("fixtures/providers/three_page.pdf");
/// 1x1 PNG.
const PNG: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0xda, 0x63, 0xfc, 0xcf, 0xc0, 0x50,
    0x0f, 0x00, 0x04, 0x85, 0x01, 0x80, 0x84, 0xa9, 0x8c, 0x21, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
    0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
];

fn cfg(base_url: &str, dialect: &str, model: &str, key: &str) -> ProviderCfg {
    ProviderCfg {
        base_url: base_url.to_string(),
        dialect: dialect.to_string(),
        model: model.to_string(),
        api_key: Secret::new(key),
        regions: vec![Region::Eu],
        zdr: true,
        timeout_ms: 30_000,
        concurrency: 4,
        rpm: None,
        price_micros_per_unit: None,
    }
}

fn health() -> Arc<ProviderHealth> {
    Arc::new(ProviderHealth::default())
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/providers/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("fixture")
}

/// A 16 kHz mono 16-bit PCM WAV of `seconds` of silence, header written by hand.
fn wav(seconds: u32) -> Bytes {
    let rate: u32 = 16_000;
    let data_len = rate * 2 * seconds;
    let mut v = Vec::with_capacity(44 + data_len as usize);
    v.extend_from_slice(b"RIFF");
    v.extend_from_slice(&(36 + data_len).to_le_bytes());
    v.extend_from_slice(b"WAVEfmt ");
    v.extend_from_slice(&16u32.to_le_bytes());
    v.extend_from_slice(&1u16.to_le_bytes()); // PCM
    v.extend_from_slice(&1u16.to_le_bytes()); // mono
    v.extend_from_slice(&rate.to_le_bytes());
    v.extend_from_slice(&(rate * 2).to_le_bytes());
    v.extend_from_slice(&2u16.to_le_bytes());
    v.extend_from_slice(&16u16.to_le_bytes());
    v.extend_from_slice(b"data");
    v.extend_from_slice(&data_len.to_le_bytes());
    v.resize(44 + data_len as usize, 0);
    Bytes::from(v)
}

/// A mock that records every request body and answers `status` + `body`.
#[derive(Clone)]
struct Capture {
    bodies: Arc<Mutex<Vec<(HeaderMap, AxumBytes)>>>,
    status: StatusCode,
    body: String,
}

async fn capture_handler(
    State(c): State<Capture>,
    headers: HeaderMap,
    body: AxumBytes,
) -> (StatusCode, String) {
    c.bodies.lock().await.push((headers, body));
    (c.status, c.body.clone())
}

async fn capture_mock(path: &str, status: StatusCode, body: String) -> (String, Capture) {
    let c = Capture {
        bodies: Arc::new(Mutex::new(Vec::new())),
        status,
        body,
    };
    let app = Router::new()
        .route(path, post(capture_handler))
        .with_state(c.clone());
    (format!("{}/v1", spawn(app).await), c)
}

#[test]
fn classify_maps_spec_codes() {
    for (s, c) in [
        (401, ErrorClass::Auth),
        (403, ErrorClass::Auth),
        (404, ErrorClass::NotFound),
        (408, ErrorClass::Retryable),
        (429, ErrorClass::Retryable),
        (500, ErrorClass::Retryable),
        (502, ErrorClass::Retryable),
        (503, ErrorClass::Retryable),
        (504, ErrorClass::Retryable),
        (400, ErrorClass::InvalidRequest),
        (413, ErrorClass::InvalidRequest),
        (422, ErrorClass::InvalidRequest),
    ] {
        assert_eq!(classify(s), c, "status {s}");
    }
}

#[test]
fn provider_error_becomes_step_error_with_snake_case_code() {
    for (class, code) in [
        (ErrorClass::Auth, "provider_auth"),
        (ErrorClass::NotFound, "provider_not_found"),
        (ErrorClass::Retryable, "provider_retryable"),
        (ErrorClass::InvalidRequest, "provider_invalid_request"),
    ] {
        let e = ProviderError {
            class,
            provider: "mistral".into(),
            status: Some(418),
            message: "boom".into(),
            retry_after_s: None,
        };
        let s: StepError = e.into();
        assert_eq!(s.class, class);
        assert_eq!(s.code, code);
        assert_eq!(s.provider.as_deref(), Some("mistral"));
        assert!(s.message.contains("boom"), "{}", s.message);
    }
}

#[test]
fn usage_cost_is_none_unless_units_and_price_are_known() {
    let u = Usage {
        units: Some(3),
        unit: UsageUnit::Pages,
    };
    assert_eq!(u.cost_micros(Some(1_000)), Some(3_000));
    assert_eq!(u.cost_micros(None), None);
    let none = Usage {
        units: None,
        unit: UsageUnit::Tokens,
    };
    assert_eq!(none.cost_micros(Some(1_000)), None);
    let huge = Usage {
        units: Some(i64::MAX),
        unit: UsageUnit::Seconds,
    };
    assert_eq!(
        huge.cost_micros(Some(2)),
        None,
        "overflow is unknown, not wrapped"
    );
}


#[tokio::test]
async fn ocr_sends_pages_and_data_url() {
    let (base, cap) = capture_mock("/v1/ocr", StatusCode::OK, fixture("mistral_ocr.json")).await;
    let client = OcrClient::new(
        &cfg(&base, "mistral", "mistral-ocr-2505", "k-ocr"),
        health(),
    )
    .unwrap();
    let out = client
        .ocr(OcrInput {
            bytes: Bytes::from_static(PDF),
            mime: "application/pdf".into(),
            pages: Some(vec![0, 2]),
        })
        .await
        .expect("ocr");
    assert_eq!(out.pages.len(), 2);
    assert!(out.pages[1].markdown.contains("| Q2 | 4.3 |"));
    assert_eq!(out.usage.units, Some(2));

    let bodies = cap.bodies.lock().await;
    assert_eq!(bodies.len(), 1, "one attempt per call");
    let (headers, raw) = &bodies[0];
    assert_eq!(headers["authorization"], "Bearer k-ocr");
    let body: Value = serde_json::from_slice(raw).unwrap();
    use base64::Engine;
    let expected_url = format!(
        "data:application/pdf;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(PDF)
    );
    assert_eq!(body["model"], "mistral-ocr-2505");
    assert_eq!(body["document"]["type"], "document_url");
    assert_eq!(body["document"]["document_url"], expected_url.as_str());
    assert_eq!(body["pages"], serde_json::json!([0, 2]));
    assert_eq!(body["include_image_base64"], false);
    drop(bodies);

    // Images go as `image_url`, and no `pages` key when all pages are wanted.
    client
        .ocr(OcrInput {
            bytes: Bytes::from_static(PNG),
            mime: "image/png".into(),
            pages: None,
        })
        .await
        .expect("image ocr");
    let bodies = cap.bodies.lock().await;
    let body: Value = serde_json::from_slice(&bodies[1].1).unwrap();
    assert_eq!(body["document"]["type"], "image_url");
    assert!(
        body["document"]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,iVBOR")
    );
    assert!(body.get("pages").is_none(), "{body}");
}

#[tokio::test]
async fn missing_usage_is_none_not_zero() {
    let (base, _cap) = capture_mock(
        "/v1/ocr",
        StatusCode::OK,
        fixture("mistral_ocr_no_usage.json"),
    )
    .await;
    let client = OcrClient::new(&cfg(&base, "mistral", "m", "k"), health()).unwrap();
    let out = client
        .ocr(OcrInput {
            bytes: Bytes::from_static(PDF),
            mime: "application/pdf".into(),
            pages: None,
        })
        .await
        .expect("ocr");
    assert_eq!(out.pages.len(), 1);
    assert_eq!(out.usage.units, None);
    assert_eq!(out.usage.cost_micros(Some(1_000)), None);

    // Same for chat without `usage` and transcription without `usage`.
    let mut chat: Value = serde_json::from_str(&fixture("openai_chat.json")).unwrap();
    chat.as_object_mut().unwrap().remove("usage");
    let (base, _) = capture_mock("/v1/chat/completions", StatusCode::OK, chat.to_string()).await;
    let vision = VisionClient::new(&cfg(&base, "openai", "m", "k"), health()).unwrap();
    let (_, usage) = vision.caption(PNG, "image/png").await.expect("caption");
    assert_eq!(usage.units, None);

    let mut tr: Value = serde_json::from_str(&fixture("openai_transcribe_verbose.json")).unwrap();
    tr.as_object_mut().unwrap().remove("usage");
    let (base, _) = capture_mock("/v1/audio/transcriptions", StatusCode::OK, tr.to_string()).await;
    let t = TranscribeClient::new(&cfg(&base, "openai", "m", "k"), health()).unwrap();
    let out = t
        .transcribe(wav(1), "a.wav", None)
        .await
        .expect("transcribe");
    assert_eq!(out.usage.units, None);
    assert_eq!(out.duration_s, Some(12.3));
}


#[tokio::test]
async fn vision_sends_image_part_and_exact_prompt() {
    assert_eq!(
        CAPTION_PROMPT,
        "Describe this figure factually in at most 120 words. Transcribe any visible text, labels and numbers exactly. Do not speculate."
    );
    let (base, cap) = capture_mock(
        "/v1/chat/completions",
        StatusCode::OK,
        fixture("openai_chat.json"),
    )
    .await;
    let client =
        VisionClient::new(&cfg(&base, "openai", "gpt-4.1-mini", "k-vis"), health()).unwrap();
    let (text, usage) = client.caption(PNG, "image/png").await.expect("caption");
    assert!(text.starts_with("A bar chart titled"), "{text}");
    assert_eq!(usage.units, Some(1182));
    assert_eq!(usage.unit, UsageUnit::Tokens);

    let bodies = cap.bodies.lock().await;
    assert_eq!(bodies.len(), 1);
    let body: Value = serde_json::from_slice(&bodies[0].1).unwrap();
    assert_eq!(body["model"], "gpt-4.1-mini");
    assert_eq!(body["max_tokens"], 300);
    let msgs = body["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["role"], "user");
    let parts = msgs[0]["content"].as_array().unwrap();
    assert_eq!(parts.len(), 2, "{body}");
    assert_eq!(parts[0]["type"], "text");
    assert_eq!(parts[0]["text"], CAPTION_PROMPT);
    assert_eq!(parts[1]["type"], "image_url");
    use base64::Engine;
    assert_eq!(
        parts[1]["image_url"]["url"],
        format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(PNG)
        )
        .as_str()
    );
}


#[tokio::test]
async fn transcribe_sends_verbose_json_multipart() {
    let (base, cap) = capture_mock(
        "/v1/audio/transcriptions",
        StatusCode::OK,
        fixture("openai_transcribe_verbose.json"),
    )
    .await;
    let client =
        TranscribeClient::new(&cfg(&base, "openai", "whisper-1", "k-tr"), health()).unwrap();
    let out = client
        .transcribe(wav(1), "speech.wav", Some("en"))
        .await
        .expect("transcribe");
    assert_eq!(out.segments.len(), 2);
    assert_eq!(out.segments[1].start_s, 4.2);
    assert_eq!(out.segments[1].text, " Revenue grew twelve percent.");
    assert_eq!(
        out.usage.units,
        Some(13),
        "12.3 s bills as 13 whole seconds"
    );

    let bodies = cap.bodies.lock().await;
    let (headers, raw) = &bodies[0];
    let ct = headers["content-type"].to_str().unwrap();
    assert!(ct.starts_with("multipart/form-data"), "{ct}");
    let text = String::from_utf8_lossy(raw);
    for needle in [
        "name=\"file\"; filename=\"speech.wav\"",
        "name=\"model\"\r\n\r\nwhisper-1\r\n",
        "name=\"response_format\"\r\n\r\nverbose_json\r\n",
        "name=\"timestamp_granularities[]\"\r\n\r\nsegment\r\n",
        "name=\"language\"\r\n\r\nen\r\n",
    ] {
        assert!(text.contains(needle), "missing {needle:?}");
    }
}


#[tokio::test]
async fn retry_after_of_a_429_reaches_the_step_error() {
    let app = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "42")],
                r#"{"error":{"message":"slow down","type":"rate_limit","code":null}}"#,
            )
        }),
    );
    let base = format!("{}/v1", spawn(app).await);
    let e = VisionClient::new(&cfg(&base, "openai", "m", "k"), health())
        .unwrap()
        .caption(PNG, "image/png")
        .await
        .expect_err("429");
    assert_eq!((e.class, e.status), (ErrorClass::Retryable, Some(429)));
    assert_eq!(e.retry_after_s, Some(42));
    let s: StepError = e.into();
    assert_eq!(s.retry_after_s, Some(42));
}


#[derive(Clone, Default)]
struct InFlight {
    now: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
}

async fn slow_chat(State(s): State<InFlight>) -> (StatusCode, String) {
    let n = s.now.fetch_add(1, Ordering::SeqCst) + 1;
    s.max.fetch_max(n, Ordering::SeqCst);
    s.total.fetch_add(1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    s.now.fetch_sub(1, Ordering::SeqCst);
    (StatusCode::OK, fixture("openai_chat.json"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_is_respected() {
    let state = InFlight::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(slow_chat))
        .with_state(state.clone());
    let base = format!("{}/v1", spawn(app).await);
    let mut c = cfg(&base, "openai", "m", "k");
    c.concurrency = 2;
    let limited = Arc::new(Limited::new(
        VisionClient::new(&c, health()).unwrap(),
        c.concurrency,
        c.rpm,
    ));
    let mut tasks = Vec::new();
    for _ in 0..5 {
        let l = limited.clone();
        tasks.push(tokio::spawn(async move {
            let (_permit, client) = l.acquire().await;
            client.caption(PNG, "image/png").await
        }));
    }
    for t in tasks {
        t.await.unwrap().expect("caption");
    }
    assert_eq!(state.total.load(Ordering::SeqCst), 5);
    assert_eq!(
        state.max.load(Ordering::SeqCst),
        2,
        "never more than 2 in flight, and 2 did run together"
    );
}

#[tokio::test]
async fn rpm_limit_spaces_calls() {
    let (base, cap) = capture_mock(
        "/v1/chat/completions",
        StatusCode::OK,
        fixture("openai_chat.json"),
    )
    .await;
    // 120 rpm = one call per 500 ms after the first.
    let limited = Limited::new(
        VisionClient::new(&cfg(&base, "openai", "m", "k"), health()).unwrap(),
        4,
        Some(120),
    );
    let t0 = Instant::now();
    for _ in 0..3 {
        let (_p, client) = limited.acquire().await;
        client.caption(PNG, "image/png").await.expect("caption");
    }
    assert_eq!(cap.bodies.lock().await.len(), 3);
    let took = t0.elapsed();
    assert!(
        took >= Duration::from_millis(900),
        "3 calls at 120 rpm take >= ~1 s: {took:?}"
    );
}

#[tokio::test]
async fn health_records_last_success_and_error_class() {
    let (ok_base, _) = capture_mock(
        "/v1/chat/completions",
        StatusCode::OK,
        fixture("openai_chat.json"),
    )
    .await;
    let (bad_base, _) = capture_mock(
        "/v1/chat/completions",
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow down","type":"retryable","code":null}}"#.into(),
    )
    .await;
    let h = health();
    for cap in [Capability::Ocr, Capability::Vision, Capability::Transcribe] {
        let s = h.get(cap);
        assert!(
            s.last_success.is_none() && s.last_error_class.is_none(),
            "{cap:?} starts empty"
        );
    }
    let before = chrono::Utc::now();
    VisionClient::new(&cfg(&ok_base, "openai", "m", "k"), h.clone())
        .unwrap()
        .caption(PNG, "image/png")
        .await
        .expect("ok");
    let s = h.get(Capability::Vision);
    assert!(s.last_success.is_some_and(|t| t >= before), "{s:?}");
    assert_eq!(s.last_error_class, None);

    VisionClient::new(&cfg(&bad_base, "openai", "m", "k"), h.clone())
        .unwrap()
        .caption(PNG, "image/png")
        .await
        .expect_err("429");
    let s2 = h.get(Capability::Vision);
    assert_eq!(s2.last_error_class, Some(ErrorClass::Retryable));
    assert!(s2.last_error_at.is_some());
    assert_eq!(
        s2.last_success, s.last_success,
        "an error keeps the last success time"
    );
    // Other capabilities untouched.
    assert!(h.get(Capability::Ocr).last_success.is_none());
    assert!(h.get(Capability::Ocr).last_error_class.is_none());

    // The JSON for /health carries times and the class only.
    let v = serde_json::to_value(h.get(Capability::Vision)).unwrap();
    assert_eq!(v["last_error_class"], "retryable");
    assert!(v["last_success"].is_string());
    assert!(
        !v.to_string().contains("127.0.0.1"),
        "no URLs in health: {v}"
    );
}
