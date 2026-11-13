//! Optional HTTP providers (OCR, vision captions, transcription) on the canonical contracts of
//! nonagenticai/provider_contracts `CONTRACTS.md`: `ocr` = Mistral `POST {base}/ocr`, `vision` =
//! OpenAI `POST {base}/chat/completions`, `transcribe` = OpenAI `POST {base}/audio/transcriptions`
//! (`base_url` includes the version segment, e.g. `https://api.mistral.ai/v1`).
//!
//! Every client makes exactly one attempt per call; retrying `Retryable` failures is the step
//! queue's job. Errors are [`ProviderError`]s classified by [`classify`]; transport failures and
//! timeouts are `Retryable`. Every call updates the shared [`ProviderHealth`]. Keys live only in a
//! sensitive `Authorization` header value: no `Debug`, error message or URL carries them.

pub mod crypto;
pub mod ocr;
pub mod registry;
pub mod transcribe;
pub mod vision;

use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::config::{ProviderCfg, Secret, redact_url};
use crate::error::{IngestError, StepError};

/// How a failed provider/step call is treated by the queue: only `Retryable` is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// 401/403: a bad or revoked key. Retrying cannot help.
    Auth,
    /// 404: the model or endpoint does not exist.
    NotFound,
    /// 408/429/5xx (and transport failures): try again after a backoff.
    Retryable,
    /// 400/413/422 and any other 4xx: the request itself is wrong.
    InvalidRequest,
}

impl ErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Auth => "auth",
            ErrorClass::NotFound => "not_found",
            ErrorClass::Retryable => "retryable",
            ErrorClass::InvalidRequest => "invalid_request",
        }
    }
}

/// Classifies an HTTP status from a provider. Callers only pass non-2xx statuses; anything
/// outside 4xx/5xx is treated as an invalid exchange.
pub fn classify(status: u16) -> ErrorClass {
    match status {
        401 | 403 => ErrorClass::Auth,
        404 => ErrorClass::NotFound,
        408 | 429 | 500..=599 => ErrorClass::Retryable,
        _ => ErrorClass::InvalidRequest,
    }
}

/// A failed provider call. `provider` is the dialect (`mistral`, `openai`); `status` is the HTTP
/// status when the provider answered, `None` for transport failures, timeouts and 2xx bodies that
/// break the contract. `message` never contains the key or the request URL.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{provider} {}{}: {message}", class.as_str(), status.map(|s| format!(" (status {s})")).unwrap_or_default())]
pub struct ProviderError {
    pub class: ErrorClass,
    pub provider: String,
    pub status: Option<u16>,
    pub message: String,
    /// `Retry-After` in seconds from a 429/503 answer (see [`retry_after`]).
    pub retry_after_s: Option<u64>,
}

impl From<ProviderError> for StepError {
    fn from(e: ProviderError) -> StepError {
        StepError {
            class: e.class,
            code: format!("provider_{}", e.class.as_str()),
            message: e.to_string(),
            provider: Some(e.provider),
            retry_after_s: e.retry_after_s,
        }
    }
}

/// The unit a capability is metered in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageUnit {
    Pages,
    Tokens,
    Seconds,
}

/// Metered usage of one call. `units` is `None` when the provider did not report it — never 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub units: Option<i64>,
    pub unit: UsageUnit,
}

impl Usage {
    /// `units * price` in micros; `None` when either is unknown (or the product overflows).
    pub fn cost_micros(&self, price: Option<i64>) -> Option<i64> {
        self.units?.checked_mul(price?)
    }
}

/// A capability's client behind its `concurrency` and `rpm` limits (`ProviderCfg`).
pub struct Limited<T> {
    inner: T,
    sem: Semaphore,
    rpm: Option<DefaultDirectRateLimiter>,
}

impl<T: fmt::Debug> fmt::Debug for Limited<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Limited")
            .field("inner", &self.inner)
            .field("available", &self.sem.available_permits())
            .field("rpm", &self.rpm.is_some())
            .finish()
    }
}

impl<T> Limited<T> {
    /// `concurrency` calls in flight at most (0 is treated as 1). `rpm` spaces call starts evenly
    /// (one every `60 s / rpm`, no burst), so a per-minute provider quota is never hit in a spike.
    pub fn new(inner: T, concurrency: usize, rpm: Option<u32>) -> Self {
        let rpm = rpm.and_then(NonZeroU32::new).map(|n| {
            let period = Duration::from_secs(60) / n.get();
            let quota = Quota::with_period(period)
                .expect("60 s / u32 is a non-zero period")
                .allow_burst(NonZeroU32::MIN);
            RateLimiter::direct(quota)
        });
        Self {
            inner,
            sem: Semaphore::new(concurrency.max(1)),
            rpm,
        }
    }

    /// Waits for a concurrency slot, then for the rate limit. Hold the permit for the whole call.
    /// The wait does not count against the step's deadline ([`crate::step_clock`]).
    pub async fn acquire(&self) -> (SemaphorePermit<'_>, &T) {
        crate::step_clock::waiting(async {
            let permit = self
                .sem
                .acquire()
                .await
                .expect("the semaphore is never closed");
            if let Some(rpm) = &self.rpm {
                rpm.until_ready().await;
            }
            (permit, &self.inner)
        })
        .await
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }
}

/// A provider capability, as configured in `providers.toml` and reported on `/health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Ocr,
    Vision,
    Transcribe,
}

impl Capability {
    pub const ALL: [Capability; 3] = [Capability::Ocr, Capability::Vision, Capability::Transcribe];

    pub fn as_str(self) -> &'static str {
        match self {
            Capability::Ocr => "ocr",
            Capability::Vision => "vision",
            Capability::Transcribe => "transcribe",
        }
    }

    pub fn parse(s: &str) -> Option<Capability> {
        Capability::ALL.into_iter().find(|c| c.as_str() == s)
    }

    /// Longest `timeout_ms` a tenant may register: below the step's run ceiling
    /// (`limits::step_deadline`), so a slow provider fails as a classified timeout.
    pub fn max_tenant_timeout_ms(self) -> u64 {
        match self {
            Capability::Ocr => 300_000,
            Capability::Vision => 110_000,
            Capability::Transcribe => 300_000,
        }
    }

    fn slot(self) -> usize {
        self as usize
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One capability's status for `/health`: times and a class only — never URLs, keys or messages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CapabilityHealth {
    pub last_success: Option<DateTime<Utc>>,
    pub last_error_class: Option<ErrorClass>,
    pub last_error_at: Option<DateTime<Utc>>,
}

/// Per-capability last success / last error, updated by every client call.
#[derive(Debug, Default)]
pub struct ProviderHealth {
    slots: Mutex<[CapabilityHealth; 3]>,
}

impl ProviderHealth {
    pub fn get(&self, cap: Capability) -> CapabilityHealth {
        self.lock()[cap.slot()]
    }

    pub fn record_success(&self, cap: Capability) {
        self.lock()[cap.slot()].last_success = Some(Utc::now());
    }

    pub fn record_error(&self, cap: Capability, class: ErrorClass) {
        let mut slots = self.lock();
        let s = &mut slots[cap.slot()];
        s.last_error_class = Some(class);
        s.last_error_at = Some(Utc::now());
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, [CapabilityHealth; 3]> {
        // The data is plain timestamps; a panic elsewhere cannot leave it inconsistent.
        self.slots.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Longest provider error text kept in a [`ProviderError`] message.
const MAX_MESSAGE_CHARS: usize = 500;

/// Largest provider response body read; a longer one is an invalid response and is never parsed
/// (a provider cannot make the worker buffer an unbounded body). Eight OCR'd pages of markdown or
/// a 10-minute verbose transcript are far below it.
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// The request body of one provider call.
pub(crate) enum Body {
    Json(Vec<u8>),
    Multipart(reqwest::multipart::Form),
}

/// HTTP plumbing shared by the three clients: one attempt, bearer auth in a sensitive header,
/// `timeout_ms`, no redirects (a redirect could carry the key to another host), status
/// classification, key redaction, health recording.
pub(crate) struct Http {
    capability: Capability,
    provider: String,
    model: String,
    base_url: String,
    timeout_ms: u64,
    http: reqwest::Client,
    auth: HeaderValue,
    /// Only to scrub a provider that echoes the key back into an error body.
    key: Secret,
    health: Arc<ProviderHealth>,
}

impl fmt::Debug for Http {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Http")
            .field("capability", &self.capability)
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("base_url", &redact_url(&self.base_url))
            .field("timeout_ms", &self.timeout_ms)
            .field("auth", &"Bearer ***")
            .finish_non_exhaustive()
    }
}

impl Http {
    /// `guarded`: a tenant registration — resolve through [`crate::netguard::GuardedResolver`]
    /// (never connect to a non-public address, also after a DNS change) and ignore proxy env
    /// vars. `providers.toml` defaults are operator-set and unguarded.
    pub(crate) fn new(
        capability: Capability,
        dialects: &[&str],
        cfg: &ProviderCfg,
        health: Arc<ProviderHealth>,
        guarded: bool,
    ) -> crate::Result<Self> {
        let cap = capability.as_str();
        if !dialects.contains(&cfg.dialect.as_str()) {
            return Err(IngestError::Config(format!(
                "providers.{cap}: unsupported dialect {:?}",
                cfg.dialect
            )));
        }
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .redirect(reqwest::redirect::Policy::none());
        if guarded {
            builder = builder
                .no_proxy()
                .dns_resolver(crate::netguard::GuardedResolver);
        }
        let http = builder
            .build()
            .map_err(|e| IngestError::Config(format!("{cap} http client: {}", e.without_url())))?;
        let mut auth =
            HeaderValue::from_str(&format!("Bearer {}", cfg.api_key.expose())).map_err(|_| {
                IngestError::Config(format!(
                    "providers.{cap}.api_key contains invalid header characters"
                ))
            })?;
        auth.set_sensitive(true);
        Ok(Self {
            capability,
            provider: cfg.dialect.clone(),
            model: cfg.model.clone(),
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            timeout_ms: cfg.timeout_ms,
            http,
            auth,
            key: cfg.api_key.clone(),
            health,
        })
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    /// A `ProviderError` from this provider; the message is scrubbed of the key and truncated.
    pub(crate) fn error(
        &self,
        class: ErrorClass,
        status: Option<u16>,
        message: impl fmt::Display,
    ) -> ProviderError {
        let mut message = message.to_string();
        let key = self.key.expose();
        if !key.is_empty() {
            message = message.replace(key, "***");
        }
        if let Some((cut, _)) = message.char_indices().nth(MAX_MESSAGE_CHARS) {
            message.truncate(cut);
            message.push('…');
        }
        ProviderError {
            class,
            provider: self.provider.clone(),
            status,
            message,
            retry_after_s: None,
        }
    }

    /// A 2xx body that breaks the contract. Not retried: a provider that answers in the wrong
    /// shape does so every time (a body cut short by the network is a transport error instead).
    pub(crate) fn invalid_response(&self, detail: impl fmt::Display) -> ProviderError {
        self.error(
            ErrorClass::InvalidRequest,
            None,
            format!("invalid provider response: {detail}"),
        )
    }

    /// Records the outcome of a whole client call on `/health` and passes it through.
    pub(crate) fn record<T>(&self, r: Result<T, ProviderError>) -> Result<T, ProviderError> {
        match &r {
            Ok(_) => self.health.record_success(self.capability),
            Err(e) => {
                self.health.record_error(self.capability, e.class);
                tracing::warn!(
                    capability = self.capability.as_str(),
                    provider = %self.provider,
                    model = %self.model,
                    class = e.class.as_str(),
                    status = e.status,
                    error = %e.message,
                    "provider call failed"
                );
            }
        }
        r
    }

    /// POSTs `body` to `{base_url}{path}` once and decodes the 2xx JSON response as `T`.
    pub(crate) async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Body,
    ) -> Result<T, ProviderError> {
        let req = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header(AUTHORIZATION, self.auth.clone());
        let req = match body {
            Body::Json(bytes) => req
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(bytes),
            Body::Multipart(form) => req.multipart(form),
        };
        let resp = req.send().await.map_err(|e| self.transport(e))?;
        let status = resp.status();
        let wait = retry_after(status.as_u16(), resp.headers());
        let bytes = self.read_capped(resp).await?;
        if !status.is_success() {
            let code = status.as_u16();
            let mut e = self.error(classify(code), Some(code), error_text(&bytes));
            e.retry_after_s = wait;
            return Err(e);
        }
        serde_json::from_slice(&bytes).map_err(|e| self.invalid_response(e))
    }

    /// The body, streamed, refused once it exceeds [`MAX_RESPONSE_BYTES`] (also up front from a
    /// declared `Content-Length`).
    async fn read_capped(&self, mut resp: reqwest::Response) -> Result<Vec<u8>, ProviderError> {
        let too_large =
            || self.invalid_response(format!("body exceeds {MAX_RESPONSE_BYTES} bytes"));
        if resp
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(too_large());
        }
        let mut out = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(|e| self.transport(e))? {
            if out.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(too_large());
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    /// Transport failure or our own timeout: always `Retryable`, no URL in the message.
    fn transport(&self, e: reqwest::Error) -> ProviderError {
        if e.is_timeout() {
            return self.error(
                ErrorClass::Retryable,
                None,
                format!("request timed out after {} ms", self.timeout_ms),
            );
        }
        let e = e.without_url();
        let mut msg = e.to_string();
        let mut src = std::error::Error::source(&e);
        while let Some(s) = src {
            msg.push_str(": ");
            msg.push_str(&s.to_string());
            src = s.source();
        }
        self.error(ErrorClass::Retryable, None, msg)
    }
}

/// The `Retry-After` of a 429 or 503 answer, in whole seconds: delta-seconds or an HTTP date
/// (a date in the past is 0). `None` for any other status, or a missing/unparsable header.
pub fn retry_after(status: u16, headers: &reqwest::header::HeaderMap) -> Option<u64> {
    if !matches!(status, 429 | 503) {
        return None;
    }
    let v = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(secs);
    }
    let at = DateTime::parse_from_rfc2822(v).ok()?.with_timezone(&Utc);
    Some(u64::try_from((at - Utc::now()).num_seconds()).unwrap_or(0))
}

/// The human-readable part of a provider error body: the contract's `error.message`, else the
/// common `message` / `detail` fields, else the raw body.
fn error_text(body: &[u8]) -> String {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) {
        for ptr in ["/error/message", "/message", "/detail", "/error"] {
            if let Some(s) = v.pointer(ptr).and_then(|x| x.as_str()) {
                return s.to_string();
            }
        }
        if let Some(d) = v.get("detail") {
            return d.to_string();
        }
    }
    let raw = String::from_utf8_lossy(body);
    let raw = raw.trim();
    if raw.is_empty() {
        "empty error body".to_string()
    } else {
        raw.to_string()
    }
}

/// `data:<mime>;base64,<bytes>`.
pub(crate) fn data_url(mime: &str, bytes: &[u8]) -> String {
    use base64::Engine;
    format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_statuses() {
        assert_eq!(classify(401), ErrorClass::Auth);
        assert_eq!(classify(403), ErrorClass::Auth);
        assert_eq!(classify(404), ErrorClass::NotFound);
        for s in [408, 429, 500, 502, 503, 599] {
            assert_eq!(classify(s), ErrorClass::Retryable, "{s}");
        }
        for s in [400, 413, 422, 409, 451] {
            assert_eq!(classify(s), ErrorClass::InvalidRequest, "{s}");
        }
    }

    #[test]
    fn serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(ErrorClass::InvalidRequest).unwrap(),
            serde_json::json!("invalid_request")
        );
        for c in [
            ErrorClass::Auth,
            ErrorClass::NotFound,
            ErrorClass::Retryable,
            ErrorClass::InvalidRequest,
        ] {
            assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
        }
    }

    #[test]
    fn error_text_prefers_contract_message() {
        assert_eq!(
            error_text(br#"{"error":{"message":"m","type":"auth","code":null}}"#),
            "m"
        );
        assert_eq!(error_text(br#"{"message":"mistral says"}"#), "mistral says");
        assert_eq!(
            error_text(br#"{"detail":[{"loc":["body"],"msg":"x"}]}"#),
            r#"[{"loc":["body"],"msg":"x"}]"#
        );
        assert_eq!(error_text(b"  upstream down \n"), "upstream down");
        assert_eq!(error_text(b""), "empty error body");
    }

    #[test]
    fn retry_after_reads_seconds_and_dates_on_429_and_503_only() {
        use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
        let h = |v: &str| {
            let mut m = HeaderMap::new();
            m.insert(RETRY_AFTER, HeaderValue::from_str(v).unwrap());
            m
        };
        assert_eq!(retry_after(429, &h("120")), Some(120));
        assert_eq!(retry_after(503, &h(" 7 ")), Some(7));
        assert_eq!(retry_after(500, &h("120")), None, "only 429/503");
        assert_eq!(retry_after(429, &HeaderMap::new()), None);
        assert_eq!(retry_after(429, &h("soon")), None);
        let future = (Utc::now() + chrono::Duration::seconds(90)).to_rfc2822();
        let got = retry_after(429, &h(&future)).expect("date");
        assert!((85..=90).contains(&got), "{got}");
        assert_eq!(
            retry_after(503, &h("Wed, 21 Oct 2015 07:28:00 GMT")),
            Some(0),
            "a past date"
        );
    }

    #[test]
    fn capability_slots_are_distinct() {
        let mut seen: Vec<usize> = Capability::ALL.iter().map(|c| c.slot()).collect();
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2]);
    }
}
