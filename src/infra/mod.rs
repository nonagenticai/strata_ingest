//! Infrastructure clients: storage_service (blobs + virus scan), docling-serve, webscraper, strata.
//!
//! Like the provider clients, each step-facing call makes exactly one attempt; retrying a
//! `Retryable` [`StepError`] is the step queue's job. Tokens only travel in headers and never
//! appear in an error message.

pub mod docling;
pub mod storage;
pub mod strata;
pub mod webscraper;

use crate::error::StepError;
use crate::providers::ErrorClass;

/// A step error attributed to `service` (stored as the step's `provider`).
pub(crate) fn step_err(
    class: ErrorClass,
    code: &str,
    service: &str,
    message: impl Into<String>,
) -> StepError {
    StepError {
        class,
        code: code.to_string(),
        message: message.into(),
        provider: Some(service.to_string()),
        retry_after_s: None,
    }
}

/// Builds the shared HTTP client for an infra service; `timeout` covers the whole exchange.
pub(crate) fn http_client(timeout: std::time::Duration) -> crate::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(timeout)
        .build()
        .map_err(|e| crate::error::IngestError::Config(format!("http client: {e}")))
}

/// `reqwest` errors carry the request URL; drop it so no query secret can leak into a step.
pub(crate) fn transport_message(e: reqwest::Error) -> String {
    let kind = if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect failed"
    } else {
        "transport error"
    };
    format!("{kind}: {}", e.without_url())
}

/// The start of a body for an error message: at most 300 chars, never the whole body.
pub(crate) fn snippet(body: &str) -> String {
    let t = body.trim();
    match t.char_indices().nth(300) {
        Some((i, _)) => format!("{}…", &t[..i]),
        None => t.to_string(),
    }
}
