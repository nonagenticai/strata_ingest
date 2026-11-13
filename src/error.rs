//! Domain error type, its HTTP mapping (`code()` / `status()`) and the axum response.
//! Body: `{"error":{"code","message"}}`. 500s log the full error and return a generic message.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::providers::ErrorClass;

/// Every fallible strata-ingest operation returns this error.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("not found")]
    NotFound,
    #[error("unknown store")]
    UnknownStore,
    #[error("target store is not a leaf")]
    InvalidTarget,
    /// A re-ingest copy's target is unusable (the source itself, not a root, another kind): 422
    /// `invalid_target`.
    #[error("invalid target store: {0}")]
    InvalidCopyTarget(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A malformed API request (unknown field, bad value, wrong part order): 400.
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    /// A `source.kind` this service cannot ingest (422 `unsupported_source_kind`).
    #[error("unsupported source kind: {0}")]
    UnsupportedSourceKind(String),
    #[error("payload too large")]
    PayloadTooLarge,
    /// The client stopped sending the request body (upload idle timeout): 408.
    #[error("request body stalled: {0}")]
    RequestTimeout(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("conflict: {0}")]
    Conflict(String),
    /// The store is in a published platform pack's subtree, which strata freezes (strata_rag
    /// `db::platform::assert_writable`): 409 `pack_published`, never retried.
    #[error("store is in a published platform pack: {0}")]
    PackPublished(String),
    /// A re-ingest copy's target must be an empty store: 409 `target_not_empty`.
    #[error("target store is not empty: {0}")]
    TargetNotEmpty(String),
    /// `DELETE` of a crawl / sitemap child (page) job: refused so the parent's `child_job_ids`
    /// (and a source run's document count) stay whole; delete the parent, which cascades.
    /// 409 `child_of_parent_job`, with `parent_job_id` in the error body.
    #[error(
        "job is a child of crawl / sitemap job {parent_job_id}; delete the parent instead \
         (DELETE /api/ingest/jobs/{parent_job_id} removes its children with it)"
    )]
    ChildOfParentJob { parent_job_id: uuid::Uuid },
    /// A step write from a worker whose claim is no longer current: the step finished, or its
    /// lease expired and it was reclaimed (a newer `attempt` owns it). The worker must drop its
    /// result and stop; nothing was written.
    #[error("lease lost on step {step} (attempt {attempt})")]
    LeaseLost { step: i64, attempt: i32 },
    /// Too much work in flight here (e.g. every upload spool slot is taken): 503 with
    /// `Retry-After`.
    #[error("busy: {message}")]
    Busy { message: String, retry_after_s: u64 },
    /// The service is starting (the database is not reachable or not migrated yet): 503 with
    /// `Retry-After`.
    #[error("not ready: the service is starting (database not available yet)")]
    NotReady,
    #[error("{service} unavailable: {message}")]
    Upstream {
        service: &'static str,
        message: String,
    },
    /// A tenant provider registration failed validation or its live probe; `code` is the specific
    /// reason (`region_not_allowed`, `zdr_required`, `dialect_not_allowed`, `invalid_provider`,
    /// `provider_host_not_allowed`, `api_key_required`, `probe_failed`): 422. Never a key.
    #[error("provider rejected: {message}")]
    ProviderRejected { code: &'static str, message: String },
    /// Tenant provider keys cannot be sealed or opened: `STRATA_INGEST_PROVIDER_KEY` is unset
    /// (503 `provider_key_unconfigured`).
    #[error("provider keys unavailable: {0}")]
    ProviderKeyUnavailable(String),
    #[error("config error: {0}")]
    Config(String),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl IngestError {
    /// Stable machine-readable code used in `{"error":{"code":...}}` bodies.
    pub fn code(&self) -> &'static str {
        match self {
            IngestError::NotFound => "not_found",
            IngestError::UnknownStore => "unknown_store",
            IngestError::InvalidTarget | IngestError::InvalidCopyTarget(_) => "invalid_target",
            IngestError::InvalidInput(_) => "invalid_input",
            IngestError::BadRequest(_) => "bad_request",
            IngestError::UnsupportedType(_) => "unsupported_type",
            IngestError::UnsupportedSourceKind(_) => "unsupported_source_kind",
            IngestError::PayloadTooLarge => "payload_too_large",
            IngestError::RequestTimeout(_) => "request_timeout",
            IngestError::Unauthorized => "unauthorized",
            IngestError::Conflict(_) => "conflict",
            IngestError::PackPublished(_) => "pack_published",
            IngestError::TargetNotEmpty(_) => "target_not_empty",
            IngestError::ChildOfParentJob { .. } => "child_of_parent_job",
            IngestError::LeaseLost { .. } => "lease_lost",
            IngestError::Upstream { .. } => "upstream_unavailable",
            IngestError::Busy { .. } => "busy",
            IngestError::NotReady => "not_ready",
            IngestError::ProviderRejected { code, .. } => code,
            IngestError::ProviderKeyUnavailable(_) => "provider_key_unconfigured",
            IngestError::Config(_)
            | IngestError::Db(_)
            | IngestError::Migrate(_)
            | IngestError::Io(_) => "internal",
        }
    }

    /// HTTP status for this error.
    pub fn status(&self) -> u16 {
        match self {
            IngestError::NotFound | IngestError::UnknownStore => 404,
            IngestError::InvalidTarget
            | IngestError::InvalidCopyTarget(_)
            | IngestError::InvalidInput(_)
            | IngestError::UnsupportedType(_)
            | IngestError::UnsupportedSourceKind(_)
            | IngestError::ProviderRejected { .. } => 422,
            IngestError::BadRequest(_) => 400,
            IngestError::PayloadTooLarge => 413,
            IngestError::RequestTimeout(_) => 408,
            IngestError::Unauthorized => 401,
            IngestError::Conflict(_)
            | IngestError::PackPublished(_)
            | IngestError::TargetNotEmpty(_)
            | IngestError::ChildOfParentJob { .. }
            | IngestError::LeaseLost { .. } => 409,
            IngestError::Upstream { .. } => 502,
            IngestError::Busy { .. }
            | IngestError::NotReady
            | IngestError::ProviderKeyUnavailable(_) => 503,
            IngestError::Config(_)
            | IngestError::Db(_)
            | IngestError::Migrate(_)
            | IngestError::Io(_) => 500,
        }
    }
}

pub type Result<T> = std::result::Result<T, IngestError>;

pub(crate) const INTERNAL_MESSAGE: &str = "internal server error";

/// `Retry-After` of a 503 `not_ready`.
pub const NOT_READY_RETRY_AFTER_S: u64 = 5;

impl IntoResponse for IngestError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let code = self.code();
        let retry_after = match &self {
            IngestError::Busy { retry_after_s, .. } => Some(*retry_after_s),
            IngestError::NotReady => Some(NOT_READY_RETRY_AFTER_S),
            _ => None,
        };
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self, detail = ?self, code, "request failed");
            INTERNAL_MESSAGE.to_string()
        } else {
            if status.is_server_error() {
                tracing::warn!(error = %self, code, "upstream failure");
            }
            self.to_string()
        };
        let mut error = json!({ "code": code, "message": message });
        if let IngestError::ChildOfParentJob { parent_job_id } = &self {
            error["parent_job_id"] = json!(parent_job_id);
        }
        let mut resp = (status, Json(json!({ "error": error }))).into_response();
        if let Some(s) = retry_after {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, s.into());
        }
        resp
    }
}

/// A pipeline step's failure, as the queue records it on the step (and on the job when final).
/// `class` decides whether `steps::fail` retries; `code` is a stable machine-readable reason.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct StepError {
    pub class: ErrorClass,
    pub code: String,
    pub message: String,
    pub provider: Option<String>,
    /// The server's `Retry-After` (seconds) on a 429/503, when it sent one: the queue waits at
    /// least this long (capped at `BACKOFF_MAX_S`) before the next attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_s: Option<u64>,
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::Value;

    use super::*;

    async fn parts(e: IngestError) -> (StatusCode, Value) {
        let resp = e.into_response();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.expect("body");
        (status, serde_json::from_slice(&bytes).expect("json body"))
    }

    fn assert_shape(v: &Value, code: &str) {
        let err = v["error"].as_object().expect("error object");
        assert_eq!(err.len(), 2, "exactly code + message: {v}");
        assert_eq!(err["code"], code);
        assert!(err["message"].as_str().is_some_and(|m| !m.is_empty()));
    }

    #[tokio::test]
    async fn unsupported_type_is_422_with_message() {
        let (s, v) = parts(IngestError::UnsupportedType("application/zip".into())).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
        assert_shape(&v, "unsupported_type");
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap()
                .contains("application/zip")
        );
    }

    #[tokio::test]
    async fn payload_too_large_is_413_and_unauthorized_401() {
        let (s, v) = parts(IngestError::PayloadTooLarge).await;
        assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);
        assert_shape(&v, "payload_too_large");
        let (s, v) = parts(IngestError::Unauthorized).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_shape(&v, "unauthorized");
    }

    #[tokio::test]
    async fn bad_request_is_400() {
        let (s, v) = parts(IngestError::BadRequest("unknown field `x`".into())).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_shape(&v, "bad_request");
    }

    #[tokio::test]
    async fn request_timeout_is_408() {
        let (s, v) = parts(IngestError::RequestTimeout("no bytes for 60 s".into())).await;
        assert_eq!(s, StatusCode::REQUEST_TIMEOUT);
        assert_shape(&v, "request_timeout");
    }

    #[tokio::test]
    async fn upstream_is_502() {
        let (s, v) = parts(IngestError::Upstream {
            service: "strata",
            message: "timeout".into(),
        })
        .await;
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert_shape(&v, "upstream_unavailable");
        assert_eq!(v["error"]["message"], "strata unavailable: timeout");
    }

    #[tokio::test]
    async fn busy_is_503_with_retry_after() {
        let resp = IngestError::Busy {
            message: "8 uploads in progress".into(),
            retry_after_s: 30,
        }
        .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()["retry-after"], "30");
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_shape(&v, "busy");
    }

    #[tokio::test]
    async fn provider_rejections_are_422_with_their_code_and_a_missing_key_503() {
        for code in [
            "region_not_allowed",
            "zdr_required",
            "dialect_not_allowed",
            "invalid_provider",
            "provider_host_not_allowed",
            "api_key_required",
            "probe_failed",
        ] {
            let (s, v) = parts(IngestError::ProviderRejected {
                code,
                message: "x".into(),
            })
            .await;
            assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{code}");
            assert_shape(&v, code);
        }
        let (s, v) = parts(IngestError::ProviderKeyUnavailable("unset".into())).await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert_shape(&v, "provider_key_unconfigured");
    }

    #[tokio::test]
    async fn child_of_parent_job_is_409_with_the_parent_id() {
        let parent = uuid::Uuid::now_v7();
        let (s, v) = parts(IngestError::ChildOfParentJob {
            parent_job_id: parent,
        })
        .await;
        assert_eq!(s, StatusCode::CONFLICT);
        let err = v["error"].as_object().expect("error object");
        assert_eq!(err.len(), 3, "code + message + parent_job_id: {v}");
        assert_eq!(err["code"], "child_of_parent_job");
        assert_eq!(err["parent_job_id"], parent.to_string());
        assert!(
            err["message"]
                .as_str()
                .unwrap()
                .contains(&parent.to_string())
        );
    }

    #[tokio::test]
    async fn internal_errors_hide_details() {
        let (s, v) = parts(IngestError::Config("SECRET-DETAIL".into())).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
        assert_shape(&v, "internal");
        assert_eq!(v["error"]["message"], INTERNAL_MESSAGE);
        assert!(!v.to_string().contains("SECRET-DETAIL"));
        let (s, v) = parts(IngestError::Db(sqlx::Error::PoolTimedOut)).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(v["error"]["message"], INTERNAL_MESSAGE);
    }

    #[test]
    fn io_and_sqlx_errors_convert_with_question_mark() {
        fn io() -> Result<()> {
            std::fs::read("/nonexistent/strata-ingest/missing")?;
            Ok(())
        }
        fn db() -> Result<()> {
            let r: std::result::Result<(), sqlx::Error> = Err(sqlx::Error::RowNotFound);
            r?;
            Ok(())
        }
        assert!(matches!(io(), Err(IngestError::Io(_))));
        assert!(matches!(db(), Err(IngestError::Db(_))));
    }
}
