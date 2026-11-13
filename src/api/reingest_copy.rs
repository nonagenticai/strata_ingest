//! `POST /api/stores/{id}/reingest-copy` and `GET /api/stores/{id}/reingest-copy/{job}` (agent
//! D93, B3 T0i): see [`crate::reingest`].

use axum::Json;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use super::AppState;
use super::auth::Tenant;
use crate::error::{IngestError, Result};
use crate::reingest::{self, ReingestCopyJob};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReingestCopyBody {
    pub target_store: Uuid,
}

fn path_err(e: PathRejection) -> IngestError {
    IngestError::BadRequest(format!("invalid id in path: {}", e.body_text()))
}

/// `POST /api/stores/{id}/reingest-copy {target_store}` → 202 with the `running` copy: re-runs
/// extraction on the retained originals of every document of `id`'s subtree with the target's
/// ocr / vision / transcribe bindings, into the empty new root `target_store` (the source tree is
/// mirrored under it). 404 `unknown_store`; 409 `pack_published`, `target_not_empty`, `conflict`
/// (a copy into the target is running); 422 `invalid_target`; 400 for a malformed body.
pub async fn start_reingest_copy(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
    body: std::result::Result<Json<ReingestCopyBody>, JsonRejection>,
) -> Result<(StatusCode, Json<ReingestCopyJob>)> {
    let Path(source) = id.map_err(path_err)?;
    let Json(body) = body.map_err(|e| IngestError::BadRequest(e.body_text()))?;
    let job = reingest::start(&s.ctx, tenant, source, body.target_store).await?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// `GET /api/stores/{id}/reingest-copy/{job}` → `{job, source_store, target_store, state,
/// documents_done, documents_total, documents_failed, documents_missing, missing_originals[],
/// error?, passes, started_at, finished_at?}`. Another tenant's copy, or one of another source,
/// is 404 `not_found`.
pub async fn get_reingest_copy(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    ids: std::result::Result<Path<(Uuid, Uuid)>, PathRejection>,
) -> Result<Json<ReingestCopyJob>> {
    let Path((source, job)) = ids.map_err(path_err)?;
    Ok(Json(reingest::get(&s.ctx.pool, tenant, source, job).await?))
}
