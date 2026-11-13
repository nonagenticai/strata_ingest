//! `ingest_assets`: the blobs a job stored in storage_service (original, figures, page images,
//! audio segments). Writes from a worker are fenced on its step claim, like every step write.

use sqlx::PgPool;
use uuid::Uuid;

use crate::db::steps::ClaimedStep;
use crate::error::{IngestError, Result};

/// `ingest_assets.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetKind {
    Original,
    Figure,
    PageImage,
    AudioSegment,
}

impl AssetKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AssetKind::Original => "original",
            AssetKind::Figure => "figure",
            AssetKind::PageImage => "page_image",
            AssetKind::AudioSegment => "audio_segment",
        }
    }
}

/// A new asset row. `page` is for figures and page images. `id`: the row's id on insert
/// (`None` = a fresh UUIDv7); a figure passes the deterministic id its key is built from.
#[derive(Debug, Clone)]
pub struct NewAsset<'a> {
    pub id: Option<Uuid>,
    pub kind: AssetKind,
    pub object_key: &'a str,
    pub bytes: u64,
    pub mime: &'a str,
    pub page: Option<i32>,
}

/// Records an asset of the step's job and returns its id. Idempotent per `(job, object_key)` (a
/// UNIQUE constraint): a retried step that stores the same key again updates that row (kind,
/// page, size, mime) and keeps its id. `LeaseLost` (nothing written) when `step` is no longer current.
pub async fn record(pool: &PgPool, step: &ClaimedStep, asset: NewAsset<'_>) -> Result<Uuid> {
    let bytes = i64::try_from(asset.bytes)
        .map_err(|_| IngestError::InvalidInput(format!("asset size {}", asset.bytes)))?;
    let mut tx = pool.begin().await?;
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR SHARE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    if current.is_none() {
        return Err(IngestError::LeaseLost {
            step: step.id,
            attempt: step.attempt,
        });
    }
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO ingest_assets (id, job_id, kind, object_key, page, bytes, mime)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (job_id, object_key)
         DO UPDATE SET kind = EXCLUDED.kind, page = EXCLUDED.page, bytes = EXCLUDED.bytes,
                       mime = EXCLUDED.mime
         RETURNING id",
    )
    .bind(asset.id.unwrap_or_else(Uuid::now_v7))
    .bind(step.job_id)
    .bind(asset.kind.as_str())
    .bind(asset.object_key)
    .bind(asset.page)
    .bind(bytes)
    .bind(asset.mime)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

/// `(object_key, mime)` of the job's asset `id`; `None` when the job has no such asset.
pub async fn key_and_mime(
    pool: &PgPool,
    job_id: Uuid,
    id: Uuid,
) -> Result<Option<(String, String)>> {
    Ok(
        sqlx::query_as("SELECT object_key, mime FROM ingest_assets WHERE job_id = $1 AND id = $2")
            .bind(job_id)
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

/// Sets the caption of the step's job's asset `id` (a retry overwrites it). Fenced on the claim:
/// `LeaseLost` (nothing written) when `step` is no longer current; `NotFound` when the job has no
/// such asset.
pub async fn set_caption(pool: &PgPool, step: &ClaimedStep, id: Uuid, caption: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let current: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM ingest_steps
         WHERE id = $1 AND attempt = $2 AND status = 'running' FOR SHARE",
    )
    .bind(step.id)
    .bind(step.attempt)
    .fetch_optional(&mut *tx)
    .await?;
    if current.is_none() {
        return Err(IngestError::LeaseLost {
            step: step.id,
            attempt: step.attempt,
        });
    }
    let n = sqlx::query("UPDATE ingest_assets SET caption = $3 WHERE id = $1 AND job_id = $2")
        .bind(id)
        .bind(step.job_id)
        .bind(caption)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n == 0 {
        return Err(IngestError::NotFound);
    }
    tx.commit().await?;
    Ok(())
}

/// Captions of the job's assets among `ids`: every id with a row maps to its caption (`None` when
/// it has none); an id without a row (an orphan placeholder) is absent.
pub async fn captions(
    pool: &PgPool,
    job_id: Uuid,
    ids: &[Uuid],
) -> Result<std::collections::HashMap<Uuid, Option<String>>> {
    if ids.is_empty() {
        return Ok(Default::default());
    }
    let rows: Vec<(Uuid, Option<String>)> =
        sqlx::query_as("SELECT id, caption FROM ingest_assets WHERE job_id = $1 AND id = ANY($2)")
            .bind(job_id)
            .bind(ids)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}
