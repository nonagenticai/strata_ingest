//! The ingest pipeline: the shared [`Ctx`] the worker pool and the HTTP API run on, content
//! detection, and the steps themselves ([`run_step`]).

pub mod audio;
pub mod bundle;
pub mod detect;
pub mod discover;
pub mod figures;
pub mod page_images;
pub mod steps;
pub mod triage;

use std::sync::Arc;

use sqlx::PgPool;

use crate::config::Config;
use crate::error::Result;
use crate::infra::docling::DoclingClient;
use crate::infra::storage::StorageClient;
use crate::infra::strata::StrataClient;
use crate::infra::webscraper::WebscraperClient;
use crate::providers::ProviderHealth;
use crate::providers::registry::Providers;

pub use steps::{StepResult, run_step};

/// Object key (storage_service) of a job's original: the uploaded file, or a URL job's scraped
/// markdown.
pub fn original_key(tenant: uuid::Uuid, job: uuid::Uuid) -> String {
    format!("{tenant}/{job}/original")
}

/// Object key of a figure extracted from the converted markdown; `asset` is its asset id.
pub fn figure_key(tenant: uuid::Uuid, job: uuid::Uuid, asset: uuid::Uuid) -> String {
    format!("{tenant}/{job}/figures/{asset}")
}

/// Object key of audio segment `n` (0-based) of an audio/video job.
pub fn audio_key(tenant: uuid::Uuid, job: uuid::Uuid, n: usize) -> String {
    format!("{tenant}/{job}/audio/{n:04}.mp3")
}

/// Artefact (`ingest_artifacts`, per job) of the Convert step: `{"pages": [markdown per page]}`.
pub const CONVERT_ARTIFACT: &str = "convert.json";

/// Artefact of the Bundle step: the markdown delivered to strata.
pub const BUNDLE_ARTIFACT: &str = "bundle.md";

/// Artefact of an OcrPages step: `{"<page index>": markdown}` for the pages that step OCR'd;
/// `step_key` is the step's key (`p{first}-{last}` or `docling-cpu`).
pub fn ocr_artifact(step_key: &str) -> String {
    format!("ocr/{step_key}.json")
}

/// Artefact of a TranscribeSegment step (`step_key` = `s{n:04}`):
/// `{"segments": [{start_s, end_s, text}], "duration_s", "offset_s"}`, times in the whole input.
pub fn transcript_artifact(step_key: &str) -> String {
    format!("transcript/{step_key}.json")
}

/// Everything a step (and the API) needs: the DB pool, the config, the infra clients and the
/// optional providers. A step resolves its provider by the job's tenant
/// ([`Providers::resolve`]: the tenant's registration, else the dev default, else none); a client
/// is only reachable through [`crate::providers::Limited::acquire`], so its `concurrency` / `rpm`
/// limits apply process-wide.
#[derive(Debug)]
pub struct Ctx {
    pub pool: PgPool,
    pub cfg: Arc<Config>,
    /// storage_service: originals and binary artefacts; it also virus-scans every upload.
    pub storage: StorageClient,
    pub docling: DoclingClient,
    pub webscraper: WebscraperClient,
    pub strata: StrataClient,
    /// Call outcomes of the deployment's default providers (`/health`); never a tenant's.
    pub health: Arc<ProviderHealth>,
    pub providers: Providers,
    /// The pod's one heavy-work slot (rulings R37): a Convert step (docling's answer, the
    /// original, the markdown and the pages are in memory at once; ffmpeg for media) and a
    /// docling-cpu OcrPages batch run one at a time. Waiting for it does not count against
    /// the step deadline ([`crate::step_clock`]).
    pub convert_slot: tokio::sync::Semaphore,
    /// Bounds and name resolution of the api / datatable source fetch (`sources::fetch`).
    pub source_fetch: crate::sources::fetch::FetchOpts,
    /// Paid provider calls whose record failed, for the worker to settle (`metering::Unrecorded`).
    pub unrecorded: crate::metering::Unrecorded,
}

/// Concurrent Convert / docling-cpu OCR runs per pod (rulings R37).
pub const CONVERT_SLOTS: usize = 1;

impl Ctx {
    /// Waits (deadline paused) for the pod's heavy-work slot; hold the permit for the work.
    pub async fn heavy_slot(&self) -> tokio::sync::SemaphorePermit<'_> {
        crate::step_clock::waiting(self.convert_slot.acquire())
            .await
            .expect("the convert slot is never closed")
    }
}

impl Ctx {
    /// Builds every client from `cfg` on a lazily connecting pool: no I/O, no migrations (`serve`
    /// applies them, retrying, once the API is listening).
    pub fn from_config(cfg: Config) -> Result<Ctx> {
        let pool = crate::db::connect_lazy(&cfg.database_url)?;
        Ctx::with_pool(pool, cfg)
    }

    /// Builds the clients from `cfg` on an existing pool (`cfg.database_url` is not used).
    pub fn with_pool(pool: PgPool, cfg: Config) -> Result<Ctx> {
        let providers = Providers::new(pool.clone(), &cfg.providers, cfg.provider_key.as_ref())?;
        Ok(Ctx {
            storage: StorageClient::new(
                &cfg.storage_url,
                &cfg.storage_bucket,
                cfg.storage_token.as_ref(),
            )?,
            docling: DoclingClient::new(&cfg.docling_url)?,
            webscraper: WebscraperClient::new(&cfg.webscraper_url, cfg.webscraper_token.clone())?,
            strata: StrataClient::new(&cfg.strata_url, cfg.strata_token.clone())?,
            pool,
            cfg: Arc::new(cfg),
            health: providers.health.clone(),
            providers,
            convert_slot: tokio::sync::Semaphore::new(CONVERT_SLOTS),
            source_fetch: crate::sources::fetch::FetchOpts::default(),
            unrecorded: crate::metering::Unrecorded::default(),
        })
    }
}
