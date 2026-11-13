//! Per-job provider resolution and the tenant / store provider-config API (spec §4.1, owner
//! ruling D92; same contract as strata_rag's `PUT/GET /api/tenant/providers` and
//! `GET/PUT/DELETE /api/stores/{id}/providers`).
//!
//! Resolution for a step of a job, per capability: the registration nearest to the job's target
//! store (`store_providers`: the store's own row, else its nearest ancestor's by the cached
//! `store_lineage`) → the tenant's registration (`tenant_providers`) → else (dev deployment
//! only) the `providers.toml` default → else the capability is unconfigured: planning takes the
//! no-provider path (docling-cpu OCR, captions skipped, audio refused) and a step that needs it
//! fails `provider_not_configured`.
//!
//! Built clients are cached per `(tenant, store or none, capability)`, keyed by the row's
//! `updated_at`. Every resolution reads the nearest store row's and the tenant row's
//! `updated_at` (indexed lookups), so a PUT on any replica switches every replica on its next
//! step; the replica that took the PUT also drops its entries at once. A cached client keeps its
//! own `concurrency` slots across steps.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::crypto::KeyCipher;
use super::ocr::{OcrClient, OcrInput};
use super::transcribe::TranscribeClient;
use super::vision::VisionClient;
use super::{Capability, Limited, ProviderError, ProviderHealth};
use crate::config::{
    DEFAULT_PROVIDER_CONCURRENCY, DEFAULT_PROVIDER_TIMEOUT_MS, Mode, ProviderCfg, ProvidersFile,
    Region, Secret, dialects_for, redact_url,
};
use crate::db::providers::{self as rows, NewProviderRow, ProviderRow};
use crate::db::store_providers as srows;
use crate::error::{IngestError, Result};
use crate::metering::{Funding, default_billing};
use crate::netguard::{self, ProviderUrlError};

/// Upper bound for one registration probe (connect + one minimal call).
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Most concurrent calls a tenant may register for one provider.
pub const MAX_TENANT_CONCURRENCY: usize = 64;

/// Where a resolved provider came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A row of the job's store or of one of its ancestors.
    Store,
    Tenant,
    Default,
}

/// One capability's provider for a step: the client behind its limits, plus what a step records
/// (`provider` = dialect, `model`, price for `cost_micros`; a registration has no price).
#[derive(Debug)]
pub struct Resolved<T> {
    pub client: Limited<T>,
    pub dialect: String,
    pub model: String,
    pub price_micros_per_unit: Option<i64>,
    pub source: Source,
    /// Who pays and the billing `provider` name (`metering`).
    pub funding: Funding,
    pub billing_provider: String,
}

#[derive(Debug, Clone)]
enum Lane {
    Ocr(Arc<Resolved<OcrClient>>),
    Vision(Arc<Resolved<VisionClient>>),
    Transcribe(Arc<Resolved<TranscribeClient>>),
}

/// A provider client kind, so resolution is written once.
pub trait Kind: Sized {
    const CAP: Capability;
    #[doc(hidden)]
    fn build(cfg: &ProviderCfg, health: Arc<ProviderHealth>, guarded: bool) -> Result<Self>;
    #[doc(hidden)]
    fn lane(r: Arc<Resolved<Self>>) -> LaneBox;
    #[doc(hidden)]
    fn of(l: &LaneBox) -> Option<Arc<Resolved<Self>>>;
}

/// Opaque cache entry (public only because [`Kind`] names it).
#[derive(Debug, Clone)]
pub struct LaneBox(Lane);

macro_rules! kind {
    ($t:ty, $cap:ident) => {
        impl Kind for $t {
            const CAP: Capability = Capability::$cap;
            fn build(
                cfg: &ProviderCfg,
                health: Arc<ProviderHealth>,
                guarded: bool,
            ) -> Result<Self> {
                if guarded {
                    <$t>::guarded(cfg, health)
                } else {
                    <$t>::new(cfg, health)
                }
            }
            fn lane(r: Arc<Resolved<Self>>) -> LaneBox {
                LaneBox(Lane::$cap(r))
            }
            fn of(l: &LaneBox) -> Option<Arc<Resolved<Self>>> {
                match &l.0 {
                    Lane::$cap(r) => Some(r.clone()),
                    _ => None,
                }
            }
        }
    };
}
kind!(OcrClient, Ocr);
kind!(VisionClient, Vision);
kind!(TranscribeClient, Transcribe);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Why a provider was refused: the 422 `error.code` plus a message without a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub code: &'static str,
    pub message: String,
}

impl From<Rejection> for IngestError {
    fn from(r: Rejection) -> IngestError {
        IngestError::ProviderRejected {
            code: r.code,
            message: r.message,
        }
    }
}

fn reject(code: &'static str, message: String) -> Rejection {
    Rejection { code, message }
}

fn url_rejection(cap: Capability, e: ProviderUrlError) -> Rejection {
    reject(e.code(), format!("{cap}: base_url {}", e.message()))
}

/// One capability of a registration, after defaults; `api_key` is `None` until the stored one is
/// kept. `Debug` redacts the key.
#[derive(Debug, Clone)]
pub struct TenantSpec {
    /// No trailing `/`; the client appends `/ocr`, `/chat/completions`, `/audio/transcriptions`.
    pub base_url: String,
    pub dialect: String,
    pub model: String,
    pub model_id: Option<String>,
    pub regions: Vec<String>,
    pub zdr: bool,
    pub timeout_ms: u64,
    pub concurrency: usize,
    pub api_key: Option<Secret>,
}

/// The rules a registration passes before its probe (spec §4.1, §7): an implemented dialect for
/// the capability; a canonical http(s) `base_url` without credentials, query or fragment; a
/// model; `regions` (eu / us) containing the deployment region; `zdr` in an `eu` deployment;
/// `timeout_ms` within the capability's cap; sane `concurrency`; a non-blank key when given.
/// The SSRF host rules ([`netguard::check_provider_registration`]) are separate (skipped by the
/// dev-only `allow_internal_provider_hosts`).
pub fn validate(
    cap: Capability,
    spec: &TenantSpec,
    region: Region,
) -> std::result::Result<(), Rejection> {
    let allowed = dialects_for(cap.as_str());
    if !allowed.contains(&spec.dialect.as_str()) {
        return Err(reject(
            "dialect_not_allowed",
            format!(
                "{cap}: dialect {:?} is not allowed (strata-ingest speaks {allowed:?} for {cap})",
                spec.dialect
            ),
        ));
    }
    let invalid = |m: String| reject("invalid_provider", format!("{cap}: {m}"));
    netguard::parse_provider_url(&spec.base_url).map_err(|e| url_rejection(cap, e))?;
    if spec.model.is_empty() {
        return Err(invalid("model must not be empty".to_string()));
    }
    if spec.model_id.as_ref().is_some_and(|m| m.is_empty()) {
        return Err(invalid("model_id must not be empty".to_string()));
    }
    if spec.regions.is_empty() {
        return Err(reject(
            "region_not_allowed",
            format!("{cap}: regions is empty; it must contain the deployment region \"{region}\""),
        ));
    }
    if let Some(r) = spec.regions.iter().find(|r| Region::parse(r).is_none()) {
        return Err(invalid(format!("unknown region {r:?} (eu or us)")));
    }
    if !spec.regions.iter().any(|r| r == region.as_str()) {
        return Err(reject(
            "region_not_allowed",
            format!(
                "{cap}: regions {:?} do not include this deployment's region \"{region}\"",
                spec.regions
            ),
        ));
    }
    if region == Region::Eu && !spec.zdr {
        return Err(reject(
            "zdr_required",
            format!("{cap}: an eu deployment requires zdr = true (zero data retention in the DPA)"),
        ));
    }
    let max_timeout = cap.max_tenant_timeout_ms();
    if spec.timeout_ms == 0 || spec.timeout_ms > max_timeout {
        return Err(invalid(format!(
            "timeout_ms must be within 1..={max_timeout}"
        )));
    }
    if spec.concurrency == 0 || spec.concurrency > MAX_TENANT_CONCURRENCY {
        return Err(invalid(format!(
            "concurrency must be within 1..={MAX_TENANT_CONCURRENCY}"
        )));
    }
    if spec
        .api_key
        .as_ref()
        .is_some_and(|k| k.expose().trim().is_empty())
    {
        return Err(invalid("api_key must not be empty".to_string()));
    }
    Ok(())
}

/// `PUT /api/tenant/providers` body: the tenant's whole provider set. A capability left out or
/// `null` is removed.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutProviders {
    #[serde(default)]
    pub ocr: Option<ProviderInput>,
    #[serde(default)]
    pub vision: Option<ProviderInput>,
    #[serde(default)]
    pub transcribe: Option<ProviderInput>,
}

/// One capability in a PUT. `api_key` is write-only: omitted = keep the stored key (same
/// `base_url` only). `Debug` redacts it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderInput {
    pub base_url: String,
    pub dialect: String,
    pub model: String,
    #[serde(default)]
    pub model_id: Option<String>,
    pub regions: Vec<String>,
    pub zdr: bool,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub concurrency: Option<usize>,
    #[serde(default)]
    pub api_key: Option<Secret>,
}

impl ProviderInput {
    pub fn into_spec(self) -> TenantSpec {
        TenantSpec {
            // not trimmed: whitespace is refused by the URL check, never silently fixed
            base_url: self.base_url.trim_end_matches('/').to_string(),
            dialect: self.dialect.trim().to_string(),
            model: self.model.trim().to_string(),
            model_id: self.model_id.map(|m| m.trim().to_string()),
            regions: self.regions.iter().map(|r| r.trim().to_string()).collect(),
            zdr: self.zdr,
            timeout_ms: self.timeout_ms.unwrap_or(DEFAULT_PROVIDER_TIMEOUT_MS),
            concurrency: self.concurrency.unwrap_or(DEFAULT_PROVIDER_CONCURRENCY),
            api_key: self.api_key,
        }
    }
}

/// `GET /api/tenant/providers` (and the PUT answer): per capability, where the provider comes
/// from and its settings. Never a key.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProvidersView {
    pub ocr: CapabilityView,
    pub vision: CapabilityView,
    pub transcribe: CapabilityView,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CapabilityView {
    /// `tenant` (a registration), `default` (the dev deployment default) or `unconfigured`; in a
    /// store view also `store` (the store's own row) or `ancestor` (inherited from `from_store`).
    pub source: &'static str,
    /// The ancestor whose row a store inherits (`source = "ancestor"` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_store: Option<Uuid>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderView>,
    /// A dev default: what it is, never where (no `base_url`, no regions).
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub default: Option<DefaultView>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DefaultView {
    pub dialect: String,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProviderView {
    pub base_url: String,
    pub dialect: String,
    pub model: String,
    pub model_id: Option<String>,
    pub regions: Vec<String>,
    pub zdr: bool,
    pub timeout_ms: u64,
    pub concurrency: usize,
    pub has_api_key: bool,
    /// RFC 3339, UTC, microseconds.
    pub updated_at: String,
}

impl ProviderView {
    fn of(row: &ProviderRow) -> Self {
        ProviderView {
            base_url: redact_url(&row.base_url),
            dialect: row.dialect.clone(),
            model: row.model.clone(),
            model_id: row.model_id.clone(),
            regions: row.regions.clone(),
            zdr: row.zdr,
            timeout_ms: u64::try_from(row.timeout_ms).unwrap_or(1),
            concurrency: usize::try_from(row.concurrency).unwrap_or(1),
            has_api_key: true,
            updated_at: row.updated_at.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
        }
    }
}

const UNCONFIGURED: CapabilityView = CapabilityView {
    source: "unconfigured",
    from_store: None,
    provider: None,
    default: None,
};

/// `GET /api/stores/{id}/providers` (and the PUT / DELETE answer): per capability, which
/// registration a job targeting this store (or a store under it) uses and its settings. Never a
/// key.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StoreProvidersView {
    pub store_id: Uuid,
    pub ocr: CapabilityView,
    pub vision: CapabilityView,
    pub transcribe: CapabilityView,
}

/// Built clients by `(tenant, store, capability)` (`store` = `None` for the tenant's own row),
/// with the row version they were built from.
type ClientCache = HashMap<(Uuid, Option<Uuid>, Capability), (DateTime<Utc>, LaneBox)>;

/// The process's provider source: the deployment layer, the tenant registry and the client cache.
pub struct Providers {
    pool: sqlx::PgPool,
    region: Region,
    mode: Mode,
    allow_internal_provider_hosts: bool,
    cipher: Option<KeyCipher>,
    /// The dev defaults' clients, built once, by capability slot (`None` in production).
    defaults: [Option<LaneBox>; 3],
    /// `/health` reports the defaults only; a tenant's provider never shows there.
    pub health: Arc<ProviderHealth>,
    cache: Mutex<ClientCache>,
    probe_timeout: Duration,
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Providers")
            .field("region", &self.region)
            .field("mode", &self.mode)
            .field(
                "allow_internal_provider_hosts",
                &self.allow_internal_provider_hosts,
            )
            .field("cipher", &self.cipher.is_some())
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

fn default_lane<T: Kind>(
    cfg: Option<&ProviderCfg>,
    health: &Arc<ProviderHealth>,
) -> Result<Option<LaneBox>> {
    cfg.map(|c| {
        let client = T::build(c, health.clone(), false)?;
        let (funding, billing_provider) = default_billing(&c.base_url, &c.dialect);
        Ok(T::lane(Arc::new(Resolved {
            client: Limited::new(client, c.concurrency, c.rpm),
            dialect: c.dialect.clone(),
            model: c.model.clone(),
            price_micros_per_unit: c.price_micros_per_unit,
            source: Source::Default,
            funding,
            billing_provider,
        })))
    })
    .transpose()
}

/// A stored row as a client config (the key opened).
fn cfg_of(row: &ProviderRow, api_key: Secret) -> ProviderCfg {
    ProviderCfg {
        base_url: row.base_url.clone(),
        dialect: row.dialect.clone(),
        model: row.model.clone(),
        api_key,
        regions: row
            .regions
            .iter()
            .filter_map(|r| Region::parse(r))
            .collect(),
        zdr: row.zdr,
        timeout_ms: u64::try_from(row.timeout_ms).unwrap_or(1),
        concurrency: usize::try_from(row.concurrency).unwrap_or(1),
        rpm: None,
        price_micros_per_unit: None,
    }
}

fn spec_cfg(spec: &TenantSpec, api_key: Secret) -> ProviderCfg {
    ProviderCfg {
        base_url: spec.base_url.clone(),
        dialect: spec.dialect.clone(),
        model: spec.model.clone(),
        api_key,
        regions: spec
            .regions
            .iter()
            .filter_map(|r| Region::parse(r))
            .collect(),
        zdr: spec.zdr,
        timeout_ms: spec.timeout_ms,
        concurrency: spec.concurrency,
        rpm: None,
        price_micros_per_unit: None,
    }
}

impl Providers {
    /// The deployment layer from `file` (dev defaults built once; no I/O), tenant registrations in
    /// `pool`, keys sealed with `provider_key` (`STRATA_INGEST_PROVIDER_KEY`, base64).
    pub fn new(
        pool: sqlx::PgPool,
        file: &ProvidersFile,
        provider_key: Option<&Secret>,
    ) -> Result<Self> {
        let health = Arc::new(ProviderHealth::default());
        let dev = file.mode == Mode::Dev;
        let pick = |c: Option<&ProviderCfg>| if dev { c.cloned() } else { None };
        let defaults = [
            default_lane::<OcrClient>(pick(file.ocr.as_ref()).as_ref(), &health)?,
            default_lane::<VisionClient>(pick(file.vision.as_ref()).as_ref(), &health)?,
            default_lane::<TranscribeClient>(pick(file.transcribe.as_ref()).as_ref(), &health)?,
        ];
        let cipher = provider_key
            .map(|k| KeyCipher::from_base64(k.expose()))
            .transpose()?;
        Ok(Providers {
            pool,
            region: file.region,
            mode: file.mode,
            allow_internal_provider_hosts: file.allow_internal_provider_hosts,
            cipher,
            defaults,
            health,
            cache: Mutex::new(HashMap::new()),
            probe_timeout: PROBE_TIMEOUT,
        })
    }

    /// Whether the deployment has a default for `cap` (what `/health` reports).
    pub fn has_default(&self, cap: Capability) -> bool {
        self.defaults[cap.slot()].is_some()
    }

    /// Startup check (after migrations): stored keys without `STRATA_INGEST_PROVIDER_KEY` refuse
    /// to start, so a deployment that lost the key fails loudly instead of failing every job.
    pub async fn check_keys(&self) -> Result<()> {
        if self.cipher.is_none()
            && (rows::any_stored_key(&self.pool).await?
                || srows::any_stored_key(&self.pool).await?)
        {
            return Err(IngestError::Config(format!(
                "tenant provider keys are stored but {} is unset; set it to the key they were \
                 sealed with",
                super::crypto::KEY_ENV
            )));
        }
        Ok(())
    }

    /// The ocr provider of a step of `job` (`None` = the tenant's, for no particular job).
    pub async fn ocr(
        &self,
        tenant: Uuid,
        job: Option<Uuid>,
    ) -> Result<Option<Arc<Resolved<OcrClient>>>> {
        self.resolve::<OcrClient>(tenant, job).await
    }

    pub async fn vision(
        &self,
        tenant: Uuid,
        job: Option<Uuid>,
    ) -> Result<Option<Arc<Resolved<VisionClient>>>> {
        self.resolve::<VisionClient>(tenant, job).await
    }

    pub async fn transcribe(
        &self,
        tenant: Uuid,
        job: Option<Uuid>,
    ) -> Result<Option<Arc<Resolved<TranscribeClient>>>> {
        self.resolve::<TranscribeClient>(tenant, job).await
    }

    /// Tenant registrations are under the SSRF guard unless the dev-only flag lifts it.
    fn guarded(&self) -> bool {
        !self.allow_internal_provider_hosts
    }

    fn cipher(&self) -> Result<&KeyCipher> {
        self.cipher.as_ref().ok_or_else(|| {
            IngestError::ProviderKeyUnavailable(format!(
                "{} is not set on this deployment",
                super::crypto::KEY_ENV
            ))
        })
    }

    fn default_of<T: Kind>(&self) -> Option<Arc<Resolved<T>>> {
        self.defaults[T::CAP.slot()].as_ref().and_then(T::of)
    }

    /// The provider for `T::CAP` of a step of `job` (of `tenant`): the row nearest to the job's
    /// store, else the tenant's registration (each cached by its `updated_at`), else the dev
    /// default, else `None`. `job = None` skips the store layer.
    pub async fn resolve<T: Kind>(
        &self,
        tenant: Uuid,
        job: Option<Uuid>,
    ) -> Result<Option<Arc<Resolved<T>>>> {
        let cap = T::CAP;
        if let Some(job) = job
            && let Some((store, at)) = srows::nearest_for_job(&self.pool, tenant, job, cap).await?
            && let Some(r) = self.scoped::<T>(tenant, Some(store), at).await?
        {
            return Ok(Some(r));
        }
        let Some(at) = rows::version(&self.pool, tenant, cap).await? else {
            lock(&self.cache).remove(&(tenant, None, cap));
            return Ok(self.default_of::<T>());
        };
        match self.scoped::<T>(tenant, None, at).await? {
            Some(r) => Ok(Some(r)),
            // deleted since the version read
            None => Ok(self.default_of::<T>()),
        }
    }

    /// The client of `store`'s row (`None` = the tenant's row) for `T::CAP` at version `at`: the
    /// cached one, else the row loaded, re-checked, opened and built. `None` when the row was
    /// deleted since the version read (the caller falls back to the next layer).
    async fn scoped<T: Kind>(
        &self,
        tenant: Uuid,
        store: Option<Uuid>,
        at: DateTime<Utc>,
    ) -> Result<Option<Arc<Resolved<T>>>> {
        let cap = T::CAP;
        let key = (tenant, store, cap);
        if let Some((v, lane)) = lock(&self.cache).get(&key)
            && *v == at
        {
            return Ok(T::of(lane));
        }
        let row = match store {
            None => rows::get(&self.pool, tenant, cap).await?,
            Some(id) => srows::get(&self.pool, tenant, id, cap).await?,
        };
        let Some(row) = row else {
            return Ok(None);
        };
        // A stored row is re-checked before a client is built from it (IP literals never pass
        // through the connect-time resolver).
        if self.guarded()
            && let Err(e) = netguard::check_provider_host(&row.base_url)
        {
            tracing::warn!(%tenant, ?store, capability = %cap, "stored provider host not allowed; refusing");
            return Err(url_rejection(cap, e).into());
        }
        let key_secret = match store {
            None => self.cipher()?.open(tenant, cap, &row.sealed_key)?,
            Some(id) => self
                .cipher()?
                .open_store(tenant, id, cap, &row.sealed_key)?,
        };
        let cfg = cfg_of(&row, key_secret);
        // a tenant's calls never show on the deployment's /health
        let client = T::build(&cfg, Arc::new(ProviderHealth::default()), self.guarded())?;
        let resolved = Arc::new(Resolved {
            client: Limited::new(client, cfg.concurrency, None),
            dialect: row.dialect.clone(),
            model: row.model.clone(),
            price_micros_per_unit: None,
            source: if store.is_some() {
                Source::Store
            } else {
                Source::Tenant
            },
            funding: Funding::TenantByo,
            billing_provider: row.dialect.clone(),
        });
        let mut cache = lock(&self.cache);
        // a newer version cached by a concurrent step wins
        match cache.get(&key) {
            Some((v, lane)) if *v > row.updated_at => Ok(T::of(lane)),
            _ => {
                cache.insert(key, (row.updated_at, T::lane(resolved.clone())));
                Ok(Some(resolved))
            }
        }
    }

    pub async fn view(&self, tenant: Uuid) -> Result<ProvidersView> {
        let stored = rows::list(&self.pool, tenant).await?;
        let view = |cap: Capability| {
            if let Some(row) = stored.iter().find(|r| r.capability == cap.as_str()) {
                return CapabilityView {
                    source: "tenant",
                    from_store: None,
                    provider: Some(ProviderView::of(row)),
                    default: None,
                };
            }
            let default = match &self.defaults[cap.slot()] {
                Some(LaneBox(Lane::Ocr(r))) => Some((r.dialect.clone(), r.model.clone())),
                Some(LaneBox(Lane::Vision(r))) => Some((r.dialect.clone(), r.model.clone())),
                Some(LaneBox(Lane::Transcribe(r))) => Some((r.dialect.clone(), r.model.clone())),
                None => None,
            };
            match default {
                Some((dialect, model)) if self.mode == Mode::Dev => CapabilityView {
                    source: "default",
                    from_store: None,
                    provider: None,
                    default: Some(DefaultView { dialect, model }),
                },
                _ => UNCONFIGURED,
            }
        };
        Ok(ProvidersView {
            ocr: view(Capability::Ocr),
            vision: view(Capability::Vision),
            transcribe: view(Capability::Transcribe),
        })
    }

    /// Replaces `tenant`'s whole set: validate, SSRF-check, resolve kept keys, probe each live
    /// (concurrently, ≤ [`PROBE_TIMEOUT`]), then write all rows in one transaction.
    pub async fn replace(&self, tenant: Uuid, body: PutProviders) -> Result<ProvidersView> {
        let cipher = self.cipher()?;
        let stored = rows::list(&self.pool, tenant).await?;
        let set = self
            .prepare(body, |cap, spec| {
                let row = kept_row(cap, spec, &stored)?;
                cipher.open(tenant, cap, &row.sealed_key)
            })
            .await?;
        let sealed = set
            .iter()
            .map(|(cap, _, key)| cipher.seal(tenant, *cap, key))
            .collect::<Result<Vec<_>>>()?;
        let result = async {
            let mut tx = self.pool.begin().await?;
            for cap in Capability::ALL {
                match set.iter().position(|(c, _, _)| *c == cap) {
                    Some(i) => {
                        rows::upsert(&mut tx, tenant, cap, &new_row(&set[i].1, &sealed[i])).await?
                    }
                    None => rows::delete(&mut tx, tenant, cap).await?,
                }
            }
            tx.commit().await?;
            Ok::<(), IngestError>(())
        }
        .await;
        // also on failure: a partly applied write must not leave a stale client behind
        lock(&self.cache).retain(|(t, st, _), _| !(*t == tenant && st.is_none()));
        result?;
        tracing::info!(
            %tenant,
            capabilities = ?set.iter().map(|(c, _, _)| c.as_str()).collect::<Vec<_>>(),
            "tenant providers replaced"
        );
        self.view(tenant).await
    }

    /// The store view of `store` (of `tenant`) whose ancestors are `ancestors` (nearest first):
    /// per capability the store's own row, else the nearest ancestor's, else the tenant view's
    /// entry.
    pub async fn store_view(
        &self,
        tenant: Uuid,
        store: Uuid,
        ancestors: &[Uuid],
    ) -> Result<StoreProvidersView> {
        let mut chain = Vec::with_capacity(ancestors.len() + 1);
        chain.push(store);
        chain.extend_from_slice(ancestors);
        let bound = srows::list_for(&self.pool, tenant, &chain).await?;
        let tenant_view = self.view(tenant).await?;
        let view = |cap: Capability, inherited: &CapabilityView| {
            for holder in &chain {
                if let Some((_, row)) = bound
                    .iter()
                    .find(|(s, r)| s == holder && r.capability == cap.as_str())
                {
                    let own = *holder == store;
                    return CapabilityView {
                        source: if own { "store" } else { "ancestor" },
                        from_store: (!own).then_some(*holder),
                        provider: Some(ProviderView::of(row)),
                        default: None,
                    };
                }
            }
            inherited.clone()
        };
        Ok(StoreProvidersView {
            store_id: store,
            ocr: view(Capability::Ocr, &tenant_view.ocr),
            vision: view(Capability::Vision, &tenant_view.vision),
            transcribe: view(Capability::Transcribe, &tenant_view.transcribe),
        })
    }

    /// Replaces `store`'s own set (a capability left out or `null` is removed: the store then
    /// inherits it). Same rules, kept-key rule (the store's own row, same `base_url`) and live
    /// probe as [`Providers::replace`]; written in one transaction. Answers the store view.
    pub async fn replace_store(
        &self,
        tenant: Uuid,
        store: Uuid,
        ancestors: &[Uuid],
        body: PutProviders,
    ) -> Result<StoreProvidersView> {
        let cipher = self.cipher()?;
        let stored: Vec<ProviderRow> = srows::list_for(&self.pool, tenant, &[store])
            .await?
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        let set = self
            .prepare(body, |cap, spec| {
                let row = kept_row(cap, spec, &stored)?;
                cipher.open_store(tenant, store, cap, &row.sealed_key)
            })
            .await?;
        let sealed = set
            .iter()
            .map(|(cap, _, key)| cipher.seal_store(tenant, store, *cap, key))
            .collect::<Result<Vec<_>>>()?;
        let result = async {
            let mut tx = self.pool.begin().await?;
            for cap in Capability::ALL {
                match set.iter().position(|(c, _, _)| *c == cap) {
                    Some(i) => {
                        srows::upsert(&mut tx, tenant, store, cap, &new_row(&set[i].1, &sealed[i]))
                            .await?
                    }
                    None => srows::delete(&mut tx, tenant, store, cap).await?,
                }
            }
            tx.commit().await?;
            Ok::<(), IngestError>(())
        }
        .await;
        lock(&self.cache).retain(|(t, st, _), _| !(*t == tenant && *st == Some(store)));
        result?;
        tracing::info!(
            %tenant,
            %store,
            capabilities = ?set.iter().map(|(c, _, _)| c.as_str()).collect::<Vec<_>>(),
            "store providers replaced"
        );
        self.store_view(tenant, store, ancestors).await
    }

    /// A PUT body under every registration rule: validated, SSRF-checked, the kept key resolved
    /// by `kept` for a capability without `api_key`, then every entry probed live (concurrently,
    /// ≤ [`PROBE_TIMEOUT`]). Nothing is written.
    async fn prepare(
        &self,
        body: PutProviders,
        kept: impl Fn(Capability, &TenantSpec) -> Result<Secret>,
    ) -> Result<Vec<(Capability, TenantSpec, Secret)>> {
        let mut set: Vec<(Capability, TenantSpec, Secret)> = Vec::new();
        for (cap, input) in [
            (Capability::Ocr, body.ocr),
            (Capability::Vision, body.vision),
            (Capability::Transcribe, body.transcribe),
        ] {
            let Some(input) = input else { continue };
            let spec = input.into_spec();
            validate(cap, &spec, self.region)?;
            if self.guarded() {
                netguard::check_provider_registration(&spec.base_url)
                    .await
                    .map_err(|e| url_rejection(cap, e))?;
            }
            let key = match &spec.api_key {
                Some(k) => k.clone(),
                None => kept(cap, &spec)?,
            };
            set.push((cap, spec, key));
        }
        futures::future::try_join_all(
            set.iter()
                .map(|(cap, spec, key)| self.probe(*cap, spec_cfg(spec, key.clone()))),
        )
        .await?;
        Ok(set)
    }

    /// The registration probe: [`Providers::probe_call`]; a failed call → 422 `probe_failed`
    /// naming the error class only (`auth`, `not_found`, `retryable`, `invalid_request`,
    /// `timeout`, `unreachable`).
    async fn probe(&self, cap: Capability, cfg: ProviderCfg) -> Result<()> {
        match self.probe_call(cap, cfg).await? {
            None => Ok(()),
            Some(class) => Err(reject("probe_failed", probe_failed_detail(cap, class)).into()),
        }
    }

    /// One probe call: a fresh client (never the cache, never `/health`), one minimal call
    /// through the same client code steps use (a 1×1 PNG for ocr / vision, half a second of
    /// silent WAV for transcribe), bounded by the probe timeout. `None` = it answered; `Some`
    /// = the error class of the failed call. A client that cannot be built → 422
    /// `invalid_provider`. Records no metering event (no step, no cost: the probe is not a
    /// tenant's use; strata spec §7).
    async fn probe_call(&self, cap: Capability, cfg: ProviderCfg) -> Result<Option<&'static str>> {
        let guarded = self.guarded();
        let health = Arc::new(ProviderHealth::default());
        let call = async {
            let built = |e: IngestError| match e {
                IngestError::Config(m) => Probe::Build(m),
                other => Probe::Other(other),
            };
            match cap {
                Capability::Ocr => {
                    let c = OcrClient::build(&cfg, health, guarded).map_err(built)?;
                    c.ocr(OcrInput {
                        bytes: Bytes::from(probe_png()),
                        mime: "image/png".to_string(),
                        pages: None,
                    })
                    .await
                    .map(drop)
                    .map_err(Probe::Call)
                }
                Capability::Vision => {
                    let c = VisionClient::build(&cfg, health, guarded).map_err(built)?;
                    c.caption(&probe_png(), "image/png")
                        .await
                        .map(drop)
                        .map_err(Probe::Call)
                }
                Capability::Transcribe => {
                    let c = TranscribeClient::build(&cfg, health, guarded).map_err(built)?;
                    c.transcribe(Bytes::from(probe_wav()), "probe.wav", None)
                        .await
                        .map(drop)
                        .map_err(Probe::Call)
                }
            }
        };
        let class = match tokio::time::timeout(self.probe_timeout, call).await {
            Ok(Ok(())) => return Ok(None),
            Ok(Err(Probe::Build(m))) => {
                return Err(reject("invalid_provider", format!("{cap}: {m}")).into());
            }
            Ok(Err(Probe::Other(e))) => return Err(e),
            Ok(Err(Probe::Call(e))) => {
                if e.status.is_none() && e.class == super::ErrorClass::Retryable {
                    if e.message.contains("timed out") {
                        "timeout"
                    } else {
                        "unreachable"
                    }
                } else {
                    e.class.as_str()
                }
            }
            Err(_) => "timeout",
        };
        tracing::info!(capability = %cap, class, "provider probe failed");
        Ok(Some(class))
    }

    /// `POST /api/tenant/providers/test` (T0t, agent 06 §3 A3): runs exactly the probe the PUT
    /// runs on one capability's registration and stores nothing (no row, no cached client, no
    /// metering event). Same validation and SSRF guard as the PUT (422 with the PUT's codes). No
    /// `api_key` → the tenant's stored key of that capability, only for the same `base_url`;
    /// else 422 `secret_required_for_new_host` (a stored key never goes to another host). A
    /// failed probe call is `ok: false` with the error class (never a 5xx, never a key or URL).
    pub async fn test(&self, tenant: Uuid, body: TestProviderBody) -> Result<ProbeOutcome> {
        let cap = Capability::parse(body.capability.trim()).ok_or_else(|| {
            IngestError::from(reject(
                "unknown_capability",
                "capability must be one of ocr, vision, transcribe".to_string(),
            ))
        })?;
        let spec = ProviderInput {
            base_url: body.base_url,
            dialect: body.dialect,
            model: body.model,
            model_id: body.model_id,
            regions: body
                .regions
                .unwrap_or_else(|| vec![self.region.as_str().to_string()]),
            zdr: body.zdr.unwrap_or(true),
            timeout_ms: body.timeout_ms,
            concurrency: None,
            api_key: body.api_key,
        }
        .into_spec();
        validate(cap, &spec, self.region)?;
        if self.guarded() {
            netguard::check_provider_registration(&spec.base_url)
                .await
                .map_err(|e| url_rejection(cap, e))?;
        }
        let key = match &spec.api_key {
            Some(k) => k.clone(),
            None => {
                let stored = rows::list(&self.pool, tenant).await?;
                let Some(row) = stored
                    .iter()
                    .find(|r| r.capability == cap.as_str() && r.base_url == spec.base_url)
                else {
                    return Err(reject(
                        "secret_required_for_new_host",
                        format!(
                            "{cap}: no key is stored for this capability and base_url; send \
                             api_key"
                        ),
                    )
                    .into());
                };
                self.cipher()?.open(tenant, cap, &row.sealed_key)?
            }
        };
        Ok(match self.probe_call(cap, spec_cfg(&spec, key)).await? {
            None => ProbeOutcome {
                ok: true,
                detail: format!("{cap}: the provider answered the probe"),
            },
            Some(class) => ProbeOutcome {
                ok: false,
                detail: probe_failed_detail(cap, class),
            },
        })
    }
}

fn probe_failed_detail(cap: Capability, class: &str) -> String {
    format!("{cap}: the provider probe failed with error class {class}")
}

/// `POST /api/tenant/providers/test` body: one capability's registration as the PUT takes it.
/// `regions` defaults to the deployment region and `zdr` to `true` (a draft slot names neither);
/// `api_key` omitted = the stored key (same `base_url` only). `Debug` redacts the key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestProviderBody {
    pub capability: String,
    pub dialect: String,
    pub base_url: String,
    pub model: String,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub regions: Option<Vec<String>>,
    #[serde(default)]
    pub zdr: Option<bool>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub api_key: Option<Secret>,
}

/// `POST /api/tenant/providers/test` answer. `detail` names the capability and, on failure, the
/// error class only.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ProbeOutcome {
    pub ok: bool,
    pub detail: String,
}

/// The stored row whose key a PUT without `api_key` keeps: only for the same `base_url` (a stored
/// key is never sent to a new host). No stored row for `cap`, or a changed `base_url` → 422
/// `api_key_required` (every implemented dialect authenticates with a bearer key).
fn kept_row<'a>(
    cap: Capability,
    spec: &TenantSpec,
    stored: &'a [ProviderRow],
) -> Result<&'a ProviderRow> {
    let required = |why: &str| -> IngestError {
        reject("api_key_required", format!("{cap}: {why}; send api_key")).into()
    };
    let Some(row) = stored.iter().find(|r| r.capability == cap.as_str()) else {
        return Err(required("no key is stored for this capability"));
    };
    if row.base_url != spec.base_url {
        return Err(required(
            "base_url changed, so the stored key is not reused",
        ));
    }
    Ok(row)
}

fn new_row<'a>(spec: &'a TenantSpec, sealed: &'a super::crypto::Sealed) -> NewProviderRow<'a> {
    NewProviderRow {
        base_url: &spec.base_url,
        dialect: &spec.dialect,
        model: &spec.model,
        model_id: spec.model_id.as_deref(),
        regions: &spec.regions,
        zdr: spec.zdr,
        timeout_ms: spec.timeout_ms,
        concurrency: spec.concurrency,
        sealed_key: sealed,
    }
}

enum Probe {
    Build(String),
    Call(ProviderError),
    Other(IngestError),
}

/// A 1×1 RGBA PNG.
fn probe_png() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
        )
        .expect("a valid constant")
}

/// 0.5 s of silence: 16 kHz mono 16-bit PCM WAV.
fn probe_wav() -> Vec<u8> {
    const RATE: u32 = 16_000;
    let data_len: u32 = RATE; // 0.5 s × 2 bytes per sample
    let mut v = Vec::with_capacity(44 + data_len as usize);
    v.extend_from_slice(b"RIFF");
    v.extend_from_slice(&(36 + data_len).to_le_bytes());
    v.extend_from_slice(b"WAVEfmt ");
    v.extend_from_slice(&16u32.to_le_bytes());
    v.extend_from_slice(&1u16.to_le_bytes()); // PCM
    v.extend_from_slice(&1u16.to_le_bytes()); // mono
    v.extend_from_slice(&RATE.to_le_bytes());
    v.extend_from_slice(&(RATE * 2).to_le_bytes());
    v.extend_from_slice(&2u16.to_le_bytes());
    v.extend_from_slice(&16u16.to_le_bytes());
    v.extend_from_slice(b"data");
    v.extend_from_slice(&data_len.to_le_bytes());
    v.resize(44 + data_len as usize, 0);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> TenantSpec {
        TenantSpec {
            base_url: "https://api.mistral.ai/v1".into(),
            dialect: "mistral".into(),
            model: "mistral-ocr-latest".into(),
            model_id: None,
            regions: vec!["eu".into()],
            zdr: true,
            timeout_ms: 60_000,
            concurrency: 4,
            api_key: Some(Secret::new("sk-unit-secret")),
        }
    }

    fn code(cap: Capability, s: &TenantSpec, region: Region) -> &'static str {
        match validate(cap, s, region) {
            Ok(()) => "ok",
            Err(r) => r.code,
        }
    }

    #[test]
    fn a_valid_spec_passes_for_each_capability() {
        assert_eq!(code(Capability::Ocr, &spec(), Region::Eu), "ok");
        let openai = TenantSpec {
            dialect: "openai".into(),
            base_url: "https://api.openai.com/v1".into(),
            ..spec()
        };
        assert_eq!(code(Capability::Vision, &openai, Region::Eu), "ok");
        assert_eq!(code(Capability::Transcribe, &openai, Region::Eu), "ok");
    }

    #[test]
    fn each_rule_has_its_code() {
        let cases: Vec<(Capability, TenantSpec, &str)> = vec![
            (
                Capability::Ocr,
                TenantSpec {
                    dialect: "openai".into(),
                    ..spec()
                },
                "dialect_not_allowed",
            ),
            (Capability::Vision, spec(), "dialect_not_allowed"),
            (
                Capability::Ocr,
                TenantSpec {
                    regions: vec!["us".into()],
                    ..spec()
                },
                "region_not_allowed",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    regions: vec![],
                    ..spec()
                },
                "region_not_allowed",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    regions: vec!["eu".into(), "asia".into()],
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    zdr: false,
                    ..spec()
                },
                "zdr_required",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    base_url: "ftp://x.example.com".into(),
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    base_url: "https://u:pw@api.mistral.ai/v1".into(),
                    ..spec()
                },
                "provider_host_not_allowed",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    base_url: "https://API.mistral.ai/v1".into(),
                    ..spec()
                },
                "provider_host_not_allowed",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    model: String::new(),
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    model_id: Some(String::new()),
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    timeout_ms: 0,
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Vision,
                TenantSpec {
                    dialect: "openai".into(),
                    timeout_ms: 110_001,
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    concurrency: 0,
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    concurrency: MAX_TENANT_CONCURRENCY + 1,
                    ..spec()
                },
                "invalid_provider",
            ),
            (
                Capability::Ocr,
                TenantSpec {
                    api_key: Some(Secret::new("  ")),
                    ..spec()
                },
                "invalid_provider",
            ),
        ];
        for (cap, s, want) in cases {
            assert_eq!(code(cap, &s, Region::Eu), want, "{cap} {s:?}");
        }
        // zdr is only required in eu
        let us = TenantSpec {
            regions: vec!["us".into()],
            zdr: false,
            ..spec()
        };
        assert_eq!(code(Capability::Ocr, &us, Region::Us), "ok");
        // the timeout caps
        assert_eq!(
            code(
                Capability::Ocr,
                &TenantSpec {
                    timeout_ms: 300_000,
                    ..spec()
                },
                Region::Eu
            ),
            "ok"
        );
    }

    #[test]
    fn put_input_debug_redacts_the_key_and_fills_defaults() {
        let body: PutProviders = serde_json::from_value(serde_json::json!({
            "ocr": {
                "base_url": "https://api.mistral.ai/v1/", "dialect": "mistral", "model": "m",
                "regions": ["eu"], "zdr": true, "api_key": "sk-debug-777"
            }
        }))
        .unwrap();
        let dbg = format!("{body:?}");
        assert!(!dbg.contains("sk-debug-777"), "{dbg}");
        let s = body.ocr.unwrap().into_spec();
        assert_eq!(s.base_url, "https://api.mistral.ai/v1");
        assert_eq!(
            (s.timeout_ms, s.concurrency),
            (DEFAULT_PROVIDER_TIMEOUT_MS, DEFAULT_PROVIDER_CONCURRENCY)
        );
        assert!(!format!("{s:?}").contains("sk-debug-777"));
    }

    #[test]
    fn unknown_capabilities_and_fields_are_refused() {
        for bad in [
            serde_json::json!({"embed": null}),
            serde_json::json!({"chat": null}),
            serde_json::json!({"ocr": {"base_url": "x", "dialect": "mistral", "model": "m",
                                       "regions": [], "zdr": true, "secret": "y"}}),
        ] {
            assert!(
                serde_json::from_value::<PutProviders>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        let empty: PutProviders = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(empty.ocr.is_none() && empty.vision.is_none() && empty.transcribe.is_none());
    }

    #[test]
    fn an_unconfigured_view_has_only_its_source() {
        assert_eq!(
            serde_json::to_value(UNCONFIGURED).unwrap(),
            serde_json::json!({"source": "unconfigured"})
        );
        let d = CapabilityView {
            source: "default",
            from_store: None,
            provider: None,
            default: Some(DefaultView {
                dialect: "mistral".into(),
                model: "m".into(),
            }),
        };
        assert_eq!(
            serde_json::to_value(d).unwrap(),
            serde_json::json!({"source": "default", "dialect": "mistral", "model": "m"})
        );
    }

    #[test]
    fn probe_payloads_are_well_formed() {
        let png = probe_png();
        assert_eq!(&png[..8], b"\x89PNG\r\n\x1a\n");
        let wav = probe_wav();
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(wav.len(), 44 + 16_000);
        assert_eq!(
            u32::from_le_bytes(wav[4..8].try_into().unwrap()) as usize,
            wav.len() - 8
        );
    }
}
