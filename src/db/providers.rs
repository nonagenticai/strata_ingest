//! `tenant_providers` rows (migration 0003). Rows are always read and written for ONE tenant.

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::error::Result;
use crate::providers::Capability;
use crate::providers::crypto::{KEY_VERSION, Sealed};

/// One stored registration. `Debug` never shows the sealed key's bytes.
#[derive(Clone)]
pub struct ProviderRow {
    pub capability: String,
    pub base_url: String,
    pub dialect: String,
    pub model: String,
    pub model_id: Option<String>,
    pub regions: Vec<String>,
    pub zdr: bool,
    pub timeout_ms: i32,
    pub concurrency: i32,
    pub sealed_key: Sealed,
    pub updated_at: DateTime<Utc>,
}

impl std::fmt::Debug for ProviderRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRow")
            .field("capability", &self.capability)
            .field("dialect", &self.dialect)
            .field("model", &self.model)
            .field("updated_at", &self.updated_at)
            .finish_non_exhaustive()
    }
}

/// The values a write stores (the key already sealed).
#[derive(Debug, Clone)]
pub struct NewProviderRow<'a> {
    pub base_url: &'a str,
    pub dialect: &'a str,
    pub model: &'a str,
    pub model_id: Option<&'a str>,
    pub regions: &'a [String],
    pub zdr: bool,
    pub timeout_ms: u64,
    pub concurrency: usize,
    pub sealed_key: &'a Sealed,
}

pub(crate) const COLUMNS: &str = "capability, base_url, dialect, model, model_id, regions, zdr, timeout_ms, \
                       concurrency, api_key_ct, api_key_nonce, updated_at";

pub(crate) fn from_row(row: &sqlx::postgres::PgRow) -> Result<ProviderRow> {
    Ok(ProviderRow {
        capability: row.try_get("capability")?,
        base_url: row.try_get("base_url")?,
        dialect: row.try_get("dialect")?,
        model: row.try_get("model")?,
        model_id: row.try_get("model_id")?,
        regions: row.try_get("regions")?,
        zdr: row.try_get("zdr")?,
        timeout_ms: row.try_get("timeout_ms")?,
        concurrency: row.try_get("concurrency")?,
        sealed_key: Sealed {
            ciphertext: row.try_get("api_key_ct")?,
            nonce: row.try_get("api_key_nonce")?,
        },
        updated_at: row.try_get("updated_at")?,
    })
}

/// All of `tenant`'s registrations.
pub async fn list<'e>(exec: impl sqlx::PgExecutor<'e>, tenant: Uuid) -> Result<Vec<ProviderRow>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM tenant_providers WHERE tenant = $1 ORDER BY capability"
    )))
    .bind(tenant)
    .fetch_all(exec)
    .await?;
    rows.iter().map(from_row).collect()
}

/// `tenant`'s registration for `cap`, if any.
pub async fn get<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    cap: Capability,
) -> Result<Option<ProviderRow>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM tenant_providers WHERE tenant = $1 AND capability = $2"
    )))
    .bind(tenant)
    .bind(cap.as_str())
    .fetch_optional(exec)
    .await?;
    row.as_ref().map(from_row).transpose()
}

/// `updated_at` of `tenant`'s `cap` registration: the cheap per-step check that a cached client
/// is still current (also when another replica took the PUT).
pub async fn version<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    cap: Capability,
) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query_scalar(
        "SELECT updated_at FROM tenant_providers WHERE tenant = $1 AND capability = $2",
    )
    .bind(tenant)
    .bind(cap.as_str())
    .fetch_optional(exec)
    .await?)
}

/// Inserts or replaces `tenant`'s `cap` registration; `updated_at` moves on every write.
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    tenant: Uuid,
    cap: Capability,
    row: &NewProviderRow<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO tenant_providers (tenant, capability, base_url, dialect, model, model_id, \
                                       regions, zdr, timeout_ms, concurrency, api_key_ct, \
                                       api_key_nonce, key_version, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, clock_timestamp()) \
         ON CONFLICT (tenant, capability) DO UPDATE SET \
           base_url = EXCLUDED.base_url, dialect = EXCLUDED.dialect, model = EXCLUDED.model, \
           model_id = EXCLUDED.model_id, regions = EXCLUDED.regions, zdr = EXCLUDED.zdr, \
           timeout_ms = EXCLUDED.timeout_ms, concurrency = EXCLUDED.concurrency, \
           api_key_ct = EXCLUDED.api_key_ct, api_key_nonce = EXCLUDED.api_key_nonce, \
           key_version = EXCLUDED.key_version, updated_at = EXCLUDED.updated_at",
    )
    .bind(tenant)
    .bind(cap.as_str())
    .bind(row.base_url)
    .bind(row.dialect)
    .bind(row.model)
    .bind(row.model_id)
    .bind(row.regions)
    .bind(row.zdr)
    .bind(i32::try_from(row.timeout_ms).unwrap_or(i32::MAX))
    .bind(i32::try_from(row.concurrency).unwrap_or(i32::MAX))
    .bind(row.sealed_key.ciphertext.as_slice())
    .bind(row.sealed_key.nonce.as_slice())
    .bind(KEY_VERSION)
    .execute(conn)
    .await?;
    Ok(())
}

/// Removes `tenant`'s `cap` registration (no-op when absent).
pub async fn delete(conn: &mut sqlx::PgConnection, tenant: Uuid, cap: Capability) -> Result<()> {
    sqlx::query("DELETE FROM tenant_providers WHERE tenant = $1 AND capability = $2")
        .bind(tenant)
        .bind(cap.as_str())
        .execute(conn)
        .await?;
    Ok(())
}

/// True when any tenant has a registration (every one holds a sealed key): the process then needs
/// `STRATA_INGEST_PROVIDER_KEY` to serve those tenants.
pub async fn any_stored_key(pool: &sqlx::PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tenant_providers)")
            .fetch_one(pool)
            .await?,
    )
}
