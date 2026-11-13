//! `store_providers` rows and the `store_lineage` cache (migration 0008). Rows are always read and
//! written for ONE tenant; a store id never reaches a row of another tenant.

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::providers::{COLUMNS, NewProviderRow, ProviderRow, from_row};
use crate::error::Result;
use crate::providers::Capability;
use crate::providers::crypto::KEY_VERSION;

/// The rows of `tenant` held by any store of `stores`, each with its store.
pub async fn list_for<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    stores: &[Uuid],
) -> Result<Vec<(Uuid, ProviderRow)>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT store, {COLUMNS} FROM store_providers WHERE tenant = $1 AND store = ANY($2) \
         ORDER BY store, capability"
    )))
    .bind(tenant)
    .bind(stores)
    .fetch_all(exec)
    .await?;
    rows.iter()
        .map(|r| -> Result<(Uuid, ProviderRow)> { Ok((r.try_get("store")?, from_row(r)?)) })
        .collect()
}

/// `store`'s own row for `cap`, if any.
pub async fn get<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    store: Uuid,
    cap: Capability,
) -> Result<Option<ProviderRow>> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {COLUMNS} FROM store_providers \
         WHERE tenant = $1 AND store = $2 AND capability = $3"
    )))
    .bind(tenant)
    .bind(store)
    .bind(cap.as_str())
    .fetch_optional(exec)
    .await?;
    row.as_ref().map(from_row).transpose()
}

/// For a step of `job` (of `tenant`): the store holding the `cap` row nearest to the job's store
/// (the store itself, else its nearest ancestor by the cached lineage) and that row's
/// `updated_at`. `None` when no store of the lineage binds `cap`. One indexed query per step.
pub async fn nearest_for_job<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    job: Uuid,
    cap: Capability,
) -> Result<Option<(Uuid, DateTime<Utc>)>> {
    let row = sqlx::query(
        "SELECT sp.store, sp.updated_at
         FROM ingest_jobs j
         LEFT JOIN store_lineage l ON l.tenant = j.tenant AND l.store = j.store_id
         CROSS JOIN LATERAL unnest(array_prepend(j.store_id, coalesce(l.ancestors, '{}'::uuid[])))
           WITH ORDINALITY AS c(store, pos)
         JOIN store_providers sp
           ON sp.tenant = j.tenant AND sp.store = c.store AND sp.capability = $3
         WHERE j.id = $1 AND j.tenant = $2
         ORDER BY c.pos
         LIMIT 1",
    )
    .bind(job)
    .bind(tenant)
    .bind(cap.as_str())
    .fetch_optional(exec)
    .await?;
    row.map(|r| -> Result<(Uuid, DateTime<Utc>)> {
        Ok((r.try_get("store")?, r.try_get("updated_at")?))
    })
    .transpose()
}

/// Inserts or replaces `store`'s `cap` row; `updated_at` moves on every write.
pub async fn upsert(
    conn: &mut sqlx::PgConnection,
    tenant: Uuid,
    store: Uuid,
    cap: Capability,
    row: &NewProviderRow<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO store_providers (tenant, store, capability, base_url, dialect, model, \
                                      model_id, regions, zdr, timeout_ms, concurrency, \
                                      api_key_ct, api_key_nonce, key_version, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, clock_timestamp()) \
         ON CONFLICT (tenant, store, capability) DO UPDATE SET \
           base_url = EXCLUDED.base_url, dialect = EXCLUDED.dialect, model = EXCLUDED.model, \
           model_id = EXCLUDED.model_id, regions = EXCLUDED.regions, zdr = EXCLUDED.zdr, \
           timeout_ms = EXCLUDED.timeout_ms, concurrency = EXCLUDED.concurrency, \
           api_key_ct = EXCLUDED.api_key_ct, api_key_nonce = EXCLUDED.api_key_nonce, \
           key_version = EXCLUDED.key_version, updated_at = EXCLUDED.updated_at",
    )
    .bind(tenant)
    .bind(store)
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

/// Removes `store`'s `cap` row (no-op when absent).
pub async fn delete(
    conn: &mut sqlx::PgConnection,
    tenant: Uuid,
    store: Uuid,
    cap: Capability,
) -> Result<()> {
    sqlx::query("DELETE FROM store_providers WHERE tenant = $1 AND store = $2 AND capability = $3")
        .bind(tenant)
        .bind(store)
        .bind(cap.as_str())
        .execute(conn)
        .await?;
    Ok(())
}

/// True when any store has a row (every one holds a sealed key).
pub async fn any_stored_key(pool: &sqlx::PgPool) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM store_providers)")
            .fetch_one(pool)
            .await?,
    )
}

/// `store`'s cached ancestors (nearest first), if its lineage was read before.
pub async fn lineage<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    store: Uuid,
) -> Result<Option<Vec<Uuid>>> {
    Ok(
        sqlx::query_scalar("SELECT ancestors FROM store_lineage WHERE tenant = $1 AND store = $2")
            .bind(tenant)
            .bind(store)
            .fetch_optional(exec)
            .await?,
    )
}

/// Records `store`'s ancestors (nearest first). A lineage never changes (strata does not
/// re-parent), so an existing row is kept.
pub async fn put_lineage<'e>(
    exec: impl sqlx::PgExecutor<'e>,
    tenant: Uuid,
    store: Uuid,
    ancestors: &[Uuid],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO store_lineage (tenant, store, ancestors) VALUES ($1, $2, $3)
         ON CONFLICT (tenant, store) DO NOTHING",
    )
    .bind(tenant)
    .bind(store)
    .bind(ancestors)
    .execute(exec)
    .await?;
    Ok(())
}
