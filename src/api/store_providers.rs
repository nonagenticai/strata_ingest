//! `GET/PUT/DELETE /api/stores/{id}/providers` (owner ruling D92, the strata_rag store-providers
//! contract for ocr / vision / transcribe): the providers bound to one strata store, used by every
//! job whose target store is that store or a store under it.
//!
//! The store is looked up in strata within the caller's tenant (`GET /api/stores/{id}` with
//! `X-Tenant-Id`): unknown there (also another tenant's) → 404 `unknown_store`. A leaf or a
//! router may bind (a router's rows are inherited by its leaves). The store's ancestors come from
//! the `store_lineage` cache, else from walking strata's `parent_id`s once.

use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Extension, Path, State};
use bytes::Bytes;
use uuid::Uuid;

use super::AppState;
use super::auth::Tenant;
use super::handlers::parse_providers_body;
use crate::db::store_providers as srows;
use crate::error::{IngestError, Result};
use crate::pipeline::Ctx;
use crate::providers::registry::{PutProviders, StoreProvidersView};

/// The deepest store tree walked; a longer `parent_id` chain is refused as a strata fault.
pub const MAX_STORE_DEPTH: usize = 32;

fn path_id(p: std::result::Result<Path<Uuid>, PathRejection>) -> Result<Uuid> {
    p.map(|Path(id)| id)
        .map_err(|e| IngestError::BadRequest(format!("invalid id in path: {}", e.body_text())))
}

fn lineage_fault(message: String) -> IngestError {
    IngestError::Upstream {
        service: "strata",
        message,
    }
}

/// `store`'s ancestors, nearest first (`parent` = its `parent_id` as strata just reported it).
/// Read from `store_lineage`, else walked through strata once (stopping at the first ancestor
/// whose lineage is cached) and recorded. A loop, a chain deeper than [`MAX_STORE_DEPTH`] or an
/// ancestor strata no longer knows → 502 `upstream_unavailable`.
pub(super) async fn ancestors_of(
    ctx: &Ctx,
    tenant: Uuid,
    store: Uuid,
    parent: Option<Uuid>,
) -> Result<Vec<Uuid>> {
    if let Some(cached) = srows::lineage(&ctx.pool, tenant, store).await? {
        return Ok(cached);
    }
    let mut chain: Vec<Uuid> = Vec::new();
    let mut next = parent;
    while let Some(p) = next {
        if p == store || chain.contains(&p) {
            return Err(lineage_fault(format!(
                "store {store}: its parent chain loops at {p}"
            )));
        }
        chain.push(p);
        if let Some(cached) = srows::lineage(&ctx.pool, tenant, p).await? {
            if cached.contains(&store) || cached.iter().any(|a| chain.contains(a)) {
                return Err(lineage_fault(format!(
                    "store {store}: its parent chain loops at {p}"
                )));
            }
            chain.extend(cached);
            break;
        }
        if chain.len() > MAX_STORE_DEPTH {
            return Err(lineage_fault(format!(
                "store {store}: more than {MAX_STORE_DEPTH} ancestors"
            )));
        }
        next = match ctx.strata.store(tenant, p).await? {
            Some(info) => info.parent_id,
            None => {
                return Err(lineage_fault(format!(
                    "store {store}: ancestor {p} is unknown to strata"
                )));
            }
        };
    }
    if chain.len() > MAX_STORE_DEPTH {
        return Err(lineage_fault(format!(
            "store {store}: more than {MAX_STORE_DEPTH} ancestors"
        )));
    }
    srows::put_lineage(&ctx.pool, tenant, store, &chain).await?;
    Ok(chain)
}

/// Records `store`'s lineage (a job is about to be created for it): nothing when it is cached,
/// else one strata lookup of the store and the walk of its ancestors.
pub(super) async fn record_lineage(ctx: &Ctx, tenant: Uuid, store: Uuid) -> Result<()> {
    if srows::lineage(&ctx.pool, tenant, store).await?.is_some() {
        return Ok(());
    }
    lineage(ctx, tenant, store).await.map(drop)
}

/// The store (of `tenant`, in strata) and its ancestors; unknown → 404 `unknown_store`.
async fn lineage(ctx: &Ctx, tenant: Uuid, store: Uuid) -> Result<Vec<Uuid>> {
    let Some(info) = ctx.strata.store(tenant, store).await? else {
        return Err(IngestError::UnknownStore);
    };
    ancestors_of(ctx, tenant, store, info.parent_id).await
}

/// `GET /api/stores/{id}/providers`: per capability (ocr, vision, transcribe) which registration
/// a job targeting this store uses (`store` | `ancestor` + `from_store` | `tenant` | `default` |
/// `unconfigured`) and its settings. Never a key: only `has_api_key`.
pub async fn get_store_providers(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<Json<StoreProvidersView>> {
    let store = path_id(id)?;
    let ancestors = lineage(&s.ctx, tenant, store).await?;
    Ok(Json(
        s.ctx
            .providers
            .store_view(tenant, store, &ancestors)
            .await?,
    ))
}

/// `PUT /api/stores/{id}/providers`: replaces the store's own registrations (a capability left
/// out or `null` is removed: the store inherits it again). Same body, validation, SSRF guard,
/// kept-key rule and live probe as `PUT /api/tenant/providers`. Answers the store view.
pub async fn put_store_providers(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
    body: Bytes,
) -> Result<Json<StoreProvidersView>> {
    let store = path_id(id)?;
    let put = parse_providers_body(&body)?;
    let ancestors = lineage(&s.ctx, tenant, store).await?;
    Ok(Json(
        s.ctx
            .providers
            .replace_store(tenant, store, &ancestors, put)
            .await?,
    ))
}

/// `DELETE /api/stores/{id}/providers`: removes the store's own registrations (= PUT `{}`);
/// answers the store view (the store now inherits).
pub async fn delete_store_providers(
    State(s): State<AppState>,
    Extension(Tenant(tenant)): Extension<Tenant>,
    id: std::result::Result<Path<Uuid>, PathRejection>,
) -> Result<Json<StoreProvidersView>> {
    let store = path_id(id)?;
    let ancestors = lineage(&s.ctx, tenant, store).await?;
    Ok(Json(
        s.ctx
            .providers
            .replace_store(tenant, store, &ancestors, PutProviders::default())
            .await?,
    ))
}
