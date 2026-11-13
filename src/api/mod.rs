//! HTTP API (axum 0.8): unversioned `/api/ingest/...` routes, `/api/tenant/providers` (the
//! tenant's own ocr / vision / transcribe providers, `/test` probes one without storing),
//! `/api/stores/{id}/providers` and `/api/stores/{id}/reingest-copy` and the unauthenticated `/health`,
//! on the same [`Ctx`] as the worker pool. Layers as strata_rag: bearer auth (→ [`Tenant`]),
//! request body limit (`max_upload_bytes + 1 MiB`, 413 rewritten to the JSON error body), trace.
//!
//! The domain error's `IntoResponse` lives in `crate::error` (no second impl here).
//!
//! [`Tenant`]: auth::Tenant

pub mod auth;
pub mod handlers;
pub mod health;
pub mod reingest_copy;
pub mod sources;
pub mod store_providers;
pub mod upload;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, map_response};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::error::{IngestError, Result};
use crate::pipeline::Ctx;

/// Room for the multipart framing and the non-file fields on top of one maximal file.
pub const BODY_OVERHEAD: u64 = 1024 * 1024;
/// After a shutdown signal: how long in-flight requests, then the workers' current steps, may
/// take before `serve` returns anyway (an abandoned step's lease expires and it is reclaimed).
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-request API timings. Production uses the defaults; tests shorten them.
#[derive(Debug, Clone, Copy)]
pub struct ApiOpts {
    /// An upload whose body sends no bytes for this long is aborted with 408 `request_timeout`.
    pub upload_idle_timeout: Duration,
    /// Multipart upload requests handled at once; one more is refused 503 `busy` with
    /// `Retry-After` (rulings R38). Bounds the scratch disk (each spools up to
    /// `max_upload_bytes`: 8 × 100 MiB inside the pod's 4 Gi `/tmp`) and the bodies in flight.
    pub max_concurrent_uploads: usize,
    /// How long `DELETE /api/ingest/jobs/{id}` waits for its blob + row deletes (which run in a
    /// task of their own) before it answers `202` with the job still `deleting`.
    pub delete_wait: Duration,
}

impl Default for ApiOpts {
    fn default() -> Self {
        Self {
            upload_idle_timeout: upload::UPLOAD_IDLE_TIMEOUT,
            max_concurrent_uploads: upload::MAX_CONCURRENT_UPLOADS,
            delete_wait: handlers::DELETE_WAIT,
        }
    }
}

/// Handler state: the shared context, the `/health` cache and the API timings.
#[derive(Clone)]
pub struct AppState {
    pub ctx: Arc<Ctx>,
    pub health: Arc<health::HealthCache>,
    pub opts: ApiOpts,
    /// [`ApiOpts::max_concurrent_uploads`] permits.
    pub uploads: Arc<tokio::sync::Semaphore>,
    /// The database is reachable and migrated: job routes are served (else 503 `not_ready`).
    pub ready: Arc<AtomicBool>,
}

/// How long a refused request's remaining body (503 `busy` / `not_ready`, an upload error) is
/// read and discarded before the error is sent, so the client sees the answer instead of a
/// connection reset. Bounded in bytes by the body limit layer.
pub const REFUSED_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Reads and discards `body` for at most [`REFUSED_DRAIN_TIMEOUT`].
pub(crate) async fn drain(body: Body) {
    use futures::StreamExt;
    let mut stream = body.into_data_stream();
    let all = async { while let Some(Ok(_)) = stream.next().await {} };
    let _ = tokio::time::timeout(REFUSED_DRAIN_TIMEOUT, all).await;
}

/// Answers every route but `/health` with 503 `not_ready` (body drained) until `ready`.
async fn readiness_gate(
    State(ready): State<Arc<AtomicBool>>,
    req: Request,
    next: Next,
) -> Response {
    if ready.load(Ordering::Acquire) || req.uri().path() == "/health" {
        return next.run(req).await;
    }
    drain(req.into_body()).await;
    IngestError::NotReady.into_response()
}

pub fn router(ctx: Arc<Ctx>) -> Router {
    router_with(ctx, ApiOpts::default())
}

/// [`router`] with explicit timings.
pub fn router_with(ctx: Arc<Ctx>, opts: ApiOpts) -> Router {
    router_gated(ctx, opts, Arc::new(AtomicBool::new(true)))
}

/// [`router_with`] whose job routes answer 503 `not_ready` until `ready` is set.
pub fn router_gated(ctx: Arc<Ctx>, opts: ApiOpts, ready: Arc<AtomicBool>) -> Router {
    let licence =
        ctx.cfg.licence.as_ref().map(|c| {
            auth::LicenceIntrospector::new(c).expect("licence introspect HTTP client builds")
        });
    let tokens = auth::Auth::new(auth::ApiTokens::new(&ctx.cfg.tokens), licence);
    let limit = usize::try_from(
        ctx.cfg
            .limits
            .max_upload_bytes
            .saturating_add(BODY_OVERHEAD),
    )
    .unwrap_or(usize::MAX);
    let state = AppState {
        ctx,
        health: Arc::default(),
        uploads: Arc::new(tokio::sync::Semaphore::new(
            opts.max_concurrent_uploads.max(1),
        )),
        opts,
        ready: ready.clone(),
    };
    Router::new()
        .route(
            "/api/ingest/jobs",
            post(handlers::create_jobs).get(handlers::list_jobs),
        )
        .route(
            "/api/ingest/jobs/{id}",
            get(handlers::get_job).delete(handlers::delete_job),
        )
        .route("/api/ingest/jobs/{id}/cancel", post(handlers::cancel_job))
        .route("/api/ingest/jobs/{id}/retry", post(handlers::retry_job))
        .route(
            "/api/tenant/providers",
            get(handlers::get_tenant_providers).put(handlers::put_tenant_providers),
        )
        .route(
            "/api/tenant/providers/test",
            post(handlers::test_tenant_provider),
        )
        .route(
            "/api/stores/{id}/providers",
            get(store_providers::get_store_providers)
                .put(store_providers::put_store_providers)
                .delete(store_providers::delete_store_providers),
        )
        .route(
            "/api/stores/{id}/reingest-copy",
            post(reingest_copy::start_reingest_copy),
        )
        .route(
            "/api/stores/{id}/reingest-copy/{job}",
            get(reingest_copy::get_reingest_copy),
        )
        .route("/health", get(handlers::health))
        .with_state(state)
        // Inside the auth layer: unauthenticated requests still get 401, not a hint.
        .layer(axum::middleware::from_fn_with_state(ready, readiness_gate))
        // Every route (and every unknown path) but `/health` needs a bearer token; the token
        // decides the tenant. Unauthenticated requests learn nothing about routes.
        .layer(axum::middleware::from_fn_with_state(
            tokens,
            auth::require_token,
        ))
        // axum's own 2 MB default would win over the tower-http limit; disable it.
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(limit))
        // tower-http answers 413 in plain text; rewrite it into the standard JSON error body.
        .layer(map_response(json_413))
        .layer(TraceLayer::new_for_http())
}

/// Rewrites any 413 into `{"error":{"code":"payload_too_large",...}}`.
async fn json_413(resp: Response) -> Response {
    if resp.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return IngestError::PayloadTooLarge.into_response();
    }
    resp
}

/// Runs the API on `listener` and `ctx.cfg.workers` workers until `signal` resolves; then stops
/// accepting, lets in-flight requests finish, cancels the workers (each gives the step it holds
/// `worker::SHUTDOWN_GRACE` to finish, then releases it for another pod) and returns. Each drain
/// phase is bounded by [`DRAIN_TIMEOUT`]. The database must already be migrated.
pub async fn serve(
    ctx: Arc<Ctx>,
    listener: TcpListener,
    signal: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    serve_starting(ctx, listener, signal, async { Ok(()) }).await
}

/// [`serve`] with a `startup` phase (the DB connect + migrations, retried): the API listens at
/// once — `/health` answers (db `down` until ready), every other route 503 `not_ready` — and
/// the workers start only once `startup` succeeded. A failed `startup` stops the server and is
/// returned (the process exits non-zero); a signal during `startup` stops cleanly.
pub async fn serve_starting(
    ctx: Arc<Ctx>,
    listener: TcpListener,
    signal: impl Future<Output = ()> + Send + 'static,
    startup: impl Future<Output = Result<()>> + Send + 'static,
) -> Result<()> {
    let shutdown = CancellationToken::new();
    let ready = Arc::new(AtomicBool::new(false));
    let workers = tokio::spawn({
        let (ctx, shutdown, ready) = (ctx.clone(), shutdown.clone(), ready.clone());
        async move {
            tokio::select! {
                r = startup => if let Err(e) = r {
                    shutdown.cancel();
                    return Err(e);
                },
                () = shutdown.cancelled() => return Ok(()),
            }
            ready.store(true, Ordering::Release);
            tracing::info!(
                workers = ctx.cfg.workers,
                "database ready; starting workers"
            );
            // Cost events wait in metering_outbox (written with each step's result); the drainer
            // delivers them while METERING_REDIS_URL is set. It stops after the workers, so the
            // last steps' events get a final pass.
            let drain_stop = CancellationToken::new();
            let drainer = match ctx.cfg.metering.clone() {
                Some(m) => Some(tokio::spawn(crate::metering::run_drainer(
                    ctx.pool.clone(),
                    m,
                    drain_stop.clone(),
                ))),
                None => {
                    tracing::info!(
                        "metering transport off (METERING_REDIS_URL unset): cost events are kept \
                         in metering_outbox until it is set"
                    );
                    None
                }
            };
            // `deleting` jobs nobody finishes (pod died mid-delete, storage_service failed).
            let sweeper = tokio::spawn(crate::worker::run_delete_sweeper(
                ctx.clone(),
                crate::worker::DELETE_SWEEP_INTERVAL,
                crate::db::jobs::DELETING_STALE_AFTER,
                shutdown.clone(),
            ));
            // Re-ingest copies (`reingest`): a drive in progress is dropped mid-way at shutdown;
            // its lease expires and another pod resumes it.
            let reingest = tokio::spawn(crate::reingest::run_driver(
                ctx.clone(),
                crate::reingest::DRIVE_INTERVAL,
                shutdown.clone(),
            ));
            crate::worker::run_workers(ctx.clone(), ctx.cfg.workers, shutdown).await;
            drain_stop.cancel();
            if let Some(d) = drainer {
                let _ = d.await;
            }
            // A pass in progress is dropped mid-way; the next pod's sweeper resumes it.
            sweeper.abort();
            reingest.abort();
            Ok::<(), IngestError>(())
        }
    });
    let addr = listener.local_addr()?;
    tracing::info!(%addr, workers = ctx.cfg.workers, "strata-ingest API listening");
    let trigger = shutdown.clone();
    let stop = shutdown.clone();
    let server = axum::serve(listener, router_gated(ctx, ApiOpts::default(), ready))
        .with_graceful_shutdown(async move {
            tokio::select! {
                () = signal => tracing::info!("shutdown signal received, draining"),
                () = stop.cancelled() => tracing::info!("startup failed, stopping"),
            }
            trigger.cancel();
        })
        .into_future();
    tokio::pin!(server);
    let served = tokio::select! {
        r = &mut server => r,
        () = async {
            shutdown.cancelled().await;
            tokio::time::sleep(DRAIN_TIMEOUT).await;
        } => {
            tracing::warn!("in-flight requests did not finish within the drain timeout");
            Ok(())
        }
    };
    shutdown.cancel();
    let started = match tokio::time::timeout(DRAIN_TIMEOUT, workers).await {
        Ok(Ok(r)) => {
            if r.is_ok() {
                tracing::info!("workers stopped");
            }
            r
        }
        Ok(Err(e)) => {
            tracing::error!(error = %e, "worker pool ended abnormally");
            Ok(())
        }
        Err(_) => {
            tracing::warn!(
                "workers did not stop within the drain timeout; unreleased leases will expire"
            );
            Ok(())
        }
    };
    started?;
    served?;
    Ok(())
}

/// Resolves on SIGTERM or ctrl-c.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "cannot listen for ctrl-c");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
