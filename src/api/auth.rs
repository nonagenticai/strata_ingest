//! Bearer token check (`STRATA_INGEST_API_TOKENS`) in front of every route except `/health`.
//! The matched token decides the caller's tenant, handed to handlers as a [`Tenant`] extension.
//!
//! As strata_rag: tokens are compared as SHA-256 digests with `subtle`'s constant-time equality,
//! against every configured token without an early exit, and the tenant is picked with a
//! constant-time select, so neither which token matched nor any token's length shows in the
//! response time. Tokens are never logged.
//!
//! Licence tokens (only when `STRATA_INGEST_LICENSE_INTROSPECT_URL` is set): a bearer that
//! matches no static token is sent to the licences service's `POST /license/introspect`
//! (`{"token"}` → `{active, tenant_id, app: {key, revoked}, …}`). Accepted only when `active`,
//! the licence's `app.key` is one of `STRATA_INGEST_LICENSE_APP_KEYS` and the app is not revoked;
//! the licence's `tenant_id` is the caller's tenant. An `X-Tenant-Id` header that names another
//! tenant is 403 `tenant_mismatch`. Positive answers are cached for at most 60 s (bounded, keyed by
//! the token's SHA-256); negatives are not cached. Fail closed: an introspect transport error,
//! non-2xx or unparseable answer is 503 `licence_unavailable`; an inactive licence is 401;
//! an active licence of another app is 403 `licence_app_not_allowed`. Without the URL, behaviour is exactly the static-token check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, HeaderName, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::{Choice, ConditionallySelectable, ConstantTimeEq};
use uuid::Uuid;

use crate::config::{LicenceCfg, Secret};

/// The only path served without a token (matched exactly).
pub const PUBLIC_PATH: &str = "/health";

const UNAUTHORIZED_MESSAGE: &str = "missing or invalid bearer token";

/// The authenticated caller's tenant; every job read and write is scoped by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tenant(pub Uuid);

/// Digests of the accepted tokens with their tenants. There is no "auth off" mode: config
/// requires at least one token, and with none every request but `/health` is refused.
#[derive(Clone, Default)]
pub struct ApiTokens {
    entries: Arc<Vec<([u8; 32], [u8; 16])>>,
}

impl ApiTokens {
    pub fn new(tokens: &[(Uuid, Secret)]) -> Self {
        Self {
            entries: Arc::new(
                tokens
                    .iter()
                    .map(|(tenant, t)| (digest(t.expose()), *tenant.as_bytes()))
                    .collect(),
            ),
        }
    }

    /// The tenant of `presented`, in constant time over all tokens (config rejects duplicate
    /// tokens, so at most one matches).
    pub fn resolve(&self, presented: &str) -> Option<Uuid> {
        let d = digest(presented);
        let mut hit = Choice::from(0);
        let mut tenant = [0u8; 16];
        for (token, id) in self.entries.iter() {
            let m = token.ct_eq(&d);
            for (out, b) in tenant.iter_mut().zip(id) {
                *out = u8::conditional_select(out, b, m);
            }
            hit |= m;
        }
        bool::from(hit).then(|| Uuid::from_bytes(tenant))
    }
}

/// How long a positive introspect answer is reused.
pub const LICENCE_CACHE_TTL: Duration = Duration::from_secs(60);
/// Most cached licence tokens; a full cache first drops expired entries, then everything.
pub const LICENCE_CACHE_MAX: usize = 4096;
/// Whole introspect exchange.
const INTROSPECT_TIMEOUT: Duration = Duration::from_secs(5);

static X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");

/// Why a licence token was not turned into a tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenceError {
    /// Not an active licence (unknown, inactive, revoked app, no tenant): 401.
    Invalid,
    /// An active licence of an app not in `STRATA_INGEST_LICENSE_APP_KEYS`: authenticated, not
    /// entitled (403 `licence_app_not_allowed`).
    AppNotAllowed,
    /// The licences service could not answer (503; fail closed).
    Unavailable,
}

#[derive(Deserialize)]
struct Introspection {
    active: bool,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    app: Option<IntrospectedApp>,
}

#[derive(Deserialize)]
struct IntrospectedApp {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    revoked: Option<bool>,
}

/// Client for the licences service's `POST /license/introspect`, with the positive cache.
pub struct LicenceIntrospector {
    http: reqwest::Client,
    url: String,
    app_keys: Vec<String>,
    ttl: Duration,
    max: usize,
    cache: Mutex<HashMap<[u8; 32], (Uuid, Instant)>>,
}

impl LicenceIntrospector {
    pub fn new(cfg: &LicenceCfg) -> crate::Result<Self> {
        Self::with_cache(cfg, LICENCE_CACHE_TTL, LICENCE_CACHE_MAX)
    }

    /// [`Self::new`] with an explicit cache TTL (capped at [`LICENCE_CACHE_TTL`]) and size.
    pub fn with_cache(cfg: &LicenceCfg, ttl: Duration, max: usize) -> crate::Result<Self> {
        Ok(Self {
            http: crate::infra::http_client(INTROSPECT_TIMEOUT)?,
            url: cfg.introspect_url.clone(),
            app_keys: cfg.app_keys.clone(),
            ttl: ttl.min(LICENCE_CACHE_TTL),
            max: max.max(1),
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// The tenant of the licence `token`, from the cache or the licences service.
    pub async fn resolve(&self, token: &str) -> Result<Uuid, LicenceError> {
        let key = digest(token);
        if let Some(tenant) = self.cached(&key) {
            return Ok(tenant);
        }
        let tenant = self.introspect(token).await?;
        self.store(key, tenant);
        Ok(tenant)
    }

    fn cached(&self, key: &[u8; 32]) -> Option<Uuid> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        match cache.get(key).map(|(t, at)| (*t, at.elapsed() < self.ttl)) {
            Some((tenant, true)) => Some(tenant),
            Some((_, false)) => {
                cache.remove(key);
                None
            }
            None => None,
        }
    }

    fn store(&self, key: [u8; 32], tenant: Uuid) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= self.max && !cache.contains_key(&key) {
            let ttl = self.ttl;
            cache.retain(|_, (_, at)| at.elapsed() < ttl);
            if cache.len() >= self.max {
                cache.clear();
            }
        }
        cache.insert(key, (tenant, Instant::now()));
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    async fn introspect(&self, token: &str) -> Result<Uuid, LicenceError> {
        let resp = self
            .http
            .post(&self.url)
            .json(&json!({ "token": token }))
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(error = %crate::infra::transport_message(e), "licence introspect failed");
                LicenceError::Unavailable
            })?;
        let status = resp.status();
        if !status.is_success() {
            tracing::warn!(
                status = status.as_u16(),
                "licence introspect answered non-2xx"
            );
            return Err(LicenceError::Unavailable);
        }
        let body: Introspection = resp.json().await.map_err(|e| {
            tracing::warn!(error = %e.without_url(), "licence introspect answer unreadable");
            LicenceError::Unavailable
        })?;
        if !body.active {
            return Err(LicenceError::Invalid);
        }
        let Some(app) = body.app.as_ref().filter(|a| a.revoked != Some(true)) else {
            return Err(LicenceError::Invalid);
        };
        let Some(key) = app.key.as_deref() else {
            return Err(LicenceError::Invalid);
        };
        if !self.app_keys.iter().any(|a| a == key) {
            return Err(LicenceError::AppNotAllowed);
        }
        body.tenant_id
            .as_deref()
            .and_then(|t| Uuid::parse_str(t.trim()).ok())
            .filter(|t| !t.is_nil())
            .ok_or(LicenceError::Invalid)
    }
}

/// The auth middleware's state: the static tokens plus, when configured, licence introspection.
#[derive(Clone, Default)]
pub struct Auth {
    pub tokens: ApiTokens,
    pub licence: Option<Arc<LicenceIntrospector>>,
}

impl Auth {
    pub fn new(tokens: ApiTokens, licence: Option<LicenceIntrospector>) -> Self {
        Self {
            tokens,
            licence: licence.map(Arc::new),
        }
    }
}

impl From<ApiTokens> for Auth {
    fn from(tokens: ApiTokens) -> Self {
        Self {
            tokens,
            licence: None,
        }
    }
}

/// An `X-Tenant-Id` header, when present, must agree with the licence: the tenant uuid itself,
/// or `Bearer <the same token>` (the estate's licence-header form).
fn tenant_header_agrees(headers: &HeaderMap, tenant: Uuid, token: &str) -> bool {
    let Some(v) = headers.get(&X_TENANT_ID) else {
        return true;
    };
    let Ok(v) = v.to_str() else {
        return false;
    };
    let v = v.trim();
    if let Some((scheme, t)) = v.split_once(' ')
        && scheme.eq_ignore_ascii_case("bearer")
    {
        return bool::from(digest(t.trim()).ct_eq(&digest(token)));
    }
    Uuid::parse_str(v).is_ok_and(|h| h == tenant)
}

fn digest(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

/// `Authorization: Bearer <token>`: scheme case-insensitive, surrounding whitespace ignored.
fn bearer(headers: &HeaderMap) -> Option<&str> {
    let v = headers.get(AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, token) = v.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
}

pub async fn require_token(State(auth): State<Auth>, mut req: Request, next: Next) -> Response {
    if req.uri().path() == PUBLIC_PATH {
        return next.run(req).await;
    }
    // Owned: the request is mutated (extensions) while the token is still needed.
    let Some(token) = bearer(req.headers()).map(str::to_string) else {
        return unauthorized();
    };
    if let Some(tenant) = auth.tokens.resolve(&token) {
        req.extensions_mut().insert(Tenant(tenant));
        return next.run(req).await;
    }
    let Some(licence) = &auth.licence else {
        return unauthorized();
    };
    match licence.resolve(&token).await {
        Ok(tenant) if tenant_header_agrees(req.headers(), tenant, &token) => {
            req.extensions_mut().insert(Tenant(tenant));
            next.run(req).await
        }
        Ok(_) => error_response(
            StatusCode::FORBIDDEN,
            "tenant_mismatch",
            "X-Tenant-Id does not match the licence's tenant",
        ),
        Err(LicenceError::Invalid) => unauthorized(),
        Err(LicenceError::AppNotAllowed) => error_response(
            StatusCode::FORBIDDEN,
            "licence_app_not_allowed",
            "the licence does not grant access to this service",
        ),
        Err(LicenceError::Unavailable) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "licence_unavailable",
            "licence check unavailable, try again",
        ),
    }
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}

/// 401 in the standard `{"error":{"code","message"}}` body, with `WWW-Authenticate: Bearer`.
fn unauthorized() -> Response {
    let mut resp = (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": {"code": "unauthorized", "message": UNAUTHORIZED_MESSAGE}})),
    )
        .into_response();
    resp.headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_str(v).unwrap());
        h
    }

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer(&headers("Bearer abc")), Some("abc"));
        assert_eq!(bearer(&headers("  bearer   abc  ")), Some("abc"));
        assert_eq!(bearer(&headers("BEARER abc")), Some("abc"));
        assert_eq!(bearer(&headers("Basic abc")), None);
        assert_eq!(bearer(&headers("Bearer ")), None);
        assert_eq!(bearer(&headers("abc")), None);
        assert_eq!(bearer(&HeaderMap::new()), None);
    }

    #[test]
    fn tokens_resolve_to_their_own_tenant_only() {
        let (a, b) = (Uuid::now_v7(), Uuid::now_v7());
        let t = ApiTokens::new(&[(a, Secret::new("tok-one")), (b, Secret::new("tok-two"))]);
        assert_eq!(t.resolve("tok-one"), Some(a));
        assert_eq!(t.resolve("tok-two"), Some(b));
        for bad in ["tok-on", "tok-one1", "tok-onetok-two", "", "TOK-ONE"] {
            assert_eq!(t.resolve(bad), None, "{bad}");
        }
        assert_eq!(ApiTokens::new(&[]).resolve("tok-one"), None);
    }

    /// `/health` is the only open path, matched exactly.
    #[tokio::test]
    async fn only_the_exact_health_path_is_open() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let tokens = ApiTokens::new(&[(Uuid::now_v7(), Secret::new("tok"))]);
        let app = Router::new().fallback(|| async { StatusCode::OK }).layer(
            axum::middleware::from_fn_with_state(Auth::from(tokens), require_token),
        );
        let status = |uri: &'static str| {
            let app = app.clone();
            async move {
                let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
                app.oneshot(req).await.unwrap().status()
            }
        };
        assert_eq!(status("/health").await, StatusCode::OK);
        assert_eq!(status("/health?verbose=1").await, StatusCode::OK);
        for gated in [
            "/healthz",
            "/health/",
            "/health/x",
            "/Health",
            "//health",
            "/api/health",
            "/api/ingest/jobs",
        ] {
            assert_eq!(status(gated).await, StatusCode::UNAUTHORIZED, "{gated}");
        }
    }

    // ---- licence introspection (mock licences service) ----

    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::body::Body;
    use axum::routing::post;
    use tower::ServiceExt;

    const LIC_TENANT: &str = "0199f0a0-2222-7000-8000-000000000002";

    /// Mock `POST /license/introspect`: `lic-ok` is an active strata-ingest licence, `lic-other`
    /// an active licence of another app, `lic-revoked-app` one whose app is revoked, `lic-500`
    /// fails, `lic-garbage` answers non-JSON; anything else is `token_not_found`.
    async fn mock_licences() -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let app = Router::new().route(
            "/license/introspect",
            post(move |Json(body): Json<serde_json::Value>| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    let lic = |key: &str, revoked: bool| {
                        json!({"active": true, "tenant_id": LIC_TENANT, "license_id": "l1",
                               "status": "active",
                               "app": {"id": "a1", "key": key, "revoked": revoked}})
                    };
                    match body["token"].as_str().unwrap_or("") {
                        "lic-ok" => {
                            (StatusCode::OK, Json(lic("strata-ingest", false))).into_response()
                        }
                        "lic-other" => (StatusCode::OK, Json(lic("kanban", false))).into_response(),
                        "lic-revoked-app" => {
                            (StatusCode::OK, Json(lic("strata-ingest", true))).into_response()
                        }
                        "lic-500" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                        "lic-garbage" => (StatusCode::OK, "not json").into_response(),
                        _ => (
                            StatusCode::OK,
                            Json(json!({"active": false, "reason": "token_not_found"})),
                        )
                            .into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/license/introspect"), calls)
    }

    fn lic_cfg(url: &str) -> LicenceCfg {
        LicenceCfg {
            introspect_url: url.to_string(),
            app_keys: vec!["strata-ingest".to_string()],
        }
    }

    const STATIC_TENANT: &str = "0199f0a0-1111-7000-8000-000000000001";

    /// A router echoing the resolved tenant, behind `require_token` with `auth`.
    fn echo_app(auth: Auth) -> Router {
        Router::new()
            .fallback(|req: Request| async move {
                req.extensions()
                    .get::<Tenant>()
                    .map(|t| t.0.to_string())
                    .unwrap_or_default()
            })
            .layer(axum::middleware::from_fn_with_state(auth, require_token))
    }

    async fn call(app: &Router, bearer: &str, x_tenant: Option<&str>) -> (StatusCode, String) {
        let mut b = axum::http::Request::builder()
            .uri("/api/ingest/jobs")
            .header(AUTHORIZATION, format!("Bearer {bearer}"));
        if let Some(x) = x_tenant {
            b = b.header("x-tenant-id", x);
        }
        let resp = app
            .clone()
            .oneshot(b.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    fn static_tokens() -> ApiTokens {
        ApiTokens::new(&[(
            Uuid::parse_str(STATIC_TENANT).unwrap(),
            Secret::new("static-tok"),
        )])
    }

    #[tokio::test]
    async fn licence_token_resolves_to_licence_tenant() {
        let (url, calls) = mock_licences().await;
        let lic = LicenceIntrospector::new(&lic_cfg(&url)).unwrap();
        let app = echo_app(Auth::new(static_tokens(), Some(lic)));

        // Static token: unchanged, never introspected.
        assert_eq!(
            call(&app, "static-tok", None).await,
            (StatusCode::OK, STATIC_TENANT.to_string())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        assert_eq!(
            call(&app, "lic-ok", None).await,
            (StatusCode::OK, LIC_TENANT.to_string())
        );
        // Agreeing X-Tenant-Id (uuid form, or Bearer of the same token) is fine.
        assert_eq!(
            call(&app, "lic-ok", Some(LIC_TENANT)).await.0,
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "lic-ok", Some("Bearer lic-ok")).await.0,
            StatusCode::OK
        );
        // Second+ calls came from the cache.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disagreeing_tenant_header_is_403() {
        let (url, _) = mock_licences().await;
        let lic = LicenceIntrospector::new(&lic_cfg(&url)).unwrap();
        let app = echo_app(Auth::new(static_tokens(), Some(lic)));
        for bad in [STATIC_TENANT, "not-a-uuid", "Bearer other-token", ""] {
            let (s, body) = call(&app, "lic-ok", Some(bad)).await;
            assert_eq!(s, StatusCode::FORBIDDEN, "{bad:?}");
            assert!(body.contains("tenant_mismatch"), "{body}");
        }
    }

    #[tokio::test]
    async fn invalid_licences_are_401_and_not_cached() {
        let (url, calls) = mock_licences().await;
        let lic = LicenceIntrospector::new(&lic_cfg(&url)).unwrap();
        let lic = Arc::new(lic);
        let app = echo_app(Auth {
            tokens: static_tokens(),
            licence: Some(lic.clone()),
        });
        for bad in ["lic-unknown", "lic-revoked-app"] {
            let (s, _) = call(&app, bad, None).await;
            assert_eq!(s, StatusCode::UNAUTHORIZED, "{bad}");
        }
        // Active licence of another app: authenticated, not entitled.
        let (s, body) = call(&app, "lic-other", None).await;
        assert_eq!(s, StatusCode::FORBIDDEN);
        assert!(body.contains("licence_app_not_allowed"), "{body}");
        assert_eq!(
            call(&app, "lic-unknown", None).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(lic.cache_len(), 0);
    }

    #[tokio::test]
    async fn introspect_failure_fails_closed_with_503() {
        let (url, _) = mock_licences().await;
        let lic = LicenceIntrospector::new(&lic_cfg(&url)).unwrap();
        let app = echo_app(Auth::new(static_tokens(), Some(lic)));
        for t in ["lic-500", "lic-garbage"] {
            let (s, body) = call(&app, t, None).await;
            assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "{t}");
            assert!(body.contains("licence_unavailable"), "{body}");
        }
        // Unreachable licences service.
        let dead =
            LicenceIntrospector::new(&lic_cfg("http://127.0.0.1:1/license/introspect")).unwrap();
        let app = echo_app(Auth::new(static_tokens(), Some(dead)));
        assert_eq!(
            call(&app, "lic-ok", None).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn without_introspect_url_licence_tokens_are_401() {
        let (_, calls) = mock_licences().await;
        let app = echo_app(Auth::from(static_tokens()));
        assert_eq!(call(&app, "lic-ok", None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(call(&app, "static-tok", None).await.0, StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn app_keys_are_configurable() {
        let (url, _) = mock_licences().await;
        let cfg = LicenceCfg {
            introspect_url: url,
            app_keys: vec!["kanban".to_string()],
        };
        let lic = LicenceIntrospector::new(&cfg).unwrap();
        assert_eq!(
            lic.resolve("lic-other").await,
            Ok(Uuid::parse_str(LIC_TENANT).unwrap())
        );
        assert_eq!(
            lic.resolve("lic-ok").await,
            Err(LicenceError::AppNotAllowed)
        );
    }

    #[tokio::test]
    async fn cache_expires_and_is_bounded() {
        let (url, calls) = mock_licences().await;
        let lic =
            LicenceIntrospector::with_cache(&lic_cfg(&url), Duration::from_millis(50), 1).unwrap();
        lic.resolve("lic-ok").await.unwrap();
        lic.resolve("lic-ok").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(80)).await;
        lic.resolve("lic-ok").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        // Bound: storing a second key into a full cache of 1 never grows past 1.
        lic.store(digest("another"), Uuid::now_v7());
        assert_eq!(lic.cache_len(), 1);
        // TTL is capped at 60 s whatever is asked for.
        let long =
            LicenceIntrospector::with_cache(&lic_cfg(&url), Duration::from_secs(3600), 8).unwrap();
        assert_eq!(long.ttl, LICENCE_CACHE_TTL);
    }
}
