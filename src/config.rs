//! Runtime configuration: `STRATA_INGEST_*` env vars plus the providers file
//! (`STRATA_INGEST_PROVIDERS`, default `/etc/strata-ingest/providers.toml`).
//!
//! Validation failures are `IngestError::Config`; `strata-ingest check-config` turns them into a
//! non-zero exit. Secrets never appear in `Debug` output or in error messages.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;
use uuid::Uuid;

use crate::error::{IngestError, Result};
use crate::limits::Limits;

pub const DEFAULT_DATABASE_URL: &str =
    "postgres://strata_ingest:strata_ingest@127.0.0.1:5441/strata_ingest";
pub const DEFAULT_BIND: &str = "127.0.0.1:9135";
/// storage_service in-cluster (Service `storage-api`, namespace `storage`). It also scans every
/// upload with ClamAV, so there is no MinIO or clamd of our own.
pub const DEFAULT_STORAGE_URL: &str = "http://127.0.0.1:8391";
pub const DEFAULT_STORAGE_BUCKET: &str = "strata-ingest";
pub const DEFAULT_DOCLING_URL: &str = "http://127.0.0.1:5002";
pub const DEFAULT_WEBSCRAPER_URL: &str = "http://127.0.0.1:9119";
pub const DEFAULT_STRATA_URL: &str = "http://127.0.0.1:8090";
pub const DEFAULT_WORKERS: usize = 4;
pub const DEFAULT_PROVIDERS: &str = "/etc/strata-ingest/providers.toml";

pub const DEFAULT_PROVIDER_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_PROVIDER_CONCURRENCY: usize = 4;

/// A secret string (key, token, password). `Debug` never prints the value; there is no `Display`.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

/// Where a deployment (and its tenants' data) lives, and where a provider processes data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    Eu,
    Us,
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Region::Eu => "eu",
            Region::Us => "us",
        })
    }
}

/// `dev` may carry default providers (our own mlx_api / `pcontract fake`) used by every tenant
/// without a registration; `production` may not: each tenant registers its own
/// (`PUT /api/tenant/providers`). A file without `mode` is `dev` (the cluster file predates it).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Dev,
    Production,
}

impl Region {
    pub fn as_str(self) -> &'static str {
        match self {
            Region::Eu => "eu",
            Region::Us => "us",
        }
    }

    pub fn parse(s: &str) -> Option<Region> {
        match s {
            "eu" => Some(Region::Eu),
            "us" => Some(Region::Us),
            _ => None,
        }
    }
}

/// One capability's HTTP provider. In the file, `api_key` is a reference (`env:NAME` or
/// `file:/path`); after [`ProvidersFile::load`] it holds the resolved key.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCfg {
    pub base_url: String,
    pub dialect: String,
    pub model: String,
    pub api_key: Secret,
    pub regions: Vec<Region>,
    pub zdr: bool,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub rpm: Option<u32>,
    #[serde(default)]
    pub price_micros_per_unit: Option<i64>,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_PROVIDER_TIMEOUT_MS
}

fn default_concurrency() -> usize {
    DEFAULT_PROVIDER_CONCURRENCY
}

/// `providers.toml`: the deployment region plus the optional capabilities. Any other section or
/// field is an error. Build it with [`ProvidersFile::load`] — a bare `toml::from_str` leaves the
/// `api_key` references unresolved and skips validation.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvidersFile {
    pub region: Region,
    /// `dev` (default) or `production` (no capability sections allowed).
    #[serde(default)]
    pub mode: Mode,
    /// Dev only (`true` with `mode = "production"` is a startup error): tenant registrations may
    /// use `http` and internal hosts. Default `false`: the SSRF guard (`netguard`) applies to every
    /// tenant registration. The sections below are operator-set and never guarded.
    #[serde(default)]
    pub allow_internal_provider_hosts: bool,
    pub ocr: Option<ProviderCfg>,
    pub vision: Option<ProviderCfg>,
    pub transcribe: Option<ProviderCfg>,
}

/// v1 dialects per capability (what `providers::{ocr, vision, transcribe}` speak); anything else
/// is a startup error, or a 422 `dialect_not_allowed` on a tenant registration.
pub const DIALECTS: [(&str, &[&str]); 3] = [
    ("ocr", &["mistral"]),
    ("vision", &["openai"]),
    ("transcribe", &["openai"]),
];

/// The dialects implemented for capability `name`.
pub fn dialects_for(name: &str) -> &'static [&'static str] {
    DIALECTS
        .iter()
        .find(|(cap, _)| *cap == name)
        .map(|(_, d)| *d)
        .unwrap_or(&[])
}

impl ProvidersFile {
    /// Reads `path`, resolves every `api_key` reference against `env` (or the referenced file),
    /// then [`validate`](Self::validate)s.
    pub fn load(path: &Path, env: &HashMap<String, String>) -> Result<ProvidersFile> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            config_err(format!(
                "cannot read providers file {}: {e}",
                path.display()
            ))
        })?;
        let mut file: ProvidersFile = toml::from_str(&text).map_err(|e| {
            // `message()` only: the Display form quotes the offending source line, which could be
            // an inline key.
            let line = e
                .span()
                .map(|s| format!(" (line {})", text[..s.start].lines().count().max(1)))
                .unwrap_or_default();
            config_err(format!(
                "providers file {}: {}{line}",
                path.display(),
                e.message()
            ))
        })?;
        for (name, cfg) in file.capabilities_mut() {
            if let Some(cfg) = cfg {
                cfg.api_key = resolve_key(name, cfg.api_key.expose(), env)?;
                cfg.base_url = cfg.base_url.trim().trim_end_matches('/').to_string();
            }
        }
        file.validate()?;
        Ok(file)
    }

    /// Every configured capability with its name, in a fixed order.
    pub fn capabilities(&self) -> [(&'static str, Option<&ProviderCfg>); 3] {
        [
            ("ocr", self.ocr.as_ref()),
            ("vision", self.vision.as_ref()),
            ("transcribe", self.transcribe.as_ref()),
        ]
    }

    fn capabilities_mut(&mut self) -> [(&'static str, Option<&mut ProviderCfg>); 3] {
        [
            ("ocr", self.ocr.as_mut()),
            ("vision", self.vision.as_mut()),
            ("transcribe", self.transcribe.as_mut()),
        ]
    }

    /// Startup rules (spec §4, §7): known dialect for the capability; `regions` contains the
    /// deployment region; `zdr = true` in an `eu` deployment; sane numbers and URL.
    pub fn validate(&self) -> Result<()> {
        if self.mode == Mode::Production {
            if self.allow_internal_provider_hosts {
                return Err(config_err(
                    "allow_internal_provider_hosts = true is only allowed with mode = \"dev\" \
                     (it lets tenants point strata-ingest at internal hosts)"
                        .to_string(),
                ));
            }
            if let Some((name, _)) = self.capabilities().iter().find(|(_, c)| c.is_some()) {
                return Err(config_err(format!(
                    "a production deployment has no default providers: remove [{name}] (tenants \
                     register their own with PUT /api/tenant/providers)"
                )));
            }
        }
        for (name, cfg) in self.capabilities() {
            let Some(cfg) = cfg else { continue };
            let allowed = dialects_for(name);
            if !allowed.contains(&cfg.dialect.as_str()) {
                return Err(config_err(format!(
                    "providers.{name}: unknown dialect {:?} (v1 supports: {})",
                    cfg.dialect,
                    allowed.join(", ")
                )));
            }
            if !cfg.regions.contains(&self.region) {
                let regions: Vec<String> = cfg.regions.iter().map(Region::to_string).collect();
                return Err(config_err(format!(
                    "providers.{name}: regions [{}] do not include the deployment region \"{}\"",
                    regions.join(", "),
                    self.region
                )));
            }
            if self.region == Region::Eu && !cfg.zdr {
                return Err(config_err(format!(
                    "providers.{name}: zdr = false is not allowed in an eu deployment \
                     (zero data retention is required)"
                )));
            }
            if !(cfg.base_url.starts_with("http://") || cfg.base_url.starts_with("https://")) {
                return Err(config_err(format!(
                    "providers.{name}.base_url must start with http:// or https://"
                )));
            }
            if cfg.model.trim().is_empty() {
                return Err(config_err(format!("providers.{name}.model is empty")));
            }
            if cfg.api_key.expose().is_empty() {
                return Err(config_err(format!("providers.{name}.api_key is empty")));
            }
            if cfg.timeout_ms == 0 {
                return Err(config_err(format!(
                    "providers.{name}.timeout_ms must be >= 1"
                )));
            }
            if cfg.concurrency == 0 {
                return Err(config_err(format!(
                    "providers.{name}.concurrency must be >= 1"
                )));
            }
            if cfg.rpm == Some(0) {
                return Err(config_err(format!("providers.{name}.rpm must be >= 1")));
            }
            if cfg.price_micros_per_unit.is_some_and(|p| p < 0) {
                return Err(config_err(format!(
                    "providers.{name}.price_micros_per_unit must be >= 0"
                )));
            }
        }
        Ok(())
    }
}

/// `env:NAME` → the (trimmed) value of `NAME` in `env`; `file:/path` → the trimmed file contents.
/// Anything else is an inline key and is refused without echoing it.
fn resolve_key(cap: &str, raw: &str, env: &HashMap<String, String>) -> Result<Secret> {
    let raw = raw.trim();
    let value = if let Some(name) = raw.strip_prefix("env:") {
        let name = name.trim();
        get(env, name).map(str::to_string).ok_or_else(|| {
            config_err(format!(
                "providers.{cap}.api_key: env var {name} is unset or empty"
            ))
        })?
    } else if let Some(path) = raw.strip_prefix("file:") {
        let path = path.trim();
        std::fs::read_to_string(path)
            .map_err(|e| {
                config_err(format!(
                    "providers.{cap}.api_key: cannot read key file {path}: {e}"
                ))
            })?
            .trim()
            .to_string()
    } else {
        return Err(config_err(format!(
            "providers.{cap}.api_key must reference the key as \"env:NAME\" or \"file:/path\"; \
             inline keys are not allowed"
        )));
    };
    if value.is_empty() {
        return Err(config_err(format!("providers.{cap}.api_key is empty")));
    }
    Ok(Secret::new(value))
}

/// `Debug` is hand-written so URL passwords never reach a log line; `Secret`s redact themselves.
#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
    /// `STRATA_INGEST_API_TOKENS="<tenant-uuid>:<token>,…"`: the matched token decides the tenant.
    pub tokens: Vec<(Uuid, Secret)>,
    /// storage_service base URL (`STRATA_INGEST_STORAGE_URL`).
    pub storage_url: String,
    /// Bucket for originals and binary artefacts (`STRATA_INGEST_STORAGE_BUCKET`).
    pub storage_bucket: String,
    /// Optional licence token, sent as `X-Tenant-Id: Bearer <token>`
    /// (`STRATA_INGEST_STORAGE_TOKEN`); without it the in-cluster caller is the platform tenant.
    pub storage_token: Option<Secret>,
    pub docling_url: String,
    pub webscraper_url: String,
    pub webscraper_token: Option<Secret>,
    pub strata_url: String,
    pub strata_token: Secret,
    pub workers: usize,
    pub limits: Limits,
    pub providers: ProvidersFile,
    /// `STRATA_INGEST_PROVIDER_KEY`: base64 of the 32-byte key sealing tenant provider keys
    /// (validated at load). Unset: `PUT /api/tenant/providers` answers 503
    /// `provider_key_unconfigured`, and `serve` refuses to start when stored keys exist.
    pub provider_key: Option<Secret>,
    /// `METERING_REDIS_URL` / `METERING_STREAM`: the outbox drainer's transport to billing
    /// (`metering::run_drainer`). `None`: no drainer (events still accumulate in `metering_outbox`).
    pub metering: Option<crate::metering::MeteringCfg>,
    /// `STRATA_INGEST_LICENSE_INTROSPECT_URL` (+ `STRATA_INGEST_LICENSE_APP_KEYS`): a bearer that
    /// matches no static token is introspected as a licence token. `None`: static tokens only.
    pub licence: Option<LicenceCfg>,
}

/// Default `STRATA_INGEST_LICENSE_APP_KEYS`: the licences service's app key for this service.
pub const DEFAULT_LICENSE_APP_KEYS: &str = "strata-ingest";

/// Licence-token auth (`src/api/auth.rs`): the licences service's `POST /license/introspect`
/// URL and the app keys whose licences grant access.
#[derive(Clone, PartialEq, Eq)]
pub struct LicenceCfg {
    pub introspect_url: String,
    pub app_keys: Vec<String>,
}

impl fmt::Debug for LicenceCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LicenceCfg")
            .field("introspect_url", &redact_url(&self.introspect_url))
            .field("app_keys", &self.app_keys)
            .finish()
    }
}

impl LicenceCfg {
    /// `None` when `STRATA_INGEST_LICENSE_INTROSPECT_URL` is unset or empty.
    pub fn from_map(env: &HashMap<String, String>) -> Result<Option<Self>> {
        const URL_KEY: &str = "STRATA_INGEST_LICENSE_INTROSPECT_URL";
        const KEYS_KEY: &str = "STRATA_INGEST_LICENSE_APP_KEYS";
        if get(env, URL_KEY).is_none() {
            return Ok(None);
        }
        let introspect_url = url(env, URL_KEY, "")?;
        let app_keys: Vec<String> = get(env, KEYS_KEY)
            .unwrap_or(DEFAULT_LICENSE_APP_KEYS)
            .split(',')
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .collect();
        if app_keys.is_empty() {
            return Err(config_err(format!(
                "{KEYS_KEY} must name at least one app key"
            )));
        }
        Ok(Some(Self {
            introspect_url,
            app_keys,
        }))
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("database_url", &redact_url(&self.database_url))
            .field("bind", &self.bind)
            .field("tokens", &self.tokens)
            .field("storage_url", &redact_url(&self.storage_url))
            .field("storage_bucket", &self.storage_bucket)
            .field("storage_token", &self.storage_token)
            .field("docling_url", &redact_url(&self.docling_url))
            .field("webscraper_url", &redact_url(&self.webscraper_url))
            .field("webscraper_token", &self.webscraper_token)
            .field("strata_url", &redact_url(&self.strata_url))
            .field("strata_token", &self.strata_token)
            .field("workers", &self.workers)
            .field("limits", &self.limits)
            .field("providers", &self.providers)
            .field("provider_key", &self.provider_key)
            .field("metering", &self.metering)
            .field("licence", &self.licence)
            .finish()
    }
}

impl Config {
    /// Reads the process environment.
    pub fn from_env() -> Result<Self> {
        let process: HashMap<String, String> = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect();
        Self::from_map(&process)
    }

    /// Builds and validates a config from a key/value map. Loads the providers file named by
    /// `STRATA_INGEST_PROVIDERS`, resolving `env:` keys against the same map.
    pub fn from_map(env: &HashMap<String, String>) -> Result<Self> {
        let providers_path =
            PathBuf::from(get(env, "STRATA_INGEST_PROVIDERS").unwrap_or(DEFAULT_PROVIDERS));
        let cfg = Config {
            database_url: get(env, "STRATA_INGEST_DATABASE_URL")
                .unwrap_or(DEFAULT_DATABASE_URL)
                .to_string(),
            bind: parse(
                env,
                "STRATA_INGEST_BIND",
                DEFAULT_BIND.parse::<SocketAddr>().expect("valid default"),
            )?,
            tokens: parse_tokens(get(env, "STRATA_INGEST_API_TOKENS"))?,
            storage_url: url(env, "STRATA_INGEST_STORAGE_URL", DEFAULT_STORAGE_URL)?,
            storage_bucket: get(env, "STRATA_INGEST_STORAGE_BUCKET")
                .unwrap_or(DEFAULT_STORAGE_BUCKET)
                .to_string(),
            storage_token: get(env, "STRATA_INGEST_STORAGE_TOKEN").map(Secret::new),
            docling_url: url(env, "STRATA_INGEST_DOCLING_URL", DEFAULT_DOCLING_URL)?,
            webscraper_url: url(env, "STRATA_INGEST_WEBSCRAPER_URL", DEFAULT_WEBSCRAPER_URL)?,
            webscraper_token: get(env, "STRATA_INGEST_WEBSCRAPER_TOKEN").map(Secret::new),
            strata_url: url(env, "STRATA_INGEST_STRATA_URL", DEFAULT_STRATA_URL)?,
            strata_token: required_secret(env, "STRATA_INGEST_STRATA_TOKEN")?,
            workers: parse(env, "STRATA_INGEST_WORKERS", DEFAULT_WORKERS)?,
            limits: Limits::from_map(env)?,
            providers: ProvidersFile::load(&providers_path, env)?,
            provider_key: match get(env, crate::providers::crypto::KEY_ENV) {
                Some(raw) => {
                    crate::providers::crypto::decode_key(raw)?;
                    Some(Secret::new(raw))
                }
                None => None,
            },
            metering: crate::metering::MeteringCfg::from_map(env)?,
            licence: LicenceCfg::from_map(env)?,
        };
        if cfg.workers == 0 {
            return Err(config_err("STRATA_INGEST_WORKERS must be >= 1".to_string()));
        }
        Ok(cfg)
    }
}

/// `"<tenant-uuid>:<token>,…"`. Required, non-empty; no nil tenant, no empty or duplicate token.
/// Errors name the entry's position, never its text (it may be a bare token).
fn parse_tokens(raw: Option<&str>) -> Result<Vec<(Uuid, Secret)>> {
    const KEY: &str = "STRATA_INGEST_API_TOKENS";
    let raw = raw.ok_or_else(|| {
        config_err(format!(
            "{KEY} is required: comma-separated <tenant-uuid>:<token> pairs"
        ))
    })?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for (i, entry) in raw
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .enumerate()
    {
        let n = i + 1;
        let (tenant, token) = entry
            .split_once(':')
            .ok_or_else(|| config_err(format!("{KEY} entry {n} is not <tenant-uuid>:<token>")))?;
        let tenant = Uuid::parse_str(tenant.trim())
            .map_err(|_| config_err(format!("{KEY} entry {n}: tenant is not a uuid")))?;
        if tenant.is_nil() {
            return Err(config_err(format!(
                "{KEY} entry {n}: the nil tenant uuid is not allowed"
            )));
        }
        let token = token.trim();
        if token.is_empty() {
            return Err(config_err(format!("{KEY} entry {n}: empty token")));
        }
        if !seen.insert(token.to_string()) {
            return Err(config_err(format!(
                "{KEY} entry {n}: duplicate token (each token must map to one tenant)"
            )));
        }
        out.push((tenant, Secret::new(token)));
    }
    if out.is_empty() {
        return Err(config_err(format!("{KEY} holds no tokens")));
    }
    Ok(out)
}

fn required_secret(env: &HashMap<String, String>, key: &str) -> Result<Secret> {
    get(env, key)
        .map(Secret::new)
        .ok_or_else(|| config_err(format!("{key} is required")))
}

fn config_err(msg: String) -> IngestError {
    IngestError::Config(msg)
}

/// Trimmed value, or `None` when absent / empty / whitespace-only.
pub(crate) fn get<'a>(env: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    env.get(key).map(|s| s.trim()).filter(|s| !s.is_empty())
}

pub(crate) fn parse<T>(env: &HashMap<String, String>, key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    match get(env, key) {
        None => Ok(default),
        Some(raw) => raw
            .parse::<T>()
            .map_err(|e| config_err(format!("{key}={raw:?} is invalid: {e}"))),
    }
}

fn url(env: &HashMap<String, String>, key: &str, default: &str) -> Result<String> {
    let raw = get(env, key).unwrap_or(default).trim_end_matches('/');
    if raw.starts_with("http://") || raw.starts_with("https://") {
        Ok(raw.to_string())
    } else {
        Err(config_err(format!(
            "{key}={:?} must start with http:// or https://",
            redact_url(raw)
        )))
    }
}

/// Replaces a URL's password (userinfo and `password` / `sslpassword` query params) with `***`.
/// Copied from strata_rag.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let (prefix, rest) = url.split_at(scheme_end + 3);
    match userinfo_bounds(rest) {
        Some((colon, at)) => format!(
            "{prefix}{}:***{}",
            &rest[..colon],
            mask_query_secrets(&rest[at..])
        ),
        None => format!("{prefix}{}", mask_query_secrets(rest)),
    }
}

/// Byte offsets in `rest` (the URL after `://`) of the `:` that ends the user name and the `@` that
/// ends the userinfo, when the URL carries a password.
fn userinfo_bounds(rest: &str) -> Option<(usize, usize)> {
    let delim = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..delim];
    if let Some(at) = authority.rfind('@') {
        return Some((authority[..at].find(':')?, at));
    }
    // No `@` before the first delimiter: either no userinfo at all, or a password that holds an
    // unencoded `/`, `?` or `#`. With a `:` before that delimiter and an `@` after it, the latter is
    // assumed, except for a plain `host:port` whose path/query merely contains an `@`.
    let colon = authority.find(':')?;
    let at = rest.rfind('@')?;
    let after_colon = &authority[colon + 1..];
    let numeric_port = !after_colon.is_empty() && after_colon.bytes().all(|b| b.is_ascii_digit());
    if numeric_port && !rest[at + 1..].contains('/') {
        return None;
    }
    Some((colon, at))
}

/// Mask the values of `password` / `sslpassword` query parameters (key match is case-insensitive).
fn mask_query_secrets(s: &str) -> String {
    let Some(q) = s.find('?') else {
        return s.to_string();
    };
    let (head, query) = s.split_at(q + 1);
    let (query, fragment) = match query.find('#') {
        Some(h) => query.split_at(h),
        None => (query, ""),
    };
    let masked: Vec<String> = query
        .split('&')
        .map(|param| match param.split_once('=') {
            Some((key, _))
                if key.eq_ignore_ascii_case("password")
                    || key.eq_ignore_ascii_case("sslpassword") =>
            {
                format!("{key}=***")
            }
            _ => param.to_string(),
        })
        .collect();
    format!("{head}{}{fragment}", masked.join("&"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_masks_passwords_only() {
        assert_eq!(
            redact_url("postgres://u:pw@h:5441/db"),
            "postgres://u:***@h:5441/db"
        );
        assert_eq!(redact_url("postgres://h:5441/db"), "postgres://h:5441/db");
        assert_eq!(
            redact_url("postgres://h/db?user=u&password=pw"),
            "postgres://h/db?user=u&password=***"
        );
        assert_eq!(
            redact_url("postgres://u:p/w@h:5441/db"),
            "postgres://u:***@h:5441/db"
        );
    }

    #[test]
    fn inline_key_error_never_echoes_the_key() {
        let e = resolve_key("ocr", "sk-live-abc", &HashMap::new()).unwrap_err();
        assert!(!e.to_string().contains("sk-live-abc"));
    }

    #[test]
    fn parse_error_message_omits_the_source_line() {
        let path = std::env::temp_dir().join(format!("strata-ingest-unit-{}.toml", Uuid::now_v7()));
        // Unquoted inline key: a TOML syntax error on the very line holding the key.
        std::fs::write(&path, "region = \"eu\"\n[ocr]\napi_key = sk-live-xyz\n").unwrap();
        let e = ProvidersFile::load(&path, &HashMap::new()).unwrap_err();
        let _ = std::fs::remove_file(&path);
        let m = e.to_string();
        assert!(!m.contains("sk-live-xyz"), "{m}");
        assert!(m.contains("line 3"), "{m}");
    }
}
