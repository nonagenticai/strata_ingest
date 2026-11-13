//! `POST /api/ingest/jobs` with a structured source (`source.kind` = `api` | `datatable`):
//!
//! ```json
//! {"store_id": "<leaf>", "source_id": "<caller's source id>", "source_run": "<sync run>",
//!  "source": {"kind": "api" | "datatable",
//!             "config": {"url": "https://…", "auth": {"type": "none|bearer|header", "header_name": "X-Api-Key"},
//!                        "format": "csv|xlsx", "sheet": "Customers", "header_row": true},
//!             "credentials": {"secret": "…"} | null},
//!  "mapping": [{"source_field": "customer_id", "entity": "Customer", "attribute": "customer_id"}],
//!  "options": {…}}
//! ```
//!
//! The source is fetched while the request is open (`sources::fetch`, the `url`-job host rules
//! plus a resolved-address check, pinned connection, bounded redirects, size and time), split
//! into records (`sources::records`, `sources::xlsx`) and stored as ONE job with one Deliver step
//! per record. The credentials are used for that fetch only: they are taken out of the body
//! before anything else reads it, so no stored row, step, log line or error (serde's included)
//! can carry them.
//!
//! - Request-shape problems: 400 (`bad_request`); an unknown `source.kind`: 422
//!   `unsupported_source_kind`; unknown/router store: 404/422, as for every job.
//! - A fetch or parse failure (source down, 401, blocked address, bad CSV, no records, …): 202
//!   with the job `failed` and its `error` saying why (no steps; a retry is 409).

use std::time::Duration;

use axum::http::HeaderName;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::handlers::{
    bad, initial_meta, parse_options, parse_source_id, parse_source_run, validate_store,
};
use crate::config::Secret;
use crate::db::jobs::{self, NewJob, RecordDelivery};
use crate::doc_fields::{DocFields, ProvenanceIn};
use crate::error::{IngestError, Result};
use crate::limits::{
    CRAWL_DEFAULT_DEPTH, CRAWL_DEFAULT_PAGES, CRAWL_MAX_DEPTH, CRAWL_MAX_PAGES,
    SITEMAP_DEFAULT_URLS, SITEMAP_MAX_URLS,
};
use crate::netguard;
use crate::pipeline::Ctx;
use crate::pipeline::discover::{CrawlConfig, Discovery, SitemapConfig};
use crate::sources::fetch::{self, Auth};
use crate::sources::records::{csv_records_with, json_records_with};
use crate::sources::xlsx::xlsx_records_with;
use crate::sources::{
    MAX_MAPPINGS, MAX_NAME_CHARS, Mapping, PARSE_TIMEOUT, ParseLimits, Record, SourceError, render,
};

/// The `source.kind`s this service ingests: structured (`api`, `datatable`) and discovery
/// (`crawl`, `sitemap`, see [`discovery_job`]).
pub const SOURCE_KINDS: [&str; 4] = ["api", "datatable", "crawl", "sitemap"];

/// Source kinds whose job is a parent that discovers pages (`pipeline::discover`).
pub const DISCOVERY_KINDS: [&str; 2] = ["crawl", "sitemap"];

/// Header names a source credential may not be sent as.
const FORBIDDEN_HEADERS: [&str; 13] = [
    "host",
    "cookie",
    "connection",
    "keep-alive",
    "proxy-authorization",
    "proxy-authenticate",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-type",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceJobIn {
    store_id: Uuid,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    source_run: Option<String>,
    source: SourceIn,
    #[serde(default)]
    mapping: Vec<Mapping>,
    #[serde(default)]
    options: Option<Value>,
    #[serde(default)]
    topic_keys: Option<Vec<String>>,
    #[serde(default)]
    supersedes: Option<Vec<String>>,
    #[serde(default)]
    provenance: Option<ProvenanceIn>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceIn {
    kind: String,
    config: ConfigIn,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigIn {
    url: String,
    #[serde(default)]
    auth: Option<AuthIn>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    sheet: Option<String>,
    /// Datatable only: whether the first non-empty row is the header (default true).
    #[serde(default)]
    header_row: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthIn {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    header_name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Csv,
    Xlsx,
}

/// True when a JSON job body describes a structured source.
pub(super) fn is_source_body(v: &Value) -> bool {
    v.get("source").is_some()
}

/// Takes `source.credentials` out of the body. Every error message here is fixed text: nothing
/// of the value is echoed.
fn take_credentials(body: &mut Map<String, Value>) -> Result<Option<Secret>> {
    let source = body
        .get_mut("source")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| bad("`source` must be an object"))?;
    let shape = || bad("`source.credentials` must be null or {\"secret\": \"<non-empty string>\"}");
    match source.remove("credentials") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(mut o)) => {
            let secret = match o.remove("secret") {
                Some(Value::String(s)) if !s.is_empty() => s,
                _ => return Err(shape()),
            };
            if !o.is_empty() {
                return Err(shape());
            }
            Ok(Some(Secret::new(secret)))
        }
        Some(_) => Err(shape()),
    }
}

fn check_name(what: &str, s: &str) -> Result<()> {
    let t = s.trim();
    if t.is_empty() || t.chars().count() > MAX_NAME_CHARS || t.chars().any(char::is_control) {
        return Err(bad(format!(
            "every mapping `{what}` must be 1 to {MAX_NAME_CHARS} characters without control characters"
        )));
    }
    Ok(())
}

fn check_mapping(mapping: &[Mapping]) -> Result<Vec<Mapping>> {
    if mapping.len() > MAX_MAPPINGS {
        return Err(bad(format!(
            "`mapping` holds more than {MAX_MAPPINGS} entries"
        )));
    }
    mapping
        .iter()
        .map(|m| {
            check_name("source_field", &m.source_field)?;
            check_name("entity", &m.entity)?;
            check_name("attribute", &m.attribute)?;
            Ok(Mapping {
                source_field: m.source_field.trim().to_string(),
                entity: m.entity.trim().to_string(),
                attribute: m.attribute.trim().to_string(),
            })
        })
        .collect()
}

/// The credential as it will be presented. A secret given with `auth.type = none` is ignored.
fn auth_of(auth: Option<&AuthIn>, secret: Option<Secret>) -> Result<Auth> {
    let Some(a) = auth else {
        return Ok(Auth::None);
    };
    let needs = |secret: Option<Secret>| {
        secret.ok_or_else(|| {
            bad(format!(
                "`source.config.auth.type` {} needs `source.credentials.secret`",
                a.kind
            ))
        })
    };
    let valid_value = |s: &Secret, prefix: &str| {
        axum::http::HeaderValue::from_str(&format!("{prefix}{}", s.expose()))
            .map(|_| ())
            .map_err(|_| bad("`source.credentials.secret` is not a valid HTTP header value"))
    };
    match (a.kind.as_str(), a.header_name.as_deref()) {
        ("none", None) => Ok(Auth::None),
        ("bearer", None) => {
            let s = needs(secret)?;
            valid_value(&s, "Bearer ")?;
            Ok(Auth::Bearer(s))
        }
        ("header", Some(name)) => {
            let lower = name.trim().to_ascii_lowercase();
            let header = HeaderName::from_bytes(lower.as_bytes())
                .ok()
                .filter(|_| !FORBIDDEN_HEADERS.contains(&lower.as_str()))
                .ok_or_else(|| {
                    bad(
                        "`source.config.auth.header_name` must be an HTTP header name such as \
                         X-Api-Key (not Host, Cookie, Content-Type or a hop-by-hop header)",
                    )
                })?;
            let s = needs(secret)?;
            valid_value(&s, "")?;
            Ok(Auth::Header(header, s))
        }
        _ => Err(bad(
            "`source.config.auth.type` must be none, bearer or header; `header_name` goes only \
             with header (and is required there)",
        )),
    }
}

fn format_of(kind: &str, cfg: &ConfigIn, url: &url::Url) -> Result<Option<Format>> {
    if kind == "api" {
        if cfg.format.is_some() || cfg.sheet.is_some() || cfg.header_row.is_some() {
            return Err(bad(
                "`format`, `sheet` and `header_row` are for datatable sources only",
            ));
        }
        return Ok(None);
    }
    let format = match cfg.format.as_deref() {
        Some("csv") => Format::Csv,
        Some("xlsx") => Format::Xlsx,
        Some(other) => {
            return Err(bad(format!(
                "`source.config.format` must be csv or xlsx, not {other:?}"
            )));
        }
        None => {
            let path = url.path().to_ascii_lowercase();
            if path.ends_with(".csv") {
                Format::Csv
            } else if path.ends_with(".xlsx") {
                Format::Xlsx
            } else {
                return Err(bad(
                    "`source.config.format` (csv or xlsx) is required when the URL does not end in .csv or .xlsx",
                ));
            }
        }
    };
    if cfg.sheet.is_some() && format != Format::Xlsx {
        return Err(bad("`sheet` is for xlsx files only"));
    }
    if cfg
        .sheet
        .as_deref()
        .is_some_and(|s| s.trim().is_empty() || s.chars().count() > 31)
    {
        return Err(bad("`sheet` must be a sheet name of 1 to 31 characters"));
    }
    Ok(Some(format))
}

/// Fetches and splits the source.
async fn records_of(
    ctx: &Ctx,
    url: &url::Url,
    auth: &Auth,
    format: Option<Format>,
    sheet: Option<&str>,
    header_row: bool,
) -> std::result::Result<(Vec<Record>, Option<String>), SourceError> {
    let fetched = fetch::fetch(url, auth, &ctx.source_fetch).await?;
    // Parsing is CPU work: off the async runtime, bounded by a deadline the parsers check
    // themselves (and by the same deadline here, should a parser ever stall between checks).
    let limits = ParseLimits::standard();
    let body = fetched.body.clone();
    let sheet = sheet.map(str::to_string);
    let task = tokio::task::spawn_blocking(move || match format {
        None => json_records_with(&body, limits),
        Some(Format::Csv) => csv_records_with(&body, limits, header_row),
        Some(Format::Xlsx) => xlsx_records_with(&body, sheet.as_deref(), limits, header_row),
    });
    let timeout = || {
        SourceError::invalid(
            "parse_timeout",
            format!(
                "splitting the source into records took longer than {} s",
                PARSE_TIMEOUT.as_secs()
            ),
        )
    };
    let records = match tokio::time::timeout(PARSE_TIMEOUT + Duration::from_secs(5), task).await {
        Ok(Ok(r)) => r?,
        Ok(Err(_)) => {
            return Err(SourceError::retryable(
                "parse_failed",
                "the parser stopped unexpectedly",
            ));
        }
        Err(_) => return Err(timeout()),
    };
    if records.is_empty() {
        return Err(SourceError::invalid(
            "no_records",
            "the source holds no records; nothing was ingested (and nothing replaced)",
        ));
    }
    Ok((records, fetched.content_type))
}

/// The structured-source job of a JSON body (module docs) → `(job id, status)`.
pub(super) async fn source_job(
    ctx: &Ctx,
    tenant: Uuid,
    mut body: Value,
) -> Result<(Uuid, &'static str)> {
    let discovery = body
        .get("source")
        .and_then(|s| s.get("kind"))
        .and_then(Value::as_str)
        .is_some_and(|k| DISCOVERY_KINDS.contains(&k));
    if discovery {
        return discovery_job(ctx, tenant, body).await;
    }
    let obj = body
        .as_object_mut()
        .ok_or_else(|| bad("the body must be a JSON object"))?;
    // First: the credentials leave the body, so nothing below can echo them.
    let secret = take_credentials(obj)?;
    match obj
        .get("source")
        .and_then(|s| s.get("kind"))
        .and_then(Value::as_str)
    {
        Some(k) if SOURCE_KINDS.contains(&k) => {}
        Some(k) => {
            return Err(IngestError::UnsupportedSourceKind(format!(
                "source kind `{}` is not supported; supported: {}",
                k.chars().take(40).collect::<String>(),
                SOURCE_KINDS.join(", ")
            )));
        }
        None => {
            return Err(IngestError::UnsupportedSourceKind(format!(
                "`source.kind` is missing or not a string; supported: {}",
                SOURCE_KINDS.join(", ")
            )));
        }
    }
    let b: SourceJobIn =
        serde_json::from_value(body).map_err(|e| bad(format!("invalid body: {e}")))?;
    let kind = b.source.kind.as_str();
    let url = netguard::check_url(&b.source.config.url)?;
    let format = format_of(kind, &b.source.config, &url)?;
    let auth = auth_of(b.source.config.auth.as_ref(), secret)?;
    let mapping = check_mapping(&b.mapping)?;
    let source_id = parse_source_id(b.source_id)?;
    let source_run = parse_source_run(b.source_run, source_id.as_deref())?;
    let options = parse_options(b.options)?;
    let fields = DocFields::parse(b.topic_keys, b.supersedes, b.provenance)?;
    validate_store(ctx, tenant, b.store_id).await?;
    super::store_providers::record_lineage(ctx, tenant, b.store_id).await?;

    let sheet = b.source.config.sheet.as_deref().map(str::trim);
    let header_row = b.source.config.header_row.unwrap_or(true);
    // What is stored and shown: the URL without query and fragment (a `?api_key=` must not
    // land in the job row or its views); the fetch uses the full URL.
    let mut label = url.clone();
    label.set_query(None);
    label.set_fragment(None);
    let mut stored = json!({
        "kind": kind, "url": label.as_str(), "query_removed": url.query().is_some(),
        "auth": {
            "type": b.source.config.auth.as_ref().map_or("none", |a| a.kind.as_str()),
            "header_name": b.source.config.auth.as_ref().and_then(|a| a.header_name.as_deref()),
        },
        "mapping": mapping,
    });
    if let Some(f) = format {
        stored["format"] = json!(if f == Format::Csv { "csv" } else { "xlsx" });
        stored["sheet"] = json!(sheet);
        stored["header_row"] = json!(header_row);
    }
    let mut meta = Map::new();
    if let Some(sid) = &source_id {
        meta.insert("source_id".into(), json!(sid));
    }
    if let Some(run) = &source_run {
        meta.insert("source_run".into(), json!(run));
    }
    // Every record's document gets the source's topic_keys / supersedes / provenance.
    fields.insert_into(&mut meta);
    let id = Uuid::now_v7();
    let (records, content_type) =
        match records_of(ctx, &url, &auth, format, sheet, header_row).await {
            Ok(r) => r,
            Err(e) => {
                drop(auth);
                tracing::info!(job = %id, code = e.code, "structured source fetch failed");
                stored["records"] = json!(0);
                let new = NewJob {
                    tenant,
                    store_id: b.store_id,
                    source: stored,
                    options,
                };
                let meta = Value::Object(meta);
                jobs::create_ended(&ctx.pool, id, new, &meta, "failed", &e.step_error()).await?;
                return Ok((id, "failed"));
            }
        };
    drop(auth);
    stored["records"] = json!(records.len());
    meta.insert("records".into(), json!(records.len()));
    if let Some(ct) = content_type {
        meta.insert("mime".into(), json!(ct));
    }
    let noun = if kind == "api" { "Item" } else { "Row" };
    let deliveries: Vec<RecordDelivery> = records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let n = i + 1;
            let doc = render(r, &mapping, n, noun, label.as_str(), source_id.as_deref());
            let key = format!("{n:06}");
            let artifact = format!("record/{key}.md");
            RecordDelivery {
                input: json!({
                    "bundle_key": artifact, "title": doc.title, "source": doc.source,
                    "metadata": doc.metadata,
                }),
                key,
                artifact,
                text: doc.text,
            }
        })
        .collect();
    let new = NewJob {
        tenant,
        store_id: b.store_id,
        source: stored,
        options,
    };
    jobs::create_records(&ctx.pool, id, new, &Value::Object(meta), &deliveries).await?;
    Ok((id, "queued"))
}

// ---------------------------------------------------------------------------------------------
// Discovery sources: crawl, sitemap

/// Entries of `allowed_domains`, `include` and `exclude`, at most.
pub const MAX_DISCOVERY_LIST: usize = 50;
/// Longest `include` / `exclude` substring and `link_query`.
pub const MAX_FILTER_CHARS: usize = 200;
pub const MAX_LINK_QUERY_CHARS: usize = 500;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryJobIn {
    store_id: Uuid,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    source_run: Option<String>,
    source: DiscoverySourceIn,
    #[serde(default)]
    options: Option<Value>,
    #[serde(default)]
    topic_keys: Option<Vec<String>>,
    #[serde(default)]
    supersedes: Option<Vec<String>>,
    #[serde(default)]
    provenance: Option<ProvenanceIn>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoverySourceIn {
    kind: String,
    config: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CrawlIn {
    url: String,
    #[serde(default)]
    max_depth: Option<u32>,
    #[serde(default)]
    max_pages: Option<usize>,
    #[serde(default)]
    allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    link_query: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SitemapIn {
    url: String,
    #[serde(default)]
    max_urls: Option<usize>,
    #[serde(default)]
    include: Option<Vec<String>>,
    #[serde(default)]
    exclude: Option<Vec<String>>,
}

/// A 422 `invalid_input` (the standard error envelope).
fn invalid(msg: impl Into<String>) -> IngestError {
    IngestError::InvalidInput(msg.into())
}

/// The start / sitemap URL under the url-job guard (`netguard::check_url`); refused → 422.
fn discovery_url(raw: &str) -> Result<url::Url> {
    netguard::check_url(raw).map_err(|e| match e {
        IngestError::BadRequest(m) => invalid(format!("`source.config.url`: {m}")),
        e => e,
    })
}

/// A bare hostname: lowercase ASCII letters, digits, `-` and `.`, 1..=253 bytes, no empty label.
fn hostname(raw: &str) -> Option<String> {
    let h = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    let ok = !h.is_empty()
        && h.len() <= 253
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        && h.split('.').all(|l| !l.is_empty());
    ok.then_some(h)
}

/// A list of 1..=[`MAX_DISCOVERY_LIST`] strings of 1..=`max` characters, no control characters.
fn string_list(what: &str, v: Vec<String>, max: usize) -> Result<Vec<String>> {
    if v.is_empty() || v.len() > MAX_DISCOVERY_LIST {
        return Err(invalid(format!(
            "`{what}` must hold 1 to {MAX_DISCOVERY_LIST} entries"
        )));
    }
    v.into_iter()
        .map(|s| {
            if s.is_empty() || s.chars().count() > max || s.chars().any(char::is_control) {
                Err(invalid(format!(
                    "every `{what}` entry must be 1 to {max} characters without control characters"
                )))
            } else {
                Ok(s)
            }
        })
        .collect()
}

fn crawl_config(config: Value) -> Result<CrawlConfig> {
    let c: CrawlIn = serde_json::from_value(config)
        .map_err(|e| invalid(format!("invalid `source.config` for crawl: {e}")))?;
    let url = discovery_url(&c.url)?;
    let max_depth = c.max_depth.unwrap_or(CRAWL_DEFAULT_DEPTH);
    if !(1..=CRAWL_MAX_DEPTH).contains(&max_depth) {
        return Err(invalid(format!(
            "`max_depth` must be 1 to {CRAWL_MAX_DEPTH}"
        )));
    }
    let max_pages = c.max_pages.unwrap_or(CRAWL_DEFAULT_PAGES);
    if !(1..=CRAWL_MAX_PAGES).contains(&max_pages) {
        return Err(invalid(format!(
            "`max_pages` must be 1 to {CRAWL_MAX_PAGES}"
        )));
    }
    let allowed_domains = match c.allowed_domains {
        None => vec![
            url.host_str()
                .and_then(hostname)
                .ok_or_else(|| invalid("`source.config.url` has no host"))?,
        ],
        Some(list) => string_list("allowed_domains", list, 253)?
            .iter()
            .map(|d| {
                hostname(d).ok_or_else(|| {
                    invalid(
                        "every `allowed_domains` entry must be a bare hostname such as \
                         example.com (no scheme, port or path)",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let link_query = match c.link_query {
        None => None,
        Some(q) => {
            let t = q.trim();
            if t.is_empty()
                || t.chars().count() > MAX_LINK_QUERY_CHARS
                || t.chars().any(char::is_control)
            {
                return Err(invalid(format!(
                    "`link_query` must be 1 to {MAX_LINK_QUERY_CHARS} characters without control characters"
                )));
            }
            Some(t.to_string())
        }
    };
    Ok(CrawlConfig {
        url: url.to_string(),
        max_depth,
        max_pages,
        allowed_domains,
        link_query,
    })
}

fn sitemap_config(config: Value) -> Result<SitemapConfig> {
    let c: SitemapIn = serde_json::from_value(config)
        .map_err(|e| invalid(format!("invalid `source.config` for sitemap: {e}")))?;
    let url = discovery_url(&c.url)?;
    let max_urls = c.max_urls.unwrap_or(SITEMAP_DEFAULT_URLS);
    if !(1..=SITEMAP_MAX_URLS).contains(&max_urls) {
        return Err(invalid(format!(
            "`max_urls` must be 1 to {SITEMAP_MAX_URLS}"
        )));
    }
    let include = match c.include {
        None => vec![],
        Some(v) => string_list("include", v, MAX_FILTER_CHARS)?,
    };
    let exclude = match c.exclude {
        None => vec![],
        Some(v) => string_list("exclude", v, MAX_FILTER_CHARS)?,
    };
    Ok(SitemapConfig {
        url: url.to_string(),
        max_urls,
        include,
        exclude,
    })
}

/// A crawl / sitemap job (`source.kind` = `crawl` | `sitemap`) → `(parent job id, "queued")`:
///
/// ```json
/// {"store_id": "<leaf>", "source_id": "…", "source_run": "…", "options": {…},
///  "source": {"kind": "crawl", "config": {"url": "https://…", "max_depth": 2, "max_pages": 50,
///                                         "allowed_domains": ["example.com"], "link_query": "…"}}}
/// {"store_id": "<leaf>", "source": {"kind": "sitemap",
///  "config": {"url": "https://…/sitemap.xml", "max_urls": 200, "include": ["/docs/"], "exclude": ["/old/"]}}}
/// ```
///
/// Nothing is fetched here: the job's Discover step finds the pages and creates one url child job
/// each (`pipeline::discover`). `credentials` and `mapping` belong to api / datatable sources (a
/// non-null one is 422, its value never echoed); every config problem (unknown field, range,
/// a URL the url-job guard refuses) is 422 `invalid_input`; options / source_id / source_run /
/// store as for every job.
async fn discovery_job(ctx: &Ctx, tenant: Uuid, mut body: Value) -> Result<(Uuid, &'static str)> {
    let obj = body
        .as_object_mut()
        .ok_or_else(|| bad("the body must be a JSON object"))?;
    // A credential has no use here: refused before anything parses (and could echo) it.
    let source = obj
        .get_mut("source")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| bad("`source` must be an object"))?;
    match source.remove("credentials") {
        None | Some(Value::Null) => {}
        Some(_) => {
            return Err(invalid(
                "`source.credentials` is for api and datatable sources only",
            ));
        }
    }
    match obj.remove("mapping") {
        None | Some(Value::Null) => {}
        Some(_) => return Err(invalid("`mapping` is for api and datatable sources only")),
    }
    let b: DiscoveryJobIn =
        serde_json::from_value(body).map_err(|e| invalid(format!("invalid body: {e}")))?;
    let discovery = match b.source.kind.as_str() {
        "crawl" => Discovery::Crawl(crawl_config(b.source.config)?),
        "sitemap" => Discovery::Sitemap(sitemap_config(b.source.config)?),
        other => {
            return Err(IngestError::UnsupportedSourceKind(format!(
                "source kind `{}` is not a discovery kind",
                other.chars().take(40).collect::<String>()
            )));
        }
    };
    let source_id = parse_source_id(b.source_id)?;
    let source_run = parse_source_run(b.source_run, source_id.as_deref())?;
    let options = parse_options(b.options)?;
    let fields = DocFields::parse(b.topic_keys, b.supersedes, b.provenance)?;
    validate_store(ctx, tenant, b.store_id).await?;
    super::store_providers::record_lineage(ctx, tenant, b.store_id).await?;
    let stored = serde_json::to_value(&discovery).map_err(|e| invalid(e.to_string()))?;
    let id = Uuid::now_v7();
    let new = NewJob {
        tenant,
        store_id: b.store_id,
        source: stored,
        options,
    };
    // Every page's child job inherits these (`jobs::add_children`).
    let mut meta = initial_meta(source_id.as_deref(), source_run.as_deref());
    fields.add_to(&mut meta);
    jobs::create_discovery(&ctx.pool, id, new, &meta).await?;
    Ok((id, "queued"))
}
