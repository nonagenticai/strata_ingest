//! The Discover step of a crawl / sitemap parent job (`source.kind` = `crawl` | `sitemap`): finds
//! the pages and creates one `url` child job per page (`jobs::add_children`: same tenant, store,
//! options and `meta.source_id`, `parent_job_id` set, the url pipeline from Acquire on). The
//! parent completes when Discover does; its job view counts the children.
//!
//! - **crawl**: webscraper's `POST /api/scrape/url` returns ONE page (its `follow_links` exists
//!   only on `/api/scrape/bulk`, whose Scrapy crawl still answers with the start page alone and
//!   no markdown), so the crawl is a bounded breadth-first walk here: the start page is depth 0;
//!   the links of a page at depth < `max_depth` are followed when http(s), on an
//!   `allowed_domains` host (or a subdomain), not a binary resource, accepted by
//!   `netguard::check_url`, fragment stripped and not seen before; at most `max_pages` pages.
//!   Before a page is scraped its host is resolved with `netguard::resolve_public` (the
//!   structured-source guard: every address public, no loopback / private / link-local / CGNAT /
//!   metadata): a refused page is skipped (`skipped_unsafe`), a page whose host does not resolve
//!   too (`skipped_unresolvable`; the start page's is retried). Neither counts against
//!   `max_pages`. Each page is scraped ONCE, with its images: the page original (markdown + inlined images,
//!   `page_images`) is stored at the child's original key (storage_service scans it) and the
//!   child's source says so (`prefetched`), so the child's Acquire records it instead of
//!   scraping again. A page webscraper refuses (4xx, failed, empty) becomes a child without
//!   `prefetched`: it scrapes itself and fails visibly. Each page's child and its progress record
//!   (`discover/{child id}.json`: url, depth, links) are written in one transaction, so a retried
//!   Discover replays the recorded pages without scraping them again.
//! - **sitemap**: the sitemap is fetched with the guarded fetcher (`sources::fetch`: netguard,
//!   pinned addresses, redirects, size, time), gunzipped when it is gzip, and parsed (`<urlset>` /
//!   `<sitemapindex>`, index nesting followed `SITEMAP_MAX_LEVELS` deep, at most
//!   `SITEMAP_MAX_FETCHES` documents and `SITEMAP_MAX_CHILD_SITEMAPS` nested sitemaps queued).
//!   Parsing streams: each `<loc>` is decided as it is read and only accepted ones are kept; a
//!   document stops being read at `SITEMAP_MAX_ENTRIES` `<loc>`s or once `max_urls` URLs are
//!   accepted, so the step holds at most `max_urls` + `SITEMAP_MAX_CHILD_SITEMAPS` URLs. Page
//!   `<loc>`s are kept when they contain an `include` substring (when given) and no `exclude`
//!   one, pass `netguard::check_url`, deduplicated. Children scrape their page themselves.
//!
//! Discovery that finds no page (an empty sitemap, filters that keep nothing, a refused start
//! page) FAILS the parent with `no_pages_discovered` (not retried) instead of completing empty:
//! a completed run tells care-bus to replace the source's earlier documents, and an empty one
//! would wipe them.
//!
//! A Retryable failure (webscraper or the sitemap host unavailable) retries the step like any
//! other; once its attempts are used up the parent fails.

use std::collections::{HashSet, VecDeque};

use quick_xml::Reader;
use quick_xml::events::Event;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::detect::Detected;
use super::page_images::inline_page_images;
use super::steps::StepResult;
use super::{Ctx, original_key};
use crate::db::artifacts;
use crate::db::jobs::{self, Job, NewChild};
use crate::db::steps::{ClaimedStep, StepOutput};
use crate::error::{IngestError, StepError};
use crate::infra::storage::StorageError;
use crate::infra::webscraper::ScrapeOpts;
use crate::limits::{
    CRAWL_MAX_LINKS_PER_PAGE, SITEMAP_MAX_BYTES, SITEMAP_MAX_CHILD_SITEMAPS, SITEMAP_MAX_ENTRIES,
    SITEMAP_MAX_FETCHES, SITEMAP_MAX_LEVELS,
};
use crate::netguard::{self, ResolveError};
use crate::providers::ErrorClass;
use crate::sources::fetch::{self, Auth};

/// A parent job's `source`, as the API stores it (`api::sources`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Discovery {
    Crawl(CrawlConfig),
    Sitemap(SitemapConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrawlConfig {
    pub url: String,
    pub max_depth: u32,
    pub max_pages: usize,
    /// Lowercase hostnames; a link's host must equal one or be a subdomain of one.
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub link_query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SitemapConfig {
    pub url: String,
    pub max_urls: usize,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// A child's id: a UUIDv8 over sha256(parent id, page URL), so a retried Discover derives the
/// same ids and its inserts are no-ops.
pub fn child_id(parent: Uuid, url: &str) -> Uuid {
    let mut h = Sha256::new();
    h.update(parent.as_bytes());
    h.update(b"child");
    h.update(url.as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::new_v8(bytes)
}

/// The progress record of one crawled page (`discover/{child id}.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PageRecord {
    url: String,
    depth: u32,
    /// In-scope links of the page (already filtered and normalised), in page order.
    links: Vec<String>,
    /// The page original was stored for the child (it does not scrape again).
    prefetched: bool,
    #[serde(default)]
    final_url: Option<String>,
    /// Why the page was not prefetched (webscraper's error code), if it was not.
    #[serde(default)]
    error: Option<String>,
}

/// Runs a parent job's Discover step.
pub async fn discover(ctx: &Ctx, step: &ClaimedStep) -> StepResult {
    let parent = jobs::job(&ctx.pool, step.tenant, step.job_id)
        .await
        .map_err(internal)?;
    let d: Discovery = serde_json::from_value(parent.source.clone()).map_err(|e| {
        err(
            ErrorClass::InvalidRequest,
            "invalid_source",
            format!("a Discover step needs a crawl or sitemap source: {e}"),
        )
    })?;
    match d {
        Discovery::Crawl(cfg) => crawl(ctx, step, &parent, &cfg).await,
        Discovery::Sitemap(cfg) => sitemap(ctx, step, &parent, &cfg).await,
    }
}

// ---------------------------------------------------------------------------------------------
// crawl

/// Path suffixes of resources a page scrape cannot turn into a document.
const SKIPPED_SUFFIXES: [&str; 28] = [
    ".pdf", ".zip", ".gz", ".tgz", ".rar", ".7z", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".ico", ".bmp", ".tif", ".tiff", ".mp3", ".mp4", ".m4a", ".wav", ".avi", ".mov", ".webm",
    ".css", ".js", ".woff", ".woff2", ".exe",
];

/// `u` without its fragment.
fn normalize(u: &url::Url) -> url::Url {
    let mut u = u.clone();
    u.set_fragment(None);
    u
}

/// Whether `u`'s host is one of `allowed` (lowercase) or a subdomain of one.
fn host_allowed(u: &url::Url, allowed: &[String]) -> bool {
    let Some(host) = u.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    allowed
        .iter()
        .any(|d| host == *d || host.ends_with(&format!(".{d}")))
}

fn skipped_resource(u: &url::Url) -> bool {
    let path = u.path().to_ascii_lowercase();
    SKIPPED_SUFFIXES.iter().any(|s| path.ends_with(s))
}

/// The links of a page the crawl may follow (module docs), normalised, deduplicated, in page
/// order, at most [`CRAWL_MAX_LINKS_PER_PAGE`].
fn crawl_links(raw: &[String], allowed: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for link in raw {
        if out.len() >= CRAWL_MAX_LINKS_PER_PAGE {
            break;
        }
        let Ok(u) = netguard::check_url(link) else {
            continue;
        };
        let u = normalize(&u);
        if !host_allowed(&u, allowed) || skipped_resource(&u) {
            continue;
        }
        let s = u.to_string();
        if seen.insert(s.clone()) {
            out.push(s);
        }
    }
    out
}

async fn crawl(ctx: &Ctx, step: &ClaimedStep, parent: &Job, cfg: &CrawlConfig) -> StepResult {
    let start = netguard::check_url(&cfg.url)
        .map_err(|e| err(ErrorClass::InvalidRequest, "invalid_source", e.to_string()))?;
    let start = normalize(&start).to_string();
    let opts = ScrapeOpts {
        download_images: true,
        link_query: cfg.link_query.clone(),
    };
    let mut queue: VecDeque<(String, u32)> = VecDeque::from([(start.clone(), 0)]);
    let mut seen: HashSet<String> = HashSet::from([start]);
    let (mut pages, mut prefetched, mut replayed, mut created) = (0usize, 0usize, 0usize, 0u64);
    let (mut skipped_unsafe, mut skipped_unresolvable) = (0usize, 0usize);
    while let Some((url, depth)) = queue.pop_front() {
        if pages >= cfg.max_pages {
            break;
        }
        let child = child_id(parent.id, &url);
        let rkey = format!("discover/{child}.json");
        let record: PageRecord = match artifacts::get(&ctx.pool, parent.id, &rkey)
            .await
            .map_err(internal)?
        {
            Some(body) => {
                replayed += 1;
                serde_json::from_str(&body).map_err(|e| {
                    err(
                        ErrorClass::InvalidRequest,
                        "invalid_artefact",
                        format!("{rkey}: {e}"),
                    )
                })?
            }
            None => {
                match page_host(ctx, &url).await {
                    HostCheck::Public => {}
                    HostCheck::Unsafe => {
                        tracing::info!(job = %parent.id, url = %url, "crawl page skipped: its host is not public");
                        skipped_unsafe += 1;
                        continue;
                    }
                    HostCheck::Unresolvable(why) if depth == 0 => {
                        return Err(err(
                            ErrorClass::Retryable,
                            "fetch_unresolvable",
                            format!("the crawl start page's host cannot be resolved: {why}"),
                        ));
                    }
                    HostCheck::Unresolvable(why) => {
                        tracing::info!(job = %parent.id, url = %url, why = %why, "crawl page skipped: its host does not resolve");
                        skipped_unresolvable += 1;
                        continue;
                    }
                }
                let (record, source) =
                    scrape_page(ctx, parent, child, &url, depth, cfg, &opts).await?;
                let body = serde_json::to_string(&record)
                    .map_err(|e| internal(IngestError::InvalidInput(e.to_string())))?;
                created += jobs::add_children(
                    &ctx.pool,
                    step,
                    parent,
                    &[NewChild { id: child, source }],
                    Some((rkey.as_str(), body.as_str())),
                )
                .await
                .map_err(internal)?;
                record
            }
        };
        pages += 1;
        if record.prefetched {
            prefetched += 1;
        }
        if let Some(f) = &record.final_url {
            seen.insert(f.clone());
        }
        if depth < cfg.max_depth {
            for link in &record.links {
                if seen.insert(link.clone()) {
                    queue.push_back((link.clone(), depth + 1));
                }
            }
        }
    }
    if pages == 0 {
        return Err(no_pages(format!(
            "the crawl found no page to ingest ({skipped_unsafe} skipped as non-public, \
             {skipped_unresolvable} unresolvable)"
        )));
    }
    Ok((
        done(json!({
            "kind": "crawl",
            "pages": pages,
            "prefetched": prefetched,
            "deferred": pages - prefetched,
            "replayed": replayed,
            "children_created": created,
            "skipped_unsafe": skipped_unsafe,
            "skipped_unresolvable": skipped_unresolvable,
            "frontier_left": queue.len(),
        })),
        vec![],
    ))
}

/// What [`page_host`] found.
enum HostCheck {
    Public,
    /// A non-public address (or not a URL at all): never scraped.
    Unsafe,
    Unresolvable(String),
}

/// Resolves a crawl page's host with the structured-source guard ([`netguard::resolve_public`],
/// the deployment's [`netguard::Resolution`]): only a host whose every address is public is
/// scraped. `check_url` (syntax, internal names, IP literals) already passed.
async fn page_host(ctx: &Ctx, page: &str) -> HostCheck {
    let Ok(u) = url::Url::parse(page) else {
        return HostCheck::Unsafe;
    };
    match netguard::resolve_public(&u, &ctx.source_fetch.resolution).await {
        Ok(_) => HostCheck::Public,
        Err(ResolveError::Blocked) => HostCheck::Unsafe,
        Err(ResolveError::Unresolvable(why)) => HostCheck::Unresolvable(why),
    }
}

/// Discovery found nothing: the parent fails (module docs), not retried.
fn no_pages(message: String) -> StepError {
    err(ErrorClass::InvalidRequest, "no_pages_discovered", message)
}

/// Scrapes one crawl page (with images) and stores its original for the child. Returns the
/// page's progress record and the child's source. A non-retryable scrape failure is not an
/// error: the child is created without `prefetched` (it scrapes itself and fails visibly).
async fn scrape_page(
    ctx: &Ctx,
    parent: &Job,
    child: Uuid,
    url: &str,
    depth: u32,
    cfg: &CrawlConfig,
    opts: &ScrapeOpts,
) -> Result<(PageRecord, Value), StepError> {
    let mut source = json!({ "kind": "url", "url": url, "parent_job_id": parent.id });
    let deferred = |code: &str| PageRecord {
        url: url.to_string(),
        depth,
        links: vec![],
        prefetched: false,
        final_url: None,
        error: Some(code.to_string()),
    };
    let page = match ctx.webscraper.scrape_with(url, opts).await {
        Ok(p) => p,
        Err(e) if e.class == ErrorClass::Retryable => return Err(e),
        Err(e) => {
            tracing::info!(job = %parent.id, url, code = %e.code, "crawl page not scraped; its child will try");
            return Ok((deferred(e.code.as_str()), source));
        }
    };
    let links = if depth < cfg.max_depth {
        crawl_links(&page.links, &cfg.allowed_domains)
    } else {
        vec![]
    };
    let final_url = url::Url::parse(&page.final_url)
        .ok()
        .map(|u| normalize(&u).to_string());
    let max = ctx.cfg.limits.max_upload_bytes;
    let images = inline_page_images(
        &page.markdown,
        &page.final_url,
        &page.images,
        ctx.cfg.limits.figure_min_bytes,
        max,
    );
    let report = images.report();
    let markdown = images.markdown;
    let size = markdown.len() as u64;
    if size > max {
        let mut r = deferred("too_large");
        r.links = links;
        r.final_url = final_url;
        return Ok((r, source));
    }
    let key = original_key(parent.tenant, child);
    let prefetched = match ctx
        .storage
        .upload_bytes(&key, markdown, Detected::Markdown.mime())
        .await
    {
        Ok(up) => json!({
            "bytes": size, "scan": up.verdict().as_str(), "final_url": page.final_url,
            "title": page.title, "images": report,
        }),
        // Nothing was stored; the child is rejected as `infected` by its Acquire.
        Err(StorageError::Infected(signature)) => json!({
            "bytes": size, "scan": "infected", "signature": signature,
            "final_url": page.final_url, "title": page.title,
        }),
        Err(e) => return Err(e.step_error(&key)),
    };
    source["prefetched"] = prefetched;
    Ok((
        PageRecord {
            url: url.to_string(),
            depth,
            links,
            prefetched: true,
            final_url,
            error: None,
        },
        source,
    ))
}

// ---------------------------------------------------------------------------------------------
// sitemap

/// What a sitemap document is: an index (`<sitemapindex>`, whose `<sitemap><loc>`s name nested
/// sitemaps) or a URL set (`<urlset>`, whose `<url><loc>`s name pages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SitemapKind {
    Index,
    Urls,
}

/// How far [`parse_sitemap`] read a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed {
    pub kind: SitemapKind,
    /// Non-empty `<loc>`s handed to the callback.
    pub locs: usize,
    /// Reading stopped before the end: the callback said stop, or `max_locs` were read.
    pub stopped: bool,
}

fn invalid_sitemap(message: impl Into<String>) -> StepError {
    err(ErrorClass::InvalidRequest, "invalid_sitemap", message)
}

/// `body` inflated when it is gzip (magic `1f 8b`), at most [`SITEMAP_MAX_BYTES`].
pub fn inflate(body: &[u8]) -> Result<Vec<u8>, StepError> {
    if !body.starts_with(&[0x1f, 0x8b]) {
        if body.len() as u64 > SITEMAP_MAX_BYTES {
            return Err(sitemap_too_large());
        }
        return Ok(body.to_vec());
    }
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(body)
        .take(SITEMAP_MAX_BYTES + 1)
        .read_to_end(&mut out)
        .map_err(|_| invalid_sitemap("the sitemap is not valid gzip"))?;
    if out.len() as u64 > SITEMAP_MAX_BYTES {
        return Err(sitemap_too_large());
    }
    Ok(out)
}

fn sitemap_too_large() -> StepError {
    err(
        ErrorClass::InvalidRequest,
        "sitemap_too_large",
        format!("the sitemap inflates past {SITEMAP_MAX_BYTES} bytes"),
    )
}

/// Parses a sitemap document (UTF-8 XML, namespaces ignored), streaming: every non-empty `<loc>`
/// is handed to `on_loc` as it is read, which answers whether to read on. At most `max_locs`
/// `<loc>`s are read. Nothing is collected here: the caller keeps what it accepts.
pub fn parse_sitemap(
    body: &[u8],
    max_locs: usize,
    mut on_loc: impl FnMut(SitemapKind, &str) -> bool,
) -> Result<Parsed, StepError> {
    let xml = std::str::from_utf8(body).map_err(|_| invalid_sitemap("the sitemap is not UTF-8"))?;
    let xml = xml.trim_start_matches('\u{feff}');
    let mut r = Reader::from_str(xml);
    let mut kind: Option<SitemapKind> = None;
    let (mut in_entry, mut in_loc) = (false, false);
    let mut buf = String::new();
    let (mut locs, mut stopped) = (0usize, false);
    loop {
        let ev = r
            .read_event()
            .map_err(|_| invalid_sitemap("the sitemap is not well-formed XML"))?;
        match &ev {
            Event::Start(e) => match e.local_name().as_ref() {
                "urlset" if kind.is_none() => kind = Some(SitemapKind::Urls),
                "sitemapindex" if kind.is_none() => kind = Some(SitemapKind::Index),
                _ if kind.is_none() => {
                    return Err(invalid_sitemap(
                        "not a sitemap: the root element is neither urlset nor sitemapindex",
                    ));
                }
                "url" | "sitemap" => in_entry = true,
                "loc" if in_entry => {
                    in_loc = true;
                    buf.clear();
                }
                _ => {}
            },
            Event::End(e) => match e.local_name().as_ref() {
                "loc" if in_loc => {
                    in_loc = false;
                    let t = buf.trim();
                    if let (Some(k), false) = (kind, t.is_empty()) {
                        if locs >= max_locs {
                            stopped = true;
                            break;
                        }
                        locs += 1;
                        if !on_loc(k, t) {
                            stopped = true;
                            break;
                        }
                    }
                }
                "url" | "sitemap" => in_entry = false,
                _ => {}
            },
            Event::Text(_) | Event::CData(_) | Event::GeneralRef(_) if in_loc => {
                crate::sources::xlsx::push_text(&mut buf, &ev);
            }
            Event::Eof => break,
            _ => {}
        }
    }
    match kind {
        Some(kind) => Ok(Parsed {
            kind,
            locs,
            stopped,
        }),
        None => Err(invalid_sitemap(
            "not a sitemap: no urlset or sitemapindex element",
        )),
    }
}

/// Whether a sitemap URL passes the `include` / `exclude` substring filters.
fn wanted(url: &str, cfg: &SitemapConfig) -> bool {
    (cfg.include.is_empty() || cfg.include.iter().any(|s| url.contains(s.as_str())))
        && !cfg.exclude.iter().any(|s| url.contains(s.as_str()))
}

/// The state of one sitemap walk. Bounded: `urls` / `seen` hold at most `max_urls` page URLs,
/// `pending` / `maps_seen` at most [`SITEMAP_MAX_CHILD_SITEMAPS`] (+ the root) sitemap URLs.
#[derive(Debug, Default)]
struct Walk {
    /// Accepted page URLs, in sitemap order.
    urls: Vec<String>,
    seen: HashSet<String>,
    /// Sitemaps to fetch, with their nesting level (the root is 0).
    pending: VecDeque<(url::Url, u32)>,
    /// Normalised URLs of the sitemaps queued so far (the root included).
    maps_seen: HashSet<String>,
    /// Nested sitemaps queued (≤ [`SITEMAP_MAX_CHILD_SITEMAPS`]).
    queued: usize,
    /// Sitemap documents fetched (≤ [`SITEMAP_MAX_FETCHES`]).
    fetched: usize,
    /// Nested sitemaps not followed: past a cap or the nesting depth, gone or broken.
    skipped: usize,
    /// `<loc>`s read over all documents.
    locs_read: usize,
    /// Documents whose reading stopped early (`max_urls` reached or `SITEMAP_MAX_ENTRIES` read).
    truncated: usize,
}

impl Walk {
    /// Takes one `<loc>` of a document at `level`; answers whether to read on.
    fn take(&mut self, kind: SitemapKind, loc: &str, level: u32, cfg: &SitemapConfig) -> bool {
        match kind {
            SitemapKind::Index => {
                if level >= SITEMAP_MAX_LEVELS || self.queued >= SITEMAP_MAX_CHILD_SITEMAPS {
                    self.skipped += 1;
                    return true;
                }
                if let Ok(u) = netguard::check_url(loc)
                    && self.maps_seen.insert(normalize(&u).to_string())
                {
                    self.queued += 1;
                    self.pending.push_back((u, level + 1));
                }
                true
            }
            SitemapKind::Urls => {
                if self.urls.len() >= cfg.max_urls {
                    return false;
                }
                if let Ok(u) = netguard::check_url(loc) {
                    let s = normalize(&u).to_string();
                    if wanted(&s, cfg) && self.seen.insert(s.clone()) {
                        self.urls.push(s);
                    }
                }
                self.urls.len() < cfg.max_urls
            }
        }
    }
}

/// Fetches one sitemap document with the guarded fetcher.
async fn fetch_sitemap(ctx: &Ctx, url: &url::Url) -> Result<bytes::Bytes, StepError> {
    let fetched = fetch::fetch(url, &Auth::None, &ctx.source_fetch)
        .await
        .map_err(|e| e.step_error())?;
    Ok(fetched.body)
}

/// Inflates and parses one document into `walk`, off the async runtime; hands `walk` back with
/// the parse result.
async fn read_sitemap(
    walk: Walk,
    body: bytes::Bytes,
    level: u32,
    cfg: &SitemapConfig,
) -> Result<(Walk, Result<Parsed, StepError>), StepError> {
    let cfg = cfg.clone();
    tokio::task::spawn_blocking(move || {
        let mut walk = walk;
        let parsed = inflate(&body).and_then(|xml| {
            parse_sitemap(&xml, SITEMAP_MAX_ENTRIES, |kind, loc| {
                walk.take(kind, loc, level, &cfg)
            })
        });
        (walk, parsed)
    })
    .await
    .map_err(|e| internal(IngestError::Io(std::io::Error::other(e))))
}

async fn sitemap(ctx: &Ctx, step: &ClaimedStep, parent: &Job, cfg: &SitemapConfig) -> StepResult {
    let root = netguard::check_url(&cfg.url)
        .map_err(|e| err(ErrorClass::InvalidRequest, "invalid_source", e.to_string()))?;
    let mut walk = Walk::default();
    walk.maps_seen.insert(normalize(&root).to_string());
    walk.pending.push_back((root, 0));
    while let Some((map, level)) = walk.pending.pop_front() {
        if walk.urls.len() >= cfg.max_urls {
            break;
        }
        if walk.fetched >= SITEMAP_MAX_FETCHES {
            walk.skipped += 1;
            continue;
        }
        walk.fetched += 1;
        // The root sitemap decides the job; a nested one that is gone or broken is skipped
        // (URLs read before a parse error are kept), unless the failure is worth retrying.
        let parsed = match fetch_sitemap(ctx, &map).await {
            Ok(body) => {
                let (w, parsed) = read_sitemap(walk, body, level, cfg).await?;
                walk = w;
                parsed
            }
            Err(e) => Err(e),
        };
        match parsed {
            Ok(p) => {
                walk.locs_read += p.locs;
                if p.stopped {
                    walk.truncated += 1;
                }
            }
            Err(e) if level == 0 || e.class == ErrorClass::Retryable => return Err(e),
            Err(e) => {
                tracing::info!(job = %parent.id, code = %e.code, "nested sitemap skipped");
                walk.skipped += 1;
            }
        }
    }
    if walk.urls.is_empty() {
        return Err(no_pages(format!(
            "the sitemap lists no page to ingest ({} <loc>s read from {} sitemaps, none kept by \
             the filters and the url guard)",
            walk.locs_read, walk.fetched
        )));
    }
    let children: Vec<NewChild> = walk
        .urls
        .iter()
        .map(|u| NewChild {
            id: child_id(parent.id, u),
            source: json!({ "kind": "url", "url": u, "parent_job_id": parent.id }),
        })
        .collect();
    let created = jobs::add_children(&ctx.pool, step, parent, &children, None)
        .await
        .map_err(internal)?;
    Ok((
        done(json!({
            "kind": "sitemap",
            "urls": walk.urls.len(),
            "children_created": created,
            "sitemaps_fetched": walk.fetched,
            "sitemaps_queued": walk.queued,
            "sitemaps_skipped": walk.skipped,
            "locs_read": walk.locs_read,
            "sitemaps_truncated": walk.truncated,
        })),
        vec![],
    ))
}

// ---------------------------------------------------------------------------------------------
// helpers (as in `steps`)

fn done(output: Value) -> StepOutput {
    StepOutput {
        output,
        ..StepOutput::default()
    }
}

fn err(class: ErrorClass, code: &str, message: impl Into<String>) -> StepError {
    StepError {
        class,
        code: code.to_string(),
        message: message.into(),
        provider: None,
        retry_after_s: None,
    }
}

/// A DB (or other internal) failure: retryable; `LeaseLost` keeps its own code.
fn internal(e: IngestError) -> StepError {
    let code = match e {
        IngestError::LeaseLost { .. } => "lease_lost",
        _ => "internal",
    };
    err(ErrorClass::Retryable, code, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_are_scoped_normalised_and_deduplicated() {
        let allowed = vec!["example.com".to_string()];
        let raw: Vec<String> = [
            "https://example.com/a#top",
            "https://example.com/a",
            "https://docs.example.com/b",
            "https://other.org/c",
            "https://example.com/file.PDF",
            "mailto:x@example.com",
            "http://localhost/x",
            "https://notexample.com/d",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            crawl_links(&raw, &allowed),
            ["https://example.com/a", "https://docs.example.com/b"]
        );
    }

    #[test]
    fn child_ids_are_deterministic() {
        let p = Uuid::now_v7();
        assert_eq!(child_id(p, "https://a/"), child_id(p, "https://a/"));
        assert_ne!(child_id(p, "https://a/"), child_id(p, "https://b/"));
        assert_eq!(child_id(p, "https://a/").get_version_num(), 8);
    }

    /// Every `<loc>` of `body` (at most `max`), with the parse summary.
    fn collect(body: &[u8], max: usize) -> Result<(Vec<String>, Parsed), StepError> {
        let mut locs = Vec::new();
        let parsed = parse_sitemap(body, max, |_, l| {
            locs.push(l.to_string());
            true
        })?;
        Ok((locs, parsed))
    }

    fn urlset(n: usize) -> String {
        let urls: String = (0..n)
            .map(|i| format!("<url><loc>https://example.com/p/{i}</loc></url>"))
            .collect();
        format!(r#"<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>"#)
    }

    fn sitemap_cfg(max_urls: usize) -> SitemapConfig {
        SitemapConfig {
            url: "https://example.com/sitemap.xml".into(),
            max_urls,
            include: vec![],
            exclude: vec![],
        }
    }

    #[test]
    fn sitemaps_parse_and_gzip_inflates() {
        let set = br#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a?x=1&amp;y=2</loc><lastmod>2026-01-01</lastmod></url>
  <url><loc> https://example.com/b </loc></url>
</urlset>"#;
        let (locs, parsed) = collect(set, SITEMAP_MAX_ENTRIES).unwrap();
        assert_eq!(
            locs,
            ["https://example.com/a?x=1&y=2", "https://example.com/b"]
        );
        assert_eq!(
            parsed,
            Parsed {
                kind: SitemapKind::Urls,
                locs: 2,
                stopped: false
            }
        );
        let index = br#"<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/s1.xml</loc></sitemap></sitemapindex>"#;
        let (locs, parsed) = collect(index, SITEMAP_MAX_ENTRIES).unwrap();
        assert_eq!(locs, ["https://example.com/s1.xml"]);
        assert_eq!(parsed.kind, SitemapKind::Index);
        assert_eq!(
            collect(b"<html><body/></html>", 10).unwrap_err().code,
            "invalid_sitemap"
        );
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut gz, set).unwrap();
        let gz = gz.finish().unwrap();
        assert_eq!(inflate(&gz).unwrap(), set.to_vec());
        assert_eq!(inflate(set).unwrap(), set.to_vec());
    }

    #[test]
    fn parsing_stops_at_the_per_document_cap_and_when_told() {
        let doc = urlset(10);
        let (locs, parsed) = collect(doc.as_bytes(), 3).unwrap();
        assert_eq!(locs.len(), 3);
        assert_eq!((parsed.locs, parsed.stopped), (3, true));
        // Exactly at the cap: read to the end, not stopped.
        let (locs, parsed) = collect(doc.as_bytes(), 10).unwrap();
        assert_eq!((locs.len(), parsed.locs, parsed.stopped), (10, 10, false));
        // The callback stops the read.
        let mut seen = 0;
        let parsed = parse_sitemap(doc.as_bytes(), SITEMAP_MAX_ENTRIES, |_, _| {
            seen += 1;
            seen < 4
        })
        .unwrap();
        assert_eq!((seen, parsed.locs, parsed.stopped), (4, 4, true));
    }

    #[test]
    fn a_walk_holds_at_most_max_urls_and_the_child_sitemap_cap() {
        // A urlset far larger than max_urls: reading stops at the max_urls-th accepted URL.
        let cfg = sitemap_cfg(5);
        let mut walk = Walk::default();
        let doc = urlset(2000);
        let parsed = parse_sitemap(doc.as_bytes(), SITEMAP_MAX_ENTRIES, |k, l| {
            walk.take(k, l, 0, &cfg)
        })
        .unwrap();
        assert_eq!((parsed.locs, parsed.stopped), (5, true));
        assert_eq!(walk.urls.len(), 5);
        assert_eq!(walk.seen.len(), 5);

        // An index naming far more sitemaps than the cap: only the cap is queued, the rest is
        // counted, never held.
        let cfg = sitemap_cfg(1000);
        let mut walk = Walk::default();
        let maps: String = (0..SITEMAP_MAX_CHILD_SITEMAPS * 3)
            .map(|i| format!("<sitemap><loc>https://example.com/s/{i}.xml</loc></sitemap>"))
            .collect();
        let index = format!("<sitemapindex>{maps}</sitemapindex>");
        parse_sitemap(index.as_bytes(), SITEMAP_MAX_ENTRIES, |k, l| {
            walk.take(k, l, 0, &cfg)
        })
        .unwrap();
        assert_eq!(walk.pending.len(), SITEMAP_MAX_CHILD_SITEMAPS);
        assert_eq!(walk.maps_seen.len(), SITEMAP_MAX_CHILD_SITEMAPS);
        assert_eq!(walk.queued, SITEMAP_MAX_CHILD_SITEMAPS);
        assert_eq!(walk.skipped, SITEMAP_MAX_CHILD_SITEMAPS * 2);
        // A second index adds nothing: the cap is per walk, not per document.
        parse_sitemap(index.as_bytes(), SITEMAP_MAX_ENTRIES, |k, l| {
            walk.take(k, l, 1, &cfg)
        })
        .unwrap();
        assert_eq!(walk.pending.len(), SITEMAP_MAX_CHILD_SITEMAPS);
        // An index at the deepest level queues nothing.
        let mut deep = Walk::default();
        parse_sitemap(index.as_bytes(), SITEMAP_MAX_ENTRIES, |k, l| {
            deep.take(k, l, SITEMAP_MAX_LEVELS, &cfg)
        })
        .unwrap();
        assert!(deep.pending.is_empty());
    }
}
