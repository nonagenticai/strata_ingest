//! The guarded GET of an api / datatable source.
//!
//! Every hop (the URL and each redirect target): [`netguard::check_url`] (http(s), no userinfo,
//! no internal hostname or non-public literal IP), then [`netguard::resolve_public`] (every
//! resolved address public), then a connection pinned to exactly those addresses (no proxy, no
//! automatic redirects, so nothing re-resolves the name behind the check). At most
//! [`MAX_REDIRECTS`] redirects; the credential is sent only to the URL's own origin. The whole
//! exchange is bounded by [`FetchOpts::timeout`] and the body by [`FetchOpts::max_bytes`].
//!
//! The credential never appears in an error: messages are built from status codes and error
//! kinds only (never a response body), and [`fetch`] scrubs the secret out of every message as
//! a second guard.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderName, HeaderValue, LOCATION};

use super::SourceError;
use crate::config::Secret;
use crate::netguard::{self, Resolution, ResolveError};

pub const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
/// The largest source body read (a JSON response, a CSV or XLSX file).
pub const MAX_FETCH_BYTES: u64 = 25 * 1024 * 1024;
pub const MAX_REDIRECTS: usize = 3;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetch bounds and name resolution. Production uses [`Default`]; tests shorten the bounds and
/// pin names to their own listeners.
#[derive(Debug, Clone)]
pub struct FetchOpts {
    pub timeout: Duration,
    pub max_bytes: u64,
    pub resolution: Resolution,
}

impl Default for FetchOpts {
    fn default() -> Self {
        Self {
            timeout: FETCH_TIMEOUT,
            max_bytes: MAX_FETCH_BYTES,
            resolution: Resolution::default(),
        }
    }
}

/// How the credential is presented.
#[derive(Debug)]
pub enum Auth {
    None,
    /// `Authorization: Bearer <secret>`.
    Bearer(Secret),
    /// `<name>: <secret>`.
    Header(HeaderName, Secret),
}

impl Auth {
    fn secret(&self) -> Option<&str> {
        match self {
            Auth::None => None,
            Auth::Bearer(s) | Auth::Header(_, s) => Some(s.expose()),
        }
    }

    /// The header to send, marked sensitive. `None` for [`Auth::None`]; an invalid value was
    /// refused when the request was parsed (see `api::sources`).
    fn header(&self) -> Option<(HeaderName, HeaderValue)> {
        let (name, value) = match self {
            Auth::None => return None,
            Auth::Bearer(s) => (AUTHORIZATION, format!("Bearer {}", s.expose())),
            Auth::Header(n, s) => (n.clone(), s.expose().to_string()),
        };
        let mut v = HeaderValue::from_str(&value).ok()?;
        v.set_sensitive(true);
        Some((name, v))
    }
}

/// What the source answered.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub body: Bytes,
    pub content_type: Option<String>,
}

/// GETs `url` under the guard (module docs). Errors carry no credential.
pub async fn fetch(url: &url::Url, auth: &Auth, opts: &FetchOpts) -> Result<Fetched, SourceError> {
    let r = match tokio::time::timeout(opts.timeout, fetch_hops(url, auth, opts)).await {
        Ok(r) => r,
        Err(_) => Err(SourceError::retryable(
            "fetch_timeout",
            format!(
                "the source did not answer within {} s",
                opts.timeout.as_secs()
            ),
        )),
    };
    r.map_err(|mut e| {
        if let Some(s) = auth.secret().filter(|s| !s.is_empty()) {
            e.message = e.message.replace(s, "[redacted]");
        }
        e
    })
}

fn blocked() -> SourceError {
    SourceError::invalid(
        "fetch_blocked",
        "the source host is a private, loopback, link-local or internal address",
    )
}

async fn fetch_hops(url: &url::Url, auth: &Auth, opts: &FetchOpts) -> Result<Fetched, SourceError> {
    let origin = url.origin();
    let mut current = url.clone();
    for hop in 0..=MAX_REDIRECTS {
        let checked = netguard::check_url(current.as_str()).map_err(|_| blocked())?;
        let addrs = netguard::resolve_public(&checked, &opts.resolution)
            .await
            .map_err(|e| match e {
                ResolveError::Blocked => blocked(),
                ResolveError::Unresolvable(why) => SourceError::retryable(
                    "fetch_unresolvable",
                    format!("the source host cannot be resolved: {why}"),
                ),
            })?;
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(opts.timeout);
        if let Some(url::Host::Domain(d)) = checked.host() {
            builder = builder.resolve_to_addrs(d, &addrs);
        }
        let client = builder
            .build()
            .map_err(|_| SourceError::retryable("fetch_failed", "HTTP client unavailable"))?;
        let mut req = client
            .get(checked.clone())
            .header(ACCEPT, "application/json, text/csv, */*;q=0.5");
        // The credential belongs to the source's origin only, never to a redirect elsewhere.
        if checked.origin() == origin
            && let Some((name, value)) = auth.header()
        {
            req = req.header(name, value);
        }
        let resp = req.send().await.map_err(transport)?;
        let status = resp.status();
        if status.is_redirection() {
            let next = resp
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|loc| checked.join(loc).ok())
                .ok_or_else(|| {
                    SourceError::invalid(
                        "fetch_failed",
                        format!(
                            "the source answered {} without a usable Location",
                            status.as_u16()
                        ),
                    )
                })?;
            if hop == MAX_REDIRECTS {
                break;
            }
            current = next;
            continue;
        }
        if !status.is_success() {
            let s = status.as_u16();
            let message = format!("the source answered HTTP {s}");
            return Err(match s {
                408 | 429 | 500..=599 => SourceError::retryable("source_unavailable", message),
                401 | 403 => SourceError::invalid("source_auth", message),
                _ => SourceError::invalid("source_rejected", message),
            });
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let too_large = || {
            SourceError::invalid(
                "too_large",
                format!("the source body exceeds {} bytes", opts.max_bytes),
            )
        };
        if resp.content_length().is_some_and(|n| n > opts.max_bytes) {
            return Err(too_large());
        }
        let mut body = BytesMut::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(transport)?;
            if (body.len() + chunk.len()) as u64 > opts.max_bytes {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        return Ok(Fetched {
            body: body.freeze(),
            content_type,
        });
    }
    Err(SourceError::invalid(
        "too_many_redirects",
        format!("the source redirected more than {MAX_REDIRECTS} times"),
    ))
}

/// A transport failure, by kind only (reqwest's message would carry the URL).
fn transport(e: reqwest::Error) -> SourceError {
    if e.is_timeout() {
        SourceError::retryable("fetch_timeout", "the source timed out")
    } else if e.is_connect() {
        SourceError::retryable("fetch_failed", "cannot connect to the source")
    } else {
        SourceError::retryable("fetch_failed", "the source connection failed")
    }
}
