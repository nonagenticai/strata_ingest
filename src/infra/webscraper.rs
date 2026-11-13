//! webscraper_service client: `POST {base}/api/scrape/url` with `{url, scraper_type: "auto"}`
//! (plus `download_images: true` and `link_query` / `sort_links_by_score` when asked, see
//! [`ScrapeOpts`]), licence token as `X-Tenant-Id: Bearer <token>` when configured. Response
//! fields used (webscraper_service `src/schemas/scrape.py` `ScrapeResponse`): `url`,
//! `final_url`, `title`, `markdown_content`, `status` (`completed` | `failed` | …), `error`,
//! `links[].url` and `images[]` (`src`, `alt`, `mime_type`, `content_base64`). The answer is
//! read up to `MAX_SCRAPE_RESPONSE_BYTES`; a larger one with images is asked for again without.

use std::fmt;
use std::time::Duration;

use futures::StreamExt;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{http_client, snippet, step_err, transport_message};
use crate::config::Secret;
use crate::error::StepError;
use crate::limits::MAX_SCRAPE_RESPONSE_BYTES;
use crate::providers::{ErrorClass, classify};

/// Browser-rendered pages can be slow; the scraper has its own per-page timeout below this.
pub const TIMEOUT: Duration = Duration::from_secs(180);
const SERVICE: &str = "webscraper";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scraped {
    /// After redirects; the requested URL when the scraper did not report one.
    pub final_url: String,
    pub title: Option<String>,
    pub markdown: String,
    /// `links[].url`, in the scraper's order (empty ones dropped).
    pub links: Vec<String>,
    /// `images[]` (only with [`ScrapeOpts::download_images`] do they carry content).
    pub images: Vec<PageImage>,
}

/// One `images[]` entry of a scrape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageImage {
    /// The image URL as the scraper resolved it (absolute, or a `data:` URI).
    pub src: String,
    pub alt: String,
    /// The downloaded image's MIME type.
    pub mime: Option<String>,
    /// The downloaded bytes, base64 (standard alphabet).
    pub content_base64: Option<String>,
}

/// What to ask the scraper for besides the markdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeOpts {
    /// `download_images: true`: the page's images come back with their bytes.
    pub download_images: bool,
    /// `link_query` + `sort_links_by_score: true`: links ranked by relevance to this topic.
    pub link_query: Option<String>,
}

pub struct WebscraperClient {
    http: reqwest::Client,
    base: String,
    /// `Bearer <token>`, marked sensitive.
    auth: Option<HeaderValue>,
}

impl fmt::Debug for WebscraperClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebscraperClient")
            .field("base", &crate::config::redact_url(&self.base))
            .field("token", &self.auth.as_ref().map(|_| "***"))
            .finish()
    }
}

#[derive(Deserialize)]
struct ScrapeResponse {
    url: Option<String>,
    final_url: Option<String>,
    title: Option<String>,
    markdown_content: Option<String>,
    status: Option<String>,
    error: Option<String>,
    #[serde(default)]
    links: Option<Vec<LinkIn>>,
    #[serde(default)]
    images: Option<Vec<ImageIn>>,
}

#[derive(Deserialize)]
struct LinkIn {
    #[serde(default)]
    url: Option<String>,
}

#[derive(Deserialize)]
struct ImageIn {
    #[serde(default)]
    src: Option<String>,
    #[serde(default)]
    alt: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    content_base64: Option<String>,
}

impl WebscraperClient {
    pub fn new(base_url: &str, token: Option<Secret>) -> crate::Result<Self> {
        let auth = token
            .map(|t| {
                let mut v =
                    HeaderValue::from_str(&format!("Bearer {}", t.expose())).map_err(|_| {
                        crate::error::IngestError::Config(
                            "STRATA_INGEST_WEBSCRAPER_TOKEN is not a valid header value".into(),
                        )
                    })?;
                v.set_sensitive(true);
                Ok::<_, crate::error::IngestError>(v)
            })
            .transpose()?;
        Ok(Self {
            http: http_client(TIMEOUT)?,
            base: base_url.trim().trim_end_matches('/').to_string(),
            auth,
        })
    }

    /// Fetches `url` as markdown. One attempt.
    pub async fn scrape(&self, url: &str) -> Result<Scraped, StepError> {
        self.scrape_with(url, &ScrapeOpts::default()).await
    }

    /// [`scrape`](Self::scrape) with [`ScrapeOpts`]. An answer over `MAX_SCRAPE_RESPONSE_BYTES`
    /// with images asked for is fetched once more without them (the page keeps its links).
    pub async fn scrape_with(&self, url: &str, opts: &ScrapeOpts) -> Result<Scraped, StepError> {
        let mut req = json!({ "url": url, "scraper_type": "auto" });
        if let Some(q) = &opts.link_query {
            req["link_query"] = json!(q);
            req["sort_links_by_score"] = json!(true);
        }
        let body = if opts.download_images {
            let mut with = req.clone();
            with["download_images"] = json!(true);
            match self.post(&with).await? {
                Some(b) => b,
                None => {
                    tracing::warn!(
                        max = MAX_SCRAPE_RESPONSE_BYTES,
                        "webscraper answer with images is too large; scraping without images"
                    );
                    self.post(&req)
                        .await?
                        .ok_or_else(|| too_large_answer(url))?
                }
            }
        } else {
            self.post(&req)
                .await?
                .ok_or_else(|| too_large_answer(url))?
        };
        Self::parse(url, &body)
    }

    /// One POST; `Ok(None)` when the answer is larger than `MAX_SCRAPE_RESPONSE_BYTES`.
    async fn post(&self, req_body: &Value) -> Result<Option<String>, StepError> {
        let mut req = self
            .http
            .post(format!("{}/api/scrape/url", self.base))
            .json(req_body);
        if let Some(auth) = &self.auth {
            req = req.header("X-Tenant-Id", auth.clone());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| retryable(transport_message(e)))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .map_err(|e| retryable(transport_message(e)))?;
            let s = status.as_u16();
            let msg = format!("webscraper status {s}: {}", snippet(&body));
            return Err(match classify(s) {
                ErrorClass::Retryable => retryable(msg),
                // Our licence token was refused: not this URL's fault, and usually a licences
                // service blip or a rotation in progress, so retried (R41).
                ErrorClass::Auth => {
                    step_err(ErrorClass::Retryable, "webscraper_auth", SERVICE, msg)
                }
                _ => step_err(ErrorClass::InvalidRequest, "fetch_rejected", SERVICE, msg),
            });
        }
        if resp
            .content_length()
            .is_some_and(|n| n > MAX_SCRAPE_RESPONSE_BYTES)
        {
            return Ok(None);
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| retryable(transport_message(e)))?;
            if (buf.len() + chunk.len()) as u64 > MAX_SCRAPE_RESPONSE_BYTES {
                return Ok(None);
            }
            buf.extend_from_slice(&chunk);
        }
        let body = String::from_utf8_lossy(&buf).into_owned();
        Ok(Some(body))
    }

    fn parse(url: &str, body: &str) -> Result<Scraped, StepError> {
        let r: ScrapeResponse = serde_json::from_str(body)
            .map_err(|e| retryable(format!("webscraper response is not a ScrapeResponse: {e}")))?;
        if r.status.as_deref() == Some("failed") {
            return Err(step_err(
                ErrorClass::InvalidRequest,
                "fetch_failed",
                SERVICE,
                format!(
                    "scrape failed: {}",
                    snippet(r.error.as_deref().unwrap_or("no error given"))
                ),
            ));
        }
        let markdown = r.markdown_content.unwrap_or_default();
        if markdown.trim().is_empty() {
            return Err(step_err(
                ErrorClass::InvalidRequest,
                "empty_page",
                SERVICE,
                format!("no markdown content extracted from {url}"),
            ));
        }
        Ok(Scraped {
            final_url: r
                .final_url
                .filter(|u| !u.is_empty())
                .or(r.url)
                .unwrap_or_else(|| url.to_string()),
            title: r.title.filter(|t| !t.trim().is_empty()),
            markdown,
            links: r
                .links
                .unwrap_or_default()
                .into_iter()
                .filter_map(|l| l.url.filter(|u| !u.trim().is_empty()))
                .collect(),
            images: r
                .images
                .unwrap_or_default()
                .into_iter()
                .filter_map(|i| {
                    Some(PageImage {
                        src: i.src.filter(|s| !s.trim().is_empty())?,
                        alt: i.alt.unwrap_or_default(),
                        mime: i.mime_type,
                        content_base64: i.content_base64,
                    })
                })
                .collect(),
        })
    }
}

fn too_large_answer(url: &str) -> StepError {
    step_err(
        ErrorClass::InvalidRequest,
        "fetch_too_large",
        SERVICE,
        format!("the scraped page {url} is larger than {MAX_SCRAPE_RESPONSE_BYTES} bytes"),
    )
}

fn retryable(message: String) -> StepError {
    step_err(ErrorClass::Retryable, "fetch_unavailable", SERVICE, message)
}
