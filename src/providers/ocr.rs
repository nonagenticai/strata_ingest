//! OCR client, Mistral OCR contract: `POST {base}/ocr` with
//! `{model, document: {type: "document_url", document_url: "data:<mime>;base64,…"}
//! (or `{type: "image_url", image_url: …}` for images), pages?, include_image_base64: false}`,
//! answered by `{pages: [{index, markdown, …}], usage_info: {pages_processed}}`.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use super::{Body, Capability, Http, ProviderError, ProviderHealth, Usage, UsageUnit, data_url};
use crate::config::ProviderCfg;

/// A document (PDF) or a single image to OCR.
#[derive(Debug, Clone)]
pub struct OcrInput {
    pub bytes: Bytes,
    pub mime: String,
    /// 0-based page indices; `None` = all pages.
    pub pages: Option<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OcrPage {
    /// 0-based page index in the document.
    pub index: u32,
    pub markdown: String,
}

/// One entry per OCR'd page, ascending `index`, no duplicates; usage in pages. Whether every
/// requested page came back is the caller's check (the OcrPages step fails `ocr_missing_page`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OcrPages {
    pub pages: Vec<OcrPage>,
    pub usage: Usage,
}

pub struct OcrClient {
    http: Http,
}

impl fmt::Debug for OcrClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OcrClient")
            .field("http", &self.http)
            .finish()
    }
}

#[derive(Deserialize)]
struct OcrResponse {
    pages: Vec<RawPage>,
    #[serde(default)]
    usage_info: Option<UsageInfo>,
}

#[derive(Deserialize)]
struct RawPage {
    index: u32,
    markdown: String,
}

#[derive(Deserialize)]
struct UsageInfo {
    #[serde(default)]
    pages_processed: Option<i64>,
}

impl OcrClient {
    pub fn new(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Ocr, &["mistral"], cfg, health, false)?,
        })
    }

    /// A tenant-registered provider: connects only to public addresses, no proxy.
    pub fn guarded(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Ocr, &["mistral"], cfg, health, true)?,
        })
    }

    pub async fn ocr(&self, input: OcrInput) -> Result<OcrPages, ProviderError> {
        let r = self.call(&input).await;
        self.http.record(r)
    }

    async fn call(&self, input: &OcrInput) -> Result<OcrPages, ProviderError> {
        let url = data_url(&input.mime, &input.bytes);
        let document = if input.mime.starts_with("image/") {
            json!({"type": "image_url", "image_url": url})
        } else {
            json!({"type": "document_url", "document_url": url})
        };
        let mut body = json!({
            "model": self.http.model(),
            "document": document,
            "include_image_base64": false,
        });
        if let Some(pages) = &input.pages {
            body["pages"] = json!(pages);
        }
        let bytes = serde_json::to_vec(&body).expect("a json! value always serializes");
        let resp: OcrResponse = self.http.post("/ocr", Body::Json(bytes)).await?;

        let mut pages: Vec<OcrPage> = resp
            .pages
            .into_iter()
            .map(|p| OcrPage {
                index: p.index,
                markdown: p.markdown,
            })
            .collect();
        pages.sort_by_key(|p| p.index);
        if pages.windows(2).any(|w| w[0].index == w[1].index) {
            return Err(self.http.invalid_response("duplicate page index"));
        }
        Ok(OcrPages {
            pages,
            usage: Usage {
                units: resp.usage_info.and_then(|u| u.pages_processed),
                unit: UsageUnit::Pages,
            },
        })
    }
}
