//! docling-serve (v1.37.0) client: `POST {base}/v1/convert/file`, multipart field `files` plus
//! form options, answered by `{document: {md_content, …}, status, errors: […], processing_time,
//! timings}`.
//!
//! Pages: docling's markdown separates pages with [`PAGE_BREAK`], but only where the page of
//! the next serialized item changes (measured on v1.37): a page without items (a scan before
//! OCR, a blank page) gets no segment of its own, wherever it sits, and an all-scanned PDF comes
//! back as empty markdown. So `json` is requested too and [`DoclingResult::markdown`] is rebuilt
//! from its provenance (rulings R30): segment *i* is the *i*-th distinct page of the body items
//! (each item counted on its first `prov` page, so items spanning pages are fine), every other
//! page is empty, and splitting on [`PAGE_BREAK`] yields exactly one entry per page, in order.
//! Provenance that contradicts the segments → InvalidRequest `convert_failed`, never a guess.
//!
//! `success` / `partial_success` → ok (errors are logged), unless the markdown is empty or
//! whitespace and the input has no pages → InvalidRequest `empty_conversion`; `failure` / `skipped` → InvalidRequest
//! `convert_failed`; 5xx, 408/429, timeouts and transport failures → Retryable. An answer over
//! [`MAX_RESPONSE_BYTES`] → InvalidRequest `conversion_too_large` (read streamed, never whole).
//!
//! `page_range` (rulings R37): docling converts only that window and reports its pages under
//! their whole-document numbers (measured on v1.37); the markdown is then padded to the window,
//! so it splits into one entry per page of the window, in order.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;

use super::{http_client, snippet, step_err, transport_message};
use crate::error::StepError;
use crate::providers::{ErrorClass, classify};

/// Large documents (hundreds of pages, `table_mode=accurate` on CPU) take many minutes
/// (rulings R37). The Convert step deadline sits above this.
pub const TIMEOUT: Duration = Duration::from_secs(1800);
/// Largest docling answer read (rulings R37): md + json with every picture embedded can be
/// hundreds of MB for an image-heavy PDF. Past it → InvalidRequest `conversion_too_large`.
pub const MAX_RESPONSE_BYTES: u64 = 512 * 1024 * 1024;
/// Docling's page separator in the markdown; the convert step splits pages on it.
pub const PAGE_BREAK: &str = "<!-- page-break -->";
const SERVICE: &str = "docling";

/// Per-call OCR options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoclingOpts {
    pub do_ocr: bool,
    pub force_ocr: bool,
    /// docling's `ocr_engine`; `easyocr` is the working engine in this image (rulings R9).
    pub ocr_engine: String,
    /// 1-based inclusive page range to convert (docling `page_range`); `None` = all pages.
    /// docling then reports only those pages, numbered as in the whole document.
    pub page_range: Option<(u32, u32)>,
    /// Pictures embedded as data URIs (`embedded`, needed for figures) or left as placeholders
    /// (`placeholder`: the OCR path, which only wants text).
    pub embed_images: bool,
}

impl Default for DoclingOpts {
    fn default() -> Self {
        Self {
            do_ocr: true,
            force_ocr: false,
            ocr_engine: "easyocr".to_string(),
            page_range: None,
            embed_images: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoclingResult {
    /// Padded so it splits on [`PAGE_BREAK`] into exactly `page_count` pages when docling
    /// reported pages (see the module docs).
    pub markdown: String,
    /// Pages docling reported (`json_content.pages`); 0 for formats without pages (docx, html…)
    /// or when the JSON was not returned.
    pub page_count: usize,
    /// `success` or `partial_success`.
    pub status: String,
    /// docling's `errors[]`, one line each (non-empty only for `partial_success`, normally).
    pub errors: Vec<String>,
    /// Pages (1-based, whole-document numbers) inside the reported range that a
    /// `partial_success` left out (their conversion failed): each is an empty page in
    /// `markdown`, so it counts as low-text and is OCR'd.
    pub missing_pages: Vec<usize>,
}

pub struct DoclingClient {
    http: reqwest::Client,
    base: String,
    max_response: u64,
}

impl fmt::Debug for DoclingClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DoclingClient")
            .field("base", &crate::config::redact_url(&self.base))
            .finish()
    }
}

#[derive(Deserialize)]
struct ConvertResponse {
    #[serde(default)]
    document: Option<Document>,
    status: String,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct Document {
    #[serde(default)]
    md_content: Option<String>,
    #[serde(default)]
    json_content: Option<DocJson>,
}

/// The parts of a DoclingDocument that say on which pages its items sit.
#[derive(Deserialize, Default)]
struct DocJson {
    #[serde(default)]
    pages: std::collections::HashMap<String, serde_json::Value>,
    #[serde(default)]
    texts: Vec<DocItem>,
    #[serde(default)]
    tables: Vec<DocItem>,
    #[serde(default)]
    pictures: Vec<DocItem>,
    #[serde(default)]
    key_value_items: Vec<DocItem>,
    #[serde(default)]
    form_items: Vec<DocItem>,
}

#[derive(Deserialize)]
struct DocItem {
    #[serde(default)]
    prov: Vec<Prov>,
    #[serde(default)]
    content_layer: Option<String>,
}

#[derive(Deserialize)]
struct Prov {
    page_no: usize,
}

impl DocJson {
    /// Sorted distinct 1-based pages the markdown serializer walks: the first `prov` page of
    /// every body item, whatever it serializes to (an empty table on a scanned page still opens
    /// a segment — measured). Furniture (page headers/footers) is not in the markdown.
    fn item_pages(&self) -> Vec<usize> {
        let mut pages: Vec<usize> = [
            &self.texts,
            &self.tables,
            &self.pictures,
            &self.key_value_items,
            &self.form_items,
        ]
        .into_iter()
        .flatten()
        .filter(|i| i.content_layer.as_deref().is_none_or(|l| l == "body"))
        .filter_map(|i| i.prov.first().map(|p| p.page_no))
        .collect();
        pages.sort_unstable();
        pages.dedup();
        pages
    }
}

/// `md` rebuilt so it splits on [`PAGE_BREAK`] into exactly `page_count` pages (see the module
/// docs). `item_pages` are [`DocJson::item_pages`]. Unchanged when the page count is unknown (0)
/// or already matched; Err when the provenance contradicts the segments.
fn pad_pages(md: &str, page_count: usize, item_pages: &[usize]) -> Result<String, String> {
    if page_count == 0 {
        return Ok(md.to_string());
    }
    let segments: Vec<&str> = md.split(PAGE_BREAK).collect();
    if segments.len() == page_count {
        return Ok(md.to_string());
    }
    if let Some(&bad) = item_pages.iter().find(|&&p| p == 0 || p > page_count) {
        return Err(format!(
            "docling places an item on page {bad}; the document has {page_count} pages"
        ));
    }
    let mut pages = vec![String::new(); page_count];
    if item_pages.is_empty() {
        // Nothing on any page (an all-scanned PDF): only empty markdown is consistent.
        if !md.trim().is_empty() {
            return Err(format!(
                "docling returned text but places no item on any of the {page_count} pages"
            ));
        }
    } else if segments.len() == item_pages.len() {
        for (seg, &page) in segments.iter().zip(item_pages) {
            pages[page - 1] = seg.trim_matches(|c| c == '\n' || c == '\r').to_string();
        }
    } else {
        return Err(format!(
            "docling markdown has {} page segments but its items sit on {} distinct pages \
             {item_pages:?} of {page_count}",
            segments.len(),
            item_pages.len()
        ));
    }
    Ok(pages.join(&format!("\n\n{PAGE_BREAK}\n\n")))
}

impl DoclingClient {
    pub fn new(base_url: &str) -> crate::Result<Self> {
        Ok(Self {
            http: http_client(TIMEOUT)?,
            base: base_url.trim().trim_end_matches('/').to_string(),
            max_response: MAX_RESPONSE_BYTES,
        })
    }

    /// The same client with another response cap (tests).
    pub fn with_max_response(mut self, bytes: u64) -> Self {
        self.max_response = bytes;
        self
    }

    /// `GET /health` answers 2xx within `timeout`.
    pub async fn health(&self, timeout: std::time::Duration) -> bool {
        self.http
            .get(format!("{}/health", self.base))
            .timeout(timeout)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    /// Converts one file to markdown. One attempt.
    pub async fn convert(
        &self,
        bytes: Bytes,
        filename: &str,
        opts: DoclingOpts,
    ) -> Result<DoclingResult, StepError> {
        let len = bytes.len() as u64;
        let file = Part::stream_with_length(reqwest::Body::from(bytes), len)
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")
            .map_err(|e| {
                step_err(
                    ErrorClass::InvalidRequest,
                    "convert_failed",
                    SERVICE,
                    e.to_string(),
                )
            })?;
        // `to_formats` is a list in docling's form model: one repeated field per value.
        let mut form = Form::new()
            .part("files", file)
            .text("to_formats", "md")
            .text("to_formats", "json")
            .text(
                "image_export_mode",
                if opts.embed_images {
                    "embedded"
                } else {
                    "placeholder"
                },
            )
            .text("md_page_break_placeholder", PAGE_BREAK)
            .text("abort_on_error", "false")
            .text("table_mode", "accurate")
            .text("do_ocr", opts.do_ocr.to_string())
            .text("force_ocr", opts.force_ocr.to_string())
            .text("ocr_engine", opts.ocr_engine);
        if let Some((first, last)) = opts.page_range {
            // A pair in docling's form model: one repeated field per bound.
            form = form
                .text("page_range", first.to_string())
                .text("page_range", last.to_string());
        }
        let resp = self
            .http
            .post(format!("{}/v1/convert/file", self.base))
            .multipart(form)
            .send()
            .await
            .map_err(|e| retryable(transport_message(e)))?;
        let status = resp.status();
        let body = read_capped(resp, self.max_response).await?;
        if !status.is_success() {
            let class = match classify(status.as_u16()) {
                ErrorClass::Retryable => ErrorClass::Retryable,
                // A 4xx means docling refused this input: retrying the same bytes cannot help.
                _ => ErrorClass::InvalidRequest,
            };
            let code = if class == ErrorClass::Retryable {
                "convert_unavailable"
            } else {
                "convert_failed"
            };
            return Err(step_err(
                class,
                code,
                SERVICE,
                format!("docling status {}: {}", status.as_u16(), snippet(&body)),
            ));
        }
        let parsed: ConvertResponse = serde_json::from_str(&body)
            .map_err(|e| retryable(format!("docling response is not the convert contract: {e}")))?;
        let errors: Vec<String> = parsed.errors.iter().map(error_line).collect();
        match parsed.status.as_str() {
            "success" | "partial_success" => {
                if !errors.is_empty() {
                    tracing::warn!(
                        filename,
                        status = %parsed.status,
                        errors = ?errors,
                        "docling converted with errors"
                    );
                }
                let (markdown, json) = parsed
                    .document
                    .map(|d| (d.md_content.unwrap_or_default(), d.json_content))
                    .unwrap_or_default();
                let json = json.unwrap_or_default();
                let partial = parsed.status == "partial_success";
                let (first_page, page_count, missing_pages) =
                    page_window(&json, partial).map_err(|m| {
                        step_err(ErrorClass::InvalidRequest, "convert_failed", SERVICE, m)
                    })?;
                if !missing_pages.is_empty() {
                    tracing::warn!(
                        filename,
                        missing = ?missing_pages,
                        "docling partial_success left pages out; they are kept as empty pages"
                    );
                }
                // Nothing to deliver; the same file converts to nothing again on a retry. A
                // paginated input (a scan) without text is pages to OCR, not nothing.
                if markdown.trim().is_empty() && page_count == 0 {
                    return Err(step_err(
                        ErrorClass::InvalidRequest,
                        "empty_conversion",
                        SERVICE,
                        format!(
                            "docling status {} produced no markdown{}",
                            parsed.status,
                            if errors.is_empty() {
                                String::new()
                            } else {
                                format!(": {}", snippet(&errors.join("; ")))
                            }
                        ),
                    ));
                }
                // Item pages relative to the window (page `first_page` → 1); one before the
                // window becomes 0, which pad_pages rejects as contradicting.
                let item_pages: Vec<usize> = json
                    .item_pages()
                    .into_iter()
                    .map(|p| (p + 1).saturating_sub(first_page))
                    .collect();
                let markdown = pad_pages(&markdown, page_count, &item_pages).map_err(|m| {
                    step_err(ErrorClass::InvalidRequest, "convert_failed", SERVICE, m)
                })?;
                Ok(DoclingResult {
                    markdown,
                    page_count,
                    status: parsed.status,
                    errors,
                    missing_pages,
                })
            }
            "failure" | "skipped" => Err(step_err(
                ErrorClass::InvalidRequest,
                "convert_failed",
                SERVICE,
                format!(
                    "docling status {}: {}",
                    parsed.status,
                    snippet(&errors.join("; "))
                ),
            )),
            // `pending` / `started` (or anything new) from the synchronous endpoint: not a result.
            other => Err(retryable(format!("docling returned status {other:?}"))),
        }
    }
}

/// The pages docling reported, as (first 1-based page number, count, missing); (1, 0, []) when
/// none. They must be contiguous (a `page_range` window, or the whole document), except after a
/// `partial_success` (`allow_gaps`): docling then leaves out the pages it failed to convert, the
/// window spans first..=last and `missing` lists the pages inside it it did not report.
fn page_window(json: &DocJson, allow_gaps: bool) -> Result<(usize, usize, Vec<usize>), String> {
    let mut nums = Vec::with_capacity(json.pages.len());
    for k in json.pages.keys() {
        nums.push(
            k.parse::<usize>()
                .map_err(|_| format!("docling reports a page {k:?}"))?,
        );
    }
    nums.sort_unstable();
    nums.dedup();
    let (Some(&first), Some(&last)) = (nums.first(), nums.last()) else {
        return Ok((1, 0, vec![]));
    };
    if first == 0 {
        return Err(format!("docling reports a page 0 in {nums:?}"));
    }
    let missing: Vec<usize> = (first..=last)
        .filter(|n| nums.binary_search(n).is_err())
        .collect();
    if !missing.is_empty() && !allow_gaps {
        return Err(format!("docling reports non-contiguous pages {nums:?}"));
    }
    Ok((first, last - first + 1, missing))
}

/// The response body, read as it streams in; more than `max` bytes → InvalidRequest
/// `conversion_too_large` (the same input converts as large on a retry).
async fn read_capped(mut resp: reqwest::Response, max: u64) -> Result<String, StepError> {
    if resp.content_length().is_some_and(|n| n > max) {
        return Err(too_large(max));
    }
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| retryable(transport_message(e)))?
    {
        if (buf.len() + chunk.len()) as u64 > max {
            return Err(too_large(max));
        }
        buf.extend_from_slice(&chunk);
    }
    String::from_utf8(buf).map_err(|e| retryable(format!("docling response is not UTF-8: {e}")))
}

fn too_large(max: u64) -> StepError {
    step_err(
        ErrorClass::InvalidRequest,
        "conversion_too_large",
        SERVICE,
        format!("docling's answer exceeds {max} bytes (embedded images of a large document)"),
    )
}

/// `{component_type, module_name, error_message}` → `module_name: error_message`.
fn error_line(v: &serde_json::Value) -> String {
    match (v.get("module_name"), v.get("error_message")) {
        (Some(m), Some(msg)) => format!(
            "{}: {}",
            m.as_str().unwrap_or_default(),
            msg.as_str().unwrap_or_default()
        ),
        _ => v.to_string(),
    }
}

fn retryable(message: String) -> StepError {
    step_err(
        ErrorClass::Retryable,
        "convert_unavailable",
        SERVICE,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(md: &str) -> Vec<String> {
        md.split(PAGE_BREAK).map(|p| p.trim().to_string()).collect()
    }

    fn join(segs: &[&str]) -> String {
        segs.join(&format!("\n\n{PAGE_BREAK}\n\n"))
    }

    #[test]
    fn scan_text_scan_text_places_each_segment_on_its_page() {
        // Measured on docling-serve-cpu v1.37.0: 4 pages, items on 2 and 4 → 2 segments.
        let md = pad_pages(&join(&["two", "four"]), 4, &[2, 4]).unwrap();
        assert_eq!(split(&md), ["", "two", "", "four"]);
    }

    #[test]
    fn consecutive_empty_middle_pages() {
        // text, scan, scan, text (measured: 2 segments, items on 1 and 4).
        let md = pad_pages(&join(&["one", "four"]), 4, &[1, 4]).unwrap();
        assert_eq!(split(&md), ["one", "", "", "four"]);
        let md = pad_pages(&join(&["one", "five"]), 6, &[1, 5]).unwrap();
        assert_eq!(split(&md), ["one", "", "", "", "five", ""]);
    }

    #[test]
    fn leading_and_trailing_empty_pages() {
        // scan, text, scan (measured: one segment, items on 2).
        assert_eq!(split(&pad_pages("two", 3, &[2]).unwrap()), ["", "two", ""]);
        assert_eq!(split(&pad_pages("a", 2, &[1]).unwrap()), ["a", ""]);
        // An item spanning pages 2-3 counts on page 2 (its first prov).
        assert_eq!(
            split(&pad_pages(&join(&["t2", "t4"]), 5, &[2, 4]).unwrap()),
            ["", "t2", "", "t4", ""]
        );
    }

    #[test]
    fn all_empty_pages() {
        assert_eq!(split(&pad_pages("", 3, &[]).unwrap()), ["", "", ""]);
        assert_eq!(split(&pad_pages("", 1, &[]).unwrap()), [""]);
        // scanned.pdf measured: empty markdown, one empty table on page 2.
        assert_eq!(split(&pad_pages("", 3, &[2]).unwrap()), ["", "", ""]);
    }

    #[test]
    fn matching_or_unknown_page_counts_leave_markdown_alone() {
        assert_eq!(pad_pages("x", 0, &[]).unwrap(), "x");
        let md = join(&["one", "", "three"]);
        assert_eq!(pad_pages(&md, 3, &[1, 2, 3]).unwrap(), md);
    }

    #[test]
    fn contradicting_provenance_fails() {
        // Two segments, three item pages.
        assert!(pad_pages(&join(&["a", "b"]), 4, &[1, 2, 4]).is_err());
        // An item past the last page.
        assert!(pad_pages("a", 3, &[4]).is_err());
        // Text with no item anywhere.
        assert!(pad_pages("a", 3, &[]).is_err());
    }

    #[test]
    fn page_window_is_the_contiguous_reported_range() {
        let j = |keys: &[&str]| DocJson {
            pages: keys
                .iter()
                .map(|k| (k.to_string(), serde_json::Value::Null))
                .collect(),
            ..DocJson::default()
        };
        assert_eq!(page_window(&j(&[]), false).unwrap(), (1, 0, vec![]));
        assert_eq!(
            page_window(&j(&["1", "2", "3"]), false).unwrap(),
            (1, 3, vec![])
        );
        // page_range=9..16 (measured on v1.37: keys are whole-document page numbers).
        assert_eq!(
            page_window(&j(&["10", "9", "11"]), false).unwrap(),
            (9, 3, vec![])
        );
        assert!(page_window(&j(&["2", "4"]), false).is_err());
        assert!(page_window(&j(&["0", "1"]), false).is_err());
        assert!(page_window(&j(&["0", "1"]), true).is_err());
        assert!(page_window(&j(&["x"]), true).is_err());
    }

    #[test]
    fn page_window_after_partial_success_spans_the_gaps() {
        let j = |keys: &[&str]| DocJson {
            pages: keys
                .iter()
                .map(|k| (k.to_string(), serde_json::Value::Null))
                .collect(),
            ..DocJson::default()
        };
        assert_eq!(page_window(&j(&["2", "4"]), true).unwrap(), (2, 3, vec![3]));
        assert_eq!(
            page_window(&j(&["1", "5", "2"]), true).unwrap(),
            (1, 5, vec![3, 4])
        );
        assert_eq!(page_window(&j(&["1", "2"]), true).unwrap(), (1, 2, vec![]));
    }

    #[test]
    fn item_pages_are_distinct_first_prov_pages_of_body_items() {
        let j: DocJson = serde_json::from_value(serde_json::json!({
            "pages": {"1": {}, "2": {}, "3": {}, "4": {}},
            "texts": [
                {"prov": [{"page_no": 1}], "content_layer": "furniture"},
                {"prov": [{"page_no": 4}], "content_layer": "body"},
                {"prov": [{"page_no": 2}, {"page_no": 3}]}
            ],
            "tables": [{"prov": [{"page_no": 2}]}],
            "pictures": [{"prov": []}]
        }))
        .unwrap();
        assert_eq!(j.item_pages(), [2, 4]);
    }
}
