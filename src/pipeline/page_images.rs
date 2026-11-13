//! Page images of a URL job (spec §6). webscraper downloads the page's images
//! (`download_images: true`); strata-ingest never fetches an image URL itself. [`inline_page_images`]
//! keeps the usable ones — a [`PAGE_IMAGE_MIMES`] type, `figure_min_bytes` ..=
//! [`MAX_PAGE_IMAGE_BYTES`] decoded, at most [`MAX_PAGE_IMAGES`] per page — and turns each into a
//! `data:` URI image in the page markdown: the first `![alt](src)` that references it gets the
//! data URI as its target; a kept image no tag references is appended under a final `## Images`
//! heading. Convert then takes those data URIs out exactly like docling's embedded figures
//! (`figures::extract_figures`: asset row, blob under `{tenant}/{job}/figures/{id}`, placeholder,
//! Caption step), so captions and the bundle's `![<caption>](asset:<id>)` inlining apply
//! unchanged. Images that are not kept leave their markdown untouched. The page original
//! (markdown + data URIs) is virus-scanned by storage_service when Acquire stores it, and never
//! grows past `max_bytes` (images that would not fit stay links).

use std::collections::{HashMap, HashSet};

use base64::Engine as _;

use super::figures::image_tags;
use crate::infra::webscraper::PageImage;
use crate::limits::{MAX_PAGE_IMAGE_BYTES, MAX_PAGE_IMAGES, PAGE_IMAGE_MIMES};

/// What [`inline_page_images`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inlined {
    pub markdown: String,
    /// Kept images whose markdown reference now carries their bytes.
    pub referenced: usize,
    /// Kept images no tag referenced, appended under `## Images`.
    pub appended: usize,
    /// Images not kept: no bytes, another type, too small or too large, past the per-page cap,
    /// or past the size budget.
    pub dropped: usize,
}

impl Inlined {
    /// The acquire step's report.
    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({
            "kept": self.referenced + self.appended,
            "referenced": self.referenced,
            "appended": self.appended,
            "dropped": self.dropped,
        })
    }
}

/// A kept image: its resolved URL, alt text, normalised mime and base64 payload.
struct Kept<'a> {
    key: String,
    alt: &'a str,
    mime: &'static str,
    data: &'a str,
}

impl Kept<'_> {
    fn uri_len(&self) -> u64 {
        ("data:".len() + self.mime.len() + ";base64,".len() + self.data.len()) as u64
    }

    fn push_uri(&self, out: &mut String) {
        out.push_str("data:");
        out.push_str(self.mime);
        out.push_str(";base64,");
        out.push_str(self.data);
    }
}

/// `mime` as one of [`PAGE_IMAGE_MIMES`]: lowercased, parameters stripped, `image/jpg` = jpeg.
fn page_mime(mime: Option<&str>) -> Option<&'static str> {
    let m = mime?.split(';').next()?.trim().to_ascii_lowercase();
    let m = if m == "image/jpg" {
        "image/jpeg".to_string()
    } else {
        m
    };
    PAGE_IMAGE_MIMES.iter().copied().find(|x| *x == m)
}

/// The URL part of a markdown image target: `<url>` or the first word (`url "title"`).
fn link_target(raw: &str) -> &str {
    let t = raw.trim();
    if let Some(rest) = t.strip_prefix('<') {
        return rest.split('>').next().unwrap_or(rest);
    }
    t.split_whitespace().next().unwrap_or(t)
}

/// `target` as an absolute URL string (resolved against `base` when relative), fragment kept.
fn resolve(base: Option<&url::Url>, target: &str) -> Option<String> {
    match url::Url::parse(target) {
        Ok(u) => Some(u.to_string()),
        Err(_) => base?.join(target).ok().map(|u| u.to_string()),
    }
}

fn is_data_uri(s: &str) -> bool {
    s.get(..5).is_some_and(|p| p.eq_ignore_ascii_case("data:"))
}

/// Alt text that fits `![…]`: no brackets, no line breaks or other control characters.
fn clean_alt(alt: &str) -> String {
    alt.chars()
        .map(|c| {
            if c == '[' || c == ']' || c.is_control() {
                ' '
            } else {
                c
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Inlines the page's usable images into `md` (module docs). `page_url` (the final URL) resolves
/// relative image links; `min_bytes` is `Limits::figure_min_bytes`; `max_bytes` bounds the
/// resulting markdown (`Limits::max_upload_bytes`). Images with a `data:` src are left to Convert
/// (it already takes data URIs out of the markdown).
pub fn inline_page_images(
    md: &str,
    page_url: &str,
    images: &[PageImage],
    min_bytes: u64,
    max_bytes: u64,
) -> Inlined {
    let base = url::Url::parse(page_url).ok();
    let mut kept: Vec<Kept<'_>> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut dropped = 0usize;
    let mut scratch: Vec<u8> = Vec::new();
    for img in images {
        let src = img.src.trim();
        if src.is_empty() || is_data_uri(src) {
            continue;
        }
        let Some(key) = resolve(base.as_ref(), src) else {
            continue;
        };
        if !seen.insert(key.clone()) {
            continue; // the same image twice: once is enough
        }
        let (Some(mime), Some(data)) = (
            page_mime(img.mime.as_deref()),
            img.content_base64.as_deref(),
        ) else {
            dropped += 1;
            continue;
        };
        let data = data.trim();
        // 4 base64 chars carry 3 bytes: refuse an oversized payload before decoding it.
        if (data.len() as u64 / 4).saturating_mul(3) > MAX_PAGE_IMAGE_BYTES + 3 {
            dropped += 1;
            continue;
        }
        scratch.clear();
        if base64::engine::general_purpose::STANDARD
            .decode_vec(data, &mut scratch)
            .is_err()
        {
            dropped += 1;
            continue;
        }
        let n = scratch.len() as u64;
        if n < min_bytes || n > MAX_PAGE_IMAGE_BYTES || kept.len() >= MAX_PAGE_IMAGES {
            dropped += 1;
            continue;
        }
        kept.push(Kept {
            key,
            alt: &img.alt,
            mime,
            data,
        });
    }
    if kept.is_empty() {
        return Inlined {
            markdown: md.to_string(),
            dropped,
            ..Inlined::default()
        };
    }

    let by_key: HashMap<&str, usize> = kept
        .iter()
        .enumerate()
        .map(|(i, k)| (k.key.as_str(), i))
        .collect();
    let mut used = vec![false; kept.len()];
    let mut size = md.len() as u64;
    let mut out = String::with_capacity(md.len());
    let mut at = 0;
    let mut referenced = 0usize;
    for tag in image_tags(md) {
        let target = link_target(&md[tag.target.clone()]);
        if is_data_uri(target) {
            continue;
        }
        let Some(key) = resolve(base.as_ref(), target) else {
            continue;
        };
        let Some(&i) = by_key.get(key.as_str()) else {
            continue;
        };
        if used[i] || size + kept[i].uri_len() > max_bytes {
            continue; // a second reference, or no room: the link stays as it is
        }
        used[i] = true;
        referenced += 1;
        size += kept[i].uri_len();
        out.push_str(&md[at..tag.target.start]);
        kept[i].push_uri(&mut out);
        at = tag.target.end;
    }
    out.push_str(&md[at..]);

    let mut appended = 0usize;
    for (i, k) in kept.iter().enumerate() {
        if used[i] {
            continue;
        }
        let alt = clean_alt(k.alt);
        // `\n## Images\n` once, then `\n![alt](uri)\n` per image.
        let line = (alt.len() + 6) as u64 + k.uri_len();
        let heading = if appended == 0 { 12 } else { 0 };
        if size + line + heading > max_bytes {
            dropped += 1;
            continue;
        }
        if appended == 0 {
            if !(out.is_empty() || out.ends_with('\n')) {
                out.push('\n');
            }
            out.push_str("\n## Images\n");
        }
        out.push_str("\n![");
        out.push_str(&alt);
        out.push_str("](");
        k.push_uri(&mut out);
        out.push_str(")\n");
        size += line + heading;
        appended += 1;
    }
    Inlined {
        markdown: out,
        referenced,
        appended,
        dropped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b64(n: usize, byte: u8) -> String {
        base64::engine::general_purpose::STANDARD.encode(vec![byte; n])
    }

    fn img(src: &str, mime: &str, data: Option<String>) -> PageImage {
        PageImage {
            src: src.to_string(),
            alt: "alt".to_string(),
            mime: Some(mime.to_string()),
            content_base64: data,
        }
    }

    #[test]
    fn kept_images_replace_their_reference_and_others_stay() {
        let md = "# P\n\n![logo](/img/logo.png)\n\n![doc](https://cdn.example.com/doc.pdf)\n\n\
                  ![big](big.jpg \"Big\")\n";
        let images = [
            img(
                "https://example.com/img/logo.png",
                "image/png",
                Some(b64(10, 1)),
            ),
            img(
                "https://cdn.example.com/doc.pdf",
                "application/pdf",
                Some(b64(100, 2)),
            ),
            img(
                "https://example.com/a/big.jpg",
                "image/jpeg; q=1",
                Some(b64(100, 3)),
            ),
        ];
        let x = inline_page_images(md, "https://example.com/a/", &images, 50, u64::MAX);
        assert_eq!((x.referenced, x.appended, x.dropped), (1, 0, 2));
        assert!(
            x.markdown.contains("![logo](/img/logo.png)"),
            "{}",
            x.markdown
        );
        assert!(
            x.markdown
                .contains("![doc](https://cdn.example.com/doc.pdf)")
        );
        assert!(
            x.markdown
                .contains(&format!("![big](data:image/jpeg;base64,{})", b64(100, 3))),
            "{}",
            x.markdown
        );
        assert!(!x.markdown.contains("## Images"));
    }

    #[test]
    fn unreferenced_images_are_appended_and_caps_hold() {
        let md = "text without images";
        let images: Vec<PageImage> = (0..MAX_PAGE_IMAGES + 3)
            .map(|i| {
                img(
                    &format!("https://example.com/{i}.png"),
                    "image/png",
                    Some(b64(64, i as u8)),
                )
            })
            .collect();
        let x = inline_page_images(md, "https://example.com/", &images, 1, u64::MAX);
        assert_eq!(
            (x.referenced, x.appended, x.dropped),
            (0, MAX_PAGE_IMAGES, 3)
        );
        assert!(
            x.markdown
                .starts_with("text without images\n\n## Images\n\n![alt](data:image/png;base64,")
        );
        assert_eq!(
            x.markdown.matches("](data:image/png;base64,").count(),
            MAX_PAGE_IMAGES
        );
        // A size budget below one more image keeps the markdown as it was.
        let x = inline_page_images(
            md,
            "https://example.com/",
            &images[..1],
            1,
            md.len() as u64 + 10,
        );
        assert_eq!((x.appended, x.dropped), (0, 1));
        assert_eq!(x.markdown, md);
        // Too large decoded: dropped.
        let huge = PageImage {
            content_base64: Some("A".repeat((MAX_PAGE_IMAGE_BYTES as usize / 3 + 2) * 4)),
            ..img("https://example.com/h.png", "image/png", None)
        };
        let x = inline_page_images(md, "https://example.com/", &[huge], 1, u64::MAX);
        assert_eq!((x.appended, x.dropped), (0, 1));
    }

    #[test]
    fn appended_images_become_figures() {
        let images = [img(
            "https://example.com/x.gif",
            "image/gif",
            Some(b64(40, 9)),
        )];
        let x = inline_page_images("p", "https://example.com/", &images, 1, u64::MAX);
        let f = super::super::figures::extract_figures(&x.markdown, 1, 10, |_| uuid::Uuid::nil());
        assert_eq!(f.figures.len(), 1);
        assert_eq!(f.figures[0].mime, "image/gif");
        assert_eq!(f.figures[0].decode(&x.markdown).unwrap(), vec![9u8; 40]);
    }
}
