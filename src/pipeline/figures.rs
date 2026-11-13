//! Figures embedded in docling's markdown (`image_export_mode=embedded`: `![alt](data:…)`).
//! [`extract_figures`] takes every data-URI image out of the markdown. A figure that is an
//! allowed image type, decodes, has at least `figure_min_bytes` bytes and fits under
//! `max_figures_per_job` becomes an asset and leaves the placeholder line
//! `<!-- figure:{asset_id} -->` (rulings R3). Anything else carried in a data URI (icons, other
//! mime types, garbage, figures past the cap) is removed. Image links that are not data URIs are
//! left as they are.

use std::ops::Range;

use base64::Engine as _;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Data-URI mime types kept as figures (what a vision provider accepts). Matched
/// case-insensitively, parameters stripped; `image/jpg` is read as `image/jpeg`.
pub const FIGURE_MIMES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/tiff",
];

/// A figure taken out of the markdown. `idx` counts every data-URI image in document order
/// (dropped ones included), so it is stable for the same docling output. The bytes are not held:
/// `data` is the base64 payload's span in the markdown given to [`extract_figures`]; decode them
/// one figure at a time with [`Figure::decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Figure {
    pub idx: usize,
    pub id: Uuid,
    pub mime: &'static str,
    pub data: Range<usize>,
}

impl Figure {
    /// The figure's bytes, decoded from `md` (the markdown [`extract_figures`] read). `None` only
    /// if `md` is not that markdown: the payload was validated during extraction.
    pub fn decode(&self, md: &str) -> Option<Vec<u8>> {
        let data = md.get(self.data.clone())?;
        base64::engine::general_purpose::STANDARD
            .decode(data.trim())
            .ok()
    }
}

/// Data-URI images [`extract_figures`] removed, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Dropped {
    /// Decoded to fewer than `min_bytes` bytes.
    pub small: usize,
    /// Not an allowed image mime, not base64, or not decodable.
    pub unsupported: usize,
    /// Past `max_figures`.
    pub over_cap: usize,
}

impl Dropped {
    pub fn total(&self) -> usize {
        self.small + self.unsupported + self.over_cap
    }
}

/// What [`extract_figures`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extracted {
    pub markdown: String,
    pub figures: Vec<Figure>,
    pub dropped: Dropped,
}

/// A markdown image tag `![alt](target)`: its byte span and the target's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageTag {
    pub start: usize,
    pub end: usize,
    pub target: Range<usize>,
}

/// Every `![alt](target)` in `md`, in order. Alt and target stay on one line; the target ends at
/// the first `)` (base64 has none).
///
/// Linear in `md.len()`: every search starts after the end of the previous one. When a tag at
/// `start` fails at position `p` (the first `]`/newline of its alt, or the first `)`/newline of
/// its target), every `![` between `start` and `p` would stop at the same `p` and fail the same
/// way, so the scan resumes after `p` (hostile markdown such as `![a](![a](![a](…` cannot make it
/// quadratic).
pub fn image_tags(md: &str) -> Vec<ImageTag> {
    scan_tags(md, &mut 0)
}

/// [`image_tags`], adding to `examined` every byte its searches look at (a byte looked at twice
/// counts twice): the linear-time bound is asserted on this count, not on a wall clock.
fn scan_tags(md: &str, examined: &mut usize) -> Vec<ImageTag> {
    // A search from `from` that stopped at `found` (or ran to the end) looked at these bytes.
    let mut look = |from: usize, found: Option<usize>, needle: usize| {
        *examined += found.map_or(md.len() - from, |f| f - from + needle);
    };
    let mut tags = Vec::new();
    let mut i = 0;
    loop {
        let found = md[i..].find("![").map(|o| i + o);
        look(i, found, 2);
        let Some(start) = found else { break };
        let alt = start + 2;
        let close = md[alt..].find(['\n', ']']).map(|o| alt + o);
        look(alt, close, 1);
        let Some(close) = close else {
            break; // no `]` left: no later tag can close either
        };
        if !md[close..].starts_with("](") {
            i = close + 1;
            continue;
        }
        let t0 = close + 2;
        let t1 = md[t0..].find(['\n', ')']).map(|o| t0 + o);
        look(t0, t1, 1);
        let Some(t1) = t1 else {
            break; // no `)` left
        };
        i = t1 + 1;
        if md[t1..].starts_with(')') {
            tags.push(ImageTag {
                start,
                end: t1 + 1,
                target: t0..t1,
            });
        }
    }
    tags
}

/// The placeholder line a kept figure leaves in the markdown (rulings R3).
pub fn placeholder(id: Uuid) -> String {
    format!("<!-- figure:{id} -->")
}

/// A figure's asset id: a UUIDv8 over sha256(job id, `idx`). A retried Convert derives the same
/// ids, so its asset rows and blob keys (`…/figures/{id}`) are rewritten in place.
pub fn figure_id(job: Uuid, idx: usize) -> Uuid {
    let mut h = Sha256::new();
    h.update(job.as_bytes());
    h.update(b"figure");
    h.update((idx as u64).to_be_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::new_v8(bytes)
}

/// Takes the data-URI images out of `md` (see the module doc). A kept figure is replaced by its
/// [`placeholder`] on a line of its own, with the id `id_for(idx)`; every other data-URI image
/// is removed. At most `max_figures` are kept. Payloads are validated by decoding into one
/// reused buffer; no figure's bytes are kept (see [`Figure::decode`]).
pub fn extract_figures(
    md: &str,
    min_bytes: u64,
    max_figures: usize,
    mut id_for: impl FnMut(usize) -> Uuid,
) -> Extracted {
    let mut out = String::with_capacity(md.len().min(1 << 20));
    let mut figures = Vec::new();
    let mut dropped = Dropped::default();
    let mut scratch = Vec::new();
    let mut at = 0;
    let mut idx = 0;
    for tag in image_tags(md) {
        let target = &md[tag.target.clone()];
        let Some(uri) = parse_data_uri(target) else {
            continue; // not a data URI: the link stays
        };
        out.push_str(&md[at..tag.start]);
        at = tag.end;
        let this = idx;
        idx += 1;
        let Some((mime, data)) = uri else {
            dropped.unsupported += 1;
            continue;
        };
        scratch.clear();
        if base64::engine::general_purpose::STANDARD
            .decode_vec(data.trim(), &mut scratch)
            .is_err()
        {
            dropped.unsupported += 1;
            continue;
        }
        if (scratch.len() as u64) < min_bytes {
            dropped.small += 1;
            continue;
        }
        if figures.len() >= max_figures {
            dropped.over_cap += 1;
            continue;
        }
        let id = id_for(this);
        if !(out.is_empty() || out.ends_with('\n')) {
            out.push('\n');
        }
        out.push_str(&placeholder(id));
        if !(at == md.len() || md[at..].starts_with('\n')) {
            out.push('\n');
        }
        // `data` is a subslice of `md`: its offset is its span in `md`.
        let offset = data.as_ptr() as usize - md.as_ptr() as usize;
        figures.push(Figure {
            idx: this,
            id,
            mime,
            data: offset..offset + data.len(),
        });
    }
    out.push_str(&md[at..]);
    Extracted {
        markdown: out,
        figures,
        dropped,
    }
}

/// A `data:` target. `None`: not a data URI (a link, left alone). `Some(None)`: a data URI that
/// is not a base64 image of an allowed type (removed). `Some(Some((mime, payload)))`: a candidate
/// figure, mime normalised to an entry of [`FIGURE_MIMES`].
fn parse_data_uri(target: &str) -> Option<Option<(&'static str, &str)>> {
    let t = target.trim();
    if !t.get(..5).is_some_and(|p| p.eq_ignore_ascii_case("data:")) {
        return None;
    }
    let Some((head, data)) = t[5..].split_once(',') else {
        return Some(None);
    };
    let mut params = head.split(';');
    let mime = params.next().unwrap_or("").trim().to_ascii_lowercase();
    if !params.any(|p| p.trim().eq_ignore_ascii_case("base64")) {
        return Some(None);
    }
    let mime = if mime == "image/jpg" {
        "image/jpeg".to_string()
    } else {
        mime
    };
    Some(
        FIGURE_MIMES
            .iter()
            .find(|m| **m == mime)
            .map(|m| (*m, data)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_found_and_malformed_ones_skipped() {
        let md = "![a](x) ![b\n](y) ![c] (z) ![](data:,)";
        let tags: Vec<&str> = image_tags(md).iter().map(|t| &md[t.start..t.end]).collect();
        assert_eq!(tags, ["![a](x)", "![](data:,)"]);
        // A failed tag does not hide a later valid one.
        let md = "![a](b\n![c] ![d](e)";
        let tags: Vec<&str> = image_tags(md).iter().map(|t| &md[t.start..t.end]).collect();
        assert_eq!(tags, ["![d](e)"]);
    }

    #[test]
    fn hostile_nested_tags_scan_in_linear_time() {
        // One 32 KiB line of unterminated tags, then a newline and a real tag. The bound is on
        // bytes examined, not wall time (load-independent): linear scanning looks at each byte
        // a small constant number of times; rescanning from `start + 2` after a failed tag
        // looks at ~len²/10 bytes (verified by mutation: 6.9e9 bytes for 256 KiB). The input is
        // small so a quadratic regression fails in seconds instead of hanging.
        let mut md = "![a](".repeat((32 << 10) / 5);
        md.push_str("\n![ok](x)");
        let mut examined = 0;
        let tags = scan_tags(&md, &mut examined);
        assert_eq!(tags.len(), 1);
        assert_eq!(&md[tags[0].start..tags[0].end], "![ok](x)");
        assert!(
            examined <= 3 * md.len(),
            "examined {examined} of {}",
            md.len()
        );
        // Also unterminated alts: `![![![![…` with no `]` at all.
        let alts = "![".repeat(16 << 10);
        let mut examined = 0;
        assert!(scan_tags(&alts, &mut examined).is_empty());
        assert!(
            examined <= 3 * alts.len(),
            "examined {examined} of {}",
            alts.len()
        );
        let x = extract_figures(&md, 1, 10, |_| Uuid::nil());
        assert_eq!(x.markdown, md);
    }

    #[test]
    fn mime_allow_list_and_normalisation() {
        let enc = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
        let big = enc(&[9u8; 32]);
        let md = format!(
            "![](data:image/JPG;name=x.jpg;base64,{big})\n\
             ![](data:text/html;base64,{big})\n\
             ![](data:image/svg+xml;base64,{big})\n\
             ![](data:image/png,{big})\n\
             ![](data:image/webp;base64,!!!)\n\
             ![](DATA:image/gif;base64,{big})\n"
        );
        let x = extract_figures(&md, 16, 10, |i| Uuid::from_u128(i as u128));
        let mimes: Vec<_> = x.figures.iter().map(|f| (f.idx, f.mime)).collect();
        assert_eq!(mimes, [(0, "image/jpeg"), (5, "image/gif")]);
        assert_eq!(
            x.dropped,
            Dropped {
                small: 0,
                unsupported: 4,
                over_cap: 0
            }
        );
        assert!(!x.markdown.contains("data:"), "{}", x.markdown);
        assert!(!x.markdown.contains("DATA:"), "{}", x.markdown);
    }

    #[test]
    fn figures_past_the_cap_are_dropped() {
        let big = base64::engine::general_purpose::STANDARD.encode([3u8; 32]);
        let small = base64::engine::general_purpose::STANDARD.encode([3u8; 4]);
        let md = format!(
            "![](data:image/png;base64,{small})\n{}",
            format!("![](data:image/png;base64,{big})\n").repeat(5)
        );
        let x = extract_figures(&md, 16, 3, |i| Uuid::from_u128(i as u128));
        let idx: Vec<usize> = x.figures.iter().map(|f| f.idx).collect();
        assert_eq!(idx, [1, 2, 3]);
        assert_eq!(
            x.dropped,
            Dropped {
                small: 1,
                unsupported: 0,
                over_cap: 2
            }
        );
        assert_eq!(x.markdown.matches("<!-- figure:").count(), 3);
        assert!(!x.markdown.contains("data:"));
        for f in &x.figures {
            assert_eq!(f.decode(&md).unwrap(), [3u8; 32]);
        }
    }

    #[test]
    fn figure_ids_are_deterministic_per_job_and_index() {
        let job = Uuid::now_v7();
        assert_eq!(figure_id(job, 3), figure_id(job, 3));
        assert_ne!(figure_id(job, 3), figure_id(job, 4));
        assert_ne!(figure_id(job, 3), figure_id(Uuid::now_v7(), 3));
        assert_eq!(figure_id(job, 0).get_version_num(), 8);
    }

    #[test]
    fn placeholder_gets_its_own_line_only_when_needed() {
        let big = base64::engine::general_purpose::STANDARD.encode([1u8; 16]);
        let id = Uuid::nil();
        let md = format!("![x](data:image/png;base64,{big})\nnext");
        let x = extract_figures(&md, 16, 10, |_| id);
        assert_eq!(x.markdown, format!("{}\nnext", placeholder(id)));
        assert_eq!(x.figures[0].mime, "image/png");
        assert_eq!(x.figures[0].decode(&md).unwrap(), [1u8; 16]);
    }
}
