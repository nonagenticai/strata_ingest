//! Page triage over docling's markdown: split into pages, find the low-text ones, and choose
//! which pages go to OCR and how they are batched into `OcrPages` steps.

use serde::Deserialize;

use super::figures::image_tags;
use crate::infra::docling::PAGE_BREAK;

/// Most pages one provider `OcrPages` step carries.
pub const OCR_PAGES_PER_STEP: usize = 8;

/// Docling's markdown split on its page-break placeholder. The blank lines docling puts around
/// a break are not page text; no break = one page.
pub fn split_pages(md: &str) -> Vec<String> {
    md.split(PAGE_BREAK)
        .map(|p| p.trim_matches(|c| c == '\n' || c == '\r').to_string())
        .collect()
}

/// Non-whitespace characters of `page`, not counting `![..](..)` image tags.
pub fn text_chars(page: &str) -> usize {
    let mut n = 0;
    let mut at = 0;
    for tag in image_tags(page) {
        n += count(&page[at..tag.start]);
        at = tag.end;
    }
    n + count(&page[at..])
}

fn count(s: &str) -> usize {
    s.chars().filter(|c| !c.is_whitespace()).count()
}

/// 0-based indices of the pages with fewer than `min_chars` text characters (see [`text_chars`]).
pub fn low_text_pages(pages: &[String], min_chars: usize) -> Vec<u32> {
    pages
        .iter()
        .enumerate()
        .filter(|(_, p)| text_chars(p) < min_chars)
        .map(|(i, _)| i as u32)
        .collect()
}

/// The job's `options.ocr`. Missing = `auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OcrMode {
    Off,
    #[default]
    Auto,
    Force,
}

impl OcrMode {
    pub fn as_str(self) -> &'static str {
        match self {
            OcrMode::Off => "off",
            OcrMode::Auto => "auto",
            OcrMode::Force => "force",
        }
    }

    /// Pages to OCR: `off` none, `force` all, `auto` the low-text ones.
    pub fn select(self, page_count: usize, low_text: &[u32]) -> Vec<u32> {
        match self {
            OcrMode::Off => vec![],
            OcrMode::Force => (0..page_count as u32).collect(),
            OcrMode::Auto => low_text.to_vec(),
        }
    }
}

/// Batches sorted, distinct `pages` into runs of consecutive pages, each cut into chunks of at
/// most `max`. Every batch is contiguous, so its key `p{first}-{last}` names exactly its pages.
pub fn ocr_batches(pages: &[u32], max: usize) -> Vec<Vec<u32>> {
    let max = max.max(1);
    let mut batches: Vec<Vec<u32>> = Vec::new();
    for &p in pages {
        match batches.last_mut() {
            Some(b) if b.len() < max && b.last().is_some_and(|&l| l + 1 == p) => b.push(p),
            _ => batches.push(vec![p]),
        }
    }
    batches
}

/// The step key of a contiguous batch: `p{first}-{last}`, 0-based.
pub fn batch_key(batch: &[u32]) -> String {
    let first = batch.first().copied().unwrap_or(0);
    let last = batch.last().copied().unwrap_or(first);
    format!("p{first}-{last}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_are_contiguous_and_capped() {
        let pages: Vec<u32> = [0, 1, 2, 5].into_iter().chain(7..25).collect();
        let b = ocr_batches(&pages, 8);
        let keys: Vec<String> = b.iter().map(|b| batch_key(b)).collect();
        assert_eq!(keys, ["p0-2", "p5-5", "p7-14", "p15-22", "p23-24"]);
        assert!(ocr_batches(&[], 8).is_empty());
    }

    #[test]
    fn ocr_mode_selects() {
        assert_eq!(OcrMode::Off.select(3, &[1]), Vec::<u32>::new());
        assert_eq!(OcrMode::Auto.select(3, &[1]), vec![1]);
        assert_eq!(OcrMode::Force.select(3, &[1]), vec![0, 1, 2]);
    }
}
