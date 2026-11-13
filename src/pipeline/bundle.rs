//! The markdown bundle delivered to strata.
//!
//! - Documents/images: page *i* (0-based) becomes `<!-- page:{i+1} -->\n` + its text, pages joined
//!   by a blank line. The text is the page's OCR markdown when OCR produced any (provider image
//!   refs such as `![img-0.jpeg](img-0.jpeg)` dropped), followed by that page's figure placeholder
//!   lines so their captions survive (rulings R17); otherwise Convert's markdown as it is (tables
//!   stay docling's markdown tables).
//! - Audio/video: no page anchors; one `## {start}–{end}` section per ~60 s of transcript segments.
//! - Figure placeholders (`<!-- figure:{uuid} -->`, rulings R3) become `![{caption}](asset:{uuid})`
//!   (`![](asset:{uuid})` without a caption). A placeholder whose id is not in `captions` (no
//!   asset row: an orphan) is removed, its line too when nothing else is on it.
//! - Rulings R19: the anchors, section headers and `asset:` links are Bundle's alone. In the
//!   document's own text (pages, OCR, transcript, captions, title) every `<!--` / `<img` becomes
//!   `&lt;…` (no forged `<!-- page:N -->`) and every image link whose target mentions `asset:` is
//!   dropped. That pass runs over each whole line with the job's placeholders held as sentinels,
//!   so removing an orphan cannot join a forgery up; the sentinels become links afterwards.
//! - `# {title}` leads only when the body has no top-level (`# `) heading outside code fences.

use std::collections::{BTreeMap, HashMap, HashSet};

use uuid::Uuid;

use super::figures::image_tags;
pub use crate::providers::transcribe::Segment;

/// Transcript section length: a section takes segments starting within this many seconds of its
/// first segment's start.
pub const TRANSCRIPT_WINDOW_S: f64 = 60.0;
/// `source` sent to strata is cut to this many chars.
pub const MAX_SOURCE_CHARS: usize = 1024;

const MARK: &str = "<!-- figure:";
const MARK_END: &str = " -->";
const UUID_LEN: usize = 36;

/// What [`build_bundle`] assembles. `captions` holds every figure that has an asset row (the value
/// is its caption, `None` without one); `transcript` set = an audio/video job (`pages`/`ocr` are
/// then ignored).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BundleInput {
    pub title: Option<String>,
    pub pages: Vec<String>,
    pub ocr: BTreeMap<u32, String>,
    pub captions: HashMap<Uuid, Option<String>>,
    pub transcript: Option<Vec<Segment>>,
}

/// One unit of the body: Bundle's own header (a page anchor or a transcript section header) and
/// the document text under it, placeholders still unresolved.
struct Unit {
    header: String,
    text: String,
}

/// The bundle's markdown (see the module docs).
pub fn build_bundle(input: &BundleInput) -> String {
    let body = units(input)
        .into_iter()
        .map(|u| {
            let text = render(&u.text, &input.captions);
            if input.transcript.is_some() {
                format!("{}\n\n{text}", u.header)
            } else {
                format!("{}\n{text}", u.header)
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    match input.title.as_deref().map(|t| sanitise(&single_line(t))) {
        Some(title) if !title.is_empty() && !has_top_level_heading(&body) => {
            format!("# {title}\n\n{body}")
        }
        _ => body,
    }
}

/// Whether the bundle carries any content besides Bundle's anchors, section headers and title:
/// some non-whitespace document text or a figure link. A bundle without is not delivered.
pub fn has_content(input: &BundleInput) -> bool {
    units(input)
        .iter()
        .any(|u| !render(&u.text, &input.captions).trim().is_empty())
}

/// The figure ids the bundle references (placeholders actually present after OCR overrides and
/// R17), first occurrence order, each once. The Bundle step looks up exactly these captions.
pub fn figure_refs(input: &BundleInput) -> Vec<Uuid> {
    let mut seen = HashSet::new();
    units(input)
        .iter()
        .flat_map(|u| placeholders(&u.text))
        .map(|(_, id)| id)
        .filter(|id| seen.insert(*id))
        .collect()
}

/// `s` cut to at most `max` chars, on a char boundary.
pub fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((at, _)) => &s[..at],
        None => s,
    }
}

/// Whitespace runs (newlines included) collapsed to one space, ends trimmed.
fn single_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The body's units: a transcript's sections, else one per page.
fn units(input: &BundleInput) -> Vec<Unit> {
    if let Some(segments) = &input.transcript {
        return transcript_sections(segments);
    }
    input
        .pages
        .iter()
        .enumerate()
        .map(|(i, page)| {
            let text = match input.ocr.get(&(i as u32)).map(|o| strip_image_refs(o)) {
                Some(ocr) if !ocr.trim().is_empty() => with_page_figures(&ocr, page),
                _ => page.clone(),
            };
            Unit {
                header: format!("<!-- page:{} -->", i + 1),
                text,
            }
        })
        .collect()
}

/// OCR text replacing `page`, followed by `page`'s figure placeholders, one per paragraph (R17).
fn with_page_figures(ocr: &str, page: &str) -> String {
    let ocr = ocr.trim_matches(|c| c == '\n' || c == '\r');
    let mut out = ocr.to_string();
    for (_, id) in placeholders(page) {
        out.push_str("\n\n");
        out.push_str(&super::figures::placeholder(id));
    }
    out
}

/// OCR markdown without image tags: the provider's are refs to images it did not return (e.g.
/// `![img-0.jpeg](img-0.jpeg)`). A line that held only tags goes too.
fn strip_image_refs(md: &str) -> String {
    let mut lines = Vec::new();
    for line in md.split('\n') {
        let tags = image_tags(line);
        if tags.is_empty() {
            lines.push(line.to_string());
            continue;
        }
        let mut kept = String::with_capacity(line.len());
        let mut at = 0;
        for t in &tags {
            kept.push_str(&line[at..t.start]);
            at = t.end;
        }
        kept.push_str(&line[at..]);
        if !kept.trim().is_empty() {
            lines.push(kept);
        }
    }
    lines.join("\n")
}

/// Every well-formed placeholder in `text`: its byte span and id, in order.
fn placeholders(text: &str) -> Vec<(std::ops::Range<usize>, Uuid)> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(off) = text[i..].find(MARK) {
        let start = i + off;
        let id_at = start + MARK.len();
        i = id_at;
        let Some(id) = text
            .get(id_at..id_at + UUID_LEN)
            .and_then(|s| Uuid::parse_str(s).ok())
        else {
            continue;
        };
        let end_at = id_at + UUID_LEN;
        if text[end_at..].starts_with(MARK_END) {
            let end = end_at + MARK_END.len();
            out.push((start..end, id));
            i = end;
        }
    }
    out
}

/// Stands in for a link Bundle generates while the document text around it is sanitised. NUL is
/// removed from document text first, so the text cannot contain or forge a sentinel.
const SENTINEL: char = '\u{0}';

/// Document text → bundle text (R19). Per line: the job's placeholders become sentinels (an
/// orphan is removed); the whole line is then sanitised ([`sanitise`]) in one pass, so text that
/// only joins up once a placeholder is gone (`<!<!-- figure:… -->-- page:5 -->`) is caught; only
/// then do the sentinels become Bundle's image links. A line that only removals emptied goes too.
fn render(text: &str, captions: &HashMap<Uuid, Option<String>>) -> String {
    let mut lines = Vec::new();
    for line in text.split('\n') {
        let mut joined = String::with_capacity(line.len());
        let mut links = Vec::new();
        let mut at = 0;
        for (span, id) in placeholders(line) {
            joined.push_str(&line[at..span.start].replace(SENTINEL, ""));
            if let Some(caption) = captions.get(&id) {
                let alt = caption.as_deref().map(alt_text).unwrap_or_default();
                links.push(format!("![{alt}](asset:{id})"));
                joined.push(SENTINEL);
            }
            at = span.end;
        }
        joined.push_str(&line[at..].replace(SENTINEL, ""));
        let clean = sanitise(&joined);
        let mut out = String::with_capacity(clean.len());
        let mut links = links.into_iter();
        for part in clean.split(SENTINEL).enumerate() {
            if part.0 > 0 {
                out.push_str(&links.next().unwrap_or_default());
            }
            out.push_str(part.1);
        }
        if !out.trim().is_empty() || line.trim().is_empty() {
            lines.push(out);
        }
    }
    lines.join("\n")
}

/// Document text with no `asset:` image links and no HTML openers that matter (`<!--` and `<img`
/// → `&lt;…`): neither a page anchor nor a figure can be forged by the document. Image tags whose
/// target mentions `asset:` are dropped until none is left (a removal can join a new one up).
fn sanitise(text: &str) -> String {
    let mut text = text.to_string();
    loop {
        let forged: Vec<_> = image_tags(&text)
            .into_iter()
            .filter(|t| {
                text[t.target.clone()]
                    .to_ascii_lowercase()
                    .contains("asset:")
            })
            .collect();
        if forged.is_empty() {
            break;
        }
        let mut kept = String::with_capacity(text.len());
        let mut at = 0;
        for t in &forged {
            kept.push_str(&text[at..t.start]);
            at = t.end;
        }
        kept.push_str(&text[at..]);
        text = kept;
    }
    neutralise(&text)
}

/// `<!--` and `<img` (any case) → `&lt;!--` / `&lt;img`.
fn neutralise(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for (i, c) in text.char_indices() {
        let rest = &text[i + c.len_utf8()..];
        let opener =
            rest.starts_with("!--") || rest.get(..3).is_some_and(|r| r.eq_ignore_ascii_case("img"));
        if c == '<' && opener {
            out.push_str("&lt;");
        } else {
            out.push(c);
        }
    }
    out
}

/// A caption as image alt text: one line, no `<!--` / `<img`, `\`, `[` and `]` escaped.
fn alt_text(caption: &str) -> String {
    neutralise(&single_line(caption))
        .replace('\\', "\\\\")
        .replace('[', "\\[")
        .replace(']', "\\]")
}

/// A line opening an ATX top-level heading (`#` alone or `# …`, up to 3 spaces of indent),
/// outside ``` / ~~~ code fences.
fn has_top_level_heading(md: &str) -> bool {
    let mut fence: Option<char> = None;
    for line in md.lines() {
        let indent = line.len() - line.trim_start_matches(' ').len();
        if indent > 3 {
            continue;
        }
        let l = &line[indent..];
        let opens = ['`', '~']
            .into_iter()
            .find(|&c| l.starts_with(&String::from(c).repeat(3)));
        match (fence, opens) {
            (None, Some(c)) => fence = Some(c),
            (Some(f), Some(c)) if f == c => fence = None,
            (Some(_), _) => {}
            (None, None) => {
                if l == "#" || l.starts_with("# ") || l.starts_with("#\t") {
                    return true;
                }
            }
        }
    }
    false
}

/// `## {start}–{end}` + the segments' text, per window of [`TRANSCRIPT_WINDOW_S`]; a window whose
/// segments carry no text is left out.
fn transcript_sections(segments: &[Segment]) -> Vec<Unit> {
    let mut sections = Vec::new();
    let mut i = 0;
    while i < segments.len() {
        let start = segments[i].start_s;
        let mut end = segments[i].end_s;
        let mut texts = Vec::new();
        let mut j = i;
        while j < segments.len() && (j == i || segments[j].start_s < start + TRANSCRIPT_WINDOW_S) {
            let text = single_line(&segments[j].text);
            if !text.is_empty() {
                texts.push(text);
            }
            if segments[j].end_s > end {
                end = segments[j].end_s;
            }
            j += 1;
        }
        if !texts.is_empty() {
            sections.push(Unit {
                header: format!("## {}–{}", timestamp(start), timestamp(end)),
                text: texts.join(" "),
            });
        }
        i = j;
    }
    sections
}

/// `mm:ss`, or `hh:mm:ss` from one hour; whole seconds (floor), negative/NaN as 0.
fn timestamp(s: f64) -> String {
    let t = if s.is_finite() && s > 0.0 {
        s.floor() as u64
    } else {
        0
    };
    let (h, m, sec) = (t / 3600, (t / 60) % 60, t % 60);
    if h > 0 {
        format!("{h:02}:{m:02}:{sec:02}")
    } else {
        format!("{m:02}:{sec:02}")
    }
}
