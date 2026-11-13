//! Content detection by bytes, never by extension (the extension only separates
//! `Text` from `Markdown`).
//!
//! Two-phase use (ruling R4): the API sniffs only the first 64 KiB of an upload. A zip
//! container cannot be classified from a head alone (its central directory sits at the
//! END of the file), so the API checks [`is_zip`] first and defers those to the worker's
//! Detect step, which downloads the whole original to a scratch file and calls
//! [`sniff_zip_file`] (rulings R27). `sniff` on a truncated zip yields
//! `UnsupportedType("application/zip")`.

use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::IngestError;

/// Serialises adjacently tagged, e.g. `{"type":"pdf"}` / `{"type":"image","mime":"image/png"}`
/// (the Detect step hands it to Convert in the step input).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "mime", rename_all = "snake_case")]
pub enum Detected {
    Text,
    Markdown,
    Html,
    Pdf,
    Docx,
    Pptx,
    Xlsx,
    Epub,
    Odt,
    /// Carries the mime type.
    Image(String),
    Audio(String),
    Video(String),
}

impl Detected {
    /// The content's mime type (recorded as the job's `meta.mime`).
    pub fn mime(&self) -> &str {
        match self {
            Detected::Text => "text/plain",
            Detected::Markdown => "text/markdown",
            Detected::Html => "text/html",
            Detected::Pdf => "application/pdf",
            Detected::Docx => {
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }
            Detected::Pptx => {
                "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            }
            Detected::Xlsx => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Detected::Epub => "application/epub+zip",
            Detected::Odt => "application/vnd.oasis.opendocument.text",
            Detected::Image(m) | Detected::Audio(m) | Detected::Video(m) => m,
        }
    }
}

const SUPPORTED_IMAGES: &[&str] = &[
    "image/png",
    "image/jpeg",
    "image/tiff",
    "image/webp",
    "image/bmp",
];
const SUPPORTED_AUDIO: &[&str] = &[
    "audio/mpeg",
    "audio/x-wav",
    "audio/x-flac",
    "audio/ogg",
    "audio/m4a",
    "audio/aac",
];
const SUPPORTED_VIDEO: &[&str] = &[
    "video/mp4",
    "video/webm",
    "video/quicktime",
    "video/x-matroska",
];

fn unsupported(mime: &str) -> IngestError {
    IngestError::UnsupportedType(mime.to_string())
}

/// True when the bytes start with a zip local-file-header (or empty-archive) signature.
/// The API uses this to defer zip classification to the worker, which sees the full body.
pub fn is_zip(head: &[u8]) -> bool {
    head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06")
}

/// Bytes of the head considered when deciding "is this text" (API and worker agree).
const TEXT_WINDOW: usize = 64 * 1024;

/// API entry point: classify the first 64 KiB of an upload. `Ok(None)` means the head is a
/// zip container (including plain zips): the central directory is at the END of the file,
/// so the decision is deferred to the worker's [`sniff`] on the full body. Non-zip heads
/// are classified exactly as [`sniff`] would (`Err` = reject with 422 now).
pub fn sniff_head(head: &[u8], filename: Option<&str>) -> Result<Option<Detected>, IngestError> {
    if is_zip(head) {
        return Ok(None);
    }
    sniff(head, filename).map(Some)
}

/// Worker entry point: classify a COMPLETE file body. Zip magic is resolved with
/// [`sniff_zip`]. Do NOT call this on a truncated zip head (it would return
/// `UnsupportedType("application/zip")`); use [`sniff_head`] for heads. Text decoding only
/// ever looks at the first 64 KiB, so cost and verdict do not depend on file size.
pub fn sniff(head: &[u8], filename: Option<&str>) -> Result<Detected, IngestError> {
    if head.is_empty() {
        return Err(unsupported("application/x-empty"));
    }
    if is_zip(head) {
        return sniff_zip(head);
    }
    // Text view (bounded). A text head beats weak magics such as "BM" or "MZ".
    let text = decode_text(&head[..head.len().min(TEXT_WINDOW)]);
    // `%PDF-` must open the file (after an optional BOM / whitespace), not merely appear.
    let lead = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let lead = &lead[lead.iter().take_while(|b| b.is_ascii_whitespace()).count()..];
    if lead.starts_with(b"%PDF-") {
        return Ok(Detected::Pdf);
    }
    // infer also matches html/xml/shell text; those are handled by the text path below.
    let magic = infer::get(head)
        .filter(|t| !t.mime_type().starts_with("text/"))
        .filter(|_| text.is_none())
        .filter(|t| t.mime_type() != "image/bmp" || plausible_bmp(head));
    if let Some(t) = magic {
        let mime = t.mime_type();
        let pick = |list: &[&str]| list.contains(&mime);
        return if pick(SUPPORTED_IMAGES) {
            Ok(Detected::Image(mime.into()))
        } else if pick(SUPPORTED_AUDIO) {
            Ok(Detected::Audio(mime.into()))
        } else if pick(SUPPORTED_VIDEO) {
            Ok(Detected::Video(mime.into()))
        } else {
            Err(unsupported(mime))
        };
    }
    let Some(text) = text else {
        return Err(unsupported("application/octet-stream"));
    };
    let lower: String = text
        .trim_start()
        .chars()
        .take(64)
        .collect::<String>()
        .to_ascii_lowercase();
    if lower.starts_with("<!doctype html") || lower.starts_with("<html") {
        return Ok(Detected::Html);
    }
    let ext = filename
        .and_then(|f| f.rsplit_once('.'))
        .map(|(_, e)| e.to_ascii_lowercase());
    Ok(match ext.as_deref() {
        Some("md" | "markdown") => Detected::Markdown,
        _ => Detected::Text,
    })
}

/// BMP's magic is two bytes ("BM"); also require a known DIB header size at offset 14.
fn plausible_bmp(head: &[u8]) -> bool {
    head.get(14..18).is_some_and(|b| {
        matches!(
            u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            12 | 40 | 52 | 56 | 64 | 108 | 124
        )
    })
}

/// Decode `head` as text: UTF-8 (optional BOM) or UTF-16 with BOM; no NULs. A multi-byte
/// sequence cut by the head boundary is tolerated. `None` = not text.
pub(crate) fn decode_text(head: &[u8]) -> Option<String> {
    if let Some(rest) = head
        .strip_prefix(b"\xff\xfe")
        .map(|r| (r, true))
        .or_else(|| head.strip_prefix(b"\xfe\xff").map(|r| (r, false)))
        .map(|(r, le)| {
            let (pairs, _) = r.as_chunks::<2>();
            let units: Vec<u16> = pairs
                .iter()
                .map(|&c| {
                    if le {
                        u16::from_le_bytes(c)
                    } else {
                        u16::from_be_bytes(c)
                    }
                })
                .collect();
            units
        })
    {
        let mut out = String::new();
        let mut it = char::decode_utf16(rest.iter().copied()).peekable();
        while let Some(r) = it.next() {
            match r {
                Ok('\0') => return None,
                Ok(c) => out.push(c),
                // a trailing lone high surrogate is a cut, anything else is garbage
                Err(_) if it.peek().is_none() => {}
                Err(_) => return None,
            }
        }
        return Some(out);
    }
    let body = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let valid = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(e) if e.error_len().is_none() => {
            // incomplete sequence at the very end only
            std::str::from_utf8(&body[..e.valid_up_to()]).ok()?
        }
        Err(_) => return None,
    };
    if valid.contains('\0') {
        return None;
    }
    Some(valid.to_string())
}

/// Classify a COMPLETE zip archive by its structure. Anything that is not a recognised
/// office/ebook container (including corrupt or truncated archives) is
/// `UnsupportedType`.
pub fn sniff_zip(bytes: &[u8]) -> Result<Detected, IngestError> {
    sniff_zip_reader(Cursor::new(bytes))
}

/// [`sniff_zip`] over any seekable reader (a [`sniff_zip_file`] scratch file, or memory).
pub fn sniff_zip_reader<R: Read + Seek>(reader: R) -> Result<Detected, IngestError> {
    let zip_err = || unsupported("application/zip");
    let mut zip = zip::ZipArchive::new(reader).map_err(|_| zip_err())?;
    if let Ok(mut f) = zip.by_name("mimetype") {
        let mut buf = Vec::new();
        f.by_ref()
            .take(128)
            .read_to_end(&mut buf)
            .map_err(|_| zip_err())?;
        let m = String::from_utf8_lossy(&buf).trim().to_string();
        return match m.as_str() {
            "application/epub+zip" => Ok(Detected::Epub),
            "application/vnd.oasis.opendocument.text" => Ok(Detected::Odt),
            // known ODF/ebook siblings are named; anything else is never echoed back
            "application/vnd.oasis.opendocument.spreadsheet"
            | "application/vnd.oasis.opendocument.presentation"
            | "application/vnd.oasis.opendocument.graphics" => Err(IngestError::UnsupportedType(m)),
            _ => Err(zip_err()),
        };
    }
    if zip.by_name("[Content_Types].xml").is_ok() {
        let has = |name: &str| zip.file_names().any(|n| n.is_ok_and(|n| n == name));
        if has("word/document.xml") {
            return Ok(Detected::Docx);
        }
        if has("ppt/presentation.xml") {
            return Ok(Detected::Pptx);
        }
        if has("xl/workbook.xml") {
            return Ok(Detected::Xlsx);
        }
    }
    Err(zip_err())
}

/// Tail read to find the end-of-central-directory record: the record (22 B) + the largest
/// comment (65 535 B) + the zip64 locator (20 B) and record (56 B), rounded up.
pub const ZIP_TAIL: u64 = 66 * 1024;
/// Largest central directory considered; anything bigger is not an office/ebook document (the
/// zip crate parses the whole directory into memory).
pub const MAX_CENTRAL_DIR: u64 = 8 * 1024 * 1024;
/// Most entries an archive may declare (a large real xlsx/docx has a few thousand).
pub const MAX_ZIP_ENTRIES: u64 = 100_000;

/// The worker's zip path (rulings R27): classify a COMPLETE zip archive held in a (scratch)
/// file. The end-of-central-directory record (zip64 too) is checked first: a directory over
/// [`MAX_CENTRAL_DIR`] or [`MAX_ZIP_ENTRIES`], or one that does not fit the file, is
/// `UnsupportedType("application/zip")` before the zip crate parses anything. Detection then
/// opens at most two entries and reads at most 128 bytes of one (no decompression bomb).
/// Blocking I/O: run it on a blocking thread.
pub fn sniff_zip_file(mut file: std::fs::File) -> Result<Detected, IngestError> {
    let zip_err = || unsupported("application/zip");
    let io = |_: std::io::Error| zip_err();
    let size = file.seek(SeekFrom::End(0)).map_err(io)?;
    if size < 22 {
        return Err(zip_err());
    }
    let tail_start = size.saturating_sub(ZIP_TAIL);
    let tail = read_at(&mut file, tail_start, size - tail_start).map_err(io)?;
    let eocd = find_eocd(&tail).ok_or_else(zip_err)?;
    let mut entries = u64::from(le16(&tail, eocd + 10)?);
    let mut cd_size = u64::from(le32(&tail, eocd + 12)?);
    let mut cd_off = u64::from(le32(&tail, eocd + 16)?);
    if cd_size == 0xFFFF_FFFF || cd_off == 0xFFFF_FFFF || entries == 0xFFFF {
        // zip64: the locator sits right before the EOCD record and points at the zip64 record.
        let loc = eocd.checked_sub(20).ok_or_else(zip_err)?;
        if tail.get(loc..loc + 4) != Some(b"PK\x06\x07") {
            return Err(zip_err());
        }
        let rec_off = le64(&tail, loc + 8)?;
        if rec_off.checked_add(56).is_none_or(|e| e > size) {
            return Err(zip_err());
        }
        let rec = read_at(&mut file, rec_off, 56).map_err(io)?;
        if !rec.starts_with(b"PK\x06\x06") {
            return Err(zip_err());
        }
        entries = le64(&rec, 32)?;
        cd_size = le64(&rec, 40)?;
        cd_off = le64(&rec, 48)?;
    }
    if cd_size > MAX_CENTRAL_DIR
        || entries > MAX_ZIP_ENTRIES
        || cd_off.checked_add(cd_size).is_none_or(|e| e > size)
    {
        return Err(zip_err());
    }
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    sniff_zip_reader(std::io::BufReader::new(file))
}

fn read_at(file: &mut std::fs::File, at: u64, len: u64) -> std::io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(at))?;
    let mut buf = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    file.take(len).read_to_end(&mut buf)?;
    Ok(buf)
}

/// The last end-of-central-directory signature whose record (with its comment) fits the tail.
fn find_eocd(tail: &[u8]) -> Option<usize> {
    let last = tail.len().checked_sub(22)?;
    (0..=last).rev().find(|&i| {
        tail[i..].starts_with(b"PK\x05\x06")
            && le16(tail, i + 20).is_ok_and(|c| i + 22 + usize::from(c) <= tail.len())
    })
}

fn le16(b: &[u8], at: usize) -> Result<u16, IngestError> {
    b.get(at..at + 2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
        .ok_or_else(|| unsupported("application/zip"))
}

fn le32(b: &[u8], at: usize) -> Result<u32, IngestError> {
    b.get(at..at + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
        .ok_or_else(|| unsupported("application/zip"))
}

fn le64(b: &[u8], at: usize) -> Result<u64, IngestError> {
    b.get(at..at + 8)
        .map(|s| u64::from_le_bytes(s.try_into().expect("8 bytes")))
        .ok_or_else(|| unsupported("application/zip"))
}
