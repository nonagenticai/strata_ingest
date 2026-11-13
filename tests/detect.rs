use strata_ingest::IngestError;
use strata_ingest::pipeline::detect::{Detected, is_zip, sniff, sniff_zip};

fn fx(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/detect/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}
fn head(name: &str) -> Vec<u8> {
    let mut b = fx(name);
    b.truncate(64 * 1024);
    b
}
fn s(name: &str, filename: Option<&str>) -> Result<Detected, IngestError> {
    sniff(&head(name), filename)
}
fn img(m: &str) -> Detected {
    Detected::Image(m.into())
}
fn aud(m: &str) -> Detected {
    Detected::Audio(m.into())
}
fn vid(m: &str) -> Detected {
    Detected::Video(m.into())
}
fn unsupported(r: Result<Detected, IngestError>) -> String {
    match r {
        Err(IngestError::UnsupportedType(m)) => m,
        other => panic!("expected UnsupportedType, got {other:?}"),
    }
}

#[test]
fn pdf() {
    assert_eq!(s("sample.pdf", Some("a.pdf")).unwrap(), Detected::Pdf);
}
#[test]
fn pdf_with_leading_junk_within_1k_is_pdf() {
    let mut b = b"\n\n  ".to_vec();
    b.extend(fx("sample.pdf"));
    assert_eq!(sniff(&b, None).unwrap(), Detected::Pdf);
}
#[test]
fn office_containers() {
    assert_eq!(s("sample.docx", Some("a.docx")).unwrap(), Detected::Docx);
    assert_eq!(s("sample.pptx", None).unwrap(), Detected::Pptx);
    assert_eq!(s("sample.xlsx", None).unwrap(), Detected::Xlsx);
    assert_eq!(s("sample.epub", None).unwrap(), Detected::Epub);
    assert_eq!(s("sample.odt", None).unwrap(), Detected::Odt);
}
#[test]
fn container_extension_is_ignored() {
    // a docx saved with a wrong name is still a docx
    assert_eq!(s("sample.docx", Some("a.pdf")).unwrap(), Detected::Docx);
}
#[test]
fn images() {
    assert_eq!(s("sample.png", None).unwrap(), img("image/png"));
    assert_eq!(s("sample.jpg", None).unwrap(), img("image/jpeg"));
    assert_eq!(s("sample.tiff", None).unwrap(), img("image/tiff"));
    assert_eq!(s("sample.webp", None).unwrap(), img("image/webp"));
    assert_eq!(s("sample.bmp", None).unwrap(), img("image/bmp"));
}
#[test]
fn gif_is_unsupported() {
    assert_eq!(unsupported(s("sample.gif", None)), "image/gif");
}
#[test]
fn audio() {
    assert_eq!(s("sample.mp3", None).unwrap(), aud("audio/mpeg"));
    assert_eq!(s("sample.wav", None).unwrap(), aud("audio/x-wav"));
    assert_eq!(s("sample.flac", None).unwrap(), aud("audio/x-flac"));
    assert_eq!(s("sample.ogg", None).unwrap(), aud("audio/ogg"));
    assert_eq!(s("sample.m4a", None).unwrap(), aud("audio/m4a"));
}
#[test]
fn video() {
    assert_eq!(s("sample.mp4", None).unwrap(), vid("video/mp4"));
    assert_eq!(s("sample.webm", None).unwrap(), vid("video/webm"));
    assert_eq!(s("sample.mov", None).unwrap(), vid("video/quicktime"));
    assert_eq!(s("sample.mkv", None).unwrap(), vid("video/x-matroska"));
}
#[test]
fn text_and_markdown_differ_only_by_extension() {
    let t = b"# heading\n\nplain words\n";
    assert_eq!(sniff(t, Some("n.txt")).unwrap(), Detected::Text);
    assert_eq!(sniff(t, None).unwrap(), Detected::Text);
    assert_eq!(sniff(t, Some("n.md")).unwrap(), Detected::Markdown);
    assert_eq!(sniff(t, Some("N.MARKDOWN")).unwrap(), Detected::Markdown);
}
#[test]
fn markdown_named_pdf_bytes_is_pdf() {
    assert_eq!(s("sample.pdf", Some("n.md")).unwrap(), Detected::Pdf);
}
#[test]
fn html_detected_after_bom_and_whitespace() {
    assert_eq!(s("sample.html", Some("a.html")).unwrap(), Detected::Html);
    assert_eq!(
        sniff(b"<HTML lang=en><body>", None).unwrap(),
        Detected::Html
    );
}
#[test]
fn html_named_docx_is_html() {
    assert_eq!(
        s("html_named.docx", Some("report.docx")).unwrap(),
        Detected::Html
    );
}
#[test]
fn pdf_named_zip_is_zip_not_pdf() {
    // plain zip renamed .pdf: never Pdf; a zip without office markers is unsupported
    assert!(is_zip(&head("pdf_named_zip.pdf")));
    assert_eq!(
        unsupported(s("pdf_named_zip.pdf", Some("x.pdf"))),
        "application/zip"
    );
}
#[test]
fn plain_zip_is_unsupported() {
    assert_eq!(
        unsupported(s("plain.zip", Some("a.zip"))),
        "application/zip"
    );
    assert_eq!(unsupported(sniff_zip(&fx("plain.zip"))), "application/zip");
}
#[test]
fn zip_with_truncated_head_defers_via_is_zip() {
    let full = fx("sample.pptx");
    let head = &full[..full.len() / 4];
    assert!(is_zip(head));
    // the API defers on is_zip(head); the worker sniffs the full body
    assert_eq!(sniff(&full, None).unwrap(), Detected::Pptx);
    assert_eq!(sniff_zip(&full).unwrap(), Detected::Pptx);
}
#[test]
fn is_zip_false_for_non_zip() {
    assert!(!is_zip(&fx("sample.pdf")));
    assert!(!is_zip(b""));
}
#[test]
fn corrupt_zip_is_unsupported_not_panic() {
    let mut b = b"PK\x03\x04".to_vec();
    b.extend([0u8; 40]);
    unsupported(sniff_zip(&b));
}
#[test]
fn empty_file_is_rejected() {
    assert!(matches!(
        s("empty.bin", Some("a.txt")),
        Err(IngestError::UnsupportedType(_))
    ));
}
#[test]
fn exe_is_unsupported() {
    unsupported(s("sample.exe", Some("setup.pdf")));
}
#[test]
fn binary_junk_with_nul_is_unsupported() {
    unsupported(sniff(b"abc\0def", Some("a.txt")));
}
#[test]
fn utf16_text_detected_as_text() {
    assert_eq!(s("utf16.txt", Some("a.txt")).unwrap(), Detected::Text);
}
#[test]
fn head_cut_inside_utf8_char_is_still_text() {
    let mut b = "aé".as_bytes().to_vec();
    b.pop(); // cut mid-codepoint
    assert_eq!(sniff(&b, None).unwrap(), Detected::Text);
}
#[test]
fn invalid_utf8_is_unsupported() {
    unsupported(sniff(&[0x66, 0xff, 0xfe, 0x41, 0x80, 0x81], None));
}

// ---- fix round 1 ----
use strata_ingest::pipeline::detect::sniff_head;

#[test]
fn weak_magics_do_not_hijack_text() {
    assert_eq!(
        sniff(b"BMW quarterly report\n", Some("a.txt")).unwrap(),
        Detected::Text
    );
    assert_eq!(
        sniff(b"MZ-5 notes\n\nmore", Some("n.md")).unwrap(),
        Detected::Markdown
    );
}
#[test]
fn bm_binary_without_dib_header_is_not_an_image() {
    let mut b = b"BM".to_vec();
    b.extend([0u8, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0, 0, 0]);
    unsupported(sniff(&b, None));
}
#[test]
fn sniff_head_defers_zip_heads() {
    let docx = fx("sample.docx");
    assert_eq!(sniff_head(&docx[..4096], None).unwrap(), None);
    assert_eq!(sniff_head(&head("plain.zip"), None).unwrap(), None);
    assert_eq!(
        sniff_head(&head("sample.pdf"), None).unwrap(),
        Some(Detected::Pdf)
    );
    assert!(sniff_head(&head("sample.exe"), None).is_err());
    assert!(sniff_head(b"", None).is_err());
}
#[test]
fn text_verdict_ignores_bytes_past_64kib() {
    let mut b = vec![b'a'; 200 * 1024];
    b[100 * 1024] = 0xff;
    assert_eq!(sniff(&b, None).unwrap(), Detected::Text);
    // cut mid-codepoint exactly at the 64 KiB boundary
    let mut c = vec![b'a'; 64 * 1024 - 1];
    c.extend("é".as_bytes()); // 0xC3 at index 65535, 0xA9 beyond the window
    c.extend(vec![b'b'; 1000]);
    assert_eq!(sniff(&c, None).unwrap(), Detected::Text);
}
#[test]
fn pdf_marker_must_open_the_file() {
    assert_eq!(
        sniff(b"# notes\n\nthe %PDF- header is magic\n", Some("n.md")).unwrap(),
        Detected::Markdown
    );
    assert_eq!(
        sniff(b"\xef\xbb\xbf \n%PDF-1.7\n", None).unwrap(),
        Detected::Pdf
    );
}
#[test]
fn ooxml_needs_its_main_part_not_a_bare_dir() {
    use std::io::Write;
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut z = zip::ZipWriter::new(&mut buf);
        let o = zip::write::SimpleFileOptions::default();
        z.start_file("[Content_Types].xml", o).unwrap();
        z.write_all(b"<Types/>").unwrap();
        z.add_directory("word/", o).unwrap();
        z.finish().unwrap();
    }
    assert_eq!(unsupported(sniff_zip(buf.get_ref())), "application/zip");
}
#[test]
fn unknown_mimetype_entry_is_never_echoed() {
    use std::io::Write;
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut z = zip::ZipWriter::new(&mut buf);
        let o = zip::write::SimpleFileOptions::default();
        z.start_file("mimetype", o).unwrap();
        z.write_all(b"secret<script>").unwrap();
        z.finish().unwrap();
    }
    assert_eq!(unsupported(sniff_zip(buf.get_ref())), "application/zip");
}
