//! XLSX (Office Open XML spreadsheet) rows with `zip` + `quick-xml`: the workbook's sheet list,
//! its relationships, the shared strings and one worksheet (the named `sheet`, else the first).
//!
//! Cell values: shared and inline strings, formula string results, booleans (`TRUE`/`FALSE`)
//! and numbers as written in the file. Dates are numbers in XLSX (styles decide how Excel shows
//! them) and come out as the serial number; error cells are empty. Macros, formulas and styles
//! are never evaluated.

use std::collections::HashMap;
use std::io::{Cursor, Read};

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

use super::records::Table;
use super::{Budget, MAX_CELL_CHARS, ParseLimits, Record, SourceError};

/// Most bytes one XML part of the workbook may inflate to (zip-bomb bound).
pub const MAX_PART_BYTES: u64 = 64 * 1024 * 1024;

fn invalid(message: impl Into<String>) -> SourceError {
    SourceError::invalid("invalid_xlsx", message)
}

fn read_part(
    zip: &mut zip::ZipArchive<Cursor<&[u8]>>,
    name: &str,
) -> Result<Option<String>, SourceError> {
    let file = match zip.by_name(name) {
        Ok(f) => f,
        Err(zip::result::ZipError::FileNotFound) => return Ok(None),
        Err(_) => return Err(invalid(format!("the workbook part {name} cannot be read"))),
    };
    let mut buf = Vec::new();
    file.take(MAX_PART_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|_| invalid(format!("the workbook part {name} cannot be inflated")))?;
    if buf.len() as u64 > MAX_PART_BYTES {
        return Err(SourceError::invalid(
            "too_large",
            format!("the workbook part {name} inflates past {MAX_PART_BYTES} bytes"),
        ));
    }
    String::from_utf8(buf)
        .map(Some)
        .map_err(|_| invalid(format!("the workbook part {name} is not UTF-8")))
}

fn attr(e: &BytesStart<'_>, name: &str) -> Option<String> {
    e.try_get_attribute(name).ok().flatten().and_then(|a| {
        a.normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .ok()
            .map(|v| v.into_owned())
    })
}

fn xml_err(part: &str) -> impl Fn(quick_xml::Error) -> SourceError + '_ {
    move |_| invalid(format!("the workbook part {part} is not well-formed XML"))
}

/// Text content: text, CDATA and entity references (`&amp;`, `&#10;`).
pub(crate) fn push_text(out: &mut String, ev: &Event<'_>) {
    match ev {
        Event::Text(t) => out.push_str(&t.xml10_content()),
        Event::CData(t) => out.push_str(t),
        Event::GeneralRef(r) => {
            if let Ok(Some(c)) = r.resolve_char_ref() {
                out.push(c);
            } else if let Some(s) = quick_xml::escape::resolve_predefined_entity(r) {
                out.push_str(s);
            }
        }
        _ => {}
    }
}

/// `(name, relationship id)` of every sheet, in workbook order.
fn sheets(xml: &str) -> Result<Vec<(String, String)>, SourceError> {
    let mut r = Reader::from_str(xml);
    let mut out = Vec::new();
    loop {
        match r.read_event().map_err(xml_err("xl/workbook.xml"))? {
            Event::Start(e) | Event::Empty(e) if e.local_name().as_ref() == "sheet" => {
                if let (Some(name), Some(id)) = (attr(&e, "name"), attr(&e, "r:id")) {
                    out.push((name, id));
                }
            }
            Event::Eof => return Ok(out),
            _ => {}
        }
    }
}

/// Relationship id → part name inside the archive.
fn relationships(xml: &str) -> Result<HashMap<String, String>, SourceError> {
    let mut r = Reader::from_str(xml);
    let mut out = HashMap::new();
    loop {
        match r
            .read_event()
            .map_err(xml_err("xl/_rels/workbook.xml.rels"))?
        {
            Event::Start(e) | Event::Empty(e) if e.local_name().as_ref() == "Relationship" => {
                if let (Some(id), Some(target)) = (attr(&e, "Id"), attr(&e, "Target")) {
                    let part = match target.strip_prefix('/') {
                        Some(abs) => abs.to_string(),
                        None => format!("xl/{target}"),
                    };
                    out.insert(id, part);
                }
            }
            Event::Eof => return Ok(out),
            _ => {}
        }
    }
}

/// The shared-string table: each `<si>`'s text (rich-text runs joined, phonetic runs skipped).
/// A string over Excel's own cell limit is `cell_too_large`.
fn shared_strings(xml: &str, budget: &Budget) -> Result<Vec<String>, SourceError> {
    let mut r = Reader::from_str(xml);
    let mut out = Vec::new();
    let (mut cur, mut in_si, mut in_t, mut in_rph) = (String::new(), false, false, false);
    loop {
        let ev = r.read_event().map_err(xml_err("xl/sharedStrings.xml"))?;
        match &ev {
            Event::Start(e) => match e.local_name().as_ref() {
                "si" => {
                    in_si = true;
                    cur.clear();
                }
                "t" => in_t = true,
                "rPh" => in_rph = true,
                _ => {}
            },
            Event::Empty(e) if e.local_name().as_ref() == "si" => out.push(String::new()),
            Event::End(e) => match e.local_name().as_ref() {
                "si" => {
                    in_si = false;
                    check_cell(&cur)?;
                    out.push(std::mem::take(&mut cur));
                    if out.len().is_multiple_of(1024) {
                        budget.check_deadline()?;
                    }
                }
                "t" => in_t = false,
                "rPh" => in_rph = false,
                _ => {}
            },
            Event::Eof => return Ok(out),
            _ if in_si && in_t && !in_rph => push_text(&mut cur, &ev),
            _ => {}
        }
    }
}

/// Excel's cell limit ([`MAX_CELL_CHARS`]).
fn check_cell(s: &str) -> Result<(), SourceError> {
    // Bytes first: no char count for the common short string.
    if s.len() > MAX_CELL_CHARS && s.chars().count() > MAX_CELL_CHARS {
        return Err(SourceError::invalid(
            "cell_too_large",
            format!("a cell holds more than Excel's {MAX_CELL_CHARS} characters"),
        ));
    }
    Ok(())
}

/// 0-based column of a cell reference (`B12` → 1).
fn column(cell_ref: &str) -> Option<usize> {
    let letters: Vec<u8> = cell_ref
        .bytes()
        .take_while(u8::is_ascii_alphabetic)
        .collect();
    if letters.is_empty() || letters.len() > 3 {
        return None;
    }
    let n = letters.iter().fold(0usize, |n, b| {
        n * 26 + usize::from(b.to_ascii_uppercase() - b'A' + 1)
    });
    Some(n - 1)
}

/// The worksheet's rows into `table`, each cell placed at its column. Every cell value is
/// charged to the budget BEFORE it is materialised (a shared string repeated by many cells
/// counts each time), and a column past `max_columns` is refused before a row grows to it.
fn sheet_rows(
    xml: &str,
    part: &str,
    strings: &[String],
    table: &mut Table,
) -> Result<(), SourceError> {
    let max_columns = table.budget.limits.max_columns;
    let mut r = Reader::from_str(xml);
    let mut row: Option<Vec<String>> = None;
    // The open cell: column, type, collected text; whether we are inside <v> or <is>/<t>.
    let mut cell: Option<(usize, String, String)> = None;
    let (mut in_v, mut in_is_t) = (false, false);
    loop {
        let ev = r.read_event().map_err(xml_err(part))?;
        match &ev {
            Event::Start(e) | Event::Empty(e) => {
                let empty = matches!(ev, Event::Empty(_));
                match e.local_name().as_ref() {
                    "row" if !empty => row = Some(Vec::new()),
                    "c" if !empty => {
                        let next = row.as_ref().map_or(0, Vec::len);
                        let col = attr(e, "r").and_then(|r| column(&r)).unwrap_or(next);
                        if col >= max_columns {
                            return Err(table.budget.too_many_columns());
                        }
                        let t = attr(e, "t").unwrap_or_else(|| "n".into());
                        cell = Some((col, t, String::new()));
                    }
                    "v" if !empty => in_v = true,
                    "t" if !empty && cell.is_some() => in_is_t = true,
                    _ => {}
                }
            }
            Event::End(e) => match e.local_name().as_ref() {
                "v" => in_v = false,
                "t" => in_is_t = false,
                "c" => {
                    if let (Some((col, t, raw)), Some(row)) = (cell.take(), row.as_mut()) {
                        let value = match t.as_str() {
                            "s" => match raw
                                .trim()
                                .parse::<usize>()
                                .ok()
                                .and_then(|i| strings.get(i))
                            {
                                Some(shared) => {
                                    table.budget.charge(shared.len())?;
                                    shared.clone()
                                }
                                None => String::new(),
                            },
                            "b" => match raw.trim() {
                                "1" => "TRUE".into(),
                                "0" => "FALSE".into(),
                                _ => String::new(),
                            },
                            "e" => String::new(),
                            _ => {
                                check_cell(&raw)?;
                                table.budget.charge(raw.len())?;
                                raw
                            }
                        };
                        if row.len() <= col {
                            row.resize(col + 1, String::new());
                        }
                        row[col] = value;
                    }
                }
                "row" => {
                    if let Some(row) = row.take() {
                        table.push_row(row, true)?;
                    }
                }
                _ => {}
            },
            Event::Eof => return Ok(()),
            _ if in_v || in_is_t => {
                if let Some((_, _, raw)) = cell.as_mut() {
                    push_text(raw, &ev);
                }
            }
            _ => {}
        }
    }
}

/// The records of sheet `sheet` (else the first sheet) of an XLSX file under
/// [`ParseLimits::standard`]. See [`xlsx_records_with`].
pub fn xlsx_records(body: &[u8], sheet: Option<&str>) -> Result<Vec<Record>, SourceError> {
    xlsx_records_with(body, sheet, ParseLimits::standard(), true)
}

/// The records of sheet `sheet` (else the first sheet; first non-empty row = header, unless
/// `header_row` is false: then columns are `column_N`) under `limits`. An unknown sheet name is `sheet_not_found` (the message lists the sheets).
pub fn xlsx_records_with(
    body: &[u8],
    sheet: Option<&str>,
    limits: ParseLimits,
    header_row: bool,
) -> Result<Vec<Record>, SourceError> {
    let mut zip = zip::ZipArchive::new(Cursor::new(body))
        .map_err(|_| invalid("the file is not an XLSX workbook (not a zip archive)"))?;
    let workbook = read_part(&mut zip, "xl/workbook.xml")?
        .ok_or_else(|| invalid("the file is not an XLSX workbook (no xl/workbook.xml)"))?;
    let all = sheets(&workbook)?;
    let (name, rid) = match sheet {
        Some(want) => all
            .iter()
            .find(|(n, _)| n == want)
            .or_else(|| all.iter().find(|(n, _)| n.eq_ignore_ascii_case(want)))
            .ok_or_else(|| {
                let names: Vec<&str> = all.iter().map(|(n, _)| n.as_str()).collect();
                SourceError::invalid(
                    "sheet_not_found",
                    format!("the workbook has no sheet {want:?}; its sheets: {names:?}"),
                )
            })?,
        None => all
            .first()
            .ok_or_else(|| invalid("the workbook has no sheets"))?,
    };
    let rels = read_part(&mut zip, "xl/_rels/workbook.xml.rels")?
        .ok_or_else(|| invalid("the workbook has no xl/_rels/workbook.xml.rels"))?;
    let part = relationships(&rels)?
        .remove(rid)
        .ok_or_else(|| invalid(format!("sheet {name:?} has no worksheet part")))?;
    let mut table = Table::with_header(limits, header_row);
    let strings = match read_part(&mut zip, "xl/sharedStrings.xml")? {
        Some(xml) => shared_strings(&xml, &table.budget)?,
        None => Vec::new(),
    };
    let xml = read_part(&mut zip, &part)?
        .ok_or_else(|| invalid(format!("the worksheet part {part} is missing")))?;
    sheet_rows(&xml, &part, &strings, &mut table)?;
    Ok(table.finish())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Write;

    use super::*;

    /// A minimal workbook: `sheets` = `(name, sheet XML <sheetData> body)`; shared strings given.
    pub(crate) fn workbook(sheets: &[(&str, &str)], shared: &[&str]) -> Vec<u8> {
        let mut buf = Cursor::new(Vec::new());
        let mut z = zip::ZipWriter::new(&mut buf);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        let mut wb = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>"#,
        );
        let mut rels = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">"#,
        );
        for (i, (name, data)) in sheets.iter().enumerate() {
            let n = i + 1;
            wb.push_str(&format!(
                r#"<sheet name="{name}" sheetId="{n}" r:id="rId{n}"/>"#
            ));
            rels.push_str(&format!(
                r#"<Relationship Id="rId{n}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{n}.xml"/>"#
            ));
            z.start_file(format!("xl/worksheets/sheet{n}.xml"), opts)
                .unwrap();
            z.write_all(
                format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{data}</sheetData></worksheet>"#
                )
                .as_bytes(),
            )
            .unwrap();
        }
        wb.push_str("</sheets></workbook>");
        rels.push_str("</Relationships>");
        z.start_file("xl/workbook.xml", opts).unwrap();
        z.write_all(wb.as_bytes()).unwrap();
        z.start_file("xl/_rels/workbook.xml.rels", opts).unwrap();
        z.write_all(rels.as_bytes()).unwrap();
        let mut sst = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">"#,
        );
        for s in shared {
            sst.push_str(&format!("<si>{s}</si>"));
        }
        sst.push_str("</sst>");
        z.start_file("xl/sharedStrings.xml", opts).unwrap();
        z.write_all(sst.as_bytes()).unwrap();
        z.finish().unwrap();
        buf.into_inner()
    }

    #[test]
    fn reads_the_named_sheet_with_every_cell_type() {
        let data = r#"<row r="1"><c r="A1" t="s"><v>0</v></c><c r="C1" t="s"><v>1</v></c></row>
            <row r="2"><c r="A2" t="inlineStr"><is><t>C-1 &amp; co</t></is></c><c r="B2" t="b"><v>1</v></c><c r="C2"><v>3.5</v></c></row>
            <row r="3"><c r="A3" t="str"><f>A1</f><v>calc</v></c><c r="C3" t="e"><v>#DIV/0!</v></c></row>"#;
        let book = workbook(
            &[
                ("Other", "<row r=\"1\"><c r=\"A1\"><v>9</v></c></row>"),
                ("Customers", data),
            ],
            &[
                "<t>id</t>",
                "<r><t>am</t></r><r><t>ount</t></r><rPh><t>x</t></rPh>",
            ],
        );
        let rec = |f: &[(&str, &str)]| -> Record {
            f.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert_eq!(
            xlsx_records(&book, Some("Customers")).unwrap(),
            vec![
                rec(&[("id", "C-1 & co"), ("column_2", "TRUE"), ("amount", "3.5")]),
                rec(&[("id", "calc")]),
            ]
        );
        // No sheet named: the first one (a header row only: no records).
        assert_eq!(xlsx_records(&book, None).unwrap(), Vec::<Record>::new());
        // Case-insensitive fallback; unknown names list the sheets.
        assert_eq!(xlsx_records(&book, Some("customers")).unwrap().len(), 2);
        let e = xlsx_records(&book, Some("Nope")).unwrap_err();
        assert_eq!(e.code, "sheet_not_found");
        assert!(e.message.contains("Customers"), "{}", e.message);
        let one = ParseLimits {
            max_records: 1,
            ..ParseLimits::standard()
        };
        assert_eq!(
            xlsx_records_with(&book, Some("Customers"), one, true)
                .unwrap_err()
                .code,
            "too_many_records"
        );
    }

    #[test]
    fn without_a_header_row_the_first_row_is_data() {
        let data = r#"<row r="1"><c r="A1" t="s"><v>0</v></c><c r="C1"><v>7</v></c></row>
            <row r="2"><c r="B2" t="inlineStr"><is><t>Delft</t></is></c></row>"#;
        let book = workbook(&[("Rows", data)], &["<t>C-1</t>"]);
        let rec = |f: &[(&str, &str)]| -> Record {
            f.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let limits = ParseLimits::standard();
        assert_eq!(
            xlsx_records_with(&book, None, limits, false).unwrap(),
            vec![
                rec(&[("column_1", "C-1"), ("column_3", "7")]),
                rec(&[("column_2", "Delft")]),
            ]
        );
        // The default: the first row names the columns.
        assert_eq!(
            xlsx_records_with(&book, None, limits, true).unwrap(),
            vec![rec(&[("column_2", "Delft")])]
        );
    }

    #[test]
    fn a_far_column_is_refused_not_allocated() {
        let book = workbook(
            &[("S", r#"<row r="1"><c r="XFD1"><v>1</v></c></row>"#)],
            &[],
        );
        assert_eq!(
            xlsx_records(&book, None).unwrap_err().code,
            "too_many_columns"
        );
    }

    #[test]
    fn a_shared_string_over_the_excel_cell_limit_is_refused() {
        let big = format!("<t>{}</t>", "a".repeat(32_768));
        let book = workbook(
            &[("S", r#"<row r="1"><c r="A1" t="s"><v>0</v></c></row>"#)],
            &[&big],
        );
        assert_eq!(
            xlsx_records(&book, None).unwrap_err().code,
            "cell_too_large"
        );
    }

    #[test]
    fn repeated_shared_strings_are_bounded_by_the_byte_budget() {
        // One 32,000-char shared string referenced by 2,100 cells: ~67 MB materialised.
        let big = format!("<t>{}</t>", "a".repeat(32_000));
        let mut data = String::new();
        for r in 1..=210 {
            data.push_str(&format!("<row r=\"{r}\">"));
            for c in 0..10 {
                let col = (b'A' + c) as char;
                data.push_str(&format!(r#"<c r="{col}{r}" t="s"><v>0</v></c>"#));
            }
            data.push_str("</row>");
        }
        let book = workbook(&[("S", &data)], &[&big]);
        assert_eq!(
            xlsx_records(&book, None).unwrap_err().code,
            "records_too_large"
        );
    }

    #[test]
    fn a_parse_past_its_deadline_stops() {
        let mut data = String::new();
        for r in 1..=600 {
            data.push_str(&format!(r#"<row r="{r}"><c r="A{r}"><v>{r}</v></c></row>"#));
        }
        let book = workbook(&[("S", &data)], &[]);
        let late = ParseLimits {
            deadline: Some(std::time::Instant::now()),
            ..ParseLimits::standard()
        };
        assert_eq!(
            xlsx_records_with(&book, None, late, true).unwrap_err().code,
            "parse_timeout"
        );
    }

    #[test]
    fn not_a_workbook() {
        assert_eq!(
            xlsx_records(b"id,name\n", None).unwrap_err().code,
            "invalid_xlsx"
        );
        assert_eq!(column("B12"), Some(1));
        assert_eq!(column("AA1"), Some(26));
        assert_eq!(column("12"), None);
    }
}
