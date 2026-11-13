//! Splitting a fetched body into records: a JSON API response, or a CSV file. (XLSX: `xlsx`.)

use serde_json::Value;

use std::collections::{HashMap, HashSet};

use super::{Budget, ParseLimits, Record, SourceError};

/// Keys tried, in order, when a JSON object holds several arrays.
pub const ARRAY_KEYS: [&str; 7] = [
    "items", "data", "results", "records", "rows", "value", "entries",
];

/// The records of an api response. Accepted shapes:
/// - a JSON array: each item is a record;
/// - a JSON object holding exactly one array (any key), or several where one is named `items`,
///   `data`, `results`, `records`, `rows`, `value` or `entries` (tried in that order).
///
/// An object item is flattened: nested keys join with `.` (`address.city`), array elements by
/// index (`lines.0.sku`) unless the array holds only scalars, which are joined with `; `. A
/// scalar item becomes the one field `value`. `null` values are left out.
pub fn json_records(body: &[u8]) -> Result<Vec<Record>, SourceError> {
    json_records_with(body, ParseLimits::standard())
}

/// [`json_records`] under `limits` (records, fields per record, bytes, deadline).
pub fn json_records_with(body: &[u8], limits: ParseLimits) -> Result<Vec<Record>, SourceError> {
    let mut budget = Budget::new(limits);
    let v: Value = serde_json::from_slice(body).map_err(|e| {
        SourceError::invalid(
            "invalid_json",
            format!("the response is not valid JSON ({})", e.classify_name()),
        )
    })?;
    let items = match v {
        Value::Array(a) => a,
        Value::Object(mut o) => {
            let arrays: Vec<String> = o
                .iter()
                .filter(|(_, v)| v.is_array())
                .map(|(k, _)| k.clone())
                .collect();
            let key = if arrays.len() == 1 {
                Some(arrays[0].clone())
            } else {
                ARRAY_KEYS
                    .iter()
                    .find(|k| arrays.iter().any(|a| a == *k))
                    .map(|k| k.to_string())
            };
            match key.and_then(|k| o.remove(&k)) {
                Some(Value::Array(a)) => a,
                _ => {
                    return Err(SourceError::invalid(
                        "unrecognised_shape",
                        format!(
                            "the response is a JSON object without one array of records; expected \
                             an array, or an object holding one (one array key, or one of {})",
                            ARRAY_KEYS.join(", ")
                        ),
                    ));
                }
            }
        }
        _ => {
            return Err(SourceError::invalid(
                "unrecognised_shape",
                "the response is a JSON scalar; expected an array of records or an object holding one",
            ));
        }
    };
    if items.len() > limits.max_records {
        return Err(budget.too_many_records());
    }
    let mut records = Vec::with_capacity(items.len());
    for item in items {
        budget.check_deadline()?;
        let mut out = Vec::new();
        match item {
            Value::Object(_) | Value::Array(_) => {
                flatten("", &item, 0, &mut 0, &mut out, &mut budget)?
            }
            other => flatten("value", &other, 0, &mut 0, &mut out, &mut budget)?,
        }
        if !out.is_empty() {
            records.push(out);
        }
    }
    Ok(records)
}

/// `serde_json` error categories, without the position or any of the input.
trait Classify {
    fn classify_name(&self) -> &'static str;
}

impl Classify for serde_json::Error {
    fn classify_name(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Io => "read error",
            serde_json::error::Category::Syntax => "syntax error",
            serde_json::error::Category::Data => "unexpected data",
            serde_json::error::Category::Eof => "truncated",
        }
    }
}

fn join(prefix: &str, key: &str) -> String {
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}.{key}")
    }
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) if s.trim().is_empty() => None,
        Value::String(s) => Some(s.trim().to_string()),
        Value::Bool(_) | Value::Number(_) => Some(v.to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

/// Longest JSON key accepted (bytes).
pub const MAX_KEY_BYTES: usize = 256;
/// Deepest JSON nesting accepted inside one record.
pub const MAX_DEPTH: usize = 32;
/// Most JSON nodes (values of any kind) one record may hold.
pub const MAX_NODES_PER_ITEM: usize = 200_000;

/// Walks one record. EVERY node visited is counted and charged (the field-name prefix it
/// copies, plus one), so empty `{}` / `[]` / `null` children cost budget and reach the deadline
/// check too; keys over [`MAX_KEY_BYTES`], nesting past [`MAX_DEPTH`] and more than
/// [`MAX_NODES_PER_ITEM`] nodes are refused.
fn flatten(
    prefix: &str,
    v: &Value,
    depth: usize,
    nodes: &mut usize,
    out: &mut Record,
    budget: &mut Budget,
) -> Result<(), SourceError> {
    *nodes += 1;
    if *nodes > MAX_NODES_PER_ITEM {
        return Err(SourceError::invalid(
            "too_many_nodes",
            format!("a record holds more than {MAX_NODES_PER_ITEM} JSON values"),
        ));
    }
    if depth > MAX_DEPTH {
        return Err(SourceError::invalid(
            "too_deep",
            format!("a record nests deeper than {MAX_DEPTH} levels"),
        ));
    }
    budget.charge(prefix.len() + 1)?;
    match v {
        Value::Object(o) => {
            for (k, v) in o {
                if k.len() > MAX_KEY_BYTES {
                    return Err(SourceError::invalid(
                        "key_too_long",
                        format!("a JSON key is longer than {MAX_KEY_BYTES} bytes"),
                    ));
                }
                flatten(&join(prefix, k), v, depth + 1, nodes, out, budget)?;
            }
        }
        Value::Array(a) if a.iter().all(|x| !x.is_array() && !x.is_object()) => {
            *nodes += a.len();
            let parts: Vec<String> = a.iter().filter_map(scalar).collect();
            if !parts.is_empty() {
                push_field(prefix.to_string(), parts.join("; "), out, budget)?;
            }
        }
        Value::Array(a) => {
            for (i, v) in a.iter().enumerate() {
                flatten(
                    &join(prefix, &i.to_string()),
                    v,
                    depth + 1,
                    nodes,
                    out,
                    budget,
                )?;
            }
        }
        _ => {
            if let Some(s) = scalar(v) {
                push_field(prefix.to_string(), s, out, budget)?;
            }
        }
    }
    Ok(())
}

fn push_field(
    name: String,
    value: String,
    out: &mut Record,
    budget: &mut Budget,
) -> Result<(), SourceError> {
    if out.len() >= budget.limits.max_columns {
        return Err(budget.too_many_columns());
    }
    budget.charge(name.len() + value.len())?;
    out.push((name, value));
    Ok(())
}

/// Unique, non-empty column names: a blank header is `column_N` (1-based), a repeated one gets
/// `_2`, `_3`, … (skipping names already taken). Linear: a per-base counter, a set of names.
pub fn header_names(raw: &[String]) -> Vec<String> {
    let mut taken: HashSet<String> = HashSet::with_capacity(raw.len());
    let mut next: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(raw.len());
    for (i, h) in raw.iter().enumerate() {
        let base = match h.trim() {
            "" => format!("column_{}", i + 1),
            t => t.to_string(),
        };
        let mut name = base.clone();
        if taken.contains(&name) {
            let n = next.entry(base.clone()).or_insert(2);
            loop {
                name = format!("{base}_{n}");
                *n += 1;
                if !taken.contains(&name) {
                    break;
                }
            }
        }
        taken.insert(name.clone());
        out.push(name);
    }
    out
}

/// Rows to records as they are read: the first non-empty row is the header (unless the table
/// was made [`Table::with_header`] `false`: then every row is data and the columns are named
/// `column_1`, `column_2`, …); empty cells are left out. Every bound of the [`Budget`] (columns, records, bytes, deadline) is enforced per
/// row, so a source over a limit stops at the first row past it.
#[derive(Debug)]
pub struct Table {
    pub budget: Budget,
    names: Option<Vec<String>>,
    records: Vec<Record>,
}

impl Table {
    pub fn new(limits: ParseLimits) -> Self {
        Self::with_header(limits, true)
    }

    /// `header_row` false: no row is taken as the header; columns are `column_N` (1-based).
    pub fn with_header(limits: ParseLimits, header_row: bool) -> Self {
        Self {
            budget: Budget::new(limits),
            names: if header_row { None } else { Some(Vec::new()) },
            records: Vec::new(),
        }
    }

    /// Adds one row. `values_charged`: the caller already charged the cell values (XLSX
    /// charges each cell before materialising it); names are always charged here.
    pub fn push_row(&mut self, row: Vec<String>, values_charged: bool) -> Result<(), SourceError> {
        self.budget.tick()?;
        if row.len() > self.budget.limits.max_columns {
            return Err(self.budget.too_many_columns());
        }
        if row.iter().all(|c| c.trim().is_empty()) {
            return Ok(());
        }
        let Some(names) = &self.names else {
            let names = header_names(&row);
            self.budget.charge(names.iter().map(String::len).sum())?;
            self.names = Some(names);
            return Ok(());
        };
        if self.records.len() >= self.budget.limits.max_records {
            return Err(self.budget.too_many_records());
        }
        let mut record = Vec::new();
        for (i, c) in row.into_iter().enumerate() {
            let v = c.trim();
            if v.is_empty() {
                continue;
            }
            let name = names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("column_{}", i + 1));
            let cost = name.len() + if values_charged { 0 } else { v.len() };
            self.budget.charge(cost)?;
            record.push((name, v.to_string()));
        }
        self.records.push(record);
        Ok(())
    }

    pub fn finish(self) -> Vec<Record> {
        self.records
    }
}

/// [`Table`] over ready rows (the first one is the header), with the standard limits.
pub fn table_records(rows: Vec<Vec<String>>) -> Result<Vec<Record>, SourceError> {
    let mut t = Table::new(ParseLimits::standard());
    for row in rows {
        t.push_row(row, false)?;
    }
    Ok(t.finish())
}

/// The delimiter of a CSV file: whichever of `,` `;` tab `|` occurs most in its first line
/// (`,` on a tie or when none does). Dutch spreadsheet exports use `;`.
fn sniff_delimiter(body: &[u8]) -> u8 {
    let line = body.split(|b| *b == b'\n').next().unwrap_or_default();
    let mut best = (b',', 0usize);
    for d in *b",;\t|" {
        let n = line.iter().filter(|b| **b == d).count();
        if n > best.1 {
            best = (d, n);
        }
    }
    best.0
}

/// [`csv_records_with`] under [`ParseLimits::standard`].
pub fn csv_records(body: &[u8]) -> Result<Vec<Record>, SourceError> {
    csv_records_with(body, ParseLimits::standard(), true)
}

/// The records of a CSV file: the first non-empty row is the header (`header_row` false: no
/// header, columns `column_N`). UTF-8 (a BOM is skipped;
/// invalid bytes are replaced). Rows may be shorter or longer than the header. Streams: stops at
/// the first row past a limit.
pub fn csv_records_with(
    body: &[u8],
    limits: ParseLimits,
    header_row: bool,
) -> Result<Vec<Record>, SourceError> {
    let body = body.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(body);
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(sniff_delimiter(body))
        .has_headers(false)
        .flexible(true)
        .from_reader(body);
    let mut table = Table::with_header(limits, header_row);
    let mut rec = csv::ByteRecord::new();
    loop {
        let more = rdr.read_byte_record(&mut rec).map_err(|e| {
            let at = e
                .position()
                .map(|p| format!(" at line {}", p.line()))
                .unwrap_or_default();
            SourceError::invalid("invalid_csv", format!("the CSV file cannot be read{at}"))
        })?;
        if !more {
            break;
        }
        if rec.len() > limits.max_columns {
            return Err(table.budget.too_many_columns());
        }
        let row = rec
            .iter()
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect();
        table.push_row(row, false)?;
    }
    Ok(table.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(fields: &[(&str, &str)]) -> Record {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn json_array_and_object_shapes() {
        let arr =
            br#"[{"id": 1, "name": "A", "tags": ["x", "y"], "addr": {"city": "U"}, "n": null},
                       "plain"]"#;
        assert_eq!(
            json_records(arr).unwrap(),
            vec![
                r(&[
                    ("addr.city", "U"),
                    ("id", "1"),
                    ("name", "A"),
                    ("tags", "x; y")
                ]),
                r(&[("value", "plain")]),
            ]
        );
        let one = br#"{"total": 1, "customers": [{"id": "C-1"}]}"#;
        assert_eq!(json_records(one).unwrap(), vec![r(&[("id", "C-1")])]);
        let named = br#"{"links": [], "data": [{"id": 2}], "next": null}"#;
        assert_eq!(json_records(named).unwrap(), vec![r(&[("id", "2")])]);
        let lines = br#"[{"lines": [{"sku": "S1"}, {"sku": "S2"}]}]"#;
        assert_eq!(
            json_records(lines).unwrap(),
            vec![r(&[("lines.0.sku", "S1"), ("lines.1.sku", "S2")])]
        );
    }

    #[test]
    fn json_errors_say_what_without_quoting_the_body() {
        let e = json_records(br#"{"a": [1], "b": [2]}"#).unwrap_err();
        assert_eq!(e.code, "unrecognised_shape");
        let e = json_records(b"{\"secret-ish\": ").unwrap_err();
        assert_eq!(e.code, "invalid_json");
        assert!(!e.message.contains("secret-ish"), "{}", e.message);
        assert_eq!(json_records(b"42").unwrap_err().code, "unrecognised_shape");
    }

    #[test]
    fn header_dedup_is_linear() {
        let raw = vec!["same".to_string(); 1000];
        let t = std::time::Instant::now();
        let names = header_names(&raw);
        assert!(
            t.elapsed() < std::time::Duration::from_millis(100),
            "{:?}",
            t.elapsed()
        );
        assert_eq!(names[0], "same");
        assert_eq!(names[999], "same_1000");
        // A generated name never collides with a real header.
        let raw: Vec<String> = ["a", "a_2", "a"].iter().map(|s| s.to_string()).collect();
        assert_eq!(header_names(&raw), ["a", "a_2", "a_3"]);
    }

    #[test]
    fn csv_without_a_header_row_keeps_the_first_row_as_data() {
        let body = b"C-1;Acme\n\nC-2;;Delft\n";
        let limits = ParseLimits::standard();
        assert_eq!(
            csv_records_with(body, limits, false).unwrap(),
            vec![
                r(&[("column_1", "C-1"), ("column_2", "Acme")]),
                r(&[("column_1", "C-2"), ("column_3", "Delft")]),
            ]
        );
        // The default still takes the first row as the header.
        assert_eq!(
            csv_records_with(body, limits, true).unwrap(),
            vec![r(&[("C-1", "C-2"), ("column_3", "Delft")])]
        );
    }

    #[test]
    fn csv_and_json_parses_past_their_deadline_stop() {
        let late = ParseLimits {
            deadline: Some(std::time::Instant::now()),
            ..ParseLimits::standard()
        };
        let mut body = String::from("id\n");
        for i in 0..600 {
            body.push_str(&format!("{i}\n"));
        }
        assert_eq!(
            csv_records_with(body.as_bytes(), late, true)
                .unwrap_err()
                .code,
            "parse_timeout"
        );
        assert_eq!(
            json_records_with(b"[{\"a\": 1}]", late).unwrap_err().code,
            "parse_timeout"
        );
    }

    #[test]
    fn empty_children_under_a_huge_key_fail_fast_inside_the_parser() {
        // `[{"<200 KB key>": [{}, {}, …]}]`: no field is ever stored, but every child copies the
        // key prefix. Must stop on its own (no outer timeout here), quickly.
        let key = "k".repeat(200 * 1024);
        let children = vec!["{}"; 20_000].join(",");
        let body = format!(r#"[{{"{key}": [{children}]}}]"#);
        let t = std::time::Instant::now();
        let e = json_records(body.as_bytes()).unwrap_err();
        assert!(
            ["key_too_long", "records_too_large", "parse_timeout"].contains(&e.code),
            "{e:?}"
        );
        assert!(
            t.elapsed() < std::time::Duration::from_secs(1),
            "{:?}",
            t.elapsed()
        );
    }

    #[test]
    fn deep_nesting_and_node_count_are_capped() {
        let deep = format!("[{}1{}]", "[".repeat(40), "]".repeat(40));
        assert_eq!(json_records(deep.as_bytes()).unwrap_err().code, "too_deep");
        let many = format!("[[{}]]", vec!["{}"; 200_001].join(","));
        assert_eq!(
            json_records(many.as_bytes()).unwrap_err().code,
            "too_many_nodes"
        );
        // Within the caps: fine.
        let ok = format!("[{}1{}]", "[".repeat(10), "]".repeat(10));
        assert!(json_records(ok.as_bytes()).is_ok());
    }

    #[test]
    fn json_field_and_record_caps() {
        let small = ParseLimits {
            max_records: 2,
            max_columns: 3,
            ..ParseLimits::standard()
        };
        assert_eq!(
            json_records_with(b"[1, 2, 3]", small).unwrap_err().code,
            "too_many_records"
        );
        assert_eq!(
            json_records_with(br#"[{"a":1,"b":2,"c":3,"d":4}]"#, small)
                .unwrap_err()
                .code,
            "too_many_columns"
        );
    }

    #[test]
    fn csv_column_cap() {
        let header: Vec<String> = (0..1001).map(|i| format!("c{i}")).collect();
        let body = format!(
            "{}
{}
",
            header.join(","),
            vec!["x"; 1001].join(",")
        );
        assert_eq!(
            csv_records(body.as_bytes()).unwrap_err().code,
            "too_many_columns"
        );
    }

    #[test]
    fn csv_record_cap_stops_while_parsing() {
        let mut body = String::from("id\n");
        for i in 0..(crate::sources::MAX_RECORDS + 5) {
            body.push_str(&format!("{i}\n"));
        }
        assert_eq!(
            csv_records(body.as_bytes()).unwrap_err().code,
            "too_many_records"
        );
    }

    #[test]
    fn csv_materialised_bytes_are_bounded() {
        // 1,000 columns with 200-char names: every cell carries its name, so 400 rows of "x"
        // materialise ~80 MB from a ~0.8 MB file.
        let header: Vec<String> = (0..1000).map(|i| format!("{i:0>200}")).collect();
        let mut body = header.join(",");
        body.push('\n');
        let row = vec!["x"; 1000].join(",");
        for _ in 0..400 {
            body.push_str(&row);
            body.push('\n');
        }
        assert_eq!(
            csv_records(body.as_bytes()).unwrap_err().code,
            "records_too_large"
        );
    }

    #[test]
    fn csv_semicolon_bom_blank_headers_and_ragged_rows() {
        let body = "\u{feff}id;naam;;id\nC-1;Acme;x;dup\n\n;;;\nC-2;\"B; V\"\n".as_bytes();
        assert_eq!(
            csv_records(body).unwrap(),
            vec![
                r(&[
                    ("id", "C-1"),
                    ("naam", "Acme"),
                    ("column_3", "x"),
                    ("id_2", "dup")
                ]),
                r(&[("id", "C-2"), ("naam", "B; V")]),
            ]
        );
        assert_eq!(
            csv_records(b"a,b\n1,2,3\n").unwrap(),
            vec![r(&[("a", "1"), ("b", "2"), ("column_3", "3")])]
        );
        assert!(csv_records(b"").unwrap().is_empty());
    }
}
