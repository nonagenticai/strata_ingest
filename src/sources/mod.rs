//! Structured sources (`source.kind` = `api` | `datatable`): fetched once while the API request
//! is open (the credentials exist only for that fetch and are never stored), split into records
//! (one JSON array item, or one table row) and rendered into one document per record.
//!
//! A record's text is a readable header of its mapped ontology attributes
//! (`Entity.attribute: value`, one per line), then every field as `name: value`. The same
//! attributes go to strata as document metadata `{"source_id", "attributes": {...}}`.

pub mod fetch;
pub mod records;
pub mod xlsx;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::error::StepError;
use crate::providers::ErrorClass;

/// Most records (documents) one source run may produce.
pub const MAX_RECORDS: usize = 10_000;
/// Most mapping entries one request may carry.
pub const MAX_MAPPINGS: usize = 200;
/// Longest `source_id`, mapping name, entity or attribute (strata_rag's `source_id` limit).
pub const MAX_NAME_CHARS: usize = 200;
/// Longest attribute value carried in the strata metadata; the text keeps the whole value.
pub const MAX_META_VALUE_CHARS: usize = 2000;
/// strata_rag refuses metadata over 64 KiB; above this the attributes stay in the text only.
pub const MAX_META_BYTES: usize = 60 * 1024;

/// One mapping entry: field `source_field` of a record carries ontology attribute
/// `entity.attribute`. For an api record the field name is the flattened JSON path
/// (`address.city`, `lines.0.sku`); for a table row it is the column header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub source_field: String,
    pub entity: String,
    pub attribute: String,
}

/// A record: its fields in source order, `(name, value)`; empty values are left out.
pub type Record = Vec<(String, String)>;

/// Most columns a table (or fields an api record) may have.
pub const MAX_COLUMNS: usize = 1000;
/// Most bytes all records together may materialise (names and values; a shared XLSX string or
/// a column name is counted every time a cell repeats it).
pub const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
/// Excel's own limit on one cell's text, in characters.
pub const MAX_CELL_CHARS: usize = 32_767;
/// How long splitting a fetched body into records may take.
pub const PARSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Bounds of one parse. [`ParseLimits::standard`] in production; tests shrink them.
#[derive(Debug, Clone, Copy)]
pub struct ParseLimits {
    pub max_records: usize,
    pub max_columns: usize,
    pub max_bytes: usize,
    /// Checked while parsing; `None` = no deadline.
    pub deadline: Option<std::time::Instant>,
}

impl ParseLimits {
    /// The production bounds, with a deadline [`PARSE_TIMEOUT`] from now.
    pub fn standard() -> Self {
        Self {
            max_records: MAX_RECORDS,
            max_columns: MAX_COLUMNS,
            max_bytes: MAX_RECORD_BYTES,
            deadline: Some(std::time::Instant::now() + PARSE_TIMEOUT),
        }
    }
}

/// The running cost of a parse against its [`ParseLimits`].
#[derive(Debug)]
pub struct Budget {
    pub limits: ParseLimits,
    bytes: usize,
    ticks: u32,
}

impl Budget {
    pub fn new(limits: ParseLimits) -> Self {
        Self {
            limits,
            bytes: 0,
            ticks: 0,
        }
    }

    /// Charges `n` materialised bytes (before they are allocated).
    pub fn charge(&mut self, n: usize) -> Result<(), SourceError> {
        self.bytes = self.bytes.saturating_add(n);
        if self.bytes > self.limits.max_bytes {
            return Err(SourceError::invalid(
                "records_too_large",
                format!(
                    "the records would take more than {} bytes",
                    self.limits.max_bytes
                ),
            ));
        }
        self.tick()
    }

    /// Checks the deadline every 256 calls.
    pub fn tick(&mut self) -> Result<(), SourceError> {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks.is_multiple_of(256) {
            self.check_deadline()?;
        }
        Ok(())
    }

    pub fn check_deadline(&self) -> Result<(), SourceError> {
        if self
            .limits
            .deadline
            .is_some_and(|d| std::time::Instant::now() >= d)
        {
            return Err(SourceError::invalid(
                "parse_timeout",
                format!(
                    "splitting the source into records took longer than {} s",
                    PARSE_TIMEOUT.as_secs()
                ),
            ));
        }
        Ok(())
    }

    pub fn too_many_columns(&self) -> SourceError {
        SourceError::invalid(
            "too_many_columns",
            format!("a record has more than {} columns", self.limits.max_columns),
        )
    }

    pub fn too_many_records(&self) -> SourceError {
        SourceError::invalid(
            "too_many_records",
            format!(
                "the source holds more than {} records",
                self.limits.max_records
            ),
        )
    }
}

/// A fetch or parse failure. Its message never holds a credential (the fetch scrubs it) and
/// never holds the source body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError {
    pub class: ErrorClass,
    pub code: &'static str,
    pub message: String,
}

impl SourceError {
    pub fn invalid(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::InvalidRequest,
            code,
            message: message.into(),
        }
    }

    pub fn retryable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Retryable,
            code,
            message: message.into(),
        }
    }

    /// As the job's `error` (provider `source`).
    pub fn step_error(&self) -> StepError {
        StepError {
            class: self.class,
            code: self.code.to_string(),
            message: self.message.clone(),
            provider: Some("source".to_string()),
            retry_after_s: None,
        }
    }
}

/// A record rendered for delivery.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordDoc {
    pub title: String,
    pub text: String,
    /// strata's `source` label.
    pub source: String,
    /// strata's document metadata; `None` when there is nothing to say.
    pub metadata: Option<Value>,
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn cut(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Renders record number `n` (1-based) of a source. `noun` names a record in titles (`Item`,
/// `Row`); `label` is the strata `source` prefix (the source URL without its query).
pub fn render(
    record: &Record,
    mapping: &[Mapping],
    n: usize,
    noun: &str,
    label: &str,
    source_id: Option<&str>,
) -> RecordDoc {
    let value_of = |field: &str| {
        record
            .iter()
            .find(|(k, _)| k == field)
            .map(|(_, v)| v.as_str())
    };
    let mut header = Vec::new();
    let mut attributes = Map::new();
    for m in mapping {
        if let Some(v) = value_of(&m.source_field) {
            let key = format!("{}.{}", m.entity, m.attribute);
            header.push(format!("{key}: {}", one_line(v)));
            attributes.insert(key, json!(cut(v, MAX_META_VALUE_CHARS)));
        }
    }
    let title = mapping
        .iter()
        .find_map(|m| value_of(&m.source_field).map(|v| (m, v)))
        .map(|(m, v)| format!("{} {}", m.entity, cut(&one_line(v), 120)))
        .unwrap_or_else(|| format!("{noun} {n}"));
    let fields: Vec<String> = record.iter().map(|(k, v)| format!("{k}: {v}")).collect();
    let text = if header.is_empty() {
        fields.join("\n")
    } else {
        format!("{}\n\n{}", header.join("\n"), fields.join("\n"))
    };
    let mut meta = Map::new();
    if let Some(sid) = source_id {
        meta.insert("source_id".into(), json!(sid));
    }
    if !attributes.is_empty() {
        let with = Value::Object(attributes);
        if serde_json::to_vec(&with).map_or(usize::MAX, |b| b.len()) <= MAX_META_BYTES {
            meta.insert("attributes".into(), with);
        }
    }
    RecordDoc {
        title,
        text,
        source: format!("{label}#{}-{n}", noun.to_ascii_lowercase()),
        metadata: (!meta.is_empty()).then_some(Value::Object(meta)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(fields: &[(&str, &str)]) -> Record {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn map(f: &str, e: &str, a: &str) -> Mapping {
        Mapping {
            source_field: f.into(),
            entity: e.into(),
            attribute: a.into(),
        }
    }

    #[test]
    fn mapped_attributes_lead_the_text_and_fill_the_metadata() {
        let r = rec(&[
            ("customer_id", "C-10482"),
            ("name", "Acme\n BV"),
            ("city", "Utrecht"),
        ]);
        let m = [
            map("customer_id", "Customer", "customer_id"),
            map("name", "Customer", "name"),
            map("missing", "Customer", "vat"),
        ];
        let d = render(
            &r,
            &m,
            3,
            "Item",
            "https://api.example.com/customers",
            Some("src-1"),
        );
        assert_eq!(
            d.text,
            "Customer.customer_id: C-10482\nCustomer.name: Acme BV\n\n\
             customer_id: C-10482\nname: Acme\n BV\ncity: Utrecht"
        );
        assert_eq!(d.title, "Customer C-10482");
        assert_eq!(d.source, "https://api.example.com/customers#item-3");
        assert_eq!(
            d.metadata,
            Some(json!({"source_id": "src-1", "attributes": {
                "Customer.customer_id": "C-10482", "Customer.name": "Acme\n BV"}}))
        );
    }

    #[test]
    fn without_mapping_the_text_is_the_fields() {
        let d = render(
            &rec(&[("a", "1")]),
            &[],
            7,
            "Row",
            "https://x.example/t.csv",
            None,
        );
        assert_eq!(d.text, "a: 1");
        assert_eq!(d.title, "Row 7");
        assert_eq!(d.metadata, None);
    }
}
