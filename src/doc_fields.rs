//! A document's platform-knowledge fields, as strata_rag's JSON ingest takes them (strata_rag
//! `src/ingest.rs` `KnowledgeTag`, README "Document sources and re-sync"):
//!
//! - `topic_keys`: topic keys the document covers, at most [`MAX_TOPIC_KEYS`] keys of
//!   1..=[`MAX_TOPIC_KEY_CHARS`] chars of `[a-z0-9_./-]`;
//! - `supersedes`: topic keys the document overrides, same limits;
//! - `provenance`: `{source_url, license, retrieved_at}`, each non-blank (trimmed) and at most
//!   [`MAX_PROVENANCE_FIELD_CHARS`] chars, `retrieved_at` RFC 3339. strata stores it as
//!   `metadata.provenance` and, for a platform-owned store, requires it with an allow-listed
//!   licence (422 at delivery otherwise: step code `provenance_rejected`).
//!
//! Every job kind takes them (request level; a `urls` item may override them per URL). They are
//! validated here with strata's limits, so a bad value is a 400 before any job exists, kept in
//! the job's `meta` under the same names (crawl / sitemap children inherit them) and sent on
//! every delivery. Left out → not sent, so strata keeps a re-ingested document's stored values.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{IngestError, Result};

/// Most keys in each of `topic_keys` and `supersedes` (strata's limit).
pub const MAX_TOPIC_KEYS: usize = 32;
/// Longest topic key, in chars (strata's limit).
pub const MAX_TOPIC_KEY_CHARS: usize = 128;
/// Longest `provenance.*` field, in chars (strata's limit).
pub const MAX_PROVENANCE_FIELD_CHARS: usize = 2048;
/// The job `meta` keys these fields are kept under, which are also strata's body fields.
pub const DOC_FIELD_KEYS: [&str; 3] = ["topic_keys", "supersedes", "provenance"];

/// `provenance` as received; every field required, no others.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceIn {
    pub source_url: String,
    pub license: String,
    pub retrieved_at: String,
}

/// A validated `provenance` (fields trimmed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Provenance {
    pub source_url: String,
    pub license: String,
    pub retrieved_at: String,
}

/// The validated fields of a document (`None` = not given).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DocFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_keys: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
}

fn bad(msg: String) -> IngestError {
    IngestError::BadRequest(msg)
}

/// A topic key: 1..=[`MAX_TOPIC_KEY_CHARS`] of `[a-z0-9_./-]` (strata's `valid_topic_key`).
pub fn valid_topic_key(k: &str) -> bool {
    !k.is_empty()
        && k.len() <= MAX_TOPIC_KEY_CHARS
        && k.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'/' | b'-')
        })
}

/// `name` (`topic_keys` / `supersedes`): at most [`MAX_TOPIC_KEYS`] valid keys, returned sorted
/// and deduplicated (as strata stores them); 400 otherwise.
pub fn topic_keys(name: &str, keys: Vec<String>) -> Result<Vec<String>> {
    if keys.len() > MAX_TOPIC_KEYS {
        return Err(bad(format!(
            "`{name}` holds {} keys; the limit is {MAX_TOPIC_KEYS}",
            keys.len()
        )));
    }
    if let Some(k) = keys.iter().find(|k| !valid_topic_key(k)) {
        let shown: String = k.chars().take(MAX_TOPIC_KEY_CHARS + 1).collect();
        return Err(bad(format!(
            "`{name}`: {shown:?} is not a topic key (1 to {MAX_TOPIC_KEY_CHARS} characters of \
             [a-z0-9_./-])"
        )));
    }
    let mut keys = keys;
    keys.sort();
    keys.dedup();
    Ok(keys)
}

impl ProvenanceIn {
    /// Trims each field; each must be non-blank and at most [`MAX_PROVENANCE_FIELD_CHARS`]
    /// chars, and `retrieved_at` an RFC 3339 timestamp (400 otherwise).
    pub fn validate(self) -> Result<Provenance> {
        let field = |name: &str, v: String| -> Result<String> {
            let v = v.trim();
            if v.is_empty() {
                return Err(bad(format!("`provenance.{name}` is required")));
            }
            if v.chars().count() > MAX_PROVENANCE_FIELD_CHARS {
                return Err(bad(format!(
                    "`provenance.{name}` must be at most {MAX_PROVENANCE_FIELD_CHARS} characters"
                )));
            }
            Ok(v.to_string())
        };
        let p = Provenance {
            source_url: field("source_url", self.source_url)?,
            license: field("license", self.license)?,
            retrieved_at: field("retrieved_at", self.retrieved_at)?,
        };
        if chrono::DateTime::parse_from_rfc3339(&p.retrieved_at).is_err() {
            return Err(bad(
                "`provenance.retrieved_at` must be an RFC 3339 timestamp such as \
                 2026-10-21T08:00:00Z"
                    .to_string(),
            ));
        }
        Ok(p)
    }
}

impl DocFields {
    /// Validates the three fields as received (400 on the first bad one).
    pub fn parse(
        topic_keys_in: Option<Vec<String>>,
        supersedes_in: Option<Vec<String>>,
        provenance_in: Option<ProvenanceIn>,
    ) -> Result<DocFields> {
        Ok(DocFields {
            topic_keys: topic_keys_in
                .map(|k| topic_keys("topic_keys", k))
                .transpose()?,
            supersedes: supersedes_in
                .map(|k| topic_keys("supersedes", k))
                .transpose()?,
            provenance: provenance_in.map(ProvenanceIn::validate).transpose()?,
        })
    }

    /// `self` with every field `over` gives replacing this one's.
    pub fn overlay(&self, over: &DocFields) -> DocFields {
        DocFields {
            topic_keys: over.topic_keys.clone().or_else(|| self.topic_keys.clone()),
            supersedes: over.supersedes.clone().or_else(|| self.supersedes.clone()),
            provenance: over.provenance.clone().or_else(|| self.provenance.clone()),
        }
    }

    /// Puts the given fields into a job's `meta` (under [`DOC_FIELD_KEYS`]).
    pub fn insert_into(&self, meta: &mut Map<String, Value>) {
        if let Ok(Value::Object(m)) = serde_json::to_value(self) {
            meta.extend(m);
        }
    }

    /// [`Self::insert_into`] for a `meta` value (an object; anything else is left alone).
    pub fn add_to(&self, meta: &mut Value) {
        if let Some(m) = meta.as_object_mut() {
            self.insert_into(m);
        }
    }
}

/// Parses a multipart text field holding `name`'s JSON (`topic_keys` / `supersedes`: an array of
/// strings; `provenance`: an object): 400 when it is not that JSON.
pub fn from_json_text<T: serde::de::DeserializeOwned>(name: &str, text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(|e| bad(format!("`{name}` is not valid: {e}")))
}

/// What a Deliver step sends besides the document: the job's `meta` fields (the caller's), with
/// a re-ingest copy's `knowledge` (its source document's fields) taking precedence key by key.
/// No fields in `meta` → `knowledge` unchanged.
pub fn with_job_fields(meta: &Value, knowledge: Option<Value>) -> Option<Value> {
    let mut m = Map::new();
    for key in DOC_FIELD_KEYS {
        if let Some(v) = meta.get(key).filter(|v| !v.is_null()) {
            m.insert(key.into(), v.clone());
        }
    }
    if m.is_empty() {
        return knowledge;
    }
    match knowledge {
        Some(Value::Object(k)) => m.extend(k),
        Some(other) => return Some(other),
        None => {}
    }
    Some(Value::Object(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn prov(url: &str, lic: &str, at: &str) -> ProvenanceIn {
        ProvenanceIn {
            source_url: url.into(),
            license: lic.into(),
            retrieved_at: at.into(),
        }
    }

    #[test]
    fn keys_follow_strata_limits() {
        assert_eq!(
            topic_keys("topic_keys", vec!["b/x".into(), "a.1".into(), "b/x".into()]).unwrap(),
            vec!["a.1".to_string(), "b/x".to_string()]
        );
        assert!(topic_keys("topic_keys", vec![]).unwrap().is_empty());
        let long = "a".repeat(129);
        for k in ["", "Upper", "sp ace", "ü", long.as_str()] {
            assert!(
                topic_keys("topic_keys", vec![k.to_string()]).is_err(),
                "{k}"
            );
        }
        assert!(topic_keys("supersedes", vec!["a".repeat(128)]).is_ok());
        let many: Vec<String> = (0..33).map(|i| format!("k{i}")).collect();
        assert!(topic_keys("supersedes", many).is_err());
    }

    #[test]
    fn provenance_is_trimmed_and_checked() {
        let p = prov(
            " https://x.eu/a ",
            " CC-BY-4.0",
            "2026-10-21T08:00:00+02:00",
        )
        .validate()
        .unwrap();
        assert_eq!(p.source_url, "https://x.eu/a");
        assert_eq!(p.license, "CC-BY-4.0");
        for bad in [
            prov(" ", "l", "2026-10-21T08:00:00Z"),
            prov("u", "", "2026-10-21T08:00:00Z"),
            prov("u", "l", "yesterday"),
            prov("u", "l", "2026-10-21"),
            prov(&"u".repeat(2049), "l", "2026-10-21T08:00:00Z"),
        ] {
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn overlay_meta_and_delivery_fields() {
        let base = DocFields::parse(
            Some(vec!["a".into()]),
            None,
            Some(prov("u", "l", "2026-10-21T08:00:00Z")),
        )
        .unwrap();
        let over = DocFields::parse(Some(vec!["b".into()]), Some(vec![]), None).unwrap();
        let both = base.overlay(&over);
        let mut meta = json!({"source_id": "s"});
        both.add_to(&mut meta);
        assert_eq!(
            meta,
            json!({"source_id": "s", "topic_keys": ["b"], "supersedes": [],
                   "provenance": {"source_url": "u", "license": "l",
                                  "retrieved_at": "2026-10-21T08:00:00Z"}})
        );
        // Nothing in meta → the copy's knowledge (or none) unchanged.
        assert_eq!(with_job_fields(&json!({"source_id": "s"}), None), None);
        let k = json!({"metadata": {}, "topic_keys": ["c"]});
        assert_eq!(
            with_job_fields(&json!({}), Some(k.clone())),
            Some(k.clone())
        );
        // meta fields, a copy's knowledge winning per key.
        assert_eq!(
            with_job_fields(&meta, Some(k)),
            Some(
                json!({"topic_keys": ["c"], "supersedes": [], "metadata": {},
                        "provenance": meta["provenance"]})
            )
        );
    }
}
