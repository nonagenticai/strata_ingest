#![forbid(unsafe_code)]
//! strata-ingest: turns uploaded files and URLs into markdown bundles and delivers them to strata.
//! CPU-only; OCR, captions and transcription are optional HTTP providers.

pub mod api;
pub mod cli;
pub mod config;
pub mod db;
pub mod doc_fields;
pub mod error;
pub mod infra;
pub mod limits;
pub mod metering;
pub mod netguard;
pub mod pipeline;
pub mod providers;
pub mod reingest;
pub mod sources;
pub mod step_clock;
#[cfg(any(test, feature = "testutil"))]
pub mod testutil;
pub mod worker;

pub use error::{IngestError, Result};
