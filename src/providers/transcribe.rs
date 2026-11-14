//! Transcription, OpenAI audio contract: `POST {base}/audio/transcriptions` as multipart
//! (`file`, `model`, `response_format=verbose_json`, `timestamp_granularities[]=segment`,
//! `language`?), answered by `{duration, segments: [{start, end, text}], usage: {seconds}}`.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use uuid::Uuid;

use super::{Body, Capability, Http, ProviderError, ProviderHealth, Usage, UsageUnit};
use crate::config::ProviderCfg;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Segment {
    pub start_s: f64,
    pub end_s: f64,
    pub text: String,
}

/// Ordered segments; `usage` in whole seconds (`usage.seconds` rounded up).
#[derive(Debug, Clone, PartialEq)]
pub struct Transcript {
    pub segments: Vec<Segment>,
    pub duration_s: Option<f64>,
    pub usage: Usage,
}

pub struct TranscribeClient {
    http: Http,
}

impl fmt::Debug for TranscribeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TranscribeClient")
            .field("http", &self.http)
            .finish()
    }
}

#[derive(Deserialize)]
struct VerboseResponse {
    #[serde(default)]
    duration: Option<f64>,
    segments: Vec<RawSegment>,
    #[serde(default)]
    usage: Option<AudioUsage>,
}

#[derive(Deserialize)]
struct RawSegment {
    start: f64,
    end: f64,
    text: String,
}

#[derive(Deserialize)]
struct AudioUsage {
    #[serde(default)]
    seconds: Option<f64>,
}

/// A content type for the upload from the file name; providers mostly sniff, but some check it.
fn audio_mime(filename: &str) -> &'static str {
    let ext = filename
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "wav" => "audio/wav",
        "mp3" | "mpga" | "mpeg" => "audio/mpeg",
        "m4a" | "mp4" => "audio/mp4",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "flac" => "audio/flac",
        "webm" => "audio/webm",
        _ => "application/octet-stream",
    }
}

/// Billable whole seconds: `ceil(seconds)`; unknown for a missing, negative or non-finite value.
fn billable_seconds(seconds: Option<f64>) -> Option<i64> {
    let s = seconds?;
    // `as` saturates; anything near i64::MAX is nonsense either way.
    (s.is_finite() && s >= 0.0).then(|| s.ceil() as i64)
}

impl TranscribeClient {
    pub fn new(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Transcribe, &["openai"], cfg, health, false)?,
        })
    }

    /// A tenant-registered provider: connects only to public addresses, no proxy.
    pub fn guarded(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Transcribe, &["openai"], cfg, health, true)?,
        })
    }

    /// Transcribes `audio` (sent as `filename`); `language` is an ISO-639-1 hint.
    pub async fn transcribe(
        &self,
        audio: Bytes,
        filename: &str,
        language: Option<&str>,
    ) -> Result<Transcript, ProviderError> {
        self.transcribe_for(None, audio, filename, language).await
    }

    /// [`Self::transcribe`] for a tenant's step: the call carries `X-Effective-Tenant-Id` (see
    /// [`super::EFFECTIVE_TENANT_HEADER`]) — this is the call that decides which tenant the
    /// receiving service's usage event names.
    pub async fn transcribe_for(
        &self,
        effective_tenant: Option<Uuid>,
        audio: Bytes,
        filename: &str,
        language: Option<&str>,
    ) -> Result<Transcript, ProviderError> {
        let r = self.call(effective_tenant, audio, filename, language).await;
        self.http.record(r)
    }

    async fn call(
        &self,
        effective_tenant: Option<Uuid>,
        audio: Bytes,
        filename: &str,
        language: Option<&str>,
    ) -> Result<Transcript, ProviderError> {
        let len = audio.len() as u64;
        let file = Part::stream_with_length(reqwest::Body::from(audio), len)
            .file_name(filename.to_string())
            .mime_str(audio_mime(filename))
            .expect("static mime strings parse");
        let mut form = Form::new()
            .part("file", file)
            .text("model", self.http.model().to_string())
            .text("response_format", "verbose_json")
            .text("timestamp_granularities[]", "segment");
        if let Some(lang) = language {
            form = form.text("language", lang.to_string());
        }
        let resp: VerboseResponse = self
            .http
            .post(
                "/audio/transcriptions",
                Body::Multipart(form),
                effective_tenant,
            )
            .await?;
        let mut prev_end = 0.0_f64;
        let mut segments = Vec::with_capacity(resp.segments.len());
        for s in resp.segments {
            if !(s.start.is_finite() && s.end.is_finite() && s.start >= 0.0 && s.end >= s.start) {
                return Err(self.http.invalid_response(format!(
                    "segment [{}, {}] is not a valid interval",
                    s.start, s.end
                )));
            }
            if s.start + 1e-6 < prev_end {
                return Err(self.http.invalid_response("segments are not ordered"));
            }
            prev_end = s.end;
            segments.push(Segment {
                start_s: s.start,
                end_s: s.end,
                text: s.text,
            });
        }
        Ok(Transcript {
            segments,
            duration_s: resp.duration,
            usage: Usage {
                units: billable_seconds(resp.usage.and_then(|u| u.seconds)),
                unit: UsageUnit::Seconds,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn billable_seconds_rounds_up_and_rejects_nonsense() {
        assert_eq!(billable_seconds(Some(12.3)), Some(13));
        assert_eq!(billable_seconds(Some(120.0)), Some(120));
        assert_eq!(billable_seconds(Some(0.0)), Some(0));
        assert_eq!(billable_seconds(Some(-1.0)), None);
        assert_eq!(billable_seconds(Some(f64::NAN)), None);
        assert_eq!(billable_seconds(None), None);
    }

    #[test]
    fn audio_mime_by_extension() {
        assert_eq!(audio_mime("a.WAV"), "audio/wav");
        assert_eq!(audio_mime("talk.mp3"), "audio/mpeg");
        assert_eq!(audio_mime("noext"), "application/octet-stream");
    }
}
