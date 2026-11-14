//! Figure captions, OpenAI chat-completions contract: `POST {base}/chat/completions` with one user
//! message holding the caption prompt and the image as a `data:` URL `image_url` part.

use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use super::{Body, Capability, Http, ProviderError, ProviderHealth, Usage, UsageUnit, data_url};
use crate::config::ProviderCfg;

/// The caption instruction (spec; exact text).
pub const CAPTION_PROMPT: &str = "Describe this figure factually in at most 120 words. Transcribe any visible text, labels and numbers exactly. Do not speculate.";
/// Caption length cap sent as `max_tokens`.
pub const CAPTION_MAX_TOKENS: u32 = 300;

pub struct VisionClient {
    http: Http,
}

impl fmt::Debug for VisionClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VisionClient")
            .field("http", &self.http)
            .finish()
    }
}

#[derive(Deserialize)]
struct ChatResponse {
    #[serde(default)]
    choices: Vec<Choice>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Deserialize)]
struct Message {
    #[serde(default)]
    content: Option<String>,
}

#[derive(Deserialize)]
struct ChatUsage {
    #[serde(default)]
    total_tokens: Option<i64>,
}

impl VisionClient {
    pub fn new(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Vision, &["openai"], cfg, health, false)?,
        })
    }

    /// A tenant-registered provider: connects only to public addresses, no proxy.
    pub fn guarded(cfg: &ProviderCfg, health: Arc<ProviderHealth>) -> crate::Result<Self> {
        Ok(Self {
            http: Http::new(Capability::Vision, &["openai"], cfg, health, true)?,
        })
    }

    /// A factual caption of `image` (`mime` e.g. `image/png`) and its token usage.
    pub async fn caption(
        &self,
        image: &[u8],
        mime: &str,
    ) -> Result<(String, Usage), ProviderError> {
        self.caption_for(None, image, mime).await
    }

    /// [`Self::caption`] for a tenant's step: the call carries `X-Effective-Tenant-Id` (see
    /// [`super::EFFECTIVE_TENANT_HEADER`]).
    pub async fn caption_for(
        &self,
        effective_tenant: Option<Uuid>,
        image: &[u8],
        mime: &str,
    ) -> Result<(String, Usage), ProviderError> {
        let r = self.call(effective_tenant, image, mime).await;
        self.http.record(r)
    }

    async fn call(
        &self,
        effective_tenant: Option<Uuid>,
        image: &[u8],
        mime: &str,
    ) -> Result<(String, Usage), ProviderError> {
        let body = json!({
            "model": self.http.model(),
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": CAPTION_PROMPT},
                    {"type": "image_url", "image_url": {"url": data_url(mime, image)}},
                ],
            }],
            "max_tokens": CAPTION_MAX_TOKENS,
        });
        let bytes = serde_json::to_vec(&body).expect("a json! value always serializes");
        let resp: ChatResponse = self
            .http
            .post("/chat/completions", Body::Json(bytes), effective_tenant)
            .await?;
        let text = resp
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| self.http.invalid_response("no caption text in choices[0]"))?;
        Ok((
            text,
            Usage {
                units: resp.usage.and_then(|u| u.total_tokens),
                unit: UsageUnit::Tokens,
            },
        ))
    }
}

/// Longest text [`is_refusal`] considers: a refusal is a short sentence, a real caption that
/// merely quotes one ("I cannot assist with…" on a pictured sign) is usually longer.
const REFUSAL_MAX_CHARS: usize = 300;

/// Openings that make a short answer a refusal rather than a description.
const REFUSAL_PREFIXES: &[&str] = &[
    "i'm sorry",
    "i am sorry",
    "sorry, but i",
    "sorry, i",
    "i apologize",
    "i apologise",
    "i cannot",
    "i can't",
    "i can not",
    "i'm unable",
    "i am unable",
    "i'm not able",
    "i am not able",
    "as an ai",
];

/// Phrases that make a short answer a refusal wherever they appear.
const REFUSAL_PHRASES: &[&str] = &[
    "cannot assist with",
    "can't assist with",
    "unable to assist",
    "can't help with that",
    "cannot help with that",
    "not able to help with that",
    "unable to help with that",
    "cannot provide a description",
    "can't provide a description",
];

/// Whether a vision answer is a refusal ("I'm sorry, but I cannot assist with that.") instead of
/// a caption. Only texts of at most 300 chars are considered; the check is case-insensitive and
/// treats a curly apostrophe (’) as a straight one. A caption that only quotes such words (a sign
/// reading "Sorry, we're closed") is not a refusal.
pub fn is_refusal(text: &str) -> bool {
    let t = text.trim();
    if t.chars().count() > REFUSAL_MAX_CHARS {
        return false;
    }
    let t = t.to_lowercase().replace('\u{2019}', "'");
    REFUSAL_PREFIXES.iter().any(|p| t.starts_with(p))
        || REFUSAL_PHRASES.iter().any(|p| t.contains(p))
}

#[cfg(test)]
mod tests {
    use super::is_refusal;

    #[test]
    fn refusals_are_recognised() {
        for t in [
            "I'm sorry, but I cannot assist with that.",
            "I\u{2019}m sorry, but I cannot assist with that.",
            "  i'M SoRrY, I can't do that.  \n",
            "I am sorry.",
            "Sorry, but I can't describe this image.",
            "Sorry, I cannot.",
            "I apologize, but this image is unclear.",
            "I apologise.",
            "I cannot describe this figure.",
            "I can\u{2019}t see the image.",
            "I can not help.",
            "I'm unable to view images.",
            "I am unable to process this.",
            "I'm not able to describe it.",
            "I am not able to do that.",
            "As an AI, I cannot see images.",
            "Unfortunately, I cannot assist with that request.",
            "This is something I can't assist with.",
            "Regrettably I am unable to assist.",
            "Sadly I cannot help with that.",
            "I'd rather say I'm not able to help with that.",
            "Hmm, unable to help with that.",
            "Unfortunately I cannot provide a description of this image.",
            "Unfortunately I can\u{2019}t provide a description.",
        ] {
            assert!(is_refusal(t), "{t:?}");
        }
    }

    #[test]
    fn real_captions_are_not_refusals() {
        let long = format!(
            "A slide titled \"Support policy\" listing what the help desk does. {} \
             The last bullet reads \"I cannot assist with hardware repairs\".",
            "Each row names a service, its hours and its contact address. ".repeat(5)
        );
        assert!(long.chars().count() > 300);
        for t in [
            "A sign reading \"Sorry, we're closed\" on a glass door.",
            "A bar chart titled \"I can't believe it's not butter\" sales by year.",
            "A line chart of revenue from 2019 to 2024, rising from 1.2 to 3.4 million.",
            "",
            long.as_str(),
        ] {
            assert!(!is_refusal(t), "{t:?}");
        }
    }
}
