use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

/// Raw deserialized form of a single JSONL line. Only fields we care about
/// are bound; everything else is ignored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRecord {
    #[serde(rename = "type")]
    pub record_type: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub session_id: Option<String>,
    pub is_sidechain: Option<bool>,
    pub agent_id: Option<String>,
    pub message: Option<RawMessage>,
    /// Raw toolUseResult — may be a string (error/rejection) or an object
    /// (completed/launched agent). Only the object form carries agentId.
    pub tool_use_result: Option<serde_json::Value>,
    pub agent_setting: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RawMessage {
    pub model: Option<String>,
    pub usage: Option<RawUsage>,
    /// Content can be a string (e.g. task-notification XML embedded by the
    /// agent harness) or an array of structured content blocks. We only care
    /// about the array case; a string is silently ignored.
    #[serde(default, deserialize_with = "deserialize_content")]
    pub content: Option<Vec<RawContentBlock>>,
}

#[derive(Debug, Deserialize)]
pub struct RawUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation: Option<RawCacheCreation>,
}

#[derive(Debug, Deserialize)]
pub struct RawCacheCreation {
    pub ephemeral_5m_input_tokens: Option<u64>,
    pub ephemeral_1h_input_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct RawContentBlock {
    #[serde(rename = "type")]
    pub block_type: Option<String>,
    pub name: Option<String>,
    /// Kept as Value for lazy subagent_type/description extraction.
    pub input: Option<serde_json::Value>,
    /// Present on tool_use blocks (the block's id used by tool_result to correlate).
    pub id: Option<String>,
    /// Present on tool_result blocks; correlates back to the tool_use block id.
    pub tool_use_id: Option<String>,
}

/// Deserialize a message `content` field that can be either a JSON array of
/// content blocks or a plain string (e.g. task-notification XML embedded by
/// the agent harness). Array → `Some(blocks)`; string or null → `None`.
fn deserialize_content<'de, D>(deserializer: D) -> Result<Option<Vec<RawContentBlock>>, D::Error>
where
    D: Deserializer<'de>,
{
    let val = Value::deserialize(deserializer)?;
    match val {
        Value::Array(arr) => {
            let blocks: Result<Vec<RawContentBlock>, _> = arr
                .into_iter()
                .map(|v| serde_json::from_value(v).map_err(serde::de::Error::custom))
                .collect();
            Ok(Some(blocks?))
        }
        Value::Null | Value::String(_) => Ok(None),
        other => Err(serde::de::Error::custom(format!(
            "expected array or string for content, got {}",
            other
        ))),
    }
}
