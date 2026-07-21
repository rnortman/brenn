use serde::{Deserialize, Serialize};

/// Warnings collected during a run, surfaced via stderr and in the JSON Report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Warning {
    pub kind: WarningKind,
    pub message: String,
    pub context: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningKind {
    MalformedLine,
    SchemaMismatch,
    UnknownModel,
    OrphanAgentId,
    /// Non-fatal filesystem error (e.g. mtime read failure during recency scan).
    IoWarning,
}

impl Warning {
    pub fn malformed_line(file: &str, line_no: usize, error: &str) -> Self {
        Self {
            kind: WarningKind::MalformedLine,
            message: format!("malformed JSON on line {line_no} of {file}: {error}"),
            context: serde_json::json!({
                "file": file,
                "line_no": line_no,
                "error": error,
            }),
        }
    }

    pub fn schema_mismatch(file: &str, line_no: usize, detail: &str) -> Self {
        Self {
            kind: WarningKind::SchemaMismatch,
            message: format!("schema mismatch on line {line_no} of {file}: {detail}"),
            context: serde_json::json!({
                "file": file,
                "line_no": line_no,
                "detail": detail,
            }),
        }
    }

    pub fn unknown_model(model: &str) -> Self {
        Self {
            kind: WarningKind::UnknownModel,
            message: format!("unknown model '{model}': cost will be null"),
            context: serde_json::json!({ "model": model }),
        }
    }

    pub fn orphan_agent_id(session_id: &str, agent_id: &str) -> Self {
        Self {
            kind: WarningKind::OrphanAgentId,
            message: format!(
                "agentId '{agent_id}' in session '{session_id}' not found in parent file; \
                 classified as 'unknown'"
            ),
            context: serde_json::json!({
                "session_id": session_id,
                "agent_id": agent_id,
            }),
        }
    }
}
