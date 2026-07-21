//! Types for messages received from CC's stdout (CC → Brenn).

use brenn_lib::ws_types::PermissionModeValue;
use serde::{Deserialize, Serialize};

/// A message received from CC's stdout.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CcIncoming {
    #[serde(rename = "control_response")]
    ControlResponse { response: ControlResponsePayload },

    #[serde(rename = "system")]
    System(SystemMessage),

    #[serde(rename = "assistant")]
    Assistant(AssistantMessage),

    #[serde(rename = "stream_event")]
    StreamEvent(StreamEventMessage),

    #[serde(rename = "user")]
    User(UserMessage),

    #[serde(rename = "control_request")]
    ControlRequest {
        request_id: String,
        request: CcControlRequest,
    },

    #[serde(rename = "result")]
    Result(ResultMessage),

    #[serde(rename = "rate_limit_event")]
    RateLimitEvent(RateLimitEventMessage),

    #[serde(rename = "control_cancel_request")]
    ControlCancelRequest { request_id: String },
}

// --- Control Response ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponsePayload {
    pub subtype: String,
    pub request_id: Option<String>,
    #[serde(default)]
    pub response: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

// --- System ---

/// System messages from CC, discriminated by `subtype`.
///
/// `Init` is the only subtype we actively handle. Unknown subtypes deserialize
/// into `Unknown` via `#[serde(other)]` — these are fire-and-forget from CC's
/// side (no response expected), so it's safe to accept and alert on them rather
/// than failing to parse.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subtype")]
pub enum SystemMessage {
    /// Session initialization message. Contains session metadata.
    #[serde(rename = "init")]
    Init {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        tools: Option<Vec<String>>,
        #[serde(default)]
        mcp_servers: Option<Vec<McpServerStatus>>,
        #[serde(default)]
        model: Option<String>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        claude_code_version: Option<String>,
        /// Field name is `permissionMode` on the wire — CC uses camelCase
        /// for this one field (most others are snake_case). Observed in
        /// init frames from CC >= 2.1.111. If CC ever renames or drops
        /// the field, `handle_initialized` fires a missing-field alert.
        /// Any string deserializes successfully: known value is `Auto`;
        /// unknown values land in `Other(s)`.
        #[serde(default, rename = "permissionMode")]
        permission_mode: Option<PermissionModeValue>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Status update (e.g. "compacting" during `/compact`).
    #[serde(rename = "status")]
    Status {
        /// The status string, or `None` when the status is cleared.
        status: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Compact boundary marker — emitted after compaction completes.
    #[serde(rename = "compact_boundary")]
    CompactBoundary {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        compact_metadata: Option<CompactMetadata>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Task tool lifecycle: task spawned. Informational — Brenn's model is
    /// turn-oriented and these describe internal progress of a `Task` tool's
    /// local-agent / local-bash work. Parsed + logged + dropped. Fields are
    /// modelled loosely (all optional, `extra` flatten) for protocol
    /// resilience, matching the CompactBoundary pattern. Python SDK 0.1.59
    /// treats these three as required-field typed messages, but we don't
    /// surface them to the UI so Brenn can be more lenient.
    #[serde(rename = "task_started")]
    TaskStarted {
        #[serde(default)]
        task_id: Option<String>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        tool_use_id: Option<String>,
        #[serde(default)]
        task_type: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Task tool lifecycle: progress update.
    #[serde(rename = "task_progress")]
    TaskProgress {
        #[serde(default)]
        task_id: Option<String>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        last_tool_name: Option<String>,
        #[serde(default)]
        tool_use_id: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        // `usage` (total_tokens / tool_uses / duration_ms) preserved in extra;
        // no Brenn consumer today.
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Task tool lifecycle: task completed / failed / stopped.
    #[serde(rename = "task_notification")]
    TaskNotification {
        #[serde(default)]
        task_id: Option<String>,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        output_file: Option<String>,
        #[serde(default)]
        summary: Option<String>,
        #[serde(default)]
        tool_use_id: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Task tool lifecycle: sparse patch update. `patch` is opaque JSON —
    /// the Python SDK doesn't type this either (treated as generic
    /// SystemMessage there). Keep the raw payload for forensics.
    #[serde(rename = "task_updated")]
    TaskUpdated {
        #[serde(default)]
        task_id: Option<String>,
        #[serde(default)]
        patch: Option<serde_json::Value>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    /// Unknown system subtype. CC may add new subtypes at any time — these are
    /// informational (no response expected), so we accept them and alert.
    #[serde(other)]
    Unknown,
}

/// Metadata from a compact_boundary message.
///
/// Fields are `Option` for CC protocol resilience — CC may change the
/// compact_boundary shape in future versions. Consumers should handle
/// `None` gracefully (log but don't panic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMetadata {
    /// What triggered the compaction (e.g. "manual", "auto").
    #[serde(default)]
    pub trigger: Option<String>,
    /// Token count before compaction.
    #[serde(default)]
    pub pre_tokens: Option<u64>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerStatus {
    pub name: String,
    pub status: String,
}

// --- Assistant ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub message: AssistantContent,
    pub uuid: String,
    #[serde(default)]
    pub parent_tool_use_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantContent {
    pub role: String,
    pub content: Vec<ContentBlock>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "thinking")]
    Thinking {
        thinking: String,
        #[serde(default)]
        signature: Option<String>,
    },
    /// Unknown content block type — CC may introduce new ones.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    /// Tokens served from the prompt cache on this turn.
    #[serde(default)]
    pub cache_read_input_tokens: Option<u64>,
    /// Tokens written into the prompt cache on this turn. The wire may carry
    /// a nested `cache_creation` object (with ephemeral sub-fields); the
    /// total is surfaced here as a flat field and the nested object lands in
    /// `extra` for forensics.
    #[serde(default)]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

// --- Stream Event ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventMessage {
    pub uuid: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub event: serde_json::Value,
}

// --- User (tool results from CC) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMessage {
    pub message: serde_json::Value,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub tool_use_result: Option<serde_json::Value>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

// --- Control Request from CC ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "subtype")]
pub enum CcControlRequest {
    #[serde(rename = "can_use_tool")]
    CanUseTool {
        tool_name: String,
        tool_use_id: String,
        input: serde_json::Value,
        #[serde(default)]
        permission_suggestions: Option<Vec<serde_json::Value>>,
        #[serde(default)]
        decision_reason: Option<String>,
        #[serde(flatten)]
        extra: serde_json::Value,
    },
    #[serde(rename = "hook_callback")]
    HookCallback {
        callback_id: String,
        #[serde(default)]
        tool_use_id: Option<String>,
        input: HookInput,
    },
    // No #[serde(other)] here deliberately. An unknown control request subtype
    // means CC is asking us something we can't answer — we'd have to guess at
    // the response format. Per philosophy, we kill the session rather than risk
    // doing the wrong thing. The parse failure path in tasks.rs handles this by
    // detecting "type": "control_request" in the raw JSON and killing the session.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookInput {
    pub hook_event_name: String,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_input: Option<serde_json::Value>,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub tool_response: Option<serde_json::Value>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

// --- Result ---

/// Per-model usage entry from `result.modelUsage`. Keys in the map are model
/// slugs as they appear in the CC stream, including any `[1m]` suffix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelUsageEntry {
    /// Authoritative max context window for this model (e.g. 200_000, 1_000_000).
    ///
    /// `Option` so that `null` or a missing field still lets the surrounding
    /// `ResultMessage` parse (rather than routing the whole frame to
    /// `UnrecognizedMessage`). Consumers must check: the active model's entry
    /// with `context_window == None` is a protocol violation and must panic.
    /// Non-active (subagent) entries with `None` are logged at WARN and
    /// skipped.
    #[serde(default, rename = "contextWindow")]
    pub context_window: Option<u64>,
    /// Maximum output tokens for this model.
    #[serde(default, rename = "maxOutputTokens")]
    pub max_output_tokens: Option<u64>,
    /// Per-model cumulative cost in USD.
    #[serde(default, rename = "costUSD")]
    pub cost_usd: Option<f64>,
    /// Other fields (inputTokens, outputTokens, cacheReadInputTokens,
    /// cacheCreationInputTokens) are not consumed by Brenn at this time.
    /// Captured in `extra` for future features.
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

/// Per-turn origin stamp CC places on autonomous result frames.
///
/// Absent on ordinary (Brenn-initiated) turn results; present on turns CC runs
/// on its own initiative (e.g. a `task-notification` turn after a background
/// subagent completes). `kind` is a plain `String`, never an enum: CC may add
/// kinds without notice and parsing must never fail on an unknown one.
///
/// Identifier trap: the result-frame kind is `"task-notification"` (hyphen),
/// distinct from the `task_notification` (underscore) system-message subtype.
/// They are different strings in different frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultOrigin {
    pub kind: String,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultMessage {
    #[serde(default)]
    pub subtype: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub duration_api_ms: Option<u64>,
    #[serde(default)]
    pub is_error: Option<bool>,
    #[serde(default)]
    pub num_turns: Option<u64>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub total_cost_usd: Option<f64>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub stop_reason: Option<String>,
    /// Per-model usage map. Keys are model slugs as they appear in `system/init`,
    /// including any `[1m]` suffix. Absent on some compaction-result frames;
    /// present on regular turn-completion frames in CC >= 2.1.123.
    #[serde(default, rename = "modelUsage")]
    pub model_usage: Option<std::collections::HashMap<String, ModelUsageEntry>>,
    /// Origin stamp on CC-autonomous turns. Absent on Brenn-initiated turns.
    /// The compaction state machine uses this to distinguish a background turn
    /// (e.g. `task-notification`) from the foreground turn it is waiting on.
    #[serde(default)]
    pub origin: Option<ResultOrigin>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

// --- Rate Limit Event ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitEventMessage {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub rate_limit_info: Option<serde_json::Value>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_system_init() {
        // CC sends `permissionMode` (camelCase) for this one field — verified
        // in staging ndjson. Other init fields remain snake_case.
        let json = r#"{
            "type": "system",
            "subtype": "init",
            "session_id": "abc-123",
            "tools": ["Read", "Write", "Bash"],
            "mcp_servers": [{"name": "pfin", "status": "connected"}],
            "model": "claude-opus-4-6-20250528",
            "cwd": "/home/user/project",
            "claude_code_version": "2.0.1",
            "permissionMode": "default"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse system init");
        match msg {
            CcIncoming::System(SystemMessage::Init {
                session_id,
                tools,
                model,
                cwd,
                mcp_servers,
                permission_mode,
                ..
            }) => {
                assert_eq!(session_id.as_deref(), Some("abc-123"));
                assert_eq!(tools.as_ref().unwrap().len(), 3);
                assert_eq!(model.as_deref(), Some("claude-opus-4-6-20250528"));
                assert_eq!(cwd.as_deref(), Some("/home/user/project"));
                let servers = mcp_servers.as_ref().unwrap();
                assert_eq!(servers.len(), 1);
                assert_eq!(servers[0].name, "pfin");
                assert_eq!(
                    permission_mode,
                    Some(PermissionModeValue::Other("default".into()))
                );
            }
            other => panic!("expected System/Init, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_init_auto_permission_mode() {
        // CC production path: `permissionMode: "auto"` → `PermissionModeValue::Auto`.
        let json = r#"{
            "type": "system",
            "subtype": "init",
            "session_id": "abc-123",
            "tools": ["Read"],
            "model": "claude-opus-4-6-20250528",
            "cwd": "/home/user/project",
            "permissionMode": "auto"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse system init");
        match msg {
            CcIncoming::System(SystemMessage::Init {
                permission_mode, ..
            }) => {
                assert_eq!(permission_mode, Some(PermissionModeValue::Auto));
            }
            other => panic!("expected System/Init, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_init_missing_permission_mode() {
        // CC omits permission_mode entirely — must deserialize to None.
        let json = r#"{
            "type": "system",
            "subtype": "init",
            "session_id": "abc-123",
            "tools": ["Read"],
            "model": "claude-opus-4-6-20250528",
            "cwd": "/home/user/project"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse system init");
        match msg {
            CcIncoming::System(SystemMessage::Init {
                permission_mode, ..
            }) => {
                assert!(permission_mode.is_none());
            }
            other => panic!("expected System/Init, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_init_null_permission_mode() {
        // Explicit `"permissionMode": null` must deserialize to None too,
        // matching the field-omitted case (edge case 7 in the design).
        let json = r#"{
            "type": "system",
            "subtype": "init",
            "session_id": "abc-123",
            "tools": ["Read"],
            "model": "claude-opus-4-6-20250528",
            "cwd": "/home/user/project",
            "permissionMode": null
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse system init");
        match msg {
            CcIncoming::System(SystemMessage::Init {
                permission_mode, ..
            }) => {
                assert!(permission_mode.is_none());
            }
            other => panic!("expected System/Init, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_init_snake_case_permission_mode_does_not_match() {
        // Regression guard: staging shipped broken because the initial
        // design assumed CC sent `permission_mode` (snake_case). CC
        // actually sends `permissionMode`. If someone ever "fixes" the
        // serde rename back to snake_case, this test blows up loudly.
        let json = r#"{
            "type": "system",
            "subtype": "init",
            "session_id": "abc-123",
            "tools": ["Read"],
            "model": "claude-opus-4-6-20250528",
            "cwd": "/home/user/project",
            "permission_mode": "auto"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse system init");
        match msg {
            CcIncoming::System(SystemMessage::Init {
                permission_mode, ..
            }) => {
                assert!(
                    permission_mode.is_none(),
                    "snake_case `permission_mode` must NOT bind — CC \
                     sends camelCase `permissionMode`"
                );
            }
            other => panic!("expected System/Init, got {other:?}"),
        }
    }

    #[test]
    fn parse_unknown_system_subtype() {
        // Uses a deliberately made-up subtype. Don't use a real CC subtype
        // here — the typed variants below need to stay reachable in the
        // match.
        let json = r#"{
            "type": "system",
            "subtype": "subtype_from_the_future",
            "whatever": "anything"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse unknown system subtype");
        assert!(matches!(msg, CcIncoming::System(SystemMessage::Unknown)));
    }

    #[test]
    fn parse_task_started() {
        let json = r#"{
            "type": "system",
            "subtype": "task_started",
            "task_id": "bdghkyrp3",
            "tool_use_id": "toolu_01WSEVM",
            "description": "Background sleep to pass idle threshold",
            "task_type": "local_bash",
            "uuid": "3795465f-08e2-4f14-a55b-bcbc195abc9d",
            "session_id": "bc967dc7-2c8c-417c-bdb9-830ed1baa038"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse task_started");
        match msg {
            CcIncoming::System(SystemMessage::TaskStarted {
                task_id,
                description,
                task_type,
                tool_use_id,
                session_id,
                ..
            }) => {
                assert_eq!(task_id.as_deref(), Some("bdghkyrp3"));
                assert_eq!(
                    description.as_deref(),
                    Some("Background sleep to pass idle threshold")
                );
                assert_eq!(task_type.as_deref(), Some("local_bash"));
                assert_eq!(tool_use_id.as_deref(), Some("toolu_01WSEVM"));
                assert_eq!(
                    session_id.as_deref(),
                    Some("bc967dc7-2c8c-417c-bdb9-830ed1baa038")
                );
            }
            other => panic!("expected System/TaskStarted, got {other:?}"),
        }
    }

    #[test]
    fn parse_task_progress_minimal() {
        // Protocol resilience: only the fields we'd need at a minimum.
        // CC's real task_progress has more fields (see extra), but the
        // variant must parse when they're absent.
        let json = r#"{
            "type": "system",
            "subtype": "task_progress",
            "task_id": "ab52885d"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse task_progress minimal");
        match msg {
            CcIncoming::System(SystemMessage::TaskProgress { task_id, .. }) => {
                assert_eq!(task_id.as_deref(), Some("ab52885d"));
            }
            other => panic!("expected System/TaskProgress, got {other:?}"),
        }
    }

    #[test]
    fn parse_task_progress_preserves_usage_in_extra() {
        // `usage` isn't modelled at the variant level; it should survive
        // via the `extra` flatten for forensics.
        let json = r#"{
            "type": "system",
            "subtype": "task_progress",
            "task_id": "ab52885d",
            "description": "Running something",
            "last_tool_name": "Bash",
            "usage": {"total_tokens": 10205, "tool_uses": 1, "duration_ms": 3176}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse task_progress");
        match msg {
            CcIncoming::System(SystemMessage::TaskProgress {
                last_tool_name,
                extra,
                ..
            }) => {
                assert_eq!(last_tool_name.as_deref(), Some("Bash"));
                assert_eq!(
                    extra
                        .get("usage")
                        .and_then(|v| v.get("total_tokens"))
                        .and_then(|v| v.as_u64()),
                    Some(10205)
                );
            }
            other => panic!("expected System/TaskProgress, got {other:?}"),
        }
    }

    #[test]
    fn parse_task_notification() {
        let json = r#"{
            "type": "system",
            "subtype": "task_notification",
            "task_id": "bdghkyrp3",
            "tool_use_id": "toolu_01WSEVM",
            "status": "completed",
            "output_file": "/tmp/claude-1000/sess/tasks/bdghkyrp3.output",
            "summary": "Background command completed (exit code 0)",
            "session_id": "bc967dc7-2c8c-417c-bdb9-830ed1baa038",
            "uuid": "f40b1d58-1c18-450e-be88-ed73f7759d84"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse task_notification");
        match msg {
            CcIncoming::System(SystemMessage::TaskNotification {
                task_id,
                status,
                output_file,
                summary,
                ..
            }) => {
                assert_eq!(task_id.as_deref(), Some("bdghkyrp3"));
                assert_eq!(status.as_deref(), Some("completed"));
                assert_eq!(
                    output_file.as_deref(),
                    Some("/tmp/claude-1000/sess/tasks/bdghkyrp3.output")
                );
                assert!(summary.as_deref().unwrap().contains("exit code 0"));
            }
            other => panic!("expected System/TaskNotification, got {other:?}"),
        }
    }

    #[test]
    fn parse_task_updated_patch_opaque() {
        // The `patch` field is an arbitrary sparse JSON object. It must
        // round-trip as serde_json::Value without Brenn making structural
        // assumptions — the Python SDK doesn't type it either.
        let json = r#"{
            "type": "system",
            "subtype": "task_updated",
            "task_id": "bdghkyrp3",
            "patch": {"status": "completed", "end_time": 1776369947341},
            "uuid": "5e66a50d-1e27-454e-8e27-87bbcf13b82e",
            "session_id": "bc967dc7-2c8c-417c-bdb9-830ed1baa038"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse task_updated");
        match msg {
            CcIncoming::System(SystemMessage::TaskUpdated { task_id, patch, .. }) => {
                assert_eq!(task_id.as_deref(), Some("bdghkyrp3"));
                let patch = patch.expect("patch present");
                assert_eq!(
                    patch.get("status").and_then(|v| v.as_str()),
                    Some("completed")
                );
                assert_eq!(
                    patch.get("end_time").and_then(|v| v.as_u64()),
                    Some(1776369947341u64)
                );
            }
            other => panic!("expected System/TaskUpdated, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_status_compacting() {
        let json = r#"{
            "type": "system",
            "subtype": "status",
            "status": "compacting",
            "session_id": "7799f829-28ba-4125-955a-ab9ee83b23bd",
            "uuid": "2bc4dd46-996b-4596-8f30-c7e0dca8a73e"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse status compacting");
        match msg {
            CcIncoming::System(SystemMessage::Status {
                status, session_id, ..
            }) => {
                assert_eq!(status.as_deref(), Some("compacting"));
                assert_eq!(
                    session_id.as_deref(),
                    Some("7799f829-28ba-4125-955a-ab9ee83b23bd")
                );
            }
            other => panic!("expected System/Status, got {other:?}"),
        }
    }

    #[test]
    fn parse_system_status_null() {
        let json = r#"{
            "type": "system",
            "subtype": "status",
            "status": null,
            "session_id": "7799f829-28ba-4125-955a-ab9ee83b23bd"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse status null");
        match msg {
            CcIncoming::System(SystemMessage::Status { status, .. }) => {
                assert!(status.is_none());
            }
            other => panic!("expected System/Status, got {other:?}"),
        }
    }

    #[test]
    fn parse_compact_boundary() {
        let json = r#"{
            "type": "system",
            "subtype": "compact_boundary",
            "session_id": "7799f829-28ba-4125-955a-ab9ee83b23bd",
            "uuid": "10306939-194d-42e1-811f-d63bf9a3c194",
            "compact_metadata": {"trigger": "manual", "pre_tokens": 16807}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse compact_boundary");
        match msg {
            CcIncoming::System(SystemMessage::CompactBoundary {
                session_id,
                compact_metadata,
                ..
            }) => {
                assert_eq!(
                    session_id.as_deref(),
                    Some("7799f829-28ba-4125-955a-ab9ee83b23bd")
                );
                let meta = compact_metadata.unwrap();
                assert_eq!(meta.trigger.as_deref(), Some("manual"));
                assert_eq!(meta.pre_tokens, Some(16807));
            }
            other => panic!("expected System/CompactBoundary, got {other:?}"),
        }
    }

    #[test]
    fn parse_assistant_with_text_and_tool_use() {
        let json = r#"{
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me check that."},
                    {"type": "tool_use", "id": "toolu_abc", "name": "Bash", "input": {"command": "date"}},
                    {"type": "thinking", "thinking": "I should run date", "signature": "sig123"}
                ],
                "model": "claude-opus-4-6-20250528",
                "usage": {"input_tokens": 100, "output_tokens": 50}
            },
            "uuid": "msg-uuid-1",
            "parent_tool_use_id": null
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse assistant");
        match msg {
            CcIncoming::Assistant(asst) => {
                assert_eq!(asst.uuid, "msg-uuid-1");
                assert!(asst.parent_tool_use_id.is_none());
                assert_eq!(asst.message.content.len(), 3);
                match &asst.message.content[0] {
                    ContentBlock::Text { text } => assert_eq!(text, "Let me check that."),
                    other => panic!("expected Text, got {other:?}"),
                }
                match &asst.message.content[1] {
                    ContentBlock::ToolUse { id, name, .. } => {
                        assert_eq!(id, "toolu_abc");
                        assert_eq!(name, "Bash");
                    }
                    other => panic!("expected ToolUse, got {other:?}"),
                }
                match &asst.message.content[2] {
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => {
                        assert_eq!(thinking, "I should run date");
                        assert_eq!(signature.as_deref(), Some("sig123"));
                    }
                    other => panic!("expected Thinking, got {other:?}"),
                }
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
    }

    #[test]
    fn parse_unknown_content_block_type() {
        let json = r#"{
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "hello"},
                    {"type": "brand_new_block_type", "data": "whatever"}
                ]
            },
            "uuid": "msg-1"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse with unknown block");
        match msg {
            CcIncoming::Assistant(asst) => {
                assert_eq!(asst.message.content.len(), 2);
                assert!(matches!(
                    &asst.message.content[0],
                    ContentBlock::Text { .. }
                ));
                assert!(matches!(&asst.message.content[1], ContentBlock::Unknown));
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
    }

    #[test]
    fn parse_control_request_can_use_tool() {
        let json = r#"{
            "type": "control_request",
            "request_id": "req_42",
            "request": {
                "subtype": "can_use_tool",
                "tool_name": "Write",
                "tool_use_id": "toolu_xyz",
                "input": {"file_path": "/tmp/foo.txt", "content": "hello"},
                "decision_reason": "Path is outside allowed directories"
            }
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse can_use_tool");
        match msg {
            CcIncoming::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "req_42");
                match request {
                    CcControlRequest::CanUseTool {
                        tool_name,
                        tool_use_id,
                        ..
                    } => {
                        assert_eq!(tool_name, "Write");
                        assert_eq!(tool_use_id, "toolu_xyz");
                    }
                    other => panic!("expected CanUseTool, got {other:?}"),
                }
            }
            other => panic!("expected ControlRequest, got {other:?}"),
        }
    }

    #[test]
    fn parse_hook_callback() {
        let json = r#"{
            "type": "control_request",
            "request_id": "req_7",
            "request": {
                "subtype": "hook_callback",
                "callback_id": "hook_pre_tool_0",
                "tool_use_id": "toolu_abc",
                "input": {
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Bash",
                    "tool_input": {"command": "date"},
                    "tool_use_id": "toolu_abc",
                    "session_id": "sess-1",
                    "cwd": "/home/user"
                }
            }
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse hook_callback");
        match msg {
            CcIncoming::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "req_7");
                match request {
                    CcControlRequest::HookCallback {
                        callback_id, input, ..
                    } => {
                        assert_eq!(callback_id, "hook_pre_tool_0");
                        assert_eq!(input.hook_event_name, "PreToolUse");
                        assert_eq!(input.tool_name.as_deref(), Some("Bash"));
                    }
                    other => panic!("expected HookCallback, got {other:?}"),
                }
            }
            other => panic!("expected ControlRequest, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_message() {
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "duration_ms": 12345,
            "duration_api_ms": 10000,
            "is_error": false,
            "num_turns": 3,
            "session_id": "sess-1",
            "total_cost_usd": 0.05,
            "usage": {"input_tokens": 2000, "output_tokens": 500},
            "stop_reason": "end_turn"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result");
        match msg {
            CcIncoming::Result(res) => {
                assert_eq!(res.subtype.as_deref(), Some("success"));
                assert_eq!(res.total_cost_usd, Some(0.05));
                assert_eq!(res.num_turns, Some(3));
                assert_eq!(res.stop_reason.as_deref(), Some("end_turn"));
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_with_task_notification_origin() {
        // A CC-autonomous turn stamps `origin.kind = "task-notification"`.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "num_turns": 1,
            "result": ".",
            "terminal_reason": "completed",
            "origin": {"kind": "task-notification"}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result with origin");
        match msg {
            CcIncoming::Result(res) => {
                let origin = res.origin.expect("origin should be Some");
                assert_eq!(origin.kind, "task-notification");
                // `terminal_reason` is not a typed field — it lands in extra.
                assert_eq!(
                    res.extra.get("terminal_reason").and_then(|v| v.as_str()),
                    Some("completed")
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_without_origin_is_none() {
        // Ordinary Brenn-initiated turn: no origin field → None, extra untouched.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.05,
            "custom_extra": "kept"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result without origin");
        match msg {
            CcIncoming::Result(res) => {
                assert!(res.origin.is_none(), "origin must be None when absent");
                assert_eq!(
                    res.extra.get("custom_extra").and_then(|v| v.as_str()),
                    Some("kept"),
                    "unrelated extra fields must survive"
                );
                assert!(
                    res.extra.get("origin").is_none(),
                    "origin must be consumed by the typed field, not left in extra"
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_origin_unknown_kind_preserved() {
        // An unknown kind string must parse verbatim, never reject the frame.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "origin": {"kind": "kind-from-the-future", "detail": 7}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result with novel origin");
        match msg {
            CcIncoming::Result(res) => {
                let origin = res.origin.expect("origin Some");
                assert_eq!(origin.kind, "kind-from-the-future");
                // Extra keys inside the origin object survive via flatten.
                assert_eq!(origin.extra.get("detail").and_then(|v| v.as_u64()), Some(7));
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn result_origin_round_trips() {
        // Serialize → deserialize preserves kind and extra origin keys.
        let original = ResultMessage {
            subtype: Some("success".into()),
            duration_ms: None,
            duration_api_ms: None,
            is_error: Some(false),
            num_turns: Some(1),
            session_id: None,
            total_cost_usd: None,
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: None,
            origin: Some(ResultOrigin {
                kind: "task-notification".into(),
                extra: serde_json::json!({"task_id": "abc123"}),
            }),
            extra: serde_json::json!({}),
        };
        let wire = serde_json::to_string(&original).expect("serialize");
        let parsed: ResultMessage = serde_json::from_str(&wire).expect("deserialize");
        let origin = parsed.origin.expect("origin round-trips");
        assert_eq!(origin.kind, "task-notification");
        assert_eq!(
            origin.extra.get("task_id").and_then(|v| v.as_str()),
            Some("abc123")
        );
    }

    #[test]
    fn parse_control_cancel_request() {
        let json = r#"{"type": "control_cancel_request", "request_id": "req_99"}"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse cancel");
        match msg {
            CcIncoming::ControlCancelRequest { request_id } => {
                assert_eq!(request_id, "req_99");
            }
            other => panic!("expected ControlCancelRequest, got {other:?}"),
        }
    }

    #[test]
    fn parse_stream_event() {
        let json = r#"{
            "type": "stream_event",
            "uuid": "msg-1",
            "session_id": "sess-1",
            "event": {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse stream_event");
        match msg {
            CcIncoming::StreamEvent(se) => {
                assert_eq!(se.uuid, "msg-1");
                assert!(se.event.get("type").is_some());
            }
            other => panic!("expected StreamEvent, got {other:?}"),
        }
    }

    #[test]
    fn parse_rate_limit_event() {
        let json = r#"{
            "type": "rate_limit_event",
            "uuid": "msg-1",
            "session_id": "sess-1",
            "rate_limit_info": {"status": "allowed_warning", "utilization": 0.85}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse rate_limit_event");
        assert!(matches!(msg, CcIncoming::RateLimitEvent(_)));
    }

    #[test]
    fn parse_control_response() {
        let json = r#"{
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": "req_1"
            }
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse control_response");
        match msg {
            CcIncoming::ControlResponse { response } => {
                assert_eq!(response.subtype, "success");
                assert_eq!(response.request_id.as_deref(), Some("req_1"));
            }
            other => panic!("expected ControlResponse, got {other:?}"),
        }
    }

    #[test]
    fn parse_user_tool_result() {
        let json = r#"{
            "type": "user",
            "uuid": "msg-2",
            "session_id": "sess-1",
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "toolu_abc", "content": "file.txt"}]
            },
            "tool_use_result": {"stdout": "file.txt", "stderr": "", "interrupted": false}
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse user tool result");
        match msg {
            CcIncoming::User(u) => {
                assert_eq!(u.uuid.as_deref(), Some("msg-2"));
                assert!(u.tool_use_result.is_some());
            }
            other => panic!("expected User, got {other:?}"),
        }
    }

    #[test]
    fn unknown_fields_preserved_via_flatten() {
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "brand_new_field": "some_value",
            "another_new_field": 42
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse with unknown fields");
        match msg {
            CcIncoming::Result(res) => {
                assert_eq!(
                    res.extra.get("brand_new_field").and_then(|v| v.as_str()),
                    Some("some_value")
                );
                assert_eq!(
                    res.extra.get("another_new_field").and_then(|v| v.as_u64()),
                    Some(42)
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_with_modelusage() {
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.05,
            "modelUsage": {
                "claude-opus-4-7[1m]": {
                    "contextWindow": 1000000,
                    "maxOutputTokens": 32000,
                    "costUSD": 0.05,
                    "inputTokens": 5000,
                    "outputTokens": 200
                }
            }
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result with modelUsage");
        match msg {
            CcIncoming::Result(res) => {
                let mu = res.model_usage.expect("model_usage should be Some");
                let entry = mu.get("claude-opus-4-7[1m]").expect("entry should exist");
                assert_eq!(entry.context_window, Some(1_000_000));
                assert_eq!(entry.max_output_tokens, Some(32_000));
                assert_eq!(entry.cost_usd, Some(0.05));
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_modelusage_multi_key() {
        // Subagent + top-level: both entries must be parsed.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "modelUsage": {
                "claude-opus-4-7[1m]": {
                    "contextWindow": 1000000,
                    "costUSD": 0.10
                },
                "claude-haiku-3-5": {
                    "contextWindow": 200000,
                    "costUSD": 0.001
                }
            }
        }"#;
        let msg: CcIncoming =
            serde_json::from_str(json).expect("parse result with multi-key modelUsage");
        match msg {
            CcIncoming::Result(res) => {
                let mu = res.model_usage.expect("model_usage should be Some");
                assert_eq!(mu.len(), 2);
                assert_eq!(
                    mu.get("claude-opus-4-7[1m]").and_then(|e| e.context_window),
                    Some(1_000_000)
                );
                assert_eq!(
                    mu.get("claude-haiku-3-5").and_then(|e| e.context_window),
                    Some(200_000)
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_without_modelusage() {
        // Compaction-result frames lack modelUsage — model_usage must be None.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "total_cost_usd": 0.001
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse result without modelUsage");
        match msg {
            CcIncoming::Result(res) => {
                assert!(
                    res.model_usage.is_none(),
                    "model_usage should be None when absent"
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_result_modelusage_null_context_window_parses() {
        // contextWindow: null must NOT reject the whole result message — it
        // yields ModelUsageEntry with context_window = None.
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "modelUsage": {
                "claude-opus-4-7[1m]": {
                    "contextWindow": null,
                    "costUSD": 0.05
                }
            }
        }"#;
        let msg: CcIncoming =
            serde_json::from_str(json).expect("result with null contextWindow should parse");
        match msg {
            CcIncoming::Result(res) => {
                let mu = res.model_usage.expect("model_usage Some");
                let entry = mu.get("claude-opus-4-7[1m]").expect("entry exists");
                assert!(
                    entry.context_window.is_none(),
                    "context_window should be None for null wire value"
                );
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn parse_usage_with_cache_fields() {
        let json = r#"{
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "hello"}],
                "model": "claude-opus-4-7",
                "usage": {
                    "input_tokens": 1000,
                    "output_tokens": 200,
                    "cache_read_input_tokens": 50000,
                    "cache_creation_input_tokens": 5000
                }
            },
            "uuid": "test-uuid"
        }"#;
        let msg: CcIncoming = serde_json::from_str(json).expect("parse assistant with usage");
        match msg {
            CcIncoming::Assistant(a) => {
                let usage = a.message.usage.expect("usage should be Some");
                assert_eq!(usage.input_tokens, Some(1000));
                assert_eq!(usage.output_tokens, Some(200));
                assert_eq!(usage.cache_read_input_tokens, Some(50_000));
                assert_eq!(usage.cache_creation_input_tokens, Some(5_000));
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
    }
}
