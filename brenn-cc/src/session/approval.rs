//! Approval routing types for tool permission and hook callbacks.

use tokio::sync::oneshot;

/// A tool approval request delivered to the consumer via SessionEvent.
pub struct ApprovalRequest {
    /// The CC request_id this approval corresponds to.
    pub request_id: String,
    /// What kind of approval is needed.
    pub kind: ApprovalKind,
    /// Send the decision back through this channel.
    pub response_tx: oneshot::Sender<ApprovalDecision>,
}

/// The kind of approval CC is requesting.
#[derive(Debug)]
pub enum ApprovalKind {
    /// CC's permission system wants approval for a tool.
    Permission {
        tool_name: String,
        tool_use_id: String,
        input: serde_json::Value,
    },
    /// PreToolUse hook — can allow, deny, or modify input.
    PreToolUse {
        callback_id: String,
        tool_name: String,
        tool_input: serde_json::Value,
        tool_use_id: String,
    },
    /// PostToolUse hook — can replace MCP tool output.
    PostToolUse {
        callback_id: String,
        tool_name: String,
        tool_input: serde_json::Value,
        tool_response: serde_json::Value,
        tool_use_id: String,
    },
    /// Other hook events (Stop, UserPromptSubmit, etc.) — just continue.
    OtherHook {
        callback_id: String,
        event_name: String,
    },
}

/// The consumer's decision on an approval request.
#[derive(Debug)]
pub enum ApprovalDecision {
    /// Allow a Permission or PreToolUse request.
    /// `updated_input` is required for Permission (echo original if unchanged),
    /// optional for PreToolUse.
    Allow {
        updated_input: Option<serde_json::Value>,
    },
    /// Deny a Permission or PreToolUse request.
    Deny { reason: String },
    /// Continue a PostToolUse or OtherHook, optionally replacing MCP tool output.
    Continue { updated_output: Option<String> },
}
