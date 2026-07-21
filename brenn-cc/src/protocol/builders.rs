//! Builder functions for outgoing CC messages.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::RngExt;

use super::outgoing::*;

/// Atomic counter for generating unique request IDs within a session.
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique request ID in the format `req_{counter}_{4-byte-hex}`.
pub fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut buf = [0u8; 4];
    rand::rng().fill(&mut buf[..]);
    format!("req_{counter}_{}", hex::encode(buf))
}

/// Build an initialization request with optional hook and agent configuration.
pub fn initialize(hooks: Option<HooksConfig>, agents: Option<serde_json::Value>) -> CcOutgoing {
    CcOutgoing::ControlRequest {
        request_id: next_request_id(),
        request: BrennControlRequest::Initialize { hooks, agents },
    }
}

/// Build a user message.
pub fn user_message(text: &str) -> CcOutgoing {
    CcOutgoing::User {
        message: UserContent {
            role: "user".into(),
            content: vec![UserContentBlock::Text {
                text: text.to_string(),
            }],
        },
    }
}

/// Build a user message with additional context blocks.
///
/// The message contains the user's text as the first content block,
/// followed by one text block per context string. Context blocks are
/// compact JSON with a `"context"` key identifying the type.
pub fn user_message_with_context(text: &str, context_blocks: &[String]) -> CcOutgoing {
    let mut content = vec![UserContentBlock::Text {
        text: text.to_string(),
    }];
    for block in context_blocks {
        content.push(UserContentBlock::Text {
            text: block.clone(),
        });
    }
    CcOutgoing::User {
        message: UserContent {
            role: "user".into(),
            content,
        },
    }
}

/// Build a set_model control request.
pub fn set_model(model: &str) -> CcOutgoing {
    CcOutgoing::ControlRequest {
        request_id: next_request_id(),
        request: BrennControlRequest::SetModel {
            model: model.to_string(),
        },
    }
}

/// Build an interrupt request.
pub fn interrupt() -> CcOutgoing {
    CcOutgoing::ControlRequest {
        request_id: next_request_id(),
        request: BrennControlRequest::Interrupt {},
    }
}

/// Build a permission allow response. `updated_input` is required per the
/// CC protocol — echo the original input if no modifications.
pub fn permission_allow(request_id: &str, input: &serde_json::Value) -> CcOutgoing {
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "behavior": "allow",
                "updatedInput": input,
            }),
        },
    }
}

/// Build a permission deny response.
pub fn permission_deny(request_id: &str, message: &str) -> CcOutgoing {
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "behavior": "deny",
                "message": message,
            }),
        },
    }
}

/// Build a PreToolUse hook "no opinion" response — continues without granting
/// or denying permission. CC will fall through to its normal permission flow
/// (e.g., `--permission-prompt-tool`). Use this when the hook has no reason to
/// override the default permission behavior.
pub fn hook_pre_no_opinion(request_id: &str) -> CcOutgoing {
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "continue": true,
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                },
            }),
        },
    }
}

/// Build a PreToolUse hook allow response. Sets `permissionDecision: "allow"`,
/// which tells CC to skip its normal permission prompt for this tool use.
pub fn hook_pre_allow(request_id: &str, updated_input: Option<&serde_json::Value>) -> CcOutgoing {
    let mut hook_output = serde_json::json!({
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "permissionDecisionReason": "Approved by Brenn",
    });
    if let Some(input) = updated_input {
        hook_output["updatedInput"] = input.clone();
    }
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "continue": true,
                "hookSpecificOutput": hook_output,
            }),
        },
    }
}

/// Build a PreToolUse hook deny response.
pub fn hook_pre_deny(request_id: &str, reason: &str) -> CcOutgoing {
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "continue": true,
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                },
            }),
        },
    }
}

/// Build a PostToolUse hook response, optionally replacing MCP tool output.
pub fn hook_post(request_id: &str, updated_output: Option<&str>) -> CcOutgoing {
    let mut hook_output = serde_json::json!({
        "hookEventName": "PostToolUse",
    });
    if let Some(output) = updated_output {
        hook_output["updatedMCPToolOutput"] = serde_json::Value::String(output.to_string());
    }
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "continue": true,
                "hookSpecificOutput": hook_output,
            }),
        },
    }
}

/// Build a generic hook continue response for non-tool events
/// (Stop, UserPromptSubmit, etc.).
pub fn hook_continue(request_id: &str, event_name: &str) -> CcOutgoing {
    CcOutgoing::ControlResponse {
        response: BrennControlResponse {
            subtype: "success".into(),
            request_id: request_id.to_string(),
            response: serde_json::json!({
                "continue": true,
                "hookSpecificOutput": {
                    "hookEventName": event_name,
                },
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_are_unique() {
        let id1 = next_request_id();
        let id2 = next_request_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("req_"));
        assert!(id2.starts_with("req_"));
    }

    #[test]
    fn permission_allow_includes_updated_input() {
        let input = serde_json::json!({"file_path": "/tmp/foo.txt"});
        let msg = permission_allow("req_1", &input);
        let json = serde_json::to_value(&msg).expect("serialize");
        let resp = &json["response"]["response"];
        assert_eq!(resp["behavior"], "allow");
        assert_eq!(resp["updatedInput"]["file_path"], "/tmp/foo.txt");
    }

    #[test]
    fn permission_deny_includes_message() {
        let msg = permission_deny("req_2", "Not allowed");
        let json = serde_json::to_value(&msg).expect("serialize");
        let resp = &json["response"]["response"];
        assert_eq!(resp["behavior"], "deny");
        assert_eq!(resp["message"], "Not allowed");
    }

    #[test]
    fn hook_pre_no_opinion_omits_permission_decision() {
        let msg = hook_pre_no_opinion("req_no_opinion");
        let json = serde_json::to_value(&msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PreToolUse");
        assert!(
            hook.get("permissionDecision").is_none(),
            "no-opinion response must not include permissionDecision"
        );
    }

    #[test]
    fn hook_pre_allow_format() {
        let msg = hook_pre_allow("req_3", None);
        let json = serde_json::to_value(&msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PreToolUse");
        assert_eq!(hook["permissionDecision"], "allow");
    }

    #[test]
    fn hook_pre_deny_format() {
        let msg = hook_pre_deny("req_4", "Blocked by policy");
        let json = serde_json::to_value(&msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["permissionDecision"], "deny");
        assert_eq!(hook["permissionDecisionReason"], "Blocked by policy");
    }

    #[test]
    fn hook_post_with_updated_output() {
        let msg = hook_post("req_5", Some("real tool output"));
        let json = serde_json::to_value(&msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PostToolUse");
        assert_eq!(hook["updatedMCPToolOutput"], "real tool output");
    }

    #[test]
    fn hook_post_without_updated_output() {
        let msg = hook_post("req_6", None);
        let json = serde_json::to_value(&msg).expect("serialize");
        let hook = &json["response"]["response"]["hookSpecificOutput"];
        assert_eq!(hook["hookEventName"], "PostToolUse");
        assert!(hook.get("updatedMCPToolOutput").is_none());
    }

    #[test]
    fn user_message_format() {
        let msg = user_message("What is 2+2?");
        let json = serde_json::to_value(&msg).expect("serialize");
        assert_eq!(json["type"], "user");
        assert_eq!(json["message"]["role"], "user");
        assert_eq!(json["message"]["content"][0]["text"], "What is 2+2?");
    }

    #[test]
    fn user_message_with_context_format() {
        let context = vec![
            r#"{"context":"selected_tasks","tasks":[{"ref":"life:todo/buy-groceries.md","tldr":"Buy groceries"}]}"#.to_string(),
        ];
        let msg = user_message_with_context("Which should I do first?", &context);
        let json = serde_json::to_value(&msg).expect("serialize");
        assert_eq!(json["type"], "user");
        assert_eq!(json["message"]["role"], "user");
        let content = json["message"]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 2, "expected 2 content blocks");
        assert_eq!(content[0]["text"], "Which should I do first?");
        assert_eq!(content[1]["type"], "text");
        // The context block should be valid JSON.
        let ctx_text = content[1]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(ctx_text).expect("parse context JSON");
        assert_eq!(parsed["context"], "selected_tasks");
    }

    #[test]
    fn user_message_with_empty_context_has_single_block() {
        let msg = user_message_with_context("Hello", &[]);
        let json = serde_json::to_value(&msg).expect("serialize");
        let content = json["message"]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["text"], "Hello");
    }

    #[test]
    fn set_model_format() {
        let msg = set_model("opus");
        let json = serde_json::to_value(&msg).expect("serialize");
        assert_eq!(json["type"], "control_request");
        assert_eq!(json["request"]["subtype"], "set_model");
        assert_eq!(json["request"]["model"], "opus");
        // Must have a request_id.
        assert!(json["request_id"].as_str().unwrap().starts_with("req_"));
    }
}
