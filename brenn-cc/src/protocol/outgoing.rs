//! Types for messages sent to CC's stdin (Brenn → CC).

use serde::Serialize;

/// A message sent to CC's stdin.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum CcOutgoing {
    #[serde(rename = "control_request")]
    ControlRequest {
        request_id: String,
        request: BrennControlRequest,
    },

    #[serde(rename = "control_response")]
    ControlResponse { response: BrennControlResponse },

    #[serde(rename = "user")]
    User { message: UserContent },
}

/// A control request from Brenn to CC.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "subtype")]
pub enum BrennControlRequest {
    #[serde(rename = "initialize")]
    Initialize {
        #[serde(skip_serializing_if = "Option::is_none")]
        hooks: Option<HooksConfig>,
        #[serde(skip_serializing_if = "Option::is_none")]
        agents: Option<serde_json::Value>,
    },
    #[serde(rename = "interrupt")]
    Interrupt {},
    #[serde(rename = "set_model")]
    SetModel { model: String },
}

/// Hook configuration for the initialize request.
#[derive(Debug, Clone, Serialize)]
pub struct HooksConfig {
    #[serde(rename = "PreToolUse", skip_serializing_if = "Option::is_none")]
    pub pre_tool_use: Option<Vec<HookMatcher>>,
    #[serde(rename = "PostToolUse", skip_serializing_if = "Option::is_none")]
    pub post_tool_use: Option<Vec<HookMatcher>>,
}

/// A hook matcher entry.
#[derive(Debug, Clone, Serialize)]
pub struct HookMatcher {
    #[serde(rename = "hookCallbackIds")]
    pub hook_callback_ids: Vec<String>,
    pub timeout: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
}

/// A control response from Brenn to CC.
#[derive(Debug, Clone, Serialize)]
pub struct BrennControlResponse {
    pub subtype: String,
    pub request_id: String,
    pub response: serde_json::Value,
}

/// User message content.
#[derive(Debug, Clone, Serialize)]
pub struct UserContent {
    pub role: String,
    pub content: Vec<UserContentBlock>,
}

/// A content block in a user message.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum UserContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: serde_json::Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_user_message() {
        let msg = CcOutgoing::User {
            message: UserContent {
                role: "user".into(),
                content: vec![UserContentBlock::Text {
                    text: "Hello".into(),
                }],
            },
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse back");
        assert_eq!(parsed["type"], "user");
        assert_eq!(parsed["message"]["role"], "user");
        assert_eq!(parsed["message"]["content"][0]["type"], "text");
        assert_eq!(parsed["message"]["content"][0]["text"], "Hello");
    }

    #[test]
    fn serialize_initialize_with_hooks() {
        let msg = CcOutgoing::ControlRequest {
            request_id: "req_1".into(),
            request: BrennControlRequest::Initialize {
                hooks: Some(HooksConfig {
                    pre_tool_use: Some(vec![HookMatcher {
                        hook_callback_ids: vec!["hook_pre_tool_0".into()],
                        timeout: 120,
                        matcher: None,
                    }]),
                    post_tool_use: Some(vec![HookMatcher {
                        hook_callback_ids: vec!["hook_post_tool_0".into()],
                        timeout: 10,
                        matcher: None,
                    }]),
                }),
                agents: None,
            },
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse back");
        assert_eq!(parsed["type"], "control_request");
        assert_eq!(parsed["request"]["subtype"], "initialize");
        assert!(parsed["request"]["hooks"]["PreToolUse"].is_array());
        assert!(parsed["request"]["hooks"]["PostToolUse"].is_array());
    }

    #[test]
    fn serialize_control_response_allow() {
        let msg = CcOutgoing::ControlResponse {
            response: BrennControlResponse {
                subtype: "success".into(),
                request_id: "req_42".into(),
                response: serde_json::json!({
                    "behavior": "allow",
                    "updatedInput": {"file_path": "/tmp/foo.txt"}
                }),
            },
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse back");
        assert_eq!(parsed["response"]["subtype"], "success");
        assert_eq!(parsed["response"]["request_id"], "req_42");
        assert_eq!(parsed["response"]["response"]["behavior"], "allow");
    }

    #[test]
    fn serialize_interrupt() {
        let msg = CcOutgoing::ControlRequest {
            request_id: "req_5".into(),
            request: BrennControlRequest::Interrupt {},
        };
        let json = serde_json::to_string(&msg).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse back");
        assert_eq!(parsed["request"]["subtype"], "interrupt");
    }
}
