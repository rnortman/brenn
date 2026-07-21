//! Built-in tool implementations and tool registry construction.
//!
//! Tools that ship with Brenn core (not from app extensions). CC's standard
//! tools (Bash, Edit, etc.) use the built-in formatters in `approval_formatter.rs`
//! rather than the AppTool trait. All brenn MCP tools are registered here.

mod ask_user_question;
mod export_usage;
pub(crate) mod git_repo;
pub(crate) mod messaging;
pub(crate) mod mqtt;
pub(crate) mod pwa_push;

pub use ask_user_question::AskUserQuestionTool;
pub use export_usage::ExportUsageTool;
pub use git_repo::{GitRepoCommitAndPushTool, GitRepoRunTool};
pub use messaging::MessageSendTool;
pub use mqtt::MqttSendTool;
pub use pwa_push::PushSendTool;

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::app::{AppTool, AutoApproveTool};

/// Build the global tool registry from built-in tools and app extensions.
///
/// Panics on duplicate tool names — each name must be globally unique.
///
/// TODO(tool-registry-absorb-apptool): this legacy `AppTool` display registry
/// coexists with the first-class `tool_registry::ToolRegistry`. Its per-tool
/// metadata (summary formatting, auto-approve) should eventually fold into
/// `ToolDescriptor` so there is a single tool table.
pub fn build_tool_registry(
    app_tools: Vec<Box<dyn AppTool>>,
) -> Arc<HashMap<String, Arc<dyn AppTool>>> {
    let mut registry = HashMap::<String, Arc<dyn AppTool>>::new();

    // Built-in tools.
    let builtins: Vec<Box<dyn AppTool>> = vec![
        Box::new(AskUserQuestionTool),
        Box::new(ExportUsageTool),
        Box::new(GitRepoCommitAndPushTool),
        Box::new(GitRepoRunTool),
        // Messaging: BrennSend has a custom summary formatter; the
        // others are auto-approved with the generic JSON summary.
        Box::new(MessageSendTool),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_LIST_CHANNELS_TOOL)),
        Box::new(AutoApproveTool(
            messaging::MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL,
        )),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_QUERY_CHANNEL_TOOL)),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_SUBSCRIBE_TOOL)),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_UNSUBSCRIBE_TOOL)),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_PENDING_LIST_TOOL)),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_CANCEL_TOOL)),
        Box::new(AutoApproveTool(messaging::MCP_MESSAGE_EDIT_TOOL)),
        // Automation tools: all four auto-approved (budget is the control).
        Box::new(AutoApproveTool(brenn_lib::automation::MCP_AUTO_CREATE_TOOL)),
        Box::new(AutoApproveTool(brenn_lib::automation::MCP_AUTO_LIST_TOOL)),
        Box::new(AutoApproveTool(brenn_lib::automation::MCP_AUTO_EDIT_TOOL)),
        Box::new(AutoApproveTool(brenn_lib::automation::MCP_AUTO_DELETE_TOOL)),
        // PWA push: PwaPushSend has a custom summary formatter;
        // PwaPushChannelGet is read-only and auto-approved.
        Box::new(PushSendTool),
        Box::new(AutoApproveTool(pwa_push::MCP_PUSH_LIST_TARGETS_TOOL)),
        // MQTT tools: MqttSend has a custom summary formatter.
        Box::new(MqttSendTool),
    ];
    for tool in builtins {
        let name = tool.name().to_string();
        assert!(
            registry.insert(name.clone(), Arc::from(tool)).is_none(),
            "duplicate tool name in registry: {name}"
        );
    }

    // App extension tools.
    for tool in app_tools {
        let name = tool.name().to_string();
        assert!(
            registry.insert(name.clone(), Arc::from(tool)).is_none(),
            "duplicate tool name in registry: {name}"
        );
    }

    Arc::new(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::ws_types::ToolResponseDecision;

    #[test]
    fn registry_contains_ask_user_question() {
        let registry = build_tool_registry(vec![]);
        assert!(
            registry.contains_key("AskUserQuestion"),
            "should contain built-in AskUserQuestion tool"
        );
    }

    #[test]
    fn registry_contains_export_usage_tool() {
        let registry = build_tool_registry(vec![]);
        assert!(
            registry.contains_key("mcp__brenn__ExportUsage"),
            "should contain built-in ExportUsageTool"
        );
    }

    #[test]
    fn export_usage_dispatches_through_registry() {
        let registry = build_tool_registry(vec![]);
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
        });
        let html = crate::approval_formatter::format_tool_display(
            &registry,
            "mcp__brenn__ExportUsage",
            &input,
        );
        assert!(
            html.contains("tool-file"),
            "should have tool-file class: {html}"
        );
        assert!(
            html.contains("<brenn-tool-approve"),
            "should be wrapped in tool-approve: {html}"
        );
    }

    #[test]
    fn registry_includes_app_tools() {
        struct FakeTool;
        impl AppTool for FakeTool {
            fn name(&self) -> &str {
                "mcp__fake__tool"
            }
        }

        let registry = build_tool_registry(vec![Box::new(FakeTool)]);
        assert!(registry.contains_key("AskUserQuestion"));
        assert!(
            registry.contains_key("mcp__fake__tool"),
            "should contain app-provided tool"
        );
    }

    #[test]
    #[should_panic(expected = "duplicate tool name")]
    fn registry_panics_on_duplicate_name() {
        struct DuplicateTool;
        impl AppTool for DuplicateTool {
            fn name(&self) -> &str {
                "AskUserQuestion"
            }
        }

        // Should panic because AskUserQuestion is already a built-in.
        build_tool_registry(vec![Box::new(DuplicateTool)]);
    }

    #[test]
    fn registry_lookup_returns_correct_tool() {
        struct CustomTool;
        impl AppTool for CustomTool {
            fn name(&self) -> &str {
                "mcp__test__custom"
            }
            fn auto_approve(&self) -> bool {
                true
            }
        }

        let registry = build_tool_registry(vec![Box::new(CustomTool)]);
        let tool = registry.get("mcp__test__custom").unwrap();
        assert_eq!(tool.name(), "mcp__test__custom");
        assert!(tool.auto_approve());
    }

    #[test]
    fn ask_user_question_dispatches_through_registry() {
        // End-to-end: tool_registry lookup → format_display → component HTML.
        let registry = build_tool_registry(vec![]);
        let input = serde_json::json!({
            "questions": [{
                "header": "Color",
                "question": "Pick one",
                "options": [{"label": "Red", "description": "warm"}],
                "multiSelect": false
            }]
        });

        let html =
            crate::approval_formatter::format_tool_display(&registry, "AskUserQuestion", &input);
        assert!(
            html.contains("<brenn-ask-user-question>"),
            "should produce custom component, got: {html}"
        );
        assert!(
            !html.contains("<brenn-tool-approve"),
            "should NOT fall through to generic wrapper: {html}"
        );
    }

    #[test]
    fn ask_user_question_summary_dispatches_through_registry() {
        let registry = build_tool_registry(vec![]);
        let input = serde_json::json!({
            "questions": [{"header": "Q", "question": "test?", "options": [], "multiSelect": false}]
        });
        let decision = ToolResponseDecision::Allow {
            updated_input: Some(serde_json::json!({"answers": {"test?": "yes"}})),
        };

        let html = crate::approval_formatter::format_tool_summary(
            &registry,
            "AskUserQuestion",
            &input,
            &decision,
        );
        assert!(
            html.contains("ts-answer"),
            "should use AppTool summary formatter: {html}"
        );
        assert!(html.contains("yes"), "should contain answer: {html}");
    }

    // -----------------------------------------------------------------------
    // test-3: registry contains all five renamed messaging/push tools
    // -----------------------------------------------------------------------

    /// The registry must contain all five renamed messaging and push tools by
    /// their final MCP names. Verifies that the tool constants and the
    /// `build_tool_registry` call sites are kept in lockstep.
    #[test]
    fn registry_contains_all_renamed_messaging_and_push_tools() {
        let registry = build_tool_registry(vec![]);
        let expected = [
            messaging::MCP_MESSAGE_LIST_CHANNELS_TOOL, // mcp__brenn__MessageChannelList
            messaging::MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL, // mcp__brenn__MessageSubscriptionList
            messaging::MCP_MESSAGE_SEND_TOOL,          // mcp__brenn__BrennSend
            messaging::MCP_MESSAGE_QUERY_CHANNEL_TOOL, // mcp__brenn__MessageChannelGet
            messaging::MCP_MESSAGE_PENDING_LIST_TOOL,  // mcp__brenn__BrennPendingList
            messaging::MCP_MESSAGE_CANCEL_TOOL,        // mcp__brenn__BrennMessageCancel
            messaging::MCP_MESSAGE_EDIT_TOOL,          // mcp__brenn__BrennMessageEdit
            pwa_push::MCP_PUSH_SEND_TOOL,              // mcp__brenn__PwaPushSend
            pwa_push::MCP_PUSH_LIST_TARGETS_TOOL,      // mcp__brenn__PwaPushChannelGet
        ];
        for name in expected {
            assert!(
                registry.contains_key(name),
                "registry should contain {name:?}"
            );
        }
    }
}
