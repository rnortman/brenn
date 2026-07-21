//! AppTool impls and tool name constants for the MQTT virtual tools.
//!
//! `MqttSendTool` is auto-approved and has a custom `format_summary` that renders
//! a "Sent MQTT message" chat-history card.

use brenn_lib::app::AppTool;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

pub const MCP_MQTT_SEND_TOOL: &str = "mcp__brenn__MqttSend";

/// Auto-approved `MqttSend` tool with a custom chat-history card.
pub struct MqttSendTool;

impl AppTool for MqttSendTool {
    fn name(&self) -> &str {
        MCP_MQTT_SEND_TOOL
    }

    fn auto_approve(&self) -> bool {
        true
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        _decision: &ToolResponseDecision,
    ) -> Option<String> {
        let to = tool_input.get("to").and_then(|v| v.as_str()).unwrap_or("");
        let qos = tool_input.get("qos").and_then(|v| v.as_u64()).unwrap_or(1);
        let retain = tool_input
            .get("retain")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let status_badge = render_mqtt_status_badge(tool_input.get("_outcome"));
        let retain_badge = if retain {
            r#"<span class="brenn-mqtt-retain">retain</span>"#
        } else {
            ""
        };
        Some(format!(
            r#"<details class="brenn-mqtt brenn-mqtt-send">
  <summary>
    {status_badge}
    <span class="brenn-mqtt-to">{to}</span>
    <span class="brenn-mqtt-qos">QoS {qos}</span>
    {retain_badge}
  </summary>
</details>"#,
            to = html_escape(to),
        ))
    }
}

fn render_mqtt_status_badge(outcome: Option<&serde_json::Value>) -> String {
    let Some(outcome) = outcome else {
        return r#"<span class="brenn-mqtt-status brenn-mqtt-status-pending">Sent</span>"#
            .to_string();
    };
    let ok = outcome.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    if ok {
        r#"<span class="brenn-mqtt-status brenn-mqtt-status-ok">delivered</span>"#.to_string()
    } else {
        let err = outcome
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("send failed");
        format!(
            r#"<span class="brenn-mqtt-status brenn-mqtt-status-err">{err}</span>"#,
            err = html_escape(err),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::ws_types::ToolResponseDecision;

    #[test]
    fn mqtt_send_tool_is_auto_approve() {
        let t = MqttSendTool;
        assert_eq!(t.name(), MCP_MQTT_SEND_TOOL);
        assert!(t.auto_approve());
    }

    #[test]
    fn mqtt_send_summary_includes_to_and_qos() {
        let t = MqttSendTool;
        let input = serde_json::json!({
            "to": "mqtt:ha:home/sensor/temp",
            "body": "42",
            "qos": 1,
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            html.contains("brenn-mqtt-send"),
            "missing card class: {html}"
        );
        assert!(
            html.contains("mqtt:ha:home/sensor/temp"),
            "missing to: {html}"
        );
        assert!(html.contains("QoS 1"), "missing qos: {html}");
        assert!(
            !html.contains("retain"),
            "should not show retain badge: {html}"
        );
    }

    #[test]
    fn mqtt_send_summary_shows_retain_badge() {
        let t = MqttSendTool;
        let input = serde_json::json!({
            "to": "mqtt:ha:status",
            "body": "online",
            "retain": true,
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            html.contains("brenn-mqtt-retain"),
            "missing retain badge: {html}"
        );
    }

    #[test]
    fn mqtt_send_summary_ok_outcome() {
        let t = MqttSendTool;
        let input = serde_json::json!({
            "to": "mqtt:ha:foo",
            "body": "x",
            "_outcome": { "ok": true },
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            html.contains("brenn-mqtt-status-ok"),
            "expected ok badge: {html}"
        );
    }

    #[test]
    fn mqtt_send_summary_err_outcome() {
        let t = MqttSendTool;
        let input = serde_json::json!({
            "to": "mqtt:ha:foo",
            "body": "x",
            "_outcome": { "ok": false, "error": "not connected" },
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            html.contains("brenn-mqtt-status-err"),
            "expected err badge: {html}"
        );
        assert!(html.contains("not connected"), "missing error text: {html}");
    }

    #[test]
    fn mqtt_send_summary_escapes_html_in_to() {
        let t = MqttSendTool;
        let input = serde_json::json!({
            "to": "mqtt:ha:<script>",
            "body": "x",
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            !html.contains("<script>"),
            "to field must be HTML-escaped: {html}"
        );
        assert!(
            html.contains("&lt;script&gt;"),
            "missing escaped to: {html}"
        );
    }
}
