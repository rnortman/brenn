//! Tool-specific formatting for approval dialogs and tool-use summaries.
//!
//! Converts tool name + raw JSON input into human-readable HTML for display
//! in the browser's approval container (interactive) and chat history (read-only).
//! Application logic stays in Rust; the frontend just sets innerHTML.
//!
//! Registered `AppTool` implementations get first priority. Built-in CC tools
//! (Bash, Edit, Write, Read) use hardcoded formatters that wrap content in
//! `<brenn-tool-approve>`. Unknown tools get a JSON fallback, also wrapped.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::app::{AppTool, wrap_in_tool_approve};
use brenn_lib::approval_rules::ApprovalMatch;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

/// Format a tool's input as HTML for display in the approval container.
///
/// Dispatch order:
/// 1. Registered `AppTool` via `tool_registry` — if it returns `Some(html)`, use it.
/// 2. Built-in formatters for CC's standard tools (Bash, Edit, Write, Read) —
///    output is wrapped in `<brenn-tool-approve>` with embedded default patterns.
/// 3. JSON fallback for unknown tools — also wrapped in `<brenn-tool-approve>`.
pub fn format_tool_display(
    tool_registry: &HashMap<String, Arc<dyn AppTool>>,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> String {
    // Try registered tool first.
    if let Some(tool) = tool_registry.get(tool_name)
        && let Some(html) = tool.format_display(tool_input)
    {
        return html;
    }

    // Built-in formatters for CC's standard tools.
    let content = match tool_name {
        "Bash" => format_bash(tool_input),
        "Edit" => format_edit(tool_input),
        "Write" => format_write(tool_input),
        "Read" => format_read(tool_input),
        _ => format_fallback(tool_input),
    };

    wrap_in_tool_approve(tool_name, tool_input, &content)
}

fn format_bash(input: &serde_json::Value) -> String {
    let command = input
        .get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("<no command>");
    format!(r#"<pre class="tool-code">{}</pre>"#, html_escape(command))
}

fn format_edit(input: &serde_json::Value) -> String {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    let old_string = input
        .get("old_string")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let new_string = input
        .get("new_string")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    format!(
        r#"<div class="tool-file">{}</div><div class="tool-diff"><div class="tool-diff-old"><pre>{}</pre></div><div class="tool-diff-new"><pre>{}</pre></div></div>"#,
        html_escape(file_path),
        html_escape(old_string),
        html_escape(new_string),
    )
}

fn format_write(input: &serde_json::Value) -> String {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    let content = input.get("content").and_then(|v| v.as_str()).unwrap_or("");

    // Truncate long content for display (UTF-8 safe).
    let display_content = brenn_lib::util::truncate_with_marker(content, 2000);

    format!(
        r#"<div class="tool-file">{}</div><pre class="tool-code">{}</pre>"#,
        html_escape(file_path),
        html_escape(&display_content),
    )
}

fn format_read(input: &serde_json::Value) -> String {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");

    format!(r#"<div class="tool-file">{}</div>"#, html_escape(file_path))
}

fn format_fallback(input: &serde_json::Value) -> String {
    let json = serde_json::to_string_pretty(input).unwrap_or_else(|_| input.to_string());
    format!(r#"<pre class="tool-code">{}</pre>"#, html_escape(&json))
}

// ---------------------------------------------------------------------------
// Tool-use summaries (read-only, compact, for chat history)
// ---------------------------------------------------------------------------

/// Render a compact read-only summary of a completed tool use for inline
/// display in the chat history. The `decision` determines whether the tool
/// was allowed (with optional modified input) or denied.
///
/// Dispatch order mirrors `format_tool_display`: registered tools first,
/// then built-in formatters, then fallback.
pub fn format_tool_summary(
    tool_registry: &HashMap<String, Arc<dyn AppTool>>,
    tool_name: &str,
    tool_input: &serde_json::Value,
    decision: &ToolResponseDecision,
) -> String {
    // Try registered tool first.
    if let Some(tool) = tool_registry.get(tool_name)
        && let Some(html) = tool.format_summary(tool_input, decision)
    {
        return html;
    }

    let denied = matches!(decision, ToolResponseDecision::Deny { .. });
    let detail = match tool_name {
        "Bash" => summary_bash(tool_input),
        "Edit" => summary_edit(tool_input),
        "Write" => summary_write(tool_input),
        "Read" => summary_read(tool_input),
        "Glob" => summary_glob(tool_input),
        "Grep" => summary_grep(tool_input),
        "ToolSearch" => summary_toolsearch(tool_input),
        t if t.contains("DisplayFile") => summary_display_file(tool_input),
        _ => summary_fallback(tool_name, tool_input),
    };
    if denied {
        format!(r#"<span class="ts-denied" title="Denied">✘</span> {detail}"#)
    } else {
        detail
    }
}

/// Maximum display size for pretty-printed JSON in detail view (bytes).
/// Applied to the pretty-printed representation, not the raw JSON.
/// Shared with graf error truncation via `brenn_lib::util::GRAF_ERROR_MAX_BYTES`.
const DETAIL_JSON_MAX_DISPLAY: usize = brenn_lib::util::GRAF_ERROR_MAX_BYTES;

/// Render the expanded detail view for a tool-use summary.
///
/// Contains approval info, pretty-printed input JSON, and (for approved tools)
/// pretty-printed result JSON. Returns HTML to be set inside a `<details>` body.
pub fn format_tool_detail(
    tool_input: &serde_json::Value,
    tool_response: Option<&serde_json::Value>,
    decision: &ToolResponseDecision,
    approval_match: Option<&ApprovalMatch>,
) -> String {
    let mut html = String::with_capacity(512);

    // --- Approval info ---
    html.push_str(r#"<div class="td-approval">"#);
    let denied = matches!(decision, ToolResponseDecision::Deny { .. });
    if denied {
        let reason = match decision {
            ToolResponseDecision::Deny { reason } => reason.as_deref(),
            _ => None,
        };
        if let Some(r) = reason {
            html.push_str(&format!(
                r#"<span class="td-denied">Denied:</span> {}"#,
                html_escape(r)
            ));
        } else {
            html.push_str(r#"<span class="td-denied">Denied</span>"#);
        }
    } else {
        match approval_match {
            Some(ApprovalMatch::GlobalTool) => {
                html.push_str(
                    r#"<span class="td-auto">Auto-approved:</span> global read-only tool"#,
                );
            }
            Some(ApprovalMatch::ConfigRule { pattern }) => {
                html.push_str(&format!(
                    r#"<span class="td-auto">Auto-approved:</span> config rule <code>{}</code>"#,
                    html_escape(pattern)
                ));
            }
            Some(ApprovalMatch::AlwaysAllowRule { pattern }) => {
                html.push_str(&format!(
                    r#"<span class="td-auto">Auto-approved:</span> always-allow rule <code>{}</code>"#,
                    html_escape(pattern)
                ));
            }
            Some(ApprovalMatch::NoMatch) => {
                html.push_str(r#"<span class="td-manual">Approved by user</span>"#);
            }
            None => {
                html.push_str(r#"<span class="td-auto">Auto-approved by CC</span>"#);
            }
        }
    }
    html.push_str("</div>");

    // --- Tool input ---
    html.push_str(r#"<div class="td-section"><div class="td-heading">Input</div>"#);
    let input_json =
        serde_json::to_string_pretty(tool_input).unwrap_or_else(|_| tool_input.to_string());
    let input_display = truncate_detail_json(&input_json);
    html.push_str(&format!(
        r#"<pre class="td-json">{}</pre>"#,
        html_escape(&input_display)
    ));
    html.push_str("</div>");

    // --- Tool result (only if approved and present) ---
    if !denied && let Some(response) = tool_response {
        html.push_str(r#"<div class="td-section"><div class="td-heading">Result</div>"#);
        let result_display = format_tool_response_display(response);
        html.push_str(&format!(
            r#"<pre class="td-json">{}</pre>"#,
            html_escape(&result_display)
        ));
        html.push_str("</div>");
    }

    html
}

/// Format a tool response for display. Handles both string and structured responses.
fn format_tool_response_display(response: &serde_json::Value) -> String {
    // Tool responses from CC are often just strings (file contents, command output).
    // For Bash results in particular, the output is typically a JSON array with a
    // single text content block. Try to extract something readable.
    let display = if let Some(s) = response.as_str() {
        s.to_string()
    } else if response.is_array() {
        // Array of content blocks — try to concatenate text blocks.
        let texts: Vec<&str> = response
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|block| {
                if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                    block.get("text").and_then(|t| t.as_str())
                } else {
                    None
                }
            })
            .collect();
        if texts.is_empty() {
            serde_json::to_string_pretty(response).unwrap_or_else(|_| response.to_string())
        } else {
            texts.join("\n")
        }
    } else {
        serde_json::to_string_pretty(response).unwrap_or_else(|_| response.to_string())
    };

    truncate_detail_json(&display)
}

/// Truncate pretty-printed text for detail display (UTF-8 safe).
fn truncate_detail_json(text: &str) -> String {
    brenn_lib::util::truncate_with_marker(text, DETAIL_JSON_MAX_DISPLAY)
}

fn summary_bash(input: &serde_json::Value) -> String {
    let command = input
        .get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("<no command>");
    let display = truncate_summary(command, 120);
    format!(r#"<code class="ts-cmd">{}</code>"#, html_escape(&display))
}

fn summary_file_field(input: &serde_json::Value, field_name: &str) -> String {
    let value = input
        .get(field_name)
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    format!(r#"<span class="ts-file">{}</span>"#, html_escape(value))
}

fn summary_edit(input: &serde_json::Value) -> String {
    summary_file_field(input, "file_path")
}

fn summary_write(input: &serde_json::Value) -> String {
    summary_file_field(input, "file_path")
}

fn summary_read(input: &serde_json::Value) -> String {
    summary_file_field(input, "file_path")
}

fn summary_glob(input: &serde_json::Value) -> String {
    let pattern = input.get("pattern").and_then(|v| v.as_str()).unwrap_or("*");
    format!(
        r#"<code class="ts-pattern">{}</code>"#,
        html_escape(pattern)
    )
}

fn summary_grep(input: &serde_json::Value) -> String {
    let pattern = input.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
    let path = input.get("path").and_then(|v| v.as_str()).unwrap_or(".");
    format!(
        r#"<code class="ts-pattern">{}</code> in <span class="ts-file">{}</span>"#,
        html_escape(pattern),
        html_escape(path),
    )
}

fn summary_toolsearch(input: &serde_json::Value) -> String {
    let query = input.get("query").and_then(|v| v.as_str()).unwrap_or("");
    format!(r#"<code class="ts-pattern">{}</code>"#, html_escape(query))
}

fn summary_display_file(input: &serde_json::Value) -> String {
    let file_path = input
        .get("file_path")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    format!(
        r#"<span class="ts-file ts-artifact" data-artifact-path="{}">{}</span>"#,
        html_escape(file_path),
        html_escape(file_path),
    )
}

fn summary_fallback(tool_name: &str, input: &serde_json::Value) -> String {
    let json = serde_json::to_string_pretty(input).unwrap_or_else(|_| input.to_string());
    let display = truncate_summary(&json, 200);
    format!(
        r#"<span class="ts-file">{}</span> <code class="ts-pattern">{}</code>"#,
        html_escape(tool_name),
        html_escape(&display),
    )
}

/// Truncate text for compact summary display (UTF-8 safe).
fn truncate_summary(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text.to_string();
    }
    let byte_end = text
        .char_indices()
        .nth(max_chars)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    format!("{}…", &text[..byte_end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_registry() -> HashMap<String, Arc<dyn AppTool>> {
        HashMap::new()
    }

    #[test]
    fn bash_formats_command_in_tool_approve() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "ls -la"});
        let html = format_tool_display(&reg, "Bash", &input);
        assert!(html.contains("ls -la"));
        assert!(html.contains("tool-code"));
        assert!(
            html.contains("<brenn-tool-approve"),
            "should be wrapped: {html}"
        );
        assert!(
            html.contains("tool-name=\"Bash\""),
            "should have tool name attr: {html}"
        );
    }

    #[test]
    fn bash_escapes_html_in_command() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "echo '<script>alert(1)</script>'"});
        let html = format_tool_display(&reg, "Bash", &input);
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn edit_shows_file_and_diff() {
        let reg = empty_registry();
        let input = serde_json::json!({
            "file_path": "/tmp/test.rs",
            "old_string": "fn old() {}",
            "new_string": "fn new() {}"
        });
        let html = format_tool_display(&reg, "Edit", &input);
        assert!(html.contains("/tmp/test.rs"));
        assert!(html.contains("fn old() {}"));
        assert!(html.contains("fn new() {}"));
        assert!(html.contains("tool-diff-old"));
        assert!(html.contains("tool-diff-new"));
        assert!(html.contains("<brenn-tool-approve"));
    }

    #[test]
    // Tests the Write formatter's own 2000-byte truncation threshold via
    // format_write -> truncate_with_marker(content, 2000). Does NOT exercise
    // truncate_detail_json (which uses DETAIL_JSON_MAX_DISPLAY = 4096 via
    // format_tool_detail). See detail_truncates_large_result for that path.
    fn write_truncates_long_content() {
        let reg = empty_registry();
        let content = "x".repeat(3000);
        let input = serde_json::json!({
            "file_path": "/tmp/big.txt",
            "content": content
        });
        let html = format_tool_display(&reg, "Write", &input);
        assert!(html.contains("[truncated, 3000 bytes total]"));
    }

    #[test]
    // Same as write_truncates_long_content but with multibyte chars. Tests the
    // Write formatter's 2000-byte path, not truncate_detail_json.
    fn write_truncates_multibyte_safely() {
        let reg = empty_registry();
        let content = "é".repeat(1500); // 3000 bytes
        let input = serde_json::json!({
            "file_path": "/tmp/test.txt",
            "content": content
        });
        let html = format_tool_display(&reg, "Write", &input);
        assert!(html.contains("[truncated, 3000 bytes total]"));
    }

    #[test]
    fn unknown_tool_uses_json_fallback_wrapped() {
        let reg = empty_registry();
        let input = serde_json::json!({"foo": "bar"});
        let html = format_tool_display(&reg, "UnknownTool", &input);
        assert!(html.contains("foo"));
        assert!(html.contains("bar"));
        assert!(
            html.contains("<brenn-tool-approve"),
            "should be wrapped: {html}"
        );
    }

    #[test]
    fn default_patterns_embedded_in_script_tag() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "ls -la"});
        let html = format_tool_display(&reg, "Bash", &input);
        assert!(
            html.contains("default_patterns"),
            "should embed patterns: {html}"
        );
        assert!(
            html.contains("application/json"),
            "should have script tag: {html}"
        );
    }

    #[test]
    fn registered_tool_takes_priority() {
        struct CustomTool;
        impl AppTool for CustomTool {
            fn name(&self) -> &str {
                "mcp__test__custom"
            }
            fn format_display(&self, _input: &serde_json::Value) -> Option<String> {
                Some("<div class=\"custom-display\">custom approval</div>".to_string())
            }
        }

        let mut reg: HashMap<String, Arc<dyn AppTool>> = HashMap::new();
        reg.insert("mcp__test__custom".into(), Arc::new(CustomTool));

        let input = serde_json::json!({"data": "value"});
        let html = format_tool_display(&reg, "mcp__test__custom", &input);
        assert!(
            html.contains("custom-display"),
            "should use registered tool: {html}"
        );
        assert!(
            !html.contains("<brenn-tool-approve"),
            "should NOT be wrapped: {html}"
        );
    }

    #[test]
    fn registered_tool_falls_through_to_builtins() {
        // A registered tool that returns None falls through to built-in formatters.
        struct NullTool;
        impl AppTool for NullTool {
            fn name(&self) -> &str {
                "Bash"
            }
        }

        let mut reg: HashMap<String, Arc<dyn AppTool>> = HashMap::new();
        reg.insert("Bash".into(), Arc::new(NullTool));

        let input = serde_json::json!({"command": "ls"});
        let html = format_tool_display(&reg, "Bash", &input);
        // Should fall through to the built-in Bash formatter.
        assert!(html.contains("ls"), "should use built-in: {html}");
        assert!(
            html.contains("<brenn-tool-approve"),
            "should be wrapped: {html}"
        );
    }

    // --- Tool summary tests ---

    fn allowed() -> ToolResponseDecision {
        ToolResponseDecision::Allow {
            updated_input: None,
        }
    }

    fn denied() -> ToolResponseDecision {
        ToolResponseDecision::Deny { reason: None }
    }

    #[test]
    fn summary_bash_shows_command() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "cargo build"});
        let html = format_tool_summary(&reg, "Bash", &input, &allowed());
        assert!(html.contains("cargo build"), "got: {html}");
        assert!(html.contains("ts-cmd"), "should have cmd class: {html}");
    }

    #[test]
    fn summary_bash_denied() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "rm -rf /"});
        let html = format_tool_summary(&reg, "Bash", &input, &denied());
        assert!(html.contains("ts-denied"), "should show denied: {html}");
        assert!(
            html.contains("rm -rf /"),
            "should still show command: {html}"
        );
    }

    #[test]
    fn summary_edit_shows_file() {
        let reg = empty_registry();
        let input =
            serde_json::json!({"file_path": "src/main.rs", "old_string": "a", "new_string": "b"});
        let html = format_tool_summary(&reg, "Edit", &input, &allowed());
        assert!(html.contains("src/main.rs"), "got: {html}");
    }

    #[test]
    fn summary_read_shows_file() {
        let reg = empty_registry();
        let input = serde_json::json!({"file_path": "/home/user/code.rs"});
        let html = format_tool_summary(&reg, "Read", &input, &allowed());
        assert!(html.contains("/home/user/code.rs"), "got: {html}");
    }

    #[test]
    fn summary_grep_shows_pattern_and_path() {
        let reg = empty_registry();
        let input = serde_json::json!({"pattern": "fn main", "path": "src/"});
        let html = format_tool_summary(&reg, "Grep", &input, &allowed());
        assert!(html.contains("fn main"), "got: {html}");
        assert!(html.contains("src/"), "got: {html}");
    }

    #[test]
    fn summary_display_file_has_artifact_attr() {
        let reg = empty_registry();
        let input = serde_json::json!({"file_path": "docs/plan.md"});
        let html = format_tool_summary(&reg, "mcp__brenn__DisplayFile", &input, &allowed());
        assert!(
            html.contains("data-artifact-path"),
            "should have artifact data attr: {html}"
        );
        assert!(html.contains("docs/plan.md"), "got: {html}");
    }

    #[test]
    fn summary_registered_tool_takes_priority() {
        struct CustomTool;
        impl AppTool for CustomTool {
            fn name(&self) -> &str {
                "mcp__test__custom"
            }
            fn format_summary(
                &self,
                _input: &serde_json::Value,
                _decision: &ToolResponseDecision,
            ) -> Option<String> {
                Some("<span class=\"custom-summary\">custom</span>".to_string())
            }
        }

        let mut reg: HashMap<String, Arc<dyn AppTool>> = HashMap::new();
        reg.insert("mcp__test__custom".into(), Arc::new(CustomTool));

        let input = serde_json::json!({"data": "value"});
        let html = format_tool_summary(&reg, "mcp__test__custom", &input, &allowed());
        assert!(
            html.contains("custom-summary"),
            "should use registered tool: {html}"
        );
    }

    #[test]
    fn summary_escapes_html() {
        let reg = empty_registry();
        let input = serde_json::json!({"command": "<script>alert(1)</script>"});
        let html = format_tool_summary(&reg, "Bash", &input, &allowed());
        assert!(!html.contains("<script>"), "should escape: {html}");
        assert!(html.contains("&lt;script&gt;"), "got: {html}");
    }

    #[test]
    fn summary_truncates_long_bash() {
        let reg = empty_registry();
        let long_cmd = "x".repeat(200);
        let input = serde_json::json!({"command": long_cmd});
        let html = format_tool_summary(&reg, "Bash", &input, &allowed());
        assert!(html.contains('…'), "should truncate: {html}");
    }

    // --- Tool detail tests ---

    #[test]
    fn detail_global_tool_shows_auto_approved() {
        let input = serde_json::json!({"file_path": "/tmp/test.rs"});
        let response = serde_json::json!("file contents here");
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::GlobalTool),
        );
        assert!(html.contains("Auto-approved"), "got: {html}");
        assert!(html.contains("global read-only tool"), "got: {html}");
        assert!(html.contains("td-auto"), "got: {html}");
        assert!(html.contains("/tmp/test.rs"), "input shown: {html}");
        assert!(html.contains("file contents here"), "result shown: {html}");
    }

    #[test]
    fn detail_config_rule_shows_pattern() {
        let input = serde_json::json!({"command": "cargo test"});
        let response = serde_json::json!("ok");
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::ConfigRule {
                pattern: r"cargo\b.*".to_string(),
            }),
        );
        assert!(html.contains("config rule"), "got: {html}");
        assert!(html.contains(r"cargo\b.*"), "pattern shown: {html}");
    }

    #[test]
    fn detail_always_allow_rule_shows_pattern() {
        let input = serde_json::json!({"command": "git status"});
        let html = format_tool_detail(
            &input,
            None,
            &allowed(),
            Some(&ApprovalMatch::AlwaysAllowRule {
                pattern: r"git status\b.*".to_string(),
            }),
        );
        assert!(html.contains("always-allow rule"), "got: {html}");
        assert!(html.contains(r"git status\b.*"), "pattern shown: {html}");
    }

    #[test]
    fn detail_manual_approval() {
        let input = serde_json::json!({"command": "rm -rf /"});
        let response = serde_json::json!("done");
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::NoMatch),
        );
        assert!(html.contains("Approved by user"), "got: {html}");
        assert!(html.contains("td-manual"), "got: {html}");
        assert!(html.contains("rm -rf /"), "input shown: {html}");
        assert!(html.contains("done"), "result shown: {html}");
    }

    #[test]
    fn detail_denied_shows_reason_no_result() {
        let input = serde_json::json!({"command": "rm -rf /"});
        let response = serde_json::json!("should not appear");
        let html = format_tool_detail(
            &input,
            Some(&response),
            &ToolResponseDecision::Deny {
                reason: Some("dangerous command".to_string()),
            },
            Some(&ApprovalMatch::NoMatch),
        );
        assert!(html.contains("Denied"), "got: {html}");
        assert!(html.contains("dangerous command"), "reason shown: {html}");
        assert!(html.contains("td-denied"), "got: {html}");
        // Result section should NOT be rendered for denied tools.
        assert!(
            !html.contains(">Result<"),
            "result section heading should not appear: {html}"
        );
        assert!(
            !html.contains("should not appear"),
            "result content should be hidden for denied: {html}"
        );
    }

    #[test]
    fn detail_denied_no_reason() {
        let input = serde_json::json!({"command": "nope"});
        let html = format_tool_detail(
            &input,
            None,
            &ToolResponseDecision::Deny { reason: None },
            Some(&ApprovalMatch::NoMatch),
        );
        assert!(html.contains("Denied"), "got: {html}");
        assert!(!html.contains("Denied:"), "no colon without reason: {html}");
    }

    #[test]
    fn detail_truncates_large_result() {
        let input = serde_json::json!({"file_path": "big.txt"});
        let big_result = serde_json::json!("x".repeat(5000));
        let html = format_tool_detail(
            &input,
            Some(&big_result),
            &allowed(),
            Some(&ApprovalMatch::GlobalTool),
        );
        assert!(html.contains("truncated"), "should truncate: {html}");
        assert!(
            html.contains("5000 bytes total"),
            "should report size: {html}"
        );
        // Input section should NOT be truncated (it's small).
        assert!(html.contains("big.txt"), "input still shown: {html}");
    }

    #[test]
    fn detail_extracts_text_content_blocks() {
        let input = serde_json::json!({"command": "echo hi"});
        let response = serde_json::json!([
            {"type": "text", "text": "hello world"},
            {"type": "text", "text": "second block"}
        ]);
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::NoMatch),
        );
        assert!(html.contains("hello world"), "got: {html}");
        assert!(html.contains("second block"), "got: {html}");
    }

    #[test]
    fn detail_non_text_content_blocks_fall_back_to_json() {
        let input = serde_json::json!({"command": "ls"});
        let response = serde_json::json!([
            {"type": "image", "url": "http://example.com/img.png"}
        ]);
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::NoMatch),
        );
        // No text blocks → should fall back to pretty-printed JSON.
        assert!(html.contains("image"), "got: {html}");
        assert!(html.contains("example.com"), "got: {html}");
    }

    #[test]
    fn detail_object_response_pretty_prints() {
        let input = serde_json::json!({"command": "curl api"});
        let response = serde_json::json!({"status": 200, "body": "ok"});
        let html = format_tool_detail(
            &input,
            Some(&response),
            &allowed(),
            Some(&ApprovalMatch::NoMatch),
        );
        assert!(html.contains("status"), "got: {html}");
        assert!(html.contains("200"), "got: {html}");
    }

    #[test]
    fn detail_escapes_html_in_input() {
        let input = serde_json::json!({"command": "<script>alert(1)</script>"});
        let html = format_tool_detail(&input, None, &allowed(), Some(&ApprovalMatch::NoMatch));
        assert!(!html.contains("<script>alert"), "should escape: {html}");
        assert!(html.contains("&lt;script&gt;"), "got: {html}");
    }

    #[test]
    fn detail_escapes_html_in_pattern() {
        let input = serde_json::json!({"command": "test"});
        let html = format_tool_detail(
            &input,
            None,
            &allowed(),
            Some(&ApprovalMatch::ConfigRule {
                pattern: "<b>bold</b>".to_string(),
            }),
        );
        assert!(
            !html.contains("<b>bold</b>"),
            "pattern should be escaped: {html}"
        );
        assert!(html.contains("&lt;b&gt;"), "got: {html}");
    }

    #[test]
    fn detail_no_result_section_when_none() {
        let input = serde_json::json!({"file_path": "test.rs"});
        let html = format_tool_detail(&input, None, &allowed(), Some(&ApprovalMatch::GlobalTool));
        // Should have Input section but no Result section.
        assert!(html.contains(">Input<"), "got: {html}");
        assert!(!html.contains(">Result<"), "no result section: {html}");
    }

    fn export_usage_registry() -> Arc<HashMap<String, Arc<dyn AppTool>>> {
        // Use the full built-in registry so ExportUsageTool is registered.
        crate::tools::build_tool_registry(vec![])
    }

    /// _git_sync_mount present → warning div with the slug.
    #[test]
    fn export_usage_git_synced_shows_warning() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
            "_git_sync_mount": "life",
        });
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            html.contains("tool-export-warn"),
            "must include warning class: {html}"
        );
        assert!(html.contains("life"), "must include mount slug: {html}");
        assert!(
            html.contains("git-synced"),
            "must mention git-synced: {html}"
        );
    }

    /// No _git_sync_mount → no warning div.
    #[test]
    fn export_usage_no_git_sync_mount_no_warning() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
        });
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            !html.contains("tool-export-warn"),
            "must not include warning class when _git_sync_mount absent: {html}"
        );
    }

    /// HTML in the _git_sync_mount slug is escaped; no injection into the warning div.
    #[test]
    fn export_usage_escapes_html_in_git_sync_mount() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
            "_git_sync_mount": "<evil>",
        });
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            html.contains("&lt;evil&gt;"),
            "slug must be HTML-escaped: {html}"
        );
        assert!(
            !html.contains("<evil>"),
            "raw slug tag must not appear in output: {html}"
        );
    }

    #[test]
    fn export_usage_shows_output_file_and_scope() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
        });
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            html.contains("/data/exports/usage.csv"),
            "must show output_file: {html}"
        );
        assert!(html.contains("sessions"), "must show kind: {html}");
        assert!(
            html.contains("row scope: your account only"),
            "must confirm caller-only row scope: {html}"
        );
        assert!(
            html.contains("<brenn-tool-approve"),
            "must be wrapped in tool-approve: {html}"
        );
    }

    #[test]
    fn export_usage_summary_shows_output_file() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "events",
            "format": "json",
        });
        let html = format_tool_summary(
            &reg,
            "mcp__brenn__ExportUsage",
            &input,
            &ToolResponseDecision::Allow {
                updated_input: None,
            },
        );
        assert!(
            html.contains("/data/exports/usage.csv"),
            "summary must show output_file: {html}"
        );
        assert!(html.contains("ts-file"), "must have ts-file class: {html}");
    }

    #[test]
    fn export_usage_escapes_html_in_output_file() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/<evil>/usage.csv",
            "kind": "sessions",
            "format": "csv",
        });
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            !html.contains("<evil>"),
            "must escape HTML in output_file: {html}"
        );
        assert!(
            html.contains("&lt;evil&gt;"),
            "must have escaped form: {html}"
        );
    }

    /// `format_tool_display` for ExportUsage returns a `tool-export-error` div
    /// when `output_file` is absent from the tool input.
    #[test]
    fn export_usage_display_missing_output_file_returns_error_div() {
        let reg = export_usage_registry();
        let input = serde_json::json!({});
        let html = format_tool_display(&reg, "mcp__brenn__ExportUsage", &input);
        assert!(
            html.contains("tool-export-error"),
            "must include tool-export-error class for missing output_file: {html}"
        );
        assert!(
            html.contains("missing required field output_file"),
            "must explain the missing field: {html}"
        );
    }

    /// `format_tool_summary` for ExportUsage returns a `ts-error` span
    /// when `output_file` is absent from the tool input.
    #[test]
    fn export_usage_summary_missing_output_file_returns_error_span() {
        let reg = export_usage_registry();
        let input = serde_json::json!({});
        let html = format_tool_summary(&reg, "mcp__brenn__ExportUsage", &input, &allowed());
        assert!(
            html.contains("ts-error"),
            "must include ts-error class for missing output_file: {html}"
        );
        assert!(
            html.contains("missing output_file"),
            "must mention missing field: {html}"
        );
    }

    /// End-to-end: format_tool_summary with Deny dispatches through registry and
    /// applies the denied prefix internally in ExportUsageTool (not in the outer
    /// format_tool_summary fallback path). Catches a regression in registry early-
    /// return at line 131 that would bypass denied-prefix for registered tools.
    #[test]
    fn export_usage_summary_deny_end_to_end() {
        let reg = export_usage_registry();
        let input = serde_json::json!({
            "output_file": "/data/exports/usage.csv",
            "kind": "sessions",
            "format": "csv",
        });
        let html = format_tool_summary(
            &reg,
            "mcp__brenn__ExportUsage",
            &input,
            &ToolResponseDecision::Deny { reason: None },
        );
        assert!(
            html.contains("ts-denied"),
            "denied summary must have ts-denied class: {html}"
        );
        assert!(
            html.contains("/data/exports/usage.csv"),
            "denied summary must show output_file: {html}"
        );
        assert!(
            html.contains("ts-file"),
            "denied summary must have ts-file class: {html}"
        );
    }

    #[test]
    fn detail_cc_auto_approved_shows_auto_approved_by_cc() {
        let input = serde_json::json!({"command": "git status"});
        let response = serde_json::json!("On branch main");
        let html = format_tool_detail(&input, Some(&response), &allowed(), None);
        assert!(
            html.contains("Auto-approved by CC"),
            "should show CC auto-approval: {html}"
        );
        assert!(html.contains("td-auto"), "got: {html}");
    }
}
