//! AskUserQuestion tool — renders CC's structured question dialog.
//!
//! Produces a `<brenn-ask-user-question>` custom element with embedded JSON
//! containing the questions, server-rendered markdown HTML, and settings.
//! The component handles all user interaction and dispatches `brenn-tool-response`.

use brenn_lib::app::AppTool;
use brenn_lib::util::{html_escape, json_for_script_tag};
use brenn_lib::ws_types::ToolResponseDecision;
use tracing::warn;

pub struct AskUserQuestionTool;

impl AppTool for AskUserQuestionTool {
    fn name(&self) -> &str {
        "AskUserQuestion"
    }

    fn format_display(&self, tool_input: &serde_json::Value) -> Option<String> {
        let Some(questions) = tool_input.get("questions").and_then(|q| q.as_array()) else {
            warn!("AskUserQuestion tool_input missing or malformed 'questions' field");
            return None;
        };

        // Build rendered markdown for each question's text, labels, and descriptions.
        let rendered_questions: Vec<serde_json::Value> = questions
            .iter()
            .map(|q| {
                let question_html = q
                    .get("question")
                    .and_then(|v| v.as_str())
                    .map(crate::markdown::render_markdown)
                    .unwrap_or_default();

                let options: Vec<serde_json::Value> = q
                    .get("options")
                    .and_then(|o| o.as_array())
                    .map(|opts| {
                        opts.iter()
                            .map(|opt| {
                                let label_html = opt
                                    .get("label")
                                    .and_then(|v| v.as_str())
                                    .map(crate::markdown::render_markdown)
                                    .unwrap_or_default();
                                let description_html = opt
                                    .get("description")
                                    .and_then(|v| v.as_str())
                                    .map(crate::markdown::render_markdown)
                                    .unwrap_or_default();
                                serde_json::json!({
                                    "label_html": label_html,
                                    "description_html": description_html,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                serde_json::json!({
                    "question_html": question_html,
                    "options": options,
                })
            })
            .collect();

        // Determine enter_sends: true for single-select single-question.
        let enter_sends = questions.len() == 1
            && !questions[0]
                .get("multiSelect")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

        let payload = serde_json::json!({
            "questions": tool_input["questions"],
            "rendered": { "questions": rendered_questions },
            "enter_sends": enter_sends,
        });

        let payload_json = json_for_script_tag(&payload);

        Some(format!(
            "<brenn-ask-user-question>\n\
             <script type=\"application/json\">{payload_json}</script>\n\
             </brenn-ask-user-question>"
        ))
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let mut out = String::new();
        let denied = matches!(decision, ToolResponseDecision::Deny { .. });

        // Extract answers from the Allow decision's updated_input.
        let answers = match decision {
            ToolResponseDecision::Allow {
                updated_input: Some(ui),
            } => ui.get("answers").cloned(),
            _ => None,
        };

        if let Some(questions) = tool_input.get("questions").and_then(|q| q.as_array()) {
            for q in questions {
                let header = q
                    .get("header")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Question");
                let question_text = q.get("question").and_then(|v| v.as_str()).unwrap_or("");

                out.push_str(&format!(
                    r#"<div class="ts-question"><strong>{}</strong>: {}"#,
                    html_escape(header),
                    html_escape(question_text),
                ));

                // Show the answer if available.
                if let Some(answers) = &answers
                    && let Some(answer) = answers.get(question_text).and_then(|v| v.as_str())
                {
                    out.push_str(&format!(
                        r#" <span class="ts-answer">&rarr; {}</span>"#,
                        html_escape(answer),
                    ));
                }

                out.push_str("</div>");
            }
        }

        if denied {
            out.push_str(r#"<div class="ts-denied-note">Cancelled</div>"#);
        }

        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_display_produces_component_tag() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({
            "questions": [{
                "header": "Color",
                "question": "Pick a color",
                "options": [
                    {"label": "Red", "description": "A warm color"},
                    {"label": "Blue", "description": "A cool color"}
                ],
                "multiSelect": false
            }]
        });
        let html = tool.format_display(&input).unwrap();
        assert!(
            html.contains("<brenn-ask-user-question>"),
            "should have component tag: {html}"
        );
        assert!(
            html.contains("application/json"),
            "should have script tag: {html}"
        );
        assert!(
            html.contains("Pick a color"),
            "should contain question text: {html}"
        );
        assert!(
            html.contains("enter_sends"),
            "should contain enter_sends: {html}"
        );
    }

    #[test]
    fn format_display_returns_none_on_bad_input() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({"not_questions": true});
        assert!(tool.format_display(&input).is_none());
    }

    #[test]
    fn format_display_enter_sends_true_for_single_select() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({
            "questions": [{"header": "Q", "question": "?", "options": [], "multiSelect": false}]
        });
        let html = tool.format_display(&input).unwrap();
        assert!(
            html.contains("\"enter_sends\":true"),
            "single-select single-question should have enter_sends true: {html}"
        );
    }

    #[test]
    fn format_display_enter_sends_false_for_multi_select() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({
            "questions": [{"header": "Q", "question": "?", "options": [], "multiSelect": true}]
        });
        let html = tool.format_display(&input).unwrap();
        assert!(
            html.contains("\"enter_sends\":false"),
            "multi-select should have enter_sends false: {html}"
        );
    }

    #[test]
    fn format_summary_with_answers() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({
            "questions": [{"header": "Color", "question": "Favorite?", "options": [], "multiSelect": false}]
        });
        let decision = ToolResponseDecision::Allow {
            updated_input: Some(serde_json::json!({
                "answers": {"Favorite?": "blue"}
            })),
        };
        let html = tool.format_summary(&input, &decision).unwrap();
        assert!(html.contains("Favorite?"), "got: {html}");
        assert!(html.contains("blue"), "should contain answer: {html}");
        assert!(
            html.contains("ts-answer"),
            "should have answer class: {html}"
        );
    }

    #[test]
    fn format_summary_denied() {
        let tool = AskUserQuestionTool;
        let input = serde_json::json!({
            "questions": [{"header": "Q", "question": "test?", "options": [], "multiSelect": false}]
        });
        let decision = ToolResponseDecision::Deny { reason: None };
        let html = tool.format_summary(&input, &decision).unwrap();
        assert!(html.contains("Cancelled"), "got: {html}");
    }
}
