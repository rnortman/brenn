//! Parsing Claude Code PreToolUse hook input.
//!
//! Only *added* text is extracted: `content` for Write, `new_string` for
//! Edit. Existing file content is never scanned here, which grandfathers
//! prior violations with no path or line bookkeeping.

use std::path::PathBuf;

/// Added text above this size is skipped. Secrets buried in megabyte blobs
/// are the push gate's problem, and that layer has no cap.
pub const SIZE_CAP_BYTES: usize = 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub struct Added {
    pub file_path: PathBuf,
    pub text: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HookError {
    /// The matcher delivered a tool this wrapper does not know how to read.
    /// Passing it silently would be a hole in the scrub.
    UnknownTool(String),
}

fn required_str<'a>(input: &'a serde_json::Value, key: &str, tool: &str) -> &'a str {
    input
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("hook input for {tool} has no string `{key}` in tool_input"))
}

pub fn extract(payload: &serde_json::Value) -> Result<Added, HookError> {
    let tool = payload
        .get("tool_name")
        .and_then(|v| v.as_str())
        .expect("hook input has no string `tool_name`");
    let input = payload
        .get("tool_input")
        .expect("hook input has no `tool_input`");

    let text_key = match tool {
        "Write" => "content",
        "Edit" => "new_string",
        other => return Err(HookError::UnknownTool(other.to_string())),
    };

    Ok(Added {
        file_path: PathBuf::from(required_str(input, "file_path", tool)),
        text: required_str(input, text_key, tool).to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn write_yields_its_content() {
        let payload = json!({
            "tool_name": "Write",
            "tool_input": {"file_path": "/repo/src/a.rs", "content": "fn main() {}"}
        });
        assert_eq!(
            extract(&payload).unwrap(),
            Added {
                file_path: PathBuf::from("/repo/src/a.rs"),
                text: "fn main() {}".into()
            }
        );
    }

    #[test]
    fn edit_yields_new_string_only() {
        let payload = json!({
            "tool_name": "Edit",
            "tool_input": {
                "file_path": "/repo/src/a.rs",
                "old_string": "let name = \"grandfathered\";",
                "new_string": "let name = \"alice\";"
            }
        });
        let added = extract(&payload).unwrap();
        assert_eq!(added.text, "let name = \"alice\";");
        assert!(
            !added.text.contains("grandfathered"),
            "old_string must never be scanned"
        );
    }

    #[test]
    fn unknown_tool_is_an_error_not_a_silent_pass() {
        let payload = json!({"tool_name": "NotebookEdit", "tool_input": {}});
        assert_eq!(
            extract(&payload),
            Err(HookError::UnknownTool("NotebookEdit".into()))
        );
    }

    #[test]
    #[should_panic(expected = "no string `tool_name`")]
    fn missing_tool_name_panics() {
        extract(&json!({"tool_input": {}})).ok();
    }

    #[test]
    #[should_panic(expected = "no `tool_input`")]
    fn missing_tool_input_panics() {
        extract(&json!({"tool_name": "Write"})).ok();
    }

    #[test]
    #[should_panic(expected = "no string `content`")]
    fn write_without_content_panics() {
        extract(&json!({"tool_name": "Write", "tool_input": {"file_path": "/a.rs"}})).ok();
    }

    #[test]
    #[should_panic(expected = "no string `new_string`")]
    fn edit_without_new_string_panics() {
        extract(&json!({"tool_name": "Edit", "tool_input": {"file_path": "/a.rs"}})).ok();
    }

    #[test]
    #[should_panic(expected = "no string `file_path`")]
    fn missing_file_path_panics() {
        extract(&json!({"tool_name": "Write", "tool_input": {"content": "x"}})).ok();
    }
}
