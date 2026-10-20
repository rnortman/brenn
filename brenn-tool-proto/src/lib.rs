//! Wire contract of the per-conversation tool socket.
//!
//! One connection carries one request: a single line of JSON from the client,
//! a single line of JSON back, then the server closes. JSON escapes newlines,
//! so the newline is the only framing.
//!
//! ```jsonc
//! {"v":1,"op":"list"}
//! {"v":1,"op":"describe","tool":"BrennSend"}
//! {"v":1,"op":"call","tool":"BrennSend","input":{…}}
//!
//! {"v":1,"ok":true,"result":<value>}
//! {"v":1,"ok":false,"error":{"kind":"<kind>","detail":"<text>"}}
//! {"v":1,"ok":false,"error":{"kind":"tool_error","output":<the tool's error JSON>}}
//! ```

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The protocol version every request and response carries as `v`.
pub const PROTOCOL_VERSION: u32 = 1;

/// The largest request line the server reads, newline excluded. A transport
/// bound on what one connection may make the server buffer.
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The environment variable naming the socket inside the agent's environment.
pub const SOCKET_ENV: &str = "BRENN_TOOL_SOCKET";

/// A field that is present is `Some`, even when its value is JSON `null`;
/// only an absent field (through `#[serde(default)]`) is `None`.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// A request, as the client means it.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// The whole catalog, available or not, without schemas.
    List,
    /// One catalog entry with its input schema.
    Describe { tool: String },
    /// Run a tool.
    Call { tool: String, input: Value },
}

/// A request line as it is read.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequest {
    v: u32,
    op: String,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default, deserialize_with = "present")]
    input: Option<Value>,
}

/// A request line as it is written, borrowing the request's fields.
#[derive(Serialize)]
struct RawRequestOut<'a> {
    v: u32,
    op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<&'a Value>,
}

impl Request {
    /// The request as one JSON line, without the trailing newline.
    pub fn to_line(&self) -> String {
        let (op, tool, input) = match self {
            Request::List => ("list", None, None),
            Request::Describe { tool } => ("describe", Some(tool.as_str()), None),
            Request::Call { tool, input } => ("call", Some(tool.as_str()), Some(input)),
        };
        let raw = RawRequestOut {
            v: PROTOCOL_VERSION,
            op,
            tool,
            input,
        };
        serde_json::to_string(&raw).expect("a request always serializes")
    }

    /// Parse one request line. `Err` names what is wrong with it.
    pub fn parse(line: &[u8]) -> Result<Request, String> {
        let raw: RawRequest =
            serde_json::from_slice(line).map_err(|e| format!("malformed request: {e}"))?;
        if raw.v != PROTOCOL_VERSION {
            return Err(format!(
                "unsupported protocol version {} (this server speaks {PROTOCOL_VERSION})",
                raw.v
            ));
        }
        let field = |name: &str, present: bool| {
            if present {
                Ok(())
            } else {
                Err(format!("`{}` requires `{name}`", raw.op))
            }
        };
        let forbid = |name: &str, present: bool| {
            if present {
                Err(format!("`{}` takes no `{name}`", raw.op))
            } else {
                Ok(())
            }
        };
        match raw.op.as_str() {
            "list" => {
                forbid("tool", raw.tool.is_some())?;
                forbid("input", raw.input.is_some())?;
                Ok(Request::List)
            }
            "describe" => {
                field("tool", raw.tool.is_some())?;
                forbid("input", raw.input.is_some())?;
                Ok(Request::Describe {
                    tool: raw.tool.expect("checked present"),
                })
            }
            "call" => {
                field("tool", raw.tool.is_some())?;
                field("input", raw.input.is_some())?;
                Ok(Request::Call {
                    tool: raw.tool.expect("checked present"),
                    input: raw.input.expect("checked present"),
                })
            }
            other => Err(format!("unknown op {other:?}")),
        }
    }
}

/// Why the server did not return a result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireError {
    /// Not JSON, wrong `v`, unknown `op`, missing fields, or over the size cap.
    InvalidRequest { detail: String },
    /// The name is not in this agent's catalog.
    UnknownTool { detail: String },
    /// The tool exists but is not a command; the detail names the reason.
    NotAvailable { detail: String },
    /// The input failed the tool's schema.
    InvalidArgs { detail: String },
    /// The conversation is not ready to serve calls.
    NotReady { detail: String },
    /// Too many requests are open on this socket at once.
    Busy { detail: String },
    /// The tool ran and failed; `output` is the tool's own error JSON.
    ToolError { output: Value },
}

impl WireError {
    /// The wire spelling of the error's kind.
    pub fn kind(&self) -> &'static str {
        match self {
            WireError::InvalidRequest { .. } => "invalid_request",
            WireError::UnknownTool { .. } => "unknown_tool",
            WireError::NotAvailable { .. } => "not_available",
            WireError::InvalidArgs { .. } => "invalid_args",
            WireError::NotReady { .. } => "not_ready",
            WireError::Busy { .. } => "busy",
            WireError::ToolError { .. } => "tool_error",
        }
    }

    /// The refusal's text; `None` for `tool_error`, which carries the tool's
    /// output instead.
    pub fn detail(&self) -> Option<&str> {
        match self {
            WireError::InvalidRequest { detail }
            | WireError::UnknownTool { detail }
            | WireError::NotAvailable { detail }
            | WireError::InvalidArgs { detail }
            | WireError::NotReady { detail }
            | WireError::Busy { detail } => Some(detail),
            WireError::ToolError { .. } => None,
        }
    }
}

/// A response: the result, or why there is none.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Ok(Value),
    Err(WireError),
}

/// A response line as it is read.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawResponse {
    v: u32,
    ok: bool,
    #[serde(default, deserialize_with = "present")]
    result: Option<Value>,
    #[serde(default)]
    error: Option<WireError>,
}

/// A response line as it is written, borrowing the response's payload.
#[derive(Serialize)]
struct RawResponseOut<'a> {
    v: u32,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a WireError>,
}

impl Response {
    /// The response as one JSON line, without the trailing newline.
    pub fn to_line(&self) -> String {
        let raw = match self {
            Response::Ok(v) => RawResponseOut {
                v: PROTOCOL_VERSION,
                ok: true,
                result: Some(v),
                error: None,
            },
            Response::Err(e) => RawResponseOut {
                v: PROTOCOL_VERSION,
                ok: false,
                result: None,
                error: Some(e),
            },
        };
        serde_json::to_string(&raw).expect("a response always serializes")
    }

    /// Parse one response line. `Err` names what is wrong with it.
    pub fn parse(line: &[u8]) -> Result<Response, String> {
        let raw: RawResponse =
            serde_json::from_slice(line).map_err(|e| format!("malformed response: {e}"))?;
        if raw.v != PROTOCOL_VERSION {
            return Err(format!(
                "unsupported protocol version {} (this client speaks {PROTOCOL_VERSION})",
                raw.v
            ));
        }
        match (raw.ok, raw.result, raw.error) {
            (true, Some(result), None) => Ok(Response::Ok(result)),
            (false, None, Some(error)) => Ok(Response::Err(error)),
            (true, _, _) => Err("`ok: true` requires `result` and no `error`".into()),
            (false, _, _) => Err("`ok: false` requires `error` and no `result`".into()),
        }
    }
}

/// One catalog entry as `list` (without `input_schema`) and `describe` (with
/// it) return it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    pub name: String,
    pub description: String,
    pub available: bool,
    /// Why the tool is not a command; absent when it is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn requests_round_trip() {
        for req in [
            Request::List,
            Request::Describe {
                tool: "BrennSend".into(),
            },
            Request::Call {
                tool: "BrennSend".into(),
                input: json!({"to": "brenn:x", "body": "line\nbreak"}),
            },
            Request::Call {
                tool: "T".into(),
                input: Value::Null,
            },
        ] {
            let line = req.to_line();
            assert!(!line.contains('\n'), "{line}");
            assert_eq!(Request::parse(line.as_bytes()), Ok(req));
        }
    }

    #[test]
    fn request_wire_spelling() {
        assert_eq!(Request::List.to_line(), r#"{"v":1,"op":"list"}"#);
        assert_eq!(
            Request::Call {
                tool: "T".into(),
                input: json!({})
            }
            .to_line(),
            r#"{"v":1,"op":"call","tool":"T","input":{}}"#
        );
    }

    #[test]
    fn malformed_requests_are_refused() {
        for bad in [
            &b"not json"[..],
            br#"{"op":"list"}"#,
            br#"{"v":2,"op":"list"}"#,
            br#"{"v":1,"op":"frobnicate"}"#,
            br#"{"v":1,"op":"describe"}"#,
            br#"{"v":1,"op":"call","tool":"T"}"#,
            br#"{"v":1,"op":"call","input":{}}"#,
            br#"{"v":1,"op":"list","tool":"T"}"#,
            br#"{"v":1,"op":"list","input":null}"#,
            br#"{"v":1,"op":"list","extra":1}"#,
            br#"[1,2]"#,
        ] {
            assert!(
                Request::parse(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn responses_round_trip() {
        for resp in [
            Response::Ok(json!({"message_id": "m"})),
            Response::Ok(Value::Null),
            Response::Err(WireError::Busy {
                detail: "too many".into(),
            }),
            Response::Err(WireError::ToolError {
                output: json!({"ok": false, "error": "no"}),
            }),
        ] {
            assert_eq!(Response::parse(resp.to_line().as_bytes()), Ok(resp));
        }
    }

    #[test]
    fn response_wire_spelling() {
        assert_eq!(
            Response::Ok(json!([])).to_line(),
            r#"{"v":1,"ok":true,"result":[]}"#
        );
        assert_eq!(
            Response::Err(WireError::NotAvailable {
                detail: "requires_approval".into()
            })
            .to_line(),
            r#"{"v":1,"ok":false,"error":{"kind":"not_available","detail":"requires_approval"}}"#
        );
        assert_eq!(
            Response::Err(WireError::ToolError {
                output: json!({"error": "x"})
            })
            .to_line(),
            r#"{"v":1,"ok":false,"error":{"kind":"tool_error","output":{"error":"x"}}}"#
        );
    }

    #[test]
    fn every_error_kind_matches_its_wire_tag() {
        let d = || "d".to_string();
        for e in [
            WireError::InvalidRequest { detail: d() },
            WireError::UnknownTool { detail: d() },
            WireError::NotAvailable { detail: d() },
            WireError::InvalidArgs { detail: d() },
            WireError::NotReady { detail: d() },
            WireError::Busy { detail: d() },
            WireError::ToolError { output: json!({}) },
        ] {
            let v = serde_json::to_value(&e).unwrap();
            assert_eq!(v["kind"], e.kind());
            assert_eq!(v.get("detail").and_then(Value::as_str), e.detail());
        }
    }

    #[test]
    fn inconsistent_responses_are_refused() {
        for bad in [
            &br#"{"v":1,"ok":true}"#[..],
            br#"{"v":1,"ok":false}"#,
            br#"{"v":1,"ok":false,"result":null,"error":{"kind":"busy","detail":"d"}}"#,
            br#"{"v":1,"ok":true,"result":1,"error":{"kind":"busy","detail":"d"}}"#,
            br#"{"v":2,"ok":true,"result":1}"#,
            br#"{"v":1,"ok":false,"error":{"kind":"mystery","detail":"d"}}"#,
        ] {
            assert!(
                Response::parse(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn catalog_entry_omits_absent_fields() {
        let e = CatalogEntry {
            name: "BrennSend".into(),
            description: "d".into(),
            available: true,
            reason: None,
            input_schema: None,
        };
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            json!({"name": "BrennSend", "description": "d", "available": true})
        );
    }
}
