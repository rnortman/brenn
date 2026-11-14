//! Wire contract of the per-conversation tool socket.
//!
//! One connection carries one request: a single line of JSON from the client,
//! a single line of JSON back, then the server closes. JSON escapes newlines,
//! so the newline is the only framing.
//!
//! ```jsonc
//! {"v":2,"op":"list"}
//! {"v":2,"op":"describe","tool":"BrennSend"}
//! {"v":2,"op":"call","tool":"BrennSend","input":{…}}
//!
//! {"v":2,"ok":true,"result":<value>}
//! {"v":2,"ok":false,"error":{"kind":"<kind>","detail":"<text>"}}
//! {"v":2,"ok":false,"error":{"kind":"tool_error","output":<the tool's error JSON>}}
//! ```
//!
//! `save` carries bytes after its response line. The request names one part
//! of a message; a success line carries a [`SaveHeader`] as its result, and
//! exactly `size` raw bytes follow it before the server closes. An error line
//! is followed by nothing.
//!
//! ```jsonc
//! {"v":2,"op":"save","message_id":"<uuid>","attachment":"<name>"}
//! {"v":2,"op":"save","message_id":"<uuid>","body":true}
//!
//! {"v":2,"ok":true,"result":{"size":482113,"content_type":"application/pdf"}}
//! ```
//!
//! `send_files` carries bytes after its request line. The request is a
//! `BrennSend` input and the files to attach, each with its declared size.
//! The server answers with the go-ahead line, or an error line and a close;
//! after the go-ahead the client writes each file's bytes in the order
//! declared, exactly `size` each, and the server's next line is the
//! `BrennSend` answer or an error.
//!
//! ```jsonc
//! {"v":2,"op":"send_files","input":{…},"files":[{"name":"scan.pdf","content_type":"application/pdf","size":482113}]}
//!
//! {"v":2,"go":true}
//! ```

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The protocol version every request and response carries as `v`.
pub const PROTOCOL_VERSION: u32 = 2;

/// The largest request line the server reads, newline excluded. A transport
/// bound on what one connection may make the server buffer.
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The most bytes either side moves in one piece of a transfer.
pub const TRANSFER_PIECE_BYTES: usize = 256 * 1024;

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
    /// Stream the bytes of one part of a message the conversation was shown.
    /// `message_id` is passed as given; the server judges it.
    Save { message_id: String, part: SavePart },
    /// Publish a `BrennSend` whose message carries `files` as attachments;
    /// their bytes follow the go-ahead line.
    SendFiles { input: Value, files: Vec<FileDecl> },
}

/// One file a `send_files` attaches, as the client declares it. The server
/// judges the name and the type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDecl {
    /// The attachment name on the new message.
    pub name: String,
    pub content_type: String,
    /// The exact count of bytes the client sends for this file.
    pub size: u64,
}

/// The line the server writes when a `send_files` may send its bytes.
pub fn go_ahead_line() -> String {
    serde_json::to_string(&GoAhead {
        v: PROTOCOL_VERSION,
        go: true,
    })
    .expect("a go-ahead always serializes")
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoAhead {
    v: u32,
    go: bool,
}

/// What the server answered a `send_files` before any byte crossed.
#[derive(Debug, Clone, PartialEq)]
pub enum SendFilesReply {
    /// Send the bytes.
    GoAhead,
    /// Refused; nothing follows.
    Refused(WireError),
}

impl SendFilesReply {
    /// Parse the server's first line to a `send_files`: the go-ahead, or an
    /// error response. A success response is not a valid first line.
    pub fn parse(line: &[u8]) -> Result<SendFilesReply, String> {
        if let Ok(go) = serde_json::from_slice::<GoAhead>(line) {
            if go.v != PROTOCOL_VERSION {
                return Err(format!(
                    "unsupported protocol version {} (this client speaks {PROTOCOL_VERSION})",
                    go.v
                ));
            }
            if !go.go {
                return Err("a go-ahead line carries `go: true`".into());
            }
            return Ok(SendFilesReply::GoAhead);
        }
        match Response::parse(line)? {
            Response::Err(e) => Ok(SendFilesReply::Refused(e)),
            Response::Ok(_) => Err("a result before the go-ahead".into()),
        }
    }
}

/// The part of a message a `save` names.
#[derive(Debug, Clone, PartialEq)]
pub enum SavePart {
    /// The attachment of that name, passed as given.
    Attachment(String),
    /// The message's body.
    Body,
}

/// The result of a successful `save`: how many raw bytes follow the line, and
/// their content type; `None` for an untyped text body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveHeader {
    pub size: u64,
    /// Present on the wire even when `null`.
    #[serde(deserialize_with = "Option::deserialize")]
    pub content_type: Option<String>,
}

impl SaveHeader {
    /// The header a success line's result carries.
    pub fn from_result(result: Value) -> Result<SaveHeader, String> {
        serde_json::from_value(result).map_err(|e| format!("malformed save header: {e}"))
    }

    /// The header as a success line's result.
    pub fn to_result(&self) -> Value {
        serde_json::to_value(self).expect("a save header always serializes")
    }
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
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    attachment: Option<String>,
    #[serde(default)]
    body: Option<bool>,
    #[serde(default)]
    files: Option<Vec<FileDecl>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachment: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    files: Option<&'a [FileDecl]>,
}

impl Request {
    /// The request as one JSON line, without the trailing newline.
    pub fn to_line(&self) -> String {
        let mut raw = RawRequestOut {
            v: PROTOCOL_VERSION,
            op: "",
            tool: None,
            input: None,
            message_id: None,
            attachment: None,
            body: None,
            files: None,
        };
        match self {
            Request::List => raw.op = "list",
            Request::Describe { tool } => {
                raw.op = "describe";
                raw.tool = Some(tool);
            }
            Request::Call { tool, input } => {
                raw.op = "call";
                raw.tool = Some(tool);
                raw.input = Some(input);
            }
            Request::Save { message_id, part } => {
                raw.op = "save";
                raw.message_id = Some(message_id);
                match part {
                    SavePart::Attachment(name) => raw.attachment = Some(name),
                    SavePart::Body => raw.body = Some(true),
                }
            }
            Request::SendFiles { input, files } => {
                raw.op = "send_files";
                raw.input = Some(input);
                raw.files = Some(files);
            }
        }
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
        let forbid_save_fields = || {
            forbid("message_id", raw.message_id.is_some())?;
            forbid("attachment", raw.attachment.is_some())?;
            forbid("body", raw.body.is_some())
        };
        let forbid_files = || forbid("files", raw.files.is_some());
        match raw.op.as_str() {
            "list" => {
                forbid("tool", raw.tool.is_some())?;
                forbid("input", raw.input.is_some())?;
                forbid_save_fields()?;
                forbid_files()?;
                Ok(Request::List)
            }
            "describe" => {
                field("tool", raw.tool.is_some())?;
                forbid("input", raw.input.is_some())?;
                forbid_save_fields()?;
                forbid_files()?;
                Ok(Request::Describe {
                    tool: raw.tool.expect("checked present"),
                })
            }
            "save" => {
                forbid("tool", raw.tool.is_some())?;
                forbid("input", raw.input.is_some())?;
                forbid_files()?;
                field("message_id", raw.message_id.is_some())?;
                let part = match (raw.attachment, raw.body) {
                    (Some(name), None) => SavePart::Attachment(name),
                    (None, Some(true)) => SavePart::Body,
                    (None, Some(false)) => return Err("`save` takes `body` only as `true`".into()),
                    _ => {
                        return Err("`save` requires exactly one of `attachment` and `body`".into());
                    }
                };
                Ok(Request::Save {
                    message_id: raw.message_id.expect("checked present"),
                    part,
                })
            }
            "call" => {
                field("tool", raw.tool.is_some())?;
                field("input", raw.input.is_some())?;
                forbid_save_fields()?;
                forbid_files()?;
                Ok(Request::Call {
                    tool: raw.tool.expect("checked present"),
                    input: raw.input.expect("checked present"),
                })
            }
            "send_files" => {
                forbid("tool", raw.tool.is_some())?;
                field("input", raw.input.is_some())?;
                field("files", raw.files.is_some())?;
                forbid_save_fields()?;
                Ok(Request::SendFiles {
                    input: raw.input.expect("checked present"),
                    files: raw.files.expect("checked present"),
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
    /// A transfer's byte phase did not complete: the deadline passed, or the
    /// store could not take the bytes.
    TransferAborted { detail: String },
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
            WireError::TransferAborted { .. } => "transfer_aborted",
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
            | WireError::Busy { detail }
            | WireError::TransferAborted { detail } => Some(detail),
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
            Request::Save {
                message_id: "m".into(),
                part: SavePart::Attachment("scan.pdf".into()),
            },
            Request::Save {
                message_id: "m".into(),
                part: SavePart::Body,
            },
            Request::SendFiles {
                input: json!({"to": "brenn:x", "body": "b"}),
                files: vec![
                    FileDecl {
                        name: "scan.pdf".into(),
                        content_type: "application/pdf".into(),
                        size: 482113,
                    },
                    FileDecl {
                        name: "empty".into(),
                        content_type: "text/plain".into(),
                        size: 0,
                    },
                ],
            },
            Request::SendFiles {
                input: json!({}),
                files: vec![],
            },
        ] {
            let line = req.to_line();
            assert!(!line.contains('\n'), "{line}");
            assert_eq!(Request::parse(line.as_bytes()), Ok(req));
        }
    }

    #[test]
    fn request_wire_spelling() {
        assert_eq!(Request::List.to_line(), r#"{"v":2,"op":"list"}"#);
        assert_eq!(
            Request::Call {
                tool: "T".into(),
                input: json!({})
            }
            .to_line(),
            r#"{"v":2,"op":"call","tool":"T","input":{}}"#
        );
        assert_eq!(
            Request::Save {
                message_id: "m".into(),
                part: SavePart::Attachment("a.pdf".into()),
            }
            .to_line(),
            r#"{"v":2,"op":"save","message_id":"m","attachment":"a.pdf"}"#
        );
        assert_eq!(
            Request::Save {
                message_id: "m".into(),
                part: SavePart::Body,
            }
            .to_line(),
            r#"{"v":2,"op":"save","message_id":"m","body":true}"#
        );
        assert_eq!(
            Request::SendFiles {
                input: json!({"to": "brenn:x"}),
                files: vec![FileDecl {
                    name: "a".into(),
                    content_type: "text/plain".into(),
                    size: 3,
                }],
            }
            .to_line(),
            r#"{"v":2,"op":"send_files","input":{"to":"brenn:x"},"files":[{"name":"a","content_type":"text/plain","size":3}]}"#
        );
    }

    #[test]
    fn the_go_ahead_line() {
        assert_eq!(go_ahead_line(), r#"{"v":2,"go":true}"#);
        assert_eq!(
            SendFilesReply::parse(go_ahead_line().as_bytes()),
            Ok(SendFilesReply::GoAhead)
        );
        let refused = Response::Err(WireError::UnknownTool { detail: "d".into() }).to_line();
        assert_eq!(
            SendFilesReply::parse(refused.as_bytes()),
            Ok(SendFilesReply::Refused(WireError::UnknownTool {
                detail: "d".into()
            }))
        );
        for bad in [
            &br#"{"v":2,"go":false}"#[..],
            br#"{"v":1,"go":true}"#,
            br#"{"v":2,"go":true,"extra":1}"#,
            br#"{"v":2,"ok":true,"result":{}}"#,
            br#"not json"#,
        ] {
            assert!(
                SendFilesReply::parse(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn save_headers_round_trip() {
        for header in [
            SaveHeader {
                size: 482113,
                content_type: Some("application/pdf".into()),
            },
            SaveHeader {
                size: 0,
                content_type: None,
            },
        ] {
            let line = Response::Ok(header.to_result()).to_line();
            let Ok(Response::Ok(result)) = Response::parse(line.as_bytes()) else {
                panic!("{line}");
            };
            assert_eq!(SaveHeader::from_result(result), Ok(header));
        }
        assert_eq!(
            Response::Ok(
                SaveHeader {
                    size: 3,
                    content_type: None
                }
                .to_result()
            )
            .to_line(),
            r#"{"v":2,"ok":true,"result":{"content_type":null,"size":3}}"#
        );
        for bad in [
            json!({"size": 3}),
            json!({"size": -1, "content_type": null}),
            json!({"size": 3, "content_type": null, "extra": 1}),
            json!([]),
        ] {
            assert!(SaveHeader::from_result(bad.clone()).is_err(), "{bad}");
        }
    }

    #[test]
    fn malformed_requests_are_refused() {
        for bad in [
            &b"not json"[..],
            br#"{"op":"list"}"#,
            br#"{"v":1,"op":"list"}"#,
            br#"{"v":3,"op":"list"}"#,
            br#"{"v":2,"op":"frobnicate"}"#,
            br#"{"v":2,"op":"describe"}"#,
            br#"{"v":2,"op":"call","tool":"T"}"#,
            br#"{"v":2,"op":"call","input":{}}"#,
            br#"{"v":2,"op":"list","tool":"T"}"#,
            br#"{"v":2,"op":"list","input":null}"#,
            br#"{"v":2,"op":"list","extra":1}"#,
            br#"[1,2]"#,
            br#"{"v":2,"op":"save","attachment":"a"}"#,
            br#"{"v":2,"op":"save","message_id":"m"}"#,
            br#"{"v":2,"op":"save","message_id":"m","attachment":"a","body":true}"#,
            br#"{"v":2,"op":"save","message_id":"m","body":false}"#,
            br#"{"v":2,"op":"save","message_id":"m","body":"yes"}"#,
            br#"{"v":2,"op":"save","message_id":"m","body":true,"tool":"T"}"#,
            br#"{"v":2,"op":"save","message_id":"m","body":true,"input":{}}"#,
            br#"{"v":2,"op":"list","message_id":"m"}"#,
            br#"{"v":2,"op":"describe","tool":"T","body":true}"#,
            br#"{"v":2,"op":"call","tool":"T","input":{},"attachment":"a"}"#,
            br#"{"v":2,"op":"send_files","files":[]}"#,
            br#"{"v":2,"op":"send_files","input":{}}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":[],"tool":"T"}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":[],"message_id":"m"}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":[{"name":"a","size":1}]}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":[{"name":"a","content_type":"t/p","size":-1}]}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":[{"name":"a","content_type":"t/p","size":1,"x":1}]}"#,
            br#"{"v":2,"op":"send_files","input":{},"files":{}}"#,
            br#"{"v":2,"op":"call","tool":"T","input":{},"files":[]}"#,
            br#"{"v":2,"op":"list","files":[]}"#,
            br#"{"v":2,"op":"save","message_id":"m","body":true,"files":[]}"#,
        ] {
            assert!(
                Request::parse(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    /// A v1 client is refused by name, with the version this side speaks.
    #[test]
    fn a_v1_request_is_refused_naming_both_versions() {
        let err = Request::parse(br#"{"v":1,"op":"list"}"#).unwrap_err();
        assert!(err.contains("unsupported protocol version 1"), "{err}");
        assert!(err.contains("speaks 2"), "{err}");
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
            r#"{"v":2,"ok":true,"result":[]}"#
        );
        assert_eq!(
            Response::Err(WireError::NotAvailable {
                detail: "requires_approval".into()
            })
            .to_line(),
            r#"{"v":2,"ok":false,"error":{"kind":"not_available","detail":"requires_approval"}}"#
        );
        assert_eq!(
            Response::Err(WireError::ToolError {
                output: json!({"error": "x"})
            })
            .to_line(),
            r#"{"v":2,"ok":false,"error":{"kind":"tool_error","output":{"error":"x"}}}"#
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
            WireError::TransferAborted { detail: d() },
        ] {
            let v = serde_json::to_value(&e).unwrap();
            assert_eq!(v["kind"], e.kind());
            assert_eq!(v.get("detail").and_then(Value::as_str), e.detail());
        }
    }

    #[test]
    fn inconsistent_responses_are_refused() {
        for bad in [
            &br#"{"v":2,"ok":true}"#[..],
            br#"{"v":2,"ok":false}"#,
            br#"{"v":2,"ok":false,"result":null,"error":{"kind":"busy","detail":"d"}}"#,
            br#"{"v":2,"ok":true,"result":1,"error":{"kind":"busy","detail":"d"}}"#,
            br#"{"v":1,"ok":true,"result":1}"#,
            br#"{"v":2,"ok":false,"error":{"kind":"mystery","detail":"d"}}"#,
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
