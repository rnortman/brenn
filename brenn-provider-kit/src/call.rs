//! A request's error answer, a nested call's arguments — an `api-request`
//! call or a call to another tool — and what a request does next.

use std::collections::BTreeMap;

use base64ct::{Base64, Encoding};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::activation::NESTED_TOOL;

/// The error kinds a request is answered with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Denied,
    InvalidArgs,
    Internal,
}

/// A request's error answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub kind: ErrorKind,
    pub detail: String,
}

/// An `internal` failure carrying `detail`.
pub fn internal(detail: impl Into<String>) -> Failure {
    Failure {
        kind: ErrorKind::Internal,
        detail: detail.into(),
    }
}

/// The method, host and path of a nested call, for the messages that name it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub method: String,
    pub host: String,
    pub path: String,
}

/// One query parameter: a single value, or several that `api-request` sends
/// as the key repeated once per value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryValue {
    One(String),
    Many(Vec<String>),
}

/// A nested call's query parameters, by key.
pub type Query = BTreeMap<String, QueryValue>;

/// A query from `(key, value)` pairs, each sent once.
pub fn query<const N: usize>(pairs: [(&str, String); N]) -> Query {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), QueryValue::One(value)))
        .collect()
}

/// A request body: text, sent as `body`; bytes, sent as `body_base64`
/// (standard padded base64, as `api-request` reads it); the request's own
/// attachment of that name, forwarded as the call's attachment and named in
/// `body_attachment` — no byte copied; or a body the component builds when
/// the call is issued (`Logic::body`), sent as the call's attachment
/// [`BUILT_BODY_ATTACHMENT`]. One by construction; `api-request` refuses a
/// call carrying two.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    Text(String),
    Bytes(Vec<u8>),
    Attachment(String),
    /// Built by the component in the activation that issues the call, and
    /// held by the call's message alone. A stored call records it as
    /// `body_built`, which `api-request` never receives: the loop sends the
    /// call with `body_attachment` naming [`BUILT_BODY_ATTACHMENT`].
    Built,
}

/// The attachment name [`ApiCall::answer_as_attachment`] asks the answer's
/// body under.
pub const ANSWER_ATTACHMENT: &str = "body";

/// The attachment name a built body ([`Body::Built`]) is sent under.
pub const BUILT_BODY_ATTACHMENT: &str = "body";

/// One `api-request` call's arguments. Every optional field is left off the
/// wire when unset, so a component sends only what it uses: `account` only
/// to an OAuth provider or a key provider with per-user accounts
/// (`api-request` refuses it for a key provider holding one deployment key),
/// and `headers` only where the API wants one. It parses back from what it writes:
/// an absent field is unset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "ApiCallWire", try_from = "ApiCallWire")]
pub struct ApiCall {
    pub provider: String,
    pub account: Option<String>,
    pub method: String,
    pub host: String,
    pub path: String,
    pub query: Query,
    pub headers: BTreeMap<String, String>,
    /// The request body; `api-request` sends a text body as
    /// `application/json` when no `Content-Type` is given, and an attachment
    /// body as the blob's content type.
    pub body: Option<Body>,
    /// The name a 2xx answer's body comes back under, as an attachment of
    /// the result rather than inside its value.
    pub response_attachment: Option<String>,
}

/// [`ApiCall`] as `api-request` reads it: the body is `body` when text and
/// `body_base64` when bytes. `body_built` is the stored form of
/// [`Body::Built`], written only when true.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiCallWire {
    provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account: Option<String>,
    method: String,
    host: String,
    path: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    query: Query,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_attachment: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    body_built: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response_attachment: Option<String>,
}

impl From<ApiCall> for ApiCallWire {
    fn from(call: ApiCall) -> Self {
        let body_built = call.body == Some(Body::Built);
        let (body, body_base64, body_attachment) = match call.body {
            None | Some(Body::Built) => (None, None, None),
            Some(Body::Text(text)) => (Some(text), None, None),
            Some(Body::Bytes(bytes)) => (None, Some(Base64::encode_string(&bytes)), None),
            Some(Body::Attachment(name)) => (None, None, Some(name)),
        };
        ApiCallWire {
            provider: call.provider,
            account: call.account,
            method: call.method,
            host: call.host,
            path: call.path,
            query: call.query,
            headers: call.headers,
            body,
            body_base64,
            body_attachment,
            body_built,
            response_attachment: call.response_attachment,
        }
    }
}

impl TryFrom<ApiCallWire> for ApiCall {
    type Error = String;

    fn try_from(wire: ApiCallWire) -> Result<Self, String> {
        let body = match (
            wire.body,
            wire.body_base64,
            wire.body_attachment,
            wire.body_built,
        ) {
            (None, None, None, false) => None,
            (Some(text), None, None, false) => Some(Body::Text(text)),
            (None, Some(encoded), None, false) => Some(Body::Bytes(
                Base64::decode_vec(&encoded).map_err(|_| "`body_base64` is not base64")?,
            )),
            (None, None, Some(name), false) => Some(Body::Attachment(name)),
            (None, None, None, true) => Some(Body::Built),
            _ => {
                return Err(
                    "`body`, `body_base64`, `body_attachment` and `body_built` are exclusive"
                        .to_string(),
                );
            }
        };
        Ok(ApiCall {
            provider: wire.provider,
            account: wire.account,
            method: wire.method,
            host: wire.host,
            path: wire.path,
            query: wire.query,
            headers: wire.headers,
            body,
            response_attachment: wire.response_attachment,
        })
    }
}

impl ApiCall {
    /// This call with its 2xx answer's body asked for as the result's
    /// attachment [`ANSWER_ATTACHMENT`].
    pub fn answer_as_attachment(self) -> ApiCall {
        ApiCall {
            response_attachment: Some(ANSWER_ATTACHMENT.to_string()),
            ..self
        }
    }

    pub fn target(&self) -> Target {
        Target {
            method: self.method.clone(),
            host: self.host.clone(),
            path: self.path.clone(),
        }
    }
}

/// A call to a tool other than `api-request` that the instance is granted:
/// the tool's registry name and its arguments, sent as they are. It carries
/// no attachment and asks for none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    pub tool: String,
    pub args: Value,
}

/// What a nested call asks of which tool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Nested {
    /// An `api-request` call.
    Api(ApiCall),
    /// A call to another tool the instance is granted.
    Tool(ToolCall),
}

impl Nested {
    /// The registry name of the tool the call reaches.
    pub fn tool(&self) -> &str {
        match self {
            Nested::Api(_) => NESTED_TOOL,
            Nested::Tool(call) => &call.tool,
        }
    }

    /// The `api-request` call's arguments; `None` for a call to another tool.
    pub fn api(&self) -> Option<&ApiCall> {
        match self {
            Nested::Api(api) => Some(api),
            Nested::Tool(_) => None,
        }
    }

    /// What the messages about this call name it by.
    pub fn target(&self) -> Outstanding {
        match self {
            Nested::Api(api) => Outstanding::Api(api.target()),
            Nested::Tool(call) => Outstanding::Tool(call.tool.clone()),
        }
    }
}

/// The nested call a request has outstanding, for the messages that name it:
/// an `api-request` call's method, host and path, or another tool's name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Outstanding {
    Api(Target),
    Tool(String),
}

impl Outstanding {
    /// The `api-request` call's target, which the `read_api_*` readers take;
    /// `internal` when the outstanding call reached another tool, whose
    /// answer is not `api-request`'s envelope.
    pub fn api(&self) -> Result<&Target, Failure> {
        match self {
            Outstanding::Api(target) => Ok(target),
            Outstanding::Tool(tool) => Err(internal(format!(
                "the outstanding call reached `{tool}`, whose answer is not api-request's"
            ))),
        }
    }
}

/// A nested call to issue: the request's step it is, and what it asks of
/// which tool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub step: u8,
    pub request: Nested,
}

impl Call {
    /// Step `step` as the `api-request` call `api`.
    pub fn api(step: u8, api: ApiCall) -> Call {
        Call {
            step,
            request: Nested::Api(api),
        }
    }

    /// Step `step` as a call to `tool` with `args`.
    pub fn tool(step: u8, tool: impl Into<String>, args: Value) -> Call {
        Call {
            step,
            request: Nested::Tool(ToolCall {
                tool: tool.into(),
                args,
            }),
        }
    }
}

/// What a request does next after its nested call answered.
// One `Next` exists per step, returned by value and consumed at once; boxing
// `Call` would cost every component's `Next::Call` literal for no saving.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum Next {
    Complete(Value),
    Fail(Failure),
    /// Issue the request's next step, carrying what this one learned.
    Call {
        call: Call,
        derived: Map<String, Value>,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn bare() -> ApiCall {
        ApiCall {
            provider: "maps".to_string(),
            account: None,
            method: "GET".to_string(),
            host: "maps.example.com".to_string(),
            path: "/v1/places".to_string(),
            query: Query::new(),
            headers: BTreeMap::new(),
            body: None,
            response_attachment: None,
        }
    }

    #[test]
    fn an_attachment_body_and_an_attachment_answer_are_api_requests_arguments() {
        let mut call = bare().answer_as_attachment();
        call.method = "PUT".to_string();
        call.body = Some(Body::Attachment("file".to_string()));
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(
            wire,
            json!({
                "provider": "maps",
                "method": "PUT",
                "host": "maps.example.com",
                "path": "/v1/places",
                "body_attachment": "file",
                "response_attachment": ANSWER_ATTACHMENT,
            })
        );
        assert_eq!(serde_json::from_value::<ApiCall>(wire).unwrap(), call);
        for other in [json!({ "body": "{}" }), json!({ "body_base64": "AAEC" })] {
            let mut both = serde_json::to_value(&call).unwrap();
            both.as_object_mut()
                .unwrap()
                .extend(other.as_object().unwrap().clone());
            let refused = serde_json::from_value::<ApiCall>(both).unwrap_err();
            assert!(refused.to_string().contains("exclusive"), "{refused}");
        }
    }

    #[test]
    fn an_unset_optional_field_is_absent_from_the_wire() {
        assert_eq!(
            serde_json::to_value(bare()).unwrap(),
            json!({
                "provider": "maps",
                "method": "GET",
                "host": "maps.example.com",
                "path": "/v1/places",
            })
        );
    }

    #[test]
    fn every_set_field_is_on_the_wire() {
        let mut call = bare();
        call.account = Some("alice@example.com".to_string());
        call.query = query([("q", "cafe".to_string())]);
        call.query.insert(
            "fields".to_string(),
            QueryValue::Many(vec!["id".to_string(), "name".to_string()]),
        );
        call.headers
            .insert("X-Goog-FieldMask".to_string(), "places.id".to_string());
        call.method = "POST".to_string();
        call.body = Some(Body::Text("{}".to_string()));
        assert_eq!(
            serde_json::to_value(call).unwrap(),
            json!({
                "provider": "maps",
                "account": "alice@example.com",
                "method": "POST",
                "host": "maps.example.com",
                "path": "/v1/places",
                "query": { "fields": ["id", "name"], "q": "cafe" },
                "headers": { "X-Goog-FieldMask": "places.id" },
                "body": "{}",
            })
        );
    }

    #[test]
    fn a_call_parses_back_from_its_wire_form() {
        let mut full = bare();
        full.account = Some("alice@example.com".to_string());
        full.query = query([("q", "cafe".to_string())]);
        full.query.insert(
            "fields".to_string(),
            QueryValue::Many(vec!["id".to_string(), "name".to_string()]),
        );
        full.headers
            .insert("X-Goog-FieldMask".to_string(), "places.id".to_string());
        full.body = Some(Body::Text("{}".to_string()));
        let mut bytes = bare();
        bytes.body = Some(Body::Bytes(vec![0, 1, 2, 0xff]));
        for api in [bare(), full, bytes] {
            let call = Call::api(2, api);
            let wire = serde_json::to_vec(&call).unwrap();
            assert_eq!(serde_json::from_slice::<Call>(&wire).unwrap(), call);
        }
    }

    #[test]
    fn a_byte_body_is_sent_as_padded_standard_base64() {
        let mut call = bare();
        call.method = "PUT".to_string();
        call.body = Some(Body::Bytes(vec![0xfb, 0xff]));
        assert_eq!(
            serde_json::to_value(call).unwrap(),
            json!({
                "provider": "maps",
                "method": "PUT",
                "host": "maps.example.com",
                "path": "/v1/places",
                "body_base64": "+/8=",
            })
        );
    }

    #[test]
    fn a_wire_form_with_both_bodies_or_a_bad_base64_does_not_parse() {
        let mut both = serde_json::to_value(bare()).unwrap();
        both["body"] = json!("{}");
        both["body_base64"] = json!("AAEC");
        let refused = serde_json::from_value::<ApiCall>(both).unwrap_err();
        assert!(refused.to_string().contains("exclusive"), "{refused}");
        let mut bad = serde_json::to_value(bare()).unwrap();
        bad["body_base64"] = json!("not base64!");
        let refused = serde_json::from_value::<ApiCall>(bad).unwrap_err();
        assert!(refused.to_string().contains("not base64"), "{refused}");
    }

    #[test]
    fn a_one_element_list_parses_back_as_a_list() {
        let mut api = bare();
        api.query.insert(
            "fields".to_string(),
            QueryValue::Many(vec!["id".to_string()]),
        );
        let wire = serde_json::to_vec(&api).unwrap();
        assert_eq!(serde_json::from_slice::<ApiCall>(&wire).unwrap(), api);
    }

    #[test]
    fn a_call_with_a_field_this_build_does_not_write_does_not_parse() {
        let wire = json!({
            "step": 1,
            "request": { "api": serde_json::to_value(bare()).unwrap() },
            "port": 1,
        });
        assert!(serde_json::from_value::<Call>(wire).is_err());
        let mut api = serde_json::to_value(bare()).unwrap();
        api["port"] = json!(443);
        assert!(serde_json::from_value::<ApiCall>(api).is_err());
    }

    #[test]
    fn a_built_body_is_stored_as_body_built_and_exclusive_with_every_other_body() {
        let mut built = bare();
        built.method = "PUT".to_string();
        built.body = Some(Body::Built);
        let wire = serde_json::to_value(&built).unwrap();
        assert_eq!(
            wire,
            json!({
                "provider": "maps",
                "method": "PUT",
                "host": "maps.example.com",
                "path": "/v1/places",
                "body_built": true,
            })
        );
        let call = Call::api(3, built);
        let stored = serde_json::to_vec(&call).unwrap();
        assert_eq!(serde_json::from_slice::<Call>(&stored).unwrap(), call);
        for other in [
            json!({ "body": "{}" }),
            json!({ "body_base64": "AAEC" }),
            json!({ "body_attachment": "file" }),
        ] {
            let mut both = wire.clone();
            both.as_object_mut()
                .unwrap()
                .extend(other.as_object().unwrap().clone());
            let refused = serde_json::from_value::<ApiCall>(both).unwrap_err();
            assert!(refused.to_string().contains("exclusive"), "{refused}");
        }
        let mut unbuilt = serde_json::to_value(bare()).unwrap();
        unbuilt["body_built"] = json!(false);
        assert_eq!(serde_json::from_value::<ApiCall>(unbuilt).unwrap(), bare());
    }

    #[test]
    fn a_call_targets_its_method_host_and_path() {
        assert_eq!(
            bare().target(),
            Target {
                method: "GET".to_string(),
                host: "maps.example.com".to_string(),
                path: "/v1/places".to_string(),
            }
        );
    }

    #[test]
    fn a_stored_call_names_which_tool_it_reaches() {
        assert_eq!(
            serde_json::to_value(Call::api(1, bare())).unwrap(),
            json!({
                "step": 1,
                "request": { "api": {
                    "provider": "maps",
                    "method": "GET",
                    "host": "maps.example.com",
                    "path": "/v1/places",
                } },
            })
        );
        let tool = Call::tool(
            2,
            "one-click-unsubscribe",
            json!({ "url": "https://example.com/u" }),
        );
        let wire = serde_json::to_value(&tool).unwrap();
        assert_eq!(
            wire,
            json!({
                "step": 2,
                "request": { "tool": {
                    "tool": "one-click-unsubscribe",
                    "args": { "url": "https://example.com/u" },
                } },
            })
        );
        assert_eq!(serde_json::from_value::<Call>(wire).unwrap(), tool);
        let mut extra = serde_json::to_value(&tool).unwrap();
        extra["request"]["tool"]["attachment"] = json!("file");
        assert!(serde_json::from_value::<Call>(extra).is_err());
        let both = json!({
            "step": 1,
            "request": { "api": serde_json::to_value(bare()).unwrap(), "tool": {
                "tool": "x", "args": {},
            } },
        });
        assert!(serde_json::from_value::<Call>(both).is_err());
    }

    #[test]
    fn a_nested_call_names_its_tool_and_its_target() {
        let api = Nested::Api(bare());
        assert_eq!(api.tool(), "api-request");
        assert_eq!(api.api(), Some(&bare()));
        assert_eq!(api.target(), Outstanding::Api(bare().target()));
        assert_eq!(api.target().api(), Ok(&bare().target()));
        let tool = Call::tool(1, "one-click-unsubscribe", json!({})).request;
        assert_eq!(tool.tool(), "one-click-unsubscribe");
        assert_eq!(tool.api(), None);
        assert_eq!(
            tool.target(),
            Outstanding::Tool("one-click-unsubscribe".to_string())
        );
        assert_eq!(
            tool.target().api(),
            Err(internal(
                "the outstanding call reached `one-click-unsubscribe`, whose answer is not \
                 api-request's"
            ))
        );
    }
}
