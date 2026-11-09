//! A request's error answer, a nested `api-request` call's arguments, and what
//! a request does next.

use std::collections::BTreeMap;

use base64ct::{Base64, Encoding};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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

/// A request body: text, sent as `body`, or bytes, sent as `body_base64`
/// (standard padded base64, as `api-request` reads it). One or the other by
/// construction; `api-request` refuses a call carrying both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    Text(String),
    Bytes(Vec<u8>),
}

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
    /// `application/json` when no `Content-Type` is given.
    pub body: Option<Body>,
}

/// [`ApiCall`] as `api-request` reads it: the body is `body` when text and
/// `body_base64` when bytes.
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
}

impl From<ApiCall> for ApiCallWire {
    fn from(call: ApiCall) -> Self {
        let (body, body_base64) = match call.body {
            None => (None, None),
            Some(Body::Text(text)) => (Some(text), None),
            Some(Body::Bytes(bytes)) => (None, Some(Base64::encode_string(&bytes))),
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
        }
    }
}

impl TryFrom<ApiCallWire> for ApiCall {
    type Error = String;

    fn try_from(wire: ApiCallWire) -> Result<Self, String> {
        let body = match (wire.body, wire.body_base64) {
            (None, None) => None,
            (Some(text), None) => Some(Body::Text(text)),
            (None, Some(encoded)) => Some(Body::Bytes(
                Base64::decode_vec(&encoded).map_err(|_| "`body_base64` is not base64")?,
            )),
            (Some(_), Some(_)) => return Err("`body` and `body_base64` are exclusive".to_string()),
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
        })
    }
}

impl ApiCall {
    pub fn target(&self) -> Target {
        Target {
            method: self.method.clone(),
            host: self.host.clone(),
            path: self.path.clone(),
        }
    }
}

/// A nested call to issue: the request's step it is, and its arguments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub step: u8,
    pub api: ApiCall,
}

/// What a request does next after its nested call answered.
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
            let call = Call { step: 2, api };
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
        let wire = json!({ "step": 1, "api": serde_json::to_value(bare()).unwrap(), "port": 1 });
        assert!(serde_json::from_value::<Call>(wire).is_err());
        let mut api = serde_json::to_value(bare()).unwrap();
        api["port"] = json!(443);
        assert!(serde_json::from_value::<ApiCall>(api).is_err());
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
}
