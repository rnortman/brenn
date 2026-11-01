//! A request's error answer, the registry tool a nested call reaches and its
//! arguments, and what a request does next.

use std::collections::BTreeMap;
use std::fmt;

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

/// What a step addressed, for the in-flight row and for the messages that
/// name it: an `api-request` call's method, host and path, or a
/// `device-location` call, which has none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tool", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Target {
    ApiRequest {
        method: String,
        host: String,
        path: String,
    },
    DeviceLocation,
}

impl fmt::Display for Target {
    /// `GET host/path` for an API step, `device-location` for a location step.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::ApiRequest { method, host, path } => write!(f, "{method} {host}{path}"),
            Target::DeviceLocation => f.write_str("device-location"),
        }
    }
}

/// One query parameter: a single value, or several that `api-request` sends
/// as the key repeated once per value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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

/// One `api-request` call's arguments. Every optional field is left off the
/// wire when unset, so a component sends only what it uses: `account` only
/// to an OAuth provider (`api-request` refuses it for a key provider), and
/// `headers` only where the API wants one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApiCall {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub method: String,
    pub host: String,
    pub path: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub query: Query,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// The request body as text; `api-request` sends it as
    /// `application/json` when no `Content-Type` is given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl ApiCall {
    pub fn target(&self) -> Target {
        Target::ApiRequest {
            method: self.method.clone(),
            host: self.host.clone(),
            path: self.path.clone(),
        }
    }
}

/// The registry tool a step calls, with its arguments. The kit knows these
/// two; a provider that needs another widens the enum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Nested {
    ApiRequest(ApiCall),
    /// `device-location` with no arguments: every sharing device of the user
    /// the request serves.
    // TODO(security-nested-location-answer-retained-in-durable-result-window):
    // the whole answer lands in the caller's durable result inbox.
    DeviceLocation,
}

/// A nested call's arguments as they go on the wire: the `ApiCall` in its
/// own field order, or `{}`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum NestedArgs<'a> {
    Api(&'a ApiCall),
    None {},
}

impl Nested {
    /// The tool's registry name.
    pub fn tool(&self) -> &'static str {
        match self {
            Nested::ApiRequest(_) => "api-request",
            Nested::DeviceLocation => "device-location",
        }
    }

    /// The arguments as `call_async_for` sends them.
    pub fn args(&self) -> NestedArgs<'_> {
        match self {
            Nested::ApiRequest(api) => NestedArgs::Api(api),
            Nested::DeviceLocation => NestedArgs::None {},
        }
    }

    /// What the in-flight row records and the messages name.
    pub fn target(&self) -> Target {
        match self {
            Nested::ApiRequest(api) => api.target(),
            Nested::DeviceLocation => Target::DeviceLocation,
        }
    }
}

/// A nested call to issue: the request's step it is, and the tool it calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    pub step: u8,
    pub nested: Nested,
}

impl Call {
    /// Step `step` as an `api-request` call.
    pub fn api(step: u8, api: ApiCall) -> Call {
        Call {
            step,
            nested: Nested::ApiRequest(api),
        }
    }

    /// Step `step` as a `device-location` call.
    pub fn device_location(step: u8) -> Call {
        Call {
            step,
            nested: Nested::DeviceLocation,
        }
    }
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
        call.body = Some("{}".to_string());
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
    fn a_call_targets_its_method_host_and_path() {
        assert_eq!(
            bare().target(),
            Target::ApiRequest {
                method: "GET".to_string(),
                host: "maps.example.com".to_string(),
                path: "/v1/places".to_string(),
            }
        );
    }

    #[test]
    fn a_nested_calls_tool_args_and_target() {
        let api = Call::api(2, bare());
        assert_eq!(api.step, 2);
        assert_eq!(api.nested.tool(), "api-request");
        assert_eq!(
            serde_json::to_string(&api.nested.args()).unwrap(),
            serde_json::to_string(&bare()).unwrap()
        );
        assert_eq!(api.nested.target(), bare().target());

        let here = Call::device_location(3);
        assert_eq!(here.step, 3);
        assert_eq!(here.nested.tool(), "device-location");
        assert_eq!(serde_json::to_string(&here.nested.args()).unwrap(), "{}");
        assert_eq!(here.nested.target(), Target::DeviceLocation);
    }

    #[test]
    fn a_target_round_trips_tagged_with_its_tool() {
        for (target, wire) in [
            (
                bare().target(),
                json!({
                    "tool": "api-request",
                    "method": "GET",
                    "host": "maps.example.com",
                    "path": "/v1/places",
                }),
            ),
            (Target::DeviceLocation, json!({ "tool": "device-location" })),
        ] {
            assert_eq!(serde_json::to_value(&target).unwrap(), wire);
            assert_eq!(serde_json::from_value::<Target>(wire).unwrap(), target);
        }
    }

    #[test]
    fn a_target_displays_what_the_step_addressed() {
        assert_eq!(
            bare().target().to_string(),
            "GET maps.example.com/v1/places"
        );
        assert_eq!(Target::DeviceLocation.to_string(), "device-location");
    }
}
