//! A request's error answer, a nested `api-request` call's arguments, and what
//! a request does next.

use std::collections::BTreeMap;

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

/// One `api-request` call's arguments. Every optional field is left off the
/// wire when unset, so a component sends only what it uses: `account` only
/// to an OAuth provider (`api-request` refuses it for a key provider), and
/// `headers` only where the API wants one. It parses back from what it writes:
/// an absent field is unset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiCall {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub method: String,
    pub host: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: Query,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// The request body as text; `api-request` sends it as
    /// `application/json` when no `Content-Type` is given.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
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
        full.body = Some("{}".to_string());
        for api in [bare(), full] {
            let call = Call { step: 2, api };
            let wire = serde_json::to_vec(&call).unwrap();
            assert_eq!(serde_json::from_slice::<Call>(&wire).unwrap(), call);
        }
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
