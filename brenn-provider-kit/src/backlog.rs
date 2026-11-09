//! The backlog row: a request whose nested call the loop has not issued yet,
//! because the activation that would have issued it had spent its call
//! budget. Keyed by the request's arrival time then pending id, so a
//! byte-ordered scan reads the oldest request first.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::call::Call;
use crate::row::{InFlight, Tool, tool_name};

/// A queued request: everything its in-flight row needs, and the whole call,
/// which has not been issued.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "T: Tool")]
pub struct Queued<T: Tool> {
    /// The pending id as the host spells it.
    pub id: String,
    #[serde(with = "tool_name")]
    pub tool: T,
    /// The caller's arguments, already validated once.
    pub args: Value,
    /// What earlier steps learned that later steps need.
    pub derived: Map<String, Value>,
    pub call: Call,
    /// When the request arrived, which its deadline counts from.
    pub received_at_ms: u64,
    /// When this entry was written, which its wait in the backlog counts
    /// from: the arrival for a first step, the earlier step's answer for a
    /// later one.
    pub queued_at_ms: u64,
}

/// The arrival time and pending id a backlog key spells, or why the key is
/// not one [`Queued::key`] writes.
pub fn parse_key(key: &[u8]) -> Result<(u64, &str), String> {
    let malformed = || {
        format!(
            "backlog key {} is not `<20-digit arrival>/<id>`",
            String::from_utf8_lossy(key)
        )
    };
    let key = std::str::from_utf8(key).map_err(|_| malformed())?;
    let (at, id) = key.split_once('/').ok_or_else(malformed)?;
    if at.len() != 20 || !at.bytes().all(|b| b.is_ascii_digit()) || id.is_empty() {
        return Err(malformed());
    }
    Ok((at.parse().map_err(|_| malformed())?, id))
}

impl<T: Tool> Queued<T> {
    /// The store key: arrival time, zero-padded so bytes order as numbers,
    /// then the pending id. A request holds at most one entry.
    pub fn key(&self) -> String {
        format!("{:020}/{}", self.received_at_ms, self.id)
    }

    /// The in-flight row this request keeps once its call is issued at `now_ms`.
    pub fn in_flight(&self, now_ms: u64) -> InFlight<T> {
        InFlight::new(
            &self.id,
            self.tool,
            &self.args,
            &self.call,
            self.derived.clone(),
            self.received_at_ms,
            now_ms,
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a Queued row serializes")
    }

    /// A row this crate did not write is a corrupt store.
    pub fn decode(bytes: &[u8]) -> Result<Queued<T>, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("backlog row does not parse: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::call::{ApiCall, Body, Query, QueryValue, query};
    use crate::fixture::Fixture;

    fn queued(received_at_ms: u64, id: &str) -> Queued<Fixture> {
        Queued {
            id: id.to_string(),
            tool: Fixture::Search,
            args: json!({ "q": "x" }),
            derived: Map::from_iter([("seen".to_string(), json!(3))]),
            call: Call {
                step: 2,
                api: ApiCall {
                    provider: "example".to_string(),
                    account: None,
                    method: "GET".to_string(),
                    host: "api.example.com".to_string(),
                    path: "/v1/items".to_string(),
                    query: Query::new(),
                    headers: BTreeMap::new(),
                    body: None,
                },
            },
            received_at_ms,
            queued_at_ms: received_at_ms + 500,
        }
    }

    #[test]
    fn a_queued_row_round_trips_with_its_whole_call() {
        let bare = queued(1_000, "a");
        let mut full = queued(1_000, "b");
        full.call.api.account = Some("alice@example.com".to_string());
        full.call.api.query = query([("q", "cafe".to_string())]);
        full.call.api.query.insert(
            "fields".to_string(),
            QueryValue::Many(vec!["id".to_string()]),
        );
        full.call
            .api
            .headers
            .insert("X-Field-Mask".to_string(), "id".to_string());
        full.call.api.method = "POST".to_string();
        full.call.api.body = Some(Body::Text("{\"a\":1}".to_string()));
        for row in [bare, full] {
            assert_eq!(Queued::decode(&row.encode()), Ok(row));
        }
    }

    #[test]
    fn a_queued_row_with_an_unknown_field_does_not_parse() {
        let base: Value = serde_json::from_slice(&queued(1, "a").encode()).unwrap();
        let mut top = base.clone();
        top["added_later"] = json!(1);
        let mut in_call = base.clone();
        in_call["call"]["added_later"] = json!(1);
        let mut in_api = base;
        in_api["call"]["api"]["added_later"] = json!(1);
        for wire in [top, in_call, in_api] {
            let error = Queued::<Fixture>::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
            assert!(
                error.starts_with("backlog row does not parse: unknown field `added_later`"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_queued_row_naming_an_undeclared_tool_does_not_parse() {
        let mut wire: Value = serde_json::from_slice(&queued(1, "a").encode()).unwrap();
        wire["tool"] = json!("Search");
        let error = Queued::<Fixture>::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `Search`"),
            "{error}"
        );
    }

    #[test]
    fn backlog_keys_order_by_arrival_then_id() {
        let mut keys = [
            queued(10_000, "a").key(),
            queued(9, "b").key(),
            queued(9, "a").key(),
            queued(100, "a").key(),
        ];
        keys.sort();
        assert_eq!(
            keys,
            [
                "00000000000000000009/a",
                "00000000000000000009/b",
                "00000000000000000100/a",
                "00000000000000010000/a",
            ]
        );
    }

    #[test]
    fn a_key_parses_back_to_its_arrival_and_id() {
        let row = queued(1_234, "r/7");
        assert_eq!(parse_key(row.key().as_bytes()), Ok((1_234, "r/7")));
        for bad in [
            &b"1234/r"[..],
            b"0000000000000000123x/r",
            b"00000000000000001234",
            b"00000000000000001234/",
            b"\xff",
        ] {
            let error = parse_key(bad).unwrap_err();
            assert!(
                error.ends_with("is not `<20-digit arrival>/<id>`"),
                "{error}"
            );
        }
    }

    #[test]
    fn issuing_a_queued_request_keeps_its_arrival_and_state() {
        let row = queued(1_000, "a");
        let state = row.in_flight(5_000);
        assert_eq!(state.id, "a");
        assert_eq!(state.received_at_ms, 1_000);
        assert_eq!(state.issued_at_ms, 5_000);
        assert_eq!(state.step, 2);
        assert_eq!(state.tool, Fixture::Search);
        assert_eq!(state.args, row.args);
        assert_eq!(state.derived, row.derived);
        assert_eq!(state.target, row.call.api.target());
    }
}
