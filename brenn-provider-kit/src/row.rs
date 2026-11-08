//! The in-flight row: what the component's store holds for a request between
//! the activation that issues its nested call and the one that receives the
//! answer, keyed by the call id.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::activation::INFLIGHT_TTL_MS;
use crate::call::{Call, Failure, Target, internal};

/// A provided-tool enum as the spec scaffold generates it: each tool has a
/// registry name, and a name maps back to at most one tool.
pub trait Tool: Copy + fmt::Debug + PartialEq {
    fn name(self) -> &'static str;
    fn from_name(name: &str) -> Option<Self>;
}

/// The correlation id of a request's nested call: `<pending id>/<step>`.
pub(crate) fn call_id(id: &str, step: u8) -> String {
    format!("{id}/{step}")
}

/// A request's row. Its JSON names the tool by registry name, so a row
/// outlives a reordering of the enum but not a renamed or removed tool.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "T: Tool")]
pub struct InFlight<T: Tool> {
    /// The pending id as the host spells it.
    pub id: String,
    /// When the request arrived, which its deadline counts from.
    pub received_at_ms: u64,
    #[serde(with = "tool_name")]
    pub tool: T,
    pub step: u8,
    /// The caller's arguments, already validated once.
    pub args: Value,
    /// What earlier steps learned that later steps need.
    pub derived: Map<String, Value>,
    /// The nested call outstanding.
    pub target: Target,
    pub issued_at_ms: u64,
}

impl<T: Tool> InFlight<T> {
    pub fn new(
        id: &str,
        tool: T,
        args: &Value,
        call: &Call,
        derived: Map<String, Value>,
        received_at_ms: u64,
        now_ms: u64,
    ) -> InFlight<T> {
        InFlight {
            id: id.to_string(),
            received_at_ms,
            tool,
            step: call.step,
            args: args.clone(),
            derived,
            target: call.api.target(),
            issued_at_ms: now_ms,
        }
    }

    /// The row for this request's next step, `call`, carrying what earlier
    /// steps learned in `derived` and the request's arrival time.
    pub fn next(&self, call: &Call, derived: Map<String, Value>, now_ms: u64) -> InFlight<T> {
        InFlight::new(
            &self.id,
            self.tool,
            &self.args,
            call,
            derived,
            self.received_at_ms,
            now_ms,
        )
    }

    pub fn call_id(&self) -> String {
        call_id(&self.id, self.step)
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an InFlight row serializes")
    }

    /// A row this crate did not write is a corrupt store.
    pub fn decode(bytes: &[u8]) -> Result<InFlight<T>, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("in-flight row does not parse: {e}"))
    }
}

/// Whether a stored in-flight row has outlived any result that could still
/// arrive. Only the row's issue time is read: a row that old is garbage
/// whatever else it holds, including a row a different build wrote in a shape
/// this one does not parse. A row with no readable issue time is a corrupt
/// store.
pub(crate) fn row_expired(bytes: &[u8], now_ms: u64) -> Result<bool, String> {
    Ok(issued_at_ms(bytes)?.saturating_add(INFLIGHT_TTL_MS) < now_ms)
}

/// A stored in-flight row's issue time, read alone.
pub(crate) fn issued_at_ms(bytes: &[u8]) -> Result<u64, String> {
    #[derive(Deserialize)]
    struct Issued {
        issued_at_ms: u64,
    }
    let row: Issued = serde_json::from_slice(bytes)
        .map_err(|e| format!("in-flight row has no readable issue time: {e}"))?;
    Ok(row.issued_at_ms)
}

/// The in-flight state a result for request `id`, correlated by `call_id`,
/// answers; `row` is what the store holds under `call_id`. No row means the
/// result is a duplicate or arrived after its row was swept, and is ignored.
/// A row belonging to another request is a host fault.
pub(crate) fn claim<T: Tool>(
    id: &str,
    call_id: &str,
    row: Option<&[u8]>,
) -> Result<Option<InFlight<T>>, String> {
    let Some(bytes) = row else {
        return Ok(None);
    };
    let state = InFlight::<T>::decode(bytes)?;
    if state.id != id {
        return Err(format!(
            "result {call_id} serves {id}, but its in-flight row is {}'s",
            state.id
        ));
    }
    Ok(Some(state))
}

/// A row's stored arguments, parsed again. They were validated when the
/// request was served, so a failure here is the component's.
pub fn stored<T: Tool, A>(
    state: &InFlight<T>,
    parse: fn(&Value) -> Result<A, String>,
) -> Result<A, Failure> {
    parse(&state.args).map_err(|reason| {
        internal(format!(
            "stored {} arguments no longer parse: {reason}",
            state.tool.name()
        ))
    })
}

/// A row naming a step its tool does not have: the component's fault.
pub fn no_such_step<T: Tool>(tool: T, step: u8) -> Failure {
    internal(format!(
        "in-flight row names step {step} of {}, which has no such step",
        tool.name()
    ))
}

/// A stored row names its tool by the registry name the specification gives
/// it; a name the specification no longer declares does not parse.
pub(crate) mod tool_name {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::Tool;

    pub fn serialize<S: Serializer, T: Tool>(tool: &T, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(tool.name())
    }

    pub fn deserialize<'de, D: Deserializer<'de>, T: Tool>(deserializer: D) -> Result<T, D::Error> {
        let name = String::deserialize(deserializer)?;
        T::from_name(&name)
            .ok_or_else(|| D::Error::custom(format!("no provided tool is named `{name}`")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;
    use crate::call::{ApiCall, ErrorKind, Query};
    use crate::fixture::Fixture;
    use crate::testutil::failure;

    fn call(step: u8, path: &str) -> Call {
        Call {
            step,
            api: ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "api.example.com".to_string(),
                path: path.to_string(),
                query: Query::new(),
                headers: BTreeMap::new(),
                body: None,
            },
        }
    }

    fn state(args: Value) -> InFlight<Fixture> {
        InFlight::new(
            &"a".repeat(32),
            Fixture::Lookup,
            &args,
            &call(1, "/v1/items/one"),
            Map::new(),
            900,
            1_000,
        )
    }

    #[test]
    fn in_flight_round_trips_with_a_string_id() {
        let state = state(json!({ "id": "one", "account": "alice@example.com" }));
        assert_eq!(state.call_id(), format!("{}/1", "a".repeat(32)));
        assert_eq!(
            state.target,
            Target {
                method: "GET".to_string(),
                host: "api.example.com".to_string(),
                path: "/v1/items/one".to_string(),
            }
        );
        let bytes = state.encode();
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["id"], json!("a".repeat(32)));
        assert_eq!(wire["tool"], json!("fixture-lookup"));
        assert_eq!(InFlight::decode(&bytes).unwrap(), state);
        assert!(InFlight::<Fixture>::decode(b"{}").is_err());
    }

    #[test]
    fn a_rows_wire_shape_is_its_fields_in_order() {
        let state = state(json!({ "q": "x" }));
        assert_eq!(
            String::from_utf8(state.encode()).unwrap(),
            format!(
                "{{\"id\":\"{}\",\"received_at_ms\":900,\"tool\":\"fixture-lookup\",\"step\":1,\
                 \"args\":{{\"q\":\"x\"}},\
                 \"derived\":{{}},\"target\":{{\"method\":\"GET\",\"host\":\"api.example.com\",\
                 \"path\":\"/v1/items/one\"}},\"issued_at_ms\":1000}}",
                "a".repeat(32)
            )
        );
    }

    #[test]
    fn the_next_step_keeps_the_request_and_takes_the_new_call() {
        let first = state(json!({ "q": "x" }));
        let derived = Map::from_iter([("seen".to_string(), json!(3))]);
        let next = first.next(&call(2, "/v1/items/two"), derived.clone(), 2_000);
        assert_eq!(
            next,
            InFlight {
                step: 2,
                derived,
                target: call(2, "/v1/items/two").api.target(),
                issued_at_ms: 2_000,
                ..first
            }
        );
    }

    #[test]
    fn a_row_names_each_tool_by_its_registry_name() {
        for tool in Fixture::ALL {
            let row = InFlight {
                tool,
                ..state(json!({}))
            };
            let wire: Value = serde_json::from_slice(&row.encode()).unwrap();
            assert_eq!(wire["tool"], json!(tool.name()));
            assert_eq!(InFlight::decode(&row.encode()), Ok(row));
        }
    }

    #[test]
    fn a_row_naming_an_undeclared_tool_does_not_parse() {
        let mut wire: Value = serde_json::from_slice(&state(json!({})).encode()).unwrap();
        wire["tool"] = json!("Lookup");
        let error = InFlight::<Fixture>::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `Lookup`"),
            "{error}"
        );
    }

    #[test]
    fn a_row_expires_only_after_the_ttl() {
        let row = state(json!({})).encode();
        assert_eq!(row_expired(&row, 1_000), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
    }

    #[test]
    fn a_row_another_build_wrote_still_expires() {
        let mut wire: Value = serde_json::from_slice(&state(json!({})).encode()).unwrap();
        wire["tool"] = json!("no-such-tool");
        wire["added_later"] = json!(1);
        wire.as_object_mut().unwrap().remove("target");
        let row = serde_json::to_vec(&wire).unwrap();
        assert!(InFlight::<Fixture>::decode(&row).is_err());
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
    }

    #[test]
    fn a_row_with_a_field_this_build_does_not_write_does_not_parse() {
        let base: Value = serde_json::from_slice(&state(json!({})).encode()).unwrap();
        let mut extra = base.clone();
        extra["added_later"] = json!(1);
        let mut extra_in_target = base;
        extra_in_target["target"]["port"] = json!(443);
        for (wire, field) in [(extra, "added_later"), (extra_in_target, "port")] {
            let error =
                InFlight::<Fixture>::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
            assert!(
                error.starts_with(&format!(
                    "in-flight row does not parse: unknown field `{field}`"
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn a_row_with_no_issue_time_is_a_corrupt_store() {
        for row in [&b"{}"[..], b"not json", br#"{"issued_at_ms":"soon"}"#] {
            let error = row_expired(row, 0).unwrap_err();
            assert!(
                error.starts_with("in-flight row has no readable issue time: "),
                "{error}"
            );
        }
    }

    #[test]
    fn a_result_claims_its_requests_row_or_ignores_a_missing_one() {
        let state = state(json!({}));
        let call_id = state.call_id();
        assert_eq!(
            claim(&state.id, &call_id, Some(&state.encode())),
            Ok(Some(state.clone()))
        );
        assert_eq!(claim::<Fixture>(&state.id, &call_id, None), Ok(None));
    }

    #[test]
    fn a_result_whose_row_does_not_parse_is_a_corrupt_store() {
        let error = claim::<Fixture>(&"a".repeat(32), "x/1", Some(b"{}")).unwrap_err();
        assert!(
            error.starts_with("in-flight row does not parse: "),
            "{error}"
        );
    }

    #[test]
    fn a_result_for_another_requests_row_is_a_host_fault() {
        let state = state(json!({}));
        let other = "b".repeat(32);
        assert_eq!(
            claim::<Fixture>(&other, &state.call_id(), Some(&state.encode())),
            Err(format!(
                "result {}/1 serves {other}, but its in-flight row is {}'s",
                "a".repeat(32),
                "a".repeat(32)
            ))
        );
    }

    fn parse_q(args: &Value) -> Result<String, String> {
        args.get("q")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| "`q` is required".to_string())
    }

    #[test]
    fn advance_refuses_stored_arguments_that_no_longer_parse() {
        assert_eq!(
            stored(&state(json!({ "q": "x" })), parse_q),
            Ok("x".to_string())
        );
        assert_eq!(
            stored(&state(json!({})), parse_q),
            Err(failure(
                ErrorKind::Internal,
                "stored fixture-lookup arguments no longer parse: `q` is required"
            ))
        );
    }

    #[test]
    fn advance_refuses_a_step_the_tool_does_not_have() {
        assert_eq!(
            no_such_step(Fixture::Search, 2),
            failure(
                ErrorKind::Internal,
                "in-flight row names step 2 of fixture-search, which has no such step"
            )
        );
    }
}
