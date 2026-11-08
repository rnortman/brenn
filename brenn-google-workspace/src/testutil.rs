//! Test helpers the Workspace components' `logic_test`s share.

use std::collections::BTreeMap;

use brenn_provider_kit::testutil::{failure, google, http, nested, truncated};
use brenn_provider_kit::{
    ApiCall, Call, ErrorKind, InFlight, NestedError, Next, Probes, Tool, internal,
};
use serde_json::{Value, json};

use crate::calendar::CALENDAR_ID_MAX_CHARS;
use crate::scope::{Scope, google_error, scope_hint};

/// A config lookup over `pairs`, as `Settings::from_lookup` reads one.
pub fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: BTreeMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |key| map.get(key).cloned()
}

/// A string of `n` characters, each two bytes in UTF-8.
pub fn wide(n: usize) -> String {
    "é".repeat(n)
}

/// A `key` of `max` characters is admitted and one of `max + 1` refused,
/// both in characters of two UTF-8 bytes, beside the arguments `base`.
pub fn admits_characters(
    plan_with: fn(Value) -> Result<ApiCall, String>,
    base: Value,
    key: &str,
    max: usize,
) {
    let with = |value: String| {
        let mut args = base.clone();
        args[key] = json!(value);
        plan_with(args)
    };
    assert!(with(wide(max)).is_ok(), "{key} at {max} characters");
    assert_eq!(
        with(wide(max + 1)).unwrap_err(),
        format!("`{key}` is longer than {max} characters"),
    );
}

/// `n` characters of the widest serialized width an argument parsed by
/// `capped` alone can take: `capped` admits controls, and U+0001 has no
/// short escape, so each is `\u0001`, 6 bytes, when serialized once
/// (`account`) and `\\u0001`, 7 bytes, inside a POST body serialized
/// again as the args. Four-byte characters are the widest only for an
/// argument whose parser refuses every character JSON escapes.
pub fn escaped_widest(n: usize) -> String {
    "\u{1}".repeat(n)
}

/// The `description` of the spec's `provide <block>` block, as the spec
/// writes it (escapes unexpanded).
pub fn description(spec: &str, block: &str) -> String {
    let opening = format!("provide {block} {{");
    spec.lines()
        .map(str::trim)
        .skip_while(|line| *line != opening)
        .find_map(|line| line.strip_prefix("description = \""))
        .unwrap_or_else(|| panic!("the spec's {block} block has no description"))
        .to_string()
}

/// A 200 read `api-request` cut at its byte cap: `bytes` long, the body a
/// JSON object's opening.
pub fn cut_read(bytes: Value) -> Result<Value, NestedError> {
    truncated(200, bytes, "{\"id")
}

/// The answer a step completed with.
pub fn completed(next: Next) -> Value {
    let Next::Complete(value) = next else {
        panic!("expected a completed answer, got {next:?}");
    };
    value
}

/// The row the next step is stored under, read back as the next
/// activation reads it, and the call it issues.
pub fn follow<T: Tool>(state: &InFlight<T>, next: Next) -> (InFlight<T>, Call) {
    let Next::Call { call, derived } = next else {
        panic!("expected a next step, got {next:?}");
    };
    let row = InFlight::decode(&state.next(&call, derived, 2_000).encode()).unwrap();
    assert_eq!(row.step, call.step);
    assert_eq!(row.target, call.api.target());
    assert_eq!(row.issued_at_ms, 2_000);
    (row, call)
}

/// `n` characters of four UTF-8 bytes, the first naming `i` so that
/// strings built for different `i` differ.
pub fn four_byte(i: usize, n: usize) -> String {
    let first = char::from_u32(0x1_0000 + u32::try_from(i).unwrap()).unwrap();
    std::iter::once(first)
        .chain(std::iter::repeat_n('\u{1d11e}', n - 1))
        .collect()
}

/// A `no_body_refused` detail at `status`, read back as the API it names
/// and the rest after ` answers `: what that API answers, and a write step's
/// hint where one was appended. `None` for any other detail.
pub fn no_body_parts(detail: &str, status: u64) -> Option<(&str, &str)> {
    detail
        .strip_prefix(&format!(
            "Google answered HTTP {status} with no body where "
        ))?
        .split_once(" answers ")
}

/// An events.get answer of one event with attendees, conferencing and a
/// recurring series' instance id.
pub const EVENT: &str = include_str!("fixtures/event.json");
/// The id `EVENT` carries.
pub const EVENT_ID: &str = "weekly0123456789_20261104T140000Z";

/// The fixture with `changes` applied, top-level keys replaced or, when
/// null, removed.
pub fn event_with(changes: Value) -> String {
    let mut body: Value = serde_json::from_str(EVENT).unwrap();
    for (key, value) in changes.as_object().unwrap() {
        if value.is_null() {
            body.as_object_mut().unwrap().remove(key);
        } else {
            body[key] = value.clone();
        }
    }
    body.to_string()
}

/// The probes of a calendar id argument: what `is_calendar_id` admits and
/// refuses past the length its schema states.
pub fn calendar_id_probes() -> Probes {
    Probes {
        admitted: [
            wide(CALENDAR_ID_MAX_CHARS),
            "team@group.calendar.example.com".to_string(),
            "#x?+".to_string(),
            "...".to_string(),
            ".a".to_string(),
        ]
        .into_iter()
        .map(Value::String)
        .collect(),
        refused: [
            wide(CALENDAR_ID_MAX_CHARS + 1).as_str(),
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a%b",
            "a\"b",
            "a\u{1}b",
            "a\u{85}b",
            "a\u{9f}b",
            "a b",
            "a\u{3000}b",
        ]
        .into_iter()
        .map(|id| json!(id))
        .chain([json!(1)])
        .collect(),
    }
}

/// Every state in `states` — each a step that only reads under `scope` —
/// fails bare when `answered` (the component's `advance`) meets a failure:
/// the kit's relay texts for a nested error, a not-JSON 200 and a redirect,
/// `google_error` under `scope` for Google's statuses, and the nested
/// `denied` relayed as `Denied`. No failure carries a write hint.
pub fn assert_read_failures_are_bare<T: Tool>(
    states: &[InFlight<T>],
    scope: Scope,
    answered: impl Fn(&InFlight<T>, Result<Value, NestedError>) -> Next,
) {
    for state in states {
        let at = format!("{} step {}", state.tool.name(), state.step);
        for (outcome, detail) in [
            (
                nested("invalid_args", "bad wrapper"),
                "api-request rejected the wrapper's request: bad wrapper",
            ),
            (
                nested("rate_limited", "slow down"),
                "rate_limited: slow down",
            ),
            (
                http(200, "<html>"),
                "Google returned HTTP 200 with a body that is not JSON",
            ),
            (
                http(302, ""),
                "Google answered 302 with a redirect, which Brenn does not follow",
            ),
        ] {
            assert_eq!(
                answered(state, outcome),
                Next::Fail(internal(detail)),
                "{at}"
            );
        }
        for status in [500, 503, 429] {
            assert_eq!(
                answered(state, http(status, "{}")),
                Next::Fail(google_error(status, "{}", scope)),
                "{at}"
            );
        }
        let denied = format!(
            "Google PERMISSION_DENIED (HTTP 403): Insufficient Permission. {}",
            scope_hint(scope)
        );
        for (outcome, kind, detail) in [
            (
                http(400, &google(400, "INVALID_ARGUMENT", "Invalid value")),
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Invalid value",
            ),
            (
                http(
                    403,
                    &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
                ),
                ErrorKind::Denied,
                denied.as_str(),
            ),
            (
                http(404, &google(404, "NOT_FOUND", "Not Found")),
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Not Found",
            ),
        ] {
            assert_eq!(
                answered(state, outcome),
                Next::Fail(failure(kind, detail)),
                "{at}"
            );
        }
        let Next::Fail(refused) = answered(state, nested("denied", "no clause admits it")) else {
            panic!("{at} fails");
        };
        assert_eq!(refused.kind, ErrorKind::Denied, "{at}");
        assert!(
            refused
                .detail
                .starts_with("api-request refused the nested ")
                && refused.detail.ends_with(": no clause admits it"),
            "{at}: {refused:?}"
        );
    }
}
