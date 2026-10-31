//! The wrapper's decisions, in `std` plus `serde`/`serde_json`: reading the
//! instance's `config`, validating a tool's arguments, building each Gmail
//! call as `api-request` arguments, reading `api-request`'s answer, and
//! compacting Google's JSON into the tool's result. Nothing here names the
//! guest SDK, so the whole module runs on the host under test. Tools are the
//! generated `Provided` enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::spec::Provided;

/// An in-flight row older than this is garbage: longer than the host's
/// 240 s provided-tool ceiling plus `api-request`'s 60 s HTTP timeout, so no
/// row whose result can still arrive is ever swept.
pub const INFLIGHT_TTL_MS: u64 = 300_000;

/// A compact result larger than this is a bug in a size bound, answered
/// `internal` rather than sent.
pub const COMPACT_RESULT_CEILING: usize = 256 * 1024;

/// The most bytes an `account` argument may hold: a mailbox address's limit.
const ACCOUNT_MAX_BYTES: usize = 320;

/// The most bytes a Gmail id (message, thread or label) may hold. Ids are one
/// path segment of letters, digits, `_` and `-`, which `api-request` neither
/// decodes nor refuses.
const ID_MAX_BYTES: usize = 128;

/// The most characters of a Google error message copied into a result.
const GOOGLE_MESSAGE_MAX_CHARS: usize = 1000;

/// The path Gmail lists the user's labels at, and reads one label below.
const LABELS_PATH: &str = "/gmail/v1/users/me/labels";
/// labels.list's mask: what a listing carries.
const LABELS_LIST_FIELDS: &str = "labels(id,name,type)";
/// labels.get's mask: one label with its counts.
const LABEL_GET_FIELDS: &str =
    "id,name,type,messagesTotal,messagesUnread,threadsTotal,threadsUnread";
/// labels.get's counts, as Google names them and as the answer names them.
const LABEL_COUNTS: &[(&str, &str)] = &[
    ("messagesTotal", "messages_total"),
    ("messagesUnread", "messages_unread"),
    ("threadsTotal", "threads_total"),
    ("threadsUnread", "threads_unread"),
];

/// The sentence every Google 403 is answered with: Gmail answers an account
/// connected without the read scope this way, and the component does not read
/// Google's reason codes to tell that refusal from another.
const SCOPE_HINT: &str = "If Google names insufficient authentication scopes, the account was \
                          connected without the Gmail read scope: run oauth-connect with that \
                          scope set and relay its steps.";

/// The instance's `config` map, validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The `oauth_provider` every call names.
    pub provider: String,
    /// The `host[:port]` Gmail is called on.
    pub gmail_host: String,
}

impl Settings {
    /// Read the map through `get`. A missing key, an empty value, or a host
    /// containing `/` is an error naming the key: nothing a misconfigured
    /// instance answered would be right.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings, String> {
        let required = |key: &str| -> Result<String, String> {
            match get(key) {
                None => Err(format!("config {key} is required")),
                Some(value) if value.is_empty() => Err(format!("config {key} is empty")),
                Some(value) => Ok(value),
            }
        };
        let host = |key: &str| -> Result<String, String> {
            let value = required(key)?;
            if value.contains('/') {
                return Err(format!(
                    "config {key}: {value:?} is not a host[:port]; it contains `/`"
                ));
            }
            Ok(value)
        };
        Ok(Settings {
            provider: required("provider")?,
            gmail_host: host("gmail_host")?,
        })
    }
}

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

fn internal(detail: impl Into<String>) -> Failure {
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

/// One `api-request` call's arguments. `account`, when the caller named one,
/// is passed on every call a request makes, so a request never mixes
/// accounts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApiCall {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub method: String,
    pub host: String,
    pub path: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
}

impl ApiCall {
    /// A GET on the Gmail host for `account`.
    fn gmail_get(
        settings: &Settings,
        account: Option<&str>,
        path: String,
        query: BTreeMap<String, String>,
    ) -> ApiCall {
        ApiCall {
            provider: settings.provider.clone(),
            account: account.map(str::to_string),
            method: "GET".to_string(),
            host: settings.gmail_host.clone(),
            path,
            query,
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

/// A nested call to issue: the request's step it is, and its arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    pub step: u8,
    pub api: ApiCall,
}

// TODO(provider-kit): the in-flight row (`call_id`, `InFlight`, `row_expired`,
// `claim`), the argument helpers, `NestedError` and the reading of
// `api-request`'s answer envelope are copied from `google-maps-tools`.

/// The correlation id of a request's nested call: `<pending id>/<step>`.
pub fn call_id(id: &str, step: u8) -> String {
    format!("{id}/{step}")
}

/// What the store holds for a request between the activation that issues its
/// nested call and the one that receives the answer, keyed by the call id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InFlight {
    /// The pending id as the host spells it.
    pub id: String,
    #[serde(with = "provided_name")]
    pub tool: Provided,
    pub step: u8,
    /// The caller's arguments, already validated once.
    pub args: Value,
    /// What earlier steps learned that later steps need.
    pub derived: Map<String, Value>,
    /// The nested call outstanding.
    pub target: Target,
    pub issued_at_ms: u64,
}

impl InFlight {
    pub fn new(
        id: &str,
        tool: Provided,
        args: &Value,
        call: &Call,
        derived: Map<String, Value>,
        now_ms: u64,
    ) -> InFlight {
        InFlight {
            id: id.to_string(),
            tool,
            step: call.step,
            args: args.clone(),
            derived,
            target: call.api.target(),
            issued_at_ms: now_ms,
        }
    }

    pub fn call_id(&self) -> String {
        call_id(&self.id, self.step)
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an InFlight row serializes")
    }

    /// A row this module did not write is a corrupt store.
    pub fn decode(bytes: &[u8]) -> Result<InFlight, String> {
        serde_json::from_slice(bytes).map_err(|e| format!("in-flight row does not parse: {e}"))
    }
}

/// Whether a stored in-flight row has outlived any result that could still
/// arrive. Only the row's issue time is read: a row that old is garbage
/// whatever else it holds, including a row a different build wrote in a shape
/// this one does not parse. A row with no readable issue time is a corrupt
/// store.
pub fn row_expired(bytes: &[u8], now_ms: u64) -> Result<bool, String> {
    #[derive(Deserialize)]
    struct Issued {
        issued_at_ms: u64,
    }
    let row: Issued = serde_json::from_slice(bytes)
        .map_err(|e| format!("in-flight row has no readable issue time: {e}"))?;
    Ok(row.issued_at_ms.saturating_add(INFLIGHT_TTL_MS) < now_ms)
}

/// The in-flight state a result for request `id`, correlated by `call_id`,
/// answers; `row` is what the store holds under `call_id`. No row means the
/// result is a duplicate or arrived after its row was swept, and is ignored.
/// A row belonging to another request is a host fault.
pub fn claim(id: &str, call_id: &str, row: Option<&[u8]>) -> Result<Option<InFlight>, String> {
    let Some(bytes) = row else {
        return Ok(None);
    };
    let state = InFlight::decode(bytes)?;
    if state.id != id {
        return Err(format!(
            "result {call_id} serves {id}, but its in-flight row is {}'s",
            state.id
        ));
    }
    Ok(Some(state))
}

/// A stored row names its tool by the registry name the specification gives
/// it; a name the specification no longer declares does not parse.
mod provided_name {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::spec::Provided;

    pub fn serialize<S: Serializer>(tool: &Provided, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(tool.name())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Provided, D::Error> {
        let name = String::deserialize(deserializer)?;
        Provided::from_name(&name)
            .ok_or_else(|| D::Error::custom(format!("no provided tool is named `{name}`")))
    }
}

/// How a request ends after its nested call answered.
#[derive(Clone, Debug, PartialEq)]
pub enum Next {
    Complete(Value),
    Fail(Failure),
}

type Args = Map<String, Value>;

fn object<'a>(args: &'a Value, allowed: &[&str]) -> Result<&'a Args, String> {
    let Value::Object(map) = args else {
        return Err("the arguments must be a JSON object".to_string());
    };
    if let Some(unknown) = map.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!(
            "unknown argument `{unknown}`; this tool takes {}",
            allowed
                .iter()
                .map(|key| format!("`{key}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(map)
}

/// A string argument of at most `cap` bytes, checked before anything else is
/// done with it.
fn capped<'a>(args: &'a Args, key: &str, cap: usize) -> Result<Option<&'a str>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => {
            if value.len() > cap {
                return Err(format!("`{key}` is longer than {cap} bytes"));
            }
            if value.trim().is_empty() {
                return Err(format!("`{key}` is empty"));
            }
            Ok(Some(value))
        }
        Some(_) => Err(format!("`{key}` must be a string")),
    }
}

/// The Google account to act on, when the caller named one.
fn account(args: &Args) -> Result<Option<String>, String> {
    Ok(capped(args, "account", ACCOUNT_MAX_BYTES)?.map(str::to_string))
}

/// A Gmail id argument: one path segment of letters, digits, `_` and `-`.
fn gmail_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(id))
            if (1..=ID_MAX_BYTES).contains(&id.len())
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') =>
        {
            Ok(Some(id.clone()))
        }
        Some(_) => Err(format!(
            "`{key}` must be 1 to {ID_MAX_BYTES} letters, digits, `_` or `-`"
        )),
    }
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::GmailLabels => {
            let labels = LabelsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: labels_call(&labels, settings),
            })
        }
    }
}

/// `GmailLabels`' arguments: one label by `id`, or the listing.
struct LabelsArgs {
    id: Option<String>,
    account: Option<String>,
}

impl LabelsArgs {
    fn parse(args: &Value) -> Result<LabelsArgs, String> {
        let map = object(args, &["id", "account"])?;
        Ok(LabelsArgs {
            id: gmail_id(map, "id")?,
            account: account(map)?,
        })
    }
}

/// labels.get for one label, or labels.list.
fn labels_call(args: &LabelsArgs, settings: &Settings) -> ApiCall {
    let (path, fields) = match &args.id {
        Some(id) => (format!("{LABELS_PATH}/{id}"), LABEL_GET_FIELDS),
        None => (LABELS_PATH.to_string(), LABELS_LIST_FIELDS),
    };
    ApiCall::gmail_get(
        settings,
        args.account.as_deref(),
        path,
        BTreeMap::from([("fields".to_string(), fields.to_string())]),
    )
}

/// A nested call's typed error, as the executor wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedError {
    pub kind: String,
    pub detail: String,
}

/// `text` cut to `max` characters, ending in `…` when cut.
fn cut(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        None => text.to_string(),
        Some((at, _)) => format!("{}…", &text[..at]),
    }
}

/// The detail of a nested call that issued but could not be queued: Brenn's
/// refusal text plus the one sentence of operator guidance.
pub fn not_issued(refusal: &str) -> String {
    format!(
        "api-request could not be issued: {refusal}. If this names the clause, the instance's \
         `tool api-request` grant must admit the method, host and path."
    )
}

/// One line for the log: status, size and truncation, or the error kind.
/// Never a body or a header.
pub fn result_summary(outcome: &Result<Value, NestedError>) -> String {
    match outcome {
        Ok(value) => format!(
            "status {} bytes {} truncated {}",
            value.get("status").unwrap_or(&Value::Null),
            value.get("bytes").unwrap_or(&Value::Null),
            value.get("truncated").unwrap_or(&Value::Null),
        ),
        Err(error) => format!("error {}", error.kind),
    }
}

/// What a nested `api-request` call answered, read.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    /// Google's 2xx JSON body.
    Google(Value),
    /// `api-request`'s `credential_invalid` value: the user has no usable
    /// credential, and the request is answered with this value unchanged so
    /// the caller runs `oauth-connect`.
    Relay(Value),
    /// Google's answer was cut at the provider's read limit; the size as the
    /// messages state it (`<n> bytes`, or `of unstated length`).
    Truncated(String),
}

/// `api-request`'s answer read: Google's JSON, the credential relay, a cut
/// answer for the caller to decide on, or the request's error answer. Only
/// `error.status` and `error.message` are ever copied out of a non-2xx body.
pub fn read_api_result(
    outcome: Result<Value, NestedError>,
    target: &Target,
) -> Result<Answer, Failure> {
    let value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let NestedError { kind, detail } = error;
            return Err(match kind.as_str() {
                "denied" => Failure {
                    kind: ErrorKind::Denied,
                    detail: format!(
                        "api-request refused the nested {} {}{}: {detail}",
                        target.method, target.host, target.path
                    ),
                },
                "invalid_args" => internal(format!(
                    "api-request rejected the wrapper's request: {detail}"
                )),
                "internal" => internal(format!("api-request: {detail}")),
                other => internal(format!("{other}: {detail}")),
            });
        }
    };
    if value.get("error").and_then(Value::as_str) == Some("credential_invalid") {
        return Ok(Answer::Relay(value));
    }
    let status = value
        .get("status")
        .and_then(Value::as_u64)
        .ok_or_else(|| internal("api-request's answer carries no status"))?;
    if value.get("truncated").and_then(Value::as_bool) == Some(true) {
        // On a cut answer `bytes` is the response's Content-Length, not a
        // count of what was read; null when Google sent none.
        return Ok(Answer::Truncated(
            match value.get("bytes").and_then(Value::as_u64) {
                Some(bytes) => format!("{bytes} bytes"),
                None => "of unstated length".to_string(),
            },
        ));
    }
    if value.get("body_base64").is_some() {
        return Err(internal(format!(
            "Google returned a non-text body ({status})"
        )));
    }
    if (300..400).contains(&status) {
        return Err(internal(format!(
            "Google answered {status} with a redirect, which Brenn does not follow"
        )));
    }
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(|| internal("api-request's answer carries no body"))?;
    if status >= 400 {
        return Err(google_error(status, body));
    }
    if !(200..300).contains(&status) {
        return Err(internal(format!(
            "Google answered HTTP {status}, which is neither a success nor an error"
        )));
    }
    serde_json::from_str(body).map(Answer::Google).map_err(|_| {
        internal(format!(
            "Google returned HTTP {status} with a body that is not JSON"
        ))
    })
}

/// A cut answer where the step has no use for one.
fn cut_answer(size: &str) -> Failure {
    internal(format!(
        "Google's answer ({size}) exceeded the provider's max_response_bytes and was cut"
    ))
}

/// A 4xx/5xx answer: Google's status and message when the body is Google's
/// standard error shape, nothing of it otherwise. 400 and 404 are the caller's
/// arguments; 403 is Google's refusal, answered with the scope hint; every
/// other status (401 after `api-request`'s retry, 429, 5xx) is the wrapper's
/// or Google's.
// TODO(errhandling-gmail-403-always-denied-with-scope-hint): a quota or
// API-not-enabled 403 is answered `denied` with the scope hint.
// TODO(errhandling-gmail-4xx-on-fixed-request-blamed-on-caller): a 400/404 on
// a request built from constants alone is answered `invalid_args`.
fn google_error(status: u64, body: &str) -> Failure {
    let kind = match status {
        400 | 404 => ErrorKind::InvalidArgs,
        403 => ErrorKind::Denied,
        _ => ErrorKind::Internal,
    };
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let error = parsed.as_ref().and_then(|body| body.get("error"));
    let field = |name: &str| error.and_then(|e| e.get(name)).and_then(Value::as_str);
    let mut detail = match (field("status"), field("message")) {
        (Some(google_status), Some(message)) => format!(
            "Google {google_status} (HTTP {status}): {}",
            cut(message, GOOGLE_MESSAGE_MAX_CHARS)
        ),
        _ => format!("Google HTTP {status}, body not in the standard error shape"),
    };
    if kind == ErrorKind::Denied {
        let stop = if detail.ends_with('.') { "" } else { "." };
        detail = format!("{detail}{stop} {SCOPE_HINT}");
    }
    Failure { kind, detail }
}

/// What the request does with its nested call's answer: complete or fail.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
    let body = match read_api_result(outcome, &state.target) {
        Ok(Answer::Google(body)) => body,
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Truncated(size)) => return Next::Fail(cut_answer(&size)),
        Err(failure) => return Next::Fail(failure),
    };
    let answer = match (state.tool, state.step) {
        (Provided::GmailLabels, 1) => {
            stored(state, LabelsArgs::parse).and_then(|args| match args.id {
                Some(_) => compact_label(&body),
                None => compact_labels(&body),
            })
        }
        (tool, step) => Err(internal(format!(
            "in-flight row names step {step} of {}, which has no such step",
            tool.name()
        ))),
    };
    match answer {
        Ok(value) => bounded(value, COMPACT_RESULT_CEILING),
        Err(failure) => Next::Fail(failure),
    }
}

/// A row's stored arguments, parsed again. They were validated when the
/// request was served, so a failure here is the wrapper's.
fn stored<T>(state: &InFlight, parse: fn(&Value) -> Result<T, String>) -> Result<T, Failure> {
    parse(&state.args).map_err(|reason| {
        internal(format!(
            "stored {} arguments no longer parse: {reason}",
            state.tool.name()
        ))
    })
}

/// labels.list as `{ "labels": [{ "id", "name", "type" }] }` in Google's
/// order. Google omits `labels` when there are none, which is an empty list.
fn compact_labels(body: &Value) -> Result<Value, Failure> {
    let Value::Object(body) = body else {
        return Err(shape("labels.list"));
    };
    let labels = match body.get("labels") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| label(item, "labels.list", &[]))
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(shape("labels.list")),
    };
    let mut out = Args::new();
    out.insert("labels".to_string(), Value::Array(labels));
    Ok(Value::Object(out))
}

/// labels.get as one label with its counts, each when Google gives it.
fn compact_label(body: &Value) -> Result<Value, Failure> {
    label(body, "labels.get", LABEL_COUNTS)
}

/// One label: its `id` (required), `name` and `type`, and the `extra` fields
/// renamed, each copied when present.
fn label(item: &Value, api: &str, extra: &[(&str, &str)]) -> Result<Value, Failure> {
    let Some(id @ Value::String(_)) = item.get("id") else {
        return Err(shape(api));
    };
    let mut out = Args::new();
    out.insert("id".to_string(), id.clone());
    copy(&mut out, "name", item.get("name"));
    copy(&mut out, "type", item.get("type"));
    for (google, name) in extra {
        copy(&mut out, name, item.get(*google));
    }
    Ok(Value::Object(out))
}

/// `value` as the request's answer, unless it serializes larger than
/// `ceiling` — a bound that was wrong, reported rather than sent.
pub fn bounded(value: Value, ceiling: usize) -> Next {
    let size = serde_json::to_vec(&value)
        .expect("a JSON value serializes")
        .len();
    if size > ceiling {
        return Next::Fail(internal(format!(
            "compact result of {size} bytes exceeds the wrapper's ceiling"
        )));
    }
    Next::Complete(value)
}

fn shape(api: &str) -> Failure {
    internal(format!("unexpected Gmail {api} response shape"))
}

/// Copy `from` into `to[name]` when present and not null; absent fields are
/// omitted, never invented.
fn copy(to: &mut Args, name: &str, from: Option<&Value>) {
    if let Some(value) = from.filter(|value| !value.is_null()) {
        to.insert(name.to_string(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    const BASE: &[(&str, &str)] = &[("provider", "google"), ("gmail_host", "gmail.example.com")];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE)).unwrap()
    }

    fn with(key: &'static str, value: &'static str) -> Vec<(&'static str, &'static str)> {
        let mut pairs: Vec<_> = BASE.iter().filter(|(k, _)| *k != key).copied().collect();
        pairs.push((key, value));
        pairs
    }

    #[test]
    fn settings_read_every_key() {
        assert_eq!(
            settings(),
            Settings {
                provider: "google".to_string(),
                gmail_host: "gmail.example.com".to_string(),
            }
        );
        let s = Settings::from_lookup(lookup(&with("gmail_host", "127.0.0.1:8080"))).unwrap();
        assert_eq!(s.gmail_host, "127.0.0.1:8080");
    }

    #[test]
    fn settings_refuse_a_missing_key_by_name() {
        for key in ["provider", "gmail_host"] {
            let pairs: Vec<_> = BASE.iter().filter(|(k, _)| *k != key).copied().collect();
            let err = Settings::from_lookup(lookup(&pairs)).unwrap_err();
            assert_eq!(err, format!("config {key} is required"));
        }
    }

    #[test]
    fn settings_refuse_bad_values() {
        for key in ["provider", "gmail_host"] {
            let err = Settings::from_lookup(lookup(&with(key, ""))).unwrap_err();
            assert_eq!(err, format!("config {key} is empty"));
        }
        let err = Settings::from_lookup(lookup(&with("gmail_host", "gmail.example.com/gmail")))
            .unwrap_err();
        assert_eq!(
            err,
            "config gmail_host: \"gmail.example.com/gmail\" is not a host[:port]; it contains `/`"
        );
    }

    /// The `config` map of the `GoogleWorkspaceTools` instance example in
    /// `docs/google-workspace.md` — the first `config = {` after that
    /// instance's opening line — is one this parser accepts, with the values
    /// the guide states.
    #[test]
    fn the_google_workspace_guide_config_map_parses() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../docs/google-workspace.md");
        let doc = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        let mut lines = doc
            .lines()
            .map(str::trim)
            .skip_while(|line| *line != "new workspace: GoogleWorkspaceTools {");
        assert_eq!(
            lines.next(),
            Some("new workspace: GoogleWorkspaceTools {"),
            "docs/google-workspace.md carries no `new workspace: GoogleWorkspaceTools {{` line"
        );
        let mut lines = lines.skip_while(|line| *line != "config = {");
        assert_eq!(
            lines.next(),
            Some("config = {"),
            "the guide's GoogleWorkspaceTools instance carries no `config = {{` line"
        );
        let pairs: Vec<(String, String)> = lines
            .take_while(|line| *line != "};")
            .map(|line| {
                let (key, value) = line
                    .trim_end_matches(',')
                    .split_once(" = ")
                    .unwrap_or_else(|| panic!("{line:?} is not `key = \"value\"`"));
                let value = value
                    .strip_prefix('"')
                    .and_then(|v| v.strip_suffix('"'))
                    .unwrap_or_else(|| panic!("{line:?} has no quoted value"));
                (key.to_string(), value.to_string())
            })
            .collect();
        let pairs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let s = Settings::from_lookup(lookup(&pairs)).unwrap_or_else(|error| {
            panic!("the guide's config map fails every activation: {error}")
        });
        assert_eq!(s.provider, "google");
        assert_eq!(s.gmail_host, "gmail.googleapis.com");
    }

    fn labels(args: Value) -> Result<ApiCall, String> {
        plan(Provided::GmailLabels, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn fields(mask: &str) -> BTreeMap<String, String> {
        BTreeMap::from([("fields".to_string(), mask.to_string())])
    }

    #[test]
    fn labels_without_an_id_lists_them() {
        let call = labels(json!({})).unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/labels".to_string(),
                query: fields("labels(id,name,type)"),
            }
        );
        // `api-request` refuses an argument it does not know, so an absent
        // account is no key at all.
        assert_eq!(
            serde_json::to_value(&call).unwrap(),
            json!({
                "provider": "google",
                "method": "GET",
                "host": "gmail.example.com",
                "path": "/gmail/v1/users/me/labels",
                "query": { "fields": "labels(id,name,type)" },
            })
        );
    }

    #[test]
    fn labels_with_an_id_reads_one_with_its_counts() {
        let call = labels(json!({ "id": "Label_12" })).unwrap();
        assert_eq!(call.path, "/gmail/v1/users/me/labels/Label_12");
        assert_eq!(
            call.query,
            fields("id,name,type,messagesTotal,messagesUnread,threadsTotal,threadsUnread")
        );
        let call = labels(json!({ "id": "INBOX" })).unwrap();
        assert_eq!(call.path, "/gmail/v1/users/me/labels/INBOX");
    }

    #[test]
    fn the_named_account_is_passed_on() {
        let call = labels(json!({ "account": "alice@example.com" })).unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            serde_json::to_value(&call).unwrap()["account"],
            json!("alice@example.com")
        );
    }

    #[test]
    fn labels_refuse_bad_arguments() {
        assert_eq!(
            labels(json!({ "name": "INBOX" })).unwrap_err(),
            "unknown argument `name`; this tool takes `id`, `account`"
        );
        assert_eq!(
            labels(json!([])).unwrap_err(),
            "the arguments must be a JSON object"
        );
        let id_error = "`id` must be 1 to 128 letters, digits, `_` or `-`";
        for id in [
            json!(""),
            json!("Label 12"),
            json!("../profile"),
            json!("Label%2F1"),
            json!("é"),
            json!("x".repeat(129)),
            json!(12),
        ] {
            assert_eq!(labels(json!({ "id": id })).unwrap_err(), id_error, "{id}");
        }
        assert!(labels(json!({ "id": "a-b_C9".repeat(21) })).is_ok());
        assert_eq!(
            labels(json!({ "account": "" })).unwrap_err(),
            "`account` is empty"
        );
        assert_eq!(
            labels(json!({ "account": 1 })).unwrap_err(),
            "`account` must be a string"
        );
        let long = format!("{}@example.com", "a".repeat(320));
        assert_eq!(
            labels(json!({ "account": long })).unwrap_err(),
            "`account` is longer than 320 bytes"
        );
    }

    fn target() -> Target {
        Target {
            method: "GET".to_string(),
            host: "gmail.example.com".to_string(),
            path: "/gmail/v1/users/me/labels".to_string(),
        }
    }

    fn nested(kind: &str, detail: &str) -> Result<Value, NestedError> {
        Err(NestedError {
            kind: kind.to_string(),
            detail: detail.to_string(),
        })
    }

    fn http(status: u64, body: &str) -> Result<Value, NestedError> {
        Ok(json!({
            "status": status,
            "headers": { "content-type": "application/json" },
            "bytes": body.len(),
            "truncated": false,
            "body": body,
        }))
    }

    fn read(outcome: Result<Value, NestedError>) -> Result<Answer, Failure> {
        read_api_result(outcome, &target())
    }

    fn failure(kind: ErrorKind, detail: &str) -> Failure {
        Failure {
            kind,
            detail: detail.to_string(),
        }
    }

    fn credential_invalid(account: Value) -> Value {
        json!({
            "error": "credential_invalid",
            "provider": "google",
            "account": account,
            "detail": "no account is connected for this provider",
        })
    }

    #[test]
    fn a_missing_credential_is_relayed_unchanged() {
        for account in [Value::Null, json!("alice@example.com")] {
            let value = credential_invalid(account);
            assert_eq!(read(Ok(value.clone())), Ok(Answer::Relay(value)));
        }
    }

    #[test]
    fn a_denied_nested_call_is_denied_naming_it() {
        assert_eq!(
            read(nested(
                "denied",
                "several google accounts are connected; name one as `account`"
            )),
            Err(failure(
                ErrorKind::Denied,
                "api-request refused the nested GET gmail.example.com/gmail/v1/users/me/labels: \
                 several google accounts are connected; name one as `account`"
            ))
        );
    }

    #[test]
    fn other_nested_errors_are_internal() {
        assert_eq!(
            read(nested("invalid_args", "bad query")),
            Err(failure(
                ErrorKind::Internal,
                "api-request rejected the wrapper's request: bad query"
            ))
        );
        assert_eq!(
            read(nested(
                "internal",
                "result exceeds [messaging] max_body_bytes"
            )),
            Err(failure(
                ErrorKind::Internal,
                "api-request: result exceeds [messaging] max_body_bytes"
            ))
        );
        assert_eq!(
            read(nested("rate_limited", "slow down")),
            Err(failure(ErrorKind::Internal, "rate_limited: slow down"))
        );
    }

    #[test]
    fn a_truncated_answer_states_its_size() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 300000, "truncated": true, "body": "{\"lab",
        }));
        assert_eq!(
            read(outcome),
            Ok(Answer::Truncated("300000 bytes".to_string()))
        );
        let unstated = Ok(json!({
            "status": 200, "headers": {}, "bytes": null, "truncated": true, "body": "",
        }));
        assert_eq!(
            read(unstated),
            Ok(Answer::Truncated("of unstated length".to_string()))
        );
        assert_eq!(
            cut_answer("of unstated length"),
            failure(
                ErrorKind::Internal,
                "Google's answer (of unstated length) exceeded the provider's \
                 max_response_bytes and was cut"
            )
        );
    }

    #[test]
    fn a_binary_body_or_a_redirect_is_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": 3, "truncated": false, "body_base64": "AAEC",
        }));
        assert_eq!(
            read(outcome),
            Err(failure(
                ErrorKind::Internal,
                "Google returned a non-text body (200)"
            ))
        );
        assert_eq!(
            read(http(302, "")),
            Err(failure(
                ErrorKind::Internal,
                "Google answered 302 with a redirect, which Brenn does not follow"
            ))
        );
    }

    fn google(code: u64, status: &str, message: &str) -> String {
        json!({ "error": { "code": code, "status": status, "message": message, "details": [{ "secret": "x" }] } })
            .to_string()
    }

    #[test]
    fn a_google_error_carries_its_status_and_message_only() {
        assert_eq!(
            read(http(
                400,
                &google(400, "INVALID_ARGUMENT", "Invalid pageToken")
            )),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Invalid pageToken"
            ))
        );
        assert_eq!(
            read(http(
                404,
                &google(404, "NOT_FOUND", "Requested entity was not found.")
            )),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Requested entity was not found."
            ))
        );
        for (code, status) in [
            (401, "UNAUTHENTICATED"),
            (429, "RESOURCE_EXHAUSTED"),
            (500, "INTERNAL"),
            (503, "UNAVAILABLE"),
        ] {
            assert_eq!(
                read(http(code, &google(code, status, "later"))),
                Err(failure(
                    ErrorKind::Internal,
                    &format!("Google {status} (HTTP {code}): later")
                ))
            );
        }
    }

    #[test]
    fn a_google_403_is_denied_with_the_scope_hint() {
        assert_eq!(
            read(http(
                403,
                &google(
                    403,
                    "PERMISSION_DENIED",
                    "Request had insufficient authentication scopes."
                )
            )),
            Err(failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail read scope: run oauth-connect with that scope set \
                 and relay its steps."
            ))
        );
        assert_eq!(
            read(http(403, "<html>SECRET</html>")),
            Err(failure(
                ErrorKind::Denied,
                &format!("Google HTTP 403, body not in the standard error shape. {SCOPE_HINT}")
            ))
        );
    }

    #[test]
    fn a_long_google_message_is_cut() {
        let message = "x".repeat(GOOGLE_MESSAGE_MAX_CHARS + 50);
        let detail = read(http(500, &google(500, "INTERNAL", &message)))
            .unwrap_err()
            .detail;
        assert_eq!(
            detail,
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(GOOGLE_MESSAGE_MAX_CHARS)
            )
        );
    }

    #[test]
    fn an_error_body_not_in_googles_shape_is_not_copied() {
        assert_eq!(
            read(http(400, "{\"error\": \"SECRET\"}")),
            Err(failure(
                ErrorKind::InvalidArgs,
                "Google HTTP 400, body not in the standard error shape"
            ))
        );
    }

    #[test]
    fn a_success_body_must_be_json() {
        assert_eq!(
            read(http(200, "not json")),
            Err(failure(
                ErrorKind::Internal,
                "Google returned HTTP 200 with a body that is not JSON"
            ))
        );
        assert_eq!(
            read(http(200, "{\"a\": 1}")),
            Ok(Answer::Google(json!({ "a": 1 })))
        );
    }

    #[test]
    fn an_answer_without_status_or_body_is_internal() {
        assert_eq!(
            read(Ok(json!({ "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no status"
            ))
        );
        assert_eq!(
            read(Ok(json!({ "status": 200, "truncated": false }))),
            Err(failure(
                ErrorKind::Internal,
                "api-request's answer carries no body"
            ))
        );
        assert!(read(http(101, "")).unwrap_err().detail.contains("HTTP 101"));
    }

    #[test]
    fn the_summary_names_status_size_and_truncation_only() {
        assert_eq!(
            result_summary(&http(200, "{\"secret\": 1}")),
            "status 200 bytes 13 truncated false"
        );
        assert_eq!(result_summary(&nested("denied", "secret")), "error denied");
    }

    #[test]
    fn not_issued_carries_brenns_text_and_the_guidance() {
        let detail = not_issued("call-async api-request: denied: no clause admits GET");
        assert!(detail.starts_with(
            "api-request could not be issued: call-async api-request: denied: no clause admits GET."
        ));
        assert!(detail.ends_with("must admit the method, host and path."));
    }

    fn labels_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailLabels, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::GmailLabels,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    const LABELS_BODY: &str = r#"{
      "labels": [
        { "id": "INBOX", "name": "INBOX", "type": "system" },
        { "id": "CATEGORY_PROMOTIONS", "name": "CATEGORY_PROMOTIONS", "type": "system" },
        { "id": "Label_7", "name": "Receipts", "type": "user" },
        { "id": "Label_8" }
      ]
    }"#;

    #[test]
    fn labels_list_compacts_in_googles_order() {
        let state = labels_state(json!({}));
        assert_eq!(
            advance(&state, http(200, LABELS_BODY)),
            Next::Complete(json!({
                "labels": [
                    { "id": "INBOX", "name": "INBOX", "type": "system" },
                    { "id": "CATEGORY_PROMOTIONS", "name": "CATEGORY_PROMOTIONS", "type": "system" },
                    { "id": "Label_7", "name": "Receipts", "type": "user" },
                    { "id": "Label_8" },
                ],
            }))
        );
        assert_eq!(
            advance(&state, http(200, "{}")),
            Next::Complete(json!({ "labels": [] }))
        );
    }

    #[test]
    fn label_get_carries_the_counts_google_gives() {
        let state = labels_state(json!({ "id": "Label_7" }));
        let body = r#"{ "id": "Label_7", "name": "Receipts", "type": "user",
            "messagesTotal": 120, "messagesUnread": 3, "threadsTotal": 98, "threadsUnread": 2 }"#;
        assert_eq!(
            advance(&state, http(200, body)),
            Next::Complete(json!({
                "id": "Label_7", "name": "Receipts", "type": "user",
                "messages_total": 120, "messages_unread": 3,
                "threads_total": 98, "threads_unread": 2,
            }))
        );
        let sparse = r#"{ "id": "INBOX", "name": "INBOX", "type": "system", "messagesUnread": 0 }"#;
        assert_eq!(
            advance(&labels_state(json!({ "id": "INBOX" })), http(200, sparse)),
            Next::Complete(json!({
                "id": "INBOX", "name": "INBOX", "type": "system", "messages_unread": 0,
            }))
        );
    }

    #[test]
    fn labels_refuse_an_unexpected_shape() {
        let list = labels_state(json!({}));
        for body in [
            "[]",
            "{\"labels\": {}}",
            "{\"labels\": [1]}",
            "{\"labels\": [{}]}",
        ] {
            assert_eq!(
                advance(&list, http(200, body)),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Gmail labels.list response shape"
                )),
                "{body}"
            );
        }
        let one = labels_state(json!({ "id": "INBOX" }));
        assert_eq!(
            advance(&one, http(200, "{\"name\": \"INBOX\"}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Gmail labels.get response shape"
            ))
        );
    }

    #[test]
    fn advance_relays_a_missing_credential_unchanged() {
        let value = credential_invalid(Value::Null);
        assert_eq!(
            advance(&labels_state(json!({})), Ok(value.clone())),
            Next::Complete(value)
        );
    }

    #[test]
    fn advance_answers_a_cut_listing_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": null, "truncated": true, "body": "{",
        }));
        assert_eq!(
            advance(&labels_state(json!({})), outcome),
            Next::Fail(cut_answer("of unstated length"))
        );
    }

    #[test]
    fn advance_passes_a_failed_call_through() {
        assert_eq!(
            advance(
                &labels_state(json!({ "id": "Label_404" })),
                http(404, &google(404, "NOT_FOUND", "Invalid label: Label_404"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Invalid label: Label_404"
            ))
        );
    }

    #[test]
    fn advance_refuses_a_step_the_tool_does_not_have() {
        let mut state = labels_state(json!({}));
        state.step = 2;
        assert_eq!(
            advance(&state, http(200, "{}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names step 2 of gmail-labels, which has no such step"
            ))
        );
    }

    #[test]
    fn advance_refuses_stored_arguments_that_no_longer_parse() {
        let mut state = labels_state(json!({}));
        state.args = json!({ "id": "a/b" });
        assert_eq!(
            advance(&state, http(200, LABELS_BODY)),
            Next::Fail(failure(
                ErrorKind::Internal,
                "stored gmail-labels arguments no longer parse: `id` must be 1 to 128 letters, \
                 digits, `_` or `-`"
            ))
        );
    }

    #[test]
    fn the_result_ceiling_reports_rather_than_sends() {
        let value = json!({ "text": "x".repeat(100) });
        assert_eq!(bounded(value.clone(), 1_000), Next::Complete(value.clone()));
        let size = serde_json::to_vec(&value).unwrap().len();
        assert_eq!(
            bounded(value, 50),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!("compact result of {size} bytes exceeds the wrapper's ceiling")
            ))
        );
    }

    #[test]
    fn in_flight_round_trips_with_a_string_id() {
        let state = labels_state(json!({ "id": "INBOX", "account": "alice@example.com" }));
        assert_eq!(state.call_id(), format!("{}/1", "a".repeat(32)));
        assert_eq!(
            state.target,
            Target {
                method: "GET".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/labels/INBOX".to_string(),
            }
        );
        let bytes = state.encode();
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["id"], json!("a".repeat(32)));
        assert_eq!(wire["tool"], json!("gmail-labels"));
        assert_eq!(InFlight::decode(&bytes).unwrap(), state);
        assert!(InFlight::decode(b"{}").is_err());
    }

    #[test]
    fn a_row_expires_only_after_the_ttl() {
        let row = labels_state(json!({})).encode();
        assert_eq!(row_expired(&row, 1_000), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
    }

    #[test]
    fn a_row_another_build_wrote_still_expires() {
        let mut wire: Value = serde_json::from_slice(&labels_state(json!({})).encode()).unwrap();
        wire["tool"] = json!("no-such-tool");
        wire["added_later"] = json!(1);
        wire.as_object_mut().unwrap().remove("target");
        let row = serde_json::to_vec(&wire).unwrap();
        assert!(InFlight::decode(&row).is_err());
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS), Ok(false));
        assert_eq!(row_expired(&row, 1_000 + INFLIGHT_TTL_MS + 1), Ok(true));
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
        let state = labels_state(json!({}));
        let call_id = state.call_id();
        assert_eq!(
            claim(&state.id, &call_id, Some(&state.encode())),
            Ok(Some(state.clone()))
        );
        assert_eq!(claim(&state.id, &call_id, None), Ok(None));
    }

    #[test]
    fn a_result_for_another_requests_row_is_a_host_fault() {
        let state = labels_state(json!({}));
        let other = "b".repeat(32);
        assert_eq!(
            claim(&other, &state.call_id(), Some(&state.encode())),
            Err(format!(
                "result {}/1 serves {other}, but its in-flight row is {}'s",
                "a".repeat(32),
                "a".repeat(32)
            ))
        );
        let error = claim(&other, "x/1", Some(b"{}")).unwrap_err();
        assert!(
            error.starts_with("in-flight row does not parse: "),
            "{error}"
        );
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = labels_state(json!({}));
        for tool in Provided::ALL {
            let row = InFlight {
                tool,
                ..state.clone()
            };
            let wire: Value = serde_json::from_slice(&row.encode()).unwrap();
            assert_eq!(wire["tool"], json!(tool.name()));
            assert_eq!(InFlight::decode(&row.encode()).unwrap().tool, tool);
        }
        let mut wire: Value = serde_json::from_slice(&state.encode()).unwrap();
        wire["tool"] = json!("GmailLabels");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `GmailLabels`"),
            "{error}"
        );
    }
}
