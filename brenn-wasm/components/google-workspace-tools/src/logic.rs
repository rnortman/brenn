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

// The decoder is this module's child in the component and the host test
// alike: the path resolves beside this file whether it is `crate::logic` or
// the crate root.
#[path = "decode.rs"]
mod decode;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

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

/// The most UTF-8 bytes of a Google error message copied into a result.
const GOOGLE_MESSAGE_MAX_BYTES: usize = 1000;

/// The most UTF-8 bytes of a message header's value copied into a result. A
/// sender writes these, and nothing bounds their length.
///
/// After `plain`, a string serializes to at most two bytes per UTF-8 byte, so
/// a value cut here serializes to at most 2 003 bytes (`…` is three). A
/// search line is then at most 8 438 serialized bytes plus its label names
/// (three headers and a snippet at 2 003 each, two 128-byte ids, keys and
/// date), the page's envelope at most 8 300 (a page token of
/// `PAGE_MAX_BYTES` serializes to at most 8 206), and a page of 25 lines
/// stays under `COMPACT_RESULT_CEILING` while each line's label names
/// serialize to at most 1 715 bytes. A page past that answers `internal`.
const HEADER_MAX_BYTES: usize = 1000;

/// The most UTF-8 bytes of a message's snippet copied into a result. Google's
/// snippet is itself a short preview; the bound only closes the page's size
/// arithmetic, so a cut is not flagged.
const SNIPPET_MAX_BYTES: usize = 1000;

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

/// The step that names a finished answer's user labels, whichever tool's.
const LABELS_STEP: u8 = 100;
/// The finishing step's mask: names only.
const LABEL_NAMES_FIELDS: &str = "labels(id,name)";
/// The prefix of Google's user-label ids; every system label's id is its name.
const USER_LABEL_PREFIX: &str = "Label_";

/// The path Gmail lists messages at, and reads one message below.
const MESSAGES_PATH: &str = "/gmail/v1/users/me/messages";
/// messages.list's mask: the ids, the page token, the estimate.
const MESSAGES_LIST_FIELDS: &str = "messages(id,threadId),nextPageToken,resultSizeEstimate";
/// messages.get's metadata mask: what one search line needs.
const METADATA_FIELDS: &str =
    "id,threadId,labelIds,snippet,internalDate,sizeEstimate,payload/headers";
/// The headers a search line carries, requested by repeating `metadataHeaders`.
const SEARCH_HEADERS: &[&str] = &["From", "Subject", "Date"];

/// The most bytes a search query may hold.
const QUERY_MAX_BYTES: usize = 1024;
/// The most bytes a page token may hold.
const PAGE_MAX_BYTES: usize = 4096;
/// `GmailSearch`'s `max`: the range and the default.
const SEARCH_MAX_MIN: u64 = 1;
const SEARCH_MAX_MAX: u64 = 25;
const SEARCH_MAX_DEFAULT: u64 = 10;
/// `GmailSearch`'s per-message steps: listed id `i` is read at step `2 + i`.
const SEARCH_FIRST_GET: u8 = 2;
const SEARCH_LAST_GET: u8 = SEARCH_FIRST_GET + SEARCH_MAX_MAX as u8 - 1;

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

/// One query parameter: a single value, or several that `api-request` sends
/// as the key repeated once per value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum QueryValue {
    One(String),
    Many(Vec<String>),
}

type Query = BTreeMap<String, QueryValue>;

/// A query from `(key, value)` pairs, each sent once.
fn query<const N: usize>(pairs: [(&str, String); N]) -> Query {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), QueryValue::One(value)))
        .collect()
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
    pub query: Query,
}

impl ApiCall {
    /// A GET on the Gmail host for `account`.
    fn gmail_get(
        settings: &Settings,
        account: Option<&str>,
        path: String,
        query: Query,
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

// TODO(provider-kit): the in-flight row and step machinery (`call_id`,
// `InFlight` with `next`, `row_expired`, `claim`, `stored`, `Next::Call`), the
// argument helpers, `NestedError`, the reading of `api-request`'s answer
// envelope and `lib.rs` are copied from `google-maps-tools`.

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

    /// The row for this request's next step, `call`, carrying what earlier
    /// steps learned in `derived`.
    pub fn next(&self, call: &Call, derived: Map<String, Value>, now_ms: u64) -> InFlight {
        InFlight::new(&self.id, self.tool, &self.args, call, derived, now_ms)
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

/// Whether `id` is one path segment of letters, digits, `_` and `-`.
fn is_gmail_id(id: &str) -> bool {
    (1..=ID_MAX_BYTES).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A Gmail id argument: one path segment of letters, digits, `_` and `-`.
fn gmail_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(id)) if is_gmail_id(id) => Ok(Some(id.clone())),
        Some(_) => Err(format!(
            "`{key}` must be 1 to {ID_MAX_BYTES} letters, digits, `_` or `-`"
        )),
    }
}

/// An integer argument in `min..=max`.
fn integer(args: &Args, key: &str, min: u64, max: u64) -> Result<Option<u64>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(value) => match value.as_u64() {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(format!("`{key}` must be an integer from {min} to {max}")),
        },
    }
}

/// A boolean argument; absent is `false`.
fn flag(args: &Args, key: &str) -> Result<bool, String> {
    match args.get(key) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(format!("`{key}` must be true or false")),
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
        Provided::GmailSearch => {
            let search = SearchArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: search_list_call(&search, settings),
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
    // TODO(provider-schema-parse-parity): these bounds and the `gmail-labels`
    // `input_schema` in the spec are stated separately.
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
        query([("fields", fields.to_string())]),
    )
}

/// `GmailSearch`'s arguments.
struct SearchArgs {
    q: String,
    max: u64,
    page: Option<String>,
    spam_trash: bool,
    account: Option<String>,
}

impl SearchArgs {
    // TODO(provider-schema-parse-parity): these bounds and the `gmail-search`
    // `input_schema` in the spec are stated separately.
    fn parse(args: &Value) -> Result<SearchArgs, String> {
        let map = object(args, &["q", "max", "page", "spam_trash", "account"])?;
        Ok(SearchArgs {
            q: capped(map, "q", QUERY_MAX_BYTES)?
                .ok_or("`q` is required")?
                .to_string(),
            max: integer(map, "max", SEARCH_MAX_MIN, SEARCH_MAX_MAX)?.unwrap_or(SEARCH_MAX_DEFAULT),
            page: capped(map, "page", PAGE_MAX_BYTES)?.map(str::to_string),
            spam_trash: flag(map, "spam_trash")?,
            account: account(map)?,
        })
    }
}

/// messages.list for the query, one page of at most `max` ids.
fn search_list_call(args: &SearchArgs, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("q", args.q.clone()),
        ("maxResults", args.max.to_string()),
        ("fields", MESSAGES_LIST_FIELDS.to_string()),
    ]);
    if let Some(page) = &args.page {
        query.insert("pageToken".to_string(), QueryValue::One(page.clone()));
    }
    if args.spam_trash {
        query.insert(
            "includeSpamTrash".to_string(),
            QueryValue::One("true".to_string()),
        );
    }
    ApiCall::gmail_get(
        settings,
        args.account.as_deref(),
        MESSAGES_PATH.to_string(),
        query,
    )
}

/// messages.get of one message's metadata: the search line's headers, labels
/// and snippet, no body.
fn message_metadata_call(id: &str, account: Option<&str>, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("format", "metadata".to_string()),
        ("fields", METADATA_FIELDS.to_string()),
    ]);
    query.insert(
        "metadataHeaders".to_string(),
        QueryValue::Many(SEARCH_HEADERS.iter().map(|h| h.to_string()).collect()),
    );
    ApiCall::gmail_get(settings, account, format!("{MESSAGES_PATH}/{id}"), query)
}

/// labels.list for the names of the user labels an answer carries.
fn label_names_call(account: Option<&str>, settings: &Settings) -> ApiCall {
    ApiCall::gmail_get(
        settings,
        account,
        LABELS_PATH.to_string(),
        query([("fields", LABEL_NAMES_FIELDS.to_string())]),
    )
}

/// A header value as answered, and whether it was cut to its bound.
#[derive(Debug, PartialEq, Eq)]
struct HeaderValue {
    value: String,
    cut: bool,
}

/// The value of the header `name` in `message`'s `payload.headers` list,
/// passed through `plain` and cut to `HEADER_MAX_BYTES`. `None` when the
/// message has no such header, or no `payload` or `headers` at all, as Google
/// omits them when no header matches the mask. A `payload` that is not an
/// object or a header list not of Gmail's shape is Google's fault, reported
/// against `api`.
fn header_value(message: &Value, name: &str, api: &str) -> Result<Option<HeaderValue>, Failure> {
    let headers = match message.get("payload") {
        None => return Ok(None),
        Some(Value::Object(payload)) => match payload.get("headers") {
            None => return Ok(None),
            Some(headers) => headers,
        },
        Some(_) => return Err(shape(api)),
    };
    let found = decode::header(headers, name).map_err(|decode::MalformedHeaders| shape(api))?;
    Ok(found.map(|value| {
        let (value, cut) = cut(&plain(value), HEADER_MAX_BYTES);
        HeaderValue { value, cut }
    }))
}

/// A message's snippet as answered: character references decoded, passed
/// through `plain` and cut to `SNIPPET_MAX_BYTES`. `None` when Google gave
/// none; a snippet that is not a string is Google's fault.
fn snippet_value(message: &Value, api: &str) -> Result<Option<String>, Failure> {
    match message.get("snippet") {
        None => Ok(None),
        Some(Value::String(snippet)) => {
            let decoded = plain(&decode::decode_entities(snippet));
            Ok(Some(cut(&decoded, SNIPPET_MAX_BYTES).0))
        }
        Some(_) => Err(shape(api)),
    }
}

/// A nested call's typed error, as the executor wrote it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NestedError {
    pub kind: String,
    pub detail: String,
}

/// `text` cut to the longest prefix of at most `max_bytes` UTF-8 bytes that
/// ends on a char boundary, ending in `…` when cut, and whether it was cut. A
/// character straddling the bound is dropped whole.
fn cut(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_string(), false);
    }
    let at = text.floor_char_boundary(max_bytes);
    (format!("{}…", &text[..at]), true)
}

/// `text` with every C0 control character other than tab, newline and
/// carriage return replaced by a space. Each of those would serialize as six
/// bytes (`\u00XX`) and has no use in a header, a snippet or a body.
fn plain(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '\t' | '\n' | '\r' => c,
            '\u{0}'..='\u{1f}' => ' ',
            _ => c,
        })
        .collect()
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
    /// Google's whole 4xx/5xx answer: its HTTP status, for a step that
    /// treats one status as an outcome, and the failure every other step
    /// answers with.
    GoogleError { status: u64, failure: Failure },
}

/// `api-request`'s answer read: Google's JSON, the credential relay, a cut
/// answer or Google's error for the caller to decide on, or the request's
/// error answer. Only `error.status` and `error.message` are ever copied out
/// of a non-2xx body.
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
        return Ok(Answer::GoogleError {
            status,
            failure: google_error(status, body),
        });
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
            cut(message, GOOGLE_MESSAGE_MAX_BYTES).0
        ),
        _ => format!("Google HTTP {status}, body not in the standard error shape"),
    };
    if kind == ErrorKind::Denied {
        let stop = if detail.ends_with('.') { "" } else { "." };
        detail = format!("{detail}{stop} {SCOPE_HINT}");
    }
    Failure { kind, detail }
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step. Every answer completed here is held to the result
/// ceiling; a relayed credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
    let body = match read_api_result(outcome, &state.target) {
        Ok(Answer::Google(body)) => body,
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Truncated(size)) => return Next::Fail(cut_answer(&size)),
        // A message deleted between the listing and its read is skipped, not
        // the search's failure.
        Ok(Answer::GoogleError { status: 404, .. })
            if matches!(
                (state.tool, state.step),
                (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET)
            ) =>
        {
            return settle(search_message(state, None, settings));
        }
        Ok(Answer::GoogleError { failure, .. }) => return Next::Fail(failure),
        Err(failure) => return Next::Fail(failure),
    };
    settle(match (state.tool, state.step) {
        (Provided::GmailLabels, 1) => stored(state, LabelsArgs::parse)
            .and_then(|args| match args.id {
                Some(_) => compact_label(&body),
                None => compact_labels(&body),
            })
            .map(Next::Complete),
        (Provided::GmailSearch, 1) => search_listed(state, &body, settings),
        (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET) => {
            search_message(state, Some(&body), settings)
        }
        (Provided::GmailSearch, LABELS_STEP) => named(state, &body),
        (tool, step) => Err(internal(format!(
            "in-flight row names step {step} of {}, which has no such step",
            tool.name()
        ))),
    })
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling.
fn settle(decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => bounded(value, COMPACT_RESULT_CEILING),
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

/// messages.list read: an empty page completes at once; otherwise the first
/// listed message is read next, the ids and the page's token and estimate
/// riding in `derived`.
fn search_listed(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, SearchArgs::parse)?;
    let Value::Object(body) = body else {
        return Err(shape("messages.list"));
    };
    let ids: Vec<Value> = match body.get("messages") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item.get("id") {
                Some(Value::String(id)) if is_gmail_id(id) => Ok(json!(id)),
                _ => Err(shape("messages.list")),
            })
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(shape("messages.list")),
    };
    if ids.len() as u64 > args.max {
        return Err(internal(format!(
            "Gmail messages.list answered {} messages for maxResults {}",
            ids.len(),
            args.max
        )));
    }
    let mut derived = Args::new();
    match body.get("nextPageToken") {
        None => {}
        Some(Value::String(token)) if token.len() <= PAGE_MAX_BYTES => {
            derived.insert("next_page".to_string(), json!(token));
        }
        Some(_) => return Err(shape("messages.list")),
    }
    match body.get("resultSizeEstimate") {
        None => {}
        Some(estimate @ Value::Number(_)) => {
            derived.insert("result_size_estimate".to_string(), estimate.clone());
        }
        Some(_) => return Err(shape("messages.list")),
    }
    let Some(Value::String(first)) = ids.first() else {
        return Ok(Next::Complete(search_answer(derived)));
    };
    let call = Call {
        step: SEARCH_FIRST_GET,
        api: message_metadata_call(first, args.account.as_deref(), settings),
    };
    derived.insert("ids".to_string(), Value::Array(ids));
    derived.insert("lines".to_string(), Value::Array(Vec::new()));
    derived.insert("unavailable".to_string(), Value::Array(Vec::new()));
    Ok(Next::Call { call, derived })
}

/// One listed message's metadata read — or, for `None`, found deleted —
/// recorded in `derived`; then the next listed message is read, or the
/// search is finished.
fn search_message(
    state: &InFlight,
    body: Option<&Value>,
    settings: &Settings,
) -> Result<Next, Failure> {
    let args = stored(state, SearchArgs::parse)?;
    let mut derived = state.derived.clone();
    let index = usize::from(state.step - SEARCH_FIRST_GET);
    let ids = derived_list(&derived, "ids")?;
    let Some(Value::String(id)) = ids.get(index) else {
        return Err(internal(format!(
            "in-flight row at gmail-search step {} lists no message id for it",
            state.step
        )));
    };
    let next_id = ids.get(index + 1).cloned();
    let (key, entry) = match body {
        Some(body) => ("lines", search_line(body, id)?),
        None => ("unavailable", json!(id)),
    };
    let mut list = derived_list(&derived, key)?;
    list.push(entry);
    derived.insert(key.to_string(), Value::Array(list));
    match next_id {
        Some(Value::String(next)) => Ok(Next::Call {
            call: Call {
                step: state.step + 1,
                api: message_metadata_call(&next, args.account.as_deref(), settings),
            },
            derived,
        }),
        Some(_) => Err(internal(
            "in-flight row lists a message id that is not a string",
        )),
        None => finish(search_answer(derived), args.account.as_deref(), settings),
    }
}

/// The array `derived[key]` that an earlier step of this request wrote.
fn derived_list(derived: &Args, key: &str) -> Result<Vec<Value>, Failure> {
    match derived.get(key) {
        Some(Value::Array(items)) => Ok(items.clone()),
        _ => Err(internal(format!("in-flight row carries no `{key}` list"))),
    }
}

/// `GmailSearch`'s answer from what its steps gathered in `derived`:
/// `messages` (empty when nothing matched), Google's `next_page` and
/// `result_size_estimate` when it gave them, and `unavailable` when a listed
/// message could not be read.
fn search_answer(derived: Args) -> Value {
    let mut out = Args::new();
    out.insert(
        "messages".to_string(),
        derived.get("lines").cloned().unwrap_or_else(|| json!([])),
    );
    copy(&mut out, "next_page", derived.get("next_page"));
    copy(
        &mut out,
        "result_size_estimate",
        derived.get("result_size_estimate"),
    );
    if let Some(Value::Array(gone)) = derived.get("unavailable")
        && !gone.is_empty()
    {
        out.insert("unavailable".to_string(), Value::Array(gone.clone()));
    }
    Value::Object(out)
}

/// One message's metadata as a search line: `id`, `thread_id`, the `from`,
/// `subject` and `date` header values when present, each through
/// `header_value` with the cut fields named, sorted, in `headers_truncated`,
/// `internal_date` as RFC 3339, the snippet through `snippet_value`, and the
/// label ids as `labels`. A body for another message than `id`, or a thread
/// id that is not a Gmail id, is Google's fault.
fn search_line(body: &Value, id: &str) -> Result<Value, Failure> {
    let api = "messages.get";
    if body.get("id").and_then(Value::as_str) != Some(id) {
        return Err(shape(api));
    }
    let mut out = Args::new();
    out.insert("id".to_string(), json!(id));
    match body.get("threadId") {
        None => {}
        Some(Value::String(thread)) if is_gmail_id(thread) => {
            out.insert("thread_id".to_string(), json!(thread));
        }
        Some(_) => return Err(shape(api)),
    }
    let mut truncated = Vec::new();
    for name in SEARCH_HEADERS {
        if let Some(header) = header_value(body, name, api)? {
            let field = name.to_ascii_lowercase();
            if header.cut {
                truncated.push(field.clone());
            }
            out.insert(field, json!(header.value));
        }
    }
    if !truncated.is_empty() {
        truncated.sort();
        out.insert("headers_truncated".to_string(), json!(truncated));
    }
    match body.get("internalDate") {
        None => {}
        Some(Value::String(ms)) => {
            let date = decode::internal_date(ms).ok_or_else(|| shape(api))?;
            out.insert("internal_date".to_string(), json!(date));
        }
        Some(_) => return Err(shape(api)),
    }
    if let Some(snippet) = snippet_value(body, api)? {
        out.insert("snippet".to_string(), json!(snippet));
    }
    out.insert("labels".to_string(), label_ids(body, api)?);
    Ok(Value::Object(out))
}

/// A message's `labelIds` as an array of Gmail ids; absent is none.
fn label_ids(message: &Value, api: &str) -> Result<Value, Failure> {
    match message.get("labelIds") {
        None => Ok(json!([])),
        Some(Value::Array(ids)) if ids.iter().all(|id| id.as_str().is_some_and(is_gmail_id)) => {
            Ok(Value::Array(ids.clone()))
        }
        Some(_) => Err(shape(api)),
    }
}

/// A finished answer completes, unless one of its `labels` arrays carries a
/// user-label id: then labels.list is issued to name it, the answer waiting
/// in `derived`.
fn finish(answer: Value, account: Option<&str>, settings: &Settings) -> Result<Next, Failure> {
    if !needs_label_names(&answer) {
        return Ok(Next::Complete(answer));
    }
    Ok(Next::Call {
        call: Call {
            step: LABELS_STEP,
            api: label_names_call(account, settings),
        },
        derived: Args::from_iter([("answer".to_string(), answer)]),
    })
}

/// Whether any `labels` array of strings in `answer` holds a user-label id.
fn needs_label_names(answer: &Value) -> bool {
    match answer {
        Value::Object(map) => map.iter().any(|(key, value)| match value {
            Value::Array(items) if key == "labels" => items.iter().any(|item| {
                item.as_str()
                    .is_some_and(|id| id.starts_with(USER_LABEL_PREFIX))
            }),
            other => needs_label_names(other),
        }),
        Value::Array(items) => items.iter().any(needs_label_names),
        _ => false,
    }
}

/// Every label id in every `labels` array of strings in `answer` replaced by
/// its name; an id `names` lacks stays an id.
fn name_labels(answer: &mut Value, names: &BTreeMap<String, String>) {
    match answer {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                match value {
                    Value::Array(items) if key == "labels" => {
                        for item in items {
                            if let Some(name) = item.as_str().and_then(|id| names.get(id)) {
                                *item = json!(name);
                            }
                        }
                    }
                    other => name_labels(other, names),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                name_labels(item, names);
            }
        }
        _ => {}
    }
}

/// The finishing step: labels.list's names applied to the answer that waited
/// for them.
fn named(state: &InFlight, body: &Value) -> Result<Next, Failure> {
    let Some(answer) = state.derived.get("answer") else {
        return Err(internal(
            "in-flight row at the label-names step carries no answer",
        ));
    };
    let Value::Object(body) = body else {
        return Err(shape("labels.list"));
    };
    let mut names = BTreeMap::new();
    match body.get("labels") {
        None => {}
        Some(Value::Array(items)) => {
            for item in items {
                let Some(id) = item.get("id").and_then(Value::as_str) else {
                    return Err(shape("labels.list"));
                };
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    names.insert(id.to_string(), name.to_string());
                }
            }
        }
        Some(_) => return Err(shape("labels.list")),
    }
    let mut answer = answer.clone();
    name_labels(&mut answer, &names);
    Ok(Next::Complete(answer))
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

    fn fields(mask: &str) -> Query {
        query([("fields", mask.to_string())])
    }

    /// `advance` under the test settings.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        advance(state, outcome, &settings())
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

    fn refused(status: u64, kind: ErrorKind, detail: &str) -> Result<Answer, Failure> {
        Ok(Answer::GoogleError {
            status,
            failure: failure(kind, detail),
        })
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
            refused(
                400,
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Invalid pageToken"
            )
        );
        assert_eq!(
            read(http(
                404,
                &google(404, "NOT_FOUND", "Requested entity was not found.")
            )),
            refused(
                404,
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Requested entity was not found."
            )
        );
        for (code, status) in [
            (401, "UNAUTHENTICATED"),
            (429, "RESOURCE_EXHAUSTED"),
            (500, "INTERNAL"),
            (503, "UNAVAILABLE"),
        ] {
            assert_eq!(
                read(http(code, &google(code, status, "later"))),
                refused(
                    code,
                    ErrorKind::Internal,
                    &format!("Google {status} (HTTP {code}): later")
                )
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
            refused(
                403,
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail read scope: run oauth-connect with that scope set \
                 and relay its steps."
            )
        );
        assert_eq!(
            read(http(403, "<html>SECRET</html>")),
            refused(
                403,
                ErrorKind::Denied,
                &format!("Google HTTP 403, body not in the standard error shape. {SCOPE_HINT}")
            )
        );
    }

    #[test]
    fn a_long_google_message_is_cut() {
        let message = "x".repeat(GOOGLE_MESSAGE_MAX_BYTES + 50);
        let Ok(Answer::GoogleError { failure, .. }) =
            read(http(500, &google(500, "INTERNAL", &message)))
        else {
            panic!("a 500 is Google's error");
        };
        let detail = failure.detail;
        assert_eq!(
            detail,
            format!(
                "Google INTERNAL (HTTP 500): {}…",
                "x".repeat(GOOGLE_MESSAGE_MAX_BYTES)
            )
        );
    }

    #[test]
    fn an_error_body_not_in_googles_shape_is_not_copied() {
        assert_eq!(
            read(http(400, "{\"error\": \"SECRET\"}")),
            refused(
                400,
                ErrorKind::InvalidArgs,
                "Google HTTP 400, body not in the standard error shape"
            )
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
            answered(&state, http(200, LABELS_BODY)),
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
            answered(&state, http(200, "{}")),
            Next::Complete(json!({ "labels": [] }))
        );
    }

    #[test]
    fn label_get_carries_the_counts_google_gives() {
        let state = labels_state(json!({ "id": "Label_7" }));
        let body = r#"{ "id": "Label_7", "name": "Receipts", "type": "user",
            "messagesTotal": 120, "messagesUnread": 3, "threadsTotal": 98, "threadsUnread": 2 }"#;
        assert_eq!(
            answered(&state, http(200, body)),
            Next::Complete(json!({
                "id": "Label_7", "name": "Receipts", "type": "user",
                "messages_total": 120, "messages_unread": 3,
                "threads_total": 98, "threads_unread": 2,
            }))
        );
        let sparse = r#"{ "id": "INBOX", "name": "INBOX", "type": "system", "messagesUnread": 0 }"#;
        assert_eq!(
            answered(&labels_state(json!({ "id": "INBOX" })), http(200, sparse)),
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
                answered(&list, http(200, body)),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Gmail labels.list response shape"
                )),
                "{body}"
            );
        }
        let one = labels_state(json!({ "id": "INBOX" }));
        assert_eq!(
            answered(&one, http(200, "{\"name\": \"INBOX\"}")),
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
            answered(&labels_state(json!({})), Ok(value.clone())),
            Next::Complete(value)
        );
    }

    #[test]
    fn advance_answers_a_cut_listing_internal() {
        let outcome = Ok(json!({
            "status": 200, "headers": {}, "bytes": null, "truncated": true, "body": "{",
        }));
        assert_eq!(
            answered(&labels_state(json!({})), outcome),
            Next::Fail(cut_answer("of unstated length"))
        );
    }

    #[test]
    fn advance_passes_a_failed_call_through() {
        assert_eq!(
            answered(
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
            answered(&state, http(200, "{}")),
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
            answered(&state, http(200, LABELS_BODY)),
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

    fn search(args: Value) -> Result<ApiCall, String> {
        plan(Provided::GmailSearch, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    #[test]
    fn search_lists_one_page_of_the_query() {
        let call = search(json!({ "q": "from:alice@example.com is:unread" })).unwrap();
        assert_eq!(
            serde_json::to_value(&call).unwrap(),
            json!({
                "provider": "google",
                "method": "GET",
                "host": "gmail.example.com",
                "path": "/gmail/v1/users/me/messages",
                "query": {
                    "q": "from:alice@example.com is:unread",
                    "maxResults": "10",
                    "fields": "messages(id,threadId),nextPageToken,resultSizeEstimate",
                },
            })
        );
    }

    #[test]
    fn search_passes_page_spam_trash_max_and_account() {
        let call = search(json!({
            "q": "invoice", "max": 25, "page": "token-2", "spam_trash": true,
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.query["maxResults"], QueryValue::One("25".to_string()));
        assert_eq!(
            call.query["pageToken"],
            QueryValue::One("token-2".to_string())
        );
        assert_eq!(
            call.query["includeSpamTrash"],
            QueryValue::One("true".to_string())
        );
        let call = search(json!({ "q": "invoice", "max": 1, "spam_trash": false })).unwrap();
        assert_eq!(call.query["maxResults"], QueryValue::One("1".to_string()));
        assert!(!call.query.contains_key("includeSpamTrash"));
        assert!(!call.query.contains_key("pageToken"));
    }

    #[test]
    fn search_refuses_bad_arguments() {
        for (args, error) in [
            (json!({}), "`q` is required"),
            (json!({ "q": " " }), "`q` is empty"),
            (json!({ "q": 1 }), "`q` must be a string"),
            (
                json!({ "q": "x".repeat(1025) }),
                "`q` is longer than 1024 bytes",
            ),
            (
                json!({ "q": "x", "max": 0 }),
                "`max` must be an integer from 1 to 25",
            ),
            (
                json!({ "q": "x", "max": 26 }),
                "`max` must be an integer from 1 to 25",
            ),
            (
                json!({ "q": "x", "max": "5" }),
                "`max` must be an integer from 1 to 25",
            ),
            (
                json!({ "q": "x", "page": "p".repeat(4097) }),
                "`page` is longer than 4096 bytes",
            ),
            (
                json!({ "q": "x", "spam_trash": "yes" }),
                "`spam_trash` must be true or false",
            ),
            (
                json!({ "q": "x", "limit": 5 }),
                "unknown argument `limit`; this tool takes `q`, `max`, `page`, `spam_trash`, \
                 `account`",
            ),
        ] {
            assert_eq!(search(args.clone()).unwrap_err(), error, "{args}");
        }
        assert!(search(json!({ "q": "x".repeat(1024) })).is_ok());
    }

    #[test]
    fn a_repeated_query_key_is_sent_as_an_array() {
        let call = message_metadata_call("m1", None, &settings());
        assert_eq!(
            serde_json::to_value(&call).unwrap(),
            json!({
                "provider": "google",
                "method": "GET",
                "host": "gmail.example.com",
                "path": "/gmail/v1/users/me/messages/m1",
                "query": {
                    "format": "metadata",
                    "metadataHeaders": ["From", "Subject", "Date"],
                    "fields": "id,threadId,labelIds,snippet,internalDate,sizeEstimate,\
                               payload/headers",
                },
            })
        );
    }

    fn search_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailSearch, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::GmailSearch,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    /// The row the next step is stored under, read back as the next
    /// activation reads it, and the call it issues.
    fn follow(state: &InFlight, next: Next) -> (InFlight, Call) {
        let Next::Call { call, derived } = next else {
            panic!("expected a next step, got {next:?}");
        };
        let row = InFlight::decode(&state.next(&call, derived, 2_000).encode()).unwrap();
        assert_eq!(row.step, call.step);
        assert_eq!(row.target, call.api.target());
        assert_eq!(row.issued_at_ms, 2_000);
        (row, call)
    }

    fn listing(ids: &[&str], extra: Value) -> String {
        let mut body = json!({
            "messages": ids.iter().map(|id| json!({ "id": id, "threadId": "t1" })).collect::<Vec<_>>(),
        });
        for (key, value) in extra.as_object().unwrap() {
            body[key] = value.clone();
        }
        body.to_string()
    }

    fn metadata(id: &str, labels: &[&str]) -> String {
        json!({
            "id": id,
            "threadId": format!("t-{id}"),
            "labelIds": labels,
            "snippet": "It&#39;s at noon &amp; on &quot;us&quot;",
            "internalDate": "1793455509123",
            "sizeEstimate": 2048,
            "payload": { "headers": [
                { "name": "From", "value": "Alice <alice@example.com>" },
                { "name": "subject", "value": "Lunch" },
                { "name": "Date", "value": "Sat, 31 Oct 2026 14:05:09 +0000" },
            ] },
        })
        .to_string()
    }

    fn line(id: &str, labels: Value) -> Value {
        json!({
            "id": id,
            "thread_id": format!("t-{id}"),
            "from": "Alice <alice@example.com>",
            "subject": "Lunch",
            "date": "Sat, 31 Oct 2026 14:05:09 +0000",
            "internal_date": "2026-10-31T14:05:09Z",
            "snippet": "It's at noon & on \"us\"",
            "labels": labels,
        })
    }

    #[test]
    fn search_reads_each_listed_message_then_names_user_labels() {
        let args = json!({ "q": "lunch", "max": 3, "account": "alice@example.com" });
        let state = search_state(args);
        let list = listing(
            &["m1", "m2", "m3"],
            json!({ "nextPageToken": "next-1", "resultSizeEstimate": 40 }),
        );
        let (state, call) = follow(&state, answered(&state, http(200, &list)));
        assert_eq!(call.step, 2);
        assert_eq!(
            call.api,
            message_metadata_call("m1", Some("alice@example.com"), &settings())
        );
        let (state, call) = follow(
            &state,
            answered(&state, http(200, &metadata("m1", &["INBOX", "UNREAD"]))),
        );
        assert_eq!(
            (call.step, call.api.path.as_str()),
            (3, "/gmail/v1/users/me/messages/m2")
        );
        let (state, call) = follow(
            &state,
            answered(&state, http(200, &metadata("m2", &["Label_7", "INBOX"]))),
        );
        assert_eq!(
            (call.step, call.api.path.as_str()),
            (4, "/gmail/v1/users/me/messages/m3")
        );
        let (state, call) = follow(&state, answered(&state, http(200, &metadata("m3", &[]))));
        assert_eq!(call.step, 100);
        assert_eq!(
            call.api,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "GET".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/labels".to_string(),
                query: fields("labels(id,name)"),
            }
        );
        let names = r#"{ "labels": [
            { "id": "INBOX", "name": "INBOX" },
            { "id": "Label_7", "name": "Receipts" }
        ] }"#;
        assert_eq!(
            answered(&state, http(200, names)),
            Next::Complete(json!({
                "messages": [
                    line("m1", json!(["INBOX", "UNREAD"])),
                    line("m2", json!(["Receipts", "INBOX"])),
                    line("m3", json!([])),
                ],
                "next_page": "next-1",
                "result_size_estimate": 40,
            }))
        );
    }

    #[test]
    fn a_search_with_system_labels_only_completes_without_naming_them() {
        let state = search_state(json!({ "q": "lunch" }));
        let list = listing(&["m1"], json!({ "resultSizeEstimate": 1 }));
        let (state, _) = follow(&state, answered(&state, http(200, &list)));
        assert_eq!(
            answered(
                &state,
                http(200, &metadata("m1", &["INBOX", "CATEGORY_UPDATES"]))
            ),
            Next::Complete(json!({
                "messages": [line("m1", json!(["INBOX", "CATEGORY_UPDATES"]))],
                "result_size_estimate": 1,
            }))
        );
    }

    #[test]
    fn an_empty_search_completes_after_one_call() {
        let state = search_state(json!({ "q": "nothing" }));
        assert_eq!(
            answered(&state, http(200, r#"{ "resultSizeEstimate": 0 }"#)),
            Next::Complete(json!({ "messages": [], "result_size_estimate": 0 }))
        );
        assert_eq!(
            answered(&state, http(200, r#"{ "messages": [] }"#)),
            Next::Complete(json!({ "messages": [] }))
        );
    }

    #[test]
    fn a_message_deleted_mid_search_is_listed_unavailable() {
        let state = search_state(json!({ "q": "lunch" }));
        let list = listing(&["m1", "m2", "m3"], json!({ "resultSizeEstimate": 3 }));
        let (state, _) = follow(&state, answered(&state, http(200, &list)));
        let gone = google(404, "NOT_FOUND", "Requested entity was not found.");
        let (state, call) = follow(&state, answered(&state, http(404, &gone)));
        assert_eq!(call.api.path, "/gmail/v1/users/me/messages/m2");
        let (state, _) = follow(&state, answered(&state, http(200, &metadata("m2", &[]))));
        assert_eq!(
            answered(&state, http(404, &gone)),
            Next::Complete(json!({
                "messages": [line("m2", json!([]))],
                "result_size_estimate": 3,
                "unavailable": ["m1", "m3"],
            }))
        );
    }

    #[test]
    fn a_404_mid_search_is_read_by_the_one_envelope_reader() {
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1"], json!({})))),
        );
        // An envelope without `truncated` is read as whole, as every step
        // reads it.
        let untold = Ok(json!({ "status": 404, "headers": {}, "bytes": 2, "body": "{}" }));
        assert_eq!(
            answered(&state, untold),
            Next::Complete(json!({ "messages": [], "unavailable": ["m1"] }))
        );
        let binary = Ok(json!({
            "status": 404, "headers": {}, "bytes": 3, "truncated": false, "body_base64": "AAEC",
        }));
        assert_eq!(
            answered(&state, binary),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google returned a non-text body (404)"
            ))
        );
    }

    #[test]
    fn a_search_reads_all_25_messages_max_allows() {
        let ids: Vec<String> = (1..=25).map(|i| format!("m{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let list = listing(&id_refs, json!({}));
        let gone = google(404, "NOT_FOUND", "Requested entity was not found.");
        // The 25th message read, then found deleted, at step 26.
        for (last, read, unavailable) in [
            (http(200, &metadata("m25", &[])), 25, None),
            (http(404, &gone), 24, Some(json!(["m25"]))),
        ] {
            let state = search_state(json!({ "q": "lunch", "max": 25 }));
            let (mut state, mut call) = follow(&state, answered(&state, http(200, &list)));
            for (i, id) in ids[..24].iter().enumerate() {
                assert_eq!(
                    (call.step, call.api.path.as_str()),
                    (
                        2 + i as u8,
                        format!("/gmail/v1/users/me/messages/{id}").as_str()
                    )
                );
                (state, call) = follow(&state, answered(&state, http(200, &metadata(id, &[]))));
            }
            assert_eq!(
                (call.step, call.api.path.as_str()),
                (26, "/gmail/v1/users/me/messages/m25")
            );
            let Next::Complete(answer) = answered(&state, last) else {
                panic!("the search did not complete at its 25th message");
            };
            assert_eq!(answer["messages"].as_array().unwrap().len(), read);
            assert_eq!(answer.get("unavailable").cloned(), unavailable);
        }
    }

    #[test]
    fn a_long_header_value_is_cut() {
        let with_subject = |id: &str, subject: String| {
            let mut body: Value = serde_json::from_str(&metadata(id, &[])).unwrap();
            body["payload"]["headers"][1]["value"] = json!(subject);
            http(200, &body.to_string())
        };
        // A sender's own trailing `…` at exactly the bound is not a cut: one
        // byte, 498 two-byte `é` and the three-byte `…` are 1 000 bytes.
        let at_bound = format!("a{}…", "é".repeat(498));
        assert_eq!(at_bound.len(), HEADER_MAX_BYTES);
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1", "m2", "m3"], json!({})))),
        );
        let (state, _) = follow(
            &state,
            answered(
                &state,
                with_subject("m1", "é".repeat(HEADER_MAX_BYTES / 2 + 1)),
            ),
        );
        let (state, _) = follow(
            &state,
            answered(&state, with_subject("m2", at_bound.clone())),
        );
        let Next::Complete(answer) = answered(&state, http(200, &metadata("m3", &[]))) else {
            panic!("the search did not complete");
        };
        let lines = answer["messages"].as_array().unwrap();
        assert_eq!(
            lines[0]["subject"],
            json!(format!("{}…", "é".repeat(HEADER_MAX_BYTES / 2)))
        );
        assert_eq!(lines[0]["from"], json!("Alice <alice@example.com>"));
        assert_eq!(lines[0]["headers_truncated"], json!(["subject"]));
        assert_eq!(lines[1]["subject"], json!(at_bound));
        assert_eq!(lines[1].get("headers_truncated"), None);
        assert_eq!(lines[2].get("headers_truncated"), None);
    }

    #[test]
    fn every_cut_header_on_a_line_is_flagged_in_sorted_order() {
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1"], json!({})))),
        );
        let mut body: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        for header in 0..3 {
            body["payload"]["headers"][header]["value"] = json!("x".repeat(HEADER_MAX_BYTES + 1));
        }
        let Next::Complete(answer) = answered(&state, http(200, &body.to_string())) else {
            panic!("the search did not complete");
        };
        assert_eq!(
            answer["messages"][0]["headers_truncated"],
            json!(["date", "from", "subject"])
        );
    }

    #[test]
    fn header_value_reports_a_cut_only_past_the_bound() {
        let message = |value: String| json!({ "payload": { "headers": [{ "name": "Subject", "value": value }] } });
        let at = message("a".repeat(HEADER_MAX_BYTES));
        assert_eq!(
            header_value(&at, "subject", "messages.get"),
            Ok(Some(HeaderValue {
                value: "a".repeat(HEADER_MAX_BYTES),
                cut: false
            }))
        );
        let past = message("a".repeat(HEADER_MAX_BYTES + 1));
        assert_eq!(
            header_value(&past, "Subject", "messages.get"),
            Ok(Some(HeaderValue {
                value: format!("{}…", "a".repeat(HEADER_MAX_BYTES)),
                cut: true
            }))
        );
        assert_eq!(header_value(&past, "From", "messages.get"), Ok(None));
    }

    #[test]
    fn plain_replaces_c0_controls_other_than_tab_newline_and_return() {
        assert_eq!(plain("a\u{0}b\u{1b}c\u{1f}d"), "a b c d");
        assert_eq!(plain("a\tb\nc\rd"), "a\tb\nc\rd");
        assert_eq!(plain("a\u{7f}é😀"), "a\u{7f}é😀");
    }

    #[test]
    fn cut_bounds_utf8_bytes_on_a_char_boundary() {
        assert_eq!(cut(&"a".repeat(1000), 1000), ("a".repeat(1000), false));
        assert_eq!(
            cut(&"a".repeat(1001), 1000),
            (format!("{}…", "a".repeat(1000)), true)
        );
        assert_eq!(cut(&"é".repeat(500), 1000), ("é".repeat(500), false));
        // A four-byte character straddling the bound is dropped whole.
        let straddling = format!("{}😀", "a".repeat(998));
        assert_eq!(
            cut(&straddling, 1000),
            (format!("{}…", "a".repeat(998)), true)
        );
        assert_eq!(cut("", 0), (String::new(), false));
    }

    #[test]
    fn a_header_of_control_characters_answers_spaces_unflagged() {
        let message = json!({ "payload": { "headers": [
            { "name": "Subject", "value": "\u{1}".repeat(HEADER_MAX_BYTES) }
        ] } });
        assert_eq!(
            header_value(&message, "Subject", "messages.get"),
            Ok(Some(HeaderValue {
                value: " ".repeat(HEADER_MAX_BYTES),
                cut: false
            }))
        );
    }

    #[test]
    fn a_long_snippet_is_cut_unflagged() {
        let at = json!({ "snippet": "a".repeat(SNIPPET_MAX_BYTES) });
        assert_eq!(
            snippet_value(&at, "messages.get"),
            Ok(Some("a".repeat(SNIPPET_MAX_BYTES)))
        );
        // Measured after the references are decoded and controls replaced.
        let past = json!({ "snippet": format!("{}&amp;\u{2}", "a".repeat(SNIPPET_MAX_BYTES - 1)) });
        assert_eq!(
            snippet_value(&past, "messages.get"),
            Ok(Some(format!("{}&…", "a".repeat(SNIPPET_MAX_BYTES - 1))))
        );
        assert_eq!(snippet_value(&json!({}), "messages.get"), Ok(None));
    }

    #[test]
    fn the_widest_search_page_fits_the_ceiling() {
        // 128-byte ids, the longest a Gmail id may be.
        let id = |prefix: char, i: usize| format!("{prefix}{i:0>127}");
        let ids: Vec<String> = (0..25).map(|i| id('m', i)).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let list = listing(
            &id_refs,
            json!({ "nextPageToken": "\"".repeat(PAGE_MAX_BYTES), "resultSizeEstimate": u64::MAX }),
        );
        // Eight user labels per message, named so that each line's `labels`
        // serializes to 1 715 bytes, the most the page's size arithmetic allows.
        let label_ids: Vec<String> = (0..8).map(|i| format!("Label_{i}")).collect();
        let names: Vec<Value> = (0..8)
            .map(|i| json!({ "id": label_ids[i], "name": "n".repeat(if i < 2 { 212 } else { 211 }) }))
            .collect();
        // `"` and `\` serialize to two bytes each, the widest after `plain`.
        let widest = |i: usize| {
            json!({
                "id": ids[i],
                "threadId": id('t', i),
                "labelIds": label_ids,
                "snippet": "\\".repeat(SNIPPET_MAX_BYTES + 1),
                "internalDate": "1793455509123",
                "payload": { "headers": [
                    { "name": "From", "value": "\"".repeat(HEADER_MAX_BYTES + 1) },
                    { "name": "Subject", "value": "\"".repeat(HEADER_MAX_BYTES + 1) },
                    { "name": "Date", "value": "\"".repeat(HEADER_MAX_BYTES + 1) },
                ] },
            })
            .to_string()
        };
        let state = search_state(json!({ "q": "lunch", "max": 25 }));
        let (mut state, _) = follow(&state, answered(&state, http(200, &list)));
        for i in 0..24 {
            (state, _) = follow(&state, answered(&state, http(200, &widest(i))));
        }
        let (state, call) = follow(&state, answered(&state, http(200, &widest(24))));
        assert_eq!(call.step, LABELS_STEP);
        let listing = json!({ "labels": names }).to_string();
        let Next::Complete(answer) = answered(&state, http(200, &listing)) else {
            panic!("the widest page did not complete");
        };
        let lines = answer["messages"].as_array().unwrap();
        assert_eq!(lines.len(), 25);
        assert_eq!(
            lines[24]["headers_truncated"],
            json!(["date", "from", "subject"])
        );
        for line in lines {
            assert_eq!(serde_json::to_vec(&line["labels"]).unwrap().len(), 1715);
        }
        assert!(serde_json::to_vec(&answer).unwrap().len() <= COMPACT_RESULT_CEILING);
    }

    #[test]
    fn a_message_without_payload_or_headers_has_no_header() {
        for message in [json!({}), json!({ "payload": {} })] {
            assert_eq!(header_value(&message, "Subject", "messages.get"), Ok(None));
        }
    }

    #[test]
    fn malformed_headers_fail_the_search() {
        for payload in [
            json!("headers"),
            json!({ "headers": { "Subject": "Lunch" } }),
            json!({ "headers": [{ "name": "Subject", "value": 7 }] }),
            json!({ "headers": [{ "value": "nameless" }] }),
        ] {
            let state = search_state(json!({ "q": "lunch" }));
            let (state, _) = follow(
                &state,
                answered(&state, http(200, &listing(&["m1"], json!({})))),
            );
            let mut body: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
            body["payload"] = payload.clone();
            let Next::Fail(failure) = answered(&state, http(200, &body.to_string())) else {
                panic!("a payload of {payload} did not fail the search");
            };
            assert_eq!(failure.kind, ErrorKind::Internal);
            assert_eq!(
                failure.detail,
                "unexpected Gmail messages.get response shape"
            );
        }
    }

    #[test]
    fn a_404_on_the_listing_is_the_callers() {
        let state = search_state(json!({ "q": "lunch", "page": "stale" }));
        assert_eq!(
            answered(&state, http(404, &google(404, "NOT_FOUND", "gone"))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): gone"
            ))
        );
    }

    #[test]
    fn any_other_failure_mid_search_fails_the_search() {
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1", "m2"], json!({})))),
        );
        assert_eq!(
            answered(&state, http(500, &google(500, "INTERNAL", "later"))),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google INTERNAL (HTTP 500): later"
            ))
        );
        let cut = Ok(json!({
            "status": 404, "headers": {}, "bytes": 900000, "truncated": true, "body": "{",
        }));
        assert_eq!(
            answered(&state, cut),
            Next::Fail(cut_answer("900000 bytes"))
        );
        let value = credential_invalid(json!("alice@example.com"));
        assert_eq!(answered(&state, Ok(value.clone())), Next::Complete(value));
    }

    #[test]
    fn search_refuses_an_unexpected_listing() {
        let state = search_state(json!({ "q": "lunch", "max": 2 }));
        let shape_error = Next::Fail(failure(
            ErrorKind::Internal,
            "unexpected Gmail messages.list response shape",
        ));
        for body in [
            "[]".to_string(),
            r#"{ "messages": {} }"#.to_string(),
            r#"{ "messages": [{ "threadId": "t1" }] }"#.to_string(),
            listing(&["m/1"], json!({})),
            listing(&["m1"], json!({ "nextPageToken": 7 })),
            listing(&["m1"], json!({ "resultSizeEstimate": "many" })),
            listing(
                &["m1"],
                json!({ "nextPageToken": "p".repeat(PAGE_MAX_BYTES + 1) }),
            ),
        ] {
            assert_eq!(answered(&state, http(200, &body)), shape_error, "{body}");
        }
        assert_eq!(
            answered(&state, http(200, &listing(&["m1", "m2", "m3"], json!({})))),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Gmail messages.list answered 3 messages for maxResults 2"
            ))
        );
    }

    #[test]
    fn search_refuses_an_unexpected_message() {
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1"], json!({})))),
        );
        let shape_error = Next::Fail(failure(
            ErrorKind::Internal,
            "unexpected Gmail messages.get response shape",
        ));
        let mut wrong_date: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        wrong_date["internalDate"] = json!("yesterday");
        let mut wrong_labels: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        wrong_labels["labelIds"] = json!([1]);
        let mut wrong_snippet: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        wrong_snippet["snippet"] = json!(null);
        let mut label_not_an_id: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        label_not_an_id["labelIds"] = json!(["INBOX", "Label 7"]);
        let mut thread_not_an_id: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        thread_not_an_id["threadId"] = json!("t/1");
        let mut thread_too_long: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        thread_too_long["threadId"] = json!("t".repeat(ID_MAX_BYTES + 1));
        let mut thread_not_a_string: Value = serde_json::from_str(&metadata("m1", &[])).unwrap();
        thread_not_a_string["threadId"] = json!(7);
        for body in [
            metadata("m2", &[]),
            "{}".to_string(),
            wrong_date.to_string(),
            wrong_labels.to_string(),
            wrong_snippet.to_string(),
            label_not_an_id.to_string(),
            thread_not_an_id.to_string(),
            thread_too_long.to_string(),
            thread_not_a_string.to_string(),
        ] {
            assert_eq!(answered(&state, http(200, &body)), shape_error, "{body}");
        }
    }

    #[test]
    fn a_sparse_message_answers_what_google_gave() {
        let state = search_state(json!({ "q": "lunch" }));
        let (state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1"], json!({})))),
        );
        assert_eq!(
            answered(&state, http(200, r#"{ "id": "m1" }"#)),
            Next::Complete(json!({ "messages": [{ "id": "m1", "labels": [] }] }))
        );
    }

    #[test]
    fn a_search_row_missing_what_earlier_steps_wrote_is_internal() {
        let state = search_state(json!({ "q": "lunch" }));
        let (mut state, _) = follow(
            &state,
            answered(&state, http(200, &listing(&["m1"], json!({})))),
        );
        state.derived.remove("lines");
        assert_eq!(
            answered(&state, http(200, &metadata("m1", &[]))),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row carries no `lines` list"
            ))
        );
        state.step = 3;
        assert_eq!(
            answered(&state, http(200, &metadata("m1", &[]))),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row at gmail-search step 3 lists no message id for it"
            ))
        );
        state.step = 27;
        assert_eq!(
            answered(&state, http(200, "{}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row names step 27 of gmail-search, which has no such step"
            ))
        );
    }

    #[test]
    fn label_names_are_needed_only_for_user_labels() {
        assert!(!needs_label_names(
            &json!({ "labels": ["INBOX", "UNREAD"] })
        ));
        assert!(needs_label_names(
            &json!({ "labels": ["INBOX", "Label_3"] })
        ));
        assert!(needs_label_names(
            &json!({ "messages": [{ "labels": [] }, { "labels": ["Label_3"] }] })
        ));
        // Only a `labels` array names labels.
        assert!(!needs_label_names(
            &json!({ "subject": "Label_3", "ids": ["Label_3"] })
        ));
    }

    #[test]
    fn naming_rewrites_every_labels_array_and_keeps_unknown_ids() {
        let mut answer = json!({
            "labels": ["Label_1", "INBOX"],
            "messages": [{ "labels": ["Label_2", "Label_9"], "subject": "Label_1" }],
        });
        let names = BTreeMap::from([
            ("Label_1".to_string(), "Receipts".to_string()),
            ("Label_2".to_string(), "Travel/2026".to_string()),
        ]);
        name_labels(&mut answer, &names);
        assert_eq!(
            answer,
            json!({
                "labels": ["Receipts", "INBOX"],
                "messages": [{ "labels": ["Travel/2026", "Label_9"], "subject": "Label_1" }],
            })
        );
    }

    fn naming_state(derived: Args) -> InFlight {
        let state = search_state(json!({ "q": "lunch" }));
        let call = Call {
            step: LABELS_STEP,
            api: label_names_call(None, &settings()),
        };
        state.next(&call, derived, 2_000)
    }

    #[test]
    fn the_naming_step_reads_labels_list() {
        let waiting = json!({ "messages": [{ "id": "m1", "labels": ["Label_7", "Label_8"] }] });
        let state = naming_state(Args::from_iter([("answer".to_string(), waiting)]));
        assert_eq!(
            answered(
                &state,
                http(
                    200,
                    r#"{ "labels": [{ "id": "Label_7", "name": "Receipts" }, { "id": "Label_8" }] }"#
                )
            ),
            Next::Complete(
                json!({ "messages": [{ "id": "m1", "labels": ["Receipts", "Label_8"] }] })
            )
        );
        assert_eq!(
            answered(&state, http(200, "{}")),
            Next::Complete(
                json!({ "messages": [{ "id": "m1", "labels": ["Label_7", "Label_8"] }] })
            )
        );
        for body in [
            "[]",
            r#"{ "labels": {} }"#,
            r#"{ "labels": [{ "name": "x" }] }"#,
        ] {
            assert_eq!(
                answered(&state, http(200, body)),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Gmail labels.list response shape"
                )),
                "{body}"
            );
        }
        assert_eq!(
            answered(&naming_state(Args::new()), http(200, "{}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row at the label-names step carries no answer"
            ))
        );
    }
}
