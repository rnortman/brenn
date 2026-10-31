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

/// The most UTF-8 bytes of a recipient header's value (`To`, `Cc`, `Bcc`,
/// `Reply-To`) copied into a result: about 110 recipients written as
/// `Alice Example <alice@example.com>, `.
const RECIPIENT_HEADER_MAX_BYTES: usize = 4000;
/// The headers bounded at `RECIPIENT_HEADER_MAX_BYTES`, matched
/// case-insensitively; every other header is bounded at `HEADER_MAX_BYTES`.
const RECIPIENT_HEADERS: &[&str] = &["To", "Cc", "Bcc", "Reply-To"];

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

/// The headers `GmailRead` answers, each under its snake-case name.
const READ_HEADERS: &[&str] = &[
    "From",
    "To",
    "Cc",
    "Bcc",
    "Reply-To",
    "Subject",
    "Date",
    "Message-ID",
    "In-Reply-To",
];
/// `GmailRead`'s `max_chars`: the range and the default.
const MAX_CHARS_MIN: u64 = 200;
const MAX_CHARS_MAX: u64 = 60_000;
const MAX_CHARS_DEFAULT: u64 = 12_000;
/// `GmailRead`'s second step: the message's structure, read when the full
/// message was cut at the provider's read limit.
const STRUCTURE_STEP: u8 = 2;
/// messages.get's structure mask: everything but `data`, the tree listed to
/// the payload and three levels of parts below it.
const STRUCTURE_FIELDS: &str = "id,threadId,labelIds,snippet,internalDate,sizeEstimate,\
                                payload(mimeType,filename,headers,body(size,attachmentId),\
                                parts(mimeType,filename,headers,body(size,attachmentId),\
                                parts(mimeType,filename,headers,body(size,attachmentId),\
                                parts(mimeType,filename,headers,body(size,attachmentId)))))";

/// The path Gmail reads one thread below.
const THREADS_PATH: &str = "/gmail/v1/users/me/threads";
/// threads.get's metadata mask: each message's id, labels, snippet, receipt
/// time and the requested headers.
const THREAD_FIELDS: &str = "id,messages(id,labelIds,snippet,internalDate,payload/headers)";
/// The header a thread answers once, from its first message; a message of
/// the thread answers it only where it differs.
const THREAD_SUBJECT: &str = "Subject";
/// The headers a thread's messages carry, requested by repeating
/// `metadataHeaders`.
const THREAD_HEADERS: &[&str] = &["From", "To", "Cc", THREAD_SUBJECT, "Date"];
/// `GmailThread`'s `max`: the range and the default.
const THREAD_MAX_MIN: u64 = 1;
const THREAD_MAX_MAX: u64 = 100;
const THREAD_MAX_DEFAULT: u64 = 30;

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

/// A `format=metadata` read's query: the `fields` mask, and `headers`
/// requested by repeating `metadataHeaders`.
fn metadata_query(fields: &str, headers: &[&str]) -> Query {
    let mut query = query([
        ("format", "metadata".to_string()),
        ("fields", fields.to_string()),
    ]);
    query.insert(
        "metadataHeaders".to_string(),
        QueryValue::Many(headers.iter().map(|h| h.to_string()).collect()),
    );
    query
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
    /// A GET on the Gmail host for `account`. Every Gmail read adds
    /// `prettyPrint=false` to `query`: Google's indentation would count
    /// against the read limit and the bus cap like any other byte.
    fn gmail_get(
        settings: &Settings,
        account: Option<&str>,
        path: String,
        mut query: Query,
    ) -> ApiCall {
        query.insert(
            "prettyPrint".to_string(),
            QueryValue::One("false".to_string()),
        );
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
        Provided::GmailRead => {
            let read = ReadArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: read_call(&read, None, settings),
            })
        }
        Provided::GmailThread => {
            let thread = ThreadArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: thread_call(&thread, settings),
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
    ApiCall::gmail_get(
        settings,
        account,
        format!("{MESSAGES_PATH}/{id}"),
        metadata_query(METADATA_FIELDS, SEARCH_HEADERS),
    )
}

/// `GmailRead`'s arguments.
struct ReadArgs {
    id: String,
    max_chars: usize,
    account: Option<String>,
}

impl ReadArgs {
    // TODO(provider-schema-parse-parity): these bounds and the `gmail-read`
    // `input_schema` in the spec are stated separately.
    fn parse(args: &Value) -> Result<ReadArgs, String> {
        let map = object(args, &["id", "max_chars", "account"])?;
        let max_chars =
            integer(map, "max_chars", MAX_CHARS_MIN, MAX_CHARS_MAX)?.unwrap_or(MAX_CHARS_DEFAULT);
        Ok(ReadArgs {
            id: gmail_id(map, "id")?.ok_or("`id` is required")?,
            max_chars: usize::try_from(max_chars).expect("max_chars is at most 60 000"),
            account: account(map)?,
        })
    }
}

/// messages.get of the whole message, or, with `fields`, of the parts of it
/// that mask names.
fn read_call(args: &ReadArgs, fields: Option<&str>, settings: &Settings) -> ApiCall {
    let mut query = query([("format", "full".to_string())]);
    if let Some(fields) = fields {
        query.insert("fields".to_string(), QueryValue::One(fields.to_string()));
    }
    ApiCall::gmail_get(
        settings,
        args.account.as_deref(),
        format!("{MESSAGES_PATH}/{}", args.id),
        query,
    )
}

/// `GmailThread`'s arguments.
struct ThreadArgs {
    id: String,
    max: usize,
    account: Option<String>,
}

impl ThreadArgs {
    // TODO(provider-schema-parse-parity): these bounds and the `gmail-thread`
    // `input_schema` in the spec are stated separately.
    fn parse(args: &Value) -> Result<ThreadArgs, String> {
        let map = object(args, &["id", "max", "account"])?;
        let max =
            integer(map, "max", THREAD_MAX_MIN, THREAD_MAX_MAX)?.unwrap_or(THREAD_MAX_DEFAULT);
        Ok(ThreadArgs {
            id: gmail_id(map, "id")?.ok_or("`id` is required")?,
            max: usize::try_from(max).expect("max is at most 100"),
            account: account(map)?,
        })
    }
}

/// threads.get of one thread's messages' metadata: headers, labels and
/// snippets, no bodies.
fn thread_call(args: &ThreadArgs, settings: &Settings) -> ApiCall {
    ApiCall::gmail_get(
        settings,
        args.account.as_deref(),
        format!("{THREADS_PATH}/{}", args.id),
        metadata_query(THREAD_FIELDS, THREAD_HEADERS),
    )
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
/// passed through `plain` and cut to its bound: `RECIPIENT_HEADER_MAX_BYTES`
/// for a recipient header, `HEADER_MAX_BYTES` for any other. `None` when the
/// message has no such header, or no `payload` or `headers` at all, as Google
/// omits them when no header matches the mask. A `payload` that is not an
/// object or a header list not of Gmail's shape is Google's fault, reported
/// against `api`.
fn header_value(message: &Value, name: &str, api: &str) -> Result<Option<HeaderValue>, Failure> {
    let found = raw_header(message, name, api)?;
    let bound = if RECIPIENT_HEADERS
        .iter()
        .any(|recipient| recipient.eq_ignore_ascii_case(name))
    {
        RECIPIENT_HEADER_MAX_BYTES
    } else {
        HEADER_MAX_BYTES
    };
    Ok(found.map(|value| {
        let (value, cut) = cut(&plain(value), bound);
        HeaderValue { value, cut }
    }))
}

/// The value of the header `name` in `message`'s `payload.headers` list as
/// Google sent it, for comparison only: never copied into an answer. Absent
/// and malformed as `header_value` reads them.
fn raw_header<'a>(message: &'a Value, name: &str, api: &str) -> Result<Option<&'a str>, Failure> {
    let headers = match message.get("payload") {
        None => return Ok(None),
        Some(Value::Object(payload)) => match payload.get("headers") {
            None => return Ok(None),
            Some(headers) => headers,
        },
        Some(_) => return Err(shape(api)),
    };
    decode::header(headers, name).map_err(|decode::MalformedHeaders| shape(api))
}

/// `message`'s `names` headers into `out`, each present one under its
/// snake-case name (`Reply-To` as `reply_to`) through `header_value`, and the
/// fields cut, sorted, under `headers_truncated` when any was.
fn answer_headers(
    out: &mut Args,
    message: &Value,
    names: &[&str],
    api: &str,
) -> Result<(), Failure> {
    let mut truncated = Vec::new();
    for name in names {
        if let Some(header) = header_value(message, name, api)? {
            let field = name.to_ascii_lowercase().replace('-', "_");
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
    Ok(())
}

/// `message_fields` of the message asked for by `id`; a message whose id is
/// another is Google's fault.
fn asked_message_fields(
    body: &Value,
    id: &str,
    headers: &[&str],
    api: &str,
) -> Result<Args, Failure> {
    if body.get("id").and_then(Value::as_str) != Some(id) {
        return Err(shape(api));
    }
    message_fields(body, headers, api)
}

/// What every message-bearing answer carries of one message: `id`,
/// `thread_id` when Google gave it, the `headers` through `answer_headers`,
/// `internal_date` as RFC 3339 and `labels` as label ids. An id, thread id
/// or label id that is not a Gmail id is Google's fault.
fn message_fields(body: &Value, headers: &[&str], api: &str) -> Result<Args, Failure> {
    let id = match body.get("id") {
        Some(Value::String(id)) if is_gmail_id(id) => id,
        _ => return Err(shape(api)),
    };
    let mut out = Args::new();
    out.insert("id".to_string(), json!(id));
    match body.get("threadId") {
        None => {}
        Some(Value::String(thread)) if is_gmail_id(thread) => {
            out.insert("thread_id".to_string(), json!(thread));
        }
        Some(_) => return Err(shape(api)),
    }
    answer_headers(&mut out, body, headers, api)?;
    received(&mut out, body, api)?;
    out.insert("labels".to_string(), label_ids(body, api)?);
    Ok(out)
}

/// `message`'s `internalDate` into `out` as `internal_date`, RFC 3339, when
/// Google gave it; one that is not epoch milliseconds is Google's fault.
fn received(out: &mut Args, message: &Value, api: &str) -> Result<(), Failure> {
    match message.get("internalDate") {
        None => {}
        Some(Value::String(ms)) => {
            let date = decode::internal_date(ms).ok_or_else(|| shape(api))?;
            out.insert("internal_date".to_string(), json!(date));
        }
        Some(_) => return Err(shape(api)),
    }
    Ok(())
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
        // A message too large to read whole is read again without its body.
        Ok(Answer::Truncated(size))
            if matches!((state.tool, state.step), (Provided::GmailRead, 1)) =>
        {
            return settle(state.tool, read_cut(state, &size, settings));
        }
        // TODO(errhandling-cut-thread-read-has-no-fallback-and-max-cannot-help):
        // a cut thread read fails whatever `max` is; it has no narrower re-read.
        // TODO(errhandling-refused-search-message-read-fails-the-page): a
        // search's per-message read cut at the read limit fails the page here.
        Ok(Answer::Truncated(size)) => return Next::Fail(cut_answer(&size)),
        // A message deleted between the listing and its read is skipped, not
        // the search's failure.
        Ok(Answer::GoogleError { status: 404, .. })
            if matches!(
                (state.tool, state.step),
                (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET)
            ) =>
        {
            return settle(state.tool, search_message(state, None, settings));
        }
        Ok(Answer::GoogleError { failure, .. }) => return Next::Fail(failure),
        // TODO(errhandling-refused-search-message-read-fails-the-page): a
        // search's per-message read refused on the bus cap fails the page here.
        Err(failure) => return Next::Fail(failure),
    };
    settle(
        state.tool,
        match (state.tool, state.step) {
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
            (Provided::GmailRead, 1) => read_full(state, &body, settings),
            (Provided::GmailRead, STRUCTURE_STEP) => read_structure(state, &body, settings),
            (Provided::GmailThread, 1) => thread_read(state, &body, settings),
            (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
                named(state, &body)
            }
            (tool, step) => Err(internal(format!(
                "in-flight row names step {step} of {}, which has no such step",
                tool.name()
            ))),
        },
    )
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling — after `GmailRead`'s text is cut, or `GmailThread`'s
/// oldest messages dropped, to fit it.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let value = match tool {
                Provided::GmailRead => fit_text(value, COMPACT_RESULT_CEILING),
                Provided::GmailThread => fit_thread(value, COMPACT_RESULT_CEILING),
                Provided::GmailLabels | Provided::GmailSearch => value,
            };
            bounded(value, COMPACT_RESULT_CEILING)
        }
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

/// messages.get (full) read: the message's headers, labels, text and
/// attachments, then its user labels named.
fn read_full(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ReadArgs::parse)?;
    let api = "messages.get";
    let mut out = asked_message_fields(body, &args.id, READ_HEADERS, api)?;
    let extracted = extract(body, api)?;
    let text = extracted
        .text_parts
        .iter()
        .map(|(text, _)| text.as_str())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let text = plain(&text);
    let sources = || extracted.text_parts.iter().map(|(_, source)| *source);
    let text_source = if extracted.text_parts.is_empty() {
        "none"
    } else if sources().all(|s| s == decode::Source::Plain) {
        "text/plain"
    } else if sources().all(|s| s == decode::Source::Html) {
        "text/html"
    } else {
        "mixed"
    };
    let text_chars = text.chars().count();
    let text = match text.char_indices().nth(args.max_chars) {
        Some((end, _)) => {
            out.insert("text_truncated".to_string(), json!(true));
            text[..end].to_string()
        }
        None => text,
    };
    let mut notes = notes(&extracted, false);
    if text.is_empty() && extracted.text_by_reference {
        notes.push(
            "the body is an attachment reference, not fetched; it is listed under attachments"
                .to_string(),
        );
    }
    out.insert("text".to_string(), json!(text));
    out.insert("text_source".to_string(), json!(text_source));
    out.insert("text_chars".to_string(), json!(text_chars));
    out.insert("attachments".to_string(), attachments(&extracted));
    if !notes.is_empty() {
        out.insert("decoding_notes".to_string(), json!(notes));
    }
    finish(Value::Object(out), args.account.as_deref(), settings)
}

/// messages.get (full) cut at the read limit: the message is read again
/// through the structure mask, its size waiting in `derived`.
fn read_cut(state: &InFlight, size: &str, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ReadArgs::parse)?;
    Ok(Next::Call {
        call: Call {
            step: STRUCTURE_STEP,
            api: read_call(&args, Some(STRUCTURE_FIELDS), settings),
        },
        derived: Args::from_iter([("size".to_string(), json!(size))]),
    })
}

/// messages.get (structure) read: the headers, labels, snippet and
/// attachments, `text` null and `text_omitted` saying why; then the user
/// labels named. A structure answer itself cut is the step's failure.
fn read_structure(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ReadArgs::parse)?;
    let api = "messages.get";
    let Some(Value::String(size)) = state.derived.get("size") else {
        return Err(internal(
            "in-flight row at the structure step carries no size",
        ));
    };
    let mut out = asked_message_fields(body, &args.id, READ_HEADERS, api)?;
    let estimate = match body.get("sizeEstimate") {
        None => String::new(),
        Some(estimate) => format!(
            "; sizeEstimate {}",
            estimate.as_u64().ok_or_else(|| shape(api))?
        ),
    };
    if let Some(snippet) = snippet_value(body, api)? {
        out.insert("snippet".to_string(), json!(snippet));
    }
    let extracted = extract(body, api)?;
    let notes = notes(&extracted, true);
    out.insert("text".to_string(), Value::Null);
    out.insert(
        "text_omitted".to_string(),
        json!(format!(
            "Google's answer ({size}{estimate}) exceeds api-request's read limit for this \
             provider (max_response_bytes); headers and attachments are listed, the body is not"
        )),
    );
    out.insert("attachments".to_string(), attachments(&extracted));
    if !notes.is_empty() {
        out.insert("decoding_notes".to_string(), json!(notes));
    }
    finish(Value::Object(out), args.account.as_deref(), settings)
}

/// A message's `payload` read by `decode::extract`. A message without one, a
/// tree not of Gmail's shape, or a part whose data is not base64url is
/// Google's fault.
fn extract(body: &Value, api: &str) -> Result<decode::Extracted, Failure> {
    let payload = body.get("payload").ok_or_else(|| shape(api))?;
    decode::extract(payload).map_err(|error| match error {
        decode::WalkError::Shape => shape(api),
        decode::WalkError::Base64(part) => internal(format!(
            "Gmail {api} part {} carries body data that is not base64url",
            bounded_quote(&part)
        )),
    })
}

/// A sender-written string quoted in a fixed sentence: through `plain`, cut
/// to `HEADER_MAX_BYTES`.
fn bounded_quote(text: &str) -> String {
    cut(&plain(text), HEADER_MAX_BYTES).0
}

/// The decoding notes as answered. `structure` says the tree came through
/// the structure mask, whose depth is why a part's children go unlisted.
fn notes(extracted: &decode::Extracted, structure: bool) -> Vec<String> {
    extracted
        .notes
        .iter()
        .map(|note| match note {
            decode::Note::UnknownCharset(name) => format!(
                "charset \"{}\" is not supported; decoded as UTF-8, non-ASCII characters may \
                 be wrong",
                bounded_quote(name)
            ),
            decode::Note::InvalidUtf8 => {
                "a text part is not valid UTF-8; its invalid bytes are shown as \u{fffd}"
                    .to_string()
            }
            decode::Note::UnlistedChildren { mime_type, depth } if structure => format!(
                "a {} part at depth {depth} has children the structure mask does not list; \
                 attachments below it are not listed",
                bounded_quote(mime_type)
            ),
            decode::Note::UnlistedChildren { mime_type, depth } => format!(
                "a {} part at depth {depth} has no children in Google's answer; nothing below \
                 it is shown",
                bounded_quote(mime_type)
            ),
        })
        .collect()
}

/// The attachments as answered: `filename` and `mime_type` through `plain`
/// and cut to `HEADER_MAX_BYTES`, `size` when Google gave it, `inline`.
fn attachments(extracted: &decode::Extracted) -> Value {
    Value::Array(
        extracted
            .attachments
            .iter()
            .map(|attachment| {
                let mut entry = Args::new();
                entry.insert(
                    "filename".to_string(),
                    json!(bounded_quote(&attachment.filename)),
                );
                entry.insert(
                    "mime_type".to_string(),
                    json!(bounded_quote(&attachment.mime_type)),
                );
                if let Some(size) = attachment.size {
                    entry.insert("size".to_string(), json!(size));
                }
                entry.insert("inline".to_string(), json!(attachment.inline));
                Value::Object(entry)
            })
            .collect(),
    )
}

/// The serialized width of `c` in a JSON string, as `serde_json` writes it.
fn json_width(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        '\u{0}'..='\u{1f}' => 6,
        _ => c.len_utf8(),
    }
}

/// `answer` with its `text` cut to the longest prefix with which the whole
/// answer serializes within `ceiling`, and `text_truncated` set, when it does
/// not fit as it is. An answer that fits, or has no text, is unchanged; one
/// that does not fit with an empty text is left for the ceiling to refuse.
fn fit_text(mut answer: Value, ceiling: usize) -> Value {
    if serialized_len(&answer) <= ceiling {
        return answer;
    }
    let Some(Value::String(text)) = answer.get_mut("text").map(std::mem::take) else {
        return answer;
    };
    answer["text"] = json!("");
    answer["text_truncated"] = json!(true);
    let budget = ceiling.saturating_sub(serialized_len(&answer));
    let mut used = 0;
    let mut end = 0;
    for (at, c) in text.char_indices() {
        used += json_width(c);
        if used > budget {
            break;
        }
        end = at + c.len_utf8();
    }
    answer["text"] = json!(text[..end]);
    answer
}

/// `answer` with its oldest messages dropped, each counted in
/// `messages_omitted`, until it serializes within `ceiling`. An answer that
/// fits is unchanged.
fn fit_thread(mut answer: Value, ceiling: usize) -> Value {
    loop {
        let len = serialized_len(&answer);
        if len <= ceiling {
            return answer;
        }
        let Some(Value::Array(messages)) = answer.get_mut("messages") else {
            return answer;
        };
        if messages.is_empty() {
            return answer;
        }
        // Each dropped message takes its serialization and a comma with it;
        // the fewest that cover the excess are dropped, and the loop measures
        // again, since `messages_omitted` may have grown.
        let excess = len - ceiling;
        let mut removed = 0;
        let mut dropped = 0;
        while dropped < messages.len() && removed < excess {
            removed += serialized_len(&messages[dropped]) + 1;
            dropped += 1;
        }
        messages.drain(..dropped);
        let omitted = answer
            .get("messages_omitted")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        answer["messages_omitted"] = json!(omitted + dropped as u64);
    }
}

fn serialized_len(value: &Value) -> usize {
    serde_json::to_vec(value)
        .expect("a JSON value serializes")
        .len()
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

/// One message's metadata as a search line: `asked_message_fields` with the
/// `from`, `subject` and `date` headers, and the snippet through
/// `snippet_value`.
fn search_line(body: &Value, id: &str) -> Result<Value, Failure> {
    let api = "messages.get";
    let mut out = asked_message_fields(body, id, SEARCH_HEADERS, api)?;
    if let Some(snippet) = snippet_value(body, api)? {
        out.insert("snippet".to_string(), json!(snippet));
    }
    Ok(Value::Object(out))
}

/// threads.get read: the newest `max` messages in thread order, the rest
/// counted; the thread's subject is its first message's, and a message
/// answers its own subject only when that differs, both compared as Google
/// sent them. Then the user labels are named.
fn thread_read(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ThreadArgs::parse)?;
    let api = "threads.get";
    if body.get("id").and_then(Value::as_str) != Some(args.id.as_str()) {
        return Err(shape(api));
    }
    let messages = match body.get("messages") {
        Some(Value::Array(messages)) if !messages.is_empty() => messages,
        _ => return Err(shape(api)),
    };
    let mut out = Args::new();
    out.insert("id".to_string(), json!(args.id));
    answer_headers(&mut out, &messages[0], &[THREAD_SUBJECT], api)?;
    let subject = raw_header(&messages[0], THREAD_SUBJECT, api)?;
    let omitted = messages.len().saturating_sub(args.max);
    let answered = messages[omitted..]
        .iter()
        .map(|message| thread_message(message, subject, api))
        .collect::<Result<Vec<_>, _>>()?;
    out.insert("message_count".to_string(), json!(messages.len()));
    if omitted > 0 {
        out.insert("messages_omitted".to_string(), json!(omitted));
    }
    out.insert("messages".to_string(), Value::Array(answered));
    finish(Value::Object(out), args.account.as_deref(), settings)
}

/// One message of a thread: `message_fields` with the `THREAD_HEADERS`,
/// `THREAD_SUBJECT` among them only when it differs from the thread's
/// `subject`, and the snippet through `snippet_value`. `THREAD_FIELDS` asks
/// for no `threadId`, so none is answered.
fn thread_message(message: &Value, subject: Option<&str>, api: &str) -> Result<Value, Failure> {
    let differs = raw_header(message, THREAD_SUBJECT, api)? != subject;
    let headers: Vec<&str> = THREAD_HEADERS
        .iter()
        .copied()
        .filter(|&name| differs || name != THREAD_SUBJECT)
        .collect();
    let mut out = message_fields(message, &headers, api)?;
    if let Some(snippet) = snippet_value(message, api)? {
        out.insert("snippet".to_string(), json!(snippet));
    }
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
    let size = serialized_len(&value);
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

    /// A Gmail call's query that names only the `fields` mask.
    fn fields(mask: &str) -> Query {
        query([
            ("fields", mask.to_string()),
            ("prettyPrint", "false".to_string()),
        ])
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
                "query": { "fields": "labels(id,name,type)", "prettyPrint": "false" },
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
                    "prettyPrint": "false",
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
                    "prettyPrint": "false",
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

    const PLAIN: &str = include_str!("fixtures/plain.json");
    const ALTERNATIVE: &str = include_str!("fixtures/alternative.json");
    const NESTED: &str = include_str!("fixtures/nested.json");
    const LATIN1: &str = include_str!("fixtures/latin1.json");
    const CP1252: &str = include_str!("fixtures/cp1252.json");
    const UNKNOWN_CHARSET: &str = include_str!("fixtures/unknown-charset.json");
    const STRUCTURE: &str = include_str!("fixtures/structure.json");
    const STRUCTURE_DEEP: &str = include_str!("fixtures/structure-deep.json");
    const HTML_ONLY: &str = include_str!("fixtures/html-only.json");
    const ALTERNATIVE_HTML_RICHER: &str = include_str!("fixtures/alternative-html-richer.json");

    fn read_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailRead, &args, &settings()).unwrap();
        InFlight::new(
            &"r".repeat(32),
            Provided::GmailRead,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    /// `GmailRead { id: "m1" }` answered with `body` at its first step.
    fn read_m1(body: &str) -> Next {
        answered(&read_state(json!({ "id": "m1" })), http(200, body))
    }

    fn completed(next: Next) -> Value {
        let Next::Complete(value) = next else {
            panic!("expected a completed answer, got {next:?}");
        };
        value
    }

    /// The fields every fixture's top-level headers and metadata answer.
    fn read_fields(labels: Value) -> Value {
        json!({
            "id": "m1",
            "thread_id": "t1",
            "labels": labels,
            "from": "Alice Example <alice@example.com>",
            "to": "Bob Example <bob@example.com>",
            "subject": "Quarterly numbers",
            "date": "Sat, 31 Oct 2026 14:05:09 +0000",
            "message_id": "<m1@example.com>",
            "internal_date": "2026-10-31T14:05:09Z",
        })
    }

    fn with_fields(mut base: Value, extra: Value) -> Value {
        for (key, value) in extra.as_object().unwrap() {
            base[key] = value.clone();
        }
        base
    }

    #[test]
    fn read_plans_one_full_read() {
        let call = plan(
            Provided::GmailRead,
            &json!({ "id": "m1", "account": "alice@example.com" }),
            &settings(),
        )
        .unwrap();
        assert_eq!(call.step, 1);
        assert_eq!(
            serde_json::to_value(&call.api).unwrap(),
            json!({
                "provider": "google",
                "account": "alice@example.com",
                "method": "GET",
                "host": "gmail.example.com",
                "path": "/gmail/v1/users/me/messages/m1",
                "query": { "format": "full", "prettyPrint": "false" },
            })
        );
        for (args, error) in [
            (json!({}), "`id` is required"),
            (
                json!({ "id": "m/1" }),
                "`id` must be 1 to 128 letters, digits, `_` or `-`",
            ),
            (
                json!({ "id": "m1", "max_chars": 199 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "m1", "max_chars": 60001 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "m1", "format": "raw" }),
                "unknown argument `format`; this tool takes `id`, `max_chars`, `account`",
            ),
        ] {
            assert_eq!(
                plan(Provided::GmailRead, &args, &settings()).unwrap_err(),
                error,
                "{args}"
            );
        }
    }

    #[test]
    fn a_plain_message_reads_whole() {
        let text = "Hi Bob,\n\nThe numbers are in. Café at noon? 😀\n\n— Alice\n";
        assert_eq!(
            completed(read_m1(PLAIN)),
            with_fields(
                read_fields(json!(["INBOX", "UNREAD"])),
                json!({
                    "text": text,
                    "text_source": "text/plain",
                    "text_chars": text.chars().count(),
                    "attachments": [],
                })
            )
        );
    }

    #[test]
    fn an_alternative_reads_its_plain_part() {
        let answer = completed(read_m1(ALTERNATIVE));
        assert_eq!(answer["text"], json!("The numbers are in.\r\n"));
        assert_eq!(answer["text_source"], json!("text/plain"));
        assert_eq!(answer.get("decoding_notes"), None);
    }

    #[test]
    fn a_nested_message_lists_its_attachments_and_names_its_labels() {
        let state = read_state(json!({ "id": "m1" }));
        let (row, call) = follow(&state, answered(&state, http(200, NESTED)));
        assert_eq!(call.step, LABELS_STEP);
        let names = r#"{ "labels": [{ "id": "Label_7", "name": "Receipts" }] }"#;
        assert_eq!(
            completed(answered(&row, http(200, names))),
            with_fields(
                read_fields(json!(["INBOX", "Receipts"])),
                json!({
                    "text": "See the report.\n",
                    "text_source": "text/plain",
                    "text_chars": 16,
                    "attachments": [
                        { "filename": "Q3 report.pdf", "mime_type": "application/pdf",
                          "size": 48213, "inline": false },
                        { "filename": "logo.png", "mime_type": "image/png",
                          "size": 1024, "inline": true },
                    ],
                })
            )
        );
    }

    #[test]
    fn three_charsets_decode_and_any_other_is_noted() {
        assert_eq!(
            completed(read_m1(LATIN1))["text"],
            json!("Café crème, 5 °C\n")
        );
        assert_eq!(
            completed(read_m1(CP1252))["text"],
            json!("\u{201c}Quoted\u{201d} \u{2013} €10 \u{2026} \u{fffd}\n")
        );
        let answer = completed(read_m1(UNKNOWN_CHARSET));
        assert_eq!(
            answer["text"],
            json!("Plain ASCII, then \u{fffd}\u{fffd}\n")
        );
        assert_eq!(
            answer["decoding_notes"],
            json!([
                "charset \"KOI8-R\" is not supported; decoded as UTF-8, non-ASCII characters \
                 may be wrong"
            ])
        );
    }

    /// A one-part message: `part` as its payload, with `headers` of its own.
    fn one_part(part: Value, headers: Value) -> String {
        let mut payload = part;
        payload["headers"] = headers;
        json!({ "id": "m1", "threadId": "t1", "payload": payload }).to_string()
    }

    #[test]
    fn an_html_only_message_reads_as_text() {
        let text = "October\u{a0}news\n\n\
                    Hello Bob,\n\n\
                    Prices are up 5\u{a0}&percnt; \u{2014} see the report.\n\n\
                    Plan\tPrice\n\n\
                    Basic\t€10\n\n\
                    Pro\t€25\n\n\
                    Faster sync\n\
                    Café mode\n\n\
                    © 2026 Example Corp · Unsubscribe";
        assert_eq!(
            completed(read_m1(HTML_ONLY)),
            json!({
                "id": "m1",
                "thread_id": "t1",
                "labels": ["INBOX", "CATEGORY_UPDATES"],
                "from": "Example Corp <news@example.com>",
                "to": "Bob Example <bob@example.com>",
                "subject": "October news",
                "date": "Sat, 31 Oct 2026 14:05:09 +0000",
                "message_id": "<m1@example.com>",
                "internal_date": "2026-10-31T14:05:09Z",
                "text": text,
                "text_source": "text/html",
                "text_chars": text.chars().count(),
                "attachments": [],
            })
        );
    }

    #[test]
    fn an_alternative_reads_its_plain_part_even_when_the_html_says_more() {
        let answer = completed(read_m1(ALTERNATIVE_HTML_RICHER));
        assert_eq!(answer["text"], json!("Hello Bob,\r\nSee the report.\r\n"));
        assert_eq!(answer["text_source"], json!("text/plain"));
        assert_eq!(answer.get("decoding_notes"), None);
    }

    #[test]
    fn an_html_message_with_no_text_answers_empty_html_text() {
        // "<img src=\"cid:logo\">"
        let body = one_part(
            json!({ "mimeType": "text/html", "body": { "size": 20, "data": "PGltZyBzcmM9ImNpZDpsb2dvIj4" } }),
            json!([]),
        );
        let answer = completed(read_m1(&body));
        assert_eq!(answer["text"], json!(""));
        assert_eq!(answer["text_source"], json!("text/html"));
        assert_eq!(answer["text_chars"], json!(0));
        assert_eq!(answer.get("decoding_notes"), None);
    }

    #[test]
    fn read_text_keeps_tab_newline_and_return_and_spaces_other_controls() {
        // "a\tb\r\nc\x01d", base64url.
        let body = one_part(
            json!({ "mimeType": "text/plain", "body": { "size": 7, "data": "YQliDQpjAWQ" } }),
            json!([]),
        );
        let answer = completed(read_m1(&body));
        assert_eq!(answer["text"], json!("a\tb\r\nc d"));
        assert_eq!(answer["text_chars"], json!(8));
    }

    /// A `multipart/mixed` message of `parts`.
    fn mixed(parts: Value) -> String {
        one_part(
            json!({ "mimeType": "multipart/mixed", "parts": parts }),
            json!([]),
        )
    }

    #[test]
    fn text_parts_join_with_a_blank_line_and_mixed_sources_say_so() {
        let leaf = |data: &str| json!({ "mimeType": "text/plain", "body": { "data": data } });
        let html = json!({ "mimeType": "text/html", "body": { "data": "PHA-aGk8L3A-" } });
        // "a", "b"
        let answer = completed(read_m1(&mixed(json!([leaf("YQ"), leaf("Yg")]))));
        assert_eq!(answer["text"], json!("a\n\nb"));
        assert_eq!(answer["text_source"], json!("text/plain"));
        let answer = completed(read_m1(&mixed(json!([leaf("YQ"), html, leaf("Yg")]))));
        assert_eq!(answer["text"], json!("a\n\nhi\n\nb"));
        assert_eq!(answer["text_source"], json!("mixed"));
        assert_eq!(answer.get("decoding_notes"), None);
    }

    #[test]
    fn a_full_read_says_what_it_could_not_show() {
        let answer = completed(read_m1(&mixed(json!([
            { "mimeType": "text/plain", "body": { "data": "Yf9i" } },
            { "mimeType": "message/rfc822" },
        ]))));
        assert_eq!(answer["text"], json!("a\u{fffd}b"));
        assert_eq!(
            answer["decoding_notes"],
            json!([
                "a text part is not valid UTF-8; its invalid bytes are shown as \u{fffd}",
                "a message/rfc822 part at depth 1 has no children in Google's answer; nothing \
                 below it is shown",
            ])
        );
    }

    #[test]
    fn fit_text_cuts_to_the_longest_text_that_fits() {
        let text = "a\"\\\n\r\t\u{8}\u{c}\u{1}é😀".repeat(20);
        let answer = json!({ "id": "m1", "text": text });
        let with_text = |text: &str| {
            let mut answer = answer.clone();
            answer["text"] = json!(text);
            answer["text_truncated"] = json!(true);
            serialized_len(&answer)
        };
        let whole = serialized_len(&answer);
        assert_eq!(fit_text(answer.clone(), whole), answer);
        for ceiling in with_text("")..whole {
            let fitted = fit_text(answer.clone(), ceiling);
            let cut = fitted["text"].as_str().unwrap();
            assert_eq!(fitted["text_truncated"], json!(true));
            assert!(serialized_len(&fitted) <= ceiling, "{ceiling}");
            let next = text[cut.len()..].chars().next().unwrap();
            assert!(
                with_text(&text[..cut.len() + next.len_utf8()]) > ceiling,
                "{ceiling}"
            );
        }
    }

    #[test]
    fn a_body_by_reference_is_listed_and_said() {
        let body = one_part(
            json!({ "mimeType": "text/plain", "body": { "size": 9, "attachmentId": "ATT-1" } }),
            json!([]),
        );
        let answer = completed(read_m1(&body));
        assert_eq!(answer["text"], json!(""));
        assert_eq!(answer["text_source"], json!("none"));
        assert_eq!(
            answer["attachments"],
            json!([{ "filename": "", "mime_type": "text/plain", "size": 9, "inline": false }])
        );
        assert_eq!(
            answer["decoding_notes"],
            json!([
                "the body is an attachment reference, not fetched; it is listed under attachments"
            ])
        );
    }

    #[test]
    fn data_that_is_not_base64url_is_internal_naming_the_part() {
        let body = one_part(
            json!({ "partId": "0.1", "mimeType": "text/plain", "body": { "data": "a*b" } }),
            json!([]),
        );
        assert_eq!(
            read_m1(&body),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Gmail messages.get part 0.1 carries body data that is not base64url"
            ))
        );
        for payload in [
            json!({ "body": {} }),
            json!({ "mimeType": "multipart/mixed", "parts": {} }),
            json!({ "mimeType": "text/plain", "body": { "data": 7 } }),
        ] {
            assert_eq!(
                read_m1(&json!({ "id": "m1", "payload": payload }).to_string()),
                Next::Fail(shape("messages.get")),
                "{payload}"
            );
        }
    }

    #[test]
    fn sender_strings_in_attachments_and_notes_are_plain_and_bounded() {
        let body = one_part(
            json!({ "mimeType": "multipart/mixed", "parts": [
                { "mimeType": "text/plain", "body": { "data": "aGk" },
                  "headers": [{ "name": "Content-Type",
                                "value": format!("text/plain; charset={}", "x".repeat(1001)) }] },
                { "mimeType": "y".repeat(1001), "filename": "a\u{1}b\u{7f}", "body": {} },
                { "mimeType": "image/png", "filename": "z".repeat(1001), "body": {} },
            ] }),
            json!([]),
        );
        let answer = completed(read_m1(&body));
        let cut = |c: &str| format!("{}…", c.repeat(1000));
        assert_eq!(
            answer["attachments"],
            json!([
                { "filename": "a b\u{7f}", "mime_type": cut("y"), "inline": false },
                { "filename": cut("z"), "mime_type": "image/png", "inline": false },
            ])
        );
        assert_eq!(
            answer["decoding_notes"],
            json!([format!(
                "charset \"{}\" is not supported; decoded as UTF-8, non-ASCII characters may be \
                 wrong",
                cut("x")
            )])
        );
    }

    #[test]
    fn recipient_headers_are_bounded_at_4000_bytes_and_flagged() {
        let headers = |to: usize, subject: usize| {
            json!([
                { "name": "TO", "value": "t".repeat(to) },
                { "name": "Subject", "value": "s".repeat(subject) },
            ])
        };
        let leaf = json!({ "mimeType": "text/plain", "body": { "data": "aGk" } });
        let answer = completed(read_m1(&one_part(leaf.clone(), headers(4000, 1000))));
        assert_eq!(answer["to"], json!("t".repeat(4000)));
        assert_eq!(answer.get("headers_truncated"), None);
        let answer = completed(read_m1(&one_part(leaf.clone(), headers(4001, 1000))));
        assert_eq!(answer["to"], json!(format!("{}…", "t".repeat(4000))));
        assert_eq!(answer["headers_truncated"], json!(["to"]));
        let answer = completed(read_m1(&one_part(leaf, headers(4001, 1001))));
        assert_eq!(answer["subject"], json!(format!("{}…", "s".repeat(1000))));
        assert_eq!(answer["headers_truncated"], json!(["subject", "to"]));
    }

    #[test]
    fn text_is_cut_at_max_chars_on_a_char_boundary() {
        let text = "é".repeat(150) + &"😀".repeat(100);
        let data = base64_of(text.as_bytes());
        let body = one_part(
            json!({ "mimeType": "text/plain", "body": { "data": data } }),
            json!([]),
        );
        let state = read_state(json!({ "id": "m1", "max_chars": 200 }));
        let answer = completed(answered(&state, http(200, &body)));
        assert_eq!(answer["text"], json!("é".repeat(150) + &"😀".repeat(50)));
        assert_eq!(answer["text_chars"], json!(250));
        assert_eq!(answer["text_truncated"], json!(true));
    }

    /// `bytes` in base64url without padding, for building bodies in tests.
    fn base64_of(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = chunk
                .iter()
                .enumerate()
                .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
            for i in 0..=chunk.len() {
                out.push(char::from(ALPHABET[(n >> (18 - 6 * i) & 63) as usize]));
            }
        }
        out
    }

    #[test]
    fn a_text_past_the_ceiling_is_cut_to_fit_after_naming() {
        let text = "😀".repeat(60_000);
        let headers: Vec<Value> = READ_HEADERS
            .iter()
            .map(|name| json!({ "name": name, "value": "\"".repeat(5000) }))
            .collect();
        let body = |labels: Value| {
            json!({ "id": "m1", "labelIds": labels, "payload": {
                "mimeType": "text/plain", "headers": headers,
                "body": { "data": base64_of(text.as_bytes()) },
            } })
            .to_string()
        };
        let state = read_state(json!({ "id": "m1", "max_chars": 60_000 }));
        let fitted = completed(answered(&state, http(200, &body(json!(["INBOX"])))));
        let plain_len = fitted["text"].as_str().unwrap().len();
        assert!(plain_len < text.len(), "{plain_len}");
        assert_eq!(fitted["text_chars"], json!(60_000));
        assert_eq!(fitted["text_truncated"], json!(true));
        // Each char of the text is 4 bytes, so the most that fits is within 4
        // bytes of the ceiling.
        let fills = |answer: &Value| {
            let len = serialized_len(answer);
            len <= COMPACT_RESULT_CEILING && len > COMPACT_RESULT_CEILING - 4
        };
        assert!(fills(&fitted));

        let (row, call) = follow(
            &state,
            answered(&state, http(200, &body(json!(["Label_12"])))),
        );
        assert_eq!(call.step, LABELS_STEP);
        let long_name = "n".repeat(225);
        let names = json!({ "labels": [{ "id": "Label_12", "name": long_name }] }).to_string();
        let named = completed(answered(&row, http(200, &names)));
        assert_eq!(named["labels"], json!([long_name]));
        assert!(named["text"].as_str().unwrap().len() < plain_len);
        assert!(fills(&named));
    }

    fn cut_read(bytes: Value) -> Result<Value, NestedError> {
        Ok(json!({
            "status": 200, "headers": {}, "bytes": bytes, "truncated": true, "body": "{\"id",
        }))
    }

    #[test]
    fn a_cut_read_falls_back_to_the_structure() {
        let state = read_state(json!({ "id": "m1" }));
        let (row, call) = follow(&state, answered(&state, cut_read(json!(300000))));
        assert_eq!(call.step, STRUCTURE_STEP);
        assert_eq!(call.api.path, "/gmail/v1/users/me/messages/m1");
        assert_eq!(
            call.api.query["format"],
            QueryValue::One("full".to_string())
        );
        assert_eq!(
            call.api.query["fields"],
            QueryValue::One(
                "id,threadId,labelIds,snippet,internalDate,sizeEstimate,payload(mimeType,\
                 filename,headers,body(size,attachmentId),parts(mimeType,filename,headers,\
                 body(size,attachmentId),parts(mimeType,filename,headers,body(size,\
                 attachmentId),parts(mimeType,filename,headers,body(size,attachmentId)))))"
                    .to_string()
            )
        );
        assert_eq!(
            completed(answered(&row, http(200, STRUCTURE))),
            with_fields(
                read_fields(json!(["INBOX", "UNREAD"])),
                json!({
                    "snippet": "The numbers are in & attached",
                    "text": null,
                    "text_omitted": "Google's answer (300000 bytes; sizeEstimate 4096) exceeds \
                                     api-request's read limit for this provider \
                                     (max_response_bytes); headers and attachments are listed, \
                                     the body is not",
                    "attachments": [{ "filename": "Q3 report.pdf",
                                      "mime_type": "application/pdf", "size": 48213,
                                      "inline": false }],
                })
            )
        );
        let (row, _) = follow(&state, answered(&state, cut_read(Value::Null)));
        let answer = completed(answered(&row, http(200, STRUCTURE)));
        assert!(
            answer["text_omitted"]
                .as_str()
                .unwrap()
                .starts_with("Google's answer (of unstated length; sizeEstimate 4096) exceeds"),
            "{answer}"
        );
        assert_eq!(
            answered(&row, cut_read(json!(900000))),
            Next::Fail(cut_answer("900000 bytes"))
        );
    }

    #[test]
    fn a_structure_too_deep_to_list_says_so() {
        let state = read_state(json!({ "id": "m1" }));
        let (row, _) = follow(&state, answered(&state, cut_read(json!(300000))));
        let answer = completed(answered(&row, http(200, STRUCTURE_DEEP)));
        assert_eq!(
            answer["attachments"],
            json!([{ "filename": "inner.pdf", "mime_type": "application/pdf", "size": 2048,
                     "inline": false }])
        );
        assert_eq!(
            answer["decoding_notes"],
            json!([
                "a multipart/alternative part at depth 3 has children the structure mask does \
                 not list; attachments below it are not listed"
            ])
        );
    }

    const THREAD: &str = include_str!("fixtures/thread.json");

    fn thread_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailThread, &args, &settings()).unwrap();
        InFlight::new(
            &"t".repeat(32),
            Provided::GmailThread,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    /// The thread fixture with `edit` applied to its messages.
    fn thread_body(edit: impl FnOnce(&mut Vec<Value>)) -> String {
        let mut body: Value = serde_json::from_str(THREAD).unwrap();
        let Value::Array(messages) = &mut body["messages"] else {
            panic!("the fixture lists messages");
        };
        edit(messages);
        body.to_string()
    }

    /// `message`'s header `name` set to `value`, replacing any of that name.
    fn set_header(message: &mut Value, name: &str, value: String) {
        let Value::Array(headers) = &mut message["payload"]["headers"] else {
            panic!("the message has headers");
        };
        headers.retain(|h| !h["name"].as_str().unwrap().eq_ignore_ascii_case(name));
        headers.push(json!({ "name": name, "value": value }));
    }

    #[test]
    fn thread_plans_one_metadata_read() {
        let call = plan(
            Provided::GmailThread,
            &json!({ "id": "t1", "max": 5, "account": "alice@example.com" }),
            &settings(),
        )
        .unwrap();
        assert_eq!(call.step, 1);
        assert_eq!(
            serde_json::to_value(&call.api).unwrap(),
            json!({
                "provider": "google",
                "account": "alice@example.com",
                "method": "GET",
                "host": "gmail.example.com",
                "path": "/gmail/v1/users/me/threads/t1",
                "query": {
                    "format": "metadata",
                    "metadataHeaders": ["From", "To", "Cc", "Subject", "Date"],
                    "fields": "id,messages(id,labelIds,snippet,internalDate,payload/headers)",
                    "prettyPrint": "false",
                },
            })
        );
        for (args, error) in [
            (json!({}), "`id` is required"),
            (
                json!({ "id": "t/1" }),
                "`id` must be 1 to 128 letters, digits, `_` or `-`",
            ),
            (
                json!({ "id": "t1", "max": 0 }),
                "`max` must be an integer from 1 to 100",
            ),
            (
                json!({ "id": "t1", "max": 101 }),
                "`max` must be an integer from 1 to 100",
            ),
            (
                json!({ "id": "t1", "bodies": true }),
                "unknown argument `bodies`; this tool takes `id`, `max`, `account`",
            ),
        ] {
            assert_eq!(
                plan(Provided::GmailThread, &args, &settings()).unwrap_err(),
                error,
                "{args}"
            );
        }
    }

    #[test]
    fn a_thread_answers_its_messages_in_order_and_names_their_labels() {
        let state = thread_state(json!({ "id": "t1", "account": "alice@example.com" }));
        let (row, call) = follow(&state, answered(&state, http(200, THREAD)));
        assert_eq!(call.step, LABELS_STEP);
        assert_eq!(call.api.account.as_deref(), Some("alice@example.com"));
        let names = r#"{ "labels": [{ "id": "Label_7", "name": "Projects" }] }"#;
        assert_eq!(
            completed(answered(&row, http(200, names))),
            json!({
                "id": "t1",
                "subject": "Planning",
                "message_count": 4,
                "messages": [
                    { "id": "m1", "from": "Alice Example <alice@example.com>",
                      "to": "Bob Example <bob@example.com>",
                      "date": "Sat, 31 Oct 2026 14:05:09 +0000",
                      "internal_date": "2026-10-31T14:05:09Z",
                      "snippet": "Can we meet Tuesday?", "labels": ["SENT"] },
                    { "id": "m2", "from": "Bob Example <bob@example.com>",
                      "to": "Alice Example <alice@example.com>",
                      "cc": "Carol Example <carol@example.com>",
                      "date": "Sat, 31 Oct 2026 15:05:09 +0000",
                      "internal_date": "2026-10-31T15:05:09Z",
                      "snippet": "Tuesday works & I'll book a room",
                      "labels": ["INBOX", "Projects"] },
                    { "id": "m3", "from": "Carol Example <carol@example.com>",
                      "to": "Alice Example <alice@example.com>, Bob Example <bob@example.com>",
                      "subject": "Re: Planning (room booked)",
                      "date": "Sat, 31 Oct 2026 16:05:09 +0000",
                      "internal_date": "2026-10-31T16:05:09Z",
                      "snippet": "Room 4 is booked",
                      "labels": ["INBOX", "UNREAD", "Projects"] },
                    { "id": "m4", "from": "Bob Example <bob@example.com>",
                      "to": "Carol Example <carol@example.com>",
                      "date": "Sat, 31 Oct 2026 17:05:09 +0000",
                      "internal_date": "2026-10-31T17:05:09Z",
                      "snippet": "See you there", "labels": ["INBOX", "UNREAD"] },
                ],
            })
        );
    }

    #[test]
    fn a_thread_longer_than_max_keeps_the_newest() {
        let state = thread_state(json!({ "id": "t1", "max": 2 }));
        let (row, _) = follow(&state, answered(&state, http(200, THREAD)));
        let answer = completed(answered(&row, http(200, "{}")));
        assert_eq!(answer["subject"], json!("Planning"));
        assert_eq!(answer["message_count"], json!(4));
        assert_eq!(answer["messages_omitted"], json!(2));
        let ids: Vec<&str> = answer["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["m3", "m4"]);
        assert_eq!(answer["messages"][1].get("subject"), None);

        let whole = thread_state(json!({ "id": "t1", "max": 4 }));
        let (row, _) = follow(&whole, answered(&whole, http(200, THREAD)));
        let answer = completed(answered(&row, http(200, "{}")));
        assert_eq!(answer.get("messages_omitted"), None);
        assert_eq!(answer["messages"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn thread_headers_are_bounded_and_flagged_per_object() {
        let body = thread_body(|messages| {
            set_header(&mut messages[0], "Subject", "s".repeat(1001));
            set_header(&mut messages[1], "Subject", "s".repeat(1001));
            set_header(&mut messages[1], "Cc", "c".repeat(4001));
            set_header(&mut messages[2], "Subject", "s".repeat(1000) + "t");
            set_header(&mut messages[3], "To", "t".repeat(4000));
            for message in messages.iter_mut() {
                message["labelIds"] = json!(["INBOX"]);
            }
        });
        let answer = completed(answered(
            &thread_state(json!({ "id": "t1" })),
            http(200, &body),
        ));
        let cut = |c: &str, n: usize| format!("{}…", c.repeat(n));
        assert_eq!(answer["subject"], json!(cut("s", 1000)));
        assert_eq!(answer["headers_truncated"], json!(["subject"]));
        let messages = answer["messages"].as_array().unwrap();
        assert_eq!(messages[0].get("subject"), None);
        assert_eq!(messages[0].get("headers_truncated"), None);
        assert_eq!(messages[1].get("subject"), None);
        assert_eq!(messages[1]["cc"], json!(cut("c", 4000)));
        assert_eq!(messages[1]["headers_truncated"], json!(["cc"]));
        assert_eq!(messages[2]["subject"], json!(cut("s", 1000)));
        assert_eq!(messages[2]["headers_truncated"], json!(["subject"]));
        assert_eq!(messages[3]["subject"], json!("Planning"));
        assert_eq!(messages[3]["to"], json!("t".repeat(4000)));
        assert_eq!(messages[3].get("headers_truncated"), None);
    }

    /// A thread of `count` messages, each with `To` and `Cc` at their bound in
    /// `"` (the widest serialization) and labelled `labels`.
    fn wide_thread(count: usize, labels: &[&str]) -> String {
        let messages: Vec<Value> = (0..count)
            .map(|i| {
                json!({
                    "id": format!("m{i}"),
                    "labelIds": labels,
                    "snippet": "Hello",
                    "internalDate": "1793455509000",
                    "payload": { "headers": [
                        { "name": "From", "value": "Alice Example <alice@example.com>" },
                        { "name": "To", "value": "\"".repeat(4000) },
                        { "name": "Cc", "value": "\"".repeat(4000) },
                        { "name": "Subject", "value": if i == 0 { "Planning" } else { "Re: Planning" } },
                    ] },
                })
            })
            .collect();
        json!({ "id": "t1", "messages": messages }).to_string()
    }

    /// The ids `answer` kept, which must be the newest of a `count`-message
    /// thread, with every other message counted as omitted.
    fn kept_newest(answer: &Value, count: usize) -> usize {
        let messages = answer["messages"].as_array().unwrap();
        let kept = messages.len();
        assert!(kept > 0 && kept < count, "{kept}");
        assert_eq!(answer["messages_omitted"], json!(count - kept));
        assert_eq!(answer["message_count"], json!(count));
        for (i, message) in messages.iter().enumerate() {
            assert_eq!(message["id"], json!(format!("m{}", count - kept + i)));
        }
        assert_eq!(answer["subject"], json!("Planning"));
        assert!(serialized_len(answer) <= COMPACT_RESULT_CEILING);
        kept
    }

    #[test]
    fn a_thread_past_the_ceiling_keeps_fewer_of_the_newest() {
        let state = thread_state(json!({ "id": "t1", "max": 100 }));
        let answer = completed(answered(&state, http(200, &wide_thread(100, &["INBOX"]))));
        let unlabelled = kept_newest(&answer, 100);
        // Dropped no more than it had to: one more message would not fit.
        let mut one_more = answer.clone();
        let Value::Array(messages) = &mut one_more["messages"] else {
            unreachable!()
        };
        messages.insert(0, messages[0].clone());
        one_more["messages_omitted"] = json!(100 - unlabelled - 1);
        assert!(serialized_len(&one_more) > COMPACT_RESULT_CEILING);

        // `max` leaves 40 out before the ceiling drops more; the count covers
        // both, and the same newest messages are kept.
        let capped = thread_state(json!({ "id": "t1", "max": 60 }));
        let answer = completed(answered(&capped, http(200, &wide_thread(100, &["INBOX"]))));
        assert_eq!(kept_newest(&answer, 100), unlabelled);
        assert!(unlabelled < 60);

        let (row, call) = follow(
            &state,
            answered(&state, http(200, &wide_thread(100, &["INBOX", "Label_12"]))),
        );
        assert_eq!(call.step, LABELS_STEP);
        let long_name = "n".repeat(225);
        let names = json!({ "labels": [{ "id": "Label_12", "name": long_name }] }).to_string();
        let named = completed(answered(&row, http(200, &names)));
        assert_eq!(named["messages"][0]["labels"], json!(["INBOX", long_name]));
        assert!(kept_newest(&named, 100) < unlabelled);
    }

    #[test]
    fn fit_thread_leaves_an_answer_that_fits() {
        let answer = json!({ "id": "t1", "message_count": 1, "messages": [{ "id": "m1" }] });
        assert_eq!(fit_thread(answer.clone(), COMPACT_RESULT_CEILING), answer);
        let fitted = fit_thread(answer.clone(), serialized_len(&answer) - 1);
        assert_eq!(fitted["messages"], json!([]));
        assert_eq!(fitted["messages_omitted"], json!(1));

        // A count `max` already started is added to, not replaced.
        let mut capped = answer;
        capped["messages_omitted"] = json!(2);
        let fitted = fit_thread(capped.clone(), serialized_len(&capped) - 1);
        assert_eq!(fitted["messages"], json!([]));
        assert_eq!(fitted["messages_omitted"], json!(3));
    }

    #[test]
    fn thread_refuses_an_unexpected_shape_or_a_cut_answer() {
        let state = thread_state(json!({ "id": "t1" }));
        for body in [
            r#"{ "id": "t2", "messages": [{ "id": "m1" }] }"#.to_string(),
            r#"{ "id": "t1" }"#.to_string(),
            r#"{ "id": "t1", "messages": [] }"#.to_string(),
            r#"{ "id": "t1", "messages": {} }"#.to_string(),
            r#"{ "id": "t1", "messages": [{ "id": "m/1" }] }"#.to_string(),
            r#"{ "id": "t1", "messages": [{ "id": 7 }] }"#.to_string(),
            r#"{ "id": "t1", "messages": [{ "id": "m1", "labelIds": ["a b"] }] }"#.to_string(),
            r#"{ "id": "t1", "messages": [{ "id": "m1", "internalDate": 7 }] }"#.to_string(),
            r#"{ "id": "t1", "messages": [{ "id": "m1", "payload": { "headers": {} } }] }"#
                .to_string(),
            thread_body(|messages| messages[3]["id"] = json!("m 4")),
        ] {
            assert_eq!(
                answered(&state, http(200, &body)),
                Next::Fail(shape("threads.get")),
                "{body}"
            );
        }
        assert_eq!(
            answered(&state, cut_read(json!(300000))),
            Next::Fail(cut_answer("300000 bytes"))
        );
    }

    #[test]
    fn a_message_of_a_thread_past_max_is_not_read() {
        // The oldest message is malformed, but `max` leaves it out.
        let body = thread_body(|messages| messages[0]["labelIds"] = json!(["a b"]));
        let state = thread_state(json!({ "id": "t1", "max": 3 }));
        let (row, _) = follow(&state, answered(&state, http(200, &body)));
        let answer = completed(answered(&row, http(200, "{}")));
        assert_eq!(answer["messages_omitted"], json!(1));
    }
}
