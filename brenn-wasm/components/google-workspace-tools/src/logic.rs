//! The wrapper's decisions, in `std` plus `serde_json`: reading the
//! instance's `config`, validating a tool's arguments, building each Gmail
//! and Calendar call as `api-request` arguments, reading `api-request`'s
//! answer, and compacting Google's JSON into the tool's result. Nothing here
//! names the guest SDK, so the whole module runs on the host under test.
//! Tools are the generated `Provided` enum, whose SDK-facing parts are
//! wasm32-only.

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

use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, ErrorKind, Failure,
    GOOGLE_MESSAGE_MAX_BYTES, NestedError, Next, Query, QueryValue, Tool, Unreadable, bounded,
    capped, copy, cut, flag, google_error_detail, host, integer, internal, no_such_step, object,
    query, read_api_result, required, serialized_len, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The most characters an `account` argument may hold: a mailbox address's
/// limit.
const ACCOUNT_MAX_CHARS: usize = 320;
/// The most UTF-8 bytes of an email address copied into an event answer (the
/// organizer's and each attendee's). The same number as `ACCOUNT_MAX_CHARS`,
/// counted in bytes because it sizes the answer against the byte-counted
/// result ceiling.
const ACCOUNT_MAX_BYTES: usize = 320;

/// The most characters a Gmail id (message, thread or label) may hold. Ids
/// are one path segment of letters, digits, `_` and `-`, which `api-request`
/// neither decodes nor refuses; being ASCII, an id's characters are its bytes,
/// so the bound holds for ids Google answers too.
const ID_MAX_CHARS: usize = 128;

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
/// serialize to at most 1 715 bytes. A page past that answers `internal`. A
/// listed message is a line or an `unavailable` entry, and an entry (an id and
/// a reason cut to `GOOGLE_MESSAGE_MAX_BYTES`) serializes to at most 2 155.
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
/// message's read was cut at the provider's read limit or refused by the host.
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
/// `GmailThread`'s second step: the thread read again without `To` and `Cc`,
/// when the first read was cut or refused.
const THREAD_NARROW_STEP: u8 = 2;
/// The headers the narrower thread read asks for: the recipient headers, which
/// a wide thread is wide in, left out.
const THREAD_NARROW_HEADERS: &[&str] = &["From", THREAD_SUBJECT, "Date"];
/// `GmailThread`'s `max`: the range and the default.
const THREAD_MAX_MIN: u64 = 1;
const THREAD_MAX_MAX: u64 = 100;
const THREAD_MAX_DEFAULT: u64 = 30;

/// The most characters a search query may hold.
const QUERY_MAX_CHARS: usize = 1024;
/// The most characters a `page` argument may hold.
const PAGE_MAX_CHARS: usize = 4096;
/// The most bytes of a page token Google answers as `nextPageToken`. Counted
/// in bytes because it sizes the answer; no more than `PAGE_MAX_CHARS`, as
/// asserted below, so a token Google gave is one `page` admits back.
const PAGE_MAX_BYTES: usize = 4096;
const _: () = assert!(PAGE_MAX_BYTES <= PAGE_MAX_CHARS);
/// `GmailSearch`'s `max`: the range and the default.
const SEARCH_MAX_MIN: u64 = 1;
const SEARCH_MAX_MAX: u64 = 25;
const SEARCH_MAX_DEFAULT: u64 = 10;
/// `GmailSearch`'s per-message steps: listed id `i` is read at step `2 + i`.
const SEARCH_FIRST_GET: u8 = 2;
const SEARCH_LAST_GET: u8 = SEARCH_FIRST_GET + SEARCH_MAX_MAX as u8 - 1;

/// The path Calendar lists the user's calendars at.
const CALENDAR_LIST_PATH: &str = "/calendar/v3/users/me/calendarList";
/// calendarList.list's mask: what a listing line carries, and the page token.
const CALENDAR_LIST_FIELDS: &str = "items(id,summary,primary,accessRole,timeZone),nextPageToken";
/// calendarList.list's page size: the most entries whose worst-case answer
/// stays under `COMPACT_RESULT_CEILING` (an entry serializes to at most
/// 1 297 bytes; 100 of them and a page token to about 138 000).
const CALENDAR_LIST_PAGE: usize = 100;

/// The path Calendar lists a calendar's events below: `{CALENDARS_PATH}/{id}/events`.
const CALENDARS_PATH: &str = "/calendar/v3/calendars";
/// The calendar a tool reads when the caller names none.
const PRIMARY_CALENDAR: &str = "primary";
/// events.list's mask: what an events line carries, the page token and the
/// calendar's time zone. Of the attendees only the entry Google marks `self`
/// is read: the calendar read's own, the user's on `primary`.
const EVENTS_LIST_FIELDS: &str = "items(id,summary,start,end,location,status,organizer/email,\
                                  attendees(self,responseStatus)),nextPageToken,timeZone";
/// events.list's `maxAttendees`: past it Google returns only the calendar
/// read's own attendee entry, which is the one an events line answers.
const EVENTS_LIST_ATTENDEES: &str = "1";
/// `CalendarEvents`' `max`: the range and the default. A line serializes to
/// at most about 3 900 bytes, so 50 lines and the page's envelope stay under
/// `COMPACT_RESULT_CEILING` (about 203 000); 70 would not.
const EVENTS_MAX_MIN: u64 = 1;
const EVENTS_MAX_MAX: u64 = 50;
const EVENTS_MAX_DEFAULT: u64 = 50;
/// What the caller can narrow when a window's events are unreadable.
const EVENTS_HINT: &str = "ask for fewer events with `max`, or a shorter `from`/`to` window";

/// events.get's mask: one event's own text, its times, organizer, attendees,
/// recurrence and conference link.
const EVENT_GET_FIELDS: &str = "id,status,summary,description,location,start,end,\
                                organizer(email,displayName,self),\
                                attendees(email,displayName,responseStatus,optional,self),\
                                attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
                                conferenceData/entryPoints(entryPointType,uri)";
/// events.get's `maxAttendees`: past it Google returns only the calendar
/// read's own attendee entry and marks the event `attendeesOmitted`. It bounds the read
/// and the answer alike.
const ATTENDEES_MAX: usize = 50;
/// The most UTF-8 bytes of one event's text field (`summary`, `location`, the
/// organizer's name, a recurrence line, the conference link) copied into an
/// answer.
const EVENT_TEXT_MAX_BYTES: usize = 1000;
/// The most recurrence lines an event answers; past it `recurrence_truncated`.
const RECURRENCE_MAX: usize = 20;
/// The attendee responses Google writes, and the names they are counted under.
const RESPONSES: &[(&str, &str)] = &[
    ("accepted", "accepted"),
    ("declined", "declined"),
    ("tentative", "tentative"),
    ("needsAction", "needs_action"),
];

/// The path Calendar answers free/busy queries at.
const FREEBUSY_PATH: &str = "/calendar/v3/freeBusy";
/// The most calendars one free/busy query may name: Google's limit.
const FREEBUSY_CALENDARS_MAX: usize = 50;
/// What the caller can narrow when a free/busy answer is unreadable or past
/// the result ceiling.
const FREEBUSY_HINT: &str = "ask for a shorter `from`/`to` window, or fewer `calendars`";
/// The most calendar ids a free/busy refusal quotes from each side of a
/// mismatch between the calendars asked and those Google answered.
const QUOTED_IDS_MAX: usize = 5;

/// The most characters a calendar id argument may hold.
const CALENDAR_ID_MAX_CHARS: usize = 320;
/// The most bytes of a calendar id Google answers in `calendarList.list`.
/// Counted in bytes because it sizes the listing against the result ceiling;
/// no more than `CALENDAR_ID_MAX_CHARS`, as asserted below, so a listed id is
/// one `calendar` admits back.
const CALENDAR_ID_MAX_BYTES: usize = 320;
const _: () = assert!(CALENDAR_ID_MAX_BYTES <= CALENDAR_ID_MAX_CHARS);
/// The most characters an event id may hold: Google's base32hex ids are at
/// most 1 024 characters, an instance's suffix included. Being ASCII, an id's
/// characters are its bytes.
const EVENT_ID_MAX_CHARS: usize = 1024;
/// The most characters a time argument may hold. Times are passed to Google
/// as given; Google refuses one that is not RFC 3339 with an offset.
const TIME_MAX_CHARS: usize = 64;
/// The most UTF-8 bytes of text copied into one line of a listing.
const LISTING_TEXT_MAX_BYTES: usize = 300;
/// The most UTF-8 bytes of a value from Google's own vocabulary (a role, a
/// time zone, a timestamp) copied into an answer. No honest value reaches
/// it; it exists so an answer's size is bounded, and a cut is not flagged.
const SHORT_MAX_BYTES: usize = 64;

/// The Google API a tool reads, where a message to the caller differs by it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    Gmail,
    Calendar,
}

/// The API `tool` reads.
fn family(tool: Provided) -> Family {
    match tool {
        Provided::GmailLabels
        | Provided::GmailSearch
        | Provided::GmailRead
        | Provided::GmailThread => Family::Gmail,
        Provided::CalendarList
        | Provided::CalendarEvents
        | Provided::CalendarEvent
        | Provided::CalendarFreeBusy => Family::Calendar,
    }
}

impl Family {
    /// The API's name as the user knows it.
    fn name(self) -> &'static str {
        match self {
            Family::Gmail => "Gmail",
            Family::Calendar => "Calendar",
        }
    }
}

/// The sentence every Google 403 is answered with: Google answers an account
/// connected without the family's read scope this way, and the component does
/// not read Google's reason codes to tell that refusal from another.
fn scope_hint(family: Family) -> String {
    format!(
        "If Google names insufficient authentication scopes, the account was connected without \
         the {} read scope: run oauth-connect with that scope set and relay its steps.",
        family.name()
    )
}

/// The instance's `config` map, validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The `oauth_provider` every call names.
    pub provider: String,
    /// The `host[:port]` Gmail is called on.
    pub gmail_host: String,
    /// The `host[:port]` Calendar is called on.
    pub calendar_host: String,
}

impl Settings {
    /// Read the map through `get`. A missing key, an empty value, or a host
    /// containing `/` is an error naming the key: nothing a misconfigured
    /// instance answered would be right.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings, String> {
        Ok(Settings {
            provider: required(&get, "provider")?,
            gmail_host: host(&get, "gmail_host")?,
            calendar_host: host(&get, "calendar_host")?,
        })
    }
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

/// A GET on `host` for `account`.
fn get_call(
    settings: &Settings,
    host: &str,
    account: Option<&str>,
    path: String,
    query: Query,
) -> ApiCall {
    new_call(settings, "GET", host, account, path, query, None)
}

/// A POST on `host` for `account` carrying `body` as JSON text, which
/// `api-request` sends as `application/json` when no `Content-Type` is given.
fn post_json_call(
    settings: &Settings,
    host: &str,
    account: Option<&str>,
    path: String,
    query: Query,
    body: Value,
) -> ApiCall {
    let body = serde_json::to_string(&body).expect("a JSON value serializes");
    new_call(settings, "POST", host, account, path, query, Some(body))
}

/// One `api-request` call's arguments. `account`, when the caller named one,
/// is passed on every call a request makes, so a request never mixes
/// accounts. Every Google call adds `prettyPrint=false` to `query`: Google's
/// indentation would count against the read limit and the bus cap like any
/// other byte.
fn new_call(
    settings: &Settings,
    method: &str,
    host: &str,
    account: Option<&str>,
    path: String,
    mut query: Query,
    body: Option<String>,
) -> ApiCall {
    query.insert(
        "prettyPrint".to_string(),
        QueryValue::One("false".to_string()),
    );
    ApiCall {
        provider: settings.provider.clone(),
        account: account.map(str::to_string),
        method: method.to_string(),
        host: host.to_string(),
        path,
        query,
        headers: BTreeMap::new(),
        body,
    }
}

impl Tool for Provided {
    fn name(self) -> &'static str {
        Provided::name(self)
    }

    fn from_name(name: &str) -> Option<Provided> {
        Provided::from_name(name)
    }
}

/// A request's in-flight row, naming one of this component's tools.
pub type InFlight = brenn_provider_kit::InFlight<Provided>;

/// The Google account to act on, when the caller named one.
fn account(args: &Args) -> Result<Option<String>, String> {
    Ok(capped(args, "account", ACCOUNT_MAX_CHARS)?.map(str::to_string))
}

/// Whether `id` is 1 to `max` letters, digits, `_` and `-`: one path segment
/// as Google writes its token ids.
fn is_token(id: &str, max: usize) -> bool {
    (1..=max).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Whether `id` is a Gmail message, thread or label id.
fn is_gmail_id(id: &str) -> bool {
    is_token(id, ID_MAX_CHARS)
}

/// An id argument held to `valid`; a refusal says "`key` must be" `what`.
fn id_arg(
    args: &Args,
    key: &str,
    valid: fn(&str) -> bool,
    what: impl FnOnce() -> String,
) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(id)) if valid(id) => Ok(Some(id.clone())),
        Some(_) => Err(format!("`{key}` must be {}", what())),
    }
}

/// A Gmail id argument: one path segment of letters, digits, `_` and `-`.
fn gmail_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_gmail_id, || {
        format!("1 to {ID_MAX_CHARS} letters, digits, `_` or `-`")
    })
}

/// Whether `id` has a calendar id's shape, whatever its length: one path
/// segment `api-request` takes, so not `.` or `..`, with no `/`, `\`, `%` or
/// `"`, and no control or whitespace character. `#`, `@`, `?`, `.` and `+` are
/// content.
fn calendar_id_shape(id: &str) -> bool {
    id != "."
        && id != ".."
        && !id
            .chars()
            .any(|c| matches!(c, '/' | '\\' | '%' | '"') || c.is_control() || c.is_whitespace())
}

/// Whether `id` is a calendar id argument: 1 to `CALENDAR_ID_MAX_CHARS`
/// characters of a calendar id's shape. Arguments are counted in characters,
/// as the tool's `input_schema` states them; an answer's ids are held to
/// `is_listed_calendar_id`.
fn is_calendar_id(id: &str) -> bool {
    (1..=CALENDAR_ID_MAX_CHARS).contains(&id.chars().count()) && calendar_id_shape(id)
}

/// Whether `id` is a calendar id Google may answer in `calendarList.list`: 1
/// to `CALENDAR_ID_MAX_BYTES` bytes of a calendar id's shape. Counted in bytes
/// so a listing's size stays bounded against the result ceiling.
fn is_listed_calendar_id(id: &str) -> bool {
    (1..=CALENDAR_ID_MAX_BYTES).contains(&id.len()) && calendar_id_shape(id)
}

/// What `is_calendar_id` admits, as a refusal states it.
fn calendar_id_rule() -> String {
    format!(
        "a calendar id of 1 to {CALENDAR_ID_MAX_CHARS} characters with no `/`, `\\`, `%`, \
         `\"`, space or control character"
    )
}

/// What `is_calendar_id` admits, as the `pattern` an `input_schema` states it.
/// `\x00-\x1f` with `\x7f-\x9f` is `char::is_control`, and ECMA-262's `\s`
/// is Unicode whitespace less U+0085 (a control character, refused anyway)
/// plus U+FEFF. Two differences remain, both leaving the schema narrower than
/// the parser: an id holding U+FEFF is refused, and, the quantifier counting
/// UTF-16 units without the `u` flag, an id of characters outside the Basic
/// Multilingual Plane is admitted only up to half the length. Tests compare
/// this text with the spec's and never run it as a regex, so these claims
/// about what it matches are checked by reading, not by a test.
#[cfg(test)]
fn calendar_id_pattern() -> String {
    format!(r#"^(?!\.\.?$)[^\s/\\%"\x00-\x1f\x7f-\x9f]{{1,{CALENDAR_ID_MAX_CHARS}}}$"#)
}

/// A calendar id argument, refused here rather than by `api-request` so a bad
/// id is the caller's to fix.
fn calendar_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_calendar_id, calendar_id_rule)
}

/// Whether `id` is an event id as Google writes them: 1 to
/// `EVENT_ID_MAX_CHARS` letters, digits, `_` and `-`.
fn is_event_id(id: &str) -> bool {
    is_token(id, EVENT_ID_MAX_CHARS)
}

/// An event id argument: one path segment of letters, digits, `_` and `-`.
fn event_id(args: &Args, key: &str) -> Result<Option<String>, String> {
    id_arg(args, key, is_event_id, || {
        format!("1 to {EVENT_ID_MAX_CHARS} letters, digits, `_` or `-`")
    })
}

/// A time argument, passed to Google as given.
fn time(args: &Args, key: &str) -> Result<Option<String>, String> {
    Ok(capped(args, key, TIME_MAX_CHARS)?.map(str::to_string))
}

/// The `calendars` argument: 1 to `FREEBUSY_CALENDARS_MAX` distinct calendar
/// ids, each as `calendar_id` bounds it; absent is `primary` alone. Google
/// answers one entry per distinct id, so a repeated id is refused.
fn calendars(args: &Args) -> Result<Vec<String>, String> {
    let Some(value) = args.get("calendars") else {
        return Ok(vec![PRIMARY_CALENDAR.to_string()]);
    };
    let ids = match value {
        Value::Array(ids) if (1..=FREEBUSY_CALENDARS_MAX).contains(&ids.len()) => ids,
        _ => {
            return Err(format!(
                "`calendars` must be an array of 1 to {FREEBUSY_CALENDARS_MAX} calendar ids"
            ));
        }
    };
    let mut out: Vec<String> = Vec::with_capacity(ids.len());
    for id in ids {
        match id {
            Value::String(id) if is_calendar_id(id) => {
                if out.contains(id) {
                    return Err(format!("`calendars` names {id:?} twice"));
                }
                out.push(id.clone());
            }
            _ => {
                return Err(format!(
                    "each of `calendars` must be {}",
                    calendar_id_rule()
                ));
            }
        }
    }
    Ok(out)
}

/// The `max_chars` argument: `MAX_CHARS_MIN` to `MAX_CHARS_MAX`, default
/// `MAX_CHARS_DEFAULT`.
fn max_chars(args: &Args) -> Result<usize, String> {
    let n = integer(args, "max_chars", MAX_CHARS_MIN, MAX_CHARS_MAX)?.unwrap_or(MAX_CHARS_DEFAULT);
    Ok(usize::try_from(n).expect("max_chars is at most MAX_CHARS_MAX"))
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
                api: thread_call(&thread, THREAD_HEADERS, settings),
            })
        }
        Provided::CalendarList => {
            let list = CalendarListArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: calendar_list_call(&list, settings),
            })
        }
        Provided::CalendarEvents => {
            let events = EventsArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: events_list_call(&events, settings),
            })
        }
        Provided::CalendarEvent => {
            let event = EventArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: event_get_call(&event, settings),
            })
        }
        Provided::CalendarFreeBusy => {
            let free_busy = FreeBusyArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: free_busy_call(&free_busy, settings),
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
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "account"];

    fn parse(args: &Value) -> Result<LabelsArgs, String> {
        let map = object(args, Self::KEYS)?;
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
    get_call(
        settings,
        &settings.gmail_host,
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
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["q", "max", "page", "spam_trash", "account"];

    fn parse(args: &Value) -> Result<SearchArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(SearchArgs {
            q: capped(map, "q", QUERY_MAX_CHARS)?
                .ok_or("`q` is required")?
                .to_string(),
            max: integer(map, "max", SEARCH_MAX_MIN, SEARCH_MAX_MAX)?.unwrap_or(SEARCH_MAX_DEFAULT),
            page: capped(map, "page", PAGE_MAX_CHARS)?.map(str::to_string),
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
    get_call(
        settings,
        &settings.gmail_host,
        args.account.as_deref(),
        MESSAGES_PATH.to_string(),
        query,
    )
}

/// messages.get of one message's metadata: the search line's headers, labels
/// and snippet, no body.
fn message_metadata_call(id: &str, account: Option<&str>, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        &settings.gmail_host,
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
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "max_chars", "account"];

    fn parse(args: &Value) -> Result<ReadArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(ReadArgs {
            id: gmail_id(map, "id")?.ok_or("`id` is required")?,
            max_chars: max_chars(map)?,
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
    get_call(
        settings,
        &settings.gmail_host,
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
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "max", "account"];

    fn parse(args: &Value) -> Result<ThreadArgs, String> {
        let map = object(args, Self::KEYS)?;
        let max =
            integer(map, "max", THREAD_MAX_MIN, THREAD_MAX_MAX)?.unwrap_or(THREAD_MAX_DEFAULT);
        Ok(ThreadArgs {
            id: gmail_id(map, "id")?.ok_or("`id` is required")?,
            max: usize::try_from(max).expect("max is at most 100"),
            account: account(map)?,
        })
    }
}

/// threads.get of one thread's messages' metadata: `headers`, labels and
/// snippets, no bodies.
fn thread_call(args: &ThreadArgs, headers: &[&str], settings: &Settings) -> ApiCall {
    get_call(
        settings,
        &settings.gmail_host,
        args.account.as_deref(),
        format!("{THREADS_PATH}/{}", args.id),
        metadata_query(THREAD_FIELDS, headers),
    )
}

/// `CalendarList`'s arguments.
struct CalendarListArgs {
    page: Option<String>,
    account: Option<String>,
}

impl CalendarListArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["page", "account"];

    fn parse(args: &Value) -> Result<CalendarListArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(CalendarListArgs {
            page: capped(map, "page", PAGE_MAX_CHARS)?.map(str::to_string),
            account: account(map)?,
        })
    }
}

/// calendarList.list, one page of at most `CALENDAR_LIST_PAGE` entries.
fn calendar_list_call(args: &CalendarListArgs, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("maxResults", CALENDAR_LIST_PAGE.to_string()),
        ("fields", CALENDAR_LIST_FIELDS.to_string()),
    ]);
    if let Some(page) = &args.page {
        query.insert("pageToken".to_string(), QueryValue::One(page.clone()));
    }
    get_call(
        settings,
        &settings.calendar_host,
        args.account.as_deref(),
        CALENDAR_LIST_PATH.to_string(),
        query,
    )
}

/// `CalendarEvents`' arguments.
struct EventsArgs {
    calendar: String,
    from: String,
    to: String,
    q: Option<String>,
    max: usize,
    page: Option<String>,
    account: Option<String>,
}

impl EventsArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["calendar", "from", "to", "q", "max", "page", "account"];

    fn parse(args: &Value) -> Result<EventsArgs, String> {
        let map = object(args, Self::KEYS)?;
        let max =
            integer(map, "max", EVENTS_MAX_MIN, EVENTS_MAX_MAX)?.unwrap_or(EVENTS_MAX_DEFAULT);
        Ok(EventsArgs {
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            from: time(map, "from")?.ok_or("`from` is required")?,
            to: time(map, "to")?.ok_or("`to` is required")?,
            q: capped(map, "q", QUERY_MAX_CHARS)?.map(str::to_string),
            max: usize::try_from(max).expect("max is at most 50"),
            page: capped(map, "page", PAGE_MAX_CHARS)?.map(str::to_string),
            account: account(map)?,
        })
    }
}

/// events.list of the window `from`..`to`, recurring events expanded into
/// their instances in start order, one page of at most `max`. The calendar id
/// goes into the path as it is; `api-request` percent-encodes it.
fn events_list_call(args: &EventsArgs, settings: &Settings) -> ApiCall {
    let mut query = query([
        ("timeMin", args.from.clone()),
        ("timeMax", args.to.clone()),
        ("singleEvents", "true".to_string()),
        ("orderBy", "startTime".to_string()),
        ("maxResults", args.max.to_string()),
        ("maxAttendees", EVENTS_LIST_ATTENDEES.to_string()),
        ("fields", EVENTS_LIST_FIELDS.to_string()),
    ]);
    if let Some(q) = &args.q {
        query.insert("q".to_string(), QueryValue::One(q.clone()));
    }
    if let Some(page) = &args.page {
        query.insert("pageToken".to_string(), QueryValue::One(page.clone()));
    }
    get_call(
        settings,
        &settings.calendar_host,
        args.account.as_deref(),
        format!("{CALENDARS_PATH}/{}/events", args.calendar),
        query,
    )
}

/// `CalendarEvent`'s arguments.
struct EventArgs {
    id: String,
    calendar: String,
    max_chars: usize,
    account: Option<String>,
}

impl EventArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "calendar", "max_chars", "account"];

    fn parse(args: &Value) -> Result<EventArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(EventArgs {
            id: event_id(map, "id")?.ok_or("`id` is required")?,
            calendar: calendar_id(map, "calendar")?.unwrap_or_else(|| PRIMARY_CALENDAR.to_string()),
            max_chars: max_chars(map)?,
            account: account(map)?,
        })
    }
}

/// events.get of one event, at most `ATTENDEES_MAX` attendees listed. The
/// calendar id goes into the path as it is; `api-request` percent-encodes it.
fn event_get_call(args: &EventArgs, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        &settings.calendar_host,
        args.account.as_deref(),
        format!("{CALENDARS_PATH}/{}/events/{}", args.calendar, args.id),
        query([
            ("maxAttendees", ATTENDEES_MAX.to_string()),
            ("fields", EVENT_GET_FIELDS.to_string()),
        ]),
    )
}

/// `CalendarFreeBusy`'s arguments.
struct FreeBusyArgs {
    from: String,
    to: String,
    calendars: Vec<String>,
    account: Option<String>,
}

impl FreeBusyArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["from", "to", "calendars", "account"];

    fn parse(args: &Value) -> Result<FreeBusyArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(FreeBusyArgs {
            from: time(map, "from")?.ok_or("`from` is required")?,
            to: time(map, "to")?.ok_or("`to` is required")?,
            calendars: calendars(map)?,
            account: account(map)?,
        })
    }
}

/// freebusy.query for the window `from`..`to` over `calendars`. The POST
/// changes nothing; `api-request` admits it only for a stamped call.
// TODO(correctness-free-busy-nested-call-can-exceed-the-args-cap): the body
// carries every id uncut and is escaped again inside the nested call's args,
// so fifty maximal ids of four-byte characters can exceed the host's 64 KiB
// args cap. Such an admitted `calendars` is answered `internal`, not
// `invalid_args` naming `calendars`.
fn free_busy_call(args: &FreeBusyArgs, settings: &Settings) -> ApiCall {
    let items: Vec<Value> = args
        .calendars
        .iter()
        .map(|id| json!({ "id": id }))
        .collect();
    post_json_call(
        settings,
        &settings.calendar_host,
        args.account.as_deref(),
        FREEBUSY_PATH.to_string(),
        Query::new(),
        json!({
            "timeMin": args.from,
            "timeMax": args.to,
            "items": items,
            "calendarExpansionMax": args.calendars.len(),
        }),
    )
}

/// labels.list for the names of the user labels an answer carries.
fn label_names_call(account: Option<&str>, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        &settings.gmail_host,
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

/// `text`'s first `max_chars` characters, and whether any were left out.
fn chars_prefix(text: &str, max_chars: usize) -> (&str, bool) {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => (&text[..end], true),
        None => (text, false),
    }
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

/// `why`'s rendering as copied into an answer: through `plain`, cut to
/// `GOOGLE_MESSAGE_MAX_BYTES`, since a refusal's detail is the host's and
/// unbounded.
fn quoted(why: &Unreadable, estimate: Option<u64>) -> String {
    cut(&plain(&why.render(estimate)), GOOGLE_MESSAGE_MAX_BYTES).0
}

/// A 4xx/5xx answer, its detail as `google_error_detail` writes it. 400 and
/// 404 are the caller's arguments; 403 is Google's refusal, answered with the
/// family's scope hint; every other status (401 after `api-request`'s retry,
/// 429, 5xx) is the wrapper's or Google's.
// TODO(errhandling-gmail-403-always-denied-with-scope-hint): a quota or
// API-not-enabled 403 is answered `denied` with the scope hint.
// TODO(errhandling-gmail-4xx-on-fixed-request-blamed-on-caller): a 400/404 on
// a request built from constants alone is answered `invalid_args`.
fn google_error(status: u64, body: &str, family: Family) -> Failure {
    let kind = match status {
        400 | 404 => ErrorKind::InvalidArgs,
        403 => ErrorKind::Denied,
        _ => ErrorKind::Internal,
    };
    let mut detail = google_error_detail(status, body);
    if kind == ErrorKind::Denied {
        let stop = if detail.ends_with('.') { "" } else { "." };
        detail = format!("{detail}{stop} {}", scope_hint(family));
    }
    Failure { kind, detail }
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step. Every answer completed here is held to the result
/// ceiling; a relayed credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
    let body = match read_api_result(outcome, &state.target) {
        Ok(Answer::Body(body)) => body,
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return unreadable(state, why, settings),
        // A message deleted between the listing and its read is listed
        // unavailable, not the search's failure.
        Ok(Answer::Error { status: 404, .. })
            if matches!(
                (state.tool, state.step),
                (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET)
            ) =>
        {
            return settle(state.tool, search_message(state, Read::Deleted, settings));
        }
        Ok(Answer::Error { status, body }) => {
            return Next::Fail(google_error(status, &body, family(state.tool)));
        }
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
                search_message(state, Read::Body(&body), settings)
            }
            (Provided::GmailRead, 1) => read_full(state, &body, settings),
            (Provided::GmailRead, STRUCTURE_STEP) => read_structure(state, &body, settings),
            (Provided::GmailThread, 1 | THREAD_NARROW_STEP) => thread_read(state, &body, settings),
            (Provided::CalendarList, 1) => compact_calendar_list(&body).map(Next::Complete),
            (Provided::CalendarEvents, 1) => stored(state, EventsArgs::parse)
                .and_then(|args| compact_events(&body, args.max))
                .map(Next::Complete),
            (Provided::CalendarEvent, 1) => stored(state, EventArgs::parse)
                .and_then(|args| compact_event(&body, &args))
                .map(Next::Complete),
            (Provided::CalendarFreeBusy, 1) => stored(state, FreeBusyArgs::parse)
                .and_then(|args| compact_free_busy(&body, &args))
                .map(Next::Complete),
            (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
                named(state, &body)
            }
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested read that was cut or refused, decided by the step's shape: a
/// per-message read of a search page is listed unavailable and the page goes
/// on; a message's or a thread's first read is asked for again narrower,
/// carrying why; a window's events or free/busy fail with why and what to
/// narrow; every other step fails with why.
fn unreadable(state: &InFlight, why: Unreadable, settings: &Settings) -> Next {
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET) => {
                search_message(state, Read::Unreadable(why), settings)
            }
            (Provided::GmailRead, 1) => stored(state, ReadArgs::parse).map(|args| Next::Call {
                call: Call {
                    step: STRUCTURE_STEP,
                    api: read_call(&args, Some(STRUCTURE_FIELDS), settings),
                },
                derived: why.to_derived(),
            }),
            (Provided::GmailThread, 1) => stored(state, ThreadArgs::parse).map(|args| Next::Call {
                call: Call {
                    step: THREAD_NARROW_STEP,
                    api: thread_call(&args, THREAD_NARROW_HEADERS, settings),
                },
                derived: why.to_derived(),
            }),
            (Provided::GmailLabels, 1)
            | (Provided::GmailSearch, 1)
            | (Provided::GmailRead, STRUCTURE_STEP)
            | (Provided::GmailThread, THREAD_NARROW_STEP)
            | (Provided::CalendarList | Provided::CalendarEvent, 1)
            | (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
                Err(why.failure(None))
            }
            (Provided::CalendarEvents, 1) => Err(why.failure(Some(EVENTS_HINT))),
            (Provided::CalendarFreeBusy, 1) => Err(why.failure(Some(FREEBUSY_HINT))),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling — after `GmailRead`'s text or `CalendarEvent`'s
/// description is cut, or `GmailThread`'s oldest messages dropped, to fit it;
/// a `CalendarFreeBusy` answer past it is refused with what to narrow.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let (value, hint) = match tool {
                Provided::GmailRead => (fit_text(value, "text", COMPACT_RESULT_CEILING), None),
                Provided::CalendarEvent => {
                    (fit_text(value, "description", COMPACT_RESULT_CEILING), None)
                }
                Provided::GmailThread => (fit_thread(value, COMPACT_RESULT_CEILING), None),
                Provided::CalendarFreeBusy => (value, Some(FREEBUSY_HINT)),
                Provided::GmailLabels
                | Provided::GmailSearch
                | Provided::CalendarList
                | Provided::CalendarEvents => (value, None),
            };
            bounded(value, COMPACT_RESULT_CEILING, hint)
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
    next_page(&mut derived, body, "messages.list")?;
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

/// What one listed message's read gave a search page.
enum Read<'a> {
    /// Google's metadata answer.
    Body(&'a Value),
    /// Google's 404: deleted between the listing and the read.
    Deleted,
    /// Cut or refused.
    Unreadable(Unreadable),
}

/// One listed message's read recorded in `derived` — a line from its
/// metadata, or an `unavailable` entry with its `id` and the `reason` — then
/// the next listed message is read, or the search is finished.
fn search_message(state: &InFlight, read: Read, settings: &Settings) -> Result<Next, Failure> {
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
    let unavailable = |reason: String| ("unavailable", json!({ "id": id, "reason": reason }));
    let (key, entry) = match read {
        Read::Body(body) => ("lines", search_line(body, id)?),
        Read::Deleted => unavailable("deleted (Google 404)".to_string()),
        Read::Unreadable(why) => unavailable(quoted(&why, None)),
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
    let mut notes = notes(&extracted, false);
    if text.is_empty() && extracted.text_by_reference {
        notes.push(
            "the body is an attachment reference, not fetched; it is listed under attachments"
                .to_string(),
        );
    }
    put_cut_text(&mut out, "text", &text, args.max_chars);
    out.insert("text_source".to_string(), json!(text_source));
    out.insert("attachments".to_string(), attachments(&extracted));
    if !notes.is_empty() {
        out.insert("decoding_notes".to_string(), json!(notes));
    }
    finish(Value::Object(out), args.account.as_deref(), settings)
}

/// messages.get (structure) read, after the full read was cut or refused:
/// the headers, labels, snippet and attachments, `text` null and
/// `text_omitted` saying why; then the user labels named.
fn read_structure(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ReadArgs::parse)?;
    let api = "messages.get";
    let why = Unreadable::from_derived(&state.derived)?;
    let mut out = asked_message_fields(body, &args.id, READ_HEADERS, api)?;
    let estimate = match body.get("sizeEstimate") {
        None => None,
        Some(estimate) => Some(estimate.as_u64().ok_or_else(|| shape(api))?),
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
            "{}; headers and attachments are listed, the body is not",
            quoted(&why, estimate)
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

/// `text`, `plain`ed and cut at `max_chars` characters, under `key`, with
/// `{key}_chars` its whole length and `{key}_truncated` set when it was cut.
fn put_cut_text(out: &mut Args, key: &str, text: &str, max_chars: usize) {
    let text = plain(text);
    let (cut, truncated) = chars_prefix(&text, max_chars);
    if truncated {
        out.insert(format!("{key}_truncated"), json!(true));
    }
    out.insert(key.to_string(), json!(cut));
    out.insert(format!("{key}_chars"), json!(text.chars().count()));
}

/// `answer` with its text `key` cut to the longest prefix with which the
/// whole answer serializes within `ceiling`, and `{key}_truncated` set, when
/// it does not fit as it is. An answer that fits, or has no such text, is
/// unchanged; one that does not fit with an empty text is left for the ceiling
/// to refuse.
fn fit_text(mut answer: Value, key: &str, ceiling: usize) -> Value {
    if serialized_len(&answer) <= ceiling {
        return answer;
    }
    let Some(Value::String(text)) = answer.get_mut(key).map(std::mem::take) else {
        return answer;
    };
    answer[key] = json!("");
    answer[format!("{key}_truncated")] = json!(true);
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
    answer[key] = json!(text[..end]);
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

/// The array `derived[key]` that an earlier step of this request wrote.
fn derived_list(derived: &Args, key: &str) -> Result<Vec<Value>, Failure> {
    match derived.get(key) {
        Some(Value::Array(items)) => Ok(items.clone()),
        _ => Err(internal(format!("in-flight row carries no `{key}` list"))),
    }
}

/// `GmailSearch`'s answer from what its steps gathered in `derived`:
/// `messages` (empty when nothing matched), Google's `next_page` and
/// `result_size_estimate` when it gave them, and `unavailable` — each `id`
/// and `reason` — when a listed message could not be read.
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
/// sent them. Then the user labels are named. At the narrower step the
/// messages carry no `to` or `cc`, and `recipients_omitted` says why.
fn thread_read(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, ThreadArgs::parse)?;
    let api = "threads.get";
    let (headers, recipients_omitted) = if state.step == THREAD_NARROW_STEP {
        let why = Unreadable::from_derived(&state.derived)?;
        (THREAD_NARROW_HEADERS, Some(quoted(&why, None)))
    } else {
        (THREAD_HEADERS, None)
    };
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
        .map(|message| thread_message(message, headers, subject, api))
        .collect::<Result<Vec<_>, _>>()?;
    out.insert("message_count".to_string(), json!(messages.len()));
    if omitted > 0 {
        out.insert("messages_omitted".to_string(), json!(omitted));
    }
    if let Some(why) = recipients_omitted {
        out.insert("recipients_omitted".to_string(), json!(why));
    }
    out.insert("messages".to_string(), Value::Array(answered));
    finish(Value::Object(out), args.account.as_deref(), settings)
}

/// One message of a thread: `message_fields` with `headers`,
/// `THREAD_SUBJECT` among them only when it differs from the thread's
/// `subject`, and the snippet through `snippet_value`. `THREAD_FIELDS` asks
/// for no `threadId`, so none is answered.
fn thread_message(
    message: &Value,
    headers: &[&str],
    subject: Option<&str>,
    api: &str,
) -> Result<Value, Failure> {
    let differs = raw_header(message, THREAD_SUBJECT, api)? != subject;
    let headers: Vec<&str> = headers
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

/// One compacted object of a Calendar answer: the fields copied so far, and
/// the names of those cut at a bound that is flagged.
struct Compact<'a> {
    item: &'a Args,
    api: &'a str,
    out: Args,
    truncated: Vec<String>,
}

impl<'a> Compact<'a> {
    fn of(item: &'a Value, api: &'a str) -> Result<Compact<'a>, Failure> {
        let Value::Object(item) = item else {
            return Err(shape(api));
        };
        Ok(Compact {
            item,
            api,
            out: Args::new(),
            truncated: Vec::new(),
        })
    }

    /// Google's string `key`, when present; any other value is Google's
    /// fault.
    fn string(&self, key: &str) -> Result<Option<&'a str>, Failure> {
        match self.item.get(key) {
            None => Ok(None),
            Some(Value::String(value)) => Ok(Some(value)),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's id `key` under `name`, required and held to `valid`: an id is
    /// a handle the caller passes back, so it is never cut.
    fn id(&mut self, key: &str, name: &str, valid: fn(&str) -> bool) -> Result<(), Failure> {
        match self.string(key)? {
            Some(id) if valid(id) => {
                self.out.insert(name.to_string(), json!(id));
                Ok(())
            }
            _ => Err(shape(self.api)),
        }
    }

    /// Google's text `key` under `name`, through `plain` and cut to `bound`
    /// UTF-8 bytes; a cut is listed in `fields_truncated`.
    fn text(&mut self, key: &str, name: &str, bound: usize) -> Result<(), Failure> {
        if let Some(value) = self.string(key)? {
            self.put_text(name, value, bound);
        }
        Ok(())
    }

    /// `value` under `name`, through `plain` and cut to `bound` UTF-8 bytes;
    /// a cut is listed in `fields_truncated`.
    fn put_text(&mut self, name: &str, value: &str, bound: usize) {
        let (value, cut) = cut(&plain(value), bound);
        if cut {
            self.truncated.push(name.to_string());
        }
        self.out.insert(name.to_string(), json!(value));
    }

    /// Google's object `key`, when present; any other value is Google's
    /// fault.
    fn object(&self, key: &str) -> Result<Option<Compact<'a>>, Failure> {
        match self.item.get(key) {
            None => Ok(None),
            Some(value) => Compact::of(value, self.api).map(Some),
        }
    }

    /// Google's list `key`; Google omits a list with nothing in it, so
    /// absent is empty. Any other value is Google's fault.
    fn array(&self, key: &str) -> Result<&'a [Value], Failure> {
        match self.item.get(key) {
            None => Ok(&[]),
            Some(Value::Array(items)) => Ok(items),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's vocabulary `key` under `name`; see `put_short`.
    fn short(&mut self, key: &str, name: &str) -> Result<(), Failure> {
        if let Some(value) = self.string(key)? {
            self.put_short(name, value);
        }
        Ok(())
    }

    /// Vocabulary `value` under `name`, through `plain` and cut to
    /// `SHORT_MAX_BYTES` unflagged.
    fn put_short(&mut self, name: &str, value: &str) {
        let value = cut(&plain(value), SHORT_MAX_BYTES).0;
        self.out.insert(name.to_string(), json!(value));
    }

    /// Google's flag `key`: absent is `false`; a non-bool is Google's fault.
    fn flag(&self, key: &str) -> Result<bool, Failure> {
        match self.item.get(key) {
            None => Ok(false),
            Some(Value::Bool(flag)) => Ok(*flag),
            Some(_) => Err(shape(self.api)),
        }
    }

    /// Google's flag `key` under `name`, only when it is `true`.
    fn when_true(&mut self, key: &str, name: &str) -> Result<(), Failure> {
        if self.flag(key)? {
            self.out.insert(name.to_string(), json!(true));
        }
        Ok(())
    }

    /// Google's event time `key` under the same name, as Google gave it:
    /// `{ "date_time", "time_zone"? }` for a timed event, `{ "date" }` for an
    /// all-day one. One with both or neither is Google's fault.
    fn when(&mut self, key: &str) -> Result<(), Failure> {
        let Some(mut time) = self.object(key)? else {
            return Ok(());
        };
        time.short("dateTime", "date_time")?;
        time.short("timeZone", "time_zone")?;
        time.short("date", "date")?;
        if time.out.contains_key("date_time") == time.out.contains_key("date") {
            return Err(shape(self.api));
        }
        self.out.insert(key.to_string(), time.done());
        Ok(())
    }

    /// The object, with `fields_truncated` sorted when a field was cut.
    fn done(mut self) -> Value {
        if !self.truncated.is_empty() {
            self.truncated.sort();
            self.out
                .insert("fields_truncated".to_string(), json!(self.truncated));
        }
        Value::Object(self.out)
    }
}

/// Google's `nextPageToken` into `out` as `next_page`, when Google gave one;
/// one that `page` would refuse back — not a string, blank, or longer than
/// `PAGE_MAX_BYTES` — is Google's fault.
fn next_page(out: &mut Args, body: &Args, api: &str) -> Result<(), Failure> {
    match body.get("nextPageToken") {
        None => {}
        Some(Value::String(token)) if token.len() <= PAGE_MAX_BYTES && !token.trim().is_empty() => {
            out.insert("next_page".to_string(), json!(token));
        }
        Some(_) => return Err(shape(api)),
    }
    Ok(())
}

/// calendarList.list as `{ "calendars": [...], "next_page"? }` in Google's
/// order: each entry's `id`, `summary`, `primary` (only when true),
/// `access_role` and `time_zone`. Google omits `items` when there are none,
/// which is an empty list; more entries than the page size asked for is
/// Google's fault.
fn compact_calendar_list(body: &Value) -> Result<Value, Failure> {
    let api = "calendarList.list";
    let mut page = Compact::of(body, api)?;
    let items = page.array("items")?;
    if items.len() > CALENDAR_LIST_PAGE {
        return Err(internal(format!(
            "Google {api} answered {} items for maxResults {CALENDAR_LIST_PAGE}",
            items.len()
        )));
    }
    let calendars = items
        .iter()
        .map(|item| calendar_entry(item, api))
        .collect::<Result<Vec<_>, _>>()?;
    page.out
        .insert("calendars".to_string(), Value::Array(calendars));
    next_page(&mut page.out, page.item, api)?;
    Ok(page.done())
}

/// One calendar of the user's list.
fn calendar_entry(item: &Value, api: &str) -> Result<Value, Failure> {
    let mut entry = Compact::of(item, api)?;
    entry.id("id", "id", is_listed_calendar_id)?;
    entry.text("summary", "summary", LISTING_TEXT_MAX_BYTES)?;
    entry.when_true("primary", "primary")?;
    entry.short("accessRole", "access_role")?;
    entry.short("timeZone", "time_zone")?;
    Ok(entry.done())
}

/// events.list as `{ "events": [...], "time_zone"?, "next_page"? }` in
/// Google's order (start time). Google omits `items` when there are none,
/// which is an empty list; more events than `max` is Google's fault.
fn compact_events(body: &Value, max: usize) -> Result<Value, Failure> {
    let api = "events.list";
    let mut page = Compact::of(body, api)?;
    let items = page.array("items")?;
    if items.len() > max {
        return Err(internal(format!(
            "Google {api} answered {} events for maxResults {max}",
            items.len()
        )));
    }
    let events = items
        .iter()
        .map(|item| event_line(item, api))
        .collect::<Result<Vec<_>, _>>()?;
    page.out.insert("events".to_string(), Value::Array(events));
    page.short("timeZone", "time_zone")?;
    next_page(&mut page.out, page.item, api)?;
    Ok(page.done())
}

/// One event of a window: `id`, `summary`, `start`, `end`, `location`,
/// `status`, `organizer` (the organizer's email) and `self_response` (the
/// calendar read's own `responseStatus`, when that calendar is an attendee).
fn event_line(item: &Value, api: &str) -> Result<Value, Failure> {
    let mut line = Compact::of(item, api)?;
    line.id("id", "id", is_event_id)?;
    line.text("summary", "summary", LISTING_TEXT_MAX_BYTES)?;
    line.when("start")?;
    line.when("end")?;
    line.text("location", "location", LISTING_TEXT_MAX_BYTES)?;
    line.short("status", "status")?;
    if let Some(organizer) = line.object("organizer")?
        && let Some(email) = organizer.string("email")?
    {
        line.put_text("organizer", email, ACCOUNT_MAX_BYTES);
    }
    if let Some(response) = self_response(&line)? {
        line.put_short("self_response", response);
    }
    Ok(line.done())
}

/// The `responseStatus` of the attendee entry marked `self`, when Google
/// returned one; every other entry is ignored. Two `self` entries are
/// Google's fault.
fn self_response<'a>(event: &Compact<'a>) -> Result<Option<&'a str>, Failure> {
    let mut found = None;
    for attendee in event.array("attendees")? {
        let attendee = Compact::of(attendee, event.api)?;
        if !attendee.flag("self")? {
            continue;
        }
        if found.is_some() {
            return Err(shape(event.api));
        }
        found = Some(attendee.string("responseStatus")?);
    }
    Ok(found.flatten())
}

/// events.get as one event: `id`, `summary`, `description` (cut at
/// `max_chars` characters, `description_chars` its whole length), `start`,
/// `end`, `location`, `status`, `organizer`, `self_response`, `attendees`
/// with their counts or `attendees_omitted`, `recurrence`,
/// `recurring_event_id` and `conference`. An event whose id is not the one
/// asked for is Google's fault.
fn compact_event(body: &Value, args: &EventArgs) -> Result<Value, Failure> {
    let api = "events.get";
    let mut event = Compact::of(body, api)?;
    if event.string("id")? != Some(args.id.as_str()) {
        return Err(shape(api));
    }
    event.id("id", "id", is_event_id)?;
    event.text("summary", "summary", EVENT_TEXT_MAX_BYTES)?;
    if let Some(description) = event.string("description")? {
        put_cut_text(&mut event.out, "description", description, args.max_chars);
    }
    event.when("start")?;
    event.when("end")?;
    event.text("location", "location", EVENT_TEXT_MAX_BYTES)?;
    event.short("status", "status")?;
    if let Some(mut organizer) = event.object("organizer")? {
        organizer.text("email", "email", ACCOUNT_MAX_BYTES)?;
        organizer.text("displayName", "name", EVENT_TEXT_MAX_BYTES)?;
        organizer.when_true("self", "self")?;
        event.out.insert("organizer".to_string(), organizer.done());
    }
    if let Some(response) = self_response(&event)? {
        event.put_short("self_response", response);
    }
    attendees(&mut event)?;
    recurrence(&mut event)?;
    if event.item.contains_key("recurringEventId") {
        event.id("recurringEventId", "recurring_event_id", is_event_id)?;
    }
    if let Some(link) = conference(&event)? {
        event.put_text("conference", link, EVENT_TEXT_MAX_BYTES);
    }
    Ok(event.done())
}

/// The event's `attendees` as Google sent them, each `email`, `name`,
/// `response` and `optional` (only when true). With the whole list, its
/// length as `attendees_total` and its responses counted in
/// `attendee_responses`; when Google left attendees out, `attendees_omitted`
/// instead. More than `ATTENDEES_MAX` entries, or a response outside Google's
/// four in a list being counted, is Google's fault.
fn attendees(event: &mut Compact<'_>) -> Result<(), Failure> {
    let api = event.api;
    let attendees = event.array("attendees")?;
    if attendees.len() > ATTENDEES_MAX {
        return Err(shape(api));
    }
    let omitted = event.flag("attendeesOmitted")?;
    let mut counts = [0_usize; RESPONSES.len()];
    let mut entries = Vec::with_capacity(attendees.len());
    for attendee in attendees {
        let mut entry = Compact::of(attendee, api)?;
        entry.text("email", "email", ACCOUNT_MAX_BYTES)?;
        entry.text("displayName", "name", LISTING_TEXT_MAX_BYTES)?;
        let response = entry.string("responseStatus")?;
        if let Some(response) = response {
            entry.put_short("response", response);
        }
        entry.when_true("optional", "optional")?;
        if !omitted {
            let counted = RESPONSES
                .iter()
                .position(|(google, _)| Some(*google) == response)
                .ok_or_else(|| shape(api))?;
            counts[counted] += 1;
        }
        entries.push(entry.done());
    }
    if omitted {
        event
            .out
            .insert("attendees_omitted".to_string(), json!(true));
    } else {
        event
            .out
            .insert("attendees_total".to_string(), json!(entries.len()));
        let responses: Args = RESPONSES
            .iter()
            .zip(counts)
            .map(|((_, name), count)| (name.to_string(), json!(count)))
            .collect();
        event
            .out
            .insert("attendee_responses".to_string(), Value::Object(responses));
    }
    event
        .out
        .insert("attendees".to_string(), Value::Array(entries));
    Ok(())
}

/// The event's first `RECURRENCE_MAX` recurrence lines, each through `plain`
/// and cut to `EVENT_TEXT_MAX_BYTES` (a cut lists `recurrence` in
/// `fields_truncated`), and `recurrence_truncated` past them.
fn recurrence(event: &mut Compact<'_>) -> Result<(), Failure> {
    let lines = event.array("recurrence")?;
    if lines.is_empty() {
        return Ok(());
    }
    let mut cut_any = false;
    let mut kept = Vec::with_capacity(lines.len().min(RECURRENCE_MAX));
    for line in lines.iter().take(RECURRENCE_MAX) {
        let Value::String(line) = line else {
            return Err(shape(event.api));
        };
        let (line, was_cut) = cut(&plain(line), EVENT_TEXT_MAX_BYTES);
        cut_any |= was_cut;
        kept.push(line);
    }
    if cut_any {
        event.truncated.push("recurrence".to_string());
    }
    event.out.insert("recurrence".to_string(), json!(kept));
    if lines.len() > RECURRENCE_MAX {
        event
            .out
            .insert("recurrence_truncated".to_string(), json!(true));
    }
    Ok(())
}

/// The event's conference link: its `hangoutLink`, else the `uri` of the
/// first `video` entry point of its `conferenceData`.
fn conference<'a>(event: &Compact<'a>) -> Result<Option<&'a str>, Failure> {
    if let Some(link) = event.string("hangoutLink")? {
        return Ok(Some(link));
    }
    let Some(data) = event.object("conferenceData")? else {
        return Ok(None);
    };
    for point in data.array("entryPoints")? {
        let point = Compact::of(point, event.api)?;
        if point.string("entryPointType")? == Some("video") {
            return point.string("uri");
        }
    }
    Ok(None)
}

/// freebusy.query as `{ "from"?, "to"?, "calendars": { <id>: { "busy"?,
/// "errors"? } } }`: Google's echo of the window, and per calendar asked its
/// busy intervals (`start`, `end`, as Google gave them) or the reasons Google
/// could not answer for it. The empty `busy` Google sends beside reasons is
/// left out, since it would read as free time. The calendars must be exactly
/// those asked, and the refusal otherwise names the ids on each side of the
/// difference; one with neither `busy` nor reasons is Google's fault. The
/// calendars are keyed by the ids as asked, uncut: fifty ids of
/// `CALENDAR_ID_MAX_CHARS` characters are at most 64 000 bytes of keys, and
/// the result ceiling's refusal in `settle` is what bounds the whole answer.
fn compact_free_busy(body: &Value, args: &FreeBusyArgs) -> Result<Value, Failure> {
    let api = "freebusy.query";
    let mut answer = Compact::of(body, api)?;
    answer.short("timeMin", "from")?;
    answer.short("timeMax", "to")?;
    let Some(Value::Object(google)) = answer.item.get("calendars") else {
        return Err(shape(api));
    };
    let unasked: Vec<&String> = google
        .keys()
        .filter(|id| !args.calendars.contains(*id))
        .collect();
    let unanswered: Vec<&String> = args
        .calendars
        .iter()
        .filter(|id| !google.contains_key(*id))
        .collect();
    if !(unasked.is_empty() && unanswered.is_empty()) {
        return Err(internal(format!(
            "unexpected Google {api} response shape: calendars answered but not asked: {}; \
             calendars asked but not answered: {}",
            quoted_ids(&unasked),
            quoted_ids(&unanswered)
        )));
    }
    let mut calendars = Args::new();
    for (id, calendar) in google {
        let calendar = Compact::of(calendar, api)?;
        let busy = calendar
            .array("busy")?
            .iter()
            .map(|interval| busy_interval(interval, api))
            .collect::<Result<Vec<_>, _>>()?;
        let errors = calendar
            .array("errors")?
            .iter()
            .map(|error| match Compact::of(error, api)?.string("reason")? {
                Some(reason) => Ok(json!(cut(&plain(reason), SHORT_MAX_BYTES).0)),
                None => Err(shape(api)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Args::new();
        if errors.is_empty() {
            if !calendar.item.contains_key("busy") {
                return Err(shape(api));
            }
            out.insert("busy".to_string(), Value::Array(busy));
        } else {
            if !busy.is_empty() {
                out.insert("busy".to_string(), Value::Array(busy));
            }
            out.insert("errors".to_string(), Value::Array(errors));
        }
        calendars.insert(id.clone(), Value::Object(out));
    }
    answer
        .out
        .insert("calendars".to_string(), Value::Object(calendars));
    Ok(answer.done())
}

/// Calendar ids for a refusal's detail: the first `QUOTED_IDS_MAX`, each
/// through `bounded_quote`, then how many more; `none` when empty.
fn quoted_ids(ids: &[&String]) -> String {
    if ids.is_empty() {
        return "none".to_string();
    }
    let mut out = ids
        .iter()
        .take(QUOTED_IDS_MAX)
        .map(|id| format!("\"{}\"", bounded_quote(id)))
        .collect::<Vec<_>>()
        .join(", ");
    if ids.len() > QUOTED_IDS_MAX {
        out.push_str(&format!(" and {} more", ids.len() - QUOTED_IDS_MAX));
    }
    out
}

/// One busy interval: Google's `start` and `end`, both required.
fn busy_interval(interval: &Value, api: &str) -> Result<Value, Failure> {
    let mut out = Compact::of(interval, api)?;
    out.short("start", "start")?;
    out.short("end", "end")?;
    if !(out.out.contains_key("start") && out.out.contains_key("end")) {
        return Err(shape(api));
    }
    Ok(out.done())
}

fn shape(api: &str) -> Failure {
    internal(format!("unexpected Google {api} response shape"))
}

#[cfg(test)]
mod tests {
    use brenn_provider_kit::testutil::{failure, google, http, nested, truncated};
    use brenn_provider_kit::{
        Bound, Probes, ToolBounds, check, hold_probes, hold_required, input_schema, probes,
    };
    use serde_json::{Map, json};

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    const BASE: &[(&str, &str)] = &[
        ("provider", "google"),
        ("gmail_host", "gmail.example.com"),
        ("calendar_host", "calendar.example.com"),
    ];

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
                calendar_host: "calendar.example.com".to_string(),
            }
        );
        let s = Settings::from_lookup(lookup(&with("gmail_host", "127.0.0.1:8080"))).unwrap();
        assert_eq!(s.gmail_host, "127.0.0.1:8080");
    }

    #[test]
    fn settings_refuse_a_missing_key_by_name() {
        for key in ["provider", "gmail_host", "calendar_host"] {
            let pairs: Vec<_> = BASE.iter().filter(|(k, _)| *k != key).copied().collect();
            let err = Settings::from_lookup(lookup(&pairs)).unwrap_err();
            assert_eq!(err, format!("config {key} is required"));
        }
    }

    #[test]
    fn settings_refuse_bad_values() {
        for key in ["provider", "gmail_host", "calendar_host"] {
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
        assert_eq!(s.calendar_host, "www.googleapis.com");
    }

    /// The guide quotes, in its answer summary and in the assistant's
    /// instructions, the two description sentences that say whose response
    /// `self_response` and `organizer.self` carry; each quote must stay the
    /// spec's words.
    #[test]
    fn the_google_workspace_guide_quotes_the_calendar_descriptions() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let read = |relative: &str| {
            let path = root.join(relative);
            std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()))
        };
        let spec = read("config/specs/google-workspace-tools.brenn");
        let description = |block: &str| -> String {
            let opening = format!("provide {block} {{");
            spec.lines()
                .map(str::trim)
                .skip_while(|line| *line != opening)
                .find_map(|line| line.strip_prefix("description = \""))
                .unwrap_or_else(|| panic!("the spec's {block} block has no description"))
                .to_string()
        };
        // From the opening words to the delimiter, the delimiter kept when
        // `keep` is set.
        let quoted = |text: &str, opening: &str, delimiter: &str, keep: bool| -> String {
            let start = text
                .find(opening)
                .unwrap_or_else(|| panic!("no {opening:?} in the description"));
            let end = start
                + text[start..]
                    .find(delimiter)
                    .unwrap_or_else(|| panic!("no {delimiter:?} after {opening:?}"));
            let end = if keep { end + 1 } else { end };
            text[start..end].to_string()
        };
        let collapse = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
        let guide = collapse(&read("docs/google-workspace.md"));
        for sentence in [
            quoted(
                &description("calendar-events"),
                "the answer to the invitation of the calendar read",
                ")",
                false,
            ),
            quoted(
                &description("calendar-event"),
                "The calendar read is the user on",
                ". ",
                true,
            ),
        ] {
            assert_eq!(
                guide.matches(&collapse(&sentence)).count(),
                2,
                "docs/google-workspace.md must quote {sentence:?} in section 5 and section 6"
            );
        }
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
                headers: BTreeMap::new(),
                body: None,
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
            "`account` is longer than 320 characters"
        );
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
    fn a_quoted_unreadable_is_plain_and_bounded() {
        let refused = Unreadable::Refused("result\u{1}exceeds max_body_bytes".to_string());
        assert_eq!(
            quoted(&refused, Some(4096)),
            "the host did not deliver api-request's answer: result exceeds max_body_bytes; \
             sizeEstimate 4096"
        );
        // A host detail copied into an answer is bounded.
        let long = Unreadable::Refused("x".repeat(5000));
        assert_eq!(
            quoted(&long, None).len(),
            GOOGLE_MESSAGE_MAX_BYTES + "…".len()
        );
    }

    #[test]
    fn a_google_error_maps_its_status_to_a_kind() {
        for (code, status, kind) in [
            (400, "INVALID_ARGUMENT", ErrorKind::InvalidArgs),
            (404, "NOT_FOUND", ErrorKind::InvalidArgs),
            (401, "UNAUTHENTICATED", ErrorKind::Internal),
            (429, "RESOURCE_EXHAUSTED", ErrorKind::Internal),
            (500, "INTERNAL", ErrorKind::Internal),
            (503, "UNAVAILABLE", ErrorKind::Internal),
        ] {
            assert_eq!(
                google_error(code, &google(code, status, "later"), Family::Gmail),
                failure(kind, &format!("Google {status} (HTTP {code}): later")),
                "{code}"
            );
        }
    }

    #[test]
    fn a_google_403_is_denied_with_the_scope_hint() {
        assert_eq!(
            google_error(
                403,
                &google(
                    403,
                    "PERMISSION_DENIED",
                    "Request had insufficient authentication scopes."
                ),
                Family::Gmail
            ),
            failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail read scope: run oauth-connect with that scope set \
                 and relay its steps."
            )
        );
        assert_eq!(
            google_error(403, "<html>SECRET</html>", Family::Gmail),
            failure(
                ErrorKind::Denied,
                &format!(
                    "Google HTTP 403, body not in the standard error shape. {}",
                    scope_hint(Family::Gmail)
                )
            )
        );
    }

    #[test]
    fn advance_answers_a_google_error_through_the_family_mapping() {
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
                    "unexpected Google labels.list response shape"
                )),
                "{body}"
            );
        }
        let one = labels_state(json!({ "id": "INBOX" }));
        assert_eq!(
            answered(&one, http(200, "{\"name\": \"INBOX\"}")),
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google labels.get response shape"
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
        assert_eq!(
            answered(&labels_state(json!({})), truncated(200, Value::Null, "{")),
            Next::Fail(Unreadable::Cut("of unstated length".to_string()).failure(None))
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
                "`q` is longer than 1024 characters",
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
                "`page` is longer than 4096 characters",
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
                headers: BTreeMap::new(),
                body: None,
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
                "unavailable": [deleted("m1"), deleted("m3")],
            }))
        );
    }

    fn deleted(id: &str) -> Value {
        json!({ "id": id, "reason": "deleted (Google 404)" })
    }

    #[test]
    fn a_message_cut_or_refused_mid_search_is_listed_unavailable() {
        let cut = truncated(200, json!(900000), "{");
        let refused = nested("internal", "result exceeds [messaging] max_body_bytes");
        for (outcome, reason) in [
            (
                cut,
                "Google's answer (900000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes)",
            ),
            (
                refused,
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes",
            ),
        ] {
            let state = search_state(json!({ "q": "lunch" }));
            let list = listing(&["m1", "m2"], json!({ "nextPageToken": "next-1" }));
            let (state, _) = follow(&state, answered(&state, http(200, &list)));
            let (state, call) = follow(&state, answered(&state, outcome));
            assert_eq!(call.api.path, "/gmail/v1/users/me/messages/m2");
            assert_eq!(
                answered(&state, http(200, &metadata("m2", &[]))),
                Next::Complete(json!({
                    "messages": [line("m2", json!([]))],
                    "next_page": "next-1",
                    "unavailable": [{ "id": "m1", "reason": reason }],
                }))
            );
        }
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
            Next::Complete(json!({ "messages": [], "unavailable": [deleted("m1")] }))
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
            (http(404, &gone), 24, Some(json!([deleted("m25")]))),
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
                "unexpected Google messages.get response shape"
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
        assert_eq!(
            answered(&state, nested("denied", "no clause")),
            Next::Fail(failure(
                ErrorKind::Denied,
                "api-request refused the nested GET gmail.example.com/gmail/v1/users/me/\
                 messages/m1: no clause"
            ))
        );
        let value = credential_invalid(json!("alice@example.com"));
        assert_eq!(answered(&state, Ok(value.clone())), Next::Complete(value));
    }

    #[test]
    fn search_refuses_an_unexpected_listing() {
        let state = search_state(json!({ "q": "lunch", "max": 2 }));
        let shape_error = Next::Fail(failure(
            ErrorKind::Internal,
            "unexpected Google messages.list response shape",
        ));
        for body in [
            "[]".to_string(),
            r#"{ "messages": {} }"#.to_string(),
            r#"{ "messages": [{ "threadId": "t1" }] }"#.to_string(),
            listing(&["m/1"], json!({})),
            listing(&["m1"], json!({ "nextPageToken": 7 })),
            listing(&["m1"], json!({ "nextPageToken": "" })),
            listing(&["m1"], json!({ "nextPageToken": " \t" })),
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
            "unexpected Google messages.get response shape",
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
        thread_too_long["threadId"] = json!("t".repeat(ID_MAX_CHARS + 1));
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
                    "unexpected Google labels.list response shape"
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
        assert_eq!(fit_text(answer.clone(), "text", whole), answer);
        for ceiling in with_text("")..whole {
            let fitted = fit_text(answer.clone(), "text", ceiling);
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
        truncated(200, bytes, "{\"id")
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
            Next::Fail(Unreadable::Cut("900000 bytes".to_string()).failure(None))
        );
    }

    #[test]
    fn a_refused_read_falls_back_to_the_structure() {
        let state = read_state(json!({ "id": "m1" }));
        let refusal = "result exceeds [messaging] max_body_bytes";
        let (row, call) = follow(&state, answered(&state, nested("internal", refusal)));
        assert_eq!(call.step, STRUCTURE_STEP);
        assert_eq!(
            call.api.query["fields"],
            QueryValue::One(STRUCTURE_FIELDS.to_string())
        );
        let answer = completed(answered(&row, http(200, STRUCTURE)));
        assert_eq!(answer["text"], Value::Null);
        assert_eq!(
            answer["text_omitted"],
            json!(
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes; sizeEstimate 4096; headers and attachments are listed, the \
                 body is not"
            )
        );
        // The narrower read refused too is the request's failure, no hint.
        assert_eq!(
            answered(&row, nested("internal", refusal)),
            Next::Fail(failure(
                ErrorKind::Internal,
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes"
            ))
        );
        // A structure row that lost its reason is refused, not guessed at.
        let mut lost = row;
        lost.derived = Map::new();
        assert_eq!(
            answered(&lost, http(200, STRUCTURE)),
            Next::Fail(failure(
                ErrorKind::Internal,
                "in-flight row at a re-read step carries no `unreadable` reason"
            ))
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
    fn a_thread_cut_or_refused_is_read_again_without_recipients() {
        for (outcome, why) in [
            (
                cut_read(json!(300000)),
                "Google's answer (300000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes)",
            ),
            (
                nested("internal", "result exceeds [messaging] max_body_bytes"),
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes",
            ),
        ] {
            let state =
                thread_state(json!({ "id": "t1", "max": 3, "account": "alice@example.com" }));
            let (row, call) = follow(&state, answered(&state, outcome));
            assert_eq!(call.step, THREAD_NARROW_STEP);
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
                        "metadataHeaders": ["From", "Subject", "Date"],
                        "fields": "id,messages(id,labelIds,snippet,internalDate,payload/headers)",
                        "prettyPrint": "false",
                    },
                })
            );
            // Google answers the narrower mask without `To` and `Cc`.
            let narrow = thread_body(|messages| {
                for message in messages.iter_mut() {
                    let Value::Array(headers) = &mut message["payload"]["headers"] else {
                        panic!("the message has headers");
                    };
                    headers.retain(|h| !matches!(h["name"].as_str(), Some("To" | "Cc")));
                }
            });
            let (named_row, names_call) = follow(&row, answered(&row, http(200, &narrow)));
            assert_eq!(names_call.step, LABELS_STEP);
            assert_eq!(names_call.api.account.as_deref(), Some("alice@example.com"));
            let names = r#"{ "labels": [{ "id": "Label_7", "name": "Projects" }] }"#;
            let answer = completed(answered(&named_row, http(200, names)));
            assert_eq!(answer["recipients_omitted"], json!(why));
            assert_eq!(answer["subject"], json!("Planning"));
            assert_eq!(answer["message_count"], json!(4));
            assert_eq!(answer["messages_omitted"], json!(1));
            let messages = answer["messages"].as_array().unwrap();
            assert_eq!(messages.len(), 3);
            for message in messages {
                assert_eq!(message.get("to"), None, "{message}");
                assert_eq!(message.get("cc"), None, "{message}");
                assert!(message.get("from").is_some(), "{message}");
            }
            assert_eq!(messages[0]["labels"], json!(["INBOX", "Projects"]));
            assert_eq!(messages[1]["subject"], json!("Re: Planning (room booked)"));

            // Even if Google sent `To` and `Cc` under the narrower mask, they
            // are not answered.
            let Next::Call { derived, .. } = answered(&row, http(200, THREAD)) else {
                panic!("the thread's label names are read next");
            };
            assert_eq!(derived["answer"]["messages"][0]["id"], json!("m2"));
            assert_eq!(derived["answer"]["messages"][0].get("to"), None);
            assert_eq!(derived["answer"]["messages"][0].get("cc"), None);

            // The narrower read cut or refused again is the request's failure.
            assert_eq!(
                answered(&row, cut_read(json!(200000))),
                Next::Fail(Unreadable::Cut("200000 bytes".to_string()).failure(None))
            );
        }
    }

    #[test]
    fn thread_refuses_an_unexpected_shape() {
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

    #[test]
    fn a_google_403_on_a_calendar_tool_names_the_calendar_scope() {
        let body = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        assert_eq!(
            google_error(403, &body, Family::Calendar),
            failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Calendar read scope: run oauth-connect with that scope \
                 set and relay its steps."
            )
        );
        let state = calendar_list_state(json!({}));
        assert_eq!(
            answered(&state, http(403, &body)),
            Next::Fail(failure(
                ErrorKind::Denied,
                &format!(
                    "Google PERMISSION_DENIED (HTTP 403): Request had insufficient \
                     authentication scopes. {}",
                    scope_hint(Family::Calendar)
                )
            ))
        );
    }

    #[test]
    fn every_tool_names_its_family() {
        for tool in Provided::ALL {
            let expected = if tool.name().starts_with("gmail-") {
                Family::Gmail
            } else {
                assert!(tool.name().starts_with("calendar-"), "{}", tool.name());
                Family::Calendar
            };
            assert_eq!(family(tool), expected, "{}", tool.name());
        }
    }

    #[test]
    fn a_calendar_id_is_one_segment_api_request_admits() {
        for ok in [
            "primary",
            "alice@example.com",
            "en.usa#holiday@group.v.calendar.google.com",
            "c_0123?x+y@group.calendar.google.com",
            &"a".repeat(CALENDAR_ID_MAX_CHARS),
        ] {
            assert!(is_calendar_id(ok), "{ok:?}");
        }
        for bad in [
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a%40b",
            "a\"b",
            "a b",
            "a\tb",
            "a\u{0}b",
            "a\u{a0}b",
            &"a".repeat(CALENDAR_ID_MAX_CHARS + 1),
        ] {
            assert!(!is_calendar_id(bad), "{bad:?}");
        }
    }

    fn calendar_list(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarList, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn calendar_list_query(extra: &[(&str, &str)]) -> Query {
        let mut query = query([
            ("maxResults", "100".to_string()),
            (
                "fields",
                "items(id,summary,primary,accessRole,timeZone),nextPageToken".to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ]);
        for (key, value) in extra {
            query.insert(key.to_string(), QueryValue::One(value.to_string()));
        }
        query
    }

    #[test]
    fn calendar_list_reads_one_page_on_the_calendar_host() {
        assert_eq!(
            calendar_list(json!({})).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/users/me/calendarList".to_string(),
                query: calendar_list_query(&[]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call =
            calendar_list(json!({ "page": "page-2", "account": "alice@example.com" })).unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.query, calendar_list_query(&[("pageToken", "page-2")]));
    }

    #[test]
    fn calendar_list_refuses_bad_arguments() {
        assert_eq!(
            calendar_list(json!({ "calendar": "primary" })).unwrap_err(),
            "unknown argument `calendar`; this tool takes `page`, `account`"
        );
        assert_eq!(
            calendar_list(json!({ "page": "" })).unwrap_err(),
            "`page` is empty"
        );
        assert_eq!(
            calendar_list(json!({ "page": "x".repeat(PAGE_MAX_CHARS + 1) })).unwrap_err(),
            "`page` is longer than 4096 characters"
        );
        assert_eq!(
            calendar_list(json!({ "page": 2 })).unwrap_err(),
            "`page` must be a string"
        );
        assert_eq!(
            calendar_list(json!({ "account": "x".repeat(321) })).unwrap_err(),
            "`account` is longer than 320 characters"
        );
    }

    const CALENDAR_LIST: &str = include_str!("fixtures/calendar-list.json");

    fn calendar_list_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarList, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarList,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    fn list_calendars(body: &str) -> Next {
        answered(&calendar_list_state(json!({})), http(200, body))
    }

    #[test]
    fn calendar_list_compacts_in_googles_order() {
        assert_eq!(
            list_calendars(CALENDAR_LIST),
            Next::Complete(json!({
                "calendars": [
                    {
                        "id": "alice@example.com",
                        "summary": "alice@example.com",
                        "primary": true,
                        "access_role": "owner",
                        "time_zone": "America/New_York",
                    },
                    {
                        "id": "c_0123456789abcdef@group.calendar.google.com",
                        "summary": "Family \"shared\"",
                        "access_role": "writer",
                        "time_zone": "America/Chicago",
                    },
                    {
                        "id": "en.usa#holiday@group.v.calendar.google.com",
                        "summary": "Holidays in United States",
                        "access_role": "reader",
                        "time_zone": "America/New_York",
                    },
                    {
                        "id": "bob@example.com",
                        "access_role": "freeBusyReader",
                        "time_zone": "Europe/London",
                    },
                ],
                "next_page": "page-2",
            }))
        );
        assert_eq!(
            list_calendars("{}"),
            Next::Complete(json!({ "calendars": [] }))
        );
        assert_eq!(
            list_calendars(r#"{ "items": [{ "id": "primary", "primary": false }] }"#),
            Next::Complete(json!({ "calendars": [{ "id": "primary" }] }))
        );
    }

    #[test]
    fn a_calendar_summary_is_plain_and_cut_flagged_and_vocabulary_cut_unflagged() {
        let body = json!({ "items": [{
            "id": "alice@example.com",
            "summary": format!("a\u{1}{}", "é".repeat(200)),
            "accessRole": "r".repeat(SHORT_MAX_BYTES + 10),
            "timeZone": "America/New_York",
        }] });
        let answer = completed(list_calendars(&body.to_string()));
        let entry = &answer["calendars"][0];
        let summary = entry["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.ends_with('…'), "{summary}");
        assert!(summary.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert_eq!(entry["fields_truncated"], json!(["summary"]));
        assert_eq!(
            entry["access_role"],
            json!(format!("{}…", "r".repeat(SHORT_MAX_BYTES)))
        );
    }

    #[test]
    fn calendar_list_refuses_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google calendarList.list response shape",
            ))
        };
        for body in [
            json!([]),
            json!({ "items": {} }),
            json!({ "items": [1] }),
            json!({ "items": [{}] }),
            json!({ "items": [{ "id": 7 }] }),
            json!({ "items": [{ "id": "a/b" }] }),
            json!({ "items": [{ "id": "x".repeat(CALENDAR_ID_MAX_BYTES + 1) }] }),
            json!({ "items": [{ "id": "primary", "summary": 1 }] }),
            json!({ "items": [{ "id": "primary", "primary": "yes" }] }),
            json!({ "items": [{ "id": "primary", "timeZone": null }] }),
            json!({ "items": [], "nextPageToken": 3 }),
            json!({ "items": [], "nextPageToken": "" }),
            json!({ "items": [], "nextPageToken": " " }),
            json!({ "items": [], "nextPageToken": "x".repeat(PAGE_MAX_BYTES + 1) }),
        ] {
            assert_eq!(list_calendars(&body.to_string()), shape(), "{body}");
        }
    }

    #[test]
    fn calendar_list_refuses_more_entries_than_it_asked_for() {
        let items: Vec<Value> = (0..=CALENDAR_LIST_PAGE)
            .map(|i| json!({ "id": format!("c{i}@example.com") }))
            .collect();
        assert_eq!(
            list_calendars(&json!({ "items": items }).to_string()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google calendarList.list answered 101 items for maxResults 100"
            ))
        );
    }

    #[test]
    fn calendar_list_relays_googles_errors() {
        let state = calendar_list_state(json!({ "page": "stale" }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Invalid page token"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Invalid page token"
            ))
        );
    }

    #[test]
    fn the_widest_calendar_list_page_fits_the_ceiling() {
        // Every string at its bound and as wide as it serializes: a summary
        // of quotes past its cut, a full-length id, vocabulary past its cut.
        let items: Vec<Value> = (0..CALENDAR_LIST_PAGE)
            .map(|i| {
                let id = format!("{i:03}{}", "a".repeat(CALENDAR_ID_MAX_BYTES - 3));
                json!({
                    "id": id,
                    "summary": "\"".repeat(LISTING_TEXT_MAX_BYTES + 1),
                    "primary": true,
                    "accessRole": "\"".repeat(SHORT_MAX_BYTES + 1),
                    "timeZone": "\"".repeat(SHORT_MAX_BYTES + 1),
                })
            })
            .collect();
        let body = json!({ "items": items, "nextPageToken": "\"".repeat(PAGE_MAX_BYTES) });
        let answer = completed(list_calendars(&body.to_string()));
        assert_eq!(
            answer["calendars"].as_array().unwrap().len(),
            CALENDAR_LIST_PAGE
        );
        assert_eq!(
            answer["calendars"][0]["fields_truncated"],
            json!(["summary"])
        );
        let size = serialized_len(&answer);
        assert!(size < 140_000, "{size}");
        assert!(size < COMPACT_RESULT_CEILING);
    }

    #[test]
    fn a_calendar_id_argument_is_refused_before_any_call() {
        let refusal = "`calendar` must be a calendar id of 1 to 320 characters with no `/`, `\\`, `%`, \
                       `\"`, space or control character";
        for bad in [
            json!("a/b"),
            json!("a%40b"),
            json!("a b"),
            json!(""),
            json!("x".repeat(CALENDAR_ID_MAX_CHARS + 1)),
            json!(7),
        ] {
            assert_eq!(
                events(json!({ "calendar": bad, "from": FROM, "to": TO })).unwrap_err(),
                refusal,
                "{bad}"
            );
        }
    }

    #[test]
    fn an_event_id_is_googles_base32hex_with_an_instance_suffix() {
        for ok in [
            "0a1b2c3d4e5f",
            "weekly0123456789_20261104T140000Z",
            &"a".repeat(EVENT_ID_MAX_CHARS),
        ] {
            assert!(is_event_id(ok), "{ok:?}");
        }
        for bad in ["", "a b", "a/b", "a.b", &"a".repeat(EVENT_ID_MAX_CHARS + 1)] {
            assert!(!is_event_id(bad), "{bad:?}");
        }
    }

    const FROM: &str = "2026-11-02T00:00:00-05:00";
    const TO: &str = "2026-11-09T00:00:00-05:00";

    fn events(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarEvents, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn events_query(max: &str, extra: &[(&str, &str)]) -> Query {
        let mut query = query([
            ("timeMin", FROM.to_string()),
            ("timeMax", TO.to_string()),
            ("singleEvents", "true".to_string()),
            ("orderBy", "startTime".to_string()),
            ("maxResults", max.to_string()),
            ("maxAttendees", "1".to_string()),
            (
                "fields",
                "items(id,summary,start,end,location,status,organizer/email,\
                 attendees(self,responseStatus)),nextPageToken,timeZone"
                    .to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ]);
        for (key, value) in extra {
            query.insert(key.to_string(), QueryValue::One(value.to_string()));
        }
        query
    }

    #[test]
    fn events_reads_one_window_of_primary_on_the_calendar_host() {
        assert_eq!(
            events(json!({ "from": FROM, "to": TO })).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/primary/events".to_string(),
                query: events_query("50", &[]),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call = events(json!({
            "calendar": "en.usa#holiday@group.v.calendar.google.com",
            "from": FROM,
            "to": TO,
            "q": "lunch & co",
            "max": 7,
            "page": "page-2",
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(
            call.path,
            "/calendar/v3/calendars/en.usa#holiday@group.v.calendar.google.com/events"
        );
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            call.query,
            events_query("7", &[("q", "lunch & co"), ("pageToken", "page-2")])
        );
    }

    #[test]
    fn events_refuse_bad_arguments() {
        for (args, refusal) in [
            (json!({ "to": TO }), "`from` is required"),
            (json!({ "from": FROM }), "`to` is required"),
            (json!({ "from": " ", "to": TO }), "`from` is empty"),
            (
                json!({ "from": FROM, "to": "x".repeat(TIME_MAX_CHARS + 1) }),
                "`to` is longer than 64 characters",
            ),
            (
                json!({ "from": FROM, "to": TO, "max": 0 }),
                "`max` must be an integer from 1 to 50",
            ),
            (
                json!({ "from": FROM, "to": TO, "max": 51 }),
                "`max` must be an integer from 1 to 50",
            ),
            (json!({ "from": FROM, "to": TO, "q": "" }), "`q` is empty"),
            (
                json!({ "from": FROM, "to": TO, "page": "x".repeat(PAGE_MAX_CHARS + 1) }),
                "`page` is longer than 4096 characters",
            ),
            (
                json!({ "from": FROM, "to": TO, "id": "e1" }),
                "unknown argument `id`; this tool takes `calendar`, `from`, `to`, `q`, `max`, \
                 `page`, `account`",
            ),
        ] {
            assert_eq!(events(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    const EVENTS_LIST: &str = include_str!("fixtures/events-list.json");

    fn events_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarEvents, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarEvents,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    fn list_events(body: &str) -> Next {
        answered(
            &events_state(json!({ "from": FROM, "to": TO })),
            http(200, body),
        )
    }

    #[test]
    fn events_compact_in_googles_order_with_the_users_own_response() {
        assert_eq!(
            list_events(EVENTS_LIST),
            Next::Complete(json!({
                "events": [
                    {
                        "id": "0a1b2c3d4e5f6g7h8i9j",
                        "summary": "Planning",
                        "start": {
                            "date_time": "2026-11-02T09:00:00-05:00",
                            "time_zone": "America/New_York",
                        },
                        "end": {
                            "date_time": "2026-11-02T10:00:00-05:00",
                            "time_zone": "America/New_York",
                        },
                        "location": "Room 4 \"north\"",
                        "status": "confirmed",
                        "organizer": "alice@example.com",
                        "self_response": "accepted",
                    },
                    {
                        "id": "allday0123456789",
                        "summary": "Holiday",
                        "start": { "date": "2026-11-03" },
                        "end": { "date": "2026-11-04" },
                        "status": "confirmed",
                        "organizer": "en.usa#holiday@group.v.calendar.google.com",
                    },
                    {
                        "id": "weekly0123456789_20261104T140000Z",
                        "summary": "Weekly sync",
                        "start": { "date_time": "2026-11-04T14:00:00Z" },
                        "end": { "date_time": "2026-11-04T14:30:00Z" },
                        "status": "confirmed",
                        "organizer": "bob@example.com",
                        "self_response": "declined",
                    },
                    {
                        "id": "offsite0123456789",
                        "summary": "Offsite",
                        "start": {
                            "date_time": "2026-11-05T09:00:00-08:00",
                            "time_zone": "America/Los_Angeles",
                        },
                        "end": {
                            "date_time": "2026-11-05T17:00:00-08:00",
                            "time_zone": "America/Los_Angeles",
                        },
                        "status": "tentative",
                        "organizer": "bob@example.com",
                    },
                ],
                "time_zone": "America/New_York",
                "next_page": "page-2",
            }))
        );
        assert_eq!(list_events("{}"), Next::Complete(json!({ "events": [] })));
        assert_eq!(
            list_events(r#"{ "items": [{ "id": "e1", "attendees": [{ "self": true }] }] }"#),
            Next::Complete(json!({ "events": [{ "id": "e1" }] }))
        );
    }

    #[test]
    fn an_events_line_cuts_and_flags_its_text_and_organizer() {
        let organizer = format!("{}@example.com", "o".repeat(ACCOUNT_MAX_BYTES));
        let body = json!({ "items": [{
            "id": "e1",
            "summary": format!("a\u{1}{}", "é".repeat(200)),
            "location": "Room 4",
            "status": "s".repeat(SHORT_MAX_BYTES + 10),
            "organizer": { "email": organizer },
            "attendees": [{ "self": true, "responseStatus": "r".repeat(SHORT_MAX_BYTES + 1) }],
        }] });
        let answer = completed(list_events(&body.to_string()));
        let line = &answer["events"][0];
        let summary = line["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(line["organizer"].as_str().unwrap().ends_with('…'));
        assert_eq!(line["fields_truncated"], json!(["organizer", "summary"]));
        assert_eq!(
            line["status"],
            json!(format!("{}…", "s".repeat(SHORT_MAX_BYTES)))
        );
        assert_eq!(
            line["self_response"],
            json!(format!("{}…", "r".repeat(SHORT_MAX_BYTES)))
        );
    }

    #[test]
    fn an_events_lines_organizer_is_cut_in_bytes() {
        // 160 two-byte characters and a 4-byte domain: 164 characters, 324
        // bytes. Under the argument's 320-character bound, over the answer's
        // 320-byte one.
        let organizer = format!("{}@x.y", "é".repeat(160));
        assert!(organizer.chars().count() <= ACCOUNT_MAX_CHARS);
        let body = json!({ "items": [{ "id": "e1", "organizer": { "email": organizer } }] });
        let answer = completed(list_events(&body.to_string()));
        let line = &answer["events"][0];
        let cut_email = line["organizer"].as_str().unwrap();
        assert_eq!(cut_email, format!("{}…", "é".repeat(ACCOUNT_MAX_BYTES / 2)));
        assert_eq!(line["fields_truncated"], json!(["organizer"]));
    }

    /// A string of `n` characters, each two bytes in UTF-8.
    fn wide(n: usize) -> String {
        "é".repeat(n)
    }

    /// A `key` of `max` characters is admitted and one of `max + 1` refused,
    /// both in characters of two UTF-8 bytes, beside the arguments `base`.
    fn admits_characters(
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

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(search, json!({}), "q", QUERY_MAX_CHARS);
        admits_characters(search, json!({ "q": "x" }), "page", PAGE_MAX_CHARS);
        admits_characters(labels, json!({}), "account", ACCOUNT_MAX_CHARS);
        admits_characters(calendar_list, json!({}), "page", PAGE_MAX_CHARS);
        admits_characters(events, json!({ "to": TO }), "from", TIME_MAX_CHARS);
        admits_characters(free_busy, json!({ "from": FROM }), "to", TIME_MAX_CHARS);
        admits_characters(
            events,
            json!({ "from": FROM, "to": TO }),
            "q",
            QUERY_MAX_CHARS,
        );
        let calendar = |id: String| events(json!({ "calendar": id, "from": FROM, "to": TO }));
        assert!(calendar(wide(CALENDAR_ID_MAX_CHARS)).is_ok());
        assert!(calendar(wide(CALENDAR_ID_MAX_CHARS + 1)).is_err());
        let calendars =
            |id: String| free_busy(json!({ "from": FROM, "to": TO, "calendars": [id] }));
        assert!(calendars(wide(CALENDAR_ID_MAX_CHARS)).is_ok());
        assert!(calendars(wide(CALENDAR_ID_MAX_CHARS + 1)).is_err());
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::GmailLabels => (LabelsArgs::KEYS, &[]),
            Provided::GmailSearch => (SearchArgs::KEYS, &["q"]),
            Provided::GmailRead => (ReadArgs::KEYS, &["id"]),
            Provided::GmailThread => (ThreadArgs::KEYS, &["id"]),
            Provided::CalendarList => (CalendarListArgs::KEYS, &[]),
            Provided::CalendarEvents => (EventsArgs::KEYS, &["from", "to"]),
            Provided::CalendarEvent => (EventArgs::KEYS, &["id"]),
            Provided::CalendarFreeBusy => (FreeBusyArgs::KEYS, &["from", "to"]),
        };
        let text = |max_chars| Bound::Text {
            max_chars,
            format: None,
        };
        let integer = |min, max| Bound::Integer { min, max };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (_, "account") => text(ACCOUNT_MAX_CHARS),
                (_, "q") => text(QUERY_MAX_CHARS),
                (_, "page") => text(PAGE_MAX_CHARS),
                (_, "from" | "to") => text(TIME_MAX_CHARS),
                (_, "calendar") => Bound::Pattern(calendar_id_pattern()),
                (_, "calendars") => Bound::Array {
                    min: 1,
                    max: FREEBUSY_CALENDARS_MAX,
                    unique: true,
                    item: Box::new(Bound::Pattern(calendar_id_pattern())),
                },
                (_, "max_chars") => integer(MAX_CHARS_MIN, MAX_CHARS_MAX),
                (_, "spam_trash") => Bound::Bool,
                (Provided::GmailLabels | Provided::GmailRead | Provided::GmailThread, "id") => {
                    Bound::Token {
                        max_chars: ID_MAX_CHARS,
                    }
                }
                (Provided::CalendarEvent, "id") => Bound::Token {
                    max_chars: EVENT_ID_MAX_CHARS,
                },
                (Provided::GmailSearch, "max") => integer(SEARCH_MAX_MIN, SEARCH_MAX_MAX),
                (Provided::GmailThread, "max") => integer(THREAD_MAX_MIN, THREAD_MAX_MAX),
                (Provided::CalendarEvents, "max") => integer(EVENTS_MAX_MIN, EVENTS_MAX_MAX),
                _ => panic!("{tool:?} takes `{key}`, which no bound here states"),
            };
            (key, bound)
        };
        ToolBounds {
            properties: keys.iter().copied().map(property).collect(),
            required,
        }
    }

    /// Arguments `tool` admits, every required one present, into which a
    /// probe of one argument is set.
    fn admitted_args(tool: Provided) -> Value {
        match tool {
            Provided::GmailLabels | Provided::CalendarList => json!({}),
            Provided::GmailSearch => json!({ "q": "from:alice" }),
            Provided::GmailRead | Provided::GmailThread => json!({ "id": "m1" }),
            Provided::CalendarEvent => json!({ "id": "e1" }),
            Provided::CalendarEvents | Provided::CalendarFreeBusy => {
                json!({ "from": FROM, "to": TO })
            }
        }
    }

    /// The probes of the arguments whose bounds are this component's: the
    /// calendar id, alone as `calendar` and as `calendars`' items.
    fn calendar_probes(key: &str) -> Probes {
        let ids = |n: usize| json!((0..n).map(|i| format!("c{i}")).collect::<Vec<_>>());
        match key {
            "calendar" => Probes {
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
            },
            "calendars" => Probes {
                admitted: vec![ids(1), ids(FREEBUSY_CALENDARS_MAX)],
                refused: vec![
                    ids(0),
                    ids(FREEBUSY_CALENDARS_MAX + 1),
                    json!(["c0", "c0"]),
                    json!([""]),
                    json!(["a/b"]),
                    json!([1]),
                    json!("primary"),
                ],
            },
            other => panic!("`{other}` has no probes"),
        }
    }

    /// Each tool's `input_schema` states exactly the bounds its parser
    /// enforces, and the parser admits and refuses every bound's probes,
    /// naming the argument it refuses: the schema and the parser are held to
    /// the same table from both sides.
    #[test]
    fn every_tools_schema_states_what_its_parser_enforces() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-workspace-tools.brenn");
        let spec = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        for tool in Provided::ALL {
            let name = tool.name();
            let bounds = bounds(tool);
            if let Err(error) = check(&input_schema(&spec, name), &bounds) {
                panic!("`{name}`'s input_schema: {error}");
            }
            let plan_with = |args: &Value| plan(tool, args, &settings()).map(drop);
            let base = admitted_args(tool);
            for key in bounds.required {
                hold_required(name, key, &base, plan_with);
            }
            for (key, bound) in &bounds.properties {
                let probes = probes(bound).unwrap_or_else(|| calendar_probes(key));
                hold_probes(name, key, &probes, &base, plan_with);
            }
        }
    }

    #[test]
    fn a_listed_calendar_id_is_bounded_in_bytes() {
        let id = wide(CALENDAR_ID_MAX_BYTES / 2 + 1);
        assert!(is_calendar_id(&id), "an argument of 161 characters");
        assert!(!is_listed_calendar_id(&id), "an answer of 322 bytes");
        assert!(is_listed_calendar_id(&wide(CALENDAR_ID_MAX_BYTES / 2)));
        for bad in ["", ".", "..", "a/b", "a b"] {
            assert!(!is_listed_calendar_id(bad), "{bad:?}");
        }
        let body = json!({ "items": [{ "id": id }] });
        assert_eq!(
            list_calendars(&body.to_string()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google calendarList.list response shape"
            ))
        );
    }

    #[test]
    fn events_refuse_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google events.list response shape",
            ))
        };
        let event = |extra: Value| {
            let mut item = json!({ "id": "e1" });
            item.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({ "items": [item] })
        };
        for body in [
            json!([]),
            json!({ "items": {} }),
            json!({ "items": [1] }),
            json!({ "items": [{}] }),
            json!({ "items": [{ "id": "e.1" }] }),
            json!({ "items": [{ "id": "e".repeat(EVENT_ID_MAX_CHARS + 1) }] }),
            event(json!({ "summary": 1 })),
            event(json!({ "start": "2026-11-02" })),
            event(json!({ "start": {} })),
            event(json!({ "start": { "timeZone": "UTC" } })),
            event(json!({ "start": { "date": "2026-11-02", "dateTime": "2026-11-02T00:00:00Z" } })),
            event(json!({ "end": { "date": 20261102 } })),
            event(json!({ "organizer": "bob@example.com" })),
            event(json!({ "organizer": { "email": 1 } })),
            event(json!({ "attendees": {} })),
            event(json!({ "attendees": ["alice@example.com"] })),
            event(json!({ "attendees": [{ "self": "yes" }] })),
            event(json!({ "attendees": [{ "self": true, "responseStatus": 1 }] })),
            event(json!({ "attendees": [{ "self": true }, { "self": true }] })),
            json!({ "items": [], "timeZone": 5 }),
            json!({ "items": [], "nextPageToken": "" }),
        ] {
            assert_eq!(list_events(&body.to_string()), shape(), "{body}");
        }
    }

    #[test]
    fn events_refuse_more_events_than_they_asked_for() {
        let items: Vec<Value> = (0..=7).map(|i| json!({ "id": format!("e{i}") })).collect();
        let state = events_state(json!({ "from": FROM, "to": TO, "max": 7 }));
        assert_eq!(
            answered(&state, http(200, &json!({ "items": items }).to_string())),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google events.list answered 8 events for maxResults 7"
            ))
        );
    }

    #[test]
    fn events_relay_googles_errors() {
        let state = events_state(json!({ "from": "monday", "to": TO }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Bad Request"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Bad Request"
            ))
        );
    }

    #[test]
    fn the_widest_events_page_fits_the_ceiling() {
        // Every string at its bound and as wide as it serializes.
        let quotes = |n: usize| "\"".repeat(n);
        let when = || json!({ "dateTime": quotes(SHORT_MAX_BYTES + 1), "timeZone": quotes(SHORT_MAX_BYTES + 1) });
        let items: Vec<Value> = (0..EVENTS_MAX_MAX)
            .map(|i| {
                json!({
                    "id": format!("{i:02}{}", "a".repeat(EVENT_ID_MAX_CHARS - 2)),
                    "summary": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "location": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "status": quotes(SHORT_MAX_BYTES + 1),
                    "start": when(),
                    "end": when(),
                    "organizer": { "email": quotes(ACCOUNT_MAX_BYTES + 1) },
                    "attendees": [{ "self": true, "responseStatus": quotes(SHORT_MAX_BYTES + 1) }],
                })
            })
            .collect();
        let body = json!({
            "items": items,
            "timeZone": quotes(SHORT_MAX_BYTES + 1),
            "nextPageToken": quotes(PAGE_MAX_BYTES),
        });
        let answer = completed(list_events(&body.to_string()));
        assert_eq!(
            answer["events"].as_array().unwrap().len(),
            usize::try_from(EVENTS_MAX_MAX).unwrap()
        );
        assert_eq!(
            answer["events"][0]["fields_truncated"],
            json!(["location", "organizer", "summary"])
        );
        let size = serialized_len(&answer);
        assert!(size < 210_000, "{size}");
        assert!(size < COMPACT_RESULT_CEILING);
    }

    fn event(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarEvent, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    fn event_query() -> Query {
        query([
            ("maxAttendees", "50".to_string()),
            (
                "fields",
                "id,status,summary,description,location,start,end,\
                 organizer(email,displayName,self),\
                 attendees(email,displayName,responseStatus,optional,self),\
                 attendeesOmitted,recurrence,recurringEventId,hangoutLink,\
                 conferenceData/entryPoints(entryPointType,uri)"
                    .to_string(),
            ),
            ("prettyPrint", "false".to_string()),
        ])
    }

    #[test]
    fn event_reads_one_event_of_primary_on_the_calendar_host() {
        assert_eq!(
            event(json!({ "id": "e1" })).unwrap(),
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "GET".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/calendars/primary/events/e1".to_string(),
                query: event_query(),
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let call = event(json!({
            "id": "weekly0123456789_20261104T140000Z",
            "calendar": "en.usa#holiday@group.v.calendar.google.com",
            "max_chars": 200,
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(
            call.path,
            "/calendar/v3/calendars/en.usa#holiday@group.v.calendar.google.com/events/\
             weekly0123456789_20261104T140000Z"
        );
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.query, event_query());
    }

    #[test]
    fn event_refuses_bad_arguments() {
        let bad_id = "`id` must be 1 to 1024 letters, digits, `_` or `-`";
        for (args, refusal) in [
            (json!({}), "`id` is required"),
            (json!({ "id": "" }), bad_id),
            (json!({ "id": "e.1" }), bad_id),
            (json!({ "id": "e/1" }), bad_id),
            (json!({ "id": "e".repeat(EVENT_ID_MAX_CHARS + 1) }), bad_id),
            (json!({ "id": 1 }), bad_id),
            (
                json!({ "id": "e1", "calendar": "a/b" }),
                "`calendar` must be a calendar id of 1 to 320 characters with no `/`, `\\`, `%`, \
                 `\"`, space or control character",
            ),
            (
                json!({ "id": "e1", "max_chars": 199 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "e1", "max_chars": 60_001 }),
                "`max_chars` must be an integer from 200 to 60000",
            ),
            (
                json!({ "id": "e1", "from": "x" }),
                "unknown argument `from`; this tool takes `id`, `calendar`, `max_chars`, `account`",
            ),
        ] {
            assert_eq!(event(args.clone()).unwrap_err(), refusal, "{args}");
        }
    }

    const EVENT: &str = include_str!("fixtures/event.json");
    const EVENT_ID: &str = "weekly0123456789_20261104T140000Z";

    fn event_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarEvent, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarEvent,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    fn read_event(body: &str) -> Next {
        answered(&event_state(json!({ "id": EVENT_ID })), http(200, body))
    }

    /// The fixture with `changes` applied, top-level keys replaced or, when
    /// null, removed.
    fn event_with(changes: Value) -> String {
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

    #[test]
    fn event_compacts_with_its_attendees_and_their_responses() {
        let description = "<p>Agenda:</p>\n<ul><li>roadmap</li><li>hiring &amp; budget</li></ul>";
        assert_eq!(
            read_event(EVENT),
            Next::Complete(json!({
                "id": EVENT_ID,
                "summary": "Weekly sync",
                "description": description,
                "description_chars": description.chars().count(),
                "start": {
                    "date_time": "2026-11-04T09:00:00-05:00",
                    "time_zone": "America/New_York",
                },
                "end": {
                    "date_time": "2026-11-04T09:30:00-05:00",
                    "time_zone": "America/New_York",
                },
                "location": "Room 4 \"north\"",
                "status": "confirmed",
                "organizer": { "email": "bob@example.com", "name": "Bob Example" },
                "self_response": "tentative",
                "attendees": [
                    { "email": "bob@example.com", "name": "Bob Example", "response": "accepted" },
                    { "email": "alice@example.com", "response": "tentative" },
                    {
                        "email": "carol@example.com",
                        "name": "Carol",
                        "response": "declined",
                        "optional": true,
                    },
                    { "email": "dave@example.com", "response": "needsAction" },
                ],
                "attendees_total": 4,
                "attendee_responses": {
                    "accepted": 1,
                    "declined": 1,
                    "tentative": 1,
                    "needs_action": 1,
                },
                "recurring_event_id": "weekly0123456789",
                "conference": "https://meet.example.com/abc-defg-hij",
            }))
        );
    }

    #[test]
    fn an_event_with_attendees_left_out_answers_the_users_own_entry_and_no_counts() {
        let answer = completed(read_event(&event_with(json!({
            "attendees": [{ "email": "alice@example.com", "self": true, "responseStatus": "accepted" }],
            "attendeesOmitted": true,
        }))));
        assert_eq!(
            answer["attendees"],
            json!([{ "email": "alice@example.com", "response": "accepted" }])
        );
        assert_eq!(answer["self_response"], json!("accepted"));
        assert_eq!(answer["attendees_omitted"], json!(true));
        assert!(answer.get("attendees_total").is_none(), "{answer}");
        assert!(answer.get("attendee_responses").is_none(), "{answer}");

        // Left out with no entry of the user's: nothing listed, nothing counted.
        let answer = completed(read_event(&event_with(
            json!({ "attendees": null, "attendeesOmitted": true }),
        )));
        assert_eq!(answer["attendees"], json!([]));
        assert_eq!(answer["attendees_omitted"], json!(true));
        assert!(answer.get("self_response").is_none(), "{answer}");

        // No guests at all: an empty list, counted.
        let answer = completed(read_event(&event_with(json!({ "attendees": null }))));
        assert_eq!(answer["attendees"], json!([]));
        assert_eq!(answer["attendees_total"], json!(0));
        assert_eq!(
            answer["attendee_responses"],
            json!({ "accepted": 0, "declined": 0, "tentative": 0, "needs_action": 0 })
        );
    }

    #[test]
    fn an_event_answers_its_recurrence_and_conference_and_all_day_times() {
        let rules: Vec<String> = (0..=RECURRENCE_MAX)
            .map(|i| format!("EXDATE:202611{i:02}"))
            .collect();
        let answer = completed(read_event(&event_with(json!({
            "start": { "date": "2026-11-04" },
            "end": { "date": "2026-11-05" },
            "recurrence": rules,
            "recurringEventId": null,
            "hangoutLink": null,
            "conferenceData": { "entryPoints": [
                { "entryPointType": "phone", "uri": "tel:+1-555-0100" },
                { "entryPointType": "video", "uri": "https://video.example.com/j/1" },
                { "entryPointType": "video", "uri": "https://video.example.com/j/2" },
            ] },
        }))));
        assert_eq!(answer["start"], json!({ "date": "2026-11-04" }));
        assert_eq!(answer["end"], json!({ "date": "2026-11-05" }));
        assert_eq!(answer["recurrence"], json!(rules[..RECURRENCE_MAX]));
        assert_eq!(answer["recurrence_truncated"], json!(true));
        assert!(answer.get("recurring_event_id").is_none(), "{answer}");
        assert_eq!(answer["conference"], json!("https://video.example.com/j/1"));

        let answer = completed(read_event(&event_with(json!({
            "recurrence": ["RRULE:FREQ=WEEKLY"],
            "hangoutLink": null,
            "conferenceData": { "entryPoints": [{ "entryPointType": "phone", "uri": "tel:1" }] },
        }))));
        assert_eq!(answer["recurrence"], json!(["RRULE:FREQ=WEEKLY"]));
        assert!(answer.get("recurrence_truncated").is_none(), "{answer}");
        assert!(answer.get("conference").is_none(), "{answer}");

        // A Meet link wins over a video entry point of the conference data.
        let answer = completed(read_event(&event_with(json!({
            "hangoutLink": "https://meet.example.com/abc",
            "conferenceData": { "entryPoints": [
                { "entryPointType": "video", "uri": "https://video.example.com/j/1" },
            ] },
        }))));
        assert_eq!(answer["conference"], json!("https://meet.example.com/abc"));
    }

    #[test]
    fn an_event_cuts_and_flags_its_text_and_emails() {
        let long = format!("a\u{1}{}", "é".repeat(EVENT_TEXT_MAX_BYTES));
        let email = format!("{}@example.com", "o".repeat(ACCOUNT_MAX_BYTES));
        let answer = completed(read_event(&event_with(json!({
            "summary": long,
            "location": long,
            "status": "s".repeat(SHORT_MAX_BYTES + 1),
            "organizer": { "email": email, "displayName": long, "self": true },
            "attendees": [{
                "email": email,
                "displayName": long,
                "self": true,
                "responseStatus": "accepted",
            }],
            "recurrence": [long, "RRULE:FREQ=DAILY"],
            "hangoutLink": long,
        }))));
        let summary = answer["summary"].as_str().unwrap();
        assert!(summary.starts_with("a é"), "{summary}");
        assert!(summary.len() <= EVENT_TEXT_MAX_BYTES + '…'.len_utf8());
        let location = answer["location"].as_str().unwrap();
        assert!(location.ends_with('…'), "{location}");
        assert!(location.len() <= EVENT_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(location.len() > LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert_eq!(
            answer["fields_truncated"],
            json!(["conference", "location", "recurrence", "summary"])
        );
        assert_eq!(answer["recurrence"][1], json!("RRULE:FREQ=DAILY"));
        assert_eq!(
            answer["status"],
            json!(format!("{}…", "s".repeat(SHORT_MAX_BYTES)))
        );
        let organizer = &answer["organizer"];
        assert_eq!(organizer["self"], json!(true));
        assert_eq!(organizer["fields_truncated"], json!(["email", "name"]));
        assert!(organizer["email"].as_str().unwrap().ends_with('…'));
        let attendee = &answer["attendees"][0];
        assert_eq!(attendee["fields_truncated"], json!(["email", "name"]));
        let name = attendee["name"].as_str().unwrap();
        assert!(name.len() <= LISTING_TEXT_MAX_BYTES + '…'.len_utf8());
        assert!(attendee.get("self").is_none(), "{attendee}");
    }

    #[test]
    fn an_event_description_is_cut_at_max_chars_and_not_decoded() {
        let description = format!("a&amp;\u{1}{}", "é".repeat(300));
        let body = event_with(json!({ "description": description }));
        let state = event_state(json!({ "id": EVENT_ID, "max_chars": 200 }));
        let answer = completed(answered(&state, http(200, &body)));
        assert_eq!(
            answer["description"],
            json!(format!("a&amp; {}", "é".repeat(193)))
        );
        assert_eq!(answer["description_truncated"], json!(true));
        assert_eq!(answer["description_chars"], json!(307));

        let answer = completed(read_event(&event_with(json!({ "description": null }))));
        assert!(answer.get("description").is_none(), "{answer}");
        assert!(answer.get("description_chars").is_none(), "{answer}");
    }

    #[test]
    fn an_event_refuses_an_unexpected_shape() {
        let shape = || {
            Next::Fail(failure(
                ErrorKind::Internal,
                "unexpected Google events.get response shape",
            ))
        };
        let attendee = |n: usize| {
            (0..n)
                .map(|i| json!({ "email": format!("a{i}@example.com"), "responseStatus": "accepted" }))
                .collect::<Vec<_>>()
        };
        for body in [
            "[]".to_string(),
            event_with(json!({ "id": "e1" })),
            event_with(json!({ "id": null })),
            event_with(json!({ "recurringEventId": "weekly.1" })),
            event_with(json!({ "recurringEventId": "w".repeat(EVENT_ID_MAX_CHARS + 1) })),
            event_with(json!({ "description": 1 })),
            event_with(json!({ "start": { "timeZone": "UTC" } })),
            event_with(json!({ "organizer": "bob@example.com" })),
            event_with(json!({ "organizer": { "self": "yes" } })),
            event_with(json!({ "attendees": attendee(ATTENDEES_MAX + 1) })),
            event_with(
                json!({ "attendees": [{ "email": "a@example.com", "responseStatus": "maybe" }] }),
            ),
            event_with(json!({ "attendees": [{ "email": "a@example.com" }] })),
            event_with(json!({ "attendees": [{ "email": 1, "responseStatus": "accepted" }] })),
            event_with(
                json!({ "attendees": [{ "optional": "no", "responseStatus": "accepted" }] }),
            ),
            event_with(json!({ "attendeesOmitted": "true" })),
            event_with(json!({ "recurrence": "RRULE:FREQ=DAILY" })),
            event_with(json!({ "recurrence": [1] })),
            event_with(json!({ "hangoutLink": 1 })),
            event_with(json!({ "hangoutLink": null, "conferenceData": [] })),
            event_with(
                json!({ "hangoutLink": null, "conferenceData": { "entryPoints": [{ "entryPointType": "video", "uri": 1 }] } }),
            ),
        ] {
            assert_eq!(read_event(&body), shape(), "{body}");
        }
        // Exactly the bound is Google's own list, and an unknown response is
        // not counted against when the list is not being counted.
        let answer = completed(read_event(&event_with(
            json!({ "attendees": attendee(ATTENDEES_MAX) }),
        )));
        assert_eq!(answer["attendees_total"], json!(ATTENDEES_MAX));
        let answer = completed(read_event(&event_with(json!({
            "attendees": [{ "email": "a@example.com", "self": true, "responseStatus": "maybe" }],
            "attendeesOmitted": true,
        }))));
        assert_eq!(answer["self_response"], json!("maybe"));
    }

    #[test]
    fn event_relays_googles_errors_with_the_calendar_hint() {
        let state = event_state(json!({ "id": "e1" }));
        assert_eq!(
            answered(&state, http(404, &google(404, "NOT_FOUND", "Not Found"))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google NOT_FOUND (HTTP 404): Not Found"
            ))
        );
        let Next::Fail(Failure { kind, detail }) = answered(
            &state,
            http(
                403,
                &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
            ),
        ) else {
            panic!("a 403 did not fail");
        };
        assert_eq!(kind, ErrorKind::Denied);
        assert!(detail.ends_with(&scope_hint(Family::Calendar)), "{detail}");
    }

    #[test]
    fn the_widest_event_fits_the_ceiling_with_its_description_fitted() {
        // Every string at its bound and as wide as it serializes.
        let quotes = |n: usize| "\"".repeat(n);
        let id = "a".repeat(EVENT_ID_MAX_CHARS);
        let when = || json!({ "dateTime": quotes(SHORT_MAX_BYTES + 1), "timeZone": quotes(SHORT_MAX_BYTES + 1) });
        let attendees: Vec<Value> = (0..ATTENDEES_MAX)
            .map(|_| {
                json!({
                    "email": quotes(ACCOUNT_MAX_BYTES + 1),
                    "displayName": quotes(LISTING_TEXT_MAX_BYTES + 1),
                    "responseStatus": "needsAction",
                    "optional": true,
                })
            })
            .collect();
        let body = json!({
            "id": id,
            "summary": quotes(EVENT_TEXT_MAX_BYTES + 1),
            // Four bytes a character, the widest a character serializes
            // without escaping, so `max_chars` of them pass the ceiling.
            "description": "😀".repeat(MAX_CHARS_MAX as usize + 1),
            "location": quotes(EVENT_TEXT_MAX_BYTES + 1),
            "status": quotes(SHORT_MAX_BYTES + 1),
            "start": when(),
            "end": when(),
            "organizer": {
                "email": quotes(ACCOUNT_MAX_BYTES + 1),
                "displayName": quotes(EVENT_TEXT_MAX_BYTES + 1),
                "self": true,
            },
            "attendees": attendees,
            "recurrence": vec![quotes(EVENT_TEXT_MAX_BYTES + 1); RECURRENCE_MAX + 1],
            "recurringEventId": id,
            "hangoutLink": quotes(EVENT_TEXT_MAX_BYTES + 1),
        });
        let state = event_state(json!({ "id": id, "max_chars": MAX_CHARS_MAX }));
        let answer = completed(answered(&state, http(200, &body.to_string())));
        let mut without = answer.clone();
        without.as_object_mut().unwrap().remove("description");
        let rest = serialized_len(&without);
        assert!(rest < 126_000, "{rest}");
        // The description is fitted into what is left, never emptied.
        let kept = answer["description"].as_str().unwrap().chars().count();
        assert!(kept > 30_000 && kept < MAX_CHARS_MAX as usize, "{kept}");
        assert_eq!(answer["description_truncated"], json!(true));
        assert_eq!(answer["description_chars"], json!(MAX_CHARS_MAX + 1));
        let size = serialized_len(&answer);
        assert!(
            size <= COMPACT_RESULT_CEILING && size > COMPACT_RESULT_CEILING - 4,
            "{size}"
        );
    }

    fn free_busy(args: Value) -> Result<ApiCall, String> {
        plan(Provided::CalendarFreeBusy, &args, &settings()).map(|call| {
            assert_eq!(call.step, 1);
            call.api
        })
    }

    /// The JSON body a free/busy call carries.
    fn free_busy_body(call: &ApiCall) -> Value {
        serde_json::from_str(call.body.as_deref().expect("a POST carries a body")).unwrap()
    }

    #[test]
    fn free_busy_posts_one_query_for_primary_on_the_calendar_host() {
        let call = free_busy(json!({ "from": FROM, "to": TO })).unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: None,
                method: "POST".to_string(),
                host: "calendar.example.com".to_string(),
                path: "/calendar/v3/freeBusy".to_string(),
                query: query([("prettyPrint", "false".to_string())]),
                headers: BTreeMap::new(),
                body: call.body.clone(),
            }
        );
        assert_eq!(
            free_busy_body(&call),
            json!({
                "timeMin": FROM,
                "timeMax": TO,
                "items": [{ "id": "primary" }],
                "calendarExpansionMax": 1,
            })
        );
        assert_eq!(
            serde_json::to_value(&call).unwrap()["body"],
            json!(call.body.as_deref().unwrap())
        );

        let holidays = "en.usa#holiday@group.v.calendar.google.com";
        let call = free_busy(json!({
            "from": FROM,
            "to": TO,
            "calendars": ["primary", holidays],
            "account": "alice@example.com",
        }))
        .unwrap();
        assert_eq!(call.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            free_busy_body(&call),
            json!({
                "timeMin": FROM,
                "timeMax": TO,
                "items": [{ "id": "primary" }, { "id": holidays }],
                "calendarExpansionMax": 2,
            })
        );
        // A GET carries no body, and `api-request` is not sent one.
        let get = event(json!({ "id": "e1" })).unwrap();
        assert!(serde_json::to_value(&get).unwrap().get("body").is_none());
    }

    #[test]
    fn free_busy_refuses_bad_arguments() {
        let window = |extra: Value| {
            let mut args = json!({ "from": FROM, "to": TO });
            args.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            args
        };
        let count = "`calendars` must be an array of 1 to 50 calendar ids";
        let bad_id = "each of `calendars` must be a calendar id of 1 to 320 characters with no `/`, \
                      `\\`, `%`, `\"`, space or control character";
        for (args, refusal) in [
            (json!({ "to": TO }), "`from` is required".to_string()),
            (json!({ "from": FROM }), "`to` is required".to_string()),
            (
                json!({ "from": " ", "to": TO }),
                "`from` is empty".to_string(),
            ),
            (
                json!({ "from": FROM, "to": "t".repeat(TIME_MAX_CHARS + 1) }),
                "`to` is longer than 64 characters".to_string(),
            ),
            (window(json!({ "calendars": [] })), count.to_string()),
            (
                window(json!({ "calendars": vec!["c"; FREEBUSY_CALENDARS_MAX + 1] })),
                count.to_string(),
            ),
            (window(json!({ "calendars": "primary" })), count.to_string()),
            (window(json!({ "calendars": ["a/b"] })), bad_id.to_string()),
            (window(json!({ "calendars": ["a b"] })), bad_id.to_string()),
            (
                window(json!({ "calendars": ["c".repeat(CALENDAR_ID_MAX_CHARS + 1)] })),
                bad_id.to_string(),
            ),
            (window(json!({ "calendars": [1] })), bad_id.to_string()),
            (
                window(json!({ "calendars": ["primary", "bob@example.com", "primary"] })),
                "`calendars` names \"primary\" twice".to_string(),
            ),
            (
                window(json!({ "max": 5 })),
                "unknown argument `max`; this tool takes `from`, `to`, `calendars`, `account`"
                    .to_string(),
            ),
        ] {
            assert_eq!(free_busy(args.clone()).unwrap_err(), refusal, "{args}");
        }
        let fifty: Vec<String> = (0..FREEBUSY_CALENDARS_MAX)
            .map(|i| format!("c{i}"))
            .collect();
        let call = free_busy(window(json!({ "calendars": fifty }))).unwrap();
        assert_eq!(free_busy_body(&call)["calendarExpansionMax"], json!(50));
    }

    const FREE_BUSY: &str = include_str!("fixtures/free-busy.json");
    const FREE_BUSY_CALENDARS: [&str; 3] = [
        "primary",
        "en.usa#holiday@group.v.calendar.google.com",
        "bob@example.com",
    ];

    fn free_busy_state(args: Value) -> InFlight {
        let call = plan(Provided::CalendarFreeBusy, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::CalendarFreeBusy,
            &args,
            &call,
            Map::new(),
            1_000,
        )
    }

    /// The fixture's three calendars asked, answered `body`.
    fn read_free_busy(body: &str) -> Next {
        let state = free_busy_state(json!({
            "from": FROM,
            "to": TO,
            "calendars": FREE_BUSY_CALENDARS,
        }));
        answered(&state, http(200, body))
    }

    /// The fixture with its `calendars` entry `id` replaced by `entry`, or
    /// removed when `entry` is null.
    fn free_busy_with(id: &str, entry: Value) -> String {
        let mut body: Value = serde_json::from_str(FREE_BUSY).unwrap();
        let calendars = body["calendars"].as_object_mut().unwrap();
        if entry.is_null() {
            calendars.remove(id);
        } else {
            calendars.insert(id.to_string(), entry);
        }
        body.to_string()
    }

    #[test]
    fn free_busy_answers_each_calendar_asked() {
        assert_eq!(
            completed(read_free_busy(FREE_BUSY)),
            json!({
                "from": "2026-11-03T05:00:00.000Z",
                "to": "2026-11-04T05:00:00.000Z",
                "calendars": {
                    "primary": { "busy": [
                        { "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" },
                        { "start": "2026-11-03T18:30:00Z", "end": "2026-11-03T19:00:00Z" },
                    ] },
                    // Free all window: an empty list, not an absent one.
                    "en.usa#holiday@group.v.calendar.google.com": { "busy": [] },
                    // Unknown, not free: Google's empty `busy` is left out.
                    "bob@example.com": { "errors": ["notFound"] },
                },
            })
        );
        // Busy intervals beside an error are kept.
        let partial = free_busy_with(
            "bob@example.com",
            json!({
                "errors": [{ "domain": "global", "reason": "tooManyCalendarsRequested" }],
                "busy": [{ "start": "2026-11-03T16:00:00Z", "end": "2026-11-03T17:00:00Z" }],
            }),
        );
        assert_eq!(
            completed(read_free_busy(&partial))["calendars"]["bob@example.com"],
            json!({
                "busy": [{ "start": "2026-11-03T16:00:00Z", "end": "2026-11-03T17:00:00Z" }],
                "errors": ["tooManyCalendarsRequested"],
            })
        );
    }

    #[test]
    fn free_busy_vocabulary_is_plain_and_cut_unflagged() {
        let long = format!("2026-11-03T14:00:00Z\u{7}{}", "9".repeat(SHORT_MAX_BYTES));
        let body = free_busy_with(
            "primary",
            json!({ "busy": [{ "start": long, "end": "2026-11-03T15:00:00Z" }] }),
        );
        let answer = completed(read_free_busy(&body));
        let start = answer["calendars"]["primary"]["busy"][0]["start"]
            .as_str()
            .unwrap();
        // Cut at the bound, `…` after it.
        assert_eq!(start.len(), SHORT_MAX_BYTES + '…'.len_utf8());
        assert!(
            start.starts_with("2026-11-03T14:00:00Z ") && start.ends_with('…'),
            "{start}"
        );
        assert!(
            answer["calendars"]["primary"]["busy"][0]
                .get("fields_truncated")
                .is_none()
        );
        let body = free_busy_with(
            "bob@example.com",
            json!({ "errors": [{ "domain": "global", "reason": "r".repeat(SHORT_MAX_BYTES + 1) }] }),
        );
        let answer = completed(read_free_busy(&body));
        assert_eq!(
            answer["calendars"]["bob@example.com"]["errors"][0]
                .as_str()
                .unwrap()
                .len(),
            SHORT_MAX_BYTES + '…'.len_utf8()
        );
    }

    #[test]
    fn free_busy_refuses_an_unexpected_shape() {
        let mut no_calendars: Value = serde_json::from_str(FREE_BUSY).unwrap();
        no_calendars.as_object_mut().unwrap().remove("calendars");
        for body in [
            "[]".to_string(),
            no_calendars.to_string(),
            json!({ "calendars": [] }).to_string(),
            // A calendar that says nothing, or is not of Google's shape.
            free_busy_with("primary", json!({})),
            free_busy_with("primary", json!([])),
            free_busy_with("primary", json!({ "busy": {} })),
            free_busy_with(
                "primary",
                json!({ "busy": [{ "start": "2026-11-03T14:00:00Z" }] }),
            ),
            free_busy_with("primary", json!({ "busy": [{ "start": 1, "end": 2 }] })),
            free_busy_with("primary", json!({ "busy": ["2026-11-03T14:00:00Z"] })),
            free_busy_with("primary", json!({ "errors": [{ "domain": "global" }] })),
            free_busy_with("primary", json!({ "errors": "notFound" })),
            free_busy_with("primary", json!({ "errors": [], "busy": "x" })),
            json!({ "timeMin": 1, "calendars": {} }).to_string(),
        ] {
            assert_eq!(
                read_free_busy(&body),
                Next::Fail(failure(
                    ErrorKind::Internal,
                    "unexpected Google freebusy.query response shape"
                )),
                "{body}"
            );
        }
    }

    #[test]
    fn free_busy_names_the_calendars_asked_and_answered_apart() {
        let shape = "unexpected Google freebusy.query response shape";
        let mut stranger: Value = serde_json::from_str(FREE_BUSY).unwrap();
        stranger["calendars"]["carol@example.com"] = json!({ "busy": [] });
        let mut members: Value = serde_json::from_str(FREE_BUSY).unwrap();
        let calendars = members["calendars"].as_object_mut().unwrap();
        calendars.remove("bob@example.com");
        for n in 0..7 {
            calendars.insert(format!("member{n}@example.com"), json!({ "busy": [] }));
        }
        for (body, detail) in [
            (
                free_busy_with("primary", Value::Null),
                format!(
                    "{shape}: calendars answered but not asked: none; calendars asked but not \
                     answered: \"primary\""
                ),
            ),
            (
                stranger.to_string(),
                format!(
                    "{shape}: calendars answered but not asked: \"carol@example.com\"; calendars \
                     asked but not answered: none"
                ),
            ),
            (
                members.to_string(),
                format!(
                    "{shape}: calendars answered but not asked: \"member0@example.com\", \
                     \"member1@example.com\", \"member2@example.com\", \"member3@example.com\", \
                     \"member4@example.com\" and 2 more; calendars asked but not answered: \
                     \"bob@example.com\""
                ),
            ),
        ] {
            assert_eq!(
                read_free_busy(&body),
                Next::Fail(failure(ErrorKind::Internal, &detail)),
                "{body}"
            );
        }
    }

    #[test]
    fn free_busy_relays_googles_errors_with_the_calendar_hint() {
        let state = free_busy_state(json!({ "from": "monday", "to": TO }));
        assert_eq!(
            answered(
                &state,
                http(400, &google(400, "INVALID_ARGUMENT", "Bad Request"))
            ),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "Google INVALID_ARGUMENT (HTTP 400): Bad Request"
            ))
        );
        let Next::Fail(Failure { kind, detail }) = answered(
            &state,
            http(
                403,
                &google(403, "PERMISSION_DENIED", "Insufficient Permission"),
            ),
        ) else {
            panic!("a 403 did not fail");
        };
        assert_eq!(kind, ErrorKind::Denied);
        assert!(detail.ends_with(&scope_hint(Family::Calendar)), "{detail}");
    }

    /// A free/busy body whose answer, once compacted, serializes to exactly
    /// `size` bytes: `primary` busy with as many intervals as fit, the first
    /// ones' `end` padded (within `SHORT_MAX_BYTES`) to land on it.
    fn free_busy_answering(size: usize) -> String {
        let interval = || json!({ "start": "2026-11-03T14:00:00Z", "end": "2026-11-03T15:00:00Z" });
        let body = |busy: &[Value]| json!({ "timeMin": "f", "timeMax": "t", "calendars": { "primary": { "busy": busy } } });
        let compacted = |busy: &[Value]| {
            serialized_len(
                &json!({ "from": "f", "to": "t", "calendars": { "primary": { "busy": busy } } }),
            )
        };
        let mut busy = vec![interval()];
        while compacted(&busy) + serialized_len(&interval()) < size {
            busy.push(interval());
        }
        let mut deficit = size - compacted(&busy);
        for item in &mut busy {
            let end = item["end"].as_str().unwrap().to_string();
            let pad = deficit.min(SHORT_MAX_BYTES - end.len());
            item["end"] = json!(format!("{end}{}", "0".repeat(pad)));
            deficit -= pad;
        }
        assert_eq!(deficit, 0);
        body(&busy).to_string()
    }

    #[test]
    fn free_busy_past_the_ceiling_is_refused_with_what_to_narrow() {
        let state = free_busy_state(json!({ "from": FROM, "to": TO }));
        for size in [COMPACT_RESULT_CEILING - 1, COMPACT_RESULT_CEILING] {
            let answer = completed(answered(&state, http(200, &free_busy_answering(size))));
            assert_eq!(serialized_len(&answer), size);
        }
        let over = COMPACT_RESULT_CEILING + 1;
        assert_eq!(
            answered(&state, http(200, &free_busy_answering(over))),
            Next::Fail(failure(
                ErrorKind::Internal,
                &format!(
                    "compact result of {over} bytes exceeds the wrapper's ceiling; ask for \
                     a shorter `from`/`to` window, or fewer `calendars`"
                )
            ))
        );
    }

    #[test]
    fn every_step_answers_an_unreadable_read_by_its_shape() {
        let refusal = "result exceeds [messaging] max_body_bytes";
        let outcomes = || {
            [
                (
                    cut_read(json!(300000)),
                    Unreadable::Cut("300000 bytes".to_string()),
                ),
                (
                    nested("internal", refusal),
                    Unreadable::Refused(refusal.to_string()),
                ),
            ]
        };
        let at = |mut state: InFlight, step: u8| {
            state.step = step;
            state
        };
        let name = |state: &InFlight| format!("{} step {}", state.tool.name(), state.step);

        // Neither one of many nor with a narrower form: the rendering alone.
        for state in [
            labels_state(json!({})),
            search_state(json!({ "q": "lunch" })),
            at(read_state(json!({ "id": "m1" })), STRUCTURE_STEP),
            at(thread_state(json!({ "id": "t1" })), THREAD_NARROW_STEP),
            calendar_list_state(json!({})),
            event_state(json!({ "id": "e1" })),
            at(search_state(json!({ "q": "lunch" })), LABELS_STEP),
            at(read_state(json!({ "id": "m1" })), LABELS_STEP),
            at(thread_state(json!({ "id": "t1" })), LABELS_STEP),
        ] {
            for (outcome, why) in outcomes() {
                assert_eq!(
                    answered(&state, outcome),
                    Next::Fail(why.failure(None)),
                    "{}",
                    name(&state)
                );
            }
        }

        // A window the caller can narrow: the rendering and what to narrow.
        for (state, hint, wording) in [
            (
                events_state(json!({ "from": FROM, "to": TO })),
                EVENTS_HINT,
                "; ask for fewer events with `max`, or a shorter `from`/`to` window",
            ),
            (
                free_busy_state(json!({ "from": FROM, "to": TO })),
                FREEBUSY_HINT,
                "; ask for a shorter `from`/`to` window, or fewer `calendars`",
            ),
        ] {
            for (outcome, why) in outcomes() {
                let answer = answered(&state, outcome);
                assert_eq!(
                    answer,
                    Next::Fail(why.failure(Some(hint))),
                    "{}",
                    name(&state)
                );
                let Next::Fail(Failure { detail, .. }) = answer else {
                    panic!("{answer:?}");
                };
                assert!(detail.ends_with(wording), "{detail}");
            }
        }

        // A narrower form: one re-read, carrying why.
        for (state, step) in [
            (read_state(json!({ "id": "m1" })), STRUCTURE_STEP),
            (thread_state(json!({ "id": "t1" })), THREAD_NARROW_STEP),
        ] {
            for (outcome, why) in outcomes() {
                let Next::Call { call, derived } = answered(&state, outcome) else {
                    panic!("{} did not read again", name(&state));
                };
                assert_eq!(call.step, step, "{}", name(&state));
                assert_eq!(derived, why.to_derived());
            }
        }

        // One of many: every per-message step of a full page lists the
        // message unavailable and goes on.
        let ids: Vec<String> = (1..=25).map(|i| format!("m{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let state = search_state(json!({ "q": "lunch", "max": 25 }));
        let (row, _) = follow(
            &state,
            answered(&state, http(200, &listing(&id_refs, json!({})))),
        );
        for step in SEARCH_FIRST_GET..=SEARCH_LAST_GET {
            let index = usize::from(step - SEARCH_FIRST_GET);
            for (outcome, why) in outcomes() {
                let entry = json!([{ "id": ids[index], "reason": why.render(None) }]);
                match answered(&at(row.clone(), step), outcome) {
                    Next::Call { call, derived } => {
                        assert!(step < SEARCH_LAST_GET, "{step}");
                        assert_eq!(call.step, step + 1);
                        assert_eq!(
                            call.api.path,
                            format!("/gmail/v1/users/me/messages/{}", ids[index + 1])
                        );
                        assert_eq!(derived["unavailable"], entry);
                    }
                    Next::Complete(answer) => {
                        assert_eq!(step, SEARCH_LAST_GET);
                        assert_eq!(answer, json!({ "messages": [], "unavailable": entry }));
                    }
                    other => panic!("step {step}: {other:?}"),
                }
            }
        }

        // A step the tool does not have is refused as such.
        for (outcome, _) in outcomes() {
            assert_eq!(
                answered(&at(labels_state(json!({})), 2), outcome),
                Next::Fail(no_such_step(Provided::GmailLabels, 2))
            );
        }
    }

    #[test]
    fn every_tools_first_read_has_an_unreadable_answer() {
        // `unreadable` runs only on a cut or refused read, so a tool missing
        // its arm there would pass every other test and be answered as a
        // step it does not have.
        for tool in Provided::ALL {
            let mut state = labels_state(json!({}));
            state.tool = tool;
            for outcome in [
                cut_read(json!(300000)),
                nested("internal", "result exceeds [messaging] max_body_bytes"),
            ] {
                assert_ne!(
                    answered(&state, outcome),
                    Next::Fail(no_such_step(tool, 1)),
                    "{}",
                    tool.name()
                );
            }
        }
    }
}
