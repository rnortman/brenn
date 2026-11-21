//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `GmailLabels`', `GmailSearch`'s, `GmailRead`'s, `GmailThread`'s,
//! `GmailAttachment`'s and `GmailBody`'s arguments, building their Gmail calls
//! as `api-request` arguments, reading `api-request`'s answer, compacting
//! Google's JSON into the tool's result, decoding a fetched attachment from
//! one blob into another, and writing a message's body forms as blobs from
//! the blob Gmail's answer arrived in, through the kit's [`Step`]. Nothing here names the guest SDK, so
//! the whole module runs on the host under test. Tools are the generated `Provided` enum, whose SDK-facing
//! parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hasher};

use brenn_google_workspace::args::{
    QUERY_MAX_CHARS, account, id_arg, id_list, is_token, max_chars,
};
use brenn_google_workspace::bytes;
use brenn_google_workspace::call::get_call;
use brenn_google_workspace::gmail::attachment::{DecodeError, Decoder, part_tree_fields};
use brenn_google_workspace::gmail::{
    ATTACHMENT_PART_DEPTH, HEADER_MAX_BYTES, MESSAGES_PATH, PART_ID_MAX_CHARS, decode, gmail_id,
    is_gmail_id, is_part_id, metadata_query, part_tree_mask, raw_header,
};
use brenn_google_workspace::scope::{Scope, google_error, no_body_refused};
use brenn_google_workspace::settings::Settings;
use brenn_google_workspace::text::{
    PAGE_MAX_CHARS, bounded_quote, fit_text, next_page, plain, plain_byte, plain_changes,
    put_cut_text, shape,
};
use brenn_provider_kit::{
    Answer, ApiCall, Append, Args, Blob, BlobAnswer, COMPACT_RESULT_CEILING, Call, CreateError,
    ErrorKind, Failure, Logic, NestedError, Next, Query, QueryValue, Step, Tool, Unreadable,
    bounded, capped, copy, cut, flag, google, integer, internal, no_such_step, object, query,
    read_api_blob, read_api_result, serialized_len, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;
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
/// `GmailRead`'s second step: the message's structure, read when the full
/// message's read was cut at the provider's read limit or refused by the host.
const STRUCTURE_STEP: u8 = 2;
/// Where `GmailRead` sends a caller whose message it answered without a body:
/// `GmailBody`'s answer is a blob, which neither `max_response_bytes` nor the
/// bus cap bounds.
const TO_GMAIL_BODY: &str = "GmailBody answers the body as files";
/// What the structure mask lists of the message itself.
const STRUCTURE_MESSAGE_FIELDS: &str = "id,threadId,labelIds,snippet,internalDate,sizeEstimate";
/// What the structure mask lists of each part: everything but `data`.
const STRUCTURE_PART_FIELDS: &str = "partId,mimeType,filename,headers,body(size,attachmentId)";
/// How many levels of parts below the payload the structure mask lists.
const STRUCTURE_PART_DEPTH: usize = 3;

/// messages.get's structure mask: the message's fields, and the tree listed
/// to the payload and [`STRUCTURE_PART_DEPTH`] levels of parts below it.
fn structure_fields() -> String {
    format!(
        "{STRUCTURE_MESSAGE_FIELDS},{}",
        part_tree_mask(STRUCTURE_PART_FIELDS, STRUCTURE_PART_DEPTH)
    )
}

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

/// `GmailSearch`'s `max`: the range and the default.
const SEARCH_MAX_MIN: u64 = 1;
const SEARCH_MAX_MAX: u64 = 25;
const SEARCH_MAX_DEFAULT: u64 = 10;
/// `GmailSearch`'s per-message steps: listed id `i` is read at step `2 + i`.
const SEARCH_FIRST_GET: u8 = 2;
const SEARCH_LAST_GET: u8 = SEARCH_FIRST_GET + SEARCH_MAX_MAX as u8 - 1;

/// `GmailAttachment`'s second step: the attachment's body, asked for as an
/// attachment of `api-request`'s answer, decoded into a blob of its own.
const FETCH_STEP: u8 = 2;
/// The API `GmailAttachment`'s second step reads.
const ATTACHMENTS_GET: &str = "messages.attachments.get";
/// The largest attachment `GmailAttachment` returns, in decoded bytes:
/// Gmail's own ceiling for received mail, and the largest round size whose
/// base64url JSON (about 4/3 of it) fits the host's default largest blob.
pub const MAX_ATTACHMENT_BYTES: u64 = 50_000_000;
/// The most characters of an `attachmentId` the fetch puts in its path. Gmail
/// documents no length; its ids are a few hundred characters.
const ATTACHMENT_ID_MAX_CHARS: usize = 4096;
/// The name of the completion's attachment that holds the decoded file.
const FILE_ATTACHMENT: &str = "attachment";
/// The content type of a file whose part names none the host accepts.
const OCTET_STREAM: &str = "application/octet-stream";

/// The largest answer from Gmail `GmailBody` reads, in bytes of the JSON as
/// it arrives: the whole answer is held in the guest, parsed, and turned
/// into its forms there, so the bound is what fits the guest's fuel and
/// memory. Checked on the answer's blob before a byte of it is read. Memory
/// sets it: it is the largest multiple of 128 KiB at which every answer the
/// bound's tests build completes in three quarters of the guest's memory,
/// peaking at no more than 8.69 MiB. What fails one step higher is a shape
/// those tests build by hand, not a message known to arrive: an answer of
/// about 69,000 parts, each only `{"mimeType":"x"}`, needs nearly all 16 MiB.
/// Whether Gmail sends an answer of that shape is not established; a part
/// that also carries a `partId`, `filename`, `headers` or `body` is longer,
/// so fewer fit under the bound. Fuel at the bound is about 125 a byte of
/// answer for the costliest of those shapes in fuel, an answer of many
/// one-character HTML parts, against the 216.9 that 1/1.4 of the
/// activation's floor and attachment grant gives at that size.
// TODO(gmail-body-forms-above-bound): answer the forms of a message whose
// answer is over this bound.
pub const BODY_ANSWER_MAX: u64 = 1024 * 1024;
/// The routes a message over `GmailBody`'s bound still has.
const BODY_ROUTES: &str = "GmailRead answers its text cut, and a caller granted api-request \
                           reads the whole message with `format=raw` and `response_attachment` \
                           (docs/providers/google.md)";
/// The content types of the forms.
const TEXT_PLAIN_UTF8: &str = "text/plain; charset=utf-8";
const TEXT_HTML_UTF8: &str = "text/html; charset=utf-8";

/// The scope `tool`'s step `step` needs. A step the tool does not have is
/// never asked for: `advance` maps an error only for a step it issued.
fn scope(tool: Provided, step: u8) -> Scope {
    match (tool, step) {
        (
            Provided::GmailLabels
            | Provided::GmailSearch
            | Provided::GmailRead
            | Provided::GmailThread
            | Provided::GmailAttachment
            | Provided::GmailBody,
            _,
        ) => Scope::GmailRead,
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

/// This component's step functions, for the kit's activation loop.
pub struct GmailReads;

impl Logic for GmailReads {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(
        tool: Provided,
        args: &Value,
        _: &Step<'_>,
        settings: &Settings,
    ) -> Result<Call, String> {
        plan(tool, args, settings)
    }

    fn advance(
        state: &InFlight,
        outcome: Result<Value, NestedError>,
        step: &mut Step<'_>,
        settings: &Settings,
    ) -> Next {
        advance(state, outcome, step, settings)
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
                api: thread_call(&thread, THREAD_HEADERS, settings),
            })
        }
        Provided::GmailAttachment => {
            let attachment = AttachmentArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: part_tree_call(&attachment, settings),
            })
        }
        Provided::GmailBody => {
            let body = BodyArgs::parse(args)?;
            Ok(Call {
                step: 1,
                api: body_call(&body, settings),
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
        args.account.as_deref(),
        format!("{THREADS_PATH}/{}", args.id),
        metadata_query(THREAD_FIELDS, headers),
    )
}

/// `GmailAttachment`'s arguments: the message and the part of it to fetch.
struct AttachmentArgs {
    id: String,
    part_id: String,
    account: Option<String>,
}

impl AttachmentArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "part_id", "account"];

    fn parse(args: &Value) -> Result<AttachmentArgs, String> {
        let map = object(args, Self::KEYS)?;
        Ok(AttachmentArgs {
            id: gmail_id(map, "id")?.ok_or("`id` is required")?,
            part_id: id_arg(map, "part_id", is_part_id, || {
                format!("1 to {PART_ID_MAX_CHARS} letters, digits or `.`")
            })?
            .ok_or("`part_id` is required")?,
            account: account(map)?,
        })
    }
}

/// messages.get of the message's part tree to [`ATTACHMENT_PART_DEPTH`]
/// levels below the payload, without any part's data.
fn part_tree_call(args: &AttachmentArgs, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        args.account.as_deref(),
        format!("{MESSAGES_PATH}/{}", args.id),
        query([
            ("format", "full".to_string()),
            ("fields", part_tree_fields()),
        ]),
    )
}

/// messages.attachments.get of `attachment_id`, its body answered as an
/// attachment.
fn attachment_call(args: &AttachmentArgs, attachment_id: &str, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        args.account.as_deref(),
        format!("{MESSAGES_PATH}/{}/attachments/{attachment_id}", args.id),
        Query::new(),
    )
    .answer_as_attachment()
}

/// A form of a message's body `GmailBody` answers, in the order the forms are
/// written and attached.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Form {
    /// The text `GmailRead` answers, uncut.
    Text,
    /// The HTML view's parts, decoded.
    Html,
    /// The HTML view's parts read as text.
    Rendered,
}

impl Form {
    const ALL: [Form; 3] = [Form::Text, Form::Html, Form::Rendered];

    /// The form's name, as `forms` takes it and as its attachment is named.
    fn name(self) -> &'static str {
        match self {
            Form::Text => "text",
            Form::Html => "html",
            Form::Rendered => "rendered",
        }
    }

    fn from_name(name: &str) -> Option<Form> {
        Form::ALL.into_iter().find(|form| form.name() == name)
    }
}

/// `GmailBody`'s arguments: the message, and its forms in [`Form::ALL`]
/// order.
struct BodyArgs {
    id: String,
    forms: Vec<Form>,
    account: Option<String>,
}

impl BodyArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["id", "forms", "account"];

    fn parse(args: &Value) -> Result<BodyArgs, String> {
        let map = object(args, Self::KEYS)?;
        let id = gmail_id(map, "id")?.ok_or("`id` is required")?;
        let names = id_list(
            map,
            "forms",
            (1, Form::ALL.len()),
            "form names",
            |name| Form::from_name(name).is_some(),
            || "one of `text`, `html` and `rendered`".to_string(),
        )?
        .ok_or("`forms` is required")?;
        let mut forms: Vec<Form> = names
            .iter()
            .map(|name| Form::from_name(name).expect("id_list admitted only form names"))
            .collect();
        forms.sort_unstable();
        Ok(BodyArgs {
            id,
            forms,
            account: account(map)?,
        })
    }

    fn wants(&self, form: Form) -> bool {
        self.forms.contains(&form)
    }
}

/// messages.get of the whole message, its answer asked for as an attachment.
fn body_call(args: &BodyArgs, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        args.account.as_deref(),
        format!("{MESSAGES_PATH}/{}", args.id),
        query([("format", "full".to_string())]),
    )
    .answer_as_attachment()
}

/// labels.list for the names of the user labels an answer carries.
fn label_names_call(account: Option<&str>, settings: &Settings) -> ApiCall {
    get_call(
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

/// `why`'s rendering as copied into an answer: through `plain`, cut to
/// `google::MESSAGE_MAX_BYTES`, since a refusal's detail is the host's and
/// unbounded.
fn quoted(why: &Unreadable, estimate: Option<u64>) -> String {
    cut(
        &plain(&why.render(
            google::API_NAME,
            estimate.map(|n| format!("sizeEstimate {n}")).as_deref(),
        )),
        google::MESSAGE_MAX_BYTES,
    )
    .0
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step. Every answer completed here is held to the result
/// ceiling; a relayed credential answer is `api-request`'s own and is not.
pub fn advance(
    state: &InFlight,
    outcome: Result<Value, NestedError>,
    step: &mut Step<'_>,
    settings: &Settings,
) -> Next {
    match (state.tool, state.step) {
        (Provided::GmailAttachment, FETCH_STEP) => {
            return attachment_fetched(state, outcome, step);
        }
        (Provided::GmailBody, 1) => return body_read(state, outcome, step),
        _ => {}
    }
    let body = match read_api_result(outcome, &state.target, google::API_NAME) {
        Ok(Answer::Body(body)) => body,
        Ok(Answer::NoBody { status }) => return settle(state.tool, no_body(state, status)),
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
            return Next::Fail(google_error(status, &body, scope(state.tool, state.step)));
        }
        Ok(Answer::Bytes { status, .. }) => {
            return Next::Fail(internal(format!(
                "{} returned a non-text body ({status})",
                google::API_NAME
            )));
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
            (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
                named(state, &body)
            }
            (Provided::GmailAttachment, 1) => part_located(state, &body, settings),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested read that was cut or refused, decided by the step's shape: a
/// per-message read of a search page is listed unavailable and the page goes
/// on; a message's or a thread's first read is asked for again narrower,
/// carrying why; a message's narrower read fails with why and the route to
/// its body; every other step fails with why.
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
                    api: read_call(&args, Some(&structure_fields()), settings),
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
            (Provided::GmailRead, STRUCTURE_STEP) => {
                Err(why.failure(google::API_NAME, Some(TO_GMAIL_BODY)))
            }
            (Provided::GmailLabels, 1)
            | (Provided::GmailSearch, 1)
            | (Provided::GmailAttachment, 1)
            | (Provided::GmailThread, THREAD_NARROW_STEP)
            | (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
                Err(why.failure(google::API_NAME, None))
            }
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A 2xx with no body, which no step here accepts: refused, naming the API
/// whose answer it expected, as that step's `shape` failure names it.
fn no_body(state: &InFlight, status: u64) -> Result<Next, Failure> {
    let (api, answers) = match (state.tool, state.step) {
        (Provided::GmailLabels, 1) => match stored(state, LabelsArgs::parse) {
            Ok(LabelsArgs { id: Some(_), .. }) => ("labels.get", "JSON"),
            Ok(LabelsArgs { id: None, .. }) => ("labels.list", "JSON"),
            Err(failure) => return Err(failure),
        },
        (Provided::GmailSearch, 1) => ("messages.list", "JSON"),
        (Provided::GmailSearch, SEARCH_FIRST_GET..=SEARCH_LAST_GET)
        | (Provided::GmailRead, 1 | STRUCTURE_STEP)
        | (Provided::GmailAttachment, 1) => ("messages.get", "JSON"),
        (Provided::GmailThread, 1 | THREAD_NARROW_STEP) => ("threads.get", "JSON"),
        (Provided::GmailSearch | Provided::GmailRead | Provided::GmailThread, LABELS_STEP) => {
            ("labels.list", "JSON")
        }
        (tool, step) => return Err(no_such_step(tool, step)),
    };
    Err(no_body_refused(status, api, answers))
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling — after `GmailRead`'s text is cut, or `GmailThread`'s
/// oldest messages dropped, to fit it.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let value = match tool {
                Provided::GmailRead => fit_text(value, "text", COMPACT_RESULT_CEILING),
                Provided::GmailThread => fit_thread(value, COMPACT_RESULT_CEILING),
                Provided::GmailLabels
                | Provided::GmailSearch
                | Provided::GmailAttachment
                | Provided::GmailBody => value,
            };
            bounded(value, COMPACT_RESULT_CEILING, None)
        }
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}
/// An `invalid_args` failure carrying `detail`: the caller named something
/// this tool cannot return, and a retry cannot help.
fn invalid_args(detail: String) -> Failure {
    Failure {
        kind: ErrorKind::InvalidArgs,
        detail,
    }
}

/// `GmailAttachment`'s part-tree read: the part named `part_id`, found within
/// [`ATTACHMENT_PART_DEPTH`] levels of the payload, fetched next by its
/// `attachmentId`, its `filename`, `mimeType` and `body.size` riding in
/// `derived`. A part that is not there, is carried inside the message, or is
/// over [`MAX_ATTACHMENT_BYTES`] is the caller's `invalid_args`.
fn part_located(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let args = stored(state, AttachmentArgs::parse)?;
    let api = "messages.get";
    let payload = body.get("payload").ok_or_else(|| shape(api))?;
    let Some(part) = find_part(payload, &args.part_id, 0, api)? else {
        return Err(invalid_args(format!(
            "message `{}` has no part `{}` within {ATTACHMENT_PART_DEPTH} levels of its payload, \
             the depth GmailAttachment reads; use a `part_id` GmailRead lists",
            args.id, args.part_id
        )));
    };
    let part_body = match part.get("body") {
        None => None,
        Some(Value::Object(part_body)) => Some(part_body),
        Some(_) => return Err(shape(api)),
    };
    let attachment_id = match part_body.and_then(|part_body| part_body.get("attachmentId")) {
        None => {
            return Err(invalid_args(format!(
                "part `{}` is carried inside the message, not as an attachment Gmail serves; \
                 read it with GmailRead, or whole with GmailBody when it is body text",
                args.part_id
            )));
        }
        Some(Value::String(id)) => id,
        Some(_) => return Err(shape(api)),
    };
    let size = part_body
        .and_then(|part_body| part_body.get("size"))
        .and_then(Value::as_u64)
        .ok_or_else(|| shape(api))?;
    if size > MAX_ATTACHMENT_BYTES {
        return Err(invalid_args(format!(
            "attachment of {size} bytes exceeds the {MAX_ATTACHMENT_BYTES} bytes GmailAttachment \
             returns"
        )));
    }
    if !is_token(attachment_id, ATTACHMENT_ID_MAX_CHARS) {
        return Err(shape(api));
    }
    let text = |key: &str| match part.get(key) {
        None => Ok(String::new()),
        Some(Value::String(text)) => Ok(bounded_quote(text)),
        Some(_) => Err(shape(api)),
    };
    let derived = Args::from_iter([
        ("filename".to_string(), json!(text("filename")?)),
        ("mime_type".to_string(), json!(text("mimeType")?)),
        ("size".to_string(), json!(size)),
    ]);
    Ok(Next::Call {
        call: Call {
            step: FETCH_STEP,
            api: attachment_call(&args, attachment_id, settings),
        },
        derived,
    })
}

/// The part of `part`'s tree, `part` at `depth` below the payload, whose
/// `partId` is `part_id`, searched to [`ATTACHMENT_PART_DEPTH`]. A part that
/// is not an object, or a `partId` or `parts` not of Gmail's shape, is
/// Google's fault.
fn find_part<'a>(
    part: &'a Value,
    part_id: &str,
    depth: usize,
    api: &str,
) -> Result<Option<&'a Value>, Failure> {
    let Value::Object(map) = part else {
        return Err(shape(api));
    };
    match map.get("partId") {
        Some(Value::String(id)) if id == part_id => return Ok(Some(part)),
        None | Some(Value::String(_)) => {}
        Some(_) => return Err(shape(api)),
    }
    if depth == ATTACHMENT_PART_DEPTH {
        return Ok(None);
    }
    match map.get("parts") {
        None => Ok(None),
        Some(Value::Array(parts)) => {
            for child in parts {
                if let Some(found) = find_part(child, part_id, depth + 1, api)? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        }
        Some(_) => Err(shape(api)),
    }
}

/// `GmailAttachment`'s fetch answered: Gmail's JSON, an attachment of the
/// answer, decoded into the completion's attachment [`FILE_ATTACHMENT`]; a
/// typed refusal of `api-request`'s, Google's error or an unreadable answer
/// fails the request.
fn attachment_fetched(
    state: &InFlight,
    outcome: Result<Value, NestedError>,
    step: &mut Step<'_>,
) -> Next {
    let answer = match read_api_blob(outcome, &state.target, google::API_NAME) {
        Ok(BlobAnswer::Blob { attachment, .. }) => attachment,
        Ok(BlobAnswer::Relay(value)) => return Next::Complete(value),
        Ok(BlobAnswer::Unreadable(why)) => {
            return Next::Fail(why.failure(google::API_NAME, None));
        }
        Ok(BlobAnswer::Error { status, body }) => {
            return Next::Fail(google_error(status, &body, scope(state.tool, state.step)));
        }
        Ok(BlobAnswer::TooLarge { limit, .. }) => {
            return Next::Fail(invalid_args(format!(
                "the attachment's encoded form exceeds the host's largest blob (max_blob_bytes, \
                 {limit} bytes)"
            )));
        }
        Ok(BlobAnswer::StoreFull) => {
            return Next::Fail(internal(
                "the host's blob store is full (blob_budget_bytes)",
            ));
        }
        Err(failure) => return Next::Fail(failure),
    };
    settle(state.tool, decode_attachment(state, &answer, step))
}

/// The text `derived[key]` the part-tree read wrote.
fn derived_text(derived: &Args, key: &str) -> Result<String, Failure> {
    derived
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| internal(format!("in-flight row carries no `{key}`")))
}

/// Gmail's answer, the step's answer attachment `answer`, read piece by piece
/// through the [`Decoder`] into a new blob of the part's `mimeType` (or
/// [`OCTET_STREAM`] when the host refuses it), attached to the completion.
/// Data that is not base64url, a stated `size` that is not the decoded
/// length, or a decoded length that is not the part's listed size fails the
/// request before the blob is sealed: a blob of the wrong bytes is never
/// handed over.
fn decode_attachment(state: &InFlight, answer: &str, step: &mut Step<'_>) -> Result<Next, Failure> {
    let args = stored(state, AttachmentArgs::parse)?;
    let filename = derived_text(&state.derived, "filename")?;
    let mime_type = derived_text(&state.derived, "mime_type")?;
    let listed = state
        .derived
        .get("size")
        .and_then(Value::as_u64)
        .ok_or_else(|| internal("in-flight row carries no `size`"))?;
    let undecodable = |error| match error {
        DecodeError::Shape => shape(ATTACHMENTS_GET),
        DecodeError::Base64 => internal(format!(
            "Gmail {ATTACHMENTS_GET} carries `data` that is not base64url"
        )),
    };
    let mismatch = |decoded: u64| {
        internal(format!(
            "Gmail listed part `{}` at {listed} bytes and its data decodes to {decoded}",
            args.part_id
        ))
    };
    let blob = step.open_answer(answer)?;
    let file = {
        let content_type = if mime_type.is_empty() {
            OCTET_STREAM
        } else {
            mime_type.as_str()
        };
        let mut file = match step.append(content_type) {
            Err(CreateError::InvalidContentType(_)) => step.append(OCTET_STREAM)?,
            created => created?,
        };
        let mut decoder = Decoder::new();
        let mut decoded = Vec::new();
        let mut total = 0u64;
        for piece in step.pieces(&blob) {
            decoded.clear();
            decoder.push(&piece?, &mut decoded).map_err(undecodable)?;
            total += decoded.len() as u64;
            if total > listed {
                return Err(mismatch(total));
            }
            file.write(&decoded)?;
        }
        let finished = decoder.finish().map_err(undecodable)?;
        if let Some(stated) = finished.size
            && stated != finished.decoded
        {
            return Err(internal(format!(
                "Gmail {ATTACHMENTS_GET} states size {stated} and its data decodes to {} bytes",
                finished.decoded
            )));
        }
        if finished.decoded != listed {
            return Err(mismatch(finished.decoded));
        }
        file.finish()?
    };
    step.attach(FILE_ATTACHMENT, file);
    Ok(Next::Complete(json!({
        "id": args.id,
        "part_id": args.part_id,
        "filename": filename,
        "mime_type": mime_type,
        "size": listed,
        "attachment": FILE_ATTACHMENT,
    })))
}

/// `GmailBody`'s read answered: Gmail's JSON, an attachment of the answer,
/// turned into the forms asked for; a typed refusal of `api-request`'s,
/// Google's error or an unreadable answer fails the request.
fn body_read(state: &InFlight, outcome: Result<Value, NestedError>, step: &mut Step<'_>) -> Next {
    let answer = match read_api_blob(outcome, &state.target, google::API_NAME) {
        Ok(BlobAnswer::Blob { attachment, .. }) => attachment,
        Ok(BlobAnswer::Relay(value)) => return Next::Complete(value),
        Ok(BlobAnswer::Unreadable(why)) => {
            return Next::Fail(why.failure(google::API_NAME, None));
        }
        Ok(BlobAnswer::Error { status, body }) => {
            return Next::Fail(google_error(status, &body, scope(state.tool, state.step)));
        }
        Ok(BlobAnswer::TooLarge { limit, .. }) => {
            return Next::Fail(invalid_args(format!(
                "the message's answer from Gmail exceeds the host's largest blob \
                 (max_blob_bytes, {limit} bytes); GmailRead answers its text cut"
            )));
        }
        Ok(BlobAnswer::StoreFull) => {
            return Next::Fail(internal(
                "the host's blob store is full (blob_budget_bytes)",
            ));
        }
        Err(failure) => return Next::Fail(failure),
    };
    settle(state.tool, body_forms(state, &answer, step))
}

/// Gmail's answer, the step's answer attachment `answer`, read whole when it
/// is at most [`BODY_ANSWER_MAX`] bytes and parsed into the message's part
/// tree, which borrows from it; the walk's leaves and the notes' sentences
/// made, the buffer is dropped, and then each form asked for
/// written to a blob of its own, in [`Form::ALL`] order, each leaf's text
/// dropped once no form left to write reads it. Every form is sealed before
/// the first is attached, so a failure attaches nothing.
fn body_forms(state: &InFlight, answer: &str, step: &mut Step<'_>) -> Result<Next, Failure> {
    let args = stored(state, BodyArgs::parse)?;
    let api = "messages.get";
    let blob = step.open_answer(answer)?;
    if blob.size() > BODY_ANSWER_MAX {
        return Err(invalid_args(format!(
            "the message's answer from Gmail is {} bytes, over the {BODY_ANSWER_MAX} bytes \
             GmailBody reads; {BODY_ROUTES}",
            blob.size()
        )));
    }
    let size = usize::try_from(blob.size()).expect("BODY_ANSWER_MAX fits the target's usize");
    let mut bytes = Vec::with_capacity(size);
    for piece in step.pieces(&blob) {
        bytes.extend_from_slice(&piece?);
    }
    // The tree and the notes borrow their strings from the answer, which is
    // held until the walk and the notes have read them.
    let message = decode::Message::from_slice(&bytes).map_err(|error| walk_failure(error, api))?;
    if message.id != args.id {
        return Err(shape(api));
    }
    let wanted = decode::Wanted {
        text: args.wants(Form::Text),
        html: args.wants(Form::Html),
        rendered: args.wants(Form::Rendered),
    };
    let (mut bodies, notes) =
        decode::bodies(&message.payload, wanted).map_err(|error| walk_failure(error, api))?;
    let answered = body_notes(&notes, bodies.by_reference)?;
    drop(notes);
    drop(message);
    drop(bytes);
    let max_write = usize::try_from(step.limits()?.max_write_bytes.max(1))
        .expect("a u32 fits the target's usize");

    let mut out = Args::new();
    out.insert("id".to_string(), json!(args.id));
    let mut made = Vec::with_capacity(args.forms.len());
    for (at, &form) in args.forms.iter().enumerate() {
        let (value, blob) = body_form(step, &bodies, form, max_write)?;
        out.insert(form.name().to_string(), value);
        if let Some(blob) = blob {
            made.push((form, blob));
        }
        release(&mut bodies, &args.forms[at + 1..]);
    }
    if !answered.is_empty() {
        out.insert(
            "decoding_notes".to_string(),
            Value::Array(answered.into_iter().map(Value::String).collect()),
        );
    }
    for (form, blob) in made {
        step.attach(form.name(), blob);
    }
    Ok(Next::Complete(Value::Object(out)))
}

/// `bodies`' decoding notes as answered: each sentence once, in the order
/// first said, then the sentence counting the parts that came by reference.
/// A message can make many distinct sentences; once their bytes alone are
/// over the result ceiling the answer would be too, so it is refused there.
/// The distinct notes are counted first at the length of their sentence
/// with nothing quoted, which no sentence is shorter than, so a message
/// whose notes are over the ceiling by that count is refused before any
/// sentence is made.
fn body_notes(notes: &[decode::Note<'_>], by_reference: usize) -> Result<Vec<String>, Failure> {
    let over = || {
        internal(format!(
            "the message's decoding notes alone are over the {COMPACT_RESULT_CEILING}-byte \
             result ceiling"
        ))
    };
    let charset_floor = note(&decode::Note::UnknownCharset("".into()), false).len();
    let utf8_floor = note(&decode::Note::InvalidUtf8, false).len();
    let children_floor = note(
        &decode::Note::UnlistedChildren {
            mime_type: "".into(),
            depth: 0,
        },
        false,
    )
    .len();
    let mut seen = Distinct::new(notes, note_hash, same_note);
    let mut distinct = Vec::new();
    let mut len = 0;
    for (at, one) in notes.iter().enumerate() {
        if !seen.first(at) {
            continue;
        }
        len += match one {
            decode::Note::UnknownCharset(_) => charset_floor,
            decode::Note::InvalidUtf8 => utf8_floor,
            decode::Note::UnlistedChildren { .. } => children_floor,
        };
        if len > COMPACT_RESULT_CEILING {
            return Err(over());
        }
        distinct.push(one);
    }

    // Distinct notes say one sentence only where quoting made their names
    // alike: a name cut, or a control character in it made a space.
    let quoted_alike = distinct.iter().any(|one| match one {
        decode::Note::UnknownCharset(name)
        | decode::Note::UnlistedChildren {
            mime_type: name, ..
        } => name.len() > HEADER_MAX_BYTES || plain_changes(name.as_bytes()),
        decode::Note::InvalidUtf8 => false,
    });
    let mut sentences: Vec<String> = distinct.into_iter().map(|one| note(one, false)).collect();
    let kept: Vec<usize> = if quoted_alike {
        let mut seen = Distinct::new(
            &sentences,
            |sentence| sip(sentence.as_bytes()),
            |a, b| bytes::same(a.as_bytes(), b.as_bytes()),
        );
        (0..sentences.len()).filter(|&at| seen.first(at)).collect()
    } else {
        (0..sentences.len()).collect()
    };
    let mut answered = Vec::with_capacity(kept.len() + 1);
    let mut len = 0;
    for at in kept {
        let sentence = std::mem::take(&mut sentences[at]);
        len += sentence.len();
        if len > COMPACT_RESULT_CEILING {
            return Err(over());
        }
        answered.push(sentence);
    }
    // TODO(gmail-body-part-by-reference-unread): fetch a text part Gmail
    // carries by reference and read it into the forms.
    match by_reference {
        0 => {}
        1 => answered.push(
            "1 text part came as an attachment reference and is in no form; GmailRead lists \
             it under attachments, and one listed with a `part_id` is fetched with \
             GmailAttachment"
                .to_string(),
        ),
        n => answered.push(format!(
            "{n} text parts came as attachment references and are in no form; GmailRead lists \
             them under attachments, and one listed with a `part_id` is fetched with \
             GmailAttachment"
        )),
    }
    Ok(answered)
}

/// Which items of a list are the first of their kind, asked in order.
///
/// Items are told apart by a 64-bit hash and compared only with an item
/// that hashes alike, so the work is each item hashed once, a search of
/// integers, and a comparison with its earlier equal; an item is never
/// searched against other items' text. The items here are notes and
/// sentences whose text is the sender's, which it can make long and alike;
/// the hash is SipHash, so making many items hash alike is out of its
/// reach.
struct Distinct<'a, T> {
    items: &'a [T],
    hash: fn(&T) -> u64,
    same: fn(&T, &T) -> bool,
    /// Each hash seen, with the first item that has it.
    firsts: BTreeMap<u64, usize>,
    /// Each later distinct item whose hash an earlier one has, by hash.
    collided: BTreeSet<(u64, usize)>,
}

impl<'a, T> Distinct<'a, T> {
    fn new(items: &'a [T], hash: fn(&T) -> u64, same: fn(&T, &T) -> bool) -> Self {
        Distinct {
            items,
            hash,
            same,
            firsts: BTreeMap::new(),
            collided: BTreeSet::new(),
        }
    }

    /// Whether `items[at]` is the first of its kind, every item before it
    /// having been asked.
    fn first(&mut self, at: usize) -> bool {
        let item = &self.items[at];
        let key = (self.hash)(item);
        let earlier = *self.firsts.entry(key).or_insert(at);
        if earlier == at {
            return true;
        }
        if (self.same)(&self.items[earlier], item)
            || self
                .collided
                .range((key, 0)..=(key, usize::MAX))
                .any(|&(_, seen)| (self.same)(&self.items[seen], item))
        {
            return false;
        }
        self.collided.insert((key, at));
        true
    }
}

/// SipHash of `bytes`, in one write.
fn sip(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

/// Whether two notes are the same note.
fn same_note(a: &decode::Note<'_>, b: &decode::Note<'_>) -> bool {
    use decode::Note::{InvalidUtf8, UnknownCharset, UnlistedChildren};
    match (a, b) {
        (UnknownCharset(a), UnknownCharset(b)) => bytes::same(a.as_bytes(), b.as_bytes()),
        (InvalidUtf8, InvalidUtf8) => true,
        (
            UnlistedChildren {
                mime_type: a,
                depth: a_depth,
            },
            UnlistedChildren {
                mime_type: b,
                depth: b_depth,
            },
        ) => a_depth == b_depth && bytes::same(a.as_bytes(), b.as_bytes()),
        _ => false,
    }
}

/// A note's hash for [`Distinct`]: its quoted string's, with its kind and
/// depth mixed in.
fn note_hash(note: &decode::Note<'_>) -> u64 {
    // An odd multiplier keeps distinct depths distinct.
    const MIX: u64 = 0x9e37_79b9_7f4a_7c15;
    match note {
        decode::Note::UnknownCharset(name) => sip(name.as_bytes()),
        decode::Note::InvalidUtf8 => 0,
        decode::Note::UnlistedChildren { mime_type, depth } => {
            sip(mime_type.as_bytes()) ^ (*depth as u64 + 1).wrapping_mul(MIX)
        }
    }
}

/// `form` of `bodies`: its answer and the blob holding it, or its answer
/// alone when the form is absent.
fn body_form(
    step: &Step<'_>,
    bodies: &decode::Bodies,
    form: Form,
    max_write: usize,
) -> Result<(Value, Option<Blob>), Failure> {
    let leaves = |view: &[usize]| {
        view.iter()
            .map(|&leaf| &bodies.leaves[leaf])
            .collect::<Vec<_>>()
    };
    let (view, absent) = match form {
        Form::Text => (&bodies.text, "no text part"),
        Form::Html | Form::Rendered => (&bodies.html, "no HTML part"),
    };
    if view.is_empty() {
        return Ok((json!({ "absent": absent }), None));
    }
    let leaves = leaves(view);
    let (segments, separator, through_plain, content_type, mut value) = match form {
        Form::Text => {
            let source = |source| leaves.iter().all(|leaf| leaf.source == source);
            let source = if source(decode::Source::Plain) {
                "text/plain"
            } else if source(decode::Source::Html) {
                "text/html"
            } else {
                "mixed"
            };
            let mut segments = Vec::with_capacity(leaves.len());
            for leaf in &leaves {
                segments.push(kept(leaf.text())?);
            }
            (
                segments,
                "\n\n",
                true,
                TEXT_PLAIN_UTF8,
                json!({ "source": source }),
            )
        }
        Form::Html => {
            let mut segments = Vec::with_capacity(leaves.len());
            for leaf in &leaves {
                segments.push(kept(leaf.decoded.as_deref())?);
            }
            (
                segments,
                "\n",
                false,
                TEXT_HTML_UTF8,
                json!({ "parts": leaves.len() }),
            )
        }
        Form::Rendered => {
            let mut segments = Vec::with_capacity(leaves.len());
            for leaf in &leaves {
                segments.push(kept(leaf.rendering.as_deref())?);
            }
            (
                segments,
                "\n\n",
                true,
                TEXT_PLAIN_UTF8,
                json!({ "parts": leaves.len() }),
            )
        }
    };
    let segments: Vec<&str> = if through_plain {
        segments
            .into_iter()
            .filter(|text| !text.is_empty())
            .collect()
    } else {
        segments
    };
    let size = segments.iter().map(|text| text.len()).sum::<usize>()
        + separator.len() * segments.len().saturating_sub(1);
    if size == 0 {
        return Ok((json!({ "absent": "empty" }), None));
    }
    let blob = write_form(
        step,
        content_type,
        &segments,
        separator,
        through_plain,
        max_write,
    )?;
    value["attachment"] = json!(form.name());
    value["size"] = json!(blob.size());
    Ok((value, Some(blob)))
}

/// A leaf's text a form reads, which the walk keeps for every form asked for.
fn kept(text: Option<&str>) -> Result<&str, Failure> {
    text.ok_or_else(|| internal("a part a form reads was not kept by the walk"))
}

/// A blob of `content_type` holding `segments` joined by `separator`, through
/// [`plain`] when `through_plain`, written in slices of at most `max_write`
/// bytes so no write holds a whole segment.
fn write_form(
    step: &Step<'_>,
    content_type: &str,
    segments: &[&str],
    separator: &str,
    through_plain: bool,
    max_write: usize,
) -> Result<Blob, Failure> {
    let mut blob = step.append(content_type)?;
    let mut gathered = Vec::new();
    for (at, segment) in segments.iter().enumerate() {
        if at > 0 {
            // `plain` leaves a separator as it is.
            gather(
                &mut blob,
                &mut gathered,
                separator.as_bytes(),
                false,
                max_write,
            )?;
        }
        gather(
            &mut blob,
            &mut gathered,
            segment.as_bytes(),
            through_plain,
            max_write,
        )?;
    }
    if !gathered.is_empty() {
        blob.write(&gathered)?;
    }
    blob.finish()
}

/// The length under which [`gather`] gathers a piece with others.
const GATHERED_UNDER: usize = 4096;

/// `bytes`, through [`plain`] when `through_plain`, written to `blob` after
/// what `gathered` holds. A piece shorter than [`GATHERED_UNDER`] is gathered
/// with others into one write of at most `max_write` bytes, since each write
/// costs more than a short piece's bytes; a longer one is written in slices
/// of at most `max_write`.
fn gather(
    blob: &mut Append<'_>,
    gathered: &mut Vec<u8>,
    bytes: &[u8],
    through_plain: bool,
    max_write: usize,
) -> Result<(), Failure> {
    if bytes.len() < GATHERED_UNDER && gathered.len() + bytes.len() <= max_write {
        if through_plain && plain_changes(bytes) {
            gathered.extend(bytes.iter().copied().map(plain_byte));
        } else {
            gathered.extend_from_slice(bytes);
        }
        return Ok(());
    }
    if !gathered.is_empty() {
        blob.write(gathered)?;
        gathered.clear();
    }
    // TODO(blob-append-write-copies-whole-argument): pass each segment whole
    // once the kit's `Append::write` stops copying its argument.
    for slice in bytes.chunks(max_write) {
        if through_plain && plain_changes(slice) {
            gathered.extend(slice.iter().copied().map(plain_byte));
            blob.write(gathered)?;
            gathered.clear();
        } else {
            blob.write(slice)?;
        }
    }
    Ok(())
}

/// `bodies` with every leaf's text that none of the forms `later` reads
/// dropped. `text` is written first, so no later form reads a plain leaf.
fn release(bodies: &mut decode::Bodies, later: &[Form]) {
    let (html, rendered) = (later.contains(&Form::Html), later.contains(&Form::Rendered));
    let mut in_html = vec![false; bodies.leaves.len()];
    for &leaf in &bodies.html {
        in_html[leaf] = true;
    }
    for (leaf, in_html) in bodies.leaves.iter_mut().zip(in_html) {
        if !(html && in_html && leaf.source == decode::Source::Html) {
            leaf.decoded = None;
        }
        if !(rendered && in_html) {
            leaf.rendering = None;
        }
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
    let mut notes = notes(&extracted.notes, false);
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
    let notes = notes(&extracted.notes, true);
    out.insert("text".to_string(), Value::Null);
    out.insert(
        "text_omitted".to_string(),
        json!(format!(
            "{}; headers and attachments are listed, the body is not; {TO_GMAIL_BODY}",
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
fn extract<'a>(body: &'a Value, api: &str) -> Result<decode::Extracted<'a>, Failure> {
    let payload = body.get("payload").ok_or_else(|| shape(api))?;
    decode::Part::from_value(payload)
        .and_then(|payload| decode::extract(&payload))
        .map_err(|error| walk_failure(error, api))
}

/// Why a message's tree could not be read, as Google's fault.
fn walk_failure(error: decode::WalkError, api: &str) -> Failure {
    match error {
        decode::WalkError::Shape => shape(api),
        decode::WalkError::Base64(part) => internal(format!(
            "Gmail {api} part {} carries body data that is not base64url",
            bounded_quote(&part)
        )),
    }
}

/// The decoding notes as answered. `structure` says the tree came through
/// the structure mask, whose depth is why a part's children go unlisted.
fn notes(notes: &[decode::Note<'_>], structure: bool) -> Vec<String> {
    notes.iter().map(|one| note(one, structure)).collect()
}

/// One note as [`notes`] answers it.
fn note(note: &decode::Note<'_>, structure: bool) -> String {
    // Pieces joined, not formatted: a message can make thousands of notes.
    let joined = |pieces: &[&str]| {
        let mut sentence = String::with_capacity(pieces.iter().map(|piece| piece.len()).sum());
        for piece in pieces {
            sentence.push_str(piece);
        }
        sentence
    };
    match note {
        decode::Note::UnknownCharset(name) => joined(&[
            "charset \"",
            &bounded_quote(name),
            "\" is not supported; decoded as UTF-8, non-ASCII characters may be wrong",
        ]),
        decode::Note::InvalidUtf8 => {
            "a text part is not valid UTF-8; its invalid bytes are shown as \u{fffd}".to_string()
        }
        decode::Note::UnlistedChildren { mime_type, depth } => joined(&[
            "a ",
            &bounded_quote(mime_type),
            " part at depth ",
            &depth.to_string(),
            if structure {
                " has children the structure mask does not list; attachments below it are \
                 not listed"
            } else {
                " has no children in Google's answer; nothing below it is shown"
            },
        ]),
    }
}

/// The attachments as answered: `filename` and `mime_type` through `plain`
/// and cut to `HEADER_MAX_BYTES`, `size` when Google gave it, `inline`, and
/// `part_id` when the part has one the attachment fetch takes.
fn attachments(extracted: &decode::Extracted<'_>) -> Value {
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
                if let Some(part_id) = &attachment.part_id {
                    entry.insert("part_id".to_string(), json!(part_id));
                }
                Value::Object(entry)
            })
            .collect(),
    )
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

#[cfg(test)]
mod tests {
    use brenn_google_workspace::args::{ACCOUNT_MAX_CHARS, MAX_CHARS_MAX, MAX_CHARS_MIN};
    use brenn_google_workspace::gmail::ID_MAX_CHARS;
    use brenn_google_workspace::testutil::{
        admits_characters, assert_read_failures_are_bare, base64url, completed, cut_read, follow,
        lookup, no_body_parts,
    };
    use brenn_google_workspace::text::PAGE_MAX_BYTES;
    use brenn_provider_kit::testutil::{
        Answered, Completion, MemoryHost, binary, blob_answer, failure, google, http, nested,
        store_full, too_large, truncated,
    };
    use brenn_provider_kit::{
        Bound, Delivered, Input, Probes, Request, ToolBounds, WriteError, check, hold_probes,
        hold_required, input_schema, probes,
    };
    use serde_json::{Map, json};

    use super::*;

    const BASE: &[(&str, &str)] = &[("provider", "google"), ("gmail_host", "gmail.example.com")];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE), "gmail_host").unwrap()
    }

    /// `advance` under the test settings, on a step whose answer carries no
    /// attachment.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        let host = MemoryHost::new();
        let mut step = Step::answering(&host, "r000", "c000", Vec::new(), Vec::new());
        advance(state, outcome, &mut step, &settings())
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
                response_attachment: None,
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
            google::MESSAGE_MAX_BYTES + "…".len()
        );
    }

    #[test]
    fn advance_answers_a_google_error_through_the_status_mapping() {
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
            Next::Fail(
                Unreadable::Cut("of unstated length".to_string()).failure(google::API_NAME, None)
            )
        );
    }

    #[test]
    fn advance_answers_a_binary_listing_internal() {
        assert_eq!(
            answered(&labels_state(json!({})), binary(200, &[0, 1, 2])),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google returned a non-text body (200)"
            ))
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
            1_000,
        )
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
                response_attachment: None,
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
        assert_eq!(
            answered(&state, binary(404, &[0, 1, 2])),
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google answered HTTP 404 with a non-text body"
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
            1_000,
        )
    }

    /// `GmailRead { id: "m1" }` answered with `body` at its first step.
    fn read_m1(body: &str) -> Next {
        answered(&read_state(json!({ "id": "m1" })), http(200, body))
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
                          "size": 48213, "inline": false, "part_id": "1" },
                        { "filename": "logo.png", "mime_type": "image/png",
                          "size": 1024, "inline": true, "part_id": "2" },
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
        let data = base64url(text.as_bytes());
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
                "body": { "data": base64url(text.as_bytes()) },
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
                "id,threadId,labelIds,snippet,internalDate,sizeEstimate,payload(partId,\
                 mimeType,filename,headers,body(size,attachmentId),parts(partId,mimeType,\
                 filename,headers,body(size,attachmentId),parts(partId,mimeType,filename,\
                 headers,body(size,attachmentId),parts(partId,mimeType,filename,headers,\
                 body(size,attachmentId)))))"
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
                                     the body is not; GmailBody answers the body as files",
                    "attachments": [{ "filename": "Q3 report.pdf",
                                      "mime_type": "application/pdf", "size": 48213,
                                      "inline": false, "part_id": "1" }],
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
            Next::Fail(failure(
                ErrorKind::Internal,
                "Google's answer (900000 bytes) exceeds api-request's read limit for this \
                 provider (max_response_bytes); GmailBody answers the body as files"
            ))
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
            QueryValue::One(structure_fields())
        );
        let answer = completed(answered(&row, http(200, STRUCTURE)));
        assert_eq!(answer["text"], Value::Null);
        assert_eq!(
            answer["text_omitted"],
            json!(
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes; sizeEstimate 4096; headers and attachments are listed, the \
                 body is not; GmailBody answers the body as files"
            )
        );
        // The narrower read refused too is the request's failure, naming the
        // tool that reads the body.
        assert_eq!(
            answered(&row, nested("internal", refusal)),
            Next::Fail(failure(
                ErrorKind::Internal,
                "the host did not deliver api-request's answer: result exceeds [messaging] \
                 max_body_bytes; GmailBody answers the body as files"
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
                Next::Fail(
                    Unreadable::Cut("200000 bytes".to_string()).failure(google::API_NAME, None)
                )
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

    /// A `GmailAttachment` row at its first step, as `plan` issues it.
    fn attachment_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailAttachment, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::GmailAttachment,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    /// The attachment `messages.attachments.get` answers for `bytes`.
    fn attachments_get(bytes: &[u8]) -> Vec<u8> {
        json!({ "attachmentId": "ANGjdJ8", "size": bytes.len(), "data": base64url(bytes) })
            .to_string()
            .into_bytes()
    }

    /// A part of the tree the part-tree mask lists.
    fn part(id: &str, mime_type: &str, filename: &str, body: Value) -> Value {
        json!({ "partId": id, "mimeType": mime_type, "filename": filename, "body": body })
    }

    /// The PDF attachment at part `1`, of `size` bytes.
    fn pdf(size: u64) -> Value {
        part(
            "1",
            "application/pdf",
            "report.pdf",
            json!({ "size": size, "attachmentId": "ANGjdJ8" }),
        )
    }

    /// A message whose payload holds a text part `0` and `attachment`.
    fn part_tree(attachment: Value) -> String {
        let mut payload = part("", "multipart/mixed", "", json!({ "size": 0 }));
        payload["parts"] = json!([
            part("0", "text/plain", "", json!({ "size": 5 })),
            attachment
        ]);
        json!({ "payload": payload }).to_string()
    }

    /// A message whose attachment, part `id`, sits `depth` levels of
    /// `multipart/mixed` below the payload.
    fn nested_part_tree(depth: usize, id: &str) -> String {
        let mut node = part(
            id,
            "application/pdf",
            "deep.pdf",
            json!({ "size": 3, "attachmentId": "ANGjdJ8" }),
        );
        for level in (0..depth).rev() {
            let mut parent = part(
                &format!("x{level}"),
                "multipart/mixed",
                "",
                json!({ "size": 0 }),
            );
            parent["parts"] = json!([node]);
            node = parent;
        }
        json!({ "payload": node }).to_string()
    }

    /// The part-tree read of `GmailAttachment { id: "m1", part_id }`
    /// answered with `body`.
    fn located(part_id: &str, body: &str) -> Next {
        answered(
            &attachment_state(json!({ "id": "m1", "part_id": part_id })),
            http(200, body),
        )
    }

    /// The row of the fetch step `located` issues.
    fn fetch_state(part_id: &str, body: &str) -> InFlight {
        let state = attachment_state(json!({ "id": "m1", "part_id": part_id }));
        follow(&state, answered(&state, http(200, body))).0
    }

    #[test]
    fn attachment_reads_the_part_tree_first() {
        let call = plan(
            Provided::GmailAttachment,
            &json!({ "id": "m1", "part_id": "1.2", "account": "alice@example.com" }),
            &settings(),
        )
        .unwrap();
        assert_eq!(call.step, 1);
        assert_eq!(call.api.method, "GET");
        assert_eq!(call.api.path, "/gmail/v1/users/me/messages/m1");
        assert_eq!(
            call.api.query,
            query([
                ("format", "full".to_string()),
                ("fields", part_tree_fields()),
                ("prettyPrint", "false".to_string()),
            ])
        );
        assert_eq!(call.api.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.api.response_attachment, None);
    }

    #[test]
    fn a_located_part_is_fetched_as_an_attachment() {
        let Next::Call { call, derived } = located("1", &part_tree(pdf(3))) else {
            panic!("the part was not fetched");
        };
        assert_eq!(call.step, FETCH_STEP);
        assert_eq!(call.api.method, "GET");
        assert_eq!(
            call.api.path,
            "/gmail/v1/users/me/messages/m1/attachments/ANGjdJ8"
        );
        assert_eq!(call.api.query, fields_free_query());
        assert_eq!(call.api.body, None);
        assert_eq!(call.api.response_attachment.as_deref(), Some("body"));
        assert_eq!(
            Value::Object(derived),
            json!({ "filename": "report.pdf", "mime_type": "application/pdf", "size": 3 })
        );
    }

    /// The query of a call that names no `fields` mask.
    fn fields_free_query() -> Query {
        query([("prettyPrint", "false".to_string())])
    }

    #[test]
    fn a_part_is_found_within_eight_levels_and_not_below() {
        let Next::Call { call, .. } =
            located("deep", &nested_part_tree(ATTACHMENT_PART_DEPTH, "deep"))
        else {
            panic!("a part at depth 8 was not fetched");
        };
        assert_eq!(call.step, FETCH_STEP);
        assert_eq!(
            located("deep", &nested_part_tree(ATTACHMENT_PART_DEPTH + 1, "deep")),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "message `m1` has no part `deep` within 8 levels of its payload, the depth \
                 GmailAttachment reads; use a `part_id` GmailRead lists"
            ))
        );
    }

    #[test]
    fn a_part_gmail_does_not_serve_is_the_callers() {
        assert_eq!(
            located("7", &part_tree(pdf(3))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "message `m1` has no part `7` within 8 levels of its payload, the depth \
                 GmailAttachment reads; use a `part_id` GmailRead lists"
            ))
        );
        assert_eq!(
            located("0", &part_tree(pdf(3))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "part `0` is carried inside the message, not as an attachment Gmail serves; \
                 read it with GmailRead, or whole with GmailBody when it is body text"
            ))
        );
        assert_eq!(
            located("1", &part_tree(pdf(MAX_ATTACHMENT_BYTES + 1))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "attachment of 50000001 bytes exceeds the 50000000 bytes GmailAttachment returns"
            ))
        );
        let Next::Call { derived, .. } = located("1", &part_tree(pdf(MAX_ATTACHMENT_BYTES))) else {
            panic!("the largest attachment was not fetched");
        };
        assert_eq!(derived["size"], json!(MAX_ATTACHMENT_BYTES));
    }

    #[test]
    fn a_part_tree_not_of_gmails_shape_is_internal() {
        let shape = Next::Fail(failure(
            ErrorKind::Internal,
            "unexpected Google messages.get response shape",
        ));
        let with = |edit: &dyn Fn(&mut Value)| {
            let mut attachment = pdf(3);
            edit(&mut attachment);
            located("1", &part_tree(attachment))
        };
        for (what, next) in [
            ("no payload", located("1", "{}")),
            ("a part not an object", located("1", &part_tree(json!("1")))),
            (
                "parts not a list",
                located("1", &json!({ "payload": { "parts": {} } }).to_string()),
            ),
            ("a partId not a string", with(&|p| p["partId"] = json!(1))),
            ("a body not an object", with(&|p| p["body"] = json!([]))),
            (
                "no size",
                with(&|p| p["body"] = json!({ "attachmentId": "A" })),
            ),
            ("a negative size", with(&|p| p["body"]["size"] = json!(-1))),
            (
                "an id not a string",
                with(&|p| p["body"]["attachmentId"] = json!(1)),
            ),
            (
                "an id with `/`",
                with(&|p| p["body"]["attachmentId"] = json!("a/b")),
            ),
            (
                "an id over 4096",
                with(&|p| p["body"]["attachmentId"] = json!("a".repeat(4097))),
            ),
            (
                "a filename not a string",
                with(&|p| p["filename"] = json!(1)),
            ),
            (
                "a mimeType not a string",
                with(&|p| p["mimeType"] = json!(1)),
            ),
        ] {
            assert_eq!(next, shape, "{what}");
        }
        let Next::Call { .. } = with(&|p| p["body"]["attachmentId"] = json!("a".repeat(4096)))
        else {
            panic!("a 4096-character id was refused");
        };
    }

    #[test]
    fn a_part_carries_its_filename_and_type_bounded() {
        let long = "é".repeat(600);
        let mut attachment = pdf(3);
        attachment["filename"] = json!(long);
        attachment.as_object_mut().unwrap().remove("mimeType");
        let Next::Call { derived, .. } = located("1", &part_tree(attachment)) else {
            panic!("the part was not fetched");
        };
        assert_eq!(derived["filename"], json!(bounded_quote(&long)));
        assert!(derived["filename"].as_str().unwrap().ends_with('…'));
        assert_eq!(derived["mime_type"], json!(""));
    }

    /// One `GmailAttachment` request run through the kit's loop on `host`:
    /// the part-tree read answered with `tree`, the fetch answered with
    /// `encoded` as its attachment. The request's completion.
    fn fetch_through_the_loop(host: &MemoryHost, tree: &str, encoded: Vec<u8>) -> Completion {
        let id = "r000".to_string();
        let request = Request {
            id: id.clone(),
            tool: Provided::GmailAttachment.name().to_string(),
            args: json!({ "id": "m1", "part_id": "1" }),
            attachments: Vec::new(),
        };
        let deliver = |now_ms, results| Input {
            now_ms,
            requests: Vec::new(),
            results,
        };
        host.activate::<GmailReads>(
            &settings(),
            Input {
                now_ms: 0,
                requests: vec![request],
                results: Vec::new(),
            },
        )
        .unwrap();
        let read = host.take_calls().remove(0);
        host.activate::<GmailReads>(
            &settings(),
            deliver(
                1,
                vec![Delivered {
                    call_id: read.call_id,
                    serving: Some(id.clone()),
                    outcome: http(200, tree),
                    attachments: Vec::new(),
                }],
            ),
        )
        .unwrap();
        let fetch = host.take_calls().remove(0);
        assert_eq!(fetch.call.response_attachment.as_deref(), Some("body"));
        let size = encoded.len() as u64;
        let answer = host.seed_result(&fetch.call_id, "body", "application/json", encoded);
        host.activate::<GmailReads>(
            &settings(),
            deliver(
                2,
                vec![Delivered {
                    call_id: fetch.call_id,
                    serving: Some(id),
                    outcome: blob_answer("body", size),
                    attachments: vec![answer],
                }],
            ),
        )
        .unwrap();
        let mut completions = host.take_completions();
        assert_eq!(completions.len(), 1);
        completions.remove(0)
    }

    /// A host reading and writing 1 024 bytes at a time.
    fn small_pieces() -> MemoryHost {
        let host = MemoryHost::new();
        host.set_blob_limits(Some(brenn_provider_kit::BlobLimits {
            max_read_bytes: 1024,
            max_write_bytes: 1024,
        }));
        host
    }

    /// `n` bytes of every value, in order.
    fn file(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 7 % 256) as u8).collect()
    }

    #[test]
    fn a_fetched_attachment_is_decoded_into_the_completion() {
        let bytes = file(5_000);
        let host = small_pieces();
        let completion = fetch_through_the_loop(
            &host,
            &part_tree(pdf(bytes.len() as u64)),
            attachments_get(&bytes),
        );
        assert_eq!(
            completion.answer,
            Answered::Value(json!({
                "id": "m1",
                "part_id": "1",
                "filename": "report.pdf",
                "mime_type": "application/pdf",
                "size": 5_000,
                "attachment": "attachment",
            }))
        );
        assert_eq!(completion.attachments.len(), 1);
        let blob = &completion.attachments[0];
        assert_eq!(blob.name, "attachment");
        assert_eq!(blob.content_type, "application/pdf");
        assert_eq!(blob.bytes, bytes);
    }

    #[test]
    fn an_empty_attachment_is_an_empty_blob() {
        let completion =
            fetch_through_the_loop(&MemoryHost::new(), &part_tree(pdf(0)), attachments_get(&[]));
        assert_eq!(completion.attachments.len(), 1);
        assert!(completion.attachments[0].bytes.is_empty());
    }

    #[test]
    fn a_type_the_host_refuses_is_octet_stream() {
        let bytes = file(10);
        let host = MemoryHost::new();
        host.refuse_next_create(CreateError::InvalidContentType("bad".to_string()));
        let mut attachment = pdf(10);
        attachment["mimeType"] = json!("not a type");
        let completion =
            fetch_through_the_loop(&host, &part_tree(attachment), attachments_get(&bytes));
        let Answered::Value(value) = &completion.answer else {
            panic!("{completion:?}");
        };
        assert_eq!(value["mime_type"], json!("not a type"));
        assert_eq!(completion.attachments[0].content_type, OCTET_STREAM);
        assert_eq!(completion.attachments[0].bytes, bytes);
        // A part with no type is octet-stream without asking.
        let mut untyped = pdf(10);
        untyped.as_object_mut().unwrap().remove("mimeType");
        let completion = fetch_through_the_loop(
            &MemoryHost::new(),
            &part_tree(untyped),
            attachments_get(&bytes),
        );
        assert_eq!(completion.attachments[0].content_type, OCTET_STREAM);
    }

    /// The error a completion answered.
    fn error(completion: &Completion) -> (ErrorKind, &str) {
        assert!(completion.attachments.is_empty(), "{completion:?}");
        match &completion.answer {
            Answered::Error(kind, detail) => (*kind, detail.as_str()),
            Answered::Value(value) => panic!("completed with {value}"),
        }
    }

    #[test]
    fn a_wrong_or_undecodable_answer_completes_nothing() {
        let bytes = file(3_000);
        let tree = part_tree(pdf(3_000));
        let stated = json!({ "size": 2_999, "data": base64url(&bytes) });
        let cases = [
            (
                "a stated size that is not the data's",
                stated.to_string().into_bytes(),
                "Gmail messages.attachments.get states size 2999 and its data decodes to 3000 \
                 bytes",
            ),
            (
                "data shorter than the part",
                attachments_get(&bytes[..2_000]),
                "Gmail listed part `1` at 3000 bytes and its data decodes to 2000",
            ),
            (
                "data longer than the part",
                attachments_get(&file(4_000)),
                "Gmail listed part `1` at 3000 bytes and its data decodes to 4000",
            ),
            (
                "data not base64url",
                br#"{"size": 3000, "data": "ab*d"}"#.to_vec(),
                "Gmail messages.attachments.get carries `data` that is not base64url",
            ),
            (
                "an object cut short",
                attachments_get(&bytes)[..100].to_vec(),
                "unexpected Google messages.attachments.get response shape",
            ),
        ];
        for (what, encoded, detail) in cases {
            let completion = fetch_through_the_loop(&MemoryHost::new(), &tree, encoded);
            assert_eq!(error(&completion), (ErrorKind::Internal, detail), "{what}");
        }
    }

    #[test]
    fn a_write_the_host_refuses_fails_naming_the_bound() {
        let bytes = file(3_000);
        let host = small_pieces();
        host.set_write_budget(2_048);
        let completion =
            fetch_through_the_loop(&host, &part_tree(pdf(3_000)), attachments_get(&bytes));
        assert_eq!(
            error(&completion),
            (
                ErrorKind::Internal,
                "blob write: the activation's bound on blob calls or on bytes written is reached"
            )
        );
        let host = small_pieces();
        host.refuse_next_write(WriteError::BudgetExhausted);
        let completion =
            fetch_through_the_loop(&host, &part_tree(pdf(3_000)), attachments_get(&bytes));
        assert_eq!(
            error(&completion),
            (
                ErrorKind::Internal,
                "blob write: the host's blob store is full (blob_budget_bytes)"
            )
        );
    }

    #[test]
    fn the_fetchs_refusals_and_errors_are_mapped() {
        let state = fetch_state("1", &part_tree(pdf(3)));
        assert_eq!(state.step, FETCH_STEP);
        assert_eq!(
            answered(&state, too_large(67_108_864, Some(70_000_000))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "the attachment's encoded form exceeds the host's largest blob (max_blob_bytes, \
                 67108864 bytes)"
            ))
        );
        assert_eq!(
            answered(&state, store_full()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "the host's blob store is full (blob_budget_bytes)"
            ))
        );
        let not_found = google(404, "NOT_FOUND", "Requested entity was not found.");
        assert_eq!(
            answered(&state, http(404, &not_found)),
            Next::Fail(google_error(404, &not_found, Scope::GmailRead))
        );
        let relayed = credential_invalid(json!("alice@example.com"));
        assert_eq!(
            answered(&state, Ok(relayed.clone())),
            Next::Complete(relayed)
        );
        assert_eq!(
            answered(&state, cut_read(json!(300000))),
            Next::Fail(Unreadable::Cut("300000 bytes".to_string()).failure(google::API_NAME, None))
        );
        let Next::Fail(Failure { kind, detail }) = answered(&state, http(200, "{}")) else {
            panic!("an inline answer was accepted");
        };
        assert_eq!(kind, ErrorKind::Internal);
        assert!(detail.contains("inline"), "{detail}");
    }

    /// A `GmailBody` row at its one step, as `plan` issues it.
    fn body_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailBody, &args, &settings()).unwrap();
        InFlight::new(
            &"b".repeat(32),
            Provided::GmailBody,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    /// A text leaf of `mime_type` holding `bytes`, its charset declared when
    /// `charset` names one.
    fn text_leaf(mime_type: &str, charset: Option<&str>, bytes: &[u8]) -> Value {
        let mut leaf = json!({
            "partId": "", "mimeType": mime_type, "filename": "",
            "body": { "size": bytes.len(), "data": base64url(bytes) },
        });
        if let Some(charset) = charset {
            leaf["headers"] = json!([
                { "name": "Content-Type", "value": format!("{mime_type}; charset={charset}") }
            ]);
        }
        leaf
    }

    fn plain_leaf(text: &str) -> Value {
        text_leaf("text/plain", Some("UTF-8"), text.as_bytes())
    }

    fn html_leaf(html: &str) -> Value {
        text_leaf("text/html", Some("UTF-8"), html.as_bytes())
    }

    /// A `multipart/*` part of `subtype` holding `parts`.
    fn multipart(subtype: &str, parts: Vec<Value>) -> Value {
        json!({
            "partId": "", "mimeType": format!("multipart/{subtype}"), "filename": "",
            "body": { "size": 0 }, "parts": parts,
        })
    }

    /// messages.get's answer for message `m1` with `payload`.
    fn gmail_message(payload: Value) -> Vec<u8> {
        json!({ "id": "m1", "threadId": "t1", "snippet": "Lunch?", "payload": payload })
            .to_string()
            .into_bytes()
    }

    /// One `GmailBody` request with `args` run through the kit's loop on
    /// `host`, its read answered with `encoded` as its attachment. The
    /// request's completion.
    fn body_through_the_loop(host: &MemoryHost, args: Value, encoded: Vec<u8>) -> Completion {
        let id = "r000".to_string();
        let request = Request {
            id: id.clone(),
            tool: Provided::GmailBody.name().to_string(),
            args,
            attachments: Vec::new(),
        };
        host.activate::<GmailReads>(
            &settings(),
            Input {
                now_ms: 0,
                requests: vec![request],
                results: Vec::new(),
            },
        )
        .unwrap();
        let read = host.take_calls().remove(0);
        assert_eq!(read.call.response_attachment.as_deref(), Some("body"));
        let size = encoded.len() as u64;
        let answer = host.seed_result(&read.call_id, "body", "application/json", encoded);
        host.activate::<GmailReads>(
            &settings(),
            Input {
                now_ms: 1,
                requests: Vec::new(),
                results: vec![Delivered {
                    call_id: read.call_id,
                    serving: Some(id),
                    outcome: blob_answer("body", size),
                    attachments: vec![answer],
                }],
            },
        )
        .unwrap();
        let mut completions = host.take_completions();
        assert_eq!(completions.len(), 1);
        completions.remove(0)
    }

    /// `GmailBody` of message `m1` in `forms`, answered with `payload`: the
    /// answer and each blob's name, content type and text.
    fn body_forms_of(forms: Value, payload: Value) -> (Value, Vec<(String, String, String)>) {
        let completion = body_through_the_loop(
            &MemoryHost::new(),
            json!({ "id": "m1", "forms": forms }),
            gmail_message(payload),
        );
        let Answered::Value(value) = completion.answer else {
            panic!("{completion:?}");
        };
        let blobs = completion
            .attachments
            .into_iter()
            .map(|blob| {
                (
                    blob.name,
                    blob.content_type,
                    String::from_utf8(blob.bytes).unwrap(),
                )
            })
            .collect();
        (value, blobs)
    }

    const ALL_FORMS: [&str; 3] = ["text", "html", "rendered"];

    #[test]
    fn body_plans_one_full_read_answered_as_an_attachment() {
        let call = plan(
            Provided::GmailBody,
            &json!({ "id": "m1", "forms": ["html"], "account": "alice@example.com" }),
            &settings(),
        )
        .unwrap();
        assert_eq!(call.step, 1);
        assert_eq!(call.api.method, "GET");
        assert_eq!(call.api.path, "/gmail/v1/users/me/messages/m1");
        assert_eq!(
            call.api.query,
            query([
                ("format", "full".to_string()),
                ("prettyPrint", "false".to_string()),
            ])
        );
        assert_eq!(call.api.body, None);
        assert_eq!(call.api.account.as_deref(), Some("alice@example.com"));
        assert_eq!(call.api.response_attachment.as_deref(), Some("body"));
    }

    #[test]
    fn body_refuses_bad_arguments() {
        let forms = "`forms` must be an array of 1 to 3 form names";
        let form = "each of `forms` must be one of `text`, `html` and `rendered`";
        for (args, error) in [
            (json!({ "forms": ["text"] }), "`id` is required"),
            (
                json!({ "id": "m/1", "forms": ["text"] }),
                "`id` must be 1 to 128 letters, digits, `_` or `-`",
            ),
            (json!({ "id": "m1" }), "`forms` is required"),
            (json!({ "id": "m1", "forms": [] }), forms),
            (
                json!({ "id": "m1", "forms": ["text", "html", "rendered", "text"] }),
                forms,
            ),
            (json!({ "id": "m1", "forms": "text" }), forms),
            (json!({ "id": "m1", "forms": ["Text"] }), form),
            (json!({ "id": "m1", "forms": ["txt"] }), form),
            (json!({ "id": "m1", "forms": [1] }), form),
            (
                json!({ "id": "m1", "forms": ["html", "html"] }),
                "`forms` names \"html\" twice",
            ),
            (
                json!({ "id": "m1", "forms": ["text"], "max_chars": 10 }),
                "unknown argument `max_chars`; this tool takes `id`, `forms`, `account`",
            ),
            (
                json!({ "id": "m1", "forms": ["text"], "account": "" }),
                "`account` is empty",
            ),
            (
                json!({ "id": "m1", "forms": ["text"], "account": 1 }),
                "`account` must be a string",
            ),
        ] {
            assert_eq!(
                plan(Provided::GmailBody, &args, &settings()).unwrap_err(),
                error,
                "{args}"
            );
        }
    }

    #[test]
    fn body_forms_are_taken_in_any_order_and_kept_in_form_order() {
        for (names, expected) in [
            (
                json!(["rendered", "text"]),
                vec![Form::Text, Form::Rendered],
            ),
            (json!(["rendered", "html", "text"]), Form::ALL.to_vec()),
            (json!(["html"]), vec![Form::Html]),
        ] {
            let args = BodyArgs::parse(&json!({ "id": "m1", "forms": names })).unwrap();
            assert_eq!(args.forms, expected, "{names}");
        }
    }

    #[test]
    fn an_alternative_answers_its_plain_text_its_html_and_the_html_rendered() {
        let payload = multipart(
            "alternative",
            vec![
                plain_leaf("Lunch at noon?"),
                html_leaf("<p>Lunch at <b>noon</b>?</p>"),
            ],
        );
        let (value, blobs) = body_forms_of(json!(["rendered", "text", "html"]), payload);
        let rendered = decode::html_to_text("<p>Lunch at <b>noon</b>?</p>");
        assert_eq!(
            value,
            json!({
                "id": "m1",
                "text": { "attachment": "text", "size": 14, "source": "text/plain" },
                "html": { "attachment": "html", "size": 28, "parts": 1 },
                "rendered": { "attachment": "rendered", "size": rendered.len(), "parts": 1 },
            })
        );
        let utf8_plain = "text/plain; charset=utf-8".to_string();
        assert_eq!(
            blobs,
            vec![
                (
                    "text".to_string(),
                    utf8_plain.clone(),
                    "Lunch at noon?".to_string()
                ),
                (
                    "html".to_string(),
                    "text/html; charset=utf-8".to_string(),
                    "<p>Lunch at <b>noon</b>?</p>".to_string()
                ),
                ("rendered".to_string(), utf8_plain, rendered),
            ]
        );
    }

    #[test]
    fn only_the_forms_asked_for_are_answered_and_attached() {
        let payload = multipart(
            "alternative",
            vec![plain_leaf("Lunch?"), html_leaf("<p>Lunch?</p>")],
        );
        for forms in [
            vec!["text"],
            vec!["html"],
            vec!["rendered"],
            vec!["html", "text"],
            vec!["rendered", "html"],
        ] {
            let (value, blobs) = body_forms_of(json!(forms), payload.clone());
            let mut expected: Vec<&str> = ALL_FORMS
                .into_iter()
                .filter(|form| forms.contains(form))
                .collect();
            let names: Vec<&str> = blobs.iter().map(|(name, ..)| name.as_str()).collect();
            assert_eq!(names, expected, "{forms:?}");
            expected.push("id");
            expected.sort_unstable();
            let keys: Vec<&str> = value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(keys, expected, "{forms:?}");
        }
    }

    /// `GmailRead`'s `text` of message `m1` answered with `payload`, under a
    /// `max_chars` that does not cut it.
    fn read_text(payload: &Value) -> String {
        let body = String::from_utf8(gmail_message(payload.clone())).unwrap();
        let answer = completed(answered(
            &read_state(json!({ "id": "m1", "max_chars": MAX_CHARS_MAX })),
            http(200, &body),
        ));
        assert_eq!(answer.get("text_truncated"), None);
        answer["text"].as_str().unwrap().to_string()
    }

    #[test]
    fn the_text_form_is_gmail_reads_text_uncut() {
        let controls = "tab\there\u{1}bell\u{7}\r\nend";
        for payload in [
            plain_leaf("Lunch at noon?"),
            html_leaf("<div>Lunch &amp; <i>talk</i></div><p>at noon</p>"),
            multipart(
                "alternative",
                vec![plain_leaf(" \n "), html_leaf("<p>Hi</p>")],
            ),
            multipart(
                "mixed",
                vec![
                    plain_leaf(controls),
                    plain_leaf(""),
                    html_leaf("<p>Second\u{2}</p>"),
                    text_leaf("text/plain", Some("windows-1252"), b"caf\xe9 \x93q\x94"),
                ],
            ),
            multipart(
                "alternative",
                vec![
                    plain_leaf("é".repeat(40_000).as_str()),
                    html_leaf("<p>long</p>"),
                ],
            ),
        ] {
            let (value, blobs) = body_forms_of(json!(["text"]), payload.clone());
            let text = read_text(&payload);
            assert_eq!(blobs.len(), 1, "{value}");
            assert_eq!(blobs[0].2, text);
            assert_eq!(value["text"]["size"], json!(text.len()));
        }
        let (value, _) = body_forms_of(
            json!(["text"]),
            multipart("mixed", vec![plain_leaf("a"), html_leaf("<p>b</p>")]),
        );
        assert_eq!(value["text"]["source"], json!("mixed"));
        let (value, blobs) = body_forms_of(json!(ALL_FORMS), html_leaf("<p>Only</p>"));
        assert_eq!(value["text"]["source"], json!("text/html"));
        assert_eq!(blobs[0].2, blobs[2].2, "an HTML-only text is its rendering");
    }

    #[test]
    fn html_is_decoded_and_unchanged_and_several_parts_join_by_a_newline() {
        let payload = multipart(
            "mixed",
            vec![
                text_leaf("text/html", Some("windows-1252"), b"<p>caf\xe9\x01</p>"),
                plain_leaf("between"),
                html_leaf("<p>\u{2}second</p>"),
            ],
        );
        let (value, blobs) = body_forms_of(json!(["html", "rendered"]), payload);
        assert_eq!(value["html"]["parts"], json!(2));
        assert_eq!(value["rendered"]["parts"], json!(2));
        assert_eq!(blobs[0].2, "<p>café\u{1}</p>\n<p>\u{2}second</p>");
        let rendered = [
            decode::html_to_text("<p>café\u{1}</p>"),
            decode::html_to_text("<p>\u{2}second</p>"),
        ];
        assert_eq!(
            blobs[1].2,
            plain(&rendered.join("\n\n")),
            "rendered through plain"
        );
        assert!(!blobs[1].2.contains(['\u{1}', '\u{2}']));
    }

    #[test]
    fn a_form_the_message_lacks_is_absent_saying_why() {
        let (value, blobs) = body_forms_of(json!(ALL_FORMS), plain_leaf("Lunch?"));
        assert_eq!(value["html"], json!({ "absent": "no HTML part" }));
        assert_eq!(value["rendered"], json!({ "absent": "no HTML part" }));
        assert_eq!(blobs.len(), 1);

        let pdf = json!({
            "partId": "1", "mimeType": "application/pdf", "filename": "a.pdf",
            "body": { "size": 3, "attachmentId": "ANGjdJ8" },
        });
        let (value, blobs) = body_forms_of(json!(ALL_FORMS), multipart("mixed", vec![pdf]));
        assert_eq!(
            value,
            json!({
                "id": "m1",
                "text": { "absent": "no text part" },
                "html": { "absent": "no HTML part" },
                "rendered": { "absent": "no HTML part" },
            })
        );
        assert!(blobs.is_empty());

        let (value, blobs) = body_forms_of(json!(ALL_FORMS), html_leaf(""));
        for form in ALL_FORMS {
            assert_eq!(value[form], json!({ "absent": "empty" }), "{form}");
        }
        assert!(blobs.is_empty());
        let (value, _) =
            body_forms_of(json!(["text", "rendered"]), html_leaf("<style>p{}</style>"));
        assert_eq!(value["text"], json!({ "absent": "empty" }));
        assert_eq!(value["rendered"], json!({ "absent": "empty" }));
    }

    #[test]
    fn decoding_notes_are_said_once_and_count_parts_by_reference() {
        let by_reference = |mime_type: &str| {
            json!({
                "partId": "2", "mimeType": mime_type, "filename": "",
                "body": { "size": 9, "attachmentId": "ANGjdJ8" },
            })
        };
        let payload = multipart(
            "mixed",
            vec![
                text_leaf("text/plain", Some("koi8-r"), b"one"),
                text_leaf("text/html", Some("koi8-r"), b"<p>two</p>"),
                text_leaf("text/plain", Some("UTF-8"), b"\xff"),
                by_reference("text/plain"),
                by_reference("text/html"),
            ],
        );
        let (value, _) = body_forms_of(json!(ALL_FORMS), payload);
        assert_eq!(
            value["decoding_notes"],
            json!([
                "charset \"koi8-r\" is not supported; decoded as UTF-8, non-ASCII characters may \
                 be wrong",
                "a text part is not valid UTF-8; its invalid bytes are shown as \u{fffd}",
                "2 text parts came as attachment references and are in no form; GmailRead lists \
                 them under attachments, and one listed with a `part_id` is fetched with \
                 GmailAttachment",
            ])
        );
        let (value, _) = body_forms_of(
            json!(["html"]),
            multipart("mixed", vec![plain_leaf("one"), by_reference("text/plain")]),
        );
        assert_eq!(
            value["decoding_notes"],
            json!([
                "1 text part came as an attachment reference and is in no form; GmailRead lists \
                 it under attachments, and one listed with a `part_id` is fetched with \
                 GmailAttachment"
            ])
        );
        // A note about a part no form asked for reads is not said.
        let (value, _) = body_forms_of(
            json!(["html"]),
            multipart(
                "mixed",
                vec![
                    text_leaf("text/plain", Some("koi8-r"), b"one"),
                    html_leaf("<p>two</p>"),
                ],
            ),
        );
        assert_eq!(value.get("decoding_notes"), None);
    }

    /// messages.get's answer for a plain message `m1`, padded to exactly
    /// `size` bytes.
    fn answer_of(size: u64) -> Vec<u8> {
        let base = gmail_message(plain_leaf("Lunch?")).len();
        let mut answer: Value =
            serde_json::from_slice(&gmail_message(plain_leaf("Lunch?"))).unwrap();
        answer["snippet"] = json!("x".repeat(size as usize - base + "Lunch?".len()));
        let answer = answer.to_string().into_bytes();
        assert_eq!(answer.len() as u64, size);
        answer
    }

    #[test]
    fn an_answer_at_the_bound_is_read_and_one_over_is_refused_unread() {
        let completion = body_through_the_loop(
            &MemoryHost::new(),
            json!({ "id": "m1", "forms": ["text"] }),
            answer_of(BODY_ANSWER_MAX),
        );
        assert_eq!(completion.attachments.len(), 1, "{completion:?}");
        assert_eq!(completion.attachments[0].bytes, b"Lunch?");
        // The open is the step's one blob call: a read would be refused.
        let host = MemoryHost::new();
        host.set_blob_call_budget(1);
        let completion = body_through_the_loop(
            &host,
            json!({ "id": "m1", "forms": ["text"] }),
            answer_of(BODY_ANSWER_MAX + 1),
        );
        assert_eq!(
            error(&completion),
            (
                ErrorKind::InvalidArgs,
                format!(
                    "the message's answer from Gmail is {} bytes, over the {BODY_ANSWER_MAX} \
                     bytes GmailBody reads; GmailRead answers its text cut, and a caller \
                     granted api-request reads the whole message with `format=raw` and \
                     `response_attachment` (docs/providers/google.md)",
                    BODY_ANSWER_MAX + 1
                )
                .as_str()
            )
        );
    }

    #[test]
    fn the_reads_refusals_and_errors_are_mapped() {
        let state = body_state(json!({ "id": "m1", "forms": ["text"] }));
        assert_eq!(
            answered(&state, too_large(67_108_864, Some(70_000_000))),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "the message's answer from Gmail exceeds the host's largest blob \
                 (max_blob_bytes, 67108864 bytes); GmailRead answers its text cut"
            ))
        );
        assert_eq!(
            answered(&state, store_full()),
            Next::Fail(failure(
                ErrorKind::Internal,
                "the host's blob store is full (blob_budget_bytes)"
            ))
        );
        let not_found = google(404, "NOT_FOUND", "Requested entity was not found.");
        assert_eq!(
            answered(&state, http(404, &not_found)),
            Next::Fail(google_error(404, &not_found, Scope::GmailRead))
        );
        let relayed = credential_invalid(json!("alice@example.com"));
        assert_eq!(
            answered(&state, Ok(relayed.clone())),
            Next::Complete(relayed)
        );
        assert_eq!(
            answered(&state, cut_read(json!(300000))),
            Next::Fail(Unreadable::Cut("300000 bytes".to_string()).failure(google::API_NAME, None))
        );
        let Next::Fail(Failure { kind, detail }) = answered(&state, http(200, "{}")) else {
            panic!("an inline answer was accepted");
        };
        assert_eq!(kind, ErrorKind::Internal);
        assert!(detail.contains("inline"), "{detail}");
    }

    #[test]
    fn an_answer_not_of_gmails_shape_completes_nothing() {
        let shape = "unexpected Google messages.get response shape";
        let mut other = serde_json::from_slice::<Value>(&gmail_message(plain_leaf("a"))).unwrap();
        other["id"] = json!("m2");
        let mut bad_data = plain_leaf("a");
        bad_data["partId"] = json!("1.2");
        bad_data["body"]["data"] = json!("ab*d");
        for (what, encoded, detail) in [
            ("not JSON", b"<html>".to_vec(), shape),
            (
                "cut short",
                gmail_message(plain_leaf("a"))[..20].to_vec(),
                shape,
            ),
            ("another message", other.to_string().into_bytes(), shape),
            (
                "a part not of Gmail's shape",
                gmail_message(json!({ "mimeType": 7 })),
                shape,
            ),
            (
                "data not base64url",
                gmail_message(bad_data),
                "Gmail messages.get part 1.2 carries body data that is not base64url",
            ),
        ] {
            let completion = body_through_the_loop(
                &MemoryHost::new(),
                json!({ "id": "m1", "forms": ALL_FORMS }),
                encoded,
            );
            assert_eq!(error(&completion), (ErrorKind::Internal, detail), "{what}");
        }
    }

    #[test]
    fn notes_over_the_result_ceiling_alone_complete_nothing() {
        let leaves = |count: usize| {
            (0..count)
                .map(|at| text_leaf("text/plain", Some(&format!("x-{at}")), b"a"))
                .collect()
        };
        let sentence = |at: usize| {
            format!(
                "charset \"x-{at}\" is not supported; decoded as UTF-8, non-ASCII characters \
                 may be wrong"
            )
        };
        // The most distinct sentences whose bytes fit the ceiling.
        let (mut fit, mut len) = (0, 0);
        while len + sentence(fit).len() <= COMPACT_RESULT_CEILING {
            len += sentence(fit).len();
            fit += 1;
        }
        // Nine tenths of them are answered, quotes and commas included.
        let most = fit * 9 / 10;
        let (value, _) = body_forms_of(json!(["text"]), multipart("mixed", leaves(most)));
        assert_eq!(value["decoding_notes"].as_array().unwrap().len(), most);
        // One more is over by the sentences made; as many as the ceiling
        // holds sentences with nothing quoted, and one, are over before any
        // is made.
        let floor = note(&decode::Note::UnknownCharset("".into()), false).len();
        for count in [fit + 1, COMPACT_RESULT_CEILING / floor + 1] {
            let completion = body_through_the_loop(
                &MemoryHost::new(),
                json!({ "id": "m1", "forms": ["text"] }),
                gmail_message(multipart("mixed", leaves(count))),
            );
            assert_eq!(
                error(&completion),
                (
                    ErrorKind::Internal,
                    "the message's decoding notes alone are over the 262144-byte result ceiling"
                ),
                "{count} distinct notes"
            );
            assert!(completion.attachments.is_empty());
        }
        // Said once each, repeats cost nothing toward the ceiling.
        let mut repeated = leaves(64);
        repeated.extend(leaves(64));
        let (value, _) = body_forms_of(json!(["text"]), multipart("mixed", repeated));
        let notes = value["decoding_notes"].as_array().unwrap();
        assert_eq!(notes.len(), 64);
        assert_eq!(notes[63], json!(sentence(63)));
    }

    #[test]
    fn the_first_of_each_distinct_item_is_told_in_order() {
        let items = ["b", "a", "b", "c", "a", "b"];
        let mut seen = Distinct::new(&items, |item| sip(item.as_bytes()), |a, b| a == b);
        let firsts: Vec<bool> = (0..items.len()).map(|at| seen.first(at)).collect();
        assert_eq!(firsts, [true, true, false, true, false, false]);

        // Items that all hash alike are still told apart.
        let items = [1, 2, 1, 3, 2];
        let mut seen = Distinct::new(&items, |_| 7, |a, b| a == b);
        let firsts: Vec<bool> = (0..items.len()).map(|at| seen.first(at)).collect();
        assert_eq!(firsts, [true, true, false, true, false]);
    }

    #[test]
    fn notes_are_the_same_exactly_when_equal() {
        let notes = [
            decode::Note::InvalidUtf8,
            decode::Note::UnknownCharset("x".into()),
            decode::Note::UnknownCharset("y".into()),
            decode::Note::UnlistedChildren {
                mime_type: "x".into(),
                depth: 1,
            },
            decode::Note::UnlistedChildren {
                mime_type: "x".into(),
                depth: 2,
            },
            decode::Note::UnlistedChildren {
                mime_type: "y".into(),
                depth: 1,
            },
        ];
        for a in &notes {
            for b in &notes {
                assert_eq!(same_note(a, b), a == b, "{a:?} {b:?}");
            }
        }
    }

    #[test]
    fn charsets_quoted_alike_are_one_sentence() {
        // Names alike in every byte the sentence quotes: cut alike, or
        // alike once a control character is a space.
        let long = "x".repeat(HEADER_MAX_BYTES);
        let quoted_alike = [
            [format!("{long}1"), format!("{long}2")],
            ["x\u{1}".to_string(), "x\u{2}".to_string()],
        ];
        for names in quoted_alike {
            let leaves = names
                .iter()
                .map(|name| text_leaf("text/plain", Some(name), b"a"))
                .collect();
            let (value, _) = body_forms_of(json!(["text"]), multipart("mixed", leaves));
            assert_eq!(value["decoding_notes"].as_array().unwrap().len(), 1);
        }
    }

    /// An alternative of 3 000 bytes of plain text and of HTML.
    fn wide_alternative() -> Vec<u8> {
        gmail_message(multipart(
            "alternative",
            vec![
                plain_leaf(&"p".repeat(3_000)),
                html_leaf(&format!("<p>{}</p>", "h".repeat(2_993))),
            ],
        ))
    }

    #[test]
    fn a_refused_create_or_write_fails_with_nothing_attached() {
        let args = || json!({ "id": "m1", "forms": ALL_FORMS });
        let host = small_pieces();
        host.refuse_next_create(CreateError::BudgetExhausted);
        assert_eq!(
            error(&body_through_the_loop(&host, args(), wide_alternative())),
            (
                ErrorKind::Internal,
                "blob create: the host's blob store is full (blob_budget_bytes)"
            )
        );
        // The text form is written whole; the HTML form passes the budget.
        let host = small_pieces();
        host.set_write_budget(4_096);
        assert_eq!(
            error(&body_through_the_loop(&host, args(), wide_alternative())),
            (
                ErrorKind::Internal,
                "blob write: the activation's bound on blob calls or on bytes written is reached"
            )
        );
        let host = small_pieces();
        host.refuse_next_write(WriteError::BudgetExhausted);
        assert_eq!(
            error(&body_through_the_loop(&host, args(), wide_alternative())),
            (
                ErrorKind::Internal,
                "blob write: the host's blob store is full (blob_budget_bytes)"
            )
        );
    }

    #[test]
    fn small_read_and_write_pieces_give_the_same_forms() {
        let args = json!({ "id": "m1", "forms": ALL_FORMS });
        let whole = body_through_the_loop(&MemoryHost::new(), args.clone(), wide_alternative());
        let pieces = body_through_the_loop(&small_pieces(), args, wide_alternative());
        assert_eq!(whole.attachments.len(), 3);
        assert_eq!(whole, pieces);
        assert_eq!(whole.attachments[0].bytes, "p".repeat(3_000).as_bytes());
    }

    /// Every step `tool` issues a call at.
    fn steps(tool: Provided) -> Vec<u8> {
        match tool {
            Provided::GmailLabels => vec![1],
            Provided::GmailSearch => [1, LABELS_STEP]
                .into_iter()
                .chain(SEARCH_FIRST_GET..=SEARCH_LAST_GET)
                .collect(),
            Provided::GmailRead => vec![1, STRUCTURE_STEP, LABELS_STEP],
            Provided::GmailThread => vec![1, THREAD_NARROW_STEP, LABELS_STEP],
            // The fetch step's answer is an attachment, not JSON; its tests
            // are its own.
            Provided::GmailAttachment => vec![1],
            // Its one step's answer is an attachment, not JSON.
            Provided::GmailBody => vec![],
        }
    }

    /// A row of each tool at its first step, as `plan` issues it.
    fn first_reads() -> [InFlight; 6] {
        let reads = [
            labels_state(json!({})),
            search_state(json!({ "q": "lunch" })),
            read_state(json!({ "id": "m1" })),
            thread_state(json!({ "id": "t1" })),
            attachment_state(json!({ "id": "m1", "part_id": "1" })),
            body_state(json!({ "id": "m1", "forms": ["text"] })),
        ];
        for tool in Provided::ALL {
            assert!(
                reads.iter().any(|state| state.tool == tool),
                "{} has no state here",
                tool.name()
            );
        }
        reads
    }

    #[test]
    fn every_step_names_its_scope() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                assert_eq!(
                    scope(tool, step),
                    Scope::GmailRead,
                    "{} step {step}",
                    tool.name()
                );
            }
        }
        assert_eq!(
            scope(Provided::GmailAttachment, FETCH_STEP),
            Scope::GmailRead
        );
        assert_eq!(scope(Provided::GmailBody, 1), Scope::GmailRead);
    }

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(search, json!({}), "q", QUERY_MAX_CHARS);
        admits_characters(search, json!({ "q": "x" }), "page", PAGE_MAX_CHARS);
        admits_characters(labels, json!({}), "account", ACCOUNT_MAX_CHARS);
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. Every bound here is one the kit's probes hold.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::GmailLabels => (LabelsArgs::KEYS, &[]),
            Provided::GmailSearch => (SearchArgs::KEYS, &["q"]),
            Provided::GmailRead => (ReadArgs::KEYS, &["id"]),
            Provided::GmailThread => (ThreadArgs::KEYS, &["id"]),
            Provided::GmailAttachment => (AttachmentArgs::KEYS, &["id", "part_id"]),
            Provided::GmailBody => (BodyArgs::KEYS, &["id", "forms"]),
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
                (_, "max_chars") => integer(MAX_CHARS_MIN, MAX_CHARS_MAX),
                (_, "spam_trash") => Bound::Bool,
                (_, "id") => Bound::Token {
                    max_chars: ID_MAX_CHARS,
                },
                (Provided::GmailSearch, "max") => integer(SEARCH_MAX_MIN, SEARCH_MAX_MAX),
                (Provided::GmailThread, "max") => integer(THREAD_MAX_MIN, THREAD_MAX_MAX),
                (Provided::GmailAttachment, "part_id") => {
                    Bound::Pattern(format!("^[A-Za-z0-9.]{{1,{PART_ID_MAX_CHARS}}}$"))
                }
                (Provided::GmailBody, "forms") => Bound::Array {
                    min: 1,
                    max: Form::ALL.len(),
                    unique: true,
                    item: Box::new(Bound::Enum(Form::ALL.map(Form::name).to_vec())),
                },
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
            Provided::GmailLabels => json!({}),
            Provided::GmailSearch => json!({ "q": "from:alice" }),
            Provided::GmailRead | Provided::GmailThread => json!({ "id": "m1" }),
            Provided::GmailAttachment => json!({ "id": "m1", "part_id": "1" }),
            Provided::GmailBody => json!({ "id": "m1", "forms": ["text"] }),
        }
    }

    /// The probes of an argument whose bound is a pattern of this component's
    /// own, or an array, which the kit has none for.
    fn pattern_probes(key: &str) -> Probes {
        let strings = |ids: &[&str]| ids.iter().map(|id| json!(id)).collect::<Vec<_>>();
        match key {
            "part_id" => Probes {
                admitted: strings(&["1", "0", "1.2.10", "Ab.9", &"1.".repeat(32)]),
                refused: strings(&[
                    "",
                    &"1".repeat(PART_ID_MAX_CHARS + 1),
                    "1-2",
                    "1_2",
                    "1/2",
                    "1 2",
                    "é",
                ])
                .into_iter()
                .chain([json!(1)])
                .collect(),
            },
            "forms" => Probes {
                admitted: vec![
                    json!(["text"]),
                    json!(["rendered"]),
                    json!(["rendered", "html", "text"]),
                ],
                refused: vec![
                    json!([]),
                    json!(["text", "html", "rendered", "text"]),
                    json!(["text", "text"]),
                    json!(["raw"]),
                    json!(["Text"]),
                    json!([1]),
                    json!("text"),
                ],
            },
            other => panic!("no pattern probes for `{other}`"),
        }
    }

    /// The component's specification, as the package ships it.
    fn spec_text() -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-gmail-read.brenn");
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()))
    }

    /// Each tool's `input_schema` states exactly the bounds its parser
    /// enforces, and the parser admits and refuses every bound's probes,
    /// naming the argument it refuses: the schema and the parser are held to
    /// the same table from both sides.
    #[test]
    fn every_tools_schema_states_what_its_parser_enforces() {
        let spec = spec_text();
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
                let probes = probes(bound).unwrap_or_else(|| pattern_probes(key));
                hold_probes(name, key, &probes, &base, plan_with);
            }
        }
    }

    /// `GmailBody`'s description states the bound the tool refuses above,
    /// so the model is told the figure the tool enforces.
    #[test]
    fn gmail_bodys_description_states_its_bound() {
        let spec = spec_text();
        let (_, block) = spec
            .split_once("provide gmail-body {")
            .expect("the spec has a `provide gmail-body` block");
        let description = block
            .lines()
            .map(str::trim)
            .take_while(|line| *line != "}")
            .find_map(|line| line.strip_prefix("description = "))
            .expect("`provide gmail-body` has a description");
        let stated = format!("over {BODY_ANSWER_MAX} bytes is refused");
        assert!(
            description.contains(&stated),
            "`gmail-body`'s description must say \"{stated}\""
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
            at(thread_state(json!({ "id": "t1" })), THREAD_NARROW_STEP),
            at(search_state(json!({ "q": "lunch" })), LABELS_STEP),
            at(read_state(json!({ "id": "m1" })), LABELS_STEP),
            at(thread_state(json!({ "id": "t1" })), LABELS_STEP),
        ] {
            for (outcome, why) in outcomes() {
                assert_eq!(
                    answered(&state, outcome),
                    Next::Fail(why.failure(google::API_NAME, None)),
                    "{}",
                    name(&state)
                );
            }
        }

        // A message's narrower read: the rendering and the route to its body.
        let state = at(read_state(json!({ "id": "m1" })), STRUCTURE_STEP);
        for (outcome, why) in outcomes() {
            assert_eq!(
                answered(&state, outcome),
                Next::Fail(why.failure(google::API_NAME, Some(TO_GMAIL_BODY))),
                "{}",
                name(&state)
            );
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
                let entry =
                    json!([{ "id": ids[index], "reason": why.render(google::API_NAME, None) }]);
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
    fn an_empty_answer_is_internal_at_every_step() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                let mut state = labels_state(json!({}));
                state.tool = tool;
                state.step = step;
                for status in [200, 204] {
                    let Next::Fail(Failure { kind, detail }) = answered(&state, http(status, ""))
                    else {
                        panic!("{} step {step} accepted no body", tool.name());
                    };
                    assert_eq!(kind, ErrorKind::Internal);
                    assert_eq!(
                        no_body_parts(&detail, status).map(|(_, answers)| answers),
                        Some("JSON"),
                        "{detail}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_step_names_one_api_for_no_body_and_for_a_bad_shape() {
        // Every step of every tool, both of `GmailLabels`' calls, and each
        // search per-message read on a row that lists its ids.
        let ids: Vec<String> = (1..=25).map(|i| format!("m{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let search = search_state(json!({ "q": "lunch", "max": 25 }));
        let (listed, _) = follow(
            &search,
            answered(&search, http(200, &listing(&id_refs, json!({})))),
        );
        let bases = [
            labels_state(json!({})),
            labels_state(json!({ "id": "INBOX" })),
            listed,
            read_state(json!({ "id": "m1" })),
            thread_state(json!({ "id": "t1" })),
            attachment_state(json!({ "id": "m1", "part_id": "1" })),
            body_state(json!({ "id": "m1", "forms": ["text"] })),
        ];
        for tool in Provided::ALL {
            assert!(
                bases.iter().any(|state| state.tool == tool),
                "{} has no state here",
                tool.name()
            );
        }
        let detail = |next: Next| match next {
            Next::Fail(Failure { detail, .. }) => detail,
            other => panic!("{other:?}"),
        };
        for base in &bases {
            for step in steps(base.tool) {
                let mut state = base.clone();
                state.step = step;
                // The label-names step reads after an answer it carries, and
                // a narrower re-read after the reason it was asked for.
                state
                    .derived
                    .entry("answer".to_string())
                    .or_insert_with(|| json!({}));
                for (key, value) in Unreadable::Cut("300000 bytes".to_string()).to_derived() {
                    state.derived.entry(key).or_insert(value);
                }
                let name = format!("{} step {step}", state.tool.name());
                let empty = detail(answered(&state, http(204, "")));
                let api = no_body_parts(&empty, 204)
                    .map(|(api, _)| api)
                    .unwrap_or_else(|| panic!("{name}: {empty}"));
                let bad = detail(answered(&state, http(200, "42")));
                assert!(
                    bad.starts_with(&format!("unexpected Google {api} response shape")),
                    "{name}: {bad}"
                );
            }
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

    #[test]
    fn every_reads_failure_is_bare_and_names_the_read_scope() {
        // No step here writes, so every step's failure is the bare one: a
        // failure Google may have acted under carries no hint. A search's
        // per-message reads are left out: their 404 lists the message
        // unavailable (`a_message_deleted_mid_search_is_listed_unavailable`).
        let mut states = Vec::new();
        for state in first_reads() {
            for step in steps(state.tool) {
                if state.tool == Provided::GmailSearch
                    && (SEARCH_FIRST_GET..=SEARCH_LAST_GET).contains(&step)
                {
                    continue;
                }
                states.push(InFlight {
                    step,
                    ..state.clone()
                });
            }
        }
        assert_read_failures_are_bare(&states, Scope::GmailRead, answered);
    }
}
