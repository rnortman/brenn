//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `GmailDraft`'s arguments, building its Gmail calls as `api-request`
//! arguments, and reading `api-request`'s answer. Nothing here names the
//! guest SDK, so the whole module runs on the host under test. The tool is
//! the generated `Provided` enum, whose SDK-facing parts are wasm32-only.

// The host test's crate root is this file, so it declares the generated
// module itself; in the component, `lib.rs` does.
#[cfg(test)]
mod spec;

// The draft builder is this module's child in the component and the host
// test alike: the path resolves beside this file whether it is
// `crate::logic` or the crate root.
#[path = "compose.rs"]
mod compose;

use brenn_google_workspace::args::{account, is_printable_ascii};
use brenn_google_workspace::call::{get_call, json_call};
#[cfg(test)]
use brenn_google_workspace::gmail::decode;
use brenn_google_workspace::gmail::{
    HEADER_MAX_BYTES, MESSAGES_PATH, gmail_id, is_gmail_id, metadata_query, raw_header,
};
use brenn_google_workspace::scope::{Scope, google_error, no_body_refused, with_write_hint};
use brenn_google_workspace::settings::Settings;
use brenn_google_workspace::text::shape;
use brenn_provider_kit::{
    Answer, ApiCall, Args, COMPACT_RESULT_CEILING, Call, ErrorKind, Failure, Logic, NestedError,
    Next, Query, Tool, Unreadable, bounded, capped, copy, google, no_such_step, object,
    read_api_result, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The path Gmail creates drafts at. Nothing below it is ever called: a
/// draft is sent only by the user, from Gmail.
const DRAFTS_PATH: &str = "/gmail/v1/users/me/drafts";
/// `GmailDraft`'s read of the message a reply answers.
const DRAFT_ORIGINAL_STEP: u8 = 1;
/// `GmailDraft`'s `POST`: a fresh draft's first step, a reply's second.
const DRAFT_CREATE_STEP: u8 = 2;
/// The fields a reply reads of its original.
const DRAFT_ORIGINAL_FIELDS: &str = "id,threadId,payload/headers";
/// The headers a reply derives its own from.
const DRAFT_ORIGINAL_HEADERS: &[&str] = &["Subject", "Message-ID", "References"];
/// The most bytes of an original's subject a reply derives its own from.
const DERIVED_SUBJECT_MAX_BYTES: usize = HEADER_MAX_BYTES;
/// The most bytes of a header value a draft carries: RFC 5322 holds a line to
/// 998 characters, and the longest header name written before a bounded value
/// is `In-Reply-To: ` (13), so a value this long fits its line unfolded.
const HEADER_VALUE_MAX_BYTES: usize = 980;
/// The most characters a draft's `subject` may hold.
const SUBJECT_MAX_CHARS: usize = 500;
/// The most characters a draft's `body` may hold, as its schema states it.
const BODY_MAX_CHARS: usize = 20_000;
/// The most bytes a draft's body may hold in its CRLF form, a lone line break
/// counting as the two bytes it becomes. The body travels as base64 of that
/// form inside `raw`, itself base64url of the message, inside the nested
/// call's args, which the host caps at 64 KiB: with `to`, `cc` and a reply's
/// `In-Reply-To` and `References` at `HEADER_VALUE_MAX_BYTES`, `subject` at
/// its cap as encoded-words, the longest thread id and `account` at its
/// escaped width, the widest admitted call serializes to about 49 000 bytes.
const BODY_MAX_BYTES: usize = 20_000;
/// What the caller is told when a draft's `POST` may have been acted on and
/// its answer was lost or unreadable.
const DRAFT_UNKNOWN_HINT: &str = "the draft may have been created before its answer was lost: \
                                  look for it with GmailSearch (`q: in:drafts`) before asking \
                                  again, or the user has two";

/// The scope `tool`'s step `step` needs. A step the tool does not have is
/// never asked for: `advance` maps an error only for a step it issued.
fn scope(tool: Provided, step: u8) -> Scope {
    match (tool, step) {
        (Provided::GmailDraft, DRAFT_ORIGINAL_STEP) => Scope::GmailRead,
        (Provided::GmailDraft, _) => Scope::GmailCompose,
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
pub struct GmailCompose;

impl Logic for GmailCompose {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
        plan(tool, args, settings)
    }

    fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
        advance(state, outcome, settings)
    }
}

/// Validate a request's arguments and build its first nested call. An error is
/// the request's `invalid_args` detail, naming the argument.
pub fn plan(tool: Provided, args: &Value, settings: &Settings) -> Result<Call, String> {
    match tool {
        Provided::GmailDraft => {
            let draft = DraftArgs::parse(args)?;
            Ok(match (&draft.reply_to_id, &draft.subject) {
                (Some(id), _) => Call {
                    step: DRAFT_ORIGINAL_STEP,
                    api: draft_original_call(id, draft.account.as_deref(), settings),
                },
                (None, Some(subject)) => Call {
                    step: DRAFT_CREATE_STEP,
                    api: draft_create_call(&draft, subject, None, settings),
                },
                (None, None) => {
                    unreachable!("a draft's parse refuses one without `subject` or `reply_to_id`")
                }
            })
        }
    }
}

/// `GmailDraft`'s arguments, each header value held to what the message
/// builder writes without encoding or stripping.
struct DraftArgs {
    to: String,
    cc: Option<String>,
    /// `None` only for a reply, which derives its subject from the original.
    subject: Option<String>,
    body: compose::Body,
    reply_to_id: Option<String>,
    account: Option<String>,
}

impl DraftArgs {
    /// Every argument the tool takes.
    const KEYS: &[&str] = &["to", "cc", "subject", "body", "reply_to_id", "account"];

    fn parse(args: &Value) -> Result<DraftArgs, String> {
        let map = object(args, Self::KEYS)?;
        let to = header_ascii(map, "to")?.ok_or("`to` is required")?;
        let cc = header_ascii(map, "cc")?;
        let reply_to_id = gmail_id(map, "reply_to_id")?;
        let subject = subject(map)?;
        if subject.is_none() && reply_to_id.is_none() {
            return Err("`subject` is required unless `reply_to_id` is given".to_string());
        }
        Ok(DraftArgs {
            to,
            cc,
            subject,
            body: body(map)?.ok_or("`body` is required")?,
            reply_to_id,
            account: account(map)?,
        })
    }
}

/// What a reply takes from its original, each value held to what the message
/// builder writes as is.
struct Reply {
    /// The original's thread, which the draft is filed in.
    thread_id: String,
    /// The threading headers, when the original's `Message-ID` is one
    /// printable ASCII token of at most `HEADER_VALUE_MAX_BYTES`.
    threading: Option<Threading>,
}

/// A reply's `In-Reply-To` and `References`.
struct Threading {
    /// The original's `Message-ID`.
    in_reply_to: String,
    /// The original's `References` and `Message-ID` joined, or the
    /// `Message-ID` alone when the original's `References` is absent, not
    /// printable ASCII, or too long to join.
    references: String,
}

/// A recipient header argument: 1 to `HEADER_VALUE_MAX_BYTES` printable
/// ASCII characters, so it is written folded and never encoded. A display
/// name in another script would need the address list parsed to encode it.
fn header_ascii(args: &Args, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if is_printable_ascii(value, HEADER_VALUE_MAX_BYTES) => {
            Ok(Some(value.clone()))
        }
        Some(_) => Err(format!(
            "`{key}` must be 1 to {HEADER_VALUE_MAX_BYTES} printable ASCII characters (a \
             display name in another script is left out or written in ASCII)"
        )),
    }
}

/// A draft's `subject`: at most `SUBJECT_MAX_CHARS` characters, no control
/// character (a line break included).
fn subject(args: &Args) -> Result<Option<String>, String> {
    let Some(subject) = capped(args, "subject", SUBJECT_MAX_CHARS)? else {
        return Ok(None);
    };
    if subject.chars().any(char::is_control) {
        return Err("`subject` holds a control character".to_string());
    }
    Ok(Some(subject.to_string()))
}

/// A draft's `body` in its CRLF form: at most `BODY_MAX_CHARS` characters as
/// given and `BODY_MAX_BYTES` bytes as sent; any character.
fn body(args: &Args) -> Result<Option<compose::Body>, String> {
    let Some(body) = capped(args, "body", BODY_MAX_CHARS)? else {
        return Ok(None);
    };
    let body = compose::canonical(body);
    if body.as_str().len() > BODY_MAX_BYTES {
        return Err(format!(
            "`body` is longer than {BODY_MAX_BYTES} bytes (UTF-8, a line break counting as two)"
        ));
    }
    Ok(Some(body))
}

/// messages.get of a reply's original: its thread and the headers the reply
/// derives its own from, no body.
fn draft_original_call(id: &str, account: Option<&str>, settings: &Settings) -> ApiCall {
    get_call(
        settings,
        account,
        format!("{MESSAGES_PATH}/{id}"),
        metadata_query(DRAFT_ORIGINAL_FIELDS, DRAFT_ORIGINAL_HEADERS),
    )
}

/// drafts.create of the message the arguments describe under `subject`, as
/// `raw`, filed in the original's thread for a reply. The `POST` creates a
/// draft and sends nothing.
fn draft_create_call(
    args: &DraftArgs,
    subject: &str,
    reply: Option<&Reply>,
    settings: &Settings,
) -> ApiCall {
    let threading = reply.and_then(|reply| reply.threading.as_ref());
    let message = compose::message(&compose::Message {
        to: &args.to,
        cc: args.cc.as_deref(),
        subject,
        in_reply_to: threading.map(|threading| threading.in_reply_to.as_str()),
        references: threading.map(|threading| threading.references.as_str()),
        body: &args.body,
    });
    let mut message = json!({ "raw": compose::base64url(&message) });
    if let Some(reply) = reply {
        message["threadId"] = json!(reply.thread_id);
    }
    json_call(
        settings,
        "POST",
        args.account.as_deref(),
        DRAFTS_PATH.to_string(),
        Query::new(),
        json!({ "message": message }),
    )
}

/// A reply's original read: the draft's `POST` next, filed in the original's
/// thread with the headers derived from the original's, carrying in
/// `derived` the original's `thread_id`, which the answer is checked
/// against, and the `in_reply_to` and derived `subject` it names. An
/// original whose id is not the one asked for is Google's fault.
fn draft_reply(state: &InFlight, body: &Value, settings: &Settings) -> Result<Next, Failure> {
    let api = "messages.get";
    let args = stored(state, DraftArgs::parse)?;
    let asked = args
        .reply_to_id
        .as_deref()
        .expect("a draft reads an original only for `reply_to_id`");
    if body.get("id").and_then(Value::as_str) != Some(asked) {
        return Err(shape(api));
    }
    let thread_id = match body.get("threadId") {
        Some(Value::String(id)) if is_gmail_id(id) => id.clone(),
        _ => return Err(shape(api)),
    };
    let message_id = raw_header(body, "Message-ID", api)?
        .filter(|id| is_printable_ascii(id, HEADER_VALUE_MAX_BYTES) && !id.contains(' '));
    let original_references = raw_header(body, "References", api)?;
    let original_subject = raw_header(body, "Subject", api)?;
    let threading = message_id.map(|id| Threading {
        in_reply_to: id.to_string(),
        references: match original_references {
            Some(references)
                if is_printable_ascii(references, HEADER_VALUE_MAX_BYTES)
                    && references.len() + 1 + id.len() <= HEADER_VALUE_MAX_BYTES =>
            {
                format!("{references} {id}")
            }
            _ => id.to_string(),
        },
    });
    let mut derived = Args::new();
    derived.insert("thread_id".to_string(), json!(thread_id));
    if let Some(threading) = &threading {
        derived.insert("in_reply_to".to_string(), json!(threading.in_reply_to));
    }
    let subject = match &args.subject {
        Some(subject) => subject.clone(),
        None => {
            let subject = derived_subject(original_subject)?;
            derived.insert("subject".to_string(), json!(subject));
            subject
        }
    };
    let reply = Reply {
        thread_id,
        threading,
    };
    Ok(Next::Call {
        call: Call {
            step: DRAFT_CREATE_STEP,
            api: draft_create_call(&args, &subject, Some(&reply), settings),
        },
        derived,
    })
}

/// A reply's subject from its original's: every control character a space,
/// so no line break reaches the header, then `Re: ` before it unless it
/// already starts with `re:` in any case after leading whitespace. An absent
/// or blank original derives `Re:` alone; one over
/// `DERIVED_SUBJECT_MAX_BYTES` is refused, the caller to give `subject`.
fn derived_subject(original: Option<&str>) -> Result<String, Failure> {
    let original: String = original
        .unwrap_or_default()
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if original.len() > DERIVED_SUBJECT_MAX_BYTES {
        return Err(Failure {
            kind: ErrorKind::InvalidArgs,
            detail: format!(
                "the original's subject is longer than {DERIVED_SUBJECT_MAX_BYTES} bytes; give \
                 `subject`"
            ),
        });
    }
    let start = original.trim_start();
    if start.is_empty() {
        return Ok("Re:".to_string());
    }
    if start
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("re:"))
    {
        return Ok(original);
    }
    Ok(format!("Re: {original}"))
}

/// drafts.create's answer, Google's draft resource, as the draft's ids:
/// `draft_id`, `message_id`, and `thread_id` when Google gave it; for a
/// reply, also the `in_reply_to` and derived `subject` the draft carries,
/// from `derived`, and `in_thread: false` when Google filed the draft in a
/// thread other than the original's. Nothing the caller wrote is echoed.
fn compact_draft(body: &Value, derived: &Args) -> Result<Value, Failure> {
    let api = "drafts.create";
    let gmail_id = |value: Option<&Value>| match value {
        Some(Value::String(id)) if is_gmail_id(id) => Ok(id.clone()),
        _ => Err(shape(api)),
    };
    let draft = gmail_id(body.get("id"))?;
    let Some(Value::Object(message)) = body.get("message") else {
        return Err(shape(api));
    };
    let mut out = Args::new();
    out.insert("draft_id".to_string(), json!(draft));
    out.insert(
        "message_id".to_string(),
        json!(gmail_id(message.get("id"))?),
    );
    if let Some(thread) = message.get("threadId") {
        let thread = gmail_id(Some(thread))?;
        if derived
            .get("thread_id")
            .is_some_and(|original| original != thread.as_str())
        {
            out.insert("in_thread".to_string(), json!(false));
        }
        out.insert("thread_id".to_string(), json!(thread));
    }
    copy(&mut out, "in_reply_to", derived.get("in_reply_to"));
    copy(&mut out, "subject", derived.get("subject"));
    Ok(Value::Object(out))
}

/// What the request does with its nested call's answer: complete, fail, or
/// issue its next step. Every answer completed here is held to the result
/// ceiling; a relayed credential answer is `api-request`'s own and is not.
pub fn advance(state: &InFlight, outcome: Result<Value, NestedError>, settings: &Settings) -> Next {
    let body = match read_api_result(outcome, &state.target, google::API_NAME) {
        Ok(Answer::Body(body)) => body,
        Ok(Answer::NoBody { status }) => return settle(state.tool, no_body(state, status)),
        Ok(Answer::Relay(value)) => return Next::Complete(value),
        Ok(Answer::Unreadable(why)) => return unreadable(state, why),
        Ok(Answer::Error { status, body }) => {
            return Next::Fail(at_write(
                state,
                google_error(status, &body, scope(state.tool, state.step)),
            ));
        }
        Err(failure) => return Next::Fail(at_write(state, failure)),
    };
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::GmailDraft, DRAFT_ORIGINAL_STEP) => draft_reply(state, &body, settings),
            (Provided::GmailDraft, DRAFT_CREATE_STEP) => compact_draft(&body, &state.derived)
                .map(Next::Complete)
                .map_err(|failure| at_write(state, failure)),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// A nested read that was cut or refused, decided by the step's shape: a
/// write step fails with why and its hint; a reply's read of its original
/// fails with why.
fn unreadable(state: &InFlight, why: Unreadable) -> Next {
    if let Some(hint) = write_hint(state.tool, state.step) {
        return settle(state.tool, Err(why.failure(google::API_NAME, Some(hint))));
    }
    settle(
        state.tool,
        match (state.tool, state.step) {
            (Provided::GmailDraft, DRAFT_ORIGINAL_STEP) => Err(why.failure(google::API_NAME, None)),
            (tool, step) => Err(no_such_step(tool, step)),
        },
    )
}

/// The hint a write step's failure carries when Google may have acted: what
/// the caller can look for, or that asking again is safe. `None` for a read.
/// Every write step has its row here and nowhere else.
fn write_hint(tool: Provided, step: u8) -> Option<&'static str> {
    match (tool, step) {
        (Provided::GmailDraft, DRAFT_CREATE_STEP) => Some(DRAFT_UNKNOWN_HINT),
        _ => None,
    }
}

fn at_write(state: &InFlight, failure: Failure) -> Failure {
    with_write_hint(write_hint(state.tool, state.step), failure)
}

/// A 2xx with no body, decided by the step: every step refuses it, naming
/// the API whose answer it expected, as that step's `shape` failure names it,
/// with a write step's hint.
fn no_body(state: &InFlight, status: u64) -> Result<Next, Failure> {
    let (api, answers) = match (state.tool, state.step) {
        (Provided::GmailDraft, DRAFT_CREATE_STEP) => ("drafts.create", "the draft"),
        (Provided::GmailDraft, DRAFT_ORIGINAL_STEP) => ("messages.get", "JSON"),
        (tool, step) => return Err(no_such_step(tool, step)),
    };
    Err(at_write(state, no_body_refused(status, api, answers)))
}

/// A step's decision as the request's next move, a completed answer held to
/// the result ceiling.
fn settle(tool: Provided, decided: Result<Next, Failure>) -> Next {
    match decided {
        Ok(Next::Complete(value)) => {
            let (value, hint) = match tool {
                Provided::GmailDraft => (value, None),
            };
            bounded(value, COMPACT_RESULT_CEILING, hint)
        }
        Ok(next) => next,
        Err(failure) => Next::Fail(failure),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_google_workspace::args::{
        ACCOUNT_MAX_CHARS, HOST_ARGS_CAP_BYTES, printable_ascii_pattern,
    };
    use brenn_google_workspace::gmail::ID_MAX_CHARS;
    use brenn_google_workspace::scope::scope_hint;
    use brenn_google_workspace::testutil::{
        admits_characters, cut_read, description, escaped_widest, follow, four_byte, lookup,
        no_body_parts, wide,
    };
    use brenn_provider_kit::testutil::{failure, google, http, nested};
    use brenn_provider_kit::{
        Bound, Probes, QueryValue, ToolBounds, check, hold_probes, hold_required, input_schema,
        internal, probes, query,
    };
    use serde_json::{Map, json};

    use super::*;

    const BASE: &[(&str, &str)] = &[("provider", "google"), ("gmail_host", "gmail.example.com")];

    fn settings() -> Settings {
        Settings::from_lookup(lookup(BASE), "gmail_host").unwrap()
    }

    /// `advance` on the row, under the test settings.
    fn answered(state: &InFlight, outcome: Result<Value, NestedError>) -> Next {
        advance(state, outcome, &settings())
    }

    /// A draft's arguments: `alice@example.com`, `Lunch`, `Noon?`, with
    /// `extra` set over them.
    fn draft_args(extra: Value) -> Value {
        let mut args = json!({ "to": "alice@example.com", "subject": "Lunch", "body": "Noon?" });
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        args
    }

    fn draft(args: Value) -> Result<ApiCall, String> {
        plan(Provided::GmailDraft, &args, &settings()).map(|call| {
            assert_eq!(call.step, DRAFT_CREATE_STEP);
            call.api
        })
    }

    fn draft_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailDraft, &args, &settings()).unwrap();
        InFlight::new(
            &"a".repeat(32),
            Provided::GmailDraft,
            &args,
            &call,
            Map::new(),
            1_000,
            1_000,
        )
    }

    /// A reply's state at its original's read: `draft_args` without
    /// `subject`, replying to `m0`, with `extra` set over them.
    fn reply_state(extra: Value) -> InFlight {
        let mut args = draft_args(json!({ "reply_to_id": "m0" }));
        args.as_object_mut().unwrap().remove("subject");
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let state = draft_state(args);
        assert_eq!(state.step, DRAFT_ORIGINAL_STEP);
        state
    }

    /// A reply's original as messages.get answers its metadata: thread `t1`,
    /// with `headers` as name and value pairs.
    fn original(headers: &[(&str, &str)]) -> Result<Value, NestedError> {
        let headers: Vec<Value> = headers
            .iter()
            .map(|(name, value)| json!({ "name": name, "value": value }))
            .collect();
        http(
            200,
            &json!({ "id": "m0", "threadId": "t1", "payload": { "headers": headers } }).to_string(),
        )
    }

    /// What a reply posts after reading `headers` of its original: the
    /// draft's call, decoded `raw`, and the `derived` its answer reads.
    fn replied(state: &InFlight, headers: &[(&str, &str)]) -> (ApiCall, String, Args) {
        let Next::Call { call, derived } = answered(state, original(headers)) else {
            panic!("a reply posts after its read");
        };
        assert_eq!(call.step, DRAFT_CREATE_STEP);
        let message = raw_message(&call.api);
        (call.api, message, derived)
    }

    /// A message's header lines before the empty line, unfolded: each
    /// CRLF before a continuation's space removed.
    fn message_headers(message: &str) -> Vec<String> {
        let (head, _) = message.split_once("\r\n\r\n").unwrap();
        let mut lines: Vec<String> = Vec::new();
        for line in head.split("\r\n") {
            if line.starts_with(' ') {
                lines.last_mut().unwrap().push_str(line);
            } else {
                lines.push(line.to_string());
            }
        }
        lines
    }

    /// The value of `message`'s header `name`, unfolded.
    fn message_header(message: &str, name: &str) -> Option<String> {
        message_headers(message)
            .into_iter()
            .find_map(|line| line.strip_prefix(&format!("{name}: ")).map(str::to_string))
    }

    /// The message a draft call carries in `raw`, decoded.
    fn raw_message(call: &ApiCall) -> String {
        let body: Value = serde_json::from_str(call.body.as_deref().unwrap()).unwrap();
        let raw = body["message"]["raw"].as_str().unwrap();
        assert!(
            raw.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "unpadded base64url"
        );
        String::from_utf8(decode::base64url(raw).unwrap()).unwrap()
    }

    /// A message's body, its base64 lines decoded.
    fn message_body(message: &str) -> String {
        let (_, body) = message.split_once("\r\n\r\n").unwrap();
        String::from_utf8(decode::base64url(body).unwrap()).unwrap()
    }

    #[test]
    fn a_draft_posts_one_raw_message_on_the_gmail_host() {
        let call = draft(draft_args(json!({
            "cc": "Bob Example <bob@example.com>",
            "account": "alice@example.com",
        })))
        .unwrap();
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "POST".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/drafts".to_string(),
                query: query([("prettyPrint", "false".to_string())]),
                headers: BTreeMap::new(),
                body: call.body.clone(),
            }
        );
        let body: Value = serde_json::from_str(call.body.as_deref().unwrap()).unwrap();
        assert_eq!(
            body.as_object().unwrap().keys().collect::<Vec<_>>(),
            ["message"]
        );
        assert_eq!(
            body["message"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            ["raw"],
            "a fresh draft names no thread"
        );
        assert_eq!(
            raw_message(&call),
            "To: alice@example.com\r\n\
             Cc: Bob Example <bob@example.com>\r\n\
             Subject: Lunch\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             \r\n\
             Tm9vbj8=\r\n"
        );
    }

    #[test]
    fn a_non_ascii_subject_and_body_are_encoded() {
        let call = draft(draft_args(json!({
            "subject": "Grüße aus Köln",
            "body": "Привет,\nмир\r\n🎵",
        })))
        .unwrap();
        let message = raw_message(&call);
        assert!(message.is_ascii(), "{message}");
        assert!(
            message.contains("\r\nSubject: =?utf-8?B?R3LDvMOfZSBhdXMgS8O2bG4=?=\r\n"),
            "{message}"
        );
        assert_eq!(message_body(&message), "Привет,\r\nмир\r\n🎵");
    }

    #[test]
    fn a_draft_refuses_bad_arguments() {
        let header = |key: &str| {
            format!(
                "`{key}` must be 1 to 980 printable ASCII characters (a display \
                                 name in another script is left out or written in ASCII)"
            )
        };
        let without = |key: &str| {
            let mut args = draft_args(json!({}));
            args.as_object_mut().unwrap().remove(key);
            args
        };
        for (args, refusal) in [
            (without("to"), "`to` is required".to_string()),
            (
                without("subject"),
                "`subject` is required unless `reply_to_id` is given".to_string(),
            ),
            (
                draft_args(json!({ "reply_to_id": "m/1" })),
                "`reply_to_id` must be 1 to 128 letters, digits, `_` or `-`".to_string(),
            ),
            (without("body"), "`body` is required".to_string()),
            (draft_args(json!({ "to": "" })), header("to")),
            (
                draft_args(json!({ "to": "Zoë <zoe@example.com>" })),
                header("to"),
            ),
            (
                draft_args(json!({ "to": "a@example.com\r\nBcc: b@example.com" })),
                header("to"),
            ),
            (draft_args(json!({ "cc": "x".repeat(981) })), header("cc")),
            (
                draft_args(json!({ "cc": ["bob@example.com"] })),
                header("cc"),
            ),
            (
                draft_args(json!({ "subject": "Lunch\r\nBcc: bob@example.com" })),
                "`subject` holds a control character".to_string(),
            ),
            (
                draft_args(json!({ "subject": "Lunch\u{85}" })),
                "`subject` holds a control character".to_string(),
            ),
            (
                draft_args(json!({ "subject": "x".repeat(SUBJECT_MAX_CHARS + 1) })),
                "`subject` is longer than 500 characters".to_string(),
            ),
            (
                draft_args(json!({ "body": " \n" })),
                "`body` is empty".to_string(),
            ),
            (
                draft_args(json!({ "bcc": "bob@example.com" })),
                "unknown argument `bcc`; this tool takes `to`, `cc`, `subject`, `body`, \
                 `reply_to_id`, `account`"
                    .to_string(),
            ),
        ] {
            assert_eq!(draft(args.clone()).unwrap_err(), refusal, "{args}");
        }
        // A body may hold any character.
        let call = draft(draft_args(json!({ "body": "a\u{1}\tb\r" }))).unwrap();
        assert_eq!(message_body(&raw_message(&call)), "a\u{1}\tb\r\n");
    }

    #[test]
    fn draft_requires_a_subject_unless_it_replies() {
        let mut args = draft_args(json!({}));
        args.as_object_mut().unwrap().remove("subject");
        assert_eq!(
            draft(args.clone()).unwrap_err(),
            "`subject` is required unless `reply_to_id` is given"
        );
        args["reply_to_id"] = json!("m0");
        assert!(plan(Provided::GmailDraft, &args, &settings()).is_ok());
    }

    #[test]
    fn a_reply_reads_its_original_then_posts_in_its_thread() {
        let state = reply_state(json!({ "account": "alice@example.com" }));
        let read = plan(Provided::GmailDraft, &state.args, &settings()).unwrap();
        let mut expected_query = metadata_query(DRAFT_ORIGINAL_FIELDS, DRAFT_ORIGINAL_HEADERS);
        expected_query.insert(
            "prettyPrint".to_string(),
            QueryValue::One("false".to_string()),
        );
        assert_eq!(
            read.api,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "GET".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/gmail/v1/users/me/messages/m0".to_string(),
                query: expected_query,
                headers: BTreeMap::new(),
                body: None,
            }
        );
        let headers = [
            ("Subject", "Lunch"),
            ("Message-ID", "<b@example.com>"),
            ("References", "<a@example.com>"),
        ];
        let Next::Call { call, derived } = answered(&state, original(&headers)) else {
            panic!("a reply posts after its read");
        };
        assert_eq!(call.step, DRAFT_CREATE_STEP);
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.path, DRAFTS_PATH);
        assert_eq!(call.api.account.as_deref(), Some("alice@example.com"));
        let body: Value = serde_json::from_str(call.api.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["message"]["threadId"], json!("t1"));
        assert_eq!(
            raw_message(&call.api),
            "To: alice@example.com\r\n\
             Subject: Re: Lunch\r\n\
             In-Reply-To: <b@example.com>\r\n\
             References: <a@example.com> <b@example.com>\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Transfer-Encoding: base64\r\n\
             \r\n\
             Tm9vbj8=\r\n"
        );
        let (row, _) = follow(&state, Next::Call { call, derived });
        let resource = json!({ "id": "r1", "message": { "id": "m9", "threadId": "t1" } });
        assert_eq!(
            answered(&row, http(200, &resource.to_string())),
            Next::Complete(json!({
                "draft_id": "r1",
                "message_id": "m9",
                "thread_id": "t1",
                "in_reply_to": "<b@example.com>",
                "subject": "Re: Lunch",
            }))
        );
    }

    #[test]
    fn a_reply_filed_outside_the_originals_thread_says_so() {
        let state = reply_state(json!({}));
        let Next::Call { call, derived } = answered(&state, original(&[("Subject", "Lunch")]))
        else {
            panic!("a reply posts after its read");
        };
        assert_eq!(derived.get("thread_id"), Some(&json!("t1")));
        let (row, _) = follow(&state, Next::Call { call, derived });
        for (message, expected) in [
            (
                json!({ "id": "m9", "threadId": "t1" }),
                json!({ "draft_id": "r1", "message_id": "m9", "thread_id": "t1", "subject": "Re: Lunch" }),
            ),
            (
                json!({ "id": "m9", "threadId": "t2" }),
                json!({
                    "draft_id": "r1",
                    "message_id": "m9",
                    "thread_id": "t2",
                    "in_thread": false,
                    "subject": "Re: Lunch",
                }),
            ),
            (
                json!({ "id": "m9" }),
                json!({ "draft_id": "r1", "message_id": "m9", "subject": "Re: Lunch" }),
            ),
        ] {
            let resource = json!({ "id": "r1", "message": message });
            assert_eq!(
                answered(&row, http(200, &resource.to_string())),
                Next::Complete(expected),
                "{resource}"
            );
        }
    }

    #[test]
    fn a_reply_derives_its_headers_from_the_original() {
        let state = reply_state(json!({}));
        let id = "<b@example.com>";
        let long_id = format!("<{}>", "i".repeat(HEADER_VALUE_MAX_BYTES - 2));
        let joinable = "r".repeat(HEADER_VALUE_MAX_BYTES - id.len() - 1);
        let too_long = format!("{joinable}r");
        // `Message-ID` and `References` as the original carries them, and the
        // draft's `In-Reply-To` and `References`.
        for (headers, in_reply_to, references) in [
            (vec![("Message-ID", id)], Some(id), Some(id.to_string())),
            (
                vec![("Message-ID", id), ("References", "<a@x> <z@x>")],
                Some(id),
                Some(format!("<a@x> <z@x> {id}")),
            ),
            (
                vec![("Message-ID", id), ("References", &joinable)],
                Some(id),
                Some(format!("{joinable} {id}")),
            ),
            (
                vec![("Message-ID", id), ("References", &too_long)],
                Some(id),
                Some(id.to_string()),
            ),
            (
                vec![("Message-ID", id), ("References", "<a@x>\t<z@x>")],
                Some(id),
                Some(id.to_string()),
            ),
            (
                vec![("Message-ID", id), ("References", "<ä@x>")],
                Some(id),
                Some(id.to_string()),
            ),
            (
                vec![("Message-ID", id), ("References", "")],
                Some(id),
                Some(id.to_string()),
            ),
            (
                vec![("Message-ID", &long_id)],
                Some(long_id.as_str()),
                Some(long_id.clone()),
            ),
            (vec![], None, None),
            (vec![("References", "<a@x>")], None, None),
            (vec![("Message-ID", "<b @x>")], None, None),
            (vec![("Message-ID", "<b\r\n@x>")], None, None),
            (vec![("Message-ID", "<bé@x>")], None, None),
            (vec![("Message-ID", "")], None, None),
            (vec![("Message-ID", &format!("{long_id}x"))], None, None),
        ] {
            let (call, message, derived) = replied(&state, &headers);
            assert_eq!(
                message_header(&message, "In-Reply-To").as_deref(),
                in_reply_to,
                "{headers:?}"
            );
            assert_eq!(
                message_header(&message, "References"),
                references,
                "{headers:?}"
            );
            assert_eq!(
                derived.get("in_reply_to"),
                in_reply_to.map(|id| json!(id)).as_ref(),
                "{headers:?}"
            );
            // The thread is the original's whether or not its headers were
            // usable.
            let body: Value = serde_json::from_str(call.body.as_deref().unwrap()).unwrap();
            assert_eq!(body["message"]["threadId"], json!("t1"));
        }
    }

    #[test]
    fn a_reply_derives_its_subject_from_the_original() {
        let state = reply_state(json!({}));
        let long = "s".repeat(DERIVED_SUBJECT_MAX_BYTES);
        let long_mapped = format!("{}\u{85}", "s".repeat(DERIVED_SUBJECT_MAX_BYTES - 1));
        let long_spaced = format!("{} ", "s".repeat(DERIVED_SUBJECT_MAX_BYTES - 1));
        for (original, derived) in [
            (Some("Lunch"), "Re: Lunch".to_string()),
            (Some("Re: Lunch"), "Re: Lunch".to_string()),
            (Some("  RE:Lunch"), "  RE:Lunch".to_string()),
            (Some("re:"), "re:".to_string()),
            (Some("Rex"), "Re: Rex".to_string()),
            (
                Some("Lunch\r\n\tplans\u{85}"),
                "Re: Lunch   plans ".to_string(),
            ),
            (Some("Grüße"), "Re: Grüße".to_string()),
            (Some(""), "Re:".to_string()),
            (Some(" \t"), "Re:".to_string()),
            (None, "Re:".to_string()),
            (Some(&long), format!("Re: {long}")),
            // Bounded after the mapping: 1001 bytes as sent, 1000 as derived.
            (Some(&long_mapped), format!("Re: {long_spaced}")),
        ] {
            let headers: Vec<(&str, &str)> = original.map(|s| ("Subject", s)).into_iter().collect();
            let (_, message, answer) = replied(&state, &headers);
            assert_eq!(answer.get("subject"), Some(&json!(derived)), "{original:?}");
            let subject = message_header(&message, "Subject").unwrap();
            let written = if subject.starts_with("=?") {
                compose::decoded_words(&subject)
            } else {
                subject
            };
            assert_eq!(written, derived, "{original:?}");
        }
        // Over the bound, the caller is to give `subject`; nothing is posted.
        let over = "s".repeat(DERIVED_SUBJECT_MAX_BYTES + 1);
        assert_eq!(
            answered(&state, original(&[("Subject", &over)])),
            Next::Fail(failure(
                ErrorKind::InvalidArgs,
                "the original's subject is longer than 1000 bytes; give `subject`"
            ))
        );
        // A caller's subject is used as given, and the answer names none.
        let given = reply_state(json!({ "subject": "Lunch plans" }));
        let (_, message, answer) = replied(&given, &[("Subject", &over)]);
        assert!(
            message.contains("\r\nSubject: Lunch plans\r\n"),
            "{message}"
        );
        assert_eq!(answer.get("subject"), None);
    }

    #[test]
    fn a_reply_reads_an_original_of_another_shape_as_internal() {
        let state = reply_state(json!({}));
        for body in [
            json!([]),
            json!({ "id": "m0" }),
            json!({ "threadId": "t1" }),
            json!({ "id": "other", "threadId": "t1" }),
            json!({ "id": "m0", "threadId": "t 1" }),
            json!({ "id": "m0", "threadId": 1 }),
            json!({ "id": "m0", "threadId": "t1", "payload": [] }),
            json!({ "id": "m0", "threadId": "t1", "payload": { "headers": {} } }),
        ] {
            assert_eq!(
                answered(&state, http(200, &body.to_string())),
                Next::Fail(internal("unexpected Google messages.get response shape")),
                "{body}"
            );
        }
    }

    #[test]
    fn a_reply_read_403_names_the_read_scope_and_a_404_is_the_callers() {
        let state = reply_state(json!({}));
        let body = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        let Next::Fail(denied) = answered(&state, http(403, &body)) else {
            panic!("a 403 fails");
        };
        assert_eq!(denied.kind, ErrorKind::Denied);
        assert!(
            denied
                .detail
                .contains("connected without the Gmail read scope"),
            "{}",
            denied.detail
        );
        let Next::Fail(missing) = answered(
            &state,
            http(
                404,
                &google(404, "NOT_FOUND", "Requested entity was not found."),
            ),
        ) else {
            panic!("a 404 fails");
        };
        assert_eq!(missing.kind, ErrorKind::InvalidArgs);
        assert!(
            !missing.detail.contains(DRAFT_UNKNOWN_HINT),
            "{}",
            missing.detail
        );
        // A read failure is not a write's: no hint.
        assert_eq!(
            answered(&state, http(500, "{}")),
            Next::Fail(google_error(500, "{}", Scope::GmailRead))
        );
    }

    #[test]
    fn a_draft_answers_its_ids() {
        let state = draft_state(draft_args(json!({})));
        let resource = json!({
            "id": "r-123",
            "message": { "id": "m1", "threadId": "t1", "labelIds": ["DRAFT"] },
        });
        assert_eq!(
            answered(&state, http(200, &resource.to_string())),
            Next::Complete(json!({ "draft_id": "r-123", "message_id": "m1", "thread_id": "t1" }))
        );
        let resource = json!({ "id": "r-123", "message": { "id": "m1" } });
        assert_eq!(
            answered(&state, http(200, &resource.to_string())),
            Next::Complete(json!({ "draft_id": "r-123", "message_id": "m1" }))
        );
    }

    #[test]
    fn a_draft_answer_of_another_shape_is_internal_with_the_hint() {
        let state = draft_state(draft_args(json!({})));
        let refusal =
            format!("unexpected Google drafts.create response shape; {DRAFT_UNKNOWN_HINT}");
        for resource in [
            json!([]),
            json!({ "message": { "id": "m1" } }),
            json!({ "id": "r/1", "message": { "id": "m1" } }),
            json!({ "id": "r1" }),
            json!({ "id": "r1", "message": "m1" }),
            json!({ "id": "r1", "message": { "id": 1 } }),
            json!({ "id": "r1", "message": { "id": "m1", "threadId": "t 1" } }),
        ] {
            assert_eq!(
                answered(&state, http(200, &resource.to_string())),
                Next::Fail(internal(refusal.clone())),
                "{resource}"
            );
        }
    }

    #[test]
    fn a_draft_403_names_the_compose_scope() {
        let body = google(
            403,
            "PERMISSION_DENIED",
            "Request had insufficient authentication scopes.",
        );
        assert_eq!(
            answered(&draft_state(draft_args(json!({}))), http(403, &body)),
            Next::Fail(failure(
                ErrorKind::Denied,
                "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication \
                 scopes. If Google names insufficient authentication scopes, the account was \
                 connected without the Gmail compose scope: run oauth-connect with that scope \
                 set and relay its steps."
            ))
        );
    }

    #[test]
    fn a_draft_never_calls_a_send_path() {
        for args in [
            draft_args(json!({})),
            draft_args(json!({ "cc": "bob@example.com", "account": "alice@example.com" })),
        ] {
            let call = draft(args).unwrap();
            assert_eq!(call.method, "POST");
            assert_eq!(call.path, DRAFTS_PATH);
            assert!(!call.path.contains("send"));
            assert!(call.query.keys().all(|key| key == "prettyPrint"));
        }
        // A reply reads its original and then posts the draft.
        let state = reply_state(json!({}));
        assert_eq!(state.target.method, "GET");
        assert_eq!(state.target.path, format!("{MESSAGES_PATH}/m0"));
        let (row, call) = follow(&state, answered(&state, original(&[])));
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.path, DRAFTS_PATH);
        // Every answer to the `POST` completes or fails; none issues a call.
        for state in [draft_state(draft_args(json!({}))), row] {
            for outcome in [
                http(200, r#"{"id":"r1","message":{"id":"m1"}}"#),
                http(204, ""),
                http(500, "{}"),
                cut_read(json!(300000)),
            ] {
                assert!(
                    !matches!(answered(&state, outcome), Next::Call { .. }),
                    "a draft posts once"
                );
            }
        }
    }

    #[test]
    fn the_widest_draft_fits_the_args_cap() {
        // A reply whose original carries `In-Reply-To` and `References` at
        // their bounds and the longest thread id; the caller's subject is
        // wider than any derived one (2 000 bytes against 1 004).
        let with = |body: String| {
            let args = json!({
                "to": "x".repeat(HEADER_VALUE_MAX_BYTES),
                "cc": "y".repeat(HEADER_VALUE_MAX_BYTES),
                "subject": four_byte(0, SUBJECT_MAX_CHARS),
                "body": body,
                "reply_to_id": "m".repeat(ID_MAX_CHARS),
                "account": escaped_widest(ACCOUNT_MAX_CHARS),
            });
            let call = plan(Provided::GmailDraft, &args, &settings())?;
            let state = InFlight::new(
                "a",
                Provided::GmailDraft,
                &args,
                &call,
                Map::new(),
                1_000,
                1_000,
            );
            let message_id = format!("<{}>", "i".repeat(HEADER_VALUE_MAX_BYTES - 2));
            let original = json!({
                "id": "m".repeat(ID_MAX_CHARS),
                "threadId": "t".repeat(ID_MAX_CHARS),
                "payload": { "headers": [
                    { "name": "Message-ID", "value": message_id },
                    { "name": "References", "value": "<r@example.com>" },
                ] },
            });
            let Next::Call { call, .. } = answered(&state, http(200, &original.to_string())) else {
                panic!("a reply posts after its read");
            };
            let message = raw_message(&call.api);
            // Both at the bound: a `Message-ID` that leaves no room to join
            // the original's `References` is the draft's `References` alone.
            assert!(message.contains(&format!("\r\nIn-Reply-To: {message_id}\r\n")));
            assert!(message.contains(&format!("\r\nReferences: {message_id}\r\n")));
            Ok::<ApiCall, String>(call.api)
        };
        let four_byte_body = four_byte(1, BODY_MAX_BYTES / 4);
        assert_eq!(four_byte_body.len(), BODY_MAX_BYTES);
        let newline_dense = format!("x{}", "\n".repeat(BODY_MAX_BYTES / 2 - 1));
        assert_eq!(
            compose::canonical(&newline_dense).as_str().len(),
            BODY_MAX_BYTES - 1
        );
        for body in [four_byte_body, newline_dense] {
            let call = with(body).unwrap();
            let args = serde_json::to_vec(&call).unwrap();
            assert!(
                args.len() <= HOST_ARGS_CAP_BYTES,
                "{} bytes of args",
                args.len()
            );
        }
        assert_eq!(
            with(format!("x{}", "\n".repeat(BODY_MAX_CHARS - 1))).unwrap_err(),
            "`body` is longer than 20000 bytes (UTF-8, a line break counting as two)"
        );
    }

    #[test]
    fn a_row_names_every_provided_tool_by_its_registry_name() {
        let state = draft_state(draft_args(json!({})));
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
        wire["tool"] = json!("GmailDraft");
        let error = InFlight::decode(&serde_json::to_vec(&wire).unwrap()).unwrap_err();
        assert!(
            error.contains("no provided tool is named `GmailDraft`"),
            "{error}"
        );
    }

    /// Every step `tool` issues a call at.
    fn steps(tool: Provided) -> Vec<u8> {
        match tool {
            Provided::GmailDraft => vec![DRAFT_ORIGINAL_STEP, DRAFT_CREATE_STEP],
        }
    }

    #[test]
    fn every_step_names_its_scope() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                let expected = match (tool, step) {
                    (Provided::GmailDraft, DRAFT_CREATE_STEP) => Scope::GmailCompose,
                    _ => Scope::GmailRead,
                };
                assert_eq!(scope(tool, step), expected, "{} step {step}", tool.name());
            }
        }
    }

    #[test]
    fn argument_caps_count_characters_not_bytes() {
        admits_characters(draft, draft_args(json!({})), "account", ACCOUNT_MAX_CHARS);
        admits_characters(draft, draft_args(json!({})), "subject", SUBJECT_MAX_CHARS);
        // `body`'s character cap holds for ASCII; for wider text its byte
        // budget is the narrower rule.
        let body = |text: String| draft(draft_args(json!({ "body": text })));
        assert!(body("x".repeat(BODY_MAX_CHARS)).is_ok());
        assert_eq!(
            body("x".repeat(BODY_MAX_CHARS + 1)).unwrap_err(),
            "`body` is longer than 20000 characters"
        );
        let over_bytes = "`body` is longer than 20000 bytes (UTF-8, a line break counting as two)";
        assert!(body(wide(BODY_MAX_BYTES / 2)).is_ok());
        assert_eq!(body(wide(BODY_MAX_BYTES / 2 + 1)).unwrap_err(), over_bytes);
        assert_eq!(
            body(format!("x{}", "\n".repeat(BODY_MAX_CHARS - 1))).unwrap_err(),
            over_bytes
        );
    }

    /// Every argument `tool` takes, with the bound its parser holds it to,
    /// and the arguments it requires: the parser's own constants and key
    /// lists, as `every_tools_schema_states_what_its_parser_enforces` holds
    /// the spec to them. Three parser rules are outside what a schema
    /// states, each leaving the schema wider than the parser and each in the
    /// description: a draft's `body` holds at most `BODY_MAX_BYTES` in its
    /// CRLF form, as `every_description_states_its_parsers_budgets` holds; its
    /// `subject` holds no control character; and, cross-field, a draft
    /// without `reply_to_id` requires `subject`, as
    /// `draft_requires_a_subject_unless_it_replies` holds. The per-value
    /// probes are this component's, in `workspace_probes`.
    fn bounds(tool: Provided) -> ToolBounds<'static> {
        let (keys, required): (&'static [&'static str], &'static [&'static str]) = match tool {
            Provided::GmailDraft => (DraftArgs::KEYS, &["to", "body"]),
        };
        let text = |max_chars| Bound::Text {
            max_chars,
            format: None,
        };
        let property = |key: &'static str| {
            let bound = match (tool, key) {
                (_, "account") => text(ACCOUNT_MAX_CHARS),
                (Provided::GmailDraft, "to" | "cc") => {
                    Bound::Pattern(printable_ascii_pattern(HEADER_VALUE_MAX_BYTES))
                }
                (Provided::GmailDraft, "subject") => text(SUBJECT_MAX_CHARS),
                (Provided::GmailDraft, "body") => text(BODY_MAX_CHARS),
                (Provided::GmailDraft, "reply_to_id") => Bound::Token {
                    max_chars: ID_MAX_CHARS,
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
            Provided::GmailDraft => draft_args(json!({})),
        }
    }

    /// The probes of the arguments whose bounds are this component's, or
    /// whose parser holds a rule past the kit's bound: a draft's header
    /// values and body. `None` where the kit's probes for the bound hold.
    fn workspace_probes(tool: Provided, key: &str, bound: &Bound) -> Option<Probes> {
        let strings = |values: &[&str]| values.iter().map(|v| json!(v)).collect::<Vec<_>>();
        Some(match (tool, key) {
            (Provided::GmailDraft, "to" | "cc") => Probes {
                admitted: strings(&[
                    &"x".repeat(HEADER_VALUE_MAX_BYTES),
                    "Alice Example <alice@example.com>, bob@example.com",
                    "~",
                ]),
                refused: [
                    strings(&[
                        &"x".repeat(HEADER_VALUE_MAX_BYTES + 1),
                        "",
                        "é",
                        "a\rb",
                        "a\nb",
                        "a\tb",
                        "a\u{7f}b",
                    ]),
                    vec![json!(1)],
                ]
                .concat(),
            },
            (Provided::GmailDraft, "subject") => {
                let mut probes = probes(bound).expect("a text bound has probes");
                probes
                    .refused
                    .extend(strings(&["a\u{1}b", "a\r\nb", "a\tb", "a\u{85}b"]));
                probes
            }
            (Provided::GmailDraft, "body") => Probes {
                admitted: strings(&[
                    &"x".repeat(BODY_MAX_CHARS),
                    &wide(BODY_MAX_BYTES / 2),
                    &format!("x{}", "\n".repeat(BODY_MAX_BYTES / 2 - 1)),
                    "a\u{1}\r\n\tb",
                ]),
                refused: [
                    strings(&[
                        &"x".repeat(BODY_MAX_CHARS + 1),
                        // The byte budget the schema cannot state.
                        &wide(BODY_MAX_BYTES / 2 + 1),
                        &format!("x{}", "\n".repeat(BODY_MAX_CHARS - 1)),
                        "",
                        " \t\n\u{85}\u{3000}",
                    ]),
                    vec![json!(1)],
                ]
                .concat(),
            },
            _ => return None,
        })
    }

    /// The component's specification, as the package ships it.
    fn spec_text() -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../config/specs/google-gmail-compose.brenn");
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
                let probes = workspace_probes(tool, key, bound)
                    .or_else(|| probes(bound))
                    .unwrap_or_else(|| panic!("`{name}`'s `{key}` has no probes"));
                hold_probes(name, key, &probes, &base, plan_with);
            }
        }
    }

    /// The parser budgets no schema can state: each tool's description gives
    /// the number its parser's constant holds.
    #[test]
    fn every_description_states_its_parsers_budgets() {
        let spec = spec_text();
        for (tool, budget) in [
            (
                Provided::GmailDraft,
                format!(
                    "at most {BODY_MAX_CHARS} characters and about {BODY_MAX_BYTES} bytes of \
                     UTF-8 text, where a line break counts as two"
                ),
            ),
            (
                Provided::GmailDraft,
                format!("each 1 to {HEADER_VALUE_MAX_BYTES} printable ASCII characters"),
            ),
            (
                Provided::GmailDraft,
                format!(
                    "an original whose subject is over {DERIVED_SUBJECT_MAX_BYTES} bytes is \
                     refused"
                ),
            ),
        ] {
            assert!(
                description(&spec, tool.name()).contains(&budget),
                "`{}`'s description must say {budget:?}",
                tool.name()
            );
        }
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

        // A reply's read of its original: the rendering alone.
        let state = reply_state(json!({}));
        for (outcome, why) in outcomes() {
            assert_eq!(
                answered(&state, outcome),
                Next::Fail(why.failure(google::API_NAME, None)),
                "{}",
                name(&state)
            );
        }

        // The draft's `POST`: the rendering and its hint.
        let state = draft_state(draft_args(json!({})));
        let wording = "; the draft may have been created before its answer was lost: look for it \
                       with GmailSearch (`q: in:drafts`) before asking again, or the user has two";
        for (outcome, why) in outcomes() {
            let answer = answered(&state, outcome);
            assert_eq!(
                answer,
                Next::Fail(why.failure(google::API_NAME, Some(DRAFT_UNKNOWN_HINT))),
                "{}",
                name(&state)
            );
            let Next::Fail(Failure { detail, .. }) = answer else {
                panic!("{answer:?}");
            };
            assert!(detail.ends_with(wording), "{detail}");
        }

        for (outcome, _) in outcomes() {
            assert_eq!(
                answered(&at(draft_state(draft_args(json!({}))), 3), outcome),
                Next::Fail(no_such_step(Provided::GmailDraft, 3))
            );
        }
    }

    /// What `tool`'s step `step` expects Google to answer, as a no-body
    /// refusal names it.
    fn no_body_answers(tool: Provided, step: u8) -> &'static str {
        match (tool, step) {
            (Provided::GmailDraft, DRAFT_CREATE_STEP) => "the draft",
            _ => "JSON",
        }
    }

    #[test]
    fn an_empty_answer_is_internal_at_every_step() {
        for tool in Provided::ALL {
            for step in steps(tool) {
                let mut state = draft_state(draft_args(json!({})));
                state.tool = tool;
                state.step = step;
                for status in [200, 204] {
                    let Next::Fail(Failure { kind, detail }) = answered(&state, http(status, ""))
                    else {
                        panic!("{} step {step} accepted no body", tool.name());
                    };
                    assert_eq!(kind, ErrorKind::Internal);
                    let answers = no_body_answers(tool, step);
                    let expected = match write_hint(tool, step) {
                        Some(hint) => format!("{answers}; {hint}"),
                        None => answers.to_string(),
                    };
                    assert_eq!(
                        no_body_parts(&detail, status).map(|(_, rest)| rest),
                        Some(expected.as_str()),
                        "{detail}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_step_names_one_api_for_no_body_and_for_a_bad_shape() {
        let bases = [reply_state(json!({}))];
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
            let mut state = reply_state(json!({}));
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

    /// The row of `tool` at its write step `step`, as `write_hint` names it.
    fn write_step_state(tool: Provided, step: u8) -> InFlight {
        let state = match (tool, step) {
            (Provided::GmailDraft, DRAFT_CREATE_STEP) => draft_state(draft_args(json!({}))),
            _ => panic!("no row for {} step {step}", tool.name()),
        };
        assert_eq!((state.tool, state.step), (tool, step));
        state
    }

    #[test]
    fn every_write_steps_failure_carries_its_hint_only_when_google_may_have_acted() {
        let mut write_steps = 0;
        for tool in Provided::ALL {
            for step in steps(tool) {
                let Some(hint) = write_hint(tool, step) else {
                    continue;
                };
                write_steps += 1;
                let state = write_step_state(tool, step);
                let at = format!("{} step {step}", tool.name());
                let hinted = |detail: &str| Next::Fail(internal(format!("{detail}; {hint}")));
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
                    assert_eq!(answered(&state, outcome), hinted(detail), "{at}");
                }
                for outcome in [
                    http(500, "{}"),
                    http(503, "{}"),
                    http(429, "{}"),
                    cut_read(json!(300000)),
                ] {
                    let Next::Fail(failed) = answered(&state, outcome) else {
                        panic!("{at} fails");
                    };
                    assert_eq!(failed.kind, ErrorKind::Internal, "{at}");
                    assert!(
                        failed.detail.ends_with(&format!("; {hint}")),
                        "{at}: {failed:?}"
                    );
                }
                // Google's refusal, or `api-request`'s, changed nothing: its
                // kind and message, no hint.
                let denied = format!(
                    "Google PERMISSION_DENIED (HTTP 403): Insufficient Permission. {}",
                    scope_hint(scope(tool, step))
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
                        answered(&state, outcome),
                        Next::Fail(failure(kind, detail)),
                        "{at}"
                    );
                }
                let Next::Fail(refused) = answered(&state, nested("denied", "no clause admits it"))
                else {
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
        assert!(write_steps > 0);
        // A read step's failure carries no hint.
        assert_eq!(
            answered(
                &reply_state(json!({})),
                nested("invalid_args", "bad wrapper")
            ),
            Next::Fail(internal(
                "api-request rejected the wrapper's request: bad wrapper"
            ))
        );
    }
}
