//! The wrapper's decisions, in `std` plus `serde_json`: validating
//! `GmailDraft`'s arguments and files, building its Gmail calls as
//! `api-request` arguments, building the message a draft uploads as the
//! call's body, and reading `api-request`'s answer. Nothing here names the
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
use brenn_google_workspace::call::{get_call, new_call};
#[cfg(test)]
use brenn_google_workspace::gmail::decode;
use brenn_google_workspace::gmail::{
    HEADER_MAX_BYTES, MESSAGES_PATH, gmail_id, is_gmail_id, metadata_query, raw_header,
};
use brenn_google_workspace::scope::{Scope, google_error, no_body_refused, with_write_hint};
use brenn_google_workspace::settings::Settings;
use brenn_google_workspace::text::shape;
use brenn_provider_kit::{
    Answer, ApiCall, Args, Attached, Blob, Body, COMPACT_RESULT_CEILING, Call, ErrorKind, Failure,
    Logic, NestedError, Next, Queued, Step, Tool, Unreadable, bounded, capped, copy, google,
    internal, no_such_step, object, query, read_api_result, stored,
};
use serde_json::{Value, json};

use crate::spec::Provided;

/// The path Gmail creates a draft at from an uploaded message. Nothing below
/// it is ever called: a draft is sent only by the user, from Gmail.
const DRAFTS_UPLOAD_PATH: &str = "/upload/gmail/v1/users/me/drafts";
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
/// counting as the two bytes it becomes. The message is built as the upload's
/// body, so the body is not under the host's cap on a nested call's
/// arguments; it rides the request's in-flight row whole, which every settle
/// of the row decodes, a late answer's included, on the envelope term alone.
/// Measured on the real host, five late answers to the widest reply settled
/// in one activation, the body of U+0001, the costliest content for both
/// steps:
/// - to the upload, the settle decodes the row: about 42.0M fuel of a 120.6M
///   grant over 12 envelopes, 0.348 of it, at about 2,000 fuel a body byte,
///   meeting the 19/28 margin at a body of about 39,900 bytes;
/// - to the read of the original, the settle also parses the arguments again
///   in `draft_reply`, and the kit drops the upload it returns: about 49.8M
///   of a 72.0M grant over 7 envelopes, 0.692 of it, at about 2,380 fuel a
///   body byte, meeting the margin at a body of about 19,600 bytes.
///
/// The lower figure, about 19,600 bytes, is where a late-answer activation
/// meets the margin: at this bound the read step's late answers are over it,
/// though inside the grant. An activation settling late answers to both
/// steps was not measured; it is bounded by five of the read step's, the
/// costlier, and as the two steps' activations are granted differently, its
/// share is taken as no higher than the read step's. The bound is kept at
/// 20,000 by choice and not raised; before any rise the builds and both
/// steps' late answers would be measured at it.
// TODO(gmail-draft-late-answers-over-fuel-margin): five late answers to the
// widest reply's read settle over the margin at this bound.
const BODY_MAX_BYTES: usize = 20_000;
/// The most files a draft carries.
const FILES_MAX: usize = 5;
/// The most bytes a draft's files hold together. Base64 in 76-character
/// lines writes 78 bytes for every 57, so files this size write at most
/// 14 348 954 bytes, and everything else in the widest built message is
/// under 65 536: one built message is under 14 414 490 bytes, and the kit's
/// `BUILD_BUDGET` of four in one activation write under 57 657 960 bytes of
/// the host's 64 MiB an activation. Measured on the real host, a build costs
/// about 28 fuel a file byte of the 128 the host grants one, and the
/// costliest activation building drafts at this bound, two replies, completes
/// on about 0.22 of its production grant, inside the 19/28 margin.
const FILES_MAX_BYTES: u64 = 10 * 1024 * 1024;
/// The most characters a file's name holds: the name the caller attached it
/// under, the file name the recipient sees.
const FILE_NAME_MAX_CHARS: usize = 64;
/// The most bytes a file's content type holds as the host records it.
const CONTENT_TYPE_MAX_BYTES: usize = 255;
/// The file bytes the base64 encoder takes at once: a whole number of lines,
/// so each slice encodes to 79 872 bytes the build writes in one call.
const ENCODE_SLICE_BYTES: usize = compose::LINE_INPUT_BYTES * 1024;
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

    fn plan(
        tool: Provided,
        args: &Value,
        step: &Step<'_>,
        settings: &Settings,
    ) -> Result<Call, String> {
        plan(tool, args, step.request_attachments(), settings)
    }

    fn advance(
        state: &InFlight,
        outcome: Result<Value, NestedError>,
        _: &mut Step<'_>,
        settings: &Settings,
    ) -> Next {
        advance(state, outcome, settings)
    }

    fn body(call: &Queued<Provided>, step: &Step<'_>, _: &Settings) -> Result<Blob, Failure> {
        match (call.tool, call.call.step) {
            (Provided::GmailDraft, DRAFT_CREATE_STEP) => draft_message(call, step),
            (tool, step) => Err(no_such_step(tool, step)),
        }
    }
}

/// Validate a request's arguments and files and build its first nested call.
/// An error is the request's `invalid_args` detail, naming the argument or
/// the bound.
pub fn plan(
    tool: Provided,
    args: &Value,
    attachments: &[Attached],
    settings: &Settings,
) -> Result<Call, String> {
    match tool {
        Provided::GmailDraft => {
            let draft = DraftArgs::parse(args)?;
            files(attachments)?;
            let account = draft.account.as_deref();
            Ok(match &draft.reply_to_id {
                Some(id) => Call {
                    step: DRAFT_ORIGINAL_STEP,
                    api: draft_original_call(id, account, settings),
                },
                None => Call {
                    step: DRAFT_CREATE_STEP,
                    api: upload_call(account, false, settings),
                },
            })
        }
    }
}

/// The request's files as a draft takes them: at most `FILES_MAX`, at most
/// `FILES_MAX_BYTES` together, each named as a quoted `filename` holds it
/// unescaped and typed as a header line holds it unfolded.
fn files(attachments: &[Attached]) -> Result<(), String> {
    if attachments.len() > FILES_MAX {
        return Err(format!(
            "`GmailDraft` takes at most {FILES_MAX} files; this call attaches {}",
            attachments.len()
        ));
    }
    let total: u64 = attachments.iter().map(|file| file.size).sum();
    if total > FILES_MAX_BYTES {
        return Err(format!(
            "the files are {total} bytes together; a draft takes at most {FILES_MAX_BYTES}"
        ));
    }
    for file in attachments {
        if !is_file_name(&file.name) {
            return Err(format!(
                "attachment {:?}'s name must be 1 to {FILE_NAME_MAX_CHARS} letters, digits, \
                 `.`, `_` or `-`",
                file.name
            ));
        }
        if !is_printable_ascii(&file.content_type, CONTENT_TYPE_MAX_BYTES) {
            return Err(format!(
                "attachment `{}`'s content type must be 1 to {CONTENT_TYPE_MAX_BYTES} printable \
                 ASCII characters",
                file.name
            ));
        }
    }
    Ok(())
}

/// Whether `name` is 1 to `FILE_NAME_MAX_CHARS` of `[A-Za-z0-9._-]`.
fn is_file_name(name: &str) -> bool {
    (1..=FILE_NAME_MAX_CHARS).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
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

// TODO(gmail-draft-upload-unconfirmed-against-live-google): create a draft
// with a file and a reply draft through a deployed Brenn against a real
// account, and correct the form if Google refuses it.
/// drafts.create by upload, its body the message `Logic::body` builds: for a
/// new draft the message alone (`uploadType=media`), for a reply the
/// metadata naming the thread and the message (`uploadType=multipart`). The
/// `POST` creates a draft and sends nothing.
fn upload_call(account: Option<&str>, reply: bool, settings: &Settings) -> ApiCall {
    let upload_type = if reply { "multipart" } else { "media" };
    new_call(
        settings,
        "POST",
        account,
        DRAFTS_UPLOAD_PATH.to_string(),
        query([("uploadType", upload_type.to_string())]),
        Some(Body::Built),
    )
}

/// The message a draft uploads, built front to back as the call's body: for
/// a reply, inside the `multipart/related` frame naming its thread; the
/// head; the text's base64; each file's part, its bytes read in pieces and
/// encoded in slices so the build holds one piece and one slice's lines at
/// a time. Everything but the files comes from the caller's arguments and,
/// for a reply, `derived`; a row this build did not write is `internal`.
fn draft_message(call: &Queued<Provided>, step: &Step<'_>) -> Result<Blob, Failure> {
    let args = DraftArgs::parse(&call.args).map_err(|reason| {
        internal(format!(
            "stored {} arguments no longer parse: {reason}",
            call.tool.name()
        ))
    })?;
    files(&call.attachments).map_err(|reason| {
        internal(format!(
            "stored {} attachments no longer pass: {reason}",
            call.tool.name()
        ))
    })?;
    let reply = args.reply_to_id.is_some();
    let thread_id = if reply {
        Some(derived_value(call, "thread_id", is_gmail_id)?.ok_or_else(|| unwritten("thread_id"))?)
    } else {
        None
    };
    let is_header = |value: &str| is_printable_ascii(value, HEADER_VALUE_MAX_BYTES);
    let subject = match &args.subject {
        Some(subject) => subject.as_str(),
        None => derived_value(call, "subject", |subject| {
            !subject.chars().any(char::is_control)
        })?
        .ok_or_else(|| unwritten("subject"))?,
    };
    let head = compose::Head {
        to: &args.to,
        cc: args.cc.as_deref(),
        subject,
        in_reply_to: derived_value(call, "in_reply_to", is_header)?,
        references: derived_value(call, "references", is_header)?,
    };
    let content_type = if reply {
        compose::RELATED_TYPE
    } else {
        compose::MESSAGE_TYPE
    };
    let mut out = step.append(content_type)?;
    if let Some(thread_id) = thread_id {
        out.write(compose::related_open(thread_id).as_bytes())?;
    }
    let with_files = !call.attachments.is_empty();
    out.write(compose::head(&head, with_files).as_bytes())?;
    let mut lines = compose::Lines::default();
    // One slice's lines and the line a held remainder completes.
    let mut encoded = Vec::with_capacity(compose::encoded_len(
        ENCODE_SLICE_BYTES + compose::LINE_INPUT_BYTES,
    ));
    lines.push(args.body.as_str().as_bytes(), &mut encoded);
    lines.finish(&mut encoded);
    out.write(&encoded)?;
    for file in &call.attachments {
        out.write(compose::file_head(&file.name, &file.content_type).as_bytes())?;
        let blob = step.open_request(&file.name)?;
        let mut lines = compose::Lines::default();
        for piece in step.pieces(&blob) {
            for slice in piece?.chunks(ENCODE_SLICE_BYTES) {
                encoded.clear();
                lines.push(slice, &mut encoded);
                out.write(&encoded)?;
            }
        }
        encoded.clear();
        lines.finish(&mut encoded);
        out.write(&encoded)?;
    }
    if with_files {
        out.write(compose::MIXED_CLOSE.as_bytes())?;
    }
    if reply {
        out.write(compose::RELATED_CLOSE.as_bytes())?;
    }
    out.finish()
}

/// `derived`'s string `key`, when present, held to `valid`: anything else is
/// a row this build did not write.
fn derived_value<'a>(
    call: &'a Queued<Provided>,
    key: &str,
    valid: impl Fn(&str) -> bool,
) -> Result<Option<&'a str>, Failure> {
    match call.derived.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if valid(value) => Ok(Some(value)),
        Some(_) => Err(unwritten(key)),
    }
}

/// A stored reply missing `key` from `derived`, or holding one this build
/// would not have written.
fn unwritten(key: &str) -> Failure {
    internal(format!(
        "stored GmailDraft reply has no usable `{key}` in `derived`"
    ))
}

/// A reply's original read: the draft's upload next, carrying in `derived`
/// the original's `thread_id`, which the upload names and the answer is
/// checked against, the `in_reply_to` and `references` the message carries
/// when the original has a usable `Message-ID`, and the derived `subject`.
/// An original whose id is not the one asked for is Google's fault.
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
    if let Some(threading) = threading {
        derived.insert("in_reply_to".to_string(), json!(threading.in_reply_to));
        derived.insert("references".to_string(), json!(threading.references));
    }
    if args.subject.is_none() {
        derived.insert(
            "subject".to_string(),
            json!(derived_subject(original_subject)?),
        );
    }
    Ok(Next::Call {
        call: Call {
            step: DRAFT_CREATE_STEP,
            api: upload_call(args.account.as_deref(), true, settings),
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
        Ok(Answer::Bytes { status, .. }) => {
            let failure = internal(format!(
                "{} returned a non-text body ({status})",
                google::API_NAME
            ));
            return Next::Fail(at_write(state, failure));
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

    use brenn_google_workspace::args::{ACCOUNT_MAX_CHARS, printable_ascii_pattern};
    use brenn_google_workspace::gmail::ID_MAX_CHARS;
    use brenn_google_workspace::scope::scope_hint;
    use brenn_google_workspace::testutil::{
        admits_characters, cut_read, description, escaped_widest, follow, four_byte, lookup,
        no_body_parts, wide,
    };
    use brenn_provider_kit::testutil::{Answered, MemoryHost, failure, google, http, nested};
    use brenn_provider_kit::{
        BUILD_BUDGET, Bound, Delivered, Input, Probes, QueryValue, Request, ToolBounds, WriteError,
        check, hold_probes, hold_required, input_schema, internal, probes,
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
        plan(Provided::GmailDraft, &args, &[], &settings()).map(|call| {
            assert_eq!(call.step, DRAFT_CREATE_STEP);
            call.api
        })
    }

    fn draft_state(args: Value) -> InFlight {
        let call = plan(Provided::GmailDraft, &args, &[], &settings()).unwrap();
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

    /// What a reply uploads after reading `headers` of its original: the
    /// draft's call, the message its upload carries, and the `derived` the
    /// build and the answer read. The upload names thread `t1`.
    fn replied(state: &InFlight, headers: &[(&str, &str)]) -> (ApiCall, String, Args) {
        let Next::Call { call, derived } = answered(state, original(headers)) else {
            panic!("a reply uploads after its read");
        };
        assert_eq!(call.step, DRAFT_CREATE_STEP);
        let (thread, message) = uploaded(&built(&state.args, &derived, &call, &[]));
        assert_eq!(thread.as_deref(), Some("t1"));
        (call.api, String::from_utf8(message).unwrap(), derived)
    }

    /// A file the request attaches: its name, content type and bytes.
    type File = (String, String, Vec<u8>);

    fn file(name: &str, content_type: &str, bytes: Vec<u8>) -> File {
        (name.to_string(), content_type.to_string(), bytes)
    }

    /// `len` bytes of every value.
    fn bytes(len: usize, seed: usize) -> Vec<u8> {
        (0..len)
            .map(|i| ((i * 7 + seed * 13) % 256) as u8)
            .collect()
    }

    /// The header entries `files` make on a request.
    fn attached(files: &[File]) -> Vec<Attached> {
        files
            .iter()
            .map(|(name, content_type, bytes)| Attached {
                name: name.clone(),
                size: bytes.len() as u64,
                content_type: content_type.clone(),
            })
            .collect()
    }

    /// An upload's body: its content type and bytes.
    type Upload = (String, Vec<u8>);

    /// The body `Logic::body` builds for `call` of request `r000`, under
    /// `args`, `derived` and the request's `files`.
    fn built(args: &Value, derived: &Args, call: &Call, files: &[File]) -> Upload {
        let host = MemoryHost::new();
        for (name, content_type, bytes) in files {
            host.seed_request("r000", name, content_type, bytes.clone());
        }
        let queued = Queued {
            id: "r000".to_string(),
            tool: Provided::GmailDraft,
            args: args.clone(),
            derived: derived.clone(),
            call: call.clone(),
            received_at_ms: 0,
            queued_at_ms: 0,
            attachments: attached(files),
        };
        let step = Step::planning(&host, "r000", attached(files));
        let blob = GmailCompose::body(&queued, &step, &settings()).unwrap();
        let mut bytes = Vec::new();
        for piece in step.pieces(&blob) {
            bytes.extend(piece.unwrap());
        }
        (blob.content_type().to_string(), bytes)
    }

    /// A new draft planned from `args` with `files`, and the message its
    /// upload carries.
    fn new_draft(args: Value, files: &[File]) -> (ApiCall, String) {
        let call = plan(Provided::GmailDraft, &args, &attached(files), &settings()).unwrap();
        assert_eq!(call.step, DRAFT_CREATE_STEP);
        let (thread, message) = uploaded(&built(&args, &Args::new(), &call, files));
        assert_eq!(thread, None, "a new draft names no thread");
        (call.api, String::from_utf8(message).unwrap())
    }

    const RELATED_OPEN: &str =
        "--brenn-draft-related\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n";
    const RELATED_MESSAGE: &str =
        "\r\n--brenn-draft-related\r\nContent-Type: message/rfc822\r\n\r\n";
    const RELATED_END: &str = "\r\n--brenn-draft-related--\r\n";

    /// The thread an upload names and the message it carries: a media
    /// upload is the message alone; a multipart upload is exactly the
    /// metadata `{"message":{"threadId":…}}` and the message.
    fn uploaded((content_type, bytes): &Upload) -> (Option<String>, Vec<u8>) {
        if content_type == "message/rfc822" {
            return (None, bytes.clone());
        }
        assert_eq!(
            content_type,
            "multipart/related; boundary=brenn-draft-related"
        );
        let rest = bytes
            .strip_prefix(RELATED_OPEN.as_bytes())
            .expect("the metadata part opens the upload");
        let at = rest
            .windows(RELATED_MESSAGE.len())
            .position(|window| window == RELATED_MESSAGE.as_bytes())
            .expect("the message part follows the metadata");
        let metadata: Value = serde_json::from_slice(&rest[..at]).unwrap();
        let message = rest[at + RELATED_MESSAGE.len()..]
            .strip_suffix(RELATED_END.as_bytes())
            .expect("the closing delimiter ends the upload");
        let thread = metadata["message"]["threadId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(metadata, json!({ "message": { "threadId": thread } }));
        (Some(thread), message.to_vec())
    }

    /// One part of a `multipart/mixed` message: its headers as written, and
    /// its content, base64 decoded.
    type Part = (String, Vec<u8>);

    /// A message carrying files: its head through the `multipart/mixed`
    /// opening, and its parts in order, the text first.
    fn mixed(message: &str) -> (String, Vec<Part>) {
        let (head, rest) = message.split_once("\r\n\r\n").unwrap();
        assert!(
            head.ends_with("\r\nContent-Type: multipart/mixed; boundary=brenn-draft-mixed"),
            "{head}"
        );
        let rest = rest
            .strip_suffix("--brenn-draft-mixed--\r\n")
            .expect("the closing delimiter ends the message");
        let mut parts = rest.split("--brenn-draft-mixed\r\n");
        assert_eq!(parts.next(), Some(""), "the first delimiter opens the body");
        let parts = parts
            .map(|part| {
                let (headers, lines) = part.split_once("\r\n\r\n").unwrap();
                (headers.to_string(), decode::base64url(lines).unwrap())
            })
            .collect();
        (head.to_string(), parts)
    }

    /// A file part's headers for `name` and `content_type`.
    fn file_part(name: &str, content_type: &str) -> String {
        format!(
            "Content-Type: {content_type}\r\n\
             Content-Disposition: attachment; filename=\"{name}\"\r\n\
             Content-Transfer-Encoding: base64"
        )
    }

    const TEXT_PART: &str =
        "Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64";

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

    /// A message's body, its base64 lines decoded.
    fn message_body(message: &str) -> String {
        let (_, body) = message.split_once("\r\n\r\n").unwrap();
        String::from_utf8(decode::base64url(body).unwrap()).unwrap()
    }

    #[test]
    fn a_draft_uploads_one_message_on_the_gmail_host() {
        let (call, message) = new_draft(
            draft_args(json!({
                "cc": "Bob Example <bob@example.com>",
                "account": "alice@example.com",
            })),
            &[],
        );
        assert_eq!(
            call,
            ApiCall {
                provider: "google".to_string(),
                account: Some("alice@example.com".to_string()),
                method: "POST".to_string(),
                host: "gmail.example.com".to_string(),
                path: "/upload/gmail/v1/users/me/drafts".to_string(),
                query: query([
                    ("prettyPrint", "false".to_string()),
                    ("uploadType", "media".to_string()),
                ]),
                headers: BTreeMap::new(),
                body: Some(Body::Built),
                response_attachment: None,
            }
        );
        assert_eq!(
            message,
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
        let (_, message) = new_draft(
            draft_args(json!({
                "subject": "Grüße aus Köln",
                "body": "Привет,\nмир\r\n🎵",
            })),
            &[],
        );
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
        let (_, message) = new_draft(draft_args(json!({ "body": "a\u{1}\tb\r" })), &[]);
        assert_eq!(message_body(&message), "a\u{1}\tb\r\n");
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
        assert!(plan(Provided::GmailDraft, &args, &[], &settings()).is_ok());
    }

    #[test]
    fn a_reply_reads_its_original_then_uploads_in_its_thread() {
        let state = reply_state(json!({ "account": "alice@example.com" }));
        let read = plan(Provided::GmailDraft, &state.args, &[], &settings()).unwrap();
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
                response_attachment: None,
            }
        );
        let headers = [
            ("Subject", "Lunch"),
            ("Message-ID", "<b@example.com>"),
            ("References", "<a@example.com>"),
        ];
        let Next::Call { call, derived } = answered(&state, original(&headers)) else {
            panic!("a reply uploads after its read");
        };
        assert_eq!(call.step, DRAFT_CREATE_STEP);
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.path, DRAFTS_UPLOAD_PATH);
        assert_eq!(call.api.account.as_deref(), Some("alice@example.com"));
        assert_eq!(
            call.api.query.get("uploadType"),
            Some(&QueryValue::One("multipart".to_string()))
        );
        assert_eq!(call.api.body, Some(Body::Built));
        assert_eq!(
            derived,
            json!({
                "thread_id": "t1",
                "in_reply_to": "<b@example.com>",
                "references": "<a@example.com> <b@example.com>",
                "subject": "Re: Lunch",
            })
            .as_object()
            .unwrap()
            .clone()
        );
        let (thread, message) = uploaded(&built(&state.args, &derived, &call, &[]));
        assert_eq!(thread.as_deref(), Some("t1"));
        assert_eq!(
            String::from_utf8(message).unwrap(),
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
            panic!("a reply uploads after its read");
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
            // The thread is the original's whether or not its headers were
            // usable: `replied` holds the upload to `t1`.
            let (_, message, derived) = replied(&state, &headers);
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
            assert_eq!(
                derived.get("references"),
                references.map(|references| json!(references)).as_ref(),
                "{headers:?}"
            );
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
            assert_eq!(call.path, DRAFTS_UPLOAD_PATH);
            assert!(!call.path.contains("send"));
            assert_eq!(
                call.query.keys().collect::<Vec<_>>(),
                ["prettyPrint", "uploadType"]
            );
        }
        let state = reply_state(json!({}));
        assert_eq!(state.target.method, "GET");
        assert_eq!(state.target.path, format!("{MESSAGES_PATH}/m0"));
        let (row, call) = follow(&state, answered(&state, original(&[])));
        assert_eq!(call.api.method, "POST");
        assert_eq!(call.api.path, DRAFTS_UPLOAD_PATH);
        assert_eq!(
            call.api.query.keys().collect::<Vec<_>>(),
            ["prettyPrint", "uploadType"]
        );
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
            let plan_with = |args: &Value| plan(tool, args, &[], &settings()).map(drop);
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
            (
                Provided::GmailDraft,
                format!("at most {FILES_MAX} files of at most {FILES_MAX_BYTES} bytes together"),
            ),
            (
                Provided::GmailDraft,
                "attached under the name the call gives it, which is the file name the \
                 recipient sees"
                    .to_string(),
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

    #[test]
    fn a_new_draft_carries_its_files_as_mixed_parts_in_order() {
        let pdf = file("report.pdf", "application/pdf", bytes(5_000, 1));
        let (_, message) = new_draft(draft_args(json!({})), std::slice::from_ref(&pdf));
        let (head, parts) = mixed(&message);
        assert_eq!(
            head,
            "To: alice@example.com\r\n\
             Subject: Lunch\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: multipart/mixed; boundary=brenn-draft-mixed"
        );
        assert_eq!(
            parts,
            [
                (TEXT_PART.to_string(), b"Noon?".to_vec()),
                (file_part("report.pdf", "application/pdf"), pdf.2.clone()),
            ]
        );

        let five = [
            file("a.txt", "text/plain; charset=utf-8", bytes(1, 2)),
            file("empty.bin", "application/octet-stream", Vec::new()),
            file("mail.eml", "message/rfc822", bytes(57, 3)),
            file("bundle", "Multipart/Mixed; boundary=x", bytes(58, 4)),
            file("photo.JPG", "image/jpeg", bytes(70_000, 5)),
        ];
        let (_, message) = new_draft(draft_args(json!({})), &five);
        let (_, parts) = mixed(&message);
        let expected: Vec<Part> = [(TEXT_PART.to_string(), b"Noon?".to_vec())]
            .into_iter()
            .chain([
                (
                    file_part("a.txt", "text/plain; charset=utf-8"),
                    five[0].2.clone(),
                ),
                (
                    file_part("empty.bin", "application/octet-stream"),
                    Vec::new(),
                ),
                (
                    file_part("mail.eml", "application/octet-stream"),
                    five[2].2.clone(),
                ),
                (
                    file_part("bundle", "application/octet-stream"),
                    five[3].2.clone(),
                ),
                (file_part("photo.JPG", "image/jpeg"), five[4].2.clone()),
            ])
            .collect();
        assert_eq!(parts, expected);
    }

    /// Requests `r001` to `r005`, each a reply to `m0` without `subject`;
    /// the fifth attaches `files`. Their reads answered in one activation
    /// spend the build budget on the first four, so the fifth's upload is
    /// queued and built when a later activation drains it.
    #[test]
    fn a_reply_built_from_the_backlog_carries_its_files_and_derived_headers() {
        let host = MemoryHost::new();
        let pdf = file("report.pdf", "application/pdf", bytes(3_000, 6));
        let ids: Vec<String> = (1..=BUILD_BUDGET + 1).map(|i| format!("r{i:03}")).collect();
        let mut args = draft_args(json!({ "reply_to_id": "m0" }));
        args.as_object_mut().unwrap().remove("subject");
        let requests = ids
            .iter()
            .map(|id| Request {
                id: id.clone(),
                tool: Provided::GmailDraft.name().to_string(),
                args: args.clone(),
                attachments: if *id == ids[BUILD_BUDGET] {
                    vec![host.seed_request(id, &pdf.0, &pdf.1, pdf.2.clone())]
                } else {
                    Vec::new()
                },
            })
            .collect();
        host.activate::<GmailCompose>(
            &settings(),
            Input {
                now_ms: 0,
                requests,
                results: Vec::new(),
            },
        )
        .unwrap();
        let reads = host.take_calls();
        assert_eq!(reads.len(), BUILD_BUDGET + 1);
        let headers = [
            ("Subject", "Lunch"),
            ("Message-ID", "<b@example.com>"),
            ("References", "<a@example.com>"),
        ];
        let results = reads
            .iter()
            .map(|read| Delivered {
                call_id: read.call_id.clone(),
                serving: Some(read.id.clone()),
                outcome: original(&headers),
                attachments: Vec::new(),
            })
            .collect();
        host.activate::<GmailCompose>(
            &settings(),
            Input {
                now_ms: 1,
                results,
                requests: Vec::new(),
            },
        )
        .unwrap();
        let uploads = host.take_calls();
        assert_eq!(uploads.len(), BUILD_BUDGET);
        assert!(uploads.iter().all(|upload| upload.id != ids[BUILD_BUDGET]));
        assert_eq!(
            host.rows(brenn_provider_kit::activation::BACKLOG_NS).len(),
            1,
            "the fifth upload waits"
        );

        let answer = json!({ "id": "r1", "message": { "id": "m9", "threadId": "t1" } });
        host.activate::<GmailCompose>(
            &settings(),
            Input {
                now_ms: 2,
                results: vec![Delivered {
                    call_id: uploads[0].call_id.clone(),
                    serving: Some(uploads[0].id.clone()),
                    outcome: http(200, &answer.to_string()),
                    attachments: Vec::new(),
                }],
                requests: Vec::new(),
            },
        )
        .unwrap();
        let drained = host.take_calls();
        assert_eq!(drained.len(), 1);
        let upload = &drained[0];
        assert_eq!(upload.id, ids[BUILD_BUDGET]);
        assert_eq!(upload.call.path, DRAFTS_UPLOAD_PATH);
        let (_, body) = upload.body.clone().expect("the upload carries its body");
        let (thread, message) = uploaded(&(upload.body_content_type.clone().unwrap(), body));
        assert_eq!(thread.as_deref(), Some("t1"));
        let message = String::from_utf8(message).unwrap();
        let (head, parts) = mixed(&message);
        assert_eq!(
            head,
            "To: alice@example.com\r\n\
             Subject: Re: Lunch\r\n\
             In-Reply-To: <b@example.com>\r\n\
             References: <a@example.com> <b@example.com>\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: multipart/mixed; boundary=brenn-draft-mixed"
        );
        assert_eq!(
            parts,
            [
                (TEXT_PART.to_string(), b"Noon?".to_vec()),
                (file_part("report.pdf", "application/pdf"), pdf.2.clone()),
            ]
        );
    }

    /// `n` attachments of `size` bytes each, named `f1.bin` onward.
    fn sized(n: usize, size: u64) -> Vec<Attached> {
        (1..=n)
            .map(|i| Attached {
                name: format!("f{i}.bin"),
                size,
                content_type: "application/octet-stream".to_string(),
            })
            .collect()
    }

    #[test]
    fn a_draft_refuses_files_past_its_bounds_before_any_call() {
        let new = draft_args(json!({}));
        let mut reply = draft_args(json!({ "reply_to_id": "m0" }));
        reply.as_object_mut().unwrap().remove("subject");
        let at_bound = {
            let mut files = sized(FILES_MAX, FILES_MAX_BYTES / FILES_MAX as u64);
            files[0].size += FILES_MAX_BYTES % FILES_MAX as u64;
            files
        };
        assert_eq!(
            at_bound.iter().map(|file| file.size).sum::<u64>(),
            FILES_MAX_BYTES
        );
        let mut over = at_bound.clone();
        over[4].size += 1;
        let renamed = |name: &str| {
            let mut files = sized(1, 1);
            files[0].name = name.to_string();
            files
        };
        let retyped = |content_type: &str| {
            let mut files = sized(1, 1);
            files[0].content_type = content_type.to_string();
            files
        };
        let name = "attachment {:?}'s name must be 1 to 64 letters, digits, `.`, `_` or `-`";
        let content_type =
            "attachment `f1.bin`'s content type must be 1 to 255 printable ASCII characters";
        for args in [&new, &reply] {
            for (files, refusal) in [
                (
                    sized(FILES_MAX + 1, 1),
                    "`GmailDraft` takes at most 5 files; this call attaches 6".to_string(),
                ),
                (
                    over.clone(),
                    "the files are 10485761 bytes together; a draft takes at most 10485760"
                        .to_string(),
                ),
                (renamed(""), name.replace("{:?}", "\"\"")),
                (renamed("a b"), name.replace("{:?}", "\"a b\"")),
                (renamed("é.txt"), name.replace("{:?}", "\"é.txt\"")),
                (
                    renamed(&"n".repeat(FILE_NAME_MAX_CHARS + 1)),
                    name.replace("{:?}", &format!("{:?}", "n".repeat(65))),
                ),
                (retyped("text/plain\r\nBcc: x"), content_type.to_string()),
                (retyped(""), content_type.to_string()),
                (
                    retyped(&"t".repeat(CONTENT_TYPE_MAX_BYTES + 1)),
                    content_type.to_string(),
                ),
            ] {
                assert_eq!(
                    plan(Provided::GmailDraft, args, &files, &settings()).unwrap_err(),
                    refusal,
                    "{files:?}"
                );
            }
            assert!(plan(Provided::GmailDraft, args, &at_bound, &settings()).is_ok());
            let widest = vec![Attached {
                name: "n".repeat(FILE_NAME_MAX_CHARS),
                size: 0,
                content_type: "t".repeat(CONTENT_TYPE_MAX_BYTES),
            }];
            assert!(plan(Provided::GmailDraft, args, &widest, &settings()).is_ok());
        }
        // Refused in the activation that receives it: neither the upload nor
        // a reply's read is issued.
        let host = MemoryHost::new();
        let requests = [new, reply]
            .into_iter()
            .enumerate()
            .map(|(i, args)| Request {
                id: format!("r{i:03}"),
                tool: Provided::GmailDraft.name().to_string(),
                args,
                attachments: sized(FILES_MAX + 1, 1),
            })
            .collect();
        host.activate::<GmailCompose>(
            &settings(),
            Input {
                now_ms: 0,
                requests,
                results: Vec::new(),
            },
        )
        .unwrap();
        assert!(host.take_calls().is_empty());
        let completions = host.take_completions();
        assert_eq!(completions.len(), 2);
        for completion in completions {
            assert!(
                matches!(
                    completion.answer,
                    Answered::Error(ErrorKind::InvalidArgs, _)
                ),
                "{completion:?}"
            );
        }
    }

    /// The widest draft's upload: a reply with every argument and derived
    /// header at its bound and five files with the longest names and
    /// content types. No line but a delimiter starts with `--`, every line
    /// is under RFC 5322's 998, and what is not the files' base64 is under
    /// the 65 536 bytes the write bound's arithmetic allows a message.
    #[test]
    fn the_widest_upload_keeps_its_lines_and_its_frame_in_bounds() {
        let args = json!({
            "to": "x".repeat(HEADER_VALUE_MAX_BYTES),
            "cc": "y".repeat(HEADER_VALUE_MAX_BYTES),
            "subject": four_byte(0, SUBJECT_MAX_CHARS),
            "body": four_byte(1, BODY_MAX_BYTES / 4),
            "reply_to_id": "m".repeat(ID_MAX_CHARS),
            "account": escaped_widest(ACCOUNT_MAX_CHARS),
        });
        let message_id = format!("<{}>", "i".repeat(HEADER_VALUE_MAX_BYTES - 2));
        let mut derived = Args::new();
        derived.insert("thread_id".to_string(), json!("t".repeat(ID_MAX_CHARS)));
        derived.insert("in_reply_to".to_string(), json!(message_id));
        derived.insert("references".to_string(), json!(message_id));
        let content_type = format!(
            "application/x-{}",
            "t".repeat(CONTENT_TYPE_MAX_BYTES - "application/x-".len())
        );
        let files: Vec<File> = (0..FILES_MAX)
            .map(|i| {
                let name = format!("{i}{}", "n".repeat(FILE_NAME_MAX_CHARS - 1));
                file(&name, &content_type, bytes(1_000 + i, i))
            })
            .collect();
        let call = Call {
            step: DRAFT_CREATE_STEP,
            api: upload_call(None, true, &settings()),
        };
        let upload = built(&args, &derived, &call, &files);
        let text = String::from_utf8(upload.1.clone()).unwrap();
        for line in text.split("\r\n") {
            assert!(line.len() <= 998, "{} bytes", line.len());
            if line.starts_with("--") {
                assert!(
                    [
                        "--brenn-draft-related",
                        "--brenn-draft-related--",
                        "--brenn-draft-mixed",
                        "--brenn-draft-mixed--",
                    ]
                    .contains(&line),
                    "{line:?}"
                );
            }
        }
        let files_base64: usize = files
            .iter()
            .map(|(_, _, bytes)| compose::encoded_len(bytes.len()))
            .sum();
        let remainder = upload.1.len() - files_base64;
        assert!(remainder < 65_536, "{remainder} bytes besides the files");

        let (thread, message) = uploaded(&upload);
        assert_eq!(thread, Some("t".repeat(ID_MAX_CHARS)));
        let message = String::from_utf8(message).unwrap();
        assert_eq!(
            message_header(&message, "In-Reply-To"),
            Some(message_id.clone())
        );
        let (_, parts) = mixed(&message);
        assert_eq!(parts.len(), FILES_MAX + 1);
        for ((name, _, bytes), (headers, decoded)) in files.iter().zip(&parts[1..]) {
            assert_eq!(*headers, file_part(name, &content_type));
            assert_eq!(decoded, bytes);
        }
    }

    #[test]
    fn a_build_that_cannot_finish_fails_its_request_and_issues_nothing() {
        let pdf = file("report.pdf", "application/pdf", bytes(100, 7));
        let run = |host: &MemoryHost| {
            let attachments = vec![host.seed_request("r000", &pdf.0, &pdf.1, pdf.2.clone())];
            host.activate::<GmailCompose>(
                &settings(),
                Input {
                    now_ms: 0,
                    requests: vec![Request {
                        id: "r000".to_string(),
                        tool: Provided::GmailDraft.name().to_string(),
                        args: draft_args(json!({})),
                        attachments,
                    }],
                    results: Vec::new(),
                },
            )
            .unwrap();
            assert!(host.take_calls().is_empty(), "no upload is attempted");
            let completions = host.take_completions();
            assert_eq!(completions.len(), 1);
            match &completions[0].answer {
                Answered::Error(ErrorKind::Internal, _) => {}
                other => panic!("{other:?}"),
            }
        };
        let refused = MemoryHost::new();
        refused.refuse_next_write(WriteError::Quota);
        run(&refused);
        let ended = MemoryHost::new();
        ended.requests_not_pending(true);
        run(&ended);

        // Only the create step has a body.
        let host = MemoryHost::new();
        let state = reply_state(json!({}));
        let queued = Queued {
            id: "r000".to_string(),
            tool: Provided::GmailDraft,
            args: state.args.clone(),
            derived: Args::new(),
            call: plan(Provided::GmailDraft, &state.args, &[], &settings()).unwrap(),
            received_at_ms: 0,
            queued_at_ms: 0,
            attachments: Vec::new(),
        };
        let step = Step::planning(&host, "r000", Vec::new());
        assert_eq!(
            GmailCompose::body(&queued, &step, &settings()),
            Err(no_such_step(Provided::GmailDraft, DRAFT_ORIGINAL_STEP))
        );
        // A reply's upload whose row lacks the thread is not one this build
        // wrote.
        let queued = Queued {
            call: Call {
                step: DRAFT_CREATE_STEP,
                api: upload_call(None, true, &settings()),
            },
            ..queued
        };
        assert_eq!(
            GmailCompose::body(&queued, &step, &settings()),
            Err(internal(
                "stored GmailDraft reply has no usable `thread_id` in `derived`"
            ))
        );
    }

    #[test]
    fn a_stored_reply_that_no_longer_passes_is_internal() {
        let state = reply_state(json!({}));
        let derived: Args = json!({
            "thread_id": "t1",
            "in_reply_to": "<m0@example.com>",
            "references": "<m0@example.com>",
            "subject": "Re: Lunch",
        })
        .as_object()
        .unwrap()
        .clone();
        let row = Queued {
            id: "r000".to_string(),
            tool: Provided::GmailDraft,
            args: state.args.clone(),
            derived: derived.clone(),
            call: Call {
                step: DRAFT_CREATE_STEP,
                api: upload_call(None, true, &settings()),
            },
            received_at_ms: 0,
            queued_at_ms: 0,
            attachments: Vec::new(),
        };
        let host = MemoryHost::new();
        let step = Step::planning(&host, "r000", Vec::new());
        let build = |row: &Queued<Provided>| GmailCompose::body(row, &step, &settings());
        assert!(build(&row).is_ok(), "the row as written builds");

        let long = "a".repeat(HEADER_VALUE_MAX_BYTES + 1);
        let mut cases: Vec<(&str, Option<Value>)> = vec![("subject", None)];
        for key in ["thread_id", "subject", "in_reply_to", "references"] {
            cases.push((key, Some(json!(1))));
            cases.push((key, Some(json!("a\r\nBcc: bob@example.com"))));
        }
        for key in ["in_reply_to", "references"] {
            cases.push((key, Some(json!(long))));
        }
        for (key, value) in cases {
            let mut derived = derived.clone();
            match &value {
                Some(value) => derived.insert(key.to_string(), value.clone()),
                None => derived.remove(key),
            };
            let row = Queued {
                derived,
                ..row.clone()
            };
            assert_eq!(build(&row), Err(unwritten(key)), "{key} = {value:?}");
        }

        let mut args = row.args.clone();
        args.as_object_mut().unwrap().remove("to");
        let unparsed = Queued {
            args,
            ..row.clone()
        };
        let refiled = Queued {
            attachments: vec![Attached {
                name: "a b".to_string(),
                size: 1,
                content_type: "text/plain".to_string(),
            }],
            ..row
        };
        for (row, words) in [
            (unparsed, "arguments no longer parse: "),
            (refiled, "attachments no longer pass: "),
        ] {
            let failure = build(&row).unwrap_err();
            assert_eq!(failure.kind, ErrorKind::Internal);
            let words = format!("stored {} {words}", Provided::GmailDraft.name());
            assert!(failure.detail.starts_with(&words), "{}", failure.detail);
        }
    }
}
