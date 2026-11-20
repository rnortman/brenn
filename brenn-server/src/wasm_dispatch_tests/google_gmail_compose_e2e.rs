//! The `google-gmail-compose` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use std::sync::Arc;
use std::time::Duration;

use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_messaging::testutils::{held, owed_everywhere, publish_attached};
use brenn_messaging_store::blob::BlobHold;
use brenn_messaging_store::testutils::seal_bytes;
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use brenn_wasm_dispatch::ActivationLimits;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, APP, BEARER, Clauses, FirstRead, GMAIL_COMPOSE, PRODUCER,
    RigOptions, SRC, google_query, insufficient_scope_body, scope_denied,
};
use super::kit_body_fuel_measure::{AMPLE_FUEL, PRECISION, narrow};
use super::kit_file_e2e::BUILD_BUDGET;
use super::provider_rig::{MARGIN, fuel};
use super::stub_rig::{Pairs, StubRig, assert_could_not_be_issued, file_bytes};

const MESSAGES: &str = "/gmail/v1/users/me/messages";
const UPLOAD: &str = "/upload/gmail/v1/users/me/drafts";

/// The instance's `api-request` clauses: a reply's read of its original, and
/// the draft's upload.
const CLAUSES: Clauses<'static> = &[("GET", "/gmail/v1/users/me/messages/*"), ("POST", UPLOAD)];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&GMAIL_COMPOSE, CLAUSES).await
}

/// The rig with `clauses` and alice's one account connected.
async fn rig_with(clauses: Clauses<'_>) -> StubRig {
    workspace::rig_with(&GMAIL_COMPOSE, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// The credential cases' first call: a new draft's upload.
fn first_reads() -> [FirstRead; 1] {
    [FirstRead {
        tool: "GmailDraft",
        slug: "gmail-draft",
        args: json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" }),
        method: "POST",
        path: UPLOAD,
        body: draft_body(),
    }]
}

/// With no account connected, `api-request`'s `credential_invalid` value is
/// the tool's answer unchanged, and the component makes no further call.
#[tokio::test]
async fn no_connected_account_is_relayed_as_credential_invalid() {
    for read in first_reads() {
        workspace::assert_no_connected_account(&GMAIL_COMPOSE, CLAUSES, read).await;
    }
}

/// With two accounts connected, a call naming none is `denied` with
/// `api-request`'s list of them, and a call naming one sends that account's
/// bearer.
#[tokio::test]
async fn several_accounts_need_one_named() {
    for read in first_reads() {
        workspace::assert_several_accounts_need_one_named(&GMAIL_COMPOSE, CLAUSES, read).await;
    }
}

/// A component calling a tool on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
#[tokio::test]
async fn a_userless_caller_is_denied_with_the_reason() {
    for read in first_reads() {
        workspace::assert_a_userless_caller_is_denied_with_the_reason(
            &GMAIL_COMPOSE,
            CLAUSES,
            read,
        )
        .await;
    }
}

/// Google's draft resource for a new draft, as drafts.create answers it.
fn draft_body() -> String {
    serde_json::to_string_pretty(&json!({
        "id": "r-5521",
        "message": { "id": "m9", "threadId": "t9", "labelIds": ["DRAFT"] },
    }))
    .unwrap()
}

/// The tool's answer to [`draft_body`].
fn draft_answer() -> serde_json::Value {
    json!({ "draft_id": "r-5521", "message_id": "m9", "thread_id": "t9" })
}

/// Every upload the stub received, in order: its `Content-Type` and body.
fn uploads(r: &StubRig) -> Vec<(String, Vec<u8>)> {
    let stub = r.stub.lock().unwrap();
    stub.api_seen
        .iter()
        .filter(|seen| !seen.body.is_empty())
        .map(|seen| {
            let content_type = seen
                .headers
                .get("content-type")
                .expect("the upload names its type")
                .to_str()
                .unwrap()
                .to_string();
            (content_type, seen.body.clone())
        })
        .collect()
}

/// The message `upload` carries, read as Gmail reads it: a new draft's
/// upload is `message/rfc822`, the message alone; a reply's is
/// `multipart/related`, exactly the metadata naming `thread` and the
/// message.
fn message_in((content_type, body): &(String, Vec<u8>), thread: Option<&str>) -> String {
    let body = std::str::from_utf8(body).expect("a message is ASCII");
    let message = match thread {
        None => {
            assert_eq!(content_type, "message/rfc822");
            body
        }
        Some(thread) => {
            assert_eq!(
                content_type,
                "multipart/related; boundary=brenn-draft-related"
            );
            let rest = body
                .strip_prefix(
                    "--brenn-draft-related\r\n\
                     Content-Type: application/json; charset=UTF-8\r\n\r\n",
                )
                .expect("the metadata part opens the upload");
            let (metadata, rest) = rest
                .split_once("\r\n--brenn-draft-related\r\nContent-Type: message/rfc822\r\n\r\n")
                .expect("the message part follows the metadata");
            let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
            assert_eq!(
                metadata,
                json!({ "message": { "threadId": thread } }),
                "a reply names its thread"
            );
            rest.strip_suffix("\r\n--brenn-draft-related--\r\n")
                .expect("the closing delimiter ends the upload")
        }
    };
    message.to_string()
}

/// An entity's headers, unfolded and in order, and what follows the empty
/// line after them. No line is over RFC 5322's 998 bytes.
fn split_entity(entity: &str) -> (Vec<(String, String)>, &str) {
    let (head, rest) = entity.split_once("\r\n\r\n").expect("a header block");
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in head.split("\r\n") {
        assert!(line.len() <= 998, "a header line over 998: {line:?}");
        match line.strip_prefix(' ') {
            Some(continued) => {
                let (_, value) = headers.last_mut().expect("a folded line continues one");
                value.push(' ');
                value.push_str(continued);
            }
            None => {
                let (name, value) = line.split_once(": ").expect("a header line");
                headers.push((name.to_string(), value.to_string()));
            }
        }
    }
    (headers, rest)
}

/// Base64 lines of at most 76 characters, each ending CRLF, decoded.
fn decoded_lines(lines: &str) -> Vec<u8> {
    use base64ct::{Base64, Encoding as _};

    let mut joined = String::with_capacity(lines.len());
    for line in lines.split_terminator("\r\n") {
        assert!(line.len() <= 76, "a base64 line over 76: {line:?}");
        joined.push_str(line);
    }
    assert!(
        lines.is_empty() || lines.ends_with("\r\n"),
        "the last line ends CRLF"
    );
    Base64::decode_vec(&joined).unwrap()
}

/// The one upload's message: its headers, every header line within 76
/// characters, and its single-part body, base64 decoded.
fn uploaded(r: &StubRig, thread: Option<&str>) -> (Vec<(String, String)>, String) {
    let uploads = uploads(r);
    assert_eq!(uploads.len(), 1, "exactly one upload carried a body");
    let message = message_in(&uploads[0], thread);
    let (head, _) = message.split_once("\r\n\r\n").expect("a header block");
    for line in head.split("\r\n") {
        assert!(line.len() <= 76, "a header line over 76: {line:?}");
    }
    let (headers, body) = split_entity(&message);
    (headers, String::from_utf8(decoded_lines(body)).unwrap())
}

/// The text of a header written as RFC 2047 `B` encoded-words in UTF-8.
fn decoded_words(value: &str) -> String {
    use base64ct::{Base64, Encoding as _};

    let mut bytes = Vec::new();
    for word in value.split(' ') {
        let encoded = word
            .strip_prefix("=?utf-8?B?")
            .and_then(|word| word.strip_suffix("?="))
            .unwrap_or_else(|| panic!("not an encoded-word: {word:?}"));
        bytes.extend(Base64::decode_vec(encoded).unwrap());
    }
    String::from_utf8(bytes).unwrap()
}

/// The headers every draft carries after its addressing and subject.
fn mime_headers() -> [(String, String); 3] {
    [
        ("MIME-Version", "1.0"),
        ("Content-Type", "text/plain; charset=utf-8"),
        ("Content-Transfer-Encoding", "base64"),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()))
}

/// `GmailDraft` creates a draft with one media upload to drafts.create
/// carrying alice's bearer and the message: the given recipients and
/// subject, no `From` or `Date`, and the body with CRLF line breaks. The
/// answer names the draft, its message and its thread.
#[tokio::test]
async fn gmail_draft_uploads_one_message_with_the_users_bearer() {
    let r = rig().await;
    r.answer(UPLOAD, draft_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({
            "to": "Bob Example <bob@example.com>, carol@example.com",
            "cc": "dave@example.com",
            "subject": "Lunch on Friday",
            "body": "Hi Bob,\nNoon at the usual place?\n",
        }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), Ok(draft_answer()));
    assert_eq!(
        r.seen(),
        [(
            "POST".to_string(),
            UPLOAD.to_string(),
            google_query(&[("uploadType", "media")])
        )]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    let (headers, body) = uploaded(&r, None);
    let expected: Vec<(String, String)> = [
        ("To", "Bob Example <bob@example.com>, carol@example.com"),
        ("Cc", "dave@example.com"),
        ("Subject", "Lunch on Friday"),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .into_iter()
    .chain(mime_headers())
    .collect();
    assert_eq!(headers, expected);
    assert_eq!(body, "Hi Bob,\r\nNoon at the usual place?\r\n");
    r.assert_finished(&id);
}

/// A subject outside ASCII is written as encoded-words that decode back to
/// it, and a body outside ASCII travels as base64 of its UTF-8.
#[tokio::test]
async fn gmail_draft_encodes_a_non_ascii_subject_and_body() {
    let subject = "Grüße aus Köln — 東京の会議について、来週の予定を確認させてください";
    let r = rig().await;
    r.answer(UPLOAD, draft_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({
            "to": "bob@example.com",
            "subject": subject,
            "body": "Ça va ?\r\n日本語の本文。\n",
        }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), Ok(draft_answer()));
    let (headers, body) = uploaded(&r, None);
    assert_eq!(headers.len(), 5, "{headers:?}");
    assert_eq!(
        headers[0],
        ("To".to_string(), "bob@example.com".to_string())
    );
    let (name, value) = &headers[1];
    assert_eq!(name, "Subject");
    assert!(value.contains(' '), "a long subject spans words: {value}");
    assert_eq!(decoded_words(value), subject);
    assert_eq!(headers[2..], mime_headers());
    assert_eq!(body, "Ça va ?\r\n日本語の本文。\r\n");
    r.assert_finished(&id);
}

/// A line break in `to`, `cc` or `subject` is refused `invalid_args` naming
/// the argument, before any call is issued.
#[tokio::test]
async fn gmail_draft_refuses_a_header_with_a_line_break_before_any_call() {
    let ascii = "must be 1 to 980 printable ASCII characters (a display name in another script \
                 is left out or written in ASCII)";
    let cases = [
        (
            "to",
            "bob@example.com\r\nBcc: eve@example.com",
            format!("`to` {ascii}"),
        ),
        (
            "cc",
            "carol@example.com\nBcc: eve@example.com",
            format!("`cc` {ascii}"),
        ),
        (
            "subject",
            "Lunch\r\nBcc: eve@example.com",
            "`subject` holds a control character".to_string(),
        ),
        (
            "subject",
            "Lunch\rBcc: eve@example.com",
            "`subject` holds a control character".to_string(),
        ),
    ];
    for (key, value, refusal) in cases {
        let r = rig().await;
        let mut args = json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" });
        args[key] = json!(value);
        let call = r.call_for_alice("GmailDraft", args);
        let id = r.requested_id().await;
        r.serve().await;

        assert_eq!(
            call.await.unwrap(),
            Err(ToolError::InvalidArgs(refusal)),
            "{key}: {value:?}"
        );
        r.owed(&r.executor_sub(), 0).await;
        assert!(r.seen().is_empty(), "{key}: a refused draft reaches no API");
        r.assert_finished(&id);
    }
}

/// Google's 403 on the draft's upload is `denied`, telling the assistant to
/// reconnect with the Gmail compose scope.
#[tokio::test]
async fn a_draft_403_names_the_compose_scope() {
    let r = rig().await;
    r.respond(UPLOAD, 403, insufficient_scope_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail compose"));
    r.assert_finished(&id);
}

/// An instance that kept the JSON drafts clause in place of the upload's
/// has the draft's upload refused when it is queued, even though the reply
/// read's `GET` clause stands: the request is answered `internal` with
/// Brenn's text and the clause guidance.
#[tokio::test]
async fn a_draft_post_without_its_clause_could_not_be_issued() {
    let clauses = [
        ("GET", "/gmail/v1/users/me/messages/*"),
        ("POST", "/gmail/v1/users/me/drafts"),
    ];
    let r = rig_with(&clauses).await;
    let call = r.call_for_alice(
        "GmailDraft",
        json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" }),
    );
    let id = r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);
}

/// messages.get's metadata query for a reply's original: its thread and the
/// headers the reply derives its own from.
fn original_query() -> Pairs {
    google_query(&[
        ("format", "metadata"),
        ("fields", "id,threadId,payload/headers"),
        ("metadataHeaders", "Subject"),
        ("metadataHeaders", "Message-ID"),
        ("metadataHeaders", "References"),
    ])
}

/// Google's metadata answer for the original `m1` in thread `t9`, the
/// thread `draft_body` files the draft in, carrying `headers` as name and
/// value pairs.
fn original_body(headers: &[(&str, &str)]) -> String {
    let headers: Vec<serde_json::Value> = headers
        .iter()
        .map(|(name, value)| json!({ "name": name, "value": value }))
        .collect();
    serde_json::to_string_pretty(&json!({
        "id": "m1",
        "threadId": "t9",
        "payload": { "headers": headers },
    }))
    .unwrap()
}

/// A reply with no `subject` reads its original's metadata, then uploads
/// one draft whose metadata names the original's thread, its message
/// carrying `In-Reply-To`, `References` (the original's followed by its
/// `Message-ID`) and the original's subject after `Re: `. The answer names
/// the `Message-ID` answered and the derived subject.
#[tokio::test]
async fn gmail_draft_replies_in_the_originals_thread() {
    let r = rig().await;
    r.answer(
        &format!("{MESSAGES}/m1"),
        original_body(&[
            ("Subject", "Lunch on Friday"),
            ("Message-ID", "<c@mail.example.com>"),
            ("References", "<a@mail.example.com> <b@mail.example.com>"),
        ]),
    );
    r.answer(UPLOAD, draft_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({ "to": "bob@example.com", "body": "Noon works.", "reply_to_id": "m1" }),
    );
    let id = r.requested_id().await;
    r.run(2).await;

    let mut answer = draft_answer();
    answer["in_reply_to"] = json!("<c@mail.example.com>");
    answer["subject"] = json!("Re: Lunch on Friday");
    assert_eq!(call.await.unwrap(), Ok(answer));
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                format!("{MESSAGES}/m1"),
                original_query()
            ),
            (
                "POST".to_string(),
                UPLOAD.to_string(),
                google_query(&[("uploadType", "multipart")])
            ),
        ]
    );
    assert_eq!(
        r.bearers(),
        [format!("Bearer {BEARER}"), format!("Bearer {BEARER}")]
    );
    let (headers, body) = uploaded(&r, Some("t9"));
    let expected: Vec<(String, String)> = [
        ("To", "bob@example.com"),
        ("Subject", "Re: Lunch on Friday"),
        ("In-Reply-To", "<c@mail.example.com>"),
        (
            "References",
            "<a@mail.example.com> <b@mail.example.com> <c@mail.example.com>",
        ),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .into_iter()
    .chain(mime_headers())
    .collect();
    assert_eq!(headers, expected);
    assert_eq!(body, "Noon works.");
    r.assert_finished(&id);
}

/// A reply to an original without a `Message-ID` uploads with the
/// original's thread and no `In-Reply-To` or `References`; the answer names
/// no `in_reply_to`, and a caller's `subject` is used as given.
#[tokio::test]
async fn gmail_draft_replies_to_an_original_without_a_message_id() {
    let r = rig().await;
    r.answer(
        &format!("{MESSAGES}/m1"),
        original_body(&[("Subject", "Lunch on Friday")]),
    );
    r.answer(UPLOAD, draft_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({
            "to": "bob@example.com",
            "subject": "Re: Lunch",
            "body": "Noon works.",
            "reply_to_id": "m1",
        }),
    );
    let id = r.requested_id().await;
    r.run(2).await;

    assert_eq!(call.await.unwrap(), Ok(draft_answer()));
    let (headers, _) = uploaded(&r, Some("t9"));
    let expected: Vec<(String, String)> = [("To", "bob@example.com"), ("Subject", "Re: Lunch")]
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .into_iter()
        .chain(mime_headers())
        .collect();
    assert_eq!(headers, expected);
    r.assert_finished(&id);
}

/// Google's 403 on a reply's read of its original is `denied` naming the
/// Gmail read scope, which that read needs, and no draft is uploaded.
#[tokio::test]
async fn a_reply_read_403_names_the_read_scope() {
    let r = rig().await;
    r.respond(&format!("{MESSAGES}/m1"), 403, insufficient_scope_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({ "to": "bob@example.com", "body": "Noon works.", "reply_to_id": "m1" }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail read"));
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            format!("{MESSAGES}/m1"),
            original_query()
        )]
    );
    r.assert_finished(&id);
}

/// The most files a draft carries.
const FILES_MAX: usize = 5;
/// The most bytes a draft's files hold together.
const FILES_MAX_BYTES: usize = 10 * 1024 * 1024;
/// The most bytes a draft's body holds in its CRLF form.
const BODY_MAX_BYTES: usize = 20_000;
/// What the widest message holds besides its files' base64, at most: the
/// remainder the component's unit test holds the widest upload under.
const FRAME_MAX_BYTES: u64 = 65_536;

/// A file a draft is handed: the name it is attached under, its content
/// type and its bytes.
struct File {
    name: String,
    content_type: String,
    bytes: Vec<u8>,
}

fn file(name: &str, content_type: &str, bytes: Vec<u8>) -> File {
    File {
        name: name.to_string(),
        content_type: content_type.to_string(),
        bytes,
    }
}

/// `files` sealed in the rig's blob store, each under its content type.
fn sealed(r: &StubRig, files: &[File]) -> Vec<(String, BlobHold)> {
    let store = r.messenger.blob_store().expect("a blob store");
    files
        .iter()
        .map(|f| (f.name.clone(), seal_bytes(store, &f.content_type, &f.bytes)))
        .collect()
}

/// `files` sealed and handed to a call under their names.
fn held_files(r: &StubRig, files: &[File]) -> HeldAttachments {
    let holds = sealed(r, files);
    let named: Vec<(&str, &BlobHold)> = holds.iter().map(|(n, h)| (n.as_str(), h)).collect();
    held(&named)
}

/// A file part's headers for a file named `name` of `content_type`.
fn file_headers(name: &str, content_type: &str) -> Vec<(String, String)> {
    [
        ("Content-Type", content_type.to_string()),
        (
            "Content-Disposition",
            format!("attachment; filename=\"{name}\""),
        ),
        ("Content-Transfer-Encoding", "base64".to_string()),
    ]
    .map(|(name, value)| (name.to_string(), value))
    .to_vec()
}

/// A part of a `multipart/mixed` message as read: its headers and its bytes,
/// base64 decoded.
type Part = (Vec<(String, String)>, Vec<u8>);

/// A `multipart/mixed` message as read: its headers, its text, and the
/// parts after the text in order.
fn mixed(message: &str) -> (Vec<(String, String)>, String, Vec<Part>) {
    let (headers, rest) = split_entity(message);
    let rest = rest
        .strip_prefix("--brenn-draft-mixed\r\n")
        .expect("the text part opens the body");
    let rest = rest
        .strip_suffix("--brenn-draft-mixed--\r\n")
        .expect("the closing delimiter ends the message");
    let mut parts = rest.split("--brenn-draft-mixed\r\n").map(|part| {
        let (headers, content) = split_entity(part);
        (headers, decoded_lines(content))
    });
    let (text_headers, text) = parts.next().expect("the text part");
    assert_eq!(text_headers, mime_headers()[1..]);
    (headers, String::from_utf8(text).unwrap(), parts.collect())
}

/// `message` is `multipart/mixed`, its text `body` and then a part for each
/// of `files` in order, each under its name and type and decoding to its
/// bytes.
fn assert_carries(message: &str, body: &str, files: &[File]) {
    let (headers, text, parts) = mixed(message);
    assert_eq!(
        headers[headers.len() - 2..],
        [
            ("MIME-Version", "1.0"),
            (
                "Content-Type",
                "multipart/mixed; boundary=brenn-draft-mixed"
            ),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
    );
    assert_eq!(text, body);
    assert_eq!(parts.len(), files.len(), "a part for each file");
    for (f, (headers, bytes)) in files.iter().zip(&parts) {
        assert_eq!(*headers, file_headers(&f.name, &f.content_type));
        assert!(
            *bytes == f.bytes,
            "{}: the part decodes to the file",
            f.name
        );
    }
}

/// The rig with every clause, alice's one account connected, and app
/// [`APP`]'s conversations.
async fn conversation_rig() -> StubRig {
    workspace::rig_built(
        &GMAIL_COMPOSE,
        CLAUSES,
        RigOptions {
            conversations: true,
            ..RigOptions::default()
        },
    )
    .await
}

/// The files the conversation tests attach, in the order the call names
/// them.
fn two_files() -> [File; 2] {
    [
        file(
            "agenda.txt",
            "text/plain; charset=utf-8",
            b"1. Lunch\r\n2. Budget\r\n".to_vec(),
        ),
        file("report.pdf", "application/pdf", file_bytes(300_000)),
    ]
}

/// Alice's conversation calls `GmailDraft` with `args`, attaching `files`:
/// a producer publishes them on [`SRC`] as one message, the conversation
/// reads it, and the call's `attachments` argument names each file of that
/// message under its own name. The rig runs the request through `steps`
/// nested calls; the call's answer, once the request is finished.
async fn draft_from_a_conversation(
    r: &StubRig,
    mut args: Value,
    files: &[File],
    steps: usize,
) -> Value {
    let conversation = r.conversation(APP, "alice").await;
    let holds = sealed(r, files);
    let named: Vec<(&str, &BlobHold)> = holds.iter().map(|(n, h)| (n.as_str(), h)).collect();
    let shown = publish_attached(
        &r.messenger,
        PRODUCER,
        SRC,
        "files for a draft",
        &named,
        None,
    )
    .await;
    drop(holds);
    conversation
        .test_post_tool_use("MessageChannelGet", json!({ "address": SRC, "limit": 50 }))
        .await;
    args["attachments"] = files
        .iter()
        .map(|f| {
            (
                f.name.clone(),
                json!({ "message_id": shown.to_string(), "name": f.name }),
            )
        })
        .collect::<serde_json::Map<String, Value>>()
        .into();
    let call = {
        let conversation = conversation.clone();
        tokio::spawn(async move { conversation.test_post_tool_use("GmailDraft", args).await })
    };
    let id = r.requested_id().await;
    r.run(steps).await;
    let answer = call.await.unwrap();
    r.assert_finished(&id);
    answer
}

/// A conversation's `GmailDraft` naming two files it was shown uploads one
/// message: the text first, then each file as a part under the name the
/// call gave it and the type it was sealed with.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversations_draft_uploads_its_files_as_parts() {
    let r = conversation_rig().await;
    r.answer(UPLOAD, draft_body());
    let files = two_files();
    let answer = draft_from_a_conversation(
        &r,
        json!({ "to": "bob@example.com", "subject": "Notes", "body": "Both attached.\n" }),
        &files,
        1,
    )
    .await;

    assert_eq!(answer, draft_answer());
    assert_eq!(
        r.seen(),
        [(
            "POST".to_string(),
            UPLOAD.to_string(),
            google_query(&[("uploadType", "media")])
        )]
    );
    let uploads = uploads(&r);
    assert_eq!(uploads.len(), 1, "one upload");
    let message = message_in(&uploads[0], None);
    let (headers, _) = split_entity(&message);
    assert_eq!(
        headers[..2],
        [("To", "bob@example.com"), ("Subject", "Notes")]
            .map(|(name, value)| (name.to_string(), value.to_string()))
    );
    assert_carries(&message, "Both attached.\r\n", &files);
}

/// A conversation's reply naming two files reads its original, then
/// uploads one `multipart` draft naming the original's thread, the message
/// inside it carrying the threading headers, the text and the files.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversations_reply_uploads_its_files_in_the_thread() {
    let r = conversation_rig().await;
    r.answer(
        &format!("{MESSAGES}/m1"),
        original_body(&[
            ("Subject", "Lunch on Friday"),
            ("Message-ID", "<c@mail.example.com>"),
        ]),
    );
    r.answer(UPLOAD, draft_body());
    let files = two_files();
    let answer = draft_from_a_conversation(
        &r,
        json!({ "to": "bob@example.com", "body": "Both attached.", "reply_to_id": "m1" }),
        &files,
        2,
    )
    .await;

    let mut expected = draft_answer();
    expected["in_reply_to"] = json!("<c@mail.example.com>");
    expected["subject"] = json!("Re: Lunch on Friday");
    assert_eq!(answer, expected);
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                format!("{MESSAGES}/m1"),
                original_query()
            ),
            (
                "POST".to_string(),
                UPLOAD.to_string(),
                google_query(&[("uploadType", "multipart")])
            ),
        ]
    );
    let uploads = uploads(&r);
    assert_eq!(uploads.len(), 1, "one upload");
    let message = message_in(&uploads[0], Some("t9"));
    let (headers, _) = split_entity(&message);
    assert_eq!(
        headers[..4],
        [
            ("To", "bob@example.com"),
            ("Subject", "Re: Lunch on Friday"),
            ("In-Reply-To", "<c@mail.example.com>"),
            ("References", "<c@mail.example.com>"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()))
    );
    assert_carries(&message, "Both attached.", &files);
}

/// Six files, or files one byte over the bound together, are refused
/// `invalid_args` naming the bound before any call: for a reply, before its
/// original is read.
#[tokio::test(flavor = "multi_thread")]
async fn a_draft_past_its_file_bounds_is_refused_before_any_call() {
    let six: Vec<File> = (0..=FILES_MAX)
        .map(|i| file(&format!("f{i}.txt"), "text/plain", vec![b'x'; 10]))
        .collect();
    let over = vec![
        file("a.bin", "application/pdf", file_bytes(FILES_MAX_BYTES / 2)),
        file(
            "b.bin",
            "application/pdf",
            file_bytes(FILES_MAX_BYTES / 2 + 1),
        ),
    ];
    let cases = [
        (
            six,
            "`GmailDraft` takes at most 5 files; this call attaches 6".to_string(),
        ),
        (
            over,
            format!(
                "the files are {} bytes together; a draft takes at most {FILES_MAX_BYTES}",
                FILES_MAX_BYTES + 1
            ),
        ),
    ];
    for (files, refusal) in &cases {
        for reply in [false, true] {
            let r = rig().await;
            let mut args =
                json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" });
            if reply {
                args["reply_to_id"] = json!("m1");
            }
            let call = r.call_for_alice_with("GmailDraft", args, held_files(&r, files));
            let id = r.requested_id().await;
            r.serve().await;

            assert_eq!(
                call.await.unwrap().err(),
                Some(ToolError::InvalidArgs(refusal.clone())),
                "reply: {reply}"
            );
            r.owed(&r.executor_sub(), 0).await;
            assert!(r.seen().is_empty(), "a refused draft reaches no API");
            r.assert_finished(&id);
        }
    }
}

/// What fills a measured draft's body: one unit repeated, after `lead`
/// where the unit alone would be refused as blank, and how many units fill
/// it to [`BODY_MAX_BYTES`] in its CRLF form.
#[derive(Clone, Copy, Debug)]
struct Content {
    name: &'static str,
    lead: &'static str,
    unit: &'static str,
    at_bound: usize,
}

impl Content {
    /// A body of at most `bytes` in its CRLF form.
    fn body(self, bytes: usize) -> String {
        let mut body = self.lead.to_string();
        body.push_str(&self.unit.repeat(self.at_bound * bytes / BODY_MAX_BYTES));
        body
    }
}

const ASCII: Content = Content {
    name: "ASCII",
    lead: "",
    unit: "a",
    at_bound: 20_000,
};
const LINE_FEEDS: Content = Content {
    name: "line feeds",
    lead: "a",
    unit: "\n",
    at_bound: 9_999,
};
const CONTROL: Content = Content {
    name: "U+0001",
    lead: "",
    unit: "\u{1}",
    at_bound: 20_000,
};
const QUOTES: Content = Content {
    name: "quotes",
    lead: "",
    unit: "\"",
    at_bound: 20_000,
};
const BACKSLASHES: Content = Content {
    name: "backslashes",
    lead: "",
    unit: "\\",
    at_bound: 20_000,
};
const FOUR_BYTE: Content = Content {
    name: "U+1D11E",
    lead: "",
    unit: "\u{1d11e}",
    at_bound: 5_000,
};

/// The bodies a build is measured with.
const BUILD_CONTENTS: [Content; 5] = [ASCII, LINE_FEEDS, CONTROL, QUOTES, FOUR_BYTE];
/// The bodies a late answer's settle is measured with.
const LATE_CONTENTS: [Content; 5] = [CONTROL, QUOTES, BACKSLASHES, FOUR_BYTE, LINE_FEEDS];

/// The bus cap the bound's rigs run with, the deployer guide's figure: a
/// body of [`CONTROL`] at its bound is admitted only above the default.
const BUS_CAP: usize = 393_216;

/// The widest reply's thread: a Gmail id at its 128 characters.
fn wide_thread() -> String {
    "t".repeat(128)
}

/// The widest draft's arguments: `to` and `cc` at 980 characters, a
/// subject of 500 four-byte characters, a body of `content` at
/// `body_bytes`, alice's account named, and for a reply the original
/// [`widest_original`] answers. The account is the rig's, not one at the
/// argument's 320-character cap: the rig's credential vault stores an
/// account as a file named for it, which the file system holds to 255
/// bytes (`vault-account-file-name-too-long`).
fn widest_args(content: Content, body_bytes: usize, reply: bool) -> Value {
    let mut args = json!({
        "to": "x".repeat(980),
        "cc": "y".repeat(980),
        "subject": "\u{1f600}".repeat(500),
        "body": content.body(body_bytes),
        "account": ACCOUNT,
    });
    if reply {
        args["reply_to_id"] = json!("m1");
    }
    args
}

/// The original the widest reply answers: in [`wide_thread`], its
/// `Message-ID` 980 bytes, which is then the whole of the reply's
/// `References` too.
fn widest_original() -> String {
    serde_json::to_string(&json!({
        "id": "m1",
        "threadId": wide_thread(),
        "payload": { "headers": [
            { "name": "Subject", "value": "Lunch" },
            { "name": "Message-ID", "value": format!("<{}>", "i".repeat(978)) },
        ] },
    }))
    .unwrap()
}

/// How files of [`FILES_MAX_BYTES`] together are split.
#[derive(Clone, Copy, Debug)]
enum Split {
    One,
    FiveEqual,
    FourBytesAndTheRest,
}

/// Files of [`FILES_MAX_BYTES`] together, split as `split`, named for
/// request `request`.
fn bound_files(split: Split, request: usize) -> Vec<File> {
    let sizes = match split {
        Split::One => vec![FILES_MAX_BYTES],
        Split::FiveEqual => vec![FILES_MAX_BYTES / FILES_MAX; FILES_MAX],
        Split::FourBytesAndTheRest => vec![1, 1, 1, 1, FILES_MAX_BYTES - 4],
    };
    assert_eq!(sizes.iter().sum::<usize>(), FILES_MAX_BYTES);
    sizes
        .into_iter()
        .enumerate()
        .map(|(i, size)| {
            file(
                &format!("r{request}-{i}.pdf"),
                "application/pdf",
                file_bytes(size),
            )
        })
        .collect()
}

/// A rig at the bound: alice's one account connected, the bus cap at
/// [`BUS_CAP`], and the stub answering the widest reply's original and
/// every upload.
async fn bound_rig() -> StubRig {
    let r = workspace::rig_built(
        &GMAIL_COMPOSE,
        CLAUSES,
        RigOptions {
            max_body_bytes: Some(BUS_CAP),
            ..RigOptions::default()
        },
    )
    .await;
    r.answer(&format!("{MESSAGES}/m1"), widest_original());
    r.answer(UPLOAD, draft_body());
    r
}

/// Bring a [`bound_rig`] to the activation that builds the widest drafts,
/// one a set of `files`, their bodies of `content` at their bound: for new
/// drafts the activation handed the requests, for replies the one handed
/// their originals' answers. The rig and the calls.
async fn to_the_build(
    reply: bool,
    content: Content,
    files: &[Vec<File>],
) -> (StubRig, Vec<JoinHandle<Result<ToolOutput, ToolError>>>) {
    let r = bound_rig().await;
    let args = widest_args(content, BODY_MAX_BYTES, reply);
    let calls = files
        .iter()
        .map(|files| r.call_for_alice_with("GmailDraft", args.clone(), held_files(&r, files)))
        .collect();
    r.owed(&r.provider_sub, files.len()).await;
    if reply {
        r.serve().await;
        r.run_nested_calls(files.len()).await;
    }
    (r, calls)
}

/// Whether the provider has issued `n` uploads: the build activation
/// completed.
async fn built(r: &StubRig, n: usize) -> bool {
    owed_everywhere(&r.messenger, &r.executor_sub()).await.len() == n
}

/// The production grant of the activation `r`'s provider is owed next, by
/// the host's fuel rule over every envelope it is handed, the new bodies and
/// the pending requests' not handed new, and the attachments of both: the
/// component is granted `blobs`.
async fn production_grant(r: &StubRig) -> u64 {
    let ports = r.owed_windows().await;
    let envelopes: usize = ports.iter().map(|port| port.entries.len()).sum();
    let mut body_bytes = 0u64;
    let mut attachment_bytes = 0u64;
    let mut new_ids = Vec::new();
    for port in &ports {
        for (_, envelope) in &port.entries[port.new_from..] {
            body_bytes += envelope.body.len() as u64;
            attachment_bytes += envelope
                .attachments
                .iter()
                .map(|(_, blob)| blob.size)
                .sum::<u64>();
            if port.port == TOOL_REQUEST_INPUT_PORT {
                let request: Value =
                    serde_json::from_str(envelope.body.as_text().unwrap()).unwrap();
                new_ids.push(request["id"].as_str().unwrap().to_string());
            }
        }
    }
    let new_ids: Vec<&str> = new_ids.iter().map(String::as_str).collect();
    let pending = r.runtime.pending_load(&r.provider.slug, &new_ids);
    attachment_bytes += pending
        .attachments
        .iter()
        .map(|blob| blob.size)
        .sum::<u64>();
    brenn_wasm::processor_fuel(
        envelopes,
        body_bytes,
        pending.body_bytes,
        Some(attachment_bytes),
    )
}

/// The text a body of `content` at its bound decodes to: CRLF line breaks.
fn sent_body(content: Content) -> String {
    content.body(BODY_MAX_BYTES).replace('\n', "\r\n")
}

/// The widest drafts at their bound, one a set of `files`, their bodies of
/// `content`, are built inside the margin and uploaded: each upload is the
/// message its arguments and files describe. The uploads.
async fn assert_built_inside_the_margin(
    reply: bool,
    content: Content,
    files: &[Vec<File>],
) -> Vec<(String, Vec<u8>)> {
    let n = files.len();
    let (r, calls) = to_the_build(reply, content, files).await;
    r.serve_under(Some(MARGIN)).await;
    assert!(
        built(&r, n).await,
        "every upload was built inside the margin"
    );
    r.run_nested_calls(n).await;
    r.serve().await;
    for call in calls {
        let answer = call.await.unwrap();
        assert!(answer.is_ok(), "{:?}", answer.err());
    }
    let uploads = uploads(&r);
    assert_eq!(uploads.len(), n, "every draft uploaded");
    let thread = reply.then(wide_thread);
    let body = sent_body(content);
    let mut carried = vec![false; n];
    for upload in &uploads {
        let message = message_in(upload, thread.as_deref());
        if files.iter().all(Vec::is_empty) {
            let (_, text) = split_entity(&message);
            assert!(String::from_utf8(decoded_lines(text)).unwrap() == body);
            continue;
        }
        let (_, _, parts) = mixed(&message);
        let first = &parts[0].0[1].1;
        let at = files
            .iter()
            .position(|files| first.contains(&format!("\"{}\"", files[0].name)))
            .expect("the upload carries one request's files");
        assert_carries(&message, &body, &files[at]);
        carried[at] = true;
    }
    if !files.iter().all(Vec::is_empty) {
        assert!(carried.iter().all(|c| *c), "each request's files uploaded");
    }
    uploads
}

/// The split of the files the build costs most on, as a share of its
/// production grant, by `measure_draft_builds`; the splits differ by under
/// 0.1%.
const COSTLIEST_SPLIT: Split = Split::FourBytesAndTheRest;
/// The body the build costs most on, as a share of its production grant,
/// by `measure_draft_builds`.
const COSTLIEST_BUILD: Content = QUOTES;

/// Two widest new drafts, each with files at the bound, arriving in one
/// activation are both built and uploaded inside the margin. Measured by
/// `measure_draft_builds`, the activation completes on about 602.5M of a
/// 2,756.9M production grant, 0.219 of it, the file bytes costing about 28
/// fuel each. A failure here is answered by measuring again and correcting
/// this figure or the component's bounds, never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn two_new_drafts_at_the_file_bound_upload_inside_the_margin() {
    let files: Vec<_> = (0..2).map(|i| bound_files(COSTLIEST_SPLIT, i)).collect();
    assert_built_inside_the_margin(false, COSTLIEST_BUILD, &files).await;
}

/// Two widest replies, each with files at the bound, their originals
/// answered into one activation, are both built and uploaded inside the
/// margin; and the largest upload, with the whole frame the widest message
/// may hold added, written [`BUILD_BUDGET`] times stays inside the host's
/// write bound on one activation. Measured, the activation completes on
/// about 606.3M of a 2,757.7M production grant, 0.220 of it.
#[tokio::test(flavor = "multi_thread")]
async fn two_replies_at_the_file_bound_upload_inside_the_margin() {
    let files: Vec<_> = (0..2).map(|i| bound_files(COSTLIEST_SPLIT, i)).collect();
    let uploads = assert_built_inside_the_margin(true, COSTLIEST_BUILD, &files).await;
    let largest = uploads
        .iter()
        .map(|(_, body)| body.len() as u64)
        .max()
        .unwrap();
    assert!(
        (largest + FRAME_MAX_BYTES) * BUILD_BUDGET as u64
            <= brenn_wasm::PROCESSOR_MAX_BLOB_BYTES_WRITTEN_PER_ACTIVATION,
        "{largest} bytes"
    );
}

/// Two widest new drafts without files, their bodies at the bound, are
/// built and uploaded inside the margin. Measured, the activation completes
/// on about 17.5M of a 72.6M production grant, 0.241 of it.
#[tokio::test(flavor = "multi_thread")]
async fn two_new_drafts_without_files_upload_inside_the_margin() {
    assert_built_inside_the_margin(false, COSTLIEST_BUILD, &[vec![], vec![]]).await;
}

/// Two widest replies without files, their bodies at the bound, their
/// originals answered into one activation, are built and uploaded inside
/// the margin. Measured, the activation completes on about 21.1M of a 73.4M
/// production grant, 0.288 of it: the costliest share of any build.
#[tokio::test(flavor = "multi_thread")]
async fn two_replies_without_files_upload_inside_the_margin() {
    assert_built_inside_the_margin(true, COSTLIEST_BUILD, &[vec![], vec![]]).await;
}

/// The measured cells run this many at a time.
const MEASURED_AT_ONCE: usize = 6;

/// One build measured: new drafts or replies, how many in the activation,
/// the split of their files or none, and their bodies.
#[derive(Clone, Copy, Debug)]
struct Cell {
    reply: bool,
    n: usize,
    split: Option<Split>,
    content: Content,
}

impl Cell {
    fn label(self) -> String {
        format!(
            "{} x{}, files {}, body {}",
            if self.reply { "reply" } else { "new" },
            self.n,
            self.split.map_or("none".to_string(), |s| format!("{s:?}")),
            self.content.name
        )
    }
}

/// `cell`'s build activation run under `limits` of its production grant:
/// whether it completed, the production grant, and, for new drafts that
/// did not, the reason the host gave for failing them.
async fn build_run(
    cell: Cell,
    limits: impl FnOnce(u64) -> Option<ActivationLimits>,
) -> (bool, u64, Option<String>) {
    let files: Vec<Vec<File>> = (0..cell.n)
        .map(|i| {
            cell.split
                .map_or_else(Vec::new, |split| bound_files(split, i))
        })
        .collect();
    let (r, calls) = to_the_build(cell.reply, cell.content, &files).await;
    let grant = production_grant(&r).await;
    r.serve_under(limits(grant)).await;
    let completed = built(&r, cell.n).await;
    let mut reason = None;
    for call in calls {
        if !completed && !cell.reply && reason.is_none() {
            if let Ok(Ok(Err(ToolError::Internal(detail)))) =
                tokio::time::timeout(Duration::from_secs(10), call).await
            {
                reason = Some(detail);
            }
        } else {
            call.abort();
        }
    }
    (completed, grant, reason)
}

/// The least grant `run` completes on, to within [`PRECISION`], given the
/// production grant it completes on.
async fn least_under<F, Fut>(grant: u64, run: F) -> u64
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    narrow(0, grant, PRECISION, &|fuel| {
        let completed = run(fuel);
        async move { (completed.await, 0) }
    })
    .await
}

/// `cell` measured: the least grant it completes on and its share of the
/// production grant, or, where the production grant does not carry it,
/// what it ends on. The host's own share holds the grant computed here: a
/// share 2% over the least completes, one 2% under does not.
async fn measure_cell(cell: Cell) -> (String, Option<u64>) {
    let label = cell.label();
    let (completed, grant, reason) = build_run(cell, |_| None).await;
    if !completed {
        let (ample, _, _) = build_run(cell, |_| fuel(AMPLE_FUEL)).await;
        let ended = if ample {
            "fuel".to_string()
        } else {
            reason.unwrap_or_else(|| "memory or the epoch deadline".to_string())
        };
        return (
            format!("{label}: does not complete on its production grant {grant}; ends on {ended}"),
            None,
        );
    }
    let least = least_under(
        grant,
        |g| async move { build_run(cell, |_| fuel(g)).await.0 },
    )
    .await;
    const PARTS: u64 = 1 << 20;
    let share = |scale: f64| {
        let numerator = (least as f64 * scale / grant as f64 * PARTS as f64) as u64;
        move |_: u64| {
            Some(ActivationLimits::FuelShare {
                numerator,
                denominator: PARTS,
            })
        }
    };
    assert!(build_run(cell, share(1.02)).await.0, "{label}: 2% over");
    assert!(!build_run(cell, share(0.98)).await.0, "{label}: 2% under");
    (
        format!(
            "{label}: completes on {least} of a production grant of {grant}, {:.4} of it",
            least as f64 / grant as f64
        ),
        Some(least),
    )
}

/// Run `measure` on every one of `inputs`, [`MEASURED_AT_ONCE`] at a time;
/// the results in the inputs' order.
async fn measure_all<I, O, F, Fut>(inputs: Vec<I>, measure: F) -> Vec<O>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut,
    Fut: std::future::Future<Output = O> + Send + 'static,
{
    let permits = Arc::new(tokio::sync::Semaphore::new(MEASURED_AT_ONCE));
    let tasks: Vec<_> = inputs
        .into_iter()
        .map(|input| {
            let permits = permits.clone();
            let run = measure(input);
            tokio::spawn(async move {
                let _permit = permits.acquire_owned().await.unwrap();
                run.await
            })
        })
        .collect();
    let mut out = Vec::new();
    for task in tasks {
        out.push(task.await.expect("a measured cell"));
    }
    out
}

/// The least grant and share of the production grant each build activation
/// completes on: new drafts and replies, one and two to an activation,
/// files of [`FILES_MAX_BYTES`] together in each split and none, each body
/// at its bound; and the fuel a file byte costs, each split's least less
/// that of no files, over the file bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement: run by hand"]
async fn measure_draft_builds() {
    let splits = [
        None,
        Some(Split::One),
        Some(Split::FiveEqual),
        Some(Split::FourBytesAndTheRest),
    ];
    let mut cells = Vec::new();
    for reply in [false, true] {
        for n in [1, 2] {
            for content in BUILD_CONTENTS {
                for split in splits {
                    cells.push(Cell {
                        reply,
                        n,
                        split,
                        content,
                    });
                }
            }
        }
    }
    let results = measure_all(cells.clone(), measure_cell).await;
    for (line, _) in &results {
        println!("{line}");
    }
    for (cell, (_, least)) in cells.iter().zip(&results) {
        let (Some(_), Some(least)) = (cell.split, least) else {
            continue;
        };
        let without = cells
            .iter()
            .zip(&results)
            .find(|(other, _)| {
                other.split.is_none()
                    && other.reply == cell.reply
                    && other.n == cell.n
                    && other.content.name == cell.content.name
            })
            .and_then(|(_, (_, least))| *least);
        if let Some(without) = without {
            let file_bytes = (cell.n * FILES_MAX_BYTES) as f64;
            println!(
                "{}: {:.1} fuel a file byte",
                cell.label(),
                (*least as f64 - without as f64) / file_bytes
            );
        }
    }
}

/// How many late answers one activation settles in the measurement.
const LATE_ANSWERS: usize = 5;

/// The widest in-flight row's files: five of one byte, each under a
/// 64-character name and a 255-byte content type.
fn widest_row_files(request: usize) -> Vec<File> {
    let content_type = format!("application/x-{}", "t".repeat(255 - "application/x-".len()));
    (0..FILES_MAX)
        .map(|i| {
            let name = format!("{request}{i}{}", "n".repeat(62));
            file(&name, &content_type, vec![b'x'])
        })
        .collect()
}

/// The step of the widest reply that its late answers answer.
#[derive(Clone, Copy, Debug)]
enum LateStep {
    /// The read of the reply's original: the settle parses the arguments
    /// again, and the kit drops the upload it returns.
    Read,
    /// The reply's upload.
    Upload,
}

/// Bring a [`bound_rig`] to the activation that settles [`LATE_ANSWERS`]
/// late answers to `step` of the widest reply, its body of `content` at
/// `body_bytes`, carrying [`widest_row_files`]. Each reply is called and
/// served; for [`LateStep::Upload`] its original's read is answered and the
/// activation that builds its upload run. The stub holds the step's request
/// until it takes a permit, and the request is made a late answer. Only then
/// are the five answered, so no answer reaches the provider before the last
/// is issued.
async fn late_answers(step: LateStep, content: Content, body_bytes: usize) -> StubRig {
    let r = bound_rig().await;
    let args = widest_args(content, body_bytes, true);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    if let LateStep::Read = step {
        r.stub.lock().unwrap().api_gate = Some(gate.clone());
    }
    let mut steps = Vec::new();
    for n in 1..=LATE_ANSWERS {
        let call = r.call_for_alice_with(
            "GmailDraft",
            args.clone(),
            held_files(&r, &widest_row_files(n)),
        );
        r.requested_id().await;
        r.serve().await;
        let seen = match step {
            LateStep::Read => n,
            LateStep::Upload => {
                r.stub.lock().unwrap().api_gate = None;
                r.run_nested().await;
                r.stub.lock().unwrap().api_gate = Some(gate.clone());
                r.serve().await;
                2 * n
            }
        };
        steps.push(r.end_held(call, seen).await);
    }
    r.answer_late(&gate, steps).await;
    r
}

/// Whether `r`'s late answers to `step` were all settled: no row is left.
/// For [`LateStep::Read`] the settle must also have issued nothing: the
/// executor is owed no upload and the stub saw the five reads alone, so a
/// request left pending by the staging cannot pass as a settle.
async fn settled(r: &StubRig, step: LateStep) -> bool {
    let settled = r.settled_late(LATE_ANSWERS);
    if settled && let LateStep::Read = step {
        assert!(built(r, 0).await, "no upload issued for an ended request");
        let seen = r.seen();
        assert_eq!(seen.len(), LATE_ANSWERS, "{seen:?}");
        assert!(
            seen.iter()
                .all(|(method, path, _)| method == "GET" && path == &format!("{MESSAGES}/m1")),
            "{seen:?}"
        );
    }
    settled
}

/// The late answers' settle to `step`, of a body of `content` at
/// `body_bytes`, run under `limits` of its production grant: whether every
/// row was settled, the production grant, and the envelopes the activation
/// is handed.
async fn settle_run(
    step: LateStep,
    content: Content,
    body_bytes: usize,
    limits: impl FnOnce(u64) -> Option<ActivationLimits>,
) -> (bool, u64, usize) {
    let r = late_answers(step, content, body_bytes).await;
    let envelopes = r
        .owed_windows()
        .await
        .iter()
        .map(|port| port.entries.len())
        .sum();
    let grant = production_grant(&r).await;
    r.serve_under(limits(grant)).await;
    (settled(&r, step).await, grant, envelopes)
}

/// The body the late answers' settle to the upload step costs most on, by
/// `measure_draft_late_answers`.
const COSTLIEST_LATE: Content = CONTROL;
/// The body the late answers' settle to the read step costs most on, by
/// `measure_draft_late_answers`.
const COSTLIEST_LATE_READ: Content = CONTROL;

/// Five late answers to the widest reply's upload, its body of U+0001 at the
/// bound, the costliest content, its row carrying five files under the
/// longest names and content types, are settled in one activation under the
/// margin:
/// the kit decodes each in-flight row and completes a request the host no
/// longer holds, leaving no row. Measured by `measure_draft_late_answers`,
/// the activation is handed 12 envelopes and completes on about 42.0M of a
/// 120.6M production grant, 0.348 of it. A failure here is answered by
/// measuring again and correcting this figure or the component's bounds,
/// never by raising the share.
#[tokio::test(flavor = "multi_thread")]
async fn five_late_answers_to_the_widest_draft_settle_inside_the_margin() {
    let r = late_answers(LateStep::Upload, COSTLIEST_LATE, BODY_MAX_BYTES).await;
    r.serve_under(Some(MARGIN)).await;
    assert!(
        settled(&r, LateStep::Upload).await,
        "the five late answers were settled"
    );
}

/// Five late answers to the widest reply's read of its original, its body
/// of U+0001 at the bound, the costliest content, its row carrying five
/// files under the longest names and content types, are settled in one
/// activation on its production grant: each settle decodes the in-flight
/// row and parses the arguments again, the kit drops the upload for a
/// request the host no longer holds, and no row is left and nothing is
/// issued. Measured by `measure_draft_late_answers`, the activation is
/// handed 7 envelopes and completes on about 49.8M of a 72.0M production
/// grant, 0.692 of it: over the margin, so this holds the grant and not the
/// margin. A failure here is answered by measuring again, never by raising
/// the share.
#[tokio::test(flavor = "multi_thread")]
async fn five_late_answers_to_the_widest_replys_read_settle() {
    let r = late_answers(LateStep::Read, COSTLIEST_LATE_READ, BODY_MAX_BYTES).await;
    r.serve_under(None).await;
    assert!(
        settled(&r, LateStep::Read).await,
        "the five late answers were settled"
    );
}

/// For each step of the widest reply, each body content, at 20,000 bytes
/// and at 10,000: the envelopes the late answers' settle is handed, its
/// production grant, and the least grant and share it completes on; then
/// per step and content the settle's base and slope a body byte, and the
/// body at which the least grant would meet the margin.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a measurement: run by hand"]
async fn measure_draft_late_answers() {
    let sizes = [BODY_MAX_BYTES / 2, BODY_MAX_BYTES];
    let steps = [LateStep::Read, LateStep::Upload];
    let mut inputs = Vec::new();
    for step in steps {
        for content in LATE_CONTENTS {
            for size in sizes {
                inputs.push((step, content, size));
            }
        }
    }
    let results = measure_all(inputs.clone(), |(step, content, size)| async move {
        let (completed, grant, envelopes) = settle_run(step, content, size, |_| None).await;
        if !completed {
            let (ample, _, _) = settle_run(step, content, size, |_| fuel(AMPLE_FUEL)).await;
            return (grant, envelopes, None, ample);
        }
        let least = least_under(grant, |g| async move {
            settle_run(step, content, size, |_| fuel(g)).await.0
        })
        .await;
        (grant, envelopes, Some(least), true)
    })
    .await;
    for ((step, content, size), (grant, envelopes, least, ample)) in inputs.iter().zip(&results) {
        match least {
            Some(least) => println!(
                "late answers to {step:?}, {} at {size} bytes: {envelopes} envelopes, \
                 grant {grant}, completes on {least}, {:.4} of it",
                content.name,
                *least as f64 / *grant as f64
            ),
            None => println!(
                "late answers to {step:?}, {} at {size} bytes: {envelopes} envelopes, \
                 grant {grant}, does not complete on it; {}",
                content.name,
                if *ample {
                    "completes on ample fuel"
                } else {
                    "nor on ample fuel"
                }
            ),
        }
    }
    let cells = steps
        .iter()
        .flat_map(|step| LATE_CONTENTS.iter().map(move |content| (step, content)));
    for (pair, (step, content)) in results.chunks(2).zip(cells) {
        let [(grant, _, Some(small), _), (_, _, Some(large), _)] = pair else {
            continue;
        };
        let slope = (*large as f64 - *small as f64) / (sizes[1] - sizes[0]) as f64;
        let base = *small as f64 - slope * sizes[0] as f64;
        let margin = *grant as f64 * 19.0 / 28.0;
        println!(
            "late answers to {step:?}, {}: base {base:.0}, {slope:.1} fuel a body byte; \
             meets the margin of {margin:.0} at a body of about {:.0} bytes",
            content.name,
            (margin - base) / slope
        );
    }
}
