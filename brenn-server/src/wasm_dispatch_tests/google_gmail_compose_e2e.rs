//! The `google-gmail-compose` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use brenn_tool_registry::ToolError;
use serde_json::json;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, Clauses, FirstRead, GMAIL_COMPOSE, google_query,
    insufficient_scope_body, scope_denied,
};
use super::stub_rig::{Pairs, StubRig, assert_could_not_be_issued};

const MESSAGES: &str = "/gmail/v1/users/me/messages";
const DRAFTS: &str = "/gmail/v1/users/me/drafts";

/// The instance's `api-request` clauses: a reply's read of its original, and
/// the draft's `POST`.
const CLAUSES: Clauses<'static> = &[("GET", "/gmail/v1/users/me/messages/*"), ("POST", DRAFTS)];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&GMAIL_COMPOSE, CLAUSES).await
}

/// The rig with `clauses` and alice's one account connected.
async fn rig_with(clauses: Clauses<'_>) -> StubRig {
    workspace::rig_with(&GMAIL_COMPOSE, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// The credential cases' first call: a new draft's `POST`.
fn first_reads() -> [FirstRead; 1] {
    [FirstRead {
        tool: "GmailDraft",
        slug: "gmail-draft",
        args: json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" }),
        method: "POST",
        path: DRAFTS,
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

/// A draft's message as the one `POST` carried it: the request body is
/// `{"message": {"raw": …}}`, with `threadId` for a reply in `thread` and
/// nothing else, `raw` is base64url, and the message is its headers,
/// unfolded and in order, and its body, base64 decoded.
fn posted_message(bodies: &[Vec<u8>], thread: Option<&str>) -> (Vec<(String, String)>, String) {
    use base64ct::{Base64, Base64UrlUnpadded, Encoding as _};

    assert_eq!(bodies.len(), 1, "exactly one POST carried a body");
    let request: serde_json::Value = serde_json::from_slice(&bodies[0]).unwrap();
    let raw = request["message"]["raw"].as_str().expect("a raw message");
    let expected = match thread {
        Some(thread) => json!({ "message": { "raw": raw, "threadId": thread } }),
        None => json!({ "message": { "raw": raw } }),
    };
    assert_eq!(
        request, expected,
        "a draft names its thread only as a reply"
    );
    let message = String::from_utf8(Base64UrlUnpadded::decode_vec(raw).unwrap()).unwrap();
    let (head, body) = message.split_once("\r\n\r\n").expect("a header block");
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in head.split("\r\n") {
        assert!(line.len() <= 76, "a header line over 76: {line:?}");
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
    for line in body.split_terminator("\r\n") {
        assert!(line.len() <= 76, "a body line over 76: {line:?}");
    }
    let body = Base64::decode_vec(&body.replace("\r\n", "")).unwrap();
    (headers, String::from_utf8(body).unwrap())
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

/// `GmailDraft` creates a draft with one `POST` to drafts.create carrying
/// alice's bearer and the message as `raw`: the given recipients and
/// subject, no `From` or `Date`, and the body with CRLF line breaks. The
/// answer names the draft, its message and its thread.
#[tokio::test]
async fn gmail_draft_posts_one_raw_message_with_the_users_bearer() {
    let r = rig().await;
    r.answer(DRAFTS, draft_body());
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
        [("POST".to_string(), DRAFTS.to_string(), google_query(&[]))]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    let (headers, body) = posted_message(&r.bodies(), None);
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
    r.answer(DRAFTS, draft_body());
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
    let (headers, body) = posted_message(&r.bodies(), None);
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

/// Google's 403 on the draft's `POST` is `denied`, telling the assistant to
/// reconnect with the Gmail compose scope.
#[tokio::test]
async fn a_draft_403_names_the_compose_scope() {
    let r = rig().await;
    r.respond(DRAFTS, 403, insufficient_scope_body());
    let call = r.call_for_alice(
        "GmailDraft",
        json!({ "to": "bob@example.com", "subject": "Lunch", "body": "Friday?" }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail compose"));
    r.assert_finished(&id);
}

/// Without the drafts clause the draft's `POST` is refused when it is
/// queued, even though the reply read's `GET` clause stands: the request is
/// answered `internal` with Brenn's text and the clause guidance.
#[tokio::test]
async fn a_draft_post_without_its_clause_could_not_be_issued() {
    let clauses: Vec<(&str, &str)> = CLAUSES
        .iter()
        .copied()
        .filter(|&(_, path)| path != DRAFTS)
        .collect();
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

/// A reply with no `subject` reads its original's metadata, then posts one
/// draft in the original's thread carrying `In-Reply-To`, `References` (the
/// original's followed by its `Message-ID`) and the original's subject after
/// `Re: `. The answer names the `Message-ID` answered and the derived
/// subject.
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
    r.answer(DRAFTS, draft_body());
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
            ("POST".to_string(), DRAFTS.to_string(), google_query(&[])),
        ]
    );
    assert_eq!(
        r.bearers(),
        [format!("Bearer {BEARER}"), format!("Bearer {BEARER}")]
    );
    let (headers, body) = posted_message(&r.bodies(), Some("t9"));
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

/// A reply to an original without a `Message-ID` posts the original's
/// `threadId` with no `In-Reply-To` or `References`; the answer names no
/// `in_reply_to`, and a caller's `subject` is used as given.
#[tokio::test]
async fn gmail_draft_replies_to_an_original_without_a_message_id() {
    let r = rig().await;
    r.answer(
        &format!("{MESSAGES}/m1"),
        original_body(&[("Subject", "Lunch on Friday")]),
    );
    r.answer(DRAFTS, draft_body());
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
    let (headers, _) = posted_message(&r.bodies(), Some("t9"));
    let expected: Vec<(String, String)> = [("To", "bob@example.com"), ("Subject", "Re: Lunch")]
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .into_iter()
        .chain(mime_headers())
        .collect();
    assert_eq!(headers, expected);
    r.assert_finished(&id);
}

/// Google's 403 on a reply's read of its original is `denied` naming the
/// Gmail read scope, which that read needs, and no draft is posted.
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
