//! The `google-gmail-read` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use brenn_tool_registry::ToolError;
use serde_json::json;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, BOTH_WAYS, Clauses, FirstRead, GMAIL_READ, OVERSIZED,
    Unreadable, assert_unreadable, google_query, insufficient_scope_body, scope_denied,
};
use super::stub_rig::{Pairs, StubRig, assert_could_not_be_issued};

const LABELS: &str = "/gmail/v1/users/me/labels";
const MESSAGES: &str = "/gmail/v1/users/me/messages";
const THREADS: &str = "/gmail/v1/users/me/threads";

/// The instance's `api-request` clauses: one per endpoint the tools call.
const CLAUSES: Clauses<'static> = &[
    ("GET", LABELS),
    ("GET", "/gmail/v1/users/me/labels/*"),
    ("GET", MESSAGES),
    ("GET", "/gmail/v1/users/me/messages/*"),
    ("GET", "/gmail/v1/users/me/threads/*"),
];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&GMAIL_READ, CLAUSES).await
}

/// The rig with `clauses` and alice's one account connected.
async fn rig_with(clauses: Clauses<'_>) -> StubRig {
    workspace::rig_with(&GMAIL_READ, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// The rig with every clause and alice's one account connected,
/// `api-request` reading at most `max_response_bytes` of each of Google's
/// answers.
async fn rig_reading_at_most(max_response_bytes: usize) -> StubRig {
    workspace::rig_reading_at_most(&GMAIL_READ, CLAUSES, max_response_bytes).await
}

/// The rig for `way`, with every clause and alice's one account connected.
async fn unreadable_rig(way: Unreadable) -> StubRig {
    workspace::unreadable_rig(&GMAIL_READ, CLAUSES, way).await
}

/// labels.list's query as the `GmailLabels` listing sends it.
fn labels_list_query() -> Pairs {
    google_query(&[("fields", "labels(id,name,type)")])
}

/// messages.get's metadata query for one search line: the three
/// `metadataHeaders`, each its own query key.
fn metadata_query() -> Pairs {
    google_query(&[
        ("format", "metadata"),
        (
            "fields",
            "id,threadId,labelIds,snippet,internalDate,sizeEstimate,payload/headers",
        ),
        ("metadataHeaders", "Date"),
        ("metadataHeaders", "From"),
        ("metadataHeaders", "Subject"),
    ])
}

/// Google's listing of alice's labels: two system labels and one of hers.
fn labels_body() -> String {
    serde_json::to_string_pretty(&json!({
        "labels": [
            { "id": "INBOX", "name": "INBOX", "type": "system" },
            { "id": "UNREAD", "name": "UNREAD", "type": "system" },
            { "id": "Label_7", "name": "Receipts", "type": "user" },
        ],
    }))
    .unwrap()
}

/// messages.list's query for `q` at the default page size.
fn search_list_query(q: &str) -> Pairs {
    google_query(&[
        ("q", q),
        ("maxResults", "10"),
        (
            "fields",
            "messages(id,threadId),nextPageToken,resultSizeEstimate",
        ),
    ])
}

/// messages.list's page listing `ids`.
fn listing_body(ids: &[&str]) -> String {
    let messages: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| json!({ "id": id, "threadId": format!("t-{id}") }))
        .collect();
    json!({ "messages": messages, "resultSizeEstimate": ids.len() }).to_string()
}

/// One message's metadata as messages.get answers it.
fn message_body(id: &str, from: &str, subject: &str, labels: &[&str], snippet: &str) -> String {
    serde_json::to_string_pretty(&json!({
        "id": id,
        "threadId": format!("t-{id}"),
        "labelIds": labels,
        "snippet": snippet,
        "internalDate": "1793455509000",
        "sizeEstimate": 2048,
        "payload": { "headers": [
            { "name": "From", "value": from },
            { "name": "Subject", "value": subject },
            { "name": "Date", "value": "Sat, 31 Oct 2026 14:05:09 +0000" },
        ] },
    }))
    .unwrap()
}

/// `GmailLabels` lists alice's labels with one `GET` carrying her bearer and
/// the listing's `fields` mask, answered compactly in Google's order.
#[tokio::test]
async fn labels_lists_with_the_users_bearer() {
    let r = rig().await;
    r.answer(LABELS, labels_body());
    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;

    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["tool"], "api-request");
    assert_eq!(nested[0]["serving"], id.as_str());
    assert_eq!(nested[0]["call_id"], format!("{id}/1"));
    r.executor.drain_step().await;
    r.owed(&r.provider_sub, 1).await;
    assert_eq!(r.inflight_keys(), [format!("{id}/1")]);
    r.serve().await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        json!({ "labels": [
            { "id": "INBOX", "name": "INBOX", "type": "system" },
            { "id": "UNREAD", "name": "UNREAD", "type": "system" },
            { "id": "Label_7", "name": "Receipts", "type": "user" },
        ] })
    );
    assert_eq!(
        r.seen(),
        [("GET".to_string(), LABELS.to_string(), labels_list_query())]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    r.assert_finished(&id);
}

/// `GmailLabels { id }` reads the one label with its counts.
#[tokio::test]
async fn labels_with_an_id_reads_its_counts() {
    let r = rig().await;
    r.answer(
        "/gmail/v1/users/me/labels/Label_7",
        serde_json::to_string_pretty(&json!({
            "id": "Label_7",
            "name": "Receipts",
            "type": "user",
            "messagesTotal": 42,
            "messagesUnread": 3,
            "threadsTotal": 40,
            "threadsUnread": 3,
        }))
        .unwrap(),
    );
    let call = r.call_for_alice("GmailLabels", json!({ "id": "Label_7" }));
    let id = r.requested_id().await;
    r.run(1).await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        json!({
            "id": "Label_7",
            "name": "Receipts",
            "type": "user",
            "messages_total": 42,
            "messages_unread": 3,
            "threads_total": 40,
            "threads_unread": 3,
        })
    );
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            "/gmail/v1/users/me/labels/Label_7".to_string(),
            google_query(&[(
                "fields",
                "id,name,type,messagesTotal,messagesUnread,threadsTotal,threadsUnread"
            )]),
        )]
    );
    r.assert_finished(&id);
}

/// `GmailSearch` lists one page, reads each listed message's metadata with
/// the three `metadataHeaders` repeated as separate keys, then lists the
/// labels because a message carries a user label, and answers the lines with
/// that label named and Google's page token.
#[tokio::test]
async fn search_lists_reads_each_message_then_names_labels() {
    let r = rig().await;
    r.answer(
        MESSAGES,
        serde_json::to_string_pretty(&json!({
            "messages": [
                { "id": "m1", "threadId": "t-m1" },
                { "id": "m2", "threadId": "t-m2" },
            ],
            "nextPageToken": "page-2",
            "resultSizeEstimate": 12,
        }))
        .unwrap(),
    );
    r.answer(
        &format!("{MESSAGES}/m1"),
        message_body(
            "m1",
            "Example Corp <billing@example.com>",
            "Your receipt",
            &["INBOX", "Label_7"],
            "Total: $12 &amp; tax",
        ),
    );
    r.answer(
        &format!("{MESSAGES}/m2"),
        message_body(
            "m2",
            "Bob Example <bob@example.com>",
            "Lunch?",
            &["INBOX", "UNREAD"],
            "Are you free",
        ),
    );
    r.answer(LABELS, labels_body());
    let call = r.call_for_alice(
        "GmailSearch",
        json!({ "q": "from:example.com newer_than:7d", "max": 2 }),
    );
    let id = r.requested_id().await;

    // The listing, then one read per listed id, then the label names.
    r.serve().await;
    for step in [1, 2, 3, 100] {
        assert_eq!(r.inflight_keys(), [format!("{id}/{step}")]);
        let result = r.run_nested().await;
        assert_eq!(result["call_id"], format!("{id}/{step}"));
        r.serve().await;
    }

    let answer = call.await.unwrap().expect("the component answered");
    let line = |id: &str, from: &str, subject: &str, labels: &[&str], snippet: &str| {
        json!({
            "id": id,
            "thread_id": format!("t-{id}"),
            "from": from,
            "subject": subject,
            "date": "Sat, 31 Oct 2026 14:05:09 +0000",
            "internal_date": "2026-10-31T14:05:09Z",
            "snippet": snippet,
            "labels": labels,
        })
    };
    assert_eq!(
        answer,
        json!({
            "messages": [
                line(
                    "m1",
                    "Example Corp <billing@example.com>",
                    "Your receipt",
                    &["INBOX", "Receipts"],
                    "Total: $12 & tax",
                ),
                line(
                    "m2",
                    "Bob Example <bob@example.com>",
                    "Lunch?",
                    &["INBOX", "UNREAD"],
                    "Are you free",
                ),
            ],
            "next_page": "page-2",
            "result_size_estimate": 12,
        })
    );

    let get = |id: &str| {
        (
            "GET".to_string(),
            format!("{MESSAGES}/{id}"),
            metadata_query(),
        )
    };
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                MESSAGES.to_string(),
                google_query(&[
                    ("q", "from:example.com newer_than:7d"),
                    ("maxResults", "2"),
                    (
                        "fields",
                        "messages(id,threadId),nextPageToken,resultSizeEstimate"
                    ),
                ]),
            ),
            get("m1"),
            get("m2"),
            (
                "GET".to_string(),
                LABELS.to_string(),
                google_query(&[("fields", "labels(id,name)")]),
            ),
        ]
    );
    assert_eq!(r.bearers(), vec![format!("Bearer {BEARER}"); 4]);
    r.assert_finished(&id);
}

/// `GmailSearch { page }` sends the token as `pageToken`, and a page with no
/// messages completes after the one listing call.
#[tokio::test]
async fn search_sends_the_page_token_and_an_empty_page_completes() {
    let r = rig().await;
    r.answer(MESSAGES, json!({ "resultSizeEstimate": 0 }).to_string());
    let call = r.call_for_alice(
        "GmailSearch",
        json!({ "q": "is:starred", "page": "page-2", "spam_trash": true }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(answer, json!({ "messages": [], "result_size_estimate": 0 }));
    assert_eq!(
        r.seen(),
        [(
            "GET".to_string(),
            MESSAGES.to_string(),
            google_query(&[
                ("q", "is:starred"),
                ("maxResults", "10"),
                ("pageToken", "page-2"),
                ("includeSpamTrash", "true"),
                (
                    "fields",
                    "messages(id,threadId),nextPageToken,resultSizeEstimate"
                ),
            ]),
        )]
    );
    r.assert_finished(&id);
}

/// A page whose messages carry only system labels completes after its reads,
/// with no labels.list, and answers the label ids as Google gave them.
#[tokio::test]
async fn search_with_only_system_labels_skips_the_label_names() {
    let r = rig().await;
    r.answer(MESSAGES, listing_body(&["m2"]));
    r.answer(
        &format!("{MESSAGES}/m2"),
        message_body(
            "m2",
            "Bob Example <bob@example.com>",
            "Lunch?",
            &["INBOX", "UNREAD"],
            "Are you free",
        ),
    );
    let call = r.call_for_alice("GmailSearch", json!({ "q": "is:unread" }));
    let id = r.requested_id().await;
    r.run(2).await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        json!({
            "messages": [{
                "id": "m2",
                "thread_id": "t-m2",
                "from": "Bob Example <bob@example.com>",
                "subject": "Lunch?",
                "date": "Sat, 31 Oct 2026 14:05:09 +0000",
                "internal_date": "2026-10-31T14:05:09Z",
                "snippet": "Are you free",
                "labels": ["INBOX", "UNREAD"],
            }],
            "result_size_estimate": 1,
        })
    );
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                MESSAGES.to_string(),
                search_list_query("is:unread"),
            ),
            (
                "GET".to_string(),
                format!("{MESSAGES}/m2"),
                metadata_query(),
            ),
        ]
    );
    r.owed(&r.executor_sub(), 0).await;
    r.assert_finished(&id);
}

/// Google's 403 on a message read after the listing succeeded is the
/// search's `denied`: no later message is read, and no row is left.
#[tokio::test]
async fn a_google_403_mid_search_fails_the_search() {
    let r = rig().await;
    r.answer(MESSAGES, listing_body(&["m1", "m2"]));
    r.respond(&format!("{MESSAGES}/m1"), 403, insufficient_scope_body());
    let call = r.call_for_alice("GmailSearch", json!({ "q": "is:unread" }));
    let id = r.requested_id().await;
    r.run(2).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail read"));
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                MESSAGES.to_string(),
                search_list_query("is:unread"),
            ),
            (
                "GET".to_string(),
                format!("{MESSAGES}/m1"),
                metadata_query(),
            ),
        ]
    );
    r.owed(&r.executor_sub(), 0).await;
    r.assert_finished(&id);
}

/// The credential cases' first call: `GmailLabels`' bare `GET`.
fn first_reads() -> [FirstRead; 1] {
    [FirstRead {
        tool: "GmailLabels",
        slug: "gmail-labels",
        args: json!({}),
        method: "GET",
        path: LABELS,
        body: labels_body(),
    }]
}

/// With no account connected, `api-request`'s `credential_invalid` value is
/// the tool's answer unchanged, and the component makes no further call.
#[tokio::test]
async fn no_connected_account_is_relayed_as_credential_invalid() {
    for read in first_reads() {
        workspace::assert_no_connected_account(&GMAIL_READ, CLAUSES, read).await;
    }
}

/// With two accounts connected, a call naming none is `denied` with
/// `api-request`'s list of them, and a call naming one sends that account's
/// bearer.
#[tokio::test]
async fn several_accounts_need_one_named() {
    for read in first_reads() {
        workspace::assert_several_accounts_need_one_named(&GMAIL_READ, CLAUSES, read).await;
    }
}

/// A component calling a tool on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
#[tokio::test]
async fn a_userless_caller_is_denied_with_the_reason() {
    for read in first_reads() {
        workspace::assert_a_userless_caller_is_denied_with_the_reason(&GMAIL_READ, CLAUSES, read)
            .await;
    }
}

/// Google's 403 is the tool's `denied`: Google's status and message, and the
/// sentence telling the assistant to reconnect with the read scope.
#[tokio::test]
async fn a_google_403_is_denied_with_the_scope_hint() {
    let r = rig().await;
    r.respond(LABELS, 403, insufficient_scope_body());
    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail read"));
    r.assert_finished(&id);
}

/// A nested call the instance's grant does not admit is refused when it is
/// queued: the request is answered `internal` with Brenn's text and the clause
/// guidance, and nothing reaches the API.
#[tokio::test]
async fn a_clause_miss_is_internal_could_not_be_issued() {
    let clauses: Vec<(&str, &str)> = CLAUSES
        .iter()
        .copied()
        .filter(|&(_, path)| path != LABELS)
        .collect();
    let r = rig_with(&clauses).await;
    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);
}

fn full_read_query() -> Pairs {
    google_query(&[("format", "full")])
}

/// One message as messages.get (full) answers it: a `multipart/mixed` of a
/// `text/plain` part whose body is `text_data` (base64url) and a PDF
/// attachment, labelled `labels`. `text_data` of `None` is the structure
/// mask's answer, which carries no `data`.
fn read_body(labels: &[&str], text_data: Option<&str>) -> String {
    let mut text_body = json!({ "size": 30 });
    if let Some(data) = text_data {
        text_body["data"] = json!(data);
    }
    serde_json::to_string_pretty(&json!({
        "id": "m1",
        "threadId": "t-m1",
        "labelIds": labels,
        "snippet": "Hi Bob, The numbers are in &amp; attached",
        "sizeEstimate": 9000,
        "internalDate": "1793455509000",
        "payload": {
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": [
                { "name": "From", "value": "Alice Example <alice@example.com>" },
                { "name": "To", "value": "Bob Example <bob@example.com>" },
                { "name": "Subject", "value": "Quarterly numbers" },
                { "name": "Date", "value": "Sat, 31 Oct 2026 14:05:09 +0000" },
                { "name": "Message-ID", "value": "<m1@example.com>" },
            ],
            "body": { "size": 0 },
            "parts": [
                {
                    "mimeType": "text/plain",
                    "filename": "",
                    "headers": [
                        { "name": "Content-Type", "value": "text/plain; charset=\"UTF-8\"" },
                    ],
                    "body": text_body,
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "Q3 report.pdf",
                    "headers": [
                        { "name": "Content-Type", "value": "application/pdf" },
                        { "name": "Content-Disposition",
                          "value": "attachment; filename=\"Q3 report.pdf\"" },
                    ],
                    "body": { "size": 48213, "attachmentId": "ATT-pdf" },
                },
            ],
        },
    }))
    .unwrap()
}

/// The headers and metadata `GmailRead` answers for [`read_body`]'s message,
/// with `labels` as answered and `rest` added.
fn read_answer(labels: &[&str], rest: serde_json::Value) -> serde_json::Value {
    let mut answer = json!({
        "id": "m1",
        "thread_id": "t-m1",
        "labels": labels,
        "from": "Alice Example <alice@example.com>",
        "to": "Bob Example <bob@example.com>",
        "subject": "Quarterly numbers",
        "date": "Sat, 31 Oct 2026 14:05:09 +0000",
        "message_id": "<m1@example.com>",
        "internal_date": "2026-10-31T14:05:09Z",
        "attachments": [{ "filename": "Q3 report.pdf", "mime_type": "application/pdf",
                          "size": 48213, "inline": false }],
    });
    for (key, value) in rest.as_object().unwrap() {
        answer[key] = value.clone();
    }
    answer
}

/// `GmailRead` reads the message with `format=full`, decodes its plain part,
/// lists its attachment, then lists the labels because the message carries a
/// user label, and answers the label named.
#[tokio::test]
async fn read_decodes_the_full_message_and_names_its_labels() {
    let r = rig().await;
    let path = format!("{MESSAGES}/m1");
    // "Hi Bob,\n\nThe numbers are in.\n", base64url without padding.
    r.answer(
        &path,
        read_body(
            &["INBOX", "Label_7"],
            Some("SGkgQm9iLAoKVGhlIG51bWJlcnMgYXJlIGluLgo"),
        ),
    );
    r.answer(LABELS, labels_body());
    let call = r.call_for_alice("GmailRead", json!({ "id": "m1" }));
    let id = r.requested_id().await;

    r.serve().await;
    for step in [1, 100] {
        assert_eq!(r.inflight_keys(), [format!("{id}/{step}")]);
        r.run_nested().await;
        r.serve().await;
    }

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        read_answer(
            &["INBOX", "Receipts"],
            json!({
                "text": "Hi Bob,\n\nThe numbers are in.\n",
                "text_source": "text/plain",
                "text_chars": 29,
            })
        )
    );
    assert_eq!(
        r.seen(),
        [
            ("GET".to_string(), path, full_read_query()),
            (
                "GET".to_string(),
                LABELS.to_string(),
                google_query(&[("fields", "labels(id,name)")]),
            ),
        ]
    );
    assert_eq!(r.bearers(), vec![format!("Bearer {BEARER}"); 2]);
    r.assert_finished(&id);
}

/// `GmailRead` on a message whose only body is HTML answers its text:
/// the style dropped, paragraphs as blank lines, the link's text without its
/// address, the entity decoded.
#[tokio::test]
async fn an_html_only_read_answers_its_text() {
    let r = rig().await;
    let path = format!("{MESSAGES}/m1");
    // `<html><head><style>p{color:red}</style></head><body><p>Hi Bob,</p><p>The
    // numbers are <a href="https://example.com/q3">in</a> &amp;
    // attached.</p></body></html>`, base64url without padding.
    let html = "PGh0bWw-PGhlYWQ-PHN0eWxlPnB7Y29sb3I6cmVkfTwvc3R5bGU-PC9oZWFkPjxib2R5PjxwPkhpIEJvYiw8\
                L3A-PHA-VGhlIG51bWJlcnMgYXJlIDxhIGhyZWY9Imh0dHBzOi8vZXhhbXBsZS5jb20vcTMiPmluPC9hPiAm\
                YW1wOyBhdHRhY2hlZC48L3A-PC9ib2R5PjwvaHRtbD4";
    let mut body: serde_json::Value = serde_json::from_str(&read_body(&["INBOX"], None)).unwrap();
    body["payload"] = json!({
        "mimeType": "text/html",
        "filename": "",
        "headers": body["payload"]["headers"],
        "body": { "size": 158, "data": html },
    });
    r.answer(&path, body.to_string());
    let call = r.call_for_alice("GmailRead", json!({ "id": "m1" }));
    let id = r.requested_id().await;
    r.run(1).await;

    let text = "Hi Bob,\n\nThe numbers are in & attached.";
    let mut expected = read_answer(
        &["INBOX"],
        json!({
            "text": text,
            "text_source": "text/html",
            "text_chars": text.chars().count(),
        }),
    );
    expected["attachments"] = json!([]);
    assert_eq!(call.await.unwrap(), Ok(expected));
    assert_eq!(r.seen(), [("GET".to_string(), path, full_read_query())]);
    r.assert_finished(&id);
}

/// Google's 404 on a read (an id that names no message) is the caller's
/// `invalid_args` with Google's message.
#[tokio::test]
async fn a_google_404_on_a_read_is_invalid_args() {
    let r = rig().await;
    r.respond(
        &format!("{MESSAGES}/m404"),
        404,
        json!({ "error": {
            "code": 404,
            "message": "Requested entity was not found.",
            "status": "NOT_FOUND",
        } })
        .to_string()
        .into_bytes(),
    );
    let call = r.call_for_alice("GmailRead", json!({ "id": "m404" }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Err(ToolError::InvalidArgs(
            "Google NOT_FOUND (HTTP 404): Requested entity was not found.".to_string()
        ))
    );
    r.owed(&r.executor_sub(), 0).await;
    r.assert_finished(&id);
}

/// A message larger than the provider's read limit comes back `truncated`:
/// `GmailRead` reads it again through the structure mask and answers its
/// headers, snippet and attachments with `text` null and `text_omitted`
/// giving Google's stated size.
#[tokio::test]
async fn a_read_past_the_limit_falls_back_to_the_structure() {
    let r = rig_reading_at_most(4096).await;
    let path = format!("{MESSAGES}/m1");
    // 6 000 bytes of text, 8 000 of base64url: past the 4 096-byte limit.
    let full = read_body(&["INBOX"], Some(&"QUFB".repeat(2000)));
    r.answer(&path, full.clone());
    let call = r.call_for_alice("GmailRead", json!({ "id": "m1" }));
    let id = r.requested_id().await;

    r.serve().await;
    r.run_nested().await;
    // The stub answers by path alone; what Google answers under the
    // structure mask is the same message without its `data`.
    let structure = read_body(&["INBOX"], None);
    assert!(structure.len() < 4096, "{}", structure.len());
    r.answer(&path, structure);
    r.serve().await;
    assert_eq!(r.inflight_keys(), [format!("{id}/2")]);
    r.run_nested().await;
    r.serve().await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(
        answer,
        read_answer(
            &["INBOX"],
            json!({
                "snippet": "Hi Bob, The numbers are in & attached",
                "text": null,
                "text_omitted": format!(
                    "Google's answer ({} bytes; sizeEstimate 9000) exceeds api-request's read \
                     limit for this provider (max_response_bytes); headers and attachments are \
                     listed, the body is not",
                    full.len()
                ),
            })
        )
    );
    assert_eq!(
        r.seen(),
        [
            ("GET".to_string(), path.clone(), full_read_query()),
            (
                "GET".to_string(),
                path,
                google_query(&[
                    ("format", "full"),
                    (
                        "fields",
                        "id,threadId,labelIds,snippet,internalDate,sizeEstimate,\
                         payload(mimeType,filename,headers,body(size,attachmentId),\
                         parts(mimeType,filename,headers,body(size,attachmentId),\
                         parts(mimeType,filename,headers,body(size,attachmentId),\
                         parts(mimeType,filename,headers,body(size,attachmentId)))))",
                    ),
                ]),
            ),
        ]
    );
    r.assert_finished(&id);
}

/// One message of a thread as threads.get (metadata) answers it.
fn thread_message_body(id: &str, from: &str, subject: &str, labels: &[&str]) -> serde_json::Value {
    json!({
        "id": id,
        "labelIds": labels,
        "snippet": format!("{subject} &amp; more"),
        "internalDate": "1793455509000",
        "payload": { "headers": [
            { "name": "From", "value": from },
            { "name": "To", "value": "Bob Example <bob@example.com>" },
            { "name": "Subject", "value": subject },
            { "name": "Date", "value": "Sat, 31 Oct 2026 14:05:09 +0000" },
        ] },
    })
}

/// `GmailThread` reads the thread in one metadata call with the five
/// `metadataHeaders`, keeps the newest `max` messages and counts the rest,
/// answers the thread's subject as its first message's even though that
/// message is left out, and names the user label a kept message carries.
#[tokio::test]
async fn thread_reads_once_and_keeps_the_newest_messages() {
    let r = rig().await;
    let path = format!("{THREADS}/t1");
    r.answer(
        &path,
        serde_json::to_string_pretty(&json!({
            "id": "t1",
            "messages": [
                thread_message_body("m1", "Alice Example <alice@example.com>", "Planning", &["SENT"]),
                thread_message_body("m2", "Bob Example <bob@example.com>", "Planning", &["INBOX"]),
                thread_message_body(
                    "m3",
                    "Carol Example <carol@example.com>",
                    "Re: Planning",
                    &["INBOX", "Label_7"],
                ),
            ],
        }))
        .unwrap(),
    );
    r.answer(LABELS, labels_body());
    let call = r.call_for_alice("GmailThread", json!({ "id": "t1", "max": 2 }));
    let id = r.requested_id().await;

    r.serve().await;
    for step in [1, 100] {
        assert_eq!(r.inflight_keys(), [format!("{id}/{step}")]);
        r.run_nested().await;
        r.serve().await;
    }

    let answer = call.await.unwrap().expect("the component answered");
    let kept = |id: &str, from: &str, labels: &[&str]| {
        json!({
            "id": id,
            "from": from,
            "to": "Bob Example <bob@example.com>",
            "date": "Sat, 31 Oct 2026 14:05:09 +0000",
            "internal_date": "2026-10-31T14:05:09Z",
            "labels": labels,
        })
    };
    let mut m2 = kept("m2", "Bob Example <bob@example.com>", &["INBOX"]);
    m2["snippet"] = json!("Planning & more");
    let mut m3 = kept(
        "m3",
        "Carol Example <carol@example.com>",
        &["INBOX", "Receipts"],
    );
    m3["snippet"] = json!("Re: Planning & more");
    m3["subject"] = json!("Re: Planning");
    assert_eq!(
        answer,
        json!({
            "id": "t1",
            "subject": "Planning",
            "message_count": 3,
            "messages_omitted": 1,
            "messages": [m2, m3],
        })
    );
    assert_eq!(
        r.seen(),
        [
            (
                "GET".to_string(),
                path,
                google_query(&[
                    ("format", "metadata"),
                    (
                        "fields",
                        "id,messages(id,labelIds,snippet,internalDate,payload/headers)",
                    ),
                    ("metadataHeaders", "From"),
                    ("metadataHeaders", "To"),
                    ("metadataHeaders", "Cc"),
                    ("metadataHeaders", "Subject"),
                    ("metadataHeaders", "Date"),
                ]),
            ),
            (
                "GET".to_string(),
                LABELS.to_string(),
                google_query(&[("fields", "labels(id,name)")]),
            ),
        ]
    );
    assert_eq!(r.bearers(), vec![format!("Bearer {BEARER}"); 2]);
    r.assert_finished(&id);
}

/// A search page whose first listed message cannot be read whole answers its
/// other message and its page token, and names the first in `unavailable`
/// with the reason.
#[tokio::test]
async fn a_search_page_names_a_message_it_cannot_read_and_answers_the_rest() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        r.answer(
            MESSAGES,
            json!({
                "messages": [
                    { "id": "m1", "threadId": "t-m1" },
                    { "id": "m2", "threadId": "t-m2" },
                ],
                "nextPageToken": "page-2",
            })
            .to_string(),
        );
        let wide = message_body(
            "m1",
            "Bob Example <bob@example.com>",
            "Lunch?",
            &["INBOX"],
            &"a".repeat(OVERSIZED),
        );
        r.answer(&format!("{MESSAGES}/m1"), wide.clone());
        r.answer(
            &format!("{MESSAGES}/m2"),
            message_body(
                "m2",
                "Bob Example <bob@example.com>",
                "Lunch?",
                &["INBOX", "UNREAD"],
                "Are you free",
            ),
        );
        let call = r.call_for_alice("GmailSearch", json!({ "q": "is:unread" }));
        let id = r.requested_id().await;
        r.run(3).await;

        let answer = call.await.unwrap().expect("the component answered");
        assert_eq!(
            answer["messages"],
            json!([{
                "id": "m2",
                "thread_id": "t-m2",
                "from": "Bob Example <bob@example.com>",
                "subject": "Lunch?",
                "date": "Sat, 31 Oct 2026 14:05:09 +0000",
                "internal_date": "2026-10-31T14:05:09Z",
                "snippet": "Are you free",
                "labels": ["INBOX", "UNREAD"],
            }]),
            "{way:?}"
        );
        assert_eq!(answer["next_page"], "page-2");
        let unavailable = answer["unavailable"]
            .as_array()
            .expect("unavailable listed");
        assert_eq!(unavailable.len(), 1, "{way:?}");
        assert_eq!(unavailable[0]["id"], "m1");
        assert_unreadable(way, unavailable[0]["reason"].as_str().unwrap(), wide.len());
        assert_eq!(r.seen().len(), 3, "{way:?}");
        r.owed(&r.executor_sub(), 0).await;
        r.assert_finished(&id);
    }
}

/// One message of a thread as threads.get answers the narrower mask: no `To`.
fn narrow_thread_message_body(id: &str, from: &str, subject: &str) -> serde_json::Value {
    let mut message = thread_message_body(id, from, subject, &["INBOX"]);
    message["payload"]["headers"]
        .as_array_mut()
        .unwrap()
        .retain(|header| header["name"] != "To");
    message
}

/// A thread whose recipients make it too large to read whole is read again
/// with only `From`, `Subject` and `Date`, and answered with
/// `recipients_omitted` saying why.
#[tokio::test]
async fn a_thread_too_large_to_read_whole_is_read_again_without_recipients() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        let path = format!("{THREADS}/t1");
        let mut wide_message = thread_message_body(
            "m1",
            "Alice Example <alice@example.com>",
            "Planning",
            &["INBOX"],
        );
        wide_message["payload"]["headers"][1]["value"] = json!("b".repeat(OVERSIZED));
        let wide = json!({ "id": "t1", "messages": [wide_message] }).to_string();
        r.answer(&path, wide.clone());
        let call = r.call_for_alice("GmailThread", json!({ "id": "t1" }));
        let id = r.requested_id().await;

        r.serve().await;
        r.run_nested().await;
        // The stub answers by path alone; what Google answers under the
        // narrower headers is the same thread without `To`.
        r.answer(
            &path,
            json!({ "id": "t1", "messages": [narrow_thread_message_body(
                "m1",
                "Alice Example <alice@example.com>",
                "Planning",
            )] })
            .to_string(),
        );
        r.serve().await;
        assert_eq!(r.inflight_keys(), [format!("{id}/2")]);
        r.run_nested().await;
        r.serve().await;

        let answer = call.await.unwrap().expect("the component answered");
        assert_unreadable(
            way,
            answer["recipients_omitted"].as_str().unwrap(),
            wide.len(),
        );
        assert_eq!(answer["subject"], "Planning");
        assert_eq!(answer["message_count"], 1);
        assert_eq!(
            answer["messages"],
            json!([{
                "id": "m1",
                "from": "Alice Example <alice@example.com>",
                "date": "Sat, 31 Oct 2026 14:05:09 +0000",
                "internal_date": "2026-10-31T14:05:09Z",
                "snippet": "Planning & more",
                "labels": ["INBOX"],
            }]),
            "{way:?}"
        );
        let seen = r.seen();
        assert_eq!(seen.len(), 2, "{way:?}");
        assert_eq!(
            seen[1],
            (
                "GET".to_string(),
                path,
                google_query(&[
                    ("format", "metadata"),
                    (
                        "fields",
                        "id,messages(id,labelIds,snippet,internalDate,payload/headers)",
                    ),
                    ("metadataHeaders", "From"),
                    ("metadataHeaders", "Subject"),
                    ("metadataHeaders", "Date"),
                ]),
            )
        );
        r.assert_finished(&id);
    }
}

/// A thread whose narrower read is unreadable too is `internal`, naming why.
#[tokio::test]
async fn a_thread_unreadable_twice_is_internal() {
    for way in BOTH_WAYS {
        let r = unreadable_rig(way).await;
        let wide_message = thread_message_body(
            "m1",
            "Alice Example <alice@example.com>",
            &"s".repeat(OVERSIZED),
            &["INBOX"],
        );
        let wide = json!({ "id": "t1", "messages": [wide_message] }).to_string();
        r.answer(&format!("{THREADS}/t1"), wide.clone());
        let call = r.call_for_alice("GmailThread", json!({ "id": "t1" }));
        let id = r.requested_id().await;
        r.run(2).await;

        let Err(ToolError::Internal(detail)) = call.await.unwrap() else {
            panic!("{way:?}: the thread was not refused internal");
        };
        assert_unreadable(way, &detail, wide.len());
        assert_eq!(r.seen().len(), 2, "{way:?}");
        r.owed(&r.executor_sub(), 0).await;
        r.assert_finished(&id);
    }
}

/// A message read refused by the bus cap falls back to its structure as a
/// cut one does, `text_omitted` quoting the host's refusal.
#[tokio::test]
async fn a_read_refused_by_the_bus_cap_falls_back_to_the_structure() {
    let r = rig().await;
    let path = format!("{MESSAGES}/m1");
    let full = read_body(&["INBOX"], Some(&"QUFB".repeat(OVERSIZED / 4)));
    let full_len = full.len();
    r.answer(&path, full);
    let call = r.call_for_alice("GmailRead", json!({ "id": "m1" }));
    let id = r.requested_id().await;

    r.serve().await;
    r.run_nested().await;
    r.answer(&path, read_body(&["INBOX"], None));
    r.serve().await;
    assert_eq!(r.inflight_keys(), [format!("{id}/2")]);
    r.run_nested().await;
    r.serve().await;

    let answer = call.await.unwrap().expect("the component answered");
    assert_eq!(answer["text"], serde_json::Value::Null);
    assert_eq!(answer["snippet"], "Hi Bob, The numbers are in & attached");
    let omitted = answer["text_omitted"].as_str().unwrap();
    let why = omitted
        .strip_suffix("; sizeEstimate 9000; headers and attachments are listed, the body is not")
        .unwrap_or_else(|| panic!("{omitted}"));
    assert_unreadable(Unreadable::Refused, why, full_len);
    assert_eq!(r.seen().len(), 2);
    r.assert_finished(&id);
}
