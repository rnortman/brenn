//! The `google-workspace-tools` component, end to end.
//!
//! The compiled component serves its provided tools through the one registry:
//! an app conversation acting for alice calls a tool, the request lands on the
//! component's `brenn:tool-requests/<slug>` inbox, the component issues its
//! nested `api-request`s through a real `WasmProvidesHost`, the executor runs
//! each against the OAuth test stub with the credential alice connected, and
//! the result comes back on the component's `brenn:tool-results/<slug>` inbox.
//! The Gmail host the component's `config` names is the stub, which answers by
//! request path. The component and the executor are stepped by hand; the
//! conversation's call runs on a spawned task.

use super::*;

use std::collections::{BTreeMap, HashMap};

use brenn_lib::config::{PACKAGED, config_from_dsl};
use brenn_lib::messaging::config::ProvidedToolDecl;
use brenn_messaging::MountDebt;
use brenn_oauth::testutil;
use brenn_tool_registry::ToolError;
use serde_json::json;

use super::provider_rig::CallerSetup;
use super::stub_rig::{self, Pairs, StubProvider, StubRig, assert_could_not_be_issued, pairs};

const WORKSPACE_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_google_workspace_tools.wasm"
);

/// The component's packaged specification.
const WORKSPACE_SPEC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../config/specs/google-workspace-tools.brenn"
);

/// The providing instance.
const PROVIDER: &str = "workspace";
/// The component that calls a provided tool on its own account.
const CALLER: &str = "agenda";
/// The OAuth provider the instance's `config` names.
const OAUTH_PROVIDER: &str = "google";

/// Alice's connected account, and the bearer its credential holds.
const ACCOUNT: &str = "alice@example.com";
const BEARER: &str = "AT-alice";
/// A second account alice connected, and its bearer.
const WORK_ACCOUNT: &str = "alice.work@example.com";
const WORK_BEARER: &str = "AT-alice-work";

const LABELS: &str = "/gmail/v1/users/me/labels";
const MESSAGES: &str = "/gmail/v1/users/me/messages";
const THREADS: &str = "/gmail/v1/users/me/threads";

/// The instance's `api-request` clauses, as exact path, each a `GET` on the
/// stub's host for any user: one per endpoint the tools built so far call.
const CLAUSES: &[&str] = &[
    LABELS,
    "/gmail/v1/users/me/labels/*",
    MESSAGES,
    "/gmail/v1/users/me/messages/*",
    "/gmail/v1/users/me/threads/*",
];

/// The component's `provide` statements, lowered from its packaged
/// specification under one instance of its class.
fn provided() -> Vec<ProvidedToolDecl> {
    let spec = std::fs::read_to_string(WORKSPACE_SPEC)
        .unwrap_or_else(|e| panic!("read {WORKSPACE_SPEC}: {e}"));
    let config = config_from_dsl(&format!(
        r#"{PACKAGED}{spec}{PACKAGED}
new {PROVIDER}: GoogleWorkspaceTools {{
    grants = [log, store, config, tools, provides];
    store_path = "/var/lib/brenn/google-workspace-tools.db";
    tool api-request {{}}
}}
"#
    ));
    config
        .wasm_consumers
        .into_iter()
        .find(|consumer| consumer.slug == PROVIDER)
        .expect("the instance lowers")
        .provides
}

/// The accounts alice has connected: the account and its access token.
type Connected<'a> = &'a [(&'a str, &'a str)];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    rig_with(CLAUSES, &[(ACCOUNT, BEARER)], None).await
}

/// Stand up the rig: the provider's `api-request` grant holds `clauses`,
/// alice has connected `connected`, and `caller` is the calling component, if
/// any.
async fn rig_with(
    clauses: &[&str],
    connected: Connected<'_>,
    caller: Option<CallerSetup<'_>>,
) -> StubRig {
    rig_configured(clauses, connected, caller, |_| {}).await
}

/// The rig with every clause and alice's one account connected, `api-request`
/// reading at most `max_response_bytes` of each of Google's answers.
async fn rig_reading_at_most(max_response_bytes: usize) -> StubRig {
    rig_configured(CLAUSES, &[(ACCOUNT, BEARER)], None, |provider| {
        provider.raw.max_response_bytes = max_response_bytes;
    })
    .await
}

/// [`rig_with`], with `configure` applied to the OAuth provider's
/// configuration before the service is built.
async fn rig_configured(
    clauses: &[&str],
    connected: Connected<'_>,
    caller: Option<CallerSetup<'_>>,
    configure: impl FnOnce(&mut brenn_lib::config::OAuthProviderConfig),
) -> StubRig {
    let started = stub_rig::start().await;
    let host = started.host.clone();
    let (name, mut provider) = testutil::provider_config(OAUTH_PROVIDER, &started.base);
    configure(&mut provider);
    let (service, vault) = testutil::service(vec![(name, provider)]);
    for (account, bearer) in connected {
        let mut credential = testutil::credential(OAUTH_PROVIDER, account, "RT-alice");
        credential.access_token = bearer.to_string();
        service.vault().store("alice", &credential).await;
    }
    StubRig::build(
        started,
        StubProvider {
            slug: PROVIDER,
            wasm: WORKSPACE_WASM,
            decls: provided(),
            native: brenn_oauth::tools::registered(Some(&service), BTreeMap::new()),
            clauses: clauses
                .iter()
                .map(|&path| {
                    vec![
                        ("provider", OAUTH_PROVIDER),
                        ("user", "*"),
                        ("method", "GET"),
                        ("path", path),
                    ]
                })
                .collect(),
            config: HashMap::from([
                ("provider".to_string(), OAUTH_PROVIDER.to_string()),
                ("gmail_host".to_string(), host),
            ]),
            caller,
            vault,
        },
    )
    .await
}

/// The query pairs a Gmail call sends: `expected` plus the `prettyPrint=false`
/// every Gmail call carries.
fn gmail_query(expected: &[(&str, &str)]) -> Pairs {
    pairs(&[expected, &[("prettyPrint", "false")]].concat())
}

/// labels.list's query as the `GmailLabels` listing sends it.
fn labels_list_query() -> Pairs {
    gmail_query(&[("fields", "labels(id,name,type)")])
}

/// messages.get's metadata query for one search line: the three
/// `metadataHeaders`, each its own query key.
fn metadata_query() -> Pairs {
    gmail_query(&[
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

/// Google's 403 for a token without the Gmail read scope.
fn insufficient_scope_body() -> Vec<u8> {
    json!({
        "error": {
            "code": 403,
            "message": "Request had insufficient authentication scopes.",
            "status": "PERMISSION_DENIED",
            "details": [{ "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT" }],
        },
    })
    .to_string()
    .into_bytes()
}

/// The tool's `denied` for Google's 403 over [`insufficient_scope_body`].
fn scope_denied() -> Result<serde_json::Value, ToolError> {
    Err(ToolError::Denied(
        "Google PERMISSION_DENIED (HTTP 403): Request had insufficient authentication scopes. \
         If Google names insufficient authentication scopes, the account was connected without \
         the Gmail read scope: run oauth-connect with that scope set and relay its steps."
            .to_string(),
    ))
}

/// messages.list's query for `q` at the default page size.
fn search_list_query(q: &str) -> Pairs {
    gmail_query(&[
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
            gmail_query(&[(
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
                gmail_query(&[
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
                gmail_query(&[("fields", "labels(id,name)")]),
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
            gmail_query(&[
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
    r.fail(&format!("{MESSAGES}/m1"), 403, insufficient_scope_body());
    let call = r.call_for_alice("GmailSearch", json!({ "q": "is:unread" }));
    let id = r.requested_id().await;
    r.run(2).await;

    assert_eq!(call.await.unwrap(), scope_denied());
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

/// With no account connected, `api-request`'s `credential_invalid` value is
/// the tool's answer unchanged, and the component makes no further call.
#[tokio::test]
async fn no_connected_account_is_relayed_as_credential_invalid() {
    let r = rig_with(CLAUSES, &[], None).await;
    let call = r.call_for_alice("GmailSearch", json!({ "q": "is:unread" }));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({
            "error": "credential_invalid",
            "provider": OAUTH_PROVIDER,
            "account": null,
            "detail": "no account is connected for this provider",
        }))
    );
    r.owed(&r.executor_sub(), 0).await;
    assert!(
        r.seen().is_empty(),
        "no API was called without a credential"
    );
    r.assert_finished(&id);
}

/// With two accounts connected, a call naming none is `denied` with
/// `api-request`'s list of them, and a call naming one sends that account's
/// bearer.
#[tokio::test]
async fn several_accounts_need_one_named() {
    let r = rig_with(
        CLAUSES,
        &[(ACCOUNT, BEARER), (WORK_ACCOUNT, WORK_BEARER)],
        None,
    )
    .await;
    r.answer(LABELS, labels_body());

    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;
    r.run(1).await;
    let answer = call.await.unwrap();
    let Err(ToolError::Denied(detail)) = answer else {
        panic!("expected a denied answer, got {answer:?}");
    };
    assert!(
        detail.starts_with(&format!(
            "api-request refused the nested GET {}{LABELS}: several \"google\" accounts are \
             connected; name one as `account`: ",
            r.host
        )) && detail.contains(ACCOUNT)
            && detail.contains(WORK_ACCOUNT),
        "{detail}"
    );
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);

    let call = r.call_for_alice("GmailLabels", json!({ "account": WORK_ACCOUNT }));
    let id = r.requested_id().await;
    r.run(1).await;
    call.await.unwrap().expect("the named account answered");
    assert_eq!(r.bearers(), [format!("Bearer {WORK_BEARER}")]);
    r.assert_finished(&id);
}

/// A component calling `gmail-labels` on its own account has no user, and an
/// instance whose clauses say `user = "*"` lends none: the caller's result is
/// `denied` carrying `api-request`'s explanation, and nothing reaches the API.
#[tokio::test]
async fn a_userless_caller_is_denied_with_the_reason() {
    let r = rig_with(
        CLAUSES,
        &[(ACCOUNT, BEARER)],
        Some(CallerSetup {
            slug: CALLER,
            tool: "gmail-labels",
            args: json!({}),
        }),
    )
    .await;
    let caller = r.caller();
    testutils::insert_bus_message(&r.messenger, &caller.trigger, "go", ChannelScheme::Brenn).await;
    drain_step(&caller.component, &caller.sub, MountDebt::Settled).await;

    // The executor hands the call to the provider, which issues its `GET`.
    r.executor.drain_step().await;
    let id = r.requested_id().await;
    r.serve().await;
    let nested = r.owed(&r.executor_sub(), 1).await;
    assert_eq!(nested[0]["serving"], id.as_str());

    r.executor.drain_step().await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.owed(&r.provider_sub, 1).await;
    r.serve().await;

    let results = r.owed(&caller.sub, 1).await;
    assert_eq!(results[0]["tool"], "gmail-labels");
    let error = &results[0]["outcome"]["err"];
    assert_eq!(error["kind"], "denied", "{}", results[0]);
    let detail = error["detail"].as_str().expect("a detail");
    assert!(
        detail.starts_with(&format!(
            "api-request refused the nested GET {}{LABELS}: ",
            r.host
        )) && detail.contains("a caller with no acting user borrows the one user"),
        "{detail}"
    );
    r.assert_finished(&id);
}

/// Google's 403 is the tool's `denied`: Google's status and message, and the
/// sentence telling the assistant to reconnect with the read scope.
#[tokio::test]
async fn a_google_403_is_denied_with_the_scope_hint() {
    let r = rig().await;
    r.fail(LABELS, 403, insufficient_scope_body());
    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied());
    r.assert_finished(&id);
}

/// A nested call the instance's grant does not admit is refused when it is
/// queued: the request is answered `internal` with Brenn's text and the clause
/// guidance, and nothing reaches the API.
#[tokio::test]
async fn a_clause_miss_is_internal_could_not_be_issued() {
    let clauses: Vec<&str> = CLAUSES
        .iter()
        .copied()
        .filter(|path| *path != LABELS)
        .collect();
    let r = rig_with(&clauses, &[(ACCOUNT, BEARER)], None).await;
    let call = r.call_for_alice("GmailLabels", json!({}));
    let id = r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);
}

fn full_read_query() -> Pairs {
    gmail_query(&[("format", "full")])
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
                gmail_query(&[("fields", "labels(id,name)")]),
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
    r.fail(
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
                gmail_query(&[
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
                gmail_query(&[
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
                gmail_query(&[("fields", "labels(id,name)")]),
            ),
        ]
    );
    assert_eq!(r.bearers(), vec![format!("Bearer {BEARER}"); 2]);
    r.assert_finished(&id);
}
