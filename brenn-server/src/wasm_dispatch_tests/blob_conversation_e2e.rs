//! A conversation's tool call carrying blobs, through a compiled provider.
//!
//! `processor-blob-provider-test` provides `blob-echo` (`BlobEcho`), which
//! accepts attachments; the same provider is also declared to provide
//! `plain-echo` (`PlainEcho`), which does not. Conversations call them through
//! a real bridge, on the PostToolUse hook path and through the tool socket
//! with the real `brenn-tool`. App `pa` is granted both tools and so holds its
//! result inbox; app `watcher` is granted none, and its policy covers
//! `tool-results/` and `tool-requests/` and lets it subscribe at runtime. A
//! producer publishes the blobs a conversation is shown on `brenn:src`. The
//! provider is stepped by hand; a conversation's call runs on a task of its
//! own.

use super::*;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Stdio;

use brenn_envelope::BlobId;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::app_tool_result_inbox_address;
use brenn_lib::messaging::config::{ProvidedToolDecl, Sink};
use brenn_lib::tools::{ResolvedRateLimit, ResolvedToolGrant};
use brenn_messaging::testutils::{publish_attached, redeem_all, seal_blob, test_channel_entry};
use brenn_messaging_store::db::bus_gc_evict_channel;
use brenn_tool_proto::SOCKET_ENV;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::provider_rig::{ConversationSetup, ProviderRig, ProviderSetup};
use crate::active_bridge::ActiveBridge;

const PROVIDER_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../brenn-wasm/target/components/brenn_processor_blob_provider_test.wasm"
);

/// The `brenn-tool` binary built with this server.
const BIN: &str = env!("BRENN_TOOL_BIN");

/// The providing instance.
const PROVIDER: &str = "prov";
/// The app granted both tools.
const APP: &str = "pa";
/// The app that watches the tool substrate and is granted no tool.
const WATCHER: &str = "watcher";
const PRODUCER: &str = "blob-producer";
const SRC: &str = "brenn:src";
const DST: &str = "brenn:dst";
/// The provider's request inbox and its component result inbox.
const PROVIDER_REQUESTS: &str = "brenn:tool-requests/prov";
const PROVIDER_RESULTS: &str = "brenn:tool-results/prov";

/// The arguments the fixture's modes read, as a conversation's schema names
/// them: a conversation's call is refused any argument its schema omits.
fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "mode": { "type": "string" },
            "name": { "type": "string" },
            "hex": { "type": "string" },
            "content_type": { "type": "string" },
            "as": { "type": "string" },
        },
        "required": ["mode"],
    })
}

fn decl(name: &str, mcp_name: &str, accepts_attachments: bool) -> ProvidedToolDecl {
    ProvidedToolDecl {
        name: name.to_string(),
        mcp_name: mcp_name.to_string(),
        description: "Test fixture: answer by the `mode` argument.".to_string(),
        input_schema: schema(),
        max_concurrency: 4,
        auto_approve: true,
        accepts_attachments,
    }
}

/// An app that reads and publishes `brenn:src` and `brenn:dst`, edited by
/// `edit`.
fn app(slug: &str, edit: impl FnOnce(&mut AppPolicy)) -> AppConfig {
    let mut cfg = crate::test_support::app_config::default_test_app_config(slug, slug);
    let policy = cfg.policy_mut();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.grants.insert(AppCapability::MessagingPublish);
    for name in ["src", "dst"] {
        policy
            .acls
            .brenn_subscribe
            .push(ChannelMatcher::Exact(name.to_string()));
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(name.to_string()));
    }
    edit(policy);
    cfg
}

struct Rig {
    base: ProviderRig,
    apps: Arc<IndexMap<String, AppConfig>>,
}

impl std::ops::Deref for Rig {
    type Target = ProviderRig;

    fn deref(&self) -> &ProviderRig {
        &self.base
    }
}

/// Stand up the rig; `pa`'s two grants carry `rate_limit`.
async fn rig_with(rate_limit: Option<ResolvedRateLimit>) -> Rig {
    let granted = ResolvedToolGrant {
        acl: vec![],
        rate_limit,
    };
    let apps = IndexMap::from([
        (
            APP.to_string(),
            app(APP, |policy| {
                for tool in ["blob-echo", "plain-echo"] {
                    policy.tool_grants.insert(tool.to_string(), granted.clone());
                }
            }),
        ),
        (
            WATCHER.to_string(),
            app(WATCHER, |policy| {
                policy.grants.insert(AppCapability::DynamicSubscribe);
                for prefix in ["tool-results/", "tool-requests/"] {
                    policy
                        .acls
                        .brenn_subscribe
                        .push(ChannelMatcher::Prefix(prefix.to_string()));
                }
            }),
        ),
    ]);
    let base = ProviderRig::build(ProviderSetup {
        slug: PROVIDER,
        decls: vec![
            decl("blob-echo", "BlobEcho", true),
            decl("plain-echo", "PlainEcho", false),
        ],
        native: vec![],
        grants: BTreeMap::new(),
        component: ProcessorLoadSpec {
            grants: [
                ComponentGrant::Log,
                ComponentGrant::Tools,
                ComponentGrant::Provides,
                ComponentGrant::Blobs,
            ]
            .into_iter()
            .collect(),
            ..ProcessorLoadSpec::minimal(Path::new(PROVIDER_WASM), PROVIDER)
        },
        caller: None,
        conversations: Some(ConversationSetup {
            apps: apps.clone(),
            channels: ["src", "dst"]
                .into_iter()
                .map(|name| test_channel_entry(name, vec![]))
                .collect(),
            producer: PRODUCER,
        }),
        blob_limits: None,
        limits: None,
    })
    .await;
    Rig {
        base,
        apps: Arc::new(apps),
    }
}

async fn rig() -> Rig {
    rig_with(None).await
}

impl Rig {
    /// A fresh conversation of `app_slug` acting for `username`.
    async fn conversation(&self, app_slug: &str, username: &str) -> Arc<ActiveBridge> {
        ActiveBridge::test_new_for_conversation(
            self.messenger.clone(),
            self.apps.clone(),
            app_slug,
            username,
            self.registry.clone(),
        )
        .await
    }

    /// A message from the producer on `brenn:src` carrying `bytes` as
    /// `scan.pdf`, and the blob's id.
    async fn produce(&self, bytes: &[u8]) -> (Uuid, BlobId) {
        let hold = seal_blob(&self.messenger, bytes);
        let id = hold.id().clone();
        let message = publish_attached(
            &self.messenger,
            PRODUCER,
            SRC,
            "a scan",
            &[("scan.pdf", &hold)],
            None,
        )
        .await;
        (message, id)
    }

    /// The rows of `address`, oldest first: each row's attachment ids by name.
    async fn rows_on(&self, address: &str) -> Vec<BTreeMap<String, String>> {
        testutils::retained_on(&self.messenger, address)
            .await
            .iter()
            .map(|(_, envelope)| testutils::attachment_ids(envelope))
            .collect()
    }

    /// Nothing is owed to the provider: no request reached its inbox.
    async fn provider_owes_nothing(&self) {
        assert!(
            testutils::owed_everywhere(&self.messenger, &self.provider_sub)
                .await
                .is_empty(),
            "a request reached the provider"
        );
    }

    fn sealed(&self) -> usize {
        self.messenger
            .blob_store()
            .expect("a blob store")
            .usage()
            .sealed
    }

    fn blobs(&self) -> &brenn_messaging::testutils::TempBlobDir {
        self.base.blobs.as_ref().expect("a rig with conversations")
    }
}

/// The `attachments` argument handing `name` of `message_id` as `doc`.
fn hand(message_id: Uuid, name: &str) -> Value {
    json!({ "doc": { "message_id": message_id.to_string(), "name": name } })
}

/// Run `tool` on the hook path for `conversation`, on a task of its own.
fn post(conversation: &Arc<ActiveBridge>, tool: &str, input: Value) -> JoinHandle<Value> {
    let conversation = conversation.clone();
    let tool = tool.to_string();
    tokio::spawn(async move { conversation.test_post_tool_use(&tool, input).await })
}

/// Run `tool` on the hook path for `conversation` and wait for its answer:
/// for a tool that never reaches the provider.
async fn post_now(conversation: &Arc<ActiveBridge>, tool: &str, input: Value) -> Value {
    conversation.test_post_tool_use(tool, input).await
}

struct Run {
    code: Option<i32>,
    stdout: Value,
}

/// Run `brenn-tool` with `args` against `conversation`'s socket, on a task of
/// its own, and parse the one JSON value it prints.
fn cli(conversation: &ActiveBridge, args: &[&str]) -> JoinHandle<Run> {
    let socket = conversation.test_tool_socket_path();
    let args: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
    tokio::spawn(async move {
        let out = tokio::process::Command::new(BIN)
            .args(&args)
            .env(SOCKET_ENV, &socket)
            .stdin(Stdio::null())
            .output()
            .await
            .expect("spawn brenn-tool");
        let stdout = String::from_utf8(out.stdout).expect("stdout is UTF-8");
        let stdout = serde_json::from_str(&stdout).unwrap_or_else(|e| {
            panic!(
                "stdout is not one JSON value ({e}): {stdout:?}; stderr: {}",
                String::from_utf8_lossy(&out.stderr)
            )
        });
        Run {
            code: out.status.code(),
            stdout,
        }
    })
}

/// `brenn-tool call <tool> <input>`, on a task of its own.
fn cli_call(conversation: &ActiveBridge, tool: &str, input: &Value) -> JoinHandle<Run> {
    cli(conversation, &["call", tool, &input.to_string()])
}

/// Read `address` with `MessageChannelGet` as `conversation`.
async fn read(conversation: &Arc<ActiveBridge>, address: &str) -> Value {
    post_now(
        conversation,
        "MessageChannelGet",
        json!({ "address": address, "limit": 50 }),
    )
    .await
}

/// Assert `answer` is a naming refusal of kind `reason`.
fn refused(answer: &Value, reason: &str) {
    assert_eq!(answer["ok"], json!(false), "expected a refusal: {answer}");
    assert_eq!(answer["reason"], json!(reason), "wrong reason: {answer}");
}

/// The message id of a handed result.
fn message_id(answer: &Value) -> Uuid {
    answer["message_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a handed result: {answer}"))
        .parse()
        .expect("a uuid")
}

/// `BlobEcho` in `make` mode: the provider answers with a blob holding
/// `bytes`, as `out.txt`.
fn make(bytes: &[u8]) -> Value {
    json!({ "mode": "make", "hex": hex::encode(bytes), "content_type": "text/plain", "as": "out.txt" })
}

/// Call `BlobEcho` to make `bytes` on the hook path and serve it: the handed
/// result.
async fn made(r: &Rig, conversation: &Arc<ActiveBridge>, bytes: &[u8]) -> Value {
    let call = post(conversation, "BlobEcho", make(bytes));
    r.requested_id().await;
    r.serve().await;
    call.await.expect("the call's task")
}

/// The bare value, on both entries: a conversation hands the provider
/// an attachment of a message it read; the request carries the blob it names
/// and the provider's `args` have no `attachments`; the provider reads its
/// bytes; an answer with no attachments is the bare value and publishes
/// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversation_hands_the_provider_a_blob_on_both_entries() {
    let r = rig().await;
    let conversation = r.conversation(APP, "alice").await;
    let bytes = b"%PDF-1.7 handed bytes";
    let (source, blob) = r.produce(bytes).await;
    read(&conversation, SRC).await;
    let sealed = r.sealed();
    let input = json!({ "mode": "read", "name": "doc", "attachments": hand(source, "scan.pdf") });
    let expected = json!({
        "range": hex::encode(bytes),
        "size": bytes.len(),
        "content_type": "application/octet-stream",
    });

    let call = post(&conversation, "BlobEcho", input.clone());
    let requests = r.owed(&r.provider_sub, 1).await;
    assert_eq!(
        requests[0]["args"],
        json!({ "mode": "read", "name": "doc" })
    );
    assert_eq!(
        r.rows_on(PROVIDER_REQUESTS).await,
        vec![BTreeMap::from([(
            "doc".to_string(),
            blob.as_str().to_string()
        )])],
        "the request names the blob the conversation handed over"
    );
    r.serve().await;
    assert_eq!(call.await.unwrap(), expected);

    let call = cli_call(&conversation, "BlobEcho", &input);
    r.requested_id().await;
    r.serve().await;
    let run = call.await.unwrap();
    assert_eq!(run.code, Some(0), "{}", run.stdout);
    assert_eq!(run.stdout, expected);

    assert!(
        r.rows_on(&app_tool_result_inbox_address(APP))
            .await
            .is_empty(),
        "an answer with no attachments publishes nothing"
    );
    assert_eq!(r.sealed(), sealed, "no byte was copied");
}

/// On both entries: the provider's answer carries a blob it made; the
/// conversation is shown `{"result", "message_id", "attachments"}` with no
/// blob id, forwards the attachment with `BrennSend` (the same blob), and
/// saves it with `brenn-tool save` (the same bytes).
#[tokio::test(flavor = "multi_thread")]
async fn a_providers_blob_reaches_the_conversation_on_both_entries() {
    let r = rig().await;
    let conversation = r.conversation(APP, "alice").await;
    let inbox = app_tool_result_inbox_address(APP);
    let shown = json!({ "out.txt": { "size": 14, "content_type": "text/plain" } });

    let hooked = made(&r, &conversation, b"made on a hook").await;
    let call = cli_call(&conversation, "BlobEcho", &make(b"made by socket"));
    r.requested_id().await;
    r.serve().await;
    let socketed = call.await.unwrap();
    assert_eq!(socketed.code, Some(0), "{}", socketed.stdout);

    let rows = r.rows_on(&inbox).await;
    assert_eq!(rows.len(), 2, "one inbox message per handed result");
    let dir = tempfile::tempdir().unwrap();
    for ((answer, bytes), row) in [
        (&hooked, &b"made on a hook"[..]),
        (&socketed.stdout, &b"made by socket"[..]),
    ]
    .into_iter()
    .zip(&rows)
    {
        assert_eq!(answer["result"], json!({ "made": 14 }), "{answer}");
        assert_eq!(answer["attachments"], shown, "{answer}");
        let made = &row["out.txt"];
        assert!(
            !answer.to_string().contains(made.as_str()),
            "a blob id leaked: {answer}"
        );
        let id = message_id(answer);

        let sent = cli_call(
            &conversation,
            "BrennSend",
            &json!({ "to": DST, "body": "fwd", "attachments": {
                "copy.txt": { "message_id": id.to_string(), "name": "out.txt" },
            }}),
        )
        .await
        .unwrap();
        assert_eq!(sent.code, Some(0), "{}", sent.stdout);
        let forwarded = r.rows_on(DST).await;
        assert_eq!(
            forwarded.last().and_then(|row| row.get("copy.txt")),
            Some(made),
            "the forward names the blob the provider made"
        );

        let copy = dir.path().join(format!("{id}.txt"));
        let saved = cli(
            &conversation,
            &[
                "save",
                &id.to_string(),
                "--attachment",
                "out.txt",
                "--out",
                &copy.display().to_string(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(saved.code, Some(0), "{}", saved.stdout);
        assert_eq!(std::fs::read(&copy).unwrap(), bytes);
    }
}

/// Conversation-side refusals, on both entries: a name the
/// conversation was not shown, 33 attachments, a malformed attachment name and
/// a malformed message id each draw a naming refusal before the provider's
/// inbox or the grant's rate bucket is reached; `attachments` on a tool that
/// does not accept them is an unknown argument.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_name_never_reaches_the_provider_or_the_rate_bucket() {
    let r = rig_with(Some(ResolvedRateLimit {
        burst: 1,
        sustained_per_minute: 1,
    }))
    .await;
    let conversation = r.conversation(APP, "alice").await;
    let (unread, _) = r.produce(b"never read").await;
    let many: serde_json::Map<String, Value> = (0..33)
        .map(|i| {
            (
                format!("a{i}"),
                json!({ "message_id": unread.to_string(), "name": "scan.pdf" }),
            )
        })
        .collect();
    let cases = [
        (hand(unread, "scan.pdf"), "not_read"),
        (Value::Object(many), "too_many_attachments"),
        (
            json!({ "../doc": { "message_id": unread.to_string(), "name": "scan.pdf" } }),
            "malformed_attachment_name",
        ),
        (
            json!({ "doc": { "message_id": "nope", "name": "scan.pdf" } }),
            "malformed_message_id",
        ),
    ];
    for (attachments, reason) in cases {
        let input = json!({ "mode": "read", "name": "doc", "attachments": attachments });
        refused(
            &post_now(&conversation, "BlobEcho", input.clone()).await,
            reason,
        );
        let run = cli_call(&conversation, "BlobEcho", &input).await.unwrap();
        assert_eq!(run.code, Some(1), "{}", run.stdout);
        refused(&run.stdout, reason);
    }

    let input = json!({ "mode": "read", "attachments": hand(unread, "scan.pdf") });
    let answer = post_now(&conversation, "PlainEcho", input.clone()).await;
    assert_eq!(answer["error"], json!("invalid_args"), "{answer}");
    assert!(
        answer["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("unknown argument")),
        "{answer}"
    );
    let run = cli_call(&conversation, "PlainEcho", &input).await.unwrap();
    assert_eq!(run.code, Some(2), "{}", run.stdout);
    assert_eq!(run.stdout["error"], json!("invalid_args"), "{}", run.stdout);
    r.provider_owes_nothing().await;

    // A spent bucket would hold this call for the minute a token takes to
    // refill.
    let call = post(&conversation, "BlobEcho", make(b"after the refusals"));
    tokio::time::timeout(std::time::Duration::from_secs(30), r.requested_id())
        .await
        .expect("the bucket's one call was not spent: the call does not wait for a token");
    r.serve().await;
    assert_eq!(call.await.unwrap()["result"], json!({ "made": 18 }));
}

/// A result's attachment is nameable only by the conversation it was
/// handed to. A second conversation of the same app, and a conversation of an
/// app whose policy covers `tool-results/` and may subscribe at runtime, are
/// refused it `not_read`; neither lists the inbox, reads it, or (the watcher)
/// subscribes to it. The watcher still lists and reads a component's
/// result inbox.
#[tokio::test(flavor = "multi_thread")]
async fn a_results_attachment_is_for_the_conversation_it_was_handed_to() {
    let r = rig().await;
    let first = r.conversation(APP, "alice").await;
    let answer = made(&r, &first, b"first's result").await;
    let id = message_id(&answer);
    let inbox = app_tool_result_inbox_address(APP);
    let forward = json!({ "to": DST, "body": "fwd", "attachments": {
        "copy.txt": { "message_id": id.to_string(), "name": "out.txt" },
    }});

    let second = r.conversation(APP, "bob").await;
    let watcher = r.conversation(WATCHER, "carol").await;
    let unknown = read(&second, "brenn:no-such-channel").await;
    for other in [&second, &watcher] {
        refused(
            &post_now(other, "BrennSend", forward.clone()).await,
            "not_read",
        );
        let listed = post_now(other, "MessageChannelList", json!({})).await;
        assert!(!listed.to_string().contains(&inbox), "{listed}");
        let got = read(other, &inbox).await;
        assert_eq!(
            got.to_string().replace(&inbox, "brenn:no-such-channel"),
            unknown.to_string(),
            "the inbox reads as an unknown channel"
        );
    }
    // Refused by its policy, and in the same words as an inbox no app has:
    // the answer does not say whether the inbox exists.
    let subscribe =
        |address: &str| json!({ "address": address, "push_depth": 0, "retain_depth": 1 });
    let subscribed = post_now(&watcher, "MessageSubscribe", subscribe(&inbox)).await;
    assert_eq!(subscribed["ok"], json!(false), "{subscribed}");
    assert_eq!(
        subscribed["error"],
        json!(format!(
            "subscribe to {inbox:?} denied by this app's access policy"
        )),
        "{subscribed}"
    );
    let absent = app_tool_result_inbox_address("no-such-app");
    let nowhere = post_now(&watcher, "MessageSubscribe", subscribe(&absent)).await;
    assert_eq!(
        subscribed.to_string().replace(&inbox, &absent),
        nowhere.to_string(),
        "the inbox subscribes as one no app has"
    );
    assert!(r.rows_on(DST).await.is_empty(), "nothing was forwarded");

    let listed = post_now(&watcher, "MessageChannelList", json!({})).await;
    assert!(listed.to_string().contains(PROVIDER_RESULTS), "{listed}");
    assert!(read(&watcher, PROVIDER_RESULTS).await.is_array());
}

/// A watcher of `tool-requests/` reads the request another conversation's
/// call put on the provider's inbox: the arguments and the attachment's name,
/// size and content type, and no blob id. Naming the attachment is
/// `tool_channel_attachment`; the provider still reads the bytes and the
/// call completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_watcher_sees_a_requests_attachment_and_cannot_name_it() {
    let r = rig().await;
    let caller = r.conversation(APP, "alice").await;
    let bytes = b"handed to the provider";
    let (source, blob) = r.produce(bytes).await;
    read(&caller, SRC).await;
    let call = post(
        &caller,
        "BlobEcho",
        json!({ "mode": "read", "name": "doc", "attachments": hand(source, "scan.pdf") }),
    );
    r.requested_id().await;

    let watcher = r.conversation(WATCHER, "carol").await;
    let got = read(&watcher, PROVIDER_REQUESTS).await;
    let messages = got.as_array().unwrap_or_else(|| panic!("a read: {got}"));
    assert_eq!(messages.len(), 1, "{got}");
    let request = &messages[0];
    let body: Value =
        serde_json::from_str(request["body"].as_str().expect("a text body")).expect("JSON");
    assert_eq!(body["args"], json!({ "mode": "read", "name": "doc" }));
    assert_eq!(
        request["attachments"],
        json!({ "doc": { "size": bytes.len(), "content_type": "application/octet-stream" } })
    );
    assert!(
        !got.to_string().contains(blob.as_str()),
        "a blob id leaked: {got}"
    );
    let request_id: Uuid = request["message_id"]
        .as_str()
        .expect("a message id")
        .parse()
        .expect("a uuid");
    refused(
        &post_now(
            &watcher,
            "BrennSend",
            json!({ "to": DST, "body": "fwd", "attachments": {
                "copy.pdf": { "message_id": request_id.to_string(), "name": "doc" },
            }}),
        )
        .await,
        "tool_channel_attachment",
    );
    assert!(r.rows_on(DST).await.is_empty(), "nothing was forwarded");

    r.serve().await;
    assert_eq!(call.await.unwrap()["range"], json!(hex::encode(bytes)));
}

/// The result inbox: once the inbox holds its standing depth of later
/// results, the first result's attachment is `not_retained`, for a forward
/// and for a tool call (which never reaches the provider). After a GC pass
/// and a reap its blob file is gone and the later ones' remain.
#[tokio::test(flavor = "multi_thread")]
async fn a_result_past_the_inbox_window_is_not_retained_and_reaped() {
    let r = rig().await;
    let conversation = r.conversation(APP, "alice").await;
    let inbox = app_tool_result_inbox_address(APP);
    let entry = r
        .messenger
        .directory()
        .resolve(&inbox)
        .expect("the app's result inbox");
    let depth = match entry.resolved_channel.standing_retain_depth {
        Depth::Bounded(depth) => depth,
        Depth::Unbounded => panic!("a result inbox has a bounded standing depth"),
    };

    let mut ids = Vec::new();
    for i in 0..=depth {
        let answer = made(&r, &conversation, format!("result {i:04}").as_bytes()).await;
        ids.push(message_id(&answer));
    }
    let made: Vec<String> = r
        .rows_on(&inbox)
        .await
        .into_iter()
        .map(|row| row["out.txt"].clone())
        .collect();
    assert_eq!(made.len(), ids.len());

    refused(
        &post_now(
            &conversation,
            "BrennSend",
            json!({ "to": DST, "body": "fwd", "attachments": {
                "copy.txt": { "message_id": ids[0].to_string(), "name": "out.txt" },
            }}),
        )
        .await,
        "not_retained",
    );
    refused(
        &post_now(
            &conversation,
            "BlobEcho",
            json!({ "mode": "read", "name": "doc", "attachments": hand(ids[0], "out.txt") }),
        )
        .await,
        "not_retained",
    );
    r.provider_owes_nothing().await;
    let second = redeem_all(&r.messenger, &BlobId::new(made[1].clone()))
        .await
        .expect("a retained result's blob");
    assert_eq!(second, b"result 0001");

    {
        let conn = r.messenger.db().lock().await;
        bus_gc_evict_channel(
            &conn,
            entry.uuid,
            &entry.address,
            entry.transport_type,
            depth,
            Sink::Drop,
            None,
        );
    }
    r.messenger.reap_blobs().await;
    let files: Vec<bool> = made
        .iter()
        .map(|id| r.blobs().holds_file(&BlobId::new(id.clone())))
        .collect();
    assert!(!files[0], "the first result's blob was reaped");
    assert!(files[1..].iter().all(|held| *held), "{files:?}");
}

/// A reload race: the app's result inbox leaves the directory while a call
/// is in flight. The call is answered `internal` and the provider's blob goes
/// with the answer.
#[tokio::test(flavor = "multi_thread")]
async fn an_inbox_removed_during_a_call_answers_internal() {
    let r = rig().await;
    let conversation = r.conversation(APP, "alice").await;
    let sealed = r.sealed();
    let call = post(&conversation, "BlobEcho", make(b"never delivered"));
    r.requested_id().await;
    let inbox = r
        .messenger
        .directory()
        .resolve(&app_tool_result_inbox_address(APP))
        .expect("the app's result inbox");
    assert!(r.messenger.directory().remove_channel(&inbox.uuid));

    r.serve().await;
    let answer = call.await.unwrap();
    assert_eq!(answer["error_type"], json!("internal"), "{answer}");
    assert_eq!(
        answer["error"],
        json!(
            "the result's attachments could not be delivered: this app has no tool-result \
             channel now"
        )
    );
    assert_eq!(
        r.sealed(),
        sealed,
        "the provider's blob went with the answer"
    );
}
