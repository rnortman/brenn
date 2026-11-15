//! Blobs carried by registry tool calls, through a booted process: two real
//! components, the provider runtime, the tool executor and a blob store.
//!
//! The document declares the `processor-blob-provider-test` artifact as a
//! providing class serving `blob-echo` (which accepts attachments), granted the
//! host tool `attachment-echo` for its nested calls, and the
//! `processor-blob-caller-test` artifact as a caller granted `blob-echo`. A
//! trigger on the work channel tells the caller which call to make and which
//! blobs to carry; the caller reports each result it reads back, bytes as hex,
//! on the sink channel.
//!
//! The reload case is about the one channel the feature adds to the plan: an
//! app's result inbox, minted when an app is granted its first asynchronous
//! tool, and the tuning blocks that may and may not size it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use brenn_lib::config::PACKAGED;
use brenn_lib::messaging::ParticipantId;
use brenn_lib::messaging::config::{Depth, Sink};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::testutils::{attachment_ids, held, redeem_all, retained_on, seal_blob};
use brenn_messaging::{PublishOrigin, PublishResult, Urgency};
use brenn_messaging_store::blob::{BlobHold, BlobUsage, HeldAttachments};
use brenn_messaging_store::db::bus_gc_evict_channel;
use brenn_tool_registry::testutil::{ATTACHMENT_ECHO, AttachmentEchoTool};
use brenn_tool_registry::tool::{RegisteredTool, ToolCtx, ToolOutput};
use brenn_tool_registry::{ToolError, ToolRegistry};
use serde_json::{Value, json};

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, READER, Tree, assert_applied, async_pull_tool, boot_with, document, hex,
    install_named_package, install_package_from, poll_until, seat_a_conversation, staged_module,
};
use brenn_messaging::config_reload::Outcome;

/// The providing instance's slug.
const PROVIDER: &str = "provider";

/// The calling instance's slug.
const CALLER: &str = "caller";

/// The tool the provider serves.
const BLOB_ECHO: &str = "blob-echo";

const PROVIDER_ARTIFACT: &str = "brenn_processor_blob_provider_test.wasm";
const CALLER_ARTIFACT: &str = "brenn_processor_blob_caller_test.wasm";

/// The package the caller's class is installed under, beside the fenced
/// module the provider's class is declared in.
const CALLER_MODULE: &str = "blobcaller";

/// The caller's class, as its artifact's own spec declares it.
const CALLER_CLASS: &str = "component BlobCaller {
    abi = processor;
    requires = [ports, log, tools, blobs];
    in in;
    in tool-results;
    out out;
}
";

/// The channel the caller reports results on.
const SINK: &str = "brenn:sink";

/// The work channel the caller is triggered from.
const WORK: &str = "brenn:work";

const TOOL_REQUESTS: &str = "brenn:tools/blob-echo";
const PROVIDER_INBOX: &str = "brenn:tool-requests/provider";
const CALLER_RESULTS: &str = "brenn:tool-results/caller";
const PROVIDER_RESULTS: &str = "brenn:tool-results/provider";
const ECHO_REQUESTS: &str = "brenn:tools/attachment-echo";

/// The document: the provider's class in the fenced module, both instances,
/// the sink, and the blob store at `blob_dir`.
fn blob_document(blob_dir: &std::path::Path) -> String {
    format!(
        "use @{CALLER_MODULE}::*;\n{}",
        document(&format!(
            r#"
messaging {{
    blob_dir = "{blob_dir}";
}}

channel sink at "{SINK}" {{
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
}}
{PACKAGED}component BlobProvider {{
    abi = processor;
    requires = [log, tools, provides, blobs];
    in tool-results;
    in tool-requests;
    provide blob-echo {{
        mcp_name = "BlobEcho";
        description = "Read the handed attachments and answer with blobs, by the mode argument.";
        input_schema = """{{ "type": "object", "properties": {{ "mode": {{ "type": "string" }} }}, "required": ["mode"] }}""";
        accepts_attachments = true;
        max_concurrency = 2;
    }}
}}
{PACKAGED}
new {PROVIDER}: BlobProvider {{
    grants = [log, tools, provides, blobs];
    tool {ATTACHMENT_ECHO} {{}}
}}

new {CALLER}: BlobCaller {{
    grants = [ports, log, tools, blobs];
    in in <- work {{ push_depth = 4; }}
    out out -> sink;
    tool {BLOB_ECHO} {{}}
}}
"#,
            blob_dir = blob_dir.display(),
        ))
    )
}

/// What one boot of the blob document keeps alive: the tree, the installed
/// packages and the blob directory, which a second boot reuses.
struct Rig {
    tree: Tree,
    components: tempfile::TempDir,
    blobs: tempfile::TempDir,
}

impl Rig {
    /// Write the document and install both packages.
    fn new() -> Self {
        let blobs = tempfile::tempdir().expect("a blob directory");
        let components = tempfile::tempdir().expect("a components root");
        let tree = Tree::holding(&blob_document(blobs.path()));
        std::fs::write(
            tree.modules().join(format!("{CALLER_MODULE}.brenn")),
            CALLER_CLASS,
        )
        .expect("the caller's module is writable");
        install_named_package(
            components.path(),
            CALLER_MODULE,
            CALLER_CLASS,
            CALLER_ARTIFACT,
        );
        install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);
        Rig {
            tree,
            components,
            blobs,
        }
    }

    /// Boot over `db`, or a fresh database, with the host tool the provider's
    /// nested calls reach.
    async fn boot(&self, db: Option<brenn_db::Db>, live: bool) -> Booted {
        let fresh = db.is_none();
        let booted = boot_with(
            &self.tree,
            BootFixture {
                db,
                components_roots: vec![self.components.path().to_path_buf()],
                native_tools: vec![RegisteredTool::Async(Arc::new(AttachmentEchoTool::new()))],
                dispatcher: live,
                executor: live,
                ..BootFixture::default()
            },
        )
        .await;
        if fresh {
            assert_eq!(store_usage(&booted), usage(0, 0), "the store starts empty");
        }
        booted
    }

    /// Every file under the blob directory other than the store's record of
    /// the database that owns it.
    fn files(&self) -> Vec<std::path::PathBuf> {
        fn walk(dir: &std::path::Path, into: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("the blob directory is readable") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    walk(&path, into);
                } else {
                    into.push(path);
                }
            }
        }
        let mut files = Vec::new();
        walk(self.blobs.path(), &mut files);
        let owner = self.blobs.path().join("owner");
        files.retain(|file| *file != owner);
        files
    }
}

fn usage(sealed: usize, charged_bytes: u64) -> BlobUsage {
    BlobUsage {
        sealed,
        unsealed: 0,
        charged_bytes,
    }
}

fn store_usage(booted: &Booted) -> BlobUsage {
    booted
        .messenger
        .blob_store()
        .expect("the document configures a blob store")
        .usage()
}

fn bytes_len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).expect("a test blob's length fits in u64")
}

/// Publish one trigger on the work channel, as the reader's conversation,
/// carrying `attach`.
async fn trigger(booted: &Booted, body: Value, attach: &[(&str, &BlobHold)]) {
    let published = booted
        .messenger
        .publish_with_attachments(
            PublishOrigin::Conversation { id: 1 },
            READER,
            WORK,
            body.to_string().into(),
            Urgency::Normal,
            None,
            None,
            None,
            held(attach),
        )
        .await;
    assert!(
        matches!(published, PublishResult::Ok { .. }),
        "{published:?}"
    );
}

/// The caller's reports, once at least `wanted` have arrived.
async fn reports(booted: &mut Booted, wanted: usize) -> Vec<Value> {
    booted
        .bodies_until(SINK, wanted)
        .await
        .iter()
        .map(|body| serde_json::from_str(body).expect("a report is JSON"))
        .collect()
}

/// The one report for `call_id`.
fn report_for<'a>(reports: &'a [Value], call_id: &str) -> &'a Value {
    let found: Vec<&Value> = reports.iter().filter(|r| r["call_id"] == call_id).collect();
    assert_eq!(found.len(), 1, "one report for {call_id}: {reports:?}");
    found[0]
}

/// One stored message: its body, parsed, and the blob id of each attachment
/// it names.
#[derive(Debug)]
struct Row {
    /// The channel's retention-order sequence, which positions compare to.
    seq: i64,
    body: Value,
    attachments: BTreeMap<String, String>,
}

/// Every message on `address`, oldest first, read out of the table: the tool
/// channels are readable by no app.
async fn rows_on(booted: &Booted, address: &str) -> Vec<Row> {
    retained_on(&booted.messenger, address)
        .await
        .into_iter()
        .map(|(seq, envelope)| Row {
            seq,
            body: serde_json::from_str(
                envelope
                    .body
                    .as_text()
                    .expect("a tool message body is text"),
            )
            .expect("a tool message body is JSON"),
            attachments: attachment_ids(&envelope),
        })
        .collect()
}

fn names(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, id)| (name.to_string(), id.to_string()))
        .collect()
}

/// Delete every row of every durable channel, as a GC pass at frontier 0
/// would, then reap what nothing holds any more.
async fn evict_everything_and_reap(booted: &Booted) {
    {
        let conn = booted.messenger.db().lock().await;
        for entry in booted.messenger.directory().list_durable() {
            assert_eq!(
                entry.resolved_channel.sink,
                Sink::Drop,
                "{} archives; this fixture evicts by dropping",
                entry.address
            );
            bus_gc_evict_channel(
                &conn,
                entry.uuid,
                &entry.address,
                entry.transport_type,
                0,
                Sink::Drop,
                None,
            );
        }
    }
    booted.messenger.reap_blobs().await;
}

/// Evict and reap, then assert no blob is left in the store or on disk.
async fn assert_nothing_outlives(booted: &Booted, rig: &Rig) {
    evict_everything_and_reap(booted).await;
    assert_eq!(store_usage(booted), usage(0, 0));
    assert_eq!(rig.files(), Vec::<std::path::PathBuf>::new());
}

/// Wait until the component `slug` has read past the newest message on
/// `address`: the drain step took it. Positions advance before the guest
/// runs, so the activation that took it may not have run yet.
async fn read_past_newest(booted: &Booted, address: &str, slug: &str) {
    let newest = rows_on(booted, address)
        .await
        .last()
        .expect("a message to read past")
        .seq;
    poll_until(
        &format!("{slug}'s read past {address} #{newest}"),
        1,
        None,
        async || {
            super::driver::tests::cursor_of(
                &booted.messenger,
                address,
                &ParticipantId::for_wasm(slug),
            )
            .await
            .filter(|cursor| cursor.next_owed_seq > newest)
            .into_iter()
            .collect::<Vec<_>>()
        },
    )
    .await;
}

/// A boot of the blob document, live, with the reader's conversation seated
/// for the triggers.
async fn live(rig: &Rig) -> Booted {
    let booted = rig.boot(None, true).await;
    seat_a_conversation(&booted.db, 1).await;
    booted
}

// ── Component round trips ─────────────────────────────────────────────────

/// The caller hands a blob it was handed to `blob-echo`; the provider
/// reads a range of it and answers with a blob it makes. Every hop names the
/// blob the test sealed by reference, the answer is the one other blob, and
/// the caller reads both the range and the answer's bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_handed_blob_and_a_made_one_cross_by_reference() {
    let rig = Rig::new();
    let mut booted = live(&rig).await;
    let doc_bytes = b"the handed document";
    let answer_bytes = b"the provider's answer";
    let doc = seal_blob(&booted.messenger, doc_bytes);
    let doc_id = doc.id().as_str().to_string();

    trigger(
        &booted,
        json!({
            "tool": BLOB_ECHO,
            "call_id": "p1",
            "args": {
                "mode": "make",
                "hex": hex(answer_bytes),
                "content_type": "text/plain",
                "as": "answer",
                "read": { "name": "doc", "offset": 4, "len": 6 },
            },
            "attach": { "doc": { "from": "doc" } },
        }),
        &[("doc", &doc)],
    )
    .await;
    drop(doc);

    let reports = reports(&mut booted, 1).await;
    let report = report_for(&reports, "p1");
    assert_eq!(
        report["ok"],
        json!({ "made": answer_bytes.len(), "range": hex(&doc_bytes[4..10]) }),
        "{report}"
    );
    assert_eq!(
        report["attachments"],
        json!([{
            "name": "answer",
            "size": answer_bytes.len(),
            "content_type": "text/plain",
            "hex": hex(answer_bytes),
        }]),
        "{report}"
    );

    let doc_by_name = names(&[("doc", &doc_id)]);
    assert_eq!(rows_on(&booted, WORK).await[0].attachments, doc_by_name);
    let requests = rows_on(&booted, TOOL_REQUESTS).await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].attachments, doc_by_name);
    assert_eq!(requests[0].body["call_id"], "p1");
    assert!(
        requests[0].body.get("attachments").is_none(),
        "the body is unchanged: {}",
        requests[0].body
    );
    let inbox = rows_on(&booted, PROVIDER_INBOX).await;
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].attachments, doc_by_name);
    let results = rows_on(&booted, CALLER_RESULTS).await;
    assert_eq!(results.len(), 1);
    let answer_id = results[0].attachments["answer"].clone();
    assert_ne!(answer_id, doc_id);
    assert_eq!(results[0].attachments.len(), 1);

    assert_eq!(
        store_usage(&booted),
        usage(2, bytes_len(doc_bytes) + bytes_len(answer_bytes)),
        "two blobs and no copy at any hop"
    );
    assert_nothing_outlives(&booted, &rig).await;
}

/// The provider answers with the blob it was handed: the result names the
/// request's blob, and the store holds that one blob throughout.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_answers_with_the_blob_it_was_handed() {
    let rig = Rig::new();
    let mut booted = live(&rig).await;
    let doc_bytes = b"round and back";
    let doc = seal_blob(&booted.messenger, doc_bytes);
    let doc_id = doc.id().as_str().to_string();

    trigger(
        &booted,
        json!({
            "tool": BLOB_ECHO,
            "call_id": "p2",
            "args": { "mode": "return-handed" },
            "attach": { "doc": { "from": "doc" } },
        }),
        &[("doc", &doc)],
    )
    .await;
    drop(doc);

    let reports = reports(&mut booted, 1).await;
    let report = report_for(&reports, "p2");
    assert_eq!(report["ok"], json!({ "returned": ["doc"] }), "{report}");
    assert_eq!(report["attachments"][0]["hex"], hex(doc_bytes), "{report}");
    let doc_by_name = names(&[("doc", &doc_id)]);
    assert_eq!(
        rows_on(&booted, PROVIDER_INBOX).await[0].attachments,
        doc_by_name
    );
    assert_eq!(
        rows_on(&booted, CALLER_RESULTS).await[0].attachments,
        doc_by_name
    );
    assert_eq!(store_usage(&booted), usage(1, bytes_len(doc_bytes)));
    assert_nothing_outlives(&booted, &rig).await;
}

/// Through the executor: the provider's nested call hands the blob it
/// was handed to the host tool `attachment-echo`, which returns it; the
/// provider completes with it. One blob, named by every hop.
#[tokio::test(flavor = "multi_thread")]
async fn a_nested_call_hands_the_blob_to_a_host_tool_and_back() {
    let rig = Rig::new();
    let mut booted = live(&rig).await;
    let doc_bytes = b"nested and returned";
    let doc = seal_blob(&booted.messenger, doc_bytes);
    let doc_id = doc.id().as_str().to_string();

    trigger(
        &booted,
        json!({
            "tool": BLOB_ECHO,
            "call_id": "p6",
            "args": { "mode": "nested", "tool": ATTACHMENT_ECHO },
            "attach": { "doc": { "from": "doc" } },
        }),
        &[("doc", &doc)],
    )
    .await;
    drop(doc);

    let reports = reports(&mut booted, 1).await;
    let report = report_for(&reports, "p6");
    assert_eq!(
        report["ok"],
        json!({
            "args": {},
            "received": {
                "doc": { "size": doc_bytes.len(), "content_type": "application/octet-stream" },
            },
        }),
        "{report}"
    );
    assert_eq!(report["attachments"][0]["name"], "doc", "{report}");
    assert_eq!(report["attachments"][0]["hex"], hex(doc_bytes), "{report}");

    let doc_by_name = names(&[("doc", &doc_id)]);
    let nested = rows_on(&booted, ECHO_REQUESTS).await;
    assert_eq!(nested.len(), 1);
    assert_eq!(nested[0].attachments, doc_by_name);
    assert_eq!(
        rows_on(&booted, PROVIDER_RESULTS).await[0].attachments,
        doc_by_name
    );
    assert_eq!(
        rows_on(&booted, CALLER_RESULTS).await[0].attachments,
        doc_by_name
    );
    assert_eq!(store_usage(&booted), usage(1, bytes_len(doc_bytes)));
    assert_nothing_outlives(&booted, &rig).await;
}

// ── Lifetimes ─────────────────────────────────────────────────────────────

/// `blob-echo` as the registry holds it.
fn blob_echo(booted: &Booted) -> Arc<dyn brenn_tool_registry::tool::AsyncTool> {
    let registry: Arc<ToolRegistry> = booted.driver.env().tool_registry.load();
    match registry.get(BLOB_ECHO) {
        Some(RegisteredTool::Async(tool)) => tool.clone(),
        _ => panic!("blob-echo is not a registered async tool"),
    }
}

/// The context of a call to `blob-echo` made as the caller component would
/// make it, to be answered within `deadline`.
fn waiter_ctx(deadline: Duration) -> ToolCtx {
    ToolCtx {
        caller: ParticipantId::for_wasm(CALLER),
        grant: ResolvedToolGrant {
            acl: vec![],
            rate_limit: None,
        },
        acting_conversation_id: None,
        acting_user: None,
        deadline: Some(tokio::time::Instant::now() + deadline),
        caller_opens_blobs: true,
    }
}

/// Call `blob-echo` as its waiter, answering by `deadline`.
async fn call_blob_echo(
    booted: &Booted,
    args: Value,
    attachments: HeldAttachments,
    deadline: Duration,
) -> Result<ToolOutput, ToolError> {
    let ctx = waiter_ctx(deadline);
    blob_echo(booted).execute(&ctx, args, attachments).await
}

/// The pending id of the newest request on the provider's inbox, once there
/// are `wanted`.
async fn newest_request_id(booted: &Booted, wanted: usize) -> String {
    let rows = poll_until(
        "requests on the provider's inbox",
        wanted,
        None,
        async || rows_on(booted, PROVIDER_INBOX).await,
    )
    .await;
    rows.last().expect("a request").body["id"]
        .as_str()
        .expect("a request body names its pending id")
        .to_string()
}

/// Ask the provider to complete `target` late, with a blob it makes; answers
/// the word that completion drew.
async fn complete_late(booted: &Booted, target: &str) -> Value {
    call_blob_echo(
        booted,
        json!({ "mode": "complete-late", "target": target, "hex": "0f0f" }),
        HeldAttachments::none(),
        Duration::from_secs(30),
    )
    .await
    .expect("the late completion is answered")
    .value["late"]
        .clone()
}

/// Every way a call ends lets go of its blobs: a caller that trapped
/// after queueing published nothing; a provider that answered and then failed
/// or trapped still delivered its blob; a call past its deadline and one whose
/// waiter went away each leave the provider's late completion `not-pending`.
/// Once every channel is evicted and the store reaped, no blob is left in the
/// store or on disk.
#[tokio::test(flavor = "multi_thread")]
async fn every_holder_lets_go_and_nothing_outlives_the_windows() {
    let rig = Rig::new();
    let mut booted = live(&rig).await;

    // The caller traps after queueing: its buffered request and the blob it
    // made go with the activation. The next trigger's call is the only one
    // that reaches the bus. The trapped trigger is drained on its own before
    // the next is published, so the trap cannot take the next one with it.
    trigger(
        &booted,
        json!({
            "tool": BLOB_ECHO,
            "call_id": "trapped",
            "args": { "mode": "return-handed" },
            "attach": { "doc": { "make": { "hex": "aaaa" } } },
            "trap": true,
        }),
        &[],
    )
    .await;
    read_past_newest(&booted, WORK, CALLER).await;
    trigger(
        &booted,
        json!({
            "tool": BLOB_ECHO,
            "call_id": "after-trap",
            "args": { "mode": "return-handed" },
            "attach": { "doc": { "make": { "hex": "bbbb" } } },
        }),
        &[],
    )
    .await;
    let seen = reports(&mut booted, 1).await;
    assert_eq!(
        report_for(&seen, "after-trap")["attachments"][0]["hex"],
        "bbbb"
    );
    // The caller is activated serially, so the trapped activation ended
    // before the one that queued `after-trap` began. That call's request and
    // its result name the one blob it made; a blob the trapped activation
    // left behind would be a second.
    assert_eq!(
        store_usage(&booted),
        usage(1, bytes_len(&[0xbb, 0xbb])),
        "only the after-trap call's blob is left: the trapped one's is gone"
    );
    let requests = rows_on(&booted, TOOL_REQUESTS).await;
    assert_eq!(
        requests
            .iter()
            .map(|r| r.body["call_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["after-trap"],
        "the trapped activation flushed nothing"
    );

    // The provider completes, then fails its activation; then, in an
    // activation of its own, completes and traps. Each completion stands with
    // its blob. One at a time: a trap fails every request its activation took
    // and had not completed.
    for (n, (mode, bytes)) in [("ok-then-err", "cccc"), ("ok-then-trap", "dddd")]
        .into_iter()
        .enumerate()
    {
        trigger(
            &booted,
            json!({
                "tool": BLOB_ECHO,
                "call_id": mode,
                "args": { "mode": mode, "hex": bytes },
            }),
            &[],
        )
        .await;
        let seen = reports(&mut booted, n + 2).await;
        let report = report_for(&seen, mode);
        assert_eq!(report["ok"], json!({ "made": 2 }), "{report}");
        assert_eq!(report["attachments"][0]["hex"], bytes, "{report}");
    }

    // A call past its deadline: the waiter answers `internal`, and the
    // provider's late completion, blob and all, is `not-pending`.
    let before = rows_on(&booted, PROVIDER_INBOX).await.len();
    let held_doc = seal_blob(&booted.messenger, b"held past the deadline");
    let answered = call_blob_echo(
        &booted,
        json!({ "mode": "hold" }),
        held(&[("doc", &held_doc)]),
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        answered.map(|output| output.value),
        Err(ToolError::Internal(
            "provider did not complete within the ceiling".to_string()
        ))
    );
    let expired = newest_request_id(&booted, before + 1).await;
    assert_eq!(complete_late(&booted, &expired).await, "not-pending");

    // A waiter dropped before the provider completes.
    let dropped_doc = seal_blob(&booted.messenger, b"waiter went away");
    let waiter = tokio::spawn({
        let tool = blob_echo(&booted);
        let attachments = held(&[("doc", &dropped_doc)]);
        let ctx = waiter_ctx(Duration::from_secs(60));
        async move {
            tool.execute(&ctx, json!({ "mode": "hold" }), attachments)
                .await
        }
    });
    // The held request and the late completion's, then this one.
    let abandoned = newest_request_id(&booted, before + 3).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(complete_late(&booted, &abandoned).await, "not-pending");

    drop((held_doc, dropped_doc));
    assert_nothing_outlives(&booted, &rig).await;
}

// ── Restart ───────────────────────────────────────────────────────────────

/// A request with an attachment rests on the provider's inbox while the
/// process stops. Booted again over the same database and blob directory,
/// with an empty pending table, the row still names the blob and it is
/// redeemable; the provider is activated on the request and may open it; its
/// completion of that request is `not-pending`; and once the inbox is evicted
/// and the store reaped, nothing is left.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_on_the_providers_inbox_keeps_its_blob_across_a_restart() {
    let rig = Rig::new();
    let db = brenn_server::test_support::init_db_memory();
    let doc_bytes = b"survives the restart";

    // A process with no dispatcher: the request is published and nobody is
    // woken for it.
    let first = rig.boot(Some(db.clone()), false).await;
    let doc = seal_blob(&first.messenger, doc_bytes);
    let doc_id = doc.id().clone();
    let waiter = tokio::spawn({
        let tool = blob_echo(&first);
        let attachments = held(&[("doc", &doc)]);
        let ctx = waiter_ctx(Duration::from_secs(60));
        async move {
            tool.execute(&ctx, json!({ "mode": "read", "name": "doc" }), attachments)
                .await
        }
    });
    let restarted = newest_request_id(&first, 1).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    drop(doc);
    drop(first);

    let second = rig.boot(Some(db), true).await;
    let inbox = rows_on(&second, PROVIDER_INBOX).await;
    assert_eq!(inbox.len(), 1);
    assert_eq!(
        inbox[0].attachments,
        names(&[("doc", doc_id.as_str())]),
        "the row still names the blob"
    );
    assert_eq!(
        redeem_all(&second.messenger, &doc_id).await.unwrap(),
        doc_bytes,
        "the blob the row names is redeemable"
    );
    // The provider is activated on the request it was handed before the
    // restart: its position moves past it.
    read_past_newest(&second, PROVIDER_INBOX, PROVIDER).await;
    assert_eq!(complete_late(&second, &restarted).await, "not-pending");
    // What the provider opens of the blob is what was sealed before the
    // restart.
    let hold = second
        .messenger
        .retain_blob(&doc_id)
        .await
        .expect("the blob is held by its row");
    let read = call_blob_echo(
        &second,
        json!({ "mode": "read", "name": "doc" }),
        held(&[("doc", &hold)]),
        Duration::from_secs(30),
    )
    .await
    .expect("the provider reads the blob");
    assert_eq!(read.value["range"], hex(doc_bytes));
    drop(hold);

    assert_nothing_outlives(&second, &rig).await;
}

// ── Reload: an app's result inbox ─────────────────────────────────────────

/// The app granted an asynchronous tool by the reload candidate.
const PULLER: &str = "puller";

const PULLER_INBOX: &str = "brenn:app-tool-results/puller";

/// The puller agent, granted the async stub tool `apull` when `granted`,
/// which gives its app a result inbox, and `tuning` beside it.
fn puller_document(granted: bool, tuning: &str) -> String {
    let grant = if granted {
        "    tool apull { allow { repo = \"notes\"; } }\n"
    } else {
        ""
    };
    document(&format!(
        r#"
agent Puller() {{
    working_dir = ".";
{grant}}}

new {PULLER}: Puller();
{tuning}"#
    ))
}

/// A tuning block at `address` (`at "…"` or `at prefix "…"`), with `extra`
/// in its body.
fn tuning_block(address: &str, extra: &str) -> String {
    format!(
        "channel at {address} {{\n    push_depth = 1;\n    retain_depth = 4;\n    \
         standing_retain_depth = 4;\n{extra}}}\n"
    )
}

/// Boot the puller with no grant, and reload onto the puller granted `apull`
/// with `tuning`.
async fn reload_granting(tuning: &str) -> (Booted, brenn_messaging::config_reload::ReloadStatus) {
    let tree = Tree::holding(&puller_document(false, ""));
    let mut booted = boot_with(
        &tree,
        BootFixture {
            native_tools: vec![async_pull_tool()],
            ..BootFixture::default()
        },
    )
    .await;
    assert!(booted.messenger.directory().resolve(PULLER_INBOX).is_none());
    tree.write(&puller_document(true, tuning));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    (booted, status)
}

fn inbox_depth(booted: &Booted) -> Depth {
    booted
        .messenger
        .directory()
        .resolve(PULLER_INBOX)
        .expect("the app's result inbox is in the directory")
        .resolved_channel
        .standing_retain_depth
}

/// An app's first asynchronous grant adds its result inbox at reload, at the
/// tool family's depth. An exact tuning block sizes it when it names the app
/// and is refused when it names an app with no such grant; a block over
/// `brenn:tool-results/` does not size it and one over
/// `brenn:app-tool-results/` does; a `send_rate` in either kind of block for
/// it is refused.
#[tokio::test(flavor = "multi_thread")]
async fn an_apps_first_async_grant_adds_its_result_inbox_at_reload() {
    let (booted, status) = reload_granting("").await;
    assert_applied(&status);
    assert!(
        status
            .delta
            .channels_added
            .iter()
            .any(|a| a == PULLER_INBOX),
        "{:?}",
        status.delta.channels_added
    );
    assert_eq!(inbox_depth(&booted), Depth::Bounded(16));

    let exact = format!("\"{PULLER_INBOX}\"");
    let (booted, status) = reload_granting(&tuning_block(&exact, "")).await;
    assert_applied(&status);
    assert_eq!(inbox_depth(&booted), Depth::Bounded(4));

    let (_, status) = reload_granting(&tuning_block("\"brenn:app-tool-results/nobody\"", "")).await;
    assert_eq!(status.outcome, Outcome::Refused, "{status:?}");
    assert!(
        status
            .refusals
            .iter()
            .any(|r| r.contains("an app holding an async tool grant")),
        "{:?}",
        status.refusals
    );

    let (booted, status) =
        reload_granting(&tuning_block("prefix \"brenn:tool-results/\"", "")).await;
    assert_applied(&status);
    assert_eq!(inbox_depth(&booted), Depth::Bounded(16));

    let (booted, status) =
        reload_granting(&tuning_block("prefix \"brenn:app-tool-results/\"", "")).await;
    assert_applied(&status);
    assert_eq!(inbox_depth(&booted), Depth::Bounded(4));

    let send_rate = "    send_rate = { burst = 1, refill_interval_secs = 1, refill = 1 };\n";
    for address in [exact.as_str(), "prefix \"brenn:app-tool-results/\""] {
        let (_, status) = reload_granting(&tuning_block(address, send_rate)).await;
        assert_eq!(status.outcome, Outcome::Refused, "{address}: {status:?}");
        assert!(
            status.refusals.iter().any(|r| r.contains("send_rate")),
            "{address}: {:?}",
            status.refusals
        );
    }
}
