//! The correctness oracle, and the cases that need a process rather than a
//! plan.
//!
//! The rule every other test here serves one clause of, stated whole: after a
//! successful reload the process is in the state a fresh boot of the new
//! document would have produced. [`the_reloaded_process_matches_a_fresh_boot`]
//! checks it literally — boot A, reload to A′, and boot A′ a second time over a
//! copy of the database taken before the reload, then compare the two
//! processes. Nothing derived from the delta is trusted: the snapshot is read
//! off the live directory, the registrations, the running registry, the ring
//! stores and the durable rows.
//!
//! The rest are the cases the earlier increments' in-memory fixture could not
//! reach: a channel under a publisher while its consumer is being retired, a
//! subscriber that only the live directory knows about, and an artifact that
//! moved under a document that did not.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use brenn_lib::messaging::config::{Depth, NoiseLevel};
use brenn_lib::messaging::{SubscriberEntry, SubscriberEntryKind};
use brenn_messaging::Messenger;
use brenn_server::test_support::init_db_file;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, READER, Tree, async_tool_registry, boot, boot_with, document,
    document_with_a_consumer, install_package, install_package_from, seat_a_conversation,
    staged_module, subscriber_debug_lines,
};
use brenn_messaging::config_reload::Outcome;

// ── The oracle ────────────────────────────────────────────────────────────

/// Everything about a running process that a fresh boot of the same document
/// must reproduce.
///
/// Every field is read off the running system rather than off the plan that
/// produced it: a reload that told itself it had converged and had not would
/// pass a comparison of plans and fail this one.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    /// Directory entries, one rendered line each, sorted by address.
    channels: Vec<String>,
    /// Every subscriber kind the directory names, and what the target resolver
    /// answers for it.
    registrations: Vec<String>,
    /// The consumer slugs whose tasks are running.
    running: Vec<String>,
    /// Addresses holding a ring store — the non-durable channels.
    rings: Vec<String>,
    /// The durable channel rows, and every subscriber position.
    rows: Vec<String>,
    /// The executor's per-caller tool grant table, one rendered line per
    /// caller, sorted by caller.
    grants: Vec<String>,
}

/// One directory entry as a line: its identity, its tuning, its metadata and
/// its subscribers.
///
/// `resolved_channel` and each subscriber go in through `Debug`, so a field
/// added to either joins the comparison by existing rather than by being
/// remembered here.
fn entry_line(entry: &brenn_lib::messaging::ChannelEntry) -> String {
    let subscribers = subscriber_debug_lines(entry);
    format!(
        "{} uuid={} transport={:?} mount={:?} description={:?} tuning={:?} subscribers=[{}]",
        entry.address,
        entry.uuid,
        entry.transport_type,
        entry.mount,
        entry.description,
        entry.resolved_channel,
        subscribers.join(" | "),
    )
}

async fn snapshot(booted: &Booted) -> Snapshot {
    let entries = booted.messenger.directory().list();

    let mut channels: Vec<String> = entries.iter().map(|entry| entry_line(entry)).collect();
    channels.sort();

    let mut kinds: Vec<SubscriberEntryKind> = Vec::new();
    for entry in &entries {
        for subscriber in &entry.subscribers {
            if !kinds.contains(&subscriber.kind) {
                kinds.push(subscriber.kind.clone());
            }
        }
    }
    let mut registrations: Vec<String> = kinds
        .iter()
        .map(|kind| {
            let wake = booted
                .messenger
                .subscriber_registration(kind)
                .map(|registration| registration.wake);
            format!("{kind:?} wake={wake:?}")
        })
        .collect();
    registrations.sort();

    let mut running: Vec<String> = booted.driver.registry().keys().cloned().collect();
    running.sort();

    let mut rings: Vec<String> = booted
        .messenger
        .ring_stores()
        .stores()
        .iter()
        .map(|store| store.address().to_string())
        .collect();
    rings.sort();

    let mut grants: Vec<String> = booted
        .tool_caller_grants
        .as_ref()
        .map(|table| table.snapshot())
        .unwrap_or_default()
        .into_iter()
        .map(|(caller, grants)| format!("{caller} {grants:?}"))
        .collect();
    // Sorted rather than taken in map order: the outer map is a `HashMap`, so
    // its iteration order is per-instance and the oracle compares two distinct
    // processes.
    grants.sort();

    Snapshot {
        channels,
        registrations,
        running,
        rings,
        rows: durable_rows(&booted.messenger).await,
        grants,
    }
}

/// The channel rows and the subscriber positions, as lines.
///
/// `resume_epoch` is deliberately absent: it is minted with the row, so two
/// processes that each created a row for the same channel hold different ones
/// and always will. Everything else about a row is the document's.
async fn durable_rows(messenger: &Messenger) -> Vec<String> {
    let conn = messenger.db().lock().await;
    let mut rows: Vec<String> = conn
        .prepare("SELECT address, description, transport_type FROM messaging_channels")
        .expect("the channels table is readable")
        .query_map([], |row| {
            Ok(format!(
                "channel {} description={:?} transport={}",
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .expect("the channel rows read")
        .map(|row| row.expect("a channel row"))
        .collect();

    rows.extend(
        conn.prepare(
            "SELECT c.subscriber, ch.address, c.push_depth, c.next_owed_seq \
             FROM messaging_subscriber_cursors c \
             JOIN messaging_channels ch ON ch.uuid = c.channel_uuid",
        )
        .expect("the cursor table is readable")
        .query_map([], |row| {
            Ok(format!(
                "cursor {} on {} push_depth={} next_owed_seq={}",
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .expect("the cursor rows read")
        .map(|row| row.expect("a cursor row")),
    );

    rows.sort();
    rows
}

/// Copy the database as it stands, consistently, to `to`.
///
/// `VACUUM INTO` rather than a file copy: the production pragmas put the
/// connection in WAL mode, where the file on disk is not the database.
async fn copy_database(db: &brenn_db::Db, to: &std::path::Path) {
    let conn = db.lock().await;
    conn.execute(
        "VACUUM INTO ?1",
        rusqlite::params![to.to_str().expect("a UTF-8 path")],
    )
    .expect("the database copies");
}

/// A consumer holding no tool grant at all, stamped into both documents.
///
/// Without it every consumer in the fixture holds a grant and the oracle
/// comparison never sees the grantless shape of the caller table.
const QUIET: &str = r#"new quiet: Plain {
    grants = [ports];
    in inbound <- sink { push_depth = 2; }
    out digest -> scratch;
}"#;

/// A: one consumer holding an async tool grant, so the process the oracle
/// compares has a non-empty executor grant table to compare.
fn document_with_a_tool_granted_consumer() -> String {
    document(&format!(
        r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    out digest;
}}

component Plain {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{}
new sifter: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool apull {{ allow {{ repo = "brenn"; }} }}
}}

{QUIET}
"#,
        brenn_lib::config::PACKAGED,
        brenn_lib::config::PACKAGED,
    ))
}

/// A′: [`document_with_a_tool_granted_consumer`] plus a channel and a second
/// consumer of the same component, granted a different repo, so the reload has
/// an added channel, an added consumer, an added caller key, and — the
/// package's spec bytes having moved with the document's packaged half — a
/// consumer that is changed rather than merely present.
fn document_with_two_consumers() -> String {
    document(&format!(
        r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}

channel digested at "brenn:digested" {{
    push_depth = 2;
    retain_depth = 8;
    standing_retain_depth = 8;
}}
{}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    out digest;
}}

component Plain {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{}
new sifter: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool apull {{ allow {{ repo = "brenn"; }} }}
}}

new grinder: Demo {{
    grants = [ports, tools];
    in inbound <- sink {{ push_depth = 2; }}
    out digest -> digested;
    tool apull {{ allow {{ repo = "notes"; }} }}
}}

{QUIET}
"#,
        brenn_lib::config::PACKAGED,
        brenn_lib::config::PACKAGED,
    ))
}

/// The correctness oracle: reload A→A′ and boot A′ fresh over the database as
/// it stood before the reload, then compare the two processes.
///
/// The database is copied *before* the reload rather than after, so the fresh
/// boot starts from what a restart at that moment would have started from —
/// and every row the reload wrote is the reload's own claim, which is exactly
/// what is under test.
///
/// **What "a fresh boot" means here.** The other side is `boot_with`, the test
/// fixture's boot, not `run_server`: it lowers the document with the same
/// planner, then loads, wires and starts what the plan names, and builds the
/// baseline the same way production does. What it does not reproduce is the
/// rest of the composition root — the dispatcher, session, ingress and GC
/// lifetime tasks, the HTTP and attach surfaces, the webhook and MQTT
/// peripheries, the tool executor's own drain task, and the boot-time
/// `booted` status publish. Those are boot's, not the reload's, and the
/// comparison is over the six things a reload edits: directory entries with
/// their subscribers, subscriber registrations, running consumer tasks, ring
/// stores, durable rows, and the executor's tool grant table. The wiring a
/// reload edits that the fixture *does* reproduce is asserted to be
/// production's shape by another test — the planned baseline directory, by
/// `brenn-messaging-boot`'s carried-directory test.
#[tokio::test(flavor = "multi_thread")]
async fn the_reloaded_process_matches_a_fresh_boot() {
    let store = tempfile::tempdir().expect("a directory for the databases");
    let components = tempfile::tempdir().expect("a components root");
    let roots = vec![components.path().to_path_buf()];

    let tree = Tree::holding(&document_with_a_tool_granted_consumer());
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_with(
        &tree,
        BootFixture {
            db: Some(init_db_file(&store.path().join("running.db"))),
            components_roots: roots.clone(),
            tool_registry: Some(async_tool_registry()),
            ..BootFixture::default()
        },
    )
    .await;

    let restart_point = store.path().join("restart.db");
    copy_database(&booted.db, &restart_point).await;

    tree.write(&document_with_two_consumers());
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.consumers_added, vec!["grinder".to_string()]);

    let reloaded = snapshot(&booted).await;
    // The comparison is only worth anything if there is something to compare:
    // both consumers running, and the added channel in the directory.
    assert_eq!(reloaded.running, vec!["grinder", "quiet", "sifter"]);
    assert!(
        reloaded
            .channels
            .iter()
            .any(|line| line.starts_with("brenn:digested ")),
        "{:?}",
        reloaded.channels
    );
    assert_eq!(
        reloaded.grants.len(),
        2,
        "the two granted consumers hold a caller key each and the grantless one holds none: {:?}",
        reloaded.grants
    );

    // A fresh boot of A′ over the database as it stood before the reload:
    // what the operator would have got by restarting the service instead.
    let restarted = boot_with(
        &tree,
        BootFixture {
            db: Some(init_db_file(&restart_point)),
            components_roots: roots,
            tool_registry: Some(async_tool_registry()),
            ..BootFixture::default()
        },
    )
    .await;
    assert_eq!(reloaded, snapshot(&restarted).await);
}

/// A channel under a continuous publisher while its only consumer is retired.
///
/// The retired-key rule is what this exercises from the outside: a publish that
/// resolved the channel before the consumer's subscriber entry left holds a
/// snapshot that still names it, and the wake it raises arrives after the
/// binding became a tombstone. The rule says that wake is dropped; the
/// alternative — the panic a never-registered key gets — would take the process
/// down for a subscriber that merely left.
#[tokio::test(flavor = "multi_thread")]
async fn publishing_across_a_consumers_retirement_never_panics() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
    seat_a_conversation(&booted.db, 1).await;

    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicUsize::new(0));
    let publisher = tokio::spawn({
        let messenger = booted.messenger.clone();
        let stop = stop.clone();
        let published = published.clone();
        async move {
            // Uncapped and unpaced: the reload window is what has to be
            // covered, and a publisher that fell quiet inside it would leave
            // the retirement unraced while the test still passed. The
            // fixture's send budget and the work channel's send rate are
            // sized for this (see `boot_with` and `document`).
            while !stop.load(Ordering::Relaxed) {
                let outcome = publish_work(&messenger).await;
                assert!(
                    matches!(outcome, brenn_messaging::PublishResult::Ok { .. }),
                    "a publish to a declared channel must go through: {outcome:?}"
                );
                published.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        }
    });

    tree.write(&document(""));
    let before = published.load(Ordering::Relaxed);
    booted.driver.reload(TriggerSource::Signal).await;
    let during = published.load(Ordering::Relaxed) - before;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.consumers_removed, vec!["sifter".to_string()]);

    stop.store(true, Ordering::Relaxed);
    publisher.await.expect("the publisher survived the reload");
    assert!(
        during > 0,
        "publishes have to land inside the reload window for this to be a race \
         at all; the publisher made {during} of them there"
    );

    let kind = SubscriberEntryKind::Wasm("sifter".to_string());
    assert!(booted.driver.registry().is_empty());
    assert!(booted.messenger.subscriber_registration_retired(&kind));
    assert!(booted.router.delivery_binding_retired(&kind));

    // The channel outlives its consumer, and publishing to it is still an
    // ordinary publish — a channel does not care that nobody is subscribed.
    assert!(matches!(
        publish_work(&booted.messenger).await,
        brenn_messaging::PublishResult::Ok { .. }
    ));
}

/// A retune of a channel's `send_rate` takes the buckets already drawn against
/// it with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_retune_takes_the_send_rate_buckets_with_it() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            tool_registry: Some(async_tool_registry()),
            ..BootFixture::default()
        },
    )
    .await;
    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is declared")
        .uuid;

    seat_a_conversation(&booted.db, 1).await;
    assert!(matches!(
        publish_work(&booted.messenger).await,
        brenn_messaging::PublishResult::Ok { .. }
    ));
    assert!(
        booted.messenger.send_rate_bucket_channels().contains(&work),
        "the publish drew a bucket, so there is something for the reload to evict",
    );

    tree.write(&document_with_a_consumer().replace("burst = 1000000", "burst = 900000"));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.channels_changed, vec!["brenn:work"]);

    assert!(
        !booted.messenger.send_rate_bucket_channels().contains(&work),
        "the retuned entry's buckets went with the entry: {:?}",
        booted.messenger.send_rate_bucket_channels(),
    );
}

/// One publish to the work channel, as the reader app.
async fn publish_work(messenger: &Arc<Messenger>) -> brenn_messaging::PublishResult {
    messenger
        .publish(
            brenn_messaging::PublishOrigin::Conversation { id: 1 },
            READER,
            "brenn:work",
            "tick",
            brenn_messaging::Urgency::Normal,
            None,
            None,
            None,
        )
        .await
}

/// A subscriber the plan cannot see, on a channel the candidate retunes.
///
/// The first convergibility rule answers for the subscribers boot folded onto
/// an entry; the second is for the ones that arrived since — an attach-minted
/// `Surface` entry, a dynamic app subscription, a session streaming from the
/// channel. Re-creating the entry would drop them, so the reload refuses and
/// names them.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_only_the_live_directory_holds_refuses_the_reload() {
    let tree = Tree::holding(&document(""));
    let mut booted = boot(&tree, Vec::new()).await;
    let booted_sha = booted.driver.baseline().document.document_sha256.clone();

    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is declared");
    assert!(booted.messenger.directory().add_subscriber(
        &work.uuid,
        SubscriberEntry {
            kind: SubscriberEntryKind::Surface("wall".to_string()),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Silent,
            wake_min: None,
        },
    ));

    // The one edit is the work channel's standing depth, which makes it a
    // changed entry — and a changed entry is remove-then-add, which is what the
    // attached surface would not survive.
    tree.write(&document("").replace("standing_retain_depth = 64;", "standing_retain_depth = 32;"));
    assert!(
        booted
            .driver
            .prepare_and_report(TriggerSource::Signal)
            .await
            .is_none()
    );

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(status.refusals.len(), 1, "{:?}", status.refusals);
    assert!(
        status.refusals[0].contains("brenn:work")
            && status.refusals[0].contains("wall")
            && status.refusals[0].ends_with(super::NEEDS_RESTART),
        "{:?}",
        status.refusals
    );
    // Refused means untouched: the surface is still subscribed and the process
    // still projects what it booted.
    assert_eq!(
        booted.driver.baseline().document.document_sha256,
        booted_sha
    );
    let work = booted
        .messenger
        .directory()
        .resolve("brenn:work")
        .expect("the work channel is still declared");
    assert!(work.subscribers.iter().any(|subscriber| {
        matches!(&subscriber.kind, SubscriberEntryKind::Surface(slug) if slug == "wall")
    }));
}

/// The artifact moves and the document does not: a bundle installed under a
/// running consumer.
///
/// Nothing in the text changed, so the raw-document comparison and the
/// resolved-value comparison both say nothing happened. What makes this a
/// change is the package record the driver re-reads off the roots at plan time
/// — without it the process would keep executing bytes the roots no longer
/// hold, which is a running system no document describes.
#[tokio::test(flavor = "multi_thread")]
async fn an_artifact_that_moved_under_an_unmoved_document_is_a_changed_consumer() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_with_a_consumer());
    let module = staged_module(&tree);
    let original = "brenn_processor_demo.wasm";
    let replacement = "brenn_processor_dual.wasm";
    let original_sha256 = install_package_from(components.path(), &module, original);
    let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
    let booted_sha = booted.driver.baseline().document.document_sha256.clone();

    // The bundle install: same package name, same authored spec, different
    // bytes under it.
    let replacement_sha256 = install_package_from(components.path(), &module, replacement);

    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.consumers_changed, vec!["sifter".to_string()]);
    assert!(status.delta.consumers_added.is_empty());
    assert!(status.delta.channels_added.is_empty());
    // The document never moved, so the identity the process reports is the one
    // it booted with.
    assert_eq!(status.document_sha256.as_deref(), Some(&*booted_sha));

    // What is in service is bound to the bytes now under the root — which is
    // the whole of the claim, and the one thing a delta computed off the text
    // alone could not have got right.
    let running = booted
        .driver
        .registry()
        .get("sifter")
        .expect("the replacement is in service");
    assert_eq!(running.verified.artifact_sha256, replacement_sha256);
    assert_ne!(running.verified.artifact_sha256, original_sha256);
    // The authored spec never moved with it: a bundle upgrade is new bytes
    // under an unmoved contract.
    assert_eq!(
        running.verified.spec_sha256.as_deref(),
        Some(&*brenn_lib::util::sha256_hex(module.as_bytes())),
    );
}
