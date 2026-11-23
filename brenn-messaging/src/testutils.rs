//! Test helpers for building minimal in-memory `Messenger` fixtures with a single
//! WASM-subscriber channel.
//!
//! Available under `#[cfg(test)]` or when the `testutils` feature is enabled.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use uuid::Uuid;

use super::{
    ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, SubscriberRegistration, Urgency, WakeEconomics, WakeMin, WakeRouter,
    canonical_address,
    config::{self, Depth, MessagingGlobalConfig, NoiseLevel},
    db::{self, insert_message, upsert_channels},
    ephemeral_channel_uuid_from_name,
    query::NoopWakeRouter,
};
use brenn_db::Db;
use brenn_lib::access::AppPolicy;

/// Build a subscriber-registration map from a `slug → policy` map for a single
/// kind, applying `wake` to every entry. The per-kind builders below
/// (`wasm_registrations`, `surface_registrations`, `system_registrations`) wrap
/// this so test call sites that previously installed a per-kind policy side map
/// (`with_wasm_policies` etc.) install the equivalent registrations through the
/// one `with_subscriber_registrations` installer.
fn registrations_for(
    policies: HashMap<String, AppPolicy>,
    to_kind: impl Fn(String) -> SubscriberEntryKind,
    wake: WakeEconomics,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    policies
        .into_iter()
        .map(|(slug, policy)| {
            (
                to_kind(slug),
                SubscriberRegistration {
                    policy: Arc::new(policy),
                    wake,
                },
            )
        })
        .collect()
}

/// A `Messenger` over an empty channel directory and an in-memory database,
/// publishing under `origin`.
///
/// For suites whose subject is machinery around the bus — frame decoding, caps,
/// alerting, runtime shape — which need a `Messenger` to hold and never project
/// a message through it. A suite that actually delivers wants one of the
/// channel-carrying builders below. Wakes through the no-op router: wake routing
/// belongs to a composition root above.
pub fn empty_directory_messenger(origin: &str) -> Arc<Messenger> {
    empty_directory_messenger_over(db::init_db_memory(), origin)
}

/// [`empty_directory_messenger`] over a caller's `db`, which must carry the
/// messaging tables: for a test that also reads or writes that database
/// directly.
pub fn empty_directory_messenger_over(db: Db, origin: &str) -> Arc<Messenger> {
    Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from(origin),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
}

/// An `AppPolicy` authorizing delivery on every bus scheme a fixture channel can
/// carry — `brenn:`, `ephemeral:`, and `local:` — each scoped by `matcher`.
///
/// The three schemes gate on three distinct transport grants, so a fixture whose
/// channels span them needs all three. `ChannelMatcher::Prefix(String::new())`
/// is the covering case; a matcher naming something else is the denied one.
pub fn bus_delivery_policy(matcher: brenn_lib::access::acl::ChannelMatcher) -> AppPolicy {
    let mut policy = AppPolicy::default();
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::EphemeralSubscribe);
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::LocalSubscribe);
    policy.acls.brenn_subscribe.push(matcher.clone());
    policy.acls.ephemeral_subscribe.push(matcher.clone());
    policy.acls.local_subscribe.push(matcher);
    policy
}

/// Registrations for every non-`App` subscriber on `entries`, under one policy
/// scoped by `matcher`.
///
/// Boot registers each such subscriber with the policy its ACL derives, and the
/// delivery-time gate reads that registration — so a fixture whose subscriber
/// has none is a subscriber the fail-closed gate denies. Deriving from the
/// channels' own subscriber lists means a case that adds a subscriber gets its
/// registration without editing a second list. `App` subscribers are excluded:
/// their policy comes from the apps map, not the registry.
pub fn bus_subscriber_registrations(
    entries: &[ChannelEntry],
    matcher: brenn_lib::access::acl::ChannelMatcher,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    let policy = Arc::new(bus_delivery_policy(matcher));
    entries
        .iter()
        .flat_map(|entry| entry.subscribers.iter())
        .filter(|sub| !matches!(sub.kind, SubscriberEntryKind::App(_)))
        .map(|sub| {
            (
                sub.kind.clone(),
                SubscriberRegistration {
                    policy: Arc::clone(&policy),
                    wake: WakeEconomics::Eager,
                },
            )
        })
        .collect()
}

/// [`bus_subscriber_registrations`] at the covering scope — the ordinary case,
/// where a fixture's subscribers are all authorized for the channels they are
/// wired to.
pub fn covering_subscriber_registrations(
    entries: &[ChannelEntry],
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    bus_subscriber_registrations(
        entries,
        brenn_lib::access::acl::ChannelMatcher::Prefix(String::new()),
    )
}

/// A covering registration for one WASM consumer slug — for the fixture that
/// attaches its subscriber directly instead of declaring it on the channel, so
/// [`covering_subscriber_registrations`] would find nothing to register.
pub fn covering_wasm_registrations(
    slug: &str,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    wasm_registrations(HashMap::from([(
        slug.to_string(),
        bus_delivery_policy(brenn_lib::access::acl::ChannelMatcher::Prefix(String::new())),
    )]))
}

/// Registrations for WASM consumer subscribers (`Eager` wake), from a
/// `slug → policy` map.
pub fn wasm_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::Wasm, WakeEconomics::Eager)
}

/// Registrations for surface subscribers (`Eager` wake), from a `slug → policy`
/// map. One per surface: authority is per-surface, and the directory is cut at
/// (surface, channel), so a component instance names no registration of its own.
pub fn surface_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::Surface, WakeEconomics::Eager)
}

/// Registrations for remote-attacher subscribers (`Eager` wake), from a
/// `slug → policy` map. One per `[[remote]]`: a remote is a single principal
/// with no finer grain.
pub fn remote_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::Remote, WakeEconomics::Eager)
}

/// Registrations for system-substrate subscribers (`Eager` wake), from a
/// `component → policy` map.
pub fn system_registrations(
    policies: HashMap<String, AppPolicy>,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    registrations_for(policies, SubscriberEntryKind::System, WakeEconomics::Eager)
}

/// Build a single-WASM-subscriber `ChannelEntry`.
///
/// Channel-level `push_depth`, `retain_depth`, and `standing_retain_depth` are
/// fixed `Depth::Unbounded` to keep fixture construction simple; parameterize if
/// a test needs bounded channel depth. Only the *subscriber* depths vary and are
/// taken as parameters.
///
/// `noise = Silent`, `sink = Drop`, `transport_type = Brenn`, `mount = None`.
pub fn wasm_channel_entry(
    slug: &str,
    channel_name: &str,
    push_depth: Depth,
    retain_depth: Depth,
) -> Arc<ChannelEntry> {
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address(channel_name);
    Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr,
        description: None,
        resolved_channel: config::ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: config::NoiseLevel::Silent,
            sink: config::Sink::Drop,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth,
            retain_depth,
            noise: config::NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    })
}

/// A single WASM-subscriber registration for a `ChannelEntry`: unbounded
/// depths, `noise = Silent`, no per-subscription `wake_min`.
pub fn wasm_subscriber_entry(slug: &str) -> SubscriberEntry {
    SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: config::NoiseLevel::Silent,
        wake_min: None,
    }
}

// The default `brenn:` `ChannelEntry` builder lives in `brenn_lib`, beside the
// type it constructs, so the tests of both crates share one literal.
pub use brenn_lib::messaging::test_support::test_channel_entry;

/// Build an `ephemeral:` `ChannelEntry` with the deterministic name-derived uuid.
///
/// The channel rung is transparent to global: an ephemeral binding that states
/// no push_depth/noise inherits these.
pub fn ephemeral_channel_entry(name: &str, retain_depth: u64) -> ChannelEntry {
    ChannelEntry {
        uuid: ephemeral_channel_uuid_from_name(name),
        address: format!("ephemeral:{name}"),
        description: None,
        resolved_channel: config::ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Bounded(retain_depth),
            standing_retain_depth: Depth::Bounded(retain_depth),
            noise: NoiseLevel::Silent,
            sink: config::Sink::Drop,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Ephemeral,
        mount: None,
    }
}

/// `entry`, declared `non_text`: it may hold bodies that are not text.
pub fn declared_non_text(mut entry: ChannelEntry) -> ChannelEntry {
    entry.resolved_channel.non_text = true;
    entry
}

/// Build a `local:` `ChannelEntry` with the deterministic name-derived uuid —
/// non-durable like `ephemeral:`, and confined on top of it.
pub fn local_channel_entry(name: &str, retain_depth: u64) -> ChannelEntry {
    ChannelEntry {
        uuid: super::local_channel_uuid_from_name(name),
        address: format!("local:{name}"),
        transport_type: ChannelScheme::Local,
        ..ephemeral_channel_entry(name, retain_depth)
    }
}

/// Build an in-memory `Messenger` with a single WASM-subscriber channel, using a
/// noop wake router. The subscriber is attached at head, so every message a test
/// publishes afterwards is unseen to it.
///
/// Returns `(messenger, channel_entry, wasm_subscriber_id)`.
///
/// For callers that need `Depth::Unbounded` for both depths, use the terse wrapper
/// [`build_wasm_messenger_unbounded`].
pub async fn build_wasm_messenger(
    slug: &str,
    channel_name: &str,
    push_depth: Depth,
    retain_depth: Depth,
) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
    let db = crate::db::init_db_memory();
    let entry = wasm_channel_entry(slug, channel_name, push_depth, retain_depth);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&*entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_subscriber_registrations(std::slice::from_ref(
        &*entry,
    )));
    let wasm_sub = ParticipantId::for_wasm(slug);
    attach_wasm_port(&messenger, &entry, slug, &wasm_sub, push_depth).await;
    (messenger, entry, wasm_sub)
}

/// Give a WASM subscriber its position on `entry` — what a real boot does before
/// any message reaches the port. A sampled port is never delivered to and holds
/// none.
///
/// The stored depth is a cache the window read retunes from its own argument, so
/// a test that reads at a different depth than it attached at gets the depth it
/// asked for.
pub async fn attach_wasm_port(
    messenger: &Messenger,
    entry: &ChannelEntry,
    slug: &str,
    subscriber: &ParticipantId,
    push_depth: Depth,
) {
    if !push_depth.is_push_enabled() {
        return;
    }
    messenger
        .attach_subscriber(&entry.address, slug, subscriber, push_depth)
        .await;
}

/// Put `client`'s egress channel in service: take the egress reader's position
/// on it at the channel's `retain_depth`.
/// An MQTT commit lands only while that position exists, so a fixture without a
/// hub calls this for every client it publishes to; one that wants the channel
/// out of service does not.
///
/// # Panics
///
/// If the client's egress channel is not in the messenger's directory.
pub async fn attach_mqtt_egress_reader(messenger: &Messenger, client: &str) {
    let address = super::mqtt_egress_address(client);
    let depth = messenger
        .directory()
        .resolve(&address)
        .unwrap_or_else(|| panic!("testutils: egress channel {address:?} is not in the directory"))
        .resolved_channel
        .retain_depth;
    messenger
        .attach_subscriber(
            &address,
            super::mqtt_outbound::MQTT_OUTBOUND_COMPONENT,
            &super::mqtt_outbound::mqtt_outbound_participant(),
            depth,
        )
        .await;
}

/// A second `Messenger` over an existing database and channel — a host restart,
/// as observable from inside one process.
///
/// It attaches nothing: what makes this a restart rather than a fresh boot is
/// that the durable cursor row survives, so the returned messenger reads the
/// position the previous one left. A caller that wants the boot attach as well
/// calls [`attach_wasm_port`] on the result.
pub fn restart_wasm_messenger(db: Db, entry: &ChannelEntry) -> Arc<Messenger> {
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry.clone()]));
    Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_subscriber_registrations(std::slice::from_ref(
        entry,
    )))
}

/// Terse wrapper around [`build_wasm_messenger`] for the common `Unbounded`/`Unbounded` case.
pub async fn build_wasm_messenger_unbounded(
    slug: &str,
    channel_name: &str,
) -> (Arc<Messenger>, Arc<ChannelEntry>, ParticipantId) {
    build_wasm_messenger(slug, channel_name, Depth::Unbounded, Depth::Unbounded).await
}

/// Insert one bus message on `channel` at an explicit timestamp and return its
/// uuid.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather
/// than `Brenn`. `ts_ns` is the message timestamp in nanoseconds since the Unix
/// epoch. Use [`insert_bus_message`] when the exact timestamp does not matter.
///
/// Names no subscriber: a commit writes the message and nothing per-subscriber,
/// so who reads it is decided by each position at its own read.
pub async fn insert_bus_message_at(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
    ts_ns: i64,
) -> Uuid {
    insert_bus_message_from(
        messenger,
        channel,
        "test-sender",
        body,
        envelope_type,
        ts_ns,
    )
    .await
}

/// [`insert_bus_message_at`] with the envelope's `sender` chosen by the caller:
/// for a test that needs a message from a particular participant, bypassing the
/// publish gates that would admit or refuse it.
pub async fn insert_bus_message_from(
    messenger: &Messenger,
    channel: &ChannelEntry,
    sender: &str,
    body: &str,
    envelope_type: ChannelScheme,
    ts_ns: i64,
) -> Uuid {
    insert_bus_body_from(
        messenger,
        channel,
        sender,
        &body.into(),
        envelope_type,
        None,
        ts_ns,
    )
    .await
}

/// [`insert_bus_message_from`] for a body of any kind, optionally parked until
/// `deliver_after`: for a test that needs a typed or non-text body on a durable
/// channel without a publish door that can produce one.
pub async fn insert_bus_body_from(
    messenger: &Messenger,
    channel: &ChannelEntry,
    sender: &str,
    body: &brenn_lib::messaging::MessageBody,
    envelope_type: ChannelScheme,
    deliver_after: Option<chrono::DateTime<chrono::Utc>>,
    ts_ns: i64,
) -> Uuid {
    let conn = messenger.db().lock().await;
    insert_message(
        &conn,
        channel.uuid,
        "test",
        sender,
        body,
        Urgency::Normal,
        envelope_type,
        None,
        None,
        deliver_after,
        None,
        ts_ns,
    )
    .uuid
}

/// Insert one bus message on `channel` at the current wall-clock time and return
/// its uuid.
///
/// `envelope_type` is caller-supplied because some channels use `Webhook` rather
/// than `Brenn`. Use [`insert_bus_message_at`] when the test needs to control the
/// exact timestamp.
pub async fn insert_bus_message(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
) -> Uuid {
    let ts_ns = db::utc_to_ns(chrono::Utc::now());
    insert_bus_message_at(messenger, channel, body, envelope_type, ts_ns).await
}

/// Everything `subscriber` is still owed, across every channel it holds a
/// position on, as `(channel address, envelope)` — oldest first within each
/// channel.
///
/// A pure read: it moves no position and charges nothing, so a case may call it
/// before and after a drain to see what the drain consumed. A channel the
/// subscriber holds no position on contributes nothing, which is what makes this
/// safe to call over the whole directory.
///
/// One side effect it does have: reading at `Depth::Unbounded` retunes the
/// cursor's stored `push_depth` to unbounded, because the caller of a window is
/// the authority on depth. The next production read retunes it back, but a case
/// asserting clamp or drop behaviour must not sample it between this call and
/// that read — pass through a real drain instead.
///
/// The unbounded read is not incidental and cannot be narrowed: the question
/// this answers is "everything still owed", which a window at the subscriber's
/// registered depth would clamp. Restoring the prior depth afterwards would need
/// a depth read the store trait does not expose.
pub async fn owed_everywhere(
    messenger: &Messenger,
    subscriber: &ParticipantId,
) -> Vec<(String, Arc<brenn_envelope::MessageEnvelope>)> {
    let mut owed = Vec::new();
    for entry in messenger.directory().list() {
        let store = messenger.store_for(&entry);
        if !store.has_deliverable(subscriber).await {
            continue;
        }
        let window = store
            .window(subscriber, Depth::Unbounded, Depth::Bounded(0))
            .await
            .unwrap_or_else(|| {
                panic!(
                    "owed_everywhere: {} answers has_deliverable for {} yet holds no position for it",
                    entry.address,
                    subscriber.as_str()
                )
            });
        for (_, envelope) in window.new_entries() {
            owed.push((entry.address.clone(), Arc::clone(envelope)));
        }
    }
    owed
}

/// Advance `subscriber` past everything it is currently owed on
/// `channel_address` — what a real consumer's drain does at its ack point.
///
/// For a case that needs a prior step's output out of the way before reading the
/// next one: the position is the only delivery state, so consuming means moving
/// it. It reads at `Depth::Unbounded` and so retunes the cursor's stored
/// `push_depth` exactly as [`owed_everywhere`] does — same caveat.
pub async fn consume_owed(
    messenger: &Messenger,
    channel_address: &str,
    subscriber: &ParticipantId,
) {
    let store = messenger.store_for_address(channel_address);
    let window = store
        .window(subscriber, Depth::Unbounded, Depth::Bounded(0))
        .await
        .unwrap_or_else(|| {
            panic!("consume_owed: {subscriber:?} holds no position on {channel_address}")
        });
    if let Some((through, seen_floor)) = window.advance_span() {
        store
            .advance(subscriber, through, seen_floor)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "consume_owed: {subscriber:?} lost its position on {channel_address} between the read and the advance"
                )
            });
    }
}

/// Insert a low-urgency bus message on `channel` — the shape a sampled-port
/// fixture wants, where the message is context for every reader and wakes
/// nobody.
///
/// Returns the inserted `message_uuid`.
pub async fn insert_retain_only(
    messenger: &Messenger,
    channel: &ChannelEntry,
    body: &str,
    envelope_type: ChannelScheme,
) -> Uuid {
    let conn = messenger.db().lock().await;
    let ts_ns = db::utc_to_ns(chrono::Utc::now());
    insert_message(
        &conn,
        channel.uuid,
        "test",
        "test-sender",
        &body.into(),
        Urgency::Low,
        envelope_type,
        None,
        None,
        None,
        None,
        ts_ns,
    )
    .uuid
}

/// A blob directory under the test's temporary directory, removed with
/// everything in it when dropped.
pub struct TempBlobDir(std::path::PathBuf);

impl TempBlobDir {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!("brenn-blobs-{}", Uuid::new_v4()));
        TempBlobDir(path)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.0
    }

    /// Whether the blob `id` still has a file.
    pub fn holds_file(&self, id: &crate::BlobId) -> bool {
        brenn_messaging_store::testutils::holds_file(&self.0, id)
    }

    /// Cut the blob `id`'s file to zero bytes behind the store's back, so a
    /// read of any of its bytes panics.
    pub fn empty_file(&self, id: &crate::BlobId) {
        brenn_messaging_store::testutils::empty_file(&self.0, id);
    }

    /// The name of every entry in the directory, sorted.
    pub fn files(&self) -> Vec<String> {
        brenn_messaging_store::testutils::blob_files(&self.0)
    }
}

impl Default for TempBlobDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TempBlobDir {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).expect("removing a test blob directory");
        }
    }
}

/// Open a blob store over a fresh temporary directory against `messenger`'s
/// database, and install it on `messenger`, which must not yet be shared.
pub async fn install_blob_store(messenger: Arc<Messenger>) -> (Arc<Messenger>, TempBlobDir) {
    install_blob_store_with(messenger, TEST_BLOB_LIMITS).await
}

/// [`install_blob_store`] with a store of `limits`.
pub async fn install_blob_store_with(
    messenger: Arc<Messenger>,
    limits: brenn_messaging_store::blob::BlobLimits,
) -> (Arc<Messenger>, TempBlobDir) {
    let dir = TempBlobDir::new();
    (open_blob_store(messenger, dir.path(), limits).await, dir)
}

/// The limits of the blob store [`install_blob_store`] and
/// [`install_blob_store_at`] open: 1 MiB per blob, 4 MiB in total.
pub const TEST_BLOB_LIMITS: brenn_messaging_store::blob::BlobLimits =
    brenn_messaging_store::blob::BlobLimits {
        max_blob_bytes: 1 << 20,
        budget_bytes: 4 << 20,
    };

/// Open a blob store over `dir` against `messenger`'s database — a reopen when
/// a store was opened there before — and install it on `messenger`, which must
/// not yet be shared.
pub async fn install_blob_store_at(
    messenger: Arc<Messenger>,
    dir: &std::path::Path,
) -> Arc<Messenger> {
    open_blob_store(messenger, dir, TEST_BLOB_LIMITS).await
}

async fn open_blob_store(
    messenger: Arc<Messenger>,
    dir: &std::path::Path,
    limits: brenn_messaging_store::blob::BlobLimits,
) -> Arc<Messenger> {
    let blobs = {
        let conn = messenger.db().lock().await;
        brenn_messaging_store::blob::BlobStore::open(dir, limits, &conn)
    };
    messenger.with_blob_store(blobs)
}

/// Registrations for the system component `publisher`, allowed to publish onto
/// every `brenn:` and `ephemeral:` channel.
pub fn covering_system_publisher(
    publisher: &str,
) -> HashMap<SubscriberEntryKind, SubscriberRegistration> {
    let mut policy = AppPolicy::default();
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::EphemeralPublish);
    let everything = brenn_lib::access::acl::ChannelMatcher::Prefix(String::new());
    policy.acls.brenn_publish.push(everything.clone());
    policy.acls.ephemeral_publish.push(everything);
    system_registrations(HashMap::from([(publisher.to_string(), policy)]))
}

/// A messenger over `db` whose directory holds `entries`, with the durable ones
/// upserted into `db` and a ring store for each other one, a blob store over
/// `blob_dir`, and [`covering_system_publisher`] for `publisher`. Wakes through
/// the no-op router.
pub async fn blob_messenger(
    db: Db,
    entries: Vec<ChannelEntry>,
    publisher: &str,
    blob_dir: &std::path::Path,
) -> Arc<Messenger> {
    let (durable, rings): (Vec<ChannelEntry>, Vec<ChannelEntry>) = entries
        .iter()
        .cloned()
        .partition(|entry| entry.capabilities().durable);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &durable);
    }
    let messenger = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(entries)),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_system_publisher(publisher))
    .with_ring_stores(Arc::new(brenn_messaging_store::store::RingStores::build(
        &rings,
    )));
    install_blob_store_at(messenger, blob_dir).await
}

/// Publish `body` from the system component `publisher` onto `address`, naming
/// each hold under its name, and return the message's id.
///
/// # Panics
///
/// If the publish is not `Ok`.
pub async fn publish_attached(
    messenger: &Messenger,
    publisher: &str,
    address: &str,
    body: &str,
    holds: &[(&str, &brenn_messaging_store::blob::BlobHold)],
    deliver_after: Option<chrono::DateTime<chrono::Utc>>,
) -> Uuid {
    match messenger
        .publish_from_system_with_attachments(
            publisher,
            address,
            body.into(),
            Urgency::Normal,
            None,
            deliver_after,
            brenn_messaging_store::testutils::held(holds),
        )
        .await
    {
        crate::PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("{address}: expected Ok, got {other:?}"),
    }
}

pub use brenn_messaging_store::testutils::held;

/// Every retained message on `address`, oldest first, each with its retention
/// sequence, read out of the table rather than through `Messenger::query`,
/// which answers what some app may read.
///
/// # Panics
///
/// If `address` is not in the directory.
pub async fn retained_on(
    messenger: &Messenger,
    address: &str,
) -> Vec<(i64, brenn_envelope::MessageEnvelope)> {
    let uuid = messenger
        .directory()
        .resolve(address)
        .unwrap_or_else(|| panic!("{address} is in the directory"))
        .uuid;
    let conn = messenger.db().lock().await;
    db::load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
}

/// The blob id of each attachment `envelope` names, by name.
pub fn attachment_ids(
    envelope: &brenn_envelope::MessageEnvelope,
) -> std::collections::BTreeMap<String, String> {
    envelope
        .attachments
        .iter()
        .map(|(name, reference)| (name.as_str().to_string(), reference.id.as_str().to_string()))
        .collect()
}

/// Redeem `id` through `messenger` and read all of it, or the refusal.
pub async fn redeem_all(
    messenger: &Messenger,
    id: &crate::BlobId,
) -> Result<Vec<u8>, brenn_messaging_store::blob::BlobError> {
    let hold = messenger.retain_blob(id).await?;
    let size = usize::try_from(hold.reference().size).expect("a blob size fits in usize");
    hold.read(0, size)
}

/// Create, write and seal one blob in `messenger`'s blob store.
pub fn seal_blob(messenger: &Messenger, bytes: &[u8]) -> brenn_messaging_store::blob::BlobHold {
    brenn_messaging_store::testutils::seal_bytes(
        messenger
            .blob_store()
            .expect("the messenger has a blob store"),
        "application/octet-stream",
        bytes,
    )
}
