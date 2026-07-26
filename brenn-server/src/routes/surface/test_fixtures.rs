//! Shared fixtures for the two surface integration suites (`ws_tests.rs` and
//! `client_tests.rs`). Both drive the same surface protocol — one by hand over
//! the wire, one through the real client crate — so they need the same
//! `deskbar` surface shape, store construction, publish helper, and zero-alert
//! guard. Keeping one copy here means a `ResolvedSurface` shape change or a
//! synchronization fix lands once, not once per suite.
//!
//! This is surface-suite-specific and deliberately not in `crate::test_support`
//! (which holds crate-wide helpers); it consumes `test_support` rather than
//! belonging to it.

use std::sync::{Arc, Mutex};

use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db;
use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, MessagingGlobalConfig, ResolvedComponent, ResolvedSurface,
    SurfaceOutput, SurfaceSendBudget, build_channel_entries,
};
use brenn_lib::messaging::store::RingStores;
use brenn_lib::messaging::testutils::ephemeral_channel_entry;
use brenn_lib::messaging::{ChannelEntry, ParticipantId, Urgency};
use brenn_lib::obs::alerting::AlertDispatcher;

use super::build_surface_runtimes;
use crate::state::AppState;
use crate::test_support::state::test_state_with_capturing_alerter;
use crate::test_support::surface::SurfaceFixture;

/// The origin stamped on fixture `Messenger` instances; a reserved-system-app
/// publisher resolves to `app:<slug>@<origin>`.
pub(crate) const TEST_ORIGIN: &str = "test-origin";

/// The body cap every surface fixture runtime is built with. Assertions
/// against a differently-typed target (e.g. `Welcome.max_body_bytes: u64`) cast
/// at the site rather than defining a parallel const.
pub(crate) const TEST_MAX_BODY_BYTES: usize = 65_536;

/// The ephemeral subscription channel the `deskbar` fixture binds.
pub(crate) const EPH_ADDR: &str = "ephemeral:protobar-demo";
/// Bare channel name (scheme stripped) the ACL matcher keys on.
pub(crate) const EPH_NAME: &str = "protobar-demo";
/// The component the `deskbar` fixture binds `EPH_ADDR` to.
pub(crate) const COMPONENT: &str = "protobar";
/// The port `EPH_ADDR` binds on `COMPONENT`.
pub(crate) const PORT: &str = "messages";

/// Policy granting ephemeral subscribe on each named channel, so the runtime's
/// own live attach passes its delivery-time ACL.
pub(crate) fn subscribe_policy(names: &[&str]) -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::EphemeralSubscribe);
    p.acls.ephemeral_subscribe = names
        .iter()
        .map(|n| ChannelMatcher::Exact(n.to_string()))
        .collect();
    p
}

/// Policy granting ephemeral publish on each named channel, so the runtime's own
/// publishes pass the output-time ACL.
pub(crate) fn publish_policy(names: &[&str]) -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::EphemeralPublish);
    p.acls.ephemeral_publish = names
        .iter()
        .map(|n| ChannelMatcher::Exact(n.to_string()))
        .collect();
    p
}

/// A `deskbar` surface binding the one ephemeral subscription channel to
/// `(protobar, messages)`, with a policy that covers it.
pub(crate) fn deskbar_sub() -> ResolvedSurface {
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(EPH_ADDR, COMPONENT, PORT)
        .policy(subscribe_policy(&[EPH_NAME]))
        .build()
}

/// `deskbar_sub`'s surface with its one binding turned into a **context feed**:
/// `push_depth = 0`, so the subscription has no push window, with retained
/// context behind it. Not declarable from config while `dom` is the only ABI
/// (boot rejects depth 0 there), hence built here.
pub(crate) fn deskbar_context_feed() -> ResolvedSurface {
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe_at_depths(EPH_ADDR, COMPONENT, PORT, 0, 4)
        .policy(subscribe_policy(&[EPH_NAME]))
        .build()
}

/// The fixture's ring stores, one per given non-durable channel. Single
/// construction site so every surface test exercises the same registry shape.
pub(crate) fn fixture_stores(entries: Vec<ChannelEntry>) -> Arc<RingStores> {
    Arc::new(RingStores::build(&entries))
}

/// Directory holding one declared `brenn:` channel with the given bare address,
/// built the same way boot does (`build_channel_entries`). `standing_retain_depth`
/// sets the channel's standing retain depth (so its reap frontier is exactly that
/// value, no subscribers); `None` leaves it at the global default (`Unbounded` →
/// pinned). Shared by the single-writer channel-validation suites in `mod.rs` and
/// `description.rs`.
pub(crate) fn directory_with_standing(
    bare_address: &str,
    standing_retain_depth: Option<Depth>,
) -> MessagingDirectory {
    let raw = ChannelConfigRaw {
        send_rate: None,
        uuid: Some("11111111-1111-4111-8111-111111111111".to_string()),
        address: bare_address.to_string(),
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entries = build_channel_entries(&[raw], &MessagingGlobalConfig::default());
    MessagingDirectory::with_entries(entries)
}

/// Directory with one declared `brenn:` channel at the default (pinned) standing
/// depth.
pub(crate) fn directory_with(bare_address: &str) -> MessagingDirectory {
    directory_with_standing(bare_address, None)
}

/// A minimal single-component surface whose sole output binding targets
/// `channel_address` — the foreign-writer case both single-writer sweeps reject.
/// All other fields are inert defaults the sweep never reads.
pub(crate) fn surface_outputting_to(channel_address: &str) -> ResolvedSurface {
    ResolvedSurface {
        slug: "writer-surface".to_string(),
        skin: "bench".to_string(),
        components: vec![ResolvedComponent {
            instance: "writer".to_string(),
            kind: "writer".to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }],
        subscriptions: vec![],
        durable_subscriptions: vec![],
        local_channels: vec![],
        outputs: vec![SurfaceOutput {
            channel_address: channel_address.to_string(),
            instance: "writer".to_string(),
            port: "out".to_string(),
            default_urgency: Urgency::Normal,
            budget: brenn_budget::SinkBudget {
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            },
        }],
        policy: AppPolicy::default(),
        allowed_users: vec![],
        publish_burst: 60,
        publish_per_sec: 1,
    }
}

/// Capturing-alerter test state whose surface is backed by real ring stores
/// carrying the fixture channels.
///
/// `alerts` collects `(source, detail)` for any dispatched security event;
/// `flusher` shares the dispatcher's channel so `assert_no_alerts` can barrier
/// the background drainer before reading. `stores` is the same registry the
/// runtime's `Messenger` holds, so a test can commit into it in-process.
pub(crate) struct SurfaceTestHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    pub stores: Arc<RingStores>,
    /// The same `Messenger` the runtime holds — the reader for the live-stream
    /// counters and the non-durable incarnation epoch.
    pub messenger: Arc<brenn_lib::messaging::Messenger>,
}

/// Build a `SurfaceTestHarness` around `surface` and stores carrying `entries`.
///
/// The capturing alerter's drainer `JoinHandle` is dropped here: dropping a
/// tokio `JoinHandle` detaches the task, which keeps draining for the test's
/// lifetime. `flusher` (a clone of the dispatcher) is the barrier onto that
/// task; no test needs to await the handle.
pub(crate) fn surface_harness(
    db: &db::Db,
    surface: ResolvedSurface,
    entries: Vec<ChannelEntry>,
) -> SurfaceTestHarness {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    let stores = fixture_stores(entries.clone());
    // The surface publishes through the Messenger, so the fixture needs one
    // with the channels, stores, and a subscriber registration carrying the
    // surface's boot-resolved policy.
    let messenger = fixture_messenger(db, &entries, &surface, Arc::clone(&stores));
    state.surfaces = Arc::new(build_surface_runtimes(
        vec![surface],
        Some(Arc::clone(&messenger)),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    SurfaceTestHarness {
        state,
        alerts,
        flusher,
        stores,
        messenger,
    }
}

/// A `Messenger` over the fixture's channels, wired the way boot wires one.
pub(crate) fn fixture_messenger(
    db: &db::Db,
    entries: &[ChannelEntry],
    surface: &ResolvedSurface,
    stores: Arc<RingStores>,
) -> Arc<brenn_lib::messaging::Messenger> {
    use brenn_lib::messaging::{
        SubscriberEntryKind, SubscriberRegistration, WakeEconomics, WakeRouter,
    };

    let messenger = brenn_lib::messaging::Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(entries.to_vec())),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig {
            max_body_bytes: TEST_MAX_BODY_BYTES,
            ..Default::default()
        },
    );
    let registration = SubscriberRegistration {
        policy: Arc::new(surface.policy.clone()),
        wake: WakeEconomics::Eager,
    };
    messenger
        .with_subscriber_registrations(
            [(
                SubscriberEntryKind::Surface {
                    slug: surface.slug.clone(),
                    instance: None,
                },
                registration,
            )]
            .into_iter()
            .collect(),
        )
        .with_ring_stores(stores)
}

/// The dominant pattern in both suites: a `deskbar_sub` surface over a registry
/// whose single channel has the given retain depth.
pub(crate) fn subscribe_harness(db: &db::Db, retain_depth: u64) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_sub(),
        vec![ephemeral_channel_entry(EPH_NAME, retain_depth)],
    )
}

/// Publish `n` copies of `body` onto `addr` (bare name `name`) as sender
/// `surface:{sender}`. Each sender has its own rate bucket, so splitting a flood
/// across senders keeps every one under the per-sender burst. Asserts each
/// publish is accepted — a failure means ACL/scheme/rate drift.
pub(crate) fn publish_as(stores: &RingStores, sender: &str, addr: &str, body: &str, n: usize) {
    let participant = ParticipantId::for_surface(sender);
    for _ in 0..n {
        commit_eph(stores, addr, &participant, body);
    }
}

/// Commit one message on an `ephemeral:` channel, bypassing the publish gates.
pub(super) fn commit_eph(stores: &RingStores, addr: &str, sender: &ParticipantId, body: &str) {
    stores
        .get_by_address(addr)
        .unwrap_or_else(|| panic!("channel {addr:?} has no store in this registry"))
        .append(brenn_lib::messaging::MessageEnvelope {
            message_id: uuid::Uuid::new_v4(),
            source: "test".into(),
            channel: addr.to_string(),
            sender: sender.as_str().into(),
            publish_ts: chrono::Utc::now(),
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Ephemeral,
        });
}

/// Publish one message onto the fixture channel `EPH_ADDR` as a distinct
/// publisher.
pub(crate) fn publish(stores: &RingStores, body: &str) {
    publish_as(stores, "publisher", EPH_ADDR, body, 1);
}

/// Mint the resume cursor a page would hold after receiving the message
/// `message_id`: the message's own channel, epoch, and retention position, under
/// the store's real incarnation, so it is not caught as stale when echoed back to
/// a live durable subscribe.
pub(crate) async fn durable_resume(db: &db::Db, message_id: i64) -> brenn_surface_proto::Cursor {
    let (channel_uuid, seq) = {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT channel_uuid, retained_seq FROM messaging_messages WHERE id = ?1",
            rusqlite::params![message_id],
            |row| {
                let uuid: Vec<u8> = row.get(0)?;
                let seq: i64 = row.get(1)?;
                Ok((uuid::Uuid::from_slice(&uuid).expect("channel uuid"), seq))
            },
        )
        .unwrap_or_else(|e| panic!("no retained message {message_id} to resume from: {e}"))
    };
    durable_resume_at(db, channel_uuid, seq as u64).await
}

/// Mint a resume cursor naming an arbitrary position in a channel's retention
/// order — including `0`, the anchor a page carries before it has received
/// anything on the channel.
pub(crate) async fn durable_resume_at(
    db: &db::Db,
    channel_uuid: uuid::Uuid,
    seq: u64,
) -> brenn_surface_proto::Cursor {
    let (incarnation, epoch) = {
        let conn = db.lock().await;
        (
            brenn_lib::messaging::db::read_store_identity(&conn).incarnation,
            brenn_lib::messaging::db::channel_resume_epoch(&conn, channel_uuid),
        )
    };
    super::cursor::mint(incarnation, epoch, seq)
}

/// Drain the capturing alerter's channel, then assert no security event was
/// captured. `flusher` shares the dispatcher's channel; `flush` is a FIFO
/// barrier, so any alert dispatched before this call is in `alerts` by the time
/// the assertion runs — without it the read races the background drainer.
///
/// The barrier proves visibility of alerts *already dispatched*; it does not wait
/// wall-clock for the server to reach its dispatch point, so each caller must
/// already have a happens-before edge (an observed response frame, a barrier, an
/// observed close) proving server-side processing finished.
pub(crate) async fn assert_no_alerts(
    flusher: &AlertDispatcher,
    alerts: &Arc<Mutex<Vec<(String, String)>>>,
    context: &str,
) {
    flusher.flush().await;
    let captured = alerts.lock().unwrap();
    assert!(
        captured.is_empty(),
        "{context}: a conformant session emitted security events: {captured:?}"
    );
}
