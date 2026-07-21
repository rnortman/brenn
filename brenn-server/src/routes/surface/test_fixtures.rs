//! Shared fixtures for the two surface integration suites (`ws_tests.rs` and
//! `client_tests.rs`). Both drive the same surface protocol — one by hand over
//! the wire, one through the real client crate — so they need the same
//! `deskbar` surface shape, bus construction, publish helper, and zero-alert
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
use brenn_lib::messaging::testutils::ephemeral_channel_entry;
use brenn_lib::messaging::{
    EphemeralBus, EphemeralChannelEntry, EphemeralPublishResult, ParticipantId, Urgency,
};
use brenn_lib::obs::alerting::AlertDispatcher;

use super::build_surface_runtimes;
use crate::state::AppState;
use crate::test_support::state::test_state_with_capturing_alerter;
use crate::test_support::surface::SurfaceFixture;

/// The origin stamped on fixture `Messenger`/`EphemeralBus` instances; a
/// reserved-system-app publisher resolves to `app:<slug>@<origin>`.
pub(crate) const TEST_ORIGIN: &str = "test-origin";

/// The body cap every surface fixture bus and runtime is built with. Assertions
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
/// own `bus.subscribe` passes its delivery-time ACL.
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
/// `bus.publish` passes its output-time ACL.
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

/// A fixture `EphemeralBus` carrying the given channels, with the shared test
/// origin and body cap. Single construction site so every surface test exercises
/// the same bus configuration.
pub(crate) fn fixture_bus(entries: Vec<EphemeralChannelEntry>) -> Arc<EphemeralBus> {
    EphemeralBus::new(entries, Arc::from(TEST_ORIGIN), TEST_MAX_BODY_BYTES)
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
        uuid: "11111111-1111-4111-8111-111111111111".to_string(),
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

/// Capturing-alerter test state whose surface is backed by a real `EphemeralBus`
/// carrying the fixture channels.
///
/// `alerts` collects `(source, detail)` for any dispatched security event;
/// `flusher` shares the dispatcher's channel so `assert_no_alerts` can barrier
/// the background drainer before reading. `bus` is the same `Arc` the runtime
/// holds, so a test can publish into it in-process.
pub(crate) struct SurfaceTestHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    pub bus: Arc<EphemeralBus>,
}

/// Build a `SurfaceTestHarness` around `surface` and a bus carrying `entries`.
///
/// The capturing alerter's drainer `JoinHandle` is dropped here: dropping a
/// tokio `JoinHandle` detaches the task, which keeps draining for the test's
/// lifetime. `flusher` (a clone of the dispatcher) is the barrier onto that
/// task; no test needs to await the handle.
pub(crate) fn surface_harness(
    db: &db::Db,
    surface: ResolvedSurface,
    entries: Vec<EphemeralChannelEntry>,
) -> SurfaceTestHarness {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    let bus = fixture_bus(entries);
    state.surfaces = Arc::new(build_surface_runtimes(
        vec![surface],
        bus.clone(),
        None,
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
    }
}

/// The dominant pattern in both suites: a `deskbar_sub` surface over a bus whose
/// single channel has the given retain depth and capacity.
pub(crate) fn subscribe_harness(
    db: &db::Db,
    retain_depth: u64,
    capacity: u32,
) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_sub(),
        vec![ephemeral_channel_entry(EPH_NAME, retain_depth, capacity)],
    )
}

/// Publish `n` copies of `body` onto `addr` (bare name `name`) as sender
/// `surface:{sender}`. Each sender has its own rate bucket, so splitting a flood
/// across senders keeps every one under the per-sender burst. Asserts each
/// publish is accepted — a failure means ACL/scheme/rate drift.
pub(crate) fn publish_as(
    bus: &EphemeralBus,
    sender: &str,
    addr: &str,
    name: &str,
    body: &str,
    n: usize,
) {
    let participant = ParticipantId::for_surface(sender);
    let policy = publish_policy(&[name]);
    for _ in 0..n {
        assert!(
            matches!(
                bus.publish(&participant, &policy, addr, body, Urgency::Normal),
                EphemeralPublishResult::Ok { .. }
            ),
            "fixture publish must succeed (ACL/scheme/rate drift otherwise)"
        );
    }
}

/// Publish one message onto the fixture channel `EPH_ADDR` as a distinct
/// publisher.
pub(crate) fn publish(bus: &EphemeralBus, body: &str) {
    publish_as(bus, "publisher", EPH_ADDR, EPH_NAME, body, 1);
}

/// Mint a durable resume cursor carrying the store's real identity, so it is not
/// caught by the stale-store arms when echoed back to a live durable subscribe.
/// Tests simulating a reconnect for a known high-water id use this.
pub(crate) async fn durable_resume(db: &db::Db, hw: i64) -> brenn_surface_proto::Cursor {
    durable_resume_with_confirm(db, hw, vec![]).await
}

/// Like [`durable_resume`] but carrying a below-water confirm set — the ack
/// evidence a page echoes for a below-water row it received.
pub(crate) async fn durable_resume_with_confirm(
    db: &db::Db,
    hw: i64,
    confirm: Vec<i64>,
) -> brenn_surface_proto::Cursor {
    let id = {
        let conn = db.lock().await;
        brenn_lib::messaging::db::read_store_identity(&conn)
    };
    super::cursor::mint_durable(id.generation, id.incarnation, hw, confirm)
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
