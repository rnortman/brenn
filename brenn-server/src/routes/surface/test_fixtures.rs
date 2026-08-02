//! Shared fixtures for the surface route's integration suite (`ws_tests.rs`)
//! and for the boot-side suites that install a surface on an `AppState`: the
//! `deskbar` surface shape, store construction, the publish helper, and the
//! zero-alert guard. Keeping one copy here means a `ResolvedSurface` shape
//! change or a synchronization fix lands once, not once per caller.
//!
//! This is surface-suite-specific and deliberately not in `crate::test_support`
//! (which holds crate-wide helpers); it consumes `test_support` rather than
//! belonging to it.
//!
//! **Rigs model only configurations boot would accept.** A fixture installed
//! into a post-boot harness must satisfy every assertion boot makes about a
//! surface it starts — its transportable outputs name declared channels, and its
//! own policy covers each of those outputs
//! (`bootstrap::messaging::assert_output_bindings_covered`). A fixture that
//! diverges lets a reader reconcile a production/fixture disagreement toward the
//! fixture. The coverage half is enforced in [`install_surface_runtimes`], which
//! every rig that puts surfaces on an `AppState` calls, over the whole set it
//! installs. The rule governs running-server rigs only: a fixture whose purpose
//! is to pin boot's own *rejection* (e.g. [`surface_outputting_to`]) models
//! config under validation, not a running server, and is exempt by nature.

use std::sync::{Arc, Mutex};

use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db;
use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{
    AttachSendBudget, ChannelConfigRaw, Depth, MessagingGlobalConfig, ResolvedComponent,
    ResolvedSurface, SurfaceOutput, build_channel_entries,
};
use brenn_lib::messaging::store::RingStores;
use brenn_lib::messaging::{ChannelEntry, Urgency};
use brenn_lib::obs::alerting::AlertDispatcher;

use super::{SurfaceDescriptionParams, SurfaceRuntime, build_surface_runtimes};
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
/// The port `COMPONENT` publishes on.
const OUT_PORT: &str = "out";

/// A `deskbar` surface binding the fixture channel both ways on `COMPONENT`: a
/// subscription so an attachment can be delivered on it, and an output so the
/// attachment can publish onto it under the component's own attribution. One
/// channel for both directions makes a published message its own delivery, which
/// is what lets a single socket prove the whole loop.
///
/// Shared by both attachment suites so a new required grant or binding reaches
/// both at once. Empty `allowed_users` admits any authenticated user.
pub(crate) fn deskbar_loop(allowed_users: Vec<String>) -> ResolvedSurface {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralSubscribe);
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(EPH_ADDR, COMPONENT, PORT)
        .output(EPH_ADDR, COMPONENT, OUT_PORT)
        .policy(policy)
        .allowed_users(allowed_users)
        .build()
}

/// The fixture's ring stores, one per channel among `entries`.
///
/// Rejects a durable entry: a durable channel's retention is the database's, so
/// a fixture that declares one owes it a DB row, and `declare_channels` is the
/// only thing here that writes one.
pub(crate) fn fixture_stores(entries: &[ChannelEntry]) -> Arc<RingStores> {
    assert!(
        entries.iter().all(|e| !e.capabilities().durable),
        "a durable channel's retention is the database's, not a ring store; \
         build the registry with declare_channels"
    );
    Arc::new(RingStores::build(entries))
}

/// Declare `entries` the way boot does: every durable channel gets its DB row,
/// every non-durable one an in-memory ring store. Returns the store registry.
///
/// The caller hands the whole set to the directory — boot's final directory
/// carries both halves — so this is the one place a fixture's durable channel
/// becomes a channel the messenger can resolve.
pub(crate) async fn declare_channels(db: &db::Db, entries: &[ChannelEntry]) -> Arc<RingStores> {
    let (nondurable, durable): (Vec<ChannelEntry>, Vec<ChannelEntry>) = entries
        .iter()
        .cloned()
        .partition(|e| !e.capabilities().durable);
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, &durable);
    }
    fixture_stores(&nondurable)
}

/// One `brenn:` channel declaration built the way boot builds it
/// (`build_channel_entries` over a raw `[[channel]]` shape), at the given uuid so
/// a test can read the channel's persisted rows back.
pub(crate) fn brenn_channel_entry(bare_address: &str, uuid: uuid::Uuid) -> ChannelEntry {
    channel_entry_at(bare_address, &uuid.to_string(), None)
}

/// The shared raw→resolved path behind the declaration helpers.
fn channel_entry_at(
    bare_address: &str,
    uuid: &str,
    standing_retain_depth: Option<Depth>,
) -> ChannelEntry {
    let raw = ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid.to_string()),
        address: Some(bare_address.to_string()),
        address_prefix: None,
        description: None,
        // The channel's own rungs sit at its standing depth so a fixture channel
        // never becomes the thing under test: a surface test measures the
        // binding's window, not the channel's.
        push_depth: Some(standing_retain_depth.unwrap_or(Depth::Unbounded)),
        retain_depth: Some(standing_retain_depth.unwrap_or(Depth::Unbounded)),
        standing_retain_depth: standing_retain_depth.or(Some(Depth::Unbounded)),
        noise: None,
        sink: None,
        wake_min: None,
    };
    build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one raw channel resolves to one entry")
}

/// Directory holding one declared `brenn:` channel with the given bare address,
/// built the same way boot does (`build_channel_entries`). `standing_retain_depth`
/// sets the channel's standing retain depth (so its reap frontier is exactly that
/// value, no subscribers); `None` leaves it at the global default (`Unbounded` →
/// pinned).
pub(crate) fn directory_with_standing(
    bare_address: &str,
    standing_retain_depth: Option<Depth>,
) -> MessagingDirectory {
    MessagingDirectory::with_entries(vec![channel_entry_at(
        bare_address,
        "11111111-1111-4111-8111-111111111111",
        standing_retain_depth,
    )])
}

/// Directory with one declared `brenn:` channel at the default (pinned) standing
/// depth.
pub(crate) fn directory_with(bare_address: &str) -> MessagingDirectory {
    directory_with_standing(bare_address, None)
}

/// A minimal single-component surface whose sole output binding targets
/// `channel_address` — the foreign-writer case both single-writer sweeps reject.
/// All other fields are inert defaults the sweep never reads.
///
/// Exempt from the module's boot-validity rule: its default policy covers no
/// output, which is a shape boot refuses, and refusing it is the point. It is
/// config under validation, never installed into a running harness.
pub(crate) fn surface_outputting_to(channel_address: &str) -> ResolvedSurface {
    ResolvedSurface {
        slug: "writer-surface".to_string(),
        skin: "bench".to_string(),
        components: vec![ResolvedComponent {
            instance: "writer".to_string(),
            kind: "writer".to_string(),
            abi: brenn_surface_schema::Abi::Dom,
            send_budget: AttachSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }],
        subscriptions: vec![],
        wire_subscriptions: vec![],
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
/// the background drainer before reading. `status_uuid` names the primary
/// surface's derived status channel, so a test can read the terminal stamp back.
pub(crate) struct SurfaceTestHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    /// The uuid of the primary surface's derived status channel.
    pub status_uuid: uuid::Uuid,
}

/// Build a `SurfaceTestHarness` around `surface` and a directory carrying
/// `entries` plus the surface's own derived status channel.
pub(crate) async fn surface_harness(
    db: &db::Db,
    surface: ResolvedSurface,
    entries: Vec<ChannelEntry>,
) -> SurfaceTestHarness {
    surface_harness_with_siblings(db, surface, vec![], entries).await
}

/// [`surface_harness`] with further surfaces installed alongside the primary
/// one, for the cases whose subject is what one attachment can reach of
/// *another* surface's wiring. Only the primary surface gets a messenger built
/// around its subscribers; the siblings are installed for their authority maps.
///
/// The capturing alerter's drainer `JoinHandle` is dropped here: dropping a
/// tokio `JoinHandle` detaches the task, which keeps draining for the test's
/// lifetime. `flusher` (a clone of the dispatcher) is the barrier onto that
/// task; no test needs to await the handle.
///
/// **Every rig this builds tears down the way a booted surface does.** The last
/// attachment of a surface makes the route write its terminal `disconnected`
/// stamp, which a rig missing the status channel or the telemetry grant answers
/// with a broken-boot-invariant panic on the connection task — a panic tokio
/// absorbs, leaving the provoking test green. So the status channel is declared
/// and the substrate grant injected here, off the same description prefix the
/// runtimes are built with, rather than left to each caller.
pub(crate) async fn surface_harness_with_siblings(
    db: &db::Db,
    surface: ResolvedSurface,
    siblings: Vec<ResolvedSurface>,
    mut entries: Vec<ChannelEntry>,
) -> SurfaceTestHarness {
    let params = crate::test_support::surface::description_params();
    let status_uuid = uuid::Uuid::new_v4();
    entries.push(brenn_channel_entry(
        &super::description::surface_status_bare(&params.prefix, &surface.slug),
        status_uuid,
    ));

    let mut surfaces = vec![surface];
    surfaces.extend(siblings);
    crate::bootstrap::messaging::inject_surface_geometry_status_grants(
        &mut surfaces,
        &params.prefix,
    );
    // Before the messenger: its directory carries the surface's subscribers, so
    // a subscription derived later would be invisible to the live fan-out. The
    // uuids come off the entries the directory is built from, as boot's do.
    for surface in &mut surfaces {
        crate::test_support::surface::derive_wire_subscriptions(surface);
        crate::test_support::surface::bind_wire_subscription_uuids(surface, &entries);
    }
    let stores = declare_channels(db, &entries).await;

    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    // The surface publishes through the Messenger, so the fixture needs one
    // with the channels, stores, and a subscriber registration carrying the
    // surface's boot-resolved policy.
    let router = Arc::new(crate::messaging_router::WakeRouterImpl::new(
        crate::active_bridge::ActiveBridges::new(),
    ));
    router.register_surface_delivery_routes(&surfaces[0]);
    let messenger = fixture_messenger(
        db,
        &entries,
        &surfaces[0],
        Arc::clone(&stores),
        router.clone(),
        TEST_MAX_BODY_BYTES,
    );
    state.messenger = Some(Arc::clone(&messenger));
    state.surfaces = Arc::new(install_surface_runtimes(
        surfaces,
        Some(Arc::clone(&messenger)),
        TEST_MAX_BODY_BYTES,
        None,
        params,
    ));
    router.set_state(state.clone());
    SurfaceTestHarness {
        state,
        alerts,
        flusher,
        status_uuid,
    }
}

/// A harness over surfaces a real boot resolved, rather than surfaces a fixture
/// wrote.
///
/// For the cases whose subject is boot's own derivation: an auto channel's
/// address, its ring depth, and the ACL matchers that authorize its endpoints
/// are all derived from `[[connection]]` and io_port declarations. A
/// fixture-built surface would carry a policy the test wrote itself, which
/// proves nothing about the wiring.
pub(crate) struct BootedSurfaceHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    /// The `Messenger` that boot produced, fully configured.
    pub messenger: Arc<brenn_lib::messaging::Messenger>,
    pub surfaces: Vec<ResolvedSurface>,
}

impl BootedSurfaceHarness {
    /// The channel address boot resolved for the first surface's
    /// `(instance, port)` subscription. An anonymous auto channel's address is a
    /// uuid nobody wrote down, so a test reads it back off the resolution rather
    /// than recomputing it.
    pub fn surface_sub_address(&self, instance: &str, port: &str) -> String {
        self.surfaces[0]
            .subscriptions
            .iter()
            .find(|b| b.instance == instance && b.port == port)
            .unwrap_or_else(|| panic!("boot resolved no subscription on {instance}/{port}"))
            .channel_address
            .clone()
    }
}

/// Boot `config` with the standard no-op periphery and install its surfaces on a
/// capturing-alerter `AppState`, over the boot's own messenger.
///
/// A config handed here must declare each of its surfaces' derived status
/// channels, for the reason [`surface_harness_with_siblings`] declares one: the
/// last attachment's teardown writes the terminal `disconnected` stamp, and a
/// rig that cannot take that publish panics on a task nothing asserts against.
pub(crate) async fn booted_surface_harness(
    db: &db::Db,
    config: &brenn_lib::config::BrennConfig,
) -> BootedSurfaceHarness {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    let apps: Arc<indexmap::IndexMap<String, brenn_lib::config::AppConfig>> =
        Arc::new(indexmap::IndexMap::new());
    let result = crate::bootstrap::messaging::test_fixtures::boot_messaging_with(
        config,
        db.clone(),
        &apps,
        flusher.clone(),
        TEST_ORIGIN,
    )
    .await;
    let messenger = result
        .messenger
        .expect("a config with a surface configures messaging");
    state.surfaces = Arc::new(install_surface_runtimes(
        result.surfaces.clone(),
        Some(Arc::clone(&messenger)),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    // The publish path reaches an attached page through the router, so the
    // router needs the state its session registry lives on and a delivery route
    // per surface principal — the wiring `bootstrap/mod.rs` does once the real
    // `AppState` exists.
    let router = result
        .router
        .as_ref()
        .expect("a config with a surface configures messaging");
    for surface in &result.surfaces {
        router.register_surface_delivery_routes(surface);
    }
    router.set_state(state.clone());
    BootedSurfaceHarness {
        state,
        alerts,
        flusher,
        messenger,
        surfaces: result.surfaces,
    }
}

/// Build the runtimes for `surfaces` and hand them back for an `AppState` to
/// hold, asserting first that the whole installed set satisfies boot's
/// output-coverage rule (the module's boot-validity rule).
///
/// The assertion runs in boot's own order — a rig that widens a policy through a
/// substrate injector has already done so by the time it installs — so an output
/// bound to the configured error channel is covered by the injected grant rather
/// than failing coverage a fixture constructor away from the injection. It also
/// sees every surface a rig installs, including second surfaces that no
/// messenger was built for.
pub(crate) fn install_surface_runtimes(
    mut surfaces: Vec<ResolvedSurface>,
    messenger: Option<Arc<brenn_lib::messaging::Messenger>>,
    max_body_bytes: usize,
    error_channel: Option<String>,
    surface_description: SurfaceDescriptionParams,
) -> std::collections::HashMap<String, Arc<SurfaceRuntime>> {
    for surface in &mut surfaces {
        crate::test_support::surface::derive_wire_subscriptions(surface);
    }
    crate::bootstrap::messaging::assert_output_bindings_covered(&surfaces);
    build_surface_runtimes(
        surfaces,
        messenger,
        max_body_bytes,
        error_channel,
        surface_description,
    )
}

/// A `Messenger` over the fixture's channels, wired the way boot wires one: the
/// surface's own policy at **every** principal grain
/// (`ResolvedSurface::principals` — the kernel identity plus one per declared
/// component instance), and one send-budget bucket per principal
/// `ResolvedSurface::principal_send_budgets` names. Both read the same
/// declaration set boot reads, so a fixture cannot register or budget a
/// different principal set than the surface it installs.
///
/// Every grain is registered because surface target resolution fails closed on a
/// missing registration: an instance-grain miss is a silent delivery denial, not
/// an error, so a kernel-grain-only map would make a durable subscription's
/// deliveries vanish instead of failing loudly. A missing budget bucket is a "no
/// send budget" panic at the publish gate, not a silently unmetered publish.
///
/// `max_body_bytes` is a parameter rather than [`TEST_MAX_BODY_BYTES`] because
/// the body cap is a subject of its own: the oversize arms of the telemetry
/// stamp are only reachable against a cap the composed body exceeds.
pub(crate) fn fixture_messenger(
    db: &db::Db,
    entries: &[ChannelEntry],
    surface: &ResolvedSurface,
    stores: Arc<RingStores>,
    router: Arc<crate::messaging_router::WakeRouterImpl>,
    max_body_bytes: usize,
) -> Arc<brenn_lib::messaging::Messenger> {
    use brenn_lib::messaging::{
        SubscriberEntryKind, SubscriberRegistration, WakeEconomics, WakeRouter,
    };

    let messenger = brenn_lib::messaging::Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(
            project_surface_subscribers(entries, surface),
        )),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig {
            max_body_bytes,
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
                SubscriberEntryKind::Surface(surface.slug.clone()),
                registration,
            )]
            .into(),
        )
        .with_attach_send_budgets(brenn_lib::messaging::attach_principal_budgets(
            brenn_lib::messaging::AttachScope::surface(&surface.slug),
            surface.principal_send_budgets().collect(),
        ))
        .with_ring_stores(stores)
}

/// Project the surface's resolved wire subscriptions onto the channel entries,
/// as boot's `finalize_directory_with_subscribers` does: the commit's surface
/// fan-out resolves its targets from the directory, so a subscription the entry
/// does not carry is fed nothing.
///
/// Keyed by uuid, as boot keys it. The caller has already joined each
/// subscription to its entry (`bind_wire_subscription_uuids`), so this cannot
/// silently match nothing.
pub(super) fn project_surface_subscribers(
    entries: &[ChannelEntry],
    surface: &ResolvedSurface,
) -> Vec<ChannelEntry> {
    entries
        .iter()
        .cloned()
        .map(|mut entry| {
            entry.subscribers.extend(
                surface
                    .wire_subscriptions
                    .iter()
                    .filter(|sub| sub.subscription.channel_uuid == entry.uuid)
                    .map(|sub| brenn_lib::messaging::SubscriberEntry {
                        kind: brenn_lib::messaging::SubscriberEntryKind::Surface(
                            surface.slug.clone(),
                        ),
                        push_depth: sub.subscription.push_depth,
                        retain_depth: sub.subscription.retain_depth,
                        noise: sub.subscription.noise,
                        wake_min: None,
                    }),
            );
            entry
        })
        .collect()
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
