//! Surface rigs that stand a whole `AppState` up: the `deskbar` surface
//! installed on real ring stores, its wake router wired to the state, and the
//! zero-alert guard the route's integration suites assert with.
//!
//! The surface shapes, channel declarations and runtime installer these build on
//! are `brenn_surface_server::test_fixtures`, a crate below; what is here is the
//! part that needs the server's state and its wake router.
//!
//! **Rigs model only configurations boot would accept.** A fixture installed
//! into a post-boot harness must satisfy every assertion boot makes about a
//! surface it starts; the coverage half is enforced in
//! `brenn_surface_server::test_fixtures::install_surface_runtimes`, which every
//! rig here calls, over the whole set it installs.

use std::sync::{Arc, Mutex};

use brenn_lib::messaging::ChannelEntry;
use brenn_lib::messaging::config::ResolvedSurface;
use brenn_obs::alerting::AlertDispatcher;
use brenn_surface_server::fixtures_config;
use brenn_surface_server::test_fixtures::{
    TEST_MAX_BODY_BYTES, brenn_channel_entry, declare_channels, fixture_messenger,
    install_surface_runtimes,
};

use crate::state::AppState;
use crate::test_support::state::test_state_with_capturing_alerter;

/// Capturing-alerter test state whose surface is backed by real ring stores
/// carrying the fixture channels.
///
/// `alerts` collects `(source, detail)` for any dispatched security event;
/// `flusher` shares the dispatcher's channel so `assert_no_alerts` can barrier
/// the background drainer before reading. `status_uuid` names the primary
/// surface's derived status channel, so a test can read the terminal stamp back.
pub struct SurfaceTestHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    /// The uuid of the primary surface's derived status channel.
    pub status_uuid: uuid::Uuid,
}

/// Build a `SurfaceTestHarness` around `surface` and a directory carrying
/// `entries` plus the surface's own derived status channel.
pub async fn surface_harness(
    db: &brenn_db::Db,
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
pub async fn surface_harness_with_siblings(
    db: &brenn_db::Db,
    surface: ResolvedSurface,
    siblings: Vec<ResolvedSurface>,
    mut entries: Vec<ChannelEntry>,
) -> SurfaceTestHarness {
    let params = fixtures_config::description_params();
    let status_uuid = uuid::Uuid::new_v4();
    entries.push(brenn_channel_entry(
        &brenn_surface_server::description::surface_status_bare(&params.prefix, &surface.slug),
        status_uuid,
    ));

    let mut surfaces = vec![surface];
    surfaces.extend(siblings);
    brenn_surface_server::boot_policy::inject_surface_geometry_status_grants(
        &mut surfaces,
        &params.prefix,
    );
    // Before the messenger: its directory carries the surface's subscribers, so
    // a subscription derived later would be invisible to the live fan-out. The
    // uuids come off the entries the directory is built from, as boot's do.
    for surface in &mut surfaces {
        fixtures_config::derive_wire_subscriptions(surface);
        fixtures_config::bind_wire_subscription_uuids(surface, &entries);
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

/// Drain the capturing alerter's channel, then assert no security event was
/// captured. `flusher` shares the dispatcher's channel; `flush` is a FIFO
/// barrier, so any alert dispatched before this call is in `alerts` by the time
/// the assertion runs — without it the read races the background drainer.
///
/// The barrier proves visibility of alerts *already dispatched*; it does not wait
/// wall-clock for the server to reach its dispatch point, so each caller must
/// already have a happens-before edge (an observed response frame, a barrier, an
/// observed close) proving server-side processing finished.
pub async fn assert_no_alerts(
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
