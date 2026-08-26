//! Surface test harness that boots the real composition root rather than
//! assembling fixtures by hand.

use std::sync::{Arc, Mutex};

use brenn_lib::messaging::config::ResolvedSurface;
use brenn_obs::alerting::AlertDispatcher;
use brenn_server::state::AppState;
use brenn_server::test_support::state::test_state_with_capturing_alerter;
use brenn_surface_server::test_fixtures::{
    TEST_MAX_BODY_BYTES, TEST_ORIGIN, install_surface_runtimes,
};

/// A harness over surfaces a real boot resolved, rather than surfaces a fixture
/// wrote.
///
/// For the cases whose subject is boot's own derivation: an auto channel's
/// address, its ring depth, and the ACL matchers that authorize its endpoints
/// are all derived from `link` and io_port declarations. A
/// fixture-built surface would carry a policy the test wrote itself, which
/// proves nothing about the wiring.
pub struct BootedSurfaceHarness {
    pub state: AppState,
    pub alerts: Arc<Mutex<Vec<(String, String)>>>,
    pub flusher: AlertDispatcher,
    /// The `Messenger` that boot produced, fully configured.
    pub messenger: Arc<brenn_messaging::Messenger>,
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
pub async fn booted_surface_harness(
    db: &brenn_db::Db,
    config: &brenn_lib::config::BrennConfig,
) -> BootedSurfaceHarness {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();
    let apps: Arc<indexmap::IndexMap<String, brenn_lib::config::AppConfig>> =
        Arc::new(indexmap::IndexMap::new());
    let result = brenn_messaging_boot::test_fixtures::boot_messaging_with(
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
        brenn_surface_server::fixtures_config::description_params(),
    ));
    // The publish path reaches an attached page through the router, so the
    // router needs the state its session registry lives on and a delivery route
    // per surface principal.
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
