//! Shared `#[cfg(test)]` fixtures for the core and driver test suites, so a
//! change to the standard config or `Welcome` shape lands in one place.

use std::time::Duration;

use brenn_surface_proto::{Binding, LocalChannel, OutputBinding};
use uuid::Uuid;

use crate::CoreConfig;

/// The standard client config fixture (deskbar surface, ws.ts-parity defaults).
pub(crate) fn cfg() -> CoreConfig {
    CoreConfig {
        url: "wss://host/surface/deskbar/ws".into(),
        build_id: "buildxyz".into(),
        initial_backoff: Duration::from_secs(3),
        max_backoff: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(15),
        liveness_multiplier: 3,
        // Pin the jitter seed so the core stays deterministic in tests; the fleet
        // gets distinct per-target seeds via `transport::entropy::seed`.
        backoff_jitter_seed: 0,
        // Pin the page-load epoch for the same reason: a test asserting an exact
        // `LocalPos` needs it fixed. Real pages mint one per load in
        // `handle::new`.
        local_epoch: TEST_LOCAL_EPOCH,
    }
}

/// The pinned page-load epoch [`cfg`] gives the core, so a test can assert the
/// exact `LocalPos` the router assigns.
pub(crate) const TEST_LOCAL_EPOCH: Uuid =
    Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00e0);

/// The standard ungranted `deskbar`/`protobar` `Welcome` frame with
/// caller-chosen bindings. Delegates to the shared fixtures builder,
/// pinning the grant off and the single `protobar` instance the client suites
/// assume; the alert-grant and multi-instance cases call the fixtures builder
/// directly.
pub(crate) fn welcome_frame(subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) -> String {
    brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
        subscriptions,
        outputs,
        alert_granted: false,
        takeover_granted: false,
        components: vec!["protobar"],
        error_report_floor: None,
        surface_description: brenn_surface_proto::SurfaceDescription {
            status_interval_secs: 60,
        },
        local_channels: Vec::new(),
        max_body_bytes: brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
    })
}

/// The standard `deskbar`/`protobar` `Welcome` plus page-local wiring: the
/// caller's `local_channels` router table alongside its bindings.
pub(crate) fn welcome_frame_local(
    subscriptions: Vec<Binding>,
    outputs: Vec<OutputBinding>,
    local_channels: Vec<LocalChannel>,
) -> String {
    brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
        subscriptions,
        outputs,
        alert_granted: false,
        takeover_granted: false,
        components: vec!["protobar"],
        error_report_floor: None,
        surface_description: brenn_surface_proto::SurfaceDescription {
            status_interval_secs: 60,
        },
        local_channels,
        max_body_bytes: brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
    })
}

/// The standard `deskbar`/`protobar` `Welcome` with the error-report floor
/// advertised at `warn`, so `ClientHandle::report` publishes warn/error reports
/// to the reserved `#brenn`/`error-reports` port.
pub(crate) fn welcome_frame_reports(subscriptions: Vec<Binding>) -> String {
    welcome_frame_reports_with_outputs_and_subs(subscriptions, vec![])
}

/// As [`welcome_frame_reports`] but with caller-chosen output bindings and no
/// subscriptions — the state the publish path exercises.
pub(crate) fn welcome_frame_reports_with_outputs(outputs: Vec<OutputBinding>) -> String {
    welcome_frame_reports_with_outputs_and_subs(vec![], outputs)
}

fn welcome_frame_reports_with_outputs_and_subs(
    subscriptions: Vec<Binding>,
    outputs: Vec<OutputBinding>,
) -> String {
    brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
        subscriptions,
        outputs,
        alert_granted: false,
        takeover_granted: false,
        components: vec!["protobar"],
        error_report_floor: Some(brenn_surface_proto::LogLevel::Warn),
        surface_description: brenn_surface_proto::SurfaceDescription {
            status_interval_secs: 60,
        },
        local_channels: Vec::new(),
        max_body_bytes: brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
    })
}
