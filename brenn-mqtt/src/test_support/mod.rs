//! Fixtures for tests that need a real broker.
//!
//! Compiled only under the `testutils` feature.
//! The crates above this one drive reload, ingress routing and egress through
//! the same `mosquitto` a production session dials, so the broker harness and
//! its throwaway TLS material live here rather than in one suite's `tests/`
//! directory where only that suite can reach them.
//!
//! Nothing here is reachable from a release build: the release configurations
//! clear the feature.

pub mod broker;
pub mod certs;
pub mod client;
pub mod poll;

pub use broker::{
    BrokerHarness, DEFAULT_ACL, DEFAULT_CONF_TEMPLATE, log_records_publish_to_subscriber,
    log_records_unsubscribe,
};
pub use client::{
    await_puback, direct_publisher_acked, session_client_id, wait_for_filter_acked, wait_for_health,
};
pub use poll::{POLL_INTERVAL, poll_until};

/// The environment variable a broker-backed test target sets to opt in.
///
/// Spelled once here and read by [`broker_available`]; the `env` entry of every
/// such target in `BUILD.bazel` is the other half of the pair.
pub const BROKER_GATE: &str = "BRENN_MQTT_INTEGRATION";

/// Set by Bazel's test runner in every test action; no other runner sets it.
pub const BAZEL_TEST_MARKER: &str = "TEST_SRCDIR";

/// Whether the cases that need a real `mosquitto` may run.
///
/// The one place this decision is made, for every suite in the workspace that
/// drives a broker: `mosquitto` is a system binary, so a developer running a
/// test binary by hand is not required to have one and the cases return without
/// spawning anything. Under Bazel the gate is set by the target, so an unset
/// gate there can only mean the target lost its `env` entry — and a skip would
/// report a pass over nothing asserted, which is a panic instead.
///
/// # Panics
///
/// If the gate is unset under Bazel's test runner.
pub fn broker_available() -> bool {
    if std::env::var_os(BROKER_GATE).is_some() {
        return true;
    }
    assert!(
        std::env::var_os(BAZEL_TEST_MARKER).is_none(),
        "{BROKER_GATE} is unset under Bazel's test runner ({BAZEL_TEST_MARKER} is set): the \
         broker test target lost its `env` entry, and skipping would report a pass over nothing \
         asserted",
    );
    eprintln!("{BROKER_GATE} unset; skipping the cases that need a broker");
    false
}

/// Return from the calling test unless a broker may be started.
///
/// The statement form of [`broker_available`], so a suite's cases open with one
/// line and no suite writes the policy itself.
#[macro_export]
macro_rules! broker_gate {
    () => {
        if !$crate::test_support::broker_available() {
            return;
        }
    };
}
