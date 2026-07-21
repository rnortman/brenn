pub(crate) mod app_config;
pub(crate) mod http;
pub(crate) mod mqtt;
pub(crate) mod state;
pub(crate) mod surface;
pub(crate) mod wasm;

/// Canonical build-id fixture for tests. Every test `AppState` is built with
/// this value (via `AppState::for_test` and the ad-hoc test constructors), and
/// the handshake tests build their `?build=` URLs from it — so the stale-client
/// comparison is exercised through the same state field production uses.
pub(crate) const TEST_BUILD_ID: &str = "test-build";

/// No-op `IngressRouter` for test construction. Does nothing on `submit_ingress`.
/// Shared single definition so automation-fixture helpers do not each define
/// an identical private struct.
pub(crate) struct NoopEventRouter;

#[async_trait::async_trait]
impl brenn_lib::automation::IngressRouter for NoopEventRouter {
    async fn submit_ingress(
        &self,
        _conversation_id: i64,
        _app_slug: &str,
        _source: &str,
        _summary: &str,
        _payload: &str,
        _urgency: brenn_lib::messaging::Urgency,
    ) {
    }
}
