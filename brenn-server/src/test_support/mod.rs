//! Fixtures this crate's tests are built on, `pub` behind `testutils` so the
//! crates above build their tests on the same ones.

pub mod app_config;
pub mod http;
pub mod mqtt;
pub mod state;
pub mod wasm;

/// Open an in-memory database carrying the server's slice.
///
/// Uses `crate::db::run_server_slice_migrations` — the same function the
/// production opener uses — so a test never asserts against a narrower
/// schema than the running server creates.
pub fn init_db_memory() -> brenn_db::Db {
    let conn = brenn_db::open_connection_memory();
    crate::db::run_server_slice_migrations(&conn);
    brenn_db::into_db(conn)
}

/// Canonical build-id fixture for tests. Every test `AppState` is built with
/// this value (via `AppState::for_test` and the ad-hoc test constructors), and
/// the handshake tests build their `?build=` URLs from it — so the stale-client
/// comparison is exercised through the same state field production uses.
pub const TEST_BUILD_ID: &str = "test-build";

/// No-op `IngressRouter` for test construction. Does nothing on `submit_ingress`.
/// Shared single definition so automation-fixture helpers do not each define
/// an identical private struct.
pub struct NoopEventRouter;

#[async_trait::async_trait]
impl brenn_automation::IngressRouter for NoopEventRouter {
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
