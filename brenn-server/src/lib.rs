//! Brenn server library: router, routes, WS, state, and startup composition.
//!
//! The thin `brenn` binary crate holds `main()` and the compile-time
//! `BUILD_ID`; this library holds everything that is heavily tested and must
//! not vary with the build id. The build id enters as a runtime-threaded
//! `&'static str` (see `state::AppState::build_id`), never as a compile-time
//! const in this crate.
//!
//! Most modules are `pub`: the composition root (`brenn-bootstrap`) assembles
//! the server out of them, and it lives above this crate. `test_support` is
//! `pub` behind `testutils` for the same reason.

pub mod active_bridge;
mod automation_intercept;
mod cc_schema_drift;
mod client_ip;
pub mod db;
mod idle_hooks;
mod intercept_helpers;
mod messaging_intercept;
pub mod messaging_router;
mod middleware;
mod model_cache;
mod mqtt_intercept;
pub mod mqtt_router;
mod mqtt_subscribe;
mod path_validate;
mod pwa_push_intercept;
pub mod repo_sync;
pub mod router;
pub mod routes;
pub mod state;
#[cfg(any(test, feature = "testutils"))]
pub mod test_support;
#[cfg(test)]
mod wasm_dispatch_tests;
pub mod webhook_router;
