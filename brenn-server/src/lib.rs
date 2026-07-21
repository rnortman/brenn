//! Brenn server library: router, routes, WS, state, and startup composition.
//!
//! The thin `brenn` binary crate holds `main()` and the compile-time
//! `BUILD_ID`; this library holds everything that is heavily tested and must
//! not vary with the build id. The build id enters as a runtime-threaded
//! `&'static str` (see `bootstrap::run_server` and `state::AppState::build_id`),
//! never as a compile-time const in this crate.

mod active_bridge;
mod approval_formatter;
mod artifact;
mod artifact_snapshot;
mod automation_intercept;
pub mod bootstrap;
mod cc_message_prefix;
mod cc_schema_drift;
pub mod cli;
mod client_ip;
mod frontmatter;
mod git_ops;
mod git_subprocess;
mod history;
mod hooks;
mod idle_hooks;
mod intercept_helpers;
mod markdown;
mod messaging_intercept;
mod messaging_router;
mod middleware;
mod mqtt_intercept;
mod mqtt_router;
mod mqtt_subscribe;
mod path_validate;
mod pid_file;
mod pwa_push_intercept;
mod repo_clone;
mod repo_sync;
mod router;
mod routes;
mod state;
mod system_message;
#[cfg(test)]
mod test_support;
pub mod tool_registry;
mod tools;
mod wasm_dispatch;
mod webhook_router;
