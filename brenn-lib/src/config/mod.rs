mod alerting;
mod app;
mod attachment;
mod automation;
mod brenn;
mod claude_defaults;
mod container;
mod events;
mod frontmatter;
mod hooks;
mod llm_chat;
mod logging;
mod mcp;
mod observability;
mod path_mapper;
mod repo;
mod resolve;
mod secret;
mod security;
mod server;
mod surface_description;
#[cfg(any(test, feature = "testutils"))]
mod test_fixtures;
pub mod wasm;
mod watchdog;

pub use alerting::*;
pub use app::*;
pub use attachment::*;
pub use automation::*;
#[cfg(test)]
pub(crate) use brenn::load_config_from;
pub use brenn::*;
pub use claude_defaults::*;
pub use container::*;
pub use events::*;
pub use frontmatter::*;
pub use hooks::*;
pub use llm_chat::*;
pub use logging::*;
pub use mcp::*;
pub use observability::*;
pub use path_mapper::*;
pub use repo::*;
#[cfg(test)]
pub(crate) use resolve::shallow_merge_toml;
pub use resolve::{ResolvedConfig, validate_and_resolve};
pub(crate) use secret::{load_secret_file, load_secret_file_private};
pub use security::*;
pub use server::*;
pub use surface_description::*;
#[cfg(any(test, feature = "testutils"))]
pub use test_fixtures::test_app_config;
pub use watchdog::*;

#[cfg(test)]
mod tests;
