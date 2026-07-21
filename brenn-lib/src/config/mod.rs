mod alerting;
mod app;
mod attachment;
mod brenn;
mod claude_defaults;
mod container;
mod events;
mod frontmatter;
mod hooks;
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
pub mod wasm;
mod watchdog;

pub use alerting::*;
pub use app::*;
pub use attachment::*;
#[cfg(test)]
pub(crate) use brenn::load_config_from;
pub use brenn::*;
pub use claude_defaults::*;
pub use container::*;
pub use events::*;
pub use frontmatter::*;
pub use hooks::*;
pub use logging::*;
pub use mcp::*;
pub use observability::*;
pub use path_mapper::*;
pub use repo::*;
#[cfg(test)]
pub(crate) use resolve::shallow_merge_toml;
pub use resolve::{ResolvedConfig, validate_and_resolve};
pub(crate) use secret::load_secret_file;
pub use security::*;
pub use server::*;
pub use surface_description::*;
pub use watchdog::*;

#[cfg(test)]
mod tests;
