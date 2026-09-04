mod alerting;
mod app;
mod attachment;
mod automation;
mod brenn;
mod claude_defaults;
mod claude_profile;
mod container;
pub(crate) mod dsl_lower;
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
/// What a document load or check reads: the root and its module roots.
pub use brenn_dsl::DocumentInputs;
pub use claude_defaults::*;
pub use claude_profile::*;
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
pub use secret::SecretString;
pub(crate) use secret::{load_secret_file, load_secret_file_private};
pub use security::*;
pub use server::*;
pub use surface_description::*;
#[cfg(any(test, feature = "testutils"))]
pub use test_fixtures::{
    PACKAGED, PACKAGED_MODULE, config_from_dsl, declaring_text, lower_document,
    remote_exact_ceiling, remote_fleet, remote_prefix_ceiling, remote_raw, repo_sync_at,
    sole_refusal, split_packaged, stage_fixture, test_app_config,
};
pub use watchdog::*;

#[cfg(test)]
mod tests;
