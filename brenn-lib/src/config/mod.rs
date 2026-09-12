mod alerting;
mod app;
mod app_table;
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
mod mounts;
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
pub use app_table::{AppRef, AppTable};
pub use attachment::*;
pub use automation::*;
#[cfg(test)]
pub(crate) use brenn::load_config_from;
pub use brenn::*;
/// The approval-rule shape `AppConfigRaw::approval_rules` holds, re-exported
/// so a caller that reads or builds an agent block need not name the crate the
/// matcher lives in.
pub use brenn_approval_rules::ApprovalRuleConfig;
/// What a document load or check reads: the root and its module roots.
pub use brenn_dsl::DocumentInputs;
/// Which vocabulary a document is read as.
pub use brenn_dsl::DocumentRole;
/// One config-carrying mount, as a compile input.
pub use brenn_dsl::MountedRoot;
/// One file of a loaded document: its place within the document, and its hash.
pub use brenn_dsl::SourceFile;
/// One config-carrying mount named by a `--mounted` flag rather than by a
/// mounts document. Beside [`deployment_inputs`], which is the other way a
/// [`MountedRoot`] reaches the compiler.
pub use brenn_dsl::mounted_flag;
/// A list of install roots and how it was named — a flag, or the mounts.
pub use brenn_dsl::roots::RootList;
pub use claude_defaults::*;
pub use claude_profile::*;
pub use container::*;
pub use events::*;
pub use frontmatter::*;
pub use hooks::*;
pub use llm_chat::*;
pub use logging::*;
pub use mcp::*;
pub use mounts::*;
pub use observability::*;
pub use path_mapper::*;
pub use repo::*;
#[cfg(test)]
pub(crate) use resolve::shallow_merge_toml;
pub use resolve::{
    ResolvedConfig, pwa_push_grant_without_section, resolve_apps, validate_and_resolve,
};
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
