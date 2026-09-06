//! Dispatch suites that need this crate's wiring.
//!
//! The dispatcher and the bulk of its suites live in `brenn-wasm-dispatch`,
//! below this crate. The three families here drive the same dispatcher through
//! the tool executor, the repo-sync fixtures and the webhook router — all of
//! which live in this crate — so they run in this crate's test binary and build
//! on the scaffolding the lower crate exposes behind `testutils`.

use std::sync::Arc;

pub use brenn_wasm_dispatch::tests::*;

// These suites reach tables only the server's migration slice creates.
pub use crate::test_support::init_db_memory as init_db_memory_server_slice;

/// Give the tool executor its position on its request channels. Fixtures that
/// insert a request before building the executor need this to have run first.
pub async fn attach_tool_executor(messenger: &Arc<brenn_messaging::Messenger>) {
    brenn_messaging::system::SystemInbox::new(
        brenn_tool_registry::executor::TOOL_EXECUTOR_COMPONENT,
        Arc::clone(messenger),
        Arc::new(tokio::sync::Notify::new()),
    )
    .attach()
    .await;
}

/// A consumer's result inbox entry carrying the consumer's own Wasm subscriber,
/// plus the entry's resolved channel for the `inbox_input_port` call beside it.
///
/// The subscriber's depths and noise come from `inbox_subscription` so a tuned
/// inbox cannot make a fixture's channel entry and its input port describe two
/// different subscriptions.
pub fn inbox_entry_with_own_subscriber(
    slug: &str,
    tuning: &brenn_lib::messaging::config::SystemChannelTuning,
    defaults: &brenn_lib::messaging::config::MessagingGlobalConfig,
) -> (
    brenn_lib::messaging::ChannelEntry,
    brenn_lib::messaging::config::ResolvedChannel,
) {
    let mut inbox = brenn_tool_registry::bus_wiring::result_inbox_entry(slug, tuning, defaults);
    let channel = inbox.resolved_channel.clone();
    let sub = brenn_tool_registry::bus_wiring::inbox_subscription(slug, &channel);
    inbox
        .subscribers
        .push(brenn_lib::messaging::SubscriberEntry {
            kind: brenn_lib::messaging::SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: sub.push_depth,
            retain_depth: sub.retain_depth,
            noise: sub.noise,
            wake_min: None,
        });
    (inbox, channel)
}

mod git_pipeline_e2e;
mod git_sync_consumer;
mod tool_e2e;
