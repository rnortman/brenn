//! The boot-path contract every packaged provider class meets: from an
//! operator's document to an agent's tool list.
//!
//! A class's boot test writes the document — the class's packaged
//! specification, the provider block its instance's clauses name, one instance
//! with its result-inbox tuning block, and an agent granted every tool the
//! class provides — and hands it here with the native tools `run_server` would
//! build over that document.

use brenn_lib::config::BrennConfig;
use brenn_lib::messaging::SubscriberEntryKind;
use brenn_lib::messaging::config::Depth;
use brenn_tool_registry::RegisteredTool;

use super::driver::tests::{BootFixture, Tree, boot_with, install_package_from, staged_module};

/// One packaged provider class's document and what its boot must show.
pub(super) struct ProviderClassBoot<'a> {
    /// The operator's document.
    pub tree: &'a Tree,
    /// The component's artifact, installed under a fresh components root.
    pub artifact: &'a str,
    /// The instance's slug.
    pub slug: &'a str,
    /// The agent granted every tool.
    pub agent: &'a str,
    /// The tools the class provides, by registry name and MCP name, in
    /// declaration order.
    pub tools: &'a [(&'a str, &'a str)],
    /// The result inbox's depth, as the document's tuning block sets it.
    pub result_depth: u64,
}

/// Boot `class`'s document over the native tools `native` builds from its
/// lowered configuration, and assert: the class provides exactly `tools`;
/// each is a provided tool in the registry and on the agent's written list;
/// the request inbox is as deep as the class's summed `max_concurrency`; the
/// result inbox takes the tuning block's depth, with the instance as its one
/// subscriber.
pub(super) async fn assert_provider_class_boots(
    class: ProviderClassBoot<'_>,
    native: impl FnOnce(&BrennConfig) -> Vec<RegisteredTool>,
) {
    let components = tempfile::tempdir().expect("a components root");
    install_package_from(
        components.path(),
        &staged_module(class.tree),
        class.artifact,
    );

    let config = class.tree.load().config;
    let provides = &config
        .wasm_consumers
        .iter()
        .find(|consumer| consumer.slug == class.slug)
        .expect("the instance lowers")
        .provides;
    let declared: Vec<(&str, &str)> = provides
        .iter()
        .map(|decl| (decl.name.as_str(), decl.mcp_name.as_str()))
        .collect();
    assert_eq!(
        declared, class.tools,
        "the class provides exactly these tools"
    );
    // Σ `max_concurrency` over the class's `provide` statements.
    let request_inbox_depth: u64 = provides
        .iter()
        .map(|decl| u64::from(decl.max_concurrency))
        .sum();

    let booted = boot_with(
        class.tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            native_tools: native(&config),
            ..BootFixture::default()
        },
    )
    .await;

    let registry = booted.driver.env().tool_registry.clone();
    for (name, mcp_name) in class.tools {
        assert!(registry.is_provided(name), "{name}");
        let descriptor = registry
            .get(name)
            .expect("the provided tool is registered")
            .descriptor();
        assert_eq!(descriptor.mcp_name, format!("mcp__brenn__{mcp_name}"));
    }

    let apps = booted.messenger.app_table().load();
    let agent = &apps[class.agent];
    let written: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(agent.virtual_tools_path()).expect("boot wrote the list"),
    )
    .expect("the list is JSON");
    let names: Vec<&str> = written
        .as_array()
        .expect("the list is an array")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    for (_, mcp_name) in class.tools {
        assert!(names.contains(mcp_name), "{mcp_name} not in {names:?}");
    }

    let directory = booted.messenger.directory();
    let requests = directory
        .resolve(&format!("brenn:tool-requests/{}", class.slug))
        .expect("the request inbox is minted");
    assert_eq!(
        requests.resolved_channel.retain_depth,
        Depth::Bounded(request_inbox_depth)
    );
    let results = directory
        .resolve(&format!("brenn:tool-results/{}", class.slug))
        .expect("the result inbox is minted");
    assert_eq!(
        results.resolved_channel.retain_depth,
        Depth::Bounded(class.result_depth)
    );
    let result_subscribers: Vec<&SubscriberEntryKind> =
        results.subscribers.iter().map(|s| &s.kind).collect();
    assert_eq!(
        result_subscribers,
        [&SubscriberEntryKind::Wasm(class.slug.to_string())]
    );
}
