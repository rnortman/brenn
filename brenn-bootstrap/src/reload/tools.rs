//! What a reload does to the tool registry.
//!
//! The registry grows and never shrinks at reload. The candidate is the running
//! registry extended by the tools the candidate document newly declares: the
//! provided tools of every consumer that provides in the candidate and not in
//! the running process, and `api-request` when the process has no native one
//! and the candidate declares a key provider. Every tool already registered is
//! carried as the same entry, so its semaphore and its callers' rate buckets
//! survive the swap. Level 1 refuses everything that would take a tool away or change
//! one.

use std::sync::Arc;

use brenn_lib::config::{API_REQUEST_TOOL, ApiKeyTable, AppConfig, BrennConfig};
use brenn_tool_registry::tool::AsyncTool as _;
use brenn_tool_registry::{ProviderRuntime, RegisteredTool, ToolRegistry};
use indexmap::IndexMap;

/// Which tools a reload registers, and the registry commit installs.
pub(crate) struct ToolsDelta {
    /// Canonical names the candidate registers and the running registry lacks,
    /// sorted.
    pub added: Vec<String>,
    /// The registry commit installs: the running one extended, or the running
    /// `Arc` itself when `added` is empty.
    pub candidate: Arc<ToolRegistry>,
}

impl Default for ToolsDelta {
    /// Nothing added, over an empty registry: what a delta computed by
    /// anything but prepare carries.
    fn default() -> Self {
        Self {
            added: Vec::new(),
            candidate: Arc::new(ToolRegistry::new(Vec::new())),
        }
    }
}

/// The registry a fresh boot of `candidate` would hold, built by extending
/// `running`.
///
/// A provided tool is built for each declaration of every candidate consumer
/// that provides and whose slug provides nothing in `running`; a slug that
/// does provide there declares the same tools in the candidate, which level 1
/// established, so its tools are carried. `api-request` is built over `keys`
/// when `running` lacks the native tool and the candidate declares a key
/// provider: the tool is absent only from a process with no OAuth service, which a reload cannot
/// add, so `None` is what a fresh boot of the candidate passes. The decision
/// reads the document, not the resolved key table, because the plan needs the
/// registry before the key files are read; a declared provider resolves to
/// exactly one table entry or refuses the reload.
///
/// # Panics
///
/// In boot's wording, on a document verdict: an added provided tool whose
/// `mcp_name` is reserved by a core virtual tool or an integration of any agent
/// in `apps`, and an added tool whose name or MCP name is already registered,
/// including the native `api-request` arriving where a provided tool holds the
/// name.
pub(crate) fn candidate_tools(
    running: &Arc<ToolRegistry>,
    candidate: &BrennConfig,
    apps: &IndexMap<String, AppConfig>,
    runtime: &Arc<ProviderRuntime>,
    keys: &ApiKeyTable,
) -> ToolsDelta {
    let providing = running.providing_instances();
    let arriving: Vec<_> = candidate
        .wasm_consumers
        .iter()
        .filter(|consumer| {
            !consumer.provides.is_empty() && !providing.contains(consumer.slug.as_str())
        })
        .collect();
    crate::assert_provided_names_unreserved(arriving.iter().copied(), apps);
    let provided = crate::provided_tools(arriving.iter().copied(), runtime);

    // The native `api-request` is missing only from a process with no OAuth
    // service. A provided tool holding the name does not stand in for it: the
    // native one is still added, and the collision is refused.
    let native_api_request =
        running.get(API_REQUEST_TOOL).is_some() && !running.is_provided(API_REQUEST_TOOL);
    let native: Vec<RegisteredTool> = if !native_api_request
        && brenn_oauth::tools::api_request_registered(
            false,
            !candidate.api_key_providers.is_empty(),
        ) {
        vec![brenn_oauth::tools::api_request(None, keys.clone())]
    } else {
        Vec::new()
    };

    let mut added: Vec<String> = native
        .iter()
        .map(|tool| tool.descriptor().name.to_string())
        .chain(
            provided
                .iter()
                .map(|tool| tool.descriptor().name.to_string()),
        )
        .collect();
    added.sort();
    if added.is_empty() {
        return ToolsDelta {
            added,
            candidate: running.clone(),
        };
    }
    ToolsDelta {
        added,
        candidate: Arc::new(running.extended(native, provided)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use brenn_lib::config::test_api_key_provider_raw;
    use brenn_lib::messaging::config::{ProvidedToolDecl, WasmConsumerConfigRaw};

    use super::*;

    fn decl(name: &str, mcp_name: &str) -> ProvidedToolDecl {
        ProvidedToolDecl {
            name: name.to_string(),
            mcp_name: mcp_name.to_string(),
            description: "Echo the arguments.".to_string(),
            input_schema: serde_json::json!({ "type": "object" }),
            max_concurrency: 2,
            auto_approve: true,
        }
    }

    fn provider(slug: &str, tools: Vec<ProvidedToolDecl>) -> WasmConsumerConfigRaw {
        WasmConsumerConfigRaw {
            provides: tools,
            ..WasmConsumerConfigRaw::minimal(slug, "processor-provider-test", &[])
        }
    }

    fn document(consumers: Vec<WasmConsumerConfigRaw>, key_provider: bool) -> BrennConfig {
        let mut config = BrennConfig {
            wasm_consumers: consumers,
            ..BrennConfig::default()
        };
        if key_provider {
            config.api_key_providers.insert(
                "maps".to_string(),
                test_api_key_provider_raw("https://api.example.com"),
            );
        }
        config
    }

    fn no_keys() -> ApiKeyTable {
        ApiKeyTable::new(Arc::new(BTreeMap::new()))
    }

    /// The running registry a fresh boot of `config` would hold, over `runtime`.
    fn booted(config: &BrennConfig, runtime: &Arc<ProviderRuntime>) -> Arc<ToolRegistry> {
        let native = if brenn_oauth::tools::api_request_registered(
            false,
            !config.api_key_providers.is_empty(),
        ) {
            vec![brenn_oauth::tools::api_request(None, no_keys())]
        } else {
            Vec::new()
        };
        Arc::new(crate::build_tool_registry(
            native,
            &config.wasm_consumers,
            &IndexMap::new(),
            runtime,
        ))
    }

    fn runtime() -> Arc<ProviderRuntime> {
        let (alert, _drain) = brenn_obs::alerting::AlertDispatcher::noop();
        ProviderRuntime::new(alert)
    }

    #[tokio::test]
    async fn a_newly_providing_slug_is_added_and_a_providing_one_carried() {
        let runtime = runtime();
        let before = document(vec![provider("old", vec![decl("echo", "Echo")])], false);
        let running = booted(&before, &runtime);
        let after = document(
            vec![
                provider("old", vec![decl("echo", "Echo")]),
                provider(
                    "new",
                    vec![decl("shout", "Shout"), decl("whisper", "Whisper")],
                ),
            ],
            false,
        );
        let delta = candidate_tools(&running, &after, &IndexMap::new(), &runtime, &no_keys());
        assert_eq!(delta.added, ["shout", "whisper"]);
        assert_eq!(delta.candidate.provided_by("shout"), Some("new"));
        assert_eq!(delta.candidate.provided_by("echo"), Some("old"));
        assert!(
            std::ptr::eq(
                running.get("echo").unwrap().descriptor(),
                delta.candidate.get("echo").unwrap().descriptor(),
            ),
            "a carried tool is the running registry's own entry",
        );
    }

    #[tokio::test]
    async fn nothing_added_is_the_running_registry_itself() {
        let runtime = runtime();
        let config = document(vec![provider("old", vec![decl("echo", "Echo")])], true);
        let running = booted(&config, &runtime);
        let delta = candidate_tools(&running, &config, &IndexMap::new(), &runtime, &no_keys());
        assert!(delta.added.is_empty());
        assert!(Arc::ptr_eq(&running, &delta.candidate));
    }

    #[tokio::test]
    async fn api_request_is_added_iff_absent_and_a_key_provider_is_declared() {
        let runtime = runtime();
        let without = document(vec![], false);
        let with = document(vec![], true);

        let bare = booted(&without, &runtime);
        let delta = candidate_tools(&bare, &with, &IndexMap::new(), &runtime, &no_keys());
        assert_eq!(delta.added, ["api-request"]);
        assert!(delta.candidate.get("api-request").is_some());

        let delta = candidate_tools(&bare, &without, &IndexMap::new(), &runtime, &no_keys());
        assert!(
            delta.added.is_empty(),
            "no provider declared, nothing added"
        );

        let holding = booted(&with, &runtime);
        let delta = candidate_tools(&holding, &with, &IndexMap::new(), &runtime, &no_keys());
        assert!(delta.added.is_empty(), "already registered, nothing added");
    }

    #[tokio::test]
    #[should_panic(expected = "tool `echo` of `[[wasm_consumer]] new` has mcp_name")]
    async fn a_reserved_mcp_name_refuses() {
        let runtime = runtime();
        let running = booted(&document(vec![], false), &runtime);
        let reserved = brenn_envelope::addressing::CORE_VIRTUAL_TOOL_NAMES[0];
        let after = document(vec![provider("new", vec![decl("echo", reserved)])], false);
        candidate_tools(&running, &after, &IndexMap::new(), &runtime, &no_keys());
    }

    #[tokio::test]
    #[should_panic(
        expected = "tool \"api-request\" is provided by [[wasm_consumer]] \"s\" and registered \
                    natively"
    )]
    async fn the_first_key_provider_beside_a_provided_api_request_refuses() {
        let runtime = runtime();
        let lookalike = || provider("s", vec![decl(API_REQUEST_TOOL, "ApiRequestLookalike")]);
        let running = booted(&document(vec![lookalike()], false), &runtime);
        let after = document(vec![lookalike()], true);
        candidate_tools(&running, &after, &IndexMap::new(), &runtime, &no_keys());
    }

    #[tokio::test]
    #[should_panic(expected = "echo")]
    async fn a_name_already_registered_refuses() {
        let runtime = runtime();
        let before = document(vec![provider("old", vec![decl("echo", "Echo")])], false);
        let running = booted(&before, &runtime);
        let after = document(
            vec![
                provider("old", vec![decl("echo", "Echo")]),
                provider("new", vec![decl("echo", "Other")]),
            ],
            false,
        );
        candidate_tools(&running, &after, &IndexMap::new(), &runtime, &no_keys());
    }
}
