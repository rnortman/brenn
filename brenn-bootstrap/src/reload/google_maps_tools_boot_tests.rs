//! The `google-maps-tools` component's boot path, from a document to an
//! agent's tool list.
//!
//! The document declares the component's class as its packaged specification
//! states it, word for word, installed with the component's own artifact; one
//! instance of it with the key provider, the `api-request` clauses, the
//! `config` map and the result-inbox tuning block an operator writes; and a
//! containerized agent granted every tool the class provides. The registry is
//! built over the document's key providers as `run_server` builds it.

use brenn_lib::config::PACKAGED;
use brenn_lib::messaging::SubscriberEntryKind;
use brenn_lib::messaging::config::Depth;
use std::os::unix::fs::PermissionsExt;

use super::driver::tests::{BootFixture, Tree, boot_with, install_package_from, staged_module};

/// The instance's slug.
const MAPS: &str = "maps";

/// The agent granted the tools.
const ASSISTANT: &str = "assistant";

/// The component's artifact.
const ARTIFACT: &str = "brenn_google_maps_tools.wasm";

/// The tools the class provides, by registry name and MCP name, in
/// declaration order.
const TOOLS: &[(&str, &str)] = &[
    ("weather", "Weather"),
    ("geocode", "Geocode"),
    ("directions", "Directions"),
];

/// The class's packaged specification.
fn spec() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../config/specs/google-maps-tools.brenn");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The operator's document: the class, the key provider, the tuning block,
/// one instance with its nine clauses and `config`, and an agent granted
/// every tool.
fn document(home: &std::path::Path, key_file: &std::path::Path, store: &std::path::Path) -> String {
    let grants: String = TOOLS
        .iter()
        .map(|(name, _)| format!("    tool {name} {{}}\n"))
        .collect();
    super::driver::tests::document(&format!(
        r#"{PACKAGED}{spec}{PACKAGED}
container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

api_key_provider google-maps {{
    key_file = "{key_file}";
    header = "X-Goog-Api-Key";
    api_hosts = [
        "https://weather.googleapis.com",
        "https://geocode.googleapis.com",
        "https://places.googleapis.com",
        "https://routes.googleapis.com"
    ];
}}

channel at "brenn:tool-results/{MAPS}" {{
    push_depth = 1;
    retain_depth = 64;
    standing_retain_depth = 64;
    noise = alarm;
}}

new {MAPS}: GoogleMapsTools {{
    grants = [log, store, config, tools, provides];
    store_path = "{store}";
    tool api-request {{
        allow {{ provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/currentConditions:lookup"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/forecast/hours:lookup"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/forecast/days:lookup"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "weather.googleapis.com"; path = "/v1/publicAlerts:lookup"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "geocode.googleapis.com"; path = "/v4/geocode/address"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "geocode.googleapis.com"; path = "/v4/geocode/location"; }}
        allow {{ provider = "google-maps"; method = "POST"; host = "places.googleapis.com";  path = "/v1/places:searchText"; }}
        allow {{ provider = "google-maps"; method = "GET";  host = "places.googleapis.com";  path = "/v1/places/*"; }}
        allow {{ provider = "google-maps"; method = "POST"; host = "routes.googleapis.com";  path = "/directions/v2:computeRoutes"; }}
        rate_limit {{ burst = 20; sustained_per_minute = 60; }}
    }}
    config = {{
        provider = "google-maps",
        weather_host = "weather.googleapis.com",
        geocoding_host = "geocode.googleapis.com",
        places_host = "places.googleapis.com",
        routes_host = "routes.googleapis.com",
        units = "IMPERIAL"
    }};
}}

agent Assistant() {{
    working_dir = ".";
    container = "sandbox";
    container_working_dir = "/work";
{grants}}}

new {ASSISTANT}: Assistant();
"#,
        spec = spec(),
        home = home.display(),
        key_file = key_file.display(),
        store = store.display(),
    ))
}

/// The packaged class boots: each tool is a provided tool in the registry and
/// on the agent's written list, the request inbox is as deep as the class's
/// summed concurrency, and the result inbox takes the tuning block's depth.
#[tokio::test(flavor = "multi_thread")]
async fn google_maps_tools_boots_into_an_agents_tool_list() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let key_file = host.path().join("maps-api-key");
    std::fs::write(&key_file, "K3Y-deployment-secret\n").expect("write the key file");
    std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600))
        .expect("the key file is private");
    let store = host.path().join("google-maps-tools.db");
    let tree = Tree::holding(&document(home.path(), &key_file, &store));
    install_package_from(components.path(), &staged_module(&tree), ARTIFACT);

    let config = tree.load().config;
    let provides = &config
        .wasm_consumers
        .iter()
        .find(|consumer| consumer.slug == MAPS)
        .expect("the instance lowers")
        .provides;
    let declared: Vec<(&str, &str)> = provides
        .iter()
        .map(|decl| (decl.name.as_str(), decl.mcp_name.as_str()))
        .collect();
    assert_eq!(declared, TOOLS, "the class provides exactly these tools");
    // Σ `max_concurrency` over the class's `provide` statements.
    let request_inbox_depth: u64 = provides
        .iter()
        .map(|decl| u64::from(decl.max_concurrency))
        .sum();

    let key_providers = brenn_lib::config::resolve_api_key_providers(&config);
    let booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            native_tools: brenn_oauth::tools::registered(None, key_providers),
            ..BootFixture::default()
        },
    )
    .await;

    let registry = booted.driver.env().tool_registry.clone();
    for (name, mcp_name) in TOOLS {
        assert!(registry.is_provided(name), "{name}");
        let descriptor = registry
            .get(name)
            .expect("the provided tool is registered")
            .descriptor();
        assert_eq!(descriptor.mcp_name, format!("mcp__brenn__{mcp_name}"));
    }

    let apps = booted.messenger.app_table().load();
    let assistant = &apps[ASSISTANT];
    let written: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(assistant.virtual_tools_path()).expect("boot wrote the list"),
    )
    .expect("the list is JSON");
    let names: Vec<&str> = written
        .as_array()
        .expect("the list is an array")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    for (_, mcp_name) in TOOLS {
        assert!(names.contains(mcp_name), "{mcp_name} not in {names:?}");
    }

    let directory = booted.messenger.directory();
    let requests = directory
        .resolve(&format!("brenn:tool-requests/{MAPS}"))
        .expect("the request inbox is minted");
    assert_eq!(
        requests.resolved_channel.retain_depth,
        Depth::Bounded(request_inbox_depth)
    );
    let results = directory
        .resolve(&format!("brenn:tool-results/{MAPS}"))
        .expect("the result inbox is minted");
    assert_eq!(results.resolved_channel.retain_depth, Depth::Bounded(64));
    let result_subscribers: Vec<&SubscriberEntryKind> =
        results.subscribers.iter().map(|s| &s.kind).collect();
    assert_eq!(
        result_subscribers,
        [&SubscriberEntryKind::Wasm(MAPS.to_string())]
    );
}
