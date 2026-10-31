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
use std::os::unix::fs::PermissionsExt;

use super::driver::tests::Tree;
use super::provider_class_boot::{ProviderClassBoot, assert_provider_class_boots};

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
    ("places-search", "PlacesSearch"),
    ("place-details", "PlaceDetails"),
    ("search-along-route", "SearchAlongRoute"),
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

/// The packaged class boots over the document's key providers: each tool is a
/// provided tool in the registry and on the agent's written list, the request
/// inbox is as deep as the class's summed concurrency, and the result inbox
/// takes the tuning block's depth.
#[tokio::test(flavor = "multi_thread")]
async fn google_maps_tools_boots_into_an_agents_tool_list() {
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let key_file = host.path().join("maps-api-key");
    std::fs::write(&key_file, "K3Y-deployment-secret\n").expect("write the key file");
    std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600))
        .expect("the key file is private");
    let store = host.path().join("google-maps-tools.db");
    let tree = Tree::holding(&document(home.path(), &key_file, &store));

    assert_provider_class_boots(
        ProviderClassBoot {
            tree: &tree,
            artifact: ARTIFACT,
            slug: MAPS,
            agent: ASSISTANT,
            tools: TOOLS,
            result_depth: 64,
        },
        |config| {
            brenn_oauth::tools::registered(
                None,
                brenn_lib::config::resolve_api_key_providers(config),
            )
        },
    )
    .await;
}
