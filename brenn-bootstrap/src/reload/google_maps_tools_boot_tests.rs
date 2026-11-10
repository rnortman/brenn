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

use super::driver::tests::{Tree, key_file};
use super::provider_class_boot::{ProviderClassBoot, assert_provider_class_boots};

/// The instance's slug.
pub(super) const MAPS: &str = "maps";

/// The agent granted the tools.
const ASSISTANT: &str = "assistant";

/// The component's artifact.
pub(super) const ARTIFACT: &str = "brenn_google_maps_tools.wasm";

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
/// one instance with its nine clauses, `extra` and `config`, and an agent
/// granted every tool.
pub(super) fn document(
    home: &std::path::Path,
    key_file: &std::path::Path,
    store: &std::path::Path,
    extra: &str,
) -> String {
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
{extra}    config = {{
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

/// Boot the packaged class with `extra` in the instance, over the document's
/// key providers plus `device-location`, asserting what
/// [`assert_provider_class_boots`] asserts.
async fn boot(extra: &str) {
    boot_edited(extra, |document| document).await;
}

/// [`boot`] over the document as `edit` returns it.
async fn boot_edited(extra: &str, edit: impl FnOnce(String) -> String) {
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let key_file = key_file(host.path().join("maps-api-key"), "K3Y-deployment-secret\n");
    let store = host.path().join("google-maps-tools.db");
    let tree = Tree::holding(&edit(document(home.path(), &key_file, &store, extra)));

    assert_provider_class_boots(
        ProviderClassBoot {
            tree: &tree,
            artifact: ARTIFACT,
            slug: MAPS,
            agent: ASSISTANT,
            tools: TOOLS,
            result_depth: 64,
        },
        // `api-request` over the document's key providers is the boot
        // fixture's own registration.
        |_| {
            brenn_device_location::registered(brenn_db::into_db(brenn_db::open_connection_memory()))
                .0
        },
    )
    .await;
}

/// The packaged class boots over the document's key providers: each tool is a
/// provided tool in the registry and on the agent's written list, the request
/// inbox is as deep as the class's summed concurrency, and the result inbox
/// takes the tuning block's depth.
#[tokio::test(flavor = "multi_thread")]
async fn google_maps_tools_boots_into_an_agents_tool_list() {
    boot("").await;
}

/// A component's calls carry no acting user, so boot refuses the instance a
/// `device-location` grant though the component provides tools.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "tool_grant for \"device-location\" is refused: the tool acts as the acting user \
                and a wasm consumer's calls carry none"
)]
async fn the_instance_is_refused_the_device_location_grant() {
    boot("    tool device-location {}\n").await;
}

/// The instance's clauses are held to the document's key providers at boot:
/// a `user` on the deployment-key provider admits no call and is refused in
/// the registry's words.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "wasm consumer \"maps\": tool_grant for \"api-request\" has an invalid ACL clause: \
                `user` and `account` have no meaning for api_key_provider \"google-maps\""
)]
async fn an_instance_clause_naming_a_user_on_the_deployment_key_is_refused() {
    boot_edited("", |document| {
        document.replacen(
            r#"allow { provider = "google-maps"; method = "GET";"#,
            r#"allow { provider = "google-maps"; user = "alice"; method = "GET";"#,
            1,
        )
    })
    .await;
}

/// With no provider of either kind `api-request` is not registered, so an
/// agent's grant naming it is refused as an unknown tool rather than by what
/// its clause names.
#[tokio::test]
#[should_panic(expected = "app \"assistant\": tool_grant names unknown tool \"api-request\"")]
async fn with_no_provider_an_api_request_grant_names_an_unknown_tool() {
    let tree = Tree::holding(&super::driver::tests::document(&format!(
        r#"
agent Assistant() {{
    working_dir = ".";
    tool api-request {{
        allow {{ provider = "*"; user = "alice"; }}
    }}
}}

new {ASSISTANT}: Assistant();
"#
    )));
    let document = tree.load();
    let apps = super::driver::tests::resolve_fixture_apps(&document.config, &tree.runtime_dir());
    let (alert, _drain) = brenn_obs::alerting::AlertDispatcher::noop();
    let registry = crate::build_tool_registry(
        brenn_oauth::tools::registered(
            None,
            brenn_lib::config::ApiKeyTable::new(std::sync::Arc::new(
                brenn_lib::config::resolve_api_key_providers(&document.config),
            )),
        ),
        &document.config.wasm_consumers,
        &apps,
        &brenn_tool_registry::ProviderRuntime::new(alert),
    );
    registry.validate_config(&apps);
}
