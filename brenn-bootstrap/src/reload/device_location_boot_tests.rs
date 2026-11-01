//! The `device-location` tool's boot path: an operator's `tool
//! device-location {}` grant validated against the registry `run_server`
//! builds, and the app gate the WebSocket location messages read.

use brenn_lib::config::PACKAGED;

use super::driver::tests::{
    BootFixture, Tree, boot_with, install_package, resolve_fixture_apps, staged_module,
};

/// The agent holding the grant.
const ASSISTANT: &str = "assistant";

/// A document with one containerised agent; `agent_lines` are added to its
/// body.
fn document(home: &std::path::Path, agent_lines: &str) -> String {
    super::driver::tests::document(&format!(
        r#"
container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

agent Assistant() {{
    working_dir = ".";
    container = "sandbox";
    container_working_dir = "/work";
{agent_lines}}}

new {ASSISTANT}: Assistant();
"#,
        home = home.display(),
    ))
}

/// Resolve `tree`'s apps and validate their grants against a registry built
/// over the native tools `run_server` always registers this one with.
fn validate(tree: &Tree) -> indexmap::IndexMap<String, brenn_lib::config::AppConfig> {
    let document = tree.load();
    let apps = resolve_fixture_apps(&document.config, &tree.runtime_dir());
    let (alert, _drain) = brenn_obs::alerting::AlertDispatcher::noop();
    let runtime = brenn_tool_registry::ProviderRuntime::new(alert);
    let registry = crate::build_tool_registry(
        brenn_device_location::registered(brenn_db::into_db(brenn_db::open_connection_memory())),
        &document.config.wasm_consumers,
        &apps,
        &runtime,
    );
    registry.validate_config(&apps);
    apps
}

/// A single-user containerised agent may hold the grant, and holding it is
/// what turns the app's location gate on.
#[tokio::test]
async fn a_containerised_single_user_agent_holds_the_grant() {
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&document(
        home.path(),
        &format!("    tool {} {{}}\n", brenn_device_location::NAME),
    ));
    let apps = validate(&tree);
    assert!(apps[ASSISTANT].device_location_enabled());
}

/// Without the grant the gate is off.
#[tokio::test]
async fn an_agent_without_the_grant_has_location_off() {
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&document(home.path(), ""));
    let apps = validate(&tree);
    assert!(!apps[ASSISTANT].device_location_enabled());
}

/// The tool acts as the user; a multiuser agent's conversation has no single
/// one, so boot refuses the grant.
#[tokio::test]
#[should_panic(expected = "the tool acts as a user and the app is multiuser")]
async fn a_multiuser_agent_is_refused_the_grant() {
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&document(
        home.path(),
        &format!(
            "    multiuser = true;\n    allowed_users = [\"alice\", \"bob\"];\n    tool {} {{}}\n",
            brenn_device_location::NAME
        ),
    ));
    validate(&tree);
}

/// A component that provides no tool never serves a call that carries a user,
/// so boot refuses it the grant. (A providing component holding it boots:
/// `google_maps_tools_boot_tests`.)
#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = "tool_grant for \"device-location\" is refused: the tool acts as the acting user, \
                and a component that provides no tool never serves a call that carries one"
)]
async fn a_component_that_provides_nothing_is_refused_the_grant() {
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&super::driver::tests::document(&format!(
        r#"
container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}

{PACKAGED}component Demo {{
    abi = processor;
    requires = [ports, tools];
    in inbound;
    in tool-results;
    out digest;
}}
{PACKAGED}
new locator: Demo {{
    grants = [ports, tools];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
    tool {name} {{}}
}}
"#,
        home = home.path().display(),
        name = brenn_device_location::NAME,
    )));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            native_tools: brenn_device_location::registered(brenn_db::into_db(
                brenn_db::open_connection_memory(),
            )),
            ..BootFixture::default()
        },
    )
    .await;
}
