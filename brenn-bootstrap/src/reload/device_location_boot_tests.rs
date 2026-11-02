//! The `device-location` tool's boot path: an operator's `tool
//! device-location {}` grant validated against the registry `run_server`
//! builds, and the app gate the WebSocket location messages read.

use super::driver::tests::{Tree, resolve_fixture_apps};

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
