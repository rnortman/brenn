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
        brenn_device_location::registered(brenn_db::into_db(brenn_db::open_connection_memory())).0,
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

/// The extra declarations of a document whose consumer reads `user`'s
/// location channel, granted it exactly, and writes the work channel.
fn location_consumer(user: &str) -> String {
    use brenn_lib::config::PACKAGED;
    format!(
        r#"{PACKAGED}component LocationAutomation {{
    abi = processor;
    requires = [ports];
    in fixes;
    out digest;
}}
{PACKAGED}
new {user}-fixes: LocationAutomation {{
    grants = [ports];
    acl subscribe [exact "brenn:device-location.{user}"];
    in fixes <- "brenn:device-location.{user}" {{ push_depth = 1; retain_depth = 16; }}
    out digest -> work;
}}
"#
    )
}

/// A consumer bound to a user's location channel boots when the user is
/// registered: boot mints the channel from the users table, and the
/// consumer's input port resolves to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_reading_a_registered_users_channel_boots() {
    use super::driver::tests::{BootFixture, boot_with, install_package, staged_module};

    let db = brenn_server::test_support::init_db_memory();
    {
        let conn = db.lock().await;
        brenn_db::auth::user::create_user(&conn, "alice", "$argon2id$fake");
    }
    let tree = Tree::holding(&super::driver::tests::document(&location_consumer("alice")));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let booted = boot_with(
        &tree,
        BootFixture {
            db: Some(db),
            components_roots: vec![components.path().to_path_buf()],
            ..BootFixture::default()
        },
    )
    .await;

    let entry = booted
        .messenger
        .directory()
        .resolve("brenn:device-location.alice")
        .expect("boot minted alice's channel");
    assert!(
        entry.subscribers.iter().any(|sub| matches!(
            &sub.kind,
            brenn_lib::messaging::SubscriberEntryKind::Wasm(slug) if slug == "alice-fixes"
        )),
        "the consumer reads alice's channel: {:?}",
        entry.subscribers
    );
}

/// A user registered after boot has a live channel but is not in the plan a
/// reload lowers, so a candidate binding a consumer to it is refused with the
/// directory miss; a restart mints the user from the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_cannot_bind_a_user_registered_since_boot() {
    use super::driver::TriggerSource;
    use super::driver::tests::{boot, install_package, seat_user, staged_module};
    use brenn_messaging::config_reload::Outcome;

    let tree = Tree::holding(&super::driver::tests::document(""));
    let components = tempfile::tempdir().expect("a components root");
    let mut booted = boot(&tree, vec![components.path().to_path_buf()]).await;
    seat_user(&booted, "alice").await;
    assert!(
        booted
            .messenger
            .directory()
            .resolve("brenn:device-location.alice")
            .is_some(),
        "registration minted alice's channel"
    );

    tree.write(&super::driver::tests::document(&location_consumer("alice")));
    install_package(components.path(), &staged_module(&tree));
    assert!(
        booted
            .driver
            .prepare_and_report(TriggerSource::Bus)
            .await
            .is_none()
    );

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status
            .refusals
            .iter()
            .any(|r| r.contains("no user named \"alice\"")),
        "{:?}",
        status.refusals
    );
}
