//! A provided tool's boot path, from a compiled document to an agent's tool
//! list, and the reload that may not move it.
//!
//! The document declares a providing class whose package is the
//! `processor-provider-test` artifact, one instance of it, and a containerized
//! agent granted the tool. The boot fixture builds the registry with
//! [`crate::build_tool_registry`] over the compiled document and the one
//! provider runtime it hands to the provider's host, and validates its grants
//! against the resolved apps, as `run_server` does; the consumer is loaded
//! through `load_consumer`.

use brenn_lib::config::PACKAGED;
use brenn_lib::integration::CliAccess;
use brenn_lib::messaging::SubscriberEntryKind;
use brenn_lib::messaging::config::Depth;
use brenn_messaging::config_reload::Outcome;
use brenn_tool_registry::TOOL_EXECUTOR_COMPONENT;

use super::NEEDS_RESTART;
use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Tree, async_pull_tool, boot_with, install_package_from, staged_module,
};

/// The providing instance's slug.
const PROVIDER: &str = "provider";

/// The agent granted the provided tool.
const ASSISTANT: &str = "assistant";

/// The fixture artifact the providing class is packaged with.
const PROVIDER_ARTIFACT: &str = "brenn_processor_provider_test.wasm";

/// The providing class's body: `echo-for-user` declared as the fixture
/// artifact's own spec declares it, or nothing provided at all.
fn provider_class(provides: bool) -> String {
    if provides {
        r#"    requires = [tools, provides];
    in tool-results;
    in tool-requests;
    provide echo-for-user {
        mcp_name = "EchoForUser";
        description = "Answer by the mode argument, for the caller's user.";
        input_schema = """{ "type": "object", "properties": { "mode": { "type": "string" } }, "required": ["mode"] }""";
        max_concurrency = 2;
    }
"#
        .to_string()
    } else {
        "    requires = [tools];\n    in tool-results;\n".to_string()
    }
}

/// A document with one instance of the providing class (granted `apull` too,
/// since the artifact imports `tools`) and an agent granted `echo-for-user`.
/// The agent runs in a container whose home is `home`: a provided tool acts as
/// its caller's user, and that grant is refused on a bare agent.
fn providing_document(provides: bool, home: &std::path::Path) -> String {
    let grants = if provides { "tools, provides" } else { "tools" };
    super::driver::tests::document(&format!(
        r#"{PACKAGED}component Provider {{
    abi = processor;
{class}}}
{PACKAGED}
container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

new {PROVIDER}: Provider {{
    grants = [{grants}];
    tool apull {{ allow {{ repo = "notes"; }} }}
}}

agent Assistant() {{
    working_dir = ".";
    container = "sandbox";
    container_working_dir = "/work";
    tool echo-for-user {{}}
}}

new {ASSISTANT}: Assistant();
"#,
        class = provider_class(provides),
        home = home.display(),
    ))
}

/// The document's provided tool reaches the registry, the plan and the
/// agent's tool list; a reload dropping the `provide` statement is refused
/// for a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_provided_tool_boots_into_an_agents_tool_list() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&providing_document(true, home.path()));
    install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);

    let mut booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            native_tools: vec![async_pull_tool()],
            ..BootFixture::default()
        },
    )
    .await;

    let registry = booted.driver.env().tool_registry.clone();
    assert!(registry.is_provided("echo-for-user"));
    let descriptor = registry
        .get("echo-for-user")
        .expect("the provided tool is registered")
        .descriptor();
    assert_eq!(descriptor.mcp_name, "mcp__brenn__EchoForUser");

    // The agent's tool list: the file boot wrote, and the CLI access its
    // catalog carries.
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
    assert!(names.contains(&"EchoForUser"), "{names:?}");
    let offered = brenn_server::active_bridge::virtual_tools(assistant, &registry);
    let echo = offered
        .iter()
        .find(|tool| tool.name == "EchoForUser")
        .expect("the agent is offered the provided tool");
    assert_eq!(echo.cli, CliAccess::Available);

    // The plan: the provider's request inbox, as deep as its declared
    // concurrency and subscribed by the provider alone; and the executor on
    // the tool's request channel.
    let directory = booted.messenger.directory();
    let inbox = directory
        .resolve(&format!("brenn:tool-requests/{PROVIDER}"))
        .expect("the request inbox is minted");
    assert_eq!(inbox.resolved_channel.retain_depth, Depth::Bounded(2));
    let inbox_subscribers: Vec<&SubscriberEntryKind> =
        inbox.subscribers.iter().map(|s| &s.kind).collect();
    assert_eq!(
        inbox_subscribers,
        [&SubscriberEntryKind::Wasm(PROVIDER.to_string())]
    );
    let requests = directory
        .resolve("brenn:tools/echo-for-user")
        .expect("the tool's request channel is minted");
    assert!(
        requests
            .subscribers
            .iter()
            .any(|s| s.kind == SubscriberEntryKind::System(TOOL_EXECUTOR_COMPONENT.to_string())),
        "{:?}",
        requests.subscribers
    );

    tree.write(&providing_document(false, home.path()));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(
        status.refusals,
        [format!(
            "wasm_consumers[{PROVIDER}].provides removed: {NEEDS_RESTART}"
        )]
    );
}
