//! A provided tool's boot path, from a compiled document to an agent's tool
//! list, and the reloads that add tools to a running process.
//!
//! The document declares a providing class whose package is the
//! `processor-provider-test` artifact, one instance of it, and a containerized
//! agent granted the tool. The boot fixture builds the registry with
//! [`crate::build_tool_registry`] over the compiled document and the one
//! provider runtime it hands to the provider's host, and validates its grants
//! against the resolved apps, as `run_server` does; the consumer is loaded
//! through `load_consumer`.
//!
//! The reload cases boot a process holding an asynchronous tool, so that it
//! runs the tool executor a tool added at reload is served by, and call the
//! added tool after the reload: from the agent through the registry cell, and
//! from a component through the executor's new subscription.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use brenn_lib::config::PACKAGED;
use brenn_lib::integration::CliAccess;
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{ParticipantId, SubscriberEntryKind};
use brenn_lib::tools::ResolvedToolGrant;
use brenn_messaging::config_reload::{Outcome, ReloadStatus};
use brenn_tool_registry::TOOL_EXECUTOR_COMPONENT;
use serde_json::{Value, json};

use super::NEEDS_RESTART;
use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, READER, Tree, assert_applied, async_pull_tool, boot_with, cursor_of,
    install_named_package, install_package_from, key_file, reload_onto, seat_a_conversation,
    staged_module,
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
    providing_document_with(provides, home, "", "")
}

/// [`providing_document`] with `agent_extra` in the agent's body and `extra`
/// after it.
fn providing_document_with(
    provides: bool,
    home: &std::path::Path,
    agent_extra: &str,
    extra: &str,
) -> String {
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
{agent_extra}}}

new {ASSISTANT}: Assistant();
{extra}"#,
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

    let registry = booted.driver.env().tool_registry.load();
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

    // A tool added to the instance's `provide` set: its request inbox would be
    // re-created under calls in flight.
    let gained = providing_document(true, home.path()).replacen(
        "    max_concurrency = 2;\n    }\n",
        r#"    max_concurrency = 2;
    }
    provide echo-again {
        mcp_name = "EchoAgain";
        description = "Answer again.";
        input_schema = """{ "type": "object" }""";
        max_concurrency = 1;
    }
"#,
        1,
    );
    assert_ne!(gained, providing_document(true, home.path()));
    tree.write(&gained);
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert_eq!(
        status.refusals,
        [format!(
            "wasm_consumers[{PROVIDER}].provides differs: {NEEDS_RESTART}"
        )]
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

// ── Reloads that add tools ────────────────────────────────────────────────

/// The package the calling component's class is installed under, beside the
/// fenced module: the caller runs another artifact than the provider.
const CALLER_MODULE: &str = "caller";

/// The calling component's artifact: on a trigger it calls the tool its
/// `config` names with the arguments it names, and republishes every result
/// it receives to `out`.
const CALLER_ARTIFACT: &str = "brenn_processor_tool_test.wasm";

/// The calling component's class.
const CALLER_CLASS: &str = "component Caller {
    abi = processor;
    requires = [ports, config, tools];
    in trigger;
    in tool-results;
    out out;
}
";

/// The channel the calling component republishes its results on.
const SINK: &str = "brenn:sink";

/// What a reload candidate's agent and component call the added tool with.
fn echo_args() -> Value {
    json!({ "mode": "echo" })
}

/// The agent alone, in its container, granted nothing: the document a
/// providing instance arrives in.
fn agent_document(home: &std::path::Path) -> String {
    agent_document_with(home, "")
}

/// [`agent_document`] with `extra` after the agent.
fn agent_document_with(home: &std::path::Path, extra: &str) -> String {
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
}}

new {ASSISTANT}: Assistant();
{extra}"#,
        home = home.display(),
    ))
}

/// `document` importing the caller's module, which [`stage_caller`] writes.
fn with_caller(document: &str) -> String {
    format!("use @{CALLER_MODULE}::*;\n{document}")
}

/// The sink channel and one instance of the calling component: triggered by
/// the work channel, calling `tool` with `args` under the grant body
/// `clauses`.
fn caller_instance(tool: &str, args: &Value, clauses: &str) -> String {
    format!(
        r#"
channel sink at "{SINK}" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}

new caller: Caller {{
    grants = [ports, config, tools];
    config = {{ tool = "{tool}", args = """{args}""" }};
    in trigger <- work {{ push_depth = 4; }}
    out out -> sink;
    tool {tool} {{ {clauses} }}
}}
"#
    )
}

/// Write the caller's module beside the tree's fenced one and install its
/// package under `components`.
fn stage_caller(tree: &Tree, components: &std::path::Path) {
    std::fs::write(
        tree.modules().join(format!("{CALLER_MODULE}.brenn")),
        CALLER_CLASS,
    )
    .expect("the caller's module is writable");
    install_named_package(components, CALLER_MODULE, CALLER_CLASS, CALLER_ARTIFACT);
}

/// Publish one trigger on the work channel, as the reader agent's
/// conversation.
async fn trigger(booted: &Booted) {
    seat_a_conversation(&booted.db, 1).await;
    let published = booted
        .messenger
        .publish(
            brenn_messaging::PublishOrigin::Conversation { id: 1 },
            READER,
            "brenn:work",
            r#"{"go":true}"#,
            brenn_messaging::Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(published, brenn_messaging::PublishResult::Ok { .. }),
        "{published:?}"
    );
}

/// The body of the first result envelope the calling component republished.
async fn first_result(booted: &mut Booted) -> Value {
    let bodies = booted.bodies_until(SINK, 1).await;
    let envelope: Value =
        serde_json::from_str(&bodies[0]).expect("the republished envelope is JSON");
    serde_json::from_str(
        envelope["body"]
            .as_str()
            .expect("the envelope carries a body"),
    )
    .expect("the result body is JSON")
}

/// Call `echo-for-user` as the agent's conversation would: through the
/// process's registry cell, under the grant the swapped agent table holds.
async fn agent_calls_echo(booted: &Booted) -> Value {
    let env = booted.driver.env();
    let grant = env.apps.load()[ASSISTANT]
        .policy
        .tool_grants
        .get("echo-for-user")
        .cloned()
        .expect("the agent table grants the tool");
    let registry = env.tool_registry.load();
    tokio::time::timeout(
        Duration::from_secs(30),
        registry.invoke_for_llm(
            "mcp__brenn__EchoForUser",
            ParticipantId::for_app(ASSISTANT, "test-origin"),
            grant,
            None,
            Some(brenn_tool_registry::ActingUser {
                id: 1,
                username: "alice".to_string(),
            }),
            echo_args(),
            &env.alert_dispatcher,
        ),
    )
    .await
    .expect("the provider answers")
    .expect("the call succeeds")
}

fn assert_channel_added(status: &ReloadStatus, address: &str) {
    assert!(
        status.delta.channels_added.iter().any(|a| a == address),
        "{address} not in {:?}",
        status.delta.channels_added
    );
}

/// The executor's subscriber entry and position on `address`.
async fn assert_executor_serves(booted: &Booted, address: &str) {
    let entry = booted
        .messenger
        .directory()
        .resolve(address)
        .expect("the request channel is in the directory");
    assert!(
        entry
            .subscribers
            .iter()
            .any(|s| s.kind == SubscriberEntryKind::System(TOOL_EXECUTOR_COMPONENT.to_string())),
        "{:?}",
        entry.subscribers
    );
    assert!(
        cursor_of(
            &booted.messenger,
            address,
            &ParticipantId::for_system(TOOL_EXECUTOR_COMPONENT)
        )
        .await
        .is_some(),
        "the executor holds a position on {address}"
    );
}

/// A boot fixture over `components` holding `apull`, so the process runs a
/// tool executor.
fn fixture(components: &tempfile::TempDir, dispatcher: bool, executor: bool) -> BootFixture {
    BootFixture {
        components_roots: vec![components.path().to_path_buf()],
        native_tools: vec![async_pull_tool()],
        dispatcher,
        executor,
        ..BootFixture::default()
    }
}

/// A providing instance and the agent's grant on its tool arrive in one
/// reload: the tool is registered as the instance's, its request channel is
/// reported, the agent's tool list names it, and the agent's call is
/// answered by the provider.
#[tokio::test(flavor = "multi_thread")]
async fn a_providing_consumer_added_serves_the_agent() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&agent_document(home.path()));
    let mut booted = boot_with(&tree, fixture(&components, true, false)).await;

    tree.write(&providing_document(true, home.path()));
    install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_applied(&status);
    assert_eq!(status.delta.consumers_added, [PROVIDER]);
    assert_eq!(status.delta.agents_changed, [ASSISTANT]);
    assert_channel_added(&status, "brenn:tools/echo-for-user");

    let registry = booted.driver.env().tool_registry.load();
    assert_eq!(registry.provided_by("echo-for-user"), Some(PROVIDER));
    let apps = booted.driver.env().apps.load();
    let written =
        std::fs::read_to_string(apps[ASSISTANT].virtual_tools_path()).expect("the list is staged");
    assert!(written.contains("\"EchoForUser\""), "{written}");

    assert_eq!(agent_calls_echo(&booted).await["mode"], "echo");
}

/// The same reload with a component granted the added tool: its call travels
/// the executor's new subscription, the swapped registry, the caller grant
/// table and the provider, and the result reaches the component.
#[tokio::test(flavor = "multi_thread")]
async fn a_providing_consumer_added_serves_a_component() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&agent_document(home.path()));
    let mut booted = boot_with(&tree, fixture(&components, true, true)).await;

    let candidate = with_caller(&providing_document_with(
        true,
        home.path(),
        "",
        &caller_instance("echo-for-user", &echo_args(), ""),
    ));
    tree.write(&candidate);
    install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);
    stage_caller(&tree, components.path());
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_applied(&status);
    assert_eq!(status.delta.consumers_added, [PROVIDER, "caller"]);
    assert_executor_serves(&booted, "brenn:tools/echo-for-user").await;

    trigger(&booted).await;
    let result = first_result(&mut booted).await;
    assert_eq!(result["tool"], "echo-for-user", "{result}");
    assert_eq!(result["outcome"]["ok"], echo_args(), "{result}");
}

/// An instance that provided nothing gains a `provide` block: it is replaced
/// by the changed-consumer path and its tool is callable.
#[tokio::test(flavor = "multi_thread")]
async fn an_existing_consumer_that_gains_provide_serves_its_tool() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    // The slug running another artifact, under a class that provides nothing.
    let tree = Tree::holding(&agent_document_with(
        home.path(),
        &format!(
            r#"channel sink at "{SINK}" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{PACKAGED}component Provider {{
    abi = processor;
    requires = [ports];
    in inbound;
    out digest;
}}
{PACKAGED}
new {PROVIDER}: Provider {{
    grants = [ports];
    in inbound <- work {{ push_depth = 4; }}
    out digest -> sink;
}}
"#
        ),
    ));
    install_package_from(
        components.path(),
        &staged_module(&tree),
        "brenn_processor_demo.wasm",
    );
    let mut booted = boot_with(&tree, fixture(&components, true, false)).await;

    tree.write(&providing_document(true, home.path()));
    install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_applied(&status);
    assert_eq!(status.delta.consumers_changed, [PROVIDER]);
    assert!(status.delta.consumers_added.is_empty());
    assert_eq!(
        booted
            .driver
            .env()
            .tool_registry
            .load()
            .provided_by("echo-for-user"),
        Some(PROVIDER)
    );
    assert_eq!(agent_calls_echo(&booted).await["mode"], "echo");
}

/// The first `api_key_provider` registers `api-request`, and the executor
/// answers a component's call on it: the refusal of a write by a caller with
/// no acting user is `execute`'s own, after the provider resolved against the
/// swapped key table. No request leaves the process.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_key_provider_is_answered_by_the_executor() {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&super::driver::tests::document(""));
    let mut booted = boot_with(&tree, fixture(&components, true, true)).await;

    let host = tempfile::tempdir().expect("a host directory");
    let provider = key_provider("maps", &key_file(host.path().join("maps"), "K3Y-maps\n"));
    let args = json!({
        "provider": "maps",
        "method": "POST",
        "host": "api.example.com",
        "path": "/v1/things",
    });
    let candidate = with_caller(&super::driver::tests::document(&format!(
        "{provider}{}",
        caller_instance("api-request", &args, r#"allow { provider = "maps"; }"#)
    )));
    tree.write(&candidate);
    stage_caller(&tree, components.path());
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_applied(&status);
    assert_eq!(status.delta.api_key_providers_added, ["maps"]);
    assert_channel_added(&status, "brenn:tools/api-request");
    assert!(
        booted
            .driver
            .env()
            .tool_registry
            .load()
            .get("api-request")
            .is_some()
    );
    assert!(booted.api_keys.load().get("maps").is_some());
    assert_executor_serves(&booted, "brenn:tools/api-request").await;

    trigger(&booted).await;
    let result = first_result(&mut booted).await;
    assert_eq!(result["tool"], "api-request", "{result}");
    assert!(
        result
            .to_string()
            .contains("a caller with no acting user may only read"),
        "{result}"
    );
}

/// The deployer's case: a running process serving one packaged provider
/// component gains a second, with its key provider, its result-inbox tuning
/// block, its instance's clauses and the agent's grant, in one reload.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_provider_component_arrives_beside_the_first() {
    use super::google_maps_tools_boot_tests as maps;

    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let (base, candidate) = owners_documents(home.path(), host.path());
    let tree = Tree::holding(&base);
    install_package_from(components.path(), &staged_module(&tree), maps::ARTIFACT);
    let mut booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.path().to_path_buf()],
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await;

    write_root(&tree, &candidate);
    stage_echo(&tree, components.path());
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_applied(&status);
    assert_eq!(status.delta.consumers_added, [PROVIDER]);
    assert_eq!(status.delta.api_key_providers_added, [ECHO_KEYS]);
    assert_eq!(status.delta.agents_changed, [ASSISTANT]);
    assert_channel_added(&status, "brenn:tools/echo-for-user");
    assert_channel_added(&status, &format!("brenn:tool-results/{PROVIDER}"));
    assert!(booted.api_keys.load().get(ECHO_KEYS).is_some());
    assert_eq!(agent_calls_echo(&booted).await["mode"], "echo");
}

/// The package the second provider component's class is installed under.
const ECHO_MODULE: &str = "echo";

/// The second component's key provider.
const ECHO_KEYS: &str = "echo-keys";

/// The second provider component's class: the providing class over the
/// provider fixture, in a module of its own.
fn echo_module() -> String {
    format!(
        "component Echo {{\n    abi = processor;\n{}}}\n",
        provider_class(true)
    )
}

/// Write the second component's module beside the tree's fenced one and
/// install its package under `components`.
fn stage_echo(tree: &Tree, components: &std::path::Path) {
    let module = echo_module();
    std::fs::write(tree.modules().join(format!("{ECHO_MODULE}.brenn")), &module)
        .expect("the module is writable");
    install_named_package(components, ECHO_MODULE, &module, PROVIDER_ARTIFACT);
}

/// A deployment-key provider block named `name` over the key file `key`, on
/// one API host.
fn key_provider(name: &str, key: &std::path::Path) -> String {
    format!(
        "\napi_key_provider {name} {{\n    key_file = \"{}\";\n    header = \"X-Api-Key\";\n    \
         api_hosts = [\"https://api.example.com\"];\n}}\n",
        key.display()
    )
}

/// The `google-maps-tools` boot document, and that document with a second
/// provider component added the way a deployer adds one: a key provider, the
/// result-inbox tuning block, an instance whose `api-request` clauses name
/// the new key provider, and the agent's grant on its tool. The second
/// component's module is [`stage_echo`]'s to write.
fn owners_documents(home: &std::path::Path, host: &std::path::Path) -> (String, String) {
    use super::google_maps_tools_boot_tests as maps;

    let maps = maps::document(
        home,
        &key_file(host.join("maps"), "K3Y-maps\n"),
        &host.join("google-maps-tools.db"),
        "",
    );
    let anchor = "    tool weather {}\n";
    assert!(maps.contains(anchor), "the agent is granted the maps tools");
    let candidate = format!(
        r#"use @{ECHO_MODULE}::*;
{agent}
{keys}
channel at "brenn:tool-results/{PROVIDER}" {{
    push_depth = 1;
    retain_depth = 16;
    standing_retain_depth = 16;
    noise = alarm;
}}

new {PROVIDER}: Echo {{
    grants = [tools, provides];
    tool api-request {{
        allow {{ provider = "{ECHO_KEYS}"; method = "GET"; host = "api.example.com"; path = "/v1/*"; }}
    }}
}}
"#,
        agent = maps.replacen(anchor, &format!("{anchor}    tool echo-for-user {{}}\n"), 1),
        keys = key_provider(ECHO_KEYS, &key_file(host.join(ECHO_KEYS), "K3Y-echo\n")),
    );
    (maps, candidate)
}

/// Write `document`'s root half and leave the tree's fenced module as it
/// stands, as a deployer edits the document and not the installed package:
/// the fenced half is written line for line, so text added anywhere outside
/// the fence would move the specification hash the package is bound to.
fn write_root(tree: &Tree, document: &str) {
    let (_, root) = brenn_lib::config::split_packaged(document).expect("the document is fenced");
    std::fs::write(tree.root(), root).expect("the document is writable");
}

/// A native async tool admitting one call at a time, whose calls count
/// themselves in and then wait for a permit on `gate`.
fn gated_tool(
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<AtomicUsize>,
) -> brenn_tool_registry::RegisteredTool {
    use brenn_tool_registry::ToolError;
    use brenn_tool_registry::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
    use brenn_tool_registry::tool::{AsyncTool, RegisteredTool, ToolCtx};

    struct Gated {
        descriptor: ToolDescriptor,
        gate: Arc<tokio::sync::Semaphore>,
        entered: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AsyncTool for Gated {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(
            &self,
            _args: &Value,
            _clauses: &[brenn_lib::tools::AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        async fn execute(&self, _ctx: &ToolCtx, _args: Value) -> Result<Value, ToolError> {
            let n = self.entered.fetch_add(1, Ordering::SeqCst) + 1;
            self.gate
                .acquire()
                .await
                .expect("the gate stays open")
                .forget();
            Ok(json!({ "call": n }))
        }
    }
    RegisteredTool::Async(Arc::new(Gated {
        descriptor: ToolDescriptor {
            name: "gated",
            mcp_name: "mcp__brenn__Gated",
            description: "one call at a time",
            input_schema: json!({ "type": "object" }),
            class: ToolClass::Async { max_concurrency: 1 },
            acl_keys: &[],
            idempotency: Idempotency::Natural,
            auto_approve: true,
            user_binding: brenn_tool_registry::UserBinding::None,
        },
        gate,
        entered,
    }))
}

/// A tool the reload did not add keeps its concurrency bound across the
/// swap: a call admitted on the new registry waits behind one in flight on
/// the old, because both acquire the one semaphore.
#[tokio::test(flavor = "multi_thread")]
async fn an_unchanged_tool_keeps_its_bound_across_the_swap() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let tree = Tree::holding(&super::driver::tests::document(""));
    let mut booted = boot_with(
        &tree,
        BootFixture {
            native_tools: vec![gated_tool(gate.clone(), entered.clone())],
            ..BootFixture::default()
        },
    )
    .await;

    let alert = booted.driver.env().alert_dispatcher.clone();
    let call = |registry: Arc<brenn_tool_registry::ToolRegistry>| {
        let alert = alert.clone();
        tokio::spawn(async move {
            registry
                .invoke_for_llm(
                    "mcp__brenn__Gated",
                    ParticipantId::for_app(ASSISTANT, "test-origin"),
                    ResolvedToolGrant {
                        acl: vec![],
                        rate_limit: None,
                    },
                    None,
                    None,
                    json!({}),
                    &alert,
                )
                .await
        })
    };
    let before = booted.driver.env().tool_registry.load();
    let first = call(before.clone());
    super::driver::tests::poll_until("calls in execute", 1, None, async || {
        vec![(); entered.load(Ordering::SeqCst)]
    })
    .await;

    let host = tempfile::tempdir().expect("a host directory");
    let status = reload_onto(
        &mut booted,
        &tree,
        &super::driver::tests::document(&key_provider(
            "maps",
            &key_file(host.path().join("maps"), "K3Y-maps\n"),
        )),
    )
    .await;
    assert_applied(&status);
    let after = booted.driver.env().tool_registry.load();
    assert!(!Arc::ptr_eq(&before, &after), "the reload swapped the cell");

    let second = call(after);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "the second call waits for the first's permit"
    );
    gate.add_permits(2);
    let first = first
        .await
        .expect("the first call's task")
        .expect("the first call");
    let second = second
        .await
        .expect("the second call's task")
        .expect("the second call");
    assert_eq!(
        (first["call"].clone(), second["call"].clone()),
        (json!(1), json!(2))
    );
}

/// A reload adding a provider whose package is not installed is refused, and
/// the cell still holds the registry the process booted with.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_reload_leaves_the_registry() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&agent_document(home.path()));
    let mut booted = boot_with(&tree, fixture(&components, false, false)).await;
    let before = booted.driver.env().tool_registry.load();

    let status = reload_onto(&mut booted, &tree, &providing_document(true, home.path())).await;
    assert_eq!(status.outcome, Outcome::Refused, "{status:?}");
    assert!(Arc::ptr_eq(
        &before,
        &booted.driver.env().tool_registry.load()
    ));
}

// ── The oracle over the transitions that add tools ────────────────────────

/// The tool lines of the reloaded process name `tool`.
fn assert_tool_line(tools: &[String], tool: &str) {
    assert!(
        tools
            .iter()
            .any(|line| line.starts_with(&format!("{tool} "))),
        "{tool} not in {tools:?}"
    );
}

/// A providing instance and the agent's grant on its tool, against a fresh
/// boot of the document holding both.
#[tokio::test(flavor = "multi_thread")]
async fn a_providing_consumer_added_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let tree = Tree::holding(&agent_document(home.path()));

    super::oracle_tests::a_reload_matches_a_fresh_boot(
        &tree,
        |db| BootFixture {
            db: Some(db),
            ..fixture(&components, false, false)
        },
        async |booted| {
            tree.write(&providing_document(true, home.path()));
            install_package_from(components.path(), &staged_module(&tree), PROVIDER_ARTIFACT);
            booted.driver.reload(TriggerSource::Signal).await;
        },
        |reloaded| assert_tool_line(reloaded.tools(), "echo-for-user"),
    )
    .await;
}

/// The first `api_key_provider`, against a fresh boot of the document
/// declaring it.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_key_provider_matches_a_fresh_boot() {
    let components = tempfile::tempdir().expect("a components root");
    let host = tempfile::tempdir().expect("a host directory");
    let tree = Tree::holding(&super::driver::tests::document(""));
    let key = key_file(host.path().join("maps"), "K3Y-maps\n");

    super::oracle_tests::a_reload_matches_a_fresh_boot(
        &tree,
        |db| BootFixture {
            db: Some(db),
            ..fixture(&components, false, false)
        },
        async |booted| {
            tree.write(&super::driver::tests::document(&key_provider("maps", &key)));
            booted.driver.reload(TriggerSource::Signal).await;
        },
        |reloaded| assert_tool_line(reloaded.tools(), "api-request"),
    )
    .await;
}

/// The deployer's second provider component, against a fresh boot of the
/// document holding both components.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_provider_component_matches_a_fresh_boot() {
    use super::google_maps_tools_boot_tests as maps;

    let components = tempfile::tempdir().expect("a components root");
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let (base, candidate) = owners_documents(home.path(), host.path());
    let tree = Tree::holding(&base);
    install_package_from(components.path(), &staged_module(&tree), maps::ARTIFACT);

    super::oracle_tests::a_reload_matches_a_fresh_boot(
        &tree,
        |db| BootFixture {
            db: Some(db),
            components_roots: vec![components.path().to_path_buf()],
            ..BootFixture::default()
        },
        async |booted| {
            write_root(&tree, &candidate);
            stage_echo(&tree, components.path());
            booted.driver.reload(TriggerSource::Signal).await;
        },
        |reloaded| assert_tool_line(reloaded.tools(), "echo-for-user"),
    )
    .await;
}
