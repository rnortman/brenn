//! The `google-workspace-tools` component's boot path, from a document to an
//! agent's tool list.
//!
//! The document declares the component's class as its packaged specification
//! states it, word for word, installed with the component's own artifact; the
//! server's vault and the `oauth_provider` an operator writes; one instance of
//! the class with its `user = "*"` clauses, `config` map and result-inbox
//! tuning block; and a containerized agent granted every tool the class
//! provides. The registry is built over the OAuth service `run_server` builds:
//! the document's provider and vault, resolved by `resolve_oauth`.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;

use brenn_lib::config::PACKAGED;

use super::driver::tests::{Tree, resolve_fixture_apps};
use super::provider_class_boot::{ProviderClassBoot, assert_provider_class_boots};

/// The instance's slug.
const WORKSPACE: &str = "workspace";

/// The agent granted the tools.
const ASSISTANT: &str = "assistant";

/// The component's artifact.
const ARTIFACT: &str = "brenn_google_workspace_tools.wasm";

/// The OAuth provider the instance's clauses and `config` name.
const PROVIDER: &str = "google";

/// The tools the class provides, by registry name and MCP name, in
/// declaration order.
const TOOLS: &[(&str, &str)] = &[
    ("gmail-labels", "GmailLabels"),
    ("gmail-search", "GmailSearch"),
];

/// The paths the instance's clauses admit, one per endpoint the tools call.
const PATHS: &[&str] = &[
    "/gmail/v1/users/me/labels",
    "/gmail/v1/users/me/labels/*",
    "/gmail/v1/users/me/messages",
    "/gmail/v1/users/me/messages/*",
];

/// The class's packaged specification.
fn spec() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../config/specs/google-workspace-tools.brenn");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The operator's document: the class, the server's vault, the OAuth
/// provider, the tuning block, one instance with its clauses and `config`,
/// and an agent granted every tool.
fn document(
    home: &std::path::Path,
    vault: &std::path::Path,
    client_file: &std::path::Path,
    store: &std::path::Path,
) -> String {
    let grants: String = TOOLS
        .iter()
        .map(|(name, _)| format!("    tool {name} {{}}\n"))
        .collect();
    let clauses: String = PATHS
        .iter()
        .map(|path| {
            format!(
                "        allow {{ provider = \"{PROVIDER}\"; user = \"*\"; method = \"GET\"; \
                 host = \"gmail.googleapis.com\"; path = \"{path}\"; }}\n"
            )
        })
        .collect();
    super::driver::tests::document(&format!(
        r#"{PACKAGED}{spec}{PACKAGED}
server {{
    public_url = "https://brenn.example.com";
    vault_dir = "{vault}";
}}

container sandbox {{
    image = "example.com/cc:latest";
    home_dir = "{home}";
}}

oauth_provider {PROVIDER} {{
    app_name = "Brenn";
    client_secret_file = "{client_file}";
    authorize_url = "https://accounts.google.com/o/oauth2/v2/auth";
    token_url = "https://oauth2.googleapis.com/token";
    revoke_url = "https://oauth2.googleapis.com/revoke";
    userinfo_url = "https://openidconnect.googleapis.com/v1/userinfo";
    account_field = "email";
    base_scopes = ["openid", "https://www.googleapis.com/auth/userinfo.email"];
    scope_sets = {{
        gmail_readonly = ["https://www.googleapis.com/auth/gmail.readonly"]
    }};
    api_hosts = ["https://gmail.googleapis.com"];
}}

channel at "brenn:tool-results/{WORKSPACE}" {{
    push_depth = 1;
    retain_depth = 64;
    standing_retain_depth = 64;
    noise = alarm;
}}

new {WORKSPACE}: GoogleWorkspaceTools {{
    grants = [log, store, config, tools, provides];
    store_path = "{store}";
    tool api-request {{
{clauses}        rate_limit {{ burst = 60; sustained_per_minute = 120; }}
    }}
    config = {{
        provider = "{PROVIDER}",
        gmail_host = "gmail.googleapis.com"
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
        vault = vault.display(),
        client_file = client_file.display(),
        store = store.display(),
    ))
}

/// The packaged class boots beside an `oauth_provider`: the document's
/// provider and vault resolve, each tool is a provided tool in the registry
/// and on the agent's written list, the instance's `user = "*"` clauses
/// validate against the provider, the request inbox is as deep as the class's
/// summed concurrency, and the result inbox takes the tuning block's depth.
#[tokio::test(flavor = "multi_thread")]
async fn google_workspace_tools_boots_into_an_agents_tool_list() {
    let home = tempfile::tempdir().expect("a container home");
    let host = tempfile::tempdir().expect("a host directory");
    let vault = host.path().join("vault");
    std::fs::create_dir(&vault).expect("a vault directory");
    std::fs::set_permissions(&vault, std::fs::Permissions::from_mode(0o700))
        .expect("the vault is private");
    // The client file the provider's console issues, registered for the
    // server's callback.
    let client_file = host.path().join("client_secret.json");
    std::fs::write(
        &client_file,
        serde_json::json!({ "web": {
            "client_id": "brenn-client.apps.example.com",
            "client_secret": "client-deployment-secret",
            "redirect_uris": [format!("https://brenn.example.com/oauth/{PROVIDER}/callback")],
        } })
        .to_string(),
    )
    .expect("write the client file");
    std::fs::set_permissions(&client_file, std::fs::Permissions::from_mode(0o600))
        .expect("the client file is private");
    let store = host.path().join("google-workspace-tools.db");
    let tree = Tree::holding(&document(home.path(), &vault, &client_file, &store));

    let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
    assert_provider_class_boots(
        ProviderClassBoot {
            tree: &tree,
            artifact: ARTIFACT,
            slug: WORKSPACE,
            agent: ASSISTANT,
            tools: TOOLS,
            result_depth: 64,
        },
        |config| {
            let apps = resolve_fixture_apps(config, &tree.runtime_dir());
            let resolved = brenn_lib::config::resolve_oauth(config, &apps)
                .expect("the document declares a provider");
            let service = std::sync::Arc::new(brenn_oauth::OAuthService::new(resolved, alerts));
            brenn_oauth::tools::registered(Some(&service), BTreeMap::new())
        },
    )
    .await;
}
