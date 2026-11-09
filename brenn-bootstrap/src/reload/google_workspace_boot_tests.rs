//! The Google Workspace components' boot path, from a document to an agent's
//! tool list: one case per component.
//!
//! Each case's document declares one component's class as its packaged
//! specification states it, word for word, installed with the component's own
//! artifact; the server's vault and the `oauth_provider` an operator writes;
//! one instance of the class with its `user = "*"` clauses, one-key `config`
//! map and result-inbox tuning block; and a containerized agent granted every
//! tool the class provides. The registry is built over the OAuth service
//! `run_server` builds: the document's provider and vault, resolved by
//! `resolve_oauth`.

use std::os::unix::fs::PermissionsExt;

use brenn_lib::config::PACKAGED;

use super::driver::tests::{Tree, resolve_fixture_apps};
use super::provider_class_boot::{ProviderClassBoot, assert_provider_class_boots};

/// The agent granted the tools.
const ASSISTANT: &str = "assistant";

/// The OAuth provider every instance's clauses and `config` name.
const PROVIDER: &str = "google";

/// Gmail's and Calendar's API hosts.
const GMAIL_HOST: &str = "gmail.googleapis.com";
const CALENDAR_HOST: &str = "www.googleapis.com";

/// One component's class and the instance of it each case declares.
struct Class {
    /// The component's artifact.
    artifact: &'static str,
    /// The class's packaged specification, under `config/specs/`.
    spec: &'static str,
    /// The class's name.
    class: &'static str,
    /// The instance's slug.
    slug: &'static str,
    /// The `config` key the component reads its host under, and the host.
    host_key: &'static str,
    host: &'static str,
    /// The tools the class provides, by registry name and MCP name, in
    /// declaration order.
    tools: &'static [(&'static str, &'static str)],
    /// The method and path each of the instance's clauses admits on `host`,
    /// one per endpoint the class's tools call.
    clauses: &'static [(&'static str, &'static str)],
    /// The result inbox's depth, as the tuning block sets it.
    result_depth: u64,
}

const GMAIL_READ: Class = Class {
    artifact: "brenn_google_gmail_read.wasm",
    spec: "google-gmail-read.brenn",
    class: "GoogleGmailRead",
    slug: "gmail-read",
    host_key: "gmail_host",
    host: GMAIL_HOST,
    tools: &[
        ("gmail-labels", "GmailLabels"),
        ("gmail-search", "GmailSearch"),
        ("gmail-read", "GmailRead"),
        ("gmail-thread", "GmailThread"),
    ],
    clauses: &[
        ("GET", "/gmail/v1/users/me/labels"),
        ("GET", "/gmail/v1/users/me/labels/*"),
        ("GET", "/gmail/v1/users/me/messages"),
        ("GET", "/gmail/v1/users/me/messages/*"),
        ("GET", "/gmail/v1/users/me/threads/*"),
    ],
    result_depth: 32,
};

const GMAIL_COMPOSE: Class = Class {
    artifact: "brenn_google_gmail_compose.wasm",
    spec: "google-gmail-compose.brenn",
    class: "GoogleGmailCompose",
    slug: "gmail-compose",
    host_key: "gmail_host",
    host: GMAIL_HOST,
    tools: &[("gmail-draft", "GmailDraft")],
    clauses: &[
        ("GET", "/gmail/v1/users/me/messages/*"),
        ("POST", "/gmail/v1/users/me/drafts"),
    ],
    result_depth: 16,
};

const GMAIL_ORGANIZE: Class = Class {
    artifact: "brenn_google_gmail_organize.wasm",
    spec: "google-gmail-organize.brenn",
    class: "GoogleGmailOrganize",
    slug: "gmail-organize",
    host_key: "gmail_host",
    host: GMAIL_HOST,
    tools: &[("gmail-modify-labels", "GmailModifyLabels")],
    clauses: &[("POST", "/gmail/v1/users/me/messages/batchModify")],
    result_depth: 16,
};

const CALENDAR_READ: Class = Class {
    artifact: "brenn_google_calendar_read.wasm",
    spec: "google-calendar-read.brenn",
    class: "GoogleCalendarRead",
    slug: "calendar-read",
    host_key: "calendar_host",
    host: CALENDAR_HOST,
    tools: &[
        ("calendar-list", "CalendarList"),
        ("calendar-events", "CalendarEvents"),
        ("calendar-event", "CalendarEvent"),
        ("calendar-free-busy", "CalendarFreeBusy"),
    ],
    clauses: &[
        ("GET", "/calendar/v3/users/me/calendarList"),
        ("GET", "/calendar/v3/calendars/*/events"),
        ("GET", "/calendar/v3/calendars/*/events/*"),
        ("POST", "/calendar/v3/freeBusy"),
    ],
    result_depth: 32,
};

const CALENDAR_WRITE: Class = Class {
    artifact: "brenn_google_calendar_write.wasm",
    spec: "google-calendar-write.brenn",
    class: "GoogleCalendarWrite",
    slug: "calendar-write",
    host_key: "calendar_host",
    host: CALENDAR_HOST,
    tools: &[
        ("calendar-create", "CalendarCreate"),
        ("calendar-update", "CalendarUpdate"),
        ("calendar-delete", "CalendarDelete"),
    ],
    clauses: &[
        ("GET", "/calendar/v3/calendars/*/events/*"),
        ("POST", "/calendar/v3/calendars/*/events"),
        ("PATCH", "/calendar/v3/calendars/*/events/*"),
        ("DELETE", "/calendar/v3/calendars/*/events/*"),
    ],
    result_depth: 16,
};

/// The class's packaged specification.
fn spec(class: &Class) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../config/specs")
        .join(class.spec);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The operator's document: the class, the server's vault, the OAuth
/// provider, the tuning block, one instance with its clauses and `config`,
/// and an agent granted every tool.
fn document(
    class: &Class,
    home: &std::path::Path,
    vault: &std::path::Path,
    client_file: &std::path::Path,
    store: &std::path::Path,
) -> String {
    let grants: String = class
        .tools
        .iter()
        .map(|(name, _)| format!("    tool {name} {{}}\n"))
        .collect();
    let clauses: String = class
        .clauses
        .iter()
        .map(|(method, path)| {
            format!(
                "        allow {{ provider = \"{PROVIDER}\"; user = \"*\"; method = \"{method}\"; \
                 host = \"{host}\"; path = \"{path}\"; }}\n",
                host = class.host,
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
        gmail_readonly = ["https://www.googleapis.com/auth/gmail.readonly"],
        gmail_compose = ["https://www.googleapis.com/auth/gmail.compose"],
        gmail_organize = ["https://www.googleapis.com/auth/gmail.labels", "https://www.googleapis.com/auth/gmail.modify"],
        calendar_readonly = ["https://www.googleapis.com/auth/calendar.readonly"],
        calendar_events = ["https://www.googleapis.com/auth/calendar.events"]
    }};
    api_hosts = ["https://{GMAIL_HOST}", "https://{CALENDAR_HOST}"];
}}

channel at "brenn:tool-results/{slug}" {{
    push_depth = 1;
    retain_depth = {depth};
    standing_retain_depth = {depth};
    noise = alarm;
}}

new {slug}: {class_name} {{
    grants = [log, store, config, tools, provides];
    store_path = "{store}";
    tool api-request {{
{clauses}        rate_limit {{ burst = 60; sustained_per_minute = 120; }}
    }}
    config = {{
        provider = "{PROVIDER}",
        {host_key} = "{host}"
    }};
}}

agent Assistant() {{
    working_dir = ".";
    container = "sandbox";
    container_working_dir = "/work";
{grants}}}

new {ASSISTANT}: Assistant();
"#,
        spec = spec(class),
        slug = class.slug,
        depth = class.result_depth,
        class_name = class.class,
        host_key = class.host_key,
        host = class.host,
        home = home.display(),
        vault = vault.display(),
        client_file = client_file.display(),
        store = store.display(),
    ))
}

/// `class` boots beside an `oauth_provider`: the document's provider and
/// vault resolve, each tool is a provided tool in the registry and on the
/// agent's written list, the instance's `user = "*"` clauses validate against
/// the provider, the request inbox is as deep as the class's summed
/// concurrency, and the result inbox takes the tuning block's depth.
async fn assert_class_boots(class: &Class) {
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
    let store = host.path().join(format!("google-{}.db", class.slug));
    let tree = Tree::holding(&document(class, home.path(), &vault, &client_file, &store));

    let (alerts, _alerts_task) = brenn_obs::alerting::noop_alert_dispatcher();
    assert_provider_class_boots(
        ProviderClassBoot {
            tree: &tree,
            artifact: class.artifact,
            slug: class.slug,
            agent: ASSISTANT,
            tools: class.tools,
            result_depth: class.result_depth,
        },
        |config| {
            let apps = resolve_fixture_apps(config, &tree.runtime_dir());
            let resolved = brenn_lib::config::resolve_oauth(config, &apps)
                .expect("the document declares a provider");
            let service = std::sync::Arc::new(brenn_oauth::OAuthService::new(resolved, alerts));
            brenn_oauth::tools::registered(Some(&service), brenn_lib::config::ApiKeyTable::empty())
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn google_gmail_read_boots_into_an_agents_tool_list() {
    assert_class_boots(&GMAIL_READ).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn google_gmail_compose_boots_into_an_agents_tool_list() {
    assert_class_boots(&GMAIL_COMPOSE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn google_gmail_organize_boots_into_an_agents_tool_list() {
    assert_class_boots(&GMAIL_ORGANIZE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn google_calendar_read_boots_into_an_agents_tool_list() {
    assert_class_boots(&CALENDAR_READ).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn google_calendar_write_boots_into_an_agents_tool_list() {
    assert_class_boots(&CALENDAR_WRITE).await;
}
