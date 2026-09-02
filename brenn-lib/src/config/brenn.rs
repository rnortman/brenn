use std::collections::HashMap;
use std::path::{Path, PathBuf};

use brenn_dsl::DocumentInputs;
use brenn_dsl::diag::Diagnostic;

use super::alerting::AlertingConfig;
use super::app::AppConfigRaw;
use super::claude_defaults::ClaudeDefaultsConfig;
use super::container::ContainerConfig;
use super::events::EventsConfig;
use super::logging::LoggingConfig;
use super::observability::ObservabilityConfig;
use super::repo::{RepoDeclRaw, RepoSyncConfig};
use super::security::SecurityConfig;
use super::server::{DatabaseConfig, ServerConfig};
use super::surface_description::SurfaceDescriptionConfig;
use super::wasm::WasmConfig;
use super::watchdog::WatchdogConfig;

/// CC built-in tools we've vetted and expect to exist.
/// Used for three purposes:
/// 1. Config validation: warn if `disabled_tools` contains unknown entries.
/// 2. `--tools` whitelist computation: `CC_KNOWN_TOOLS - disabled_tools`.
/// 3. Runtime validation: alert if CC reports tools not in this list.
///
/// MCP tools (`mcp__*`) are NOT included — they're managed separately.
/// Audited from a live CC session's `system/init` response (CC 2.1.112,
/// 2026-04-18; includes tools surfaced by
/// `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`).
pub const CC_KNOWN_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "Bash",
    "CronCreate",
    "CronDelete",
    "CronList",
    "Edit",
    "EnterPlanMode",
    "EnterWorktree",
    "ExitPlanMode",
    "ExitWorktree",
    "Glob",
    "Grep",
    "LSP",
    "Monitor",
    "NotebookEdit",
    "PushNotification",
    "Read",
    "RemoteTrigger",
    "ScheduleWakeup",
    "SendMessage",
    "Skill",
    "Task",
    "TaskCreate",
    "TaskGet",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "TaskUpdate",
    "TeamCreate",
    "TeamDelete",
    "TodoWrite",
    "ToolSearch",
    "WebFetch",
    "WebSearch",
    "Write",
];

/// Top-level Brenn configuration.
///
/// Defaults are production-hardened (absolute paths, secure cookies on, etc.).
/// Use `brenn.dev.brenn` for local development.
#[derive(Debug, Default, PartialEq)]
pub struct BrennConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    pub security: SecurityConfig,
    pub alerting: Option<AlertingConfig>,
    pub claude_defaults: ClaudeDefaultsConfig,
    /// Repo-sync settings (polling interval, staleness cap, webhook URL prefix
    /// override). See `docs/designs/repo-sync.md`. Omitting the section falls
    /// back to `RepoSyncConfig::default()`.
    pub repo_sync: RepoSyncConfig,
    /// Top-level repo declarations. Each repo is cloned to `<repo_dir>/<slug>/`
    /// on startup if the directory doesn't exist.
    pub repos: Vec<RepoDeclRaw>,
    /// Podman container definitions. Apps reference these by name.
    pub container: HashMap<String, ContainerConfig>,
    /// Global integration defaults. Apps enable integrations by name and can
    /// override specific config keys per-app via `integration_config`.
    pub integrations: HashMap<String, toml::Value>,
    /// Per-app configurations, one per `agent` instantiation. At least one must
    /// be defined. Validated and converted to a HashMap keyed by slug via
    /// `validate_and_resolve`.
    pub apps: Vec<AppConfigRaw>,
    /// Top-level `channel` declarations — one entry for every pub/sub
    /// scheme, durable or not. The address's scheme picks the channel's
    /// capabilities. See `crate::messaging::config::ChannelConfigRaw`.
    pub channels: Vec<crate::messaging::config::ChannelConfigRaw>,
    /// Global messaging defaults (the `messaging` section). Defaults to
    /// `MessagingGlobalConfig::default()` when absent.
    pub messaging: crate::messaging::config::MessagingGlobalConfig,
    /// Observability settings (the `observability` section). Defaults to
    /// `ObservabilityConfig::default()` when absent.
    pub observability: ObservabilityConfig,
    /// Surface self-description settings (the `surface_description` section). Defaults to
    /// `SurfaceDescriptionConfig::default()` (no prefix ⇒ feature off) when absent.
    pub surface_description: SurfaceDescriptionConfig,
    /// Chat-over-pubsub settings (the `llm_chat` section). Defaults to
    /// `LlmChatConfig::default()` when absent.
    pub llm_chat: super::llm_chat::LlmChatConfig,
    /// Global VAPID keypair and subject for PWA push notifications.
    /// The `pwa_push` section. Defaults to `PwaPushGlobalConfig::default()`
    /// (all-None) when absent — safe zero values; no keypair loaded unless
    /// an app has `pwa_push.enabled = true`.
    pub pwa_push: crate::pwa_push::config::PwaPushGlobalConfig,
    /// Automation engine config (the `automation` section). Defaults to
    /// `AutomationGlobalConfig::default()` when absent.
    pub automation: crate::config::AutomationGlobalConfig,
    /// Top-level `mqtt_client` declarations. Each entry defines an MQTT
    /// client (the app-independent connection to a remote MQTT broker/server).
    /// Apps address it for egress via the `mqtt_publish` ACL naming the client
    /// slug, and subscribe to it with a `subscribe` statement naming
    /// `mqtt:<client>:<topic>` (ingress).
    pub mqtt_clients: Vec<crate::mqtt::config::MqttClientConfigRaw>,
    /// Top-level `webhook` declarations. Each entry defines an inbound HTTP
    /// webhook endpoint. Apps bind to endpoints with a `subscribe` statement
    /// naming `webhook:<slug>`.
    pub webhook_endpoints: Vec<crate::webhook::config::WebhookEndpointConfigRaw>,
    /// Events table retention settings (the `events` section). Defaults to
    /// `EventsConfig::default()` when absent (7-day delivered-row retention).
    pub events: EventsConfig,
    /// One entry per WASM-consumer instantiation; see
    /// [`crate::messaging::config::WasmConsumerConfigRaw`] for per-entry fields.
    pub wasm_consumers: Vec<crate::messaging::config::WasmConsumerConfigRaw>,
    /// Top-level `surface` declarations; see
    /// [`crate::messaging::config::SurfaceConfigRaw`] for per-entry fields.
    pub surfaces: Vec<crate::messaging::config::SurfaceConfigRaw>,
    /// Top-level `remote` declarations — authenticated native-daemon attachers on
    /// the same attach transport the browser surfaces use; see
    /// [`crate::messaging::remote::RemoteConfigRaw`] for per-entry fields.
    pub remotes: Vec<crate::messaging::remote::RemoteConfigRaw>,
    /// `link` declarations — auto channels named by nothing, brought into
    /// existence by the ports bound to them rather than by a `channel`
    /// declaration. No document spells this form yet, so the field is always
    /// empty. See [`crate::messaging::config::LinkConfigRaw`].
    pub links: Vec<crate::messaging::config::LinkConfigRaw>,
    /// Global WASM-host policy (the `wasm` section). Controls defaults such as
    /// store size limits. Omitting the block is equivalent to `WasmConfig::default()`.
    pub wasm: WasmConfig,
    /// Bridge-wedge watchdog settings (the `watchdog` section). Defaults to
    /// `WatchdogConfig::default()` when absent (30 s sweep, 60 s wedge grace).
    pub watchdog: WatchdogConfig,
}

/// Sort the collections in a loaded config whose order the runtime ignores.
///
/// **Comparison-only.** Nothing the runtime boots goes through here: this exists
/// so two configs that say the same thing in a different order compare equal.
/// The rule for adding a collection is a citation to the runtime code proving
/// its order is dead, and nothing weaker — a wrong sort makes the comparison
/// report false *equality*, which is the unacceptable direction. A false
/// difference is merely annoying, so an uncited collection stays where it is.
///
/// What is sorted, and why its order is dead:
///
/// - **Grants.** App, surface, wasm-consumer and remote grants alike resolve
///   into a `GrantSet` — a `BTreeSet` — with duplicates refused
///   (`build_app_policy`, `build_wasm_policy`, `build_attach_policy`). Set
///   semantics: position carries nothing.
/// - **ACL matcher lists.** Every enforcement site is `.any(...)` over the
///   whole list and an empty list denies (`AppPolicy::acl_covers` and its
///   siblings). No first-match anywhere.
/// - **Remote subscribe ACLs.** The ceiling fold is max over *every* matching
///   entry, explicitly not the first (`RemoteSubscribeAcl::ceiling_for`), so a
///   broad prefix and a deeper exact entry mean the same thing in either order.
///
/// Deliberately left order-compared: hook command lists, process `args`,
/// container `extra_args` / `extra_mounts`, tool grants, and the block arrays
/// themselves — all of those the runtime reads in order.
pub fn sort_order_dead_collections(config: &mut BrennConfig) {
    // Every ACL-bearing block is destructured with no `..`, so a matcher list
    // added to any of them fails compilation here rather than quietly staying
    // order-compared. Matcher order comes from the types' derived `Ord`, which
    // ranks exactly the fields their derived `PartialEq` compares.
    for app in &mut config.apps {
        let crate::config::app::AppConfigRaw {
            slug: _,
            name: _,
            description: _,
            icon: _,
            working_dir: _,
            model: _,
            models: _,
            single_instance: _,
            singleton: _,
            persistent: _,
            idle_timeout_secs: _,
            compact_reminder_pct: _,
            compact_soft_pct: _,
            compact_red_pct: _,
            compact_hard_pct: _,
            compact_reminder_tokens: _,
            compact_soft_tokens: _,
            compact_red_tokens: _,
            compact_hard_tokens: _,
            compact_idle_secs: _,
            idle_hook_secs: _,
            allowed_users: _,
            disabled_tools: _,
            mcp_servers: _,
            multiuser: _,
            prefix_username: _,
            prefix_timestamp: _,
            prefix_device: _,
            container: _,
            container_working_dir: _,
            start_hooks: _,
            post_pull_hooks: _,
            startup_hooks: _,
            cc_extra_args: _,
            approval_rules: _,
            attachment_targets: _,
            integrations: _,
            integration_config: _,
            mounts: _,
            extra_mounts: _,
            history_replay_limit: _,
            frontmatter: _,
            messaging: _,
            pwa_push: _,
            webhook_subscriptions: _,
            mqtt_subscriptions: _,
            grants,
            acl,
            tool_grants: _,
        } = app;
        grants.sort_unstable();
        let crate::access::raw::AppAclRaw {
            mqtt_subscribe,
            mqtt_publish,
            brenn_subscribe,
            brenn_publish,
            ephemeral_publish,
            ephemeral_subscribe,
            local_publish,
            webhook,
        } = acl;
        mqtt_subscribe.sort_unstable();
        mqtt_publish.sort_unstable();
        brenn_subscribe.sort_unstable();
        brenn_publish.sort_unstable();
        ephemeral_publish.sort_unstable();
        ephemeral_subscribe.sort_unstable();
        local_publish.sort_unstable();
        webhook.sort_unstable();
    }
    for surface in &mut config.surfaces {
        let crate::messaging::config::SurfaceConfigRaw {
            slug: _,
            grants,
            subscribe_acl,
            publish_acl,
            ephemeral_subscribe_acl,
            ephemeral_publish_acl,
            components: _,
            subscriptions: _,
            outputs: _,
            io_ports: _,
            skin: _,
            allowed_users: _,
            publish_burst: _,
            publish_per_sec: _,
        } = surface;
        grants.sort_unstable();
        subscribe_acl.sort_unstable();
        publish_acl.sort_unstable();
        ephemeral_subscribe_acl.sort_unstable();
        ephemeral_publish_acl.sort_unstable();
    }
    for consumer in &mut config.wasm_consumers {
        let crate::messaging::config::WasmConsumerConfigRaw {
            slug: _,
            package: _,
            spec_sha256: _,
            declared_out_ports: _,
            grants,
            store_path: _,
            store_size_limit: _,
            subscriptions: _,
            outputs: _,
            io_ports: _,
            subscribe_acl,
            ephemeral_subscribe_acl,
            local_subscribe_acl,
            publish_acl,
            ephemeral_publish_acl,
            local_publish_acl,
            mqtt_publish_acl,
            mqtt_subscribe_acl,
            webhook_acl,
            config: _,
            activation_burst: _,
            activation_min_period_ms: _,
            mqtt_outputs: _,
            tool_grants: _,
        } = consumer;
        grants.sort_unstable();
        subscribe_acl.sort_unstable();
        ephemeral_subscribe_acl.sort_unstable();
        local_subscribe_acl.sort_unstable();
        publish_acl.sort_unstable();
        ephemeral_publish_acl.sort_unstable();
        local_publish_acl.sort_unstable();
        mqtt_publish_acl.sort_unstable();
        mqtt_subscribe_acl.sort_unstable();
        webhook_acl.sort_unstable();
    }
    for remote in &mut config.remotes {
        let crate::messaging::remote::RemoteConfigRaw {
            slug: _,
            token_file: _,
            grants,
            subscribe_acl,
            ephemeral_subscribe_acl,
            publish_acl,
            ephemeral_publish_acl,
            publish_burst: _,
            publish_per_sec: _,
            max_sessions: _,
            max_subscriptions: _,
        } = remote;
        grants.sort_unstable();
        subscribe_acl.sort_unstable();
        ephemeral_subscribe_acl.sort_unstable();
        publish_acl.sort_unstable();
        ephemeral_publish_acl.sort_unstable();
    }
}

/// Load configuration from a config file.
///
/// If `path` is `Some`, reads that file: a `.brenn` document compiles and lowers
/// to a `BrennConfig`, and no other extension is a config. If `None`, probes the
/// current working directory for `brenn.brenn`. If it does not exist, returns
/// `BrennConfig::default()`.
///
/// # Panics
///
/// Panics if:
/// - `path` is `Some` and the file doesn't exist or fails to compile or lower
/// - `path` is `Some` and its extension is not `brenn`
/// - `path` is `None` and whether the fallback name exists cannot be determined
/// - `path` is `None` and the fallback that exists fails to load
pub fn load_config(path: Option<&Path>, module_roots: &[PathBuf]) -> BrennConfig {
    let cwd = std::env::current_dir().expect("failed to determine current directory");
    load_config_from(path, module_roots, &cwd)
}

/// Load configuration, probing `fallback_dir` for a config when no explicit path
/// is given. Separated from `load_config` for testability (avoids
/// `set_current_dir` in tests, which is process-global and not thread-safe).
///
/// This sits above [`DocumentInputs`]: with no `--config` there may be no
/// document at all, so the struct is built only once the fallback has found one.
pub(crate) fn load_config_from(
    path: Option<&Path>,
    module_roots: &[PathBuf],
    fallback_dir: &Path,
) -> BrennConfig {
    let root = match path {
        Some(p) => p.to_path_buf(),
        None => match fallback_config(fallback_dir) {
            Some(found) => found,
            None => return BrennConfig::default(),
        },
    };
    let inputs = DocumentInputs {
        root,
        module_roots: module_roots.to_vec(),
    };
    // Boot is `check_config` plus the one thing a boot does that a check does
    // not: it dies on the report. One dispatch, so what the check tool accepts
    // and what boots cannot diverge in either direction.
    check_config(&inputs).unwrap_or_else(|report| panic!("{report}"))
}

/// The name a `--config`-less boot answers to, in the fallback directory.
const FALLBACK_NAME: &str = "brenn.brenn";

/// The config file in `fallback_dir`, or nothing where it holds none.
///
/// # Panics
///
/// Panics where the name can neither be confirmed present nor confirmed
/// absent — an unreadable directory, a symlink loop. Read as "absent", the
/// server would boot on defaults with a config sitting right there, and the
/// operator would have no way to tell.
fn fallback_config(fallback_dir: &Path) -> Option<PathBuf> {
    let path = fallback_dir.join(FALLBACK_NAME);
    let exists = path.try_exists().unwrap_or_else(|error| {
        panic!(
            "no --config was given and whether {} exists cannot be determined: {error}",
            path.display(),
        )
    });
    exists.then_some(path)
}

fn unrecognized_extension(path: &Path) -> String {
    format!(
        "config file {}: unrecognized extension — a config is a `.brenn` document",
        path.display(),
    )
}

/// Which stage of the `.brenn` pipeline refused a document, and with what.
///
/// The two stages are different fixes — a document that does not compile is a
/// document, one that does not lower is a configuration — so the report names
/// which it was.
struct DslFailure {
    stage: &'static str,
    diagnostics: Vec<Diagnostic>,
}

impl DslFailure {
    fn render(&self, path: &Path) -> String {
        format!(
            "failed to {} config file {}:\n{}",
            self.stage,
            path.display(),
            brenn_dsl::diag::render_all(&self.diagnostics),
        )
    }
}

/// A `.brenn` document, compiled and lowered, reporting rather than panicking.
///
/// The root is the root module of its document tree, so its own directory is
/// where `use` resolves from; the module roots are where `use @<name>::…`
/// resolves from instead.
fn read_dsl(inputs: &DocumentInputs) -> Result<BrennConfig, DslFailure> {
    let config = brenn_dsl::compile(inputs).map_err(|diagnostics| DslFailure {
        stage: "compile",
        diagnostics,
    })?;
    super::dsl_lower::lower(config).map_err(|diagnostics| DslFailure {
        stage: "lower",
        diagnostics,
    })
}

/// Validate a config file the way boot loads it, reporting instead of dying.
///
/// The one extension dispatch: [`load_config_from`] boots through this, adding
/// only the panic, so what this accepts and what boots accepts are the same set
/// by construction.
///
/// What it deliberately does not run is `validate_and_resolve`. That pass is
/// environment-dependent — it stats container home directories and takes the
/// integration registry and the runtime dir — so a check run on a workstation
/// against a config destined for another host must not fail on the
/// workstation's filesystem. `Ok` therefore means "this file is a config", not
/// "this config will boot on every host".
///
/// It reads no facts beyond its declared inputs: the root path, and the module
/// roots packaged imports resolve against. A module root is an input like the
/// root file is — the caller asserts these are the module bytes to check
/// against — and the boot-time hash binding is what catches a caller who
/// asserted wrong.
pub fn check_config(inputs: &DocumentInputs) -> Result<BrennConfig, String> {
    let path = inputs.root.as_path();
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("brenn") => read_dsl(inputs).map_err(|failure| failure.render(path)),
        // The check tool reports; only boot panics.
        _ => Err(unrecognized_extension(path)),
    }
}
