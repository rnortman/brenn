use std::collections::HashMap;
use std::path::Path;

use brenn_dsl::diag::Diagnostic;
use serde::Deserialize;

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
/// Use `brenn.dev.toml` for local development.
#[derive(Debug, Deserialize, Default, PartialEq)]
#[serde(default, deny_unknown_fields)]
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
    #[serde(default, rename = "repo")]
    pub repos: Vec<RepoDeclRaw>,
    /// Podman container definitions. Apps reference these by name.
    #[serde(default)]
    pub container: HashMap<String, ContainerConfig>,
    /// Global integration defaults. Apps enable integrations by name and can
    /// override specific config keys per-app via `integration_config`.
    #[serde(default)]
    pub integrations: HashMap<String, toml::Value>,
    /// Per-app configurations. At least one must be defined.
    /// Deserialized as a Vec from TOML `[[app]]` array, then validated and
    /// converted to a HashMap keyed by slug via `validate_and_resolve`.
    #[serde(rename = "app")]
    pub apps: Vec<AppConfigRaw>,
    /// Top-level `[[channel]]` declarations — one table for every pub/sub
    /// scheme, durable or not. The address's scheme picks the channel's
    /// capabilities. See `crate::messaging::config::ChannelConfigRaw`.
    #[serde(default, rename = "channel")]
    pub channels: Vec<crate::messaging::config::ChannelConfigRaw>,
    /// Global messaging defaults (`[messaging]`). Defaults to
    /// `MessagingGlobalConfig::default()` when absent.
    #[serde(default)]
    pub messaging: crate::messaging::config::MessagingGlobalConfig,
    /// Observability settings (`[observability]`). Defaults to
    /// `ObservabilityConfig::default()` when absent.
    #[serde(default)]
    pub observability: ObservabilityConfig,
    /// Surface self-description settings (`[surface_description]`). Defaults to
    /// `SurfaceDescriptionConfig::default()` (no prefix ⇒ feature off) when absent.
    #[serde(default)]
    pub surface_description: SurfaceDescriptionConfig,
    /// Chat-over-pubsub settings (`[llm_chat]`). Defaults to
    /// `LlmChatConfig::default()` when absent.
    #[serde(default)]
    pub llm_chat: super::llm_chat::LlmChatConfig,
    /// Global VAPID keypair and subject for PWA push notifications.
    /// `[pwa_push]` block. Defaults to `PwaPushGlobalConfig::default()`
    /// (all-None) when absent — safe zero values; no keypair loaded unless
    /// an app has `pwa_push.enabled = true`.
    #[serde(default)]
    pub pwa_push: crate::pwa_push::config::PwaPushGlobalConfig,
    /// Automation engine config (`[automation]`). Defaults to
    /// `AutomationGlobalConfig::default()` when absent.
    #[serde(default)]
    pub automation: crate::config::AutomationGlobalConfig,
    /// Top-level `[[mqtt_client]]` declarations. Each entry defines an MQTT
    /// client (the app-independent connection to a remote MQTT broker/server).
    /// Apps address it for egress via the `mqtt_publish` ACL naming the client
    /// slug, and subscribe to it via `[[app.mqtt_subscription]]` naming
    /// `mqtt:<client>:<topic>` (ingress).
    #[serde(default, rename = "mqtt_client")]
    pub mqtt_clients: Vec<crate::mqtt::config::MqttClientConfigRaw>,
    /// Top-level `[[webhook_endpoint]]` declarations. Each entry defines an
    /// inbound HTTP webhook endpoint. Apps bind to endpoints via
    /// `[[app.webhook_subscription]]`.
    #[serde(default, rename = "webhook_endpoint")]
    pub webhook_endpoints: Vec<crate::webhook::config::WebhookEndpointConfigRaw>,
    /// Events table retention settings (`[events]`). Defaults to
    /// `EventsConfig::default()` when absent (7-day delivered-row retention).
    #[serde(default)]
    pub events: EventsConfig,
    /// TOML `[[wasm_consumer]]` blocks; see [`crate::messaging::config::WasmConsumerConfigRaw`]
    /// for per-entry fields.
    #[serde(default, rename = "wasm_consumer")]
    pub wasm_consumers: Vec<crate::messaging::config::WasmConsumerConfigRaw>,
    /// Top-level `[[surface]]` blocks; see
    /// [`crate::messaging::config::SurfaceConfigRaw`] for per-entry fields.
    #[serde(default, rename = "surface")]
    pub surfaces: Vec<crate::messaging::config::SurfaceConfigRaw>,
    /// Top-level `[[remote]]` blocks — authenticated native-daemon attachers on
    /// the same attach transport the browser surfaces use; see
    /// [`crate::messaging::remote::RemoteConfigRaw`] for per-entry fields.
    #[serde(default, rename = "remote")]
    pub remotes: Vec<crate::messaging::remote::RemoteConfigRaw>,
    /// Top-level `[[connection]]` blocks — auto channels declared by the ports
    /// they wire together rather than by a `[[channel]]` block. See
    /// [`crate::messaging::config::ConnectionConfigRaw`].
    #[serde(default, rename = "connection")]
    pub connections: Vec<crate::messaging::config::ConnectionConfigRaw>,
    /// Global WASM-host policy (`[wasm]` block). Controls defaults such as
    /// store size limits. Omitting the block is equivalent to `WasmConfig::default()`.
    #[serde(default)]
    pub wasm: WasmConfig,
    /// Bridge-wedge watchdog settings (`[watchdog]`). Defaults to
    /// `WatchdogConfig::default()` when absent (30 s sweep, 60 s wedge grace).
    #[serde(default)]
    pub watchdog: WatchdogConfig,
}

/// Load configuration from a config file.
///
/// If `path` is `Some`, reads that file, dispatching on its extension: `.toml`
/// parses as TOML, `.brenn` compiles as a DSL document and lowers to the same
/// `BrennConfig`. If `None`, looks for `brenn.toml` in the current working
/// directory. If neither exists, returns `BrennConfig::default()`.
///
/// # Panics
///
/// Panics if:
/// - `path` is `Some` and the file doesn't exist or fails to parse
/// - `path` is `Some` and its extension is neither `toml` nor `brenn`
/// - `path` is `None` and `brenn.toml` exists in cwd but fails to parse
/// - The file contains unrecognized keys or invalid values
pub fn load_config(path: Option<&Path>) -> BrennConfig {
    let cwd = std::env::current_dir().expect("failed to determine current directory");
    load_config_from(path, &cwd)
}

/// Load configuration, using `fallback_dir` to find `brenn.toml` when no explicit
/// path is given. Separated from `load_config` for testability (avoids
/// `set_current_dir` in tests, which is process-global and not thread-safe).
///
/// The extension dispatch applies to an explicit path only; the fallback names
/// `brenn.toml` and reads TOML.
pub(crate) fn load_config_from(path: Option<&Path>, fallback_dir: &Path) -> BrennConfig {
    match path {
        // Boot is `check_config` plus the two things a boot does that a check
        // does not: it prints the warnings and it dies on the report. One
        // dispatch, so what the check tool accepts and what boots cannot
        // diverge in either direction.
        //
        // Warnings go to stderr rather than through the log because
        // observability is initialized after the config is loaded, which is the
        // same reason the failure here is a raw panic.
        Some(p) => {
            let (warnings, config) = check_config(p);
            for warning in &warnings {
                eprintln!("warning: {warning}");
            }
            config.unwrap_or_else(|report| panic!("{report}"))
        }
        None => {
            let default_path = fallback_dir.join("brenn.toml");
            if default_path.exists() {
                read_toml(&default_path).unwrap_or_else(|report| panic!("{report}"))
            } else {
                BrennConfig::default()
            }
        }
    }
}

/// What to say about a path that is neither of the two forms a config takes.
fn unrecognized_extension(path: &Path) -> String {
    format!(
        "config file {}: unrecognized extension — a config is either a `.toml` file or a \
         `.brenn` document",
        path.display(),
    )
}

/// A TOML config file, read and parsed, or what to report about it.
fn read_toml(path: &Path) -> Result<BrennConfig, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
    toml::from_str(&contents)
        .map_err(|e| format!("failed to parse config file {}: {e}", path.display()))
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
/// The file is the root module of its document tree, so its own directory is
/// where `use` resolves from.
///
/// Warnings ride beside the result rather than inside its `Ok` arm: a document
/// that compiles with warnings and then fails lowering has both to report, and
/// a caller that could only see warnings on success would drop them exactly
/// when they are most likely to matter.
fn read_dsl(path: &Path) -> (Vec<Diagnostic>, Result<BrennConfig, DslFailure>) {
    let output = match brenn_dsl::compile(path) {
        Ok(output) => output,
        Err(diagnostics) => {
            return (
                Vec::new(),
                Err(DslFailure {
                    stage: "compile",
                    diagnostics,
                }),
            );
        }
    };
    let config = super::dsl_lower::lower(output.config).map_err(|diagnostics| DslFailure {
        stage: "lower",
        diagnostics,
    });
    (output.warnings, config)
}

/// Validate a config file the way boot loads it, reporting instead of dying.
///
/// The one extension dispatch: [`load_config_from`] boots through this, adding
/// only the stderr print and the panic, so what this accepts and what boots
/// accepts are the same set by construction. The warnings ride beside the
/// result for the reason [`read_dsl`] states: a failing check still reports the
/// document's warnings.
///
/// What it deliberately does not run is `validate_and_resolve`. That pass is
/// environment-dependent — it stats container home directories and takes the
/// integration registry and the runtime dir — so a check run on a workstation
/// against a config destined for another host must not fail on the
/// workstation's filesystem. `Ok` therefore means "this file is a config", not
/// "this config will boot on every host".
pub fn check_config(path: &Path) -> (Vec<String>, Result<BrennConfig, String>) {
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("toml") => (Vec::new(), read_toml(path)),
        Some("brenn") => {
            let (warnings, config) = read_dsl(path);
            (
                warnings.iter().map(Diagnostic::render).collect(),
                config.map_err(|failure| failure.render(path)),
            )
        }
        // The check tool reports; only boot panics.
        _ => (Vec::new(), Err(unrecognized_extension(path))),
    }
}
