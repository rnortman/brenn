use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

use crate::integration::Integration;

use super::attachment::{AttachmentTarget, AttachmentTargetRaw};
use super::container::ContainerSpawnConfig;
use super::frontmatter::FrontmatterRenderConfig;
use super::hooks::{PostPullHooksConfig, StartHooksConfig, StartupHooksConfig};
use super::mcp::McpServerConfig;
use super::path_mapper::PathMapper;
use super::repo::{MountConfigRaw, ResolvedMount};

/// Raw per-app config as deserialized from TOML `[[app]]`.
/// Validated and resolved into `AppConfig` by `validate_and_resolve`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfigRaw {
    /// URL-safe identifier (e.g. "pfin"). Must match `[a-z0-9][a-z0-9-]*`.
    pub slug: String,
    /// Human-readable display name. Defaults to slug if omitted.
    pub name: Option<String>,
    /// Short description shown on the app selector landing page.
    pub description: Option<String>,
    /// Icon shown on the app selector card (emoji or short string).
    pub icon: Option<String>,
    /// CC subprocess working directory / repo root (host path).
    /// Optional when a mount has `working_dir = true`.
    pub working_dir: Option<PathBuf>,
    /// CC model override. If omitted, uses `claude_defaults.model`.
    pub model: Option<String>,
    /// Enforce at most one active CC session for this app (globally).
    #[serde(default)]
    pub single_instance: bool,
    /// Singleton mode: one conversation per user, no conversation list.
    /// Mutually exclusive with `multiuser`.
    #[serde(default)]
    pub singleton: bool,
    /// Persistent mode: CC survives browser tab closes and shuts down
    /// after an idle timeout instead of immediately.
    #[serde(default)]
    pub persistent: bool,
    /// Idle timeout in seconds before killing CC when no subscribers are
    /// connected. Only meaningful when `persistent = true`. Default: 1800 (30 min).
    pub idle_timeout_secs: Option<u64>,
    /// Context usage percentage for the LLM nudge reminder.
    /// Only meaningful for `singleton` apps. Default: 60.
    pub compact_reminder_pct: Option<u8>,
    /// Context usage percentage to trigger soft compaction (idle + high context).
    /// Required for `singleton` apps. Default: 75.
    pub compact_soft_pct: Option<u8>,
    /// Context usage percentage for the UI red indicator.
    /// Only meaningful for `singleton` apps. Default: 80.
    pub compact_red_pct: Option<u8>,
    /// Context usage percentage to trigger hard (immediate) compaction.
    /// Only meaningful for `singleton` apps. Default: 95.
    pub compact_hard_pct: Option<u8>,
    /// Absolute context-token threshold for the LLM nudge reminder. Optional
    /// alternative to `compact_reminder_pct`; whichever fires first wins.
    /// No cross-validation against the percentage threshold — the validator
    /// does not know the model's `max_tokens`, so the deployer is responsible
    /// for setting sensible values for both knobs.
    pub compact_reminder_tokens: Option<u64>,
    /// Absolute context-token threshold for soft compaction (idle + high
    /// context). Optional alternative to `compact_soft_pct`; whichever fires
    /// first wins.
    pub compact_soft_tokens: Option<u64>,
    /// Absolute context-token threshold for the UI red indicator. Optional
    /// alternative to `compact_red_pct`; whichever fires first wins.
    pub compact_red_tokens: Option<u64>,
    /// Absolute context-token threshold for hard (immediate) compaction.
    /// Optional alternative to `compact_hard_pct`; whichever fires first
    /// wins.
    pub compact_hard_tokens: Option<u64>,
    /// Minimum idle seconds before soft compaction triggers.
    /// Default: 270 (4m30s) — kept just under the 5-minute prompt-cache TTL
    /// so soft compaction fires before the cache window closes.
    pub compact_idle_secs: Option<u64>,
    /// Idle hook timer in seconds: idle hooks fire when both CC and the UI
    /// have been quiet for at least this long. `0` disables idle hooks.
    /// Default: 2700 (45 min).
    pub idle_hook_secs: Option<u64>,
    /// Usernames with access. Empty = all users have access.
    #[serde(default)]
    pub allowed_users: Vec<String>,
    /// CC built-in tools to disable (blacklist).
    #[serde(default)]
    pub disabled_tools: Vec<String>,
    /// Additional MCP servers for this app (merged with the base Brenn MCP server).
    #[serde(default)]
    pub mcp_servers: HashMap<String, McpServerConfig>,
    /// Enable multiuser mode: shared conversations, cross-user participation.
    #[serde(default)]
    pub multiuser: bool,
    /// Prepend `[username ...]` to messages sent to CC. Defaults to `multiuser` value.
    pub prefix_username: Option<bool>,
    /// Prepend timestamp (with timezone) to messages sent to CC. Defaults to `multiuser` value.
    pub prefix_timestamp: Option<bool>,
    /// Prepend device slug to messages sent to CC. Defaults to `true`.
    pub prefix_device: Option<bool>,
    /// Name of a `[container.<name>]` definition. If absent, CC runs as a bare process.
    pub container: Option<String>,
    /// CC's working directory inside the container. Optional — derived from
    /// convention when working dir comes from a repo mount.
    pub container_working_dir: Option<PathBuf>,
    /// Custom scripts to run before CC spawns on new conversations.
    pub start_hooks: Option<StartHooksConfig>,
    /// Hook scripts that run after a successful repo pull advances HEAD.
    pub post_pull_hooks: Option<PostPullHooksConfig>,
    /// Hook scripts that run once at server startup after all startup pulls succeed.
    pub startup_hooks: Option<StartupHooksConfig>,
    /// Extra CLI arguments passed verbatim to the `claude` command.
    #[serde(default)]
    pub cc_extra_args: Vec<String>,
    /// Static auto-approval rules (pattern-based). Checked before DB rules.
    #[serde(default)]
    pub approval_rules: Vec<crate::approval_rules::ApprovalRuleConfig>,
    /// App-defined attachment targets (e.g. "Import bank export").
    #[serde(default)]
    pub attachment_targets: Vec<AttachmentTargetRaw>,
    /// Integrations to enable for this app (by name, using global defaults).
    #[serde(default)]
    pub integrations: Vec<String>,
    /// Per-app integration config overrides. Keys are integration names;
    /// values override/extend the global `[integrations.<name>]` section.
    /// Listing a name here implicitly enables it (no need to also list in
    /// `integrations`).
    #[serde(default)]
    pub integration_config: HashMap<String, toml::Value>,
    /// Repo mounts for this app. Each mount references a top-level `[[repo]]`
    /// by slug, with optional access level and working-dir designation.
    #[serde(default, rename = "mount")]
    pub mounts: Vec<MountConfigRaw>,
    /// Extra bind mounts injected only into this app's container, in
    /// addition to the container-level `extra_mounts`. Same
    /// `host:container[:opts]` format. Only valid for containerized apps;
    /// validation panics if a bare app sets it. Same path-translation
    /// caveat as the container-level field: host paths inside these
    /// mounts are opaque to brenn's `PathMapper`.
    #[serde(default)]
    pub extra_mounts: Vec<String>,
    /// Maximum number of messages to replay at full fidelity on connect.
    /// History beyond this limit is available via simplified backward pagination.
    /// Default: 2000.
    pub history_replay_limit: Option<usize>,
    /// Optional per-app rendering rules for YAML frontmatter blocks at
    /// the top of markdown files. See `FrontmatterRenderConfig`.
    #[serde(default)]
    pub frontmatter: FrontmatterRenderConfig,
    /// Messaging participation. Absent → app cannot publish or subscribe.
    /// See `crate::messaging::config::MessagingConfigRaw`.
    pub messaging: Option<crate::messaging::config::MessagingConfigRaw>,
    /// PWA push participation. Absent or `enabled = false` → app cannot
    /// publish push notifications and clients must not register subscriptions.
    /// See `crate::pwa_push::config::AppPwaPushBlock`.
    pub pwa_push: Option<crate::pwa_push::config::AppPwaPushBlock>,
    /// Webhook subscriptions for this app. Each entry references a
    /// `[[webhook_endpoint]]` by slug.
    /// See `crate::webhook::config::AppWebhookSubscriptionRaw`.
    #[serde(default, rename = "webhook_subscription")]
    pub webhook_subscriptions: Vec<crate::webhook::config::AppWebhookSubscriptionRaw>,
    /// MQTT ingress subscriptions for this app. Each entry names a channel by
    /// its full address `mqtt:<client>:<topic>` (client mandatory).
    /// See `crate::mqtt::config::AppMqttIngressSubscriptionRaw`.
    #[serde(default, rename = "mqtt_subscription")]
    pub mqtt_subscriptions: Vec<crate::mqtt::config::AppMqttIngressSubscriptionRaw>,
    /// Layer-1 capability grants for this app (deny-by-default). Absent ⇒ no
    /// grants. Resolved into the app's `AppPolicy` by the
    /// `resolve_access_policies` phase in `config/resolve.rs` (access-control
    /// design §2.5.1/§2.5.2).
    ///
    /// Migration-forcing (§2.5.1 / §8 decision-2): the legacy implicit-capability
    /// *authorization booleans* (`[app.messaging].enabled`, `[app.pwa_push].enabled`)
    /// were removed. Because the raw config structs carry
    /// `#[serde(deny_unknown_fields)]`, a stale config that still sets one of those
    /// booleans now fails to parse with a precise error, forcing the operator to
    /// migrate to this explicit `grants` surface (tests in `config/tests/app_parse.rs`
    /// and `messaging/config.rs`/`pwa_push/config.rs`). `grants`/`acl` themselves
    /// remain `#[serde(default)]` (absent ⇒ deny-everything).
    #[serde(default)]
    pub grants: Vec<crate::access::AppCapability>,
    /// Layer-2 ACL block (`[app.acl.*]`). Absent ⇒ all matcher lists empty.
    /// Resolved into the app's `AppPolicy` by the `resolve_access_policies` phase
    /// in `config/resolve.rs` (access-control design §2.5.1/§2.5.2).
    #[serde(default)]
    pub acl: crate::access::raw::AppAclRaw,
    /// Tool grants for this app (`[[app.tool_grant]]`). Each authorizes
    /// addressing a registry tool, optionally narrowed by an `acl` and throttled
    /// by `rate_limit`. Absent ⇒ no explicit grants (an app with git mounts still
    /// earns an implicit `git-repo-pull` grant during resolution). Resolved into
    /// the app's `AppPolicy::tool_grants` by the `resolve_access_policies` phase.
    #[serde(default, rename = "tool_grant")]
    pub tool_grants: Vec<crate::tools::config::ToolGrantRaw>,
}

/// Resolved per-app configuration with defaults applied.
#[derive(Clone)]
pub struct AppConfig {
    pub slug: String,
    pub name: String,
    /// Short description shown on the app selector landing page.
    pub description: String,
    /// Icon shown on the app selector card (emoji or short string).
    pub icon: String,
    /// Host-side working directory. Attachments are stored here.
    pub working_dir: PathBuf,
    pub model: String,
    pub single_instance: bool,
    /// Singleton mode: one conversation per user, no conversation list.
    pub singleton: bool,
    /// Persistent mode: CC survives browser tab closes and shuts down
    /// after an idle timeout instead of immediately.
    pub persistent: bool,
    /// Idle timeout for persistent apps. `None` when `persistent` is false.
    /// Default: 30 minutes when `persistent` is true and no override given.
    pub idle_timeout: Option<std::time::Duration>,
    /// Compaction config for singleton apps. `None` when compaction is not configured.
    pub compaction: Option<CompactionConfig>,
    /// Idle hook delay in seconds. `0` = disabled (no idle hooks fire).
    /// See `IdleHook` in `brenn/src/idle_hooks.rs` for the full lifecycle.
    pub idle_hook_secs: u64,
    pub allowed_users: Vec<String>,
    pub disabled_tools: Vec<String>,
    pub mcp_servers: HashMap<String, McpServerConfig>,
    /// Multiuser mode: conversations default to shared, cross-user participation allowed.
    pub multiuser: bool,
    /// Prepend `[username ...]` to messages sent to CC.
    pub prefix_username: bool,
    /// Prepend timestamp (with timezone) to messages sent to CC.
    pub prefix_timestamp: bool,
    /// Prepend device slug to messages sent to CC. Default: true.
    pub prefix_device: bool,
    /// Path mapper for translating between host and CC-visible paths.
    pub path_mapper: PathMapper,
    /// Container spawn config. None for bare-process apps.
    pub container_spawn: Option<ContainerSpawnConfig>,
    /// Start hooks to run before CC spawns on new conversations.
    pub start_hooks: StartHooksConfig,
    /// Hook scripts that run after a successful repo pull advances HEAD.
    pub post_pull_hooks: PostPullHooksConfig,
    /// Hook scripts that run once at server startup after all startup pulls succeed.
    pub startup_hooks: StartupHooksConfig,
    /// Extra CLI arguments passed verbatim to the `claude` command.
    pub cc_extra_args: Vec<String>,
    /// Static auto-approval rules from the TOML config.
    pub approval_rules: Vec<crate::approval_rules::ApprovalRuleConfig>,
    /// App-defined attachment targets.
    pub attachment_targets: Vec<AttachmentTarget>,
    /// Enabled integrations for this app, keyed by integration name.
    pub integrations: HashMap<String, Arc<dyn Integration>>,
    /// Resolved repo mounts for this app, for auto-pull, LLM tools, and
    /// container bind mount generation.
    pub mounts: Vec<ResolvedMount>,
    /// Maximum number of messages to replay at full fidelity on connect.
    /// History beyond this limit is available via simplified backward pagination.
    pub history_replay_limit: usize,
    /// Per-app rendering rules for YAML frontmatter blocks at the top
    /// of markdown files (DisplayFile / `/file/` route). See
    /// `FrontmatterRenderConfig`.
    pub frontmatter: FrontmatterRenderConfig,
    /// Host-side per-app runtime state directory.
    ///
    /// Writes here are per-app and, for containerized apps, automatically
    /// visible inside the container via the existing `home_dir → container_home`
    /// mount — no new bind mounts required.
    ///
    /// **Containerized apps**: `<container_spawn.home_dir>/.config/brenn/<slug>/`
    /// (the CC-visible path resolves via `path_mapper.to_container`).
    ///
    /// **Bare apps**: `$XDG_RUNTIME_DIR/brenn/<slug>/` when `XDG_RUNTIME_DIR` is
    /// set (per-uid, pruned at logout by systemd); otherwise `/tmp/brenn/<slug>/`.
    ///
    /// **Invariant: must not be trusted to be empty.** Across restarts (bare: same
    /// uid restart reuses the dir; containerized: home_dir persists) a prior
    /// process may have left files here. All writers must overwrite unconditionally
    /// or namespace their filenames; no writer may assume emptiness.
    ///
    /// Created unconditionally at config-resolve time; panics on failure (Brenn
    /// robustness principle: startup FS failure is a config/permission bug, not
    /// transient).
    pub state_dir: PathBuf,
    /// Resolved per-app messaging config. `None` when the app has no
    /// `[app.messaging]` section. See
    /// `crate::messaging::config::ResolvedMessagingConfig`.
    pub messaging: Option<crate::messaging::config::ResolvedMessagingConfig>,
    /// Resolved global `[messaging].default_send_budget`. Stamped on
    /// every `AppConfig` regardless of whether the app participates in
    /// messaging — needed by `messaging_send_budget()` so apps without
    /// a `[app.messaging]` block still see the operator's configured
    /// default. See design §7.7.
    pub messaging_default_send_budget: u32,
    /// Per-app pwa_push config block. `None` when the app has no
    /// `[app.pwa_push]` section. See `crate::pwa_push::config::AppPwaPushBlock`.
    pub pwa_push: Option<crate::pwa_push::config::AppPwaPushBlock>,
    /// Resolved webhook subscriptions for this app. Empty vec when the app
    /// declares no `[[app.webhook_subscription]]` blocks.
    pub webhook_subscriptions: Vec<crate::webhook::config::ResolvedWebhookSubscription>,
    /// Resolved MQTT ingress subscriptions for this app. Empty vec when the app
    /// declares no `[[app.mqtt_subscription]]` blocks.
    pub mqtt_subscriptions: Vec<crate::mqtt::config::ResolvedMqttIngressSubscription>,
    /// Resolved access-control policy (grants + ACLs) for this app. Built from
    /// the operator's explicit `grants`/`[app.acl.*]` config. `Default` (empty,
    /// deny-everything) until populated by the access-policy resolution phase.
    /// See `crate::access::AppPolicy`.
    pub policy: crate::access::AppPolicy,
}

impl AppConfig {
    /// Host-side path to the virtual tools JSON consumed by noop_mcp.
    /// Callers that need the CC-visible path for a containerized app must run
    /// the returned path through `self.path_mapper.to_container`.
    pub fn virtual_tools_path(&self) -> PathBuf {
        self.state_dir.join("virtual-tools.json")
    }

    /// Check if a username has access to this app.
    /// Empty `allowed_users` means all users have access.
    pub fn user_has_access(&self, username: &str) -> bool {
        self.allowed_users.is_empty() || self.allowed_users.iter().any(|u| u == username)
    }

    /// Whether messaging is enabled for this app.
    ///
    /// Returns `true` if the app is authorized to participate in messaging —
    /// i.e. its resolved policy grants `MessagingPublish` **or**
    /// `MessagingSubscribe`. The `[app.messaging]` block is retained only for
    /// delivery settings (the per-app `send_budget`, read via
    /// `messaging_send_budget()`); it does not grant authorization. Gates
    /// `messaging::publish` layer-2 denial and the publisher identity-uniqueness
    /// check in `Messenger::new`. (It does **not** gate `BrennSend` tool
    /// visibility — see the closing paragraph.)
    ///
    /// **Deliberately a participation flag, not a publish gate.** The `OR`
    /// reproduces the old single-boolean `[app.messaging].enabled` semantics,
    /// which this Phase-0 re-expression is required to keep
    /// authorization-equivalent (design §2.5.1/§2.7). The publish/subscribe
    /// split (Phase 2) gates the publish *enforcement* path on
    /// `MessagingPublish` directly — `publish/mod.rs` (Seam A), the automation
    /// fire-time re-check (Seam B), and the `AutomationEngine::create` / `edit`
    /// grant pre-checks — so a `messaging_subscribe`-only
    /// app can no longer publish, fire, or author SendMessage jobs. This `OR`
    /// is retained only for participation-wide concerns: the identity-uniqueness
    /// assertion in `Messenger::new`, the `resolve_sender` read/management path,
    /// and the subsystem-boot gate.
    ///
    /// `BrennSend` tool *visibility* (Phase 4, design §2.1) gates on the
    /// `MessagingPublish` grant directly (`integration.rs`,
    /// `messaging_virtual_tools`), **not** on this `OR` — a subscribe-only app
    /// is no longer offered the publish tool. Both halves of the
    /// publish/subscribe split (enforcement, Phase 2; visibility, Phase 4) are
    /// now done; this method is no longer on either side of that split.
    pub fn messaging_enabled(&self) -> bool {
        self.policy
            .has_grant(crate::access::AppCapability::MessagingPublish)
            || self
                .policy
                .has_grant(crate::access::AppCapability::MessagingSubscribe)
    }

    /// Whether PWA push is enabled for this app.
    ///
    /// Returns `true` if the app's resolved policy grants `PwaPush`. `PwaPush`
    /// is scope-less (a pure grant, no ACL), so the grant alone is the gate.
    /// The `[app.pwa_push]` block is retained only for delivery settings
    /// (e.g. `default_title`); it does not grant authorization. Gates WS
    /// subscription messages, `PushSend` / `PushListTargets` tool execution,
    /// and `MessageListChannels` pwa_push enumeration.
    pub fn pwa_push_enabled(&self) -> bool {
        self.policy.has_grant(crate::access::AppCapability::PwaPush)
    }

    /// Resolved messaging send budget for this app. Per design §7.7:
    /// per-app override (`[app.messaging.send_budget]`) → global default
    /// (`[messaging].default_send_budget`) → 100.
    ///
    /// Apps with no `[app.messaging]` block return the global default
    /// unchanged. The reset path uses this to avoid silently ignoring
    /// the configured global default when an operator sets it.
    pub fn messaging_send_budget(&self) -> u32 {
        self.messaging
            .as_ref()
            .map(|m| m.send_budget)
            .unwrap_or(self.messaging_default_send_budget)
    }
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppConfig")
            .field("slug", &self.slug)
            .field("name", &self.name)
            .field("singleton", &self.singleton)
            .field("persistent", &self.persistent)
            .field(
                "integrations",
                &self.integrations.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Resolved compaction configuration for singleton apps.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Context usage percentage for the LLM nudge reminder ("consider compacting").
    pub reminder_pct: u8,
    /// Context usage percentage to trigger soft compaction (idle + high context).
    pub soft_pct: u8,
    /// Context usage percentage for the UI red indicator (user urgency signal).
    pub red_pct: u8,
    /// Context usage percentage to trigger hard (immediate) compaction.
    pub hard_pct: u8,
    /// Absolute reminder threshold in tokens. `None` = percentage-only.
    pub reminder_tokens: Option<u64>,
    /// Absolute soft threshold in tokens. `None` = percentage-only.
    pub soft_tokens: Option<u64>,
    /// Absolute red threshold in tokens. `None` = percentage-only.
    pub red_tokens: Option<u64>,
    /// Absolute hard threshold in tokens. `None` = percentage-only.
    pub hard_tokens: Option<u64>,
    /// Minimum idle duration before soft compaction triggers.
    pub idle_duration: std::time::Duration,
}

/// Test-only Default for `AppConfigRaw`. Provides empty/false/None values for all
/// fields. Tests that need a specific slug or other non-empty fields should override
/// via struct update syntax: `AppConfigRaw { slug: "myapp".into(), ..Default::default() }`.
///
/// Manual impl rather than `#[derive(Default)]` because `slug: String` derives as `""`
/// which is not a valid slug — making this `#[cfg(test)]-only` prevents production code
/// from accidentally constructing an invalid raw config via `Default::default()`.
#[cfg(test)]
impl Default for AppConfigRaw {
    fn default() -> Self {
        Self {
            slug: String::new(),
            name: None,
            description: None,
            icon: None,
            working_dir: None,
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: std::collections::HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            integration_config: std::collections::HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: super::frontmatter::FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }
    }
}

impl CompactionConfig {
    pub(super) const DEFAULT_REMINDER_PCT: u8 = 60;
    pub(super) const DEFAULT_SOFT_PCT: u8 = 75;
    pub(super) const DEFAULT_RED_PCT: u8 = 80;
    pub(super) const DEFAULT_HARD_PCT: u8 = 95;
    pub(super) const DEFAULT_IDLE_SECS: u64 = 270;
}
