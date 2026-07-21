use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Top-level repo declaration from `[[repo]]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepoDeclRaw {
    /// URL-safe identifier (`[a-z0-9][a-z0-9-]*`). Globally unique across repos.
    pub slug: String,
    /// Git remote URL for cloning.
    pub remote: String,
    /// Default auto-pull for apps that mount this repo. Default: true.
    #[serde(default = "default_true")]
    pub auto_pull: bool,
}

/// Per-app mount from `[[app.mount]]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountConfigRaw {
    /// Slug of a `[[repo]]` entry.
    pub repo: String,
    /// Access level for this mount. Default: read-write.
    #[serde(default)]
    pub access: AccessLevel,
    /// If true, this repo is the app's working directory. At most one per app.
    #[serde(default)]
    pub working_dir: bool,
    /// Override the repo's `auto_pull` default for this mount.
    pub auto_pull: Option<bool>,
    /// Designate this mount as the *primary* for its clone. Only one mount
    /// per clone (across all apps) may be primary; consumers of the primary
    /// mount are the ones notified when a pull produces a conflict.
    /// See `docs/designs/repo-sync.md`.
    ///
    /// Required (exactly one) when the clone has >1 RW mount; optional
    /// (implicit) when the clone has exactly one RW mount; forbidden when
    /// the clone is RO-only. Resolved in `validate_and_resolve`.
    #[serde(default)]
    pub primary: bool,
}

/// Access level for a repo mount.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AccessLevel {
    #[default]
    ReadWrite,
    ReadOnly,
}

/// Resolved mount (on AppConfig), produced from `[[repo]]` + `[[app.mount]]`.
#[derive(Debug, Clone)]
pub struct ResolvedMount {
    /// Repo slug.
    pub slug: String,
    /// Host-side path to the repo root (`<repo_dir>/<slug>`).
    pub host_path: PathBuf,
    /// Container-side path (`<container_home>/repos/<slug>`). None for bare apps.
    pub container_path: Option<PathBuf>,
    /// Access level for this mount.
    pub access: AccessLevel,
    /// Whether to auto-pull on new conversation start.
    pub auto_pull: bool,
    /// Whether this mount is the app's working directory.
    pub is_working_dir: bool,
    /// True when this mount is the primary owner for its clone. Consumers of
    /// the primary mount receive `repo_sync:conflict` notifications; non-primary
    /// consumers do not. Resolved by `validate_and_resolve` per the rules
    /// documented on `MountConfigRaw::primary`.
    pub primary: bool,
}

impl ResolvedMount {
    /// Returns the path CC (or any in-container agent tool) sees for this
    /// mount: `container_path` for containerized apps, `host_path` for bare.
    /// Callers include `git -C` commands, `--add-dir` flags for CC, and
    /// agent-facing manifests like the noop MCP virtual-tools file.
    pub fn visible_path(&self, containerized: bool) -> &Path {
        if containerized {
            self.container_path
                .as_deref()
                .expect("container_path required for containerized apps")
        } else {
            &self.host_path
        }
    }
}

/// Repo-sync feature config from `[repo_sync]`. See `docs/designs/repo-sync.md`.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct RepoSyncConfig {
    /// Poll interval in seconds. Applied uniformly to every unique remote.
    /// Default: 300 (5 min).
    pub poll_interval_secs: u64,
    /// Drain-time staleness cap in days. Pending `repo_sync:*` events for
    /// conversations whose `updated_at` is older than this are marked
    /// delivered *without* injection. Other event sources are unaffected.
    /// Default: 7.
    pub stale_conversation_days: u64,
}

impl Default for RepoSyncConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 300,
            stale_conversation_days: 7,
        }
    }
}

fn default_true() -> bool {
    true
}
