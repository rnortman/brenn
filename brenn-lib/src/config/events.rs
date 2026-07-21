use serde::Deserialize;

/// Retention settings for the `events` table.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct EventsConfig {
    /// Retention window in days for delivered events.
    /// Delivered rows older than this are deleted by the hourly cleanup loop.
    /// Undelivered rows are never deleted here regardless of age (the
    /// repo_sync staleness pass handles those for `repo_sync:*` only).
    /// Default: 7.
    pub delivered_retention_days: u64,
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            delivered_retention_days: 7,
        }
    }
}
