use serde::Deserialize;

/// Top-level `[surface_description]` config section.
///
/// Roots the surface self-description topology: a boot-published family of
/// retained help/schema/index documents plus the runtime per-surface
/// geometry/status channels, all derived by convention from `prefix`. The
/// topology is always live: every derived channel is published, every surface
/// reports geometry/status. An operator who does not want the telemetry does not
/// subscribe to the channels.
///
/// Values are validated at boot (prefix a well-formed bare-name segment,
/// `status_interval_secs` in range), not at parse time, mirroring the other
/// boot-validated messaging config.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct SurfaceDescriptionConfig {
    /// Bare-name namespace rooting every derived channel (e.g. `"surface"` ⇒
    /// `brenn:surface.index`, `brenn:surface.surface.<slug>.help`, …).
    pub prefix: String,
    /// Heartbeat cadence handed to shells for the status snapshot, seconds.
    /// Boot-validated to `5..=3600`.
    pub status_interval_secs: u32,
}

/// Default namespace rooting the derived channels. Reached whenever the key is
/// omitted, section present or absent (the container is `#[serde(default)]`).
fn default_prefix() -> String {
    "surface".to_string()
}

/// Default heartbeat cadence: a heartbeat, not a meter (durable-channel
/// doctrine). Used both as the field default when the section is present but the
/// key omitted, and via the manual `Default` impl when the section is absent.
fn default_status_interval_secs() -> u32 {
    60
}

impl Default for SurfaceDescriptionConfig {
    fn default() -> Self {
        Self {
            prefix: default_prefix(),
            status_interval_secs: default_status_interval_secs(),
        }
    }
}
