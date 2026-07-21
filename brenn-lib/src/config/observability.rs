use brenn_surface_proto::LogLevel;
use serde::Deserialize;

/// Top-level `[observability]` config section.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    pub usage: UsageObservabilityConfig,

    /// Durable channel that surface error reports are published onto (by each
    /// surface under its own `surface:<slug>` identity). Full `brenn:` address
    /// (e.g. `"brenn:surface-errors"`). `None` ⇒ no channel; surfaces keep their
    /// reports console-only.
    pub surface_error_channel: Option<String>,

    /// Minimum level a surface publishes to `surface_error_channel`. A conforming
    /// shell publishes reports at this level and above and keeps lower levels
    /// console-only; delivered to the shell in the `Welcome` floor field.
    /// Only meaningful when `surface_error_channel` is set. Serde-typed as a
    /// [`LogLevel`], so an invalid level string fails config parse. Default
    /// `warn`.
    #[serde(default = "default_surface_error_publish_floor")]
    pub surface_error_publish_floor: LogLevel,
}

/// Default [`ObservabilityConfig::surface_error_publish_floor`]: `warn` — the
/// admission floor the interim server-side path enforced, preserved as the
/// default publish floor.
fn default_surface_error_publish_floor() -> LogLevel {
    LogLevel::Warn
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            usage: UsageObservabilityConfig::default(),
            surface_error_channel: None,
            surface_error_publish_floor: default_surface_error_publish_floor(),
        }
    }
}

/// Usage-observability sub-section (`[observability.usage]`).
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UsageObservabilityConfig {
    /// Inactivity gap in minutes that closes a usage session. Default: 30.
    pub session_gap_minutes: u32,
}

impl Default for UsageObservabilityConfig {
    fn default() -> Self {
        Self {
            session_gap_minutes: 30,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surface_error_publish_floor_defaults_to_warn() {
        let cfg: ObservabilityConfig = toml::from_str("").expect("empty section parses");
        assert_eq!(cfg.surface_error_publish_floor, LogLevel::Warn);
    }

    #[test]
    fn surface_error_publish_floor_parses_configured_level() {
        let cfg: ObservabilityConfig =
            toml::from_str("surface_error_publish_floor = \"error\"").expect("valid level parses");
        assert_eq!(cfg.surface_error_publish_floor, LogLevel::Error);
    }

    #[test]
    fn surface_error_publish_floor_rejects_invalid_level() {
        // An unknown level string fails config parse (serde), not silently at boot.
        let err = toml::from_str::<ObservabilityConfig>("surface_error_publish_floor = \"fatal\"");
        assert!(
            err.is_err(),
            "invalid floor level must be rejected at parse"
        );
    }
}
