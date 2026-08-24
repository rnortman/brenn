use brenn_surface_schema::LogLevel;

/// Top-level `[observability]` config section.
#[derive(Debug, PartialEq)]
pub struct ObservabilityConfig {
    pub usage: UsageObservabilityConfig,

    /// Durable channel that surface error reports are published onto (by each
    /// surface under its own `surface:<slug>` identity). Full `brenn:` address
    /// (e.g. `"brenn:surface-errors"`). `None` ⇒ no channel; surfaces keep their
    /// reports console-only.
    pub surface_error_channel: Option<String>,

    /// Minimum level a surface publishes to `surface_error_channel`. A conforming
    /// shell publishes reports at this level and above and keeps lower levels
    /// console-only; delivered to the shell in its bindings document.
    /// Only meaningful when `surface_error_channel` is set. Serde-typed as a
    /// [`LogLevel`], so an invalid level string fails config parse. Default
    /// `warn`.
    pub surface_error_publish_floor: LogLevel,
}

/// Default [`ObservabilityConfig::surface_error_publish_floor`]: `warn` — the
/// admission floor the interim server-side path enforced, preserved as the
/// default publish floor.
pub(crate) fn default_surface_error_publish_floor() -> LogLevel {
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
#[derive(Debug, PartialEq)]
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
    use crate::config::sole_refusal;

    #[test]
    fn defaults_match_documented_values() {
        let cfg = ObservabilityConfig::default();
        assert_eq!(cfg.surface_error_publish_floor, LogLevel::Warn);
        assert_eq!(cfg.surface_error_channel, None);
        assert_eq!(cfg.usage.session_gap_minutes, 30);
    }

    #[test]
    fn an_unknown_publish_floor_word_is_refused() {
        let refusal = sole_refusal("observability { surface_error_publish_floor = fatal; }\n");
        let rendered = refusal.render();
        assert!(
            rendered.contains("surface_error_publish_floor"),
            "the refusal must name the key: {rendered}"
        );
        assert!(
            rendered.contains("fatal"),
            "the refusal must quote the word it rejected: {rendered}"
        );
    }
}
