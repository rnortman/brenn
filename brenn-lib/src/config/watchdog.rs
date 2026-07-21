use serde::Deserialize;

/// Settings for the bridge-wedge watchdog (`[watchdog]`).
///
/// The watchdog sweeps every live bridge on an interval, looking for a bridge
/// whose event loop has died or whose session I/O is dead while the bridge
/// still believes CC is busy. Defaults are chosen so omitting the section
/// requires no config-file change at deploy.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct WatchdogConfig {
    /// How often the watchdog sweeps the bridge registry, in seconds.
    pub sweep_interval_secs: u64,
    /// How long a "busy but dead I/O" bridge must stay wedged before the
    /// watchdog acts, in seconds. Converted to a whole number of sweeps
    /// (rounded up, minimum one). The deterministic dead-event-loop predicate
    /// ignores this grace and fires on the first sweep.
    pub wedge_grace_secs: u64,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            sweep_interval_secs: 30,
            wedge_grace_secs: 60,
        }
    }
}

impl WatchdogConfig {
    /// Number of consecutive wedged sweeps required before the grace-gated
    /// predicate acts. `wedge_grace_secs / sweep_interval_secs`, rounded up,
    /// clamped to at least one so a wedge always eventually fires even when the
    /// grace is shorter than a single sweep.
    ///
    /// `sweep_interval_secs` is guaranteed `>= 1` by config validation
    /// (`validate_and_resolve`), so the division cannot divide by zero.
    pub fn grace_sweeps(&self) -> u32 {
        let sweeps = self
            .wedge_grace_secs
            .div_ceil(self.sweep_interval_secs)
            .max(1);
        u32::try_from(sweeps).expect("watchdog grace_sweeps exceeds u32::MAX")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn defaults_match_documented_values() {
        let c = WatchdogConfig::default();
        assert_eq!(c.sweep_interval_secs, 30);
        assert_eq!(c.wedge_grace_secs, 60);
        assert_eq!(c.grace_sweeps(), 2);
    }

    #[test]
    fn empty_toml_yields_defaults() {
        // An omitted [watchdog] table must deserialize to the defaults so no
        // config-file change is required at deploy.
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            watchdog: WatchdogConfig,
        }
        let w: Wrapper = toml::from_str("").expect("empty config parses");
        assert_eq!(w.watchdog.sweep_interval_secs, 30);
        assert_eq!(w.watchdog.wedge_grace_secs, 60);
    }

    #[test]
    fn partial_table_keeps_other_default() {
        let c: WatchdogConfig =
            toml::from_str("sweep_interval_secs = 10").expect("partial config parses");
        assert_eq!(c.sweep_interval_secs, 10);
        assert_eq!(c.wedge_grace_secs, 60); // still the default
    }

    #[test]
    fn unknown_field_rejected() {
        assert!(toml::from_str::<WatchdogConfig>("bogus = 1").is_err());
    }

    #[test]
    fn grace_sweeps_rounding_and_clamping() {
        // Exact multiple.
        assert_eq!(
            WatchdogConfig {
                sweep_interval_secs: 30,
                wedge_grace_secs: 60,
            }
            .grace_sweeps(),
            2
        );
        // Rounds up on non-exact division.
        assert_eq!(
            WatchdogConfig {
                sweep_interval_secs: 30,
                wedge_grace_secs: 61,
            }
            .grace_sweeps(),
            3
        );
        // Zero grace clamps to one so a wedge still eventually fires.
        assert_eq!(
            WatchdogConfig {
                sweep_interval_secs: 30,
                wedge_grace_secs: 0,
            }
            .grace_sweeps(),
            1
        );
        // Grace shorter than a sweep clamps to one.
        assert_eq!(
            WatchdogConfig {
                sweep_interval_secs: 120,
                wedge_grace_secs: 60,
            }
            .grace_sweeps(),
            1
        );
    }
}
