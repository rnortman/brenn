use crate::config::EventsConfig;

#[test]
fn events_config_defaults_match_design() {
    assert_eq!(
        EventsConfig::default().delivered_retention_days,
        7,
        "default delivered_retention_days must be 7 (matches design)"
    );
}

/// TOML round-trip: explicit value is parsed correctly.
#[test]
fn events_config_toml_parses_explicit_value() {
    let config: crate::config::BrennConfig =
        toml::from_str("[events]\ndelivered_retention_days = 14").unwrap();
    assert_eq!(
        config.events.delivered_retention_days, 14,
        "explicit delivered_retention_days=14 must parse to 14"
    );
}

/// TOML round-trip: omitting the [events] section uses the default.
#[test]
fn events_config_toml_omit_section_uses_default() {
    let config: crate::config::BrennConfig = toml::from_str("").unwrap();
    assert_eq!(
        config.events.delivered_retention_days, 7,
        "omitting [events] must yield default of 7"
    );
}
