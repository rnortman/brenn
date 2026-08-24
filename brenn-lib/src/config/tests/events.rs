use crate::config::EventsConfig;
use crate::config::config_from_dsl;

#[test]
fn events_config_defaults_match_design() {
    assert_eq!(
        EventsConfig::default().delivered_retention_days,
        7,
        "default delivered_retention_days must be 7 (matches design)"
    );
}

#[test]
fn events_config_lowers_an_explicit_value() {
    let config = config_from_dsl("events { delivered_retention_days = 14; }");
    assert_eq!(
        config.events.delivered_retention_days, 14,
        "explicit delivered_retention_days=14 must reach the config"
    );
}

#[test]
fn events_config_omitted_section_uses_default() {
    let config = config_from_dsl("");
    assert_eq!(
        config.events.delivered_retention_days, 7,
        "omitting `events` must yield the default of 7"
    );
}
