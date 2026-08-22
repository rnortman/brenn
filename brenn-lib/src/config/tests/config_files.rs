//! The shipped config files at the repo root, checked as files.
//!
//! Two claims live here. The first is that each file is a config the runtime
//! could take: it loads, it sets the one field the messaging bootstrap requires
//! unconditionally, and both channel-tuning passes run clean over its channel
//! blocks. The second is that each `.brenn` document and its TOML twin are the
//! *same* config — the twins exist only until the TOML front end retires, and
//! nothing but this assertion keeps them from drifting apart in the meantime.

use super::*;
use crate::config::brenn::{canonicalize_config_addresses, check_config};

/// A config file at the repo root, loaded the way boot loads it.
///
/// Routed through `check_config` rather than `toml::from_str` so a `.brenn`
/// document gets the whole pipeline — parse, resolve, derive, lower — and its
/// diagnostics rather than a parse error about an unexpected `/`.
fn load_config_file(filename: &str) -> BrennConfig {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(filename);
    check_config(&path).unwrap_or_else(|report| panic!("{filename} must load: {report}"))
}

// Full validation needs host-side paths to exist, so it runs only at server
// startup. This invariant needs none of those paths and catches a regression
// at `make check` time that only a live start would otherwise surface.
fn assert_config_file_messaging_invariant(filename: &str) {
    let config = load_config_file(filename);
    let public_url = config
        .server
        .public_url
        .as_deref()
        .unwrap_or_else(|| panic!("{filename} must set server.public_url; it is required"));
    assert!(
        !public_url.is_empty(),
        "{filename} sets an empty server.public_url; it must be a well-formed URL"
    );
    // Every depth a `[[channel]]` block owns is required, and nothing under
    // the block supplies one. Running both passes here catches a misconfigured
    // block at `make check` time rather than only at a live server start.
    crate::messaging::config::build_channel_entries(&config.channels, &config.messaging);
    crate::messaging::config::build_system_channel_tuning(&config.channels, &config.messaging);
}

/// A `.brenn` document and its TOML twin are one config.
///
/// The comparison is the differ's: addresses canonicalized so a bare TOML
/// `address` and a scheme-qualified lowered one are not reported as different,
/// and then `PartialEq` over the whole value. Nothing is sorted — array order is
/// semantic in a `BrennConfig`, so an ordering regression in lowering trips this
/// too.
///
/// TODO(dsl-toml-twins): this assertion and the TOML side of it retire together
/// with the TOML front end.
fn assert_twins(document: &str, toml: &str) {
    let mut from_dsl = load_config_file(document);
    let mut from_toml = load_config_file(toml);
    canonicalize_config_addresses(&mut from_dsl);
    canonicalize_config_addresses(&mut from_toml);
    assert_eq!(
        from_dsl, from_toml,
        "{document} and {toml} are no longer the same config; \
         run `brenn config-diff {document} {toml}` for the diff"
    );
}

#[test]
fn brenn_dev_toml_parses() {
    assert_config_file_messaging_invariant("brenn.dev.toml");
}

#[test]
fn brenn_e2e_toml_parses() {
    assert_config_file_messaging_invariant("brenn.e2e.toml");
}

#[test]
fn brenn_dev_brenn_parses() {
    assert_config_file_messaging_invariant("brenn.dev.brenn");
}

#[test]
fn brenn_e2e_brenn_parses() {
    assert_config_file_messaging_invariant("brenn.e2e.brenn");
}

#[test]
fn brenn_e2e_twins_agree() {
    assert_twins("brenn.e2e.brenn", "brenn.e2e.toml");
}

#[test]
fn brenn_dev_twins_agree() {
    assert_twins("brenn.dev.brenn", "brenn.dev.toml");
}
