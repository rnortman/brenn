//! The shipped config files at the repo root, checked as files.
//!
//! One claim lives here: each file is a config the runtime could take — it
//! loads, it sets the one field the messaging bootstrap requires
//! unconditionally, and both channel-tuning passes run clean over its channel
//! blocks.

use super::*;
use crate::config::brenn::check_config;

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
    // Every depth a `channel` block owns is required, and nothing under
    // the block supplies one. Running both passes here catches a misconfigured
    // block at `make check` time rather than only at a live server start.
    crate::messaging::config::build_channel_entries(&config.channels, &config.messaging);
    crate::messaging::config::build_system_channel_tuning(&config.channels, &config.messaging);
}

#[test]
fn brenn_dev_brenn_parses() {
    assert_config_file_messaging_invariant("brenn.dev.brenn");
}

#[test]
fn brenn_e2e_brenn_parses() {
    assert_config_file_messaging_invariant("brenn.e2e.brenn");
}
