//! `brenn config-diff <a> <b>`: are two config files the same configuration?
//!
//! The comparison is over parsed [`BrennConfig`] values, not over documents:
//! defaults are applied, key order is gone, collections whose order the runtime
//! ignores are sorted, and what is left is what the runtime will actually see.
//! Everything downstream of `load_config` is provenance-blind, so TOML-vs-DSL,
//! TOML-vs-TOML and DSL-vs-DSL are all the same question, and each side loads
//! through the same extension dispatch `--config` uses.
//!
//! This is the migration tool for the `.brenn` front end: it is how an operator
//! proves a translated config is the config they were running.

use std::path::Path;

use brenn_lib::config::{
    BrennConfig, canonicalize_config_addresses, load_config, sort_order_dead_collections,
};
use similar::TextDiff;

/// Load both files, compare, print the verdict. Returns whether they are equal,
/// which the binary turns into its exit status.
///
/// # Panics
///
/// Panics if either file fails to load — the differ compares valid configs, and
/// an invalid one is a louder failure than a diff.
pub fn run_config_diff(a: &Path, b: &Path) -> bool {
    let config_a = load_config(Some(a));
    let config_b = load_config(Some(b));
    let (equal, rendering) = diff(
        config_a,
        config_b,
        &a.display().to_string(),
        &b.display().to_string(),
    );
    print!("{rendering}");
    equal
}

/// Two loaded configs compared: whether they are equal, and the text to print.
/// The rendering ends in a newline in both arms.
pub(crate) fn diff(
    mut a: BrennConfig,
    mut b: BrennConfig,
    label_a: &str,
    label_b: &str,
) -> (bool, String) {
    canonicalize_config_addresses(&mut a);
    canonicalize_config_addresses(&mut b);
    // Comparison-only normalization: two configs that list the same ACL
    // matchers or the same grants in a different order are one configuration,
    // and a derived ACL's order is whatever the derivation pass emitted.
    sort_order_dead_collections(&mut a);
    sort_order_dead_collections(&mut b);
    if a == b {
        return (
            true,
            format!("{label_a} and {label_b} are the same config\n"),
        );
    }
    let text_a = format!("{a:#?}\n");
    let text_b = format!("{b:#?}\n");
    // Two configs that compare unequal and render identically hold a field that
    // is not equal to itself: a non-finite float, which TOML spells `nan` and
    // which nothing rejects before `validate_and_resolve`. Reporting "these
    // differ" over an empty diff is a verdict nobody can act on.
    assert!(
        text_a != text_b,
        "{label_a} and {label_b} compare unequal but render identically: a `nan` float is \
         never equal to itself, not even to the same file's own copy of it"
    );
    let rendering = TextDiff::from_lines(&text_a, &text_b)
        .unified_diff()
        .header(label_a, label_b)
        .to_string();
    (false, rendering)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config with one durable channel at `address`, everything else default.
    fn one_channel(address: &str) -> BrennConfig {
        toml::from_str(&format!(
            r#"
[[channel]]
address = "{address}"
uuid = "11111111-2222-3333-4444-555555555555"
push_depth = 4
retain_depth = 4
standing_retain_depth = 4
"#
        ))
        .unwrap()
    }

    #[test]
    fn equal_configs_report_equal() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "a.brenn",
            "b.toml",
        );
        assert!(equal);
        assert_eq!(rendering, "a.brenn and b.toml are the same config\n");
    }

    #[test]
    fn unequal_configs_report_a_unified_diff() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:bob-desk.in"),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal);
        assert!(rendering.contains("--- a.brenn"), "{rendering}");
        assert!(rendering.contains("+++ b.toml"), "{rendering}");
        assert!(rendering.contains("-  "), "{rendering}");
        assert!(
            rendering.contains("\"brenn:alice-desk.in\","),
            "{rendering}"
        );
        assert!(rendering.contains("\"brenn:bob-desk.in\","), "{rendering}");
    }

    #[test]
    fn a_bare_address_equals_its_brenn_qualified_twin() {
        let (equal, _) = diff(
            one_channel("alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "bare.toml",
            "qualified.toml",
        );
        assert!(equal);
    }

    #[test]
    fn a_bare_tuning_prefix_equals_its_brenn_qualified_twin() {
        let tuning = |address_prefix: &str| -> BrennConfig {
            toml::from_str(&format!(
                r#"
[[channel]]
address_prefix = "{address_prefix}"
push_depth = 4
retain_depth = 4
standing_retain_depth = 4
"#
            ))
            .unwrap()
        };
        let (equal, _) = diff(
            tuning("tool-results/"),
            tuning("brenn:tool-results/"),
            "bare.toml",
            "qualified.toml",
        );
        assert!(equal);
    }

    /// Exercises the extension dispatch: a `.brenn` document and the TOML it
    /// translates to are the same configuration. Also catches a differ that
    /// loads both sides as TOML or panics on a `.brenn` path.
    #[test]
    fn a_brenn_document_and_its_toml_twin_are_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("main.brenn");
        std::fs::write(
            &document,
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        )
        .unwrap();
        let twin = dir.path().join("twin.toml");
        std::fs::write(
            &twin,
            r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 16
"#,
        )
        .unwrap();
        assert!(run_config_diff(&document, &twin));
    }

    /// Exit status 0 on configs that differ would be a false "safe to deploy".
    #[test]
    fn a_brenn_document_and_a_different_toml_are_not_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("main.brenn");
        std::fs::write(
            &document,
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        )
        .unwrap();
        let other = dir.path().join("other.toml");
        std::fs::write(
            &other,
            r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "alice-alerts"
push_depth = 4
retain_depth = 128
standing_retain_depth = 16
"#,
        )
        .unwrap();
        assert!(!run_config_diff(&document, &other));
    }

    /// A `nan` compares false against its own copy, so the equality check and
    /// the rendering disagree. Reporting "these differ" over an empty diff is a
    /// verdict nobody can act on, so the differ dies instead.
    #[test]
    #[should_panic(expected = "compare unequal but render identically")]
    fn a_non_finite_float_is_refused_rather_than_diffed_to_nothing() {
        let with_nan = || -> BrennConfig {
            toml::from_str(
                r#"
[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["log"]

[[wasm_consumer.io_port]]
port = "tick"
push_depth = 1
retain_depth = 2
amplification = nan
"#,
            )
            .unwrap()
        };
        diff(with_nan(), with_nan(), "a.toml", "a.toml");
    }

    /// One app whose `brenn_subscribe` matchers appear **in the TOML text** in
    /// the order `matchers` gives, and whose grants likewise. Order lives in the
    /// fixture rather than in a `.reverse()` on the loaded struct, so the test
    /// exercises what `config-diff` actually faces: two documents that list the
    /// same authority in a different order.
    ///
    /// Each entry is `{ kind, name }`: `exact` and `prefix` on the same string
    /// are different authority, so the kind is part of the fixture.
    fn app_with_acl(matchers: [(&str, &str); 3], grants: [&str; 2]) -> BrennConfig {
        let [grant_a, grant_b] = grants;
        let entries: String = matchers
            .iter()
            .map(|(kind, name)| format!("[[app.acl.brenn_subscribe]]\n{kind} = \"{name}\"\n\n"))
            .collect();
        toml::from_str(&format!(
            r#"
[[app]]
slug = "alice"
grants = ["{grant_a}", "{grant_b}"]

{entries}
[[app.acl.brenn_publish]]
exact = "alice-out"
"#
        ))
        .unwrap()
    }

    const APP_MATCHERS: [(&str, &str); 3] = [
        ("exact", "alice-cmd"),
        ("prefix", "alice-desk."),
        ("exact", "alice-log"),
    ];

    /// Grants are set-semantic; document order carries nothing.
    #[test]
    fn grant_order_does_not_make_a_difference() {
        let (equal, rendering) = diff(
            app_with_acl(APP_MATCHERS, ["messaging_subscribe", "messaging_publish"]),
            app_with_acl(APP_MATCHERS, ["messaging_publish", "messaging_subscribe"]),
            "derived.brenn",
            "explicit.toml",
        );
        assert!(equal, "grant order is set membership: {rendering}");
    }

    /// ACL matcher order carries no authority; enforcement treats the list as a
    /// set.
    #[test]
    fn acl_matcher_order_does_not_make_a_difference() {
        let mut reversed = APP_MATCHERS;
        reversed.reverse();
        let (equal, rendering) = diff(
            app_with_acl(reversed, ["messaging_subscribe", "messaging_publish"]),
            app_with_acl(APP_MATCHERS, ["messaging_subscribe", "messaging_publish"]),
            "derived.brenn",
            "explicit.toml",
        );
        assert!(equal, "matcher order is dead: {rendering}");
    }

    /// The failure direction the sort must never reach: two configs whose
    /// authority genuinely differs comparing equal because the sort ranked
    /// fewer fields than equality compares. One case per way a channel matcher
    /// can differ.
    #[test]
    fn a_channel_matcher_that_differs_in_content_is_still_a_difference() {
        let baseline = || app_with_acl(APP_MATCHERS, ["messaging_subscribe", "messaging_publish"]);
        let cases = [
            (
                "a different exact channel",
                [
                    ("exact", "alice-other"),
                    ("prefix", "alice-desk."),
                    ("exact", "alice-log"),
                ],
            ),
            (
                "a different prefix",
                [
                    ("exact", "alice-cmd"),
                    ("prefix", "alice-other."),
                    ("exact", "alice-log"),
                ],
            ),
            (
                "prefix where the baseline says exact",
                [
                    ("prefix", "alice-cmd"),
                    ("prefix", "alice-desk."),
                    ("exact", "alice-log"),
                ],
            ),
        ];
        for (what, matchers) in cases {
            let (equal, _) = diff(
                app_with_acl(matchers, ["messaging_subscribe", "messaging_publish"]),
                baseline(),
                "a.brenn",
                "b.toml",
            );
            assert!(!equal, "{what} is an authority difference");
        }
    }

    /// One app with two entries on every ACL plane the apps arm sorts. Each
    /// plane's entries are written to the TOML in reverse text order when
    /// `reversed`, so the order under test is the document's, and `second_mqtt`
    /// / `second_endpoint` vary the *content* of one entry per matcher type.
    fn app_with_every_plane(
        second_mqtt: (&str, &str),
        second_endpoint: &str,
        reversed: bool,
    ) -> BrennConfig {
        let (client, topic_filter) = second_mqtt;
        let plane = |name: &str, entries: [String; 2]| -> String {
            let [first, second] = entries;
            let (first, second) = if reversed {
                (second, first)
            } else {
                (first, second)
            };
            format!("{name} = [{first}, {second}]\n")
        };
        let acl = [
            plane(
                "mqtt_subscribe",
                [
                    r#"{ client = "home", topic_filter = "sensors/+/temp" }"#.to_owned(),
                    format!(r#"{{ client = "{client}", topic_filter = "{topic_filter}" }}"#),
                ],
            ),
            plane(
                "mqtt_publish",
                [
                    r#"{ client = "home" }"#.to_owned(),
                    format!(r#"{{ client = "{client}" }}"#),
                ],
            ),
            plane(
                "webhook",
                [
                    r#"{ endpoint = "alice-push" }"#.to_owned(),
                    format!(r#"{{ endpoint = "{second_endpoint}" }}"#),
                ],
            ),
            plane(
                "ephemeral_subscribe",
                [
                    r#"{ exact = "alice-desk" }"#.to_owned(),
                    r#"{ exact = "alice-bar" }"#.to_owned(),
                ],
            ),
            plane(
                "ephemeral_publish",
                [
                    r#"{ exact = "alice-acks" }"#.to_owned(),
                    r#"{ exact = "alice-out" }"#.to_owned(),
                ],
            ),
            plane(
                "local_publish",
                [
                    r#"{ exact = "alice-inner" }"#.to_owned(),
                    r#"{ exact = "alice-outer" }"#.to_owned(),
                ],
            ),
        ]
        .concat();
        toml::from_str(&format!(
            r#"
[[app]]
slug = "alice"
grants = ["mqtt_subscribe", "mqtt_publish", "webhook", "ephemeral_subscribe"]

[app.acl]
{acl}
"#
        ))
        .unwrap()
    }

    fn app_every_plane_baseline(reversed: bool) -> BrennConfig {
        app_with_every_plane(("shed", "sensors/+/humidity"), "alice-mail", reversed)
    }

    /// Every app ACL plane is sorted, not just the `brenn:` ones — and the MQTT
    /// and webhook matcher types' `Ord` derives rank exactly the fields their
    /// `PartialEq` compares, so a reorder of any plane compares equal.
    #[test]
    fn every_app_acl_plane_is_order_dead() {
        let (equal, rendering) = diff(
            app_every_plane_baseline(true),
            app_every_plane_baseline(false),
            "derived.brenn",
            "explicit.toml",
        );
        assert!(equal, "no app ACL plane is order-sensitive: {rendering}");
    }

    /// The other half of the `Ord`-ranks-what-`PartialEq`-compares invariant:
    /// an MQTT client, an MQTT topic filter or a webhook endpoint that really
    /// differs must survive the sort as a difference.
    #[test]
    fn an_mqtt_or_webhook_matcher_that_differs_in_content_is_still_a_difference() {
        let cases = [
            (
                "an MQTT client",
                app_with_every_plane(("garage", "sensors/+/humidity"), "alice-mail", false),
            ),
            (
                "an MQTT topic filter",
                app_with_every_plane(("shed", "sensors/+/pressure"), "alice-mail", false),
            ),
            (
                "a webhook endpoint",
                app_with_every_plane(("shed", "sensors/+/humidity"), "alice-alerts", false),
            ),
        ];
        for (what, config) in cases {
            let (equal, _) = diff(config, app_every_plane_baseline(false), "a.brenn", "b.toml");
            assert!(!equal, "{what} is authority, not order");
        }
    }

    /// One WASM consumer with two entries on every plane the consumers arm
    /// sorts. No other fixture in the tree reaches that arm.
    fn consumer_with_acls(channels: [&str; 2], endpoints: [&str; 2]) -> BrennConfig {
        let [channel_a, channel_b] = channels;
        let [endpoint_a, endpoint_b] = endpoints;
        toml::from_str(&format!(
            r#"
[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["log", "config"]
subscribe_acl = [{{ exact = "{channel_a}" }}, {{ exact = "{channel_b}" }}]
ephemeral_subscribe_acl = [{{ exact = "alice-desk" }}, {{ exact = "alice-bar" }}]
local_subscribe_acl = [{{ exact = "alice-inner" }}, {{ exact = "alice-outer" }}]
publish_acl = [{{ exact = "{channel_b}" }}, {{ prefix = "{channel_a}." }}]
ephemeral_publish_acl = [{{ exact = "alice-acks" }}, {{ exact = "alice-out" }}]
local_publish_acl = [{{ exact = "alice-loop" }}, {{ exact = "alice-tick" }}]
mqtt_publish_acl = [{{ client = "home" }}, {{ client = "shed" }}]
mqtt_subscribe_acl = [
    {{ client = "home", topic_filter = "sensors/+/temp" }},
    {{ client = "shed", topic_filter = "sensors/+/humidity" }},
]
webhook_acl = [{{ endpoint = "{endpoint_a}" }}, {{ endpoint = "{endpoint_b}" }}]
"#
        ))
        .unwrap()
    }

    #[test]
    fn a_wasm_consumers_grants_and_acl_planes_are_order_dead() {
        let mut reordered =
            consumer_with_acls(["alice-cmd", "alice-log"], ["alice-push", "alice-mail"]);
        let consumer = &mut reordered.wasm_consumers[0];
        consumer.grants.reverse();
        consumer.subscribe_acl.reverse();
        consumer.ephemeral_subscribe_acl.reverse();
        consumer.local_subscribe_acl.reverse();
        consumer.publish_acl.reverse();
        consumer.ephemeral_publish_acl.reverse();
        consumer.local_publish_acl.reverse();
        consumer.mqtt_publish_acl.reverse();
        consumer.mqtt_subscribe_acl.reverse();
        consumer.webhook_acl.reverse();
        let (equal, rendering) = diff(
            reordered,
            consumer_with_acls(["alice-cmd", "alice-log"], ["alice-push", "alice-mail"]),
            "derived.brenn",
            "explicit.toml",
        );
        assert!(equal, "no consumer plane is order-sensitive: {rendering}");
    }

    #[test]
    fn a_wasm_consumer_acl_that_differs_in_content_is_still_a_difference() {
        let (equal, _) = diff(
            consumer_with_acls(["alice-cmd", "alice-other"], ["alice-push", "alice-mail"]),
            consumer_with_acls(["alice-cmd", "alice-log"], ["alice-push", "alice-mail"]),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal, "a consumer's subscribe channel is authority");

        let (equal, _) = diff(
            consumer_with_acls(["alice-cmd", "alice-log"], ["alice-push", "alice-alerts"]),
            consumer_with_acls(["alice-cmd", "alice-log"], ["alice-push", "alice-mail"]),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal, "a consumer's webhook endpoint is authority");
    }

    /// One surface with two entries on each of the four ACL planes the surfaces
    /// arm sorts, plus two grants.
    fn surface_with_acls(channels: [&str; 2]) -> BrennConfig {
        let [channel_a, channel_b] = channels;
        toml::from_str(&format!(
            r#"
[[surface]]
slug = "bar"
grants = ["subscribe", "publish"]
subscribe_acl = [{{ exact = "{channel_a}" }}, {{ exact = "{channel_b}" }}]
publish_acl = [{{ exact = "{channel_b}" }}, {{ prefix = "{channel_a}." }}]
ephemeral_subscribe_acl = [{{ exact = "bar-desk" }}, {{ exact = "bar-mode" }}]
ephemeral_publish_acl = [{{ exact = "bar-acks" }}, {{ exact = "bar-out" }}]
"#
        ))
        .unwrap()
    }

    #[test]
    fn a_surfaces_grants_and_acl_planes_are_order_dead() {
        let mut reordered = surface_with_acls(["bar-a", "bar-b"]);
        let surface = &mut reordered.surfaces[0];
        surface.grants.reverse();
        surface.subscribe_acl.reverse();
        surface.publish_acl.reverse();
        surface.ephemeral_subscribe_acl.reverse();
        surface.ephemeral_publish_acl.reverse();
        let (equal, rendering) = diff(
            reordered,
            surface_with_acls(["bar-a", "bar-b"]),
            "derived.brenn",
            "explicit.toml",
        );
        assert!(equal, "no surface plane is order-sensitive: {rendering}");
    }

    #[test]
    fn a_surface_acl_that_differs_in_content_is_still_a_difference() {
        let (equal, _) = diff(
            surface_with_acls(["bar-a", "bar-other"]),
            surface_with_acls(["bar-a", "bar-b"]),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal, "a surface's subscribe channel is authority");
    }

    /// A remote's subscribe ACL folds max over *every* matching entry, so entry
    /// order carries nothing there either — but the ceilings themselves do.
    #[test]
    fn remote_subscribe_acl_order_does_not_make_a_difference_but_its_ceilings_do() {
        let remote = |first_push: u64| -> BrennConfig {
            toml::from_str(&format!(
                r#"
[[remote]]
slug = "pod-kitchen"
token_file = "/home/alice/.secrets/pod-kitchen.token"
grants = ["subscribe"]

[[remote.subscribe_acl]]
prefix = "alice-pod."
push_depth = {first_push}
retain_depth = 8

[[remote.subscribe_acl]]
exact = "alice-pod.out.utterance"
push_depth = 16
retain_depth = 32
"#
            ))
            .unwrap()
        };
        let mut reordered = remote(4);
        reordered.remotes[0].subscribe_acl.reverse();
        let (equal, rendering) = diff(reordered, remote(4), "a.brenn", "b.toml");
        assert!(equal, "{rendering}");

        let (equal, _) = diff(remote(4), remote(8), "a.brenn", "b.toml");
        assert!(!equal, "a ceiling is authored data, not order");
    }

    /// The other direction of the rule: a collection the runtime reads in order
    /// stays order-compared, so a reordered hook script list is a difference.
    #[test]
    fn hook_command_order_still_makes_a_difference() {
        let hooks = |scripts: [&str; 2]| -> BrennConfig {
            toml::from_str(&format!(
                r#"
[[app]]
slug = "alice"

[app.start_hooks]
host = ["{}", "{}"]
"#,
                scripts[0], scripts[1]
            ))
            .unwrap()
        };
        let (equal, _) = diff(
            hooks(["git fetch", "cargo build"]),
            hooks(["cargo build", "git fetch"]),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal, "hook scripts run in the order they are written");
    }

    #[test]
    fn a_non_brenn_scheme_is_left_alone() {
        let (equal, rendering) = diff(
            one_channel("ephemeral:alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "ephemeral.toml",
            "durable.toml",
        );
        assert!(!equal, "{rendering}");
    }
}
