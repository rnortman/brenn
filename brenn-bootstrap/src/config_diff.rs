//! `brenn config-diff <a> <b>`: are two config files the same configuration?
//!
//! The comparison is over loaded [`BrennConfig`] values, not over documents:
//! defaults are applied, key order is gone, collections whose order the runtime
//! ignores are sorted, and what is left is what the runtime will actually see.
//! Each side loads through the same extension dispatch `--config` uses.
//!
//! This is how an operator proves a rewritten document — split into modules,
//! stamped from assemblies, refactored any other way — is still the config they
//! were running.

use std::path::Path;

use brenn_lib::config::{BrennConfig, load_config, sort_order_dead_collections};
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
    // is not equal to itself: a non-finite float, which nothing rejects before
    // `validate_and_resolve`. Reporting an empty diff as "these differ" is a
    // verdict nobody can act on.
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

    use brenn_lib::access::raw::{AppAclRaw, ChannelMatcherRaw, WebhookMatcherRaw};
    use brenn_lib::config::config_from_dsl;

    /// A config with one durable channel at `address`, everything else default.
    fn one_channel(address: &str) -> BrennConfig {
        config_from_dsl(&format!(
            r#"
channel target at "{address}" {{
    push_depth = 4;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
"#
        ))
    }

    #[test]
    fn equal_configs_report_equal() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "a.brenn",
            "b.brenn",
        );
        assert!(equal);
        assert_eq!(rendering, "a.brenn and b.brenn are the same config\n");
    }

    #[test]
    fn unequal_configs_report_a_unified_diff() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:bob-desk.in"),
            "a.brenn",
            "b.brenn",
        );
        assert!(!equal);
        assert!(rendering.contains("--- a.brenn"), "{rendering}");
        assert!(rendering.contains("+++ b.brenn"), "{rendering}");
        assert!(rendering.contains("-  "), "{rendering}");
        assert!(
            rendering.contains("\"brenn:alice-desk.in\","),
            "{rendering}"
        );
        assert!(rendering.contains("\"brenn:bob-desk.in\","), "{rendering}");
    }

    const IN_ORDER: &str = r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#;

    fn write(dir: &std::path::Path, name: &str, document: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, document).unwrap();
        path
    }

    /// Exercises the extension dispatch end to end: two `.brenn` documents that
    /// say the same thing in a different key order are one config — the
    /// comparison is over loaded values, and document order is gone by then.
    /// Also catches a differ that panics on a `.brenn` path.
    #[test]
    fn two_brenn_documents_saying_the_same_thing_are_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(dir.path(), "in-order.brenn", IN_ORDER);
        let b = write(
            dir.path(),
            "reordered.brenn",
            r#"
channel alerts at "brenn:alice-alerts" {
    standing_retain_depth = 16;
    retain_depth = 128;
    push_depth = 8;
}
"#,
        );
        assert!(run_config_diff(&a, &b));
    }

    /// Exit status 0 on configs that differ would be a false "safe to deploy".
    #[test]
    fn two_brenn_documents_saying_different_things_are_not_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(dir.path(), "in-order.brenn", IN_ORDER);
        let b = write(
            dir.path(),
            "other.brenn",
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 4;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        );
        assert!(!run_config_diff(&a, &b));
    }

    /// A `nan` compares false against its own copy, so the equality check and
    /// the rendering disagree. Reporting "these differ" over an empty diff is a
    /// verdict nobody can act on, so the differ dies instead.
    ///
    /// No document states a non-finite float — the DSL has no `nan` literal — so
    /// the value is written onto the lowered config. The differ's assert guards
    /// against a float arriving from anywhere at all, which is why it still
    /// earns a test.
    #[test]
    #[should_panic(expected = "compare unequal but render identically")]
    fn a_non_finite_float_is_refused_rather_than_diffed_to_nothing() {
        let with_nan = || -> BrennConfig {
            let mut config = config_from_dsl(
                r#"
channel acks at "ephemeral:sink.acks" { push_depth = 1; retain_depth = 2; }

component Sink {
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    io tick;
    out done;
}

new sink: Sink {
    slug = "sink";
    grants = [ports];

    io tick { push_depth = 1; retain_depth = 2; amplification = 1; }
    out done -> acks { urgency = low; }
}
"#,
            );
            config.wasm_consumers[0].io_ports[0].amplification = Some(f64::NAN);
            config
        };
        diff(with_nan(), with_nan(), "a.brenn", "a.brenn");
    }

    /// One agent whose `brenn_subscribe` matchers appear **in the document** in
    /// the order `matchers` gives, and whose grants likewise. Order lives in the
    /// fixture rather than in a `.reverse()` on the loaded struct, so the test
    /// exercises what `config-diff` actually faces: two documents that list the
    /// same authority in a different order.
    ///
    /// Each entry is `{ kind, name }`: `exact` and `prefix` on the same string
    /// are different authority, so the kind is part of the fixture.
    fn app_with_acl(matchers: [(&str, &str); 3], grants: [&str; 2]) -> BrennConfig {
        let [grant_a, grant_b] = grants;
        let entries = matchers
            .iter()
            .map(|(kind, name)| format!("{kind} \"brenn:{name}\""))
            .collect::<Vec<_>>()
            .join(", ");
        config_from_dsl(&format!(
            r#"
agent Assistant() {{
    grants = [{grant_a}, {grant_b}];

    acl subscribe [{entries}];
    acl publish [exact "brenn:alice-out"];
}}

new alice: Assistant();
"#
        ))
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
            app_with_acl(APP_MATCHERS, ["subscribe", "publish"]),
            app_with_acl(APP_MATCHERS, ["publish", "subscribe"]),
            "derived.brenn",
            "explicit.brenn",
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
            app_with_acl(reversed, ["subscribe", "publish"]),
            app_with_acl(APP_MATCHERS, ["subscribe", "publish"]),
            "derived.brenn",
            "explicit.brenn",
        );
        assert!(equal, "matcher order is dead: {rendering}");
    }

    /// The failure direction the sort must never reach: two configs whose
    /// authority genuinely differs comparing equal because the sort ranked
    /// fewer fields than equality compares. One case per way a channel matcher
    /// can differ.
    #[test]
    fn a_channel_matcher_that_differs_in_content_is_still_a_difference() {
        let baseline = || app_with_acl(APP_MATCHERS, ["subscribe", "publish"]);
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
                "exact where the baseline says prefix",
                [
                    ("exact", "alice-cmd"),
                    ("exact", "alice-desk."),
                    ("exact", "alice-log"),
                ],
            ),
        ];
        for (what, matchers) in cases {
            let (equal, _) = diff(
                app_with_acl(matchers, ["subscribe", "publish"]),
                baseline(),
                "a.brenn",
                "b.brenn",
            );
            assert!(!equal, "{what} is an authority difference");
        }
    }

    /// A bearer-token webhook endpoint block for `slug`, so an `endpoint`
    /// matcher below has something to name.
    fn webhook_block(slug: &str) -> String {
        let handle = slug.replace('-', "_");
        format!(
            r#"
webhook {handle} {{
    slug = "{slug}";
    mount = "/webhooks/{slug}";

    signature {{
        scheme = bearer-token;
        header = "authorization";
    }}

    token phone {{ secret_file = "/home/alice/.secrets/{slug}.token"; }}
}}
"#
        )
    }

    /// One agent with two entries on every ACL plane the apps arm sorts. Each
    /// plane's entries are written to the document in reverse text order when
    /// `reversed`, so the order under test is the document's.
    ///
    /// Both instances declare the same clients, endpoints and channels: the
    /// content-difference test below varies an ACL entry on the lowered struct
    /// instead of in the document, so that what differs is the ACL plane alone.
    ///
    /// The lowered planes are witnessed before returning: without the witness,
    /// a lowering change that emptied a plane would leave every order test on
    /// it comparing two empty vectors.
    fn app_with_every_plane(reversed: bool) -> BrennConfig {
        let (client, topic_filter) = ("shed", "sensors/+/humidity");
        let second_endpoint = "alice-mail";
        let plane = |statement: &str, entries: [String; 2]| -> String {
            let [first, second] = entries;
            let (first, second) = if reversed {
                (second, first)
            } else {
                (first, second)
            };
            format!("    acl {statement} [{first}, {second}];\n")
        };
        let acl = [
            plane(
                "subscribe",
                [
                    r#"topic_filter "mqtt:home:sensors/+/temp""#.to_owned(),
                    format!(r#"topic_filter "mqtt:{client}:{topic_filter}""#),
                ],
            ),
            plane(
                "publish",
                [
                    r#"client "mqtt:home""#.to_owned(),
                    format!(r#"client "mqtt:{client}""#),
                ],
            ),
            plane(
                "subscribe",
                [
                    r#"endpoint "webhook:alice-push""#.to_owned(),
                    format!(r#"endpoint "webhook:{second_endpoint}""#),
                ],
            ),
            plane(
                "subscribe",
                [
                    r#"exact "ephemeral:alice-desk""#.to_owned(),
                    r#"exact "ephemeral:alice-bar""#.to_owned(),
                ],
            ),
            plane(
                "publish",
                [
                    r#"exact "ephemeral:alice-acks""#.to_owned(),
                    r#"exact "ephemeral:alice-out""#.to_owned(),
                ],
            ),
            plane(
                "publish",
                [
                    r#"exact "local:alice-inner""#.to_owned(),
                    r#"exact "local:alice-outer""#.to_owned(),
                ],
            ),
        ]
        .concat();
        let push = webhook_block("alice-push");
        let second = webhook_block(second_endpoint);
        let config = config_from_dsl(&format!(
            r#"
mqtt_client home {{ url = "mqtts://broker.example.com:8883"; }}
mqtt_client {client} {{ url = "mqtts://{client}.example.com:8883"; }}
{push}{second}
agent Assistant() {{
    grants = [subscribe, publish];

{acl}
    subscribe "mqtt:home:sensors/+/temp" {{ push_depth = 1; retain_depth = 1; }}
    subscribe "webhook:alice-push" {{ push_depth = 1; retain_depth = 1; }}
}}

new alice: Assistant();
"#
        ));
        let acl = &config.apps[0].acl;
        for (plane, len) in [
            ("mqtt_subscribe", acl.mqtt_subscribe.len()),
            ("mqtt_publish", acl.mqtt_publish.len()),
            ("webhook", acl.webhook.len()),
            ("ephemeral_subscribe", acl.ephemeral_subscribe.len()),
            ("ephemeral_publish", acl.ephemeral_publish.len()),
            ("local_publish", acl.local_publish.len()),
        ] {
            assert_eq!(len, 2, "{plane} holds the two entries the document states");
        }
        config
    }

    /// Every app ACL plane is sorted, not just the `brenn:` ones — and the MQTT
    /// and webhook matcher types' `Ord` derives rank exactly the fields their
    /// `PartialEq` compares, so a reorder of any plane compares equal.
    #[test]
    fn every_app_acl_plane_is_order_dead() {
        let (equal, rendering) = diff(
            app_with_every_plane(true),
            app_with_every_plane(false),
            "derived.brenn",
            "explicit.brenn",
        );
        assert!(equal, "no app ACL plane is order-sensitive: {rendering}");
    }

    /// The other half of the `Ord`-ranks-what-`PartialEq`-compares invariant:
    /// an MQTT client, an MQTT topic filter or a webhook endpoint that really
    /// differs must survive the sort as a difference.
    #[test]
    fn an_mqtt_or_webhook_matcher_that_differs_in_content_is_still_a_difference() {
        // The variation is applied to the lowered ACL entry, not to the
        // document: a differently-named client or endpoint in the document would
        // also change its declaration block, and the configs would compare
        // unequal on that alone whatever the sort did to the ACL plane.
        type VaryOneEntry = fn(&mut AppAclRaw);
        let cases: [(&str, VaryOneEntry); 3] = [
            ("an MQTT client", |acl| {
                acl.mqtt_publish[1].client = "garage".to_string();
            }),
            ("an MQTT topic filter", |acl| {
                acl.mqtt_subscribe[1].topic_filter = "sensors/+/pressure".to_string();
            }),
            ("a webhook endpoint", |acl| {
                acl.webhook[1].endpoint = "alice-alerts".to_string();
            }),
        ];
        for (what, vary) in cases {
            let mut varied = app_with_every_plane(false);
            vary(&mut varied.apps[0].acl);
            let (equal, _) = diff(varied, app_with_every_plane(false), "a.brenn", "b.brenn");
            assert!(!equal, "{what} is authority, not order");
        }
    }

    /// One WASM consumer with two entries on every plane the consumers arm
    /// sorts. No other fixture in the tree reaches that arm.
    ///
    /// Every instance declares the same channels and endpoints, and the lowered
    /// planes are witnessed before returning, for the reasons on
    /// `app_with_every_plane`.
    fn consumer_with_acls() -> BrennConfig {
        let (channel_a, channel_b) = ("alice-cmd", "alice-log");
        let (endpoint_a, endpoint_b) = ("alice-push", "alice-mail");
        let first = webhook_block(endpoint_a);
        let second = webhook_block(endpoint_b);
        let config = config_from_dsl(&format!(
            r#"
mqtt_client home {{ url = "mqtts://broker.example.com:8883"; }}
mqtt_client shed {{ url = "mqtts://shed.example.com:8883"; }}
{first}{second}
channel inbox at "brenn:{channel_a}" {{
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 8;
}}

channel outbox at "brenn:{channel_b}" {{
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 8;
}}

component Sink {{
    abi = processor;
    component_path = "/lib/brenn_sink.wasm";
    in inbound;
    out outbound;
}}

new sink: Sink {{
    slug = "sink";
    grants = [log, config, ports, mqtt];

    acl subscribe [
        exact inbox,
        exact outbox,
        exact "ephemeral:alice-desk",
        exact "ephemeral:alice-bar",
        exact "local:alice-inner",
        exact "local:alice-outer",
        topic_filter "mqtt:home:sensors/+/temp",
        topic_filter "mqtt:shed:sensors/+/humidity",
        endpoint "webhook:{endpoint_a}",
        endpoint "webhook:{endpoint_b}"
    ];
    acl publish [
        exact outbox,
        prefix "brenn:{channel_a}.",
        exact "ephemeral:alice-acks",
        exact "ephemeral:alice-out",
        exact "local:alice-loop",
        exact "local:alice-tick",
        client "mqtt:home",
        client "mqtt:shed"
    ];

    in inbound <- inbox {{ push_depth = 1; retain_depth = 2; }}
    out outbound -> outbox {{ urgency = low; }}
}}
"#
        ));
        let consumer = &config.wasm_consumers[0];
        assert_eq!(
            consumer.grants.len(),
            4,
            "the four grants the document states"
        );
        for (plane, len) in [
            ("subscribe_acl", consumer.subscribe_acl.len()),
            ("publish_acl", consumer.publish_acl.len()),
            (
                "ephemeral_subscribe_acl",
                consumer.ephemeral_subscribe_acl.len(),
            ),
            (
                "ephemeral_publish_acl",
                consumer.ephemeral_publish_acl.len(),
            ),
            ("local_subscribe_acl", consumer.local_subscribe_acl.len()),
            ("local_publish_acl", consumer.local_publish_acl.len()),
            ("mqtt_subscribe_acl", consumer.mqtt_subscribe_acl.len()),
            ("mqtt_publish_acl", consumer.mqtt_publish_acl.len()),
            ("webhook_acl", consumer.webhook_acl.len()),
        ] {
            assert_eq!(len, 2, "{plane} holds the two entries the document states");
        }
        config
    }

    #[test]
    fn a_wasm_consumers_grants_and_acl_planes_are_order_dead() {
        let mut reordered = consumer_with_acls();
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
            consumer_with_acls(),
            "derived.brenn",
            "explicit.brenn",
        );
        assert!(equal, "no consumer plane is order-sensitive: {rendering}");
    }

    #[test]
    fn a_wasm_consumer_acl_that_differs_in_content_is_still_a_difference() {
        // Varied on the lowered entry, not in the document: see the app arm's
        // content test for why the declarations have to stay identical.
        let mut varied = consumer_with_acls();
        varied.wasm_consumers[0].subscribe_acl[1] =
            ChannelMatcherRaw::Exact("alice-other".to_string());
        let (equal, _) = diff(varied, consumer_with_acls(), "a.brenn", "b.brenn");
        assert!(!equal, "a consumer's subscribe channel is authority");

        let mut varied = consumer_with_acls();
        varied.wasm_consumers[0].webhook_acl[1] = WebhookMatcherRaw {
            endpoint: "alice-alerts".to_string(),
        };
        let (equal, _) = diff(varied, consumer_with_acls(), "a.brenn", "b.brenn");
        assert!(!equal, "a consumer's webhook endpoint is authority");
    }

    /// One surface with two entries on each of the four ACL planes the surfaces
    /// arm sorts, plus two grants.
    ///
    /// Every instance declares the same channels, and the lowered planes are
    /// witnessed before returning, for the reasons on `app_with_every_plane`.
    fn surface_with_acls() -> BrennConfig {
        let (channel_a, channel_b) = ("bar-a", "bar-b");
        let config = config_from_dsl(&format!(
            r#"
channel inbox at "brenn:{channel_a}" {{
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 8;
}}

channel outbox at "brenn:{channel_b}" {{
    push_depth = 4;
    retain_depth = 8;
    standing_retain_depth = 8;
}}

component Panel {{
    abi = dom;
    in messages;
    out outbound;
}}

surface bar {{
    slug = "bar";
    grants = [subscribe, publish];

    acl subscribe [exact inbox, exact outbox];
    acl publish [exact outbox, prefix "brenn:{channel_a}."];
    acl subscribe [exact "ephemeral:bar-desk", exact "ephemeral:bar-mode"];
    acl publish [exact "ephemeral:bar-acks", exact "ephemeral:bar-out"];

    new panel: Panel {{
        in messages <- inbox {{ push_depth = 1; retain_depth = 2; }}
        out outbound -> outbox {{ urgency = low; }}
    }}
}}
"#
        ));
        let surface = &config.surfaces[0];
        assert_eq!(
            surface.grants.len(),
            4,
            "the two stated grants plus the two the ephemeral ACL planes derive"
        );
        for (plane, len) in [
            ("subscribe_acl", surface.subscribe_acl.len()),
            ("publish_acl", surface.publish_acl.len()),
            (
                "ephemeral_subscribe_acl",
                surface.ephemeral_subscribe_acl.len(),
            ),
            ("ephemeral_publish_acl", surface.ephemeral_publish_acl.len()),
        ] {
            assert_eq!(len, 2, "{plane} holds the two entries the document states");
        }
        config
    }

    #[test]
    fn a_surfaces_grants_and_acl_planes_are_order_dead() {
        let mut reordered = surface_with_acls();
        let surface = &mut reordered.surfaces[0];
        surface.grants.reverse();
        surface.subscribe_acl.reverse();
        surface.publish_acl.reverse();
        surface.ephemeral_subscribe_acl.reverse();
        surface.ephemeral_publish_acl.reverse();
        let (equal, rendering) = diff(
            reordered,
            surface_with_acls(),
            "derived.brenn",
            "explicit.brenn",
        );
        assert!(equal, "no surface plane is order-sensitive: {rendering}");
    }

    #[test]
    fn a_surface_acl_that_differs_in_content_is_still_a_difference() {
        let mut varied = surface_with_acls();
        varied.surfaces[0].subscribe_acl[1] = ChannelMatcherRaw::Exact("bar-other".to_string());
        let (equal, _) = diff(varied, surface_with_acls(), "a.brenn", "b.brenn");
        assert!(!equal, "a surface's subscribe channel is authority");
    }

    /// A remote's subscribe ACL folds max over *every* matching entry, so entry
    /// order carries nothing there either — but the ceilings themselves do.
    #[test]
    fn remote_subscribe_acl_order_does_not_make_a_difference_but_its_ceilings_do() {
        let remote = |first_push: u64| -> BrennConfig {
            config_from_dsl(&format!(
                r#"
remote pod_kitchen {{
    token_file = "/home/alice/.secrets/pod-kitchen.token";
    grants = [subscribe];

    acl subscribe [
        prefix "brenn:alice-pod." {{ push_depth = {first_push}, retain_depth = 8 }},
        exact "brenn:alice-pod.out.utterance" {{ push_depth = 16, retain_depth = 32 }}
    ];
}}
"#
            ))
        };
        let mut reordered = remote(4);
        reordered.remotes[0].subscribe_acl.reverse();
        let (equal, rendering) = diff(reordered, remote(4), "a.brenn", "b.brenn");
        assert!(equal, "{rendering}");

        let (equal, _) = diff(remote(4), remote(8), "a.brenn", "b.brenn");
        assert!(!equal, "a ceiling is authored data, not order");
    }

    /// The other direction of the rule: a collection the runtime reads in order
    /// stays order-compared, so a reordered hook script list is a difference.
    #[test]
    fn hook_command_order_still_makes_a_difference() {
        let hooks = |scripts: [&str; 2]| -> BrennConfig {
            config_from_dsl(&format!(
                r#"
agent Assistant() {{
    start_hooks {{ host = ["{}", "{}"]; }}
}}

new alice: Assistant();
"#,
                scripts[0], scripts[1]
            ))
        };
        let (equal, _) = diff(
            hooks(["git fetch", "cargo build"]),
            hooks(["cargo build", "git fetch"]),
            "a.brenn",
            "b.brenn",
        );
        assert!(!equal, "hook scripts run in the order they are written");
    }
}
