//! The authority model: which family every matcher lands in, which entity types
//! have that family, and what a matcher comes to once its scheme is stripped.
//!
//! Documents are written the way an operator writes them and the derived
//! authority is asserted on the far side. Channels, identity and the wire-kind
//! fold have their own suite.

mod support;

use brenn_dsl::derived::{DAclSet, DMatcher};
use brenn_dsl::diag::Diagnostic;
use brenn_dsl::fixture_text::processor_header;
use fltk_serde_core::Spanned;
use support::{
    PACKAGED, at, derive_errors, derive_errors_tree, derive_refusal, derive_refusals,
    derive_refusals_tree, derived, derived_tree, durable, messages, nondurable, packaged,
};

// ── fixtures ─────────────────────────────────────────────────────────────────

/// The class every consumer fixture instantiates.
///
/// Every port here is `optional`: a fixture class exists so each case binds only
/// the ports the case is about, and a required port would answer every other
/// case with an unconnected-port refusal instead of the one it asked for. The
/// required-port contract itself is asserted in the resolve suite.
///
/// Its grant declaration cannot be read the same way — a processor class has no
/// `optional` — so the class states exactly what its instance grants and the
/// caller passes that list in. A case whose grant word is refused before it
/// becomes a right passes an empty list, so the fit check has nothing to say
/// about it and the case's own refusal is the sole one.
fn sink(requires: &str) -> String {
    format!(
        "{}component Sink {{\n    {}\n    optional out events;\n}}\n{}",
        PACKAGED,
        processor_header(requires),
        PACKAGED,
    )
}

// Every fixture takes the rights it grants, because agreement is checked in both
// directions: a right over an empty list is refused just as an entry no right
// admits is. The bare form grants the planes most of the suite's documents state,
// and a document that states something else says so at the call site.

/// A surface granting the rights named and holding the statements written into
/// it.
fn surface_with(grants: &str, statements: &str) -> String {
    format!("surface alice_desk {{\n    grants = [{grants}];\n{statements}}}\n")
}

/// A surface stating subscribe authority.
fn surface(statements: &str) -> String {
    surface_with("subscribe", statements)
}

/// A top-level component instance granting the capabilities named and holding
/// the statements written into it.
fn consumer(grants: &str, statements: &str) -> String {
    consumer_needing(grants, grants, statements)
}

/// The same, where the class's needs and the instance's grants differ — which
/// they do exactly where the granted word is not a capability the fit check
/// ever sees.
fn consumer_needing(requires: &str, grants: &str, statements: &str) -> String {
    format!(
        "{}new alice_sink: Sink {{\n    \n    grants = [{grants}];\n{statements}}}\n",
        sink(requires)
    )
}

/// An agent granting the rights named and holding the statements written into
/// it.
fn agent_with(grants: &str, statements: &str) -> String {
    format!(
        "agent Assistant() {{\n    name = \"Assistant\";\n    grants = [{grants}];\n\
         {statements}}}\nnew alice: Assistant();\n"
    )
}

/// An agent with no `grants` key at all, which is the posture of holding no
/// rights.
fn agent(statements: &str) -> String {
    format!(
        "agent Assistant() {{\n    name = \"Assistant\";\n{statements}}}\n\
         new alice: Assistant();\n"
    )
}

/// A remote granting the rights named and holding the statements written into
/// it.
fn remote_with(grants: &str, statements: &str) -> String {
    format!(
        "remote bob_pod {{\n    token_file = \"/home/alice/.secrets/bob-pod.token\";\n    \
         grants = [{grants}];\n{statements}}}\n"
    )
}

/// A remote stating subscribe authority.
fn remote(statements: &str) -> String {
    remote_with("subscribe", statements)
}

/// An mqtt client and a webhook, so an address under either scheme names
/// something declared.
const INGRESS: &str = concat!(
    "mqtt_client bob_hub {\n    url = \"mqtts://hub.example.com:8883\";\n}\n",
    "webhook alice_inbox {\n    slug = \"alice-inbox\";\n}\n",
);

/// Every pattern in one channel family, in order.
fn patterns(entries: &[DMatcher]) -> Vec<&str> {
    entries.iter().map(DMatcher::pattern).collect()
}

#[test]
fn a_surface_holds_the_two_transportable_planes() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        surface_with(
            "subscribe, publish",
            concat!(
                "    acl subscribe [exact cmd, prefix \"brenn:alice.in.\"];\n",
                "    acl publish [exact cache];\n",
            ),
        ),
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice.cmd", "alice.in."]);
    assert_eq!(patterns(&acl.ephemeral_publish), ["alice.cache"]);
    assert!(acl.brenn_publish.is_empty());
    assert!(acl.ephemeral_subscribe.is_empty());
}

#[test]
fn an_exact_matcher_naming_a_channel_becomes_that_channels_bare_name() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        surface("    acl subscribe [exact cmd];\n"),
    ));
    match &config.surfaces[0].acl.brenn_subscribe[0] {
        DMatcher::Exact(pattern) => assert_eq!(pattern.value(), "alice.cmd"),
        other => panic!("expected an exact entry, found {other:?}"),
    }
}

#[test]
fn a_consumer_holds_every_family_the_runtime_keeps_a_list_for() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        consumer(
            "ports, mqtt",
            concat!(
                "    acl subscribe [\n",
                "        prefix \"brenn:alice.in.\",\n",
                "        prefix \"ephemeral:alice.\",\n",
                "        prefix \"local:alice/\",\n",
                "        topic_filter \"mqtt:bob_hub:alice/+/status\",\n",
                "        endpoint \"webhook:alice-inbox\"\n",
                "    ];\n",
                "    acl publish [\n",
                "        prefix \"brenn:alice.out.\",\n",
                "        prefix \"ephemeral:alice.out.\",\n",
                "        prefix \"local:alice/out/\",\n",
                "        client \"mqtt:bob_hub\"\n",
                "    ];\n",
            ),
        ),
    ));
    let DAclSet {
        brenn_subscribe,
        brenn_publish,
        ephemeral_subscribe,
        ephemeral_publish,
        local_subscribe,
        local_publish,
        mqtt_subscribe,
        mqtt_publish,
        webhook,
    } = &config.consumers[0].acl;
    assert_eq!(patterns(brenn_subscribe), ["alice.in."]);
    assert_eq!(patterns(brenn_publish), ["alice.out."]);
    assert_eq!(patterns(ephemeral_subscribe), ["alice."]);
    assert_eq!(patterns(ephemeral_publish), ["alice.out."]);
    assert_eq!(patterns(local_subscribe), ["alice/"]);
    assert_eq!(patterns(local_publish), ["alice/out/"]);
    assert_eq!(mqtt_subscribe[0].client.value(), "bob_hub");
    assert_eq!(mqtt_subscribe[0].topic_filter.value(), "alice/+/status");
    assert_eq!(mqtt_publish[0].client.value(), "bob_hub");
    assert_eq!(webhook[0].endpoint.value(), "alice-inbox");
}

#[test]
fn an_agent_holds_every_family_but_confined_subscribe() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        agent_with(
            "subscribe, publish",
            concat!(
                "    acl subscribe [\n",
                "        prefix \"brenn:alice.in.\",\n",
                "        prefix \"ephemeral:alice.\",\n",
                "        topic_filter \"mqtt:bob_hub:alice/status\",\n",
                "        endpoint \"webhook:alice-inbox\"\n",
                "    ];\n",
                "    acl publish [\n",
                "        prefix \"brenn:alice.out.\",\n",
                "        prefix \"ephemeral:alice.out.\",\n",
                "        prefix \"local:alice/\",\n",
                "        client \"mqtt:bob_hub\"\n",
                "    ];\n",
            ),
        ),
    ));
    let acl = &config.agents[0].acl;
    assert_eq!(patterns(&acl.local_publish), ["alice/"]);
    assert!(acl.local_subscribe.is_empty());
    assert_eq!(acl.webhook.len(), 1);
    assert_eq!(acl.mqtt_subscribe.len(), 1);
    assert_eq!(acl.mqtt_publish.len(), 1);
}

#[test]
fn a_remotes_subscribe_entries_carry_ceilings_and_its_publish_entries_do_not() {
    let config = derived(&remote_with(
        "subscribe, publish",
        concat!(
            "    acl subscribe [\n",
            "        prefix \"brenn:alice.out.\" { push_depth = 0, retain_depth = 32 },\n",
            "        prefix \"ephemeral:alice.\" { push_depth = 8, retain_depth = 1 }\n",
            "    ];\n",
            "    acl publish [prefix \"brenn:alice.in.\", prefix \"ephemeral:bob.\"];\n",
        ),
    ));
    let authority = &config.remotes[0];
    assert_eq!(authority.subscribe[0].m.pattern(), "alice.out.");
    assert_eq!(authority.subscribe[0].push_depth, 0);
    assert_eq!(authority.subscribe[0].retain_depth, 32);
    assert_eq!(authority.ephemeral_subscribe[0].push_depth, 8);
    assert_eq!(patterns(&authority.publish), ["alice.in."]);
    assert_eq!(patterns(&authority.ephemeral_publish), ["bob."]);
}

#[test]
fn a_surface_holds_no_confined_authority() {
    assert_eq!(
        derive_refusal(&surface("    acl publish [prefix \"local:alice/\"];\n")),
        "surface `alice_desk` can hold no `local_publish` authority: the runtime keeps \
         no such list for a surface"
    );
}

#[test]
fn a_surface_holds_no_ingress_authority() {
    let refusals = derive_refusals(&format!(
        "{}{}",
        INGRESS,
        surface(concat!(
            "    acl subscribe [\n",
            "        topic_filter \"mqtt:bob_hub:alice/status\",\n",
            "        endpoint \"webhook:alice-inbox\"\n",
            "    ];\n",
            "    acl publish [client \"mqtt:bob_hub\"];\n",
        )),
    ));
    let named: Vec<&str> = refusals
        .iter()
        .map(|message| match message {
            m if m.contains("`mqtt_subscribe`") => "mqtt_subscribe",
            m if m.contains("`mqtt_publish`") => "mqtt_publish",
            m if m.contains("`webhook`") => "webhook",
            m => panic!("{m}"),
        })
        .collect();
    assert_eq!(named, ["mqtt_subscribe", "webhook", "mqtt_publish"]);
}

#[test]
fn an_agent_holds_no_confined_subscribe_authority_by_design() {
    assert_eq!(
        derive_refusal(&agent("    acl subscribe [prefix \"local:alice/\"];\n")),
        "agent `alice` can hold no `local_subscribe` authority: a confined channel has \
         no delivery path to an agent, so the runtime keeps no such list — deliberately, \
         not by omission"
    );
}

#[test]
fn a_remote_holds_the_two_transportable_planes_and_nothing_else() {
    let refusals = derive_refusals(&format!(
        "{}{}",
        INGRESS,
        remote(concat!(
            "    acl subscribe [\n",
            "        prefix \"local:alice/\",\n",
            "        endpoint \"webhook:alice-inbox\"\n",
            "    ];\n",
        )),
    ));
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert!(refusals[0].contains("`local_subscribe`"), "{refusals:?}");
    assert!(refusals[1].contains("`webhook`"), "{refusals:?}");
}

#[test]
fn nothing_publishes_to_a_webhook() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            agent("    acl publish [endpoint \"webhook:alice-inbox\"];\n"),
        )),
        "a webhook is inbound only, so there is no publishing to one: an endpoint \
         belongs on the subscribe plane"
    );
}

#[test]
fn an_acl_statement_names_a_plane() {
    assert_eq!(
        derive_refusal(&surface(
            "    acl ephemeral_subscribe [prefix \"ephemeral:a.\"];\n"
        )),
        "`ephemeral_subscribe` is not a plane; an acl statement names `subscribe` or \
         `publish`, and which scheme it is about comes from its matchers"
    );
}

#[test]
fn a_matcher_pattern_leads_with_a_scheme() {
    assert_eq!(
        derive_refusal(&surface("    acl subscribe [prefix \"alice.\"];\n")),
        "`alice.` names no scheme, so there is no family it is about; a matcher pattern \
         leads with `brenn:`, `ephemeral:`, `local:`, `webhook:` or `mqtt:`"
    );
}

#[test]
fn a_matcher_pattern_under_an_unknown_scheme_names_no_family() {
    assert_eq!(
        derive_refusal(&surface("    acl subscribe [prefix \"amqp:alice.\"];\n")),
        "`amqp:alice.` names no scheme, so there is no family it is about; a matcher \
         pattern leads with `brenn:`, `ephemeral:`, `local:`, `webhook:` or `mqtt:`"
    );
}

#[test]
fn a_matcher_pattern_under_the_runtime_internal_push_scheme_names_no_family() {
    // `pwa_push:` is a scheme the runtime knows and the language does not, so a
    // matcher naming it takes the unknown-scheme path and the `PwaPush` arm of
    // the family fold stays unreachable.
    assert_eq!(
        derive_refusal(&surface(
            "    acl subscribe [prefix \"pwa_push:alice.\"];\n"
        )),
        "`pwa_push:alice.` names no scheme, so there is no family it is about; a matcher \
         pattern leads with `brenn:`, `ephemeral:`, `local:`, `webhook:` or `mqtt:`"
    );
}

#[test]
fn each_family_admits_only_the_kind_its_entries_are_written_with() {
    for (statement, expected) in [
        (
            "    acl subscribe [exact \"mqtt:bob_hub:alice/status\"];\n",
            "`exact` is not how an entry in `mqtt_subscribe` is written; that family \
             takes `topic_filter`",
        ),
        (
            "    acl subscribe [topic_filter \"brenn:alice.cmd\"];\n",
            "`topic_filter` is not how an entry in `brenn_subscribe` is written; that \
             family takes `exact` and `prefix`",
        ),
        (
            "    acl publish [endpoint \"mqtt:bob_hub\"];\n",
            "`endpoint` is not how an entry in `mqtt_publish` is written; that family \
             takes `client`",
        ),
        (
            "    acl subscribe [client \"webhook:alice-inbox\"];\n",
            "`client` is not how an entry in `webhook` is written; that family takes \
             `endpoint`",
        ),
    ] {
        assert_eq!(
            derive_refusal(&format!("{}{}", INGRESS, agent(statement))),
            expected
        );
    }
}

#[test]
fn a_matcher_naming_a_declared_channel_is_exact() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            durable("cmd", "brenn:alice.cmd"),
            surface("    acl subscribe [prefix cmd];\n"),
        )),
        "`prefix` names a declared channel, and a declared channel is one address: \
         write `exact`, or write the pattern `prefix` names as a string"
    );
}

#[test]
fn a_matcher_over_a_whole_plane_is_refused() {
    assert_eq!(
        derive_refusal(&surface("    acl subscribe [prefix \"brenn:\"];\n")),
        "this matcher names a scheme and nothing under it: an empty pattern matches \
         every channel on the plane"
    );
}

#[test]
fn a_prefix_stops_at_a_segment_boundary() {
    assert_eq!(
        derive_refusal(&surface("    acl subscribe [prefix \"brenn:alert\"];\n")),
        "the prefix `alert` does not end at a segment boundary (`/` or `.`), so it \
         over-matches every sibling name it is the start of"
    );
}

#[test]
fn every_boundary_a_prefix_may_stop_at_is_accepted() {
    let config = derived(&surface(
        "    acl subscribe [prefix \"brenn:alice.\", prefix \"brenn:alice/\"];\n",
    ));
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["alice.", "alice/"]
    );
}

#[test]
fn no_matcher_reaches_into_the_anonymous_namespace() {
    for pattern in [
        "exact \"brenn:auto\"",
        "exact \"brenn:auto.7f3\"",
        "prefix \"brenn:auto.\"",
        "prefix \"brenn:auto/\"",
    ] {
        let refusal = derive_refusal(&surface(&format!("    acl subscribe [{pattern}];\n")));
        assert!(
            refusal.contains("reaches into the reserved anonymous namespace"),
            "{pattern}: {refusal}"
        );
    }
}

#[test]
fn a_name_beside_the_anonymous_namespace_is_an_ordinary_channel() {
    let config = derived(&surface(
        "    acl subscribe [prefix \"brenn:automation.\"];\n",
    ));
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["automation."]
    );
}

#[test]
fn a_topic_filter_entry_names_both_a_client_and_a_filter() {
    for (pattern, expected) in [
        (
            "topic_filter \"mqtt:bob_hub\"",
            "`mqtt:bob_hub` names a client and no filter; a topic_filter entry is \
             written `mqtt:<client>:<filter>`",
        ),
        (
            "topic_filter \"mqtt::alice/status\"",
            "`mqtt::alice/status` leaves half of the entry empty; a topic_filter entry \
             names both the client that connects and the filter it subscribes",
        ),
        (
            "topic_filter \"mqtt:bob_hub:\"",
            "`mqtt:bob_hub:` leaves half of the entry empty; a topic_filter entry names \
             both the client that connects and the filter it subscribes",
        ),
    ] {
        assert_eq!(
            derive_refusal(&format!(
                "{}{}",
                INGRESS,
                agent(&format!("    acl subscribe [{pattern}];\n"))
            )),
            expected
        );
    }
}

#[test]
fn a_topic_filter_keeps_every_separator_after_the_client() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        agent_with(
            "subscribe",
            "    acl subscribe [topic_filter \"mqtt:bob_hub:alice/a:b/#\"];\n",
        ),
    ));
    let entry = &config.agents[0].acl.mqtt_subscribe[0];
    assert_eq!(entry.client.value(), "bob_hub");
    assert_eq!(entry.topic_filter.value(), "alice/a:b/#");
}

#[test]
fn publishing_to_mqtt_is_scoped_to_a_client_and_nothing_narrower() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            agent("    acl publish [client \"mqtt:bob_hub:alice/status\"];\n"),
        )),
        "`mqtt:bob_hub:alice/status` names more than a client; publishing is scoped to \
         the client and has no topic dimension to narrow"
    );
}

#[test]
fn an_outbound_mqtt_entry_names_a_client() {
    assert_eq!(
        derive_refusal(&agent("    acl publish [client \"mqtt:\"];\n")),
        "`mqtt:` names no client; an outbound mqtt entry is written `mqtt:<client>`"
    );
}

#[test]
fn every_mqtt_entry_names_a_declared_client() {
    for statement in [
        "    acl subscribe [topic_filter \"mqtt:charlie_hub:alice/status\"];\n",
        "    acl publish [client \"mqtt:charlie_hub\"];\n",
    ] {
        assert_eq!(
            derive_refusal(&format!("{}{}", INGRESS, agent(statement))),
            "no `mqtt_client` is named `charlie_hub`, so nothing connects on this entry's \
             behalf; an mqtt address names a client this configuration declares"
        );
    }
}

#[test]
fn every_endpoint_entry_names_a_declared_webhook() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            agent("    acl subscribe [endpoint \"webhook:charlie-inbox\"];\n"),
        )),
        "no `webhook` is named `charlie-inbox`, so no endpoint mints that channel; an \
         endpoint names a webhook this configuration declares"
    );
}

#[test]
fn a_remotes_subscribe_entry_states_both_ceilings() {
    for (tail, missing) in [
        (" { retain_depth = 4 }", "push_depth"),
        (" { push_depth = 4 }", "retain_depth"),
    ] {
        assert_eq!(
            derive_refusal(&remote(&format!(
                "    acl subscribe [prefix \"brenn:alice.\"{tail}];\n"
            ))),
            format!(
                "a remote's subscribe entry states {missing}: a network peer holds no \
                 channel declaration to inherit a depth from"
            )
        );
    }
}

#[test]
fn a_remotes_subscribe_entry_with_no_tail_at_all_states_both_ceilings() {
    let refusals = derive_refusals(&remote("    acl subscribe [prefix \"brenn:alice.\"];\n"));
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(refusals[0].contains("states push_depth"), "{refusals:?}");
}

#[test]
fn a_remotes_retained_window_is_never_empty() {
    assert_eq!(
        derive_refusal(&remote(
            "    acl subscribe [prefix \"brenn:alice.\" { push_depth = 4, retain_depth = 0 }];\n"
        )),
        "a remote's retain_depth is at least 1: a subscription that retains nothing has \
         nothing for a cursor to resume against"
    );
}

#[test]
fn a_remotes_ceiling_is_a_count_and_never_an_unbounded_window() {
    assert_eq!(
        derive_refusal(&remote(
            "    acl subscribe \
             [prefix \"brenn:alice.\" { push_depth = \"unbounded\", retain_depth = 4 }];\n"
        )),
        "push_depth is a string, and a remote's ceiling is a plain count: an unbounded \
         window is not an answer a network peer may be given"
    );
}

#[test]
fn a_remotes_ceiling_is_never_a_negative_count() {
    // A negative depth is the other way to spell an unbounded window: cast rather
    // than converted, `-1` is every message the channel ever held.
    for (tail, key) in [
        ("push_depth = -1, retain_depth = 4", "push_depth"),
        ("push_depth = 4, retain_depth = -1", "retain_depth"),
    ] {
        assert_eq!(
            derive_refusal(&remote(&format!(
                "    acl subscribe [prefix \"brenn:alice.\" {{ {tail} }}];\n"
            ))),
            format!("{key} is -1, and a depth is a count")
        );
    }
}

#[test]
fn a_remotes_subscribe_entry_carries_the_two_ceilings_and_no_other_key() {
    assert_eq!(
        derive_refusal(&remote(
            "    acl subscribe \
             [prefix \"brenn:alice.\" \
             { push_depth = 4, retain_depth = 4, standing_retain_depth = 8 }];\n"
        )),
        "`standing_retain_depth` is not part of a remote's subscribe entry: it states \
         push_depth and retain_depth and nothing else"
    );
}

#[test]
fn every_other_family_refuses_a_tail_outright() {
    assert_eq!(
        derive_refusal(&surface(
            "    acl subscribe [prefix \"brenn:alice.\" { push_depth = 4 }];\n"
        )),
        "`push_depth` is not part of an entry in `brenn_subscribe`: that family's \
         entries are a pattern and nothing else"
    );
}

#[test]
fn a_remotes_publish_entry_refuses_a_tail_too() {
    assert_eq!(
        derive_refusal(&remote(
            "    acl publish [prefix \"brenn:alice.\" { retain_depth = 4 }];\n"
        )),
        "`retain_depth` is not part of an entry in `brenn_publish`: that family's \
         entries are a pattern and nothing else"
    );
}

#[test]
fn a_grant_merges_into_the_principals_own_families() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        surface("    acl subscribe [prefix \"brenn:alice.in.\"];\n"),
        "grant alice_desk subscribe exact cmd;\n",
    ));
    // Explicit entries first, then granted ones: the model is a function of the
    // statement set and not of a traversal order.
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["alice.in.", "alice.cmd"]
    );
}

#[test]
fn a_grant_is_held_to_the_principals_own_family_table() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            surface(""),
            "grant alice_desk publish prefix \"local:alice/\";\n",
        )),
        "surface `alice_desk` can hold no `local_publish` authority: the runtime keeps \
         no such list for a surface"
    );
}

#[test]
fn no_grant_reaches_a_remotes_subscribe_plane() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            remote(""),
            "grant bob_pod subscribe prefix \"brenn:alice.\" \
             { push_depth = 4, retain_depth = 4 };\n",
        )),
        "a grant cannot reach the subscribe plane of remote `bob_pod`: its entries cap \
         how deep a subscription may be held, and one ceiling per remote is what makes \
         that a bound — write the entry in the remote's own `acl subscribe`"
    );
}

#[test]
fn a_grant_reaches_a_remotes_publish_plane() {
    let config = derived(&format!(
        "{}{}",
        remote_with("publish", ""),
        "grant bob_pod publish prefix \"ephemeral:alice.\";\n",
    ));
    assert_eq!(patterns(&config.remotes[0].ephemeral_publish), ["alice."]);
}

// ── which principal an authority belongs to ──────────────────────────────────

#[test]
fn each_authority_belongs_to_the_entity_at_its_own_index() {
    // Two of each same-typed entity, each holding statements only it could hold:
    // the vectors are parallel by position, so a mis-keyed lookup would hand one
    // principal another's rights and the length assertion would not notice.
    let config = derived(&format!(
        "{}{}{}{}{}{}{}",
        durable("alice_cmd", "brenn:alice.cmd"),
        durable("bob_cmd", "brenn:bob.cmd"),
        "surface alice_desk {\n    grants = [subscribe];\n    \
         acl subscribe [prefix \"brenn:alice.in.\"];\n}\n",
        "surface bob_desk {\n    grants = [subscribe];\n    \
         acl subscribe [prefix \"brenn:bob.in.\"];\n}\n",
        "agent Assistant() {\n    name = \"Assistant\";\n    grants = [publish];\n    \
         acl publish [prefix \"local:alice/\"];\n}\nnew alice: Assistant();\n",
        "agent Helper() {\n    name = \"Helper\";\n    grants = [publish];\n    \
         acl publish [prefix \"local:bob/\"];\n}\nnew bob: Helper();\n",
        // Into the second surface, which is the one a positional slip would miss.
        "grant bob_desk subscribe exact bob_cmd;\n",
    ));

    let surfaces: Vec<&str> = config
        .resolved
        .surfaces
        .iter()
        .map(|surface| surface.slug.value().as_str())
        .collect();
    assert_eq!(surfaces, ["alice_desk", "bob_desk"]);
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["alice.in."]
    );
    assert_eq!(
        patterns(&config.surfaces[1].acl.brenn_subscribe),
        ["bob.in.", "bob.cmd"]
    );

    let agents: Vec<&str> = config
        .resolved
        .agents
        .iter()
        .map(|agent| agent.slug.value().as_str())
        .collect();
    assert_eq!(agents, ["alice", "bob"]);
    assert_eq!(patterns(&config.agents[0].acl.local_publish), ["alice/"]);
    assert_eq!(patterns(&config.agents[1].acl.local_publish), ["bob/"]);
}

// ── what a binding derives ───────────────────────────────────────────────────

/// A surface-placed class with a port facing each way and one facing both,
/// requiring exactly the words its instance grants — an interface word cannot be
/// optional, so the class states what the case gives it. Every port is
/// `optional` for the reason `SINK` states.
fn panel_class(requires: &str) -> String {
    format!(
        "component Panel {{\n    {}\n    optional in messages;\n    \
         optional out acks;\n    optional io tick;\n}}\n",
        processor_header(requires)
    )
}

/// A processor class a top-level instance is made of. Every port is `optional`
/// for the reason `sink` states.
fn relay_class(requires: &str) -> String {
    format!(
        "{}component Relay {{\n    {}\n    optional in inbound;\n    \
         optional out outbound;\n    optional io acks;\n}}\n{}",
        PACKAGED,
        processor_header(requires),
        PACKAGED,
    )
}

/// A surface holding the statements written into it and one `Panel` holding the
/// bindings.
fn panel_surface_with(grants: &str, statements: &str, bindings: &str) -> String {
    // The instance grants `ports` exactly when the bindings written into it
    // send: a grant with nothing to send to is dead config and refused on its
    // own, which would answer every one of these documents with the wrong
    // message.
    let sends = bindings.contains("out ") || bindings.contains("io ");
    let ports = if sends { "ports" } else { "" };
    format!(
        "{}surface alice_desk {{\n    grants = [{grants}];\n{statements}    \
         new p1: Panel {{\n        grants = [{ports}];\n{bindings}    }}\n}}\n",
        panel_class(ports)
    )
}

/// A surface stating subscribe authority, with one `Panel` holding the bindings.
fn panel_surface(statements: &str, bindings: &str) -> String {
    panel_surface_with("subscribe", statements, bindings)
}

/// A surface with one `Panel` whose own body is written out: its grants and
/// whatever statements and bindings it holds.
fn placed_panel(grants: &str, body: &str) -> String {
    format!(
        "{}surface alice_desk {{\n    grants = [subscribe, publish];\n    \
         acl subscribe [prefix \"brenn:alice.\"];\n    acl publish [prefix \"brenn:alice.\"];\n    \
         new p1: Panel {{\n        grants = [{grants}];\n{body}    }}\n}}\n",
        panel_class(grants)
    )
}

/// A top-level instance granting the capabilities named and holding the
/// statements and bindings written into it.
fn relay_with(grants: &str, body: &str) -> String {
    format!(
        "{}new alice_relay: Relay {{\n    \n    grants = [{grants}];\n{body}}}\n",
        relay_class(grants)
    )
}

/// A top-level instance whose lists demand no capability word.
fn relay(body: &str) -> String {
    relay_with("", body)
}

/// A component whose every port is inbound, for the cases that need two
/// positions on one ingress address.
const FAN_IN: &str = concat!(
    packaged!(),
    "component FanIn {\n",
    "    ",
    brenn_dsl::processor_needs!(""),
    "\n",
    "    optional in first;\n",
    "    optional in second;\n",
    "    optional in third;\n",
    "}\n",
    packaged!(),
);

/// A top-level `FanIn` instance holding the bindings written into it.
fn fan_in(body: &str) -> String {
    format!(
        "{FAN_IN}new alice_fan_in: FanIn {{\n    \n    \
         grants = [];\n{body}}}\n"
    )
}

#[test]
fn a_binding_derives_an_exact_entry_on_the_plane_its_port_faces() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        panel_surface_with(
            "subscribe, publish",
            "",
            concat!(
                "        in messages <- cmd;\n",
                "        out acks -> cache;\n",
            ),
        ),
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice.cmd"]);
    assert_eq!(patterns(&acl.ephemeral_publish), ["alice.cache"]);
    assert!(acl.brenn_publish.is_empty());
    assert!(acl.ephemeral_subscribe.is_empty());
}

#[test]
fn an_io_binding_derives_on_both_planes() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_surface_with("subscribe, publish", "", "        io tick <-> cmd;\n"),
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice.cmd"]);
    assert_eq!(patterns(&acl.brenn_publish), ["alice.cmd"]);
}

#[test]
fn a_free_io_port_derives_nothing() {
    let config = derived(&panel_surface_with(
        "",
        "",
        "        io tick { push_depth = 1; retain_depth = 2; }\n",
    ));
    let acl = &config.surfaces[0].acl;
    assert!(acl.brenn_subscribe.is_empty());
    assert!(acl.brenn_publish.is_empty());
}

#[test]
fn two_positions_on_one_channel_derive_one_entry() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_class(""),
        concat!(
            "surface alice_desk {\n",
            "    grants = [subscribe];\n",
            "    new p1: Panel {\n        grants = [];\n        in messages <- cmd;\n    }\n",
            "    new p2: Panel {\n        grants = [];\n        in messages <- cmd;\n    }\n",
            "}\n",
        ),
    ));
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["alice.cmd"]
    );
}

#[test]
fn a_surfaces_confined_binding_derives_nothing_and_is_not_covered() {
    // A surface holds no local family: the page it is served to authorizes the
    // confined ring, so there is nothing to derive and nothing to cover — even
    // beside a statement that suppresses derivation on the plane.
    let config = derived(&panel_surface(
        "    acl subscribe [prefix \"brenn:alice.\"];\n",
        "        in messages <- \"local:alice/theme\";\n",
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice."]);
    assert!(acl.local_subscribe.is_empty());
}

#[test]
fn a_consumer_derives_on_both_planes_and_from_the_ingress_families() {
    let config = derived(&format!(
        "{}{}{}{}",
        INGRESS,
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        relay_with(
            "ports",
            concat!(
                "    in inbound <- \"webhook:alice-inbox\";\n",
                "    out outbound -> cache;\n",
                "    io acks <-> cmd;\n",
            ),
        ),
    ));
    let acl = &config.consumers[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice.cmd"]);
    assert_eq!(patterns(&acl.brenn_publish), ["alice.cmd"]);
    assert_eq!(patterns(&acl.ephemeral_publish), ["alice.cache"]);
    assert_eq!(
        acl.webhook
            .iter()
            .map(|entry| entry.endpoint.value().as_str())
            .collect::<Vec<&str>>(),
        ["alice-inbox"]
    );
}

#[test]
fn an_mqtt_subscription_derives_the_topic_as_its_filter() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        relay("    in inbound <- \"mqtt:bob_hub:alice/status\";\n"),
    ));
    let entry = &config.consumers[0].acl.mqtt_subscribe[0];
    assert_eq!(entry.client.value(), "bob_hub");
    assert_eq!(entry.topic_filter.value(), "alice/status");
}

#[test]
fn a_consumer_derives_its_confined_positions() {
    // Unlike a surface, a component holds both local families: what reaches it
    // over a confined channel is authorized by a list of its own.
    let config = derived(&relay_with(
        "ports",
        concat!(
            "    in inbound <- \"local:alice/theme\";\n",
            "    out outbound -> \"local:alice/out\";\n",
        ),
    ));
    let acl = &config.consumers[0].acl;
    assert_eq!(patterns(&acl.local_subscribe), ["alice/theme"]);
    assert_eq!(patterns(&acl.local_publish), ["alice/out"]);
}

#[test]
fn two_positions_on_one_endpoint_derive_one_entry() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        fan_in(concat!(
            "    in first <- \"webhook:alice-inbox\";\n",
            "    in second <- \"webhook:alice-inbox\";\n",
        )),
    ));
    assert_eq!(config.consumers[0].acl.webhook.len(), 1);
}

#[test]
fn one_client_and_two_filters_derive_one_entry_each() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        fan_in(concat!(
            "    in first <- \"mqtt:bob_hub:alice/status\";\n",
            "    in second <- \"mqtt:bob_hub:alice/status\";\n",
            "    in third <- \"mqtt:bob_hub:alice/other\";\n",
        )),
    ));
    let entries = &config.consumers[0].acl.mqtt_subscribe;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.topic_filter.value().as_str())
            .collect::<Vec<&str>>(),
        ["alice/status", "alice/other"],
    );
    assert!(
        entries
            .iter()
            .all(|entry| entry.client.value() == "bob_hub"),
        "{entries:?}"
    );
}

#[test]
fn an_agent_subscription_derives_on_the_subscribe_plane() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        agent_with("subscribe", "    subscribe cmd { push_depth = 8; }\n"),
    ));
    assert_eq!(
        patterns(&config.agents[0].acl.brenn_subscribe),
        ["alice.cmd"]
    );
}

#[test]
fn a_tool_namespace_address_stays_a_literal_and_derives_an_entry() {
    let config = derived(&agent_with(
        "subscribe",
        "    subscribe \"brenn:tool-results/alice\";\n",
    ));
    assert_eq!(
        patterns(&config.agents[0].acl.brenn_subscribe),
        ["tool-results/alice"]
    );
}

// ── suppression and coverage ─────────────────────────────────────────────────

#[test]
fn an_explicit_statement_suppresses_derivation_on_its_plane_alone() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        panel_surface_with(
            "subscribe, publish",
            "    acl subscribe [prefix \"brenn:alice.\"];\n",
            concat!(
                "        in messages <- cmd;\n",
                "        out acks -> cache;\n",
            ),
        ),
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice."]);
    // The publish plane states nothing, so the binding on it still derives.
    assert_eq!(patterns(&acl.ephemeral_publish), ["alice.cache"]);
}

#[test]
fn a_grant_merges_where_a_statement_suppresses_derivation() {
    let config = derived(&format!(
        "{}{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:alice.\"];\n",
            "        in messages <- cmd;\n",
        ),
        "grant alice_desk subscribe exact cache;\n",
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice."]);
    assert_eq!(patterns(&acl.ephemeral_subscribe), ["alice.cache"]);
}

#[test]
fn a_binding_a_grant_covers_is_authorized() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:bob.\"];\n",
            "        in messages <- cmd;\n",
        ),
        "grant alice_desk subscribe exact cmd;\n",
    ));
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["bob.", "alice.cmd"]
    );
}

#[test]
fn a_binding_no_explicit_entry_covers_is_refused() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:bob.\"];\n",
            "        in messages <- cmd;\n",
        ),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("this binding reaches `brenn:alice.cmd`, which nothing in"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "messages <- cmd"));
    assert_eq!(errors[0].related.len(), 1, "{:?}", errors[0].related);
    assert_eq!(errors[0].related[0].0, "`acl subscribe` is written here");
}

#[test]
fn a_statement_suppresses_derivation_on_every_scheme_of_its_plane() {
    // A statement is about a plane, not about a scheme: a binding on another
    // scheme of the same plane derives nothing either, so what was written has to
    // cover it.
    let source = format!(
        "{}{}",
        nondurable("cache", "ephemeral:alice.cache"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:alice.\"];\n",
            "        in messages <- cache;\n",
        ),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("nothing in alice_desk's `ephemeral_subscribe` authority covers"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "messages <- cache"));
}

#[test]
fn a_grant_on_another_scheme_is_that_schemes_whole_authority() {
    let config = derived(&format!(
        "{}{}{}",
        nondurable("cache", "ephemeral:alice.cache"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:alice.\"];\n",
            "        in messages <- cache;\n",
        ),
        "grant alice_desk subscribe prefix \"ephemeral:alice.\";\n",
    ));
    let acl = &config.surfaces[0].acl;
    assert_eq!(patterns(&acl.brenn_subscribe), ["alice."]);
    // The granted prefix and nothing else: the binding beside it derived no
    // exact entry of its own.
    assert_eq!(patterns(&acl.ephemeral_subscribe), ["alice."]);
}

#[test]
fn a_prefix_entry_covers_every_name_it_starts() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_surface(
            "    acl subscribe [prefix \"brenn:alice.\"];\n",
            "        in messages <- cmd;\n",
        ),
    ));
    assert_eq!(
        patterns(&config.surfaces[0].acl.brenn_subscribe),
        ["alice."]
    );
}

#[test]
fn an_uncovered_webhook_subscription_is_refused() {
    let refusal = derive_refusal(&format!(
        "{}{}{}",
        INGRESS,
        "webhook bob_inbox {\n    slug = \"bob-inbox\";\n}\n",
        relay(concat!(
            "    acl subscribe [endpoint \"webhook:bob-inbox\"];\n",
            "    in inbound <- \"webhook:alice-inbox\";\n",
        )),
    ));
    assert!(
        refusal.contains("this binding reaches `webhook:alice-inbox`"),
        "{refusal}"
    );
}

#[test]
fn an_uncovered_mqtt_subscription_is_the_runtimes_gate() {
    // Deliberately not checked: covering an mqtt subscription takes filter-subset
    // logic, which the runtime holds and applies on every delivery.
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        relay(concat!(
            "    acl subscribe [topic_filter \"mqtt:bob_hub:bob/#\"];\n",
            "    in inbound <- \"mqtt:bob_hub:alice/status\";\n",
        )),
    ));
    let entries = &config.consumers[0].acl.mqtt_subscribe;
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].topic_filter.value(), "bob/#");
}

#[test]
fn an_uncovered_agent_subscription_is_refused() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        agent(concat!(
            "    acl subscribe [prefix \"brenn:bob.\"];\n",
            "    subscribe cmd;\n",
        )),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].message.contains("this subscription reaches"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "cmd;"));
}

// ── what a position may name ─────────────────────────────────────────────────

#[test]
fn a_transportable_literal_naming_no_declaration_is_refused_in_every_position() {
    for (what, source) in [
        (
            "a surface binding",
            panel_surface("", "        in messages <- \"brenn:alice.cmd\";\n"),
        ),
        (
            "a consumer subscription",
            relay("    in inbound <- \"ephemeral:alice.cache\";\n"),
        ),
        (
            "a consumer output",
            relay("    out outbound -> \"brenn:alice.out\";\n"),
        ),
        (
            "an agent subscription",
            agent("    subscribe \"brenn:alice.cmd\";\n"),
        ),
    ] {
        let refusal = derive_refusal(&source);
        assert!(
            refusal.contains("names no declared channel"),
            "{what}: {refusal}"
        );
    }
}

#[test]
fn a_position_naming_an_undeclared_ingress_block_is_refused_under_suppression() {
    // The reference is checked whether or not the plane derives: a statement that
    // suppresses derivation cannot hide a slug nothing answers to.
    for (what, source) in [
        (
            "an endpoint",
            relay(concat!(
                "    acl subscribe [prefix \"brenn:alice.\"];\n",
                "    in inbound <- \"webhook:bob-inbox\";\n",
            )),
        ),
        (
            "a client",
            relay(concat!(
                "    acl subscribe [prefix \"brenn:alice.\"];\n",
                "    in inbound <- \"mqtt:charlie_hub:alice/status\";\n",
            )),
        ),
    ] {
        let refusals = derive_refusals(&format!("{INGRESS}{source}"));
        assert!(
            refusals.iter().any(|refusal| refusal.starts_with("no `")),
            "{what}: {refusals:?}"
        );
    }
}

#[test]
fn a_position_on_a_scheme_it_cannot_carry_is_refused() {
    for (what, needle, source) in [
        (
            "mqtt on a surface binding",
            "surface `alice_desk` can hold no `mqtt_subscribe` authority, so this binding \
             cannot name `mqtt:`",
            panel_surface(
                "",
                "        in messages <- \"mqtt:bob_hub:alice/status\";\n",
            ),
        ),
        (
            "a webhook on a surface binding",
            "surface `alice_desk` can hold no `webhook` authority, so this binding cannot \
             name `webhook:`",
            panel_surface("", "        in messages <- \"webhook:alice-inbox\";\n"),
        ),
        (
            "mqtt on a consumer output",
            "a consumer binding on the publish plane cannot name `mqtt:`",
            relay("    out outbound -> \"mqtt:bob_hub\";\n"),
        ),
        (
            "a confined channel on an agent subscription",
            "agent `alice` can hold no `local_subscribe` authority, so this subscription \
             cannot name `local:`",
            agent("    subscribe \"local:alice/theme\";\n"),
        ),
    ] {
        let refusals = derive_refusals(&format!("{INGRESS}{source}"));
        assert!(
            refusals.iter().any(|refusal| refusal.contains(needle)),
            "{what}: {refusals:?}"
        );
    }
}

#[test]
fn an_io_binding_is_refused_on_the_plane_the_scheme_cannot_carry() {
    // The ingress families are inbound: an `io` port that names one is legal on
    // the plane it can carry and refused on the other.
    let refusals = derive_refusals(&format!(
        "{}{}",
        INGRESS,
        relay("    io acks <-> \"webhook:alice-inbox\";\n"),
    ));
    assert!(
        refusals.iter().any(|refusal| refusal
            .contains("a consumer binding on the publish plane cannot name `webhook:`")),
        "{refusals:?}"
    );
}

// ── grants: classification, expansion, agreement ─────────────────────────────

/// The tokens an entity's grants came to.
fn granted(grants: &[Spanned<String>]) -> Vec<&str> {
    grants.iter().map(|token| token.value().as_str()).collect()
}

#[test]
fn a_plane_word_expands_to_one_token_per_scheme_it_has_entries_on() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        nondurable("cache", "ephemeral:alice.cache"),
        surface_with(
            "subscribe, publish, alert",
            concat!(
                "    acl subscribe [exact cmd, exact cache];\n",
                "    acl publish [exact cmd, exact cache];\n",
            ),
        ),
    ));
    assert_eq!(
        granted(&config.surfaces[0].grants),
        [
            "subscribe",
            "ephemeral_subscribe",
            "publish",
            "ephemeral_publish",
            "alert"
        ]
    );
}

#[test]
fn a_plane_word_expands_to_nothing_for_a_scheme_with_no_entries() {
    let config = derived(&format!(
        "{}{}",
        nondurable("cache", "ephemeral:alice.cache"),
        surface_with("subscribe, alert", "    acl subscribe [exact cache];\n"),
    ));
    assert_eq!(
        granted(&config.surfaces[0].grants),
        ["ephemeral_subscribe", "alert"]
    );
}

#[test]
fn an_agents_plane_words_expand_into_its_own_spellings() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        agent_with(
            "subscribe, publish, dynamic_subscribe, pwa_push",
            concat!(
                "    acl subscribe [\n",
                "        prefix \"brenn:alice.in.\",\n",
                "        prefix \"ephemeral:alice.\",\n",
                "        topic_filter \"mqtt:bob_hub:alice/status\",\n",
                "        endpoint \"webhook:alice-inbox\"\n",
                "    ];\n",
                "    acl publish [\n",
                "        prefix \"brenn:alice.out.\",\n",
                "        prefix \"ephemeral:alice.out.\",\n",
                "        prefix \"local:alice/\",\n",
                "        client \"mqtt:bob_hub\"\n",
                "    ];\n",
            ),
        ),
    ));
    assert_eq!(
        granted(&config.agents[0].grants),
        [
            "messaging_subscribe",
            "ephemeral_subscribe",
            "mqtt_subscribe",
            "webhook",
            "messaging_publish",
            "ephemeral_publish",
            "local_publish",
            "mqtt_publish",
            "dynamic_subscribe",
            "pwa_push"
        ]
    );
}

#[test]
fn a_remotes_plane_words_expand_the_way_a_surfaces_do() {
    let config = derived(&remote_with(
        "subscribe, publish, alert",
        concat!(
            "    acl subscribe [\n",
            "        prefix \"brenn:alice.\" { push_depth = 4, retain_depth = 8 },\n",
            "        prefix \"ephemeral:alice.\" { push_depth = 4, retain_depth = 8 }\n",
            "    ];\n",
            "    acl publish [prefix \"brenn:bob.\", prefix \"ephemeral:bob.\"];\n",
        ),
    ));
    assert_eq!(
        granted(&config.remotes[0].grants),
        [
            "subscribe",
            "ephemeral_subscribe",
            "publish",
            "ephemeral_publish",
            "alert"
        ]
    );
}

#[test]
fn a_wasm_consumers_words_cross_as_written() {
    // Every transport right a component has the runtime reads off its ACLs, so
    // there is nothing here to expand and the order is the operator's.
    let config = derived(&format!(
        "{}{}",
        nondurable("cache", "ephemeral:alice.cache"),
        relay_with("store, ports, log", "    out outbound -> cache;\n"),
    ));
    assert_eq!(
        granted(&config.consumers[0].grants),
        ["store", "ports", "log"]
    );
}

#[test]
fn scope_that_arrives_only_by_a_grant_is_scope_the_word_expands_over() {
    let config = derived(&format!(
        "{}{}{}",
        durable("cmd", "brenn:alice.cmd"),
        surface_with("subscribe", ""),
        "grant alice_desk subscribe exact cmd;\n",
    ));
    assert_eq!(granted(&config.surfaces[0].grants), ["subscribe"]);
}

#[test]
fn scope_a_binding_derived_is_scope_the_word_expands_over() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        panel_surface("", "        in messages <- cmd;\n"),
    ));
    assert_eq!(granted(&config.surfaces[0].grants), ["subscribe"]);
}

#[test]
fn a_word_that_names_no_right_is_refused_with_the_ones_that_do() {
    // A word that spells a capability another entity type holds is no more a right
    // here than a word that spells nothing at all: every vocabulary answers for
    // itself.
    for (source, expected) in [
        (
            surface_with("dance", ""),
            "`dance` is not a right surface grants hold; they name `subscribe`, `publish` \
             or `alert`",
        ),
        (
            surface_with("ports", ""),
            "`ports` is not a right surface grants hold; they name `subscribe`, `publish` \
             or `alert`",
        ),
        (
            surface_with("dynamic_subscribe", ""),
            "`dynamic_subscribe` is not a right surface grants hold; they name `subscribe`, \
             `publish` or `alert`",
        ),
        (
            agent_with("takeover", ""),
            "`takeover` is not a right agent grants hold; they name `subscribe`, `publish`, \
             `dynamic_subscribe` or `pwa_push`",
        ),
        (
            remote_with("takeover", ""),
            "`takeover` is not a right remote grants hold; they name `subscribe`, `publish` \
             or `alert`",
        ),
        (
            surface_with("takeover", ""),
            // A surface and a remote answer with one vocabulary: takeover is a
            // page capability a component holds, not a right over the wire.
            "`takeover` is not a right surface grants hold; they name `subscribe`, `publish` \
             or `alert`",
        ),
        (
            consumer_needing("", "takeover", ""),
            "`takeover` is a page capability; a top-level consumer has no page",
        ),
        (
            consumer_needing("", "dom", ""),
            "`dom` is a page capability; a top-level consumer has no page to mutate",
        ),
        (
            consumer_needing("", "page-dom", ""),
            "`page-dom` is a page capability; a top-level consumer has no page to arrange",
        ),
    ] {
        assert_eq!(derive_refusal(&source), expected);
    }
}

#[test]
fn a_right_granted_twice_is_refused() {
    let source = surface_with(
        "subscribe, subscribe",
        "    acl subscribe [prefix \"brenn:a.\"];\n",
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "`subscribe` is granted twice; one statement of a right is what holds it"
    );
    assert_eq!(errors[0].related.len(), 1, "{:?}", errors[0].related);
    assert_eq!(errors[0].related[0].0, "it is granted here");
}

#[test]
fn a_raw_scheme_compound_token_is_refused_by_name() {
    for (source, expected) in [
        (
            surface_with("ephemeral_subscribe", ""),
            "`ephemeral_subscribe` is how the config this lowers to spells one scheme of one \
             plane; surface grants name the plane and take their schemes from the entries — write \
             `subscribe`",
        ),
        (
            remote_with("ephemeral_publish", ""),
            "`ephemeral_publish` is how the config this lowers to spells one scheme of one \
             plane; remote grants name the plane and take their schemes from the entries — write \
             `publish`",
        ),
        (
            agent_with("messaging_subscribe", ""),
            "`messaging_subscribe` is how the config this lowers to spells one scheme of one \
             plane; agent grants name the plane and take their schemes from the entries — write \
             `subscribe`",
        ),
        (
            agent_with("webhook", ""),
            "`webhook` is how the config this lowers to spells one scheme of one plane; agent grants \
             name the plane and take their schemes from the entries — write `subscribe`",
        ),
        (
            agent_with("mqtt_publish", ""),
            "`mqtt_publish` is how the config this lowers to spells one scheme of one plane; \
             agent grants name the plane and take their schemes from the entries — write \
             `publish`",
        ),
    ] {
        assert_eq!(derive_refusal(&source), expected);
    }
}

#[test]
fn a_plane_word_on_a_wasm_list_is_refused() {
    assert_eq!(
        derive_refusal(&consumer_needing("", "subscribe", "")),
        "consumer `alice_sink` states no `subscribe` right: a component's grants name the \
         capability interfaces it is given, and its transport rights are read off its bindings \
         and acl statements"
    );
}

#[test]
fn a_plane_word_over_an_empty_plane_is_refused() {
    assert_eq!(
        derive_refusal(&surface_with(
            "subscribe, publish",
            "    acl subscribe [prefix \"brenn:alice.\"];\n"
        )),
        "surface `alice_desk` grants `publish` and states no publish entry on any scheme, so \
         the right reaches nothing: an acl statement or a bound port is what gives it \
         something to authorize"
    );
}

#[test]
fn an_entry_no_plane_word_admits_is_refused() {
    for (what, source, expected) in [
        (
            "a statement",
            agent_with("", "    acl subscribe [prefix \"brenn:alice.\"];\n"),
            "agent `alice` holds a `brenn_subscribe` entry and grants no `subscribe`, so \
             nothing consults it: the plane's right is what admits the transport",
        ),
        (
            "an agent with no grants key at all",
            agent("    acl publish [prefix \"local:alice/\"];\n"),
            "agent `alice` holds a `local_publish` entry and grants no `publish`, so nothing \
             consults it: the plane's right is what admits the transport",
        ),
        (
            "a derived entry",
            format!(
                "{}{}",
                durable("cmd", "brenn:alice.cmd"),
                panel_surface_with("", "", "        in messages <- cmd;\n"),
            ),
            "surface `alice_desk` holds a `brenn_subscribe` entry and grants no `subscribe`, \
             so nothing consults it: the plane's right is what admits the transport",
        ),
    ] {
        assert_eq!(derive_refusal(&source), expected, "{what}");
    }
}

#[test]
fn a_dynamic_subscribe_right_does_not_stand_in_for_the_plane() {
    assert_eq!(
        derive_refusal(&agent_with("dynamic_subscribe", "")),
        "agent `alice` grants `dynamic_subscribe` and not `subscribe`: the subscribe tool \
         decides with the transport right as well, so on its own it reaches nothing"
    );
}

#[test]
fn a_dynamic_subscribe_right_beside_the_plane_stands() {
    let config = derived(&agent_with(
        "subscribe, dynamic_subscribe",
        "    acl subscribe [prefix \"brenn:alice.\"];\n",
    ));
    assert_eq!(
        granted(&config.agents[0].grants),
        ["messaging_subscribe", "dynamic_subscribe"]
    );
}

#[test]
fn a_scopeless_right_answers_to_no_list() {
    // `alert` and `pwa_push` reach a device rather than a channel, so there is
    // no list for them to agree with.
    let config = derived(&surface_with("alert", ""));
    assert_eq!(granted(&config.surfaces[0].grants), ["alert"]);
}

#[test]
fn a_consumer_that_sends_grants_ports() {
    for (what, body) in [
        ("a bound output", "    out outbound -> cache;\n"),
        (
            "a publish entry",
            "    acl publish [prefix \"ephemeral:alice.\"];\n",
        ),
    ] {
        assert_eq!(
            derive_refusal(&format!(
                "{}{}",
                nondurable("cache", "ephemeral:alice.cache"),
                relay_with("", body),
            )),
            "consumer `alice_relay` sends and grants no `ports`: the messaging interface it \
             publishes through is what `ports` gives it",
            "{what}"
        );
    }
}

#[test]
fn a_ports_right_with_nothing_to_send_is_refused() {
    assert_eq!(
        derive_refusal(&relay_with("ports", "")),
        "consumer `alice_relay` grants `ports` and neither binds an output nor states a \
         publish entry, so the interface reaches nothing"
    );
}

#[test]
fn a_consumer_that_publishes_to_a_broker_grants_mqtt() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            relay_with("", "    acl publish [client \"mqtt:bob_hub\"];\n"),
        )),
        "consumer `alice_relay` holds an `mqtt_publish` entry and grants no `mqtt`: the \
         broker interface is what `mqtt` gives it"
    );
}

#[test]
fn an_mqtt_right_with_no_broker_entry_is_refused() {
    assert_eq!(
        derive_refusal(&relay_with("mqtt", "")),
        "consumer `alice_relay` grants `mqtt` and states no `mqtt_publish` entry, so the \
         broker interface reaches nothing"
    );
}

#[test]
fn a_consumer_states_its_grants() {
    assert_eq!(
        derive_refusal(&format!("{}new alice_sink: Sink {{\n    \n}}\n", sink(""))),
        "consumer `alice_sink` states no `grants`: what a component is given is \
         deny-by-default, so an empty list is written `grants = [];` rather than left out"
    );
}

#[test]
fn a_refused_statement_is_not_followed_by_a_refusal_about_its_consequence() {
    // Agreement asks whether the words and the lists say the same thing. A refused
    // statement means the lists are not what the document says, so the question is
    // not asked and the report stays about the cause.
    let refusals = derive_refusals(&surface_with(
        "subscribe",
        "    acl subscribe [prefix \"local:alice/\"];\n",
    ));
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].contains("can hold no `local_subscribe` authority"),
        "{refusals:?}"
    );
}

#[test]
fn a_refused_tail_is_not_followed_by_a_refusal_about_its_consequence() {
    // An illegal tail key leaves the entry standing, so without the refusal being
    // recorded agreement would go on to ask about a list the document has already
    // been told is wrong.
    let refusals = derive_refusals(&surface_with(
        "",
        "    acl subscribe [prefix \"brenn:alice.\" { push_depth = 4 }];\n",
    ));
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].contains("is not part of an entry in"),
        "{refusals:?}"
    );
}

#[test]
fn a_refused_grants_word_is_not_followed_by_a_refusal_about_its_consequence() {
    // The classified list is not what the document states either, so the same rule
    // holds: the refusal stands alone and the operator fixes one thing.
    for (what, source) in [
        (
            "a compound token beside the entry the plane word would have admitted",
            format!(
                "{}{}",
                INGRESS,
                agent_with(
                    "mqtt_subscribe",
                    "    subscribe \"mqtt:bob_hub:alice/status\";\n"
                ),
            ),
        ),
        (
            "no grants key at all beside an output that demands `ports`",
            format!(
                "{}new alice_relay: Relay {{\n    out outbound -> \"local:alice/out\";\n}}\n",
                relay_class("")
            ),
        ),
    ] {
        let refusals = derive_refusals(&source);
        assert_eq!(refusals.len(), 1, "{what}: {refusals:?}");
    }
}

// ── where a refusal points ───────────────────────────────────────────────────

#[test]
fn a_kind_that_does_not_fit_the_family_points_at_the_kind_word() {
    let source = format!(
        "{}{}",
        INGRESS,
        agent("    acl subscribe [exact \"mqtt:bob_hub:alice/status\"];\n")
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].message.contains("is not how an entry in"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "exact \"mqtt:"));
}

#[test]
fn a_declared_channel_under_the_wrong_kind_points_at_the_kind_word() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        surface("    acl subscribe [prefix cmd];\n")
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].message.contains("names a declared channel"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "prefix cmd"));
}

#[test]
fn a_pattern_rule_points_at_the_pattern() {
    let source = surface("    acl subscribe [prefix \"brenn:alice\"];\n");
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("does not end at a segment boundary"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "brenn:alice\""));
}

#[test]
fn a_family_the_entity_lacks_points_at_the_pattern() {
    let source = surface("    acl publish [prefix \"local:alice/\"];\n");
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("can hold no `local_publish` authority"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "local:alice/"));
}

#[test]
fn a_right_that_reaches_nothing_points_at_the_word() {
    let source = surface_with(
        "subscribe, publish",
        "    acl subscribe [prefix \"brenn:a.\"];\n",
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].message.contains("grants `publish` and states no"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "publish]"));
}

#[test]
fn an_expanded_token_carries_the_span_of_the_word_it_came_from() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        surface_with("subscribe", "    acl subscribe [exact cmd];\n"),
    );
    let config = derived(&source);
    let token = &config.surfaces[0].grants[0];
    assert_eq!(token.value(), "subscribe");
    assert_eq!(
        Diagnostic::span_line_col(token.span()),
        at(&source, "subscribe];")
    );
}

#[test]
fn an_entry_no_plane_word_admits_points_at_the_entry() {
    // One arm of the entry-span table per shape that is not a plain channel
    // matcher: a refusal about a list has to indict the text the list came from.
    for (what, source, needle, fragment) in [
        (
            "a ceiling",
            remote_with(
                "",
                "    acl subscribe [prefix \"brenn:alice.\" \
                 { push_depth = 4, retain_depth = 8 }];\n",
            ),
            "brenn:alice.",
            "holds a `brenn_subscribe` entry",
        ),
        (
            "an outbound mqtt entry",
            format!(
                "{}{}",
                INGRESS,
                relay_with("", "    acl publish [client \"mqtt:bob_hub\"];\n"),
            ),
            "mqtt:bob_hub",
            "holds an `mqtt_publish` entry",
        ),
    ] {
        let errors = derive_errors(&source);
        assert_eq!(errors.len(), 1, "{what}: {errors:?}");
        assert!(
            errors[0].message.contains(fragment),
            "{what}: {:?}",
            errors[0].message
        );
        assert_eq!(errors[0].line_col(), at(&source, needle), "{what}");
    }
}

#[test]
fn a_tail_that_carries_nothing_points_at_the_value_written() {
    let source = surface("    acl subscribe [prefix \"brenn:alice.\" { push_depth = 4 }];\n");
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].message.contains("is not part of an entry in"),
        "{:?}",
        errors[0].message
    );
    assert_eq!(errors[0].line_col(), at(&source, "4 }"));
}

// ── what a placed instance holds ─────────────────────────────────────────────

#[test]
fn a_placed_instance_states_its_grants_or_is_refused() {
    let source = format!(
        "{}surface alice_desk {{\n    grants = [];\n    new p1: Panel {{}}\n}}\n",
        panel_class("")
    );
    assert_eq!(
        derive_refusal(&source),
        "component `alice_desk.p1` states no `grants`: what a component is given is \
         deny-by-default, so an empty list is written `grants = [];` rather than left out"
    );
}

#[test]
fn a_backend_only_capability_is_refused_on_a_placed_instance() {
    for (word, expected) in [
        (
            "store",
            "`store` is backend-only in v1; a surface-hosted component cannot be granted it",
        ),
        (
            "mqtt",
            "`mqtt` is backend-only in v1; a surface-hosted component cannot be granted it",
        ),
    ] {
        assert!(
            derive_refusals(&placed_panel(word, "")).contains(&expected.to_string()),
            "{:?}",
            derive_refusals(&placed_panel(word, ""))
        );
    }
}

#[test]
fn a_page_capability_is_a_placed_instances_to_hold() {
    let config = derived(&placed_panel("takeover", ""));
    assert_eq!(
        config.surface_components[0][0]
            .grants
            .iter()
            .map(|word| word.value().as_str())
            .collect::<Vec<_>>(),
        ["takeover"]
    );
}

/// The two DOM capabilities travel the same path as `takeover`: the placement
/// is what grants them, and they reach the instance's resolved list as written.
#[test]
fn the_dom_capabilities_are_a_placed_instances_to_hold() {
    assert_eq!(
        granted(&derived(&placed_panel("dom", "")).surface_components[0][0].grants),
        ["dom"]
    );
    assert_eq!(
        granted(&derived(&placed_panel("dom, page-dom", "")).surface_components[0][0].grants),
        ["dom", "page-dom"]
    );
}

/// The pair, at grant grain: the class-grain rule holds the two lists together,
/// and this is the grant-grain half of the same rule, which is what answers a
/// placement that consents to one word of a pair its class states both of.
/// Page-wide authority with no scoped capability under it is an instance that
/// cannot mutate what it arranges and is not mountable at all.
#[test]
fn page_dom_granted_without_dom_is_refused() {
    let source = format!(
        "{}surface alice_desk {{\n    grants = [];\n    \
         new p1: Panel {{\n        grants = [page-dom];\n    }}\n}}\n",
        panel_class("dom, page-dom")
    );
    assert!(
        derive_refusals(&source).contains(
            &"component `alice_desk.p1` grants `page-dom` and not `dom`: the page-wide capability \
         arranges other instances' elements and mutates them through the scoped one, and only \
         the scoped one makes an instance mountable, so the pair is granted together or not \
         at all"
                .to_string()
        ),
        "{:?}",
        derive_refusals(&source)
    );
}

/// And the spec is still the vocabulary: a placement cannot grant a DOM
/// capability the class named in neither list.
#[test]
fn a_dom_grant_no_spec_permits_is_refused() {
    assert_eq!(
        derive_refusal(concat!(
            "component Needy {\n    abi = processor; requires = []; optional = [takeover];\n",
            "    optional in messages;\n}\n",
            "surface alice_desk {\n    grants = [];\n",
            "    new p1: Needy {\n        grants = [dom];\n    }\n}\n",
        )),
        "component `alice_desk.p1` grants `dom`, which `Needy` neither requires nor lists \
         optional: the spec is the vocabulary"
    );
}

#[test]
fn a_placed_instances_bindings_derive_its_own_authority() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        placed_panel(
            "ports",
            "        in messages <- cmd;\n        out acks -> \"local:p1/acks\";\n",
        ),
    ));
    let instance = &config.surface_components[0][0];
    assert_eq!(patterns(&instance.acl.brenn_subscribe), ["alice.cmd"]);
    // The confined ring is the containment case: the instance holds the entry
    // its own binding derived, and its surface holds no local family at all.
    assert_eq!(patterns(&instance.acl.local_publish), ["p1/acks"]);
    assert!(config.surfaces[0].acl.local_publish.is_empty());
}

#[test]
fn an_explicit_statement_on_a_placed_instance_is_its_whole_plane() {
    let config = derived(&format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        placed_panel(
            "",
            "        acl subscribe [prefix \"brenn:alice.\"];\n        in messages <- cmd;\n",
        ),
    ));
    // The statement suppressed derivation, so the entry is the one written —
    // a prefix, not the binding's exact name.
    assert_eq!(
        patterns(&config.surface_components[0][0].acl.brenn_subscribe),
        ["alice."]
    );
}

#[test]
fn a_binding_outside_a_placed_instances_own_statement_is_refused() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        placed_panel(
            "",
            "        acl subscribe [exact \"brenn:alice.other\"];\n        in messages <- cmd;\n",
        ),
    );
    assert_eq!(
        derive_refusal(&source),
        "this binding reaches `brenn:alice.cmd`, which nothing in alice_desk.p1's \
         `brenn_subscribe` authority covers: an explicit `acl subscribe` is the whole \
         authority for the plane, so a binding beside it derives nothing"
    );
}

#[test]
fn a_placed_instance_that_sends_and_grants_no_ports_is_refused() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        placed_panel("log", "        out acks -> cmd;\n"),
    );
    assert_eq!(
        derive_refusal(&source),
        "component `alice_desk.p1` sends and grants no `ports`: the messaging interface it \
         publishes through is what `ports` gives it"
    );
}

#[test]
fn a_placed_instance_may_grant_ports_for_a_free_io_port() {
    // A free `io` port mints a page-local ring the instance publishes into, so
    // `ports` has something to reach even though no channel is named and no
    // entry is derived.
    let config = derived(&placed_panel(
        "ports",
        "        io tick { push_depth = 1; retain_depth = 2; }\n",
    ));
    assert!(config.surface_components[0][0].acl.local_publish.is_empty());
}

#[test]
fn a_placed_instances_free_io_port_demands_ports() {
    // Boot must count both halves of every `io` port when it asks the same
    // question, so a document the front end passes here would panic there.
    assert_eq!(
        derive_refusal(&placed_panel(
            "log",
            "        io tick { push_depth = 1; retain_depth = 2; }\n",
        )),
        "component `alice_desk.p1` sends and grants no `ports`: the messaging interface it \
         publishes through is what `ports` gives it"
    );
}

#[test]
fn a_placed_instances_ports_grant_with_nothing_to_send_is_refused() {
    let source = format!(
        "{}{}",
        durable("cmd", "brenn:alice.cmd"),
        placed_panel("ports", "        in messages <- cmd;\n"),
    );
    assert_eq!(
        derive_refusal(&source),
        "component `alice_desk.p1` grants `ports` and neither binds an output nor states a \
         publish entry, so the interface reaches nothing"
    );
}

#[test]
fn an_unbindable_scheme_under_a_placed_instance_is_refused_once() {
    // The scheme refusal, like the undeclared-channel one, is about the
    // position — which the surface owns. Counted rather than searched: a
    // duplicate would pass an `any` check and read as two mistakes.
    assert_eq!(
        derive_refusal(&placed_panel(
            "",
            "        in messages <- \"mqtt:bob_hub:alice/status\";\n",
        )),
        "surface `alice_desk` can hold no `mqtt_subscribe` authority, so this binding cannot \
         name `mqtt:`: the runtime keeps no such list for a surface"
    );
}

#[test]
fn a_malformed_address_under_a_placed_instance_is_refused_once() {
    assert_eq!(
        derive_refusal(&placed_panel("", "        in messages <- \"alice.cmd\";\n")),
        "address `alice.cmd` names no scheme; expected one of brenn:, ephemeral:, local:, \
         webhook:, mqtt:"
    );
}

#[test]
fn an_undeclared_channel_under_a_placed_instance_is_refused_once() {
    // The binding is a position of both the instance and its surface, and one
    // mistake earns one message: the surface's, which owns the position.
    let source = placed_panel("", "        in messages <- \"brenn:alice.cmd\";\n");
    assert_eq!(
        derive_refusal(&source),
        "`brenn:alice.cmd` names no declared channel, so this binding of surface `alice_desk` \
         attaches to nothing: a transportable channel exists because a `channel` block \
         declares it"
    );
}

// ── an instance's grants against its class's needs ────────────────────────────
//
// The class is the author's statement of what the component cannot run without;
// the instance's list is the deployer's act of consent. Neither is derived from
// the other, so the fit is checked in both directions, at both placements. The
// fixtures here name `log` rather than `ports` so no agreement rule answers the
// case before the fit does.

/// A surface-placed class needing one capability, and a surface holding one
/// instance of it with the grants named.
fn needy_panel(needs: &str, grants: &str) -> String {
    format!(
        "// ── packaged ──\n\
         component Needy {{\n    abi = processor; {needs};\n    optional in messages;\n}}\n\
         // ── packaged ──\n\
         surface alice_desk {{\n    grants = [];\n    \
         new p1: Needy {{\n        grants = [{grants}];\n    }}\n}}\n"
    )
}

/// A processor class needing one capability, and a top-level instance of it with
/// the grants named.
fn needy_sink(needs: &str, grants: &str) -> String {
    format!(
        "{PACKAGED}component Needy {{\n    abi = processor; {needs};\n    \
         optional in inbound;\n}}\n{PACKAGED}new alice_sink: Needy {{\n    \n    \
         grants = [{grants}];\n}}\n"
    )
}

#[test]
fn a_placed_instance_missing_a_required_capability_is_refused() {
    let source = needy_panel("requires = [log]", "");
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "component `alice_desk.p1` does not grant `log`, which `Needy` requires: a component \
         runs with what it was given, and this one was not given what it needs"
    );
    // The omission is the instance's, and the contract it broke is the class's.
    assert_eq!(errors[0].line_col(), at(&source, "p1"));
    assert_eq!(errors[0].related[0].0, "required here");
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        at(&source, "requires = [log]").map(|(line, _)| line)
    );
}

#[test]
fn a_consumer_missing_a_required_capability_is_refused() {
    assert_eq!(
        derive_refusal(&needy_sink("requires = [log]", "")),
        "consumer `alice_sink` does not grant `log`, which `Needy` requires: a component \
         runs with what it was given, and this one was not given what it needs"
    );
}

/// Every requirement is named, so a deployment under-granting two capabilities
/// sees both rather than one per run.
#[test]
fn every_unmet_requirement_is_reported() {
    let errors = derive_refusals(&needy_panel("requires = [log, alert]", ""));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(errors[0].contains("`log`"), "{errors:?}");
    assert!(errors[1].contains("`alert`"), "{errors:?}");
}

/// The other direction: a capability the spec never asked for is authority
/// nothing reads, so the grant is refused where it was written.
#[test]
fn a_grant_the_spec_never_asked_for_is_refused() {
    let source = needy_panel("requires = []", "alert");
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "component `alice_desk.p1` grants `alert`, which `Needy` neither requires nor lists \
         optional: the spec is the vocabulary"
    );
    assert_eq!(errors[0].line_col(), at(&source, "alert]"));
    assert_eq!(errors[0].related[0].0, "declared here");
}

#[test]
fn a_consumers_undeclared_grant_is_refused() {
    assert_eq!(
        derive_refusal(&needy_sink("requires = []", "alert")),
        "consumer `alice_sink` grants `alert`, which `Needy` neither requires nor lists \
         optional: the spec is the vocabulary"
    );
}

/// What `optional` means: the deployment decides, and both decisions compile.
#[test]
fn an_optional_capability_may_be_granted_or_left_out() {
    for grants in ["", "takeover"] {
        let config = derived(&needy_panel("requires = []; optional = [takeover]", grants));
        assert_eq!(config.surface_components[0].len(), 1, "granting {grants:?}");
    }
}

/// A requirement met is silent, and the word crosses into the derived model as
/// written: the fit check adds a question, not a transformation.
#[test]
fn a_met_requirement_crosses_as_written() {
    let config = derived(&needy_sink("requires = [log]", "log"));
    assert_eq!(granted(&config.consumers[0].grants), ["log"]);
}

/// The two questions compose without either standing in for the other: a
/// processor class may legally require `store`, and every surface placement of
/// it is refused twice — the host cannot implement the word, and the word
/// therefore never reaches the instance's grants. Both messages are true of the
/// same contradiction.
#[test]
fn a_surface_placement_of_a_backend_only_requirement_is_refused_twice() {
    let errors = derive_refusals(concat!(
        "component Needy {\n    abi = processor; requires = [store];\n",
        "    optional in inbound;\n}\n",
        "surface alice_desk {\n    grants = [];\n",
        "    new p1: Needy {\n        grants = [store];\n    }\n}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert_eq!(
        errors[0],
        "`store` is backend-only in v1; a surface-hosted component cannot be granted it"
    );
    assert!(errors[1].contains("does not grant `store`"), "{errors:?}");
}

/// The list nobody wrote is one refusal, not one plus a fit refusal per
/// requirement: the words are not what the document states, so the fit has
/// nothing to ask about.
#[test]
fn a_missing_grants_list_is_not_followed_by_fit_refusals() {
    let source = concat!(
        "component Needy {\n    abi = processor; requires = [log, alert];\n    optional in messages;\n}\n",
        "surface alice_desk {\n    grants = [];\n    new p1: Needy {}\n}\n",
    );
    assert_eq!(
        derive_refusal(source),
        "component `alice_desk.p1` states no `grants`: what a component is given is \
         deny-by-default, so an empty list is written `grants = [];` rather than left out"
    );
}

/// A page capability the spec never named is refused as an over-grant, with the
/// wiring it was granted for in place: what the class permits is the only rule
/// that speaks for `takeover`, because the agreement rules pair only `ports` and
/// `mqtt` with an entry.
#[test]
fn a_takeover_grant_no_spec_permits_is_refused_even_with_the_wiring() {
    let errors = derive_refusals(concat!(
        "component Needy {\n    abi = processor; requires = [ports];\n",
        "    optional in messages;\n    out takeover;\n}\n",
        "surface alice_desk {\n    grants = [];\n",
        "    new p1: Needy {\n        grants = [ports, takeover];\n",
        "        out takeover -> \"local:brenn/takeover\";\n    }\n}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0],
        "component `alice_desk.p1` grants `takeover`, which `Needy` neither requires nor \
         lists optional: the spec is the vocabulary"
    );
}

/// The same grant the spec lists optional stands with no takeover wiring at all:
/// the fit check compares grants to the spec and nothing else, so it neither
/// demands a binding nor answers for one.
#[test]
fn a_permitted_takeover_grant_needs_no_takeover_binding() {
    let config = derived(concat!(
        "component Needy {\n    abi = processor; requires = []; optional = [takeover];\n",
        "    optional in messages;\n    optional out takeover;\n}\n",
        "surface alice_desk {\n    grants = [];\n",
        "    new p1: Needy {\n        grants = [takeover];\n    }\n}\n",
    ));
    assert_eq!(
        granted(&config.surface_components[0][0].grants),
        ["takeover"]
    );
}

/// A word the vocabulary does not hold is refused once, as a word: the fit
/// check reads the classified rights, so a garbage word never reaches it.
#[test]
fn a_garbage_word_does_not_also_fail_the_fit() {
    let errors = derive_refusals(&needy_panel("requires = []", "frobnicate"));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].starts_with("`frobnicate` is not a right"),
        "{errors:?}"
    );
}

// ── tool grants ──────────────────────────────────────────────────────────────
//
// A `tool` statement and the `tools` word are one configuration written in two
// places, and the pass refuses either without the other — the same split-kill
// rule it applies to a right and the list it reaches.

/// One `tool` statement per registry tool, resolved into the grant beside the
/// word that consents to it.
#[test]
fn a_tool_statement_lands_beside_the_word_that_consents_to_it() {
    let config = derived(&consumer(
        "tools",
        "    tool git-repo-pull {\n        allow { repo = \"ws\"; }\n        \
         allow { repo = \"notes\"; }\n        \
         rate_limit { burst = 2; sustained_per_minute = 10; }\n    }\n",
    ));
    let grants = &config.resolved.consumers[0].tools;
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].tool.value(), "git-repo-pull");
    assert_eq!(
        grants[0].clauses,
        vec![
            vec![("repo".to_string(), "ws".to_string())],
            vec![("repo".to_string(), "notes".to_string())],
        ]
    );
    let limit = grants[0].rate_limit.expect("the bucket");
    assert_eq!((limit.burst, limit.sustained_per_minute), (2, 10));
}

/// An `allow`-less grant is the statement that every invocation of the tool is
/// admitted, not an omission.
#[test]
fn a_tool_statement_with_no_allow_block_admits_everything() {
    let config = derived(&consumer("tools", "    tool git-repo-pull {}\n"));
    let grants = &config.resolved.consumers[0].tools;
    assert!(grants[0].clauses.is_empty());
    assert_eq!(grants[0].rate_limit, None);
}

#[test]
fn a_tools_grant_with_no_tool_statement_is_refused() {
    assert_eq!(
        derive_refusal(&consumer("tools", "")),
        "consumer `alice_sink` grants `tools` and states no `tool`: a grant that \
         names no tool reaches nothing"
    );
}

#[test]
fn a_tool_statement_with_no_tools_grant_is_refused() {
    assert_eq!(
        derive_refusal(&consumer(
            "",
            "    tool git-repo-pull {\n        allow { repo = \"ws\"; }\n    }\n"
        )),
        "consumer `alice_sink` states a `tool` grant and does not grant `tools`: \
         the word is what consents to reaching the registry at all"
    );
}

/// An agent's tool authority is the statements themselves: there is no
/// agent-side word to couple them to, so a `tool` block stands alone.
#[test]
fn an_agent_states_tool_grants_with_no_word_beside_them() {
    let config = derived(&agent(
        "    tool git-repo-pull {\n        allow { repo = \"ws\"; }\n    }\n",
    ));
    let grants = &config.resolved.agents[0].tools;
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].tool.value(), "git-repo-pull");
}

// ── the egress budget an mqtt_publish entry carries ──────────────────────────
//
// The entry that authorizes a client is what mints its sink, so the two sink
// knobs ride that entry's tail rather than a table of their own.

#[test]
fn a_client_entry_carries_the_sinks_budget() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        consumer(
            "mqtt",
            "    acl publish [client \"mqtt:bob_hub\" { publish_per_activation = 2.0, \
             publish_capacity = 4 }];\n",
        ),
    ));
    let sink = &config.consumers[0].acl.mqtt_publish[0];
    assert_eq!(sink.client.value(), "bob_hub");
    assert_eq!(sink.publish_per_activation, Some(2.0));
    assert_eq!(sink.publish_capacity, Some(4.0));
}

#[test]
fn a_client_entry_with_no_tail_takes_the_default_budget() {
    let config = derived(&format!(
        "{}{}",
        INGRESS,
        consumer("mqtt", "    acl publish [client \"mqtt:bob_hub\"];\n"),
    ));
    let sink = &config.consumers[0].acl.mqtt_publish[0];
    assert_eq!(sink.publish_per_activation, None);
    assert_eq!(sink.publish_capacity, None);
}

#[test]
fn a_budget_key_the_sink_has_no_knob_for_is_refused_by_name() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            consumer(
                "mqtt",
                "    acl publish [client \"mqtt:bob_hub\" { publish_burst = 2 }];\n",
            ),
        )),
        "`publish_burst` is not part of an `mqtt_publish` entry: the sink it mints takes \
         publish_per_activation and publish_capacity and nothing else"
    );
}

#[test]
fn a_budget_that_is_not_a_number_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            consumer(
                "mqtt",
                "    acl publish [client \"mqtt:bob_hub\" { publish_capacity = \"lots\" }];\n",
            ),
        )),
        "publish_capacity is a string, and a budget is a number of tokens"
    );
}

/// An agent reaches the broker through the app host, which budgets its own
/// egress; only a component holds a sink of its own to tune.
#[test]
fn a_budget_on_a_principal_that_holds_no_sink_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            agent_with(
                "publish",
                "    acl publish [client \"mqtt:bob_hub\" { publish_capacity = 2 }];\n",
            ),
        )),
        "`publish_capacity` is not part of agent `alice`'s `mqtt_publish` entry: an egress \
         budget tunes the sink a component holds, and this entity publishes through a \
         host that budgets its own"
    );
}

#[test]
fn one_client_holds_one_budget() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            INGRESS,
            consumer(
                "mqtt",
                concat!(
                    "    acl publish [\n",
                    "        client \"mqtt:bob_hub\" { publish_capacity = 2 },\n",
                    "        client \"mqtt:bob_hub\" { publish_capacity = 4 }\n",
                    "    ];\n",
                ),
            ),
        )),
        "consumer `alice_sink` states an egress budget for client `bob_hub` twice: one \
         client is one sink, and one sink holds one budget"
    );
}

// ── principals ───────────────────────────────────────────────────────────────
//
// A bare principal is authority and nothing else, declared to be delegated
// from. Every rule here is one relation — attenuation — applied at a different
// pair, and the two dead-config rules are its inverse: consent text that
// consents to nothing.
//
// Every fixture writes a chain, because attenuation is a relation between two
// authorities and a chain of two is the smallest thing it has anything to say
// about.

/// One link of a chain: a principal, and the arrangement stamped under it.
///
/// Every fixture writes a chain, because attenuation is a relation between two
/// authorities and a chain of two is the smallest thing it has anything to say
/// about. Every link carries an arrangement, because the inverse direction
/// judges a principal by what is under it: a principal nothing is under
/// delegates to nothing, and a word or line no arrangement under it needs is
/// dead config. So a link of a passing chain holds exactly what it writes, and
/// a link whose body is refused holds nothing at all — a refused body is judged
/// in neither direction.
struct Link<'a> {
    /// The principal's body.
    body: &'a str,
    /// The grant words the arrangement's consumer holds. Backend words, since
    /// the arrangement is a top-level consumer; the two grant vocabularies
    /// meeting in one namespace has its own case in the ceiling suite below.
    words: &'a str,
    /// The matchers the arrangement's own `acl subscribe` statement writes
    /// beside the channel it declares, which is the reach it asks for beyond
    /// its default. Empty for an arrangement that asks for none — an explicit
    /// statement is the whole authority for the plane, so the statement is
    /// written only where there is something to add to it.
    reach: &'a str,
}

/// The principals a chain declares, root first, each under the one before it.
const CHAIN: [&str; 3] = ["site", "ui", "kitchen"];

/// A chain of principals, each with an arrangement stamped under it.
fn chain(links: &[Link<'_>]) -> String {
    let mut module = String::new();
    let mut document = String::new();
    for (index, link) in links.iter().enumerate() {
        let name = CHAIN[index];
        module.push_str(&format!(
            "component Sink{index} {{ {} in messages; }}\n\
             assembly Hold{index}() {{\n\
             \x20   channel out at \"ephemeral:{name}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
             \x20   new consume: Sink{index} {{ grants = [{}]; {}in messages <- out; }}\n\
             }}\n",
            processor_header(link.words),
            link.words,
            statement(link.reach),
        ));
        let under = match index {
            0 => String::new(),
            _ => format!(" under {}", CHAIN[index - 1]),
        };
        document.push_str(&format!(
            "principal {name}{under} {{\n{}}}\nnew by_{name}: Hold{index}() under {name};\n",
            link.body,
        ));
    }
    format!("{PACKAGED}{module}{PACKAGED}{document}")
}

/// The `acl subscribe` statement an arrangement writes for the reach it asks
/// for beyond the channel it declares, which it names beside it: an explicit
/// statement is the whole authority for its plane, so a binding beside one
/// derives nothing.
fn statement(reach: &str) -> String {
    match reach.is_empty() {
        true => String::new(),
        false => format!("acl subscribe [exact out, {reach}]; "),
    }
}

/// A packaged module holding two arrangements, for the fixtures where one
/// principal is wider than any single arrangement under it.
///
/// `Page` is a surface with a placed instance — an attach word and a
/// capability; `Logger` is a top-level consumer holding `log`. Two of them
/// because a principal is reusable and is judged by the union of everything
/// under it: a word one arrangement does not need is dead config unless
/// another under the same principal holds it, which one arrangement cannot
/// show.
fn two_arrangements(document: &str) -> String {
    format!(
        "{PACKAGED}component Panel {{ {} in messages; }}\n\
         component Feed {{ {} in messages; }}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       new panel: Panel {{ grants = [dom]; in messages <- out; }}\n\
         \x20   }}\n\
         }}\n\
         assembly Logger(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   new consume: Feed {{ grants = [log]; in messages <- out; }}\n\
         }}\n{PACKAGED}{document}",
        processor_header("dom"),
        processor_header("log"),
    )
}

/// A root principal holds what it writes, on either axis or both.
///
/// The assertion is that the chain compiles: the child narrows what the parent
/// wrote, and every excess is refused, so a root whose axis the pass had
/// dropped would refuse the child's narrowing of it. Words and reach are
/// listed together here because one narrowing case per shape is the same claim
/// three times — the observable consequences of a *wrongly* built axis have
/// their own cases below (`a_child_inherits_the_axis_it_does_not_write`, the
/// excess and narrows-nothing refusals).
///
/// Each case's arrangement holds exactly what its link writes, which is what
/// makes the chain live text in both directions at once.
#[test]
fn a_root_principal_holds_the_axes_it_writes() {
    for (root, child) in [
        (
            Link {
                body: "    grants = [alert, config, log];\n",
                words: "alert, config, log",
                reach: "",
            },
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
        ),
        (
            Link {
                body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                words: "",
                reach: "prefix \"brenn:house.\"",
            },
            Link {
                body: "    acl subscribe [prefix \"brenn:house.cmd.\"];\n",
                words: "",
                reach: "prefix \"brenn:house.cmd.\"",
            },
        ),
        (
            Link {
                body: concat!(
                    "    grants = [alert, log];\n",
                    "    acl subscribe [prefix \"brenn:house.\"];\n",
                ),
                words: "alert, log",
                reach: "prefix \"brenn:house.\"",
            },
            Link {
                body: "    grants = [log];\n",
                words: "log",
                reach: "prefix \"brenn:house.\"",
            },
        ),
    ] {
        let source = chain(&[root, child]);
        let config = derived(&source);
        // Nothing lowers a principal, which is what the two declarations beside
        // an entity space holding only the arrangements asserts.
        assert_eq!(config.resolved.principals.len(), 2, "{source}");
        assert!(config.resolved.surfaces.is_empty());
        assert_eq!(config.resolved.consumers.len(), 2);
    }
}

/// The child writes one axis and inherits the other, which is the whole reason
/// replacement is per axis rather than wholesale: `ui` holds `site`'s reach
/// without restating it, and narrowing its reach later cannot silently widen
/// its words.
#[test]
fn a_child_inherits_the_axis_it_does_not_write() {
    let config = derived(&chain(&[
        Link {
            body: concat!(
                "    grants = [alert, log];\n",
                "    acl subscribe [prefix \"brenn:house.\"];\n",
            ),
            words: "alert, log",
            reach: "prefix \"brenn:house.\"",
        },
        Link {
            body: "    grants = [log];\n",
            words: "log",
            reach: "prefix \"brenn:house.\"",
        },
        // A third principal under `ui` narrows the reach `ui` never wrote,
        // which only holds if `ui` inherited it.
        Link {
            body: "    acl subscribe [prefix \"brenn:house.kitchen.\"];\n",
            words: "log",
            reach: "prefix \"brenn:house.kitchen.\"",
        },
    ]));
    assert_eq!(config.resolved.principals.len(), 3);
}

/// An `acl` line replaces the inherited entries of the family it resolves to
/// and no other, so a child narrowing the durable plane keeps the ephemeral
/// reach it was given.
#[test]
fn a_line_replaces_only_the_family_it_resolves_to() {
    let config = derived(&chain(&[
        Link {
            body: "    acl subscribe [prefix \"brenn:house.\", prefix \"ephemeral:house.\"];\n",
            words: "",
            reach: "prefix \"brenn:house.\", prefix \"ephemeral:house.\"",
        },
        Link {
            body: "    acl subscribe [prefix \"brenn:house.cmd.\"];\n",
            words: "",
            reach: "prefix \"brenn:house.cmd.\", prefix \"ephemeral:house.\"",
        },
        Link {
            body: "    acl subscribe [prefix \"ephemeral:house.kitchen.\"];\n",
            words: "",
            reach: "prefix \"brenn:house.cmd.\", \
                   prefix \"ephemeral:house.kitchen.\"",
        },
    ]));
    assert_eq!(config.resolved.principals.len(), 3);
}

/// Exact under prefix holds, which is what lets a chain name one address out of
/// a family its parent holds whole. The assertion is the *absence* of an excess
/// refusal: the one message is that the exact line caps nothing.
///
/// It caps nothing by construction, and that is the honest consequence of the
/// two rules together rather than a fixture that could be written better. Every
/// address an exact entry can name is either a channel the arrangement declares
/// or one it was handed, and both are consent already given — so a ceiling
/// spells `exact` only where the reach it names is already the arrangement's
/// default.
#[test]
fn an_exact_entry_narrows_a_prefix_its_parent_holds() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            durable("cmd", "brenn:house.cmd"),
            chain(&[
                Link {
                    body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                    words: "",
                    reach: "prefix \"brenn:house.\"",
                },
                Link {
                    body: "    acl subscribe [exact cmd];\n",
                    words: "",
                    reach: "",
                },
            ]),
        )),
        "this `acl subscribe` line caps nothing any arrangement under `ui` reaches in \
         `brenn_subscribe` beyond its own channels and what it was handed; a ceiling line \
         nothing needs is dead config"
    );
}

#[test]
fn a_word_the_parent_does_not_hold_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
            Link {
                body: "    grants = [alert, tools];\n",
                words: "",
                reach: "",
            },
        ])),
        "`tools` is not a word `site` holds, which `ui` is under: a principal holds no \
         more than the one it is under, and everything a chain delegates is written at \
         its root"
    );
}

#[test]
fn reach_the_parent_does_not_hold_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                words: "",
                reach: "prefix \"brenn:house.\"",
            },
            Link {
                body: "    acl subscribe [prefix \"brenn:other.\"];\n",
                words: "",
                reach: "",
            },
        ])),
        "`other.` is not reach `site` holds, which `ui` is under: a principal holds no \
         more than the one it is under, and everything a chain delegates is written at \
         its root"
    );
}

/// A prefix reaches addresses an exact entry does not, so subsumption does not
/// hold in that direction however narrow the prefix looks.
///
/// The exact line the parent writes is itself dead config — see
/// [`an_exact_entry_narrows_a_prefix_its_parent_holds`] — so the excess is the
/// first of two messages rather than the only one.
#[test]
fn a_prefix_over_an_exact_entry_is_refused() {
    let refusals = derive_refusals(&format!(
        "{}{}",
        durable("cmd", "brenn:house.cmd"),
        chain(&[
            Link {
                body: "    acl subscribe [exact cmd];\n",
                words: "",
                reach: "",
            },
            Link {
                body: "    acl subscribe [prefix \"brenn:house.cmd.\"];\n",
                words: "",
                reach: "",
            },
        ]),
    ));
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert_eq!(
        refusals[0],
        "`house.cmd.` is not reach `site` holds, which `ui` is under: a principal \
         holds no more than the one it is under, and everything a chain delegates is \
         written at its root"
    );
    // And the second is the other rule reading the same pair of lines: `site`'s
    // own `exact` line caps nothing, because every address an exact entry can
    // name is a channel the arrangement declared or was handed. The count is
    // asserted so a third message about one mistake fails here.
    assert_eq!(
        refusals[1],
        "this `acl subscribe` line caps nothing any arrangement under `site` reaches in \
         `brenn_subscribe` beyond its own channels and what it was handed; a ceiling line \
         nothing needs is dead config"
    );
}

/// The root rule, stated as a refusal: reach that the root never wrote cannot
/// first appear below it, because the operator's authority is not inheritable
/// and an axis a root does not write is empty.
#[test]
fn reach_under_a_parent_that_wrote_none_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
            Link {
                body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                words: "",
                reach: "",
            },
        ])),
        "`house.` is not reach `site` holds, which `ui` is under: a principal holds \
         no more than the one it is under, and everything a chain delegates is written at \
         its root"
    );
}

#[test]
fn a_body_that_writes_no_axis_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
            Link {
                body: "",
                words: "",
                reach: "",
            },
        ])),
        "`ui` is under `site` and narrows nothing: a principal's body writes the axis it \
         narrows, and `site` is already a name for what this holds"
    );
}

#[test]
fn a_words_axis_equal_to_the_inherited_one_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
            Link {
                body: "    grants = [log, alert];\n",
                words: "",
                reach: "",
            },
        ])),
        "this `grants` list is what `site` holds, so it narrows nothing; a ceiling axis \
         that caps nothing is dead config"
    );
}

#[test]
fn a_reach_line_equal_to_the_inherited_one_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                words: "",
                reach: "prefix \"brenn:house.\"",
            },
            Link {
                body: "    acl subscribe [prefix \"brenn:house.\"];\n",
                words: "",
                reach: "",
            },
        ])),
        "this `acl subscribe` line is what `site` holds in `brenn_subscribe`, so it narrows \
         nothing; a ceiling line that caps nothing is dead config"
    );
}

#[test]
fn a_ceiling_word_in_no_grant_vocabulary_is_refused() {
    let refusal = derive_refusal(&chain(&[Link {
        body: "    grants = [dom, chrome];\n",
        words: "",
        reach: "",
    }]));
    assert!(
        refusal.starts_with(
            "`chrome` is not a grant word, so it caps nothing; a ceiling names `alert`, "
        ),
        "{refusal}"
    );
}

/// A confined channel reaches the one component that binds it and is authorized
/// by the host that serves it, so there is no reach for a ceiling to cap.
#[test]
fn a_confined_family_in_a_ceiling_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[Link {
            body: "    acl subscribe [prefix \"local:brenn/\"];\n",
            words: "",
            reach: "",
        }])),
        "a ceiling caps no `local_subscribe` authority: a confined channel reaches the \
         one component that binds it, authorized by the host that serves it"
    );
}

/// A window belongs on the position that holds it; a ceiling says how far reach
/// goes, not how deep.
#[test]
fn a_depth_tail_on_a_ceiling_entry_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[Link {
            body: "    acl subscribe [prefix \"brenn:house.\" { push_depth = 4 }];\n",
            words: "",
            reach: "",
        }])),
        "`push_depth` is a depth, and a ceiling caps reach rather than depth"
    );
}

#[test]
fn a_cycle_of_principals_is_refused() {
    assert_eq!(
        derive_refusal(
            "principal a under b {\n    grants = [dom];\n}\n\
             principal b under a {\n    grants = [log];\n}\n",
        ),
        "`b` is under `a`, which is under `b`; a chain of principals bottoms out at the \
         operator"
    );
}

#[test]
fn under_naming_a_running_entity_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            agent_with("subscribe", ""),
            "principal ui under alice {\n    grants = [dom];\n}\n",
        )),
        "`alice` is an instance; a principal is declared under a `principal`"
    );
}

#[test]
fn under_naming_a_channel_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            durable("cmd", "brenn:house.cmd"),
            "principal ui under cmd {\n    grants = [dom];\n}\n",
        )),
        "`cmd` is a channel; a principal is declared under a `principal`"
    );
}

#[test]
fn under_naming_nothing_is_refused() {
    assert_eq!(
        derive_refusal("principal ui under site {\n    grants = [dom];\n}\n"),
        "`site` is not declared in this file"
    );
}

/// Two spellings of one thing is the mistake worth naming: a grant widens a
/// running entity, and a principal's authority is its body. Exactly one
/// diagnostic — the running-entity refusal must not fire for the same handle.
#[test]
fn a_grant_naming_a_principal_is_refused_once() {
    let refusals = derive_refusals(&format!(
        "{}{}",
        chain(&[
            Link {
                body: "    grants = [alert, log];\n",
                words: "alert, log",
                reach: "",
            },
            Link {
                body: "    grants = [log];\n",
                words: "log",
                reach: "",
            },
        ]),
        "grant ui publish prefix \"brenn:house.\";\n",
    ));
    assert_eq!(
        refusals,
        vec![
            "`ui` is a principal; its authority is written in its body, and a grant widens \
             a running entity"
                .to_string()
        ]
    );
}

#[test]
fn a_body_item_that_is_not_authority_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[
            Link {
                body: "    grants = [log];\n    slug = \"site\";\n",
                words: "",
                reach: "",
            },
            Link {
                body: "    grants = [];\n",
                words: "",
                reach: "",
            },
        ])),
        "a principal is authority and nothing else: `grants` and `acl` lines"
    );
}

/// A packaged module declares no principal: the consent would then be the
/// author's words, and a pin bump could widen it without a character changing
/// in the deployer's file.
#[test]
fn a_packaged_module_declares_no_principal() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}{}",
            PACKAGED, "principal ui {\n    grants = [dom];\n}\n", PACKAGED,
        )),
        "a packaged module declares no principal; what an arrangement holds is the \
         deployment's to give"
    );
}

// ── stamp ceilings ───────────────────────────────────────────────────────────
//
// A stamp of a packaged assembly imports config text its author wrote into the
// deployment's trust anchor. The ceiling is what the deployment says that text
// may come to, and every case here is the same fold: what the arrangement
// confers against what consents to it.
//
// The fixtures are fenced, so the assembly is a packaged module's and its
// stamp in the document half is the packaged boundary.

/// A packaged arrangement holding a top-level consumer: a channel of its own, a
/// consumer bound between a handed channel and that channel, one capability.
fn packaged_loop(body: &str) -> String {
    format!(
        "{PACKAGED}component Sink {{\n    {}\n    in messages;\n    optional out events;\n}}\n\
         \n\
         assembly Loop(slug: String, source: Channel) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   new consume: Sink {{\n\
         \x20       grants = [ports];\n\
         \x20       in messages <- source;\n\
         \x20       out events -> out;\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         channel bar at \"ephemeral:bar\" {{ push_depth = 4; retain_depth = 16; }}\n\
         new demo: Loop(slug = \"demo\", source = bar){body}\n",
        processor_header("ports"),
    )
}

/// A packaged arrangement holding a surface with a placed instance: attach
/// words on the surface, a capability on the instance, a channel of its own.
fn packaged_page(body: &str) -> String {
    format!(
        "{PACKAGED}component Panel {{\n    {}\n    in messages;\n}}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       new panel: Panel {{ grants = [dom]; in messages <- out; }}\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         new demo: Page(slug = \"demo\"){body}\n",
        processor_header("dom"),
    )
}

/// The common case: the deployer writes the words the arrangement holds, and
/// the reach it needs is the channel it declares and the one it was handed,
/// which the stamp consents to by stamping and by passing.
#[test]
fn a_ceiling_covering_what_the_arrangement_confers_passes() {
    let config = derived(&packaged_loop(" { grants = [ports]; }"));
    assert_eq!(config.resolved.stamps.len(), 1);
    assert_eq!(config.consumers[0].grants.len(), 1);
}

/// The same over a surface and its placed instance: the attach word and the
/// capability word are one namespace, so one list caps both.
#[test]
fn a_ceiling_covers_a_surface_and_its_instances() {
    let config = derived(&packaged_page(" { grants = [dom, subscribe]; }"));
    assert_eq!(config.resolved.stamps.len(), 1);
}

/// The description shape, which is why the ceiling's default is right: an
/// arrangement that declares channels and holds nothing needs no ceiling text,
/// and the stamp is one line.
#[test]
fn an_arrangement_that_confers_nothing_needs_no_ceiling() {
    let config = derived(&format!(
        "{PACKAGED}assembly Describe(slug: String) {{\n\
         \x20   channel geometry at f\"brenn:surface.{{slug}}.geometry\" {{ push_depth = 1; retain_depth = 1; standing_retain_depth = 1; }}\n\
         }}\n{PACKAGED}\
         new demo: Describe(slug = \"demo\");\n"
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
    let stamp = &config.resolved.stamps[0];
    assert!(stamp.grants.is_none() && stamp.acls.is_empty());
}

/// A word the arrangement holds and the ceiling does not is the whole point:
/// the refusal names the word, the instance holding it, and writes the
/// whole line the deployer needs.
#[test]
fn a_word_beyond_the_ceiling_is_refused_with_the_line_to_write() {
    let source = packaged_page(" { grants = [subscribe]; }");
    let errors = derive_errors(&source);
    assert_eq!(
        errors[0].message,
        "stamping `Page` from `@fixtures` confers `dom` on `demo.page.panel`, which this \
         stamp's ceiling does not cover: a packaged arrangement holds what the deployment \
         stamps it with, so write it — `grants = [dom, subscribe];`"
    );
    // At the stamp — the deployer's file, which is where the line goes — with
    // the author's `grants` word as the related site.
    assert_eq!(errors[0].line_col(), at(&source, "demo: Page"));
    assert_eq!(errors[0].related[0].0, "`demo.page.panel` holds it here");
}

/// With nothing written the ceiling is empty, so every word is reported and
/// every one carries the same suggested line: fixing one fixes all.
#[test]
fn an_empty_ceiling_reports_every_word_with_one_line() {
    let refusals = derive_refusals(&packaged_page(";"));
    assert_eq!(refusals.len(), 2);
    for refusal in &refusals {
        assert!(
            refusal.ends_with("so write it — `grants = [dom, subscribe];`"),
            "{refusal}"
        );
    }
}

/// A ceiling under a principal suggests the principal instead: the consent
/// belongs where the deployer put it, and a stamp's body can only narrow what
/// the principal holds.
#[test]
fn a_word_beyond_a_principal_names_the_principal() {
    let refusal = derive_refusal(&format!(
        "principal ui {{ grants = [subscribe]; }}\n{}",
        packaged_page(" under ui;")
    ));
    assert_eq!(
        refusal,
        "stamping `Page` from `@fixtures` confers `dom` on `demo.page.panel`, which this \
         stamp's ceiling does not cover: a packaged arrangement holds what the deployment \
         stamps it with, so add `dom` to `ui`, or stamp under a principal that holds it"
    );
}

/// The principal holds the word and the stamp's own body hands down less, so
/// the edit that fixes it is the stamp's `grants` line: naming the principal
/// would name a line that already holds it.
#[test]
fn a_word_a_stamps_own_body_dropped_names_the_stamps_ceiling() {
    let refusal = derive_refusal(&format!(
        "principal ui {{ grants = [dom, subscribe]; }}\n{}",
        packaged_page(" under ui { grants = [subscribe]; }")
    ));
    assert!(
        refusal.ends_with(
            "so add `dom` to this stamp's ceiling — `ui` holds it, and this stamp's body \
             hands down less"
        ),
        "{refusal}"
    );
}

/// The same discrimination on the reach axis, per family: `ui` covers the
/// entry and the stamp's own `acl` line does not, so the line to widen is the
/// stamp's.
#[test]
fn reach_a_stamps_own_body_dropped_names_the_stamps_ceiling() {
    let refusal = derive_refusal(&format!(
        "{PACKAGED}component Panel {{ {} in messages; }}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       acl subscribe [exact out, prefix \"brenn:site.house.\", prefix \"brenn:site.shed.\"];\n\
         \x20       new panel: Panel {{ grants = [dom]; in messages <- out; }}\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         principal ui {{ grants = [dom, subscribe]; acl subscribe [prefix \"brenn:site.\"]; }}\n\
         new demo: Page(slug = \"demo\") under ui {{ acl subscribe [prefix \"brenn:site.house.\"]; }}\n",
        processor_header("dom"),
    ));
    assert!(
        refusal.ends_with(
            "add `acl subscribe [prefix \"brenn:site.shed.\"];` to this stamp's ceiling if that \
             reach is wanted — `ui` holds it, and this stamp's body hands down less"
        ),
        "{refusal}"
    );
}

/// A word enters a chain at its root and a child can only narrow, so a word
/// missing from `ui` is missing from everything `ui` is under too, and the
/// suggestion names them.
#[test]
fn a_word_beyond_a_chain_names_every_principal_it_must_reach() {
    let refusal = derive_refusal(&two_arrangements(
        "principal site { grants = [log, subscribe]; }\n\
         principal ui under site { grants = [subscribe]; }\n\
         new logger: Logger(slug = \"logger\") under site;\n\
         new demo: Page(slug = \"demo\") under ui;\n",
    ));
    assert!(
        refusal.ends_with("so add `dom` to `ui`, and to the principals it is under: `site`"),
        "{refusal}"
    );
}

/// Reach the arrangement writes for itself beyond its own channels is refused
/// at the author's statement, with the stamp as the related site and the line
/// the deployer would have to write.
#[test]
fn reach_beyond_the_ceiling_is_refused_at_the_authors_statement() {
    let source = format!(
        "{PACKAGED}component Panel {{ {} in messages; }}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       acl subscribe [exact out, prefix \"brenn:house.\"];\n\
         \x20       new panel: Panel {{ grants = [dom]; in messages <- out; }}\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         new demo: Page(slug = \"demo\") {{ grants = [dom, subscribe]; }}\n",
        processor_header("dom"),
    );
    let errors = derive_errors(&source);
    assert_eq!(
        errors[0].message,
        "`acl subscribe [prefix \"brenn:house.\"]` on `demo.page` reaches beyond what `Page` from \
         `@fixtures` declares or was handed, and the stamp `demo` consents to none of it — \
         add `acl subscribe [prefix \"brenn:house.\"];` to the stamp if that reach is wanted"
    );
    assert_eq!(errors[0].line_col(), at(&source, "brenn:house."));
    assert_eq!(errors[0].related[0].0, "stamped here");
}

/// A prefix over a handed channel does not hold: the deployer handed one
/// address, and a prefix reaches addresses they did not.
#[test]
fn a_prefix_over_a_handed_channel_is_refused() {
    let refusal = derive_refusal(&format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Loop(source: Channel) {{\n\
         \x20   new consume: Sink {{\n\
         \x20       grants = [];\n\
         \x20       acl subscribe [prefix \"ephemeral:bar.\"];\n\
         \x20       in messages <- source;\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         channel bar at \"ephemeral:bar.in\" {{ push_depth = 4; retain_depth = 16; }}\n\
         new demo: Loop(source = bar);\n",
        processor_header(""),
    ));
    assert!(
        refusal.starts_with(
            "`acl subscribe [prefix \"ephemeral:bar.\"]` on `demo.consume` reaches beyond what \
             `Loop` from `@fixtures` declares or was handed"
        ),
        "{refusal}"
    );
}

/// A binding to a `webhook:` literal reaches an endpoint the deployer declared
/// and did not hand in, so it is reach the ceiling has to cover.
#[test]
fn a_binding_to_an_endpoint_is_reach_the_ceiling_covers() {
    let arrangement = format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Hook() {{\n\
         \x20   new consume: Sink {{ grants = []; in messages <- \"webhook:alice-inbox\"; }}\n\
         }}\n{PACKAGED}\
         webhook alice_inbox {{ slug = \"alice-inbox\"; }}\n\
         new demo: Hook(){}\n",
        processor_header(""),
        "{BODY}",
    );
    let refusal = derive_refusal(&arrangement.replace("{BODY}", ";"));
    assert!(
        refusal.starts_with("`acl subscribe [endpoint \"webhook:alice-inbox\"]` on `demo.consume`"),
        "{refusal}"
    );
    let config = derived(&arrangement.replace(
        "{BODY}",
        " { acl subscribe [endpoint \"webhook:alice-inbox\"]; }",
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
}

/// A `grant` the deployer writes at top level is consent already given, so it
/// is not reach the stamp confers — the arrangement passes with nothing
/// written.
#[test]
fn a_deployer_grant_into_a_subtree_is_not_conferred() {
    let config = derived(&format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Loop() {{\n\
         \x20   channel out at \"ephemeral:demo.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   new consume: Sink {{ grants = []; in messages <- out; }}\n\
         }}\n{PACKAGED}\
         channel wide at \"ephemeral:wide\" {{ push_depth = 4; retain_depth = 16; }}\n\
         new demo: Loop();\n\
         grant demo.consume subscribe exact wide;\n",
        processor_header(""),
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
    assert_eq!(config.consumers[0].acl.ephemeral_subscribe.len(), 2);
}

/// The same reach written *inside* the arrangement is the author's, and it is
/// conferred: an assembly widening an entity is reach the stamp hands out.
#[test]
fn a_grant_inside_an_arrangement_is_conferred() {
    let source = format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Loop(target: Agent) {{\n\
         \x20   channel out at \"ephemeral:demo.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   new consume: Sink {{ grants = []; in messages <- out; }}\n\
         \x20   grant target subscribe prefix \"ephemeral:demo.\";\n\
         }}\n{PACKAGED}\
         agent Assistant() {{ name = \"Assistant\"; grants = [subscribe]; }}\n\
         new alice: Assistant();\n\
         new demo: Loop(target = alice){};\n",
        processor_header(""),
        "{BODY}",
    );
    let refusal = derive_refusal(&source.replace("{BODY}", ""));
    assert!(
        refusal.starts_with("`acl subscribe [prefix \"ephemeral:demo.\"]` on `alice`"),
        "{refusal}"
    );
    let config =
        derived(&source.replace("{BODY}", " { acl subscribe [prefix \"ephemeral:demo.\"]; }"));
    assert_eq!(config.resolved.stamps.len(), 1);
}

/// `under p;` with no body is how a stamp says "exactly `p`": the ceiling is
/// the principal's authority unchanged, and there is no narrowing to ask about.
#[test]
fn a_stamp_under_a_principal_with_no_body_holds_exactly_it() {
    let config = derived(&format!(
        "principal ui {{ grants = [dom, subscribe]; }}\n{}",
        packaged_page(" under ui;")
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
    let stamp = &config.resolved.stamps[0];
    assert_eq!(
        stamp.under.as_ref().map(|p| p.dotted()).as_deref(),
        Some("ui")
    );
    assert!(!stamp.wrote_body);
}

/// A body under a principal narrows it, and what the arrangement confers is
/// held to the narrowed ceiling rather than to the principal.
#[test]
fn a_body_under_a_principal_narrows_what_the_arrangement_may_hold() {
    // `log` is what the page's ceiling narrows away, and the second
    // arrangement is what keeps it from being dead config: a principal wider
    // than one arrangement is exactly what makes it worth declaring.
    let tree = |body: &str| {
        two_arrangements(&format!(
            "principal ui {{ grants = [dom, log, subscribe]; }}\n\
             new logger: Logger(slug = \"logger\") under ui;\n\
             new demo: Page(slug = \"demo\") under ui{body}\n"
        ))
    };
    let config = derived(&tree(" { grants = [dom, subscribe]; }"));
    assert_eq!(config.resolved.stamps.len(), 2);
    // The same narrowing with the word the arrangement needs taken away is the
    // refusal, and it names the ceiling rather than the principal above it.
    let refusal = derive_refusal(&tree(" { grants = [subscribe]; }"));
    assert!(
        refusal.contains("confers `dom` on `demo.page.panel`"),
        "{refusal}"
    );
}

/// `alert` is one word across both grant vocabularies, so one ceiling word
/// covers a consumer's alert capability and a surface's alert attach right at
/// once — the deployer consents to being paged once, not twice.
#[test]
fn one_alert_word_covers_both_vocabularies() {
    let config = derived(&format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Both(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{ slug = slug; grants = [subscribe, alert]; acl subscribe [exact out]; }}\n\
         \x20   new consume: Sink {{ grants = [alert]; in messages <- out; }}\n\
         }}\n{PACKAGED}\
         new demo: Both(slug = \"demo\") {{ grants = [alert, subscribe]; }}\n",
        processor_header("alert"),
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
}

/// An assembly may `new` an agent, and the agent's stated words count toward
/// what the stamp confers like any other entity's.
#[test]
fn an_agents_words_count_toward_what_a_stamp_confers() {
    let arrangement = "\
agent Assistant() {
    name = \"Assistant\";
    slug = \"desk-pa\";
    grants = [pwa_push];
}

assembly Desk() {
    new pa: Assistant();
}
";
    let refusal = derive_refusal(&format!(
        "{arrangement}new desk: Desk() {{ grants = []; }}\n"
    ));
    assert!(
        refusal.contains("confers `pwa_push` on `desk.pa`"),
        "{refusal}"
    );
    let config = derived(&format!(
        "{arrangement}new desk: Desk() {{ grants = [pwa_push]; }}\n"
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
}

// ── nested stamps ────────────────────────────────────────────────────────────
//
// Two packaged modules, the outer arrangement stamping the inner. The outer
// ceiling is a statement about the whole subtree, so what the inner
// arrangement confers is counted against it whether or not the inner `new`
// records a stamp of its own.

/// The inner arrangement: a surface with one placed instance holding one
/// capability.
const INNER: &str = "\
component Panel { abi = processor; requires = [dom]; in messages; }

assembly Inner(slug: String) {
    channel out at f\"ephemeral:{slug}.out\" { push_depth = 4; retain_depth = 16; }
    surface page {
        slug = slug;
        grants = [subscribe];
        new panel: Panel { grants = [dom]; in messages <- out; }
    }
}
";

/// The outer arrangement: one entity of its own holding a word the inner
/// arrangement does not need, so a nested body narrowing the enclosing ceiling
/// narrows something real, and the inner stamp written with whatever the case
/// writes.
fn outer(nested: &str) -> String {
    format!(
        "use @inner::*;\n\n\
         component Ping {{ abi = processor; requires = [alert]; in messages; }}\n\
         \n\
         assembly Outer() {{\n\
         \x20   channel tick at \"ephemeral:outer.tick\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   new ping: Ping {{ grants = [alert]; in messages <- tick; }}\n\
         \x20   new page: Inner(slug = \"demo\"){nested}\n\
         }}\n"
    )
}

/// A nested `new` with nothing written records no stamp of its own, so the
/// entities it emits belong to the deployer's boundary and its one ceiling
/// covers the whole expansion.
#[test]
fn a_bare_nested_stamp_is_covered_by_the_outer_ceiling() {
    let config = derived_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, dom, subscribe]; }\n",
        ),
        ("@outer", &outer(";")),
        ("@inner", INNER),
    ]);
    assert_eq!(config.resolved.stamps.len(), 1);
    assert_eq!(config.resolved.stamps[0].handle.dotted(), "demo");
}

/// And the outer ceiling is what refuses it: a word the inner arrangement
/// holds and the outer stamp does not cover is reported at the outer stamp.
#[test]
fn the_outer_ceiling_refuses_what_the_inner_arrangement_confers() {
    let refusals = derive_refusals_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, subscribe]; }\n",
        ),
        ("@outer", &outer(";")),
        ("@inner", INNER),
    ]);
    assert_eq!(refusals.len(), 1);
    assert!(
        refusals[0].contains("confers `dom` on `demo.page.page.panel`"),
        "{}",
        refusals[0]
    );
}

/// An author narrowing a nested arrangement is the doctrine in the author's
/// hands: the nested body records a stamp, its own subtree is held to it, and
/// the enclosing ceiling still bounds the whole.
#[test]
fn a_nested_author_body_records_a_stamp_and_binds_its_subtree() {
    let config = derived_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, dom, subscribe]; }\n",
        ),
        ("@outer", &outer(" { grants = [dom, subscribe]; }")),
        ("@inner", INNER),
    ]);
    let stamps: Vec<String> = config
        .resolved
        .stamps
        .iter()
        .map(|stamp| stamp.handle.dotted())
        .collect();
    assert_eq!(stamps, ["demo", "demo.page"]);
    assert_eq!(
        config.resolved.stamps[1].parent,
        Some(brenn_dsl::resolved::StampId(0))
    );
    assert!(config.resolved.stamps[1].packaged_site);
}

/// A nested body cannot widen the ceiling it arrives inside: it narrows the
/// enclosing one, and a word the enclosing ceiling does not hold is refused at
/// the nested stamp.
#[test]
fn a_nested_body_beyond_the_enclosing_ceiling_is_refused() {
    let refusals = derive_refusals_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, subscribe]; }\n",
        ),
        ("@outer", &outer(" { grants = [dom, subscribe]; }")),
        ("@inner", INNER),
    ]);
    assert!(
        refusals.iter().any(|refusal| refusal
            == "`dom` is not a word the ceiling on the stamp `demo` holds, which the stamp \
                `demo.page` is under: a stamp's ceiling holds no more than what it is under, \
                and everything a chain delegates is written at its root"),
        "{refusals:?}"
    );
}

/// An author's own attenuation that the nested arrangement exceeds is the
/// author's bug, and the refusal says so: no line the deployer writes answers
/// a ceiling written in the arrangement's own text.
#[test]
fn a_refusal_at_a_packaged_stamp_names_the_author() {
    let refusals = derive_refusals_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, dom, subscribe]; }\n",
        ),
        ("@outer", &outer(" { grants = [subscribe]; }")),
        ("@inner", INNER),
    ]);
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].contains("confers `dom` on `demo.page.page.panel`"),
        "{}",
        refusals[0]
    );
    assert!(
        refusals[0].contains("that line is the author's: this stamp is the arrangement's own text"),
        "{}",
        refusals[0]
    );
}

/// A dead ceiling line an author wrote is the author's to delete: no
/// deployment that stamps the bundle can edit the module the line is in, so the
/// refusal says whose text it is, as the fit rule does.
#[test]
fn dead_ceiling_text_in_packaged_text_names_the_author() {
    // The shared `outer` fixture holds one word beyond the inner arrangement's,
    // which a nested body cannot both narrow and hold dead text in. Two words
    // beyond is what leaves room for one of each.
    let module = "use @inner::*;\n\n\
                  component Ping { abi = processor; requires = [alert, log]; in messages; }\n\
                  \n\
                  assembly Outer() {\n\
                  \x20   channel tick at \"ephemeral:outer.tick\" \
                  { push_depth = 4; retain_depth = 16; }\n\
                  \x20   new ping: Ping { grants = [alert, log]; in messages <- tick; }\n\
                  \x20   new page: Inner(slug = \"demo\") { grants = [alert, dom, subscribe]; }\n\
                  }\n";
    let refusals = derive_refusals_tree(&[
        (
            "",
            "use @outer::*;\n\nnew demo: Outer() { grants = [alert, dom, log, subscribe]; }\n",
        ),
        ("@outer", module),
        ("@inner", INNER),
    ]);
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert_eq!(
        refusals[0],
        "`alert` caps nothing — no instance stamped by `Inner` from `@inner` holds it; a \
         ceiling word nothing reaches is dead config — and that line is the author's: this \
         stamp is the arrangement's own text"
    );
}

/// A `Principal` parameter is how an assembly reaches a principal, and a tree
/// assembly carrying one to a subject stamp is the demo's shape.
#[test]
fn a_principal_parameter_carries_a_ceiling_into_a_tree_assembly() {
    let config = derived_tree(&[
        (
            "",
            "use @inner::*;\n\n\
             principal demo_ui { grants = [dom, subscribe]; }\n\
             assembly Deployment(ui: Principal) {\n\
             \x20   new page: Inner(slug = \"demo\") under ui;\n\
             }\n\
             new deployment: Deployment(ui = demo_ui);\n",
        ),
        ("@inner", INNER),
    ]);
    assert_eq!(config.resolved.stamps.len(), 1);
    assert_eq!(
        config.resolved.stamps[0].under.as_ref().map(|p| p.dotted()),
        Some("demo_ui".to_string())
    );
}

/// A principal handed into a stamped arrangement is a bound of its own and
/// does not widen the ceiling it arrives inside: what the text says is what
/// the ceiling is, so the wider principal is refused rather than intersected.
///
/// The refusal is anchored at the `under` clause that chose the principal —
/// which for a nested stamp is the author's file — with the argument that
/// handed it in as a related site, because that is the other place the
/// arrangement can be given something narrower.
#[test]
fn a_handed_principal_wider_than_the_enclosing_ceiling_is_refused() {
    let holder = "use @inner::*;\n\nassembly Holder(ui: Principal) {\n\
                  \x20   new page: Inner(slug = \"demo\") under ui { grants = [dom]; }\n\
                  }\n";
    let errors = derive_errors_tree(&[
        (
            "",
            "use @holder::*;\n\n\
             principal wide { grants = [dom, page-dom, subscribe]; }\n\
             new demo: Holder(ui = wide) { grants = [dom, subscribe]; }\n",
        ),
        ("@holder", holder),
        ("@inner", INNER),
    ]);
    let refusal = errors
        .iter()
        .find(|error| {
            error
                .message
                .starts_with("`wide` holds the word `page-dom`")
        })
        .unwrap_or_else(|| panic!("{:?}", messages(&errors)));
    assert_eq!(
        refusal.message,
        "`wide` holds the word `page-dom`, which the ceiling on the stamp `demo` does \
         not: a principal handed into a stamped arrangement is a bound of its own, and \
         the ceiling it arrives inside is not widened by one"
    );
    assert_eq!(refusal.line_col(), at(holder, "ui { grants"));
    let related: Vec<&str> = refusal
        .related
        .iter()
        .map(|(what, _)| what.as_str())
        .collect();
    assert_eq!(
        related,
        [
            "held here",
            "handed in here",
            "the enclosing ceiling is written here"
        ]
    );
}

// ── what the fold counts, and what it does not ───────────────────────────────

/// A top-level consumer inside a packaged arrangement holds capabilities the
/// stamp has to cover, exactly as a surface-placed instance does — the
/// `DemoLoop` shape, whose words reach the fold through the consumer vector
/// rather than through a surface.
#[test]
fn a_consumers_word_beyond_the_ceiling_is_refused() {
    let refusal = derive_refusal(&packaged_loop(";"));
    assert!(
        refusal.contains("confers `ports` on `demo.consume`"),
        "{refusal}"
    );
    assert!(
        refusal.ends_with("so write it — `grants = [ports];`"),
        "{refusal}"
    );
}

/// Confined reach is the serving host's to authorize, not the deployment's: a
/// ceiling refuses a line in a confined family, so a confined entry the
/// arrangement derives cannot be reach the ceiling has to cover — the refusal
/// would name a line that is itself refused. Every real chrome binds
/// `local:brenn/theme`, so this is the shape that has to compile.
#[test]
fn a_confined_binding_needs_no_ceiling_line() {
    let config = derived(&format!(
        "{PACKAGED}component Panel {{ {} in messages; in theme; }}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       new panel: Panel {{\n\
         \x20           grants = [dom];\n\
         \x20           in messages <- out;\n\
         \x20           in theme <- \"local:brenn/theme\" {{ push_depth = 1; }}\n\
         \x20       }}\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         new demo: Page(slug = \"demo\") {{ grants = [dom, subscribe]; }}\n",
        processor_header("dom"),
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
    // The confined entry is derived and held, which is what makes its absence
    // from the fit check a decision rather than an accident.
    assert_eq!(config.resolved.surfaces[0].components[0].bindings.len(), 2);
    assert_eq!(
        patterns(&config.surface_components[0][0].acl.local_subscribe),
        ["brenn/theme"]
    );
}

/// The exact arm of the line a reach refusal writes: an address no declared
/// channel holds, which is the only shape the arrangement can have written —
/// a literal naming a declared channel is refused as a second spelling of its
/// handle, and a channel of the subtree's own or one handed in is default
/// reach. Following the suggestion compiles, which is the whole claim the
/// refusal makes.
#[test]
fn the_exact_arm_of_a_reach_suggestion_is_followable() {
    let arrangement = format!(
        "{PACKAGED}component Panel {{ {} in messages; }}\n\
         \n\
         assembly Page(slug: String) {{\n\
         \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
         \x20   surface page {{\n\
         \x20       slug = slug;\n\
         \x20       grants = [subscribe];\n\
         \x20       acl subscribe [exact out, exact \"brenn:elsewhere\"];\n\
         \x20       new panel: Panel {{ grants = [dom]; in messages <- out; }}\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         new demo: Page(slug = \"demo\") {{ grants = [dom, subscribe];{} }}\n",
        processor_header("dom"),
        "{LINE}",
    );
    let refusal = derive_refusal(&arrangement.replace("{LINE}", ""));
    assert_eq!(
        refusal,
        "`acl subscribe [exact \"brenn:elsewhere\"]` on `demo.page` reaches beyond what `Page` \
         from `@fixtures` declares or was handed, and the stamp `demo` consents to none of it \
         — add `acl subscribe [exact \"brenn:elsewhere\"];` to the stamp if that reach is wanted"
    );
    let config =
        derived(&arrangement.replace("{LINE}", " acl subscribe [exact \"brenn:elsewhere\"];"));
    assert_eq!(config.resolved.stamps.len(), 1);
}

// ── mqtt reach in a ceiling ──────────────────────────────────────────────────
//
// The reach that leaves the machine. Both transport families compare by
// equality — a client for a sink, a client and a filter for a subscription —
// so a ceiling names the same spelling the arrangement wrote and nothing about
// a wildcard is arithmetic.

/// A packaged bridge holding both mqtt planes: a subscription over a wildcard
/// filter and a sink with a budget of its own.
fn packaged_bridge(body: &str) -> String {
    format!(
        "{PACKAGED}component Sink {{ {} in messages; }}\n\
         \n\
         assembly Bridge(source: Channel) {{\n\
         \x20   new consume: Sink {{\n\
         \x20       grants = [mqtt];\n\
         \x20       acl subscribe [exact source, topic_filter \"mqtt:bob_hub:house/#\"];\n\
         \x20       acl publish [client \"mqtt:bob_hub\" {{ publish_capacity = 4 }}];\n\
         \x20       in messages <- source;\n\
         \x20   }}\n\
         }}\n{PACKAGED}\
         {INGRESS}\
         channel bar at \"ephemeral:bar\" {{ push_depth = 4; retain_depth = 16; }}\n\
         new demo: Bridge(source = bar){body}\n",
        processor_header("mqtt"),
    )
}

/// The ceiling names the same client and the same filter, and the budget tail
/// the arrangement wrote is not part of what is compared: a ceiling caps reach,
/// and the sink's window belongs on the entry that mints it.
#[test]
fn a_ceiling_covers_conferred_mqtt_reach() {
    let config = derived(&packaged_bridge(
        " { grants = [mqtt]; acl subscribe [topic_filter \"mqtt:bob_hub:house/#\"]; \
         acl publish [client \"mqtt:bob_hub\"]; }",
    ));
    assert_eq!(config.resolved.stamps.len(), 1);
    assert_eq!(config.consumers[0].acl.mqtt_publish.len(), 1);
    assert_eq!(config.consumers[0].acl.mqtt_subscribe.len(), 1);
}

/// A filter that differs is not narrower, it is other: there is no arithmetic
/// over a topic filter, so a ceiling that names one filter consents to that
/// filter and no other.
#[test]
fn a_ceiling_whose_topic_filter_differs_is_refused() {
    let refusal = derive_refusal(&packaged_bridge(
        " { grants = [mqtt]; acl subscribe [topic_filter \"mqtt:bob_hub:house/lamp\"]; \
         acl publish [client \"mqtt:bob_hub\"]; }",
    ));
    assert_eq!(
        refusal,
        "`acl subscribe [topic_filter \"mqtt:bob_hub:house/#\"]` on `demo.consume` reaches \
         beyond what `Bridge` from `@fixtures` declares or was handed, and the stamp `demo` \
         consents to none of it — add `acl subscribe [topic_filter \"mqtt:bob_hub:house/#\"];` \
         to the stamp if that reach is wanted"
    );
}

// ── a refusal in consent text stops there ────────────────────────────────────
//
// Ceiling text the compiler could not read is not the ceiling the document
// states, so nothing under it is judged against it. The refusal already
// reported is the answer; a second one per conferred word would bury it.

/// A mistyped word in a stamp's own ceiling is one message, not one per word
/// the arrangement holds.
#[test]
fn a_refused_ceiling_word_suppresses_the_fit_check() {
    assert_eq!(
        derive_refusal(&packaged_page(" { grants = [dom, chrome, subscribe]; }")),
        "`chrome` is not a grant word, so it caps nothing; a ceiling names `alert`, \
         `config`, `dom`, `dynamic_subscribe`, `ephemeral_publish`, `ephemeral_subscribe`, \
         `log`, `mqtt`, `page-dom`, `ports`, `publish`, `pwa_push`, `store`, `subscribe`, \
         `takeover` or `tools`"
    );
}

/// And a mistyped word in a *principal* is one message too, however many
/// stamps and child principals are under it. A root principal that was refused
/// holds the operator's empty authority, so judging anything against it would
/// report every word every arrangement under it confers, each suggesting a fix
/// to a principal that already holds the word.
#[test]
fn a_refused_principal_body_suppresses_everything_under_it() {
    assert_eq!(
        derive_refusals(&two_arrangements(
            "principal ui { grants = [dom, subscribe, prots]; }\n\
             principal narrow under ui { grants = [dom]; }\n\
             new demo: Page(slug = \"demo\") under ui;\n\
             new other: Page(slug = \"other\") under narrow;\n",
        ))
        .len(),
        1
    );
}

/// The same promise on the shape a packaged deployment tree makes normal: a
/// nested stamp whose `under` is a `Principal` parameter, inside an enclosing
/// stamp whose own ceiling was refused. The enclosing ceiling then holds what
/// nobody wrote, so the handed principal is not compared against it — a
/// comparison that would report the deployer's own ceiling words back at them
/// as reach the ceiling does not hold, each with a fix that changes nothing.
#[test]
fn a_refused_enclosing_ceiling_suppresses_the_handed_principals_bound() {
    let holder = "use @inner::*;\n\nassembly Holder(ui: Principal) {\n\
                  \x20   new page: Inner(slug = \"demo\") under ui { grants = [dom]; }\n\
                  }\n";
    let refusals = derive_refusals_tree(&[
        (
            "",
            "use @holder::*;\n\n\
             principal wide { grants = [dom, subscribe]; }\n\
             new demo: Holder(ui = wide) { grants = [dom, subscribe, prots]; }\n",
        ),
        ("@holder", holder),
        ("@inner", INNER),
    ]);
    assert!(
        refusals[0].starts_with("`prots` is not a grant word"),
        "{refusals:?}"
    );
    assert_eq!(refusals.len(), 1, "{refusals:?}");
}

/// A line that reaches *further* than the axis it replaces is a widening, and
/// exactly one message says so. The narrows-nothing rule is mutual coverage in
/// both directions for this reason: a one-directional test fires here too, and
/// telling the reader that a line which holds strictly more is a second
/// spelling of what it replaced contradicts the excess refusal beside it.
#[test]
fn a_reach_line_that_widens_is_refused_once() {
    let refusals = derive_refusals(&chain(&[
        Link {
            body: "    acl subscribe [prefix \"brenn:house.\", prefix \"brenn:shed.\"];\n",
            words: "",
            reach: "prefix \"brenn:house.\", prefix \"brenn:shed.\"",
        },
        Link {
            body: "    acl subscribe [prefix \"brenn:house.\", prefix \"brenn:shed.\", \
                   prefix \"brenn:barn.\"];\n",
            words: "",
            reach: "",
        },
    ]));
    assert!(
        refusals[0].starts_with("`barn.` is not reach `site` holds"),
        "{refusals:?}"
    );
    assert_eq!(refusals.len(), 1, "{refusals:?}");
}

// ── consent text that consents to nothing ──────────────────────────
//
// The inverse direction, and the half that makes a stamp's ceiling a statement
// of consent rather than a bound: a word or a line that caps nothing is
// authority the deployment stated and the arrangement never asked for. Left
// standing it outlives the bundle revision that needed it, and the next pin
// bump that re-introduces that authority needs no new consent.

/// A word no instance in the subtree holds.
#[test]
fn a_ceiling_word_the_arrangement_does_not_hold_is_dead() {
    let source = packaged_page(" { grants = [dom, subscribe, tools]; }");
    let errors = derive_errors(&source);
    assert_eq!(
        errors[0].message,
        "`tools` caps nothing — no instance stamped by `Page` from `@fixtures` holds it; \
         a ceiling word nothing reaches is dead config"
    );
    // At the word, which is the text to delete.
    assert_eq!(errors[0].line_col(), at(&source, "tools]"));
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
}

/// The description shape with a `grants` line written anyway: an arrangement
/// that holds no capability is stamped with no list at all, and an empty one
/// reads as though the deployment had something in mind.
#[test]
fn an_empty_grants_line_over_a_confer_nothing_arrangement_is_dead() {
    assert_eq!(
        derive_refusal(&format!(
            "{PACKAGED}assembly Describe(slug: String) {{\n\
             \x20   channel geometry at f\"brenn:surface.{{slug}}.geometry\" {{ push_depth = 1; retain_depth = 1; standing_retain_depth = 1; }}\n\
             }}\n{PACKAGED}\
             new demo: Describe(slug = \"demo\") {{ grants = []; }}\n"
        )),
        "no instance stamped by `Describe` from `@fixtures` holds a capability, so this \
         `grants` line caps nothing; the stamp of an arrangement that holds no capability \
         writes no `grants` line"
    );
}

/// An `acl` line in a family the arrangement reaches nowhere.
#[test]
fn a_ceiling_line_in_a_family_the_arrangement_never_reaches_is_dead() {
    let source = packaged_loop(" { grants = [ports]; acl publish [prefix \"brenn:demo.\"]; }");
    let errors = derive_errors(&source);
    assert_eq!(
        errors[0].message,
        "this `acl publish` line caps nothing `Loop` from `@fixtures` reaches in \
         `brenn_publish` beyond its own channels and what it was handed; a ceiling line \
         nothing needs is dead config"
    );
    assert_eq!(errors[0].line_col(), at(&source, "publish [prefix"));
    assert_eq!(errors.len(), 1, "{:?}", messages(&errors));
}

/// And one that re-states reach the stamp already consented to by stamping the
/// arrangement that declares the channel. Default reach is the reason the
/// common case writes no `acl` line at all, so a line that only covers it is
/// consent given twice.
#[test]
fn a_ceiling_line_over_the_arrangements_own_channel_is_dead() {
    let refusal = derive_refusal(&packaged_loop(
        " { grants = [ports]; acl publish [prefix \"ephemeral:demo.\"]; }",
    ));
    assert!(
        refusal.starts_with("this `acl publish` line caps nothing"),
        "{refusal}"
    );
}

/// A stamp whose ceiling text was refused for what it says is judged in the
/// inverse direction by nothing. The excess is the one message; a line that
/// reaches beyond the principal and covers nothing the arrangement asked for
/// would otherwise draw two refusals for one line, and the fix for the excess
/// is what decides whether the line is really dead.
#[test]
fn a_refused_stamp_ceiling_is_not_also_judged_dead() {
    let refusals = derive_refusals(&two_arrangements(
        "principal ui { grants = [dom, subscribe]; }\n\
         new demo: Page(slug = \"demo\") under ui { acl publish [prefix \"brenn:x.\"]; }\n",
    ));
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].contains("is not reach `ui` holds"),
        "{refusals:?}"
    );
}

/// A principal handed to a class that never writes `under` with it was
/// delegated all the same: the dead text is the parameter the class dropped, in
/// a file that may be the author's, and the deployer's declaration is not where
/// to report it. So no refusal at the declaration.
#[test]
fn a_handed_principal_a_class_never_uses_is_not_refused_at_the_declaration() {
    let config = derived_tree(&[
        (
            "",
            "use @inner::*;\n\n\
             principal demo_ui { grants = [dom, subscribe]; }\n\
             assembly Deployment(ui: Principal) {\n\
             \x20   new page: Inner(slug = \"demo\") { grants = [dom, subscribe]; }\n\
             }\n\
             new deployment: Deployment(ui = demo_ui);\n",
        ),
        ("@inner", INNER),
    ]);
    let handed: Vec<String> = config
        .resolved
        .handed_principals
        .iter()
        .map(|handle| handle.dotted())
        .collect();
    assert_eq!(handed, ["demo_ui"]);
    assert!(
        config
            .resolved
            .stamps
            .iter()
            .all(|stamp| stamp.under.is_none()),
        "no stamp is under it, which is what the refusal would have read"
    );
}

/// A stamp with no body of its own states nothing, so there is nothing that
/// could be dead — the shape every description stamp has.
#[test]
fn a_stamp_with_no_body_is_judged_in_neither_direction() {
    let config = derived(&two_arrangements(
        "principal ui { grants = [dom, log, subscribe]; }\n\
         new logger: Logger(slug = \"logger\") under ui;\n\
         new demo: Page(slug = \"demo\") under ui;\n",
    ));
    assert_eq!(config.resolved.stamps.len(), 2);
    assert!(config.resolved.stamps.iter().all(|stamp| !stamp.wrote_body));
}

/// A principal is judged by the union of everything under it, which is what
/// makes one worth declaring rather than writing a ceiling per stamp: a word
/// one arrangement narrows away is live text as long as another under the same
/// principal holds it.
#[test]
fn a_word_one_arrangement_under_a_principal_holds_is_live() {
    let tree = |stamps: &str| {
        two_arrangements(&format!(
            "principal ui {{ grants = [dom, log, subscribe]; }}\n{stamps}"
        ))
    };
    let both = "new logger: Logger(slug = \"logger\") under ui;\n\
                new demo: Page(slug = \"demo\") under ui { grants = [dom, subscribe]; }\n";
    let config = derived(&tree(both));
    assert_eq!(config.resolved.stamps.len(), 2);
    // Take the arrangement that needs `log` away and the word caps nothing:
    // the union is over what is there, not over what the deployment meant.
    let refusal = derive_refusal(&tree(
        "new demo: Page(slug = \"demo\") under ui { grants = [dom, subscribe]; }\n",
    ));
    assert!(refusal.starts_with("`log` caps nothing"), "{refusal}");
}

/// A word on a principal that no arrangement under it holds is dead config,
/// refused where the deployment wrote it — the same rule a stamp's own ceiling
/// is held to, over the union instead of over one subtree.
#[test]
fn a_principal_word_nothing_under_it_holds_is_refused() {
    let source = two_arrangements(
        "principal ui { grants = [dom, subscribe, tools]; }\n\
         new demo: Page(slug = \"demo\") under ui;\n",
    );
    let errors = derive_errors(&source);
    assert_eq!(
        messages(&errors),
        [
            "`tools` caps nothing — no arrangement under `ui` holds it; a ceiling word \
          nothing reaches is dead config"
        ]
    );
    assert_eq!(errors[0].line_col(), at(&source, "tools"));
}

/// The same for reach: a line in a family nothing under the principal reaches
/// beyond its own channels caps nothing, and is refused at the plane word.
#[test]
fn a_principal_line_nothing_under_it_needs_is_refused() {
    let source = two_arrangements(
        "principal ui {\n\
         \x20   grants = [dom, subscribe];\n\
         \x20   acl publish [prefix \"brenn:house.\"];\n\
         }\n\
         new demo: Page(slug = \"demo\") under ui;\n",
    );
    let errors = derive_errors(&source);
    assert_eq!(
        messages(&errors),
        [
            "this `acl publish` line caps nothing any arrangement under `ui` reaches in \
          `brenn_publish` beyond its own channels and what it was handed; a ceiling line \
          nothing needs is dead config"
        ]
    );
    assert_eq!(errors[0].line_col(), at(&source, "publish [prefix"));
}

/// An empty `grants` list on a principal nothing under which holds a
/// capability: the list is a statement, and the statement is that the
/// deployment had something in mind.
#[test]
fn an_empty_grants_list_on_a_principal_that_delegates_none_is_refused() {
    assert_eq!(
        derive_refusal(&chain(&[Link {
            body: "    grants = [];\n    acl subscribe [prefix \"brenn:house.\"];\n",
            words: "",
            reach: "prefix \"brenn:house.\"",
        }])),
        "no arrangement under `site` holds a capability, so this `grants` line caps \
         nothing; a principal that delegates no capability writes no `grants` line"
    );
}

/// A principal nothing is under delegates nothing, which makes every word and
/// line it writes text about nothing at all. Refused where it is declared, as
/// an unused `uuid_pin` is.
#[test]
fn a_principal_nothing_is_under_is_refused() {
    let source = "principal ui {\n    grants = [dom];\n}\n";
    let errors = derive_errors(source);
    assert_eq!(
        messages(&errors),
        [
            "`ui` delegates to nothing: no stamp is under it and no principal is declared \
          under it, so the authority it writes reaches no arrangement"
        ]
    );
    assert_eq!(errors[0].line_col(), at(source, "ui {"));
}

/// A chain that reaches no arrangement is one message at its leaf, not one per
/// link: a principal with a child delegates through it, so the leaf is where
/// the whole chain's failure to reach anything is reported. Reporting every
/// ancestor, and then every word each of them wrote, is one mistake fanned out
/// over a document.
#[test]
fn a_chain_that_reaches_nothing_is_refused_at_its_leaf() {
    assert_eq!(
        derive_refusal(
            "principal site {\n    grants = [dom, log];\n}\n\
             principal ui under site {\n    grants = [dom];\n}\n",
        ),
        "`ui` delegates to nothing: no stamp is under it and no principal is declared \
         under it, so the authority it writes reaches no arrangement"
    );
}

/// A principal whose own text was refused is judged in the inverse direction by
/// nothing. The excess is the one message; reporting the words it wrote as dead
/// beside it would be the same mistake told twice, and the fix for the excess
/// is what decides whether any of them is really dead.
#[test]
fn a_refused_principal_body_is_not_also_judged_dead() {
    let refusals = derive_refusals(&two_arrangements(
        "principal ui { grants = [dom, log, subscribe]; }\n\
         principal narrow under ui { grants = [dom, subscribe, tools]; }\n\
         new logger: Logger(slug = \"logger\") under ui;\n\
         new demo: Page(slug = \"demo\") under narrow;\n",
    ));
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert!(
        refusals[0].starts_with("`tools` is not a word"),
        "{refusals:?}"
    );
}

// ── malformed consent text ───────────────────────────────────────────────────

/// A `grants` value that is not a word list is refused where it is written, and
/// the principal is withheld rather than half-read.
#[test]
fn a_malformed_grants_axis_on_a_principal_is_refused() {
    assert_eq!(
        derive_refusal("principal ui {\n    grants = \"dom\";\n}\n"),
        "expected a list of bare words, found a string"
    );
}

/// The same on a stamp, where the blast radius is larger: the `new` expands
/// nothing, so every entity the arrangement would have emitted is absent. One
/// message even so — a top-level `grant` naming a vanished entity does not
/// earn a second.
#[test]
fn a_malformed_grants_axis_on_a_stamp_ceiling_is_refused() {
    let source = format!(
        "{}channel wide at \"ephemeral:wide\" {{ push_depth = 4; retain_depth = 16; }}\n\
         grant demo.page subscribe exact wide;\n",
        packaged_page(" { grants = \"dom\"; }"),
    );
    assert_eq!(
        derive_refusal(&source),
        "expected a list of bare words, found a string"
    );
}

// ── cycle shapes ─────────────────────────────────────────────────────────────
//
// The cycle refusal is what makes the derive pass's ordering walk sound: a
// chain that does not bottom out is refused here, so the walk asserts it can
// order every chain rather than tolerating one it cannot.

/// The one-member cycle: a principal under itself.
#[test]
fn a_principal_under_itself_is_refused() {
    assert_eq!(
        derive_refusal("principal a under a {\n    grants = [dom];\n}\n"),
        "`a` is under `a`; a chain of principals bottoms out at the operator"
    );
}

/// Three members, which is the only shape that exercises the repetition in the
/// reading.
#[test]
fn a_three_member_cycle_reads_the_whole_chain() {
    assert_eq!(
        derive_refusal(
            "principal a under c {\n    grants = [dom];\n}\n\
             principal b under a {\n    grants = [dom];\n}\n\
             principal c under b {\n    grants = [dom];\n}\n",
        ),
        "`b` is under `a`, which is under `c`, which is under `b`; a chain of principals \
         bottoms out at the operator"
    );
}

/// A chain that *enters* a cycle from outside it is walked from every start,
/// and the promise is one refusal for the cycle rather than one per start.
#[test]
fn a_cycle_reached_from_a_tail_is_refused_once() {
    assert_eq!(
        derive_refusal(
            "principal c under b {\n    grants = [dom];\n}\n\
             principal b under a {\n    grants = [dom];\n}\n\
             principal a under b {\n    grants = [dom];\n}\n",
        ),
        "`a` is under `b`, which is under `a`; a chain of principals bottoms out at the \
         operator"
    );
}

// ── the `Principal` parameter's edges ────────────────────────────────────────

/// A principal is a top-level declaration, so nothing stamps one and reaching
/// under an instance handle for one names nothing.
#[test]
fn a_principal_argument_under_an_instance_handle_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{PACKAGED}assembly Held(slug: String) {{\n\
             \x20   channel out at f\"ephemeral:{{slug}}.out\" {{ push_depth = 4; retain_depth = 16; }}\n\
             }}\n\
             \n\
             assembly Pod() {{ new inner: Held(slug = \"in\"); }}\n\
             assembly Deployment(ui: Principal) {{ new held: Held(slug = \"d\") under ui; }}\n{PACKAGED}\
             new pod: Pod();\n\
             new deployment: Deployment(ui = pod.inner);\n"
        )),
        "parameter `ui` is a `Principal`; `pod.inner` is stamped by an instantiation, and a \
         principal is a top-level declaration"
    );
}

/// An `under` naming a segment under a principal names nothing, at a
/// declaration and at a stamp alike.
#[test]
fn under_reaching_under_a_principals_name_is_refused() {
    assert_eq!(
        derive_refusal(
            "principal site {\n    grants = [dom];\n}\n\
             principal ui under site.x {\n    grants = [dom];\n}\n",
        ),
        "`site` is not an instance, so `.x` names nothing"
    );
    assert_eq!(
        derive_refusal(&format!(
            "principal ui {{ grants = [dom, subscribe]; }}\n{}",
            packaged_page(" under ui.x;"),
        )),
        "`ui` is not an instance, so `.x` names nothing"
    );
}

/// An assembly hands its own `Principal` parameter to a nested assembly's,
/// which is the shape a real deployment tree has: one principal declared at the
/// root and passed down. The stamp at the bottom is checked against that
/// principal, which the refusal naming it is what proves.
#[test]
fn a_principal_parameter_forwards_through_a_nested_assembly() {
    let tree = |grants: &str| {
        format!(
            "use @inner::*;\n\n\
             principal demo_ui {{ grants = [{grants}]; }}\n\
             assembly Middle(ui: Principal) {{\n\
             \x20   new page: Inner(slug = \"demo\") under ui;\n\
             }}\n\
             assembly Deployment(ui: Principal) {{\n\
             \x20   new middle: Middle(ui = ui);\n\
             }}\n\
             new deployment: Deployment(ui = demo_ui);\n"
        )
    };
    let config = derived_tree(&[("", &tree("dom, subscribe")), ("@inner", INNER)]);
    assert_eq!(
        config.resolved.stamps[0].under.as_ref().map(|p| p.dotted()),
        Some("demo_ui".to_string())
    );
    // The word taken away is refused against the principal the chain forwarded,
    // not against an empty authority.
    let refusals = derive_refusals_tree(&[("", &tree("subscribe")), ("@inner", INNER)]);
    let refusal = only_refusal(&refusals);
    assert!(
        refusal.ends_with("so add `dom` to `demo_ui`, or stamp under a principal that holds it"),
        "{refusal}"
    );
}

/// The one message of a tree refused with exactly one.
fn only_refusal(refusals: &[String]) -> &str {
    match refusals {
        [one] => one,
        many => panic!("expected one refusal, found {many:?}"),
    }
}
