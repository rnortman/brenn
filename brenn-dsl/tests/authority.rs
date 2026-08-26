//! The authority model: which family every matcher lands in, which entity types
//! have that family, and what a matcher comes to once its scheme is stripped.
//!
//! Documents are written the way an operator writes them and the derived
//! authority is asserted on the far side. Channels, identity and the wire-kind
//! fold have their own suite.

mod support;

use brenn_dsl::derived::{DAclSet, DMatcher};
use brenn_dsl::diag::Diagnostic;
use brenn_dsl::{dom_any, processor_any};
use fltk_serde_core::Spanned;
use support::{at, derive_errors, derive_refusal, derive_refusals, derived, durable, nondurable};

// ── fixtures ─────────────────────────────────────────────────────────────────

/// The class every consumer fixture instantiates.
///
/// Every port here is `optional`: a fixture class exists so each case binds only
/// the ports the case is about, and a required port would answer every other
/// case with an unconnected-port refusal instead of the one it asked for. The
/// required-port contract itself is asserted in the resolve suite.
///
/// Its grant declarations are read the same way: `requires` is empty and
/// `optional` is every word this host admits, so each case grants what the case
/// is about without the spec fit answering it first. The fit contract itself is
/// asserted below.
const SINK: &str = concat!(
    "component Sink {\n",
    "    ",
    processor_any!(),
    "\n",
    "    optional out events;\n",
    "}\n",
);

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
    format!(
        "{SINK}new alice_sink: Sink {{\n    component_path = \"sink.wasm\";\n    \
         grants = [{grants}];\n{statements}}}\n"
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
         window is not an answer a network principal may be given"
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

/// A dom class with a port facing each way and one facing both. Every port is
/// `optional` for the reason `SINK` states.
const PANEL: &str = concat!(
    "component Panel {\n",
    "    ",
    dom_any!(),
    "\n",
    "    optional in messages;\n",
    "    optional out acks;\n",
    "    optional io tick;\n",
    "}\n",
);

/// A processor class a top-level instance is made of. Every port is `optional`
/// for the reason `SINK` states.
const RELAY: &str = concat!(
    "component Relay {\n",
    "    ",
    processor_any!(),
    "\n",
    "    optional in inbound;\n",
    "    optional out outbound;\n",
    "    optional io acks;\n",
    "}\n",
);

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
        "{PANEL}surface alice_desk {{\n    grants = [{grants}];\n{statements}    \
         new p1: Panel {{\n        grants = [{ports}];\n{bindings}    }}\n}}\n"
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
        "{PANEL}surface alice_desk {{\n    grants = [subscribe, publish];\n    \
         acl subscribe [prefix \"brenn:alice.\"];\n    acl publish [prefix \"brenn:alice.\"];\n    \
         new p1: Panel {{\n        grants = [{grants}];\n{body}    }}\n}}\n"
    )
}

/// A top-level instance granting the capabilities named and holding the
/// statements and bindings written into it.
fn relay_with(grants: &str, body: &str) -> String {
    format!(
        "{RELAY}new alice_relay: Relay {{\n    component_path = \"relay.wasm\";\n    \
         grants = [{grants}];\n{body}}}\n"
    )
}

/// A top-level instance whose lists demand no capability word.
fn relay(body: &str) -> String {
    relay_with("", body)
}

/// A component whose every port is inbound, for the cases that need two
/// positions on one ingress address.
const FAN_IN: &str = concat!(
    "component FanIn {\n",
    "    ",
    processor_any!(),
    "\n",
    "    optional in first;\n",
    "    optional in second;\n",
    "    optional in third;\n",
    "}\n",
);

/// A top-level `FanIn` instance holding the bindings written into it.
fn fan_in(body: &str) -> String {
    format!(
        "{FAN_IN}new alice_fan_in: FanIn {{\n    component_path = \"fan-in.wasm\";\n    \
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
        PANEL,
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
            consumer("takeover", ""),
            "`takeover` is a page capability; a top-level consumer has no page",
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
        derive_refusal(&consumer("subscribe", "")),
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
        derive_refusal(&format!(
            "{SINK}new alice_sink: Sink {{\n    component_path = \"sink.wasm\";\n}}\n"
        )),
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
                "{RELAY}new alice_relay: Relay {{\n    out outbound -> \"local:alice/out\";\n}}\n"
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
    let source =
        format!("{PANEL}surface alice_desk {{\n    grants = [];\n    new p1: Panel {{}}\n}}\n");
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
        assert_eq!(derive_refusal(&placed_panel(word, "")), expected);
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

/// A dom class needing one capability, and a surface holding one instance of it
/// with the grants named.
fn needy_panel(needs: &str, grants: &str) -> String {
    format!(
        "component Needy {{\n    abi = dom; {needs};\n    optional in messages;\n}}\n\
         surface alice_desk {{\n    grants = [];\n    \
         new p1: Needy {{\n        grants = [{grants}];\n    }}\n}}\n"
    )
}

/// A processor class needing one capability, and a top-level instance of it with
/// the grants named.
fn needy_sink(needs: &str, grants: &str) -> String {
    format!(
        "component Needy {{\n    abi = processor; {needs};\n    optional in inbound;\n}}\n\
         new alice_sink: Needy {{\n    component_path = \"needy.wasm\";\n    \
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
    for grants in ["", "log"] {
        let config = derived(&needy_panel("requires = []; optional = [log]", grants));
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
        "component Needy {\n    abi = dom; requires = [log, alert];\n    optional in messages;\n}\n",
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
        "component Needy {\n    abi = dom; requires = [ports];\n",
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
        "component Needy {\n    abi = dom; requires = []; optional = [takeover];\n",
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
         budget tunes the sink a component holds, and this principal publishes through a \
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
