//! Assembly expansion: what an instantiation stamps, and what it refuses.
//!
//! Every case goes through the I/O-free core, so a whole tree is spelled inline
//! and the resolved config is read back for the handles the expansion minted.

mod support;

use brenn_dsl::resolved::{ChanId, RChanRef, RMatcherVal, RValue, ResolvedConfig};
use support::{compile_tree, refusal, refusals, resolved, resolved_tree};

/// The dotted handles of every channel the config carries, in id order.
fn channels(config: &ResolvedConfig) -> Vec<String> {
    config
        .channels
        .iter()
        .map(|channel| channel.handle.dotted())
        .collect()
}

// ── what an instantiation stamps ─────────────────────────────────────────────

/// The acid test: one assembly, two instantiations, two disjoint entity sets.
const PODS: &str = "\
// ── packaged ──
component Panel { abi = dom; requires = []; in messages; }
// ── packaged ──

assembly Pod(slug: String, owner: Agent) {
    channel messages at f\"brenn:{slug}.in.p1.messages\";
    surface panel {
        slug = slug;
        grants = [subscribe];
        new view: Panel { in messages <- messages; }
    }
    grant owner subscribe prefix f\"brenn:{slug}.\";
}

agent Assistant(name: String) {
    slug = name;
    model = \"sonnet\";
}

new alice_pa: Assistant(name = \"alice-pa\");
new bob_pa: Assistant(name = \"bob-pa\");
new alice: Pod(slug = \"alice\", owner = alice_pa);
new bob: Pod(slug = \"bob\", owner = bob_pa);
";

#[test]
fn two_instantiations_stamp_two_disjoint_entity_sets() {
    let config = resolved(PODS);
    assert_eq!(channels(&config), ["alice.messages", "bob.messages"]);
    let addresses: Vec<&str> = config
        .channels
        .iter()
        .map(|channel| channel.address.value().as_str())
        .collect();
    assert_eq!(
        addresses,
        ["brenn:alice.in.p1.messages", "brenn:bob.in.p1.messages"]
    );
    let surfaces: Vec<String> = config
        .surfaces
        .iter()
        .map(|surface| surface.handle.dotted())
        .collect();
    assert_eq!(surfaces, ["alice.panel", "bob.panel"]);
}

#[test]
fn a_stamped_binding_names_the_channel_its_own_instantiation_stamped() {
    let config = resolved(PODS);
    let bound: Vec<&RChanRef> = config
        .surfaces
        .iter()
        .flat_map(|surface| &surface.components)
        .flat_map(|component| &component.bindings)
        .filter_map(|binding| binding.chan.as_ref())
        .collect();
    // Each surface's component is bound to its own instantiation's channel,
    // which is what the ids say: nothing crossed between the two sets.
    assert_eq!(bound.len(), 2);
    for (index, chan) in bound.iter().enumerate() {
        match chan {
            RChanRef::Decl(id) => assert_eq!(id.0, index),
            other => panic!("expected a declared channel, found {other:?}"),
        }
    }
}

#[test]
fn a_grant_over_an_agent_parameter_names_the_agent_the_argument_named() {
    let config = resolved(PODS);
    let principals: Vec<String> = config
        .grants
        .iter()
        .map(|grant| grant.principal.dotted())
        .collect();
    assert_eq!(principals, ["alice_pa", "bob_pa"]);
}

#[test]
fn a_stamped_surface_slugs_to_what_its_parameter_said() {
    let config = resolved(PODS);
    let slugs: Vec<&str> = config
        .surfaces
        .iter()
        .map(|surface| surface.slug.value().as_str())
        .collect();
    assert_eq!(slugs, ["alice", "bob"]);
}

#[test]
fn a_stamped_entity_with_no_slug_defaults_to_its_full_dotted_handle() {
    let config = resolved(
        "\
assembly Pod(slug: String) {
    surface panel { grants = [subscribe]; }
}

new alice: Pod(slug = \"alice\");
",
    );
    assert_eq!(config.surfaces[0].slug.value(), "alice.panel");
}

#[test]
fn a_nested_assembly_stamps_under_the_whole_path() {
    let config = resolved(
        "\
assembly Inner(addr: String) {
    channel messages at f\"{addr}\";
}

assembly Outer(slug: String) {
    channel status at f\"brenn:{slug}.status\";
    new pod: Inner(addr = f\"brenn:{slug}.in.p1.messages\");
}

new alice: Outer(slug = \"alice\");
",
    );
    assert_eq!(channels(&config), ["alice.status", "alice.pod.messages"]);
}

#[test]
fn a_body_names_a_channel_a_nested_instantiation_stamped() {
    let config = resolved(
        "\
// ── packaged ──
component Panel { abi = dom; requires = []; in messages; }
// ── packaged ──

assembly Inner(addr: String) {
    channel messages at f\"{addr}\";
}

assembly Outer(slug: String) {
    new pod: Inner(addr = f\"brenn:{slug}.in.p1.messages\");
    surface panel {
        grants = [subscribe];
        new view: Panel { in messages <- pod.messages; }
    }
}

new alice: Outer(slug = \"alice\");
",
    );
    let binding = &config.surfaces[0].components[0].bindings[0];
    assert_eq!(binding.chan, Some(RChanRef::Decl(ChanId(0))));
}

#[test]
fn a_reference_from_outside_reaches_a_stamped_channel_through_the_instance() {
    let config = resolved(
        "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.p1.messages\";
}

new alice: Pod(slug = \"alice\");
new sink: Sink { in messages <- alice.messages; }
",
    );
    let binding = &config.consumers[0].bindings[0];
    assert_eq!(binding.chan, Some(RChanRef::Decl(ChanId(0))));
}

#[test]
fn a_stamped_agent_takes_the_arguments_the_assembly_passed_on() {
    let config = resolved(
        "\
agent Assistant(name: String) {
    slug = name;
    model = \"sonnet\";
}

assembly Pod(slug: String) {
    new pa: Assistant(name = slug);
}

new alice: Pod(slug = \"alice-pa\");
",
    );
    assert_eq!(config.agents[0].handle.dotted(), "alice.pa");
    assert_eq!(config.agents[0].slug.value(), "alice-pa");
}

#[test]
fn an_assembly_declared_in_a_module_stamps_into_the_file_that_instantiated_it() {
    let config = resolved_tree(&[
        (
            "",
            "use wiring::Pod;\n\nnew alice: Pod(slug = \"alice\");\n",
        ),
        (
            "wiring",
            "const scheme = \"brenn:\";\n\nassembly Pod(slug: String) {\n    channel messages at f\"{scheme}{slug}.in.p1.messages\";\n}\n",
        ),
    ]);
    assert_eq!(channels(&config), ["alice.messages"]);
    assert_eq!(
        config.channels[0].address.value(),
        "brenn:alice.in.p1.messages"
    );
}

// ── what it refuses ──────────────────────────────────────────────────────────

#[test]
fn an_assembly_instantiation_takes_no_body() {
    assert_eq!(
        refusal(
            "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.messages\";
}

new alice: Pod(slug = \"alice\") { chrome = false; }
"
        ),
        "an assembly instantiation takes arguments, not a body; per-instance values \
         are assembly parameters"
    );
}

#[test]
fn an_instantiation_cycle_names_the_chain() {
    assert_eq!(
        refusal(
            "\
assembly Outer() {
    new inner: Inner();
}

assembly Inner() {
    new outer: Outer();
}

new alice: Outer();
"
        ),
        "instantiating `Outer` reaches itself: Outer -> Inner -> Outer"
    );
}

#[test]
fn an_assembly_that_instantiates_itself_is_a_cycle_too() {
    assert_eq!(
        refusal(
            "\
assembly Pod() {
    new inner: Pod();
}

new alice: Pod();
"
        ),
        "instantiating `Pod` reaches itself: Pod -> Pod"
    );
}

#[test]
fn an_argument_the_assembly_does_not_take_names_its_parameters() {
    assert_eq!(
        refusal(
            "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.messages\";
}

new alice: Pod(slug = \"alice\", skin = \"bench\");
"
        ),
        "`Pod` has no parameter `skin`; it takes `slug: String`"
    );
}

#[test]
fn a_missing_argument_is_reported_at_the_instantiation() {
    assert_eq!(
        refusal(
            "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.messages\";
}

new alice: Pod();
"
        ),
        "`Pod` takes `slug`, and this instantiation states no value for it"
    );
}

#[test]
fn a_channel_argument_must_name_a_channel() {
    assert_eq!(
        refusal(
            "\
const bench = \"bench\";

assembly Pod(source: Channel) {
    channel messages at \"brenn:alice.messages\";
}

new alice: Pod(source = bench);
"
        ),
        "`bench` names a constant, not a channel"
    );
}

#[test]
fn a_channel_parameter_carries_the_channel_into_the_body() {
    let config = resolved(
        "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

channel bench_status at \"brenn:bench.status\";

assembly Pod(source: Channel) {
    new sink: Sink { in messages <- source; }
}

new alice: Pod(source = bench_status);
",
    );
    let binding = &config.consumers[0].bindings[0];
    assert_eq!(binding.chan, Some(RChanRef::Decl(ChanId(0))));
}

#[test]
fn a_grant_over_a_parameter_that_is_not_an_agent_says_so() {
    assert_eq!(
        refusal(
            "\
assembly Pod(slug: String) {
    grant slug subscribe prefix \"brenn:alice.\";
}

new alice: Pod(slug = \"alice\");
"
        ),
        "parameter `slug` names a value, and a grant names a principal"
    );
}

#[test]
fn a_body_reference_to_a_name_the_declaring_file_does_not_reach_is_refused() {
    // `skin` is the root's constant, and the assembly was written in a module
    // that never imported it: a class means what it meant where it was written.
    let errors = compile_tree(&[
        (
            "",
            "use wiring::Pod;\n\nconst skin = \"bench\";\n\nnew alice: Pod();\n",
        ),
        (
            "wiring",
            "assembly Pod() {\n    channel messages at f\"brenn:{skin}.messages\";\n}\n",
        ),
    ])
    .expect_err("a refusal");
    assert_eq!(
        errors[0].message,
        "`skin` is not declared in module `wiring`"
    );
}

#[test]
fn two_instantiations_stamping_one_identity_cite_both() {
    let messages = refusals(
        "\
assembly Pod(slug: String) {
    surface panel { slug = slug; grants = [subscribe]; }
}

new alice: Pod(slug = \"panel\");
new bob: Pod(slug = \"panel\");
",
    );
    assert_eq!(
        messages,
        ["two surfaces resolve to the identity `panel`".to_string()]
    );
}

#[test]
fn two_channels_under_one_name_in_an_assembly_body_are_refused() {
    let messages = refusals(
        "\
assembly Pod() {
    channel messages at \"brenn:alice.messages\";
    channel messages at \"brenn:bob.messages\";
}

new alice: Pod();
",
    );
    assert_eq!(
        messages,
        ["`messages` is declared twice in assembly `Pod`".to_string()]
    );
}

#[test]
fn a_channel_and_an_instance_under_one_name_in_an_assembly_body_are_refused() {
    // Different tables, one handle: without the check the surface and the
    // channel would both be emitted and `alice.panel` would name one of them.
    let messages = refusals(
        "\
// ── packaged ──
component Panel { abi = dom; requires = []; }
// ── packaged ──

assembly Pod() {
    channel panel at \"brenn:alice.panel\";
    surface panel { grants = [subscribe]; }
}

new alice: Pod();
",
    );
    assert_eq!(
        messages,
        ["`panel` is declared twice in assembly `Pod`".to_string()]
    );
}

#[test]
fn a_duplicate_in_an_assembly_body_is_reported_once_per_definition() {
    // Definition-site: two instantiations do not double the report.
    let messages = refusals(
        "\
assembly Pod() {
    channel messages at \"brenn:alice.messages\";
    channel messages at \"brenn:bob.messages\";
}

new alice: Pod();
new bob: Pod();
",
    );
    assert_eq!(messages.len(), 1, "{messages:?}");
}

#[test]
fn a_channel_named_through_an_instance_that_stamped_none_is_refused() {
    // The likeliest typo in the whole feature: reaching into an instance for a
    // channel by the wrong name.
    assert_eq!(
        refusal(
            "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.p1.messages\";
}

new alice: Pod(slug = \"alice\");
new sink: Sink { in messages <- alice.nope; }
"
        ),
        "`alice` stamps no channel `alice.nope`"
    );
}

#[test]
fn a_dotted_tail_on_a_channel_names_nothing() {
    assert_eq!(
        refusal(
            "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

channel bench_status at \"brenn:bench.status\";

new sink: Sink { in messages <- bench_status.tail; }
"
        ),
        "`bench_status` is not an instance, so `.tail` names nothing"
    );
}

#[test]
fn a_dotted_tail_on_a_channel_parameter_names_nothing() {
    assert_eq!(
        refusal(
            "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

channel bench_status at \"brenn:bench.status\";

assembly Pod(source: Channel) {
    new sink: Sink { in messages <- source.tail; }
}

new alice: Pod(source = bench_status);
"
        ),
        "parameter `source` is not an instance, so `.tail` names nothing"
    );
}

#[test]
fn a_dotted_tail_on_a_class_names_nothing() {
    assert_eq!(
        refusal(
            "\
assembly Pod() {
    channel messages at \"brenn:alice.messages\";
}

new alice: Pod.Inner();
"
        ),
        "a class is named directly; `.Inner` names nothing in `Pod`"
    );
}

#[test]
fn an_agent_parameter_used_as_a_value_says_what_it_names() {
    assert_eq!(
        refusal(
            "\
agent Assistant(name: String) {
    slug = name;
    model = \"sonnet\";
}

assembly Pod(owner: Agent) {
    channel messages at f\"brenn:{owner}.messages\";
}

new alice_pa: Assistant(name = \"alice-pa\");
new alice: Pod(owner = alice_pa);
"
        ),
        "parameter `owner` names an agent, which is not a value"
    );
}

#[test]
fn a_string_parameter_subscribed_to_says_what_it_names() {
    assert_eq!(
        refusal(
            "\
// ── packaged ──
component Sink { abi = processor; requires = []; in messages; }
// ── packaged ──

assembly Pod(slug: String) {
    new sink: Sink { in messages <- slug; }
}

new alice: Pod(slug = \"alice\");
"
        ),
        "parameter `slug` names a value, not a channel"
    );
}

#[test]
fn a_repo_parameter_reaches_a_stamped_agents_mount() {
    let config = resolved(
        "\
repo notes { remote = \"https://example.com/notes.git\"; }

agent Assistant(name: String, ws: Repo) {
    slug = name;
    model = \"sonnet\";
    mount ws { working_dir = true; }
}

assembly Pod(name: String, ws: Repo) {
    new pa: Assistant(name = name, ws = ws);
}

new alice: Pod(name = \"alice-pa\", ws = notes);
",
    );
    assert_eq!(config.agents[0].handle.dotted(), "alice.pa");
    assert_eq!(config.agents[0].mounts[0].repo.dotted(), "notes");
}

#[test]
fn a_table_parameter_reaches_a_stamped_components_config() {
    let config = resolved(
        "\
// ── packaged ──
component Sink { abi = processor; requires = []; }
// ── packaged ──

assembly Pod(tuning: Table) {
    new sink: Sink { config = tuning; }
}

new alice: Pod(tuning = { soft_pct = 80 });
",
    );
    let (key, config_attr) = &config.consumers[0].attrs[0];
    assert_eq!(key, "config");
    match config_attr.value() {
        RValue::Table(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "soft_pct");
            assert_eq!(fields[0].1.value(), &RValue::Int(80));
        }
        other => panic!("expected a table, found {other:?}"),
    }
}

#[test]
fn a_grant_in_an_assembly_body_names_a_parameter() {
    // Without the refusal a body-level bare name records a principal with no
    // instance prefix: two instantiations write one grant twice, and a name the
    // body itself stamps silently attaches the authority to a top-level entity.
    assert_eq!(
        refusal(
            "\
agent Assistant(name: String) {
    slug = name;
    model = \"sonnet\";
}

assembly Pod(slug: String) {
    new pa: Assistant(name = slug);
    grant pa subscribe prefix f\"brenn:{slug}.\";
}

new alice: Pod(slug = \"alice-pa\");
"
        ),
        "`pa` is not a parameter of this assembly, and an assembly grants \
         about its parameters; pass the principal in"
    );
}

#[test]
fn a_parameter_colliding_with_a_handle_the_body_stamps_is_refused() {
    // `subscribe ch` would resolve to the parameter and the body's own channel
    // would be unreachable from inside the body that declares it.
    assert_eq!(
        refusal(
            "\
assembly Pod(messages: Channel) {
    channel messages at \"brenn:alice.messages\";
}

channel bench_status at \"brenn:bench.status\";

new alice: Pod(messages = bench_status);
"
        ),
        "parameter `messages` collides with a handle assembly `Pod` stamps; \
         nothing shadows here"
    );
}

#[test]
fn an_agent_argument_naming_an_assembly_instance_is_refused() {
    assert_eq!(
        refusal(
            "\
agent Assistant(name: String, peer: Agent) {
    slug = name;
    model = \"sonnet\";
}

assembly Pod() {
    channel messages at \"brenn:alice.messages\";
}

new alice: Pod();
new bob_pa: Assistant(name = \"bob-pa\", peer = alice);
"
        ),
        "parameter `peer` is an `Agent`; `alice` instantiates `Pod`, which is an assembly"
    );
}

/// One assembly stamping an agent, a second consuming it — the composition
/// assemblies exist for. `ORDER` is the two instantiations, spelled either way
/// round.
fn wired(order: &str) -> String {
    format!(
        "\
agent Assistant(name: String) {{
    slug = name;
    model = \"sonnet\";
}}

assembly Pod(slug: String) {{
    channel messages at f\"brenn:{{slug}}.in.messages\";
    new pa: Assistant(name = slug);
}}

assembly Watch(peer: Agent, feed: Channel) {{
    surface board {{
        grants = [subscribe];
    }}
    grant peer subscribe exact feed;
}}

{order}"
    )
}

const PRODUCER_FIRST: &str = "\
new alice: Pod(slug = \"alice\");
new watch: Watch(peer = alice.pa, feed = alice.messages);
";

const CONSUMER_FIRST: &str = "\
new watch: Watch(peer = alice.pa, feed = alice.messages);
new alice: Pod(slug = \"alice\");
";

#[test]
fn an_agent_argument_may_name_a_stamped_agent() {
    let config = resolved(&wired(PRODUCER_FIRST));
    let principals: Vec<String> = config
        .grants
        .iter()
        .map(|grant| grant.principal.dotted())
        .collect();
    assert_eq!(principals, ["alice.pa"]);
    assert_eq!(
        config
            .agents
            .iter()
            .map(|agent| agent.handle.dotted())
            .collect::<Vec<_>>(),
        ["alice.pa"]
    );
}

#[test]
fn an_instantiation_resolves_the_same_whichever_order_it_is_written_in() {
    let first = resolved(&wired(PRODUCER_FIRST));
    let second = resolved(&wired(CONSUMER_FIRST));
    // Channels, surfaces and grants all land in source order either way, so the
    // two configs are the same document read twice.
    assert_eq!(channels(&first), ["alice.messages"]);
    assert_eq!(channels(&second), ["alice.messages"]);
    let handles = |config: &ResolvedConfig| -> Vec<String> {
        config
            .surfaces
            .iter()
            .map(|surface| surface.handle.dotted())
            .collect()
    };
    assert_eq!(handles(&first), ["watch.board"]);
    assert_eq!(handles(&second), handles(&first));
    assert_eq!(first.surfaces, second.surfaces);
    assert_eq!(first.grants, second.grants);
    assert_eq!(first.agents, second.agents);
    assert_eq!(first.channels, second.channels);
}

#[test]
fn two_instantiations_stamped_in_either_order_number_their_channels_in_source_order() {
    let config = resolved(
        "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.messages\";
}

assembly Watch(feed: Channel) {
    channel echo at \"brenn:watch.out.echo\";
}

new watch: Watch(feed = alice.messages);
new alice: Pod(slug = \"alice\");
",
    );
    // `watch` expands second and mints its id second, but it is written first,
    // and that is the order the config carries.
    assert_eq!(channels(&config), ["watch.echo", "alice.messages"]);
    assert_eq!(config.channels[0].address.value(), "brenn:watch.out.echo");
}

#[test]
fn instantiations_waiting_on_each_other_are_named_together() {
    let errors = compile_tree(&[(
        "",
        "\
assembly Pod(feed: Channel) {
    channel messages at \"brenn:alice.in.messages\";
}

new alice: Pod(feed = bob.messages);
new bob: Pod(feed = alice.messages);
",
    )])
    .expect_err("a knot");
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "these instantiations wait on each other, so none of them can expand: `alice`, `bob`"
    );
    let waits: Vec<&str> = errors[0]
        .related
        .iter()
        .map(|(note, _)| note.as_str())
        .collect();
    assert_eq!(
        waits,
        [
            "`alice` waits on `bob.messages`",
            "`bob` waits on `alice.messages`"
        ]
    );
}

#[test]
fn a_channel_argument_naming_nothing_is_still_undeclared() {
    assert_eq!(
        refusal(
            "\
assembly Watch(feed: Channel) {
    surface board { grants = [subscribe]; }
}

new watch: Watch(feed = missing);
"
        ),
        "`missing` is not declared in this file"
    );
}

#[test]
fn an_agent_argument_naming_a_stamped_surface_is_refused() {
    assert_eq!(
        refusal(
            "\
assembly Pod() {
    surface panel { grants = [subscribe]; }
}

assembly Watch(peer: Agent) {
    grant peer subscribe prefix \"brenn:alice.\";
}

new alice: Pod();
new watch: Watch(peer = alice.panel);
"
        ),
        "parameter `peer` is an `Agent`; `alice.panel` is a surface"
    );
}

#[test]
fn an_agent_argument_naming_nothing_an_instantiation_stamped_is_refused() {
    assert_eq!(
        refusal(
            "\
assembly Pod() {
    surface panel { grants = [subscribe]; }
}

assembly Watch(peer: Agent) {
    grant peer subscribe prefix \"brenn:alice.\";
}

new alice: Pod();
new watch: Watch(peer = alice.pa);
"
        ),
        "`alice` stamps no entity `alice.pa`"
    );
}

#[test]
fn two_instantiations_that_stamp_one_address_cite_both() {
    let errors = compile_tree(&[(
        "",
        concat!(
            "assembly Pod(slug: String) {\n",
            "    channel messages at \"brenn:alice.in.messages\";\n",
            "}\n",
            "new alice: Pod(slug = \"alice\");\n",
            "new bob: Pod(slug = \"bob\");\n",
        ),
    )])
    .expect_err("one address stamped twice");
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "two channels declare the address `brenn:alice.in.messages`"
    );
    assert_eq!(errors[0].related[0].0, "`alice.messages` declares it here");
}

#[test]
fn a_stamped_channel_is_named_rather_than_spelled_out() {
    assert_eq!(
        refusal(concat!(
            "component Panel { abi = dom; requires = []; in messages; }\n",
            "assembly Pod(slug: String) {\n",
            "    channel messages at f\"brenn:{slug}.in.messages\";\n",
            "    surface panel {\n",
            "        grants = [subscribe];\n",
            "        new view: Panel { in messages <- \"brenn:alice.in.messages\"; }\n",
            "    }\n",
            "}\n",
            "new alice: Pod(slug = \"alice\");\n",
        )),
        "`brenn:alice.in.messages` is the address channel `alice.messages` declares; \
         name the channel, not its address"
    );
}

/// Two stamped channels whose completion order is not their source order, and a
/// reference to each: the renumbering has to move the ids *and* carry every
/// reference with them, so each id is dereferenced rather than compared.
#[test]
fn a_reference_follows_a_stamped_channel_through_the_renumbering() {
    let config = resolved(
        "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.messages\";
}

assembly Watch(feed: Channel) {
    channel echo at \"brenn:watch.out.echo\";
}

new watch: Watch(feed = alice.messages);
new alice: Pod(slug = \"alice\");

surface board {
    grants = [subscribe];
    acl subscribe [exact alice.messages, exact watch.echo];
}
",
    );
    // `watch` completes second and mints its id second; source order puts it
    // first, and the references have to name the position, not the mint.
    assert_eq!(channels(&config), ["watch.echo", "alice.messages"]);
    let matchers = &config.surfaces[0].acls[0].matchers;
    let addressed = |index: usize| -> &str {
        match matchers[index].val.value() {
            RMatcherVal::Chan(id) => config.channels[id.0].address.value().as_str(),
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(addressed(0), "brenn:alice.in.messages");
    assert_eq!(addressed(1), "brenn:watch.out.echo");
}

/// A `new` inside an assembly body takes arguments too, and they resolve in the
/// file that declared the assembly — so a body reaching a sibling's stamped
/// channel waits for it exactly the way a top-level argument does.
#[test]
fn a_nested_argument_waits_for_a_sibling_instantiation() {
    let document = |order: &str| {
        format!(
            "\
assembly Pod(slug: String) {{
    channel messages at f\"brenn:{{slug}}.in.messages\";
}}

assembly Inner(feed: Channel) {{
    surface board {{
        grants = [subscribe];
        acl subscribe [exact feed];
    }}
}}

assembly Outer() {{
    new inner: Inner(feed = alice.messages);
}}

{order}"
        )
    };
    const PRODUCER_FIRST: &str = "\
new alice: Pod(slug = \"alice\");
new outer: Outer();
";
    const CONSUMER_FIRST: &str = "\
new outer: Outer();
new alice: Pod(slug = \"alice\");
";
    let first = resolved(&document(PRODUCER_FIRST));
    let second = resolved(&document(CONSUMER_FIRST));
    assert_eq!(
        first
            .surfaces
            .iter()
            .map(|surface| surface.handle.dotted())
            .collect::<Vec<_>>(),
        ["outer.inner.board"]
    );
    assert_eq!(first.surfaces, second.surfaces);
    assert_eq!(first.channels, second.channels);
}

/// An instantiation that was refused stamps nothing. Whatever was waiting on it
/// is dropped rather than re-attempted, so one operator mistake is one
/// diagnostic instead of a cascade blaming references that are only broken
/// because their producer is.
#[test]
fn an_instantiation_waiting_on_a_refused_sibling_reports_nothing_of_its_own() {
    let errors = refusals(
        "\
agent Assistant(name: String) {
    slug = name;
}

assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.messages\";
    new pa: Assistant(name = slug);
}

assembly Watch(peer: Agent, feed: Channel) {
    surface board { grants = [subscribe]; }
    grant peer subscribe exact feed;
}

new watch: Watch(peer = alice.pa, feed = alice.messages);
new alice: Pod();
",
    );
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("slug"), "{errors:?}");
}

#[test]
fn an_agent_argument_naming_a_stamped_channel_says_it_is_a_channel() {
    assert_eq!(
        refusal(
            "\
assembly Pod(slug: String) {
    channel messages at f\"brenn:{slug}.in.messages\";
}

assembly Watch(peer: Agent) {
    grant peer subscribe prefix \"brenn:alice.\";
}

new alice: Pod(slug = \"alice\");
new watch: Watch(peer = alice.messages);
"
        ),
        "parameter `peer` is an `Agent`; `alice.messages` is a channel"
    );
}

/// No assembly can stamp a repo, so a `Repo` argument reaching under an
/// instance handle names nothing there whatever the handle points at.
#[test]
fn a_repo_argument_naming_a_stamped_entity_is_refused() {
    assert_eq!(
        refusal(
            "\
assembly Pod() {
    surface panel { grants = [subscribe]; }
}

assembly Use(ws: Repo) {
    surface board { grants = [subscribe]; }
}

new alice: Pod();
new use_it: Use(ws = alice.panel);
"
        ),
        "parameter `ws` is a `Repo`; `alice.panel` is stamped by an instantiation, \
         and an instantiation stamps no repo"
    );
}

// ── links ────────────────────────────────────────────────────────────────────

/// A link is stamped per instantiation, exactly as a channel is: two
/// instantiations of one assembly are two anonymous channels, and a binding
/// written in the body names the one its own instantiation stamped.
#[test]
fn two_instantiations_stamp_two_links() {
    let config = resolved(
        "// ── packaged ──\n\
         component Duplex { abi = processor; requires = [ports]; io feed; }\n\
         // ── packaged ──\n\
         assembly Pod(slug: String) {\n    \
             link relay;\n    \
             new left: Duplex {\n        slug = f\"{slug}-left\";\n        \
             \n        \
             io feed <-> relay { push_depth = 4; retain_depth = 4; }\n    }\n    \
             new right: Duplex {\n        slug = f\"{slug}-right\";\n        \
             \n        \
             io feed <-> relay { push_depth = 4; retain_depth = 4; }\n    }\n}\n\
         new a: Pod(slug = \"a\");\n\
         new b: Pod(slug = \"b\");\n",
    );
    assert_eq!(
        config
            .links
            .iter()
            .map(|link| link.handle.dotted())
            .collect::<Vec<_>>(),
        vec!["a.relay", "b.relay"]
    );
    // Each stamping's four bindings name their own link, and a `LinkId` is the
    // position it indexes.
    for (index, consumer) in config.consumers.iter().enumerate() {
        let Some(RChanRef::Link(id)) = &consumer.bindings[0].chan else {
            panic!("a link-bound binding");
        };
        assert_eq!(id.0, index / 2);
    }
}

/// A link an assembly stamped is reached from outside through the handle it was
/// stamped under: cross-assembly wiring is the reason to declare a link inside
/// an assembly at all, so the dotted path resolves to the same anonymous ring
/// the body binds directly.
#[test]
fn a_stamped_link_is_reached_by_its_dotted_handle() {
    let config = resolved(
        "// ── packaged ──\n\
         component Duplex { abi = processor; requires = [ports]; io feed; }\n\
         // ── packaged ──\n\
         // ── packaged ──\n\
          component Sink { abi = processor; requires = []; in quiet; }\n\
          // ── packaged ──\n\
         assembly Pod() {\n    link relay;\n    \
         new inner: Duplex {\n        slug = \"inner\";\n        \
         \n        \
         io feed <-> relay { push_depth = 4; retain_depth = 4; }\n    }\n}\n\
         new pod: Pod();\n\
         new outer: Sink {\n    slug = \"outer\";\n    \
         \n    \
         in quiet <- pod.relay { push_depth = 2; retain_depth = 2; }\n}\n",
    );
    assert_eq!(
        config
            .links
            .iter()
            .map(|link| link.handle.dotted())
            .collect::<Vec<_>>(),
        vec!["pod.relay"]
    );
    // Both bindings — the one written inside the body and the one that reached
    // in from outside — name the one link the instantiation stamped.
    let bound: Vec<&RChanRef> = config
        .consumers
        .iter()
        .flat_map(|consumer| &consumer.bindings)
        .filter_map(|binding| binding.chan.as_ref())
        .collect();
    assert_eq!(bound.len(), 2, "{bound:?}");
    for reference in bound {
        let RChanRef::Link(id) = reference else {
            panic!("a link-bound binding, not {reference:?}");
        };
        assert_eq!(id.0, 0);
    }
}

/// Top-level links and stamped ones are minted in two phases into one id space,
/// and a `LinkId` is the position it indexes in the emitted list. A document
/// holding both is the only shape an off-by-base error in the second phase can
/// break.
#[test]
fn top_level_and_stamped_links_share_one_id_space() {
    let config = resolved(
        "// ── packaged ──\n\
         component Duplex { abi = processor; requires = [ports]; io feed; optional io spare; }\n\
         // ── packaged ──\n\
         assembly Pod(slug: String) {\n    link inner;\n    \
         new node: Duplex {\n        slug = f\"{slug}-node\";\n        \
         \n        \
         io feed <-> inner { push_depth = 4; retain_depth = 4; }\n        \
         io spare <-> shared { push_depth = 4; retain_depth = 4; }\n    }\n}\n\
         link shared;\n\
         new x: Pod(slug = \"x\");\n\
         new y: Pod(slug = \"y\");\n",
    );
    assert_eq!(
        config
            .links
            .iter()
            .map(|link| link.handle.dotted())
            .collect::<Vec<_>>(),
        vec!["shared", "x.inner", "y.inner"]
    );
    // Each binding indexes the link it named: the shared top-level one keeps id
    // 0, and each stamping's own sits at the position its handle does.
    for (index, consumer) in config.consumers.iter().enumerate() {
        let ids: Vec<usize> = consumer
            .bindings
            .iter()
            .filter_map(|binding| match binding.chan.as_ref() {
                Some(RChanRef::Link(id)) => Some(id.0),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![index + 1, 0], "{ids:?}");
    }
}

/// A handle naming no declared link is refused the way any unresolved name is.
#[test]
fn a_binding_naming_no_declaration_is_refused() {
    let refusal = refusal(
        "// ── packaged ──\n\
         component Sink { abi = processor; requires = [ports]; io feed; }\n\
         // ── packaged ──\n\
         new s: Sink {\n    slug = \"s\";\n    \n    \
         io feed <-> relay { push_depth = 1; retain_depth = 1; }\n}\n",
    );
    assert!(refusal.contains("relay"), "{refusal}");
}
