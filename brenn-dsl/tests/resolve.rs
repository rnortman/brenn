//! The index and constant passes, through the I/O-free core.
//!
//! `resolve_files` takes modules already parsed, so a whole tree is spelled
//! inline here and only the tree-on-disk cases go through `compile`.

mod support;

use brenn_dsl::model::IntOrWord;
use brenn_dsl::resolved::{ChanId, MatcherKind, RChanRef, RMatcherVal, RTail, RValue};
use fltk_cst_core::Span;
use fltk_serde_core::Spanned;
use support::{
    at, compile, compile_tree, refusal, refusal_tree, refusals, refusals_tree, resolve_errors,
    resolved, resolved_tree,
};

// ── constants ────────────────────────────────────────────────────────────────

#[test]
fn every_legal_escape_decodes() {
    compile("const text = \"a\\\\b\\\"c\\nd\\te\\rf\\0g\";\n").expect("the whole escape set");
}

#[test]
fn an_unknown_escape_names_the_set() {
    assert_eq!(
        refusal("const text = \"a\\qb\";\n"),
        "unknown escape `\\q`; known: \\\\ \\\" \\n \\t \\r \\0"
    );
}

#[test]
fn a_raw_string_has_no_escapes_to_decode() {
    compile("const text = \"\"\"a\\qb\"\"\";\n").expect("raw content is the value");
}

#[test]
fn a_constant_is_a_literal() {
    assert_eq!(
        refusal("const skin = \"bench\";\nconst other = skin;\n"),
        "a constant is a literal; a reference is not one"
    );
    assert_eq!(
        refusal("const host = f\"{skin}.example.com\";\n"),
        "a constant is a literal; an f-string is not one"
    );
}

#[test]
fn the_leaves_only_rule_reaches_all_the_way_down() {
    assert_eq!(
        refusal("const nested = [1, { inner = [2, skin] }];\n"),
        "a constant is a literal; a reference is not one",
        "a reference three levels down is still a reference"
    );
}

#[test]
fn a_parameter_default_is_a_literal_too() {
    assert_eq!(
        refusal("const skin = \"bench\";\nagent Pa(look: String = skin) {\n}\n"),
        "a parameter default is a literal; a reference is not one"
    );
}

// ── the file's own scope ─────────────────────────────────────────────────────

#[test]
fn a_name_declared_twice_cites_both_declarations() {
    let source = "const skin = \"bench\";\nsurface skin {\n    grants = [subscribe];\n}\n";
    let errors = compile(source).expect_err("two declarations, one name");
    assert_eq!(errors[0].message, "`skin` is declared twice in this file");
    assert_eq!(errors[0].related.len(), 1);
    assert_eq!(errors[0].line_col(), Some((2, 9)));
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        Some(1)
    );
}

#[test]
fn a_channel_tuning_block_declares_no_name() {
    compile("channel at prefix \"mqtt:broker:alice/\" {\n    push_depth = 4;\n}\n\nchannel at prefix \"mqtt:broker:bob/\" {\n    push_depth = 4;\n}\n")
        .expect("two tunings are not two declarations of one name");
}

// ── imports ──────────────────────────────────────────────────────────────────

#[test]
fn a_named_import_brings_one_name_in() {
    compile_tree(&[
        (
            "",
            "use shared::Deskbar;\n\nnew alice_desk: Deskbar(slug = \"alice-desk\");\n",
        ),
        ("shared", "assembly Deskbar(slug: String) {\n}\n"),
    ])
    .expect("the import");
}

#[test]
fn a_glob_import_brings_every_local_name_in() {
    compile_tree(&[
        ("", "use shared::*;\n"),
        (
            "shared",
            "const skin = \"bench\";\n\nassembly Deskbar(slug: String) {\n}\n",
        ),
    ])
    .expect("both names");
}

#[test]
fn an_import_of_a_name_a_module_does_not_declare_says_so() {
    let errors = compile_tree(&[
        ("", "use shared::Deskbar;\n"),
        ("shared", "const skin = \"bench\";\n"),
    ])
    .expect_err("no such item");
    assert_eq!(errors[0].message, "module `shared` declares no `Deskbar`");
}

#[test]
fn an_import_colliding_with_a_local_name_cites_the_declaration() {
    let errors = compile_tree(&[
        ("", "use shared::skin;\n\nconst skin = \"slab\";\n"),
        ("shared", "const skin = \"bench\";\n"),
    ])
    .expect_err("nothing shadows");
    assert_eq!(
        errors[0].message,
        "importing `skin` collides with a declaration in this file"
    );
    assert_eq!(errors[0].related.len(), 1);
}

#[test]
fn a_glob_does_not_re_export_what_it_imported() {
    let errors = compile_tree(&[
        ("", "use middle::skin;\n"),
        ("middle", "use leaf::*;\n"),
        ("leaf", "const skin = \"bench\";\n"),
    ])
    .expect_err("an import is not a declaration");
    assert_eq!(errors[0].message, "module `middle` declares no `skin`");
}

#[test]
fn a_use_names_an_item() {
    assert_eq!(
        refusal("use shared;\n"),
        "a `use` names an item: `use module::Item;`, or `use module::*;` for all of them"
    );
}

#[test]
fn a_module_path_is_written_with_colons() {
    assert_eq!(
        refusal("use shared.Deskbar;\n"),
        "a module path is written with `::`, not `.`"
    );
}

// ── class parameters ─────────────────────────────────────────────────────────

#[test]
fn a_parameter_declared_twice_cites_the_first() {
    let errors = compile("assembly Deskbar(slug: String, slug: String) {\n}\n")
        .expect_err("one name, two parameters");
    assert_eq!(errors[0].message, "parameter `slug` is declared twice");
    assert_eq!(errors[0].related.len(), 1);
}

#[test]
fn a_parameter_colliding_with_a_top_level_name_is_refused_at_the_parameter() {
    let errors = compile("const skin = \"bench\";\n\nassembly Deskbar(skin: String) {\n}\n")
        .expect_err("nothing shadows");
    assert_eq!(
        errors[0].message,
        "parameter `skin` collides with a constant of the same name; nothing shadows here"
    );
    assert_eq!(
        errors[0].line_col(),
        Some((3, 18)),
        "the definition site, so adding a declaration cannot silently change a body"
    );
}

#[test]
fn a_parameter_type_the_language_does_not_have_names_the_ones_it_does() {
    assert_eq!(
        refusal("assembly Deskbar(driver: Widget) {\n}\n"),
        "`Widget` is not a parameter type; expected one of String, Int, Bool, Table, Channel, Agent, Repo"
    );
}

#[test]
fn every_parameter_type_the_language_has_is_accepted() {
    compile(
        "assembly Deskbar(a: String, b: Int, c: Bool, d: Table, e: Channel, f: Agent, g: Repo) {\n}\n",
    )
    .expect("the whole set");
}

// ── accumulation ─────────────────────────────────────────────────────────────

#[test]
fn independent_errors_are_all_reported() {
    let messages = refusals(
        "const skin = \"bench\";\nconst skin = \"slab\";\n\nassembly Deskbar(driver: Widget) {\n}\n",
    );
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert!(messages.iter().any(|m| m.contains("declared twice")));
    assert!(messages.iter().any(|m| m.contains("not a parameter type")));
}

// ── entities ─────────────────────────────────────────────────────────────────

#[test]
fn a_channel_carries_its_address_and_its_tuning() {
    let config = resolved(concat!(
        "const desk = \"alice-desk\";\n",
        "channel messages_p1 at f\"brenn:{desk}.in.p1.messages\" {\n",
        "    description = \"Panel one.\";\n",
        "}\n",
    ));
    assert_eq!(config.channels.len(), 1);
    let channel = &config.channels[0];
    assert_eq!(channel.handle.dotted(), "messages_p1");
    assert_eq!(channel.address.value(), "brenn:alice-desk.in.p1.messages");
    let description = channel.attrs.description.as_ref().expect("the description");
    assert_eq!(
        description.value.value(),
        &RValue::Str("Panel one.".to_string())
    );
}

#[test]
fn a_channel_with_no_body_carries_no_tuning() {
    let config = resolved("channel acks at \"ephemeral:alice-desk.acks\";\n");
    assert_eq!(config.channels.len(), 1);
    assert!(config.channels[0].attrs.push_depth.is_none());
}

#[test]
fn a_tuning_block_is_not_a_channel() {
    let config = resolved(concat!(
        "channel at prefix \"mqtt:broker:alice/\" {\n",
        "    push_depth = 4;\n",
        "}\n",
    ));
    assert!(config.channels.is_empty(), "a tuning declares no handle");
    assert_eq!(config.tunings.len(), 1);
    assert!(config.tunings[0].is_prefix);
}

#[test]
fn an_address_with_no_scheme_names_the_schemes() {
    assert_eq!(
        refusal("channel messages at \"alice-desk.messages\";\n"),
        "address `alice-desk.messages` names no scheme; expected one of \
         brenn:, ephemeral:, local:, webhook:, mqtt:"
    );
}

#[test]
fn the_runtime_internal_push_scheme_is_not_spellable() {
    // `pwa_push:` is a scheme the runtime knows and the language does not: an
    // address leading with it takes the same path as any unrecognized prefix,
    // and the schemes it is offered are the spellable ones.
    assert_eq!(
        refusal("channel alerts at \"pwa_push:alerts\";\n"),
        "address `pwa_push:alerts` names no scheme; expected one of \
         brenn:, ephemeral:, local:, webhook:, mqtt:"
    );
}

#[test]
fn the_push_scheme_is_not_spellable_in_a_pin_either() {
    // Every statement form that carries a literal address asks the same
    // question, and the `unreachable!` arms downstream rest on all of them
    // asking it: a pin key naming `pwa_push:` is refused here, not derived.
    assert_eq!(
        refusal(
            "uuid_pins {\n    \"pwa_push:alerts\" = \
             \"11111111-2222-5333-8444-555555555555\";\n}\n"
        ),
        "address `pwa_push:alerts` names no scheme; expected one of \
         brenn:, ephemeral:, local:, webhook:, mqtt:"
    );
}

#[test]
fn the_push_scheme_is_not_spellable_as_a_binding_target_either() {
    assert_eq!(
        refusal(concat!(
            "component Sink {\n    abi = processor; requires = [];\n    out events;\n}\n",
            "new alice_sink: Sink {\n",
            "    component_path = \"sink.wasm\";\n",
            "    out events -> \"pwa_push:alerts\";\n",
            "}\n",
        )),
        "address `pwa_push:alerts` names no scheme; expected one of \
         brenn:, ephemeral:, local:, webhook:, mqtt:"
    );
}

#[test]
fn an_empty_address_is_still_positioned() {
    let source = "const skin = \"bench\";\nchannel messages at \"\";\n";
    let errors = compile(source).expect_err("an empty address names no scheme");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].line_col(), Some((2, 21)));
}

#[test]
fn a_scheme_and_nothing_else_is_not_an_address() {
    assert_eq!(
        refusal("channel messages at \"brenn:\";\n"),
        "`brenn:` is a scheme and nothing else; an address names something under it"
    );
}

#[test]
fn an_exact_matcher_resolves_to_the_channel_it_names() {
    let config = resolved(concat!(
        "channel utterance at \"brenn:alice-pod.out.utterance\";\n",
        "remote bob_pod {\n",
        "    token_file = \"/home/alice/.secrets/bob-pod.token\";\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [exact utterance];\n",
        "}\n",
    ));
    let acl = &config.remotes[0].acls[0];
    assert_eq!(acl.plane.value(), "subscribe");
    assert_eq!(acl.matchers[0].val.value(), &RMatcherVal::Chan(ChanId(0)));
}

#[test]
fn a_matcher_naming_something_that_is_not_a_channel_says_what_it_named() {
    assert_eq!(
        refusal(concat!(
            "repo notes { remote = \"git@example.com:alice/notes.git\"; }\n",
            "remote bob_pod {\n",
            "    token_file = \"/t\";\n",
            "    grants = [subscribe];\n",
            "    acl subscribe [exact notes];\n",
            "}\n",
        )),
        "`notes` names a repo, not a channel"
    );
}

#[test]
fn a_top_level_acl_is_refused_with_the_form_that_works() {
    assert_eq!(
        refusal("acl subscribe [prefix \"brenn:alice-desk.\"];\n"),
        "an acl statement needs an enclosing entity body (surface, agent, remote, \
         or a new instance); at top level, grant authority to a named principal \
         with `grant`"
    );
}

#[test]
fn a_grant_carries_the_principal_it_names() {
    let config = resolved(concat!(
        "remote bob_pod {\n",
        "    token_file = \"/t\";\n",
        "    grants = [subscribe];\n",
        "}\n",
        "grant bob_pod subscribe prefix \"brenn:alice-desk.\";\n",
    ));
    let grant = &config.grants[0];
    assert_eq!(grant.principal.dotted(), "bob_pod");
    assert_eq!(grant.plane.value(), "subscribe");
    assert_eq!(grant.m.kind.value(), &MatcherKind::Prefix);
}

#[test]
fn a_remote_slug_defaults_to_its_handle() {
    let config = resolved(concat!(
        "remote bob_pod {\n",
        "    token_file = \"/t\";\n",
        "    grants = [subscribe];\n",
        "}\n",
    ));
    assert_eq!(config.remotes[0].slug.value(), "bob_pod");
}

#[test]
fn a_webhook_takes_the_slug_it_states() {
    let config = resolved(concat!(
        "webhook push_alice {\n",
        "    slug = \"push-alice\";\n",
        "    mount = \"/webhooks/push-alice\";\n",
        "    signature { scheme = bearer-token; header = \"authorization\"; }\n",
        "}\n",
    ));
    let webhook = &config.webhooks[0];
    assert_eq!(webhook.handle.dotted(), "push_alice");
    assert_eq!(webhook.slug.value(), "push-alice");
}

#[test]
fn a_sections_own_vocabulary_still_refuses_an_unknown_key() {
    assert!(
        refusal(concat!(
            "alerting {\n",
            "    max_alerts = 10;\n",
            "    window_secs = 600;\n",
            "    ntfy { host = \"ntfy.example.com\"; }\n",
            "}\n",
        ))
        .contains("host"),
    );
}

#[test]
fn a_webhook_block_the_body_does_not_admit_is_refused() {
    assert!(
        refusal(concat!(
            "webhook push_alice {\n",
            "    replay { store_path = \"/home/alice/state/replay.db\"; }\n",
            "}\n",
        ))
        .starts_with("`replay` is not a block a webhook body admits"),
    );
}

#[test]
fn the_named_definitions_resolve_their_bodies() {
    let config = resolved(concat!(
        "const kb = \"/home/alice/kb\";\n",
        "repo notes { remote = \"git@example.com:alice/notes.git\"; }\n",
        "mqtt_client broker { url = \"mqtts://broker.example.com:8883\"; }\n",
        "mcp_server tools { command = \"tools\"; env = { TOOLS_ROOT = kb }; }\n",
    ));
    assert_eq!(config.repos[0].handle.dotted(), "notes");
    assert_eq!(config.mqtt_clients[0].handle.dotted(), "broker");
    let env = &config.mcp_servers[0].attrs.env.as_ref().expect("env").value;
    assert_eq!(
        env.value(),
        &RValue::Table(vec![(
            "TOOLS_ROOT".to_string(),
            Spanned::new(RValue::Str("/home/alice/kb".to_string()), Span::unknown()),
        )])
    );
}

#[test]
fn a_uuid_pin_carries_the_address_it_pins() {
    let config = resolved(concat!(
        "uuid_pins {\n",
        "    \"brenn:alice-desk.in.p1.messages\" = \"0191f0c4-1b2a-7c3d-8e4f-5a6b7c8d9e0f\";\n",
        "}\n",
    ));
    assert_eq!(
        config.uuid_pins[0].address.value(),
        "brenn:alice-desk.in.p1.messages"
    );
}

#[test]
fn an_unknown_section_key_is_refused_at_the_key() {
    assert!(
        refusal(concat!(
            "server {\n",
            "    public_url = \"https://brenn.example.com\";\n",
            "    bind = \"127.0.0.1:3000\";\n",
            "}\n",
        ))
        .contains("bind"),
    );
}

// ── qualified references ─────────────────────────────────────────────────────

#[test]
fn a_qualified_reference_reads_another_modules_declaration() {
    let config = resolved_tree(&[
        ("", "channel status at f\"brenn:{shared::skin}.status\";\n"),
        ("shared", "const skin = \"bench\";\n"),
    ]);
    assert_eq!(config.channels[0].address.value(), "brenn:bench.status");
}

#[test]
fn a_qualified_reference_to_a_name_a_module_lacks_says_which_module() {
    assert_eq!(
        refusal_tree(&[
            ("", "channel status at f\"brenn:{shared::skin}.status\";\n"),
            ("shared", "const host = \"example.com\";\n"),
        ]),
        "`skin` is not declared in module `shared`"
    );
}

#[test]
fn a_qualified_reference_through_a_module_that_is_not_there_says_so() {
    assert_eq!(
        refusal_tree(&[("", "channel status at f\"brenn:{wiring::skin}.status\";\n")]),
        "no module `wiring`"
    );
}

#[test]
fn a_module_segment_cannot_follow_an_instance_segment() {
    assert_eq!(
        refusal_tree(&[
            (
                "",
                "channel status at f\"brenn:{shared::defaults.inner::skin}.status\";\n"
            ),
            ("shared", "const defaults = { inner = 1 };\n"),
        ]),
        "a `::` module segment cannot follow a `.` segment"
    );
}

// ── channels: ids, and what a lookup can fail on ─────────────────────────────

#[test]
fn a_channels_id_is_its_position_among_the_channels() {
    let config = resolved(concat!(
        "channel first at \"brenn:alice-desk.first\";\n",
        "channel second at \"brenn:alice-desk.second\";\n",
        "remote bob_pod {\n",
        "    token_file = \"/t\";\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [exact second];\n",
        "}\n",
    ));
    assert_eq!(config.channels.len(), 2);
    assert_eq!(config.channels[1].handle.dotted(), "second");
    let matcher = &config.remotes[0].acls[0].matchers[0];
    assert_eq!(matcher.val.value(), &RMatcherVal::Chan(ChanId(1)));
}

#[test]
fn a_matcher_naming_nothing_declared_says_the_name_is_not_declared() {
    assert_eq!(
        refusal(concat!(
            "remote bob_pod {\n",
            "    token_file = \"/t\";\n",
            "    grants = [subscribe];\n",
            "    acl subscribe [exact utterance];\n",
            "}\n",
        )),
        "`utterance` is not declared in this file"
    );
}

#[test]
fn a_matcher_naming_a_channel_whose_address_was_refused_says_both() {
    let messages = refusals(concat!(
        "channel utterance at \"alice-pod.out.utterance\";\n",
        "remote bob_pod {\n",
        "    token_file = \"/t\";\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [exact utterance];\n",
        "}\n",
    ));
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert!(messages[0].starts_with("address `alice-pod.out.utterance`"));
    assert_eq!(
        messages[1],
        "channel `utterance` did not resolve to an address"
    );
}

#[test]
fn a_channel_body_that_is_refused_keeps_the_ids_of_the_channels_after_it() {
    let messages = refusals(concat!(
        "channel first at \"brenn:alice-desk.first\" {\n",
        "    description = nope;\n",
        "}\n",
        "channel second at \"brenn:alice-desk.second\";\n",
    ));
    assert_eq!(
        messages,
        ["`nope` is not declared in this file"],
        "the body's own error is the whole report; the id invariant holds"
    );
}

// ── identity, and the sections dispatch ──────────────────────────────────────

#[test]
fn a_slug_that_is_not_a_string_falls_back_to_the_handle() {
    assert_eq!(
        refusals(concat!(
            "webhook push_alice {\n",
            "    slug = 5;\n",
            "    mount = \"/webhooks/push-alice\";\n",
            "}\n",
        )),
        ["a slug is a string; this is an integer"]
    );
}

#[test]
fn an_observability_sub_block_is_checked_against_its_own_vocabulary() {
    assert!(
        refusal(concat!(
            "observability {\n",
            "    usage { session_gap_minutes = 30; nope = 1; }\n",
            "}\n",
        ))
        .contains("nope"),
    );
}

/// The grammar admits a section inside any section, and only `alerting` and
/// `observability` have a vocabulary for what they hold. Every other section
/// holds nothing, and that is checked here rather than at boot.
#[test]
fn a_sub_block_of_a_section_that_holds_none_is_refused() {
    assert_eq!(
        refusal(concat!(
            "container alice {\n",
            "    image = \"example.com/alice:latest\";\n",
            "    home_dir = \"/home/alice\";\n",
            "    anything { whatever = 1; }\n",
            "}\n",
        )),
        "the `container` block holds no sub-blocks, so `anything` has no meaning here"
    );
}

/// A kindword the parent's sub-block vocabulary knows, under a parent that has
/// none: `usage` is `observability`'s, and a `server` holds nothing at all.
#[test]
fn a_known_sub_block_kindword_under_the_wrong_parent_is_refused() {
    assert_eq!(
        refusal(concat!(
            "server {\n",
            "    public_url = \"https://brenn.example.com\";\n",
            "    usage { session_gap_minutes = 30; }\n",
            "}\n",
        )),
        "the `server` block holds no sub-blocks, so `usage` has no meaning here"
    );
}

/// Nesting one level deeper than the vocabulary goes: `ntfy` is a sub-block,
/// and a sub-block of it is refused at the same check.
#[test]
fn a_section_nested_inside_a_sub_block_is_refused() {
    assert_eq!(
        refusal(concat!(
            "alerting {\n",
            "    max_alerts = 5;\n",
            "    window_secs = 60;\n",
            "    ntfy {\n",
            "        url = \"https://ntfy.example.com/alice\";\n",
            "        mail { to = \"alice@example.com\"; }\n",
            "    }\n",
            "}\n",
        )),
        "the `ntfy` block holds no sub-blocks, so `mail` has no meaning here"
    );
}

// ── section multiplicity ─────────────────────────────────────────────────────
//
// At most one unnamed section per kindword, at most one named section per
// (kindword, name), counted over the written document.

/// The refusal cites both sites, because which of the two is the mistake is the
/// reader's to decide.
#[test]
fn a_second_section_of_one_kindword_is_refused_at_both() {
    let errors = compile(concat!(
        "server { public_url = \"https://brenn.example.com\"; }\n",
        "server { public_url = \"https://brenn.example.org\"; }\n",
    ))
    .expect_err("two `server` sections");
    assert_eq!(errors.len(), 1, "{:?}", errors);
    assert_eq!(
        errors[0].message,
        "a document states `server` once, and this is the second"
    );
    assert_eq!(errors[0].related.len(), 1, "{}", errors[0].render());
    assert_eq!(errors[0].related[0].0, "first stated here");
    // Which site is which is the whole value of this diagnostic: the editor
    // opens on the second occurrence, and the first is what "first stated here"
    // points at. Swapping the two would read as an instruction to delete the
    // line to keep.
    assert_eq!(errors[0].line_col(), Some((2, 1)));
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        Some(1)
    );
}

/// The walk is flat across every file of a document, so two files stating one
/// section state it twice.
#[test]
fn a_section_duplicated_across_an_import_is_refused() {
    assert_eq!(
        refusal_tree(&[
            (
                "",
                "use wiring::*;\nserver { public_url = \"https://brenn.example.com\"; }\n"
            ),
            (
                "wiring",
                "server { public_url = \"https://brenn.example.org\"; }\n"
            ),
        ]),
        "a document states `server` once, and this is the second"
    );
}

/// A named section is one per name: two containers with different names are not
/// a duplicate, and two of one name are.
#[test]
fn a_named_section_is_one_per_name() {
    resolved(concat!(
        "container alice { image = \"example.com/cc:latest\"; home_dir = \"/home/alice\"; }\n",
        "container bob { image = \"example.com/cc:latest\"; home_dir = \"/home/bob\"; }\n",
    ));
    assert_eq!(
        refusal(concat!(
            "container alice { image = \"example.com/cc:latest\"; home_dir = \"/home/one\"; }\n",
            "container alice { image = \"example.com/cc:latest\"; home_dir = \"/home/two\"; }\n",
        )),
        "a document states `container alice` once, and this is the second"
    );
}

/// Sub-blocks are counted the same way, scoped to the parent that holds them.
#[test]
fn a_second_sub_block_of_one_kindword_is_refused() {
    assert_eq!(
        refusal(concat!(
            "alerting {\n",
            "    max_alerts = 5;\n",
            "    window_secs = 60;\n",
            "    ntfy { url = \"https://ntfy.example.com/alice-one\"; }\n",
            "    ntfy { url = \"https://ntfy.example.com/alice-two\"; }\n",
            "}\n",
        )),
        "a `alerting` section states `ntfy` once, and this is the second"
    );
}

/// The other parent that nests: both arms of the nesting list are exercised, so
/// dropping either from it fails a test rather than silently losing check-time
/// duplicate detection for that parent's sub-blocks.
#[test]
fn a_second_sub_block_under_the_other_nesting_parent_is_refused() {
    assert_eq!(
        refusal(concat!(
            "observability {\n",
            "    usage { session_gap_minutes = 30; }\n",
            "    usage { session_gap_minutes = 60; }\n",
            "}\n",
        )),
        "a `observability` section states `usage` once, and this is the second"
    );
}

/// A kindword the context admits none of is not counted: saying `bogus` appears
/// twice would claim one `bogus` would have been fine, and would swallow the
/// dispatch's refusal for the second one.
#[test]
fn a_repeated_kindword_the_context_admits_none_of_is_refused_only_as_unknown() {
    let messages = refusals("bogus { }\nbogus { }\n");
    assert_eq!(messages.len(), 2, "{messages:?}");
    for message in &messages {
        assert!(
            message.starts_with("`bogus` is not a block a document admits"),
            "{messages:?}"
        );
    }
}

/// Counted where a section is encountered, not where one survives: the first
/// occupies its slot even though its own body was refused, so the operator gets
/// both verdicts in one compile instead of one per run.
#[test]
fn a_refused_section_still_occupies_its_slot() {
    let messages = refusals(concat!(
        "server { nope = 1; }\n",
        "server { public_url = \"https://brenn.example.com\"; }\n",
    ));
    assert_eq!(messages.len(), 2, "{messages:?}");
    // Both verdicts, in whichever order the passes run: which pass reports
    // first is not part of the rule.
    assert!(
        messages
            .iter()
            .any(|m| m == "a document states `server` once, and this is the second"),
        "{messages:?}"
    );
    assert!(messages.iter().any(|m| m.contains("nope")), "{messages:?}");
}

/// A parent that nests nothing says so and says nothing else: counting how
/// often a sub-block appears there would tell the operator that one of them
/// would have been fine.
#[test]
fn a_repeated_sub_block_under_a_parent_that_nests_nothing_is_refused_only_for_nesting() {
    let messages = refusals(concat!(
        "server {\n",
        "    public_url = \"https://brenn.example.com\";\n",
        "    ntfy { url = \"https://ntfy.example.com/alice-one\"; }\n",
        "    ntfy { url = \"https://ntfy.example.com/alice-two\"; }\n",
        "}\n",
    ));
    assert_eq!(
        messages,
        vec![
            "the `server` block holds no sub-blocks, so `ntfy` has no meaning here".to_string(),
            "the `server` block holds no sub-blocks, so `ntfy` has no meaning here".to_string(),
        ]
    );
}

/// A webhook's blocks are a section list too, counted the same way: which
/// scheme guards an endpoint must not be chosen by declaration order.
#[test]
fn a_second_webhook_block_of_one_kindword_is_refused() {
    assert_eq!(
        refusal(concat!(
            "webhook alice_inbox {\n",
            "    mount = \"/webhooks/alice-inbox\";\n",
            "    signature { scheme = bearer-token; header = \"authorization\"; }\n",
            "    signature { scheme = hmac-stripe; header = \"stripe-signature\"; }\n",
            "}\n",
        )),
        "webhook `alice_inbox` states `signature` once, and this is the second"
    );
}

/// A credential block is named, so it is one per id: two ids are two
/// credentials, and one id twice is refused.
#[test]
fn a_webhook_credential_block_is_one_per_id() {
    resolved(concat!(
        "webhook alice_inbox {\n",
        "    mount = \"/webhooks/alice-inbox\";\n",
        "    signature { scheme = bearer-token; header = \"authorization\"; }\n",
        "    token phone { secret_file = \"/home/alice/.secrets/phone.token\"; }\n",
        "    token desk { secret_file = \"/home/alice/.secrets/desk.token\"; }\n",
        "}\n",
    ));
    assert_eq!(
        refusal(concat!(
            "webhook alice_inbox {\n",
            "    mount = \"/webhooks/alice-inbox\";\n",
            "    signature { scheme = bearer-token; header = \"authorization\"; }\n",
            "    token phone { secret_file = \"/home/alice/.secrets/one.token\"; }\n",
            "    token phone { secret_file = \"/home/alice/.secrets/two.token\"; }\n",
            "}\n",
        )),
        "webhook `alice_inbox` states `token phone` once, and this is the second"
    );
}

/// An agent's hook blocks likewise: two `start_hooks` are two answers to one
/// question.
#[test]
fn a_second_hook_block_of_one_kindword_is_refused() {
    assert_eq!(
        refusal(concat!(
            "agent Assistant() {\n",
            "    start_hooks { host = [\"git fetch\"]; }\n",
            "    start_hooks { container = [\"pf rebuild\"]; }\n",
            "}\n",
            "new alice: Assistant();\n",
        )),
        "an agent states `start_hooks` once, and this is the second"
    );
}

/// The value a resolved block — a section or a webhook sub-block, which are one
/// type — carries under `key`.
fn attr<'s>(block: &'s brenn_dsl::resolved::RSection, key: &str) -> &'s RValue {
    block
        .attrs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.value())
        .unwrap_or_else(|| panic!("the block carries `{key}`"))
}

#[test]
fn a_section_reaches_the_resolved_model_with_its_values_resolved() {
    let config = resolved(concat!(
        "const host = \"brenn.example.com\";\n",
        "server {\n",
        "    public_url = f\"https://{host}\";\n",
        "    trusted_proxy_hops = 1;\n",
        "}\n",
    ));
    let section = &config.sections[0];
    assert_eq!(section.kindword.value(), "server");
    assert_eq!(section.name, None);
    assert_eq!(
        attr(section, "public_url"),
        &RValue::Str("https://brenn.example.com".to_string())
    );
    assert_eq!(attr(section, "trusted_proxy_hops"), &RValue::Int(1));
}

#[test]
fn a_sections_token_context_crosses_as_the_word_it_was_written_as() {
    let config = resolved(concat!(
        "logging {\n",
        "    console_level = debug;\n",
        "    file_level = info;\n",
        "}\n",
    ));
    // `debug` is a word, not a reference: nothing declares it and resolving it
    // would refuse the document.
    let section = &config.sections[0];
    assert_eq!(attr(section, "console_level"), &RValue::Str("debug".into()));
    assert_eq!(attr(section, "file_level"), &RValue::Str("info".into()));
}

#[test]
fn a_named_section_carries_its_name() {
    let config = resolved(concat!(
        "container alice {\n",
        "    image = \"example.com/alice:latest\";\n",
        "    home_dir = \"/home/alice\";\n",
        "}\n",
    ));
    let section = &config.sections[0];
    assert_eq!(section.kindword.value(), "container");
    assert_eq!(
        section.name.as_ref().map(Spanned::value),
        Some(&"alice".to_string())
    );
}

#[test]
fn a_dispatched_sub_block_is_carried_under_its_parent() {
    let config = resolved(concat!(
        "alerting {\n",
        "    max_alerts = 10;\n",
        "    window_secs = 600;\n",
        "    ntfy { url = \"https://ntfy.example.com/alice-alerts\"; }\n",
        "}\n",
    ));
    let section = &config.sections[0];
    assert_eq!(attr(section, "max_alerts"), &RValue::Int(10));
    let sub = &section.subs[0];
    assert_eq!(sub.kindword.value(), "ntfy");
    assert_eq!(
        attr(sub, "url"),
        &RValue::Str("https://ntfy.example.com/alice-alerts".into())
    );
}

#[test]
fn every_unresolvable_value_in_a_typed_section_is_reported() {
    let errors = refusals(concat!(
        "server {\n",
        "    public_url = nowhere;\n",
        "    pid_file = elsewhere;\n",
        "}\n",
    ));
    // Both, in one report. The order is the vocabulary's, not the source's,
    // because that is the order the fields are carried in.
    assert_eq!(errors.len(), 2);
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error.contains("elsewhere")),
        "{errors:?}"
    );
}

#[test]
fn a_refused_section_value_does_not_silence_its_sub_blocks() {
    let errors = refusals(concat!(
        "alerting {\n",
        "    max_alerts = nowhere;\n",
        "    window_secs = 600;\n",
        "    ntfy { url = elsewhere; }\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2);
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error.contains("elsewhere")),
        "{errors:?}"
    );
}

#[test]
fn a_webhook_carries_its_typed_sub_blocks() {
    let config = resolved(concat!(
        "const secrets = \"/home/alice/secrets\";\n",
        "webhook push_alice {\n",
        "    slug = \"push-alice\";\n",
        "    mount = \"/webhooks/push-alice\";\n",
        "    signature { scheme = bearer-token; header = \"authorization\"; }\n",
        "    token primary { secret_file = f\"{secrets}/push.token\"; }\n",
        "}\n",
    ));
    let blocks = &config.webhooks[0].blocks;
    assert_eq!(blocks[0].kindword.value(), "signature");
    // The scheme is a token context and stayed the word that was written.
    assert_eq!(
        attr(&blocks[0], "scheme"),
        &RValue::Str("bearer-token".into())
    );
    assert_eq!(
        attr(&blocks[0], "header"),
        &RValue::Str("authorization".into())
    );
    assert_eq!(blocks[1].kindword.value(), "token");
    assert_eq!(
        blocks[1].name.as_ref().map(Spanned::value),
        Some(&"primary".to_string())
    );
    assert_eq!(
        attr(&blocks[1], "secret_file"),
        &RValue::Str("/home/alice/secrets/push.token".into())
    );
}

#[test]
fn a_section_nested_in_a_webhook_sub_block_is_refused() {
    assert_eq!(
        refusal(concat!(
            "webhook push_alice {\n",
            "    slug = \"push-alice\";\n",
            "    mount = \"/webhooks/push-alice\";\n",
            "    signature { scheme = bearer-token; nested { header = \"x\"; } }\n",
            "}\n",
        )),
        "the `signature` block holds no sub-blocks, so `nested` has no meaning here"
    );
}

#[test]
fn a_section_nested_in_an_agent_hook_block_is_refused() {
    assert_eq!(
        refusal(concat!(
            "agent Assistant(slug: String) {\n",
            "    slug = slug;\n",
            "    start_hooks {\n",
            "        container = [\"tool rebuild\"];\n",
            "        nested { host = [\"x\"]; }\n",
            "    }\n",
            "}\n",
            "new alice_pa: Assistant(slug = \"alice-pa\");\n",
        )),
        "the `start_hooks` block holds no sub-blocks, so `nested` has no meaning here"
    );
}

#[test]
fn an_undeclared_name_in_a_section_value_is_still_refused() {
    assert!(
        refusal(concat!("server {\n", "    public_url = nowhere;\n", "}\n",)).contains("nowhere"),
    );
}

#[test]
fn a_constant_holding_a_matcher_over_an_f_string_is_not_a_leaf() {
    assert_eq!(
        refusal("const m = prefix f\"brenn:alice-desk.\";\n"),
        "a constant is a literal; an f-string is not one"
    );
}

// ── surfaces, instances and their classes ────────────────────────────────────

/// A component class, a surface, and one instance of the class in it.
fn surface_doc(class_body: &str, inst_body: &str) -> String {
    format!(
        concat!(
            "channel messages at \"brenn:alice-desk.in.messages\";\n",
            "component Panel {{\n{}}}\n",
            "surface alice_desk {{\n",
            "    grants = [subscribe];\n",
            "    new p1: Panel {{\n{}    }}\n",
            "}}\n",
        ),
        class_body, inst_body
    )
}

#[test]
fn a_surface_carries_its_components_and_their_bindings() {
    let config = resolved(&surface_doc(
        "    abi = dom; requires = [];\n    in messages;\n",
        "        chrome = \"bare\";\n        in messages <- messages { push_depth = 4; }\n",
    ));
    let surface = &config.surfaces[0];
    assert_eq!(surface.slug.value(), "alice_desk");
    let component = &surface.components[0];
    assert_eq!(component.instance.value(), "p1");
    assert_eq!(component.class.name.value(), "Panel");
    assert_eq!(component.attrs[0].0, "chrome");
    let binding = &component.bindings[0];
    assert_eq!(binding.chan, Some(RChanRef::Decl(ChanId(0))));
    let RTail::In(tail) = &binding.tail else {
        panic!("the binding points in");
    };
    assert!(matches!(
        tail.push_depth.as_ref().expect("a push depth").value,
        IntOrWord::Int(_)
    ));
}

/// A component instance's `parked_batch_depth` is a depth, so it is projected
/// out of the body rather than resolved among its values: a bare `unbounded`
/// there is the word, not a name the scope has to hold.
#[test]
fn a_components_parked_depth_is_projected() {
    let config = resolved(&surface_doc(
        "    abi = dom; requires = [];\n    in messages;\n",
        "        parked_batch_depth = unbounded;\n        in messages <- messages;\n",
    ));
    let component = &config.surfaces[0].components[0];
    let depth = component
        .parked_batch_depth
        .as_ref()
        .expect("a parked depth");
    let IntOrWord::Word(word) = depth else {
        panic!("the written token is a word: {depth:?}");
    };
    assert_eq!(word.as_str(), "unbounded");
    assert!(
        component.attrs.is_empty(),
        "the projected key does not also ride among the values"
    );

    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    optional in messages;\n",
            "        parked_batch_depth = \"unbounded\";\n",
        )),
        "expected an integer or a bare word, found a string"
    );
}

#[test]
fn an_unknown_instance_key_names_the_legal_set() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    optional in messages;\n",
            "        component_path = \"/lib/panel.wasm\";\n",
        )),
        "`component_path` is not a key of a component instance; expected one of \
         grants, chrome, send_burst, send_refill_secs, parked_batch_depth, config"
    );
}

#[test]
fn a_component_states_its_own_authority() {
    let config = resolved(&surface_doc(
        "    abi = dom; requires = [];\n    optional in messages;\n",
        "        acl subscribe [prefix \"brenn:alice-desk.\"];\n",
    ));
    let instance = &config.surfaces[0].components[0];
    assert_eq!(instance.acls.len(), 1);
    assert_eq!(instance.acls[0].plane.value(), "subscribe");
}

#[test]
fn a_port_a_class_does_not_declare_names_the_ones_it_does() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    optional in messages;\n    optional io tick;\n",
            "        in mesages <- messages;\n",
        )),
        "`Panel` declares no port `mesages`; it declares `in messages`, `io tick`"
    );
}

#[test]
fn a_port_bound_the_wrong_way_says_which_way_it_faces() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    io messages;\n",
            "        in messages <- messages;\n",
        )),
        "port `messages` is an `io` port, bound as `in`"
    );
}

#[test]
fn a_free_io_binding_needs_a_declared_io_port() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    in messages;\n",
            "        io messages { push_depth = 1; }\n",
        )),
        "port `messages` is an `in` port, bound as `io`"
    );
}

#[test]
fn a_class_declares_each_port_once() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    optional in messages;\n    optional out messages;\n",
            "",
        )),
        "port `messages` is declared twice"
    );
}

// ── the class's own contract: which ports an instance must connect ───────────

#[test]
fn an_instance_leaving_a_required_port_unconnected_is_refused() {
    let source = surface_doc("    abi = dom; requires = [];\n    in messages;\n", "");
    let errors = resolve_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "this instance leaves port `messages` of `Panel` unconnected; bind it, or the \
         class declares it `optional`"
    );
    // The refusal sits at the `new`; the contract it broke is the related site.
    assert_eq!(errors[0].line_col(), at(&source, "p1"));
    assert_eq!(errors[0].related[0].0, "the port is declared here");
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        at(&source, "in messages;").map(|(line, _)| line)
    );
}

/// A misspelled binding satisfies nothing: it names no declared port, so the
/// port it was meant for is still unconnected and both refusals stand. Nothing
/// normalizes a name into the port it resembles.
#[test]
fn a_misspelled_binding_leaves_the_required_port_unconnected() {
    let source = surface_doc(
        "    abi = dom; requires = [];\n    in messages;\n",
        "        in mesages <- messages;\n",
    );
    let errors = resolve_errors(&source);
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "`Panel` declares no port `mesages`; it declares `in messages`"
    );
    assert_eq!(
        errors[1].message,
        "this instance leaves port `messages` of `Panel` unconnected; bind it, or the \
         class declares it `optional`"
    );
}

#[test]
fn an_optional_port_may_be_left_unconnected() {
    let config = resolved(&surface_doc(
        "    abi = dom; requires = [];\n    optional in messages;\n    in theme;\n",
        "        in theme <- messages;\n",
    ));
    let component = &config.surfaces[0].components[0];
    assert_eq!(component.bindings.len(), 1);
    assert!(component.class.ports[0].optional);
    assert!(!component.class.ports[1].optional);
}

/// A free `io` tuning block claims its port: the port is connected to its own
/// page-local ring, which is a wiring decision and not an omission.
#[test]
fn a_free_io_tuning_counts_as_connecting_the_port() {
    let config = resolved(&surface_doc(
        "    abi = dom; requires = [];\n    io tick;\n",
        "        io tick { push_depth = 1; }\n",
    ));
    assert_eq!(config.surfaces[0].components[0].bindings.len(), 1);
}

/// Every required port is named, so a class with several unwired ports reports
/// each: the deployer sees the whole list rather than fixing them one per run.
#[test]
fn a_bodyless_instance_is_refused_once_per_required_port() {
    let errors = refusals(&surface_doc(
        "    abi = dom; requires = [];\n    in messages;\n    out acks;\n    optional io tick;\n",
        "",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(errors[0].contains("`messages`"), "{errors:?}");
    assert!(errors[1].contains("`acks`"), "{errors:?}");
}

/// The check reads the ports a body *named*, not the bindings that survived: a
/// binding dropped for an unreadable channel still claims its port, and saying
/// the port is unconnected too would be a second complaint about one mistake.
#[test]
fn a_binding_that_did_not_resolve_still_claims_its_port() {
    let errors = refusals(&surface_doc(
        "    abi = dom; requires = [];\n    in messages;\n",
        "        in messages <- \"alice.cmd\";\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("names no scheme"), "{errors:?}");
}

/// The contract is the class's, so it binds a consumer exactly as it binds a
/// surface-placed instance.
#[test]
fn a_consumer_leaving_a_required_port_unconnected_is_refused() {
    let errors = refusals(concat!(
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "    out events;\n",
        "    optional in commands;\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"sink.wasm\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0],
        "this instance leaves port `events` of `Sink` unconnected; bind it, or the \
         class declares it `optional`"
    );
}

#[test]
fn an_abi_is_one_of_the_shapes_the_runtime_loads() {
    assert_eq!(
        refusal(&surface_doc("    abi = wasm;\n", "")),
        "`wasm` is not an abi; expected one of dom, processor"
    );
}

// ── what a class declares it needs ───────────────────────────────────────────
//
// Class-side, so an uninstantiated spec is checked too: a spec is a contract
// whether or not this deployment uses it. What an *instance* grants against
// these lists is the authority suite's.

/// The spec side of deny-by-default, at either abi: a class that says nothing
/// about what it needs is refused where it is declared.
#[test]
fn a_class_states_what_it_requires() {
    for abi in ["dom", "processor"] {
        let source = format!("component Panel {{\n    abi = {abi};\n}}\n");
        assert_eq!(
            refusal(&source),
            "component `Panel` states no `requires`: what a component needs is \
             deny-by-default, so a class needing nothing is written `requires = [];` \
             rather than left out",
            "at abi {abi}"
        );
    }
}

/// One vocabulary for the spec and the grant, so a word neither side spells is
/// answered with the list both of them are written from.
#[test]
fn a_need_names_a_capability() {
    assert_eq!(
        refusal("component Panel {\n    abi = dom; requires = [frobnicate];\n}\n"),
        "`frobnicate` is not a capability a component holds; a spec's `requires` names \
         the same words a `grants` list does: `ports`, `store`, `log`, `alert`, \
         `config`, `mqtt` or `takeover`"
    );
    assert!(
        refusal("component Panel {\n    abi = dom; requires = []; optional = [subscribe];\n}\n")
            .starts_with("`subscribe` is not a capability a component holds"),
        "a transport plane is not a capability on either list"
    );
}

#[test]
fn a_need_is_listed_once() {
    let source = "component Panel {\n    abi = dom; requires = [ports, ports];\n}\n";
    let errors = resolve_errors(source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "`ports` is listed twice in `requires`; one statement of a need states it"
    );
    assert_eq!(errors[0].related[0].0, "it is listed here");
}

/// The two lists are answers to different questions, so one word cannot be both
/// answers at once.
#[test]
fn a_word_is_required_or_optional_and_not_both() {
    let source = "component Panel {\n    abi = dom; requires = [ports]; optional = [ports];\n}\n";
    let errors = resolve_errors(source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "`ports` is both required and optional; it is one or the other"
    );
    assert_eq!(errors[0].related[0].0, "listed optional here");
}

/// A dom class runs on a surface and nowhere else, so a word the surface cannot
/// implement is a need no legal placement satisfies — refused at the class
/// rather than at whichever surface happened to instantiate it.
#[test]
fn a_dom_class_cannot_need_what_no_surface_admits() {
    for lists in ["requires = [{word}]", "requires = []; optional = [{word}]"] {
        for word in ["store", "mqtt"] {
            let lists = lists.replace("{word}", word);
            let source = format!("component Panel {{\n    abi = dom; {lists};\n}}\n");
            assert_eq!(
                refusal(&source),
                format!(
                    "`{word}` is backend-only in v1; a surface-hosted component cannot be \
                     granted it, and a dom class runs nowhere else, so `{word}` cannot be \
                     satisfied at any placement"
                )
            );
        }
    }
}

/// A processor class admits both hosts, so no class-side host question applies
/// to it: an instance's own words are host-checked where they are written.
#[test]
fn a_processor_class_may_need_a_backend_only_capability() {
    let config = resolved(
        "component Sink {\n    abi = processor; requires = [store]; optional = [mqtt];\n}\n",
    );
    assert!(
        config.consumers.is_empty(),
        "declaring is not instantiating"
    );
}

/// The words the class stated reach the instance, which is what the fit check
/// has to compare against: dropping a host-questionable word here would leave
/// an over-grant with nothing to contradict it.
#[test]
fn a_backend_only_requirement_reaches_the_instance_that_carries_it() {
    let config = resolved(concat!(
        "component Sink {\n    abi = processor; requires = [store]; optional = [mqtt];\n}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    grants = [store];\n",
        "}\n",
    ));
    let consumer = &config.consumers[0];
    assert_eq!(
        consumer
            .class
            .requires
            .iter()
            .map(|word| word.value().word())
            .collect::<Vec<_>>(),
        vec!["store"]
    );
    assert_eq!(
        consumer
            .class
            .optional
            .iter()
            .map(|word| word.value().word())
            .collect::<Vec<_>>(),
        vec!["mqtt"]
    );
    assert_eq!(
        consumer
            .grants
            .as_ref()
            .expect("the instance states its grants")
            .words
            .iter()
            .map(|word| word.name.value().as_str())
            .collect::<Vec<_>>(),
        vec!["store"]
    );
}

/// Every refusal the pair holds is reported in one run: an author fixing one
/// word does not discover the next on the following build.
#[test]
fn every_bad_word_in_a_spec_is_reported_at_once() {
    let errors = refusals(
        "component Panel {\n    abi = dom; requires = [frobnicate, store]; optional = [gibber];\n}\n",
    );
    assert_eq!(errors.len(), 3, "{errors:?}");
    assert!(errors[0].starts_with("`frobnicate` is not"), "{errors:?}");
    assert!(errors[1].contains("`store` cannot be"), "{errors:?}");
    assert!(errors[2].starts_with("`gibber` is not"), "{errors:?}");
}

/// A class the spec check refused is a class instances say nothing new about:
/// the same answer the abi word gets, one indirection closer to where it was
/// written.
#[test]
fn an_instance_of_a_spec_refused_class_reports_nothing_further() {
    let errors = refusals(concat!(
        "component Panel {\n    abi = dom;\n    in messages;\n}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    new p1: Panel {}\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("states no `requires`"), "{errors:?}");
}

#[test]
fn a_component_instantiation_takes_no_arguments() {
    let source = concat!(
        "component Panel {\n    abi = dom; requires = [];\n}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    new p1: Panel(skin = \"bench\");\n",
        "}\n",
    );
    assert_eq!(
        refusal(source),
        "a component instantiation takes a body, not arguments; a component class \
         has no parameters, so per-instance values are written in its body"
    );
}

#[test]
fn an_instance_name_is_what_the_runtime_calls_the_component() {
    assert_eq!(
        refusal(
            &surface_doc("    abi = dom; requires = [];\n", "")
                .replace("new p1:", "new panel_one:")
        ),
        "`panel_one` is not a legal component instance name (lowercase, digits and \
         single `-`, starting with a letter or digit)"
    );
}

#[test]
fn one_surface_has_one_component_of_each_name() {
    let source = concat!(
        "component Panel {\n    abi = dom; requires = [];\n}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    new p1: Panel {}\n",
        "    new p1: Panel {}\n",
        "}\n",
    );
    assert_eq!(refusal(source), "this surface already has a component `p1`");
}

#[test]
fn a_surface_contains_components_and_not_agents() {
    let source = concat!(
        "agent Assistant(slug: String) {\n    slug = slug;\n}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    new p1: Assistant(slug = \"alice-pa\");\n",
        "}\n",
    );
    assert_eq!(
        refusal(source),
        "a surface contains components; `Assistant` is an agent class"
    );
}

#[test]
fn a_surface_contains_components_and_not_assemblies() {
    let source = concat!(
        "assembly Desk(slug: String) {\n",
        "    channel messages at f\"brenn:{slug}.in.messages\";\n",
        "}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    new p1: Desk(slug = \"alice-desk\");\n",
        "}\n",
    );
    assert_eq!(
        refusal(source),
        "a surface contains components; `Desk` is an assembly"
    );
}

// ── consumers ────────────────────────────────────────────────────────────────

#[test]
fn a_top_level_instance_is_a_consumer() {
    let config = resolved(concat!(
        "channel out_chan at \"brenn:alice.out.events\";\n",
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "    out events;\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    slug = \"alice-sink\";\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    grants = [ports];\n",
        "    acl publish [prefix \"brenn:alice.out.\"];\n",
        "    out events -> out_chan;\n",
        "}\n",
    ));
    let consumer = &config.consumers[0];
    assert_eq!(consumer.handle.dotted(), "alice_sink");
    assert_eq!(consumer.slug.value(), "alice-sink");
    assert_eq!(consumer.acls[0].plane.value(), "publish");
    assert_eq!(consumer.bindings[0].chan, Some(RChanRef::Decl(ChanId(0))));
    let path = consumer
        .attrs
        .iter()
        .find(|(key, _)| key == "component_path")
        .map(|(_, value)| value.value());
    assert_eq!(path, Some(&RValue::Str("/lib/brenn_sink.wasm".to_string())));
}

#[test]
fn a_dom_component_has_nowhere_to_render_at_top_level() {
    assert_eq!(
        refusal(concat!(
            "component Panel {\n    abi = dom; requires = [];\n}\n",
            "new alice_panel: Panel {}\n",
        )),
        "`Panel` is a dom component, which runs inside a surface; \
         a top-level instance has nowhere to render"
    );
}

#[test]
fn a_top_level_instance_needs_an_artifact_to_load() {
    assert_eq!(
        refusal(concat!(
            "component Sink {\n    abi = processor; requires = [];\n}\n",
            "new alice_sink: Sink {}\n",
        )),
        "a top-level instance is loaded from an artifact, and this instance of `Sink` \
         states no `component_path`"
    );
}

#[test]
fn a_literal_address_binds_where_no_channel_is_declared() {
    let config = resolved(concat!(
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "    out events;\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    out events -> \"local:brenn/takeover\";\n",
        "}\n",
    ));
    match &config.consumers[0].bindings[0].chan {
        Some(RChanRef::Addr(address)) => assert_eq!(address.value(), "local:brenn/takeover"),
        other => panic!("expected a literal address, found {other:?}"),
    }
}

// ── identity ─────────────────────────────────────────────────────────────────

/// A family whose identity is only ever its handle is told to rename it.
///
/// A repo has no `slug` attr, so advising one would send the operator to a key
/// the vocabulary refuses.
#[test]
fn an_illegal_default_slug_says_what_to_write_instead() {
    assert_eq!(
        refusal("repo alice_notes {\n    remote = \"https://example.com/notes.git\";\n}\n"),
        "`alice_notes` is not a legal repo identity (lowercase, digits, `-`); \
         rename the repo `alice-notes`"
    );
}

#[test]
fn every_repo_is_a_repo_and_not_all_of_them() {
    assert!(
        refusals("repo all {\n    remote = \"https://example.com/notes.git\";\n}\n")
            .contains(&"`all` is how the runtime says every repo, so it is not a repo name".into())
    );
}

#[test]
fn two_surfaces_may_not_resolve_to_one_identity() {
    let source = concat!(
        "surface alice_desk {\n    grants = [subscribe];\n}\n",
        "surface alice_wall {\n    grants = [subscribe];\n    slug = \"alice_desk\";\n}\n",
    );
    assert_eq!(
        refusal(source),
        "two surfaces resolve to the identity `alice_desk`"
    );
}

// ── statement tails ──────────────────────────────────────────────────────────

/// A tail value that resolves to nothing withholds its statement: a mount
/// tuned by a name that stands for nothing is not a mount, and half of one is
/// worse than none.
#[test]
fn a_mount_whose_tail_value_is_unresolvable_is_withheld() {
    let errors = refusals(concat!(
        "repo notes {\n    remote = \"https://example.com/notes.git\";\n}\n",
        "agent Assistant() {\n    mount notes { working_dir = nowhere; }\n}\n",
        "new alice: Assistant();\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

/// A mount naming neither a repo that exists nor a value that resolves reports
/// both in one compile, the way its sibling statement forms do.
#[test]
fn a_mount_reports_its_repo_and_its_tail_together() {
    let errors = refusals(concat!(
        "agent Assistant() {\n    mount elsewhere { working_dir = nowhere; }\n}\n",
        "new alice: Assistant();\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error.contains("elsewhere")),
        "{errors:?}"
    );
}

/// The binding twin: an unresolvable tail value withholds the binding, and the
/// instance with it, rather than leaving a port tuned differently than the
/// document says.
#[test]
fn a_binding_whose_tail_value_is_unresolvable_is_withheld() {
    let errors = refusals(concat!(
        "channel messages at \"brenn:alice-desk.in.messages\";\n",
        "component Panel {\n",
        "    abi = processor; requires = [];\n",
        "    in messages;\n",
        "}\n",
        "new p1: Panel {\n",
        "    component_path = \"/lib/brenn_panel.wasm\";\n",
        "    grants = [];\n",
        "    in messages <- messages { amplification = nowhere; }\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

// ── agent instantiation ──────────────────────────────────────────────────────

/// An agent class exercising every statement an agent body has, and the
/// declarations its instantiations name.
const AGENT_DOC: &str = concat!(
    "channel alice_cmd at \"brenn:alice.cmd\" { push_depth = 8; }\n",
    "repo notes {\n    remote = \"https://example.com/notes.git\";\n}\n",
    "mcp_server tools {\n    command = \"tools\";\n}\n",
    "agent Assistant(slug: String, cmd: Channel, ws: Repo, model: String = \"sonnet\") {\n",
    "    slug = slug;\n",
    "    name = f\"assistant for {slug}\";\n",
    "    model = model;\n",
    "    mount ws { working_dir = true; }\n",
    "    mcp_server tools;\n",
    "    mcp_server local {\n        command = \"local-tool\";\n    }\n",
    "    subscribe cmd { push_depth = 100; }\n",
    "    start_hooks {\n        container = [\"tool rebuild\"];\n    }\n",
    "    acl subscribe [prefix \"brenn:alice.\"];\n",
    "}\n",
);

/// One instantiation of [`AGENT_DOC`]'s class, with the arguments spelled by
/// the caller.
fn agent_doc(args: &str) -> String {
    format!("{AGENT_DOC}new alice_pa: Assistant({args});\n")
}

#[test]
fn an_agent_instantiation_expands_its_class() {
    let config = resolved(&agent_doc(
        "slug = \"alice-pa\", cmd = alice_cmd, ws = notes",
    ));
    let agent = &config.agents[0];
    assert_eq!(agent.handle.dotted(), "alice_pa");
    assert_eq!(agent.slug.value(), "alice-pa");
    assert_eq!(agent.class.value(), "Assistant");
    let name = agent.attrs.name.as_ref().expect("a name");
    assert_eq!(
        name.value.value(),
        &RValue::Str("assistant for alice-pa".into())
    );
    // The default, where the instantiation stated nothing.
    let model = agent.attrs.model.as_ref().expect("a model");
    assert_eq!(model.value.value(), &RValue::Str("sonnet".into()));
    assert_eq!(agent.mounts[0].repo.dotted(), "notes");
    assert!(agent.mounts[0].tail.working_dir.is_some());
    assert_eq!(agent.subs[0].chan, RChanRef::Decl(ChanId(0)));
    assert_eq!(agent.acls[0].plane.value(), "subscribe");
    assert_eq!(agent.hooks[0].kindword.value(), "start_hooks");
    assert!(agent.hooks[0].host.is_none());
    assert_eq!(agent.mcps.len(), 2);
}

#[test]
fn two_instantiations_of_one_class_are_two_agents() {
    let source = format!(
        "{}new alice_pa: Assistant(slug = \"alice-pa\", cmd = alice_cmd, ws = notes);\n\
         new bob_pa: Assistant(slug = \"bob-pa\", cmd = alice_cmd, ws = notes);\n",
        AGENT_DOC
    );
    let config = resolved(&source);
    assert_eq!(config.agents.len(), 2);
    assert_eq!(config.agents[1].slug.value(), "bob-pa");
}

#[test]
fn an_agent_handle_is_its_slug_where_it_states_none() {
    let source = concat!(
        "agent Assistant() {\n    name = \"Assistant\";\n}\n",
        "new alice_pa: Assistant();\n",
    );
    assert_eq!(
        refusal(source),
        "`alice_pa` is not a legal agent identity (lowercase, digits, `-`); \
         state one: `slug = \"alice-pa\";`"
    );
}

#[test]
fn an_agent_instantiation_takes_arguments_and_not_a_body() {
    let source = format!(
        "{}new alice_pa: Assistant(slug = \"alice-pa\", cmd = alice_cmd, ws = notes) \
         {{ model = \"opus\"; }}\n",
        AGENT_DOC
    );
    assert_eq!(
        refusal(&source),
        "an agent instantiation takes arguments, not a body; per-instance values \
         are class parameters"
    );
}

#[test]
fn an_argument_names_a_parameter_the_class_has() {
    assert_eq!(
        refusal(&agent_doc(
            "slug = \"alice-pa\", cmd = alice_cmd, ws = notes, skin = \"bench\""
        )),
        "`Assistant` has no parameter `skin`; it takes `slug: String`, `cmd: Channel`, \
         `ws: Repo`, `model: String`"
    );
}

/// An instantiation that never reached the model is still a declaration.
///
/// A grant naming it must not report the handle as nothing: the one mistake is
/// the argument, and the grants that mention the agent are collateral.
#[test]
fn an_agent_dropped_before_emission_is_still_a_principal() {
    let doc = format!(
        concat!(
            "{}",
            "grant alice_pa subscribe exact \"local:alice.cmd\";\n",
            "grant alice_pa publish exact \"local:alice.cmd\";\n",
        ),
        agent_doc("slug = \"alice-pa\", cmd = alice_cmd, ws = notes, skin = \"bench\"")
    );
    assert_eq!(
        refusal(&doc),
        "`Assistant` has no parameter `skin`; it takes `slug: String`, `cmd: Channel`, \
         `ws: Repo`, `model: String`"
    );
}

/// The same for a consumer whose class did not resolve.
#[test]
fn a_consumer_whose_class_is_unknown_is_still_a_principal() {
    assert_eq!(
        refusals(concat!(
            "new alice_box: Missing { };\n",
            "grant alice_box subscribe exact \"local:alice.cmd\";\n",
        )),
        vec!["`Missing` is not declared in this file".to_string()]
    );
}

/// And for a consumer whose class resolves but may not be placed here.
///
/// The class is a symbol, so the withhold that keeps the grant honest is the
/// one made before the placement rules are read, not the one after.
#[test]
fn a_consumer_refused_its_placement_is_still_a_principal() {
    assert_eq!(
        refusal(concat!(
            "component Panel { abi = dom; requires = []; }\n",
            "new alice_box: Panel { };\n",
            "grant alice_box subscribe exact \"local:alice.cmd\";\n",
        )),
        "`Panel` is a dom component, which runs inside a surface; a top-level \
         instance has nowhere to render"
    );
}

/// An assembly handle is not an entity, so a grant may not name it.
///
/// Both directions of the one deliberate hole in the withholding invariant:
/// the handle stays a non-principal whether the instantiation expanded or not.
const POD: &str = concat!(
    "assembly Pod(slug: String) {\n",
    "    channel messages at f\"brenn:{slug}.messages\";\n",
    "}\n",
);

#[test]
fn a_grant_may_not_name_an_assembly_handle() {
    let source = format!(
        "{POD}new alice: Pod(slug = \"alice\");\ngrant alice subscribe exact \"local:alice.cmd\";\n"
    );
    assert_eq!(
        refusal(&source),
        "`alice` is not a principal; a grant names a surface, an agent, \
         a remote or a consumer"
    );
}

#[test]
fn an_assembly_that_did_not_expand_keeps_its_handle_a_non_principal() {
    let source = format!(
        "{POD}new alice: Pod(name = \"alice\");\ngrant alice subscribe exact \"local:alice.cmd\";\n"
    );
    assert_eq!(
        refusals(&source),
        vec![
            "`Pod` has no parameter `name`; it takes `slug: String`".to_string(),
            "`Pod` takes `slug`, and this instantiation states no value for it".to_string(),
            "`alice` is not a principal; a grant names a surface, an agent, \
             a remote or a consumer"
                .to_string(),
        ]
    );
}

/// What a refused instantiation would have stamped is declared all the same.
///
/// The entities never reach the model, so absence there says nothing about
/// whether the operator wrote them; reporting their grants would fan one bad
/// argument out into one lie per grant.
#[test]
fn a_grant_on_a_stamped_entity_survives_a_refused_instantiation() {
    let source = concat!(
        "agent Assistant(slug: String) {\n",
        "    slug = slug;\n",
        "    model = \"sonnet\";\n",
        "}\n",
        "assembly Pod(slug: String) {\n",
        "    new pa: Assistant(slug = slug);\n",
        "}\n",
        "new alice: Pod(name = \"alice\");\n",
        "grant alice.pa subscribe exact \"local:alice.cmd\";\n",
    );
    assert_eq!(
        refusals(source),
        vec![
            "`Pod` has no parameter `name`; it takes `slug: String`".to_string(),
            "`Pod` takes `slug`, and this instantiation states no value for it".to_string(),
        ]
    );
}

/// A knot of instantiations that wait on each other stamps nothing, and the
/// entities it would have stamped stay declared.
///
/// The deadlock is the only thing wrong here; a grant naming an entity inside
/// the knot must not add a second, false diagnostic.
#[test]
fn a_grant_on_a_stamped_entity_survives_a_mutual_wait_knot() {
    let source = concat!(
        "agent Assistant(slug: String) {\n",
        "    slug = slug;\n",
        "    model = \"sonnet\";\n",
        "}\n",
        "assembly Pod(slug: String, feed: Channel) {\n",
        "    channel messages at f\"brenn:{slug}.messages\";\n",
        "    new pa: Assistant(slug = slug);\n",
        "}\n",
        "new alice: Pod(slug = \"alice\", feed = bob.messages);\n",
        "new bob: Pod(slug = \"bob\", feed = alice.messages);\n",
        "grant alice.pa subscribe exact \"local:alice.cmd\";\n",
    );
    assert_eq!(
        refusals(source),
        vec![
            "these instantiations wait on each other, so none of them can expand: \
             `alice`, `bob`"
                .to_string()
        ]
    );
}

/// A withheld repo is declared, and a grant still may not name one.
///
/// The two mistakes are independent, so the first compile states both rather
/// than making the operator fix the body to discover the grant.
#[test]
fn a_grant_naming_a_withheld_repo_still_says_a_repo_is_no_principal() {
    assert_eq!(
        refusals(concat!(
            "repo notes {\n    remote = nowhere;\n}\n",
            "grant notes publish exact \"local:alice.cmd\";\n",
        )),
        vec![
            "`nowhere` is not declared in this file".to_string(),
            "`notes` is not a principal; a grant names a surface, an agent, \
             a remote or a consumer"
                .to_string(),
        ]
    );
}

#[test]
fn an_argument_is_written_once() {
    assert_eq!(
        refusal(&agent_doc(
            "slug = \"alice-pa\", slug = \"bob-pa\", cmd = alice_cmd, ws = notes"
        )),
        "argument `slug` is written twice"
    );
}

#[test]
fn a_parameter_with_no_default_is_stated() {
    assert_eq!(
        refusal(&agent_doc("slug = \"alice-pa\", cmd = alice_cmd")),
        "`Assistant` takes `ws`, and this instantiation states no value for it"
    );
}

#[test]
fn an_argument_has_the_type_its_parameter_declared() {
    assert_eq!(
        refusal(&agent_doc("slug = 7, cmd = alice_cmd, ws = notes")),
        "parameter `slug` is a `String`; this is an integer"
    );
}

#[test]
fn an_entity_parameter_is_named_and_not_written() {
    assert_eq!(
        refusal(&agent_doc(
            "slug = \"alice-pa\", cmd = \"brenn:alice.cmd\", ws = notes"
        )),
        "parameter `cmd` is a `Channel`; name one, rather than writing a string"
    );
}

#[test]
fn a_channel_parameter_names_a_channel() {
    assert_eq!(
        refusal(&agent_doc("slug = \"alice-pa\", cmd = notes, ws = notes")),
        "`notes` names a repo, not a channel"
    );
}

#[test]
fn a_repo_parameter_names_a_repo() {
    assert_eq!(
        refusal(&agent_doc(
            "slug = \"alice-pa\", cmd = alice_cmd, ws = alice_cmd"
        )),
        "parameter `ws` is a `Repo`; `alice_cmd` is a channel"
    );
}

#[test]
fn an_entity_parameter_has_no_default() {
    let source = "agent Assistant(ws: Repo = \"notes\") {\n    name = \"Assistant\";\n}\n";
    assert_eq!(
        refusal(source),
        "a `Repo` parameter names an entity, and a default is a literal; \
         every instantiation states this one"
    );
}

#[test]
fn a_mount_names_a_repo() {
    let source = concat!(
        "channel alice_cmd at \"brenn:alice.cmd\";\n",
        "agent Assistant() {\n    name = \"Assistant\";\n    mount alice_cmd;\n}\n",
        "new alice-pa: Assistant();\n",
    );
    assert_eq!(
        refusal(source),
        "a mount names a repo; `alice_cmd` is a channel"
    );
}

#[test]
fn an_mcp_reference_names_an_mcp_server() {
    let source = concat!(
        "repo notes {\n    remote = \"https://example.com/notes.git\";\n}\n",
        "agent Assistant() {\n    name = \"Assistant\";\n    mcp_server notes;\n}\n",
        "new alice-pa: Assistant();\n",
    );
    assert_eq!(
        refusal(source),
        "`notes` names a repo, not an mcp server; write a body to define one here"
    );
}

#[test]
fn an_inline_mcp_definition_shadows_nothing() {
    let source = concat!(
        "mcp_server tools {\n    command = \"tools\";\n}\n",
        "agent Assistant() {\n",
        "    name = \"Assistant\";\n",
        "    mcp_server tools {\n        command = \"other\";\n    }\n",
        "}\n",
        "new alice-pa: Assistant();\n",
    );
    assert_eq!(
        refusal(source),
        "`tools` is already an mcp server; nothing shadows here"
    );
}

#[test]
fn a_class_body_resolves_in_the_file_that_declared_it() {
    let config = resolved_tree(&[
        (
            "",
            concat!(
                "use pods::Assistant;\n",
                "new alice-pa: Assistant(slug = \"alice-pa\");\n",
            ),
        ),
        (
            "pods",
            concat!(
                "const flavor = \"sandbox\";\n",
                "agent Assistant(slug: String) {\n",
                "    slug = slug;\n",
                "    name = f\"{flavor} assistant\";\n",
                "}\n",
            ),
        ),
    ]);
    let name = config.agents[0].attrs.name.as_ref().expect("a name");
    assert_eq!(name.value.value(), &RValue::Str("sandbox assistant".into()));
}

#[test]
fn an_instantiation_does_not_repeat_a_refused_parameter_type() {
    // The definition site refuses the type; binding an argument against a type
    // the language does not have would only say it again, and wrongly.
    let messages = refusals(
        "\
agent Pa(mode: Surface) {
    slug = \"alice\";
}

new alice: Pa(mode = \"panel\");
",
    );
    assert_eq!(
        messages,
        ["`Surface` is not a parameter type; expected one of String, Int, Bool, Table, Channel, Agent, Repo".to_string()]
    );
}

#[test]
fn a_surface_slug_the_wire_cannot_carry_is_refused() {
    assert_eq!(
        refusal(concat!(
            "surface alice_desk {\n",
            "    grants = [subscribe];\n",
            "    slug = \"alice/desk\";\n",
            "}\n",
        )),
        "`alice/desk` is not a legal surface identity (letters, digits, `.`, `_`, \
         `~`, `-`); state one: `slug = \"alice-desk\";`"
    );
}

#[test]
fn a_slug_of_nothing_legal_is_told_to_write_a_name() {
    assert_eq!(
        refusal(concat!(
            "surface alice_desk {\n",
            "    grants = [subscribe];\n",
            "    slug = \"///\";\n",
            "}\n",
        )),
        "`///` is not a legal surface identity (letters, digits, `.`, `_`, `~`, \
         `-`); state one: `slug = \"a-name\";`"
    );
}

#[test]
fn a_component_instance_name_carries_no_double_hyphen() {
    assert_eq!(
        refusal(
            &surface_doc("    abi = dom; requires = [];\n", "")
                .replace("new p1:", "new panel--one:")
        ),
        "`panel--one` is not a legal component instance name (lowercase, digits and \
         single `-`, starting with a letter or digit)"
    );
}

#[test]
fn a_grant_names_a_plane_the_language_has() {
    assert_eq!(
        refusal(concat!(
            "remote bob_pod {\n",
            "    token_file = \"/t\";\n",
            "    grants = [subscribe];\n",
            "}\n",
            "grant bob_pod pubish prefix \"brenn:alice-desk.\";\n",
        )),
        "`pubish` is not a plane; a grant names `subscribe` or `publish`"
    );
}

#[test]
fn a_grant_names_something_authority_can_be_held_by() {
    assert_eq!(
        refusal(concat!(
            "channel alice_cmd at \"brenn:alice.cmd\";\n",
            "grant alice_cmd subscribe prefix \"brenn:alice-desk.\";\n",
        )),
        "`alice_cmd` is not a principal; a grant names a surface, an agent, \
         a remote or a consumer"
    );
}

// ── addresses read as a whole document ───────────────────────────────────────

#[test]
fn two_channels_may_not_declare_one_address() {
    let errors = compile(concat!(
        "channel alice_in at \"brenn:alice-desk.in.messages\";\n",
        "channel alice_also at \"brenn:alice-desk.in.messages\";\n",
    ))
    .expect_err("one address, two identities");
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "two channels declare the address `brenn:alice-desk.in.messages`"
    );
    assert_eq!(errors[0].related.len(), 1);
    assert_eq!(errors[0].related[0].0, "`alice_in` declares it here");
}

#[test]
fn a_misspelled_matcher_kind_is_refused_at_the_matcher() {
    let errors = compile(concat!(
        "channel alice_in at \"brenn:alice-desk.in.messages\";\n",
        "surface panel {\n    grants = [];\n",
        "    acl subscribe [exakt \"brenn:alice-desk.in.messages\"];\n}\n",
    ))
    .expect_err("`exakt` spells neither kind");
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "`exakt` is not a matcher kind; matchers are `exact`, `prefix`, `topic_filter`, \
         `endpoint`, `client`"
    );
    // The kind word, not the address it precedes: the typo is in the keyword.
    assert_eq!(errors[0].line_col(), Some((4, 20)));
}

#[test]
fn a_misspelled_matcher_kind_in_a_grant_is_refused_too() {
    assert_eq!(
        refusal(concat!(
            "surface panel {\n    grants = [];\n}\n",
            "grant panel subscribe exakt \"brenn:alice-desk.in.messages\";\n",
        )),
        "`exakt` is not a matcher kind; matchers are `exact`, `prefix`, `topic_filter`, \
         `endpoint`, `client`"
    );
}

/// A tuning is not an identity, so the address-uniqueness check does not see
/// one. Whether the address it names may be tuned at all is derivation's, and a
/// declarable address like this one is refused there.
#[test]
fn a_tuning_is_not_a_declaration_and_collides_with_nothing() {
    let config = resolved(concat!(
        "channel alice_in at \"brenn:alice-desk.in.messages\";\n",
        "channel at \"brenn:alice-desk.in.messages\" {\n    push_depth = 8;\n}\n",
        "channel at prefix \"brenn:alice-desk.\" {\n    push_depth = 8;\n}\n",
    ));
    assert_eq!(config.channels.len(), 1);
    assert_eq!(config.tunings.len(), 2);
    let prefixes: Vec<bool> = config
        .tunings
        .iter()
        .map(|tuning| tuning.is_prefix)
        .collect();
    assert_eq!(prefixes, vec![false, true]);
}

/// A declaration names one channel, so the family word has nothing to mean
/// there and is refused rather than dropped. The refused channel still takes
/// its id slot (the prepass minted the id); the second declaration here checks
/// that: dropping the entry would leave `bob`'s id pointing one place short,
/// and the position assert in `emit_channel` would fire.
///
/// The surface binds the refused handle, so the one-diagnostic assertion is
/// about the consequences too: the entry stays reachable under its family
/// address, and nothing downstream says a second thing about it.
#[test]
fn a_prefix_on_a_declaration_is_refused() {
    let source = concat!(
        "channel alice at prefix \"brenn:alice.\" {\n    push_depth = 4;\n}\n",
        "channel bob at \"brenn:bob.in.messages\";\n",
        "component Panel {\n    abi = dom; requires = [];\n    in messages;\n}\n",
        "surface alice_desk {\n    grants = [subscribe];\n",
        "    new p1: Panel {\n        in messages <- alice;\n    }\n}\n",
    );
    let diagnostics = resolve_errors(source);
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(
        diagnostics[0].message,
        "`prefix` names the family a handle-less tuning block tunes; a declaration \
         names exactly one channel, written `channel alice at \
         \"brenn:alice.in.messages\"`"
    );
    // The flag is a bare `bool` in the AST; the address is the nearest anchor.
    assert_eq!(diagnostics[0].line_col(), at(source, "\"brenn:alice."));
}

/// The word is refused off the AST flag, so an address the prepass could not
/// resolve does not hide it: one compile reports both, rather than the author
/// fixing the address and meeting the `prefix` refusal on the next run.
#[test]
fn a_prefix_refusal_survives_an_address_that_does_not_resolve() {
    assert_eq!(
        refusals("channel alice at prefix \"alice.\" {\n    push_depth = 4;\n}\n"),
        vec![
            "address `alice.` names no scheme; expected one of \
             brenn:, ephemeral:, local:, webhook:, mqtt:"
                .to_string(),
            "`prefix` names the family a handle-less tuning block tunes; a declaration \
             names exactly one channel, written `channel alice at \
             \"brenn:alice.in.messages\"`"
                .to_string(),
        ]
    );
}

#[test]
fn a_stamped_prefix_refusal_survives_an_address_that_does_not_resolve() {
    assert_eq!(
        refusals(concat!(
            "assembly Pod(slug: String) {\n",
            "    channel messages at prefix f\"{slug}.\";\n",
            "}\n",
            "new alice: Pod(slug = \"alice\");\n",
        )),
        vec![
            "address `alice.` names no scheme; expected one of \
             brenn:, ephemeral:, local:, webhook:, mqtt:"
                .to_string(),
            "`prefix` names the family a handle-less tuning block tunes; a declaration \
             names exactly one channel, written `channel alice at \
             \"brenn:alice.in.messages\"`"
                .to_string(),
        ]
    );
}

#[test]
fn every_repeat_of_an_address_is_reported_against_the_first_holder() {
    let errors = compile(concat!(
        "channel alice_in at \"brenn:alice-desk.in.messages\";\n",
        "channel alice_also at \"brenn:alice-desk.in.messages\";\n",
        "channel alice_third at \"brenn:alice-desk.in.messages\";\n",
    ))
    .expect_err("one address, three identities");
    assert_eq!(errors.len(), 2);
    for error in &errors {
        assert_eq!(error.related[0].0, "`alice_in` declares it here");
    }
}

#[test]
fn every_repeat_of_an_identity_is_reported_against_the_first_holder() {
    let errors = compile(concat!(
        "surface one {\n    grants = [];\n    slug = \"panel\";\n}\n",
        "surface two {\n    grants = [];\n    slug = \"panel\";\n}\n",
        "surface three {\n    grants = [];\n    slug = \"panel\";\n}\n",
    ))
    .expect_err("one slug, three surfaces");
    assert_eq!(errors.len(), 2);
    for error in &errors {
        assert_eq!(error.related[0].0, "the other one is here");
        // The first surface holds the identity; both repeats cite it.
        assert_eq!(
            error.related[0].1.line_col_inner().map(|p| p.line + 1),
            Some(3)
        );
    }
}

#[test]
fn a_binding_names_a_declared_channel_rather_than_its_address() {
    assert_eq!(
        refusal(&surface_doc(
            "    abi = dom; requires = [];\n    in messages;\n",
            "        in messages <- \"brenn:alice-desk.in.messages\";\n",
        )),
        "`brenn:alice-desk.in.messages` is the address channel `messages` declares; \
         name the channel, not its address"
    );
}

#[test]
fn a_subscription_names_a_declared_channel_rather_than_its_address() {
    assert_eq!(
        refusal(concat!(
            "channel alice_cmd at \"brenn:alice.cmd\";\n",
            "agent Assistant() {\n",
            "    slug = \"alice-pa\";\n",
            "    model = \"sonnet\";\n",
            "    subscribe \"brenn:alice.cmd\";\n",
            "}\n",
            "new alice_pa: Assistant();\n",
        )),
        "`brenn:alice.cmd` is the address channel `alice_cmd` declares; \
         name the channel, not its address"
    );
}

#[test]
fn an_exact_matcher_names_a_declared_channel_rather_than_its_address() {
    assert_eq!(
        refusal(concat!(
            "channel alice_cmd at \"brenn:alice.cmd\";\n",
            "remote bob_pod {\n",
            "    token_file = \"/t\";\n",
            "    grants = [subscribe];\n",
            "    acl subscribe [exact \"brenn:alice.cmd\"];\n",
            "}\n",
        )),
        "`brenn:alice.cmd` is the address channel `alice_cmd` declares; \
         name the channel, not its address"
    );
}

#[test]
fn a_prefix_matcher_over_a_declared_address_is_a_family_and_stands() {
    resolved(concat!(
        "channel alice_cmd at \"brenn:alice.cmd\";\n",
        "remote bob_pod {\n",
        "    token_file = \"/t\";\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [prefix \"brenn:alice.cmd\"];\n",
        "}\n",
    ));
}

#[test]
fn every_carrier_of_a_literal_address_is_held_to_the_one_spelling_rule() {
    let messages = refusals(concat!(
        "channel alice_cmd at \"brenn:alice.cmd\";\n",
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "    out events;\n",
        "}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [exact \"brenn:alice.cmd\"];\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    grants = [ports];\n",
        "    acl publish [exact \"brenn:alice.cmd\"];\n",
        "    out events -> \"brenn:alice.cmd\";\n",
        "}\n",
        "grant alice_desk subscribe exact \"brenn:alice.cmd\";\n",
    ));
    let expected = "`brenn:alice.cmd` is the address channel `alice_cmd` declares; \
                    name the channel, not its address";
    // Four carriers: a surface acl, a consumer acl, a consumer binding, a grant.
    assert_eq!(messages, vec![expected; 4]);
}

/// A matcher is writable in any value position, and the one-spelling rule
/// deliberately does not reach there: an attribute value names no channel.
#[test]
fn a_matcher_in_an_attribute_value_is_outside_the_one_spelling_rule() {
    resolved(concat!(
        "channel alice_cmd at \"brenn:alice.cmd\";\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    skin = exact \"brenn:alice.cmd\";\n",
        "}\n",
    ));
}

#[test]
fn a_literal_address_no_channel_declares_is_how_a_local_plane_is_named() {
    let config = resolved(concat!(
        "component Sink {\n    abi = processor; requires = [];\n    out events;\n}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"sink.wasm\";\n",
        "    out events -> \"local:brenn/takeover\";\n",
        "}\n",
    ));
    match &config.consumers[0].bindings[0].chan {
        Some(RChanRef::Addr(address)) => assert_eq!(address.value(), "local:brenn/takeover"),
        other => panic!("expected a literal address, found {other:?}"),
    }
}

// ── entity bodies report every refusal they hold ─────────────────────────────

#[test]
fn every_bad_value_in_a_surface_body_reaches_one_report() {
    let errors = refusals(concat!(
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    skin = nowhere;\n",
        "    allowed_users = elsewhere;\n",
        "    acl subscribe [exact missing];\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 3, "{errors:?}");
    for name in ["nowhere", "elsewhere", "missing"] {
        assert!(
            errors.iter().any(|error| error.contains(name)),
            "{name}: {errors:?}"
        );
    }
}

#[test]
fn a_surface_whose_body_was_refused_is_not_in_the_model() {
    // The refusal is what fails the compile; the point here is that the
    // half-resolved surface never reaches a later pass. Two surfaces sharing
    // one identity is a refusal of its own, and it is absent.
    let errors = refusals(concat!(
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "    skin = nowhere;\n",
        "}\n",
        "surface alice_wall {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_bad_attr_elsewhere_does_not_hide_an_illegal_slug() {
    let errors = refusals(concat!(
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    slug = \"alice/desk\";\n",
        "    skin = nowhere;\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("is not a legal surface identity")),
        "{errors:?}"
    );
}

#[test]
fn a_refused_slug_value_is_not_checked_as_an_identity() {
    // The slug's own value was refused, so there is no text to check the
    // charset of: one diagnostic, not a second about a substituted value.
    let errors = refusals(concat!(
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    slug = nowhere;\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_consumer_whose_body_was_refused_is_not_in_the_model() {
    // The same rule every other entity follows: a substituted value never
    // reaches a later pass, so the two consumers sharing one identity is not
    // reported and the one real mistake is.
    let errors = refusals(concat!(
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    slug = \"sink\";\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    store_path = nowhere;\n",
        "}\n",
        "new bob_sink: Sink {\n",
        "    slug = \"sink\";\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_consumer_with_a_refused_slug_value_does_not_fall_back_to_its_handle() {
    // A dropped `slug` key would name the consumer after its handle, and that
    // is a different identity — here one another consumer already holds. The
    // refusal is the only diagnostic; no invented collision.
    let errors = refusals(concat!(
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    slug = nowhere;\n",
        "}\n",
        "new bob_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    slug = \"alice_sink\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_refused_component_body_withholds_its_surface() {
    // A component's body is part of the surface's: a substituted value in one
    // leaves the surface half-resolved, so the identity collision it would
    // have raised is not reported.
    let errors = refusals(concat!(
        "component Panel {\n    abi = dom; requires = [];\n}\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "    new panel: Panel {\n",
        "        chrome = nowhere;\n",
        "    }\n",
        "}\n",
        "surface alice_wall {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_channel_with_a_refused_body_keeps_its_position() {
    // A channel is emitted whatever its body did: the id was minted with the
    // address, so dropping it would leave every later id pointing one place
    // short. Both bad values are reported and the attrs are discarded.
    let errors = refusals(concat!(
        "channel first at \"brenn:alice-desk.in.p1\" {\n",
        "    description = nowhere;\n",
        "    send_rate = elsewhere;\n",
        "}\n",
        "channel second at \"brenn:alice-desk.out.p1\";\n",
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    acl subscribe [exact second];\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error.contains("elsewhere")),
        "{errors:?}"
    );
}

#[test]
fn every_bad_value_in_an_agent_body_reaches_one_report() {
    let errors = refusals(concat!(
        "agent Assistant() {\n",
        "    slug = \"alice-pa\";\n",
        "    model = nowhere;\n",
        "    working_dir = elsewhere;\n",
        "}\n",
        "new alice_pa: Assistant();\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error.contains("elsewhere")),
        "{errors:?}"
    );
}

#[test]
fn every_bad_value_in_a_named_definition_reaches_one_report() {
    let errors = refusals(concat!(
        "repo notes {\n",
        "    remote = nowhere;\n",
        "    auto_pull = elsewhere;\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    // Both messages are the two refused values: the handle is legal, so
    // neither of them is an identity error.
    for name in ["nowhere", "elsewhere"] {
        assert!(
            errors.iter().any(|error| error.contains(name)),
            "{name}: {errors:?}"
        );
    }
}

#[test]
fn a_webhook_block_reports_its_bad_value_and_its_nested_block_at_once() {
    let errors = refusals(concat!(
        "webhook push_alice {\n",
        "    slug = \"push-alice\";\n",
        "    mount = \"/webhooks/push-alice\";\n",
        "    signature { scheme = bearer-token; header = nowhere; nested { header = \"x\"; } }\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| {
            error == "the `signature` block holds no sub-blocks, so `nested` has no meaning here"
        }),
        "{errors:?}"
    );
}

#[test]
fn a_withheld_named_definition_still_has_its_handle_checked() {
    // A repo's identity is its handle, so the spelling question stands
    // whatever the body did — and the pass that would have asked it is one the
    // withheld repo never reaches.
    let errors = refusals("repo alice_notes {\n    remote = nowhere;\n}\n");
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
    assert!(
        errors.iter().any(|error| error
            == "`alice_notes` is not a legal repo identity (lowercase, digits, `-`); \
                rename the repo `alice-notes`"),
        "{errors:?}"
    );
}

#[test]
fn a_withheld_mcp_server_has_no_identity_to_check() {
    // An mcp server has no wire identity, so its handle is not an identity to
    // spell-check: the refused value is the only diagnostic.
    let errors = refusals("mcp_server alice_tools {\n    command = nowhere;\n}\n");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_remote_whose_body_was_refused_is_not_in_the_model() {
    // Two files each declaring `bob_pod` resolve to one identity; the refused
    // one is withheld, so the collision is not reported and the one real
    // mistake is.
    let errors = refusals_tree(&[
        (
            "",
            concat!(
                "remote bob_pod {\n",
                "    token_file = nowhere;\n",
                "    grants = [subscribe];\n",
                "}\n",
            ),
        ),
        (
            "other",
            concat!(
                "remote bob_pod {\n",
                "    token_file = \"/t\";\n",
                "    grants = [subscribe];\n",
                "}\n",
            ),
        ),
    ]);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_refused_agent_slug_value_is_not_checked_as_an_identity() {
    // An agent spells identities in kebab and agent handles are snake case, so
    // a fallback to the handle here would tell the operator to state a slug
    // they did state. The refusal is the only diagnostic.
    let errors = refusals(concat!(
        "agent Assistant() {\n",
        "    slug = nowhere;\n",
        "}\n",
        "new alice_pa: Assistant();\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_refused_hook_block_withholds_its_agent() {
    // A hook block is part of the agent's body: an agent missing one is
    // half-resolved, so it is withheld and the identity it shares with the
    // second agent is not reported as a collision.
    let errors = refusals(concat!(
        "agent Assistant() {\n",
        "    slug = \"alice-pa\";\n",
        "    start_hooks {\n        host = nowhere;\n    }\n",
        "}\n",
        "agent Other() {\n",
        "    slug = \"alice-pa\";\n",
        "}\n",
        "new alice_pa: Assistant();\n",
        "new bob_pa: Other();\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_refused_inline_mcp_body_withholds_its_agent() {
    let errors = refusals(concat!(
        "agent Assistant() {\n",
        "    slug = \"alice-pa\";\n",
        "    mcp_server local {\n        command = nowhere;\n    }\n",
        "}\n",
        "agent Other() {\n",
        "    slug = \"alice-pa\";\n",
        "}\n",
        "new alice_pa: Assistant();\n",
        "new bob_pa: Other();\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_surface_missing_a_whole_component_is_withheld_too() {
    // A component whose class did not resolve leaves a larger hole than a
    // refused value in one does; the surface is withheld the same way, so the
    // identity it shares with the second surface is not reported.
    let errors = refusals(concat!(
        "surface alice_desk {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "    new panel: Nowhere;\n",
        "}\n",
        "surface alice_wall {\n",
        "    grants = [subscribe];\n",
        "    slug = \"panel\";\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("Nowhere"), "{errors:?}");
}

#[test]
fn a_grant_to_a_withheld_entity_is_not_a_missing_principal() {
    // The consumer was declared and is withheld for a mistake of its own;
    // reporting its grants as naming nothing would fan one bad value out into
    // one false diagnostic per grant.
    let errors = refusals(concat!(
        "component Sink {\n",
        "    abi = processor; requires = [];\n",
        "}\n",
        "new alice_sink: Sink {\n",
        "    component_path = \"/lib/brenn_sink.wasm\";\n",
        "    store_path = nowhere;\n",
        "}\n",
        "grant alice_sink subscribe prefix \"brenn:alice-desk.\";\n",
    ));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

#[test]
fn a_tuning_with_a_refused_body_reports_every_bad_value() {
    // A tuning is not in the id space, so a refused body drops it outright —
    // the counterpart of the declaration exception, and both bad values are
    // still reported.
    let errors = refusals(concat!(
        "channel at prefix \"brenn:alice-desk.\" {\n",
        "    description = nowhere;\n",
        "    send_rate = elsewhere;\n",
        "}\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    for name in ["nowhere", "elsewhere"] {
        assert!(
            errors.iter().any(|error| error.contains(name)),
            "{name}: {errors:?}"
        );
    }
}

#[test]
fn a_refused_value_in_an_assembly_body_is_reported_once_per_instantiation() {
    // Expansion clones the assembly's items, spans and all, so each stamped
    // entity carries the span the body was written with. Each instantiation
    // resolves under its own body verdict: two of them report two refusals,
    // and neither surface reaches the model.
    let errors = refusals(concat!(
        "assembly Pod(look: String) {\n",
        "    surface panel {\n",
        "        grants = [subscribe];\n",
        "        skin = look;\n",
        "        allowed_users = nowhere;\n",
        "    }\n",
        "}\n",
        "new alice_pod: Pod(look = \"bench\");\n",
        "new bob_pod: Pod(look = \"bench\");\n",
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    for error in &errors {
        assert!(error.contains("nowhere"), "{errors:?}");
    }
}

// ── the open-bodied `integration` section ────────────────────────────────────
//
// The one section with no key vocabulary: any key is legal and every value is
// carried, because what the keys mean belongs to the integration's own reader
// in the binary.

#[test]
fn an_integration_section_carries_every_key_it_was_written_with() {
    let config = resolved(concat!(
        "const root = \"/home/alice/kb\";\n",
        "integration graf {\n",
        "    command = \"graf\";\n",
        "    timeout_secs = 30;\n",
        "    strict = true;\n",
        "    env = { GRAF_ROOT = root };\n",
        "}\n",
    ));
    let section = &config.sections[0];
    assert_eq!(section.kindword.value(), "integration");
    assert_eq!(
        section.name.as_ref().map(Spanned::value),
        Some(&"graf".to_string())
    );
    assert_eq!(attr(section, "command"), &RValue::Str("graf".into()));
    assert_eq!(attr(section, "timeout_secs"), &RValue::Int(30));
    assert_eq!(attr(section, "strict"), &RValue::Bool(true));
    // A reference inside the body resolves like a reference anywhere: an open
    // body is open about its keys, not about its values.
    let RValue::Table(entries) = attr(section, "env") else {
        panic!("`env` is a table");
    };
    assert_eq!(entries[0].0, "GRAF_ROOT");
    assert_eq!(entries[0].1.value(), &RValue::Str("/home/alice/kb".into()));
}

#[test]
fn an_unresolvable_value_in_an_integration_section_is_reported() {
    let errors = refusals("integration graf { command = nowhere; }\n");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("nowhere"), "{errors:?}");
}

/// A deeper tree is written as an inline-table value, so the section itself
/// nests nothing and a block written inside it has no vocabulary and no reader.
#[test]
fn an_integration_section_holds_no_sub_blocks() {
    assert_eq!(
        refusal("integration graf {\n    limits { timeout_secs = 30; }\n}\n"),
        "the `integration` block holds no sub-blocks, so `limits` has no meaning here"
    );
}

#[test]
fn two_integration_sections_with_one_name_are_refused() {
    assert_eq!(
        refusal(concat!(
            "integration graf { command = \"graf\"; }\n",
            "integration graf { command = \"graf2\"; }\n",
        )),
        "a document states `integration graf` once, and this is the second"
    );
}

#[test]
fn two_integration_sections_under_two_names_both_stand() {
    let config = resolved(concat!(
        "integration graf { command = \"graf\"; }\n",
        "integration pfin { command = \"pf\"; }\n",
    ));
    assert_eq!(config.sections.len(), 2);
    let names: Vec<&str> = config
        .sections
        .iter()
        .map(|section| {
            section
                .name
                .as_ref()
                .expect("an integration section is named")
                .value()
                .as_str()
        })
        .collect();
    assert_eq!(names, ["graf", "pfin"]);
    assert_eq!(
        attr(&config.sections[0], "command"),
        &RValue::Str("graf".into())
    );
    assert_eq!(
        attr(&config.sections[1], "command"),
        &RValue::Str("pf".into())
    );
}

// ── `attachment_target` blocks ───────────────────────────────────────────────

/// The agent's targets reach the model in declaration order, each with its
/// `handler` block held and the handler's `type` carried as the word written.
#[test]
fn an_agents_attachment_targets_reach_the_model_in_order() {
    let config = resolved(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        handler {\n",
        "            type = command;\n",
        "            program = \"pf\";\n",
        "            args = [\"import\", \"{ofx}\"];\n",
        "            file_roles = { ofx = [\".ofx\"] };\n",
        "        }\n",
        "    }\n",
        "    attachment_target receipt {\n",
        "        name = \"receipt-scan\";\n",
        "        label = \"Scan a receipt\";\n",
        "        accept = [\".jpg\"];\n",
        "        handler { type = command; program = \"pf\"; args = [\"scan\"]; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    let targets = &config.agents[0].attachment_targets;
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets[0].name.as_ref().map(Spanned::value),
        Some(&"import".to_string())
    );
    assert_eq!(attr(&targets[0], "label"), &RValue::Str("Import".into()));
    let handler = &targets[0].subs[0];
    assert_eq!(handler.kindword.value(), "handler");
    assert_eq!(attr(handler, "type"), &RValue::Str("command".into()));
    // The `name` attr, where the block states one, overrides the block's own
    // name; this layer carries both and lowering picks.
    assert_eq!(
        attr(&targets[1], "name"),
        &RValue::Str("receipt-scan".into())
    );
}

#[test]
fn an_attachment_target_without_a_handler_is_refused() {
    assert_eq!(
        refusal(concat!(
            "agent A() {\n",
            "    attachment_target import { label = \"Import\"; accept = [\".ofx\"]; }\n",
            "}\n",
            "new alice: A();\n",
        )),
        "an `attachment_target` states no `handler` block: what an upload does has no default"
    );
}

#[test]
fn two_attachment_targets_with_one_name_are_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        handler { type = command; program = \"pf\"; args = [\"import\"]; }\n",
        "    }\n",
        "    attachment_target import {\n",
        "        label = \"Import again\";\n",
        "        accept = [\".csv\"];\n",
        "        handler { type = command; program = \"pf\"; args = [\"import\"]; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    let expected =
        "an agent states `attachment_target import` once, and this is the second".to_string();
    assert!(errors.contains(&expected), "{errors:?}");
}

#[test]
fn a_stray_block_in_an_attachment_target_is_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        retries { max = 3; }\n",
        "        handler { type = command; program = \"pf\"; args = [\"import\"]; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    assert!(
        errors.iter().any(|error| error.contains("retries")),
        "{errors:?}"
    );
}

#[test]
fn a_stray_block_as_an_attachment_targets_only_block_is_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        retries { max = 3; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    assert!(!errors.is_empty(), "a diagnostic, not a panic");
    assert!(
        errors.iter().any(|error| error.contains("retries")),
        "{errors:?}"
    );
}

/// Two handlers is two answers to what an upload runs, and the loser would be
/// invisible.
#[test]
fn two_handler_blocks_in_one_attachment_target_are_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        handler { type = command; program = \"pf\"; args = [\"import\"]; }\n",
        "        handler { type = command; program = \"pf\"; args = [\"other\"]; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    let expected = "an attachment target states `handler` once, and this is the second".to_string();
    assert!(errors.contains(&expected), "{errors:?}");
}

#[test]
fn a_handler_block_holds_no_sub_blocks() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = \"Import\";\n",
        "        accept = [\".ofx\"];\n",
        "        handler {\n",
        "            type = command;\n",
        "            program = \"pf\";\n",
        "            args = [\"import\"];\n",
        "            retries { max = 3; }\n",
        "        }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    assert!(
        errors.iter().any(|error| error
            == "the `handler` block holds no sub-blocks, so `retries` has no meaning here"),
        "{errors:?}"
    );
}

// ── `integration_config` blocks ──────────────────────────────────────────────

/// An agent's per-integration override trees reach the model keyed by the
/// block's name, with every key the open body wrote carried and resolved.
#[test]
fn an_agents_integration_configs_reach_the_model_in_order() {
    let config = resolved(concat!(
        "const data = \"/home/alice/data\";\n",
        "agent A() {\n",
        "    integration_config ledger {\n",
        "        env = { LEDGER_DATA = f\"{data}/ledger\" };\n",
        "    }\n",
        "    integration_config calendar {\n",
        "        timeout_secs = 30;\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    let blocks = &config.agents[0].integration_configs;
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].kindword.value(), "integration_config");
    assert_eq!(
        blocks[0].name.as_ref().map(Spanned::value),
        Some(&"ledger".to_string())
    );
    let RValue::Table(env) = attr(&blocks[0], "env") else {
        panic!("`env` is a table");
    };
    assert_eq!(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.value().clone()))
            .collect::<Vec<_>>(),
        vec![("LEDGER_DATA", RValue::Str("/home/alice/data/ledger".into()))]
    );
    assert_eq!(
        blocks[1].name.as_ref().map(Spanned::value),
        Some(&"calendar".to_string())
    );
    assert_eq!(attr(&blocks[1], "timeout_secs"), &RValue::Int(30));
}

/// One name is one map key, so a second block under it is a two-site refusal —
/// the same rule the hook blocks and the attachment targets follow.
#[test]
fn two_integration_configs_with_one_name_are_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    integration_config ledger { env = { X = \"1\" }; }\n",
        "    integration_config ledger { env = { X = \"2\" }; }\n",
        "}\n",
        "new alice: A();\n",
    ));
    let expected =
        "an agent states `integration_config ledger` once, and this is the second".to_string();
    assert!(errors.contains(&expected), "{errors:?}");
}

/// An open body holds no sub-blocks: what a section inside one would mean has
/// no reader, so it is refused rather than carried.
#[test]
fn a_stray_block_in_an_integration_config_is_refused() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    integration_config ledger { retries { max = 3; } }\n",
        "}\n",
        "new alice: A();\n",
    ));
    assert!(
        errors.iter().any(|error| error.contains("retries")),
        "{errors:?}"
    );
}

/// An unresolvable value inside a target withholds the agent, like any other
/// value the body could not resolve.
#[test]
fn an_unresolvable_value_in_an_attachment_target_is_reported() {
    let errors = refusals(concat!(
        "agent A() {\n",
        "    attachment_target import {\n",
        "        label = nowhere;\n",
        "        accept = [\".ofx\"];\n",
        "        handler { type = command; program = \"pf\"; args = [\"import\"]; }\n",
        "    }\n",
        "}\n",
        "new alice: A();\n",
    ));
    assert!(
        errors.iter().any(|error| error.contains("nowhere")),
        "{errors:?}"
    );
}
