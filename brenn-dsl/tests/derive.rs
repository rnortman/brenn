//! Derivation: channel roles, the channel model, channel identity, and the wire
//! kind a surface-placed component is served under.
//!
//! Everything here goes through the whole pipeline — a document is written the
//! way an operator writes it, and what comes out the far side is asserted. The
//! authority model has its own suite.

mod support;

use brenn_dsl::derive::wire_kind;
use brenn_dsl::diag::Diagnostic;
use brenn_dsl::{dom_any, processor_any};
use support::{
    derive_errors, derive_refusal, derive_refusals, derived, durable, nondurable, prefix_tuning,
    tuning,
};

/// A fixture class's header: its abi, and grant declarations permitting
/// everything its host admits so the spec fit answers nothing this suite asks.
const DOM: &str = dom_any!();
const PROCESSOR: &str = processor_any!();

// ── roles: which blocks declare and which tune ───────────────────────────────

#[test]
fn a_handle_less_block_tunes_every_family_the_system_mints() {
    let config = derived(&format!(
        "{}{}{}{}",
        tuning("mqtt:broker:alice/status"),
        tuning("webhook:alice-inbox"),
        tuning("brenn:tools/pull"),
        tuning("brenn:tool-results/alice"),
    ));
    assert_eq!(config.resolved.tunings.len(), 4);
    assert!(config.resolved.channels.is_empty());
    assert!(config.channel_uuids.is_empty());
}

#[test]
fn a_handle_less_block_on_a_declarable_address_is_refused() {
    assert_eq!(
        derive_refusal(&tuning("brenn:alice.status")),
        "`brenn:alice.status` names no system-minted family, so a handle-less block tunes \
         nothing; tuning blocks name `mqtt:`, `webhook:`, `brenn:tools/` and \
         `brenn:tool-results/` channels, and a declarable channel is written \
         `channel <handle> at \"brenn:alice.status\"`"
    );
}

#[test]
fn every_declarable_scheme_needs_a_handle() {
    for address in [
        "brenn:alice.status",
        "ephemeral:alice.cache",
        "local:alice.ring",
    ] {
        let refusal = derive_refusal(&tuning(address));
        assert!(
            refusal.contains("names no system-minted family"),
            "{refusal}"
        );
    }
}

#[test]
fn a_declaration_on_a_system_minted_address_is_refused() {
    // Every family the system mints, and a handled block on each is the only
    // path into the identity pass's "this scheme declares nothing" arm — so the
    // refusal being the *sole* diagnostic is what says that arm held the
    // parallel vectors together instead of panicking.
    for (handle, address, block) in [
        (
            "pulls",
            "brenn:tools/pull",
            durable as fn(&str, &str) -> String,
        ),
        ("results", "brenn:tool-results/alice", durable),
        ("inbox", "webhook:alice-inbox", nondurable),
        ("status", "mqtt:broker:alice/status", nondurable),
    ] {
        assert_eq!(
            derive_refusal(&block(handle, address)),
            format!(
                "`{address}` is an address the system mints, so there is nothing to \
                 declare; write `channel at \"…\"` without a handle to tune its depths"
            )
        );
    }
}

#[test]
fn a_name_beside_a_tool_namespace_is_a_declarable_address() {
    // `toolsmith` merely starts with the same letters; the namespace is the
    // segment and the boundary that closes it.
    let config = derived(&durable("smith", "brenn:toolsmith.status"));
    assert_eq!(config.resolved.channels.len(), 1);
}

#[test]
fn a_tuning_prefix_must_stop_at_a_segment_boundary() {
    assert_eq!(
        derive_refusal(&prefix_tuning("webhook:git")),
        "the tuning prefix `webhook:git` does not end at a segment boundary (`/`, `.` or \
         `:`, the last of which closes an mqtt client) — a bare byte prefix reaches past \
         the family it names"
    );
}

#[test]
fn a_malformed_prefix_does_not_hide_the_description_rule() {
    // Independent errors are all reported: the prefix and the description are two
    // rules, and fixing one should not be what reveals the other.
    let refusals = derive_refusals(
        "channel at prefix \"webhook:git\" {\n    push_depth = 4;\n    retain_depth = 16;\n    \
         standing_retain_depth = 64;\n    description = \"the inbox family\";\n}\n",
    );
    assert_eq!(refusals.len(), 2, "{refusals:?}");
    assert!(
        refusals[0].contains("does not end at a segment boundary"),
        "{refusals:?}"
    );
    assert!(
        refusals[1].contains("a tuning block carries no description"),
        "{refusals:?}"
    );
}

#[test]
fn every_boundary_a_family_ends_at_is_accepted() {
    let config = derived(&format!(
        "{}{}{}",
        prefix_tuning("brenn:tools/"),
        prefix_tuning("mqtt:broker:"),
        prefix_tuning("webhook:alice."),
    ));
    assert_eq!(config.resolved.tunings.len(), 3);
}

#[test]
fn a_prefix_naming_no_system_minted_family_is_refused() {
    let refusal = derive_refusal(&prefix_tuning("brenn:alice."));
    assert!(
        refusal.contains("names no system-minted family"),
        "{refusal}"
    );
}

#[test]
fn two_blocks_tuning_one_address_cite_each_other() {
    let errors = derive_errors(&format!(
        "{}{}",
        tuning("webhook:alice-inbox"),
        tuning("webhook:alice-inbox")
    ));
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "two blocks tune the address `webhook:alice-inbox`"
    );
    assert_eq!(errors[0].related[0].0, "the other one is here");
}

#[test]
fn two_blocks_tuning_one_prefix_are_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "{}{}",
            prefix_tuning("brenn:tools/"),
            prefix_tuning("brenn:tools/")
        )),
        "two blocks tune the prefix `brenn:tools/`"
    );
}

#[test]
fn a_third_block_tuning_one_address_is_refused_on_its_own_account() {
    // One diagnostic per repeat, each citing the block that holds the key rather
    // than the repeat before it: an author who fixes the second block is told
    // about the third in the same pass, not on the next run.
    let errors = derive_errors(&format!(
        "{}{}{}",
        tuning("webhook:alice-inbox"),
        tuning("webhook:alice-inbox"),
        tuning("webhook:alice-inbox"),
    ));
    assert_eq!(errors.len(), 2, "{errors:?}");
    for error in &errors {
        assert_eq!(
            error.message,
            "two blocks tune the address `webhook:alice-inbox`"
        );
        assert_eq!(error.related[0].0, "the other one is here");
        // The first block, in both — not the repeat before this one.
        assert_eq!(
            Diagnostic::span_line_col(&error.related[0].1),
            Some((1, 13))
        );
    }
    // Each block is five lines, so the two repeats are reported where they were
    // written.
    assert_eq!(errors[0].line_col(), Some((6, 13)));
    assert_eq!(errors[1].line_col(), Some((11, 13)));
}

#[test]
fn an_exact_key_and_a_prefix_over_the_same_text_are_not_duplicates() {
    let config = derived(&format!(
        "{}{}",
        tuning("brenn:tools/"),
        prefix_tuning("brenn:tools/")
    ));
    assert_eq!(config.resolved.tunings.len(), 2);
}

#[test]
fn a_doc_comment_on_a_tuning_block_is_refused() {
    let refusal = derive_refusal(&format!(
        "/// Alice's inbox.\n{}",
        tuning("webhook:alice-inbox")
    ));
    assert_eq!(
        refusal,
        "a tuning block carries no description: the endpoint or tool that mints the channel \
         owns it — write the note as a `//` comment"
    );
}

#[test]
fn a_description_on_a_tuning_block_is_refused() {
    assert_eq!(
        derive_refusal(
            "channel at \"webhook:alice-inbox\" {\n    description = \"inbox\";\n    \
             push_depth = 4;\n    retain_depth = 16;\n    standing_retain_depth = 64;\n}\n"
        ),
        "a tuning block carries no description: the endpoint or tool that mints the channel \
         owns it"
    );
}

// ── the channel model ────────────────────────────────────────────────────────

#[test]
fn a_declaration_states_both_windows_it_sizes() {
    let refusals = derive_refusals("channel status at \"brenn:alice.status\";\n");
    assert_eq!(
        refusals,
        vec![
            "`brenn:alice.status` requires push_depth: how deep a channel's window is sized \
             is the decision this declaration exists to record, not a default",
            "`brenn:alice.status` requires retain_depth: how deep a channel's window is \
             sized is the decision this declaration exists to record, not a default",
            "`brenn:alice.status` requires standing_retain_depth: how deep a channel's \
             window is sized is the decision this declaration exists to record, not a default",
        ]
    );
}

#[test]
fn only_a_disk_backed_declaration_states_a_standing_window() {
    let refusals = derive_refusals("channel cache at \"ephemeral:alice.cache\";\n");
    assert_eq!(refusals.len(), 2);
    for refusal in &refusals {
        assert!(!refusal.contains("standing_retain_depth"), "{refusal}");
    }
}

#[test]
fn a_standing_window_on_a_non_durable_channel_is_refused() {
    assert_eq!(
        derive_refusal(
            "channel cache at \"ephemeral:alice.cache\" {\n    push_depth = 4;\n    \
             retain_depth = 16;\n    standing_retain_depth = 64;\n}\n"
        ),
        "`ephemeral:alice.cache` is not disk-backed, so it states no standing_retain_depth: \
         the standing buffer is the durable reaper's frontier, and this channel's retention \
         is retain_depth alone"
    );
}

#[test]
fn a_sink_on_a_non_durable_channel_is_refused() {
    assert_eq!(
        derive_refusal(
            "channel ring at \"local:alice.ring\" {\n    push_depth = 4;\n    \
             retain_depth = 16;\n    sink = drop;\n}\n"
        ),
        "`local:alice.ring` is not disk-backed, so it states no sink: it evicts from memory \
         and has nothing to archive"
    );
}

#[test]
fn a_tuning_block_states_every_depth() {
    let refusals = derive_refusals("channel at \"webhook:alice-inbox\";\n");
    assert_eq!(refusals.len(), 3);
    assert_eq!(
        refusals[0],
        "the block tuning `webhook:alice-inbox` requires push_depth: a system-minted channel \
         has a bounded in-code default, and a block that tunes it states every depth"
    );
    assert!(
        refusals[2].contains("requires standing_retain_depth"),
        "{refusals:?}"
    );
}

#[test]
fn a_prefix_block_missing_a_depth_is_named_as_a_prefix() {
    let refusals = derive_refusals("channel at prefix \"brenn:tools/\";\n");
    assert_eq!(refusals.len(), 3);
    assert!(
        refusals[0].contains("the block tuning `prefix brenn:tools/` requires push_depth"),
        "{refusals:?}"
    );
}

// ── identity ─────────────────────────────────────────────────────────────────

/// The namespace seed this pass derives under, as a literal.
///
/// Pinned: a changed seed renames every persisted channel row a configuration
/// lowered from this pass created, so the value is asserted rather than merely
/// derived the same way twice.
const DSL_CHANNEL_NAMESPACE: &str = "60c1ff0d-315c-5775-a6a1-fee7ac9eb186";

/// The identity `brenn:alice.status` derives to, as a literal.
const ALICE_STATUS_UUID: &str = "0348ff60-5e62-5a28-8ad7-01a8d35725f6";

#[test]
fn the_derivation_is_pinned_to_its_namespace_and_its_address() {
    let namespace = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, b"brenn.dsl-channel");
    assert_eq!(namespace.to_string(), DSL_CHANNEL_NAMESPACE);
    let config = derived(&durable("status", "brenn:alice.status"));
    assert_eq!(
        config.channel_uuids,
        vec![Some(ALICE_STATUS_UUID.parse().expect("a uuid"))]
    );
}

#[test]
fn a_pin_wins_over_the_derivation() {
    let config = derived(&format!(
        "uuid_pins {{\n    \"brenn:alice.status\" = \
         \"11111111-2222-5333-8444-555555555555\";\n}}\n{}",
        durable("status", "brenn:alice.status")
    ));
    assert_eq!(
        config.channel_uuids,
        vec![Some(
            "11111111-2222-5333-8444-555555555555"
                .parse()
                .expect("a uuid")
        )]
    );
}

#[test]
fn a_non_durable_channel_carries_no_configured_identity() {
    let config = derived(&format!(
        "{}{}{}",
        durable("status", "brenn:alice.status"),
        nondurable("cache", "ephemeral:alice.cache"),
        nondurable("ring", "local:alice.ring"),
    ));
    assert_eq!(config.resolved.channels.len(), 3);
    assert_eq!(config.channel_uuids.len(), 3);
    assert!(config.channel_uuids[0].is_some());
    assert_eq!(config.channel_uuids[1], None);
    assert_eq!(config.channel_uuids[2], None);
}

#[test]
fn a_pin_that_is_not_a_uuid_is_refused() {
    assert_eq!(
        derive_refusal(&format!(
            "uuid_pins {{\n    \"brenn:alice.status\" = \"not-a-uuid\";\n}}\n{}",
            durable("status", "brenn:alice.status")
        )),
        "`not-a-uuid` is not a uuid: invalid character: found `n` at 0"
    );
}

#[test]
fn a_pin_refusal_says_what_is_wrong_with_the_uuid() {
    // The cause is the difference between a truncated identity and a pasted
    // wrong field, and each reads as its own message.
    assert_eq!(
        derive_refusal(&format!(
            "uuid_pins {{\n    \"brenn:alice.status\" = \"11111111-2222-5333-8444\";\n}}\n{}",
            durable("status", "brenn:alice.status")
        )),
        "`11111111-2222-5333-8444` is not a uuid: invalid group count: expected 5, found 4"
    );
}

#[test]
fn two_pins_for_one_address_cite_each_other() {
    let errors = derive_errors(&format!(
        "uuid_pins {{\n    \"brenn:alice.status\" = \
         \"11111111-2222-5333-8444-555555555555\";\n    \"brenn:alice.status\" = \
         \"11111111-2222-5333-8444-666666666666\";\n}}\n{}",
        durable("status", "brenn:alice.status")
    ));
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        "two pins name the address `brenn:alice.status`"
    );
    assert_eq!(errors[0].related[0].0, "the other one is here");
}

#[test]
fn a_pin_no_disk_backed_declaration_answers_to_is_refused() {
    for address in [
        "brenn:bob.status",
        "ephemeral:alice.cache",
        "local:alice.ring",
        "webhook:alice-inbox",
    ] {
        let refusal = derive_refusal(&format!(
            "uuid_pins {{\n    \"{address}\" = \
             \"11111111-2222-5333-8444-555555555555\";\n}}\n{}{}{}",
            durable("status", "brenn:alice.status"),
            nondurable("cache", "ephemeral:alice.cache"),
            nondurable("ring", "local:alice.ring"),
        ));
        assert_eq!(
            refusal,
            format!(
                "no disk-backed channel declares the address `{address}`, so this pin names \
                 nothing; only a `brenn:` declaration carries a configured uuid"
            )
        );
    }
}

#[test]
fn a_pin_colliding_with_a_derived_durable_identity_is_refused() {
    let errors = derive_errors(&format!(
        "uuid_pins {{\n    \"brenn:bob.status\" = \"{ALICE_STATUS_UUID}\";\n}}\n{}{}",
        durable("status", "brenn:alice.status"),
        durable("bob_status", "brenn:bob.status"),
    ));
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].message,
        format!(
            "`brenn:bob.status` and `brenn:alice.status` have the same channel uuid \
             {ALICE_STATUS_UUID}"
        )
    );
    assert_eq!(errors[0].related[0].0, "`brenn:alice.status` has it here");
}

#[test]
fn a_pin_colliding_with_a_runtime_derived_non_durable_identity_is_refused() {
    // The runtime derives `ephemeral:alice.cache`'s identity from its own seed
    // and inserts it in the same set every configured uuid joins, so a pin
    // landing on that value is a collision even though the ephemeral channel
    // carries nothing.
    let cache_uuid = "29265d4c-7a85-57e0-86cb-93cda9d1c480";
    let refusal = derive_refusal(&format!(
        "uuid_pins {{\n    \"brenn:alice.status\" = \"{cache_uuid}\";\n}}\n{}{}",
        nondurable("cache", "ephemeral:alice.cache"),
        durable("status", "brenn:alice.status"),
    ));
    assert_eq!(
        refusal,
        format!(
            "`brenn:alice.status` and `ephemeral:alice.cache` have the same channel uuid \
             {cache_uuid}"
        )
    );
}

#[test]
fn a_pin_colliding_with_a_runtime_derived_local_identity_is_refused() {
    // The `local:` seed is its own address space, and it is pinned here for the
    // same reason as the ephemeral one: the collision check is the only place a
    // drifted seed shows up before boot.
    let ring_uuid = "2039cd98-2f5d-5ad6-b5c9-3552d5fa5c1e";
    let refusal = derive_refusal(&format!(
        "uuid_pins {{\n    \"brenn:alice.status\" = \"{ring_uuid}\";\n}}\n{}{}",
        nondurable("ring", "local:alice.ring"),
        durable("status", "brenn:alice.status"),
    ));
    assert_eq!(
        refusal,
        format!(
            "`brenn:alice.status` and `local:alice.ring` have the same channel uuid \
             {ring_uuid}"
        )
    );
}

// ── the wire-kind fold ───────────────────────────────────────────────────────

/// The class names whose fold is the definition of the rule.
const FOLD_GOLDENS: [(&str, &str); 4] = [
    ("Alice", "alice"),
    ("ModeClock", "mode-clock"),
    ("P1Panel", "p1-panel"),
    ("Mode2Clock", "mode2-clock"),
];

#[test]
fn the_fold_is_its_goldens() {
    for (class, kind) in FOLD_GOLDENS {
        assert_eq!(wire_kind(class), kind, "{class}");
    }
}

/// Every class name the grammar's `cname` charset admits, over a small alphabet:
/// an uppercase head, then any number of `[a-z0-9]+[A-Z]` segments, then an
/// optional lowercase-or-digit tail. That shape — never two capitals in a row —
/// is what the fold's guarantee rests on, so the property is checked over the
/// charset rather than over the four names whose output is already pinned.
fn charset_class_names() -> Vec<String> {
    let heads = ["A", "P", "Z"];
    let middles = ["b", "1", "c2", "9x"];
    let tails = ["", "d", "3", "y7"];
    let mut names = Vec::new();
    for head in heads {
        for tail in tails {
            for segments in 0..=2 {
                for middle in middles {
                    let mut name = head.to_string();
                    for _ in 0..segments {
                        name.push_str(middle);
                        name.push('Q');
                    }
                    name.push_str(tail);
                    names.push(name);
                }
            }
        }
    }
    names
}

/// The charset the grammar accepts a class name over: `[A-Z]([a-z0-9]+[A-Z])*[a-z0-9]*`.
fn is_class_name(name: &str) -> bool {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    if !first.is_ascii_uppercase() {
        return false;
    }
    let mut previous_was_upper = true;
    for character in characters {
        if character.is_ascii_uppercase() {
            if previous_was_upper {
                return false;
            }
            previous_was_upper = true;
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            previous_was_upper = false;
        } else {
            return false;
        }
    }
    true
}

/// The runtime's `is_valid_kind`: `^[a-z0-9][a-z0-9-]*$`, and no `--`.
fn is_valid_kind(kind: &str) -> bool {
    let mut characters = kind.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    first.is_ascii_lowercase()
        && !kind.contains("--")
        && characters.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

#[test]
fn every_fold_over_the_class_charset_satisfies_the_runtime_kind_rule() {
    let names = charset_class_names();
    assert!(names.len() > 50, "{}", names.len());
    for name in names {
        assert!(is_class_name(&name), "{name} is not a class name");
        let kind = wire_kind(&name);
        assert!(is_valid_kind(&kind), "{name} folds to `{kind}`");
    }
}

#[test]
fn the_goldens_are_class_names_the_runtime_accepts_the_fold_of() {
    for (class, kind) in FOLD_GOLDENS {
        assert!(is_class_name(class), "{class}");
        assert!(is_valid_kind(kind), "{kind}");
    }
}

#[test]
fn a_surface_instance_carries_the_kind_its_class_folds_to() {
    let config = derived(concat!(
        "component ModeClock { ",
        dom_any!(),
        " optional in messages; }\n\
             component P1Panel { ",
        dom_any!(),
        " optional in messages; }\n\
             surface desk {\n    grants = [];\n    new clock: ModeClock { grants = []; }\n    \
             new panel: P1Panel { grants = []; }\n}\n",
    ));
    assert_eq!(
        config.surface_component_kinds,
        vec![vec!["mode-clock".to_string(), "p1-panel".to_string()]]
    );
}

#[test]
fn a_top_level_instance_takes_no_kind() {
    let config = derived(concat!(
        "component ModeClock { ",
        processor_any!(),
        " }\n\
             new clock: ModeClock { component_path = \"clock.wasm\"; grants = []; }\n",
    ));
    assert_eq!(config.resolved.consumers.len(), 1);
    assert!(config.surface_component_kinds.is_empty());
}

/// Two modules, each with a `Panel` class and a surface placing it. The class
/// bodies are what the two spell differently.
fn two_panels(first_body: &str, second_body: &str) -> Vec<(&'static str, String)> {
    vec![
        (
            "",
            "use one::marker_one;\nuse two::marker_two;\n".to_string(),
        ),
        (
            "one",
            format!(
                "const marker_one = 1;\ncomponent Panel {{ {first_body} }}\n\
                 surface first {{\n    grants = [];\n    new view: Panel {{ grants = []; }}\n}}\n"
            ),
        ),
        (
            "two",
            format!(
                "const marker_two = 2;\ncomponent Panel {{ {second_body} }}\n\
                 surface second {{\n    grants = [];\n    new view: Panel {{ grants = []; }}\n}}\n"
            ),
        ),
    ]
}

/// The class body every same-facts fixture uses on both sides. Its port is
/// `optional` because the instances the helper writes bind nothing.
const DOM_PANEL: &str = concat!(dom_any!(), " optional in messages;");

/// The tree helper takes borrowed sources; the fixtures build owned ones.
fn borrow<'a>(modules: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    modules
        .iter()
        .map(|(key, source)| (*key, source.as_str()))
        .collect()
}

#[test]
fn two_classes_folding_to_one_kind_with_different_facts_are_refused() {
    // Every fact the wire contract is made of: one differing fact is enough, and
    // each is asserted on its own so dropping one conjunct fails a test rather
    // than silently serving two contracts under one kind.
    for (first, second) in [
        // the abi the browser instantiates against
        (
            format!("{PROCESSOR} optional in messages;"),
            format!("{DOM} optional in messages;"),
        ),
        // a port's name and direction
        (
            format!("{DOM} optional in messages;"),
            format!("{DOM} optional out results;"),
        ),
        // how many ports there are
        (
            format!("{DOM} optional in messages;"),
            format!("{DOM} optional in messages; optional in extra;"),
        ),
        // which capabilities the spec permits
        (
            "abi = dom; requires = []; optional = [ports]; optional in messages;".to_string(),
            "abi = dom; requires = []; optional = [log]; optional in messages;".to_string(),
        ),
        // what a port carries
        (
            format!("{DOM} optional in messages: \"alice.panel@1\";"),
            format!("{DOM} optional in messages;"),
        ),
        // the order the spec's words are written in, which the comparison keeps
        (
            "abi = dom; requires = []; optional = [ports, log]; optional in messages;".to_string(),
            "abi = dom; requires = []; optional = [log, ports]; optional in messages;".to_string(),
        ),
    ] {
        let modules = two_panels(&first, &second);
        let errors = support::derive_tree(&borrow(&modules)).expect_err("one kind, two contracts");
        assert_eq!(errors.len(), 1, "{first} / {second}: {errors:?}");
        assert_eq!(
            errors[0].message,
            "two component classes are served as `panel`: `Panel` on surface `second` states \
             different facts than the one already claiming it",
            "{first} / {second}"
        );
        assert_eq!(errors[0].related[0].0, "`first` claims it here");
    }
}

/// Optionality is a wire fact like the rest: one module's `Panel` permitting an
/// unwired port and another's requiring it are two contracts under one kind.
/// Written out rather than added to the loop above because both instances have
/// to bind the port — the required side would otherwise be refused in
/// resolution, before the fold is reached.
#[test]
fn two_classes_disagreeing_on_optionality_are_refused() {
    let panel = |module: &'static str, marker: &str, surface: &str, port: &str| {
        (
            module,
            format!(
                "const {marker} = 1;\ncomponent Panel {{ {DOM} {port} }}\n\
                 surface {surface} {{\n    grants = [];\n    \
                 new view: Panel {{ grants = []; in messages <- \"local:brenn/m\"; }}\n}}\n"
            ),
        )
    };
    let modules = vec![
        (
            "",
            "use one::marker_one;\nuse two::marker_two;\n".to_string(),
        ),
        panel("one", "marker_one", "first", "optional in messages;"),
        panel("two", "marker_two", "second", "in messages;"),
    ];
    let errors = support::derive_tree(&borrow(&modules)).expect_err("one kind, two contracts");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "two component classes are served as `panel`: `Panel` on surface `second` states \
         different facts than the one already claiming it"
    );
}

/// A class's declared needs are wire facts too: one module's `Panel` that cannot
/// run without `log` and another's that merely permits it are two contracts
/// under one kind, even where both deployments happen to grant it. Written out
/// rather than added to the loop above because a requirement no instance grants
/// is refused at the instance, before the fold is reached.
#[test]
fn two_classes_disagreeing_on_declared_needs_are_refused() {
    let panel = |module: &'static str, marker: &str, surface: &str, needs: &str| {
        (
            module,
            format!(
                "const {marker} = 1;\ncomponent Panel {{ abi = dom; {needs} \
                 optional in messages; }}\n\
                 surface {surface} {{\n    grants = [];\n    \
                 new view: Panel {{ grants = [log]; }}\n}}\n"
            ),
        )
    };
    let modules = vec![
        (
            "",
            "use one::marker_one;\nuse two::marker_two;\n".to_string(),
        ),
        panel("one", "marker_one", "first", "requires = [log];"),
        panel(
            "two",
            "marker_two",
            "second",
            "requires = []; optional = [log];",
        ),
    ];
    let errors = support::derive_tree(&borrow(&modules)).expect_err("one kind, two contracts");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "two component classes are served as `panel`: `Panel` on surface `second` states \
         different facts than the one already claiming it"
    );
}

#[test]
fn two_byte_identical_classes_folding_to_one_kind_stand() {
    let modules = two_panels(DOM_PANEL, DOM_PANEL);
    let config = support::derived_tree(&borrow(&modules));
    assert_eq!(
        config.surface_component_kinds,
        vec![vec!["panel".to_string()], vec!["panel".to_string()]]
    );
}

// ── document types ───────────────────────────────────────────────────────────

/// A local channel and one surface, carrying the class declarations and
/// instances a doctype case needs.
///
/// A `local:` plane throughout: the pass keys on the resolved address and cares
/// nothing for the scheme, and a plane needs no transport grant, so a case about
/// document types states nothing about authority.
fn planes(classes: &str, instances: &str) -> String {
    format!(
        "channel m at \"local:alice.m\" {{\n    push_depth = 4;\n    retain_depth = 16;\n}}\n\
         {classes}surface page {{\n    grants = [];\n{instances}}}\n"
    )
}

/// One class with one `in messages` port, tagged or not.
fn tagged_class(name: &str, doctype: &str) -> String {
    format!("component {name} {{ {DOM} in messages{doctype}; }}\n")
}

/// One instance of `class`, binding `messages` to the declared channel.
fn tagged_inst(handle: &str, class: &str) -> String {
    format!("    new {handle}: {class} {{ grants = []; in messages <- m; }}\n")
}

#[test]
fn two_ports_agreeing_on_a_document_type_stand() {
    let source = planes(
        &format!(
            "{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", ": \"alice.panel@1\"")
        ),
        &format!(
            "{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board")
        ),
    );
    let config = support::derived(&source);
    assert_eq!(config.resolved.channels.len(), 1);
}

#[test]
fn an_untagged_port_binds_to_a_tagged_channel() {
    // The whole point of the tag being optional: a class that says nothing about
    // its payload is not thereby wrong, at either end of the same plane.
    let source = planes(
        &format!(
            "{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", "")
        ),
        &format!(
            "{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board")
        ),
    );
    support::derived(&source);
}

#[test]
fn two_ports_disagreeing_on_a_document_type_are_refused() {
    let source = planes(
        &format!(
            "{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", ": \"alice.board@1\"")
        ),
        &format!(
            "{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board")
        ),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "the ports bound to `m` (`local:alice.m`) declare 2 different document types, and \
         one channel carries one document: a tag is compared whole, so `x@2` is not `x@1`"
    );
    assert_eq!(
        errors[0]
            .related
            .iter()
            .map(|(note, _)| note.as_str())
            .collect::<Vec<_>>(),
        vec![
            "port `messages` of `Panel` declares `alice.panel@1` here",
            "port `messages` of `Board` declares `alice.board@1` here",
        ]
    );
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        support::at(&source, "\"alice.panel@1\"").map(|(line, _)| line)
    );
    assert_eq!(
        errors[0].related[1].1.line_col_inner().map(|p| p.line + 1),
        support::at(&source, "\"alice.board@1\"").map(|(line, _)| line)
    );
}

#[test]
fn a_version_bump_is_a_different_document() {
    let source = planes(
        &format!(
            "{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", ": \"alice.panel@2\"")
        ),
        &format!(
            "{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board")
        ),
    );
    assert!(
        derive_refusal(&source).contains("declare 2 different document types"),
        "no version arithmetic: `@2` is a different tag"
    );
}

#[test]
fn three_disagreeing_ports_are_one_diagnostic_with_three_sites() {
    let source = planes(
        &format!(
            "{}{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", ": \"alice.board@1\""),
            tagged_class("Sign", ": \"alice.sign@1\"")
        ),
        &format!(
            "{}{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board"),
            tagged_inst("sign", "Sign")
        ),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("declare 3 different document types"),
        "{}",
        errors[0].message
    );
    assert_eq!(errors[0].related.len(), 3);
}

#[test]
fn many_instances_of_one_class_are_one_claim() {
    // Classes are copied onto every instance, so N instances on one channel are N
    // identical records. Deduping by tag is what keeps that from reading as a
    // conflict, and from repeating a related entry per instance.
    let source = planes(
        &format!(
            "{}{}",
            tagged_class("Panel", ": \"alice.panel@1\""),
            tagged_class("Board", ": \"alice.board@1\"")
        ),
        &format!(
            "{}{}{}{}",
            tagged_inst("one", "Panel"),
            tagged_inst("two", "Panel"),
            tagged_inst("three", "Board"),
            tagged_inst("four", "Board")
        ),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].related.len(), 2);
}

#[test]
fn two_ports_of_one_class_on_one_channel_must_agree() {
    let source = planes(
        concat!(
            "component Panel { ",
            dom_any!(),
            " in messages: \"alice.panel@1\"; out results: \"alice.board@1\"; }\n",
        ),
        "    new panel: Panel { grants = [ports]; in messages <- m; out results -> m; }\n",
    );
    assert!(
        derive_refusal(&source).contains("declare 2 different document types"),
        "one class can contradict itself on one channel"
    );
}

#[test]
fn a_literal_address_participates() {
    // No `channel` block anywhere: the resolver hands the pass a resolved
    // address either way, and a plane two doctyped ports share deserves the same
    // agreement check as a declared channel.
    let source = format!(
        "{}{}surface page {{\n    grants = [];\n\
             new panel: Panel {{ grants = []; in messages <- \"local:alice.m\"; }}\n\
             new board: Board {{ grants = []; in messages <- \"local:alice.m\"; }}\n}}\n",
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_class("Board", ": \"alice.board@1\"")
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "the ports bound to `local:alice.m` declare 2 different document types, and one \
         channel carries one document: a tag is compared whole, so `x@2` is not `x@1`"
    );
}

#[test]
fn a_free_io_port_claims_nothing() {
    // A free `io` port is tuned in place and connects nothing, so its tag reaches
    // no channel and cannot disagree with one.
    let source = planes(
        concat!(
            "component Panel { ",
            dom_any!(),
            " io tick: \"alice.tick@1\"; in messages: \"alice.panel@1\"; }\n\
             component Board { ",
            dom_any!(),
            " io tick: \"alice.board@1\"; }\n",
        ),
        "    new panel: Panel { grants = [ports]; io tick { push_depth = 1; retain_depth = 2; } \
         in messages <- m; }\n\
             new board: Board { grants = [ports]; io tick { push_depth = 1; \
         retain_depth = 2; } }\n",
    );
    support::derived(&source);
}

#[test]
fn a_consumers_ports_participate() {
    // A transportable plane, because that is what a backend component and a
    // page component can actually share: one `ephemeral:` address is one
    // channel on either side of the wire.
    let source = format!(
        "channel m at \"ephemeral:alice.m\" {{\n    push_depth = 4;\n    retain_depth = 16;\n}}\n\
         component Sink {{ {PROCESSOR} in messages: \"alice.sink@1\"; }}\n\
         {}surface page {{\n    grants = [subscribe];\n{}}}\n\
         new sink: Sink {{\n    slug = \"sink\";\n    component_path = \"/tmp/sink.wasm\";\n    \
         grants = [];\n    in messages <- m;\n}}\n",
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_inst("panel", "Panel"),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0]
            .related
            .iter()
            .map(|(note, _)| note.as_str())
            .collect::<Vec<_>>(),
        vec![
            "port `messages` of `Panel` declares `alice.panel@1` here",
            "port `messages` of `Sink` declares `alice.sink@1` here",
        ]
    );
}

#[test]
fn two_surfaces_reusing_one_local_name_are_two_channels() {
    // Each surface's page ring has its own `local:` namespace, so one bare name
    // in two of them is two channels spelled alike. Two documents, no conflict.
    let page = |surface: &str, class: &str| {
        format!(
            "surface {surface} {{\n    grants = [];\n    \
             new view: {class} {{ grants = []; in messages <- \"local:alice.m\"; }}\n}}\n"
        )
    };
    let source = format!(
        "{}{}{}{}",
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_class("Board", ": \"alice.board@1\""),
        page("first", "Panel"),
        page("second", "Board"),
    );
    support::derived(&source);
}

#[test]
fn a_page_local_name_and_a_server_local_name_are_two_channels() {
    // The server ring and a page ring cannot exchange a message, so a backend
    // component and a page component naming one `local:` address are describing
    // two private channels, not disagreeing about one.
    let source = format!(
        "component Sink {{ {PROCESSOR} in messages: \"alice.sink@1\"; }}\n\
         {}surface page {{\n    grants = [];\n    \
         new panel: Panel {{ grants = []; in messages <- \"local:alice.m\"; }}\n}}\n\
         new sink: Sink {{\n    slug = \"sink\";\n    component_path = \"/tmp/sink.wasm\";\n    \
         grants = [];\n    in messages <- \"local:alice.m\";\n}}\n",
        tagged_class("Panel", ": \"alice.panel@1\""),
    );
    support::derived(&source);
}

#[test]
fn an_interpolated_tag_is_compared_as_the_string_it_resolved_to() {
    let source = planes(
        &format!(
            "const tag = \"alice.panel@1\";\n{}{}",
            tagged_class("Panel", ": f\"{tag}\""),
            tagged_class("Board", ": \"alice.panel@1\"")
        ),
        &format!(
            "{}{}",
            tagged_inst("panel", "Panel"),
            tagged_inst("board", "Board")
        ),
    );
    support::derived(&source);
}

// ── the channel's own expectation ────────────────────────────────────────────

/// A local channel declaring the document it expects.
fn expecting(tag: &str) -> String {
    format!(
        "channel m at \"local:alice.m\" {{\n    push_depth = 4;\n    retain_depth = 16;\n    \
         doctype = \"{tag}\";\n}}\n"
    )
}

#[test]
fn a_channel_doctype_a_port_matches_stands() {
    let source = format!(
        "{}{}surface page {{\n    grants = [];\n{}}}\n",
        expecting("alice.panel@1"),
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_inst("panel", "Panel"),
    );
    support::derived(&source);
}

#[test]
fn a_channel_doctype_a_port_contradicts_is_refused() {
    let source = format!(
        "{}{}surface page {{\n    grants = [];\n{}}}\n",
        expecting("alice.board@1"),
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_inst("panel", "Panel"),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "port `messages` of `Panel` declares `alice.panel@1`, and channel `m` expects \
         `alice.board@1`"
    );
    assert_eq!(
        errors[0].related[0].0,
        "the channel states its document type here"
    );
    assert_eq!(
        errors[0].related[0].1.line_col_inner().map(|p| p.line + 1),
        support::at(&source, "\"alice.board@1\"").map(|(line, _)| line)
    );
}

#[test]
fn a_channel_doctype_over_disagreeing_ports_says_both_things() {
    // Two families of diagnostic on one channel: the ports disagree with each
    // other, and the one that does not match the channel disagrees with the
    // channel too. Both are true and both are reported.
    let source = format!(
        "{}{}{}surface page {{\n    grants = [];\n{}{}}}\n",
        expecting("alice.panel@1"),
        tagged_class("Panel", ": \"alice.panel@1\""),
        tagged_class("Board", ": \"alice.board@1\""),
        tagged_inst("panel", "Panel"),
        tagged_inst("board", "Board"),
    );
    let errors = derive_errors(&source);
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert_eq!(
        errors[0].message,
        "the ports bound to `m` (`local:alice.m`) declare 2 different document types, and \
         one channel carries one document: a tag is compared whole, so `x@2` is not `x@1`"
    );
    assert_eq!(
        errors[0]
            .related
            .iter()
            .map(|(note, _)| note.as_str())
            .collect::<Vec<_>>(),
        vec![
            "port `messages` of `Panel` declares `alice.panel@1` here",
            "port `messages` of `Board` declares `alice.board@1` here",
        ]
    );
    assert_eq!(
        errors[1].message,
        "port `messages` of `Board` declares `alice.board@1`, and channel `m` expects \
         `alice.panel@1`"
    );
    assert_eq!(
        errors[1].related[0].0,
        "the channel states its document type here"
    );
}

#[test]
fn a_channel_doctype_with_no_doctyped_port_is_inert() {
    // Deliberately not dead config: the attr exists to catch a *future* binding,
    // so an expectation awaiting components is the state it is written in.
    let source = format!(
        "{}{}surface page {{\n    grants = [];\n{}}}\n",
        expecting("alice.panel@1"),
        tagged_class("Panel", ""),
        tagged_inst("panel", "Panel"),
    );
    support::derived(&source);
}

#[test]
fn a_channel_doctype_with_nothing_bound_at_all_is_inert() {
    support::derived(&expecting("alice.panel@1"));
}

#[test]
fn a_channel_doctype_that_is_not_a_tag_is_refused() {
    let source = "channel m at \"local:alice.m\" {\n    push_depth = 4;\n    \
                  retain_depth = 16;\n    doctype = 3;\n}\n";
    assert_eq!(
        derive_refusal(source),
        "a document type is a string; this is an integer"
    );
}

#[test]
fn a_tuning_states_no_doctype() {
    let source = "channel at prefix \"mqtt:broker:alice/\" {\n    push_depth = 4;\n    \
                  retain_depth = 16;\n    standing_retain_depth = 64;\n    \
                  doctype = \"alice.panel@1\";\n}\n";
    assert_eq!(
        derive_refusal(source),
        "the block tuning `prefix mqtt:broker:alice/` states no doctype: a tuning matches a \
         family the system mints, so it names no one document contract — a doctype belongs \
         on a `channel` declaration, which is one channel"
    );
}

// ── across module and assembly boundaries ────────────────────────────────────

#[test]
fn two_modules_wiring_one_plane_are_checked_against_each_other() {
    // Neither author can see the other's class; the pass is whole-document, so
    // the plane they share is where the disagreement surfaces. Both components
    // are backend-placed, which is what makes the `local:` name they both wrote
    // one channel.
    let module = |key: &'static str, marker: &str, class: &str, tag: &str, inst: &str| {
        (
            key,
            format!(
                "const {marker} = 1;\ncomponent {class} {{ {PROCESSOR} in messages: \"{tag}\"; \
                 }}\nnew {inst}: {class} {{\n    slug = \"{inst}\";\n    \
                 component_path = \"/tmp/{inst}.wasm\";\n    grants = [];\n    \
                 in messages <- \"local:alice.m\";\n}}\n"
            ),
        )
    };
    let modules = vec![
        (
            "",
            "use one::marker_one;\nuse two::marker_two;\n".to_string(),
        ),
        module("one", "marker_one", "Panel", "alice.panel@1", "panel"),
        module("two", "marker_two", "Board", "alice.board@1", "board"),
    ];
    let errors = support::derive_tree(&borrow(&modules)).expect_err("one plane, two documents");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .contains("the ports bound to `local:alice.m` declare 2 different document types"),
        "{}",
        errors[0].message
    );
    assert_eq!(errors[0].related[0].1.filename_inner(), Some("one.brenn"));
    assert_eq!(errors[0].related[1].1.filename_inner(), Some("two.brenn"));
}

#[test]
fn a_stamped_channel_is_checked_per_stamping() {
    // Two stampings of one assembly are two channels carrying identical claims,
    // so the disagreement is reported once per stamped channel and both cite the
    // one declaration in the assembly body.
    let source = concat!(
        "component Panel { ",
        dom_any!(),
        " in messages: \"alice.panel@1\"; }\n\
component Board { ",
        dom_any!(),
        " in messages: \"alice.board@1\"; }

assembly Pod(slug: String) {
    channel messages at f\"local:{slug}.messages\" { push_depth = 4; retain_depth = 16; }
    surface page {
        slug = slug;
        grants = [];
        new panel: Panel { grants = []; in messages <- messages; }
        new board: Board { grants = []; in messages <- messages; }
    }
}

new alice: Pod(slug = \"alice\");
new bob: Pod(slug = \"bob\");
",
    );
    let errors = derive_errors(source);
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert_eq!(
        errors
            .iter()
            .map(|error| error.message.as_str())
            .collect::<Vec<_>>(),
        vec![
            "the ports bound to `alice.messages` (`local:alice.messages`) declare 2 \
             different document types, and one channel carries one document: a tag is \
             compared whole, so `x@2` is not `x@1`",
            "the ports bound to `bob.messages` (`local:bob.messages`) declare 2 different \
             document types, and one channel carries one document: a tag is compared \
             whole, so `x@2` is not `x@1`",
        ]
    );
    for error in &errors {
        assert_eq!(
            error.related[0].1.line_col_inner().map(|p| p.line + 1),
            support::at(source, "\"alice.panel@1\"").map(|(line, _)| line)
        );
    }
}
