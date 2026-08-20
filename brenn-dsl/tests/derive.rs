//! Derivation: channel roles, the channel model, channel identity, and the wire
//! kind a surface-placed component is served under.
//!
//! Everything here goes through the whole pipeline — a document is written the
//! way an operator writes it, and what comes out the far side is asserted. The
//! authority model has its own suite.

mod support;

use brenn_dsl::derive::wire_kind;
use brenn_dsl::diag::Diagnostic;
use support::{
    derive_errors, derive_refusal, derive_refusals, derived, durable, nondurable, prefix_tuning,
    tuning,
};

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
    let config = derived(
        "component ModeClock { abi = dom; in messages; }\n\
         component P1Panel { abi = dom; in messages; }\n\
         surface desk {\n    grants = [];\n    new clock: ModeClock;\n    \
         new panel: P1Panel;\n}\n",
    );
    assert_eq!(
        config.surface_component_kinds,
        vec![vec!["mode-clock".to_string(), "p1-panel".to_string()]]
    );
}

#[test]
fn a_top_level_instance_takes_no_kind() {
    let config = derived(
        "component ModeClock { abi = processor; component_path = \"clock.wasm\"; }\n\
         new clock: ModeClock { grants = []; }\n",
    );
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
                 surface first {{\n    grants = [];\n    new view: Panel;\n}}\n"
            ),
        ),
        (
            "two",
            format!(
                "const marker_two = 2;\ncomponent Panel {{ {second_body} }}\n\
                 surface second {{\n    grants = [];\n    new view: Panel;\n}}\n"
            ),
        ),
    ]
}

/// The class body every same-facts fixture uses on both sides.
const DOM_PANEL: &str = "abi = dom; in messages;";

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
        ("abi = processor; in messages;", "abi = dom; in messages;"),
        // the artifact a processor class is loaded from
        (
            "abi = processor; component_path = \"one.wasm\"; in messages;",
            "abi = processor; component_path = \"two.wasm\"; in messages;",
        ),
        // a port's name and direction
        ("abi = dom; in messages;", "abi = dom; out results;"),
        // how many ports there are
        (
            "abi = dom; in messages;",
            "abi = dom; in messages; in extra;",
        ),
        // what a port carries
        (
            "abi = dom; in messages: \"alice.panel@1\";",
            "abi = dom; in messages;",
        ),
    ] {
        let modules = two_panels(first, second);
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

#[test]
fn two_byte_identical_classes_folding_to_one_kind_stand() {
    let modules = two_panels(DOM_PANEL, DOM_PANEL);
    let config = support::derived_tree(&borrow(&modules));
    assert_eq!(
        config.surface_component_kinds,
        vec![vec!["panel".to_string()], vec!["panel".to_string()]]
    );
}
