//! Every rule the grammar defines is named in a table beside the corpus fixture
//! that writes it, and a snippet only that construct produces.
//!
//! Nothing checks that the grammar's labels and the model's field names agree —
//! a disagreement is a positioned deserialize error at runtime, and the corpus
//! is the only thing that makes it happen before a user does. A rule no fixture
//! reaches is therefore a rule whose model side has never been run.
//!
//! The generated parser publishes its rule inventory as `parser::RULE_NAMES`,
//! which is the half of this that cannot go stale: adding a rule to the grammar
//! changes it. The other half is the table below, which is maintained by hand —
//! the CST has no generic node walk, and its `Debug` is deliberately
//! non-recursive, so nothing can derive which rules a parse actually reached.
//!
//! What is mechanically enforced, exactly: the two halves carry the same rule
//! names, each named fixture exists and parses, and each rule's snippet still
//! appears in the fixture named against it. The snippet is what makes the table
//! more than bookkeeping — an edit that removes a construct's last use from a
//! fixture fails here instead of leaving a green row over a rule nothing runs.
//! It is a heuristic in one direction only: a snippet present proves the text is
//! there, not that the rule matched it.

use std::collections::{BTreeMap, BTreeSet};

use brenn_dsl::parser::RULE_NAMES;

mod support;

use support::{corpus_file, corpus_text};

/// Rules reached by no fixture, and why that is deliberate.
///
/// Only trivia belongs here: it is consumed rather than parsed into the tree at
/// a labeled position, and the fixtures that exercise comment handling do so
/// through `line_comment` and `doc`. Anything else in this list is a gap.
const NOT_IN_A_FIXTURE: &[(&str, &str)] = &[(
    "_trivia",
    "the whitespace-and-comment rule the parser consumes between items",
)];

/// Which corpus fixture exercises each grammar rule, and the text in it that
/// only that rule's construct produces.
///
/// One fixture per rule is enough — coverage here means the rule is reached, not
/// that every shape of it is. The suites that read these fixtures are what
/// assert what the rule produces.
///
/// `None` is for the four rules every non-empty document reaches — the goal
/// rule, its item sum, an identifier, a value — where no snippet discriminates
/// anything and one would only pretend to.
const RULE_FIXTURES: &[(&str, &str, Option<&str>)] = &[
    // The document and its declaration vocabulary.
    ("file", "lexical.brenn", None),
    ("item", "lexical.brenn", None),
    (
        "use_stmt",
        "lexical.brenn",
        Some("use wiring::alice::Deskbar;"),
    ),
    ("const_def", "lexical.brenn", Some("const ratio = 1.5;")),
    ("section", "config.brenn", Some("server {")),
    ("attr", "config.brenn", Some("secure_cookies = true;")),
    // Channels.
    (
        "channel_def",
        "statements.brenn",
        Some("channel utterance at"),
    ),
    (
        "chan_decl",
        "statements.brenn",
        Some("channel utterance at \"brenn:alice-pod.out.utterance\""),
    ),
    (
        "chan_tuning",
        "statements.brenn",
        Some("channel at prefix \"mqtt:broker:alice/\""),
    ),
    (
        "chan_addr",
        "statements.brenn",
        Some("at \"brenn:alice-pod.out.utterance\""),
    ),
    ("chan_ref", "statements.brenn", Some("<- messages_p1;")),
    ("uuid_pins", "statements.brenn", Some("uuid_pins {")),
    (
        "pin",
        "statements.brenn",
        Some("= \"bced053b-841b-4551-9868-bfe9c47cc974\";"),
    ),
    // Classes and their parameters.
    (
        "component_class",
        "statements.brenn",
        Some("component Protobar {"),
    ),
    ("port_decl", "statements.brenn", Some("in messages;")),
    ("port_dir", "statements.brenn", Some("out outbound;")),
    (
        "port_doctype",
        "statements.brenn",
        Some(": \"brenn.panel.message@1\";"),
    ),
    (
        "agent_class",
        "statements.brenn",
        Some("agent PersonalAssistant("),
    ),
    (
        "assembly_def",
        "statements.brenn",
        Some("assembly Deskbar("),
    ),
    ("param_list", "statements.brenn", Some("(slug: String,")),
    ("param", "statements.brenn", Some("driver: Agent)")),
    (
        "param_default",
        "statements.brenn",
        Some("skin: String = \"bench\""),
    ),
    // Instantiation and wiring.
    ("surface_def", "statements.brenn", Some("surface panel {")),
    ("new_stmt", "statements.brenn", Some("new p1: Protobar {")),
    (
        "arg_list",
        "statements.brenn",
        Some("(slug = \"alice-pa\","),
    ),
    ("arg", "statements.brenn", Some("driver = alice_pa)")),
    (
        "inst_body",
        "statements.brenn",
        Some("new consume_demo: EchoStub {"),
    ),
    ("binding", "statements.brenn", Some("in messages <- ")),
    ("in_b", "statements.brenn", Some("in inbound <- ")),
    ("out_b", "statements.brenn", Some("out outbound -> ")),
    ("io_b", "statements.brenn", Some("io tick {")),
    ("io_target", "statements.brenn", Some("<-> acks;")),
    // Authority.
    ("acl_stmt", "statements.brenn", Some("acl subscribe [")),
    (
        "matcher_list",
        "statements.brenn",
        Some("[prefix \"brenn:surface.\", exact"),
    ),
    (
        "grant_stmt",
        "statements.brenn",
        Some("grant driver subscribe"),
    ),
    // The remaining entities.
    ("remote_def", "statements.brenn", Some("remote reachy00 {")),
    (
        "webhook_def",
        "sections.brenn",
        Some("webhook push_alice {"),
    ),
    ("repo_def", "entities.brenn", Some("repo life {")),
    (
        "mqtt_client_def",
        "entities.brenn",
        Some("mqtt_client broker {"),
    ),
    (
        "mcp_server_def",
        "entities.brenn",
        Some("mcp_server graf {"),
    ),
    (
        "mcp_server_stmt",
        "statements.brenn",
        Some("mcp_server pfin {"),
    ),
    (
        "mcp_server_ref",
        "statements.brenn",
        Some("mcp_server graf;"),
    ),
    ("mount_stmt", "statements.brenn", Some("mount life;")),
    (
        "subscribe_stmt",
        "statements.brenn",
        Some("subscribe alice_cmd"),
    ),
    ("body_block", "entities.brenn", Some("auto_pull = true;")),
    (
        "tail_block",
        "statements.brenn",
        Some("{ push_depth = 1000; }"),
    ),
    (
        "tail_attr",
        "statements.brenn",
        Some("{ working_dir = true; }"),
    ),
    // Values.
    ("value", "lexical.brenn", None),
    (
        "matcher",
        "lexical.brenn",
        Some("prefix \"brenn:alice-desk.\""),
    ),
    (
        "matcher_val",
        "lexical.brenn",
        Some("exact alice.desk.messages"),
    ),
    (
        "list",
        "lexical.brenn",
        Some("[subscribe, publish, takeover]"),
    ),
    (
        "inline_table",
        "lexical.brenn",
        Some("{ push_depth = 8, retain_depth = 64 }"),
    ),
    ("table_entry", "lexical.brenn", Some("retain_depth = 64 }")),
    (
        "str_like",
        "statements.brenn",
        Some("\"mqtt:broker:alice/\""),
    ),
    ("integer", "lexical.brenn", Some("= -3;")),
    ("float", "lexical.brenn", Some("= 1.5;")),
    ("boolean", "lexical.brenn", Some("= true;")),
    // Names and paths.
    ("name", "lexical.brenn", None),
    ("cname", "statements.brenn", Some("Protobar")),
    ("path", "lexical.brenn", Some("= components_dir;")),
    ("path_seg", "lexical.brenn", Some("alice.desk.messages")),
    ("mod_seg", "lexical.brenn", Some("wiring::alice")),
    ("inst_seg", "lexical.brenn", Some(".desk.messages")),
    // Strings.
    ("string", "lexical.brenn", Some("\"/home/alice/brenn/lib\"")),
    ("str_part", "lexical.brenn", Some("a \\\"quoted\\\" word")),
    ("str_frag", "lexical.brenn", Some("/home/alice/brenn/lib")),
    (
        "fstring",
        "lexical.brenn",
        Some("f\"hello {alice}, welcome\""),
    ),
    ("fstr_part", "lexical.brenn", Some("{{literal}} and")),
    ("fstr_frag", "lexical.brenn", Some(", welcome\"")),
    ("brace_escape", "lexical.brenn", Some("{{literal}}")),
    ("interp", "lexical.brenn", Some("{alice.desk}")),
    ("escape", "lexical.brenn", Some("\\\"quoted\\\"")),
    ("raw_string", "lexical.brenn", Some("\"\"\"raw text")),
    // Comments.
    (
        "line_comment",
        "lexical.brenn",
        Some("// The value vocabulary"),
    ),
    (
        "doc",
        "lexical.brenn",
        Some("/// Where component artifacts live on this host."),
    ),
    (
        "doc_line",
        "lexical.brenn",
        Some("/// Second line of the same doc comment."),
    ),
];

/// Every rule the table accounts for, exempt or covered.
fn accounted_for() -> BTreeSet<&'static str> {
    RULE_FIXTURES
        .iter()
        .map(|(rule, _, _)| *rule)
        .chain(NOT_IN_A_FIXTURE.iter().map(|(rule, _)| *rule))
        .collect()
}

#[test]
fn every_grammar_rule_is_accounted_for() {
    let inventory: BTreeSet<&str> = RULE_NAMES.iter().copied().collect();
    let accounted = accounted_for();

    let unaccounted: Vec<_> = inventory.difference(&accounted).collect();
    assert!(
        unaccounted.is_empty(),
        "these grammar rules are named by no row of the coverage table: {unaccounted:?}"
    );

    let phantom: Vec<_> = accounted.difference(&inventory).collect();
    assert!(
        phantom.is_empty(),
        "these entries name no grammar rule: {phantom:?}"
    );
}

/// A rule listed twice — plausibly with two different fixtures — reads as more
/// coverage than there is, and hides whichever entry someone meant to edit.
#[test]
fn no_rule_is_listed_twice() {
    let mut seen = BTreeMap::new();
    for (rule, fixture) in RULE_FIXTURES
        .iter()
        .map(|(rule, fixture, _)| (rule, fixture))
        .chain(NOT_IN_A_FIXTURE.iter().map(|(rule, why)| (rule, why)))
    {
        if let Some(first) = seen.insert(*rule, *fixture) {
            panic!("`{rule}` is listed twice: {first} and {fixture}");
        }
    }
}

/// Each named fixture exists and parses. A table entry pointing at a fixture
/// that is gone covers nothing, and would otherwise say so nowhere.
#[test]
fn every_named_fixture_exists_and_parses() {
    let fixtures: BTreeSet<&str> = RULE_FIXTURES.iter().map(|(_, name, _)| *name).collect();
    for fixture in fixtures {
        // The loader panics with the full path and the OS error, so a missing
        // fixture is named here without a stat of its own.
        corpus_file(fixture);
    }
}

/// Each rule's construct is still written in the fixture named against it.
///
/// This is the row's teeth: deleting the last `mount` from `statements.brenn`
/// leaves `mount_stmt`'s row naming a fixture that no longer exercises it, and
/// the name-agreement test above cannot see that.
#[test]
fn every_rule_snippet_is_still_written_in_its_fixture() {
    for (rule, fixture, snippet) in RULE_FIXTURES {
        let Some(snippet) = snippet else {
            continue;
        };
        let text = corpus_text(fixture);
        assert!(
            text.contains(snippet),
            "`{rule}` is listed against {fixture}, which no longer writes {snippet:?}"
        );
    }
}
