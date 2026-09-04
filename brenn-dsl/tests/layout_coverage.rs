//! Every grammar rule that opens a brace has a layout block in the format spec.
//!
//! The format-spec syntax has no multi-rule block and no default for
//! brace-bearing rules, so each one's layout is written by hand. Omitting one
//! fails nothing at build time: the formatter silently renders that rule's whole
//! body on one line, and the first anyone hears of it is an ugly diff in a
//! config file. This is that missing build-time failure.

use std::collections::BTreeSet;
use std::path::PathBuf;

use brenn_dsl::parser::RULE_NAMES;

/// Rules that open a brace and want no layout of their own.
///
/// `interp` is an f-string's `{path}`: it is a value, not a body, and breaking
/// inside it would change the string.
const NO_LAYOUT: &[&str] = &["interp"];

fn grammar_file(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("grammar")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// The names of the grammar rules whose definition parses a literal `{`.
///
/// A rule runs from its `name :=` line to the next one, so a definition split
/// across lines is read whole. Both spellings count: a suppressed `%"{"` and a
/// labeled `open:"{"`, which is what a rule whose body is optional must write
/// so the unparser emits the brace only where one was.
fn brace_bearing_rules(grammar: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut current: Option<(String, String)> = None;
    for line in grammar.lines() {
        let code = line.split("//").next().unwrap_or("");
        if let Some((name, rest)) = code.split_once(":=")
            && !name.trim().is_empty()
            && name.trim().chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            if let Some((name, body)) = current.take()
                && body.contains("\"{\"")
            {
                found.insert(name);
            }
            current = Some((name.trim().to_owned(), rest.to_owned()));
        } else if let Some((_, body)) = current.as_mut() {
            body.push_str(code);
        }
    }
    if let Some((name, body)) = current
        && body.contains("\"{\"")
    {
        found.insert(name);
    }
    found
}

/// The names the format spec writes a `rule` block for.
fn rules_with_layout(spec: &str) -> BTreeSet<String> {
    spec.lines()
        .filter_map(|line| {
            line.split("//")
                .next()
                .unwrap_or("")
                .trim()
                .strip_prefix("rule ")
        })
        .map(|name| name.trim().to_owned())
        .collect()
}

/// The rules the scan is expected to find.
///
/// Pinned rather than merely non-empty: the scan reads one spelling of an
/// opening brace (`%"{"`), so a rule that opens one another way would be
/// invisible to it and the gate would silently cover less than it says. Adding a
/// brace-bearing rule fails this list first, which is where the author is told
/// to write its layout block.
const BRACED_RULES: &[&str] = &[
    "agent_class",
    "assembly_def",
    "body_block",
    "component_class",
    "inline_table",
    "inst_body",
    "interp",
    "remote_def",
    "section",
    "surface_def",
    "tail_block",
    "uuid_pins",
    "webhook_def",
];

#[test]
fn the_grammar_scan_finds_every_rule_that_opens_a_brace() {
    let braced = brace_bearing_rules(&grammar_file("brenn.fltkg"));
    let expected: BTreeSet<String> = BRACED_RULES.iter().map(|name| (*name).to_owned()).collect();
    assert_eq!(
        braced, expected,
        "the set of brace-bearing rules changed; the layout spec and this list both want the news"
    );
}

#[test]
fn every_braced_grammar_rule_has_a_layout_block() {
    let braced = brace_bearing_rules(&grammar_file("brenn.fltkg"));
    let laid_out = rules_with_layout(&grammar_file("brenn.fltkfmt"));
    let missing: Vec<_> = braced
        .iter()
        .filter(|name| !laid_out.contains(*name) && !NO_LAYOUT.contains(&name.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "these rules open a brace and would render on one line: {missing:?}"
    );
}

/// A layout block naming a rule the grammar does not define is inert, and reads
/// as coverage that is not there.
///
/// The inventory is the generated parser's own `RULE_NAMES`, which is the half
/// that cannot go stale — the same source the fixture-coverage gate reads, so
/// the two suites cannot disagree about what the rule inventory is. The text
/// scan above stays because it answers a different question, one the inventory
/// does not carry: which rules open a brace.
///
/// The spec scan itself needs no pinned list of its own: with the grammar side
/// pinned above, a `rule` heuristic that stopped matching would take every
/// braced rule out of `laid_out` and fail the previous test by name.
#[test]
fn every_layout_block_names_a_rule_the_grammar_defines() {
    let defined: BTreeSet<&str> = RULE_NAMES.iter().copied().collect();

    let orphans: Vec<_> = rules_with_layout(&grammar_file("brenn.fltkfmt"))
        .into_iter()
        .filter(|name| !defined.contains(name.as_str()))
        .collect();
    assert!(orphans.is_empty(), "no such grammar rule: {orphans:?}");
}

/// The scan reads the grammar text; the inventory is what the parser was
/// generated from. A rule the scan reports that the parser does not have means
/// the scan is reading something that is not a rule definition.
#[test]
fn the_grammar_scan_reports_only_real_rules() {
    let inventory: BTreeSet<&str> = RULE_NAMES.iter().copied().collect();
    let phantom: Vec<_> = brace_bearing_rules(&grammar_file("brenn.fltkg"))
        .into_iter()
        .filter(|name| !inventory.contains(name.as_str()))
        .collect();
    assert!(
        phantom.is_empty(),
        "the scan read these as rules and the parser has no such rule: {phantom:?}"
    );
}
