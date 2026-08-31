//! What the guest-module generator refuses, and what reaches the module it
//! emits.
//!
//! The shape of a generated module is pinned by the goldens beside the
//! fixtures, so this suite is the other half: the refusals, which have no
//! output to compare, and the handful of properties a golden states only by
//! example.

mod support;

use brenn_dsl::diag::Diagnostic;
use brenn_dsl::parse_str;
use brenn_dsl::scaffold::generate;

/// The filename every fixture here is parsed under, so a diagnostic's own
/// rendering is stable.
const FILE: &str = "scaffold-fixture.brenn";

/// Generate from fixture text, taking whichever class is named.
fn scaffold(source: &str, class: Option<&str>) -> Result<String, Diagnostic> {
    let file = parse_str(source, FILE).unwrap_or_else(|error| panic!("{}", error.render()));
    generate(&file, class, "fixture.brenn", FILE)
}

/// The module a well-formed fixture produces. A fixture that refuses is a
/// broken test input here, so it panics rather than returning.
fn module(source: &str, class: Option<&str>) -> String {
    scaffold(source, class).unwrap_or_else(|error| panic!("{}", error.render()))
}

/// Why a fixture was refused.
fn refusal(source: &str, class: Option<&str>) -> Diagnostic {
    match scaffold(source, class) {
        Ok(module) => panic!("expected a refusal; generated:\n{module}"),
        Err(error) => error,
    }
}

/// A minimal processor class, with whatever body the caller states.
fn processor(body: &str) -> String {
    format!("component Fixture {{\n  abi = processor;\n  requires = [ports];\n{body}}}\n")
}

// ── class selection ──────────────────────────────────────────────────────────

#[test]
fn a_lone_class_needs_no_flag() {
    let module = module(&processor("  in orders;\n"), None);
    assert!(module.contains("InPort::Orders => \"orders\""), "{module}");
}

#[test]
fn an_assembly_beside_a_class_is_not_a_second_class() {
    // A specification shipping the assembly its component is stamped from is
    // ordinary; an assembly has no guest surface, so it does not make the
    // module ambiguous.
    let source = format!(
        "{}\nassembly Loop(slug: String) {{\n  channel out at f\"ephemeral:{{slug}}.out\" {{\n    \
         description = \"Republished bodies\";\n    push_depth = 8;\n    retain_depth = 8;\n  \
         }}\n}}\n",
        processor("  out results;\n")
    );
    let module = module(&source, None);
    assert!(module.contains("pub const fn results<"), "{module}");
}

#[test]
fn two_classes_without_a_flag_are_refused() {
    let source = format!("{}{}", processor("  in orders;\n"), {
        let mut second = processor("  in reports;\n");
        second = second.replace("component Fixture", "component Other");
        second
    });
    let error = refusal(&source, None);
    assert!(
        error.message.contains("declares 2 component classes"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("--class <Name>"),
        "{}",
        error.message
    );
    // The refusal cites both, so an author sees which two are in play.
    assert_eq!(error.related.len(), 1, "{}", error.render());
    assert_eq!(error.line_col(), Some((1, 11)), "{}", error.render());
}

#[test]
fn the_flag_picks_one_of_them() {
    let source = format!("{}{}", processor("  in orders;\n"), {
        processor("  in reports;\n").replace("component Fixture", "component Other")
    });
    let module = module(&source, Some("Other"));
    assert!(module.contains("InPort::Reports"), "{module}");
    assert!(!module.contains("Orders"), "{module}");
}

#[test]
fn the_flag_naming_no_class_is_refused() {
    let error = refusal(&processor("  in orders;\n"), Some("Missing"));
    assert!(
        error.message.contains("no component class named `Missing`"),
        "{}",
        error.message
    );
    // It lists what the module does declare rather than only what it does not.
    assert!(error.message.contains("`Fixture`"), "{}", error.message);
}

#[test]
fn a_module_with_no_class_is_refused() {
    let error = refusal("const skin = \"bench\";\n", None);
    assert!(
        error.message.contains("declares no component class"),
        "{}",
        error.message
    );
}

// ── identifier mapping ───────────────────────────────────────────────────────

#[test]
fn names_map_to_four_spellings() {
    let module = module(&processor("  io push-events;\n"), None);
    assert!(module.contains("InPort::PushEvents"), "{module}");
    assert!(
        module.contains("pub trait PushEventsPayload: serde::Serialize {}"),
        "{module}"
    );
    // The handle's type parameter is the port's own trait, not a caller-chosen
    // `Serialize`: that bound is what makes the payload checkable.
    assert!(
        module.contains("pub const fn push_events<T: PushEventsPayload>"),
        "{module}"
    );
    assert!(
        module.contains("pub const PUSH_EVENTS: &str = \"push-events\";"),
        "{module}"
    );
    // The name on the wire is what the specification spells, never the mapped
    // identifier.
    assert!(
        module.contains("brenn_guest::OutPort::new(\"push-events\")"),
        "{module}"
    );
}

#[test]
fn ports_differing_only_in_punctuation_collide() {
    let error = refusal(&processor("  in push-events;\n  in push_events;\n"), None);
    assert!(
        error
            .message
            .contains("both map to the enum variant `PushEvents`"),
        "{}",
        error.message
    );
    assert_eq!(error.related.len(), 1, "{}", error.render());
}

#[test]
fn a_collision_in_one_namespace_alone_is_still_a_collision() {
    // Neither maps to one variant — only one of them is inbound — but both map
    // to one port-name constant.
    let error = refusal(&processor("  in re-try;\n  out re_try;\n"), None);
    assert!(
        error
            .message
            .contains("both map to the port-name constant `RE_TRY`"),
        "{}",
        error.message
    );
}

/// Two outbound ports collide in the publish-handle namespace alone: neither is
/// inbound, so no variant is minted, and the constant spellings differ. Without
/// this the handle arm of the collision check could be dead and both other
/// cases would stay green — leaving the emitter free to write two `pub const
/// fn` of one name, a compile error inside generated source.
#[test]
fn outbound_ports_differing_only_in_punctuation_collide_as_handles() {
    let error = refusal(&processor("  out push-events;\n  out push_events;\n"), None);
    assert!(
        error
            .message
            .contains("both map to the publish handle `push_events`"),
        "{}",
        error.message
    );
    assert_eq!(error.related.len(), 1, "{}", error.render());
}

/// An inbound port's variant and an outbound port's payload trait share one
/// type namespace, so a specification cannot mint `PushEventsPayload` twice.
#[test]
fn a_variant_and_a_payload_trait_of_one_spelling_collide() {
    let error = refusal(
        &processor("  in push-events-payload;\n  out push-events;\n"),
        None,
    );
    assert!(
        error
            .message
            .contains("both map to the type name `PushEventsPayload`"),
        "{}",
        error.message
    );
    assert_eq!(error.related.len(), 1, "{}", error.render());
}

/// Two outbound ports whose words differ but whose camel-case spellings do not
/// collide as payload traits and nowhere else: the handles (`a_1b`, `a1b`) and
/// the constants differ, so only the type namespace catches this.
#[test]
fn outbound_ports_colliding_only_as_payload_traits_are_refused() {
    let error = refusal(&processor("  out a-1b;\n  out a1b;\n"), None);
    assert!(
        error
            .message
            .contains("both map to the payload marker trait `A1bPayload`"),
        "{}",
        error.message
    );
    assert_eq!(error.related.len(), 1, "{}", error.render());
}

/// A dom class writes neither publish handle nor payload trait, so neither
/// spelling can collide there: `in foo-payload; out foo;` mints one
/// `FooPayload` under the processor abi and none under dom. The refusal follows
/// what the emitter writes, not what the mapping could name.
#[test]
fn a_dom_class_is_not_refused_for_a_spelling_it_never_emits() {
    let source = "component Fixture {\n  abi = dom;\n  requires = [ports];\n  \
                  in foo-payload;\n  out foo;\n  out match;\n}\n";
    let module = module(source, None);
    assert!(module.contains("InPort::FooPayload"), "{module}");
    assert!(
        module.contains("pub const MATCH: &str = \"match\";"),
        "{module}"
    );
    assert!(!module.contains("pub trait"), "{module}");
    assert!(!module.contains("pub const fn foo"), "{module}");
    // The serde import exists for the trait's supertrait alone, so its absence
    // is the other half of the same branch.
    assert!(!module.contains("use brenn_guest::serde"), "{module}");
}

#[test]
fn an_outbound_port_spelling_a_keyword_is_refused() {
    let error = refusal(&processor("  out match;\n"), None);
    assert!(
        error
            .message
            .contains("maps to `match`, which is a Rust keyword"),
        "{}",
        error.message
    );
    assert_eq!(error.line_col(), Some((4, 7)), "{}", error.render());
}

#[test]
fn an_inbound_port_spelling_a_keyword_is_not() {
    // Only the spellings a port actually emits are checked, and an inbound port
    // emits no function: `in` is `InPort::In` and `port::IN`, neither of which
    // is a keyword.
    let module = module(&processor("  in in;\n"), None);
    assert!(module.contains("InPort::In => \"in\""), "{module}");
    assert!(module.contains("pub const IN: &str = \"in\";"), "{module}");
}

#[test]
fn a_name_that_is_no_identifier_is_refused() {
    let error = refusal(&processor("  in _leading;\n"), None);
    assert!(
        error.message.contains("does not map to a Rust identifier"),
        "{}",
        error.message
    );
}

// ── grants ───────────────────────────────────────────────────────────────────

#[test]
fn declared_capabilities_are_re_exported() {
    let source = "component Fixture {\n  abi = processor;\n  requires = [ports, store, log];\n  \
                  in orders;\n}\n";
    let module = module(source, None);
    assert!(module.contains("pub use brenn_guest::log;"), "{module}");
    assert!(module.contains("pub use brenn_guest::store;"), "{module}");
    // `ports` is embodied by the publish handles, so it names no module.
    assert!(!module.contains("brenn_guest::ports"), "{module}");
}

#[test]
fn an_unknown_grant_word_is_refused() {
    let source = "component Fixture {\n  abi = processor;\n  requires = [ports, telepathy];\n  \
                  in orders;\n}\n";
    let error = refusal(source, None);
    assert!(
        error
            .message
            .contains("`telepathy` is not a capability a component holds"),
        "{}",
        error.message
    );
    assert_eq!(error.line_col(), Some((3, 22)), "{}", error.render());
}

#[test]
fn an_unknown_abi_word_is_refused() {
    let source = "component Fixture {\n  abi = wasi;\n  requires = [ports];\n  in orders;\n}\n";
    let error = refusal(source, None);
    assert!(
        error.message.contains("`wasi` is not an abi"),
        "{}",
        error.message
    );
}

// ── what each abi emits ──────────────────────────────────────────────────────

#[test]
fn the_dom_emission_names_no_guest_sdk() {
    let source = "component Fixture {\n  abi = dom;\n  requires = [ports, log];\n  in theme;\n  \
                  out overlay-state;\n}\n";
    let module = module(source, None);
    // No window classifier, no publish handles, no capability re-exports: the
    // dom SDK is free functions over `&str`, so there is nothing to name.
    assert!(!module.contains("brenn_guest"), "{module}");
    assert!(!module.contains("pub const fn overlay_state"), "{module}");
    assert!(
        module.contains("pub const OVERLAY_STATE: &str = \"overlay-state\";"),
        "{module}"
    );
    assert!(module.contains("pub fn from_name("), "{module}");
}

#[test]
fn doctypes_reach_the_module_as_prose() {
    let module = module(
        &processor("  in orders: \"brenn.scaffold.orders@1\";\n"),
        None,
    );
    assert!(
        module.contains("/// Doctype: `brenn.scaffold.orders@1`."),
        "{module}"
    );
}

#[test]
fn an_interpolated_doctype_carries_no_note() {
    // The constants an f-string names are the compiler's to resolve, and a
    // doctype is a nominal tag with no runtime consumer, so the note is dropped
    // rather than the module refused.
    let source = format!(
        "const family = \"brenn.scaffold\";\n{}",
        processor("  in orders: f\"{family}.orders@1\";\n")
    );
    let module = module(&source, None);
    assert!(!module.contains("Doctype:"), "{module}");
    assert!(module.contains("InPort::Orders"), "{module}");
}

/// A doctype carrying any control character carries no note. It would be
/// written into a `///` line comment, where a bare CR ends the comment as
/// surely as a LF does — and the failure would be a lexer error inside a
/// generated file rather than anything a reader can trace to a specification.
#[test]
fn a_doctype_carrying_a_control_character_carries_no_note() {
    for escape in ["\\r", "\\n", "\\0"] {
        let module = module(
            &processor(&format!("  in orders: \"brenn{escape}.orders\";\n")),
            None,
        );
        assert!(!module.contains("Doctype:"), "{escape}: {module}");
        assert!(module.contains("InPort::Orders"), "{escape}: {module}");
    }
}

#[test]
fn the_header_names_the_specification_it_came_from() {
    let module = module(&processor("  in orders;\n"), None);
    assert!(
        module.starts_with("// Generated from fixture.brenn — do not edit.\n"),
        "{module}"
    );
}

#[test]
fn the_class_prose_is_carried_into_the_module() {
    let source = format!(
        "/// What this component is for.\n{}",
        processor("  in orders;\n")
    );
    let module = module(&source, None);
    assert!(
        module.contains("//! What this component is for."),
        "{module}"
    );
}

#[test]
fn a_class_with_no_inbound_port_yields_an_uninhabited_enum() {
    let module = module(&processor("  out beats;\n"), None);
    assert!(module.contains("pub enum InPort {}"), "{module}");
    assert!(
        module.contains("pub const ALL: [InPort; 0] = [];"),
        "{module}"
    );
    assert!(
        module.contains("pub fn from_name(_name: &str) -> Option<InPort> {"),
        "{module}"
    );
}

/// The corpus fixtures the goldens are built from parse and generate here too,
/// so a fixture that stops being generatable fails as a test rather than as a
/// build action nobody reads.
#[test]
fn every_golden_fixture_generates() {
    for name in ["processor-full", "dom-full", "no-inbound", "no-outbound"] {
        let source = support::corpus_text(&format!("scaffold/{name}.brenn"));
        let file = parse_str(&source, name).unwrap_or_else(|error| panic!("{}", error.render()));
        generate(&file, None, name, name)
            .unwrap_or_else(|error| panic!("{name}: {}", error.render()));
    }
}
