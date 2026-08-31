//! A component specification's `requires` list against what its artifact
//! actually imports.
//!
//! Three statements bound what a component may reach, and this module closes
//! the triangle. The deployment compile holds an instance's `grants` against
//! its class's `requires`, and the host holds the artifact's reflected imports
//! against that same instance's grants at load. This module holds the
//! specification against the artifact directly, so a class requiring a
//! capability its code never imports — or importing one it never declared —
//! is caught at build time rather than at deployment.
//!
//! The comparison is set equality in both directions, and both directions are
//! drift:
//!
//! - an import the specification does not require is code that grew a need its
//!   author never wrote down, so every deployment of it is over-granting
//!   against a specification that understates the component;
//! - a required grant the artifact never imports is a need the code does not
//!   have, so every deployment of it is granting authority nothing reads.
//!
//! The types interface is excluded: every component carries it, it is no
//! capability, and no grant word names it.
//!
//! Names are compared *with* their versions, byte for byte against the
//! canonical name each grant word links. That is deliberately stricter than
//! the host, which resolves an import by semver compatibility, so this check
//! can refuse an artifact the host would have accepted — never the reverse.
//! An unversioned or differently-versioned import is exactly the shape whose
//! boot outcome this check exists to front-run: the host would either bind it
//! under a rule this module does not implement, or refuse the component at
//! load and blame the deployment. Refusing it here, loudly, at build time, with
//! the canonical name in the message, is the answer that cannot be wrong. If an
//! interface version ever moves, the host's matching rule and this check are
//! updated together.

use std::collections::BTreeSet;
use std::path::Path;

use brenn_envelope::grants::ComponentGrant;

use crate::diag::Diagnostic;
use crate::model::ComponentClass;
use crate::resolved::Abi;
use crate::scaffold::select_class;

/// The types-only interface every component imports and no grant names.
///
/// Versioned, like every other name this module compares: an artifact carrying
/// some other version of it is not carrying this one, and the host resolves it
/// by the same rule it resolves a capability interface by.
const TYPES_INTERFACE: &str = "brenn:processor/types@0.1.0";

/// Which side of the equality a name failed on.
///
/// Kept as data rather than rendered on the spot so the unit tests assert the
/// classification rather than a message's prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// Interfaces the artifact imports that no required grant names.
    pub undeclared: Vec<String>,
    /// Grants the specification requires whose interface the artifact never
    /// imports.
    pub unimported: Vec<ComponentGrant>,
}

/// Hold one class's `requires` list equal to one artifact's import profile.
///
/// `imports` is the artifact's fully-qualified import names, in any order,
/// carrying the versions the artifact declares them at.
pub fn compare(requires: &[ComponentGrant], imports: &[String]) -> Result<(), Mismatch> {
    let imported: BTreeSet<&str> = imports
        .iter()
        .map(String::as_str)
        .filter(|name| *name != TYPES_INTERFACE)
        .collect();
    let mut required: BTreeSet<&str> = BTreeSet::new();
    let mut unimported: Vec<ComponentGrant> = Vec::new();
    for grant in requires {
        // A grant naming no interface has nothing on the import side to be
        // equal to. `required_grants` refuses such a word before it reaches
        // here, so this is a guard on the direct callers of `compare` rather
        // than a path the check takes; skipping is the only answer that does
        // not invent an interface for a word that names none.
        let Some(interface) = grant.wit_import() else {
            continue;
        };
        required.insert(interface);
        if !imported.contains(interface) {
            unimported.push(*grant);
        }
    }
    let undeclared: Vec<String> = imported
        .iter()
        .filter(|name| !required.contains(*name))
        .map(|name| (*name).to_string())
        .collect();
    if undeclared.is_empty() && unimported.is_empty() {
        return Ok(());
    }
    Err(Mismatch {
        undeclared,
        unimported,
    })
}

/// A WIT name without its `@version` suffix.
///
/// Splits at the first `@`: a legal WIT name carries at most one, after the
/// interface segment. Used only to *explain* a mismatch — never to decide one —
/// so that an import differing from a canonical name in its version alone is
/// reported as the version drift it is rather than as an interface nobody
/// names.
fn strip_version(name: &str) -> &str {
    name.split_once('@').map_or(name, |(base, _)| base)
}

/// The whole check over one authored specification file and one import list.
///
/// The class is selected by the same exactly-one-`component` rule the guest
/// scaffold generator uses, through the same helper: a packaged specification
/// declaring zero or several classes has no single answer to "what does this
/// artifact require", and inventing one here would let the module that
/// generates the guest and the check that judges the artifact disagree about
/// which class the package is.
pub fn check_file(spec: &Path, imports: &[String]) -> Result<(), Diagnostic> {
    let filename = spec.display().to_string();
    let file = crate::parse_file(spec)?;
    let class = select_class(&file, None, &filename)?;
    check_class(class, imports)
}

/// The check over one already-selected class.
pub fn check_class(class: &ComponentClass, imports: &[String]) -> Result<(), Diagnostic> {
    let requires = required_grants(class)?;
    let words: Vec<ComponentGrant> = requires.iter().map(|(grant, _)| *grant).collect();
    let Err(mismatch) = compare(&words, imports) else {
        return Ok(());
    };
    Err(report(class, &requires, &mismatch))
}

/// A class's `requires` words, parsed, with the span each was written at.
///
/// The abi is checked first. A dom class's grants are enforced at a binding in
/// the page, not by a linker over an import list, so holding one against an
/// artifact's imports would compare two unrelated things and call the result
/// drift.
///
/// A word naming no WIT interface is refused rather than skipped. The resolver
/// admits it — a processor class's words are host-checked at the instance, not
/// at the class — so it reaches here, and dropping it silently would make the
/// set equality partial over exactly the word the author got wrong.
fn required_grants(
    class: &ComponentClass,
) -> Result<Vec<(ComponentGrant, crate::Span)>, Diagnostic> {
    let word = &class.attrs.abi.value.name;
    match Abi::parse(word.value()) {
        Some(Abi::Processor) => {}
        _ => {
            return Err(Diagnostic::at(
                format!(
                    "`{}` states `abi = {}`; only a processor class's needs are linked as WIT \
                     imports, so only a processor class can be held against an artifact's \
                     import list",
                    class.name.value(),
                    word.value()
                ),
                word.span().clone(),
            ));
        }
    }
    let Some(attr) = class.attrs.requires.as_ref() else {
        return Err(Diagnostic::at(
            format!(
                "`{}` states no `requires`, so there is nothing to hold its artifact's imports \
                 against",
                class.name.value()
            ),
            class.name.span().clone(),
        ));
    };
    let mut grants = Vec::new();
    for word in &attr.value.words {
        let Some(grant) = ComponentGrant::parse(word.name.value()) else {
            return Err(Diagnostic::at(
                format!(
                    "`{}` is not a capability a component holds",
                    word.name.value()
                ),
                word.name.span().clone(),
            ));
        };
        if grant.wit_import().is_none() {
            return Err(Diagnostic::at(
                format!(
                    "`{}` names no WIT interface, so a processor class cannot require it: it \
                     is a page capability, consented to at a binding, and no artifact can \
                     import it",
                    word.name.value()
                ),
                word.name.span().clone(),
            ));
        }
        grants.push((grant, word.name.span().clone()));
    }
    Ok(grants)
}

/// One diagnostic naming every name that failed, on whichever side it failed.
///
/// Both halves in one report: an author fixing an undeclared import should not
/// discover an unimported requirement on the next build.
fn report(
    class: &ComponentClass,
    requires: &[(ComponentGrant, crate::Span)],
    mismatch: &Mismatch,
) -> Diagnostic {
    let name = class.name.value();
    let mut lines: Vec<String> = Vec::new();
    for import in &mismatch.undeclared {
        let exact = ComponentGrant::ALL
            .into_iter()
            .find(|grant| grant.wit_import() == Some(import.as_str()));
        // A name that matches a canonical interface but not its version is its
        // own diagnosis: proposing a grant word for it would be advice that
        // does not fix it, and calling it unnamed would be false.
        let versioned = ComponentGrant::ALL
            .into_iter()
            .find(|grant| grant.wit_import().map(strip_version) == Some(strip_version(import)));
        match (exact, versioned) {
            (Some(grant), _) => lines.push(format!(
                "the artifact imports `{import}`, which `{name}` does not require; add `{}` to \
                 its `requires`, or stop importing the interface",
                grant.word()
            )),
            (None, Some(grant)) => lines.push(format!(
                "the artifact imports `{import}`, but this host links `{}` for `{}`; the two \
                 versions must be the same name, or the host resolves the import under a rule \
                 this check does not implement",
                grant.wit_import().unwrap_or_default(),
                grant.word()
            )),
            (None, None) => lines.push(format!(
                "the artifact imports `{import}`, which no capability word names; the host \
                 links nothing for it and refuses the component at load"
            )),
        }
    }
    for grant in &mismatch.unimported {
        lines.push(format!(
            "`{name}` requires `{}`, which the artifact never imports; every deployment of it \
             grants authority the code does not read",
            grant.word()
        ));
    }
    let mut diagnostic = Diagnostic::at(
        format!(
            "`{name}`'s specification and its built artifact disagree about what it needs:\n  \
             {}",
            lines.join("\n  ")
        ),
        class.name.span().clone(),
    );
    // Every unimported requirement is pointed at where it was written; an
    // undeclared import has no site in the specification by definition, so the
    // class name is where that half lands.
    for grant in &mismatch.unimported {
        if let Some((_, span)) = requires.iter().find(|(word, _)| word == grant) {
            diagnostic
                .related
                .push((format!("`{}` is required here", grant.word()), span.clone()));
        }
    }
    diagnostic
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn an_artifact_importing_exactly_what_the_spec_requires_passes() {
        let requires = [ComponentGrant::Ports, ComponentGrant::Log];
        let imports = names(&[
            "brenn:processor/log@0.1.0",
            "brenn:processor/ports@0.1.0",
            "brenn:processor/types@0.1.0",
        ]);
        assert_eq!(compare(&requires, &imports), Ok(()));
    }

    /// The types interface is carried by every component and named by no word,
    /// so its presence must not read as an undeclared import.
    #[test]
    fn the_types_interface_is_neither_side_of_the_comparison() {
        let imports = names(&["brenn:processor/types@0.1.0"]);
        assert_eq!(compare(&[], &imports), Ok(()));
    }

    #[test]
    fn an_import_the_spec_does_not_require_is_drift() {
        let requires = [ComponentGrant::Ports];
        let imports = names(&["brenn:processor/ports@0.1.0", "brenn:processor/store@0.1.0"]);
        assert_eq!(
            compare(&requires, &imports),
            Err(Mismatch {
                undeclared: vec!["brenn:processor/store@0.1.0".to_string()],
                unimported: Vec::new(),
            })
        );
    }

    #[test]
    fn a_requirement_the_artifact_does_not_import_is_drift() {
        let requires = [ComponentGrant::Ports, ComponentGrant::Alert];
        let imports = names(&["brenn:processor/ports@0.1.0"]);
        assert_eq!(
            compare(&requires, &imports),
            Err(Mismatch {
                undeclared: Vec::new(),
                unimported: vec![ComponentGrant::Alert],
            })
        );
    }

    #[test]
    fn drift_in_both_directions_is_reported_at_once() {
        let requires = [ComponentGrant::Alert];
        let imports = names(&["brenn:processor/store@0.1.0"]);
        assert_eq!(
            compare(&requires, &imports),
            Err(Mismatch {
                undeclared: vec!["brenn:processor/store@0.1.0".to_string()],
                unimported: vec![ComponentGrant::Alert],
            })
        );
    }

    /// The host binds an import by semver compatibility; this check binds it
    /// by name. A version the canonical name does not carry is therefore drift
    /// here even where the host would have resolved it — the strict direction,
    /// which turns a boot-time refusal blamed on a deployment into a build-time
    /// refusal naming the artifact.
    #[test]
    fn an_import_at_another_version_is_drift() {
        let requires = [ComponentGrant::Config];
        for spelling in [
            "brenn:processor/config",
            "brenn:processor/config@0.2.0",
            "brenn:processor/config@0.1.1",
        ] {
            assert_eq!(
                compare(&requires, &names(&[spelling])),
                Err(Mismatch {
                    undeclared: vec![spelling.to_string()],
                    unimported: vec![ComponentGrant::Config],
                }),
                "{spelling}"
            );
        }
    }

    /// The types interface gets no exemption from the version rule: a component
    /// carrying another version of it is one the host refuses at load.
    #[test]
    fn the_types_interface_at_another_version_is_drift() {
        let imports = names(&["brenn:processor/types@0.2.0"]);
        assert_eq!(
            compare(&[], &imports),
            Err(Mismatch {
                undeclared: vec!["brenn:processor/types@0.2.0".to_string()],
                unimported: Vec::new(),
            })
        );
    }

    /// A foreign interface cannot pass as a host one: the package namespace is
    /// part of the name on both sides.
    #[test]
    fn a_foreign_interface_spelling_a_capability_word_is_undeclared() {
        let imports = names(&["wasi:logging/log@0.1.0"]);
        assert_eq!(
            compare(&[ComponentGrant::Log], &imports),
            Err(Mismatch {
                undeclared: vec!["wasi:logging/log@0.1.0".to_string()],
                unimported: vec![ComponentGrant::Log],
            })
        );
    }

    /// One import listed twice — which a scrape that lost its dedup would emit
    /// — is one import, not a second undeclared name.
    #[test]
    fn a_repeated_import_is_counted_once() {
        let imports = names(&["brenn:processor/ports@0.1.0", "brenn:processor/ports@0.1.0"]);
        assert_eq!(compare(&[ComponentGrant::Ports], &imports), Ok(()));
    }

    /// The class-level entry point, over a specification as an author writes
    /// one.
    fn refusal(source: &str, imports: &[&str]) -> String {
        let file = crate::parse_str(source, "spec.brenn").expect("the fixture parses");
        let class = select_class(&file, None, "spec.brenn").expect("one class");
        check_class(class, &names(imports))
            .expect_err("this fixture is supposed to be refused")
            .message
    }

    #[test]
    fn a_spec_matching_its_artifact_passes_through_the_class() {
        let file = crate::parse_str(
            "component Demo {\n    abi = processor; requires = [ports];\n}\n",
            "spec.brenn",
        )
        .expect("the fixture parses");
        let class = select_class(&file, None, "spec.brenn").expect("one class");
        let imports = names(&["brenn:processor/ports@0.1.0", "brenn:processor/types@0.1.0"]);
        assert!(check_class(class, &imports).is_ok());
    }

    /// A dom class's grants are gated at a binding in the page, not linked as
    /// imports, so there is nothing here to hold an artifact against.
    #[test]
    fn a_dom_class_is_not_held_against_an_import_list() {
        let message = refusal(
            "component Panel {\n    abi = dom; requires = [ports];\n}\n",
            &["brenn:processor/ports@0.1.0"],
        );
        assert!(message.contains("`abi = dom`"), "{message}");
    }

    #[test]
    fn one_report_carries_both_halves_of_the_drift() {
        let message = refusal(
            "component Sink {\n    abi = processor; requires = [alert];\n}\n",
            &["brenn:processor/store@0.1.0", "brenn:processor/types@0.1.0"],
        );
        assert!(
            message.contains("imports `brenn:processor/store@0.1.0`"),
            "{message}"
        );
        assert!(message.contains("requires `alert`"), "{message}");
    }

    /// The word an undeclared import corresponds to is named, so the fix is in
    /// the message rather than in a table the author has to find.
    #[test]
    fn an_undeclared_import_names_the_word_that_declares_it() {
        let message = refusal(
            "component Sink {\n    abi = processor; requires = [];\n}\n",
            &["brenn:processor/tools@0.1.0", "brenn:processor/types@0.1.0"],
        );
        assert!(message.contains("add `tools`"), "{message}");
    }

    /// A word naming no WIT interface is refused rather than silently dropped,
    /// so the set equality is never partial.
    #[test]
    fn a_required_word_naming_no_interface_is_refused() {
        let message = refusal(
            "component Sink {\n    abi = processor; requires = [ports, takeover];\n}\n",
            &["brenn:processor/ports@0.1.0", "brenn:processor/types@0.1.0"],
        );
        assert!(
            message.contains("`takeover` names no WIT interface"),
            "{message}"
        );
    }

    /// An import no word names is a component the host refuses at load, and the
    /// message says so rather than proposing a grant that does not exist.
    #[test]
    fn an_import_no_word_names_is_reported_as_unlinkable() {
        let message = refusal(
            "component Sink {\n    abi = processor; requires = [];\n}\n",
            &["wasi:clocks/monotonic-clock@0.2.0"],
        );
        assert!(message.contains("no capability word names"), "{message}");
    }

    /// An import that is a capability interface at some other version says so,
    /// rather than proposing a grant word that would not fix it or claiming
    /// nothing names the interface.
    #[test]
    fn a_version_drifted_import_is_reported_as_a_version() {
        let message = refusal(
            "component Sink {\n    abi = processor; requires = [ports];\n}\n",
            &["brenn:processor/ports@0.2.0", "brenn:processor/types@0.1.0"],
        );
        assert!(
            message.contains("this host links `brenn:processor/ports@0.1.0` for `ports`"),
            "{message}"
        );
    }
}
