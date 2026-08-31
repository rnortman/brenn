//! Fragments of DSL text a test fixture states, single-sourced from the
//! vocabulary they spell, and the fence that splits a fixture into a packaged
//! module and the document that imports it.
//!
//! A class states the words its instances grant and nothing else: `optional` is
//! refused for a capability that names a WIT import, because a component reaches
//! it through an import the host links only where the instance grants it. So the
//! forms here take that list rather than hold one.
//!
//! [`surface_any!`](crate::surface_any) is the exception, for the fixture that
//! grants nothing and is about something else: it permits exactly the words a
//! class may leave optional, which is every capability naming no interface.
//!
//! ```
//! const PANEL: &str = concat!(
//!     "component Panel {\n    ",
//!     brenn_dsl::surface_any!(),
//!     "\n    optional in messages;\n}\n",
//! );
//!
//! const SINK: &str = concat!(
//!     "component Sink {\n    ",
//!     brenn_dsl::processor_needs!("ports, log"),
//!     "\n    optional out events;\n}\n",
//! );
//! ```
//!
//! A `concat!` position cannot hold a `const`, and a `format!` position cannot
//! hold a macro call as an inline argument, so a suite that needs both writes
//! `const ANY: &str = surface_any!();` and uses whichever fits. Where the word
//! list is only known at runtime — a case parameterized over what it grants —
//! [`processor_header`] builds the same text.
//!
//! The module is public unconditionally, and that is a choice rather than an
//! oversight: `brenn-lib` and `brenn-bootstrap` tests need these fragments, they
//! link the ordinary library, and no `cfg` spells "a downstream crate's tests
//! only". So a little fixture prose is visible to every consumer, in-tree and
//! out. It promises nothing: an out-of-tree component that spells its own class
//! text is unaffected by a change here, and one that expands these is holding a
//! test fixture.

/// A fixture class's header for a case that grants nothing, permitting
/// everything a class may leave optional:
/// `abi = processor; requires = []; optional = [takeover];`
///
/// Every other capability names a WIT import and so cannot be optional; a
/// fixture whose instance grants one states it with [`processor_needs`].
#[macro_export]
macro_rules! surface_any {
    () => {
        "abi = processor; requires = []; optional = [takeover];"
    };
}

/// A fixture class's header, requiring exactly the words named:
/// `processor_needs!("ports, log")` is
/// `abi = processor; requires = [ports, log];`.
///
/// The list is written out rather than defaulted because an interface word
/// cannot be optional, so the words here are the words its instance grants — no
/// more and no fewer, which is both halves of the spec fit.
#[macro_export]
macro_rules! processor_needs {
    ($words:literal) => {
        concat!("abi = processor; requires = [", $words, "];")
    };
}

/// [`processor_needs`] where the word list is a value rather than a literal.
///
/// A case parameterized over what it grants has to state the same list twice —
/// once as the class's need and once as the instance's grant — and a `concat!`
/// cannot hold a variable.
pub fn processor_header(words: &str) -> String {
    format!("abi = processor; requires = [{words}];")
}

/// The fence a fixture writes around the class declarations that have to live
/// in a packaged module.
///
/// A top-level instance's class is declared in an installed component package,
/// and most fixtures are about something else entirely, so a suite writes the
/// line twice — opening and closing — and [`split_packaged`] does the rest.
/// A macro because a fixture built with `concat!` needs a literal; [`PACKAGED`]
/// is the same text where a value fits.
#[macro_export]
macro_rules! packaged_fence {
    () => {
        "// ── packaged ──\n"
    };
}

/// The name the fenced half is written out under, and so the package every
/// class a fenced fixture declares carries.
///
/// A macro for the same reason as [`packaged_fence`]: the module key a suite
/// spells is `concat!`-built. [`PACKAGED_MODULE`] is the same text as a value.
#[macro_export]
macro_rules! packaged_module {
    () => {
        "fixtures"
    };
}

/// The fence as a value.
pub const PACKAGED: &str = packaged_fence!();

/// The packaged module's name as a value.
pub const PACKAGED_MODULE: &str = packaged_module!();

/// A fenced fixture's packaged module and its root document, or nothing where
/// the fixture writes no fence.
///
/// Every fenced region joins the module; everything else stays in the document,
/// which gains the import in the opening fence's place. Both halves are written
/// line for line — a blank line stands where the other half's text was — so the
/// packaged half keeps the fixture's own numbering and the document half is off
/// by exactly the one line it gains, which is a shift a caller can correct for.
///
/// One implementation for every suite that fences: the halves a lowering test
/// stages on disk and the halves a resolver suite hands the I/O-free core are
/// the same text, and a rule added to the transform reaches both.
///
/// # Panics
///
/// If the fixture opens a fence it never closes.
pub fn split_packaged(source: &str) -> Option<(String, String)> {
    if !source.contains(PACKAGED) {
        return None;
    }
    let mut module = String::new();
    let mut document = format!("use @{PACKAGED_MODULE}::*;\n");
    let mut open = false;
    for (line, half) in fenced(source) {
        match half {
            // The fence line itself is text in neither half, and a blank line
            // in both: that is what keeps the two the same height as the
            // fixture.
            None => {
                open = !open;
                module.push('\n');
                document.push('\n');
            }
            Some(true) => {
                module.push_str(line);
                document.push('\n');
            }
            Some(false) => {
                module.push('\n');
                document.push_str(line);
            }
        }
    }
    assert!(
        !open,
        "fixture opens a packaged fence it never closes; everything after it \
         would silently join the packaged module"
    );
    Some((module, document))
}

/// Each line of a fixture and which half it belongs to: `Some(true)` inside a
/// fence, `Some(false)` outside one, `None` for a fence line, which belongs to
/// neither.
///
/// The rule stated once: [`split_packaged`] writes the halves with it, and a
/// caller that has to know where a byte landed — a span assertion correcting
/// for the import line — reads it with the same iterator.
pub fn fenced(source: &str) -> impl Iterator<Item = (&str, Option<bool>)> {
    let mut inside = false;
    source.split_inclusive('\n').map(move |line| {
        if line == PACKAGED {
            inside = !inside;
            return (line, None);
        }
        (line, Some(inside))
    })
}

#[cfg(test)]
mod tests {
    use brenn_envelope::grants::{ComponentGrant, ComponentHost};

    /// The header a class permitting every word it may leave optional states.
    fn any_header() -> String {
        let words: Vec<&str> = ComponentGrant::ALL
            .into_iter()
            .filter(|grant| grant.illegal_on(ComponentHost::Surface).is_none())
            .filter(|grant| grant.wit_import().is_none())
            .map(ComponentGrant::word)
            .collect();
        format!(
            "abi = processor; requires = []; optional = [{}];",
            words.join(", ")
        )
    }

    /// The two halves a fenced fixture splits into are as tall as the fixture,
    /// give or take the import line the document gains — which is what lets a
    /// suite assert spans against the fixture as written.
    #[test]
    fn both_halves_keep_the_fixture_s_own_numbering() {
        let fixture = concat!(
            "const before = 1;\n",
            packaged_fence!(),
            "component Panel { abi = processor; }\n",
            packaged_fence!(),
            "const after = 2;\n",
        );
        let (module, document) = crate::fixture_text::split_packaged(fixture).expect("a fence");
        let lines = |text: &str| text.matches('\n').count();
        assert_eq!(lines(&module), lines(fixture));
        assert_eq!(lines(&document), lines(fixture) + 1);
        assert!(module.contains("component Panel"));
        assert!(document.contains("const after = 2;"));
        assert!(document.starts_with("use @fixtures::*;\n"));
    }

    #[test]
    fn an_unfenced_fixture_does_not_split() {
        assert!(crate::fixture_text::split_packaged("const only = 1;\n").is_none());
    }

    /// A fence a fixture forgot to close would swallow the rest of the document
    /// into the packaged module, and the test would then assert about text that
    /// is not where its author put it.
    #[test]
    #[should_panic(expected = "never closes")]
    fn an_unclosed_fence_is_a_broken_fixture() {
        let fixture = concat!(
            "const before = 1;\n",
            packaged_fence!(),
            "component Panel { abi = processor; }\n",
        );
        let _ = crate::fixture_text::split_packaged(fixture);
    }

    #[test]
    fn the_open_header_permits_exactly_what_a_class_may_leave_optional() {
        assert_eq!(
            crate::surface_any!(),
            any_header(),
            "a grant word joined or left the vocabulary; the fixture header says otherwise"
        );
    }

    /// The literal form and the runtime form are the same text, so a fixture
    /// that has to state its needs both ways states them once.
    #[test]
    fn a_processor_header_is_the_same_text_either_way() {
        assert_eq!(
            crate::processor_needs!("ports, log"),
            crate::fixture_text::processor_header("ports, log")
        );
        assert_eq!(
            crate::processor_needs!(""),
            crate::fixture_text::processor_header("")
        );
    }
}
