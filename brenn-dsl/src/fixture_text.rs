//! Fragments of DSL text a test fixture states, single-sourced from the
//! vocabulary they spell, and the fence that splits a fixture into a packaged
//! module and the document that imports it.
//!
//! A test's `component` class is written to stay out of the way: it permits
//! every capability its host admits, so each case grants what the case is about
//! and the spec fit answers nothing else. That is a transcription of
//! [`ComponentGrant::ALL`] minus what
//! [`ComponentGrant::illegal_on`](brenn_envelope::grants::ComponentGrant::illegal_on)
//! rejects, and a transcription per fixture is the hand table the vocabulary is
//! single-sourced to avoid: an eighth variant would mean hunting every fixture
//! that permits everything and deciding, one by one, whether it still does.
//!
//! So the two headers live here once and the tests below pin them to the enum.
//! A macro rather than a `const` because a fixture is `concat!`-built from
//! literals, and `concat!` takes a macro that expands to one.
//!
//! ```
//! const PANEL: &str = concat!(
//!     "component Panel {\n    ",
//!     brenn_dsl::dom_any!(),
//!     "\n    optional in messages;\n}\n",
//! );
//! ```
//!
//! A `concat!` position cannot hold a `const`, and a `format!` position cannot
//! hold a macro call as an inline argument, so a suite that needs both writes
//! `const DOM: &str = dom_any!();` and uses whichever fits.
//!
//! The module is public unconditionally, and that is a choice rather than an
//! oversight: `brenn-lib` and `brenn-bootstrap` tests need these fragments, they
//! link the ordinary library, and no `cfg` spells "a downstream crate's tests
//! only". So two macros of fixture prose are visible to every consumer,
//! in-tree and out. They promise nothing: an out-of-tree component that spells
//! its own class text is unaffected by a change here, and one that expands these
//! is holding a test fixture.

/// A surface-hosted fixture class's header, permitting everything a surface
/// admits:
/// `abi = dom; requires = []; optional = [ports, log, alert, config, takeover];`
#[macro_export]
macro_rules! dom_any {
    () => {
        "abi = dom; requires = []; optional = [ports, log, alert, config, takeover];"
    };
}

/// A backend-hosted fixture class's header, permitting everything a top-level
/// host admits:
/// `abi = processor; requires = []; optional = [ports, store, log, alert, config, mqtt,
/// tools];`
#[macro_export]
macro_rules! processor_any {
    () => {
        "abi = processor; requires = []; optional = [ports, store, log, alert, config, mqtt, tools];"
    };
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

    /// The header a class permitting everything this host admits states.
    fn any_header(abi: &str, host: ComponentHost) -> String {
        let words: Vec<&str> = ComponentGrant::ALL
            .into_iter()
            .filter(|grant| grant.illegal_on(host).is_none())
            .map(ComponentGrant::word)
            .collect();
        format!(
            "abi = {abi}; requires = []; optional = [{}];",
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
            "component Panel { abi = dom; }\n",
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
            "component Panel { abi = dom; }\n",
        );
        let _ = crate::fixture_text::split_packaged(fixture);
    }

    #[test]
    fn each_header_permits_exactly_what_its_host_admits() {
        assert_eq!(
            crate::dom_any!(),
            any_header("dom", ComponentHost::Surface),
            "a grant word joined or left the vocabulary; the fixture header says otherwise"
        );
        assert_eq!(
            crate::processor_any!(),
            any_header("processor", ComponentHost::TopLevel),
            "a grant word joined or left the vocabulary; the fixture header says otherwise"
        );
    }
}
