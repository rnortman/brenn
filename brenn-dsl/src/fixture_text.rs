//! Fragments of DSL text a test fixture states, single-sourced from the
//! vocabulary they spell.
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
