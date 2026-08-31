//! The scaffold goldens, compiled.
//!
//! The emission names the guest SDK, which builds for wasm32 alone, so this is
//! reached through a transition. It exists because every other gate on a golden
//! is a byte comparison, which says the emitter is stable and nothing about
//! whether what it emits is Rust the toolchain accepts.
//!
//! The uninhabited-inbound shapes are the ones that need it most — no in-tree
//! component has zero inbound ports or no ports at all, so nothing else ever
//! compiles a `name` that is `match self {}` or a `from_name` that returns
//! `None` outright.

#[path = "corpus/scaffold/processor-full.rs"]
pub mod processor_full;

#[path = "corpus/scaffold/no-inbound.rs"]
pub mod no_inbound;

#[path = "corpus/scaffold/no-outbound.rs"]
pub mod no_outbound;

#[path = "corpus/scaffold/no-ports.rs"]
pub mod no_ports;

/// The payload binding the generated publish handles exist for, exercised in
/// both shapes a guest writes.
///
/// The handle takes only types bound to its own port, so what is proven here is
/// that the bound admits the two idioms at all: an owned payload named in a
/// `const`, and a borrowed one — which no `const` can name — inferred at an
/// inline call. Publishing an unbound type is a compile error by construction
/// and needs no harness.
pub mod payload_binding {
    use super::{no_inbound, processor_full};

    #[derive(brenn_guest::serde::Serialize)]
    #[serde(crate = "brenn_guest::serde")]
    pub struct Owned {
        pub count: u32,
    }

    #[derive(brenn_guest::serde::Serialize)]
    #[serde(crate = "brenn_guest::serde")]
    pub struct Body<'a> {
        pub text: &'a str,
    }

    impl processor_full::ResultsPayload for Owned {}
    // Two types bound to one port: the documented multi-impl case, compiled.
    impl processor_full::ResultsPayload for Body<'_> {}
    impl processor_full::TickPayload for Body<'_> {}
    impl no_inbound::BeatsPayload for Body<'_> {}

    pub const RESULTS: brenn_guest::OutPort<Owned> = processor_full::results();

    pub fn owned() -> Result<(), brenn_guest::Error> {
        RESULTS.publish(&Owned { count: 1 })
    }

    pub fn borrowed(text: &str) -> Result<(), brenn_guest::Error> {
        processor_full::tick().publish(&Body { text })?;
        processor_full::results().publish(&Body { text })?;
        no_inbound::beats().publish(&Body { text })
    }
}
