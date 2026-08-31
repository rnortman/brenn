//! The dom scaffold goldens, compiled.
//!
//! Every other gate on a golden is a byte comparison, which says the emitter is
//! stable and nothing about whether what it emits is Rust the toolchain accepts.
//! Two of these shapes exist nowhere else — an uninhabited port enum, and a
//! class with no ports — so without this target the first component of either
//! shape would be where the emitter's mistake surfaced.
//!
//! Only the dom emission is here: the processor goldens name the guest SDK,
//! which builds for wasm32 alone, so they are compiled by
//! `scaffold_processor_goldens` under a transition instead.

#[path = "corpus/scaffold/dom-full.rs"]
mod dom_full;

#[path = "corpus/scaffold/dom-no-inbound.rs"]
mod dom_no_inbound;

#[path = "corpus/scaffold/dom-no-ports.rs"]
mod dom_no_ports;
