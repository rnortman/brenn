//! Brenn surface echo-stub component.
//!
//! A deliberately tiny dev/test fixture, not a product: the page-hosted seam's
//! conformance component, exercising every part of it a kind can reach from one
//! place.
//!
//! It renders each delivered envelope's JSON as text into a bounded scrollback,
//! shows the summed `dropped` in a status line, offers a "send" button that
//! publishes a fixed counter body, a free-form field plus a "send custom"
//! button that publishes the field's value verbatim (the path a test drives to
//! publish a structured or markdown body), and a "panic" button that traps —
//! the last exercising the trap → error-card path from a real component.
//!
//! Its `messages` port is optional: a feeder instance binds only `out` and
//! renders nothing but its own controls.
//!
//! Everything that touches the page lives behind `cfg(target_arch = "wasm32")`:
//! the guest SDK is a wasm32 crate, and the host build carries only the
//! help-sidecar generator and the specification module's port names.

/// The help sidecar's generator. Host-only: it depends on the contract crate,
/// which is not available in the wasm32 build.
#[cfg(not(target_arch = "wasm32"))]
pub mod help;

/// Port names and types from this component's specification.
pub mod spec;

#[cfg(target_arch = "wasm32")]
mod component;
