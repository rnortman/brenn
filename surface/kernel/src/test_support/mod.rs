//! Shared `#[cfg(test)]` fixtures, so a change to a shape every suite composes
//! lands in one place.
//!
//! Four parts, split by what they can compile against. [`bindings`] builds
//! bindings documents out of the schema crate's own types, [`pages`] assembles
//! the page a document is put in force over, and [`frames`] writes the server
//! frames a scripted peer sends; all three are available to every suite on
//! every target. The `CoreConfig` and `Welcome` fixtures the core and driver suites
//! are driven with are native-only, like those suites.

pub(crate) mod bindings;
pub(crate) mod frames;
pub(crate) mod pages;

#[cfg(not(target_arch = "wasm32"))]
mod core_suites;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use core_suites::*;
