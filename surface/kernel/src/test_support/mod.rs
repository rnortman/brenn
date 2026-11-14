//! Shared `#[cfg(test)]` fixtures, so a change to a shape every suite composes
//! lands in one place.
//!
//! Three parts, split by what each builds. [`bindings`] builds bindings
//! documents out of the schema crate's own types, [`pages`] assembles the page a
//! document is put in force over, and [`frames`] writes the server frames a
//! scripted peer sends. All three are available to every suite on every target.
//! [`bindings`] and [`frames`] live in the shared test-peer crate, which the
//! page rig's kit builds its frames and documents from too.

pub(crate) use brenn_surface_test_peer::{bindings, frames};
pub(crate) mod pages;
