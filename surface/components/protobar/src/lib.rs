//! Brenn surface protobar component.
//!
//! Receive-only component (contract v0): subscribes to one ephemeral channel,
//! renders the latest live message body, shows drop/gap indicators.
//!
//! Split into a DOM-free, host-tested half — the display state machine
//! (`logic`) and the markdown tree builder (`markdown`) — and a
//! `cfg(target_arch = "wasm32")` glue module that walks the tree onto the page
//! through the guest SDK's `dom` capability.

/// The help sidecar's generator. Host-only: it depends on the contract crate,
/// which is not available in the wasm32 build.
#[cfg(not(target_arch = "wasm32"))]
pub mod help;
pub mod logic;
pub mod markdown;

/// Port names and types from this component's specification.
pub mod spec;

#[cfg(target_arch = "wasm32")]
mod component;
