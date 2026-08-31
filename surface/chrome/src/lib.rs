//! The in-tree default chrome component (`brenn-chrome`).
//!
//! Chrome is an ordinary page-hosted component: the router kernel activates it
//! like any other, and it learns everything it renders from port messages — the
//! layout channel and the five reserved `local:brenn/*` control planes. It is
//! the one instance holding `page-dom`, so it may reparent other components'
//! wrappers into its layout sections and stamp `data-theme`/`data-takeover`,
//! but it does that only in the page glue; the decision logic here is DOM-free
//! and host-tested.

/// The help sidecar's generator. Host-only: it depends on the contract and
/// schema crates, which are not available in the wasm32 build.
#[cfg(not(target_arch = "wasm32"))]
pub mod help;

/// The layout document chrome folds, and its validator.
pub mod layout;

pub mod logic;

/// The control-plane wire bodies chrome parses and publishes.
pub mod wire;

/// Port names and types from this component's specification.
pub mod spec;

#[cfg(target_arch = "wasm32")]
mod component;

/// The styling seam between the glue's marker attributes and the stylesheets
/// that select on them. Host-only, and tests alone.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod css_parity;
