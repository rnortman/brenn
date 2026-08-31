//! Brenn surface meeting component.
//!
//! Shows time-to-next-meeting and escalates through an ambient → takeover →
//! critical → overdue ladder, computing every threshold locally from the wall
//! clock (reboot-safe by construction — no stored escalation state). A
//! personal-assistant agent publishes a full upcoming-meetings snapshot to a
//! durable retained channel (`agenda` port, latest-wins); the component
//! publishes dismiss/snooze acks to a second durable channel (`acks` port,
//! subscribe **and** publish) so every device converges. At the takeover
//! threshold it publishes a takeover request on its `takeover` output port
//! (bound to `local:brenn/takeover`); chrome (on a takeover-granted surface)
//! pushes a fullscreen overlay.
//!
//! Split into a DOM-free, host-tested state machine (`logic`) and a thin
//! `cfg(target_arch = "wasm32")` page glue module.

/// The help sidecar's generator. Host-only: it depends on the contract and
/// schema crates, which are not available in the wasm32 build.
#[cfg(not(target_arch = "wasm32"))]
pub mod help;

pub mod logic;

/// Port names and types from this component's specification.
pub mod spec;

#[cfg(target_arch = "wasm32")]
mod component;

/// The styling seam between the glue's marker attributes and the stylesheets
/// that select on them. Host-only, and tests alone.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod css_parity;
