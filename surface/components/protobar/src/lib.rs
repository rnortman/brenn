//! Brenn surface protobar component.
//!
//! Receive-only component (contract v0): subscribes to one ephemeral channel,
//! renders the latest message body as text, shows drop/gap indicators.
//!
//! Split into a DOM-free, host-tested state machine (`logic`) and a thin
//! `cfg(target_arch = "wasm32")` DOM glue module.

pub mod logic;
pub mod markdown;

#[cfg(target_arch = "wasm32")]
mod component;
