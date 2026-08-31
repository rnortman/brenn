//! Brenn surface mode-clock component.
//!
//! Headless (mounted but never assigned a layout slot): it watches the browser
//! wall clock and drives the runtime dark/light theme axis by publishing a
//! `ThemeBody` on its `theme` output port, bound to the reserved
//! `local:brenn/theme` plane, which chrome writes to `data-theme` on `<body>`.
//!
//! Config arrives on the `config` port from a durable retained channel, so the
//! last-configured mode/schedule replays on reconnect. Three modes: `auto`
//! (day/night by a wall-clock schedule), fixed `dark`, and fixed `light`.
//!
//! Split into a DOM-free, host-tested state machine (`logic`) and a thin
//! `cfg(target_arch = "wasm32")` glue module. Boundary wakes are deferred
//! self-publishes on the `tick` in/out port, parked from inside the activation
//! that recomputed.

pub mod logic;

/// The help sidecar's generator, which the build runs on the host. It reads the
/// crates that define the theme axis, none of which the guest crate universe
/// carries, so it compiles where the generator runs and nowhere else.
#[cfg(not(target_arch = "wasm32"))]
pub mod help;

/// Port names and types from this component's specification.
pub mod spec;

#[cfg(target_arch = "wasm32")]
mod component;
