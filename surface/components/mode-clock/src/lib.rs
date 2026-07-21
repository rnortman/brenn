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
//! `cfg(target_arch = "wasm32")` DOM/timer glue module.

pub mod logic;

#[cfg(target_arch = "wasm32")]
mod component;
