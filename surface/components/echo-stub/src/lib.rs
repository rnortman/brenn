//! `brenn-echo-stub` — the minimal surface component.
//!
//! A deliberately tiny dev/test fixture, not a product: the first consumer of
//! component contract v0 ([`brenn_surface_contract`]) and of the shell's
//! multi-module loading path, so that path is exercised before `brenn-protobar`
//! exists. Its element is `brenn-echo-stub` (kind `echo-stub`).
//!
//! Behavior (all browser-only): it renders each `brenn-port-message`'s
//! `envelope_json` as text into a scrollback list, shows running drop and gap
//! counters from `brenn-port-drops`/`brenn-port-gap`, starts in an "awaiting
//! data" state, offers a "send" button that dispatches `brenn-port-publish` on
//! its host element (the contract's dispatch-origin rule) with a fixed counter
//! body, a free-form field plus a "send custom" button that publishes the
//! field's value verbatim (the path a test drives to publish a structured or
//! markdown body), and a "panic" button that panics — the latter exercising the
//! module panic hook → `brenn-component-panic` → shell error-card plumbing from
//! a real component.
//!
//! Everything lives behind `cfg(target_arch = "wasm32")`: the component is DOM-
//! bound, so the host build is empty and the wasm build carries the whole
//! module.

/// The browser-only component: custom-element registration, DOM rendering,
/// contract-event listeners, publish/panic buttons, and the module panic hook.
#[cfg(target_arch = "wasm32")]
mod component;
