//! The in-tree default chrome component (`brenn-chrome`).
//!
//! Chrome is an ordinary contract-v1 `dom` component: the router kernel mounts
//! it like any other, and it learns everything it renders from port messages —
//! the layout channel and the five reserved `local:brenn/*` control planes. It
//! holds the page-DOM-authority grant so it may reparent other
//! components' wrappers into its layout sections and stamp `data-theme`/
//! `data-takeover`, but it does that only in the wasm DOM half; the decision
//! logic here is DOM-free and host-tested.

pub mod logic;

#[cfg(target_arch = "wasm32")]
mod component;
