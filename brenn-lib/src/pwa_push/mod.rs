//! PWA push configuration and VAPID key material.
//!
//! Contains the `[pwa_push]` config blocks, the resolved config
//! `ResolvedConfig` carries, and the VAPID keypair
//! `resolve_pwa_push_layer` loads or generates at startup.

pub mod config;
pub mod vapid;
