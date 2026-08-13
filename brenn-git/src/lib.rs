//! Git subprocess plumbing for managed repos.
//!
//! Bottom-up:
//!
//! - [`subprocess`] — the one place `git` is spawned as a child process:
//!   timeout, bounded reads, strict-UTF-8 stdout, log-line sanitization.
//! - [`ops`] — the LLM-facing operations (status, commit-and-push, run) over a
//!   configured mount.
//! - [`pull`] — the host-side fast-forward pull of a managed clone and the
//!   classification of its outcome.
//! - [`repo_clone`] — clone-target selection and startup auto-cloning.
//! - [`sync`] — the clone index and the trigger channel the server's repo-sync
//!   manager consumes.
//!
//! Nothing here reaches into the server: the crate's whole upward surface is
//! `brenn_lib` (config types, the oneline cap) plus the alert dispatcher, so a
//! server edit leaves its tests cached.

pub mod ops;
pub mod pull;
pub mod repo_clone;
pub mod subprocess;
pub mod sync;
