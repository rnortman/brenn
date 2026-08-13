//! Presentation: turning conversation content, tool traffic, and stored files
//! into the HTML and the message shapes the client renders.
//!
//! Layered bottom-up, and that layering is the crate's whole justification for
//! existing as one unit:
//!
//! - [`markdown`] — the one markdown renderer, and the sanitizer around it.
//! - [`frontmatter`] — YAML frontmatter extraction and its rendered table.
//! - [`artifact`] / [`artifact_snapshot`] — artifact path resolution, and the
//!   content-addressed snapshots the history replay serves.
//! - [`cc_message_prefix`] / [`tools`] / [`approval_formatter`] — the outbound
//!   message prefix, the app-facing tool implementations, and the approval
//!   card each tool request renders as.
//! - [`system_message`] — system-category messages, built on the two above.
//! - [`history`] — conversation replay, built on all of them.
//!
//! Nothing here reaches upward: no router, no state, no bridge. The server's
//! routes and its bridge call in, never the other way round, so a route or
//! bridge edit leaves every test in this crate cached.

pub mod approval_formatter;
pub mod artifact;
pub mod artifact_snapshot;
pub mod cc_message_prefix;
pub mod frontmatter;
pub mod history;
pub mod markdown;
pub mod system_message;
pub mod tools;
