//! The attachment layer: the generic bus-attachment session and the seam an
//! application route supplies it.
//!
//! An *attacher* is anything that attaches to the bus over the websocket — the
//! browser surface today, a native daemon later. The attachment session knows
//! channels, cursors, and frames; it knows nothing about what the attacher does
//! with them. Everything an attachment needs that is not a transport fact —
//! which channels it may subscribe, which it may publish, what sub-identities it
//! may act as — reaches the session through [`profile::AttachProfile`], built at
//! boot by the route that owns the attacher.
//!
//! Layering: this crate sits below the HTTP server. It reaches down to
//! `brenn-attach-proto` for the wire frames, to `brenn-messaging` for the bus a
//! session publishes to and to `brenn-messaging-store` for the retention stores
//! it replays from, and to `brenn-lib`/`brenn-obs` for policy and alerting. It
//! names nothing above itself: the routes that own an attacher — the browser
//! surface and the remote — build a profile and hand it in.

pub mod cursor;
pub mod profile;
pub mod publish;
pub mod registry;
pub mod session;
pub mod socket;
pub mod subscription;

#[cfg(test)]
mod test_support;
