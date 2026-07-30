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

pub mod profile;
