//! The `remote` route: an authenticated native daemon attached to the bus.
//!
//! The second application route on the attachment stack, beside `surface`. It
//! shares the wire contract, the client planes, and the whole server session
//! with the browser route and parts from it in the four places a non-browser
//! attacher has to: a bearer token instead of a session cookie, an authority
//! lowering from `[[remote]]` instead of component bindings, its own session-cap
//! posture, and no deployment coupling to served assets — a daemon has no build
//! id to agree with.
//!
//! Nothing here is rendering-shaped. A remote has no components, no instances,
//! no geometry, and no chrome; it is one principal, `remote:<slug>`, holding
//! exactly the channels an operator wrote.

// No HTTP route registers this module yet: the profile is the authority half
// and lands ahead of the handler that authenticates a connection into it.
#![allow(dead_code)]

pub mod profile;
