// Generated from dom-no-inbound.brenn — do not edit.

//! A dom component nothing publishes to: its inbound enum is uninhabited, and
//! the name lookup answers no name. Nothing in the tree has this shape, so the
//! compile gate beside the golden is the only thing that holds it valid.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports)]

/// The ports the specification declares as inbound — `in` and `io`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPort {}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 0] = [];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {}
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(_name: &str) -> Option<InPort> {
        None
    }
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    /// Doctype: `brenn.scaffold.state@1`.
    pub const STATE: &str = "state";
}
