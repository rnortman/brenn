// Generated from dom-no-ports.brenn — do not edit.

//! A dom component with no ports at all: a legal class that talks to nothing,
//! whose emission is an uninhabited enum and an empty name module.
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
pub mod port {}
