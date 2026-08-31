// Generated from dom-full.brenn — do not edit.

//! A dom specification with every direction and an optional grant: the lighter
//! emission, with no window classifier, no publish handles and no capability
//! re-exports.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports)]

/// The ports the specification declares as inbound — `in` and `io`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPort {
    /// Doctype: `brenn.scaffold.theme@1`.
    Theme,
    Overlay,
    SelfTick,
}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 3] = [InPort::Theme, InPort::Overlay, InPort::SelfTick];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {
            InPort::Theme => "theme",
            InPort::Overlay => "overlay",
            InPort::SelfTick => "self-tick",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<InPort> {
        match name {
            "theme" => Some(InPort::Theme),
            "overlay" => Some(InPort::Overlay),
            "self-tick" => Some(InPort::SelfTick),
            _ => None,
        }
    }
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    /// Doctype: `brenn.scaffold.theme@1`.
    pub const THEME: &str = "theme";
    pub const OVERLAY: &str = "overlay";
    /// Doctype: `brenn.scaffold.overlay-state@1`.
    pub const OVERLAY_STATE: &str = "overlay-state";
    pub const SELF_TICK: &str = "self-tick";
}
