// Generated from no-outbound.brenn — do not edit.

//! A component that publishes nothing: no publish handles, and the port-name
//! module carries its inbound names alone.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports)]

/// The ports the specification declares as inbound — `in` and `io`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPort {
    Reports,
}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 1] = [InPort::Reports];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {
            InPort::Reports => "reports",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<InPort> {
        match name {
            "reports" => Some(InPort::Reports),
            _ => None,
        }
    }

    /// Classify an activation window.
    ///
    /// A port the specification does not declare is not bad input: the
    /// artifact is hash-bound to the specification that generated this
    /// module, so an undeclared port means the host handed over a window it
    /// could not have been configured to produce. The activation fails.
    #[cfg(target_arch = "wasm32")]
    pub fn of(window: &brenn_guest::PortWindow) -> Result<InPort, brenn_guest::Error> {
        InPort::from_name(window.port()).ok_or_else(|| {
            brenn_guest::Error::failed(format!(
                "activation on port `{}`, which this component does not declare",
                window.port()
            ))
        })
    }
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    pub const REPORTS: &str = "reports";
}

// One re-export per capability the specification declares. Reaching a
// capability through this module is what makes deleting its word from the
// specification break the guest compile.
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::log;
