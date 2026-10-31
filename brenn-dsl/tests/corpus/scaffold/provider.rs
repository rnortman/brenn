// Generated from provider.brenn — do not edit.

//! A component that serves registry tools: the `Provided` enum, its request
//! classifier, and the `provides` re-export.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports, clippy::enum_variant_names)]

/// The ports the specification declares as inbound — `in` and `io`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPort {
    ToolResults,
    ToolRequests,
}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 2] = [InPort::ToolResults, InPort::ToolRequests];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {
            InPort::ToolResults => "tool-results",
            InPort::ToolRequests => "tool-requests",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<InPort> {
        match name {
            "tool-results" => Some(InPort::ToolResults),
            "tool-requests" => Some(InPort::ToolRequests),
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

/// The ports the specification declares `sync` — the causes this component
/// answers a call on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPort {}

impl SyncPort {
    /// Every sync port, in the order the specification declares them.
    pub const ALL: [SyncPort; 0] = [];

    /// The name this port is called on.
    pub const fn name(self) -> &'static str {
        match self {}
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(_name: &str) -> Option<SyncPort> {
        None
    }

    /// The declared sync port this activation was called on: `Ok(None)` on an
    /// asynchronous activation, `Ok(Some(_))` on a sync call to a declared
    /// port.
    ///
    /// Any other sync cause — a name the specification does not declare, and
    /// the reserved mount port, which no specification declares — fails the
    /// activation: the artifact is hash-bound to the specification that
    /// generated this module, so the host handed over a cause it could not
    /// have been configured to produce. A kind that mounts asks
    /// `sync_is(MOUNT)` before classifying, as it must build its view before
    /// it handles anything.
    #[cfg(target_arch = "wasm32")]
    pub fn of(
        activation: &brenn_guest::Activation,
    ) -> Result<Option<SyncPort>, brenn_guest::Error> {
        let Some(port) = activation.sync() else {
            return Ok(None);
        };
        match SyncPort::from_name(port) {
            Some(port) => Ok(Some(port)),
            None => Err(brenn_guest::Error::failed(format!(
                "sync call on port `{port}`, which this component does not declare",
            ))),
        }
    }
}

// The SDK takes a declared port and nothing else: `Activation::sync_is` is
// generic over this trait, and the only other implementor is the reserved
// mount cause.
#[cfg(target_arch = "wasm32")]
impl brenn_guest::SyncPortName for SyncPort {
    fn name(self) -> &'static str {
        self.name()
    }
}

// A gesture may be wired to a declared port and to nothing else:
// `dom::listen` is generic over this trait, which the mount cause does not
// implement, and this enum is its only implementor.
#[cfg(target_arch = "wasm32")]
impl brenn_guest::ListenPort for SyncPort {}

/// The tools the specification's `provide` statements declare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provided {
    EchoForUser,
    LookUp,
}

impl Provided {
    /// Every provided tool, in the order the specification declares them.
    pub const ALL: [Provided; 2] = [Provided::EchoForUser, Provided::LookUp];

    /// The name the registry knows this tool by.
    pub const fn name(self) -> &'static str {
        match self {
            Provided::EchoForUser => "echo-for-user",
            Provided::LookUp => "look-up",
        }
    }

    /// The provided tool a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<Provided> {
        match name {
            "echo-for-user" => Some(Provided::EchoForUser),
            "look-up" => Some(Provided::LookUp),
            _ => None,
        }
    }

    /// Classify a delivered request.
    ///
    /// The host delivers a request only for a tool this class declares, and
    /// the artifact is hash-bound to the specification that generated this
    /// module, so a tool name the specification does not declare is a host
    /// fault. The activation fails.
    #[cfg(target_arch = "wasm32")]
    pub fn of(
        request: &brenn_guest::provides::ToolRequest,
    ) -> Result<Provided, brenn_guest::Error> {
        Provided::from_name(&request.tool).ok_or_else(|| {
            brenn_guest::Error::failed(format!(
                "request for tool `{}`, which this component does not provide",
                request.tool
            ))
        })
    }
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    pub const TOOL_RESULTS: &str = "tool-results";
    pub const TOOL_REQUESTS: &str = "tool-requests";
}

// One re-export per capability the specification declares. Reaching a
// capability through this module is what makes deleting its word from the
// specification break the guest compile.
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::log;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::provides;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::tools;
