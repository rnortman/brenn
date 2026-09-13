// Generated from processor-full.brenn — do not edit.

//! A specification exercising the whole generated processor surface: both port
//! directions, an `io` port, the `io state` port a component's retained state
//! lives on, an optional port, the substrate-wired tool-result inbox its
//! `tools` requirement obliges, the two call directions, doctypes, and every
//! capability word that names an SDK module.
//!
//! The prose is carried into the generated module, so this paragraph is part of
//! what the golden pins.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports)]

#[cfg(target_arch = "wasm32")]
use brenn_guest::serde;

/// The ports the specification declares as inbound — `in` and `io`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InPort {
    /// Doctype: `brenn.scaffold.commands@1`.
    Commands,
    Retries,
    ToolResults,
    Tick,
    State,
}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 5] = [
        InPort::Commands,
        InPort::Retries,
        InPort::ToolResults,
        InPort::Tick,
        InPort::State,
    ];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {
            InPort::Commands => "commands",
            InPort::Retries => "retries",
            InPort::ToolResults => "tool-results",
            InPort::Tick => "tick",
            InPort::State => "state",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<InPort> {
        match name {
            "commands" => Some(InPort::Commands),
            "retries" => Some(InPort::Retries),
            "tool-results" => Some(InPort::ToolResults),
            "tick" => Some(InPort::Tick),
            "state" => Some(InPort::State),
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
pub enum SyncPort {
    Press,
    DismissAll,
}

impl SyncPort {
    /// Every sync port, in the order the specification declares them.
    pub const ALL: [SyncPort; 2] = [SyncPort::Press, SyncPort::DismissAll];

    /// The name this port is called on.
    pub const fn name(self) -> &'static str {
        match self {
            SyncPort::Press => "press",
            SyncPort::DismissAll => "dismiss-all",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<SyncPort> {
        match name {
            "press" => Some(SyncPort::Press),
            "dismiss-all" => Some(SyncPort::DismissAll),
            _ => None,
        }
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

/// The payload types this guest publishes on the `results` port. Bind a type to
/// the port once, as an impl:
/// `impl spec::ResultsPayload for Body<'_> {}`
#[cfg(target_arch = "wasm32")]
pub trait ResultsPayload: serde::Serialize {}

/// A typed publish handle for the `results` port, over any payload bound to it
/// by `ResultsPayload`. An owned payload binds through a `const`:
/// `const OUT: OutPort<Body> = spec::results();`
/// A borrowed payload cannot be named in one, so publish it inline:
/// `spec::results().publish(&body)?`.
///
/// Doctype: `brenn.scaffold.results@1`.
#[cfg(target_arch = "wasm32")]
pub const fn results<T: ResultsPayload>() -> brenn_guest::OutPort<T> {
    brenn_guest::OutPort::new("results")
}

/// The payload types this guest publishes on the `tick` port. Bind a type to
/// the port once, as an impl:
/// `impl spec::TickPayload for Body<'_> {}`
#[cfg(target_arch = "wasm32")]
pub trait TickPayload: serde::Serialize {}

/// A typed publish handle for the `tick` port, over any payload bound to it
/// by `TickPayload`. An owned payload binds through a `const`:
/// `const OUT: OutPort<Body> = spec::tick();`
/// A borrowed payload cannot be named in one, so publish it inline:
/// `spec::tick().publish(&body)?`.
#[cfg(target_arch = "wasm32")]
pub const fn tick<T: TickPayload>() -> brenn_guest::OutPort<T> {
    brenn_guest::OutPort::new("tick")
}

/// The payload types this guest publishes on the `state` port. Bind a type to
/// the port once, as an impl:
/// `impl spec::StatePayload for Body<'_> {}`
#[cfg(target_arch = "wasm32")]
pub trait StatePayload: serde::Serialize {}

/// A typed publish handle for the `state` port, over any payload bound to it
/// by `StatePayload`. An owned payload binds through a `const`:
/// `const OUT: OutPort<Body> = spec::state();`
/// A borrowed payload cannot be named in one, so publish it inline:
/// `spec::state().publish(&body)?`.
#[cfg(target_arch = "wasm32")]
pub const fn state<T: StatePayload>() -> brenn_guest::OutPort<T> {
    brenn_guest::OutPort::new("state")
}

/// A handle on the `ask` call port, wired by the document to one peer's
/// `sync` port: `spec::ask().call(&body)?`.
#[cfg(target_arch = "wasm32")]
pub const fn ask() -> brenn_guest::calls::CallPort {
    brenn_guest::calls::CallPort::new("ask")
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    /// Doctype: `brenn.scaffold.commands@1`.
    pub const COMMANDS: &str = "commands";
    pub const RETRIES: &str = "retries";
    pub const TOOL_RESULTS: &str = "tool-results";
    /// Doctype: `brenn.scaffold.results@1`.
    pub const RESULTS: &str = "results";
    pub const TICK: &str = "tick";
    pub const STATE: &str = "state";
    pub const PRESS: &str = "press";
    pub const DISMISS_ALL: &str = "dismiss-all";
    pub const ASK: &str = "ask";
}

// One re-export per capability the specification declares. Reaching a
// capability through this module is what makes deleting its word from the
// specification break the guest compile.
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::alert;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::calls;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::config;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::dom;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::log;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::mqtt;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::page_dom;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::store;
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::tools;
