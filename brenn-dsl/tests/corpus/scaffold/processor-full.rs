// Generated from processor-full.brenn — do not edit.

//! A specification exercising the whole generated processor surface: both port
//! directions, an `io` port, an optional port, doctypes, and every capability
//! word that names an SDK module.
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
    Tick,
}

impl InPort {
    /// Every inbound port, in the order the specification declares them.
    pub const ALL: [InPort; 3] = [InPort::Commands, InPort::Retries, InPort::Tick];

    /// The name this port is published and bound under.
    pub const fn name(self) -> &'static str {
        match self {
            InPort::Commands => "commands",
            InPort::Retries => "retries",
            InPort::Tick => "tick",
        }
    }

    /// The port a name spells, or nothing where it spells none.
    pub fn from_name(name: &str) -> Option<InPort> {
        match name {
            "commands" => Some(InPort::Commands),
            "retries" => Some(InPort::Retries),
            "tick" => Some(InPort::Tick),
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

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    /// Doctype: `brenn.scaffold.commands@1`.
    pub const COMMANDS: &str = "commands";
    pub const RETRIES: &str = "retries";
    /// Doctype: `brenn.scaffold.results@1`.
    pub const RESULTS: &str = "results";
    pub const TICK: &str = "tick";
}

// One re-export per capability the specification declares. Reaching a
// capability through this module is what makes deleting its word from the
// specification break the guest compile.
#[cfg(target_arch = "wasm32")]
pub use brenn_guest::alert;
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
