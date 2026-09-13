// Generated from no-inbound.brenn — do not edit.

//! A component nothing publishes to: it is activated only for its own deferred
//! views, so its inbound enum is uninhabited and classifying a window always
//! fails.
//!
//! The whole port surface the specification states; a guest uses the part of it
//! that it needs.

#![allow(dead_code, unused_imports)]

#[cfg(target_arch = "wasm32")]
use brenn_guest::serde;

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

/// The payload types this guest publishes on the `beats` port. Bind a type to
/// the port once, as an impl:
/// `impl spec::BeatsPayload for Body<'_> {}`
#[cfg(target_arch = "wasm32")]
pub trait BeatsPayload: serde::Serialize {}

/// A typed publish handle for the `beats` port, over any payload bound to it
/// by `BeatsPayload`. An owned payload binds through a `const`:
/// `const OUT: OutPort<Body> = spec::beats();`
/// A borrowed payload cannot be named in one, so publish it inline:
/// `spec::beats().publish(&body)?`.
#[cfg(target_arch = "wasm32")]
pub const fn beats<T: BeatsPayload>() -> brenn_guest::OutPort<T> {
    brenn_guest::OutPort::new("beats")
}

/// The port names as text, for the parts of the SDK that take one.
pub mod port {
    pub const BEATS: &str = "beats";
}
