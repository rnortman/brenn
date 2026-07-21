//! `brenn-surface-kernel` — the Brenn surface kernel.
//!
//! The kernel owns a browser surface end to end. Its protocol half is a
//! sans-I/O core (a pure, synchronous state machine) driven by a small async
//! loop generic over a transport trait; the same core and driver compile to
//! `wasm32` for the browser and to native for tests, with only the transport
//! and timer shim `cfg`-gated. A correct kernel structurally cannot commit a
//! surface protocol violation: every rule the server enforces is made
//! unrepresentable or pre-validated here.
//!
//! Its platform half connects the [`ClientHandle`], processes the resolved
//! `Welcome` bindings, mounts the configured component elements, routes
//! delivered envelopes and component publish intents, publishes the reserved
//! control planes (link-state, surface-state), generates the surface
//! self-description telemetry, and renders the pre-chrome connect indicator and
//! per-component error cards. It is split for testability: [`logic`] is a
//! DOM-free decision core (host-compiled, natively unit-tested); [`dom`] is the
//! web-sys effect executor; [`entry`] holds the wasm-bindgen exports and wiring.

mod core;
mod driver;
mod handle;
// Native-only test scaffolding: the protocol-core conformance and driver suites
// run under host `cargo test`; wasm builds (browser bundle + the dom/entry
// wasm-bindgen-test suites) never pull it.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_support;
pub mod transport;

/// DOM-free platform decision core; host-compiled and natively unit-tested.
pub mod logic;

/// web-sys effect executor; browser target only.
#[cfg(target_arch = "wasm32")]
pub mod dom;

/// wasm-bindgen entry point and kernel handle; browser target only.
#[cfg(target_arch = "wasm32")]
mod entry;

#[cfg(target_arch = "wasm32")]
pub use entry::{KernelHandle, start};

/// Shared helpers for the browser-level wasm-bindgen test suites in `dom` and
/// `entry`. Test-only, browser target only.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_test_util;

pub use core::{
    ActivationOutcome, ClientCore, Command, CoreConfig, DisconnectReason, Effect, Event, Input,
    Millis, PublishBuffer, PublishStatus, ReadyActivation,
};
pub use driver::Driver;
#[cfg(target_arch = "wasm32")]
pub use handle::InFlightPublish;
pub use handle::{
    ActivationEntry, ClientConfig, ClientHandle, EventStream, PublishGate, PublishReject, new,
};
pub use transport::{TransportConnection, TransportConnector, TransportError, TransportEvent};

#[cfg(not(target_arch = "wasm32"))]
pub use transport::native::{NativeConnection, NativeConnector, insert_session_cookie};

// Signature types of `insert_session_cookie`, re-exported so out-of-tree native
// kernels can name them without guessing this crate's tungstenite pin. The
// helper's doc comment states the semver coupling to that pin.
#[cfg(not(target_arch = "wasm32"))]
pub use tokio_tungstenite::tungstenite::http::{HeaderMap, header::InvalidHeaderValue};

#[cfg(target_arch = "wasm32")]
pub use transport::websys::{WebSysConnection, WebSysConnector};

// Wire protocol types are owned by the shared proto crate; re-export it so
// callers of this crate speak the same vocabulary without a second dependency.
pub use brenn_envelope::{MessageEnvelope, Urgency};
pub use brenn_surface_proto as proto;

/// The component contract — the DOM-event seam the kernel and every component
/// module compile against. Re-exported for the same reason as [`proto`]: a
/// consumer of this crate is already on the seam and should not restate the
/// dependency to name it.
pub use brenn_surface_contract as contract;
