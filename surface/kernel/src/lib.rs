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

/// The surface's wiring: the bindings document parsed, checked against this
/// build's limits, and indexed for the surface layer's lookups.
pub mod bindings;

/// The surface's two-phase connect: the attachment's transport facts, the
/// config channel that carries its wiring, and when each becomes usable.
pub mod connect;

/// The surface's registration table and the reconcile passes that put a
/// bindings document in force over the page's stores and subscriptions.
pub mod registry;

/// The surface's rules for its own confined planes: who writes where, what a
/// plane makes of a body, and the overlay holdership the kernel remembers.
pub mod planes;

/// What the surface writes: port publishes addressed onto channels, the
/// kernel's own documents, and the per-instance flush outboxes.
pub mod outbound;

/// The geometry and status documents the surface publishes about itself.
pub mod telemetry;

/// One activation's assembly — input windows, deferred snapshots, the publish
/// buffer — and the per-instance scheduler state around it.
pub mod activation;

/// What one activation's completion writes: the buffer committed to both channel
/// classes, discarded on failure, or the instance taken terminal.
pub mod flush;

/// Everything one page holds, and the two phases of an attachment over it.
pub mod page;

/// The inbound half of a turn: the server frames the connection hands back,
/// routed into the page's tables.
pub mod inbound;

/// The outward half of a turn: activation dispatch and completion, the confined
/// release pass, the outbox retry, and what the page says about a loss.
pub mod outward;

/// The vocabulary the platform half speaks, and the fold that turns every pass's
/// answer into it.
pub mod session;

/// The per-activation publish buffer: the sole quota authority for the duration
/// of a component's handler.
pub(crate) mod publish_buffer;

mod core;
mod driver;
// The backoff-jitter seed source, crate-private: see the module doc for why it
// is not part of the attach client's shim set.
mod entropy;
mod handle;
// Test scaffolding shared across suites: the bindings-document builders on every
// target, and the native-only `CoreConfig`/`Welcome` fixtures the protocol-core
// conformance and driver suites run under host `cargo test` with.
#[cfg(test)]
mod test_support;

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
// The transport seam and its per-target implementations live in
// `brenn-attach-client` — they are attacher-generic, naming nothing about
// components, DOM, or pixels. Re-exported here so out-of-tree native kernels
// keep naming them through this crate rather than restating the dependency.
pub use brenn_attach_client::transport;
pub use brenn_attach_client::{
    TransportConnection, TransportConnector, TransportError, TransportEvent,
};

#[cfg(not(target_arch = "wasm32"))]
pub use brenn_attach_client::{NativeConnection, NativeConnector, insert_session_cookie};

// Signature types of `insert_session_cookie`, re-exported so out-of-tree native
// kernels can name them without guessing the tungstenite pin. The helper's doc
// comment states the semver coupling to that pin.
#[cfg(not(target_arch = "wasm32"))]
pub use brenn_attach_client::{HeaderMap, InvalidHeaderValue};

#[cfg(target_arch = "wasm32")]
pub use brenn_attach_client::{WebSysConnection, WebSysConnector};

// Wire protocol types are owned by the shared proto crate; re-export it so
// callers of this crate speak the same vocabulary without a second dependency.
pub use brenn_envelope::{MessageEnvelope, Urgency};
pub use brenn_surface_schema as proto;

/// The component contract — the DOM-event seam the kernel and every component
/// module compile against. Re-exported for the same reason as [`proto`]: a
/// consumer of this crate is already on the seam and should not restate the
/// dependency to name it.
pub use brenn_surface_contract as contract;
