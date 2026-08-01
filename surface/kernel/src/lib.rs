//! `brenn-surface-kernel` — the Brenn surface kernel.
//!
//! The kernel owns a browser surface end to end. It is an *application* built on
//! a bus attachment: `brenn-attach-client` holds the attacher-generic half — the
//! transport, the connection lifecycle, subscription and cursor state, the ring
//! stores, the publish and batch machinery, the confined router — and this crate
//! holds everything that is about components, DOM and pixels. Nothing here is a
//! second copy of that machinery; the seams it exposes are parameterised with
//! surface policy instead.
//!
//! What the surface layer is, in the order a turn passes through it: the wiring
//! a [`bindings`] document puts in force over a [`page`], the [`connect`]
//! sequence's two phases, [`registry`] reconciles, [`inbound`] frames, an
//! [`activation`]'s assembly and its [`flush`], the [`outward`] passes a page
//! drives itself, and the [`command`]s the platform half asks for. [`turn`]
//! routes one input into one ordered effect list, [`session`] is the vocabulary
//! those effects and events are spoken in, [`runner`] is the loop that performs
//! them, and [`front`] is the door the platform half holds.
//!
//! That platform half applies the wiring, mounts the configured component
//! elements, routes delivered envelopes and component publish intents, publishes
//! the reserved control planes (link-state, surface-state), writes the surface's
//! own geometry and status documents, and renders the pre-chrome connect
//! indicator and per-component error cards. It is split for testability:
//! [`logic`] is a DOM-free decision core (host-compiled, natively unit-tested);
//! [`dom`] is the web-sys effect executor; [`entry`] holds the wasm-bindgen
//! exports and wiring.

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

/// What the platform half asks of a page: a component's publish, a report, an
/// alert, a control plane, the two telemetry documents, and the close.
pub mod command;

/// The vocabulary the platform half speaks, and the fold that turns every pass's
/// answer into it.
pub mod session;

/// What reaches the page from outside — the connection, the peer, the deadlines,
/// the platform half — and the table that routes one of them into a whole turn.
pub mod turn;

/// The loop that joins a page to an attachment: the layer that waits, reads the
/// clocks, and performs what a turn asks for.
pub mod runner;

/// The front door: the handle the platform half holds, the channels it asks
/// through, and the snapshot that refuses a doomed publish on its own stack.
pub mod front;

/// The per-activation publish buffer: the sole quota authority for the duration
/// of a component's handler.
pub mod publish_buffer;

// The backoff-jitter seed source, crate-private: see the module doc for why it
// is not part of the attach client's shim set. Its one production caller is the
// wasm entry; the native arm exists for the native suite that pins distinctness.
#[cfg(any(target_arch = "wasm32", test))]
mod entropy;
// Test scaffolding shared across suites: the bindings-document builders, the
// page a document is put in force over, and the server frames a scripted peer
// writes.
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

pub use activation::{ActivationOutcome, ReadyActivation};
#[cfg(target_arch = "wasm32")]
pub use front::InFlightPublish;
pub use front::{ActivationEntry, EventStream, PublishReject, SurfaceGate, SurfaceHandle, new};
pub use outbound::PublishStatus;
pub use publish_buffer::PublishBuffer;
pub use runner::SurfaceRunner;
pub use session::{Effect, Event};
// The monotonic timestamp every turn is stamped with. Owned by the attach client
// (the shim that reads the per-target clock produces it); re-exported so this
// crate's callers name one type.
pub use brenn_attach_client::Millis;
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

// Two vocabularies, two re-exports, because they are two contracts: `proto` is
// the attachment protocol's frames, which name nothing about a surface, and
// `schema` is the surface application payloads that ride it. Re-exported so
// callers of this crate speak both without restating either dependency.
pub use brenn_attach_proto as proto;
pub use brenn_envelope::{MessageEnvelope, Urgency};
pub use brenn_surface_schema as schema;

/// The component contract — the DOM-event seam the kernel and every component
/// module compile against. Re-exported for the same reason as [`proto`]: a
/// consumer of this crate is already on the seam and should not restate the
/// dependency to name it.
pub use brenn_surface_contract as contract;
