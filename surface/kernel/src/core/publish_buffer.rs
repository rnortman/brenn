//! The per-activation publish buffer — the kernel's half of flush-on-ok.
//!
//! One activation gets one [`PublishBuffer`]. The core seeds it at dispatch with
//! everything a verdict needs (the instance's output bindings, their resolved
//! sink budgets plus this activation's input grant, the body cap) and hands it to
//! the entry, which publishes into it synchronously. It is the **sole quota
//! authority for the duration of the handler**: every answer is inline, so the
//! entry never waits on the driver re-entering the core mid-call, and there is no
//! quota race to lose — the buffer is single-threaded by construction.
//!
//! Nothing here touches the router or the wire. Buffered entries go somewhere
//! only when the activation completes, and only if it returned ok; on err or trap
//! the whole buffer is discarded. That is the flush rule, and keeping the buffer
//! ignorant of both destinations is what makes it impossible to leak a publish
//! out of a failed activation.
//!
//! The check order below is the wasmtime host's
//! (`brenn_wasm::ProcessorHostData::do_publish`), deliberately: a component's
//! publish must be refused for the same reason on either hosting, and "same
//! reason" includes which check wins when two would fire.

use std::collections::HashMap;

use brenn_budget::{
    MAX_PUBLISH_BYTES_PER_ACTIVATION, MAX_PUBLISH_CALLS_PER_ACTIVATION,
    MAX_PUBLISHES_PER_ACTIVATION, MILLITOKENS_PER_PUBLISH,
};
use brenn_envelope::Urgency;
use brenn_surface_contract::PublishError;

/// One publish accepted into the buffer, in call order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferedPublish {
    /// The instance's own output port name.
    pub port: String,
    /// The channel it resolved to, captured at buffer time from the bindings the
    /// activation was seeded with. Captured rather than re-resolved at flush:
    /// the resolution that authorized the publish is the one that must route it,
    /// and a `Welcome` can land between the two.
    pub channel: String,
    pub body: String,
    /// The resolved urgency: the caller's override, else the port's configured
    /// default. Resolved here for `local:` entries (the router applies no default
    /// of its own); wire entries carry the raw override on the frame and let the
    /// server apply the default it owns — see [`PublishBuffer::take`].
    pub urgency: Urgency,
    /// Whether the caller stated an urgency. The wire frame carries the override
    /// or nothing, so the server's own resolved default keeps winning for a
    /// silent caller — a client echoing back an advertised default could
    /// override the operator with a stale one.
    pub urgency_override: Option<Urgency>,
}

/// One output binding as the buffer needs it: where the port goes, what it costs
/// to send there, and what the port defaults to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputSpec {
    pub channel: String,
    pub default_urgency: Urgency,
}

/// The per-activation publish buffer and its budgets.
///
/// Not `Copy`/`Default`: a buffer only ever exists seeded, for exactly one
/// activation, and a buffer nobody seeded would silently enforce nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBuffer {
    /// This instance's bound outputs, keyed by port. An unknown key is
    /// `not-permitted` — the component named a port its config does not give it.
    outputs: HashMap<String, OutputSpec>,
    /// Remaining millitokens per output port, seeded by `seed_sink_budget` and
    /// charged `MILLITOKENS_PER_PUBLISH` per accepted publish. Whatever survives
    /// returns to the core as the next activation's carryover.
    sink_mt: HashMap<String, u64>,
    /// The surface's publish-body cap, from `Welcome`. Applied to every class,
    /// `local:` included: a component's body-size contract must not change
    /// because an operator rebound its output port, and the cap is what bounds
    /// the router's rings, which are page memory.
    max_body_bytes: u64,
    /// Accepted publishes, in call order.
    entries: Vec<BufferedPublish>,
    /// Total accepted body bytes, against `MAX_PUBLISH_BYTES_PER_ACTIVATION`.
    published_bytes: usize,
    /// Every publish call this activation, accepted or refused, against
    /// `MAX_PUBLISH_CALLS_PER_ACTIVATION`. Refusals count: a rejected call is
    /// otherwise free to a component that loops on one, and free is what makes it
    /// a flood.
    call_count: usize,
}

impl PublishBuffer {
    /// Seed a buffer for one activation. `sink_mt` is already
    /// `seed_sink_budget`-folded by the caller (carry clamped, fill and input
    /// grant added) — this type spends budgets, it does not compute them.
    pub(crate) fn new(
        outputs: HashMap<String, OutputSpec>,
        sink_mt: HashMap<String, u64>,
        max_body_bytes: u64,
    ) -> Self {
        Self {
            outputs,
            sink_mt,
            max_body_bytes,
            entries: Vec::new(),
            published_bytes: 0,
            call_count: 0,
        }
    }

    /// Publish `body` from this instance's output `port`, at the port's
    /// configured default urgency. Buffered, not sent: it reaches the router or
    /// the wire only if this activation returns ok.
    pub fn publish(&mut self, port: &str, body: String) -> Result<(), PublishError> {
        self.publish_inner(port, body, None)
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message — the counterpart of the backend guest's
    /// `publish-with-urgency`.
    pub fn publish_with_urgency(
        &mut self,
        port: &str,
        body: String,
        urgency: Urgency,
    ) -> Result<(), PublishError> {
        self.publish_inner(port, body, Some(urgency))
    }

    /// The publish path both entry points share.
    ///
    /// Check order is the wasmtime host's, and each step's *reason* for its place
    /// is the same one:
    ///
    /// 1. **Call count**, first, so a component looping on rejections pays for
    ///    them. A refused call costs no bytes and no buffer slot, so without this
    ///    the rejection path is free.
    /// 2. **Port binding** — `not-permitted`.
    /// 3. **Body cap** — `invalid-payload`.
    /// 4. **Sink bucket** — the tightest, most specific gate.
    /// 5. **Per-activation caps** — the outer backstops, bounding the page's own
    ///    memory against a component whose buckets are generous.
    ///
    /// The bucket is charged only on acceptance, so a publish refused by a later
    /// check has not spent it.
    fn publish_inner(
        &mut self,
        port: &str,
        body: String,
        urgency_override: Option<Urgency>,
    ) -> Result<(), PublishError> {
        self.call_count += 1;
        if self.call_count > MAX_PUBLISH_CALLS_PER_ACTIVATION {
            return Err(PublishError::QuotaExceeded);
        }
        let Some(spec) = self.outputs.get(port) else {
            return Err(PublishError::NotPermitted);
        };
        if body.len() as u64 > self.max_body_bytes {
            return Err(PublishError::InvalidPayload);
        }
        // A bound port always has a seeded bucket — the core seeds one per entry
        // of the same `outputs` map this resolved against — so a miss is a broken
        // kernel invariant, not a component's problem.
        let remaining =
            self.sink_mt.get(port).copied().unwrap_or_else(|| {
                panic!("surface client: no sink budget for bound port {port:?}")
            });
        if remaining < MILLITOKENS_PER_PUBLISH {
            return Err(PublishError::QuotaExceeded);
        }
        if self.entries.len() >= MAX_PUBLISHES_PER_ACTIVATION {
            return Err(PublishError::QuotaExceeded);
        }
        if self.published_bytes + body.len() > MAX_PUBLISH_BYTES_PER_ACTIVATION {
            return Err(PublishError::QuotaExceeded);
        }
        let urgency = urgency_override.unwrap_or(spec.default_urgency);
        let channel = spec.channel.clone();
        self.published_bytes += body.len();
        self.entries.push(BufferedPublish {
            port: port.to_string(),
            channel,
            body,
            urgency,
            urgency_override,
        });
        *self
            .sink_mt
            .get_mut(port)
            .expect("surface client: sink budget entry vanished between check and charge") -=
            MILLITOKENS_PER_PUBLISH;
        Ok(())
    }

    /// How many publishes were accepted. Read by the driver to mint one envelope
    /// stamp per entry before it hands the buffer back to the core.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The buffered entries in call order and the leftover per-port millitokens,
    /// consuming the buffer. Called by the core on an ok completion; on err or
    /// trap the buffer is simply dropped, entries and all.
    ///
    /// Carryover returns even though the entries do not: what a component *spent*
    /// is a fact about the activation that happened, and an err does not un-spend
    /// it. The core clamps the carry to `capacity_mt` when it seeds the next
    /// activation.
    pub(crate) fn take(self) -> (Vec<BufferedPublish>, HashMap<String, u64>) {
        (self.entries, self.sink_mt)
    }

    /// The leftover per-port millitokens without the entries — the err/trap path,
    /// where the buffer is discarded but the spending still happened.
    pub(crate) fn into_carry(self) -> HashMap<String, u64> {
        self.sink_mt
    }
}
