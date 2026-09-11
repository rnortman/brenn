//! One component, driven through each host's real scheduling path.
//!
//! `attach/conformance` is a whole attacher that is not a browser, so the
//! attachment protocol's claim of browser-neutrality is executable. This is the
//! same instrument one layer up: a whole *component* — the transplant probe —
//! put into service by each host's own machinery, so the claim that a component
//! cannot tell which host it got is executable for hosting *semantics* and not
//! only for the entry ABI.
//!
//! What the direct-drive transplant suites cannot see is exactly what this
//! crate exists for. They feed scripted activations straight to the component's
//! entry, which pins the ABI and the publish taxonomy and says nothing about
//! when a host activates, what it owes at mount, or what survives between two
//! activations. Every scenario here is one of those three.
//!
//! # Shape
//!
//! A [`Host`] is one adapter per hosting. Each [`scenarios`] submodule pairs a
//! [`MountSpec`] — the wiring the probe is mounted with — with a `run` that
//! drives the adapter and asserts. An adapter test is two lines: build the
//! adapter from the scenario's spec, hand it to the scenario's `run`.
//!
//! Waiting is the adapter's own business too: a host that can state "nothing
//! further will happen" answers [`Host::quiescent`] and [`settle`] believes it,
//! and one that can only be waited on leaves it `None` and pays the quiet
//! period. Same scenario bodies either way.
//!
//! The comparison is report-for-report, and a [`Report`] is deliberately a
//! projection rather than the probe's own JSON: message ids, wall-clock
//! instants and release times are host-minted and differ between two correct
//! hosts, so the projection keeps what the contract promises — window shape,
//! parked payloads, whether a clock was handed over, and the linear-memory
//! counter — and drops what it does not.

use std::time::Duration;

/// The probe's ports, as its specification names them.
pub mod port {
    /// Ordinary push-enabled input.
    pub const IN: &str = "in";
    /// Bound at `push_depth = 0`: context only, never a wake.
    pub const SAMPLED: &str = "sampled";
    /// The `io` port the probe's self-tick chain runs on.
    pub const TICK: &str = "tick";
    /// Echo markers and the deferral markers' target.
    pub const OUT: &str = "out";
    /// The activation report, one per activation.
    pub const REPORT: &str = "report";
}

/// How long a scenario waits for a report it expects. Generous on purpose: real
/// time with a wide bound cannot flake on scheduling jitter, it can only hang on
/// a real bug, which is the posture `attach/conformance` takes.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a scenario waits before concluding no *further* report is coming, on
/// a host that can only be waited on. A negative cannot be proven by waiting;
/// this buys confidence, and the positive half of each scenario is what pins the
/// behaviour. A host that can *state* quiescence answers [`Host::quiescent`]
/// instead and never pays it.
pub const QUIET_PERIOD: Duration = Duration::from_millis(300);

/// Polling granularity for both of the above.
const POLL: Duration = Duration::from_millis(10);

/// The wire class a channel is bound in. `Durable` survives a remount and
/// carries a parked message across it; `Ephemeral` does not.
///
/// Named by what it promises rather than by an address prefix, because the two
/// hosts spell it differently: the backend's is a `brenn:` channel backed by the
/// durable store, the surface's is a `local:` channel whose confined store
/// outlives any one registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Realm {
    Durable,
    Ephemeral,
}

/// One input port's binding. `push_depth == 0` is a sampled port: it holds no
/// position, is served its window as context, and never wakes its owner.
#[derive(Debug, Clone, Copy)]
pub struct InputBinding {
    pub port: &'static str,
    pub push_depth: u32,
    pub retain_depth: u32,
}

/// The wiring a probe instance is put into service with.
///
/// `out` and `report` are not optional in the probe's specification, so every
/// adapter binds them and the spec does not name them. `tick` is the `io` port
/// and is named here because its realm is what scenarios 3 and 8 vary.
#[derive(Debug, Clone)]
pub struct MountSpec {
    pub inputs: Vec<InputBinding>,
    /// Bind the `io tick` port in this realm; `None` leaves it unbound.
    pub tick: Option<Realm>,
    /// The probe's `tick_ms` config key. The probe re-arms its chain only when
    /// this is set, and only when `tick`'s deferred window is empty.
    pub tick_ms: Option<u64>,
}

/// What a host does with an instance that trapped. The one deliberate
/// host difference in error handling, stated in `brenn-activation`'s doctrine:
/// the backend quarantines the activation and carries on; the surface takes the
/// instance terminal, because DOM effects are immediate and non-transactional
/// and a half-built subtree is not repairable by a later activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrapDisposition {
    /// The activation is quarantined; the instance keeps being activated.
    Quarantine,
    /// The instance is terminal and is never activated again.
    Terminal,
}

/// One port's window as the report projects it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortReport {
    pub port: String,
    pub context_len: usize,
    pub new_len: usize,
    /// Messages this port's subscriber lost between two activations.
    ///
    /// No scenario here makes it nonzero — present in the comparison, not
    /// exercised.
    pub dropped: u32,
}

/// One output port's deferred window, projected to the payloads it holds in
/// release order. The release instants themselves are host clocks and are not
/// comparable between two correct hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredReport {
    pub port: String,
    pub payloads: Vec<String>,
    /// Per entry, whether its release instant had already passed when the
    /// activation ran — the entry's own `deliver_after` against the `now` the
    /// same report carries.
    ///
    /// The instants are host clocks and are dropped; their *relation* is
    /// contract. A window holds every entry no release pass has taken, so a
    /// `true` here is a due-but-unreleased entry, which both hosts must show
    /// and neither may let the component cancel.
    pub due: Vec<bool>,
}

/// One activation as the probe reported it, projected to what the contract
/// promises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub ports: Vec<PortReport>,
    pub deferred: Vec<DeferredReport>,
    /// Whether the host handed over a wall clock. The instant is host-minted;
    /// that there is one is contract.
    pub now_set: bool,
    /// Activations this instance's linear memory has seen. `1` on a conforming
    /// host, always.
    pub counter: u32,
}

impl Report {
    /// Project one report body the probe published on its `report` port.
    ///
    /// # Panics
    ///
    /// On a body that is not the probe's report shape. The probe is hash-bound
    /// to the specification these adapters wire, so a body that does not parse
    /// is the probe or the host having changed under this crate, never input.
    pub fn parse(body: &str) -> Self {
        let value: serde_json::Value =
            serde_json::from_str(body).unwrap_or_else(|e| panic!("probe report {body:?}: {e}"));
        let ports = value["ports"]
            .as_array()
            .expect("report names a ports array")
            .iter()
            .map(|p| {
                let ids = p["ids"].as_array().expect("port window names ids").len();
                let context_len = p["new_from"].as_u64().expect("new_from") as usize;
                PortReport {
                    port: p["port"].as_str().expect("port name").to_string(),
                    context_len,
                    new_len: ids - context_len,
                    dropped: p["dropped"].as_u64().expect("dropped") as u32,
                }
            })
            .collect();
        let deferred = value["deferred"]
            .as_array()
            .expect("report names a deferred array")
            .iter()
            .map(|w| {
                let entries = w["entries"].as_array().expect("deferred entries");
                DeferredReport {
                    port: w["port"]
                        .as_str()
                        .expect("deferred window port")
                        .to_string(),
                    payloads: entries
                        .iter()
                        .map(|e| e["payload"].as_str().expect("entry payload").to_string())
                        .collect(),
                    due: entries
                        .iter()
                        .map(|e| {
                            let at = e["deliver_after"].as_u64().expect("entry deliver_after");
                            value["now"].as_u64().is_some_and(|now| at <= now)
                        })
                        .collect(),
                }
            })
            .collect();
        Report {
            ports,
            deferred,
            now_set: !value["now"].is_null(),
            counter: value["counter"].as_u64().expect("counter") as u32,
        }
    }

    /// The named port's window, or `None` if the host did not present it.
    pub fn port(&self, name: &str) -> Option<&PortReport> {
        self.ports.iter().find(|p| p.port == name)
    }

    /// The named output port's deferred window, or `None` if absent.
    pub fn deferred(&self, name: &str) -> Option<&DeferredReport> {
        self.deferred.iter().find(|d| d.port == name)
    }
}

/// One hosting, driven through its own scheduling path.
///
/// An adapter is built from a scenario's [`MountSpec`] — the channels and
/// positions exist from construction, which is what lets a scenario publish
/// history *before* the probe is put into service — and [`Host::mount`] is the
/// act of putting it into service.
#[allow(async_fn_in_trait)]
pub trait Host {
    /// This host's stated disposition for a trapped instance.
    fn trap_disposition(&self) -> TrapDisposition;

    /// Put the probe instance into service. On the backend that is the consumer
    /// task starting; on the surface it is registration under a bindings
    /// document.
    async fn mount(&mut self);

    /// An external publish onto the channel the named port is bound to.
    async fn publish(&mut self, port: &str, body: &str);

    /// Take the instance out of service and put it back, over the same
    /// positions. A process restart on the backend, a page reload on the
    /// surface.
    async fn remount(&mut self) {
        self.remount_after(Duration::ZERO).await;
    }

    /// The same, with `idle` spent out of service and **no release pass run**
    /// across it. A parked message whose instant falls inside that span comes
    /// due while nobody is there to release it, which is the state scenario 9
    /// is about.
    ///
    /// Separate from [`Host::drain`] for that reason: draining is what releases
    /// on both adapters, so a scenario that slept by draining would release the
    /// very entry it is asking about.
    async fn remount_after(&mut self, idle: Duration);

    /// Every report the probe has published since the last drain, in order.
    /// Does not wait: [`settle`] and [`await_reports`] do the waiting.
    ///
    /// Draining is also where each adapter runs the host's release pass — the
    /// dispatcher's on the backend, the driver's release deadline on the page —
    /// unless [`Host::hold_releases`] is set.
    async fn drain(&mut self) -> Vec<Report>;

    /// Stop running the release pass inside [`Host::drain`], so a scenario can
    /// read reports while a due message stays unreleased.
    ///
    /// A component's deferred window holds every entry no release pass has
    /// taken, so the only way to observe a due one is to keep reading reports
    /// without releasing. Without this a scenario's own drain would take the
    /// entry it is asking about.
    fn hold_releases(&mut self, hold: bool);

    /// Whether this host can state, as of the last [`Host::drain`], that nothing
    /// further will happen without an external publish — `None` from a host that
    /// cannot.
    ///
    /// [`settle`] returns the moment a host says `true` and falls back to
    /// [`QUIET_PERIOD`] otherwise, which keeps one scenario body and pays the
    /// clock only where a deterministic answer is not available.
    fn quiescent(&self) -> Option<bool> {
        None
    }
}

/// Run until `want` reports have arrived and nothing more is ready, and return
/// everything that accumulated.
///
/// `want` is what separates the two halves of a scenario's question. A *positive*
/// expectation ("the mount activation happened") waits for its reports under
/// [`WAIT_TIMEOUT`], because the quiet period alone is too short a budget on a
/// loaded runner. A *negative* expectation ("no further activation") passes
/// `want = 0` and buys [`QUIET_PERIOD`], which is the only way to wait on
/// something not happening.
///
/// Extras are still caught either way: once `want` have arrived the quiet period
/// still has to pass with nothing new before this returns, so a host that
/// activates twice where one was expected is reported by the caller's own count.
///
/// A host with a live self-tick chain never goes quiet, which is what
/// [`await_reports`] is for.
pub async fn settle<H: Host>(host: &mut H, want: usize) -> Vec<Report> {
    let mut collected = Vec::new();
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    let mut quiet_since = std::time::Instant::now();
    loop {
        let batch = host.drain().await;
        if !batch.is_empty() {
            collected.extend(batch);
            quiet_since = std::time::Instant::now();
        }
        // A host that can state quiescence is believed at once: waiting longer
        // cannot change an answer the host already knows, whether or not `want`
        // has been reached — a short count is then the caller's failure to
        // report, with its own message.
        if host.quiescent() == Some(true) {
            return collected;
        }
        if std::time::Instant::now() >= deadline {
            return collected;
        }
        if collected.len() >= want && quiet_since.elapsed() >= QUIET_PERIOD {
            return collected;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Wait until `want` reports have accumulated since the last drain, and return
/// them.
///
/// # Panics
///
/// If fewer than `want` arrive inside [`WAIT_TIMEOUT`]. A scenario that expects
/// an activation and does not get one is the failure this crate exists to
/// report, so it fails here rather than returning short.
pub async fn await_reports<H: Host>(host: &mut H, want: usize) -> Vec<Report> {
    let mut collected = Vec::new();
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    while collected.len() < want {
        collected.extend(host.drain().await);
        if collected.len() >= want {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "waited {WAIT_TIMEOUT:?} for {want} report(s), saw {}",
            collected.len(),
        );
        tokio::time::sleep(POLL).await;
    }
    collected
}

/// Assert that every report came from a linear memory that had seen nothing
/// before it. Linear memory is activation-scoped on every host, checked
/// wherever reports are read.
pub fn assert_fresh_memory(reports: &[Report]) {
    for (i, report) in reports.iter().enumerate() {
        assert_eq!(
            report.counter, 1,
            "report {i} came from an instance on its activation {}; linear memory \
             is activation-scoped on every host",
            report.counter,
        );
    }
}

pub mod scenarios;

// The adapters. `cfg(test)` and not a separate target: the library's own
// dependency list is what keeps a host crate out of the scenarios, and this
// module is compiled only into the test binary, where every host crate is named.
#[cfg(test)]
mod tests;
