// Host-side wasmtime integration for the replay-check and processor WASM components.
//
// The bindgen! macro generates typed Rust bindings from the WIT world at
// compile time. The generated code may trigger clippy lints; any specific
// lints that fire at the pinned toolchain (1.95.0) + wasmtime 45 are
// suppressed with targeted #[allow(...)]. Blanket -A clippy::all is not used.
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::{debug, error, info, trace, warn};

use wasmtime::component::{Component, HasSelf, Linker, Resource, ResourceTable, bindgen};
use wasmtime::{Config, Engine, EngineWeak, Store, StoreLimits, StoreLimitsBuilder};

pub mod store;
pub use store::KvStore;
use store::Transaction;

// Generates types in this module: CheckInput, Header, ReplayError (from
// world-level `use types.{...}`), plus Replay, ReplayPre in this module.
// The store interface generates Host + HostTransaction traits we implement
// on StoreData, plus the add_to_linker function.
//
// with = { "brenn:replay/store/transaction": crate::store::Transaction }
// tells bindgen to use our Transaction type for Resource<Transaction> handles
// rather than the default generated opaque placeholder enum.
bindgen!({
    world: "replay",
    path: "wit/replay.wit",
    with: {
        "brenn:replay/store.transaction": crate::store::Transaction,
    },
});

// bindgen! generates CheckInput and ReplayError at this module's scope via
// the world-level `use types.{check-input, replay-error}` clause. Header is
// only in brenn::replay::types so must be re-exported explicitly.
pub use brenn::replay::store::StoreError;
pub use brenn::replay::types::Header;

use brenn_common::sanitize_untrusted_str;

/// Grace period added on top of SKEW_WINDOW_MS when computing the `last` namespace prune cutoff.
///
/// Exported so integration tests can compute the same cutoff boundary without duplicating the
/// constant. The component defines its own copy in-crate (WASM cannot import from this crate);
/// if the component's value ever changes, update both. See design §2.2.
pub const LAST_GRACE_MS: i64 = 60 * 1000; // 60 s

/// Amortization factor for the `last` namespace prune scan.
///
/// Prune runs when `received_at_ms.rem_euclid(PRUNE_GATE_MODULUS) == 0`, i.e. on approximately
/// 1/PRUNE_GATE_MODULUS of accepted checks. Exported for integration tests. The component defines
/// its own copy in-crate; if the value changes, update both.
pub const PRUNE_GATE_MODULUS: i64 = 64;

/// Store data for KV-store host support.
struct StoreData {
    resource_table: ResourceTable,
    kv_store: Arc<KvStore>,
    config: Arc<HashMap<String, String>>,
    /// Per-store resource limits (memory/table/instance caps). Installed via
    /// `store.limiter(...)` in `make_store` (H1046, design §2.4).
    limits: StoreLimits,
}

// --- Host impl for the `store` interface ---
//
// The bindgen-generated Host trait returns Result<T, StoreError> directly (not
// wrapped in wasmtime::Result). The wasmtime machinery wraps the return in
// wasmtime::Result at the call site; panics from our impl become traps.

impl brenn::replay::store::Host for StoreData {
    fn begin(&mut self) -> Result<Resource<Transaction>, StoreError> {
        store::host_begin(&mut self.resource_table, &self.kv_store)
    }
}

impl brenn::replay::store::HostTransaction for StoreData {
    fn get(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let kv = Arc::clone(
            &self
                .resource_table
                .get(&tx)
                .unwrap_or_else(|e| panic!("Transaction resource invalid in get: {e}"))
                .kv_store,
        );
        store::tx_get(&kv, &namespace, &key)
    }

    fn put(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), StoreError> {
        let kv = Arc::clone(
            &self
                .resource_table
                .get(&tx)
                .unwrap_or_else(|e| panic!("Transaction resource invalid in put: {e}"))
                .kv_store,
        );
        store::tx_put(&kv, &namespace, &key, &value)
    }

    fn delete(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<(), StoreError> {
        let kv = Arc::clone(
            &self
                .resource_table
                .get(&tx)
                .unwrap_or_else(|e| panic!("Transaction resource invalid in delete: {e}"))
                .kv_store,
        );
        store::tx_delete(&kv, &namespace, &key)
    }

    fn scan(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<store::KvPairs, StoreError> {
        let kv = Arc::clone(
            &self
                .resource_table
                .get(&tx)
                .unwrap_or_else(|e| panic!("Transaction resource invalid in scan: {e}"))
                .kv_store,
        );
        store::tx_scan(&kv, &namespace, &start, end.as_deref(), limit)
    }

    fn commit(&mut self, tx: Resource<Transaction>) -> Result<(), StoreError> {
        // The replay endpoint slug exists on `ReplayComponent` (H1046) but is not threaded
        // into `StoreData`, so pass "" for log context here. Wiring the slug through
        // `host_commit`/`host_rollback` was left out of scope by H1046 (design §2.7-E);
        // it is a small follow-up if a `slug=""` commit/rollback log ever needs disambiguation.
        store::host_commit(&mut self.resource_table, &tx, "")
    }

    fn rollback(&mut self, tx: Resource<Transaction>) {
        // See `commit`: replay slug exists on the component but is not in `StoreData`, so
        // pass "" for log context (H1046 design §2.7-E left the threading out of scope).
        store::host_rollback(&mut self.resource_table, &tx, "")
    }

    fn drop(&mut self, tx: Resource<Transaction>) -> wasmtime::Result<()> {
        store::host_drop(&mut self.resource_table, tx)
    }
}

// --- config::Host impl (replay world) ---

impl brenn::replay::config::Host for StoreData {
    fn get(&mut self, key: String) -> Option<String> {
        self.config.get(&key).cloned()
    }
}

/// Loaded, pre-linked replay-check WASM component.
///
/// `ReplayPre<StoreData>` is built once in `load`; per-call cost is
/// instantiation + store-build + the single WIT call (no per-call linker pass).
pub struct ReplayComponent {
    engine: Engine,
    replay_pre: ReplayPre<StoreData>,
    kv_store: Arc<KvStore>,
    config: Arc<HashMap<String, String>>,
    /// Endpoint slug, used for the epoch-ticker thread name and the leaked-tx
    /// cleanup `warn` (H1046, design §2.3).
    slug: Arc<str>,
}

impl ReplayComponent {
    /// Load from a .wasm component artifact at the given path, with a SQLite
    /// KV store at `store_path`.
    ///
    /// `max_page_count` is the host-enforced `PRAGMA max_page_count` value
    /// derived from the configured byte limit for this store (from
    /// `ResolvedReplayProtection.max_page_count`). Use
    /// `store::DEFAULT_MAX_PAGE_COUNT` in tests that do not need a tight cap.
    ///
    /// `config` is the operator-supplied config map (plus any host-injected
    /// `brenn.*` keys). Pass an empty map for components that read no config.
    ///
    /// `slug` is the endpoint slug, used for the epoch-ticker thread name and the
    /// leaked-tx cleanup `warn` (H1046, design §2.3).
    ///
    /// Panics on any failure — per backend robustness rules.
    pub fn load(
        slug: &str,
        path: &Path,
        store_path: &Path,
        max_page_count: u32,
        config: HashMap<String, String>,
    ) -> Self {
        // Enable fuel consumption AND epoch interruption so a pathological replay
        // guest traps (bounded) instead of hanging — mirrors the processor engine
        // config (H1046, design §2.2).
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.epoch_interruption(true);
        let engine = Engine::new(&cfg)
            .unwrap_or_else(|e| panic!("failed to initialize wasmtime replay engine: {e}"));
        let mut linker: Linker<StoreData> = Linker::new(&engine);
        brenn::replay::store::add_to_linker::<_, HasSelf<StoreData>>(&mut linker, |s| s)
            .unwrap_or_else(|e| panic!("failed to add store to linker: {e}"));
        brenn::replay::config::add_to_linker::<_, HasSelf<StoreData>>(&mut linker, |s| s)
            .unwrap_or_else(|e| panic!("failed to add config to replay linker: {e}"));
        let component = Component::from_file(&engine, path).unwrap_or_else(|e| {
            panic!(
                "failed to load WASM component from {}: {e}\n\
                 Rebuild with: make wasm-components",
                path.display()
            )
        });
        let instance_pre = linker.instantiate_pre(&component).unwrap_or_else(|e| {
            panic!(
                "WASM component pre-instantiation failed — import not linked \
                 (grant missing or WIT namespace mismatch): {e}"
            )
        });
        let replay_pre = ReplayPre::new(instance_pre).unwrap_or_else(|e| {
            panic!(
                "WASM component pre-instantiation failed — export type-check failed \
                 (stale .wasm or WIT signature changed): {e}"
            )
        });
        let kv_store = KvStore::open(store_path, max_page_count);

        // Spawn an epoch ticker thread for this engine, mirroring the processor world
        // (design §2.3). Uses an EngineWeak so the thread exits naturally when the
        // ReplayComponent is dropped (engine is the only strong ref). N replay-protected
        // endpoints = N ticker threads; N is small (single digits), acceptable.
        {
            let engine_weak: EngineWeak = engine.weak();
            std::thread::Builder::new()
                .name(format!("wasm-epoch-replay-{slug}"))
                .spawn(move || {
                    loop {
                        std::thread::sleep(std::time::Duration::from_millis(REPLAY_EPOCH_TICK_MS));
                        match engine_weak.upgrade() {
                            Some(e) => e.increment_epoch(),
                            None => break, // ReplayComponent dropped; exit ticker.
                        }
                    }
                })
                .unwrap_or_else(|e| {
                    panic!("failed to spawn replay epoch ticker thread for {slug}: {e}")
                });
        }

        Self {
            engine,
            replay_pre,
            kv_store,
            config: Arc::new(config),
            slug: Arc::from(slug),
        }
    }

    fn make_store(&self) -> Store<StoreData> {
        // Install memory/table/instance caps, per-call fuel, and the epoch deadline,
        // mirroring the processor world's bounding (H1046, design §2.4).
        // `trap_on_grow_failure(true)` makes an over-cap `memory.grow` a deterministic
        // trap rather than a -1 the guest could observe and loop on.
        let limits = StoreLimitsBuilder::new()
            .memory_size(REPLAY_MAX_MEMORY_BYTES)
            .table_elements(REPLAY_MAX_TABLE_ELEMENTS)
            .instances(REPLAY_MAX_INSTANCES)
            .tables(REPLAY_MAX_TABLES)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            StoreData {
                resource_table: ResourceTable::new(),
                kv_store: Arc::clone(&self.kv_store),
                config: Arc::clone(&self.config),
                limits,
            },
        );
        store.limiter(|d| &mut d.limits);
        store
            .set_fuel(REPLAY_FUEL)
            .unwrap_or_else(|e| panic!("failed to set fuel on replay store: {e}"));
        store.set_epoch_deadline(REPLAY_EPOCH_DEADLINE_TICKS);
        store
    }

    /// Invoke the component's `check` export.
    ///
    /// Returns `(verdict, quota_hit)` where:
    /// - `verdict` is `Ok(())` for accept, `Err(ReplayError)` for a typed reject.
    /// - `quota_hit` is `true` if the store's host-quota was hit during this call
    ///   (SQLITE_FULL fired in `tx_put`). The flag is read-and-cleared from the
    ///   KvStore's `quota_hit` AtomicBool so subsequent calls start clean.
    ///
    /// A trap (component panic, unreachable, resource exhaustion, etc.) causes this
    /// function to panic — per backend robustness rules (design §2.5).
    pub fn check(&self, input: &CheckInput) -> (Result<(), ReplayError>, bool) {
        let verdict = self
            .run_check(input, None)
            .unwrap_or_else(|e| panic!("WASM runtime error (guest trap or instantiation): {e}"));
        // Read-and-clear the quota_hit flag. SeqCst to ensure we see the write
        // that happened inside tx_put (same thread in spawn_blocking, but use
        // SeqCst for clarity and cross-thread-safe-at-Arc-boundary).
        let quota_hit = self
            .kv_store
            .quota_hit
            .swap(false, std::sync::atomic::Ordering::SeqCst);
        (verdict, quota_hit)
    }

    /// Shared dispatch core: build the store, instantiate, call `check`, then run the
    /// unconditional leaked-transaction cleanup on **every** exit path (both the
    /// instantiate-fail `Err` and the `call_check` result), and return the captured
    /// outcome (H1046, design §2.5.1).
    ///
    /// Both production `check` and the test hatches `check_raw_for_testing` /
    /// `check_with_limits` funnel through here, so the cleanup is reachable from the
    /// production path **structurally** — not by happenstance of delegation — and fires
    /// even when instantiation traps. Mirrors the processor world's
    /// `invoke`/`cleanup_leaked_tx`.
    ///
    /// `overrides` is `Some((epoch_deadline, fuel))` for the test hatch that needs to
    /// drive the epoch/fuel interrupt paths fast; production passes `None` and keeps the
    /// store's installed `REPLAY_*` budgets. Routing both through this single core (rather
    /// than letting `check_with_limits` inline its own instantiate+call+cleanup copy) keeps
    /// the cleanup path single-sourced — the test accessor exercises the same dispatch core
    /// the production path uses.
    fn run_check(
        &self,
        input: &CheckInput,
        overrides: Option<(u64, u64)>,
    ) -> wasmtime::Result<Result<(), ReplayError>> {
        let mut store = self.make_store();
        if let Some((epoch_deadline, fuel)) = overrides {
            store
                .set_fuel(fuel)
                .unwrap_or_else(|e| panic!("run_check override: set_fuel: {e}"));
            store.set_epoch_deadline(epoch_deadline);
        }
        self.run_on_store(store, input)
    }

    /// Instantiate-and-call on an already-built store, then run the unconditional
    /// leaked-transaction cleanup on **every** exit path. This is the single copy of the
    /// instantiate→call→cleanup sequence; `run_check` (production + epoch/fuel-override
    /// test hatches) and `check_with_memory_limit` (memory-override test hatch) both build
    /// their store, then delegate here — so the cleanup is single-sourced and the test
    /// accessors exercise the same dispatch body the production path uses.
    fn run_on_store(
        &self,
        mut store: Store<StoreData>,
        input: &CheckInput,
    ) -> wasmtime::Result<Result<(), ReplayError>> {
        // Capture each fallible step into a local instead of `?`-propagating mid-body,
        // so cleanup runs once on the way out regardless of where the failure occurred.
        let outcome = self
            .replay_pre
            .instantiate(&mut store)
            .and_then(|replay| replay.call_check(&mut store, input));
        self.cleanup_leaked_tx();
        outcome
    }

    /// If a store transaction is still active after `run_check`'s call returns (trap or
    /// `mem::forget` in a buggy guest), roll it back and clear the `tx_active` flag.
    ///
    /// On a guest trap the WIT `drop` destructor never runs (no guest execution after
    /// trap), so a leaked transaction on the shared `Arc<KvStore>` is not rolled back via
    /// the normal `HostTransaction::drop` path and would wedge every later check on this
    /// endpoint. Roll it back explicitly to release the write lock. Modeled on the
    /// processor world's `cleanup_leaked_tx` (H1046, design §2.5.1). Rollback failure
    /// escalates to panic (an unrollback-able store is corruption, not a tolerable state).
    fn cleanup_leaked_tx(&self) {
        if self.kv_store.is_tx_active() {
            warn!(
                slug = %self.slug,
                "wasm replay: store transaction leaked (guest trapped mid-transaction); \
                 rolling back to release write lock"
            );
            let conn = self.kv_store.lock_conn();
            store::rollback_tx(&conn).unwrap_or_else(|e| {
                panic!(
                    "cleanup_leaked_tx: ROLLBACK failed ({e}) — store may be corrupted \
                     (slug={})",
                    self.slug
                )
            });
            drop(conn);
            self.kv_store.clear_tx();
        }
    }

    /// Like `check`, but surfaces the outer `wasmtime::Result` instead of
    /// collapsing a trap to a panic.
    ///
    /// Test-only escape hatch so integration tests can distinguish a trap
    /// (outer `Err`) from a typed reject (inner `Err`). Not for production
    /// callers; use `check` instead. A thin wrapper over `run_check` — it carries
    /// no cleanup of its own; the leaked-tx cleanup lives in `run_check`.
    #[doc(hidden)]
    pub fn check_raw_for_testing(
        &self,
        input: &CheckInput,
    ) -> wasmtime::Result<Result<(), ReplayError>> {
        self.run_check(input, None)
    }

    /// Like `check_raw_for_testing`, but with an overridden epoch deadline and fuel —
    /// test use only.
    ///
    /// Allows tests to pass a tiny epoch deadline and high fuel to drive the epoch
    /// interrupt path without waiting for the production ≈ 5 s wall budget. Mirrors
    /// `ProcessorComponent::handle_with_limits` (design §4).
    ///
    /// A thin wrapper over `run_check` with the override applied — it carries no
    /// instantiate/call/cleanup body of its own, so the test accessor exercises the same
    /// dispatch core (and the same leaked-tx cleanup) the production path uses.
    #[doc(hidden)]
    pub fn check_with_limits(
        &self,
        input: &CheckInput,
        epoch_deadline: u64,
        fuel: u64,
    ) -> wasmtime::Result<Result<(), ReplayError>> {
        self.run_check(input, Some((epoch_deadline, fuel)))
    }

    /// Like `check_raw_for_testing`, but with an overridden store `memory_size` limit —
    /// test use only.
    ///
    /// Sets the store-level memory cap below a fixture's initial memory declaration so the
    /// limit fires during `replay_pre.instantiate` (an instantiation-time resource-limit
    /// failure → outer `Err`), proving the `make_store` limiter applies to instantiation
    /// and not only to a later `memory.grow` (design §3 edge-case table). Mirrors
    /// `ProcessorComponent::handle_with_memory_limit`.
    ///
    /// Pass a `memory_limit_bytes` well below the fixture's initial memory (e.g. 1) to
    /// guarantee the limit fires during instantiation rather than later.
    #[doc(hidden)]
    pub fn check_with_memory_limit(
        &self,
        input: &CheckInput,
        memory_limit_bytes: usize,
    ) -> wasmtime::Result<Result<(), ReplayError>> {
        let mut store = self.make_store();
        // StoreLimits is not mutable after construction; replace the whole limits field.
        // The rest of the store settings (fuel, epoch) come from make_store.
        store.data_mut().limits = StoreLimitsBuilder::new()
            .memory_size(memory_limit_bytes)
            .table_elements(REPLAY_MAX_TABLE_ELEMENTS)
            .instances(REPLAY_MAX_INSTANCES)
            .tables(REPLAY_MAX_TABLES)
            .trap_on_grow_failure(true)
            .build();
        self.run_on_store(store, input)
    }

    /// Expose the underlying KvStore for test assertions (e.g. is_tx_active after a leak-trap).
    ///
    /// Integration tests are external linkers so pub(crate) is insufficient — must be pub.
    /// Matches the `check_raw_for_testing` precedent (design §4.1).
    #[doc(hidden)]
    pub fn kv_store_for_testing(&self) -> &Arc<KvStore> {
        &self.kv_store
    }
}

// ---------------------------------------------------------------------------
// Processor-component runtime (§2.2)
//
// Bindgen for the `processor` WIT world: one export (`receive`), imports for
// `ports` (publish) and `store` (KV transactions).
// Generates `ProcessorPre<T>`, `Activation`, `PortWindow`, `ReceiveError` and
// the `brenn::processor::ports` / `brenn::processor::store` Host traits.
// ---------------------------------------------------------------------------

// The generated `processor` world types may trigger clippy lints (e.g. dead_code
// on generated trait impls). Suppress targeted lints only.
#[allow(dead_code)]
mod processor_bindings {
    wasmtime::component::bindgen!({
        world: "processor",
        path: "wit/processor.wit",
        with: {
            "brenn:processor/store.transaction": crate::store::Transaction,
        },
    });
}

use processor_bindings::ProcessorPre;
use processor_bindings::brenn::processor::mqtt::MqttPublishError as MqttPublishWitError;
use processor_bindings::brenn::processor::ports::PublishError;
use processor_bindings::brenn::processor::store::StoreError as ProcessorStoreError;
use processor_bindings::brenn::processor::tools::ToolError as ToolWitError;

// Re-export the receive-error type so callers can match on `ProcessorOutcome::Err(e)`.
pub use processor_bindings::brenn::processor::types::ReceiveError as ProcessorReceiveError;

/// Alert severity as seen at the brenn-wasm API boundary.
///
/// Mirror of the WIT `alert.severity` enum so the host-callback trait does not
/// expose bindgen-generated types across crate boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestAlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Host callback for guest-originated alerts.
///
/// `brenn-wasm` cannot depend on `brenn-lib` (where `AlertDispatcher` lives);
/// the binary crate implements this trait by wrapping `AlertDispatcher`.
/// Receives already-sanitized, already-truncated strings; the impl adds
/// component attribution.
pub trait ProcessorAlerter: Send + Sync {
    fn alert(&self, severity: GuestAlertSeverity, title: &str, body: &str);
}

/// Outcome of a synchronous MQTT publish, as seen at the `brenn-wasm` API
/// boundary.
///
/// Mirror of the WIT `mqtt.mqtt-publish-error` variant (plus an `Ok` arm) so the
/// host-callback seam (`MqttPublishFn`) never exposes a bindgen-generated WIT
/// type — and, more importantly, never a `brenn-lib` type. `brenn-wasm` cannot
/// depend on `brenn-lib` (where `enforce_and_publish` / `MqttEgressError` live);
/// the binary crate builds a closure that calls `enforce_and_publish` and maps
/// its `MqttEgressError` into this enum. This is the same crate-boundary seam as
/// `OutputAclFn` and `ProcessorAlerter` (design §2.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttPublishOutcome {
    /// Publish reached the broker and (for QoS >= 1) was acknowledged.
    Ok,
    /// Grant/ACL denied for the target client. Permanent for this config.
    NotPermitted,
    /// MQTT service not configured on this server (no client referenced by any
    /// ingress channel or ACL matcher). Mirrors the WIT `no-connector` variant.
    /// Permanent.
    NoConnector,
    /// Address parse / wildcard / oversize failure. Diagnostic string.
    InvalidPayload(String),
    /// Broker disconnect / submit failure / ack drop. Transient. Diagnostic.
    Broker(String),
    /// Broker rejected the publish (PUBACK reason code != success). Diagnostic.
    BrokerRejected(String),
}

/// Host callback performing a synchronous MQTT publish for the `mqtt` interface.
///
/// `brenn-wasm` cannot depend on `brenn-lib` (where the shared enforcement
/// pipeline `enforce_and_publish` lives); the binary crate builds this closure
/// over the `MqttService`, the consumer's `AppPolicy`, and the consumer slug,
/// and internally calls `enforce_and_publish`, mapping the result into
/// `MqttPublishOutcome`. Same seam as `OutputAclFn` / `ProcessorAlerter`.
///
/// Arguments are the guest-supplied `(client, topic, payload, content_type, qos,
/// retain)` exactly as received over the WIT boundary; the closure owns address
/// parsing/validation (mapping a failure to `MqttPublishOutcome::InvalidPayload`)
/// and the call to the shared enforcement pipeline.
///
/// Contract: the closure MUST NOT panic on any *guest-supplied input* — bad
/// client/topic/payload/qos must map to a `MqttPublishOutcome` variant, never a
/// panic, because `do_mqtt_publish` invokes it without a `catch_unwind` boundary.
/// A *host-side invariant violation* (a wiring bug in the closure's own plumbing,
/// not a guest input) is a different matter and SHOULD panic per the project
/// posture — the in-tree closure panics via `unreachable!` if `enforce_and_publish`
/// ever returns `BudgetExhausted` under the `SendBudget::None` it always passes.
/// In-tree closures are non-panicking on all guest inputs; out-of-tree hosts must
/// likewise handle every guest input total. Callers with no MQTT egress pass `None`
/// (the `Mqtt` capability is then never linked, so the host fn is unreachable; the
/// `None` is a structural backstop).
pub type MqttPublishFn = Arc<
    dyn Fn(String, String, Vec<u8>, Option<String>, u8, bool) -> MqttPublishOutcome + Send + Sync,
>;

/// Failure modes returned across the `ToolHost` seam.
///
/// Mirror of the WIT `tools.tool-error` variant (see `processor.wit`) so the
/// host-callback seam (`ToolHost`) never exposes a bindgen-generated WIT type —
/// and, more importantly, never a `brenn-lib`/`brenn-server` type (`brenn-wasm`
/// cannot depend on either). The binary crate builds a `ToolHost` over the native
/// `ToolRegistry` + the consumer's `AppPolicy` and maps its own `ToolError` into
/// this enum. Same crate-boundary seam as `MqttPublishOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolCallError {
    /// Unknown tool OR ungranted tool — deliberately indistinguishable.
    NotGranted,
    /// ACL clause miss; the string names the offending resource.
    Denied(String),
    /// Args JSON malformed / failed the tool's arg deserialization.
    InvalidArgs(String),
    /// The caller's per-(participant, tool) rate bucket was empty (fast class).
    RateLimited,
    /// `call-fast` on an async tool, or `call-async` on a fast tool.
    WrongClass,
    /// Tool bug the host has already alerted on; stable coarse token.
    Internal(String),
}

/// A validated, host-resolved async tool request awaiting the activation's
/// transactional flush.
///
/// `ToolHost::queue_async` performs the grant/ACL/tool-resolution work (which
/// needs `brenn-lib` types the guest side cannot see) and hands back these
/// already-resolved envelope pieces; `brenn-wasm` buffers them and the dispatch
/// layer publishes them iff the activation returns ok (increment 6). The guest
/// never names channels — `channel` and `reply_to` are host-resolved here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedToolRequest {
    /// System request channel for the target tool (`brenn:tools/<tool>`).
    pub channel: String,
    /// The caller's implicit result inbox (`brenn:tool-results/<slug>`).
    pub reply_to: String,
    /// The request envelope body JSON (`{ v, tool, call_id, caller, args, ... }`).
    pub body_json: String,
}

/// Host seam for the `tools` WIT interface — the native tool substrate as seen
/// from `brenn-wasm`.
///
/// `Some(tool_host)` on `ProcessorLoadSpec` iff the component holds the `Tools`
/// capability (asserted at load, like `mqtt_publish`). The binary crate builds
/// the concrete impl over the `ToolRegistry` + the consumer's resolved
/// `AppPolicy`; grant/ACL/class checks and the fast-tool time budget live behind
/// this seam because they need types `brenn-wasm` cannot depend on.
///
/// Contract: neither method may panic on *guest-supplied input* — a bad
/// tool/args must map to a `ToolCallError`, never a panic (the host binding calls
/// them without a `catch_unwind` boundary). A host-side wiring invariant may
/// panic per the project posture.
pub trait ToolHost: Send + Sync {
    /// Execute a fast-class tool synchronously and return its result JSON.
    ///
    /// Runs the grant + ACL (against `args_json`) + class checks, enforces the
    /// per-tool time budget (overrun = tool-bug alert, call still returns), and
    /// dispatches to the tool's synchronous `execute`.
    fn fast_call(&self, tool: &str, args_json: &str) -> Result<String, ToolCallError>;

    /// Validate an async-class tool call and resolve it into a `QueuedToolRequest`.
    ///
    /// Runs the grant + ACL + class checks synchronously (so caller mistakes fail
    /// fast) and resolves the request/reply channels; does NOT publish — the
    /// caller buffers the returned request for transactional flush.
    fn queue_async(
        &self,
        tool: &str,
        args_json: &str,
        call_id: &str,
    ) -> Result<QueuedToolRequest, ToolCallError>;
}

/// Host callback for the `tools` interface. `Some` iff the `Tools` capability is
/// linked; `None` = no tool surface (the interface is then unlinked and the host
/// fns unreachable, so `None` is a structural backstop, like `mqtt_publish`).
pub type ToolHostFn = Arc<dyn ToolHost>;

/// Cap + debug-escape a guest-controlled port name for safe logging, using the
/// `{:?}`-quoted form.
///
/// Thin wrapper over [`brenn_common::sanitize_untrusted_str`] with the
/// `PROCESSOR_MAX_PORT_NAME_LOG_BYTES` output cap, wrapped in quotes. The shared
/// function bounds the escaped output and appends its own truncation marker
/// (inside the quotes) when input is dropped, so a hostile guest cannot emit a
/// multi-megabyte log line or inject ANSI escapes.
fn cap_guest_for_log(raw: &str) -> String {
    format!(
        "\"{}\"",
        sanitize_untrusted_str(raw, PROCESSOR_MAX_PORT_NAME_LOG_BYTES)
    )
}

/// Epoch ticker interval: how often the epoch is incremented.
const PROCESSOR_EPOCH_TICK_MS: u64 = 100;

/// Epoch deadline ticks per activation (300 × 100 ms ≈ 30 s wall).
const PROCESSOR_EPOCH_DEADLINE_TICKS: u64 = 300;

/// Per-invocation resource bound constants for `ProcessorComponent` (§2.2).
///
/// Fuel: at 10M instructions per envelope entry and a default window of 32
/// entries this gives 320M instructions — comfortably above any reasonable
/// stateless JSON-processing loop, well below a CPU-spin blowup.
pub const PROCESSOR_FUEL_PER_ENVELOPE: u64 = 10_000_000;
/// Minimum fuel even for an empty window (startup/invocation overhead).
pub const PROCESSOR_FUEL_MINIMUM: u64 = 50_000_000;
/// Maximum memory a guest may grow to (bytes). 16 MiB per activation.
///
/// Cap excess is a deterministic trap: `trap_on_grow_failure(true)` is set on
/// the processor store limiter, so `memory.grow` raises a trap (never returns
/// -1) when the guest attempts to exceed this bound. The activation resolves to
/// `ProcessorOutcome::Trap` regardless of the guest's allocator behavior.
pub const PROCESSOR_MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
/// Maximum elements **per table** a processor guest's store may hold. wasmtime's
/// `table_elements` ceiling is applied to each table individually (wasmtime 45
/// `StoreLimitsBuilder::table_elements` docs), NOT as a store-wide total.
pub const PROCESSOR_MAX_TABLE_ELEMENTS: usize = 65_536;
/// Maximum WASM instances a processor guest's store may hold.
pub const PROCESSOR_MAX_INSTANCES: usize = 16;
/// Maximum number of WASM tables a processor guest's store may hold.
pub const PROCESSOR_MAX_TABLES: usize = 64;
/// Maximum length of a guest-supplied port name in the `publish` import.
///
/// Port names from a guest are wholly guest-controlled and must be bounded before
/// logging to prevent multi-megabyte log lines. Config-validated port names are
/// always ≤ this limit; unrecognized longer names are truncated in the log.
pub const PROCESSOR_MAX_PORT_NAME_LOG_BYTES: usize = 256;
/// Maximum `log.log` calls per activation (256).
///
/// Bounds log-flood DoS from hostile or buggy out-of-tree components. Calls
/// beyond this quota are dropped; the suppressed count is emitted as a single
/// post-activation `warn` (not one warn per dropped call).
pub const PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION: usize = 256;
/// Maximum `alert.alert` calls per activation (4).
///
/// Alerts page a human and are expensive; more than 4 per activation is
/// pathological and would consume process-wide rate-limit budget needed for host
/// alerts. Suppressed counts follow the same post-activation warn as logs.
pub const PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION: usize = 4;
/// Maximum bytes in a guest-supplied log message (4 KiB).
pub const PROCESSOR_MAX_LOG_MESSAGE_BYTES: usize = 4096;
/// Maximum bytes in a guest-supplied alert title (256 B).
pub const PROCESSOR_MAX_ALERT_TITLE_BYTES: usize = 256;
/// Maximum bytes in a guest-supplied alert body (4 KiB).
pub const PROCESSOR_MAX_ALERT_BODY_BYTES: usize = 4096;
/// Maximum bytes in a guest-supplied trap/error diagnostic string (4 KiB).
///
/// Applied at `sanitize_untrusted_str` call sites in the binary crate where trap messages
/// and `ReceiveError` diagnostics (guest-controlled strings) are sanitized before being
/// logged and included in alert bodies. Kept distinct from `PROCESSOR_MAX_LOG_MESSAGE_BYTES`
/// and `PROCESSOR_MAX_ALERT_BODY_BYTES` because trap/err diagnostics are a separate signal
/// path and the cap may diverge from the interactive log/alert caps in the future.
pub const PROCESSOR_MAX_DIAG_BYTES: usize = 4096;

/// Maximum bytes in a guest-supplied `tools` `args-json` (64 KiB, both classes).
///
/// A guest-controlled argument blob; over-cap is a permanent `invalid-args` (never
/// forwarded to a tool). Mirrors the host-side registry cap (the native
/// `ToolRegistry` fixes the same value); the constant is duplicated across the
/// crate boundary because `brenn-wasm` cannot depend on `brenn-server`, exactly
/// like the `MqttPublishOutcome` WIT-variant mirror.
pub const PROCESSOR_MAX_TOOL_ARGS_BYTES: usize = 64 * 1024;
/// Maximum bytes in a fast-tool result JSON returned to the guest (256 KiB).
///
/// Over-cap is a *tool bug*: the host returns `internal` and alerts (a tool must
/// not hand a guest an unbounded result). Async results are bounded separately on
/// the executor side (increment 6).
pub const PROCESSOR_MAX_FAST_TOOL_RESULT_BYTES: usize = 256 * 1024;
/// Maximum fast-tool calls per activation (64).
///
/// Bounds host-fn flood from a hostile or buggy component. Calls beyond the cap
/// return `internal`; the overflow is surfaced as a single post-activation warn
/// (same family as the publish/log per-activation quotas).
pub const PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION: usize = 64;

// ---------------------------------------------------------------------------
// Replay-world resource-bound constants (H1046, design §2.1).
//
// The replay check runs an operator-supplied guest on every inbound webhook POST
// (external/untrusted input). These constants mirror the processor world's proven
// bounding mechanism — fuel, epoch deadline, and store limits — onto the replay
// engine and store. A replay check is a single fixed-shape KV-backed nonce/timestamp
// operation (no per-envelope window), so the fuel budget is flat (not scaled) and the
// wall budget is tighter than the processor's multi-envelope window.
// ---------------------------------------------------------------------------

/// Replay epoch ticker interval: how often the replay engine's epoch is incremented.
/// Same cadence as the processor world (`PROCESSOR_EPOCH_TICK_MS`).
///
/// Crate-private — an internal ticker detail with no external consumer, matching the
/// processor precedent (`PROCESSOR_EPOCH_TICK_MS` is also `const`, not `pub const`).
const REPLAY_EPOCH_TICK_MS: u64 = 100;

/// Replay epoch deadline ticks per check (50 × 100 ms ≈ 5 s wall budget).
///
/// Deliberately tighter than the processor's 300 ticks (≈ 30 s): a replay check is one
/// fixed KV lookup/insert and should finish in milliseconds. Because the per-endpoint
/// mutex serializes all traffic to an endpoint behind a slow guest, a tighter wall
/// budget is strictly safer and has no honest cost (design §2.7-B). Not
/// operator-configurable (design §2.6).
///
/// Crate-private — an internal tuning detail with no external consumer, matching the
/// processor precedent (`PROCESSOR_EPOCH_DEADLINE_TICKS` is also `const`, not `pub const`).
/// Tests drive the epoch path with a literal `1`-tick deadline via `check_with_limits`,
/// so they do not import this constant.
const REPLAY_EPOCH_DEADLINE_TICKS: u64 = 50;

/// Fixed per-call fuel for a replay check (320M instructions).
///
/// A replay check is a single fixed-shape operation (no per-envelope window), so a flat
/// budget is correct — there is no envelope count to scale by (design §2.7-C).
///
/// The design (§2.1/§2.7-C) initially proposed 50M (matching `PROCESSOR_FUEL_MINIMUM`),
/// but the production `brenn_replay.wasm` worst-case honest check — an insert or cap-scan
/// at the high end of the 1024-nonce / 5-minute window, which prunes/scans the namespace —
/// measured ≈ 58.6M instructions, exceeding 50M. Per the design's own escape hatch
/// (§2.7-B / §5: "If a real component proves the budget too tight, raising the constant is
/// a one-line change"), the budget is raised to 320M: the same order as the processor
/// world's full-window budget (`PROCESSOR_FUEL_PER_ENVELOPE × 32`), ≈ 5.5× the measured
/// honest worst case (generous headroom), and still finite — a CPU-spinning guest
/// exhausts it well within the epoch window, and the epoch deadline (§2.7-B) is the
/// wall-clock backstop regardless.
pub const REPLAY_FUEL: u64 = 320_000_000;

/// Maximum memory a replay guest may grow to (bytes). 16 MiB per check.
///
/// Same as `PROCESSOR_MAX_MEMORY_BYTES`. A replay guest holds only the request body plus
/// small KV scratch; 16 MiB is generous. Cap excess is a deterministic trap:
/// `trap_on_grow_failure(true)` is set on the replay store limiter, so `memory.grow`
/// raises a trap (never returns -1) when the guest attempts to exceed this bound.
pub const REPLAY_MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;

/// Maximum elements **per table** a replay guest's store may hold. Same as the processor
/// world; bounds structural growth with no honest reason to differ (design §2.7-D).
pub const REPLAY_MAX_TABLE_ELEMENTS: usize = 65_536;

/// Maximum WASM instances a replay guest's store may hold. Same as the processor world.
pub const REPLAY_MAX_INSTANCES: usize = 16;

/// Maximum number of WASM tables a replay guest's store may hold. Same as the processor world.
pub const REPLAY_MAX_TABLES: usize = 64;

/// One buffered publish from a guest activation.
#[derive(Debug)]
pub struct ProcessorPublish {
    /// Logical port name (carried for logging only; channel address is resolved).
    pub port: String,
    /// Resolved channel address (attenuation-resolved at buffer time).
    pub channel_address: String,
    /// Message body (opaque string).
    pub payload: String,
    /// Urgency for this publish: guest-supplied (via `publish-with-urgency`) or
    /// the port's configured default (via `publish`).
    pub urgency: ProcessorUrgency,
    /// Reply channel address, set only for async tool-call requests (the caller's
    /// result inbox `brenn:tool-results/<slug>`). `None` for ordinary port
    /// publishes. The host resolves this address to a channel reference at flush.
    pub reply_to: Option<String>,
}

/// Logical port label carried on the `ProcessorPublish` synthesized from an async
/// tool-call request (there is no bound output port — the request rides the
/// activation's transactional flush). Logging only; the channel address is the
/// resolved `brenn:tools/<tool>` request channel.
const TOOL_REQUEST_LOG_PORT: &str = "<async-tool-request>";

/// Urgency as carried through the host buffer — mirrors the WIT `ports.urgency` enum.
///
/// Defined here (at the `brenn-wasm` API boundary) so callers don't depend on
/// bindgen-generated types. Maps 1:1 to `brenn_lib::messaging::Urgency` at the
/// `publish_from_wasm` call site in `brenn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorUrgency {
    VeryLow,
    Low,
    Normal,
    High,
}

use brenn_budget::{
    MAX_PUBLISH_BYTES_PER_ACTIVATION, MAX_PUBLISH_CALLS_PER_ACTIVATION,
    MAX_PUBLISHES_PER_ACTIVATION, seed_sink_budget,
};
/// The publish-budget vocabulary, re-exported: this crate's public API hands
/// out `SinkBudget`s and charges `MILLITOKENS_PER_PUBLISH` per publish, so the
/// names stay reachable from here. The definitions live in `brenn-budget`,
/// which every host that mints activations reads — the wasmtime host below, the
/// bus config resolver, and the surface kernel.
pub use brenn_budget::{MILLITOKENS_PER_PUBLISH, SinkBudget};

/// Resolved output-port binding plus its per-sink publish budget.
pub struct OutputPortSpec {
    /// Resolved channel address (the full `brenn:<name>` form).
    pub channel_address: String,
    /// Default urgency for publishes on this port (guest may override per call).
    pub default_urgency: ProcessorUrgency,
    /// Per-sink token-bucket budget for this port.
    pub budget: SinkBudget,
}

/// Key identifying one egress sink for per-sink publish budgeting.
///
/// `Port` is a bound `[[wasm_consumer.output]]` port name; `MqttClient` is an
/// ACL-allowed MQTT client slug. Both are host-config-derived (never
/// guest-controlled), so safe to log verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SinkKey {
    Port(String),
    MqttClient(String),
}

/// Sum the input-driven token grant (millitokens) for one activation of this
/// host's activation type.
///
/// The arithmetic is `brenn_budget::grant_input_mt`; this reads the
/// `(amplification_mt, new_count)` pairs off the wasmtime host's windows. A
/// window port absent from `amplification_mt` is a host invariant violation
/// (windows are built from the same config that populates the map) — panic.
fn compute_grant_input_mt(
    amplification_mt: &HashMap<String, u64>,
    activation: &ProcessorActivation,
) -> u64 {
    brenn_budget::grant_input_mt(activation.ports.iter().map(|pw| {
        let amp = amplification_mt.get(&pw.port).copied().unwrap_or_else(|| {
            panic!(
                "host invariant: no amplification for activation window port {:?}",
                pw.port
            )
        });
        let new_count = pw.envelopes.len().saturating_sub(pw.new_from as usize) as u64;
        (amp, new_count)
    }))
}

/// One port-window handed to `ProcessorComponent::handle`.
///
/// This host carries an envelope as its JSON text: that is what crosses the WIT
/// boundary into the guest.
pub type ProcessorPortWindow = brenn_activation::PortWindow<String>;

/// Host-side activation handed to `ProcessorComponent::handle`.
///
/// Contains one [`ProcessorPortWindow`] per bound input port, in config (`inputs`)
/// order. Every bound port appears in every activation; ports without pending rows
/// arrive as pure-context windows (`new_from == envelopes.len()`). Guests must not
/// assume `ports.len() == 1`.
pub type ProcessorActivation = brenn_activation::Activation<String>;

/// Outcome of one `ProcessorComponent::handle` invocation.
#[derive(Debug)]
pub enum ProcessorOutcome {
    /// Guest returned `result::ok`. Vec contains all buffered publishes.
    Ok(Vec<ProcessorPublish>),
    /// Guest returned `result::err` — typed batch rejection.
    Err(ProcessorReceiveError),
    /// Guest panicked, trapped, or exhausted a resource limit.
    Trap(String),
}

/// Predicate deciding whether this component may publish to a resolved channel
/// address (the full `brenn:<name>` form held in `output_ports`). Constructed by
/// the host over the component's `AppPolicy`; `brenn-wasm` never sees the policy
/// type and never parses the address — the closure owns the `brenn:`-prefix
/// convention. Returning `false` ⇒ `PublishError::NotPermitted`, identical to an
/// unbound port. Callers with no policy pass an allow-all closure (`Arc::new(|_| true)`).
///
/// Contract: the closure MUST NOT panic on any input (including non-`brenn:` or
/// empty addresses) — `do_publish` calls it without a `catch_unwind` boundary, so
/// a panic unwinds through the activation and the resulting message carries only
/// what the closure itself sets, without `do_publish`'s `slug`/`channel` context.
/// In-tree closures (`allows_brenn_publish` and allow-all) are non-panicking;
/// out-of-tree hosts must handle every input total.
pub type OutputAclFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Per-invocation store data for the `processor` world.
struct ProcessorData {
    resource_table: ResourceTable,
    limits: StoreLimits,
    /// `None` when the component does not have the `Store` grant.
    /// Unreachable via the store host impls when unlinked; `.expect` there is the
    /// structural backstop (linker gating is the real gate).
    kv_store: Option<Arc<KvStore>>,
    /// Logical port name → resolved binding + per-sink budget.
    output_ports: Arc<HashMap<String, OutputPortSpec>>,
    config: Arc<HashMap<String, String>>,
    slug: Arc<str>,
    max_payload_bytes: usize,
    publish_buffer: Vec<ProcessorPublish>,
    published_bytes: usize,
    /// Total `publish` import calls this activation (successful + rejected).
    /// Bounds log-flood DoS from repeated not-permitted / invalid-payload calls.
    publish_call_count: usize,
    /// Remaining per-sink publish budget this activation (millitokens), seeded at
    /// activation start from carryover + fill + input grant. Each accepted bus
    /// publish / attempted MQTT publish subtracts `MILLITOKENS_PER_PUBLISH`.
    publish_budget_by_sink: HashMap<SinkKey, u64>,
    /// Per-sink count of publishes suppressed this activation by an exhausted
    /// budget. Drained into a single post-activation warn.
    publish_suppressed_by_sink: HashMap<SinkKey, u32>,
    /// Host callback for guest-originated alerts (via the `alert` WIT interface).
    alerter: Arc<dyn ProcessorAlerter>,
    /// Output-channel ACL. Called in `do_publish` with the resolved channel
    /// address after the port→channel lookup; `false` ⇒ `NotPermitted`.
    output_acl: OutputAclFn,
    /// Host callback for synchronous MQTT egress (via the `mqtt` WIT interface).
    /// `None` when the component lacks the `Mqtt` grant; the host fn is then
    /// unreachable (the interface is unlinked), so `None` is a structural
    /// backstop rather than a live path.
    mqtt_publish: Option<MqttPublishFn>,
    /// Count of `log.log` calls this activation; quota enforcement.
    log_call_count: usize,
    /// Count of `alert.alert` calls this activation; quota enforcement.
    alert_call_count: usize,
    /// Host seam for the `tools` interface (fast execution + async validation).
    /// `None` when the component lacks the `Tools` grant; the tool host fns are
    /// then unreachable (the interface is unlinked), so `None` is a structural
    /// backstop rather than a live path.
    tool_host: Option<ToolHostFn>,
    /// Validated async tool requests buffered this activation. `take_ok_publishes`
    /// appends them to the publish flush iff the activation returns ok (a trap
    /// discards them); each publishes to its `brenn:tools/<tool>` request channel,
    /// which the host creates at bootstrap and the `ToolExecutor` drains.
    tool_request_buffer: Vec<QueuedToolRequest>,
    /// Count of fast-tool calls this activation; quota enforcement (anti-flood).
    tool_call_count: usize,
}

impl ProcessorData {
    fn kv_for_tx(&self, tx: &Resource<Transaction>, op: &str) -> Arc<KvStore> {
        let t = self
            .resource_table
            .get(tx)
            .unwrap_or_else(|e| panic!("Transaction resource invalid in {op}: {e}"));
        Arc::clone(&t.kv_store)
    }
}

// --- ports::Host impl ---

impl ProcessorData {
    /// Inner publish implementation shared by `publish` and `publish_with_urgency`.
    ///
    /// `guest_urgency` is `Some(u)` for an explicit guest-supplied urgency (from
    /// `publish-with-urgency`) or `None` to use the port's configured default.
    fn do_publish(
        &mut self,
        port: String,
        payload: String,
        guest_urgency: Option<processor_bindings::brenn::processor::ports::Urgency>,
    ) -> Result<(), PublishError> {
        // Per-activation total-call budget (successful + failed).
        // Checked first to bound log-flood DoS from repeated unbound-port calls by
        // hostile out-of-tree components. Failed calls consume budget because they
        // are otherwise free from the attacker's perspective (no payload bytes, no
        // successful buffer slot used).
        self.publish_call_count += 1;
        if self.publish_call_count > MAX_PUBLISH_CALLS_PER_ACTIVATION {
            return Err(PublishError::QuotaExceeded);
        }

        // Resolve port name → binding + budget (attenuation check).
        // Port name is guest-controlled: cap and debug-escape before logging to prevent
        // multi-megabyte log lines or ANSI-escape injection into the host log pipeline.
        let Some(spec) = self.output_ports.get(&port) else {
            warn!(
                slug = %self.slug,
                port = %cap_guest_for_log(&port),
                "wasm publish: not-permitted — unbound port"
            );
            return Err(PublishError::NotPermitted);
        };
        let channel_address = &spec.channel_address;
        let port_default_urgency = &spec.default_urgency;
        // Output-channel ACL gate. The bound channel must also be in the
        // component's `brenn_publish` allowlist (belt-and-suspenders with the
        // static port binding; load-bearing if WASM ever gains dynamic outputs).
        // `channel_address` is host-resolved from operator config (not
        // guest-controlled), so it is safe to log verbatim — unlike `port`.
        if !(self.output_acl)(channel_address) {
            // `port` is guest-controlled: cap and debug-escape before logging,
            // identical to the unbound-port path above. `channel_address` is
            // host-resolved (operator config), so it is logged verbatim.
            warn!(
                slug = %self.slug,
                port = %cap_guest_for_log(&port),
                channel = %channel_address,
                "wasm publish: not-permitted — channel outside output ACL"
            );
            return Err(PublishError::NotPermitted);
        }
        // Payload size gate.
        if payload.len() > self.max_payload_bytes {
            return Err(PublishError::InvalidPayload(format!(
                "payload {} bytes exceeds max {}",
                payload.len(),
                self.max_payload_bytes
            )));
        }
        // Per-sink token-bucket budget — checked before the global buffer/byte
        // backstops (tightest, most specific gate first; globals remain outer
        // defense-in-depth). A bound port always has a seeded budget entry; a
        // miss is a host invariant violation. Charged only on acceptance, at
        // buffer-push time below.
        let sink_key = SinkKey::Port(port.clone());
        let sink_budget_mt = self
            .publish_budget_by_sink
            .get(&sink_key)
            .copied()
            .unwrap_or_else(|| panic!("host invariant: no sink budget for bound port {port:?}"));
        if sink_budget_mt < MILLITOKENS_PER_PUBLISH {
            *self.publish_suppressed_by_sink.entry(sink_key).or_insert(0) += 1;
            return Err(PublishError::QuotaExceeded);
        }
        // Per-activation successful-publish budget, shared with buffered async tool
        // requests (both flush as bus publishes) so the ceiling bounds the total.
        if self.publish_buffer.len() + self.tool_request_buffer.len()
            >= MAX_PUBLISHES_PER_ACTIVATION
        {
            return Err(PublishError::QuotaExceeded);
        }
        if self.published_bytes + payload.len() > MAX_PUBLISH_BYTES_PER_ACTIVATION {
            return Err(PublishError::QuotaExceeded);
        }
        use processor_bindings::brenn::processor::ports::Urgency as WitUrgency;
        let urgency = match guest_urgency {
            Some(WitUrgency::VeryLow) => ProcessorUrgency::VeryLow,
            Some(WitUrgency::Low) => ProcessorUrgency::Low,
            Some(WitUrgency::Normal) => ProcessorUrgency::Normal,
            Some(WitUrgency::High) => ProcessorUrgency::High,
            None => *port_default_urgency,
        };
        let channel_address = channel_address.clone();
        self.published_bytes += payload.len();
        self.publish_buffer.push(ProcessorPublish {
            port,
            channel_address,
            payload,
            urgency,
            reply_to: None,
        });
        // Charge the sink on acceptance. The entry existed and had sufficient budget
        // above; a vanished entry here is a host invariant violation — fail fast.
        let remaining = self
            .publish_budget_by_sink
            .get_mut(&sink_key)
            .expect("host invariant: sink budget entry vanished between check and charge");
        *remaining -= MILLITOKENS_PER_PUBLISH;
        Ok(())
    }

    /// Synchronous MQTT publish (the `mqtt.mqtt-publish` import).
    ///
    /// Unlike `do_publish` (buffered, flushed at activation end), this goes
    /// STRAIGHT to the broker via the injected `mqtt_publish` callback (which
    /// calls the shared `enforce_and_publish` pipeline in the binary crate) and
    /// returns the broker outcome inline. No store-and-forward (design §2.5,
    /// §3.1).
    ///
    /// Per-activation flood bound: shares the `publish_call_count` budget with
    /// `do_publish`, checked against `MAX_PUBLISH_CALLS_PER_ACTIVATION`
    /// — so a hostile guest cannot flood across the combined
    /// `ports.publish` + `mqtt-publish` surface (design §2.5; deliberately reuses
    /// the existing 512 call-count cap, no new MQTT-specific constant). The 256
    /// buffered-publish count does NOT apply here — nothing is buffered.
    fn do_mqtt_publish(
        &mut self,
        client: String,
        topic: String,
        payload: Vec<u8>,
        content_type: Option<String>,
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttPublishWitError> {
        // Per-activation total-call budget, shared with `do_publish`. Bumped and
        // checked first (anti-flood) — a failed call consumes budget too, exactly
        // as in `do_publish`.
        self.publish_call_count += 1;
        if self.publish_call_count > MAX_PUBLISH_CALLS_PER_ACTIVATION {
            return Err(MqttPublishWitError::QuotaExceeded);
        }

        // `client` is guest-controlled; cap + debug-escape before logging
        // (identical treatment to the guest-controlled `port` in `do_publish`).
        let display_client = cap_guest_for_log(&client);

        // Caller-owned input validation (design §2.2), mirroring the LLM
        // intercept's up-front checks so both call sites reject malformed guest
        // input as *permanent* `invalid-payload` rather than letting it fall
        // through to a *transient*-classified `broker` error downstream.
        //
        // qos range: the WIT type is `u8` (0..=255) but only 0/1/2 are valid.
        // Without this check an out-of-range qos reaches `publish_on_handle`,
        // which returns `MqttError::NotConnected` → mapped to `broker(...)` (the
        // documented MAY-retry variant) — a guest following the retry contract
        // would loop forever on an unfixable bug. Reject it here as permanent.
        if qos > 2 {
            return Err(MqttPublishWitError::InvalidPayload(format!(
                "invalid qos {qos}: must be 0, 1, or 2"
            )));
        }
        // Per-message payload size cap. The buffered `do_publish` path enforces
        // this (and a per-activation byte budget); the synchronous MQTT path must
        // also bound a single message so a hostile guest holding the `mqtt` grant
        // cannot emit arbitrary-size broker publishes. The per-activation byte
        // budget governs the buffer and is deliberately not applied here (nothing
        // is buffered, design §2.5), but the per-message cap is load-bearing and
        // matches the documented `invalid-payload` "oversize" semantics.
        if payload.len() > self.max_payload_bytes {
            return Err(MqttPublishWitError::InvalidPayload(format!(
                "payload {} bytes exceeds max {}",
                payload.len(),
                self.max_payload_bytes
            )));
        }

        // Per-sink token-bucket budget, keyed by client slug. Charged on attempt
        // (not refunded on a broker/not-permitted outcome): this path synchronously
        // reaches the broker pipeline, so a guest retry-looping on transient broker
        // errors must burn budget or the loop is unbounded within the 512-call cap.
        // A client with no bucket entry is by construction outside the ACL — the
        // existing ACL/NoConnector handling in the callback governs; no per-sink
        // counting for it (exactly like an unbound bus port).
        let sink_key = SinkKey::MqttClient(client.clone());
        if let Some(remaining) = self.publish_budget_by_sink.get_mut(&sink_key) {
            if *remaining < MILLITOKENS_PER_PUBLISH {
                *self.publish_suppressed_by_sink.entry(sink_key).or_insert(0) += 1;
                return Err(MqttPublishWitError::QuotaExceeded);
            }
            *remaining -= MILLITOKENS_PER_PUBLISH;
        }

        // The callback is `Some` iff the `Mqtt` capability is linked (the host fn
        // is unreachable otherwise). `.expect` is the structural backstop, like
        // the `kv_store` store-host impls — linker gating is the real gate.
        let cb = self
            .mqtt_publish
            .as_ref()
            .expect("mqtt-publish host fn invoked without an mqtt_publish callback (Mqtt grant linked but callback unset)");

        match cb(client, topic, payload, content_type, qos, retain) {
            MqttPublishOutcome::Ok => Ok(()),
            MqttPublishOutcome::NotPermitted => {
                // Security-relevant: an operator policy actively blocking a WASM
                // publish. Both callers must log the denial (design §3.3); the
                // LLM intercept logs the analogous warn!. Without this, a WASM
                // ACL deny would be invisible — a posture regression vs the LLM
                // path.
                warn!(
                    slug = %self.slug,
                    client = %display_client,
                    "wasm mqtt-publish: not-permitted — client outside mqtt_publish ACL (or mqtt grant absent)"
                );
                Err(MqttPublishWitError::NotPermitted)
            }
            MqttPublishOutcome::NoConnector => {
                // The MQTT service is absent (no `[[mqtt_client]]` declared on this
                // server), so the bootstrap closure's service-`None` short-circuit
                // returned this outcome (it logs its own distinct `warn!`). A
                // per-client resolution miss can no longer occur: every ACL-allowed
                // client is validated and a session registered at boot, so on a
                // validly-booted server with a declared client the publish always
                // resolves a session. Log a server-side trace so an on-call engineer
                // can diagnose the service-absent config. `info!` rather than
                // `warn!`: it is a config gap, not a security-relevant policy denial
                // like `not-permitted`.
                info!(
                    slug = %self.slug,
                    client = %display_client,
                    "wasm mqtt-publish: no-connector — MQTT service not configured on this server"
                );
                Err(MqttPublishWitError::NoConnector)
            }
            MqttPublishOutcome::InvalidPayload(reason) => {
                Err(MqttPublishWitError::InvalidPayload(reason))
            }
            MqttPublishOutcome::Broker(reason) => {
                // A broker connectivity failure (disconnect / submit / ack drop) on
                // the primary WASM egress path. The guest sees the typed `broker`
                // error, but a guest may silently retry or swallow it — without a
                // server-side signal a persistent broker outage affecting WASM
                // components would be invisible in Brenn logs (posture: real
                // observability; errhandling review). `warn!` with the consumer slug
                // + the capped/escaped reason. (The LLM path's broker-error logging
                // is a separate pre-existing gap, not addressed here.)
                warn!(
                    slug = %self.slug,
                    client = %display_client,
                    reason = %sanitize_untrusted_str(&reason, PROCESSOR_MAX_PORT_NAME_LOG_BYTES),
                    "wasm mqtt-publish: broker error"
                );
                Err(MqttPublishWitError::Broker(reason))
            }
            MqttPublishOutcome::BrokerRejected(reason) => {
                // The broker accepted the connection but rejected the publish
                // (PUBACK/PUBCOMP reason code != success). Same observability
                // rationale as the `Broker` arm — surface a server-side signal so a
                // persistent rejection (e.g. broker-side ACL) is diagnosable.
                warn!(
                    slug = %self.slug,
                    client = %display_client,
                    reason = %sanitize_untrusted_str(&reason, PROCESSOR_MAX_PORT_NAME_LOG_BYTES),
                    "wasm mqtt-publish: broker rejected publish"
                );
                Err(MqttPublishWitError::BrokerRejected(reason))
            }
        }
    }
}

impl processor_bindings::brenn::processor::ports::Host for ProcessorData {
    fn publish(&mut self, port: String, payload: String) -> Result<(), PublishError> {
        self.do_publish(port, payload, None)
    }

    fn publish_with_urgency(
        &mut self,
        port: String,
        payload: String,
        urgency: processor_bindings::brenn::processor::ports::Urgency,
    ) -> Result<(), PublishError> {
        self.do_publish(port, payload, Some(urgency))
    }
}

// --- mqtt::Host impl ---

impl processor_bindings::brenn::processor::mqtt::Host for ProcessorData {
    fn mqtt_publish(
        &mut self,
        client: String,
        topic: String,
        payload: Vec<u8>,
        content_type: Option<String>,
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttPublishWitError> {
        self.do_mqtt_publish(client, topic, payload, content_type, qos, retain)
    }
}

// --- tools host impls ---

/// Maximum length of a guest-supplied async `call-id`. Over-cap is
/// a permanent `invalid-args`.
const PROCESSOR_MAX_TOOL_CALL_ID_LEN: usize = 128;

/// Map a `ToolCallError` (host seam) to the WIT `tool-error` the guest observes,
/// logging the security-relevant denials keyed by slug.
///
/// `tool` is guest-controlled — cap + debug-escape before logging, like `port` in
/// `do_publish`. Denial detail strings are echoed back to the guest verbatim (the
/// caller already named the resource, so no information leak) but not logged.
fn tool_call_error_to_wit(e: ToolCallError, slug: &str, tool: &str) -> ToolWitError {
    match e {
        ToolCallError::NotGranted => {
            warn!(
                slug = %slug,
                tool = %cap_guest_for_log(tool),
                "wasm tool call: not-granted (unknown or ungranted tool)"
            );
            ToolWitError::NotGranted
        }
        ToolCallError::Denied(resource) => {
            warn!(
                slug = %slug,
                tool = %cap_guest_for_log(tool),
                "wasm tool call: denied — args named a resource outside the grant"
            );
            ToolWitError::Denied(resource)
        }
        ToolCallError::InvalidArgs(detail) => ToolWitError::InvalidArgs(detail),
        ToolCallError::RateLimited => ToolWitError::RateLimited,
        ToolCallError::WrongClass => ToolWitError::WrongClass,
        ToolCallError::Internal(token) => ToolWitError::Internal(token),
    }
}

impl ProcessorData {
    /// Synchronous fast-class tool call (the `tools.call-fast` import).
    ///
    /// Guest-facing bounds enforced here (brenn-wasm owns them): a per-activation
    /// call cap (anti-flood), the `args-json` byte cap, and the returned-result
    /// byte cap. The grant/ACL/class check and the per-tool time budget live behind
    /// the `ToolHost` seam (they need the native registry + policy). A tool bug
    /// (over-cap result) alerts; a guest flood (call-count overrun) warns — the same
    /// split the design draws.
    fn do_fast_call(&mut self, tool: String, args_json: String) -> Result<String, ToolWitError> {
        // Per-activation call cap. Bumped first (a rejected call still consumes the
        // slot, exactly like `publish_call_count`). Only the first over-cap call
        // warns, to bound one warn per activation (the publish/log-quota idiom).
        self.tool_call_count += 1;
        if self.tool_call_count > PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION {
            if self.tool_call_count == PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION + 1 {
                warn!(
                    slug = %self.slug,
                    cap = PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION,
                    "wasm fast tool call: per-activation call cap exceeded; rejecting"
                );
            }
            return Err(ToolWitError::Internal("call-count-exceeded".to_string()));
        }
        // Guest-controlled args blob: cap before handing to the tool.
        if args_json.len() > PROCESSOR_MAX_TOOL_ARGS_BYTES {
            return Err(ToolWitError::InvalidArgs(format!(
                "args {} bytes exceeds max {}",
                args_json.len(),
                PROCESSOR_MAX_TOOL_ARGS_BYTES
            )));
        }
        // The seam is `Some` iff the `Tools` capability is linked (the host fn is
        // unreachable otherwise). `.expect` is the structural backstop, like the
        // `mqtt_publish` / `kv_store` host impls — linker gating is the real gate.
        let host = self.tool_host.as_ref().expect(
            "call-fast host fn invoked without a tool_host (Tools grant linked but host unset)",
        );
        match host.fast_call(&tool, &args_json) {
            Ok(result) => {
                // Over-cap result is a *tool bug*: alert + `internal`. The tool must
                // not hand a guest an unbounded blob.
                if result.len() > PROCESSOR_MAX_FAST_TOOL_RESULT_BYTES {
                    self.alerter.alert(
                        GuestAlertSeverity::Warning,
                        "fast tool result over cap",
                        &format!(
                            "fast tool {} returned {} bytes (cap {})",
                            cap_guest_for_log(&tool),
                            result.len(),
                            PROCESSOR_MAX_FAST_TOOL_RESULT_BYTES
                        ),
                    );
                    return Err(ToolWitError::Internal("result-too-large".to_string()));
                }
                Ok(result)
            }
            Err(e) => Err(tool_call_error_to_wit(e, &self.slug, &tool)),
        }
    }

    /// Validate + buffer an async-class tool call (the `tools.call-async` import).
    ///
    /// The grant/ACL/class check runs synchronously via the seam so caller mistakes
    /// fail fast; on success the resolved request is buffered for the activation's
    /// transactional flush (increment 6). Bounds enforced here: the `call-id` length
    /// cap, the `args-json` byte cap, and the per-activation request-buffer cap
    /// (async requests flush as bus publishes, so they share the publish-count
    /// ceiling).
    fn do_queue_async(
        &mut self,
        tool: String,
        args_json: String,
        call_id: &str,
    ) -> Result<(), ToolWitError> {
        // Per-activation call cap, shared with `do_fast_call`. Bumped first so a
        // *rejected* async call still consumes a slot (grant/ACL/parse failures go
        // through the seam and never reach the request buffer, so the buffer cap
        // below cannot bound them); without this a guest could loop failing
        // `call-async` — each a full host-side JSON parse + ACL walk — unbounded
        // within one activation. Only the first over-cap call warns (the
        // publish/log-quota idiom).
        self.tool_call_count += 1;
        if self.tool_call_count > PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION {
            if self.tool_call_count == PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION + 1 {
                warn!(
                    slug = %self.slug,
                    cap = PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION,
                    "wasm async tool call: per-activation call cap exceeded; rejecting"
                );
            }
            return Err(ToolWitError::Internal("call-count-exceeded".to_string()));
        }
        // Per-activation cap: buffered async requests become bus publishes at flush,
        // so they share the one per-activation publish ceiling with port publishes
        // (`do_publish` counts both buffers against the same bound). This keeps the
        // total per-activation bus writes bounded by a single constant.
        if self.publish_buffer.len() + self.tool_request_buffer.len()
            >= MAX_PUBLISHES_PER_ACTIVATION
        {
            return Err(ToolWitError::Internal(
                "too-many-async-requests".to_string(),
            ));
        }
        if call_id.len() > PROCESSOR_MAX_TOOL_CALL_ID_LEN {
            return Err(ToolWitError::InvalidArgs(format!(
                "call_id {} bytes exceeds max {}",
                call_id.len(),
                PROCESSOR_MAX_TOOL_CALL_ID_LEN
            )));
        }
        if args_json.len() > PROCESSOR_MAX_TOOL_ARGS_BYTES {
            return Err(ToolWitError::InvalidArgs(format!(
                "args {} bytes exceeds max {}",
                args_json.len(),
                PROCESSOR_MAX_TOOL_ARGS_BYTES
            )));
        }
        let host = self.tool_host.as_ref().expect(
            "call-async host fn invoked without a tool_host (Tools grant linked but host unset)",
        );
        match host.queue_async(&tool, &args_json, call_id) {
            Ok(request) => {
                // Buffered here; `take_ok_publishes` appends it to the activation's
                // publish flush iff the guest returns Ok. A trap/err drops the buffer
                // with the store, so no request is published.
                self.tool_request_buffer.push(request);
                Ok(())
            }
            Err(e) => Err(tool_call_error_to_wit(e, &self.slug, &tool)),
        }
    }

    /// Drain the activation's buffered port publishes and buffered async tool
    /// requests into one publish batch for the transactional flush.
    ///
    /// Called only on a guest `Ok` outcome. Each async tool request becomes a
    /// publish to its resolved `brenn:tools/<tool>` request channel carrying the
    /// caller's result inbox as `reply_to`; ordinary port publishes keep
    /// `reply_to: None`. On `Err`/`Trap` this is never called and both buffers drop
    /// with the store — the trap-discard guarantee.
    fn take_ok_publishes(&mut self) -> Vec<ProcessorPublish> {
        let mut publishes = std::mem::take(&mut self.publish_buffer);
        for req in std::mem::take(&mut self.tool_request_buffer) {
            publishes.push(ProcessorPublish {
                port: TOOL_REQUEST_LOG_PORT.to_string(),
                channel_address: req.channel,
                payload: req.body_json,
                urgency: ProcessorUrgency::Normal,
                reply_to: Some(req.reply_to),
            });
        }
        publishes
    }
}

impl processor_bindings::brenn::processor::tools::Host for ProcessorData {
    fn call_fast(&mut self, tool: String, args_json: String) -> Result<String, ToolWitError> {
        self.do_fast_call(tool, args_json)
    }

    fn call_async(
        &mut self,
        tool: String,
        args_json: String,
        call_id: String,
    ) -> Result<(), ToolWitError> {
        self.do_queue_async(tool, args_json, &call_id)
    }
}

// --- store::Host impl ---

impl processor_bindings::brenn::processor::store::Host for ProcessorData {
    fn begin(&mut self) -> Result<Resource<Transaction>, ProcessorStoreError> {
        // Borrow kv_store field directly (not via self.kv_store()) so the borrow
        // checker sees a disjoint field borrow from &mut self.resource_table.
        let kv = self
            .kv_store
            .as_ref()
            .expect("store host invoked without store grant — linker gating broken");
        store::host_begin(&mut self.resource_table, kv).map_err(|e| {
            // Log processor-specific context (slug, store_path) on BEGIN failure.
            // host_begin already cleared tx_active on the error path.
            // Skip logging on the nested-begin guard ("nested transaction not supported")
            // — that is a guest-side contract violation, not a host storage error.
            if let StoreError::Backend(ref detail) = e
                && detail != "nested transaction not supported"
            {
                error!(
                    slug = %self.slug,
                    store_path = %kv.path().display(),
                    err = %detail,
                    "WASM processor store BEGIN IMMEDIATE failed"
                );
            }
            store_error_to_processor(e)
        })
    }
}

// --- store::HostTransaction impl ---

impl processor_bindings::brenn::processor::store::HostTransaction for ProcessorData {
    fn get(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ProcessorStoreError> {
        let kv = self.kv_for_tx(&tx, "get");
        store::tx_get(&kv, &namespace, &key)
            .map_err(|e| log_and_convert_store_error(&self.slug, "get", e))
    }

    fn put(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ProcessorStoreError> {
        let kv = self.kv_for_tx(&tx, "put");
        store::tx_put(&kv, &namespace, &key, &value)
            .map_err(|e| log_and_convert_store_error(&self.slug, "put", e))
    }

    fn delete(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<(), ProcessorStoreError> {
        let kv = self.kv_for_tx(&tx, "delete");
        store::tx_delete(&kv, &namespace, &key)
            .map_err(|e| log_and_convert_store_error(&self.slug, "delete", e))
    }

    fn scan(
        &mut self,
        tx: Resource<Transaction>,
        namespace: String,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<store::KvPairs, ProcessorStoreError> {
        let kv = self.kv_for_tx(&tx, "scan");
        store::tx_scan(&kv, &namespace, &start, end.as_deref(), limit)
            .map_err(|e| log_and_convert_store_error(&self.slug, "scan", e))
    }

    fn commit(&mut self, tx: Resource<Transaction>) -> Result<(), ProcessorStoreError> {
        store::host_commit(&mut self.resource_table, &tx, &self.slug)
            .map_err(store_error_to_processor)
    }

    fn rollback(&mut self, tx: Resource<Transaction>) {
        store::host_rollback(&mut self.resource_table, &tx, &self.slug)
    }

    fn drop(&mut self, tx: Resource<Transaction>) -> wasmtime::Result<()> {
        store::host_drop(&mut self.resource_table, tx)
    }
}

// --- log::Host impl ---

impl processor_bindings::brenn::processor::log::Host for ProcessorData {
    fn log(&mut self, level: processor_bindings::brenn::processor::log::Level, message: String) {
        self.log_call_count += 1;
        if self.log_call_count > PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION {
            // Quota exceeded — drop and count. Suppressed count surfaced post-activation.
            return;
        }
        // Sanitize inside the macro field expression so the work is skipped when the
        // level is filtered out by the global subscriber (trace/debug in production).
        use processor_bindings::brenn::processor::log::Level;
        match level {
            Level::Trace => {
                trace!(target: "wasm_guest", slug = %self.slug, message = %sanitize_untrusted_str(&message, PROCESSOR_MAX_LOG_MESSAGE_BYTES), "wasm guest log")
            }
            Level::Debug => {
                debug!(target: "wasm_guest", slug = %self.slug, message = %sanitize_untrusted_str(&message, PROCESSOR_MAX_LOG_MESSAGE_BYTES), "wasm guest log")
            }
            Level::Info => {
                info!(target: "wasm_guest", slug = %self.slug, message = %sanitize_untrusted_str(&message, PROCESSOR_MAX_LOG_MESSAGE_BYTES), "wasm guest log")
            }
            Level::Warn => {
                warn!(target: "wasm_guest", slug = %self.slug, message = %sanitize_untrusted_str(&message, PROCESSOR_MAX_LOG_MESSAGE_BYTES), "wasm guest log")
            }
            Level::Error => {
                error!(target: "wasm_guest", slug = %self.slug, message = %sanitize_untrusted_str(&message, PROCESSOR_MAX_LOG_MESSAGE_BYTES), "wasm guest log")
            }
        }
    }
}

// --- alert::Host impl ---

impl processor_bindings::brenn::processor::alert::Host for ProcessorData {
    fn alert(
        &mut self,
        severity: processor_bindings::brenn::processor::alert::Severity,
        title: String,
        body: String,
    ) {
        self.alert_call_count += 1;
        if self.alert_call_count > PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION {
            // Quota exceeded — drop and count. Suppressed count surfaced post-activation.
            return;
        }
        let title = sanitize_untrusted_str(&title, PROCESSOR_MAX_ALERT_TITLE_BYTES);
        let body = sanitize_untrusted_str(&body, PROCESSOR_MAX_ALERT_BODY_BYTES);
        use processor_bindings::brenn::processor::alert::Severity;
        let guest_severity = match severity {
            Severity::Info => GuestAlertSeverity::Info,
            Severity::Warning => GuestAlertSeverity::Warning,
            Severity::Critical => GuestAlertSeverity::Critical,
        };
        self.alerter.alert(guest_severity, &title, &body);
    }
}

// --- config::Host impl (processor world) ---

impl processor_bindings::brenn::processor::config::Host for ProcessorData {
    fn get(&mut self, key: String) -> Option<String> {
        self.config.get(&key).cloned()
    }
}

/// Map a `StoreError` (replay world) to `ProcessorStoreError` (processor world).
///
/// `StoreError::Backend` carries raw rusqlite/SQLite diagnostics; these must NOT
/// cross the external WIT boundary (information-disclosure risk, implicit API
/// stability commitment). Callers are responsible for logging the detail before
/// calling this function. The guest receives the stable opaque token `"storage
/// error"` instead.
fn store_error_to_processor(e: StoreError) -> ProcessorStoreError {
    match e {
        StoreError::Contention => ProcessorStoreError::Contention,
        StoreError::Backend(_) => ProcessorStoreError::Backend("storage error".to_string()),
        StoreError::QuotaExceeded => ProcessorStoreError::QuotaExceeded,
    }
}

/// Map a `StoreError` (replay world) to `ProcessorStoreError` (processor world),
/// logging the host-side detail before sanitizing.
///
/// Used by `get/put/delete/scan` where rusqlite errors flow through `StoreError::Backend`
/// and the slug/op context from the call site must be logged host-side.
fn log_and_convert_store_error(slug: &str, op: &str, e: StoreError) -> ProcessorStoreError {
    if let StoreError::Backend(ref detail) = e {
        error!(
            slug = slug,
            op = op,
            detail = detail,
            "WASM processor store operation failed"
        );
    }
    store_error_to_processor(e)
}

// ---------------------------------------------------------------------------
// Capability map (§3.1) — single source of truth for grant ↔ WIT interface
// ---------------------------------------------------------------------------

/// Grantable capability interfaces of `world processor`.
///
/// One source of truth used by the linker gating (§3.3) and the import-reflection
/// check (§3.4); they share this table so they cannot drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Capability {
    Ports,
    Store,
    Log,
    Alert,
    Config,
    Mqtt,
    Tools,
}

/// Types-only import every component carries; never a grant, always permitted.
///
/// Matched with the same semver rule as capabilities (see `Capability::from_import_name`).
/// Public for drift-guard tests.
pub fn is_types_import(name: &str) -> bool {
    semver_compat_match(name, "brenn:processor/types@0.1.0")
}

/// The fully qualified WIT interface names a component artifact imports, with any
/// `@version` suffix stripped — the component's **import profile**.
///
/// This is the in-process twin of what the surface asset build extracts with
/// `wasm-tools component wit` when it writes a transpiled kind's `manifest.json`
/// (`surface/emit-processor-manifest.sh`): same names, same version-stripping, so
/// a caller can ask an artifact what it imports without shelling out to the
/// toolchain. The name stays fully qualified (`brenn:processor/store`, never
/// `store`) because the package namespace is load-bearing — a foreign
/// `wasi:logging/log` must not be able to masquerade as the host `log`.
///
/// # Panics
///
/// If the artifact cannot be read or is not a valid component.
pub fn processor_component_imports(component_path: &Path) -> Vec<String> {
    let engine = Engine::new(&Config::new())
        .unwrap_or_else(|e| panic!("failed to initialize wasmtime engine for import listing: {e}"));
    let component = Component::from_file(&engine, component_path).unwrap_or_else(|e| {
        panic!(
            "failed to load component from {} for import listing: {e}\n\
             Rebuild with: make wasm-components",
            component_path.display()
        )
    });
    normalize_import_names(
        component
            .component_type()
            .imports(&engine)
            .map(|(name, _)| name),
    )
}

/// Version-strip, sort, and dedup a component's raw import names into the
/// manifest profile shape.
///
/// Sorting is by byte order, which `surface/emit-processor-manifest.sh` matches
/// with `LC_ALL=C sort -u`; the two must agree or the parity pin between them is
/// environment-dependent.
fn normalize_import_names<'a>(raw: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut names: Vec<String> = raw.map(|n| strip_import_version(n).to_string()).collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// Drop a WIT import name's `@version` suffix, keeping the fully-qualified
/// package+interface portion (`brenn:processor/store@0.1.0` →
/// `brenn:processor/store`).
///
/// Splits at the *first* `@`: a legal WIT name carries at most one, after the
/// interface segment, so there is no second occurrence to preserve.
fn strip_import_version(name: &str) -> &str {
    name.split_once('@').map_or(name, |(base, _)| base)
}

/// Return `true` when `actual` is a semver-compatible version of `canonical`.
///
/// Mirrors wasmtime's own linker resolution (vendored wasmtime-45.0.1,
/// `wasmtime-environ-45.0.1/src/component/names.rs:293-320` — `alternate_lookup_key`):
/// - For `0.0.z`: must be identical.
/// - For `0.minor.*`: same major (0) and same minor.
/// - For `>=1.x.*`: same major only.
///
/// Package+interface portion (before `@`) must be identical.
///
/// Uses `semver::Version` for parsing; build metadata is explicitly stripped
/// before parsing because wasmtime's `alternate_lookup_key` ignores it when
/// resolving imports. Prerelease versions are rejected — wasmtime refuses them.
pub fn semver_compat_match(actual: &str, canonical: &str) -> bool {
    // Split off version suffix: "brenn:processor/ports@0.1.0" → ("brenn:processor/ports", "0.1.0")
    let (actual_iface, actual_ver) = match actual.rsplit_once('@') {
        Some(pair) => pair,
        None => return actual == canonical,
    };
    let (canon_iface, canon_ver) = match canonical.rsplit_once('@') {
        Some(pair) => pair,
        None => return actual == canonical,
    };
    if actual_iface != canon_iface {
        return false;
    }
    // Build metadata (e.g. `+abc`) is stripped before parsing — wasmtime's own
    // alternate_lookup_key ignores build metadata when resolving imports.
    let av_str = actual_ver
        .split_once('+')
        .map(|(base, _)| base)
        .unwrap_or(actual_ver);
    let cv_str = canon_ver
        .split_once('+')
        .map(|(base, _)| base)
        .unwrap_or(canon_ver);

    let av = match semver::Version::parse(av_str) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let cv = match semver::Version::parse(cv_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    // Reject prerelease versions (incompatible per wasmtime rule).
    if !av.pre.is_empty() || !cv.pre.is_empty() {
        return false;
    }

    // Rule selection is driven by the canonical (host-registered) version, mirroring
    // wasmtime's linker which resolves against the host-provided name.
    if cv.major >= 1 {
        // ≥1.0.0: same major only.
        av.major == cv.major
    } else if cv.minor > 0 {
        // 0.minor.*: same major (must both be 0) and same minor.
        av.major == cv.major && av.minor == cv.minor
    } else {
        // 0.0.z: exact version.
        av.major == cv.major && av.minor == cv.minor && av.patch == cv.patch
    }
}

impl Capability {
    /// All grantable capabilities in a stable order.
    pub const ALL: [Capability; 7] = [
        Capability::Ports,
        Capability::Store,
        Capability::Log,
        Capability::Alert,
        Capability::Config,
        Capability::Mqtt,
        Capability::Tools,
    ];

    /// Operator-facing grant name (`"ports"`, `"store"`, …).
    pub fn grant_name(self) -> &'static str {
        match self {
            Capability::Ports => "ports",
            Capability::Store => "store",
            Capability::Log => "log",
            Capability::Alert => "alert",
            Capability::Config => "config",
            Capability::Mqtt => "mqtt",
            Capability::Tools => "tools",
        }
    }

    /// Canonical host-side import key at the host's WIT version.
    ///
    /// Used for diagnostics and the drift-guard test (§8 item 8).
    /// Semver-compatible matching (not exact-string) is used in
    /// `from_import_name` — see `semver_compat_match`.
    pub fn import_name(self) -> &'static str {
        match self {
            Capability::Ports => "brenn:processor/ports@0.1.0",
            Capability::Store => "brenn:processor/store@0.1.0",
            Capability::Log => "brenn:processor/log@0.1.0",
            Capability::Alert => "brenn:processor/alert@0.1.0",
            Capability::Config => "brenn:processor/config@0.1.0",
            Capability::Mqtt => "brenn:processor/mqtt@0.1.0",
            Capability::Tools => "brenn:processor/tools@0.1.0",
        }
    }

    /// Match an import key to a capability using semver-compatible version matching.
    ///
    /// Returns `None` for unrecognized or structurally wrong names.
    pub fn from_import_name(name: &str) -> Option<Capability> {
        Capability::ALL
            .iter()
            .find(|cap| semver_compat_match(name, cap.import_name()))
            .copied()
    }

    /// Dispatch to the bindgen-generated `add_to_linker` for this interface.
    fn add_to_linker(self, linker: &mut Linker<ProcessorData>) -> wasmtime::Result<()> {
        match self {
            Capability::Ports => processor_bindings::brenn::processor::ports::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Store => processor_bindings::brenn::processor::store::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Log => processor_bindings::brenn::processor::log::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Alert => processor_bindings::brenn::processor::alert::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Config => processor_bindings::brenn::processor::config::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Mqtt => processor_bindings::brenn::processor::mqtt::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
            Capability::Tools => processor_bindings::brenn::processor::tools::add_to_linker::<
                _,
                HasSelf<ProcessorData>,
            >(linker, |d| d),
        }
    }
}

// ---------------------------------------------------------------------------

/// Load spec for `ProcessorComponent::load`.
pub struct ProcessorLoadSpec<'a> {
    pub component_path: &'a Path,
    pub slug: &'a str,
    /// Logical port name → resolved binding + per-sink publish budget.
    pub output_ports: HashMap<String, OutputPortSpec>,
    /// Logical input port name → publish amplification in millitokens. Every
    /// bound input port must appear (windows are built from the same config);
    /// a missing entry for a driven window is a host invariant violation.
    pub input_amplification_mt: HashMap<String, u64>,
    /// ACL-allowed MQTT client slug → per-sink publish budget. Empty when the
    /// component has no MQTT publish ACL.
    pub mqtt_sinks: HashMap<String, SinkBudget>,
    /// Operator-supplied config map for this component. Pass an empty map for
    /// components that read no config.
    pub config: HashMap<String, String>,
    /// Capability interfaces to link for this component (deny-by-default).
    /// Only listed capabilities are added to the linker; all others are absent.
    /// Validated by the caller (bootstrap config cross-validation) and re-asserted
    /// in `load` via `enforce_grants`.
    pub grants: std::collections::BTreeSet<Capability>,
    /// Path to the per-component SQLite KV store. `Some` iff the component has
    /// the `Store` grant; `None` = no store linked. The caller (bootstrap) enforces
    /// the invariant; `load` re-asserts via `assert!` (fires in both debug and release).
    pub store_path: Option<&'a Path>,
    pub max_page_count: u32,
    pub max_payload_bytes: usize,
    /// Host callback for guest-originated alerts (via the `alert` WIT interface).
    /// Required — no `Option`: when alerting is unconfigured, pass a no-op impl.
    pub alerter: Arc<dyn ProcessorAlerter>,
    /// Output-channel ACL. Called in `do_publish` with the resolved channel
    /// address after the port→channel lookup. Required — no `Option`; pass an
    /// allow-all closure (`Arc::new(|_| true)`) for callers with no policy.
    pub output_acl: OutputAclFn,
    /// Host callback for synchronous MQTT egress (the `mqtt` interface). `Some`
    /// iff the component has the `Mqtt` grant; `None` = no MQTT egress linked.
    /// The caller (bootstrap) enforces the invariant; the host fn is unreachable
    /// when the `Mqtt` capability is not linked, so `None` is a structural
    /// backstop.
    pub mqtt_publish: Option<MqttPublishFn>,
    /// Host seam for the `tools` interface (fast execution + async validation).
    /// `Some` iff the component has the `Tools` grant; `None` = no tool surface.
    /// The caller (bootstrap) enforces the invariant; the host fns are unreachable
    /// when the `Tools` capability is not linked, so `None` is a structural
    /// backstop.
    pub tool_host: Option<ToolHostFn>,
}

/// Loaded, pre-linked processor component for bus consumption and publication.
///
/// `ProcessorPre<ProcessorData>` is built once in `load`; per-call cost is
/// instantiation + store-build + the single `receive` WIT call.
pub struct ProcessorComponent {
    engine: Engine,
    processor_pre: ProcessorPre<ProcessorData>,
    /// `None` when the component does not have the `Store` grant.
    kv_store: Option<Arc<KvStore>>,
    /// Logical port name → resolved binding + per-sink publish budget.
    output_ports: Arc<HashMap<String, OutputPortSpec>>,
    /// Logical input port name → publish amplification in millitokens.
    input_amplification_mt: Arc<HashMap<String, u64>>,
    /// ACL-allowed MQTT client slug → per-sink publish budget.
    mqtt_sinks: Arc<HashMap<String, SinkBudget>>,
    /// Persistent per-sink token carryover between activations (millitokens).
    /// Activations for one component are serialized by the dispatch loop; the guard
    /// is held across the whole activation (`lock_carry_for_activation`), so a
    /// concurrent activation panics on `try_lock` rather than corrupting the bucket.
    publish_carry: std::sync::Mutex<HashMap<SinkKey, u64>>,
    config: Arc<HashMap<String, String>>,
    slug: Arc<str>,
    max_payload_bytes: usize,
    alerter: Arc<dyn ProcessorAlerter>,
    output_acl: OutputAclFn,
    /// `None` when the component lacks the `Mqtt` grant (the `mqtt` interface is
    /// then unlinked and the host fn unreachable).
    mqtt_publish: Option<MqttPublishFn>,
    /// `None` when the component lacks the `Tools` grant (the `tools` interface is
    /// then unlinked and the host fns unreachable).
    tool_host: Option<ToolHostFn>,
}

impl ProcessorComponent {
    /// Load a processor component from a `.wasm` artifact at `spec.component_path`.
    ///
    /// Builds a dedicated wasmtime `Engine` with fuel consumption AND epoch
    /// interruption enabled. Links only the capability interfaces listed in
    /// `spec.grants` (deny-by-default); unlisted interfaces are absent from the
    /// linker — `instantiate_pre` will reject any component that imports them.
    /// Runs `enforce_grants` before `instantiate_pre` to surface the named
    /// capability diagnostic rather than a raw wasmtime error.
    /// Spawns one process-lifetime epoch ticker thread per component.
    /// Panics on any failure — per backend robustness rules; a
    /// missing/unloadable component at bootstrap is a fatal config error.
    pub fn load(spec: ProcessorLoadSpec<'_>) -> Self {
        let mut cfg = Config::new();
        cfg.consume_fuel(true);
        cfg.epoch_interruption(true);
        let engine = Engine::new(&cfg)
            .unwrap_or_else(|e| panic!("failed to initialize wasmtime engine for processor: {e}"));

        // Conditional linking: only granted capabilities are added to the linker.
        // Deny-by-default is structural — an ungranted import makes instantiate_pre fail.
        let mut linker: Linker<ProcessorData> = Linker::new(&engine);
        for cap in &spec.grants {
            cap.add_to_linker(&mut linker).unwrap_or_else(|e| {
                panic!(
                    "failed to add {} to processor linker: {e}",
                    cap.grant_name()
                )
            });
        }

        // Invariant: store_path and Store grant must agree.
        // Bootstrap config validation enforces this; re-asserted here because
        // load() is a public API callable by out-of-tree hosts that bypass config-layer
        // cross-validation.
        assert_eq!(
            spec.store_path.is_some(),
            spec.grants.contains(&Capability::Store),
            "component {}: store_path ({}) and Store grant ({}) must both be set or both absent",
            spec.slug,
            if spec.store_path.is_some() {
                "Some"
            } else {
                "None"
            },
            if spec.grants.contains(&Capability::Store) {
                "granted"
            } else {
                "not granted"
            },
        );

        // Invariant: the MQTT-publish callback and the Mqtt grant must agree.
        // Bootstrap wires the callback iff it grants Mqtt; re-asserted here because
        // load() is a public API callable by out-of-tree hosts that bypass the
        // config-layer cross-validation. Without the callback a linked `mqtt`
        // interface would `.expect`-panic at the first guest call.
        assert_eq!(
            spec.mqtt_publish.is_some(),
            spec.grants.contains(&Capability::Mqtt),
            "component {}: mqtt_publish callback ({}) and Mqtt grant ({}) must both be set or both absent",
            spec.slug,
            if spec.mqtt_publish.is_some() {
                "Some"
            } else {
                "None"
            },
            if spec.grants.contains(&Capability::Mqtt) {
                "granted"
            } else {
                "not granted"
            },
        );

        // Invariant: the tool-host seam and the Tools grant must agree. Same
        // rationale as the mqtt assert — a linked `tools` interface without a host
        // would `.expect`-panic at the first guest call; re-asserted here because
        // `load` is a public API callable by out-of-tree hosts that bypass the
        // config-layer cross-validation.
        assert_eq!(
            spec.tool_host.is_some(),
            spec.grants.contains(&Capability::Tools),
            "component {}: tool_host seam ({}) and Tools grant ({}) must both be set or both absent",
            spec.slug,
            if spec.tool_host.is_some() {
                "Some"
            } else {
                "None"
            },
            if spec.grants.contains(&Capability::Tools) {
                "granted"
            } else {
                "not granted"
            },
        );

        let component = Component::from_file(&engine, spec.component_path).unwrap_or_else(|e| {
            panic!(
                "failed to load processor component from {}: {e}\n\
                     Rebuild with: make wasm-components",
                spec.component_path.display()
            )
        });

        // Import-reflection check: name each capability violation before
        // instantiate_pre's opaque error (§3.4). instantiate_pre remains as
        // the structural backstop (defense in depth).
        Self::enforce_grants(spec.slug, &engine, &component, &spec.grants);

        let instance_pre = linker.instantiate_pre(&component).unwrap_or_else(|e| {
            panic!(
                "processor component pre-instantiation failed — import not linked \
                 (grant missing or WIT namespace mismatch): {e}"
            )
        });
        let processor_pre = ProcessorPre::new(instance_pre).unwrap_or_else(|e| {
            panic!(
                "processor component pre-instantiation failed — export type-check failed \
                 (stale .wasm or WIT signature changed): {e}"
            )
        });

        let kv_store: Option<Arc<KvStore>> = spec
            .store_path
            .map(|p| KvStore::open(p, spec.max_page_count));

        // Spawn an epoch ticker thread for this engine.
        // Uses an EngineWeak so the thread exits naturally when ProcessorComponent
        // is dropped (engine is the only strong ref). N consumers = N ticker threads;
        // N is small (single digits), acceptable.
        {
            let engine_weak: EngineWeak = engine.weak();
            std::thread::Builder::new()
                .name(format!("wasm-epoch-{}", spec.slug))
                .spawn(move || {
                    loop {
                        std::thread::sleep(std::time::Duration::from_millis(
                            PROCESSOR_EPOCH_TICK_MS,
                        ));
                        match engine_weak.upgrade() {
                            Some(e) => e.increment_epoch(),
                            None => break, // ProcessorComponent dropped; exit ticker.
                        }
                    }
                })
                .unwrap_or_else(|e| {
                    panic!("failed to spawn epoch ticker thread for {}: {e}", spec.slug)
                });
        }

        // Persistent carryover starts at 0 for every sink (each output port and
        // each MQTT client slug). Missing keys read as 0, but seeding them keeps
        // the map self-describing.
        let mut publish_carry: HashMap<SinkKey, u64> = HashMap::new();
        for port in spec.output_ports.keys() {
            publish_carry.insert(SinkKey::Port(port.clone()), 0);
        }
        for client in spec.mqtt_sinks.keys() {
            publish_carry.insert(SinkKey::MqttClient(client.clone()), 0);
        }

        Self {
            engine,
            processor_pre,
            kv_store,
            output_ports: Arc::new(spec.output_ports),
            input_amplification_mt: Arc::new(spec.input_amplification_mt),
            mqtt_sinks: Arc::new(spec.mqtt_sinks),
            publish_carry: std::sync::Mutex::new(publish_carry),
            config: Arc::new(spec.config),
            slug: Arc::from(spec.slug),
            max_payload_bytes: spec.max_payload_bytes,
            alerter: spec.alerter,
            output_acl: spec.output_acl,
            mqtt_publish: spec.mqtt_publish,
            tool_host: spec.tool_host,
        }
    }

    /// Reflect the component's declared imports against the grant set.
    ///
    /// Called at load, between `Component::from_file` and `linker.instantiate_pre`,
    /// so capability problems surface with a named diagnostic rather than a raw
    /// wasmtime error. `instantiate_pre` remains as the structural backstop.
    ///
    /// Panics listing ALL violations so the operator can fix in one pass.
    fn enforce_grants(
        slug: &str,
        engine: &Engine,
        component: &Component,
        grants: &std::collections::BTreeSet<Capability>,
    ) {
        let component_type = component.component_type();
        let mut violations: Vec<String> = Vec::new();
        let mut imported_caps: Vec<Capability> = Vec::new();

        for (name, _item) in component_type.imports(engine) {
            if is_types_import(name) {
                // types-only import: always permitted. Structural validation of the
                // item kind (instance vs function vs value) is deferred to
                // `instantiate_pre` — a structurally-wrong types import would produce
                // an opaque wasmtime error there, which is acceptable given the
                // types interface carries no functions in any current WIT version.
            } else if let Some(cap) = Capability::from_import_name(name) {
                if grants.contains(&cap) {
                    imported_caps.push(cap);
                } else {
                    violations.push(format!(
                        "component {slug:?} requires ungranted capability {cap_name:?}",
                        cap_name = cap.grant_name(),
                    ));
                }
            } else {
                violations.push(format!(
                    "component {slug:?} imports unrecognized interface {name:?}",
                ));
            }
        }

        if !violations.is_empty() {
            panic!(
                "WASM component grant check failed:\n{}",
                violations.join("\n")
            );
        }

        // Observability: log granted set and actually-imported capability set.
        let granted_names: Vec<&str> = grants.iter().map(|c| c.grant_name()).collect();
        let imported_names: Vec<&str> = imported_caps.iter().map(|c| c.grant_name()).collect();
        info!(
            slug = slug,
            granted = ?granted_names,
            imported = ?imported_names,
            "WASM component grant check passed",
        );
    }

    /// Seed the per-sink activation budget map from persistent carryover plus
    /// this activation's fill and input grant. One entry per output port and per
    /// MQTT client sink.
    fn seed_publish_budgets(
        &self,
        carry: &HashMap<SinkKey, u64>,
        grant_input_mt: u64,
    ) -> HashMap<SinkKey, u64> {
        let mut budgets: HashMap<SinkKey, u64> =
            HashMap::with_capacity(self.output_ports.len() + self.mqtt_sinks.len());
        for (port, spec) in self.output_ports.iter() {
            let key = SinkKey::Port(port.clone());
            let carry_mt = carry.get(&key).copied().unwrap_or(0);
            let seeded = seed_sink_budget(carry_mt, spec.budget, grant_input_mt);
            budgets.insert(key, seeded);
        }
        for (client, budget) in self.mqtt_sinks.iter() {
            let key = SinkKey::MqttClient(client.clone());
            let carry_mt = carry.get(&key).copied().unwrap_or(0);
            let seeded = seed_sink_budget(carry_mt, *budget, grant_input_mt);
            budgets.insert(key, seeded);
        }
        budgets
    }

    fn make_store(
        &self,
        total_envelope_count: usize,
        publish_budget_by_sink: HashMap<SinkKey, u64>,
    ) -> Store<ProcessorData> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(PROCESSOR_MAX_MEMORY_BYTES)
            .table_elements(PROCESSOR_MAX_TABLE_ELEMENTS)
            .instances(PROCESSOR_MAX_INSTANCES)
            .tables(PROCESSOR_MAX_TABLES)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            ProcessorData {
                resource_table: ResourceTable::new(),
                limits,
                kv_store: self.kv_store.clone(),
                output_ports: Arc::clone(&self.output_ports),
                config: Arc::clone(&self.config),
                slug: Arc::clone(&self.slug),
                max_payload_bytes: self.max_payload_bytes,
                publish_buffer: Vec::new(),
                published_bytes: 0,
                publish_call_count: 0,
                publish_budget_by_sink,
                publish_suppressed_by_sink: HashMap::new(),
                alerter: Arc::clone(&self.alerter),
                output_acl: Arc::clone(&self.output_acl),
                mqtt_publish: self.mqtt_publish.clone(),
                log_call_count: 0,
                alert_call_count: 0,
                tool_host: self.tool_host.clone(),
                tool_request_buffer: Vec::new(),
                tool_call_count: 0,
            },
        );
        store.limiter(|d| &mut d.limits);
        let fuel =
            PROCESSOR_FUEL_MINIMUM.max(PROCESSOR_FUEL_PER_ENVELOPE * (total_envelope_count as u64));
        store
            .set_fuel(fuel)
            .unwrap_or_else(|e| panic!("failed to set fuel on processor store: {e}"));
        store.set_epoch_deadline(PROCESSOR_EPOCH_DEADLINE_TICKS);
        store
    }

    /// Invoke the component's `receive` export with `activation`.
    ///
    /// Returns `ProcessorOutcome::Ok(publishes)` on guest ok (publishes are the
    /// buffered messages to flush), `ProcessorOutcome::Err` on a typed guest reject,
    /// and `ProcessorOutcome::Trap` on a guest trap or resource exhaustion — does NOT
    /// panic on a trap (contained, alerted by caller).
    pub fn handle(&self, activation: ProcessorActivation) -> ProcessorOutcome {
        let total_envelopes: usize = activation.ports.iter().map(|pw| pw.envelopes.len()).sum();
        let grant_input_mt = compute_grant_input_mt(&self.input_amplification_mt, &activation);
        let mut carry = self.lock_carry_for_activation();
        let budgets = self.seed_publish_budgets(&carry, grant_input_mt);
        let store = self.make_store(total_envelopes, budgets);
        Self::invoke(store, activation, &self.processor_pre, &mut carry)
    }

    /// Acquire the per-component carry lock for the duration of one activation.
    ///
    /// The token-bucket accounting (seed from carry, spend during the guest call,
    /// write remaining back to carry) is correct only if activations for one
    /// component never overlap. The dispatch loop already serializes them; holding
    /// this guard across the whole activation makes the invariant self-enforcing —
    /// a concurrent second activation would fail `try_lock` and panic here rather
    /// than double-spend the carryover and lose the other activation's writeback.
    fn lock_carry_for_activation(&self) -> std::sync::MutexGuard<'_, HashMap<SinkKey, u64>> {
        self.publish_carry.try_lock().unwrap_or_else(|e| match e {
            std::sync::TryLockError::WouldBlock => panic!(
                "concurrent activation for component {:?} — the per-component \
                 serialization invariant is violated; token-bucket accounting is unsound",
                self.slug
            ),
            std::sync::TryLockError::Poisoned(_) => panic!("publish_carry mutex poisoned"),
        })
    }

    /// `handle` variant with an overridden epoch deadline and fuel limit — test use only.
    ///
    /// Allows tests to pass a tiny epoch deadline and high fuel to drive the epoch
    /// interrupt path without waiting for the production 30-second wall budget.
    #[doc(hidden)]
    pub fn handle_with_limits(
        &self,
        activation: ProcessorActivation,
        epoch_deadline: u64,
        fuel: u64,
    ) -> ProcessorOutcome {
        // Base store (fuel overridden below); still computes the activation-derived
        // input grant so multi-envelope budget tests driven through this path see a
        // real per-sink budget.
        let grant_input_mt = compute_grant_input_mt(&self.input_amplification_mt, &activation);
        let mut carry = self.lock_carry_for_activation();
        let budgets = self.seed_publish_budgets(&carry, grant_input_mt);
        let mut store = self.make_store(0, budgets);
        store
            .set_fuel(fuel)
            .unwrap_or_else(|e| panic!("handle_with_limits: set_fuel: {e}"));
        store.set_epoch_deadline(epoch_deadline);
        Self::invoke(store, activation, &self.processor_pre, &mut carry)
    }

    /// `handle` variant with an overridden `memory_size` store limit — test use only.
    ///
    /// Allows tests to set a store-level memory cap below the fixture's initial memory
    /// declaration, forcing `processor_pre.instantiate` to fail with a resource-limit
    /// error and return `ProcessorOutcome::Trap("instantiation failed: …")`. This is
    /// the only way to reach that arm via the public API without a custom WIT-typed WAT
    /// fixture (the production `make_store` hardcodes `PROCESSOR_MAX_MEMORY_BYTES`; all
    /// real fixture components stay within that limit).
    ///
    /// Pass a `memory_limit_bytes` well below the fixture's initial memory (e.g. 1) to
    /// guarantee the limit fires during instantiation rather than later.
    #[doc(hidden)]
    pub fn handle_with_memory_limit(
        &self,
        activation: ProcessorActivation,
        memory_limit_bytes: usize,
    ) -> ProcessorOutcome {
        let total_envelopes: usize = activation.ports.iter().map(|pw| pw.envelopes.len()).sum();
        let grant_input_mt = compute_grant_input_mt(&self.input_amplification_mt, &activation);
        let mut carry = self.lock_carry_for_activation();
        let budgets = self.seed_publish_budgets(&carry, grant_input_mt);
        let mut store = self.make_store(total_envelopes, budgets);
        // Override the memory_size limit on the already-constructed store's ProcessorData.
        // StoreLimits is not mutable after construction; instead we replace the entire limits
        // field. The rest of the store settings (fuel, epoch, etc.) come from make_store.
        store.data_mut().limits = StoreLimitsBuilder::new()
            .memory_size(memory_limit_bytes)
            .table_elements(PROCESSOR_MAX_TABLE_ELEMENTS)
            .instances(PROCESSOR_MAX_INSTANCES)
            .tables(PROCESSOR_MAX_TABLES)
            .trap_on_grow_failure(true)
            .build();
        Self::invoke(store, activation, &self.processor_pre, &mut carry)
    }

    /// Shared dispatch core: converts `activation` to WIT types, calls `receive`,
    /// extracts publishes, and runs `cleanup_leaked_tx` unconditionally.
    ///
    /// `handle`, `handle_with_limits`, and `handle_with_memory_limit` all delegate
    /// here after building the `Store` with the appropriate settings.
    fn invoke(
        mut store: Store<ProcessorData>,
        activation: ProcessorActivation,
        processor_pre: &ProcessorPre<ProcessorData>,
        carry: &mut HashMap<SinkKey, u64>,
    ) -> ProcessorOutcome {
        let wit_ports: Vec<_> = activation
            .ports
            .into_iter()
            .map(
                |pw| processor_bindings::brenn::processor::types::PortWindow {
                    port: pw.port,
                    envelopes: pw.envelopes,
                    new_from: pw.new_from,
                    // `processor.wit` types `dropped` as u32 while the carrier
                    // counts in u64. Saturate: the guest reads this as a gap
                    // signal, and a saturated count still says "you lost more
                    // than you can count".
                    dropped: u32::try_from(pw.dropped).unwrap_or(u32::MAX),
                },
            )
            .collect();
        let wit_activation =
            processor_bindings::brenn::processor::types::Activation { ports: wit_ports };
        let instance = match processor_pre.instantiate(&mut store) {
            Ok(i) => i,
            // Deliberately skips the carry writeback tail below: the guest never ran,
            // so its seeded budget (carry-clamp + fill + input grant) is forfeited and
            // the prior carry is retained untouched. This is the conservative direction
            // — a component that cannot even instantiate must not accumulate budget.
            Err(e) => return ProcessorOutcome::Trap(format!("instantiation failed: {e:#}")),
        };
        let outcome = match instance.call_receive(&mut store, &wit_activation) {
            Ok(Ok(())) => {
                // Both buffers flush together, on Ok only. On Err/Trap this arm is
                // never taken, so the buffers drop with the store below — a trapped
                // activation therefore fires no port publish AND no tool call.
                let publishes = store.data_mut().take_ok_publishes();
                ProcessorOutcome::Ok(publishes)
            }
            Ok(Err(re)) => ProcessorOutcome::Err(re),
            Err(e) => ProcessorOutcome::Trap(format!("{e:#}")),
        };
        // Correctness: on a guest trap the WIT `drop` destructor never runs (no guest
        // execution after trap), so a leaked store transaction is not rolled back via the
        // normal HostTransaction::drop path. Roll back explicitly to release the write
        // lock and reset tx_active so subsequent activations can begin a transaction.
        Self::cleanup_leaked_tx(store.data_mut());

        // Post-activation suppression warn: one line per affected activation, not per
        // dropped call — the warn path must not itself be a log flood.
        {
            let data = store.data();
            let log_suppressed = data
                .log_call_count
                .saturating_sub(PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION);
            let alert_suppressed = data
                .alert_call_count
                .saturating_sub(PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION);
            if log_suppressed > 0 || alert_suppressed > 0 {
                warn!(
                    slug = %data.slug,
                    log_suppressed = log_suppressed,
                    alert_suppressed = alert_suppressed,
                    "wasm guest log/alert quota exceeded — calls dropped"
                );
            }

            // Per-sink publish-budget suppression: one warn per activation with the
            // per-sink dropped counts (not one per dropped call). Sink keys are
            // host-config-derived, safe to log verbatim. This is the operator's
            // signal that a publish budget is being hit — the guest may swallow the
            // error, so the host must surface it.
            if !data.publish_suppressed_by_sink.is_empty() {
                warn!(
                    slug = %data.slug,
                    suppressed = ?data.publish_suppressed_by_sink,
                    "wasm publish budget exceeded — publishes dropped"
                );
            }

            // Write the remaining per-sink budget back as carryover (uncapped; the
            // capacity clamp applies at the next activation start). Consumed tokens
            // are not refunded on failure — a crash-retry loop must not amplify. The
            // caller holds the carry lock across the whole activation (seeded from
            // this same map), so the seed-spend-writeback cycle is atomic per sink.
            carry.clone_from(&data.publish_budget_by_sink);
        }

        outcome
    }

    /// If a store transaction is still active after `call_receive` returns (trap or
    /// mem::forget in a buggy guest), roll it back and clear the tx_active flag.
    /// This prevents the shared `Arc<KvStore>` from being permanently bricked.
    fn cleanup_leaked_tx(data: &mut ProcessorData) {
        if let Some(ref kv) = data.kv_store
            && kv.is_tx_active()
        {
            warn!(
                slug = %data.slug,
                "wasm processor: store transaction leaked (guest trapped mid-transaction); \
                 rolling back to release write lock"
            );
            let conn = kv.lock_conn();
            store::rollback_tx(&conn).unwrap_or_else(|e| {
                panic!(
                    "cleanup_leaked_tx: ROLLBACK failed ({e}) — store may be corrupted \
                     (slug={})",
                    data.slug
                )
            });
            kv.clear_tx();
        }
    }

    /// Expose the underlying KvStore for test assertions.
    ///
    /// Panics if this component was loaded without the `Store` grant (every
    /// caller is a store-granting test; an absent store is a test-setup error).
    #[doc(hidden)]
    pub fn kv_store_for_testing(&self) -> &Arc<KvStore> {
        self.kv_store
            .as_ref()
            .expect("kv_store_for_testing called on a component without the Store grant")
    }

    /// Expose the underlying wasmtime Engine for test-only use (e.g. calling
    /// `engine.increment_epoch()` to drive epoch-deadline tests).
    #[doc(hidden)]
    pub fn engine_for_testing(&self) -> &Engine {
        &self.engine
    }
}

// ── ProcessorData host-method unit tests ─────────────────────────────────────
//
// Direct unit tests for the store::Host / HostTransaction impls on ProcessorData.
// These test the shared free functions (host_begin, host_commit, host_rollback,
// host_drop) via ProcessorData without requiring a guest fixture component.
//
// The cleanup_leaked_tx path is also covered here because it is the correctness-1
// fix (a guest trap mid-transaction must not permanently brick the shared KvStore).

#[cfg(test)]
mod import_normalization_tests {
    use super::{normalize_import_names, strip_import_version};

    #[test]
    fn strips_only_the_version_suffix() {
        assert_eq!(
            strip_import_version("brenn:processor/store@0.1.0"),
            "brenn:processor/store"
        );
        // Unversioned names pass through untouched.
        assert_eq!(
            strip_import_version("brenn:processor/log"),
            "brenn:processor/log"
        );
    }

    #[test]
    fn keeps_names_fully_qualified() {
        // The namespace is the whole point: a foreign `wasi:logging/log` must
        // stay distinguishable from the host `brenn:processor/log`, so neither
        // may be reduced to a bare interface name.
        let names =
            normalize_import_names(["wasi:logging/log@0.2.0", "brenn:processor/log"].into_iter());
        assert_eq!(names, vec!["brenn:processor/log", "wasi:logging/log"]);
    }

    #[test]
    fn sorts_by_byte_order_and_dedups_across_versions() {
        // Two imports differing only by version collapse to one entry — the
        // case that makes dedup load-bearing rather than decorative.
        let names = normalize_import_names(
            [
                "brenn:processor/ports@0.2.0",
                "brenn:processor/config",
                "brenn:processor/ports@0.1.0",
            ]
            .into_iter(),
        );
        assert_eq!(
            names,
            vec!["brenn:processor/config", "brenn:processor/ports"]
        );
    }
}

#[cfg(test)]
mod processor_store_host_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;
    use tracing_test::traced_test;
    use wasmtime::component::ResourceTable;

    use super::*;
    use crate::store::{DEFAULT_MAX_PAGE_COUNT, KvStore};
    use processor_bindings::brenn::processor::store::{Host, HostTransaction};

    /// No-op alerter for unit tests that don't exercise the alert path.
    struct NoopAlerter;
    impl ProcessorAlerter for NoopAlerter {
        fn alert(&self, _severity: GuestAlertSeverity, _title: &str, _body: &str) {}
    }

    /// Capturing alerter for unit tests that assert an alert fired.
    struct CapturingProcessorAlerter {
        calls: Mutex<Vec<(GuestAlertSeverity, String, String)>>,
    }
    impl CapturingProcessorAlerter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }
    }
    impl ProcessorAlerter for CapturingProcessorAlerter {
        fn alert(&self, severity: GuestAlertSeverity, title: &str, body: &str) {
            self.calls
                .lock()
                .unwrap()
                .push((severity, title.to_string(), body.to_string()));
        }
    }

    /// A per-sink budget large enough that it never trips first — for tests that
    /// exercise the global 256/512/byte backstops or non-budget behavior.
    const TEST_GENEROUS_MT: u64 = 1_000_000_000;

    /// Generous `SinkBudget` for tests that must not trip the per-sink gate.
    fn generous_budget() -> SinkBudget {
        SinkBudget {
            fill_mt: TEST_GENEROUS_MT,
            capacity_mt: TEST_GENEROUS_MT,
        }
    }

    /// The production default per-sink budget shape (fill 1.0, capacity 1.0).
    fn default_budget() -> SinkBudget {
        SinkBudget {
            fill_mt: 1000,
            capacity_mt: 1000,
        }
    }

    /// A bound output port with a generous per-sink budget.
    fn test_out_spec(channel_address: &str) -> OutputPortSpec {
        OutputPortSpec {
            channel_address: channel_address.to_string(),
            default_urgency: ProcessorUrgency::Normal,
            budget: generous_budget(),
        }
    }

    /// Build a minimal `ProcessorData` over a fresh tempfile KvStore.
    fn make_processor_data() -> (NamedTempFile, Arc<KvStore>, ProcessorData) {
        let db = NamedTempFile::new().unwrap();
        let kv = KvStore::open(db.path(), DEFAULT_MAX_PAGE_COUNT);
        let data = ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: Some(Arc::clone(&kv)),
            output_ports: Arc::new(HashMap::new()),
            config: Arc::new(HashMap::new()),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: HashMap::new(),
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: None,
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: None,
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        };
        (db, kv, data)
    }

    /// Build a `ProcessorData` with a single bound output port (`"out"` →
    /// `channel_address`) and a caller-supplied `output_acl`, for exercising the
    /// `do_publish` output-ACL gate (design §2.3) at the host-fn level.
    fn make_publish_data(channel_address: &str, output_acl: OutputAclFn) -> ProcessorData {
        let mut ports = HashMap::new();
        ports.insert("out".to_string(), test_out_spec(channel_address));
        let mut budgets = HashMap::new();
        budgets.insert(SinkKey::Port("out".to_string()), TEST_GENEROUS_MT);
        ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: None,
            output_ports: Arc::new(ports),
            config: Arc::new(HashMap::new()),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: budgets,
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl,
            mqtt_publish: None,
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: None,
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        }
    }

    /// Output-ACL deny: a bound port whose channel is rejected by `output_acl`
    /// returns `PublishError::NotPermitted` and buffers nothing (design §2.3, §4
    /// "Publish-ACL deny"). Reuses the same guest-visible variant as an unbound
    /// port; the bound channel is denied because it is outside the allowlist.
    #[test]
    fn do_publish_acl_deny_returns_not_permitted_and_buffers_nothing() {
        // Deny exactly the bound channel.
        let acl: OutputAclFn = Arc::new(|addr: &str| addr != "brenn:secret");
        let mut data = make_publish_data("brenn:secret", acl);

        let err = data
            .do_publish("out".to_string(), "payload".to_string(), None)
            .expect_err("publish to a channel outside the output ACL must be denied");
        assert!(
            matches!(err, PublishError::NotPermitted),
            "ACL deny must reuse NotPermitted (same as unbound port); got {err:?}"
        );
        // Denied publish: nothing reaches the buffer.
        assert!(
            data.publish_buffer.is_empty(),
            "denied publish must not buffer the payload"
        );
        assert_eq!(
            data.published_bytes, 0,
            "denied publish must not count payload bytes"
        );
        // The deny consumes the per-activation call budget (anti-flood), exactly
        // like the unbound-port path — the gate sits after the call-count bump.
        assert_eq!(
            data.publish_call_count, 1,
            "denied publish must still consume the per-activation call budget"
        );
    }

    /// A bound port whose channel the `output_acl` permits publishes
    /// successfully, buffering the payload and counting its bytes.
    #[test]
    fn do_publish_acl_allow_buffers_publish() {
        // Allow exactly the bound channel.
        let acl: OutputAclFn = Arc::new(|addr: &str| addr == "brenn:allowed");
        let mut data = make_publish_data("brenn:allowed", acl);

        data.do_publish("out".to_string(), "payload".to_string(), None)
            .expect("publish to an in-allowlist channel must succeed");
        assert_eq!(
            data.publish_buffer.len(),
            1,
            "allowed publish must buffer exactly one message"
        );
        let buffered = &data.publish_buffer[0];
        assert_eq!(buffered.channel_address, "brenn:allowed");
        assert_eq!(buffered.payload, "payload");
        assert_eq!(
            data.published_bytes,
            "payload".len(),
            "allowed publish must count its payload bytes"
        );
        assert_eq!(data.publish_call_count, 1);
    }

    // --- do_mqtt_publish host-fn tests (design §2.5, §3.3, §4 "WASM egress") ---

    /// Build a `ProcessorData` whose MQTT-publish callback returns `outcome`.
    fn make_mqtt_data(outcome: MqttPublishOutcome) -> ProcessorData {
        let cb: MqttPublishFn = Arc::new(
            move |_client, _topic, _payload, _content_type, _qos, _retain| outcome.clone(),
        );
        ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: None,
            output_ports: Arc::new(HashMap::new()),
            config: Arc::new(HashMap::new()),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: HashMap::new(),
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: Some(cb),
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: None,
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        }
    }

    /// The callback's outcome is mapped 1:1 onto the WIT `mqtt-publish-error`
    /// variants (and `Ok`). Each arm of the seam enum reaches the right WIT arm.
    #[test]
    fn do_mqtt_publish_maps_each_outcome() {
        // Ok → Ok(()).
        let mut ok = make_mqtt_data(MqttPublishOutcome::Ok);
        ok.do_mqtt_publish("home".into(), "t".into(), vec![1], None, 0, false)
            .expect("Ok outcome must map to Ok(())");
        assert_eq!(
            ok.publish_call_count, 1,
            "a call consumes the shared budget"
        );

        // NotPermitted → not-permitted.
        let mut np = make_mqtt_data(MqttPublishOutcome::NotPermitted);
        assert!(
            matches!(
                np.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
                Err(MqttPublishWitError::NotPermitted)
            ),
            "NotPermitted must map to not-permitted"
        );

        // NoConnector → no-connector.
        let mut nc = make_mqtt_data(MqttPublishOutcome::NoConnector);
        assert!(matches!(
            nc.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
            Err(MqttPublishWitError::NoConnector)
        ));

        // InvalidPayload(reason) → invalid-payload(reason), preserving the string.
        let mut ip = make_mqtt_data(MqttPublishOutcome::InvalidPayload("bad topic".into()));
        match ip.do_mqtt_publish("home".into(), "t#".into(), vec![], None, 0, false) {
            Err(MqttPublishWitError::InvalidPayload(s)) => assert_eq!(s, "bad topic"),
            other => panic!("expected invalid-payload, got {other:?}"),
        }

        // Broker(reason) → broker(reason).
        let mut br = make_mqtt_data(MqttPublishOutcome::Broker("disconnected".into()));
        match br.do_mqtt_publish("home".into(), "t".into(), vec![], None, 1, false) {
            Err(MqttPublishWitError::Broker(s)) => assert_eq!(s, "disconnected"),
            other => panic!("expected broker, got {other:?}"),
        }

        // BrokerRejected(reason) → broker-rejected(reason).
        let mut rj = make_mqtt_data(MqttPublishOutcome::BrokerRejected("not authorized".into()));
        match rj.do_mqtt_publish("home".into(), "t".into(), vec![], None, 1, false) {
            Err(MqttPublishWitError::BrokerRejected(s)) => assert_eq!(s, "not authorized"),
            other => panic!("expected broker-rejected, got {other:?}"),
        }
    }

    /// The callback receives the guest-supplied arguments verbatim (client,
    /// topic, payload, content-type, qos, retain) — the host fn forwards, it does
    /// not reinterpret.
    #[test]
    fn do_mqtt_publish_forwards_guest_args() {
        // (client, topic, payload, content_type, qos, retain) captured by the cb.
        type SeenArgs = (String, String, Vec<u8>, Option<String>, u8, bool);
        let seen: Arc<Mutex<Option<SeenArgs>>> = Arc::new(Mutex::new(None));
        let seen_cb = Arc::clone(&seen);
        let cb: MqttPublishFn =
            Arc::new(move |client, topic, payload, content_type, qos, retain| {
                *seen_cb.lock().unwrap() =
                    Some((client, topic, payload, content_type, qos, retain));
                MqttPublishOutcome::Ok
            });
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        data.mqtt_publish = Some(cb);

        data.do_mqtt_publish(
            "home".into(),
            "sensors/temp".into(),
            vec![9, 8, 7],
            Some("application/json".into()),
            2,
            true,
        )
        .expect("Ok");

        let got = seen.lock().unwrap().clone().expect("callback was invoked");
        assert_eq!(got.0, "home");
        assert_eq!(got.1, "sensors/temp");
        assert_eq!(got.2, vec![9, 8, 7]);
        assert_eq!(got.3, Some("application/json".to_string()));
        assert_eq!(got.4, 2);
        assert!(got.5);
    }

    /// The per-activation call-count budget is *shared* with `do_publish`
    /// (design §2.5): interleaved `ports.publish` + `mqtt-publish` calls draw
    /// from the same `MAX_PUBLISH_CALLS_PER_ACTIVATION` cap, and once
    /// it is exhausted `mqtt-publish` returns `quota-exceeded`.
    #[test]
    fn do_mqtt_publish_shares_call_budget_with_do_publish() {
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        // Also bind an output port so do_publish reaches its own success path and
        // bumps the shared counter.
        let mut ports = HashMap::new();
        ports.insert("out".to_string(), test_out_spec("brenn:out"));
        data.output_ports = Arc::new(ports);
        // Generous per-sink budget so the shared 512-call cap trips first.
        data.publish_budget_by_sink
            .insert(SinkKey::Port("out".to_string()), TEST_GENEROUS_MT);

        // Spend half the budget via ports.publish, the rest via mqtt-publish.
        let half = MAX_PUBLISH_CALLS_PER_ACTIVATION / 2;
        for _ in 0..half {
            data.do_publish("out".to_string(), "p".to_string(), None)
                .expect("under budget");
        }
        for _ in half..MAX_PUBLISH_CALLS_PER_ACTIVATION {
            data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false)
                .expect("under budget");
        }
        assert_eq!(data.publish_call_count, MAX_PUBLISH_CALLS_PER_ACTIVATION);
        // One more — over the shared cap — must be quota-exceeded, even though the
        // callback would otherwise return Ok.
        assert!(
            matches!(
                data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
                Err(MqttPublishWitError::QuotaExceeded)
            ),
            "mqtt-publish past the shared call cap must be quota-exceeded"
        );
    }

    /// A `quota-exceeded` rejection still consumes a call-count slot (the bump is
    /// before the cap check), exactly like `do_publish` — so a guest cannot get
    /// free retries by spamming past the cap.
    #[test]
    fn do_mqtt_publish_quota_reject_consumes_budget() {
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        data.publish_call_count = MAX_PUBLISH_CALLS_PER_ACTIVATION;
        assert!(matches!(
            data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
            Err(MqttPublishWitError::QuotaExceeded)
        ));
        assert_eq!(
            data.publish_call_count,
            MAX_PUBLISH_CALLS_PER_ACTIVATION + 1,
            "an over-cap call still bumps the counter (no free retries)"
        );
    }

    /// Pure-MQTT call-budget boundary, independent of `do_publish` sharing the
    /// counter: exactly `MAX_PUBLISH_CALLS_PER_ACTIVATION` successful
    /// `mqtt-publish` calls all return `Ok`, and the next one (the first over the
    /// cap) returns `quota-exceeded`. A regression that skipped the MQTT counter
    /// bump (e.g. bumping only for successful publishes) would let this run past
    /// the cap and fail here, where the mixed `do_publish`+`do_mqtt_publish` test
    /// could mask it.
    #[test]
    fn do_mqtt_publish_call_budget_boundary_mqtt_only() {
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        for i in 0..MAX_PUBLISH_CALLS_PER_ACTIVATION {
            data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false)
                .unwrap_or_else(|e| panic!("call {i} within budget must be Ok, got {e:?}"));
        }
        assert_eq!(
            data.publish_call_count, MAX_PUBLISH_CALLS_PER_ACTIVATION,
            "exactly the cap many successful calls consumed exactly the cap many slots"
        );
        assert!(
            matches!(
                data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
                Err(MqttPublishWitError::QuotaExceeded)
            ),
            "the first call past the cap must be quota-exceeded"
        );
    }

    /// An out-of-range `qos` (the WIT type is `u8`, but only 0/1/2 are valid) is
    /// rejected up front as a *permanent* `invalid-payload`, never forwarded to the
    /// callback where it would surface as a transient (MAY-retry) `broker` error.
    /// Mirrors the LLM intercept's up-front qos check; keeps both call sites'
    /// input-validation aligned (correctness review).
    #[test]
    fn do_mqtt_publish_rejects_out_of_range_qos_as_invalid_payload() {
        // The callback would return Ok — proving the rejection happens before it.
        let cb_invoked = Arc::new(Mutex::new(false));
        let flag = Arc::clone(&cb_invoked);
        let cb: MqttPublishFn = Arc::new(move |_c, _t, _p, _ct, _q, _r| {
            *flag.lock().unwrap() = true;
            MqttPublishOutcome::Ok
        });
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        data.mqtt_publish = Some(cb);

        match data.do_mqtt_publish("home".into(), "t".into(), vec![1], None, 3, false) {
            Err(MqttPublishWitError::InvalidPayload(s)) => {
                assert!(s.contains("qos"), "reason should mention qos, got {s:?}")
            }
            other => panic!("qos=3 must be invalid-payload, got {other:?}"),
        }
        assert!(
            !*cb_invoked.lock().unwrap(),
            "the callback (and thus the broker) must never be reached for an invalid qos"
        );
        // The call still consumed a budget slot (validation is after the bump).
        assert_eq!(data.publish_call_count, 1);
    }

    /// A payload larger than `max_payload_bytes` is rejected as `invalid-payload`
    /// before reaching the broker — the synchronous MQTT path enforces the same
    /// per-message size bound the buffered `do_publish` path does, so a guest
    /// holding the `mqtt` grant cannot emit arbitrary-size broker publishes
    /// (security review).
    #[test]
    fn do_mqtt_publish_rejects_oversize_payload_as_invalid_payload() {
        let cb_invoked = Arc::new(Mutex::new(false));
        let flag = Arc::clone(&cb_invoked);
        let cb: MqttPublishFn = Arc::new(move |_c, _t, _p, _ct, _q, _r| {
            *flag.lock().unwrap() = true;
            MqttPublishOutcome::Ok
        });
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok); // max_payload_bytes = 1024
        data.mqtt_publish = Some(cb);

        let oversize = vec![0u8; 1025];
        match data.do_mqtt_publish("home".into(), "t".into(), oversize, None, 0, false) {
            Err(MqttPublishWitError::InvalidPayload(s)) => {
                assert!(
                    s.contains("exceeds max"),
                    "reason should mention the cap, got {s:?}"
                )
            }
            other => panic!("oversize payload must be invalid-payload, got {other:?}"),
        }
        assert!(
            !*cb_invoked.lock().unwrap(),
            "the callback (and thus the broker) must never be reached for an oversize payload"
        );
        // A payload exactly at the cap is accepted (boundary: `>` not `>=`).
        let mut at_cap = make_mqtt_data(MqttPublishOutcome::Ok);
        at_cap
            .do_mqtt_publish("home".into(), "t".into(), vec![0u8; 1024], None, 0, false)
            .expect("a payload exactly at max_payload_bytes must be accepted");
    }

    /// `not-permitted` (an operator ACL actively blocking a WASM publish) emits a
    /// server-side `warn!` carrying the consumer slug and the (capped) client slug
    /// — design §3.3 ("Both callers must log the denial"). Without this assertion a
    /// refactor that dropped the `warn!` would silently regress WASM ACL-deny
    /// observability (test review).
    #[traced_test]
    #[test]
    fn do_mqtt_publish_not_permitted_emits_warn() {
        let mut np = make_mqtt_data(MqttPublishOutcome::NotPermitted);
        let _ = np.do_mqtt_publish("blocked-client".into(), "t".into(), vec![], None, 0, false);
        assert!(
            logs_contain("not-permitted"),
            "a not-permitted MQTT publish must emit a warn! naming the denial"
        );
        assert!(
            logs_contain("test-slug"),
            "the warn! must carry the consumer slug"
        );
        assert!(
            logs_contain("blocked-client"),
            "the warn! must carry the denied client slug"
        );
    }

    // --- do_fast_call / do_queue_async host-fn tests ---

    type StubFastFn = Box<dyn Fn(&str, &str) -> Result<String, ToolCallError> + Send + Sync>;
    type StubQueueFn =
        Box<dyn Fn(&str, &str, &str) -> Result<QueuedToolRequest, ToolCallError> + Send + Sync>;

    /// Recording `ToolHost` whose fast/async responses are supplied per-test.
    struct StubToolHost {
        fast: Mutex<StubFastFn>,
        queue: Mutex<StubQueueFn>,
        /// (tool, args_json) recorded for each `fast_call`.
        fast_calls: Mutex<Vec<(String, String)>>,
    }

    impl StubToolHost {
        fn fast_ok(body: &'static str) -> Arc<Self> {
            Arc::new(Self {
                fast: Mutex::new(Box::new(move |_t, _a| Ok(body.to_string()))),
                queue: Mutex::new(Box::new(|_t, _a, _c| Err(ToolCallError::WrongClass))),
                fast_calls: Mutex::new(Vec::new()),
            })
        }
        fn fast_err(err: ToolCallError) -> Arc<Self> {
            Arc::new(Self {
                fast: Mutex::new(Box::new(move |_t, _a| Err(err.clone()))),
                queue: Mutex::new(Box::new(|_t, _a, _c| Err(ToolCallError::WrongClass))),
                fast_calls: Mutex::new(Vec::new()),
            })
        }
        fn queue_ok(req: QueuedToolRequest) -> Arc<Self> {
            Arc::new(Self {
                fast: Mutex::new(Box::new(|_t, _a| Err(ToolCallError::WrongClass))),
                queue: Mutex::new(Box::new(move |_t, _a, _c| Ok(req.clone()))),
                fast_calls: Mutex::new(Vec::new()),
            })
        }
    }

    impl ToolHost for StubToolHost {
        fn fast_call(&self, tool: &str, args_json: &str) -> Result<String, ToolCallError> {
            self.fast_calls
                .lock()
                .unwrap()
                .push((tool.to_string(), args_json.to_string()));
            (self.fast.lock().unwrap())(tool, args_json)
        }
        fn queue_async(
            &self,
            tool: &str,
            args_json: &str,
            call_id: &str,
        ) -> Result<QueuedToolRequest, ToolCallError> {
            (self.queue.lock().unwrap())(tool, args_json, call_id)
        }
    }

    /// Build a `ProcessorData` wired to `host` (Tools-grant present in effect).
    fn make_tool_data(host: ToolHostFn) -> ProcessorData {
        ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: None,
            output_ports: Arc::new(HashMap::new()),
            config: Arc::new(HashMap::new()),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: HashMap::new(),
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: None,
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: Some(host),
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        }
    }

    /// Happy path: a fast tool returns its result JSON verbatim, and the seam
    /// received the guest's (tool, args) unchanged.
    #[test]
    fn do_fast_call_returns_result_and_forwards_args() {
        let host = StubToolHost::fast_ok("{\"ok\":true}");
        let mut data = make_tool_data(host.clone());
        let out = data
            .do_fast_call("git-status".into(), "{\"repo\":\"brenn\"}".into())
            .expect("fast call must succeed");
        assert_eq!(out, "{\"ok\":true}");
        let recorded = host.fast_calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "git-status");
        assert_eq!(recorded[0].1, "{\"repo\":\"brenn\"}");
        assert_eq!(data.tool_call_count, 1);
    }

    /// A seam `NotGranted` maps to the WIT `not-granted` variant and emits a
    /// slug-keyed warn. Unknown and ungranted are one variant.
    #[traced_test]
    #[test]
    fn do_fast_call_not_granted_maps_and_warns() {
        let mut data = make_tool_data(StubToolHost::fast_err(ToolCallError::NotGranted));
        let err = data
            .do_fast_call("mystery".into(), "{}".into())
            .expect_err("ungranted tool must error");
        assert!(matches!(err, ToolWitError::NotGranted));
        assert!(logs_contain("not-granted"));
        assert!(logs_contain("test-slug"));
    }

    /// `wrong-class` from the seam (call-fast on an async tool) passes through.
    #[test]
    fn do_fast_call_wrong_class_maps() {
        let mut data = make_tool_data(StubToolHost::fast_err(ToolCallError::WrongClass));
        let err = data
            .do_fast_call("git-repo-pull".into(), "{}".into())
            .expect_err("call-fast on an async tool must error");
        assert!(matches!(err, ToolWitError::WrongClass));
    }

    /// A seam `Denied` maps to `denied(resource)`, echoing the resource verbatim.
    #[test]
    fn do_fast_call_denied_echoes_resource() {
        let mut data = make_tool_data(StubToolHost::fast_err(ToolCallError::Denied("pfin".into())));
        let err = data
            .do_fast_call("git-status".into(), "{\"repo\":\"pfin\"}".into())
            .expect_err("out-of-ACL resource must be denied");
        assert!(matches!(err, ToolWitError::Denied(ref r) if r == "pfin"));
    }

    /// Over-cap `args-json` is rejected as `invalid-args` without reaching the seam.
    #[test]
    fn do_fast_call_oversize_args_rejected_before_seam() {
        let host = StubToolHost::fast_ok("{}");
        let mut data = make_tool_data(host.clone());
        let big = "x".repeat(PROCESSOR_MAX_TOOL_ARGS_BYTES + 1);
        let err = data
            .do_fast_call("git-status".into(), big)
            .expect_err("oversize args must be rejected");
        assert!(matches!(err, ToolWitError::InvalidArgs(_)));
        assert!(
            host.fast_calls.lock().unwrap().is_empty(),
            "oversize args must not reach the tool host"
        );
    }

    /// An over-cap fast result is a tool bug: `internal` + an alert fires; the
    /// guest never receives the unbounded blob.
    #[test]
    fn do_fast_call_oversize_result_alerts_and_errors() {
        let host: Arc<StubToolHost> = Arc::new(StubToolHost {
            fast: Mutex::new(Box::new(|_t, _a| {
                Ok("y".repeat(PROCESSOR_MAX_FAST_TOOL_RESULT_BYTES + 1))
            })),
            queue: Mutex::new(Box::new(|_t, _a, _c| Err(ToolCallError::WrongClass))),
            fast_calls: Mutex::new(Vec::new()),
        });
        let alerter = CapturingProcessorAlerter::new();
        let mut data = make_tool_data(host);
        data.alerter = alerter.clone();
        let err = data
            .do_fast_call("git-status".into(), "{}".into())
            .expect_err("oversize result must error");
        assert!(matches!(err, ToolWitError::Internal(_)));
        assert_eq!(
            alerter.calls.lock().unwrap().len(),
            1,
            "an over-cap fast result must alert (tool bug)"
        );
    }

    /// The per-activation fast-call cap trips at 64: the 65th call returns
    /// `internal` and does not reach the seam.
    #[test]
    fn do_fast_call_per_activation_cap() {
        let host = StubToolHost::fast_ok("{}");
        let mut data = make_tool_data(host.clone());
        for _ in 0..PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION {
            data.do_fast_call("git-status".into(), "{}".into())
                .expect("calls within the cap succeed");
        }
        let err = data
            .do_fast_call("git-status".into(), "{}".into())
            .expect_err("the over-cap call must be rejected");
        assert!(matches!(err, ToolWitError::Internal(_)));
        assert_eq!(
            host.fast_calls.lock().unwrap().len(),
            PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION,
            "the over-cap call must not reach the tool host"
        );
    }

    /// The per-activation call cap is shared with the fast path: a flood of
    /// `call-async` — even validated ones — trips `internal` at the 65th call,
    /// and the over-cap call never buffers a request. This bounds a guest looping
    /// failing async calls (each a full host-side parse + ACL walk).
    #[test]
    fn do_queue_async_per_activation_cap() {
        let req = QueuedToolRequest {
            channel: "brenn:tools/apull".to_string(),
            reply_to: "brenn:tool-results/sync".to_string(),
            body_json: "{\"v\":1}".to_string(),
        };
        let mut data = make_tool_data(StubToolHost::queue_ok(req));
        for _ in 0..PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION {
            data.do_queue_async("apull".into(), "{}".into(), "c")
                .expect("calls within the cap succeed");
        }
        let err = data
            .do_queue_async("apull".into(), "{}".into(), "c")
            .expect_err("the over-cap async call must be rejected");
        assert!(matches!(err, ToolWitError::Internal(_)));
        assert_eq!(
            data.tool_request_buffer.len(),
            PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION,
            "the over-cap call must not buffer a request"
        );
    }

    /// A validated async call is buffered with the host-resolved envelope pieces,
    /// pending the activation's transactional flush (`take_ok_publishes`).
    #[test]
    fn do_queue_async_buffers_resolved_request() {
        let req = QueuedToolRequest {
            channel: "brenn:tools/git-repo-pull".to_string(),
            reply_to: "brenn:tool-results/webhook-sync".to_string(),
            body_json: "{\"v\":1}".to_string(),
        };
        let mut data = make_tool_data(StubToolHost::queue_ok(req.clone()));
        data.do_queue_async("git-repo-pull".into(), "{\"repos\":[]}".into(), "call-1")
            .expect("valid async call must buffer");
        assert_eq!(data.tool_request_buffer, vec![req]);
    }

    /// `take_ok_publishes` (the Ok-path flush) appends each buffered async request
    /// as a publish to its `brenn:tools/<tool>` request channel carrying the
    /// caller's inbox as `reply_to`, after any ordinary port publishes, and drains
    /// both buffers. This is the only drain, and `handle` calls it solely on Ok —
    /// so an Err/Trap activation (which never reaches this call) publishes nothing.
    #[test]
    fn take_ok_publishes_appends_tool_requests_with_reply_to() {
        let req = QueuedToolRequest {
            channel: "brenn:tools/git-repo-pull".to_string(),
            reply_to: "brenn:tool-results/sync".to_string(),
            body_json: r#"{"v":1,"tool":"git-repo-pull","call_id":"c1"}"#.to_string(),
        };
        let mut data = make_tool_data(StubToolHost::queue_ok(req.clone()));
        // A prior ordinary port publish already in the buffer.
        data.publish_buffer.push(ProcessorPublish {
            port: "out".to_string(),
            channel_address: "brenn:some-port".to_string(),
            payload: "port-body".to_string(),
            urgency: ProcessorUrgency::Normal,
            reply_to: None,
        });
        data.do_queue_async("git-repo-pull".into(), "{}".into(), "c1")
            .expect("valid async call must buffer");
        assert_eq!(data.tool_request_buffer.len(), 1);

        let publishes = data.take_ok_publishes();
        assert_eq!(publishes.len(), 2, "port publish + tool request");
        // Port publish first, unchanged (no reply_to).
        assert_eq!(publishes[0].channel_address, "brenn:some-port");
        assert_eq!(publishes[0].reply_to, None);
        // Tool request second: on its request channel, body verbatim, reply_to set.
        assert_eq!(publishes[1].channel_address, "brenn:tools/git-repo-pull");
        assert_eq!(publishes[1].payload, req.body_json);
        assert_eq!(
            publishes[1].reply_to.as_deref(),
            Some("brenn:tool-results/sync")
        );
        // Both buffers drained by the take.
        assert!(data.tool_request_buffer.is_empty());
        assert!(data.publish_buffer.is_empty());
    }

    /// An over-length `call-id` is rejected as `invalid-args`.
    #[test]
    fn do_queue_async_rejects_oversize_call_id() {
        let req = QueuedToolRequest {
            channel: "c".to_string(),
            reply_to: "r".to_string(),
            body_json: "{}".to_string(),
        };
        let mut data = make_tool_data(StubToolHost::queue_ok(req));
        let long_id = "z".repeat(PROCESSOR_MAX_TOOL_CALL_ID_LEN + 1);
        let err = data
            .do_queue_async("git-repo-pull".into(), "{}".into(), &long_id)
            .expect_err("over-length call_id must be rejected");
        assert!(matches!(err, ToolWitError::InvalidArgs(_)));
        assert!(data.tool_request_buffer.is_empty());
    }

    /// A seam error on async validation propagates and buffers nothing.
    #[test]
    fn do_queue_async_seam_error_buffers_nothing() {
        let host: Arc<StubToolHost> = Arc::new(StubToolHost {
            fast: Mutex::new(Box::new(|_t, _a| Err(ToolCallError::WrongClass))),
            queue: Mutex::new(Box::new(|_t, _a, _c| Err(ToolCallError::NotGranted))),
            fast_calls: Mutex::new(Vec::new()),
        });
        let mut data = make_tool_data(host);
        let err = data
            .do_queue_async("mystery".into(), "{}".into(), "c1")
            .expect_err("ungranted async tool must error");
        assert!(matches!(err, ToolWitError::NotGranted));
        assert!(data.tool_request_buffer.is_empty());
    }

    /// The call-budget gate sits *before* the output-ACL gate (design §2.3), so a
    /// component that floods publishes against a denying ACL still trips
    /// `QuotaExceeded` once the per-activation call budget is exhausted — it does
    /// not get `NotPermitted` forever. Pins that ordering: the first
    /// `MAX_PUBLISH_CALLS_PER_ACTIVATION` denied calls return
    /// `NotPermitted`, and the next one returns `QuotaExceeded`.
    #[test]
    fn do_publish_acl_deny_still_trips_call_budget() {
        // Deny every channel.
        let acl: OutputAclFn = Arc::new(|_| false);
        let mut data = make_publish_data("brenn:secret", acl);

        for i in 0..MAX_PUBLISH_CALLS_PER_ACTIVATION {
            let err = data
                .do_publish("out".to_string(), "payload".to_string(), None)
                .expect_err("denied publish must return an error");
            assert!(
                matches!(err, PublishError::NotPermitted),
                "call {i} within budget must be NotPermitted (ACL deny), got {err:?}"
            );
        }
        // Budget now exhausted: the gate that fires first is the call-budget check,
        // so the next call is QuotaExceeded, not NotPermitted.
        let err = data
            .do_publish("out".to_string(), "payload".to_string(), None)
            .expect_err("over-budget publish must return an error");
        assert!(
            matches!(err, PublishError::QuotaExceeded),
            "over-budget call must be QuotaExceeded (budget gate precedes ACL gate), got {err:?}"
        );
        assert!(
            data.publish_buffer.is_empty(),
            "no denied publish may ever buffer a payload"
        );
    }

    // ── per-sink publish token buckets (design §2.3, §3.4, §6) ────────────────

    /// Helper: build a single-window activation with `envelopes.len()` total and
    /// `new_from` retained-context split.
    fn window_activation(port: &str, total: usize, new_from: u32) -> ProcessorActivation {
        ProcessorActivation {
            ports: vec![ProcessorPortWindow {
                port: port.to_string(),
                envelopes: (0..total).map(|i| i.to_string()).collect(),
                new_from,
                dropped: 0,
            }],
        }
    }

    /// `compute_grant_input_mt` sums `amplification × new_envelopes` over all
    /// windows, counting only new (unprocessed) envelopes.
    #[test]
    fn grant_sums_over_windows_new_envelopes_only() {
        let amp = HashMap::from([("a".to_string(), 1000u64), ("b".to_string(), 500u64)]);
        // Window a: 5 envelopes, 2 retained ⇒ 3 new ⇒ 3000. Window b: 4 envelopes,
        // all new ⇒ 4 × 500 = 2000. A pure-context window contributes 0.
        let act = ProcessorActivation {
            ports: vec![
                ProcessorPortWindow {
                    port: "a".to_string(),
                    envelopes: (0..5).map(|i| i.to_string()).collect(),
                    new_from: 2,
                    dropped: 0,
                },
                ProcessorPortWindow {
                    port: "b".to_string(),
                    envelopes: (0..4).map(|i| i.to_string()).collect(),
                    new_from: 0,
                    dropped: 0,
                },
            ],
        };
        assert_eq!(compute_grant_input_mt(&amp, &act), 3000 + 2000);
    }

    /// Attenuation is exact in millitokens: 0.1 (= 100 mt) × 1 new envelope = 100,
    /// and a zero-new (pure-context) window grants 0.
    #[test]
    fn grant_attenuation_exact_and_zero_new() {
        let amp = HashMap::from([("in".to_string(), 100u64)]);
        assert_eq!(
            compute_grant_input_mt(&amp, &window_activation("in", 1, 0)),
            100
        );
        // 3 envelopes, all retained ⇒ 0 new ⇒ 0.
        assert_eq!(
            compute_grant_input_mt(&amp, &window_activation("in", 3, 3)),
            0
        );
    }

    /// A window port absent from the amplification map is a host invariant
    /// violation — panic.
    #[test]
    #[should_panic(expected = "no amplification")]
    fn grant_panics_on_unknown_window_port() {
        let amp = HashMap::from([("known".to_string(), 1000u64)]);
        compute_grant_input_mt(&amp, &window_activation("unknown", 1, 0));
    }

    /// Defaults (fill 1, capacity 1, amplification 1): an N-new-envelope batch
    /// yields a budget of N+1 tokens on one port — exactly N+1 publishes succeed,
    /// the next trips `quota-exceeded` and is counted once per sink.
    #[test]
    fn per_sink_budget_allows_n_plus_one_then_rejects() {
        let n: u64 = 4;
        let grant = 1000 * n; // amplification 1.0 × N new envelopes
        let budget_mt = seed_sink_budget(0, default_budget(), grant);
        assert_eq!(budget_mt, 1000 * (n + 1));

        let mut data = make_publish_data("brenn:allowed", Arc::new(|_| true));
        data.publish_budget_by_sink
            .insert(SinkKey::Port("out".to_string()), budget_mt);

        for i in 0..(n + 1) {
            data.do_publish("out".to_string(), "p".to_string(), None)
                .unwrap_or_else(|e| panic!("publish {i} within budget must succeed, got {e:?}"));
        }
        let err = data
            .do_publish("out".to_string(), "p".to_string(), None)
            .expect_err("the N+2nd publish must be rejected");
        assert!(matches!(err, PublishError::QuotaExceeded));
        assert_eq!(
            data.publish_suppressed_by_sink
                .get(&SinkKey::Port("out".to_string())),
            Some(&1),
            "one suppression counted for the port sink"
        );
    }

    /// Buckets are per sink: draining port A does not affect port B.
    #[test]
    fn per_sink_independence_across_ports() {
        let mut ports = HashMap::new();
        ports.insert("a".to_string(), test_out_spec("brenn:a"));
        ports.insert("b".to_string(), test_out_spec("brenn:b"));
        let mut budgets = HashMap::new();
        // Port A: exactly one token. Port B: generous.
        budgets.insert(SinkKey::Port("a".to_string()), 1000u64);
        budgets.insert(SinkKey::Port("b".to_string()), TEST_GENEROUS_MT);
        let mut data = ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: None,
            output_ports: Arc::new(ports),
            config: Arc::new(HashMap::new()),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: budgets,
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: None,
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: None,
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        };
        // Drain A.
        data.do_publish("a".to_string(), "p".to_string(), None)
            .expect("A first publish ok");
        assert!(matches!(
            data.do_publish("a".to_string(), "p".to_string(), None),
            Err(PublishError::QuotaExceeded)
        ));
        // B is unaffected.
        data.do_publish("b".to_string(), "p".to_string(), None)
            .expect("B publishes despite A being dry");
        assert_eq!(
            data.publish_suppressed_by_sink
                .get(&SinkKey::Port("b".to_string())),
            None,
            "B has no suppression"
        );
    }

    /// MQTT per-sink budget is charged on attempt — even a broker-error outcome
    /// consumes a token (anti-retry-amplification), and exhaustion returns
    /// `quota-exceeded`.
    #[test]
    fn mqtt_per_sink_charged_on_attempt_even_on_broker_error() {
        let mut data = make_mqtt_data(MqttPublishOutcome::Broker("down".into()));
        // Budget for exactly two attempts.
        data.publish_budget_by_sink
            .insert(SinkKey::MqttClient("home".to_string()), 2000u64);
        for _ in 0..2 {
            let r = data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false);
            assert!(
                matches!(r, Err(MqttPublishWitError::Broker(_))),
                "broker error still charges"
            );
        }
        // Third attempt: budget exhausted ⇒ quota-exceeded (before the callback).
        assert!(matches!(
            data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false),
            Err(MqttPublishWitError::QuotaExceeded)
        ));
        assert_eq!(
            data.publish_suppressed_by_sink
                .get(&SinkKey::MqttClient("home".to_string())),
            Some(&1)
        );
    }

    /// An MQTT client with no bucket entry (outside the ACL sink set) is not
    /// per-sink counted — the call-count cap alone bounds it.
    #[test]
    fn mqtt_no_sink_entry_skips_per_sink_budget() {
        let mut data = make_mqtt_data(MqttPublishOutcome::Ok);
        // No budget entry for "home".
        for _ in 0..50 {
            data.do_mqtt_publish("home".into(), "t".into(), vec![], None, 0, false)
                .expect("no per-sink limit when the client has no bucket");
        }
        assert!(data.publish_suppressed_by_sink.is_empty());
    }

    /// begin → put → commit → begin → get round-trip.
    ///
    /// The WIT-generated Host/HostTransaction traits take Resource<T> by value.
    /// After each consuming call (put, commit) we reconstruct a borrow handle from
    /// the rep so the resource table entry remains valid for subsequent operations.
    #[test]
    fn processor_store_begin_put_commit_get() {
        let (_db, _kv, mut data) = make_processor_data();

        // begin
        let tx1 = data.begin().expect("begin must succeed");
        let rep1 = tx1.rep();
        // put (consumes tx1 handle; resource table entry persists)
        data.put(
            Resource::new_borrow(rep1),
            "ns".into(),
            b"k".to_vec(),
            b"v".to_vec(),
        )
        .expect("put must succeed");
        // commit (uses get not delete, so entry persists until drop)
        data.commit(Resource::new_borrow(rep1))
            .expect("commit must succeed");
        assert!(
            !data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must be false after commit"
        );
        // Explicitly drop the resource table entry (simulates guest drop after commit).
        data.drop(Resource::new_own(rep1))
            .expect("drop of committed tx must succeed");

        // begin again for the read-back
        let tx2 = data.begin().expect("second begin must succeed");
        let rep2 = tx2.rep();
        // get (consumes tx2 handle)
        let val = data
            .get(Resource::new_borrow(rep2), "ns".into(), b"k".to_vec())
            .expect("get must succeed");
        assert_eq!(val, Some(b"v".to_vec()), "committed value must be readable");
        data.commit(Resource::new_borrow(rep2))
            .expect("second commit must succeed");
        data.drop(Resource::new_own(rep2))
            .expect("drop of second tx must succeed");
    }

    /// Double-commit returns Backend error; does not panic.
    #[test]
    fn processor_store_double_commit_returns_error() {
        let (_db, _kv, mut data) = make_processor_data();
        let tx = data.begin().expect("begin must succeed");
        let rep = tx.rep();
        data.commit(Resource::new_borrow(rep))
            .expect("first commit must succeed");
        let err = data
            .commit(Resource::new_borrow(rep))
            .expect_err("second commit must return Err");
        assert!(
            matches!(err, ProcessorStoreError::Backend(_)),
            "double-commit must return Backend error, got: {err:?}"
        );
        data.drop(Resource::new_own(rep))
            .expect("drop after double-commit must succeed");
    }

    /// rollback-after-commit is a no-op (warns, does not panic).
    #[test]
    fn processor_store_rollback_after_commit_noop() {
        let (_db, _kv, mut data) = make_processor_data();
        let tx = data.begin().expect("begin must succeed");
        let rep = tx.rep();
        data.commit(Resource::new_borrow(rep))
            .expect("commit must succeed");
        // rollback after commit: should warn and return, not panic
        data.rollback(Resource::new_borrow(rep));
        assert!(
            !data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must remain false"
        );
        data.drop(Resource::new_own(rep))
            .expect("drop after rollback-after-commit must succeed");
    }

    /// Dropped transaction (without commit/rollback) triggers cleanup_leaked_tx correctly.
    ///
    /// Simulates the correctness-1 scenario: guest traps mid-transaction, leaving
    /// tx_active set. cleanup_leaked_tx must roll back and clear the flag so the
    /// next begin succeeds.
    #[test]
    fn processor_store_cleanup_leaked_tx_allows_next_begin() {
        let (_db, _kv, mut data) = make_processor_data();

        // Simulate a guest trap mid-transaction: begin without commit/rollback,
        // then invoke cleanup_leaked_tx as ProcessorComponent::handle does.
        let _tx = data.begin().expect("begin must succeed");
        assert!(
            data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must be set after begin"
        );

        // Simulate the trap path: cleanup_leaked_tx clears the leaked transaction.
        ProcessorComponent::cleanup_leaked_tx(&mut data);
        assert!(
            !data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must be cleared after cleanup"
        );

        // The store must now accept a fresh begin.
        let tx2 = data.begin().expect("begin after cleanup must succeed");
        data.commit(tx2).expect("commit after cleanup must succeed");
    }

    /// Nested begin returns Backend error, does not clear the first transaction.
    #[test]
    fn processor_store_nested_begin_returns_error() {
        let (_db, _kv, mut data) = make_processor_data();
        let tx1 = data.begin().expect("first begin must succeed");
        let rep1 = tx1.rep();
        let err = data.begin().expect_err("nested begin must return Err");
        assert!(
            matches!(err, ProcessorStoreError::Backend(_)),
            "nested begin must return Backend error"
        );
        // First transaction must still be active.
        assert!(
            data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must still be set after nested-begin error"
        );
        // Must be able to rollback the first transaction cleanly.
        data.rollback(Resource::new_borrow(rep1));
        assert!(
            !data.kv_store.as_ref().unwrap().is_tx_active(),
            "tx_active must be false after rollback"
        );
        data.drop(Resource::new_own(rep1))
            .expect("drop after rollback must succeed");
    }
}

// ── config::Host unit tests ───────────────────────────────────────────────────
//
// Direct trait-impl tests for config::Host on ProcessorData and StoreData.
// Same style as processor_store_host_tests above.

#[cfg(test)]
mod processor_config_host_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use wasmtime::component::ResourceTable;

    use super::*;
    use crate::store::{DEFAULT_MAX_PAGE_COUNT, KvStore};
    use processor_bindings::brenn::processor::config::Host;

    struct NoopAlerter;
    impl ProcessorAlerter for NoopAlerter {
        fn alert(&self, _severity: GuestAlertSeverity, _title: &str, _body: &str) {}
    }

    fn make_data_with_config(
        entries: &[(&str, &str)],
    ) -> (NamedTempFile, Arc<KvStore>, ProcessorData) {
        let db = NamedTempFile::new().unwrap();
        let kv = KvStore::open(db.path(), DEFAULT_MAX_PAGE_COUNT);
        let config: HashMap<String, String> = entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let data = ProcessorData {
            resource_table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().build(),
            kv_store: Some(Arc::clone(&kv)),
            output_ports: Arc::new(HashMap::new()),
            config: Arc::new(config),
            slug: Arc::from("test-slug"),
            max_payload_bytes: 1024,
            publish_buffer: Vec::new(),
            published_bytes: 0,
            publish_call_count: 0,
            publish_budget_by_sink: HashMap::new(),
            publish_suppressed_by_sink: HashMap::new(),
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: None,
            log_call_count: 0,
            alert_call_count: 0,
            tool_host: None,
            tool_request_buffer: Vec::new(),
            tool_call_count: 0,
        };
        (db, kv, data)
    }

    #[test]
    fn processor_config_get_present_key() {
        let (_db, _kv, mut data) = make_data_with_config(&[("foo", "bar"), ("baz", "qux")]);
        assert_eq!(data.get("foo".to_string()), Some("bar".to_string()));
        assert_eq!(data.get("baz".to_string()), Some("qux".to_string()));
    }

    #[test]
    fn processor_config_get_absent_key() {
        let (_db, _kv, mut data) = make_data_with_config(&[("foo", "bar")]);
        assert_eq!(data.get("absent".to_string()), None);
    }

    #[test]
    fn processor_config_get_empty_map() {
        let (_db, _kv, mut data) = make_data_with_config(&[]);
        assert_eq!(data.get("any".to_string()), None);
    }
}

#[cfg(test)]
mod replay_config_host_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use wasmtime::component::ResourceTable;

    use super::*;
    use crate::store::{DEFAULT_MAX_PAGE_COUNT, KvStore};
    use brenn::replay::config::Host;

    fn make_store_data_with_config(
        entries: &[(&str, &str)],
    ) -> (NamedTempFile, Arc<KvStore>, StoreData) {
        let db = NamedTempFile::new().unwrap();
        let kv = KvStore::open(db.path(), DEFAULT_MAX_PAGE_COUNT);
        let config: HashMap<String, String> = entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let data = StoreData {
            resource_table: ResourceTable::new(),
            kv_store: Arc::clone(&kv),
            config: Arc::new(config),
            // These config-host unit tests never run the store through a limiter;
            // a default (unbounded) StoreLimits is fine here.
            limits: StoreLimitsBuilder::new().build(),
        };
        (db, kv, data)
    }

    #[test]
    fn replay_config_get_present_key() {
        let (_db, _kv, mut data) = make_store_data_with_config(&[("brenn.max-skew-secs", "300")]);
        assert_eq!(
            data.get("brenn.max-skew-secs".to_string()),
            Some("300".to_string())
        );
    }

    #[test]
    fn replay_config_get_absent_key() {
        let (_db, _kv, mut data) = make_store_data_with_config(&[]);
        assert_eq!(data.get("brenn.max-skew-secs".to_string()), None);
    }
}
