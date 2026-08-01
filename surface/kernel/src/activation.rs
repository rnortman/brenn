//! Activation assembly: what one instance sees when it runs, and the scheduler
//! state around it.
//!
//! An activation is the surface's unit of component work. Assembling one means
//! windowing every input binding the instance holds, snapshotting what it has
//! parked on each of its outputs, and seeding the buffer its publishes go into —
//! all from one read, so nothing the component is shown can shift underneath the
//! indices it names.
//!
//! **What a binding is owed is not here.** It is a position inside its channel's
//! store, so the messages exist once, retained by the channel, and a loss is
//! *this binding's* accountable drop rather than a queue of copies going short.
//! What is here is the scheduler: whether an instance is running, its sink
//! carryover, and the lifetime counters its own telemetry reports.
//!
//! # Err consumes; retention is the recovery
//!
//! Every window advances its binding's position **at assembly**. A failed
//! activation is therefore never re-driven: returning err or trapping discards
//! the buffered publishes and nothing else, and what the activation saw reappears
//! only as retained context, in this or a later window whose `retain_depth` still
//! covers it. There is no gap-and-replay choreography and no terminal port
//! failure.
//!
//! # The loudness ladder
//!
//! The surface is the single page-side enforcement site for per-binding overflow
//! loudness, and the rungs are cumulative: `silent` does nothing beyond the
//! honest `dropped` figure the window already carries, `metered` counts per
//! binding here, `alarm` asks the caller for one coalesced announcement per
//! binding, and `fatal` asks it to kill the instance. Noise governs loudness
//! only — it never changes what happens to the data, which is always
//! drop-oldest.
//!
//! Counting and announcing happen at different moments. A loss is *counted* the
//! instant it happens, so a lagging binding is on the books whether or not it
//! ever runs again; the announcement names the whole delta the binding's next
//! window reports. That is why the store hands a serve two figures and why only
//! one of them feeds the counter.
//!
//! The verdicts leave as data. Which frame an announcement becomes, and how an
//! instance is taken terminal, are the caller's — the same seam every other
//! surface-side module answers across.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};

use brenn_attach_client::publish::DeferredViews;
use brenn_attach_client::router::{LocalRouter, Origin, PlanePolicy};
use brenn_attach_client::subs::SubscriptionDepths;
use brenn_queue::CursorOverflow;
use brenn_surface_contract::{
    Activation, ActivationError, DeferredEntry, DeferredWindow, PortWindow,
};
use brenn_surface_schema::NoiseLevel;
use uuid::Uuid;

use crate::bindings::{AppliedBindings, channel_is_transportable};
use crate::publish_buffer::{OutputSpec, PublishBuffer};
use crate::registry::{BindingKey, Registrations, SurfaceStores};

/// One instance's scheduler state: what it is doing, what it has spent, and what
/// it has lost.
///
/// Nothing here is retention. The positions and the messages live in the
/// channels' stores; this is the bookkeeping that surrounds an activation.
#[derive(Debug, Default, PartialEq, Eq)]
struct InstanceSchedule {
    /// Whether an activation is in flight. Invocations are serialized per
    /// instance: anything arriving during a handler coalesces into the next
    /// activation rather than overlapping this one.
    in_flight: bool,
    /// Activations whose entry returned err, lifetime. An err is a failed
    /// activation, not a death.
    activation_failures: u64,
    /// Per-output-port millitokens carried between activations, clamped to the
    /// port's `capacity_mt` when the next activation is seeded — the clamp is the
    /// seeding side's job, since only it knows an activation is starting.
    carry_mt: HashMap<String, u64>,
    /// Lifetime drops charged against each input binding whose resolved noise is
    /// `Metered` or louder, keyed by port. Every accountable loss lands here: an
    /// eviction that outran the position, a still-retained span the advance
    /// stepped over, and the peer's own upstream loss. A `Silent` binding never
    /// appears.
    metered_drops: BTreeMap<String, u64>,
    /// Schedules of this instance's that were lost, lifetime — refused at the
    /// channel's deferred cap, or dropped along with the store of a confined
    /// channel a bindings document stopped declaring. Both are losses of a timer
    /// the component believes it set, and the flush had no error channel back to
    /// it, so this counter is their only account.
    deferred_dropped: u64,
    /// Control ops that found their message already released, lifetime. The
    /// benign race a conforming component can always lose, worth counting because
    /// one that *always* races is scheduling too close to its own activation rate.
    deferred_races: u64,
}

/// The scheduler state of every instance the page holds an activation entry for.
///
/// Keyed in instance order, so which of several ready instances runs first is a
/// property of the wiring rather than of a hash seed — and the pick *rotates*
/// through that order, so a stable order does not become a fixed one.
#[derive(Debug, Default)]
pub struct Schedules {
    instances: BTreeMap<String, InstanceSchedule>,
    /// The instance most recently assembled, if any. The next pick resumes
    /// strictly *after* it in instance order, wrapping — round-robin rather than
    /// always-lowest-name.
    ///
    /// Fairness is not cosmetic here. A component that republishes onto a
    /// `local:` channel one of its own bindings reads is ready again the instant
    /// its flush routes, so a lowest-name-wins pick would hand every activation
    /// to the same instance forever and no sibling would ever run. The cursor
    /// bounds each instance to one activation per pass over the ready set, which
    /// is what makes a caller's per-turn dispatch budget a fair one.
    dispatch_cursor: Option<String>,
}

/// The read-side context one assembly needs: the wiring it resolves against, the
/// stores it windows, and the two deferral authorities it snapshots.
///
/// One bundle rather than a positional list: the members travel together and the
/// set grows with what an activation is composed from.
pub struct ActivationCtx<'a, P> {
    /// The wiring in force. Its declaration order is the order the windows
    /// appear in.
    pub bindings: &'a AppliedBindings,
    /// The page's channel stores — mutated, because a serve advances the
    /// position it served.
    pub stores: &'a mut SurfaceStores,
    /// The confined router, asked what this instance has parked on its
    /// page-local outputs.
    pub router: &'a LocalRouter<P>,
    /// The peer's mirror, asked the same question for outputs that cross the
    /// wire.
    pub views: &'a DeferredViews,
    /// The attachment's publish-body cap, applied to every class: a component's
    /// body-size contract must not change because an operator rebound its output
    /// port.
    pub max_body_bytes: u64,
    /// The wall clock this assembly was read at, epoch milliseconds UTC. A
    /// component gets time only here — an activation stays hermetic.
    pub now_ms: u64,
}

/// How an invoked activation entry finished.
///
/// Three outcomes, not two, because err and trap are different facts about the
/// component and the design gives them different consequences. The invocation
/// boundary discriminates them: a returned `Err` is `Err`, an unwind (a JS
/// exception under wasm, a `catch_unwind` natively) is `Trap`.
///
/// The two failure arms carry the component's own account of what went wrong.
/// The kernel never parses it — every err is treated identically — but it is the
/// only answer anyone has to "failed *how*?", so it rides through to the
/// diagnostic event rather than being dropped at the boundary that observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationOutcome {
    /// Returned ok. The buffer flushes atomically, in call order.
    Ok,
    /// Returned err, with the component's description of why. The buffer is
    /// discarded and a failure is counted; the instance keeps running and keeps
    /// being delivered. A failed activation is not a death — backend parity.
    Err(ActivationError),
    /// Panicked, with the unwind's message where one could be recovered. The
    /// buffer is discarded and the instance is terminal: its memory is presumed
    /// poisoned, so nothing further is delivered to it. Terminal for that one
    /// instance, never page death.
    Trap(String),
}

/// One activation, ready to invoke: which instance, what it sees, the buffer its
/// publishes go into, and what its bindings lost getting here.
///
/// Handed out with the instance already in flight, so there is no way to obtain
/// two of these for one instance.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadyActivation {
    pub instance: String,
    /// Which registration of that instance it was assembled for. Carried back on
    /// the completion, so work of a mount that has since gone lands nowhere: an
    /// instance unmounted and mounted again under the same id is a different
    /// component with the same spelling.
    pub generation: u64,
    pub activation: Activation,
    pub buffer: PublishBuffer,
    pub drops: DropVerdicts,
}

/// The loudness ladder's answers for one assembly, above the `metered` rung this
/// module enacts itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DropVerdicts {
    /// One entry per `alarm`-or-louder binding that lost something this window
    /// reports — the coalesced announcement, one per binding per activation.
    pub announce: Vec<DropAnnouncement>,
    /// The `fatal` rung's kills, at most one per instance and naming the first
    /// such binding of each. An instance dies once however many of *its* bindings
    /// overflowed together — but one retirement can evict the positions of
    /// several instances at once, and each of them was configured to die of it,
    /// so the kill is a set rather than a slot.
    pub fatal: Vec<DropAnnouncement>,
}

impl DropVerdicts {
    /// Whether anything above the `metered` rung fired.
    pub fn is_quiet(&self) -> bool {
        self.announce.is_empty() && self.fatal.is_empty()
    }

    /// Fold another set of verdicts in: announcements accumulate, and a kill joins
    /// the set unless the instance it names is already in it — an instance dies
    /// once, for the binding that asked first.
    pub fn merge(&mut self, other: Self) {
        self.announce.extend(other.announce);
        for kill in other.fatal {
            if !self.kills(&kill.instance) {
                self.fatal.push(kill);
            }
        }
    }

    /// Whether the set already holds a kill for `instance`.
    fn kills(&self, instance: &str) -> bool {
        self.fatal.iter().any(|held| held.instance == instance)
    }
}

/// One binding's loss, as the loud rungs name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropAnnouncement {
    /// The instance whose binding lost it. Carried rather than left to the
    /// caller's context: one retirement can overflow the positions of several
    /// instances at once, so the announcement has to name its own.
    pub instance: String,
    pub port: String,
    pub channel: String,
    /// The whole delta this window reports for the binding — every earlier
    /// charge's announcement was deferred to exactly this moment.
    pub dropped: u64,
}

impl DropAnnouncement {
    /// The operator-facing sentence for this loss, one spelling for the alert,
    /// the toast, and the kill reason.
    pub fn describe(&self) -> String {
        let Self {
            instance,
            port,
            channel,
            dropped,
        } = self;
        format!(
            "{instance}: dropped {dropped} message(s) on port {port} ({channel}) — input overflow"
        )
    }
}

/// One input binding's accountable loss at assembly, before the ladder is walked.
struct DropCharge {
    port: String,
    channel: String,
    noise: NoiseLevel,
    /// Charged to the metered counter, and what arms the `fatal` kill: this
    /// serve's own span, never one an eviction already charged.
    counted: u64,
    /// The delta the loud rungs announce, or `0` to announce nothing here.
    announced: u64,
}

impl Schedules {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start holding scheduler state for `instance`.
    ///
    /// # Panics
    ///
    /// On an instance already tracked — a page cannot hold two activation entries
    /// for one instance, and the registration table refuses the second before
    /// this is reached.
    pub fn track(&mut self, instance: &str) {
        let prior = self
            .instances
            .insert(instance.to_string(), InstanceSchedule::default());
        assert!(
            prior.is_none(),
            "surface client: {instance} is already scheduled"
        );
    }

    /// Drop `instance`'s scheduler state. Its counters go with it: they described
    /// a component that is no longer mounted, and a remount is a fresh one.
    ///
    /// # Panics
    ///
    /// On an instance this table does not hold.
    pub fn forget(&mut self, instance: &str) {
        assert!(
            self.instances.remove(instance).is_some(),
            "surface client: forgetting unscheduled instance {instance}"
        );
    }

    pub fn is_tracked(&self, instance: &str) -> bool {
        self.instances.contains_key(instance)
    }

    pub fn len(&self) -> usize {
        self.instances.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// Whether an activation of `instance` is in flight. `false` for an instance
    /// this table does not hold.
    pub fn in_flight(&self, instance: &str) -> bool {
        self.instances.get(instance).is_some_and(|s| s.in_flight)
    }

    /// Lifetime count of activations of `instance` whose entry returned err.
    pub fn activation_failures(&self, instance: &str) -> u64 {
        self.instances
            .get(instance)
            .map_or(0, |s| s.activation_failures)
    }

    /// Lifetime `metered`-rung drops charged against one input binding. Zero for
    /// a binding that never dropped and for a `Silent` one, which is uncounted.
    pub fn metered_drops(&self, instance: &str, port: &str) -> u64 {
        self.instances
            .get(instance)
            .and_then(|s| s.metered_drops.get(port))
            .copied()
            .unwrap_or(0)
    }

    /// Lifetime count of `instance`'s lost schedules: refused at a channel's
    /// deferred cap, or dropped with a store a document un-declared.
    pub fn deferred_dropped(&self, instance: &str) -> u64 {
        self.instances
            .get(instance)
            .map_or(0, |s| s.deferred_dropped)
    }

    /// Lifetime count of `instance`'s control ops that lost the release race.
    pub fn deferred_races(&self, instance: &str) -> u64 {
        self.instances.get(instance).map_or(0, |s| s.deferred_races)
    }

    /// Charge one lost schedule to `instance` — refused at its channel's cap, or
    /// dropped with the channel's store. Silently ignored for an instance this
    /// table does not hold — a flush can outlive its registration.
    pub fn count_deferred_drop(&mut self, instance: &str) {
        if let Some(schedule) = self.instances.get_mut(instance) {
            schedule.deferred_dropped += 1;
        }
    }

    /// Charge one lost release race to `instance`. Silently ignored for an
    /// instance this table does not hold, for the same reason.
    pub fn count_deferred_race(&mut self, instance: &str) {
        if let Some(schedule) = self.instances.get_mut(instance) {
            schedule.deferred_races += 1;
        }
    }

    /// The next instance ready to run, or `None` when nothing is.
    ///
    /// Ready is three questions: the instance may run at all (registered, not
    /// terminal, nothing of its own in flight), and one of its bindings is owed a
    /// message its channel still holds. The owed half is asked of the positions
    /// rather than of a queue of copies, which is where coalescing comes from —
    /// arrivals move no position, so an instance woken three times is run once and
    /// the window it is assembled from serves the newest.
    ///
    /// The pick resumes after the last instance [`Self::assemble`] handed out and
    /// wraps, so each ready instance gets one activation per pass over the set and
    /// none can starve its siblings (see `dispatch_cursor`). Advancing is the
    /// assembly's, not this question's: asking twice without dispatching answers
    /// the same instance twice.
    pub fn ready<'a>(
        &'a self,
        registrations: &Registrations,
        stores: &SurfaceStores,
    ) -> Option<&'a str> {
        let runnable: Vec<&'a str> = self
            .instances
            .iter()
            .filter(|(instance, schedule)| {
                !schedule.in_flight
                    && registrations.is_registered(instance)
                    && !registrations.is_failed(instance)
            })
            .map(|(instance, _)| instance.as_str())
            .collect();
        // A cursor naming an instance that is no longer runnable — or gone
        // entirely — still lands at the right place, since the resume point is
        // the name's position in the order rather than an index.
        let start = match &self.dispatch_cursor {
            Some(last) => runnable.partition_point(|instance| *instance <= last.as_str()),
            None => 0,
        };
        runnable[start..]
            .iter()
            .chain(runnable[..start].iter())
            .copied()
            .find(|instance| stores.any_deliverable(|key: &BindingKey| key.instance == *instance))
    }

    /// Whether any instance is ready to run — the wake question, at the grain the
    /// driver's turn is budgeted in.
    pub fn has_ready(&self, registrations: &Registrations, stores: &SurfaceStores) -> bool {
        self.ready(registrations, stores).is_some()
    }

    /// Assemble one activation for `instance`: window every input binding,
    /// snapshot every output's schedule, seed the buffer, and walk the loudness
    /// ladder.
    ///
    /// The instance is in flight when this returns, whatever the caller then does
    /// with the activation — including declining to run it because the `fatal`
    /// rung killed the instance. It is also where the dispatch rotation advances,
    /// so the next [`Self::ready`] resumes after this instance whether or not the
    /// activation is ultimately invoked.
    ///
    /// `generation` is the instance's registration at this moment, carried on the
    /// answer so a completion can be matched to the mount that produced it.
    ///
    /// # Panics
    ///
    /// On an instance this table does not hold, one already in flight, or a
    /// binding whose channel has no store or whose position is missing. The last
    /// two are invariants of the reconcile passes, which create stores and
    /// positions together: answering a broken one with an empty window would leave
    /// a component silently starved of a port it is bound to.
    pub fn assemble<P: PlanePolicy>(
        &mut self,
        instance: &str,
        generation: u64,
        ctx: &mut ActivationCtx<'_, P>,
    ) -> ReadyActivation {
        let schedule = self
            .instances
            .get_mut(instance)
            .unwrap_or_else(|| panic!("surface client: assembling for unscheduled {instance}"));
        assert!(
            !schedule.in_flight,
            "surface client: {instance} already has an activation in flight"
        );
        schedule.in_flight = true;
        self.dispatch_cursor = Some(instance.to_string());

        let (ports, charges) = window_ports(instance, ctx);
        let drops = self.enact_charges(instance, charges);
        let (deferred, deferred_ids) = deferred_windows(instance, ctx);
        let buffer = self.seed_buffer(instance, &ports, deferred_ids, ctx);
        ReadyActivation {
            instance: instance.to_string(),
            generation,
            activation: Activation {
                ports,
                deferred,
                now: Some(ctx.now_ms),
            },
            buffer,
            drops,
        }
    }

    /// Charge the ladder for retirements that outran positions on `channel` — an
    /// arrival, or a depth shrink, evicting what a binding had not been served.
    ///
    /// Charged where it happens rather than at the binding's next window, so a
    /// lagging binding is on the books whether or not it ever runs again. Only the
    /// `fatal` rung announces here: the kill ends the instance, so there is no next
    /// window to announce at, while every softer rung waits for one and names the
    /// whole coalesced delta there. The still-retained remainder is counted at that
    /// window instead, so no span is counted twice.
    ///
    /// Answered in binding order, so a page several of whose bindings overflowed on
    /// one retirement reports them reproducibly.
    pub fn charge_overflow(
        &mut self,
        bindings: &AppliedBindings,
        channel: &str,
        overflow: Vec<CursorOverflow<BindingKey>>,
    ) -> DropVerdicts {
        let mut charged = overflow;
        charged.sort_by(|a, b| a.subscriber.cmp(&b.subscriber));
        let mut verdicts = DropVerdicts::default();
        for CursorOverflow {
            subscriber,
            evicted,
        } in charged
        {
            let BindingKey { instance, port } = subscriber;
            // A position can outlive its binding for exactly one pass: the store
            // reconcile trims a shrunk channel before the registered reconcile drops
            // the positions the same document unwired. Nothing is owed a charge on a
            // binding the operator has removed — the position is about to go with it.
            let Some(noise) = binding_noise(bindings, &instance, &port) else {
                continue;
            };
            let charge = DropCharge {
                port,
                channel: channel.to_string(),
                noise,
                counted: evicted,
                announced: if noise >= NoiseLevel::Fatal {
                    evicted
                } else {
                    0
                },
            };
            verdicts.merge(self.enact_charges(&instance, vec![charge]));
        }
        verdicts
    }

    /// Walk the ladder for a set of charges: count the `metered` rung here, hand
    /// the loud rungs back.
    ///
    /// The counter is best-effort against the table — a retirement can name a
    /// binding whose instance has already gone — while the loud rungs report
    /// regardless: an operator-visible loss does not become invisible because the
    /// component that suffered it left.
    fn enact_charges(&mut self, instance: &str, charges: Vec<DropCharge>) -> DropVerdicts {
        let mut verdicts = DropVerdicts::default();
        for charge in charges {
            if charge.noise >= NoiseLevel::Metered
                && charge.counted > 0
                && let Some(schedule) = self.instances.get_mut(instance)
            {
                *schedule
                    .metered_drops
                    .entry(charge.port.clone())
                    .or_insert(0) += charge.counted;
            }
            let announcement = DropAnnouncement {
                instance: instance.to_string(),
                port: charge.port,
                channel: charge.channel,
                dropped: charge.announced,
            };
            if charge.noise >= NoiseLevel::Fatal && charge.counted > 0 && !verdicts.kills(instance)
            {
                verdicts.fatal.push(announcement.clone());
            }
            if charge.noise >= NoiseLevel::Alarm && charge.announced > 0 {
                verdicts.announce.push(announcement);
            }
        }
        verdicts
    }

    /// Seed one activation's publish buffer: the instance's outputs, their sink
    /// buckets, and the body cap.
    ///
    /// Buckets are `seed_sink_budget(carry, budget, grant)` — the same arithmetic
    /// the backend runs, so a component's budget means the same thing on either
    /// hosting. The grant is the input amplification at the uniform default
    /// (`MILLITOKENS_PER_PUBLISH` per **new** envelope, never context), so a
    /// component that republishes what it consumes stays solvent at 1:1 without an
    /// operator raising a knob.
    ///
    /// `deferred_ids` is what the component's own deferred windows just presented,
    /// in window order: an index the component names is resolved through it and
    /// nothing else, so a resolution cannot address a message it was never shown.
    fn seed_buffer<P>(
        &self,
        instance: &str,
        ports: &[PortWindow],
        deferred_ids: HashMap<String, Vec<Uuid>>,
        ctx: &ActivationCtx<'_, P>,
    ) -> PublishBuffer {
        let grant = brenn_budget::grant_input_mt(
            ports
                .iter()
                .map(|w| (brenn_budget::MILLITOKENS_PER_PUBLISH, w.new_len())),
        );
        let carry = &self
            .instances
            .get(instance)
            .expect("surface client: seeding a buffer for an unscheduled instance")
            .carry_mt;
        let mut outputs = HashMap::new();
        let mut sink_mt = HashMap::new();
        for binding in ctx.bindings.outputs_of(instance) {
            outputs.insert(
                binding.port.clone(),
                OutputSpec {
                    channel: binding.channel.clone(),
                    default_urgency: binding.urgency,
                },
            );
            sink_mt.insert(
                binding.port.clone(),
                brenn_budget::seed_sink_budget(
                    carry.get(&binding.port).copied().unwrap_or(0),
                    brenn_budget::SinkBudget {
                        fill_mt: binding.fill_mt,
                        capacity_mt: binding.capacity_mt,
                    },
                    grant,
                ),
            );
        }
        PublishBuffer::new(outputs, sink_mt, ctx.max_body_bytes, deferred_ids)
    }

    /// An activation returned ok: the instance is idle again and its unspent
    /// millitokens carry.
    ///
    /// # Panics
    ///
    /// On an instance this table does not hold. A completion for an instance that
    /// deregistered mid-flight is the caller's to absorb — it holds the entry, and
    /// nothing here can order the two.
    pub fn finish_ok(&mut self, instance: &str, carry: HashMap<String, u64>) {
        let schedule = self.schedule_mut(instance);
        schedule.in_flight = false;
        schedule.carry_mt = carry;
    }

    /// An activation returned err: idle again, the failure counted, and the carry
    /// still returned. What a component *spent* is a fact about the activation
    /// that happened, and an err does not un-spend it.
    ///
    /// # Panics
    ///
    /// On an instance this table does not hold.
    pub fn finish_err(&mut self, instance: &str, carry: HashMap<String, u64>) {
        let schedule = self.schedule_mut(instance);
        schedule.in_flight = false;
        schedule.carry_mt = carry;
        schedule.activation_failures += 1;
    }

    /// The instance is terminal: nothing is in flight any more. Its counters stay
    /// — they are the account of what happened before it died — and taking it out
    /// of delivery is the registration table's half.
    ///
    /// # Panics
    ///
    /// On an instance this table does not hold.
    pub fn finish_terminal(&mut self, instance: &str) {
        self.schedule_mut(instance).in_flight = false;
    }

    fn schedule_mut(&mut self, instance: &str) -> &mut InstanceSchedule {
        self.instances
            .get_mut(instance)
            .unwrap_or_else(|| panic!("surface client: unscheduled instance {instance}"))
    }
}

/// The resolved noise of one input binding, or `None` when the wiring does not
/// hold it.
fn binding_noise(bindings: &AppliedBindings, instance: &str, port: &str) -> Option<NoiseLevel> {
    bindings
        .inputs_of(instance)
        .find(|b| b.port == port)
        .map(|b| b.noise)
}

/// Window every input binding of `instance`, in declaration order, and collect
/// what each of them lost.
///
/// Every bound port appears, present or not: a port with nothing new is a
/// pure-context window, and a component must be able to read every port's view on
/// every activation.
fn window_ports<P>(
    instance: &str,
    ctx: &mut ActivationCtx<'_, P>,
) -> (Vec<PortWindow>, Vec<DropCharge>) {
    // Lifted out first so the loop below can borrow the stores; cloning the whole
    // table instead would make every activation pay for every sibling's wiring.
    let inputs: Vec<(String, String, SubscriptionDepths, NoiseLevel)> = ctx
        .bindings
        .inputs_of(instance)
        .map(|b| {
            (
                b.port.clone(),
                b.channel.clone(),
                SubscriptionDepths {
                    push_depth: b.push_depth,
                    retain_depth: b.retain_depth,
                },
                b.noise,
            )
        })
        .collect();
    let mut ports = Vec::with_capacity(inputs.len());
    let mut charges = Vec::new();
    for (port, channel, depths, noise) in inputs {
        let key = BindingKey::new(instance, &port);
        let served = ctx
            .stores
            .get_mut(&channel)
            .unwrap_or_else(|| {
                panic!("surface client: {instance}'s port {port} is bound to {channel}, which has no store")
            })
            .serve(&key, depths)
            .unwrap_or_else(|| {
                panic!(
                    "surface client: {instance}'s push-enabled port {port} holds no position in \
                     {channel}"
                )
            });
        if served.counted > 0 || served.dropped > 0 {
            charges.push(DropCharge {
                port: port.clone(),
                channel,
                noise,
                counted: served.counted,
                announced: served.dropped,
            });
        }
        ports.push(PortWindow {
            port,
            new_from: u32::try_from(served.new_from)
                .expect("surface client: a window's depth is a config-bounded page-memory value"),
            envelopes: served.envelopes,
            dropped: served.dropped,
        });
    }
    (ports, charges)
}

/// One deferred window per bound output port of `instance`, in declaration order,
/// and the identity behind each entry of each window.
///
/// Every bound output appears, empty or not, so an index into the window means the
/// same thing on every activation. Each window is scoped to the instance's own
/// sender, so a channel two components park on still shows each of them only its
/// own schedule — and the two authorities answer the same entry shape, so the
/// class of the channel changes nothing here but where the question is asked.
///
/// The identities are taken from this one read for the reason the indices are: a
/// second read could have released an entry and shifted every index after it.
fn deferred_windows<P: PlanePolicy>(
    instance: &str,
    ctx: &ActivationCtx<'_, P>,
) -> (Vec<DeferredWindow>, HashMap<String, Vec<Uuid>>) {
    let mut windows = Vec::new();
    let mut ids = HashMap::new();
    for binding in ctx.bindings.outputs_of(instance) {
        let parked = if channel_is_transportable(&binding.channel) {
            // The peer is this channel's deferral authority, so the window is the
            // snapshot it last pushed: already release-ordered, already
            // sender-scoped, and empty where it has said nothing.
            ctx.views.get(&binding.channel, Some(instance)).to_vec()
        } else {
            ctx.router.parked_for(
                &*ctx.stores,
                &binding.channel,
                Origin::Sub(instance),
                ctx.now_ms,
            )
        };
        let (entries, port_ids): (Vec<DeferredEntry>, Vec<Uuid>) = parked
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                (
                    DeferredEntry {
                        index: u32::try_from(index).expect(
                            "surface client: a deferred set is bounded by its channel's depth",
                        ),
                        // The body, not the envelope: what a component gets back
                        // is what it handed over, on every hosting.
                        payload: entry.body,
                        deliver_after: entry.deliver_after,
                    },
                    entry.message_id,
                )
            })
            .unzip();
        ids.insert(binding.port.clone(), port_ids);
        windows.push(DeferredWindow {
            port: binding.port.clone(),
            entries,
        });
    }
    (windows, ids)
}
