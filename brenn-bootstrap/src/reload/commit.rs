//! The commit phase: walking the running process to the prepared plan.
//!
//! [`prepare`](super::driver::ReloadDriver::prepare) has already refused
//! everything that could be refused — the document compiles, lowers, and
//! differs from the baseline only in ways this phase knows how to walk, and
//! every component the delta brings into service is loaded and instantiated.
//! One question is asked again here before anything is touched — whether a
//! subscriber the plan cannot see has landed, since that answer moves while
//! prepare runs — and past that there is nothing left that can decline: a
//! failure is a host bug and panics as one, and the panic takes the process
//! with it, because a half-applied reload is a running system no document
//! describes.
//!
//! The order of the five steps below is the whole of the design. Consumers leave
//! before channels move, so nothing wakes a task that is on its way out; channels
//! are described, then removed, then added, so a rename frees its address before
//! the new entry claims it; consumers arrive last, so every channel they are
//! folded onto is already there. What each step touches is exactly what a fresh
//! boot of the candidate would have produced, which is the property the whole
//! facility rests on.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{info, warn};
use uuid::Uuid;

use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{ChannelEntry, ParticipantId, SubscriberEntryKind};
use brenn_messaging::{Messenger, WASM_WINDOW_MAX_NEW};
use brenn_messaging_boot::MessagingPlan;
use brenn_server::messaging_router::DeliveryBinding;

use brenn_wasm_dispatch::ConsumerHandle;

use crate::consumers::{ConsumerRegistry, LoadedConsumer, RunningConsumer, start_consumer};
use crate::reload::delta::{PlanDelta, live_subscriber_refusals};
use crate::reload::driver::ReloadEnv;

/// Apply a prepared reload to the running process.
///
/// One check runs before the first mutation and can still decline: the live
/// directory is asked again whether a subscriber the plan cannot see has landed
/// on a channel this walk would take away. Prepare asked that question too, but
/// the answer can change between the two — a dynamic app subscription or an
/// attach-minted surface entry arrives on the channel while prepare is hashing
/// and compiling the arriving components. Nothing has been touched yet at that
/// point, so it is a refusal like any other.
///
/// # Panics
///
/// On anything else that does not go through, including that same check asked
/// once more after the departing consumers have stopped — by then the walk has
/// begun and there is nothing to decline with. Every one of them is a host bug:
/// a consumer the delta names that the registry does not hold, a registration
/// the plan does not carry, a channel the directory will not take. The
/// alternative to panicking is a process running half of two documents; the
/// reload driver task's own abort guard is what makes the panic mean process
/// death rather than one unwound task.
pub(crate) async fn apply(
    env: &ReloadEnv,
    registry: &mut ConsumerRegistry,
    plan: &MessagingPlan,
    delta: &PlanDelta,
    loaded: Vec<(String, LoadedConsumer)>,
) -> Result<(), Vec<String>> {
    let arrived = live_subscriber_refusals(delta, env.messenger.directory());
    if !arrived.is_empty() {
        return Err(arrived);
    }

    retire_consumers(env, registry, plan, delta).await;
    // Asked again, because step 1's wait for a stopping consumer is unbounded
    // and a subscriber can arrive during it. Past the retirements nothing can
    // be declined, so a hit here is the process's life against a subscriber
    // silently dropped from a channel that is about to be re-created.
    let arrived = live_subscriber_refusals(delta, env.messenger.directory());
    assert!(
        arrived.is_empty(),
        "reload commit: a subscriber arrived on a channel this reload is taking away, after the \
         departing consumers had already stopped: {arrived:?}",
    );
    describe_channels(env, delta).await;
    remove_channels(env, delta);
    add_channels(env, delta).await;
    start_consumers(env, registry, plan, delta, loaded).await;

    // The same cross-check boot runs over its own wiring, asked of the wiring
    // this reload just produced. A failure is a defect in the steps above, not
    // a verdict on the document — the document was accepted before any of this
    // ran.
    crate::assert_every_subscriber_wired(&env.messenger, &env.router);
    Ok(())
}

/// Step 1: take every departing and replaced consumer out of service.
///
/// The order inside is load-bearing. The directory entries go first, so no new
/// snapshot names the consumer and no further wake is raised for it. The stop
/// signal and the join come next, so the activation in flight finishes and its
/// publishes go out — they were owed to the old document, and they are made
/// under a registration that is still live, so they are ACL-checked and
/// delivered as any other. Only then do the binding and the registration become
/// tombstones, which is what makes a wake still in flight resolve "gone"
/// instead of tearing the process down.
async fn retire_consumers(
    env: &ReloadEnv,
    registry: &mut ConsumerRegistry,
    plan: &MessagingPlan,
    delta: &PlanDelta,
) {
    for slug in departing(delta) {
        let kind = SubscriberEntryKind::Wasm(slug.clone());
        let live = env.messenger.directory();

        // Where it was subscribed, read before the entries are edited: the
        // positions it holds are on exactly these channels.
        let was_on: Vec<(Uuid, String)> = live
            .list()
            .iter()
            .filter(|entry| holds(entry, &kind))
            .map(|entry| (entry.uuid, entry.address.clone()))
            .collect();
        for (uuid, address) in &was_on {
            assert!(
                live.remove_subscriber(uuid, &kind).is_some(),
                "reload commit: consumer {slug:?} was read as a subscriber of channel \
                 {address:?} and the directory no longer holds it there — host bug",
            );
        }

        let running = registry.remove(&slug).unwrap_or_else(|| {
            panic!(
                "reload commit: consumer {slug:?} is in the delta but not in the registry — the \
                 delta was computed against what is running, so this is a host bug"
            )
        });
        let RunningConsumer {
            component, handle, ..
        } = running;
        stop_and_report(&slug, handle).await;
        // The component holds the consumer's KV store open, and the store file
        // admits one holder. Dropping it here — after the task that shares it
        // has joined — is what lets a replacement under the same slug open the
        // same file when it starts.
        drop(component);

        env.router.retire_delivery_binding(&kind);
        env.messenger.retire_subscriber_registration(&kind);
        if let Some(grants) = &env.tool_caller_grants {
            grants.remove_caller(ParticipantId::for_wasm(&slug).as_str());
        }

        // The cursor rows a fresh boot of the candidate would reap as orphans,
        // and only those: a position on a channel the candidate still has this
        // consumer reading is what a restart carries over, so a replaced
        // consumer resumes where it was rather than re-reading the retained
        // tail. A removed consumer keeps none, because the candidate holds no
        // subscription of its at all.
        let keeping: HashSet<Uuid> = plan
            .directory
            .list()
            .iter()
            .filter(|entry| holds(entry, &kind))
            .map(|entry| entry.uuid)
            .collect();
        let participant = ParticipantId::for_wasm(&slug);
        for (uuid, address) in &was_on {
            if !keeping.contains(uuid) {
                env.messenger.detach_subscriber(address, &participant).await;
            }
        }
        info!(slug = %slug, "reload: consumer retired");
    }
}

/// How often a consumer that has not stopped is named in the journal.
const STOP_WAIT_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Stop one consumer's task and wait for it, saying so while the wait lasts.
///
/// The wait is unbounded on purpose — a drain step that has begun runs to
/// completion, and cutting it short would drop publishes the old document was
/// owed. A guest that wedges anyway wedges this await, and with it every later
/// trigger, since the driver decides one reload at a time. So the wait is
/// reported rather than worked around: the slug is in the journal before the
/// await, and again every [`STOP_WAIT_REPORT_INTERVAL`] until it returns, which
/// is what turns "reload does nothing" into a name and an elapsed time.
async fn stop_and_report(slug: &str, handle: ConsumerHandle) {
    report_while(slug, handle.stop_and_join()).await;
}

/// Await `stopping`, naming `slug` in the journal until it resolves.
///
/// Returns only when `stopping` does. The tick arm is a report and never an
/// exit: returning early would drop the component — and with it the consumer's
/// KV store handle — while the task that shares it is still alive, so a
/// replacement under the same slug could not open the store.
async fn report_while(slug: &str, stopping: impl Future<Output = ()>) {
    info!(slug = %slug, "reload: stopping consumer");
    let since = std::time::Instant::now();
    let mut stopping = std::pin::pin!(stopping);
    let mut ticks = tokio::time::interval(STOP_WAIT_REPORT_INTERVAL);
    // The first tick completes immediately; it is this moment, already logged.
    ticks.tick().await;
    loop {
        tokio::select! {
            () = &mut stopping => return,
            _ = ticks.tick() => warn!(
                slug = %slug,
                waited_secs = since.elapsed().as_secs(),
                "reload: consumer has not stopped"
            ),
        }
    }
}

/// Step 2a: the entries that differ only in their description.
///
/// Metadata, so it is set in place: the entry keeps its uuid, its tuning and
/// every subscriber on it, and no consumer is restarted. A durable entry's row
/// is re-upserted; the resume epoch must be preserved.
async fn describe_channels(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let mut rows = Vec::new();
    for entry in &delta.channels_described {
        let applied = live.set_description(&entry.uuid, entry.description.clone());
        assert!(
            applied,
            "reload commit: channel {:?} took a description update but is not in the live \
             directory",
            entry.address,
        );
        if entry.capabilities().durable {
            let updated = live.by_uuid(&entry.uuid).expect("just described");
            rows.push(ChannelEntry::clone(&updated));
        }
        info!(address = %entry.address, "reload: channel described");
    }
    upsert(&env.messenger, &rows).await;
}

/// Step 2b: the entries the candidate does not have, and the old side of every
/// entry that moved.
///
/// A non-durable entry's ring goes with it — that is where its messages lived —
/// and so do the send-rate buckets every sender holds against it, which were
/// built at the departing entry's rate. A durable entry's row stays:
/// `upsert_channels`' contract is that a UUID the config no longer names is kept
/// for an operator to delete deliberately, which is what a restart does with it,
/// so it is what a reload does with it.
fn remove_channels(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let mut forgotten: Vec<Uuid> = Vec::new();
    for entry in leaving(delta) {
        let removed = live.remove_channel(&entry.uuid);
        assert!(
            removed,
            "reload commit: channel {:?} is in the delta but not in the live directory",
            entry.address,
        );
        if !entry.capabilities().durable {
            env.messenger.ring_stores().deregister(&entry.uuid);
        }
        forgotten.push(entry.uuid);
        info!(address = %entry.address, "reload: channel removed");
    }
    env.messenger.forget_send_rate_buckets(&forgotten);
}

/// Step 3: the entries the candidate has and the baseline did not, and the new
/// side of every entry that moved.
///
/// Each goes in with no subscribers. Rule 1 of the convergibility rules has
/// already established that every subscriber on a channel-delta entry is a
/// consumer the delta also moves, and step 5 folds each of those in as it
/// starts — so an entry that arrives empty here is an entry that is complete
/// here.
async fn add_channels(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let arriving: Vec<ChannelEntry> = joining(delta)
        .map(|entry| {
            let mut fresh = ChannelEntry::clone(entry);
            fresh.subscribers.clear();
            fresh
        })
        .collect();
    // The row before the directory entry, as at boot: an existing row keeps its
    // resume epoch, so a channel that was retuned rather than renamed keeps its
    // history.
    let rows: Vec<ChannelEntry> = arriving
        .iter()
        .filter(|entry| entry.capabilities().durable)
        .cloned()
        .collect();
    upsert(&env.messenger, &rows).await;
    for entry in arriving {
        if !entry.capabilities().durable {
            env.messenger.ring_stores().register(&entry);
        }
        info!(address = %entry.address, "reload: channel added");
        live.add_channel(entry);
    }
}

/// Step 4: put every arriving and replaced consumer into service.
///
/// The wiring is registered before the task exists, in the order boot uses: the
/// registration first, because a delivery gate that finds a subscriber without
/// one treats it as a host bug; then the subscriber entries, taken from the
/// plan rather than re-derived, so the entry this consumer joins a channel with
/// is byte-for-byte the one a fresh boot would have folded; then the delivery
/// binding, so a wake raised by the priming below has somewhere to land; then
/// the position, which primes behind the retained tail exactly as at boot; and
/// only then the task.
async fn start_consumers(
    env: &ReloadEnv,
    registry: &mut ConsumerRegistry,
    plan: &MessagingPlan,
    delta: &PlanDelta,
    loaded: Vec<(String, LoadedConsumer)>,
) {
    let mut loaded: HashMap<String, LoadedConsumer> = loaded.into_iter().collect();
    let live = env.messenger.directory();
    let mut primed_any = false;

    for slug in arriving(delta) {
        let kind = SubscriberEntryKind::Wasm(slug.clone());
        let consumer = plan
            .wasm_consumers
            .iter()
            .find(|consumer| consumer.slug == slug)
            .unwrap_or_else(|| {
                panic!(
                    "reload commit: consumer {slug:?} is in the delta but not in the plan it was \
                     computed from — host bug"
                )
            });
        let registration = plan.registrations.get(&kind).cloned().unwrap_or_else(|| {
            panic!(
                "reload commit: the plan carries no subscriber registration for consumer \
                 {slug:?} — every resolved consumer has one, so this is a host bug"
            )
        });
        env.messenger
            .register_subscriber_registration(kind.clone(), registration);

        for entry in plan.directory.list() {
            let Some(subscriber) = entry
                .subscribers
                .iter()
                .find(|sub| sub.kind.same_principal(&kind))
            else {
                continue;
            };
            let applied = live.add_subscriber(&entry.uuid, subscriber.clone());
            assert!(
                applied,
                "reload commit: consumer {slug:?} subscribes to channel {:?}, which the live \
                 directory does not hold — host bug",
                entry.address,
            );
        }

        let one = loaded.remove(&slug).unwrap_or_else(|| {
            panic!(
                "reload commit: consumer {slug:?} is arriving but prepare loaded no component for \
                 it — host bug"
            )
        });
        env.router.register_delivery_binding(
            kind.clone(),
            DeliveryBinding::ParkedNotify(one.notify.clone()),
        );
        if let Some(grants) = &env.tool_caller_grants {
            // Off the plan, which is the one derivation of this table: a caller
            // the plan does not name may address no tool, and withdrawing it is
            // how a consumer that lost its grants stops being able to.
            let caller = ParticipantId::for_wasm(&slug).as_str().to_owned();
            match plan.tool_caller_grants.get(&caller) {
                Some(granted) => grants.set_caller(caller, granted.clone()),
                None => grants.remove_caller(&caller),
            }
        }

        let participant = ParticipantId::for_wasm(&slug);
        for input in &consumer.inputs {
            // Must match the depth the port's window reads at, or the first
            // read retunes the cursor.
            let push_depth = Depth::Bounded(input.sub.push_depth.clamped_to(WASM_WINDOW_MAX_NEW));
            let attached = env
                .messenger
                .attach_subscriber(&input.sub.channel_address, &slug, &participant, push_depth)
                .await;
            primed_any |= attached == brenn_messaging_store::store::Attached::Created;
        }

        registry.insert(
            slug.clone(),
            start_consumer(one, consumer, &env.messenger, &env.alert_dispatcher),
        );
        info!(slug = %slug, "reload: consumer started");
    }

    assert!(
        loaded.is_empty(),
        "reload commit: prepare loaded components nothing started ({:?}) — host bug",
        loaded.keys().collect::<Vec<_>>(),
    );
    // Drain the primed backlog now rather than at the next poll, as boot does.
    if primed_any {
        env.messenger.dispatch_kick();
    }
}

/// The consumers leaving service: removed outright, or replaced by a new
/// instance under the same slug.
fn departing(delta: &PlanDelta) -> Vec<String> {
    delta
        .consumers_removed
        .iter()
        .chain(delta.consumers_changed.iter())
        .cloned()
        .collect()
}

/// The consumers entering service: added outright, or the replacement half of a
/// change.
fn arriving(delta: &PlanDelta) -> Vec<String> {
    delta
        .consumers_added
        .iter()
        .chain(delta.consumers_changed.iter())
        .cloned()
        .collect()
}

/// The entries leaving the directory: removed outright, or the old side of a
/// change, which the commit treats as a removal followed by an addition.
fn leaving(delta: &PlanDelta) -> impl Iterator<Item = &Arc<ChannelEntry>> {
    delta
        .channels_removed
        .iter()
        .chain(delta.channels_changed.iter().map(|change| &change.old))
}

/// The entries joining the directory: added outright, or the new side of a
/// change.
fn joining(delta: &PlanDelta) -> impl Iterator<Item = &Arc<ChannelEntry>> {
    delta
        .channels_added
        .iter()
        .chain(delta.channels_changed.iter().map(|change| &change.new))
}

/// Whether an entry carries this subscriber.
fn holds(entry: &ChannelEntry, kind: &SubscriberEntryKind) -> bool {
    entry
        .subscribers
        .iter()
        .any(|sub| sub.kind.same_principal(kind))
}

/// Write the durable rows of `entries`, if there are any.
async fn upsert(messenger: &Arc<Messenger>, entries: &[ChannelEntry]) {
    if entries.is_empty() {
        return;
    }
    let conn = messenger.db().lock().await;
    brenn_messaging_store::db::upsert_channels(&conn, entries);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// The wait loop returns when the stop does, and not on a report tick.
    ///
    /// Time is paused, so the several reporting intervals this passes through
    /// cost nothing; what they buy is the arm that a fixture consumer — which
    /// always stops at once — never reaches.
    #[tokio::test(start_paused = true)]
    async fn the_stop_wait_reports_and_never_returns_early() {
        let stopped = Arc::new(AtomicBool::new(false));
        let waiting = {
            let stopped = stopped.clone();
            async move {
                tokio::time::sleep(STOP_WAIT_REPORT_INTERVAL * 5).await;
                stopped.store(true, Ordering::SeqCst);
            }
        };
        report_while("wedged", waiting).await;
        assert!(
            stopped.load(Ordering::SeqCst),
            "the wait returned before the consumer stopped, which would drop the component out \
             from under a task still holding its store",
        );
    }
}
