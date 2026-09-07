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
//! The order of the steps below is the whole of the design. Consumers and
//! surfaces leave before channels move, so nothing wakes a task that is on its
//! way out; channels are described, then removed, then added, so a rename frees
//! its address before the new entry claims it; the surface asset roots and the
//! two surface-description registrations are swapped next, so the documents
//! republished after them have a writer whose policy admits their addresses;
//! surfaces and then consumers arrive last, so every channel they are folded
//! onto is already there. What each step touches is exactly what a fresh boot
//! of the candidate would have produced, which is the property the whole
//! facility rests on.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{info, warn};
use uuid::Uuid;

use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{ChannelEntry, ParticipantId, SubscriberEntry, SubscriberEntryKind};
use brenn_lib::wasm_package::Verified;
use brenn_messaging::{Messenger, WASM_WINDOW_MAX_NEW};
use brenn_messaging_boot::MessagingPlan;
use brenn_server::messaging_router::DeliveryBinding;

use brenn_wasm_dispatch::ConsumerHandle;

use brenn_mqtt::{IngressSubscribeOutcome, IngressUnsubscribeOutcome};

use brenn_lib::messaging::identity::AttachScope;
use brenn_server::routes::surface::SurfaceCloseReason;
use brenn_surface_server::SurfaceRuntime;

use crate::consumers::{ConsumerRegistry, LoadedConsumer, RunningConsumer, start_consumer};
use crate::reload::delta::{PlanDelta, live_subscriber_refusals};
use crate::reload::driver::ReloadEnv;
use crate::reload::mqtt::address_of;
use crate::reload::surfaces::{Arrival, SurfaceDocs};

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
    records: &HashMap<String, Verified>,
    surfaces: &SurfaceCommit<'_>,
) -> Result<Vec<String>, Vec<String>> {
    let arrived = live_subscriber_refusals(delta, env.messenger.directory());
    if !arrived.is_empty() {
        return Err(arrived);
    }

    let channels = plan.directory.list();
    let planned = PlannedSubscribers::of(&channels);

    retire_consumers(env, registry, plan, delta).await;
    retire_surfaces(env, delta).await;
    // Asked again, because the two retirements' waits — for a stopping consumer
    // and for a closing attach session — are unbounded
    // and a subscriber can arrive during either. Past them nothing can be
    // declined, so a hit here is the process's life against a subscriber
    // silently dropped from a channel that is about to be re-created.
    let arrived = live_subscriber_refusals(delta, env.messenger.directory());
    assert!(
        arrived.is_empty(),
        "reload commit: a subscriber arrived on a channel this reload is taking away, after the \
         departing consumers and surfaces had already stopped: {arrived:?}",
    );
    describe_channels(env, delta).await;
    let mut deferred = mqtt_outgoing(env, delta).await;
    remove_channels(env, delta);
    add_channels(env, delta).await;
    deferred.extend(mqtt_incoming(env, delta).await);
    refresh_surface_roots(env, surfaces.roots.clone());
    swap_surface_registrations(env, surfaces.docs);
    publish_surface_docs(env, surfaces.docs).await;
    start_surfaces(env, plan, delta, surfaces, &planned).await;
    start_consumers(env, registry, plan, delta, loaded, &planned).await;
    refresh_records(registry, records);

    // The same cross-check boot runs over its own wiring, asked of the wiring
    // this reload just produced. A failure is a defect in the steps above, not
    // a verdict on the document — the document was accepted before any of this
    // ran.
    crate::assert_every_subscriber_wired(&env.messenger, &env.router);
    Ok(deferred)
}

/// The outgoing MQTT step, between the descriptions and the channel removals.
///
/// UNSUBSCRIBE before the route goes, so a publish already in flight finds its
/// route; one that arrives after the route is gone is a benign zero-match drop.
///
/// Returns the filters the broker did not take now, for the status body.
async fn mqtt_outgoing(env: &ReloadEnv, delta: &PlanDelta) -> Vec<String> {
    let mut deferred = Vec::new();
    for client in &delta.mqtt.clients {
        for filter in client.leaving() {
            let address = address_of(&client.client, &filter.topic_filter);
            let service = mqtt_service(env, &address);
            let outcome = service
                .unsubscribe_filter(&client.client, &filter.topic_filter)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "reload commit: {address} is being unsubscribed but client {:?} has no \
                         broker session — rule 6 refused exactly this before the walk, so it is \
                         a host bug",
                        client.client,
                    )
                });
            if record_unsubscribe(&outcome, &address) {
                deferred.push(address);
            }
        }
    }
    for route in &delta.mqtt.routes_removed {
        let removed = mqtt_router(env, &route.channel_address).remove_route(route.channel_uuid);
        assert!(
            removed,
            "reload commit: mqtt channel {:?} is in the delta but the router holds no route for \
             it — host bug",
            route.channel_address,
        );
        info!(address = %route.channel_address, "reload: mqtt route removed");
    }
    deferred
}

/// The incoming MQTT step, after the channels are in the directory.
///
/// Route before SUBSCRIBE, so the first matching publish after the SUBACK has
/// somewhere to go.
async fn mqtt_incoming(env: &ReloadEnv, delta: &PlanDelta) -> Vec<String> {
    for route in &delta.mqtt.routes_added {
        let address = route.channel_address.clone();
        // Idempotent on the channel uuid, and the uuid is the plan's, so a
        // `false` here means the table already held a route the plan also
        // wants — which rule 2's added arm refused before the walk.
        let added = mqtt_router(env, &address).add_route(route.clone());
        assert!(
            added,
            "reload commit: the router already holds a route for mqtt channel {address:?}, which \
             this reload is adding — host bug",
        );
        info!(address = %address, "reload: mqtt route added");
    }
    let mut deferred = Vec::new();
    for client in &delta.mqtt.clients {
        for filter in client.joining() {
            let address = address_of(&client.client, &filter.topic_filter);
            let service = mqtt_service(env, &address);
            let outcome = service
                .subscribe_filter(&client.client, filter.topic_filter.clone(), filter.qos)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "reload commit: {address} is being subscribed but client {:?} has no \
                         broker session — rule 6 refused exactly this before the walk, so it is \
                         a host bug",
                        client.client,
                    )
                });
            if record_subscribe(&outcome, &address) {
                deferred.push(address);
            }
        }
    }
    deferred
}

/// Point every running consumer's record at the tree this reload resolved it
/// out of.
///
/// A consumer whose package moved but whose release did not is deliberately not
/// restarted, so the record it was loaded with still names the versioned tree
/// the previous install staged — a directory the installer prunes. Nothing
/// reads the paths after the load, but the registry is what a later reload
/// compares against and what an operator reads to learn where this process's
/// bytes came from, so it is the new one that is kept.
pub(crate) fn refresh_records(
    registry: &mut ConsumerRegistry,
    records: &HashMap<String, Verified>,
) {
    for (slug, record) in records {
        if let Some(running) = registry.get_mut(slug) {
            running.verified = record.clone();
        }
    }
}

/// Point the served surface asset tree at the roots this reload scanned.
///
/// A kind whose mount swapped its symlink onto a fresh versioned tree is
/// byte-for-byte the installation this process is already serving, so nothing
/// about it is refused and nothing about it moves — but the path the cell holds
/// names the tree the installer is about to prune, and `/surface-static` would
/// start answering 404 for every asset under it. Installing the scan's paths is
/// what makes the relocation a relocation.
///
/// Runs on the applied and the unchanged path alike: a byte-identical
/// re-install is exactly the case that produces no delta.
pub(crate) fn refresh_surface_roots(env: &ReloadEnv, roots: brenn_surface_server::SurfaceRoots) {
    *env.surface_roots
        .write()
        .expect("the surface-roots lock is held only for a clone and a swap") = Arc::new(roots);
}

/// Log one UNSUBSCRIBE outcome and say whether the filter left the broker only
/// on the next connect.
///
/// Every outcome is a success: the filter is out of the reconnect-survival set
/// in all three, so the state the process converges to is the planned one.
/// `SendFailed` is a `warn!` and a `deferred` entry, never a refusal and never
/// a panic, because the walk is past the point where anything may decline.
fn record_unsubscribe(outcome: &IngressUnsubscribeOutcome, address: &str) -> bool {
    match outcome {
        IngressUnsubscribeOutcome::UnsubscribedLive => {
            info!(address = %address, "reload: mqtt filter unsubscribed");
            false
        }
        IngressUnsubscribeOutcome::DeferredDisconnected => {
            // The filter left the reconnect-survival set, so the next connect
            // does not re-assert it. Converged, just not now.
            info!(address = %address, "reload: mqtt filter unsubscribed on reconnect");
            true
        }
        IngressUnsubscribeOutcome::SendFailed(error) => {
            warn!(address = %address, %error, "reload: mqtt UNSUBSCRIBE send failed");
            true
        }
    }
}

/// The same for a SUBSCRIBE: the filter is in the reconnect-survival set in all
/// three outcomes, and the two that did not reach the broker now are what the
/// status body's `deferred` list reports.
fn record_subscribe(outcome: &IngressSubscribeOutcome, address: &str) -> bool {
    match outcome {
        IngressSubscribeOutcome::SubscribedLive => {
            info!(address = %address, "reload: mqtt filter subscribed");
            false
        }
        IngressSubscribeOutcome::DeferredDisconnected => {
            info!(address = %address, "reload: mqtt filter subscribed on reconnect");
            true
        }
        IngressSubscribeOutcome::SendFailed(error) => {
            warn!(address = %address, %error, "reload: mqtt SUBSCRIBE send failed");
            true
        }
    }
}

/// The broker service this walk needs, or the host bug of not having one.
fn mqtt_service<'a>(env: &'a ReloadEnv, address: &str) -> &'a Arc<brenn_mqtt::MqttService> {
    env.mqtt_service.as_ref().unwrap_or_else(|| {
        panic!(
            "reload commit: {address} moves a broker subscription but this process has no MQTT \
             service — rule 6 refuses every client without a session, so it is a host bug"
        )
    })
}

/// The ingress router this walk needs, or the host bug of not having one.
fn mqtt_router<'a>(
    env: &'a ReloadEnv,
    address: &str,
) -> &'a Arc<brenn_server::mqtt_router::MqttEventRouterImpl> {
    env.mqtt_event_router.as_ref().unwrap_or_else(|| {
        panic!(
            "reload commit: {address} moves an ingress route but this process has no MQTT event \
             router — it exists on exactly the terms the service does, so it is a host bug"
        )
    })
}

/// Everything the walk's surface steps install: the scanned asset roots, the
/// arriving runtimes, and the documents to republish. All of it built in
/// prepare, so none of it can fail here.
pub(crate) struct SurfaceCommit<'a> {
    /// The asset roots this reload's scan resolved, installed in the cell
    /// `/surface-static` reads whether or not any surface moved.
    pub roots: &'a brenn_surface_server::SurfaceRoots,
    /// One runtime per arriving surface, by slug.
    pub runtimes: &'a HashMap<String, Arc<SurfaceRuntime>>,
    /// The description and bindings documents to republish, and the
    /// surface-description registrations to swap before publishing them.
    pub docs: &'a SurfaceDocs,
    /// `[surface_description] prefix`, which roots the status channel an
    /// arriving surface's `disconnected` stamp is written to.
    pub prefix: &'a str,
}

/// Step 2: take every retired and replaced surface out of service.
///
/// The runtime leaves the table first, so no attach lands on
/// a surface that is about to lose its wiring — a replaced slug is marked
/// reconfiguring and answers `503` until step 6 installs its successor, a
/// retired one answers `404` because that is the truth about it. The live
/// sessions are then asked to close and awaited, so each page leaves through
/// its own detach path and publishes the terminal stamp it owes. Only then do
/// the registration, the binding and the budgets go, for a surface that is
/// leaving for good; a replaced one keeps all three until step 6 overwrites
/// them, so no window exists in which a publish finds a surface half-wired.
///
/// Every surface is marked and signalled before any of them is waited on.
/// Nothing orders the departing surfaces against each other, and the whole
/// reload is stalled for the length of this step, so the window is the longest
/// surface's socket close rather than the sum of them — which is what a kind
/// upgrade, promoting every surface mounting it, would otherwise pay.
async fn retire_surfaces(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let departing = departing_surfaces(delta);

    for (surface, reason) in &departing {
        let slug = surface.slug.as_str();
        match reason {
            SurfaceCloseReason::Retired => env.surfaces.retire(slug),
            SurfaceCloseReason::Reconfigured => env.surfaces.begin_reconfigure(slug),
        }
        let asked = env.attach_registry.close_all(slug, &reason.close());
        info!(slug = %slug, sessions = asked, "reload: surface sessions asked to close");
    }

    for (surface, reason) in &departing {
        let slug = surface.slug.as_str();
        let kind = SubscriberEntryKind::Surface(slug.to_string());
        report_while("surface sessions", slug, sessions_quiet(env, slug, reason)).await;

        if *reason == SurfaceCloseReason::Retired {
            env.router.retire_delivery_binding(&kind);
            env.messenger.retire_subscriber_registration(&kind);
            env.messenger
                .remove_attach_send_budgets(AttachScope::surface(slug));
        }

        // Every entry the old value folded into, whether or not the channel
        // itself moved: a replaced surface's entries are re-derived from the
        // candidate's plan in step 6, and a retired one's are simply gone.
        //
        // By channel, not by binding: `wire_subscriptions` carries one entry
        // per (instance, channel) and the directory folds a surface onto a
        // channel once, so two components of one surface reading one channel
        // are one subscriber to remove.
        let mut unfolded: HashSet<Uuid> = HashSet::new();
        for sub in &surface.wire_subscriptions {
            let uuid = sub.subscription.channel_uuid;
            if !unfolded.insert(uuid) {
                continue;
            }
            assert!(
                live.remove_subscriber(&uuid, &kind).is_some(),
                "reload commit: surface {slug:?} was planned as a subscriber of channel {:?} and \
                 the live directory does not hold it there — host bug",
                sub.subscription.channel_address,
            );
        }
        info!(slug = %slug, "reload: surface retired");
    }
}

/// Resolves once nothing under `slug` is attached or still draining.
///
/// Polled rather than signalled: each session exits through its own task, and
/// the registry going quiet is the only fact this step needs. The wait is
/// unbounded, and [`report_while`] is what names a socket the OS will not close
/// rather than stepping over it and installing a new runtime while an old page
/// still holds the old one.
///
/// The close is re-asked on every pass, not only once before the loop. The
/// surface door resolves the runtime before it registers its session, so a
/// handler that passed that lookup before this step marked the slug can still
/// register after [`AttachRegistry::close_all`] took its snapshot; a session
/// nobody asked to leave would then hold this wait open until its user closed
/// the tab. Asking again is documented as harmless, and it is what makes the
/// wait independent of the door's internal ordering.
async fn sessions_quiet(env: &ReloadEnv, slug: &str, reason: &SurfaceCloseReason) {
    while !env.attach_registry.is_quiet(slug) {
        env.attach_registry.close_all(slug, &reason.close());
        tokio::time::sleep(SESSION_CLOSE_POLL_INTERVAL).await;
    }
}

/// How often the wait above re-reads the registry.
const SESSION_CLOSE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Step 5b: install the surface-description participants' new registrations.
///
/// Swapped under one lock, as an arriving surface's own registration is: both
/// participants are pushed unconditionally by the planner, so the key is always
/// live, and a replace leaves no moment in which it resolves "gone".
///
/// Runs on a removal-only delta too — the specs lose a matcher then, and the
/// oracle's standard is the registrations a fresh boot of the candidate builds.
fn swap_surface_registrations(env: &ReloadEnv, docs: &SurfaceDocs) {
    for (kind, registration) in &docs.registrations {
        env.messenger
            .replace_subscriber_registration(kind, registration.clone());
        info!(participant = ?kind, "reload: surface-description registration swapped");
    }
}

/// Step 5c: republish the documents prepare built.
///
/// Under the two boot identities, which step 5b has just widened to admit every
/// address in the set. Publishing before the arriving runtimes exist is safe
/// and deliberate: the channels are in the directory as of step 3, every one of
/// them is retained, and no page can attach to an arriving surface until step 6
/// inserts its runtime — so the kernel reads a retained document on attach,
/// exactly as at boot.
///
/// # Panics
///
/// On any publish that is not `Ok`. Prepare size-checked every body against the
/// same ceiling the publisher enforces and step 5b installed the writer, so
/// what is left is a host bug in a walk that is past declining.
async fn publish_surface_docs(env: &ReloadEnv, docs: &SurfaceDocs) {
    if let Err((address, outcome)) = brenn_surface_server::description::try_publish_description(
        &env.messenger,
        &docs.description,
    )
    .await
    {
        panic!(
            "reload commit: publishing the surface description document onto {address} returned \
             {outcome:?} — prepare proved the body publishable and step 5b installed the writer, \
             so this is a host bug"
        );
    }
    if let Err((address, outcome)) =
        brenn_surface_server::bindings_doc::try_publish_bindings_documents(
            &env.messenger,
            &docs.bindings,
        )
        .await
    {
        panic!(
            "reload commit: publishing the surface bindings document onto {address} returned \
             {outcome:?} — prepare proved the body publishable and step 5b installed the writer, \
             so this is a host bug"
        );
    }
    for (address, _) in docs.description.iter().chain(docs.bindings.iter()) {
        info!(address = %address, "reload: surface document republished");
    }
}

/// Step 6: put every arriving and replaced surface into service.
///
/// The wiring goes in before the runtime, in boot's order: the registration
/// first, because a delivery gate that finds a subscriber without one treats it
/// as a host bug; then the subscriber entries, taken verbatim from the
/// candidate's plan so the entry a surface joins a channel with is the one a
/// fresh boot would have folded; then the send budgets and the delivery
/// binding; then, for a surface that did not exist a moment ago, the
/// `disconnected` stamp boot writes for every surface it configures. The
/// runtime lands last, which is the write that reopens the door.
async fn start_surfaces(
    env: &ReloadEnv,
    plan: &MessagingPlan,
    delta: &PlanDelta,
    arriving: &SurfaceCommit<'_>,
    planned: &PlannedSubscribers<'_>,
) {
    let live = env.messenger.directory();
    for (surface, arrival) in crate::reload::surfaces::arriving(&delta.surfaces) {
        let slug = surface.slug.as_str();
        let kind = SubscriberEntryKind::Surface(slug.to_string());
        let registration = plan.registrations.get(&kind).cloned().unwrap_or_else(|| {
            panic!(
                "reload commit: the plan carries no subscriber registration for surface \
                 {slug:?} — every resolved surface has one, so this is a host bug"
            )
        });
        // Step 2 keeps the registration, binding, and budgets for a replaced
        // surface, so the registration is swapped in place — never absent — and
        // the binding, whose value is the same constant for every surface, is
        // left standing.
        let added = arrival == Arrival::Added;
        match added {
            true => env
                .messenger
                .register_subscriber_registration(kind.clone(), registration),
            false => env
                .messenger
                .replace_subscriber_registration(&kind, registration),
        }

        planned.fold(live, &kind, "surface");

        env.messenger.set_attach_send_budgets(
            AttachScope::surface(slug),
            brenn_messaging::attach_principal_budgets(
                AttachScope::surface(slug),
                surface.principal_send_budgets().collect(),
            ),
        );
        if added {
            env.router.register_surface_delivery_routes(surface);
        }

        let runtime = arriving.runtimes.get(slug).cloned().unwrap_or_else(|| {
            panic!(
                "reload commit: surface {slug:?} is arriving but prepare built no runtime for it \
                 — host bug"
            )
        });
        if added {
            brenn_surface_server::telemetry::publish_boot_disconnected_stamps(
                &env.messenger,
                arriving.prefix,
                std::slice::from_ref(surface),
                env.messenger.ring_epoch(),
            )
            .await;
        }
        env.surfaces.install(slug.to_string(), runtime);
        info!(slug = %slug, "reload: surface started");
    }
}

/// The surfaces leaving service, each with the reason its pages are told.
///
/// A replacement's old value is what is walked: the entries to unfold and the
/// sessions to close are the running surface's, not its successor's.
fn departing_surfaces(
    delta: &PlanDelta,
) -> Vec<(
    &brenn_lib::messaging::config::ResolvedSurface,
    SurfaceCloseReason,
)> {
    delta
        .surfaces
        .removed
        .iter()
        .map(|surface| (surface, SurfaceCloseReason::Retired))
        .chain(
            delta
                .surfaces
                .changed
                .iter()
                .map(|change| (&change.old, SurfaceCloseReason::Reconfigured)),
        )
        .collect()
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

/// How often a wait that has not finished is named in the journal.
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
    report_while("consumer", slug, handle.stop_and_join()).await;
}

/// Await `waiting`, naming `what` and `slug` in the journal until it resolves.
///
/// The one implementation of the reload's unbounded-but-reported wait, shared
/// by the consumer stop and the surface session close: both are waits on
/// something outside this process's control, both stall the whole reload, and
/// an operator watching the journal during one has to be told the same three
/// things.
///
/// Returns only when `waiting` does. The tick arm is a report and never an
/// exit: for a consumer, returning early would drop the component — and with it
/// the consumer's KV store handle — while the task that shares it is still
/// alive, so a replacement under the same slug could not open the store; for a
/// surface, it would install a new runtime while an old page still holds the
/// old one.
async fn report_while(what: &str, slug: &str, waiting: impl Future<Output = ()>) {
    info!(slug = %slug, "reload: waiting for {what} to finish");
    let since = std::time::Instant::now();
    let mut waiting = std::pin::pin!(waiting);
    let mut ticks = tokio::time::interval(STOP_WAIT_REPORT_INTERVAL);
    // The first tick completes immediately; it is this moment, already logged.
    ticks.tick().await;
    loop {
        tokio::select! {
            () = &mut waiting => return,
            _ = ticks.tick() => warn!(
                slug = %slug,
                waited_secs = since.elapsed().as_secs(),
                "reload: {what} still has not finished"
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
    planned: &PlannedSubscribers<'_>,
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

        planned.fold(live, &kind, "consumer");

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

/// The plan's subscriber entries, grouped by the principal that holds them.
///
/// Built once per commit and read by both arrival steps, so the walk over the
/// candidate's channels happens once rather than once per arriving principal.
/// The key is the directory's own subscriber identity: for every kind, the
/// derived equality this map hashes by and
/// [`SubscriberEntryKind::same_principal`] are the same relation — every field
/// either compares, one compares — which
/// `a_planned_group_is_exactly_what_same_principal_matches` holds.
struct PlannedSubscribers<'a> {
    by_principal: HashMap<&'a SubscriberEntryKind, Vec<(&'a ChannelEntry, &'a SubscriberEntry)>>,
}

impl<'a> PlannedSubscribers<'a> {
    fn of(channels: &'a [Arc<ChannelEntry>]) -> Self {
        let mut by_principal: HashMap<_, Vec<_>> = HashMap::new();
        for entry in channels {
            for subscriber in &entry.subscribers {
                by_principal
                    .entry(&subscriber.kind)
                    .or_default()
                    .push((entry.as_ref(), subscriber));
            }
        }
        Self { by_principal }
    }

    /// Fold one principal's planned entries onto the live directory.
    ///
    /// The one implementation of "put this principal on its channels", shared
    /// by the arriving consumers and the arriving surfaces: the entry each one
    /// joins with is the candidate plan's verbatim, so what the live directory
    /// ends up holding is what a fresh boot would have folded. `what` names the
    /// kind of principal in the panic.
    fn fold(
        &self,
        live: &brenn_lib::messaging::MessagingDirectory,
        kind: &SubscriberEntryKind,
        what: &str,
    ) {
        for (entry, subscriber) in self.by_principal.get(kind).into_iter().flatten() {
            let applied = live.add_subscriber(&entry.uuid, (*subscriber).clone());
            assert!(
                applied,
                "reload commit: {what} {:?} subscribes to channel {:?}, which the live directory \
                 does not hold — host bug",
                kind.slug(),
                entry.address,
            );
        }
    }
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

    /// Every SUBSCRIBE outcome is a success; only two of the three are
    /// "not at the broker yet". `deferred` is what the status body reports, so
    /// a mapping that pushed the live arm onto it would make an operator read
    /// a converged reload as a pending one.
    #[test]
    fn a_live_subscribe_is_not_deferred_and_the_other_two_are() {
        assert!(!record_subscribe(
            &IngressSubscribeOutcome::SubscribedLive,
            "mqtt:ha:a/b"
        ));
        assert!(record_subscribe(
            &IngressSubscribeOutcome::DeferredDisconnected,
            "mqtt:ha:a/b"
        ));
        assert!(record_subscribe(
            &IngressSubscribeOutcome::SendFailed("the request channel is closed".to_string()),
            "mqtt:ha:a/b",
        ));
    }

    /// The same for the outgoing direction. `SendFailed` in particular must be
    /// a `deferred` entry and not an abort: the walk is past the point where
    /// anything may decline, and the filter is out of the reconnect-survival
    /// set either way.
    #[test]
    fn a_live_unsubscribe_is_not_deferred_and_the_other_two_are() {
        assert!(!record_unsubscribe(
            &IngressUnsubscribeOutcome::UnsubscribedLive,
            "mqtt:ha:a/b"
        ));
        assert!(record_unsubscribe(
            &IngressUnsubscribeOutcome::DeferredDisconnected,
            "mqtt:ha:a/b"
        ));
        assert!(record_unsubscribe(
            &IngressUnsubscribeOutcome::SendFailed("the request channel is closed".to_string()),
            "mqtt:ha:a/b",
        ));
    }

    /// The grouping key [`PlannedSubscribers`] hashes by is the identity the
    /// fold uses to compute a principal at a time.
    ///
    /// Derived `Eq` and [`SubscriberEntryKind::same_principal`] must agree on
    /// every pair of kinds. They do because each variant carries nothing but
    /// the fields both compare; this test notices if one ever grows a field
    /// that only the derived equality reads.
    #[test]
    fn a_planned_group_is_exactly_what_same_principal_matches() {
        let kinds = [
            SubscriberEntryKind::App("a".to_string()),
            SubscriberEntryKind::App("b".to_string()),
            SubscriberEntryKind::Wasm("a".to_string()),
            SubscriberEntryKind::System("a".to_string()),
            SubscriberEntryKind::Surface("a".to_string()),
            SubscriberEntryKind::Surface("b".to_string()),
            SubscriberEntryKind::Remote("a".to_string()),
            SubscriberEntryKind::ChatConversation {
                app_slug: "a".to_string(),
                conversation_id: 1,
            },
            SubscriberEntryKind::ChatConversation {
                app_slug: "a".to_string(),
                conversation_id: 2,
            },
        ];
        for left in &kinds {
            for right in &kinds {
                assert_eq!(
                    left.same_principal(right),
                    left == right,
                    "{left:?} and {right:?} are one subscriber by one rule and two by the other",
                );
            }
        }
    }

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
        report_while("consumer", "wedged", waiting).await;
        assert!(
            stopped.load(Ordering::SeqCst),
            "the wait returned before the consumer stopped, which would drop the component out \
             from under a task still holding its store",
        );
    }
}
