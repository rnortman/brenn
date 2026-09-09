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

use brenn_lib::messaging::config::{Depth, DormantSubscription};
use brenn_lib::messaging::{ChannelEntry, ParticipantId, SubscriberEntryKind};
use brenn_lib::mqtt::config::MqttClientConfig;
use brenn_lib::wasm_package::Verified;
use brenn_messaging::{Messenger, WASM_WINDOW_MAX_NEW};
use brenn_messaging_boot::MessagingPlan;
use brenn_server::messaging_router::DeliveryBinding;

use brenn_wasm_dispatch::ConsumerHandle;

use super::webhook::WebhookArrivals;

use brenn_mqtt::{
    ArrivingFilters, IngressSubscribeOutcome, IngressUnsubscribeOutcome, register_and_spawn,
};

use brenn_lib::messaging::identity::AttachScope;
use brenn_server::routes::surface::SurfaceCloseReason;
use brenn_surface_server::SurfaceRuntime;

use crate::consumers::{ConsumerRegistry, LoadedConsumer, RunningConsumer, start_consumer};
use crate::reload::agents::AgentChange;
use crate::reload::delta::{PlanDelta, live_subscriber_refusals};
use crate::reload::driver::ReloadEnv;
use crate::reload::dynamic::{DynamicSnapshot, RevokeReason};
use crate::reload::mqtt::address_of;
use crate::reload::subscribers::PlannedSubscribers;
use crate::reload::surfaces::{Arrival, SurfaceDocs};

/// What the broker did not take during the walk, split by whether anything in
/// this process will take it later.
///
/// Both lists are addresses of moved filters and both are reported in the
/// status body; the split is the whole point, because `deferred` converges by
/// waiting and `failed` converges only after an operator fixes the client
/// declaration and restarts.
#[derive(Debug, Default)]
pub(crate) struct MqttCommitReport {
    pub(crate) deferred: Vec<String>,
    pub(crate) failed: Vec<String>,
}

/// The two halves of the walk prepare could not measure: what the broker did
/// with each moved filter, and which condemned sessions died at the swap.
#[derive(Debug, Default)]
pub(crate) struct CommitReport {
    pub(crate) mqtt: MqttCommitReport,
    /// `"<slug> conv <id>"`, the sessions killed now and the ones left to die
    /// at their turn end.
    pub(crate) sessions_retired: Vec<String>,
    pub(crate) sessions_retire_pending: Vec<String>,
}

/// Apply a prepared reload to the running process.
///
/// Three checks run before the first mutation and can still decline. Two of
/// them ask a question prepare asked whose answer can change between the two —
/// whether a subscriber the plan cannot see has landed on a channel this walk
/// would take away (a dynamic app subscription or an attach-minted surface
/// entry arriving while prepare hashes and compiles the arriving components),
/// and whether a changed agent's dynamic subscriptions still are what the
/// re-merge classified. The third is asked here alone, because it reads the
/// store: whether an arriving channel's address already belongs to a different
/// channel row. Nothing has been touched yet at that point, so each is a
/// refusal like any other.
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
    artifacts: CommitArtifacts<'_>,
) -> Result<CommitReport, Vec<String>> {
    let CommitArtifacts {
        loaded,
        records,
        surfaces,
        webhook,
    } = artifacts;
    let surfaces = &surfaces;
    let arrived = live_subscriber_refusals(delta, env.messenger.directory());
    if !arrived.is_empty() {
        return Err(arrived);
    }
    let moved = dynamic_subscriptions_moved(env, delta).await;
    if !moved.is_empty() {
        return Err(moved);
    }
    let collisions = channel_rows_collide(env, delta).await;
    if !collisions.is_empty() {
        return Err(collisions);
    }

    let channels = plan.directory.list();
    let planned = PlannedSubscribers::of(&channels);

    retire_webhook_endpoints(env, delta);
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
    // The same input hazard, for the same reason: a live session of a changed
    // agent can mint a dynamic row at any moment, authorized under the old
    // policy, and a row minted during either wait is one the re-merge never
    // classified. Past the retirements nothing can be declined.
    let moved = dynamic_subscriptions_moved(env, delta).await;
    assert!(
        moved.is_empty(),
        "reload commit: a changed agent's dynamic subscriptions moved after the departing \
         consumers and surfaces had already stopped: {moved:?}",
    );
    retire_agent_subscriptions(env, delta).await;
    describe_channels(env, delta).await;
    start_added_clients(env, delta).await;
    let mut report = MqttCommitReport {
        deferred: mqtt_outgoing(env, delta).await,
        failed: Vec::new(),
    };
    remove_channels(env, delta);
    add_channels(env, delta).await;
    swap_agents(env, plan, delta);
    let sessions = retire_stale_sessions(env, delta).await;
    start_agent_subscriptions(env, delta, &planned).await;
    restart_changed_and_stop_removed_clients(env, delta).await;
    let incoming = mqtt_incoming(env, delta).await;
    report.deferred.extend(incoming.deferred);
    report.failed = incoming.failed;
    refresh_surface_roots(env, surfaces.roots.clone());
    swap_surface_registrations(env, surfaces.docs);
    publish_surface_docs(env, surfaces.docs).await;
    start_surfaces(env, plan, delta, surfaces, &planned).await;
    release_retiring_replay_stores(webhook).await;
    start_consumers(env, registry, plan, delta, loaded, &planned).await;
    install_webhook_endpoints(env, webhook);
    refresh_records(registry, records);

    // The same cross-check boot runs over its own wiring, asked of the wiring
    // this reload just produced. A failure is a defect in the steps above, not
    // a verdict on the document — the document was accepted before any of this
    // ran.
    crate::assert_every_subscriber_wired(&env.messenger, &env.router);
    Ok(CommitReport {
        mqtt: report,
        sessions_retired: sessions.retired,
        sessions_retire_pending: sessions.pending,
    })
}

/// Whether a changed agent's dynamic subscriptions still are what prepare
/// classified.
///
/// The re-merge is a verdict about a set of rows, and that set is the one input
/// to a reload a live session can change while prepare runs: `MessageSubscribe`
/// mints a durable row or a non-durable registration at any moment, authorized
/// under the *old* policy until the swap. A pair minted after prepare read the
/// set would be left folded on terms neither document describes — an entry and
/// a cursor the candidate's ACL denies, and for `mqtt:` a broker filter and a
/// route the delta did not account for. A row dropped and re-minted at other
/// depths is the same hazard: the commit would fold it back in at the depths
/// prepare saw, so the comparison is on each pair's whole identity and not on
/// its channel.
///
/// Asked of exactly the agents this reload walks, because they are the only
/// ones whose classification this reload acts on. Asked before anything is
/// touched, so it declines like any other refusal, and once more after the
/// unbounded waits, where it can only assert.
async fn dynamic_subscriptions_moved(env: &ReloadEnv, delta: &PlanDelta) -> Vec<String> {
    if delta.agents_changed.is_empty() {
        return Vec::new();
    }
    let rows = {
        let conn = env.messenger.db().lock().await;
        brenn_messaging_store::db::load_dynamic_subscriptions(&conn)
    };
    let now = DynamicSnapshot {
        rows,
        nondurable: env.messenger.nondurable_dynamic_subs(),
    };
    let live = env.messenger.directory();
    let mut refusals = Vec::new();
    for change in &delta.agents_changed {
        let held = now.keys_of(&change.slug);
        let classified = delta.dynamic_observed.keys_of(&change.slug);
        if held == classified {
            continue;
        }
        // Named by address wherever the directory holds the channel, and by
        // uuid where it does not: a dormant row can outlive its channel entry,
        // and an operator reading this needs the pair that moved either way.
        // Deduplicated because a row re-minted on other terms is two keys on
        // one channel, which the operator reads as one moved subscription.
        let mut moved: Vec<String> = held
            .symmetric_difference(&classified)
            .map(|key| {
                let uuid = key.channel_uuid();
                match live.by_uuid(&uuid) {
                    Some(entry) => entry.address.clone(),
                    None => uuid.to_string(),
                }
            })
            .collect();
        moved.sort();
        moved.dedup();
        refusals.push(format!(
            "agent {:?} subscribed or unsubscribed dynamically while this reload was being \
             prepared ({}); reload again",
            change.slug,
            moved.join(", "),
        ));
    }
    refusals
}

/// Whether an arriving durable channel's address already belongs to a different
/// channel row.
///
/// `messaging_channels.address` is unique and a channel's store row is never
/// deleted — removing a `[[channel]]` block takes the directory entry away and
/// leaves the row for the operator to delete deliberately. So a candidate that
/// declares an address under a uuid other than the one the row carries has no
/// place to write: the insert is refused by the unique index, the update by uuid
/// matches nothing, and the directory would hold a channel with no row of its
/// own until the first publish failed the foreign key. Boot answers this with a
/// panic in the store; a reload can still decline, and does, because the whole
/// point of the door is that a document the process cannot run leaves the
/// running one alone.
///
/// A declared durable channel derives its uuid from its address, so reaching
/// this needs a `uuid_pins` entry moving one — which is exactly the shape an
/// operator re-declaring a removed channel writes when they mint a fresh uuid
/// instead of reusing the row's. Only reachable once the block's own removal has
/// been applied: while the address is still in the live directory, the arriving
/// entry is refused at prepare as a channel newly minted over one that exists.
async fn channel_rows_collide(env: &ReloadEnv, delta: &PlanDelta) -> Vec<String> {
    let arriving: Vec<(Uuid, String)> = delta
        .joining()
        .filter(|entry| entry.capabilities().durable)
        .map(|entry| (entry.uuid, entry.address.clone()))
        .collect();
    if arriving.is_empty() {
        return Vec::new();
    }
    let conn = env.messenger.db().lock().await;
    arriving
        .into_iter()
        .filter_map(|(uuid, address)| {
            let held = brenn_messaging_store::db::channel_uuid_by_address(&conn, &address)
                .unwrap_or_else(|e| {
                    panic!("reload commit: reading the channel row for {address:?}: {e}")
                })?;
            (held != uuid).then(|| {
                format!(
                    "channel {address:?} is declared under uuid {uuid}, but its address already \
                     belongs to the channel row under uuid {held}; reuse that uuid or delete the \
                     row",
                )
            })
        })
        .collect()
}

/// Step 3: take every changed agent off the channels the candidate no longer
/// has it reading.
///
/// Runs before the channel steps because the detach resolves an address in the
/// live directory, and a genuine removal — an entry that does not come back on
/// this reload's other side — owes the cursor row a deletion, which is the
/// orphan a fresh boot's reconcile would reap. A retune and a moved channel put
/// the same uuid on both sides, so neither detaches: the position is kept and
/// step 6 re-attaches it where it was.
async fn retire_agent_subscriptions(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let mut pruned_rows: Vec<(Uuid, String)> = Vec::new();
    for change in &delta.agents_changed {
        let kind = SubscriberEntryKind::App(change.slug.clone());
        let returning: HashSet<Uuid> = change.subs_added.iter().map(|(uuid, _)| *uuid).collect();
        for (uuid, address) in &change.subs_removed {
            let removed = live.remove_subscriber(uuid, &kind);
            assert!(
                removed.is_some(),
                "reload commit: agent {:?} is losing its subscription to {address:?}, which the \
                 live directory does not hold — host bug",
                change.slug,
            );
            if !returning.contains(uuid) {
                env.messenger
                    .detach_conversation(address, &change.slug)
                    .await;
            }
            info!(agent = %change.slug, address = %address, "reload: agent subscription removed");
        }
        for revoked in &change.dynamic.revoke {
            let moved = &revoked.moved;
            let removed = live.remove_subscriber(&moved.channel_uuid, &kind);
            assert!(
                removed.is_some(),
                "reload commit: agent {:?} has its dynamic subscription to {:?} revoked, which \
                 the live directory does not hold — host bug",
                change.slug,
                moved.address,
            );
            if moved.row.is_none() {
                let held = env
                    .messenger
                    .remove_nondurable_dynamic_sub(&moved.channel_uuid, &change.slug);
                assert!(
                    held,
                    "reload commit: agent {:?} has its non-durable dynamic subscription to {:?} \
                     revoked, but the messenger holds no registration for it — the in-memory set \
                     and the directory disagree (host bug)",
                    change.slug, moved.address,
                );
            }
            // The durable row and the cursor both stay: dormancy preserves
            // the cursor position so the subscription can resume if the ACL
            // comes back.
            match &revoked.reason {
                RevokeReason::AclDenies => info!(
                    agent = %change.slug,
                    address = %moved.address,
                    "reload: agent dynamic subscription revoked — the agent's policy no longer \
                     authorizes delivery on this channel; durable row retained (not pruned), \
                     subscription dormant until the ACL is re-granted",
                ),
                RevokeReason::OverStanding {
                    field,
                    granted,
                    standing,
                } => info!(
                    agent = %change.slug,
                    address = %moved.address,
                    field,
                    granted = ?granted,
                    standing = ?standing,
                    "reload: agent dynamic subscription revoked — its depth exceeds the \
                     channel's current standing_retain_depth; durable row retained (not pruned), \
                     subscription dormant until the operator raises standing or the agent \
                     re-subscribes with a conforming depth",
                ),
            }
        }
        for pruned in &change.dynamic.prune {
            // The entry itself is not removed here: the static replacement
            // is on `subs_added` and applied when those are started. Only the
            // durable row is deleted, below, in one lock scope for the whole
            // reload.
            if pruned.row.is_some() {
                pruned_rows.push((pruned.channel_uuid, change.slug.clone()));
            } else {
                // Not asserted, unlike the revoke arm: the prune arm classifies
                // a pair the candidate declares statically whether or not it is
                // folded, so a dormant durable row and a registration this
                // process never held both reach here legitimately.
                env.messenger
                    .remove_nondurable_dynamic_sub(&pruned.channel_uuid, &change.slug);
            }
            info!(
                agent = %change.slug,
                address = %pruned.address,
                "reload: agent dynamic subscription replaced by a static one",
            );
        }
        reap_previous_owner_positions(env, change).await;
    }
    // One lock scope and one batch call for every pruned row of every agent:
    // the global db mutex is what publish, delivery and wake need, and this
    // walk is inside the window where nothing can be declined.
    if !pruned_rows.is_empty() {
        let conn = env.messenger.db().lock().await;
        brenn_messaging_store::db::prune_dropped_dynamic_subscriptions(&conn, &pruned_rows);
    }
}

/// The departure an owner change is: every position the old owner's
/// conversation held on the agent's channels.
///
/// Must match what a fresh boot's reconcile would delete for the same owner
/// change: static, dynamic-live and dormant positions alike, but not the
/// conversation itself or its chat family.
///
/// The conversation is resolved in a lock scope of its own: the db mutex is not
/// reentrant and every messenger method called below takes it itself. A user or
/// conversation that does not exist held no positions.
async fn reap_previous_owner_positions(env: &ReloadEnv, change: &AgentChange) {
    if !change.owner_changed {
        return;
    }
    let Some(previous) = &change.previous_owner else {
        // Open to all: no owner resolved, so no position was held under one.
        return;
    };
    let conversation = {
        let conn = env.messenger.db().lock().await;
        brenn_db::auth::user::get_user_by_username(&conn, previous).and_then(|user| {
            brenn_db::conversation::get_singleton_conversation_id(&conn, user.id, &change.slug)
        })
    };
    let Some(conversation) = conversation else {
        return;
    };
    env.messenger
        .reap_conversation_positions(&change.slug, conversation)
        .await;
    info!(
        agent = %change.slug,
        previous_owner = %previous,
        conversation,
        "reload: the former owner's positions on the agent's channels are reaped",
    );
}

/// Step 5: install the candidate's agent map, and with it the tool list every
/// successor process will read.
///
/// From this instant every gate decides on the candidate's authority, every
/// per-call reader serves the candidate's value, and every new spawn — wake or
/// browser — builds from the candidate's per-process fields. The rename is
/// atomic on one filesystem; a failure is a host bug, because the file was
/// written into that directory a moment ago.
fn swap_agents(env: &ReloadEnv, plan: &MessagingPlan, delta: &PlanDelta) {
    let apps = plan.planned_apps().unwrap_or_else(|| {
        panic!(
            "reload commit: the plan carries no agent map, but a reload only ever plans with one \
             — host bug"
        )
    });
    env.apps.store(Arc::clone(apps));
    for change in &delta.agents_changed {
        if !change.virtual_tools_staged {
            continue;
        }
        let app = apps.get(&change.slug).unwrap_or_else(|| {
            panic!(
                "reload commit: agent {:?} is in the delta but not in the map being installed — \
                 host bug",
                change.slug,
            )
        });
        let staged = crate::reload::agents::staged_virtual_tools_path(app);
        let live = app.virtual_tools_path();
        std::fs::rename(&staged, &live).unwrap_or_else(|error| {
            panic!(
                "reload commit: renaming {} onto {} failed: {error} — the staged file was written \
                 into that directory in prepare, so this is a host bug",
                staged.display(),
                live.display(),
            )
        });
    }
}

/// Step 5b: condemn the sessions this reload moved out from under.
///
/// After the swap, so a successor is spawned from the candidate; before the
/// fold-in, so a bridge retired here is not the target of step 6's roster
/// publish. A denied user's bridge is retired whether or not the process view
/// moved: no allowed user can attach to it and no delivery targets its
/// conversation. Denied is asked of the candidate's own list, so restricting an
/// agent that was open to all severs the users it now denies even though the
/// document names none of them.
async fn retire_stale_sessions(env: &ReloadEnv, delta: &PlanDelta) -> SessionRetirements {
    let mut retired = Vec::new();
    let mut pending = Vec::new();
    let mut any_user_restricted = false;
    for change in &delta.agents_changed {
        if !change.respawn && !change.users_restricted {
            continue;
        }
        any_user_restricted |= change.users_restricted;
        if !change.users_removed.is_empty() {
            info!(
                agent = %change.slug,
                users = ?change.users_removed,
                "reload: agent no longer allows these users",
            );
        }
        // The candidate's list, resolved to ids: a bridge whose owner it does
        // not name is one no allowed user can attach to. Asked this way round
        // because an agent that was open to all and is now restricted names no
        // removed user at all.
        let allowed_ids = if change.users_restricted {
            Some(user_ids(env, &change.allowed_users).await)
        } else {
            None
        };
        let outcome = env
            .active_bridges
            .retire_for_reload(&change.slug, change.respawn, allowed_ids.as_deref())
            .await;
        retired.extend(
            outcome
                .retired
                .iter()
                .map(|id| format!("{} conv {id}", change.slug)),
        );
        pending.extend(
            outcome
                .pending
                .iter()
                .map(|id| format!("{} conv {id}", change.slug)),
        );
    }
    if any_user_restricted {
        // A connection is authorized once, at connect. The pulse is what makes
        // every open socket ask the swapped table again, so a user the
        // candidate denies is severed here rather than at their next reconnect
        // — which is what a restart would have done to them.
        // The one expected error is "no subscribers": a host with no open
        // sockets has nobody to re-ask. Every open connection is a receiver.
        let _: Result<usize, tokio::sync::broadcast::error::SendError<()>> =
            env.apps_swapped_tx.send(());
    }
    SessionRetirements { retired, pending }
}

/// The session lists the status body carries: `"<slug> conv <id>"` for what
/// died at the swap and for what is condemned and finishing its turn.
#[derive(Default)]
struct SessionRetirements {
    retired: Vec<String>,
    pending: Vec<String>,
}

/// The user ids of `usernames`, skipping any the users table does not hold.
///
/// A username in `allowed_users` with no row is a config/wiring mismatch that
/// `app_owner` already reports per delivery; a user who has never existed owns
/// no bridge, so their absence from the resolved list denies nothing that
/// exists.
async fn user_ids(env: &ReloadEnv, usernames: &[String]) -> Vec<i64> {
    if usernames.is_empty() {
        return Vec::new();
    }
    let conn = env.messenger.db().lock().await;
    usernames
        .iter()
        .filter_map(|name| {
            brenn_db::auth::user::get_user_by_username(&conn, name).map(|user| user.id)
        })
        .collect()
}

/// Step 6: put every changed agent on the channels the candidate says it reads.
///
/// The entry each subscription joins with is the candidate plan's verbatim, so
/// what the live directory ends up holding is what a fresh boot would have
/// folded. The attach is not an in-memory edit: on an agent's *first*
/// push-enabled subscription it mints the agent's singleton conversation,
/// provisions its chat channel family into the live directory and republishes
/// the agent's roster — the same sequence a runtime `MessageSubscribe` runs, and
/// the same one boot runs for the same entry.
///
/// An agent whose owner moved is re-attached over every push-enabled `App`
/// entry the *live* directory holds for it — its unmoved static entries, the
/// ones just folded, kept dynamic ones and revived ones alike — not only the
/// ones this reload moved: positions are held under the owner's conversation,
/// so a new owner has none until this runs.
async fn start_agent_subscriptions(
    env: &ReloadEnv,
    delta: &PlanDelta,
    planned: &PlannedSubscribers<'_>,
) {
    let live = env.messenger.directory();
    for change in &delta.agents_changed {
        let kind = SubscriberEntryKind::App(change.slug.clone());
        let arriving: HashSet<Uuid> = change.subs_added.iter().map(|(uuid, _)| *uuid).collect();
        planned.fold_onto(live, &kind, "agent", &arriving);
        for (entry, subscriber) in planned.of_principal(&kind) {
            if !subscriber.push_depth.is_push_enabled() {
                continue;
            }
            if !arriving.contains(&entry.uuid) {
                continue;
            }
            env.messenger
                .attach_conversation(&entry.address, &change.slug, subscriber.push_depth)
                .await;
        }
        for (_, address) in &change.subs_added {
            info!(agent = %change.slug, address = %address, "reload: agent subscription started");
        }
        for revived in &change.dynamic.revive {
            // At the row's own depths, which is what boot folds a restored row
            // back in at — the subscription the agent asked for, not the one
            // the document would have given it.
            let row = revived.row.as_ref().unwrap_or_else(|| {
                panic!(
                    "reload commit: agent {:?} has its dynamic subscription to {:?} restored, but \
                     the re-merge carried no durable row for it — a non-durable registration is \
                     registered folded and can only be revoked (host bug)",
                    change.slug, revived.address,
                )
            });
            let applied = live.add_subscriber(
                &revived.channel_uuid,
                brenn_lib::messaging::SubscriberEntry {
                    kind: kind.clone(),
                    push_depth: row.push_depth,
                    retain_depth: row.retain_depth,
                    noise: row.noise,
                    wake_min: Some(row.wake_min),
                },
            );
            assert!(
                applied,
                "reload commit: agent {:?} has its dynamic subscription to {:?} restored, but the \
                 live directory holds no such channel — host bug",
                change.slug, revived.address,
            );
            if row.push_depth.is_push_enabled() {
                env.messenger
                    .attach_conversation(&revived.address, &change.slug, row.push_depth)
                    .await;
            }
            info!(
                agent = %change.slug,
                address = %revived.address,
                "reload: agent dynamic subscription revived",
            );
        }
        if change.owner_changed {
            env.messenger
                .attach_conversation_subscribers_of(&change.slug)
                .await;
            info!(
                agent = %change.slug,
                "reload: the new owner is seated on every push-enabled entry the agent holds",
            );
        }
    }
}

/// Register and spawn a supervisor for every client the candidate adds.
///
/// Before the outgoing step and before the agent swap, so an agent whose new
/// authority names the client finds it registered from the first instant that
/// authority is live, and so the incoming step has a session to subscribe its
/// filters on.
///
/// The handle is built with an **empty** subscription list: the incoming step
/// is about to `subscribe_filter` every one of the client's filters and record
/// each outcome, and a handle pre-loaded with the union would leave those
/// moves unreported. The end state is a fresh boot's — the same filter set on
/// the handle and at the broker.
///
/// Between here and the incoming step the handle is registered and connecting,
/// so a publish through it is a normal not-connected outcome.
async fn start_added_clients(env: &ReloadEnv, delta: &PlanDelta) {
    let router: Arc<dyn brenn_mqtt::MqttEventRouter> = env.mqtt_event_router.clone();
    for client in &delta.mqtt_clients.added {
        register_and_spawn(
            &env.mqtt_service,
            Arc::clone(client),
            ArrivingFilters::Declared(Vec::new()),
            router.clone(),
        )
        .await;
        info!(
            client = %client.identity.slug,
            host = %client.identity.host,
            port = client.identity.port,
            subscriptions = 0,
            "reload: mqtt client supervisor spawned"
        );
    }
}

/// Which half of a restarted client's resolved value moved, for the journal.
///
/// The headline case this facility adds is a rotated `password_file` or
/// `ca_file` under an unmoved document, where the successor's broker
/// coordinates are the predecessor's: without this field the line says a
/// session was torn down and rebuilt and gives no reason, and the `Failed`
/// health a rejected credential produces afterwards has no antecedent in the
/// journal.
fn what_moved(old: &MqttClientConfig, new: &MqttClientConfig) -> &'static str {
    let identity = old.identity != new.identity;
    let credential = old.password != new.password || old.ca_cert_pem != new.ca_cert_pem;
    match (identity, credential) {
        (true, true) => "identity+credential",
        (true, false) => "identity",
        (false, true) => "credential",
        (false, false) => panic!(
            "reload commit: client {:?} is being restarted with nothing moved — the delta and \
             this comparison disagreeing is a host bug",
            old.identity.slug,
        ),
    }
}

/// Restart every client whose resolved value moved, then stop every one the
/// candidate no longer declares.
///
/// After the agent swap, which is the last step at which anything authorized to
/// name a removed client could have been live: its consumers were retired
/// before the channel walk, its ingress channels all left with it — the planner
/// refuses a binding on an undeclared client — and its ACL-holding agents were
/// swapped. Before the incoming step, so the filter moves land on the handle
/// that will assert them.
///
/// A changed client is swapped into the registry **first** and stopped second,
/// so `get_client` never answers `None` for a slug the document still declares.
/// The successor inherits the predecessor's subscription list — the live union
/// of static and dynamic filters as the outgoing step left it, copied after the
/// predecessor's supervisor has joined so a filter a concurrent subscribe added
/// late is not dropped — and the
/// broker sees an orderly DISCONNECT followed by a CONNECT with the same client
/// id and `clean_start(false)`, so the persistent session resumes and the new
/// supervisor re-asserts every filter on its first connect.
///
/// A removed client's session lingers at the broker until its session expiry
/// elapses, exactly as it would after a fresh boot of the new document.
///
/// One task per client, all in flight together: every join here is bounded by
/// the supervisor's own DISCONNECT drain timeout but not fast, and only the
/// stop-then-spawn pair *within* one client is ordered — distinct clients share
/// nothing. Serially, a document retiring or re-credentialling K clients
/// against a broker slow to close would stretch this step, and the whole commit
/// with it, to K drains, with no `mqtt:` route installed and no status
/// published until it finished. Concurrently the step is bounded by one drain
/// whatever K is. A panic in any of them is carried back out of the join, so a
/// commit-step assertion still takes the process down with its own payload.
async fn restart_changed_and_stop_removed_clients(env: &ReloadEnv, delta: &PlanDelta) {
    let router: Arc<dyn brenn_mqtt::MqttEventRouter> = env.mqtt_event_router.clone();
    let mut sessions = tokio::task::JoinSet::new();
    for new in &delta.mqtt_clients.changed {
        let service = env.mqtt_service.clone();
        let router = router.clone();
        let new = Arc::clone(new);
        sessions.spawn(async move {
            let slug = new.identity.slug.clone();
            let old = service.get_client(&slug).unwrap_or_else(|| {
                panic!(
                    "reload commit: client {slug:?} is being restarted but the registry holds \
                     no session for it — {SESSION_INVARIANT}, so it is a host bug",
                )
            });
            let moved = what_moved(&old.config, &new);
            let (host, port) = (new.identity.host.clone(), new.identity.port);
            let successor =
                register_and_spawn(&service, new, ArrivingFilters::Successor(&old), router).await;
            // The inherited set lands on the successor after the predecessor's
            // join, so a filter a concurrent subscribe added late is in this
            // count.
            let count = successor.subscriptions.read().await.len();
            info!(
                client = %slug,
                host = %host,
                port = port,
                subscriptions = count,
                moved = moved,
                "reload: mqtt client supervisor restarted"
            );
        });
    }
    for slug in &delta.mqtt_clients.removed {
        let service = env.mqtt_service.clone();
        let slug = slug.clone();
        sessions.spawn(async move {
            service.remove_client(&slug).stop_and_join().await;
            info!(client = %slug, "reload: mqtt client supervisor stopped");
        });
    }
    while let Some(joined) = sessions.join_next().await {
        if let Err(e) = joined {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            }
            panic!("reload commit: an mqtt client session task ended unexpectedly: {e}");
        }
    }
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
            let service = &env.mqtt_service;
            let outcome = session_or_bug(
                service
                    .unsubscribe_filter(&client.client, &filter.topic_filter)
                    .await,
                &client.client,
                &address,
                "unsubscribed",
            );
            if record_unsubscribe(&outcome, &address) {
                deferred.push(address);
            }
        }
    }
    for route in &delta.mqtt.routes_removed {
        let removed = env.mqtt_event_router.remove_route(route.channel_uuid);
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
async fn mqtt_incoming(env: &ReloadEnv, delta: &PlanDelta) -> MqttCommitReport {
    for route in &delta.mqtt.routes_added {
        let address = route.channel_address.clone();
        // Idempotent on the channel uuid, and the uuid is the plan's, so a
        // `false` here means the table already held a route the plan also
        // wants — which rule 2's added arm refused before the walk.
        let added = env.mqtt_event_router.add_route(route.clone());
        assert!(
            added,
            "reload commit: the router already holds a route for mqtt channel {address:?}, which \
             this reload is adding — host bug",
        );
        info!(address = %address, "reload: mqtt route added");
    }
    let mut report = MqttCommitReport::default();
    for client in &delta.mqtt.clients {
        for filter in client.joining() {
            let address = address_of(&client.client, &filter.topic_filter);
            let service = &env.mqtt_service;
            let outcome = session_or_bug(
                service
                    .subscribe_filter(&client.client, filter.topic_filter.clone(), filter.qos)
                    .await,
                &client.client,
                &address,
                "subscribed",
            );
            match record_subscribe(&outcome, &address) {
                SubscribeReport::AtBroker => {}
                SubscribeReport::Deferred => report.deferred.push(address),
                SubscribeReport::Failed => report.failed.push(address),
            }
        }
    }
    report
}

/// Take every removed endpoint out of the table.
///
/// From this instant a request to one of their mounts is an unrecognized URL.
/// Ordered before the consumers leave so no request can reach the event
/// router's WASM-owner guard for an endpoint whose owner is gone. A removed
/// endpoint's replay guard keeps its component until
/// [`release_retiring_replay_stores`], so an in-flight request that already
/// holds the entry still gets a real replay check.
fn retire_webhook_endpoints(env: &ReloadEnv, delta: &PlanDelta) {
    if delta.webhook.removed.is_empty() {
        return;
    }
    env.webhook.retire(&delta.webhook.removed_slugs());
    for entry in &delta.webhook.removed {
        info!(
            endpoint = %entry.slug(),
            mount = %entry.endpoint.mount,
            "reload: webhook endpoint retired"
        );
    }
}

/// Drop every replay component this reload is taking out of service.
///
/// The store-path namespace is one namespace across the webhook and consumer
/// subsystems, and the planner's uniqueness check is over the candidate alone —
/// so a candidate may hand a retiring endpoint's store path to an arriving
/// consumer, and only this order keeps the two holders from overlapping. Every
/// retiring holder is dropped here, before the first arriving store of either
/// subsystem is opened.
///
/// The lock wait is bounded by one replay `check`, which the replay wall budget
/// bounds. A request that takes the lock afterwards finds an empty slot and
/// answers `503` rather than publishing a message nothing replay-checked.
///
/// # Panics
///
/// On a retiring guard that is already empty. Every guard here is either a
/// removed endpoint's — still holding, since retiring the table entry does not
/// touch the slot — or the old guard of a changed endpoint whose component this
/// reload replaced. An empty one means requests have been answering `503`
/// against an entry nothing replaced, which is a host bug and not a state to
/// walk past.
async fn release_retiring_replay_stores(webhook: &WebhookArrivals) {
    for guard in &webhook.retiring {
        let mut slot = guard.slot.lock().await;
        assert!(
            slot.take().is_some(),
            "reload commit: the retiring replay guard over {} was already empty — host bug",
            guard.store_path.display(),
        );
        info!(
            store_path = %guard.store_path.display(),
            "reload: replay store released"
        );
    }
}

/// Open every arriving replay store, then swap the endpoint table.
///
/// In that order, and after the arriving consumers' stores: `open_store` panics
/// on a path some other component still holds, which past
/// [`release_retiring_replay_stores`] and `retire_consumers` would be a host
/// bug — a component holding a file the planner proved unique.
///
/// The swap is last so that the first request to an added mount finds the
/// channel it publishes to already in the directory and the subscriber that
/// reads it already folded on. For a changed endpoint the old entry serves
/// until the swap.
fn install_webhook_endpoints(env: &ReloadEnv, webhook: &WebhookArrivals) {
    for guard in &webhook.opening {
        let slot = guard
            .slot
            .try_lock()
            .expect("an arriving replay guard is not installed yet, so nothing else holds it");
        let component = slot.as_ref().unwrap_or_else(|| {
            panic!(
                "reload commit: the arriving replay guard over {} carries no component — host bug",
                guard.store_path.display(),
            )
        });
        component.open_store();
        info!(
            store_path = %guard.store_path.display(),
            "reload: replay store opened"
        );
    }
    if webhook.runtimes.is_empty() {
        return;
    }
    for runtime in &webhook.runtimes {
        info!(
            endpoint = %runtime.slug(),
            mount = %runtime.endpoint.mount,
            "reload: webhook endpoint installed"
        );
    }
    env.webhook.install(webhook.runtimes.clone());
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

/// Log one UNSUBSCRIBE outcome and say whether the filter is still at the
/// broker after it.
///
/// Every outcome is a success *in this process*: the filter is out of the
/// reconnect-survival set in all three, so brenn's own ingress set is the
/// planned one. On the two deferred outcomes the packet never went out, and
/// the session is persistent, so the broker keeps the filter and keeps
/// publishing on it until the session expires — see
/// `TODO(mqtt-deferred-unsubscribe-not-withdrawn)`. `SendFailed` is a `warn!`
/// and a `deferred` entry, never a refusal and never a panic, because the walk
/// is past the point where anything may decline.
fn record_unsubscribe(outcome: &IngressUnsubscribeOutcome, address: &str) -> bool {
    match outcome {
        IngressUnsubscribeOutcome::UnsubscribedLive => {
            info!(address = %address, "reload: mqtt filter unsubscribed");
            false
        }
        IngressUnsubscribeOutcome::DeferredDisconnected => {
            // No packet went out. The filter left the reconnect-survival set,
            // so nothing re-asserts it, but the broker's copy of the persistent
            // session still holds it.
            info!(address = %address, "reload: mqtt filter not withdrawn at the broker");
            true
        }
        IngressUnsubscribeOutcome::SendFailed(error) => {
            warn!(address = %address, %error, "reload: mqtt UNSUBSCRIBE send failed");
            true
        }
    }
}

/// Where one SUBSCRIBE's filter belongs in the status body.
#[derive(Debug, PartialEq, Eq)]
enum SubscribeReport {
    /// The broker has it now.
    AtBroker,
    /// The broker does not have it and a reconnect in this process will assert
    /// it: `mqtt_deferred`.
    Deferred,
    /// The broker does not have it and nothing in this process will assert it:
    /// `mqtt_failed`.
    Failed,
}

/// The same for a SUBSCRIBE: the filter is in the reconnect-survival set in all
/// four outcomes, and the three that did not reach the broker now are what the
/// status body reports — as `mqtt_deferred` where a reconnect is coming, as
/// `mqtt_failed` where the supervisor has stopped retrying.
///
/// Never a refusal and never a panic, for the reason `record_unsubscribe` is
/// not either: the walk is past the point where anything may decline.
fn record_subscribe(outcome: &IngressSubscribeOutcome, address: &str) -> SubscribeReport {
    match outcome {
        IngressSubscribeOutcome::SubscribedLive => {
            info!(address = %address, "reload: mqtt filter subscribed");
            SubscribeReport::AtBroker
        }
        IngressSubscribeOutcome::DeferredDisconnected => {
            info!(address = %address, "reload: mqtt filter subscribed on reconnect");
            SubscribeReport::Deferred
        }
        IngressSubscribeOutcome::ClientFailed(reason) => {
            // The supervisor has stopped retrying, so there is no reconnect to
            // defer to: this filter will not reach the broker until the
            // client's block is edited, which restarts its supervisor. The
            // filter is registered, so the reload applied — but an operator
            // reading `mqtt_deferred` would wait for a convergence that is not
            // coming, which is why this one is reported in a list of its own.
            warn!(
                address = %address,
                %reason,
                "reload: mqtt filter registered but its client's session has failed \
                 authoritatively; nothing will be subscribed until the client is fixed and \
                 reloaded"
            );
            SubscribeReport::Failed
        }
        IngressSubscribeOutcome::SendFailed(error) => {
            warn!(address = %address, %error, "reload: mqtt SUBSCRIBE send failed");
            SubscribeReport::Deferred
        }
    }
}

/// Why a missing broker session is a host bug in this walk and not a state the
/// document could have asked for.
const SESSION_INVARIANT: &str = "every `mqtt:` address a plan can carry names a declared client, \
                                 and a declared client has a session";

/// One broker-subscription move's outcome, or the host bug of the named client
/// having no session. `verb` is the past participle for the direction —
/// `"subscribed"` or `"unsubscribed"`.
fn session_or_bug<T>(outcome: Option<T>, client: &str, address: &str, verb: &str) -> T {
    outcome.unwrap_or_else(|| {
        panic!(
            "reload commit: {address} is being {verb} but client {client:?} has no broker \
             session — {SESSION_INVARIANT}, so it is a host bug"
        )
    })
}

/// Everything prepare built for the walk to install: the consumers it loaded,
/// what each candidate consumer's package binds to, the surface half, and the
/// webhook half. All of it built before anything could be refused, so none of
/// it can fail here — which is why it travels as one value rather than as four
/// more parameters.
pub(crate) struct CommitArtifacts<'a> {
    /// One loaded component per consumer the delta adds or changes, by slug.
    pub loaded: Vec<(String, LoadedConsumer)>,
    /// What every candidate consumer resolved to, by slug; the registry adopts
    /// these at the end of the walk.
    pub records: &'a HashMap<String, Verified>,
    pub surfaces: SurfaceCommit<'a>,
    pub webhook: &'a WebhookArrivals,
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
///
/// A durable dynamic subscription row on a leaving entry is left where it is,
/// and journalled here rather than in the status body: nothing about it moved.
/// The line is the boot merge's own, at the boot merge's level, because the
/// state is identical — anything watching the journal for a dormant
/// subscription has to see a reload-produced one too.
/// Every row that reaches this walk is one both dynamic refusals deliberately
/// passed over — a folded row's channel is rule 2's refusal and a dormant row
/// on a reconstructible address is the other's, and either would have stopped
/// the reload at prepare — so what is left is the dormant row on a removed
/// operator-declared channel, which a fresh boot of the candidate holds dormant
/// with its cursor. Logged at commit and not at prepare because prepare may
/// still refuse for another reason, and a "left dormant" line for a reload that
/// changed nothing would be false.
fn remove_channels(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let mut forgotten: Vec<Uuid> = Vec::new();
    for entry in delta.leaving() {
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
        for row in delta
            .dynamic_observed
            .rows
            .iter()
            .filter(|row| row.channel_uuid == entry.uuid)
        {
            DormantSubscription {
                channel_uuid: row.channel_uuid,
                app_slug: row.app_slug.clone(),
                channel_address: entry.address.clone(),
            }
            .warn();
        }
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
///
/// A dormant durable dynamic subscription row on an *arriving* uuid is the
/// mirror of the removal walk above, and the one case that reaches it is a
/// channel this reload re-declares under a row an earlier reload left dormant.
/// A declared durable channel's uuid is derived from its address unless the
/// document pins one, so the re-declared block lands on the same uuid the row
/// names; a pin that moves it is refused before any of this runs, because the
/// address already belongs to another channel row. A fresh boot of this
/// document re-classifies such a row — folding it, holding it dormant, or
/// deleting it where the candidate declares a static subscription — and this
/// reload re-classifies only the last of those, so the rest wait for a restart.
/// Journalled at the one moment the operator is looking, because they just
/// re-declared the block and expect delivery to resume.
async fn add_channels(env: &ReloadEnv, delta: &PlanDelta) {
    let live = env.messenger.directory();
    let arriving: Vec<ChannelEntry> = delta
        .joining()
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
        for row in delta
            .dynamic_observed
            .rows
            .iter()
            .filter(|row| row.channel_uuid == entry.uuid)
            .filter(|row| !pruned_here(delta, row.channel_uuid, &row.app_slug))
        {
            warn!(
                channel_uuid = %row.channel_uuid,
                channel = %entry.address,
                app = %row.app_slug,
                "reload: dynamic subscription still dormant — the channel is declared again but \
                 the reload does not re-classify the row against it; a restart does, and folds \
                 it back in if the agent's policy still authorizes delivery here",
            );
        }
        live.add_channel(entry);
    }
}

/// Whether this reload deleted the dynamic row for `(channel, agent)` because
/// the candidate declares a static subscription in its place.
///
/// The row is still in `dynamic_observed`, which is the set prepare classified
/// against, so the "still dormant" line has to ask: a pruned row is gone and
/// the static entry the arrival step folds in is what serves the channel.
fn pruned_here(delta: &PlanDelta, channel_uuid: Uuid, app_slug: &str) -> bool {
    delta
        .agents_changed
        .iter()
        .filter(|change| change.slug == app_slug)
        .flat_map(|change| &change.dynamic.prune)
        .any(|pruned| pruned.channel_uuid == channel_uuid)
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

    /// Every SUBSCRIBE outcome is a success, and the status body sorts them
    /// three ways: at the broker, coming on a reconnect, and coming only after
    /// an operator fixes the client. A mapping that put the live arm on a list
    /// would make an operator read a converged reload as a pending one; one
    /// that put the failed arm on `mqtt_deferred` would have them wait forever.
    #[test]
    fn each_subscribe_outcome_lands_in_its_own_status_list() {
        assert_eq!(
            record_subscribe(&IngressSubscribeOutcome::SubscribedLive, "mqtt:ha:a/b"),
            SubscribeReport::AtBroker
        );
        assert_eq!(
            record_subscribe(
                &IngressSubscribeOutcome::DeferredDisconnected,
                "mqtt:ha:a/b"
            ),
            SubscribeReport::Deferred
        );
        assert_eq!(
            record_subscribe(
                &IngressSubscribeOutcome::ClientFailed("bad user name or password".to_string()),
                "mqtt:ha:a/b",
            ),
            SubscribeReport::Failed
        );
        // A send failure on a dying event loop is the reconnect's to retry.
        assert_eq!(
            record_subscribe(
                &IngressSubscribeOutcome::SendFailed("the request channel is closed".to_string()),
                "mqtt:ha:a/b",
            ),
            SubscribeReport::Deferred
        );
    }

    /// The restart reason the journal carries, one assertion per arm.
    ///
    /// The field is the only operator-facing account of *why* a session was
    /// torn down and rebuilt mid-flight, and the credential arm is the headline
    /// case: nothing in the document moved, so a line without it is a restart
    /// with no antecedent and the `Failed` health a rejected credential
    /// produces afterwards has none either. Swap two arms and every reload case
    /// stays green while the explanation is inverted, which is what this holds.
    #[test]
    fn every_restart_reason_names_the_half_that_moved() {
        let base = brenn_lib::mqtt::test_support::test_client_config("ha");

        let mut elsewhere = base.clone();
        elsewhere.identity.port += 1;
        assert_eq!(what_moved(&base, &elsewhere), "identity");

        let mut rotated = base.clone();
        rotated.password = Some("second".to_string());
        assert_eq!(what_moved(&base, &rotated), "credential");

        let mut both = elsewhere.clone();
        both.ca_cert_pem = Some(b"-----BEGIN CERTIFICATE-----".to_vec());
        assert_eq!(what_moved(&base, &both), "identity+credential");
    }

    /// A restart with nothing moved means the delta and this comparison
    /// disagree, which no document can produce.
    #[test]
    #[should_panic(expected = "nothing moved")]
    fn a_restart_with_nothing_moved_is_a_host_bug() {
        let config = brenn_lib::mqtt::test_support::test_client_config("ha");
        let _ = what_moved(&config, &config.clone());
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
