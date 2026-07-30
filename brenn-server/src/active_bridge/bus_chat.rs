//! The conversation as a bus peer: commands in on `…in.<id>`, its record out on
//! `…out.<id>`, its tokens on `…stream.<id>`.
//!
//! A per-conversation task, spawned with the bridge and living as long as it,
//! with two legs sharing one loop so the model list and the turn ids they both
//! read need no lock:
//!
//! - **Outbound** consumes the same broadcast the attached browsers consume and
//!   republishes the chat-relevant part of it as [`ChatEvent`]/
//!   [`ChatStreamEvent`]. The bus gets the raw text the HTML was rendered from;
//!   its consumers render their own presentation. The translation matches
//!   exhaustively with no wildcard arm, so a new `WsServerMessage` variant is a
//!   compile error here rather than an event that silently never reaches the
//!   record.
//! - **Inbound** drains the conversation's cursor on the command channel and
//!   drives the bridge with what it finds. It runs no access check of its own:
//!   the bus already decided, at publish time, that this peer may command this
//!   conversation, and rebuilding that decision here is exactly the pattern the
//!   channel tree exists to retire.
//!
//! Nothing here is privileged. Every publish rides the same gate ladder as any
//! other principal, authorized by the app's derived harness policy — which
//! reaches that app's chat subtree and nothing else, and which the app's own LLM
//! does not act under. A denial means the policy or the provisioning is wrong,
//! and this task takes the cyanide capsule rather than carry on with a record it
//! cannot write.
//!
//! Peer input is the one thing that never panics: a malformed body is rejected,
//! recorded, and stepped over. The cursor advances after the command has been
//! acted on, so a crash in that window re-executes it on restart — the same
//! at-least-once posture the ambience drain has.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use brenn_cc::session::ModelOption;
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_envelope::chat::{
    self, ChatCommand, ChatEvent, ChatStreamEvent, ModelInfo, TokenKind, legacy_ws_sender,
};
use brenn_lib::config::{ChatLeaf, chat_address};
use brenn_lib::messaging::{
    Depth, MessageEnvelope, Messenger, ParticipantId, PublishResult, Urgency,
};
use brenn_lib::obs::security::{SecurityEventType, log_component_security_event};
use brenn_lib::ws_types::WsServerMessage;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use super::ActiveBridge;

/// Where one translated message goes.
#[derive(Debug, PartialEq)]
enum Outbound {
    /// The durable record.
    Record(ChatEvent),
    /// The ephemeral token stream.
    Stream(ChatStreamEvent),
}

impl Outbound {
    fn leaf(&self) -> ChatLeaf {
        match self {
            Self::Record(_) => ChatLeaf::Out,
            Self::Stream(_) => ChatLeaf::Stream,
        }
    }

    fn body(&self) -> String {
        match self {
            Self::Record(event) => chat::encode(event),
            Self::Stream(event) => chat::encode(event),
        }
    }
}

/// The bus, and the two names this conversation publishes under.
///
/// Both names are fixed for the adapter's lifetime, so they are minted once at
/// start rather than re-derived per publish: the stream leaf takes one publish
/// per token batch, for as long as the conversation lives.
struct ChatBus {
    messenger: Arc<Messenger>,
    out: String,
    stream: String,
    /// Record events the channel's send rate has refused since the last gap
    /// marker reached the record. The record is the authoritative conversation,
    /// so a consumer must never read a truncated one as complete; the count is
    /// carried until the limiter admits a publish again and then spent on one
    /// marker.
    record_drops: AtomicU64,
}

impl ChatBus {
    fn new(messenger: Arc<Messenger>, bridge: &ActiveBridge) -> Self {
        let prefix = &messenger.llm_chat().prefix;
        let out = chat_address(
            prefix,
            &bridge.app_slug,
            ChatLeaf::Out,
            bridge.conversation_id,
        );
        let stream = chat_address(
            prefix,
            &bridge.app_slug,
            ChatLeaf::Stream,
            bridge.conversation_id,
        );
        Self {
            messenger,
            out,
            stream,
            record_drops: AtomicU64::new(0),
        }
    }

    fn address(&self, outbound: &Outbound) -> &str {
        match outbound {
            Outbound::Record(_) => &self.out,
            Outbound::Stream(_) => &self.stream,
        }
    }
}

/// The protocol's model list, from what the harness reported at spawn.
fn models_from_options(options: Vec<ModelOption>) -> Vec<ModelInfo> {
    options
        .into_iter()
        .map(|m| ModelInfo {
            value: m.value,
            display_name: m.display_name,
            description: m.description,
        })
        .collect()
}

/// The protocol's model list, from a legacy broadcast.
fn models_from_broadcast(models: &[brenn_lib::ws_types::ModelInfo]) -> Vec<ModelInfo> {
    models
        .iter()
        .map(|m| ModelInfo {
            value: m.value.clone(),
            display_name: m.display_name.clone(),
            description: m.description.clone(),
        })
        .collect()
}

/// The id shared by a completed assistant message and the token batches that
/// built it.
///
/// Minted on the first token of a turn and retired when the completed message
/// carries it away, so a consumer can tell one turn's partials from the next's
/// and discard the partials it has once the authoritative message lands. A turn
/// that produces a message with no tokens (a cached or empty response) still
/// gets an id — the field is not optional, and an id no batch shares is
/// harmless.
#[derive(Debug, Default)]
struct TurnIds {
    current: Option<String>,
}

impl TurnIds {
    /// The turn in progress, minting one if this is its first token.
    fn current(&mut self) -> String {
        self.current
            .get_or_insert_with(|| uuid::Uuid::new_v4().to_string())
            .clone()
    }

    /// The turn in progress, retiring it: the next token starts a new one.
    fn finish(&mut self) -> String {
        let id = self.current();
        self.current = None;
        id
    }
}

/// Start the conversation's outbound bus leg, if this deployment has a bus.
///
/// `rx` must be a receiver created before the CC event loop starts, so the task
/// sees the conversation's first event. `models` is what the harness reported at
/// spawn; it is published once at start so a subscriber arriving later still
/// finds the model list in the record's retained window.
pub(super) fn spawn_bus_chat_adapter(
    bridge: Arc<ActiveBridge>,
    rx: broadcast::Receiver<WsServerMessage>,
    models: Vec<ModelOption>,
) {
    if bridge.messenger.is_none() {
        return;
    }
    // The handle is retained, not dropped: this task panics rather than write a
    // truncated record, and an unwatched detached task's panic would leave the
    // bridge serving browsers while the bus record stopped. The watchdog reads
    // it as a wedge.
    let handle = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, models));
    bridge.install_chat_adapter_handle(handle);
}

/// Drain the bridge's broadcast into the conversation's chat channels until the
/// bridge is gone.
async fn run_bus_chat_adapter(
    bridge: Arc<ActiveBridge>,
    mut rx: broadcast::Receiver<WsServerMessage>,
    models: Vec<ModelOption>,
) {
    let Some(messenger) = bridge.messenger.clone() else {
        return;
    };
    info!(
        conversation_id = bridge.conversation_id,
        app_slug = %bridge.app_slug,
        "chat bus adapter attached"
    );

    let bus = ChatBus::new(messenger.clone(), &bridge);
    let mut models = models_from_options(models);
    publish(
        &bridge,
        &bus,
        &Outbound::Record(ChatEvent::Models {
            available: models.clone(),
        }),
    )
    .await;

    // The cursor must exist before the drain reads it; draining a window's worth
    // of commands is this leaf's contract.
    let commands = peer_leaf(&bridge, &bus, ChatLeaf::In);
    messenger
        .attach_subscriber(
            &commands.address,
            &bridge.app_slug,
            &commands.subscriber,
            commands.push_depth,
        )
        .await;
    // The pre-warm leaf takes no attach here: its cursor lives in the ring,
    // which provisioning registers and attaches in one step, and a ring cursor
    // created here instead would be created after the publish that woke us.
    let prewarm = peer_leaf(&bridge, &bus, ChatLeaf::Wake);
    // Whatever landed while the conversation was dormant is owed now — including
    // the pre-warm that spawned this bridge, whose position has to move or the
    // next sweep spawns the conversation again.
    drain_commands(&bridge, &bus, &commands, &models).await;
    drain_prewarm(&bridge, &bus, &prewarm).await;

    let notify = Arc::clone(&bridge.chat_commands);
    let shutdown = Arc::clone(&bridge.chat_shutdown);
    let mut turns = TurnIds::default();
    loop {
        let received = tokio::select! {
            received = rx.recv() => received,
            // One notify for both leaves: the wake pass rings this bell for
            // whichever of them is owed, and both drains are cheap when nothing
            // is.
            () = notify.notified() => {
                drain_commands(&bridge, &bus, &commands, &models).await;
                drain_prewarm(&bridge, &bus, &prewarm).await;
                continue;
            }
            // The bridge is being torn down. Nothing else ends this task: the
            // bridge owns the only sender on the broadcast, and this task owns an
            // `Arc` on the bridge.
            () = shutdown.notified() => {
                flush_broadcast(&bridge, &bus, &mut rx, &mut turns, &mut models).await;
                info!(
                    conversation_id = bridge.conversation_id,
                    "chat bus adapter stopped (bridge torn down)"
                );
                return;
            }
        };
        match received {
            Ok(msg) => forward(&bridge, &bus, msg, &mut turns, &mut models).await,
            Err(broadcast::error::RecvError::Lagged(dropped)) => {
                note_broadcast_gap(&bridge, &bus, dropped).await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                info!(
                    conversation_id = bridge.conversation_id,
                    "chat bus adapter detached (bridge gone)"
                );
                return;
            }
        }
    }
}

/// Put one broadcast message on the conversation's channels.
async fn forward(
    bridge: &ActiveBridge,
    bus: &ChatBus,
    msg: WsServerMessage,
    turns: &mut TurnIds,
    models: &mut Vec<ModelInfo>,
) {
    if let WsServerMessage::ModelsAvailable { available_models } = &msg {
        let reported = models_from_broadcast(available_models);
        if reported == *models {
            // The record already says this. Repeating it would spend a slot of
            // the retained window — which is the whole history a subscriber can
            // read back — on nothing.
            return;
        }
        *models = reported;
    }
    for outbound in translate(msg, turns) {
        publish(bridge, bus, &outbound).await;
    }
}

/// Say in the record that the record has a hole, rather than let a consumer
/// believe it read a complete conversation.
async fn note_broadcast_gap(bridge: &ActiveBridge, bus: &ChatBus, dropped: u64) {
    warn!(
        conversation_id = bridge.conversation_id,
        dropped, "chat bus adapter fell behind the bridge broadcast"
    );
    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::Error {
            message: format!(
                "{dropped} conversation event(s) were dropped before reaching this channel; the \
                 record has a gap here"
            ),
            correlation: None,
        }),
    )
    .await;
}

/// Take everything the broadcast is already holding, then let go.
///
/// A teardown does not make the events that preceded it any less part of the
/// record, and the signal to stop can be selected over a `recv` that was ready
/// all along. What is broadcast after this returns is lost — the honest bound
/// when the bridge producing it is gone.
async fn flush_broadcast(
    bridge: &ActiveBridge,
    bus: &ChatBus,
    rx: &mut broadcast::Receiver<WsServerMessage>,
    turns: &mut TurnIds,
    models: &mut Vec<ModelInfo>,
) {
    loop {
        match rx.try_recv() {
            Ok(msg) => forward(bridge, bus, msg, turns, models).await,
            Err(broadcast::error::TryRecvError::Lagged(dropped)) => {
                note_broadcast_gap(bridge, bus, dropped).await;
            }
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return;
            }
        }
    }
}

/// Publish one translated message on its leaf.
///
/// # Panics
///
/// Panics when the publish is refused for a structural reason — an unprovisioned
/// channel, a policy that does not authorize the conversation's own tree, a name
/// that does not pass shape validation. Each means a boot-time invariant is
/// broken, and a conversation that cannot write its own record must stop rather
/// than run on with a silently truncated one.
async fn publish(bridge: &ActiveBridge, bus: &ChatBus, outbound: &Outbound) {
    let leaf = outbound.leaf();
    let address = bus.address(outbound);
    let body = outbound.body();
    let result = bus
        .messenger
        .publish_from_conversation(
            bridge.conversation_id,
            &bridge.app_slug,
            address,
            &body,
            Urgency::Normal,
        )
        .await;

    match result {
        PublishResult::Ok { .. } => {
            if leaf == ChatLeaf::Out {
                note_record_gap(bridge, bus).await;
            }
        }
        // The channel's own send rate is what bounds a conversation's output.
        // A refused partial on the stream is what that channel's loss contract
        // already allows; a refused event on the record is a hole, and the
        // record says so as soon as the limiter lets it.
        PublishResult::RateLimited => {
            warn!(
                conversation_id = bridge.conversation_id,
                %address, "chat publish refused by the channel send rate"
            );
            if leaf == ChatLeaf::Out {
                bus.record_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        PublishResult::BodyTooLarge { len, max } => {
            note_oversize(bridge, bus, leaf, len, max).await;
        }
        other => panic!(
            "chat bus adapter: publishing conversation {} to {address} was refused ({other:?}); \
             the conversation's channels or its app's policy are not what boot established",
            bridge.conversation_id,
        ),
    }
}

/// Put a marker on the record for whatever the send rate refused, once the
/// limiter is admitting publishes again.
///
/// Called after a record publish lands, which is the first moment a marker can
/// land too. A marker the limiter refuses in turn puts its own count back, so
/// the gap is reported by a later publish rather than lost with the events it
/// describes.
async fn note_record_gap(bridge: &ActiveBridge, bus: &ChatBus) {
    let dropped = bus.record_drops.swap(0, Ordering::Relaxed);
    if dropped == 0 {
        return;
    }
    let notice = Outbound::Record(ChatEvent::Error {
        message: format!(
            "{dropped} conversation event(s) were refused by this channel's send rate; the record \
             has a gap here"
        ),
        correlation: None,
    });
    let result = bus
        .messenger
        .publish_from_conversation(
            bridge.conversation_id,
            &bridge.app_slug,
            &bus.out,
            &notice.body(),
            Urgency::Normal,
        )
        .await;
    if !matches!(result, PublishResult::Ok { .. }) {
        error!(
            conversation_id = bridge.conversation_id,
            ?result,
            dropped,
            "could not record the send-rate gap either"
        );
        bus.record_drops.fetch_add(dropped, Ordering::Relaxed);
    }
}

/// Record that a message was too large for its channel.
///
/// The record carries a marker so a consumer sees a gap rather than silence; the
/// stream carries nothing, because a token batch that size is already a
/// corrupted partial and the completed message supersedes it.
async fn note_oversize(
    bridge: &ActiveBridge,
    bus: &ChatBus,
    leaf: ChatLeaf,
    len: usize,
    max: usize,
) {
    error!(
        conversation_id = bridge.conversation_id,
        ?leaf,
        len,
        max,
        "chat message exceeds the channel body limit"
    );
    if leaf != ChatLeaf::Out {
        return;
    }
    let notice = Outbound::Record(ChatEvent::Error {
        message: format!(
            "a conversation event of {len} bytes exceeds this channel's {max}-byte limit and was \
             not published"
        ),
        correlation: None,
    });
    let result = bus
        .messenger
        .publish_from_conversation(
            bridge.conversation_id,
            &bridge.app_slug,
            &bus.out,
            &notice.body(),
            Urgency::Normal,
        )
        .await;
    if !matches!(result, PublishResult::Ok { .. }) {
        error!(
            conversation_id = bridge.conversation_id,
            ?result,
            "could not record the oversize event either"
        );
    }
}

/// Where this conversation reads one of the leaves peers publish to it, and
/// under what identity.
struct PeerLeaf {
    address: String,
    subscriber: ParticipantId,
    push_depth: Depth,
    noise: brenn_lib::messaging::NoiseLevel,
}

/// Resolve one of the conversation's peer-facing leaves out of the directory.
///
/// The depth and the noise rung are the ones provisioning put on the
/// conversation's own subscription, not the channel's inheritance defaults: the
/// subscription is what the wake pass reads, so a drain reading anything else
/// would serve a different window than the one the wake was priced for.
///
/// # Panics
///
/// Panics when the leaf, or the conversation's subscription on it, is not there.
/// Provisioning gives every conversation its chat family at creation, and boot
/// backfills the ones that predate it, so a missing one means a bridge is running
/// for a conversation the messaging subsystem does not know about.
fn peer_leaf(bridge: &ActiveBridge, bus: &ChatBus, leaf: ChatLeaf) -> PeerLeaf {
    let address = chat_address(
        &bus.messenger.llm_chat().prefix,
        &bridge.app_slug,
        leaf,
        bridge.conversation_id,
    );
    let entry = bus
        .messenger
        .directory()
        .resolve(&address)
        .unwrap_or_else(|| {
            panic!(
                "chat bus adapter: conversation {} has no {leaf:?} channel at {address} — it was \
                 never provisioned",
                bridge.conversation_id,
            )
        });
    let subscriber = ParticipantId::for_conversation(bridge.conversation_id);
    let subscription = entry
        .subscribers
        .iter()
        .find(|s| {
            matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::ChatConversation { conversation_id, .. }
                    if *conversation_id == bridge.conversation_id
            )
        })
        .unwrap_or_else(|| {
            panic!(
                "chat bus adapter: conversation {} does not subscribe to its own {address} — \
                 provisioning did not register it",
                bridge.conversation_id,
            )
        });
    PeerLeaf {
        subscriber,
        push_depth: subscription.push_depth,
        noise: subscription.noise,
        address,
    }
}

/// Act on every command the conversation has not seen, then step the cursor
/// past them.
///
/// One pass serves the whole unseen suffix, so a notify that arrives per command
/// still executes each once: the second pass finds the cursor already past it.
/// The advance runs after the acting, which is what makes a crash mid-command a
/// re-execution rather than a silent loss.
async fn drain_commands(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    commands: &PeerLeaf,
    models: &[ModelInfo],
) {
    // Retain depth zero: the drain wants the unseen suffix and nothing else —
    // already-executed commands are not context, they are done.
    let window = bus
        .messenger
        .store_for_address(&commands.address)
        .window(&commands.subscriber, commands.push_depth, Depth::Bounded(0))
        .await;
    let Some(window) = window else {
        // The cursor is gone: an ACL that stopped covering the channel, or a
        // teardown racing this drain. Either way there is nothing owed.
        return;
    };
    if window.new_entries().is_empty() {
        return;
    }

    // A peer is driving this conversation, so the bus door takes hold of the
    // bridge for its idle window — stamped before the commands run, so a turn
    // that ends between here and the last of them cannot drain the bridge out
    // from under the rest.
    bridge.note_bus_activity().await;

    for (_, envelope) in window.new_entries() {
        execute(bridge, bus, envelope, models).await;
    }

    if let Some((through, seen_floor)) = window.advance_span() {
        bus.messenger
            .advance_subscriber(
                &commands.address,
                &commands.subscriber,
                through,
                seen_floor,
                commands.noise,
            )
            .await;
    }
}

/// Step the conversation's position past every pre-warm it has been sent.
///
/// A pre-warm's whole content is that it exists: the bridge running is the
/// response, and by the time this runs the bridge is running. So the drain reads
/// nothing and acts on nothing — it moves the cursor, and that is the point. A
/// position left owed is a position the wake pass finds owed on its next sweep,
/// and on the one after that, spawning a conversation that is already up forever.
///
/// The pass over a burst is one advance, and the messages it steps over
/// unserved are exactly what a depth-1 subscription asked for: ten wake-word
/// fires and one ask for the same single thing.
async fn drain_prewarm(bridge: &Arc<ActiveBridge>, bus: &ChatBus, prewarm: &PeerLeaf) {
    let window = bus
        .messenger
        .store_for_address(&prewarm.address)
        .window(&prewarm.subscriber, prewarm.push_depth, Depth::Bounded(0))
        .await;
    let Some(window) = window else {
        // No position: the ring was re-registered without this conversation's
        // cursor, or a teardown raced the drain. Nothing is owed either way.
        return;
    };
    if window.new_entries().is_empty() {
        return;
    }
    // Paying the spawn cost ahead of the input is the whole point of a pre-warm,
    // so it has to hold the bridge open for the input to arrive to.
    bridge.note_bus_activity().await;
    if let Some((through, seen_floor)) = window.advance_span() {
        bus.messenger
            .advance_subscriber(
                &prewarm.address,
                &prewarm.subscriber,
                through,
                seen_floor,
                prewarm.noise,
            )
            .await;
    }
}

/// Decode one envelope and act on it.
///
/// A body that does not decode is a rejection, not a fault: the peer hears about
/// it on the record, the security log records it against the sender, and the
/// drain continues. A hostile peer must not be able to end this task.
///
/// The two verbs that provoke a CC turn — `send` and `compact` — redeem the
/// envelope's impetus and then draw the conversation's impetus pool, and are
/// refused when it is empty. `stop` and `set_model` provoke no turn, so they
/// neither redeem nor draw and keep working on an exhausted conversation.
async fn execute(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    envelope: &MessageEnvelope,
    models: &[ModelInfo],
) {
    let sender = envelope.sender.as_str().to_string();
    let command: ChatCommand = match chat::decode(&envelope.body) {
        Ok(command) => command,
        Err(e) => {
            reject(
                bridge,
                bus,
                &sender,
                None,
                &format!("unusable command body: {e}"),
            )
            .await;
            return;
        }
    };

    match command {
        ChatCommand::Send {
            text,
            model,
            attachments,
            correlation,
        } => {
            if !attachments.is_empty() {
                // TODO(chat-bus-attachments): upload ids resolve through a
                // per-user registry and a bus sender maps to no user, so the
                // send is refused whole rather than sent with its files
                // silently dropped.
                reject(
                    bridge,
                    bus,
                    &sender,
                    correlation,
                    "attachments on a bus send are not supported; publish the text alone",
                )
                .await;
                return;
            }
            // Both refusals a send can meet come before anything takes effect,
            // in the order that keeps each one whole: an alias the harness does
            // not offer refuses the command before it is accepted, so it
            // redeems nothing and draws nothing; an exhausted pool refuses it
            // after redemption but before the model becomes sticky, so a
            // refused send changes nothing about the conversation.
            if let Some(model) = &model
                && !model_is_offered(bridge, bus, &sender, &correlation, model, models).await
            {
                return;
            }
            if !redeem_and_admit(
                bridge,
                bus,
                &sender,
                &correlation,
                "send",
                super::impetus_pool::carries_impetus(envelope),
            )
            .await
            {
                return;
            }
            if let Some(model) = &model {
                apply_model(bridge, bus, &correlation, model).await;
            }
            send_text(bridge, bus, &sender, correlation, &text).await;
        }
        ChatCommand::Stop { correlation } => {
            // Stopping an idle conversation is an acknowledged no-op, and a dead
            // session is what the record's `status` already reports — neither is
            // the peer's error to hear about. Both outcomes ack, so a peer
            // waiting on its correlation is never waiting on nothing.
            if let Err(e) = bridge.interrupt().await {
                warn!(
                    conversation_id = bridge.conversation_id,
                    sender = %sender,
                    correlation = ?correlation,
                    "chat stop had nothing to interrupt: {e}"
                );
            }
            ack(bridge, bus, "stop", correlation).await;
        }
        ChatCommand::SetModel { model, correlation } => {
            if model_is_offered(bridge, bus, &sender, &correlation, &model, models).await {
                apply_model(bridge, bus, &correlation, &model).await;
            }
        }
        ChatCommand::Compact { correlation } => {
            compact(
                bridge,
                bus,
                &sender,
                correlation,
                super::impetus_pool::carries_impetus(envelope),
            )
            .await;
        }
    }
}

/// Hand accepted text to the harness and put it on the record.
///
/// The record is published here, not in the outbound translation, because only
/// here is the publishing participant known. The echo carries `bus_sender` so
/// the outbound leg skips the duplicate.
///
/// The command is admitted against the conversation's impetus pool before this
/// runs; the unit it costs is drawn only once the harness has the text.
async fn send_text(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: Option<String>,
    text: &str,
) {
    // A bus peer has no device and no timezone. Its participant id stands in for
    // the username unconditionally: which peer is speaking is not optional
    // context for a conversation several of them can drive.
    let local_now = chrono::Utc::now().with_timezone(&chrono_tz::UTC);
    let cc_text = crate::cc_message_prefix::build_cc_message_text(
        text, sender, None, &local_now, true, true, false,
    );

    let cc_send_err = super::accept_user_send(
        bridge,
        super::AcceptedSend {
            text,
            cc_text,
            extra_blocks: Vec::new(),
            // The row belongs to the conversation's owner, the same default a
            // system message takes; the bus sender is on the record instead.
            sender_user_id: bridge.user_id,
            sender_tz: None,
            sender_device_id: None,
            attachments: Vec::new(),
            selected_tasks: Vec::new(),
            origin: super::SendOrigin::Bus {
                sender: sender.to_string(),
                timestamp: local_now.to_rfc3339(),
            },
            interstitial: None,
            // The bus door refills on the envelope's impetus, above, not on the
            // fact that a command arrived. Door identity cannot tell a person
            // from an automation — a surface component is as much a bus peer as
            // an LLM is — so what restores the pool has to be authority the
            // message carries and no LLM-reachable API can mint.
            restores_impetus_pool: false,
        },
    )
    .await;

    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::UserMessage {
            text: text.to_string(),
            attachments: Vec::new(),
            sender: sender.to_string(),
            correlation: correlation.clone(),
        }),
    )
    .await;

    if let Some(e) = cc_send_err {
        warn!(
            conversation_id = bridge.conversation_id,
            sender = %sender,
            "chat send persisted but did not reach the harness: {e}"
        );
        publish(
            bridge,
            bus,
            &Outbound::Record(ChatEvent::Error {
                message: "message was saved but did not reach the assistant".to_string(),
                correlation,
            }),
        )
        .await;
        return;
    }
    // The turn the pool pays for is the one the harness got.
    bridge.draw_impetus_pool().await;
}

/// Redeem the commanding envelope's impetus, then report whether the pool can
/// pay for the turn this command provokes — refusing the peer on the record when
/// it cannot.
///
/// Every turn-provoking verb goes through here so the ordering cannot drift
/// between them: redemption first, so a command carrying impetus is never
/// refused by the pool it just restored — including by the held backlog that
/// same refill releases, which draws a unit of its own and is therefore run
/// after this command's admission rather than before it. The draw stays at each
/// verb's handoff site, which is where "the harness got it" is known.
async fn redeem_and_admit(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: &Option<String>,
    command: &str,
    impetus: bool,
) -> bool {
    if !impetus {
        if bridge.impetus_pool_has_room().await {
            return true;
        }
        refuse_exhausted(bridge, bus, sender, correlation.clone(), command).await;
        return false;
    }
    bridge.reset_impetus_pool().await;
    let admitted = bridge.impetus_pool_has_room().await;
    bridge.deliver_refilled_backlog().await;
    if admitted {
        return true;
    }
    // Only a ceiling of zero gets here: the conversation is attended-only and
    // even an attended bus command provokes no turn on it.
    refuse_exhausted(bridge, bus, sender, correlation.clone(), command).await;
    false
}

/// Refuse a turn-provoking command the conversation cannot pay for.
///
/// Not a [`reject`]: an exhausted pool is not a protocol violation and the peer
/// did nothing wrong. It hears the condition and the remedy on the record, under
/// its own correlation, and no CC work is enqueued.
async fn refuse_exhausted(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: Option<String>,
    command: &str,
) {
    warn!(
        conversation_id = bridge.conversation_id,
        sender = %sender,
        command,
        "chat command refused: the conversation has spent its unattended-activity allowance"
    );
    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::Error {
            message: format!(
                "{command} refused: this conversation has spent its allowance for unattended \
                 activity. Interacting with it directly restores the allowance."
            ),
            correlation,
        }),
    )
    .await;
}

/// Whether the harness offers this alias, refusing the peer on the record when
/// it does not.
///
/// Validation is separate from [`apply_model`] because a `send` carrying a model
/// has to answer "is this alias real" before it is accepted — an unknown alias
/// refuses the whole command, and a refusal before acceptance neither redeems
/// impetus nor draws the pool.
///
/// An empty model list means the harness has not reported one yet, and any alias
/// is allowed through.
async fn model_is_offered(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: &Option<String>,
    model: &str,
    models: &[ModelInfo],
) -> bool {
    if models.is_empty() || models.iter().any(|m| m.value == model) {
        return true;
    }
    reject(
        bridge,
        bus,
        sender,
        correlation.clone(),
        &format!("unknown model {model:?}"),
    )
    .await;
    false
}

/// Make a validated alias sticky and put the change on the record.
async fn apply_model(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    correlation: &Option<String>,
    model: &str,
) {
    if let Err(e) = bridge.set_model(model).await {
        // The harness refused or is not running. The failed alias is not
        // recorded anywhere — a later spawn takes the app's configured model,
        // not this one — so the peer must hear the refusal; otherwise it waits
        // on this correlation forever.
        warn!(
            conversation_id = bridge.conversation_id,
            model, "chat set_model did not reach the harness: {e}"
        );
        publish(
            bridge,
            bus,
            &Outbound::Record(ChatEvent::Error {
                message: format!("model {model:?} did not reach the assistant: {e}"),
                correlation: correlation.clone(),
            }),
        )
        .await;
        // Not a refusal of the command around it: a `send` carrying this model
        // still has text to deliver, and if the harness is gone for that too,
        // the send says so on the record with the same correlation.
        return;
    }
    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::ModelChanged {
            model: model.to_string(),
            correlation: correlation.clone(),
        }),
    )
    .await;
}

/// Acknowledge a command whose outcome no other event carries.
async fn ack(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    command: &str,
    correlation: Option<String>,
) {
    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::Ack {
            command: command.to_string(),
            correlation,
        }),
    )
    .await;
}

/// Ask the harness to compact, refusing when one is already running.
///
/// A compaction is a CC turn, so it redeems and draws exactly as a `send` does.
/// The already-running refusal comes first: a command refused before acceptance
/// neither redeems nor draws.
async fn compact(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: Option<String>,
    impetus: bool,
) {
    if !bridge.can_start_compaction().await {
        reject(
            bridge,
            bus,
            sender,
            correlation,
            "compaction is already in progress",
        )
        .await;
        return;
    }
    if !redeem_and_admit(bridge, bus, sender, &correlation, "compact", impetus).await {
        return;
    }
    let local_now = chrono::Utc::now().with_timezone(&chrono_tz::UTC);
    let rendered = crate::system_message::render_user_compaction_request(
        sender, None, &local_now, true, true, false,
    );
    if let Err(e) = bridge.send_system_message(rendered, None).await {
        publish(
            bridge,
            bus,
            &Outbound::Record(ChatEvent::Error {
                message: format!("compaction request did not reach the assistant: {e}"),
                correlation,
            }),
        )
        .await;
        return;
    }
    bridge.draw_impetus_pool().await;
    ack(bridge, bus, "compact", correlation).await;
}

/// Refuse a command: tell the peer on the record, and record the violation
/// against its sender.
///
/// Every rejection is a security event because every rejection is a peer
/// sending something the protocol does not define. It is **not** a fail2ban
/// signal, and must not be described as one: a body that reaches here has
/// already passed session authentication and the publish ACL gate, so its
/// author is an authenticated user's browser surface, an operator-granted
/// app/WASM principal, or in-process machinery — and there is no participant →
/// IP mapping at this depth anyway. Banning an address would ban the user. The
/// lever for a hostile or broken authenticated principal is alerting and grant
/// revocation, and this sender-keyed record is what feeds it; the volume is the
/// signal, so individual rejections raise no alert. Fail2ban's surface for chat
/// is the same as for everything else — transport and auth, where the IP exists.
///
/// `reason` embeds peer-supplied bytes (a decoder's rendering of the body it
/// could not parse), so the logged form is sanitized.
async fn reject(
    bridge: &Arc<ActiveBridge>,
    bus: &ChatBus,
    sender: &str,
    correlation: Option<String>,
    reason: &str,
) {
    let safe_reason = sanitize_untrusted_str(reason, MAX_LOGGED_UNTRUSTED_BYTES);
    log_component_security_event(
        SecurityEventType::SchemaViolation,
        sender,
        &format!(
            "chat command on conversation {} rejected: {safe_reason}",
            bridge.conversation_id
        ),
    );
    publish(
        bridge,
        bus,
        &Outbound::Record(ChatEvent::Error {
            message: reason.to_string(),
            correlation,
        }),
    )
    .await;
}

/// What one bridge broadcast becomes on the bus: zero, one, or more messages.
///
/// Exhaustive by construction. The variants that produce nothing are the ones
/// that are not conversation content — browser session management, permission
/// and tool-card dialogs, app-specific panels — and each is named so that adding
/// a variant forces a decision here.
fn translate(msg: WsServerMessage, turns: &mut TurnIds) -> Vec<Outbound> {
    match msg {
        WsServerMessage::StreamToken { token } => vec![Outbound::Stream(ChatStreamEvent::Tokens {
            text: token,
            kind: TokenKind::Text,
            turn: turns.current(),
        })],
        WsServerMessage::ThinkingToken { token } => {
            vec![Outbound::Stream(ChatStreamEvent::Tokens {
                text: token,
                kind: TokenKind::Thinking,
                turn: turns.current(),
            })]
        }
        WsServerMessage::AssistantMessage { text, .. } => {
            vec![Outbound::Record(ChatEvent::AssistantMessage {
                text: expect_live(text, "AssistantMessage"),
                turn: turns.finish(),
            })]
        }
        WsServerMessage::SystemMessageBroadcast { text, category, .. } => {
            vec![Outbound::Record(ChatEvent::SystemMessage {
                text: expect_live(text, "SystemMessageBroadcast"),
                category: system_category(category),
            })]
        }
        WsServerMessage::ToolUseSummary {
            tool_name,
            summary_text,
            ..
        } => vec![Outbound::Record(ChatEvent::ToolUse {
            summary: expect_live(summary_text, "ToolUseSummary"),
            tool_name,
        })],
        // A bus-origin send is already on the record, published by the inbound
        // leg with the participant that sent it. The echo exists for the
        // browsers; recording it again would double every message.
        WsServerMessage::UserMessageEcho {
            bus_sender: Some(_),
            ..
        } => Vec::new(),
        WsServerMessage::UserMessageEcho {
            text,
            username,
            attachments,
            ..
        } => vec![Outbound::Record(ChatEvent::UserMessage {
            text,
            attachments: attachments
                .into_iter()
                .map(|a| chat::AttachmentMeta {
                    upload_id: a.upload_id,
                    filename: a.filename,
                    media_type: a.media_type,
                    size: a.size,
                })
                .collect(),
            sender: legacy_ws_sender(&username),
            correlation: None,
        })],
        WsServerMessage::Status { state } => vec![Outbound::Record(ChatEvent::Status {
            state: cc_state(state),
        })],
        WsServerMessage::Error { message } => vec![Outbound::Record(ChatEvent::Error {
            message,
            correlation: None,
        })],
        WsServerMessage::ModelsAvailable { available_models } => {
            vec![Outbound::Record(ChatEvent::Models {
                available: models_from_broadcast(&available_models),
            })]
        }
        WsServerMessage::ContextUsage {
            usage_pct,
            current_tokens,
            max_tokens,
            reminder_pct,
            red_pct,
            reminder_tokens,
            red_tokens,
        } => vec![Outbound::Record(ChatEvent::ContextUsage {
            usage_pct,
            current_tokens,
            max_tokens,
            reminder_pct,
            red_pct,
            reminder_tokens,
            red_tokens,
        })],
        WsServerMessage::CostUsage {
            last_turn_usd,
            since_last_compaction_usd,
            last_24h_usd,
        } => vec![Outbound::Record(ChatEvent::CostUsage {
            last_turn_usd,
            since_last_compaction_usd,
            last_24h_usd,
        })],

        // Tool-call permission traffic. Reserved for the conversation's
        // `approvals` leaf; it must not leak onto the record.
        WsServerMessage::PermissionRequest { .. }
        | WsServerMessage::PermissionCancelled { .. }
        | WsServerMessage::PermissionResolved { .. }
        | WsServerMessage::ToolCardRequest { .. }
        | WsServerMessage::ToolCardResolved { .. }
        | WsServerMessage::ApprovalRuleError { .. }
        // Browser session and conversation management. A bus peer addresses one
        // conversation by channel name and has no tab to manage.
        | WsServerMessage::ConversationList { .. }
        | WsServerMessage::ConversationSwitched { .. }
        | WsServerMessage::HistoryComplete { .. }
        | WsServerMessage::HistoryPage { .. }
        | WsServerMessage::Welcome { .. }
        | WsServerMessage::PresenceUpdate { .. }
        | WsServerMessage::SetLayout { .. }
        | WsServerMessage::PrivacyChanged { .. }
        | WsServerMessage::SessionStolen { .. }
        | WsServerMessage::AppBusy { .. }
        | WsServerMessage::PermissionMode { .. }
        // App panels beside the chat thread: artifacts, todo lists, upload
        // targets, push subscription. Not conversation content.
        | WsServerMessage::ArtifactContent { .. }
        | WsServerMessage::ArtifactIndex { .. }
        | WsServerMessage::TargetResult { .. }
        | WsServerMessage::TodoState { .. }
        | WsServerMessage::TodoDoneResult { .. }
        | WsServerMessage::TodoMutationResult { .. }
        | WsServerMessage::PushVapidKey { .. }
        | WsServerMessage::PushEnabled { .. } => Vec::new(),
    }
}

/// The raw text of a live broadcast.
///
/// # Panics
///
/// Panics when it is absent. Only a history-replay reconstruction lacks it, and
/// those are sent straight to one browser — one reaching the broadcast channel
/// is a bug, and a record built from HTML would be worse than no record.
fn expect_live(text: Option<String>, variant: &str) -> String {
    text.unwrap_or_else(|| {
        panic!("chat bus adapter: {variant} on the live broadcast carries no raw text")
    })
}

/// Mirror the legacy session state onto the protocol's.
fn cc_state(state: brenn_lib::ws_types::CcState) -> chat::CcState {
    use brenn_lib::ws_types::CcState as Ws;
    match state {
        Ws::Idle => chat::CcState::Idle,
        Ws::Connecting => chat::CcState::Connecting,
        Ws::Thinking => chat::CcState::Thinking,
        Ws::AwaitingApproval => chat::CcState::AwaitingApproval,
        Ws::Compacting => chat::CcState::Compacting,
        Ws::Error => chat::CcState::Error,
    }
}

/// Mirror the legacy system-message category onto the protocol's.
fn system_category(
    category: brenn_lib::ws_types::SystemMessageCategory,
) -> chat::SystemMessageCategory {
    use brenn_lib::ws_types::SystemMessageCategory as Ws;
    match category {
        Ws::MessagesReceived => chat::SystemMessageCategory::MessagesReceived,
        Ws::EventDrain => chat::SystemMessageCategory::EventDrain,
        Ws::CompactionReminder => chat::SystemMessageCategory::CompactionReminder,
        Ws::CompactionHardTrigger => chat::SystemMessageCategory::CompactionHardTrigger,
        Ws::CompactionIdlePrompt => chat::SystemMessageCategory::CompactionIdlePrompt,
        Ws::IdleHook => chat::SystemMessageCategory::IdleHook,
        Ws::CompactionUserRequest => chat::SystemMessageCategory::CompactionUserRequest,
        Ws::UiError => chat::SystemMessageCategory::UiError,
        Ws::DeviceSlugReminder => chat::SystemMessageCategory::DeviceSlugReminder,
        Ws::GrafError => chat::SystemMessageCategory::GrafError,
        Ws::CompactionFailed => chat::SystemMessageCategory::CompactionFailed,
        Ws::DebugSnapshot => chat::SystemMessageCategory::DebugSnapshot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use brenn_lib::config::LlmChatConfig;
    use brenn_lib::db::Db;
    use brenn_lib::messaging::store::RingStores;
    use brenn_lib::messaging::{MessagingDirectory, MessagingGlobalConfig, query::NoopWakeRouter};
    use brenn_lib::ws_types as ws;

    const APP: &str = "pa-bob";

    fn record(msg: WsServerMessage) -> ChatEvent {
        let mut turns = TurnIds::default();
        match translate(msg, &mut turns).pop() {
            Some(Outbound::Record(event)) => event,
            other => panic!("expected one record event, got {other:?}"),
        }
    }

    #[test]
    fn a_turns_batches_share_an_id_the_completed_message_retires() {
        let mut turns = TurnIds::default();

        let first = translate(
            WsServerMessage::StreamToken {
                token: "par".to_string(),
            },
            &mut turns,
        );
        let second = translate(
            WsServerMessage::ThinkingToken {
                token: "hmm".to_string(),
            },
            &mut turns,
        );
        let turn_of = |out: &[Outbound]| match &out[0] {
            Outbound::Stream(ChatStreamEvent::Tokens { turn, .. }) => turn.clone(),
            Outbound::Record(ChatEvent::AssistantMessage { turn, .. }) => turn.clone(),
            other => panic!("no turn on {other:?}"),
        };
        assert_eq!(
            turn_of(&first),
            turn_of(&second),
            "text and thinking batches of one turn share its id"
        );

        let completed = translate(
            WsServerMessage::AssistantMessage {
                content: "<p>hi</p>".to_string(),
                seq: Some(1),
                text: Some("hi".to_string()),
            },
            &mut turns,
        );
        assert_eq!(
            turn_of(&completed),
            turn_of(&first),
            "the completed message carries the id its batches built toward"
        );

        let next = translate(
            WsServerMessage::StreamToken {
                token: "then".to_string(),
            },
            &mut turns,
        );
        assert_ne!(
            turn_of(&next),
            turn_of(&completed),
            "the next turn is a new id"
        );
    }

    #[test]
    fn the_record_carries_raw_text_never_html() {
        let event = record(WsServerMessage::AssistantMessage {
            content: "<h1>heading</h1>".to_string(),
            seq: None,
            text: Some("# heading".to_string()),
        });
        match event {
            ChatEvent::AssistantMessage { text, .. } => assert_eq!(text, "# heading"),
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    #[test]
    #[should_panic(expected = "carries no raw text")]
    fn a_replay_reconstruction_on_the_live_broadcast_is_fatal() {
        record(WsServerMessage::AssistantMessage {
            content: "<h1>heading</h1>".to_string(),
            seq: Some(7),
            text: None,
        });
    }

    #[test]
    fn system_messages_tool_uses_and_echoes_map_across() {
        assert_eq!(
            record(WsServerMessage::SystemMessageBroadcast {
                rendered_html: "<details>…</details>".to_string(),
                category: ws::SystemMessageCategory::IdleHook,
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                seq: Some(3),
                text: Some("repos are dirty".to_string()),
            }),
            ChatEvent::SystemMessage {
                text: "repos are dirty".to_string(),
                category: chat::SystemMessageCategory::IdleHook,
            }
        );

        assert_eq!(
            record(WsServerMessage::ToolUseSummary {
                tool_name: "Read".to_string(),
                rendered_summary: "<span>/etc/hosts</span>".to_string(),
                detail_html: None,
                seq: Some(4),
                summary_text: Some("Read: /etc/hosts".to_string()),
            }),
            ChatEvent::ToolUse {
                tool_name: "Read".to_string(),
                summary: "Read: /etc/hosts".to_string(),
            }
        );

        assert_eq!(
            record(WsServerMessage::UserMessageEcho {
                text: "hello".to_string(),
                username: "bob".to_string(),
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                attachments: vec![ws::AttachmentMeta {
                    upload_id: "u1".to_string(),
                    filename: "notes.md".to_string(),
                    media_type: "text/markdown".to_string(),
                    size: 17,
                }],
                selected_tasks: vec![],
                seq: Some(5),
                bus_sender: None,
            }),
            ChatEvent::UserMessage {
                text: "hello".to_string(),
                attachments: vec![chat::AttachmentMeta {
                    upload_id: "u1".to_string(),
                    filename: "notes.md".to_string(),
                    media_type: "text/markdown".to_string(),
                    size: 17,
                }],
                sender: "legacy-ws:bob".to_string(),
                correlation: None,
            },
            "input that arrived over the legacy websocket is attributed as such"
        );
    }

    #[test]
    fn telemetry_and_state_map_across() {
        assert_eq!(
            record(WsServerMessage::Status {
                state: ws::CcState::Compacting
            }),
            ChatEvent::Status {
                state: chat::CcState::Compacting
            }
        );
        assert_eq!(
            record(WsServerMessage::Error {
                message: "CC died".to_string()
            }),
            ChatEvent::Error {
                message: "CC died".to_string(),
                correlation: None,
            }
        );
        assert_eq!(
            record(WsServerMessage::CostUsage {
                last_turn_usd: 0.01,
                since_last_compaction_usd: 0.5,
                last_24h_usd: 9.0,
            }),
            ChatEvent::CostUsage {
                last_turn_usd: 0.01,
                since_last_compaction_usd: 0.5,
                last_24h_usd: 9.0,
            }
        );
        assert_eq!(
            record(WsServerMessage::ModelsAvailable {
                available_models: vec![ws::ModelInfo {
                    value: "sonnet".to_string(),
                    display_name: "Sonnet".to_string(),
                    description: "everyday".to_string(),
                }],
            }),
            ChatEvent::Models {
                available: vec![ModelInfo {
                    value: "sonnet".to_string(),
                    display_name: "Sonnet".to_string(),
                    description: "everyday".to_string(),
                }],
            }
        );
    }

    #[test]
    fn non_conversation_traffic_reaches_neither_channel() {
        let mut turns = TurnIds::default();
        for msg in [
            WsServerMessage::PermissionCancelled {
                request_id: "r1".to_string(),
            },
            WsServerMessage::ArtifactIndex { files: vec![] },
            WsServerMessage::PushEnabled { enabled: true },
            WsServerMessage::SetLayout {
                layout: ws::PaneLayout::SinglePane,
            },
        ] {
            assert!(
                translate(msg.clone(), &mut turns).is_empty(),
                "{msg:?} is not conversation content"
            );
        }
    }

    /// A bridge whose app authors nothing and carries the derived chat-tree
    /// authority on its harness policy alone, and whose conversation has been
    /// provisioned its four chat channels.
    ///
    /// The bridge keeps a sender on its own broadcast for as long as it lives, so
    /// a test that wants the adapter's `recv` to report a closed channel feeds it
    /// from a channel of the test's own instead.
    async fn chat_bridge() -> (Arc<ActiveBridge>, Arc<Messenger>, Db) {
        chat_bridge_with(MessagingGlobalConfig::default()).await
    }

    /// [`chat_bridge`], with the bus-wide defaults the caller wants — the body
    /// limit, in practice.
    async fn chat_bridge_with(
        defaults: MessagingGlobalConfig,
    ) -> (Arc<ActiveBridge>, Arc<Messenger>, Db) {
        chat_bridge_full(ChatFixture {
            defaults,
            ..Default::default()
        })
        .await
    }

    /// [`chat_bridge`] for a persistent app, whose browser door carries a
    /// post-detach grace — so the two doors can be holding the same bridge on
    /// two different schedules.
    async fn chat_bridge_persistent(
        idle_timeout: Duration,
    ) -> (Arc<ActiveBridge>, Arc<Messenger>, Db) {
        chat_bridge_full(ChatFixture {
            idle_timeout: Some(idle_timeout),
            ..Default::default()
        })
        .await
    }

    /// [`chat_bridge`] whose conversation also subscribes to an ordinary bus
    /// channel, at the impetus-pool ceiling the caller names — for the cases
    /// where the door and the ambience drain contend for the same pool.
    async fn chat_bridge_with_ambience(ceiling: u32) -> (Arc<ActiveBridge>, Arc<Messenger>, Db) {
        chat_bridge_full(ChatFixture {
            ceiling,
            ambience: true,
            ..Default::default()
        })
        .await
    }

    /// What a chat fixture varies.
    struct ChatFixture {
        /// Bus-wide defaults — the body limit, in practice.
        defaults: MessagingGlobalConfig,
        /// The browser door's post-detach grace.
        idle_timeout: Option<Duration>,
        /// The impetus pool's ceiling.
        ceiling: u32,
        /// Whether the conversation subscribes to [`AMBIENCE_ADDRESS`], so the
        /// drain has something to deliver or hold.
        ambience: bool,
    }

    impl Default for ChatFixture {
        fn default() -> Self {
            Self {
                defaults: MessagingGlobalConfig::default(),
                idle_timeout: None,
                ceiling: 100,
                ambience: false,
            }
        }
    }

    /// An ordinary bus channel, standing in for whatever ambience an operator
    /// subscribed the conversation to.
    const AMBIENCE_ADDRESS: &str = "brenn:ambience";

    const AMBIENCE_UUID: uuid::Uuid = uuid::Uuid::from_bytes([
        0x00, 0x00, 0x00, 0x00, 0xa1, 0xb1, 0xe1, 0xce, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ]);

    /// The directory entry for [`AMBIENCE_ADDRESS`], subscribed by one app.
    fn ambience_entry(app_slug: &str) -> brenn_lib::messaging::ChannelEntry {
        use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};

        brenn_lib::messaging::ChannelEntry {
            uuid: AMBIENCE_UUID,
            address: AMBIENCE_ADDRESS.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![brenn_lib::messaging::SubscriberEntry {
                kind: brenn_lib::messaging::SubscriberEntryKind::App(app_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
            }],
            transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
            mount: None,
        }
    }

    /// One message on [`AMBIENCE_ADDRESS`], from a peer that is not this
    /// conversation.
    async fn publish_ambience(db: &Db, body: &str) {
        let conn = db.lock().await;
        brenn_lib::messaging::db::insert_message(
            &conn,
            AMBIENCE_UUID,
            "test-source",
            "app:peer",
            body,
            Urgency::Normal,
            brenn_lib::messaging::ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            brenn_lib::messaging::db::utc_to_ns(chrono::Utc::now()),
        );
    }

    async fn chat_bridge_full(fixture: ChatFixture) -> (Arc<ActiveBridge>, Arc<Messenger>, Db) {
        let ChatFixture {
            defaults,
            idle_timeout,
            ceiling,
            ambience,
        } = fixture;
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (username, password_hash, created_at) \
                 VALUES ('bob', 'x', '2026-01-01')",
                [],
            )
            .unwrap();
            let uid = conn.last_insert_rowid();
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, APP, false);
            (uid, cid)
        };

        let mut app = crate::test_support::app_config::default_test_app_config(APP, APP);
        app.messaging = Some(brenn_lib::messaging::ResolvedMessagingConfig {
            send_budget: ceiling,
            subscriptions: vec![],
        });
        app.policy = brenn_lib::access::AppPolicy::default();
        if ambience {
            // The operator-authored half: the app reads one ordinary channel,
            // and its conversation is the delivery target for what lands there.
            app.singleton = true;
            app.allowed_users = vec!["bob".to_string()];
            app.policy
                .grants
                .insert(brenn_lib::access::AppCapability::MessagingSubscribe);
            app.policy
                .acls
                .brenn_subscribe
                .push(brenn_lib::access::acl::ChannelMatcher::Prefix(
                    "ambience".to_string(),
                ));
        }
        app.chat_harness_policy = LlmChatConfig::default().harness_policy(APP);
        let mut apps = indexmap::IndexMap::new();
        apps.insert(APP.to_string(), app);

        let entries = if ambience {
            vec![ambience_entry(APP)]
        } else {
            vec![]
        };
        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(entries.clone())),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn brenn_lib::messaging::WakeRouter>,
            defaults,
        )
        .with_ring_stores(Arc::new(RingStores::empty()));
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, &entries);
            messenger.provision_conversation_chat_channels(&conn, APP, conversation_id);
        }
        // The position has to exist before the publish it is meant to catch.
        messenger.attach_conversation_subscribers().await;

        let (tx, _rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test_full(
            user_id,
            conversation_id,
            APP,
            db.clone(),
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            crate::active_bridge::test_fixtures::TestBridgeConfig {
                messenger: Some(messenger.clone()),
                idle_timeout,
                send_budget: ceiling,
                ..Default::default()
            },
        );
        (bridge, messenger, db)
    }

    /// Every body the conversation's record channel holds, in publish order.
    async fn record_bodies(messenger: &Messenger, db: &Db, conversation_id: i64) -> Vec<ChatEvent> {
        let address = chat_address(
            &messenger.llm_chat().prefix,
            APP,
            ChatLeaf::Out,
            conversation_id,
        );
        let uuid = messenger
            .directory()
            .resolve(&address)
            .expect("the record channel is provisioned")
            .uuid;
        let conn = db.lock().await;
        let mut stmt = conn
            .prepare("SELECT body FROM messaging_messages WHERE channel_uuid = ?1 ORDER BY id ASC")
            .unwrap();
        let bodies: Vec<String> = stmt
            .query_map(rusqlite::params![uuid.as_bytes().as_slice()], |r| {
                r.get::<_, String>(0)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        bodies
            .iter()
            .map(|b| chat::decode(b).expect("the adapter publishes decodable bodies"))
            .collect()
    }

    #[tokio::test]
    async fn a_turn_lands_as_batches_on_the_stream_and_a_message_on_the_record() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let (tx, rx) = broadcast::channel(64);

        let adapter = tokio::spawn(run_bus_chat_adapter(
            bridge.clone(),
            rx,
            vec![ModelOption {
                value: "sonnet".to_string(),
                display_name: "Sonnet".to_string(),
                description: "everyday".to_string(),
            }],
        ));

        tx.send(WsServerMessage::StreamToken {
            token: "he".to_string(),
        })
        .unwrap();
        tx.send(WsServerMessage::StreamToken {
            token: "llo".to_string(),
        })
        .unwrap();
        tx.send(WsServerMessage::AssistantMessage {
            content: "<p>hello</p>".to_string(),
            seq: Some(1),
            text: Some("hello".to_string()),
        })
        .unwrap();
        drop(tx);
        adapter
            .await
            .expect("the adapter exits when the bridge does");

        let events = record_bodies(&messenger, &db, conversation_id).await;
        let turn = match &events[..] {
            [
                ChatEvent::Models { available },
                ChatEvent::AssistantMessage { text, turn },
            ] => {
                assert_eq!(available.len(), 1, "the model list opens the record");
                assert_eq!(text, "hello", "the record carries raw text");
                turn.clone()
            }
            other => panic!("unexpected record: {other:?}"),
        };

        let stream = messenger
            .ring_stores()
            .get_by_address(&chat_address(
                &messenger.llm_chat().prefix,
                APP,
                ChatLeaf::Stream,
                conversation_id,
            ))
            .expect("the stream channel is provisioned");
        let batches: Vec<ChatStreamEvent> = stream
            .retained_tail(10)
            .iter()
            .map(|m| chat::decode(&m.envelope.body).expect("decodable batch"))
            .collect();
        assert_eq!(
            batches,
            vec![
                ChatStreamEvent::Tokens {
                    text: "he".to_string(),
                    kind: TokenKind::Text,
                    turn: turn.clone(),
                },
                ChatStreamEvent::Tokens {
                    text: "llo".to_string(),
                    kind: TokenKind::Text,
                    turn,
                },
            ],
            "the batches share the completed message's turn"
        );
    }

    /// The record opens with the model list and carries it again only when it
    /// changes: every row of the retained window is a row of history a repeat
    /// would displace.
    #[tokio::test]
    async fn an_unchanged_model_list_is_not_recorded_again() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let (tx, rx) = broadcast::channel(64);
        let sonnet = ws::ModelInfo {
            value: "sonnet".to_string(),
            display_name: "Sonnet".to_string(),
            description: "everyday".to_string(),
        };

        let adapter = tokio::spawn(run_bus_chat_adapter(
            bridge.clone(),
            rx,
            vec![ModelOption {
                value: "sonnet".to_string(),
                display_name: "Sonnet".to_string(),
                description: "everyday".to_string(),
            }],
        ));

        tx.send(WsServerMessage::ModelsAvailable {
            available_models: vec![sonnet.clone()],
        })
        .unwrap();
        tx.send(WsServerMessage::ModelsAvailable {
            available_models: vec![
                sonnet,
                ws::ModelInfo {
                    value: "opus".to_string(),
                    display_name: "Opus".to_string(),
                    description: "the hard ones".to_string(),
                },
            ],
        })
        .unwrap();
        drop(tx);
        adapter
            .await
            .expect("the adapter exits when the bridge does");

        let events = record_bodies(&messenger, &db, conversation_id).await;
        match &events[..] {
            [
                ChatEvent::Models { available: opening },
                ChatEvent::Models { available: changed },
            ] => {
                assert_eq!(opening.len(), 1, "the list the harness reported at spawn");
                assert_eq!(changed.len(), 2, "only the list that changed is recorded");
            }
            other => panic!("the identical list was recorded again: {other:?}"),
        }
    }

    /// Attach the command cursor the way the adapter's start does, and hand back
    /// what a drain needs.
    async fn command_cursor(bridge: &Arc<ActiveBridge>, bus: &ChatBus) -> PeerLeaf {
        let commands = peer_leaf(bridge, bus, ChatLeaf::In);
        bus.messenger
            .attach_subscriber(
                &commands.address,
                APP,
                &commands.subscriber,
                commands.push_depth,
            )
            .await;
        commands
    }

    /// Put one command on the conversation's command channel, as a peer would.
    async fn publish_command(messenger: &Messenger, conversation_id: i64, body: &str) {
        let address = chat_address(
            &messenger.llm_chat().prefix,
            APP,
            ChatLeaf::In,
            conversation_id,
        );
        let result = messenger
            .publish_from_conversation(conversation_id, APP, &address, body, Urgency::Normal)
            .await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "the command channel must accept a command: {result:?}"
        );
    }

    /// Every user-authored row the conversation holds.
    async fn user_rows(db: &Db, conversation_id: i64) -> Vec<String> {
        let conn = db.lock().await;
        brenn_lib::conversation::get_messages(&conn, conversation_id)
            .into_iter()
            .filter(|m| m.msg_type == "user")
            .map(|m| m.payload)
            .collect()
    }

    #[tokio::test]
    async fn a_command_reaches_the_harness_and_the_record_exactly_once() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let mut broadcast_rx = bridge.event_tx.subscribe();
        let mut cc_rx =
            crate::active_bridge::test_support::install_recording_session(&bridge).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "what is the balance".to_string(),
                model: None,
                attachments: vec![],
                correlation: Some("c-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let rows = user_rows(&db, conversation_id).await;
        assert_eq!(rows.len(), 1, "one accepted command is one persisted row");
        assert!(rows[0].contains("what is the balance"));

        // What the model sees, and who it is told said it: several peers can
        // drive one conversation, so the attribution is the message.
        let outgoing = cc_rx.try_recv().expect("the harness receives the turn");
        let cc_text = crate::active_bridge::test_support::user_text(&outgoing);
        assert!(
            cc_text.contains("what is the balance"),
            "the text reaches CC: {cc_text}"
        );
        assert!(
            cc_text.contains(&format!("conversation:{conversation_id}")),
            "the publishing participant is named to CC: {cc_text}"
        );

        // The dedup rests on one field crossing one seam, so take the echo the
        // production path actually broadcast and run it through the translation
        // that has to drop it.
        let echo = broadcast_rx
            .try_recv()
            .expect("the accepted send is echoed to the browsers");
        match &echo {
            WsServerMessage::UserMessageEcho { bus_sender, .. } => assert_eq!(
                bus_sender.as_deref(),
                Some(format!("conversation:{conversation_id}").as_str()),
                "the echo carries the publishing participant",
            ),
            other => panic!("expected the user echo first, got {other:?}"),
        }
        let mut turns = TurnIds::default();
        assert!(
            translate(echo, &mut turns).is_empty(),
            "the record already has this one",
        );

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            !events.iter().any(|e| matches!(e, ChatEvent::Error { .. })),
            "the send reached the harness: {events:?}",
        );
        let echoes: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ChatEvent::UserMessage {
                    text,
                    sender,
                    correlation,
                    ..
                } => Some((text.clone(), sender.clone(), correlation.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            echoes,
            vec![(
                "what is the balance".to_string(),
                format!("conversation:{conversation_id}"),
                Some("c-1".to_string()),
            )],
            "the record carries one entry, attributed to the publishing participant"
        );

        drain_commands(&bridge, &bus, &commands, &[]).await;
        assert_eq!(
            user_rows(&db, conversation_id).await.len(),
            1,
            "an advanced cursor does not re-execute"
        );
    }

    #[tokio::test]
    async fn the_broadcast_echo_of_a_bus_send_is_not_recorded_twice() {
        let mut turns = TurnIds::default();
        assert!(
            translate(
                WsServerMessage::UserMessageEcho {
                    text: "hello".to_string(),
                    username: "conversation:4".to_string(),
                    timestamp: "2026-01-01T00:00:00Z".to_string(),
                    attachments: vec![],
                    selected_tasks: vec![],
                    seq: Some(9),
                    bus_sender: Some("conversation:4".to_string()),
                },
                &mut turns,
            )
            .is_empty(),
            "the inbound leg already recorded this one"
        );
    }

    /// `stop` has no other outcome event, so its ack is the only thing a peer
    /// waiting on its correlation ever hears — including for the no-op stop of
    /// a conversation with nothing running.
    #[tokio::test]
    async fn stopping_an_idle_conversation_acks_and_records_no_error() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Stop {
                correlation: Some("s-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert_eq!(
            events,
            vec![ChatEvent::Ack {
                command: "stop".to_string(),
                correlation: Some("s-1".to_string()),
            }],
            "a no-op stop is acknowledged, not an error",
        );
    }

    /// A second compaction while one is running is refused, and refused whole:
    /// the peer hears why, and nothing is sent.
    #[tokio::test]
    async fn compaction_while_one_is_running_is_refused_with_its_correlation() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        bridge.compaction.lock().await.phase =
            crate::active_bridge::compaction::CompactionPhase::Compacting;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Compact {
                correlation: Some("k-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        match &events[..] {
            [
                ChatEvent::Error {
                    message,
                    correlation,
                },
            ] => {
                assert!(message.contains("already in progress"), "{message}");
                assert_eq!(correlation.as_deref(), Some("k-1"));
            }
            other => panic!("expected one correlated refusal, got {other:?}"),
        }
        let conn = db.lock().await;
        assert!(
            brenn_lib::conversation::get_messages(&conn, conversation_id).is_empty(),
            "a refused compaction persists nothing",
        );
    }

    /// A sticky model change announces itself on the record, carrying the
    /// correlation of the command that made it.
    #[tokio::test]
    async fn a_model_change_is_announced_with_its_correlation() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        // The harness already holds this alias, so the set is a no-op that
        // succeeds — the branch that publishes.
        *bridge.last_set_model.lock().await = Some("sonnet".to_string());

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::SetModel {
                model: "sonnet".to_string(),
                correlation: Some("m-2".to_string()),
            }),
        )
        .await;
        let models = vec![ModelInfo {
            value: "sonnet".to_string(),
            display_name: "Sonnet".to_string(),
            description: "everyday".to_string(),
        }];
        drain_commands(&bridge, &bus, &commands, &models).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert_eq!(
            events,
            vec![ChatEvent::ModelChanged {
                model: "sonnet".to_string(),
                correlation: Some("m-2".to_string()),
            }],
        );
    }

    /// And when it does not take, the peer hears that too. Nothing records
    /// the refused alias — a fresh spawn takes the app's configured model —
    /// so a silent warning here would leave a peer waiting on its correlation
    /// forever.
    #[tokio::test]
    async fn a_model_the_harness_never_got_is_reported_with_its_correlation() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        // No session installed: the alias reaches no harness.
        assert!(bridge.session.lock().await.is_none());

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::SetModel {
                model: "opus".to_string(),
                correlation: Some("m-3".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        match &record_bodies(&messenger, &db, conversation_id).await[..] {
            [
                ChatEvent::Error {
                    message,
                    correlation,
                },
            ] => {
                assert!(message.contains("opus"), "{message}");
                assert_eq!(correlation.as_deref(), Some("m-3"));
            }
            other => panic!("a model that did not take must say so: {other:?}"),
        }
        assert!(
            bridge.last_set_model.lock().await.is_none(),
            "and nothing anywhere is holding the alias for a later spawn",
        );
    }

    /// Before the harness has reported a model list there is nothing to
    /// validate against, and an alias the server cannot check is allowed
    /// through rather than refused on no evidence.
    #[tokio::test]
    async fn an_empty_model_list_accepts_any_alias() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        *bridge.last_set_model.lock().await = Some("some-future-model".to_string());

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::SetModel {
                model: "some-future-model".to_string(),
                correlation: None,
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert_eq!(
            events,
            vec![ChatEvent::ModelChanged {
                model: "some-future-model".to_string(),
                correlation: None,
            }],
            "an unvalidatable alias is not a rejection",
        );
    }

    /// The notify is how a command reaches a conversation whose adapter is
    /// already running — every other command test drains by hand, so this is
    /// the only one that proves the loop serves one at all.
    #[tokio::test]
    async fn a_notify_makes_the_running_adapter_drain() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let (tx, rx) = broadcast::channel(8);
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));

        // Give the adapter its start-up drain first, on an empty channel, so
        // the command below can only reach it through the notify.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "are you awake".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;
        for _ in 0..100 {
            bridge.chat_commands.notify_one();
            if !user_rows(&db, conversation_id).await.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        drop(tx);
        adapter
            .await
            .expect("the adapter exits when the bridge does");

        let rows = user_rows(&db, conversation_id).await;
        assert_eq!(rows.len(), 1, "the notify served the command: {rows:?}");
    }

    #[tokio::test]
    async fn a_hostile_body_is_rejected_and_the_next_command_still_works() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;

        publish_command(
            &messenger,
            conversation_id,
            "{\"v\":1,\"type\":\"detonate\"}",
        )
        .await;
        publish_command(&messenger, conversation_id, "not json at all").await;
        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "still here".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        let rejections = events
            .iter()
            .filter(|e| {
                matches!(e, ChatEvent::Error { message, .. } if message.contains("unusable command"))
            })
            .count();
        assert_eq!(rejections, 2, "each bad body is refused on the record");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ChatEvent::UserMessage { text, .. } if text == "still here")),
            "a bad body does not stop the drain: {events:?}"
        );
    }

    /// The rejection detail quotes the decoder's rendering of a body the peer
    /// chose, so it reaches the log through the untrusted-string sanitizer:
    /// escaped and length-capped, not pasted in whole.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn the_rejection_log_line_is_sanitized() {
        let (bridge, messenger, _db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;

        // An unknown command type of the peer's choosing: framing bytes up front
        // and length after. The decoder quotes the variant it did not recognize,
        // so those bytes are in the rejection reason — asserted here, because
        // the sanitizing is only interesting while that stays true.
        let body = format!(
            "{{\"v\":1,\"type\":\"deto\\u0007na\\rte{}\"}}",
            "A".repeat(brenn_common::MAX_LOGGED_UNTRUSTED_BYTES)
        );
        let reason = chat::decode::<ChatCommand>(&body)
            .expect_err("an unknown command type is a rejection")
            .to_string();
        assert!(
            reason.contains('\u{7}') && reason.contains('\r'),
            "the decoder no longer quotes the peer's bytes: {reason:?}"
        );
        assert!(reason.len() > brenn_common::MAX_LOGGED_UNTRUSTED_BYTES);

        publish_command(&messenger, conversation_id, &body).await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert!(logs_contain("schema_violation"));
        // The escaped, capped form: the peer cannot spend an unbounded slice of a
        // log line, and what does land is escaped rather than raw.
        assert!(logs_contain(brenn_common::TRUNCATION_MARKER));
        assert!(logs_contain("deto\\\\u{7}na\\\\rte"));
    }

    #[tokio::test]
    async fn attachments_and_unknown_models_are_refused_whole() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "look at this".to_string(),
                model: None,
                attachments: vec![chat::AttachmentRef {
                    upload_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                }],
                correlation: Some("a-1".to_string()),
            }),
        )
        .await;
        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "and this".to_string(),
                model: Some("gpt-4".to_string()),
                attachments: vec![],
                correlation: Some("m-1".to_string()),
            }),
        )
        .await;

        let models = vec![ModelInfo {
            value: "sonnet".to_string(),
            display_name: "Sonnet".to_string(),
            description: "everyday".to_string(),
        }];
        drain_commands(&bridge, &bus, &commands, &models).await;

        assert!(
            user_rows(&db, conversation_id).await.is_empty(),
            "a refused send persists nothing"
        );
        let events = record_bodies(&messenger, &db, conversation_id).await;
        let refusals: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ChatEvent::Error {
                    message,
                    correlation: Some(c),
                } => Some((c.clone(), message.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            refusals.len(),
            2,
            "both refusals are correlated: {events:?}"
        );
        assert_eq!(refusals[0].0, "a-1");
        assert!(refusals[0].1.contains("attachments"));
        assert_eq!(refusals[1].0, "m-1");
        assert!(refusals[1].1.contains("gpt-4"));
    }

    #[tokio::test]
    async fn the_adapter_drains_the_command_channel_on_the_way_up() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        // Attach first, so the drain below reads the position the adapter would
        // find rather than one this test created after the publish.
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let _commands = command_cursor(&bridge, &bus).await;
        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "wake up".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;

        let (tx, rx) = broadcast::channel(8);
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));
        drop(tx);
        adapter
            .await
            .expect("the adapter exits when the bridge does");

        let rows = user_rows(&db, conversation_id).await;
        assert_eq!(rows.len(), 1, "the backlog is served at attach: {rows:?}");
    }

    /// A pre-warm asks for a bridge and nothing else, so by the time the adapter
    /// runs, the whole request is already answered. What is left is the cursor:
    /// a position still owed is a position the wake pass finds owed on its next
    /// sweep, and on every sweep after that, spawning a conversation that is
    /// already up. So the drain moves it — and says nothing on the record.
    #[tokio::test]
    async fn the_adapter_steps_past_a_pre_warm_and_records_nothing() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let prewarm = peer_leaf(&bridge, &bus, ChatLeaf::Wake);

        let result = messenger
            .publish_from_conversation(conversation_id, APP, &prewarm.address, "", Urgency::VeryLow)
            .await;
        assert!(
            matches!(result, brenn_lib::messaging::PublishResult::Ok { .. }),
            "{result:?}"
        );
        assert_eq!(owed_prewarms(&messenger, &prewarm).await, 1);

        let (tx, rx) = broadcast::channel(8);
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));
        drop(tx);
        adapter
            .await
            .expect("the adapter exits when the bridge does");

        assert_eq!(
            owed_prewarms(&messenger, &prewarm).await,
            0,
            "the position moved, so the next sweep does not spawn again",
        );
        assert!(
            record_bodies(&messenger, &db, conversation_id)
                .await
                .iter()
                .all(|e| matches!(e, ChatEvent::Models { .. })),
            "a pre-warm carries no content and puts none on the record",
        );
    }

    /// How many pre-warms the conversation is still owed.
    async fn owed_prewarms(messenger: &Messenger, prewarm: &PeerLeaf) -> usize {
        messenger
            .store_for_address(&prewarm.address)
            .window(&prewarm.subscriber, prewarm.push_depth, Depth::Bounded(0))
            .await
            .expect("provisioning gave the conversation its pre-warm position")
            .new_entries()
            .len()
    }

    #[tokio::test]
    async fn a_gap_in_the_broadcast_becomes_a_gap_marker_in_the_record() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let (tx, rx) = broadcast::channel(8);

        // Overrun the channel before the adapter reads any of it: the sends are
        // synchronous and the adapter task is not polled until this test awaits.
        for i in 0..20 {
            tx.send(WsServerMessage::StreamToken {
                token: i.to_string(),
            })
            .unwrap();
        }
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));
        drop(tx);
        adapter.await.expect("the adapter survives a lag");

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                ChatEvent::Error { message, correlation: None } if message.contains("gap")
            )),
            "a dropped span must be visible in the record: {events:?}"
        );
    }

    /// An event too large for the record channel leaves a hole, and the record
    /// is the authoritative conversation — so the hole is marked rather than
    /// silent.
    #[tokio::test]
    async fn an_oversized_record_event_leaves_a_marker_and_an_oversized_batch_does_not() {
        let (bridge, messenger, db) = chat_bridge_with(MessagingGlobalConfig {
            max_body_bytes: 512,
            ..MessagingGlobalConfig::default()
        })
        .await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);

        publish(
            &bridge,
            &bus,
            &Outbound::Record(ChatEvent::AssistantMessage {
                text: "x".repeat(1_000),
                turn: "t-1".to_string(),
            }),
        )
        .await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        match &events[..] {
            [
                ChatEvent::Error {
                    message,
                    correlation: None,
                },
            ] => {
                assert!(message.contains("512"), "the limit is named: {message}");
                assert!(
                    message.contains("not published"),
                    "and so is the loss: {message}"
                );
            }
            other => panic!("expected exactly one oversize marker, got {other:?}"),
        }

        // The stream's loss contract already covers a dropped batch, and the
        // completed message supersedes it — a marker there would be noise.
        publish(
            &bridge,
            &bus,
            &Outbound::Stream(ChatStreamEvent::Tokens {
                text: "y".repeat(1_000),
                kind: TokenKind::Text,
                turn: "t-1".to_string(),
            }),
        )
        .await;
        assert_eq!(
            record_bodies(&messenger, &db, conversation_id).await.len(),
            1,
            "an oversized batch adds nothing to the record",
        );
    }

    /// A record event the send rate refuses is a hole too, and the record says
    /// so as soon as the limiter admits a publish again.
    #[tokio::test]
    async fn record_events_refused_by_the_send_rate_become_a_gap_marker() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);

        // Stand in for the limiter: the count is what a refusal leaves behind.
        bus.record_drops.store(3, Ordering::Relaxed);
        publish(
            &bridge,
            &bus,
            &Outbound::Record(ChatEvent::Status {
                state: chat::CcState::Idle,
            }),
        )
        .await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        match &events[..] {
            [
                ChatEvent::Status { .. },
                ChatEvent::Error {
                    message,
                    correlation: None,
                },
            ] => assert!(
                message.contains('3') && message.contains("gap"),
                "the marker counts what was refused: {message}"
            ),
            other => panic!("expected the event then its gap marker, got {other:?}"),
        }
        assert_eq!(
            bus.record_drops.load(Ordering::Relaxed),
            0,
            "a reported gap is not reported twice",
        );
    }

    // -----------------------------------------------------------------------
    // Bridge lifetime: the bus as a door that holds a conversation open
    // -----------------------------------------------------------------------

    /// A peer driving a conversation no browser is watching keeps its bridge —
    /// that is the whole point of the bus door. A drain flagged while the bus
    /// was quiet is cancelled by the same interaction, the way a reconnecting
    /// browser cancels one.
    #[tokio::test]
    async fn a_command_takes_the_bus_hold_and_cancels_a_flagged_drain() {
        let (bridge, messenger, _db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;

        assert_eq!(
            bridge.lifetime.verdict(0),
            crate::active_bridge::lifetime::Verdict::Drain,
            "nothing holds a bridge no browser and no peer has touched"
        );
        // The bridge was condemned while the bus was quiet.
        bridge.drain_on_idle.store(true, Ordering::SeqCst);

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Stop { correlation: None }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert!(
            matches!(
                bridge.lifetime.verdict(0),
                crate::active_bridge::lifetime::Verdict::KeepAlive { .. }
            ),
            "the peer that just spoke is holding the bridge open"
        );
        assert!(
            !bridge.drain_on_idle.load(Ordering::SeqCst),
            "the interaction reprieves a bridge that was flagged to drain"
        );
    }

    /// And the stamp is on what a drain *served*, not on the drain running. A
    /// bridge on a bus deployment runs both drains at spawn and again on every
    /// notify; if an empty pass took the hold, every browser-driven and
    /// automation-fired conversation on the deployment would be held by the bus
    /// door it never uses, and its CC would outlive every tab close.
    ///
    /// The bridge a peer's own wake bought is held at the spawn instead
    /// (`AppState::spawn_chat_wake`), which is what lets this stay narrow.
    #[tokio::test]
    async fn an_empty_drain_takes_no_bus_hold() {
        let (bridge, messenger, _db) = chat_bridge().await;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let prewarm = peer_leaf(&bridge, &bus, ChatLeaf::Wake);

        drain_commands(&bridge, &bus, &commands, &[]).await;
        drain_prewarm(&bridge, &bus, &prewarm).await;

        assert_eq!(
            bridge.lifetime.verdict(0),
            crate::active_bridge::lifetime::Verdict::Drain,
            "a drain that found nothing has nothing to hold the bridge for",
        );
    }

    /// The pre-warm exists to pay the spawn cost before the input arrives, which
    /// only works if it also holds the bridge for the input to arrive to.
    #[tokio::test]
    async fn a_pre_warm_takes_the_bus_hold() {
        let (bridge, messenger, _db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let prewarm = peer_leaf(&bridge, &bus, ChatLeaf::Wake);

        messenger
            .publish_from_conversation(conversation_id, APP, &prewarm.address, "", Urgency::VeryLow)
            .await;
        drain_prewarm(&bridge, &bus, &prewarm).await;

        assert!(
            matches!(
                bridge.lifetime.verdict(0),
                crate::active_bridge::lifetime::Verdict::KeepAlive { .. }
            ),
            "a pre-warmed bridge is held for the input it was warmed for"
        );
    }

    /// And it is a hold, not a lease forever: when the idle window passes with
    /// no further interaction and nothing else wants the bridge, the timer the
    /// interaction armed fires, finds no door holding, and the bridge goes.
    #[tokio::test]
    async fn the_bus_hold_expires_and_the_bridge_drains() {
        tokio::time::pause();
        let (bridge, messenger, _db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        bridge
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Stop { correlation: None }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;
        assert!(
            bridge.active_bridges.get(conversation_id).await.is_some(),
            "the bridge outlives the exchange itself"
        );

        // Let the timer the interaction armed reach its first poll, so the paused
        // clock has a sleep to expire.
        tokio::task::yield_now().await;

        // Past the configured window with nothing more from the bus.
        let window = Duration::from_secs(messenger.llm_chat().idle_timeout_secs);
        tokio::time::advance(window + Duration::from_secs(1)).await;
        let mut rounds = 0u32;
        while bridge.active_bridges.get(conversation_id).await.is_some() {
            assert!(
                rounds < 100,
                "the expired bus hold did not drain the bridge"
            );
            tokio::task::yield_now().await;
            rounds += 1;
        }
        assert!(
            bridge.drain_on_idle.load(Ordering::SeqCst),
            "the drain the expired hold decided on"
        );
    }

    /// Keep-alive is the OR over the doors: with a peer holding the
    /// conversation, the browser leaving decides nothing — even for an
    /// ephemeral app whose browser door carries no grace.
    #[tokio::test]
    async fn a_detaching_browser_leaves_a_bus_held_bridge_alive() {
        tokio::time::pause();
        let (bridge, messenger, _db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        bridge
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;

        // An ephemeral app (no `idle_timeout`): the browser door has no grace of
        // its own, so nothing but the bus can hold this bridge after the detach.
        bridge.add_subscriber(bridge.user_id, "bob").await;
        bridge.note_bus_activity().await;
        bridge.remove_subscriber(bridge.user_id).await;

        assert!(
            bridge.active_bridges.get(conversation_id).await.is_some(),
            "the peer's hold outlives the tab that closed"
        );
        assert!(
            !bridge.drain_on_idle.load(Ordering::SeqCst),
            "a bridge another door still wants is not condemned"
        );

        // And when that hold expires too, the bridge goes.
        tokio::task::yield_now().await;
        tokio::time::advance(
            Duration::from_secs(messenger.llm_chat().idle_timeout_secs) + Duration::from_secs(1),
        )
        .await;
        let mut rounds = 0u32;
        while bridge.active_bridges.get(conversation_id).await.is_some() {
            assert!(rounds < 100, "the last hold expired but nothing drained");
            tokio::task::yield_now().await;
            rounds += 1;
        }
    }

    /// A stop is not a bookkeeping ack: the harness is interrupted, and the
    /// state it lands in is broadcast like any other transition and reaches the
    /// record through the adapter.
    #[tokio::test]
    async fn a_stop_interrupts_the_harness_and_the_new_state_lands_on_the_record() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let mut cc_rx =
            crate::active_bridge::test_support::install_recording_session(&bridge).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Stop {
                correlation: Some("s-1".to_string()),
            }),
        )
        .await;

        // Published before the spawn, so the adapter's own startup drain is what
        // executes it — the path a peer stopping a conversation takes.
        let (tx, rx) = broadcast::channel(8);
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));

        let outgoing = tokio::time::timeout(Duration::from_secs(5), cc_rx.recv())
            .await
            .expect("the interrupt reaches the harness")
            .expect("the recording session is still open");
        assert!(
            matches!(
                outgoing.msg,
                brenn_cc::protocol::CcOutgoing::ControlRequest {
                    request: brenn_cc::protocol::BrennControlRequest::Interrupt {},
                    ..
                }
            ),
            "a stop is an interrupt control request, got {:?}",
            outgoing.msg
        );

        tx.send(WsServerMessage::Status {
            state: ws::CcState::Idle,
        })
        .unwrap();
        bridge.stop_chat_adapter();
        tokio::time::timeout(Duration::from_secs(5), adapter)
            .await
            .expect("the adapter ends on the teardown signal")
            .expect("and ends without panicking");

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert_eq!(
            events,
            vec![
                ChatEvent::Models { available: vec![] },
                ChatEvent::Ack {
                    command: "stop".to_string(),
                    correlation: Some("s-1".to_string()),
                },
                ChatEvent::Status {
                    state: chat::CcState::Idle,
                },
            ],
            "the peer hears its ack and then the state the interrupt produced",
        );
    }

    /// Both doors feed one conversation, and the record is the shared one: a
    /// send that came over the bus and a send that came from a browser both
    /// appear, each attributed to the door it arrived through, and the bus send
    /// appears exactly once despite also being echoed to the browsers.
    #[tokio::test]
    async fn the_record_holds_both_doors_sends_once_each() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "from the bus".to_string(),
                model: None,
                attachments: vec![],
                correlation: Some("c-1".to_string()),
            }),
        )
        .await;

        // The bridge's own broadcast, as production wires it — so the echo the
        // accepted bus send emits travels the real path to the translation that
        // has to drop it.
        let adapter = tokio::spawn(run_bus_chat_adapter(
            bridge.clone(),
            bridge.event_tx.subscribe(),
            vec![],
        ));

        // What the legacy websocket door broadcasts for a message typed into a
        // browser: no bus sender, so this one is the outbound leg's to record.
        bridge
            .event_tx
            .send(WsServerMessage::UserMessageEcho {
                text: "from a browser".to_string(),
                username: "bob".to_string(),
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                attachments: vec![],
                selected_tasks: vec![],
                seq: Some(2),
                bus_sender: None,
            })
            .unwrap();
        bridge.stop_chat_adapter();
        tokio::time::timeout(Duration::from_secs(5), adapter)
            .await
            .expect("the adapter ends on the teardown signal")
            .expect("and ends without panicking");

        let sends: Vec<_> = record_bodies(&messenger, &db, conversation_id)
            .await
            .into_iter()
            .filter_map(|e| match e {
                ChatEvent::UserMessage {
                    text,
                    sender,
                    correlation,
                    ..
                } => Some((text, sender, correlation)),
                _ => None,
            })
            .collect();
        assert_eq!(
            sends,
            vec![
                (
                    "from the bus".to_string(),
                    format!("conversation:{conversation_id}"),
                    Some("c-1".to_string()),
                ),
                (
                    "from a browser".to_string(),
                    "legacy-ws:bob".to_string(),
                    None,
                ),
            ],
            "one entry per accepted input, each attributed to its own door",
        );
        assert_eq!(
            user_rows(&db, conversation_id).await.len(),
            1,
            "only the bus send is persisted here; the browser's row is the \
             legacy door's own, and this echo stands in for it",
        );
    }

    /// The adapter's only other exit is every broadcast sender being dropped —
    /// which cannot happen while it holds an `Arc` on the bridge that owns one.
    /// Teardown ends it explicitly, and what the bridge broadcast before the
    /// teardown still reaches the record on the way out.
    #[tokio::test]
    async fn a_torn_down_bridge_stops_its_adapter_after_flushing_the_broadcast() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let (tx, rx) = broadcast::channel(8);
        let adapter = tokio::spawn(run_bus_chat_adapter(bridge.clone(), rx, vec![]));

        tx.send(WsServerMessage::AssistantMessage {
            content: "<p>last words</p>".to_string(),
            seq: Some(1),
            text: Some("last words".to_string()),
        })
        .unwrap();
        bridge.stop_chat_adapter();

        // The sender is still alive, so nothing but the stop signal can end this.
        tokio::time::timeout(Duration::from_secs(5), adapter)
            .await
            .expect("the adapter ends on the teardown signal")
            .expect("and ends without panicking");

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            events.iter().any(
                |e| matches!(e, ChatEvent::AssistantMessage { text, .. } if text == "last words")
            ),
            "what was broadcast before the teardown belongs on the record: {events:?}",
        );
    }

    /// The flush shares the loop's gap accounting. A teardown that swallowed a
    /// dropped span would leave the record's *last* hole unmarked — the one
    /// moment a consumer has no later event to learn from.
    #[tokio::test]
    async fn a_gap_found_on_the_way_out_is_still_a_gap_marker() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);

        // Overrun the buffer with nothing reading it, so the flush's first
        // `try_recv` reports the drop rather than a message.
        let (tx, mut rx) = broadcast::channel(4);
        for i in 0..10 {
            tx.send(WsServerMessage::StreamToken {
                token: i.to_string(),
            })
            .unwrap();
        }

        flush_broadcast(
            &bridge,
            &bus,
            &mut rx,
            &mut TurnIds::default(),
            &mut Vec::new(),
        )
        .await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                ChatEvent::Error { message, correlation: None } if message.contains("gap")
            )),
            "a span dropped during the flush must be visible in the record: {events:?}",
        );
    }

    /// Poll until the bridge's adapter task has ended.
    ///
    /// A never-installed handle reports "not finished" forever, so this also
    /// fails a teardown that runs no adapter at all.
    async fn await_adapter_finished(bridge: &ActiveBridge) {
        let mut rounds = 0u32;
        while !bridge.chat_adapter_finished() {
            assert!(
                rounds < 100,
                "the teardown never told the adapter to stop, so it holds the \
                 bridge graph for the life of the process"
            );
            tokio::task::yield_now().await;
            rounds += 1;
        }
    }

    /// A `JoinHandle` for a task that has already ended — the deterministic
    /// dead-event-loop wedge.
    async fn finished_handle() -> tokio::task::JoinHandle<()> {
        let handle = tokio::spawn(async {});
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        handle
    }

    /// The signal is only half the fix: every deregistration funnels through
    /// `kill_session`, and an adapter it does not stop keeps its task, its
    /// broadcast buffer and the whole bridge graph alive for the rest of the
    /// process — once per bridge ever killed.
    #[tokio::test]
    async fn killing_a_session_stops_the_adapter() {
        let (bridge, _messenger, _db) = chat_bridge().await;
        spawn_bus_chat_adapter(bridge.clone(), bridge.event_tx.subscribe(), vec![]);
        bridge
            .active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        bridge.kill_session(&bridge.active_bridges).await;

        await_adapter_finished(&bridge).await;
    }

    /// The other teardown path, and the worse one to leak on: the watchdog
    /// deregisters a wedged bridge on the failure path, where nobody is looking.
    #[tokio::test]
    async fn a_watchdog_reap_stops_the_adapter() {
        let (bridge, _messenger, _db) = chat_bridge().await;
        spawn_bus_chat_adapter(bridge.clone(), bridge.event_tx.subscribe(), vec![]);
        bridge
            .active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        bridge.install_event_loop_handle(finished_handle().await);

        let mut watchdog = super::super::watchdog::Watchdog::new(
            brenn_lib::config::WatchdogConfig::default(),
            bridge.active_bridges.clone(),
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        );
        watchdog.sweep().await;

        assert!(
            bridge
                .active_bridges
                .get(bridge.conversation_id)
                .await
                .is_none(),
            "the sweep reaped the wedged bridge",
        );
        await_adapter_finished(&bridge).await;
    }

    /// The hold a peer's own wake takes is on the bridge it bought, not on what
    /// that bridge's drains happen to find. A start-up drain can legitimately
    /// come up empty — a predecessor's last pass advanced the cursor, or the
    /// position is gone — and a spawned bridge that stamps nothing is held by no
    /// door and re-asked about by no timer: CC parked forever.
    #[tokio::test]
    async fn a_chat_wake_holds_the_bridge_it_bought() {
        let (bridge, _messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let state = crate::state::AppState::for_test(db, None);
        state
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;

        crate::state::run_wake_attempt(
            state.clone(),
            conversation_id,
            chrono_tz::Tz::UTC,
            crate::state::BusHold::Unheld,
        )
        .await;
        assert_eq!(
            bridge.lifetime.verdict(0),
            crate::active_bridge::lifetime::Verdict::Drain,
            "an app subscriber's wake decides nothing about the bridge's lifetime",
        );

        crate::state::run_wake_attempt(
            state,
            conversation_id,
            chrono_tz::Tz::UTC,
            crate::state::BusHold::Held,
        )
        .await;
        assert!(
            matches!(
                bridge.lifetime.verdict(0),
                crate::active_bridge::lifetime::Verdict::KeepAlive { .. }
            ),
            "the peer that asked for this conversation by name is holding it",
        );
    }

    /// A bridge two doors hold dies at the *last* expiry, not the first — which
    /// only works if each timer generation arms the next. The fired timer has to
    /// drop its own handle before re-asking, or the re-ask's fresh timer aborts
    /// the task doing the asking.
    #[tokio::test]
    async fn a_timer_generation_that_finds_another_hold_arms_the_next() {
        tokio::time::pause();
        // A browser grace far longer than the bus window, so the two holds
        // expire in a known order and the first expiry cannot be the last.
        let (bridge, messenger, _db) = chat_bridge_persistent(Duration::from_secs(1200)).await;
        let conversation_id = bridge.conversation_id;
        let bus_window = Duration::from_secs(messenger.llm_chat().idle_timeout_secs);
        bridge
            .active_bridges
            .insert(conversation_id, bridge.clone())
            .await;

        // Both doors holding, both timed: a peer spoke, and the last tab closed.
        bridge.add_subscriber(bridge.user_id, "bob").await;
        bridge.note_bus_activity().await;
        bridge.remove_subscriber(bridge.user_id).await;
        let first = timer_generation(&bridge).expect("two timed holds arm a timer");

        // Past the bus window: the shorter hold is gone, the browser's grace is
        // not, and the generation that just fired owes the next one.
        tokio::task::yield_now().await;
        tokio::time::advance(bus_window + Duration::from_secs(1)).await;
        let mut rounds = 0u32;
        while timer_generation(&bridge).is_none_or(|current| current == first) {
            assert!(
                rounds < 100,
                "the expired bus hold left the surviving browser hold with no timer of \
                 its own, so nothing will ever re-ask: the bridge lives forever"
            );
            tokio::task::yield_now().await;
            rounds += 1;
        }
        assert!(
            bridge.active_bridges.get(conversation_id).await.is_some(),
            "one door letting go is not the last one letting go",
        );

        // And past the browser's grace, the last hold goes and the bridge with it.
        tokio::time::advance(Duration::from_secs(1200)).await;
        let mut rounds = 0u32;
        while bridge.active_bridges.get(conversation_id).await.is_some() {
            assert!(rounds < 100, "the last hold expired but nothing drained");
            tokio::task::yield_now().await;
            rounds += 1;
        }
    }

    /// Which timer generation is armed, by task identity — so "a second one was
    /// armed" is distinguishable from "the first one is still stored".
    fn timer_generation(bridge: &ActiveBridge) -> Option<tokio::task::Id> {
        bridge
            .lifetime_timer
            .lock()
            .expect("lifetime_timer lock poisoned")
            .as_ref()
            .map(tokio::task::JoinHandle::id)
    }

    // -------------------------------------------------------------------
    // The impetus pool at the bus door
    // -------------------------------------------------------------------

    /// The ceiling `chat_bridge` resolves for its app.
    const POOL_CEILING: u32 = 100;

    /// What the conversation's pool holds, or `None` where nothing has touched
    /// it.
    async fn pool(db: &Db, conversation_id: i64) -> Option<u32> {
        let conn = db.lock().await;
        brenn_lib::messaging::db::read_send_budget(&conn, conversation_id)
    }

    /// Put the pool at a known level — an exhausted conversation, or one with
    /// exactly enough left to be worth counting.
    async fn set_pool(db: &Db, conversation_id: i64, remaining: u32) {
        let conn = db.lock().await;
        brenn_lib::messaging::db::reset_send_budget(&conn, conversation_id, remaining);
    }

    /// [`publish_command`] for a command carrying user-interaction authority.
    ///
    /// Stamps the impetus column directly rather than going through the publish
    /// gate, which owns the minting check in production.
    async fn publish_command_with_impetus(
        messenger: &Messenger,
        db: &Db,
        conversation_id: i64,
        body: &str,
    ) {
        publish_command(messenger, conversation_id, body).await;
        let conn = db.lock().await;
        let stamped = conn
            .execute(
                "UPDATE messaging_messages SET impetus = 'replenish' WHERE body = ?1",
                rusqlite::params![body],
            )
            .expect("stamp impetus on the command row");
        assert_eq!(stamped, 1, "exactly the command row carries the impetus");
    }

    /// Every correlated error on the record, in publish order.
    fn correlated_errors(events: &[ChatEvent]) -> Vec<(String, String)> {
        events
            .iter()
            .filter_map(|e| match e {
                ChatEvent::Error {
                    message,
                    correlation: Some(c),
                } => Some((c.clone(), message.clone())),
                _ => None,
            })
            .collect()
    }

    /// An accepted send is a real CC turn, and a real CC turn costs a unit.
    #[tokio::test]
    async fn an_accepted_send_draws_one_unit() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 10).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "how much did I spend".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert_eq!(pool(&db, conversation_id).await, Some(9));
        assert_eq!(user_rows(&db, conversation_id).await.len(), 1);
    }

    /// A command whose envelope carries impetus restores the pool before the
    /// turn it pays for.
    #[tokio::test]
    async fn a_send_carrying_impetus_refills_the_pool_and_then_draws() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 0).await;

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "a person is typing this".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert_eq!(
            pool(&db, conversation_id).await,
            Some(POOL_CEILING - 1),
            "an exhausted conversation is revived by carried impetus, not by the door"
        );
        assert_eq!(
            user_rows(&db, conversation_id).await.len(),
            1,
            "and the send goes through"
        );
    }

    /// An exhausted pool refuses the send whole: no persisted turn, no
    /// `user_message` on the record, and a correlated error the peer can read.
    #[tokio::test]
    async fn an_impetus_free_send_at_zero_is_refused_whole() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 0).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "one more, unattended".to_string(),
                model: None,
                attachments: vec![],
                correlation: Some("x-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert!(
            user_rows(&db, conversation_id).await.is_empty(),
            "a refused send enqueues no CC work and persists no row"
        );
        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ChatEvent::UserMessage { .. })),
            "and puts no user_message on the record: {events:?}"
        );
        let errors = correlated_errors(&events);
        assert_eq!(errors.len(), 1, "one correlated refusal: {events:?}");
        assert_eq!(errors[0].0, "x-1");
        assert!(
            errors[0].1.contains("allowance"),
            "the refusal names the condition and the remedy: {}",
            errors[0].1
        );
        assert_eq!(
            pool(&db, conversation_id).await,
            Some(0),
            "a refusal draws nothing"
        );
    }

    /// The verbs that provoke no CC turn are outside the pool entirely: `stop`
    /// in particular must always work, and impetus on it redeems nothing.
    #[tokio::test]
    async fn stop_and_set_model_neither_redeem_nor_draw() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 0).await;

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Stop {
                correlation: Some("s-1".to_string()),
            }),
        )
        .await;
        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::SetModel {
                model: "sonnet".to_string(),
                correlation: Some("m-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            events.iter().any(
                |e| matches!(e, ChatEvent::Ack { command, correlation: Some(c) }
                    if command == "stop" && c == "s-1")
            ),
            "stop acks on an exhausted conversation: {events:?}"
        );
        assert!(
            events.iter().any(
                |e| matches!(e, ChatEvent::ModelChanged { correlation: Some(c), .. }
                    if c == "m-1")
            ),
            "set_model still lands: {events:?}"
        );
        assert_eq!(
            pool(&db, conversation_id).await,
            Some(0),
            "neither verb redeems impetus, because neither provokes a turn"
        );
    }

    /// A compaction is a CC turn: it draws, and an exhausted pool refuses it.
    #[tokio::test]
    async fn compact_draws_one_unit_and_is_refused_at_zero() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 1).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Compact {
                correlation: Some("c-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;
        assert_eq!(
            pool(&db, conversation_id).await,
            Some(0),
            "the compaction turn costs a unit"
        );

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Compact {
                correlation: Some("c-2".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        let errors = correlated_errors(&events);
        assert_eq!(
            errors.len(),
            1,
            "the second compaction is refused: {events:?}"
        );
        assert_eq!(errors[0].0, "c-2");
        assert!(errors[0].1.contains("allowance"));
    }

    /// A send refused before acceptance is outside the pool on both sides: its
    /// impetus redeems nothing and it draws nothing.
    #[tokio::test]
    async fn a_send_refused_for_its_attachments_neither_redeems_nor_draws() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 5).await;

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "look at this".to_string(),
                model: None,
                attachments: vec![chat::AttachmentRef {
                    upload_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                }],
                correlation: Some("a-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert_eq!(
            pool(&db, conversation_id).await,
            Some(5),
            "the command never reached acceptance, so the pool never moved"
        );
    }

    /// Impetus does not propagate: the record of an attended send carries none,
    /// so a subscriber on `.out` cannot be re-armed by someone else's attention.
    #[tokio::test]
    async fn the_record_of_an_impetus_bearing_send_carries_none() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "attended".to_string(),
                model: None,
                attachments: vec![],
                correlation: None,
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let address = chat_address(
            &messenger.llm_chat().prefix,
            APP,
            ChatLeaf::Out,
            conversation_id,
        );
        let uuid = messenger
            .directory()
            .resolve(&address)
            .expect("the record channel is provisioned")
            .uuid;
        let conn = db.lock().await;
        let with_impetus: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_messages \
                 WHERE channel_uuid = ?1 AND impetus IS NOT NULL",
                rusqlite::params![uuid.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .expect("count impetus-bearing record rows");
        assert_eq!(
            with_impetus, 0,
            "nothing the machinery republishes carries attention outward"
        );
    }

    /// The pool pays for turns the harness got, not for attempts at a dying
    /// bridge: a send whose handoff fails costs nothing, so a conversation's
    /// runway of real turns cannot be spent on messages that never arrived.
    #[tokio::test]
    async fn a_send_whose_handoff_fails_draws_nothing() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        // No session installed: the harness is not there to take the text.
        set_pool(&db, conversation_id, 5).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "against a dead bridge".to_string(),
                model: None,
                attachments: vec![],
                correlation: Some("d-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert_eq!(
            pool(&db, conversation_id).await,
            Some(5),
            "an attempt that never reached the harness is not a turn"
        );
        let events = record_bodies(&messenger, &db, conversation_id).await;
        let errors = correlated_errors(&events);
        assert_eq!(errors.len(), 1, "the peer hears about it: {events:?}");
        assert_eq!(errors[0].0, "d-1");
        assert!(
            errors[0].1.contains("did not reach"),
            "and hears which failure it was: {}",
            errors[0].1
        );
    }

    /// The same ordering on the other turn-provoking verb.
    #[tokio::test]
    async fn a_compaction_whose_handoff_fails_draws_nothing() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        set_pool(&db, conversation_id, 5).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Compact {
                correlation: Some("d-2".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        assert_eq!(
            pool(&db, conversation_id).await,
            Some(5),
            "a compaction request that never reached the harness is not a turn"
        );
        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ChatEvent::Ack { command, .. } if command == "compact")),
            "and it is not acked: {events:?}"
        );
    }

    /// Refused whole means whole: a `send` carrying a model that the pool
    /// cannot pay for leaves the conversation's model where it was, so a peer
    /// cannot move it by sending into an exhausted conversation.
    #[tokio::test]
    async fn a_send_refused_at_zero_does_not_change_the_model() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 0).await;

        publish_command(
            &messenger,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "and switch to opus while you are at it".to_string(),
                model: Some("sonnet".to_string()),
                attachments: vec![],
                correlation: Some("sm-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ChatEvent::ModelChanged { .. })),
            "the model half of a refused send does not take effect: {events:?}"
        );
        assert!(
            bridge.last_set_model.lock().await.is_none(),
            "and the harness was never asked for it"
        );
        assert!(
            user_rows(&db, conversation_id).await.is_empty(),
            "nor the text half"
        );
        let errors = correlated_errors(&events);
        assert_eq!(errors.len(), 1, "one correlated refusal: {events:?}");
        assert!(errors[0].1.contains("allowance"));
    }

    /// An unknown alias refuses the send before it is accepted, so the pool is
    /// untouched on both sides — nothing redeemed, nothing drawn.
    #[tokio::test]
    async fn a_send_naming_an_unknown_model_neither_redeems_nor_draws() {
        let (bridge, messenger, db) = chat_bridge().await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 4).await;

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "hello".to_string(),
                model: Some("no-such-model".to_string()),
                attachments: vec![],
                correlation: Some("um-1".to_string()),
            }),
        )
        .await;
        drain_commands(
            &bridge,
            &bus,
            &commands,
            &[ModelInfo {
                value: "sonnet".to_string(),
                display_name: "Sonnet".to_string(),
                description: "everyday".to_string(),
            }],
        )
        .await;

        assert_eq!(
            pool(&db, conversation_id).await,
            Some(4),
            "the command never reached acceptance, so the pool never moved"
        );
        assert!(
            user_rows(&db, conversation_id).await.is_empty(),
            "and the text never reached the harness"
        );
    }

    /// The reviving command is admitted on the pool it restored, before the
    /// backlog that refill releases spends it. At a ceiling of one the ordering
    /// is the whole difference between a conversation a person can drive over
    /// the bus and one whose every attended send is refused by the batch it
    /// just released.
    #[tokio::test]
    async fn an_attended_send_is_not_refused_by_the_backlog_it_revives() {
        let (bridge, messenger, db) = chat_bridge_with_ambience(1).await;
        let conversation_id = bridge.conversation_id;
        let bus = ChatBus::new(messenger.clone(), &bridge);
        let commands = command_cursor(&bridge, &bus).await;
        let mut broadcast_rx = bridge.event_tx.subscribe();
        let _cc_rx = crate::active_bridge::test_support::install_recording_session(&bridge).await;
        set_pool(&db, conversation_id, 0).await;

        publish_ambience(&db, "held-until-someone-is-here").await;
        crate::active_bridge::deliver_conversation_backlog(&bridge)
            .await
            .expect("an exhausted conversation holds its batch, it does not fail");
        assert_eq!(
            pool(&db, conversation_id).await,
            Some(0),
            "the batch is held, not delivered"
        );
        let _ = crate::active_bridge::test_support::drain_broadcast(&mut broadcast_rx);

        publish_command_with_impetus(
            &messenger,
            &db,
            conversation_id,
            &chat::encode(&ChatCommand::Send {
                text: "a person, typing".to_string(),
                model: None,
                attachments: vec![],
                correlation: Some("r-1".to_string()),
            }),
        )
        .await;
        drain_commands(&bridge, &bus, &commands, &[]).await;

        let events = record_bodies(&messenger, &db, conversation_id).await;
        assert!(
            correlated_errors(&events).is_empty(),
            "the attended send is not refused: {events:?}"
        );
        let rows = user_rows(&db, conversation_id).await;
        assert!(
            rows.iter().any(|r| r.contains("a person, typing")),
            "it reaches the harness: {rows:?}"
        );
        let delivered: Vec<String> =
            crate::active_bridge::test_support::drain_broadcast(&mut broadcast_rx)
                .iter()
                .filter_map(|m| match m {
                    WsServerMessage::SystemMessageBroadcast { rendered_html, .. } => {
                        Some(rendered_html.clone())
                    }
                    _ => None,
                })
                .collect();
        assert_eq!(delivered.len(), 1, "and the held batch rides the same turn");
        assert!(delivered[0].contains("held-until-someone-is-here"));
    }

    // -------------------------------------------------------------------
    // The machinery cycle, and why it stops
    // -------------------------------------------------------------------

    /// One side of a mutual machinery cycle: a conversation, its bus, and the
    /// broadcast its own injections come out on.
    struct CyclePeer {
        bridge: Arc<ActiveBridge>,
        bus: ChatBus,
        broadcast: broadcast::Receiver<WsServerMessage>,
        turns: TurnIds,
        _cc: tokio::sync::mpsc::Receiver<brenn_cc::session::OutgoingEnvelope>,
    }

    /// One turn of the crank on one side: deliver whatever the peer's record is
    /// holding for this conversation, then put what that injection made the
    /// conversation say back on its own record — the republish leg, through the
    /// production translation and the production publish. Answers how many CC
    /// turns it provoked.
    async fn crank(peer: &mut CyclePeer) -> usize {
        crate::active_bridge::deliver_conversation_backlog(&peer.bridge)
            .await
            .expect("a batch is delivered or held, never failed");
        let mut turns_provoked = 0;
        for msg in crate::active_bridge::test_support::drain_broadcast(&mut peer.broadcast) {
            if matches!(msg, WsServerMessage::SystemMessageBroadcast { .. }) {
                turns_provoked += 1;
            }
            for outbound in translate(msg, &mut peer.turns) {
                publish(&peer.bridge, &peer.bus, &outbound).await;
            }
        }
        turns_provoked
    }

    /// Two apps, one conversation each, each subscribed to the other's record —
    /// the operator-authored topology behind the N≥2 machinery cycle. Both pools
    /// start at `ceiling`.
    async fn cross_subscribed_pair(ceiling: u32) -> (CyclePeer, CyclePeer, Db) {
        const A: &str = "chat-a";
        const B: &str = "chat-b";

        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_a, conv_b) = {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (username, password_hash, created_at) \
                 VALUES ('bob', 'x', '2026-01-01')",
                [],
            )
            .unwrap();
            let uid = conn.last_insert_rowid();
            let a = brenn_lib::conversation::create_conversation(&conn, uid, A, false);
            let b = brenn_lib::conversation::create_conversation(&conn, uid, B, false);
            (uid, a, b)
        };

        let chat = LlmChatConfig::default();
        let out_of = |slug: &str, id: i64| chat_address(&chat.prefix, slug, ChatLeaf::Out, id);
        let a_out = out_of(A, conv_a);
        let b_out = out_of(B, conv_b);

        // Each app reads the other's record and nothing else. The authority to
        // publish its own record stays where it belongs, on its harness policy.
        let peer_app = |slug: &str, peer_record: &str| {
            let mut app = crate::test_support::app_config::default_test_app_config(slug, slug);
            app.singleton = true;
            app.allowed_users = vec!["bob".to_string()];
            app.messaging = Some(brenn_lib::messaging::ResolvedMessagingConfig {
                send_budget: ceiling,
                subscriptions: vec![],
            });
            app.policy = brenn_lib::access::AppPolicy::default();
            app.policy
                .grants
                .insert(brenn_lib::access::AppCapability::MessagingSubscribe);
            app.policy
                .acls
                .brenn_subscribe
                .push(brenn_lib::access::acl::ChannelMatcher::Prefix(
                    peer_record
                        .strip_prefix(brenn_lib::messaging::ChannelScheme::Brenn.prefix())
                        .expect("a record address is a brenn: address")
                        .to_string(),
                ));
            app.chat_harness_policy = chat.harness_policy(slug);
            app
        };
        let mut apps = indexmap::IndexMap::new();
        apps.insert(A.to_string(), peer_app(A, &b_out));
        apps.insert(B.to_string(), peer_app(B, &a_out));

        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(NoopWakeRouter) as Arc<dyn brenn_lib::messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(RingStores::empty()));
        {
            let conn = db.lock().await;
            messenger.provision_conversation_chat_channels(&conn, A, conv_a);
            messenger.provision_conversation_chat_channels(&conn, B, conv_b);
        }

        // The cross-subscriptions themselves, each with a position primed at the
        // head of the peer's record.
        for (slug, address) in [(A, &b_out), (B, &a_out)] {
            messenger
                .subscribe_dynamic(
                    slug,
                    address,
                    brenn_lib::messaging::subscribe::DynamicSubscribeParams {
                        push_depth: brenn_lib::messaging::config::Depth::Bounded(8),
                        retain_depth: brenn_lib::messaging::config::Depth::Bounded(0),
                        noise: None,
                        wake_min: None,
                        qos: None,
                    },
                )
                .await
                .expect("the operator's cross-subscription lands");
        }

        let mut peers = Vec::new();
        for (slug, conversation_id) in [(A, conv_a), (B, conv_b)] {
            let (tx, _rx) = broadcast::channel(64);
            let bridge = ActiveBridge::inject_for_test_full(
                user_id,
                conversation_id,
                slug,
                db.clone(),
                tx,
                brenn_lib::obs::alerting::noop_alert_dispatcher().0,
                crate::active_bridge::test_fixtures::TestBridgeConfig {
                    messenger: Some(messenger.clone()),
                    send_budget: ceiling,
                    ..Default::default()
                },
            );
            let broadcast = bridge.event_tx.subscribe();
            let cc = crate::active_bridge::test_support::install_recording_session(&bridge).await;
            let bus = ChatBus::new(messenger.clone(), &bridge);
            peers.push(CyclePeer {
                bridge,
                bus,
                broadcast,
                turns: TurnIds::default(),
                _cc: cc,
            });
        }
        let b_peer = peers.pop().expect("two peers");
        let a_peer = peers.pop().expect("two peers");
        (a_peer, b_peer, db)
    }

    /// The composite property the design rests on: a cycle whose every leg is
    /// machinery — ambience injection, republish, ambience injection — halts,
    /// because each injection draws a unit and no leg carries impetus forward.
    /// Nothing in it is an LLM decision, so nothing but the pools can stop it.
    #[tokio::test]
    async fn two_cross_subscribed_conversations_halt_within_their_pools() {
        const CEILING: u32 = 2;
        let (mut a, mut b, db) = cross_subscribed_pair(CEILING).await;
        let (conv_a, conv_b) = (a.bridge.conversation_id, b.bridge.conversation_id);

        // The one thing that is not machinery: something said something once.
        publish(
            &a.bridge,
            &a.bus,
            &Outbound::Record(ChatEvent::SystemMessage {
                text: "the cycle starts here".to_string(),
                category: chat::SystemMessageCategory::MessagesReceived,
            }),
        )
        .await;

        let mut turns = 0;
        let mut rounds = 0;
        loop {
            let this_round = crank(&mut a).await + crank(&mut b).await;
            if this_round == 0 {
                break;
            }
            turns += this_round;
            rounds += 1;
            assert!(
                rounds < 20,
                "the cycle is still provoking turns after {rounds} rounds — it does not halt"
            );
        }

        assert_eq!(
            turns,
            (CEILING * 2) as usize,
            "the cascade runs exactly as long as the two pools can pay for it"
        );
        assert_eq!(pool(&db, conv_a).await, Some(0), "both pools are spent");
        assert_eq!(pool(&db, conv_b).await, Some(0), "both pools are spent");

        // And it stays halted: the records still hold what the cycle published,
        // so the batches are still owed — they are simply unaffordable.
        assert_eq!(
            crank(&mut a).await + crank(&mut b).await,
            0,
            "an exhausted pair provokes nothing further"
        );
    }
}
