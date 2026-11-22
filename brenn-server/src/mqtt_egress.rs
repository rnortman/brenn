//! The bus side of the MQTT egress readers: each client's position on its
//! egress channel, and the one sink for what became of the messages handed
//! from it.
//!
//! One identity, `system:mqtt-outbound`, holds one position per egress
//! channel. [`MqttEgressHub::open`] gives a client's reader its view of that
//! position as an [`EgressBus`]; the reader reads, advances and reports
//! through it and never touches the bus directly.
//!
//! Every report but `Delivered` and `Drained` is logged, counted and raised to
//! the operator. Repeats of a kind while its episode is open are logged and
//! counted but withheld from the alert path; the episode closes, with one
//! summary alert for what it withheld, when the client next delivers or runs
//! dry. A message the channel's window drops before the reader reached it is
//! not a report: the bus counts and raises it under the participant's `Alarm`
//! rung.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::watch;

use brenn_common::sanitize_untrusted_str;
use brenn_envelope::{MessageEnvelope, TransportMeta};
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{ParticipantId, mqtt_egress_address};
use brenn_messaging::Messenger;
use brenn_messaging::mqtt_outbound::{MQTT_OUTBOUND_COMPONENT, MQTT_OUTBOUND_NOISE};
use brenn_messaging_store::blob::BlobError;
use brenn_messaging_store::store::{Attached, MessageSeq};
use brenn_mqtt::reader::{
    DropCause, EgressBus, EgressItem, EgressMessage, EgressMeta, EgressReport, UnknownCause,
};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};

#[cfg(test)]
mod tests;

const HUB_POISONED: &str = "MQTT egress hub lock poisoned";

/// Byte cap on each piece of external text (sender, topic, broker reason) a
/// log line or alert carries.
const UNTRUSTED_TEXT_MAX: usize = 256;

/// What became of one client's handed messages since its channel was opened
/// in this process, plus the bus's overflow tally for its position.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EgressCounts {
    pub delivered: u64,
    pub rejected: u64,
    pub expired: u64,
    pub unknown: u64,
    /// Messages dropped unsent, by count, whatever the cause.
    pub dropped: u64,
    /// Messages the channel's window dropped before the reader reached them,
    /// as the bus counted them for the participant's position.
    pub overflow: u64,
}

/// The kinds of failure report, each with its own episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FailureKind {
    Rejected,
    Expired,
    Unknown,
    Dropped,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Unknown => "outcome unknown",
            Self::Dropped => "dropped",
        }
    }
}

/// One client's tallies and open episodes.
#[derive(Debug, Default)]
struct ClientState {
    counts: EgressCounts,
    /// The open episode of each kind, with how many messages it has withheld
    /// from the alert path.
    episodes: HashMap<FailureKind, u64>,
}

/// One alert the hub raises, built under the lock and sent after it.
struct Alert {
    title: String,
    body: String,
}

pub struct MqttEgressHub {
    messenger: Arc<Messenger>,
    alerts: AlertDispatcher,
    participant: ParticipantId,
    wake: watch::Sender<u64>,
    clients: Mutex<HashMap<String, ClientState>>,
}

impl MqttEgressHub {
    pub fn new(messenger: Arc<Messenger>, alerts: AlertDispatcher) -> Arc<Self> {
        Arc::new(Self {
            messenger,
            alerts,
            participant: ParticipantId::for_system(MQTT_OUTBOUND_COMPONENT),
            wake: watch::Sender::new(0),
            clients: Mutex::new(HashMap::new()),
        })
    }

    /// Wake every reader: each reads its own channel once, if it may hand
    /// off.
    pub fn wake_all(&self) {
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// Attach the participant's position on `client`'s egress channel and
    /// return the reader's view of it.
    ///
    /// An existing position resumes, so a restart or a successor session
    /// hands off exactly what was not handed. A position created here is
    /// moved past everything the channel retains: those messages were
    /// committed for an earlier client of this slug that was since removed,
    /// and are logged by count and not handed.
    ///
    /// # Panics
    ///
    /// If the channel is not in the directory.
    pub async fn open(self: &Arc<Self>, client: &str) -> Arc<dyn EgressBus> {
        let address = mqtt_egress_address(client);
        let push_depth = self.channel_depth(&address);
        let bus = Arc::new(ClientEgress {
            hub: Arc::clone(self),
            client: client.to_string(),
            address,
            push_depth,
        });
        let attached = self
            .messenger
            .attach_subscriber(
                &bus.address,
                MQTT_OUTBOUND_COMPONENT,
                &self.participant,
                push_depth,
            )
            .await;
        if matches!(attached, Attached::Created) {
            let skipped = bus.skip_unhanded().await;
            if skipped > 0 {
                tracing::warn!(
                    mqtt_client = client,
                    skipped,
                    "MQTT egress: a new position skipped messages its channel retained from \
                     before this client was added; they are not published"
                );
            }
        }
        self.clients
            .lock()
            .expect(HUB_POISONED)
            .entry(client.to_string())
            .or_default();
        bus
    }

    /// Detach the participant's position on `client`'s egress channel and
    /// forget the client's counts.
    ///
    /// # Panics
    ///
    /// If the channel is not in the directory.
    pub async fn close(&self, client: &str) {
        self.messenger
            .detach_subscriber(&mqtt_egress_address(client), &self.participant)
            .await;
        self.clients.lock().expect(HUB_POISONED).remove(client);
    }

    /// `client`'s tallies; zeros for a client never opened.
    ///
    /// # Panics
    ///
    /// If the client's egress channel is not in the directory.
    pub fn counts(&self, client: &str) -> EgressCounts {
        let mut counts = self
            .clients
            .lock()
            .expect(HUB_POISONED)
            .get(client)
            .map(|state| state.counts)
            .unwrap_or_default();
        counts.overflow = self
            .messenger
            .drop_counter(&mqtt_egress_address(client), &self.participant);
        counts
    }

    /// The participant's push depth on `address`: the channel's retain depth,
    /// as every system subscriber follows its channel's window.
    fn channel_depth(&self, address: &str) -> Depth {
        self.messenger
            .directory()
            .resolve(address)
            .unwrap_or_else(|| panic!("MQTT egress: channel {address:?} is not in the directory"))
            .resolved_channel
            .retain_depth
    }

    /// Log, count and, unless an open episode withholds it, alert `report`
    /// for `client`.
    fn record(&self, client: &str, report: EgressReport) {
        let mut alerts = Vec::new();
        {
            let mut clients = self.clients.lock().expect(HUB_POISONED);
            let state = clients.get_mut(client).unwrap_or_else(|| {
                panic!("MQTT egress: a report for client {client:?}, whose channel is not open")
            });
            match report {
                EgressReport::Delivered(message) => {
                    tracing::debug!(
                        mqtt_client = client,
                        message_id = %message.id,
                        "MQTT egress: delivered"
                    );
                    state.counts.delivered += 1;
                    close_episodes(client, state, &mut alerts);
                }
                EgressReport::Drained => close_episodes(client, state, &mut alerts),
                EgressReport::Rejected { message, reason } => {
                    let reason = sanitize_untrusted_str(&reason, UNTRUSTED_TEXT_MAX);
                    state.counts.rejected += 1;
                    let detail = format!("the broker rejected it: {reason}");
                    failure(
                        client,
                        state,
                        FailureKind::Rejected,
                        &message,
                        &detail,
                        &mut alerts,
                    );
                }
                EgressReport::Expired(message) => {
                    state.counts.expired += 1;
                    let detail = "its lifetime passed before it could be handed to the broker";
                    failure(
                        client,
                        state,
                        FailureKind::Expired,
                        &message,
                        detail,
                        &mut alerts,
                    );
                }
                EgressReport::Unknown { message, cause } => {
                    state.counts.unknown += 1;
                    let detail = match cause {
                        UnknownCause::ConnectionLost => {
                            "the connection ended before the broker answered; it is not resent"
                        }
                        UnknownCause::SessionStopped => {
                            "the client session stopped before the broker answered"
                        }
                    };
                    failure(
                        client,
                        state,
                        FailureKind::Unknown,
                        &message,
                        detail,
                        &mut alerts,
                    );
                }
                EgressReport::Dropped { cause, count } => {
                    state.counts.dropped += count;
                    dropped(client, state, cause, count, &mut alerts);
                }
            }
        }
        for alert in alerts {
            self.alerts
                .alert(AlertSeverity::Warning, alert.title, alert.body);
        }
    }
}

/// Log one failure report about `message`, and open its kind's episode with an
/// alert or count it as withheld.
fn failure(
    client: &str,
    state: &mut ClientState,
    kind: FailureKind,
    message: &EgressMeta,
    detail: &str,
    alerts: &mut Vec<Alert>,
) {
    let sender = sanitize_untrusted_str(&message.sender, UNTRUSTED_TEXT_MAX);
    let topic = sanitize_untrusted_str(&message.topic, UNTRUSTED_TEXT_MAX);
    tracing::warn!(
        mqtt_client = client,
        kind = kind.label(),
        message_id = %message.id,
        sender = %sender,
        topic = %topic,
        qos = message.qos,
        detail,
        "MQTT egress: message not delivered"
    );
    let body = format!(
        "MQTT client {client:?}: message {} from {sender} for topic {topic} (QoS {}) was not \
         delivered: {detail}.",
        message.id, message.qos
    );
    open_or_withhold(client, state, kind, 1, body, alerts);
}

/// Log a drop of `count` messages, and open the dropped episode with an alert
/// or count them as withheld.
fn dropped(
    client: &str,
    state: &mut ClientState,
    cause: DropCause,
    count: u64,
    alerts: &mut Vec<Alert>,
) {
    let detail = match cause {
        DropCause::SessionStopped => {
            "the client session stopped while they waited to be resubmitted"
        }
        DropCause::ClientRemoved => "the client was removed with them still on its channel",
    };
    tracing::warn!(
        mqtt_client = client,
        kind = FailureKind::Dropped.label(),
        count,
        detail,
        "MQTT egress: messages dropped unsent"
    );
    let body = format!("MQTT client {client:?}: {count} message(s) dropped unsent: {detail}.");
    open_or_withhold(client, state, FailureKind::Dropped, count, body, alerts);
}

fn open_or_withhold(
    client: &str,
    state: &mut ClientState,
    kind: FailureKind,
    messages: u64,
    body: String,
    alerts: &mut Vec<Alert>,
) {
    match state.episodes.get_mut(&kind) {
        Some(withheld) => *withheld += messages,
        None => {
            state.episodes.insert(kind, 0);
            alerts.push(Alert {
                title: format!("MQTT egress: {} ({client})", kind.label()),
                body,
            });
        }
    }
}

/// Close every open episode of `client`, raising one summary alert for each
/// that withheld anything.
fn close_episodes(client: &str, state: &mut ClientState, alerts: &mut Vec<Alert>) {
    for (kind, withheld) in state.episodes.drain() {
        if withheld == 0 {
            continue;
        }
        alerts.push(Alert {
            title: format!("MQTT egress: {} ({client}), summary", kind.label()),
            body: format!(
                "MQTT client {client:?}: {withheld} more message(s) {} after the first was \
                 reported; the client has since delivered or run dry.",
                kind.label()
            ),
        });
    }
}

/// One client's reader's view of its egress channel.
struct ClientEgress {
    hub: Arc<MqttEgressHub>,
    client: String,
    address: String,
    push_depth: Depth,
}

impl ClientEgress {
    /// Move the position past every retained message the head read would
    /// serve, and return how many that was. Anything below the first of them
    /// was outrun by the window and is the bus's to count.
    async fn skip_unhanded(&self) -> u64 {
        let window = self
            .hub
            .messenger
            .store_for_address(&self.address)
            .window(&self.hub.participant, self.push_depth, Depth::Bounded(0))
            .await
            .unwrap_or_else(|| self.no_position());
        let new = window.new_entries();
        let (Some((first, _)), Some((last, _))) = (new.first(), new.last()) else {
            return 0;
        };
        let count = new.len() as u64;
        self.advance_over(*last, *first).await;
        count
    }

    async fn advance_over(&self, through: MessageSeq, seen_floor: MessageSeq) {
        self.hub
            .messenger
            .advance_subscriber(
                &self.address,
                &self.hub.participant,
                through,
                seen_floor,
                MQTT_OUTBOUND_NOISE,
            )
            .await
            .unwrap_or_else(|| self.no_position());
    }

    fn no_position(&self) -> ! {
        panic!(
            "MQTT egress: system:{MQTT_OUTBOUND_COMPONENT} holds no position on {} — the reader \
             of client {:?} runs over a channel its hub never opened",
            self.address, self.client
        )
    }

    /// The reader's message for one retained entry: its transport and its
    /// payload, from the body or from its one attachment.
    async fn item(&self, seq: MessageSeq, envelope: &MessageEnvelope) -> EgressItem {
        let publish = match &envelope.transport {
            Some(TransportMeta::MqttPublish(publish)) => publish.clone(),
            other => panic!(
                "MQTT egress: message {} on the host-written channel {} carries {other:?}, not \
                 MQTT publish metadata",
                envelope.message_id, self.address
            ),
        };
        let payload = match envelope.attachments.len() {
            0 => Bytes::copy_from_slice(envelope.body.as_bytes()),
            1 => match self.attached_payload(envelope).await {
                Some(bytes) => Bytes::from(bytes),
                None => {
                    tracing::debug!(
                        mqtt_client = %self.client,
                        message_id = %envelope.message_id,
                        "MQTT egress: a message's payload blob left retention before it was read"
                    );
                    return EgressItem::PayloadGone {
                        seq,
                        id: envelope.message_id,
                    };
                }
            },
            n => panic!(
                "MQTT egress: message {} on {} carries {n} attachments; the admission commits at \
                 most one",
                envelope.message_id, self.address
            ),
        };
        EgressItem::Message(EgressMessage {
            seq,
            id: envelope.message_id,
            sender: envelope.sender.clone(),
            publish,
            payload,
        })
    }

    /// The whole of the one blob `envelope` carries, or `None` when nothing
    /// holds it any more. A blob store I/O error is a panic.
    async fn attached_payload(&self, envelope: &MessageEnvelope) -> Option<Vec<u8>> {
        let held = self
            .hub
            .messenger
            .hold_attachments(&envelope.attachments)
            .await
            .ok()?;
        let hold = held
            .holds()
            .first()
            .expect("one attachment is one hold")
            .clone();
        let len = usize::try_from(hold.reference().size).expect("a blob size fits in memory");
        let id = hold.id().clone();
        let read = tokio::task::spawn_blocking(move || hold.read(0, len))
            .await
            .expect("a blob read task does not panic or get cancelled");
        match read {
            Ok(bytes) => Some(bytes),
            Err(BlobError::Io(e)) => panic!("MQTT egress: reading blob {id}: {e}"),
            Err(e) => panic!("MQTT egress: a read of the whole of blob {id} ({len} bytes): {e}"),
        }
    }
}

#[async_trait]
impl EgressBus for ClientEgress {
    async fn unhanded(&self, limit: usize) -> Vec<EgressItem> {
        let head = self
            .hub
            .messenger
            .store_for_address(&self.address)
            .unseen_head(&self.hub.participant, self.push_depth, limit)
            .await
            .unwrap_or_else(|| self.no_position());
        let mut items = Vec::with_capacity(head.len());
        for (seq, envelope) in &head {
            items.push(self.item(*seq, envelope).await);
        }
        items
    }

    async fn advance(&self, seq: MessageSeq) {
        self.advance_over(seq, seq).await;
    }

    async fn abandon(&self) -> u64 {
        self.skip_unhanded().await
    }

    fn wakes(&self) -> watch::Receiver<u64> {
        self.hub.wake.subscribe()
    }

    fn window_depth(&self) -> Option<usize> {
        match self.push_depth {
            Depth::Bounded(n) => {
                Some(usize::try_from(n).expect("an egress window depth fits in usize"))
            }
            Depth::Unbounded => None,
        }
    }

    fn report(&self, report: EgressReport) {
        self.hub.record(&self.client, report);
    }
}
