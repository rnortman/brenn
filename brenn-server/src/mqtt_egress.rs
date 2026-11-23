//! The bus side of the MQTT egress readers: each client's position on its
//! egress channel, and the one sink for what became of the messages handed
//! from it.
//!
//! One identity, `system:mqtt-outbound`, holds one position per egress
//! channel. [`MqttEgressHub::open`] gives a client's reader its view of that
//! position as an [`EgressBus`]; the reader reads, advances and reports
//! through it and never touches the bus directly. [`MqttEgressHub::close`]
//! settles a removed client's channel: it gives up the position and reports
//! every message the channel held unread `dropped`, `removed`, whether or not
//! the client's reader was still running.
//!
//! Every report but `Delivered`, `Drained`, `Handed` and `PayloadGone` is
//! logged and counted. One whose message names a `status_to` is first
//! published there as a delivery status, on the authority of the message's
//! sender; a failure whose status was published told its sender and raises
//! nothing, except a broker's rejection, which is raised whether or not its
//! sender was told; any other failure is raised to the operator. Repeats of a kind
//! while its episode is open are logged and counted but withheld from the
//! alert path; the episode closes, with one summary alert for what it
//! withheld, when the client next delivers or runs dry, or is removed. A
//! status the bus refused is a failure of its own kind, whose episode only
//! running dry or the removal closes: refusals arrive interleaved with
//! deliveries and with published statuses. A message the channel's window
//! drops before the reader reached it is not a report: the bus counts and
//! raises it under the participant's `Alarm` rung.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::watch;
use uuid::Uuid;

use brenn_common::sanitize_untrusted_str;
use brenn_envelope::delivery::{
    DELIVERY_REASON_MAX_BYTES, DeliveryDropCause, DeliveryKind, DeliveryStatus,
    DeliveryUnknownCause, MqttDelivery,
};
use brenn_envelope::{MessageEnvelope, MqttPublishTransport, TransportMeta};
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{ParticipantId, mqtt_egress_address};
use brenn_messaging::Messenger;
use brenn_messaging::mqtt_outbound::{
    MQTT_OUTBOUND_COMPONENT, MQTT_OUTBOUND_NOISE, mqtt_outbound_participant,
};
use brenn_messaging::publish::{DeliveryStatusPublish, DeliveryStatusRefusal};
use brenn_messaging_store::blob::BlobError;
use brenn_messaging_store::store::{Attached, MessageSeq};
use brenn_mqtt::reader::{
    EgressBus, EgressItem, EgressMessage, EgressMeta, EgressReport, Sent, UnknownCause,
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
    /// Messages given up unsent, by count: by the reader as it ended, whether
    /// or not the client library ever accepted them, and by the hub, unread,
    /// when the client was removed.
    pub dropped: u64,
    /// Delivery statuses the bus refused to publish.
    pub status_unpublished: u64,
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
    StatusUnpublished,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Unknown => "outcome unknown",
            Self::Dropped => "dropped",
            Self::StatusUnpublished => "status not published",
        }
    }
}

/// What the hub records: a report from a client's reader, or one of the two
/// reports [`MqttEgressHub::close`] makes for what a removed client's channel
/// held unread.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HubReport {
    Reader(EgressReport),
    /// `count` unread messages naming no `status_to`.
    Removed {
        count: u64,
    },
    /// One unread message naming a `status_to`.
    RemovedMessage(EgressMeta),
}

/// Why messages were given up unsent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropCause {
    /// The session stopped, or the reader was retired, while the reader held
    /// them for a resubmit.
    Stopped,
    /// The client was removed with them unread on its channel.
    Removed,
}

/// Whether a report's message was told of it on its `status_to`.
#[derive(Debug)]
enum Told {
    /// The message names no `status_to`, or the report has no status.
    NoStatus,
    Published,
    /// The bus refused the status: what the operator is told of it.
    Refused {
        message_id: Uuid,
        status_to: String,
        kind: &'static str,
        refusal: DeliveryStatusRefusal,
    },
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
            participant: mqtt_outbound_participant(),
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
            // Only the count is logged: every skipped message has had its
            // report, an outcome while its client ran or `dropped`, `removed`
            // when the client was removed, except what a client deleted from
            // the configuration while the server was down left unread, which
            // nothing reports (TODO(mqtt-egress-removed-client-rows)).
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

    /// Settle a removed client's egress channel: give up the participant's
    /// position, taking in the same step every message it had not been
    /// served, report each `dropped`, `removed` — one count for those naming
    /// no `status_to`, if any, then one report per message naming one, in
    /// channel order — then close every open episode with its summary alert
    /// and forget the client's counts. A commit to the
    /// channel after the take is refused, so nothing committed to it goes
    /// unreported.
    ///
    /// The client's reader must have exited: one still running panics at its
    /// next bus call on the missing position.
    ///
    /// # Panics
    ///
    /// If the channel is not in the directory, or the participant holds no
    /// position on it.
    pub async fn close(&self, client: &str) {
        let address = mqtt_egress_address(client);
        let depth = self.channel_depth(&address);
        let unseen = self
            .messenger
            .detach_subscriber_taking_unseen(
                &address,
                &self.participant,
                depth,
                MQTT_OUTBOUND_NOISE,
            )
            .await
            .unwrap_or_else(|| {
                panic!(
                    "MQTT egress: close of client {client:?}, whose position on {address} the hub \
                     never took or has already given up"
                )
            });
        let mut unnamed = 0u64;
        let mut named = Vec::new();
        for (_, envelope) in &unseen {
            let publish = egress_publish(&address, envelope);
            if publish.status_to.is_some() {
                named.push(EgressMeta::new(
                    envelope.message_id,
                    &envelope.sender,
                    publish,
                ));
            } else {
                unnamed += 1;
            }
        }
        if unnamed > 0 {
            self.record(client, HubReport::Removed { count: unnamed })
                .await;
        }
        for message in named {
            self.record(client, HubReport::RemovedMessage(message))
                .await;
        }
        let mut alerts = Vec::new();
        {
            let mut clients = self.clients.lock().expect(HUB_POISONED);
            let mut state = clients.remove(client).unwrap_or_else(|| {
                panic!("MQTT egress: close of client {client:?}, whose channel is not open")
            });
            Recording {
                client,
                state: &mut state,
                alerts: &mut alerts,
            }
            .close_episodes(Closer::Removed);
        }
        self.raise(alerts);
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

    /// Publish `report`'s status, if it has one, then log, count and alert
    /// `report` for `client`, unless an open episode withholds the alert. A
    /// failure whose status was published is not alerted, except a broker's
    /// rejection.
    async fn record(&self, client: &str, report: HubReport) {
        let told = match delivery_status(client, &report) {
            Some((message, status)) => self.tell(message, &status).await,
            None => Told::NoStatus,
        };
        let mut alerts = Vec::new();
        {
            let mut clients = self.clients.lock().expect(HUB_POISONED);
            let state = clients.get_mut(client).unwrap_or_else(|| {
                panic!("MQTT egress: a report for client {client:?}, whose channel is not open")
            });
            let mut recording = Recording {
                client,
                state,
                alerts: &mut alerts,
            };
            if let Told::Refused {
                message_id,
                status_to,
                kind,
                refusal,
            } = &told
            {
                recording.status_refused(*message_id, status_to, kind, refusal);
            }
            let told = matches!(told, Told::Published);
            match report {
                HubReport::Reader(EgressReport::Handed(message)) => {
                    tracing::debug!(
                        mqtt_client = client,
                        message_id = %message.id,
                        "MQTT egress: handed"
                    );
                }
                HubReport::Reader(EgressReport::PayloadGone(message)) => {
                    tracing::debug!(
                        mqtt_client = client,
                        message_id = %message.id,
                        "MQTT egress: a message naming a status channel left retention before \
                         it was read"
                    );
                }
                HubReport::Reader(EgressReport::Delivered(message)) => {
                    tracing::debug!(
                        mqtt_client = client,
                        message_id = %message.id,
                        "MQTT egress: delivered"
                    );
                    recording.state.counts.delivered += 1;
                    recording.close_episodes(Closer::Delivered);
                }
                HubReport::Reader(EgressReport::Drained) => {
                    recording.close_episodes(Closer::Drained);
                }
                HubReport::Reader(EgressReport::Rejected {
                    message,
                    reason,
                    code,
                    earlier: _,
                }) => {
                    let reason = sanitize_untrusted_str(&reason, UNTRUSTED_TEXT_MAX);
                    recording.state.counts.rejected += 1;
                    // TODO(mqtt-egress-operator-text-unsent-of-possibly-sent)
                    let detail = format!("the broker rejected it: {reason}");
                    // A broker's rejection means Brenn's per-topic ACL admits
                    // what the broker forbids, which only the operator can fix.
                    let broker_answered = code.is_some();
                    recording.failure(
                        FailureKind::Rejected,
                        &message,
                        &detail,
                        told,
                        !told || broker_answered,
                    );
                }
                HubReport::Reader(EgressReport::Expired { message, sent: _ }) => {
                    recording.state.counts.expired += 1;
                    // TODO(mqtt-egress-operator-text-unsent-of-possibly-sent)
                    let detail = "its lifetime passed before it could be handed to the broker";
                    recording.failure(FailureKind::Expired, &message, detail, told, !told);
                }
                HubReport::Reader(EgressReport::Unknown { message, cause }) => {
                    recording.state.counts.unknown += 1;
                    let detail = match cause {
                        UnknownCause::ConnectionLost => {
                            "the connection ended before the broker answered; it is not resent"
                        }
                        UnknownCause::SessionStopped => {
                            "the client session stopped before the broker answered"
                        }
                    };
                    recording.failure(FailureKind::Unknown, &message, detail, told, !told);
                }
                HubReport::Reader(EgressReport::Dropped { count }) => {
                    recording.state.counts.dropped += count;
                    recording.dropped(DropCause::Stopped, count, false, true);
                }
                HubReport::Removed { count } => {
                    recording.state.counts.dropped += count;
                    recording.dropped(DropCause::Removed, count, false, true);
                }
                HubReport::Reader(EgressReport::DroppedMessage { message, sent: _ }) => {
                    recording.dropped_message(&message, DropCause::Stopped, told);
                }
                HubReport::RemovedMessage(message) => {
                    recording.dropped_message(&message, DropCause::Removed, told);
                }
            }
        }
        self.raise(alerts);
    }

    fn raise(&self, alerts: Vec<Alert>) {
        for alert in alerts {
            self.alerts
                .alert(AlertSeverity::Warning, alert.title, alert.body);
        }
    }

    /// Publish `status` about `message` to its `status_to`.
    async fn tell(&self, message: &EgressMeta, status: &DeliveryStatus) -> Told {
        let status_to = message
            .status_to
            .as_deref()
            .expect("only a message naming a status_to has a status");
        match self
            .messenger
            .publish_delivery_status(&message.sender, status_to, status)
            .await
        {
            DeliveryStatusPublish::Published => Told::Published,
            DeliveryStatusPublish::Refused(refusal) => Told::Refused {
                message_id: message.id,
                status_to: status_to.to_string(),
                kind: kind_label(&status.kind),
                refusal,
            },
        }
    }
}

/// The delivery status `report` tells, and the message it concerns, when that
/// message names a `status_to`. The one mapping from the reports and causes
/// to the status kinds and causes. A message that ends undelivered with a copy
/// the library accepted and the broker never answered is told `unknown`;
/// `rejected`, `expired` and `dropped` only a message with no such copy.
fn delivery_status<'a>(
    client: &str,
    report: &'a HubReport,
) -> Option<(&'a EgressMeta, DeliveryStatus)> {
    let report = match report {
        HubReport::Reader(report) => report,
        HubReport::RemovedMessage(message) => {
            return status_of(
                client,
                message,
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::Removed,
                },
                None,
            );
        }
        HubReport::Removed { .. } => return None,
    };
    let (message, kind, reason_code) = match report {
        EgressReport::Handed(message) => (message, DeliveryKind::Handed, None),
        EgressReport::Delivered(message) => (message, DeliveryKind::Delivered, None),
        EgressReport::Rejected {
            message,
            reason,
            code,
            earlier: Sent::Never,
        } => (
            message,
            DeliveryKind::Rejected {
                reason: sanitize_untrusted_str(reason, DELIVERY_REASON_MAX_BYTES),
            },
            *code,
        ),
        EgressReport::Rejected {
            message,
            reason: _,
            code,
            earlier: Sent::Possibly,
        } => (
            message,
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::ResendRefused,
            },
            *code,
        ),
        EgressReport::Expired {
            message,
            sent: Sent::Never,
        } => (message, DeliveryKind::Expired, None),
        EgressReport::Expired {
            message,
            sent: Sent::Possibly,
        } => (
            message,
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::LifetimeEnded,
            },
            None,
        ),
        EgressReport::Unknown { message, cause } => {
            let cause = match cause {
                UnknownCause::ConnectionLost => DeliveryUnknownCause::ConnectionLost,
                UnknownCause::SessionStopped => DeliveryUnknownCause::Stopped,
            };
            (message, DeliveryKind::Unknown { cause }, None)
        }
        EgressReport::DroppedMessage {
            message,
            sent: Sent::Never,
        } => (
            message,
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::Stopped,
            },
            None,
        ),
        EgressReport::DroppedMessage {
            message,
            sent: Sent::Possibly,
        } => (
            message,
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::Stopped,
            },
            None,
        ),
        EgressReport::PayloadGone(message) => (
            message,
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::LeftRetention,
            },
            None,
        ),
        EgressReport::Dropped { .. } | EgressReport::Drained => return None,
    };
    status_of(client, message, kind, reason_code)
}

/// The status of kind `kind` about `message`, when it names a `status_to`.
fn status_of<'a>(
    client: &str,
    message: &'a EgressMeta,
    kind: DeliveryKind,
    reason_code: Option<u8>,
) -> Option<(&'a EgressMeta, DeliveryStatus)> {
    message.status_to.as_ref()?;
    let status = DeliveryStatus {
        message_id: message.id,
        kind,
        mqtt: Some(MqttDelivery {
            client: client.to_string(),
            topic: message.topic.clone(),
            qos: message.qos,
            reason_code,
        }),
    };
    Some((message, status))
}

/// A status kind's wire word.
fn kind_label(kind: &DeliveryKind) -> &'static str {
    match kind {
        DeliveryKind::Handed => "handed",
        DeliveryKind::Delivered => "delivered",
        DeliveryKind::Rejected { .. } => "rejected",
        DeliveryKind::Expired => "expired",
        DeliveryKind::Unknown { .. } => "unknown",
        DeliveryKind::Dropped { .. } => "dropped",
    }
}

/// What a refusal means, for the operator.
fn refusal_text(refusal: &DeliveryStatusRefusal) -> String {
    match refusal {
        DeliveryStatusRefusal::ChannelGone => "the channel is no longer declared".to_string(),
        DeliveryStatusRefusal::SenderGone => {
            "no app with the sender's identity holds the publish grant any more".to_string()
        }
        DeliveryStatusRefusal::SenderNotPermitted => {
            "the sender's publish ACL no longer admits the channel".to_string()
        }
        DeliveryStatusRefusal::RateLimited => {
            "the channel's send rate for delivery statuses refused it".to_string()
        }
        DeliveryStatusRefusal::BodyTooLarge { len, max } => {
            format!("the status is {len} bytes and the bus's body cap is {max} bytes")
        }
    }
}

/// One report's accounting for one client: its state under the hub's lock,
/// and the alerts to raise once the lock is dropped.
struct Recording<'a> {
    client: &'a str,
    state: &'a mut ClientState,
    alerts: &'a mut Vec<Alert>,
}

impl Recording<'_> {
    /// Log the `kind` status about message `message_id` that the bus refused
    /// on `status_to`, count it, and open the "status not published" episode
    /// with an alert or count it as withheld.
    fn status_refused(
        &mut self,
        message_id: Uuid,
        status_to: &str,
        kind: &str,
        refusal: &DeliveryStatusRefusal,
    ) {
        let client = self.client;
        self.state.counts.status_unpublished += 1;
        let address = sanitize_untrusted_str(status_to, UNTRUSTED_TEXT_MAX);
        let why = refusal_text(refusal);
        tracing::warn!(
            mqtt_client = client,
            message_id = %message_id,
            status = kind,
            status_to = %address,
            refusal = %why,
            "MQTT egress: a delivery status was not published"
        );
        let body = format!(
            "MQTT client {client:?}: the {kind:?} status of message {} could not be published \
             to {address}: {why}. Its sender was not told.",
            message_id
        );
        self.open_or_withhold(FailureKind::StatusUnpublished, 1, body);
    }

    /// Log one failure report about `message`, whose sender was `told` of it
    /// on its `status_to`, and, when `alert`, open its kind's episode with an
    /// alert or count it as withheld.
    fn failure(
        &mut self,
        kind: FailureKind,
        message: &EgressMeta,
        detail: &str,
        told: bool,
        alert: bool,
    ) {
        let client = self.client;
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
            told,
            "MQTT egress: message not delivered"
        );
        if !alert {
            return;
        }
        let mut body = format!(
            "MQTT client {client:?}: message {} from {sender} for topic {topic} (QoS {}) was \
             not delivered: {detail}.",
            message.id, message.qos
        );
        if told {
            body.push_str(" Its sender was told on its status channel.");
        }
        self.open_or_withhold(kind, 1, body);
    }

    /// Log a drop of `count` messages, whose sender was `told` of them on its
    /// `status_to`, and, when `alert`, open the dropped episode with an alert
    /// or count them as withheld.
    fn dropped(&mut self, cause: DropCause, count: u64, told: bool, alert: bool) {
        let client = self.client;
        let detail = match cause {
            DropCause::Stopped => "the client session stopped while they waited to be resubmitted",
            DropCause::Removed => "the client was removed with them still on its channel",
        };
        // TODO(mqtt-egress-operator-text-unsent-of-possibly-sent)
        tracing::warn!(
            mqtt_client = client,
            kind = FailureKind::Dropped.label(),
            count,
            detail,
            told,
            "MQTT egress: messages dropped unsent"
        );
        if !alert {
            return;
        }
        let body = format!("MQTT client {client:?}: {count} message(s) dropped unsent: {detail}.");
        self.open_or_withhold(FailureKind::Dropped, count, body);
    }

    /// Count and log one dropped message naming a `status_to`, whose sender
    /// was `told` of it, and alert it as [`Self::dropped`] does when it was
    /// not.
    fn dropped_message(&mut self, message: &EgressMeta, cause: DropCause, told: bool) {
        self.state.counts.dropped += 1;
        tracing::debug!(
            mqtt_client = self.client,
            message_id = %message.id,
            "MQTT egress: the dropped message names a status channel"
        );
        self.dropped(cause, 1, told, !told);
    }

    fn open_or_withhold(&mut self, kind: FailureKind, messages: u64, body: String) {
        match self.state.episodes.get_mut(&kind) {
            Some(withheld) => *withheld += messages,
            None => {
                self.state.episodes.insert(kind, 0);
                self.alerts.push(Alert {
                    title: format!("MQTT egress: {} ({})", kind.label(), self.client),
                    body,
                });
            }
        }
    }

    /// Close the open episodes that `closer` closes, raising one summary
    /// alert for each that withheld anything.
    fn close_episodes(&mut self, closer: Closer) {
        let client = self.client;
        let closing: Vec<FailureKind> = self
            .state
            .episodes
            .keys()
            .copied()
            .filter(|kind| closer != Closer::Delivered || *kind != FailureKind::StatusUnpublished)
            .collect();
        for kind in closing {
            let withheld = self
                .state
                .episodes
                .remove(&kind)
                .expect("a kind just listed from the map");
            if withheld == 0 {
                continue;
            }
            let since = match (closer, kind) {
                (Closer::Removed, _) => "the client has since been removed",
                (_, FailureKind::StatusUnpublished) => "the client's reader has since run dry",
                _ => "the client has since delivered or run dry",
            };
            let body = if kind == FailureKind::StatusUnpublished {
                format!(
                    "MQTT client {client:?}: {withheld} more delivery status(es) could not be \
                     published after the first was reported; {since}."
                )
            } else {
                format!(
                    "MQTT client {client:?}: {withheld} more message(s) {} after the first was \
                     reported; {since}.",
                    kind.label()
                )
            };
            self.alerts.push(Alert {
                title: format!("MQTT egress: {} ({client}), summary", kind.label()),
                body,
            });
        }
    }
}

/// What closes episodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Closer {
    /// A delivery: every episode but "status not published", whose refusals
    /// arrive interleaved with deliveries.
    Delivered,
    /// The reader ran dry: every episode.
    Drained,
    /// The client was removed: every episode, as nothing can close them later.
    Removed,
}

/// One client's reader's view of its egress channel.
struct ClientEgress {
    hub: Arc<MqttEgressHub>,
    client: String,
    address: String,
    push_depth: Depth,
}

impl ClientEgress {
    /// Move a fresh position past every retained message the head read would
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
        self.advance_over(*last, *first).await;
        new.len() as u64
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
             of client {:?} runs over a channel its hub never opened or has closed",
            self.address, self.client
        )
    }

    /// The reader's message for one retained entry: its transport and its
    /// payload, from the body or from its one attachment.
    async fn item(&self, seq: MessageSeq, envelope: &MessageEnvelope) -> EgressItem {
        let publish = egress_publish(&self.address, envelope).clone();
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
                        meta: EgressMeta::new(envelope.message_id, &envelope.sender, &publish),
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

    async fn report(&self, report: EgressReport) {
        self.hub
            .record(&self.client, HubReport::Reader(report))
            .await;
    }
}

/// The MQTT publish metadata of a message on the egress channel `address`.
fn egress_publish<'a>(address: &str, envelope: &'a MessageEnvelope) -> &'a MqttPublishTransport {
    match &envelope.transport {
        Some(TransportMeta::MqttPublish(publish)) => publish,
        other => panic!(
            "MQTT egress: message {} on the host-written channel {address} carries {other:?}, not \
             MQTT publish metadata",
            envelope.message_id
        ),
    }
}
