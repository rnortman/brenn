//! The egress reader: one task per client that hands the client's egress
//! channel to its session.
//!
//! The reader is one subscriber with one cursor on the channel. It moves the
//! cursor past a message before it gives the message to the client library,
//! so a crash loses what was handed and never publishes anything twice. It
//! keeps at most `min(Receive Maximum, window depth)` messages outstanding —
//! in flight with the library or held for a resubmit — and learns each
//! outcome from the library's publish notice.
//!
//! It reaches the bus only through [`EgressBus`], so this crate does not
//! depend on the messaging crate; the implementation lives above both. A
//! message may name a `status_to`; the reader then reports it handed before
//! it advances over it, and reports each of its drops one by one, so that the
//! bus side can tell its sender. Every awaited bus call moves the handle's
//! progress counter, which the reader's join watches.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use brenn_envelope::{MqttExpiry, MqttPublishTransport};
use brenn_messaging_store::store::MessageSeq;

use crate::service::qos_of;
use crate::state::{EGRESS_READER_POISONED, MqttClientHandle, NoticeOutcome, notice_outcome};

#[cfg(test)]
mod tests;

/// One message the channel holds for this client, as the reader hands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressMessage {
    pub seq: MessageSeq,
    pub id: Uuid,
    pub sender: String,
    pub publish: MqttPublishTransport,
    pub payload: Bytes,
}

/// One entry of a head read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressItem {
    Message(EgressMessage),
    /// The row was read and its blob was no longer held: the message left
    /// retention between the two reads.
    PayloadGone {
        seq: MessageSeq,
        meta: EgressMeta,
    },
}

/// What a report says about the message it concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EgressMeta {
    pub id: Uuid,
    pub sender: String,
    pub topic: String,
    pub qos: u8,
    /// The `brenn:` channel the sender named for the message's statuses.
    pub status_to: Option<String>,
}

impl EgressMeta {
    /// The metadata of a message with id `id` from `sender`, published as
    /// `publish`.
    pub fn new(id: Uuid, sender: &str, publish: &MqttPublishTransport) -> Self {
        Self {
            id,
            sender: sender.to_string(),
            topic: publish.topic.clone(),
            qos: publish.qos,
            status_to: publish.status_to.clone(),
        }
    }

    fn of(message: &EgressMessage) -> Self {
        Self::new(message.id, &message.sender, &message.publish)
    }
}

/// Why a handed message's outcome is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownCause {
    /// Its connection ended before the broker answered, and it is not resent.
    ConnectionLost,
    /// The session stopped while it was in flight.
    SessionStopped,
}

/// Whether the client library ever accepted a message for sending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    /// Every submit of it was given back before the library held it: no
    /// client, a newer connection, or a full request channel.
    Never,
    /// The library accepted it; nothing the reader sees says whether a byte
    /// of it reached the broker.
    Possibly,
}

/// Why handed or unhanded messages were given up; whether the library ever
/// had them is the report's `sent`, where it carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropCause {
    /// The session stopped while the reader held them for a resubmit.
    SessionStopped,
    /// The client was removed with them still on its channel.
    ClientRemoved,
}

/// What became of the messages the reader handled. The bus side's report sink
/// is the one place each is logged, counted and raised.
#[derive(Debug, Clone, PartialEq, Eq)]
///
/// A report concerns at most one message that names a `status_to`.
pub enum EgressReport {
    /// A message naming a `status_to` is about to be advanced over and given
    /// to the library. Reported once per message, never for a resubmit.
    Handed(EgressMeta),
    Delivered(EgressMeta),
    /// The broker refused it, and `code` is its reason code; or the host
    /// refused a packet over the broker's Maximum Packet Size, and `code` is
    /// `None`. `code` is `Some` exactly when the broker answered. `earlier`
    /// says whether the library had accepted it on a submission before the
    /// one this report answers, on a connection that ended before the broker
    /// answered.
    Rejected {
        message: EgressMeta,
        reason: String,
        code: Option<u8>,
        earlier: Sent,
    },
    /// Its lifetime passed before it was (again) given to the library;
    /// `sent` says whether the library had accepted it before.
    Expired {
        message: EgressMeta,
        sent: Sent,
    },
    Unknown {
        message: EgressMeta,
        cause: UnknownCause,
    },
    /// `count` messages naming no `status_to` were given up, whether or not
    /// the library ever had them.
    Dropped {
        cause: DropCause,
        count: u64,
    },
    /// One message naming a `status_to` was given up; `sent` says whether the
    /// library ever had it.
    DroppedMessage {
        message: EgressMeta,
        cause: DropCause,
        sent: Sent,
    },
    /// A message naming a `status_to` left retention before it was read.
    PayloadGone(EgressMeta),
    /// Nothing is outstanding and the channel holds nothing unhanded.
    Drained,
}

/// What [`EgressBus::abandon`] skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abandoned {
    /// How many unhanded messages the position moved past.
    pub count: u64,
    /// The metadata of those among them that name a `status_to`.
    pub named: Vec<EgressMeta>,
}

/// The reader's view of its client's egress channel: one subscriber position.
#[async_trait]
pub trait EgressBus: Send + Sync + 'static {
    /// The oldest `limit` unhanded messages, oldest first. Moves nothing.
    async fn unhanded(&self, limit: usize) -> Vec<EgressItem>;
    /// Move the position past `seq`.
    async fn advance(&self, seq: MessageSeq);
    /// Move the position past everything retained; what was unhanded.
    async fn abandon(&self) -> Abandoned;
    /// Changes whenever the channel may hold a message this reader has not read.
    fn wakes(&self) -> watch::Receiver<u64>;
    /// The channel's retain depth, `None` when unbounded.
    fn window_depth(&self) -> Option<usize>;
    /// Log, count, alert and, for a message naming a `status_to`, publish
    /// its status. Returns once the status, if any, is committed or refused.
    async fn report(&self, report: EgressReport);
}

/// The reader's one way to its bus: each awaited call moves the handle's
/// progress counter when it returns.
struct CountedBus {
    handle: Arc<MqttClientHandle>,
    bus: Arc<dyn EgressBus>,
}

impl CountedBus {
    fn progressed(&self) {
        self.handle
            .egress_reader_progress
            .fetch_add(1, Ordering::Relaxed);
    }

    async fn unhanded(&self, limit: usize) -> Vec<EgressItem> {
        let items = self.bus.unhanded(limit).await;
        self.progressed();
        items
    }

    async fn advance(&self, seq: MessageSeq) {
        self.bus.advance(seq).await;
        self.progressed();
    }

    async fn abandon(&self) -> Abandoned {
        let abandoned = self.bus.abandon().await;
        self.progressed();
        abandoned
    }

    async fn report(&self, report: EgressReport) {
        self.bus.report(report).await;
        self.progressed();
    }

    fn wakes(&self) -> watch::Receiver<u64> {
        self.bus.wakes()
    }
}

/// The reader recorded on a handle.
pub(crate) enum ReaderSlot {
    /// No reader was ever started on this handle.
    Never,
    Running {
        join: tokio::task::JoinHandle<()>,
        retire: oneshot::Sender<()>,
    },
    /// The reader was retired or joined; none may start again.
    Gone,
}

/// Spawn `handle`'s egress reader over `bus` and record it on the handle.
///
/// # Panics
///
/// If a reader was ever started on this handle, or the channel's window depth
/// is zero (the reader could never hand anything off).
pub fn start_egress_reader(handle: &Arc<MqttClientHandle>, bus: Arc<dyn EgressBus>) {
    let slug = handle.config.identity.slug.clone();
    let window = bus.window_depth();
    assert!(
        window != Some(0),
        "egress channel of MQTT client {slug:?} has a window depth of 0: its reader could never \
         hand off a message",
    );
    let mut slot = handle.egress_reader.lock().expect(EGRESS_READER_POISONED);
    assert!(
        matches!(*slot, ReaderSlot::Never),
        "start_egress_reader called twice for MQTT client {slug:?}",
    );
    let (retire_tx, retire_rx) = oneshot::channel();
    let reader = Reader {
        handle: Arc::clone(handle),
        bus: CountedBus {
            handle: Arc::clone(handle),
            bus,
        },
        window,
        held: BTreeMap::new(),
        in_flight: BTreeMap::new(),
        unread: true,
        more: false,
        again: false,
    };
    let stop = handle.stop_tx.subscribe();
    let join = tokio::spawn(brenn_lib::panic_util::abort_on_panic(
        "MQTT egress reader panicked",
        reader.run(stop, retire_rx),
    ));
    *slot = ReaderSlot::Running {
        join,
        retire: retire_tx,
    };
}

type NoticeResult = Result<rumqttc::PublishResult, rumqttc::PublishNoticeError>;

/// A handed message the library holds, the connection it went out on, the
/// wait for its notice, and whether the library had accepted it on an earlier
/// submission.
struct InFlight {
    message: EgressMessage,
    epoch: u64,
    earlier: Sent,
    notice: Pin<Box<dyn Future<Output = NoticeResult> + Send>>,
}

/// A handed message the library does not have, and whether it ever had it.
struct Held {
    message: EgressMessage,
    sent: Sent,
}

/// The facts of the current connection a pass decides on.
struct Connection {
    epoch: u64,
    receive_max: u16,
}

/// What woke the reader.
enum Wake {
    Stop,
    Retire,
    Channel,
    Room,
    Notice(MessageSeq, NoticeResult),
    Again,
}

struct Reader {
    handle: Arc<MqttClientHandle>,
    bus: CountedBus,
    window: Option<usize>,
    /// Handed messages the library does not have, by channel order.
    held: BTreeMap<MessageSeq, Held>,
    /// Handed messages whose notice is unresolved, by channel order.
    in_flight: BTreeMap<MessageSeq, InFlight>,
    /// A channel wake arrived since the last read.
    unread: bool,
    /// The last read left, or may have left, unhanded messages behind.
    more: bool,
    /// The last pass ended on a full read with room left over: pass again
    /// once stop and retire have been checked.
    again: bool,
}

impl Reader {
    async fn run(mut self, mut stop: watch::Receiver<bool>, mut retire: oneshot::Receiver<()>) {
        let mut wakes = self.bus.wakes();
        wakes.borrow_and_update();
        let handle = Arc::clone(&self.handle);
        if !*stop.borrow_and_update() {
            self.pass().await;
        }
        loop {
            let again = self.again;
            let wake = tokio::select! {
                biased;
                stopped = stop.wait_for(|stopped| *stopped) => {
                    stopped.expect("the handle owns the stop sender and outlives its reader");
                    Wake::Stop
                }
                retired = &mut retire => {
                    retired.expect("the handle keeps the retire sender until it retires the reader");
                    Wake::Retire
                }
                changed = wakes.changed() => {
                    changed.expect("the egress bus keeps its wake sender while a reader holds it");
                    Wake::Channel
                }
                () = handle.egress_wake.notified() => Wake::Room,
                (seq, result) = next_notice(&mut self.in_flight) => Wake::Notice(seq, result),
                () = std::future::ready(()), if again => Wake::Again,
            };
            match wake {
                Wake::Stop => {
                    self.report_stopped().await;
                    return;
                }
                Wake::Retire => {
                    self.report_stopped().await;
                    let Abandoned { count, named } = self.bus.abandon().await;
                    let named = named
                        .into_iter()
                        .map(|message| (message, Sent::Never))
                        .collect();
                    self.report_dropped(DropCause::ClientRemoved, count, named)
                        .await;
                    return;
                }
                Wake::Channel => self.unread = true,
                Wake::Room | Wake::Again => {}
                Wake::Notice(seq, result) => self.resolve(seq, result).await,
            }
            self.pass().await;
        }
    }

    fn outstanding(&self) -> usize {
        self.held.len() + self.in_flight.len()
    }

    /// The most messages outstanding at once: the broker's Receive Maximum,
    /// and the window depth when the window is bounded. Each outstanding
    /// message holds its payload in memory. A payload within the bus's body
    /// cap is a body. A larger one is a blob, and the blob store's budget
    /// already bounds those bytes across every client.
    fn limit(&self, receive_max: u16) -> usize {
        let limit = match self.window {
            Some(depth) => depth.min(usize::from(receive_max)),
            None => usize::from(receive_max),
        };
        assert!(
            limit > 0,
            "MQTT client {:?}: the broker stated a Receive Maximum of 0, which MQTT 5.0 forbids \
             and the client library accepted",
            self.handle.config.identity.slug,
        );
        limit
    }

    /// Hand off what may be handed off now.
    ///
    /// The reader has no timer, so every way a pass ends with work pending
    /// names the wake that brings the next pass:
    ///
    /// - no client: the next connection's install;
    /// - a notice of an older connection unresolved, or the limit reached:
    ///   that notice, or the first in-flight notice, resolving;
    /// - a submit refused by a full request channel: the event loop taking a
    ///   request off it, which emits an outgoing event;
    /// - a submit that found a newer connection than the pass began on: that
    ///   connection's install;
    /// - a full read that left room (its messages expired, gone or rejected
    ///   as oversize): `again`.
    async fn pass(&mut self) {
        self.again = false;
        let Some(connection) = connection(&self.handle).await else {
            return;
        };
        if self
            .in_flight
            .values()
            .any(|flight| flight.epoch < connection.epoch)
        {
            return;
        }
        let limit = self.limit(connection.receive_max);
        if self.outstanding() >= limit && self.held.is_empty() {
            return;
        }

        while let Some((seq, Held { message, sent })) = self.held.pop_first() {
            let Some(expiry) = expiry_interval(&message.publish.expires, Utc::now()) else {
                self.bus
                    .report(EgressReport::Expired {
                        message: EgressMeta::of(&message),
                        sent,
                    })
                    .await;
                continue;
            };
            if let Err(message) = self.submit(message, expiry, connection.epoch, sent).await {
                self.held.insert(seq, Held { message, sent });
                return;
            }
        }

        let outstanding = self.outstanding();
        if outstanding < limit && (self.unread || self.more) {
            self.unread = false;
            let want = limit - outstanding;
            let items = self.bus.unhanded(want).await;
            assert!(
                items.len() <= want,
                "egress head read answered {} items for a limit of {want}",
                items.len(),
            );
            self.more = items.len() == want;
            let mut items = items.into_iter();
            while let Some(item) = items.next() {
                let message = match item {
                    EgressItem::PayloadGone { seq, meta } => {
                        self.bus.advance(seq).await;
                        if meta.status_to.is_some() {
                            self.bus.report(EgressReport::PayloadGone(meta)).await;
                        }
                        continue;
                    }
                    EgressItem::Message(message) => message,
                };
                let seq = message.seq;
                let expiry = expiry_interval(&message.publish.expires, Utc::now());
                if expiry.is_some() && message.publish.status_to.is_some() {
                    self.bus
                        .report(EgressReport::Handed(EgressMeta::of(&message)))
                        .await;
                }
                self.bus.advance(seq).await;
                let Some(expiry) = expiry else {
                    self.bus
                        .report(EgressReport::Expired {
                            message: EgressMeta::of(&message),
                            sent: Sent::Never,
                        })
                        .await;
                    continue;
                };
                if let Err(message) = self
                    .submit(message, expiry, connection.epoch, Sent::Never)
                    .await
                {
                    self.held.insert(
                        seq,
                        Held {
                            message,
                            sent: Sent::Never,
                        },
                    );
                    if items.len() > 0 {
                        self.more = true;
                    }
                    return;
                }
            }
        }

        self.again = self.more && self.outstanding() < limit;
        if self.held.is_empty() && self.in_flight.is_empty() && !self.unread && !self.more {
            self.bus.report(EgressReport::Drained).await;
        }
    }

    /// Give `message` to the client of connection `epoch` with the Message
    /// Expiry Interval `expiry`. `Err` gives the message back when there is
    /// no client, the client belongs to a newer connection, or its request
    /// channel refused the publish. A packet over the broker's Maximum Packet
    /// Size is reported rejected and is done. `earlier` is whether the library
    /// had accepted the message on a submission before this one.
    async fn submit(
        &mut self,
        message: EgressMessage,
        expiry: Option<u32>,
        epoch: u64,
        earlier: Sent,
    ) -> Result<(), EgressMessage> {
        let publish = &message.publish;
        assert!(
            rumqttc::valid_topic(&publish.topic),
            "egress message {} names topic {:?}, which the admission refuses",
            message.id,
            publish.topic,
        );
        let qos = qos_of(publish.qos).unwrap_or_else(|| {
            panic!(
                "egress message {} with QoS {}, which the admission refuses",
                message.id, publish.qos
            )
        });
        let properties = publish_properties(publish.content_type.clone(), expiry);

        let guard = self.handle.client.lock().await;
        let Some(client) = guard.as_ref() else {
            return Err(message);
        };
        if self.handle.connection_epoch.load(Ordering::Relaxed) != epoch {
            return Err(message);
        }
        let broker_max = self.handle.broker_max_packet_size.load(Ordering::Relaxed);
        if broker_max != 0 {
            let packet_bytes = publish_packet_size(
                &publish.topic,
                qos,
                publish.retain,
                &message.payload,
                &properties,
            );
            if packet_bytes > broker_max as usize {
                drop(guard);
                self.bus
                    .report(EgressReport::Rejected {
                        message: EgressMeta::of(&message),
                        reason: format!(
                            "the PUBLISH packet is {packet_bytes} bytes and the broker's Maximum \
                             Packet Size is {broker_max} bytes"
                        ),
                        code: None,
                        earlier,
                    })
                    .await;
                return Ok(());
            }
        }
        let topic = publish.topic.clone();
        let payload = message.payload.clone();
        let submitted = match properties {
            Some(properties) => client.try_publish_with_properties_tracked(
                topic,
                qos,
                publish.retain,
                payload,
                properties,
            ),
            None => client.try_publish_tracked(topic, qos, publish.retain, payload),
        };
        drop(guard);

        match submitted {
            Ok(notice) => {
                self.in_flight.insert(
                    message.seq,
                    InFlight {
                        message,
                        epoch,
                        earlier,
                        notice: Box::pin(notice.wait_async()),
                    },
                );
                Ok(())
            }
            Err(rumqttc::ClientError::TryRequest(_)) => Err(message),
            Err(e) => panic!(
                "tracked try-publish failed with {e:?}: a session's client is built with tracking \
                 and is never asked to await the request channel"
            ),
        }
    }

    /// Settle the in-flight message at `seq` by its notice's result.
    async fn resolve(&mut self, seq: MessageSeq, result: NoticeResult) {
        let InFlight {
            message, earlier, ..
        } = self
            .in_flight
            .remove(&seq)
            .unwrap_or_else(|| panic!("a notice resolved for {seq:?}, which is not in flight"));
        let report = match notice_outcome(result) {
            NoticeOutcome::Delivered => EgressReport::Delivered(EgressMeta::of(&message)),
            NoticeOutcome::Rejected { reason, code } => EgressReport::Rejected {
                message: EgressMeta::of(&message),
                reason,
                code: Some(code),
                earlier,
            },
            NoticeOutcome::ConnectionEnded if message.publish.qos == 1 => {
                self.held.insert(
                    seq,
                    Held {
                        message,
                        sent: Sent::Possibly,
                    },
                );
                return;
            }
            NoticeOutcome::ConnectionEnded => EgressReport::Unknown {
                message: EgressMeta::of(&message),
                cause: UnknownCause::ConnectionLost,
            },
        };
        self.bus.report(report).await;
    }

    /// Report what the reader still has as it exits: in flight is unknown,
    /// held is dropped.
    async fn report_stopped(&mut self) {
        for (_, InFlight { message, .. }) in std::mem::take(&mut self.in_flight) {
            self.bus
                .report(EgressReport::Unknown {
                    message: EgressMeta::of(&message),
                    cause: UnknownCause::SessionStopped,
                })
                .await;
        }
        let held = std::mem::take(&mut self.held);
        let count = held.len() as u64;
        let named = held
            .values()
            .filter(|held| held.message.publish.status_to.is_some())
            .map(|held| (EgressMeta::of(&held.message), held.sent))
            .collect();
        self.report_dropped(DropCause::SessionStopped, count, named)
            .await;
    }

    /// Report `count` messages dropped for `cause`, of which `named` are the
    /// ones naming a `status_to`, each with whether the library ever had it:
    /// one count for the rest, if any, then one report per named message.
    async fn report_dropped(
        &mut self,
        cause: DropCause,
        count: u64,
        named: Vec<(EgressMeta, Sent)>,
    ) {
        let unnamed = count.checked_sub(named.len() as u64).unwrap_or_else(|| {
            panic!(
                "{} dropped messages name a status_to out of {count} dropped",
                named.len()
            )
        });
        if unnamed > 0 {
            self.bus
                .report(EgressReport::Dropped {
                    cause,
                    count: unnamed,
                })
                .await;
        }
        for (message, sent) in named {
            assert!(
                message.status_to.is_some(),
                "message {} is reported dropped by itself and names no status_to",
                message.id
            );
            self.bus
                .report(EgressReport::DroppedMessage {
                    message,
                    cause,
                    sent,
                })
                .await;
        }
    }
}

/// The current connection's facts, `None` while there is no client.
async fn connection(handle: &MqttClientHandle) -> Option<Connection> {
    let guard = handle.client.lock().await;
    guard.as_ref()?;
    Some(Connection {
        epoch: handle.connection_epoch.load(Ordering::Relaxed),
        receive_max: handle.broker_receive_max.load(Ordering::Relaxed),
    })
}

/// The first in-flight notice to resolve, oldest first among those ready.
/// Pending for as long as none has, and forever when nothing is in flight.
async fn next_notice(in_flight: &mut BTreeMap<MessageSeq, InFlight>) -> (MessageSeq, NoticeResult) {
    std::future::poll_fn(|cx| {
        for (seq, flight) in in_flight.iter_mut() {
            if let Poll::Ready(result) = flight.notice.as_mut().poll(cx) {
                return Poll::Ready((*seq, result));
            }
        }
        Poll::Pending
    })
    .await
}

/// The Message Expiry Interval to send for a message whose lifetime ends at
/// `expires`, judged at `now`: `None` when the lifetime has passed, else
/// `Some(None)` for a message that never expires and `Some(Some(s))` with the
/// remaining seconds rounded up.
pub(crate) fn expiry_interval(expires: &MqttExpiry, now: DateTime<Utc>) -> Option<Option<u32>> {
    let at = match expires {
        MqttExpiry::Never => return Some(None),
        MqttExpiry::At(at) => *at,
    };
    let remaining = at - now;
    if remaining <= TimeDelta::zero() {
        return None;
    }
    let whole = remaining.num_seconds();
    let seconds = if remaining > TimeDelta::seconds(whole) {
        whole + 1
    } else {
        whole
    };
    Some(Some(u32::try_from(seconds).unwrap_or(u32::MAX)))
}

/// The PUBLISH properties carrying `content_type` and the Message Expiry
/// Interval `expiry`; `None` when there is neither.
fn publish_properties(
    content_type: Option<String>,
    expiry: Option<u32>,
) -> Option<rumqttc::PublishProperties> {
    (content_type.is_some() || expiry.is_some()).then(|| rumqttc::PublishProperties {
        content_type,
        message_expiry_interval: expiry,
        ..Default::default()
    })
}

/// The size of a PUBLISH as the client library will encode it: same topic,
/// QoS, retain, payload and properties, and a non-zero packet identifier at
/// QoS 1/2, which the encoder counts only when set. The measured packet shares
/// `payload`'s buffer.
fn publish_packet_size(
    topic: &str,
    qos: rumqttc::mqttbytes::QoS,
    retain: bool,
    payload: &bytes::Bytes,
    properties: &Option<rumqttc::PublishProperties>,
) -> usize {
    let mut packet = rumqttc::Publish::new(topic, qos, payload.clone(), properties.clone());
    packet.retain = retain;
    if qos != rumqttc::mqttbytes::QoS::AtMostOnce {
        packet.pkid = 1;
    }
    packet.size()
}
