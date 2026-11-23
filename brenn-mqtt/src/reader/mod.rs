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
//! bus side can tell its sender.
//!
//! At the session's stop, or at its own retire, the reader hands nothing
//! more. A pass checks the stop before each message it would hand, so a stop
//! that lands during a pass leaves the rest of its read on the channel; the
//! one message already being handed then either reaches the client and is
//! waited for, or finds the client cell emptied and is reported dropped. The
//! reader waits up to the client's `stop_wait_secs` for the notices of what
//! is in flight, reports each as what it resolved to and the rest unknown,
//! and exits; the handle's reader-live watch goes `false` after its last
//! report has committed. A retired reader leaves the channel as it is; what it
//! never read is reported when the client's channel is closed. A reader
//! started on a handle whose stop is already set hands nothing: it skips its
//! first pass and its first wake is the stop.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

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
    /// `count` held messages naming no `status_to` were given up as the
    /// reader ended, whether or not the library ever had them.
    Dropped {
        count: u64,
    },
    /// One held message naming a `status_to` was given up as the reader
    /// ended; `sent` says whether the library ever had it.
    DroppedMessage {
        message: EgressMeta,
        sent: Sent,
    },
    /// A message naming a `status_to` left retention before it was read.
    PayloadGone(EgressMeta),
    /// Nothing is outstanding and the channel holds nothing unhanded.
    Drained,
}

/// The reader's view of its client's egress channel: one subscriber position.
#[async_trait]
pub trait EgressBus: Send + Sync + 'static {
    /// The oldest `limit` unhanded messages, oldest first. Moves nothing.
    async fn unhanded(&self, limit: usize) -> Vec<EgressItem>;
    /// Move the position past `seq`.
    async fn advance(&self, seq: MessageSeq);
    /// Changes whenever the channel may hold a message this reader has not read.
    fn wakes(&self) -> watch::Receiver<u64>;
    /// The channel's retain depth, `None` when unbounded.
    fn window_depth(&self) -> Option<usize>;
    /// Log, count, alert and, for a message naming a `status_to`, publish
    /// its status. Returns once the status, if any, is committed or refused.
    async fn report(&self, report: EgressReport);
}

/// The reader recorded on a handle.
pub(crate) enum ReaderSlot {
    /// No reader was ever started on this handle.
    Never,
    Running {
        retire: oneshot::Sender<()>,
    },
    /// The reader was retired; none may start again.
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
    let reader = Reader::new(Arc::clone(handle), bus, window);
    let stop = handle.stop_tx.subscribe();
    handle.egress_reader_live.send_replace(true);
    let live = Arc::clone(handle);
    // The task's end is not awaited through a join handle: every waiter reads
    // the reader-live watch, which any number of them can share. A reader that
    // ends any way but returning takes the process with it, so the watch is
    // never left `true` by a dead task.
    drop(tokio::spawn(brenn_lib::panic_util::abort_on_panic(
        "MQTT egress reader panicked",
        async move {
            reader.run(stop, retire_rx).await;
            live.egress_reader_live.send_replace(false);
        },
    )));
    *slot = ReaderSlot::Running { retire: retire_tx };
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

/// Why the reader is ending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Stop,
    Retire,
}

impl Ending {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Retire => "retire",
        }
    }
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
    bus: Arc<dyn EgressBus>,
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
    fn new(handle: Arc<MqttClientHandle>, bus: Arc<dyn EgressBus>, window: Option<usize>) -> Self {
        Self {
            handle,
            bus,
            window,
            held: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            unread: true,
            more: false,
            again: false,
        }
    }

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
                Wake::Stop => return self.end(Ending::Stop).await,
                Wake::Retire => return self.end(Ending::Retire).await,
                Wake::Channel => self.unread = true,
                Wake::Room | Wake::Again => {}
                Wake::Notice(seq, result) => {
                    self.resolve(seq, result).await;
                }
            }
            self.pass().await;
        }
    }

    fn outstanding(&self) -> usize {
        self.held.len() + self.in_flight.len()
    }

    /// Whether the session's stop has been requested.
    fn stopping(&self) -> bool {
        *self.handle.stop_tx.borrow()
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
    /// A pass arms no timer — the only timer the reader ever holds is the
    /// deadline of [`Self::settle`] — so every way a pass ends with work
    /// pending names the wake that brings the next pass:
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
    ///
    /// A pass checks the session's stop before it hands each message, held or
    /// read, and returns at a stop with the rest unhanded.
    async fn pass(&mut self) {
        self.again = false;
        if self.stopping() {
            return;
        }
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
            if self.stopping() {
                self.held.insert(seq, Held { message, sent });
                return;
            }
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
                if self.stopping() {
                    self.more = true;
                    return;
                }
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

    /// Settle the in-flight message at `seq` by its notice's result. Returns
    /// whether the broker answered it; `false` when its connection ended
    /// first.
    async fn resolve(&mut self, seq: MessageSeq, result: NoticeResult) -> bool {
        let InFlight {
            message, earlier, ..
        } = self
            .in_flight
            .remove(&seq)
            .unwrap_or_else(|| panic!("a notice resolved for {seq:?}, which is not in flight"));
        let (report, answered) = match notice_outcome(result) {
            NoticeOutcome::Delivered => (EgressReport::Delivered(EgressMeta::of(&message)), true),
            NoticeOutcome::Rejected { reason, code } => (
                EgressReport::Rejected {
                    message: EgressMeta::of(&message),
                    reason,
                    code: Some(code),
                    earlier,
                },
                true,
            ),
            NoticeOutcome::ConnectionEnded if message.publish.qos == 1 => {
                self.held.insert(
                    seq,
                    Held {
                        message,
                        sent: Sent::Possibly,
                    },
                );
                return false;
            }
            NoticeOutcome::ConnectionEnded => (
                EgressReport::Unknown {
                    message: EgressMeta::of(&message),
                    cause: UnknownCause::ConnectionLost,
                },
                false,
            ),
        };
        self.bus.report(report).await;
        answered
    }

    /// The reader's last act before it exits, for a stop or a retire alike:
    /// settle and report what is still outstanding. The channel is left as it
    /// is. Logs one line as it begins and one when its last report has been
    /// made, so a stop that hangs in a bus call leaves a begin line with no end
    /// naming the client.
    async fn end(&mut self, ending: Ending) {
        let handle = Arc::clone(&self.handle);
        let client = handle.config.identity.slug.as_str();
        tracing::info!(
            client,
            ending = ending.as_str(),
            bound_secs = handle.stop_wait().as_secs(),
            in_flight = self.in_flight.len(),
            held = self.held.len(),
            "MQTT egress reader ending: waiting for what is in flight, then reporting",
        );
        self.settle(ending).await;
        self.report_stopped().await;
        tracing::info!(
            client,
            ending = ending.as_str(),
            "MQTT egress reader ended: every outcome reported",
        );
    }

    /// Wait, up to the client's stop bound, for the notices of what is in
    /// flight, and report each as it resolves. Hands nothing: messages still
    /// on the channel stay there, and held messages stay held.
    ///
    /// Ends when nothing is in flight, when the deadline passes, or when the
    /// connection ends, which resolves every remaining notice. The select
    /// prefers the notices, so a notice already resolved is read even with a
    /// bound of zero or a deadline passed during a report.
    ///
    /// Logs and returns what the wait did; `None`, logging nothing, when
    /// nothing was in flight.
    async fn settle(&mut self, ending: Ending) -> Option<SettleSummary> {
        let began = self.in_flight.len();
        if began == 0 {
            return None;
        }
        let bound = self.handle.stop_wait();
        let start = tokio::time::Instant::now();
        let deadline = tokio::time::sleep_until(start + bound);
        tokio::pin!(deadline);
        let mut answered = 0usize;
        let mut connection_ended = 0usize;
        while !self.in_flight.is_empty() {
            tokio::select! {
                biased;
                (seq, result) = next_notice(&mut self.in_flight) => {
                    if self.resolve(seq, result).await {
                        answered += 1;
                    } else {
                        connection_ended += 1;
                    }
                }
                () = &mut deadline => break,
            }
        }
        let summary = SettleSummary {
            ending,
            bound,
            waited: start.elapsed(),
            began,
            answered,
            connection_ended,
            left: self.in_flight.len(),
        };
        summary.log(&self.handle.config.identity.slug);
        Some(summary)
    }

    /// Report what the reader still has as it exits: in flight is unknown,
    /// held is dropped: one count for those naming no `status_to`, if any,
    /// then one report per message naming one.
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
        let (named, unnamed): (Vec<Held>, Vec<Held>) = held
            .into_values()
            .partition(|held| held.message.publish.status_to.is_some());
        if !unnamed.is_empty() {
            self.bus
                .report(EgressReport::Dropped {
                    count: unnamed.len() as u64,
                })
                .await;
        }
        for Held { message, sent } in named {
            self.bus
                .report(EgressReport::DroppedMessage {
                    message: EgressMeta::of(&message),
                    sent,
                })
                .await;
        }
    }
}

/// What one [`Reader::settle`] that began with messages in flight did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SettleSummary {
    ending: Ending,
    bound: Duration,
    waited: Duration,
    /// Messages in flight when the wait began.
    began: usize,
    /// Messages the broker answered during the wait, delivered or rejected.
    answered: usize,
    /// Messages whose connection ended during the wait before the broker
    /// answered them; their outcome is not known.
    connection_ended: usize,
    /// Messages still in flight when the wait ended, reported unknown.
    left: usize,
}

impl SettleSummary {
    /// Messages in flight at the start whose outcome the wait did not learn.
    fn unknown(&self) -> usize {
        self.connection_ended + self.left
    }

    /// One line: `warn` when any message's outcome is not known, `info` when
    /// the broker answered every one.
    fn log(&self, client: &str) {
        let bound_secs = self.bound.as_secs();
        let waited_ms = u64::try_from(self.waited.as_millis()).unwrap_or(u64::MAX);
        if self.unknown() > 0 {
            tracing::warn!(
                client,
                ending = self.ending.as_str(),
                bound_secs,
                waited_ms,
                in_flight = self.began,
                answered = self.answered,
                unknown = self.unknown(),
                connection_ended = self.connection_ended,
                unanswered_at_bound = self.left,
                "MQTT session ending: the broker did not answer every message in flight, because \
                 its connection ended or the stop bound passed; their outcomes are not known",
            );
        } else {
            tracing::info!(
                client,
                ending = self.ending.as_str(),
                bound_secs,
                waited_ms,
                in_flight = self.began,
                answered = self.answered,
                "MQTT session ending: the broker answered every message in flight",
            );
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
