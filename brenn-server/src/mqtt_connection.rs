//! The bus side of the MQTT connection state: one writer per client on
//! `brenn:mqtt-connection.<client>`, publishing as `system:mqtt-connection`.
//!
//! [`MqttConnectionHub::begin`] publishes a client's current state and starts
//! its follower, a task that publishes every later state the client's
//! supervisor records that differs from the last one published.
//! [`MqttConnectionHub::finish`] ends the follower and then publishes
//! `stopped`, so `stopped` is the newest message of a client whose session
//! ended in this process, and [`MqttConnectionHub::previous_end`] reads that
//! back at the next boot.
//!
//! A follower is ended by a signal and a join, never an abort: a publish it
//! has begun commits or is refused before it ends, so a client has one writer
//! at a time. A follower runs under [`abort_on_panic`]: a refusal that is a
//! host bug ends the process where it happens, never leaving a client's state
//! channel frozen on its last word.
//!
//! [`MqttConnectionHub::finish_all`] makes the hub stopping: a reload that
//! commits beside the shutdown still meets every client it adds, replaces or
//! removes. A client begun on a stopping hub is born stopped, and a replace or
//! removal of a client the stopping hub has already finished does nothing.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brenn_common::sanitize_untrusted_str;
use brenn_envelope::mqtt_connection::{
    Connection, MQTT_CONNECTION_SENDER, MQTT_CONNECTION_TEXT_MAX_BYTES, MqttConnectionState,
    PreviousEnd, PreviousSession, decode, encode, max_encoded_len,
};
use brenn_lib::messaging::config::{Depth, MQTT_CONNECTION_SEND_RATE};
use brenn_lib::messaging::mqtt_connection_address;
use brenn_lib::panic_util::abort_on_panic;
use brenn_messaging::mqtt_connection::MQTT_CONNECTION_COMPONENT;
use brenn_messaging::{Messenger, PublishResult, Urgency};
use brenn_mqtt::state::{StateAt, SupervisorState};
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};
use chrono::Utc;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

#[cfg(test)]
mod tests;

const HUB_POISONED: &str = "MQTT connection hub lock poisoned";

/// The per-client connection-state writers.
pub struct MqttConnectionHub {
    messenger: Arc<Messenger>,
    alerts: AlertDispatcher,
    clients: Mutex<Clients>,
}

/// The hub's clients, and whether [`MqttConnectionHub::finish_all`] has begun.
#[derive(Default)]
struct Clients {
    open: HashMap<String, Client>,
    /// Set by `finish_all`; never cleared.
    stopping: bool,
    /// Every client the stopping hub has finished or begun stopped: the ones a
    /// reload may still replace or remove, to no effect.
    finished: HashSet<String>,
}

impl Clients {
    /// Take `client` out to replace or remove it; `None` when the stopping
    /// hub has already finished it.
    ///
    /// # Panics
    ///
    /// If the client is neither open nor finished by the stopping hub.
    fn take(&mut self, client: &str) -> Option<Client> {
        if let Some(open) = self.open.remove(client) {
            return Some(open);
        }
        assert!(
            self.stopping && self.finished.contains(client),
            "mqtt_connection: client {client:?} is not open"
        );
        None
    }
}

/// One open client: how its previous session ended, and its follower.
struct Client {
    previous: PreviousSession,
    follower: Follower,
}

/// A running follower and the way to end it.
struct Follower {
    stop: oneshot::Sender<()>,
    /// Yields the last body the follower published when it ends.
    task: JoinHandle<String>,
    /// Publishes the send rate refused, over this client's life in the
    /// process.
    rate_refused: Arc<AtomicU64>,
}

impl MqttConnectionHub {
    pub fn new(messenger: Arc<Messenger>, alerts: AlertDispatcher) -> Arc<Self> {
        Arc::new(Self {
            messenger,
            alerts,
            clients: Mutex::new(Clients::default()),
        })
    }

    /// How `client`'s session before this process ended, read from the
    /// newest message on its connection-state channel. `resumed` is whether
    /// this process resumed the client's egress position rather than creating
    /// it: a created position has handed nothing, so whatever the channel
    /// holds is not this client's record.
    ///
    /// A record this build cannot decode is `Unclean`: the record is clean
    /// only when it says so. The operator is told.
    ///
    /// # Panics
    ///
    /// If the newest message is not from `system:mqtt-connection`: no
    /// configuration can grant a publish into the family, so another sender
    /// is a host bug, and believing it could suppress a restart marker.
    pub async fn previous_end(&self, client: &str, resumed: bool) -> PreviousEnd {
        if !resumed {
            return PreviousEnd::None;
        }
        let address = mqtt_connection_address(client);
        let tail = self
            .messenger
            .store_for_address(&address)
            .retained_tail(Depth::Bounded(1))
            .await;
        let Some(newest) = tail.last() else {
            return PreviousEnd::None;
        };
        assert_eq!(
            newest.sender, MQTT_CONNECTION_SENDER,
            "the newest message on {address} was published by `{}`, not by \
             {MQTT_CONNECTION_SENDER}; nothing may be granted a publish there",
            newest.sender,
        );
        let decoded = match newest.body.as_text() {
            Some(text) => decode(text).map_err(|e| e.to_string()),
            None => Err(format!("the body is not text ({})", newest.body.summary())),
        };
        match decoded {
            Ok(MqttConnectionState {
                connection: Connection::Stopped,
                ..
            }) => PreviousEnd::Clean,
            Ok(_) => PreviousEnd::Unclean,
            Err(why) => {
                tracing::error!(
                    client,
                    channel = %address,
                    error = %why,
                    "mqtt_connection: the connection-state record does not decode; reading the \
                     previous session as ended uncleanly",
                );
                self.alerts.alert(
                    AlertSeverity::Critical,
                    format!("MQTT connection record unreadable: {client}"),
                    format!(
                        "The newest message on {address} could not be decoded: {why}. The \
                         previous session of MQTT client '{client}' is treated as ended \
                         uncleanly, and senders that named a status_to on its unreported \
                         messages are told of a restart."
                    ),
                );
                PreviousEnd::Unclean
            }
        }
    }

    /// Log and alert that `client`'s previous session ended uncleanly, and
    /// that `told` status channels were told of the restart.
    pub fn report_unclean(&self, client: &str, told: u64) {
        tracing::warn!(
            mqtt_client = client,
            status_channels_told = told,
            "MQTT client's previous session ended uncleanly; told the status channels named \
             behind its egress position"
        );
        self.alerts.alert(
            AlertSeverity::Warning,
            format!("MQTT client restarted uncleanly: {client}"),
            format!(
                "The previous session of MQTT client '{client}' ended without its stop. Messages \
                 it handed may or may not have arrived; {told} status channel(s) were told of \
                 the restart."
            ),
        );
    }

    /// Publish `client`'s current state with `previous`, then follow `rx`.
    ///
    /// On a stopping hub the client is born stopped: its `stopped` is
    /// published in place of its current state, and nothing follows `rx`. Its
    /// session, registered after `MqttService::stop_all`, hands nothing.
    ///
    /// # Panics
    ///
    /// If the client is already open, if the longest state body for `client`
    /// exceeds the bus's `max_body_bytes`, or if the first publish is refused.
    /// That write is what makes the record say "not stopped" before the
    /// client's reader hands anything; were it lost, a crash would leave a
    /// `stopped` record over handed messages and the next boot would tell
    /// nobody.
    pub async fn begin(
        &self,
        client: &str,
        mut rx: watch::Receiver<StateAt>,
        previous: PreviousSession,
    ) {
        let stopping = {
            let clients = self.clients.lock().expect(HUB_POISONED);
            assert!(
                !clients.open.contains_key(client),
                "mqtt_connection: client {client:?} begun twice"
            );
            clients.stopping
        };
        let longest = max_encoded_len(client);
        let max = self.messenger.max_body_bytes();
        assert!(
            longest <= max,
            "mqtt_connection: a connection-state body for client {client:?} can be {longest} \
             bytes, over [messaging] max_body_bytes ({max}); raise max_body_bytes to at least \
             {longest}"
        );
        if stopping {
            self.stop_finished(client, previous).await;
            return;
        }
        let body = state_body(client, &rx.borrow_and_update(), previous);
        let outcome = self.publish(client, &body).await;
        assert!(
            matches!(outcome, PublishResult::Ok { .. }),
            "mqtt_connection: the first connection-state publish for client {client:?} was \
             refused: {outcome:?}"
        );
        self.open_or_stop(client, rx, previous, body).await;
    }

    /// Follow `rx`, a successor session's state, in place of the receiver the
    /// client's follower holds. The predecessor's follower has ended before
    /// the successor's starts. Does nothing for a client the stopping hub has
    /// finished; a client the hub turns stopping during the hand-over is
    /// finished here.
    ///
    /// # Panics
    ///
    /// If the client is neither open nor finished by the stopping hub.
    pub async fn follow(&self, client: &str, rx: watch::Receiver<StateAt>) {
        let Some(Client { previous, follower }) = self.take(client) else {
            return;
        };
        let last = end_follower(follower).await;
        self.open_or_stop(client, rx, previous, last).await;
    }

    /// End the client's follower and then publish `stopped`. A refused
    /// `stopped` is logged and alerted: the record keeps its last state, and
    /// the next boot reads the session as ended uncleanly. Does nothing for a
    /// client the stopping hub has finished.
    ///
    /// # Panics
    ///
    /// If the client is neither open nor finished by the stopping hub.
    pub async fn finish(&self, client: &str) {
        let Some(Client { previous, follower }) = self.take(client) else {
            return;
        };
        end_follower(follower).await;
        self.publish_stopped(client, previous).await;
    }

    /// Make the hub stopping, then [`Self::finish`] every client open. A
    /// client a reload begins afterwards is born stopped.
    pub async fn finish_all(&self) {
        let open: Vec<(String, Client)> = {
            let mut clients = self.clients.lock().expect(HUB_POISONED);
            clients.stopping = true;
            let open: Vec<_> = clients.open.drain().collect();
            clients
                .finished
                .extend(open.iter().map(|(client, _)| client.clone()));
            open
        };
        for (client, Client { previous, follower }) in open {
            end_follower(follower).await;
            self.publish_stopped(&client, previous).await;
        }
    }

    /// Start a follower on `rx` and open the client, unless the hub has
    /// turned stopping, in which case the client is finished instead.
    async fn open_or_stop(
        &self,
        client: &str,
        rx: watch::Receiver<StateAt>,
        previous: PreviousSession,
        last: String,
    ) {
        {
            let mut clients = self.clients.lock().expect(HUB_POISONED);
            if !clients.stopping {
                let follower = self.spawn_follower(client, rx, previous, last);
                clients
                    .open
                    .insert(client.to_string(), Client { previous, follower });
                return;
            }
        }
        self.stop_finished(client, previous).await;
    }

    /// Record `client` as finished by the stopping hub, then publish its
    /// `stopped`.
    async fn stop_finished(&self, client: &str, previous: PreviousSession) {
        self.clients
            .lock()
            .expect(HUB_POISONED)
            .finished
            .insert(client.to_string());
        self.publish_stopped(client, previous).await;
    }

    /// Publish `client`'s `stopped`; a refusal is logged at `error` and
    /// alerted.
    async fn publish_stopped(&self, client: &str, previous: PreviousSession) {
        let body = encode(&MqttConnectionState {
            client: client.to_string(),
            connection: Connection::Stopped,
            since_ms: Utc::now().timestamp_millis(),
            previous_session: previous,
        });
        let outcome = self.publish(client, &body).await;
        if !matches!(outcome, PublishResult::Ok { .. }) {
            tracing::error!(
                client,
                outcome = ?outcome,
                "mqtt_connection: the `stopped` connection state was refused; the next boot \
                 reads this client's session as ended uncleanly",
            );
            self.alerts.alert(
                AlertSeverity::Warning,
                format!("MQTT connection state not stopped: {client}"),
                format!(
                    "The `stopped` connection state of MQTT client '{client}' could not be \
                     published: {outcome:?}. The next boot reads its session as ended \
                     uncleanly, and senders that named a status_to on its last messages are \
                     told of a restart they need not have been."
                ),
            );
        }
    }

    /// How many state publishes the send rate has refused for `client` in
    /// this process.
    ///
    /// # Panics
    ///
    /// If the client is not open.
    pub fn rate_refused(&self, client: &str) -> u64 {
        self.clients
            .lock()
            .expect(HUB_POISONED)
            .open
            .get(client)
            .unwrap_or_else(|| panic!("mqtt_connection: client {client:?} is not open"))
            .follower
            .rate_refused
            .load(Ordering::Relaxed)
    }

    fn take(&self, client: &str) -> Option<Client> {
        self.clients.lock().expect(HUB_POISONED).take(client)
    }

    async fn publish(&self, client: &str, body: &str) -> PublishResult {
        publish(&self.messenger, client, body).await
    }

    fn spawn_follower(
        &self,
        client: &str,
        rx: watch::Receiver<StateAt>,
        previous: PreviousSession,
        last: String,
    ) -> Follower {
        let (stop, stopped) = oneshot::channel();
        let rate_refused = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(abort_on_panic(
            "MQTT connection-state follower panicked",
            follow(
                FollowerCtx {
                    messenger: Arc::clone(&self.messenger),
                    alerts: self.alerts.clone(),
                    client: client.to_string(),
                    previous,
                    rate_refused: Arc::clone(&rate_refused),
                },
                rx,
                stopped,
                last,
            ),
        ));
        Follower {
            stop,
            task,
            rate_refused,
        }
    }
}

/// Signal `follower` to stop and wait for it; returns the last body it
/// published.
///
/// # Panics
///
/// If the follower did not return. A follower's panic aborts the process
/// where it happens, so this is a cancelled task.
async fn end_follower(follower: Follower) -> String {
    // The receiver is gone only when the task has already ended, which the
    // join reports.
    follower.stop.send(()).ok();
    follower
        .task
        .await
        .unwrap_or_else(|e| panic!("mqtt_connection: a follower task did not return: {e}"))
}

async fn publish(messenger: &Messenger, client: &str, body: &str) -> PublishResult {
    messenger
        .publish_from_system(
            MQTT_CONNECTION_COMPONENT,
            &mqtt_connection_address(client),
            body.to_string().into(),
            Urgency::Normal,
            None,
        )
        .await
}

/// The wire state for a supervisor state. `stopped` is never mapped: only
/// [`MqttConnectionHub::finish`] writes it.
fn state_body(client: &str, at: &StateAt, previous: PreviousSession) -> String {
    let connection = match &at.state {
        SupervisorState::Connected => Connection::Connected,
        SupervisorState::Disconnected { last_error }
        | SupervisorState::Connecting { last_error } => Connection::Disconnected {
            last_error: last_error.as_deref().map(sanitize),
        },
        SupervisorState::Failed { reason } => Connection::Failed {
            reason: sanitize(reason),
        },
    };
    encode(&MqttConnectionState {
        client: client.to_string(),
        connection,
        since_ms: at.since.timestamp_millis(),
        previous_session: previous,
    })
}

fn sanitize(text: &str) -> String {
    sanitize_untrusted_str(text, MQTT_CONNECTION_TEXT_MAX_BYTES)
}

/// What a follower carries besides its receiver.
struct FollowerCtx {
    messenger: Arc<Messenger>,
    alerts: AlertDispatcher,
    client: String,
    previous: PreviousSession,
    rate_refused: Arc<AtomicU64>,
}

/// What a follower does after a state publish.
#[derive(Debug, PartialEq, Eq)]
enum Published {
    Committed,
    /// The send rate refused it: try again at the next change or after the
    /// rate's refill interval.
    RateLimited,
}

/// Class a follower's publish outcome.
///
/// # Panics
///
/// On every refusal but the send rate's. The participant's policy is
/// code-built, its address minted, its body bounded by the check in
/// [`MqttConnectionHub::begin`], and its channel is in the directory for as
/// long as the follower runs; each other refusal is a host bug.
fn class_outcome(client: &str, outcome: PublishResult) -> Published {
    match outcome {
        PublishResult::Ok { .. } => Published::Committed,
        PublishResult::RateLimited => Published::RateLimited,
        other => panic!(
            "mqtt_connection: a connection-state publish for client {client:?} was refused: \
             {other:?}"
        ),
    }
}

/// Publish every state on `rx` that differs from the last body published,
/// until `stopped` fires; returns the last body published.
///
/// The stop is raced against the watch and the retry wait, never against a
/// publish. A state the send rate refused is retried at the next change of
/// the watch or after the rate's refill interval, whichever comes first, as
/// whatever the newest state then is.
async fn follow(
    ctx: FollowerCtx,
    mut rx: watch::Receiver<StateAt>,
    mut stopped: oneshot::Receiver<()>,
    mut last: String,
) -> String {
    let retry_after = Duration::from_secs(MQTT_CONNECTION_SEND_RATE.refill_interval_secs);
    let mut refusing = false;
    let mut watch_open = true;
    loop {
        let body = state_body(&ctx.client, &rx.borrow_and_update(), ctx.previous);
        // When to try again a state the send rate refused.
        let mut retry_at: Option<tokio::time::Instant> = None;
        if body != last {
            let outcome = publish(&ctx.messenger, &ctx.client, &body).await;
            match class_outcome(&ctx.client, outcome) {
                Published::Committed => {
                    last = body;
                    refusing = false;
                }
                Published::RateLimited => {
                    ctx.rate_refused.fetch_add(1, Ordering::Relaxed);
                    retry_at = Some(tokio::time::Instant::now() + retry_after);
                    tracing::error!(
                        client = %ctx.client,
                        "mqtt_connection: the send rate refused a connection-state publish; \
                         retrying at the next change or after the refill interval",
                    );
                    if !refusing {
                        refusing = true;
                        ctx.alerts.alert(
                            AlertSeverity::Warning,
                            format!("MQTT connection state rate-limited: {}", ctx.client),
                            format!(
                                "A connection-state publish for MQTT client '{}' was refused \
                                 by the send rate. Its channel's newest message may be stale \
                                 for up to {} s after the client's state stops changing.",
                                ctx.client, MQTT_CONNECTION_SEND_RATE.refill_interval_secs,
                            ),
                        );
                    }
                }
            }
        }
        tokio::select! {
            biased;
            _ = &mut stopped => return last,
            changed = rx.changed(), if watch_open => {
                // The sender is on the client's handle: once it is gone no
                // state follows, and only the stop or a retry remains.
                if changed.is_err() {
                    watch_open = false;
                }
            }
            () = sleep_until(retry_at), if retry_at.is_some() => {}
        }
    }
}

async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
