//! The hub over a real `Messenger` and durable connection-state channel: the
//! first publish, the follower's publishes, coalescing and dedup, the send
//! rate's refusal and retry, `stopped` as the last word, the hand-over to a
//! successor's receiver, and the boot read.

use std::time::Duration;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;

use brenn_envelope::{ChannelScheme, MessageBody, Urgency as EnvelopeUrgency};
use brenn_lib::messaging::MessagingDirectory;
use brenn_lib::messaging::config::{MessagingGlobalConfig, SendRate, SystemChannelTuning};
use brenn_messaging::WakeRouter;
use brenn_messaging::mqtt_connection::{mqtt_connection_entry, mqtt_connection_spec};
use brenn_messaging::query::NoopWakeRouter;
use brenn_messaging::system::registrations_from_specs;
use brenn_messaging_store::db::{
    init_db_memory, load_channel_retained_tail, upsert_channels, utc_to_ns,
};
use brenn_messaging_store::store::NewMessage;
use brenn_obs::alerting::make_capturing_alerter_with_severity;

use super::*;

const CLIENT: &str = "home";
const ADDRESS: &str = "brenn:mqtt-connection.home";

type Captured = Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>;

struct Fixture {
    messenger: Arc<Messenger>,
    hub: Arc<MqttConnectionHub>,
    alerts: AlertDispatcher,
    captured: Captured,
}

impl Fixture {
    fn new() -> Self {
        Self::build(
            Some(MQTT_CONNECTION_SEND_RATE),
            MessagingGlobalConfig::default(),
        )
    }

    fn with_rate(send_rate: SendRate) -> Self {
        Self::build(Some(send_rate), MessagingGlobalConfig::default())
    }

    /// The fixture over `defaults`, with the client's channel at `send_rate`,
    /// or with no channel when `None`.
    fn build(send_rate: Option<SendRate>, defaults: MessagingGlobalConfig) -> Self {
        let entries: Vec<_> = send_rate
            .map(|rate| {
                let mut entry =
                    mqtt_connection_entry(CLIENT, &SystemChannelTuning::default(), &defaults);
                entry.resolved_channel.send_rate = rate;
                entry
            })
            .into_iter()
            .collect();
        let db = init_db_memory();
        upsert_channels(
            &db.try_lock()
                .expect("a fresh in-memory db is uniquely owned"),
            &entries,
        );
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test"),
            Arc::new(IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            defaults,
        )
        .with_subscriber_registrations(registrations_from_specs(&[mqtt_connection_spec()]));
        let (alerts, captured, _drainer) = make_capturing_alerter_with_severity();
        let hub = MqttConnectionHub::new(Arc::clone(&messenger), alerts.clone());
        Self {
            messenger,
            hub,
            alerts,
            captured,
        }
    }

    /// Every state on the channel, oldest first, each checked to come from
    /// the writer.
    async fn states(&self) -> Vec<MqttConnectionState> {
        let entry = self.messenger.directory().resolve(ADDRESS).unwrap();
        let conn = self.messenger.db().lock().await;
        load_channel_retained_tail(&conn, entry.uuid, Depth::Unbounded)
            .into_iter()
            .map(|(_, envelope)| {
                assert_eq!(envelope.sender, MQTT_CONNECTION_SENDER);
                decode(envelope.body.as_text().unwrap()).unwrap()
            })
            .collect()
    }

    async fn connections(&self) -> Vec<Connection> {
        self.states()
            .await
            .into_iter()
            .map(|state| state.connection)
            .collect()
    }

    /// Wait up to five seconds for the channel's newest state to be
    /// `connection`.
    async fn wait_newest(&self, connection: &Connection) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.connections().await.last() == Some(connection) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("the newest state never became {connection:?}");
        });
    }

    async fn alerts(&self) -> Vec<(AlertSeverity, String, String)> {
        self.alerts.flush().await;
        self.captured.lock().unwrap().clone()
    }

    /// Append `body` to the channel as `sender`, as a stored message.
    async fn append(&self, sender: &str, body: MessageBody) {
        self.messenger
            .store_for_address(ADDRESS)
            .append(NewMessage {
                source: "test".to_string(),
                sender: sender.to_string(),
                body,
                urgency: EnvelopeUrgency::Normal,
                envelope_type: ChannelScheme::Brenn,
                transport: None,
                reply_to: None,
                delivery_deadline: None,
                impetus: None,
                publish_ts_ns: utc_to_ns(Utc::now()),
                attachments: brenn_messaging_store::blob::HeldAttachments::none(),
            })
            .await;
    }
}

const PREVIOUS: PreviousSession = PreviousSession {
    end: PreviousEnd::Unclean,
    found_at_ms: 1_795_599_990_000,
};

fn at(state: SupervisorState, since: DateTime<Utc>) -> StateAt {
    StateAt { state, since }
}

fn disconnected(last_error: Option<&str>) -> SupervisorState {
    SupervisorState::Disconnected {
        last_error: last_error.map(str::to_string),
    }
}

fn wire_disconnected(last_error: Option<&str>) -> Connection {
    Connection::Disconnected {
        last_error: last_error.map(str::to_string),
    }
}

/// `tx`'s state replaced by `state`, its `since` kept.
fn set(tx: &watch::Sender<StateAt>, state: SupervisorState) {
    tx.send_modify(|current| current.state = state);
}

// --- begin and the follower ---

/// `begin` publishes the current state with `previous` and the state's
/// `since`; each later change of kind or error is published once, and a
/// change that maps to the body already published is not.
#[tokio::test]
async fn the_follower_publishes_each_change_and_no_repeat() {
    let fx = Fixture::new();
    let since = Utc::now();
    let (tx, rx) = watch::channel(at(disconnected(None), since));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;

    let first = fx.states().await;
    assert_eq!(
        first,
        [MqttConnectionState {
            client: CLIENT.to_string(),
            connection: wire_disconnected(None),
            since_ms: since.timestamp_millis(),
            previous_session: PREVIOUS,
        }]
    );

    // A connect attempt with no error maps to the body already published.
    set(&tx, SupervisorState::Connecting { last_error: None });
    set(&tx, SupervisorState::Connected);
    fx.wait_newest(&Connection::Connected).await;
    set(&tx, disconnected(Some("refused")));
    fx.wait_newest(&wire_disconnected(Some("refused"))).await;
    set(
        &tx,
        SupervisorState::Connecting {
            last_error: Some("refused".to_string()),
        },
    );
    set(
        &tx,
        SupervisorState::Failed {
            reason: "bad credentials".to_string(),
        },
    );
    let failed = Connection::Failed {
        reason: "bad credentials".to_string(),
    };
    fx.wait_newest(&failed).await;
    // The supervisor's terminal tail rewrites the same `Failed`.
    set(
        &tx,
        SupervisorState::Failed {
            reason: "bad credentials".to_string(),
        },
    );
    fx.hub.finish(CLIENT).await;

    assert_eq!(
        fx.connections().await,
        [
            wire_disconnected(None),
            Connection::Connected,
            wire_disconnected(Some("refused")),
            failed,
            Connection::Stopped,
        ]
    );
    assert!(
        fx.states()
            .await
            .iter()
            .all(|state| state.previous_session == PREVIOUS && state.client == CLIENT)
    );
    assert!(fx.alerts().await.is_empty());
}

/// The broker's words are sanitized and capped.
#[tokio::test]
async fn error_text_is_sanitized_and_capped() {
    let fx = Fixture::new();
    let (tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
    let raw = format!(
        "bad\u{1b}[31m{}",
        "x".repeat(2 * MQTT_CONNECTION_TEXT_MAX_BYTES)
    );
    set(
        &tx,
        SupervisorState::Failed {
            reason: raw.clone(),
        },
    );
    let expected = Connection::Failed {
        reason: sanitize_untrusted_str(&raw, MQTT_CONNECTION_TEXT_MAX_BYTES),
    };
    fx.wait_newest(&expected).await;
    let Connection::Failed { reason } = expected else {
        unreachable!()
    };
    assert!(!reason.contains('\u{1b}'));
    assert!(reason.len() < raw.len());
    fx.hub.finish(CLIENT).await;
}

/// A burst of states the follower has no chance to run between is published
/// as its newest.
#[tokio::test]
async fn a_burst_is_coalesced_to_the_newest() {
    let fx = Fixture::new();
    let (tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
    for i in 0..20 {
        set(&tx, disconnected(Some(&format!("attempt {i}"))));
    }
    set(&tx, SupervisorState::Connected);
    fx.wait_newest(&Connection::Connected).await;
    fx.hub.finish(CLIENT).await;
    assert_eq!(
        fx.connections().await,
        [
            wire_disconnected(None),
            Connection::Connected,
            Connection::Stopped
        ]
    );
}

/// A state the send rate refuses does not stop the server: it is counted,
/// alerted once per episode, and the newest state is on the channel once
/// the rate's interval has passed.
#[tokio::test]
async fn a_rate_refusal_is_retried_and_alerted_once() {
    let fx = Fixture::with_rate(SendRate {
        burst: 1,
        refill_interval_secs: 1,
        refill: 1,
    });
    let (tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;

    set(&tx, SupervisorState::Connected);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fx.hub.rate_refused(CLIENT) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first change was not refused");
    set(&tx, disconnected(Some("lost")));
    tokio::time::timeout(Duration::from_secs(5), async {
        while fx.hub.rate_refused(CLIENT) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the second change was not refused");

    fx.wait_newest(&wire_disconnected(Some("lost"))).await;
    assert_eq!(
        fx.connections().await,
        [wire_disconnected(None), wire_disconnected(Some("lost"))],
        "the refused `connected` was superseded before the retry"
    );
    let alerts = fx.alerts().await;
    assert_eq!(alerts.len(), 1, "one alert per episode: {alerts:?}");
    assert!(matches!(alerts[0].0, AlertSeverity::Warning));
    assert_eq!(alerts[0].1, "MQTT connection state rate-limited: home");
}

/// Every refusal but the send rate's is a host bug.
#[test]
fn a_follower_refusal_other_than_the_rate_panics() {
    assert_eq!(
        class_outcome(CLIENT, PublishResult::RateLimited),
        Published::RateLimited
    );
    for refusal in [
        PublishResult::UnknownChannel(ADDRESS.to_string()),
        PublishResult::MissingSender,
        PublishResult::AclDenied(ADDRESS.to_string()),
        PublishResult::MalformedAddress(ADDRESS.to_string()),
        PublishResult::BodyTooLarge { len: 2, max: 1 },
    ] {
        let shown = format!("{refusal:?}");
        let caught = std::panic::catch_unwind(|| class_outcome(CLIENT, refusal));
        assert!(caught.is_err(), "{shown} did not panic");
    }
}

/// The first publish is the write that makes the record say "not stopped":
/// its refusal stops the boot.
#[tokio::test]
#[should_panic(expected = "the first connection-state publish for client \"home\" was refused")]
async fn begin_panics_on_a_refusal() {
    let fx = Fixture::build(None, MessagingGlobalConfig::default());
    let (_tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
}

/// A body cap below the longest state body stops the boot with the remedy.
#[tokio::test]
#[should_panic(expected = "raise max_body_bytes to at least")]
async fn begin_panics_on_a_body_cap_below_a_state() {
    let fx = Fixture::build(
        Some(MQTT_CONNECTION_SEND_RATE),
        MessagingGlobalConfig {
            max_body_bytes: 64,
            ..MessagingGlobalConfig::default()
        },
    );
    let (_tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
}

/// A refused `stopped` is logged and alerted, never a panic; the record
/// keeps its last state.
#[tokio::test]
async fn a_refused_stopped_is_alerted() {
    let fx = Fixture::with_rate(SendRate {
        burst: 1,
        refill_interval_secs: 3600,
        refill: 1,
    });
    let (_tx, rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
    fx.hub.finish(CLIENT).await;
    assert_eq!(fx.connections().await, [Connection::Connected]);
    let alerts = fx.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0].1, "MQTT connection state not stopped: home");
    assert!(alerts[0].2.contains("RateLimited"));
}

/// With the watch changing while `finish` runs, `stopped` is always the
/// channel's newest message, and nothing follows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_is_the_last_word_while_the_state_changes() {
    for _ in 0..40 {
        let fx = Fixture::with_rate(SendRate {
            burst: 100_000,
            refill_interval_secs: 1,
            refill: 100_000,
        });
        let (tx, rx) = watch::channel(at(disconnected(None), Utc::now()));
        fx.hub.begin(CLIENT, rx, PREVIOUS).await;
        let tx = Arc::new(tx);
        let flipping = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flipper = tokio::spawn({
            let tx = Arc::clone(&tx);
            let flipping = Arc::clone(&flipping);
            async move {
                let mut i = 0u64;
                while flipping.load(Ordering::Relaxed) {
                    let state = if i.is_multiple_of(2) {
                        SupervisorState::Connected
                    } else {
                        disconnected(Some(&format!("lost {i}")))
                    };
                    set(&tx, state);
                    i += 1;
                    tokio::task::yield_now().await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(2)).await;
        fx.hub.finish(CLIENT).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        flipping.store(false, Ordering::Relaxed);
        flipper.await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(fx.connections().await.last(), Some(&Connection::Stopped));
    }
}

/// `finish_all` stops every open client.
#[tokio::test]
async fn finish_all_stops_every_client() {
    let fx = Fixture::new();
    let (_tx, rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
    fx.hub.finish_all().await;
    assert_eq!(
        fx.connections().await,
        [Connection::Connected, Connection::Stopped]
    );
    fx.hub.finish_all().await;
    assert_eq!(fx.connections().await.len(), 2, "nothing is left open");
}

/// After `follow`, the successor's current state is published and nothing
/// from the predecessor's receiver is; `previous_session` is unchanged.
#[tokio::test]
async fn follow_hands_the_client_to_the_successors_receiver() {
    let fx = Fixture::new();
    let (old_tx, old_rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.begin(CLIENT, old_rx, PREVIOUS).await;
    set(&old_tx, disconnected(None));
    fx.wait_newest(&wire_disconnected(None)).await;

    let (new_tx, new_rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.follow(CLIENT, new_rx).await;
    fx.wait_newest(&Connection::Connected).await;
    set(
        &old_tx,
        SupervisorState::Failed {
            reason: "the predecessor".to_string(),
        },
    );
    set(&new_tx, disconnected(Some("the successor")));
    fx.wait_newest(&wire_disconnected(Some("the successor")))
        .await;
    fx.hub.finish(CLIENT).await;

    assert_eq!(
        fx.connections().await,
        [
            Connection::Connected,
            wire_disconnected(None),
            Connection::Connected,
            wire_disconnected(Some("the successor")),
            Connection::Stopped,
        ]
    );
    assert!(
        fx.states()
            .await
            .iter()
            .all(|state| state.previous_session == PREVIOUS)
    );
}

/// A client a reload begins on a stopping hub is born stopped: `stopped` is its
/// only state, with its `previous_session`, and no follower publishes after it.
#[tokio::test]
async fn a_client_begun_on_a_stopping_hub_is_born_stopped() {
    let fx = Fixture::new();
    fx.hub.finish_all().await;
    let (tx, rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.begin(CLIENT, rx, PREVIOUS).await;
    set(&tx, disconnected(Some("after the stop")));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let states = fx.states().await;
    assert_eq!(states.len(), 1, "{states:?}");
    assert_eq!(states[0].connection, Connection::Stopped);
    assert_eq!(states[0].previous_session, PREVIOUS);
    // A reload that removes it again on the stopping hub does nothing.
    fx.hub.finish(CLIENT).await;
    assert_eq!(fx.states().await.len(), 1);
}

/// A replace or removal of a client `finish_all` has finished does nothing: no
/// state follows the `stopped`.
#[tokio::test]
async fn a_reload_after_finish_all_leaves_stopped_last() {
    let fx = Fixture::new();
    let (_old_tx, old_rx) = watch::channel(at(SupervisorState::Connected, Utc::now()));
    fx.hub.begin(CLIENT, old_rx, PREVIOUS).await;
    fx.hub.finish_all().await;

    let (new_tx, new_rx) = watch::channel(at(disconnected(Some("successor")), Utc::now()));
    fx.hub.follow(CLIENT, new_rx).await;
    set(&new_tx, SupervisorState::Connected);
    fx.hub.finish(CLIENT).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fx.connections().await,
        [Connection::Connected, Connection::Stopped]
    );
}

/// A stopping hub still refuses a removal of a client it never held.
#[tokio::test]
#[should_panic(expected = "is not open")]
async fn a_stopping_hub_panics_on_a_client_it_never_held() {
    let fx = Fixture::new();
    fx.hub.finish_all().await;
    fx.hub.finish("elsewhere").await;
}

/// An unclean previous session is reported once, at `Warning`, with how many
/// status channels were told.
#[tokio::test]
async fn an_unclean_restart_is_alerted_with_the_told_count() {
    let fx = Fixture::new();
    fx.hub.report_unclean(CLIENT, 3);
    let alerts = fx.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert!(matches!(alerts[0].0, AlertSeverity::Warning));
    assert_eq!(alerts[0].1, "MQTT client restarted uncleanly: home");
    assert!(
        alerts[0].2.contains("3 status channel(s)"),
        "{}",
        alerts[0].2
    );
}

// --- the boot read ---

fn body(connection: Connection) -> MessageBody {
    encode(&MqttConnectionState {
        client: CLIENT.to_string(),
        connection,
        since_ms: 0,
        previous_session: PREVIOUS,
    })
    .into()
}

/// A created position is `none` whatever the channel holds; a resumed one
/// reads the newest message: none, `stopped`, any other state, a body that
/// does not decode.
#[tokio::test]
async fn previous_end_reads_the_newest_record() {
    let fx = Fixture::new();
    assert_eq!(fx.hub.previous_end(CLIENT, true).await, PreviousEnd::None);
    assert_eq!(fx.hub.previous_end(CLIENT, false).await, PreviousEnd::None);

    fx.append(MQTT_CONNECTION_SENDER, body(Connection::Stopped))
        .await;
    assert_eq!(fx.hub.previous_end(CLIENT, true).await, PreviousEnd::Clean);
    assert_eq!(fx.hub.previous_end(CLIENT, false).await, PreviousEnd::None);

    for connection in [
        Connection::Connected,
        wire_disconnected(Some("lost")),
        Connection::Failed {
            reason: "gone".to_string(),
        },
    ] {
        fx.append(MQTT_CONNECTION_SENDER, body(connection.clone()))
            .await;
        assert_eq!(
            fx.hub.previous_end(CLIENT, true).await,
            PreviousEnd::Unclean,
            "{connection:?}"
        );
        assert_eq!(fx.hub.previous_end(CLIENT, false).await, PreviousEnd::None);
    }
    assert!(fx.alerts().await.is_empty());

    fx.append(
        MQTT_CONNECTION_SENDER,
        r#"{"v":2,"connection":"stopped"}"#.into(),
    )
    .await;
    assert_eq!(
        fx.hub.previous_end(CLIENT, true).await,
        PreviousEnd::Unclean
    );
    let alerts = fx.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert!(matches!(alerts[0].0, AlertSeverity::Critical));
    assert_eq!(alerts[0].1, "MQTT connection record unreadable: home");
}

/// A record from any other sender is a host bug.
#[tokio::test]
#[should_panic(expected = "not by system:mqtt-connection")]
async fn previous_end_panics_on_a_foreign_record() {
    let fx = Fixture::new();
    fx.append("app:test:mallory", body(Connection::Stopped))
        .await;
    fx.hub.previous_end(CLIENT, true).await;
}
