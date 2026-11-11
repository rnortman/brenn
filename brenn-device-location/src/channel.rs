//! The messages a user's location channel, `brenn:device-location.<username>`,
//! carries, the reads the host makes over its retained window, and the
//! consent section every write passes through.
//!
//! A device's newest message decides whether it shares location: a `fix`
//! means sharing, a `stopped` means not, and a device with no message in the
//! window does not share. Only envelopes whose sender is
//! `system:device-location` are believed; an operator's `acl publish` matcher
//! can let another participant write to the channel, and what it writes is
//! skipped. No function here logs a coordinate.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};

use brenn_messaging::device_location::DEVICE_LOCATION_COMPONENT;
use brenn_messaging::{Messenger, ParticipantId, PublishResult, Urgency, device_location_address};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::validate::ValidFix;

/// The body of one message on a location channel: JSON, tagged by `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LocationMessage {
    /// A position report.
    Fix(FixFields),
    /// The device stopped sharing: the user's menu action or the browser
    /// withdrawing its permission. Newer than every fix before it.
    Stopped {
        /// The device's id, the key readers group by.
        device_id: i64,
        /// The device's display slug when the message was published.
        device: String,
    },
}

impl LocationMessage {
    /// The device the message is about.
    pub fn device_id(&self) -> i64 {
        match self {
            LocationMessage::Fix(fix) => fix.device_id,
            LocationMessage::Stopped { device_id, .. } => *device_id,
        }
    }
}

/// The fields of a `fix` message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixFields {
    /// The device's id, the key readers group by.
    pub device_id: i64,
    /// The device's display slug when the message was published.
    pub device: String,
    /// Decimal degrees north.
    pub latitude: f64,
    /// Decimal degrees east.
    pub longitude: f64,
    /// 68 % confidence radius in metres.
    pub accuracy_m: f64,
    /// The browser's IANA zone as reported with the fix, or `None` when
    /// chrono-tz did not know the reported name.
    pub time_zone: Option<String>,
}

/// A user's believed messages, oldest first, each with its envelope's
/// `publish_ts` (the server's receipt time).
pub type Window = Vec<(DateTime<Utc>, LocationMessage)>;

// TODO(efficiency-full-window-scan-per-fix-under-global-lock): answer
// "newest message per device" without loading the whole window.
/// Read `username`'s location channel: the retained tail up to the channel's
/// standing depth, keeping only messages from `system:device-location`.
///
/// Loads and parses the whole tail under the store's database lock, so its cost
/// grows with the window, not with the number of devices.
///
/// # Panics
///
/// If the user has no channel (every registered user has one), or if a
/// message from `system:device-location` does not parse (Brenn wrote it).
pub async fn read_window(messenger: &Messenger, username: &str) -> Window {
    let address = device_location_address(username);
    let standing = messenger
        .directory()
        .resolve(&address)
        .unwrap_or_else(|| panic!("device-location: no channel {address:?} for a registered user"))
        .resolved_channel
        .standing_retain_depth;
    let writer = ParticipantId::for_system(DEVICE_LOCATION_COMPONENT);
    let tail = messenger
        .store_for_address(&address)
        .retained_tail(standing)
        .await;
    let mut window = Vec::with_capacity(tail.len());
    let mut foreign: Vec<&str> = Vec::new();
    for envelope in &tail {
        if envelope.sender != writer.as_str() {
            foreign.push(envelope.sender.as_str());
            continue;
        }
        let parsed = match envelope.body.as_text() {
            Some(text) => serde_json::from_str::<LocationMessage>(text).map_err(|e| e.to_string()),
            None => Err(format!("body is not text ({})", envelope.body.summary())),
        };
        let message = parsed.unwrap_or_else(|e| {
            panic!(
                "device-location: message {} on {address:?} from {} does not parse: {e}",
                envelope.message_id,
                writer.as_str()
            )
        });
        window.push((envelope.publish_ts, message));
    }
    if !foreign.is_empty() {
        foreign.sort_unstable();
        foreign.dedup();
        warn!(
            channel = %address,
            senders = ?foreign,
            "device-location: skipped messages from senders other than the system writer"
        );
    }
    window
}

/// Whether `device_id` shares location: its newest message in `window` is a
/// fix.
pub fn is_sharing(window: &Window, device_id: i64) -> bool {
    matches!(
        window
            .iter()
            .rev()
            .find(|(_, m)| m.device_id() == device_id),
        Some((_, LocationMessage::Fix(_)))
    )
}

/// For each device whose newest message in `window` is a fix, that fix and
/// its receipt time.
pub fn newest_fix_per_device(window: &Window) -> BTreeMap<i64, (DateTime<Utc>, FixFields)> {
    let mut newest: BTreeMap<i64, Option<(DateTime<Utc>, FixFields)>> = BTreeMap::new();
    for (at, message) in window.iter().rev() {
        newest
            .entry(message.device_id())
            .or_insert_with(|| match message {
                LocationMessage::Fix(fix) => Some((*at, fix.clone())),
                LocationMessage::Stopped { .. } => None,
            });
    }
    newest
        .into_iter()
        .filter_map(|(id, fix)| fix.map(|f| (id, f)))
        .collect()
}

/// The browser's reported zone as a name chrono-tz knows, or `None` with a
/// `warn` for one it does not. Not a security event: a browser can be newer
/// than chrono-tz.
pub fn parse_zone(reported: &str) -> Option<String> {
    match reported.parse::<chrono_tz::Tz>() {
        Ok(tz) => Some(tz.name().to_string()),
        Err(_) => {
            warn!(time_zone = %reported, "device-location: zone chrono-tz does not know");
            None
        }
    }
}

/// What a device's connection asked to record.
#[derive(Debug, Clone)]
pub enum Report {
    /// `DeviceLocationShare`: turns sharing on with this fix.
    Share {
        fix: ValidFix,
        time_zone: Option<String>,
    },
    /// `DeviceLocationFix`: recorded only while the device shares.
    Update {
        fix: ValidFix,
        time_zone: Option<String>,
    },
    /// `DeviceLocationClear`: turns sharing off.
    Clear,
}

/// One lock per user's location channel, held across every read-decide-publish
/// on it, so a decision is never made against a window another writer is about
/// to change. Per user because a decision reads and writes only that user's
/// channel; one user's reports never wait on another's. Taken before any DB
/// lock and never while one is held. Holds one entry per user who has
/// reported since boot.
static CONSENT: LazyLock<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(Default::default);

/// `username`'s consent lock, created on first use.
fn consent_lock(username: &str) -> Arc<Mutex<()>> {
    CONSENT
        .lock()
        .expect("device-location: consent lock table poisoned")
        .entry(username.to_owned())
        .or_default()
        .clone()
}

/// Record `report` from `device_id`, paired with `user_id`/`username`, and
/// answer whether the device shares location afterwards.
///
/// Under the user's consent section: a `Share` publishes a fix; an `Update`
/// publishes one only while the device shares; a `Clear` publishes `stopped`
/// only while it shares, so repeated clears add nothing. A fix the channel's
/// send rate refuses is dropped with a `warn` and the answer is the window's
/// unchanged state. A `Clear` is always answered `false`, so the device stops
/// reporting; a `stopped` the rate refuses is logged at `error`, and the
/// channel still records the device as sharing.
///
/// # Panics
///
/// On any publish outcome but success or the rate limit: each is a host
/// invariant broken (the channel exists, the writer is registered, a body is
/// a few hundred bytes).
pub async fn record(
    messenger: &Messenger,
    db: &brenn_db::Db,
    username: &str,
    user_id: i64,
    device_id: i64,
    report: Report,
) -> bool {
    let consent = consent_lock(username);
    let _consent = consent.lock().await;
    let sharing = is_sharing(&read_window(messenger, username).await, device_id);
    let device = || async {
        let conn = db.lock().await;
        brenn_db::auth::device::display_slug(&conn, device_id, user_id)
    };
    let fix_message = |fix: ValidFix, time_zone: Option<String>, device: String| {
        LocationMessage::Fix(FixFields {
            device_id,
            device,
            latitude: fix.latitude(),
            longitude: fix.longitude(),
            accuracy_m: fix.accuracy_m(),
            time_zone,
        })
    };
    let message = match report {
        Report::Share { fix, time_zone } => fix_message(fix, time_zone, device().await),
        Report::Update { fix, time_zone } if sharing => fix_message(fix, time_zone, device().await),
        Report::Update { .. } => return false,
        Report::Clear if sharing => LocationMessage::Stopped {
            device_id,
            device: device().await,
        },
        Report::Clear => return false,
    };
    let is_fix = matches!(message, LocationMessage::Fix(_));
    match publish(messenger, username, &message).await {
        Published::Ok => is_fix,
        Published::RateLimited if is_fix => {
            warn!(
                user_id,
                device_id, "device-location: send rate refused a fix; dropped"
            );
            sharing
        }
        Published::RateLimited => {
            error!(
                user_id,
                device_id,
                "device-location: send rate refused a stop; the channel still records the \
                 device as sharing"
            );
            false
        }
    }
}

/// The outcome of [`publish`] a caller acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    Ok,
    RateLimited,
}

/// Publish `message` on `username`'s channel as `system:device-location`.
///
/// # Panics
///
/// On any outcome but success or the rate limit (see [`record`]).
pub async fn publish(
    messenger: &Messenger,
    username: &str,
    message: &LocationMessage,
) -> Published {
    let address = device_location_address(username);
    let body = serde_json::to_string(message).expect("a location message serializes");
    match messenger
        .publish_from_system(
            DEVICE_LOCATION_COMPONENT,
            &address,
            body.as_str().into(),
            Urgency::Normal,
            None,
        )
        .await
    {
        PublishResult::Ok { .. } => Published::Ok,
        PublishResult::RateLimited => Published::RateLimited,
        PublishResult::UnknownChannel(addr) => panic!(
            "device-location: {addr:?} does not resolve — every registered user has a location \
             channel"
        ),
        PublishResult::BodyTooLarge { len, max } => panic!(
            "device-location: a {len}-byte location message exceeds [messaging] max_body_bytes \
             ({max}) — the cap is below the size of one fix"
        ),
        other => panic!(
            "device-location: publishing on {address:?} failed ({other:?}) — the \
             {DEVICE_LOCATION_COMPONENT} system participant is registered wherever messaging is \
             and may publish on every user's location channel"
        ),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::validate::fix;

    pub(crate) const ALICE: i64 = 1;
    pub(crate) const PHONE: i64 = 2;
    const LAPTOP: i64 = 1;

    /// The seeded database (see `crate::db::seeded_db`) under a messenger with
    /// alice's and bob's channels.
    pub(crate) async fn bus() -> (brenn_db::Db, Arc<Messenger>) {
        let db = crate::db::seeded_db();
        let messenger = brenn_messaging::device_location::device_location_messenger(
            db.clone(),
            &["alice", "bob"],
        )
        .await;
        (db, messenger)
    }

    fn fix_at(device_id: i64, latitude: f64) -> LocationMessage {
        LocationMessage::Fix(FixFields {
            device_id,
            device: format!("d{device_id}"),
            latitude,
            longitude: 2.0,
            accuracy_m: 3.0,
            time_zone: None,
        })
    }

    fn stopped(device_id: i64) -> LocationMessage {
        LocationMessage::Stopped {
            device_id,
            device: format!("d{device_id}"),
        }
    }

    fn window(messages: &[LocationMessage]) -> Window {
        let at = Utc::now();
        messages.iter().map(|m| (at, m.clone())).collect()
    }

    #[test]
    fn bodies_round_trip_in_the_documented_shape() {
        let fix = LocationMessage::Fix(FixFields {
            device_id: 7,
            device: "phone".into(),
            latitude: 52.5,
            longitude: 13.4,
            accuracy_m: 25.0,
            time_zone: Some("Europe/Berlin".into()),
        });
        let json: serde_json::Value = serde_json::to_value(&fix).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "fix", "device_id": 7, "device": "phone", "latitude": 52.5,
                "longitude": 13.4, "accuracy_m": 25.0, "time_zone": "Europe/Berlin",
            })
        );
        assert_eq!(
            serde_json::from_value::<LocationMessage>(json).unwrap(),
            fix
        );

        let stop = stopped(7);
        let json = serde_json::to_value(&stop).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"kind": "stopped", "device_id": 7, "device": "d7"})
        );
        assert_eq!(
            serde_json::from_value::<LocationMessage>(json).unwrap(),
            stop
        );

        let null_zone: LocationMessage = serde_json::from_str(
            r#"{"kind":"fix","device_id":1,"device":"x","latitude":0,"longitude":0,"accuracy_m":1,"time_zone":null}"#,
        )
        .unwrap();
        assert!(matches!(
            null_zone,
            LocationMessage::Fix(FixFields {
                time_zone: None,
                ..
            })
        ));
    }

    #[test]
    fn unknown_fields_and_kinds_do_not_parse() {
        for body in [
            r#"{"kind":"fix","device_id":1,"device":"x","latitude":0,"longitude":0,"accuracy_m":1,"time_zone":null,"extra":1}"#,
            r#"{"kind":"stopped","device_id":1,"device":"x","extra":1}"#,
            r#"{"kind":"moved","device_id":1}"#,
        ] {
            assert!(
                serde_json::from_str::<LocationMessage>(body).is_err(),
                "{body}"
            );
        }
    }

    #[test]
    fn sharing_is_the_devices_newest_message_being_a_fix() {
        assert!(!is_sharing(&window(&[]), PHONE));
        assert!(is_sharing(&window(&[fix_at(PHONE, 1.0)]), PHONE));
        assert!(!is_sharing(
            &window(&[fix_at(PHONE, 1.0), stopped(PHONE)]),
            PHONE
        ));
        assert!(is_sharing(
            &window(&[stopped(PHONE), fix_at(PHONE, 1.0)]),
            PHONE
        ));
        // Another device's messages, interleaved, decide nothing for this one.
        let w = window(&[
            fix_at(PHONE, 1.0),
            stopped(LAPTOP),
            fix_at(LAPTOP, 2.0),
            stopped(PHONE),
            fix_at(LAPTOP, 3.0),
        ]);
        assert!(!is_sharing(&w, PHONE));
        assert!(is_sharing(&w, LAPTOP));
        assert!(!is_sharing(&w, 99));
    }

    #[test]
    fn newest_fix_per_device_skips_stopped_devices() {
        let w = window(&[
            fix_at(PHONE, 1.0),
            fix_at(LAPTOP, 2.0),
            fix_at(PHONE, 4.0),
            fix_at(3, 5.0),
            stopped(3),
        ]);
        let newest = newest_fix_per_device(&w);
        let got: Vec<(i64, f64)> = newest
            .iter()
            .map(|(id, (_, f))| (*id, f.latitude))
            .collect();
        assert_eq!(got, vec![(LAPTOP, 2.0), (PHONE, 4.0)]);
    }

    #[test]
    fn an_unknown_zone_is_none_and_a_known_one_is_its_canonical_name() {
        assert_eq!(
            parse_zone("Europe/Berlin").as_deref(),
            Some("Europe/Berlin")
        );
        assert_eq!(parse_zone("Not/AZone"), None);
    }

    #[tokio::test]
    async fn the_window_believes_only_the_system_writer() {
        let (_db, m) = bus().await;
        publish(&m, "alice", &fix_at(PHONE, 1.0)).await;
        // A message another participant managed to write: skipped, not fatal.
        let entry = m
            .directory()
            .resolve(&device_location_address("alice"))
            .unwrap();
        brenn_messaging::testutils::insert_bus_message(
            &m,
            &entry,
            r#"{"kind":"stopped","device_id":2,"device":"phone"}"#,
            brenn_messaging::ChannelScheme::Brenn,
        )
        .await;
        let w = read_window(&m, "alice").await;
        assert_eq!(w.len(), 1);
        assert!(is_sharing(&w, PHONE));
        assert!(
            read_window(&m, "bob").await.is_empty(),
            "channels are per user"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "does not parse")]
    async fn an_unparseable_body_from_the_system_writer_panics() {
        let (_db, m) = bus().await;
        let published = m
            .publish_from_system(
                DEVICE_LOCATION_COMPONENT,
                &device_location_address("alice"),
                r#"{"kind":"teleported"}"#.into(),
                Urgency::Normal,
                None,
            )
            .await;
        assert!(matches!(published, PublishResult::Ok { .. }));
        read_window(&m, "alice").await;
    }

    fn update() -> Report {
        Report::Update {
            fix: fix(5.0, 6.0, 7.0),
            time_zone: None,
        }
    }

    #[tokio::test]
    async fn record_follows_the_admission_rule() {
        let (db, m) = bus().await;
        let rec = |report| record(&m, &db, "alice", ALICE, PHONE, report);

        // Not sharing: a periodic fix and a clear publish nothing.
        assert!(!rec(update()).await);
        assert!(!rec(Report::Clear).await);
        assert!(read_window(&m, "alice").await.is_empty());

        assert!(
            rec(Report::Share {
                fix: fix(1.0, 2.0, 3.0),
                time_zone: Some("Europe/Berlin".into()),
            })
            .await
        );
        assert!(rec(update()).await);
        assert!(!rec(Report::Clear).await);
        assert!(!rec(Report::Clear).await, "a second clear adds nothing");
        assert!(!rec(update()).await, "a fix cannot turn sharing back on");

        let kinds: Vec<LocationMessage> = read_window(&m, "alice")
            .await
            .into_iter()
            .map(|(_, msg)| msg)
            .collect();
        assert_eq!(kinds.len(), 3);
        assert_eq!(
            kinds[0],
            LocationMessage::Fix(FixFields {
                device_id: PHONE,
                device: "phone".into(),
                latitude: 1.0,
                longitude: 2.0,
                accuracy_m: 3.0,
                time_zone: Some("Europe/Berlin".into()),
            })
        );
        assert!(matches!(&kinds[1], LocationMessage::Fix(f) if f.latitude == 5.0));
        assert_eq!(
            kinds[2],
            LocationMessage::Stopped {
                device_id: PHONE,
                device: "phone".into()
            }
        );
    }

    /// With the send rate exhausted: a refused share answers the unchanged
    /// state, and a refused stop answers `false` so the device stops
    /// reporting, while the channel still holds its fix as newest.
    #[tokio::test]
    async fn a_rate_limited_report_publishes_nothing() {
        let db = crate::db::seeded_db();
        let one_token = brenn_messaging::config::SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        };
        let m = brenn_messaging::device_location::device_location_messenger_at_rate(
            db.clone(),
            &["alice"],
            one_token,
        )
        .await;
        let rec = |report| record(&m, &db, "alice", ALICE, PHONE, report);
        let share = || Report::Share {
            fix: fix(1.0, 2.0, 3.0),
            time_zone: None,
        };

        assert!(rec(share()).await, "the one token admits the share");
        assert!(
            rec(update()).await,
            "a refused fix answers the unchanged state"
        );
        assert!(
            !rec(Report::Clear).await,
            "a refused stop still stops the device's reporting"
        );
        let w = read_window(&m, "alice").await;
        assert_eq!(w.len(), 1, "neither the fix nor the stop was published");
        assert!(is_sharing(&w, PHONE));

        // A share from a device that is not sharing, refused: answered false,
        // and nothing reaches the channel.
        let db2 = crate::db::seeded_db();
        let m2 = brenn_messaging::device_location::device_location_messenger_at_rate(
            db2.clone(),
            &["alice"],
            one_token,
        )
        .await;
        assert_eq!(
            publish(&m2, "alice", &stopped(LAPTOP)).await,
            Published::Ok,
            "spends the one token"
        );
        assert!(!record(&m2, &db2, "alice", ALICE, PHONE, share()).await);
        let w2 = read_window(&m2, "alice").await;
        assert_eq!(w2.len(), 1);
        assert!(!is_sharing(&w2, PHONE));
    }

    /// A clear and a fix from two connections of one device, both waiting on
    /// the consent section: whichever runs second decides against the
    /// first's message, so the stop is never overtaken.
    #[tokio::test]
    async fn a_clear_racing_a_fix_ends_stopped_in_both_orders() {
        for clear_first in [true, false] {
            let (db, m) = bus().await;
            assert!(
                record(
                    &m,
                    &db,
                    "alice",
                    ALICE,
                    PHONE,
                    Report::Share {
                        fix: fix(1.0, 2.0, 3.0),
                        time_zone: None,
                    }
                )
                .await
            );
            let lock = consent_lock("alice");
            let held = lock.lock().await;
            let spawn = |report: Report| {
                let (m, db) = (m.clone(), db.clone());
                tokio::spawn(async move { record(&m, &db, "alice", ALICE, PHONE, report).await })
            };
            let wait_until_queued = || async {
                // On this single-threaded runtime a spawned task runs only
                // when this one yields, and runs until it waits on the held
                // lock; a tokio mutex then hands out in queue order.
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
            };
            let (clear, fix_task) = if clear_first {
                let c = spawn(Report::Clear);
                wait_until_queued().await;
                let f = spawn(update());
                wait_until_queued().await;
                (c, f)
            } else {
                let f = spawn(update());
                wait_until_queued().await;
                let c = spawn(Report::Clear);
                wait_until_queued().await;
                (c, f)
            };
            drop(held);
            assert!(!clear.await.unwrap());
            let fix_answer = fix_task.await.unwrap();
            assert_eq!(
                fix_answer, !clear_first,
                "a fix after the clear is refused; one before it was admitted"
            );
            let w = read_window(&m, "alice").await;
            assert!(matches!(
                w.last(),
                Some((_, LocationMessage::Stopped { .. }))
            ));
            assert!(!is_sharing(&w, PHONE));
        }
    }
}
