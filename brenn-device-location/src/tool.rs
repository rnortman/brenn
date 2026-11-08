//! `device-location`: the acting user's sharing devices — position, age, zone
//! and local time.

use async_trait::async_trait;
use brenn_lib::tools::AclClause;
use brenn_tool_registry::{
    AclDenied, AsyncTool, Idempotency, ToolClass, ToolCtx, ToolDescriptor, ToolError, UserBinding,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use tracing::debug;

use crate::channel::{FixFields, newest_fix_per_device, read_window};
use crate::db::{UserDevice, list_user_devices};
use crate::{DeviceLocationBus, NAME, STALE_AFTER_SECONDS};

/// What the answer tells the assistant when no device shares location.
const HOW_TO_ENABLE: &str = "No device shares location. The user can turn it on from the user \
                             menu in the app: \"Share location from this device\".";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceLocationArgs {
    #[serde(default)]
    device: Option<String>,
}

/// The `device-location` tool.
pub(crate) struct DeviceLocationTool {
    descriptor: ToolDescriptor,
    db: brenn_db::Db,
    bus: DeviceLocationBus,
}

impl DeviceLocationTool {
    pub(crate) fn new(db: brenn_db::Db, bus: DeviceLocationBus) -> Self {
        Self {
            descriptor: ToolDescriptor {
                name: NAME,
                mcp_name: "mcp__brenn__DeviceLocation",
                description: "Where the user is now, from the devices on which they share \
                              location with Brenn: for each sharing device its name, latitude, \
                              longitude, accuracy in metres, when Brenn received the fix and how \
                              old it is (the browser may have served a fix up to five minutes \
                              older than that), the device's time zone and the current local \
                              time there. A fix is `stale` when older than 15 minutes; tell the \
                              user its age rather than treating it as current. Pass the \
                              coordinates to `Weather` as `lat`/`lng` and the zone to the \
                              Calendar tools. Empty `devices` means no device shares location: \
                              the user turns it on from the user menu in the app (\"Share \
                              location from this device\").",
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "device": {
                            "type": "string",
                            "description": "Only this device: its id, its guessed slug or \
                                            the user's assigned slug, as DeviceList shows \
                                            them. Omitted: every sharing device.",
                        },
                    },
                    "additionalProperties": false,
                }),
                class: ToolClass::Async { max_concurrency: 4 },
                acl_keys: &[],
                idempotency: Idempotency::Natural,
                auto_approve: true,
                user_binding: UserBinding::ActingUser,
            },
            db,
            bus,
        }
    }
}

/// A device the answer names: the user's row for it and its newest fix.
struct SharingDevice<'a> {
    device: &'a UserDevice,
    reported_at: DateTime<Utc>,
    fix: FixFields,
}

/// One answer entry for `sharing`, its age and zone taken at `now`.
fn entry(sharing: &SharingDevice<'_>, now: DateTime<Utc>) -> serde_json::Value {
    let age_seconds = (now - sharing.reported_at).num_seconds().max(0);
    // The zone the fix carried is a name chrono-tz parsed when Brenn
    // published it, so one that does not parse is a body Brenn cannot read.
    let reported_zone = sharing.fix.time_zone.as_deref().map(|name| {
        name.parse::<chrono_tz::Tz>().unwrap_or_else(|_| {
            panic!("device-location: a published fix carries zone {name:?}, which does not parse")
        })
    });
    let zone = brenn_db::auth::device::effective_timezone_opt(
        &sharing.device.device_user,
        reported_zone,
        now,
    );
    serde_json::json!({
        "device": sharing.device.display_slug,
        "latitude": sharing.fix.latitude,
        "longitude": sharing.fix.longitude,
        "accuracy_m": sharing.fix.accuracy_m,
        "reported_at": sharing.reported_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        "age_seconds": age_seconds,
        "stale": age_seconds > STALE_AFTER_SECONDS,
        "time_zone": zone.map(|tz| tz.name()),
        "local_time": zone.map(|tz| now.with_timezone(&tz).to_rfc3339_opts(SecondsFormat::Secs, false)),
    })
}

#[async_trait]
impl AsyncTool for DeviceLocationTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    /// The grant alone authorises: the tool answers only the acting user's
    /// devices, so there is nothing for a clause to narrow.
    fn check_acl(&self, _args: &serde_json::Value, _acl: &[AclClause]) -> Result<(), AclDenied> {
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let user = ctx.require_acting_user(NAME)?.clone();
        let args: DeviceLocationArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("{NAME} args: {e}")))?;
        let (devices, named) = {
            let conn = self.db.lock().await;
            let named = args.device.as_deref().map(|device| {
                brenn_db::auth::device::resolve_device_ids_for_get(&conn, device, &[user.id])
            });
            (list_user_devices(&conn, user.id), named)
        };
        let mut newest =
            newest_fix_per_device(&read_window(self.bus.messenger(), &user.username).await);
        // A device the user no longer has enrolled, or never paired, is not
        // answered, whatever its messages say.
        let mut sharing: Vec<SharingDevice<'_>> = devices
            .iter()
            .filter_map(|device| {
                newest
                    .remove(&device.device_user.device_id)
                    .map(|(reported_at, fix)| SharingDevice {
                        device,
                        reported_at,
                        fix,
                    })
            })
            .collect();
        sharing.sort_by(|a, b| {
            b.reported_at.cmp(&a.reported_at).then(
                a.device
                    .device_user
                    .device_id
                    .cmp(&b.device.device_user.device_id),
            )
        });
        if let (Some(device), Some(named)) = (args.device, named) {
            let names: Vec<String> = sharing
                .iter()
                .map(|s| s.device.display_slug.clone())
                .collect();
            sharing.retain(|s| named.contains(&s.device.device_user.device_id));
            if sharing.is_empty() {
                return Err(ToolError::InvalidArgs(format!(
                    "{NAME}: no sharing device named {device:?}; sharing devices: {names:?}"
                )));
            }
        }
        let now = Utc::now();
        let devices: Vec<serde_json::Value> = sharing.iter().map(|s| entry(s, now)).collect();
        debug!(
            user_id = user.id,
            devices = devices.len(),
            "{NAME} answered"
        );
        let mut answer = serde_json::json!({
            "devices": devices,
            "stale_after_seconds": STALE_AFTER_SECONDS,
        });
        if devices.is_empty() {
            answer["how_to_enable"] = serde_json::Value::from(HOW_TO_ENABLE);
        }
        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::channel::tests::bus;
    use crate::channel::{FixFields, LocationMessage};
    use brenn_db::auth::device::set_tz_override;
    use brenn_messaging::Messenger;
    use brenn_messaging::device_location::{
        DEVICE_LOCATION_COMPONENT, insert_device_location_message,
    };
    use brenn_tool_registry::ActingUser;
    use chrono::Offset as _;

    const ALICE: i64 = 1;
    const BOB: i64 = 2;
    const LAPTOP: i64 = 1;
    const PHONE: i64 = 2;
    const TABLET: i64 = 3;

    fn user(id: i64, name: &str) -> ActingUser {
        ActingUser {
            id,
            username: name.to_string(),
        }
    }

    fn tool_ctx(acting_user: Option<ActingUser>) -> ToolCtx {
        ToolCtx {
            caller: brenn_lib::messaging::ParticipantId::for_app("pa", "test-origin"),
            grant: brenn_lib::tools::ResolvedToolGrant {
                acl: Vec::new(),
                rate_limit: None,
            },
            acting_conversation_id: Some(7),
            acting_user,
            deadline: None,
        }
    }

    fn tool(db: &brenn_db::Db, m: &Arc<Messenger>) -> DeviceLocationTool {
        let bus = DeviceLocationBus::default();
        bus.attach(m.clone());
        DeviceLocationTool::new(db.clone(), bus)
    }

    /// Put `message` on `username`'s channel from the system writer,
    /// received `seconds` ago.
    async fn put(m: &Messenger, username: &str, message: LocationMessage, seconds: i64) {
        insert_device_location_message(
            m,
            username,
            brenn_lib::messaging::ParticipantId::for_system(DEVICE_LOCATION_COMPONENT).as_str(),
            &serde_json::to_string(&message).unwrap(),
            Utc::now() - chrono::Duration::seconds(seconds),
        )
        .await;
    }

    fn fix(
        device_id: i64,
        latitude: f64,
        longitude: f64,
        accuracy_m: f64,
        zone: Option<&str>,
    ) -> LocationMessage {
        LocationMessage::Fix(FixFields {
            device_id,
            device: "published-name".into(),
            latitude,
            longitude,
            accuracy_m,
            time_zone: zone.map(str::to_string),
        })
    }

    fn at(device_id: i64, zone: Option<&str>) -> LocationMessage {
        fix(device_id, 1.0, 2.0, 3.0, zone)
    }

    async fn call(
        db: &brenn_db::Db,
        m: &Arc<Messenger>,
        who: ActingUser,
        args: serde_json::Value,
    ) -> serde_json::Value {
        tool(db, m)
            .execute(&tool_ctx(Some(who)), args)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn two_sharing_devices_most_recent_first_with_age_and_zone() {
        let (db, m) = bus().await;
        put(&m, "alice", fix(LAPTOP, 48.1, 11.6, 900.0, None), 1000).await;
        put(
            &m,
            "alice",
            fix(PHONE, 52.5, 13.4, 25.0, Some("Europe/Berlin")),
            60,
        )
        .await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["stale_after_seconds"], 900);
        assert!(out.get("how_to_enable").is_none());
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);

        let phone = &devices[0];
        assert_eq!(
            phone["device"], "phone",
            "the current display slug, not the published one"
        );
        assert_eq!(phone["latitude"], 52.5);
        assert_eq!(phone["longitude"], 13.4);
        assert_eq!(phone["accuracy_m"], 25.0);
        let age = phone["age_seconds"].as_i64().unwrap();
        assert!((59..=61).contains(&age), "{age}");
        assert_eq!(phone["stale"], false);
        assert_eq!(phone["time_zone"], "Europe/Berlin");
        let local = phone["local_time"].as_str().unwrap();
        let parsed = DateTime::parse_from_rfc3339(local).unwrap();
        let berlin_offset = Utc::now()
            .with_timezone(&chrono_tz::Europe::Berlin)
            .offset()
            .fix();
        assert_eq!(*parsed.offset(), berlin_offset);
        assert!(phone["reported_at"].as_str().unwrap().ends_with('Z'));

        let laptop = &devices[1];
        assert_eq!(laptop["device"], "laptop");
        assert_eq!(laptop["stale"], true);
        assert_eq!(laptop["time_zone"], serde_json::Value::Null);
        assert_eq!(laptop["local_time"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn the_newest_message_per_device_decides() {
        let (db, m) = bus().await;
        put(&m, "alice", fix(PHONE, 1.0, 2.0, 3.0, None), 300).await;
        put(&m, "alice", fix(PHONE, 4.0, 5.0, 6.0, None), 30).await;
        put(&m, "alice", at(LAPTOP, None), 200).await;
        put(
            &m,
            "alice",
            LocationMessage::Stopped {
                device_id: LAPTOP,
                device: "laptop".into(),
            },
            100,
        )
        .await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1, "a stopped device is not answered");
        assert_eq!(devices[0]["device"], "phone");
        assert_eq!(devices[0]["latitude"], 4.0);
    }

    #[tokio::test]
    async fn an_unenrolled_or_unpaired_device_is_not_answered() {
        let (db, m) = bus().await;
        db.lock()
            .await
            .execute(
                "UPDATE devices SET unenrolled_at = 1 WHERE id = ?1",
                rusqlite::params![TABLET],
            )
            .unwrap();
        put(&m, "alice", at(TABLET, None), 10).await;
        put(&m, "alice", at(99, None), 10).await;
        put(&m, "alice", at(PHONE, None), 20).await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device"], "phone");
    }

    #[tokio::test]
    async fn stale_is_strictly_past_the_threshold() {
        let (db, m) = bus().await;
        put(&m, "alice", at(LAPTOP, None), 950).await;
        put(&m, "alice", at(PHONE, None), 850).await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["stale"], false);
        assert_eq!(out["devices"][1]["stale"], true);
    }

    #[tokio::test]
    async fn a_live_override_wins_and_an_expired_one_is_ignored() {
        let (db, m) = bus().await;
        let far_future = Utc::now().timestamp() + 3600;
        {
            let conn = db.lock().await;
            set_tz_override(&conn, PHONE, ALICE, Some("Asia/Tokyo"), Some(far_future));
            set_tz_override(&conn, LAPTOP, ALICE, Some("Asia/Tokyo"), Some(1));
        }
        put(&m, "alice", at(LAPTOP, Some("Europe/Berlin")), 20).await;
        put(&m, "alice", at(PHONE, Some("Europe/Berlin")), 10).await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["device"], "phone");
        assert_eq!(out["devices"][0]["time_zone"], "Asia/Tokyo");
        assert_eq!(out["devices"][1]["device"], "laptop");
        assert_eq!(out["devices"][1]["time_zone"], "Europe/Berlin");
    }

    #[tokio::test]
    async fn a_live_override_answers_a_fix_with_no_zone() {
        let (db, m) = bus().await;
        set_tz_override(&*db.lock().await, PHONE, ALICE, Some("Asia/Tokyo"), None);
        put(&m, "alice", at(PHONE, None), 10).await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["time_zone"], "Asia/Tokyo");
    }

    #[tokio::test]
    async fn device_narrows_and_an_unknown_device_is_invalid_args() {
        let (db, m) = bus().await;
        put(&m, "alice", at(LAPTOP, None), 20).await;
        put(&m, "alice", at(PHONE, None), 10).await;
        let out = call(
            &db,
            &m,
            user(ALICE, "alice"),
            serde_json::json!({"device": "laptop"}),
        )
        .await;
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device"], "laptop");

        let err = tool(&db, &m)
            .execute(
                &tool_ctx(Some(user(ALICE, "alice"))),
                serde_json::json!({"device": "tablet"}),
            )
            .await;
        match err {
            Err(ToolError::InvalidArgs(msg)) => {
                assert!(msg.contains("\"tablet\""), "{msg}");
                assert!(
                    msg.contains("\"phone\"") && msg.contains("\"laptop\""),
                    "{msg}"
                );
            }
            other => panic!("expected InvalidArgs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn device_resolves_by_id_guessed_and_assigned_slug() {
        let (db, m) = bus().await;
        db.lock()
            .await
            .execute(
                "UPDATE device_users SET assigned_slug = 'pocket' WHERE device_id = ?1 AND user_id = ?2",
                rusqlite::params![PHONE, ALICE],
            )
            .unwrap();
        put(&m, "alice", at(LAPTOP, None), 20).await;
        put(&m, "alice", at(PHONE, None), 10).await;
        for name in ["pocket", "phone", "2"] {
            let out = call(
                &db,
                &m,
                user(ALICE, "alice"),
                serde_json::json!({"device": name}),
            )
            .await;
            let devices = out["devices"].as_array().unwrap();
            assert_eq!(devices.len(), 1, "{name}");
            assert_eq!(devices[0]["device"], "pocket", "{name}");
        }
    }

    #[tokio::test]
    async fn another_users_channel_never_answers() {
        let (db, m) = bus().await;
        put(&m, "bob", at(LAPTOP, None), 10).await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"], serde_json::json!([]));
        // Bob's device name narrows nothing for alice either.
        let err = tool(&db, &m)
            .execute(
                &tool_ctx(Some(user(ALICE, "alice"))),
                serde_json::json!({"device": "laptop"}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))));
        let bobs = call(&db, &m, user(BOB, "bob"), serde_json::json!({})).await;
        assert_eq!(bobs["devices"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_message_from_another_sender_is_not_answered() {
        let (db, m) = bus().await;
        insert_device_location_message(
            &m,
            "alice",
            "wasm:intruder",
            &serde_json::to_string(&at(PHONE, None)).unwrap(),
            Utc::now(),
        )
        .await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn no_sharing_device_answers_how_to_enable() {
        let (db, m) = bus().await;
        let out = call(&db, &m, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(
            out,
            serde_json::json!({
                "devices": [],
                "stale_after_seconds": 900,
                "how_to_enable": HOW_TO_ENABLE,
            })
        );
    }

    #[tokio::test]
    async fn extra_keys_are_invalid_args() {
        let (db, m) = bus().await;
        let err = tool(&db, &m)
            .execute(
                &tool_ctx(Some(user(ALICE, "alice"))),
                serde_json::json!({"x": 1}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))));
    }

    #[tokio::test]
    async fn no_acting_user_is_denied() {
        let (db, m) = bus().await;
        let err = tool(&db, &m)
            .execute(&tool_ctx(None), serde_json::json!({}))
            .await;
        assert!(
            matches!(&err, Err(ToolError::Denied(d)) if d.starts_with("device-location: acts as the acting user")),
            "{err:?}"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "executed before the messenger was attached")]
    async fn a_call_before_the_messenger_is_attached_panics() {
        let (db, _m) = bus().await;
        let outcome = DeviceLocationTool::new(db, DeviceLocationBus::default())
            .execute(&tool_ctx(Some(user(ALICE, "alice"))), serde_json::json!({}))
            .await;
        unreachable!("answered without a messenger: {outcome:?}");
    }

    #[test]
    fn descriptor_binds_the_acting_user_with_no_acl_keys() {
        let tool = DeviceLocationTool::new(crate::db::seeded_db(), DeviceLocationBus::default());
        let d = tool.descriptor();
        assert_eq!(NAME, "device-location");
        assert_eq!(d.name, NAME);
        assert_eq!(d.mcp_name, "mcp__brenn__DeviceLocation");
        assert_eq!(d.user_binding, UserBinding::ActingUser);
        assert!(d.acl_keys.is_empty());
        assert_eq!(d.class, ToolClass::Async { max_concurrency: 4 });
        assert_eq!(d.idempotency, Idempotency::Natural);
        assert!(d.auto_approve);
        assert!(
            tool.check_acl(&serde_json::json!({"device": "x"}), &[])
                .is_ok()
        );
    }
}
