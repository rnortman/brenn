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

use crate::db::{StoredFix, list_fixes_for_user};
use crate::{HOW_TO_ENABLE, NAME, STALE_AFTER_SECONDS};

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
}

impl DeviceLocationTool {
    pub(crate) fn new(db: brenn_db::Db) -> Self {
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
                              user its age rather than treating it as current. `Weather` \
                              without a location uses the most recent fix itself; to ask about \
                              another device, pass its latitude and longitude to `Weather` as \
                              `lat`/`lng`. Pass a device's `time_zone` to the Calendar tools. \
                              Empty `devices` means no device shares location: \
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
                user_binding: UserBinding::ActingOrServedUser,
            },
            db,
        }
    }
}

/// One answer entry for `fix`, its age and zone taken at `now`.
fn entry(fix: &StoredFix, now: DateTime<Utc>) -> serde_json::Value {
    let age_seconds = (now.timestamp_millis() - fix.reported_at_ms).max(0) / 1000;
    let reported_at = DateTime::from_timestamp_millis(fix.reported_at_ms)
        .unwrap_or_else(|| {
            panic!(
                "device_locations.reported_at_ms out of chrono's range: {}",
                fix.reported_at_ms
            )
        })
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let zone = brenn_db::auth::device::effective_timezone_opt(
        &fix.device_user,
        brenn_db::auth::device::stored_browser_tz(&fix.device_user),
        now,
    );
    serde_json::json!({
        "device": fix.display_slug,
        "latitude": fix.latitude,
        "longitude": fix.longitude,
        "accuracy_m": fix.accuracy_m,
        "reported_at": reported_at,
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
        let (mut fixes, named) = {
            let conn = self.db.lock().await;
            let named = args.device.as_deref().map(|device| {
                brenn_db::auth::device::resolve_device_ids_for_get(&conn, device, &[user.id])
            });
            (list_fixes_for_user(&conn, user.id), named)
        };
        if let (Some(device), Some(named)) = (args.device, named) {
            let sharing: Vec<String> = fixes.iter().map(|f| f.display_slug.clone()).collect();
            fixes.retain(|f| named.contains(&f.device_user.device_id));
            if fixes.is_empty() {
                return Err(ToolError::InvalidArgs(format!(
                    "{NAME}: no sharing device named {device:?}; sharing devices: {sharing:?}"
                )));
            }
        }
        let now = Utc::now();
        let devices: Vec<serde_json::Value> = fixes.iter().map(|f| entry(f, now)).collect();
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
    use super::*;
    use crate::db::{seeded_db, upsert_fix};
    use crate::validate::fix;
    use brenn_db::auth::device::{set_browser_tz, set_tz_override};
    use brenn_tool_registry::ActingUser;
    use chrono::Offset as _;

    const ALICE: i64 = 1;
    const BOB: i64 = 2;
    const LAPTOP: i64 = 1;
    const PHONE: i64 = 2;

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

    fn ago_ms(seconds: i64) -> i64 {
        Utc::now().timestamp_millis() - seconds * 1000
    }

    async fn call(
        db: &brenn_db::Db,
        who: ActingUser,
        args: serde_json::Value,
    ) -> serde_json::Value {
        DeviceLocationTool::new(db.clone())
            .execute(&tool_ctx(Some(who)), args)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn two_sharing_devices_most_recent_first_with_age_and_zone() {
        let db = seeded_db();
        {
            let conn = db.lock().await;
            set_browser_tz(&conn, PHONE, ALICE, Some("Europe/Berlin"));
            upsert_fix(&conn, PHONE, ALICE, &fix(52.5, 13.4, 25.0), ago_ms(60));
            upsert_fix(&conn, LAPTOP, ALICE, &fix(48.1, 11.6, 900.0), ago_ms(1000));
        }
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["stale_after_seconds"], 900);
        assert!(out.get("how_to_enable").is_none());
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);

        let phone = &devices[0];
        assert_eq!(phone["device"], "phone");
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
    async fn stale_is_strictly_past_the_threshold() {
        let db = seeded_db();
        {
            let conn = db.lock().await;
            upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(850));
            upsert_fix(&conn, LAPTOP, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(950));
        }
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["stale"], false);
        assert_eq!(out["devices"][1]["stale"], true);
    }

    #[tokio::test]
    async fn a_live_override_wins_and_an_expired_one_is_ignored() {
        let db = seeded_db();
        let far_future = Utc::now().timestamp() + 3600;
        {
            let conn = db.lock().await;
            set_browser_tz(&conn, PHONE, ALICE, Some("Europe/Berlin"));
            set_browser_tz(&conn, LAPTOP, ALICE, Some("Europe/Berlin"));
            set_tz_override(&conn, PHONE, ALICE, Some("Asia/Tokyo"), Some(far_future));
            set_tz_override(&conn, LAPTOP, ALICE, Some("Asia/Tokyo"), Some(1));
            upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(10));
            upsert_fix(&conn, LAPTOP, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(20));
        }
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["device"], "phone");
        assert_eq!(out["devices"][0]["time_zone"], "Asia/Tokyo");
        assert_eq!(out["devices"][1]["device"], "laptop");
        assert_eq!(out["devices"][1]["time_zone"], "Europe/Berlin");
    }

    #[tokio::test]
    async fn a_live_override_answers_a_device_with_no_stored_zone() {
        let db = seeded_db();
        {
            let conn = db.lock().await;
            set_tz_override(&conn, PHONE, ALICE, Some("Asia/Tokyo"), None);
            upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(10));
        }
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"][0]["time_zone"], "Asia/Tokyo");
    }

    #[tokio::test]
    async fn device_narrows_and_an_unknown_device_is_invalid_args() {
        let db = seeded_db();
        {
            let conn = db.lock().await;
            upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(10));
            upsert_fix(&conn, LAPTOP, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(20));
        }
        let out = call(
            &db,
            user(ALICE, "alice"),
            serde_json::json!({"device": "laptop"}),
        )
        .await;
        let devices = out["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0]["device"], "laptop");

        let err = DeviceLocationTool::new(db.clone())
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
        let db = seeded_db();
        {
            let conn = db.lock().await;
            conn.execute(
                "UPDATE device_users SET assigned_slug = 'pocket' WHERE device_id = ?1 AND user_id = ?2",
                rusqlite::params![PHONE, ALICE],
            )
            .unwrap();
            upsert_fix(&conn, PHONE, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(10));
            upsert_fix(&conn, LAPTOP, ALICE, &fix(1.0, 2.0, 3.0), ago_ms(20));
        }
        for name in ["pocket", "phone", "2"] {
            let out = call(
                &db,
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
    async fn another_users_rows_never_appear() {
        let db = seeded_db();
        {
            let conn = db.lock().await;
            upsert_fix(&conn, LAPTOP, BOB, &fix(1.0, 2.0, 3.0), ago_ms(10));
        }
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
        assert_eq!(out["devices"], serde_json::json!([]));
        // Bob's device name narrows nothing for alice either.
        let err = DeviceLocationTool::new(db.clone())
            .execute(
                &tool_ctx(Some(user(ALICE, "alice"))),
                serde_json::json!({"device": "laptop"}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))));
        let bobs = call(&db, user(BOB, "bob"), serde_json::json!({})).await;
        assert_eq!(bobs["devices"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn no_sharing_device_answers_how_to_enable() {
        let db = seeded_db();
        let out = call(&db, user(ALICE, "alice"), serde_json::json!({})).await;
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
        let db = seeded_db();
        let err = DeviceLocationTool::new(db)
            .execute(
                &tool_ctx(Some(user(ALICE, "alice"))),
                serde_json::json!({"x": 1}),
            )
            .await;
        assert!(matches!(err, Err(ToolError::InvalidArgs(_))));
    }

    #[tokio::test]
    async fn no_acting_user_is_denied() {
        let err = DeviceLocationTool::new(seeded_db())
            .execute(&tool_ctx(None), serde_json::json!({}))
            .await;
        assert!(
            matches!(&err, Err(ToolError::Denied(d)) if d.starts_with("device-location: acts as the acting user")),
            "{err:?}"
        );
    }

    #[test]
    fn descriptor_binds_the_acting_user_with_no_acl_keys() {
        let tool = DeviceLocationTool::new(seeded_db());
        let d = tool.descriptor();
        assert_eq!(NAME, "device-location");
        assert_eq!(d.name, NAME);
        assert_eq!(d.mcp_name, "mcp__brenn__DeviceLocation");
        assert_eq!(d.user_binding, UserBinding::ActingOrServedUser);
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
