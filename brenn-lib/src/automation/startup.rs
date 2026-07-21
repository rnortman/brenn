//! Startup-time consistency checks for the automation engine.
//!
//! Called once at bootstrap, before `run_startup_catchup`. Two checks:
//! 1. Rebind stale event conversations (user mismatch after config change).
//! 2. Bulk-disable orphaned jobs (owner app removed from config).

use chrono::Utc;

use crate::auth::user::get_user_by_username;
use crate::automation::AutomationEngine;
use crate::conversation::{create_conversation, get_conversation_opt};

/// Run startup consistency checks. Acquires the DB lock once; both checks
/// run under the same lock.
///
/// Must be called after `ingress_router.set_state(...)` and before
/// `run_startup_catchup`.
pub async fn run_startup_consistency_checks(engine: &AutomationEngine) {
    let now = Utc::now();
    let now_str = crate::db::format_ts_for_db(now);
    let conn = engine.db.lock().await;

    // --- Fix 1: Rebind stale event conversations ---

    // Collect all event-conversation mappings.
    let mappings: Vec<(String, i64)> = {
        let mut stmt = conn
            .prepare(
                "SELECT owner_app_slug, conversation_id \
                 FROM automation_app_event_conversation",
            )
            .expect("startup: prepare automation_app_event_conversation");
        stmt.query_map([], |row| {
            let slug: String = row.get(0)?;
            let conv_id: i64 = row.get(1)?;
            Ok((slug, conv_id))
        })
        .expect("startup: query automation_app_event_conversation")
        .map(|r| r.expect("startup: read automation_app_event_conversation row"))
        .collect()
    };

    for (owner_app_slug, stored_conv_id) in mappings {
        // Skip if app is no longer in config.
        let app_config = match engine.apps.get(&owner_app_slug) {
            Some(c) => c,
            None => continue,
        };

        // Skip if app has open access (no allowed_users).
        let owner_username = match app_config.allowed_users.first() {
            Some(u) => u,
            None => continue,
        };

        // Resolve user in DB.
        let owner_user = match get_user_by_username(&conn, owner_username) {
            Some(u) => u,
            None => {
                tracing::warn!(
                    owner_app_slug = %owner_app_slug,
                    owner_username = %owner_username,
                    "startup rebind: owner user not found in DB; event conversation NOT rebound \
                     — automation error reports for this app may be delivered to the wrong user \
                     until config or DB is corrected"
                );
                continue;
            }
        };
        let owner_user_id = owner_user.id;

        // Load the stored conversation.
        let stored_conv = match get_conversation_opt(&conn, stored_conv_id) {
            Some(c) => c,
            None => {
                // Conversation was deleted; remove stale mapping.
                conn.execute(
                    "DELETE FROM automation_app_event_conversation WHERE owner_app_slug = ?1",
                    rusqlite::params![owner_app_slug],
                )
                .expect("startup rebind: delete stale mapping");
                tracing::info!(
                    owner_app_slug = %owner_app_slug,
                    stored_conv_id,
                    "startup rebind: stored conversation no longer exists; deleted mapping"
                );
                continue;
            }
        };

        // If user matches, nothing to do.
        if stored_conv.user_id == owner_user_id {
            continue;
        }

        // Mismatch: create a new conversation for the current owner and rebind.
        let new_conv_id = create_conversation(&conn, owner_user_id, &owner_app_slug, false);
        conn.execute(
            "UPDATE automation_app_event_conversation \
             SET conversation_id = ?1 \
             WHERE owner_app_slug = ?2",
            rusqlite::params![new_conv_id, owner_app_slug],
        )
        .expect("startup rebind: update automation_app_event_conversation");
        tracing::info!(
            owner_app_slug = %owner_app_slug,
            old_user_id = stored_conv.user_id,
            new_user_id = owner_user_id,
            old_conv_id = stored_conv_id,
            new_conv_id,
            "startup rebind: rebound automation event conversation to new owner"
        );
    }

    // --- Fix 2: Bulk-disable orphaned jobs ---

    // Collect distinct app slugs for enabled jobs.
    let enabled_slugs: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT owner_app_slug \
                 FROM automation_jobs \
                 WHERE enabled = 1",
            )
            .expect("startup: prepare enabled_slugs");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("startup: query enabled_slugs")
            .map(|r| r.expect("startup: read enabled_slugs row"))
            .collect()
    };

    for slug in enabled_slugs {
        if engine.apps.contains_key(&slug) {
            continue;
        }

        // App is gone from config: bulk-disable all its enabled jobs.
        let rows_affected = conn
            .execute(
                "UPDATE automation_jobs \
                 SET enabled = 0, auto_disabled_at = ?1, updated_at = ?1 \
                 WHERE owner_app_slug = ?2 AND enabled = 1",
                rusqlite::params![now_str, slug],
            )
            .expect("startup orphan-disable: update automation_jobs");
        tracing::warn!(
            owner_app_slug = %slug,
            rows_disabled = rows_affected,
            "startup: disabled orphaned jobs for removed app"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::automation::test_support::{default_app_cfg, make_engine_with_apps};
    use crate::conversation::get_conversation_opt;
    use crate::db::init_db_memory;
    use rusqlite::OptionalExtension;

    /// Insert a user row and return the row_id.
    async fn insert_user(db: &crate::db::Db, username: &str) -> i64 {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?1, 'hash', '2026-01-01')",
            rusqlite::params![username],
        )
        .expect("insert user");
        conn.last_insert_rowid()
    }

    /// Create an automation_app_event_conversation row pointing at a real
    /// conversation (creates the conversation too). Returns conversation_id.
    async fn insert_event_conv(db: &crate::db::Db, app_slug: &str, user_id: i64) -> i64 {
        let conn = db.lock().await;
        let conv_id = crate::conversation::create_conversation(&conn, user_id, app_slug, false);
        conn.execute(
            "INSERT INTO automation_app_event_conversation (owner_app_slug, conversation_id) \
             VALUES (?1, ?2)",
            rusqlite::params![app_slug, conv_id],
        )
        .expect("insert automation_app_event_conversation");
        conv_id
    }

    /// Insert a single already-disabled automation_job for `app_slug`. Returns the row id.
    async fn insert_disabled_job(db: &crate::db::Db, app_slug: &str) -> i64 {
        let conn = db.lock().await;
        let now = crate::db::format_ts_for_db(Utc::now());
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, ?2, 'disabled-job', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', \
                     '{\"to\":\"brenn:ch\",\"body\":\"hi\",\"wake\":\"none\",\"reply_to\":null,\"delivery_deadline_secs\":null}', \
                     0, 0, ?3, ?3, ?3)",
            rusqlite::params![uuid_bytes, app_slug, now],
        )
        .expect("insert disabled automation_job");
        conn.last_insert_rowid()
    }

    /// Insert enabled automation_jobs for `app_slug`. Returns inserted row ids.
    async fn insert_enabled_jobs(db: &crate::db::Db, app_slug: &str, count: u32) -> Vec<i64> {
        let conn = db.lock().await;
        let now = crate::db::format_ts_for_db(Utc::now());
        let mut ids = Vec::new();
        for i in 0..count {
            let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
            conn.execute(
                "INSERT INTO automation_jobs \
                 (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
                  action_kind, action_payload, enabled, consecutive_failures, \
                  created_at, updated_at, next_fire_at) \
                 VALUES (?1, ?2, ?3, 'cron', \
                         '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                         'send_message', \
                         '{\"to\":\"brenn:ch\",\"body\":\"hi\",\"wake\":\"none\",\"reply_to\":null,\"delivery_deadline_secs\":null}', \
                         1, 0, ?4, ?4, ?4)",
                rusqlite::params![uuid_bytes, app_slug, format!("job-{i}"), now],
            )
            .expect("insert automation_job");
            ids.push(conn.last_insert_rowid());
        }
        ids
    }

    /// Read the conversation_id from automation_app_event_conversation for an app.
    async fn read_mapping(db: &crate::db::Db, app_slug: &str) -> Option<i64> {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT conversation_id FROM automation_app_event_conversation WHERE owner_app_slug = ?1",
            rusqlite::params![app_slug],
            |row| row.get(0),
        )
        .optional()
        .expect("read mapping")
    }

    // -------------------------------------------------------------------------
    // Test 1: Rebind on user mismatch
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn rebind_on_user_mismatch() {
        let db = init_db_memory();
        let user_a_id = insert_user(&db, "user_a").await;
        let user_b_id = insert_user(&db, "user_b").await;

        // Conversation owned by user_a.
        let old_conv_id = insert_event_conv(&db, "myapp", user_a_id).await;

        // Engine where myapp has allowed_users = ["user_b"].
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = default_app_cfg("myapp", false);
        cfg.allowed_users = vec!["user_b".to_string()];
        apps.insert("myapp".to_string(), cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // Mapping must point to a new conversation.
        let new_conv_id = read_mapping(&db, "myapp")
            .await
            .expect("mapping must exist");
        assert_ne!(
            new_conv_id, old_conv_id,
            "mapping must be updated to a new conversation"
        );

        // New conversation must be owned by user_b.
        let conn = db.lock().await;
        let conv = get_conversation_opt(&conn, new_conv_id).expect("new conversation must exist");
        assert_eq!(
            conv.user_id, user_b_id,
            "new conversation must be owned by user_b"
        );
    }

    // -------------------------------------------------------------------------
    // Test 2: No rebind when user matches
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn no_rebind_when_user_matches() {
        let db = init_db_memory();
        let user_a_id = insert_user(&db, "user_a").await;
        let original_conv_id = insert_event_conv(&db, "myapp", user_a_id).await;

        let mut apps = indexmap::IndexMap::new();
        let mut cfg = default_app_cfg("myapp", false);
        cfg.allowed_users = vec!["user_a".to_string()];
        apps.insert("myapp".to_string(), cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        let conv_id = read_mapping(&db, "myapp")
            .await
            .expect("mapping must exist");
        assert_eq!(
            conv_id, original_conv_id,
            "mapping must be unchanged when user matches"
        );
    }

    // -------------------------------------------------------------------------
    // Test 3: Rebind skips removed app
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn rebind_skips_removed_app() {
        let db = init_db_memory();
        let user_id = insert_user(&db, "user_a").await;
        let original_conv_id = insert_event_conv(&db, "removed-app", user_id).await;

        // Engine with NO "removed-app".
        let apps = indexmap::IndexMap::new();
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // Mapping is unchanged (no panic, no rebind).
        let conv_id = read_mapping(&db, "removed-app")
            .await
            .expect("mapping must still exist");
        assert_eq!(
            conv_id, original_conv_id,
            "mapping must be untouched for removed app"
        );
    }

    // -------------------------------------------------------------------------
    // Test 4: Bulk-disable orphaned jobs
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn bulk_disable_orphaned_jobs() {
        let db = init_db_memory();
        let job_ids = insert_enabled_jobs(&db, "gone-app", 3).await;

        // Engine with no "gone-app".
        let apps = indexmap::IndexMap::new();
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        let conn = db.lock().await;
        for job_id in job_ids {
            let (enabled, auto_disabled, updated_at): (i64, Option<String>, Option<String>) = conn
                .query_row(
                    "SELECT enabled, auto_disabled_at, updated_at FROM automation_jobs WHERE id = ?1",
                    rusqlite::params![job_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("read job");
            assert_eq!(enabled, 0, "job {job_id} must be disabled");
            assert!(
                auto_disabled.is_some(),
                "job {job_id} must have auto_disabled_at set"
            );
            assert_eq!(
                updated_at, auto_disabled,
                "job {job_id}: updated_at must equal auto_disabled_at"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Test 5: Jobs for present apps are untouched
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn jobs_for_present_apps_untouched() {
        let db = init_db_memory();
        let job_ids = insert_enabled_jobs(&db, "test-app", 2).await;

        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), default_app_cfg("test-app", false));
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        let conn = db.lock().await;
        for job_id in job_ids {
            let enabled: i64 = conn
                .query_row(
                    "SELECT enabled FROM automation_jobs WHERE id = ?1",
                    rusqlite::params![job_id],
                    |row| row.get(0),
                )
                .expect("read job");
            assert_eq!(enabled, 1, "job {job_id} must remain enabled");
        }
    }

    // -------------------------------------------------------------------------
    // Test 6: Deleted conversation row cleanup
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn deleted_conversation_mapping_cleaned_up() {
        let db = init_db_memory();
        let user_id = insert_user(&db, "user_a").await;

        // Create a real conversation, insert a mapping pointing to it, then
        // delete the conversation so the mapping becomes stale.
        let stale_conv_id = {
            let conn = db.lock().await;
            let conv_id = crate::conversation::create_conversation(&conn, user_id, "myapp", false);
            conn.execute(
                "INSERT INTO automation_app_event_conversation (owner_app_slug, conversation_id) \
                 VALUES ('myapp', ?1)",
                rusqlite::params![conv_id],
            )
            .expect("insert mapping");
            // Delete the conversation to make the mapping stale (disable FK
            // checks briefly so the delete succeeds without cascade errors from
            // other references that may not exist).
            conn.execute("PRAGMA foreign_keys = OFF", [])
                .expect("disable fk");
            conn.execute(
                "DELETE FROM conversations WHERE id = ?1",
                rusqlite::params![conv_id],
            )
            .expect("delete conversation");
            conn.execute("PRAGMA foreign_keys = ON", [])
                .expect("enable fk");
            conv_id
        };

        let mut apps = indexmap::IndexMap::new();
        let mut cfg = default_app_cfg("myapp", false);
        cfg.allowed_users = vec!["user_a".to_string()];
        apps.insert("myapp".to_string(), cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // Mapping must be deleted.
        let mapping = read_mapping(&db, "myapp").await;
        assert!(
            mapping.is_none(),
            "stale mapping pointing at non-existent conversation (id={stale_conv_id}) must be deleted"
        );
    }

    // -------------------------------------------------------------------------
    // Test 7: Mixed scenario
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn mixed_scenario() {
        let db = init_db_memory();

        // User setup.
        let user_a_id = insert_user(&db, "user_a").await;
        let user_b_id = insert_user(&db, "user_b").await;

        // App 1 "stale-app": has a mapping owned by user_a; engine says user_b.
        let old_conv_id = insert_event_conv(&db, "stale-app", user_a_id).await;

        // App 2 "removed-app": has enabled jobs, not present in engine.
        let job_ids = insert_enabled_jobs(&db, "removed-app", 2).await;

        // App 3 "healthy-app": present in engine, no issues.
        let healthy_job_ids = insert_enabled_jobs(&db, "healthy-app", 1).await;

        // Build engine.
        let mut apps = indexmap::IndexMap::new();
        let mut stale_cfg = default_app_cfg("stale-app", false);
        stale_cfg.allowed_users = vec!["user_b".to_string()];
        stale_cfg.messaging = Some(crate::messaging::config::ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        });
        apps.insert("stale-app".to_string(), stale_cfg);
        let mut healthy_cfg = default_app_cfg("healthy-app", false);
        healthy_cfg.messaging = Some(crate::messaging::config::ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        });
        apps.insert("healthy-app".to_string(), healthy_cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // stale-app: mapping must be rebound to user_b's new conversation.
        let new_conv_id = read_mapping(&db, "stale-app")
            .await
            .expect("stale-app mapping must exist");
        assert_ne!(
            new_conv_id, old_conv_id,
            "stale-app mapping must be rebound"
        );
        {
            let conn = db.lock().await;
            let conv =
                get_conversation_opt(&conn, new_conv_id).expect("new conversation must exist");
            assert_eq!(
                conv.user_id, user_b_id,
                "rebound conversation must be owned by user_b"
            );
        }

        // removed-app: jobs must be disabled with auto_disabled_at set.
        {
            let conn = db.lock().await;
            for job_id in &job_ids {
                let (enabled, auto_disabled): (i64, Option<String>) = conn
                    .query_row(
                        "SELECT enabled, auto_disabled_at FROM automation_jobs WHERE id = ?1",
                        rusqlite::params![job_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .expect("read removed-app job");
                assert_eq!(enabled, 0, "removed-app job {job_id} must be disabled");
                assert!(
                    auto_disabled.is_some(),
                    "removed-app job {job_id} must have auto_disabled_at set"
                );
            }
        }

        // healthy-app: jobs untouched.
        {
            let conn = db.lock().await;
            for job_id in &healthy_job_ids {
                let enabled: i64 = conn
                    .query_row(
                        "SELECT enabled FROM automation_jobs WHERE id = ?1",
                        rusqlite::params![job_id],
                        |row| row.get(0),
                    )
                    .expect("read healthy-app job");
                assert_eq!(enabled, 1, "healthy-app job {job_id} must remain enabled");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test 8: Empty allowed_users — mapping left untouched (open-access app)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn empty_allowed_users_skips_rebind() {
        let db = init_db_memory();
        let user_id = insert_user(&db, "user_a").await;
        let original_conv_id = insert_event_conv(&db, "open-app", user_id).await;

        // App has no allowed_users — open access.
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = default_app_cfg("open-app", false);
        cfg.allowed_users = vec![];
        apps.insert("open-app".to_string(), cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // Mapping must be unchanged: open-access apps are not rebound.
        let conv_id = read_mapping(&db, "open-app")
            .await
            .expect("mapping must still exist");
        assert_eq!(
            conv_id, original_conv_id,
            "open-access app mapping must not be rebound"
        );
    }

    // -------------------------------------------------------------------------
    // Test 9: Owner user missing from DB — mapping left untouched
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn missing_owner_user_skips_rebind() {
        let db = init_db_memory();
        let user_id = insert_user(&db, "user_a").await;
        let original_conv_id = insert_event_conv(&db, "myapp", user_id).await;

        // Config says owner is "nonexistent_user" which is not in the users table.
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = default_app_cfg("myapp", false);
        cfg.allowed_users = vec!["nonexistent_user".to_string()];
        apps.insert("myapp".to_string(), cfg);
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        // Mapping must be unchanged: can't rebind without a valid target user.
        let conv_id = read_mapping(&db, "myapp")
            .await
            .expect("mapping must still exist");
        assert_eq!(
            conv_id, original_conv_id,
            "mapping must not be rebound when owner user is absent from DB"
        );
    }

    // -------------------------------------------------------------------------
    // Test 10: Already-disabled jobs for removed app are not re-disabled
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn already_disabled_jobs_not_touched() {
        let db = init_db_memory();

        // One enabled and one already-disabled job for a removed app.
        let enabled_ids = insert_enabled_jobs(&db, "gone-app", 1).await;
        let disabled_id = insert_disabled_job(&db, "gone-app").await;

        // Engine has no "gone-app".
        let apps = indexmap::IndexMap::new();
        let engine = make_engine_with_apps(db.clone(), Arc::new(apps));

        run_startup_consistency_checks(&engine).await;

        let conn = db.lock().await;

        // The enabled job must now be disabled with auto_disabled_at set.
        for job_id in &enabled_ids {
            let (enabled, auto_disabled): (i64, Option<String>) = conn
                .query_row(
                    "SELECT enabled, auto_disabled_at FROM automation_jobs WHERE id = ?1",
                    rusqlite::params![job_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("read enabled job");
            assert_eq!(enabled, 0, "previously-enabled job must be disabled");
            assert!(auto_disabled.is_some(), "auto_disabled_at must be set");
        }

        // The already-disabled job must be unchanged: auto_disabled_at stays NULL.
        let (enabled, auto_disabled): (i64, Option<String>) = conn
            .query_row(
                "SELECT enabled, auto_disabled_at FROM automation_jobs WHERE id = ?1",
                rusqlite::params![disabled_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read disabled job");
        assert_eq!(enabled, 0, "already-disabled job must remain disabled");
        assert!(
            auto_disabled.is_none(),
            "already-disabled job must not have auto_disabled_at set by startup check"
        );
    }
}
