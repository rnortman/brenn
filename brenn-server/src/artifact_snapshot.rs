//! Artifact snapshot storage: persist file content and display events in the messages table.
//!
//! When CC calls DisplayFile, we store the raw file content (deduplicated by hash)
//! and a display event. On history replay or explicit load, we reconstruct
//! `ArtifactContent` from the stored data.

use brenn_lib::conversation::{self, MessageDirection};
use brenn_lib::rusqlite::{Connection, OptionalExtension};
use brenn_lib::ws_types::{SnapshotMetadata, WsServerMessage};
use sha2::{Digest, Sha256};

/// Result of storing an artifact snapshot.
pub struct SnapshotResult {
    /// The `id` of the `"artifact"` message (content storage row).
    pub artifact_message_id: i64,
    /// Version number for this file_path (1-indexed).
    pub version: i32,
    /// Total versions of this file_path in the conversation.
    pub total_versions: i32,
    /// Sequence number of the `"artifact_display"` message.
    pub display_seq: i64,
}

/// Store an artifact snapshot: deduplicated content + display event.
///
/// Computes SHA-256 of `content`. If the hash matches the latest stored version
/// for this `(conversation_id, file_path)`, reuses that artifact message.
/// Otherwise inserts a new artifact message with an incremented version.
/// Always inserts an artifact_display message.
///
/// Returns metadata needed for the `ArtifactContent` broadcast.
pub fn store_artifact_snapshot(
    conn: &Connection,
    conversation_id: i64,
    file_path: &str,
    content: &str,
    tool_use_id: &str,
) -> SnapshotResult {
    let content_hash = compute_content_hash(content);

    // Look up the latest artifact for this file_path.
    let latest: Option<(i64, String)> = conn
        .query_row(
            "SELECT id, json_extract(payload, '$.content_hash') \
             FROM messages \
             WHERE conversation_id = ?1 AND msg_type = 'artifact' \
               AND json_extract(payload, '$.file_path') = ?2 \
             ORDER BY seq DESC LIMIT 1",
            (conversation_id, file_path),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .expect("failed to query latest artifact");

    let (artifact_message_id, version) = match latest {
        Some((existing_id, existing_hash)) if existing_hash == content_hash => {
            // Content unchanged — reuse existing artifact message.
            let version: i32 = conn
                .query_row(
                    "SELECT json_extract(payload, '$.version') \
                     FROM messages WHERE id = ?1",
                    [existing_id],
                    |row| row.get(0),
                )
                .expect("failed to read version from existing artifact");
            (existing_id, version)
        }
        _ => {
            // New content (or first version). Count existing versions.
            let existing_count: i32 = conn
                .query_row(
                    "SELECT COUNT(*) FROM messages \
                     WHERE conversation_id = ?1 AND msg_type = 'artifact' \
                       AND json_extract(payload, '$.file_path') = ?2",
                    (conversation_id, file_path),
                    |row| row.get(0),
                )
                .expect("failed to count artifact versions");

            let version = existing_count + 1;
            let payload = serde_json::json!({
                "file_path": file_path,
                "content": content,
                "content_hash": content_hash,
                "version": version,
            });

            let (id, _seq) = conversation::append_message(
                conn,
                conversation_id,
                MessageDirection::Incoming,
                "artifact",
                None,
                Some(tool_use_id),
                &payload.to_string(),
                None,
                None,
                None,
            );
            (id, version)
        }
    };

    // Always insert a display event.
    let display_payload = serde_json::json!({
        "file_path": file_path,
        "artifact_message_id": artifact_message_id,
    });
    let (_display_id, display_seq) = conversation::append_message(
        conn,
        conversation_id,
        MessageDirection::Incoming,
        "artifact_display",
        None,
        Some(tool_use_id),
        &display_payload.to_string(),
        None,
        None,
        None,
    );

    // total_versions = version for the just-inserted case, or we need to count.
    // Since we may have reused an existing artifact, total_versions == version
    // only when we just inserted. For the reuse case, version is the reused
    // version's number, and total_versions is the same count. Either way,
    // total_versions is the count of artifact messages for this file_path.
    let total_versions: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages \
             WHERE conversation_id = ?1 AND msg_type = 'artifact' \
               AND json_extract(payload, '$.file_path') = ?2",
            (conversation_id, file_path),
            |row| row.get(0),
        )
        .expect("failed to count total artifact versions");

    SnapshotResult {
        artifact_message_id,
        version,
        total_versions,
        display_seq,
    }
}

/// Error from loading an artifact snapshot.
///
/// A single variant covers both "not found" and "wrong user" — the query
/// uses a JOIN on user_id, so unauthorized access looks identical to a
/// missing artifact. This avoids leaking existence information.
#[derive(Debug)]
pub enum LoadSnapshotError {
    /// The message_id doesn't exist, isn't an artifact, or belongs to another user.
    NotFound,
}

/// Load an artifact snapshot from the DB and return a ready-to-send `ArtifactContent`.
///
/// `user_id` is the authenticated user — we verify the artifact's conversation
/// belongs to them (or is shared and the app is multiuser). Returns an error
/// (not panic) for missing or unauthorized artifacts, since `message_id` comes
/// from the untrusted browser client.
///
/// `frontmatter_cfg` controls how a YAML frontmatter block at the top of
/// the stored content (if any) is rendered ahead of the body markdown.
// ALLOW: each argument is a distinct piece of context the caller has
// already resolved separately (DB conn, untrusted message id, auth
// user, working dir, app slug, multiuser flag, mount roots, render
// config). Grouping them into a struct would just shift the arg count
// to the struct ctor at the call site.
#[allow(clippy::too_many_arguments)]
pub fn load_artifact_snapshot(
    conn: &Connection,
    message_id: i64,
    user_id: i64,
    working_dir: &std::path::Path,
    slug: &str,
    multiuser: bool,
    mounts: &[crate::artifact::MountRoot],
    frontmatter_cfg: &brenn_lib::config::FrontmatterRenderConfig,
) -> Result<WsServerMessage, LoadSnapshotError> {
    let (conversation_id, seq, payload_str, cwd): (i64, i64, String, Option<String>) = conn
        .query_row(
            "SELECT m.conversation_id, m.seq, m.payload, c.cwd \
             FROM messages m \
             JOIN conversations c ON c.id = m.conversation_id \
             WHERE m.id = ?1 AND m.msg_type = 'artifact' \
               AND c.app_slug = ?4 \
               AND (c.user_id = ?2 OR (?3 AND c.shared = 1))",
            (message_id, user_id, multiuser, slug),
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .expect("failed to query artifact message")
        .ok_or(LoadSnapshotError::NotFound)?;

    let payload: serde_json::Value =
        serde_json::from_str(&payload_str).expect("stored artifact payload must be valid JSON");

    let file_path = payload["file_path"]
        .as_str()
        .expect("artifact payload must have file_path")
        .to_string();
    let content = payload["content"]
        .as_str()
        .expect("artifact payload must have content");
    let version = payload["version"]
        .as_i64()
        .expect("artifact payload must have version") as i32;

    let rendered_html =
        crate::frontmatter::render_markdown_with_frontmatter(content, frontmatter_cfg);

    // Count total versions for this file_path.
    let total_versions: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages \
             WHERE conversation_id = ?1 AND msg_type = 'artifact' \
               AND json_extract(payload, '$.file_path') = ?2",
            (conversation_id, &file_path),
            |row| row.get(0),
        )
        .expect("failed to count total artifact versions");

    let stable_url = cwd.and_then(|cwd| {
        crate::artifact::compute_stable_url(
            &file_path,
            std::path::Path::new(&cwd),
            working_dir,
            mounts,
            slug,
        )
    });

    Ok(WsServerMessage::ArtifactContent {
        file_path,
        rendered_html,
        raw_content: content.to_string(),
        snapshot: Some(SnapshotMetadata {
            message_id,
            version,
            total_versions,
            seq,
            stable_url,
        }),
        seq: None,
    })
}

/// Compute SHA-256 hash of content, prefixed with `sha256:`.
fn compute_content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let hash = hasher.finalize();
    format!("sha256:{}", hex::encode(hash))
}

/// Build the artifact index for a conversation: all artifact files with their versions.
///
/// Returns files ordered by first appearance (first seq in each file's versions).
/// Used to populate the file picker and to derive version counts for history replay.
pub fn get_artifact_index(
    conn: &Connection,
    conversation_id: i64,
) -> Vec<brenn_lib::ws_types::ArtifactFileInfo> {
    use brenn_lib::ws_types::{ArtifactFileInfo, ArtifactVersionInfo};

    let mut stmt = conn
        .prepare(
            "SELECT id, seq, \
                    json_extract(payload, '$.file_path'), \
                    json_extract(payload, '$.version') \
             FROM messages \
             WHERE conversation_id = ?1 AND msg_type = 'artifact' \
             ORDER BY seq",
        )
        .expect("failed to prepare artifact index query");

    let rows: Vec<(i64, i64, String, i32)> = stmt
        .query_map([conversation_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .expect("failed to query artifact index")
        .map(|r| r.expect("failed to read artifact index row"))
        .collect();

    // Group by file_path, preserving first-appearance order.
    let mut files: Vec<ArtifactFileInfo> = Vec::new();
    let mut file_positions: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for (id, seq, file_path, version) in rows {
        let entry = ArtifactVersionInfo {
            message_id: id,
            version,
            seq,
        };
        if let Some(&idx) = file_positions.get(&file_path) {
            files[idx].versions.push(entry);
        } else {
            file_positions.insert(file_path.clone(), files.len());
            files.push(ArtifactFileInfo {
                file_path,
                versions: vec![entry],
            });
        }
    }

    files
}

/// Derive version counts per file_path from an artifact index.
///
/// Convenience wrapper for history replay, which needs a `HashMap<String, i32>`
/// of total versions per file.
pub fn version_counts_from_index(
    index: &[brenn_lib::ws_types::ArtifactFileInfo],
) -> std::collections::HashMap<String, i32> {
    index
        .iter()
        .map(|f| (f.file_path.clone(), f.versions.len() as i32))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::auth::user::create_user;
    use brenn_lib::db::init_db_memory;

    #[test]
    fn first_snapshot_creates_artifact_and_display() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let result = store_artifact_snapshot(&conn, conv_id, "docs/plan.md", "# Plan", "t1");

        assert_eq!(result.version, 1);
        assert_eq!(result.total_versions, 1);

        // Verify both messages were created.
        let messages = conversation::get_messages(&conn, conv_id);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].msg_type, "artifact");
        assert_eq!(messages[1].msg_type, "artifact_display");

        // Verify artifact payload.
        let payload: serde_json::Value = serde_json::from_str(&messages[0].payload).unwrap();
        assert_eq!(payload["file_path"], "docs/plan.md");
        assert_eq!(payload["content"], "# Plan");
        assert_eq!(payload["version"], 1);
        assert!(
            payload["content_hash"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );

        // Verify display payload references the artifact.
        let display_payload: serde_json::Value =
            serde_json::from_str(&messages[1].payload).unwrap();
        assert_eq!(display_payload["artifact_message_id"], messages[0].id);
        assert_eq!(display_payload["file_path"], "docs/plan.md");
    }

    #[test]
    fn unchanged_content_deduplicates() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let r1 = store_artifact_snapshot(&conn, conv_id, "docs/plan.md", "# Plan", "t1");
        let r2 = store_artifact_snapshot(&conn, conv_id, "docs/plan.md", "# Plan", "t2");

        // Same artifact message reused.
        assert_eq!(r1.artifact_message_id, r2.artifact_message_id);
        assert_eq!(r2.version, 1);
        assert_eq!(r2.total_versions, 1);

        // Only 1 artifact message + 2 display messages.
        let messages = conversation::get_messages(&conn, conv_id);
        let artifact_count = messages.iter().filter(|m| m.msg_type == "artifact").count();
        let display_count = messages
            .iter()
            .filter(|m| m.msg_type == "artifact_display")
            .count();
        assert_eq!(artifact_count, 1);
        assert_eq!(display_count, 2);
    }

    #[test]
    fn changed_content_creates_new_version() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let r1 = store_artifact_snapshot(&conn, conv_id, "docs/plan.md", "# Plan v1", "t1");
        let r2 = store_artifact_snapshot(&conn, conv_id, "docs/plan.md", "# Plan v2", "t2");

        assert_ne!(r1.artifact_message_id, r2.artifact_message_id);
        assert_eq!(r1.version, 1);
        assert_eq!(r2.version, 2);
        assert_eq!(r2.total_versions, 2);
    }

    #[test]
    fn multiple_files_independent_versions() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let r1 = store_artifact_snapshot(&conn, conv_id, "a.md", "# A", "t1");
        let r2 = store_artifact_snapshot(&conn, conv_id, "b.md", "# B", "t2");
        let r3 = store_artifact_snapshot(&conn, conv_id, "a.md", "# A v2", "t3");

        assert_eq!(r1.version, 1);
        assert_eq!(r2.version, 1);
        assert_eq!(r3.version, 2);
        assert_eq!(r3.total_versions, 2);
        // b.md still has 1 version.
        assert_eq!(r2.total_versions, 1);
    }

    #[test]
    fn load_artifact_snapshot_returns_correct_content() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let r = store_artifact_snapshot(&conn, conv_id, "docs/test.md", "# Hello\n\nWorld", "t1");

        match load_artifact_snapshot(
            &conn,
            r.artifact_message_id,
            user_id,
            std::path::Path::new("."),
            "test",
            false,
            &[],
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
        .unwrap()
        {
            WsServerMessage::ArtifactContent {
                file_path,
                rendered_html,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "docs/test.md");
                assert!(
                    rendered_html.contains("Hello"),
                    "should render: {rendered_html}"
                );
                let snap = snapshot.expect("should have snapshot");
                assert_eq!(snap.message_id, r.artifact_message_id);
                assert_eq!(snap.version, 1);
                assert_eq!(snap.total_versions, 1);
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn load_artifact_snapshot_with_frontmatter_renders_fm_block() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let content = "---\nstatus: in_progress\npriority: 2\n---\n# Hello\n\nWorld";
        let r = store_artifact_snapshot(&conn, conv_id, "docs/task.md", content, "t1");

        match load_artifact_snapshot(
            &conn,
            r.artifact_message_id,
            user_id,
            std::path::Path::new("."),
            "test",
            false,
            &[],
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
        .unwrap()
        {
            WsServerMessage::ArtifactContent { rendered_html, .. } => {
                assert!(
                    rendered_html.contains("class=\"fm-block\""),
                    "frontmatter rendered: {rendered_html}"
                );
                assert!(
                    rendered_html.contains("<h1>Hello</h1>"),
                    "body still rendered: {rendered_html}"
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn load_artifact_snapshot_with_multiple_versions() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let r1 = store_artifact_snapshot(&conn, conv_id, "f.md", "v1", "t1");
        let r2 = store_artifact_snapshot(&conn, conv_id, "f.md", "v2", "t2");

        // Loading v1 should show total_versions = 2.
        match load_artifact_snapshot(
            &conn,
            r1.artifact_message_id,
            user_id,
            std::path::Path::new("."),
            "test",
            false,
            &[],
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
        .unwrap()
        {
            WsServerMessage::ArtifactContent { snapshot, .. } => {
                let snap = snapshot.unwrap();
                assert_eq!(snap.version, 1);
                assert_eq!(snap.total_versions, 2);
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }

        // Loading v2 should also show total_versions = 2.
        match load_artifact_snapshot(
            &conn,
            r2.artifact_message_id,
            user_id,
            std::path::Path::new("."),
            "test",
            false,
            &[],
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
        .unwrap()
        {
            WsServerMessage::ArtifactContent { snapshot, .. } => {
                let snap = snapshot.unwrap();
                assert_eq!(snap.version, 2);
                assert_eq!(snap.total_versions, 2);
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn load_nonexistent_artifact_returns_not_found() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let result = load_artifact_snapshot(
            &conn,
            99999,
            user_id,
            std::path::Path::new("."),
            "test",
            false,
            &[],
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );
        assert!(matches!(result, Err(LoadSnapshotError::NotFound)));
    }

    #[test]
    fn load_artifact_snapshot_wrong_user_returns_not_found() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, alice, "test", false);

        let r = store_artifact_snapshot(&conn, conv_id, "f.md", "secret", "t1");

        // Alice can load it.
        assert!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                alice,
                std::path::Path::new("."),
                "test",
                false,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            )
            .is_ok()
        );
        // Bob cannot.
        assert!(matches!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                bob,
                std::path::Path::new("."),
                "test",
                false,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            ),
            Err(LoadSnapshotError::NotFound)
        ));
    }

    #[test]
    fn load_artifact_snapshot_multiuser_shared_allows_non_owner() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");
        // Shared conversation in a multiuser app.
        let conv_id = conversation::create_conversation(&conn, alice, "test", true);

        let r = store_artifact_snapshot(&conn, conv_id, "f.md", "shared content", "t1");

        // Bob can load it with multiuser=true.
        assert!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                bob,
                std::path::Path::new("."),
                "test",
                true,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            )
            .is_ok()
        );

        // Bob cannot load it with multiuser=false.
        assert!(matches!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                bob,
                std::path::Path::new("."),
                "test",
                false,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            ),
            Err(LoadSnapshotError::NotFound)
        ));
    }

    #[test]
    fn load_artifact_snapshot_private_conversation_blocks_non_owner() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");
        // Private conversation even in a multiuser app.
        let conv_id = conversation::create_conversation(&conn, alice, "test", false);

        let r = store_artifact_snapshot(&conn, conv_id, "f.md", "private content", "t1");

        // Bob cannot load it even with multiuser=true (conversation is not shared).
        assert!(matches!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                bob,
                std::path::Path::new("."),
                "test",
                true,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            ),
            Err(LoadSnapshotError::NotFound)
        ));
    }

    #[test]
    fn load_artifact_snapshot_cross_app_blocked() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");
        // Shared conversation in app "alpha".
        let conv_id = conversation::create_conversation(&conn, alice, "alpha", true);

        let r = store_artifact_snapshot(&conn, conv_id, "f.md", "content", "t1");

        // Bob tries to load from app "beta" — should fail even with multiuser=true.
        assert!(matches!(
            load_artifact_snapshot(
                &conn,
                r.artifact_message_id,
                bob,
                std::path::Path::new("."),
                "beta",
                true,
                &[],
                &brenn_lib::config::FrontmatterRenderConfig::default(),
            ),
            Err(LoadSnapshotError::NotFound)
        ));
    }

    #[test]
    fn load_artifact_snapshot_mount_file_emits_mount_stable_url() {
        // Regression: an artifact whose stored display path is a mount-slug-
        // prefixed path must come back with the mount-form stable URL on
        // snapshot load. This pins the mount_roots threading through the
        // `artifact_snapshot` callsite.
        let db = init_db_memory();
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("tips.md"), "# Tips").unwrap();

        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);
        conversation::set_init_metadata(&conn, conv_id, "sonnet", cwd.path().to_str().unwrap());

        let r = store_artifact_snapshot(&conn, conv_id, "life/tips.md", "# Tips", "t1");

        let mounts = vec![crate::artifact::MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "life".into(),
        }];

        match load_artifact_snapshot(
            &conn,
            r.artifact_message_id,
            user_id,
            cwd.path(),
            "test",
            false,
            &mounts,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
        .unwrap()
        {
            WsServerMessage::ArtifactContent {
                file_path,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "life/tips.md");
                let snap = snapshot.expect("snapshot metadata");
                assert_eq!(
                    snap.stable_url.as_deref(),
                    Some("/app/test/mount/life/file/tips.md"),
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn artifact_index_query() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        store_artifact_snapshot(&conn, conv_id, "a.md", "v1", "t1");
        store_artifact_snapshot(&conn, conv_id, "a.md", "v2", "t2");
        store_artifact_snapshot(&conn, conv_id, "b.md", "v1", "t3");

        let index = get_artifact_index(&conn, conv_id);
        assert_eq!(index.len(), 2);

        // Files ordered by first appearance.
        assert_eq!(index[0].file_path, "a.md");
        assert_eq!(index[0].versions.len(), 2);
        assert_eq!(index[0].versions[0].version, 1);
        assert_eq!(index[0].versions[1].version, 2);

        assert_eq!(index[1].file_path, "b.md");
        assert_eq!(index[1].versions.len(), 1);
        assert_eq!(index[1].versions[0].version, 1);

        // Derive version counts.
        let counts = version_counts_from_index(&index);
        assert_eq!(counts.get("a.md"), Some(&2));
        assert_eq!(counts.get("b.md"), Some(&1));
    }

    #[test]
    fn artifact_index_empty_conversation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let index = get_artifact_index(&conn, conv_id);
        assert!(index.is_empty());
    }

    #[test]
    fn parent_tool_use_id_stored() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        store_artifact_snapshot(&conn, conv_id, "f.md", "content", "toolu_abc123");

        let messages = conversation::get_messages(&conn, conv_id);
        for msg in &messages {
            assert_eq!(msg.parent_tool_use_id.as_deref(), Some("toolu_abc123"));
        }
    }
}
