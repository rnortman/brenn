//! Bodies in the durable store: the storage class each kind of body lands as,
//! the decoders that read it back through every SELECT, the full-text index
//! over text rows only, the eviction archive of a stored non-text row, and the
//! migration of a database created before bodies could be bytes.

use super::helpers::*;
use crate::db::*;
use brenn_lib::messaging::config::{Depth, Sink};
use brenn_lib::messaging::{ChannelEntry, ChannelScheme, MessageBody, Urgency, canonical_address};
use chrono::{DateTime, Duration, Utc};
use rusqlite::Connection;
use uuid::Uuid;

/// Bytes that are not UTF-8.
const NOT_UTF8: &[u8] = &[0xff, 0x00, 0xfe, 0x80, 0x41];

fn binary(bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(Some("application/octet-stream"), bytes).unwrap()
}

fn markdown(text: &str) -> MessageBody {
    MessageBody::from_parts(Some("text/markdown"), text.as_bytes()).unwrap()
}

fn channel(conn: &Connection, name: &str) -> Uuid {
    let uuid = Uuid::new_v4();
    upsert_channels(
        conn,
        &[ChannelEntry {
            uuid,
            address: canonical_address(name),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );
    uuid
}

fn insert(
    conn: &Connection,
    channel_uuid: Uuid,
    body: &MessageBody,
    deliver_after: Option<DateTime<Utc>>,
) -> InsertedMessage {
    insert_message(
        conn,
        channel_uuid,
        "src",
        "alice",
        body,
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        deliver_after,
        None,
        utc_to_ns(Utc::now()),
    )
}

/// `(typeof(body), body bytes, content_type)` of row `id`, read raw.
fn stored(conn: &Connection, id: i64) -> (String, Vec<u8>, Option<String>) {
    conn.query_row(
        "SELECT typeof(body), CAST(body AS BLOB), content_type FROM messaging_messages
         WHERE id = ?1",
        rusqlite::params![id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .unwrap()
}

fn by_uuid(conn: &Connection, uuid: Uuid) -> MessageBody {
    load_envelope_by_uuid(conn, uuid).unwrap().body
}

/// Ids of the rows whose body matches `term`, through the index joined to the
/// table as the history query joins it.
fn fts_ids(conn: &Connection, term: &str) -> Vec<i64> {
    conn.prepare(
        "SELECT m.id FROM messaging_messages_fts f
         JOIN messaging_messages m ON m.id = f.rowid
         WHERE f.body MATCH ?1 ORDER BY m.id",
    )
    .unwrap()
    .query_map(rusqlite::params![term], |r| r.get(0))
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

/// The index agrees with its content: FTS5's content-comparing integrity check
/// passes, and it holds one document per TEXT row and none for a BLOB row.
fn assert_index_consistent(conn: &Connection) {
    conn.execute(
        "INSERT INTO messaging_messages_fts(messaging_messages_fts, rank)
         VALUES ('integrity-check', 1)",
        [],
    )
    .expect("FTS integrity-check against the content view");
    let indexed: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages_fts_docsize",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let text_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE typeof(body) = 'text'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(indexed, text_rows, "the index holds exactly the text rows");
}

#[test]
fn a_body_is_stored_as_its_kind_byte_exact() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "classes");

    let plain = insert(&conn, ch, &"plain words".into(), None);
    let typed = insert(&conn, ch, &markdown("# a heading"), None);
    let raw = insert(&conn, ch, &binary(NOT_UTF8), None);
    // Valid UTF-8 under a non-text type is still a non-text body.
    let utf8_bytes = insert(&conn, ch, &binary(b"looks like text"), None);

    assert_eq!(
        stored(&conn, plain.id),
        ("text".into(), b"plain words".to_vec(), None)
    );
    assert_eq!(
        stored(&conn, typed.id),
        (
            "text".into(),
            b"# a heading".to_vec(),
            Some("text/markdown".into())
        )
    );
    assert_eq!(
        stored(&conn, raw.id),
        (
            "blob".into(),
            NOT_UTF8.to_vec(),
            Some("application/octet-stream".into())
        )
    );
    assert_eq!(
        stored(&conn, utf8_bytes.id),
        (
            "blob".into(),
            b"looks like text".to_vec(),
            Some("application/octet-stream".into())
        )
    );

    assert_eq!(by_uuid(&conn, plain.uuid), MessageBody::text("plain words"));
    assert_eq!(by_uuid(&conn, typed.uuid), markdown("# a heading"));
    assert_eq!(by_uuid(&conn, raw.uuid), binary(NOT_UTF8));
    assert_eq!(by_uuid(&conn, utf8_bytes.uuid), binary(b"looks like text"));
}

#[test]
#[should_panic(expected = "is stored as BLOB")]
fn a_blob_row_without_a_content_type_panics_on_read() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "corrupt-blob");
    let row = insert(&conn, ch, &"x".into(), None);
    conn.execute(
        "UPDATE messaging_messages SET body = X'6869', content_type = NULL WHERE id = ?1",
        rusqlite::params![row.id],
    )
    .unwrap();
    load_envelope_by_uuid(&conn, row.uuid);
}

#[test]
#[should_panic(expected = "is stored as TEXT")]
fn a_text_row_with_a_non_text_content_type_panics_on_read() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "corrupt-text");
    let row = insert(&conn, ch, &"x".into(), None);
    conn.execute(
        "UPDATE messaging_messages SET content_type = 'image/png' WHERE id = ?1",
        rusqlite::params![row.id],
    )
    .unwrap();
    load_envelope_by_uuid(&conn, row.uuid);
}

/// A typed text row and a non-text row read back with the right body, content
/// type, rowid and retention sequence through every read that decodes an
/// envelope, so a `content_type` column placed off by one anywhere fails here.
#[test]
fn every_envelope_read_decodes_body_content_type_and_trailing_columns() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "placement");
    let now = Utc::now();

    let kinds = [markdown("typed *text*"), binary(NOT_UTF8)];

    // Retained rows: the history query base and the retained window.
    for body in &kinds {
        let row = insert(&conn, ch, body, None);
        let sql = format!("{SELECT_ENVELOPE_BASE}WHERE m.uuid = ?1");
        let envelope = conn
            .query_row(&sql, [row.uuid.as_bytes().to_vec()], row_to_envelope)
            .unwrap();
        assert_eq!(&envelope.body, body, "history query base");

        let tail = load_channel_retained_tail(&conn, ch, Depth::Unbounded);
        let (id, envelope) = tail.iter().find(|(_, e)| e.message_id == row.uuid).unwrap();
        assert_eq!(*id, row.id, "retained window rowid");
        assert_eq!(&envelope.body, body, "retained window body");

        let window = load_channel_retained_window_seq(&conn, ch, Depth::Unbounded);
        let (seq, envelope) = window
            .iter()
            .find(|(_, e)| e.message_id == row.uuid)
            .unwrap();
        assert_eq!(Some(*seq), row.retained_seq, "retained window seq");
        assert_eq!(&envelope.body, body);

        assert_eq!(&by_uuid(&conn, row.uuid), body, "load by uuid");
    }

    // Parked rows, one not yet due and one due: the pending list, the deferred
    // view, and the due-release read.
    for body in &kinds {
        let future = insert(&conn, ch, body, Some(now + Duration::hours(1)));
        let due = insert(&conn, ch, body, Some(now - Duration::seconds(1)));

        let pending = list_pending_messages_for_sender(&conn, "alice", Some(ch));
        for row in [&future, &due] {
            let envelope = pending.iter().find(|e| e.message_id == row.uuid).unwrap();
            assert_eq!(&envelope.body, body, "pending list");
        }

        let deferred = list_deferred_for_sender(&conn, ch, "alice");
        for row in [&future, &due] {
            let entry = deferred
                .iter()
                .find(|d| d.envelope.message_id == row.uuid)
                .unwrap();
            assert_eq!(entry.message_id, row.id, "deferred view rowid");
            assert_eq!(&entry.envelope.body, body, "deferred view body");
        }

        let released = release_due_for_channel(&conn, ch, now);
        assert_eq!(released.len(), 1, "only the due row releases");
        assert_eq!(released[0].message_id, due.id, "due-release rowid");
        assert_eq!(&released[0].envelope.body, body, "due-release body");
        assert_eq!(
            released[0].retained_seq,
            message_retained_seq(&conn, due.uuid),
            "due-release seq"
        );
    }
}

/// An edit replaces the body and its content type together, in every
/// combination of kinds and through both edit paths; the index follows.
#[test]
fn edits_replace_body_and_content_type_and_keep_the_index_on_text_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "edits");
    let later = Utc::now() + Duration::hours(1);

    let text_to_text = insert(&conn, ch, &"alpha".into(), Some(later));
    let text_to_binary = insert(&conn, ch, &markdown("charlie"), Some(later));
    let binary_to_text = insert(&conn, ch, &binary(b"echo"), Some(later));
    let binary_to_binary = insert(&conn, ch, &binary(b"golf"), Some(later));
    let retained_text = insert(&conn, ch, &"india".into(), None);
    insert(&conn, ch, &binary(b"juliet"), None);
    assert_index_consistent(&conn);
    assert_eq!(fts_ids(&conn, "alpha"), [text_to_text.id]);
    assert_eq!(fts_ids(&conn, "charlie"), [text_to_binary.id]);
    assert!(fts_ids(&conn, "echo").is_empty());
    assert!(fts_ids(&conn, "juliet").is_empty());

    let edit = |row: &InsertedMessage, body: MessageBody| {
        assert!(edit_deferred(
            &conn,
            ch,
            row.id,
            Some(&body),
            None,
            Utc::now()
        ));
        assert_index_consistent(&conn);
    };
    let update = |row: &InsertedMessage, body: MessageBody| {
        update_parked_message(
            &conn,
            row.id,
            "alice",
            &EditFieldsApplied {
                body: Some(&body),
                reply_to_uuid: None,
                deliver_after: None,
                delivery_deadline: None,
                urgency: None,
            },
        );
        assert_index_consistent(&conn);
    };
    edit(&text_to_text, markdown("bravo"));
    update(&text_to_binary, binary(b"delta"));
    edit(&binary_to_text, "foxtrot".into());
    update(&binary_to_binary, binary(NOT_UTF8));

    assert_eq!(by_uuid(&conn, text_to_text.uuid), markdown("bravo"));
    assert_eq!(by_uuid(&conn, text_to_binary.uuid), binary(b"delta"));
    assert_eq!(
        by_uuid(&conn, binary_to_text.uuid),
        MessageBody::text("foxtrot"),
        "an untyped replacement clears the content type"
    );
    assert_eq!(by_uuid(&conn, binary_to_binary.uuid), binary(NOT_UTF8));

    assert_eq!(fts_ids(&conn, "bravo"), [text_to_text.id]);
    assert_eq!(fts_ids(&conn, "foxtrot"), [binary_to_text.id]);
    assert_eq!(fts_ids(&conn, "india"), [retained_text.id]);
    for gone in ["alpha", "charlie", "delta", "echo", "golf", "juliet"] {
        assert!(fts_ids(&conn, gone).is_empty(), "{gone} is not indexed");
    }

    // Deleting rows of both kinds keeps the index equal to its content.
    conn.execute(
        "DELETE FROM messaging_messages WHERE id IN (?1, ?2)",
        rusqlite::params![text_to_text.id, text_to_binary.id],
    )
    .unwrap();
    assert_index_consistent(&conn);
    assert!(fts_ids(&conn, "bravo").is_empty());

    // The index's repair command reproduces it from the view.
    conn.execute(
        "INSERT INTO messaging_messages_fts(messaging_messages_fts) VALUES ('rebuild')",
        [],
    )
    .unwrap();
    assert_index_consistent(&conn);
    assert_eq!(fts_ids(&conn, "foxtrot"), [binary_to_text.id]);
}

/// Eviction archives a typed text row with its content type and a non-text row
/// as base64 of its stored bytes, deletes both, and leaves the index consistent.
#[test]
fn eviction_archives_stored_bodies_of_both_kinds_and_deletes_them() {
    use base64ct::Encoding as _;

    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = channel(&conn, "evict-kinds");
    insert(&conn, ch, &markdown("kilo lima"), None);
    insert(&conn, ch, &binary(NOT_UTF8), None);
    let kept = insert(&conn, ch, &"mike".into(), None);
    assert_index_consistent(&conn);

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let eviction = bus_gc_evict_channel(
        &conn,
        ch,
        "brenn:evict-kinds",
        ChannelScheme::Brenn,
        1,
        Sink::Archive,
        Some(tmp.path()),
    );
    assert_eq!(eviction.messages_evicted, 2);
    assert_index_consistent(&conn);
    assert!(fts_ids(&conn, "kilo").is_empty());
    assert_eq!(fts_ids(&conn, "mike"), [kept.id]);

    let content = std::fs::read_to_string(tmp.path()).unwrap();
    let lines: Vec<serde_json::Value> = content
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["body"], "kilo lima");
    assert_eq!(lines[0]["content_type"], "text/markdown");
    assert!(lines[1].get("body").is_none());
    assert_eq!(lines[1]["content_type"], "application/octet-stream");
    let decoded = base64ct::Base64::decode_vec(lines[1]["body_base64"].as_str().unwrap()).unwrap();
    assert_eq!(decoded, NOT_UTF8);
}

// ---------------------------------------------------------------------------
// Migration of a database created before bodies could be bytes
// ---------------------------------------------------------------------------

/// The messaging DDL's first batch exactly as the release before byte bodies
/// created it: the tables, the FTS table over `messaging_messages`, and the
/// three triggers that index every body.
const PRE_BYTES_MESSAGING_DDL: &str = r#"
        CREATE TABLE IF NOT EXISTS messaging_channels (
            uuid            BLOB PRIMARY KEY,
            address         TEXT NOT NULL UNIQUE,
            description     TEXT,
            created_at      TEXT NOT NULL,
            transport_type  TEXT NOT NULL DEFAULT 'brenn',
            -- Durable resume domain: the epoch is minted with the row and dies
            -- only with it; last_retained_seq is the per-channel dense-sequence
            -- allocator and the persisted high-water.
            resume_epoch        BLOB NOT NULL,
            last_retained_seq   INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_channels_address
            ON messaging_channels(address);

        CREATE TABLE IF NOT EXISTS messaging_messages (
            id                  INTEGER PRIMARY KEY,
            uuid                BLOB NOT NULL UNIQUE,
            channel_uuid        BLOB REFERENCES messaging_channels(uuid),
            source              TEXT NOT NULL,
            sender              TEXT NOT NULL,
            body                TEXT NOT NULL,
            urgency             TEXT NOT NULL CHECK(urgency IN ('very-low','low','normal','high')),
            reply_to_uuid       BLOB REFERENCES messaging_channels(uuid),
            delivery_deadline   TEXT,
            deliver_after       TEXT,
            publish_ts_ns       INTEGER NOT NULL,
            created_at          TEXT NOT NULL,
            envelope_type       TEXT NOT NULL DEFAULT 'brenn',
            ingress_source      TEXT,
            ingress_summary     TEXT,
            -- Dense per-channel retention-order sequence, assigned when the row
            -- enters retention (append commit, or release for a parked row);
            -- NULL while parked. Carries retention order, which the rowid cannot.
            retained_seq        INTEGER,
            -- Carried user-interaction authority, NULL on nearly every row. The
            -- value set is `Impetus::as_str`; no CHECK, because the variant set
            -- is additive by design and the decoder panics on an unknown value
            -- (the host wrote every row).
            impetus             TEXT,
            -- Non-brenn envelope rows must never have deliver_after or delivery_deadline;
            -- those are bus-only dispatch fields. This constraint makes the
            -- invariant machine-enforced rather than convention-enforced.
            CHECK(envelope_type = 'brenn' OR (deliver_after IS NULL AND delivery_deadline IS NULL))
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_messages_channel_ts
            ON messaging_messages(channel_uuid, publish_ts_ns DESC);
        CREATE INDEX IF NOT EXISTS idx_messaging_messages_deliver_after
            ON messaging_messages(deliver_after)
            WHERE deliver_after IS NOT NULL;

        CREATE VIRTUAL TABLE IF NOT EXISTS messaging_messages_fts USING fts5(
            body,
            content='messaging_messages',
            content_rowid='id'
        );
        CREATE TRIGGER IF NOT EXISTS messaging_messages_ai
            AFTER INSERT ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(rowid, body) VALUES (new.id, new.body);
            END;
        CREATE TRIGGER IF NOT EXISTS messaging_messages_ad
            AFTER DELETE ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(messaging_messages_fts, rowid, body)
                VALUES ('delete', old.id, old.body);
            END;
        CREATE TRIGGER IF NOT EXISTS messaging_messages_au
            AFTER UPDATE OF body ON messaging_messages BEGIN
                INSERT INTO messaging_messages_fts(messaging_messages_fts, rowid, body)
                VALUES ('delete', old.id, old.body);
                INSERT INTO messaging_messages_fts(rowid, body) VALUES (new.id, new.body);
            END;

        -- Durable dynamic (runtime-created) subscriptions, carrying a
        -- transport-specific MQTT `qos` column (NULL for brenn:/webhook:) and a
        -- created_at timestamp. CRUCIALLY this table is NOT truncated at boot:
        -- it is the durable truth for dynamic subs, with its own independent
        -- lifecycle. The boot merge folds these rows into the directory;
        -- runtime subscribe/unsubscribe writes here. Registration state, not
        -- position state — where a subscriber stands is
        -- messaging_subscriber_cursors' answer.
        CREATE TABLE IF NOT EXISTS messaging_dynamic_subscriptions (
            channel_uuid   BLOB NOT NULL REFERENCES messaging_channels(uuid),
            app_slug       TEXT NOT NULL,
            push_depth     TEXT NOT NULL,
            retain_depth   TEXT NOT NULL,
            noise          TEXT NOT NULL CHECK(noise IN ('silent','metered','alarm')),
            wake_min       TEXT NOT NULL CHECK(wake_min IN ('very-low','low','normal','high','never')),
            qos            INTEGER CHECK(qos IN (0,1,2)),
            created_at     TEXT NOT NULL,
            PRIMARY KEY (channel_uuid, app_slug)
        );
        CREATE INDEX IF NOT EXISTS idx_messaging_dynamic_subscriptions_app
            ON messaging_dynamic_subscriptions(app_slug);

        CREATE TABLE IF NOT EXISTS messaging_send_budget (
            conversation_id  INTEGER PRIMARY KEY REFERENCES conversations(id),
            remaining        INTEGER NOT NULL,
            last_reset_at    TEXT NOT NULL
        );
"#;

/// Every schema object's definition, for comparing two migration passes.
fn schema_snapshot(conn: &Connection) -> Vec<(String, String, Option<String>)> {
    conn.prepare("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn object_sql(conn: &Connection, name: &str) -> String {
    conn.query_row(
        "SELECT sql FROM sqlite_master WHERE name = ?1",
        rusqlite::params![name],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn a_database_from_before_byte_bodies_migrates_and_serves_its_rows_unchanged() {
    let conn = brenn_db::open_connection_memory();
    brenn_lib::db::run_slice_migrations(&conn);
    conn.execute_batch(PRE_BYTES_MESSAGING_DDL)
        .expect("create the pre-bytes messaging schema");
    let ch = Uuid::new_v4();
    let (m1, m2) = (Uuid::new_v4(), Uuid::new_v4());
    conn.execute_batch(&format!(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch,
                                         last_retained_seq)
         VALUES (X'{ch}', 'brenn:old', 'now', X'{ep}', 2);
         INSERT INTO messaging_messages
             (id, uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns,
              created_at, retained_seq)
         VALUES (1, X'{m1}', X'{ch}', 's', 'alice', 'november oscar', 'low', 1, 'now', 1),
                (2, X'{m2}', X'{ch}', 's', 'alice', '{{\"papa\":1}}', 'low', 2, 'now', 2);",
        ch = ch.simple(),
        ep = Uuid::new_v4().simple(),
        m1 = m1.simple(),
        m2 = m2.simple(),
    ))
    .expect("seed rows the way the old release wrote them");
    assert_eq!(
        fts_ids(&conn, "november"),
        [1],
        "the old index serves the row"
    );

    run_messaging_migrations(&conn);

    let sql = format!("{SELECT_ENVELOPE_BASE}WHERE m.uuid = ?1");
    for (uuid, text) in [(m1, "november oscar"), (m2, "{\"papa\":1}")] {
        let envelope = conn
            .query_row(&sql, [uuid.as_bytes().to_vec()], row_to_envelope)
            .unwrap();
        assert_eq!(envelope.body, MessageBody::text(text), "old row unchanged");
    }
    assert_eq!(fts_ids(&conn, "november"), [1]);
    assert_eq!(fts_ids(&conn, "papa"), [2]);
    assert!(
        object_sql(&conn, "messaging_messages_fts").contains("content='messaging_messages_text'"),
        "the index's content is the text view"
    );
    for trigger in ["messaging_messages_ai", "messaging_messages_au"] {
        assert!(
            object_sql(&conn, trigger).contains("typeof(new.body) = 'text'"),
            "{trigger} is the current form"
        );
    }
    for trigger in ["messaging_messages_ad", "messaging_messages_au"] {
        assert!(
            object_sql(&conn, trigger).contains("typeof(old.body) = 'text'"),
            "{trigger} is the current form"
        );
    }
    assert_index_consistent(&conn);
    assert!(
        object_sql(&conn, NON_TEXT_INDEX).contains("WHERE typeof(body) = 'blob'"),
        "the non-text channel index is built on a migrated database"
    );
    assert!(channels_holding_non_text_rows(&conn).is_empty());

    let before = schema_snapshot(&conn);
    run_messaging_migrations(&conn);
    assert_eq!(
        schema_snapshot(&conn),
        before,
        "a second pass changes nothing"
    );
    assert_index_consistent(&conn);
    assert_eq!(fts_ids(&conn, "november"), [1]);

    // The migrated store takes a non-text row without indexing it.
    let row = insert(&conn, ch, &binary(b"quebec"), None);
    assert_eq!(by_uuid(&conn, row.uuid), binary(b"quebec"));
    assert!(fts_ids(&conn, "quebec").is_empty());
    assert_index_consistent(&conn);
}

const NON_TEXT_INDEX: &str = "idx_messaging_messages_non_text_channel";

#[test]
fn a_fresh_database_has_the_non_text_channel_index() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    assert!(
        object_sql(&conn, NON_TEXT_INDEX).contains("WHERE typeof(body) = 'blob'"),
        "the partial index over BLOB bodies"
    );
}

/// The query names exactly the channels holding a non-text row, visible or
/// parked, and a text row of any type does not count.
#[test]
fn channels_holding_non_text_rows_names_exactly_those_channels() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let untyped = channel(&conn, "untyped");
    let typed = channel(&conn, "typed");
    let visible = channel(&conn, "visible");
    let parked = channel(&conn, "parked");
    let both = channel(&conn, "both");
    channel(&conn, "empty");

    insert(&conn, untyped, &MessageBody::text("alpha"), None);
    insert(&conn, typed, &markdown("# bravo"), None);
    insert(&conn, visible, &binary(NOT_UTF8), None);
    insert(&conn, visible, &MessageBody::text("charlie"), None);
    insert(
        &conn,
        parked,
        &binary(NOT_UTF8),
        Some(Utc::now() + Duration::hours(1)),
    );
    insert(&conn, both, &binary(b"delta"), None);
    insert(
        &conn,
        both,
        &binary(b"echo"),
        Some(Utc::now() + Duration::hours(1)),
    );

    let mut expected = vec![visible, parked, both];
    expected.sort();
    assert_eq!(channels_holding_non_text_rows(&conn), expected);
}

/// The planner reads the partial index rather than scanning the table.
#[test]
fn the_non_text_channel_query_uses_the_partial_index() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let plan: Vec<String> = conn
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {}",
            crate::db::bootstrap::NON_TEXT_CHANNELS_SQL
        ))
        .unwrap()
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(
        plan.iter().any(|step| step.contains(NON_TEXT_INDEX)),
        "{plan:?}"
    );
}
