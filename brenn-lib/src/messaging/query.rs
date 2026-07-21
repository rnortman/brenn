//! `MessageQueryChannel` implementation.

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

use super::config::Depth;
use super::db::{ns_to_utc, utc_to_ns};
use super::{ChannelEntry, MessageEnvelope, Messenger};

/// Common SELECT base for `messaging_messages` reads that feed [`row_to_envelope`].
///
/// Covers columns 0–12 (m.uuid .. m.envelope_type), the mandatory channel JOIN, and the
/// optional reply-to LEFT JOIN. Terminates with a trailing space **before** any FTS JOIN or
/// `WHERE` clause so callers can append either immediately.
///
/// Column layout matches [`row_to_envelope`]'s documented 0–12 contract exactly; do not
/// reorder or add columns here without updating that decoder and the byte-identity tests.
pub(super) const SELECT_ENVELOPE_BASE: &str = "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency, \
            m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns, \
            c.address, rc.address, m.envelope_type \
     FROM messaging_messages m \
     JOIN messaging_channels c ON c.uuid = m.channel_uuid \
     LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid ";

/// Common ORDER BY + LIMIT tail shared by both read sites.
///
/// Callers append `WHERE m.channel_uuid = ?` (and any filter clauses) before this.
pub(super) const SELECT_ENVELOPE_ORDER_LIMIT_TAIL: &str =
    "ORDER BY m.publish_ts_ns DESC, m.id DESC LIMIT ?";

/// Query parameters as parsed from the MCP tool input. Matches the
/// `mcp__brenn__MessageQueryChannel` schema (§3.3).
#[derive(Debug, Clone)]
pub struct MessageQuery {
    /// Channel address (`brenn:<name>`, `mqtt:<client>:<topic>`,
    /// `webhook:<endpoint>`, …).
    pub channel: String,
    /// Mandatory — capped at 500 by the tool wrapper.
    pub limit: u32,
    pub before: Option<DateTime<Utc>>,
    pub after: Option<DateTime<Utc>>,
    pub sender: Option<String>,
    /// FTS5 MATCH expression. Errors propagate up as `QueryError::Fts`.
    pub search: Option<String>,
    /// App slug of the caller. Identifies the app whose policy gates this read:
    /// the read is allowed only if `AppPolicy::allows_channel_access` holds for
    /// the calling app on this channel (the same grant+ACL pair as
    /// subscription-holding). Also used to resolve the `retain_depth` clamp: if
    /// the calling app has a subscription on this channel, the clamp is the
    /// subscriber's resolved `retain_depth`; otherwise the channel's
    /// `standing_retain_depth` is used.
    pub calling_app_slug: String,
}

#[derive(Debug)]
pub enum QueryError {
    UnknownChannel(String),
    Fts(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::UnknownChannel(addr) => write!(f, "unknown channel {addr:?}"),
            QueryError::Fts(e) => write!(f, "FTS query failed: {e}"),
        }
    }
}

impl Messenger {
    /// Run a query against the message store. Sort order is always
    /// `publish_ts_ns DESC` (most recent first).
    ///
    /// Reads are ACL-gated: the calling app must have channel access
    /// (`AppPolicy::allows_channel_access`) — the same grant + covering matcher
    /// pair that gates subscription-holding and delivery. A denied read is
    /// reported as `UnknownChannel`, byte-identical to a genuinely-unknown
    /// channel, so ACL denial does not leak channel existence. `pwa_push:` is an
    /// egress-only protocol: it is denied by the access gate here and read only
    /// through `PwaPushChannelGet`.
    pub async fn query(&self, q: &MessageQuery) -> Result<Vec<MessageEnvelope>, QueryError> {
        // The calling app's policy is plumbed by the host from the bridge, never
        // from tool input, so an unregistered slug is a host wiring bug — panic
        // rather than silently deny (same argument as list_accessible_channels).
        let policy = self.app_policy(&q.calling_app_slug).unwrap_or_else(|| {
            panic!(
                "Messenger::query: calling app {:?} is not a registered app — host \
                 wiring bug (calling_app_slug is host-plumbed from the bridge, never \
                 tool input)",
                q.calling_app_slug
            )
        });
        if !policy.allows_channel_access(&q.channel) {
            tracing::warn!(
                app = %q.calling_app_slug,
                address = %q.channel,
                "channel read denied by policy"
            );
            return Err(QueryError::UnknownChannel(q.channel.clone()));
        }
        let entry = self
            .directory
            .resolve(&q.channel)
            .ok_or_else(|| QueryError::UnknownChannel(q.channel.clone()))?;
        // Resolve the retain_depth clamp.
        let clamp = resolve_retain_depth_clamp(&entry, &q.calling_app_slug);
        let conn = self.db.lock().await;
        run_query(&conn, entry.uuid, q, clamp)
    }
}

/// Resolve the `retain_depth` clamp for a query.
///
/// Precedence:
/// 1. If the calling app is a subscriber of the channel (per the channel
///    directory entry) → clamp to that subscriber's resolved `retain_depth`.
///    The directory entry carries the resolved per-subscriber depth uniformly
///    for static (`brenn:`/`webhook:`/`mqtt:`) and dynamic subscriptions on
///    every transport.
/// 2. Else (non-subscriber caller) → clamp to the channel's `standing_retain_depth`.
///
/// Only `SubscriberEntryKind::App` entries match: the calling slug is always an
/// app slug, and app/WASM/surface slugs are distinct config namespaces, so a
/// coincidental slug collision with a WASM or surface subscriber must not leak
/// that subscriber's depth to the app.
fn resolve_retain_depth_clamp(entry: &ChannelEntry, calling_app_slug: &str) -> Depth {
    if let Some(sub) = entry.app_subscriber(calling_app_slug) {
        return sub.retain_depth;
    }
    entry.resolved_channel.standing_retain_depth
}

fn run_query(
    conn: &Connection,
    channel_uuid: Uuid,
    q: &MessageQuery,
    retain_depth_clamp: Depth,
) -> Result<Vec<MessageEnvelope>, QueryError> {
    let mut sql = String::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    // c.address resolves the channel UUID → address string.
    // rc.address resolves the nullable reply_to_uuid → address string (NULL when not a reply).
    sql.push_str(SELECT_ENVELOPE_BASE);
    if q.search.is_some() {
        sql.push_str("JOIN messaging_messages_fts fts ON fts.rowid = m.id ");
    }
    sql.push_str("WHERE m.channel_uuid = ? ");
    params.push(Box::new(channel_uuid.as_bytes().to_vec()));

    if let Some(before) = q.before {
        sql.push_str("AND m.publish_ts_ns < ? ");
        params.push(Box::new(utc_to_ns(before)));
    }
    if let Some(after) = q.after {
        sql.push_str("AND m.publish_ts_ns > ? ");
        params.push(Box::new(utc_to_ns(after)));
    }
    if let Some(sender) = &q.sender {
        sql.push_str("AND m.sender = ? ");
        params.push(Box::new(sender.clone()));
    }
    if let Some(search) = &q.search {
        sql.push_str("AND fts.messaging_messages_fts MATCH ? ");
        params.push(Box::new(search.clone()));
    }

    // Apply the retain_depth clamp (design §2.9): effective limit = min(q.limit, clamp).
    // `Unbounded` clamp = no extra restriction (only the caller-provided limit applies).
    let effective_limit: i64 = match retain_depth_clamp {
        Depth::Unbounded => q.limit as i64,
        Depth::Bounded(n) => (q.limit as u64).min(n) as i64,
    };
    sql.push_str(SELECT_ENVELOPE_ORDER_LIMIT_TAIL);
    params.push(Box::new(effective_limit));

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| QueryError::Fts(format!("prepare: {e}")))?;
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| &**p as _).collect();
    let rows = stmt
        .query_map(rusqlite::params_from_iter(param_refs), row_to_envelope)
        .map_err(|e| QueryError::Fts(format!("query: {e}")))?;

    let mut out = Vec::with_capacity(effective_limit as usize);
    for r in rows {
        let env = r.map_err(|e| QueryError::Fts(format!("row: {e}")))?;
        out.push(env);
    }
    Ok(out)
}

/// Decode one SELECT row (columns 0–12) into a [`MessageEnvelope`].
///
/// Column layout expected:
/// - 0: m.uuid (bytes)  1: m.channel_uuid (bytes, unused)  2: m.source  3: m.sender
/// - 4: m.body  5: m.urgency  6: m.reply_to_uuid (bytes, unused)
/// - 7: m.delivery_deadline  8: m.deliver_after  9: m.publish_ts_ns
/// - 10: c.address (channel address string)  11: rc.address (reply_to, nullable)
/// - 12: m.envelope_type
pub(crate) fn row_to_envelope(row: &rusqlite::Row) -> rusqlite::Result<MessageEnvelope> {
    let msg_uuid_bytes: Vec<u8> = row.get(0)?;
    // col 1 (channel_uuid bytes) is selected but not used here — address is in col 10.
    let source: String = row.get(2)?;
    let sender: String = row.get(3)?;
    let body: String = row.get(4)?;
    let urgency_str: String = row.get(5)?;
    // col 6 (reply_to_uuid bytes) is selected but not used here — address is in col 11.
    let delivery_deadline_s: Option<String> = row.get(7)?;
    let deliver_after_s: Option<String> = row.get(8)?;
    let publish_ts_ns: i64 = row.get(9)?;
    // Resolved via JOIN messaging_channels c ON c.uuid = m.channel_uuid.
    let channel: String = row.get(10)?;
    // Resolved via LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid;
    // NULL when not a reply.
    let reply_to: Option<String> = row.get(11)?;
    let envelope_type_str: String = row.get(12)?;

    let message_id = Uuid::from_slice(&msg_uuid_bytes)
        .unwrap_or_else(|e| panic!("messaging: query row uuid malformed: {e}"));
    let urgency = super::Urgency::parse(&urgency_str)
        .unwrap_or_else(|| panic!("messaging: invalid urgency {urgency_str:?}"));
    let delivery_deadline = delivery_deadline_s.and_then(|s| super::db::parse_rfc3339(&s));
    let deliver_after = deliver_after_s.and_then(|s| super::db::parse_rfc3339(&s));
    let envelope_type = super::ChannelScheme::parse(&envelope_type_str).unwrap_or_else(|| {
        panic!("messaging: unknown envelope_type {envelope_type_str:?} — host wrote every row")
    });

    Ok(MessageEnvelope {
        message_id,
        source,
        channel,
        sender,
        publish_ts: ns_to_utc(publish_ts_ns),
        body,
        reply_to,
        delivery_deadline,
        deliver_after,
        urgency,
        envelope_type,
    })
}

/// Minimal `WakeRouter` stub for use in tests and test fixtures.
///
/// Available under `#[cfg(test)]` or when the `testutils` feature is enabled.
#[cfg(any(test, feature = "testutils"))]
pub struct NoopWakeRouter;

#[cfg(any(test, feature = "testutils"))]
#[async_trait::async_trait]
impl crate::messaging::WakeRouter for NoopWakeRouter {
    async fn deliver(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
        _: &crate::messaging::MessageEnvelope,
        _push_id: i64,
        _seq: i64,
    ) -> Result<bool, String> {
        Ok(false)
    }

    async fn deliver_ingress(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
        _event: &crate::messaging::ingress::Event,
    ) -> Result<bool, String> {
        Ok(false)
    }

    fn spawn_eager_wake(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
    ) {
    }

    fn delivery_shape(
        &self,
        key: &crate::messaging::SubscriberEntryKind,
    ) -> crate::messaging::DeliveryShape {
        crate::messaging::default_delivery_shape(key)
    }

    fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db::init_db_memory;
    use crate::messaging::canonical_address;
    use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use crate::messaging::db::{insert_message_with_pushes, upsert_channels};
    use crate::messaging::{ChannelEntry, ChannelScheme};
    use crate::messaging::{
        MessagingDirectory, MessagingGlobalConfig, SubscriberEntry, SubscriberEntryKind, Urgency,
        WakeMin, WakeRouter,
    };
    use crate::pwa_push::config::AppPwaPushBlock;
    use crate::test_utils::ensure_user_and_conv;

    /// Insert a channel row into `messaging_channels` and return its UUID.
    fn make_channel(conn: &Connection, address: &str) -> Uuid {
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        upsert_channels(conn, &[entry]);
        uuid
    }

    fn insert(conn: &Connection, channel: Uuid, body: &str, sender: &str, ns: i64) {
        insert_message_with_pushes(
            conn,
            channel,
            "src",
            sender,
            body,
            Urgency::Low,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            None,
            ns,
            &[],
        );
    }

    #[test]
    fn query_returns_descending_order_with_limit() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "first", "alice", now_ns);
        insert(&conn, channel_uuid, "second", "bob", now_ns + 1_000_000);
        insert(&conn, channel_uuid, "third", "alice", now_ns + 2_000_000);

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 2,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].body, "third");
        assert_eq!(result[1].body, "second");
        // Channel address is resolved via SQL JOIN, not MessagingDirectory.
        assert_eq!(result[0].channel, canonical_address("test"));
    }

    #[test]
    fn query_filters_by_sender() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "from-alice", "alice", now_ns);
        insert(&conn, channel_uuid, "from-bob", "bob", now_ns + 1);

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100,
            before: None,
            after: None,
            sender: Some("alice".to_string()),
            search: None,
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body, "from-alice");
    }

    #[test]
    fn query_with_fts_search_finds_match() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "the quick brown fox", "u", now_ns);
        insert(&conn, channel_uuid, "an apple a day", "u", now_ns + 1);

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: Some("fox".to_string()),
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body, "the quick brown fox");
    }

    #[test]
    fn query_with_malformed_fts_returns_error() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        insert(&conn, channel_uuid, "ok", "u", utc_to_ns(Utc::now()));

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            // Unbalanced quote — invalid FTS5 syntax.
            search: Some("\"unbalanced".to_string()),
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded);
        assert!(matches!(result, Err(QueryError::Fts(_))));
    }

    #[test]
    fn query_filters_by_before_and_after() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let base = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "old", "u", base);
        insert(&conn, channel_uuid, "mid", "u", base + 5_000_000_000);
        insert(&conn, channel_uuid, "new", "u", base + 10_000_000_000);

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100,
            before: Some(ns_to_utc(base + 9_000_000_000)),
            after: Some(ns_to_utc(base + 1)),
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body, "mid");
    }

    #[test]
    fn query_returns_messages_without_reply_to() {
        // Non-reply messages (reply_to_uuid = NULL) must not be dropped by the LEFT JOIN.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "no-reply", "u", now_ns);

        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body, "no-reply");
        assert!(result[0].reply_to.is_none());
    }

    #[test]
    fn query_channel_address_resolved_via_join_not_directory() {
        // channel.address must come from the SQL JOIN, not from a MessagingDirectory.
        // Insert a channel that would not appear in any in-memory directory.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, "pwa_push:testuser");
        let now_ns = utc_to_ns(Utc::now());
        insert(&conn, channel_uuid, "push body", "u", now_ns);

        let q = MessageQuery {
            channel: "pwa_push:testuser".to_string(),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].channel, "pwa_push:testuser");
    }

    // -----------------------------------------------------------------------
    // retain_depth clamp tests (design §2.9)
    // -----------------------------------------------------------------------

    /// retain_depth clamp: Bounded(m) ⇒ at most m messages returned even when
    /// more are stored and the caller's limit is higher.
    #[test]
    fn retain_depth_bounded_clamps_result() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        // Insert 5 messages.
        for i in 0..5u32 {
            insert(
                &conn,
                channel_uuid,
                &format!("msg{i}"),
                "u",
                now_ns + i as i64 * 1_000_000,
            );
        }
        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 100, // caller wants 100
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        // clamp to 3 → at most 3 results.
        let result = run_query(&conn, channel_uuid, &q, Depth::Bounded(3)).unwrap();
        assert_eq!(result.len(), 3, "clamp to 3 should return 3 rows");
    }

    /// retain_depth clamp: Unbounded ⇒ no extra limit beyond the caller's limit.
    #[test]
    fn retain_depth_unbounded_no_extra_clamp() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        ensure_user_and_conv(&conn, 1);
        let channel_uuid = make_channel(&conn, &canonical_address("test"));
        let now_ns = utc_to_ns(Utc::now());
        for i in 0..5u32 {
            insert(
                &conn,
                channel_uuid,
                &format!("msg{i}"),
                "u",
                now_ns + i as i64 * 1_000_000,
            );
        }
        let q = MessageQuery {
            channel: canonical_address("test"),
            limit: 3, // caller wants 3
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: String::new(),
        };
        // Unbounded clamp: only the caller's limit applies.
        let result = run_query(&conn, channel_uuid, &q, Depth::Unbounded).unwrap();
        assert_eq!(result.len(), 3, "unbounded clamp respects caller limit");
    }

    // -----------------------------------------------------------------------
    // resolve_retain_depth_clamp tests via Messenger::query (design §2.9)
    //
    // These tests exercise the full precedence path through Messenger::query
    // (not just run_query with a pre-computed Depth), confirming that
    // resolve_retain_depth_clamp correctly selects subscriber vs. standing clamp.
    // -----------------------------------------------------------------------

    /// Non-subscriber caller is clamped to the channel's standing_retain_depth.
    /// With standing_retain_depth=2 and 5 messages stored, query returns at most 2.
    #[tokio::test]
    async fn non_subscriber_clamped_to_standing_retain_depth() {
        use crate::config::AppConfig;
        use crate::messaging::config::{MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink};
        use indexmap::IndexMap;

        let db = init_db_memory();
        let channel_uuid;
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            // Channel with standing_retain_depth=2.
            let uuid = Uuid::new_v4();
            let entry = ChannelEntry {
                uuid,
                address: canonical_address("news"),
                description: None,
                resolved_channel: ResolvedChannel {
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    standing_retain_depth: Depth::Bounded(2),
                    noise: NoiseLevel::Silent,
                    sink: Sink::Drop,
                    wake_min: WakeMin::Normal,
                },
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            };
            upsert_channels(&conn, &[entry]);
            channel_uuid = uuid;
            let ns = utc_to_ns(Utc::now());
            for i in 0..5u32 {
                insert(
                    &conn,
                    channel_uuid,
                    &format!("m{i}"),
                    "u",
                    ns + i as i64 * 1_000_000,
                );
            }
        }
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("news"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Bounded(2),
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let directory = MessagingDirectory::with_entries(vec![entry]);

        // caller-app has no subscription (so clamps to standing_retain_depth=2) but
        // holds a covering read ACL so it passes the gate.
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(
            "caller".to_string(),
            app_with_access("caller", &canonical_address("news")),
        );
        let messenger = Messenger::new(
            db,
            std::sync::Arc::new(directory),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );

        let q = MessageQuery {
            channel: canonical_address("news"),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "caller".to_string(),
        };
        let result = messenger.query(&q).await.unwrap();
        assert_eq!(
            result.len(),
            2,
            "non-subscriber caller must be clamped to standing_retain_depth=2"
        );
    }

    /// Non-subscriber caller with standing_retain_depth=Unbounded (legacy mapping)
    /// is unclamped — confirms §4.6 behavior preservation for non-subscriber callers.
    #[tokio::test]
    async fn non_subscriber_unbounded_standing_is_unclamped() {
        use crate::config::AppConfig;
        use crate::messaging::config::{MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink};
        use indexmap::IndexMap;

        let db = init_db_memory();
        let channel_uuid;
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            let uuid = Uuid::new_v4();
            let entry = ChannelEntry {
                uuid,
                address: canonical_address("legacy"),
                description: None,
                resolved_channel: ResolvedChannel {
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    standing_retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    sink: Sink::Drop,
                    wake_min: WakeMin::Normal,
                },
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            };
            upsert_channels(&conn, &[entry]);
            channel_uuid = uuid;
            let ns = utc_to_ns(Utc::now());
            for i in 0..5u32 {
                insert(
                    &conn,
                    channel_uuid,
                    &format!("m{i}"),
                    "u",
                    ns + i as i64 * 1_000_000,
                );
            }
        }
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("legacy"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let directory = MessagingDirectory::with_entries(vec![entry]);
        let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
        apps.insert(
            "caller".to_string(),
            app_with_access("caller", &canonical_address("legacy")),
        );
        let messenger = Messenger::new(
            db,
            std::sync::Arc::new(directory),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );

        let q = MessageQuery {
            channel: canonical_address("legacy"),
            limit: 3, // caller's limit is the only constraint
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "caller".to_string(),
        };
        let result = messenger.query(&q).await.unwrap();
        assert_eq!(
            result.len(),
            3,
            "unbounded standing_retain_depth must not clamp beyond caller limit (§4.6)"
        );
    }

    /// Subscriber caller is clamped to its own `retain_depth`, not `standing_retain_depth`.
    /// With subscriber retain_depth=Unbounded and standing=2, the subscriber sees all 5.
    /// With subscriber retain_depth=1 and standing=5, the subscriber sees at most 1.
    #[tokio::test]
    async fn subscriber_clamped_to_own_retain_depth_not_standing() {
        use crate::config::AppConfig;
        use crate::messaging::config::{
            MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
            ResolvedSubscription, Sink,
        };
        use indexmap::IndexMap;

        async fn run_case(subscriber_retain: Depth, standing: Depth, expected: usize) {
            let db = init_db_memory();
            let channel_uuid;
            {
                let conn = db.lock().await;
                ensure_user_and_conv(&conn, 1);
                let uuid = Uuid::new_v4();
                let entry = ChannelEntry {
                    uuid,
                    address: canonical_address("sub-clamp"),
                    description: None,
                    resolved_channel: ResolvedChannel {
                        push_depth: Depth::Bounded(0),
                        retain_depth: Depth::Unbounded,
                        standing_retain_depth: standing,
                        noise: NoiseLevel::Silent,
                        sink: Sink::Drop,
                        wake_min: WakeMin::Normal,
                    },
                    subscribers: vec![SubscriberEntry {
                        kind: SubscriberEntryKind::App("sub-app".to_string()),
                        push_depth: Depth::Bounded(0),
                        retain_depth: Depth::Unbounded,
                        noise: NoiseLevel::Silent,
                        wake_min: Some(WakeMin::Normal),
                    }],
                    transport_type: ChannelScheme::Brenn,
                    mount: None,
                };
                upsert_channels(&conn, &[entry]);
                channel_uuid = uuid;
                let ns = utc_to_ns(Utc::now());
                for i in 0..5u32 {
                    insert(
                        &conn,
                        channel_uuid,
                        &format!("m{i}"),
                        "u",
                        ns + i as i64 * 1_000_000,
                    );
                }
            }
            let entry = ChannelEntry {
                uuid: channel_uuid,
                address: canonical_address("sub-clamp"),
                description: None,
                resolved_channel: ResolvedChannel {
                    push_depth: Depth::Bounded(0),
                    retain_depth: Depth::Unbounded,
                    standing_retain_depth: standing,
                    noise: NoiseLevel::Silent,
                    sink: Sink::Drop,
                    wake_min: WakeMin::Normal,
                },
                subscribers: vec![SubscriberEntry {
                    kind: SubscriberEntryKind::App("sub-app".to_string()),
                    push_depth: Depth::Bounded(0),
                    retain_depth: subscriber_retain,
                    noise: NoiseLevel::Silent,
                    wake_min: Some(WakeMin::Normal),
                }],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            };
            let directory = MessagingDirectory::with_entries(vec![entry.clone()]);

            // The clamp reads the directory subscriber's retain_depth (above),
            // never this apps-map subscription. Its retain_depth deliberately
            // diverges so this test fails if the clamp ever consults the config
            // map instead of the directory.
            let mut sub_app = crate::messaging::test_support::test_app_config(
                "sub-app",
                Some(ResolvedMessagingConfig {
                    send_budget: 100,
                    subscriptions: vec![ResolvedSubscription {
                        channel_uuid,
                        channel_address: canonical_address("sub-clamp"),
                        push_depth: Depth::Bounded(0),
                        retain_depth: Depth::Unbounded,
                        noise: NoiseLevel::Silent,
                        wake_min: WakeMin::Normal,
                    }],
                }),
                vec![],
            );
            // Covering read ACL so the query passes the gate; the clamp under test
            // reads the directory subscriber, not this policy.
            sub_app.policy = covering_policy(&canonical_address("sub-clamp"));
            let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
            apps.insert("sub-app".to_string(), sub_app);

            let messenger = Messenger::new(
                db,
                std::sync::Arc::new(directory),
                std::sync::Arc::from("test-source"),
                std::sync::Arc::new(apps),
                std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
                MessagingGlobalConfig::default(),
            );

            let q = MessageQuery {
                channel: canonical_address("sub-clamp"),
                limit: 100,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: "sub-app".to_string(),
            };
            let result = messenger.query(&q).await.unwrap();
            assert_eq!(
                result.len(),
                expected,
                "subscriber retain_depth={subscriber_retain:?}, standing={standing:?}: expected {expected} results"
            );
        }

        // Subscriber retain_depth=Unbounded with standing=2 → subscriber sees all 5
        // (subscriber window is larger than standing; subscriber branch uses Unbounded clamp).
        run_case(Depth::Unbounded, Depth::Bounded(2), 5).await;

        // Subscriber retain_depth=1 with standing=5 → subscriber sees at most 1
        // (subscriber's own retain_depth is the binding constraint).
        run_case(Depth::Bounded(1), Depth::Bounded(5), 1).await;
    }

    // -----------------------------------------------------------------------
    // Directory-subscriber clamp tests: the clamp reads the channel directory
    // entry's subscriber list, covering dynamic subscribers on any transport.
    // -----------------------------------------------------------------------

    fn app_sub(slug: &str, retain: Depth) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::App(slug.to_string()),
            push_depth: Depth::Bounded(0),
            retain_depth: retain,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }
    }

    fn wasm_sub(slug: &str, retain: Depth) -> SubscriberEntry {
        SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Bounded(0),
            retain_depth: retain,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }
    }

    /// Build a single-channel messenger with `num_messages` stored rows on the
    /// channel. `apps` seeds the apps map; the clamp under test reads only the
    /// directory, so most callers pass an empty map.
    async fn clamp_messenger(
        address: &str,
        transport: ChannelScheme,
        standing: Depth,
        subscribers: Vec<SubscriberEntry>,
        num_messages: u32,
        apps: indexmap::IndexMap<String, crate::config::AppConfig>,
    ) -> std::sync::Arc<Messenger> {
        use crate::messaging::config::Sink;

        let db = init_db_memory();
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: standing,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers,
            transport_type: transport,
            mount: None,
        };
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            upsert_channels(&conn, std::slice::from_ref(&entry));
            let ns = utc_to_ns(Utc::now());
            for i in 0..num_messages {
                insert(
                    &conn,
                    uuid,
                    &format!("m{i}"),
                    "u",
                    ns + i as i64 * 1_000_000,
                );
            }
        }
        let directory = MessagingDirectory::with_entries(vec![entry]);
        Messenger::new(
            db,
            std::sync::Arc::new(directory),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// A policy that covers `address` for the read gate: the transport grant plus
    /// a broad covering matcher for the address's scheme. Lets a test register the
    /// calling app so `Messenger::query` passes the ACL gate and the test can
    /// isolate other behavior (the retain-depth clamp).
    fn covering_policy(address: &str) -> crate::access::AppPolicy {
        use crate::access::acl::{ChannelMatcher, MqttSubMatcher, WebhookMatcher};
        use crate::access::{AppCapability, AppPolicy};
        use crate::messaging::ChannelScheme;

        let mut p = AppPolicy::default();
        match ChannelScheme::split(address) {
            Some((ChannelScheme::Brenn, _)) => {
                p.grants.insert(AppCapability::MessagingSubscribe);
                p.acls.brenn_subscribe = vec![ChannelMatcher::Prefix(String::new())];
            }
            Some((ChannelScheme::Ephemeral, _)) => {
                p.grants.insert(AppCapability::EphemeralSubscribe);
                p.acls.ephemeral_subscribe = vec![ChannelMatcher::Prefix(String::new())];
            }
            Some((ChannelScheme::Mqtt, rest)) => {
                // rest == "<client>:<topic>"; cover the client with a `#` filter.
                let client = rest.split(':').next().unwrap_or_default().to_string();
                p.grants.insert(AppCapability::MqttSubscribe);
                p.acls.mqtt_subscribe = vec![MqttSubMatcher {
                    client,
                    topic_filter: "#".to_string(),
                }];
            }
            Some((ChannelScheme::Webhook, endpoint)) => {
                p.grants.insert(AppCapability::Webhook);
                p.acls.webhook = vec![WebhookMatcher {
                    endpoint: endpoint.to_string(),
                }];
            }
            // pwa_push: is egress-only and local: never reaches the server, so
            // neither has a server-side read gate to cover.
            Some((ChannelScheme::PwaPush | ChannelScheme::Local, _)) | None => {}
        }
        p
    }

    /// A minimal `AppConfig` registered with [`covering_policy`] for `address`, so
    /// the calling app passes the read gate.
    fn app_with_access(slug: &str, address: &str) -> crate::config::AppConfig {
        let mut cfg = crate::messaging::test_support::test_app_config(slug, None, vec![]);
        cfg.policy = covering_policy(address);
        cfg
    }

    /// Query `address` as `calling_app_slug` and return the row count.
    async fn query_count(messenger: &Messenger, address: &str, calling_app_slug: &str) -> usize {
        let q = MessageQuery {
            channel: address.to_string(),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: calling_app_slug.to_string(),
        };
        messenger.query(&q).await.unwrap().len()
    }

    /// Build a single-channel messenger (empty apps map — the clamp reads only
    /// the directory) with `num_messages` stored rows, run a query as
    /// `calling_app_slug`, and return the row count.
    async fn clamp_result_count(
        address: &str,
        transport: ChannelScheme,
        standing: Depth,
        subscribers: Vec<SubscriberEntry>,
        num_messages: u32,
        calling_app_slug: &str,
    ) -> usize {
        // The read gate requires the calling app to have channel access; register
        // it with a covering policy so this test isolates the clamp, not the gate.
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            calling_app_slug.to_string(),
            app_with_access(calling_app_slug, address),
        );
        let messenger = clamp_messenger(
            address,
            transport,
            standing,
            subscribers,
            num_messages,
            apps,
        )
        .await;
        query_count(&messenger, address, calling_app_slug).await
    }

    /// The bug: a subscriber present only in the directory (never in the apps
    /// map — e.g. a dynamic subscription) with a bounded `retain_depth` is
    /// clamped to that depth, not the channel's unbounded standing depth.
    #[tokio::test]
    async fn directory_subscriber_clamped_to_own_bounded_depth() {
        let n = clamp_result_count(
            &canonical_address("dyn"),
            ChannelScheme::Brenn,
            Depth::Unbounded,
            vec![app_sub("dyn-app", Depth::Bounded(2))],
            5,
            "dyn-app",
        )
        .await;
        assert_eq!(
            n, 2,
            "directory subscriber clamped to its own retain_depth=2"
        );
    }

    /// The clamp is transport-agnostic: same shape on an `mqtt:` channel.
    #[tokio::test]
    async fn directory_subscriber_clamp_mqtt_transport() {
        let n = clamp_result_count(
            "mqtt:home:sensors/temp",
            ChannelScheme::Mqtt,
            Depth::Unbounded,
            vec![app_sub("dyn-app", Depth::Bounded(2))],
            5,
            "dyn-app",
        )
        .await;
        assert_eq!(n, 2, "mqtt directory subscriber clamped to retain_depth=2");
    }

    /// Allow-path read through the gate on a `webhook:` channel: a caller with the
    /// `Webhook` grant + a covering matcher passes the gate and gets rows back.
    #[tokio::test]
    async fn directory_subscriber_clamp_webhook_transport() {
        let n = clamp_result_count(
            "webhook:inbound-hook",
            ChannelScheme::Webhook,
            Depth::Unbounded,
            vec![app_sub("dyn-app", Depth::Bounded(2))],
            5,
            "dyn-app",
        )
        .await;
        assert_eq!(
            n, 2,
            "webhook directory subscriber clamped to retain_depth=2"
        );
    }

    /// A subscriber's own depth wins even when larger than the standing depth:
    /// unbounded subscriber retain_depth over a bounded standing depth reads all.
    #[tokio::test]
    async fn directory_subscriber_depth_larger_than_standing_wins() {
        let n = clamp_result_count(
            &canonical_address("wide"),
            ChannelScheme::Brenn,
            Depth::Bounded(2),
            vec![app_sub("dyn-app", Depth::Unbounded)],
            5,
            "dyn-app",
        )
        .await;
        assert_eq!(
            n, 5,
            "unbounded subscriber depth wins over bounded standing"
        );
    }

    /// A `Wasm` subscriber whose slug coincidentally matches the calling app
    /// must not leak its depth: the app falls through to the standing depth.
    #[tokio::test]
    async fn directory_wasm_subscriber_kind_does_not_match_app_caller() {
        let n = clamp_result_count(
            &canonical_address("collide"),
            ChannelScheme::Brenn,
            Depth::Bounded(2),
            vec![wasm_sub("caller", Depth::Unbounded)],
            5,
            "caller",
        )
        .await;
        assert_eq!(
            n, 2,
            "app caller must not match a Wasm subscriber; clamped to standing=2"
        );
    }

    /// End-to-end: `subscribe_dynamic` folds the subscriber (with its resolved
    /// `retain_depth`) into the directory, and a subsequent query honors it.
    #[tokio::test]
    async fn dynamic_subscribe_then_query_honors_retain_depth() {
        use crate::messaging::subscribe::DynamicSubscribeParams;
        use crate::messaging::test_support::test_app_config;
        use indexmap::IndexMap;

        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        let mut graf = test_app_config("graf", None, vec!["u".to_string()]);
        graf.policy = covering_policy(&canonical_address("e2e"));
        apps.insert("graf".to_string(), graf);
        let messenger = clamp_messenger(
            &canonical_address("e2e"),
            ChannelScheme::Brenn,
            Depth::Unbounded,
            vec![],
            5,
            apps,
        )
        .await;

        messenger
            .subscribe_dynamic(
                "graf",
                &canonical_address("e2e"),
                DynamicSubscribeParams {
                    push_depth: Depth::Bounded(0),
                    retain_depth: Depth::Bounded(2),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .expect("dynamic subscribe succeeds");

        let n = query_count(&messenger, &canonical_address("e2e"), "graf").await;
        assert_eq!(
            n, 2,
            "dynamic subscriber clamped to its resolved retain_depth=2"
        );
    }

    /// End-to-end read cap: on a channel with a bounded standing depth, a dynamic
    /// subscribe at exactly standing is accepted, and a subsequent
    /// `MessageChannelGet` reads at most standing rows even though more were
    /// published. Ties the dynamic-path cap to the clamp it protects: the deepest
    /// window an app can grant itself is standing, and the clamp then honors it.
    #[tokio::test]
    async fn dynamic_subscribe_at_standing_caps_history_read() {
        use crate::messaging::subscribe::DynamicSubscribeParams;
        use crate::messaging::test_support::test_app_config;
        use indexmap::IndexMap;

        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        let mut graf = test_app_config("graf", None, vec!["u".to_string()]);
        graf.policy = covering_policy(&canonical_address("capped"));
        apps.insert("graf".to_string(), graf);
        // Bounded standing depth of 2; five messages published.
        let messenger = clamp_messenger(
            &canonical_address("capped"),
            ChannelScheme::Brenn,
            Depth::Bounded(2),
            vec![],
            5,
            apps,
        )
        .await;

        // Subscribe at exactly standing (2) — accepted by the cap.
        messenger
            .subscribe_dynamic(
                "graf",
                &canonical_address("capped"),
                DynamicSubscribeParams {
                    push_depth: Depth::Bounded(0),
                    retain_depth: Depth::Bounded(2),
                    noise: None,
                    wake_min: None,
                    qos: None,
                },
            )
            .await
            .expect("subscribe at standing is accepted");

        let n = query_count(&messenger, &canonical_address("capped"), "graf").await;
        assert_eq!(
            n, 2,
            "history read is capped at the standing depth the sub was allowed"
        );
    }

    // -----------------------------------------------------------------------
    // Messenger::query pwa_push dispatch tests (require async + full Messenger)
    // -----------------------------------------------------------------------

    fn make_pwa_push_app(
        slug: &str,
        pwa_push_enabled: bool,
        allowed_users: Vec<String>,
    ) -> crate::config::AppConfig {
        crate::config::AppConfig {
            slug: slug.to_string(),
            name: slug.to_string(),
            description: String::new(),
            icon: String::new(),
            working_dir: std::path::PathBuf::from("/tmp"),
            model: String::new(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None,
            idle_hook_secs: 0,
            allowed_users,
            disabled_tools: vec![],
            mcp_servers: Default::default(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: crate::config::PathMapper::Identity,
            container_spawn: None,
            start_hooks: Default::default(),
            post_pull_hooks: Default::default(),
            startup_hooks: Default::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: Default::default(),
            mounts: vec![],
            history_replay_limit: 100,
            frontmatter: Default::default(),
            state_dir: std::path::PathBuf::from("/tmp"),
            messaging: None,
            messaging_default_send_budget: 100,
            // Grant PwaPush exactly when this fixture wants push enabled, so
            // pwa_push_enabled() reflects the intended state.
            policy: {
                let mut p = crate::access::AppPolicy::default();
                if pwa_push_enabled {
                    p.grants.insert(crate::access::AppCapability::PwaPush);
                }
                p
            },
            pwa_push: if pwa_push_enabled {
                Some(AppPwaPushBlock {
                    default_title: None,
                })
            } else {
                None
            },
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        }
    }

    fn make_messenger_with_apps(
        db: crate::db::Db,
        apps: indexmap::IndexMap<String, crate::config::AppConfig>,
    ) -> std::sync::Arc<Messenger> {
        Messenger::new(
            db,
            std::sync::Arc::new(MessagingDirectory::new()),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// Build a `MessageQuery` for `channel` as `slug` with default options.
    fn q_for(channel: &str, slug: &str) -> MessageQuery {
        MessageQuery {
            channel: channel.to_string(),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: slug.to_string(),
        }
    }

    #[tokio::test]
    async fn query_pwa_push_denied_regardless_of_grant() {
        // pwa_push is egress-only: the read gate denies it even for an app holding
        // the PwaPush grant, and even when a channel row exists (no existence leak).
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            let uuid = make_channel(&conn, "pwa_push:alice");
            insert(&conn, uuid, "push-msg", "system", utc_to_ns(Utc::now()));
        }
        let mut apps = indexmap::IndexMap::new();
        apps.insert("graf".to_string(), make_pwa_push_app("graf", true, vec![]));
        let messenger = make_messenger_with_apps(db, apps);

        for addr in ["pwa_push:alice", "pwa_push:alice@laptop", "pwa_push:absent"] {
            assert!(
                matches!(
                    messenger.query(&q_for(addr, "graf")).await,
                    Err(QueryError::UnknownChannel(_))
                ),
                "pwa_push read must be denied: {addr}"
            );
        }
    }

    #[tokio::test]
    #[should_panic(expected = "not a registered app")]
    async fn query_unregistered_calling_app_panics() {
        // calling_app_slug is host-plumbed from the bridge, never tool input; an
        // unregistered slug is a wiring bug and must panic, not silently deny.
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
        }
        let messenger = make_messenger_with_apps(db, indexmap::IndexMap::new());
        let _ = messenger
            .query(&q_for(&canonical_address("news"), "ghost"))
            .await;
    }

    #[tokio::test]
    async fn query_brenn_grant_without_matcher_denied() {
        // MessagingSubscribe grant but no covering brenn_subscribe matcher → the
        // read gate denies even though the channel exists with rows.
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = crate::messaging::test_support::test_app_config("app", None, vec![]);
        let mut p = crate::access::AppPolicy::default();
        p.grants
            .insert(crate::access::AppCapability::MessagingSubscribe);
        cfg.policy = p;
        apps.insert("app".to_string(), cfg);
        let messenger = clamp_messenger(
            &canonical_address("gated"),
            ChannelScheme::Brenn,
            Depth::Unbounded,
            vec![],
            3,
            apps,
        )
        .await;
        assert!(matches!(
            messenger
                .query(&q_for(&canonical_address("gated"), "app"))
                .await,
            Err(QueryError::UnknownChannel(_))
        ));
    }

    #[tokio::test]
    async fn query_brenn_matcher_without_grant_denied() {
        // Covering brenn_subscribe matcher but no MessagingSubscribe grant → the
        // read gate denies (both factors are required).
        use crate::access::acl::ChannelMatcher;
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = crate::messaging::test_support::test_app_config("app", None, vec![]);
        let mut p = crate::access::AppPolicy::default();
        p.acls.brenn_subscribe = vec![ChannelMatcher::Prefix(String::new())];
        cfg.policy = p;
        apps.insert("app".to_string(), cfg);
        let messenger = clamp_messenger(
            &canonical_address("gated"),
            ChannelScheme::Brenn,
            Depth::Unbounded,
            vec![],
            3,
            apps,
        )
        .await;
        assert!(matches!(
            messenger
                .query(&q_for(&canonical_address("gated"), "app"))
                .await,
            Err(QueryError::UnknownChannel(_))
        ));
    }

    #[tokio::test]
    async fn query_webhook_grant_without_matcher_denied() {
        // Webhook grant but no covering webhook matcher → the gate denies even
        // though the channel exists with rows (two-factor at the query gate).
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = crate::messaging::test_support::test_app_config("app", None, vec![]);
        let mut p = crate::access::AppPolicy::default();
        p.grants.insert(crate::access::AppCapability::Webhook);
        cfg.policy = p;
        apps.insert("app".to_string(), cfg);
        let messenger = clamp_messenger(
            "webhook:gated-hook",
            ChannelScheme::Webhook,
            Depth::Unbounded,
            vec![],
            3,
            apps,
        )
        .await;
        assert!(matches!(
            messenger.query(&q_for("webhook:gated-hook", "app")).await,
            Err(QueryError::UnknownChannel(_))
        ));
    }

    #[tokio::test]
    async fn query_mqtt_grant_without_matcher_denied() {
        // MqttSubscribe grant but no covering mqtt_subscribe matcher → the gate
        // denies even though the channel exists with rows (two-factor at the gate).
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = crate::messaging::test_support::test_app_config("app", None, vec![]);
        let mut p = crate::access::AppPolicy::default();
        p.grants.insert(crate::access::AppCapability::MqttSubscribe);
        cfg.policy = p;
        apps.insert("app".to_string(), cfg);
        let messenger = clamp_messenger(
            "mqtt:home:sensors/temp",
            ChannelScheme::Mqtt,
            Depth::Unbounded,
            vec![],
            3,
            apps,
        )
        .await;
        assert!(matches!(
            messenger
                .query(&q_for("mqtt:home:sensors/temp", "app"))
                .await,
            Err(QueryError::UnknownChannel(_))
        ));
    }

    #[tokio::test]
    async fn query_denied_existing_and_absent_channel_errors_are_indistinguishable() {
        // No existence leak: a denied read of an existing channel and a read of a
        // truly-absent channel both surface UnknownChannel with the queried address.
        // Denied-but-existing: channel exists, app has no covering policy.
        let mut denied_apps = indexmap::IndexMap::new();
        denied_apps.insert(
            "app".to_string(),
            crate::messaging::test_support::test_app_config("app", None, vec![]),
        );
        let denied = clamp_messenger(
            &canonical_address("secret"),
            ChannelScheme::Brenn,
            Depth::Unbounded,
            vec![],
            3,
            denied_apps,
        )
        .await;
        let denied_err = denied
            .query(&q_for(&canonical_address("secret"), "app"))
            .await
            .expect_err("denied read is an error");

        // Absent: app has covering access but the channel does not exist.
        let mut ok_apps = indexmap::IndexMap::new();
        ok_apps.insert(
            "app".to_string(),
            app_with_access("app", &canonical_address("secret")),
        );
        let absent = make_messenger_with_apps(init_db_memory(), ok_apps);
        let absent_err = absent
            .query(&q_for(&canonical_address("secret"), "app"))
            .await
            .expect_err("absent read is an error");

        assert_eq!(denied_err.to_string(), absent_err.to_string());
    }

    #[tokio::test]
    async fn query_ephemeral_always_unknown_channel() {
        // ephemeral channels are never durable, so a read is UnknownChannel whether
        // or not the app is authorized — denied (no policy) and passed-gate (covering
        // policy, then absent from the durable directory) both yield UnknownChannel.
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
        }
        let mut apps = indexmap::IndexMap::new();
        // "denier" holds no policy; "reader" holds covering ephemeral access.
        apps.insert(
            "denier".to_string(),
            crate::messaging::test_support::test_app_config("denier", None, vec![]),
        );
        apps.insert(
            "reader".to_string(),
            app_with_access("reader", "ephemeral:live"),
        );
        let messenger = make_messenger_with_apps(db, apps);

        for slug in ["denier", "reader"] {
            assert!(matches!(
                messenger.query(&q_for("ephemeral:live", slug)).await,
                Err(QueryError::UnknownChannel(_))
            ));
        }
    }

    #[tokio::test]
    async fn query_brenn_address_does_not_match_pwa_push_messages() {
        // brenn: query does not accidentally surface pwa_push: messages.
        let db = init_db_memory();
        let brenn_uuid;
        {
            let conn = db.lock().await;
            ensure_user_and_conv(&conn, 1);
            let b_uuid = make_channel(&conn, &canonical_address("news"));
            let pwa_uuid = make_channel(&conn, "pwa_push:alice");
            let ns = utc_to_ns(Utc::now());
            insert(&conn, b_uuid, "brenn-msg", "system", ns);
            insert(&conn, pwa_uuid, "push-msg", "system", ns + 1);
            brenn_uuid = b_uuid;
        }
        // brenn: channels must be in the directory to resolve.
        let entry = ChannelEntry {
            uuid: brenn_uuid,
            address: canonical_address("news"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        let directory = MessagingDirectory::with_entries(vec![entry]);
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            "graf".to_string(),
            app_with_access("graf", &canonical_address("news")),
        );
        let messenger = Messenger::new(
            db,
            std::sync::Arc::new(directory),
            std::sync::Arc::from("test-source"),
            std::sync::Arc::new(apps),
            std::sync::Arc::new(super::NoopWakeRouter) as std::sync::Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );

        let q = MessageQuery {
            channel: canonical_address("news"),
            limit: 100,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "graf".to_string(),
        };
        let result = messenger.query(&q).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body, "brenn-msg");
    }

    // Byte-identity tests: pin each constant (and the assembled window-snapshot SQL) against
    // the pre-refactor literal text. These fail loudly if a constant is edited without updating
    // the golden, guarding against whitespace/boundary drift and silent future column-list
    // divergence.

    #[test]
    fn select_envelope_base_matches_prerefactor_literal() {
        // Golden: the base SELECT fragment from the pre-refactor run_query literal, verbatim.
        let golden = "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency, \
                      m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns, \
                      c.address, rc.address, m.envelope_type \
               FROM messaging_messages m \
               JOIN messaging_channels c ON c.uuid = m.channel_uuid \
               LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid ";
        assert_eq!(SELECT_ENVELOPE_BASE, golden);
    }

    #[test]
    fn select_envelope_order_limit_tail_matches_prerefactor_literal() {
        // Golden: the ORDER BY + LIMIT tail from the pre-refactor run_query literal, verbatim.
        let golden = "ORDER BY m.publish_ts_ns DESC, m.id DESC LIMIT ?";
        assert_eq!(SELECT_ENVELOPE_ORDER_LIMIT_TAIL, golden);
    }

    #[test]
    fn assembled_window_snapshot_sql_matches_prerefactor_literal() {
        // Golden: the complete SQL string from the pre-refactor load_window_snapshot prepare call.
        let golden = "SELECT m.uuid, m.channel_uuid, m.source, m.sender, m.body, m.urgency, \
                      m.reply_to_uuid, m.delivery_deadline, m.deliver_after, m.publish_ts_ns, \
                      c.address, rc.address, m.envelope_type \
               FROM messaging_messages m \
               JOIN messaging_channels c ON c.uuid = m.channel_uuid \
               LEFT JOIN messaging_channels rc ON rc.uuid = m.reply_to_uuid \
               WHERE m.channel_uuid = ? \
               ORDER BY m.publish_ts_ns DESC, m.id DESC \
               LIMIT ?";
        let assembled = format!(
            "{base}WHERE m.channel_uuid = ? {tail}",
            base = SELECT_ENVELOPE_BASE,
            tail = SELECT_ENVELOPE_ORDER_LIMIT_TAIL,
        );
        assert_eq!(assembled, golden);
    }
}
