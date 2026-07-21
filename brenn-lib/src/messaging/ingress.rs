//! Ingress event types and helpers for the unified delivery substrate.
//!
//! This module owns:
//!
//! - [`Event`] — the persistent shape of an ingress message in the unified
//!   store. Constructed at drain time from `kind='ingress'` rows.
//! - [`IngressOrBus`] — the tagged payload returned by the drain read path;
//!   the bus arm holds a [`MessageEnvelope`], the ingress arm holds an [`Event`].
//! - Repo-sync constants, collapser helpers, and formatting logic (previously
//!   in `brenn_lib::event_queue`).

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};

use super::MessageEnvelope;

// ---------------------------------------------------------------------------
// Event — ingress payload shape
// ---------------------------------------------------------------------------

/// A queued ingress event reconstructed from the unified pending store at
/// drain time. Carries the same fields as the old `event_queue::Event`:
/// `source`, `summary`, `payload` (the event body), and `created_at` derived
/// from `publish_ts_ns`.
///
/// `id` is the `messaging_pending_pushes.id` for the push row (used to mark
/// the row delivered after a successful drain).
///
/// `conversation_id` is preserved as `0` for synthesized events (e.g. the
/// `repo_sync:summary` sentinel built by [`collapse_repo_sync`]); callers
/// must not mark the synthesized event delivered (it has no DB row).
#[derive(Debug, Clone)]
pub struct Event {
    pub id: i64,
    pub conversation_id: i64,
    pub source: String,
    pub summary: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// IngressOrBus — tagged drain payload
// ---------------------------------------------------------------------------

/// Tagged payload returned by `load_pending_pushes_for_drain`.
///
/// `Bus` rows carry a [`MessageEnvelope`] built from `kind='brenn'` rows;
/// `Ingress` rows carry an [`Event`] reconstructed from `kind='ingress'` rows.
///
/// **Invariant:** no code path constructs a [`MessageEnvelope`] from a
/// `kind='ingress'` row. The bus arm of the row decoder only runs when
/// `kind = 'brenn'`; the ingress arm never reads `c.address`.
// TODO(ingress-retirement): once repo_sync publishes onto a real bus channel
// and the ingress rows are migrated, this enum collapses to a bare
// `MessageEnvelope` and the `Ingress`/`Event` paths are deleted.
#[derive(Debug, Clone)]
pub enum IngressOrBus {
    Ingress(Event),
    Bus(MessageEnvelope),
}

impl IngressOrBus {
    /// True iff this is the `Bus` arm.
    pub fn is_bus(&self) -> bool {
        matches!(self, Self::Bus(_))
    }

    /// Unwrap as a bus envelope by value. Panics if this is an `Ingress` row —
    /// callers that are bus-only by construction must use this to enforce
    /// the invariant (fail-fast).
    pub fn unwrap_bus(self) -> MessageEnvelope {
        match self {
            Self::Bus(e) => e,
            Self::Ingress(ev) => panic!(
                "messaging: expected bus row but got ingress row with source {:?} push_id {}",
                ev.source, ev.id
            ),
        }
    }

    /// Borrow the inner [`MessageEnvelope`] for bus-only paths.
    /// Panics if this is an `Ingress` row (fail-fast; bus-only invariant).
    pub fn unwrap_bus_ref(&self) -> &MessageEnvelope {
        match self {
            Self::Bus(e) => e,
            Self::Ingress(ev) => panic!(
                "messaging: expected bus row but got ingress row with source {:?} push_id {}",
                ev.source, ev.id
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Repo-sync constants
// ---------------------------------------------------------------------------

/// Source-column prefix used by all repo-sync event sources.
pub const REPO_SYNC_SOURCE_PREFIX: &str = "repo_sync:";

/// Event source for per-cycle fast-forward advance notifications.
pub const REPO_SYNC_SOURCE_PULLED: &str = "repo_sync:pulled";

/// Event source for per-cycle local-HEAD-advance notifications. Used when
/// advance-detection observes that HEAD moved during a cycle but the
/// movement was NOT produced by this cycle's fetch-merge (so the commits
/// were authored locally, not fetched from the remote).
pub const REPO_SYNC_SOURCE_LOCAL: &str = "repo_sync:local";

/// Event source for per-cycle conflict notifications.
pub const REPO_SYNC_SOURCE_CONFLICT: &str = "repo_sync:conflict";

/// Synthesized source injected by the drain-time collapser.
/// Never persisted — built in memory just before injection.
pub const REPO_SYNC_SOURCE_SUMMARY: &str = "repo_sync:summary";

/// Payload `kind` field for a per-cycle pulled event.
pub const REPO_SYNC_KIND_PULLED: &str = "pulled";

/// Payload `kind` field for a per-cycle local-advance event.
pub const REPO_SYNC_KIND_LOCAL: &str = "local";

/// Payload `kind` field for a per-cycle conflict event.
pub const REPO_SYNC_KIND_CONFLICT: &str = "conflict";

/// Payload `kind` field for the synthesized summary (multi-slug aggregate).
pub const REPO_SYNC_KIND_SUMMARY: &str = "git_update";

/// Sentinel `Event::id` for the in-memory-only synthesized summary event.
/// Persisted events always have `id > 0`, so `id == 0` uniquely identifies
/// a collapser-synthesized entry for downstream "mark delivered" filtering.
pub const SYNTHETIC_EVENT_ID: i64 = 0;

/// True when `source` is one of the repo-sync event sources.
pub fn is_repo_sync_source(source: &str) -> bool {
    source.starts_with(REPO_SYNC_SOURCE_PREFIX)
}

// ---------------------------------------------------------------------------
// Staleness config (process global)
// ---------------------------------------------------------------------------

/// Default drain-time staleness cap in days. Used when the server hasn't
/// called `set_repo_sync_staleness_days` (tests, unit contexts).
///
/// Must match the default on `config::RepoSyncConfig::stale_conversation_days`.
const DEFAULT_REPO_SYNC_STALENESS_DAYS: u64 = 7;

/// Process-global drain-time staleness cap. Populated at startup from
/// `[repo_sync].stale_conversation_days`. Read by drain code in
/// `active_bridge.rs` on every wake.
static REPO_SYNC_STALENESS_DAYS: AtomicU64 = AtomicU64::new(DEFAULT_REPO_SYNC_STALENESS_DAYS);

/// Maximum accepted value for `staleness_days`. Values beyond this would
/// overflow the `i64` arithmetic in staleness helpers.
pub const MAX_REPO_SYNC_STALENESS_DAYS: u64 = 365 * 1_000;

/// Set the drain-time staleness cap. Call once at server startup.
///
/// Panics (in all build modes) if `days` exceeds [`MAX_REPO_SYNC_STALENESS_DAYS`].
pub fn set_repo_sync_staleness_days(days: u64) {
    assert!(
        days <= MAX_REPO_SYNC_STALENESS_DAYS,
        "staleness_days={days} exceeds MAX_REPO_SYNC_STALENESS_DAYS={MAX_REPO_SYNC_STALENESS_DAYS}; \
         this would cause integer overflow in release builds"
    );
    REPO_SYNC_STALENESS_DAYS.store(days, Ordering::Relaxed);
}

/// Read the current drain-time staleness cap in days.
pub fn repo_sync_staleness_days() -> u64 {
    REPO_SYNC_STALENESS_DAYS.load(Ordering::Relaxed)
}

/// Maximum accepted value for `delivered_retention_days` in the event cleanup
/// loop. Values beyond this would overflow the `u64 → i64` cast.
pub const MAX_DELIVERED_RETENTION_DAYS: u64 = 365 * 1_000;

/// Validate `delivered_retention_days` before use.
///
/// Panics if `days` exceeds [`MAX_DELIVERED_RETENTION_DAYS`]. Call once at
/// startup so misconfigured values fail immediately.
pub fn assert_delivered_retention_days_valid(days: u64) {
    assert!(
        days <= MAX_DELIVERED_RETENTION_DAYS,
        "delivered_retention_days={days} exceeds MAX_DELIVERED_RETENTION_DAYS={MAX_DELIVERED_RETENTION_DAYS}; \
         this would cause integer overflow in the cleanup loop"
    );
}

// ---------------------------------------------------------------------------
// Maximum oneline cap
// ---------------------------------------------------------------------------

/// Maximum number of commit oneline entries reported in a
/// `repo_sync:pulled` event and preserved through collapse/merge.
pub const ONELINE_CAP: usize = 10;

/// Apply the [`ONELINE_CAP`] truncation rule in place. When `lines.len()`
/// exceeds the cap, keep the first `ONELINE_CAP - 1` entries and replace
/// the tail with a single `"... N more (older)"` sentinel.
pub fn cap_oneline(lines: &mut Vec<String>) {
    if lines.len() > ONELINE_CAP {
        let extra = lines.len() - (ONELINE_CAP - 1);
        lines.truncate(ONELINE_CAP - 1);
        lines.push(format!("... {extra} more (older)"));
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Format a batch of events into a single message for CC.
///
/// Returns `None` if the list is empty.
pub fn format_event_batch(events: &[Event]) -> Option<String> {
    if events.is_empty() {
        return None;
    }

    let mut lines = Vec::with_capacity(events.len() + 4);
    lines.push("[Events while you were away]".to_string());
    lines.push(String::new());

    for event in events {
        let time = event.created_at.format("%H:%M UTC");
        lines.push(format!("• {} ({}, {})", event.summary, event.source, time));
    }

    // Append full structured payloads for LLM tool use.
    lines.push(String::new());
    lines.push("Full event data (JSON):".to_string());
    let json_payloads: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "source": e.source,
                "summary": e.summary,
                "payload": serde_json::from_str::<serde_json::Value>(&e.payload)
                    .unwrap_or_else(|_| serde_json::Value::String(e.payload.clone())),
                "created_at": e.created_at.to_rfc3339(),
            })
        })
        .collect();
    lines.push(serde_json::to_string(&json_payloads).expect("serialize event batch"));

    Some(lines.join("\n"))
}

// ---------------------------------------------------------------------------
// Staleness filter
// ---------------------------------------------------------------------------

/// Seconds per day — SQL-path staleness arithmetic.
#[allow(dead_code)]
pub(crate) const SECS_PER_DAY: i64 = 86_400;

/// Partition `events` into (kept, stale). A `repo_sync:*` event is stale
/// when `conversation_updated_at` is older than `staleness_days` relative
/// to `now`. Non-repo_sync events are always kept regardless of staleness.
pub fn split_stale_repo_sync(
    events: Vec<Event>,
    conversation_updated_at: DateTime<Utc>,
    now: DateTime<Utc>,
    staleness_days: u64,
) -> (Vec<Event>, Vec<Event>) {
    let stale_cutoff = now - chrono::Duration::days(staleness_days as i64);
    if conversation_updated_at >= stale_cutoff {
        return (events, Vec::new());
    }
    let mut kept = Vec::with_capacity(events.len());
    let mut stale = Vec::new();
    for e in events {
        if is_repo_sync_source(&e.source) {
            stale.push(e);
        } else {
            kept.push(e);
        }
    }
    (kept, stale)
}

// ---------------------------------------------------------------------------
// Repo-sync collapser
// ---------------------------------------------------------------------------

/// A single collapsed `pulled` entry for the synthesized summary.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct PulledEntry {
    pub slug: String,
    pub oneline: Vec<String>,
}

/// A single collapsed `local` entry for the synthesized summary.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct LocalEntry {
    pub slug: String,
    pub oneline: Vec<String>,
}

/// A single collapsed `conflict` entry for the synthesized summary.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ConflictEntry {
    pub slug: String,
    pub reason: String,
    pub detail: String,
}

/// Typed payload for A7: repo-sync compaction summary event.
///
/// Fields in declaration order: conflicts, kind, local, pulled — alphabetical
/// so `serde_json` struct serialization matches what `json!` BTreeMap ordering
/// would produce for the same keys.
#[derive(serde::Serialize)]
pub(crate) struct RepoSyncSummaryPayload<'a> {
    // alphabetical: conflicts, kind, local, pulled
    pub(crate) conflicts: &'a [ConflictEntry],
    pub(crate) kind: &'a str,
    pub(crate) local: &'a [LocalEntry],
    pub(crate) pulled: &'a [PulledEntry],
}

/// Result of running the repo-sync collapser over a mixed event list.
#[derive(Debug)]
pub struct CollapsedDrain {
    pub events: Vec<Event>,
    pub original_repo_sync_ids: Vec<i64>,
}

/// Collapse repo-sync events in `events` into a single synthesized
/// `repo_sync:summary` entry. See `event_queue::collapse_repo_sync` for full
/// semantics; this is the canonical implementation moved here.
pub fn collapse_repo_sync(events: Vec<Event>) -> CollapsedDrain {
    let mut other: Vec<Event> = Vec::with_capacity(events.len());
    let mut repo_sync: Vec<Event> = Vec::new();
    for e in events {
        if is_repo_sync_source(&e.source) {
            repo_sync.push(e);
        } else {
            other.push(e);
        }
    }

    if repo_sync.is_empty() {
        return CollapsedDrain {
            events: other,
            original_repo_sync_ids: Vec::new(),
        };
    }

    enum SlugState {
        Pulled { oneline_newest_first: Vec<String> },
        Local { oneline_newest_first: Vec<String> },
        Conflict { reason: String, detail: String },
    }

    let mut slug_order: Vec<String> = Vec::new();
    let mut per_slug: std::collections::HashMap<String, SlugState> =
        std::collections::HashMap::new();
    let original_ids: Vec<i64> = repo_sync.iter().map(|e| e.id).collect();
    let mut malformed: Vec<Event> = Vec::new();

    for e in repo_sync {
        let payload: serde_json::Value = match serde_json::from_str(&e.payload) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    event_id = e.id,
                    source = %e.source,
                    error = %err,
                    "collapse_repo_sync: malformed JSON payload — passing through uncollapsed"
                );
                malformed.push(e);
                continue;
            }
        };
        let kind = payload.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let slug = payload
            .get("slug")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if slug.is_empty() {
            tracing::warn!(
                event_id = e.id,
                source = %e.source,
                "collapse_repo_sync: payload missing slug — passing through uncollapsed"
            );
            malformed.push(e);
            continue;
        }

        let existing = per_slug.remove(&slug);
        let first_seen = existing.is_none();

        match (kind, existing) {
            (REPO_SYNC_KIND_PULLED, existing) => {
                let mut oneline = extract_oneline(&payload);
                if let Some(SlugState::Pulled {
                    oneline_newest_first: prev,
                }) = existing
                {
                    oneline.extend(prev);
                }
                cap_oneline(&mut oneline);
                if first_seen {
                    slug_order.push(slug.clone());
                }
                per_slug.insert(
                    slug,
                    SlugState::Pulled {
                        oneline_newest_first: oneline,
                    },
                );
            }
            (REPO_SYNC_KIND_LOCAL, existing) => {
                let mut oneline = extract_oneline(&payload);
                if let Some(SlugState::Local {
                    oneline_newest_first: prev,
                }) = existing
                {
                    oneline.extend(prev);
                }
                cap_oneline(&mut oneline);
                if first_seen {
                    slug_order.push(slug.clone());
                }
                per_slug.insert(
                    slug,
                    SlugState::Local {
                        oneline_newest_first: oneline,
                    },
                );
            }
            (REPO_SYNC_KIND_CONFLICT, _existing) => {
                let reason = payload
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(reason missing)")
                    .to_string();
                let detail = payload
                    .get("detail")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if first_seen {
                    slug_order.push(slug.clone());
                }
                per_slug.insert(slug, SlugState::Conflict { reason, detail });
            }
            (other_kind, existing) => {
                if let Some(prev) = existing {
                    per_slug.insert(slug.clone(), prev);
                }
                tracing::warn!(
                    event_id = e.id,
                    kind = %other_kind,
                    slug = %slug,
                    "collapse_repo_sync: unknown repo_sync kind — passing through uncollapsed"
                );
                malformed.push(e);
            }
        }
    }

    let mut pulled: Vec<PulledEntry> = Vec::new();
    let mut local: Vec<LocalEntry> = Vec::new();
    let mut conflicts: Vec<ConflictEntry> = Vec::new();
    for slug in slug_order {
        match per_slug.remove(&slug) {
            Some(SlugState::Pulled {
                oneline_newest_first,
            }) => {
                pulled.push(PulledEntry {
                    slug,
                    oneline: oneline_newest_first,
                });
            }
            Some(SlugState::Local {
                oneline_newest_first,
            }) => {
                local.push(LocalEntry {
                    slug,
                    oneline: oneline_newest_first,
                });
            }
            Some(SlugState::Conflict { reason, detail }) => {
                conflicts.push(ConflictEntry {
                    slug,
                    reason,
                    detail,
                });
            }
            None => {}
        }
    }

    let mut events = other;
    events.extend(malformed);

    if pulled.is_empty() && local.is_empty() && conflicts.is_empty() {
        return CollapsedDrain {
            events,
            original_repo_sync_ids: original_ids,
        };
    }

    let summary_payload = RepoSyncSummaryPayload {
        conflicts: &conflicts,
        kind: REPO_SYNC_KIND_SUMMARY,
        local: &local,
        pulled: &pulled,
    };
    let summary_text = format_summary_line(&pulled, &local, &conflicts);
    events.push(Event {
        id: SYNTHETIC_EVENT_ID,
        conversation_id: SYNTHETIC_EVENT_ID,
        source: REPO_SYNC_SOURCE_SUMMARY.to_string(),
        summary: summary_text,
        payload: serde_json::to_string(&summary_payload)
            .expect("RepoSyncSummaryPayload serialization is infallible"),
        created_at: Utc::now(),
    });

    CollapsedDrain {
        events,
        original_repo_sync_ids: original_ids,
    }
}

fn extract_oneline(payload: &serde_json::Value) -> Vec<String> {
    payload
        .get("oneline")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn format_summary_line(
    pulled: &[PulledEntry],
    local: &[LocalEntry],
    conflicts: &[ConflictEntry],
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !pulled.is_empty() {
        let pulled_slugs: Vec<&str> = pulled.iter().map(|p| p.slug.as_str()).collect();
        parts.push(format!("pulled: {}", pulled_slugs.join(", ")));
    }
    if !local.is_empty() {
        let local_slugs: Vec<&str> = local.iter().map(|l| l.slug.as_str()).collect();
        parts.push(format!("local: {}", local_slugs.join(", ")));
    }
    if !conflicts.is_empty() {
        let conflict_slugs: Vec<&str> = conflicts.iter().map(|c| c.slug.as_str()).collect();
        parts.push(format!("conflicts: {}", conflict_slugs.join(", ")));
    }
    if parts.is_empty() {
        "repo sync summary".to_string()
    } else {
        format!("repo sync summary — {}", parts.join("; "))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_event(id: i64, source: &str, payload: &str) -> Event {
        Event {
            id,
            conversation_id: 1,
            source: source.to_string(),
            summary: format!("summary {id}"),
            payload: payload.to_string(),
            created_at: "2026-04-11T07:00:00Z".parse().unwrap(),
        }
    }

    fn summary_payload_json(events: &[Event]) -> serde_json::Value {
        let synth = events
            .iter()
            .find(|e| e.source == "repo_sync:summary")
            .expect("missing synthesized summary");
        serde_json::from_str(&synth.payload).unwrap()
    }

    // -----------------------------------------------------------------------
    // format_event_batch
    // -----------------------------------------------------------------------

    #[test]
    fn format_batch_none_for_empty() {
        assert!(format_event_batch(&[]).is_none());
    }

    #[test]
    fn format_batch_produces_readable_output() {
        let events = vec![
            Event {
                id: 1,
                conversation_id: 1,
                source: "cron".into(),
                summary: "Morning briefing".into(),
                payload: r#"{"job":"morning"}"#.into(),
                created_at: "2026-04-11T07:00:00Z".parse().unwrap(),
            },
            Event {
                id: 2,
                conversation_id: 1,
                source: "discord".into(),
                summary: "Message from Bob".into(),
                payload: r#"{"text":"hi"}"#.into(),
                created_at: "2026-04-11T08:15:00Z".parse().unwrap(),
            },
        ];
        let batch = format_event_batch(&events).unwrap();
        assert!(batch.contains("[Events while you were away]"));
        assert!(batch.contains("Morning briefing"));
        assert!(batch.contains("Message from Bob"));
        assert!(batch.contains("Full event data (JSON):"));
    }

    #[test]
    fn format_batch_invalid_json_payload() {
        let events = vec![Event {
            id: 1,
            conversation_id: 1,
            source: "test".into(),
            summary: "Bad payload".into(),
            payload: "not valid json".into(),
            created_at: "2026-04-11T07:00:00Z".parse().unwrap(),
        }];
        let batch = format_event_batch(&events).unwrap();
        assert!(batch.contains("not valid json"));
    }

    // -----------------------------------------------------------------------
    // is_repo_sync_source
    // -----------------------------------------------------------------------

    #[test]
    fn is_repo_sync_source_recognizes_prefixed_sources() {
        assert!(is_repo_sync_source("repo_sync:pulled"));
        assert!(is_repo_sync_source("repo_sync:local"));
        assert!(is_repo_sync_source("repo_sync:conflict"));
        assert!(is_repo_sync_source("repo_sync:summary"));
        assert!(!is_repo_sync_source("cron"));
        assert!(!is_repo_sync_source("repo_syncpulled"));
    }

    // -----------------------------------------------------------------------
    // staleness filter
    // -----------------------------------------------------------------------

    #[test]
    fn split_stale_active_conversation_keeps_everything() {
        let now = "2026-04-17T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let updated_at = now - chrono::Duration::days(1);
        let events = vec![
            mk_event(1, "cron", "{}"),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"a","oneline":["x"]}"#,
            ),
        ];
        let (kept, stale) = split_stale_repo_sync(events, updated_at, now, 7);
        assert_eq!(kept.len(), 2);
        assert!(stale.is_empty());
    }

    #[test]
    fn split_stale_drops_only_repo_sync_for_stale_conv() {
        let now = "2026-04-17T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let updated_at = now - chrono::Duration::days(30);
        let events = vec![
            mk_event(1, "cron", "{}"),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"a","oneline":["x"]}"#,
            ),
            mk_event(3, "discord", "{}"),
            mk_event(
                4,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"b","reason":"r","detail":"d"}"#,
            ),
        ];
        let (kept, stale) = split_stale_repo_sync(events, updated_at, now, 7);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].source, "cron");
        assert_eq!(kept[1].source, "discord");
        let stale_sources: Vec<&str> = stale.iter().map(|e| e.source.as_str()).collect();
        assert_eq!(
            stale_sources,
            vec!["repo_sync:pulled", "repo_sync:conflict"]
        );
    }

    #[test]
    fn split_stale_days_zero_treats_everything_as_stale() {
        let now = "2026-04-17T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let updated_at = now - chrono::Duration::nanoseconds(1);
        let events = vec![mk_event(
            1,
            "repo_sync:pulled",
            r#"{"kind":"pulled","slug":"a","oneline":["x"]}"#,
        )];
        let (kept, stale) = split_stale_repo_sync(events, updated_at, now, 0);
        assert!(kept.is_empty());
        assert_eq!(stale.len(), 1);
    }

    #[test]
    fn split_stale_boundary_exactly_at_cutoff_is_kept() {
        let now = "2026-04-17T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let updated_at = now - chrono::Duration::days(7);
        let events = vec![mk_event(
            1,
            "repo_sync:pulled",
            r#"{"kind":"pulled","slug":"a","oneline":["x"]}"#,
        )];
        let (kept, stale) = split_stale_repo_sync(events, updated_at, now, 7);
        assert_eq!(kept.len(), 1);
        assert!(stale.is_empty());
    }

    #[test]
    fn repo_sync_staleness_days_round_trip_via_atomic() {
        let original = repo_sync_staleness_days();
        set_repo_sync_staleness_days(42);
        assert_eq!(repo_sync_staleness_days(), 42);
        set_repo_sync_staleness_days(original);
    }

    #[test]
    fn set_staleness_days_at_max_does_not_panic() {
        let original = repo_sync_staleness_days();
        set_repo_sync_staleness_days(MAX_REPO_SYNC_STALENESS_DAYS);
        assert_eq!(repo_sync_staleness_days(), MAX_REPO_SYNC_STALENESS_DAYS);
        set_repo_sync_staleness_days(original);
    }

    #[test]
    #[should_panic(expected = "exceeds MAX_REPO_SYNC_STALENESS_DAYS")]
    fn set_staleness_days_above_max_panics() {
        set_repo_sync_staleness_days(MAX_REPO_SYNC_STALENESS_DAYS + 1);
    }

    // -----------------------------------------------------------------------
    // collapser
    // -----------------------------------------------------------------------

    #[test]
    fn collapser_no_repo_sync_events_pass_through() {
        let events = vec![mk_event(1, "cron", "{}"), mk_event(2, "discord", "{}")];
        let collapsed = collapse_repo_sync(events);
        assert_eq!(collapsed.events.len(), 2);
        assert!(collapsed.original_repo_sync_ids.is_empty());
        assert!(
            !collapsed
                .events
                .iter()
                .any(|e| e.source == "repo_sync:summary")
        );
    }

    #[test]
    fn collapser_single_pulled_event_synthesizes_summary() {
        let events = vec![mk_event(
            42,
            "repo_sync:pulled",
            r#"{"kind":"pulled","slug":"life","remote":"r","oneline":["abc hello"]}"#,
        )];
        let collapsed = collapse_repo_sync(events);
        assert_eq!(collapsed.original_repo_sync_ids, vec![42]);
        let payload = summary_payload_json(&collapsed.events);
        assert_eq!(payload["kind"], "git_update");
        assert_eq!(payload["pulled"][0]["slug"], "life");
        assert_eq!(payload["pulled"][0]["oneline"][0], "abc hello");
        assert!(payload["conflicts"].as_array().unwrap().is_empty());
    }

    #[test]
    fn collapser_merges_multiple_pulled_same_slug_newest_first() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"graf","oneline":["aaa first","bbb second"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"graf","oneline":["ccc third"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        let oneline = payload["pulled"][0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 3);
        assert_eq!(oneline[0], "ccc third");
        assert_eq!(oneline[1], "aaa first");
        assert_eq!(oneline[2], "bbb second");
    }

    #[test]
    fn collapser_pulled_then_conflict_keeps_conflict() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"life","oneline":["x"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"life","reason":"div","detail":"d"}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["pulled"].as_array().unwrap().is_empty());
        assert_eq!(payload["conflicts"][0]["slug"], "life");
        assert_eq!(payload["conflicts"][0]["reason"], "div");
    }

    #[test]
    fn collapser_conflict_then_pulled_keeps_pulled() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"life","reason":"old","detail":"d"}"#,
            ),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"life","oneline":["new"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["conflicts"].as_array().unwrap().is_empty());
        assert_eq!(payload["pulled"][0]["slug"], "life");
    }

    #[test]
    fn collapser_caps_merged_oneline_at_ten() {
        let first: Vec<String> = (0..10).map(|i| format!("h{i:02} commit {i}")).collect();
        let second: Vec<String> = (10..15).map(|i| format!("h{i:02} commit {i}")).collect();
        let p1 = serde_json::json!({"kind":"pulled","slug":"big","oneline": first});
        let p2 = serde_json::json!({"kind":"pulled","slug":"big","oneline": second});
        let events = vec![
            mk_event(1, "repo_sync:pulled", &p1.to_string()),
            mk_event(2, "repo_sync:pulled", &p2.to_string()),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        let oneline = payload["pulled"][0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 10);
        assert_eq!(oneline[0], "h10 commit 10");
        let last = oneline[9].as_str().unwrap();
        assert!(last.starts_with("... "), "got {last:?}");
        assert!(last.contains("more (older)"), "got {last:?}");
    }

    #[test]
    fn collapser_multi_slug_preserves_first_seen_order() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"first","oneline":["a"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"second","oneline":["b"]}"#,
            ),
            mk_event(
                3,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"first","reason":"r","detail":"d"}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert_eq!(payload["conflicts"][0]["slug"], "first");
        assert_eq!(payload["pulled"][0]["slug"], "second");
    }

    #[test]
    fn collapser_preserves_non_repo_sync_order() {
        let events = vec![
            mk_event(1, "cron", "{}"),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"x","oneline":["c"]}"#,
            ),
            mk_event(3, "discord", "{}"),
        ];
        let collapsed = collapse_repo_sync(events);
        assert_eq!(collapsed.events[0].source, "cron");
        assert_eq!(collapsed.events[1].source, "discord");
        assert_eq!(collapsed.events[2].source, "repo_sync:summary");
    }

    #[test]
    fn collapser_unknown_kind_between_valid_events_preserves_state() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"first","oneline":["a"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"second","oneline":["b"]}"#,
            ),
            mk_event(3, "repo_sync:weird", r#"{"kind":"weird","slug":"first"}"#),
            mk_event(
                4,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"first","reason":"r","detail":"d"}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert_eq!(payload["conflicts"][0]["slug"], "first");
        assert_eq!(payload["pulled"][0]["slug"], "second");
        assert!(
            collapsed
                .events
                .iter()
                .any(|e| e.id == 3 && e.source == "repo_sync:weird")
        );
    }

    #[test]
    fn collapser_malformed_payload_passes_through() {
        let events = vec![
            mk_event(1, "repo_sync:pulled", "not json"),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"good","oneline":["g"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let non_synth_ids: Vec<i64> = collapsed
            .events
            .iter()
            .filter(|e| e.source != "repo_sync:summary")
            .map(|e| e.id)
            .collect();
        assert!(
            non_synth_ids.contains(&1),
            "malformed event should be passed through, got {non_synth_ids:?}"
        );
    }

    #[test]
    fn collapse_repo_sync_groups_local_entries() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:local",
                r#"{"kind":"local","slug":"slug-a","oneline":["aa one"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:local",
                r#"{"kind":"local","slug":"slug-b","oneline":["bb one"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["pulled"].as_array().unwrap().is_empty());
        assert!(payload["conflicts"].as_array().unwrap().is_empty());
        let local = payload["local"].as_array().unwrap();
        assert_eq!(local.len(), 2);
        assert_eq!(local[0]["slug"], "slug-a");
        assert_eq!(local[1]["slug"], "slug-b");
    }

    #[test]
    fn collapse_repo_sync_local_and_pulled_coexist_per_slug_latest_wins() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"s","oneline":["from-remote"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:local",
                r#"{"kind":"local","slug":"s","oneline":["local-commit"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["pulled"].as_array().unwrap().is_empty());
        let local = payload["local"].as_array().unwrap();
        assert_eq!(local.len(), 1);
        assert_eq!(local[0]["slug"], "s");
        let oneline = local[0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 1);
        assert_eq!(oneline[0], "local-commit");

        let events = vec![
            mk_event(
                1,
                "repo_sync:local",
                r#"{"kind":"local","slug":"s","oneline":["local-commit"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"s","oneline":["from-remote"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["local"].as_array().unwrap().is_empty());
        let pulled = payload["pulled"].as_array().unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0]["slug"], "s");
        let oneline = pulled[0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 1);
        assert_eq!(oneline[0], "from-remote");
    }

    #[test]
    fn collapse_repo_sync_local_and_conflict_latest_wins() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:local",
                r#"{"kind":"local","slug":"s","oneline":["local-commit"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"s","reason":"div","detail":"d"}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["local"].as_array().unwrap().is_empty());
        let conflicts = payload["conflicts"].as_array().unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0]["slug"], "s");
    }

    #[test]
    fn collapser_conflict_then_local_keeps_local() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"life","reason":"old","detail":"d"}"#,
            ),
            mk_event(
                2,
                "repo_sync:local",
                r#"{"kind":"local","slug":"life","oneline":["new"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert!(payload["conflicts"].as_array().unwrap().is_empty());
        assert_eq!(payload["local"][0]["slug"], "life");
    }

    #[test]
    fn collapser_merges_multiple_local_same_slug_newest_first() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:local",
                r#"{"kind":"local","slug":"graf","oneline":["aaa first","bbb second"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:local",
                r#"{"kind":"local","slug":"graf","oneline":["ccc third"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        let oneline = payload["local"][0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 3);
        assert_eq!(oneline[0], "ccc third");
        assert_eq!(oneline[1], "aaa first");
        assert_eq!(oneline[2], "bbb second");
    }

    #[test]
    fn collapser_caps_merged_oneline_at_ten_for_local() {
        let first: Vec<String> = (0..10).map(|i| format!("h{i:02} commit {i}")).collect();
        let second: Vec<String> = (10..15).map(|i| format!("h{i:02} commit {i}")).collect();
        let p1 = serde_json::json!({"kind":"local","slug":"big","oneline": first});
        let p2 = serde_json::json!({"kind":"local","slug":"big","oneline": second});
        let events = vec![
            mk_event(1, "repo_sync:local", &p1.to_string()),
            mk_event(2, "repo_sync:local", &p2.to_string()),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        let oneline = payload["local"][0]["oneline"].as_array().unwrap();
        assert_eq!(oneline.len(), 10);
        assert_eq!(oneline[0], "h10 commit 10");
        let last = oneline[9].as_str().unwrap();
        assert!(last.starts_with("... "), "got {last:?}");
        assert!(last.contains("more (older)"), "got {last:?}");
    }

    #[test]
    fn summary_text_includes_local_section_when_present() {
        let events = vec![
            mk_event(
                1,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"p-slug","oneline":["x"]}"#,
            ),
            mk_event(
                2,
                "repo_sync:local",
                r#"{"kind":"local","slug":"l-slug","oneline":["y"]}"#,
            ),
            mk_event(
                3,
                "repo_sync:conflict",
                r#"{"kind":"conflict","slug":"c-slug","reason":"r","detail":"d"}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let synth = collapsed
            .events
            .iter()
            .find(|e| e.source == "repo_sync:summary")
            .expect("missing synthesized summary");
        let summary = &synth.summary;
        assert!(summary.contains("pulled: p-slug"), "got {summary:?}");
        assert!(summary.contains("local: l-slug"), "got {summary:?}");
        assert!(summary.contains("conflicts: c-slug"), "got {summary:?}");
        let pulled_idx = summary.find("pulled:").unwrap();
        let local_idx = summary.find("local:").unwrap();
        let conflicts_idx = summary.find("conflicts:").unwrap();
        assert!(pulled_idx < local_idx, "got {summary:?}");
        assert!(local_idx < conflicts_idx, "got {summary:?}");
    }

    #[test]
    fn repo_sync_summary_matches_reference() {
        let pulled = vec![PulledEntry {
            slug: "my-repo".to_string(),
            oneline: vec!["abc1234 add feature".to_string()],
        }];
        let local = vec![LocalEntry {
            slug: "local-repo".to_string(),
            oneline: vec!["def5678 local commit".to_string()],
        }];
        let conflicts = vec![ConflictEntry {
            slug: "conflict-repo".to_string(),
            reason: "diverged".to_string(),
            detail: "local has uncommitted changes".to_string(),
        }];

        let payload = RepoSyncSummaryPayload {
            conflicts: &conflicts,
            kind: REPO_SYNC_KIND_SUMMARY,
            local: &local,
            pulled: &pulled,
        };
        let produced_str = serde_json::to_string(&payload)
            .expect("RepoSyncSummaryPayload serialization is infallible");
        let reference = serde_json::json!({
            "kind": REPO_SYNC_KIND_SUMMARY,
            "pulled": pulled,
            "local": local,
            "conflicts": conflicts,
        });
        let produced_val: serde_json::Value =
            serde_json::from_str(&produced_str).expect("produced_str must be valid JSON");
        assert_eq!(produced_val, reference);
    }

    #[test]
    fn collapser_unknown_kind_on_new_slug_does_not_create_ghost_order() {
        let events = vec![
            mk_event(1, "repo_sync:weird", r#"{"kind":"weird","slug":"ghost"}"#),
            mk_event(
                2,
                "repo_sync:pulled",
                r#"{"kind":"pulled","slug":"real","oneline":["r"]}"#,
            ),
        ];
        let collapsed = collapse_repo_sync(events);
        let payload = summary_payload_json(&collapsed.events);
        assert_eq!(payload["pulled"].as_array().unwrap().len(), 1);
        assert_eq!(payload["pulled"][0]["slug"], "real");
        assert!(payload["conflicts"].as_array().unwrap().is_empty());
    }

    #[test]
    fn cap_oneline_truncates_at_cap() {
        let mut lines: Vec<String> = (0..=ONELINE_CAP).map(|i| format!("commit {i}")).collect();
        assert_eq!(lines.len(), ONELINE_CAP + 1);
        cap_oneline(&mut lines);
        assert_eq!(lines.len(), ONELINE_CAP);
        assert!(lines.last().unwrap().contains("more (older)"));
    }

    #[test]
    fn cap_oneline_at_exactly_cap_is_noop() {
        let mut lines: Vec<String> = (0..ONELINE_CAP).map(|i| format!("commit {i}")).collect();
        let before = lines.clone();
        cap_oneline(&mut lines);
        assert_eq!(lines, before);
    }
}
