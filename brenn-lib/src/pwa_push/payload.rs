//! Service-worker push payload (§2.6.3).
//!
//! JSON-serialized and encrypted per RFC 8291. The SW deserializes this after
//! decryption and calls `showNotification(title, { body, icon, badge, tag, data })`.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Payload delivered to the service worker via Web Push.
///
/// The SW reads `user_id` and compares it against the `signed_in_user_ids`
/// IndexedDB set before showing the notification (defense-in-depth; the
/// server-side current-user check is the primary defense).
///
/// All fields except `title`, `body`, and `user_id` are optional; the SW
/// falls back gracefully on missing fields (never throws).
///
/// Forward-compat: `actions` (notification action buttons) slot is reserved
/// for a future non-breaking addition.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export, export_to = "../../frontend/src/generated/")]
pub struct PushPayload {
    /// Notification title.
    pub title: String,
    /// Notification body text.
    pub body: String,
    /// Notification icon URL (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Small monochrome badge icon URL (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub badge: Option<String>,
    /// OS-side notification grouping tag (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Arbitrary JSON object data passed through to the SW (optional).
    /// Must be a JSON object (string keys, any values) — not a scalar or array.
    /// The `#[ts(type = "...")]` annotation overrides the generated TS type to
    /// `Record<string, unknown>`, which matches the Rust constraint that `data`
    /// is an object (not a scalar or array). Using `Map` on the Rust side
    /// prevents callers from accidentally passing a JSON scalar or array.
    /// Size counts toward the 3993-byte plaintext cap on the enclosing payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(type = "Record<string, unknown> | null")]
    pub data: Option<serde_json::Map<String, serde_json::Value>>,
    /// `users.id` rowid for the user this push is addressed to.
    /// The SW drops the notification if this id is not in the
    /// `signed_in_user_ids` IndexedDB set.
    ///
    /// `i64` (matches DB rowid); the `#[ts(type = "number")]` annotation
    /// matches the convention used throughout `ws_types.rs` for rowid-shaped
    /// IDs and yields JS `number` on both sides — required so the set-
    /// membership check works without bigint/number coercion.
    #[ts(type = "number")]
    pub user_id: i64,
}
