//! PWA push subscription DB operations.
//!
//! Schema is migrated by [`run_pwa_push_migrations`], called from
//! `crate::db::run_migrations` after the usage tables.
//!
//! Subscriptions are keyed by `(device_id, user_id)` UNIQUE — one active
//! push subscription per (device, user) pair. Re-registration replaces the
//! prior row (last-write-wins).
//!
//! The `messaging_channels` intern table is shared with `brenn:` messaging.
//! `ensure_pwa_channel` inserts a channel row on first publish; subscribe
//! time does NOT pre-create rows (orphan-row prevention, per design §2.5).
//!
use base64ct::{Base64UrlUnpadded, Encoding as _};
use chrono::Utc;
use rusqlite::Connection;
use uuid::Uuid;

use crate::pwa_push::endpoint_validator::{
    EndpointPolicy, RejectReason, ValidatedEndpoint, validate_endpoint,
};
use crate::pwa_push::targets::PwaPushAddress;

/// Validation errors for `PushSubscribe` wire fields.
#[derive(Debug, PartialEq, Eq)]
pub enum SubscribeValidationError {
    /// `p256dh` is not valid base64url-no-pad.
    P256dhNotBase64Url(String),
    /// `p256dh` does not decode to exactly 65 bytes.
    P256dhWrongLength(usize),
    /// `p256dh` decoded bytes do not start with `0x04` (uncompressed P-256 point marker).
    P256dhNotUncompressed,
    /// `auth` is not valid base64url-no-pad.
    AuthNotBase64Url(String),
    /// `auth` does not decode to exactly 16 bytes.
    AuthWrongLength(usize),
    /// `endpoint` failed host-level validation.
    Endpoint(RejectReason),
}

impl std::fmt::Display for SubscribeValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::P256dhNotBase64Url(e) => write!(f, "p256dh: invalid base64url: {e}"),
            Self::P256dhWrongLength(n) => {
                write!(f, "p256dh: wrong byte length {n} (expected 65)")
            }
            Self::P256dhNotUncompressed => {
                write!(f, "p256dh: first byte is not 0x04 (uncompressed point)")
            }
            Self::AuthNotBase64Url(e) => write!(f, "auth: invalid base64url: {e}"),
            Self::AuthWrongLength(n) => write!(f, "auth: wrong byte length {n} (expected 16)"),
            Self::Endpoint(reason) => {
                write!(f, "endpoint: rejected (reason={})", reason.code())
            }
        }
    }
}

/// Validate the three wire fields from a `PushSubscribe` message.
///
/// - `p256dh` must be valid base64url-no-pad decoding to exactly 65 bytes
///   starting with `0x04` (uncompressed P-256 public key).
/// - `auth` must be valid base64url-no-pad decoding to exactly 16 bytes.
/// - `endpoint` must be a valid HTTPS URL that passes the host-level SSRF
///   validation rules in `endpoint_validator` (IP-block rules and allowlist).
///
/// Returns `Ok(ValidatedEndpoint)` — the `url::Url`-normalized URL wrapped in
/// a newtype. The caller **must** pass this value to `upsert_subscription`
/// (not the raw input) to prevent parser-confusion SSRF bypass.
pub fn validate_push_subscribe_fields(
    endpoint: &str,
    p256dh: &str,
    auth: &str,
    policy: &EndpointPolicy,
) -> Result<ValidatedEndpoint, SubscribeValidationError> {
    // Validate p256dh.
    let p256dh_bytes = Base64UrlUnpadded::decode_vec(p256dh)
        .map_err(|e| SubscribeValidationError::P256dhNotBase64Url(e.to_string()))?;
    if p256dh_bytes.len() != 65 {
        return Err(SubscribeValidationError::P256dhWrongLength(
            p256dh_bytes.len(),
        ));
    }
    if p256dh_bytes[0] != 0x04 {
        return Err(SubscribeValidationError::P256dhNotUncompressed);
    }

    // Validate auth.
    let auth_bytes = Base64UrlUnpadded::decode_vec(auth)
        .map_err(|e| SubscribeValidationError::AuthNotBase64Url(e.to_string()))?;
    if auth_bytes.len() != 16 {
        return Err(SubscribeValidationError::AuthWrongLength(auth_bytes.len()));
    }

    // Validate endpoint: HTTPS URL with SSRF-safe host (IP-block rules + allowlist).
    // Returns a ValidatedEndpoint wrapping the url::Url-normalized string.
    let validated =
        validate_endpoint(endpoint, policy).map_err(SubscribeValidationError::Endpoint)?;

    Ok(validated)
}

/// Run the `pwa_push_subscriptions` table migration. Idempotent.
pub fn run_pwa_push_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS pwa_push_subscriptions (
            id              INTEGER PRIMARY KEY,
            device_id       INTEGER NOT NULL REFERENCES devices(id),
            user_id         INTEGER NOT NULL REFERENCES users(id),
            endpoint        TEXT NOT NULL,
            p256dh_b64url   TEXT NOT NULL,
            auth_b64url     TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            last_used_at    TEXT NOT NULL,
            UNIQUE (device_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pwa_push_subscriptions_user
            ON pwa_push_subscriptions(user_id);
        -- Index for the current-user correlated subquery (MAX(last_seen_at)
        -- WHERE device_id = ?). Without this index, every current-user check
        -- query scans all device_users rows for a device in O(N) per outer row.
        CREATE INDEX IF NOT EXISTS idx_device_users_device_last_seen
            ON device_users(device_id, last_seen_at DESC);
        ",
    )
    .expect("failed to run pwa_push migrations");
}

/// A subscription row as returned by list queries.
///
/// `Debug` is implemented manually to redact `endpoint` and `auth_b64url`
/// (both are sensitive — treat like cookies per design §2.9). `p256dh_b64url`
/// is a public key and is shown in full.
#[derive(Clone)]
pub struct SubscriptionRow {
    pub id: i64,
    pub device_id: i64,
    pub user_id: i64,
    pub endpoint: String,
    pub p256dh_b64url: String,
    pub auth_b64url: String,
    pub last_used_at: String,
}

impl std::fmt::Debug for SubscriptionRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact endpoint and auth_b64url: show first 16 chars + length.
        // Use char-boundary-safe truncation to avoid panicking on multi-byte UTF-8.
        let ep_preview = crate::pwa_push::endpoint_preview(&self.endpoint);
        let auth_preview: String = self.auth_b64url.chars().take(16).collect();
        f.debug_struct("SubscriptionRow")
            .field("id", &self.id)
            .field("device_id", &self.device_id)
            .field("user_id", &self.user_id)
            .field(
                "endpoint",
                &format!("{ep_preview}...(len={})", self.endpoint.len()),
            )
            .field("p256dh_b64url", &self.p256dh_b64url)
            .field(
                "auth_b64url",
                &format!("{auth_preview}...(len={})", self.auth_b64url.len()),
            )
            .field("last_used_at", &self.last_used_at)
            .finish()
    }
}

/// Upsert a subscription row for `(device_id, user_id)`.
///
/// Requires a [`ValidatedEndpoint`] — the type-system guarantee that
/// `validate_endpoint` (or `validate_push_subscribe_fields`) was called before
/// inserting. On conflict (same device+user pair) replaces endpoint and keys
/// (last-write-wins). `created_at` is only set on first insert.
pub fn upsert_subscription(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    endpoint: &ValidatedEndpoint,
    p256dh_b64url: &str,
    auth_b64url: &str,
) {
    let now = crate::db::format_ts_for_db(Utc::now());
    conn.execute(
        "INSERT INTO pwa_push_subscriptions
             (device_id, user_id, endpoint, p256dh_b64url, auth_b64url, created_at, last_used_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
         ON CONFLICT(device_id, user_id) DO UPDATE SET
             endpoint       = excluded.endpoint,
             p256dh_b64url  = excluded.p256dh_b64url,
             auth_b64url    = excluded.auth_b64url,
             last_used_at   = excluded.last_used_at",
        rusqlite::params![
            device_id,
            user_id,
            endpoint.as_str(),
            p256dh_b64url,
            auth_b64url,
            now
        ],
    )
    .expect("pwa_push: upsert_subscription");
}

/// Delete the subscription row for `(device_id, user_id)`.
///
/// No-op if no row exists (idempotent).
pub fn delete_subscription(conn: &Connection, device_id: i64, user_id: i64) {
    conn.execute(
        "DELETE FROM pwa_push_subscriptions WHERE device_id = ?1 AND user_id = ?2",
        rusqlite::params![device_id, user_id],
    )
    .expect("pwa_push: delete_subscription");
}

/// Delete all subscription rows for a given `device_id`.
///
/// Used by `unenroll_device` to atomically clean up all push subscriptions
/// for a device as part of the unenroll transaction. 0..N rows affected;
/// no panic on 0 (idempotent if called twice or for a device with no subs).
pub fn delete_all_subscriptions_for_device(conn: &Connection, device_id: i64) {
    conn.execute(
        "DELETE FROM pwa_push_subscriptions WHERE device_id = ?1",
        rusqlite::params![device_id],
    )
    .expect("pwa_push: delete_all_subscriptions_for_device");
}

/// Delete a subscription row by its primary key `id`.
///
/// Used when a push attempt returns 410/404 (push service reports the
/// subscription is gone). No-op if the row was already deleted.
pub fn delete_subscription_by_id(conn: &Connection, id: i64) {
    conn.execute(
        "DELETE FROM pwa_push_subscriptions WHERE id = ?1",
        rusqlite::params![id],
    )
    .expect("pwa_push: delete_subscription_by_id");
}

/// Update `last_used_at` for a subscription row.
///
/// Called after a successful 201 response from the push service.
pub fn touch_subscription(conn: &Connection, id: i64) {
    let now = crate::db::format_ts_for_db(Utc::now());
    conn.execute(
        "UPDATE pwa_push_subscriptions SET last_used_at = ?1 WHERE id = ?2",
        rusqlite::params![now, id],
    )
    .expect("pwa_push: touch_subscription");
}

/// Returns `true` if a subscription row exists for `(device_id, user_id)`.
pub fn subscription_exists(conn: &Connection, device_id: i64, user_id: i64) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pwa_push_subscriptions WHERE device_id = ?1 AND user_id = ?2)",
        rusqlite::params![device_id, user_id],
        |row| row.get::<_, bool>(0),
    )
    .expect("pwa_push: subscription_exists")
}

/// All subscriptions for a given user, regardless of device.
///
/// Used by fan-out sends (`pwa_push:<user>` address). Returns all
/// subscriptions; the caller applies the current-user check.
pub fn list_subscriptions_for_user(conn: &Connection, user_id: i64) -> Vec<SubscriptionRow> {
    let mut stmt = conn
        .prepare(
            "SELECT id, device_id, user_id, endpoint, p256dh_b64url, auth_b64url, last_used_at
             FROM pwa_push_subscriptions
             WHERE user_id = ?1
             ORDER BY id",
        )
        .expect("pwa_push: prepare list_subscriptions_for_user");
    stmt.query_map(rusqlite::params![user_id], row_to_subscription)
        .expect("pwa_push: query list_subscriptions_for_user")
        .map(|r| r.expect("pwa_push: read subscription row"))
        .collect()
}

/// The single subscription for a specific `(device_id, user_id)` pair.
///
/// Returns `None` if no subscription is registered for that pair.
pub fn get_subscription_for_device_user(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
) -> Option<SubscriptionRow> {
    match conn.query_row(
        "SELECT id, device_id, user_id, endpoint, p256dh_b64url, auth_b64url, last_used_at
         FROM pwa_push_subscriptions
         WHERE device_id = ?1 AND user_id = ?2",
        rusqlite::params![device_id, user_id],
        row_to_subscription,
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("pwa_push: get_subscription_for_device_user failed: {e}"),
    }
}

/// All subscriptions where the subscription's user is the most-recently-seen
/// user on that device (the current-user check from design §2.7.3 step 7).
///
/// Used by fan-out sends and `PushListTargets` to skip stale subscriptions.
pub fn list_subscriptions_current_user_only(
    conn: &Connection,
    user_id: i64,
) -> Vec<SubscriptionRow> {
    let sql = format!(
        "SELECT s.id, s.device_id, s.user_id, s.endpoint,
                s.p256dh_b64url, s.auth_b64url, s.last_used_at
         FROM pwa_push_subscriptions s
         {TOP_USER_JOIN}
         WHERE s.user_id = ?1
         ORDER BY s.id"
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("pwa_push: prepare list_subscriptions_current_user_only");
    stmt.query_map(rusqlite::params![user_id], row_to_subscription)
        .expect("pwa_push: query list_subscriptions_current_user_only")
        .map(|r| r.expect("pwa_push: read subscription row"))
        .collect()
}

/// All subscriptions across all users, with the current-user filter applied,
/// joined to user and device info for `PushListTargets`.
///
/// Returns `(subscription_row, username, assigned_slug, guessed_slug,
/// device_last_seen_at)`.
pub fn list_all_subscriptions_with_device_info(
    conn: &Connection,
) -> Vec<(SubscriptionRow, String, Option<String>, String, String)> {
    let sql = format!(
        "SELECT s.id, s.device_id, s.user_id, s.endpoint,
                s.p256dh_b64url, s.auth_b64url, s.last_used_at,
                u.username, du.assigned_slug, d.guessed_slug, d.last_seen_at
         FROM pwa_push_subscriptions s
         JOIN users u ON u.id = s.user_id
         JOIN devices d ON d.id = s.device_id
         JOIN device_users du ON du.device_id = s.device_id AND du.user_id = s.user_id
         {TOP_USER_JOIN}
         ORDER BY u.username, s.device_id"
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("pwa_push: prepare list_all_subscriptions_with_device_info");
    stmt.query_map([], |row| {
        let sub = row_to_subscription(row)?;
        let username: String = row.get(7)?;
        let assigned_slug: Option<String> = row.get(8)?;
        let guessed_slug: String = row.get(9)?;
        let device_last_seen_at: String = row.get(10)?;
        Ok((
            sub,
            username,
            assigned_slug,
            guessed_slug,
            device_last_seen_at,
        ))
    })
    .expect("pwa_push: query list_all_subscriptions_with_device_info")
    .map(|r| r.expect("pwa_push: read subscription+device row"))
    .collect()
}

/// Slug resolution predicate shared by `lookup_device_subscription` and
/// `get_subscription_with_device_last_seen_by_username_and_slug`.
///
/// Both queries bind `username` as `?1` and `slug` as `?2`.
/// `assigned_slug` is preferred; `guessed_slug` is the fallback when
/// `assigned_slug IS NULL`.
const SLUG_PREDICATE: &str =
    "(du.assigned_slug = ?2 OR (du.assigned_slug IS NULL AND d.guessed_slug = ?2))";

/// Three-way result of a device subscription lookup combining the current-user
/// check and the subscription existence check in a single query.
#[derive(Debug)]
pub enum DeviceSubscriptionLookup {
    /// No subscription row exists for this `(username, slug)` pair.
    NotFound,
    /// A subscription row exists, but the user is not the current user on the
    /// device (i.e. another user was seen more recently).
    Stale,
    /// A subscription row exists and the user is the current user on the device.
    Current(SubscriptionRow),
}

/// Left-join variant of `TOP_USER_JOIN` (see below): matches `Current` users when
/// found, but does not exclude rows when the user is stale (the join produces NULL
/// for `top_user.user_id` in that case, indicating `Stale`).
///
/// Keep this in sync with `TOP_USER_JOIN`: they share the same inner subquery —
/// only the join type differs (`LEFT JOIN` vs `JOIN`).
const TOP_USER_LEFT_JOIN: &str = "LEFT JOIN (
             SELECT du1.device_id, du1.user_id
             FROM device_users du1
             WHERE du1.last_seen_at = (
                 SELECT MAX(du2.last_seen_at) FROM device_users du2
                 WHERE du2.device_id = du1.device_id
             )
         ) top_user ON top_user.device_id = s.device_id
                   AND top_user.user_id   = s.user_id";

/// Fully-formed SQL for `lookup_device_subscription`. Built once at first use.
static LOOKUP_DEVICE_SUBSCRIPTION_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        format!(
            "SELECT s.id, s.device_id, s.user_id, s.endpoint,
                s.p256dh_b64url, s.auth_b64url, s.last_used_at,
                top_user.user_id AS is_current
         FROM pwa_push_subscriptions s
         JOIN users u ON u.id = s.user_id
         JOIN devices d ON d.id = s.device_id
         JOIN device_users du ON du.device_id = s.device_id AND du.user_id = s.user_id
         {TOP_USER_LEFT_JOIN}
         WHERE u.username = ?1
           AND {SLUG_PREDICATE}
         LIMIT 1"
        )
    });

/// Look up a subscription by username + device slug, returning a three-way
/// result that distinguishes `NotFound`, `Stale`, and `Current`.
///
/// Uses `TOP_USER_LEFT_JOIN` so that the current-user check and existence check
/// happen in a single DB round-trip without double-locking.
pub fn lookup_device_subscription(
    conn: &Connection,
    username: &str,
    slug: &str,
) -> DeviceSubscriptionLookup {
    let sql = &*LOOKUP_DEVICE_SUBSCRIPTION_SQL;
    match conn.query_row(sql, rusqlite::params![username, slug], |row| {
        let sub = row_to_subscription(row)?;
        // `is_current` is NULL when the subscription row exists (base JOINs matched)
        // but the LEFT JOIN found no top-user row for this (device, user) pair —
        // meaning the user is not the most-recently-seen user on the device (Stale).
        // It is non-NULL (Some) when the user IS the current user (Current).
        let is_current: Option<i64> = row.get(7)?;
        Ok((sub, is_current))
    }) {
        Ok((sub, Some(_))) => DeviceSubscriptionLookup::Current(sub),
        Ok((_, None)) => DeviceSubscriptionLookup::Stale,
        Err(rusqlite::Error::QueryReturnedNoRows) => DeviceSubscriptionLookup::NotFound,
        Err(e) => {
            panic!("pwa_push: lookup_device_subscription({username:?}, {slug:?}) failed: {e}")
        }
    }
}

/// Look up a subscription by username + device slug, returning the subscription
/// and the device's `last_seen_at` timestamp for computing the effective
/// `last_seen_at = max(device.last_seen_at, sub.last_used_at)`.
///
/// Applies the current-user filter (most-recently-seen user per §2.7.3 step 7).
/// Returns `None` if no subscription exists or the user is stale on the device.
///
/// On a `last_seen_at` tie (two users last seen at the same millisecond on the
/// same device), `TOP_USER_JOIN` is set-valued and `query_row` picks the first
/// row returned. `LIMIT 1` enforces determinism; the ordering is by rusqlite
/// rowid which is stable per-transaction.
pub fn get_subscription_with_device_last_seen_by_username_and_slug(
    conn: &Connection,
    username: &str,
    slug: &str,
) -> Option<(SubscriptionRow, String)> {
    let sql = format!(
        "SELECT s.id, s.device_id, s.user_id, s.endpoint,
                s.p256dh_b64url, s.auth_b64url, s.last_used_at,
                d.last_seen_at
         FROM pwa_push_subscriptions s
         JOIN users u ON u.id = s.user_id
         JOIN devices d ON d.id = s.device_id
         JOIN device_users du ON du.device_id = s.device_id AND du.user_id = s.user_id
         {TOP_USER_JOIN}
         WHERE u.username = ?1
           AND {SLUG_PREDICATE}
         LIMIT 1"
    );
    match conn.query_row(&sql, rusqlite::params![username, slug], |row| {
        let sub = row_to_subscription(row)?;
        let device_last_seen_at: String = row.get(7)?;
        Ok((sub, device_last_seen_at))
    }) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!(
            "pwa_push: get_subscription_with_device_last_seen_by_username_and_slug({username:?}, {slug:?}) failed: {e}"
        ),
    }
}

/// Look up a subscription by username + device slug (assigned preferred, else guessed).
///
/// Returns the subscription row only if found AND the subscription's user is
/// the current-user on that device (most-recently-seen user per §2.7.3 step 7).
/// Returns `None` if no subscription exists or the user is stale on the device.
///
/// Delegates to [`get_subscription_with_device_last_seen_by_username_and_slug`]
/// and discards the device `last_seen_at` field, so both functions share a
/// single SQL body.
pub fn get_subscription_by_username_and_slug(
    conn: &Connection,
    username: &str,
    slug: &str,
) -> Option<SubscriptionRow> {
    get_subscription_with_device_last_seen_by_username_and_slug(conn, username, slug)
        .map(|(sub, _)| sub)
}

/// All subscriptions for a specific user, with device info, applying the
/// current-user filter (§2.7.3 step 7).
///
/// Returns `(subscription_row, device_last_seen_at)` for each subscription
/// where the user is the most-recently-seen user on that device.
///
/// O(user-subs) — WHERE clause is pushed into SQL via a `users` JOIN.
/// Use instead of [`list_all_subscriptions_with_device_info`] when only one
/// user's subscriptions are needed.
pub fn list_subscriptions_with_device_info_for_user(
    conn: &Connection,
    username: &str,
) -> Vec<(SubscriptionRow, String)> {
    let sql = format!(
        "SELECT s.id, s.device_id, s.user_id, s.endpoint,
                s.p256dh_b64url, s.auth_b64url, s.last_used_at,
                d.last_seen_at
         FROM pwa_push_subscriptions s
         JOIN users u ON u.id = s.user_id
         JOIN devices d ON d.id = s.device_id
         JOIN device_users du ON du.device_id = s.device_id AND du.user_id = s.user_id
         {TOP_USER_JOIN}
         WHERE u.username = ?1
         ORDER BY s.id"
    );
    let mut stmt = conn
        .prepare(&sql)
        .expect("pwa_push: prepare list_subscriptions_with_device_info_for_user");
    stmt.query_map(rusqlite::params![username], |row| {
        let sub = row_to_subscription(row)?;
        let device_last_seen_at: String = row.get(7)?;
        Ok((sub, device_last_seen_at))
    })
    .expect("pwa_push: query list_subscriptions_with_device_info_for_user")
    .map(|r| r.expect("pwa_push: read subscription+device row"))
    .collect()
}

/// Intern a `messaging_channels` row for a pwa_push address.
///
/// This is the single entry point for creating channel rows for pwa_push
/// addresses. It MUST be the first step — if the address fails to parse,
/// this function panics (prevents untrusted data from polluting
/// `messaging_channels`).
///
/// Returns the UUID of the (possibly pre-existing) channel row.
///
/// # Panics
///
/// Panics if `address` does not parse as a valid `pwa_push:` address. This
/// ensures that usernames containing `:`, `@`, or whitespace (admitted by
/// `try_create_user` but banned by the address grammar) cannot create
/// malformed rows.
///
/// Intern a `pwa_push:` channel address into `messaging_channels`, returning
/// its UUID.
///
/// Takes a [`PwaPushAddress`] directly to enforce at compile time that the
/// address is well-formed before touching the database. Callers must hold a
/// fully-parsed and canonicalized address (with the username drawn from
/// `users.username`) before calling this function.
pub fn ensure_pwa_channel(conn: &Connection, address: &PwaPushAddress) -> Uuid {
    let address_str = address.to_canonical_string();
    let now = crate::db::format_ts_for_db(Utc::now());
    let new_uuid = Uuid::new_v4();
    let new_uuid_bytes = new_uuid.as_bytes().to_vec();

    // Single-statement upsert with RETURNING: the no-op `DO UPDATE SET
    // address=address` makes RETURNING fire for both the insert and conflict
    // paths, returning the canonical UUID without a second SELECT round-trip.
    let uuid_bytes: Vec<u8> = conn
        .query_row(
            "INSERT INTO messaging_channels (uuid, address, description, created_at)
             VALUES (?1, ?2, NULL, ?3)
             ON CONFLICT(address) DO UPDATE SET address = address
             RETURNING uuid",
            rusqlite::params![new_uuid_bytes, address_str, now],
            |row| row.get(0),
        )
        .expect("pwa_push: ensure_pwa_channel upsert");

    Uuid::from_slice(&uuid_bytes).unwrap_or_else(|e| {
        panic!("pwa_push: messaging_channels.uuid malformed for {address:?}: {e}")
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// SQL fragment that joins the most-recently-seen user per device.
///
/// Produces a derived table aliased as `top_user` with columns
/// `(device_id, user_id)`. Assumes the outer query aliases
/// `pwa_push_subscriptions` as `s`. Used by every query that enforces the
/// current-user check (design §2.7.3 step 7).
///
/// Keeping this as a single constant ensures all current-user check queries
/// stay in sync when the semantics change (e.g. switching to window
/// functions, adding tie-break logic).
const TOP_USER_JOIN: &str = "JOIN (
             SELECT du1.device_id, du1.user_id
             FROM device_users du1
             WHERE du1.last_seen_at = (
                 SELECT MAX(du2.last_seen_at) FROM device_users du2
                 WHERE du2.device_id = du1.device_id
             )
         ) top_user ON top_user.device_id = s.device_id
                   AND top_user.user_id   = s.user_id";

fn row_to_subscription(row: &rusqlite::Row<'_>) -> rusqlite::Result<SubscriptionRow> {
    Ok(SubscriptionRow {
        id: row.get(0)?,
        device_id: row.get(1)?,
        user_id: row.get(2)?,
        endpoint: row.get(3)?,
        p256dh_b64url: row.get(4)?,
        auth_b64url: row.get(5)?,
        last_used_at: row.get(6)?,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;

    /// Create a fully-migrated in-memory DB with two users, two devices, and
    /// the necessary `device_users` rows for tests.
    ///
    /// Users: alice (id=1), bob (id=2).
    /// Devices: laptop (id=1, guessed_slug="laptop"), phone (id=2, guessed_slug="phone").
    /// device_users: (laptop, alice), (phone, alice), (laptop, bob).
    fn setup() -> crate::db::Db {
        let db = init_db_memory();
        {
            let conn = db.blocking_lock();
            let now = crate::db::format_ts_for_db(Utc::now());
            conn.execute_batch(&format!(
                "
                INSERT INTO users (id, username, password_hash, created_at)
                    VALUES (1, 'alice', 'hash', '{now}');
                INSERT INTO users (id, username, password_hash, created_at)
                    VALUES (2, 'bob', 'hash', '{now}');
                INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                    VALUES (1, 'tok1', 'laptop', '{now}', '{now}');
                INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                    VALUES (2, 'tok2', 'phone', '{now}', '{now}');
                INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                    VALUES (1, 1, '{now}', '{now}');
                INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                    VALUES (2, 1, '{now}', '{now}');
                INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                    VALUES (1, 2, '{now}', '{now}');
                ",
            ))
            .expect("test setup");
        }
        db
    }

    /// Shorthand for `ValidatedEndpoint::for_testing(url)` in tests.
    fn ep(url: &str) -> ValidatedEndpoint {
        ValidatedEndpoint::for_testing(url)
    }

    #[test]
    fn upsert_subscription_inserts_then_replaces_on_conflict() {
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );
        let row = get_subscription_for_device_user(&conn, 1, 1).expect("row should exist");
        assert_eq!(row.endpoint, "https://ep1.example.com");
        assert_eq!(row.p256dh_b64url, "p256dh1");

        // Re-register with new keys — should replace.
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );
        let row2 = get_subscription_for_device_user(&conn, 1, 1).expect("row should still exist");
        assert_eq!(row2.endpoint, "https://ep2.example.com");
        assert_eq!(row2.p256dh_b64url, "p256dh2");
        assert_eq!(row2.auth_b64url, "auth2");
    }

    #[test]
    fn delete_subscription_removes_row() {
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");
        assert!(subscription_exists(&conn, 1, 1));
        delete_subscription(&conn, 1, 1);
        assert!(!subscription_exists(&conn, 1, 1));
    }

    #[test]
    fn delete_subscription_is_idempotent() {
        let db = setup();
        let conn = db.blocking_lock();
        // No row — should not panic.
        delete_subscription(&conn, 1, 1);
    }

    #[test]
    fn list_subscriptions_for_user_returns_all_user_devices() {
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );
        upsert_subscription(
            &conn,
            2,
            1,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );
        // Different user — should NOT appear.
        upsert_subscription(
            &conn,
            1,
            2,
            &ep("https://ep3.example.com"),
            "p256dh3",
            "auth3",
        );

        let rows = list_subscriptions_for_user(&conn, 1);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.user_id == 1));
    }

    #[test]
    fn current_user_check_skips_stale_user() {
        let db = setup();
        let conn = db.blocking_lock();
        // Device 1 has both alice (id=1) and bob (id=2).
        // Make bob the more-recently-seen user on device 1.
        //
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("update last_seen_at");

        // Subscribe alice on device 1 (now stale — bob is current on that device).
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );

        // current-user filter should return zero rows for alice on device 1.
        let rows = list_subscriptions_current_user_only(&conn, 1);
        assert!(rows.is_empty(), "stale subscription should be filtered out");

        // Subscribe bob on device 1 (current).
        upsert_subscription(
            &conn,
            1,
            2,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );
        let rows2 = list_subscriptions_current_user_only(&conn, 2);
        assert_eq!(rows2.len(), 1, "current user subscription should appear");
        assert_eq!(rows2[0].user_id, 2);
    }

    #[test]
    fn ensure_pwa_channel_creates_row_and_is_idempotent() {
        let db = setup();
        let conn = db.blocking_lock();
        let addr = PwaPushAddress::User {
            user: "alice".to_owned(),
        };
        let addr_str = addr.to_canonical_string();
        let uuid1 = ensure_pwa_channel(&conn, &addr);
        let uuid2 = ensure_pwa_channel(&conn, &addr);
        // Idempotent: same UUID returned.
        assert_eq!(uuid1, uuid2);

        // Row must exist in messaging_channels.
        let found: Vec<u8> = conn
            .query_row(
                "SELECT uuid FROM messaging_channels WHERE address = ?1",
                rusqlite::params![addr_str],
                |row| row.get(0),
            )
            .expect("row should exist");
        assert_eq!(Uuid::from_slice(&found).unwrap(), uuid1);
    }

    #[test]
    fn ensure_pwa_channel_different_addresses_get_different_uuids() {
        let db = setup();
        let conn = db.blocking_lock();
        let user_addr = PwaPushAddress::User {
            user: "alice".to_owned(),
        };
        let device_addr = PwaPushAddress::Device {
            user: "alice".to_owned(),
            device: "laptop".to_owned(),
        };
        let u1 = ensure_pwa_channel(&conn, &user_addr);
        let u2 = ensure_pwa_channel(&conn, &device_addr);
        assert_ne!(u1, u2);
    }

    // Note: the previous `ensure_pwa_channel_panics_on_unparseable_address` test
    // is no longer applicable — the signature now takes `&PwaPushAddress`, so
    // passing an invalid address string is a compile-time error, not a panic.

    #[test]
    fn subscribe_does_not_create_messaging_channels_rows() {
        // Confirm that upsert_subscription does NOT touch messaging_channels.
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_channels WHERE address LIKE 'pwa_push:%'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(
            count, 0,
            "subscribe must not pre-create messaging_channels rows"
        );
    }

    #[test]
    fn delete_subscription_by_id_removes_correct_row() {
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );
        upsert_subscription(
            &conn,
            2,
            1,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );

        let row1 = get_subscription_for_device_user(&conn, 1, 1).expect("row 1 should exist");
        let row2 = get_subscription_for_device_user(&conn, 2, 1).expect("row 2 should exist");

        // Delete only row1 by id.
        delete_subscription_by_id(&conn, row1.id);

        assert!(
            get_subscription_for_device_user(&conn, 1, 1).is_none(),
            "deleted row must be gone"
        );
        assert!(
            get_subscription_for_device_user(&conn, 2, 1).is_some(),
            "other row must remain"
        );
        // Deleting by the same id again must be a no-op (idempotent).
        delete_subscription_by_id(&conn, row1.id);
        // row2 must still be there.
        assert_eq!(
            get_subscription_for_device_user(&conn, 2, 1).unwrap().id,
            row2.id
        );
    }

    #[test]
    fn touch_subscription_updates_last_used_at() {
        let db = setup();
        let conn = db.blocking_lock();
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");
        let row = get_subscription_for_device_user(&conn, 1, 1).expect("row should exist");
        let original_ts = row.last_used_at.clone();

        // Advance time by inserting a known future timestamp via touch.
        // We can't control the clock, but we can call touch and verify the
        // timestamp changed (touch uses Utc::now() which will be >= the
        // original timestamp from the upsert a few microseconds earlier).
        touch_subscription(&conn, row.id);

        let updated = get_subscription_for_device_user(&conn, 1, 1).expect("row should exist");
        assert!(
            updated.last_used_at >= original_ts,
            "last_used_at must be >= the original timestamp after touch"
        );
    }

    #[test]
    fn list_all_subscriptions_with_device_info_filters_stale_user() {
        let db = setup();
        let conn = db.blocking_lock();
        // Device 1 has both alice (id=1) and bob (id=2).
        // Make bob the more-recently-seen user on device 1.
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("update last_seen_at");

        // Subscribe both alice and bob on device 1.
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );
        upsert_subscription(
            &conn,
            1,
            2,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );

        // list_all_subscriptions_with_device_info applies the current-user
        // check — only bob (the current user on device 1) must appear.
        let rows = list_all_subscriptions_with_device_info(&conn);
        assert_eq!(rows.len(), 1, "only current-user subscription must appear");
        assert_eq!(rows[0].0.user_id, 2, "bob is the current user on device 1");
    }

    #[test]
    fn get_subscription_by_username_and_slug_applies_current_user_check() {
        let db = setup();
        let conn = db.blocking_lock();
        // Device 1 (guessed_slug="laptop") has both alice (id=1) and bob (id=2).
        // Make bob the more-recently-seen user on device 1.
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("update last_seen_at");

        // Subscribe alice on device 1 — alice is now stale on that device.
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep1.example.com"),
            "p256dh1",
            "auth1",
        );

        // get_subscription_by_username_and_slug must return None for alice on
        // laptop because bob is the current user there.
        let result = get_subscription_by_username_and_slug(&conn, "alice", "laptop");
        assert!(
            result.is_none(),
            "stale-user subscription must not be returned"
        );

        // Subscribe bob on device 1 (current).
        upsert_subscription(
            &conn,
            1,
            2,
            &ep("https://ep2.example.com"),
            "p256dh2",
            "auth2",
        );
        let result = get_subscription_by_username_and_slug(&conn, "bob", "laptop");
        assert!(result.is_some(), "current-user subscription must be found");
        assert_eq!(result.unwrap().user_id, 2);
    }

    // ---------------------------------------------------------------------------
    // get_subscription_with_device_last_seen_by_username_and_slug tests
    // ---------------------------------------------------------------------------

    #[test]
    fn get_subscription_with_device_last_seen_returns_sub_and_device_timestamp() {
        let db = setup();
        let conn = db.blocking_lock();
        let ts = "2024-03-01T12:00:00Z";
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 1",
            rusqlite::params![ts],
        )
        .expect("set device last_seen_at");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");

        let result =
            get_subscription_with_device_last_seen_by_username_and_slug(&conn, "alice", "laptop");
        let (sub, device_ts) = result.expect("should find subscription");
        assert_eq!(sub.user_id, 1);
        assert_eq!(sub.device_id, 1);
        assert_eq!(
            device_ts, ts,
            "device_last_seen_at should match devices.last_seen_at"
        );
    }

    #[test]
    fn get_subscription_with_device_last_seen_returns_none_when_absent() {
        let db = setup();
        let conn = db.blocking_lock();
        // No subscription inserted.
        let result =
            get_subscription_with_device_last_seen_by_username_and_slug(&conn, "alice", "laptop");
        assert!(result.is_none());
    }

    #[test]
    fn get_subscription_with_device_last_seen_applies_stale_user_filter() {
        let db = setup();
        let conn = db.blocking_lock();
        // Make bob the most-recently-seen user on device 1 (alice becomes stale).
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("update last_seen_at");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");

        // alice is now stale on laptop — should return None.
        let result =
            get_subscription_with_device_last_seen_by_username_and_slug(&conn, "alice", "laptop");
        assert!(
            result.is_none(),
            "stale-user subscription must not be returned"
        );
    }

    // ---------------------------------------------------------------------------
    // list_subscriptions_with_device_info_for_user tests
    // ---------------------------------------------------------------------------

    #[test]
    fn list_subscriptions_with_device_info_for_user_returns_only_that_user() {
        let db = setup();
        let conn = db.blocking_lock();
        // alice on laptop (device 1) and phone (device 2); bob on laptop (device 1).
        upsert_subscription(&conn, 1, 1, &ep("https://ep1.example.com"), "p1", "a1");
        upsert_subscription(&conn, 2, 1, &ep("https://ep2.example.com"), "p2", "a2");
        upsert_subscription(&conn, 1, 2, &ep("https://ep3.example.com"), "p3", "a3");

        let rows = list_subscriptions_with_device_info_for_user(&conn, "alice");
        assert_eq!(rows.len(), 2, "should return both alice subscriptions");
        assert!(rows.iter().all(|(s, _)| s.user_id == 1));
    }

    #[test]
    fn list_subscriptions_with_device_info_for_user_returns_device_timestamp() {
        let db = setup();
        let conn = db.blocking_lock();
        let ts = "2024-05-01T00:00:00Z";
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 1",
            rusqlite::params![ts],
        )
        .expect("set device last_seen_at");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p", "a");

        let rows = list_subscriptions_with_device_info_for_user(&conn, "alice");
        assert_eq!(rows.len(), 1);
        let (_, device_ts) = &rows[0];
        assert_eq!(device_ts, ts);
    }

    #[test]
    fn list_subscriptions_with_device_info_for_user_applies_stale_user_filter() {
        let db = setup();
        let conn = db.blocking_lock();
        // Make bob the most-recently-seen user on device 1 (alice becomes stale).
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("update last_seen_at");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p", "a");

        // alice@laptop is stale → result should be empty.
        let rows = list_subscriptions_with_device_info_for_user(&conn, "alice");
        assert!(
            rows.is_empty(),
            "stale subscription should be excluded from results"
        );
    }

    // ---------------------------------------------------------------------------
    // lookup_device_subscription tests
    // ---------------------------------------------------------------------------

    #[test]
    fn lookup_device_subscription_returns_current() {
        let db = setup();
        let conn = db.blocking_lock();
        // alice is the only user on laptop → she is the current user.
        upsert_subscription(
            &conn,
            1,
            1,
            &ep("https://ep.example.com"),
            "p256dh1",
            "auth1",
        );
        match lookup_device_subscription(&conn, "alice", "laptop") {
            DeviceSubscriptionLookup::Current(row) => {
                assert_eq!(row.device_id, 1);
                assert_eq!(row.user_id, 1);
                assert_eq!(row.p256dh_b64url, "p256dh1");
                assert_eq!(row.auth_b64url, "auth1");
            }
            other => panic!("expected Current, got {other:?}"),
        }
    }

    #[test]
    fn lookup_device_subscription_returns_stale() {
        let db = setup();
        let conn = db.blocking_lock();
        // Subscribe alice on laptop, then make bob the current user.
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("make bob current on laptop");
        // Cross-validate: get_subscription_by_username_and_slug returns None (current-user
        // filter excludes alice) while lookup_device_subscription returns Stale.
        assert!(
            get_subscription_by_username_and_slug(&conn, "alice", "laptop").is_none(),
            "current-user query must return None when alice is stale"
        );
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "laptop"),
                DeviceSubscriptionLookup::Stale
            ),
            "must return Stale when subscription exists but user is not current"
        );
    }

    #[test]
    fn lookup_device_subscription_returns_not_found_no_subscription() {
        let db = setup();
        let conn = db.blocking_lock();
        // No subscription inserted.
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "laptop"),
                DeviceSubscriptionLookup::NotFound
            ),
            "must return NotFound when no subscription row exists"
        );
    }

    #[test]
    fn lookup_device_subscription_returns_not_found_unknown_user() {
        let db = setup();
        let conn = db.blocking_lock();
        // alice has a subscription on laptop; charlie does not exist in users at all.
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");
        assert!(
            matches!(
                lookup_device_subscription(&conn, "charlie", "laptop"),
                DeviceSubscriptionLookup::NotFound
            ),
            "must return NotFound for a username that has no users row"
        );
    }

    #[test]
    fn lookup_device_subscription_respects_assigned_slug() {
        let db = setup();
        let conn = db.blocking_lock();
        // Set an assigned_slug of "work-laptop" for alice on device 1.
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'work-laptop' WHERE device_id = 1 AND user_id = 1",
            [],
        )
        .expect("set assigned_slug");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");

        // Must find via assigned_slug → Current.
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "work-laptop"),
                DeviceSubscriptionLookup::Current(_)
            ),
            "must find subscription via assigned_slug"
        );
        // Must NOT find via the device's guessed_slug (now overridden by assigned_slug).
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "laptop"),
                DeviceSubscriptionLookup::NotFound
            ),
            "must not find subscription via guessed_slug when assigned_slug is set"
        );
    }

    /// `lookup_device_subscription` must return `Stale` when the subscription
    /// exists, bob is the current user, and the lookup is by assigned_slug.
    ///
    /// Guards against a bug where slug resolution works for Current but breaks
    /// in the stale branch of the LEFT JOIN (e.g. the SLUG_PREDICATE is not
    /// applied before the top-user check).
    #[test]
    fn lookup_device_subscription_stale_with_assigned_slug() {
        let db = setup();
        let conn = db.blocking_lock();
        // Set an assigned_slug of "work-laptop" for alice on device 1.
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'work-laptop' WHERE device_id = 1 AND user_id = 1",
            [],
        )
        .expect("set assigned_slug");
        upsert_subscription(&conn, 1, 1, &ep("https://ep.example.com"), "p256dh", "auth");

        // Make bob the current user on device 1 (later last_seen_at).
        // setup() already has a device_users row for (device 1, bob); just update it.
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?1 WHERE device_id = 1 AND user_id = 2",
            rusqlite::params![later],
        )
        .expect("make bob current on device 1");

        // alice is now stale on device 1: lookup by assigned_slug must return Stale.
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "work-laptop"),
                DeviceSubscriptionLookup::Stale
            ),
            "must return Stale when subscription exists but user is stale (assigned_slug)"
        );
        // Lookup by guessed_slug must return NotFound (overridden by assigned_slug).
        assert!(
            matches!(
                lookup_device_subscription(&conn, "alice", "laptop"),
                DeviceSubscriptionLookup::NotFound
            ),
            "must return NotFound via guessed_slug when assigned_slug is set"
        );
    }

    // ---------------------------------------------------------------------------
    // validate_push_subscribe_fields tests
    // ---------------------------------------------------------------------------

    use crate::pwa_push::endpoint_validator::RejectReason;
    use crate::pwa_push::endpoint_validator::test_helpers::{
        empty_unenforced_policy, enforced_policy,
    };
    use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};

    #[test]
    fn validate_accept_valid_fields_with_unenforced_policy() {
        // Passes with a valid FCM-like endpoint under unenforced policy.
        let result = validate_push_subscribe_fields(
            "https://fcm.googleapis.com/fcm/send/abc",
            &fake_p256dh(),
            &fake_auth(),
            &empty_unenforced_policy(),
        );
        assert!(result.is_ok(), "valid fields must pass: {result:?}");
    }

    #[test]
    fn validate_reject_private_ip_endpoint() {
        let result = validate_push_subscribe_fields(
            "https://10.0.0.5/push",
            &fake_p256dh(),
            &fake_auth(),
            &empty_unenforced_policy(),
        );
        assert_eq!(
            result,
            Err(SubscribeValidationError::Endpoint(
                RejectReason::PrivateHost
            ))
        );
    }

    #[test]
    fn validate_reject_allowlist_miss_when_enforced() {
        let result = validate_push_subscribe_fields(
            "https://evil.example.com/push",
            &fake_p256dh(),
            &fake_auth(),
            &enforced_policy(),
        );
        assert_eq!(
            result,
            Err(SubscribeValidationError::Endpoint(
                RejectReason::AllowlistMiss
            ))
        );
    }

    #[test]
    fn validate_accept_allowlisted_host_when_enforced() {
        let result = validate_push_subscribe_fields(
            "https://fcm.googleapis.com/fcm/send/abc",
            &fake_p256dh(),
            &fake_auth(),
            &enforced_policy(),
        );
        assert!(
            result.is_ok(),
            "FCM host must pass enforced policy: {result:?}"
        );
    }

    #[test]
    fn validate_reject_bad_p256dh_length() {
        let short = base64ct::Base64UrlUnpadded::encode_string(&[0u8; 32]);
        let result = validate_push_subscribe_fields(
            "https://fcm.googleapis.com/fcm/send/abc",
            &short,
            &fake_auth(),
            &empty_unenforced_policy(),
        );
        assert!(
            matches!(result, Err(SubscribeValidationError::P256dhWrongLength(32))),
            "short p256dh must fail: {result:?}"
        );
    }

    #[test]
    fn validate_reject_bad_auth_length() {
        let short = base64ct::Base64UrlUnpadded::encode_string(&[0u8; 8]);
        let result = validate_push_subscribe_fields(
            "https://fcm.googleapis.com/fcm/send/abc",
            &fake_p256dh(),
            &short,
            &empty_unenforced_policy(),
        );
        assert!(
            matches!(result, Err(SubscribeValidationError::AuthWrongLength(8))),
            "short auth must fail: {result:?}"
        );
    }
}
