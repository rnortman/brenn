//! Device identity: persistent per-browser-profile records, guessed slugs,
//! slug validation, and resolve-or-create logic.
//!
//! A "device" is a browser profile, not a physical device. Cookies are the
//! binding; the `devices` table stores one row per distinct `brenn_device=`
//! token ever issued. Membership (which users have authenticated on which
//! device) lives in `device_users`.
//!
//! # Query hygiene: `active_devices` vs `devices`
//!
//! Use the `active_devices` view (`WHERE unenrolled_at IS NULL`) for **all**
//! queries that should exclude unenrolled devices — device listing, slug
//! resolution, and visibility set construction. Use the `devices` base table
//! directly only for operations that legitimately touch unenrolled rows:
//! - `unenroll_device` (the unenroll operation itself)
//! - `assign_guessed_slug_in_tx` (slug namespace is global; unenrolled slugs remain reserved)
//! - `find_device_by_token` (already filters `unenrolled_at IS NULL`)
//! - `load_device` / `load_device_user` (load by PK; callers are already gated by middleware)

use chrono::Utc;
use rand::RngExt;
use rusqlite::Connection;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Row snapshot of a `devices` table entry.
#[derive(Debug, Clone)]
pub struct Device {
    pub id: i64,
    pub token: String,
    pub guessed_slug: String,
    pub platform: Option<String>,
    pub user_agent: Option<String>,
    pub screen_width: Option<u32>,
    pub screen_height: Option<u32>,
}

/// Row snapshot of a `device_users` join entry.
#[derive(Debug, Clone)]
pub struct DeviceUser {
    pub device_id: i64,
    pub user_id: i64,
    pub assigned_slug: Option<String>,
    pub slug_prompted_at: Option<chrono::DateTime<Utc>>,
    /// IANA timezone name override. `None` means use the browser-reported TZ.
    pub tz_override: Option<String>,
    /// Unix epoch seconds (UTC) at which the override expires. `None` means no expiry.
    /// Meaningless when `tz_override` is `None`.
    pub tz_override_expires_at: Option<i64>,
}

impl DeviceUser {
    /// Slug to show this user for this device: their assigned name if set,
    /// else the device's globally-unique guessed slug.
    pub fn display_slug<'a>(&'a self, device: &'a Device) -> &'a str {
        self.assigned_slug
            .as_deref()
            .unwrap_or(&device.guessed_slug)
    }
}

// ---------------------------------------------------------------------------
// Slug validation
// ---------------------------------------------------------------------------

/// Slug format error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlugError {
    TooShort,
    TooLong,
    MustStartWithLetter,
    BadChar(char),
    LeadingOrTrailingDash,
    DoubleDash,
}

impl SlugError {
    /// Machine-readable name for the LLM.
    pub fn name(&self) -> &'static str {
        match self {
            SlugError::TooShort => "too_short",
            SlugError::TooLong => "too_long",
            SlugError::MustStartWithLetter => "must_start_with_letter",
            SlugError::BadChar(_) => "bad_char",
            SlugError::LeadingOrTrailingDash => "leading_or_trailing_dash",
            SlugError::DoubleDash => "double_dash",
        }
    }
}

/// Validate a device slug string.
///
/// Rules:
/// - Length: 1..=32 chars.
/// - Allowed: ASCII lowercase letters, digits, `-`. Must start with a letter.
/// - No leading/trailing/double `-`.
/// - Forbidden: `]`, `[`, ` `, control chars.
///
/// Empty string is NOT validated here — callers treating `""` as a clear
/// sentinel must check for empty before calling this.
pub fn validate_slug(s: &str) -> Result<(), SlugError> {
    if s.is_empty() {
        return Err(SlugError::TooShort);
    }
    if s.len() > 32 {
        return Err(SlugError::TooLong);
    }
    let first = s.chars().next().expect("non-empty");
    if !first.is_ascii_lowercase() {
        return Err(SlugError::MustStartWithLetter);
    }
    for c in s.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
            continue;
        }
        return Err(SlugError::BadChar(c));
    }
    if s.ends_with('-') {
        return Err(SlugError::LeadingOrTrailingDash);
    }
    if s.contains("--") {
        return Err(SlugError::DoubleDash);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Guessed slug
// ---------------------------------------------------------------------------

/// Classify browser and platform from a User-Agent string and optional platform hint.
///
/// Returns `(browser, platform)` where each is a short lowercase identifier
/// ("chrome", "firefox", "edge", "safari", "unknown"; "linux", "mac",
/// "windows", "ios", "android", "unknown"). Use this instead of calling the
/// individual classifiers from outside the crate.
pub fn classify_device_info(
    user_agent: &str,
    platform: Option<&str>,
) -> (&'static str, &'static str) {
    (
        classify_browser(user_agent),
        classify_platform(user_agent, platform),
    )
}

/// Classify browser from User-Agent string.
/// Order: Edge before Chrome (Edge UA contains both).
fn classify_browser(user_agent: &str) -> &'static str {
    if user_agent.contains("Edg/") || user_agent.contains("EdgA/") {
        "edge"
    } else if user_agent.contains("Firefox/") || user_agent.contains("FxiOS/") {
        "firefox"
    } else if user_agent.contains("Chrome/") || user_agent.contains("CriOS/") {
        "chrome"
    } else if user_agent.contains("Safari/") {
        "safari"
    } else {
        "unknown"
    }
}

/// Classify platform from User-Agent string and optional `navigator.platform`.
/// Order: iOS before Mac (iOS UA often contains "Mac").
fn classify_platform(user_agent: &str, platform: Option<&str>) -> &'static str {
    // Platform string from navigator.platform / userAgentData.platform takes
    // precedence when available.
    if let Some(p) = platform {
        let p_lower = p.to_ascii_lowercase();
        if p_lower.contains("iphone") || p_lower.contains("ipad") || p_lower.contains("ipod") {
            return "ios";
        }
        if p_lower.contains("android") {
            return "android";
        }
        if p_lower.contains("linux") {
            return "linux";
        }
        if p_lower.contains("mac") {
            return "mac";
        }
        if p_lower.contains("win") {
            return "windows";
        }
    }
    // Fall back to UA string inspection.
    // iOS before Mac: "iPhone" / "iPad" / "iPod" before generic Mac detection.
    if user_agent.contains("iPhone") || user_agent.contains("iPad") || user_agent.contains("iPod") {
        return "ios";
    }
    if user_agent.contains("Android") {
        return "android";
    }
    if user_agent.contains("Linux") {
        return "linux";
    }
    if user_agent.contains("Macintosh") || user_agent.contains("Mac OS X") {
        return "mac";
    }
    if user_agent.contains("Windows") {
        return "windows";
    }
    "unknown"
}

/// Compute guessed slug base (without dedup suffix) from User-Agent and platform.
fn guess_slug_base(user_agent: &str, platform: Option<&str>) -> String {
    let (browser, plat) = classify_device_info(user_agent, platform);
    format!("{browser}-{plat}")
}

/// Assign a unique guessed slug for a new device inside a transaction.
///
/// Queries existing slugs that match the base or the `base-N` pattern,
/// computes the next free suffix, then returns the chosen slug.
///
/// The global unique index on `devices.guessed_slug` is the race backstop:
/// on `SQLITE_CONSTRAINT_UNIQUE`, the caller retries (bounded, then panics).
fn assign_guessed_slug_in_tx(conn: &Connection, base: &str) -> String {
    // Fetch all existing slugs that are `base` or `base-<digits>`.
    let pattern = format!("{base}-*");
    let mut stmt = conn
        .prepare(
            "SELECT guessed_slug FROM devices \
             WHERE guessed_slug = ?1 OR guessed_slug GLOB ?2",
        )
        .expect("prepare assign_guessed_slug");
    let existing: std::collections::HashSet<String> = stmt
        .query_map(rusqlite::params![base, pattern], |row| row.get(0))
        .expect("query guessed_slugs")
        .map(|r| r.expect("read guessed_slug"))
        .collect();

    if !existing.contains(base) {
        return base.to_string();
    }
    // Find the next free N starting at 2.
    for n in 2u32..=10000 {
        let candidate = format!("{base}-{n}");
        if !existing.contains(&candidate) {
            return candidate;
        }
    }
    panic!("guessed slug dedup exhausted for base {base:?}");
}

// ---------------------------------------------------------------------------
// Token generation
// ---------------------------------------------------------------------------

/// Generate a cryptographically random 64-char hex token (32 bytes).
fn generate_device_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes[..]);
    hex::encode(bytes)
}

// ---------------------------------------------------------------------------
// Unenroll
// ---------------------------------------------------------------------------

/// Outcome of a call to [`unenroll_device`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnenrollOutcome {
    /// This call performed the unenrollment. `unenrolled_at_ms` is the ms-epoch
    /// timestamp written to `devices.unenrolled_at`.
    Unenrolled { unenrolled_at_ms: i64 },
    /// The device was already unenrolled by a prior call. `unenrolled_at_ms` is
    /// the original unenrollment timestamp (preserved; not overwritten).
    AlreadyUnenrolled { unenrolled_at_ms: i64 },
}

/// Sentinel prefix for unenrolled device tokens. The full sentinel is
/// `UNENROLLED_TOKEN_PREFIX` + 64-hex chars (75 chars total), which the
/// cookie-shape validator at `resolve_or_create_device` can never accept
/// (it requires exactly 64 hex chars). Any future token-shape validator
/// must not accept strings starting with this prefix.
pub const UNENROLLED_TOKEN_PREFIX: &str = "UNENROLLED:";

/// Unenroll a device: set `unenrolled_at`, overwrite `token` with an
/// unmatchable sentinel, and delete all push subscriptions for the device.
/// All DB writes happen inside a single transaction and commit atomically.
///
/// # Invariants
///
/// - Both writes (row UPDATE, push-sub DELETE) are in one `unchecked_transaction()`.
///   Any future refactor that splits them into separate transactions breaks
///   the atomicity guarantee and MUST NOT pass review.
/// - The sentinel token (`UNENROLLED_TOKEN_PREFIX` + 64-hex) is 75 chars. The
///   cookie-shape validator at `resolve_or_create_device` accepts only exactly
///   64 hex chars, so no live browser cookie can ever match the sentinel.
///
/// # Panics
///
/// Panics if `device_id` does not exist in `devices` (programming error:
/// the CLI is responsible for picking a valid id via the list verb).
/// Also panics on any transaction or commit error (fail-fast posture).
pub fn unenroll_device(conn: &Connection, device_id: i64, reason: &str) -> UnenrollOutcome {
    // Use IMMEDIATE to acquire the write lock at BEGIN rather than at first write.
    // This prevents a cross-process race: two concurrent CLI processes could both
    // read `unenrolled_at IS NULL` under a DEFERRED snapshot, then both commit an
    // UPDATE overwriting the first unenrollment timestamp. IMMEDIATE forces the
    // second process to block or SQLITE_BUSY rather than producing a silent overwrite.
    // `Transaction::new_unchecked` accepts `&Connection` (same as `unchecked_transaction`
    // but with an explicit behavior argument).
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
        .expect("unenroll_device: begin IMMEDIATE transaction");

    // Step 1: read current unenrolled_at.
    let current: Option<i64> = tx
        .query_row(
            "SELECT unenrolled_at FROM devices WHERE id = ?1",
            rusqlite::params![device_id],
            |row| row.get(0),
        )
        .unwrap_or_else(|e| panic!("unenroll_device: load row for device_id={device_id}: {e}"));

    if let Some(ms) = current {
        // Already unenrolled: commit an empty transaction and return.
        tx.commit()
            .expect("unenroll_device: commit (already-unenrolled no-op)");
        return UnenrollOutcome::AlreadyUnenrolled {
            unenrolled_at_ms: ms,
        };
    }

    // Step 2: atomically set unenrolled_at and overwrite token with sentinel.
    let now_ms = chrono::Utc::now().timestamp_millis();
    // Sentinel: UNENROLLED_TOKEN_PREFIX (11 chars) + 64-hex CSPRNG suffix = 75 chars total.
    // The prefix makes it human-recognizable in DB inspection; the CSPRNG suffix
    // preserves UNIQUE constraint correctness across multiple concurrent unenrolls.
    let sentinel = format!("{}{}", UNENROLLED_TOKEN_PREFIX, generate_device_token());
    let rows_updated = tx
        .execute(
            "UPDATE devices SET unenrolled_at = ?1, token = ?2 WHERE id = ?3",
            rusqlite::params![now_ms, sentinel, device_id],
        )
        .expect("unenroll_device: UPDATE devices");
    // Row existence was verified in step 1 under the same transaction (serialized by the
    // global Db mutex). A 0 here means a concurrent DELETE happened, which is impossible
    // because we never delete device rows.
    assert_eq!(
        rows_updated, 1,
        "unenroll_device: expected 1 row updated, got {rows_updated} (device_id={device_id})"
    );

    // Step 3: delete all push subscriptions for this device.
    crate::pwa_push::db::delete_all_subscriptions_for_device(&tx, device_id);

    // Step 4: commit (fail-fast on any error — partial unenroll states are unacceptable).
    tx.commit().expect("unenroll_device: commit");

    // Step 5: emit structured audit log (outside transaction; log is observability, not
    // durable state — the commit above is the load-bearing step).
    let reason_truncated: String = reason.chars().take(256).collect();
    info!(device_id, unenrolled_at_ms = now_ms, reason = %reason_truncated, "device unenrolled");

    UnenrollOutcome::Unenrolled {
        unenrolled_at_ms: now_ms,
    }
}

// ---------------------------------------------------------------------------
// DB reads
// ---------------------------------------------------------------------------

/// Load a device row by id. Panics if not found (caller must ensure existence).
pub fn load_device(conn: &Connection, id: i64) -> Device {
    conn.query_row(
        "SELECT id, token, guessed_slug, platform, user_agent, screen_width, screen_height \
         FROM devices WHERE id = ?1",
        rusqlite::params![id],
        |row| {
            Ok(Device {
                id: row.get(0)?,
                token: row.get(1)?,
                guessed_slug: row.get(2)?,
                platform: row.get(3)?,
                user_agent: row.get(4)?,
                screen_width: row.get::<_, Option<i64>>(5)?.map(|v| v as u32),
                screen_height: row.get::<_, Option<i64>>(6)?.map(|v| v as u32),
            })
        },
    )
    .unwrap_or_else(|e| panic!("load_device id={id}: {e}"))
}

/// Load a device_users row by (device_id, user_id). Panics if not found.
pub fn load_device_user(conn: &Connection, device_id: i64, user_id: i64) -> DeviceUser {
    conn.query_row(
        "SELECT device_id, user_id, assigned_slug, slug_prompted_at, \
                tz_override, tz_override_expires_at \
         FROM device_users WHERE device_id = ?1 AND user_id = ?2",
        rusqlite::params![device_id, user_id],
        |row| {
            let slug_prompted_at: Option<String> = row.get(3)?;
            Ok(DeviceUser {
                device_id: row.get(0)?,
                user_id: row.get(1)?,
                assigned_slug: row.get(2)?,
                slug_prompted_at: slug_prompted_at.map(|s| {
                    chrono::DateTime::parse_from_rfc3339(&s)
                        .expect("slug_prompted_at stored by this app must be valid RFC-3339")
                        .to_utc()
                }),
                tz_override: row.get(4)?,
                tz_override_expires_at: row.get(5)?,
            })
        },
    )
    .unwrap_or_else(|e| panic!("load_device_user device_id={device_id} user_id={user_id}: {e}"))
}

// ---------------------------------------------------------------------------
// Timezone override resolution
// ---------------------------------------------------------------------------

/// Resolve the effective timezone for a (device, user) pair.
///
/// Priority:
/// 1. No override (`du.tz_override` is `None`) → `browser_tz`.
/// 2. Override present, not expired → the override zone.
/// 3. Override present, expired (`now >= exp`) → `browser_tz` (lazily ignored; columns not cleared).
///
/// Parse failure on `tz_override` is a panic (better dead than wrong: only the validated tool
/// writes this column; a malformed value is corruption, not user input).
pub fn effective_timezone(
    du: &DeviceUser,
    browser_tz: chrono_tz::Tz,
    now: chrono::DateTime<Utc>,
) -> chrono_tz::Tz {
    let Some(ref tz_str) = du.tz_override else {
        return browser_tz;
    };
    let override_tz: chrono_tz::Tz = tz_str
        .parse()
        .unwrap_or_else(|_| panic!("corrupt tz_override in device_users: {:?}", tz_str));
    // Check expiry.
    if let Some(exp) = du.tz_override_expires_at
        && now.timestamp() >= exp
    {
        return browser_tz;
    }
    override_tz
}

/// Write a timezone override for the given `(device_id, user_id)` row.
///
/// `tz_override`: IANA zone string to set, or `None` to clear.
/// `tz_override_expires_at`: optional Unix epoch seconds (UTC) when the override expires.
///
/// The caller is responsible for validating the timezone string before calling this.
/// Must be called inside the `bridge.db.lock()` scope (caller holds the lock).
pub fn set_tz_override(
    conn: &Connection,
    device_id: i64,
    user_id: i64,
    tz_override: Option<&str>,
    tz_override_expires_at: Option<i64>,
) {
    let rows = conn
        .execute(
            "UPDATE device_users SET tz_override = ?1, tz_override_expires_at = ?2 \
             WHERE device_id = ?3 AND user_id = ?4",
            rusqlite::params![tz_override, tz_override_expires_at, device_id, user_id],
        )
        .expect("set_tz_override UPDATE");
    assert_eq!(
        rows, 1,
        "set_tz_override: expected 1 row updated, got {rows} \
         (device_id={device_id}, user_id={user_id}) — device_users row missing"
    );
}

// ---------------------------------------------------------------------------
// Resolve-or-create
// ---------------------------------------------------------------------------

/// Look up device by token. Returns `Some(id)` if found AND the device is enrolled.
///
/// The `unenrolled_at IS NULL` predicate is the auth-time gate: a cookie matching an
/// unenrolled device resolves to `None`, which falls through to the "unknown cookie" path
/// in `resolve_or_create_device`. This is defense-in-depth alongside the sentinel-token
/// overwrite performed at unenroll time (§4.1 of the device-unenroll design).
fn find_device_by_token(conn: &Connection, token: &str) -> Option<i64> {
    match conn.query_row(
        "SELECT id FROM devices WHERE token = ?1 AND unenrolled_at IS NULL",
        rusqlite::params![token],
        |row| row.get(0),
    ) {
        Ok(id) => Some(id),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("find_device_by_token: {e}"),
    }
}

/// Upsert `device_users` membership for `(device_id, user_id)`.
/// Inserts if absent (sets `first_seen_at`), updates `last_seen_at` if present.
/// Returns `true` if a new row was inserted (i.e. first time this device-user pair is seen).
///
/// Implementation note: uses INSERT OR IGNORE + separate UPDATE rather than
/// INSERT ... ON CONFLICT DO UPDATE, because `changes()` returns 1 for both
/// branches of an upsert, making it impossible to distinguish insert from update.
/// With INSERT OR IGNORE, `changes()` returns 1 only when a row was actually inserted.
fn upsert_device_user(conn: &Connection, device_id: i64, user_id: i64) -> bool {
    let now = crate::db::format_ts_for_db(Utc::now());
    conn.execute(
        "INSERT OR IGNORE INTO device_users (device_id, user_id, first_seen_at, last_seen_at) \
         VALUES (?1, ?2, ?3, ?3)",
        rusqlite::params![device_id, user_id, now],
    )
    .expect("upsert_device_user insert");
    let is_new = conn.changes() == 1;
    if !is_new {
        conn.execute(
            "UPDATE device_users SET last_seen_at = ?3 WHERE device_id = ?1 AND user_id = ?2",
            rusqlite::params![device_id, user_id, now],
        )
        .expect("upsert_device_user update");
    }
    is_new
}

/// Update `devices.last_seen_at`.
fn touch_device(conn: &Connection, device_id: i64) {
    let now = crate::db::format_ts_for_db(Utc::now());
    conn.execute(
        "UPDATE devices SET last_seen_at = ?1 WHERE id = ?2",
        rusqlite::params![now, device_id],
    )
    .expect("touch_device");
}

/// Create a new device row. Returns `(device_id, token, guessed_slug)`.
///
/// Retries on unique-index collisions on `guessed_slug` (bounded at 10).
fn create_device(
    conn: &Connection,
    user_agent: &str,
    platform: Option<&str>,
) -> (i64, String, String) {
    let token = generate_device_token();
    let base = guess_slug_base(user_agent, platform);
    let now = crate::db::format_ts_for_db(Utc::now());

    const MAX_RETRIES: u32 = 10;
    for attempt in 0..MAX_RETRIES {
        // Compute slug inside the retry loop so each attempt re-queries.
        let slug = assign_guessed_slug_in_tx(conn, &base);
        match conn.execute(
            "INSERT INTO devices (token, guessed_slug, user_agent, last_seen_at, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?4)",
            rusqlite::params![token, slug, user_agent, now],
        ) {
            Ok(_) => {
                let id = conn.last_insert_rowid();
                info!(device_id = id, guessed_slug = %slug, "device row created");
                return (id, token, slug);
            }
            Err(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) => {
                warn!(attempt, base = %base, "guessed_slug collision on insert, retrying");
                continue;
            }
            Err(e) => panic!("create_device insert: {e}"),
        }
    }
    panic!("create_device: slug dedup failed after {MAX_RETRIES} attempts for base {base:?}");
}

/// Result of `resolve_or_create_device`.
pub struct ResolvedDevice {
    pub id: i64,
    /// `Some(token)` when a new device was created and the cookie must be set.
    /// `None` when an existing device was reused.
    pub new_token: Option<String>,
}

/// Resolve an existing device by cookie token, or create a new one.
///
/// - Cookie present and DB row exists → reuse; upsert `device_users` membership.
/// - Cookie absent, malformed, or DB row missing → create new device; insert
///   `device_users` membership.
///
/// `ua_from_http_header` is the `User-Agent` from the HTTP upgrade request.
/// `platform_from_header` is not available at HTTP time; the browser reports
/// it via `SetDeviceInfo` after connect.
///
/// # Race note
/// Two simultaneous cookieless requests can each create a device row. The
/// second write gets a `-2` suffix via dedup. The browser keeps whichever
/// `Set-Cookie` arrives last; the orphaned row stays in the DB. This is
/// pathological and documented, not defended against.
pub fn resolve_or_create_device(
    conn: &Connection,
    maybe_token: Option<&str>,
    user_id: i64,
    ua_from_http_header: &str,
) -> ResolvedDevice {
    // Validate token: must be 64 hex chars.
    let valid_token = maybe_token.and_then(|t| {
        if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
            Some(t)
        } else {
            // Log only a prefix — the full cookie value is not a valid secret
            // but echoing arbitrary user-supplied bytes verbatim is unnecessary.
            let preview = &t[..t.len().min(8)];
            info!(device_token_prefix = %preview, token_len = t.len(), "ignoring malformed device cookie");
            None
        }
    });

    if let Some(token) = valid_token {
        if let Some(device_id) = find_device_by_token(conn, token) {
            // Existing device: upsert membership + touch.
            let is_new_member = upsert_device_user(conn, device_id, user_id);
            touch_device(conn, device_id);
            if is_new_member {
                info!(device_id, user_id, "device_users membership inserted");
            }
            return ResolvedDevice {
                id: device_id,
                new_token: None,
            };
        }
        // Token present but row missing (manual DB cleanup etc.).
        // Log only the first 8 hex chars (enough for log correlation) to avoid
        // writing the full bearer credential into structured logs.
        info!(device_token_prefix = %&token[..8], "device cookie present but row missing; issuing new device");
    }

    // Create new device.
    let (device_id, new_token, _guessed_slug) = create_device(conn, ua_from_http_header, None);
    upsert_device_user(conn, device_id, user_id);
    info!(device_id, user_id, "device_users membership inserted");
    ResolvedDevice {
        id: device_id,
        new_token: Some(new_token),
    }
}

// ---------------------------------------------------------------------------
// SetDeviceInfo: update row columns from browser report
// ---------------------------------------------------------------------------

/// Update device info columns from the browser's `SetDeviceInfo` message.
///
/// Empty strings are treated as "no update" — the previously stored value
/// is retained. Dimensions outside `1..=100000` are dropped with a warning.
pub fn update_device_info(
    conn: &Connection,
    device_id: i64,
    user_agent: &str,
    platform: &str,
    screen_width: u32,
    screen_height: u32,
) {
    let now = crate::db::format_ts_for_db(Utc::now());

    // Validate dimensions.
    let valid_width = if (1..=100000).contains(&screen_width) {
        Some(screen_width as i64)
    } else {
        warn!(
            device_id,
            screen_width, "SetDeviceInfo: screen_width out of range, dropping"
        );
        None
    };
    let valid_height = if (1..=100000).contains(&screen_height) {
        Some(screen_height as i64)
    } else {
        warn!(
            device_id,
            screen_height, "SetDeviceInfo: screen_height out of range, dropping"
        );
        None
    };

    // Build the UPDATE conditionally so we don't overwrite with empty values.
    // SQLite doesn't have a convenient "update only if non-null arg" function;
    // we use CASE expressions to preserve existing values when the input is empty.
    conn.execute(
        "UPDATE devices SET
            user_agent   = CASE WHEN ?1 != '' THEN ?1 ELSE user_agent   END,
            platform     = CASE WHEN ?2 != '' THEN ?2 ELSE platform     END,
            screen_width  = CASE WHEN ?3 IS NOT NULL THEN ?3 ELSE screen_width  END,
            screen_height = CASE WHEN ?4 IS NOT NULL THEN ?4 ELSE screen_height END,
            last_seen_at = ?5
         WHERE id = ?6",
        rusqlite::params![
            user_agent,
            platform,
            valid_width,
            valid_height,
            now,
            device_id
        ],
    )
    .expect("update_device_info");
}

// ---------------------------------------------------------------------------
// slug_prompted_at update
// ---------------------------------------------------------------------------

/// Update `device_users.slug_prompted_at` to now for `(device_id, user_id)`.
pub fn touch_slug_prompted_at(conn: &Connection, device_id: i64, user_id: i64) {
    let now = crate::db::format_ts_for_db(Utc::now());
    conn.execute(
        "UPDATE device_users SET slug_prompted_at = ?1 WHERE device_id = ?2 AND user_id = ?3",
        rusqlite::params![now, device_id, user_id],
    )
    .expect("touch_slug_prompted_at");
}

// ---------------------------------------------------------------------------
// DeviceAssignSlug helpers
// ---------------------------------------------------------------------------

/// Resolve a device identifier (numeric id string or guessed_slug) to a device_id
/// scoped to the bridge user's membership.
///
/// Returns `Ok(device_id)` on success, `Err(resolve_error_json)` on failure
/// (ready to return to the LLM as-is).
pub fn resolve_device_for_assign(
    conn: &Connection,
    device_arg: &str,
    bridge_user_id: i64,
) -> Result<i64, serde_json::Value> {
    // Step 1: try numeric id.
    if let Ok(id) = device_arg.parse::<i64>() {
        if id <= 0 {
            return Err(serde_json::json!({
                "error": "ambiguous_or_not_found",
                "hint": "DeviceAssignSlug requires numeric id or guessed_slug; use DeviceGet to find one"
            }));
        }
        // Membership check for the bridge user; also gates on active_devices so
        // the membership count is zero for an unenrolled device.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM device_users \
                 WHERE device_id = ?1 AND user_id = ?2 \
                 AND EXISTS (SELECT 1 FROM active_devices ad WHERE ad.id = ?1)",
                rusqlite::params![id, bridge_user_id],
                |row| row.get(0),
            )
            .expect("resolve_device_for_assign membership check");
        if count == 0 {
            // Check existence and enrollment status in one query to differentiate
            // not_found (no such device or unenrolled) from no_membership (enrolled but
            // requesting user has no membership).
            let (total, active): (i64, i64) = conn
                .query_row(
                    "SELECT COUNT(*), \
                            COUNT(CASE WHEN unenrolled_at IS NULL THEN 1 END) \
                       FROM devices WHERE id = ?1",
                    rusqlite::params![id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("resolve_device existence+enrollment check");
            if total == 0 || active == 0 {
                return Err(serde_json::json!({"error": "not_found"}));
            }
            return Err(serde_json::json!({"error": "no_membership"}));
        }
        return Ok(id);
    }

    // Step 2: try guessed_slug (globally unique). Use active_devices so an
    // unenrolled device is treated as not found.
    let guessed_match: Option<i64> = match conn.query_row(
        "SELECT id FROM active_devices WHERE guessed_slug = ?1",
        rusqlite::params![device_arg],
        |row| row.get(0),
    ) {
        Ok(id) => Some(id),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => panic!("resolve_device_for_assign guessed_slug lookup: {e}"),
    };

    if let Some(id) = guessed_match {
        // Reject if device_arg is already another user's assigned_slug for any
        // device in this user's visibility set — silent mis-attribution otherwise.
        let collision: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM device_users \
                 WHERE assigned_slug = ?1 AND user_id = ?2",
                rusqlite::params![device_arg, bridge_user_id],
                |row| row.get(0),
            )
            .expect("resolve_device_for_assign assigned_slug collision check");
        if collision > 0 {
            return Err(serde_json::json!({
                "error": "ambiguous_or_not_found",
                "hint": "DeviceAssignSlug requires numeric id or guessed_slug; use DeviceGet to find one"
            }));
        }
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM device_users WHERE device_id = ?1 AND user_id = ?2",
                rusqlite::params![id, bridge_user_id],
                |row| row.get(0),
            )
            .expect("resolve_device_for_assign membership check (guessed)");
        if count == 0 {
            return Err(serde_json::json!({"error": "no_membership"}));
        }
        return Ok(id);
    }

    // Step 3: no match — reject (assigned slugs intentionally not accepted).
    Err(serde_json::json!({
        "error": "ambiguous_or_not_found",
        "hint": "DeviceAssignSlug requires numeric id or guessed_slug; use DeviceGet to find one"
    }))
}

/// Perform `DeviceAssignSlug` mutation.
///
/// Returns JSON to return to the LLM.
pub fn assign_device_slug(
    conn: &Connection,
    device_id: i64,
    slug: &str,
    bridge_user_id: i64,
) -> serde_json::Value {
    if slug.is_empty() {
        // Clear assigned_slug.
        let rows = conn
            .execute(
                "UPDATE device_users SET assigned_slug = NULL WHERE device_id = ?1 AND user_id = ?2",
                rusqlite::params![device_id, bridge_user_id],
            )
            .expect("assign_device_slug clear");
        // rows_affected == 0 means the device_users row was deleted between
        // resolve_device_for_assign and this call — should be impossible under
        // the caller's DB lock, but catch it defensively.
        assert_eq!(
            rows, 1,
            "assign_device_slug clear: expected 1 row updated, got {rows} (device_id={device_id}, user_id={bridge_user_id})"
        );
        info!(device_id, user_id = bridge_user_id, "assigned_slug cleared");
        return serde_json::json!({
            "ok": true,
            "device_id": device_id,
            "assigned_slug": null
        });
    }

    // Validate format.
    if let Err(e) = validate_slug(slug) {
        info!(
            device_id,
            user_id = bridge_user_id,
            slug,
            error = e.name(),
            "DeviceAssignSlug: invalid slug format"
        );
        return serde_json::json!({
            "error": "invalid_slug",
            "reason": e.name()
        });
    }

    // Attempt UPDATE.
    match conn.execute(
        "UPDATE device_users SET assigned_slug = ?1 WHERE device_id = ?2 AND user_id = ?3",
        rusqlite::params![slug, device_id, bridge_user_id],
    ) {
        Ok(rows) => {
            assert_eq!(
                rows, 1,
                "assign_device_slug: expected 1 row updated, got {rows} (device_id={device_id}, user_id={bridge_user_id})"
            );
            info!(
                device_id,
                user_id = bridge_user_id,
                slug,
                "assigned_slug set"
            );
            serde_json::json!({
                "ok": true,
                "device_id": device_id,
                "assigned_slug": slug
            })
        }
        Err(e) if e.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) => {
            // Collision: find the conflicting device.
            info!(
                device_id,
                user_id = bridge_user_id,
                slug,
                "DeviceAssignSlug: slug collision"
            );
            let (conflict_device_id, conflict_guessed_slug): (i64, String) = conn
                .query_row(
                    "SELECT du.device_id, d.guessed_slug \
                     FROM device_users du \
                     JOIN devices d ON d.id = du.device_id \
                     WHERE du.user_id = ?1 AND du.assigned_slug = ?2",
                    rusqlite::params![bridge_user_id, slug],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("assign_device_slug collision lookup");
            serde_json::json!({
                "error": "slug_collision",
                "existing_slug": slug,
                "conflicting_device_id": conflict_device_id,
                "conflicting_device_guessed_slug": conflict_guessed_slug
            })
        }
        Err(e) => panic!("assign_device_slug UPDATE: {e}"),
    }
}

// ---------------------------------------------------------------------------
// DeviceList / DeviceGet helpers
// ---------------------------------------------------------------------------

/// A device record as returned by DeviceList/DeviceGet tools.
///
/// `browser` and `platform` are derived from the raw user-agent via
/// `classify_browser`/`classify_platform` and are safe to include in LLM
/// context — the raw UA (attacker-controlled) is never exposed.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeviceRecord {
    pub id: i64,
    pub guessed_slug: String,
    pub assigned_slugs: Vec<AssignedSlugEntry>,
    /// Classified browser name (e.g. "chrome", "firefox"). Never the raw UA.
    pub browser: &'static str,
    /// Classified platform name (e.g. "linux", "ios"). Never the raw UA.
    pub platform: &'static str,
    pub screen_width: Option<u32>,
    pub screen_height: Option<u32>,
    pub last_seen_at: String,
    pub created_at: String,
}

/// Per-user assigned slug entry within a device record.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AssignedSlugEntry {
    pub user_id: i64,
    pub username: String,
    pub slug: Option<String>,
}

/// Raw SQL-fetched device row, intermediate to `DeviceRecord` construction.
/// Lives at module scope so adjacent helpers (e.g. a future `fetch_single_device_record`)
/// can reuse it without re-declaring the shape.
struct RawDevice {
    id: i64,
    guessed_slug: String,
    raw_platform: Option<String>,
    raw_user_agent: Option<String>,
    screen_width: Option<u32>,
    screen_height: Option<u32>,
    last_seen_at: String,
    created_at: String,
}

/// Fetch device records for a list of device ids, enriched with per-user
/// assigned_slugs for users in `visibility_user_ids`.
///
/// # Performance
///
/// Issues exactly 2 queries total regardless of the number of device_ids:
/// one `SELECT … WHERE id IN (?,…)` for device rows and one
/// `SELECT … WHERE du.device_id IN (?,…)` for assigned slugs.
pub fn fetch_device_records(
    conn: &Connection,
    device_ids: &[i64],
    visibility_user_ids: &[i64],
) -> Vec<DeviceRecord> {
    if device_ids.is_empty() {
        return vec![];
    }

    // Build device-id placeholders/params once; reused for both queries below.
    let (dev_placeholders, dev_params) = build_in_params(1, device_ids);

    // Query 1: batch-fetch all device rows.
    // Uses `active_devices` (not `devices`) so an unenrolled device id in the
    // input list produces no row, triggering the panic below — the correct
    // fail-fast behavior since upstream queries (list_device_ids_for_visibility_set,
    // resolve_device_ids_for_get) must have already excluded unenrolled ids.
    let device_query = format!(
        "SELECT id, guessed_slug, platform, user_agent, screen_width, screen_height, \
         last_seen_at, created_at \
         FROM active_devices WHERE id IN ({dev_placeholders})"
    );
    let mut dev_stmt = conn
        .prepare(&device_query)
        .expect("prepare fetch_device_records devices");
    let dev_param_refs = params_as_refs(&dev_params);
    // Collect raw device rows keyed by id; order is restored by the final `device_ids.iter()` pass below.
    let raw_devices: std::collections::HashMap<i64, RawDevice> = dev_stmt
        .query_map(dev_param_refs.as_slice(), |row| {
            Ok(RawDevice {
                id: row.get(0)?,
                guessed_slug: row.get(1)?,
                raw_platform: row.get::<_, Option<String>>(2)?,
                raw_user_agent: row.get::<_, Option<String>>(3)?,
                screen_width: row.get::<_, Option<i64>>(4)?.map(|v| v as u32),
                screen_height: row.get::<_, Option<i64>>(5)?.map(|v| v as u32),
                last_seen_at: row.get(6)?,
                created_at: row.get(7)?,
            })
        })
        .expect("query devices batch")
        .map(|r| {
            let d = r.expect("read device row");
            (d.id, d)
        })
        .collect();

    // Query 2: batch-fetch all assigned_slugs for visible users across all device_ids.
    // Parameters: device_ids first (?1..?N, reusing dev_placeholders), then visibility_user_ids (?N+1..?M).
    let mut assigned_slugs_by_device: std::collections::HashMap<i64, Vec<AssignedSlugEntry>> =
        std::collections::HashMap::new();
    if !visibility_user_ids.is_empty() {
        let vis_base = device_ids.len() + 1;
        let (vis_in, vis_params) = build_in_params(vis_base, visibility_user_ids);
        let mut slug_params = dev_params;
        slug_params.extend(vis_params);
        let slug_query = format!(
            "SELECT du.device_id, du.user_id, u.username, du.assigned_slug \
             FROM device_users du \
             JOIN users u ON u.id = du.user_id \
             WHERE du.device_id IN ({dev_placeholders}) AND du.user_id IN ({vis_in}) \
             ORDER BY du.device_id, du.user_id"
        );
        let mut slug_stmt = conn
            .prepare(&slug_query)
            .expect("prepare fetch_device_records slugs");
        let slug_param_refs = params_as_refs(&slug_params);
        slug_stmt
            .query_map(slug_param_refs.as_slice(), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    AssignedSlugEntry {
                        user_id: row.get(1)?,
                        username: row.get(2)?,
                        slug: row.get(3)?,
                    },
                ))
            })
            .expect("query assigned_slugs batch")
            .for_each(|r| {
                let (device_id, entry) = r.expect("read assigned_slug row");
                assigned_slugs_by_device
                    .entry(device_id)
                    .or_default()
                    .push(entry);
            });
    }

    // Assemble results in caller-supplied order, preserving ordering guarantees.
    // Panic on missing device row: if a device_id was passed to this function but has no
    // corresponding row in the DB, that is an invariant violation (caller sourced ids from the
    // same DB under the same connection/lock; a missing row means schema corruption or a TOCTOU
    // bug in the caller). Fail fast rather than silently return fewer records.
    device_ids
        .iter()
        .map(|&id| {
            let d = raw_devices.get(&id).unwrap_or_else(|| {
                panic!("device_id {id} returned by upstream query but missing from devices table")
            });
            let ua_str = d.raw_user_agent.as_deref().unwrap_or("");
            let (browser, platform) = classify_device_info(ua_str, d.raw_platform.as_deref());
            let assigned_slugs = assigned_slugs_by_device
                .get(&id)
                .cloned()
                .unwrap_or_default();
            DeviceRecord {
                id,
                guessed_slug: d.guessed_slug.clone(),
                assigned_slugs,
                browser,
                platform,
                screen_width: d.screen_width,
                screen_height: d.screen_height,
                last_seen_at: d.last_seen_at.clone(),
                created_at: d.created_at.clone(),
            }
        })
        .collect()
}

/// Build a parameterized IN-clause for a slice of `i64` ids.
///
/// `base` is the 1-based parameter index of the first id placeholder.
/// Returns `(placeholders_csv, params_vec)` where `placeholders_csv` is
/// `"?base, ?base+1, ..."` and `params_vec` contains each id boxed as `ToSql`.
///
/// Use [`params_as_refs`] to convert the returned vec for `query_map`/`query_row`.
///
/// Exposed `pub(crate)` so sibling modules (`event_queue`, `conversation`, `messaging`)
/// can share the same idiom instead of hand-rolling their own placeholder builders.
pub(crate) fn build_in_params(
    base: usize,
    ids: &[i64],
) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let placeholders = (base..base + ids.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let params: Vec<Box<dyn rusqlite::types::ToSql>> = ids
        .iter()
        .map(|&id| -> Box<dyn rusqlite::types::ToSql> { Box::new(id) })
        .collect();
    (placeholders, params)
}

/// Convert a `Vec<Box<dyn ToSql>>` to a `Vec<&dyn ToSql>` for use with
/// `Statement::query_map` / `query_row`, which require `&[&dyn ToSql]`.
///
/// Companion to [`build_in_params`]; call after appending any prefix params
/// to the vec so the entire parameter list is converted in one step.
///
/// Exposed `pub(crate)` so sibling modules can share the same pattern.
pub(crate) fn params_as_refs(
    params: &[Box<dyn rusqlite::types::ToSql>],
) -> Vec<&dyn rusqlite::types::ToSql> {
    params.iter().map(|b| b.as_ref()).collect()
}

/// List devices in the app visibility set, capped at `limit + 1` (caller
/// trims to `limit` and sets `truncated = true` when 11 come back).
///
/// Returns device ids ordered by `last_seen_at DESC`.
pub fn list_device_ids_for_visibility_set(
    conn: &Connection,
    visibility_user_ids: &[i64],
    limit_plus_one: usize,
) -> Vec<i64> {
    if visibility_user_ids.is_empty() {
        return vec![];
    }
    let (vis_placeholders, mut params) = build_in_params(1, visibility_user_ids);
    let limit_idx = visibility_user_ids.len() + 1;
    let query = format!(
        "SELECT DISTINCT d.id \
         FROM active_devices d \
         WHERE EXISTS (SELECT 1 FROM device_users du \
                        WHERE du.device_id = d.id AND du.user_id IN ({vis_placeholders}) ) \
         ORDER BY d.last_seen_at DESC \
         LIMIT ?{limit_idx}"
    );
    let mut stmt = conn.prepare(&query).expect("prepare list_device_ids");
    params.push(Box::new(limit_plus_one as i64));
    let param_refs = params_as_refs(&params);
    stmt.query_map(param_refs.as_slice(), |row| row.get(0))
        .expect("query list_device_ids")
        .map(|r| r.expect("read device_id"))
        .collect()
}

/// Resolve device ids for `DeviceGet` string lookup across the visibility set.
///
/// Resolution rules:
/// 1. Parses as positive integer → match `devices.id`; membership-scoped.
/// 2. Otherwise → union of `devices.guessed_slug = ?` (membership-scoped) and
///    `device_users.assigned_slug = ? AND user_id IN (visibility_set)`.
/// 3. Returns empty vec on no match.
pub fn resolve_device_ids_for_get(
    conn: &Connection,
    device_arg: &str,
    visibility_user_ids: &[i64],
) -> Vec<i64> {
    if visibility_user_ids.is_empty() {
        return vec![];
    }

    if let Ok(id) = device_arg.parse::<i64>() {
        if id <= 0 {
            return vec![];
        }
        // By-id branch: params are [uid[0], .., uid[N-1], device_id].
        // user_id IN (?1..?N), device_id = ?N+1.
        let (by_id_vis_list, mut params) = build_in_params(1, visibility_user_ids);
        let device_id_idx = visibility_user_ids.len() + 1;
        // Membership-scoped by-id lookup. Also gates on active_devices so an
        // unenrolled device is rejected even if its device_users rows persist.
        let count: i64 = {
            let query = format!(
                "SELECT COUNT(*) FROM device_users \
                 WHERE device_id = ?{device_id_idx} \
                 AND user_id IN ({by_id_vis_list}) \
                 AND EXISTS (SELECT 1 FROM active_devices ad WHERE ad.id = ?{device_id_idx})"
            );
            let mut stmt = conn.prepare(&query).expect("prepare device_get by id");
            params.push(Box::new(id));
            let param_refs = params_as_refs(&params);
            stmt.query_row(param_refs.as_slice(), |row| row.get(0))
                .expect("device_get by id count")
        };
        if count > 0 {
            return vec![id];
        }
        return vec![];
    }

    // String match: union guessed_slug match and assigned_slug matches.
    // Both sub-queries use the layout: ?1 = device_arg, ?2..?N+1 = visibility_user_ids.
    // Hoist the placeholder string once; params vecs are separate because each block
    // owns its Box<dyn ToSql> vec (ToSql is not Clone).
    let vis_list = {
        let (placeholders, _) = build_in_params(2, visibility_user_ids);
        placeholders
    };
    let mut ids: std::collections::HashSet<i64> = std::collections::HashSet::new();

    // Guessed slug (globally unique, but membership-scope the result).
    {
        let query = format!(
            "SELECT d.id FROM active_devices d \
             WHERE d.guessed_slug = ?1 \
             AND EXISTS (SELECT 1 FROM device_users du \
                          WHERE du.device_id = d.id AND du.user_id IN ({vis_list}))"
        );
        let mut stmt = conn.prepare(&query).expect("prepare guessed_slug lookup");
        let (_, vis_params) = build_in_params(2, visibility_user_ids);
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(device_arg.to_string())];
        params.extend(vis_params);
        let param_refs = params_as_refs(&params);
        if let Ok(id) = stmt.query_row(param_refs.as_slice(), |row| row.get::<_, i64>(0)) {
            ids.insert(id);
        }
    }

    // Assigned slug matches across the visibility set.
    {
        // Same param layout as guessed-slug block: ?1 = slug, ?2..?N+1 = visibility_user_ids.
        // Also gates on active_devices so an unenrolled device is excluded
        // even when matched by assigned_slug.
        let query = format!(
            "SELECT DISTINCT du.device_id FROM device_users du \
             WHERE du.assigned_slug = ?1 AND du.user_id IN ({vis_list}) \
             AND EXISTS (SELECT 1 FROM active_devices ad WHERE ad.id = du.device_id)"
        );
        let mut stmt = conn.prepare(&query).expect("prepare assigned_slug lookup");
        let (_, vis_params) = build_in_params(2, visibility_user_ids);
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(device_arg.to_string())];
        params.extend(vis_params);
        let param_refs = params_as_refs(&params);
        let matched: Vec<i64> = stmt
            .query_map(param_refs.as_slice(), |row| row.get(0))
            .expect("query assigned_slug")
            .map(|r| r.expect("read device_id"))
            .collect();
        for id in matched {
            ids.insert(id);
        }
    }

    let mut result: Vec<i64> = ids.into_iter().collect();
    result.sort();
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::user::create_user;
    use crate::db::init_db_memory;

    fn setup_user(conn: &Connection, username: &str) -> i64 {
        create_user(conn, username, "hash")
    }

    // ── Browser classification ────────────────────────────────────────────────

    #[test]
    fn guess_slug_base_browser_classification() {
        struct Case {
            ua: &'static str,
            expected_browser: &'static str,
        }
        let cases = [
            Case {
                ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
                expected_browser: "edge",
            },
            Case {
                ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                expected_browser: "chrome",
            },
            Case {
                ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0",
                expected_browser: "firefox",
            },
            Case {
                ua: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Version/17.5 Mobile/15E148 Safari/604.1",
                expected_browser: "safari",
            },
            Case {
                ua: "SomeOtherAgent/1.0",
                expected_browser: "unknown",
            },
        ];
        for c in &cases {
            assert_eq!(classify_browser(c.ua), c.expected_browser, "UA: {}", c.ua);
        }
    }

    #[test]
    fn guess_slug_base_platform_classification() {
        struct Case {
            ua: &'static str,
            platform: Option<&'static str>,
            expected_plat: &'static str,
        }
        let cases = [
            Case {
                ua: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Version/17.5 Mobile/15E148 Safari/604.1",
                platform: None,
                expected_plat: "ios",
            },
            // iOS via platform string.
            Case {
                ua: "SomeAgent",
                platform: Some("iPhone"),
                expected_plat: "ios",
            },
            Case {
                ua: "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0.0.0 Mobile Safari/537.36",
                platform: None,
                expected_plat: "android",
            },
            Case {
                ua: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
                platform: None,
                expected_plat: "linux",
            },
            // Mac via UA.
            Case {
                ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
                platform: None,
                expected_plat: "mac",
            },
            // Mac via platform string — but NOT iOS.
            Case {
                ua: "SomeAgent",
                platform: Some("MacIntel"),
                expected_plat: "mac",
            },
            Case {
                ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
                platform: None,
                expected_plat: "windows",
            },
            Case {
                ua: "SomeOtherAgent/1.0",
                platform: None,
                expected_plat: "unknown",
            },
        ];
        for c in &cases {
            assert_eq!(
                classify_platform(c.ua, c.platform),
                c.expected_plat,
                "UA: {} platform: {:?}",
                c.ua,
                c.platform
            );
        }
    }

    // ── Slug validation ───────────────────────────────────────────────────────

    #[test]
    fn validate_slug_acceptance_cases() {
        assert!(validate_slug("laptop").is_ok());
        assert!(validate_slug("a").is_ok());
        assert!(validate_slug("my-phone").is_ok());
        assert!(validate_slug("a1").is_ok());
        // Max length 32.
        assert!(validate_slug("a".repeat(32).as_str()).is_ok());
    }

    #[test]
    fn validate_slug_rejection_cases() {
        // Empty — too short.
        assert_eq!(validate_slug(""), Err(SlugError::TooShort));
        // Too long.
        assert_eq!(validate_slug(&"a".repeat(33)), Err(SlugError::TooLong));
        // Must start with letter.
        assert_eq!(validate_slug("1phone"), Err(SlugError::MustStartWithLetter));
        assert_eq!(validate_slug("-phone"), Err(SlugError::MustStartWithLetter));
        // Must start with lowercase letter; uppercase first char → MustStartWithLetter.
        assert_eq!(validate_slug("Phone"), Err(SlugError::MustStartWithLetter));
        assert_eq!(validate_slug("my phone"), Err(SlugError::BadChar(' ')));
        assert_eq!(validate_slug("my[phone"), Err(SlugError::BadChar('[')));
        assert_eq!(validate_slug("my]phone"), Err(SlugError::BadChar(']')));
        assert!(matches!(
            validate_slug("my\x01phone"),
            Err(SlugError::BadChar(_))
        ));
        // Trailing dash.
        assert_eq!(
            validate_slug("phone-"),
            Err(SlugError::LeadingOrTrailingDash)
        );
        // Double dash.
        assert_eq!(validate_slug("my--phone"), Err(SlugError::DoubleDash));
    }

    // ── resolve_or_create_device ─────────────────────────────────────────────

    #[test]
    fn resolve_or_create_device_first_time_creation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let resolved = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        assert!(resolved.new_token.is_some());
        let device_id = resolved.id;

        // Device row exists.
        let device = load_device(&conn, device_id);
        assert!(!device.guessed_slug.is_empty());
        assert!(device.guessed_slug.starts_with("chrome"));

        // device_users membership exists.
        let du = load_device_user(&conn, device_id, user_id);
        assert_eq!(du.device_id, device_id);
        assert_eq!(du.user_id, user_id);
        assert!(du.assigned_slug.is_none());
    }

    #[test]
    fn resolve_or_create_device_cookie_match_reuses_row() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r1 = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let token = r1.new_token.unwrap();
        let device_id = r1.id;

        let r2 = resolve_or_create_device(&conn, Some(&token), user_id, "Mozilla/5.0 Chrome/125");
        assert!(r2.new_token.is_none());
        assert_eq!(r2.id, device_id);
    }

    #[test]
    fn resolve_or_create_device_new_user_upserts_membership() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let bob = setup_user(&conn, "bob");

        let r1 = resolve_or_create_device(&conn, None, alice, "Mozilla/5.0 Chrome/125");
        let token = r1.new_token.unwrap();
        let device_id = r1.id;

        // Bob uses the same cookie (same browser, different user).
        let r2 = resolve_or_create_device(&conn, Some(&token), bob, "Mozilla/5.0 Chrome/125");
        assert!(r2.new_token.is_none()); // No new device created.
        assert_eq!(r2.id, device_id);

        // Both memberships exist.
        let _du_alice = load_device_user(&conn, device_id, alice);
        let _du_bob = load_device_user(&conn, device_id, bob);
    }

    #[test]
    fn resolve_or_create_device_malformed_token_treated_as_absent() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        // Malformed (not 64 hex chars).
        let r = resolve_or_create_device(
            &conn,
            Some("not-a-valid-token"),
            user_id,
            "Mozilla/5.0 Chrome/125",
        );
        assert!(r.new_token.is_some()); // New device created.
    }

    // ── dedup_guessed_slug ───────────────────────────────────────────────────

    #[test]
    fn dedup_guessed_slug_second_device_gets_suffix() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let bob = setup_user(&conn, "bob");

        // Both use Chrome on Linux.
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        let r1 = resolve_or_create_device(&conn, None, alice, ua);
        let r2 = resolve_or_create_device(&conn, None, bob, ua);

        let d1 = load_device(&conn, r1.id);
        let d2 = load_device(&conn, r2.id);

        assert_eq!(d1.guessed_slug, "chrome-linux");
        assert_eq!(d2.guessed_slug, "chrome-linux-2");
    }

    #[test]
    fn upsert_device_user_first_call_returns_true_second_returns_false() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");
        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        // Direct access to upsert_device_user — second call for same pair must return false.
        let is_new_first = upsert_device_user(&conn, device_id, user_id);
        assert!(
            !is_new_first,
            "second upsert for existing membership must return false (row already created by resolve_or_create_device)"
        );

        // A genuinely new pairing (new user) must return true.
        let bob = setup_user(&conn, "bob");
        let is_new_bob = upsert_device_user(&conn, device_id, bob);
        assert!(
            is_new_bob,
            "first upsert for new device-user pair must return true"
        );

        // And the repeat call for bob must also return false.
        let is_new_bob_again = upsert_device_user(&conn, device_id, bob);
        assert!(
            !is_new_bob_again,
            "second upsert for existing membership (bob) must return false"
        );
    }

    #[test]
    fn build_in_params_base_1_produces_correct_placeholders() {
        let (csv, params) = build_in_params(1, &[10_i64, 20_i64]);
        assert_eq!(csv, "?1, ?2");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_in_params_base_2_produces_correct_placeholders() {
        let (csv, params) = build_in_params(2, &[5_i64]);
        assert_eq!(csv, "?2");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn build_in_params_base_3_multiple_ids() {
        let (csv, params) = build_in_params(3, &[1_i64, 2_i64, 3_i64]);
        assert_eq!(csv, "?3, ?4, ?5");
        assert_eq!(params.len(), 3);
    }

    // ── resolve_device_for_assign — guessed_slug + collision ────────────────

    /// Positive case: resolve by guessed_slug succeeds when the caller is
    /// a member and there is no assigned_slug collision.
    #[test]
    fn device_assign_resolve_happy_path_by_guessed_slug() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        let r = resolve_or_create_device(&conn, None, alice, ua);
        let device_id = r.id;
        let device = load_device(&conn, device_id);
        let guessed = device.guessed_slug.clone();

        let result = resolve_device_for_assign(&conn, &guessed, alice);
        assert_eq!(
            result,
            Ok(device_id),
            "guessed_slug should resolve to device_id for a member: {result:?}"
        );
    }

    /// Collision rejection: if `device_arg` is already another device's
    /// `assigned_slug` for this user, the guessed_slug match must be rejected.
    #[test]
    fn device_assign_resolve_rejects_guessed_slug_when_collision_with_assigned_slug() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let ua_chrome =
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        let ua_firefox = "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0";

        // Two devices for alice.
        let r1 = resolve_or_create_device(&conn, None, alice, ua_chrome);
        let r2 = resolve_or_create_device(&conn, None, alice, ua_firefox);
        let _d1 = load_device(&conn, r1.id);
        let d2 = load_device(&conn, r2.id);

        // Assign d2's guessed_slug as d1's assigned_slug.
        // This makes d2.guessed_slug a collision target for d1.
        assign_device_slug(&conn, r1.id, &d2.guessed_slug, alice);

        // Now try to resolve using d2's guessed_slug as device_arg.
        // d2 matches on guessed_slug, but alice already has that string as an
        // assigned_slug on d1 — collision must be rejected.
        let result = resolve_device_for_assign(&conn, &d2.guessed_slug, alice);
        assert!(
            result.is_err(),
            "guessed_slug collision with an assigned_slug must be rejected; d1={}, d2={}, \
             guessed={:?}, result={result:?}",
            r1.id,
            r2.id,
            d2.guessed_slug,
        );
        let err = result.unwrap_err();
        assert_eq!(
            err["error"], "ambiguous_or_not_found",
            "error must be ambiguous_or_not_found: {err}"
        );
    }

    #[test]
    fn fetch_device_record_classifies_browser_and_platform() {
        // Verify that fetch_device_records correctly classifies raw UA into the
        // DeviceRecord.browser and DeviceRecord.platform fields (not raw strings).
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        let resolved = resolve_or_create_device(&conn, None, user_id, ua);
        let device_id = resolved.id;

        let visibility = vec![user_id];
        let records = fetch_device_records(&conn, &[device_id], &visibility);
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(
            record.browser, "chrome",
            "browser must be classified from UA"
        );
        assert_eq!(
            record.platform, "linux",
            "platform must be classified from UA"
        );
    }

    #[test]
    fn fetch_device_records_empty_returns_empty() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");
        let visibility = vec![user_id];
        // Empty device_ids → early return, no queries fired.
        let records = fetch_device_records(&conn, &[], &visibility);
        assert!(records.is_empty(), "empty device_ids must return empty vec");
    }

    #[test]
    fn fetch_device_records_multiple_devices_correct_per_device_data() {
        // Two devices with different UAs — results must be in caller-supplied order
        // with correct browser/platform per device.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let ua_chrome =
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        let ua_firefox = "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0";

        let dev1 = resolve_or_create_device(&conn, None, user_id, ua_chrome).id;
        let dev2 = resolve_or_create_device(&conn, None, user_id, ua_firefox).id;

        let visibility = vec![user_id];

        // Request in forward order.
        let records = fetch_device_records(&conn, &[dev1, dev2], &visibility);
        assert_eq!(records.len(), 2, "must return exactly 2 records");
        assert_eq!(records[0].id, dev1);
        assert_eq!(records[0].browser, "chrome");
        assert_eq!(records[1].id, dev2);
        assert_eq!(records[1].browser, "firefox");

        // Request in reverse order — output order must follow caller.
        let records_rev = fetch_device_records(&conn, &[dev2, dev1], &visibility);
        assert_eq!(records_rev[0].id, dev2);
        assert_eq!(records_rev[1].id, dev1);
    }

    #[test]
    fn fetch_device_records_visibility_scopes_assigned_slugs() {
        // A device with memberships for two users — only slugs for users in the
        // visibility set must be returned; out-of-set slugs must not leak.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice_id = setup_user(&conn, "alice");
        let bob_id = setup_user(&conn, "bob");

        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";
        // Create device for alice; bob uses the same cookie to get a membership on the same device.
        let r1 = resolve_or_create_device(&conn, None, alice_id, ua);
        let token = r1.new_token.as_deref().unwrap();
        let device_id = r1.id;
        resolve_or_create_device(&conn, Some(token), bob_id, ua);

        // Assign a slug for each user on this device.
        assign_device_slug(&conn, device_id, "alice-slug", alice_id);
        assign_device_slug(&conn, device_id, "bob-slug", bob_id);

        // Visibility set contains only alice — bob's slug must not appear.
        let records = fetch_device_records(&conn, &[device_id], &[alice_id]);
        assert_eq!(records.len(), 1);
        let slugs = &records[0].assigned_slugs;
        assert!(
            slugs.iter().all(|s| s.user_id == alice_id),
            "only alice's slug should be visible; got: {slugs:?}"
        );
        assert_eq!(slugs.len(), 1, "exactly one slug visible to alice");

        // Visibility set contains both — both slugs must appear.
        let records_both = fetch_device_records(&conn, &[device_id], &[alice_id, bob_id]);
        let slugs_both = &records_both[0].assigned_slugs;
        assert_eq!(
            slugs_both.len(),
            2,
            "both slugs visible when both in visibility set"
        );
    }

    // ── unenroll_device ──────────────────────────────────────────────────────

    #[test]
    fn unenroll_device_sets_timestamp_and_invalidates_token() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");
        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        let before_ms = chrono::Utc::now().timestamp_millis();
        let outcome = unenroll_device(&conn, device_id, "stolen");
        let after_ms = chrono::Utc::now().timestamp_millis();

        let ts = match outcome {
            UnenrollOutcome::Unenrolled { unenrolled_at_ms } => unenrolled_at_ms,
            UnenrollOutcome::AlreadyUnenrolled { .. } => {
                panic!("expected Unenrolled, got AlreadyUnenrolled")
            }
        };
        assert!(
            ts >= before_ms && ts <= after_ms,
            "unenrolled_at_ms {ts} must be in [{before_ms}, {after_ms}]"
        );

        // Verify DB state.
        let (db_ts, token): (i64, String) = conn
            .query_row(
                "SELECT unenrolled_at, token FROM devices WHERE id = ?1",
                rusqlite::params![device_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query device row");
        assert_eq!(db_ts, ts, "DB unenrolled_at must match returned timestamp");
        assert!(
            token.starts_with(UNENROLLED_TOKEN_PREFIX),
            "token must start with sentinel prefix; got: {token}"
        );
        // Sentinel length: 11 (UNENROLLED_TOKEN_PREFIX) + 64 (hex) = 75 chars total.
        assert_eq!(
            token.len(),
            75,
            "sentinel token must be 75 chars; got len={}",
            token.len()
        );
    }

    #[test]
    fn unenroll_device_is_idempotent() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");
        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        let first = unenroll_device(&conn, device_id, "first reason");
        let first_ts = match first {
            UnenrollOutcome::Unenrolled { unenrolled_at_ms } => unenrolled_at_ms,
            _ => panic!("expected Unenrolled on first call"),
        };

        // Capture the sentinel token written by the first unenroll.
        let token_after_first: String = conn
            .query_row(
                "SELECT token FROM devices WHERE id = ?1",
                rusqlite::params![device_id],
                |row| row.get(0),
            )
            .expect("query token after first unenroll");

        let second = unenroll_device(&conn, device_id, "second reason");
        match second {
            UnenrollOutcome::AlreadyUnenrolled { unenrolled_at_ms } => {
                assert_eq!(
                    unenrolled_at_ms, first_ts,
                    "second call must return first unenrollment timestamp unchanged"
                );
            }
            UnenrollOutcome::Unenrolled { .. } => {
                panic!("expected AlreadyUnenrolled on second call")
            }
        }

        // DB state: unenrolled_at unchanged; sentinel token also unchanged (the
        // AlreadyUnenrolled branch must not re-randomize the sentinel).
        let (db_ts, token_after_second): (i64, String) = conn
            .query_row(
                "SELECT unenrolled_at, token FROM devices WHERE id = ?1",
                rusqlite::params![device_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("query unenrolled_at and token");
        assert_eq!(
            db_ts, first_ts,
            "DB unenrolled_at must not change on idempotent re-call"
        );
        assert_eq!(
            token_after_second, token_after_first,
            "sentinel token must not be re-randomized on idempotent re-call"
        );
    }

    #[test]
    fn unenroll_device_deletes_push_subscriptions() {
        use crate::pwa_push::db::{subscription_exists, upsert_subscription};
        use crate::pwa_push::endpoint_validator::ValidatedEndpoint;

        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let bob = setup_user(&conn, "bob");

        let r = resolve_or_create_device(&conn, None, alice, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;
        // Also give bob membership on the same device.
        let token = r.new_token.as_deref().unwrap().to_owned();
        resolve_or_create_device(&conn, Some(&token), bob, "Mozilla/5.0 Chrome/125");

        // Insert two push subscriptions: one for alice on this device, one for bob.
        upsert_subscription(
            &conn,
            device_id,
            alice,
            &ValidatedEndpoint::for_testing("https://push.example.com/alice"),
            "p256dh-alice",
            "auth-alice",
        );
        upsert_subscription(
            &conn,
            device_id,
            bob,
            &ValidatedEndpoint::for_testing("https://push.example.com/bob"),
            "p256dh-bob",
            "auth-bob",
        );
        assert!(subscription_exists(&conn, device_id, alice));
        assert!(subscription_exists(&conn, device_id, bob));

        unenroll_device(&conn, device_id, "test cleanup");

        assert!(
            !subscription_exists(&conn, device_id, alice),
            "alice's push subscription must be deleted on unenroll"
        );
        assert!(
            !subscription_exists(&conn, device_id, bob),
            "bob's push subscription on the device must be deleted on unenroll"
        );
    }

    #[test]
    #[should_panic(expected = "unenroll_device: load row")]
    fn unenroll_device_panics_on_missing_id() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        // Device id 99999 does not exist.
        unenroll_device(&conn, 99999, "should panic");
    }

    #[test]
    fn find_device_by_token_rejects_unenrolled() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let original_token = r.new_token.unwrap();
        let device_id = r.id;

        // Before unenroll: token resolves.
        assert_eq!(
            find_device_by_token(&conn, &original_token),
            Some(device_id),
            "enrolled device must be found by token"
        );

        unenroll_device(&conn, device_id, "test");

        // After unenroll: the original token must not resolve (unenrolled_at IS NULL predicate fails).
        assert_eq!(
            find_device_by_token(&conn, &original_token),
            None,
            "unenrolled device must not be found by its original token"
        );
    }

    #[test]
    fn resolve_or_create_device_issues_new_device_for_unenrolled_cookie() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r1 = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let original_token = r1.new_token.clone().unwrap();
        let original_id = r1.id;

        unenroll_device(&conn, original_id, "test");

        // Present the unenrolled device's original cookie.
        let r2 = resolve_or_create_device(
            &conn,
            Some(&original_token),
            user_id,
            "Mozilla/5.0 Chrome/125",
        );

        // A new device must be issued (original cookie no longer resolves).
        assert!(
            r2.new_token.is_some(),
            "must issue a new token when cookie matches only an unenrolled device"
        );
        assert_ne!(
            r2.id, original_id,
            "new device id must differ from the unenrolled device id"
        );
    }

    #[test]
    fn load_device_still_loads_unenrolled() {
        // unenroll_device must not delete the row; load_device must succeed without panic.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        unenroll_device(&conn, device_id, "test");

        // Must not panic; returns Device row.
        let device = load_device(&conn, device_id);
        assert_eq!(device.id, device_id);
        // The token has been overwritten with the sentinel — load_device returns whatever is stored.
        assert!(
            device.token.starts_with(UNENROLLED_TOKEN_PREFIX),
            "token in returned Device must be the sentinel after unenroll"
        );
    }

    // ── Integration: unenroll + subscription count ───────────────────────────

    /// End-to-end: create user, create device, subscribe push, unenroll,
    /// assert no subscription remains. Complements the unit test
    /// `unenroll_device_deletes_push_subscriptions` which tests two users on
    /// the same device; this follows the design §9 integration-test scenario
    /// wording.
    #[test]
    fn unenroll_disconnects_subscription_count() {
        use crate::pwa_push::db::{subscription_exists, upsert_subscription};
        use crate::pwa_push::endpoint_validator::ValidatedEndpoint;

        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        upsert_subscription(
            &conn,
            device_id,
            user_id,
            &ValidatedEndpoint::for_testing("https://push.example.com/test"),
            "p256dh-test",
            "auth-test",
        );
        assert!(
            subscription_exists(&conn, device_id, user_id),
            "subscription must exist before unenroll"
        );

        unenroll_device(&conn, device_id, "integration test");

        assert!(
            !subscription_exists(&conn, device_id, user_id),
            "subscription must be gone after unenroll"
        );
    }

    // ── Integration: historical attribution after unenroll ───────────────────

    /// Write a `messages` row carrying `sender_device_id` for device A; unenroll A;
    /// read history for the conversation; assert no panic and the message is present
    /// with its `sender_device_id` intact (historical FK chain survives unenroll).
    #[test]
    fn unenrolled_device_historical_attribution_still_resolves() {
        use crate::conversation::{
            MessageDirection, append_message, create_conversation, get_messages,
        };

        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = setup_user(&conn, "alice");

        let r = resolve_or_create_device(&conn, None, user_id, "Mozilla/5.0 Chrome/125");
        let device_id = r.id;

        let conv_id = create_conversation(&conn, user_id, "test", false);

        // Write a user message attributed to device A.
        append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "human",
            None,
            None,
            "hello",
            Some(user_id),
            Some("UTC"),
            Some(device_id),
        );

        // Unenroll the device — row persists, FK chain intact.
        unenroll_device(&conn, device_id, "integration test");

        // Reading history must not panic; the message must appear.
        let msgs = get_messages(&conn, conv_id);
        assert_eq!(msgs.len(), 1, "message must survive device unenroll");
        assert_eq!(msgs[0].payload, "hello", "message payload must be intact");

        // Verify sender_device_id is preserved in the DB row (the FK chain survives).
        let db_device_id: Option<i64> = conn
            .query_row(
                "SELECT sender_device_id FROM messages WHERE conversation_id = ?1 ORDER BY seq LIMIT 1",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("query sender_device_id");
        assert_eq!(
            db_device_id,
            Some(device_id),
            "sender_device_id attribution must be preserved after unenroll"
        );

        // Fetching the device record by id must also succeed without panic:
        // load_device's panic-on-miss contract still holds because the row is never deleted.
        let device = load_device(&conn, device_id);
        assert_eq!(
            device.id, device_id,
            "device row must still be loadable after unenroll"
        );

        // fetch_device_records coverage for unenrolled exclusion is in
        // fetch_device_records_via_active_view.  Calling fetch_device_records with
        // an unenrolled id would panic (by design); calling it with an empty list is
        // a trivial early-return that adds no signal here, so the call is omitted.
    }

    // ── unenroll-cc-bridge-gate: active_devices exclusion ────────────────────

    /// `list_device_ids_for_visibility_set` must not include unenrolled devices.
    #[test]
    fn list_device_ids_excludes_unenrolled() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";

        let r1 = resolve_or_create_device(&conn, None, alice, ua);
        let r2 = resolve_or_create_device(
            &conn,
            None,
            alice,
            "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
        );

        // Both enrolled: both must appear.
        let ids = list_device_ids_for_visibility_set(&conn, &[alice], 100);
        assert!(ids.contains(&r1.id), "enrolled device 1 must be listed");
        assert!(ids.contains(&r2.id), "enrolled device 2 must be listed");

        // Unenroll device 1.
        unenroll_device(&conn, r1.id, "test");

        let ids_after = list_device_ids_for_visibility_set(&conn, &[alice], 100);
        assert!(
            !ids_after.contains(&r1.id),
            "unenrolled device must not appear in list"
        );
        assert!(
            ids_after.contains(&r2.id),
            "still-enrolled device must remain in list"
        );
    }

    /// `resolve_device_ids_for_get` must not resolve unenrolled devices via
    /// any lookup path: by-id, by-guessed-slug, or by-assigned-slug.
    #[test]
    fn resolve_device_ids_for_get_excludes_unenrolled() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";

        let r = resolve_or_create_device(&conn, None, alice, ua);
        let device_id = r.id;
        let device = load_device(&conn, device_id);
        let guessed = device.guessed_slug.clone();

        // Assign a custom slug before unenroll.
        assign_device_slug(&conn, device_id, "my-laptop", alice);

        // All three lookup paths succeed before unenroll.
        let vis = vec![alice];
        assert_eq!(
            resolve_device_ids_for_get(&conn, &device_id.to_string(), &vis),
            vec![device_id],
            "by-id lookup must succeed when enrolled"
        );
        assert_eq!(
            resolve_device_ids_for_get(&conn, &guessed, &vis),
            vec![device_id],
            "by-guessed-slug lookup must succeed when enrolled"
        );
        assert_eq!(
            resolve_device_ids_for_get(&conn, "my-laptop", &vis),
            vec![device_id],
            "by-assigned-slug lookup must succeed when enrolled"
        );

        unenroll_device(&conn, device_id, "test");

        // All three lookup paths must now return empty.
        assert!(
            resolve_device_ids_for_get(&conn, &device_id.to_string(), &vis).is_empty(),
            "by-id lookup must return empty for unenrolled device"
        );
        assert!(
            resolve_device_ids_for_get(&conn, &guessed, &vis).is_empty(),
            "by-guessed-slug lookup must return empty for unenrolled device"
        );
        assert!(
            resolve_device_ids_for_get(&conn, "my-laptop", &vis).is_empty(),
            "by-assigned-slug lookup must return empty for unenrolled device"
        );
    }

    /// `resolve_device_for_assign` must reject unenrolled devices via both
    /// the by-id and by-guessed-slug paths.
    #[test]
    fn resolve_device_for_assign_rejects_unenrolled() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";

        let r = resolve_or_create_device(&conn, None, alice, ua);
        let device_id = r.id;
        let device = load_device(&conn, device_id);
        let guessed = device.guessed_slug.clone();

        // Both paths succeed before unenroll.
        assert_eq!(
            resolve_device_for_assign(&conn, &device_id.to_string(), alice),
            Ok(device_id),
            "by-id must resolve when enrolled"
        );
        assert_eq!(
            resolve_device_for_assign(&conn, &guessed, alice),
            Ok(device_id),
            "by-guessed-slug must resolve when enrolled"
        );

        unenroll_device(&conn, device_id, "test");

        // By-id path must return an error (not_found; enrollment state not leaked).
        let err_id = resolve_device_for_assign(&conn, &device_id.to_string(), alice);
        assert!(
            err_id.is_err(),
            "by-id must fail for unenrolled device; got: {err_id:?}"
        );
        assert_eq!(
            err_id.unwrap_err()["error"],
            "not_found",
            "unenrolled device by id must report not_found"
        );

        // By-guessed-slug path must return an error.
        let err_slug = resolve_device_for_assign(&conn, &guessed, alice);
        assert!(
            err_slug.is_err(),
            "by-guessed-slug must fail for unenrolled device; got: {err_slug:?}"
        );
        assert_eq!(
            err_slug.unwrap_err()["error"],
            "ambiguous_or_not_found",
            "unenrolled device by guessed_slug must report ambiguous_or_not_found"
        );
    }

    /// `list_device_ids_for_visibility_set` → `fetch_device_records` pipeline:
    /// `resolve_device_for_assign` must return `no_membership` for an enrolled device
    /// that the requesting user is not a member of.  This confirms the enrollment-gate
    /// addition (EXISTS active_devices) does not collapse the no_membership branch.
    #[test]
    fn resolve_device_for_assign_enrolled_no_membership() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");
        let bob = setup_user(&conn, "bob");
        let ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36";

        // Create a device owned by alice; bob has no membership.
        let r = resolve_or_create_device(&conn, None, alice, ua);
        let device_id = r.id;

        // Bob tries to resolve the enrolled device by id — must get no_membership.
        let result = resolve_device_for_assign(&conn, &device_id.to_string(), bob);
        assert!(
            result.is_err(),
            "enrolled device with no membership must return error; got: {result:?}"
        );
        assert_eq!(
            result.unwrap_err()["error"],
            "no_membership",
            "enrolled device accessed by non-member must report no_membership"
        );
    }

    /// unenrolled device is absent from both the id list and the records.
    #[test]
    fn fetch_device_records_via_active_view() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let alice = setup_user(&conn, "alice");

        let r1 = resolve_or_create_device(
            &conn,
            None,
            alice,
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
        );
        let r2 = resolve_or_create_device(
            &conn,
            None,
            alice,
            "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
        );

        // Unenroll device 1.
        unenroll_device(&conn, r1.id, "test");

        // The pipeline: list → fetch.
        let vis = vec![alice];
        let ids = list_device_ids_for_visibility_set(&conn, &vis, 100);

        // Unenrolled device must not appear in the id list.
        assert!(
            !ids.contains(&r1.id),
            "unenrolled device must be absent from id list"
        );
        assert!(
            ids.contains(&r2.id),
            "enrolled device must be present in id list"
        );

        // fetch_device_records with the filtered id list must return only the enrolled device.
        let records = fetch_device_records(&conn, &ids, &vis);
        assert_eq!(records.len(), 1, "only one record expected");
        assert_eq!(
            records[0].id, r2.id,
            "record must be for the enrolled device"
        );
    }

    // ── effective_timezone pure-helper matrix ────────────────────────────────

    fn du_no_override() -> DeviceUser {
        DeviceUser {
            device_id: 1,
            user_id: 1,
            assigned_slug: None,
            slug_prompted_at: None,
            tz_override: None,
            tz_override_expires_at: None,
        }
    }

    fn du_with_override(tz: &str, expires_at: Option<i64>) -> DeviceUser {
        DeviceUser {
            device_id: 1,
            user_id: 1,
            assigned_slug: None,
            slug_prompted_at: None,
            tz_override: Some(tz.to_string()),
            tz_override_expires_at: expires_at,
        }
    }

    fn utc_ts(secs: i64) -> chrono::DateTime<Utc> {
        chrono::DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    /// No override → browser TZ returned.
    #[test]
    fn effective_timezone_no_override_returns_browser_tz() {
        let du = du_no_override();
        let browser = chrono_tz::America::New_York;
        let result = effective_timezone(&du, browser, Utc::now());
        assert_eq!(result, browser);
    }

    /// Active override (no expiry) → override TZ returned.
    #[test]
    fn effective_timezone_active_override_no_expiry() {
        let du = du_with_override("Asia/Tokyo", None);
        let browser = chrono_tz::America::New_York;
        let result = effective_timezone(&du, browser, Utc::now());
        assert_eq!(result, chrono_tz::Asia::Tokyo);
    }

    /// Active override with future expiry → override TZ returned.
    #[test]
    fn effective_timezone_active_override_future_expiry() {
        // now = 1000, expires_at = 2000 → not expired → override
        let du = du_with_override("Asia/Tokyo", Some(2000));
        let browser = chrono_tz::America::New_York;
        let now = utc_ts(1000);
        let result = effective_timezone(&du, browser, now);
        assert_eq!(result, chrono_tz::Asia::Tokyo);
    }

    /// Expired override (now >= exp) → browser TZ returned, columns untouched.
    #[test]
    fn effective_timezone_expired_override_returns_browser_tz() {
        // now = 2000, expires_at = 1000 → expired
        let du = du_with_override("Asia/Tokyo", Some(1000));
        let browser = chrono_tz::America::New_York;
        let now = utc_ts(2000);
        let result = effective_timezone(&du, browser, now);
        assert_eq!(result, browser);
        // Columns must not be cleared by the pure helper.
        assert_eq!(du.tz_override.as_deref(), Some("Asia/Tokyo"));
        assert_eq!(du.tz_override_expires_at, Some(1000));
    }

    /// Expiry boundary: now == exp → considered expired → browser TZ.
    #[test]
    fn effective_timezone_expiry_boundary_at_exact_second() {
        let du = du_with_override("Asia/Tokyo", Some(1000));
        let browser = chrono_tz::America::New_York;
        let now = utc_ts(1000); // exactly at boundary
        let result = effective_timezone(&du, browser, now);
        assert_eq!(result, browser);
    }

    /// Override present but expiry is None (never expires) → override TZ.
    #[test]
    fn effective_timezone_override_null_expiry_never_expires() {
        let du = du_with_override("Europe/London", None);
        let browser = chrono_tz::America::New_York;
        let result = effective_timezone(&du, browser, Utc::now());
        assert_eq!(result, chrono_tz::Europe::London);
    }

    /// Past-expires_at but tz_override = None → no override, browser TZ.
    /// (Expiry is meaningless when tz_override is NULL.)
    #[test]
    fn effective_timezone_no_tz_but_past_expiry_returns_browser_tz() {
        let du = DeviceUser {
            device_id: 1,
            user_id: 1,
            assigned_slug: None,
            slug_prompted_at: None,
            tz_override: None,
            tz_override_expires_at: Some(1), // stale expiry, no zone
        };
        let browser = chrono_tz::America::New_York;
        let result = effective_timezone(&du, browser, Utc::now());
        assert_eq!(result, browser);
    }
}
