//! Query-side `PwaPushService` methods: `list_targets`, `get_target`, the
//! `public_key_b64url` / `endpoint_policy` accessors, and the
//! `effective_last_seen` timestamp helper.
//!
//! These are the read/lookup concerns of the service. Each is implemented here
//! as an inherent method; the `impl PwaPushSender for PwaPushService` block in
//! `mod.rs` delegates to them (a Rust requirement: all methods of one trait
//! impl must live in a single `impl` block, so the trait methods stay in
//! `mod.rs` and forward to these inherent bodies).

use crate::pwa_push::db::list_subscriptions_with_device_info_for_user;
use crate::pwa_push::endpoint_validator::EndpointPolicy;
use crate::pwa_push::targets::PwaPushAddress;

use super::{GetTargetResult, PushTargetEntry, PwaPushService};

/// Compute `max(device_last_seen_at, sub_last_used_at)` as an owned `String`.
///
/// Both values are ISO 8601 timestamps stored as strings; lexicographic
/// comparison is equivalent to chronological comparison for this format.
/// Used wherever a `PushTargetEntry.last_seen_at` is computed.
fn effective_last_seen(device_last_seen_at: &str, sub_last_used_at: &str) -> String {
    if device_last_seen_at > sub_last_used_at {
        device_last_seen_at.to_owned()
    } else {
        sub_last_used_at.to_owned()
    }
}

impl PwaPushService {
    /// List push targets visible to the given app slug, filtered by the app's
    /// `user_has_access` predicate and `pwa_push_enabled` gate.
    ///
    /// Returns one `pwa_push:<u>` fan-out entry per visible user plus one
    /// `pwa_push:<u>@<d>` per visible subscription (per design §2.7.5).
    /// Subscriptions whose user is not the current-user on their device are
    /// excluded (mirrors the §2.7.3 step 7 filter so the LLM never addresses
    /// a knowably-stale target).
    pub(in crate::pwa_push) async fn list_targets_impl(
        &self,
        app_slug: &str,
    ) -> Vec<PushTargetEntry> {
        let app_config = match self.apps.get(app_slug) {
            Some(a) => a,
            None => return vec![],
        };
        if !app_config.pwa_push_enabled() {
            return vec![];
        }

        let rows: Vec<(
            crate::pwa_push::db::SubscriptionRow,
            String,
            Option<String>,
            String,
            String,
        )> = {
            let conn = self.db.lock().await;
            crate::pwa_push::db::list_all_subscriptions_with_device_info(&conn)
        };

        // Filter by ACL and build entries.
        // Track per-user fan-out last_seen_at as max across devices.
        let mut user_max_last_seen: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut device_entries: Vec<PushTargetEntry> = Vec::new();

        use crate::pwa_push::targets::{canonical_device_address, canonical_user_address};

        for (sub, username, assigned_slug, guessed_slug, device_last_seen_at) in &rows {
            if !app_config.user_has_access(username) {
                continue;
            }
            let slug = assigned_slug.as_deref().unwrap_or(guessed_slug.as_str());
            let last_seen = effective_last_seen(device_last_seen_at, &sub.last_used_at);
            device_entries.push(PushTargetEntry {
                address: canonical_device_address(username, slug),
                user: username.clone(),
                device: Some(slug.to_string()),
                last_seen_at: last_seen.clone(),
            });
            // Track user-level max.
            let entry = user_max_last_seen
                .entry(username.clone())
                .or_insert_with(|| last_seen.clone());
            if &last_seen > entry {
                *entry = last_seen;
            }
        }

        // Build fan-out (pwa_push:<u>) entries — one per unique visible user.
        let mut result: Vec<PushTargetEntry> = user_max_last_seen
            .into_iter()
            .map(|(user, last_seen_at)| PushTargetEntry {
                address: canonical_user_address(&user),
                user: user.clone(),
                device: None,
                last_seen_at,
            })
            .collect();
        // Sort fan-out entries by user for deterministic output.
        result.sort_by(|a, b| a.user.cmp(&b.user));
        result.extend(device_entries);
        result
    }

    /// Look up a single push target by parsed address without an O(N) full scan.
    ///
    /// - For `pwa_push:<u>@<d>` (Device): queries one row — O(1).
    /// - For `pwa_push:<u>` (User fan-out): queries only that user's subscriptions
    ///   — O(subscriptions for this user), not O(all subscriptions server-wide).
    ///
    /// Returns a discriminated [`GetTargetResult`] distinguishing `Found`,
    /// `NotFound`, `Forbidden`, and `Disabled`.
    pub(in crate::pwa_push) async fn get_target_impl(
        &self,
        app_slug: &str,
        parsed_addr: &PwaPushAddress,
    ) -> GetTargetResult {
        use crate::pwa_push::db::get_subscription_with_device_last_seen_by_username_and_slug;
        use crate::pwa_push::targets::{canonical_device_address, canonical_user_address};

        let Some(app_config) = self.apps.get(app_slug) else {
            // This warn fires on the assumption that app_slug is server-supplied
            // via ActiveBridge (not LLM-controlled). If that invariant changes,
            // demote to debug or add rate-limiting to prevent a log-flood /
            // fail2ban-noise primitive.
            tracing::warn!(
                app_slug,
                "get_target: unknown app slug — server config/routing bug"
            );
            return GetTargetResult::NotFound;
        };
        if !app_config.pwa_push_enabled() {
            return GetTargetResult::Disabled;
        }

        match parsed_addr {
            PwaPushAddress::Device { user, device } => {
                if !app_config.user_has_access(user) {
                    return GetTargetResult::Forbidden;
                }
                let Some((sub, device_last_seen_at)) = ({
                    let conn = self.db.lock().await;
                    get_subscription_with_device_last_seen_by_username_and_slug(&conn, user, device)
                }) else {
                    return GetTargetResult::NotFound;
                };
                let last_seen_at = effective_last_seen(&device_last_seen_at, &sub.last_used_at);
                GetTargetResult::Found(PushTargetEntry {
                    address: canonical_device_address(user, device),
                    user: user.clone(),
                    device: Some(device.clone()),
                    last_seen_at,
                })
            }
            PwaPushAddress::User { user } => {
                if !app_config.user_has_access(user) {
                    return GetTargetResult::Forbidden;
                }
                // Query only this user's subscriptions — O(user-subs), not O(all).
                let rows = {
                    let conn = self.db.lock().await;
                    list_subscriptions_with_device_info_for_user(&conn, user)
                };
                // last_seen_at = max across all subscriptions for this user.
                let max_last_seen = rows
                    .iter()
                    .map(|(sub, device_last_seen_at)| {
                        effective_last_seen(device_last_seen_at, &sub.last_used_at)
                    })
                    .max();
                let Some(last_seen_at) = max_last_seen else {
                    return GetTargetResult::NotFound;
                };
                GetTargetResult::Found(PushTargetEntry {
                    address: canonical_user_address(user),
                    user: user.clone(),
                    device: None,
                    last_seen_at,
                })
            }
        }
    }

    /// Return the VAPID public key in base64url form for `PushVapidKeyRequest`.
    pub(in crate::pwa_push) fn public_key_b64url_impl(&self) -> &str {
        &self.config.vapid.public_b64url
    }

    /// Return a reference to the endpoint validation policy.
    pub(in crate::pwa_push) fn endpoint_policy_impl(&self) -> &EndpointPolicy {
        &self.config.endpoint_policy
    }
}
