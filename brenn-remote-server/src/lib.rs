//! The server-side `remote` layer: an authenticated native daemon attached to
//! the bus, resolved at boot and authenticated on upgrade.
//!
//! The second application route on the attachment stack, beside `surface`. It
//! shares the wire contract, the client planes, and the whole server session
//! with the browser route and parts from it in the four places a non-browser
//! attacher has to: a bearer token instead of a session cookie, an authority
//! lowering from `[[remote]]` instead of component bindings, its own session-cap
//! posture, and no deployment coupling to served assets — a daemon has no build
//! id to agree with.
//!
//! Nothing here is rendering-shaped. A remote has no components, no instances,
//! no geometry, and no chrome; it is one principal, `remote:<slug>`, holding
//! exactly the channels an operator wrote.

pub mod profile;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::AttachScope;
use brenn_lib::messaging::remote::{RemoteToken, ResolvedRemote};
use brenn_messaging::Messenger;
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};

use self::profile::RemoteProfile;
use brenn_attach_server::session::sanitize_client_detail;

/// The credential an unknown slug is compared against.
///
/// An all-zeros digest, which under SHA-256 preimage resistance no presentable
/// credential can match. Its job is to keep the unknown-slug path on the same
/// code as the wrong-token path — one comparison, one refusal, one event —
/// rather than short-circuiting out of the handler on the directory lookup.
///
/// Timing parity holds through the comparison: every `RemoteToken` compare is
/// one SHA-256 of the presented credential against two fixed 32-byte values, so
/// the work is identical whether the slug is configured or not, and no length
/// class of an operator's token is distinguishable. The residual is the
/// directory lookup ahead of it — the same class of residual the login path
/// carries in its user lookup ahead of its dummy password verify.
///
/// Matching the dummy is not an authentication path in any case:
/// [`authenticate_remote`]'s success arm requires the slug to have resolved to a
/// runtime, so the dummy's only job is equalizing comparison work.
static UNKNOWN_REMOTE_TOKEN: RemoteToken = RemoteToken::unmatchable();

/// Per-remote runtime bundle, precomputed once at boot so the upgrade path does
/// no re-derivation.
///
/// The surface's twin (`SurfaceRuntime`) and deliberately smaller: a remote has
/// no assets, no bindings document, no self-description family, and no
/// server-authored telemetry. What is left is the authority, the bus handle, and
/// the credential.
pub struct RemoteRuntime {
    /// The `[[remote]]` slug, as the operator wrote it.
    pub slug: String,
    /// The expected bearer token. Private: the type exposes only a constant-time
    /// comparison, and nothing outside this module has a reason to hold one.
    token: RemoteToken,
    /// Resolved access policy, `Arc`-wrapped once for cheap per-session cloning.
    pub policy: Arc<AppPolicy>,
    /// The bus. A `[[remote]]` activates messaging at boot, so every remote has
    /// one by boot invariant.
    pub messenger: Arc<Messenger>,
    /// Server publish-body cap (config `messaging.max_body_bytes`).
    pub max_body_bytes: usize,
    /// The key this remote's sessions register under in the shared attach
    /// registry — `remote:<slug>`, the spelling the delivery path looks up.
    pub registry_key: String,
    /// This remote's `[[remote]]` block lowered to the attachment grain. Behind
    /// an `Arc` because every session of this remote holds it for its life.
    pub profile: Arc<RemoteProfile>,
}

impl RemoteRuntime {
    /// Build the runtime for one resolved `[[remote]]`.
    pub fn build(
        resolved: &ResolvedRemote,
        messenger: Arc<Messenger>,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            slug: resolved.slug.clone(),
            token: resolved.token.clone(),
            policy: Arc::new(resolved.policy.clone()),
            messenger,
            max_body_bytes,
            registry_key: AttachScope::remote(&resolved.slug)
                .registry_key()
                .into_owned(),
            profile: Arc::new(RemoteProfile::build(resolved)),
        }
    }

    /// The store's boot counter, stamped into every cursor this remote's
    /// sessions mint.
    pub fn store_incarnation(&self) -> i64 {
        self.messenger.store_incarnation()
    }
}

/// Build the per-remote runtime map, keyed by slug.
///
/// # Panics
///
/// If any `[[remote]]` is configured without a `Messenger`. A `[[remote]]` block
/// activates the messaging subsystem at boot, so a miss is a broken boot rather
/// than a runtime condition.
pub fn build_remote_runtimes(
    remotes: &[ResolvedRemote],
    messenger: Option<&Arc<Messenger>>,
    max_body_bytes: usize,
) -> HashMap<String, Arc<RemoteRuntime>> {
    if remotes.is_empty() {
        return HashMap::new();
    }
    let messenger = messenger.expect(
        "[[remote]] blocks configured but no Messenger: the any_messaging gate forces messaging \
         on whenever a remote exists",
    );
    remotes
        .iter()
        .map(|resolved| {
            (
                resolved.slug.clone(),
                Arc::new(RemoteRuntime::build(
                    resolved,
                    Arc::clone(messenger),
                    max_body_bytes,
                )),
            )
        })
        .collect()
}

/// The bearer credential on an upgrade request, or `None` when the
/// `Authorization` header is absent or is not a well-formed `Bearer <token>`.
///
/// Both misses answer `None` on purpose: the route's failure posture does not
/// distinguish "no header" from "a header we could not read", because neither
/// tells an operator anything a wrong token would not, and any distinction
/// visible to the caller is one more bit about what the server expects.
fn bearer_credential(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, credential) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let credential = credential.trim_start();
    if credential.is_empty() {
        return None;
    }
    Some(credential)
}

/// Authenticate an upgrade request against the configured `[[remote]]` blocks.
///
/// Every failure — unknown slug, missing header, malformed header, wrong token —
/// answers a byte-identical `401` and logs one `AuthFailure` carrying the client
/// IP, so fail2ban sees uniform signal and a prober learns nothing about which
/// slugs exist. No redirect: daemons do not log in. Unlike the cookie middleware,
/// which ignores a *missing* cookie because browsers wander, a missing header is
/// logged here — no legitimate anonymous traffic ever reaches `/remote/`.
///
/// The comparison runs on every path, against a dummy credential when the slug
/// names no remote ([`UNKNOWN_REMOTE_TOKEN`]), so every refusal walks the same
/// code and answers from the same place, in the same time. What that is worth,
/// and what it leaves, is written on the dummy.
pub fn authenticate_remote(
    remotes: &HashMap<String, Arc<RemoteRuntime>>,
    alert_dispatcher: &AlertDispatcher,
    slug: &str,
    headers: &HeaderMap,
    ip: IpAddr,
) -> Result<Arc<RemoteRuntime>, StatusCode> {
    let runtime = remotes.get(slug).cloned();
    let expected = runtime
        .as_ref()
        .map_or(&UNKNOWN_REMOTE_TOKEN, |runtime| &runtime.token);
    let presented = bearer_credential(headers);
    let matched = expected.verify(presented.unwrap_or_default());

    match (&runtime, presented) {
        (Some(runtime), Some(_)) if matched => return Ok(Arc::clone(runtime)),
        _ => {}
    }

    // Server-side detail only: the response is the same 401 whatever this says.
    let reason = if runtime.is_none() {
        "no such remote"
    } else if presented.is_none() {
        "missing or malformed Authorization: Bearer header"
    } else {
        "bearer token mismatch"
    };
    log_and_alert_security_event(
        alert_dispatcher,
        SecurityEventType::AuthFailure,
        ip,
        &format!("/remote/{}/ws: {reason}", sanitize_client_detail(slug)),
    );
    Err(StatusCode::UNAUTHORIZED)
}
