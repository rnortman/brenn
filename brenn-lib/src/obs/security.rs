use std::fmt;
use std::net::IpAddr;

use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};

use super::alerting::{AlertDispatcher, AlertSeverity};

/// Known security event types for structured logging and fail2ban parsing.
#[derive(Debug, Clone, Copy)]
pub enum SecurityEventType {
    AuthFailure,
    SchemaViolation,
    UnrecognizedUrl,
    /// Emitted by the `detect_rate_limit` middleware in `router.rs` when the global
    /// governor returns a 429. The middleware wraps the governor layer and has access
    /// to `ClientIp` from request extensions, so it can emit a proper security event.
    RateLimitHit,
    MalformedMessage,
    /// A push subscription endpoint was rejected because it resolves to a
    /// private/reserved IP range or was not in the configured allowlist.
    ///
    /// Emitted both at subscribe time (client-provided endpoint) and at
    /// delivery time (defense-in-depth re-validation of stored rows).
    ///
    /// Also dispatched as a phone alert (see `log_and_alert_security_event`).
    /// The existing fail2ban jail matches on `security_event = true` then
    /// `event_type`; this variant is captured without regex changes.
    SsrfAttempt,
    /// A replay-protection endpoint received a request from one `client_id`
    /// with ≥1024 non-expired nonces in its window — indicates abuse (spam-burst
    /// to fill the namespace) rather than honest retry.
    ///
    /// Fails closed: request rejected, new nonce NOT stored.
    /// Source IP banned by fail2ban after `maxretry` hits (same jail as all
    /// other `security_event = true` records — no regex changes required).
    ReplayCapHit,
    /// Envelope `sent_at` was outside the allowed clock-skew window.
    ///
    /// May indicate a misconfigured phone clock or a replay attempt with an
    /// out-of-window timestamp. Logged + alerted; fail2ban acts on repeated hits.
    ReplaySkew,
    /// Envelope nonce was already seen within its window — duplicate submission.
    ///
    /// In normal operation this should not occur (client serialises pushes via
    /// `pushLock`). When it does, it indicates a race, a retry anomaly, or a
    /// replay attack.
    ReplayDuplicate,
    /// Envelope `sent_at` was not strictly greater than the previously accepted
    /// `sent_at` for this key — monotonicity violation.
    ///
    /// In normal operation this should not occur. When it does it indicates a
    /// clock regression, a race (both fixed by `pushLock`), or a replay attack.
    ReplayMonotonicity,
    /// Replay component reported a malformed envelope (unparseable or structurally
    /// invalid, distinct from signature failure).
    ///
    /// Returns HTTP 400. Indicates a client bug or a crafted request.
    ReplayMalformed,
    /// A request violated the forwarded-header trust invariant at the transport
    /// boundary: `trusted_proxy_hops >= 1` is configured but `X-Forwarded-For`
    /// was absent, had fewer tokens than the configured hop count, or the
    /// selected token did not parse as an IP. Either the trusted proxy is
    /// misconfigured, or the request reached Brenn's port bypassing the proxy
    /// (in which case the TCP peer is the attacker). The request is rejected
    /// with HTTP 400 rather than falling back to an untrusted identity.
    ///
    /// The logged `ip` is the TCP peer (the only thing known about a request
    /// that bypassed or out-ran the proxy), which is also the fail2ban ban
    /// target. Logged via plain `log_security_event` (no phone alert) since a
    /// misconfigured proxy could fire this on every request. Captured by the
    /// existing fail2ban jail without regex changes (`security_event = true`).
    ForwardedHeaderInvalid,
    /// A surface WebSocket connection sent a frame a correct shell structurally
    /// cannot produce: unparseable JSON, an unknown frame type, a binary frame, a
    /// frame larger than the derived read cap (no config-legal frame can exceed
    /// it), a `Subscribe` to an unbound channel, a duplicate or class-mismatched
    /// `Subscribe`, a resume seq the server never assigned, an `Unsubscribe` of a
    /// non-active channel, or a `Publish` to an unbound port. Either a shell bug
    /// or tampering; both are reject-and-log per the security posture. The
    /// connection is killed with no response frame, and the source IP is banned by
    /// fail2ban after repeated hits (same `security_event = true` jail — no regex
    /// changes).
    SurfaceProtocolViolation,
    /// A hosted app's LLM `BrennSend` attempted an `ephemeral:` publish that was
    /// denied (bad address shape, unknown channel, ACL, oversized body, or no
    /// sender grant). CC output is attacker-influenceable, so a denial flood is a
    /// namespace/ACL-probing signal. Attributed to the app/conversation, not a
    /// network peer, via `log_app_security_event` — no `ip` field, so fail2ban
    /// never matches these lines; the response channel is the phone alert.
    EphemeralPublishDenied,
    /// A hosted app's LLM `BrennSend` attempted a durable `brenn:` publish that
    /// was denied (bad address shape, unknown channel, ACL, oversized body, no
    /// sender grant, or an out-of-visibility `reply_to`). The durable twin of
    /// `EphemeralPublishDenied`: a distinct variant so the event-type string
    /// stays in lockstep with the ACL name it enforces (`brenn_publish`). CC
    /// output is attacker-influenceable, so a denial flood is a namespace/ACL-
    /// probing signal. Attributed to the app/conversation, not a network peer,
    /// via `log_app_security_event` — no `ip` field, so fail2ban never matches
    /// these lines; the response channel is the phone alert.
    BrennPublishDenied,
    /// An MQTT egress publish was denied by the per-client `mqtt_publish` ACL
    /// (or the `mqtt` grant was absent, on the LLM path). Shared by both egress
    /// callers: the LLM `MqttSend` intercept (attributed to an app/conversation
    /// via `log_app_security_event`) and the WASM egress callback (attributed
    /// to a component via `log_component_security_event`). Both source strings
    /// are attacker-influenceable, so a denial flood is an ACL-probing signal.
    /// No `ip` field, so fail2ban never matches these lines; the response
    /// channel is the phone alert.
    MqttPublishDenied,
    /// A WASM consumer's activation pacer entered a throttle episode: the
    /// per-component activation token bucket emptied under sustained load and
    /// activations are being delayed (never dropped) to the configured sustained
    /// rate (mqtt-wasm-republish-pacing design §4). Signals a self-echo/runaway
    /// loop or an over-active consumer. Attributed to the component via
    /// `log_component_security_event` — no `ip` (the "attacker" is an out-of-tree
    /// guest, not a bannable peer, so fail2ban's failregex never matches), no
    /// `conversation_id` (WASM consumers have none). The response channel is the
    /// phone alert (once per process per slug).
    WasmActivationThrottled,
}

impl fmt::Display for SecurityEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthFailure => write!(f, "auth_failure"),
            Self::SchemaViolation => write!(f, "schema_violation"),
            Self::UnrecognizedUrl => write!(f, "unrecognized_url"),
            Self::RateLimitHit => write!(f, "rate_limit_hit"),
            Self::MalformedMessage => write!(f, "malformed_message"),
            Self::SsrfAttempt => write!(f, "ssrf_attempt"),
            Self::ReplayCapHit => write!(f, "replay_cap_hit"),
            Self::ReplaySkew => write!(f, "replay_skew"),
            Self::ReplayDuplicate => write!(f, "replay_duplicate"),
            Self::ReplayMonotonicity => write!(f, "replay_monotonicity"),
            Self::ReplayMalformed => write!(f, "replay_malformed"),
            Self::ForwardedHeaderInvalid => write!(f, "forwarded_header_invalid"),
            Self::SurfaceProtocolViolation => write!(f, "surface_protocol_violation"),
            Self::EphemeralPublishDenied => write!(f, "ephemeral_publish_denied"),
            Self::BrennPublishDenied => write!(f, "brenn_publish_denied"),
            Self::MqttPublishDenied => write!(f, "mqtt_publish_denied"),
            Self::WasmActivationThrottled => write!(f, "wasm_activation_throttled"),
        }
    }
}

/// Canonical vocabulary for the `kind` field of a publish-denial security
/// signal. These strings are load-bearing identifiers: they key log queries and
/// alert-dedup slots. New denial kinds are added here, never minted as ad-hoc
/// literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenialKind {
    MalformedAddress,
    UnknownChannel,
    MissingSender,
    AclDenied,
    BodyTooLarge,
    /// Grant absent entirely (layer-1), as distinguished from `AclDenied`
    /// (layer-2, grant held). Minted only by the LLM MQTT intercept's
    /// `has_grant` branch; no result enum produces it.
    GrantAbsent,
}

impl DenialKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MalformedAddress => "malformed_address",
            Self::UnknownChannel => "unknown_channel",
            Self::MissingSender => "missing_sender",
            Self::AclDenied => "acl_denied",
            Self::BodyTooLarge => "body_too_large",
            Self::GrantAbsent => "grant_absent",
        }
    }
}

impl fmt::Display for DenialKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Log a security event with structured fields for fail2ban consumption.
///
/// These events are filtered into the security log (JSON format) by the
/// `security_event = true` field. fail2ban matches on that field without
/// regex gymnastics.
///
/// **Field order coupling:** The fail2ban regex in deploy/fail2ban/brenn.conf
/// depends on `security_event` appearing before `ip` in the JSON output.
/// tracing-subscriber's JSON layer emits fields in visit order (the order
/// they appear in the tracing::warn! macro call below). Do not reorder these
/// fields without updating the fail2ban filter regex.
pub fn log_security_event(event_type: SecurityEventType, ip: IpAddr, detail: &str) {
    tracing::warn!(
        security_event = true,
        event_type = %event_type,
        ip = %ip,
        detail,
        "security event"
    );
}

/// Log a security event attributed to a hosted app / conversation rather than a
/// network peer. Emits `security_event = true` (same security-log stream) but NO
/// `ip` field: the "attacker" is our own CC subprocess, so there is nothing for
/// fail2ban to ban — the fail2ban failregex simply never matches these lines
/// (unmatched lines are ignored). The response channel is the phone alert, per
/// the standing "CC anomalies: log and surface, not fail2ban" rule.
///
/// `security_event` stays first (consistent with `log_security_event`'s
/// field-order coupling note); with no `ip` field there is no regex interaction
/// to preserve. The caller passes `detail` already sanitized — this helper
/// cannot know which of its inputs are untrusted.
pub fn log_app_security_event(
    event_type: SecurityEventType,
    app_slug: &str,
    conversation_id: i64,
    detail: &str,
) {
    tracing::warn!(
        security_event = true,
        event_type = %event_type,
        app_slug,
        conversation_id,
        detail,
        "app security event"
    );
}

/// Log a security event attributed to a WASM component rather than a network
/// peer or an LLM app/conversation. The component analog of
/// `log_app_security_event`: same `security_event = true` stream, no `ip` (the
/// "attacker" is an out-of-tree guest, not a bannable peer — fail2ban's
/// failregex never matches these lines), and no `conversation_id` (WASM
/// consumers have none). The response channel is the phone alert. The caller
/// passes `detail` already sanitized.
pub fn log_component_security_event(
    event_type: SecurityEventType,
    component_slug: &str,
    detail: &str,
) {
    tracing::warn!(
        security_event = true,
        event_type = %event_type,
        component_slug,
        detail,
        "component security event"
    );
}

/// Log a security event for fail2ban AND send a Warning-severity phone alert.
///
/// Use for most security events. For SSRF probes specifically, use
/// `log_and_alert_ssrf_attempt` which dispatches at `AlertSeverity::Critical`.
pub fn log_and_alert_security_event(
    dispatcher: &AlertDispatcher,
    event_type: SecurityEventType,
    ip: IpAddr,
    detail: &str,
) {
    log_security_event(event_type, ip, detail);
    let title = format!("Security: {event_type}");
    let body = format!("IP: {ip}\n{detail}");
    dispatcher.alert(AlertSeverity::Warning, title, body);
}

/// Log an SSRF attempt security event for fail2ban AND send a
/// **Critical**-severity phone alert.
///
/// SSRF probes against the push endpoint indicate active exploitation — on-call
/// must be woken regardless of time. Use this instead of
/// `log_and_alert_security_event` for all `SecurityEventType::SsrfAttempt`
/// call sites.
pub fn log_and_alert_ssrf_attempt(dispatcher: &AlertDispatcher, ip: IpAddr, detail: &str) {
    log_security_event(SecurityEventType::SsrfAttempt, ip, detail);
    let title = format!("Security: {}", SecurityEventType::SsrfAttempt);
    let body = format!("IP: {ip}\n{detail}");
    dispatcher.alert(AlertSeverity::Critical, title, body);
}

/// Who a publish denial is attributed to. The component path structurally has
/// no conversation id; the two arms select the log helper, the dedup-key prefix,
/// and the alert-body attribution clause.
pub enum DenialOrigin<'a> {
    App { slug: &'a str, conversation_id: i64 },
    Component { slug: &'a str },
}

/// Emit the canonical publish-denial security signal: one attributed
/// `security_event = true` log line per occurrence (no `ip`; fail2ban ignores
/// these) plus a `Warning` phone alert on the FIRST occurrence per (event-type
/// title, origin, slug, kind) per process. `address` is attacker-influenceable
/// (CC output, guest output, or broker-derived) and is sanitized here before it
/// reaches any log field or alert body.
///
/// The single owner of the denial-signal schema (title, dedup key, detail, body,
/// sanitization): the durable/ephemeral LLM `BrennSend` intercepts, the
/// automation create/edit validators, the LLM `MqttSend` intercept, and the WASM
/// MQTT egress callback all route here so the schema cannot drift. The alert
/// title is derived from `event_type` (`"Security: {event_type}"`, as
/// `log_and_alert_security_event` derives it) so each publish scheme's alerts and
/// dedup slots stay keyed to its own ACL name.
pub fn signal_publish_denial(
    dispatcher: &AlertDispatcher,
    event_type: SecurityEventType,
    origin: DenialOrigin<'_>,
    kind: DenialKind,
    address: &str,
) {
    let safe_addr = sanitize_untrusted_str(address, MAX_LOGGED_UNTRUSTED_BYTES);
    let detail = format!("kind={kind} address={safe_addr}");
    // Schema written once — severity, title, and the single `alert_once_per_process`
    // call live outside the match so the two origins cannot drift apart. The match
    // logs via the origin-appropriate helper and yields only what genuinely differs:
    // the dedup-key prefix and the attribution clause.
    let title = format!("Security: {event_type}");
    let (dedup_key, body) = match origin {
        DenialOrigin::App {
            slug,
            conversation_id,
        } => {
            log_app_security_event(event_type, slug, conversation_id, &detail);
            (
                format!("app:{slug}:{kind}"),
                format!("app {slug} (conversation {conversation_id}) denied publish: {detail}"),
            )
        }
        DenialOrigin::Component { slug } => {
            log_component_security_event(event_type, slug, &detail);
            (
                format!("component:{slug}:{kind}"),
                format!("component {slug} denied publish: {detail}"),
            )
        }
    };
    dispatcher.alert_once_per_process(AlertSeverity::Warning, title, &dedup_key, body);
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_common::TRUNCATION_MARKER;
    use tracing_test::traced_test;

    #[test]
    fn security_event_type_display() {
        assert_eq!(SecurityEventType::AuthFailure.to_string(), "auth_failure");
        assert_eq!(
            SecurityEventType::SchemaViolation.to_string(),
            "schema_violation"
        );
        assert_eq!(
            SecurityEventType::UnrecognizedUrl.to_string(),
            "unrecognized_url"
        );
        assert_eq!(
            SecurityEventType::RateLimitHit.to_string(),
            "rate_limit_hit"
        );
        assert_eq!(
            SecurityEventType::MalformedMessage.to_string(),
            "malformed_message"
        );
        assert_eq!(SecurityEventType::SsrfAttempt.to_string(), "ssrf_attempt");
        assert_eq!(
            SecurityEventType::ReplayCapHit.to_string(),
            "replay_cap_hit"
        );
        assert_eq!(SecurityEventType::ReplaySkew.to_string(), "replay_skew");
        assert_eq!(
            SecurityEventType::ReplayDuplicate.to_string(),
            "replay_duplicate"
        );
        assert_eq!(
            SecurityEventType::ReplayMonotonicity.to_string(),
            "replay_monotonicity"
        );
        assert_eq!(
            SecurityEventType::ReplayMalformed.to_string(),
            "replay_malformed"
        );
        assert_eq!(
            SecurityEventType::ForwardedHeaderInvalid.to_string(),
            "forwarded_header_invalid"
        );
        assert_eq!(
            SecurityEventType::SurfaceProtocolViolation.to_string(),
            "surface_protocol_violation"
        );
        assert_eq!(
            SecurityEventType::EphemeralPublishDenied.to_string(),
            "ephemeral_publish_denied"
        );
        assert_eq!(
            SecurityEventType::BrennPublishDenied.to_string(),
            "brenn_publish_denied"
        );
        assert_eq!(
            SecurityEventType::MqttPublishDenied.to_string(),
            "mqtt_publish_denied"
        );
        assert_eq!(
            SecurityEventType::WasmActivationThrottled.to_string(),
            "wasm_activation_throttled"
        );
    }

    /// The six `DenialKind` wire spellings are load-bearing identifiers (log
    /// query keys, alert-dedup slots). Pin every one so a rename is a
    /// deliberate, test-breaking act — mirrors `security_event_type_display`.
    #[test]
    fn denial_kind_as_str() {
        assert_eq!(DenialKind::MalformedAddress.as_str(), "malformed_address");
        assert_eq!(DenialKind::UnknownChannel.as_str(), "unknown_channel");
        assert_eq!(DenialKind::MissingSender.as_str(), "missing_sender");
        assert_eq!(DenialKind::AclDenied.as_str(), "acl_denied");
        assert_eq!(DenialKind::BodyTooLarge.as_str(), "body_too_large");
        assert_eq!(DenialKind::GrantAbsent.as_str(), "grant_absent");
        // Display matches as_str.
        assert_eq!(DenialKind::AclDenied.to_string(), "acl_denied");
    }

    /// The `format!("Security: {event_type}")` title `signal_publish_denial`
    /// derives for `EphemeralPublishDenied`. Pinned to lock the exact alert-title
    /// bytes the ephemeral scheme produces.
    const EPHEMERAL_DENIAL_ALERT_TITLE: &str = "Security: ephemeral_publish_denied";

    #[tokio::test]
    async fn signal_dedups_alert_per_app_origin_slug_kind() {
        let (dispatcher, captured, handle) =
            crate::obs::alerting::make_capturing_alerter_with_severity();
        let eph = SecurityEventType::EphemeralPublishDenied;
        let app = |slug, cid| DenialOrigin::App {
            slug,
            conversation_id: cid,
        };
        // First (app-a, unknown_channel): alerts.
        signal_publish_denial(
            &dispatcher,
            eph,
            app("app-a", 1),
            DenialKind::UnknownChannel,
            "ephemeral:x",
        );
        // Repeat same (origin, slug, kind): suppressed (a different conversation
        // id must not mint a new alert — it is not part of the dedup key).
        signal_publish_denial(
            &dispatcher,
            eph,
            app("app-a", 2),
            DenialKind::UnknownChannel,
            "ephemeral:x",
        );
        // Different kind, same app: alerts.
        signal_publish_denial(
            &dispatcher,
            eph,
            app("app-a", 3),
            DenialKind::AclDenied,
            "ephemeral:x",
        );
        // Different app, same kind: alerts.
        signal_publish_denial(
            &dispatcher,
            eph,
            app("app-b", 4),
            DenialKind::UnknownChannel,
            "ephemeral:x",
        );
        // Same slug but component origin, same kind: alerts (origin is part of
        // the dedup key, so an app and a component sharing a slug never collide).
        signal_publish_denial(
            &dispatcher,
            eph,
            DenialOrigin::Component { slug: "app-a" },
            DenialKind::UnknownChannel,
            "ephemeral:x",
        );
        drop(dispatcher);
        handle.await.unwrap();
        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 4, "dedup by (origin,slug,kind): {alerts:?}");
        for (sev, title, _body) in alerts.iter() {
            assert!(matches!(sev, AlertSeverity::Warning), "severity: {sev:?}");
            assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        }
    }

    #[tokio::test]
    async fn signal_component_body_omits_conversation_clause() {
        let (dispatcher, captured, handle) = crate::obs::alerting::make_capturing_alerter();
        signal_publish_denial(
            &dispatcher,
            SecurityEventType::MqttPublishDenied,
            DenialOrigin::Component { slug: "office" },
            DenialKind::AclDenied,
            "mqtt:office",
        );
        drop(dispatcher);
        handle.await.unwrap();
        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 1);
        let (_title, body) = &alerts[0];
        assert!(
            body.contains("component office denied publish"),
            "component attribution clause: {body}"
        );
        assert!(
            !body.contains("conversation"),
            "component body must carry no conversation clause: {body}"
        );
        assert!(
            body.contains("kind=acl_denied address=mqtt:office"),
            "detail schema: {body}"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn signal_sanitizes_address_in_log_and_alert_body() {
        for (origin, log_msg) in [
            (
                DenialOrigin::App {
                    slug: "app-a",
                    conversation_id: 1,
                },
                "app security event",
            ),
            (
                DenialOrigin::Component { slug: "comp-a" },
                "component security event",
            ),
        ] {
            let (dispatcher, captured, handle) = crate::obs::alerting::make_capturing_alerter();
            // Control chars + oversize: must arrive escaped and length-bounded on
            // both the log detail and the alert body (one sanitized `detail` feeds
            // both, so the guarantee is exercised for each origin).
            let hostile = format!("ephemeral:\n\r\tprobe{}", "A".repeat(1000));
            signal_publish_denial(
                &dispatcher,
                SecurityEventType::EphemeralPublishDenied,
                origin,
                DenialKind::AclDenied,
                &hostile,
            );
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(alerts.len(), 1);
            let (_title, body) = &alerts[0];
            let safe = sanitize_untrusted_str(&hostile, MAX_LOGGED_UNTRUSTED_BYTES);
            // The sanitized (not raw) address is what reaches the alert body.
            assert!(
                body.contains(&safe),
                "body should carry sanitized addr: {body}"
            );
            assert!(
                !body.contains("ephemeral:\n"),
                "raw control chars must not survive: {body}"
            );
            // Length bounded by the cap (marker rides outside the budget).
            assert!(
                safe.len() <= MAX_LOGGED_UNTRUSTED_BYTES + TRUNCATION_MARKER.len(),
                "safe len {}",
                safe.len()
            );
            // Log side: the denial fired through the origin-appropriate security-log
            // helper (app-attributed vs component-attributed), and the oversize
            // address was truncated in the log detail too — the trailing truncation
            // marker is the sanitizer's tell that the untrusted string was bounded
            // before it reached the log field.
            assert!(logs_contain(log_msg), "expected log message {log_msg}");
            assert!(
                logs_contain(TRUNCATION_MARKER),
                "log detail should carry the truncated (sanitized) address"
            );
        }
    }
}
