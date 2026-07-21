//! Canonical wire-shaped fixtures for the surface test suites.
//!
//! The surface client, shell, and protobar all parse the same wire-shaped
//! JSON in their tests (a delivered `MessageEnvelope`, a delivery `seq` +
//! opaque `cursor`). Keeping one copy here means a struct/serde change breaks
//! exactly one place, loudly, instead of silently rotting a hand-copied literal.
//! Envelopes are kept as JSON text on purpose: the tests exercise the wire
//! boundary, so they parse the same bytes the transport would carry.

use brenn_envelope::MessageEnvelope;
use brenn_surface_proto::{
    Abi, Binding, ComponentEntry, Cursor, DeliverTarget, LocalChannel, LogLevel, OutputBinding,
    PublishOutcome, ServerFrame, SubscribeOutcome, SurfaceBindings, SurfaceDescription,
};

/// Re-exported so consumers can name `Uuid` parameter types (page-load epochs,
/// message ids) without pinning `uuid` themselves.
pub use uuid::Uuid;

/// Parked-batch depth every fixture component carries.
///
/// A fixture's own value, not the boot default: this crate is wire-shaped and
/// depends on nothing that resolves config, so it states what it puts on the
/// frame rather than claiming to mirror a number it cannot see. A test that
/// cares about the depth sets its own.
pub const FIXTURE_PARKED_BATCH_DEPTH: u64 = 8;

/// What a `welcome_frame` fixture varies. `Default` holds the values every
/// suite shares — no bindings, nil epoch, alert granted, no components — so a
/// call site names only what it overrides. The two binding fields carry
/// different types (an output advertises its default urgency), so a swap is now
/// a compile error rather than a discipline.
pub struct WelcomeParams {
    pub subscriptions: Vec<Binding>,
    pub outputs: Vec<OutputBinding>,
    pub alert_granted: bool,
    pub takeover_granted: bool,
    pub components: Vec<&'static str>,
    /// The advertised surface error-report floor. `Some(floor)` lights the
    /// reserved `#brenn`/`error-reports` output port; `None` keeps reports
    /// console-only.
    pub error_report_floor: Option<LogLevel>,
    /// The surface self-description telemetry parameters: the interval the shell
    /// reports geometry + status on.
    pub surface_description: SurfaceDescription,
    /// The page-local channels the surface declares, with their server-resolved
    /// ring depths — the router table. Empty for surfaces with no local wiring.
    pub local_channels: Vec<LocalChannel>,
    /// The advertised publish-body cap. Varies only for suites that need two
    /// connections to advertise *different* caps — an operator can lower
    /// `messaging.max_body_bytes` and restart with no build change, so a page can
    /// reconnect to a smaller contract than the one it buffered against.
    pub max_body_bytes: u64,
}

impl Default for WelcomeParams {
    fn default() -> Self {
        WelcomeParams {
            subscriptions: Vec::new(),
            outputs: Vec::new(),
            alert_granted: true,
            takeover_granted: false,
            components: Vec::new(),
            error_report_floor: None,
            surface_description: SurfaceDescription {
                status_interval_secs: 60,
            },
            local_channels: Vec::new(),
            max_body_bytes: FIXTURE_MAX_BODY_BYTES,
        }
    }
}

/// The body cap every fixture `Welcome` advertises unless a suite overrides it.
pub const FIXTURE_MAX_BODY_BYTES: u64 = 65_536;

/// Serialize a `deskbar` `Welcome` frame from `params`. The `surface`/
/// `participant_id` and the heartbeat are fixed so a fixture reads the same in
/// every suite; everything a test varies lives in `WelcomeParams`.
/// Shared by the client core/driver suites and the shell entry integration
/// test.
///
/// The component map is completed from the bindings: any binding instance not
/// named in `components` is added as its own kind. Boot resolves both halves
/// from one declaration set and rejects a binding naming an undeclared
/// instance, so a `Welcome` binding an instance the map omits is one no server
/// can emit — a fixture that produced one would be testing against a peer that
/// does not exist. Tests that need that non-conformant frame on purpose build
/// it with [`welcome_frame_raw`].
pub fn welcome_frame(params: WelcomeParams) -> String {
    // Fixtures name kinds; each becomes a single instance whose id is the kind
    // (the config default). Binding instances the caller did not name join them,
    // so the frame satisfies boot's bindings-⊆-components invariant.
    let mut entries: Vec<ComponentEntry> = params
        .components
        .iter()
        .map(|kind| ComponentEntry {
            instance: (*kind).to_string(),
            kind: (*kind).to_string(),
            abi: Abi::Dom,
            parked_batch_depth: FIXTURE_PARKED_BATCH_DEPTH,
            config: Default::default(),
        })
        .collect();
    let binding_instances = params
        .subscriptions
        .iter()
        .map(|b| &b.instance)
        .chain(params.outputs.iter().map(|b| &b.instance));
    for instance in binding_instances {
        if !entries.iter().any(|e| e.instance == *instance) {
            entries.push(ComponentEntry {
                instance: instance.clone(),
                kind: instance.clone(),
                abi: Abi::Dom,
                parked_batch_depth: FIXTURE_PARKED_BATCH_DEPTH,
                config: Default::default(),
            });
        }
    }
    welcome_frame_raw(params, entries)
}

/// [`welcome_frame`] with the component map stated verbatim — no completion
/// from the bindings.
///
/// The escape hatch for tests that must build a `Welcome` a conforming server
/// would never send (a binding naming an instance the map omits), which is
/// exactly the peer input the client's handshake validation exists to reject.
pub fn welcome_frame_raw(params: WelcomeParams, components: Vec<ComponentEntry>) -> String {
    let WelcomeParams {
        subscriptions,
        outputs,
        alert_granted,
        takeover_granted,
        components: _,
        error_report_floor,
        surface_description,
        local_channels,
        max_body_bytes,
    } = params;
    serde_json::to_string(&ServerFrame::Welcome {
        surface: "deskbar".into(),
        participant_id: "surface:deskbar".into(),
        heartbeat_secs: 20,
        max_body_bytes,
        alert_granted,
        takeover_granted,
        error_report_floor,
        surface_description,
        bindings: SurfaceBindings {
            components,
            subscriptions,
            outputs,
            local_channels,
            chrome_instance: String::new(),
        },
    })
    .expect("surface test fixture: Welcome frame serializes")
}

/// Serialize a single-target `Deliver` frame to `instance`'s subscription on
/// `channel`, carrying `sample_envelope(body)` at delivery-time span `seq` and
/// opaque `cursor`, with `dropped` messages reported lost on that subscription
/// since its previous delivery. Shared by the client driver suite and the shell
/// entry integration test.
///
/// `instance` is the subscribing principal. It is a required argument rather than defaulted because a
/// delivery naming the wrong principal is delivered to nobody, and a fixture
/// that guessed it would make that failure look like a routing bug.
///
/// The `cursor` is opaque — a fixture treats it as a blob the client stores and
/// echoes, never as something to interpret; build one with [`wire_cursor`].
pub fn deliver_frame(
    channel: &str,
    instance: &str,
    body: &str,
    seq: u64,
    cursor: Cursor,
    dropped: u64,
) -> String {
    serde_json::to_string(&ServerFrame::Deliver {
        channel: channel.into(),
        envelope: sample_envelope(body),
        targets: vec![DeliverTarget {
            instance: instance.to_owned(),
            seq,
            cursor,
            dropped,
        }],
    })
    .expect("surface test fixture: Deliver frame serializes")
}

/// A multi-target `Deliver`: one envelope on `channel`, one target per
/// subscription — the wire shape of live fan-out to sibling instances on one
/// connection. Build targets with [`deliver_target`].
pub fn deliver_frame_multi(
    channel: &str,
    envelope: &MessageEnvelope,
    targets: Vec<DeliverTarget>,
) -> String {
    serde_json::to_string(&ServerFrame::Deliver {
        channel: channel.into(),
        envelope: envelope.clone(),
        targets,
    })
    .expect("surface test fixture: multi-target Deliver frame serializes")
}

/// One target of a multi-target `Deliver`, at its own span seq and cursor. The
/// cursor is opaque and derived from `(instance, seq)` so distinct targets carry
/// distinct blobs; a test that asserts a specific cursor builds its own.
pub fn deliver_target(instance: &str, seq: u64, dropped: u64) -> DeliverTarget {
    DeliverTarget {
        instance: instance.to_owned(),
        seq,
        cursor: wire_cursor(&format!("cursor-{instance}-{seq}")),
        dropped,
    }
}

/// Serialize an `Ok` `SubscribeResult` for `instance`'s subscription on
/// `channel` (no replay, no gap), the frame that activates a pending
/// subscription.
pub fn subscribe_result_ok(channel: &str, instance: &str) -> String {
    serde_json::to_string(&ServerFrame::SubscribeResult {
        channel: channel.into(),
        instance: instance.to_owned(),
        outcome: SubscribeOutcome::Ok,
        replay_count: 0,
        gap: None,
    })
    .expect("surface test fixture: SubscribeResult frame serializes")
}

/// Serialize an `Ok` `PublishResult` for `correlation`.
pub fn publish_result_ok(correlation: u64) -> String {
    serde_json::to_string(&ServerFrame::PublishResult {
        correlation: Some(correlation),
        outcome: PublishOutcome::Ok,
    })
    .expect("surface test fixture: PublishResult frame serializes")
}

/// The canonical minimal ephemeral `MessageEnvelope` as wire JSON text, with
/// `body` substituted and `publish_ts` chosen. Everything else is fixed so a
/// fixture reads the same in every suite. Suites that must control staleness
/// (e.g. ack pruning judged on the publish timestamp) vary `publish_ts`.
pub fn sample_envelope_json_at(body: &str, publish_ts: &str) -> String {
    serde_json::json!({
        "message_id": "00000000-0000-0000-0000-000000000001",
        "source": "src",
        "channel": "ephemeral:demo",
        "sender": "surface:deskbar",
        "publish_ts": publish_ts,
        "body": body,
        "urgency": "normal",
        "envelope_type": "ephemeral",
    })
    .to_string()
}

/// The canonical minimal ephemeral `MessageEnvelope` as wire JSON text, with
/// `body` substituted and a fixed `publish_ts`.
pub fn sample_envelope_json(body: &str) -> String {
    sample_envelope_json_at(body, "2023-11-14T22:13:20Z")
}

/// The canonical ephemeral envelope, parsed. Panics if the literal no longer
/// deserializes — a fixture is a test dependency, so a break here should fail
/// loud.
pub fn sample_envelope(body: &str) -> MessageEnvelope {
    serde_json::from_str(&sample_envelope_json(body))
        .expect("surface test fixture: sample envelope JSON deserializes")
}

/// Build an opaque wire [`Cursor`] from a blob string. The kernel stores and
/// echoes it verbatim, so a fixture uses it as a token to assert the client
/// re-presents on reconnect, never as something to interpret. The only way to
/// build a `Cursor` is a serde round-trip through a JSON string (its inner field
/// is private and it has no constructor).
pub fn wire_cursor(blob: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(blob.to_string()))
        .expect("a JSON string always deserializes into the transparent Cursor newtype")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_envelope_json_deserializes() {
        let envelope = sample_envelope("hello");
        assert_eq!(envelope.body, "hello");
        assert_eq!(
            envelope.envelope_type,
            brenn_envelope::ChannelScheme::Ephemeral
        );
    }

    #[test]
    fn wire_cursor_round_trips_as_a_string() {
        let c = wire_cursor("opaque");
        assert_eq!(
            serde_json::to_value(&c).unwrap(),
            serde_json::json!("opaque")
        );
    }
}
