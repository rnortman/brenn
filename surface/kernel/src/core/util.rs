//! Leaf helpers for [`ClientCore`](super::ClientCore): the backoff PRNG, report
//! truncation, channel-class derivation, binding lookups, and small wire/format
//! utilities. All private to `core`; the state machine in the parent module is
//! the only caller.

use std::time::Duration;

use brenn_envelope::DeliveryClass;
use brenn_surface_proto::{
    OutputBinding, PublishOutcome, ServerFrame, SurfaceBindings, surface_delivery_class,
};

use super::PublishStatus;

/// A minimal splitmix64 PRNG: one `u64` of state, advanced by the standard
/// splitmix64 step. Deterministic given its seed, dependency-free (no `rand`,
/// no `getrandom`). Used only to jitter the reconnect backoff, where the whole
/// requirement is cross-client distinctness of the schedule, not statistical
/// quality — splitmix64 vastly exceeds that bar.
pub(super) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Cap a `Log` frame field (`message` or `source`) at its proto byte limit,
/// truncating on a UTF-8 boundary and appending a marker so the receiver sees the
/// value was cut. Asserts `cap` exceeds the marker length rather than trusting it
/// by prose: a smaller cap would underflow the subtraction below (a release-mode
/// wrap into a hang), so it dies loudly instead.
pub(crate) fn truncate_report_field(value: String, cap: usize) -> String {
    const MARKER: &str = "…[truncated]";
    assert!(
        cap > MARKER.len(),
        "surface client: report-field cap {cap} is smaller than the truncation marker"
    );
    if value.len() <= cap {
        return value;
    }
    let mut end = cap - MARKER.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = value;
    out.truncate(end);
    out.push_str(MARKER);
    out
}

/// A binding channel scheme this client can route today: every scheme that binds
/// to a surface at all, now that the page-local router handles `local:`.
///
/// Kept as its own predicate rather than collapsed into
/// [`brenn_surface_proto::surface_bindable`]: this one answers "can *this
/// client* route it", which is what makes an unroutable `Welcome` binding fatal
/// here. The two agreeing today is a fact, not an identity.
pub(super) fn channel_scheme_supported(channel: &str) -> bool {
    matches!(
        surface_delivery_class(channel),
        Some(DeliveryClass::Durable | DeliveryClass::Ephemeral | DeliveryClass::Local)
    )
}

/// The output binding wired to `(instance, port)` in the current bindings, if
/// any. `Some` doubles as "this output is bound" — an unbound-port `Publish` is
/// a protocol violation, so the core resolves this before sending — the address
/// tells the core whether to route the publish locally or put it on the wire,
/// and the binding's `urgency` is the port's configured default, which the
/// local router needs to stamp its own envelopes.
///
/// A pair maps to at most one output binding, so the first match is the only
/// match.
pub(super) fn resolve_output<'a>(
    bindings: &'a SurfaceBindings,
    instance: &str,
    port: &str,
) -> Option<&'a OutputBinding> {
    bindings
        .outputs
        .iter()
        .find(|b| b.instance == instance && b.port == port)
}

/// Map the server's wire [`PublishOutcome`] to the [`PublishStatus`] surfaced on
/// [`Event::PublishResult`](super::Event::PublishResult).
pub(super) fn publish_outcome_to_status(outcome: PublishOutcome) -> PublishStatus {
    match outcome {
        PublishOutcome::Ok => PublishStatus::Ok,
        PublishOutcome::RateLimited => PublishStatus::RateLimited,
        PublishOutcome::BodyTooLarge { len, max } => PublishStatus::BodyTooLarge { len, max },
        PublishOutcome::Failed => PublishStatus::Failed,
    }
}

/// The `type` tag of a server frame, for diagnostic messages.
pub(super) fn frame_type_name(frame: &ServerFrame) -> &'static str {
    match frame {
        ServerFrame::Welcome { .. } => "Welcome",
        ServerFrame::Heartbeat => "Heartbeat",
        ServerFrame::SubscribeResult { .. } => "SubscribeResult",
        ServerFrame::Deliver { .. } => "Deliver",
        ServerFrame::ReAnchor { .. } => "ReAnchor",
        ServerFrame::PublishResult { .. } => "PublishResult",
        ServerFrame::PublishBatchResult { .. } => "PublishBatchResult",
    }
}

/// Append `?build=<url-encoded build_id>`. `url` carries no query by contract.
pub(super) fn build_connect_url(url: &str, build_id: &str) -> String {
    format!("{url}?build={}", encode_query_component(build_id))
}

/// Percent-encode a query-string component, encoding everything outside the
/// RFC 3986 unreserved set. A tiny helper rather than a dependency: build ids
/// are hex-ish and pass through unchanged, but arbitrary input stays safe.
fn encode_query_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// Milliseconds of a config `Duration`. Config durations are small (seconds);
/// a value large enough to overflow `u64` millis is a configuration error.
pub(super) fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).expect("surface client: config duration too large")
}
