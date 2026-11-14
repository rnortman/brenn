//! The page rig's kit: every frame and document the rig's scripted attach peer
//! sends, serialized from the types that define them.
//!
//! The rig runs the production kernel bundle, the production page glue and a
//! transpiled kind in node against a scripted peer. It reads this kit, which
//! `//surface/test-fixtures/peer:page_rig_kit` prints at build time, and knows no
//! field name of a frame it sends: a frame it must vary is a [template], a
//! frame plus the JSON pointers of its variable fields, found by serializing
//! the typed frame with a sentinel in each one.
//!
//! The page speaks the surface route's attach version, at which `Deliver` and
//! the page's publishes are binary messages: a big-endian `u32` header length,
//! the header JSON, then the bodies. A binary kind's template is its header as
//! the exact text the codec wrote, its slots, and the pointer of each body
//! descriptor in body order; the rig fills the header, writes each descriptor
//! and appends the bodies. The `server_samples` pin that assembly byte for
//! byte against messages the codec encoded.
//!
//! The kit holds:
//!
//! - `pages`: the named pages, `cases` and `measure`. Each states the three
//!   page metas, the location, its instances with their kind, contract and
//!   command channel, the report channel, its state-holding instances per
//!   contract, its body cap and its opening ladder (`Hello`, `Welcome`, and the
//!   config channel's `Deliver` carrying the bindings document, as its header
//!   text and the document's text). The two differ
//!   in the `Welcome` and in their cast: `cases` has one state-holder and the
//!   instances that cross the two contracts, `measure` one state-holder per
//!   measured size on each contract, since a retained state is of one size and
//!   no page-local channel can be cleared.
//! - `answers`: templates for an accepted `SubscribeResult`, `PublishResult`
//!   and `PublishBatchResult`.
//! - `deliver`: a binary template for a one-row `Deliver` on an `ephemeral:`
//!   channel.
//! - `deliver_attached`: the same row naming two attachments, and
//!   `attached`, those attachments as the header writes them.
//! - `deferred_views`: binary templates for a `DeferredView`, the one at index
//!   `n` holding `n` entries, so the rig restates a parked set of any size up
//!   to [`MAX_VIEW_ENTRIES`] without knowing the frame's shape.
//! - `server_samples`: whole `Deliver` and `DeferredView` messages the codec
//!   encoded, as hex, each with the slot values and bodies that produced it.
//! - `client_samples`: client frames of each kind the peer reads, a text frame
//!   as its text and a binary one as hex, so the rig's reader is checked
//!   against what Rust writes. The binary ones carry typed text, non-text,
//!   deferred and untyped bodies and every kind of control op.

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, ClientFrame, Codec, DeferredOpKind, DeferredViewEntry,
    PublishBatchOutcome, PublishOutcome, ServerFrame, SubscribeOutcome, Urgency, VersionRange,
    WireMessage,
};
use brenn_envelope::grants::ProcessorContract;
use brenn_envelope::{AttachmentName, Attachments, BlobId, BlobRef, MessageBody};
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{ComponentGrant, SURFACE_ATTACH_VERSION};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::bindings::{
    call, component_with_grants, doc, local, local_non_text, output, subscription,
};
use crate::frames::{client_frame, deliver, frame, row, server_hello, welcome};

/// The rig's surface slug.
pub const SLUG: &str = "rig";
/// The build id the rig's pages are rendered by.
pub const BUILD_ID: &str = "rig-build-1";
/// The host the rig's pages state they were loaded from.
pub const HOST: &str = "page.example.com";
/// The rig's fixture kind on `brenn:processor@0.1.0`.
pub const STRING_KIND: &str = "processor-page-test";
/// The rig's fixture kind on `brenn:processor@0.2.0`, the twin of
/// [`STRING_KIND`].
pub const BYTES_KIND: &str = "processor-page-bytes-test";
/// The rig's guest-SDK kind on `brenn:processor@0.2.0`, whose binary state is
/// kept through the SDK's `RetainedState` and runs the same workload.
pub const SDK_BYTES_KIND: &str = "processor-sdk-bytes-test";
/// The channel every instance's report is published on.
pub const REPORT_CHANNEL: &str = "ephemeral:rig.report";
/// The wire channel `responder`'s `feed` port is subscribed to.
pub const FEED_CHANNEL: &str = "ephemeral:rig.feed";
/// The page-local channel behind `keeper`'s `memo` io port, read by `reader`.
pub const MEMO_CHANNEL: &str = "local:rig/memo";
/// The page-local channel behind `keeper`'s `timer` io port.
pub const TIMER_CHANNEL: &str = "local:rig/timer";
/// The page-local channel behind `b-keeper`'s `memo` io port, read by `plain`.
pub const SHARED_CHANNEL: &str = "local:rig/shared";
/// The page-local channel behind `b-keeper`'s `timer` io port, declared
/// `non_text`.
pub const BYTES_TIMER_CHANNEL: &str = "local:rig/b-timer";
/// The wire channel `b-keeper`'s `out` port is bound to.
pub const BYTES_OUT_CHANNEL: &str = "ephemeral:rig.bytes-out";
/// The wire channel `b-keeper`'s `feed` port is subscribed to, declared
/// `non_text`.
pub const BYTES_FEED_CHANNEL: &str = "ephemeral:rig.bytes-feed";
/// The wire channel `b-keeper`'s `bout` port is bound to, declared `non_text`.
pub const BYTES_WIRE_CHANNEL: &str = "ephemeral:rig.bytes-wire";
/// The wire channel, declared `non_text`, the `measure` page's `b-stream`
/// instance is woken by.
pub const STREAM_CHANNEL: &str = "ephemeral:rig.stream";
/// The most entries a `DeferredView` template of the kit holds.
pub const MAX_VIEW_ENTRIES: usize = 3;
/// The body cap the `cases` page's `Welcome` states.
pub const CASES_BODY_CAP: u64 = 65_536;
/// The body cap the `measure` page's `Welcome` states: the per-activation
/// publish aggregate, which is the binding limit above it.
pub const MEASURE_BODY_CAP: u64 = 4_194_304;

/// The instances on every rig page, the chrome stand-in first, all of
/// [`STRING_KIND`]. Each page adds its state-holders after them.
pub const INSTANCES: [&str; 4] = ["chrome", "keeper", "reader", "responder"];

/// One page's state-holders: those of [`STRING_KIND`], then those of
/// [`BYTES_KIND`], then those of [`SDK_BYTES_KIND`].
#[derive(Clone, Copy)]
pub struct Staters {
    pub string: &'static [&'static str],
    pub bytes: &'static [&'static str],
    pub sdk: &'static [&'static str],
}

impl Staters {
    /// Every state-holder with its kind, in page order.
    fn all(self) -> impl Iterator<Item = (&'static str, &'static str)> {
        let string = self.string.iter().map(|s| (*s, STRING_KIND));
        let bytes = self.bytes.iter().map(|s| (*s, BYTES_KIND));
        let sdk = self.sdk.iter().map(|s| (*s, SDK_BYTES_KIND));
        string.chain(bytes).chain(sdk)
    }
}

/// What one page runs beyond [`INSTANCES`]: its state-holders, and the
/// instances that exercise the two contracts against each other, each with its
/// kind.
#[derive(Clone, Copy)]
pub struct Cast {
    pub staters: Staters,
    pub crossers: &'static [(&'static str, &'static str)],
}

/// The instances of the `cases` page that cross the two contracts:
///
/// - `b-keeper` keeps typed bodies on [`SHARED_CHANNEL`], which is text-only,
///   and non-text bodies on [`BYTES_TIMER_CHANNEL`], publishes on the
///   text-only wire channel [`BYTES_OUT_CHANNEL`] and the `non_text` one
///   [`BYTES_WIRE_CHANNEL`], reads the `non_text` wire channel
///   [`BYTES_FEED_CHANNEL`] and calls `b-responder`;
/// - `b-responder` answers calls on `ask`;
/// - `b-mixed` calls `responder`, the string-contract callee;
/// - `plain`, on the string contract, is woken by [`SHARED_CHANNEL`];
/// - `caller`, on the string contract, calls `b-responder`.
pub const CROSSERS: &[(&str, &str)] = &[
    ("b-keeper", BYTES_KIND),
    ("b-responder", BYTES_KIND),
    ("b-mixed", BYTES_KIND),
    ("plain", STRING_KIND),
    ("caller", STRING_KIND),
];

/// The `cases` page's cast: one state-holder on the string contract, one of
/// the guest-SDK kind, and the contract crossers.
pub const CASES_CAST: Cast = Cast {
    staters: Staters {
        string: &["stater"],
        bytes: &[],
        sdk: &["s-stater"],
    },
    crossers: CROSSERS,
};
/// The `measure` page's cast: on each contract, one state-holder per size the
/// state workload is timed at, and `b-stream`, a 0.2.0 instance woken by
/// binary rows on [`STREAM_CHANNEL`] through its `stream` port, push 1 and
/// retain 1.
pub const MEASURE_CAST: Cast = Cast {
    staters: Staters {
        string: &["stater-1", "stater-2", "stater-3"],
        bytes: &["b-stater-1", "b-stater-2", "b-stater-3"],
        sdk: &[],
    },
    crossers: &[("b-stream", BYTES_KIND)],
};

/// The contract `kind` is built against.
pub fn kind_contract(kind: &str) -> ProcessorContract {
    match kind {
        STRING_KIND => ProcessorContract::V0_1,
        BYTES_KIND | SDK_BYTES_KIND => ProcessorContract::V0_2,
        other => panic!("the rig stages no kind {other:?}"),
    }
}

/// The grants `kind`'s specification requires: the raw fixtures call, and the
/// guest-SDK kind logs a recovered state.
fn kind_grants(kind: &str) -> [ComponentGrant; 2] {
    match kind {
        SDK_BYTES_KIND => [ComponentGrant::Ports, ComponentGrant::Log],
        _ => [ComponentGrant::Ports, ComponentGrant::Calls],
    }
}

/// The surface's config channel, in the server's naming.
pub fn config_channel() -> String {
    format!("ephemeral:{SLUG}.surface.{SLUG}.bindings")
}

/// The wire channel `instance`'s `in` port is subscribed to: where the rig
/// delivers its commands.
pub fn command_channel(instance: &str) -> String {
    format!("ephemeral:rig.in.{instance}")
}

/// The page-local channel behind a state-holder's `state` io port.
pub fn state_channel(instance: &str) -> String {
    format!("local:rig/{instance}")
}

/// Every instance of a page running `cast`, with its kind, in order.
fn instances(cast: Cast) -> Vec<(&'static str, &'static str)> {
    INSTANCES
        .iter()
        .map(|i| (*i, STRING_KIND))
        .chain(cast.staters.all())
        .chain(cast.crossers.iter().copied())
        .collect()
}

/// The bindings document a rig page running `cast` is put in force with.
pub fn document(cast: Cast) -> BindingsDocument {
    let staters = cast.staters;
    let all = instances(cast);
    let mut components: Vec<_> = all
        .iter()
        .map(|(instance, kind)| component_with_grants(instance, kind, &kind_grants(kind)))
        .collect();
    let mut subscriptions: Vec<_> = all
        .iter()
        .map(|(instance, _)| subscription(instance, "in", &command_channel(instance), 1, 1))
        .collect();
    let mut outputs: Vec<_> = all
        .iter()
        .map(|(instance, _)| output(instance, "report", REPORT_CHANNEL))
        .collect();

    // `keeper`: two page-local io ports, retained but never waking it, and the
    // caller's end of the one call.
    for (port, channel) in [("memo", MEMO_CHANNEL), ("timer", TIMER_CHANNEL)] {
        outputs.push(output("keeper", port, channel));
        subscriptions.push(subscription("keeper", port, channel, 0, 1));
    }
    // `reader`: woken by what `keeper` keeps on `memo`.
    subscriptions.push(subscription("reader", "feed", MEMO_CHANNEL, 1, 1));
    // `responder`: the callee's end, and a wire channel the peer delivers on.
    subscriptions.push(subscription("responder", "feed", FEED_CHANNEL, 1, 1));
    // Each state-holder: the state workload's io port, on a channel of its own,
    // declared `non_text` for a holder whose contract carries non-text bodies.
    let mut locals = vec![local(MEMO_CHANNEL, 1), local(TIMER_CHANNEL, 1)];
    for (stater, kind) in staters.all() {
        let channel = state_channel(stater);
        outputs.push(output(stater, "state", &channel));
        subscriptions.push(subscription(stater, "state", &channel, 0, 1));
        locals.push(if kind_contract(kind).carries_non_text_bodies() {
            local_non_text(&channel, 1)
        } else {
            local(&channel, 1)
        });
    }
    let mut calls = vec![call("keeper", "peer", "responder", "ask")];
    let crosses = |instance: &str| cast.crossers.iter().any(|(i, _)| *i == instance);
    if crosses("b-keeper") {
        outputs.push(output("b-keeper", "memo", SHARED_CHANNEL));
        subscriptions.push(subscription("b-keeper", "memo", SHARED_CHANNEL, 0, 1));
        outputs.push(output("b-keeper", "out", BYTES_OUT_CHANNEL));
        outputs.push(output("b-keeper", "bout", BYTES_WIRE_CHANNEL));
        subscriptions.push(subscription("b-keeper", "feed", BYTES_FEED_CHANNEL, 1, 1));
        locals.push(local(SHARED_CHANNEL, 1));
        outputs.push(output("b-keeper", "timer", BYTES_TIMER_CHANNEL));
        subscriptions.push(subscription("b-keeper", "timer", BYTES_TIMER_CHANNEL, 0, 1));
        locals.push(local_non_text(BYTES_TIMER_CHANNEL, 1));
        calls.push(call("b-keeper", "peer", "b-responder", "ask"));
    }
    if crosses("plain") {
        subscriptions.push(subscription("plain", "feed", SHARED_CHANNEL, 1, 1));
    }
    if crosses("b-mixed") {
        calls.push(call("b-mixed", "peer", "responder", "ask"));
    }
    if crosses("caller") {
        calls.push(call("caller", "peer", "b-responder", "ask"));
    }
    if crosses("b-stream") {
        subscriptions.push(subscription("b-stream", "stream", STREAM_CHANNEL, 1, 1));
    }
    for c in &mut components {
        let instance = c.instance.as_str();
        if calls.iter().any(|b| b.instance == instance) {
            c.call_ports = vec!["peer".to_string()];
        }
        if calls.iter().any(|b| b.target_instance == instance) {
            c.sync_ports = vec!["ask".to_string()];
        }
    }
    let mut doc = doc(components, subscriptions, outputs, locals);
    doc.calls = calls;
    for wire in &mut doc.wire_channels {
        if [BYTES_FEED_CHANNEL, BYTES_WIRE_CHANNEL, STREAM_CHANNEL].contains(&wire.channel.as_str())
        {
            wire.non_text = true;
        }
    }
    doc
}

/// The attachment facts a rig page's `Welcome` states under `max_body_bytes`.
pub fn facts(max_body_bytes: u64) -> AttachmentFacts {
    let body = usize::try_from(max_body_bytes).expect("a body cap fits usize");
    AttachmentFacts {
        version: SURFACE_ATTACH_VERSION,
        participant_id: "surface:rig".to_string(),
        session_id: "rig-session".to_string(),
        heartbeat_secs: 30,
        max_body_bytes,
        max_frame_bytes: u64::try_from(
            Codec::negotiated(SURFACE_ATTACH_VERSION).max_client_frame_bytes(body),
        )
        .expect("a frame cap fits u64"),
        alert_granted: false,
        blobs: None,
    }
}

/// One named page: its metas, location, cast and opening ladder.
fn page(max_body_bytes: u64, cast: Cast) -> Value {
    let staters = cast.staters;
    let document = document(cast);
    let instances: Vec<Value> = instances(cast)
        .iter()
        .map(|(instance, kind)| {
            json!({
                "instance": instance,
                "kind": kind,
                "contract": kind_contract(kind).to_string(),
                "command_channel": command_channel(instance),
            })
        })
        .collect();
    json!({
        "slug": SLUG,
        "build_id": BUILD_ID,
        "config_channel": config_channel(),
        "location": { "protocol": "http:", "host": HOST },
        "instances": instances,
        "report_channel": REPORT_CHANNEL,
        "feed_channel": FEED_CHANNEL,
        "bytes_feed_channel": BYTES_FEED_CHANNEL,
        "bytes_out_channel": BYTES_OUT_CHANNEL,
        "bytes_wire_channel": BYTES_WIRE_CHANNEL,
        "stream_channel": STREAM_CHANNEL,
        "shared_channel": SHARED_CHANNEL,
        "bytes_timer_channel": BYTES_TIMER_CHANNEL,
        "state_instances": staters.string,
        "bytes_state_instances": staters.bytes,
        "sdk_state_instances": staters.sdk,
        "status_channel": document.platform.status_channel,
        "max_body_bytes": max_body_bytes,
        "ladder": {
            "hello": parse(server_hello()),
            "welcome": parse(welcome(facts(max_body_bytes))),
            "config_deliver": config_rung(&document.to_body()),
        },
    })
}

/// A sentinel for one variable field of a template.
#[derive(Clone, Copy)]
enum Sentinel {
    Text(&'static str),
    Number(u64),
}

/// The sentinels the templates are built with, by slot name. Each is a value no
/// fixed field of any template holds.
const CHANNEL: (&str, Sentinel) = ("channel", Sentinel::Text("@slot:channel"));
const CURSOR: (&str, Sentinel) = ("cursor", Sentinel::Text("@slot:cursor"));
const MESSAGE_ID: (&str, Sentinel) = (
    "message_id",
    Sentinel::Text("51075107-5107-4107-8107-510751075107"),
);
const SEQ: (&str, Sentinel) = ("seq", Sentinel::Number(7_000_000_001));
/// The content type a binary template's body is written with, which marks
/// where its descriptor sits.
const BODY_TYPE: &str = "application/x-rig-slot";
const CORRELATION: (&str, Sentinel) = ("correlation", Sentinel::Number(7_000_000_002));
const ATTRIBUTION: (&str, Sentinel) = ("attribution", Sentinel::Text("@slot:attribution"));
/// The names of a `DeferredView` template's per-entry slots.
const VIEW_MESSAGE_IDS: [&str; MAX_VIEW_ENTRIES] = ["message_id_0", "message_id_1", "message_id_2"];
const VIEW_DELIVER_AFTERS: [&str; MAX_VIEW_ENTRIES] =
    ["deliver_after_0", "deliver_after_1", "deliver_after_2"];
const REPLAY_COUNT: (&str, Sentinel) = ("replay_count", Sentinel::Number(7_000_003));

fn text(slot: (&str, Sentinel)) -> String {
    match slot.1 {
        Sentinel::Text(s) => s.to_string(),
        Sentinel::Number(_) => panic!("slot {} is numeric", slot.0),
    }
}

fn number(slot: (&str, Sentinel)) -> u64 {
    match slot.1 {
        Sentinel::Number(n) => n,
        Sentinel::Text(_) => panic!("slot {} is text", slot.0),
    }
}

/// `{ "frame": …, "slots": { name: [pointer, …] } }` for a text-kind
/// `frame`, each slot's pointers being every place its sentinel landed.
fn template(frame: &ServerFrame, slots: &[(&str, Sentinel)]) -> Value {
    let value = parse(crate::frames::frame(frame));
    let found = slot_pointers(&value, slots);
    json!({ "frame": value, "slots": found })
}

/// Every slot's pointers in `value`. A slot whose sentinel landed nowhere is a
/// template that cannot be filled, and panics.
fn slot_pointers(value: &Value, slots: &[(&str, Sentinel)]) -> serde_json::Map<String, Value> {
    let mut found = serde_json::Map::new();
    for &(name, sentinel) in slots {
        let mut pointers = Vec::new();
        find(value, sentinel, &mut String::new(), &mut pointers);
        assert!(
            !pointers.is_empty(),
            "slot {name:?} landed nowhere in {value}"
        );
        found.insert(
            name.to_string(),
            Value::Array(pointers.into_iter().map(Value::String).collect()),
        );
    }
    found
}

/// A binary message cut at its header: the header's text and the body section.
fn split_binary(message: WireMessage) -> (String, Vec<u8>) {
    let WireMessage::Binary(bytes) = message else {
        panic!("a body-carrying frame is a binary message at the surface route's version");
    };
    let len = u32::from_be_bytes(bytes[..4].try_into().expect("a four-byte prefix"));
    let end = 4 + usize::try_from(len).expect("a header length fits usize");
    let header = std::str::from_utf8(&bytes[4..end])
        .expect("a header is UTF-8")
        .to_string();
    (header, bytes[end..].to_vec())
}

/// `{ "header": text, "slots": …, "bodies": [pointer, …] }` for a binary-kind
/// `frame` whose every body is typed [`BODY_TYPE`]: the header exactly as the
/// codec wrote it, and the pointer of each body's descriptor, in body order.
/// A `DeferredView` of an empty set carries no body.
fn binary_template(frame: &ServerFrame, slots: &[(&str, Sentinel)]) -> Value {
    let (header, _) = split_binary(crate::frames::frame(frame));
    let value: Value = serde_json::from_str(&header).expect("a header is JSON");
    let found = slot_pointers(&value, slots);
    let mut typed = Vec::new();
    find(
        &value,
        Sentinel::Text(BODY_TYPE),
        &mut String::new(),
        &mut typed,
    );
    let bodies: Vec<Value> = typed
        .into_iter()
        .map(|pointer| {
            let descriptor = pointer
                .strip_suffix("/content_type")
                .unwrap_or_else(|| panic!("{BODY_TYPE} landed outside a descriptor: {pointer}"));
            Value::String(descriptor.to_string())
        })
        .collect();
    json!({ "header": header, "slots": found, "bodies": bodies })
}

/// The config channel's `Deliver` of `document`: its header text and the
/// document's text, which the rig joins into the one binary message.
fn config_rung(document: &str) -> Value {
    let (header, bodies) = split_binary(deliver(&config_channel(), document));
    assert_eq!(bodies, document.as_bytes(), "the document is the one body");
    json!({ "header": header, "body": document })
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Push the JSON pointer of every place `sentinel` occurs under `value`.
fn find(value: &Value, sentinel: Sentinel, at: &mut String, out: &mut Vec<String>) {
    let hit = match (value, sentinel) {
        (Value::String(s), Sentinel::Text(t)) => s == t,
        (Value::Number(n), Sentinel::Number(m)) => n.as_u64() == Some(m),
        _ => false,
    };
    if hit {
        out.push(at.clone());
        return;
    }
    let len = at.len();
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                at.push('/');
                at.push_str(&key.replace('~', "~0").replace('/', "~1"));
                find(child, sentinel, at, out);
                at.truncate(len);
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                at.push('/');
                at.push_str(&i.to_string());
                find(child, sentinel, at, out);
                at.truncate(len);
            }
        }
        _ => {}
    }
}

/// The answer templates, by name.
fn answers() -> Value {
    json!({
        "subscribe_result": template(
            &ServerFrame::SubscribeResult {
                channel: text(CHANNEL),
                outcome: SubscribeOutcome::Ok,
                replay_count: u32::try_from(number(REPLAY_COUNT)).expect("fits u32"),
                gap: None,
            },
            &[CHANNEL, REPLAY_COUNT],
        ),
        "publish_result": template(
            &ServerFrame::PublishResult {
                correlation: Some(number(CORRELATION)),
                outcome: PublishOutcome::Ok,
            },
            &[CORRELATION],
        ),
        "publish_batch_result": template(
            &ServerFrame::PublishBatchResult {
                correlation: number(CORRELATION),
                outcome: PublishBatchOutcome::Ok,
            },
            &[CORRELATION],
        ),
    })
}

/// A one-row `Deliver` on `channel` of `body`, at `seq` under `id` with the
/// cursor `cursor`. The row is the shared builder's, so the envelope's fixed
/// fields are the ones every suite's peer writes, `ephemeral` type included.
fn one_row_deliver(
    channel: &str,
    body: MessageBody,
    seq: u64,
    cursor: &str,
    id: Uuid,
) -> ServerFrame {
    let mut r = row(channel, "", seq, id.as_u128());
    r.envelope.body = body;
    r.cursor = serde_json::from_value(Value::String(cursor.to_string()))
        .expect("a cursor is a JSON string");
    ServerFrame::Deliver {
        channel: channel.to_string(),
        rows: vec![r],
    }
}

/// The one-row `Deliver` template.
fn deliver_template() -> Value {
    let body = MessageBody::from_parts(Some(BODY_TYPE), b"slot").expect("a typed body");
    binary_template(
        &one_row_deliver(
            &text(CHANNEL),
            body,
            number(SEQ),
            &text(CURSOR),
            Uuid::parse_str(&text(MESSAGE_ID)).expect("the sentinel is a UUID"),
        ),
        &[CHANNEL, SEQ, CURSOR, MESSAGE_ID],
    )
}

/// The attachments the `deliver_attached` template's row names.
fn rig_attachments() -> Attachments {
    let named = |name: &str, id: &str, size: u64, content_type: &str| {
        (
            AttachmentName::new(name).expect("a rig attachment name"),
            BlobRef {
                id: BlobId::new(id.to_string()),
                size,
                content_type: content_type.to_string(),
            },
        )
    };
    Attachments::from_map(
        [
            named(
                "photo",
                "5f0c0000000000000000000000000001",
                102_400,
                "image/jpeg",
            ),
            named(
                "notes.txt",
                "5f0c0000000000000000000000000002",
                12,
                "text/plain",
            ),
        ]
        .into_iter()
        .collect(),
    )
}

/// The one-row `Deliver` template whose row names [`rig_attachments`].
fn deliver_attached_template() -> Value {
    let body = MessageBody::from_parts(Some(BODY_TYPE), b"slot").expect("a typed body");
    let mut frame = one_row_deliver(
        &text(CHANNEL),
        body,
        number(SEQ),
        &text(CURSOR),
        Uuid::parse_str(&text(MESSAGE_ID)).expect("the sentinel is a UUID"),
    );
    let ServerFrame::Deliver { rows, .. } = &mut frame else {
        unreachable!("one_row_deliver builds a Deliver");
    };
    rows[0].envelope.attachments = rig_attachments();
    binary_template(&frame, &[CHANNEL, SEQ, CURSOR, MESSAGE_ID])
}

/// One `Deliver` the codec encoded, with the values and body that produced
/// it, for the rig to rebuild from [`deliver_template`] and compare.
fn deliver_sample(channel: &str, body: MessageBody, seq: u64, cursor: &str, id: Uuid) -> Value {
    let message = frame(&one_row_deliver(channel, body.clone(), seq, cursor, id));
    let WireMessage::Binary(bytes) = &message else {
        panic!("a Deliver is a binary message");
    };
    json!({
        "template": "deliver",
        "values": {
            "channel": channel,
            "seq": seq,
            "cursor": cursor,
            "message_id": id.to_string(),
        },
        "bodies": [{ "hex": hex(body.as_bytes()), "content_type": body.content_type() }],
        "hex": hex(bytes),
    })
}

/// A `DeferredView` of `entries` parked by `attribution` on `channel`.
fn deferred_view(channel: &str, attribution: &str, entries: Vec<DeferredViewEntry>) -> ServerFrame {
    ServerFrame::DeferredView {
        channel: channel.to_string(),
        attribution: Some(attribution.to_string()),
        entries,
    }
}

/// The sentinel message id of each entry of a view template, by index.
const VIEW_MESSAGE_ID_SENTINELS: [&str; MAX_VIEW_ENTRIES] = [
    "51075107-5107-4107-8107-510751075100",
    "51075107-5107-4107-8107-510751075101",
    "51075107-5107-4107-8107-510751075102",
];

/// The sentinel message id of a view template's entry `i`.
fn view_message_id(i: usize) -> Uuid {
    Uuid::parse_str(VIEW_MESSAGE_ID_SENTINELS[i]).expect("the sentinel is a UUID")
}

/// The sentinel release instant of a view template's entry `i`.
fn view_deliver_after(i: usize) -> u64 {
    7_000_100_000 + u64::try_from(i).expect("an index fits u64")
}

/// The `DeferredView` templates, the one at index `n` holding `n` entries.
fn deferred_view_templates() -> Value {
    let body = MessageBody::from_parts(Some(BODY_TYPE), b"slot").expect("a typed body");
    Value::Array(
        (0..=MAX_VIEW_ENTRIES)
            .map(|n| {
                let entries = (0..n)
                    .map(|i| DeferredViewEntry {
                        message_id: view_message_id(i),
                        body: body.clone(),
                        deliver_after: view_deliver_after(i),
                    })
                    .collect();
                let mut slots = vec![CHANNEL, ATTRIBUTION];
                for i in 0..n {
                    slots.push((
                        VIEW_MESSAGE_IDS[i],
                        Sentinel::Text(VIEW_MESSAGE_ID_SENTINELS[i]),
                    ));
                    slots.push((
                        VIEW_DELIVER_AFTERS[i],
                        Sentinel::Number(view_deliver_after(i)),
                    ));
                }
                binary_template(
                    &deferred_view(&text(CHANNEL), &text(ATTRIBUTION), entries),
                    &slots,
                )
            })
            .collect(),
    )
}

/// One `DeferredView` the codec encoded, with the values and bodies that
/// produced it, for the rig to rebuild from the template of its size.
fn deferred_view_sample(channel: &str, attribution: &str, entries: &[DeferredViewEntry]) -> Value {
    let message = frame(&deferred_view(channel, attribution, entries.to_vec()));
    let WireMessage::Binary(bytes) = &message else {
        panic!("a DeferredView is a binary message");
    };
    let mut values = serde_json::Map::new();
    values.insert("channel".to_string(), json!(channel));
    values.insert("attribution".to_string(), json!(attribution));
    for (i, entry) in entries.iter().enumerate() {
        values.insert(
            VIEW_MESSAGE_IDS[i].to_string(),
            json!(entry.message_id.to_string()),
        );
        values.insert(
            VIEW_DELIVER_AFTERS[i].to_string(),
            json!(entry.deliver_after),
        );
    }
    let bodies: Vec<Value> = entries
        .iter()
        .map(|e| json!({ "hex": hex(e.body.as_bytes()), "content_type": e.body.content_type() }))
        .collect();
    json!({
        "template": "deferred_view",
        "values": values,
        "bodies": bodies,
        "hex": hex(bytes),
    })
}

/// The binary server samples: a non-UTF-8 typed row, an untyped text row, and
/// a two-entry `DeferredView` of a non-text and an untyped text body.
fn server_samples() -> Value {
    json!([
        deliver_sample(
            BYTES_FEED_CHANNEL,
            MessageBody::from_parts(Some("application/octet-stream"), &[0xff, 0x00, 0xfe])
                .expect("a non-text body"),
            3,
            "rig-3",
            Uuid::from_u128(0xabc),
        ),
        deliver_sample(
            FEED_CHANNEL,
            MessageBody::text("é中😀\u{1}\"\\{}"),
            12,
            "rig-12",
            Uuid::from_u128(0xdef),
        ),
        deferred_view_sample(
            BYTES_WIRE_CHANNEL,
            "b-keeper",
            &[
                DeferredViewEntry {
                    message_id: Uuid::from_u128(0x5a),
                    body: MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8, 0x00])
                        .expect("a non-text body"),
                    deliver_after: 4_102_444_800_000,
                },
                DeferredViewEntry {
                    message_id: Uuid::from_u128(0x5b),
                    body: MessageBody::text("é\"{}"),
                    deliver_after: 4_102_444_800_001,
                },
            ],
        ),
    ])
}

/// One client frame of each kind the peer reads, each field the peer reads
/// present, and every kind of body and control op a batch carries.
fn client_frames() -> Vec<ClientFrame> {
    let typed = |content_type: &str, bytes: &[u8]| {
        MessageBody::from_parts(Some(content_type), bytes).expect("a well-formed body")
    };
    vec![
        ClientFrame::Hello {
            versions: VersionRange::exactly(SURFACE_ATTACH_VERSION),
            ident: BUILD_ID.to_string(),
        },
        ClientFrame::Subscribe {
            channel: command_channel("keeper"),
            push_depth: 1,
            retain_depth: 1,
            resume: None,
        },
        ClientFrame::Publish {
            channel: REPORT_CHANNEL.to_string(),
            attribution: Some("keeper".to_string()),
            body: typed("text/csv", b"a,b"),
            urgency: Urgency::Normal,
            correlation: Some(1),
            attachments: Vec::new(),
        },
        ClientFrame::PublishBatch {
            attribution: Some("b-keeper".to_string()),
            correlation: 2,
            publishes: vec![
                BatchEntry {
                    channel: REPORT_CHANNEL.to_string(),
                    body: "{}".into(),
                    urgency: Urgency::Normal,
                    deliver_after: None,
                    attachments: Vec::new(),
                },
                BatchEntry {
                    channel: BYTES_OUT_CHANNEL.to_string(),
                    body: typed("text/markdown", "# é".as_bytes()),
                    urgency: Urgency::High,
                    deliver_after: None,
                    attachments: Vec::new(),
                },
                BatchEntry {
                    channel: BYTES_FEED_CHANNEL.to_string(),
                    body: typed("application/octet-stream", &[0xff, 0x00]),
                    urgency: Urgency::Normal,
                    deliver_after: Some(4_102_444_800_000),
                    attachments: Vec::new(),
                },
            ],
            deferred_ops: vec![
                BatchDeferredOp {
                    channel: BYTES_FEED_CHANNEL.to_string(),
                    message_id: Uuid::from_u128(1),
                    op: DeferredOpKind::Edit {
                        body: Some(typed("image/jpeg", &[0xff, 0xd8])),
                        deliver_after: Some(4_102_444_800_001),
                    },
                },
                BatchDeferredOp {
                    channel: BYTES_FEED_CHANNEL.to_string(),
                    message_id: Uuid::from_u128(2),
                    op: DeferredOpKind::Edit {
                        body: None,
                        deliver_after: Some(4_102_444_800_002),
                    },
                },
                BatchDeferredOp {
                    channel: FEED_CHANNEL.to_string(),
                    message_id: Uuid::from_u128(3),
                    op: DeferredOpKind::Cancel,
                },
            ],
        },
    ]
}

/// [`client_frames`] as the page writes them: `{ "text": … }` for a text
/// message, `{ "hex": … }` for a binary one.
fn client_samples() -> Value {
    Value::Array(
        client_frames()
            .iter()
            .map(|f| match client_frame(f) {
                WireMessage::Text(text) => json!({ "text": text }),
                WireMessage::Binary(bytes) => json!({ "hex": hex(&bytes) }),
            })
            .collect(),
    )
}

/// The whole kit, as the rig reads it.
pub fn kit() -> Value {
    json!({
        "pages": {
            "cases": page(CASES_BODY_CAP, CASES_CAST),
            "measure": page(MEASURE_BODY_CAP, MEASURE_CAST),
        },
        "answers": answers(),
        "deliver": deliver_template(),
        "deliver_attached": deliver_attached_template(),
        "attached": serde_json::to_value(rig_attachments()).expect("attachments serialize"),
        "deferred_views": deferred_view_templates(),
        "server_samples": server_samples(),
        "client_samples": client_samples(),
    })
}

/// A text message's JSON.
fn parse(message: WireMessage) -> Value {
    let WireMessage::Text(text) = message else {
        panic!("a frame of a text kind is a text message");
    };
    serde_json::from_str(&text).expect("a serialized frame is JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `template` with each named slot's pointers set to the given value.
    fn fill(template: &Value, values: &[(&str, Value)]) -> ServerFrame {
        let mut frame = template["frame"].clone();
        fill_slots(&mut frame, template, values);
        crate::frames::read_server_frame(WireMessage::Text(frame.to_string()))
    }

    /// A binary `template` filled with `values` and carrying `bodies`, joined
    /// as the rig joins it, and read back.
    fn fill_binary(
        template: &Value,
        values: &[(&str, Value)],
        bodies: &[MessageBody],
    ) -> ServerFrame {
        let header_text = template["header"].as_str().expect("the header is text");
        let mut header: Value = serde_json::from_str(header_text).expect("the header is JSON");
        fill_slots(&mut header, template, values);
        let pointers = template["bodies"].as_array().expect("bodies is a list");
        assert_eq!(pointers.len(), bodies.len(), "one body per descriptor");
        for (pointer, body) in pointers.iter().zip(bodies) {
            let pointer = pointer.as_str().expect("a pointer is a string");
            let mut descriptor = json!({ "len": body.len() });
            if let Some(content_type) = body.content_type() {
                descriptor["content_type"] = json!(content_type);
            }
            *header
                .pointer_mut(pointer)
                .unwrap_or_else(|| panic!("{pointer} is in the header")) = descriptor;
        }
        crate::frames::read_server_frame(join(&header.to_string(), bodies))
    }

    /// A binary message of `header` and `bodies`.
    fn join(header: &str, bodies: &[MessageBody]) -> WireMessage {
        let mut bytes = u32::try_from(header.len())
            .expect("a header fits u32")
            .to_be_bytes()
            .to_vec();
        bytes.extend_from_slice(header.as_bytes());
        for body in bodies {
            bytes.extend_from_slice(body.as_bytes());
        }
        WireMessage::Binary(bytes)
    }

    fn unhex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
            .collect()
    }

    /// Set each named slot's pointers in `frame` to the given value.
    fn fill_slots(frame: &mut Value, template: &Value, values: &[(&str, Value)]) {
        let slots = template["slots"].as_object().expect("slots is an object");
        assert_eq!(
            slots.len(),
            values.len(),
            "every slot of {template} is filled, and no other"
        );
        for (name, value) in values {
            let pointers = slots[*name].as_array().expect("a slot is a pointer list");
            for pointer in pointers {
                let pointer = pointer.as_str().expect("a pointer is a string");
                *frame
                    .pointer_mut(pointer)
                    .unwrap_or_else(|| panic!("{pointer} is in the frame")) = value.clone();
            }
        }
    }

    /// The template a server sample is rebuilt from: `deliver`, or the
    /// `DeferredView` template of the sample's size.
    fn template_of<'k>(kit: &'k Value, sample: &Value, bodies: usize) -> &'k Value {
        match sample["template"].as_str().expect("a template name") {
            "deliver" => &kit["deliver"],
            "deferred_view" => &kit["deferred_views"][bodies],
            other => panic!("the kit has no template {other:?}"),
        }
    }

    /// The kit speaks the surface route's version, and each binary server
    /// sample is the frame its values and bodies describe, rebuilt from the
    /// template.
    #[test]
    fn the_kit_speaks_the_route_version_and_its_server_samples_decode() {
        assert_eq!(facts(CASES_BODY_CAP).version, SURFACE_ATTACH_VERSION);
        let kit = kit();
        let samples = kit["server_samples"].as_array().expect("a list");
        let names: Vec<&str> = samples
            .iter()
            .map(|s| s["template"].as_str().expect("a template name"))
            .collect();
        assert_eq!(names, ["deliver", "deliver", "deferred_view"]);
        for sample in samples {
            let values: Vec<(&str, Value)> = sample["values"]
                .as_object()
                .expect("values")
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let bodies: Vec<MessageBody> = sample["bodies"]
                .as_array()
                .expect("bodies")
                .iter()
                .map(|b| {
                    MessageBody::from_parts(
                        b["content_type"].as_str(),
                        &unhex(b["hex"].as_str().expect("hex")),
                    )
                    .expect("a sample body is well formed")
                })
                .collect();
            let encoded = crate::frames::read_server_frame(WireMessage::Binary(unhex(
                sample["hex"].as_str().expect("hex"),
            )));
            let rebuilt = fill_binary(template_of(&kit, sample, bodies.len()), &values, &bodies);
            assert_eq!(rebuilt, encoded);
            let carried: Vec<MessageBody> = match encoded {
                ServerFrame::Deliver { rows, .. } => {
                    rows.into_iter().map(|r| r.envelope.body).collect()
                }
                ServerFrame::DeferredView { entries, .. } => {
                    entries.into_iter().map(|e| e.body).collect()
                }
                other => panic!("a server sample is a Deliver or a DeferredView: {other:?}"),
            };
            assert_eq!(carried, bodies);
        }
        let first = &samples[0]["bodies"][0];
        assert_eq!(first["content_type"], "application/octet-stream");
        assert!(samples[1]["bodies"][0]["content_type"].is_null());
        let view = samples[2]["bodies"].as_array().expect("bodies");
        assert_eq!(view.len(), 2, "the view sample holds two entries");
        assert_eq!(view[0]["content_type"], "image/jpeg");
        assert!(view[1]["content_type"].is_null());
    }

    /// Each `DeferredView` template holds the entries its index says, and
    /// fills to the view its values describe.
    #[test]
    fn each_deferred_view_template_fills_every_variable_field() {
        let kit = kit();
        let templates = kit["deferred_views"].as_array().expect("a list");
        assert_eq!(templates.len(), MAX_VIEW_ENTRIES + 1);
        for (n, template) in templates.iter().enumerate() {
            let entries: Vec<DeferredViewEntry> = (0..n)
                .map(|i| DeferredViewEntry {
                    message_id: Uuid::from_u128(0x900 + i as u128),
                    body: MessageBody::from_parts(
                        Some("image/png"),
                        &[0x89, 0x50, u8::try_from(i).expect("small")],
                    )
                    .expect("a non-text body"),
                    deliver_after: 4_102_444_800_000 + i as u64,
                })
                .collect();
            let mut values = vec![
                ("channel", json!(BYTES_WIRE_CHANNEL)),
                ("attribution", json!("b-keeper")),
            ];
            for (i, e) in entries.iter().enumerate() {
                values.push((VIEW_MESSAGE_IDS[i], json!(e.message_id.to_string())));
                values.push((VIEW_DELIVER_AFTERS[i], json!(e.deliver_after)));
            }
            let bodies: Vec<MessageBody> = entries.iter().map(|e| e.body.clone()).collect();
            assert_eq!(
                fill_binary(template, &values, &bodies),
                deferred_view(BYTES_WIRE_CHANNEL, "b-keeper", entries),
                "the {n}-entry template"
            );
        }
    }

    #[test]
    fn the_subscribe_result_template_fills_channel_and_replay_count() {
        let filled = fill(
            &kit()["answers"]["subscribe_result"],
            &[
                ("channel", json!("ephemeral:rig.x")),
                ("replay_count", json!(3)),
            ],
        );
        assert_eq!(
            filled,
            ServerFrame::SubscribeResult {
                channel: "ephemeral:rig.x".to_string(),
                outcome: SubscribeOutcome::Ok,
                replay_count: 3,
                gap: None,
            }
        );
    }

    #[test]
    fn the_publish_result_templates_fill_the_correlation() {
        let answers = &kit()["answers"];
        assert_eq!(
            fill(&answers["publish_result"], &[("correlation", json!(11))]),
            ServerFrame::PublishResult {
                correlation: Some(11),
                outcome: PublishOutcome::Ok,
            }
        );
        assert_eq!(
            fill(
                &answers["publish_batch_result"],
                &[("correlation", json!(12))]
            ),
            ServerFrame::PublishBatchResult {
                correlation: 12,
                outcome: PublishBatchOutcome::Ok,
            }
        );
    }

    #[test]
    fn the_deliver_template_fills_every_variable_field() {
        let id = Uuid::from_u128(0x1234);
        let body = MessageBody::from_parts(Some("text/csv"), "é,中".as_bytes()).expect("typed");
        let filled = fill_binary(
            &kit()["deliver"],
            &[
                ("channel", json!(FEED_CHANNEL)),
                ("seq", json!(5)),
                ("cursor", json!("k5")),
                ("message_id", json!(id.to_string())),
            ],
            std::slice::from_ref(&body),
        );
        let ServerFrame::Deliver { channel, rows } = filled else {
            panic!("the deliver template is a Deliver");
        };
        assert_eq!(channel, FEED_CHANNEL);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.envelope.channel, FEED_CHANNEL);
        assert_eq!(r.envelope.body, body);
        assert_eq!(r.envelope.message_id, id);
        assert_eq!(r.seq, 5);
        assert_eq!(
            serde_json::to_value(&r.cursor).expect("serializes"),
            json!("k5")
        );
    }

    #[test]
    fn the_attached_deliver_template_carries_the_kits_attachments() {
        let kit = kit();
        let body = MessageBody::from_parts(Some("text/csv"), b"a,b").expect("typed");
        let filled = fill_binary(
            &kit["deliver_attached"],
            &[
                ("channel", json!(FEED_CHANNEL)),
                ("seq", json!(6)),
                ("cursor", json!("k6")),
                ("message_id", json!(Uuid::from_u128(0x99).to_string())),
            ],
            std::slice::from_ref(&body),
        );
        let ServerFrame::Deliver { rows, .. } = filled else {
            panic!("the attached deliver template is a Deliver");
        };
        assert_eq!(rows[0].envelope.attachments, rig_attachments());
        assert_eq!(rows[0].envelope.body, body);
        assert_eq!(
            kit["attached"]["photo"],
            json!({
                "id": "5f0c0000000000000000000000000001",
                "size": 102_400,
                "content_type": "image/jpeg",
            })
        );
    }

    #[test]
    fn each_page_ladder_parses_and_its_document_validates() {
        let kit = kit();
        for (name, cap, cast) in [
            ("cases", CASES_BODY_CAP, CASES_CAST),
            ("measure", MEASURE_BODY_CAP, MEASURE_CAST),
        ] {
            let ladder = &kit["pages"][name]["ladder"];
            let hello: ServerFrame =
                crate::frames::read_server_frame(WireMessage::Text(ladder["hello"].to_string()));
            assert!(
                matches!(hello, ServerFrame::Hello { versions, .. } if versions == VersionRange::exactly(SURFACE_ATTACH_VERSION))
            );
            let welcome: ServerFrame =
                crate::frames::read_server_frame(WireMessage::Text(ladder["welcome"].to_string()));
            let ServerFrame::Welcome { max_body_bytes, .. } = welcome else {
                panic!("{name}: the second rung is a Welcome");
            };
            assert_eq!(max_body_bytes, cap, "{name} states its body cap");
            let rung = &ladder["config_deliver"];
            let deliver: ServerFrame = crate::frames::read_server_frame(join(
                rung["header"].as_str().expect("the header is text"),
                &[MessageBody::text(
                    rung["body"].as_str().expect("the body is text"),
                )],
            ));
            let ServerFrame::Deliver { channel, rows } = deliver else {
                panic!("{name}: the third rung is a Deliver");
            };
            assert_eq!(channel, config_channel());
            assert_eq!(rows.len(), 1);
            let text = rows[0]
                .envelope
                .body
                .as_text()
                .expect("a bindings document is text");
            let doc = BindingsDocument::parse(text).expect("the document parses");
            doc.validate().expect("the document validates");
            assert_eq!(doc, document(cast));
            for (stater, _) in cast.staters.all() {
                assert!(
                    doc.local_channels
                        .iter()
                        .any(|l| l.channel == state_channel(stater)),
                    "{name}: {stater} has a page-local state channel"
                );
            }
            assert_eq!(doc.calls[0], call("keeper", "peer", "responder", "ask"));
        }
    }

    /// The `cases` page's crossers are wired as the cases that drive them
    /// assume: each call to its callee, `b-keeper`'s typed keeping shared with a
    /// string-contract reader, and its `out` and `feed` on the wire.
    #[test]
    fn the_cases_page_wires_its_contract_crossers() {
        let doc = document(CASES_CAST);
        assert_eq!(
            doc.calls,
            vec![
                call("keeper", "peer", "responder", "ask"),
                call("b-keeper", "peer", "b-responder", "ask"),
                call("b-mixed", "peer", "responder", "ask"),
                call("caller", "peer", "b-responder", "ask"),
            ]
        );
        let bound = |instance: &str, port: &str| {
            doc.subscriptions
                .iter()
                .find(|b| b.instance == instance && b.port == port)
                .unwrap_or_else(|| panic!("{instance}.{port} is subscribed"))
                .channel
                .clone()
        };
        assert_eq!(bound("b-keeper", "memo"), SHARED_CHANNEL);
        assert_eq!(bound("plain", "feed"), SHARED_CHANNEL);
        assert_eq!(bound("b-keeper", "feed"), BYTES_FEED_CHANNEL);
        assert_eq!(bound("b-keeper", "timer"), BYTES_TIMER_CHANNEL);
        assert!(doc.outputs.iter().any(|o| o.instance == "b-keeper"
            && o.port == "timer"
            && o.channel == BYTES_TIMER_CHANNEL));
        assert!(doc.outputs.iter().any(|o| o.instance == "b-keeper"
            && o.port == "out"
            && o.channel == BYTES_OUT_CHANNEL));
        assert!(doc.outputs.iter().any(|o| o.instance == "b-keeper"
            && o.port == "bout"
            && o.channel == BYTES_WIRE_CHANNEL));
        let sync_ports = |instance: &str| {
            doc.components
                .iter()
                .find(|c| c.instance == instance)
                .unwrap_or_else(|| panic!("the document has {instance}"))
                .sync_ports
                .clone()
        };
        assert_eq!(sync_ports("b-responder"), ["ask"]);
        assert_eq!(sync_ports("responder"), ["ask"]);
        assert!(sync_ports("b-mixed").is_empty());
        assert!(
            document(MEASURE_CAST).calls.len() == 1,
            "the measure page crosses nothing"
        );
    }

    /// Which channels each page declares `non_text`: page-local, `b-keeper`'s
    /// `timer` and every 0.2.0 state-holder's channel; on the wire,
    /// `b-keeper`'s `feed` and `bout` and `b-stream`'s `stream`; and nothing a
    /// 0.1.0 instance is bound to.
    #[test]
    fn each_page_declares_non_text_only_where_a_0_2_0_holder_keeps_bytes() {
        let declared_wire = |cast: Cast| -> Vec<String> {
            document(cast)
                .wire_channels
                .into_iter()
                .filter(|w| w.non_text)
                .map(|w| w.channel)
                .collect()
        };
        assert_eq!(
            declared_wire(CASES_CAST),
            [BYTES_FEED_CHANNEL, BYTES_WIRE_CHANNEL]
        );
        assert_eq!(declared_wire(MEASURE_CAST), [STREAM_CHANNEL]);
        let declared = |cast: Cast| -> Vec<String> {
            document(cast)
                .local_channels
                .into_iter()
                .filter(|l| l.non_text)
                .map(|l| l.channel)
                .collect()
        };
        assert_eq!(
            declared(CASES_CAST),
            [state_channel("s-stater"), BYTES_TIMER_CHANNEL.to_string()]
        );
        assert_eq!(
            declared(MEASURE_CAST),
            ["b-stater-1", "b-stater-2", "b-stater-3"].map(state_channel)
        );
        let cases = document(CASES_CAST);
        assert!(
            cases
                .local_channels
                .iter()
                .any(|l| l.channel == SHARED_CHANNEL && !l.non_text),
            "the shared channel stays text-only"
        );
        for doc in [cases, document(MEASURE_CAST)] {
            let string_instances: Vec<&str> = doc
                .components
                .iter()
                .filter(|c| c.kind == STRING_KIND)
                .map(|c| c.instance.as_str())
                .collect();
            let bound = doc
                .subscriptions
                .iter()
                .map(|b| (&b.instance, &b.channel))
                .chain(doc.outputs.iter().map(|o| (&o.instance, &o.channel)));
            for (instance, channel) in bound {
                if string_instances.contains(&instance.as_str()) {
                    assert!(
                        !doc.local_channels
                            .iter()
                            .any(|l| &l.channel == channel && l.non_text)
                            && !doc
                                .wire_channels
                                .iter()
                                .any(|w| &w.channel == channel && w.non_text),
                        "0.1.0 {instance} is bound to the non_text channel {channel}"
                    );
                }
            }
        }
    }

    #[test]
    fn each_instance_states_the_contract_of_its_kind() {
        let kit = kit();
        let measure = kit["pages"]["measure"]["instances"]
            .as_array()
            .expect("instances is a list");
        let contract_of = |name: &str| {
            measure
                .iter()
                .find(|i| i["instance"] == name)
                .unwrap_or_else(|| panic!("measure has {name}"))["contract"]
                .clone()
        };
        assert_eq!(contract_of("keeper"), json!("brenn:processor@0.1.0"));
        assert_eq!(contract_of("stater-1"), json!("brenn:processor@0.1.0"));
        assert_eq!(contract_of("b-stater-1"), json!("brenn:processor@0.2.0"));
        let doc = document(MEASURE_CAST);
        let kind_of = |name: &str| {
            doc.components
                .iter()
                .find(|c| c.instance == name)
                .unwrap_or_else(|| panic!("the document has {name}"))
                .kind
                .clone()
        };
        assert_eq!(kind_of("stater-1"), STRING_KIND);
        assert_eq!(kind_of("b-stater-1"), BYTES_KIND);
        let cases = kit["pages"]["cases"]["instances"]
            .as_array()
            .expect("instances is a list");
        let s_stater = cases
            .iter()
            .find(|i| i["instance"] == "s-stater")
            .expect("cases has s-stater");
        assert_eq!(s_stater["kind"], json!(SDK_BYTES_KIND));
        assert_eq!(s_stater["contract"], json!("brenn:processor@0.2.0"));
    }

    #[test]
    fn the_client_samples_are_one_of_each_frame_the_peer_reads() {
        let samples = kit()["client_samples"].clone();
        let frames: Vec<ClientFrame> = samples
            .as_array()
            .expect("the samples are an array")
            .iter()
            .map(|sample| {
                let message = match (sample["text"].as_str(), sample["hex"].as_str()) {
                    (Some(text), None) => WireMessage::Text(text.to_string()),
                    (None, Some(hex)) => WireMessage::Binary(unhex(hex)),
                    _ => panic!("a sample is text or hex: {sample}"),
                };
                crate::frames::read_client_frame(message)
            })
            .collect();
        assert_eq!(
            frames,
            client_frames(),
            "each sample is the frame it was built from"
        );
        let kinds: Vec<&str> = frames
            .iter()
            .map(|f| match f {
                ClientFrame::Hello { .. } => "hello",
                ClientFrame::Subscribe { .. } => "subscribe",
                ClientFrame::Publish { .. } => "publish",
                ClientFrame::PublishBatch { deferred_ops, .. } => {
                    assert!(!deferred_ops.is_empty(), "the batch sample states its ops");
                    "publish_batch"
                }
                other => panic!("the peer reads no {other:?}"),
            })
            .collect();
        assert_eq!(kinds, ["hello", "subscribe", "publish", "publish_batch"]);
    }
}
