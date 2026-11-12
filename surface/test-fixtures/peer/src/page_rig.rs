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
//! The kit holds:
//!
//! - `pages`: the named pages, `cases` and `measure`. Each states the three
//!   page metas, the location, its instances with their kind, contract and
//!   command channel, the report channel, its state-holding instances per
//!   contract, its body cap and its opening ladder (`Hello`, `Welcome`, and the
//!   config channel's `Deliver` carrying the bindings document). The two differ
//!   in the `Welcome` and in their cast: `cases` has one state-holder and the
//!   instances that cross the two contracts, `measure` one state-holder per
//!   measured size on each contract, since a retained state is of one size and
//!   no page-local channel can be cleared.
//! - `answers`: templates for an accepted `SubscribeResult`, `PublishResult`
//!   and `PublishBatchResult`.
//! - `deliver`: a template for a one-row `Deliver` on an `ephemeral:` channel.
//! - `client_samples`: one serialized client frame of each kind the peer reads,
//!   so the rig's reader is checked against the names Rust writes.

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, ClientFrame, Codec, DeferredOpKind, PublishBatchOutcome,
    PublishOutcome, ServerFrame, SubscribeOutcome, Urgency, VersionRange,
};
use brenn_envelope::grants::ProcessorContract;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{ComponentGrant, SURFACE_ATTACH_VERSION};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::bindings::{call, component_with_grants, doc, local, output, subscription};
use crate::frames::{deliver, row, server_hello, welcome};

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
/// The wire channel `b-keeper`'s `out` port is bound to.
pub const BYTES_OUT_CHANNEL: &str = "ephemeral:rig.bytes-out";
/// The wire channel `b-keeper`'s `feed` port is subscribed to.
pub const BYTES_FEED_CHANNEL: &str = "ephemeral:rig.bytes-feed";
/// The body cap the `cases` page's `Welcome` states.
pub const CASES_BODY_CAP: u64 = 65_536;
/// The body cap the `measure` page's `Welcome` states: the per-activation
/// publish aggregate, which is the binding limit above it.
pub const MEASURE_BODY_CAP: u64 = 4_194_304;

/// The instances on every rig page, the chrome stand-in first, all of
/// [`STRING_KIND`]. Each page adds its state-holders after them.
pub const INSTANCES: [&str; 4] = ["chrome", "keeper", "reader", "responder"];

/// One page's state-holders: those of [`STRING_KIND`], then those of
/// [`BYTES_KIND`].
#[derive(Clone, Copy)]
pub struct Staters {
    pub string: &'static [&'static str],
    pub bytes: &'static [&'static str],
}

impl Staters {
    /// Every state-holder with its kind, in page order.
    fn all(self) -> impl Iterator<Item = (&'static str, &'static str)> {
        let string = self.string.iter().map(|s| (*s, STRING_KIND));
        let bytes = self.bytes.iter().map(|s| (*s, BYTES_KIND));
        string.chain(bytes)
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
/// - `b-keeper` keeps typed bodies on [`SHARED_CHANNEL`], publishes on the wire
///   channel [`BYTES_OUT_CHANNEL`], reads the wire channel
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

/// The `cases` page's cast: one state-holder and the contract crossers.
pub const CASES_CAST: Cast = Cast {
    staters: Staters {
        string: &["stater"],
        bytes: &[],
    },
    crossers: CROSSERS,
};
/// The `measure` page's cast: on each contract, one state-holder per size the
/// state workload is timed at.
pub const MEASURE_CAST: Cast = Cast {
    staters: Staters {
        string: &["stater-1", "stater-2", "stater-3"],
        bytes: &["b-stater-1", "b-stater-2", "b-stater-3"],
    },
    crossers: &[],
};

/// The contract `kind` is built against.
pub fn kind_contract(kind: &str) -> ProcessorContract {
    match kind {
        STRING_KIND => ProcessorContract::V0_1,
        BYTES_KIND => ProcessorContract::V0_2,
        other => panic!("the rig stages no kind {other:?}"),
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
    let grants = [ComponentGrant::Ports, ComponentGrant::Calls];
    let all = instances(cast);
    let mut components: Vec<_> = all
        .iter()
        .map(|(instance, kind)| component_with_grants(instance, kind, &grants))
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
    // Each state-holder: the state workload's io port, on a channel of its own.
    let mut locals = vec![local(MEMO_CHANNEL, 1), local(TIMER_CHANNEL, 1)];
    for (stater, _) in staters.all() {
        let channel = state_channel(stater);
        outputs.push(output(stater, "state", &channel));
        subscriptions.push(subscription(stater, "state", &channel, 0, 1));
        locals.push(local(&channel, 1));
    }
    let mut calls = vec![call("keeper", "peer", "responder", "ask")];
    let crosses = |instance: &str| cast.crossers.iter().any(|(i, _)| *i == instance);
    if crosses("b-keeper") {
        outputs.push(output("b-keeper", "memo", SHARED_CHANNEL));
        subscriptions.push(subscription("b-keeper", "memo", SHARED_CHANNEL, 0, 1));
        outputs.push(output("b-keeper", "out", BYTES_OUT_CHANNEL));
        subscriptions.push(subscription("b-keeper", "feed", BYTES_FEED_CHANNEL, 1, 1));
        locals.push(local(SHARED_CHANNEL, 1));
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
        "shared_channel": SHARED_CHANNEL,
        "state_instances": staters.string,
        "bytes_state_instances": staters.bytes,
        "status_channel": document.platform.status_channel,
        "max_body_bytes": max_body_bytes,
        "ladder": {
            "hello": parse(&server_hello()),
            "welcome": parse(&welcome(facts(max_body_bytes))),
            "config_deliver": parse(&deliver(&config_channel(), &document.to_body())),
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
const BODY: (&str, Sentinel) = ("body", Sentinel::Text("@slot:body"));
const CURSOR: (&str, Sentinel) = ("cursor", Sentinel::Text("@slot:cursor"));
const MESSAGE_ID: (&str, Sentinel) = (
    "message_id",
    Sentinel::Text("51075107-5107-4107-8107-510751075107"),
);
const SEQ: (&str, Sentinel) = ("seq", Sentinel::Number(7_000_000_001));
const CORRELATION: (&str, Sentinel) = ("correlation", Sentinel::Number(7_000_000_002));
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

/// `{ "frame": …, "slots": { name: [pointer, …] } }` for `frame`, each slot's
/// pointers being every place its sentinel landed. A slot whose sentinel
/// landed nowhere is a template that cannot be filled, and panics.
fn template(frame: &ServerFrame, slots: &[(&str, Sentinel)]) -> Value {
    let value: Value =
        serde_json::from_str(&crate::frames::frame(frame)).expect("a server frame is JSON");
    let mut found = serde_json::Map::new();
    for &(name, sentinel) in slots {
        let mut pointers = Vec::new();
        find(&value, sentinel, &mut String::new(), &mut pointers);
        assert!(
            !pointers.is_empty(),
            "slot {name:?} landed nowhere in {value}"
        );
        found.insert(
            name.to_string(),
            Value::Array(pointers.into_iter().map(Value::String).collect()),
        );
    }
    json!({ "frame": value, "slots": found })
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

/// The one-row `Deliver` template. The row is the shared builder's, so the
/// envelope's fixed fields are the ones every suite's peer writes, `ephemeral`
/// type included.
fn deliver_template() -> Value {
    let channel = text(CHANNEL);
    let mut r = row(
        &channel,
        &text(BODY),
        number(SEQ),
        Uuid::parse_str(&text(MESSAGE_ID))
            .expect("the sentinel is a UUID")
            .as_u128(),
    );
    r.cursor =
        serde_json::from_value(Value::String(text(CURSOR))).expect("a cursor is a JSON string");
    template(
        &ServerFrame::Deliver {
            channel,
            rows: vec![r],
        },
        &[CHANNEL, BODY, SEQ, CURSOR, MESSAGE_ID],
    )
}

/// One client frame of each kind the peer reads, each field the peer reads
/// present.
fn client_samples() -> Value {
    let samples = [
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
            body: "{}".into(),
            urgency: Urgency::Normal,
            correlation: Some(1),
        },
        ClientFrame::PublishBatch {
            attribution: Some("keeper".to_string()),
            correlation: 2,
            publishes: vec![BatchEntry {
                channel: REPORT_CHANNEL.to_string(),
                body: "{}".into(),
                urgency: Urgency::Normal,
                deliver_after: None,
            }],
            deferred_ops: vec![BatchDeferredOp {
                channel: FEED_CHANNEL.to_string(),
                message_id: Uuid::from_u128(1),
                op: DeferredOpKind::Cancel,
            }],
        },
    ];
    Value::Array(
        samples
            .iter()
            .map(|f| {
                serde_json::from_str(&crate::frames::client_frame(f))
                    .expect("a client frame is JSON")
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
        "client_samples": client_samples(),
    })
}

fn parse(frame_text: &str) -> Value {
    serde_json::from_str(frame_text).expect("a serialized frame is JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::MessageBody;

    /// `template` with each named slot's pointers set to the given value.
    fn fill(template: &Value, values: &[(&str, Value)]) -> ServerFrame {
        let mut frame = template["frame"].clone();
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
        crate::frames::read_server_frame(&frame.to_string())
    }

    /// The page stays on attach v4, so every frame of the kit is a v4 text
    /// message: `frames::frame` and `frames::client_frame` panic on anything
    /// else, and building the kit runs every frame the rig is handed through
    /// one of them.
    #[test]
    fn every_kit_frame_is_a_text_message_at_version_4() {
        assert_eq!(SURFACE_ATTACH_VERSION, 4);
        assert_eq!(facts(CASES_BODY_CAP).version, 4);
        let kit = kit();
        assert!(kit["answers"].is_object());
        assert!(
            !kit["client_samples"]
                .as_array()
                .expect("the client samples are a list")
                .is_empty()
        );
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
        let body = "é中😀\u{1}\"\\{}";
        let filled = fill(
            &kit()["deliver"],
            &[
                ("channel", json!(FEED_CHANNEL)),
                ("body", json!(body)),
                ("seq", json!(5)),
                ("cursor", json!("k5")),
                ("message_id", json!(id.to_string())),
            ],
        );
        let ServerFrame::Deliver { channel, rows } = filled else {
            panic!("the deliver template is a Deliver");
        };
        assert_eq!(channel, FEED_CHANNEL);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.envelope.channel, FEED_CHANNEL);
        assert_eq!(r.envelope.body, MessageBody::from(body));
        assert_eq!(r.envelope.message_id, id);
        assert_eq!(r.seq, 5);
        assert_eq!(
            serde_json::to_value(&r.cursor).expect("serializes"),
            json!("k5")
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
            let hello: ServerFrame = crate::frames::read_server_frame(&ladder["hello"].to_string());
            assert!(
                matches!(hello, ServerFrame::Hello { versions, .. } if versions == VersionRange::exactly(SURFACE_ATTACH_VERSION))
            );
            let welcome: ServerFrame =
                crate::frames::read_server_frame(&ladder["welcome"].to_string());
            let ServerFrame::Welcome { max_body_bytes, .. } = welcome else {
                panic!("{name}: the second rung is a Welcome");
            };
            assert_eq!(max_body_bytes, cap, "{name} states its body cap");
            let deliver: ServerFrame =
                crate::frames::read_server_frame(&ladder["config_deliver"].to_string());
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
        assert!(doc.outputs.iter().any(|o| o.instance == "b-keeper"
            && o.port == "out"
            && o.channel == BYTES_OUT_CHANNEL));
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
    }

    #[test]
    fn the_client_samples_are_one_of_each_frame_the_peer_reads() {
        let samples = kit()["client_samples"].clone();
        let frames: Vec<ClientFrame> = samples
            .as_array()
            .expect("the samples are an array")
            .iter()
            .map(|sample| crate::frames::read_client_frame(&sample.to_string()))
            .collect();
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
