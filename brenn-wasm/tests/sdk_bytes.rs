// The guest SDK on the byte-carrying contract, through a real component on the
// backend host: `processor-sdk-bytes-test` keeps a binary retained state through
// `RetainedState` with its own `StateCodec`, and reports how the SDK's body
// accessors read the messages on its `feed` port.
//
// Each test drives activations by hand: the state window an activation is
// handed is the body the previous one published on `state`, which is what the
// channel would retain at `retain_depth = 1`.

use std::collections::{BTreeSet, HashMap};

use brenn_activation::Activation;
use brenn_envelope::MessageBody;
use brenn_wasm::{
    ComponentGrant, HostActivation, OutputPortSpec, ProcessorComponent, ProcessorLoadSpec,
    ProcessorOutcome, ProcessorPublish,
};
use serde_json::{Value, json};

mod common;

/// The content type the fixture's codec publishes its state under.
const OCTET_STREAM: &str = "application/octet-stream";

/// The fixture with `state` bound to a channel that carries non-text bodies iff
/// `non_text`, and `report` to a text-only channel.
fn load(non_text: bool) -> ProcessorComponent {
    ProcessorComponent::load(ProcessorLoadSpec {
        output_ports: HashMap::from([
            ("report".to_string(), common::out_spec("brenn:report")),
            (
                "state".to_string(),
                OutputPortSpec {
                    non_text,
                    ..common::out_spec("brenn:state")
                },
            ),
        ]),
        declared_out_ports: ["report", "state"]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
        input_amplification_mt: ["in", "feed", "state"]
            .into_iter()
            .map(|p| (p.to_string(), 1_000))
            .collect(),
        grants: [ComponentGrant::Ports, ComponentGrant::Log]
            .into_iter()
            .collect(),
        max_payload_bytes: 1024 * 1024,
        ..ProcessorLoadSpec::minimal(
            &common::artifact_path("brenn_processor_sdk_bytes_test"),
            "sdk-bytes-test",
        )
    })
}

/// An activation whose `in` window carries `commands` as one new text message
/// and whose `state` window holds `state` as context, as the retained channel
/// hands it over.
fn activation(commands: &str, state: Option<MessageBody>) -> HostActivation {
    let state: Vec<_> = state
        .into_iter()
        .map(|b| common::envelope("brenn:state", "5a", b))
        .collect();
    let held = u32::try_from(state.len()).expect("a one-entry window");
    Activation {
        ports: vec![
            common::window(
                "in",
                vec![common::envelope(
                    "brenn:in",
                    "01",
                    MessageBody::text(commands),
                )],
                0,
            ),
            common::window("state", state, held),
        ],
        deferred: vec![],
        now: Some(1_000),
        sync: None,
    }
}

/// The state publish of an ok outcome, if any, and its parsed report.
fn ok(outcome: ProcessorOutcome) -> (Option<ProcessorPublish>, Value) {
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("expected an ok outcome, got {outcome:?}");
    };
    let mut state = None;
    let mut report = None;
    for publish in publishes {
        match publish.port.as_str() {
            "state" => {
                assert!(state.is_none(), "one state publish per activation");
                state = Some(publish);
            }
            "report" => {
                let text = publish.body.as_text().expect("the report is text");
                report = Some(serde_json::from_str(text).expect("the report is JSON"));
            }
            other => panic!("a publish on an unexpected port {other:?}"),
        }
    }
    (
        state,
        report.expect("every ok activation publishes a report"),
    )
}

/// The diagnostic of an err outcome.
fn err(outcome: ProcessorOutcome) -> String {
    let ProcessorOutcome::Err(e) = outcome else {
        panic!("expected an err outcome, got {outcome:?}");
    };
    format!("{e:?}")
}

/// The oracle: the `len` bytes the fixture creates, byte i being
/// (i * 31 + 7) mod 256, with `steps` added to the little-endian `u32` in
/// bytes 0..4.
fn expected_state(len: usize, steps: u32) -> Vec<u8> {
    let mut bytes: Vec<u8> = (0..len).map(|i| ((i * 31 + 7) % 256) as u8).collect();
    let head: [u8; 4] = bytes[..4].try_into().expect("four bytes");
    let step = u32::from_le_bytes(head).wrapping_add(steps);
    bytes[..4].copy_from_slice(&step.to_le_bytes());
    bytes
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn result_word(bytes: &[u8]) -> String {
    format!("state:{}:{:016x}", bytes.len(), fnv1a64(bytes))
}

#[test]
fn a_binary_state_is_created_and_stepped_byte_for_byte() {
    let component = load(true);
    let mut stored: Option<MessageBody> = None;
    for step in 0..3 {
        let (state, report) = ok(component.handle(activation("state 1024", stored.take())));
        let expected = expected_state(1024, step);
        assert!(
            std::str::from_utf8(&expected).is_err(),
            "the state is not UTF-8"
        );
        assert_eq!(common::results(&report), [result_word(&expected)]);
        let state = state.expect("the stepped state is published");
        assert_eq!(state.channel_address, "brenn:state");
        assert_eq!(state.body.content_type(), Some(OCTET_STREAM));
        assert!(
            state.body.as_bytes() == expected.as_slice(),
            "step {step}: the stored body is not the oracle's bytes"
        );
        stored = Some(state.body);
    }

    let (state, report) = ok(component.handle(activation("noop", stored)));
    assert_eq!(common::results(&report), ["ok"]);
    assert!(state.is_none(), "an unchanged state is not stored again");
}

/// `RetainedState::around_on` stores the default on an activation that found
/// no state, and this codec's default is not a state; the fixture keeps
/// `around_on` off an activation that neither has a state nor creates one.
#[test]
fn a_noop_before_any_state_stores_nothing() {
    let component = load(true);
    let (state, report) = ok(component.handle(activation("noop", None)));
    assert_eq!(common::results(&report), ["ok"]);
    assert!(state.is_none(), "no state existed, so none is stored");
}

#[test]
fn a_binary_state_on_a_text_only_channel_fails_the_activation_under_either_loss() {
    let component = load(false);
    for command in ["state 1024", "state-recover 1024"] {
        let why = err(component.handle(activation(command, None)));
        assert!(
            why.contains("text-only-channel") && why.contains("non_text"),
            "{command}: the refusal names the channel rule and its fix: {why}"
        );
    }
}

#[test]
fn a_json_state_left_behind_is_skew_failed_or_recovered_as_asked() {
    let component = load(true);
    let json_state = || Some(MessageBody::text(r#"{"step":3}"#));

    let why = err(component.handle(activation("state 1024", json_state())));
    assert!(
        why.contains("untyped text") && why.contains(OCTET_STREAM),
        "the skew names both content types: {why}"
    );

    let (state, report) = ok(component.handle(activation("state-recover 1024", json_state())));
    let expected = expected_state(1024, 0);
    assert_eq!(common::results(&report), [result_word(&expected)]);
    let state = state.expect("the recovered state is stored from the default");
    assert_eq!(state.body.content_type(), Some(OCTET_STREAM));
    assert!(state.body.as_bytes() == expected.as_slice());
}

#[test]
fn a_body_of_the_codecs_type_the_codec_refuses_fails_the_activation() {
    let component = load(true);
    let short = Some(common::body(Some(OCTET_STREAM), &[0x01, 0x02, 0x03]));
    let why = err(component.handle(activation("state 1024", short)));
    assert!(
        why.contains("workload state is 3 bytes"),
        "the codec's own error reaches the activation: {why}"
    );
}

#[test]
fn the_sdk_reads_each_kind_of_body_as_it_is() {
    let component = load(true);
    let not_utf8: &[u8] = &[0xff, 0xd8, 0xff, 0x00];
    let feed = vec![
        common::envelope(
            "brenn:feed",
            "10",
            common::body(Some("image/jpeg"), not_utf8),
        ),
        common::envelope(
            "brenn:feed",
            "11",
            common::body(Some("text/markdown"), b"# title"),
        ),
        common::envelope("brenn:feed", "12", MessageBody::text(r#"{"n":1}"#)),
        common::envelope("brenn:feed", "13", MessageBody::text("plain words")),
    ];
    let mut activation = activation("", None);
    activation.ports.push(common::window("feed", feed, 1));
    let (_, report) = ok(component.handle(activation));

    let windows = report["windows"].as_array().expect("windows");
    assert_eq!(windows.len(), 1, "only `feed` is reported");
    let feed = &windows[0];
    assert_eq!(feed["port"], "feed");
    assert_eq!(feed["new_from"], 1);
    assert_eq!(feed["dropped"], 0);
    let entries = feed["envelopes"].as_array().expect("envelopes");
    let seen: Vec<Value> = entries
        .iter()
        .map(|e| {
            json!({
                "id": e["header"]["message_id"],
                "content_type": e["content_type"],
                "hex": e["hex"],
                "text_body": e["text_body"],
                "as_text": e["as_text"],
                "json": e["json"],
            })
        })
        .collect();
    assert_eq!(
        seen,
        [
            json!({
                "id": common::fixture_id("10"),
                "content_type": "image/jpeg",
                "hex": "ffd8ff00",
                "text_body": "malformed",
                "as_text": "malformed",
                "json": "malformed",
            }),
            json!({
                "id": common::fixture_id("11"),
                "content_type": "text/markdown",
                "hex": "23207469746c65",
                "text_body": "# title",
                "as_text": "# title",
                "json": "malformed",
            }),
            json!({
                "id": common::fixture_id("12"),
                "content_type": null,
                "hex": "7b226e223a317d",
                "text_body": r#"{"n":1}"#,
                "as_text": r#"{"n":1}"#,
                "json": {"n": 1},
            }),
            json!({
                "id": common::fixture_id("13"),
                "content_type": null,
                "hex": "706c61696e20776f726473",
                "text_body": "plain words",
                "as_text": "plain words",
                "json": "malformed",
            }),
        ]
    );
    assert_eq!(report["sync"], Value::Null);
}
