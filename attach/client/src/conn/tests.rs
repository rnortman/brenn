//! Connection-lifecycle tests. Everything is driven through [`ConnInput`] with
//! an explicit clock, so the whole state machine is exercised without a socket,
//! a timer, or a runtime — and without naming any application concept, which is
//! the crate's purity proof restated as a test property.

use super::*;

const IDENT: &str = "attacher-test-build";
const HEARTBEAT_SECS: u32 = 20;

fn config() -> ConnConfig {
    ConnConfig {
        url: "wss://host.example/attach".to_string(),
        ident: IDENT.to_string(),
        initial_backoff: Duration::from_secs(3),
        max_backoff: Duration::from_secs(60),
        connect_timeout: Duration::from_secs(10),
        liveness_multiplier: 3,
        backoff_jitter_seed: 0x5EED,
        terminal_close_code: Some(4001),
    }
}

fn peer_hello(versions: VersionRange) -> ConnInput {
    frame(ServerFrame::Hello {
        versions,
        ident: "peer-build".to_string(),
    })
}

fn welcome_frame(version: u32) -> ServerFrame {
    ServerFrame::Welcome {
        version,
        participant_id: "surface:console".to_string(),
        session_id: "sess-1".to_string(),
        heartbeat_secs: HEARTBEAT_SECS,
        max_body_bytes: 65_536,
        max_frame_bytes: 70_000,
        alert_granted: true,
    }
}

fn frame(frame: ServerFrame) -> ConnInput {
    ConnInput::TextFrame(serde_json::to_string(&frame).expect("frame serializes"))
}

/// A connection driven to `Active` at `now = 0`, with the effects of getting
/// there discarded.
fn attached() -> Connection {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    conn.on_input(frame(welcome_frame(1)), Millis(0));
    assert_eq!(conn.state(), ConnState::Active);
    conn
}

fn wakeup(effects: &[ConnEffect]) -> Option<Millis> {
    effects
        .iter()
        .find_map(|e| match e {
            ConnEffect::SetWakeup(at) => Some(*at),
            _ => None,
        })
        .expect("a wakeup was armed or disarmed")
}

fn fatal_detail(effects: &[ConnEffect]) -> String {
    effects
        .iter()
        .find_map(|e| match e {
            ConnEffect::Emit(ConnEvent::Fatal { detail }) => Some(detail.clone()),
            _ => None,
        })
        .expect("a Fatal event")
}

#[test]
fn starting_connects_and_arms_the_handshake_deadline() {
    let (conn, effects) = Connection::start(config(), Millis(1_000));
    assert_eq!(conn.state(), ConnState::Connecting);
    assert_eq!(
        effects[0],
        ConnEffect::Connect {
            url: "wss://host.example/attach".to_string()
        }
    );
    assert_eq!(wakeup(&effects), Some(Millis(11_000)));
}

/// The exchange is symmetric: this end states its whole range as soon as the
/// socket opens, without waiting to be asked.
#[test]
fn an_open_socket_sends_our_hello_unprompted() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    let step = conn.on_input(ConnInput::Opened, Millis(0));
    assert_eq!(conn.state(), ConnState::Negotiating);
    assert_eq!(
        step.effects,
        vec![ConnEffect::SendFrame(ClientFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: IDENT.to_string(),
        })]
    );
}

#[test]
fn an_overlapping_peer_range_agrees_on_the_highest_both_speak() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let step = conn.on_input(peer_hello(VersionRange { min: 1, max: 9 }), Millis(0));
    assert_eq!(conn.state(), ConnState::AwaitingWelcome);
    assert_eq!(conn.version(), Some(1));
    assert!(step.effects.is_empty(), "{:?}", step.effects);
    assert_eq!(step.routed, None);
}

/// Both ends compute the same verdict, so an incompatibility needs no refusal
/// frame — and no backoff either: the ranges are build constants.
#[test]
fn a_disjoint_peer_range_closes_terminally_without_a_refusal_frame() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let theirs = VersionRange { min: 7, max: 9 };
    let step = conn.on_input(peer_hello(theirs), Millis(0));
    assert_eq!(conn.state(), ConnState::Terminal);
    assert_eq!(
        step.effects,
        vec![
            ConnEffect::CloseTransport,
            ConnEffect::Emit(ConnEvent::Incompatible {
                ours: SUPPORTED_VERSIONS,
                theirs,
            }),
            ConnEffect::SetWakeup(None),
        ]
    );
}

#[test]
fn an_empty_peer_range_is_incompatible_rather_than_a_protocol_error() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let step = conn.on_input(peer_hello(VersionRange { min: 4, max: 1 }), Millis(0));
    assert!(matches!(
        step.effects[1],
        ConnEffect::Emit(ConnEvent::Incompatible { .. })
    ));
}

#[test]
fn a_first_frame_that_is_not_hello_is_fatal_and_names_what_arrived() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let step = conn.on_input(frame(welcome_frame(1)), Millis(0));
    assert_eq!(conn.state(), ConnState::Terminal);
    assert!(fatal_detail(&step.effects).contains("got Welcome"));
}

/// A frame this end cannot parse kills the attachment, and the diagnosis names
/// the field that failed — the attacher's own logs are the only place anyone
/// will ever learn what the peer sent.
#[test]
fn unparseable_text_is_fatal_and_diagnoses_the_failing_field() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let step = conn.on_input(
        ConnInput::TextFrame(r#"{"type":"Hello","versions":"not-a-range"}"#.to_string()),
        Millis(0),
    );
    let detail = fatal_detail(&step.effects);
    assert!(detail.contains("unparseable server frame"), "{detail}");
    assert!(detail.contains("VersionRange"), "{detail}");
}

/// Strictness is the negotiated schema's whole point: an unknown field inside a
/// known frame is a bug, not something to shrug at.
#[test]
fn an_unknown_field_in_a_known_frame_is_fatal() {
    let mut conn = attached();
    let step = conn.on_input(
        ConnInput::TextFrame(
            r#"{"type":"SubscribeResult","channel":"brenn:orders","outcome":{"kind":"Ok"},
                "replay_count":0,"depth":4}"#
                .to_string(),
        ),
        Millis(0),
    );
    assert_eq!(conn.state(), ConnState::Terminal);
    assert!(fatal_detail(&step.effects).contains("unparseable server frame"));
}

#[test]
fn welcome_states_the_attachment_contract_and_arms_liveness() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    let step = conn.on_input(frame(welcome_frame(1)), Millis(5_000));
    assert_eq!(conn.state(), ConnState::Active);
    assert!(conn.is_active());
    assert_eq!(
        step.effects[0],
        ConnEffect::Emit(ConnEvent::Attached(AttachmentFacts {
            version: 1,
            participant_id: "surface:console".to_string(),
            session_id: "sess-1".to_string(),
            heartbeat_secs: HEARTBEAT_SECS,
            max_body_bytes: 65_536,
            max_frame_bytes: 70_000,
            alert_granted: true,
        }))
    );
    // 20s heartbeat × 3 = 60s of tolerated inbound silence.
    assert_eq!(wakeup(&step.effects), Some(Millis(65_000)));
}

#[test]
fn a_welcome_restating_a_version_the_handshake_did_not_agree_is_fatal() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    let step = conn.on_input(frame(welcome_frame(2)), Millis(0));
    let detail = fatal_detail(&step.effects);
    assert!(detail.contains("version 2"), "{detail}");
    assert!(detail.contains("agreed 1"), "{detail}");
}

/// A liveness window of zero would reap the attachment on its first tick and
/// reconnect into the same `Welcome`, so the peer's number is refused rather than
/// churned on.
#[test]
fn a_welcome_stating_a_zero_heartbeat_is_fatal() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    let mut welcome = welcome_frame(1);
    let ServerFrame::Welcome { heartbeat_secs, .. } = &mut welcome else {
        unreachable!("welcome_frame builds a Welcome")
    };
    *heartbeat_secs = 0;
    let step = conn.on_input(frame(welcome), Millis(0));
    assert_eq!(conn.state(), ConnState::Terminal);
    assert!(
        fatal_detail(&step.effects).contains("zero heartbeat interval"),
        "{:?}",
        step.effects
    );
}

/// The embedder's own half of the same window. Its configuration is not peer
/// input, so a zero there is a bug in the embedder and panics.
#[test]
#[should_panic(expected = "liveness multiplier of zero")]
fn a_zero_liveness_multiplier_is_an_embedder_bug() {
    Connection::start(
        ConnConfig {
            liveness_multiplier: 0,
            ..config()
        },
        Millis(0),
    );
}

#[test]
fn a_frame_other_than_welcome_after_negotiation_is_fatal() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    let step = conn.on_input(frame(ServerFrame::Heartbeat), Millis(0));
    assert!(fatal_detail(&step.effects).contains("got Heartbeat"));
}

#[test]
fn a_heartbeat_re_arms_liveness_and_routes_nothing() {
    let mut conn = attached();
    let step = conn.on_input(frame(ServerFrame::Heartbeat), Millis(30_000));
    assert_eq!(step.routed, None);
    assert_eq!(wakeup(&step.effects), Some(Millis(90_000)));
}

/// A frame belonging to a plane above this one is handed back untouched — and
/// still counts as evidence the peer is alive.
#[test]
fn a_frame_of_another_plane_is_routed_and_still_re_arms_liveness() {
    let mut conn = attached();
    let result = ServerFrame::SubscribeResult {
        channel: "brenn:orders".to_string(),
        outcome: brenn_attach_proto::SubscribeOutcome::Ok,
        replay_count: 3,
        gap: None,
    };
    let step = conn.on_input(frame(result.clone()), Millis(10_000));
    assert_eq!(step.routed, Some(result));
    assert_eq!(wakeup(&step.effects), Some(Millis(70_000)));
}

#[test]
fn a_repeated_handshake_frame_on_a_live_attachment_is_fatal() {
    for (input, expected) in [
        (peer_hello(SUPPORTED_VERSIONS), "second Hello frame"),
        (frame(welcome_frame(1)), "second Welcome frame"),
    ] {
        let mut conn = attached();
        let step = conn.on_input(input, Millis(0));
        assert_eq!(fatal_detail(&step.effects), expected);
    }
}

#[test]
fn a_binary_frame_is_fatal_in_every_state_that_owns_a_transport() {
    let mut conn = attached();
    let step = conn.on_input(ConnInput::BinaryFrame, Millis(0));
    assert!(fatal_detail(&step.effects).contains("binary frame"));
}

#[test]
fn inbound_silence_past_the_liveness_deadline_detaches_and_backs_off() {
    let mut conn = attached();
    let step = conn.on_input(ConnInput::Tick, Millis(60_001));
    assert_eq!(conn.state(), ConnState::Backoff);
    assert_eq!(step.effects[0], ConnEffect::CloseTransport);
    assert_eq!(
        step.effects[1],
        ConnEffect::Emit(ConnEvent::Detached {
            reason: DetachReason::LivenessTimeout
        })
    );
}

#[test]
fn a_tick_before_the_deadline_only_re_arms_it() {
    let mut conn = attached();
    let step = conn.on_input(ConnInput::Tick, Millis(1_000));
    assert_eq!(conn.state(), ConnState::Active);
    assert_eq!(
        step.effects,
        vec![ConnEffect::SetWakeup(Some(Millis(60_000)))]
    );
}

#[test]
fn a_handshake_that_never_completes_closes_the_transport_and_backs_off() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    conn.on_input(ConnInput::Opened, Millis(0));
    let step = conn.on_input(ConnInput::Tick, Millis(10_000));
    assert_eq!(conn.state(), ConnState::Backoff);
    assert_eq!(step.effects[0], ConnEffect::CloseTransport);
    assert!(
        !step
            .effects
            .iter()
            .any(|e| matches!(e, ConnEffect::Emit(ConnEvent::Detached { .. }))),
        "nothing was ever attached, so nothing detached"
    );
}

/// The driver already dropped the connection before feeding the input, so the
/// state machine must not ask for a second close.
#[test]
fn a_transport_drop_while_live_detaches_without_closing_again() {
    let mut conn = attached();
    let step = conn.on_input(
        ConnInput::Disconnected {
            code: Some(1006),
            reason: String::new(),
        },
        Millis(1_000),
    );
    assert_eq!(conn.state(), ConnState::Backoff);
    assert_eq!(
        step.effects[0],
        ConnEffect::Emit(ConnEvent::Detached {
            reason: DetachReason::TransportClosed
        })
    );
    assert!(!step.effects.contains(&ConnEffect::CloseTransport));
}

#[test]
fn the_embedder_declared_close_code_is_terminal_rather_than_a_backoff() {
    let mut conn = attached();
    let step = conn.on_input(
        ConnInput::Disconnected {
            code: Some(4001),
            reason: "build 9f2c1a".to_string(),
        },
        Millis(1_000),
    );
    assert_eq!(conn.state(), ConnState::Terminal);
    assert_eq!(
        step.effects,
        vec![
            ConnEffect::Emit(ConnEvent::PeerClosedTerminal {
                code: 4001,
                reason: "build 9f2c1a".to_string(),
            }),
            ConnEffect::SetWakeup(None),
        ]
    );
}

/// A close while the socket is still being opened cannot be from the attempt in
/// flight — the connector resolves the handshake, and a close during it arrives
/// as `ConnectFailed`. So this is a straggler from a transport the driver has
/// already stopped feeding, terminal code and all, and the attempt runs on to
/// its own deadline.
#[test]
fn a_close_while_connecting_is_absorbed_as_a_straggler() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    let step = conn.on_input(
        ConnInput::Disconnected {
            code: Some(4001),
            reason: "build 9f2c1a".to_string(),
        },
        Millis(1_000),
    );
    assert!(step.effects.is_empty(), "{:?}", step.effects);
    assert_eq!(step.routed, None);
    assert_eq!(conn.state(), ConnState::Connecting);
    // The handshake deadline still governs the live attempt.
    let step = conn.on_input(ConnInput::Tick, Millis(10_000));
    assert_eq!(step.effects[0], ConnEffect::CloseTransport);
    assert_eq!(conn.state(), ConnState::Backoff);
}

#[test]
fn a_failed_connect_backs_off_without_claiming_a_detach() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    let step = conn.on_input(ConnInput::ConnectFailed, Millis(0));
    assert_eq!(conn.state(), ConnState::Backoff);
    assert_eq!(step.effects.len(), 1);
    assert!(matches!(step.effects[0], ConnEffect::SetWakeup(Some(_))));
}

/// The backoff deadlines of `count` successive failed connects, each fired to
/// get back to `Connecting` for the next one.
fn backoff_delays(config: ConnConfig, count: usize) -> Vec<u64> {
    let (mut conn, _) = Connection::start(config, Millis(0));
    let mut delays = Vec::new();
    for _ in 0..count {
        let step = conn.on_input(ConnInput::ConnectFailed, Millis(0));
        let Some(Millis(at)) = wakeup(&step.effects) else {
            panic!("backoff arms a deadline");
        };
        delays.push(at);
        conn.on_input(ConnInput::Tick, Millis(at));
    }
    delays
}

/// The nominal schedule the jitter band is measured against.
const NOMINALS: [u64; 8] = [3_000, 6_000, 12_000, 24_000, 48_000, 60_000, 60_000, 60_000];

/// The nominal doubles and caps; equal jitter keeps every draw inside
/// `[nominal/2, nominal]`, so backoff stays meaningful while a fleet
/// decorrelates.
#[test]
fn backoff_doubles_within_its_jitter_band_and_caps() {
    for (step, nominal) in backoff_delays(config(), 8).iter().zip(NOMINALS) {
        assert!(
            *step >= nominal / 2 && *step <= nominal,
            "delay {step} outside [{}, {nominal}]",
            nominal / 2
        );
    }
}

/// The jitter is load-spreading entropy, so the seed must actually reach the
/// draw: two attachers of one fleet must not share a reconnect schedule, and one
/// attacher's own draws must not sit at a fixed point in the band.
#[test]
fn different_jitter_seeds_produce_different_backoff_schedules() {
    let ours = backoff_delays(config(), 8);
    let theirs = backoff_delays(
        ConnConfig {
            backoff_jitter_seed: 0xD1FF,
            ..config()
        },
        8,
    );
    assert_ne!(ours, theirs, "a fleet on one seed reconnects in lockstep");

    assert!(
        ours.iter()
            .zip(NOMINALS)
            .any(|(delay, nominal)| *delay != nominal / 2),
        "every draw at the band floor means the jitter never moved: {ours:?}"
    );
}

#[test]
fn a_completed_handshake_resets_the_backoff_schedule() {
    let (mut conn, _) = Connection::start(config(), Millis(0));
    for _ in 0..4 {
        let step = conn.on_input(ConnInput::ConnectFailed, Millis(0));
        let Some(Millis(at)) = wakeup(&step.effects) else {
            panic!("backoff arms a deadline");
        };
        conn.on_input(ConnInput::Tick, Millis(at));
    }
    conn.on_input(ConnInput::Opened, Millis(0));
    conn.on_input(peer_hello(SUPPORTED_VERSIONS), Millis(0));
    conn.on_input(frame(welcome_frame(1)), Millis(0));
    let step = conn.on_input(
        ConnInput::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(0),
    );
    let Some(Millis(at)) = wakeup(&step.effects) else {
        panic!("backoff arms a deadline");
    };
    assert!(
        (1_500..=3_000).contains(&at),
        "first step again, not the fifth: {at}"
    );
}

#[test]
fn the_embedders_own_fatal_takes_the_ordinary_terminal_path() {
    let mut conn = attached();
    let effects = conn.go_fatal("bindings document failed validation".to_string());
    assert_eq!(conn.state(), ConnState::Terminal);
    assert_eq!(effects[0], ConnEffect::CloseTransport);
    assert_eq!(
        fatal_detail(&effects),
        "bindings document failed validation"
    );
    assert_eq!(wakeup(&effects), None);
}

#[test]
fn an_embedder_requested_close_is_terminal_and_silent() {
    let mut conn = attached();
    let effects = conn.close();
    assert_eq!(conn.state(), ConnState::Terminal);
    assert_eq!(
        effects,
        vec![ConnEffect::CloseTransport, ConnEffect::SetWakeup(None)]
    );
}

/// After the death decision, in-flight transport and timer events are expected
/// and absorbed — peer input never panics, and a terminal state never revives.
#[test]
fn a_terminal_connection_absorbs_every_further_input() {
    let mut conn = attached();
    conn.go_fatal("done".to_string());
    for input in [
        ConnInput::Opened,
        ConnInput::ConnectFailed,
        ConnInput::Disconnected {
            code: Some(4001),
            reason: String::new(),
        },
        frame(ServerFrame::Heartbeat),
        ConnInput::BinaryFrame,
        ConnInput::Tick,
    ] {
        let step = conn.on_input(input, Millis(1_000_000));
        assert!(step.effects.is_empty(), "{:?}", step.effects);
        assert_eq!(step.routed, None);
        assert_eq!(conn.state(), ConnState::Terminal);
    }
}

/// A frame from the connection that was just torn down is an ordinary async
/// race, not a bug.
#[test]
fn a_straggler_from_a_dropped_transport_is_absorbed_while_backing_off() {
    let mut conn = attached();
    conn.on_input(
        ConnInput::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(0),
    );
    let step = conn.on_input(frame(ServerFrame::Heartbeat), Millis(0));
    assert!(step.effects.is_empty());
    assert_eq!(conn.state(), ConnState::Backoff);
}

/// A host-side precondition failure is terminal wherever it is raised — it is
/// the same death as a protocol error, with its own diagnosis.
#[test]
fn a_host_fatal_is_terminal_from_any_live_state() {
    for mut conn in [
        {
            let (c, _) = Connection::start(config(), Millis(0));
            c
        },
        attached(),
    ] {
        let step = conn.on_input(
            ConnInput::HostFatal {
                detail: "the device clock reads before the Unix epoch".to_string(),
            },
            Millis(0),
        );
        assert_eq!(conn.state(), ConnState::Terminal);
        assert!(fatal_detail(&step.effects).contains("Unix epoch"));
    }
}

#[test]
fn a_dropped_connection_forgets_the_version_it_negotiated() {
    let mut conn = attached();
    assert_eq!(conn.version(), Some(1));
    conn.on_input(
        ConnInput::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(0),
    );
    assert_eq!(conn.version(), None);
}
