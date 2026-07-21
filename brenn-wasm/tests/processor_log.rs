// Integration tests for the log/alert capability of `ProcessorComponent`
// against the processor-log fixture (design §4, tests 1–9).
//
// tracing-test is compiled with `no-env-filter` so all tracing events are
// captured regardless of the `RUST_LOG` env var.  Test assertions filter on the
// `wasm_guest` target to isolate guest-emitted events from host infrastructure
// events that may also be captured.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_wasm::{
    Capability, GuestAlertSeverity, PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION,
    PROCESSOR_MAX_ALERT_TITLE_BYTES, PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION, ProcessorActivation,
    ProcessorAlerter, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome, ProcessorPortWindow,
    store::DEFAULT_MAX_PAGE_COUNT,
};
use tracing_test::traced_test;

mod common;

// ── helpers ──────────────────────────────────────────────────────────────────

fn component_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_log.wasm")
}

/// Load the processor-log fixture with the given alerter.
///
/// This fixture uses no store (`store_path: None`) — no tempfile is returned.
fn load_log_component(alerter: Arc<dyn ProcessorAlerter>) -> ProcessorComponent {
    // processor-log imports: types + log + alert
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path(),
        slug: "log-test",
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: [Capability::Log, Capability::Alert].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter,
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn noop_alerter() -> Arc<dyn ProcessorAlerter> {
    common::noop_alerter()
}

/// Build a standard MessageEnvelope JSON where `body` is the directive JSON serialised as a string.
fn make_envelope(directive: serde_json::Value) -> String {
    let body_str = directive.to_string();
    let body_escaped = serde_json::to_string(&body_str).unwrap();
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000001","source":"test","channel":"brenn:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":{body_escaped},"urgency":"normal","envelope_type":"brenn"}}"#
    )
}

fn single_activation(envelope: String) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope],
            new_from: 0,
            dropped: 0,
        }],
    }
}

fn multi_activation(envelopes: Vec<String>) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes,
            new_from: 0,
            dropped: 0,
        }],
    }
}

// ── test 1: log happy path ────────────────────────────────────────────────────

/// Each WIT log level maps to a captured tracing event under target `wasm_guest`
/// with the `slug` field and sanitised message.
#[test]
#[traced_test]
fn log_happy_path_each_level() {
    let comp = load_log_component(noop_alerter());

    for (level_str, expected_level_in_log) in [
        ("trace", "TRACE"),
        ("debug", "DEBUG"),
        ("info", "INFO"),
        ("warn", "WARN"),
        ("error", "ERROR"),
    ] {
        let envelope = make_envelope(serde_json::json!({
            "cmd": "log",
            "level": level_str,
            "message": format!("test-message-{level_str}")
        }));
        let outcome = comp.handle(single_activation(envelope));
        assert!(
            matches!(outcome, ProcessorOutcome::Ok(_)),
            "log level {level_str}: expected Ok, got {outcome:?}"
        );
        // The captured log output must contain both the level, the target, the slug, and the
        // message — verifiable via logs_contain on the formatted output.
        assert!(
            logs_contain("wasm_guest"),
            "log level {level_str}: expected target 'wasm_guest' in captured log"
        );
        assert!(
            logs_contain(&format!("test-message-{level_str}")),
            "log level {level_str}: expected message in captured log"
        );
        assert!(
            logs_contain("log-test"),
            "log level {level_str}: expected slug 'log-test' in captured log"
        );
        // Verify the expected level string appears in the captured output.
        // (tracing-test formats events with the level as a prefix like "INFO wasm_guest...")
        assert!(
            logs_contain(expected_level_in_log),
            "log level {level_str}: expected level '{expected_level_in_log}' in captured log"
        );
    }
}

// ── test 2: alert happy path ──────────────────────────────────────────────────

/// Each WIT severity maps to the correct GuestAlertSeverity; title and body
/// arrive sanitised at the alerter.
#[test]
fn alert_happy_path_each_severity() {
    let alerter = common::CapturingAlerter::new();
    let comp = load_log_component(alerter.clone());

    for severity_str in ["info", "warning", "critical"] {
        let envelope = make_envelope(serde_json::json!({
            "cmd": "alert",
            "severity": severity_str,
            "title": format!("title-{severity_str}"),
            "body": format!("body-{severity_str}")
        }));
        let outcome = comp.handle(single_activation(envelope));
        assert!(
            matches!(outcome, ProcessorOutcome::Ok(_)),
            "alert severity {severity_str}: expected Ok, got {outcome:?}"
        );
    }

    let calls = alerter.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        3,
        "expected 3 alert calls, got {}",
        calls.len()
    );

    // Verify each severity and payload mapping.
    assert!(
        matches!(calls[0].0, GuestAlertSeverity::Info),
        "first alert must be Info"
    );
    assert_eq!(calls[0].1, "title-info");
    assert_eq!(calls[0].2, "body-info");

    assert!(
        matches!(calls[1].0, GuestAlertSeverity::Warning),
        "second alert must be Warning"
    );
    assert_eq!(calls[1].1, "title-warning");
    assert_eq!(calls[1].2, "body-warning");

    assert!(
        matches!(calls[2].0, GuestAlertSeverity::Critical),
        "third alert must be Critical"
    );
    assert_eq!(calls[2].1, "title-critical");
    assert_eq!(calls[2].2, "body-critical");
}

// ── test 3: sanitization ──────────────────────────────────────────────────────

/// Messages with control chars are escaped; a `slug=evil` substring stays inside
/// the message field; an oversized message is truncated at a UTF-8 boundary.
#[test]
#[traced_test]
fn log_sanitization_escapes_control_chars() {
    let comp = load_log_component(noop_alerter());

    // Message with newline, ESC, and a fake `slug=evil` structured field attempt.
    let envelope = make_envelope(serde_json::json!({
        "cmd": "log",
        "level": "info",
        "message": "line1\nline2\x1b[31mred slug=evil rest"
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(matches!(outcome, ProcessorOutcome::Ok(_)));

    // The raw newline and ESC must not appear verbatim — they are escape_debug'd.
    // logs_contain checks the formatted string representation, which will have \n
    // and \u{1b} (Rust's escape_debug output) rather than raw control chars.
    assert!(
        !logs_contain("\x1b[31m"),
        "raw ANSI escape must not appear in log output"
    );
    // escape_debug turns '\n' into the two-character sequence backslash-n.
    // Assert the escaped form is present (sanitization ran) and the raw newline
    // is absent (no log-line injection). logs_contain splits on newlines, so a
    // needle containing a raw '\n' can never match — check the escaped form instead.
    assert!(
        logs_contain(r"line1\nline2"),
        "escaped newline sequence must appear in captured log (sanitization ran)"
    );
    // The message field value is in a single structured-field slot; slug=evil inside
    // it cannot masquerade as a separate tracing field (it's part of the message value).
    assert!(
        logs_contain("slug=evil"),
        "the literal text 'slug=evil' must appear — confined inside message field value"
    );
}

#[test]
fn alert_sanitization_truncates_and_escapes() {
    let alerter = common::CapturingAlerter::new();
    let comp = load_log_component(alerter.clone());

    // Title with control chars; body within limit.
    let envelope = make_envelope(serde_json::json!({
        "cmd": "alert",
        "severity": "warning",
        "title": "alert\x00title",
        "body": "normal body"
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(matches!(outcome, ProcessorOutcome::Ok(_)));

    let calls = alerter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    // The null byte must be escaped, not literal.
    assert!(
        !calls[0].1.contains('\x00'),
        "null byte must be escaped in alert title"
    );
    assert_eq!(calls[0].2, "normal body");
}

#[test]
fn alert_hostile_title_bounded_at_cap() {
    // A title of control chars past the 256-byte cap drives real output-bounding at
    // the WASM alert host fn — each '\x1b' escapes to `\u{1b}` (6×), so an unbounded
    // sanitizer would blow the cap. Assert the dispatched title is escaped and bounded.
    let alerter = common::CapturingAlerter::new();
    let comp = load_log_component(alerter.clone());

    let hostile_title = "\x1b".repeat(PROCESSOR_MAX_ALERT_TITLE_BYTES + 64);
    let envelope = make_envelope(serde_json::json!({
        "cmd": "alert",
        "severity": "warning",
        "title": hostile_title,
        "body": "normal body"
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(matches!(outcome, ProcessorOutcome::Ok(_)));

    let calls = alerter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let dispatched_title = &calls[0].1;
    assert!(
        !dispatched_title.contains('\x1b'),
        "raw ESC control char must be escaped in dispatched title"
    );
    assert!(
        dispatched_title.len()
            <= PROCESSOR_MAX_ALERT_TITLE_BYTES + brenn_common::TRUNCATION_MARKER.len(),
        "dispatched title must be bounded to cap + marker, got {} bytes",
        dispatched_title.len()
    );
    assert!(
        dispatched_title.ends_with(brenn_common::TRUNCATION_MARKER),
        "an over-cap title must carry the truncation marker"
    );
}

#[test]
#[traced_test]
fn log_oversized_message_truncated() {
    let comp = load_log_component(noop_alerter());

    // Build a string that is > 4096 bytes with a multi-byte char (é = U+00E9, 2 bytes in UTF-8)
    // straddling the escaped-output budget. 4095 ASCII 'a's + 'é' (2 bytes) = 4097 bytes total.
    // The sanitizer bounds escaped output to 4096 bytes: the 'a's pass through (ASCII, 1 byte
    // each), and the 'é' — as a whole escape unit — overflows the budget and is rolled back,
    // leaving 4095 'a's plus the truncation marker.
    let mut msg = "a".repeat(4095);
    msg.push('é'); // 2-byte char overflows the 4096-byte output budget
    let envelope = make_envelope(serde_json::json!({
        "cmd": "log",
        "level": "info",
        "message": msg
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok(_)),
        "oversized message must not cause an error"
    );
    // The sanitized message is 4095 'a's (escape_debug leaves ASCII unchanged) plus the
    // truncation marker. Verify the prefix is present, the marker is present, and the
    // rolled-back 'é' is absent.
    assert!(
        logs_contain(&"a".repeat(4095)),
        "truncated message must contain the 4095 'a' prefix"
    );
    assert!(
        logs_contain("…(truncated)"),
        "truncated message must carry the truncation marker"
    );
    assert!(
        !logs_contain("é"),
        "truncated message must not contain the rolled-back 'é' that overflowed the budget"
    );
}

// ── test 4: log quota ─────────────────────────────────────────────────────────

/// Fixture emits 300 log calls → exactly 256 captured; post-activation suppression
/// warn with `log_suppressed = 44` is emitted.
#[test]
#[traced_test]
fn log_quota_suppresses_excess() {
    let comp = load_log_component(noop_alerter());

    let n = PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION + 44; // 300
    let envelope = make_envelope(serde_json::json!({
        "cmd": "log_n",
        "n": n,
        "level": "info",
        "message": "quota-test-msg"
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok(_)),
        "log quota exhaustion must not fail the activation"
    );

    // Verify that exactly PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION guest log events
    // reached the subscriber (not one fewer due to '>=' vs '>' fence-post, and not
    // more due to missing quota check). logs_assert gives us filtered lines for the
    // current test scope; count those containing the wasm_guest target and message.
    logs_assert(|lines: &[&str]| {
        let count = lines
            .iter()
            .filter(|l| l.contains("wasm_guest") && l.contains("quota-test-msg"))
            .count();
        if count == PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION {
            Ok(())
        } else {
            Err(format!(
                "expected exactly {PROCESSOR_MAX_LOG_CALLS_PER_ACTIVATION} guest log events, got {count}"
            ))
        }
    });

    // Suppression warn must be emitted by invoke() after the activation.
    assert!(
        logs_contain("wasm guest log/alert quota exceeded"),
        "suppression warn must be emitted when log quota is exceeded"
    );
    // Assert the exact structured field value emitted by the suppression warn.
    // Bare "44" is a substring match that can appear in timestamps or other numeric
    // fields; "log_suppressed=44" pins the field name and value together.
    assert!(
        logs_contain("log_suppressed=44"),
        "suppression warn must carry log_suppressed=44"
    );
}

// ── test 5: alert quota ───────────────────────────────────────────────────────

/// 10 alert calls → exactly 4 captured; suppression warn present.
#[test]
#[traced_test]
fn alert_quota_suppresses_excess() {
    let alerter = common::CapturingAlerter::new();
    let comp = load_log_component(alerter.clone());

    let n = PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION + 6; // 10
    let envelope = make_envelope(serde_json::json!({
        "cmd": "alert_n",
        "n": n,
        "severity": "warning",
        "title": "quota-title",
        "body": "quota-body"
    }));
    let outcome = comp.handle(single_activation(envelope));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok(_)),
        "alert quota exhaustion must not fail the activation"
    );

    let calls = alerter.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION,
        "exactly {} alert calls must be captured; got {}",
        PROCESSOR_MAX_ALERT_CALLS_PER_ACTIVATION,
        calls.len()
    );

    // Suppression warn must appear.
    assert!(
        logs_contain("wasm guest log/alert quota exceeded"),
        "suppression warn must be emitted when alert quota is exceeded"
    );
    // Assert the exact structured field value; bare "6" matches timestamps and other numerics.
    assert!(
        logs_contain("alert_suppressed=6"),
        "suppression warn must carry alert_suppressed=6"
    );
}

// ── test 6: diagnostics survive failure ───────────────────────────────────────

/// Log-then-trap: log event is captured even though the outcome is Trap.
#[test]
#[traced_test]
fn log_survives_trap() {
    let comp = load_log_component(noop_alerter());

    // Two envelopes: first logs, second traps.
    let log_env = make_envelope(serde_json::json!({
        "cmd": "log",
        "level": "warn",
        "message": "pre-trap-log"
    }));
    let trap_env = make_envelope(serde_json::json!({"cmd": "trap"}));
    let outcome = comp.handle(multi_activation(vec![log_env, trap_env]));
    assert!(
        matches!(outcome, ProcessorOutcome::Trap(_)),
        "expected Trap outcome, got {outcome:?}"
    );
    assert!(
        logs_contain("pre-trap-log"),
        "log emitted before trap must be captured (diagnostics survive failure)"
    );
}

/// Log-then-err: log event is captured even though the outcome is Err.
#[test]
#[traced_test]
fn log_survives_err() {
    let comp = load_log_component(noop_alerter());

    let log_env = make_envelope(serde_json::json!({
        "cmd": "log",
        "level": "error",
        "message": "pre-err-log"
    }));
    let err_env = make_envelope(serde_json::json!({
        "cmd": "err",
        "message": "test-err"
    }));
    let outcome = comp.handle(multi_activation(vec![log_env, err_env]));
    assert!(
        matches!(outcome, ProcessorOutcome::Err(_)),
        "expected Err outcome, got {outcome:?}"
    );
    assert!(
        logs_contain("pre-err-log"),
        "log emitted before err must be captured (diagnostics survive failure)"
    );
}

// ── test 7: additive-import regression ───────────────────────────────────────
// processor-demo was built without log/alert imports; it must still instantiate
// and run against the new host (which now links log+alert unconditionally).
// This is covered automatically by the existing consume_engine.rs tests, which
// load processor-demo under the same ProcessorComponent::load path.  No separate
// test is needed here; the existing suite is the regression proof.

// ── test 8: DispatcherAlerter attribution ─────────────────────────────────────
// This is in brenn/src/wasm_dispatch.rs tests, not here — DispatcherAlerter is
// in the binary crate and is tested there.

// ── test 9: sanitize_diag path ────────────────────────────────────────────────
// The wasm_dispatch.rs diagnostic sinks call brenn_common::sanitize_untrusted_str;
// the trap/err-path tests in wasm_dispatch.rs cover that wiring.
