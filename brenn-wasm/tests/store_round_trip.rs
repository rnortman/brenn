// Fault-test component integration tests.
//
// Non-fault KvStore tests (write/read/scan/rollback) have been moved into
// brenn-wasm/src/store.rs as #[cfg(test)] unit tests, where pub(crate)
// visibility is accessible without widening the public API.
//
// These two tests exercise the fault-test component (brenn_replay_fault_test.wasm)
// via ReplayComponent::check_raw_for_testing, which requires an external crate
// (integration test) because ReplayComponent is in lib.rs.

mod common;

use brenn_wasm::{CheckInput, Header};

const FAULT_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_replay_fault_test.wasm"
);

fn fault_artifact_path() -> std::path::PathBuf {
    std::path::PathBuf::from(FAULT_ARTIFACT_PATH)
}

/// Build a CheckInput with the x-brenn-fault-test header set to `op`.
fn fault_input(op: &str) -> CheckInput {
    CheckInput {
        headers: vec![Header {
            name: "x-brenn-fault-test".to_string(),
            value: op.to_string(),
        }],
        body: vec![],
        received_at: 0,
        key_id: String::new(),
        endpoint_slug: "test".to_string(),
    }
}

#[test]
fn leaked_transaction_traps() {
    let (_db, component) = common::open_component(&fault_artifact_path());

    let result = component.check_raw_for_testing(&fault_input("LEAK_TX"));
    assert!(
        result.is_err(),
        "expected outer Err (trap from leaked transaction), got Ok({:?})",
        result.unwrap()
    );
    let err = result.unwrap_err();
    let err_display = format!("{err}");
    let err_debug = format!("{err:?}");
    assert!(
        err_display.contains("transaction leaked") || err_debug.contains("transaction leaked"),
        "expected 'transaction leaked' in error chain, got display: {err_display}\ndebug: {err_debug}"
    );
    // After the trap, tx_active must be cleared (host cleaned up in drop).
    assert!(
        !component.kv_store_for_testing().is_tx_active(),
        "tx_active must be cleared after leak-trap"
    );
}

#[test]
fn component_trap_is_distinguished_from_typed_error() {
    let (_db, component) = common::open_component(&fault_artifact_path());

    let result = component.check_raw_for_testing(&fault_input("TRAP"));
    assert!(
        result.is_err(),
        "expected outer Err (trap), got Ok({:?})",
        result.unwrap()
    );
}
