# brenn-mqtt Integration Tests

## MQTT Integration Suite

Tests the `MqttService` + `ConnectionSupervisor` stack against a live `mosquitto` broker over TLS.

### Prerequisites

`mosquitto` ≥ 2.0 must be installed.

A test action's `PATH` is a fixed one, not the invoking shell's.
`//brenn-mqtt:mqtt_integration` pins it to the static list in
`brenn-mqtt/BUILD.bazel`: Bazel's own default plus `/usr/sbin` and
`/usr/local/sbin`, where Debian-family packaging and source installs put
daemons. A broker anywhere else — `/opt`, a Nix profile, a Homebrew prefix on a
non-standard root — is not found there no matter what `which mosquitto` prints.

If `mosquitto` is installed at a non-default location, set:

```
export BRENN_MOSQUITTO_BIN=/path/to/mosquitto
```

It takes precedence over the `PATH` lookup, and `.bazelrc`'s
`test --test_env=BRENN_MOSQUITTO_BIN` is what forwards it into the test action.

### Running

```
bazel test //brenn-mqtt:mqtt_integration
```

The target sets the `BRENN_MQTT_INTEGRATION` gate variable itself.

### Re-verifying the Bazel-lane override

Nothing gates the `.bazelrc` pass-through: delete that line and every test still
passes, because the pinned `PATH` finds the broker anyway — while
`BRENN_MOSQUITTO_BIN` goes silently dead in the Bazel lane and the "binary not
found" panic starts advising a variable with no effect. After any edit to it,
run:

```
BRENN_MOSQUITTO_BIN=/nonexistent/x bazel test //brenn-mqtt:mqtt_integration
```

Every test must fail naming `Tried: "/nonexistent/x"`. Green means the override
is not reaching the test.

### Strict timing mode (optional)

```
bazel test //brenn-mqtt:mqtt_integration --test_env=BRENN_MQTT_INTEGRATION_STRICT=1
```

`BRENN_MQTT_INTEGRATION_STRICT=1` adds a 1.8s wall-time bound to
`quiet_period_heuristic_100_retained`. Off by default to avoid timing flakes on
loaded laptops; correctness (count == 100) is asserted unconditionally either way.

### Expected pass output

Test order varies run to run (each is an independent `multi_thread` test); the
set is what matters:

```
running 16 tests
test tls_connect_subscribe_publish_qos1_roundtrip ... ok
test retained_message_roundtrip_text ... ok
test retained_message_roundtrip_binary_content_type ... ok
test quiet_period_heuristic_100_retained ... ok
test wildcard_subscription_dispatch_plus_and_hash ... ok
test disconnect_returns_not_connected_promptly ... ok
test inbound_classification_text_vs_binary ... ok
test qos0_publish_returns_success_and_delivers ... ok
test get_retained_not_connected_before_call ... ok
test get_retained_not_connected_mid_collection ... ok
test tls13_connect_subscribe_publish_roundtrip ... ok
test stop_reports_disconnected_broker_alive ... ok
test wasm_egress_publish_reaches_broker ... ok
test wasm_egress_broker_rejected_maps_reason ... ok
test wasm_egress_acl_denied ... ok
test wasm_egress_no_connector ... ok

test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in ...s
```

The four `wasm_egress_*` tests are the WASM-egress acceptance layer: they drive
`enforce_and_publish` (the shared enforcement pipeline the WASM `mqtt:publish`
callback uses) against the live broker.

### Not part of default suite

These tests carry `tags = ["requires-network"]`, so `make check` does not run them. The gate is the `BRENN_MQTT_INTEGRATION` environment variable, which the Bazel target sets.

### Human-review gate

Before merge, the developer runs the suite 10 consecutive times with no flakes:

```
bazel test //brenn-mqtt:mqtt_integration --runs_per_test=10 --nocache_test_results
```

### Failure modes

**`mosquitto readiness timeout`**
: mosquitto exited or never bound the TLS port. The panic message includes the last 4 KiB of the log file. Most common cause: mosquitto config rejected (cert/key path wrong, ACL syntax). Check the log path printed in the panic.

**`mosquitto binary not found`**
: Install mosquitto or set `BRENN_MOSQUITTO_BIN=/path/to/mosquitto`. Under
`bazel test` this also fires with mosquitto installed and on your shell's
`PATH`, when it sits outside the `PATH` pinned in `brenn-mqtt/BUILD.bazel`
(see Prerequisites); `BRENN_MOSQUITTO_BIN` is the answer there too.

**TLS handshake failure**
: Confirm the broker URL host matches a SAN in the generated server cert (`localhost` and `127.0.0.1` are covered; other hostnames fail validation). SAN coverage is set in `tests/common/certs.rs`. Check that `tls_version_min = "1.2"` matches what the broker config negotiates. Cert expiry cannot happen — certs are generated per-run by `tests/common/certs.rs` (not_after 2125), not checked in.

**ACL denied**
: The test ACL permits `brenn/itest/#` only. Tests must use topic prefixes within that tree. If you see ACL-denied errors in mosquitto logs, check the test topic prefixes.

**Count mismatch in quiet-period test**
: `quiet_period_heuristic_100_retained` expects exactly 100 returned messages. Count < 100 means the heuristic truncated, which is a real bug.

**`wasm_egress_broker_rejected_maps_reason` fails or hangs**
: This test publishes QoS 1 to a topic **outside** `brenn/itest/#` and expects the broker to reject it with a PUBACK non-success reason code (mosquitto 2.x answers a v5 client's ACL-denied publish with reason 0x87). Two distinct failures:
  - **`Ok(())` returned** — the broker accepted a publish outside its ACL tree, so the ACL is not being enforced (bad broker config / wrong ACL file). The `BrokerRejected` assertion is meaningless until this is fixed.
  - **Timed out with no PUBACK** — the broker is silently *dropping* the denied publish rather than rejecting it (e.g. a v3.1.1 client, which has no PUBACK reason codes, or a broker configured to drop). `PubackOutcome::BrokerRejected` is then unreachable with this broker and the harness assumption needs revisiting.
