# brenn-mqtt Integration Tests

## MQTT Integration Suite

Tests the `MqttService` + `ConnectionSupervisor` stack against a live `mosquitto` broker over TLS.

### Prerequisites

None beyond Bazel. The broker is `//bazel/tools:mosquitto`, built from the
mosquitto source release `MODULE.bazel` pins (`MOSQUITTO_VERSION`, 2.0.22)
under the build's hermetic C toolchain, and handed to the test target as a
declared input: the target's `data` carries the binary and its `env` sets
`BRENN_MOSQUITTO_BIN` to the runfile path. No host install is read: the
harness points the broker's `OPENSSL_CONF` at an empty file, so its statically
linked OpenSSL never loads the host's `/etc/ssl/openssl.cnf` crypto policy.

### Running

```
bazel test //brenn-mqtt:mqtt_integration
```

The target sets the `BRENN_MQTT_INTEGRATION` gate variable itself. `make check`
runs it as part of `bazel test //...`; `tags = ["requires-network"]` is the
sandbox permission to reach loopback, not an exclusion.

### Expected pass output

The case list is the binary's own: read it with `--list` rather than from a
copy here. Test order varies run to run (each is an independent test); the set
is what matters.

The three `wasm_egress_*` tests are the WASM-egress acceptance layer: they drive
`enforce_and_publish` (the shared enforcement pipeline the WASM `mqtt:publish`
callback uses) against the live broker.

### Human-review gate

Before merge, the developer runs the suite 10 consecutive times with no flakes:

```
bazel test //brenn-mqtt:mqtt_integration --runs_per_test=10 --nocache_test_results
```

Run it at Bazel's default `--local_test_jobs`, so the ten processes run in
parallel and contend for ephemeral ports. A serialised run
(`--local_test_jobs=1`) does not exercise port contention and is not the gate.

### Failure modes

**`mosquitto readiness timeout`** / **`mosquitto exited … not a bind collision`**
: mosquitto exited, or did not log its `running` line within 2 seconds. Readiness is that line in the broker's own log, written once its listener is bound; the printed log shows how far start-up got (`starting`, `Config loaded`, `Opening ipv4 listen socket`, or nothing). The panic message includes the last 4 KiB of the log file and the broker's stderr. Most common cause: mosquitto config rejected (cert/key path wrong, ACL syntax). A config rejected before the log opens — an unreadable config file, an unknown directive — appears on stderr and only there; the log section then reads `<log file not found>`.

**`BRENN_MOSQUITTO_BIN is unset`** / **`BRENN_MOSQUITTO_BIN names …, which is not a file`**
: The test target lost its `env` entry or its `//bazel/tools:mosquitto` `data` entry. Outside Bazel, setting `BRENN_MQTT_INTEGRATION` by hand also requires setting `BRENN_MOSQUITTO_BIN` to a broker binary.

**TLS handshake failure**
: Confirm the broker URL host matches a SAN in the generated server cert (`localhost` and `127.0.0.1` are covered; other hostnames fail validation). SAN coverage is set in `brenn-mqtt/src/test_support/certs.rs`. Check that `tls_version_min = "1.2"` matches what the broker config negotiates. Cert expiry cannot happen — certs are generated per-run by `brenn-mqtt/src/test_support/certs.rs` (not_after 2125), not checked in.

**ACL denied**
: The test ACL permits `brenn/itest/#` only. Tests must use topic prefixes within that tree. If you see ACL-denied errors in mosquitto logs, check the test topic prefixes.

**`wasm_egress_broker_rejected_maps_reason` fails or hangs**
: This test publishes QoS 1 to a topic **outside** `brenn/itest/#` and expects the broker to reject it with a PUBACK non-success reason code (mosquitto 2.x answers a v5 client's ACL-denied publish with reason 0x87). Two distinct failures:
  - **`Ok(())` returned** — the broker accepted a publish outside its ACL tree, so the ACL is not being enforced (bad broker config / wrong ACL file). The `BrokerRejected` assertion is meaningless until this is fixed.
  - **Timed out with no PUBACK** — the broker is silently *dropping* the denied publish rather than rejecting it (e.g. a v3.1.1 client, which has no PUBACK reason codes, or a broker configured to drop). `PubackOutcome::BrokerRejected` is then unreachable with this broker and the harness assumption needs revisiting.
