TEST-ONLY ARTIFACTS — NOT SECRETS

TLS certificates are no longer checked in. The test CA, its `localhost` /
`127.0.0.1` server certificate, and the server key are generated fresh per test
run by `brenn_mqtt::test_support::certs` (one shared CA per test binary, via
`rcgen`), so no key material lives in the repo. Nothing to regenerate by hand.

The default listener configuration and the ACL are string constants in
`brenn_mqtt::test_support::broker`, not files here: a crate above brenn-mqtt
starts a broker without runfiles plumbing. Only the two variant listeners this
suite alone uses are files.

`passwd` is a mosquitto password file for the auth-broker template
(`mosquitto.conf.auth.tmpl`, used by `common::broker_auth`). One user,
deliberately-documented test-only credentials: `brenn-itest` /
`brenn-itest-password`. Checked in (as the `$7$` sha512-pbkdf2 hash mosquitto 2.x
writes) so tests need no `mosquitto_passwd` at runtime. If a contributor's
mosquitto is too old to read the format, the auth control test
(`auth_good_credentials_connects`) fails loudly at connect; regenerate with:

```
mosquitto_passwd -c -b passwd brenn-itest brenn-itest-password
```
