TEST-ONLY ARTIFACTS — NOT SECRETS

TLS certificates are no longer checked in. The test CA, its `localhost` /
`127.0.0.1` server certificate, and the server key are generated fresh per test
run by `brenn_mqtt::test_support::certs` (one shared CA per test binary, via
`rcgen`), so no key material lives in the repo. Nothing to regenerate by hand.

The default and password-authentication listener configurations, the ACL and
the password file are string constants in `brenn_mqtt::test_support::broker`,
not files here: a crate above brenn-mqtt starts a broker without runfiles
plumbing. Only the TLS-1.3-only listener, which this suite alone uses, is a
file.
