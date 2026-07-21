use std::net::{IpAddr, Ipv4Addr};
use std::sync::OnceLock;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};

/// Returns the absolute path to the `mqtt_assets` directory shipped alongside
/// this test crate. Asset files are resolved relative to `CARGO_MANIFEST_DIR`
/// (set by Cargo at compile time and stable across `cargo test` invocations).
pub fn mqtt_assets_dir() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR is the directory containing `brenn-lib/Cargo.toml`.
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("tests").join("mqtt_assets")
}

/// A test CA plus a server leaf certificate it issued, all in PEM form.
///
/// Generated fresh per test binary (see [`test_certs`]) rather than checked in,
/// so no key material lives in the repo. The server cert carries
/// `CN=localhost` and SANs `DNS:localhost, IP:127.0.0.1`, matching what the
/// loopback broker and the rustls client both expect.
struct TestCerts {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
}

/// Process-wide singleton so the broker harness and every client in a test
/// binary share one CA. Each test binary (each process) generates its own set;
/// nothing is shared across processes, so no cross-process coordination exists.
fn test_certs() -> &'static TestCerts {
    static CERTS: OnceLock<TestCerts> = OnceLock::new();
    CERTS.get_or_init(generate_test_certs)
}

fn generate_test_certs() -> TestCerts {
    // Validity runs to 2125 so cert expiry can never flake a test.
    let not_after = rcgen::date_time_ymd(2125, 1, 1);

    let mut ca_params =
        CertificateParams::new(Vec::<String>::new()).expect("build CA certificate params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.not_after = not_after;
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "brenn-mqtt-test-ca");
    let ca_key = KeyPair::generate().expect("generate CA key pair");
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign CA");
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut server_params =
        CertificateParams::new(Vec::<String>::new()).expect("build server certificate params");
    server_params.not_after = not_after;
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().expect("localhost DNS SAN")),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_key = KeyPair::generate().expect("generate server key pair");
    let server_cert = server_params
        .signed_by(&server_key, &issuer)
        .expect("sign server cert with test CA");

    TestCerts {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
    }
}

/// The generated CA certificate in PEM, as the byte vector rustls client config
/// expects. Every MQTT integration test trusts this CA to verify the broker.
pub fn ca_pem_bytes() -> Vec<u8> {
    test_certs().ca_pem.clone().into_bytes()
}

/// The generated CA certificate PEM. The broker harness writes it into its temp
/// dir for mosquitto's `cafile`.
pub fn ca_pem() -> &'static str {
    &test_certs().ca_pem
}

/// The generated server leaf certificate PEM, for mosquitto's `certfile`.
pub fn server_cert_pem() -> &'static str {
    &test_certs().server_cert_pem
}

/// The generated server private key PEM, for mosquitto's `keyfile`.
pub fn server_key_pem() -> &'static str {
    &test_certs().server_key_pem
}
