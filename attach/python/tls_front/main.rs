//! `tls_front`: a test-only TLS front for the binding's tests.
//!
//! Generates a fresh authority and a certificate it signs for `127.0.0.1` and
//! `localhost`, writes the authority's PEM to a file, and relays every TLS
//! connection on a loopback port to a plain loopback port. No key leaves the
//! process.
//!
//! Usage: `tls_front <upstream port> <authority PEM path>`. Prints
//! `listening on 127.0.0.1:<port>` once the authority is written and the
//! listener is bound.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::ServerConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [upstream, ca_out] = args.as_slice() else {
        eprintln!("usage: tls_front <upstream port> <authority PEM path>");
        std::process::exit(2);
    };
    let upstream: u16 = upstream.parse().expect("upstream port is a port number");
    let (ca_pem, chain, key) = certificates();
    std::fs::write(ca_out, ca_pem).expect("write the authority's PEM");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the default protocol versions")
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("the generated certificate and key form a server identity");
    let acceptor = TlsAcceptor::from(Arc::new(config));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build the runtime")
        .block_on(relay(acceptor, upstream));
}

/// The authority's PEM, and the server's chain and key.
fn certificates() -> (String, Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    // Validity runs to 2125 so expiry never fails a test.
    let not_after = rcgen::date_time_ymd(2125, 1, 1);

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("authority params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.not_after = not_after;
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "brenn-bus-test-ca");
    let ca_key = KeyPair::generate().expect("generate the authority's key");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .expect("self-sign the authority");
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut params = CertificateParams::new(Vec::<String>::new()).expect("server params");
    params.not_after = not_after;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().expect("localhost is a DNS name")),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let key = KeyPair::generate().expect("generate the server's key");
    let cert = params
        .signed_by(&key, &issuer)
        .expect("sign the server's certificate");

    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
    (ca_cert.pem(), vec![cert.der().clone()], key)
}

/// Accept TLS on a loopback port and copy each connection both ways to the
/// upstream port. A handshake the client abandons (an untrusted certificate)
/// ends that connection and nothing else. An upstream that refuses the
/// connect ends that connection with a line on stderr. How the copy ends is
/// not reported: a client that closes without TLS's close notice ends it in
/// an error as ordinary as a clean close.
async fn relay(acceptor: TlsAcceptor, upstream: u16) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind a loopback port");
    println!(
        "listening on {}",
        listener.local_addr().expect("the bound address")
    );
    loop {
        let (inbound, _) = listener.accept().await.expect("accept a connection");
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let Ok(mut tls) = acceptor.accept(inbound).await else {
                return;
            };
            let mut plain = match TcpStream::connect((Ipv4Addr::LOCALHOST, upstream)).await {
                Ok(plain) => plain,
                Err(error) => {
                    eprintln!("tls_front: connect to upstream 127.0.0.1:{upstream}: {error}");
                    return;
                }
            };
            let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
        });
    }
}
