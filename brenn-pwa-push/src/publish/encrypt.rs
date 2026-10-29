//! Web Push message encryption (RFC 8291) over the `aes128gcm` content
//! encoding (RFC 8188).
//!
//! A fresh application-server P-256 key and a fresh 16-byte salt per message;
//! the ECDH secret with the subscription's `p256dh` key and the subscription's
//! `auth` secret are combined into the input keying material (RFC 8291 §3.4),
//! and `ece_native` performs the RFC 8188 record encryption with the
//! application-server public key as the `keyid` (RFC 8291 §4).

use hkdf::Hkdf;
use p256::ecdh::SharedSecret;
use p256::elliptic_curve::Generate as _;
use p256::elliptic_curve::sec1::ToSec1Point as _;
use rand::RngExt as _;
use sha2::Sha256;

/// The subscription's 16-byte `auth` secret.
pub(crate) type Auth = [u8; 16];

/// RFC 8291 §3.4 `key_info` prefix: `"WebPush: info" || 0x00`.
const KEY_INFO_PREFIX: &[u8] = b"WebPush: info\0";

/// Encrypts `plaintext` for the subscription identified by `ua_public` and
/// `ua_auth` with a fresh salt and a fresh ephemeral key, as one record of
/// size `plaintext.len() + 17` (the smallest that holds the plaintext, the
/// padding delimiter and the 16-byte tag).
///
/// # Panics
///
/// If `plaintext.len() + 17` does not fit a `u32`, or if `ece_native` refuses
/// the input. Neither is reachable for a push payload: the payload is bounded
/// JSON, the `keyid` is a 65-byte point (≤ 255) and the single record fits its
/// record size by construction.
pub(crate) fn encrypt(plaintext: Vec<u8>, ua_public: &p256::PublicKey, ua_auth: &Auth) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut salt = [0u8; 16];
    rng.fill(&mut salt);
    let as_secret = p256::SecretKey::generate_from_rng(&mut rng);
    let record_size = u32::try_from(plaintext.len() + 17)
        .expect("push payload length + 17 fits a u32 (payloads are bounded JSON)");
    encrypt_with(salt, &as_secret, plaintext, ua_public, ua_auth, record_size).unwrap_or_else(|e| {
        panic!(
            "aes128gcm encryption refused a single record of size len + 17 with a \
                 65-byte keyid, which it cannot: {e}"
        )
    })
}

/// The deterministic core of [`encrypt`]: every input supplied by the caller.
pub(crate) fn encrypt_with(
    salt: [u8; 16],
    as_secret: &p256::SecretKey,
    plaintext: Vec<u8>,
    ua_public: &p256::PublicKey,
    ua_auth: &Auth,
    record_size: u32,
) -> Result<Vec<u8>, ece_native::Error> {
    let as_public = as_secret.public_key();
    let shared = p256::ecdh::diffie_hellman(as_secret.to_nonzero_scalar(), ua_public.as_affine());
    let ikm = derive_ikm(&shared, ua_auth, ua_public, &as_public);
    let keyid = as_public.to_sec1_point(false);
    ece_native::encrypt(
        ikm,
        salt,
        keyid.as_bytes(),
        std::iter::once(plaintext),
        record_size,
    )
}

/// RFC 8291 §3.4 `key_info`: `"WebPush: info" || 0x00 || ua_public ||
/// as_public`, both keys in uncompressed SEC1 form (65 bytes each).
fn key_info(ua_public: &p256::PublicKey, as_public: &p256::PublicKey) -> Vec<u8> {
    let ua = ua_public.to_sec1_point(false);
    let as_ = as_public.to_sec1_point(false);
    let mut info = Vec::with_capacity(KEY_INFO_PREFIX.len() + 130);
    info.extend_from_slice(KEY_INFO_PREFIX);
    info.extend_from_slice(ua.as_bytes());
    info.extend_from_slice(as_.as_bytes());
    info
}

/// RFC 8291 §3.4 `IKM = HKDF-SHA-256(salt = auth_secret, ikm = ecdh_secret,
/// info = key_info, L = 32)`. Symmetric in who computed `shared`: the
/// application server and the user agent derive the same value.
fn derive_ikm(
    shared: &SharedSecret,
    ua_auth: &Auth,
    ua_public: &p256::PublicKey,
    as_public: &p256::PublicKey,
) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(ua_auth), shared.raw_secret_bytes());
    let mut ikm = [0u8; 32];
    hk.expand(&key_info(ua_public, as_public), &mut ikm)
        .expect("32 bytes is a valid HKDF-SHA-256 output length");
    ikm
}

#[cfg(test)]
mod tests {
    use base64ct::{Base64UrlUnpadded, Encoding as _};

    use super::*;

    /// Decodes a base64url value as printed in RFC 8291, whitespace removed.
    fn b64(s: &str) -> Vec<u8> {
        let joined: String = s.split_whitespace().collect();
        Base64UrlUnpadded::decode_vec(&joined).expect("RFC 8291 value is valid base64url")
    }

    // RFC 8291 Section 5 and Appendix A, transcribed from the RFC text.
    const PLAINTEXT: &str = "V2hlbiBJIGdyb3cgdXAsIEkgd2FudCB0byBiZSBhIHdhdGVybWVsb24";
    const AS_PUBLIC: &str = "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIg
                             Dll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8";
    const AS_PRIVATE: &str = "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw";
    const UA_PUBLIC: &str = "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-
                             JvLexhqUzORcx aOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4";
    const UA_PRIVATE: &str = "q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94";
    const SALT: &str = "DGv6ra1nlYgDCS1FRnbzlw";
    const AUTH_SECRET: &str = "BTBZMqHH6r4Tts7J_aSIgg";
    const ECDH_SECRET: &str = "kyrL1jIIOHEzg3sM2ZWRHDRB62YACZhhSlknJ672kSs";
    const PRK_KEY: &str = "Snr3JMxaHVDXHWJn5wdC52WjpCtd2EIEGBykDcZW32k";
    const KEY_INFO: &str = "V2ViUHVzaDogaW5mbwAEJXGyvs3942BVG
                            q8e0PTNNmwR zr5VX4m8t7GGpTM5FzFo7OLr4BhZe9MEebhuPI-OztV3
                            ylkYfpJGmQ22ggCLDgT-M_SrDepxkU21WCP3O1SUj0Ew
                            bZIHMtu5pZpTKGSCIA5Zent7wmC6HCJ5mFgJkuk5cwAv MBKiiujwa7t45ewP";
    const IKM: &str = "S4lYMb_L0FxCeq0WhDx813KgSYqU26kOyzWUdsXYyrg";
    /// The RFC 8291 Section 5 message body: the 86-octet header followed by
    /// the ciphertext.
    const MESSAGE: &str = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27ml
                           mlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPT
                           pK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN";
    /// The Appendix A header alone, for naming a header-vs-body mismatch.
    const HEADER: &str = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z 9KsN6nGRTbVYI_c7VJSPQTBtkgcy27ml
                          mlMoZIIgDll6e3vCYLocInmYWAmS6Tlz AC8wEqKK6PBru3jl7A8";
    const RECORD_SIZE: u32 = 4096;

    fn auth() -> Auth {
        b64(AUTH_SECRET)
            .try_into()
            .expect("RFC 8291 auth secret is 16 bytes")
    }

    fn salt() -> [u8; 16] {
        b64(SALT).try_into().expect("RFC 8291 salt is 16 bytes")
    }

    fn secret(s: &str) -> p256::SecretKey {
        p256::SecretKey::from_slice(&b64(s)).expect("RFC 8291 private key is a valid scalar")
    }

    fn public(s: &str) -> p256::PublicKey {
        p256::PublicKey::from_sec1_bytes(&b64(s)).expect("RFC 8291 public key is a valid point")
    }

    /// The transcribed keys are consistent with each other: each private key
    /// derives its printed public key.
    #[test]
    fn rfc8291_appendix_a_keys_are_consistent() {
        assert_eq!(secret(AS_PRIVATE).public_key(), public(AS_PUBLIC));
        assert_eq!(secret(UA_PRIVATE).public_key(), public(UA_PUBLIC));
    }

    /// Each intermediate of the key combination against the appendix, so a
    /// wrong step is named rather than inferred from the final bytes.
    #[test]
    fn rfc8291_appendix_a_ikm_intermediates() {
        let as_secret = secret(AS_PRIVATE);
        let ua_public = public(UA_PUBLIC);
        let as_public = as_secret.public_key();

        let shared =
            p256::ecdh::diffie_hellman(as_secret.to_nonzero_scalar(), ua_public.as_affine());
        assert_eq!(
            shared.raw_secret_bytes().as_slice(),
            b64(ECDH_SECRET).as_slice(),
            "ecdh_secret"
        );

        let (prk, _) = Hkdf::<Sha256>::extract(Some(&auth()), shared.raw_secret_bytes());
        assert_eq!(prk.as_slice(), b64(PRK_KEY).as_slice(), "PRK_key");

        assert_eq!(key_info(&ua_public, &as_public), b64(KEY_INFO), "key_info");

        assert_eq!(
            derive_ikm(&shared, &auth(), &ua_public, &as_public).as_slice(),
            b64(IKM).as_slice(),
            "IKM"
        );
    }

    /// `encrypt_with` on the appendix's inputs produces the RFC's message byte
    /// for byte. A failure means the implementation is wrong; the vector is
    /// the RFC's and is never regenerated from this code or a library.
    #[test]
    fn rfc8291_appendix_a_message() {
        let out = encrypt_with(
            salt(),
            &secret(AS_PRIVATE),
            b64(PLAINTEXT),
            &public(UA_PUBLIC),
            &auth(),
            RECORD_SIZE,
        )
        .expect("RFC 8291 example encrypts");
        let header = b64(HEADER);
        assert_eq!(header.len(), 86);
        assert_eq!(&out[..86], header.as_slice(), "RFC 8188 header");
        assert_eq!(out, b64(MESSAGE), "RFC 8291 Section 5 message body");
    }

    /// Decrypts `msg` as the user agent would: the application-server key is
    /// read from the header's `keyid`, the IKM derived from the UA's side of
    /// the ECDH exchange.
    fn ua_decrypt(msg: Vec<u8>, ua_secret: &p256::SecretKey, ua_auth: &Auth) -> Vec<u8> {
        assert_eq!(msg[20], 65, "keyid length");
        let as_public =
            p256::PublicKey::from_sec1_bytes(&msg[21..86]).expect("keyid is a SEC1 P-256 point");
        let ua_public = ua_secret.public_key();
        let shared =
            p256::ecdh::diffie_hellman(ua_secret.to_nonzero_scalar(), as_public.as_affine());
        let ikm = derive_ikm(&shared, ua_auth, &ua_public, &as_public);
        ece_native::decrypt(ikm, msg).expect("message decrypts under the UA-derived IKM")
    }

    #[test]
    fn encrypt_round_trips_through_the_user_agent_side() {
        let ua_secret = secret(UA_PRIVATE);
        let ua_auth = auth();
        let plaintext = br#"{"title":"t","body":"b","user_id":1}"#.to_vec();

        let msg = encrypt(plaintext.clone(), &ua_secret.public_key(), &ua_auth);
        let record_size = u32::from_be_bytes(msg[16..20].try_into().expect("4 bytes"));
        assert_eq!(record_size as usize, plaintext.len() + 17);
        assert_eq!(ua_decrypt(msg, &ua_secret, &ua_auth), plaintext);
    }

    #[test]
    fn encrypt_uses_a_fresh_salt_and_key_per_call() {
        let ua_public = secret(UA_PRIVATE).public_key();
        let a = encrypt(b"same".to_vec(), &ua_public, &auth());
        let b = encrypt(b"same".to_vec(), &ua_public, &auth());
        assert_ne!(a[..16], b[..16], "salt must differ between calls");
        assert_ne!(
            a[21..86],
            b[21..86],
            "ephemeral key must differ between calls"
        );
    }
}
