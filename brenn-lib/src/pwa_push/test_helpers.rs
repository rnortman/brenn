use base64ct::{Base64UrlUnpadded, Encoding as _};

pub(crate) fn fake_p256dh() -> String {
    let mut bytes = vec![0x04u8];
    bytes.extend_from_slice(&[0xABu8; 64]);
    Base64UrlUnpadded::encode_string(&bytes)
}

pub(crate) fn fake_auth() -> String {
    Base64UrlUnpadded::encode_string(&[0xCDu8; 16])
}
