//! Well-formed but meaningless push-subscription credentials, for the
//! round-trip tests that need a `PushSubscription` shaped like a real one.
//!
//! The values only have to survive base64url decoding and the length checks the
//! wire types carry; nothing here is a key.

use base64ct::{Base64UrlUnpadded, Encoding as _};

pub(super) fn fake_p256dh() -> String {
    let mut bytes = vec![0x04u8];
    bytes.extend_from_slice(&[0xABu8; 64]);
    Base64UrlUnpadded::encode_string(&bytes)
}

pub(super) fn fake_auth() -> String {
    Base64UrlUnpadded::encode_string(&[0xCDu8; 16])
}
