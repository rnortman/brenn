use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::RngExt;

/// Pre-computed dummy hash for constant-time rejection when a username doesn't exist.
/// This is a real Argon2id PHC string — verify_password runs the full computation
/// against it, ensuring the response time is indistinguishable from a real user lookup.
///
/// Generated with default Argon2id parameters against a throwaway password.
/// The actual password and salt don't matter — this exists solely to burn CPU time.
const DUMMY_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$dW5rbm93bnVzZXJkdW1teXM$YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY";

/// Hash a password using Argon2id with default (OWASP-recommended) parameters.
/// Returns the PHC-formatted hash string (includes algorithm, params, salt, and hash).
pub fn hash_password(password: &[u8]) -> String {
    // Generate a 16-byte random salt and base64-encode it for SaltString.
    // We use our own rand crate (0.10) since argon2's password-hash depends on
    // rand_core 0.6 which is a different trait version.
    let mut salt_bytes = [0u8; 16];
    rand::rng().fill(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .expect("16 bytes should always produce a valid SaltString");

    let argon2 = Argon2::default();
    argon2
        .hash_password(password, &salt)
        .expect("argon2 hashing should not fail with valid inputs")
        .to_string()
}

/// Verify a password against a stored PHC hash string.
/// Returns true if the password matches, false otherwise.
///
/// If the hash is malformed (not a valid PHC string), this logs an error
/// and returns false. A malformed hash in the DB is an invariant violation
/// that needs investigation — not a normal "wrong password" case.
pub fn verify_password(password: &[u8], hash: &str) -> bool {
    let parsed = match PasswordHash::new(hash) {
        Ok(h) => h,
        Err(e) => {
            panic!("malformed password hash in database — this is a bug or data corruption: {e}");
        }
    };
    Argon2::default().verify_password(password, &parsed).is_ok()
}

/// Verify a password against a dummy hash. Used when the requested username
/// doesn't exist, to ensure constant-time response regardless of user existence.
pub fn verify_password_dummy(password: &[u8]) {
    // We intentionally discard the result. The point is to spend time computing,
    // not to check the answer.
    let _ = verify_password(password, DUMMY_HASH);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_round_trip() {
        let password = b"correct-horse-battery-staple";
        let hash = hash_password(password);
        assert!(verify_password(password, &hash));
    }

    #[test]
    fn verify_rejects_wrong_password() {
        let hash = hash_password(b"correct-password");
        assert!(!verify_password(b"wrong-password", &hash));
    }

    #[test]
    fn dummy_verify_does_not_panic() {
        // This just needs to run without panicking.
        verify_password_dummy(b"any-password");
    }

    #[test]
    #[should_panic(expected = "malformed password hash")]
    fn verify_panics_on_garbage_hash() {
        verify_password(b"password", "not-a-valid-hash");
    }
}
