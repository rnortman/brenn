//! Per-target entropy shim: the single `seed()` the handle uses to seed the
//! core's backoff-jitter PRNG.
//!
//! Confined to the transport layer so the core and driver carry no `cfg` logic,
//! following the same shape as [`clock`](crate::transport::clock). The seed only
//! needs to be **distinct per client** so a fleet reconnecting in lockstep after
//! a deploy restart decorrelates its reconnects — it is load-spreading entropy,
//! never a secret, so neither source needs to be cryptographically strong.

/// A per-construction seed for the backoff-jitter PRNG. Distinct across clients
/// (that is the whole requirement); not a secret and not cryptographically
/// strong. Each target reads its own available entropy source.
#[cfg(target_arch = "wasm32")]
pub fn seed() -> u64 {
    // `Math.random` is per-page-load entropy — exactly the cross-page
    // decorrelation the fleet needs, with no `web-sys Crypto` feature and no new
    // crate. Two draws, each scaled into a `u32`'s worth of bits, combined into
    // one `u64`. The `a << 32` deliberately discards `a`'s top 21 bits (Rust
    // `<<` does not trap on value overflow); splitmix64 mixes the seed and only
    // cross-client distinctness matters, so this is not a bug to "fix" into a
    // dependency.
    let a = (js_sys::Math::random() * (2f64.powi(53))) as u64;
    let b = (js_sys::Math::random() * (2f64.powi(53))) as u64;
    (a << 32) ^ b
}

/// A per-construction seed for the backoff-jitter PRNG. Distinct across clients
/// (that is the whole requirement); not a secret and not cryptographically
/// strong.
#[cfg(not(target_arch = "wasm32"))]
pub fn seed() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    // A fresh `RandomState`'s keys are OS-seeded and distinct per construction,
    // so hashing a constant with it yields a distinct value each call. This is
    // deliberately not `SystemTime` nanos: two reads on a coarse-resolution clock
    // can be equal, which would make the "two seeds differ" test flaky. The
    // source needn't be cryptographically strong — it is a load-spreading seed,
    // not a secret.
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u8(0);
    hasher.finish()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn seed_differs_across_calls() {
        // Distinctness is the whole requirement: two fresh `RandomState`s are
        // OS-seeded with distinct per-instance keys, so two calls differ. This is
        // deterministic in practice (not a coarse-clock read that could tie).
        assert_ne!(seed(), seed());
    }
}
