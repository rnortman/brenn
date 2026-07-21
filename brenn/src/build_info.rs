//! Compile-time build identifier shared between the Rust binary and the
//! JS bundle.
//!
//! The string is opaque from the handshake's point of view — byte-equal
//! match, nothing more. Values in practice: a short git SHA
//! (e.g. `dfdced0`), a semver tag (`0.2.0`), or `unknown-dev` for
//! Makefile-less local `cargo build` invocations when the Makefile
//! fallback is not in play.
//!
//! Uses `env!` (compile-time) rather than `option_env!` so the build
//! fails fast if `BRENN_BUILD_ID` is unset when the crate is compiled
//! directly — matches CLAUDE.md "no fallbacks, fail fast on unexpected."
//! `make build` / `make launchdev` / `make release-musl` always export
//! the variable (see the Makefile), so the only path where this compile
//! error fires is bare `cargo build` without the Makefile wrapper.
pub const BUILD_ID: &str = env!("BRENN_BUILD_ID");

/// `env!` already rejects an unset variable at compile time. This assertion
/// covers the remaining failure modes at compile time too: an accidentally
/// empty string, and a value longer than the 64-char cap. The cap keeps the
/// WS Close-frame reason (which carries `BUILD_ID`) safely below the RFC 6455
/// 123-byte limit.
const _: () = assert!(!BUILD_ID.is_empty() && BUILD_ID.len() <= 64);
