//! The half of the vend enforcement a cargo run cannot observe.
//!
//! `common::vendored_gitleaks` panics on an unvended gitleaks only when
//! `BAZEL_TEST_MARKER` is set, and reads its absence as "a developer machine
//! running cargo". If Bazel ever stops setting that variable — a runner change,
//! a wrapper that scrubs the environment, a typo in the constant — the
//! enforcement goes inert and looks exactly like the skip it is meant to allow:
//! both lanes stay green over nothing asserted.
//!
//! So this asserts the marker is live, from a target only Bazel builds. It lives
//! outside `tests/` on purpose: cargo's integration-test discovery would pick it
//! up there and fail every developer's `cargo test`, where the marker is
//! correctly absent.

#[path = "../tests/common/mod.rs"]
mod common;

#[test]
fn bazel_sets_the_marker_the_vend_check_reads() {
    assert!(
        std::env::var_os(common::BAZEL_TEST_MARKER).is_some(),
        "Bazel's test runner did not set {}, which is what tells \
         scrub/tests/common/mod.rs it is running under Bazel. Every scrub suite \
         now reads an unvended gitleaks as a developer machine and skips its \
         scan-reaching assertions instead of failing. Find the variable Bazel \
         sets in its place and point the constant at it.",
        common::BAZEL_TEST_MARKER
    );
}
