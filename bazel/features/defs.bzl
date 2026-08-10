"""Feature lists that resolve differently per build configuration."""

# The `testutils` feature as a `crate_features` value: present unless
# `--//bazel/features:testutils=False` is set. Shared so the crates that gate
# helpers on it state the condition once.
TESTUTILS_FEATURES = select({
    "//bazel/features:testutils_enabled": ["testutils"],
    "//conditions:default": [],
})
