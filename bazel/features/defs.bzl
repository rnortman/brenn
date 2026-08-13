"""Feature lists that resolve differently per build configuration."""

# The `testutils` feature as a `crate_features` value: present unless
# `--//bazel/features:testutils=False` is set. Shared so the crates that gate
# helpers on it state the condition once.
TESTUTILS_FEATURES = select({
    "//bazel/features:testutils_enabled": ["testutils"],
    "//conditions:default": [],
})

def testutils_deps(labels):
    """First-party deps that only the `testutils` half of a crate names.

    Cargo states them `optional = true` behind the feature; this is the same
    condition as a `deps` value, so a configuration that clears the feature
    also drops the build edge instead of carrying it into the release graph.

    Args:
        labels: labels reached only from code gated on `testutils`.

    Returns:
        A `select()` yielding `labels` when the feature is on, `[]` otherwise.
    """
    return select({
        "//bazel/features:testutils_enabled": labels,
        "//conditions:default": [],
    })
