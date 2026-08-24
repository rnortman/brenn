"""Declared-input plumbing for the repo-policy guards.

The guards scan the repo's own source files: condemned vocabulary, raw git
spawns, generated help sidecars, toolchain pins. Under make they take their
file set from `git ls-files`, which is invisible to a build graph — a cached
pass would replay over exactly the file that was added. Here the file set is a
`filegroup` per package, aggregated and written to a manifest, so the guards
are ordinary cached tests whose inputs are the files they read.
"""

# Everything a package holds is scanned — including BUILD.bazel and non-Rust
# assets — unless excluded here.
POLICY_SRC_EXCLUDE = [
    "**/*.db",
    "**/*.db-shm",
    "**/*.db-wal",
    "**/node_modules/**",
    "**/target/**",
    "**/dist/**",
    "brenn.brenn",
    ".bazelrc.local",
    # Operator-local state that sits beside tracked files: session credentials
    # and tool scratch. A glob sees untracked files that `git ls-files` does
    # not, so without these they would be hashed into cache keys and staged
    # into every policy test's sandbox. `*.local.*` is the repo's naming
    # convention for the family, excluded by convention rather than one name at
    # a time — nothing tracked is named that way. Two patterns because Bazel's
    # `*` does not match a leading dot, so `.gitleaks.local.toml` needs the
    # dotted form; `**` does descend into dot directories.
    "**/*.local.*",
    "**/.*.local.*",
    ".claude/scheduled_tasks.lock",
    "e2e/.auth/**",
    "rust-project.json",
]

def policy_srcs(name = "policy_srcs", extra_exclude = []):
    """Declare this package's contribution to the repo-wide policy scan.

    Globs everything the package owns. Subpackages are excluded by Bazel's own
    package boundaries and contribute their own `policy_srcs`, so the union of
    every declaration is the whole tree.

    Args:
        name: target name; the aggregate expects the default.
        extra_exclude: package-specific patterns to drop.
    """
    native.filegroup(
        name = name,
        srcs = native.glob(
            ["**"],
            exclude = POLICY_SRC_EXCLUDE + extra_exclude,
        ),
        visibility = ["//visibility:public"],
    )

def _source_manifest_impl(ctx):
    files = depset(transitive = [src[DefaultInfo].files for src in ctx.attr.srcs]).to_list()
    manifest = ctx.actions.declare_file(ctx.label.name + ".txt")
    ctx.actions.write(
        output = manifest,
        content = "".join([f.short_path + "\n" for f in files]),
    )
    return [DefaultInfo(
        files = depset([manifest]),
        runfiles = ctx.runfiles(files = files + [manifest]),
    )]

source_manifest = rule(
    doc = """One workspace-relative path per line, plus the files themselves as runfiles.

A consumer names this target in `data` and gets both the listing and everything
it lists, at the paths it lists them under.""",
    implementation = _source_manifest_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = True,
            doc = "File-providing targets to enumerate.",
        ),
    },
)
