"""Generated code as build artifacts, and the gates pinning it to the tree.

Nothing generated is committed in the end state: each family is an ordinary
action whose output feeds its consumers as a declared input. Until the make
lanes and the deploy path stop reading the committed copies, both exist, and
the parity gates here hold them byte-identical — which is also the migration's
evidence that the two generators agree.

Two shapes, because the families come in two: one file (`bindings.rs`,
`frontmatter.generated.ts`) and one directory (the ts-rs export, whose file set
is itself a thing that can drift).
"""

load("@bazel_skylib//lib:shell.bzl", "shell")
load("//bazel/platforms:defs.bzl", "HOST_ONLY")

# The gate logic lives in checked-in scripts so `//bazel/gencode/tests` can
# exercise it directly; the generated file is only the argument binding.
_CHECK_WRAPPER = """#!/usr/bin/env bash
exec {check} {args}
"""

def _check_wrapper(ctx, check, args):
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        output = script,
        content = _CHECK_WRAPPER.format(
            check = shell.quote(check.short_path),
            args = " ".join([shell.quote(a) for a in args]),
        ),
        is_executable = True,
    )
    return script

# ---------------------------------------------------------------------------
# ts-rs
# ---------------------------------------------------------------------------

def _testutils_on_impl(_settings, _attr):
    return {"//bazel/features:testutils": True}

# The generator is a test binary, and the release configuration clears
# `testutils` — under which the test binary does not compile at all. The
# feature is a property of what a test needs, not of the build being a release,
# so the generator carries it back regardless of the configuration above it.
_testutils_on = transition(
    implementation = _testutils_on_impl,
    inputs = [],
    outputs = ["//bazel/features:testutils"],
)

def _ts_rs_export_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)

    generator = ctx.executable.generator

    args = ctx.actions.args()
    args.add(out.path)
    args.add(generator.path)
    args.add(ctx.attr.filter)

    # The binary alone, not its runfiles: the exporting tests read no data, and
    # staging the runfiles would put the test target's fixtures — for
    # `brenn-server` that is the whole surface asset tree — in this action's
    # input closure, so an unrelated asset change would regenerate the types.
    ctx.actions.run(
        outputs = [out],
        executable = ctx.file._export,
        tools = [generator],
        arguments = [args],
        mnemonic = "TsRsExport",
        progress_message = "Exporting ts-rs bindings from %s" % ctx.attr.generator[0].label,
    )
    return [DefaultInfo(files = depset([out]))]

ts_rs_export = rule(
    implementation = _ts_rs_export_impl,
    doc = """One crate's ts-rs TypeScript, exported into a declared directory.

    The generator is the crate's own test binary: ts-rs exports from tests, and
    the derive emits one `export_bindings_<type>` test per `#[ts(export)]` type,
    so filtering on that prefix covers every exported type without a list here
    that a new type could be left out of.

    The binary is taken in the target configuration, not the exec one. Both are
    the host here, and asking for the exec configuration would build a second
    copy of the heaviest crates in the tree for no difference in output.
    """,
    attrs = {
        "filter": attr.string(
            default = "export_bindings_",
            doc = "libtest substring filter selecting the exporting tests.",
        ),
        "generator": attr.label(
            cfg = _testutils_on,
            executable = True,
            mandatory = True,
            doc = "The crate's test binary.",
        ),
        "_export": attr.label(
            allow_single_file = True,
            default = Label("//bazel/gencode:ts_rs_export.sh"),
        ),
    },
)

# ---------------------------------------------------------------------------
# Parity gates
# ---------------------------------------------------------------------------

def _generated_parity_test_impl(ctx):
    # The generated file's workspace-relative path may coincide with the
    # committed one's — it does for the single-file gencode families — and when
    # it does, staging both in one runfiles tree would put only one there and
    # compare it against itself. So the generated side always gets a name of its
    # own.
    generated = ctx.actions.declare_file(ctx.label.name + ".generated")
    ctx.actions.symlink(output = generated, target_file = ctx.file.generated)

    check = ctx.file._check
    script = _check_wrapper(ctx, check, [
        ctx.file.committed.short_path,
        generated.short_path,
        str(ctx.attr.committed.label),
    ])
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = [ctx.file.committed, generated, check]),
    )]

_generated_parity_test = rule(
    implementation = _generated_parity_test_impl,
    doc = """Pin a committed generated file to the bytes Bazel generates.

    Migration-window only: the make lanes and the deploy path still read the
    committed copies, so the two generators have to agree byte for byte. When
    the committed copies go, so does this rule.
    """,
    attrs = {
        "committed": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "generated": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/gencode:parity_check.sh"),
        ),
    },
    test = True,
)

def generated_parity_test(name, committed, generated):
    """See `_generated_parity_test`.

    Args:
        name: target name.
        committed: the committed copy, still read by the make lanes.
        generated: the target producing the same file as a build artifact.
    """
    _generated_parity_test(
        name = name,
        committed = committed,
        generated = generated,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )

def _generated_tree_parity_test_impl(ctx):
    generated = ctx.file.generated
    if not generated.is_directory:
        fail("%s must produce a directory" % ctx.attr.generated.label)

    check = ctx.file._check
    committed_dir = ctx.label.package + "/" + ctx.attr.committed_dir
    script = _check_wrapper(ctx, check, [
        committed_dir,
        generated.short_path,
        committed_dir,
    ])
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = ctx.files.committed + [generated, check]),
    )]

_generated_tree_parity_test = rule(
    implementation = _generated_tree_parity_test_impl,
    doc = """Pin a committed directory of generated files to a generated tree.

    The file-shaped gate cannot see a family whose membership changes; this one
    compares the sets before the bytes.
    """,
    attrs = {
        "committed": attr.label_list(
            allow_files = True,
            mandatory = True,
            doc = "Every committed file under `committed_dir`.",
        ),
        "committed_dir": attr.string(
            mandatory = True,
            doc = "Package-relative directory holding the committed copies.",
        ),
        "generated": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "A target producing exactly one directory.",
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/gencode:tree_parity_check.sh"),
        ),
    },
    test = True,
)

def generated_tree_parity_test(name, committed_dir, committed, generated):
    """See `_generated_tree_parity_test`.

    Args:
        name: target name.
        committed_dir: package-relative directory holding the committed copies.
        committed: every committed file under that directory.
        generated: the target producing the same tree as a build artifact.
    """
    _generated_tree_parity_test(
        name = name,
        committed = committed,
        committed_dir = committed_dir,
        generated = generated,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )
