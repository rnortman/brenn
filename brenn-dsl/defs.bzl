"""Target shapes the DSL package repeats: one per test suite, one per corpus file.

The parts that must agree across every copy — the fltk runtime deps, the
manifest-dir environment, the golden pair's two directions — agree by
construction.
"""

load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@crates//:defs.bzl", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_test")

def dsl_test(name):
    """An integration-test suite over the fixtures in this package.

    The suite joins `CARGO_MANIFEST_DIR` with a package-relative path, and a
    Bazel test starts in the runfiles root, so the manifest dir is this package's
    own path and the fixtures have to be declared runfiles.

    Args:
        name: the target name, and the basename of `tests/<name>.rs`.
    """
    rust_test(
        name = name,
        srcs = [
            "tests/{}.rs".format(name),
            "tests/support.rs",
        ],
        crate_root = "tests/{}.rs".format(name),
        data = native.glob([
            "tests/corpus/**",
            "grammar/*",
        ]),
        edition = "2024",
        proc_macro_deps = all_crate_deps(
            proc_macro = True,
            proc_macro_dev = True,
        ),
        rustc_env = {"CARGO_MANIFEST_DIR": native.package_name()},
        deps = all_crate_deps(
            normal = True,
            normal_dev = True,
        ) + [
            ":brenn-dsl",
            "@fltk//crates/fltk-cst-core:no_python",
            "@fltk//crates/fltk-serde-core:no_python",
        ],
    )

def _format(name, src, out):
    """Run `brennfmt` over one source file into one output file.

    The single statement of how the formatter is invoked, so the golden pair and
    the in-tree config check cannot start asking different questions.

    Args:
        name: the genrule's target name.
        src: the label of the file to format.
        out: the output file name, package-relative.
    """
    native.genrule(
        name = name,
        srcs = [src],
        outs = [out],
        cmd = "$(location //brenn-dsl:brennfmt) $(location {}) > $@".format(src),
        tools = ["//brenn-dsl:brennfmt"],
    )

def format_goldens(name):
    """The byte-exact layout golden for one corpus file, and its idempotence.

    The golden IS the definition of what the format spec produces: editing
    `grammar/brenn.fltkfmt` rebuilds the formatter and fails these until the
    golden is regenerated deliberately. Byte-exact because a formatter silently
    gaining or losing a byte changes every diff its users read.

    The two directions are one concept and take one call: formatting the source
    yields the canonical form, and formatting the canonical form changes nothing.

    Args:
        name: the corpus basename, for `tests/corpus/<name>.brenn` and
            `tests/corpus/<name>.canonical.brenn`.
    """
    canonical = "tests/corpus/{}.canonical.brenn".format(name)

    for direction, src in [
        ("formatted", "tests/corpus/{}.brenn".format(name)),
        ("reformatted", canonical),
    ]:
        _format(
            name = "{}_{}".format(name, direction),
            src = src,
            out = "{}.{}.txt".format(name, direction),
        )

    diff_test(
        name = "{}_canonical_layout_test".format(name),
        file1 = ":{}_formatted".format(name),
        file2 = canonical,
    )

    diff_test(
        name = "{}_idempotence_test".format(name),
        file1 = ":{}_reformatted".format(name),
        file2 = canonical,
    )

def format_check(name, src):
    """One in-tree `.brenn` document, asserted to be its own canonical form.

    A config file that is not byte-identical to what `brennfmt` produces from it
    fails `bazel test //...`, hence `make check`. The check is the Bazel-native
    diff rather than a `--check` flag on the formatter: the same question, with
    no dependency on a CLI surface defined outside this repo.

    Callable from any package: the formatter is addressed absolutely, so the
    check can live beside the documents it gates rather than beside the tool.

    Args:
        name: a target-name stem, unique within the calling package.
        src: the label of the document to check.
    """
    _format(
        name = "{}_config_formatted".format(name),
        src = src,
        out = "{}.config-formatted.brenn".format(name),
    )

    diff_test(
        name = "{}_canonical_format_test".format(name),
        file1 = ":{}_config_formatted".format(name),
        file2 = src,
    )
