"""Build rules for the surface browser tree.

Every crate under `surface/` that the browser loads is built twice: once as an
ordinary host `rust_library` — the DOM-free logic and the help generator, which
is what the host suites exercise — and once for wasm32. The kernel's wasm32 half
is a cdylib that `rust_wasm_bindgen` turns into the `--target web` ES-module
bundle the page imports; a component's is a component-model artifact the page
hosts through jco.

The dir → crate → artifact mapping is frozen: a component in `surface/<kind>`
is crate `brenn-<kind>` and ships under the kind. Every consumer addresses a
component by its kind, so `surface_processor_component` derives crate and
artifact from the package path instead of taking them as attributes.
"""

load("@aspect_bazel_lib//lib:copy_to_directory.bzl", "copy_to_directory")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@crates//:defs.bzl", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_doc_test", "rust_library", "rust_shared_library", "rust_test")
load("@rules_rust_wasm_bindgen//:defs.bzl", "rust_wasm_bindgen")
load("//bazel/gencode:defs.bzl", "generated_parity_test")
load("//bazel/platforms:defs.bzl", "HOST_ONLY", "WASM32_ONLY")
load("//bazel/wasm:defs.bzl", "guest_spec_scaffold", "replace_generated", "wasm_component")
load(":dist.bzl", "surface_crate_stage")

# Where the component dirs live: any direct child of these packages is a
# component, and its directory name is the kind.
_COMPONENT_PARENTS = ["surface/components"]

# Components that sit somewhere else, named one by one. `surface/chrome` is the
# in-tree default chrome — an ordinary contract component that happens to live
# beside the component dir rather than under it. Listing the package exactly is
# what keeps the parent check from admitting every other child of `surface/`,
# which would mint a crate name for `surface/schema` as readily as for a real
# component.
_COMPONENT_PACKAGES = ["surface/chrome"]

def surface_wasm_crate(
        name,
        crate_name,
        artifact,
        edition = "2024",
        deps = [],
        wasm_deps = [],
        test_deps = [],
        test_data = [],
        test_rustc_env = {},
        test_tags = [],
        sidecars = [],
        generated_srcs = {},
        visibility = ["//visibility:public"]):
    """A surface crate: host library, host tests, browser bundle, dist stage.

    Args:
        name: target name of the host library; the browser targets suffix it.
        crate_name: the Rust crate name, underscored.
        artifact: the bundle's `brenn_*` basename in the surface asset tree.
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: first-party deps both builds take.
        wasm_deps: first-party deps only the browser build takes.
        test_deps: first-party deps of the host unit tests.
        test_data: runtime files the host unit tests read.
        test_rustc_env: extra compile-time environment for the host unit tests.
        test_tags: tags for the host unit test target.
        sidecars: files shipped flat beside the bundle in the asset tree —
            documentation, and for a component its binding record and packaged
            specification; hand-authored ones are renamed to the artifact's
            basename, generated ones already carry it.
        generated_srcs: modules generated into this crate, keyed by the path
            each occupies — `{"src/spec.rs": ":<kind>_spec"}` for a
            specification-bearing component. They feed both builds: a generated
            module is plain Rust with no browser dependency, so the host half
            compiles it too and the host suites see the same port names the page
            does. See `replace_generated`.
        visibility: visibility of the host library and the bundle.
    """
    srcs = replace_generated(native.glob(["src/**/*.rs"]), generated_srcs)

    # The host half compiles the crate with its browser glue cfg'd out, which is
    # what makes it host-only: under wasm32 the same sources would want the
    # browser deps the cdylib target carries.
    rust_library(
        name = name,
        srcs = srcs,
        crate_name = crate_name,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
        deps = all_crate_deps(normal = True) + deps,
    )

    rust_test(
        name = name + "_test",
        crate = ":" + name,
        data = test_data,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro_dev = True),
        rustc_env = test_rustc_env,
        tags = test_tags,
        target_compatible_with = HOST_ONLY,
        deps = all_crate_deps(normal_dev = True) + test_deps,
    )

    rust_doc_test(
        name = name + "_doc_test",
        crate = ":" + name,
        target_compatible_with = HOST_ONLY,
    )

    rust_shared_library(
        name = name + "_module",
        srcs = srcs,
        crate_name = crate_name,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        target_compatible_with = WASM32_ONLY,
        deps = all_crate_deps(normal = True) + deps + wasm_deps,
    )

    # `rust_wasm_bindgen` transitions its input to wasm32 itself, so the bundle
    # is requested from a host-configured build like any other target.
    rust_wasm_bindgen(
        name = name + "_bundle",
        out_name = artifact,
        target = "web",
        visibility = visibility,
        wasm_file = ":" + name + "_module",
    )

    surface_crate_stage(
        name = name + "_dist",
        artifact = artifact,
        bundle = name + "_bundle",
        sidecars = sidecars,
        visibility = visibility,
    )

def _help_sidecar(kind, crate_name, artifact, edition):
    """The component's `help.md`, generated from the crate that documents it.

    The text is `<crate>::help::help_markdown()`, so the served sidecar is the
    function's output rather than a copy of it: a one-line `main.rs` printing
    the string, a host binary around it, and an action capturing its stdout.
    `print!`, not `println!` — the function's bytes are the whole file, and the
    drift gate that holds the committed copy to them compares byte for byte.

    The output is named for the artifact, which is both what the asset tree
    serves it as (so the staging rule renames nothing) and what keeps it from
    colliding with the committed `help.md` in the same package, which the drift
    gate reads.

    Args:
        kind: the component's directory name, which names its targets.
        crate_name: the underscored crate name, as the generated `main.rs`
            spells it.
        artifact: the `brenn_*` basename the sidecar is served under.
        edition: Rust edition of the generator binary.
    """
    write_file(
        name = kind + "_help_main",
        out = kind + "_help_src/main.rs",
        content = [
            "fn main() {",
            "    print!(\"{}\", " + crate_name + "::help::help_markdown());",
            "}",
            "",
        ],
    )

    rust_binary(
        name = kind + "_help_gen",
        srcs = [":" + kind + "_help_main"],
        edition = edition,
        target_compatible_with = HOST_ONLY,
        deps = [":" + kind],
    )

    native.genrule(
        name = kind + "_help",
        outs = [artifact + ".help.md"],
        cmd = "set -eu; $(execpath :%s_help_gen) > $@" % kind,
        target_compatible_with = HOST_ONLY,
        tools = [":" + kind + "_help_gen"],
    )

    # The committed copy is the drift gate's fixture, so it and the generated
    # file have to be the same bytes. The component's own
    # `help_sidecar_matches_generator` test already pins the committed copy to
    # `help_markdown()`; this pins the generated file to the committed copy, so
    # a capture that mangled the bytes cannot reach the served tree quietly.
    generated_parity_test(
        name = kind + "_help_parity_test",
        committed = "help.md",
        generated = ":" + kind + "_help",
    )

# ---------------------------------------------------------------------------
# Page-hosted processor components
# ---------------------------------------------------------------------------

# The guest hub's aliases for the two third-party crates a component crate
# needs. Named here rather than resolved from a manifest because such a crate
# is built twice from two hubs: the host half — its state machine and its help
# generator — resolves from the root workspace it is a member of, and the wasm32
# half resolves from the guest workspace, which it is not a member of and cannot
# be (a cargo member outside the workspace root is not reconstructible where the
# crate hub is generated).
_GUEST_HUB_DEPS = [
    "@wasm_crates//:serde",
    "@wasm_crates//:serde_json",
]

def surface_processor_component(
        edition = "2024",
        deps = [],
        wasm_deps = [],
        test_data = [],
        test_deps = [],
        test_rustc_env = {},
        test_tags = []):
    """A surface component hosted on the processor pipeline, named by where it lives.

    The kind is the package's directory name. There is no wasm-bindgen bundle:
    the wasm32 half is a component-model artifact, transpiled and staged by
    `surface_processor_assets` in `//surface`, and this package's slice of the
    asset tree is its documentation sidecars alone.

    The crate is still built twice. The host build is what carries the state
    machine's unit tests and the help generator, so the modules that name the
    guest SDK — the component glue, and the generated module's publish handles —
    are `cfg(target_arch = "wasm32")` and absent from it.

    Args:
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: first-party deps of the host build.
        wasm_deps: first-party deps of the wasm32 build, beside the guest SDK.
        test_data: files the host unit tests read at run time, beside this
            package's own documentation — the other half of a seam this crate's
            source cannot assert, such as the stylesheets that dress what its
            glue stamps.
        test_deps: first-party deps of the host unit tests.
        test_rustc_env: extra compile-time environment for the host unit tests.
        test_tags: tags for the host unit test target.
    """
    package = native.package_name()
    parent, _, kind = package.rpartition("/")
    if parent not in _COMPONENT_PARENTS and package not in _COMPONENT_PACKAGES:
        fail(("surface_processor_component in %r: a component lives under one of %r, or is " +
              "one of %r named exactly; the kind is its directory name") %
             (package, _COMPONENT_PARENTS, _COMPONENT_PACKAGES))

    crate_name = "brenn_" + kind.replace("-", "_")

    test_rustc_env = dict(test_rustc_env)
    test_rustc_env["CARGO_MANIFEST_DIR"] = package

    schema = native.glob(["schema.json"], allow_empty = True)
    committed_help = native.glob(["help.md"])

    guest_spec_scaffold(
        name = kind + "_spec",
        spec = "//:config/specs/" + kind + ".brenn",
    )

    generated_srcs = {"src/spec.rs": ":" + kind + "_spec"}
    srcs = replace_generated(native.glob(["src/**/*.rs"]), generated_srcs)

    rust_library(
        name = kind,
        srcs = srcs,
        crate_name = crate_name,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        target_compatible_with = HOST_ONLY,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + deps,
    )

    rust_test(
        name = kind + "_test",
        crate = ":" + kind,
        data = committed_help + schema + test_data,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro_dev = True),
        rustc_env = test_rustc_env,
        tags = test_tags,
        target_compatible_with = HOST_ONLY,
        deps = all_crate_deps(normal_dev = True) + test_deps,
    )

    rust_doc_test(
        name = kind + "_doc_test",
        crate = ":" + kind,
        target_compatible_with = HOST_ONLY,
    )

    rust_shared_library(
        name = kind + "_module",
        srcs = srcs,
        crate_name = crate_name,
        edition = edition,
        target_compatible_with = WASM32_ONLY,
        deps = [
            "//brenn-wasm/components/guest:brenn-guest",
        ] + _GUEST_HUB_DEPS + wasm_deps,
    )

    wasm_component(
        name = kind + "_component",
        module = ":" + kind + "_module",
        visibility = ["//visibility:public"],
    )

    _help_sidecar(
        artifact = crate_name,
        crate_name = crate_name,
        edition = edition,
        kind = kind,
    )

    # This package's slice of the asset tree. A page-hosted kind's artifact,
    # record and packaged specification all live under `processor/<kind>/`,
    # staged in `//surface`; what ships flat beside the other components'
    # bundles is documentation, which is served by kind-derived name whatever
    # hosts the kind.
    copy_to_directory(
        name = kind + "_dist",
        srcs = [":" + kind + "_help"] + schema,
        replace_prefixes = {
            "schema.json": crate_name + ".schema.json",
        },
        root_paths = [package],
        target_compatible_with = HOST_ONLY,
        visibility = ["//visibility:public"],
    )
