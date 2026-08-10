"""Build rules for the surface browser tree.

Every crate under `surface/` that the browser loads is built twice: once as an
ordinary host `rust_library` — the DOM-free logic and the help generator, which
is what the host suites exercise — and once as a wasm32 cdylib that
`rust_wasm_bindgen` turns into the `--target web` ES-module bundle the page
imports.

The dir → crate → artifact mapping is frozen: a component in `surface/<kind>`
is crate `brenn-<kind>` and ships as `brenn_<kind with - -> _>`. Every consumer
addresses components by that artifact name, so `surface_component` derives all
three from the package path instead of taking them as attributes.
"""

load("@crates//:defs.bzl", "all_crate_deps")
load("@rules_rust//rust:defs.bzl", "rust_doc_test", "rust_library", "rust_shared_library", "rust_test")
load("@rules_rust_wasm_bindgen//:defs.bzl", "rust_wasm_bindgen")
load("//bazel/platforms:defs.bzl", "HOST_ONLY", "WASM32_ONLY")
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
        sidecars: documentation files shipped beside the bundle, renamed to the
            artifact's basename in the asset tree.
        visibility: visibility of the host library and the bundle.
    """
    srcs = native.glob(["src/**/*.rs"])

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

def surface_component(
        edition = "2024",
        deps = [],
        wasm_deps = [],
        test_deps = [],
        test_rustc_env = {},
        test_tags = []):
    """A surface component crate, named entirely by where it lives.

    The kind is the package's directory name; the crate is `brenn-<kind>` and
    the artifact `brenn_<kind>` with hyphens underscored. Passing any of the
    three is not offered: a component whose dir, crate and artifact disagree is
    unloadable, and the convention is what the server, the bindings document and
    the asset handler all address it by.

    Args:
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: first-party deps both builds take.
        wasm_deps: first-party deps only the browser build takes.
        test_deps: first-party deps of the host unit tests.
        test_rustc_env: extra compile-time environment for the host unit tests.
        test_tags: tags for the host unit test target.
    """
    package = native.package_name()
    parent, _, kind = package.rpartition("/")
    if parent not in _COMPONENT_PARENTS and package not in _COMPONENT_PACKAGES:
        fail(("surface_component in %r: a component lives under one of %r, or is one of " +
              "%r named exactly; the kind is its directory name") %
             (package, _COMPONENT_PARENTS, _COMPONENT_PACKAGES))

    # The crate name is also the artifact basename: underscored once, addressed
    # under that spelling by the server, the bindings document and the page.
    crate_name = "brenn_" + kind.replace("-", "_")

    # The help sidecar is read by the crate's own drift-gate test through
    # `env!("CARGO_MANIFEST_DIR")`, which rules_rust otherwise bakes in as an
    # absolute execroot path no runfile lives under. The workspace-relative form
    # resolves from the runfiles root, where a Bazel test starts.
    test_rustc_env = dict(test_rustc_env)
    test_rustc_env["CARGO_MANIFEST_DIR"] = package

    # Every component documents itself, so `help.md` is not optional; a schema
    # is, and a component that ships none contributes no schema sidecar.
    sidecars = native.glob(["help.md"]) + native.glob(["schema.json"], allow_empty = True)

    surface_wasm_crate(
        name = kind,
        artifact = crate_name,
        crate_name = crate_name,
        deps = deps,
        edition = edition,
        sidecars = sidecars,
        test_data = sidecars,
        test_deps = test_deps,
        test_rustc_env = test_rustc_env,
        test_tags = test_tags,
        wasm_deps = wasm_deps,
    )
