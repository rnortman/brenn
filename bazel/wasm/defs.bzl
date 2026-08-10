"""Build rules for the wasm32 guest tree and the WIT component pipeline.

Every crate under `brenn-wasm/components/` builds for wasm32-unknown-unknown and
for nothing else. The two crate macros here carry that fact — the target triple,
the guest crate hub, the globbed sources — so a crate's BUILD.bazel states only
what is specific to it.

Above them sits the component pipeline: `wit_bindgen_rust` generates a raw
crate's bindings, `wasm_component` transitions a core module into the wasm32
configuration and wraps it with `wasm-tools component new`, and
`wasi_import_test` asserts the result imports nothing from `wasi:`.
"""

load("@bazel_skylib//lib:shell.bzl", "shell")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_shared_library")
load("@wasm_crates//:defs.bzl", "all_crate_deps")
load("//bazel/platforms:defs.bzl", "HOST_ONLY", "WASM32_ONLY")

def wasm_guest_library(name, edition = "2024", deps = [], compile_data = [], visibility = None):
    """An rlib in the guest workspace, built for wasm32-unknown-unknown.

    Args:
        name: target name; also the crate name.
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: first-party dependencies. Third-party ones come from the hub.
        compile_data: files read at macro-expansion time (WIT worlds).
        visibility: target visibility.
    """
    rust_library(
        name = name,
        srcs = native.glob(["src/**/*.rs"]),
        compile_data = compile_data,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        target_compatible_with = WASM32_ONLY,
        visibility = visibility,
        deps = all_crate_deps(normal = True) + deps,
    )

def wasm_guest_cdylib(
        name,
        edition = "2024",
        deps = [],
        compile_data = [],
        bindings = None,
        visibility = None):
    """A component crate's core WASM module, built for wasm32-unknown-unknown.

    The output is a plain WASM module, not yet a WIT component: `wasm_component`
    runs a layer up.

    Args:
        name: target name; also the crate name.
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: first-party dependencies. Third-party ones come from the hub.
        compile_data: files read at macro-expansion time (WIT worlds).
        bindings: for a raw-WIT crate, the `wit_bindgen_rust` target supplying
            `src/bindings.rs`. The committed copy of that file is dropped from
            the source glob so the generated one is what compiles.
        visibility: target visibility.
    """
    srcs = native.glob(["src/**/*.rs"])
    if bindings:
        srcs = [s for s in srcs if s != "src/bindings.rs"] + [bindings]

    rust_shared_library(
        name = name,
        srcs = srcs,
        compile_data = compile_data,
        edition = edition,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        target_compatible_with = WASM32_ONLY,
        visibility = visibility,
        deps = all_crate_deps(normal = True) + deps,
    )

# ---------------------------------------------------------------------------
# wit-bindgen
# ---------------------------------------------------------------------------

def _wit_bindgen_rust_impl(ctx):
    out = ctx.actions.declare_file("src/bindings.rs")
    world = ctx.file.wit.basename[:-len(".wit")]

    # wit-bindgen names its output after the world, not after a flag, so the
    # generated file is moved into place. A private scratch dir keeps the
    # intermediate out of the declared output tree.
    ctx.actions.run_shell(
        outputs = [out],
        # The generated-from file is declared alongside the closure it may
        # reach: the two attributes are independent, and an action that read
        # `wit` without declaring it would stop rebuilding when it changed.
        inputs = depset([ctx.file.wit], transitive = [depset(ctx.files.wit_srcs)]),
        tools = [ctx.file._wit_bindgen],
        command = """
set -eu
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
"$1" rust "$2" --runtime-path wit_bindgen_rt --out-dir "$scratch"
mv "$scratch/$3.rs" "$4"
""",
        arguments = [
            ctx.file._wit_bindgen.path,
            ctx.file.wit.path,
            world,
            out.path,
        ],
        mnemonic = "WitBindgenRust",
        progress_message = "Generating WIT bindings for %s" % ctx.label,
    )
    return [DefaultInfo(files = depset([out]))]

wit_bindgen_rust = rule(
    implementation = _wit_bindgen_rust_impl,
    doc = """Generate a raw-WIT crate's `src/bindings.rs` from its WIT world.

    The bare wit-bindgen CLI is invoked with no SDK in the graph, so a crate
    that compiles against this output has proven the WIT is directly
    consumable.
    """,
    attrs = {
        "wit": attr.label(
            allow_single_file = [".wit"],
            mandatory = True,
            doc = "The WIT file naming the world to generate.",
        ),
        "wit_srcs": attr.label(
            allow_files = True,
            mandatory = True,
            doc = "Every WIT file the world may reach, as action inputs.",
        ),
        "_wit_bindgen": attr.label(
            allow_single_file = True,
            cfg = "exec",
            default = Label("//bazel/tools:wit-bindgen"),
        ),
    },
)

# ---------------------------------------------------------------------------
# wasm-tools component new
# ---------------------------------------------------------------------------

def _wasm32_transition_impl(_settings, _attr):
    # Components are always optimized: the make lane builds them `--release`,
    # and an artifact that differs between a dev and a release build would make
    # the host tests that load it configuration-dependent.
    return {
        "//command_line_option:compilation_mode": "opt",
        "//command_line_option:platforms": str(Label("//bazel/platforms:wasm32")),
    }

_wasm32_transition = transition(
    implementation = _wasm32_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:compilation_mode",
        "//command_line_option:platforms",
    ],
)

def _module_file(target):
    """The single `.wasm` core module a cdylib target produces."""
    modules = [f for f in target[DefaultInfo].files.to_list() if f.extension == "wasm"]
    if len(modules) != 1:
        fail("expected exactly one .wasm output from %s, got %s" % (target.label, modules))
    return modules[0]

def _wasm_component_impl(ctx):
    # An attribute carrying a transition arrives as a list, one entry per
    # resulting configuration; this transition produces exactly one.
    module = _module_file(ctx.attr.module[0])

    # The artifact keeps the core module's basename — the frozen dir → crate →
    # artifact convention the deploy manifest and the host tests both name.
    out = ctx.actions.declare_file("%s/%s" % (ctx.label.name, module.basename))
    ctx.actions.run(
        outputs = [out],
        inputs = [module],
        executable = ctx.file._wasm_tools,
        arguments = ["component", "new", module.path, "-o", out.path],
        mnemonic = "WasmComponentNew",
        progress_message = "Wrapping %s as a WIT component" % module.basename,
    )
    return [DefaultInfo(files = depset([out]))]

_wasm_component = rule(
    implementation = _wasm_component_impl,
    doc = """Wrap a core WASM module into a WIT component.

    The module is built under the wasm32 platform by an incoming transition, so
    a host-configured `bazel build //...` reaches the guest tree through this
    rule instead of skipping it.
    """,
    attrs = {
        "module": attr.label(
            cfg = _wasm32_transition,
            mandatory = True,
            doc = "The `wasm_guest_cdylib` target producing the core module.",
        ),
        "_wasm_tools": attr.label(
            allow_single_file = True,
            cfg = "exec",
            default = Label("//bazel/tools:wasm-tools"),
        ),
    },
)

def wasm_component(name, module, visibility = None):
    """A WIT component plus the WASI-import gate that every component must pass.

    Pairing them here is what makes the gate structural: a component cannot be
    added to the tree without acquiring its test.

    Args:
        name: target name; the artifact keeps the module's own basename.
        module: the `wasm_guest_cdylib` target producing the core module.
        visibility: visibility of the component target.
    """
    _wasm_component(
        name = name,
        module = module,
        visibility = visibility,
    )
    _wasi_import_test(
        name = name + "_wasi_imports_test",
        component = name,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )

# ---------------------------------------------------------------------------
# Host-side fixtures
# ---------------------------------------------------------------------------

def _component_fixtures_impl(ctx):
    # The host suites address artifacts as `<manifest dir>/../brenn-wasm/
    # target/components/<basename>.wasm`, a path baked into `env!` call sites in
    # both crates. Reproducing that layout inside the runfiles tree is what lets
    # the same source read from the cargo target dir under make and from
    # runfiles under Bazel.
    outs = []
    for target in ctx.attr.components:
        component = _module_file(target)
        out = ctx.actions.declare_file("target/components/" + component.basename)
        ctx.actions.symlink(output = out, target_file = component)
        outs.append(out)
    return [DefaultInfo(files = depset(outs), runfiles = ctx.runfiles(files = outs))]

component_fixtures = rule(
    implementation = _component_fixtures_impl,
    doc = """Stage every component under `brenn-wasm/target/components/`.

    Declared in `//brenn-wasm`, so the runfiles paths match the cargo target
    dir the host tests name.
    """,
    attrs = {
        "components": attr.label_list(
            mandatory = True,
            doc = "Every `wasm_component` target the host suites load.",
        ),
    },
)

# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

# The gate logic lives in a checked-in script so it can be exercised directly by
# `//bazel/wasm/tests`; the generated file is only the argument binding.
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

def _wasi_import_test_impl(ctx):
    component = ctx.file.component
    check = ctx.file._check
    script = _check_wrapper(
        ctx,
        check,
        [ctx.file._wasm_tools.short_path, component.short_path],
    )
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = [component, ctx.file._wasm_tools, check]),
    )]

_wasi_import_test = rule(
    implementation = _wasi_import_test_impl,
    doc = """Assert a component's WIT world imports nothing from `wasi:`.

    The host's wasmtime linker provides no WASI, so a component that acquires a
    WASI import fails to instantiate at runtime rather than at build time.
    """,
    attrs = {
        "component": attr.label(
            allow_single_file = [".wasm"],
            mandatory = True,
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:wasi_import_check.sh"),
        ),
        "_wasm_tools": attr.label(
            allow_single_file = True,
            cfg = "exec",
            default = Label("//bazel/tools:wasm-tools"),
        ),
    },
    test = True,
)

def _deployed_components_test_impl(ctx):
    components = [_module_file(t) for t in ctx.attr.components]
    check = ctx.file._check
    names = ctx.file._names
    script = _check_wrapper(
        ctx,
        check,
        [
            names.short_path,
            " ".join(sorted([c.basename for c in components])),
            ctx.file.manifest.short_path,
            str(ctx.attr.manifest.label),
        ],
    )
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = [ctx.file.manifest, check, names]),
    )]

_deployed_components_test = rule(
    implementation = _deployed_components_test_impl,
    doc = """Assert every artifact the deploy manifest names is actually built.

    The manifest is what the packaging step ships; a name in it that no target
    produces ships nothing, and the failure would first appear on the deploy
    target.
    """,
    attrs = {
        "components": attr.label_list(
            mandatory = True,
            doc = "Every `wasm_component` target in the tree, deployed or not.",
        ),
        "manifest": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:deployed_components_check.sh"),
        ),
        "_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:manifest_names.sh"),
        ),
    },
    test = True,
)

def deployed_components_test(name, manifest, components):
    """See `_deployed_components_test`.

    Args:
        name: target name.
        manifest: the deploy manifest listing artifact basenames.
        components: every `wasm_component` target in the tree.
    """
    _deployed_components_test(
        name = name,
        components = components,
        manifest = manifest,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )
