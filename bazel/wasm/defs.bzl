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
    # Components are always optimized: the guests are built `--release`,
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
# Component packages
# ---------------------------------------------------------------------------

# A package's file grammar, on the Starlark side. A package is a directory named
# for the package, holding the record under a fixed basename, the artifact under
# its built basename, and — for a processor world — the packaged specification
# under `<name>.brenn`. The record states the artifact's basename, so nothing
# outside this file has to derive it.
_RECORD_NAME = "package.json"

ComponentPackageInfo = provider(
    doc = "One built component package: the name it resolves under, and its files.",
    fields = {
        "files": "depset of the files in the package directory",
        "package": "the package name, which is the directory's basename",
    },
)

def _component_package_impl(ctx):
    artifact = _module_file(ctx.attr.component)
    package = ctx.attr.package

    # Package-relative outputs under the target's own name: two targets
    # packaging one component would otherwise declare the same files.
    prefix = "%s/%s" % (ctx.label.name, package)
    record = ctx.actions.declare_file("%s/%s" % (prefix, _RECORD_NAME))

    # The artifact is staged into the directory rather than left in its own
    # output tree: the package is what a host resolves, so every file the record
    # names has to be reachable from the directory alone.
    staged = ctx.actions.declare_file("%s/%s" % (prefix, artifact.basename))
    ctx.actions.symlink(output = staged, target_file = artifact)

    outputs = [record]
    args = [package, ctx.attr.world, artifact.path, record.path]
    inputs = [artifact]
    if ctx.file.spec:
        packaged_spec = ctx.actions.declare_file("%s/%s.brenn" % (prefix, package))
        outputs.append(packaged_spec)
        inputs.append(ctx.file.spec)
        args += [ctx.file.spec.path, packaged_spec.path]

    ctx.actions.run(
        outputs = outputs,
        inputs = inputs + [ctx.file._wit_lib],
        executable = ctx.file._emit,
        tools = [ctx.file._wasm_tools],
        env = {
            "WASM_TOOLS": ctx.file._wasm_tools.path,
            "WIT_LIB": ctx.file._wit_lib.path,
        },
        arguments = args,
        mnemonic = "ComponentPackage",
        progress_message = "Packaging %s" % package,
    )
    files = depset(outputs + [staged])
    return [
        DefaultInfo(files = files),
        ComponentPackageInfo(package = package, files = files),
    ]

_component_package = rule(
    implementation = _component_package_impl,
    doc = """A shipped component's package directory: artifact, record, spec.

    The directory's basename is the name a configuration resolves the component
    by, and the record repeats it, so a directory renamed in transit is a
    refusal at boot rather than a component loaded under the wrong name.
    """,
    attrs = {
        "component": attr.label(
            mandatory = True,
            doc = "The `wasm_component` target producing the artifact.",
        ),
        "package": attr.string(
            mandatory = True,
            doc = "The package name; also the directory's basename.",
        ),
        "spec": attr.label(
            allow_single_file = [".brenn"],
            doc = "The authored specification, for a `brenn:processor` component.",
        ),
        "world": attr.string(
            mandatory = True,
            values = ["brenn:processor", "brenn:replay"],
            doc = "The WIT package the artifact targets.",
        ),
        "_emit": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:emit_package.sh"),
        ),
        "_wasm_tools": attr.label(
            allow_single_file = True,
            cfg = "exec",
            default = Label("//bazel/tools:wasm-tools"),
        ),
        "_wit_lib": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:wit_lib.sh"),
        ),
    },
)

def component_package(name, package, component, world, spec = None, visibility = None):
    """See `_component_package`.

    Args:
        name: target name; also the directory its outputs are declared under.
        package: the package name a configuration resolves the component by.
        component: the `wasm_component` target producing the artifact.
        world: `brenn:processor` or `brenn:replay`.
        spec: the authored specification; required iff the world is
            `brenn:processor`, and refused otherwise. Its authored basename must
            be `<package>.brenn`, which the emitter asserts.
        visibility: target visibility.
    """
    _component_package(
        name = name,
        component = component,
        package = package,
        spec = spec,
        world = world,
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

# ---------------------------------------------------------------------------
# The installed layout, for a locally run server
# ---------------------------------------------------------------------------

def _component_install_tree_impl(ctx):
    outs = []
    seen = {}
    for target in ctx.attr.packages:
        info = target[ComponentPackageInfo]
        if info.package in seen:
            fail("two targets package the name %s; a components root holds one directory per package" % info.package)
        seen[info.package] = True
        for src in info.files.to_list():
            out = ctx.actions.declare_file("%s/%s/%s" % (ctx.label.name, info.package, src.basename))
            ctx.actions.symlink(output = out, target_file = src)
            outs.append(out)
    return [DefaultInfo(files = depset(outs), runfiles = ctx.runfiles(files = outs))]

_component_install_tree = rule(
    implementation = _component_install_tree_impl,
    doc = """A components root: one package directory per shipped component.

    The build declares each package under its own target's output directory, and
    a host resolves `<root>/<name>/` from the name a configuration states. This
    is that root, so a server started from the workspace resolves a component
    the way a deployment does — package verification included.
    """,
    attrs = {
        "packages": attr.label_list(
            mandatory = True,
            providers = [ComponentPackageInfo],
            doc = "The `component_package` targets to stage.",
        ),
    },
)

def component_install_tree(name, packages, visibility = None):
    """See `_component_install_tree`.

    Args:
        name: target name; also the directory its outputs are declared under.
        packages: the `component_package` targets to stage.
        visibility: target visibility.
    """
    _component_install_tree(
        name = name,
        packages = packages,
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

# ---------------------------------------------------------------------------
# Host-side fixtures
# ---------------------------------------------------------------------------

def _component_fixtures_impl(ctx):
    # The host suites address artifacts as `<manifest dir>/../brenn-wasm/
    # target/components/<basename>.wasm`, a path baked into `env!` call sites in
    # both crates. Reproducing that layout inside the runfiles tree is what lets
    # the same source read from the guest crate's own output tree and from
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

    Declared in `//brenn-wasm`, so the runfiles paths match the workspace-relative
    directory the host tests name.
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
    packaged = [t[ComponentPackageInfo].package for t in ctx.attr.packages]
    check = ctx.file._check
    names = ctx.file._names
    script = _check_wrapper(
        ctx,
        check,
        [
            names.short_path,
            " ".join(sorted(packaged)),
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
    doc = """Hold the deploy manifest and the packaged set equal, both directions.

    The manifest names packages, and a package is what a host resolves a
    component by, so a name in it that no `component_package` target produces
    ships nothing and the failure would first appear on the deploy target. The
    other direction is the release's module root: a package the manifest omits
    stages its authored module there with no component installed beside it,
    which the release contract test refuses far from the manifest that caused
    it.
    """,
    attrs = {
        "manifest": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "packages": attr.label_list(
            mandatory = True,
            providers = [ComponentPackageInfo],
            doc = "Every `component_package` target in the tree.",
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

def deployed_components_test(name, manifest, packages):
    """See `_deployed_components_test`.

    Args:
        name: target name.
        manifest: the deploy manifest listing the package names that ship.
        packages: every `component_package` target in the tree.
    """
    _deployed_components_test(
        name = name,
        manifest = manifest,
        packages = packages,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )
