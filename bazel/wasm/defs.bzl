"""Build rules for the wasm32 guest tree and the WIT component pipeline.

Every crate under `brenn-wasm/components/` builds for wasm32-unknown-unknown and
for nothing else. The two crate macros here carry that fact — the target triple,
the platform constraint, the globbed sources — so a crate's BUILD.bazel states
only what is specific to it: its dependencies, from whichever crate hub the
calling repository owns, and its generated modules.

Above them sits the component pipeline: `wit_bindgen_rust` generates a raw
crate's bindings, `guest_spec_scaffold` generates a specification-bearing
crate's port module, `wasm_component` transitions a core module into the wasm32
configuration and wraps it with `wasm-tools component new`, and
`wasi_import_test` asserts the result imports nothing from `wasi:`.

## External authoring contract

This file is loaded across module boundaries: a repository that declares
`bazel_dep(name = "brenn")` builds its components with these macros, so they
are an external API with the same standing as the WIT worlds under
`brenn-wasm/wit/`. The contract is exactly these:

  `wasm_guest_library`, `wasm_guest_cdylib`, `wit_bindgen_rust`,
  `shared_guest_bindings`, `guest_spec_scaffold`, `wasm_component`,
  `component_package`, `component_install_tree`, `component_bundle`,
  `grant_parity_test`, `deployed_components_test`, `config_fit_test`.

Three are not: `component_fixtures`, whose output path is only correct when
declared from `//brenn-wasm`; `wasm32_build`, a reacher for wasm32-only
libraries inside this tree's own `bazel build //...`; and `wit_root`, which
stages a WIT package for a host crate's `bindgen!` and is this tree's own
plumbing — a consumer depends on such a crate, it never declares one. An
out-of-tree caller of any of the three is on its own.

Evolution policy: these macros are a **build-time** contract, and build-time
contracts are hard-cut. A consumer loads them from the commit it names in its
own `git_override`, so an attribute or output that moves costs that consumer a
migration when it next bumps the pin, and nothing before — nothing already
deployed reads a macro. `examples/component/` is the canary that a cut is
complete on this side, and `brenn-component-demo` is where it is paid.

That is not the rule the *runtime* contracts are under: what a running host
reads out of a bundle built earlier — the WIT worlds, the two records, the
served surface layout, the packaged-module subset of the DSL — moves only
additively, because the host and the bundle are installed by different
releases. Both rules are stated in one place, `docs/component-packages.md`,
*Contract evolution*.

Label anchoring: a string that names a target inside a macro body resolves
against the *caller's* package and repository mapping, so from another module
it names the wrong thing or nothing. Every implicit label in this file and in
the `.bzl` files it loads is `Label(...)`-wrapped. A bare label string in a
macro body is a bug unless it names one of the macro's own arguments.
"""

load("@bazel_skylib//lib:shell.bzl", "shell")
load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_shared_library")
load("//bazel/platforms:defs.bzl", "HOST_ONLY", "WASM32_ONLY")

def wasm_guest_library(
        name,
        edition = "2024",
        deps = [],
        proc_macro_deps = [],
        compile_data = [],
        visibility = None):
    """An rlib in a guest workspace, built for wasm32-unknown-unknown.

    Dependencies are taken verbatim. A crate's third-party deps come from its
    own repository's hub — `all_crate_deps(normal = True)` from
    `@wasm_crates//:defs.bzl` in this tree — and the macro names no hub itself,
    so it is callable from a module whose hub is not this one.

    Args:
        name: target name; also the crate name.
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: every non-proc-macro dependency, first- and third-party.
        proc_macro_deps: every proc-macro dependency.
        compile_data: files read at macro-expansion time (WIT worlds).
        visibility: target visibility.
    """
    rust_library(
        name = name,
        srcs = native.glob(["src/**/*.rs"]),
        compile_data = compile_data,
        edition = edition,
        proc_macro_deps = proc_macro_deps,
        target_compatible_with = WASM32_ONLY,
        visibility = visibility,
        deps = deps,
    )

# Where a crate's `mod bindings;` resolves.
BINDINGS_PATH = "src/bindings.rs"

def replace_generated(srcs, generated):
    """Swap generated modules into a crate's globbed source list.

    One statement of the swap for every generator: a module generated into the
    crate takes the path it occupies there, and the committed file at that path
    — where one exists at all — is dropped so the generated one is what
    compiles. A new generator is a new entry at a call site, not a new keyword
    argument here.

    Args:
        srcs: the globbed source list.
        generated: `{"src/<module>.rs": <label>}` — the path each generated
            module occupies in the crate, and the target that produces it.

    Returns:
        The source list with each generated module in its crate path's place.
    """
    return [s for s in srcs if s not in generated] + generated.values()

def wasm_guest_cdylib(
        name,
        edition = "2024",
        deps = [],
        proc_macro_deps = [],
        compile_data = [],
        generated_srcs = {},
        shared_bindings = None,
        visibility = None):
    """A component crate's core WASM module, built for wasm32-unknown-unknown.

    The output is a plain WASM module, not yet a WIT component: `wasm_component`
    runs a layer up. Dependencies are taken verbatim, as `wasm_guest_library`
    takes them.

    Args:
        name: target name; also the crate name.
        edition: Rust edition; must equal the crate's Cargo.toml edition.
        deps: every non-proc-macro dependency, first- and third-party.
        proc_macro_deps: every proc-macro dependency.
        compile_data: files read at macro-expansion time (WIT worlds).
        generated_srcs: modules generated into this crate, keyed by the path
            each occupies — `{"src/spec.rs": ":spec"}` for a
            specification-bearing one. See `replace_generated`.
        shared_bindings: a `wit_bindgen_rust` target generating a world several
            crates share. Naming it places the generation at this crate's
            bindings path and substitutes it, so `BINDINGS_PATH` is stated here
            and not in each crate. Naming it alongside a `generated_srcs` entry
            for that same path is an error.
        visibility: target visibility.
    """
    if shared_bindings:
        if BINDINGS_PATH in generated_srcs:
            fail(("%s: shared_bindings names %s and generated_srcs also has an entry " +
                  "for %s. Two statements of where this crate's bindings come from; " +
                  "state one.") % (name, shared_bindings, BINDINGS_PATH))
        bindings = name + "_bindings"
        shared_guest_bindings(name = bindings, shared = shared_bindings)
        generated_srcs = dict(generated_srcs)
        generated_srcs[BINDINGS_PATH] = ":" + bindings

    srcs = replace_generated(native.glob(["src/**/*.rs"]), generated_srcs)

    rust_shared_library(
        name = name,
        srcs = srcs,
        compile_data = compile_data,
        edition = edition,
        proc_macro_deps = proc_macro_deps,
        target_compatible_with = WASM32_ONLY,
        visibility = visibility,
        deps = deps,
    )

# ---------------------------------------------------------------------------
# wit-bindgen
# ---------------------------------------------------------------------------

def _wit_bindgen_rust_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.out)
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
    doc = """Generate a WIT world's Rust bindings.

    The bare wit-bindgen CLI is invoked with no SDK in the graph, so a crate
    that compiles against this output has proven the WIT is directly
    consumable.

    Declared in the crate that consumes it, the default `out` puts the module
    at the path the crate's `mod bindings;` resolves. A world several crates
    generate identically is instead declared once, under a path of its own, and
    reaches each crate through `wasm_guest_cdylib`'s `shared_bindings`.
    """,
    attrs = {
        "out": attr.string(
            default = BINDINGS_PATH,
            doc = "Package-relative path of the generated module.",
        ),
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

def shared_guest_bindings(name, shared):
    """Place a shared bindings generation at this crate's bindings path.

    rules_rust resolves `mod bindings;` by path adjacency to the crate root, so
    a generation declared in another package cannot be compiled where it lies —
    it has to exist under this package's path.

    The primitive underneath `wasm_guest_cdylib`'s `shared_bindings`, which is
    what a crate states; calling it directly means also wiring the
    substitution by hand.

    Args:
        name: target name.
        shared: the `wit_bindgen_rust` target generating the world.
    """
    copy_file(
        name = name,
        src = shared,
        out = BINDINGS_PATH,
        allow_symlink = True,
    )

# ---------------------------------------------------------------------------
# spec scaffolding
# ---------------------------------------------------------------------------

def _guest_spec_scaffold_impl(ctx):
    out = ctx.actions.declare_file("src/spec.rs")

    args = ctx.actions.args()
    args.add(ctx.executable._dsl_cli)
    args.add(ctx.executable._rustfmt)
    args.add(out)
    args.add(ctx.file.spec)
    args.add(ctx.attr.class_name)

    # The emitter writes valid Rust and rustfmt decides how it is laid out.
    # Predicting the layout in the emitter instead would put a copy of rustfmt's
    # heuristics there, one construct at a time, and every shape no fixture
    # covers would emit code the toolchain reformats.
    ctx.actions.run_shell(
        outputs = [out],
        inputs = [ctx.file.spec],
        tools = [
            ctx.attr._dsl_cli[DefaultInfo].files_to_run,
            ctx.attr._rustfmt[DefaultInfo].files_to_run,
        ],
        command = """
set -eu
if [ -n "$5" ]; then
    "$1" scaffold --class "$5" -o "$3" "$4"
else
    "$1" scaffold -o "$3" "$4"
fi
"$2" --edition 2024 "$3"
""",
        arguments = [args],
        mnemonic = "GuestSpecScaffold",
        progress_message = "Scaffolding %s from %s" % (ctx.label, ctx.file.spec.basename),
    )
    return [DefaultInfo(files = depset([out]))]

guest_spec_scaffold = rule(
    implementation = _guest_spec_scaffold_impl,
    doc = """Generate a component crate's `src/spec.rs` from its specification.

    The output is untracked: the specification label is an action input, so
    editing the specification rebuilds the module and every guest that has not
    kept up stops compiling. That is the drift gate a committed copy would need
    a parity test for.

    The label taken here is the same one the component's package embeds and the
    host hash-binds at boot, so the bytes that generate this code are the bytes
    the running system checks.
    """,
    attrs = {
        "spec": attr.label(
            allow_single_file = [".brenn"],
            mandatory = True,
            doc = "The authored specification to generate from.",
        ),
        "class_name": attr.string(
            doc = "Which component class to generate from, where the document declares more than one.",
        ),
        "_dsl_cli": attr.label(
            cfg = "exec",
            default = Label("//brenn-dsl:dsl_cli"),
            executable = True,
        ),
        "_rustfmt": attr.label(
            cfg = "exec",
            default = Label("@rules_rust//tools/upstream_wrapper:rustfmt"),
            executable = True,
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

def _wasm32_build_impl(ctx):
    return [DefaultInfo(files = depset(transitive = [
        target[DefaultInfo].files
        for target in ctx.attr.target
    ]))]

wasm32_build = rule(
    implementation = _wasm32_build_impl,
    doc = """Build a wasm32-only target from a host-configured command line.

    A crate marked `target_compatible_with = WASM32_ONLY` is skipped, not built,
    by `bazel build //...` on a host platform — every guest crate in the tree is
    reached instead through a component's incoming transition. A wasm32 library
    that no component depends on has no such reacher, so it would sit in the
    tree compiled by nothing. This is that reacher: it transitions its target
    into the wasm32 configuration and forwards what the target produced.
    """,
    attrs = {
        "target": attr.label(
            cfg = _wasm32_transition,
            mandatory = True,
            doc = "The wasm32-only target to build.",
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
# The release of a component repository
# ---------------------------------------------------------------------------

def _sole_stage_directory(target):
    """The single directory a surface staging target produces."""
    files = target[DefaultInfo].files.to_list()
    if len(files) != 1 or not files[0].is_directory:
        fail("%s must produce exactly one directory, got %s" % (target.label, files))
    return files[0]

def _component_bundle_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)

    args = ctx.actions.args()
    args.add("--out", out.path)
    args.add("--names", ctx.file._names)
    args.add("--stage-lib", ctx.file._stage_lib)
    inputs = []

    seen = {}
    for target in ctx.attr.packages:
        info = target[ComponentPackageInfo]
        if info.package in seen:
            fail("two targets package the name %s; a components root holds one directory per package" % info.package)
        seen[info.package] = True
        files = info.files.to_list()
        args.add_all(files, before_each = "--package")
        inputs.extend(files)

    if ctx.attr.packages:
        args.add("--manifest", ctx.file.manifest)
        inputs.append(ctx.file.manifest)

    stages = [_sole_stage_directory(t) for t in ctx.attr.surface_kinds]

    # Without this each stage would arrive as its expanded file list, and the
    # assembly would be handed files where it expects directories.
    args.add_all(stages, before_each = "--surface-stage", expand_directories = False)
    inputs.extend(stages)

    specs = ctx.files.spec_root
    args.add_all(specs, before_each = "--spec")
    inputs.extend(specs)

    ctx.actions.run(
        outputs = [out],
        inputs = depset(inputs),
        executable = ctx.file._assemble,
        tools = [ctx.file._names, ctx.file._stage_lib],
        arguments = [args],
        mnemonic = "ComponentBundle",
        progress_message = "Staging the component bundle at %s" % out.short_path,
    )
    return [DefaultInfo(
        files = depset([out]),
        runfiles = ctx.runfiles(files = [out]),
    )]

_component_bundle = rule(
    implementation = _component_bundle_impl,
    doc = """The unpacked shape of a component repository's release tarball.

    Up to three of the subdirectories brenn's own tarball carries, and each
    installs into a root of its own beside brenn's: `components/` with the
    manifest naming its packages, `surface/` with one transpiled tree per
    page-hosted kind, and `modules/` with the authored module of every one of
    them. `VERSION` and the install script are deliberately absent, as they are
    from brenn's: both are the deploying repo's, added beside its `tar`.
    """,
    attrs = {
        "manifest": attr.label(
            allow_single_file = True,
            doc = "The manifest naming the backend packages that ship.",
        ),
        "packages": attr.label_list(
            providers = [ComponentPackageInfo],
            doc = "The `component_package` targets to stage under `components/`.",
        ),
        "spec_root": attr.label_list(
            allow_files = [".brenn"],
            doc = "The authored module root, held set-equal to the staged `modules/`.",
        ),
        "surface_kinds": attr.label_list(
            doc = "The `surface_processor_assets` targets to stage under `surface/`.",
        ),
        "_assemble": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:bundle_assemble.sh"),
        ),
        "_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:manifest_names.sh"),
        ),
        "_stage_lib": attr.label(
            allow_single_file = True,
            default = Label("//bazel/release:stage_lib.sh"),
        ),
    },
)

def _bundle_contract_test_impl(ctx):
    tree = _sole_stage_directory(ctx.attr.bundle)
    check = ctx.file._check
    names = ctx.file._names
    record_lib = ctx.file._record_lib

    args = [names.short_path, record_lib.short_path, tree.short_path]
    files = [tree, check, names, record_lib]
    if ctx.file.manifest:
        args += ["--manifest", ctx.file.manifest.short_path]
        files.append(ctx.file.manifest)

    return [DefaultInfo(
        executable = _check_wrapper(ctx, check, args),
        runfiles = ctx.runfiles(files = files),
    )]

_bundle_contract_test = rule(
    implementation = _bundle_contract_test_impl,
    doc = """Hold a staged bundle to the records inside it.

    The same script brenn's own release tree is held to, over the same three
    trees: a rule tightened for one is tightened for both, and a bundle that
    passes here is one an installer can copy directory by directory.
    """,
    attrs = {
        "bundle": attr.label(
            mandatory = True,
            doc = "The `component_bundle` target to check.",
        ),
        "manifest": attr.label(
            allow_single_file = True,
            doc = "The manifest the staged copy is compared against.",
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/release:bundle_check.sh"),
        ),
        "_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:manifest_names.sh"),
        ),
        "_record_lib": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:record_lib.sh"),
        ),
    },
    test = True,
)

def component_bundle(name, spec_root = None, packages = [], surface_kinds = [], manifest = None, visibility = None):
    """A component repository's release tree, plus the gate on its contract.

    Pairing them here makes the gate structural, as `release_package` does for
    brenn's own tarball: the tree cannot be added to a graph without the check
    that every record in it binds the bytes beside it.

    Args:
        name: target name; also the staging directory's name.
        spec_root: the authored `.brenn` files of this repository's module root —
            a `glob` of it, so the label tracks the directory. The staged
            `modules/` tree is held set-equal to this, byte for byte. Required,
            possibly empty: a deployment's config gate certifies `use @…;`
            imports against the authored root rather than against a built
            bundle, and this is the only thing that makes the two the same set.
        packages: the `component_package` targets to stage under `components/`.
            Exactly the manifest's set — a bundle repository has no unshipped
            packages, so the assembly holds the two equal in both directions.
        surface_kinds: the `surface_processor_assets` targets whose transpiled
            trees stage under `surface/`.
        manifest: the manifest naming the shipped packages; required iff
            `packages` is non-empty, and refused otherwise.
        visibility: visibility of the staged tree.
    """
    if spec_root == None:
        fail("component_bundle(%s) states no spec_root; name the authored module root, even if it is empty" % name)
    if not packages and not surface_kinds:
        fail("component_bundle(%s) stages neither packages nor surface kinds, so it ships nothing" % name)
    if packages and not manifest:
        fail("component_bundle(%s) stages packages, so it needs the manifest that names them" % name)
    if manifest and not packages:
        fail("component_bundle(%s) names a manifest but stages no packages" % name)

    _component_bundle(
        name = name,
        manifest = manifest,
        packages = packages,
        spec_root = spec_root,
        surface_kinds = surface_kinds,
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

    _bundle_contract_test(
        name = name + "_contract",
        bundle = ":" + name,
        manifest = manifest,
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

def _grant_parity_test_impl(ctx):
    component = ctx.file.component
    check = ctx.file._check
    wit_lib = ctx.file._wit_lib
    dsl_cli = ctx.executable._dsl_cli
    script = _check_wrapper(
        ctx,
        check,
        [
            ctx.file._wasm_tools.short_path,
            wit_lib.short_path,
            dsl_cli.short_path,
            component.short_path,
            ctx.file.spec.short_path,
        ],
    )
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(files = [
            component,
            ctx.file.spec,
            ctx.file._wasm_tools,
            check,
            wit_lib,
            dsl_cli,
        ]).merge(ctx.attr._dsl_cli[DefaultInfo].default_runfiles),
    )]

_grant_parity_test = rule(
    implementation = _grant_parity_test_impl,
    doc = """Hold a specification's `requires` list equal to its artifact's imports.

    The third edge of the triangle. The deployment compile holds an instance's
    grants against the class's `requires`, and the host holds the artifact's
    imports against those same grants at load; without this, a specification
    that misstates its own component reaches a deployment before anything
    contradicts it, and fails there as the deployer's problem.

    A test rather than a step inside the package emitter: the emit script would
    have to learn the grant vocabulary, which lives in Rust and is single-sourced
    with the host's linker gating. `make check` runs the whole test graph and CD
    re-runs it against the pinned ref, so this gates every release either way.
    """,
    attrs = {
        "component": attr.label(
            allow_single_file = [".wasm"],
            mandatory = True,
            doc = "The built artifact whose imports are read.",
        ),
        "spec": attr.label(
            allow_single_file = [".brenn"],
            mandatory = True,
            doc = "The authored specification the component's package carries.",
        ),
        "_check": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:grant_parity_check.sh"),
        ),
        "_dsl_cli": attr.label(
            cfg = "exec",
            default = Label("//brenn-dsl:dsl_cli"),
            executable = True,
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
    test = True,
)

def grant_parity_test(name, component, spec):
    """The specification-against-artifact gate for one processor component.

    Args:
        name: target name.
        component: the `wasm_component` target holding the built artifact.
        spec: the authored `.brenn` specification label.
    """
    _grant_parity_test(
        name = name,
        component = component,
        spec = spec,
        size = "small",
        target_compatible_with = HOST_ONLY,
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

def _module_root(file):
    """The directory holding `file`, as the test's runfiles tree names it.

    A file at the workspace root has no directory segment, and `--modules ""` is
    a refusal about an unreadable directory rather than about where modules may
    live, so that case is named `.` — the smallest layout a consumer can have is
    one spec beside its `BUILD.bazel`.
    """
    root = file.short_path[:-(len(file.basename) + 1)]
    return root if root else "."

def _config_fit_test_impl(ctx):
    dsl_cli = ctx.executable._dsl_cli
    roots = []
    for module in ctx.files.modules:
        root = _module_root(module)
        if root not in roots:
            roots.append(root)
    if not roots:
        fail("`modules` names no `.brenn` file, so there is no module root to check against")
    args = ["check"]
    for root in sorted(roots):
        args.append("--modules")
        args.append(root)
    args.append(ctx.file.config.short_path)
    script = _check_wrapper(ctx, dsl_cli, args)
    return [DefaultInfo(
        executable = script,
        runfiles = ctx.runfiles(
            files = [ctx.file.config, dsl_cli] + ctx.files.modules + ctx.files.tree,
        ).merge(ctx.attr._dsl_cli[DefaultInfo].default_runfiles),
    )]

_config_fit_test = rule(
    implementation = _config_fit_test_impl,
    doc = """Compile one root document against the module roots a deployment gives it.

    The fit an author cannot check by building components: whether the document
    that stamps them resolves, whether every binding's doctypes propagate, and
    whether the vocabulary it imports is the vocabulary the module roots
    actually carry. Everything else in the gate set holds a component against
    its own specification; this holds a deployment against the set of them.

    The module roots are derived rather than declared: a `--modules` per
    distinct directory of the files named, which is the same rule a host is
    started with and the same one an installer's roots satisfy.
    """,
    attrs = {
        "config": attr.label(
            allow_single_file = [".brenn"],
            mandatory = True,
            doc = "The root document. Its directory is where `use` resolves from.",
        ),
        "modules": attr.label_list(
            allow_files = [".brenn"],
            mandatory = True,
            doc = "The packaged modules `use @<name>::…` resolves against, as " +
                  "files or filegroups of them. One `--modules` per distinct " +
                  "directory among them.",
        ),
        "tree": attr.label_list(
            allow_files = [".brenn"],
            default = [],
            doc = "The rest of the config tree the root imports with a plain " +
                  "`use`, which resolves against the root's own directory " +
                  "rather than against a module root.",
        ),
        "_dsl_cli": attr.label(
            cfg = "exec",
            default = Label("//brenn-dsl:dsl_cli"),
            executable = True,
        ),
    },
    test = True,
)

def config_fit_test(name, config, modules, tree = []):
    """See `_config_fit_test`.

    Args:
        name: target name.
        config: the root `.brenn` document.
        modules: the packaged modules it imports, as files or filegroups.
        tree: the root's own sibling documents, when it imports any.
    """
    _config_fit_test(
        name = name,
        config = config,
        modules = modules,
        tree = tree,
        size = "small",
        target_compatible_with = HOST_ONLY,
    )


# ---------------------------------------------------------------------------
# WIT staged as a directory, for a host crate's `bindgen!`
# ---------------------------------------------------------------------------

def _wit_root_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add(out.path)
    args.add_all(ctx.files.srcs)
    ctx.actions.run_shell(
        outputs = [out],
        inputs = ctx.files.srcs,
        arguments = [args],
        command = 'dest="$1"; shift; mkdir -p "$dest"; for src in "$@"; do cp "$src" "$dest/"; done',
        mnemonic = "WitRoot",
        progress_message = "Staging the WIT package for %s" % ctx.label,
    )
    return [DefaultInfo(files = depset([out]))]

wit_root = rule(
    implementation = _wit_root_impl,
    doc = """The WIT files of one package, staged flat into a declared directory.

    `wasmtime::component::bindgen!` resolves its `path` against
    `CARGO_MANIFEST_DIR` and then canonicalizes, so the env var has to name a
    real directory holding the world. A literal workspace-relative value is only
    right in this repository's own execroot: from a consumer module brenn's
    packages sit under `external/<canonical>/`, and a host crate a consumer
    tests against would look for the WIT where nothing is. The directory this
    declares is reached with `$(execpath)`, which carries that prefix wherever
    the crate is built from.
    """,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".wit"],
            mandatory = True,
            doc = "The package's `.wit` files, staged by basename.",
        ),
    },
)
