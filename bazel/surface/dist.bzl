"""Assembly of `surface/dist`, the browser asset tree.

What the server serves and the deploy tarball carries is one directory: the
wasm-bindgen bundles under their frozen `brenn_*` names, each component's
documentation sidecars renamed to match, each dom kind's binding record and
packaged specification beside its bundle, and one jco-transpiled tree per
processor kind beside the boot-validation manifest that binds it to the
component bytes it came from.

Each crate stages its own contribution, so the dir → artifact naming stays in
the package that owns the crate, and `surface_dist` merges the stages.
"""

load("@aspect_bazel_lib//lib:copy_to_directory.bzl", "copy_to_directory")
load("@aspect_rules_js//js:defs.bzl", "js_run_binary")
load("//bazel/platforms:defs.bzl", "HOST_ONLY")

def surface_crate_stage(name, bundle, artifact, sidecars = [], visibility = ["//visibility:public"]):
    """One crate's slice of the asset tree: its bundle, plus renamed sidecars.

    Args:
        name: target name; also the staging directory's name.
        bundle: name of the `rust_wasm_bindgen` target in this package whose
            files land at the root of the tree.
        artifact: the bundle's `brenn_*` basename, which the sidecars take too.
        sidecars: the crate's files that ship flat beside the bundle — its
            documentation, and for a component the binding record and packaged
            specification. Hand-authored ones (`schema.json`) are renamed here;
            generated ones arrive already named for the artifact.
        visibility: visibility of the staging directory.
    """
    package = native.package_name()

    copy_to_directory(
        name = name,
        srcs = [":" + bundle] + sidecars,
        # `rust_wasm_bindgen` emits a `.empty` marker in its snippets directory;
        # the served asset tree has no use for it.
        exclude_srcs_patterns = ["**/snippets/.empty"],
        # The bundle's files sit one level deeper than the sidecars; the longest
        # matching root path wins, so both land flat.
        replace_prefixes = {
            "schema.json": artifact + ".schema.json",
        },
        root_paths = [
            package + "/" + bundle,
            package,
        ],
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

# ---------------------------------------------------------------------------
# dom records
# ---------------------------------------------------------------------------

# A dom kind's file grammar, on the Starlark side. `bazel/surface/dom_names.sh`
# is the same statement for the shell readers — the record emitter and the
# staged-tree gate — and `brenn_surface_contract` states it for the host. The
# emitter is handed these paths and holds every one of them to the grammar, so
# the two sides are checked against each other on every build rather than by
# inspection.
_MODULE_EXT = ".js"

_MODULE_WASM_EXT = "_bg.wasm"

_RECORD_EXT = ".manifest.json"

_SPEC_EXT = ".spec.brenn"

def _dom_package_impl(ctx):
    bundle_files = ctx.attr.bundle[DefaultInfo].files.to_list()
    module_name = ctx.attr.artifact + _MODULE_EXT
    wasm_name = ctx.attr.artifact + _MODULE_WASM_EXT
    module = None
    module_wasm = None
    # Selection is by basename, so a bundle emitting a second file under either
    # name — a snippet, a nested copy — would make the choice depend on
    # iteration order and could hash a file the tree never serves. Two matches
    # is a build failure, not a coin flip.
    for file in bundle_files:
        if file.basename == module_name:
            if module != None:
                fail("%s: bundle %s emits two files named %s (%s, %s); the record can bind only the one served from the asset root" % (
                    ctx.label,
                    ctx.attr.bundle.label,
                    module_name,
                    module.path,
                    file.path,
                ))
            module = file
        elif file.basename == wasm_name:
            if module_wasm != None:
                fail("%s: bundle %s emits two files named %s (%s, %s); the record can bind only the one served from the asset root" % (
                    ctx.label,
                    ctx.attr.bundle.label,
                    wasm_name,
                    module_wasm.path,
                    file.path,
                ))
            module_wasm = file
    if module == None or module_wasm == None:
        fail("%s: bundle %s emits no %s/%s pair; got %s" % (
            ctx.label,
            ctx.attr.bundle.label,
            module_name,
            wasm_name,
            [f.basename for f in bundle_files],
        ))

    record = ctx.actions.declare_file(ctx.attr.artifact + _RECORD_EXT)
    spec = ctx.actions.declare_file(ctx.attr.artifact + _SPEC_EXT)

    args = ctx.actions.args()
    args.add(ctx.attr.kind)
    args.add(module)
    args.add(module_wasm)
    args.add(ctx.file.spec)
    args.add(record)
    args.add(spec)

    ctx.actions.run(
        outputs = [record, spec],
        inputs = [module, module_wasm, ctx.file.spec, ctx.file._wit_lib, ctx.file._dom_names],
        executable = ctx.file._emitter,
        arguments = [args],
        env = {
            "DOM_NAMES": ctx.file._dom_names.path,
            "WIT_LIB": ctx.file._wit_lib.path,
        },
        mnemonic = "SurfaceDomPackage",
        progress_message = "Emitting surface dom record for %s" % ctx.attr.kind,
    )
    return [DefaultInfo(files = depset([record, spec]))]

_dom_package = rule(
    implementation = _dom_package_impl,
    doc = """`brenn_<kind>.manifest.json` + `brenn_<kind>.spec.brenn`.

    The dom analog of the processor kind's manifest: the record binds the served
    module pair to the specification the component was authored against, and the
    packaged specification is the author's file verbatim. Both land flat in the
    asset root beside the pair, because that is where a wasm-bindgen bundle
    ships.
    """,
    attrs = {
        "artifact": attr.string(
            mandatory = True,
            doc = "The bundle's `brenn_*` basename, which the record's files take too.",
        ),
        "bundle": attr.label(
            mandatory = True,
            doc = "The `rust_wasm_bindgen` target whose module pair is hashed.",
        ),
        "kind": attr.string(mandatory = True),
        "spec": attr.label(
            allow_single_file = [".brenn"],
            mandatory = True,
            doc = "The component's authored specification, copied into the tree.",
        ),
        "_dom_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/surface:dom_names.sh"),
        ),
        "_emitter": attr.label(
            allow_single_file = True,
            default = Label("//bazel/surface:emit_dom_manifest.sh"),
        ),
        "_wit_lib": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:wit_lib.sh"),
        ),
    },
)

def surface_dom_package(name, kind, artifact, bundle, spec, visibility = ["//visibility:public"]):
    """Emit one dom kind's binding record and packaged specification.

    Args:
        name: target name.
        kind: the component kind, which the record states and boot looks it up by.
        artifact: the bundle's `brenn_*` basename.
        bundle: the `rust_wasm_bindgen` target holding the served module pair.
        spec: the component's authored specification under `config/specs`. Its
            hash is the record's binding to the configuration that names this
            kind, so a kind whose spec is unauthored has no build.
        visibility: visibility of the emitted files.
    """
    _dom_package(
        name = name,
        artifact = artifact,
        bundle = bundle,
        kind = kind,
        spec = spec,
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

# ---------------------------------------------------------------------------
# jco
# ---------------------------------------------------------------------------

def _processor_stage_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add(ctx.attr.kind)
    args.add(ctx.file.component)
    args.add(ctx.file.transpiled.path)
    args.add(ctx.file.jco_version)
    args.add(ctx.file.spec)
    args.add(ctx.file._emitter)
    args.add(out.path)

    ctx.actions.run(
        outputs = [out],
        inputs = [
            ctx.file.component,
            ctx.file.transpiled,
            ctx.file.jco_version,
            ctx.file.spec,
            ctx.file._emitter,
            ctx.file._wit_lib,
        ],
        tools = [ctx.file._wasm_tools],
        executable = ctx.file._stage,
        arguments = [args],
        env = {
            "WASM_TOOLS": ctx.file._wasm_tools.path,
            "WIT_LIB": ctx.file._wit_lib.path,
        },
        mnemonic = "SurfaceProcessorStage",
        progress_message = "Staging surface processor assets for %s" % ctx.attr.kind,
    )
    return [DefaultInfo(files = depset([out]))]

_processor_stage = rule(
    implementation = _processor_stage_impl,
    doc = """`processor/<kind>/`: the transpiled tree, the component, the manifest.

    The manifest is emitted by the same script the component build runs, so the
    staged manifest and the component's own are comparable byte for byte.
    """,
    attrs = {
        "component": attr.label(
            allow_single_file = [".wasm"],
            mandatory = True,
            doc = "The component artifact the transpile consumed.",
        ),
        "jco_version": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "A file holding the jco version, recorded in the manifest.",
        ),
        "kind": attr.string(mandatory = True),
        "spec": attr.label(
            allow_single_file = [".brenn"],
            mandatory = True,
            doc = "The component's authored specification, copied into the tree.",
        ),
        "transpiled": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The `js_run_binary` output directory jco wrote.",
        ),
        "_emitter": attr.label(
            allow_single_file = True,
            default = Label("//surface:emit-processor-manifest.sh"),
        ),
        "_stage": attr.label(
            allow_single_file = True,
            default = Label("//bazel/surface:processor_stage.sh"),
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

def surface_processor_assets(name, kind, component, jco_version, spec, visibility = ["//visibility:public"]):
    """Transpile one processor component and stage the result for `surface/dist`.

    `--instantiation async` is load-bearing: it makes the emitted module export
    an `instantiate(getCoreModule, imports)` function instead of instantiating
    at module scope, which is what one compiled module per kind and one
    instantiation per instance requires.

    Args:
        name: target name; also the staging directory's name.
        kind: the processor kind, which names the transpiled module tree.
        component: the `wasm_component` target to transpile.
        jco_version: a file holding the jco version, recorded in the manifest.
        spec: the component's authored specification under `config/specs`. Its
            hash is the manifest's binding to the configuration that names this
            kind, so a kind whose spec is unauthored has no build.
        visibility: visibility of the staging directory.
    """
    transpiled = name + "_transpile"

    js_run_binary(
        name = transpiled,
        srcs = [component],
        args = [
            "transpile",
            # Paths are relative to the bin dir the js tool runs in, which is
            # what `$(rootpath)` yields for a generated file.
            "$(rootpath %s)" % component,
            "--instantiation",
            "async",
            "--name",
            kind,
            "--out-dir",
            "%s/%s" % (native.package_name(), transpiled),
        ],
        out_dirs = [transpiled],
        target_compatible_with = HOST_ONLY,
        tool = "//surface:jco",
    )

    _processor_stage(
        name = name,
        component = component,
        jco_version = jco_version,
        kind = kind,
        spec = spec,
        target_compatible_with = HOST_ONLY,
        transpiled = ":" + transpiled,
        visibility = visibility,
    )

# ---------------------------------------------------------------------------
# The tree
# ---------------------------------------------------------------------------

def _stage_directory(target):
    """The single directory a staging target produces."""
    files = target[DefaultInfo].files.to_list()
    if len(files) != 1 or not files[0].is_directory:
        fail("%s must produce exactly one directory, got %s" % (target.label, files))
    return files[0]

def _surface_dist_impl(ctx):
    stages = [_stage_directory(t) for t in ctx.attr.stages]
    out = ctx.actions.declare_directory(ctx.attr.dirname)

    args = ctx.actions.args()
    args.add(out.path)

    # Without this each stage would arrive as its expanded file list, and the
    # merge would be handed files where it expects directories.
    args.add_all(stages, expand_directories = False)

    ctx.actions.run(
        outputs = [out],
        inputs = stages,
        executable = ctx.file._merge,
        arguments = [args],
        mnemonic = "SurfaceDist",
        progress_message = "Assembling %s" % out.short_path,
    )
    return [DefaultInfo(
        files = depset([out]),
        runfiles = ctx.runfiles(files = [out]),
    )]

surface_dist = rule(
    implementation = _surface_dist_impl,
    doc = """Merge the staged trees into the served asset directory.

    Declared in `//surface` under the name `dist`, so a test that reads the tree
    finds it at the same workspace-relative path the build writes and the
    server config names.
    """,
    attrs = {
        "dirname": attr.string(
            mandatory = True,
            doc = "Name of the output directory, relative to this package.",
        ),
        "stages": attr.label_list(
            mandatory = True,
            doc = "Staging targets, each producing exactly one directory.",
        ),
        "_merge": attr.label(
            allow_single_file = True,
            default = Label("//bazel/surface:merge_stages.sh"),
        ),
    },
)
