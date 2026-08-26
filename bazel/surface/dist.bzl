"""Assembly of `surface/dist`, the browser asset tree.

What the server serves and the deploy tarball carries is one directory: the
wasm-bindgen bundles under their frozen `brenn_*` names, each component's
documentation sidecars renamed to match, and one jco-transpiled tree per
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
        sidecars: the crate's documentation files, in the package. Hand-authored
            ones (`schema.json`) are renamed here; the generated help sidecar
            arrives already named for the artifact.
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
# jco
# ---------------------------------------------------------------------------

def _processor_stage_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add(ctx.attr.kind)
    args.add(ctx.file.component)
    args.add(ctx.file.transpiled.path)
    args.add(ctx.file.jco_version)
    args.add(ctx.file._emitter)
    args.add(out.path)

    ctx.actions.run(
        outputs = [out],
        inputs = [
            ctx.file.component,
            ctx.file.transpiled,
            ctx.file.jco_version,
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

def surface_processor_assets(name, kind, component, jco_version, visibility = ["//visibility:public"]):
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
