"""The deploy tarball's staged tree.

What ships is one directory: the two host binaries, the two served asset trees,
and the WASM component packages the deploy manifest names. Assembling it here rather
than in the deploying repo's workflow is what makes the layout a build output
with declared inputs — a dropped asset fails a test instead of arriving as a
404 on the deploy target.

The binaries are the only part of the tree that is architecture-specific, so
they carry the platform rather than the invocation: an incoming transition puts
them under the musl platform while the browser bundles, the components and the
Python stub stay host-configured. One `bazel build` therefore produces the
whole tree.
"""

load("@bazel_skylib//rules:native_binary.bzl", "native_test")
load("//bazel/platforms:defs.bzl", "HOST_ONLY")

_MUSL_PLATFORM = str(Label("//bazel/platforms:linux_x86_64_musl"))
_STATIC_MUSL_FLAG = str(Label("//bazel/release:static_musl"))
_PLATFORMS = "//command_line_option:platforms"

def _binaries_transition_impl(settings, _attr):
    if settings[_STATIC_MUSL_FLAG]:
        return {_PLATFORMS: [_MUSL_PLATFORM]}

    # A dev build of the package reuses the host binaries the
    # rest of the graph already built, so the packaging logic and its gates are
    # exercised on every `bazel test //...` for the price of a copy.
    return {_PLATFORMS: [str(p) for p in settings[_PLATFORMS]]}

_binaries_transition = transition(
    implementation = _binaries_transition_impl,
    inputs = [_STATIC_MUSL_FLAG, _PLATFORMS],
    outputs = [_PLATFORMS],
)

def _sole_file(target, what):
    files = target[DefaultInfo].files.to_list()
    if len(files) != 1:
        fail("%s must produce exactly one %s, got %s" % (target.label, what, files))
    return files[0]

def _sole_directory(target):
    directory = _sole_file(target, "directory")
    if not directory.is_directory:
        fail("%s must produce a directory, got %s" % (target.label, directory))
    return directory

def _release_package_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)

    binaries = [_sole_file(t, "executable") for t in ctx.attr.binaries]
    frontend = _sole_directory(ctx.attr.frontend)
    surface = _sole_directory(ctx.attr.surface)

    args = ctx.actions.args()
    args.add("--out", out.path)
    args.add("--manifest", ctx.file.manifest)
    args.add("--names", ctx.file._manifest_names)
    args.add("--dom-names", ctx.file._dom_names)
    args.add("--record-lib", ctx.file._record_lib)
    args.add("--frontend", frontend.path)
    args.add("--surface", surface.path)
    args.add_all(binaries, before_each = "--bin")
    args.add_all(ctx.files.lib_files, before_each = "--lib")
    args.add_all(ctx.files.packages, before_each = "--package")
    args.add_all(ctx.files.modules, before_each = "--module")

    ctx.actions.run(
        outputs = [out],
        inputs = depset(
            binaries + ctx.files.packages + ctx.files.modules +
            ctx.files.lib_files + [ctx.file.manifest],
            transitive = [
                ctx.attr.frontend[DefaultInfo].files,
                ctx.attr.surface[DefaultInfo].files,
            ],
        ),
        executable = ctx.file._assemble,
        tools = [
            ctx.file._manifest_names,
            ctx.file._dom_names,
            ctx.file._record_lib,
        ],
        arguments = [args],
        mnemonic = "ReleasePackage",
        progress_message = "Staging the release tree at %s" % out.short_path,
    )
    return [DefaultInfo(
        files = depset([out]),
        runfiles = ctx.runfiles(files = [out]),
    )]

_release_package = rule(
    implementation = _release_package_impl,
    doc = """The unpacked shape of the deploy tarball, as one directory.

    `VERSION` and `deploy.sh` are deliberately absent: the script is the
    deploying repo's file and the version is the pin that repo resolved, so
    both are added there, beside the `tar` invocation.
    """,
    attrs = {
        "binaries": attr.label_list(
            allow_empty = False,
            cfg = _binaries_transition,
            mandatory = True,
            doc = "Host binaries, installed to `bin/`.",
        ),
        "frontend": attr.label(
            mandatory = True,
            doc = "The frontend asset tree, copied to `frontend/`.",
        ),
        "lib_files": attr.label_list(
            allow_files = True,
            doc = "Loose files installed to `lib/`, the MCP stub among them.",
        ),
        "manifest": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The deploy manifest naming the components that ship.",
        ),
        "modules": attr.label_list(
            allow_empty = False,
            allow_files = [".brenn"],
            mandatory = True,
            doc = "The authored modules of the backend components that ship, staged under `modules/`.",
        ),
        "packages": attr.label_list(
            allow_empty = False,
            mandatory = True,
            doc = "The built package directories; the manifest picks the shipped ones.",
        ),
        "surface": attr.label(
            mandatory = True,
            doc = "The surface asset tree, copied to `surface/`.",
        ),
        "_assemble": attr.label(
            allow_single_file = True,
            default = Label("//bazel/release:assemble.sh"),
        ),
        "_dom_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/surface:dom_names.sh"),
        ),
        "_manifest_names": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:manifest_names.sh"),
        ),
        "_record_lib": attr.label(
            allow_single_file = True,
            default = Label("//bazel/wasm:record_lib.sh"),
        ),
    },
)

def release_package(name, manifest, binaries, packages, modules, frontend, surface, lib_files = [], visibility = None):
    """The staged release tree, plus the gate on the contract `deploy.sh` reads.

    Pairing them here makes the gate structural: the tree cannot be added to
    the graph without the check that it holds what the deploy script reaches
    for, and the check's linkage arm follows the same flag the binaries'
    transition does.

    Args:
        name: target name; also the staging directory's name.
        manifest: the deploy manifest naming the packages that ship.
        binaries: host binaries, installed to `bin/`.
        packages: every `component_package` target in the tree.
        modules: the authored module of every backend component that ships.
        frontend: the frontend asset tree.
        surface: the surface asset tree.
        lib_files: loose files installed to `lib/`.
        visibility: visibility of the staged tree.
    """
    _release_package(
        name = name,
        binaries = binaries,
        frontend = frontend,
        lib_files = lib_files,
        manifest = manifest,
        modules = modules,
        packages = packages,
        surface = surface,
        target_compatible_with = HOST_ONLY,
        visibility = visibility,
    )

    native_test(
        name = name + "_contract",
        size = "small",
        src = "//bazel/release:package_check.sh",
        args = [
            "$(rootpath //bazel/wasm:manifest_names.sh)",
            "$(rootpath //bazel/wasm:record_lib.sh)",
            "$(rootpath //bazel/surface:dom_names.sh)",
            "$(rootpath :%s)" % name,
            "$(rootpath %s)" % manifest,
        ] + select({
            "//bazel/release:musl_binaries": ["static"],
            "//conditions:default": ["dynamic"],
        }),
        data = [
            manifest,
            "//bazel/surface:dom_names.sh",
            "//bazel/wasm:manifest_names.sh",
            "//bazel/wasm:record_lib.sh",
            ":" + name,
        ],
        out = name + "_contract.run",
        target_compatible_with = HOST_ONLY,
    )
