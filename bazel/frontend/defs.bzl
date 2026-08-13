"""The frontend's esbuild bundles.

One target per entry point. The bundler runs over the staged TypeScript tree,
so the generated
protocol types and stylesheet reach it as a dependency edge rather than as
files something else must have written first.
"""

load("@aspect_rules_js//js:defs.bzl", "js_run_binary")
load("//bazel/platforms:defs.bzl", "HOST_ONLY")

def frontend_bundle(
        name,
        out,
        entry,
        src_tree,
        node_modules,
        tool,
        sourcemap = False,
        build_id = True,
        visibility = ["//visibility:private"]):
    """Bundle one entry point of the staged frontend tree.

    Args:
        name: target name.
        out: the bundle's basename, which is also its served path in `dist`.
        entry: the entry point's path within the staged tree's `src/`.
        src_tree: the staged TypeScript tree to bundle out of.
        node_modules: the linked npm tree the bundle resolves imports against.
        tool: the `js_binary` wrapping `esbuild-bundle.mjs`.
        sourcemap: emit `<out>.map` beside the bundle.
        build_id: substitute the build id, which on stamped builds makes the
            bundle depend on the workspace status file.
        visibility: visibility of the bundle.
    """
    outs = [out]
    if sourcemap:
        outs.append(out + ".map")

    args = [
        # The tool runs with the bin directory as its working directory, so a
        # generated input is named by its bin-relative path and outputs are
        # written to theirs. The entry point is named relative to the tree
        # instead, which is what the bundle's own module names are relative to.
        "--root",
        "$(rootpath %s)" % src_tree,
        "--entry",
        "src/%s" % entry,
        "--outfile",
        "%s/%s" % (native.package_name(), out),
    ]
    if sourcemap:
        args.append("--sourcemap")
    if build_id:
        args.append("--build-id")

    # On a stamped build the bundler is told so, instead of inferring it from an
    # environment variable whose absence also means "dev". A release that cannot
    # reach the status file then fails the build rather than baking in the
    # placeholder and shipping a browser that disagrees with the backend about
    # which build is running.
    stamp_args = select({
        "//bazel/frontend:stamped": ["--require-stamp"],
        "//conditions:default": [],
    }) if build_id else []

    js_run_binary(
        name = name,
        srcs = [
            src_tree,
            node_modules,
        ],
        args = args + stamp_args,
        outs = outs,
        # The bundler resolves the staged tree's real location so the module
        # names it writes into the bundle are the source tree's. rules_js
        # patches node's `fs` to stop exactly that resolution, and it patches
        # per invocation, so the tool's own setting is not enough. esbuild is a
        # native process and resolves regardless; the patch would only put the
        # two out of step.
        patch_node_fs = False,
        # -1: the build id is baked in on release builds and left as its
        # placeholder on dev builds, so a dev bundle never keys on the status
        # file and never rebuilds because the working tree moved.
        stamp = -1 if build_id else 0,
        target_compatible_with = HOST_ONLY,
        tool = tool,
        visibility = visibility,
    )
