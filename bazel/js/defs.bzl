"""Paths for tools that run with the output tree's root as their cwd.

`js_binary`'s launcher changes directory into `BAZEL_BINDIR` before it execs, so
every path a `js_run_binary` hands its tool — inputs to read, directories to
write — is relative to the root of the output tree.

`$(rootpath)` spells that only for the main repository. A file of an external
repository has a short path of `../<canonical name>/…`, while the tree the tool
walks holds it at `external/<canonical name>/…`, so a `$(rootpath)` argument
resolves out of the output tree instead of into it. That is invisible in
brenn's own build, where brenn is the main repository, and is what every module
that depends on brenn sees.

Not part of the external authoring contract: this is how brenn's own macros
spell their arguments, not something a consumer calls.
"""

def bindir_relative(path):
    """`path`, a file or directory name in the calling package, relative to the bin dir.

    Call from a macro body, where `native.package_name()` and
    `native.repo_name()` describe the package the BUILD file is in.

    Args:
        path: a package-relative file or directory name — a declared output, or
            a file the macro itself staged into the package.

    Returns:
        The same file named the way a tool whose cwd is the output tree's root
        must name it.
    """
    package = native.package_name()
    if package:
        path = "%s/%s" % (package, path)
    repo = native.repo_name()
    if repo:
        path = "external/%s/%s" % (repo, path)
    return path
