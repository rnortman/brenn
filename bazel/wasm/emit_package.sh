#!/usr/bin/env bash
# Emit one component package's binding record, and the packaged copy of its
# specification.
#
# Usage: emit_package.sh <name> <world> <artifact> <record-out>
#                        [<spec-in> <spec-out>]
#
# WASM_TOOLS names the `wasm-tools` binary; unset, one is looked up on PATH.
# WIT_LIB names the shared WIT-scraping library; unset, it is read from its path
# relative to the execroot.
#
# A shipped component is three sibling files sharing the artifact's stem: the
# `.wasm`, the `.spec.brenn` copy of the specification its author wrote, and the
# `.package.json` record emitted here. The record is the statement that those
# files were produced together — `artifact_sha256` and `spec_sha256` are what
# the host re-computes at boot, so an artifact installed beside a spec it was
# not built with is a refusal to boot rather than a component running against a
# contract nobody checked.
#
# The record is an external contract: an out-of-tree component ships one too, in
# this shape, or it does not load. `v` is the whole compatibility story — there
# are no shims, and the reader refuses an unknown field.
#
# `world` is the WIT package the artifact targets. It is declared on the target
# rather than read out of the artifact because an import-GC'd component can
# carry no brenn import at all, and a component with no imports still belongs to
# exactly one world. What is read out of the artifact is the contradiction: a
# `brenn:` import naming a package other than the declared world means the
# declaration is stale, and a stale world tag is what lets a replay artifact be
# installed against a processor record.
#
# Replay-world components package as artifact plus record and no spec: they have
# no component class, no ports and no grants, so a spec for one would be
# vocabulary with nothing to say. The spec fields are present exactly when the
# world is `brenn:processor`, in both directions.
set -euo pipefail

if [ "$#" -ne 4 ] && [ "$#" -ne 6 ]; then
    echo "usage: $0 <name> <world> <artifact> <record-out> [<spec-in> <spec-out>]" >&2
    exit 2
fi

name="$1"
world="$2"
artifact="$3"
record_out="$4"
spec_in="${5:-}"
spec_out="${6:-}"

case "$world" in
    brenn:processor | brenn:replay) ;;
    *)
        echo "emit_package: $name declares world '$world', which is not one this host" \
             "links. The worlds are brenn:processor and brenn:replay." >&2
        exit 1
        ;;
esac

# Spec-iff-processor, at the emitter as well as at the reader: a replay package
# carrying a spec and a processor package carrying none are both records that
# would describe a component shape that does not exist.
if [ "$world" = brenn:processor ] && [ -z "$spec_in" ]; then
    echo "emit_package: $name targets brenn:processor and must package the specification" \
         "its author wrote; a processor component with no spec is one a deployment could" \
         "only instantiate by authoring the class itself." >&2
    exit 1
fi
if [ "$world" = brenn:replay ] && [ -n "$spec_in" ]; then
    echo "emit_package: $name targets brenn:replay, which has no component class, no ports" \
         "and no grants; there is nothing for a spec to state." >&2
    exit 1
fi

if [ ! -f "$artifact" ]; then
    echo "emit_package: $artifact is not a readable file" >&2
    exit 1
fi
if [ -n "$spec_in" ] && [ ! -f "$spec_in" ]; then
    echo "emit_package: $spec_in is not a readable file" >&2
    exit 1
fi

artifact_name="$(basename "$artifact")"
artifact_sha="$(sha256sum "$artifact" | awk '{print $1}')"

# Only `brenn:` packages are judged. A `wasi:` import is a different failure
# with its own gate (`wasi_import_check.sh`), and reporting it twice would send
# the reader to the wrong fix. The scrape refuses an import line it cannot read
# fully, so "no contradiction found" cannot mean "nothing was looked at".
. "${WIT_LIB:-bazel/wasm/wit_lib.sh}"
# Assigned rather than read straight into the loop: a scrape that refuses must
# fail the emit, and `set -e` sees a failed command substitution in an
# assignment and not one in a redirection.
imports="$(wit_imports "$artifact")"
while read -r import; do
    [ -n "$import" ] || continue
    package="${import%%/*}"
    case "$package" in
        brenn:*) ;;
        *) continue ;;
    esac
    if [ "$package" != "$world" ]; then
        echo "emit_package: $name declares world '$world' but its artifact imports from" \
             "'$package'. The declaration is stale: a component that moved between worlds" \
             "keeps its old tag until this is updated, and the tag is what the host checks" \
             "the record against." >&2
        exit 1
    fi
done <<< "$imports"

{
    printf '{\n'
    printf '  "v": 1,\n'
    printf '  "name": "%s",\n' "$(json_escape "$name")"
    printf '  "world": "%s",\n' "$(json_escape "$world")"
    printf '  "artifact": "%s",\n' "$(json_escape "$artifact_name")"
    if [ -n "$spec_in" ]; then
        spec_sha="$(sha256sum "$spec_in" | awk '{print $1}')"
        printf '  "artifact_sha256": "%s",\n' "$(json_escape "$artifact_sha")"
        printf '  "spec": "%s",\n' "$(json_escape "$(basename "$spec_out")")"
        printf '  "spec_sha256": "%s"\n' "$(json_escape "$spec_sha")"
    else
        printf '  "artifact_sha256": "%s"\n' "$(json_escape "$artifact_sha")"
    fi
    printf '}\n'
} > "$record_out"

# The packaged copy is byte-for-byte the author's file under the artifact's
# stem, so every package file is derivable from the artifact's basename and the
# hash in the record is the hash of what boot reads.
if [ -n "$spec_in" ]; then
    cp -L "$spec_in" "$spec_out"
fi
