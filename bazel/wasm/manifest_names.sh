#!/usr/bin/env bash
# The deploy manifest's grammar, in one place.
#
# Usage: manifest_names.sh <manifest>
#
# Prints the component basenames the manifest names, one per line, in file
# order. An entry is a line with its `#` comment removed and every whitespace
# character stripped; a line left empty by that is not an entry. A final line
# with no trailing newline is still read — it is the entry most likely to have
# just been appended.
#
# Three places act on this grammar: the gate asserting every named artifact is
# built, the assembly that copies what it names into the release tree, and the
# gate on the staged tree. The manifest is the single statement of the deployed
# set, so its grammar is stated once too — three readers that agree only by
# inspection disagree the first time the format grows an annotation, and then a
# gate passes a manifest the assembly rejects.
#
# Emptiness is not judged here: each caller has its own account of what an
# empty manifest means, and all three treat it as a failure.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <manifest>" >&2
    exit 2
fi
manifest="$1"
if [ ! -f "$manifest" ]; then
    echo "ERROR: $manifest is not a readable file" >&2
    exit 1
fi

# `|| [ -n "$line" ]` is what keeps the unterminated final line: `read` reports
# failure on it, having filled the variable anyway.
while read -r line || [ -n "$line" ]; do
    line="${line%%#*}"
    # A pattern substitution rather than a `tr` pipeline: one fork per manifest
    # line for a name that cannot contain whitespace.
    line="${line//[[:space:]]/}"
    [ -n "$line" ] || continue
    printf '%s\n' "$line"
done < "$manifest"
