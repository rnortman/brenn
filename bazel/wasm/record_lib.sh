# Reading a component package's binding records: one field out of one record,
# and the set of dom-kind records a staged surface tree holds.
#
# Sourced, not executed: `RECORD_LIB` names this file for the callers that take
# it as an argument.
#
# The record is JSON, but the only readers in shell are gates that must not
# depend on a JSON parser being installed on a build machine. The shape the
# emitter writes is fixed — one `"key": "value"` per line — so a line scrape is
# enough, and stating it once is what keeps the staged-tree gate and the
# emitter's own test from drifting apart: a record format change that reaches
# one scrape and not the other leaves the unfixed one matching nothing, which is
# indistinguishable from a field that is not there.

# The string value of `<key>` in `<record>`, or empty when the record does not
# state it. Callers treat empty as "not stated"; a record that legitimately
# holds an empty string for a field it must state is not a shape this emitter
# writes.
#
# Usage: record_field <record> <key>
record_field() {
    sed -n "s/^[[:space:]]*\"$2\"[[:space:]]*:[[:space:]]*\"\\(.*\\)\"[[:space:]]*,\\{0,1\\}[[:space:]]*$/\\1/p" "$1"
}

# Every dom-kind record in a staged surface tree, one path per line, sorted.
#
# A dom kind's record sits flat in the tree under wasm-bindgen's stem for the
# kind, so the set is a directory listing and not a list anything states. Stated
# once here because both the release assembler and the staged-tree gate walk it,
# for staging and for verifying the same files.
#
# Usage: surface_dom_records <surface-dir>
surface_dom_records() {
    find -L "$1" -maxdepth 1 -name 'brenn_*.manifest.json' | LC_ALL=C sort
}

# The naming rules a component package's record is held to, as a stream of
# facts one per line, `<kind><TAB><value>`:
#
#   fail <message>   a rule the package breaks, in the caller's own voice
#   artifact <name>  the artifact basename the record states
#   spec <name>      the specification basename, absent when it states none
#
# Usage: package_shape <package-dir>
#
# Stated here because three readers hold a package to these rules — the host at
# boot, the workspace components root's gate, and the staged release tree's —
# and only the two shell ones can share a statement of them. A rule added to the
# host's reader that reaches one gate and not the other ships a tree one gate
# passed and the deploy target refuses. Presence and hashes are the caller's:
# one gate re-computes them, the other only asserts the files are there.
package_shape() {
    _shape_dir="$1"
    _shape_name="$(basename "$_shape_dir")"
    _shape_record="$_shape_dir/package.json"

    # The directory's basename is the name a configuration states, and the
    # record repeats it: a package under any other name is one the host refuses.
    _shape_stated="$(record_field "$_shape_record" name)"
    if [ "$_shape_stated" != "$_shape_name" ]; then
        printf 'fail\t%s\n' "the record calls itself $_shape_stated, but the package is named $_shape_name"
    fi

    # The artifact keeps the stem the build gave it, so its name is the
    # record's to state rather than the host's to derive — which makes the
    # separator and the extension the only things holding it to the directory.
    _shape_artifact="$(record_field "$_shape_record" artifact)"
    case "$_shape_artifact" in
        "") printf 'fail\t%s\n' "the record states no artifact" ;;
        */*) printf 'fail\t%s\n' "the record names the artifact $_shape_artifact, which reaches outside the package directory" ;;
        *.wasm) printf 'artifact\t%s\n' "$_shape_artifact" ;;
        *) printf 'fail\t%s\n' "the record names $_shape_artifact as its artifact, which is not a component" ;;
    esac

    # A specification, where the record names one, is read under the package's
    # own name and under no other.
    _shape_spec="$(record_field "$_shape_record" spec)"
    if [ -n "$_shape_spec" ]; then
        if [ "$_shape_spec" = "$_shape_name.brenn" ]; then
            printf 'spec\t%s\n' "$_shape_spec"
        else
            printf 'fail\t%s\n' "the record names $_shape_spec as its spec, but the host derives that name as $_shape_name.brenn and reads no other file"
        fi
    fi
}
