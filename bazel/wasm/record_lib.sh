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
