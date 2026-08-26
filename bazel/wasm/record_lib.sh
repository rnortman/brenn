# Reading one field out of a component package's binding record.
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
