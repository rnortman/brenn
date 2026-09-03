#!/usr/bin/env bash
# The fit gate's other direction.
#
# `config_fit_test` is the only gate an out-of-tree deployer has over its own
# root document, and the in-tree call of it is the passing case. A gate that
# went permanently green — a wrapper that lost the exit status, the wrong file
# handed to `check` — would fail silently: every consumer's fit target would
# pass and the first refusal would be a boot panic on the target host. So the
# refusal is driven here, over a document whose vocabulary resolves out of the
# same two module roots and whose stamp does not fit.
#
# A sibling test rather than an `expect_failure` knob on the macro: a macro that
# can be told to want a failure is one an author can leave pointed the wrong way.
set -uo pipefail

dsl_cli="$1"
config="$2"
shift 2

roots=()
for module in "$@"; do
    dir="$(dirname "$module")"
    case " ${roots[*]-} " in
        *" $dir "*) ;;
        *) roots+=("--modules" "$dir") ;;
    esac
done

if out=$("$dsl_cli" check "${roots[@]}" "$config" 2>&1); then
    echo "FAIL: a document that does not fit its module roots was accepted: $out"
    exit 1
fi
if ! printf '%s' "$out" | grep -qF -e "parameter \`slug\` is a \`String\`"; then
    echo "FAIL: the refusal does not name the misfit: $out"
    exit 1
fi
echo "config_fit refusal: the gate refuses a document that does not fit"
