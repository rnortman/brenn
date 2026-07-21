#!/usr/bin/env sh
# Guards the three ways the scrub gate can be live in name only:
#   1. the repo's live hook shims drifting from the template other repos copy,
#   2. a clone where core.hooksPath was never pointed at .githooks, so no
#      git hook runs at all,
#   3. an installed brenn-scrub older than scrub/ source, so hook layers run
#      wrapper logic that no longer matches the tree.
# Run from the repo root.
set -eu

fail=0
err() { echo "scrub-selfcheck: $*" >&2; fail=1; }

# 1. Shim sync. pre-commit is deliberately repo-specific (it calls brenn's
# `make check` directly); everything else must be byte-identical. The two
# gitleaks.toml files are compared by the scrub crate's
# `repo_template_matches_the_tracked_public_config` test, which also runs under
# `make check` -- one authority for that invariant, not two normalizers in two
# languages that can drift apart.
check_same() {
    cmp -s "$1" "$2" || err "$1 differs from $2 -- shims live in the template; update both."
}

check_same .claude/hooks/scrub-check.sh scrub/repo-template/claude-hooks/scrub-check.sh
check_same .claude/hooks/format.sh scrub/repo-template/claude-hooks/format.sh
check_same .githooks/pre-push scrub/repo-template/githooks/pre-push

# 2. Hook activation. Skipped in CI, which checks out without hooks by design
# and is a scanning layer in its own right.
if [ -z "${CI:-}" ] && [ -e .git ]; then
    hp=$(git config --get core.hooksPath || true)
    [ "$hp" = ".githooks" ] || err "core.hooksPath is '${hp:-unset}', not .githooks -- commits and pushes on this clone skip the scrub gate. Run: make setup-hooks"
fi

# 3. Installed-binary staleness. Only meaningful where the binary exists;
# absence is already reported by the hooks themselves.
bin=$(command -v brenn-scrub || true)
if [ -n "$bin" ]; then
    newer=$(find scrub/src scrub/Cargo.toml -newer "$bin" -print -quit)
    [ -z "$newer" ] || err "installed brenn-scrub ($bin) is older than scrub/ source ($newer) -- hooks are running stale wrapper logic. Run: make setup-hooks
  (This compares mtimes, so a branch switch or rebase touching scrub/ can trip it without the binary's behavior having changed. Reinstalling is still the fix.)"
fi

[ "$fail" -eq 0 ] || exit 1
echo "scrub-selfcheck: ok"
