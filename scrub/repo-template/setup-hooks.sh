#!/bin/bash
# Activate the tracked git hooks in a repo that has no Makefile. Idempotent.
set -eu

git config core.hooksPath .githooks
rm -f .git/hooks/pre-commit

command -v brenn-scrub >/dev/null 2>&1 || \
    echo "brenn-scrub not on PATH: cargo install --path <brenn>/scrub"
command -v gitleaks >/dev/null 2>&1 || \
    echo "gitleaks not on PATH: install the release pinned in scrub/src/gitleaks.rs"

echo "setup-hooks: done."
