#!/usr/bin/env bash
# Create test graf repos and manifest for local dev.
# Idempotent — safe to re-run. Deletes and recreates everything.
#
# Usage: ./dev/setup-graf-testdata.sh
# Prerequisites: graf binary on PATH, built from ../graf/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRAFDATA="$SCRIPT_DIR/grafdata"
MANIFEST="$SCRIPT_DIR/graf-manifest.toml"

# Verify graf is available.
if ! command -v graf &>/dev/null; then
    echo "ERROR: graf not found on PATH. Build and install from ../graf/" >&2
    exit 1
fi

# Clean slate.
rm -rf "$GRAFDATA" "$MANIFEST"
mkdir -p "$GRAFDATA"

# Touch the manifest file so graf manifest commands don't fall back to
# ~/.config/graf/manifest.toml.
touch "$MANIFEST"
export GRAF_MANIFEST="$MANIFEST"

# --- Date computation ---
# We compute dates relative to today so the test data always shows
# interesting groupings (Overdue, Today, Tomorrow, named weekday, etc.)

today=$(date +%Y-%m-%d)
tomorrow=$(date -d "+1 day" +%Y-%m-%d)
in2days=$(date -d "+2 days" +%Y-%m-%d)
in3days=$(date -d "+3 days" +%Y-%m-%d)
in10days=$(date -d "+10 days" +%Y-%m-%d)
overdue="2026-03-15"
now_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# --- Helper ---

create_todo() {
    local file="$1"
    local tldr="$2"
    local priority="$3"
    local date="$4"       # empty string for undated
    local rrule="$5"      # empty string for non-recurring
    local due_date="${6:-}"   # empty string for no due date

    local frontmatter="---
tldr: \"$tldr\"
status: todo
priority: $priority
created: '$now_ts'
updated: '$now_ts'"

    if [[ -n "$date" ]]; then
        frontmatter+="
tentative_date: '$date'"
    fi

    if [[ -n "$due_date" ]]; then
        frontmatter+="
due_date: '$due_date'"
    fi

    if [[ -n "$rrule" ]]; then
        frontmatter+="
rrule: '$rrule'"
    fi

    frontmatter+="
---
"
    mkdir -p "$(dirname "$file")"
    echo "$frontmatter" > "$file"
}

# --- Repo: work ---

WORK="$GRAFDATA/work"
mkdir -p "$WORK/todo"

git init "$WORK" --quiet
git -C "$WORK" config user.email "test@test.com"
git -C "$WORK" config user.name "Test"

create_todo "$WORK/todo/quarterly-review.md"  "Quarterly review prep"       1 "$overdue"  "" "$overdue"
create_todo "$WORK/todo/deploy-api.md"        "Deploy API to staging"       2 "$today"    ""
create_todo "$WORK/todo/write-tests.md"       "Write integration tests"     3 "$tomorrow" ""
create_todo "$WORK/todo/design-review.md"     "Design review for auth"      2 "$in3days"  ""
create_todo "$WORK/todo/sprint-planning.md"   "Sprint planning"             3 "$in10days" ""
create_todo "$WORK/todo/tech-debt.md"         "Address tech debt backlog"   4 ""          ""
create_todo "$WORK/todo/standup.md"           "Daily standup"               2 "$today"    "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR"

git -C "$WORK" add .
git -C "$WORK" commit -m "initial test data" --quiet

graf manifest init --id "test.dev/work" --slug "work" --repo "$WORK"
graf manifest add "$WORK"

echo "  Created work repo: 7 tasks"

# --- Repo: life ---

LIFE="$GRAFDATA/life"
mkdir -p "$LIFE/todo"

git init "$LIFE" --quiet
git -C "$LIFE" config user.email "test@test.com"
git -C "$LIFE" config user.name "Test"

create_todo "$LIFE/todo/dentist.md"    "Call dentist"              1 "$in2days"  ""
create_todo "$LIFE/todo/groceries.md"  "Buy groceries"             3 "$today"    ""
create_todo "$LIFE/todo/read-book.md"  "Finish reading Dune"       4 ""          ""

git -C "$LIFE" add .
git -C "$LIFE" commit -m "initial test data" --quiet

graf manifest init --id "test.dev/life" --slug "life" --repo "$LIFE"
graf manifest add "$LIFE"

echo "  Created life repo: 3 tasks"

# --- Validate ---

echo ""
echo "Validating manifest..."
graf manifest check --json
echo ""
echo "Querying todos..."
graf todo --json | python3 -c "
import sys, json
data = json.load(sys.stdin)
tasks = data.get('tasks', [])
print(f'  {len(tasks)} tasks across {len(set(t.get(\"repo\",\"?\") for t in tasks))} repos')
for t in tasks:
    date = t.get('effective_date', 'undated')
    repo = t.get('repo', '?')
    print(f'    P{t[\"priority\"]}  {date:12s}  [{repo}]  {t[\"tldr\"]}')
"

echo ""
echo "Done. Manifest at: $MANIFEST"
echo "Set GRAF_MANIFEST=$MANIFEST when running brenn."
