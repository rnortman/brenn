# Scrub template for other repos

Everything a repo needs to join the scrub gate. All of it is shims around the
installed `brenn-scrub` binary; no scanning logic is duplicated per repo.

Copy into the target repo:

| From here | To there |
|---|---|
| `gitleaks.toml` | `.gitleaks.toml` |
| `claude-hooks/scrub-check.sh`, `claude-hooks/format.sh` | `.claude/hooks/` |
| `githooks/pre-commit`, `githooks/pre-push` | `.githooks/` |
| `setup-hooks.sh` | repo root (only if the repo has no Makefile) |

Then:

1. Add `.gitleaks.local.toml` to the repo's `.gitignore`. That is the standard
   location for the optional site-local rule overlay; without one, scans run the
   public rules only, silently.
2. Merge the `hooks` stanza below into `.claude/settings.json`.
3. Run `./setup-hooks.sh` (or `make setup-hooks`) to point git at `.githooks`.
4. `chmod +x` every copied script.

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "\"$CLAUDE_PROJECT_DIR\"/.claude/hooks/scrub-check.sh"
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "\"$CLAUDE_PROJECT_DIR\"/.claude/hooks/format.sh"
          }
        ]
      }
    ]
  }
}
```

## Write-destination exemptions

Some write destinations sit outside any gated repo and should skip the
write-time hook entirely — a deliberately ungated checkout an agent writes into,
for instance. Name them in a TOML file and point `BRENN_SCRUB_WRITE_EXEMPT` at
it:

```toml
# Absolute write destinations exempt from the write-time scrub.
paths = ["/absolute/path/to/checkout"]
```

A `hook`-mode write whose resolved destination lies under one of those paths
exits 0 with an audit line on stderr; every other write is scanned as usual.
This is hook-mode only — `staged`, `range`, and `tree` never honor it, so the
commit and push gates still see everything.

Setting the variable declares the file required: a missing, unreadable, or
malformed file (bad TOML, an empty `paths`, a relative or nonexistent entry, an
entry inside a gated repo, or one broad enough to be a typo) blocks every write
until it is fixed. Entries must be absolute and must resolve on disk. The file
is discovered only through the variable — **never commit it to a repo**; keep it
wherever the site's other private scrub config lives, and export the variable
from the same place.

## Running it by hand

Two modes are meant for a human, and neither reads stdin — run them bare
inside the repo:

- `brenn-scrub staged` — scan what is staged right now. Same check the
  pre-commit hook runs.
- `brenn-scrub tree` — scan every tracked file. This is the burndown command,
  and the one a green repo is declared on. `brenn-scrub tree <path>` narrows it
  to a subtree; `--exclude <prefix>` (repeatable) skips a repo-relative prefix,
  names each exclusion on stderr, and fails outright if a prefix matches no
  tracked file. Its stdout is a JSON object of the findings plus the exclusions
  that produced them — capture it as a worklist.

`brenn-scrub hook` and `brenn-scrub range` are not human-facing: they consume
protocols produced by Claude Code and by git respectively, and refuse to run
with a terminal on stdin. `brenn-scrub <mode> --help` documents any mode's
input, scope, and exit codes.

`pre-push` is enforcing: a finding exits non-zero and blocks the push. Get
`brenn-scrub tree` green on the repo before activating the hooks, or pushes
will be blocked until the tree is clean.

The per-repo `.gitleaks.toml` may diverge in its path allowlists. It must not
diverge in rules — those belong upstream in this template.

In brenn itself the live copies must match this template byte-for-byte
(`.gitleaks.toml`, both `.claude` hooks, `.githooks/pre-push`);
`scrub/selfcheck.sh` runs under `make check` and fails on drift.
