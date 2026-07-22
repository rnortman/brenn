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

## Destination-based gating

The write-time `hook` decides which repo a write belongs to from the write
*destination*, not from the session's working directory. A destination inside a
gated repo — a git repo with a `.gitleaks.toml` at its root — is scanned against
that repo's config, at its repo-relative path. A destination outside any gated
repo (no repo at all, or a repo without the file) is not the gate's concern: it
exits 0 with an audit line on stderr and is not scanned. The commit and push
gates still scan everything that lands in a gated repo, so nothing reaches a
remote unscanned.

This makes a single hook registration safe from any session: it no-ops on
writes outside gated repos and scans writes into gated ones against the correct
config, even when the session is rooted somewhere else entirely.

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
