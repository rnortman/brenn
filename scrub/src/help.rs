//! Usage text.
//!
//! Each mode documents its stdin contract explicitly, because two of the four
//! consume a protocol defined by someone else and the other two consume
//! nothing at all -- a distinction that is invisible from a one-line summary
//! and that a caller cannot guess.

pub const UMBRELLA: &str = "\
usage: brenn-scrub <mode> [options]

  hook                       write-time check; invoked by a Claude Code
                             PreToolUse hook
  staged                     scan the staged diff; invoked by .githooks/pre-commit
  range [--warn-only]        scan a pushed range and tip tree; invoked by
                             .githooks/pre-push
  tree [<path>] [--exclude <prefix>]...
                             scan tracked files; run by hand

stdin: `hook` and `range` read a protocol on stdin (see their --help).
`staged` and `tree` read nothing -- run them bare inside a repo.

`brenn-scrub <mode> --help` documents one mode in full.";

pub const HOOK: &str = "\
usage: brenn-scrub hook

Scans the text a Write or Edit tool call is about to add, before it lands.

stdin: the Claude Code PreToolUse event JSON, delivered by Claude Code itself.
       Fields read: tool_name, tool_input (content for Write, new_string for
       Edit, plus file_path), cwd. Only added text is scanned, so existing
       content in the target file is never re-flagged.
scans: the added text only, at the target's repo-relative path.
exits: 0 clean; 2 blocks the write and shows stderr to the agent; 1 when
       gitleaks is missing (non-blocking warning).
gating: the write destination decides the repo, not the current directory. A
       destination inside a gated repo -- a git repo with a .gitleaks.toml at
       its root -- is scanned against that repo's config. A destination outside
       any gated repo (no repo, or a repo without the file) is not this gate's
       business and exits 0 with an audit line naming the destination; the
       commit and push gates still scan everything that lands in a gated repo.
called by: the PreToolUse stanza in .claude/settings.json. Not a human-facing
       command -- there is no supported way to type its input by hand.";

pub const STAGED: &str = "\
usage: brenn-scrub staged

Scans the added lines of the staged diff.

stdin: nothing -- do not pipe input. Piped input is ignored entirely.
scans: added lines of `git diff --cached`, per file, at their repo paths.
exits: 0 clean; 1 on findings or when gitleaks is missing or off-pin.
called by: .githooks/pre-commit, before `make check`, so a scrub failure
       surfaces in seconds. Safe to run by hand in a repo with staged changes.";

pub const RANGE: &str = "\
usage: brenn-scrub range [--warn-only]

Scans everything a push would send: every commit's diff in the pushed range,
plus the tree at each tip.

stdin: the git pre-push contract, one line per ref (githooks(5)):

           <local ref> <local sha> <remote ref> <remote sha>

       for example:

           refs/heads/main a1b2c3 refs/heads/main d4e5f6

       An all-zero remote sha means a new ref (scanned back to the merge-base
       with the default branch); an all-zero local sha means a deletion
       (nothing to scan).
scans: per-commit diffs with every rule; tip trees with the tree rule set.
exits: 0 clean; 1 on findings. With --warn-only, findings are reported and the
       exit is 0 -- the pre-green rollout state.
called by: .githooks/pre-push. Not a human-facing command -- there is no
       supported way to type its input by hand.";

pub const TREE: &str = "\
usage: brenn-scrub tree [<path>] [--exclude <prefix>]...

Scans every tracked file. This is the burndown command, and the one a green
tree is declared on.

stdin: nothing -- do not pipe input. Piped input is ignored entirely.
scans: tracked files only (never untracked or ignored ones), from the working
       tree, so uncommitted fixes count. Diff-only rules are dropped, since a
       grandfathered backlog would otherwise make the scan permanently red.
<path>: restrict to a subtree.
--exclude <prefix>: skip a repo-relative prefix; repeatable. Matching is on
       whole path components. Excluded files are never scanned and never
       reported, each exclusion is named on stderr, and a prefix matching no
       tracked file in scope is a hard error.
stdout: a JSON object {\"findings\": [...], \"excluded\": [...]} -- exactly the
       findings the exit code was decided on, plus the scope that produced
       them. Capture it as a worklist.
exits: 0 when findings is empty; 1 otherwise, or when gitleaks is missing or
       off-pin.
called by: a human.";

/// Help for a mode verb, or the umbrella text.
pub fn for_mode(verb: Option<&str>) -> &'static str {
    match verb {
        Some("hook") => HOOK,
        Some("staged") => STAGED,
        Some("range") => RANGE,
        Some("tree") => TREE,
        _ => UMBRELLA,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_mode_help_states_its_stdin_contract() {
        assert!(HOOK.contains("stdin: the Claude Code PreToolUse event JSON"));
        assert!(RANGE.contains("<local ref> <local sha> <remote ref> <remote sha>"));
        assert!(STAGED.contains("stdin: nothing -- do not pipe input"));
        assert!(TREE.contains("stdin: nothing -- do not pipe input"));
    }

    #[test]
    fn hook_help_documents_destination_gating() {
        assert!(HOOK.contains("gated repo"));
        assert!(HOOK.contains(".gitleaks.toml"));
        assert!(HOOK.contains("decides the repo"));
        // Mechanism only -- the help must not name what the gate guards.
        crate::message::neutral::assert_neutral(HOOK, "hook gating text");
    }

    #[test]
    fn every_mode_help_names_its_caller_and_exit_codes() {
        for text in [HOOK, STAGED, RANGE, TREE] {
            assert!(text.contains("called by:"), "missing caller: {text}");
            assert!(text.contains("exits:"), "missing exit codes: {text}");
        }
    }

    #[test]
    fn umbrella_lists_all_four_modes() {
        for verb in ["hook", "staged", "range", "tree"] {
            assert!(UMBRELLA.contains(verb));
            assert_eq!(for_mode(Some(verb)), super::for_mode(Some(verb)));
            assert_ne!(for_mode(Some(verb)), UMBRELLA);
        }
        assert_eq!(for_mode(None), UMBRELLA);
    }

    /// `--help` is the most-pasted output the binary has; it carries the same
    /// neutrality bar as the rejection ladder.
    #[test]
    fn every_help_text_is_neutral() {
        for (name, text) in [
            ("umbrella", UMBRELLA),
            ("hook", HOOK),
            ("staged", STAGED),
            ("range", RANGE),
            ("tree", TREE),
        ] {
            crate::message::neutral::assert_neutral(text, name);
        }
    }

    #[test]
    fn tree_help_documents_exclusion_and_the_stdout_object() {
        assert!(TREE.contains("--exclude <prefix>"));
        assert!(TREE.contains("hard error"));
        assert!(TREE.contains("\"findings\""));
        assert!(TREE.contains("\"excluded\""));
    }
}
