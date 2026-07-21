//! brenn-scrub -- the one scrub entry point for every layer.
//!
//! Modes: `hook`, `staged`, `range`, `tree` -- see the `help` module for each
//! mode's input contract, scope, and exit codes.
//!
//! Diff-scanning modes enforce every rule. Tree-scanning modes drop the
//! diff-only rules (see gitleaks::DIFF_ONLY_RULES) so a grandfathered backlog
//! cannot make them permanently un-green-able.

mod config;
mod exclude;
mod exempt;
mod git;
mod gitleaks;
mod help;
mod hook;
mod message;
mod mirror;

use exclude::Exclusions;
use gitleaks::{Finding, Version};
use mirror::Mirror;
use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// PreToolUse blocks the write and feeds stderr back to the agent on exit 2.
const EXIT_BLOCK: u8 = 2;
const EXIT_FAIL: u8 = 1;
const EXIT_OK: u8 = 0;

const INSTALL_HINT: &str = "gitleaks not found on PATH. Install the pinned release \
    (see `make setup-hooks`) so scrub checks can run.";

/// Strict reader for a scrub file-discovery env var. An empty value is
/// deliberately *not* treated as unset: setting the var at all declares its
/// file required, so `VAR=""` (an unset variable expanded in a shell profile)
/// must reach the downstream discovery panic rather than silently degrade the
/// gate. A non-UTF-8 value violates the same contract and panics here for the
/// same reason. `declares` names the required file in the panic message.
fn strict_env(var: &str, declares: &str) -> Option<String> {
    std::env::var_os(var).map(|value| {
        value.into_string().unwrap_or_else(|raw| {
            panic!(
                "{var} is set to a non-UTF-8 value ({raw:?}); setting it declares the \
                 {declares} required, so it cannot be treated as unset"
            )
        })
    })
}

/// An empty `BRENN_SCRUB_DENYLIST` must reach the discovery panic rather than
/// silently degrade the gate to public-only rules.
fn overlay_env() -> Option<String> {
    strict_env(config::OVERLAY_ENV, "overlay")
}

fn resolve_for(repo: &Path) -> config::Resolved {
    config::resolve(repo, overlay_env().as_deref())
}

/// An empty `BRENN_SCRUB_WRITE_EXEMPT` reaches `exempt::load`, which blocks on
/// the missing target rather than silently degrading.
fn exempt_env() -> Option<String> {
    strict_env(exempt::EXEMPT_ENV, "write-exemption file")
}

/// Version handling for the gates: any deviation from the pin is fatal, since
/// a gate silently scanning with an unvalidated engine is worse than no gate.
fn require_gitleaks() -> Result<(), String> {
    match Version::detect() {
        Version::Match => Ok(()),
        Version::Mismatch(found) => Err(Version::mismatch_message(&found)),
        Version::Missing => Err(INSTALL_HINT.to_string()),
    }
}

/// Shared opening of every gating mode: require a pinned gitleaks, locate the
/// repo, resolve the config, and emit the one info line naming what was loaded.
/// One copy, so the info line's shape and the version policy cannot drift
/// between modes.
fn gate_preamble(mode: &str) -> Result<(PathBuf, config::Resolved), ExitCode> {
    if let Err(msg) = require_gitleaks() {
        eprintln!("brenn-scrub: {msg}");
        return Err(ExitCode::from(EXIT_FAIL));
    }
    let repo = git::repo_root(&std::env::current_dir().expect("cannot read cwd"));
    let resolved = resolve_for(&repo);
    eprintln!("brenn-scrub {mode}: {}", resolved.summary());
    Ok((repo, resolved))
}

fn scan_mirror(resolved: &config::Resolved, mirror: &Mirror) -> Vec<Finding> {
    let raw = gitleaks::scan_dir(&resolved.config_path, mirror.root());
    gitleaks::relativize(raw, mirror.root())
}

fn report(findings: &[Finding], heading: &str) {
    eprintln!("{}", message::rejection(findings, heading));
}

/// `hook` and `range` consume a protocol produced by Claude Code and by git.
/// A terminal on stdin means neither producer is there, and reading would just
/// block with no output -- so say what was expected instead of hanging.
fn terminal_stdin_message(verb: &str) -> String {
    format!(
        "brenn-scrub {verb}: stdin is a terminal, but this mode reads a protocol on stdin \
         and is meant to be run by a hook, not by hand. Run `brenn-scrub {verb} --help` for \
         the exact format, or use `staged` / `tree` for hand-run scans."
    )
}

fn refuse_terminal_stdin(verb: &str) -> bool {
    if !std::io::stdin().is_terminal() {
        return false;
    }
    eprintln!("{}", terminal_stdin_message(verb));
    true
}

fn misconfigured_hook_message(tool: &str) -> String {
    format!(
        "brenn-scrub: hook misconfigured -- tool `{tool}` reached the scrub check but \
         this wrapper cannot read its added text. Either narrow the PreToolUse matcher \
         or teach brenn-scrub about `{tool}`. Refusing to pass it unscanned."
    )
}

fn size_cap_message(path: &Path, len: usize) -> String {
    format!(
        "brenn-scrub: skipping scan of {} ({len} bytes exceeds the {} byte cap); \
         the push gate scans it uncapped.",
        path.display(),
        hook::SIZE_CAP_BYTES
    )
}

/// The one line an exempt-matched write prints before exiting 0. Names the
/// resolved destination, the matched root, and the file the roots came from,
/// so the opt-out is auditable. Local-only (a passing hook's stderr ships
/// nowhere), but it carries the same neutrality bar as every emitted string.
fn exempt_audit_message(dest: &Path, root: &Path, source: &Path) -> String {
    format!(
        "brenn-scrub hook: write to {} is exempt from the write-time scrub\n  \
         (matched {} from {})",
        dest.display(),
        root.display(),
        source.display()
    )
}

// ---------------------------------------------------------------- hook mode

/// Hook mode fails *closed*.
///
/// Only exit 2 blocks a PreToolUse write; a panic exits 101, which Claude
/// Code reports as a hook error and lets the write through. Every internal
/// failure here -- broken overlay, gitleaks malfunction, unreadable report --
/// is therefore translated into a block. The gates keep panicking, where a
/// nonzero exit already means "refused".
fn mode_hook() -> ExitCode {
    if refuse_terminal_stdin("hook") {
        return ExitCode::from(EXIT_FAIL);
    }
    let hushed = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(mode_hook_inner);
    std::panic::set_hook(hushed);

    match result {
        Ok(code) => code,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("unknown internal error");
            eprintln!("brenn-scrub: internal error, refusing to pass this write unscanned: {msg}");
            ExitCode::from(EXIT_BLOCK)
        }
    }
}

fn mode_hook_inner() -> ExitCode {
    let mut raw = String::new();
    std::io::stdin()
        .read_to_string(&mut raw)
        .expect("cannot read hook input");
    let payload: serde_json::Value =
        serde_json::from_str(&raw).expect("hook input is not valid JSON");

    let added = match hook::extract(&payload) {
        Ok(a) => a,
        Err(hook::HookError::UnknownTool(tool)) => {
            eprintln!("{}", misconfigured_hook_message(&tool));
            return ExitCode::from(EXIT_BLOCK);
        }
    };

    let cwd = payload
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("cannot read cwd"));

    // The exemption is judged on the write destination and runs before every
    // other step: an exempt destination legitimately has no `.gitleaks.toml`
    // and needs no scanner, so it must precede repo/config resolution, the
    // version probe, and the size cap. A load or match panic becomes a block,
    // per the hook's fail-closed contract.
    if let Some(exempt) = exempt::load(exempt_env().as_deref())
        && let Some(root) = exempt.matched_root(&added.file_path, &cwd)
    {
        eprintln!(
            "{}",
            exempt_audit_message(&added.file_path, root, exempt.source())
        );
        return ExitCode::from(EXIT_OK);
    }

    // Missing or unpinned gitleaks must not block an author mid-edit; the
    // gates downstream are hard failures and will catch anything missed here.
    match Version::detect() {
        Version::Match => {}
        Version::Mismatch(found) => eprintln!("brenn-scrub: {}", Version::mismatch_message(&found)),
        Version::Missing => {
            eprintln!("brenn-scrub: {INSTALL_HINT}");
            return ExitCode::from(EXIT_FAIL);
        }
    }

    if added.text.len() > hook::SIZE_CAP_BYTES {
        eprintln!("{}", size_cap_message(&added.file_path, added.text.len()));
        return ExitCode::from(EXIT_OK);
    }

    let repo = git::repo_root(&cwd);

    let mirror = Mirror::new();
    mirror.write(
        &mirror::repo_relative(&added.file_path, &repo),
        added.text.as_bytes(),
    );
    let findings = scan_mirror(&resolve_for(&repo), &mirror);

    if findings.is_empty() {
        return ExitCode::from(EXIT_OK);
    }
    report(
        &findings,
        "brenn-scrub blocked this write: the added text matches a scrub rule.",
    );
    ExitCode::from(EXIT_BLOCK)
}

// -------------------------------------------------------------- staged mode

fn mode_staged() -> ExitCode {
    let (repo, resolved) = match gate_preamble("staged") {
        Ok(v) => v,
        Err(code) => return code,
    };

    // Path identity comes from `--name-only`, never from diff headers, so
    // staged content cannot spoof the file a hunk is attributed to.
    let mirror = Mirror::new();
    let mut any = false;
    for path in git::staged_files(&repo) {
        let added = git::added_lines(&git::staged_diff_for(&repo, &path));
        if added.is_empty() {
            continue;
        }
        mirror.write(&path, added.as_bytes());
        any = true;
    }
    if !any {
        return ExitCode::from(EXIT_OK);
    }

    let findings = scan_mirror(&resolved, &mirror);
    if findings.is_empty() {
        return ExitCode::from(EXIT_OK);
    }
    report(
        &findings,
        "brenn-scrub blocked this commit: staged changes match a scrub rule.",
    );
    ExitCode::from(EXIT_FAIL)
}

// --------------------------------------------------------------- range mode

fn mode_range(warn_only: bool) -> ExitCode {
    if refuse_terminal_stdin("range") {
        return ExitCode::from(EXIT_FAIL);
    }
    let (repo, resolved) = match gate_preamble("range") {
        Ok(v) => v,
        Err(code) => return code,
    };

    let mut raw = String::new();
    std::io::stdin()
        .read_to_string(&mut raw)
        .expect("cannot read pre-push input");

    let mut findings = Vec::new();
    let mut scanned_tips: Vec<String> = Vec::new();
    for update in git::parse_push_refs(&raw) {
        let Some(log_opts) = git::log_opts_for(&repo, &update) else {
            continue;
        };
        // Per-commit diffs: the full rule set, diff-only rules included.
        findings.extend(gitleaks::scan_git(&resolved.config_path, &repo, &log_opts));

        // Tip tree: what the remote ends up holding, tree rule set.
        let tip = match &update {
            git::RefUpdate::New { local } => local,
            git::RefUpdate::Update { local, .. } => local,
            git::RefUpdate::Delete => continue,
        };
        // Pushing several refs at one tip (a branch and its tag) would
        // otherwise report every finding once per ref and inflate the
        // burndown count.
        if scanned_tips.iter().any(|t| t == tip) {
            continue;
        }
        scanned_tips.push(tip.clone());

        let mirror = Mirror::new();
        git::extract_tree(&repo, tip, mirror.root());
        findings.extend(gitleaks::apply_tree_filter(scan_mirror(&resolved, &mirror)));
    }

    if findings.is_empty() {
        return ExitCode::from(EXIT_OK);
    }
    if warn_only {
        report(
            &findings,
            "brenn-scrub (warn-only): this push would fail the scrub gate.",
        );
        return ExitCode::from(EXIT_OK);
    }
    report(
        &findings,
        "brenn-scrub blocked this push: pushed commits match a scrub rule.",
    );
    ExitCode::from(EXIT_FAIL)
}

// ---------------------------------------------------------------- tree mode

/// Tree mode's stdout: the findings the exit code was decided on, together
/// with the scope that produced them, so a captured worklist can never be read
/// as a scan of more than it was.
#[derive(serde::Serialize)]
struct TreeOutput<'a> {
    findings: &'a [Finding],
    excluded: Vec<String>,
}

fn mode_tree(scope: Option<&str>, exclusions: &Exclusions) -> ExitCode {
    let (repo, resolved) = match gate_preamble("tree") {
        Ok(v) => v,
        Err(code) => return code,
    };

    let tracked = git::tracked_files(&repo, scope);
    // A scan of zero files is never a meaningful green -- it is a mistyped
    // scope.
    assert!(
        !tracked.is_empty(),
        "scope matched no tracked files{}; refusing to report a clean tree",
        scope.map(|s| format!(" ({s})")).unwrap_or_default()
    );

    let (tracked, dropped) = exclusions.partition(tracked);
    for (prefix, count) in &dropped {
        eprintln!("EXCLUDED: {prefix} ({count} files not scanned)");
    }
    assert!(
        !tracked.is_empty(),
        "every tracked file in scope was excluded; refusing to report a clean tree"
    );

    // Anything tracked but unmirrored narrows the scan while it still reports
    // a clean tree, so the only tolerated case -- a staged deletion, which has
    // no content -- is enumerated from git rather than inferred from a
    // stat failure, and it is announced.
    let deleted = git::deleted_files(&repo);
    let mirror = Mirror::new();
    for rel in tracked {
        let src = repo.join(&rel);
        match std::fs::symlink_metadata(&src) {
            Ok(md) if md.is_file() => mirror.link_or_copy(&rel, &src),
            // git stores a symlink as a blob holding its target path and scans
            // that text; mirroring the target string keeps the tree scan
            // seeing what a history scan sees.
            Ok(md) if md.is_symlink() => {
                let target = std::fs::read_link(&src)
                    .unwrap_or_else(|e| panic!("cannot read symlink {}: {e}", src.display()));
                mirror.write(&rel, target.as_os_str().as_encoded_bytes());
            }
            Ok(_) => panic!(
                "tracked path {} is neither a regular file nor a symlink; \
                 refusing to report a clean tree with it unscanned",
                src.display()
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                assert!(
                    deleted.contains(&rel),
                    "tracked path {} is missing from the worktree but is not a staged \
                     deletion; refusing to report a clean tree with it unscanned",
                    src.display()
                );
                eprintln!("SKIPPED: {} (staged deletion, no content)", rel.display());
            }
            Err(e) => panic!("cannot stat tracked path {}: {e}", src.display()),
        }
    }

    let findings = gitleaks::apply_tree_filter(scan_mirror(&resolved, &mirror));

    let output = TreeOutput {
        findings: &findings,
        excluded: exclusions.as_strings(),
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&output).expect("cannot serialize findings")
    );

    if findings.is_empty() {
        eprintln!("brenn-scrub tree: clean");
        return ExitCode::from(EXIT_OK);
    }
    report(
        &findings,
        &format!("brenn-scrub tree: {} finding(s).", findings.len()),
    );
    ExitCode::from(EXIT_FAIL)
}

// -------------------------------------------------------------------- entry

/// Bad invocation: the mode's own help on stderr, nonzero exit.
///
/// A botched *hook* invocation blocks rather than warns. Exit 1 from the hook
/// path means "write landed unscanned", so a stray argument in the PreToolUse
/// command string would silently disable the write-time layer everywhere that
/// config is pulled; the rest of hook mode fails closed and so does this.
fn usage(verb: Option<&str>) -> ExitCode {
    eprintln!("{}", help::for_mode(verb));
    match verb {
        Some("hook") => ExitCode::from(EXIT_BLOCK),
        _ => ExitCode::from(EXIT_FAIL),
    }
}

/// What the command line asked for.
///
/// Unrecognized arguments are rejected rather than ignored: a mistyped
/// `--warnonly` would silently run the push gate *enforcing*, and a stray
/// flag in tree mode would be read as a git pathspec that matches nothing and
/// reports a clean tree.
#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Hook,
    Staged,
    Range {
        warn_only: bool,
    },
    Tree {
        scope: Option<String>,
        exclude: Vec<String>,
    },
    /// Help was asked for: the mode's text on stdout, exit 0.
    Help(Option<String>),
    /// Bad invocation: the mode's text on stderr, nonzero.
    Usage(Option<String>),
}

fn wants_help(args: &[String]) -> bool {
    args.iter().any(|a| a == "--help" || a == "-h")
}

fn parse_tree(rest: &[String]) -> Mode {
    let mut scope: Option<String> = None;
    let mut exclude = Vec::new();
    let mut it = rest.iter();
    while let Some(arg) = it.next() {
        if arg == "--exclude" {
            match it.next() {
                Some(p) if !p.starts_with('-') && !p.is_empty() => exclude.push(p.clone()),
                _ => return Mode::Usage(Some("tree".into())),
            }
        } else if arg.starts_with('-') || scope.is_some() {
            return Mode::Usage(Some("tree".into()));
        } else {
            scope = Some(arg.clone());
        }
    }
    Mode::Tree { scope, exclude }
}

fn dispatch(args: &[String]) -> Mode {
    let verb = args.first().map(String::as_str);
    let rest = args.get(1..).unwrap_or(&[]);
    if wants_help(args) {
        return Mode::Help(verb.filter(|v| !v.starts_with('-')).map(str::to_string));
    }
    let named = || verb.map(str::to_string);
    match verb {
        Some("hook") if rest.is_empty() => Mode::Hook,
        Some("staged") if rest.is_empty() => Mode::Staged,
        Some("range") => match rest {
            [] => Mode::Range { warn_only: false },
            [one] if one == "--warn-only" => Mode::Range { warn_only: true },
            _ => Mode::Usage(named()),
        },
        Some("tree") => parse_tree(rest),
        Some("hook") | Some("staged") => Mode::Usage(named()),
        _ => Mode::Usage(None),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(&args) {
        Mode::Hook => mode_hook(),
        Mode::Staged => mode_staged(),
        Mode::Range { warn_only } => mode_range(warn_only),
        Mode::Tree { scope, exclude } => mode_tree(
            scope.as_deref(),
            &Exclusions::new(exclude.into_iter().map(PathBuf::from).collect()),
        ),
        Mode::Help(verb) => {
            println!("{}", help::for_mode(verb.as_deref()));
            ExitCode::from(EXIT_OK)
        }
        Mode::Usage(verb) => usage(verb.as_deref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispatch_of(args: &[&str]) -> Mode {
        let owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        dispatch(&owned)
    }

    #[test]
    fn each_mode_is_selected_by_its_verb() {
        assert_eq!(dispatch_of(&["hook"]), Mode::Hook);
        assert_eq!(dispatch_of(&["staged"]), Mode::Staged);
        assert_eq!(dispatch_of(&["range"]), Mode::Range { warn_only: false });
        assert_eq!(
            dispatch_of(&["tree"]),
            Mode::Tree {
                scope: None,
                exclude: vec![]
            }
        );
    }

    #[test]
    fn warn_only_is_opt_in_and_exact() {
        assert_eq!(
            dispatch_of(&["range", "--warn-only"]),
            Mode::Range { warn_only: true }
        );
        // A typo must not silently enforce; enforcing is the destructive
        // direction during the warn-only rollout.
        assert_eq!(
            dispatch_of(&["range", "--warnonly"]),
            Mode::Usage(Some("range".into()))
        );
        assert_eq!(
            dispatch_of(&["range", "--warn-only", "x"]),
            Mode::Usage(Some("range".into()))
        );
    }

    #[test]
    fn tree_takes_a_scope_but_rejects_unknown_flags() {
        assert_eq!(
            dispatch_of(&["tree", "src"]),
            Mode::Tree {
                scope: Some("src".into()),
                exclude: vec![]
            }
        );
        // Would otherwise become a pathspec matching nothing -> false green.
        assert_eq!(
            dispatch_of(&["tree", "--anything"]),
            Mode::Usage(Some("tree".into()))
        );
        assert_eq!(
            dispatch_of(&["tree", "src", "extra"]),
            Mode::Usage(Some("tree".into()))
        );
    }

    #[test]
    fn tree_collects_repeated_exclusions_alongside_a_scope() {
        assert_eq!(
            dispatch_of(&[
                "tree",
                "--exclude",
                "docs/adr",
                "--exclude",
                "brenn-prod.toml"
            ]),
            Mode::Tree {
                scope: None,
                exclude: vec!["docs/adr".into(), "brenn-prod.toml".into()]
            }
        );
        assert_eq!(
            dispatch_of(&["tree", "--exclude", "docs/adr", "docs"]),
            Mode::Tree {
                scope: Some("docs".into()),
                exclude: vec!["docs/adr".into()]
            }
        );
    }

    /// A swallowed flag would silently become the excluded prefix, quietly
    /// widening the scan's blind spot.
    #[test]
    fn exclude_demands_a_value_that_is_not_another_flag() {
        assert_eq!(
            dispatch_of(&["tree", "--exclude"]),
            Mode::Usage(Some("tree".into()))
        );
        assert_eq!(
            dispatch_of(&["tree", "--exclude", "--warn-only"]),
            Mode::Usage(Some("tree".into()))
        );
    }

    #[test]
    fn help_is_available_bare_and_per_mode() {
        assert_eq!(dispatch_of(&["--help"]), Mode::Help(None));
        assert_eq!(
            dispatch_of(&["tree", "--help"]),
            Mode::Help(Some("tree".into()))
        );
        assert_eq!(
            dispatch_of(&["range", "-h"]),
            Mode::Help(Some("range".into()))
        );
    }

    /// These are the strings an author is most likely to paste into a public
    /// issue, so they carry the same neutrality bar as the rejection ladder.
    #[test]
    fn every_message_this_module_emits_is_neutral() {
        use message::neutral::assert_neutral;
        assert_neutral(INSTALL_HINT, "install hint");
        for verb in ["hook", "range"] {
            assert_neutral(&terminal_stdin_message(verb), "terminal-stdin refusal");
        }
        assert_neutral(
            &misconfigured_hook_message("SomeFutureTool"),
            "hook-misconfigured message",
        );
        assert_neutral(
            &size_cap_message(Path::new("src/a.rs"), 1),
            "size-cap message",
        );
        assert_neutral(
            &exempt_audit_message(
                Path::new("src/a.rs"),
                Path::new("/srv/data/annex"),
                Path::new("/etc/scrub/exempt.toml"),
            ),
            "exempt audit line",
        );
    }

    /// The guard itself needs a pty to exercise and is deliberately left to
    /// manual use; its failure mode is a hang, not a scrub hole. What must not
    /// rot is the message, which is the only thing that tells a human why the
    /// command they typed by hand refused to run.
    #[test]
    fn terminal_stdin_refusal_names_the_producer_and_the_way_out() {
        for verb in ["hook", "range"] {
            let msg = terminal_stdin_message(verb);
            assert!(msg.contains("stdin is a terminal"), "{msg}");
            assert!(
                msg.contains(&format!("brenn-scrub {verb} --help")),
                "must point at the mode's own help: {msg}"
            );
            assert!(
                msg.contains("staged") && msg.contains("tree"),
                "must name the hand-runnable modes: {msg}"
            );
        }
    }

    #[test]
    fn stray_arguments_and_unknown_verbs_are_rejected() {
        assert_eq!(dispatch_of(&[]), Mode::Usage(None));
        assert_eq!(
            dispatch_of(&["hook", "--x"]),
            Mode::Usage(Some("hook".into()))
        );
        assert_eq!(
            dispatch_of(&["staged", "extra"]),
            Mode::Usage(Some("staged".into()))
        );
        assert_eq!(dispatch_of(&["nonsense"]), Mode::Usage(None));
    }
}
