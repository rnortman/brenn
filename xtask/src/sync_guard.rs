//! Sync guard: cross-file version pins that no compiler checks.
//!
//! `MODULE.bazel` is what builds; `rust-toolchain.toml` is what the editor,
//! rustfmt, and rust-analyzer read. Two files naming the same rustc, and
//! nothing but this guard to say when a bump touched one and not the other —
//! at which point the toolchain that gates the tree and the toolchain the
//! author sees diverge silently.
//!
//! The Rust edition is the same shape of pin: cargo compiles a crate at the
//! edition in its `Cargo.toml`, Bazel at the one in its `BUILD.bazel`, and two
//! green lanes can be compiling the same sources under different language
//! semantics.
//!
//! The stamp key is a third: the workspace status script emits it and the
//! `brenn` binary's `rustc_env` substitutes it, and a rename on either side
//! bakes the literal placeholder into the release binary with every gate green.
//!
//! The wasm-bindgen CLI is a fourth, and the loudest when it slips: the
//! generated JS glue and the crate that answers it are one protocol with a
//! version number, and a bundle built by the wrong CLI fails in the browser
//! rather than at the build.
//!
//! The npm trees are a fifth, and the only ones with two lockfiles each: cargo's
//! lane installs from `package-lock.json` and Bazel's resolves
//! `pnpm-lock.yaml`, so every dependency version is decided twice from one
//! manifest. A skew there means the two lanes emit browser assets from
//! different bundlers, transpilers, or framework versions while every gate
//! stays green. Node itself is the same shape: the manifest states a floor and
//! `MODULE.bazel` pins the hermetic toolchain that must satisfy it.
//!
//! The workflow's cron expressions are a sixth, and they fail by absence rather
//! than by error: each is written once under `on.schedule` and once more in the
//! `if:` of the job it selects, and a schedule edited on one side alone leaves
//! that job never running again — no failure, no annotation anyone reads.
//!
//! Its dispatch inputs are the same shape, one notch less silent: an input is
//! declared once under `on.workflow_dispatch.inputs` and read once per job that
//! selects on it, and a rename on either side leaves an operator dispatching
//! with the box checked and every selected job skipping.
//!
//! The CI disk cache is written twice too — as the `--disk_cache` of the `ci`
//! config and as the directory the workflow's cache steps carry — and a skew
//! there is the quietest failure in the file: the restore matches nothing, the
//! save banks a directory Bazel never wrote, and every run is a correct, green,
//! cold build.
//!
//! The vended gitleaks is the last: the scrub suites' harness reads its path
//! from one environment variable and `scrub/BUILD.bazel` sets that variable, and
//! the harness treats absence as "this machine has no gitleaks, skip". A rename
//! on either side leaves three suites green over no assertions at all.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The Starlark constant `MODULE.bazel` passes to `rust.toolchain(versions=)`.
const MODULE_VERSION_NAME: &str = "RUST_VERSION";

/// The Starlark constant `MODULE.bazel` pins the wasm-bindgen CLI archives to.
const MODULE_WASM_BINDGEN_NAME: &str = "WASM_BINDGEN_VERSION";

/// The `[workspace.dependencies]` key naming the crate half of that pair.
const WASM_BINDGEN_CRATE: &str = "wasm-bindgen";

/// The Starlark constant `MODULE.bazel` pins the hermetic node toolchain to.
const MODULE_NODE_NAME: &str = "NODE_VERSION";

/// The npm package the surface asset pipeline is built around.
const JCO_PACKAGE: &str = "@bytecodealliance/jco";

/// The manifest pinning it, and the two lockfiles that resolve that pin — one
/// per build system, both installed from.
const SURFACE_MANIFEST: &str = "surface/package.json";
const SURFACE_PNPM_LOCK: &str = "surface/pnpm-lock.yaml";
const SURFACE_NPM_LOCK: &str = "surface/package-lock.json";

/// The npm trees Bazel builds, each carrying both lockfiles. `e2e` is absent
/// because it drives real browsers and stays outside the build graph, so it has
/// no second lockfile to disagree with.
const NPM_TREES: [&str; 2] = ["surface", "frontend"];

/// The edition rust targets take when their `BUILD.bazel` states none. Held
/// equal to the crate macros' own defaults by `macro_default_violations` rather
/// than by this comment.
const DEFAULT_EDITION: &str = "2024";

/// The Starlark files whose macro defaults `DEFAULT_EDITION` mirrors. Every
/// macro that emits a rust target for a crate belongs here: a `BUILD.bazel`
/// that states no edition takes the default of whichever macro it calls.
const MACRO_DEFS: [&str; 2] = ["bazel/wasm/defs.bzl", "bazel/surface/defs.bzl"];

/// Below this many `BUILD.bazel`/`Cargo.toml` pairs the edition arm is not
/// clean, it is not running: the tree has forty-odd crates carrying both files,
/// and an arm that iterates a collapsed file set reports no violations exactly
/// as a healthy one does.
const MIN_EDITION_PAIRS: usize = 20;

/// The workspace-status script emitting the build-id stamp key.
const STATUS_SCRIPT: &str = "bazel/workspace_status.sh";

/// The `BUILD.bazel` whose `rustc_env` substitutes that key.
const STAMP_CONSUMER: &str = "brenn/BUILD.bazel";

/// The frontend bundler, which reads the same key out of the status file.
const STAMP_BUNDLER: &str = "frontend/esbuild-bundle-opts.mjs";

/// The gate that greps the built bundles for the unsubstituted placeholder. It
/// spells the placeholder itself, because a `BUILD.bazel` naming the build-id
/// variable is what the leaf guard forbids — so the spelling needs holding too,
/// or the gate looks for a placeholder nothing writes and passes over every
/// bundle.
const STAMP_ARTIFACT_CHECK: &str = "bazel/frontend/build_id_check.sh";

/// The Starlark constants `MODULE.bazel` pins the gitleaks release archives to.
/// Only the x86_64 checksum is held: it is the asset the workflow downloads.
const MODULE_GITLEAKS_NAME: &str = "GITLEAKS_VERSION";
const MODULE_GITLEAKS_SHA_NAME: &str = "GITLEAKS_SHA256_X86_64";

/// The wrapper whose CLI and config surface were validated against that
/// release, and the integration harness carrying its own copy of the pin (the
/// crate is a binary, so the constant is not importable from a test).
const GITLEAKS_WRAPPER: &str = "scrub/src/gitleaks.rs";
const GITLEAKS_HARNESS: &str = "scrub/tests/common/mod.rs";

/// The rust constant both of those name it under.
const GITLEAKS_CONST: &str = "PINNED_VERSION";

/// The workflow that downloads the same release for the tree scan — commit-hook
/// machinery outside the build graph, and therefore the one site nothing else
/// holds.
const CI_WORKFLOW: &str = ".github/workflows/ci.yml";
const CI_GITLEAKS_VERSION: &str = "GITLEAKS_VERSION";
const CI_GITLEAKS_SHA: &str = "GITLEAKS_SHA256";

/// Below this many cron expressions the cron arm is not clean, it is not
/// reading: two empty sets agree, and the workflow's scheduled jobs are the
/// migration's whole comparison window.
const MIN_WORKFLOW_CRONS: usize = 1;

/// Below this many dispatch inputs the input arm is not clean either: the same
/// two-empty-sets-agree shape, over the switch that turns the comparison jobs on
/// by hand.
const MIN_WORKFLOW_INPUTS: usize = 1;

/// The bazelrc config whose disk cache the workflow carries between runs, and
/// the flag naming the directory.
const BAZELRC: &str = ".bazelrc";
const CI_CONFIG: &str = "ci";
const DISK_CACHE_FLAG: &str = "disk_cache";

/// The GC cap on that cache. The workflow's cache-size step reads it out of
/// `.bazelrc` to place its warning watermark, so its spelling is a coupling
/// between the two files.
const GC_MAX_SIZE_FLAG: &str = "experimental_disk_cache_gc_max_size";

/// A cached directory is restored once and saved once, so the configured path
/// appears in the workflow at least twice. One occurrence means one half of the
/// pair moved and the other did not.
const CACHE_STEPS_PER_DIRECTORY: usize = 2;

/// The stamp key: the build-id variable under Bazel's stable-key prefix.
///
/// Assembled at runtime rather than written out, so this file does not itself
/// name the variable — the build-id guard scans it like any other source.
fn stamp_key() -> String {
    format!("STABLE_{}", crate::build_id_guard::TOKEN)
}

fn violations_from(toolchain_channel: &str, module_version: &str) -> Vec<String> {
    if toolchain_channel == module_version {
        return Vec::new();
    }
    vec![format!(
        "rust-toolchain.toml channel is {toolchain_channel:?} but MODULE.bazel \
         {MODULE_VERSION_NAME} is {module_version:?}. Bump both: the first is what the editor and \
         rustfmt run, the second is what builds and gates."
    )]
}

/// `[toolchain] channel` from a `rust-toolchain.toml`.
fn toolchain_channel(text: &str) -> String {
    let parsed: toml::Value = toml::from_str(text)
        .unwrap_or_else(|e| panic!("sync guard: malformed rust-toolchain.toml: {e}"));
    parsed
        .get("toolchain")
        .and_then(|t| t.get("channel"))
        .and_then(|c| c.as_str())
        .unwrap_or_else(|| panic!("sync guard: rust-toolchain.toml has no [toolchain] channel"))
        .to_string()
}

/// The string literal assigned to `name` at the top level of `MODULE.bazel`.
///
/// A read of the Starlark source rather than of a Bazel query: the guard runs
/// inside a Bazel test, where invoking Bazel again is not available.
fn module_pin(text: &str, name: &str) -> String {
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix('=') else {
            continue;
        };
        let value = rest.trim().trim_matches('"');
        assert!(
            !value.is_empty(),
            "sync guard: MODULE.bazel {name} has an empty value"
        );
        return value.to_string();
    }
    panic!("sync guard: MODULE.bazel has no {name} assignment");
}

/// The string literal a `const <name>: &str = "..."` states.
fn rust_str_const(rel: &str, text: &str, name: &str) -> String {
    let needle = format!("const {name}: &str =");
    for line in text.lines() {
        let Some((_, rest)) = line.split_once(&needle) else {
            continue;
        };
        let value = rest.trim().trim_end_matches(';').trim().trim_matches('"');
        assert!(
            !value.is_empty(),
            "sync guard: {rel} declares {name} with an empty value"
        );
        return value.to_string();
    }
    panic!("sync guard: {rel} declares no {name}");
}

/// The scalar a workflow states for an `env:` key.
///
/// A line read rather than a YAML parse: the guard runs inside a Bazel test
/// against one file whose shape it fully controls, and the alternative is a
/// serde_yaml dependency for two strings.
fn workflow_env_value(text: &str, key: &str) -> String {
    let needle = format!("{key}:");
    let mut found: Option<String> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix(&needle) else {
            continue;
        };
        let value = rest.trim().trim_matches('"');
        assert!(
            !value.is_empty(),
            "sync guard: {CI_WORKFLOW} states {key} with an empty value"
        );
        assert!(
            found.is_none(),
            "sync guard: {CI_WORKFLOW} states {key} more than once; the guard would hold only one"
        );
        found = Some(value.to_string());
    }
    found.unwrap_or_else(|| panic!("sync guard: {CI_WORKFLOW} states no {key}"))
}

/// Every cron expression the workflow's `on.schedule` block declares.
fn workflow_schedule_crons(text: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("- cron:") else {
            continue;
        };
        let value = rest.trim().trim_matches('"').trim_matches('\'');
        assert!(
            !value.is_empty(),
            "sync guard: {CI_WORKFLOW} declares a schedule with an empty cron expression"
        );
        found.insert(value.to_string());
    }
    found
}

/// Every cron expression a job condition selects itself on.
///
/// A workflow run fires for whichever schedule ticked, and every job in the file
/// runs unless its `if:` says otherwise — so a scheduled job identifies its own
/// tick by comparing `github.event.schedule` against the literal.
fn workflow_job_crons(text: &str) -> BTreeSet<String> {
    let needle = "github.event.schedule ==";
    let mut found = BTreeSet::new();
    for (index, _) in text.match_indices(needle) {
        let rest = text[index + needle.len()..].trim_start();
        let quoted = rest.strip_prefix('\'').unwrap_or_else(|| {
            panic!(
                "sync guard: {CI_WORKFLOW} compares {needle} against something this guard cannot \
                 read as a single-quoted literal: {:?}",
                rest.lines().next().unwrap_or_default()
            )
        });
        let (value, _) = quoted.split_once('\'').unwrap_or_else(|| {
            panic!("sync guard: {CI_WORKFLOW} has an unterminated cron literal after {needle}")
        });
        found.insert(value.to_string());
    }
    found
}

/// A cron expression is written twice — once as the trigger, once as the
/// condition of the job it belongs to — and nothing but this holds the pair
/// equal.
///
/// The two directions fail differently. A declared schedule nothing selects
/// wakes the workflow for a run in which every job skips. A selected schedule
/// nothing declares is the silent one: that job simply never runs again, which
/// is the failure mode the comparison window cannot afford, because the window
/// exists to catch a divergence before teardown deletes the other side.
fn workflow_cron_violations(
    declared: &BTreeSet<String>,
    selected: &BTreeSet<String>,
) -> Vec<String> {
    let mut found = Vec::new();
    for cron in selected.difference(declared) {
        found.push(format!(
            "{CI_WORKFLOW} has a job selecting itself on schedule {cron:?}, which `on.schedule` \
             does not declare. That job never runs again, and an absent job reports nothing."
        ));
    }
    for cron in declared.difference(selected) {
        found.push(format!(
            "{CI_WORKFLOW} declares schedule {cron:?}, which no job selects. The tick starts a run \
             in which every job skips."
        ));
    }
    if declared.len() < MIN_WORKFLOW_CRONS {
        found.push(format!(
            "the cron arm read {} schedule expression(s) from {CI_WORKFLOW}, below the floor of \
             {MIN_WORKFLOW_CRONS} — the reader stopped matching, and two empty sets agree.",
            declared.len(),
        ));
    }
    found
}

/// Every input `on.workflow_dispatch` declares.
///
/// A block-scoped line scan: `workflow_dispatch:` opens the trigger, `inputs:`
/// inside it opens the declarations, and each key one level in is an input name.
fn workflow_dispatch_inputs(text: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    let mut dispatch_indent: Option<usize> = None;
    let mut inputs_indent: Option<usize> = None;
    for line in text.lines() {
        let trimmed = line.trim_end();
        let content = trimmed.trim_start();
        if content.is_empty() || content.starts_with('#') {
            continue;
        }
        let indent = trimmed.len() - content.len();
        if content == "workflow_dispatch:" {
            dispatch_indent = Some(indent);
            inputs_indent = None;
            continue;
        }
        let Some(dispatch) = dispatch_indent else {
            continue;
        };
        if indent <= dispatch {
            dispatch_indent = None;
            inputs_indent = None;
            continue;
        }
        match inputs_indent {
            None => {
                if content == "inputs:" {
                    inputs_indent = Some(indent);
                }
            }
            Some(inputs) if indent <= inputs => {
                inputs_indent = if content == "inputs:" {
                    Some(indent)
                } else {
                    None
                };
            }
            Some(inputs) if indent == inputs + 2 => {
                if let Some(name) = content.strip_suffix(':') {
                    found.insert(name.to_string());
                }
            }
            Some(_) => {}
        }
    }
    found
}

/// Every dispatch input a job condition reads.
fn workflow_input_references(text: &str) -> BTreeSet<String> {
    let needle = "github.event.inputs.";
    let mut found = BTreeSet::new();
    for (index, _) in text.match_indices(needle) {
        let name: String = text[index + needle.len()..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .collect();
        assert!(
            !name.is_empty(),
            "sync guard: {CI_WORKFLOW} reads {needle} with no input name after it"
        );
        found.insert(name);
    }
    found
}

/// A dispatch input is declared once and read once per job that selects on it,
/// and nothing but this holds the spellings equal.
///
/// Both directions fail by absence. An input nothing reads is a switch on the
/// dispatch form that turns nothing on. A reference nothing declares is worse
/// only because the operator believes otherwise: the expression evaluates to
/// nothing, every job selecting on it skips, and the run concludes success while
/// the results the operator dispatched for were never produced.
fn workflow_input_violations(
    declared: &BTreeSet<String>,
    referenced: &BTreeSet<String>,
) -> Vec<String> {
    let mut found = Vec::new();
    for name in referenced.difference(declared) {
        found.push(format!(
            "{CI_WORKFLOW} has a job selecting on dispatch input {name:?}, which \
             `on.workflow_dispatch.inputs` does not declare. The expression is never true, so the \
             switch is dead and the jobs behind it skip silently."
        ));
    }
    for name in declared.difference(referenced) {
        found.push(format!(
            "{CI_WORKFLOW} declares dispatch input {name:?}, which no job reads. A checkbox on the \
             dispatch form that turns nothing on is worse than none."
        ));
    }
    if declared.len() < MIN_WORKFLOW_INPUTS {
        found.push(format!(
            "the dispatch-input arm read {} input(s) from {CI_WORKFLOW}, below the floor of \
             {MIN_WORKFLOW_INPUTS} — the reader stopped matching, and two empty sets agree.",
            declared.len(),
        ));
    }
    found
}

/// Every value a `.bazelrc` config stanza gives a flag, in file order.
///
/// Repeating a flag inside a config is legal and bazel takes the last, so the
/// count is data for the caller rather than an error here.
fn bazelrc_flag_values(text: &str, config: &str, flag: &str) -> Vec<String> {
    let needle = format!("build:{config} --{flag}=");
    let mut found = Vec::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix(&needle) else {
            continue;
        };
        let value = rest.trim();
        assert!(
            !value.is_empty(),
            "sync guard: {BAZELRC} states `build:{config} --{flag}=` with no value"
        );
        found.push(value.to_string());
    }
    found
}

/// The value a `.bazelrc` config stanza gives a flag, where the guard reading it
/// holds exactly one.
fn bazelrc_flag_value(text: &str, config: &str, flag: &str) -> String {
    let mut found = bazelrc_flag_values(text, config, flag);
    assert!(
        found.len() < 2,
        "sync guard: {BAZELRC} states --{flag} for config {config} more than once; the guard \
         would hold only one"
    );
    found.pop().unwrap_or_else(|| {
        panic!("sync guard: {BAZELRC} config {config} states no --{flag}");
    })
}

/// Every directory the workflow's cache steps name, in file order.
fn workflow_cache_paths(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("path:") else {
            continue;
        };
        let value = rest.trim().trim_matches('"');
        assert!(
            !value.is_empty(),
            "sync guard: {CI_WORKFLOW} has a cache step with an empty path"
        );
        found.push(value.to_string());
    }
    found
}

/// The directory the `ci` config writes its action cache to is the directory the
/// workflow has to carry between runs.
///
/// Nothing fails when they disagree: the restore matches a path Bazel never
/// writes, the save banks a directory Bazel never wrote, and every run is a
/// full cold build — green, correct, and as slow as the lane this migration
/// exists to replace, with nothing reporting it.
fn ci_disk_cache_violations(configured: &str, cache_paths: &[String]) -> Vec<String> {
    let carried = cache_paths.iter().filter(|p| *p == configured).count();
    if carried >= CACHE_STEPS_PER_DIRECTORY {
        return Vec::new();
    }
    vec![format!(
        "{BAZELRC} points `build:{CI_CONFIG} --{DISK_CACHE_FLAG}` at {configured:?}, which \
         {CI_WORKFLOW} names in {carried} cache step(s) — a restore and a save is \
         {CACHE_STEPS_PER_DIRECTORY}. It caches {cache_paths:?}. A cache keyed on a directory \
         Bazel does not write makes every run a cold build and reports nothing."
    )]
}

/// The cache-size step warns off a fraction of the GC cap `build:{CI_CONFIG}`
/// sets, and reads that cap by matching one `.bazelrc` line byte for byte:
/// `<digits>G`, stated once.
///
/// Nothing fails when the cap stops matching that shape. The step still prints a
/// size, so it still looks like it works, while the annotation — the only part
/// of it anyone sees on a green run — can never fire again and the cache
/// saturates unreported. Respelling the cap is also the first remediation the
/// measurement exists to prescribe, so the likeliest edit is the one that
/// silences the reader. Stating it twice is worse than harmless: bazel takes the
/// last, the step's `tail` agrees, but the two files now disagree about which
/// number is the cap unless a human reads both.
fn ci_cache_watermark_violations(workflow: &str, caps: &[String]) -> Vec<String> {
    if !workflow.contains(GC_MAX_SIZE_FLAG) {
        return vec![format!(
            "{CI_WORKFLOW} names no --{GC_MAX_SIZE_FLAG}, so nothing there reads the cap this \
             check pins. If the cache-size step is gone, delete this check with it; if the flag \
             was renamed, rename it here."
        )];
    }
    if caps.len() != 1 {
        return vec![format!(
            "{BAZELRC} states `build:{CI_CONFIG} --{GC_MAX_SIZE_FLAG}` {} time(s), and the \
             cache-size step in {CI_WORKFLOW} places its watermark from exactly one. None leaves \
             the step reporting a size it can never warn about; more than one leaves the cap \
             stated in two places for a human to reconcile.",
            caps.len()
        )];
    }
    let cap = &caps[0];
    let digits = cap.strip_suffix('G').unwrap_or_default();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return vec![format!(
            "{BAZELRC} states `build:{CI_CONFIG} --{GC_MAX_SIZE_FLAG}={cap}`, which the \
             cache-size step in {CI_WORKFLOW} cannot read: it matches `<digits>G` and nothing \
             else. A cap it cannot read costs no correctness and all of the signal — the step \
             goes on printing a size and never warns again."
        )];
    }
    Vec::new()
}

/// scrub is validated against exactly one gitleaks release, and four files name
/// it: the wrapper's pin, the test harness's copy of that pin, `MODULE.bazel`'s
/// archive pin, and the workflow that downloads the same asset for the tree
/// scan. A skew means the scan deciding what ships was run by an engine the
/// wrapper was never validated against — with every gate green, because each
/// side is internally consistent.
fn gitleaks_violations(
    module_version: &str,
    module_sha: &str,
    wrapper_version: &str,
    harness_version: &str,
    workflow_version: &str,
    workflow_sha: &str,
) -> Vec<String> {
    let mut found = Vec::new();
    for (rel, version) in [
        (GITLEAKS_WRAPPER, wrapper_version),
        (GITLEAKS_HARNESS, harness_version),
        (CI_WORKFLOW, workflow_version),
    ] {
        if version != module_version {
            found.push(format!(
                "{rel} names gitleaks {version:?} but MODULE.bazel {MODULE_GITLEAKS_NAME} fetches \
                 {module_version:?}. The wrapper is validated against one release; a scan run by \
                 another is a gate nobody checked."
            ));
        }
    }
    if workflow_sha != module_sha {
        found.push(format!(
            "{CI_WORKFLOW} verifies its gitleaks download against {workflow_sha:?} but \
             MODULE.bazel {MODULE_GITLEAKS_SHA_NAME} pins {module_sha:?} for the same asset. \
             Release assets are mutable, so two checksums for one URL is two different binaries."
        ));
    }
    found
}

/// The version requirement `[workspace.dependencies]` states for `crate_name`,
/// as written.
fn workspace_dependency_req(text: &str, crate_name: &str) -> String {
    let parsed: toml::Value = toml::from_str(text)
        .unwrap_or_else(|e| panic!("sync guard: malformed root Cargo.toml: {e}"));
    let entry = parsed
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(|d| d.get(crate_name))
        .unwrap_or_else(|| {
            panic!("sync guard: root Cargo.toml [workspace.dependencies] has no {crate_name}")
        });
    let req = match entry {
        toml::Value::String(req) => Some(req.as_str()),
        table => table.get("version").and_then(|v| v.as_str()),
    };
    req.unwrap_or_else(|| {
        panic!(
            "sync guard: root Cargo.toml [workspace.dependencies] {crate_name} states no version"
        )
    })
    .to_string()
}

/// The CLI that writes a crate's JS glue and the crate that answers it are one
/// version, so the crate pin has to be exact and the Bazel archive pin has to
/// equal it.
fn wasm_bindgen_violations(crate_req: &str, module_version: &str) -> Vec<String> {
    let Some(crate_version) = crate_req.strip_prefix('=') else {
        return vec![format!(
            "root Cargo.toml pins {WASM_BINDGEN_CRATE} as {crate_req:?}, which is a range: the \
             crate and the CLI that generates its glue must be one exact version, written \
             `=<version>`."
        )];
    };
    if crate_version == module_version {
        return Vec::new();
    }
    vec![format!(
        "root Cargo.toml pins the {WASM_BINDGEN_CRATE} crate at {crate_version:?} but MODULE.bazel \
         {MODULE_WASM_BINDGEN_NAME} fetches CLI {module_version:?}. Bump both: mismatched glue \
         fails in the browser, not in the build."
    )]
}

/// The version `surface/package.json` pins the transpiler to.
///
/// The pin is what the build's manifest emitter records, so a range would put a
/// version in the manifest that no install is held to.
fn npm_manifest_pin(text: &str) -> String {
    let parsed: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("sync guard: malformed {SURFACE_MANIFEST}: {e}"));
    parsed
        .get("dependencies")
        .and_then(|d| d.get(JCO_PACKAGE))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("sync guard: {SURFACE_MANIFEST} has no {JCO_PACKAGE} dependency"))
        .to_string()
}

/// Every package an npm lockfile's own install resolves, by name.
///
/// Nested `node_modules/<a>/node_modules/<b>` keys are npm's hoisting record for
/// a conflicting transitive version, not what the tree installs at top level, so
/// they are skipped. Parsed once per file: the agreement arm below asks about
/// every direct dependency in turn, and re-parsing the whole lockfile per lookup
/// is the shape the next arm would copy.
fn npm_lock_versions(rel: &str, text: &str) -> BTreeMap<String, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|e| panic!("sync guard: malformed {rel}: {e}"));
    let mut found = BTreeMap::new();
    let Some(packages) = parsed.get("packages").and_then(|p| p.as_object()) else {
        return found;
    };
    for (key, entry) in packages {
        let Some(name) = key.strip_prefix("node_modules/") else {
            continue;
        };
        if name.contains("/node_modules/") {
            continue;
        }
        let Some(version) = entry.get("version").and_then(|v| v.as_str()) else {
            continue;
        };
        found.insert(name.to_string(), version.to_string());
    }
    found
}

/// The version `surface/package-lock.json` resolves the transpiler to.
fn npm_lock_version(text: &str) -> String {
    npm_lock_versions(SURFACE_NPM_LOCK, text)
        .remove(JCO_PACKAGE)
        .unwrap_or_else(|| {
            panic!("sync guard: {SURFACE_NPM_LOCK} resolves no node_modules/{JCO_PACKAGE}")
        })
}

/// The version `surface/pnpm-lock.yaml` resolves the transpiler to.
///
/// The transpiler is a direct dependency of that tree, so the root importer
/// states it — read through the same reader every other package goes through,
/// because the `packages:` keys carry peer suffixes that are resolutions rather
/// than versions.
fn pnpm_lock_version(text: &str) -> String {
    pnpm_importer_versions(text)
        .remove(JCO_PACKAGE)
        .unwrap_or_else(|| {
            panic!("sync guard: {SURFACE_PNPM_LOCK} resolves no {JCO_PACKAGE} version")
        })
}

/// One pin, two lockfiles, two build systems installing from them.
fn jco_violations(pin: &str, npm_lock: &str, pnpm_lock: &str) -> Vec<String> {
    let mut found = Vec::new();
    if pin.starts_with(['^', '~', '>', '<', '*']) {
        found.push(format!(
            "{SURFACE_MANIFEST} pins {JCO_PACKAGE} as {pin:?}, which is a range. The build records \
             the pin as the transpiler version in every processor manifest, so it has to be the \
             version that ran."
        ));
        return found;
    }
    for (lock, version) in [(SURFACE_NPM_LOCK, npm_lock), (SURFACE_PNPM_LOCK, pnpm_lock)] {
        if version != pin {
            found.push(format!(
                "{SURFACE_MANIFEST} pins {JCO_PACKAGE} at {pin:?} but {lock} resolves \
                 {version:?}. Re-import the lockfile: the two lanes would transpile the browser \
                 assets with different transpilers."
            ));
        }
    }
    found
}

/// Every `(name, version)` pair an npm lockfile installs, nested copies
/// included.
///
/// `npm_lock_versions` deliberately reports only the hoisted top layer, which is
/// what a direct dependency resolves to; this is the whole graph, keyed by name,
/// because a name can legitimately appear at two versions (one hoisted, one
/// nested under the dependent that pinned it).
fn npm_graph_versions(rel: &str, text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|e| panic!("sync guard: malformed {rel}: {e}"));
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let Some(packages) = parsed.get("packages").and_then(|p| p.as_object()) else {
        return found;
    };
    for (key, entry) in packages {
        // The last `node_modules/` segment names the package; everything before
        // it is the chain of dependents it was nested under.
        let Some((_, name)) = key.rsplit_once("node_modules/") else {
            continue;
        };
        let Some(version) = entry.get("version").and_then(|v| v.as_str()) else {
            continue;
        };
        found
            .entry(name.to_string())
            .or_default()
            .insert(version.to_string());
    }
    found
}

/// Every `(name, version)` pair a pnpm lockfile's `packages:` block resolves.
///
/// The keys are `name@version`, two spaces in, with scoped names quoted — the
/// same line shape `pnpm_importer_versions` reads one block up. The `snapshots:`
/// block repeats them with peer suffixes and is skipped by the indent-0 reset.
fn pnpm_graph_versions(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut in_packages = false;
    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        if indent == 0 {
            in_packages = trimmed == "packages:";
            continue;
        }
        if !in_packages || indent != 2 {
            continue;
        }
        let Some(key) = trimmed.trim_start().strip_suffix(':') else {
            continue;
        };
        let key = key.trim_matches('\'');
        // A scoped name starts with `@`, so the separator is the last one.
        let Some((name, version)) = key.rsplit_once('@') else {
            continue;
        };
        found
            .entry(name.to_string())
            .or_default()
            .insert(version.to_string());
    }
    found
}

/// Every package a `package.json` depends on directly, runtime and dev alike.
///
/// Derived rather than listed, so a dependency added to a tree joins the
/// comparison below without anyone remembering to add it here.
fn direct_dependencies(rel: &str, text: &str) -> BTreeSet<String> {
    let parsed: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|e| panic!("sync guard: malformed {rel}: {e}"));
    let mut found = BTreeSet::new();
    for field in ["dependencies", "devDependencies"] {
        let Some(table) = parsed.get(field).and_then(|d| d.as_object()) else {
            continue;
        };
        found.extend(table.keys().cloned());
    }
    found
}

/// What a pnpm lockfile's root importer resolves each of its direct
/// dependencies to.
///
/// The `importers:` block is the readable half: it states one version per
/// declared dependency, where the `packages:` keys carry peer suffixes
/// (`vitest@4.1.1(vite@8.0.16(...))`) that are not versions. A line scan is the
/// whole need; a YAML parser is not.
fn pnpm_importer_versions(text: &str) -> BTreeMap<String, String> {
    let mut found = BTreeMap::new();
    let mut in_root_importer = false;
    let mut package: Option<String> = None;
    for line in text.lines() {
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim_end();
        if indent == 0 {
            in_root_importer = false;
            continue;
        }
        if indent == 2 {
            // `  .:` opens the root importer; `  <path>:` opens another one.
            in_root_importer = trimmed == "  .:";
            continue;
        }
        if !in_root_importer {
            continue;
        }
        if indent == 6 {
            package = trimmed
                .trim_start()
                .strip_suffix(':')
                .map(|name| name.trim_matches('\'').to_string());
            continue;
        }
        if indent == 8 {
            let Some(version) = trimmed.trim_start().strip_prefix("version: ") else {
                continue;
            };
            let Some(name) = package.take() else {
                continue;
            };
            // Peer-dependency suffixes identify a resolution, not a version.
            let version = version.split('(').next().unwrap_or(version);
            found.insert(name, version.to_string());
        }
    }
    found
}

/// Two lockfiles over one manifest have to resolve the same dependency graph.
///
/// Two comparisons, because they fail differently. The direct one reads each
/// lockfile's record of what the manifest asked for, so a dependency one
/// lockfile has and the other does not is named as such. The graph one compares
/// every `(name, version)` pair either lockfile installs, transitive included:
/// `vite` is nobody's direct dependency and an `npm audit fix` that bumps it
/// without a `pnpm import` leaves the two lanes bundling the browser assets with
/// different toolchains, every gate green.
///
/// The overlap with `jco_violations` is deliberate and small: that arm holds the
/// transpiler pin *exact*, because the build records it in every processor
/// manifest.
fn lockfile_agreement_violations(
    tree: &str,
    manifest: &str,
    npm_lock: &str,
    pnpm_lock: &str,
) -> Vec<String> {
    let manifest_rel = format!("{tree}/package.json");
    let npm_rel = format!("{tree}/package-lock.json");
    let pnpm_rel = format!("{tree}/pnpm-lock.yaml");
    let direct = direct_dependencies(&manifest_rel, manifest);
    if direct.is_empty() {
        return vec![format!(
            "{manifest_rel} declares no dependencies, so the lockfile comparison has nothing to \
             compare. A tree with no dependencies does not need two lockfiles."
        )];
    }
    let npm = npm_lock_versions(&npm_rel, npm_lock);
    let pnpm = pnpm_importer_versions(pnpm_lock);
    let mut found = Vec::new();
    for package in direct {
        let from_npm = npm.get(&package).cloned();
        let from_pnpm = pnpm.get(&package).cloned();
        match (from_npm, from_pnpm) {
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => found.push(format!(
                "{npm_rel} resolves {package} to {a:?} but {pnpm_rel} resolves {b:?}. Re-import the \
                 pnpm lockfile: the make lane and the Bazel lane are building against different \
                 versions of it."
            )),
            (a, b) => found.push(format!(
                "{manifest_rel} depends on {package}, resolved by {npm_rel} to {a:?} and by \
                 {pnpm_rel} to {b:?}. A dependency one lockfile has and the other does not is a \
                 lockfile that was not re-imported."
            )),
        }
    }

    let npm_graph = npm_graph_versions(&npm_rel, npm_lock);
    let pnpm_graph = pnpm_graph_versions(pnpm_lock);
    if npm_graph.is_empty() || pnpm_graph.is_empty() {
        found.push(format!(
            "the graph comparison read {} package(s) from {npm_rel} and {} from {pnpm_rel}. An \
             empty side compares nothing and reports clean.",
            npm_graph.len(),
            pnpm_graph.len(),
        ));
        return found;
    }
    let names: BTreeSet<&String> = npm_graph.keys().chain(pnpm_graph.keys()).collect();
    for name in names {
        let from_npm = npm_graph.get(name);
        let from_pnpm = pnpm_graph.get(name);
        if from_npm == from_pnpm {
            continue;
        }
        found.push(format!(
            "{npm_rel} installs {name} at {:?} but {pnpm_rel} installs it at {:?}. Re-import the \
             pnpm lockfile: transitive versions decide what the two lanes actually build with.",
            from_npm.map(|v| v.iter().cloned().collect::<Vec<_>>()),
            from_pnpm.map(|v| v.iter().cloned().collect::<Vec<_>>()),
        ));
    }
    found
}

/// A `<major>.<minor>.<patch>` string as a comparable triple.
///
/// Whole, not truncated to the major: node features land in minors, so a floor
/// of `>=22.12.0` is not satisfied by 22.0.0, and an arm comparing majors alone
/// reports a pin below the floor it names as clean.
fn version_triple(rel: &str, version: &str) -> (u32, u32, u32) {
    let mut parts = version.split('.');
    let mut triple = [0u32; 3];
    for slot in &mut triple {
        let parsed = parts.next().and_then(|part| part.parse().ok());
        *slot = parsed.unwrap_or_else(|| {
            panic!(
                "sync guard: {rel} states {version:?}, which is not a numeric \
                 <major>.<minor>.<patch> version"
            )
        });
    }
    (triple[0], triple[1], triple[2])
}

/// The `engines.node` floor `surface/package.json` states, as written.
fn node_engine_floor(text: &str) -> String {
    let parsed: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("sync guard: malformed {SURFACE_MANIFEST}: {e}"));
    parsed
        .get("engines")
        .and_then(|e| e.get("node"))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("sync guard: {SURFACE_MANIFEST} states no engines.node"))
        .to_string()
}

/// The hermetic node the Bazel lane runs must satisfy the floor the npm tree
/// declares, which is what the make lane's preflight checks of the system node.
fn node_floor_violations(floor: &str, module_node: &str) -> Vec<String> {
    let Some(minimum) = floor.strip_prefix(">=") else {
        return vec![format!(
            "{SURFACE_MANIFEST} states engines.node as {floor:?}; the floor is read as `>=<version>` \
             and nothing else is understood."
        )];
    };
    if version_triple(SURFACE_MANIFEST, minimum) <= version_triple("MODULE.bazel", module_node) {
        return Vec::new();
    }
    vec![format!(
        "MODULE.bazel {MODULE_NODE_NAME} is {module_node:?}, below the {floor:?} floor \
         {SURFACE_MANIFEST} declares. The hermetic toolchain is what the Bazel lane runs."
    )]
}

/// `rustc_env` keys that restate a crate's cargo identity, and the `[package]`
/// field each has to equal.
///
/// A target states these when something downstream reads them: wasm-bindgen
/// bakes the pair into the snippet directory names it emits, so a build whose
/// crate identity differs from cargo's produces a differently-shaped bundle
/// tree from the same sources.
const CARGO_ENV_FIELDS: [(&str, &str); 2] =
    [("CARGO_PKG_NAME", "name"), ("CARGO_PKG_VERSION", "version")];

/// Every distinct `edition = "…"` literal in a `BUILD.bazel`, in file order.
///
/// A text scan rather than a Starlark evaluation: the guard runs inside a Bazel
/// test, where loading the build files again is not available.
fn build_editions(text: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in text.lines() {
        let Some((_, rest)) = line.split_once("edition = \"") else {
            continue;
        };
        let Some((value, _)) = rest.split_once('"') else {
            continue;
        };
        found.insert(value.to_string());
    }
    found
}

/// The `[package] edition` of a cargo manifest, or `None` for a manifest that
/// declares no package (a virtual workspace root).
fn package_edition(rel: &Path, text: &str) -> Option<String> {
    let parsed: toml::Value = toml::from_str(text)
        .unwrap_or_else(|e| panic!("sync guard: malformed {}: {e}", rel.display()));
    let package = parsed.get("package")?;
    let edition = package.get("edition").and_then(|e| e.as_str());
    Some(
        edition
            .unwrap_or_else(|| {
                panic!(
                    "sync guard: {} declares a package with no literal `edition`, which is what \
                     the Bazel edition attribute is held equal to",
                    rel.display()
                )
            })
            .to_string(),
    )
}

/// Every literal a `BUILD.bazel` assigns to a `rustc_env` key, and how many
/// times the key is named at all.
///
/// Both halves matter. A file with two rust targets can restate the key twice
/// and only one of them be wrong, so every occurrence is collected rather than
/// the first. And a restatement this scan cannot parse — different spacing, a
/// variable instead of a literal — is indistinguishable from no restatement at
/// all if only the parsed ones are counted, so the mentions are counted too and
/// the caller treats a shortfall as a violation.
fn rustc_env_literals(text: &str, key: &str) -> (Vec<String>, usize) {
    let mention = format!("\"{key}\"");
    let mentions = text.matches(&mention).count();
    let needle = format!("{mention}: \"");
    let mut values = Vec::new();
    for (index, _) in text.match_indices(&needle) {
        let rest = &text[index + needle.len()..];
        if let Some((value, _)) = rest.split_once('"') {
            values.push(value.to_string());
        }
    }
    (values, mentions)
}

/// A `BUILD.bazel` that restates its crate's cargo identity has to restate it
/// correctly: the manifest is where the name and version are decided, and the
/// Bazel literal is a copy that nothing else compares.
fn cargo_env_violations(
    build_rel: &Path,
    cargo_rel: &Path,
    build_text: &str,
    cargo_text: &str,
) -> Vec<String> {
    let parsed: toml::Value = toml::from_str(cargo_text)
        .unwrap_or_else(|e| panic!("sync guard: malformed {}: {e}", cargo_rel.display()));
    let mut found = Vec::new();
    for (key, field) in CARGO_ENV_FIELDS {
        let (stated_values, mentions) = rustc_env_literals(build_text, key);
        if mentions == 0 {
            continue;
        }
        if stated_values.len() != mentions {
            found.push(format!(
                "{} names {key} {mentions} time(s) but states a readable string literal for \
                 {} of them. A restatement this guard cannot read is a restatement nothing \
                 checks; write it as `\"{key}\": \"<value>\"`.",
                build_rel.display(),
                stated_values.len(),
            ));
        }
        let declared = parsed
            .get("package")
            .and_then(|p| p.get(field))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "sync guard: {} states {key}, but {} declares no [package] {field}",
                    build_rel.display(),
                    cargo_rel.display(),
                )
            });
        for stated in stated_values.iter().filter(|v| v.as_str() != declared) {
            found.push(format!(
                "{} states {key} = {stated:?} but {} declares {field} = {declared:?}. The Bazel \
                 literal is a copy of the manifest's value, and wasm-bindgen names its emitted \
                 snippet directories from the pair.",
                build_rel.display(),
                cargo_rel.display(),
            ));
        }
    }
    found
}

fn edition_violations_from(
    build_rel: &Path,
    cargo_rel: &Path,
    build_editions: &BTreeSet<String>,
    cargo_edition: &str,
) -> Vec<String> {
    if build_editions.is_empty() {
        if cargo_edition == DEFAULT_EDITION {
            return Vec::new();
        }
        return vec![format!(
            "{} states no edition, so its rust targets take the macro default {DEFAULT_EDITION:?}, \
             but {} declares {cargo_edition:?}.",
            build_rel.display(),
            cargo_rel.display(),
        )];
    }
    build_editions
        .iter()
        .filter(|e| e.as_str() != cargo_edition)
        .map(|e| {
            format!(
                "{} builds at edition {e:?} but {} declares {cargo_edition:?}. Bump both: cargo \
                 compiles from the manifest, Bazel from the attribute.",
                build_rel.display(),
                cargo_rel.display(),
            )
        })
        .collect()
}

/// The edition literals a crate macro's parameters default to must be the one
/// `DEFAULT_EDITION` names.
///
/// Without this, bumping the macro default and not the constant leaves a crate
/// whose `BUILD.bazel` states no edition building at the new one while this
/// guard compares its manifest against the old one and calls the pair in sync —
/// the silent divergence the edition arm exists to kill, one level up.
fn macro_default_violations(defs: &str, text: &str) -> Vec<String> {
    let defaults = build_editions(text);
    if defaults.len() == 1 && defaults.contains(DEFAULT_EDITION) {
        return Vec::new();
    }
    if defaults.is_empty() {
        return vec![format!(
            "{defs} states no `edition` default, but the edition arm holds every \
             `BUILD.bazel` that states none to {DEFAULT_EDITION:?}. Either the macros lost their \
             default or the scan stopped matching."
        )];
    }
    vec![format!(
        "{defs} defaults `edition` to {defaults:?} but xtask's DEFAULT_EDITION is \
         {DEFAULT_EDITION:?}. Bump both: the first is what a BUILD.bazel stating no edition \
         builds at, the second is what that BUILD.bazel is checked against."
    )]
}

/// The stamp key is written in four files and substituted by Bazel between
/// them: the script that emits it, the binary's `rustc_env`, the frontend
/// bundler that reads it out of the status file, and the gate that greps the
/// built bundles for the placeholder that survives when nothing substituted it.
///
/// A rename on any side is not an error anywhere: Bazel substitutes nothing and
/// the release artifact reports the literal placeholder as its version. The two
/// consumers must agree with each other as well as with the script, or a
/// release ships a backend and a browser bundle claiming different builds.
fn stamp_key_violations(
    key: &str,
    status_sh: &str,
    consumer_build: &str,
    bundler: &str,
    artifact_check: &str,
) -> Vec<String> {
    let mut found = Vec::new();
    if !status_sh.contains(&format!("echo \"{key} ")) {
        found.push(format!(
            "{STATUS_SCRIPT} emits no {key:?} stamp key. {STAMP_CONSUMER} substitutes it, and an \
             unemitted key is left in the binary as its literal placeholder."
        ));
    }
    if !consumer_build.contains(&format!("{{{key}}}")) {
        found.push(format!(
            "{STAMP_CONSUMER} does not substitute {key:?}. {STATUS_SCRIPT} emits it; a consumer \
             naming a different key bakes in a placeholder that no stamp ever replaces."
        ));
    }
    if !bundler.contains(&format!("\"{key}\"")) {
        found.push(format!(
            "{STAMP_BUNDLER} does not read {key:?}. {STATUS_SCRIPT} emits it; a bundler looking \
             for a different key falls back to its placeholder on every release, so the browser \
             and the backend disagree about which build is running."
        ));
    }
    if !artifact_check.contains(&format!("{{{key}}}")) {
        found.push(format!(
            "{STAMP_ARTIFACT_CHECK} does not look for the {key:?} placeholder. It greps the built \
             bundles for exactly that string; spelled any other way it finds nothing and reports \
             every bundle clean."
        ));
    }
    found
}

/// One violation per package whose two build systems disagree about it — its
/// edition, or the cargo identity its `BUILD.bazel` restates — plus one if too
/// few packages were examined for the arm to mean anything.
fn manifest_pair_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut found = Vec::new();
    let mut pairs = 0usize;
    for build_rel in files
        .iter()
        .filter(|rel| rel.file_name().is_some_and(|n| n == "BUILD.bazel"))
    {
        let cargo_rel = build_rel.with_file_name("Cargo.toml");
        if !files.contains(&cargo_rel) {
            continue;
        }
        let read = |rel: &Path| {
            std::fs::read_to_string(root.join(rel))
                .unwrap_or_else(|e| panic!("sync guard: cannot read {}: {e}", rel.display()))
        };
        let cargo_text = read(&cargo_rel);
        let Some(cargo_edition) = package_edition(&cargo_rel, &cargo_text) else {
            continue;
        };
        pairs += 1;
        let build_text = read(build_rel);
        found.extend(edition_violations_from(
            build_rel,
            &cargo_rel,
            &build_editions(&build_text),
            &cargo_edition,
        ));
        found.extend(cargo_env_violations(
            build_rel,
            &cargo_rel,
            &build_text,
            &cargo_text,
        ));
    }
    if pairs < MIN_EDITION_PAIRS {
        found.push(format!(
            "the edition arm examined {pairs} BUILD.bazel/Cargo.toml pair(s), below the floor of \
             {MIN_EDITION_PAIRS} — the file set has collapsed, and an arm with nothing to compare \
             reports clean."
        ));
    }
    found
}

/// True if every pin that two files must state identically is stated
/// identically: the Rust version, each crate's edition, and the build-id stamp
/// key.
pub fn run_sync_guard(root: &Path, files: &[PathBuf]) -> bool {
    let read = |rel: &str| {
        std::fs::read_to_string(root.join(rel))
            .unwrap_or_else(|e| panic!("sync guard: cannot read {rel}: {e}"))
    };
    let module = read("MODULE.bazel");
    let mut found = violations_from(
        &toolchain_channel(&read("rust-toolchain.toml")),
        &module_pin(&module, MODULE_VERSION_NAME),
    );
    found.extend(wasm_bindgen_violations(
        &workspace_dependency_req(&read("Cargo.toml"), WASM_BINDGEN_CRATE),
        &module_pin(&module, MODULE_WASM_BINDGEN_NAME),
    ));
    let workflow = read(CI_WORKFLOW);
    let gitleaks_harness = read(GITLEAKS_HARNESS);
    found.extend(gitleaks_violations(
        &module_pin(&module, MODULE_GITLEAKS_NAME),
        &module_pin(&module, MODULE_GITLEAKS_SHA_NAME),
        &rust_str_const(GITLEAKS_WRAPPER, &read(GITLEAKS_WRAPPER), GITLEAKS_CONST),
        &rust_str_const(GITLEAKS_HARNESS, &gitleaks_harness, GITLEAKS_CONST),
        &workflow_env_value(&workflow, CI_GITLEAKS_VERSION),
        &workflow_env_value(&workflow, CI_GITLEAKS_SHA),
    ));
    found.extend(workflow_cron_violations(
        &workflow_schedule_crons(&workflow),
        &workflow_job_crons(&workflow),
    ));
    found.extend(workflow_input_violations(
        &workflow_dispatch_inputs(&workflow),
        &workflow_input_references(&workflow),
    ));
    let bazelrc = read(BAZELRC);
    found.extend(ci_disk_cache_violations(
        &bazelrc_flag_value(&bazelrc, CI_CONFIG, DISK_CACHE_FLAG),
        &workflow_cache_paths(&workflow),
    ));
    found.extend(ci_cache_watermark_violations(
        &workflow,
        &bazelrc_flag_values(&bazelrc, CI_CONFIG, GC_MAX_SIZE_FLAG),
    ));
    let surface_manifest = read(SURFACE_MANIFEST);
    found.extend(jco_violations(
        &npm_manifest_pin(&surface_manifest),
        &npm_lock_version(&read(SURFACE_NPM_LOCK)),
        &pnpm_lock_version(&read(SURFACE_PNPM_LOCK)),
    ));
    found.extend(node_floor_violations(
        &node_engine_floor(&surface_manifest),
        &module_pin(&module, MODULE_NODE_NAME),
    ));
    for tree in NPM_TREES {
        found.extend(lockfile_agreement_violations(
            tree,
            &read(&format!("{tree}/package.json")),
            &read(&format!("{tree}/package-lock.json")),
            &read(&format!("{tree}/pnpm-lock.yaml")),
        ));
    }
    for defs in MACRO_DEFS {
        found.extend(macro_default_violations(defs, &read(defs)));
    }
    found.extend(stamp_key_violations(
        &stamp_key(),
        &read(STATUS_SCRIPT),
        &read(STAMP_CONSUMER),
        &read(STAMP_BUNDLER),
        &read(STAMP_ARTIFACT_CHECK),
    ));
    found.extend(manifest_pair_violations(root, files));
    if found.is_empty() {
        return true;
    }
    eprintln!("sync guard: version pins that must be equal are not:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_pins_pass_and_unequal_pins_fail() {
        assert!(violations_from("1.95.0", "1.95.0").is_empty());
        let out = violations_from("1.95.0", "1.97.1");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("1.97.1"), "{}", out[0]);
    }

    #[test]
    fn the_channel_comes_out_of_the_toolchain_table() {
        assert_eq!(
            toolchain_channel("[toolchain]\nchannel = \"1.95.0\"\ncomponents = [\"clippy\"]\n"),
            "1.95.0"
        );
    }

    #[test]
    #[should_panic(expected = "no [toolchain] channel")]
    fn a_channelless_toolchain_file_panics() {
        toolchain_channel("[toolchain]\ncomponents = []\n");
    }

    #[test]
    fn the_module_version_comes_out_of_the_starlark_assignment() {
        assert_eq!(
            module_pin(
                "bazel_dep(name = \"x\")\n\nRUST_VERSION = \"1.95.0\"\n\nrust = 1\n",
                MODULE_VERSION_NAME
            ),
            "1.95.0"
        );
    }

    #[test]
    #[should_panic(expected = "no RUST_VERSION assignment")]
    fn a_module_file_without_the_pin_panics() {
        module_pin("bazel_dep(name = \"x\")\n", MODULE_VERSION_NAME);
    }

    #[test]
    fn the_wasm_bindgen_requirement_is_read_in_either_manifest_form() {
        assert_eq!(
            workspace_dependency_req(
                "[workspace.dependencies]\nwasm-bindgen = \"=0.2.125\"\n",
                WASM_BINDGEN_CRATE
            ),
            "=0.2.125"
        );
        assert_eq!(
            workspace_dependency_req(
                "[workspace.dependencies]\nwasm-bindgen = { version = \"=0.2.125\" }\n",
                WASM_BINDGEN_CRATE
            ),
            "=0.2.125"
        );
    }

    #[test]
    #[should_panic(expected = "has no wasm-bindgen")]
    fn a_workspace_without_the_crate_pin_panics() {
        workspace_dependency_req(
            "[workspace.dependencies]\nserde = \"1\"\n",
            WASM_BINDGEN_CRATE,
        );
    }

    #[test]
    fn the_cli_pin_is_held_to_the_crate_pin_and_the_crate_pin_to_being_exact() {
        assert!(wasm_bindgen_violations("=0.2.125", "0.2.125").is_empty());

        let skewed = wasm_bindgen_violations("=0.2.125", "0.2.121");
        assert_eq!(skewed.len(), 1, "{skewed:?}");
        assert!(skewed[0].contains("0.2.121"), "{}", skewed[0]);

        let ranged = wasm_bindgen_violations("0.2.125", "0.2.125");
        assert_eq!(ranged.len(), 1, "{ranged:?}");
        assert!(ranged[0].contains("is a range"), "{}", ranged[0]);
    }

    const A_SURFACE_MANIFEST: &str = r#"{
      "engines": {"node": ">=20.0.0"},
      "dependencies": {"@bytecodealliance/jco": "1.4.0"}
    }"#;

    const A_SURFACE_PNPM_LOCK: &str = "\
importers:

  .:
    dependencies:
      '@bytecodealliance/jco':
        specifier: 1.4.0
        version: 1.4.0

packages:

  '@bytecodealliance/jco@1.4.0':
    resolution: {integrity: x}
";

    #[test]
    fn the_transpiler_version_is_read_out_of_the_manifest_and_both_lockfiles() {
        assert_eq!(npm_manifest_pin(A_SURFACE_MANIFEST), "1.4.0");
        assert_eq!(
            npm_lock_version(
                r#"{"packages": {"node_modules/@bytecodealliance/jco": {"version": "1.4.0"}}}"#
            ),
            "1.4.0"
        );
        assert_eq!(pnpm_lock_version(A_SURFACE_PNPM_LOCK), "1.4.0");
    }

    #[test]
    fn a_peer_suffixed_resolution_is_not_read_as_the_transpiler_version() {
        let suffixed = A_SURFACE_PNPM_LOCK.replace(
            "  '@bytecodealliance/jco@1.4.0':",
            "  '@bytecodealliance/jco@1.4.0(acorn@8.17.0)':",
        );
        assert_eq!(pnpm_lock_version(&suffixed), "1.4.0");
    }

    #[test]
    #[should_panic(expected = "resolves no @bytecodealliance/jco")]
    fn a_pnpm_lock_without_the_package_panics() {
        pnpm_lock_version(
            "importers:\n\n  .:\n    dependencies:\n      acorn:\n        version: 8.17.0\n",
        );
    }

    #[test]
    fn both_lockfiles_are_held_to_the_pin_and_the_pin_to_being_exact() {
        assert!(jco_violations("1.4.0", "1.4.0", "1.4.0").is_empty());

        let npm_skew = jco_violations("1.4.0", "1.3.0", "1.4.0");
        assert_eq!(npm_skew.len(), 1, "{npm_skew:?}");
        assert!(npm_skew[0].contains(SURFACE_NPM_LOCK), "{}", npm_skew[0]);

        let pnpm_skew = jco_violations("1.4.0", "1.4.0", "1.5.0");
        assert_eq!(pnpm_skew.len(), 1, "{pnpm_skew:?}");
        assert!(pnpm_skew[0].contains(SURFACE_PNPM_LOCK), "{}", pnpm_skew[0]);

        let ranged = jco_violations("^1.4.0", "1.4.0", "1.4.0");
        assert_eq!(ranged.len(), 1, "{ranged:?}");
        assert!(ranged[0].contains("is a range"), "{}", ranged[0]);
    }

    const A_FRONTEND_MANIFEST: &str = r#"{
      "dependencies": {"lit": "^3.3.2"},
      "devDependencies": {"typescript": "^5.9.3", "vitest": "^4.1.1"}
    }"#;

    // `vite` is nobody's direct dependency: it is the transitive half the graph
    // comparison exists for.
    const A_FRONTEND_NPM_LOCK: &str = r#"{"packages": {
      "": {"name": "frontend"},
      "node_modules/lit": {"version": "3.3.2"},
      "node_modules/typescript": {"version": "5.9.3"},
      "node_modules/vite": {"version": "8.0.16"},
      "node_modules/vitest": {"version": "4.1.1"}
    }}"#;

    const A_FRONTEND_PNPM_LOCK: &str = "\
importers:

  .:
    dependencies:
      lit:
        specifier: ^3.3.2
        version: 3.3.2
    devDependencies:
      typescript:
        specifier: ^5.9.3
        version: 5.9.3
      vitest:
        specifier: ^4.1.1
        version: 4.1.1(@types/node@25.6.0)(vite@8.0.16(esbuild@0.28.1))

packages:

  lit@3.3.2:
    resolution: {integrity: x}

  typescript@5.9.3:
    resolution: {integrity: x}

  vite@8.0.16:
    resolution: {integrity: x}

  vitest@4.1.1:
    resolution: {integrity: x}

snapshots:

  vitest@4.1.1(@types/node@25.6.0)(vite@8.0.16(esbuild@0.28.1)):
    dependencies:
      vite: 8.0.16
";

    #[test]
    fn every_declared_dependency_is_collected_from_both_manifest_tables() {
        assert_eq!(
            direct_dependencies("frontend/package.json", A_FRONTEND_MANIFEST),
            ["lit", "typescript", "vitest"]
                .iter()
                .map(|s| (*s).to_string())
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn the_root_importer_gives_one_version_per_dependency_without_its_peer_suffix() {
        let versions = pnpm_importer_versions(A_FRONTEND_PNPM_LOCK);
        assert_eq!(versions.get("lit").map(String::as_str), Some("3.3.2"));
        assert_eq!(versions.get("vitest").map(String::as_str), Some("4.1.1"));
        assert_eq!(versions.len(), 3, "{versions:?}");
    }

    #[test]
    fn a_second_importer_does_not_answer_for_the_root_one() {
        let two = format!(
            "{A_FRONTEND_PNPM_LOCK}\n  packages/other:\n    dependencies:\n      lit:\n        specifier: ^2.0.0\n        version: 2.0.0\n"
        );
        assert_eq!(
            pnpm_importer_versions(&two).get("lit").map(String::as_str),
            Some("3.3.2")
        );
    }

    fn agreement(npm_lock: &str, pnpm_lock: &str) -> Vec<String> {
        lockfile_agreement_violations("frontend", A_FRONTEND_MANIFEST, npm_lock, pnpm_lock)
    }

    #[test]
    fn two_lockfiles_resolving_the_same_versions_agree() {
        assert!(agreement(A_FRONTEND_NPM_LOCK, A_FRONTEND_PNPM_LOCK).is_empty());
    }

    #[test]
    fn a_version_skew_between_the_lockfiles_is_reported_per_package() {
        let skewed = A_FRONTEND_NPM_LOCK.replace("3.3.2", "3.3.1");
        let out = agreement(&skewed, A_FRONTEND_PNPM_LOCK);
        // Both arms see a direct dependency skew, and say different things
        // about it.
        assert_eq!(out.len(), 2, "{out:?}");
        assert!(out.iter().all(|v| v.contains("lit")), "{out:?}");
        assert!(
            out[0].contains("the make lane and the Bazel lane"),
            "{}",
            out[0]
        );
        assert!(out[1].contains("transitive versions"), "{}", out[1]);
    }

    #[test]
    fn a_transitive_skew_no_manifest_names_is_reported() {
        // The failure the direct arm cannot see: an `npm audit fix` bumps a
        // package nobody declares, and the two lanes bundle with different
        // toolchains.
        let skewed = A_FRONTEND_NPM_LOCK.replace("8.0.16", "8.0.15");
        let out = agreement(&skewed, A_FRONTEND_PNPM_LOCK);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("vite"), "{}", out[0]);
        assert!(out[0].contains("8.0.15"), "{}", out[0]);
    }

    #[test]
    fn a_package_only_one_lockfile_installs_is_reported() {
        let extra = A_FRONTEND_NPM_LOCK.replace(
            r#""node_modules/vite": {"version": "8.0.16"},"#,
            r#""node_modules/vite": {"version": "8.0.16"}, "node_modules/rollup": {"version": "4.0.0"},"#,
        );
        let out = agreement(&extra, A_FRONTEND_PNPM_LOCK);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("rollup"), "{}", out[0]);
    }

    #[test]
    fn a_dependency_missing_from_one_lockfile_is_reported() {
        let out = agreement(r#"{"packages": {}}"#, A_FRONTEND_PNPM_LOCK);
        // Three direct dependencies, plus the graph arm refusing to compare
        // against an empty side.
        assert_eq!(out.len(), 4, "{out:?}");
        assert!(out[0].contains("was not re-imported"), "{}", out[0]);
        assert!(out[3].contains("reports clean"), "{}", out[3]);
    }

    #[test]
    fn the_graph_readers_see_every_installed_version() {
        let npm = npm_graph_versions("frontend/package-lock.json", A_FRONTEND_NPM_LOCK);
        let pnpm = pnpm_graph_versions(A_FRONTEND_PNPM_LOCK);
        assert_eq!(npm, pnpm, "npm {npm:?} pnpm {pnpm:?}");
        assert_eq!(npm.len(), 4, "{npm:?}");
        // The `snapshots:` block repeats every package with peer suffixes; a
        // reader that fell into it would report `vite@8.0.16(esbuild@0.28.1)`
        // as a version.
        assert_eq!(
            pnpm.get("vite")
                .map(|v| v.iter().cloned().collect::<Vec<_>>()),
            Some(vec!["8.0.16".to_string()])
        );
    }

    #[test]
    fn the_npm_graph_reader_keeps_a_nested_copy_beside_the_hoisted_one() {
        let both = r#"{"packages": {
          "node_modules/commander": {"version": "12.1.0"},
          "node_modules/terser/node_modules/commander": {"version": "2.20.3"}
        }}"#;
        let graph = npm_graph_versions("surface/package-lock.json", both);
        assert_eq!(
            graph
                .get("commander")
                .map(|v| v.iter().cloned().collect::<Vec<_>>()),
            // Lexicographic, because a version set is compared, not ordered.
            Some(vec!["12.1.0".to_string(), "2.20.3".to_string()])
        );
    }

    #[test]
    fn the_pnpm_graph_reader_unquotes_a_scoped_name() {
        let text =
            "packages:\n\n  '@bytecodealliance/jco@1.4.0':\n    resolution: {integrity: x}\n";
        let graph = pnpm_graph_versions(text);
        assert_eq!(
            graph
                .get("@bytecodealliance/jco")
                .map(|v| v.iter().cloned().collect::<Vec<_>>()),
            Some(vec!["1.4.0".to_string()])
        );
    }

    #[test]
    fn a_nested_hoisting_entry_does_not_answer_for_a_top_level_package() {
        let nested = r#"{"packages": {
          "": {"name": "frontend"},
          "node_modules/lit": {"version": "3.3.2"},
          "node_modules/vitest/node_modules/lit": {"version": "2.0.0"}
        }}"#;
        let versions = npm_lock_versions("frontend/package-lock.json", nested);
        assert_eq!(versions.get("lit").map(String::as_str), Some("3.3.2"));
        assert_eq!(versions.len(), 1, "{versions:?}");
    }

    #[test]
    fn a_manifest_with_no_dependencies_fails_instead_of_reporting_clean() {
        let out = lockfile_agreement_violations("frontend", "{}", "{}", "");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("nothing to compare"), "{}", out[0]);
    }

    #[test]
    fn the_hermetic_node_must_satisfy_the_declared_floor() {
        assert_eq!(node_engine_floor(A_SURFACE_MANIFEST), ">=20.0.0");
        assert!(node_floor_violations(">=20.0.0", "22.20.0").is_empty());
        assert!(node_floor_violations(">=20.0.0", "20.11.0").is_empty());

        let below = node_floor_violations(">=20.0.0", "18.20.0");
        assert_eq!(below.len(), 1, "{below:?}");
        assert!(below[0].contains("18.20.0"), "{}", below[0]);

        // A minor-qualified floor is enforced as written, not truncated to its
        // major: the same major below the stated minor is a violation.
        assert!(node_floor_violations(">=22.12.0", "22.20.0").is_empty());
        let minor_below = node_floor_violations(">=22.12.0", "22.0.0");
        assert_eq!(minor_below.len(), 1, "{minor_below:?}");
        assert!(minor_below[0].contains("22.0.0"), "{}", minor_below[0]);

        let patch_below = node_floor_violations(">=22.12.1", "22.12.0");
        assert_eq!(patch_below.len(), 1, "{patch_below:?}");

        let unreadable = node_floor_violations("^20.0.0", "22.20.0");
        assert_eq!(unreadable.len(), 1, "{unreadable:?}");
        assert!(unreadable[0].contains("read as"), "{}", unreadable[0]);
    }

    fn editions(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|v| (*v).to_string()).collect()
    }

    fn cargo_env_check(build_text: &str, cargo_text: &str) -> Vec<String> {
        cargo_env_violations(
            Path::new("c/BUILD.bazel"),
            Path::new("c/Cargo.toml"),
            build_text,
            cargo_text,
        )
    }

    const A_MANIFEST: &str = "[package]\nname = \"a-crate\"\nversion = \"0.1.0\"\n";

    #[test]
    fn a_restated_cargo_identity_is_held_to_the_manifest() {
        let agreeing = "rust_library(\n    rustc_env = {\n        \"CARGO_PKG_NAME\": \
                        \"a-crate\",\n        \"CARGO_PKG_VERSION\": \"0.1.0\",\n    },\n)\n";
        assert!(cargo_env_check(agreeing, A_MANIFEST).is_empty());

        let renamed = agreeing.replace("a-crate", "another-crate");
        let out = cargo_env_check(&renamed, A_MANIFEST);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("another-crate"), "{}", out[0]);

        let bumped = agreeing.replace("0.1.0", "0.2.0");
        let out = cargo_env_check(&bumped, A_MANIFEST);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("0.2.0"), "{}", out[0]);
    }

    #[test]
    fn a_build_file_restating_nothing_is_not_checked() {
        assert!(cargo_env_check("rust_library(\n    name = \"a\",\n)\n", A_MANIFEST).is_empty());
    }

    #[test]
    fn a_second_target_restating_the_identity_is_checked_too() {
        // Two rust targets in one file, only the second one wrong. Reading the
        // first match and stopping calls that clean.
        let two = "rust_library(\n    rustc_env = {\"CARGO_PKG_NAME\": \"a-crate\"},\n)\n\
                   rust_shared_library(\n    rustc_env = {\"CARGO_PKG_NAME\": \"a-crte\"},\n)\n";
        let out = cargo_env_check(two, A_MANIFEST);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("a-crte"), "{}", out[0]);
    }

    #[test]
    fn a_restatement_this_scan_cannot_read_is_a_violation_not_a_skip() {
        // Written without the canonical space, or through a variable: the value
        // is still what wasm-bindgen names its snippet dirs from, and returning
        // "nothing restated" would leave it unchecked.
        for build_text in [
            "rust_library(\n    rustc_env = {\"CARGO_PKG_NAME\":\"a-crate\"},\n)\n",
            "rust_library(\n    rustc_env = {\"CARGO_PKG_NAME\": CRATE},\n)\n",
        ] {
            let out = cargo_env_check(build_text, A_MANIFEST);
            assert_eq!(out.len(), 1, "{out:?}");
            assert!(out[0].contains("cannot read"), "{}", out[0]);
        }
    }

    fn edition_check(build_editions: &BTreeSet<String>, cargo_edition: &str) -> Vec<String> {
        edition_violations_from(
            Path::new("c/BUILD.bazel"),
            Path::new("c/Cargo.toml"),
            build_editions,
            cargo_edition,
        )
    }

    #[test]
    fn every_edition_literal_in_a_build_file_is_collected() {
        assert_eq!(
            build_editions(
                "rust_library(\n    edition = \"2024\",\n)\nrust_test(\n    edition = \"2021\",\n)\n"
            ),
            editions(&["2021", "2024"])
        );
        assert!(build_editions("rust_doc_test(\n    name = \"d\",\n)\n").is_empty());
    }

    #[test]
    fn matching_editions_pass_and_a_mismatch_is_reported_per_literal() {
        assert!(edition_check(&editions(&["2024"]), "2024").is_empty());
        let out = edition_check(&editions(&["2021", "2024"]), "2024");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("edition \"2021\""), "{}", out[0]);
    }

    #[test]
    fn a_build_file_stating_no_edition_is_held_to_the_macro_default() {
        assert!(edition_check(&editions(&[]), DEFAULT_EDITION).is_empty());
        let out = edition_check(&editions(&[]), "2021");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("macro default"), "{}", out[0]);
    }

    #[test]
    fn a_virtual_workspace_manifest_has_no_package_edition() {
        assert_eq!(
            package_edition(
                Path::new("Cargo.toml"),
                "[workspace]\nmembers = [\"a\"]\nresolver = \"3\"\n"
            ),
            None
        );
    }

    #[test]
    #[should_panic(expected = "no literal `edition`")]
    fn a_package_without_a_literal_edition_panics() {
        package_edition(Path::new("c/Cargo.toml"), "[package]\nname = \"c\"\n");
    }

    #[test]
    fn the_macro_default_is_held_equal_to_the_constant() {
        let agreeing = format!(
            "def wasm_guest_library(name, edition = {DEFAULT_EDITION:?}):\n    pass\n\
             def wasm_guest_cdylib(name, edition = {DEFAULT_EDITION:?}):\n    pass\n"
        );
        let defs = MACRO_DEFS[0];
        assert!(macro_default_violations(defs, &agreeing).is_empty());

        let bumped = "def wasm_guest_library(name, edition = \"2027\"):\n    pass\n";
        let out = macro_default_violations(defs, bumped);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("2027"), "{}", out[0]);

        let gone = macro_default_violations(defs, "def wasm_guest_library(name):\n    pass\n");
        assert_eq!(gone.len(), 1, "{gone:?}");
        assert!(
            gone[0].contains("states no `edition` default"),
            "{}",
            gone[0]
        );
    }

    fn stamp_sources(
        emitted: &str,
        substituted: &str,
        read: &str,
        grepped: &str,
    ) -> (String, String, String, String) {
        (
            format!("set -euo pipefail\necho \"{emitted} ${{V}}\"\n"),
            format!("rust_binary(\n    rustc_env = {{\"X\": \"{{{substituted}}}\"}},\n)\n"),
            format!("export const STAMP_KEY = \"{read}\";\n"),
            format!("PLACEHOLDER=\"{{{grepped}}}\"\n"),
        )
    }

    #[test]
    fn a_stamp_key_written_the_same_way_in_every_file_passes() {
        let key = stamp_key();
        let (sh, build, mjs, check) = stamp_sources(&key, &key, &key, &key);
        assert!(stamp_key_violations(&key, &sh, &build, &mjs, &check).is_empty());
    }

    #[test]
    fn a_renamed_stamp_key_on_any_side_is_reported() {
        let key = stamp_key();
        let other = "STABLE_SOMETHING_ELSE";
        for (sources, needle) in [
            (stamp_sources(other, &key, &key, &key), "emits no"),
            (
                stamp_sources(&key, other, &key, &key),
                "does not substitute",
            ),
            (stamp_sources(&key, &key, other, &key), "does not read"),
            (stamp_sources(&key, &key, &key, other), "does not look for"),
        ] {
            let (sh, build, mjs, check) = sources;
            let out = stamp_key_violations(&key, &sh, &build, &mjs, &check);
            assert_eq!(out.len(), 1, "{needle}: {out:?}");
            assert!(out[0].contains(needle), "{}", out[0]);
        }
    }

    #[test]
    fn an_edition_arm_with_no_pairs_fails_instead_of_reporting_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let out = manifest_pair_violations(tmp.path(), &[PathBuf::from("README.md")]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("below the floor"), "{}", out[0]);
    }

    #[test]
    fn a_rust_string_constant_is_read_in_either_visibility() {
        assert_eq!(
            rust_str_const(
                GITLEAKS_WRAPPER,
                "use std::process::Command;\n\npub const PINNED_VERSION: &str = \"8.30.0\";\n",
                GITLEAKS_CONST,
            ),
            "8.30.0"
        );
        assert_eq!(
            rust_str_const(
                GITLEAKS_HARNESS,
                "const PINNED_VERSION: &str = \"8.30.0\";\n",
                GITLEAKS_CONST,
            ),
            "8.30.0"
        );
    }

    #[test]
    #[should_panic(expected = "declares no PINNED_VERSION")]
    fn a_source_without_the_pin_panics() {
        rust_str_const(
            GITLEAKS_WRAPPER,
            "pub const OTHER: &str = \"x\";\n",
            GITLEAKS_CONST,
        );
    }

    #[test]
    fn a_workflow_env_value_is_read_quoted_or_bare() {
        let text = "    env:\n      GITLEAKS_VERSION: 8.30.0\n      OTHER: \"yes\"\n";
        assert_eq!(workflow_env_value(text, CI_GITLEAKS_VERSION), "8.30.0");
        assert_eq!(workflow_env_value(text, "OTHER"), "yes");
    }

    #[test]
    #[should_panic(expected = "more than once")]
    fn a_workflow_stating_the_key_twice_panics() {
        workflow_env_value(
            "      GITLEAKS_VERSION: 8.30.0\n      GITLEAKS_VERSION: 8.29.0\n",
            CI_GITLEAKS_VERSION,
        );
    }

    #[test]
    #[should_panic(expected = "states no GITLEAKS_SHA256")]
    fn a_workflow_without_the_checksum_panics() {
        workflow_env_value("      GITLEAKS_VERSION: 8.30.0\n", CI_GITLEAKS_SHA);
    }

    #[test]
    fn one_gitleaks_release_across_all_four_sites_passes() {
        assert!(
            gitleaks_violations("8.30.0", "abc", "8.30.0", "8.30.0", "8.30.0", "abc").is_empty()
        );
    }

    #[test]
    fn a_gitleaks_pin_that_moved_on_one_side_is_reported() {
        for (wrapper, harness, workflow, rel) in [
            ("8.29.0", "8.30.0", "8.30.0", GITLEAKS_WRAPPER),
            ("8.30.0", "8.29.0", "8.30.0", GITLEAKS_HARNESS),
            ("8.30.0", "8.30.0", "8.29.0", CI_WORKFLOW),
        ] {
            let out = gitleaks_violations("8.30.0", "abc", wrapper, harness, workflow, "abc");
            assert_eq!(out.len(), 1, "{out:?}");
            assert!(out[0].starts_with(rel), "{}", out[0]);
            assert!(out[0].contains("8.29.0"), "{}", out[0]);
        }
    }

    #[test]
    fn two_checksums_for_one_gitleaks_asset_are_reported() {
        let out = gitleaks_violations("8.30.0", "abc", "8.30.0", "8.30.0", "8.30.0", "def");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("two different binaries"), "{}", out[0]);
    }

    const A_WORKFLOW: &str = "\
on:
  schedule:
    - cron: \"17 5 * * *\"
    - cron: '43 6 * * 1'

jobs:
  parity:
    if: github.event.schedule == '17 5 * * *' || github.event.inputs.run_it == 'true'
  canary:
    if: github.event.schedule == '43 6 * * 1'
";

    fn crons(text: &str) -> Vec<String> {
        workflow_cron_violations(&workflow_schedule_crons(text), &workflow_job_crons(text))
    }

    #[test]
    fn both_cron_sites_are_read_however_the_yaml_quotes_them() {
        let declared = workflow_schedule_crons(A_WORKFLOW);
        assert_eq!(declared, workflow_job_crons(A_WORKFLOW), "{declared:?}");
        assert_eq!(declared.len(), 2, "{declared:?}");
        assert!(declared.contains("17 5 * * *"), "{declared:?}");
    }

    #[test]
    fn a_workflow_whose_two_cron_sites_agree_passes() {
        assert!(crons(A_WORKFLOW).is_empty());
    }

    #[test]
    fn a_schedule_moved_on_one_side_only_is_reported_in_both_directions() {
        // The edit that makes a comparison job stop running: the trigger moves,
        // the condition that names it does not.
        let out = crons(&A_WORKFLOW.replace("\"17 5 * * *\"", "\"18 5 * * *\""));
        assert_eq!(out.len(), 2, "{out:?}");
        assert!(out[0].contains("never runs again"), "{}", out[0]);
        assert!(out[0].contains("17 5 * * *"), "{}", out[0]);
        assert!(out[1].contains("every job skips"), "{}", out[1]);
        assert!(out[1].contains("18 5 * * *"), "{}", out[1]);
    }

    #[test]
    fn a_workflow_with_no_schedule_at_all_fails_the_floor() {
        let out = crons("on:\n  pull_request:\n\njobs:\n  check:\n");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("below the floor"), "{}", out[0]);
    }

    #[test]
    #[should_panic(expected = "cannot read as a single-quoted literal")]
    fn a_cron_condition_this_guard_cannot_read_panics() {
        workflow_job_crons("    if: github.event.schedule == env.CRON\n");
    }

    const A_DISPATCH_WORKFLOW: &str = "\
on:
  push:
    branches: [main]
  pull_request:
  workflow_dispatch:
    inputs:
      run_comparisons:
        description: \"Also run the comparison jobs\"
        type: boolean
        default: false

jobs:
  parity:
    if: github.event.schedule == '17 5 * * *' || github.event.inputs.run_comparisons == 'true'
  canary:
    if: github.event.inputs.run_comparisons == 'true'
";

    fn inputs(text: &str) -> Vec<String> {
        workflow_input_violations(
            &workflow_dispatch_inputs(text),
            &workflow_input_references(text),
        )
    }

    #[test]
    fn a_dispatch_input_is_read_from_its_declaration_and_from_every_reference() {
        let declared = workflow_dispatch_inputs(A_DISPATCH_WORKFLOW);
        assert_eq!(declared.len(), 1, "{declared:?}");
        assert!(declared.contains("run_comparisons"), "{declared:?}");
        // Two references, one name: the reader is a set, and `default: false` in
        // the declaration is not an input of its own.
        assert_eq!(
            workflow_input_references(A_DISPATCH_WORKFLOW),
            declared,
            "{declared:?}"
        );
    }

    #[test]
    fn a_workflow_whose_dispatch_input_sites_agree_passes() {
        assert!(inputs(A_DISPATCH_WORKFLOW).is_empty());
    }

    #[test]
    fn an_input_renamed_on_one_side_only_is_reported_in_both_directions() {
        // The edit that leaves the manual switch dead: the declaration is
        // renamed, the conditions that read it are not.
        let out =
            inputs(&A_DISPATCH_WORKFLOW.replace("      run_comparisons:", "      comparisons:"));
        assert_eq!(out.len(), 2, "{out:?}");
        assert!(out[0].contains("does not declare"), "{}", out[0]);
        assert!(out[0].contains("run_comparisons"), "{}", out[0]);
        assert!(out[1].contains("no job reads"), "{}", out[1]);
        assert!(out[1].contains("comparisons"), "{}", out[1]);
    }

    #[test]
    fn a_workflow_with_no_dispatch_inputs_at_all_fails_the_floor() {
        let out = inputs("on:\n  pull_request:\n\njobs:\n  check:\n");
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("below the floor"), "{}", out[0]);
    }

    #[test]
    fn another_trigger_declaring_inputs_does_not_answer_for_the_dispatch_ones() {
        // `workflow_call` carries an `inputs:` block of its own, and a reader
        // that took any `inputs:` block would report its keys as dispatch
        // switches.
        let two = A_DISPATCH_WORKFLOW.replace(
            "  pull_request:\n",
            "  workflow_call:\n    inputs:\n      ref:\n        type: string\n",
        );
        assert_eq!(
            workflow_dispatch_inputs(&two),
            workflow_dispatch_inputs(A_DISPATCH_WORKFLOW)
        );
    }

    #[test]
    #[should_panic(expected = "with no input name after it")]
    fn an_input_reference_naming_nothing_panics() {
        workflow_input_references("    if: github.event.inputs. == 'true'\n");
    }

    const A_BAZELRC: &str = "\
build --disk_cache=~/.cache/brenn-bazel-disk
build:ci --disk_cache=.bazel-disk-cache
build:ci --color=no
build:cd --disk_cache=~/.build-cache/bazel-disk
";

    #[test]
    fn a_config_stanzas_flag_value_is_read_without_the_other_stanzas() {
        assert_eq!(
            bazelrc_flag_value(A_BAZELRC, CI_CONFIG, DISK_CACHE_FLAG),
            ".bazel-disk-cache"
        );
        assert_eq!(
            bazelrc_flag_value(A_BAZELRC, "cd", DISK_CACHE_FLAG),
            "~/.build-cache/bazel-disk"
        );
    }

    #[test]
    #[should_panic(expected = "states no --disk_cache")]
    fn a_config_without_the_flag_panics() {
        bazelrc_flag_value("build:ci --color=no\n", CI_CONFIG, DISK_CACHE_FLAG);
    }

    #[test]
    #[should_panic(expected = "more than once")]
    fn a_config_stating_the_flag_twice_panics() {
        bazelrc_flag_value(
            "build:ci --disk_cache=a\nbuild:ci --disk_cache=b\n",
            CI_CONFIG,
            DISK_CACHE_FLAG,
        );
    }

    #[test]
    fn the_configured_disk_cache_must_be_the_directory_the_workflow_carries() {
        let workflow = "\
      - uses: actions/cache/restore@v4
        with:
          path: .bazel-disk-cache
          key: bazel-disk-x
      - uses: actions/cache/restore@v4
        with:
          path: ~/.brenn-ci-tools
          key: tools-x
      - uses: actions/cache/save@v4
        with:
          path: .bazel-disk-cache
          key: bazel-disk-x
";
        let paths = workflow_cache_paths(workflow);
        assert_eq!(paths.len(), 3, "{paths:?}");
        assert!(ci_disk_cache_violations(".bazel-disk-cache", &paths).is_empty());

        // The restore half moved and the save half did not: one occurrence, so
        // the pair is broken even though the name still appears.
        let one = workflow_cache_paths(&workflow.replacen(
            "path: .bazel-disk-cache",
            "path: .bazel-cache",
            1,
        ));
        let out = ci_disk_cache_violations(".bazel-disk-cache", &one);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("1 cache step(s)"), "{}", out[0]);

        // And the other direction: the config moved, both workflow halves did
        // not.
        let out = ci_disk_cache_violations(".bazel-disk-cache-2", &paths);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("0 cache step(s)"), "{}", out[0]);
    }

    #[test]
    #[should_panic(expected = "cache step with an empty path")]
    fn a_cache_step_with_no_path_panics() {
        workflow_cache_paths("        with:\n          path:\n");
    }

    #[test]
    fn every_value_a_config_gives_a_flag_is_read_in_file_order() {
        assert_eq!(
            bazelrc_flag_values(
                "build:ci --disk_cache=a\nbuild:cd --disk_cache=z\nbuild:ci --disk_cache=b\n",
                CI_CONFIG,
                DISK_CACHE_FLAG,
            ),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    const A_WATERMARK_STEP: &str = "\
          cap=$(sed -n 's/^build:ci --experimental_disk_cache_gc_max_size=\\([0-9]\\{1,\\}\\)G$/\\1/p' .bazelrc | tail -n1)
";

    #[test]
    fn a_cap_the_watermark_step_can_read_is_no_violation() {
        assert!(
            ci_cache_watermark_violations(A_WATERMARK_STEP, &["8G".to_string()]).is_empty(),
            "the shipped shape has to pass"
        );
    }

    #[test]
    fn a_cap_respelled_out_of_the_steps_pattern_is_a_violation() {
        for cap in ["8192M", "7.5G", "8", "8g", "G"] {
            let out = ci_cache_watermark_violations(A_WATERMARK_STEP, &[cap.to_string()]);
            assert_eq!(out.len(), 1, "{cap}: {out:?}");
            assert!(out[0].contains("<digits>G"), "{}", out[0]);
        }
    }

    #[test]
    fn a_cap_stated_zero_or_twice_is_a_violation() {
        for caps in [vec![], vec!["8G".to_string(), "6G".to_string()]] {
            let count = caps.len();
            let out = ci_cache_watermark_violations(A_WATERMARK_STEP, &caps);
            assert_eq!(out.len(), 1, "{count}: {out:?}");
            assert!(out[0].contains(&format!("{count} time(s)")), "{}", out[0]);
        }
    }

    #[test]
    fn a_workflow_that_stopped_reading_the_cap_is_a_violation() {
        let out = ci_cache_watermark_violations("      - run: make bazel-check\n", &["8G".into()]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("delete this check with it"), "{}", out[0]);
    }
}
