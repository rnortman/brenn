// Git sync consumer (brenn:processor world).
//
// The terminal stage of the git webhook pipeline. It subscribes to the
// normalized push-event channel, maps each push's remote URLs to the repo slugs
// it is configured to sync, and issues an async `git-repo-pull` tool call for
// the matched slugs. The pull result arrives later as a separate activation on
// the tool-result inbox, where the consumer logs/alerts per outcome and
// republishes an outcome event for observability and future fan-out.
//
// Grants: `ports` (publish outcomes) + `store` (monotonic call-id counter) +
// `log` + `alert` + `config` (remote→slug map) + a `git-repo-pull` tool grant.
//
// Remote→slug mapping lives here (the parser is forge-specific and
// repo-oblivious; this consumer owns the grant whose ACL is slug-vocabulary).
// It is read from the flat `[wasm_consumer.config]` table: `repo_slugs` is a
// comma-separated index and `remote:<slug>` holds each remote URL. A slug listed
// in `repo_slugs` with no `remote:<slug>` key fails the activation
// (`receive-error` → quarantine + alert: fail fast on operator misconfig).
//
// Two activation shapes, distinguished by the input port an envelope arrives on:
//
//   - `push-events`: parse the push event; match configured slugs whose remote
//     is in `event.remotes`; on a non-empty match, allocate a monotonic
//     `call_id = "pull-<seq>"` in `store` and fire one
//     `call-async("git-repo-pull", {"repos":[matched...]}, call_id)`.
//   - `tool-results`: parse the result; log/alert per repo outcome; republish
//     the per-repo array as an outcome event on `outcomes` (ok outcomes only).

use brenn_guest::{
    Activation, Error, Processor, alert, config, log, publish_json, serde_json, store, tools,
};
use serde::{Deserialize, Serialize};

/// Input port carrying normalized push events (bound to `brenn:git-repo-sync`).
const PUSH_EVENTS_PORT: &str = "push-events";
/// Input port carrying async tool results (the derived tool-result inbox).
const TOOL_RESULTS_PORT: &str = "tool-results";
/// Output port carrying pull outcome events (bound to
/// `brenn:git-repo-sync-outcomes`).
const OUTCOMES_PORT: &str = "outcomes";
/// The async tool this consumer holds a grant for.
const TOOL: &str = "git-repo-pull";

/// Store namespace + key for the monotonic call-id counter.
const SEQ_NS: &str = "seq";
const SEQ_KEY: &[u8] = b"pull-seq";

/// The normalized push event published by `git-forge-parser`. Only the
/// fields this consumer acts on are modeled; unknown extra fields are ignored
/// (additive schema evolution). `v` bumps only on an incompatible change.
#[derive(Deserialize)]
struct PushEvent {
    v: u32,
    event: String,
    remotes: Vec<String>,
}

/// The v1 tool-result envelope delivered on the tool-result inbox.
#[derive(Deserialize)]
struct ToolResult {
    v: u32,
    call_id: String,
    outcome: Outcome,
}

/// `{ok: {repos: [...]}}` on success, `{err: {kind, detail}}` on a typed error.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Outcome {
    Ok(OkOutcome),
    Err(ErrOutcome),
}

#[derive(Deserialize)]
struct OkOutcome {
    /// Per-repo result values, passed through verbatim into the outcome event.
    repos: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct ErrOutcome {
    kind: String,
    detail: String,
}

/// The per-repo fields this consumer inspects to choose a log/alert lane. Parsed
/// from each verbatim repo value; unknown fields ignored.
#[derive(Deserialize)]
struct RepoOutcome {
    slug: String,
    ok: bool,
    #[serde(default)]
    advanced: Option<bool>,
    #[serde(default)]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    detail: Option<String>,
}

/// The outcome event republished on `outcomes`. `repos` is the tool
/// result's per-repo array verbatim.
#[derive(Serialize)]
struct OutcomeEvent<'a> {
    v: u32,
    call_id: &'a str,
    repos: &'a [serde_json::Value],
}

/// Args for the `git-repo-pull` async tool.
#[derive(Serialize)]
struct PullArgs<'a> {
    repos: &'a [String],
}

struct GitSyncConsumer;

/// Read the operator's remote→slug map from config, in `repo_slugs` order.
/// Each of these is a fatal operator misconfig → `receive-error` (fail fast, the
/// same lane as the missing-key case): a slug with no `remote:<slug>` key, a
/// `remote:<slug>` value that is empty (or whitespace-only), or a slug that
/// appears more than once in `repo_slugs`. Remote values are trimmed so a padded
/// config value still matches the parser's trimmed event remotes.
fn load_repo_map() -> Result<Vec<(String, String)>, Error> {
    let raw = config::require::<String>("repo_slugs")?;
    let mut map: Vec<(String, String)> = Vec::new();
    for slug in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if map.iter().any(|(s, _)| s == slug) {
            return Err(Error::failed(format!(
                "git-sync-consumer: repo_slugs lists '{slug}' more than once"
            )));
        }
        let remote = config::get(&format!("remote:{slug}")).ok_or_else(|| {
            Error::failed(format!(
                "git-sync-consumer: repo_slugs lists '{slug}' but config has no 'remote:{slug}' key"
            ))
        })?;
        let remote = remote.trim().to_string();
        if remote.is_empty() {
            return Err(Error::failed(format!(
                "git-sync-consumer: 'remote:{slug}' is empty"
            )));
        }
        map.push((slug.to_string(), remote));
    }
    Ok(map)
}

/// Allocate the next monotonic call-id sequence number, committing it before the
/// caller buffers the tool request. The counter commits in its own durability
/// domain (a guest `commit` during the activation), so a later trap can advance
/// the counter without a request reaching the bus — a harmless sequence gap
/// (never a reuse), backstopped by the poller.
fn next_seq() -> Result<u64, Error> {
    let tx = store::begin()?;
    let prev = match tx.get(SEQ_NS, SEQ_KEY)? {
        Some(bytes) => {
            let s = String::from_utf8(bytes)
                .map_err(|e| Error::failed(format!("git-sync-consumer: seq not utf8: {e}")))?;
            s.parse::<u64>()
                .map_err(|e| Error::failed(format!("git-sync-consumer: seq not a number: {e}")))?
        }
        None => 0,
    };
    let next = prev + 1;
    tx.put(SEQ_NS, SEQ_KEY, next.to_string().as_bytes())?;
    tx.commit()?;
    Ok(next)
}

/// Handle one push event: match configured slugs and fire the async pull.
fn handle_push_event(repo_map: &[(String, String)], body: &str) -> Result<(), Error> {
    let event: PushEvent = serde_json::from_str(body)
        .map_err(|e| Error::malformed(format!("git-sync-consumer: push event JSON: {e}")))?;
    if event.v != 1 {
        return Err(Error::failed(format!(
            "git-sync-consumer: unknown push-event schema version {}",
            event.v
        )));
    }
    if event.event != "push" {
        log::info(format!(
            "git-sync-consumer: non-push event '{}' on push-events; dropping",
            event.event
        ));
        return Ok(());
    }

    let matched: Vec<String> = repo_map
        .iter()
        .filter(|(_, remote)| event.remotes.iter().any(|r| r == remote))
        .map(|(slug, _)| slug.clone())
        .collect();
    if matched.is_empty() {
        log::info(format!(
            "git-sync-consumer: push for unconfigured remote(s) {:?}; dropping",
            event.remotes
        ));
        return Ok(());
    }

    let seq = next_seq()?;
    let call_id = format!("pull-{seq}");
    tools::call_async_json(TOOL, &PullArgs { repos: &matched }, &call_id)?;
    log::info(format!(
        "git-sync-consumer: {call_id} → git-repo-pull for {matched:?}"
    ));
    Ok(())
}

/// Log/alert one repo outcome per its result lane.
fn report_repo(repo: &serde_json::Value) -> Result<(), Error> {
    let r = RepoOutcome::deserialize(repo)
        .map_err(|e| Error::malformed(format!("git-sync-consumer: repo outcome JSON: {e}")))?;
    if r.ok {
        match r.advanced {
            Some(true) => log::info(format!("git-sync-consumer: {} advanced", r.slug)),
            _ => log::debug(format!("git-sync-consumer: {} up-to-date", r.slug)),
        }
        return Ok(());
    }
    let error_type = r.error_type.as_deref().unwrap_or("unknown");
    let error = r.error.as_deref().unwrap_or("");
    let detail = r.detail.as_deref().unwrap_or("");
    match error_type {
        // Transient network blips: the poller retries on its own clock, so
        // alerting on each would be noise — warn log only.
        "transient" => log::warn(format!(
            "git-sync-consumer: {} transient pull error: {error}",
            r.slug
        )),
        // Auth/conflict/unknown all require human action (credentials,
        // non-ff divergence, or a stale remote→slug map naming an absent slug).
        _ => alert::alert(
            alert::Severity::Warning,
            format!("git-repo-pull {error_type} for {}", r.slug),
            format!("{error} {detail}"),
        ),
    }
    Ok(())
}

/// Handle one tool result: report each repo, then republish the outcome event.
fn handle_tool_result(body: &str) -> Result<(), Error> {
    let result: ToolResult = serde_json::from_str(body)
        .map_err(|e| Error::malformed(format!("git-sync-consumer: tool result JSON: {e}")))?;
    if result.v != 1 {
        return Err(Error::failed(format!(
            "git-sync-consumer: unknown tool-result schema version {}",
            result.v
        )));
    }
    match result.outcome {
        Outcome::Err(e) => {
            // Revoked grant or internal tool failure — operator-actionable.
            alert::alert(
                alert::Severity::Warning,
                format!("git-repo-pull call failed ({})", e.kind),
                format!("call_id={} detail={}", result.call_id, e.detail),
            );
        }
        Outcome::Ok(ok) => {
            for repo in &ok.repos {
                report_repo(repo)?;
            }
            publish_json(
                OUTCOMES_PORT,
                &OutcomeEvent {
                    v: 1,
                    call_id: &result.call_id,
                    repos: &ok.repos,
                },
            )?;
        }
    }
    Ok(())
}

impl Processor for GitSyncConsumer {
    fn receive(activation: Activation) -> Result<(), Error> {
        // Read once per activation; config is process-lifetime-fixed. A missing
        // remote key fails the whole activation (fail-fast misconfig).
        let repo_map = load_repo_map()?;
        for window in activation.port_windows() {
            let port = window.port();
            for result in window.new_envelopes() {
                let env = result?;
                match port {
                    PUSH_EVENTS_PORT => handle_push_event(&repo_map, &env.body)?,
                    TOOL_RESULTS_PORT => handle_tool_result(&env.body)?,
                    other => {
                        return Err(Error::failed(format!(
                            "git-sync-consumer: envelope on unknown input port {other}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

brenn_guest::export_processor!(GitSyncConsumer);
