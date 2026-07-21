// Git forge push-webhook parser (brenn:processor world).
//
// The parser is the first sandboxed stage of the git webhook pipeline. It
// subscribes to per-forge webhook channels (one input port per forge) and
// republishes a normalized push event onto the repo-sync channel. Forge
// discrimination is structural: the input port names the forge, so the guest
// never sniffs headers to guess it.
//
// Grants: `ports` (publish) + `log`. Stateless — no store, config, or alert.
//
// Per new envelope on an input port:
//   1. Parse the message body as a `WebhookEnvelope`. Failure returns an
//      activation error → host quarantine + alert (an unparseable envelope is a
//      host-side bug, not traffic).
//   2. Read the forge's event-type header (case-insensitive name). The forgejo
//      port reads `X-Forgejo-Event`, falling back to `X-Gitea-Event`; the github
//      port reads `X-GitHub-Event`. Value other than `push` → info log, drop.
//      Header absent → warn log, drop (authenticated sender, wrong dialect).
//   3. Parse the forge JSON body for `repository.ssh_url` / `repository.clone_url`.
//      Malformed JSON or no non-empty URL → warn log, drop.
//   4. Publish the normalized push event to the `push-events` port.

use brenn_guest::{
    Activation, Error, MessageEnvelopeExt, Processor, WebhookEnvelope, log, publish_json,
    serde_json,
};
use serde::{Deserialize, Serialize};

/// Output port carrying normalized push events (bound to `brenn:git-repo-sync`).
const PUSH_EVENTS_PORT: &str = "push-events";
/// Input port for Forgejo webhooks (bound to `webhook:git-forgejo`).
const FORGEJO_PORT: &str = "forgejo";
/// Input port for GitHub webhooks (bound to `webhook:git-github`).
const GITHUB_PORT: &str = "github";

/// Subset of a forge push payload we extract remotes from. Both Forgejo and
/// GitHub push payloads carry `repository.ssh_url` / `repository.clone_url`
/// (GitHub's is a superset for these fields). An empty string is treated as
/// absent.
#[derive(Deserialize)]
struct ForgePayload {
    repository: ForgeRepository,
}

#[derive(Deserialize)]
struct ForgeRepository {
    ssh_url: Option<String>,
    clone_url: Option<String>,
}

/// Normalized push event published on `push-events`. `remotes` is the ordered,
/// deduplicated list of non-empty ssh-then-clone URLs. Additive schema: `v`
/// bumps only on an incompatible change.
#[derive(Serialize)]
struct PushEvent<'a> {
    v: u32,
    event: &'a str,
    forge: &'a str,
    endpoint: &'a str,
    remotes: Vec<String>,
    received_at: String,
}

struct GitForgeParser;

/// Case-insensitive header lookup. `WebhookEnvelope` headers carry lowercased
/// names, but `eq_ignore_ascii_case` keeps this robust regardless.
fn header<'a>(env: &'a WebhookEnvelope, name: &str) -> Option<&'a str> {
    env.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Resolve the forge name and event-type header value for an input port.
/// Returns `Err` for an unbound port name (host-config bug → quarantine).
fn forge_and_event<'a>(
    port: &str,
    env: &'a WebhookEnvelope,
) -> Result<(&'static str, Option<&'a str>), Error> {
    match port {
        FORGEJO_PORT => Ok((
            "forgejo",
            header(env, "x-forgejo-event").or_else(|| header(env, "x-gitea-event")),
        )),
        GITHUB_PORT => Ok(("github", header(env, "x-github-event"))),
        other => Err(Error::failed(format!(
            "git-forge-parser: envelope arrived on unknown input port {other}"
        ))),
    }
}

/// Ordered, deduplicated non-empty remotes: ssh_url first, then clone_url.
fn extract_remotes(payload: &ForgePayload) -> Vec<String> {
    let mut remotes = Vec::new();
    for url in [&payload.repository.ssh_url, &payload.repository.clone_url]
        .into_iter()
        .flatten()
    {
        let url = url.trim();
        if !url.is_empty() && !remotes.iter().any(|r| r == url) {
            remotes.push(url.to_string());
        }
    }
    remotes
}

impl Processor for GitForgeParser {
    fn receive(activation: Activation) -> Result<(), Error> {
        for window in activation.port_windows() {
            let port = window.port();
            for result in window.new_envelopes() {
                let msg = result?;
                // Step 1: an unparseable webhook envelope is a host-side bug. The
                // SDK helper also rejects a non-webhook envelope misrouted here.
                let env = msg.webhook_body()?;

                // Step 2: event-type filter.
                let (forge, event_type) = forge_and_event(port, &env)?;
                let Some(event_type) = event_type else {
                    log::warn(format!(
                        "git-forge-parser: {port} envelope (endpoint {}) missing event-type \
                         header; dropping",
                        env.endpoint_slug
                    ));
                    continue;
                };
                if event_type != "push" {
                    log::info(format!(
                        "git-forge-parser: non-push event '{event_type}' on {port}; dropping"
                    ));
                    continue;
                }

                // Step 3: parse the forge payload and extract remotes.
                let payload: ForgePayload = match serde_json::from_str(&env.body) {
                    Ok(p) => p,
                    Err(e) => {
                        log::warn(format!(
                            "git-forge-parser: {port} push payload JSON parse failed ({e}); \
                             dropping"
                        ));
                        continue;
                    }
                };
                let remotes = extract_remotes(&payload);
                if remotes.is_empty() {
                    log::warn(format!(
                        "git-forge-parser: {port} push payload has no remote URL; dropping"
                    ));
                    continue;
                }

                // Step 4: publish the normalized push event.
                let event = PushEvent {
                    v: 1,
                    event: "push",
                    forge,
                    endpoint: &env.endpoint_slug,
                    remotes,
                    received_at: env.received_at.to_rfc3339(),
                };
                publish_json(PUSH_EVENTS_PORT, &event)?;
            }
        }
        Ok(())
    }
}

brenn_guest::export_processor!(GitForgeParser);
