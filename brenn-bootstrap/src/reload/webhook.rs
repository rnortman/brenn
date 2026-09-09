//! The webhook half of level 2: which endpoints moved.
//!
//! The baseline is the live endpoint table, not a plan field: an endpoint is
//! resolved off the document and installed in the `WebhookService`, and what
//! the process is serving right now is the only baseline that answers "would a
//! fresh boot of the candidate serve something else". Both sides are compared
//! in *resolved* form — mount, owner, ceiling, content type, urgency, replay
//! configuration and the signing secrets' bytes — so a rotated secret under an
//! unmoved document is a change, and a re-spelled block that resolves to the
//! same endpoint is not.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::config::Roots;

use brenn_lib::wasm_package::Verified;
use brenn_lib::webhook::config::ResolvedWebhookEndpoint;
use brenn_webhook::{EndpointRuntime, ReplayGuard};
use indexmap::IndexMap;

/// An endpoint present on both sides under one slug whose resolved value moved.
pub(crate) struct WebhookChange {
    /// What is serving now: the entry commit replaces, and the guard a
    /// carried-forward replay store travels on.
    pub old: Arc<EndpointRuntime>,
    /// What the candidate resolves to: the endpoint commit installs.
    pub new: Arc<ResolvedWebhookEndpoint>,
}

/// Which endpoints a reload retires, installs, or replaces.
///
/// Keyed by slug: the slug is the endpoint's identity in the table, in the
/// event router's re-lookup and in the channel address it mints, so an endpoint
/// that kept its slug and moved its mount is one entry to walk rather than a
/// removal beside an addition.
#[derive(Default)]
pub(crate) struct WebhookDelta {
    /// Endpoints the candidate declares and the process is not serving, in
    /// candidate document order.
    pub added: Vec<Arc<ResolvedWebhookEndpoint>>,
    /// Entries the process is serving and the candidate does not declare. The
    /// whole runtime, not its slug: commit takes the endpoint out of the table
    /// and empties its replay guard, and both are on the entry the comparison
    /// already held.
    pub removed: Vec<Arc<EndpointRuntime>>,
    /// Endpoints on both sides whose resolved value moved, or whose replay
    /// component's package now verifies to different bytes — in candidate
    /// document order, which is the order the status body reports and commit
    /// installs them in. `removed` is the one list with no document order to
    /// follow (its side is a hash map), so it is sorted by slug.
    pub changed: Vec<WebhookChange>,
    /// What each replay-protected candidate endpoint's package verifies to
    /// right now, keyed by slug. Read to classify a package bump under an
    /// unmoved document, and again when prepare decides whether a changed
    /// endpoint's running guard can be carried forward.
    pub releases: HashMap<String, Verified>,
}

impl WebhookDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }

    /// The removed endpoints' slugs.
    pub fn removed_slugs(&self) -> Vec<String> {
        self.removed
            .iter()
            .map(|entry| entry.slug().to_string())
            .collect()
    }
}

/// Whether the entry serving `slug` was compiled from bytes other than the ones
/// the candidate's package verifies to now.
///
/// Only a live guard can disagree: an endpoint that has no replay protection on
/// one side and has it on the other differs in its resolved value already, and
/// is classified on that.
fn replay_release_moved(entry: &EndpointRuntime, candidate_release: Option<&Verified>) -> bool {
    match (entry.replay.as_ref(), candidate_release) {
        (Some(guard), Some(release)) => !guard.verified.same_release(release),
        _ => false,
    }
}

/// Classify the live table against the candidate's resolved endpoints.
///
/// `releases` is what each replay-protected candidate endpoint's package
/// verifies to on the host right now. An endpoint whose document did not move
/// but whose package ships new bytes is `changed`, the way a `[[wasm_consumer]]`
/// over a bumped package is: a fresh boot would compile the new artifact, so a
/// reload must too.
pub(crate) fn webhook_delta(
    live: &HashMap<String, Arc<EndpointRuntime>>,
    candidate: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    releases: HashMap<String, Verified>,
) -> WebhookDelta {
    let mut delta = WebhookDelta {
        releases,
        ..WebhookDelta::default()
    };
    for (slug, endpoint) in candidate {
        match live.get(slug) {
            None => delta.added.push(Arc::clone(endpoint)),
            Some(entry) => {
                if *entry.endpoint != **endpoint
                    || replay_release_moved(entry, delta.releases.get(slug))
                {
                    delta.changed.push(WebhookChange {
                        old: Arc::clone(entry),
                        new: Arc::clone(endpoint),
                    });
                }
            }
        }
    }
    for (slug, entry) in live {
        if !candidate.contains_key(slug) {
            delta.removed.push(Arc::clone(entry));
        }
    }
    // The baseline is a hash map, so its walk order is nondeterministic.
    // Sorted by slug so downstream consumers see a stable order.
    delta.removed.sort_by(|a, b| a.slug().cmp(b.slug()));
    delta
}

/// What every replay-protected candidate endpoint's package verifies to on the
/// host right now, keyed by slug.
///
/// A root read, not a compile: the artifact's bytes are hashed and its record
/// checked, which is what `verify_consumer` does for a `[[wasm_consumer]]`. The
/// compile is prepare's step 7w, and only for the endpoints the delta says are
/// arriving.
///
/// # Panics
///
/// When no declared mount offers a components root, or when a declared
/// package is missing, cross-wired or fails its digest check. Prepare calls
/// this under `catch_quietly`, so each is an environment refusal.
pub(crate) fn replay_releases(
    candidate: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    roots: &Roots,
) -> HashMap<String, Verified> {
    let mut releases = HashMap::new();
    for (slug, endpoint) in candidate {
        let Some(rp) = endpoint.replay_protection.as_ref() else {
            continue;
        };
        let components_roots = brenn_lib::wasm_package::require_components_root(
            &roots.components_roots,
            &format!("webhook endpoint {slug:?} replay protection"),
        );
        releases.insert(
            slug.clone(),
            brenn_lib::wasm_package::verify_replay(components_roots, &rp.component, slug),
        );
    }
    releases
}

/// What prepare built for commit's webhook steps.
pub(crate) struct WebhookArrivals {
    /// One runtime per endpoint this reload installs or replaces, in the order
    /// commit installs them.
    pub runtimes: Vec<Arc<EndpointRuntime>>,
    /// The guards whose component was compiled by this reload and whose store
    /// file is not open yet. Commit opens each after every retiring holder has
    /// been dropped, and never a carried-forward guard: that store is already
    /// open and a second open panics.
    pub opening: Vec<Arc<ReplayGuard>>,
    /// The guards commit empties before any arriving store is opened: every
    /// removed endpoint's, and the old guard of every changed endpoint that got
    /// a fresh one.
    pub retiring: Vec<Arc<ReplayGuard>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::messaging::Urgency;
    use brenn_lib::webhook::config::{ResolvedReplayProtection, WebhookOwner};
    use brenn_lib::webhook::scheme::{HexFormat, SignatureAlgorithm, SignatureScheme};

    fn endpoint(slug: &str, secret: &[u8]) -> Arc<ResolvedWebhookEndpoint> {
        let mut keys = HashMap::new();
        keys.insert("k1".to_string(), secret.to_vec());
        Arc::new(ResolvedWebhookEndpoint {
            slug: slug.to_string(),
            mount: format!("/webhooks/{slug}"),
            description: None,
            transport_ceiling_bytes: 4096,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: None,
                keys: keys.into_iter().collect(),
            },
            owner: WebhookOwner::App(Arc::from("hookapp")),
            urgency: Urgency::Normal,
            replay_protection: None,
        })
    }

    fn replay_protected(slug: &str) -> Arc<ResolvedWebhookEndpoint> {
        let bare = endpoint(slug, b"s3cret");
        Arc::new(ResolvedWebhookEndpoint {
            slug: bare.slug.clone(),
            mount: bare.mount.clone(),
            description: None,
            transport_ceiling_bytes: bare.transport_ceiling_bytes,
            content_type: bare.content_type.clone(),
            scheme: bare.scheme.clone(),
            owner: bare.owner.clone(),
            urgency: bare.urgency,
            replay_protection: Some(ResolvedReplayProtection {
                component: "replay-generic".to_string(),
                store_path: std::path::PathBuf::from(format!("/state/{slug}.sqlite")),
                max_page_count: 16,
                config: HashMap::new(),
            }),
        })
    }

    fn verified(sha: &str) -> Verified {
        Verified {
            artifact: std::path::PathBuf::from("/components/replay-generic/replay.wasm"),
            root: std::path::PathBuf::from("/components"),
            world: "brenn:replay".to_string(),
            artifact_sha256: sha.to_string(),
            spec_sha256: None,
        }
    }

    /// A guard is only a holder here — no component is loaded, which is all a
    /// classification needs.
    fn guard(slug: &str, sha: &str) -> Arc<ReplayGuard> {
        Arc::new(ReplayGuard {
            store_path: std::path::PathBuf::from(format!("/state/{slug}.sqlite")),
            verified: verified(sha),
            slot: tokio::sync::Mutex::new(None),
        })
    }

    fn live(entries: Vec<Arc<EndpointRuntime>>) -> HashMap<String, Arc<EndpointRuntime>> {
        entries
            .into_iter()
            .map(|entry| (entry.slug().to_string(), entry))
            .collect()
    }

    fn candidate(
        endpoints: Vec<Arc<ResolvedWebhookEndpoint>>,
    ) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
        endpoints
            .into_iter()
            .map(|endpoint| (endpoint.slug.clone(), endpoint))
            .collect()
    }

    /// The same document on both sides moves nothing.
    #[test]
    fn an_unmoved_document_is_an_empty_delta() {
        let serving = live(vec![EndpointRuntime::new(
            endpoint("inbox", b"s3cret"),
            None,
        )]);
        let delta = webhook_delta(
            &serving,
            &candidate(vec![endpoint("inbox", b"s3cret")]),
            HashMap::new(),
        );
        assert!(delta.is_empty(), "nothing moved");
        assert!(delta.removed_slugs().is_empty());
    }

    /// The secret bytes are part of the comparison: a rotation under an
    /// otherwise identical block is exactly one changed endpoint.
    #[test]
    fn a_rotated_secret_is_one_change() {
        let serving = live(vec![EndpointRuntime::new(endpoint("inbox", b"old"), None)]);
        let delta = webhook_delta(
            &serving,
            &candidate(vec![endpoint("inbox", b"new")]),
            HashMap::new(),
        );
        assert_eq!(delta.changed.len(), 1);
        assert_eq!(delta.changed[0].new.slug, "inbox");
        assert!(delta.added.is_empty() && delta.removed.is_empty());
    }

    /// `removed` is slug-sorted whatever order the live map walks in, because
    /// the status body reads it verbatim.
    #[test]
    fn removed_slugs_are_sorted_whatever_the_baseline_walk_order() {
        let serving = live(vec![
            EndpointRuntime::new(endpoint("zulu", b"s"), None),
            EndpointRuntime::new(endpoint("alpha", b"s"), None),
            EndpointRuntime::new(endpoint("mike", b"s"), None),
        ]);
        let delta = webhook_delta(&serving, &candidate(vec![]), HashMap::new());
        assert_eq!(
            delta.removed_slugs(),
            vec!["alpha".to_string(), "mike".to_string(), "zulu".to_string()],
        );
    }

    /// `added` and `changed` follow candidate document order, which is the
    /// order the status body reports.
    #[test]
    fn added_and_changed_follow_candidate_order() {
        let serving = live(vec![EndpointRuntime::new(endpoint("mike", b"old"), None)]);
        let delta = webhook_delta(
            &serving,
            &candidate(vec![
                endpoint("zulu", b"s"),
                endpoint("mike", b"new"),
                endpoint("alpha", b"s"),
            ]),
            HashMap::new(),
        );
        let added: Vec<&str> = delta.added.iter().map(|ep| ep.slug.as_str()).collect();
        assert_eq!(added, vec!["zulu", "alpha"]);
        assert_eq!(delta.changed.len(), 1);
    }

    /// A bundle that ships new bytes under the package an unmoved
    /// `replay_protection` block names is a changed endpoint: a fresh boot
    /// would compile the new artifact, so a reload must install it too.
    #[test]
    fn a_bumped_replay_package_is_a_change_under_an_unmoved_document() {
        let serving = live(vec![EndpointRuntime::new(
            replay_protected("inbox"),
            Some(guard("inbox", &"a".repeat(64))),
        )]);
        let mut releases = HashMap::new();
        releases.insert("inbox".to_string(), verified(&"b".repeat(64)));
        let delta = webhook_delta(
            &serving,
            &candidate(vec![replay_protected("inbox")]),
            releases,
        );
        assert_eq!(delta.changed.len(), 1, "the package moved");
        assert_eq!(delta.changed[0].new.slug, "inbox");
    }

    /// The same bytes under the same document move nothing, so a re-deploy or
    /// a rollback whose artifact is identical does not hand the store over.
    #[test]
    fn an_unbumped_replay_package_is_not_a_change() {
        let serving = live(vec![EndpointRuntime::new(
            replay_protected("inbox"),
            Some(guard("inbox", &"a".repeat(64))),
        )]);
        let mut releases = HashMap::new();
        releases.insert("inbox".to_string(), verified(&"a".repeat(64)));
        let delta = webhook_delta(
            &serving,
            &candidate(vec![replay_protected("inbox")]),
            releases,
        );
        assert!(delta.is_empty(), "identical bytes are not a handover");
    }
}
