//! `RemoteProfile` lowering tests, driven off real resolved `[[remote]]` blocks
//! so the authoring shape and the authority answers are proved together.

use brenn_lib::messaging::config::MessagingGlobalConfig;
use brenn_lib::messaging::remote::{
    DEFAULT_REMOTE_MAX_SESSIONS, DEFAULT_REMOTE_MAX_SUBSCRIPTIONS, RemoteConfigRaw, resolve_remotes,
};

use super::*;

/// A representative fleet-driver block: a roster read at depth 1/1, the two
/// outbound conversation leaves under prefixes, and publish rights on the two
/// inbound ones.
const FLEET: &str = r#"
slug = "pod-kitchen"
token_file = "TOKEN_FILE"
grants = ["subscribe", "publish", "ephemeral_subscribe", "ephemeral_publish", "alert"]
subscribe_acl = [
  { exact  = "chat.app.home.roster", push_depth = 1, retain_depth = 1 },
  { prefix = "chat.app.home.out.",   push_depth = 8, retain_depth = 64 },
]
ephemeral_subscribe_acl = [
  { prefix = "chat.app.home.stream.", push_depth = 32, retain_depth = 32 },
]
publish_acl           = [ { prefix = "chat.app.home.in." } ]
ephemeral_publish_acl = [ { prefix = "chat.app.home.wake." } ]
"#;

/// A 0600 token file, written fresh per fixture so resolution exercises the real
/// mode-checked load rather than a stubbed token.
fn write_token() -> tempfile::NamedTempFile {
    use std::io::Write as _;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"s3cret-token\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    f
}

/// Resolve one `[[remote]]` body and lower it. The token file is returned
/// alongside so the caller holds it open for the length of the test.
fn build(body: &str) -> (RemoteProfile, tempfile::NamedTempFile) {
    let token = write_token();
    let toml = body.replace("TOKEN_FILE", &token.path().display().to_string());
    let raw: RemoteConfigRaw = toml::from_str(&toml).expect("[[remote]] block must parse");
    let resolved = resolve_remotes(&[raw], &MessagingGlobalConfig::default());
    (RemoteProfile::build(&resolved[0]), token)
}

fn fleet() -> (RemoteProfile, tempfile::NamedTempFile) {
    build(FLEET)
}

fn facts(push_depth: u64, retain_depth: u64) -> SubscriptionFacts {
    SubscriptionFacts {
        push_depth,
        retain_depth,
    }
}

/// **The matcher is the authority, not an enumeration.** A conversation id the
/// operator never wrote — and could not have, since the conversation is created
/// at runtime — is admitted at the prefix entry's ceilings, on both schemes.
#[test]
fn a_prefix_grant_admits_a_channel_no_operator_enumerated() {
    let (profile, _token) = fleet();
    assert_eq!(
        profile.subscribable("brenn:chat.app.home.out.4271"),
        Some(facts(8, 64))
    );
    assert_eq!(
        profile.subscribable("ephemeral:chat.app.home.stream.4271"),
        Some(facts(32, 32))
    );
    assert_eq!(
        profile.subscribable("brenn:chat.app.home.roster"),
        Some(facts(1, 1))
    );
}

/// **The scheme is half the question.** The ephemeral prefix and the durable
/// prefix are separate ACLs, so naming one scheme's channel under the other's
/// address answers the same "not yours" as an address nobody granted.
#[test]
fn the_two_subscribe_acls_do_not_leak_across_schemes() {
    let (profile, _token) = fleet();
    assert_eq!(profile.subscribable("ephemeral:chat.app.home.out.1"), None);
    assert_eq!(profile.subscribable("brenn:chat.app.home.stream.1"), None);
    assert_eq!(profile.subscribable("brenn:chat.app.other.out.1"), None);
    // The inbound leaves are publish rights only; nothing may subscribe them.
    assert_eq!(profile.subscribable("brenn:chat.app.home.in.1"), None);
}

/// **A confined channel is nobody's over the network.** `local:` belongs to the
/// host that holds it, and neither direction admits one however the matchers
/// read — nor does an address carrying no scheme at all.
#[test]
fn confined_and_unschemed_addresses_are_refused_in_both_directions() {
    let (profile, _token) = build(
        r#"
slug = "greedy"
token_file = "TOKEN_FILE"
grants = ["subscribe", "publish"]
subscribe_acl = [ { prefix = "a.", push_depth = 4, retain_depth = 4 } ]
publish_acl   = [ { prefix = "a." } ]
"#,
    );
    for address in ["local:a.confined", "a.bare", "mqtt:a.x", "webhook:a.x", ""] {
        assert_eq!(profile.subscribable(address), None, "subscribe {address}");
        assert!(!profile.publishable(None, address), "publish {address}");
    }
}

/// **Overlapping entries fold by max, per knob.** An operator who writes a broad
/// prefix and a deeper exact entry under it means the deeper number for that one
/// channel, and the two knobs fold independently.
#[test]
fn overlapping_entries_fold_by_max_per_knob() {
    let (profile, _token) = build(
        r#"
slug = "folder"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [
  { prefix = "chat.",     push_depth = 4,  retain_depth = 128 },
  { exact  = "chat.deep", push_depth = 32, retain_depth = 8 },
]
"#,
    );
    assert_eq!(
        profile.subscribable("brenn:chat.deep"),
        Some(facts(32, 128))
    );
    assert_eq!(
        profile.subscribable("brenn:chat.other"),
        Some(facts(4, 128))
    );
}

/// **One principal, no sub-identities.** A remote publishes as itself; a named
/// attribution is refused on a channel it would otherwise be allowed, so the
/// refusal is the attribution's and not the channel's.
#[test]
fn a_named_attribution_is_refused_on_a_granted_channel() {
    let (profile, _token) = fleet();
    assert!(profile.publishable(None, "brenn:chat.app.home.in.9"));
    assert!(!profile.publishable(Some("brain"), "brenn:chat.app.home.in.9"));
    assert_eq!(
        profile
            .admit_attribution(None)
            .map(|id| id.as_str().to_string()),
        Some("remote:pod-kitchen".to_string())
    );
    assert!(profile.admit_attribution(Some("brain")).is_none());
    assert_eq!(profile.attacher().as_str(), "remote:pod-kitchen");
}

/// **Publish rights are ACL-shaped too.** Both schemes' publish ACLs answer from
/// the resolved policy, and a channel outside them is refused.
#[test]
fn publish_answers_come_from_the_resolved_policy() {
    let (profile, _token) = fleet();
    assert!(profile.publishable(None, "ephemeral:chat.app.home.wake.9"));
    assert!(!profile.publishable(None, "brenn:chat.app.home.wake.9"));
    assert!(!profile.publishable(None, "ephemeral:chat.app.home.in.9"));
    assert!(!profile.publishable(None, "brenn:chat.app.other.in.9"));
    // The subscribe side grants this one; publishing onto it is a separate right
    // the operator did not write.
    assert!(!profile.publishable(None, "brenn:chat.app.home.roster"));
}

/// **A deprovision race is never a broken invariant.** Every channel a remote
/// touches is runtime-provisioned, so the posture is `Diagnostic` throughout —
/// there is no channel whose refusal should take the process down.
#[test]
fn every_channel_carries_the_diagnostic_posture() {
    let (profile, _token) = fleet();
    for channel in [
        "brenn:chat.app.home.in.1",
        "ephemeral:chat.app.home.wake.1",
        "brenn:nothing-granted",
    ] {
        assert_eq!(profile.publish_posture(channel), PublishPosture::Diagnostic);
    }
}

/// **The runtime entry carries the profile's ceilings, not the wire's.** Two
/// sessions of one remote must mint the same entry, so the depths come off the
/// ACL and the kind names the slug.
#[test]
fn the_runtime_entry_is_minted_from_the_acl_ceilings() {
    let (profile, _token) = fleet();
    let entry = profile
        .runtime_entry("brenn:chat.app.home.out.4271")
        .expect("a granted channel mints an entry");
    assert!(matches!(
        &entry.kind,
        SubscriberEntryKind::Remote(slug) if slug == "pod-kitchen"
    ));
    assert_eq!(entry.push_depth, Depth::Bounded(8));
    assert_eq!(entry.retain_depth, Depth::Bounded(64));
    assert_eq!(entry.noise, NoiseLevel::Metered);
    assert!(entry.wake_min.is_none(), "an attacher is eagerly woken");

    let ephemeral = profile
        .runtime_entry("ephemeral:chat.app.home.stream.7")
        .expect("a granted ephemeral channel mints an entry");
    assert_eq!(ephemeral.push_depth, Depth::Bounded(32));
    assert_eq!(ephemeral.retain_depth, Depth::Bounded(32));
}

/// **No entry without a grant.** The entry hook answers exactly where
/// `subscribable` does, so nothing the attacher may not hold can reach the
/// directory through it.
#[test]
fn an_ungranted_channel_mints_no_entry() {
    let (profile, _token) = fleet();
    assert!(
        profile
            .runtime_entry("brenn:chat.app.other.out.1")
            .is_none()
    );
    assert!(profile.runtime_entry("local:secret").is_none());
    // Publish-only channels are not subscriptions and mint nothing.
    assert!(profile.runtime_entry("brenn:chat.app.home.in.1").is_none());
}

/// **The caps are the operator's numbers, and the two session grains collapse.**
/// The account behind a remote attachment is the remote, so per-account and
/// per-attacher are one number.
#[test]
fn the_caps_come_from_config_and_the_session_grains_collapse() {
    let (profile, _token) = fleet();
    assert_eq!(
        profile.session_caps(),
        SessionCaps {
            per_attacher: DEFAULT_REMOTE_MAX_SESSIONS as usize,
            per_account: DEFAULT_REMOTE_MAX_SESSIONS as usize,
        }
    );
    assert_eq!(
        profile.max_active_subscriptions(),
        DEFAULT_REMOTE_MAX_SUBSCRIPTIONS as usize
    );

    let (tight, _token) = build(
        r#"
slug = "strict"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { prefix = "a.", push_depth = 1, retain_depth = 1 } ]
max_sessions = 1
max_subscriptions = 3
"#,
    );
    assert_eq!(
        tight.session_caps(),
        SessionCaps {
            per_attacher: 1,
            per_account: 1,
        }
    );
    assert_eq!(tight.max_active_subscriptions(), 3);
}

/// **The subscribe burst holds a full reconcile.** A remote at its subscription
/// cap that swaps its whole set — every held channel unsubscribed, every new one
/// subscribed — must not be killed for doing exactly what the roster told it to.
#[test]
fn the_subscribe_burst_admits_a_whole_reconcile() {
    let (profile, _token) = build(
        r#"
slug = "churner"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { prefix = "a.", push_depth = 1, retain_depth = 1 } ]
max_subscriptions = 40
"#,
    );
    assert_eq!(profile.subscribe_burst(), 80);
    assert!(profile.subscribe_burst() as usize >= 2 * profile.max_active_subscriptions());
}

/// **The alert plane and the publish bucket are grants, and deny-by-default.**
#[test]
fn the_alert_grant_and_publish_bucket_are_the_operators() {
    let (granted, _token) = build(
        r#"
slug = "pager"
token_file = "TOKEN_FILE"
grants = ["alert", "publish"]
publish_acl = [ { prefix = "a." } ]
publish_burst = 7
publish_per_sec = 3
"#,
    );
    assert!(granted.alert_granted());
    assert_eq!(
        granted.publish_rate(),
        PublishRate {
            burst: 7,
            per_sec: 3
        }
    );

    let (silent, _token) = build(
        r#"
slug = "quiet"
token_file = "TOKEN_FILE"
grants = ["publish"]
publish_acl = [ { prefix = "a." } ]
"#,
    );
    assert!(!silent.alert_granted());
}

/// **No parked-set mirrors without an attribution to hold one.** The seeding
/// sequence walks an empty list, and the budget scope is the operator's slug.
#[test]
fn a_remote_seeds_no_deferred_views() {
    let (profile, _token) = fleet();
    assert!(profile.deferred_view_targets().is_empty());
    assert_eq!(
        profile.attach_scope(),
        brenn_lib::messaging::AttachScope::remote("pod-kitchen")
    );
}

/// **A deprovision race may never take the server down on this route.** A
/// remote's targets are minted at runtime, so a flush entry naming one that
/// vanished under it is the operator's own topology moving, not the server
/// disagreeing with itself. This is the answer the batch path reads to choose
/// between dropping the entry and panicking.
#[test]
fn a_vanished_target_is_a_race_for_a_remote() {
    let (profile, _token) = fleet();
    assert_eq!(
        profile.missing_channel_posture(),
        MissingChannelPosture::Race
    );
}
