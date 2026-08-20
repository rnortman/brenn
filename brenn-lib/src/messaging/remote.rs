//! `[[remote]]` — the authenticated native-daemon attacher.
//!
//! A remote is the second `AttachProfile` principal: same wire contract, same
//! session machinery, and the same deny-by-default posture as a browser
//! surface, differing in the four places a non-browser attacher has to differ —
//! authentication (a bearer token from a mode-checked file rather than a session
//! cookie), authority lowering, session-cap posture, and the absence of any
//! deployment coupling to served assets.
//!
//! This module owns the operator-facing block and its boot resolution. The
//! authoring shape parts from `[[surface]]` in one way: a surface enumerates the
//! channels it binds, so its ACLs need no depths, while a remote subscribes at
//! runtime to channels that come into being at runtime, so every
//! subscribe-direction matcher states the `push_depth`/`retain_depth` ceiling
//! the route answers a subscribe with. There are no silent defaults there: what
//! a network principal may hold is the operator's sentence to write.

use std::path::PathBuf;

use serde::Deserialize;

use crate::access::AppPolicy;
use crate::access::acl::ChannelMatcher;
use crate::access::raw::ChannelMatcherRaw;
use crate::messaging::config::{
    DEFAULT_SURFACE_PUBLISH_BURST, DEFAULT_SURFACE_PUBLISH_PER_SEC, MessagingGlobalConfig,
};

/// Default concurrent sessions admitted per `[[remote]]` when `max_sessions` is
/// unset.
///
/// Two, so a reconnect after a netsplit lands in the free slot while the
/// half-dead session drains out through the heartbeat watchdog. Sessions are
/// safe to overlap by construction — cursors are client-held, subscriptions are
/// per-session, and no consumption state is shared — so the second slot costs
/// nothing but the send budget the two already share.
pub const DEFAULT_REMOTE_MAX_SESSIONS: u32 = 2;

/// Default concurrent subscriptions admitted per remote session when
/// `max_subscriptions` is unset.
///
/// The cap exists because a prefix-granted attacher's active-subscription state
/// is otherwise bounded only by how many channels its ACL ever matches. 256 is
/// far above the fleet-driver case (two channels per conversation) and far below
/// anything that strains a session's per-subscription bookkeeping.
pub const DEFAULT_REMOTE_MAX_SUBSCRIPTIONS: u32 = 256;

/// Grantable transport rights for a `[[remote]]` bus participant
/// (operator-facing).
///
/// The `[[surface]]` vocabulary minus `takeover`: a fullscreen overlay is a
/// rendering-application capability, and a daemon has nothing to render. Every
/// other right is the same right — one token per delivery class × direction,
/// plus the alert plane — so an operator who can read a `[[surface]]` block can
/// read this one.
///
/// Serde `snake_case`, matching `SurfaceGrant`'s authoring tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteGrant {
    /// Durable (`brenn:`) delivery to the remote. Maps to `MessagingSubscribe`.
    Subscribe,
    /// Durable (`brenn:`) publish from the remote. Maps to `MessagingPublish`.
    Publish,
    /// Ephemeral (`ephemeral:`) delivery to the remote. Maps to `EphemeralSubscribe`.
    EphemeralSubscribe,
    /// Ephemeral (`ephemeral:`) publish from the remote. Maps to `EphemeralPublish`.
    EphemeralPublish,
    /// Alert (phone/operator paging) emission from the remote. Deny-by-default:
    /// without this grant the remote has no alert plane.
    Alert,
}

/// One subscribe-direction ACL entry: a channel matcher plus the depth ceilings
/// the route answers a matching subscribe with.
///
/// Authored flat — `{ prefix = "chat.app.home.out.", push_depth = 8,
/// retain_depth = 64 }` — so the matcher reads the same as everywhere else in
/// the config while carrying the two numbers a runtime-minted subscription
/// cannot inherit from a `[[channel]]` block it may not have. Exactly one of
/// `exact`/`prefix` must be present; resolution rejects both and neither.
///
/// Depths are plain integers rather than the `Depth` ladder: `"unbounded"` is
/// not an answer a B7 network principal may be given, so the type does not
/// offer it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteSubscribeAclRaw {
    /// `exact = "channel"` — matches this channel and no other.
    #[serde(default)]
    pub exact: Option<String>,
    /// `prefix = "channel-prefix."` — matches every channel under this prefix.
    /// Must end at a segment boundary, as everywhere else.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Push-row depth ceiling for a matching subscription. `0` is legal and
    /// means pull-only: the remote reads through its cursor and no live copy is
    /// queued for it.
    pub push_depth: u64,
    /// Retained-replay depth ceiling for a matching subscription. At least 1 —
    /// a zero-retention subscription would have nothing to resume a cursor
    /// against.
    pub retain_depth: u64,
}

/// Top-level `[[remote]]` block.
///
/// Declares an authenticated native daemon as an ACL-bounded bus participant.
/// `grants` is required with no default, exactly as `[[surface]]` and
/// `[[wasm_consumer]]`: the operator states intent, and deny-by-default reads
/// straight off the config. `deny_unknown_fields` here and on the nested ACL
/// entry closes the door on typos.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteConfigRaw {
    /// Globally unique slug; becomes `remote:<slug>` as the participant
    /// identity and the attach registry key. Charset enforced at resolution:
    /// `[A-Za-z0-9._~-]+`, no `:`/`@`/`#`.
    pub slug: String,
    /// Path to the file holding this remote's bearer token. Read at boot;
    /// missing, unreadable, empty, or group/world-accessible is a boot panic.
    pub token_file: PathBuf,
    /// Transport rights for this remote (deny-by-default). Required — no default.
    pub grants: Vec<RemoteGrant>,
    /// Durable (`brenn:`) subscribe ACL — bare channel names, no scheme.
    #[serde(default)]
    pub subscribe_acl: Vec<RemoteSubscribeAclRaw>,
    /// Ephemeral (`ephemeral:`) subscribe ACL — bare channel names, no scheme.
    #[serde(default)]
    pub ephemeral_subscribe_acl: Vec<RemoteSubscribeAclRaw>,
    /// Durable (`brenn:`) publish ACL — bare channel names, no scheme. No
    /// depths: a publish target has no queue of the publisher's to size.
    #[serde(default)]
    pub publish_acl: Vec<ChannelMatcherRaw>,
    /// Ephemeral (`ephemeral:`) publish ACL — bare channel names, no scheme.
    #[serde(default)]
    pub ephemeral_publish_acl: Vec<ChannelMatcherRaw>,
    /// Per-connection publish burst (tokens). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_BURST` — the same default and the same ceiling
    /// rule as a surface's, because it is the same bucket in front of the same
    /// bus gate.
    #[serde(default)]
    pub publish_burst: Option<u32>,
    /// Per-connection sustained publish refill (tokens/sec). Absent =
    /// `DEFAULT_SURFACE_PUBLISH_PER_SEC`.
    #[serde(default)]
    pub publish_per_sec: Option<u32>,
    /// Concurrent sessions admitted for this remote. Absent =
    /// [`DEFAULT_REMOTE_MAX_SESSIONS`]. `1` is the strict single-connection
    /// posture, at the price of up to one watchdog interval of lockout after a
    /// netsplit.
    #[serde(default)]
    pub max_sessions: Option<u32>,
    /// Concurrent subscriptions admitted per session. Absent =
    /// [`DEFAULT_REMOTE_MAX_SUBSCRIPTIONS`].
    #[serde(default)]
    pub max_subscriptions: Option<u32>,
}

/// A remote's bearer token, loaded from its `token_file`.
///
/// Stored as the SHA-256 digest of the token, never the plaintext: the secret
/// stops being process-resident once the constructor returns, and every
/// comparison is between two 32-byte values. That is what makes verification
/// timing independent of *both* tokens' lengths — the stored side is a fixed
/// width by construction, so no length class of the configured token is
/// distinguishable, and an unmatchable dummy costs exactly what a real token
/// costs.
///
/// Comparison is the only way out of the type: there is no accessor returning
/// the secret, and `Debug` renders neither the bytes nor the length, so a token
/// cannot reach a log through a derived format of any struct that holds one.
///
/// `PartialEq` is written rather than derived, and routes through the same
/// comparison [`RemoteToken::verify`] does. A derived one would be `[u8; 32]`'s
/// short-circuiting compare, which makes `RemoteToken::new(presented) == expected`
/// — the most natural spelling an auth path could reach for — a byte-position
/// oracle on the digest.
#[derive(Clone, Eq)]
pub struct RemoteToken([u8; 32]);

impl PartialEq for RemoteToken {
    fn eq(&self, other: &Self) -> bool {
        crate::util::ct_eq_bytes(&self.0, &other.0)
    }
}

impl RemoteToken {
    /// Wrap an already-loaded token, digesting it on the way in.
    pub fn new(token: impl AsRef<str>) -> Self {
        Self(crate::util::sha256(token.as_ref().as_bytes()))
    }

    /// A token no presentable credential can match.
    ///
    /// The digest is all zeros — not the digest of any string — so matching it
    /// would require a SHA-256 preimage of `0x00…00`. Callers use it to give an
    /// unknown principal the same comparison work as a known one.
    pub const fn unmatchable() -> Self {
        Self([0u8; 32])
    }

    /// Constant-time equality against a presented credential.
    ///
    /// Both sides are 32-byte digests, so the work is identical whatever either
    /// token's length: no byte position and no length class leaks.
    pub fn verify(&self, presented: &str) -> bool {
        crate::util::ct_eq_bytes(&self.0, &crate::util::sha256(presented.as_bytes()))
    }
}

impl std::fmt::Debug for RemoteToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteToken(<redacted>)")
    }
}

/// One resolved subscribe-direction ACL entry: which channels, and how deep the
/// route may let a matching subscription go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSubscribeCeiling {
    /// The resolved, validated matcher.
    pub matcher: ChannelMatcher,
    /// Push-row depth ceiling for a matching subscription.
    pub push_depth: u64,
    /// Retained-replay depth ceiling for a matching subscription.
    pub retain_depth: u64,
}

/// The depths a matching subscribe is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteDepths {
    /// Push-row depth ceiling.
    pub push_depth: u64,
    /// Retained-replay depth ceiling.
    pub retain_depth: u64,
}

/// One direction's subscribe ACL: the matchers, and the fold that answers a
/// channel.
///
/// The fold is **max over every matching entry**, the surface's rule: an
/// operator who writes a broad prefix and then a deeper exact entry for one
/// channel under it means the deeper number for that channel, not "whichever
/// entry happens to be listed first".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteSubscribeAcl(pub Vec<RemoteSubscribeCeiling>);

impl RemoteSubscribeAcl {
    /// The ceiling this ACL answers `channel` (a bare, scheme-stripped name)
    /// with, or `None` when no entry matches — which is the ACL saying "not
    /// yours", indistinguishable to the attacher from "no such channel".
    ///
    /// Total and I/O-free: this is what the route's profile answers a subscribe
    /// from.
    pub fn ceiling_for(&self, channel: &str) -> Option<RemoteDepths> {
        self.0
            .iter()
            .filter(|entry| entry.matcher.matches(channel))
            .fold(None, |acc: Option<RemoteDepths>, entry| {
                Some(match acc {
                    None => RemoteDepths {
                        push_depth: entry.push_depth,
                        retain_depth: entry.retain_depth,
                    },
                    Some(prev) => RemoteDepths {
                        push_depth: prev.push_depth.max(entry.push_depth),
                        retain_depth: prev.retain_depth.max(entry.retain_depth),
                    },
                })
            })
    }

    /// Whether this ACL authorizes nothing at all.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A fully resolved, boot-cross-validated `[[remote]]` block.
#[derive(Debug, Clone)]
pub struct ResolvedRemote {
    /// `remote:<slug>` participant identity source, and the attach registry key
    /// once prefixed.
    pub slug: String,
    /// The expected bearer token, loaded from the mode-checked `token_file`.
    pub token: RemoteToken,
    /// Resolved access-control policy (grants + ACLs), built via
    /// `build_remote_policy`. The authority the session lowers to.
    pub policy: AppPolicy,
    /// Durable subscribe ceilings, in declaration order.
    pub subscribe_ceilings: RemoteSubscribeAcl,
    /// Ephemeral subscribe ceilings, in declaration order.
    pub ephemeral_subscribe_ceilings: RemoteSubscribeAcl,
    /// Per-connection publish token-bucket burst (tokens), default applied.
    pub publish_burst: u32,
    /// Per-connection publish token-bucket sustained refill (tokens/sec),
    /// default applied.
    pub publish_per_sec: u32,
    /// Concurrent sessions admitted, default applied.
    pub max_sessions: u32,
    /// Concurrent subscriptions admitted per session, default applied.
    pub max_subscriptions: u32,
}

impl ResolvedRemote {
    /// The ceiling this remote's ACLs answer a durable (`brenn:`) subscribe of
    /// the bare `channel` with.
    pub fn durable_ceiling(&self, channel: &str) -> Option<RemoteDepths> {
        self.subscribe_ceilings.ceiling_for(channel)
    }

    /// The ceiling this remote's ACLs answer an ephemeral subscribe of the bare
    /// `channel` with.
    pub fn ephemeral_ceiling(&self, channel: &str) -> Option<RemoteDepths> {
        self.ephemeral_subscribe_ceilings.ceiling_for(channel)
    }
}

/// Resolve and cross-validate every `[[remote]]` block, loading each one's
/// bearer token.
///
/// Fail-fast on operator-authored config, as every other resolver here:
///
/// 1. A duplicate or charset-invalid slug. `#` is rejected along with `:`/`@`
///    so a remote slug can never compose into the `surface:<slug>#<instance>`
///    shape a participant id parser would recover differently.
/// 2. A subscribe ACL entry naming neither or both of `exact`/`prefix`, a
///    `retain_depth` of zero, or a matcher `resolve_channel` rejects.
/// 3. **Grant/ACL inconsistency, both directions**: a granted scheme×direction
///    right with an empty ACL list, or a non-empty ACL list whose right is not
///    granted. This is deliberately stricter than the surface and WASM paths,
///    which let the two-factor check deny quietly. For a network principal, a
///    dead grant and an orphaned ACL are both config bugs an operator wants to
///    hear about at boot rather than diagnose from a refused subscribe. `alert`
///    has no ACL dimension and is exempt.
/// 4. A zero or over-ceiling publish rate, by the same rule and for the same
///    layering reason as `[[surface]]`: the per-connection bucket must trip no
///    later than the bus per-sender gate.
/// 5. A zero `max_sessions` (a remote that may never attach is dead config) or
///    zero `max_subscriptions` (likewise for one that may never subscribe).
/// 6. A token file that is missing, unreadable, empty, or readable by any other
///    local account.
///
/// Slug disjointness against `[[surface]]` is deliberately *not* checked: the
/// registry key and the participant id both carry the `remote:` prefix, so the
/// two keyspaces cannot collide, and forbidding a pod and a page from sharing an
/// operator's name for one deployment would be a rule with no failure behind it.
///
/// # Panics
///
/// On any of the above.
pub fn resolve_remotes(
    raw_remotes: &[RemoteConfigRaw],
    globals: &MessagingGlobalConfig,
) -> Vec<ResolvedRemote> {
    use std::collections::HashSet;

    let mut seen: HashSet<&str> = HashSet::new();
    for r in raw_remotes {
        assert!(
            seen.insert(r.slug.as_str()),
            "config: duplicate [[remote]] slug {:?} — each remote slug must be unique",
            r.slug,
        );
    }

    raw_remotes
        .iter()
        .map(|remote| resolve_remote(remote, globals))
        .collect()
}

fn resolve_remote(remote: &RemoteConfigRaw, globals: &MessagingGlobalConfig) -> ResolvedRemote {
    use crate::messaging::is_unreserved_char;

    let slug = &remote.slug;
    assert!(
        !slug.is_empty(),
        "config: [[remote]] slug must be non-empty"
    );
    assert!(
        slug.chars().all(is_unreserved_char),
        "config: [[remote]] slug {slug:?} must consist of RFC 3986 unreserved characters only \
         (A-Za-z0-9._~-)",
    );

    let subscribe_matchers = lower_subscribe_matchers(slug, "subscribe_acl", &remote.subscribe_acl);
    let ephemeral_subscribe_matchers = lower_subscribe_matchers(
        slug,
        "ephemeral_subscribe_acl",
        &remote.ephemeral_subscribe_acl,
    );

    let policy = crate::access::resolve::build_remote_policy(
        slug,
        remote.grants.iter().copied(),
        &subscribe_matchers,
        &remote.publish_acl,
        &ephemeral_subscribe_matchers,
        &remote.ephemeral_publish_acl,
    );

    assert_grants_and_acls_agree(remote);
    let (publish_burst, publish_per_sec) = resolve_publish_rate(remote, globals);

    let max_sessions = remote.max_sessions.unwrap_or(DEFAULT_REMOTE_MAX_SESSIONS);
    assert!(
        max_sessions >= 1,
        "config: [[remote]] {slug:?}: max_sessions must be >= 1 — a remote that may never \
         attach is dead config; delete the block instead",
    );
    let max_subscriptions = remote
        .max_subscriptions
        .unwrap_or(DEFAULT_REMOTE_MAX_SUBSCRIPTIONS);
    assert!(
        max_subscriptions >= 1,
        "config: [[remote]] {slug:?}: max_subscriptions must be >= 1 — a remote that may never \
         subscribe holds only publish rights; drop the subscribe grants instead",
    );

    let token = load_remote_token(slug, &remote.token_file);

    ResolvedRemote {
        slug: slug.clone(),
        token,
        policy: policy.clone(),
        subscribe_ceilings: zip_ceilings(&policy.acls.brenn_subscribe, &remote.subscribe_acl),
        ephemeral_subscribe_ceilings: zip_ceilings(
            &policy.acls.ephemeral_subscribe,
            &remote.ephemeral_subscribe_acl,
        ),
        publish_burst,
        publish_per_sec,
        max_sessions,
        max_subscriptions,
    }
}

/// Convert the depth-carrying authoring shape into the matcher vocabulary the
/// policy lowering takes, validating the one-of rule and the retain floor.
///
/// The depths ride alongside in declaration order and are zipped back onto the
/// resolved matchers afterwards ([`zip_ceilings`]), so the matcher validation
/// stays in the single place every other ACL list is validated.
fn lower_subscribe_matchers(
    slug: &str,
    list: &str,
    entries: &[RemoteSubscribeAclRaw],
) -> Vec<ChannelMatcherRaw> {
    entries
        .iter()
        .map(|entry| {
            assert!(
                entry.retain_depth >= 1,
                "config: [[remote]] {slug:?}: {list} entry has retain_depth = 0 — a subscription \
                 retaining nothing has no window for a cursor to resume against; give it a depth \
                 or drop the entry",
            );
            match (&entry.exact, &entry.prefix) {
                (Some(exact), None) => ChannelMatcherRaw::Exact(exact.clone()),
                (None, Some(prefix)) => ChannelMatcherRaw::Prefix(prefix.clone()),
                (Some(_), Some(_)) => panic!(
                    "config: [[remote]] {slug:?}: {list} entry sets both exact and prefix — a \
                     matcher is one or the other",
                ),
                (None, None) => panic!(
                    "config: [[remote]] {slug:?}: {list} entry sets neither exact nor prefix — \
                     every matcher names what it matches",
                ),
            }
        })
        .collect()
}

/// Pair each resolved matcher with the depths its authoring entry carried.
/// Order-dependent by construction: `lower_subscribe_matchers` preserves
/// declaration order and `build_remote_policy` maps one matcher to one.
fn zip_ceilings(
    resolved: &[ChannelMatcher],
    entries: &[RemoteSubscribeAclRaw],
) -> RemoteSubscribeAcl {
    assert_eq!(
        resolved.len(),
        entries.len(),
        "remote ACL lowering lost an entry — resolution maps one matcher to one authored entry",
    );
    RemoteSubscribeAcl(
        resolved
            .iter()
            .zip(entries)
            .map(|(matcher, entry)| RemoteSubscribeCeiling {
                matcher: matcher.clone(),
                push_depth: entry.push_depth,
                retain_depth: entry.retain_depth,
            })
            .collect(),
    )
}

/// Boot-panic on any grant without its ACL list, or any ACL list without its
/// grant.
fn assert_grants_and_acls_agree(remote: &RemoteConfigRaw) {
    let slug = &remote.slug;
    let checks: [(RemoteGrant, &str, bool); 4] = [
        (
            RemoteGrant::Subscribe,
            "subscribe_acl",
            !remote.subscribe_acl.is_empty(),
        ),
        (
            RemoteGrant::Publish,
            "publish_acl",
            !remote.publish_acl.is_empty(),
        ),
        (
            RemoteGrant::EphemeralSubscribe,
            "ephemeral_subscribe_acl",
            !remote.ephemeral_subscribe_acl.is_empty(),
        ),
        (
            RemoteGrant::EphemeralPublish,
            "ephemeral_publish_acl",
            !remote.ephemeral_publish_acl.is_empty(),
        ),
    ];
    for (grant, list, acl_present) in checks {
        let granted = remote.grants.contains(&grant);
        assert!(
            !granted || acl_present,
            "config: [[remote]] {slug:?}: grant {grant:?} is authored but {list} is empty — the \
             right would authorize nothing; either list the channels or drop the grant",
        );
        assert!(
            !acl_present || granted,
            "config: [[remote]] {slug:?}: {list} lists channels but the matching grant \
             {grant:?} is not authored — the ACL would never be consulted; either grant the \
             right or drop the list",
        );
    }
}

/// The per-connection publish bucket, defaults applied and both layering
/// ceilings enforced against the global send rate.
fn resolve_publish_rate(remote: &RemoteConfigRaw, globals: &MessagingGlobalConfig) -> (u32, u32) {
    let slug = &remote.slug;
    let send_rate = globals.default_send_rate;
    // The per-second comparison below is unit-valid only while the refill
    // interval is one second — the same assertion the surface path makes.
    assert_eq!(
        send_rate.refill_interval_secs, 1,
        "config: [messaging].default_send_rate.refill_interval_secs must be 1 — the per-remote \
         publish_per_sec ceiling is compared against `refill` as a per-second rate",
    );
    let publish_burst = remote
        .publish_burst
        .unwrap_or(DEFAULT_SURFACE_PUBLISH_BURST);
    let publish_per_sec = remote
        .publish_per_sec
        .unwrap_or(DEFAULT_SURFACE_PUBLISH_PER_SEC);
    assert!(
        publish_burst >= 1,
        "config: [[remote]] {slug:?}: publish_burst must be >= 1 (a zero budget with publish \
         grants is a contradiction; omit the grants instead)",
    );
    assert!(
        publish_per_sec >= 1,
        "config: [[remote]] {slug:?}: publish_per_sec must be >= 1 (a zero budget with publish \
         grants is a contradiction; omit the grants instead)",
    );
    assert!(
        publish_burst <= send_rate.burst,
        "config: [[remote]] {slug:?}: publish_burst {publish_burst} exceeds the default \
         send-rate burst ({}); the per-connection bucket must trip first",
        send_rate.burst,
    );
    assert!(
        publish_per_sec <= send_rate.refill,
        "config: [[remote]] {slug:?}: publish_per_sec {publish_per_sec} exceeds the default send \
         rate ({}/s); the per-connection bucket must trip first",
        send_rate.refill,
    );
    (publish_burst, publish_per_sec)
}

/// Load a remote's bearer token: the shared secret-file reader, plus the mode
/// check a credential that authenticates a network principal earns.
fn load_remote_token(slug: &str, path: &std::path::Path) -> RemoteToken {
    let label = format!("[[remote]] {slug:?} token_file");
    RemoteToken::new(crate::config::load_secret_file_private(&label, path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::AppCapability;

    fn write_token(contents: &str) -> tempfile::NamedTempFile {
        use std::io::Write as _;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        f
    }

    /// Parse a `[[remote]]` block, substituting `TOKEN_FILE` in the TOML with a
    /// freshly written 0600 token file so every test exercises the real load.
    fn parse(toml_body: &str) -> (RemoteConfigRaw, tempfile::NamedTempFile) {
        let token = write_token("s3cret-token\n");
        let body = toml_body.replace("TOKEN_FILE", &token.path().display().to_string());
        let raw: RemoteConfigRaw = toml::from_str(&body).expect("[[remote]] block must parse");
        (raw, token)
    }

    fn resolve_one(toml_body: &str) -> ResolvedRemote {
        let (raw, _token) = parse(toml_body);
        let mut resolved = resolve_remotes(&[raw], &MessagingGlobalConfig::default());
        resolved.pop().expect("one resolved remote")
    }

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

    #[test]
    fn the_fleet_driver_block_resolves() {
        let remote = resolve_one(FLEET);
        assert_eq!(remote.slug, "pod-kitchen");
        assert!(remote.policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(remote.policy.has_grant(AppCapability::MessagingPublish));
        assert!(remote.policy.has_grant(AppCapability::EphemeralSubscribe));
        assert!(remote.policy.has_grant(AppCapability::EphemeralPublish));
        assert!(remote.policy.has_grant(AppCapability::SurfaceAlert));
        // Takeover has no remote token, so no path grants it.
        assert!(!remote.policy.has_grant(AppCapability::SurfaceTakeover));
        assert_eq!(
            remote.durable_ceiling("chat.app.home.roster"),
            Some(RemoteDepths {
                push_depth: 1,
                retain_depth: 1
            })
        );
        assert_eq!(
            remote.durable_ceiling("chat.app.home.out.42"),
            Some(RemoteDepths {
                push_depth: 8,
                retain_depth: 64
            })
        );
        assert_eq!(
            remote.ephemeral_ceiling("chat.app.home.stream.42"),
            Some(RemoteDepths {
                push_depth: 32,
                retain_depth: 32
            })
        );
        assert_eq!(remote.durable_ceiling("chat.app.other.out.1"), None);
        // The publish ACLs are the policy's, unchanged by the depth detour.
        assert!(remote.policy.allows_brenn_publish("chat.app.home.in.42"));
        assert!(!remote.policy.allows_brenn_publish("chat.app.other.in.42"));
        assert_eq!(remote.max_sessions, DEFAULT_REMOTE_MAX_SESSIONS);
        assert_eq!(remote.max_subscriptions, DEFAULT_REMOTE_MAX_SUBSCRIPTIONS);
        assert_eq!(remote.publish_burst, DEFAULT_SURFACE_PUBLISH_BURST);
        assert_eq!(remote.publish_per_sec, DEFAULT_SURFACE_PUBLISH_PER_SEC);
    }

    #[test]
    fn overlapping_matchers_fold_by_max() {
        let acl = RemoteSubscribeAcl(vec![
            RemoteSubscribeCeiling {
                matcher: ChannelMatcher::Prefix("chat.".to_string()),
                push_depth: 4,
                retain_depth: 128,
            },
            RemoteSubscribeCeiling {
                matcher: ChannelMatcher::Exact("chat.deep".to_string()),
                push_depth: 32,
                retain_depth: 8,
            },
        ]);
        assert_eq!(
            acl.ceiling_for("chat.deep"),
            Some(RemoteDepths {
                push_depth: 32,
                retain_depth: 128
            })
        );
        assert_eq!(
            acl.ceiling_for("chat.shallow"),
            Some(RemoteDepths {
                push_depth: 4,
                retain_depth: 128
            })
        );
        assert_eq!(acl.ceiling_for("other"), None);
    }

    #[test]
    fn push_depth_zero_is_a_legal_pull_only_ceiling() {
        let remote = resolve_one(
            r#"
slug = "puller"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { exact = "cold.archive", push_depth = 0, retain_depth = 512 } ]
"#,
        );
        assert_eq!(
            remote.durable_ceiling("cold.archive"),
            Some(RemoteDepths {
                push_depth: 0,
                retain_depth: 512
            })
        );
    }

    #[test]
    #[should_panic(expected = "retain_depth = 0")]
    fn retain_depth_zero_is_rejected() {
        resolve_one(
            r#"
slug = "no-retention"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { exact = "a.b", push_depth = 1, retain_depth = 0 } ]
"#,
        );
    }

    #[test]
    #[should_panic(expected = "sets both exact and prefix")]
    fn a_matcher_naming_both_kinds_is_rejected() {
        resolve_one(
            r#"
slug = "ambiguous"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { exact = "a.b", prefix = "a.", push_depth = 1, retain_depth = 1 } ]
"#,
        );
    }

    #[test]
    #[should_panic(expected = "sets neither exact nor prefix")]
    fn a_matcher_naming_no_kind_is_rejected() {
        resolve_one(
            r#"
slug = "empty-matcher"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
subscribe_acl = [ { push_depth = 1, retain_depth = 1 } ]
"#,
        );
    }

    /// No silent default for what a network principal may hold.
    #[test]
    fn subscribe_depths_are_required() {
        let err = toml::from_str::<RemoteConfigRaw>(
            r#"
slug = "sloppy"
token_file = "/dev/null"
grants = ["subscribe"]
subscribe_acl = [ { exact = "a.b" } ]
"#,
        )
        .expect_err("missing depths must fail to parse");
        assert!(
            err.to_string().contains("push_depth"),
            "error should name the missing field: {err}"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(
            toml::from_str::<RemoteConfigRaw>(
                "slug = \"x\"\ntoken_file = \"/dev/null\"\ngrants = []\nmax_session = 1\n"
            )
            .is_err(),
            "a typoed top-level key must not parse",
        );
        assert!(
            toml::from_str::<RemoteConfigRaw>(
                "slug = \"x\"\ntoken_file = \"/dev/null\"\ngrants = [\"subscribe\"]\n\
                 subscribe_acl = [ { exact = \"a\", push_depth = 1, retain_depth = 1, \
                 noise = \"metered\" } ]\n"
            )
            .is_err(),
            "a typoed ACL key must not parse",
        );
    }

    #[test]
    fn takeover_is_not_a_remote_grant() {
        assert!(
            toml::from_str::<RemoteConfigRaw>(
                "slug = \"x\"\ntoken_file = \"/dev/null\"\ngrants = [\"takeover\"]\n"
            )
            .is_err(),
        );
    }

    #[test]
    #[should_panic(expected = "grant Subscribe is authored but subscribe_acl is empty")]
    fn a_grant_without_its_acl_is_rejected() {
        resolve_one(
            r#"
slug = "dead-grant"
token_file = "TOKEN_FILE"
grants = ["subscribe"]
"#,
        );
    }

    #[test]
    #[should_panic(expected = "publish_acl lists channels but the matching grant Publish")]
    fn an_acl_without_its_grant_is_rejected() {
        resolve_one(
            r#"
slug = "orphan-acl"
token_file = "TOKEN_FILE"
grants = []
publish_acl = [ { prefix = "a." } ]
"#,
        );
    }

    #[test]
    fn alert_needs_no_acl() {
        let remote = resolve_one(
            r#"
slug = "pager"
token_file = "TOKEN_FILE"
grants = ["alert"]
"#,
        );
        assert!(remote.policy.has_grant(AppCapability::SurfaceAlert));
        assert!(remote.subscribe_ceilings.is_empty());
    }

    #[test]
    #[should_panic(expected = "must consist of RFC 3986 unreserved characters")]
    fn a_slug_with_a_participant_separator_is_rejected() {
        resolve_one(
            r#"
slug = "pod#kitchen"
token_file = "TOKEN_FILE"
grants = ["alert"]
"#,
        );
    }

    #[test]
    #[should_panic(expected = "duplicate [[remote]] slug")]
    fn duplicate_slugs_are_rejected() {
        let (raw, _token) = parse(
            r#"
slug = "twin"
token_file = "TOKEN_FILE"
grants = ["alert"]
"#,
        );
        resolve_remotes(&[raw.clone(), raw], &MessagingGlobalConfig::default());
    }

    #[test]
    #[should_panic(expected = "publish_burst 1000 exceeds")]
    fn a_publish_burst_above_the_bus_gate_is_rejected() {
        resolve_one(
            r#"
slug = "firehose"
token_file = "TOKEN_FILE"
grants = ["publish"]
publish_acl = [ { prefix = "a." } ]
publish_burst = 1000
"#,
        );
    }

    #[test]
    #[should_panic(expected = "publish_per_sec 1000 exceeds")]
    fn a_publish_per_sec_above_the_bus_gate_is_rejected() {
        resolve_one(
            r#"
slug = "firehose"
token_file = "TOKEN_FILE"
grants = ["publish"]
publish_acl = [ { prefix = "a." } ]
publish_per_sec = 1000
"#,
        );
    }

    #[test]
    #[should_panic(expected = "publish_burst must be >= 1")]
    fn a_zero_publish_burst_is_rejected() {
        resolve_one(
            r#"
slug = "mute"
token_file = "TOKEN_FILE"
grants = ["publish"]
publish_acl = [ { prefix = "a." } ]
publish_burst = 0
"#,
        );
    }

    #[test]
    #[should_panic(expected = "publish_per_sec must be >= 1")]
    fn a_zero_publish_per_sec_is_rejected() {
        resolve_one(
            r#"
slug = "mute"
token_file = "TOKEN_FILE"
grants = ["publish"]
publish_acl = [ { prefix = "a." } ]
publish_per_sec = 0
"#,
        );
    }

    #[test]
    #[should_panic(expected = "max_sessions must be >= 1")]
    fn zero_sessions_is_rejected() {
        resolve_one(
            r#"
slug = "never"
token_file = "TOKEN_FILE"
grants = ["alert"]
max_sessions = 0
"#,
        );
    }

    #[test]
    #[should_panic(expected = "max_subscriptions must be >= 1")]
    fn zero_subscriptions_is_rejected() {
        resolve_one(
            r#"
slug = "deaf"
token_file = "TOKEN_FILE"
grants = ["alert"]
max_subscriptions = 0
"#,
        );
    }

    #[test]
    #[should_panic(expected = "slug must be non-empty")]
    fn an_empty_slug_is_rejected() {
        resolve_one(
            r#"
slug = ""
token_file = "TOKEN_FILE"
grants = ["alert"]
"#,
        );
    }

    #[test]
    #[should_panic(expected = "failed to read secret file")]
    fn a_missing_token_file_is_a_boot_panic() {
        let raw: RemoteConfigRaw = toml::from_str(
            r#"
slug = "ghost"
token_file = "/nonexistent/brenn/remote.token"
grants = ["alert"]
"#,
        )
        .unwrap();
        resolve_remotes(&[raw], &MessagingGlobalConfig::default());
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "group/world-accessible")]
    fn a_world_readable_token_file_is_a_boot_panic() {
        use std::os::unix::fs::PermissionsExt as _;
        let token = write_token("s3cret-token\n");
        std::fs::set_permissions(token.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        let raw: RemoteConfigRaw = toml::from_str(&format!(
            "slug = \"leaky\"\ntoken_file = \"{}\"\ngrants = [\"alert\"]\n",
            token.path().display(),
        ))
        .unwrap();
        resolve_remotes(&[raw], &MessagingGlobalConfig::default());
    }

    #[test]
    fn the_token_verifies_and_never_renders() {
        let remote = resolve_one(
            r#"
slug = "pager"
token_file = "TOKEN_FILE"
grants = ["alert"]
"#,
        );
        assert!(remote.token.verify("s3cret-token"));
        assert!(!remote.token.verify("s3cret-toker"));
        assert!(!remote.token.verify("s3cret-token-longer"));
        assert!(!remote.token.verify(""));
        let rendered = format!("{:?}", remote.token);
        assert!(
            !rendered.contains("s3cret"),
            "token must not render its bytes: {rendered}"
        );
        assert!(
            !format!("{remote:?}").contains("s3cret"),
            "nor through the resolved block's own Debug",
        );
    }

    /// `==` answers what `verify` answers. The point is not that the results
    /// agree — a derived `PartialEq` would agree too — but that the operator an
    /// auth path reaches for first cannot be the short-circuiting one.
    #[test]
    fn token_equality_is_the_constant_time_comparison() {
        let expected = RemoteToken::new("s3cret-token");
        assert_eq!(RemoteToken::new("s3cret-token"), expected);
        assert_ne!(RemoteToken::new("s3cret-toker"), expected);
        assert_ne!(RemoteToken::new("s3cret-token-longer"), expected);
        assert_ne!(RemoteToken::new(""), expected);
    }

    /// Verification is correct across length classes — the property the digest
    /// representation buys is that these all cost the same, and the least this
    /// can pin is that none of them answers wrongly.
    #[test]
    fn tokens_of_any_length_verify_correctly() {
        for token in [
            "",
            "x",
            &"a".repeat(55)[..],
            &"a".repeat(64)[..],
            &"a".repeat(200)[..],
        ] {
            let stored = RemoteToken::new(token);
            assert!(stored.verify(token), "{} bytes must verify", token.len());
            assert!(
                !stored.verify(&format!("{token}x")),
                "a longer credential must not verify against {} bytes",
                token.len(),
            );
            assert!(
                token.is_empty() || !stored.verify(&"b".repeat(token.len())),
                "an equal-length wrong credential must not verify",
            );
        }
    }

    /// The unknown-slug dummy: no credential matches it, whatever its shape.
    #[test]
    fn the_unmatchable_token_matches_nothing() {
        let dummy = RemoteToken::unmatchable();
        for presented in ["", " ", &" ".repeat(64)[..], "s3cret-token", "\0"] {
            assert!(
                !dummy.verify(presented),
                "the dummy must refuse {presented:?}",
            );
        }
        assert_ne!(dummy, RemoteToken::new(""));
        assert_ne!(dummy, RemoteToken::new(" ".repeat(64)));
    }

    /// A digest-stored token renders neither its bytes nor its length.
    #[test]
    fn debug_reveals_no_length() {
        let short = format!("{:?}", RemoteToken::new("x"));
        let long = format!("{:?}", RemoteToken::new("x".repeat(200)));
        assert_eq!(short, long, "Debug must not vary with the token");
        assert!(!short.contains("200"), "no length in {short}");
    }
}
