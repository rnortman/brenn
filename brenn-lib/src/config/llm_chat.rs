//! `[llm_chat]` config and the channel-name grammar it roots.
//!
//! A conversation talks to the bus over a family of channels derived from one
//! configured prefix. Every name in that family is minted by [`chat_bare_name`]
//! and nothing else, so provisioning, ACL authoring, and the adapter cannot
//! drift on the shape.

use serde::Deserialize;

use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};
use crate::messaging::{ChannelScheme, WakeMin, is_unreserved_char};

/// Top-level `[llm_chat]` config section.
///
/// Values are validated at boot, not at parse time: a malformed value is a
/// refusal to start.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct LlmChatConfig {
    /// Bare-name segment rooting every chat channel address (e.g. `"chat"` ⇒
    /// `brenn:chat.app.<slug>.in.<id>`). Contains no `.` — it is one segment,
    /// not a path.
    pub prefix: String,
    /// Retained window on the durable command and record channels, in messages.
    /// This window *is* the conversation history a fresh subscriber can read.
    pub retained_window: u32,
    /// Wake threshold on the command channel: a command published below it
    /// parks until something else activates the conversation.
    pub wake_min: WakeMin,
    /// How long a bus-driven bridge stays alive after its last interaction, so
    /// a peer driving a conversation in bursts does not pay startup per burst.
    pub idle_timeout_secs: u64,
}

/// Default namespace rooting the derived chat channels.
fn default_prefix() -> String {
    "chat".to_string()
}

/// Default retained window. Generous by intent: on a chat channel the window is
/// the history, and a conversation's `.out` traffic is low-rate enough that a
/// thousand messages is months of record rather than hours.
fn default_retained_window() -> u32 {
    1000
}

/// Default idle timeout for a bus-driven bridge, seconds.
fn default_idle_timeout_secs() -> u64 {
    300
}

impl Default for LlmChatConfig {
    fn default() -> Self {
        Self {
            prefix: default_prefix(),
            retained_window: default_retained_window(),
            wake_min: WakeMin::Normal,
            idle_timeout_secs: default_idle_timeout_secs(),
        }
    }
}

/// Largest accepted `retained_window`. The window is retained per conversation
/// and per durable channel, so an unbounded value is unbounded DB growth
/// multiplied by the conversation count.
const MAX_RETAINED_WINDOW: u32 = 100_000;

/// Largest accepted `idle_timeout_secs` — a day. Longer is a keep-forever
/// policy expressed as a timeout, which is a different feature.
const MAX_IDLE_TIMEOUT_SECS: u64 = 86_400;

impl LlmChatConfig {
    /// Boot validation. Every failure here is operator config, never
    /// attacker-reachable.
    ///
    /// # Panics
    ///
    /// Panics if `prefix` is not a well-formed single bare-name segment, if
    /// `retained_window` is zero or above [`MAX_RETAINED_WINDOW`], or if
    /// `idle_timeout_secs` is zero or above [`MAX_IDLE_TIMEOUT_SECS`].
    pub fn validate(&self) {
        let prefix = self.prefix.as_str();
        assert!(
            !prefix.is_empty()
                && prefix.chars().all(is_unreserved_char)
                && !prefix.contains(CHAT_SEGMENT_SEP),
            "boot: [llm_chat] prefix {prefix:?} is not a well-formed bare-name segment \
             (allowed: A-Za-z0-9_~-, non-empty, no {CHAT_SEGMENT_SEP:?}) — it roots every chat \
             channel address. Refusing to start (fail-fast on invalid config)."
        );
        assert!(
            (1..=MAX_RETAINED_WINDOW).contains(&self.retained_window),
            "boot: [llm_chat] retained_window = {} is out of range 1..={MAX_RETAINED_WINDOW} — \
             the window is the conversation history and is retained per conversation. Refusing \
             to start (fail-fast on invalid config).",
            self.retained_window,
        );
        assert!(
            (1..=MAX_IDLE_TIMEOUT_SECS).contains(&self.idle_timeout_secs),
            "boot: [llm_chat] idle_timeout_secs = {} is out of range 1..={MAX_IDLE_TIMEOUT_SECS} \
             — a bus-driven bridge must eventually go idle. Refusing to start (fail-fast on \
             invalid config).",
            self.idle_timeout_secs,
        );
    }

    /// Extend an LLM app's resolved policy with authority over its own chat
    /// subtree: one `Prefix("<prefix>.app.<slug>.")` matcher on each of the four
    /// pub/sub ACL lists the chat channels use, plus the transport grant each of
    /// those lists is narrowed by.
    ///
    /// A conversation must publish its record and stream and read its commands,
    /// and it does so through the production gate ladder like every other
    /// principal — there is no publisher bypass anywhere — so the authority has
    /// to exist as policy. It is derived here rather than authored because the
    /// conversation ids that terminate the names are minted at runtime, while
    /// matchers are runtime patterns: one app-level prefix covers every present
    /// and future conversation.
    ///
    /// The app's own LLM publishes as the `App` principal under this same
    /// policy, so it can also publish into its conversations' tree; sender
    /// attribution (`app:<slug>@<server>` versus `conversation:<id>`) is what
    /// keeps the record honest, and cross-app isolation is untouched.
    ///
    /// Idempotent per list and per grant: an operator who authored the same
    /// coverage keeps their entry and gains a redundant one, which changes no
    /// decision.
    pub fn grant_app_chat_tree(&self, app_slug: &str, policy: &mut AppPolicy) {
        let matcher = ChannelMatcher::Prefix(chat_app_prefix(&self.prefix, app_slug));
        for list in [
            &mut policy.acls.brenn_publish,
            &mut policy.acls.brenn_subscribe,
            &mut policy.acls.ephemeral_publish,
            &mut policy.acls.ephemeral_subscribe,
        ] {
            list.push(matcher.clone());
        }
        for grant in [
            AppCapability::MessagingPublish,
            AppCapability::MessagingSubscribe,
            AppCapability::EphemeralPublish,
            AppCapability::EphemeralSubscribe,
        ] {
            policy.grants.insert(grant);
        }
    }
}

// ── Derived addresses ──────────────────────────────────────────────────────

/// Segment separator in a chat channel name.
const CHAT_SEGMENT_SEP: char = '.';

/// Literal second segment of every chat channel name. It reserves root-level
/// room beside the per-app subtree for future siblings under the prefix, which
/// would otherwise collide with an app slug.
const CHAT_APP_SEGMENT: &str = "app";

/// The traffic leaf of a chat channel name — the segment between the owning
/// app's slug and the conversation id.
///
/// The conversation id is the terminal segment because it is the only segment
/// minted at runtime. That ordering is what lets an exact matcher name one
/// conversation and a segment-boundary prefix name every conversation of an
/// app, per leaf, with no wildcard matcher: a grant may cover "may drive every
/// conversation of this app" without also covering "may forge its record".
///
/// The cost is that a conversation owns no single subtree of its own — its
/// channels are siblings under the per-leaf subtrees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatLeaf {
    /// Commands from peers to the conversation.
    In,
    /// The conversation record: messages, status, errors, telemetry.
    Out,
    /// Token batches.
    Stream,
    /// Pre-warm signal: bodies are ignored, the message's existence is the
    /// signal.
    Wake,
    /// Tool-call permission flow. **Reserved, unbuilt** — the name is fixed so
    /// the grammar cannot collide once the flow exists, and so "may chat" and
    /// "may approve tool calls" are separately grantable today.
    Approvals,
}

impl ChatLeaf {
    /// Every leaf. The single source enumerating callers use, so a new leaf
    /// cannot be added and silently skipped by a hand-listed set.
    pub const ALL: [ChatLeaf; 5] = [
        Self::In,
        Self::Out,
        Self::Stream,
        Self::Wake,
        Self::Approvals,
    ];

    /// The leaf's name segment.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
            Self::Stream => "stream",
            Self::Wake => "wake",
            Self::Approvals => "approvals",
        }
    }

    /// The address scheme this leaf's traffic rides. Durable where the content
    /// is the record and must survive restart; ephemeral where loss is
    /// preferable to a persistence round trip.
    pub fn scheme(self) -> ChannelScheme {
        match self {
            Self::In | Self::Out | Self::Approvals => ChannelScheme::Brenn,
            Self::Stream | Self::Wake => ChannelScheme::Ephemeral,
        }
    }
}

/// `<prefix>.app.<app_slug>.<leaf>.<conversation_id>` — the sole derivation of a
/// chat channel name.
pub fn chat_bare_name(
    prefix: &str,
    app_slug: &str,
    leaf: ChatLeaf,
    conversation_id: i64,
) -> String {
    let leaf = leaf.as_str();
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}{leaf}{CHAT_SEGMENT_SEP}{conversation_id}"
    )
}

/// [`chat_bare_name`] with its leaf's address scheme, e.g.
/// `brenn:chat.app.<slug>.in.<id>`.
pub fn chat_address(prefix: &str, app_slug: &str, leaf: ChatLeaf, conversation_id: i64) -> String {
    format!(
        "{}{}",
        leaf.scheme().prefix(),
        chat_bare_name(prefix, app_slug, leaf, conversation_id)
    )
}

/// `<prefix>.app.<app_slug>.` — the segment-boundary prefix covering every chat
/// channel of one app, across leaves and conversations.
pub fn chat_app_prefix(prefix: &str, app_slug: &str) -> String {
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}"
    )
}

/// `<prefix>.app.<app_slug>.<leaf>.` — the segment-boundary prefix covering one
/// leaf of every conversation of one app. The fleet-grain grant: it reaches
/// every conversation on that leaf and no other leaf.
pub fn chat_leaf_prefix(prefix: &str, app_slug: &str, leaf: ChatLeaf) -> String {
    let leaf = leaf.as_str();
    format!(
        "{prefix}{CHAT_SEGMENT_SEP}{CHAT_APP_SEGMENT}{CHAT_SEGMENT_SEP}{app_slug}{CHAT_SEGMENT_SEP}{leaf}{CHAT_SEGMENT_SEP}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::acl::ChannelMatcher;
    use crate::messaging::gates::well_formed_name;

    #[test]
    fn names_match_the_pinned_grammar() {
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::In, 42),
            "chat.app.alice.in.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Out, 42),
            "chat.app.alice.out.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Stream, 42),
            "chat.app.alice.stream.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Wake, 42),
            "chat.app.alice.wake.42"
        );
        assert_eq!(
            chat_bare_name("chat", "alice", ChatLeaf::Approvals, 42),
            "chat.app.alice.approvals.42"
        );
        assert_eq!(
            chat_bare_name("talk", "alice", ChatLeaf::In, 42),
            "talk.app.alice.in.42",
            "the configured prefix roots the tree"
        );
    }

    #[test]
    fn addresses_carry_their_leafs_scheme() {
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::In, 42),
            "brenn:chat.app.alice.in.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Out, 42),
            "brenn:chat.app.alice.out.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Approvals, 42),
            "brenn:chat.app.alice.approvals.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Stream, 42),
            "ephemeral:chat.app.alice.stream.42"
        );
        assert_eq!(
            chat_address("chat", "alice", ChatLeaf::Wake, 42),
            "ephemeral:chat.app.alice.wake.42"
        );
    }

    #[test]
    fn every_derived_address_passes_the_publish_gate() {
        for leaf in ChatLeaf::ALL {
            let address = chat_address("chat", "alice", leaf, 42);
            let bare = well_formed_name(&address, leaf.scheme())
                .unwrap_or_else(|| panic!("{address} is a well-formed channel address"));
            assert_eq!(bare, chat_bare_name("chat", "alice", leaf, 42));
        }
        // A negative conversation id is not reachable from the DB, but the
        // minus sign is in the unreserved set, so the grammar survives it.
        let address = chat_address("chat", "alice", ChatLeaf::In, -1);
        assert!(well_formed_name(&address, ChannelScheme::Brenn).is_some());
    }

    #[test]
    fn exact_grants_name_one_conversation() {
        let driver = ChannelMatcher::Exact(chat_bare_name("chat", "alice", ChatLeaf::In, 4));
        assert!(driver.matches("chat.app.alice.in.4"));
        assert!(
            !driver.matches("chat.app.alice.in.42"),
            "an exact id must not cover an id it is a digit-prefix of"
        );
        assert!(!driver.matches("chat.app.alice.out.4"));
        assert!(!driver.matches("chat.app.bob.in.4"));
    }

    #[test]
    fn leaf_prefix_grants_one_leaf_of_every_conversation() {
        let fleet = ChannelMatcher::Prefix(chat_leaf_prefix("chat", "alice", ChatLeaf::In));
        assert!(fleet.matches(&chat_bare_name("chat", "alice", ChatLeaf::In, 4)));
        assert!(fleet.matches(&chat_bare_name("chat", "alice", ChatLeaf::In, 4242)));
        assert!(
            !fleet.matches(&chat_bare_name("chat", "alice", ChatLeaf::Out, 4)),
            "a fleet driver must not be able to forge the record"
        );
        assert!(!fleet.matches(&chat_bare_name("chat", "alice", ChatLeaf::Approvals, 4)));
        assert!(!fleet.matches(&chat_bare_name("chat", "bob", ChatLeaf::In, 4)));
    }

    #[test]
    fn app_prefix_stops_at_the_segment_boundary() {
        let app = ChannelMatcher::Prefix(chat_app_prefix("chat", "alice"));
        for leaf in ChatLeaf::ALL {
            assert!(app.matches(&chat_bare_name("chat", "alice", leaf, 4)));
        }
        assert!(
            !app.matches("chat.app.alicebob.in.7"),
            "a slug prefix must not reach a longer slug"
        );
        assert!(!app.matches(&chat_bare_name("chat", "bob", ChatLeaf::In, 4)));
    }

    #[test]
    fn defaults_validate() {
        LlmChatConfig::default().validate();
    }

    #[test]
    fn the_derived_grant_covers_every_leaf_of_every_conversation() {
        let mut policy = AppPolicy::default();
        LlmChatConfig::default().grant_app_chat_tree("alice", &mut policy);

        assert!(policy.has_grant(AppCapability::MessagingPublish));
        assert!(policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(policy.has_grant(AppCapability::EphemeralPublish));
        assert!(policy.has_grant(AppCapability::EphemeralSubscribe));

        // Every leaf, on a conversation id no config could have named — the
        // matcher is a runtime pattern, so ids minted later are already covered.
        for leaf in ChatLeaf::ALL {
            let name = chat_bare_name("chat", "alice", leaf, 987654);
            let (publishes, subscribes) = match leaf.scheme() {
                ChannelScheme::Brenn => (
                    policy.allows_brenn_publish(&name),
                    policy.allows_brenn_delivery(&name),
                ),
                ChannelScheme::Ephemeral => (
                    policy.allows_ephemeral_publish(&name),
                    policy.allows_ephemeral_delivery(&name),
                ),
                other => panic!("chat leaves ride brenn:/ephemeral: only, got {other:?}"),
            };
            assert!(publishes, "{name} must be publishable");
            assert!(subscribes, "{name} must be deliverable");
        }
    }

    #[test]
    fn the_derived_grant_stops_at_the_owning_app() {
        let mut policy = AppPolicy::default();
        LlmChatConfig::default().grant_app_chat_tree("alice", &mut policy);

        assert!(!policy.allows_brenn_publish(&chat_bare_name("chat", "bob", ChatLeaf::Out, 1)));
        assert!(!policy.allows_brenn_delivery(&chat_bare_name("chat", "bob", ChatLeaf::In, 1)));
        assert!(!policy.allows_brenn_publish("chat.app.alicebob.out.1"));
        // Nothing outside the chat tree comes along with it.
        assert!(!policy.allows_brenn_publish("alerts.high"));
        assert!(!policy.allows_brenn_publish("chat.index"));
    }

    #[test]
    fn the_derived_grant_follows_the_configured_prefix() {
        let config = LlmChatConfig {
            prefix: "talk".to_string(),
            ..LlmChatConfig::default()
        };
        let mut policy = AppPolicy::default();
        config.grant_app_chat_tree("alice", &mut policy);

        assert!(policy.allows_brenn_publish("talk.app.alice.out.1"));
        assert!(!policy.allows_brenn_publish("chat.app.alice.out.1"));
    }

    #[test]
    fn the_derived_grant_leaves_authored_entries_alone() {
        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MessagingPublish);
        policy.acls.brenn_publish = vec![ChannelMatcher::Exact("outbox".to_string())];

        LlmChatConfig::default().grant_app_chat_tree("alice", &mut policy);

        assert!(policy.allows_brenn_publish("outbox"));
        assert!(policy.allows_brenn_publish("chat.app.alice.out.1"));
    }

    #[test]
    #[should_panic(expected = "not a well-formed bare-name segment")]
    fn multi_segment_prefix_panics() {
        LlmChatConfig {
            prefix: "chat.app".to_string(),
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "not a well-formed bare-name segment")]
    fn empty_prefix_panics() {
        LlmChatConfig {
            prefix: String::new(),
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "not a well-formed bare-name segment")]
    fn reserved_char_in_prefix_panics() {
        LlmChatConfig {
            prefix: "chat/app".to_string(),
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "retained_window = 0")]
    fn zero_retained_window_panics() {
        LlmChatConfig {
            retained_window: 0,
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "idle_timeout_secs = 0")]
    fn zero_idle_timeout_panics() {
        LlmChatConfig {
            idle_timeout_secs: 0,
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "retained_window = 100001")]
    fn a_retained_window_past_the_maximum_panics() {
        LlmChatConfig {
            retained_window: MAX_RETAINED_WINDOW + 1,
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    #[should_panic(expected = "idle_timeout_secs = 86401")]
    fn an_idle_timeout_past_the_maximum_panics() {
        LlmChatConfig {
            idle_timeout_secs: MAX_IDLE_TIMEOUT_SECS + 1,
            ..LlmChatConfig::default()
        }
        .validate();
    }

    /// Both maxima are accepted values, not rejected ones — the range is
    /// inclusive at the top and an off-by-one either way fails here.
    #[test]
    fn both_maxima_are_accepted() {
        LlmChatConfig {
            retained_window: MAX_RETAINED_WINDOW,
            idle_timeout_secs: MAX_IDLE_TIMEOUT_SECS,
            ..LlmChatConfig::default()
        }
        .validate();
    }

    #[test]
    fn section_parses_with_partial_keys() {
        let parsed: LlmChatConfig =
            toml::from_str("prefix = \"talk\"\nwake_min = \"very-low\"\n").expect("section parses");
        assert_eq!(
            parsed,
            LlmChatConfig {
                prefix: "talk".to_string(),
                retained_window: default_retained_window(),
                wake_min: WakeMin::VeryLow,
                idle_timeout_secs: default_idle_timeout_secs(),
            }
        );
    }
}
