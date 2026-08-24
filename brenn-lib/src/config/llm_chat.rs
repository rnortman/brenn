//! `[llm_chat]` config: the prefix rooting every chat channel address, the
//! retention and wake parameters of a conversation's channel family, and the
//! harness policy derived from them.
//!
//! The names themselves are minted in [`brenn_envelope::chat`], beside the
//! protocol whose addresses they are; this module supplies the prefix they are
//! rooted at.

use brenn_envelope::chat::{CHAT_SEGMENT_SEP, ChatLeaf, chat_leaf_prefix};

use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};
use crate::messaging::{ChannelScheme, WakeMin, is_unreserved_name};

/// Top-level `[llm_chat]` config section.
///
/// Values are validated at boot, not at parse time: a malformed value is a
/// refusal to start.
#[derive(Debug, Clone, PartialEq, Eq)]
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
            is_unreserved_name(prefix) && !prefix.contains(CHAT_SEGMENT_SEP),
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

    /// The harness policy for one LLM app: authority over that app's
    /// conversation leaves and nothing else. One
    /// `Prefix("<prefix>.app.<slug>.<leaf>.")` matcher per leaf on each pub/sub
    /// ACL list that leaf's scheme is gated by, plus the transport grant each of
    /// those lists is narrowed by.
    ///
    /// This is the authority the *harness* acts under — the server-side
    /// machinery wrapping the LLM: the adapter that publishes a conversation's
    /// record and stream and reads its commands. That machinery runs the
    /// production gate ladder like every other principal — there is no publisher
    /// bypass anywhere — so its authority has to exist as policy. It is derived
    /// rather than authored because the conversation ids that terminate the
    /// names are minted at runtime, while matchers are runtime patterns: one
    /// leaf-level prefix covers every present and future conversation.
    ///
    /// **Per leaf rather than one app-wide prefix**, so the app's roster channel
    /// — which sits in the leaf position and is written by a reserved system
    /// identity — stays outside every principal's reach but that one. An
    /// app-wide prefix would cover it, and a state channel with two possible
    /// writers is a state channel with no owner.
    ///
    /// It is deliberately a **separate** policy object from the app's authored
    /// one. The app's LLM publishes as the `App` principal under the authored
    /// policy; folding the chat tree into that policy would hand the LLM its own
    /// command channel, its own record, and bus tools it was never granted.
    ///
    /// Cross-app isolation is untouched: each prefix stops at the owning app's
    /// slug boundary.
    pub fn harness_policy(&self, app_slug: &str) -> AppPolicy {
        let mut policy = AppPolicy::default();
        for leaf in ChatLeaf::ALL {
            let matcher = ChannelMatcher::Prefix(chat_leaf_prefix(&self.prefix, app_slug, leaf));
            let (publish, subscribe) = match leaf.scheme() {
                ChannelScheme::Brenn => (
                    &mut policy.acls.brenn_publish,
                    &mut policy.acls.brenn_subscribe,
                ),
                ChannelScheme::Ephemeral => (
                    &mut policy.acls.ephemeral_publish,
                    &mut policy.acls.ephemeral_subscribe,
                ),
                other => panic!("chat leaves ride brenn:/ephemeral: only, got {other:?}"),
            };
            publish.push(matcher.clone());
            subscribe.push(matcher);
        }
        for grant in [
            AppCapability::MessagingPublish,
            AppCapability::MessagingSubscribe,
            AppCapability::EphemeralPublish,
            AppCapability::EphemeralSubscribe,
        ] {
            policy.grants.insert(grant);
        }
        policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::chat::{
        chat_address, chat_app_prefix, chat_bare_name, chat_roster_bare_name,
    };

    use crate::access::acl::ChannelMatcher;
    use crate::messaging::gates::well_formed_name;

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
    fn the_harness_policy_covers_every_leaf_of_every_conversation() {
        let policy = LlmChatConfig::default().harness_policy("alice");

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
    fn the_harness_policy_stops_at_the_owning_app() {
        let policy = LlmChatConfig::default().harness_policy("alice");

        assert!(!policy.allows_brenn_publish(&chat_bare_name("chat", "bob", ChatLeaf::Out, 1)));
        assert!(!policy.allows_brenn_delivery(&chat_bare_name("chat", "bob", ChatLeaf::In, 1)));
        assert!(!policy.allows_brenn_publish("chat.app.alicebob.out.1"));
        // Nothing outside the chat tree comes along with it.
        assert!(!policy.allows_brenn_publish("alerts.high"));
        assert!(!policy.allows_brenn_publish("chat.index"));
    }

    #[test]
    fn the_harness_policy_follows_the_configured_prefix() {
        let config = LlmChatConfig {
            prefix: "talk".to_string(),
            ..LlmChatConfig::default()
        };
        let policy = config.harness_policy("alice");

        assert!(policy.allows_brenn_publish("talk.app.alice.out.1"));
        assert!(!policy.allows_brenn_publish("chat.app.alice.out.1"));
    }

    /// The app's roster is written by a reserved system identity and read by
    /// whoever the operator grants it. The harness is neither: it drives
    /// conversations, it does not decide which exist.
    #[test]
    fn the_harness_policy_does_not_reach_the_roster() {
        let policy = LlmChatConfig::default().harness_policy("alice");
        let roster = chat_roster_bare_name("chat", "alice");

        assert!(!policy.allows_brenn_publish(&roster));
        assert!(!policy.allows_brenn_delivery(&roster));
    }

    /// The harness policy is built from nothing: it carries exactly the four
    /// transport grants and one matcher per leaf on each list that leaf's
    /// scheme is gated by, so no authored entry can leak into it and it can
    /// never widen an app's own reach.
    #[test]
    fn the_harness_policy_carries_nothing_but_the_conversation_leaves() {
        let policy = LlmChatConfig::default().harness_policy("alice");

        let leaves = |scheme: ChannelScheme| -> Vec<ChannelMatcher> {
            ChatLeaf::ALL
                .into_iter()
                .filter(|leaf| leaf.scheme() == scheme)
                .map(|leaf| ChannelMatcher::Prefix(chat_leaf_prefix("chat", "alice", leaf)))
                .collect()
        };
        assert_eq!(policy.acls.brenn_publish, leaves(ChannelScheme::Brenn));
        assert_eq!(policy.acls.brenn_subscribe, leaves(ChannelScheme::Brenn));
        assert_eq!(
            policy.acls.ephemeral_publish,
            leaves(ChannelScheme::Ephemeral)
        );
        assert_eq!(
            policy.acls.ephemeral_subscribe,
            leaves(ChannelScheme::Ephemeral)
        );

        for granted in [
            AppCapability::MessagingPublish,
            AppCapability::MessagingSubscribe,
            AppCapability::EphemeralPublish,
            AppCapability::EphemeralSubscribe,
        ] {
            assert!(
                policy.has_grant(granted),
                "{granted:?} is a transport grant"
            );
        }
        for withheld in [
            AppCapability::DynamicSubscribe,
            AppCapability::LocalPublish,
            AppCapability::LocalSubscribe,
            AppCapability::PwaPush,
            AppCapability::MqttPublish,
            AppCapability::Webhook,
        ] {
            assert!(
                !policy.has_grant(withheld),
                "{withheld:?} is not the harness's business"
            );
        }
        assert!(policy.tool_grants.is_empty());
        assert!(policy.acls.local_publish.is_empty());
        assert!(policy.acls.mqtt_publish.is_empty());
        assert!(policy.acls.webhook.is_empty());
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
}
