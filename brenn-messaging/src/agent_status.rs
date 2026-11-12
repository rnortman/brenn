//! The agent-status facility's bus identity: the three channels an operator
//! declares to turn it on, and the `system:agent-status` participant that
//! publishes on them.
//!
//! The channels are **declared, not minted**: the operator writes the three
//! channel blocks, sizes them and grants other principals access like any other
//! channel, and their presence is what activates the facility. All three, or
//! none.
//!
//! - `ephemeral:agent.sessions` is **state**: a snapshot of every conversation
//!   with a running Claude Code process subsumes the one before it. In memory,
//!   because no process survives a restart and a stale snapshot after one is
//!   worse than none.
//! - `brenn:agent.ended` is a **fact** channel: one message per run of a
//!   session that ended. Durable, so a consumer down across a restart still
//!   finds the facts in its window.
//! - `brenn:agent.accounts` is **state**, and durable: it is the account
//!   ledger's storage, read back once at boot to seed it.
//!
//! The bodies are [`brenn_envelope::agent`]'s.

use brenn_envelope::ChannelScheme;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::messaging::config::SendRate;

use crate::ChannelEntry;
use crate::system::SystemParticipantSpec;

/// System-participant component name; the identity is `system:agent-status`,
/// spelled for readers as [`brenn_envelope::agent::AGENT_STATUS_SENDER`].
pub const AGENT_STATUS_COMPONENT: &str = "agent-status";

/// The lowest send rate boot accepts on each of the three channels.
///
/// A system publish draws on no send budget but passes the per-(sender,
/// channel) send-rate gate, and the reporter treats a refused publish as fatal.
/// The numbers are written out rather than taken from `SendRate::default()`, so
/// that moving the host's general default does not move this floor unnoticed.
pub const AGENT_STATUS_MIN_SEND_RATE: SendRate = SendRate {
    burst: 240,
    refill_interval_secs: 1,
    refill: 30,
};

/// Whether `rate` admits less than [`AGENT_STATUS_MIN_SEND_RATE`]: a smaller
/// burst, or a smaller sustained rate. The sustained rates are compared by
/// cross-multiplying in `u128`, which cannot overflow for a `u32` times a
/// `u64`; `refill_interval_secs` has no upper bound.
fn below_floor(rate: &SendRate) -> bool {
    let floor = AGENT_STATUS_MIN_SEND_RATE;
    let sustained = u128::from(rate.refill) * u128::from(floor.refill_interval_secs);
    let floor_sustained = u128::from(floor.refill) * u128::from(rate.refill_interval_secs);
    rate.burst < floor.burst || sustained < floor_sustained
}

/// Snapshot of every conversation with a running Claude Code process.
pub const SESSIONS_ADDRESS: &str = "ephemeral:agent.sessions";

/// One message per run of a session that ended.
pub const ENDED_ADDRESS: &str = "brenn:agent.ended";

/// Snapshot of every configured Claude profile as an account.
pub const ACCOUNTS_ADDRESS: &str = "brenn:agent.accounts";

/// The scheme-stripped name, the grain ACLs match at.
///
/// # Panics
///
/// If `address` carries no known scheme prefix. All three are constants here.
fn bare(address: &'static str) -> &'static str {
    ChannelScheme::split(address)
        .unwrap_or_else(|| panic!("{address:?} carries no known scheme"))
        .1
}

/// The `system:agent-status` participant, or `None` when the document declares
/// none of the three channels.
///
/// It may publish on exactly the three channels, each in its scheme's ACL
/// family, and subscribes to the accounts channel alone, to read the retained
/// ledger at boot.
///
/// # Panics
///
/// If only some of the three channels are declared, naming the missing ones;
/// if the sessions channel retains nothing, which would leave a consumer that
/// starts later with no snapshot to read; and if the accounts channel has a
/// standing window of zero, which would lose the ledger across a restart; and
/// if any of the three resolves a send rate below
/// [`AGENT_STATUS_MIN_SEND_RATE`].
pub fn agent_status_spec(entries: &[ChannelEntry]) -> Option<SystemParticipantSpec> {
    let find = |address: &str| entries.iter().find(|e| e.address == address);
    let declared = [SESSIONS_ADDRESS, ENDED_ADDRESS, ACCOUNTS_ADDRESS].map(find);
    let (sessions, accounts) = match declared {
        [None, None, None] => return None,
        [Some(sessions), Some(ended), Some(accounts)] => {
            for entry in [sessions, ended, accounts] {
                let rate = entry.resolved_channel.send_rate;
                assert!(
                    !below_floor(&rate),
                    "[[channel]] {:?} resolves a send_rate of {rate:?}, below the agent-status \
                     floor {AGENT_STATUS_MIN_SEND_RATE:?} — a publish the rate gate refuses stops \
                     the server; write `send_rate` on the channel's declaration at or above the \
                     floor, as the AgentStatus assembly does, because a declaration that states \
                     none takes [messaging] default_send_rate",
                    entry.address,
                );
            }
            (sessions, accounts)
        }
        _ => {
            let missing: Vec<&str> = [SESSIONS_ADDRESS, ENDED_ADDRESS, ACCOUNTS_ADDRESS]
                .into_iter()
                .zip(declared)
                .filter_map(|(address, entry)| entry.is_none().then_some(address))
                .collect();
            panic!(
                "the agent-status channels are all three or none, and {missing:?} is not \
                 declared — a facility missing a channel reports part of what it knows and looks \
                 complete"
            )
        }
    };
    let retain = sessions.resolved_channel.retain_depth;
    assert!(
        retain.is_push_enabled(),
        "[[channel]] {SESSIONS_ADDRESS:?} has a retain_depth of {retain:?} — the sessions channel \
         is state, and a channel that retains nothing leaves a consumer that starts later with \
         no snapshot; size it to at least one",
    );
    let standing = accounts.resolved_channel.standing_retain_depth;
    assert!(
        standing.is_push_enabled(),
        "[[channel]] {ACCOUNTS_ADDRESS:?} has a standing_retain_depth of {standing:?} — the \
         accounts channel is the account ledger's storage, and a channel that retains nothing \
         loses every profile's spend at restart; size it to at least one",
    );

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Exact(bare(SESSIONS_ADDRESS).to_string()));
    for address in [ENDED_ADDRESS, ACCOUNTS_ADDRESS] {
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(bare(address).to_string()));
    }
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Exact(bare(ACCOUNTS_ADDRESS).to_string()));
    Some(SystemParticipantSpec {
        component: AGENT_STATUS_COMPONENT,
        policy,
        subscriptions: vec![ACCOUNTS_ADDRESS.to_string()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::test_support::test_channel_entry;

    fn entry(address: &'static str, retain: Depth, standing: Depth) -> ChannelEntry {
        let (scheme, name) = ChannelScheme::split(address).expect("a schemed address");
        let mut entry = test_channel_entry(name, Vec::new());
        entry.address = address.to_string();
        entry.transport_type = scheme;
        entry.resolved_channel.retain_depth = retain;
        entry.resolved_channel.standing_retain_depth = standing;
        entry
    }

    fn sessions() -> ChannelEntry {
        entry(SESSIONS_ADDRESS, Depth::Bounded(1), Depth::Bounded(1))
    }

    fn ended() -> ChannelEntry {
        entry(ENDED_ADDRESS, Depth::Bounded(64), Depth::Bounded(64))
    }

    fn accounts() -> ChannelEntry {
        entry(ACCOUNTS_ADDRESS, Depth::Bounded(1), Depth::Bounded(1))
    }

    #[test]
    fn a_document_declaring_none_of_the_channels_has_no_participant() {
        assert!(
            agent_status_spec(&[entry("brenn:other", Depth::Bounded(1), Depth::Bounded(1))])
                .is_none()
        );
    }

    #[test]
    fn the_declared_three_yield_the_participant() {
        let spec = agent_status_spec(&[sessions(), ended(), accounts()]).expect("all declared");
        assert_eq!(spec.component, AGENT_STATUS_COMPONENT);
        assert_eq!(spec.subscriptions, vec![ACCOUNTS_ADDRESS.to_string()]);
        assert!(spec.policy.grants.has(AppCapability::MessagingPublish));
        assert!(spec.policy.grants.has(AppCapability::EphemeralPublish));
        assert!(spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert_eq!(
            spec.policy.acls.ephemeral_publish,
            vec![ChannelMatcher::Exact("agent.sessions".to_string())]
        );
        assert_eq!(
            spec.policy.acls.brenn_publish,
            vec![
                ChannelMatcher::Exact("agent.ended".to_string()),
                ChannelMatcher::Exact("agent.accounts".to_string()),
            ]
        );
        assert_eq!(
            spec.policy.acls.brenn_subscribe,
            vec![ChannelMatcher::Exact("agent.accounts".to_string())]
        );
    }

    /// The participant reads nothing but its own ledger and writes nothing but
    /// its three channels.
    #[test]
    fn the_participant_holds_no_other_authority() {
        let spec = agent_status_spec(&[sessions(), ended(), accounts()]).expect("all declared");
        assert!(!spec.policy.grants.has(AppCapability::EphemeralSubscribe));
        assert!(!spec.policy.grants.has(AppCapability::LocalPublish));
        assert!(!spec.policy.grants.has(AppCapability::MqttPublish));
        assert!(spec.policy.acls.ephemeral_subscribe.is_empty());
        assert!(
            !spec
                .policy
                .acls
                .brenn_subscribe
                .contains(&ChannelMatcher::Exact("agent.ended".to_string()))
        );
    }

    #[test]
    #[should_panic(expected = "[\"brenn:agent.ended\"] is not declared")]
    fn a_partial_declaration_is_refused_naming_what_is_missing() {
        drop(agent_status_spec(&[sessions(), accounts()]));
    }

    #[test]
    #[should_panic(expected = "[\"ephemeral:agent.sessions\", \"brenn:agent.accounts\"]")]
    fn a_lone_channel_is_refused_naming_the_other_two() {
        drop(agent_status_spec(&[ended()]));
    }

    #[test]
    #[should_panic(expected = "retain_depth of Bounded(0)")]
    fn a_sessions_channel_that_retains_nothing_is_refused() {
        drop(agent_status_spec(&[
            entry(SESSIONS_ADDRESS, Depth::Bounded(0), Depth::Bounded(0)),
            ended(),
            accounts(),
        ]));
    }

    fn with_rate(mut entry: ChannelEntry, rate: SendRate) -> ChannelEntry {
        entry.resolved_channel.send_rate = rate;
        entry
    }

    /// The three channels with `rate` on the one at `index`, the floor on the
    /// others.
    fn rated_at(index: usize, rate: SendRate) -> Vec<ChannelEntry> {
        let mut all = vec![sessions(), ended(), accounts()]
            .into_iter()
            .map(|e| with_rate(e, AGENT_STATUS_MIN_SEND_RATE))
            .collect::<Vec<_>>();
        all[index] = with_rate(all[index].clone(), rate);
        all
    }

    fn panics(entries: Vec<ChannelEntry>) -> Option<String> {
        std::panic::catch_unwind(|| drop(agent_status_spec(&entries)))
            .err()
            .map(|payload| match payload.downcast::<String>() {
                Ok(message) => *message,
                Err(_) => "<non-string panic>".to_string(),
            })
    }

    #[test]
    fn the_sender_constant_is_the_participants_identity() {
        assert_eq!(
            crate::ParticipantId::for_system(AGENT_STATUS_COMPONENT).as_str(),
            brenn_envelope::agent::AGENT_STATUS_SENDER
        );
    }

    #[test]
    fn the_floor_and_a_higher_rate_are_accepted() {
        for index in 0..3 {
            assert!(panics(rated_at(index, AGENT_STATUS_MIN_SEND_RATE)).is_none());
            let higher = SendRate {
                burst: 1000,
                refill_interval_secs: 1,
                refill: 100,
            };
            assert!(panics(rated_at(index, higher)).is_none());
            // The same sustained rate, stated per ten seconds.
            let scaled = SendRate {
                burst: 240,
                refill_interval_secs: 10,
                refill: 300,
            };
            assert!(panics(rated_at(index, scaled)).is_none());
        }
    }

    #[test]
    fn a_burst_below_the_floor_is_refused_on_each_channel() {
        let rate = SendRate {
            burst: 239,
            ..AGENT_STATUS_MIN_SEND_RATE
        };
        for (index, address) in [SESSIONS_ADDRESS, ENDED_ADDRESS, ACCOUNTS_ADDRESS]
            .into_iter()
            .enumerate()
        {
            let message = panics(rated_at(index, rate)).expect("refused");
            assert!(message.contains(address), "{message}");
            assert!(message.contains("write `send_rate`"), "{message}");
        }
    }

    #[test]
    fn a_sustained_rate_below_the_floor_is_refused_on_each_channel() {
        let slower = SendRate {
            burst: 1000,
            refill_interval_secs: 1,
            refill: 29,
        };
        let stretched = SendRate {
            burst: 1000,
            refill_interval_secs: 2,
            refill: 59,
        };
        for (index, address) in [SESSIONS_ADDRESS, ENDED_ADDRESS, ACCOUNTS_ADDRESS]
            .into_iter()
            .enumerate()
        {
            for rate in [slower, stretched] {
                let message = panics(rated_at(index, rate)).expect("refused");
                assert!(message.contains(address), "{message}");
            }
        }
    }

    /// A `u64` product would wrap here and pass a rate that is effectively zero.
    #[test]
    fn an_unbounded_refill_interval_is_below_the_floor() {
        let rate = SendRate {
            refill_interval_secs: u64::MAX,
            ..AGENT_STATUS_MIN_SEND_RATE
        };
        assert!(panics(rated_at(0, rate)).is_some());
    }

    #[test]
    #[should_panic(expected = "standing_retain_depth of Bounded(0)")]
    fn an_accounts_channel_with_no_standing_window_is_refused() {
        drop(agent_status_spec(&[
            sessions(),
            ended(),
            entry(ACCOUNTS_ADDRESS, Depth::Bounded(1), Depth::Bounded(0)),
        ]));
    }
}
