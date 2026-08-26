//! Which depths and knobs a channel block states, and which it may not.
//!
//! Presence and absence only — the shape of a block, not the values in it. What
//! a depth may *be* (a count, the word `unbounded`, the noise and sink
//! vocabularies), which names are reserved, whether a uuid is well formed: all
//! of that is the runtime's, where the types that carry those values live.
//!
//! Two readers consult these predicates: the configuration front end, which
//! refuses a malformed block with a diagnostic, and the boot builders, which
//! panic on one. Each keeps its own failure mode; only the answers are shared.
//!
//! Deliberately left to the runtime alone, because each is a rule about a value
//! rather than about presence: a non-durable channel's `retain_depth` must be
//! bounded, and a tuning block's `retain_depth` must not be zero.

/// A depth a channel block can state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChannelDepthKey {
    /// How many unseen messages one activation hands over.
    PushDepth,
    /// The window a subscriber can see behind its cursor.
    RetainDepth,
    /// The durable reaper's frontier: what the channel keeps for subscribers
    /// that do not exist yet.
    StandingRetainDepth,
}

impl ChannelDepthKey {
    /// Every depth, in the order a block states them.
    pub const ALL: [ChannelDepthKey; 3] = [
        ChannelDepthKey::PushDepth,
        ChannelDepthKey::RetainDepth,
        ChannelDepthKey::StandingRetainDepth,
    ];

    /// The key this depth is written as.
    pub fn word(self) -> &'static str {
        match self {
            Self::PushDepth => "push_depth",
            Self::RetainDepth => "retain_depth",
            Self::StandingRetainDepth => "standing_retain_depth",
        }
    }
}

/// Which role a channel block plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelBlockRole {
    /// Mints a channel entry for an operator-owned `brenn:`/`ephemeral:`/`local:`
    /// channel.
    Declaring,
    /// Supplies depths and knobs for channels the system mints. Mints nothing.
    Tuning,
}

/// Must a block of this role, on a channel of this durability, state this depth?
///
/// A declaring block sizes the window it mints, so push and retain are always
/// required; the standing buffer exists only on disk, so it is required exactly
/// when the channel is durable. A tuning block states every depth whatever the
/// channel's durability — the family it tunes has a bounded in-code default for
/// each, and a block that supplied some of them would silently inherit the rest.
pub fn depth_required(key: ChannelDepthKey, role: ChannelBlockRole, durable: bool) -> bool {
    match role {
        ChannelBlockRole::Tuning => true,
        ChannelBlockRole::Declaring => match key {
            ChannelDepthKey::PushDepth | ChannelDepthKey::RetainDepth => true,
            ChannelDepthKey::StandingRetainDepth => durable,
        },
    }
}

/// The durability a tuning block passes to [`depth_required`].
///
/// The tuning row is durability-blind — every depth is required whatever the
/// tuned family is — so the value is inert, and a caller names it rather than
/// classifying a scheme for an answer that cannot change.
pub const TUNING_DURABILITY_IGNORED: bool = false;

/// May a declaring block of this durability state `standing_retain_depth`?
///
/// Only a disk-backed channel has a standing buffer: it is the durable reaper's
/// frontier, and a non-durable channel's retention is `retain_depth` alone.
pub fn standing_admitted(durable: bool) -> bool {
    durable
}

/// May a declaring block of this durability state a sink?
///
/// A non-durable channel evicts from memory and has nothing to archive.
pub fn sink_admitted(durable: bool) -> bool {
    durable
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The arm list is exhaustive: a new depth key stops compilation until it
    /// is written down here, next to the `ALL` row it also needs.
    #[test]
    fn all_is_walked_by_its_exhaustive_guard() {
        for key in ChannelDepthKey::ALL {
            match key {
                ChannelDepthKey::PushDepth
                | ChannelDepthKey::RetainDepth
                | ChannelDepthKey::StandingRetainDepth => {}
            }
            assert!(
                ChannelDepthKey::ALL.contains(&key),
                "ChannelDepthKey::ALL is missing {key:?}"
            );
        }
    }

    #[test]
    fn every_word_is_the_key_it_names() {
        assert_eq!(ChannelDepthKey::PushDepth.word(), "push_depth");
        assert_eq!(ChannelDepthKey::RetainDepth.word(), "retain_depth");
        assert_eq!(
            ChannelDepthKey::StandingRetainDepth.word(),
            "standing_retain_depth"
        );
        let mut words: Vec<&str> = ChannelDepthKey::ALL.iter().map(|key| key.word()).collect();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), ChannelDepthKey::ALL.len());
    }

    /// The whole table, 3 keys × 2 roles × 2 durabilities, asserted rather than
    /// derived — a rule changed on purpose is one line here, and a rule changed
    /// by accident is a red test.
    #[test]
    fn the_presence_table_is_exhaustively_what_it_says() {
        use ChannelBlockRole::{Declaring, Tuning};
        use ChannelDepthKey::{PushDepth, RetainDepth, StandingRetainDepth};
        let expected = [
            ((PushDepth, Declaring, false), true),
            ((PushDepth, Declaring, true), true),
            ((PushDepth, Tuning, false), true),
            ((PushDepth, Tuning, true), true),
            ((RetainDepth, Declaring, false), true),
            ((RetainDepth, Declaring, true), true),
            ((RetainDepth, Tuning, false), true),
            ((RetainDepth, Tuning, true), true),
            ((StandingRetainDepth, Declaring, false), false),
            ((StandingRetainDepth, Declaring, true), true),
            ((StandingRetainDepth, Tuning, false), true),
            ((StandingRetainDepth, Tuning, true), true),
        ];
        assert_eq!(
            expected.len(),
            ChannelDepthKey::ALL.len() * 2 * 2,
            "the table covers every key, role and durability",
        );
        for ((key, role, durable), required) in expected {
            assert_eq!(
                depth_required(key, role, durable),
                required,
                "{} on a {role:?} block, durable = {durable}",
                key.word(),
            );
        }
    }

    #[test]
    fn only_a_disk_backed_channel_stands_or_archives() {
        assert!(standing_admitted(true));
        assert!(!standing_admitted(false));
        assert!(sink_admitted(true));
        assert!(!sink_admitted(false));
    }

    /// A depth a block may not state is never one it must state — the two
    /// predicates cannot both be true, on any row.
    #[test]
    fn admittance_and_requirement_never_disagree() {
        for durable in [false, true] {
            assert!(
                standing_admitted(durable)
                    || !depth_required(
                        ChannelDepthKey::StandingRetainDepth,
                        ChannelBlockRole::Declaring,
                        durable
                    ),
            );
        }
    }
}
