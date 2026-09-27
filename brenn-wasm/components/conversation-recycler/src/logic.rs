//! The recycler's decisions, in plain `std`: reading the knobs, counting
//! interactions against per-port high-water marks, and deciding whether to
//! fire, re-arm the tick, or go quiet. Every timestamp is epoch milliseconds.

pub const DEFAULT_IDLE_SECS: u64 = 180;
pub const DEFAULT_MAX_INTERACTIONS: u64 = 50;
pub const DEFAULT_SETTLE_SECS: u64 = 10;
/// The tick never polls more often than this.
const MIN_POLL_MS: u64 = 1000;

/// The store keys, in encode order.
pub const STORE_KEYS: [&str; 5] = [
    "interactions",
    "last_activity_ms",
    "mark/interactions",
    "mark/activity",
    "generation",
];

/// The instance's three `config` knobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Knobs {
    pub idle_secs: u64,
    pub max_interactions: u64,
    pub settle_secs: u64,
}

impl Knobs {
    /// Read the three keys through `get`. An absent key takes its default.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Knobs, String> {
        let read = |key: &str, unit: &str, default: u64| -> Result<u64, String> {
            match get(key) {
                None => Ok(default),
                Some(s) => s
                    .parse::<u64>()
                    .map_err(|_| format!("config {key}: {s:?} is not a whole number of {unit}")),
            }
        };
        let knobs = Knobs {
            idle_secs: read("idle_secs", "seconds", DEFAULT_IDLE_SECS)?,
            max_interactions: read("max_interactions", "interactions", DEFAULT_MAX_INTERACTIONS)?,
            settle_secs: read("settle_secs", "seconds", DEFAULT_SETTLE_SECS)?,
        };
        // Zero would fire the max rule with nothing counted.
        if knobs.max_interactions == 0 {
            return Err("config max_interactions: must be at least 1".to_string());
        }
        Ok(knobs)
    }

    /// max(MIN_POLL_MS, min(idle_secs, settle_secs) × 500), saturating.
    pub fn poll_ms(&self) -> u64 {
        self.idle_secs
            .min(self.settle_secs)
            .saturating_mul(500)
            .max(MIN_POLL_MS)
    }
}

/// What the store keeps between activations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// Interactions counted since the last fire.
    pub interactions: u64,
    /// The newest publish time seen on either port.
    pub last_activity_ms: u64,
    /// The newest publish time counted on `interactions`.
    pub interactions_mark_ms: u64,
    /// The newest publish time seen on `activity`.
    pub activity_mark_ms: u64,
    /// The last epoch published; zero before the first fire.
    pub generation: u64,
}

/// What one activation does after observing its windows.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// Publish `epoch`; cancel the parked tick.
    Fire {
        epoch: String,
        interactions: u64,
        idle_ms: u64,
    },
    /// Park the next tick at `at_ms`.
    Rearm { at_ms: u64 },
    /// Nothing to reset: cancel any parked tick.
    Idle,
}

impl State {
    /// Rebuild the state from the store through `read`. An absent key is zero;
    /// a value that is not a big-endian `u64` is refused naming its key.
    pub fn decode(read: impl Fn(&str) -> Option<Vec<u8>>) -> Result<State, String> {
        let mut values = [0u64; 5];
        for (value, key) in values.iter_mut().zip(STORE_KEYS) {
            if let Some(bytes) = read(key) {
                let n = bytes.len();
                let bytes: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| format!("store recycler/{key}: {n} bytes, expected 8"))?;
                *value = u64::from_be_bytes(bytes);
            }
        }
        let [
            interactions,
            last_activity_ms,
            interactions_mark_ms,
            activity_mark_ms,
            generation,
        ] = values;
        Ok(State {
            interactions,
            last_activity_ms,
            interactions_mark_ms,
            activity_mark_ms,
            generation,
        })
    }

    /// Every field as a store key and its big-endian bytes, in `STORE_KEYS` order.
    pub fn encode(&self) -> [(&'static str, [u8; 8]); 5] {
        let values = [
            self.interactions,
            self.last_activity_ms,
            self.interactions_mark_ms,
            self.activity_mark_ms,
            self.generation,
        ];
        let mut out = [("", [0u8; 8]); 5];
        for ((slot, key), value) in out.iter_mut().zip(STORE_KEYS).zip(values) {
            *slot = (key, value.to_be_bytes());
        }
        out
    }

    /// One message on `interactions`, published at `publish_ms`. A message no
    /// newer than the mark is a redelivery and counts nothing.
    pub fn observe_interaction(&mut self, publish_ms: u64) {
        if publish_ms <= self.interactions_mark_ms {
            return;
        }
        self.interactions = self
            .interactions
            .checked_add(1)
            .expect("interaction count overflowed u64");
        self.interactions_mark_ms = publish_ms;
        self.last_activity_ms = self.last_activity_ms.max(publish_ms);
    }

    /// One message on `activity`: moves the idle clock, counts nothing.
    pub fn observe_activity(&mut self, publish_ms: u64) {
        if publish_ms <= self.activity_mark_ms {
            return;
        }
        self.activity_mark_ms = publish_ms;
        self.last_activity_ms = self.last_activity_ms.max(publish_ms);
    }

    /// Fire, re-arm or go quiet at `now_ms`. A fire advances the generation and
    /// resets the count; the marks and the idle clock are never reset.
    pub fn decide(&mut self, now_ms: u64, knobs: &Knobs) -> Decision {
        let idle = now_ms.saturating_sub(self.last_activity_ms);
        let idle_rule = self.interactions >= 1 && idle >= knobs.idle_secs.saturating_mul(1000);
        let max_rule = self.interactions >= knobs.max_interactions
            && idle >= knobs.settle_secs.saturating_mul(1000);
        if idle_rule || max_rule {
            self.generation = self
                .generation
                .checked_add(1)
                .expect("epoch generation overflowed u64");
            let interactions = self.interactions;
            self.interactions = 0;
            return Decision::Fire {
                epoch: self.generation.to_string(),
                interactions,
                idle_ms: idle,
            };
        }
        if self.interactions >= 1 {
            Decision::Rearm {
                at_ms: now_ms.saturating_add(knobs.poll_ms()),
            }
        } else {
            Decision::Idle
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn defaults() -> Knobs {
        Knobs::from_lookup(|_| None).expect("defaults parse")
    }

    fn knobs_from(pairs: &[(&str, &str)]) -> Result<Knobs, String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Knobs::from_lookup(|k| map.get(k).cloned())
    }

    /// Unpack a `Fire`, panicking on anything else.
    fn fired(decision: Decision) -> (String, u64, u64) {
        let Decision::Fire {
            epoch,
            interactions,
            idle_ms,
        } = decision
        else {
            panic!("expected Fire, got {decision:?}");
        };
        (epoch, interactions, idle_ms)
    }

    #[test]
    fn absent_keys_take_their_defaults() {
        let knobs = defaults();
        assert_eq!(knobs.idle_secs, 180);
        assert_eq!(knobs.max_interactions, 50);
        assert_eq!(knobs.settle_secs, 10);
    }

    #[test]
    fn a_set_key_overrides_its_default() {
        let knobs = knobs_from(&[("idle_secs", "120"), ("max_interactions", "7")]).unwrap();
        assert_eq!(
            knobs,
            Knobs {
                idle_secs: 120,
                max_interactions: 7,
                settle_secs: 10,
            }
        );
    }

    #[test]
    fn an_unparseable_value_is_refused_naming_its_key() {
        let err = knobs_from(&[("idle_secs", "soon")]).unwrap_err();
        assert!(err.contains("idle_secs"), "{err}");
        assert!(err.contains("seconds"), "{err}");
        let err = knobs_from(&[("max_interactions", "-1")]).unwrap_err();
        assert!(err.contains("max_interactions"), "{err}");
        assert!(err.contains("interactions"), "{err}");
    }

    #[test]
    fn zero_max_interactions_is_refused_and_zero_windows_are_not() {
        let err = knobs_from(&[("max_interactions", "0")]).unwrap_err();
        assert_eq!(err, "config max_interactions: must be at least 1");
        let knobs = knobs_from(&[("idle_secs", "0"), ("settle_secs", "0")]).unwrap();
        assert_eq!(knobs.idle_secs, 0);
        assert_eq!(knobs.settle_secs, 0);
    }

    #[test]
    fn poll_ms_is_half_the_shorter_window_and_never_under_a_second() {
        let knobs = |idle_secs, settle_secs| Knobs {
            idle_secs,
            max_interactions: 50,
            settle_secs,
        };
        assert_eq!(knobs(180, 10).poll_ms(), 5000);
        assert_eq!(knobs(4, 10).poll_ms(), 2000);
        assert_eq!(knobs(1, 0).poll_ms(), 1000);
    }

    #[test]
    fn an_interaction_counts_once_by_its_timestamp() {
        let mut state = State::default();
        for ms in [1000, 1000, 900, 2000] {
            state.observe_interaction(ms);
        }
        assert_eq!(state.interactions, 2);
        assert_eq!(state.interactions_mark_ms, 2000);
        assert_eq!(state.last_activity_ms, 2000);
    }

    #[test]
    fn activity_moves_the_idle_clock_without_counting() {
        let mut state = State::default();
        state.observe_activity(5000);
        assert_eq!(state.interactions, 0);
        assert_eq!(state.last_activity_ms, 5000);
        assert_eq!(state.activity_mark_ms, 5000);

        state.observe_activity(4000);
        assert_eq!(
            state.activity_mark_ms, 5000,
            "a redelivered activity changes nothing"
        );
        assert_eq!(state.last_activity_ms, 5000);

        state.observe_interaction(3000);
        assert_eq!(state.interactions, 1, "the marks are per port");
        assert_eq!(state.last_activity_ms, 5000);
    }

    #[test]
    fn a_redelivered_tail_neither_counts_nor_restarts_the_clock() {
        let mut state = State {
            interactions: 3,
            last_activity_ms: 10_000,
            interactions_mark_ms: 10_000,
            ..State::default()
        };
        for ms in [8000, 9000, 10_000] {
            state.observe_interaction(ms);
        }
        assert_eq!(state.interactions, 3);
        assert_eq!(state.last_activity_ms, 10_000);

        let (epoch, interactions, idle_ms) = fired(state.decide(190_000, &defaults()));
        assert_eq!(epoch, "1");
        assert_eq!(interactions, 3);
        assert_eq!(idle_ms, 180_000);
    }

    #[test]
    fn the_idle_rule_fires_once_the_visitor_has_gone() {
        let knobs = defaults();
        // One interaction, its last activity at 0.
        let mut state = State {
            interactions: 1,
            ..State::default()
        };

        assert_eq!(
            state.decide(179_999, &knobs),
            Decision::Rearm { at_ms: 184_999 }
        );
        let (epoch, interactions, idle_ms) = fired(state.decide(180_000, &knobs));
        assert_eq!(epoch, "1");
        assert_eq!(interactions, 1);
        assert_eq!(idle_ms, 180_000);
        assert_eq!(state.interactions, 0);
        assert_eq!(state.generation, 1);
    }

    #[test]
    fn the_max_interactions_rule_fires_after_the_settle_window() {
        let knobs = defaults();
        let mut state = State {
            interactions: 50,
            ..State::default()
        };

        assert!(
            matches!(state.decide(9_999, &knobs), Decision::Rearm { .. }),
            "inside the settle window"
        );
        let (epoch, interactions, idle_ms) = fired(state.decide(10_000, &knobs));
        assert_eq!(epoch, "1");
        assert_eq!(interactions, 50);
        assert_eq!(idle_ms, 10_000);
    }

    #[test]
    fn nothing_fires_or_polls_without_an_interaction() {
        let mut state = State {
            generation: 4,
            ..State::default()
        };
        assert_eq!(state.decide(u64::MAX, &defaults()), Decision::Idle);
        assert_eq!(state.generation, 4);
    }

    /// Only an observation or a fire changes the state, so an activation with
    /// nothing new has nothing to write back.
    #[test]
    fn a_tick_with_nothing_new_leaves_the_state_as_loaded() {
        let mut state = State {
            interactions: 2,
            last_activity_ms: 10_000,
            interactions_mark_ms: 10_000,
            generation: 3,
            ..State::default()
        };
        let before = state.clone();
        assert_eq!(
            state.decide(11_000, &defaults()),
            Decision::Rearm { at_ms: 16_000 }
        );
        assert_eq!(state, before);

        let mut state = State {
            generation: 4,
            ..State::default()
        };
        let before = state.clone();
        assert_eq!(state.decide(u64::MAX, &defaults()), Decision::Idle);
        assert_eq!(state, before);
    }

    #[test]
    fn each_fire_advances_the_generation_across_a_store_round_trip() {
        let knobs = defaults();
        let mut state = State::default();
        state.observe_interaction(1000);
        let (first, _, _) = fired(state.decide(1_000_000, &knobs));
        state.observe_interaction(2_000_000);
        let (second, _, _) = fired(state.decide(3_000_000, &knobs));
        assert_eq!((first.as_str(), second.as_str()), ("1", "2"));

        let stored: BTreeMap<&str, Vec<u8>> = state
            .encode()
            .into_iter()
            .map(|(key, bytes)| (key, bytes.to_vec()))
            .collect();
        let mut restored = State::decode(|k| stored.get(k).cloned()).unwrap();
        assert_eq!(restored, state);

        restored.observe_interaction(4_000_000);
        let (third, interactions, _) = fired(restored.decide(5_000_000, &knobs));
        assert_eq!(third, "3");
        assert_eq!(interactions, 1);
    }

    #[test]
    fn an_empty_store_decodes_to_the_zero_state() {
        assert_eq!(State::decode(|_| None).unwrap(), State::default());
    }

    #[test]
    fn a_value_of_the_wrong_width_is_refused_naming_its_key() {
        let err = State::decode(|k| (k == "generation").then(|| vec![0u8; 4])).unwrap_err();
        assert!(err.contains("recycler/generation"), "{err}");
        assert!(err.contains("4 bytes"), "{err}");
    }
}
