//! Conversation recycler (brenn:processor world).
//!
//! Each activation reads the three `config` knobs, loads the recycler's state
//! from `store` in one transaction, and observes every new envelope: one on
//! `interactions` counts and moves the idle clock, one on `activity` only moves
//! the clock, and a wake on `tick` is only a wake. It then decides. A fire
//! publishes the next epoch generation on `epoch` and cancels the parked tick;
//! a count still short of a rule re-parks the tick; a zero count parks
//! nothing. A changed state is written back and committed before `receive`
//! returns, and publishes flush only on that `Ok`, so the store always commits
//! before the epoch reaches the bus. An activation that changed nothing, such as
//! a tick with nothing new, rolls back and writes nothing.

mod logic;
mod spec;

use std::collections::BTreeMap;

use crate::logic::{Decision, Knobs, STORE_KEYS, State};
use crate::spec::port::{EPOCH, TICK};
use crate::spec::{InPort, config, log, store};
use brenn_guest::{Activation, Error, MessageEnvelope, Processor, publish, repark};

const STORE_NAMESPACE: &str = "recycler";
/// A tick's body is irrelevant — the wake is the message.
const TICK_BODY: &str = "{}";

struct ConversationRecycler;

/// An envelope's publish time in epoch milliseconds.
fn publish_ms(env: &MessageEnvelope) -> Result<u64, Error> {
    u64::try_from(env.publish_ts.timestamp_millis()).map_err(|_| {
        Error::malformed(format!(
            "envelope on {} has publish_ts {} before 1970",
            env.channel, env.publish_ts
        ))
    })
}

impl Processor for ConversationRecycler {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        let knobs = Knobs::from_lookup(config::get).map_err(Error::failed)?;
        let now = activation
            .now()
            .expect("backend activation carries a host-stamped now");

        let tx = store::begin()?;
        let mut raw: BTreeMap<&str, Vec<u8>> = BTreeMap::new();
        for key in STORE_KEYS {
            if let Some(bytes) = tx.get(STORE_NAMESPACE, key.as_bytes())? {
                raw.insert(key, bytes);
            }
        }
        let mut state = State::decode(|k| raw.get(k).cloned()).map_err(Error::failed)?;
        let loaded = state.clone();

        for window in activation.port_windows() {
            match InPort::of(window)? {
                InPort::Interactions => {
                    for env in window.new_envelopes() {
                        state.observe_interaction(publish_ms(&env?)?);
                    }
                }
                InPort::Activity => {
                    for env in window.new_envelopes() {
                        state.observe_activity(publish_ms(&env?)?);
                    }
                }
                InPort::Tick => {}
            }
        }

        let decision = state.decide(now, &knobs);
        let changed = state != loaded;
        // An epoch must never reach the bus with its generation unrecorded.
        assert!(
            changed || !matches!(decision, Decision::Fire { .. }),
            "BUG: a fire advances the generation, so it always has state to commit"
        );
        if changed {
            for (key, bytes) in state.encode() {
                tx.put(STORE_NAMESPACE, key.as_bytes(), &bytes)?;
            }
        }

        match decision {
            Decision::Fire {
                epoch,
                interactions,
                idle_ms,
            } => {
                publish(EPOCH, &epoch)?;
                repark(&activation, TICK, TICK_BODY, None);
                log::info(format!(
                    "published epoch {epoch} after {interactions} interactions and {idle_ms} ms idle"
                ));
            }
            Decision::Rearm { at_ms } => repark(&activation, TICK, TICK_BODY, Some(at_ms)),
            Decision::Idle => repark(&activation, TICK, TICK_BODY, None),
        }

        if changed {
            tx.commit()?;
        } else {
            tx.rollback();
        }
        Ok(None)
    }
}

brenn_guest::export_processor!(ConversationRecycler);
