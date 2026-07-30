//! Randomized invariant tests for the ring, the cursor, and the deferred set.
//!
//! The generator is a fixed-seed linear congruential sequence rather than a
//! property-testing dependency: the crate is deliberately dependency-free, and a
//! deterministic sequence also means a failure reproduces exactly from the seed
//! printed in the assertion.

use crate::{
    Advance, DeferredSet, ReplayDecision, Resume, RetainedRing, SubscriberCursor,
    retention_frontier,
};

/// A deterministic source of small numbers.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        // Numerical Recipes' 64-bit constants; any full-period LCG does.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const SEEDS: [u64; 8] = [1, 7, 42, 99, 1_000, 65_537, 8_675_309, 4_294_967_291];

#[test]
fn ring_holds_a_dense_ascending_suffix_within_its_depth() {
    for seed in SEEDS {
        let mut rng = Lcg(seed);
        let depth = rng.below(6);
        let mut ring: RetainedRing<u64, u8> = RetainedRing::new(1, depth);
        let mut appended = 0u64;

        for _ in 0..200 {
            let value = rng.next();
            let append = ring.append(value);
            appended += 1;

            assert_eq!(append.seq, appended, "seed {seed}: seqs must be dense");
            assert!(
                ring.len() as u64 <= depth,
                "seed {seed}: ring must stay within depth"
            );
            assert_eq!(ring.newest_seq(), appended, "seed {seed}");

            let seqs: Vec<u64> = ring.iter().map(|e| e.seq).collect();
            let expected: Vec<u64> = ((appended + 1).saturating_sub(seqs.len() as u64)..=appended)
                .take(seqs.len())
                .collect();
            assert_eq!(
                seqs, expected,
                "seed {seed}: the ring is the newest contiguous suffix"
            );
        }
    }
}

#[test]
fn replay_from_a_covered_resume_is_exactly_what_is_owed() {
    for seed in SEEDS {
        let mut rng = Lcg(seed);
        let depth = 1 + rng.below(8);
        let mut ring: RetainedRing<u64, u8> = RetainedRing::new(1, depth);
        for _ in 0..100 {
            ring.append(rng.next());
        }

        let oldest = ring.oldest_seq().expect("depth >= 1 retains");
        let newest = ring.newest_seq();
        for last_seen in oldest - 1..=newest {
            let replay = ring.replay(Some(Resume {
                epoch: 1,
                seq: last_seen,
            }));
            let owed: Vec<u64> = ring.since(last_seen).map(|e| e.seq).collect();
            assert_eq!(
                replay.messages.iter().map(|e| e.seq).collect::<Vec<_>>(),
                owed,
                "seed {seed}: covered resume replays exactly the owed suffix"
            );
            let expected = if last_seen == newest {
                ReplayDecision::UpToDate
            } else {
                ReplayDecision::Exact
            };
            assert_eq!(replay.decision, expected, "seed {seed}");
        }
    }
}

/// Serve `cursor` its window and advance over it, as a push consumer does.
fn serve(
    cursor: &mut SubscriberCursor,
    ring: &RetainedRing<u64, u8>,
    push_limit: u64,
    retain_limit: u64,
) -> (Vec<u64>, Advance) {
    let window = cursor.window(ring, push_limit, retain_limit);
    // Everything unseen the window carried reached the subscriber, whether as
    // new or — for a retain limit above the push limit — as context.
    let unseen_from = cursor.next_owed();
    let served: Vec<u64> = window
        .entries
        .iter()
        .filter(|e| e.seq >= unseen_from)
        .map(|e| e.message)
        .collect();
    let advance = match window.advance_span() {
        Some((through, seen_floor)) => cursor.advance(ring, through, seen_floor),
        None => Advance {
            dropped: 0,
            noise_charge: 0,
        },
    };
    (served, advance)
}

/// Over any interleaving of appends and reads, each published message is either
/// handed to the subscriber or reported as its drop, exactly once — and the
/// noise stream partitions the same losses between the eviction that retired
/// them and the advance that passed them, with no double- or under-report.
#[test]
fn every_published_message_is_delivered_or_reported_exactly_once() {
    for seed in SEEDS {
        let mut rng = Lcg(seed);
        let ring_depth = 1 + rng.below(6);
        // A sampled (`push_depth = 0`) subscriber is deliberately outside this
        // accounting: it is never delivered to and never reported against.
        let push_depth = 1 + rng.below(4);
        let retain_depth = rng.below(4);
        let mut ring: RetainedRing<u64, u8> = RetainedRing::new(1, ring_depth);
        let mut cursor = SubscriberCursor::primed(&ring, push_depth);

        let mut published = 0u64;
        let mut delivered: Vec<u64> = Vec::new();
        let mut dropped = 0u64;
        let mut evicted_reports = 0u64;
        let mut noise = 0u64;

        for _ in 0..300 {
            if rng.below(3) > 0 {
                published += 1;
                let before = retention_frontier(&ring);
                ring.append(published);
                evicted_reports += cursor.evicted_since(&ring, before);
            } else {
                let (served, advance) = serve(&mut cursor, &ring, push_depth, retain_depth);
                delivered.extend(served);
                dropped += advance.dropped;
                noise += advance.noise_charge;
            }
        }
        let (served, advance) = serve(&mut cursor, &ring, push_depth, retain_depth);
        delivered.extend(served);
        dropped += advance.dropped;
        noise += advance.noise_charge;

        assert!(
            delivered.windows(2).all(|w| w[0] < w[1]),
            "seed {seed}: deliveries are strictly ascending and never duplicated"
        );
        assert_eq!(
            delivered.len() as u64 + dropped,
            published,
            "seed {seed}: every message is delivered or reported dropped, never both"
        );
        assert_eq!(
            evicted_reports + noise,
            dropped,
            "seed {seed}: the noise stream covers each drop exactly once"
        );
    }
}

#[test]
fn deferred_set_releases_in_time_order_and_never_exceeds_its_cap() {
    for seed in SEEDS {
        let mut rng = Lcg(seed);
        let cap = 1 + rng.below(8);
        let mut set: DeferredSet<u64> = DeferredSet::new(Some(cap));
        let mut parked = 0u64;
        let mut released = 0u64;
        let mut last_release_time = 0u64;
        let mut now = 0u64;
        let mut ids: Vec<u64> = Vec::new();

        for _ in 0..300 {
            match rng.below(4) {
                0 | 1 => {
                    let release_at = now + 1 + rng.below(20);
                    if let Ok(id) = set.park("alice", release_at, release_at) {
                        parked += 1;
                        ids.push(id);
                    }
                    assert!(
                        set.len() as u64 <= cap,
                        "seed {seed}: cap is never exceeded"
                    );
                }
                2 => {
                    if !ids.is_empty() {
                        let at = usize::try_from(rng.below(ids.len() as u64)).unwrap();
                        let id = ids.remove(at);
                        if set.cancel(id).is_some() {
                            parked -= 1;
                        }
                    }
                }
                _ => {
                    now += rng.below(8);
                    for entry in set.release_due(now) {
                        assert!(
                            entry.release_at >= last_release_time,
                            "seed {seed}: releases are monotone in release time"
                        );
                        assert!(
                            entry.release_at <= now,
                            "seed {seed}: nothing releases before it is due"
                        );
                        last_release_time = entry.release_at;
                        released += 1;
                        ids.retain(|id| *id != entry.id);
                    }
                }
            }
            assert_eq!(
                set.len() as u64,
                parked - released,
                "seed {seed}: occupancy accounts for every park, cancel, and release"
            );
            match set.next_release() {
                Some(next) => assert_eq!(
                    Some(next),
                    set.iter().map(|e| e.release_at).min(),
                    "seed {seed}: the head is the earliest due entry"
                ),
                None => assert!(set.is_empty(), "seed {seed}"),
            }
        }
    }
}
