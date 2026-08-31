//! The `u64` encoding behind a DOM capability handle: slot index, generation,
//! and the arithmetic that keeps a stale handle from naming a stranger.
//!
//! This is the confinement mechanism's numeric half. A handle table reuses a
//! slot once the element it held is destroyed, so the only thing standing
//! between a handle held across that reuse and the element that took the slot's
//! place is the generation packed alongside the index. That makes an off-by-one
//! here a cross-element aliasing bug rather than a cosmetic one.

/// A handle over a slot index and that slot's generation.
///
/// The index rides the low half incremented by one, which is what keeps zero
/// out of the valid range: slot 0 of generation 1 is `0x1_0000_0001`.
pub fn pack(index: usize, generation: u32) -> u64 {
    assert!(generation != 0, "dom: zero is not a generation");
    let index = index
        .checked_add(1)
        .and_then(|index| u32::try_from(index).ok())
        .expect("dom: a handle table over 4G slots");
    (u64::from(generation) << 32) | u64::from(index)
}

/// The slot index and generation a handle names, or `None` for one that cannot
/// name a slot at all.
pub fn unpack(handle: u64) -> Option<(usize, u32)> {
    let index = usize::try_from(handle & 0xffff_ffff).ok()?.checked_sub(1)?;
    let generation = u32::try_from(handle >> 32).ok()?;
    (generation != 0).then_some((index, generation))
}

/// The generation a freed slot moves on to. Wraps back to 1 rather than through
/// 0, which is not a generation.
pub fn next_generation(generation: u32) -> u32 {
    match generation.wrapping_add(1) {
        0 => 1,
        next => next,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_names_no_slot() {
        assert_eq!(unpack(0), None);
    }

    #[test]
    fn a_handle_with_no_generation_names_no_slot() {
        // The low half alone is a plausible-looking small integer, which is
        // exactly what an uninitialised guest field holds.
        assert_eq!(unpack(1), None);
        assert_eq!(unpack(0xffff_ffff), None);
    }

    #[test]
    fn the_index_round_trips_at_both_ends_of_the_range() {
        assert_eq!(unpack(pack(0, 1)), Some((0, 1)));
        assert_eq!(unpack(pack(0, u32::MAX)), Some((0, u32::MAX)));
        let last = u32::MAX as usize - 1;
        assert_eq!(unpack(pack(last, 7)), Some((last, 7)));
    }

    #[test]
    fn slot_zero_of_generation_one_is_not_zero() {
        assert_eq!(pack(0, 1), 0x1_0000_0001);
    }

    #[test]
    fn the_generation_is_carried_beside_the_index_and_not_into_it() {
        let (index, generation) = unpack(pack(9, 4)).expect("a packed handle unpacks");
        assert_eq!((index, generation), (9, 4));
        assert_ne!(pack(9, 4), pack(9, 5), "a reuse changes the handle");
        assert_ne!(pack(9, 4), pack(10, 4), "a sibling slot changes the handle");
    }

    #[test]
    fn a_generation_never_moves_on_to_zero() {
        assert_eq!(next_generation(1), 2);
        assert_eq!(next_generation(u32::MAX), 1);
    }

    #[test]
    #[should_panic(expected = "not a generation")]
    fn packing_a_zero_generation_is_refused() {
        pack(0, 0);
    }
}
