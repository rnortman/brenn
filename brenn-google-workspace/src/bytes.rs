//! Scans and comparisons of bytes eight at a time. A guest is charged fuel
//! per instruction, and a loop that tests one byte at a time spends most of
//! its instructions on the loop itself; these test a word of eight bytes at
//! once.
//! A mask marks each byte of a word that passes a test with that byte's high
//! bit, exactly: no byte's test disturbs another's.

/// One in every byte of a word.
const ONES: u64 = u64::from_le_bytes([1; 8]);
/// The high bit of every byte of a word.
const HIGH: u64 = u64::from_le_bytes([0x80; 8]);

/// The bytes of `word` under `n`, which is at most 0x80. Each byte has its
/// high bit set before `n` is taken from it, so no subtraction borrows from
/// the next byte.
pub(crate) const fn below(word: u64, n: u8) -> u64 {
    !((word | HIGH).wrapping_sub(ONES * n as u64)) & !word & HIGH
}

/// The bytes of `word` that are `byte`.
pub(crate) const fn equal(word: u64, byte: u8) -> u64 {
    below(word ^ (ONES * byte as u64), 1)
}

/// The bytes of `word` that are 0x80 or more.
pub(crate) const fn high(word: u64) -> u64 {
    word & HIGH
}

/// The first index from `at` of a byte `stop` holds, or `bytes.len()`.
/// `mask` marks the bytes of a word that `stop` holds. The two bytes from
/// `at` are tested alone first, so stops close together cost a test each.
#[inline]
pub(crate) fn find(
    bytes: &[u8],
    at: usize,
    mask: impl Fn(u64) -> u64,
    stop: impl Fn(u8) -> bool,
) -> usize {
    if bytes.get(at).is_some_and(|&b| stop(b)) {
        return at;
    }
    if bytes.get(at + 1).is_some_and(|&b| stop(b)) {
        return at + 1;
    }
    let mut words = bytes[at..].chunks_exact(8);
    for (index, chunk) in (&mut words).enumerate() {
        let marked = mask(u64::from_le_bytes(chunk.try_into().expect("eight bytes")));
        if marked != 0 {
            return at + index * 8 + (marked.trailing_zeros() / 8) as usize;
        }
    }
    let tail = bytes.len() - words.remainder().len();
    words
        .remainder()
        .iter()
        .position(|&b| stop(b))
        .map_or(bytes.len(), |found| tail + found)
}

/// Whether `a` and `b` are the same bytes, compared a word at a time.
#[inline]
pub fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let (a_words, b_words) = (a.chunks_exact(8), b.chunks_exact(8));
    let tails_same = a_words.remainder() == b_words.remainder();
    tails_same
        && a_words.zip(b_words).all(|(a, b)| {
            u64::from_ne_bytes(a.try_into().expect("a word"))
                == u64::from_ne_bytes(b.try_into().expect("a word"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each byte's mark in `mask`, first byte first.
    fn marks(mask: u64) -> [bool; 8] {
        mask.to_le_bytes().map(|b| b == 0x80)
    }

    #[test]
    fn a_mask_marks_exactly_the_bytes_that_pass() {
        for b in 0..=255u8 {
            for other in [0x00, b'a', 0x7f, 0x80, 0xff] {
                for at in 0..8 {
                    let mut bytes = [other; 8];
                    bytes[at] = b;
                    let word = u64::from_le_bytes(bytes);
                    for n in [1, 0x09, 0x0e, 0x20, 0x21, 0x80] {
                        assert_eq!(
                            marks(below(word, n)),
                            bytes.map(|x| x < n),
                            "{bytes:x?} {n:#x}"
                        );
                    }
                    for c in [0x00, b'&', b'<', 0x0b, 0x7f] {
                        assert_eq!(
                            marks(equal(word, c)),
                            bytes.map(|x| x == c),
                            "{bytes:x?} {c:#x}"
                        );
                    }
                    assert_eq!(marks(high(word)), bytes.map(|x| x >= 0x80), "{bytes:x?}");
                }
            }
        }
    }

    #[test]
    fn find_is_the_first_stop_from_where_it_starts() {
        let stop = |b: u8| b == b'<';
        let mask = |w: u64| equal(w, b'<');
        for len in 0..40 {
            for first in 0..len {
                for second in first..len {
                    let mut bytes = vec![b'x'; len];
                    bytes[first] = b'<';
                    bytes[second] = b'<';
                    for from in 0..=len {
                        let expected = [first, second, len]
                            .into_iter()
                            .find(|&at| at >= from)
                            .expect("len is at least from");
                        assert_eq!(
                            find(&bytes, from, mask, stop),
                            expected,
                            "{len} {first} {second} {from}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn bytes_are_the_same_exactly_when_equal() {
        let texts = [
            "",
            "a",
            "ab",
            "abcdefgh",
            "abcdefgi",
            "abcdefghi",
            "abcdefghj",
            "bbcdefghi",
        ];
        for a in texts {
            for b in texts {
                assert_eq!(same(a.as_bytes(), b.as_bytes()), a == b, "{a:?} {b:?}");
            }
        }
    }
}
