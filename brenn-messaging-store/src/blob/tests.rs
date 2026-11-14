use std::ops::Range;
use std::sync::Arc;

use brenn_envelope::{AttachmentName, BlobId, MAX_ATTACHMENTS_PER_MESSAGE};

use super::*;

const LIMITS: BlobLimits = BlobLimits {
    max_blob_bytes: 1 << 20,
    budget_bytes: 4 << 20,
};

struct Fixture {
    dir: tempfile::TempDir,
    store: Arc<BlobStore>,
}

fn fixture_with(limits: BlobLimits) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStore::for_tests(dir.path(), limits);
    Fixture { dir, store }
}

fn fixture() -> Fixture {
    fixture_with(LIMITS)
}

impl Fixture {
    fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn sealed(&self, bytes: &[u8]) -> BlobHold {
        let mut w = self.store.create("application/octet-stream").unwrap();
        w.write(0, bytes).unwrap();
        w.seal().unwrap()
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn usage(sealed: usize, unsealed: usize, charged_bytes: u64) -> BlobUsage {
    BlobUsage {
        sealed,
        unsealed,
        charged_bytes,
    }
}

#[test]
fn pieces_in_order_seal_and_read_back() {
    let f = fixture();
    let mut w = f.store.create("image/jpeg").unwrap();
    w.write(0, b"hello ").unwrap();
    w.write(6, b"world").unwrap();
    assert_eq!(w.extent(), 11);
    let hold = w.seal().unwrap();
    let r = hold.reference();
    assert_eq!(r.size, 11);
    assert_eq!(r.content_type, "image/jpeg");
    assert_eq!(hold.read(0, 11).unwrap(), b"hello world");
    assert_eq!(hold.read(6, 5).unwrap(), b"world");
    assert_eq!(hold.read(11, 0).unwrap(), b"");
}

#[test]
fn pieces_out_of_order_and_overwritten() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(6, b"world").unwrap();
    w.write(0, b"HELLO ").unwrap();
    w.write(0, b"hello").unwrap();
    w.write(3, b"LO wo").unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.read(0, 11).unwrap(), b"helLO world");
}

#[test]
fn id_is_32_lowercase_hex_and_the_file_name() {
    let f = fixture();
    let hold = f.sealed(b"x");
    let id = hold.id().as_str().to_owned();
    assert_eq!(id.len(), 32);
    assert!(
        id.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    assert!(is_minted_id(&id));
    assert_eq!(f.files(), vec![id]);
    let other = f.sealed(b"y");
    assert_ne!(other.id(), hold.id());
}

#[test]
fn every_minted_id_is_recognised_as_a_blob_file_name() {
    for _ in 0..256 {
        assert!(is_minted_id(mint_id().as_str()));
    }
    assert!(!is_minted_id(&"0".repeat(31)));
    assert!(!is_minted_id(&"A".repeat(32)));
    assert!(!is_minted_id(&format!("{}g", "0".repeat(31))));
}

#[test]
fn a_message_without_attachments_shares_the_one_empty_hold_set() {
    let (_, holds) = HeldAttachments::none().into_parts();
    assert!(holds.is_empty());
    assert!(Arc::ptr_eq(&holds, &no_holds()));
}

#[test]
fn file_mode_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture();
    let hold = f.sealed(b"x");
    let meta = std::fs::metadata(f.dir.path().join(hold.id().as_str())).unwrap();
    assert_eq!(meta.permissions().mode() & 0o777, 0o600);
}

#[test]
fn read_past_the_size_is_refused() {
    let f = fixture();
    let hold = f.sealed(b"0123456789");
    for (offset, len) in [(0, 11), (10, 1), (11, 0), (u64::MAX, 1)] {
        match hold.read(offset, len) {
            Err(BlobError::OutOfRange {
                offset: o,
                len: l,
                size: 10,
            }) => assert_eq!((o, l), (offset, len)),
            other => panic!("({offset}, {len}): {other:?}"),
        }
    }
}

#[test]
fn read_into_fills_the_buffer_and_refuses_past_the_size_untouched() {
    let f = fixture();
    let hold = f.sealed(b"0123456789");
    let mut buf = [b'x'; 4];
    hold.read_into(3, &mut buf).unwrap();
    assert_eq!(&buf, b"3456");
    hold.read_into(6, &mut buf).unwrap();
    assert_eq!(&buf, b"6789");
    let mut buf = [b'x'; 4];
    match hold.read_into(7, &mut buf) {
        Err(BlobError::OutOfRange {
            offset: 7,
            len: 4,
            size: 10,
        }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(&buf, b"xxxx");
}

#[test]
fn seal_over_a_gap_is_refused_naming_it_then_accepted_once_filled() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(2, b"cd").unwrap();
    w.write(6, b"gh").unwrap();
    let refused = w.seal().unwrap_err();
    assert_eq!(refused.gap, 0..2);
    let mut w = refused.writer;
    w.write(0, b"ab").unwrap();
    let refused = w.seal().unwrap_err();
    assert_eq!(refused.gap, 4..6);
    let mut w = refused.writer;
    w.write(4, b"ef").unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.read(0, 8).unwrap(), b"abcdefgh");
}

#[test]
fn empty_blob_seals() {
    let f = fixture();
    let w = f.store.create("application/octet-stream").unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.reference().size, 0);
    assert_eq!(hold.read(0, 0).unwrap(), b"");
    assert_eq!(f.store.usage(), usage(1, 0, 0));
}

#[test]
fn empty_write_changes_nothing() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(1000, b"").unwrap();
    assert_eq!(w.extent(), 0);
    assert_eq!(f.store.usage(), usage(0, 1, 0));
}

#[test]
fn cap_refuses_including_a_one_byte_write_far_out() {
    let f = fixture_with(BlobLimits {
        max_blob_bytes: 100,
        budget_bytes: 1000,
    });
    let mut w = f.store.create("text/plain").unwrap();
    assert!(matches!(
        w.write(0, &[0; 101]),
        Err(BlobError::TooLarge { max: 100 })
    ));
    assert!(matches!(
        w.write(100, b"x"),
        Err(BlobError::TooLarge { max: 100 })
    ));
    assert!(matches!(
        w.write(u64::MAX, b"x"),
        Err(BlobError::TooLarge { max: 100 })
    ));
    w.write(99, b"x").unwrap();
    assert_eq!(w.extent(), 100);
}

#[test]
fn budget_refuses_including_a_one_byte_write_far_out() {
    let f = fixture_with(BlobLimits {
        max_blob_bytes: 100,
        budget_bytes: 150,
    });
    let _first = f.sealed(&[1; 100]);
    let mut w = f.store.create("text/plain").unwrap();
    assert!(matches!(
        w.write(0, &[0; 51]),
        Err(BlobError::BudgetExhausted { budget: 150 })
    ));
    assert!(matches!(
        w.write(50, b"x"),
        Err(BlobError::BudgetExhausted { budget: 150 })
    ));
    // Nothing was charged or written by the refusals.
    assert_eq!(w.extent(), 0);
    assert_eq!(f.store.usage(), usage(1, 1, 100));
    w.write(49, b"x").unwrap();
    assert_eq!(f.store.usage(), usage(1, 1, 150));
}

#[test]
fn refused_write_leaves_the_writer_usable() {
    let f = fixture_with(BlobLimits {
        max_blob_bytes: 10,
        budget_bytes: 100,
    });
    let mut w = f.store.create("text/plain").unwrap();
    w.write(0, b"abc").unwrap();
    assert!(w.write(2, &[0; 20]).is_err());
    w.write(3, b"def").unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.read(0, 6).unwrap(), b"abcdef");
}

/// Leave `w` as a write of `len` bytes at `offset` that failed after its
/// first `landed` bytes reached the file — what a short `pwrite` followed by
/// an error leaves — and run the writer's repair: the charge is reserved as
/// `write` reserves it, the landed bytes are written, then
/// `undo_failed_write` runs.
fn fail_write(w: &mut BlobWriter, offset: u64, len: u64, landed: &[u8]) {
    let end = offset + len;
    let new_extent = w.extent.max(end);
    w.store.recharge(&w.id, w.extent, new_extent).unwrap();
    w.file.write_all_at(landed, offset).unwrap();
    w.undo_failed_write(offset..end, new_extent, &io::Error::other("injected"));
}

#[test]
fn a_failed_write_past_the_extent_leaves_the_file_at_the_extent() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(0, &[1; 100]).unwrap();
    fail_write(&mut w, 100, 1000, &[9; 500]);
    assert_eq!(w.extent(), 100);
    assert_eq!(f.store.usage(), usage(0, 1, 100));
    let path = f.dir.path().join(w.id.as_str());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 100);

    // The bytes below the failed range are intact, so the blob seals at the
    // extent its file has.
    let hold = w.seal().unwrap();
    assert_eq!(hold.reference().size, 100);
    assert_eq!(hold.read(0, 100).unwrap(), [1; 100]);
}

#[test]
fn a_failed_write_over_written_bytes_must_be_rewritten_before_seal() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(0, &[1; 100]).unwrap();
    fail_write(&mut w, 50, 100, &[9; 20]);
    assert_eq!(w.extent(), 100);
    assert_eq!(f.store.usage(), usage(0, 1, 100));
    let path = f.dir.path().join(w.id.as_str());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 100);

    let refused = w.seal().unwrap_err();
    assert_eq!(refused.gap, 50..100);
    let mut w = refused.writer;
    w.write(50, &[3; 50]).unwrap();
    let hold = w.seal().unwrap();
    let mut expected = vec![1; 50];
    expected.extend([3; 50]);
    assert_eq!(hold.read(0, 100).unwrap(), expected);
}

#[test]
fn remove_range_cuts_and_splits() {
    assert_eq!(remove_range(&ranges(&[]), 3..5), ranges(&[]));
    assert_eq!(
        remove_range(&ranges(&[(0, 10)]), 3..5),
        ranges(&[(0, 3), (5, 10)])
    );
    assert_eq!(
        remove_range(&ranges(&[(0, 4), (6, 10)]), 2..8),
        ranges(&[(0, 2), (8, 10)])
    );
    assert_eq!(
        remove_range(&ranges(&[(0, 4), (6, 10)]), 0..10),
        ranges(&[])
    );
    assert_eq!(
        remove_range(&ranges(&[(0, 4), (6, 10)]), 4..6),
        ranges(&[(0, 4), (6, 10)])
    );
    assert_eq!(remove_range(&ranges(&[(0, 4)]), 2..100), ranges(&[(0, 2)]));
}

#[test]
fn range_set_is_bounded() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    for i in 0..MAX_WRITTEN_RANGES as u64 {
        w.write(i * 2, b"x").unwrap();
    }
    assert!(matches!(
        w.write(MAX_WRITTEN_RANGES as u64 * 2, b"x"),
        Err(BlobError::TooFragmented)
    ));
    // A write that merges ranges is still accepted at the bound.
    w.write(1, b"x").unwrap();
    w.write(MAX_WRITTEN_RANGES as u64 * 2, b"x").unwrap();
}

/// Ranges from `(start, end)` pairs.
fn ranges(pairs: &[(u64, u64)]) -> Vec<Range<u64>> {
    pairs.iter().map(|&(start, end)| start..end).collect()
}

#[test]
fn merge_range_merges_overlapping_and_adjacent() {
    assert_eq!(merge_range(&ranges(&[]), 3..5), ranges(&[(3, 5)]));
    assert_eq!(
        merge_range(&ranges(&[(0, 2), (8, 9)]), 4..5),
        ranges(&[(0, 2), (4, 5), (8, 9)])
    );
    assert_eq!(
        merge_range(&ranges(&[(0, 2), (8, 9)]), 2..8),
        ranges(&[(0, 9)])
    );
    assert_eq!(
        merge_range(&ranges(&[(0, 2), (4, 6), (8, 9)]), 1..5),
        ranges(&[(0, 6), (8, 9)])
    );
    assert_eq!(
        merge_range(&ranges(&[(3, 4)]), 0..1),
        ranges(&[(0, 1), (3, 4)])
    );
    assert_eq!(
        merge_range(&ranges(&[(3, 4)]), 5..6),
        ranges(&[(3, 4), (5, 6)])
    );
}

#[test]
fn first_gap_finds_the_lowest_uncovered_range() {
    assert_eq!(first_gap(&ranges(&[]), 0), None);
    assert_eq!(first_gap(&ranges(&[(0, 5)]), 5), None);
    assert_eq!(first_gap(&ranges(&[(2, 5)]), 5), Some(0..2));
    assert_eq!(first_gap(&ranges(&[(0, 2), (3, 5)]), 5), Some(2..3));
}

#[test]
fn unsealed_blob_is_not_retainable() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(0, b"x").unwrap();
    let id = BlobId::new(f.files().pop().unwrap());
    assert!(matches!(f.store.retain(&id), Retain::NotFound));
    let hold = w.seal().unwrap();
    assert_eq!(hold.id(), &id);
    match f.store.retain(&id) {
        Retain::Held(h) => assert_eq!(h.read(0, 1).unwrap(), b"x"),
        other => panic!("sealed, held blob not retained: {other:?}"),
    }
}

#[test]
fn unknown_id_is_not_retainable() {
    let f = fixture();
    let id = BlobId::new("0".repeat(32));
    assert!(matches!(f.store.retain(&id), Retain::NotFound));
}

#[test]
fn dropped_writer_removes_its_file_and_its_charge() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(0, &[7; 64]).unwrap();
    assert_eq!(f.files().len(), 1);
    assert_eq!(f.store.usage(), usage(0, 1, 64));
    drop(w);
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
}

#[test]
fn writer_dropped_after_a_refused_seal_is_removed() {
    let f = fixture();
    let mut w = f.store.create("text/plain").unwrap();
    w.write(4, b"x").unwrap();
    let refused = w.seal().unwrap_err();
    drop(refused);
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
}

#[test]
fn sealed_unpublished_blob_goes_when_its_producer_drops_it() {
    let f = fixture();
    let hold = f.sealed(&[1; 32]);
    let id = hold.id().clone();
    assert_eq!(f.store.usage(), usage(1, 0, 32));
    drop(hold);
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
    assert!(matches!(f.store.retain(&id), Retain::NotFound));
}

#[test]
fn every_hold_must_drop_before_the_blob_goes() {
    let f = fixture();
    let hold = f.sealed(b"abc");
    let id = hold.id().clone();
    let clone = hold.clone();
    let Retain::Held(retained) = f.store.retain(&id) else {
        panic!("not retained");
    };
    drop(hold);
    drop(retained);
    assert_eq!(f.files().len(), 1);
    assert_eq!(clone.read(0, 3).unwrap(), b"abc");
    drop(clone);
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
}

#[test]
fn invalid_content_type_is_refused() {
    let f = fixture();
    for bad in ["", "text", "text/", "a b/c", "tëxt/plain"] {
        assert!(
            matches!(f.store.create(bad), Err(BlobError::InvalidContentType(_))),
            "{bad:?}"
        );
    }
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
}

#[test]
fn reading_a_held_blob_whose_file_is_gone_panics() {
    let f = fixture();
    let hold = f.sealed(b"abc");
    let path = f.dir.path().join(hold.id().as_str());
    std::fs::remove_file(&path).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hold.read(0, 1)))
        .expect_err("read of a missing held file returned");
    let message = panic.downcast_ref::<String>().unwrap();
    assert!(message.contains("is missing"), "{message}");
    // Put the file back so the hold's drop can unlink it.
    std::fs::write(&path, b"abc").unwrap();
}

#[test]
fn reading_a_held_blob_whose_file_was_cut_short_panics() {
    let f = fixture();
    let hold = f.sealed(b"abcdef");
    let path = f.dir.path().join(hold.id().as_str());
    std::fs::write(&path, b"abc").unwrap();
    // A range inside what is left still reads.
    assert_eq!(hold.read(0, 3).unwrap(), b"abc");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hold.read(2, 4)))
        .expect_err("read past the end of a cut file returned");
    let message = panic.downcast_ref::<String>().unwrap();
    assert!(
        message.contains("ends inside the read of 4 bytes at offset 2")
            && message.contains("size is 6"),
        "{message}"
    );
}

#[test]
fn held_attachments_records_refs_and_one_hold_per_blob() {
    let f = fixture();
    let a = f.sealed(b"aaaa");
    let b = f.sealed(b"bb");
    let held = HeldAttachments::new([
        (name("photo"), a.clone()),
        (name("thumb"), b.clone()),
        (name("photo-again"), a.clone()),
    ])
    .unwrap();
    assert_eq!(held.refs().len(), 3);
    assert_eq!(held.refs().get(&name("photo")), Some(&a.reference()));
    assert_eq!(held.refs().get(&name("photo-again")), Some(&a.reference()));
    assert_eq!(held.refs().get(&name("thumb")), Some(&b.reference()));
    assert_eq!(held.holds().len(), 2);

    // The set alone keeps both blobs.
    drop(a);
    drop(b);
    assert_eq!(f.files().len(), 2);
    drop(held);
    assert!(f.files().is_empty());
}

#[test]
fn held_attachments_none_is_empty() {
    let none = HeldAttachments::none();
    assert!(none.is_empty());
    assert!(none.refs().is_empty());
    assert!(none.holds().is_empty());
}

#[test]
fn held_attachments_refuses_a_duplicate_name() {
    let f = fixture();
    let a = f.sealed(b"a");
    let b = f.sealed(b"b");
    assert_eq!(
        HeldAttachments::new([(name("x"), a), (name("x"), b)]).unwrap_err(),
        AttachmentError::DuplicateName(name("x"))
    );
    // The refused set dropped its holds.
    assert!(f.files().is_empty());
}

#[test]
fn held_attachments_refuses_too_many() {
    let f = fixture();
    let hold = f.sealed(b"a");
    let at_bound = (0..MAX_ATTACHMENTS_PER_MESSAGE).map(|i| (name(&format!("n{i}")), hold.clone()));
    assert_eq!(
        HeldAttachments::new(at_bound).unwrap().refs().len(),
        MAX_ATTACHMENTS_PER_MESSAGE
    );
    let over = (0..=MAX_ATTACHMENTS_PER_MESSAGE).map(|i| (name(&format!("n{i}")), hold.clone()));
    assert_eq!(
        HeldAttachments::new(over).unwrap_err(),
        AttachmentError::TooMany {
            count: MAX_ATTACHMENTS_PER_MESSAGE + 1
        }
    );
}

#[test]
fn holds_are_shareable_across_threads() {
    let f = fixture();
    let hold = f.sealed(&[9; 1000]);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let h = hold.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let c = h.clone();
                    assert_eq!(c.read(500, 10).unwrap(), vec![9; 10]);
                }
            })
        })
        .collect();
    drop(hold);
    for t in threads {
        t.join().unwrap();
    }
    assert!(f.files().is_empty());
    assert_eq!(f.store.usage(), usage(0, 0, 0));
}

// ── Reserve ─────────────────────────────────────────────────────────────

/// A reservation charges the blob's whole extent before any byte, and the
/// writes that fill it charge nothing more.
#[test]
fn reserve_charges_the_whole_extent_before_any_write() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    w.reserve(10).unwrap();
    assert_eq!(w.extent(), 10);
    assert_eq!(f.store.usage(), usage(0, 1, 10));
    w.write(0, b"01234").unwrap();
    w.write(5, b"56789").unwrap();
    assert_eq!(f.store.usage(), usage(0, 1, 10));
    let hold = w.seal().unwrap();
    assert_eq!(hold.reference().size, 10);
    assert_eq!(hold.read(0, 10).unwrap(), b"0123456789");
    assert_eq!(f.store.usage(), usage(1, 0, 10));
}

/// A reserved extent is a promise of bytes: seal refuses until every byte
/// below it is written.
#[test]
fn seal_after_reserve_refuses_until_the_extent_is_filled() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    w.reserve(8).unwrap();
    w.write(0, b"abcd").unwrap();
    let SealRefused { mut writer, gap } = w.seal().unwrap_err();
    assert_eq!(gap, 4..8);
    writer.write(4, b"efgh").unwrap();
    let hold = writer.seal().unwrap();
    assert_eq!(hold.read(0, 8).unwrap(), b"abcdefgh");
}

/// A reservation past the per-blob cap is refused and changes nothing.
#[test]
fn reserve_past_the_blob_cap_is_refused_too_large() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    let err = w.reserve(LIMITS.max_blob_bytes + 1).unwrap_err();
    assert!(matches!(err, BlobError::TooLarge { max } if max == LIMITS.max_blob_bytes));
    assert_eq!(w.extent(), 0);
    assert_eq!(f.store.usage(), usage(0, 1, 0));
    w.reserve(LIMITS.max_blob_bytes).unwrap();
}

/// A reservation that would pass the store's budget is refused and changes
/// nothing; one that fits beside the others is charged.
#[test]
fn reserve_past_the_budget_is_refused_budget_exhausted() {
    let f = fixture_with(BlobLimits {
        max_blob_bytes: 100,
        budget_bytes: 150,
    });
    let mut first = f.store.create("application/pdf").unwrap();
    first.reserve(100).unwrap();
    let mut second = f.store.create("application/pdf").unwrap();
    let err = second.reserve(51).unwrap_err();
    assert!(matches!(err, BlobError::BudgetExhausted { budget: 150 }));
    assert_eq!(second.extent(), 0);
    assert_eq!(f.store.usage(), usage(0, 2, 100));
    second.reserve(50).unwrap();
    assert_eq!(f.store.usage(), usage(0, 2, 150));
}

/// A reservation at or below the extent changes nothing.
#[test]
fn reserve_below_the_extent_changes_nothing() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    w.write(0, b"abcdef").unwrap();
    w.reserve(3).unwrap();
    w.reserve(6).unwrap();
    assert_eq!(w.extent(), 6);
    assert_eq!(f.store.usage(), usage(0, 1, 6));
    w.reserve(0).unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.reference().size, 6);
}

/// A reserved writer dropped unfilled deletes its file and returns its
/// whole charge.
#[test]
fn a_reserved_writer_dropped_unfilled_returns_its_charge() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    w.reserve(1000).unwrap();
    w.write(0, b"part").unwrap();
    assert_eq!(f.store.usage(), usage(0, 1, 1000));
    assert_eq!(f.files().len(), 1);
    drop(w);
    assert_eq!(f.store.usage(), usage(0, 0, 0));
    assert!(f.files().is_empty());
}

/// A zero-byte reservation seals an empty blob.
#[test]
fn a_zero_byte_reservation_seals_an_empty_blob() {
    let f = fixture();
    let mut w = f.store.create("application/pdf").unwrap();
    w.reserve(0).unwrap();
    let hold = w.seal().unwrap();
    assert_eq!(hold.reference().size, 0);
    assert_eq!(hold.read(0, 0).unwrap(), b"");
}
