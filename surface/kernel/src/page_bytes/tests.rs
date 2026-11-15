//! The writer buffer's rules and the ledger's count of what is alive.

use super::*;

const BIG: u64 = PAGE_MADE_BLOB_BYTES;

#[test]
fn out_of_order_and_overlapping_writes_seal_to_the_last_bytes_written() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    w.write(6, b"world", BIG).unwrap();
    w.write(0, b"HELLO ", BIG).unwrap();
    w.write(0, b"he", BIG).unwrap();
    assert_eq!(w.len(), 11);
    let sealed = w.seal().unwrap();
    assert_eq!(sealed.bytes(), b"heLLO world");
    assert_eq!(sealed.content_type(), "text/plain");
}

#[test]
fn an_empty_write_changes_nothing_however_far_out() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    w.write(u64::MAX, b"", 0).unwrap();
    assert_eq!(w.len(), 0);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn the_ledger_counts_a_writer_at_its_length_and_a_sealed_blob_once() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    assert_eq!(ledger.held(), 0);
    w.write(10, b"ab", BIG).unwrap();
    assert_eq!(ledger.held(), 12);
    // An overwrite inside the buffer grows nothing.
    w.write(0, b"0123456789", BIG).unwrap();
    assert_eq!(ledger.held(), 12);
    let sealed = w.seal().unwrap();
    let shared = sealed.clone();
    assert!(shared.same_blob(&sealed));
    assert_eq!(ledger.held(), 12);
    drop(sealed);
    assert_eq!(ledger.held(), 12, "a holder remains");
    drop(shared);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn a_dropped_writer_and_a_refused_seal_return_their_bytes() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    w.write(4, b"xy", BIG).unwrap();
    let (w, gap) = w.seal().unwrap_err();
    assert_eq!(gap, 0..4);
    assert_eq!(ledger.held(), 6);
    drop(w);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn equal_bytes_are_not_the_same_blob() {
    let ledger = MadeLedger::new();
    let seal = |bytes: &[u8]| {
        let mut w = WriteBuffer::new(&ledger, "text/plain".into());
        w.write(0, bytes, BIG).unwrap();
        w.seal().unwrap()
    };
    assert!(!seal(b"a").same_blob(&seal(b"a")));
}

#[test]
fn the_largest_blob_bounds_the_end_of_a_write() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    w.write(0, b"abcd", 4).unwrap();
    assert_eq!(w.write(3, b"ab", 4), Err(BufferRefusal::BlobTooLarge));
    assert_eq!(w.write(u64::MAX, b"a", 4), Err(BufferRefusal::BlobTooLarge));
    assert_eq!(w.len(), 4);
}

#[test]
fn the_page_budget_is_shared_by_every_buffer() {
    let ledger = MadeLedger::new();
    let mut first = WriteBuffer::new(&ledger, "text/plain".into());
    first.write(BIG - 2, b"ab", BIG).unwrap();
    assert!(!ledger.has_room());
    let mut second = WriteBuffer::new(&ledger, "text/plain".into());
    assert_eq!(
        second.write(0, b"a", BIG),
        Err(BufferRefusal::BudgetExhausted)
    );
    // Rewriting bytes the first already holds grows nothing, so it fits.
    first.write(0, b"zz", BIG).unwrap();
    drop(first);
    assert!(ledger.has_room());
    second.write(0, b"a", BIG).unwrap();
    assert_eq!(ledger.held(), 1);
}

#[test]
fn the_written_range_bound_refuses_one_more_disjoint_range() {
    let ledger = MadeLedger::new();
    let mut w = WriteBuffer::new(&ledger, "text/plain".into());
    for i in 0..SURFACE_MAX_WRITTEN_RANGES as u64 {
        w.write(i * 2, b"a", BIG).unwrap();
    }
    let len = w.len();
    let held = ledger.held();
    assert_eq!(
        w.write(len + 1, b"a", BIG),
        Err(BufferRefusal::TooFragmented)
    );
    assert_eq!(
        (w.len(), ledger.held()),
        (len, held),
        "the refusal changed nothing"
    );
    // Filling a hole merges two ranges into one, which is always accepted.
    w.write(1, b"a", BIG).unwrap();
    w.write(len + 1, b"a", BIG).unwrap();
}
