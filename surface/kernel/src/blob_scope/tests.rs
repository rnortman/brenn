//! The scope's authority rules and answers against a scripted cache: what a
//! component may name, what `open` and `read` answer for each thing the cache
//! can hold, and which handles are faults.

use std::collections::BTreeMap;

use brenn_activation::PortWindow;
use brenn_envelope::grants::ComponentGrant;
use brenn_envelope::{Attachments, BlobId, ChannelScheme, Urgency};

use crate::test_support::bindings as fixtures;

use super::*;

const WIRE: &str = "brenn:site.bar.in";
const OTHER: &str = "brenn:site.bar.other";

/// What the cache holds, by `(channel, message id, name)`; anything absent is
/// [`Cached::Absent`].
#[derive(Default)]
struct FakeCache(BTreeMap<(String, Uuid, String), Holding>);

enum Holding {
    Bytes(Vec<u8>),
    Gone,
}

impl FakeCache {
    fn bytes(mut self, channel: &str, id: u128, name: &str, bytes: &[u8]) -> Self {
        self.0.insert(
            (channel.into(), Uuid::from_u128(id), name.into()),
            Holding::Bytes(bytes.to_vec()),
        );
        self
    }

    fn gone(mut self, channel: &str, id: u128, name: &str) -> Self {
        self.0.insert(
            (channel.into(), Uuid::from_u128(id), name.into()),
            Holding::Gone,
        );
        self
    }
}

impl CacheView for FakeCache {
    fn cached(&self, channel: &str, message_id: Uuid, name: &AttachmentName) -> Cached<'_> {
        match self
            .0
            .get(&(channel.to_string(), message_id, name.as_str().to_string()))
        {
            Some(Holding::Bytes(bytes)) => Cached::Bytes(bytes),
            Some(Holding::Gone) => Cached::Gone,
            None => Cached::Absent,
        }
    }
}

fn env(id: u128, channel: &str, attachments: &[(&str, u64)]) -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::from_u128(id),
        source: "test".into(),
        channel: channel.into(),
        sender: "app:cam".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: format!("m{id}").into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
        transport: None,
        attachments: Attachments::from_map(
            attachments
                .iter()
                .map(|&(name, size)| {
                    (
                        AttachmentName::new(name).expect("a fixture attachment name"),
                        BlobRef {
                            id: BlobId::new(format!("b{id}{name}")),
                            size,
                            content_type: "image/jpeg".into(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

/// Port `in` on `WIRE` windows messages 1 (`a`, 4 bytes) and 2 (`a`, `b`).
fn scope() -> BlobScope {
    let mut scope = BlobScope::empty();
    scope.add_window(
        "in",
        WIRE.into(),
        &[
            env(1, WIRE, &[("a", 4)]),
            env(2, WIRE, &[("a", 3), ("b", 0)]),
        ],
    );
    scope
}

fn cache() -> FakeCache {
    FakeCache::default().bytes(WIRE, 1, "a", b"\x00\xff\x10\x20")
}

#[test]
fn an_attachment_in_the_window_opens_and_reads_its_bytes() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    assert_eq!(scope.size(handle), Ok(4));
    assert_eq!(scope.content_type(handle), Ok("image/jpeg"));
    assert_eq!(
        scope.read(&cache, handle, 0, 4),
        Ok(Ok(vec![0x00, 0xff, 0x10, 0x20]))
    );
    assert_eq!(scope.read(&cache, handle, 1, 2), Ok(Ok(vec![0xff, 0x10])));
}

#[test]
fn a_port_index_or_name_outside_the_windows_is_not_permitted() {
    let mut scope = scope();
    let cache = cache();
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "elsewhere", 0, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 2, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", u32::MAX, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 0, "b"),
        Err(OpenRefusal::NotPermitted)
    );
    // Not a well-formed name at all, so no message carries it.
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 0, "../a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(scope.live_handles(), 0);
}

#[test]
fn a_blob_id_opens_nothing() {
    // The only way to name an attachment is (port, index, name); an id read out
    // of a header is a name no message carries.
    let mut scope = scope();
    assert_eq!(
        scope.open(&cache(), &PageBlobs::new(), "in", 0, "b1a"),
        Err(OpenRefusal::NotPermitted)
    );
}

#[test]
fn an_empty_scope_names_nothing() {
    let mut scope = BlobScope::empty();
    assert_eq!(
        scope.open(&cache(), &PageBlobs::new(), "in", 0, "a"),
        Err(OpenRefusal::NotPermitted)
    );
}

#[test]
fn a_message_the_server_says_left_its_window_is_not_found() {
    let mut scope = scope();
    let cache = FakeCache::default().gone(WIRE, 2, "a");
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 1, "a"),
        Err(OpenRefusal::NotFound)
    );
    // The other attachment of the same message is the cache's own question.
    assert!(scope.open(&cache, &PageBlobs::new(), "in", 1, "b").is_ok());
}

#[test]
fn bytes_not_on_the_page_open_and_read_unavailable() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 1, "a")
        .expect("opens without bytes");
    assert_eq!(scope.size(handle), Ok(3));
    assert_eq!(
        scope.read(&cache, handle, 0, 3),
        Ok(Err(ReadRefusal::Unavailable))
    );
}

#[test]
fn a_gone_entry_after_open_reads_unavailable() {
    // The open saw bytes on their way; the server then answered `NotRetained`.
    let mut scope = scope();
    let handle = scope
        .open(&FakeCache::default(), &PageBlobs::new(), "in", 1, "a")
        .expect("opens");
    let cache = FakeCache::default().gone(WIRE, 2, "a");
    assert_eq!(
        scope.read(&cache, handle, 0, 1),
        Ok(Err(ReadRefusal::Unavailable))
    );
}

#[test]
fn read_bounds() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    assert_eq!(
        scope.read(&cache, handle, 0, SURFACE_MAX_READ_BYTES + 1),
        Ok(Err(ReadRefusal::TooLarge))
    );
    assert_eq!(
        scope.read(&cache, handle, 2, 3),
        Ok(Err(ReadRefusal::OutOfRange))
    );
    assert_eq!(
        scope.read(&cache, handle, u64::MAX, 1),
        Ok(Err(ReadRefusal::OutOfRange))
    );
    assert_eq!(scope.read(&cache, handle, 2, 0), Ok(Ok(Vec::new())));
    assert_eq!(scope.read(&cache, handle, 4, 0), Ok(Ok(Vec::new())));
    assert_eq!(
        scope.read(&cache, handle, 5, 0),
        Ok(Err(ReadRefusal::OutOfRange))
    );
}

#[test]
fn a_zero_length_attachment_reads_empty() {
    let mut scope = scope();
    let cache = FakeCache::default().bytes(WIRE, 2, "b", b"");
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 1, "b")
        .expect("opens");
    assert_eq!(scope.size(handle), Ok(0));
    assert_eq!(scope.read(&cache, handle, 0, 0), Ok(Ok(Vec::new())));
}

#[test]
fn the_sixty_fifth_handle_is_quota_exceeded_until_one_is_dropped() {
    let mut scope = scope();
    let cache = cache();
    let handles: Vec<u32> = (0..SURFACE_MAX_HANDLES)
        .map(|_| {
            scope
                .open(&cache, &PageBlobs::new(), "in", 0, "a")
                .expect("within the quota")
        })
        .collect();
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 0, "a"),
        Err(OpenRefusal::QuotaExceeded)
    );
    scope.drop_handle(handles[0]).expect("held");
    assert!(scope.open(&cache, &PageBlobs::new(), "in", 0, "a").is_ok());
}

#[test]
fn a_dropped_or_never_minted_handle_is_a_fault() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    scope.drop_handle(handle).expect("held");
    assert_eq!(scope.size(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.content_type(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.read(&cache, handle, 0, 1), Err(UnknownHandle(handle)));
    assert_eq!(scope.drop_handle(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.size(999), Err(UnknownHandle(999)));
    // Numbers are not reused, so a stale one never names a newer attachment.
    let next = scope
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    assert_ne!(next, handle);
}

#[test]
fn another_activations_handle_is_a_fault() {
    let cache = cache();
    let mut first = scope();
    let second = scope();
    let theirs = first
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    assert_eq!(second.size(theirs), Err(UnknownHandle(theirs)));
    assert_eq!(
        second.read(&cache, theirs, 0, 1),
        Err(UnknownHandle(theirs))
    );
}

#[test]
fn the_cache_is_read_under_the_ports_channel() {
    // The same message id on another channel is another cache entry.
    let mut scope = BlobScope::empty();
    scope.add_window("other", OTHER.into(), &[env(1, OTHER, &[("a", 4)])]);
    let cache = cache();
    let handle = scope
        .open(&cache, &PageBlobs::new(), "other", 0, "a")
        .expect("opens");
    assert_eq!(
        scope.read(&cache, handle, 0, 4),
        Ok(Err(ReadRefusal::Unavailable))
    );
}

fn activation(windows: Vec<(&str, Vec<MessageEnvelope>)>) -> PageActivation {
    PageActivation {
        ports: windows
            .into_iter()
            .map(|(port, envelopes)| PortWindow {
                port: port.into(),
                new_from: 0,
                dropped: 0,
                envelopes,
            })
            .collect(),
        deferred: Vec::new(),
        now: None,
        sync: None,
    }
}

fn bindings(grants: &[ComponentGrant]) -> AppliedBindings {
    let doc = fixtures::doc(
        vec![
            fixtures::component_with_grants("p1", "kind", grants),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            fixtures::subscription("p1", "in", WIRE, 1, 1),
            fixtures::subscription("p1", "other", OTHER, 1, 1),
        ],
        Vec::new(),
        Vec::new(),
    );
    AppliedBindings::apply(&doc.to_body()).expect("the fixture document applies")
}

#[test]
fn a_granted_instances_scope_is_its_windows_under_their_bound_channels() {
    let bindings = bindings(&[ComponentGrant::Ports, ComponentGrant::Blobs]);
    let mut scope = BlobScope::of(
        &bindings,
        "p1",
        &activation(vec![
            ("in", vec![env(1, WIRE, &[("a", 4)])]),
            ("other", vec![env(7, OTHER, &[("z", 1)])]),
        ]),
        &HandleNumbers::new(),
    );
    let cache = cache().bytes(OTHER, 7, "z", b"\x01");
    let handle = scope
        .open(&cache, &PageBlobs::new(), "in", 0, "a")
        .expect("opens");
    assert_eq!(
        scope.read(&cache, handle, 0, 4).map(|r| r.map(|b| b.len())),
        Ok(Ok(4))
    );
    let handle = scope
        .open(&cache, &PageBlobs::new(), "other", 0, "z")
        .expect("opens");
    assert_eq!(scope.read(&cache, handle, 0, 1), Ok(Ok(vec![0x01])));
}

#[test]
fn an_ungranted_instances_scope_is_empty() {
    let bindings = bindings(&[ComponentGrant::Ports]);
    let mut scope = BlobScope::of(
        &bindings,
        "p1",
        &activation(vec![("in", vec![env(1, WIRE, &[("a", 4)])])]),
        &HandleNumbers::new(),
    );
    assert_eq!(
        scope.open(&cache(), &PageBlobs::new(), "in", 0, "a"),
        Err(OpenRefusal::NotPermitted)
    );
}

/// A mount, a gesture or a peer's call windows the request on a sync port no
/// binding names, beside every bound port.
#[test]
fn a_sync_activations_request_window_is_outside_the_scope() {
    let bindings = bindings(&[ComponentGrant::Blobs]);
    let mut sync = activation(vec![
        ("in", vec![env(1, WIRE, &[("a", 4)])]),
        ("call", vec![env(2, WIRE, &[("b", 4)])]),
    ]);
    sync.sync = Some("call".into());
    let mut scope = BlobScope::of(&bindings, "p1", &sync, &HandleNumbers::new());
    let cache = cache();
    assert!(scope.open(&cache, &PageBlobs::new(), "in", 0, "a").is_ok());
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "call", 0, "b"),
        Err(OpenRefusal::NotPermitted)
    );
}

#[test]
#[should_panic(expected = "do not bind")]
fn a_window_on_an_unbound_port_panics() {
    let bindings = bindings(&[ComponentGrant::Blobs]);
    let _ = BlobScope::of(
        &bindings,
        "p1",
        &activation(vec![("nowhere", vec![env(1, WIRE, &[("a", 4)])])]),
        &HandleNumbers::new(),
    );
}

// ── the producing side ─────────────────────────────────────────────────────

const TEXT: &str = "text/plain";
const LARGEST: u64 = PAGE_MADE_BLOB_BYTES;

/// A sealed blob of `bytes` under `scope`, its writer's number with it.
fn made(scope: &mut BlobScope, ledger: &MadeLedger, bytes: &[u8]) -> (u32, u32) {
    let writer = scope.create(ledger, TEXT).expect("creates");
    scope
        .write(writer, 0, bytes, LARGEST)
        .expect("a live writer")
        .expect("writes");
    let blob = scope.seal(writer).expect("a live writer").expect("seals");
    (writer, blob)
}

#[test]
fn a_made_blob_reads_back_what_was_written_out_of_order() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, "application/octet-stream").unwrap();
    scope
        .write(writer, 3, &[0xff, 0xfe], LARGEST)
        .unwrap()
        .unwrap();
    scope
        .write(writer, 0, &[0, 1, 2, 9], LARGEST)
        .unwrap()
        .unwrap();
    let blob = scope.seal(writer).unwrap().unwrap();
    assert_ne!(blob, writer, "the blob is a new handle");
    assert_eq!(scope.size(blob), Ok(5));
    assert_eq!(scope.content_type(blob), Ok("application/octet-stream"));
    // The later write covered byte 3.
    assert_eq!(
        scope.read(&FakeCache::default(), blob, 0, 5),
        Ok(Ok(vec![0, 1, 2, 9, 0xfe]))
    );
    assert_eq!(
        scope.read(&FakeCache::default(), blob, 4, 2),
        Ok(Err(ReadRefusal::OutOfRange))
    );
    assert!(matches!(scope.blob(blob), Ok(Blob::Bytes(_))));
}

#[test]
fn a_gap_is_answered_then_filled_and_sealed() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    scope.write(writer, 0, b"ab", LARGEST).unwrap().unwrap();
    scope.write(writer, 5, b"f", LARGEST).unwrap().unwrap();
    assert_eq!(scope.seal(writer), Ok(Err(Gap { offset: 2, len: 3 })));
    // The writer stays open and counted.
    assert_eq!(scope.live_handles(), 1);
    scope.write(writer, 2, b"cde", LARGEST).unwrap().unwrap();
    let blob = scope.seal(writer).unwrap().unwrap();
    assert_eq!(
        scope.read(&FakeCache::default(), blob, 0, 6),
        Ok(Ok(b"abcdef".to_vec()))
    );
}

#[test]
fn an_empty_writer_seals_to_an_empty_blob() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let (_, blob) = made(&mut scope, &ledger, b"");
    assert_eq!(scope.size(blob), Ok(0));
    assert_eq!(
        scope.read(&FakeCache::default(), blob, 0, 0),
        Ok(Ok(Vec::new()))
    );
}

#[test]
fn a_zero_length_write_changes_nothing_as_the_backend_stores() {
    // An empty write at offset 1000 is accepted and the extent stays 0, as in
    // the backend store.
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    assert_eq!(scope.write(writer, 1000, b"", LARGEST), Ok(Ok(())));
    assert_eq!(scope.write(writer, u64::MAX, b"", 0), Ok(Ok(())));
    assert_eq!(ledger.held(), 0);
    let blob = scope.seal(writer).unwrap().unwrap();
    assert_eq!(scope.size(blob), Ok(0));
}

#[test]
fn a_spent_writer_faults_on_write_and_seal_and_drops_freeing_nothing() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let (writer, blob) = made(&mut scope, &ledger, b"abc");
    assert_eq!(
        scope.write(writer, 0, b"x", LARGEST),
        Err(WriterFault::Spent(writer))
    );
    assert_eq!(scope.seal(writer), Err(WriterFault::Spent(writer)));
    assert_eq!(scope.drop_writer(writer), Ok(()));
    assert_eq!(ledger.held(), 3, "the blob still holds its bytes");
    assert_eq!(scope.size(blob), Ok(3));
    // Once dropped, the writer's number names nothing.
    assert_eq!(scope.drop_writer(writer), Err(WriterFault::Unknown(writer)));
}

#[test]
fn writer_and_blob_numbers_are_not_interchangeable() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let open_writer = scope.create(&ledger, TEXT).unwrap();
    let (spent, blob) = made(&mut scope, &ledger, b"a");
    // A writer is not a blob, spent or not.
    for writer in [open_writer, spent] {
        assert_eq!(scope.size(writer), Err(UnknownHandle(writer)));
        assert_eq!(
            scope.read(&FakeCache::default(), writer, 0, 0),
            Err(UnknownHandle(writer))
        );
        assert_eq!(scope.drop_handle(writer), Err(UnknownHandle(writer)));
        assert!(scope.blob(writer).is_err());
    }
    // A blob is not a writer.
    assert_eq!(
        scope.write(blob, 0, b"x", LARGEST),
        Err(WriterFault::Unknown(blob))
    );
    assert_eq!(scope.seal(blob), Err(WriterFault::Unknown(blob)));
    assert_eq!(scope.drop_writer(blob), Err(WriterFault::Unknown(blob)));
    // Nor is a number nobody minted.
    assert_eq!(
        scope.write(999, 0, b"x", LARGEST),
        Err(WriterFault::Unknown(999))
    );
}

#[test]
fn another_activations_writer_or_blob_is_a_fault() {
    // Two scopes of the page at once, as a caller's and its sync callee's are;
    // the second has minted as many handles as the first.
    let ledger = MadeLedger::new();
    let numbers = HandleNumbers::new();
    let mut first = BlobScope::numbered(&numbers);
    let mut second = BlobScope::numbered(&numbers);
    let writer = first.create(&ledger, TEXT).unwrap();
    let (_, blob) = made(&mut first, &ledger, b"a");
    let own_writer = second.create(&ledger, TEXT).unwrap();
    made(&mut second, &ledger, b"b");
    assert_eq!(
        second.write(writer, 0, b"x", LARGEST),
        Err(WriterFault::Unknown(writer))
    );
    assert_eq!(second.seal(writer), Err(WriterFault::Unknown(writer)));
    assert_eq!(second.size(blob), Err(UnknownHandle(blob)));
    assert_eq!(second.drop_handle(blob), Err(UnknownHandle(blob)));
    assert_eq!(second.write(own_writer, 0, b"x", LARGEST), Ok(Ok(())));
}

#[test]
fn a_handle_from_an_earlier_activation_names_nothing() {
    // A number the page minted for an earlier activation is never minted
    // again, so a later scope that minted as many handles holds none of them.
    let ledger = MadeLedger::new();
    let numbers = HandleNumbers::new();
    let mut earlier = BlobScope::numbered(&numbers);
    let kept_writer = earlier.create(&ledger, TEXT).unwrap();
    let (writer, blob) = made(&mut earlier, &ledger, b"a");
    drop(earlier);
    let mut later = BlobScope::numbered(&numbers);
    let later_writer = later.create(&ledger, TEXT).unwrap();
    let (_, later_blob) = made(&mut later, &ledger, b"bb");
    for stale in [kept_writer, writer, blob] {
        assert_ne!(stale, later_writer);
        assert_ne!(stale, later_blob);
        assert_eq!(later.size(stale), Err(UnknownHandle(stale)));
        assert_eq!(later.drop_handle(stale), Err(UnknownHandle(stale)));
        assert_eq!(
            later.write(stale, 0, b"x", LARGEST),
            Err(WriterFault::Unknown(stale))
        );
        assert_eq!(later.drop_writer(stale), Err(WriterFault::Unknown(stale)));
    }
    assert_eq!(later.size(later_blob), Ok(2));
    assert_eq!(
        ledger.held(),
        2,
        "only the later blob: the earlier scope freed its own"
    );
}

#[test]
fn create_checks_the_handle_bound_then_the_type_then_the_budget() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    // Fill the page's budget with one writer.
    let big = scope.create(&ledger, TEXT).unwrap();
    for i in 0..(PAGE_MADE_BLOB_BYTES / u64::from(SURFACE_MAX_WRITE_BYTES)) {
        let chunk = vec![0u8; SURFACE_MAX_WRITE_BYTES as usize];
        scope
            .write(big, i * u64::from(SURFACE_MAX_WRITE_BYTES), &chunk, LARGEST)
            .unwrap()
            .unwrap();
    }
    assert_eq!(ledger.held(), PAGE_MADE_BLOB_BYTES);
    // Type ahead of budget.
    assert!(matches!(
        scope.create(&ledger, "not a type"),
        Err(CreateRefusal::InvalidContentType(_))
    ));
    assert_eq!(
        scope.create(&ledger, TEXT),
        Err(CreateRefusal::BudgetExhausted)
    );
    // The handle bound ahead of both.
    let mut full = BlobScope::empty();
    let free = MadeLedger::new();
    for _ in 0..SURFACE_MAX_HANDLES {
        full.create(&free, TEXT).unwrap();
    }
    assert_eq!(
        full.create(&free, "not a type"),
        Err(CreateRefusal::QuotaExceeded)
    );
    assert_eq!(
        full.create(&ledger, TEXT),
        Err(CreateRefusal::QuotaExceeded)
    );
    assert_eq!(CreateRefusal::QuotaExceeded.tag(), "quota-exceeded");
    assert_eq!(
        CreateRefusal::InvalidContentType(String::new()).tag(),
        "invalid-content-type"
    );
    assert_eq!(CreateRefusal::BudgetExhausted.tag(), "budget-exhausted");
}

#[test]
fn writers_and_blobs_share_the_handle_bound_and_a_seal_never_trips_it() {
    let ledger = MadeLedger::new();
    let mut scope = scope();
    let cache = cache();
    let opened = scope.open(&cache, &PageBlobs::new(), "in", 0, "a").unwrap();
    let writers: Vec<u32> = (1..SURFACE_MAX_HANDLES)
        .map(|_| scope.create(&ledger, TEXT).unwrap())
        .collect();
    assert_eq!(scope.live_handles(), SURFACE_MAX_HANDLES);
    assert_eq!(
        scope.create(&ledger, TEXT),
        Err(CreateRefusal::QuotaExceeded)
    );
    assert_eq!(
        scope.open(&cache, &PageBlobs::new(), "in", 0, "a"),
        Err(OpenRefusal::QuotaExceeded)
    );
    // At the bound, a seal passes the writer's slot to the blob.
    assert!(scope.seal(writers[0]).unwrap().is_ok());
    assert_eq!(scope.live_handles(), SURFACE_MAX_HANDLES);
    // Dropping the spent writer frees no slot; dropping a blob does.
    scope.drop_writer(writers[0]).unwrap();
    assert_eq!(
        scope.create(&ledger, TEXT),
        Err(CreateRefusal::QuotaExceeded)
    );
    scope.drop_handle(opened).unwrap();
    assert!(scope.create(&ledger, TEXT).is_ok());
}

#[test]
fn write_checks_its_bounds_in_order_and_a_refusal_changes_nothing() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    let max = SURFACE_MAX_WRITE_BYTES as usize;

    // too-large ahead of everything: the write is also past the largest blob.
    assert_eq!(
        scope.write(writer, u64::MAX, &vec![0; max + 1], 0),
        Ok(Err(WriteRefusal::TooLarge))
    );
    // blob-too-large ahead of the budget and the range bound.
    assert_eq!(
        scope.write(writer, 4, b"ab", 5),
        Ok(Err(WriteRefusal::BlobTooLarge))
    );
    assert_eq!(scope.bytes_written(), 0);
    assert_eq!(ledger.held(), 0);
    assert_eq!(scope.write(writer, 3, b"ab", 5), Ok(Ok(())));
    assert_eq!(scope.bytes_written(), 2);
    assert_eq!(ledger.held(), 5);
    assert_eq!(WriteRefusal::TooLarge.tag(), "too-large");
    assert_eq!(WriteRefusal::BlobTooLarge.tag(), "blob-too-large");
    assert_eq!(WriteRefusal::BudgetExhausted.tag(), "budget-exhausted");
    assert_eq!(WriteRefusal::TooFragmented.tag(), "too-fragmented");
    assert_eq!(WriteRefusal::QuotaExceeded.tag(), "quota-exceeded");
}

#[test]
fn the_activations_bytes_written_bound_counts_overwrites_and_comes_before_the_blob() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    let chunk = vec![7u8; SURFACE_MAX_WRITE_BYTES as usize];
    // Rewriting the same mebibyte: the page holds 1 MiB, the activation has
    // written 64.
    let rounds = SURFACE_MAX_BYTES_WRITTEN_PER_ACTIVATION / u64::from(SURFACE_MAX_WRITE_BYTES);
    for _ in 0..rounds {
        scope.write(writer, 0, &chunk, LARGEST).unwrap().unwrap();
    }
    assert_eq!(
        scope.bytes_written(),
        SURFACE_MAX_BYTES_WRITTEN_PER_ACTIVATION
    );
    assert_eq!(ledger.held(), u64::from(SURFACE_MAX_WRITE_BYTES));
    // One more byte is quota-exceeded, even where it would be blob-too-large.
    assert_eq!(
        scope.write(writer, 0, b"x", LARGEST),
        Ok(Err(WriteRefusal::QuotaExceeded))
    );
    assert_eq!(
        scope.write(writer, 10, b"x", 1),
        Ok(Err(WriteRefusal::QuotaExceeded))
    );
    // An empty write is still accepted.
    assert_eq!(scope.write(writer, 0, b"", LARGEST), Ok(Ok(())));
    // A new activation starts its own count.
    let mut next = BlobScope::empty();
    let other = next.create(&ledger, TEXT).unwrap();
    assert_eq!(next.write(other, 0, b"x", LARGEST), Ok(Ok(())));
}

#[test]
fn the_page_budget_is_checked_before_the_range_bound() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    for i in 0..crate::page_bytes::SURFACE_MAX_WRITTEN_RANGES as u64 {
        scope.write(writer, i * 2, b"a", LARGEST).unwrap().unwrap();
    }
    let len = 2 * crate::page_bytes::SURFACE_MAX_WRITTEN_RANGES as u64;
    // Far enough out to pass the page's budget and a new disjoint range.
    assert_eq!(
        scope.write(writer, PAGE_MADE_BLOB_BYTES, b"a", u64::MAX),
        Ok(Err(WriteRefusal::BudgetExhausted))
    );
    assert_eq!(
        scope.write(writer, len + 1, b"a", LARGEST),
        Ok(Err(WriteRefusal::TooFragmented))
    );
    assert_eq!(ledger.held(), len - 1);
}

#[test]
fn the_largest_blob_is_the_door_s_when_smaller() {
    use brenn_attach_proto::BlobFacts;
    let door = |max_blob_bytes| BlobFacts {
        max_blob_bytes,
        max_chunk_bytes: 256 << 10,
        max_handles: 64,
        max_bytes_held: 4 * max_blob_bytes,
        max_inflight_reads: 4,
        upload: true,
    };
    assert_eq!(largest_made_blob(None), PAGE_MADE_BLOB_BYTES);
    assert_eq!(largest_made_blob(Some(&door(1 << 20))), 1 << 20);
    assert_eq!(
        largest_made_blob(Some(&door(PAGE_MADE_BLOB_BYTES * 2))),
        PAGE_MADE_BLOB_BYTES
    );
}

#[test]
fn the_ledger_is_back_to_zero_when_the_scope_goes() {
    // An activation's scope is dropped with its frame on `ok`, `err` and a trap
    // alike: everything it made and nothing else held is freed.
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let open_writer = scope.create(&ledger, TEXT).unwrap();
    scope
        .write(open_writer, 0, b"abcd", LARGEST)
        .unwrap()
        .unwrap();
    made(&mut scope, &ledger, b"xyz");
    assert_eq!(ledger.held(), 7);
    drop(scope);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn dropping_a_writer_or_the_last_holder_of_a_blob_frees_its_bytes() {
    let ledger = MadeLedger::new();
    let mut scope = BlobScope::empty();
    let writer = scope.create(&ledger, TEXT).unwrap();
    scope.write(writer, 0, b"abcd", LARGEST).unwrap().unwrap();
    scope.drop_writer(writer).unwrap();
    assert_eq!(ledger.held(), 0);

    let (_, blob) = made(&mut scope, &ledger, b"xyz");
    let Ok(Blob::Bytes(shared)) = scope.blob(blob).cloned() else {
        panic!("a made blob is page bytes");
    };
    scope.drop_handle(blob).unwrap();
    assert_eq!(ledger.held(), 3, "another holder keeps the bytes");
    drop(shared);
    assert_eq!(ledger.held(), 0);
}

#[test]
fn an_opened_attachment_is_a_backend_reference_by_its_delivery() {
    let mut scope = scope();
    let handle = scope
        .open(&cache(), &PageBlobs::new(), "in", 1, "b")
        .unwrap();
    let Ok(Blob::Remote(remote)) = scope.blob(handle) else {
        panic!("an opened attachment is a backend reference");
    };
    assert_eq!(remote.channel, WIRE);
    assert_eq!(remote.message_id, Uuid::from_u128(2));
    assert_eq!(remote.name.as_str(), "b");
    assert_eq!(remote.blob.id, BlobId::new("b2b".to_string()));
    assert_eq!(remote.blob.size, 0);
}
