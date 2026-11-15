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
    let handle = scope.open(&cache, "in", 0, "a").expect("opens");
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
        scope.open(&cache, "elsewhere", 0, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, "in", 2, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, "in", u32::MAX, "a"),
        Err(OpenRefusal::NotPermitted)
    );
    assert_eq!(
        scope.open(&cache, "in", 0, "b"),
        Err(OpenRefusal::NotPermitted)
    );
    // Not a well-formed name at all, so no message carries it.
    assert_eq!(
        scope.open(&cache, "in", 0, "../a"),
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
        scope.open(&cache(), "in", 0, "b1a"),
        Err(OpenRefusal::NotPermitted)
    );
}

#[test]
fn an_empty_scope_names_nothing() {
    let mut scope = BlobScope::empty();
    assert_eq!(
        scope.open(&cache(), "in", 0, "a"),
        Err(OpenRefusal::NotPermitted)
    );
}

#[test]
fn a_message_the_server_says_left_its_window_is_not_found() {
    let mut scope = scope();
    let cache = FakeCache::default().gone(WIRE, 2, "a");
    assert_eq!(scope.open(&cache, "in", 1, "a"), Err(OpenRefusal::NotFound));
    // The other attachment of the same message is the cache's own question.
    assert!(scope.open(&cache, "in", 1, "b").is_ok());
}

#[test]
fn bytes_not_on_the_page_open_and_read_unavailable() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope
        .open(&cache, "in", 1, "a")
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
        .open(&FakeCache::default(), "in", 1, "a")
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
    let handle = scope.open(&cache, "in", 0, "a").expect("opens");
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
    let handle = scope.open(&cache, "in", 1, "b").expect("opens");
    assert_eq!(scope.size(handle), Ok(0));
    assert_eq!(scope.read(&cache, handle, 0, 0), Ok(Ok(Vec::new())));
}

#[test]
fn the_sixty_fifth_handle_is_quota_exceeded_until_one_is_dropped() {
    let mut scope = scope();
    let cache = cache();
    let handles: Vec<u32> = (0..SURFACE_MAX_HANDLES)
        .map(|_| scope.open(&cache, "in", 0, "a").expect("within the quota"))
        .collect();
    assert_eq!(
        scope.open(&cache, "in", 0, "a"),
        Err(OpenRefusal::QuotaExceeded)
    );
    scope.drop_handle(handles[0]).expect("held");
    assert!(scope.open(&cache, "in", 0, "a").is_ok());
}

#[test]
fn a_dropped_or_never_minted_handle_is_a_fault() {
    let mut scope = scope();
    let cache = cache();
    let handle = scope.open(&cache, "in", 0, "a").expect("opens");
    scope.drop_handle(handle).expect("held");
    assert_eq!(scope.size(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.content_type(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.read(&cache, handle, 0, 1), Err(UnknownHandle(handle)));
    assert_eq!(scope.drop_handle(handle), Err(UnknownHandle(handle)));
    assert_eq!(scope.size(999), Err(UnknownHandle(999)));
    // Numbers are not reused, so a stale one never names a newer attachment.
    let next = scope.open(&cache, "in", 0, "a").expect("opens");
    assert_ne!(next, handle);
}

#[test]
fn another_activations_handle_is_a_fault() {
    let cache = cache();
    let mut first = scope();
    let second = scope();
    let theirs = first.open(&cache, "in", 0, "a").expect("opens");
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
    let handle = scope.open(&cache, "other", 0, "a").expect("opens");
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
    );
    let cache = cache().bytes(OTHER, 7, "z", b"\x01");
    let handle = scope.open(&cache, "in", 0, "a").expect("opens");
    assert_eq!(
        scope.read(&cache, handle, 0, 4).map(|r| r.map(|b| b.len())),
        Ok(Ok(4))
    );
    let handle = scope.open(&cache, "other", 0, "z").expect("opens");
    assert_eq!(scope.read(&cache, handle, 0, 1), Ok(Ok(vec![0x01])));
}

#[test]
fn an_ungranted_instances_scope_is_empty() {
    let bindings = bindings(&[ComponentGrant::Ports]);
    let mut scope = BlobScope::of(
        &bindings,
        "p1",
        &activation(vec![("in", vec![env(1, WIRE, &[("a", 4)])])]),
    );
    assert_eq!(
        scope.open(&cache(), "in", 0, "a"),
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
    let mut scope = BlobScope::of(&bindings, "p1", &sync);
    let cache = cache();
    assert!(scope.open(&cache, "in", 0, "a").is_ok());
    assert_eq!(
        scope.open(&cache, "call", 0, "b"),
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
    );
}
