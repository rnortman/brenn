//! The page's blob table: what a commit records, what the sweep keeps, and what
//! `open` on a page-local port yields for each kind of blob.

use std::collections::BTreeMap;

use brenn_envelope::{AttachmentName, Attachments, ChannelScheme, MessageEnvelope, Urgency};
use uuid::Uuid;

use crate::blob_scope::{BlobScope, CacheView, OpenRefusal, Remote, largest_made_blob};
use crate::fetch::Cached;
use crate::page_bytes::MadeLedger;
use crate::registry::new_stores;

use super::*;

const LOCAL: &str = "local:app/notes";
const WIRE: &str = "brenn:site.bar.in";

/// A cache holding the bytes of every backend reference it is asked about.
struct Holds(Vec<u8>);

impl CacheView for Holds {
    fn cached(&self, _channel: &str, _message_id: Uuid, _name: &AttachmentName) -> Cached<'_> {
        Cached::Bytes(&self.0)
    }
}

/// A cache holding nothing.
struct Empty;

impl CacheView for Empty {
    fn cached(&self, _channel: &str, _message_id: Uuid, _name: &AttachmentName) -> Cached<'_> {
        Cached::Absent
    }
}

fn name(name: &str) -> AttachmentName {
    AttachmentName::new(name).expect("a fixture attachment name")
}

/// Page bytes `bytes`, made through a scope as a component makes them.
fn made(ledger: &MadeLedger, bytes: &[u8]) -> Blob {
    let mut scope = BlobScope::empty();
    let writer = scope.create(ledger, "application/octet-stream").unwrap();
    scope
        .write(writer, 0, bytes, largest_made_blob(None))
        .unwrap()
        .unwrap();
    let blob = scope.seal(writer).unwrap().unwrap();
    scope.blob(blob).unwrap().clone()
}

fn remote(id: &str, size: u64) -> Blob {
    Blob::Remote(Remote {
        channel: WIRE.to_string(),
        message_id: Uuid::from_u128(9),
        name: name("photo"),
        blob: BlobRef {
            id: BlobId::new(id.to_string()),
            size,
            content_type: "image/png".to_string(),
        },
    })
}

fn envelope(id: u128, attachments: &[(&str, &BlobRef)]) -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::from_u128(id),
        source: "test".into(),
        channel: LOCAL.into(),
        sender: "app:page#p1".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: format!("m{id}").into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Local,
        transport: None,
        attachments: Attachments::from_map(
            attachments
                .iter()
                .map(|&(n, blob)| (name(n), blob.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

/// Stores holding the page-local channel at `depth`.
fn stores(depth: u64) -> SurfaceStores {
    let mut stores = new_stores(Uuid::from_u128(0xB10B));
    stores.ensure(LOCAL, depth);
    stores
}

#[test]
fn page_bytes_are_shared_under_fresh_page_ids() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"\x00\xff\x01");
    let mut table = PageBlobs::new();
    let first = table.commit(&blob);
    let second = table.commit(&blob);
    assert_eq!(first.id.as_str(), "page-1");
    assert_eq!(second.id.as_str(), "page-2");
    assert_eq!(
        (first.size, first.content_type.as_str()),
        (3, "application/octet-stream")
    );
    assert_eq!(table.get(&first.id), Some(&blob));
    assert_eq!(ledger.held(), 3, "shared, not copied");
}

#[test]
fn a_backend_reference_is_recorded_under_the_backends_id() {
    let mut table = PageBlobs::new();
    let older = remote("0123abcd", 5);
    let reference = table.commit(&older);
    assert_eq!(reference.id.as_str(), "0123abcd");
    let newer = Blob::Remote(Remote {
        message_id: Uuid::from_u128(10),
        ..match remote("0123abcd", 5) {
            Blob::Remote(r) => r,
            Blob::Bytes(_) => unreachable!(),
        }
    });
    table.commit(&newer);
    assert_eq!(table.len(), 1);
    assert_eq!(
        table.get(&reference.id),
        Some(&newer),
        "the newer origin replaces the older"
    );
}

#[test]
fn the_sweep_keeps_what_a_retained_or_parked_message_names_and_frees_the_rest() {
    let ledger = MadeLedger::new();
    let mut table = PageBlobs::new();
    let kept = table.commit(&made(&ledger, b"kept"));
    let parked = table.commit(&made(&ledger, b"parked"));
    let orphan = table.commit(&made(&ledger, b"orphan"));
    let mut stores = stores(4);
    let store = stores.get_mut(LOCAL).unwrap();
    store.append_minted(envelope(1, &[("a", &kept)]));
    store
        .park("app:page#p1", envelope(2, &[("a", &parked)]), u64::MAX)
        .expect("room to park");
    table.sweep(&stores);
    assert!(table.get(&kept.id).is_some());
    assert!(table.get(&parked.id).is_some());
    assert!(table.get(&orphan.id).is_none());
    assert_eq!(ledger.held(), 4 + 6, "the orphan's bytes are freed");
}

#[test]
fn the_sweep_frees_page_bytes_once_the_last_naming_message_is_evicted() {
    let ledger = MadeLedger::new();
    let mut table = PageBlobs::new();
    let reference = table.commit(&made(&ledger, b"bytes"));
    let mut stores = stores(1);
    stores
        .get_mut(LOCAL)
        .unwrap()
        .append_minted(envelope(1, &[("a", &reference)]));
    table.sweep(&stores);
    assert_eq!(ledger.held(), 5);
    stores
        .get_mut(LOCAL)
        .unwrap()
        .append_minted(envelope(2, &[]));
    table.sweep(&stores);
    assert!(table.is_empty());
    assert_eq!(ledger.held(), 0);
}

#[test]
fn a_wire_store_naming_an_id_does_not_keep_it() {
    let mut table = PageBlobs::new();
    let reference = table.commit(&remote("0123abcd", 5));
    let mut stores = stores(4);
    stores.ensure(WIRE, 4);
    stores.get_mut(WIRE).unwrap().insert(MessageEnvelope {
        channel: WIRE.into(),
        envelope_type: ChannelScheme::Brenn,
        ..envelope(1, &[("a", &reference)])
    });
    table.sweep(&stores);
    assert!(table.is_empty());
}

/// A scope windowing one page-local message that names `reference` as `a`.
fn local_scope(reference: &BlobRef) -> BlobScope {
    let mut scope = BlobScope::empty();
    scope.add_window("notes", LOCAL.into(), &[envelope(1, &[("a", reference)])]);
    scope
}

#[test]
fn opening_page_bytes_on_a_page_local_port_shares_them() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"\x00\x01\x02\x03");
    let mut table = PageBlobs::new();
    let reference = table.commit(&blob);
    let mut scope = local_scope(&reference);
    let handle = scope.open(&Empty, &table, "notes", 0, "a").expect("opens");
    assert_eq!(scope.blob(handle), Ok(&blob));
    assert_eq!(scope.read(&Empty, handle, 1, 2), Ok(Ok(vec![1, 2])));
    assert_eq!(ledger.held(), 4);
}

#[test]
fn a_backend_reference_on_a_page_local_port_reads_while_the_page_holds_its_bytes() {
    let mut table = PageBlobs::new();
    let reference = table.commit(&remote("0123abcd", 3));
    let mut scope = local_scope(&reference);
    let handle = scope.open(&Empty, &table, "notes", 0, "a").expect("opens");
    assert_eq!(scope.blob(handle), Ok(&remote("0123abcd", 3)));
    assert_eq!(
        scope.read(&Holds(b"abc".to_vec()), handle, 0, 3),
        Ok(Ok(b"abc".to_vec()))
    );
    assert_eq!(
        scope.read(&Empty, handle, 0, 3),
        Ok(Err(crate::blob_scope::ReadRefusal::Unavailable))
    );
}

#[test]
fn an_id_the_table_no_longer_holds_is_not_found() {
    let reference = PageBlobs::new().commit(&remote("0123abcd", 3));
    let mut scope = local_scope(&reference);
    assert_eq!(
        scope.open(&Empty, &PageBlobs::new(), "notes", 0, "a"),
        Err(OpenRefusal::NotFound)
    );
}

#[test]
fn a_handle_keeps_page_bytes_readable_after_the_table_lets_go() {
    let ledger = MadeLedger::new();
    let mut table = PageBlobs::new();
    let reference = table.commit(&made(&ledger, b"held"));
    let mut scope = local_scope(&reference);
    let handle = scope.open(&Empty, &table, "notes", 0, "a").expect("opens");
    table.sweep(&stores(4));
    assert!(table.is_empty());
    assert_eq!(scope.read(&Empty, handle, 0, 4), Ok(Ok(b"held".to_vec())));
    drop(scope);
    assert_eq!(ledger.held(), 0);
}
