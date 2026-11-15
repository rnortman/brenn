//! The fetcher driven against a real page — real stores and positions, a real
//! blob plane — with the server's answers scripted frame by frame, so each
//! assertion reads what the store holds, what the cache answers, or what goes on
//! the wire.

use std::collections::BTreeMap;

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::subs::SubscriptionDepths;
use brenn_attach_proto::{BlobFacts, BlobOpenOutcome, BlobReadOutcome, ServerFrame};
use brenn_envelope::{BlobId, BlobRef, ChannelScheme, Urgency};
use brenn_surface_schema::WireChannel;
use brenn_surface_schema::bindings::BindingsDocument;

use crate::blob_plane::PlaneAnswer;
use crate::registry::BindingKey;
use crate::session::Effect;
use crate::test_support::bindings as fixtures;
use crate::test_support::pages;
use crate::turn::{Input, on_input};

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "brenn:site.bar.in";
const EPOCH: Uuid = Uuid::from_u128(0xfe7c);
const NOW: Millis = Millis(10_000);
const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

fn door() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 64 * MIB,
        max_chunk_bytes: CHUNK_BYTES,
        max_handles: 64,
        max_bytes_held: 256 * MIB,
        max_inflight_reads: 4,
        upload: false,
    }
}

fn facts() -> AttachmentFacts {
    AttachmentFacts {
        blobs: Some(door()),
        ..pages::facts()
    }
}

/// `p1` reads `WIRE` at depth 1 — a frame stream's binding — and the channel
/// fetches attachments up to `bound`.
fn doc(bound: u64) -> BindingsDocument {
    let mut doc = fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![fixtures::subscription("p1", "in", WIRE, 1, 1)],
        Vec::new(),
        Vec::new(),
    );
    doc.wire_channels = vec![WireChannel {
        fetch_bytes: bound,
        ..fixtures::wire(WIRE)
    }];
    doc
}

fn page(bound: u64) -> SurfacePage {
    pages::configured_page(CONFIG, EPOCH, facts(), &["p1"], &doc(bound), NOW)
}

fn name(name: &str) -> AttachmentName {
    AttachmentName::new(name).expect("a fixture attachment name")
}

/// Message `id` on `WIRE`, naming one attachment per `(name, size)`.
fn env(id: u128, attachments: &[(&str, u64)]) -> MessageEnvelope {
    MessageEnvelope {
        message_id: Uuid::from_u128(id),
        source: "test".into(),
        channel: WIRE.into(),
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
        attachments: brenn_envelope::Attachments::from_map(
            attachments
                .iter()
                .map(|&(n, size)| {
                    (
                        name(n),
                        BlobRef {
                            id: BlobId::new(format!("b{id}{n}")),
                            size,
                            content_type: "image/jpeg".into(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ),
    }
}

fn deliver(page: &mut SurfacePage, envs: Vec<MessageEnvelope>, now: Millis) -> Fetched {
    on_rows(page, WIRE, envs, now)
}

/// The ids the store retains, oldest first.
fn stored(page: &SurfacePage) -> Vec<u128> {
    page.stores
        .get(WIRE)
        .expect("the wire store")
        .retained()
        .map(|(envelope, _)| envelope.message_id.as_u128())
        .collect()
}

/// Every `BlobOpen` among `frames`: its handle and message id.
fn opens(frames: &[ClientFrame]) -> Vec<(BlobHandle, u128)> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            ClientFrame::BlobOpen {
                handle, message_id, ..
            } => Some((*handle, message_id.as_u128())),
            _ => None,
        })
        .collect()
}

/// Every `BlobRead` among `frames`: its correlation, offset and length.
fn reads(frames: &[ClientFrame]) -> Vec<(u64, u64, u64)> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            ClientFrame::BlobRead {
                correlation,
                offset,
                len,
                ..
            } => Some((*correlation, *offset, *len)),
            _ => None,
        })
        .collect()
}

fn opened(handle: BlobHandle, size: u64) -> ServerFrame {
    ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::Ok {
            size,
            content_type: "image/jpeg".into(),
        },
    }
}

/// Settle one blob answer on the page's plane and hand the fetcher its share,
/// as the inbound pass does.
fn on_blob_frame(
    page: &mut SurfacePage,
    frame: ServerFrame,
    now: Millis,
) -> Result<Fetched, String> {
    let PlaneAnswer {
        frames,
        fetch,
        upload,
    } = page.plane.on_frame(frame)?;
    assert!(upload.is_empty(), "the fetch suite uploads nothing");
    on_blob_events(page, frames, fetch, now)
}

fn answer(page: &mut SurfacePage, frame: ServerFrame, now: Millis) -> Fetched {
    on_blob_frame(page, frame, now).expect("an answer the page is owed")
}

/// The byte at `offset` of every fixture blob.
fn pattern(offset: u64) -> u8 {
    (offset % 251) as u8
}

/// Answer `handle`'s open with `size` and every read that follows with the
/// pattern, until the fetch closes. Answers the frames of the last answer.
fn fetch_whole(page: &mut SurfacePage, handle: BlobHandle, size: u64, now: Millis) -> Fetched {
    let mut last = answer(page, opened(handle, size), now);
    let mut pending = reads(&last.frames);
    while let Some((correlation, offset, len)) = pending.first().copied() {
        pending.remove(0);
        let bytes = (offset..offset + len).map(pattern).collect();
        last = answer(
            page,
            ServerFrame::BlobReadResult {
                correlation,
                outcome: BlobReadOutcome::Ok { bytes },
            },
            now,
        );
        pending.extend(reads(&last.frames));
    }
    last
}

fn cached_len(page: &SurfacePage, id: u128, n: &str) -> Option<usize> {
    match page.fetch.cached(WIRE, Uuid::from_u128(id), &name(n)) {
        Cached::Bytes(bytes) => Some(bytes.len()),
        Cached::Gone | Cached::Absent => None,
    }
}

#[test]
fn the_deadline_counts_chunks_per_attachment_under_a_ceiling() {
    assert_eq!(hold_deadline_ms([0]), 3_000);
    assert_eq!(hold_deadline_ms([CHUNK_BYTES]), 3_000);
    assert_eq!(hold_deadline_ms([MIB]), 6_000);
    assert_eq!(hold_deadline_ms([7 * MIB]), 30_000);
    assert_eq!(hold_deadline_ms([64 * MIB]), HOLD_MAX_MS);
    // Thirty-two one-byte attachments come to 34 s before the ceiling.
    assert_eq!(hold_deadline_ms([1; 32]), HOLD_MAX_MS);
    assert_eq!(hold_deadline_ms([1; 3]), 5_000);
}

#[test]
fn a_row_naming_nothing_fetched_on_an_empty_hold_is_stored_in_the_pass() {
    let mut page = page(MIB);
    let fetched = deliver(
        &mut page,
        vec![env(1, &[]), env(2, &[("big", 2 * MIB)])],
        NOW,
    );
    assert_eq!(stored(&page), vec![2], "depth 1 keeps the newest");
    assert!(
        fetched.frames.is_empty(),
        "nothing over the bound is fetched"
    );
    assert_eq!(cached_len(&page, 2, "big"), None);
}

#[test]
fn a_fetched_row_enters_the_store_with_its_bytes_and_the_handle_is_closed() {
    let mut page = page(MIB);
    let size = 600 * KIB;
    let fetched = deliver(&mut page, vec![env(1, &[("frame", size)])], NOW);
    assert!(stored(&page).is_empty(), "held until its bytes arrive");
    let [(handle, 1)] = opens(&fetched.frames)[..] else {
        panic!("one open: {:?}", fetched.frames)
    };
    let last = fetch_whole(&mut page, handle, size, NOW);
    assert_eq!(stored(&page), vec![1]);
    assert!(last.frames.contains(&ClientFrame::BlobClose { handle }));
    let Cached::Bytes(bytes) = page.fetch.cached(WIRE, Uuid::from_u128(1), &name("frame")) else {
        panic!("the bytes are cached")
    };
    assert!(
        bytes
            .iter()
            .enumerate()
            .all(|(i, b)| *b == pattern(i as u64))
    );
    assert_eq!(page.fetch.cache_bytes(), size);
}

#[test]
fn release_is_in_arrival_order_and_a_plain_row_waits_behind_a_fetching_one() {
    let mut page = page(MIB);
    let fetched = deliver(&mut page, vec![env(1, &[("a", 10)]), env(2, &[])], NOW);
    assert!(stored(&page).is_empty(), "the plain row waits its turn");
    let [(handle, 1)] = opens(&fetched.frames)[..] else {
        panic!("one open")
    };
    fetch_whole(&mut page, handle, 10, NOW);
    let window = page
        .stores
        .get_mut(WIRE)
        .expect("store")
        .serve(
            &BindingKey::new("p1", "in"),
            SubscriptionDepths {
                push_depth: 1,
                retain_depth: 1,
            },
        )
        .expect("p1 holds a position");
    assert_eq!(
        window
            .envelopes
            .iter()
            .map(|e| e.message_id.as_u128())
            .collect::<Vec<_>>(),
        vec![2],
        "both released in one turn; the newest is what a depth-1 binding is handed"
    );
}

#[test]
fn four_rows_fetch_at_once_and_overflow_evicts_the_oldest_waiting_row() {
    let mut page = page(MIB);
    // Depth 1: the hold has room for five rows.
    let rows = (1..=7).map(|id| env(id, &[("f", 10)])).collect();
    let fetched = deliver(&mut page, rows, NOW);
    let opened_ids: Vec<u128> = opens(&fetched.frames).iter().map(|(_, id)| *id).collect();
    assert_eq!(opened_ids, vec![1, 2, 3, 4], "four fetch; the rest wait");
    // Rows 5 and 6 were evicted in turn by 6 and 7; the four fetching ones
    // never are.
    for (handle, _) in opens(&fetched.frames) {
        fetch_whole(&mut page, handle, 10, NOW);
    }
    let later = page.fetch.wakeup_change();
    assert!(matches!(later, Some(Some(_))), "row 7 is fetching now");
    let store = page.stores.get_mut(WIRE).expect("store");
    let window = store
        .serve(
            &BindingKey::new("p1", "in"),
            SubscriptionDepths {
                push_depth: 1,
                retain_depth: 1,
            },
        )
        .expect("p1 holds a position");
    assert_eq!(window.envelopes[0].message_id.as_u128(), 4);
    // Two evicted upstream of the store, three rolled out of a depth-1 ring.
    assert_eq!(window.dropped, 5);
}

#[test]
fn a_full_hold_with_nothing_waiting_releases_its_head_as_it_is() {
    let mut page = page(MIB);
    let rows: Vec<_> = (1..=4).map(|id| env(id, &[("f", 10)])).collect();
    deliver(&mut page, rows, NOW);
    // Four fetching plus one settled behind them fill the hold of five.
    deliver(&mut page, vec![env(5, &[])], NOW);
    assert!(stored(&page).is_empty());
    deliver(&mut page, vec![env(6, &[])], NOW);
    assert_eq!(stored(&page), vec![1], "the head goes in without its bytes");
    assert_eq!(cached_len(&page, 1, "f"), None);
}

#[test]
fn a_deadline_releases_the_row_and_a_late_fetch_still_lands() {
    let mut page = page(MIB);
    let fetched = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    let [(handle, _)] = opens(&fetched.frames)[..] else {
        panic!("one open")
    };
    let deadline = NOW.saturating_add_ms(3_000);
    assert_eq!(page.fetch.wakeup_change(), Some(Some(deadline)));
    assert!(on_due(&mut page, Millis(deadline.0 - 1)).frames.is_empty());
    assert!(stored(&page).is_empty());
    on_due(&mut page, deadline);
    assert_eq!(stored(&page), vec![1], "released without its bytes");
    assert_eq!(page.fetch.wakeup_change(), Some(None));
    assert_eq!(cached_len(&page, 1, "f"), None);
    fetch_whole(&mut page, handle, 10, deadline);
    assert_eq!(
        cached_len(&page, 1, "f"),
        Some(10),
        "readable by a later activation"
    );
}

#[test]
fn a_hold_the_peer_never_answers_empties_within_the_aggregate_bound() {
    let names: Vec<String> = (0..32).map(|i| format!("a{i}")).collect();
    let many: Vec<(&str, u64)> = names.iter().map(|n| (n.as_str(), 1)).collect();
    // H = depth 1 + 4 = 5 rows, each naming 32 one-byte attachments on a
    // 256 KiB channel, or with the last naming none.
    for plain_tail in [false, true] {
        let mut page = page(256 * KIB);
        let mut rows: Vec<_> = (1..=4).map(|id| env(id, &many)).collect();
        rows.push(if plain_tail {
            env(5, &[])
        } else {
            env(5, &many)
        });
        deliver(&mut page, rows, NOW);
        let first = NOW.saturating_add_ms(HOLD_MAX_MS);
        on_due(&mut page, Millis(first.0 - 1));
        assert!(
            stored(&page).is_empty(),
            "no row before its batch's ceiling"
        );
        on_due(&mut page, first);
        if plain_tail {
            assert_eq!(
                stored(&page),
                vec![5],
                "the plain row last, behind its batch"
            );
            continue;
        }
        assert_eq!(stored(&page), vec![4], "rows 1-4 at the first ceiling");
        let bound = NOW.saturating_add_ms(2 * HOLD_MAX_MS);
        on_due(&mut page, Millis(bound.0 - 1));
        assert_eq!(stored(&page), vec![4], "row 5 started at the first ceiling");
        on_due(&mut page, bound);
        assert_eq!(stored(&page), vec![5], "the last row at ⌈H / 4⌉ · 30 s");
    }
}

#[test]
fn not_retained_is_gone_and_a_failed_read_is_unfetched_and_reported() {
    let mut page = page(MIB);
    let fetched = deliver(&mut page, vec![env(1, &[("a", 10), ("b", 10)])], NOW);
    let [(a, _), (b, _)] = opens(&fetched.frames)[..] else {
        panic!("two opens")
    };
    answer(
        &mut page,
        ServerFrame::BlobOpenResult {
            handle: a,
            outcome: BlobOpenOutcome::NotRetained,
        },
        NOW,
    );
    let read = answer(&mut page, opened(b, 10), NOW);
    let [(correlation, 0, 10)] = reads(&read.frames)[..] else {
        panic!("one read")
    };
    let failed = answer(
        &mut page,
        ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::StorageFailed,
        },
        NOW,
    );
    assert!(
        failed
            .frames
            .contains(&ClientFrame::BlobClose { handle: b })
    );
    assert_eq!(failed.reports.len(), 1);
    assert_eq!(failed.reports[0].level, LogLevel::Warn);
    assert_eq!(stored(&page), vec![1], "both settled");
    let id = Uuid::from_u128(1);
    assert_eq!(page.fetch.cached(WIRE, id, &name("a")), Cached::Gone);
    assert_eq!(page.fetch.cached(WIRE, id, &name("b")), Cached::Absent);
}

#[test]
fn a_tool_channel_refusal_is_reported_once_per_channel() {
    let mut page = page(MIB);
    let fetched = deliver(
        &mut page,
        vec![env(1, &[("a", 10)]), env(2, &[("a", 10)])],
        NOW,
    );
    let mut reports = 0;
    for (handle, _) in opens(&fetched.frames) {
        let refused = ServerFrame::BlobOpenResult {
            handle,
            outcome: BlobOpenOutcome::ToolChannel,
        };
        reports += answer(&mut page, refused, NOW).reports.len();
    }
    assert_eq!(reports, 1);
    assert_eq!(stored(&page), vec![2]);
}

/// A page with two rows fetching, `1` and `2`, each naming one 10-byte
/// attachment; answers their open handles.
fn two_fetching() -> (SurfacePage, BlobHandle, BlobHandle) {
    let mut page = page(MIB);
    let fetched = deliver(
        &mut page,
        vec![env(1, &[("a", 10)]), env(2, &[("a", 10)])],
        NOW,
    );
    let [(a, _), (b, _)] = opens(&fetched.frames)[..] else {
        panic!("two opens")
    };
    (page, a, b)
}

fn fatal(page: &mut SurfacePage, frame: ServerFrame) -> String {
    on_blob_frame(page, frame, NOW).expect_err("an answer the page cannot reconcile")
}

#[test]
fn answers_the_peer_cannot_send_are_fatal() {
    let (mut page, a, _) = two_fetching();
    let denied = fatal(
        &mut page,
        ServerFrame::BlobOpenResult {
            handle: a,
            outcome: BlobOpenOutcome::NoSuchAttachment,
        },
    );
    assert!(denied.contains("NoSuchAttachment"), "{denied}");
    assert!(denied.contains("delivery named"), "{denied}");

    let (mut page, a, _) = two_fetching();
    let size = fatal(&mut page, opened(a, 11));
    assert!(size.contains("answered 11 bytes"), "{size}");
    assert!(size.contains("named 10 bytes"), "{size}");

    let (mut page, a, _) = two_fetching();
    let content_type = fatal(
        &mut page,
        ServerFrame::BlobOpenResult {
            handle: a,
            outcome: BlobOpenOutcome::Ok {
                size: 10,
                content_type: "text/plain".into(),
            },
        },
    );
    assert!(content_type.contains("\"text/plain\""), "{content_type}");
    assert!(content_type.contains("\"image/jpeg\""), "{content_type}");

    let (mut page, _, _) = two_fetching();
    let unowed = fatal(
        &mut page,
        ServerFrame::BlobReadResult {
            correlation: 99,
            outcome: BlobReadOutcome::Ok { bytes: vec![] },
        },
    );
    assert!(unowed.contains("is not owed"), "{unowed}");
}

#[test]
fn a_message_leaving_the_store_deletes_its_entry_and_closes_its_fetch() {
    let mut page = page(MIB);
    let fetched = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    let [(handle, _)] = opens(&fetched.frames)[..] else {
        panic!("one open")
    };
    on_due(&mut page, NOW.saturating_add_ms(3_000));
    assert_eq!(
        stored(&page),
        vec![1],
        "released by its deadline, still fetching"
    );
    let next = deliver(&mut page, vec![env(2, &[])], NOW.saturating_add_ms(3_000));
    assert_eq!(stored(&page), vec![2]);
    assert!(next.frames.contains(&ClientFrame::BlobClose { handle }));
    // The late open answer finds nothing to fill.
    let late = answer(&mut page, opened(handle, 10), NOW);
    assert!(late.frames.is_empty());
    assert_eq!(page.fetch.cache_bytes(), 0);
}

#[test]
fn room_is_made_from_the_oldest_stored_message_and_never_passes_the_page_cap() {
    let mut page = page(64 * MIB);
    let size = 40 * MIB;
    let first = deliver(&mut page, vec![env(1, &[("f", size)])], NOW);
    fetch_whole(&mut page, opens(&first.frames)[0].0, size, NOW);
    assert_eq!(page.fetch.cache_bytes(), size);
    let second = deliver(&mut page, vec![env(2, &[("f", size)])], NOW);
    assert_eq!(second.reports.len(), 1, "the drop for room is reported");
    assert_eq!(
        cached_len(&page, 1, "f"),
        None,
        "the stored message's bytes went"
    );
    fetch_whole(&mut page, opens(&second.frames)[0].0, size, NOW);
    assert_eq!(cached_len(&page, 2, "f"), Some(size as usize));
    assert!(page.fetch.cache_bytes() <= PAGE_BLOB_BYTES);
    // A stream past the cap drops for room on every frame; the channel's
    // drops were already reported on this attachment.
    let third = deliver(&mut page, vec![env(3, &[("f", size)])], NOW);
    assert_eq!(cached_len(&page, 2, "f"), None, "dropped for room again");
    assert!(third.reports.is_empty(), "{:?}", third.reports);
}

/// Deliver message `id` naming one 10-byte attachment, open it and answer its
/// read `outcome`; answers how many reports that answer produced.
fn refuse_read(page: &mut SurfacePage, id: u128, outcome: BlobReadOutcome) -> usize {
    let fetched = deliver(page, vec![env(id, &[("f", 10)])], NOW);
    let [(handle, _)] = opens(&fetched.frames)[..] else {
        panic!("one open: {:?}", fetched.frames)
    };
    let read = answer(page, opened(handle, 10), NOW);
    let [(correlation, _, _)] = reads(&read.frames)[..] else {
        panic!("one read")
    };
    answer(
        page,
        ServerFrame::BlobReadResult {
            correlation,
            outcome,
        },
        NOW,
    )
    .reports
    .len()
}

#[test]
fn a_fetch_trouble_is_reported_once_per_channel_and_reason_until_the_page_reattaches() {
    let mut page = page(MIB);
    assert_eq!(refuse_read(&mut page, 1, BlobReadOutcome::StorageFailed), 1);
    assert_eq!(
        refuse_read(&mut page, 2, BlobReadOutcome::StorageFailed),
        0,
        "the same reason on the same channel"
    );
    assert_eq!(
        refuse_read(&mut page, 3, BlobReadOutcome::TooManyReads),
        1,
        "another reason"
    );
    page.on_detached();
    page.on_attached(facts());
    page.subs
        .on_subscribe_result(CONFIG, brenn_attach_proto::SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    page.apply_config(&doc(MIB).to_body(), NOW)
        .expect("the document applies");
    on_config(&mut page, NOW).expect("the attachment has a door");
    assert_eq!(
        refuse_read(&mut page, 4, BlobReadOutcome::StorageFailed),
        1,
        "a new attachment reports again"
    );
}

#[test]
fn a_held_rows_bytes_are_never_dropped_for_room() {
    let mut page = page(64 * MIB);
    let size = 40 * MIB;
    let fetched = deliver(
        &mut page,
        vec![env(1, &[("f", size)]), env(2, &[("f", size)])],
        NOW,
    );
    // Row 1's reservation leaves no room for row 2's, and nothing stored can
    // give it up.
    assert_eq!(opens(&fetched.frames).len(), 1);
    assert_eq!(stored(&page), Vec::<u128>::new());
    fetch_whole(&mut page, opens(&fetched.frames)[0].0, size, NOW);
    assert_eq!(
        stored(&page),
        vec![2],
        "row 2 settled unfetched behind row 1"
    );
    assert_eq!(cached_len(&page, 2, "f"), None);
}

#[test]
fn a_detach_returns_fetches_to_wanted_and_the_next_attachment_fetches_them_whole() {
    let mut page = page(MIB);
    let size = 600 * KIB;
    let fetched = deliver(&mut page, vec![env(1, &[("f", size)])], NOW);
    let [(handle, _)] = opens(&fetched.frames)[..] else {
        panic!("one open")
    };
    let read = answer(&mut page, opened(handle, size), NOW);
    let (correlation, offset, len) = reads(&read.frames)[0];
    answer(
        &mut page,
        ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::Ok {
                bytes: (offset..offset + len).map(pattern).collect(),
            },
        },
        NOW,
    );
    page.on_detached();
    assert!(stored(&page).is_empty(), "the row stays held");
    assert_eq!(page.fetch.cache_bytes(), 0, "partial bytes are gone");
    page.on_attached(facts());
    page.subs
        .on_subscribe_result(CONFIG, brenn_attach_proto::SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    page.apply_config(&doc(MIB).to_body(), NOW)
        .expect("the document applies");
    let refetch = on_config(&mut page, NOW).expect("the attachment has a door");
    let [(again, 1)] = opens(&refetch.frames)[..] else {
        panic!("fetched again: {:?}", refetch.frames)
    };
    fetch_whole(&mut page, again, size, NOW);
    assert_eq!(stored(&page), vec![1]);
    assert_eq!(cached_len(&page, 1, "f"), Some(size as usize));
}

#[test]
fn a_gated_channel_on_an_attachment_without_a_door_is_fatal() {
    let mut page = pages::configured_page(CONFIG, EPOCH, pages::facts(), &["p1"], &doc(MIB), NOW);
    let err = on_config(&mut page, NOW).expect_err("a gated channel with no door");
    assert!(err.contains("opened no blob door"), "{err}");
    let mut ungated = pages::configured_page(CONFIG, EPOCH, pages::facts(), &["p1"], &doc(0), NOW);
    assert!(on_config(&mut ungated, NOW).is_ok());
}

#[test]
fn the_fetch_deadline_is_armed_by_the_turn_and_disarmed_when_it_fires() {
    let mut page = page(MIB);
    deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    let deadline = NOW.saturating_add_ms(3_000);
    let armed = on_input(&mut page, Input::FetchDue, NOW, 0);
    assert!(armed.contains(&Effect::SetFetchWakeup(Some(deadline))));
    let fired = on_input(&mut page, Input::FetchDue, deadline, 0);
    assert!(fired.contains(&Effect::SetFetchWakeup(None)));
    assert_eq!(stored(&page), vec![1]);
}

/// Release pops only a hold's head, so the timer is the head's deadline: a
/// shorter one behind it that passes first wakes nothing.
#[test]
fn the_wakeup_is_the_heads_deadline_and_a_shorter_one_behind_it_arms_nothing() {
    let mut page = page(8 * MIB);
    deliver(
        &mut page,
        vec![env(1, &[("f", 7 * CHUNK_BYTES)]), env(2, &[("f", 10)])],
        NOW,
    );
    let head = NOW.saturating_add_ms(9_000);
    assert_eq!(page.fetch.wakeup_change(), Some(Some(head)));
    on_due(&mut page, NOW.saturating_add_ms(3_000));
    assert!(stored(&page).is_empty(), "row 2 is behind the head");
    assert_eq!(
        page.fetch.wakeup_change(),
        None,
        "still the head's deadline"
    );
    on_due(&mut page, head);
    assert_eq!(stored(&page), vec![2], "both out at the head's deadline");
    assert_eq!(page.fetch.wakeup_change(), Some(None));
}

/// A page under a door whose handles and bytes held are `max_handles` and
/// `max_bytes_held`, fetching up to `bound`.
fn page_under(max_handles: u32, max_bytes_held: u64, bound: u64) -> SurfacePage {
    let facts = AttachmentFacts {
        blobs: Some(BlobFacts {
            max_handles,
            max_bytes_held,
            ..door()
        }),
        ..pages::facts()
    };
    pages::configured_page(CONFIG, EPOCH, facts, &["p1"], &doc(bound), NOW)
}

#[test]
fn an_attachment_over_the_bytes_held_is_never_opened_reported_once_and_its_row_released() {
    let mut page = page_under(64, 100, MIB);
    let first = deliver(&mut page, vec![env(1, &[("f", 200)])], NOW);
    assert!(opens(&first.frames).is_empty());
    assert_eq!(first.reports.len(), 1);
    assert_eq!(first.reports[0].level, LogLevel::Warn);
    assert_eq!(stored(&page), vec![1]);
    let second = deliver(&mut page, vec![env(2, &[("f", 200)])], NOW);
    assert!(opens(&second.frames).is_empty());
    assert!(second.reports.is_empty(), "{:?}", second.reports);
    assert_eq!(stored(&page), vec![2]);
}

#[test]
fn fetches_on_the_wire_stay_within_the_doors_handles_and_bytes_held() {
    // Two handles; then 25 bytes held, which two 10-byte fetches leave no
    // room in.
    for mut page in [page_under(2, 256 * MIB, MIB), page_under(64, 25, MIB)] {
        let fetched = deliver(
            &mut page,
            vec![
                env(1, &[("f", 10)]),
                env(2, &[("f", 10)]),
                env(3, &[("f", 10)]),
            ],
            NOW,
        );
        let [(first, 1), (_, 2)] = opens(&fetched.frames)[..] else {
            panic!("two opens: {:?}", fetched.frames)
        };
        let done = fetch_whole(&mut page, first, 10, NOW);
        assert!(
            done.frames
                .contains(&ClientFrame::BlobClose { handle: first })
        );
        assert_eq!(
            opens(&done.frames)
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<_>>(),
            vec![3],
            "the third opens once the first closes"
        );
    }
}

#[test]
fn a_bound_refusal_of_an_open_is_unfetched_reported_once_and_releases_its_row() {
    for outcome in [
        BlobOpenOutcome::HandleLimit,
        BlobOpenOutcome::BytesHeldLimit { size: 10 },
    ] {
        let mut page = page(MIB);
        let fetched = deliver(
            &mut page,
            vec![env(1, &[("f", 10)]), env(2, &[("f", 10)])],
            NOW,
        );
        let [(a, _), (b, _)] = opens(&fetched.frames)[..] else {
            panic!("two opens")
        };
        let refuse = |handle| ServerFrame::BlobOpenResult {
            handle,
            outcome: outcome.clone(),
        };
        let first = answer(&mut page, refuse(a), NOW);
        assert_eq!(first.reports.len(), 1, "{outcome:?}");
        assert_eq!(first.reports[0].level, LogLevel::Warn);
        assert!(opens(&first.frames).is_empty(), "not opened again");
        assert_eq!(
            page.fetch.cached(WIRE, Uuid::from_u128(1), &name("f")),
            Cached::Absent
        );
        assert_eq!(stored(&page), vec![1], "row 2 is still fetching");
        let second = answer(&mut page, refuse(b), NOW);
        assert!(second.reports.is_empty(), "{outcome:?}");
        assert_eq!(stored(&page), vec![2]);
    }
}

#[test]
fn a_document_dropping_a_gated_channel_takes_its_hold_and_closes_its_fetch() {
    let mut page = page(MIB);
    let fetched = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    let [(handle, _)] = opens(&fetched.frames)[..] else {
        panic!("one open")
    };
    let read = answer(&mut page, opened(handle, 10), NOW);
    let [(correlation, _, _)] = reads(&read.frames)[..] else {
        panic!("one read")
    };
    assert_eq!(
        page.fetch.wakeup_change(),
        Some(Some(NOW.saturating_add_ms(3_000)))
    );
    let without = fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component(fixtures::CHROME),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    page.apply_config(&without.to_body(), NOW)
        .expect("the document applies");
    let dropped = on_config(&mut page, NOW).expect("nothing is gated");
    assert!(dropped.frames.contains(&ClientFrame::BlobClose { handle }));
    assert_eq!(page.fetch.cache_bytes(), 0);
    assert_eq!(page.fetch.wakeup_change(), Some(None), "no hold is left");
    // The read the close ended is answered late, and absorbed.
    let late = answer(
        &mut page,
        ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::Ok {
                bytes: (0..10).map(pattern).collect(),
            },
        },
        NOW,
    );
    assert!(late.frames.is_empty());
    assert!(late.reports.is_empty());
}

#[test]
fn a_replayed_row_is_held_fetched_and_stored_once() {
    let mut page = page(MIB);
    let first = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    let [(handle, _)] = opens(&first.frames)[..] else {
        panic!("one open")
    };
    let held = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    assert!(opens(&held.frames).is_empty(), "replayed while held");
    fetch_whole(&mut page, handle, 10, NOW);
    assert_eq!(stored(&page), vec![1]);
    let after = deliver(&mut page, vec![env(1, &[("f", 10)])], NOW);
    assert!(opens(&after.frames).is_empty(), "replayed after release");
    assert_eq!(stored(&page), vec![1]);
    assert_eq!(cached_len(&page, 1, "f"), Some(10));
}

const ERRORS: &str = "brenn:site.surface.bar.errors";

/// The error reports a turn published, as `(source, level, message)`.
fn error_reports(effects: &[Effect]) -> Vec<(String, LogLevel, String)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendFrame(ClientFrame::Publish { channel, body, .. }) if channel == ERRORS => {
                let report: brenn_surface_schema::telemetry::ErrorReportDocument =
                    serde_json::from_str(body.as_text().expect("a report is text"))
                        .expect("an error report body");
                Some((report.source, report.level, report.message))
            }
            _ => None,
        })
        .collect()
}

/// Both folds a fetch report reaches the error channel through: a delivery's
/// (an attachment over the bytes held) and a blob answer's (a failed read).
#[test]
fn a_fetch_report_is_published_on_the_error_channel_under_the_fetch_source() {
    let mut document = doc(MIB);
    document.platform.error_channel = Some(ERRORS.to_string());
    document.platform.error_report_floor = Some(LogLevel::Warn);
    let facts = AttachmentFacts {
        blobs: Some(BlobFacts {
            max_bytes_held: 100,
            ..door()
        }),
        ..pages::facts()
    };
    let mut page = pages::configured_page(CONFIG, EPOCH, facts, &["p1"], &document, NOW);
    let subscribed = ServerFrame::SubscribeResult {
        channel: WIRE.to_string(),
        outcome: brenn_attach_proto::SubscribeOutcome::Ok,
        replay_count: 0,
        gap: None,
    };
    on_input(&mut page, Input::Frame(subscribed), NOW, 0);
    let delivery = |seq: u64, size: u64| {
        let mut row = crate::test_support::frames::row(WIRE, "m", seq, u128::from(seq));
        row.envelope.attachments = env(u128::from(seq), &[("f", size)]).attachments;
        Input::Frame(ServerFrame::Deliver {
            channel: WIRE.to_string(),
            rows: vec![row],
        })
    };

    let over = on_input(&mut page, delivery(1, 200), NOW, 0);
    let [(source, level, message)] = &error_reports(&over)[..] else {
        panic!("one report: {over:?}")
    };
    assert_eq!((source.as_str(), *level), ("kernel:fetch", LogLevel::Warn));
    assert!(message.contains(WIRE), "{message}");

    let fetching = on_input(&mut page, delivery(2, 10), NOW, 0);
    let [handle] = fetching
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendFrame(ClientFrame::BlobOpen { handle, .. }) => Some(*handle),
            _ => None,
        })
        .collect::<Vec<_>>()[..]
    else {
        panic!("one open: {fetching:?}")
    };
    let read = on_input(&mut page, Input::Frame(opened(handle, 10)), NOW, 0);
    let [correlation] = read
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendFrame(ClientFrame::BlobRead { correlation, .. }) => Some(*correlation),
            _ => None,
        })
        .collect::<Vec<_>>()[..]
    else {
        panic!("one read: {read:?}")
    };
    let failed = on_input(
        &mut page,
        Input::Frame(ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::StorageFailed,
        }),
        NOW,
        0,
    );
    let [(source, level, message)] = &error_reports(&failed)[..] else {
        panic!("one report: {failed:?}")
    };
    assert_eq!((source.as_str(), *level), ("kernel:fetch", LogLevel::Warn));
    assert!(message.contains("StorageFailed"), "{message}");
}
