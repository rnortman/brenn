//! What a page fetches of the attachments its wire channels deliver: the arrival
//! hold, the attachment cache, and the blob plane that fills it.
//!
//! An activation on a page is one synchronous stretch, so a component's `read`
//! answers from bytes the page already holds. They are fetched before the row
//! naming them enters the page's store: on a **gated** channel — one whose fetch
//! bound is above zero — a delivered row waits in the channel's arrival hold
//! until every attachment of it the page fetches has settled, and is then
//! inserted exactly as a row on any other channel is. The store, its cursors,
//! readiness and coalescing never see the hold.
//!
//! # The hold
//!
//! Per channel, delivered rows not yet stored, oldest first. A row is waiting,
//! fetching or settled. Three rules move it after every event that can:
//!
//! 1. **Start.** While fewer than [`HOLD_FETCHING_ROWS`] rows of the channel are
//!    fetching, the oldest waiting row starts: its deadline runs from now and a
//!    cache entry is made for each eligible attachment.
//! 2. **Release.** While the oldest row is settled or past its deadline, it is
//!    inserted into the store. Release is in arrival order, so the store's order
//!    is the channel's; a row released by its deadline keeps its fetches running.
//! 3. **Overflow.** The hold has room for the store's depth plus
//!    [`HOLD_FETCHING_ROWS`] rows. A row arriving at a full hold evicts the
//!    oldest waiting row, counted as a loss upstream of the store; with no row
//!    waiting, the oldest row is released as it is.
//!
//! A row that names nothing the page fetches, arriving at an empty hold, is
//! stored in the same pass, as is every row on a channel with no hold and no
//! bound.
//!
//! # The cache
//!
//! One entry per `(channel, message id, attachment name)` the page fetches,
//! living while its message is in the channel's hold or store. Bytes being
//! fetched are reserved at the attachment's full size, and reservations plus
//! fetched bytes stay within [`PAGE_BLOB_BYTES`]: room is made by dropping the
//! bytes of stored messages, oldest first, and an entry that still does not fit
//! is never fetched.
//!
//! # The plane
//!
//! One [`BlobPlane`] per page. An entry is opened, read whole, and closed as soon
//! as its bytes are here, so the server's handles bound fetches in flight, not
//! attachments cached. A detach returns every fetch in flight to wanted; the next
//! attachment fetches it again.
//!
//! Nothing here reads a clock or sends a frame: every pass answers frames to
//! send, rows it inserted with the positions they overflowed, and reports for the
//! page's error channel. Each kind of trouble — a refusal reason, a drop for
//! room — is reported once per channel per connection, so a fault that hits
//! every frame of a stream is one report and not one per frame.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use brenn_attach_client::Millis;
use brenn_attach_client::blobs::{BlobEvent, BlobPlane, OpenError, OpenRefusal, ReadRefusal};
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::store::CursorOverflow;
use brenn_attach_proto::{
    AttachmentName, BLOB_CHUNK_BYTES, BlobFacts, BlobHandle, ClientFrame, ServerFrame,
};
use brenn_envelope::MessageEnvelope;
use brenn_surface_schema::{LogLevel, PAGE_BLOB_BYTES};
use uuid::Uuid;

use crate::activation::DropVerdicts;
use crate::bindings::AppliedBindings;
use crate::page::SurfacePage;
use crate::registry::{BindingKey, SurfaceStores};

/// The most rows of one channel's hold fetching at once.
pub const HOLD_FETCHING_ROWS: usize = 4;

/// A row's deadline before its attachments' chunks are counted.
pub const HOLD_BASE_MS: u64 = 2_000;

/// What each chunk of each eligible attachment adds to a row's deadline.
pub const HOLD_CHUNK_MS: u64 = 1_000;

/// The ceiling on a row's deadline, whatever it names.
pub const HOLD_MAX_MS: u64 = 30_000;

/// The chunk a row's deadline counts in: one read answer's worth.
const CHUNK_BYTES: u64 = BLOB_CHUNK_BYTES as u64;

/// How long a row that starts fetching may hold its channel: [`HOLD_BASE_MS`]
/// plus [`HOLD_CHUNK_MS`] per chunk of each eligible attachment — at least one
/// per attachment, since each costs a read answer — capped at [`HOLD_MAX_MS`].
pub fn hold_deadline_ms(sizes: impl IntoIterator<Item = u64>) -> u64 {
    let chunks = sizes
        .into_iter()
        .map(|size| size.div_ceil(CHUNK_BYTES).max(1))
        .fold(0u64, u64::saturating_add);
    HOLD_BASE_MS
        .saturating_add(chunks.saturating_mul(HOLD_CHUNK_MS))
        .min(HOLD_MAX_MS)
}

/// Names one fetch: an entry's open and read on one attachment. Never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FetchId(u64);

/// A report for the page's error channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchReport {
    pub level: LogLevel,
    pub message: String,
}

/// What a fetch pass produced, before its insertions are charged.
#[derive(Debug, Default)]
pub struct FetchStep {
    /// Frames to send, in order.
    pub frames: Vec<ClientFrame>,
    /// Per release, the channel and the positions its insert overflowed, in
    /// release order.
    pub released: Vec<(String, Vec<CursorOverflow<BindingKey>>)>,
    pub reports: Vec<FetchReport>,
}

/// What a fetch pass produced, its insertions charged to the loudness ladder.
#[derive(Debug, Default, PartialEq)]
pub struct Fetched {
    pub frames: Vec<ClientFrame>,
    pub drops: DropVerdicts,
    pub reports: Vec<FetchReport>,
}

/// What the cache holds of one attachment, as a reader sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cached<'a> {
    /// The attachment's bytes, whole.
    Bytes(&'a [u8]),
    /// The server answered that the message left the channel's window.
    Gone,
    /// The bytes are not on this page: never fetched, still on their way,
    /// refused, failed, or dropped for room.
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    channel: String,
    message_id: Uuid,
    name: AttachmentName,
}

#[derive(Debug)]
enum State {
    /// Eligible; no fetch on the wire.
    Wanted,
    Fetching {
        fetch: FetchId,
        handle: BlobHandle,
        bytes: Vec<u8>,
    },
    Fetched(Vec<u8>),
    /// The server answered `NotRetained`.
    Gone,
    /// Refused, failed, or its bytes were dropped for room. Not fetched again.
    Unfetched,
}

#[derive(Debug)]
struct Entry {
    size: u64,
    content_type: String,
    /// The arrival order of the row naming it, page-wide.
    arrival: u64,
    state: State,
}

impl Entry {
    fn settled(&self) -> bool {
        !matches!(self.state, State::Wanted | State::Fetching { .. })
    }

    /// What it counts against [`PAGE_BLOB_BYTES`].
    fn cache_bytes(&self) -> u64 {
        match &self.state {
            State::Fetching { .. } => self.size,
            State::Fetched(bytes) => bytes.len() as u64,
            State::Wanted | State::Gone | State::Unfetched => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Waiting,
    Fetching { deadline: Millis },
    Settled,
}

#[derive(Debug)]
struct HeldRow {
    envelope: MessageEnvelope,
    arrival: u64,
    /// The attachments the channel's bound at arrival made eligible.
    eligible: Vec<AttachmentName>,
    phase: Phase,
}

/// The page's attachment fetching: every hold, the cache, and the plane.
pub struct Fetch {
    plane: BlobPlane<FetchId>,
    /// The live attachment's blob door; `None` while detached or doorless. A
    /// copy of the facts the plane keeps privately, set and cleared with them.
    door: Option<BlobFacts>,
    holds: BTreeMap<String, VecDeque<HeldRow>>,
    entries: BTreeMap<EntryKey, Entry>,
    /// The entry each fetch on the wire fills.
    fetches: BTreeMap<FetchId, EntryKey>,
    next_fetch: u64,
    next_arrival: u64,
    /// The `(channel, trouble)` pairs already reported on this connection: each
    /// kind of fetch trouble is reported once per channel until the next
    /// detach, so a fault that hits every frame of a stream is one report.
    reported: BTreeSet<(String, &'static str)>,
    /// The deadline last stated to the runner.
    armed: Option<Millis>,
}

impl Default for Fetch {
    fn default() -> Self {
        Self::new()
    }
}

impl Fetch {
    pub fn new() -> Self {
        Self {
            plane: BlobPlane::new(),
            door: None,
            holds: BTreeMap::new(),
            entries: BTreeMap::new(),
            fetches: BTreeMap::new(),
            next_fetch: 1,
            next_arrival: 1,
            reported: BTreeSet::new(),
            armed: None,
        }
    }

    /// A new attachment is live. Nothing is fetched until a pass runs.
    pub fn on_attached(&mut self, facts: &AttachmentFacts) {
        self.plane.on_attached(facts);
        self.door = facts.blobs;
    }

    /// The attachment went away: every fetch in flight returns to wanted, its
    /// partial bytes discarded. Fetched bytes and held rows stay; trouble still
    /// present on the next attachment is reported again.
    pub fn on_detached(&mut self) {
        // Every outstanding open and read is reported `Detached`, which is what
        // the reset below does to every fetching entry whatever it was owed.
        self.plane.on_detached();
        self.door = None;
        self.fetches.clear();
        self.reported.clear();
        for entry in self.entries.values_mut() {
            if matches!(entry.state, State::Fetching { .. }) {
                entry.state = State::Wanted;
            }
        }
    }

    /// What the cache holds of attachment `name` of message `message_id` on
    /// `channel`.
    pub fn cached(&self, channel: &str, message_id: Uuid, name: &AttachmentName) -> Cached<'_> {
        let key = EntryKey {
            channel: channel.to_string(),
            message_id,
            name: name.clone(),
        };
        match self.entries.get(&key).map(|entry| &entry.state) {
            Some(State::Fetched(bytes)) => Cached::Bytes(bytes),
            Some(State::Gone) => Cached::Gone,
            _ => Cached::Absent,
        }
    }

    /// Bytes reserved for fetches plus bytes fetched, across the page.
    pub fn cache_bytes(&self) -> u64 {
        self.entries
            .values()
            .map(Entry::cache_bytes)
            .fold(0, u64::saturating_add)
    }

    /// The earliest deadline among the holds' heads, if it moved since it was
    /// last stated: `Some(None)` disarms.
    ///
    /// Only a head is ever released, so a deadline behind one wakes nothing:
    /// when it passes first, its row goes out with the head that releases it.
    ///
    /// # Panics
    ///
    /// If a head is waiting or settled. Rows start oldest first, so no waiting
    /// row is ahead of a fetching one, and every pass runs the rules until
    /// nothing moves: a settled head is released, and a waiting head has no
    /// fetching row anywhere in its hold and so is started.
    pub fn wakeup_change(&mut self) -> Option<Option<Millis>> {
        let next = self
            .holds
            .iter()
            .filter_map(|(channel, hold)| {
                let head = hold.front()?;
                match head.phase {
                    Phase::Fetching { deadline } => Some(deadline),
                    Phase::Waiting | Phase::Settled => panic!(
                        "surface client: the head of {channel:?}'s hold is {:?} after the rules \
                         ran",
                        head.phase
                    ),
                }
            })
            .min();
        (next != self.armed).then(|| {
            self.armed = next;
            next
        })
    }

    /// Take one delivery pass on a channel that is not the config channel.
    ///
    /// # Panics
    ///
    /// If `channel` has no store.
    pub fn on_rows(
        &mut self,
        stores: &mut SurfaceStores,
        bindings: &AppliedBindings,
        channel: &str,
        rows: Vec<MessageEnvelope>,
        now: Millis,
    ) -> FetchStep {
        let mut step = FetchStep::default();
        let bound = bindings.fetch_bound(channel);
        // The rules run after each row, as after any arrival: a row that starts
        // fetching is never the one a later row of the same pass evicts.
        for envelope in rows {
            // An attachment over the bound is never fetched and reads as
            // unavailable, whole or in part.
            // TODO(surface-blob-partial-read-over-bound): serve a range of it.
            let eligible: Vec<AttachmentName> = envelope
                .attachments
                .iter()
                .filter(|(_, blob)| blob.size <= bound)
                .map(|(name, _)| name.clone())
                .collect();
            let hold_empty = self.holds.get(channel).is_none_or(VecDeque::is_empty);
            if hold_empty && eligible.is_empty() {
                self.insert(stores, channel, envelope, &mut step);
                continue;
            }
            let store = store_mut(stores, channel);
            let id = envelope.message_id;
            let hold = self.holds.entry(channel.to_string()).or_default();
            if hold.iter().any(|row| row.envelope.message_id == id)
                || store.retained().any(|(stored, _)| stored.message_id == id)
            {
                continue;
            }
            let capacity = usize::try_from(store.depth())
                .unwrap_or(usize::MAX)
                .saturating_add(HOLD_FETCHING_ROWS);
            if hold.len() >= capacity {
                match hold.iter().position(|row| row.phase == Phase::Waiting) {
                    Some(index) => {
                        hold.remove(index);
                        store.count_server_drops(1);
                    }
                    None => {
                        let head = hold.pop_front().expect("a full hold has a head");
                        self.insert(stores, channel, head.envelope, &mut step);
                    }
                }
            }
            let arrival = self.next_arrival;
            self.next_arrival += 1;
            let phase = if eligible.is_empty() {
                Phase::Settled
            } else {
                Phase::Waiting
            };
            self.holds
                .entry(channel.to_string())
                .or_default()
                .push_back(HeldRow {
                    envelope,
                    arrival,
                    eligible,
                    phase,
                });
            self.settle(stores, now, &mut step);
        }
        step
    }

    /// Settle one blob answer.
    ///
    /// `Err` is a peer contract the page cannot reconcile: an answer nothing
    /// asked for, an open whose facts contradict the delivered reference, or a
    /// denial of an attachment the server's own delivery named.
    pub fn on_frame(
        &mut self,
        stores: &mut SurfaceStores,
        frame: ServerFrame,
        now: Millis,
    ) -> Result<FetchStep, String> {
        let mut step = FetchStep::default();
        let blob = self.plane.on_frame(frame)?;
        step.frames.extend(blob.frames);
        for event in blob.events {
            self.on_event(event, &mut step)?;
        }
        self.settle(stores, now, &mut step);
        Ok(step)
    }

    /// A deadline fired: release whatever is past its own.
    pub fn on_due(&mut self, stores: &mut SurfaceStores, now: Millis) -> FetchStep {
        let mut step = FetchStep::default();
        self.settle(stores, now, &mut step);
        step
    }

    /// A bindings document was put in force over the stores.
    ///
    /// A channel whose store is gone takes its hold, entries and fetches with
    /// it; every other channel's entries are swept against what its store now
    /// retains. `Err` when the document gates a channel and the live attachment
    /// opened no blob door: the peer contradicts its own wiring.
    pub fn on_config(
        &mut self,
        stores: &mut SurfaceStores,
        bindings: &AppliedBindings,
        now: Millis,
    ) -> Result<FetchStep, String> {
        if self.door.is_none()
            && let Some(channel) = bindings.gated_channels().next()
        {
            return Err(format!(
                "the bindings document fetches attachments on {channel}, and the attachment \
                 opened no blob door"
            ));
        }
        let mut step = FetchStep::default();
        let gone: Vec<String> = self
            .holds
            .keys()
            .chain(self.entries.keys().map(|key| &key.channel))
            .filter(|channel| stores.get(channel).is_none())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for channel in &gone {
            self.holds.remove(channel);
            self.sweep(stores, channel, &mut step);
        }
        let swept: BTreeSet<String> = self.entries.keys().map(|key| key.channel.clone()).collect();
        for channel in &swept {
            self.sweep(stores, channel, &mut step);
        }
        self.settle(stores, now, &mut step);
        Ok(step)
    }

    /// Run the three rules and start fetches until nothing moves.
    fn settle(&mut self, stores: &mut SurfaceStores, now: Millis, step: &mut FetchStep) {
        loop {
            let mut moved = false;
            let channels: Vec<String> = self.holds.keys().cloned().collect();
            for channel in &channels {
                moved |= self.advance_hold(stores, channel, now, step);
            }
            self.holds.retain(|_, hold| !hold.is_empty());
            moved |= self.start_fetches(step);
            if !moved {
                break;
            }
        }
    }

    /// Settle, start and release one channel's hold. Answers whether anything
    /// moved.
    fn advance_hold(
        &mut self,
        stores: &mut SurfaceStores,
        channel: &str,
        now: Millis,
        step: &mut FetchStep,
    ) -> bool {
        let mut moved = false;
        let Some(mut hold) = self.holds.remove(channel) else {
            return false;
        };
        for row in hold.iter_mut() {
            if matches!(row.phase, Phase::Fetching { .. }) && self.row_settled(channel, row) {
                row.phase = Phase::Settled;
                moved = true;
            }
        }
        loop {
            let fetching = hold
                .iter()
                .filter(|row| matches!(row.phase, Phase::Fetching { .. }))
                .count();
            if fetching >= HOLD_FETCHING_ROWS {
                break;
            }
            let Some(row) = hold.iter_mut().find(|row| row.phase == Phase::Waiting) else {
                break;
            };
            moved = true;
            let refs: Vec<(AttachmentName, u64, String)> = row
                .eligible
                .iter()
                .map(|name| {
                    let blob = row
                        .envelope
                        .attachments
                        .get(name)
                        .expect("surface client: an eligible name is one of the row's own");
                    (name.clone(), blob.size, blob.content_type.clone())
                })
                .collect();
            row.phase = Phase::Fetching {
                deadline: now
                    .saturating_add_ms(hold_deadline_ms(refs.iter().map(|(_, size, _)| *size))),
            };
            for (name, size, content_type) in refs {
                self.entries.insert(
                    EntryKey {
                        channel: channel.to_string(),
                        message_id: row.envelope.message_id,
                        name,
                    },
                    Entry {
                        size,
                        content_type,
                        arrival: row.arrival,
                        state: State::Wanted,
                    },
                );
            }
        }
        while let Some(head) = hold.front() {
            let due = match head.phase {
                Phase::Settled => true,
                Phase::Fetching { deadline } => deadline <= now,
                Phase::Waiting => false,
            };
            if !due {
                break;
            }
            let head = hold.pop_front().expect("the head was just read");
            moved = true;
            // Back in place before the insert, so the sweep behind it still sees
            // the rows that are held.
            self.holds.insert(channel.to_string(), hold);
            self.insert(stores, channel, head.envelope, step);
            hold = self
                .holds
                .remove(channel)
                .expect("surface client: the hold was just put back");
        }
        self.holds.insert(channel.to_string(), hold);
        moved
    }

    fn row_settled(&self, channel: &str, row: &HeldRow) -> bool {
        row.eligible.iter().all(|name| {
            let key = EntryKey {
                channel: channel.to_string(),
                message_id: row.envelope.message_id,
                name: name.clone(),
            };
            self.entries.get(&key).is_none_or(Entry::settled)
        })
    }

    /// Insert one row into its channel's store, the way every delivered row is,
    /// and sweep the cache against what the store retains after it.
    fn insert(
        &mut self,
        stores: &mut SurfaceStores,
        channel: &str,
        envelope: MessageEnvelope,
        step: &mut FetchStep,
    ) {
        let overflow = store_mut(stores, channel).insert(envelope);
        step.released.push((channel.to_string(), overflow));
        self.sweep(stores, channel, step);
    }

    /// Delete every entry on `channel` whose message is neither held nor
    /// retained, closing a fetch still running for it.
    fn sweep(&mut self, stores: &SurfaceStores, channel: &str, step: &mut FetchStep) {
        let mut live: BTreeSet<Uuid> = self
            .holds
            .get(channel)
            .into_iter()
            .flatten()
            .map(|row| row.envelope.message_id)
            .collect();
        if let Some(store) = stores.get(channel) {
            live.extend(store.retained().map(|(envelope, _)| envelope.message_id));
        }
        let dead: Vec<EntryKey> = self
            .entries
            .keys()
            .filter(|key| key.channel == channel && !live.contains(&key.message_id))
            .cloned()
            .collect();
        for key in dead {
            let entry = self
                .entries
                .remove(&key)
                .expect("the key came from the map");
            if let State::Fetching { fetch, handle, .. } = entry.state {
                self.fetches.remove(&fetch);
                // The reads this ends are reported `Closed` and are this
                // deletion's own; nothing is left to settle.
                step.frames.extend(self.plane.close(handle).frames);
            }
        }
    }

    /// Start wanted entries — held rows' first, then stored messages', oldest
    /// first — while the door's handles and bytes held and the page's cache
    /// bound allow. Answers whether anything moved.
    fn start_fetches(&mut self, step: &mut FetchStep) -> bool {
        let Some(door) = self.door else {
            return false;
        };
        let held: BTreeSet<(String, Uuid)> = self
            .holds
            .iter()
            .flat_map(|(channel, hold)| {
                hold.iter()
                    .map(move |row| (channel.clone(), row.envelope.message_id))
            })
            .collect();
        let is_held = |key: &EntryKey| held.contains(&(key.channel.clone(), key.message_id));
        let mut wanted: Vec<(bool, u64, EntryKey)> = self
            .entries
            .iter()
            .filter(|(_, entry)| matches!(entry.state, State::Wanted))
            .map(|(key, entry)| (!is_held(key), entry.arrival, key.clone()))
            .collect();
        wanted.sort();
        let mut moved = false;
        for (_, _, key) in wanted {
            let size = self.entries[&key].size;
            if size > door.max_bytes_held {
                moved = true;
                self.entries
                    .get_mut(&key)
                    .expect("the key came from the map")
                    .state = State::Unfetched;
                self.report(
                    step,
                    &key.channel,
                    "attachments over the bytes held",
                    LogLevel::Warn,
                    || {
                        format!(
                            "attachment {} of message {} on {} is {size} bytes, more than the \
                         server lets one session hold ({}); it is not fetched",
                            key.name, key.message_id, key.channel, door.max_bytes_held
                        )
                    },
                );
                continue;
            }
            let in_flight = self
                .entries
                .values()
                .filter(|entry| matches!(entry.state, State::Fetching { .. }))
                .map(|entry| entry.size)
                .fold(0, u64::saturating_add);
            if !door.admits_bytes_held(in_flight, size) {
                break;
            }
            if !self.make_room(size, &is_held, step) {
                moved = true;
                self.entries
                    .get_mut(&key)
                    .expect("the key came from the map")
                    .state = State::Unfetched;
                continue;
            }
            let fetch = FetchId(self.next_fetch);
            match self
                .plane
                .open(fetch, &key.channel, key.message_id, key.name.clone())
            {
                Ok((handle, frame)) => {
                    self.next_fetch += 1;
                    moved = true;
                    step.frames.push(frame);
                    self.fetches.insert(fetch, key.clone());
                    self.entries
                        .get_mut(&key)
                        .expect("the key came from the map")
                        .state = State::Fetching {
                        fetch,
                        handle,
                        bytes: Vec::new(),
                    };
                }
                Err(OpenError::HandleLimit) => break,
            }
        }
        moved
    }

    /// Make room in the cache for `size` more bytes by dropping fetched bytes of
    /// stored messages, oldest arrival first. Answers whether it fits.
    fn make_room(
        &mut self,
        size: u64,
        is_held: &impl Fn(&EntryKey) -> bool,
        step: &mut FetchStep,
    ) -> bool {
        let fits = |used: u64| used.checked_add(size).is_some_and(|t| t <= PAGE_BLOB_BYTES);
        let mut used = self.cache_bytes();
        if fits(used) {
            return true;
        }
        let mut droppable: Vec<(u64, EntryKey)> = self
            .entries
            .iter()
            .filter(|(key, entry)| matches!(entry.state, State::Fetched(_)) && !is_held(key))
            .map(|(key, entry)| (entry.arrival, key.clone()))
            .collect();
        droppable.sort();
        for (_, key) in droppable {
            if fits(used) {
                break;
            }
            let entry = self
                .entries
                .get_mut(&key)
                .expect("the key came from the map");
            used -= entry.cache_bytes();
            entry.state = State::Unfetched;
            self.report(step, &key.channel, "drops for room", LogLevel::Warn, || {
                format!(
                    "the page dropped fetched attachment bytes of messages on {} to make \
                     room for newer attachments; those attachments now read as unavailable",
                    key.channel
                )
            });
        }
        fits(used)
    }

    /// Report `trouble` on `channel` unless it was already reported since the
    /// last detach.
    fn report(
        &mut self,
        step: &mut FetchStep,
        channel: &str,
        trouble: &'static str,
        level: LogLevel,
        detail: impl FnOnce() -> String,
    ) {
        if self.reported.insert((channel.to_string(), trouble)) {
            step.reports.push(FetchReport {
                level,
                message: format!(
                    "{}; later {trouble} on {channel} are not reported again until the page \
                     reattaches",
                    detail()
                ),
            });
        }
    }

    fn on_event(&mut self, event: BlobEvent<FetchId>, step: &mut FetchStep) -> Result<(), String> {
        match event {
            BlobEvent::Opened {
                tag,
                handle,
                size,
                content_type,
            } => {
                // An entry deleted while its open was on the wire closed the
                // handle; the late answer has nothing left to fill.
                let Some(key) = self.fetches.get(&tag) else {
                    return Ok(());
                };
                let entry = &self.entries[key];
                if entry.size != size || entry.content_type != content_type {
                    return Err(format!(
                        "BlobOpenResult for attachment {} of message {} on {} answered {size} \
                         bytes of {content_type:?}, and the delivery named {} bytes of {:?}",
                        key.name, key.message_id, key.channel, entry.size, entry.content_type
                    ));
                }
                let (_, frames) = self
                    .plane
                    .read(handle, tag, 0, size)
                    .expect("surface client: a handle just opened reads its whole size");
                step.frames.extend(frames);
            }
            BlobEvent::OpenRefused { tag, reason, .. } => {
                let Some(key) = self.fetches.remove(&tag) else {
                    return Ok(());
                };
                let state = match reason {
                    OpenRefusal::NotRetained => State::Gone,
                    OpenRefusal::ToolChannel => {
                        self.report(step, &key.channel, "tool-channel refusals", LogLevel::Info, || {
                            format!(
                                "{} is a tool channel, whose attachments the server never hands \
                                 out; the page fetches none of them",
                                key.channel
                            )
                        });
                        State::Unfetched
                    }
                    OpenRefusal::HandleLimit => {
                        self.report_bound_refusal(
                            step,
                            &key,
                            "open refusals (HandleLimit)",
                            reason,
                        );
                        State::Unfetched
                    }
                    OpenRefusal::BytesHeldLimit { .. } => {
                        let trouble = "open refusals (BytesHeldLimit)";
                        self.report_bound_refusal(step, &key, trouble, reason);
                        State::Unfetched
                    }
                    OpenRefusal::Detached => State::Wanted,
                    OpenRefusal::NoSuchAttachment => {
                        return Err(format!(
                            "BlobOpenResult NoSuchAttachment for attachment {} of message {} on \
                             {}, which the server's own delivery named",
                            key.name, key.message_id, key.channel
                        ));
                    }
                };
                self.entry_mut(&key).state = state;
            }
            BlobEvent::Chunk {
                handle,
                offset,
                bytes,
                ..
            } => {
                // Among the fetches on the wire, which `max_handles` bounds, not
                // every entry the cache holds.
                let entries = &self.entries;
                let key = self
                    .fetches
                    .values()
                    .find(|key| {
                        matches!(entries[*key].state, State::Fetching { handle: h, .. } if h == handle)
                    })
                    .expect("surface client: a chunk arrives only for a read the page holds");
                let entry = self
                    .entries
                    .get_mut(key)
                    .expect("surface client: a fetch on the wire has its entry");
                let State::Fetching { bytes: buffer, .. } = &mut entry.state else {
                    unreachable!("surface client: the entry was found fetching")
                };
                assert_eq!(
                    offset,
                    buffer.len() as u64,
                    "surface client: the chunks of one whole read arrive in order"
                );
                buffer.extend(bytes);
            }
            BlobEvent::ReadDone { tag, handle, .. } => {
                let key = self
                    .fetches
                    .remove(&tag)
                    .expect("surface client: a read ends only for a fetch the page holds");
                let entry = self.entry_mut(&key);
                let State::Fetching { bytes, .. } =
                    std::mem::replace(&mut entry.state, State::Unfetched)
                else {
                    unreachable!("surface client: a fetch's entry is fetching")
                };
                assert_eq!(
                    bytes.len() as u64,
                    entry.size,
                    "surface client: a whole read fills its attachment"
                );
                entry.state = State::Fetched(bytes);
                step.frames.extend(self.plane.close(handle).frames);
            }
            BlobEvent::ReadRefused {
                tag,
                handle,
                reason,
                ..
            } => {
                let key = self
                    .fetches
                    .remove(&tag)
                    .expect("surface client: a read ends only for a fetch the page holds");
                let state = match reason {
                    ReadRefusal::Detached => State::Wanted,
                    ReadRefusal::Closed => unreachable!(
                        "surface client: the page closes a fetching handle only when it deletes \
                         the entry"
                    ),
                    ReadRefusal::OutOfRange { .. }
                    | ReadRefusal::UnknownHandle
                    | ReadRefusal::NotSealed
                    | ReadRefusal::TooManyReads
                    | ReadRefusal::StorageFailed => {
                        let trouble = match reason {
                            ReadRefusal::OutOfRange { .. } => "read refusals (OutOfRange)",
                            ReadRefusal::UnknownHandle => "read refusals (UnknownHandle)",
                            ReadRefusal::NotSealed => "read refusals (NotSealed)",
                            ReadRefusal::TooManyReads => "read refusals (TooManyReads)",
                            ReadRefusal::StorageFailed => "read refusals (StorageFailed)",
                            ReadRefusal::Detached | ReadRefusal::Closed => {
                                unreachable!("surface client: matched by the arms above")
                            }
                        };
                        self.report(step, &key.channel, trouble, LogLevel::Warn, || {
                            format!(
                                "the server refused a read of attachment {} of message {} on {} \
                                 ({reason:?})",
                                key.name, key.message_id, key.channel
                            )
                        });
                        step.frames.extend(self.plane.close(handle).frames);
                        State::Unfetched
                    }
                };
                self.entry_mut(&key).state = state;
            }
            BlobEvent::Sealed { .. } | BlobEvent::UploadRefused { .. } => {
                unreachable!("surface client: the page makes no blob")
            }
        }
        Ok(())
    }

    /// Report an open the server refused against a bound the page keeps to.
    fn report_bound_refusal(
        &mut self,
        step: &mut FetchStep,
        key: &EntryKey,
        trouble: &'static str,
        reason: OpenRefusal,
    ) {
        self.report(step, &key.channel, trouble, LogLevel::Warn, || {
            format!(
                "the server refused to open attachment {} of message {} on {} ({reason:?}) \
                 within the bounds the page keeps to",
                key.name, key.message_id, key.channel
            )
        });
    }

    fn entry_mut(&mut self, key: &EntryKey) -> &mut Entry {
        self.entries
            .get_mut(key)
            .expect("surface client: a fetch on the wire has its entry")
    }
}

fn store_mut<'a>(
    stores: &'a mut SurfaceStores,
    channel: &str,
) -> &'a mut brenn_attach_client::store::ChannelStore<BindingKey> {
    stores.get_mut(channel).unwrap_or_else(|| {
        panic!("surface client: delivery on {channel:?}, which the page retains nothing for")
    })
}

/// Charge a step's insertions to the loudness ladder.
///
/// # Panics
///
/// With no bindings document in force: rows are released only onto channels a
/// document named.
fn charged(page: &mut SurfacePage, step: FetchStep) -> Fetched {
    let FetchStep {
        frames,
        released,
        reports,
    } = step;
    let mut drops = DropVerdicts::default();
    if !released.is_empty() {
        let bindings = page
            .connect
            .bindings()
            .expect("surface client: a released row implies a document in force");
        for (channel, overflow) in released {
            drops.merge(page.schedules.charge_overflow(bindings, &channel, overflow));
        }
    }
    Fetched {
        frames,
        drops,
        reports,
    }
}

/// Take one delivery pass on an application channel into the page.
///
/// # Panics
///
/// With no bindings document in force, or on a channel with no store.
pub fn on_rows(
    page: &mut SurfacePage,
    channel: &str,
    rows: Vec<MessageEnvelope>,
    now: Millis,
) -> Fetched {
    let SurfacePage {
        connect,
        stores,
        fetch,
        ..
    } = page;
    let bindings = connect
        .bindings()
        .expect("surface client: a subscribed channel implies a document in force");
    let step = fetch.on_rows(stores, bindings, channel, rows, now);
    charged(page, step)
}

/// Settle one blob answer on the page. `Err` is fatal on the connection.
pub fn on_blob_frame(
    page: &mut SurfacePage,
    frame: ServerFrame,
    now: Millis,
) -> Result<Fetched, String> {
    let step = page.fetch.on_frame(&mut page.stores, frame, now)?;
    Ok(charged(page, step))
}

/// The fetch deadline fired.
pub fn on_due(page: &mut SurfacePage, now: Millis) -> Fetched {
    let step = page.fetch.on_due(&mut page.stores, now);
    charged(page, step)
}

/// Reconcile fetching against the document just put in force. `Err` is fatal
/// on the connection.
///
/// # Panics
///
/// With no bindings document in force.
pub fn on_config(page: &mut SurfacePage, now: Millis) -> Result<Fetched, String> {
    let SurfacePage {
        connect,
        stores,
        fetch,
        ..
    } = page;
    let bindings = connect
        .bindings()
        .expect("surface client: the document just applied is in force");
    let step = fetch.on_config(stores, bindings, now)?;
    Ok(charged(page, step))
}
