//! The deferred set: messages published with a future release time, parked
//! until it arrives.

/// A parked message's identity within one deferred set. Stable for the life of
/// the entry: an edit keeps it, a cancel or a release retires it, and no later
/// park reuses it.
pub type DeferredId = u64;

/// A wall-clock instant, in milliseconds since the Unix epoch, UTC.
///
/// The clock is always the caller's: this crate never reads one, so the same
/// code runs on a host that has `SystemTime` and one that does not.
pub type ReleaseTime = u64;

/// One parked message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deferred<M> {
    pub id: DeferredId,
    /// The identity that parked it. The only authorization key the set knows:
    /// a view or an edit scoped to a sender can reach nothing else.
    pub sender: String,
    pub release_at: ReleaseTime,
    pub message: M,
}

/// A park rejected because the set is already at its cap.
///
/// Never a drop-oldest: silently cancelling scheduled work is worse than
/// refusing to schedule more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaExceeded {
    /// The cap that was reached, in messages.
    pub cap: u64,
}

/// An edit or cancel that named an entry the set does not hold — released,
/// cancelled, or never parked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoSuchDeferred(pub DeferredId);

/// One channel's parked messages, ordered by release time.
///
/// Parked messages are not in retention, so the retention bound does not apply
/// to them and the set carries its own cap. The cap is channel-wide, shared
/// across senders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredSet<M> {
    /// Maximum parked messages, channel-wide. `None` is unbounded — meaningful
    /// only for a channel whose retention is itself unbounded by operator
    /// choice.
    cap: Option<u64>,
    next_id: DeferredId,
    /// Sorted ascending by `(release_at, id)`, so the head is always the next
    /// due entry and a view is already in release order.
    entries: Vec<Deferred<M>>,
}

impl<M> DeferredSet<M> {
    pub fn new(cap: Option<u64>) -> Self {
        Self {
            cap,
            next_id: 1,
            entries: Vec::new(),
        }
    }

    pub fn cap(&self) -> Option<u64> {
        self.cap
    }

    /// Retune the cap. A shrink below the current occupancy never evicts: the
    /// existing entries keep their release times and the set refuses new parks
    /// until it drains under the new cap.
    pub fn set_cap(&mut self, cap: Option<u64>) {
        self.cap = cap;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Park a message for release at `release_at`.
    pub fn park(
        &mut self,
        sender: impl Into<String>,
        message: M,
        release_at: ReleaseTime,
    ) -> Result<DeferredId, QuotaExceeded> {
        if let Some(cap) = self.cap
            && self.entries.len() as u64 >= cap
        {
            return Err(QuotaExceeded { cap });
        }
        let id = self.next_id;
        self.next_id += 1;
        let entry = Deferred {
            id,
            sender: sender.into(),
            release_at,
            message,
        };
        let at = self.position_for(release_at, id);
        self.entries.insert(at, entry);
        Ok(id)
    }

    /// When the next entry comes due, or `None` when nothing is parked. The
    /// deadline a release loop waits on.
    pub fn next_release(&self) -> Option<ReleaseTime> {
        self.entries.first().map(|e| e.release_at)
    }

    /// Remove and return every entry due at or before `now`, in release order.
    pub fn release_due(&mut self, now: ReleaseTime) -> Vec<Deferred<M>> {
        let due = self
            .entries
            .iter()
            .take_while(|e| e.release_at <= now)
            .count();
        self.entries.drain(..due).collect()
    }

    /// Every entry, release order, oldest release first.
    pub fn iter(&self) -> impl Iterator<Item = &Deferred<M>> {
        self.entries.iter()
    }

    /// One sender's parked messages, release order.
    ///
    /// This is the whole authorization story for a per-sender view: the filter
    /// is structural, so a caller scoped to a sender can never observe another
    /// sender's parked message, and no identity check is needed at the point of
    /// an edit or a cancel that names an entry from such a view.
    pub fn for_sender<'a>(&'a self, sender: &'a str) -> impl Iterator<Item = &'a Deferred<M>> {
        self.entries.iter().filter(move |e| e.sender == sender)
    }

    pub fn get(&self, id: DeferredId) -> Option<&Deferred<M>> {
        self.entries.iter().find(|e| e.id == id)
    }

    /// Remove one entry. `None` when it is no longer parked, which a caller
    /// racing the release loop must expect rather than treat as a bug.
    pub fn cancel(&mut self, id: DeferredId) -> Option<Deferred<M>> {
        let at = self.entries.iter().position(|e| e.id == id)?;
        Some(self.entries.remove(at))
    }

    /// Replace one entry's payload, its release time, or both, keeping its id.
    ///
    /// Errs when the entry is no longer parked; the caller decides whether the
    /// race is benign.
    pub fn edit(
        &mut self,
        id: DeferredId,
        message: Option<M>,
        release_at: Option<ReleaseTime>,
    ) -> Result<(), NoSuchDeferred> {
        let at = self
            .entries
            .iter()
            .position(|e| e.id == id)
            .ok_or(NoSuchDeferred(id))?;
        if let Some(message) = message {
            self.entries[at].message = message;
        }
        let Some(release_at) = release_at else {
            return Ok(());
        };
        let mut entry = self.entries.remove(at);
        entry.release_at = release_at;
        let to = self.position_for(release_at, id);
        self.entries.insert(to, entry);
        Ok(())
    }

    /// Where an entry with this release time and id belongs in the sorted
    /// vector: after every entry that is due earlier, and after every
    /// equally-due entry parked before it, so equal release times release in
    /// park order.
    fn position_for(&self, release_at: ReleaseTime, id: DeferredId) -> usize {
        self.entries
            .partition_point(|e| (e.release_at, e.id) < (release_at, id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(cap: Option<u64>) -> DeferredSet<&'static str> {
        DeferredSet::new(cap)
    }

    #[test]
    fn park_orders_by_release_then_park_order() {
        let mut s = set(None);
        s.park("alice", "late", 300).unwrap();
        s.park("alice", "early", 100).unwrap();
        s.park("alice", "tie-second", 100).unwrap();
        assert_eq!(
            s.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["early", "tie-second", "late"]
        );
        assert_eq!(s.next_release(), Some(100));
    }

    #[test]
    fn release_due_takes_only_matured_entries() {
        let mut s = set(None);
        s.park("alice", "a", 100).unwrap();
        s.park("alice", "b", 200).unwrap();
        s.park("alice", "c", 300).unwrap();
        let due = s.release_due(200);
        assert_eq!(
            due.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(s.len(), 1);
        assert_eq!(s.next_release(), Some(300));
    }

    #[test]
    fn release_due_before_anything_matures_is_empty() {
        let mut s = set(None);
        s.park("alice", "a", 100).unwrap();
        assert!(s.release_due(99).is_empty());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn cap_rejects_rather_than_evicting() {
        let mut s = set(Some(2));
        s.park("alice", "a", 100).unwrap();
        s.park("bob", "b", 100).unwrap();
        assert_eq!(s.park("alice", "c", 100), Err(QuotaExceeded { cap: 2 }));
        // The cap is channel-wide: both senders' entries survive the rejection.
        assert_eq!(s.len(), 2);
        assert_eq!(s.for_sender("bob").count(), 1);
    }

    #[test]
    fn cap_frees_up_as_entries_release() {
        let mut s = set(Some(1));
        s.park("alice", "a", 100).unwrap();
        assert!(s.park("alice", "b", 200).is_err());
        s.release_due(100);
        assert!(s.park("alice", "b", 200).is_ok());
    }

    #[test]
    fn zero_cap_parks_nothing() {
        let mut s = set(Some(0));
        assert_eq!(s.park("alice", "a", 100), Err(QuotaExceeded { cap: 0 }));
    }

    #[test]
    fn shrinking_the_cap_never_evicts() {
        let mut s = set(Some(4));
        s.park("alice", "a", 100).unwrap();
        s.park("alice", "b", 100).unwrap();
        s.set_cap(Some(1));
        assert_eq!(s.len(), 2);
        assert!(s.park("alice", "c", 100).is_err());
    }

    #[test]
    fn for_sender_sees_only_its_own_entries() {
        let mut s = set(None);
        s.park("alice", "a1", 200).unwrap();
        s.park("bob", "b1", 100).unwrap();
        s.park("alice", "a2", 300).unwrap();
        assert_eq!(
            s.for_sender("alice").map(|e| e.message).collect::<Vec<_>>(),
            vec!["a1", "a2"]
        );
        assert_eq!(
            s.for_sender("bob").map(|e| e.message).collect::<Vec<_>>(),
            vec!["b1"]
        );
    }

    #[test]
    fn cancel_removes_by_id_and_is_not_reused() {
        let mut s = set(None);
        let a = s.park("alice", "a", 100).unwrap();
        let b = s.park("alice", "b", 200).unwrap();
        assert_eq!(s.cancel(a).map(|e| e.message), Some("a"));
        assert!(s.cancel(a).is_none());
        let c = s.park("alice", "c", 300).unwrap();
        assert_ne!(c, a);
        assert_ne!(c, b);
    }

    #[test]
    fn edit_payload_keeps_position_and_id() {
        let mut s = set(None);
        let a = s.park("alice", "a", 100).unwrap();
        s.park("alice", "b", 200).unwrap();
        s.edit(a, Some("a-edited"), None).unwrap();
        assert_eq!(
            s.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["a-edited", "b"]
        );
        assert_eq!(s.get(a).map(|e| e.id), Some(a));
    }

    #[test]
    fn edit_release_time_reorders() {
        let mut s = set(None);
        let a = s.park("alice", "a", 100).unwrap();
        s.park("alice", "b", 200).unwrap();
        s.edit(a, None, Some(300)).unwrap();
        assert_eq!(
            s.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        assert_eq!(s.next_release(), Some(200));
    }

    #[test]
    fn edit_of_a_released_entry_errs() {
        let mut s = set(None);
        let a = s.park("alice", "a", 100).unwrap();
        s.release_due(100);
        assert_eq!(s.edit(a, Some("x"), None), Err(NoSuchDeferred(a)));
    }
}
