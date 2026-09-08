//! The plan's subscriber entries, grouped by the principal that holds them.
//!
//! One grouping of one walk, read by everything that asks "what does this
//! principal subscribe to in the candidate": level 2's agent delta asks it of
//! both plans while it is deciding what moved, and commit asks it of the
//! candidate while it folds each arriving principal onto its channels.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use brenn_lib::messaging::{
    ChannelEntry, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
};

/// The plan's subscriber entries, grouped by the principal that holds them.
///
/// Built once per commit and read by both arrival steps, so the walk over the
/// candidate's channels happens once rather than once per arriving principal.
/// The key is the directory's own subscriber identity: for every kind, the
/// derived equality this map hashes by and
/// [`SubscriberEntryKind::same_principal`] are the same relation — every field
/// either compares, one compares — which
/// `a_planned_group_is_exactly_what_same_principal_matches` holds.
pub(crate) struct PlannedSubscribers<'a> {
    by_principal: HashMap<&'a SubscriberEntryKind, Vec<(&'a ChannelEntry, &'a SubscriberEntry)>>,
}

impl<'a> PlannedSubscribers<'a> {
    pub(crate) fn of(channels: &'a [Arc<ChannelEntry>]) -> Self {
        let mut by_principal: HashMap<_, Vec<_>> = HashMap::new();
        for entry in channels {
            for subscriber in &entry.subscribers {
                by_principal
                    .entry(&subscriber.kind)
                    .or_default()
                    .push((entry.as_ref(), subscriber));
            }
        }
        Self { by_principal }
    }

    /// What one principal subscribes to in this plan, in the plan's own order.
    pub(crate) fn of_principal(
        &self,
        kind: &SubscriberEntryKind,
    ) -> &[(&'a ChannelEntry, &'a SubscriberEntry)] {
        self.by_principal.get(kind).map_or(&[], Vec::as_slice)
    }

    /// Fold one principal's planned entries onto the live directory.
    ///
    /// The one implementation of "put this principal on its channels", shared
    /// by the arriving consumers and the arriving surfaces: the entry each one
    /// joins with is the candidate plan's verbatim, so what the live directory
    /// ends up holding is what a fresh boot would have folded. `what` names the
    /// kind of principal in the panic.
    pub(crate) fn fold(&self, live: &MessagingDirectory, kind: &SubscriberEntryKind, what: &str) {
        self.fold_where(live, kind, what, |_| true);
    }

    /// The same fold, restricted to the channels named by `uuids`.
    ///
    /// An agent is not an arriving principal: it keeps every subscription this
    /// reload did not move, so only the entries level 2 put on its `subs_added`
    /// side are folded, and the rest are left exactly where they are.
    pub(crate) fn fold_onto(
        &self,
        live: &MessagingDirectory,
        kind: &SubscriberEntryKind,
        what: &str,
        uuids: &HashSet<Uuid>,
    ) {
        self.fold_where(live, kind, what, |entry| uuids.contains(&entry.uuid));
    }

    fn fold_where(
        &self,
        live: &MessagingDirectory,
        kind: &SubscriberEntryKind,
        what: &str,
        wanted: impl Fn(&ChannelEntry) -> bool,
    ) {
        for (entry, subscriber) in self.of_principal(kind) {
            if !wanted(entry) {
                continue;
            }
            let applied = live.add_subscriber(&entry.uuid, (*subscriber).clone());
            assert!(
                applied,
                "reload commit: {what} {:?} subscribes to channel {:?}, which the live directory \
                 does not hold — host bug",
                kind.slug(),
                entry.address,
            );
        }
    }
}
