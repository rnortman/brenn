//! The bookkeeping half of a DOM capability handle table: slots, reuse, and the
//! reclamation that keeps the kernel from being the last strong reference to a
//! destroyed node.
//!
//! The table is generic over what it holds and asks nothing of the DOM:
//! containment — the one question only a real tree can answer — arrives as a
//! predicate from the caller. That is what lets the free-list and generation
//! arithmetic, whose failure modes are a silent leak and a cross-element handle
//! alias, be exercised on every target the crate builds for rather than only in
//! the browser. [`crate::dom_host`] holds the one instantiation that matters,
//! over `web_sys::Element`.

use crate::dom_handle::{next_generation, pack, unpack};

/// One slot of a [`HandleTable`]: the value it holds, or the vacancy left behind
/// when that value was freed.
struct Slot<T> {
    held: Option<T>,
    /// Bumped every time the slot is freed, so a handle minted before the reuse
    /// fails to resolve against the stranger that took its place. Starts at 1;
    /// wraps back to 1 rather than through 0, which is not a generation.
    generation: u32,
}

/// One instance's handles.
///
/// The table is the whole confinement mechanism, so it is deliberately dumb: a
/// vector of slots the instance may name, each handle packing its slot index
/// and that slot's current generation. Zero is never a valid handle, so a
/// component that ships an uninitialised field traps instead of hitting an
/// element.
///
/// Canonicalisation is a linear scan, and stays one on purpose: the alternative
/// is a mark on the element itself, which `set-attribute`'s `data-` family would
/// let the component forge. Only the handful of operations that *find* an
/// element the component did not create scan — `root`, `parent`, the page-DOM
/// reads and a gesture's target walk — while `create-element`, the hot one,
/// [mints](Self::mint) without looking: an element the document just made is
/// provably in no table, so scanning for it would be pure waste on the path
/// every render walks.
///
/// # Reclamation
///
/// A slot is freed when the element it holds is destroyed — which is what
/// `dom.remove` and `dom.set-text` do to a subtree, and what
/// [`free_within`](Self::free_within) is called with. The freed slot joins the
/// free list, its generation moves on, and the handle that named it traps from
/// then on. Without this the kernel would be the last strong reference to every
/// node a rendering instance ever built, for the page's life.
pub struct HandleTable<T> {
    slots: Vec<Slot<T>>,
    /// Indices of freed slots, newest first. Reuse keeps the table's length at
    /// the instance's live high-water mark rather than at its lifetime total.
    free: Vec<usize>,
}

/// Derived `Default` would demand `T: Default`, which nothing here needs: an
/// empty table is empty whatever it would hold.
impl<T> Default for HandleTable<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl<T: Clone + PartialEq> HandleTable<T> {
    /// This instance's handle for `held`, minting one if it has none.
    pub fn handle_for(&mut self, held: &T) -> u64 {
        if let Some(handle) = self.lookup(held) {
            return handle;
        }
        self.mint(held)
    }

    /// A fresh handle for a value the caller knows is not already held.
    ///
    /// Skips the canonicality scan — the caller must ensure the value appears
    /// in no table, or a duplicate breaks handle identity.
    pub fn mint(&mut self, held: &T) -> u64 {
        match self.free.pop() {
            Some(index) => {
                // A real assertion, not a debug one: the free list is the only
                // thing saying this slot is vacant, and overwriting a live slot
                // would put two handles on one element — a confinement break,
                // and a silent one. Release builds strip `debug_assert!`, so
                // this is where the capsule gets bitten.
                assert!(
                    self.slots[index].held.is_none(),
                    "dom: the free list named a live slot"
                );
                self.slots[index].held = Some(held.clone());
                pack(index, self.slots[index].generation)
            }
            None => {
                self.slots.push(Slot {
                    held: Some(held.clone()),
                    generation: 1,
                });
                pack(self.slots.len() - 1, 1)
            }
        }
    }

    /// This instance's existing handle for `held`, or `None` if it has never
    /// been named here — or was named and reclaimed.
    pub fn lookup(&self, held: &T) -> Option<u64> {
        self.slots.iter().enumerate().find_map(|(index, slot)| {
            (slot.held.as_ref() == Some(held)).then(|| pack(index, slot.generation))
        })
    }

    /// The value `handle` names, or the refusal that traps the caller.
    ///
    /// The two ways to miss get two messages, because they lay blame in opposite
    /// directions: a handle whose slot moved on may name an element somebody
    /// destroyed — possibly a `page-dom` holder, in which case the trapping
    /// instance did nothing wrong — while a handle that never named a slot here
    /// is the caller's own bug. The first message hedges, because a generation
    /// this table never issued lands there too and is the caller's bug as much
    /// as an out-of-range index is. The error card and the operator's log line
    /// carry this text and, in the browser, are the only signal there is.
    pub fn get(&self, handle: u64) -> Result<T, String> {
        let Some((index, generation)) = unpack(handle) else {
            return Err(unknown_handle(handle));
        };
        let Some(slot) = self.slots.get(index) else {
            return Err(unknown_handle(handle));
        };
        match &slot.held {
            Some(held) if slot.generation == generation => Ok(held.clone()),
            // Either the slot is vacant, or it was reused, or the generation was
            // never issued at all.
            _ => Err(format!(
                "dom: handle {handle} was reclaimed when its element was destroyed, \
                 or never named a live element of this instance"
            )),
        }
    }

    /// Free every live slot whose value `inside` accepts, and `root` itself when
    /// `with_root`.
    ///
    /// The two callers are the two operations that destroy elements: `remove`
    /// takes the root with the subtree, `set-text` replaces the children and
    /// leaves the parent standing. Containment is asked of the caller rather
    /// than tracked here, because the DOM is where the tree actually is.
    pub fn free_within(&mut self, root: &T, with_root: bool, inside: impl Fn(&T) -> bool) {
        for (index, slot) in self.slots.iter_mut().enumerate() {
            let Some(held) = slot.held.as_ref() else {
                continue;
            };
            if !inside(held) {
                continue;
            }
            if !with_root && held == root {
                continue;
            }
            free_slot(slot, index, &mut self.free);
        }
    }

    /// Free the one slot holding `root` itself, if this table holds it.
    ///
    /// The identity-only half of [`free_within`](Self::free_within), for the
    /// case where the DOM says nothing can be under `root`: no containment
    /// question, and the walk stops at the first match because a slot's value is
    /// canonical per table.
    pub fn free_exact(&mut self, root: &T) {
        let found = self
            .slots
            .iter()
            .position(|slot| slot.held.as_ref() == Some(root));
        if let Some(index) = found {
            free_slot(&mut self.slots[index], index, &mut self.free);
        }
    }

    /// How many handles this instance holds live. Test-facing.
    #[cfg(test)]
    pub fn live(&self) -> usize {
        self.slots.iter().filter(|slot| slot.held.is_some()).count()
    }
}

/// Vacate `slot`, move its generation on, and offer the index for reuse.
fn free_slot<T>(slot: &mut Slot<T>, index: usize, free: &mut Vec<usize>) {
    slot.held = None;
    slot.generation = next_generation(slot.generation);
    free.push(index);
}

/// The refusal for a handle that never named a slot in this table.
fn unknown_handle(handle: u64) -> String {
    format!("dom: handle {handle} names no element of this instance")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node of a toy tree: an id, and the ids of everything under it. Enough
    /// to answer the one question the real table asks the DOM.
    #[derive(Clone, PartialEq, Debug)]
    struct Node {
        id: u32,
        under: Vec<u32>,
    }

    fn leaf(id: u32) -> Node {
        Node { id, under: vec![] }
    }

    fn parent(id: u32, under: &[u32]) -> Node {
        Node {
            id,
            under: under.to_vec(),
        }
    }

    /// The containment predicate a real caller supplies from the tree.
    fn inside(root: &Node) -> impl Fn(&Node) -> bool + '_ {
        move |node: &Node| node.id == root.id || root.under.contains(&node.id)
    }

    #[test]
    fn a_handle_is_canonical_per_value_and_is_never_zero() {
        let mut table = HandleTable::default();
        let a = leaf(1);
        let b = leaf(2);
        let first = table.handle_for(&a);
        assert_ne!(first, 0, "zero is never a valid handle");
        assert_eq!(
            table.handle_for(&a),
            first,
            "the same node, the same handle"
        );
        assert_ne!(table.handle_for(&b), first, "two nodes, two handles");
        assert_eq!(table.live(), 2);
        assert_eq!(table.get(first).expect("the node is there"), a);
    }

    #[test]
    fn minting_appends_without_canonicalising() {
        let mut table = HandleTable::default();
        let a = leaf(1);
        let first = table.mint(&a);
        let second = table.mint(&leaf(2));
        assert_ne!(first, second, "two mints, two handles");
        assert_eq!(table.lookup(&a), Some(first), "a minted handle is found");
    }

    #[test]
    fn removing_a_subtree_frees_the_root_and_everything_under_it() {
        let mut table = HandleTable::default();
        let root = parent(1, &[2, 3]);
        let kids = [leaf(2), leaf(3)];
        let root_handle = table.handle_for(&root);
        let kid_handles: Vec<u64> = kids.iter().map(|kid| table.handle_for(kid)).collect();
        let bystander = leaf(9);
        let bystander_handle = table.handle_for(&bystander);

        table.free_within(&root, true, inside(&root));

        assert!(table.get(root_handle).is_err(), "the root is gone");
        for handle in kid_handles {
            assert!(table.get(handle).is_err(), "a descendant is gone");
        }
        assert!(
            table.get(bystander_handle).is_ok(),
            "a node outside the subtree is untouched"
        );
        assert_eq!(table.live(), 1);
    }

    #[test]
    fn clearing_the_text_frees_the_children_and_spares_the_parent() {
        let mut table = HandleTable::default();
        let root = parent(1, &[2, 3]);
        let kids = [leaf(2), leaf(3)];
        let root_handle = table.handle_for(&root);
        let kid_handles: Vec<u64> = kids.iter().map(|kid| table.handle_for(kid)).collect();

        table.free_within(&root, false, inside(&root));

        assert!(
            table.get(root_handle).is_ok(),
            "the node whose text was set survives"
        );
        for handle in kid_handles {
            assert!(table.get(handle).is_err(), "a replaced child is gone");
        }
        assert_eq!(table.live(), 1);
    }

    #[test]
    fn freeing_by_identity_takes_only_the_named_slot() {
        let mut table = HandleTable::default();
        let doomed = leaf(1);
        let sibling = leaf(2);
        let doomed_handle = table.handle_for(&doomed);
        let sibling_handle = table.handle_for(&sibling);

        table.free_exact(&doomed);

        assert!(table.get(doomed_handle).is_err(), "the leaf is gone");
        assert!(table.get(sibling_handle).is_ok(), "its sibling is not");
    }

    #[test]
    fn a_reused_slot_refuses_the_handle_that_held_it() {
        let mut table = HandleTable::default();
        let doomed = leaf(1);
        let stale = table.handle_for(&doomed);
        table.free_exact(&doomed);

        let successor = leaf(2);
        let fresh = table.handle_for(&successor);
        assert_ne!(fresh, stale, "the generation moved with the reuse");
        assert_eq!(table.live(), 1, "the slot was reused, not appended to");
        assert!(
            table.get(stale).is_err(),
            "the stale handle does not alias onto the stranger"
        );
        assert_eq!(table.get(fresh).expect("the successor is there"), successor);
    }

    #[test]
    fn a_mint_and_trim_cycle_returns_the_table_to_its_baseline() {
        // The leak regression, pinned as a live count: echo-stub's shape, a
        // capped scrollback that removes the oldest entry as it appends a new
        // one, run far enough that a non-reclaiming table would be ten times the
        // size of a reclaiming one.
        let mut table = HandleTable::default();
        table.handle_for(&leaf(0));
        let baseline = table.live();

        let mut live: std::collections::VecDeque<Node> = std::collections::VecDeque::new();
        for step in 0..100u32 {
            let entry = parent(step * 2 + 1, &[step * 2 + 2]);
            let label = leaf(step * 2 + 2);
            table.handle_for(&entry);
            table.handle_for(&label);
            live.push_back(entry);
            if live.len() > 10 {
                let oldest = live.pop_front().expect("a capped deque has a front");
                table.free_within(&oldest, true, inside(&oldest));
            }
        }
        assert_eq!(
            table.live(),
            baseline + live.len() * 2,
            "only the on-screen entries and their labels are still held"
        );
    }

    #[test]
    fn a_reclaimed_handle_and_an_unknown_one_lay_blame_differently() {
        // The two refusals reach the same error card, and the removal that made
        // a handle stale may have been another instance's, so the text is the
        // only thing that says whose bug it is.
        let mut table = HandleTable::default();
        let root = parent(1, &[2]);
        let child = table.handle_for(&leaf(2));
        table.free_within(&root, true, inside(&root));

        let stale = table.get(child).expect_err("the child was reclaimed");
        assert!(stale.contains("reclaimed"), "{stale}");
        let unknown = table.get(u64::MAX).expect_err("nothing names that slot");
        assert!(unknown.contains("names no element"), "{unknown}");
    }

    #[test]
    fn an_unknown_handle_is_a_refusal() {
        let mut table = HandleTable::default();
        table.handle_for(&leaf(1));
        assert!(table.get(0).is_err(), "zero names nothing");
        assert!(table.get(2).is_err(), "an unminted handle names nothing");
        assert!(table.get(u64::MAX).is_err());
    }

    #[test]
    fn a_fabricated_generation_over_a_live_slot_is_still_refused() {
        // The index is in range and the slot is live; only the generation is
        // invented. Nothing is handed back.
        let mut table = HandleTable::default();
        let handle = table.handle_for(&leaf(1));
        let (index, generation) = unpack(handle).expect("a minted handle unpacks");
        let fabricated = pack(index, generation.wrapping_add(7));
        assert!(table.get(fabricated).is_err());
    }

    #[test]
    fn one_instances_handles_name_nothing_in_anothers_table() {
        // The whole confinement mechanism: two tables mint the same numbers over
        // different values, so a handle passed between instances resolves to
        // that instance's own node or to nothing at all.
        let mine: HandleTable<Node> = HandleTable::default();
        let mut yours = HandleTable::default();
        let secret = leaf(1);
        let handle = yours.handle_for(&secret);
        assert!(mine.get(handle).is_err());
        assert_eq!(mine.lookup(&secret), None);
    }
}
