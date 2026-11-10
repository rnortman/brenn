//! The swappable tool registry.
//!
//! Every holder of the registry — the LLM adapter, each bridge, each WASM
//! consumer's host, the tool executor, the reload driver — holds a clone of
//! one [`ToolRegistryCell`] and loads a snapshot per operation, so a reload
//! can install a registry with more tools by swapping the cell.
//!
//! [`ToolRegistryCell::load`] clones one `Arc` out from under a read lock and
//! releases it. The snapshot is owned, so it may be held across an `await`;
//! the lock never is. A call that loaded a snapshot completes on it.

use std::sync::{Arc, RwLock};

use crate::registry::ToolRegistry;

/// The registry every holder reads. Clones share one cell.
#[derive(Clone)]
pub struct ToolRegistryCell(Arc<RwLock<Arc<ToolRegistry>>>);

impl ToolRegistryCell {
    pub fn new(registry: Arc<ToolRegistry>) -> Self {
        Self(Arc::new(RwLock::new(registry)))
    }

    /// The current registry. Cheap: one `Arc` clone under a read lock.
    ///
    /// Panics if a writer panicked while holding the lock: a process whose
    /// registry swap was interrupted is in a state no document describes.
    pub fn load(&self) -> Arc<ToolRegistry> {
        Arc::clone(&self.0.read().expect("BUG: ToolRegistryCell lock poisoned"))
    }

    /// Install `registry`, visible to every subsequent [`Self::load`].
    pub fn store(&self, registry: Arc<ToolRegistry>) {
        *self.0.write().expect("BUG: ToolRegistryCell lock poisoned") = registry;
    }
}

impl From<Arc<ToolRegistry>> for ToolRegistryCell {
    fn from(registry: Arc<ToolRegistry>) -> Self {
        Self::new(registry)
    }
}

impl From<ToolRegistry> for ToolRegistryCell {
    fn from(registry: ToolRegistry) -> Self {
        Self::new(Arc::new(registry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_one_cell_and_see_a_store() {
        let cell = ToolRegistryCell::from(ToolRegistry::new(vec![]));
        let clone = cell.clone();
        let before = cell.load();
        let next = Arc::new(ToolRegistry::new(vec![]));
        clone.store(next.clone());
        assert!(Arc::ptr_eq(&cell.load(), &next));
        assert!(Arc::ptr_eq(&clone.load(), &next));
        assert!(
            !Arc::ptr_eq(&before, &next),
            "a snapshot loaded before the store is the old registry"
        );
    }
}
