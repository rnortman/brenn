//! Shared by the in-tree Google Workspace components; not on the build-time
//! contract list in `docs/component-packages.md`. If the components move out
//! of tree, this crate moves with them.
//!
//! Std, `serde_json`, `brenn-cal` and `brenn-provider-kit` only: nothing here
//! names a component's generated `Provided`, so every item runs on the host
//! under test and links into each component's guest build alike.

pub mod args;
pub mod calendar;
pub mod call;
pub mod gmail;
pub mod scope;
pub mod settings;
#[cfg(any(test, feature = "testutils"))]
pub mod testutil;
pub mod text;
