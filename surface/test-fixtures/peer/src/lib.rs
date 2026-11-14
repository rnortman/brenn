//! The scripted attach peer's side of a fixture page, shared by the kernel's
//! suites and the page rig.
//!
//! [`frames`] are the server frames a scripted peer writes and [`bindings`] the
//! bindings documents a fixture page is wired with. [`page_rig`] is the page
//! rig's kit, built from them. They sit apart from `brenn_surface_test_fixtures`
//! because they need the attachment client and protocol, which the component
//! suites that use that crate have no reason to link.

pub mod bindings;
pub mod frames;
pub mod page_rig;
