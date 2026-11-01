//! Device location: a browser-reported position fix per `(device, user)`, the
//! bounds it is validated against, and the `device-location` registry tool
//! that answers the acting user's sharing devices.
//!
//! The PWA reports a fix only while the user shares location from that
//! device; the WebSocket handlers in `brenn-server` validate it with
//! [`validate::validate_fix`] and store it through [`db`]. The tool reads the
//! store, never the bus. No function here logs a coordinate.

use std::sync::Arc;

use brenn_tool_registry::RegisteredTool;

pub mod db;
mod tool;
pub mod validate;

/// The tool's registry name, as an app's `tool device-location {}` grant names it.
pub const NAME: &str = "device-location";

/// A fix older than this, counted from the server's receipt, is answered as
/// `stale`. The PWA reports every five minutes while visible (`REPORT_INTERVAL_MS`
/// in `frontend/src/location.ts`), with a browser cache age of up to five
/// minutes; fifteen is the first round number past both.
pub const STALE_AFTER_SECONDS: i64 = 900;

/// What the answer tells the assistant when no device shares location.
pub const HOW_TO_ENABLE: &str = "No device shares location. The user can turn it on from the user \
                                 menu in the app: \"Share location from this device\".";

/// The tools this crate registers: `device-location`, over `db`.
pub fn registered(db: brenn_db::Db) -> Vec<RegisteredTool> {
    vec![RegisteredTool::Async(Arc::new(
        tool::DeviceLocationTool::new(db),
    ))]
}
