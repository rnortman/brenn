//! Device location: a browser-reported position fix per `(device, user)`, the
//! bounds it is validated against, the per-user location channel it is
//! published on, and the `device-location` registry tool that answers the
//! acting user's sharing devices.
//!
//! The PWA reports a fix only while the user shares location from that
//! device; the WebSocket handlers in `brenn-server` validate it with
//! [`validate::validate_fix`] and record it through [`channel::record`] on
//! `brenn:device-location.<username>`. The tool reads that channel's window as
//! host code, without a subscription. No function here logs a coordinate.

use std::sync::{Arc, OnceLock};

use brenn_messaging::Messenger;
use brenn_tool_registry::RegisteredTool;

pub mod channel;
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

/// The tool's handle on the messenger, which is built after the registry:
/// attached once messaging exists, read on every call.
#[derive(Clone, Default)]
pub struct DeviceLocationBus(Arc<OnceLock<Arc<Messenger>>>);

impl DeviceLocationBus {
    /// Attach the messenger the tool reads channels through.
    ///
    /// # Panics
    ///
    /// If a messenger is already attached: there is one per process.
    pub fn attach(&self, messenger: Arc<Messenger>) {
        if self.0.set(messenger).is_err() {
            panic!("BUG: the device-location tool's messenger is attached twice");
        }
    }

    fn messenger(&self) -> &Arc<Messenger> {
        self.0
            .get()
            .expect("BUG: the device-location tool executed before the messenger was attached")
    }
}

/// The tools this crate registers: `device-location`, over `db`, and the
/// handle its messenger is attached through.
pub fn registered(db: brenn_db::Db) -> (Vec<RegisteredTool>, DeviceLocationBus) {
    let bus = DeviceLocationBus::default();
    let tools = vec![RegisteredTool::Async(Arc::new(
        tool::DeviceLocationTool::new(db, bus.clone()),
    ))];
    (tools, bus)
}
