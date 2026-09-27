//! Boot seeding shared by the system participants that read state channels.

use std::sync::Arc;

use brenn_messaging::Messenger;
use brenn_messaging::system::SystemInbox;
use tokio::sync::Notify;

/// Attach the system participant `component` and hand the latest retained
/// message on each of its channels to `apply(address, body)`.
///
/// The retained state is learned by a *read*, not by delivery: a system
/// subscriber's position is durable, so after the first boot the retained
/// message is behind the cursor and never arrives as new. The returned inbox is
/// the one that did the read, so the drain task built on it resumes from the
/// position attached here. An empty channel calls nothing.
pub(crate) async fn attach_and_seed(
    component: &'static str,
    messenger: Arc<Messenger>,
    notify: Arc<Notify>,
    mut apply: impl FnMut(&str, &str),
) -> SystemInbox {
    let inbox = SystemInbox::new(component, messenger, notify);
    inbox.attach().await;
    for (address, window) in inbox.snapshot().await {
        // Newest last, new or context alike: the channel carries state, so only
        // the latest message means anything.
        if let Some((_, envelope)) = window.entries.last() {
            apply(&address, &envelope.body);
        }
    }
    inbox
}
