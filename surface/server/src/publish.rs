//! The one publish loop the surface substrate's documents travel through.
//!
//! Bindings documents and description documents are published under two
//! different reserved system identities, but the loop is the same: address by
//! address, in the order the builder produced them, stopping at the first
//! publish that is not `Ok`. Written once so the two paths cannot drift into
//! publishing in different orders or reporting different halves of a failure.

use brenn_lib::messaging::Urgency;
use brenn_messaging::{Messenger, PublishResult};

/// Publish every document under one reserved system identity, reporting the
/// first address that did not succeed.
pub(crate) async fn try_publish_from_system(
    messenger: &Messenger,
    component: &str,
    docs: &[(String, String)],
) -> Result<(), (String, PublishResult)> {
    for (address, body) in docs {
        let result = messenger
            .publish_from_system(component, address, body, Urgency::Normal, None)
            .await;
        match result {
            PublishResult::Ok { .. } => {}
            other => return Err((address.clone(), other)),
        }
    }
    Ok(())
}

/// The boot form: the same loop, with a failure ending the process.
///
/// `what` names the document family in the message, and `remedy` writes the
/// operator's way out of an oversize body, which is the one arm an operator can
/// reach — `max_body_bytes` is theirs to set and these documents grow with the
/// configuration.
///
/// # Panics
///
/// On any publish that does not return `Ok`.
pub(crate) async fn publish_from_system_or_panic(
    messenger: &Messenger,
    component: &str,
    docs: &[(String, String)],
    what: &str,
    remedy: fn(usize) -> String,
) {
    let Err((address, result)) = try_publish_from_system(messenger, component, docs).await else {
        return;
    };
    match result {
        PublishResult::BodyTooLarge { len, max } => panic!(
            "boot: {what} publish to {address:?} rejected — the document is {len} bytes but \
             [messaging] max_body_bytes is {max}. {} Refusing to start (fail-fast on invalid \
             config).",
            remedy(len),
        ),
        other => panic!(
            "boot: {what} publish to {address:?} did not succeed ({other:?}) — the reserved \
             system publisher's policy and the boot-validated channels make this unreachable, so \
             a failure is a host bug. Refusing to start."
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use brenn_lib::messaging::{ChannelScheme, MessagingDirectory, MessagingGlobalConfig};
    use brenn_messaging::system::{SystemParticipantSpec, registrations_from_specs};

    use super::*;
    use crate::test_fixtures::{TEST_ORIGIN, brenn_channel_entry, declare_channels};

    /// The publisher these documents travel under, granted exactly the three
    /// fixture addresses.
    const COMPONENT: &str = "surface-config";

    /// Three declared `brenn:` channels and a messenger whose system publisher
    /// may write all three, under `max_body_bytes`.
    async fn rig(
        db: &brenn_db::Db,
        bares: &[String],
        max_body_bytes: usize,
    ) -> (Arc<Messenger>, Vec<uuid::Uuid>) {
        let uuids: Vec<uuid::Uuid> = bares.iter().map(|_| uuid::Uuid::new_v4()).collect();
        let entries: Vec<_> = bares
            .iter()
            .zip(&uuids)
            .map(|(bare, uuid)| brenn_channel_entry(bare, *uuid))
            .collect();
        let stores = declare_channels(db, &entries).await;
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(brenn_messaging::query::NoopWakeRouter),
            MessagingGlobalConfig {
                max_body_bytes,
                ..Default::default()
            },
        );
        let spec = SystemParticipantSpec::publish_only(COMPONENT, ChannelScheme::Brenn, bares);
        let messenger = messenger
            .with_subscriber_registrations(registrations_from_specs(&[spec]))
            .with_ring_stores(stores);
        (messenger, uuids)
    }

    /// How many rows a channel persisted, which is what says a document in the
    /// list was published rather than skipped.
    async fn rows_on(db: &brenn_db::Db, channel_uuid: uuid::Uuid) -> i64 {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn addresses(bares: &[String]) -> Vec<String> {
        bares.iter().map(|bare| format!("brenn:{bare}")).collect()
    }

    /// Every document lands, in order, and the loop reports success.
    #[tokio::test]
    async fn every_document_that_fits_is_published() {
        let db = brenn_messaging_store::db::init_db_memory();
        let bares: Vec<String> = ["one", "two", "three"].map(String::from).to_vec();
        let (messenger, uuids) = rig(&db, &bares, 4_096).await;
        let docs: Vec<(String, String)> = addresses(&bares)
            .into_iter()
            .map(|address| (address, "body".to_string()))
            .collect();

        try_publish_from_system(&messenger, COMPONENT, &docs)
            .await
            .expect("three small bodies fit");

        for uuid in uuids {
            assert_eq!(rows_on(&db, uuid).await, 1);
        }
    }

    /// The contract the boot wrapper is built on: stop at the *first* document
    /// that does not succeed, name that address, and publish nothing after it.
    /// A caller that reported the wrong address would send an operator to the
    /// wrong surface.
    #[tokio::test]
    async fn the_first_oversize_document_is_the_one_reported_and_the_rest_are_not_published() {
        let db = brenn_messaging_store::db::init_db_memory();
        let bares: Vec<String> = ["one", "two", "three"].map(String::from).to_vec();
        let (messenger, uuids) = rig(&db, &bares, 16).await;
        let addresses = addresses(&bares);
        let docs = vec![
            (addresses[0].clone(), "small".to_string()),
            (addresses[1].clone(), "x".repeat(64)),
            (addresses[2].clone(), "small".to_string()),
        ];

        let (address, result) = try_publish_from_system(&messenger, COMPONENT, &docs)
            .await
            .expect_err("the second body is over the cap");

        assert_eq!(address, addresses[1]);
        assert!(
            matches!(result, PublishResult::BodyTooLarge { .. }),
            "{result:?}"
        );
        assert_eq!(rows_on(&db, uuids[0]).await, 1, "the first one landed");
        assert_eq!(rows_on(&db, uuids[1]).await, 0, "the oversize one did not");
        assert_eq!(
            rows_on(&db, uuids[2]).await,
            0,
            "the loop stopped at the failure rather than publishing past it"
        );
    }
}
