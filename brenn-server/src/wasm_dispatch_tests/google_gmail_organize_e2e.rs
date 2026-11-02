//! The `google-gmail-organize` component, end to end, on the rig in
//! [`super::google_workspace_rig`].

use brenn_tool_registry::ToolError;
use serde_json::json;

use super::google_workspace_rig::{
    self as workspace, ACCOUNT, BEARER, Clauses, GMAIL_ORGANIZE, google_query,
    insufficient_scope_body, scope_denied,
};
use super::stub_rig::{StubRig, assert_could_not_be_issued};

const BATCH_MODIFY: &str = "/gmail/v1/users/me/messages/batchModify";

/// The instance's `api-request` clause: the one endpoint the tool calls.
const CLAUSES: Clauses<'static> = &[("POST", BATCH_MODIFY)];

/// The rig with every clause and alice's one account connected.
async fn rig() -> StubRig {
    workspace::rig(&GMAIL_ORGANIZE, CLAUSES).await
}

/// The rig with `clauses` and alice's one account connected.
async fn rig_with(clauses: Clauses<'_>) -> StubRig {
    workspace::rig_with(&GMAIL_ORGANIZE, clauses, &[(ACCOUNT, BEARER)], None).await
}

/// What `GmailModifyLabels` is called with in these cases: two messages
/// gain `Label_7` and leave the inbox.
fn modify_args() -> serde_json::Value {
    json!({ "ids": ["m1", "m2"], "add": ["Label_7"], "remove": ["INBOX"] })
}

/// `GmailModifyLabels` changes the labels with one `POST` to batchModify
/// carrying alice's bearer and the ids and label lists; Google's 204 with
/// no body is the change accepted.
#[tokio::test]
async fn gmail_modify_labels_posts_one_batch_modify_and_a_204_is_success() {
    let r = rig().await;
    r.respond(BATCH_MODIFY, 204, Vec::new());
    let call = r.call_for_alice("GmailModifyLabels", modify_args());
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({ "accepted": true, "ids": 2, "add": ["Label_7"], "remove": ["INBOX"] }))
    );
    assert_eq!(
        r.seen(),
        [(
            "POST".to_string(),
            BATCH_MODIFY.to_string(),
            google_query(&[])
        )]
    );
    assert_eq!(r.bearers(), [format!("Bearer {BEARER}")]);
    let bodies = r.bodies();
    assert_eq!(bodies.len(), 1, "exactly one POST carried a body");
    let request: serde_json::Value = serde_json::from_slice(&bodies[0]).unwrap();
    assert_eq!(
        request,
        json!({ "ids": ["m1", "m2"], "addLabelIds": ["Label_7"], "removeLabelIds": ["INBOX"] })
    );
    r.assert_finished(&id);
}

/// A 2xx carrying a JSON body is the change accepted as well: Google's
/// acceptance, whatever it answered with.
#[tokio::test]
async fn a_batch_modify_answered_with_json_is_success() {
    let r = rig().await;
    r.answer(BATCH_MODIFY, "{}");
    let call = r.call_for_alice(
        "GmailModifyLabels",
        json!({ "ids": ["m1"], "remove": ["UNREAD"] }),
    );
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(
        call.await.unwrap(),
        Ok(json!({ "accepted": true, "ids": 1, "remove": ["UNREAD"] }))
    );
    r.assert_finished(&id);
}

/// Google's 403 on the batchModify is `denied`, telling the assistant to
/// reconnect with the Gmail organize scope.
#[tokio::test]
async fn a_modify_labels_403_names_the_organize_scope() {
    let r = rig().await;
    r.respond(BATCH_MODIFY, 403, insufficient_scope_body());
    let call = r.call_for_alice("GmailModifyLabels", modify_args());
    let id = r.requested_id().await;
    r.run(1).await;

    assert_eq!(call.await.unwrap(), scope_denied("Gmail organize"));
    r.assert_finished(&id);
}

/// Without the batchModify clause the `POST` is refused when it is queued,
/// even though another Gmail `POST` clause stands (a grant holds at least
/// one): the request is answered `internal` with Brenn's text and the clause
/// guidance.
#[tokio::test]
async fn a_batch_modify_post_without_its_clause_could_not_be_issued() {
    let r = rig_with(&[("POST", "/gmail/v1/users/me/drafts")]).await;
    let call = r.call_for_alice("GmailModifyLabels", modify_args());
    let id = r.requested_id().await;
    r.serve().await;

    assert_could_not_be_issued(call.await.unwrap());
    r.owed(&r.executor_sub(), 0).await;
    assert!(r.seen().is_empty(), "a refused call reaches no API");
    r.assert_finished(&id);
}

/// A label change naming no label, or one label both to add and to
/// remove, is refused `invalid_args` before any call is issued.
#[tokio::test]
async fn gmail_modify_labels_refuses_bad_arguments_before_any_call() {
    for (args, refusal) in [
        (
            json!({ "ids": ["m1"] }),
            "`add` or `remove` must name a label",
        ),
        (
            json!({ "ids": ["m1"], "add": ["INBOX"], "remove": ["INBOX"] }),
            "`add` and `remove` both name `INBOX`",
        ),
        (
            json!({ "ids": ["m1"], "add": ["TRASH"] }),
            "`add` may not name `TRASH`: these tools never move mail to Trash or Spam",
        ),
    ] {
        let r = rig().await;
        let call = r.call_for_alice("GmailModifyLabels", args.clone());
        let id = r.requested_id().await;
        r.serve().await;

        assert_eq!(
            call.await.unwrap(),
            Err(ToolError::InvalidArgs(refusal.to_string())),
            "{args}"
        );
        r.owed(&r.executor_sub(), 0).await;
        assert!(
            r.seen().is_empty(),
            "{args}: a refused change reaches no API"
        );
        r.assert_finished(&id);
    }
}
