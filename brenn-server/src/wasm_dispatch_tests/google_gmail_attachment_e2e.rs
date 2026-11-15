//! `GmailAttachment`, end to end, on the rig in [`super::google_workspace_rig`]:
//! the compiled `google-gmail-read` component reads the part tree, asks
//! `api-request` for Gmail's answer as an attachment, decodes it by range
//! through the real guest adapter into a new blob, and completes with it. The
//! rig's blob store has production limits and the component runs under the
//! production fuel rule unless a case says otherwise.

use base64ct::{Base64UrlUnpadded, Encoding};
use brenn_envelope::AttachmentName;
use brenn_messaging_store::blob::{BlobLimits, BlobUsage};
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use serde_json::{Value, json};

use brenn_lib::messaging::app_tool_result_inbox_address;
use brenn_messaging::testutils::{publish_attached, redeem_all, seal_blob};
use uuid::Uuid;

use super::google_workspace_rig::{
    self as workspace, APP, Clauses, GMAIL_READ, PRODUCER, RigOptions, SRC, google_query,
};
use super::stub_rig::{StubRig, file_bytes, production_blob_limits};
use super::*;

const MESSAGES: &str = "/gmail/v1/users/me/messages";
/// The instance's result inbox, where `api-request`'s answers rest.
const PROVIDER_RESULTS: &str = "brenn:tool-results/gmail-read";
/// The name of the completion's attachment.
const FILE: &str = "attachment";
/// `GmailAttachment`'s largest attachment, in bytes.
const MAX_ATTACHMENT_BYTES: u64 = 50_000_000;

/// The clauses `GmailRead` and `GmailAttachment` need, and the upload a
/// conversation sends a file to.
const CLAUSES: Clauses<'static> = &[
    ("GET", "/gmail/v1/users/me/messages/*"),
    ("GET", "/gmail/v1/users/me/messages/*/attachments/*"),
    ("POST", UPLOAD),
];
/// Gmail's media upload of a whole message.
const UPLOAD: &str = "/upload/gmail/v1/users/me/messages";

async fn rig() -> StubRig {
    workspace::rig(&GMAIL_READ, CLAUSES).await
}

/// The rig with app [`APP`]'s conversations.
async fn conversation_rig() -> StubRig {
    workspace::rig_built(
        &GMAIL_READ,
        CLAUSES,
        RigOptions {
            conversations: true,
            ..RigOptions::default()
        },
    )
    .await
}

/// Gmail's `messages.attachments.get` answer for `bytes`: its size and its
/// data in base64url, unpadded.
fn encoded(bytes: &[u8]) -> Vec<u8> {
    format!(
        r#"{{"size":{},"data":"{}"}}"#,
        bytes.len(),
        Base64UrlUnpadded::encode_string(bytes)
    )
    .into_bytes()
}

/// Message `id` as `messages.get` answers it: a plain-text part beside a PDF
/// attachment `part_id` of `size` bytes, served as `attachment_id`, that sits
/// `depth` levels below the payload (1 is a child of the payload).
fn message(id: &str, depth: usize, part_id: &str, attachment_id: &str, size: u64) -> String {
    assert!(depth >= 1, "an attachment is below the payload");
    let mut part = json!({
        "partId": part_id,
        "mimeType": "application/pdf",
        "filename": "scan.pdf",
        "headers": [
            { "name": "Content-Disposition", "value": "attachment; filename=\"scan.pdf\"" },
        ],
        "body": { "size": size, "attachmentId": attachment_id },
    });
    for level in (1..depth).rev() {
        part = json!({
            "partId": format!("w{level}"),
            "mimeType": "multipart/mixed",
            "filename": "",
            "body": { "size": 0 },
            "parts": [part],
        });
    }
    json!({
        "id": id,
        "threadId": format!("t-{id}"),
        "labelIds": ["INBOX"],
        "snippet": "hi",
        "sizeEstimate": 4096,
        "internalDate": "1793455509000",
        "payload": {
            "partId": "",
            "mimeType": "multipart/mixed",
            "filename": "",
            "headers": [
                { "name": "From", "value": "Alice Example <alice@example.com>" },
                { "name": "Subject", "value": "A scan" },
                { "name": "Date", "value": "Sat, 31 Oct 2026 14:05:09 +0000" },
            ],
            "body": { "size": 0 },
            "parts": [
                {
                    "partId": "0",
                    "mimeType": "text/plain",
                    "filename": "",
                    "body": { "size": 3, "data": "aGkK" },
                },
                part,
            ],
        },
    })
    .to_string()
}

/// The path `GmailAttachment`'s fetch of `attachment_id` of `id` reaches.
fn attachment_path(id: &str, attachment_id: &str) -> String {
    format!("{MESSAGES}/{id}/attachments/{attachment_id}")
}

/// Serve message `id`, its PDF of `bytes` at `depth` as part `part_id`, and
/// the fetch of that PDF.
fn serve_message(r: &StubRig, id: &str, depth: usize, part_id: &str, bytes: &[u8]) -> Vec<u8> {
    let attachment_id = format!("ATT-{id}");
    r.answer(
        &format!("{MESSAGES}/{id}"),
        message(id, depth, part_id, &attachment_id, bytes.len() as u64),
    );
    let answer = encoded(bytes);
    r.answer(&attachment_path(id, &attachment_id), answer.clone());
    answer
}

/// `GmailRead`'s listing of `id`'s attachments.
async fn listed(r: &StubRig, id: &str) -> Value {
    let call = r.call_for_alice("GmailRead", json!({ "id": id }));
    let request = r.requested_id().await;
    r.run(1).await;
    let answer = call.await.unwrap().expect("GmailRead answered");
    r.assert_finished(&request);
    answer["attachments"].clone()
}

/// Call `GmailAttachment` for part `part_id` of `id`, running `steps` nested
/// calls; the whole output.
async fn fetch(
    r: &StubRig,
    id: &str,
    part_id: &str,
    steps: usize,
) -> Result<ToolOutput, ToolError> {
    r.call_running(
        "GmailAttachment",
        json!({ "id": id, "part_id": part_id }),
        HeldAttachments::none(),
        steps,
    )
    .await
}

/// The completion's attachment: its content type and every byte.
fn file_of(output: &ToolOutput) -> (String, Vec<u8>) {
    let name = AttachmentName::new(FILE).unwrap();
    assert_eq!(
        output.attachments.refs().iter().count(),
        1,
        "one attachment: {:?}",
        output.attachments.refs()
    );
    let hold = output
        .attachments
        .hold(&name)
        .unwrap_or_else(|| panic!("the answer carries `{FILE}`"));
    let size = usize::try_from(hold.reference().size).unwrap();
    let bytes = hold.read(0, size).expect("the attachment reads");
    (hold.reference().content_type.clone(), bytes)
}

/// The attachment ids by name of each row on the instance's result inbox.
async fn result_rows(r: &StubRig) -> Vec<std::collections::BTreeMap<String, String>> {
    testutils::retained_on(&r.messenger, PROVIDER_RESULTS)
        .await
        .iter()
        .map(|(_, envelope)| testutils::attachment_ids(envelope))
        .collect()
}

/// The `fields` mask of the part-tree read: the part's fields, nested to
/// eight levels of `parts` below the payload, no `data`.
fn assert_part_tree_mask(fields: &str) {
    assert!(
        fields.starts_with("payload(partId,mimeType,filename,body(size,attachmentId),parts("),
        "{fields}"
    );
    assert_eq!(fields.matches("parts(").count(), 8, "{fields}");
    assert!(!fields.contains("data"), "{fields}");
}

/// `GmailRead` lists the PDF with `part_id`; `GmailAttachment` reads the part
/// tree, fetches Gmail's base64url JSON as an attachment of `api-request`'s
/// result, and answers the 3 MiB file decoded, byte for byte, in the part's
/// type. The encoded answer rests on the instance's result inbox; the decoded
/// file is the caller's.
#[tokio::test(flavor = "multi_thread")]
async fn a_listed_part_is_fetched_decoded() {
    let r = rig().await;
    let bytes = file_bytes(3 << 20);
    let answer = serve_message(&r, "m1", 1, "1", &bytes);

    let attachments = listed(&r, "m1").await;
    assert_eq!(attachments[0]["part_id"], "1", "{attachments}");
    assert_eq!(attachments[0]["size"], bytes.len(), "{attachments}");
    let before = r.blob_usage();
    let seen_before = r.seen().len();

    let output = fetch(&r, "m1", "1", 2)
        .await
        .expect("GmailAttachment answered");
    assert_eq!(
        output.value,
        json!({
            "id": "m1",
            "part_id": "1",
            "filename": "scan.pdf",
            "mime_type": "application/pdf",
            "size": bytes.len(),
            "attachment": FILE,
        })
    );
    let (content_type, got) = file_of(&output);
    assert_eq!(content_type, "application/pdf");
    assert!(got == bytes, "the decoded file is the stub's bytes");

    let seen = &r.seen()[seen_before..];
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[0].0, "GET");
    assert_eq!(seen[0].1, format!("{MESSAGES}/m1"));
    let fields = seen[0]
        .2
        .iter()
        .find(|(key, _)| key == "fields")
        .map(|(_, value)| value.clone())
        .expect("the part-tree read sends a mask");
    assert_part_tree_mask(&fields);
    assert_eq!(
        seen[0].2,
        google_query(&[("fields", &fields), ("format", "full")])
    );
    assert_eq!(
        (seen[1].0.as_str(), seen[1].1.as_str(), seen[1].2.clone()),
        (
            "GET",
            attachment_path("m1", "ATT-m1").as_str(),
            google_query(&[])
        )
    );

    let rows = result_rows(&r).await;
    let with_blobs: Vec<_> = rows.iter().filter(|row| !row.is_empty()).collect();
    assert_eq!(with_blobs.len(), 1, "{rows:?}");
    assert_eq!(with_blobs[0].len(), 1, "{rows:?}");
    let encoded_and_decoded = (answer.len() + bytes.len()) as u64;
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: before.sealed + 2,
            unsealed: 0,
            charged_bytes: before.charged_bytes + encoded_and_decoded,
        }
    );
    drop(output);
    assert_eq!(
        r.blob_usage().charged_bytes,
        before.charged_bytes + answer.len() as u64,
        "the encoded answer still rests on the result inbox"
    );
}

/// An attachment eight levels below the payload is listed with a `part_id`
/// and fetched; the same attachment nine levels down is listed without one,
/// and naming its `partId` is refused at the part-tree read.
#[tokio::test(flavor = "multi_thread")]
async fn eight_levels_deep_is_named_and_fetched_and_nine_is_not() {
    let r = rig().await;
    let bytes = file_bytes(5_000);
    serve_message(&r, "m8", 8, "deep", &bytes);
    serve_message(&r, "m9", 9, "deeper", &bytes);

    let attachments = listed(&r, "m8").await;
    assert_eq!(attachments[0]["part_id"], "deep", "{attachments}");
    let output = fetch(&r, "m8", "deep", 2).await.expect("fetched");
    assert!(file_of(&output).1 == bytes);

    let attachments = listed(&r, "m9").await;
    assert_eq!(
        attachments.as_array().map(Vec::len),
        Some(1),
        "{attachments}"
    );
    assert!(
        attachments[0].get("part_id").is_none(),
        "nine levels down is listed unnamed: {attachments}"
    );
    let seen = r.seen().len();
    let refused = fetch(&r, "m9", "deeper", 1).await;
    let Err(ToolError::InvalidArgs(detail)) = refused else {
        panic!("expected invalid_args, got {:?}", refused.map(|o| o.value));
    };
    assert_eq!(
        detail,
        "message `m9` has no part `deeper` within 8 levels of its payload, the depth \
         GmailAttachment reads; use a `part_id` GmailRead lists"
    );
    assert_eq!(r.seen().len(), seen + 1, "only the part tree was read");
}

/// The attachment fuel cap does not bite below one blob of the default
/// largest size, so the encoded answer `GmailAttachment` decodes, which that
/// size bounds, gets the full rate.
#[test]
fn one_default_largest_blob_gets_the_full_attachment_fuel_rate() {
    assert!(
        production_blob_limits().max_blob_bytes * brenn_wasm::PROCESSOR_FUEL_PER_ATTACHMENT_BYTE
            <= brenn_wasm::PROCESSOR_MAX_ATTACHMENT_FUEL
    );
}

/// The largest attachment `GmailAttachment` returns, 50,000,000 bytes, under
/// production blob limits and the production fuel rule: its encoded answer
/// fits the default `max_blob_bytes`, the decode finishes inside the fuel the
/// attachment bytes add, and the two blobs are what the store holds after —
/// the encoded one on the instance's result inbox, the decoded one the
/// caller's. One byte more is refused at the part-tree read, before any fetch.
#[tokio::test(flavor = "multi_thread")]
async fn the_largest_attachment_is_fetched_and_one_byte_more_is_refused() {
    let r = rig().await;
    let bytes = file_bytes(MAX_ATTACHMENT_BYTES as usize);
    let answer = serve_message(&r, "big", 1, "1", &bytes);
    assert!(
        (answer.len() as u64) <= production_blob_limits().max_blob_bytes,
        "the encoded answer fits the default largest blob"
    );
    let before = r.blob_usage();

    let output = fetch(&r, "big", "1", 2).await.expect("fetched");
    assert_eq!(output.value["size"], MAX_ATTACHMENT_BYTES);
    assert!(file_of(&output).1 == bytes, "every byte decoded");
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: before.sealed + 2,
            unsealed: 0,
            charged_bytes: before.charged_bytes + answer.len() as u64 + MAX_ATTACHMENT_BYTES,
        }
    );
    let rows = result_rows(&r).await;
    assert_eq!(rows.iter().filter(|row| !row.is_empty()).count(), 1);
    drop(output);

    r.answer(
        &format!("{MESSAGES}/over"),
        message("over", 1, "1", "ATT-over", MAX_ATTACHMENT_BYTES + 1),
    );
    let seen = r.seen().len();
    let refused = fetch(&r, "over", "1", 1).await;
    let Err(ToolError::InvalidArgs(detail)) = refused else {
        panic!("expected invalid_args, got {:?}", refused.map(|o| o.value));
    };
    assert_eq!(
        detail,
        "attachment of 50000001 bytes exceeds the 50000000 bytes GmailAttachment returns"
    );
    assert_eq!(r.seen().len(), seen + 1, "nothing was fetched");
}

/// A host refusal through the real guest adapter: the store's budget holds
/// the encoded answer and not the decoded file beside it, so a write of the
/// decode is refused for the pool. `GmailAttachment` answers `internal`
/// naming the blob store, completes no attachment, and the store holds the
/// encoded answer alone afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_store_refuses_the_decode_and_leaves_only_the_answer() {
    let bytes = file_bytes(3 << 20);
    let answer_len = encoded(&bytes).len() as u64;
    let r = workspace::rig_built(
        &GMAIL_READ,
        CLAUSES,
        RigOptions {
            blob_limits: Some(BlobLimits {
                max_blob_bytes: production_blob_limits().max_blob_bytes,
                budget_bytes: answer_len + (1 << 20),
            }),
            ..RigOptions::default()
        },
    )
    .await;
    serve_message(&r, "m1", 1, "1", &bytes);
    assert_eq!(r.blob_usage().charged_bytes, 0);

    let refused = fetch(&r, "m1", "1", 2).await;
    let Err(ToolError::Internal(detail)) = refused else {
        panic!("expected internal, got {:?}", refused.map(|o| o.value));
    };
    assert!(
        detail.contains("the host's blob store is full (blob_budget_bytes)"),
        "{detail}"
    );
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: 1,
            unsealed: 0,
            charged_bytes: answer_len,
        },
        "the decoded file's writer was dropped; the encoded answer rests"
    );
    let rows = result_rows(&r).await;
    assert_eq!(rows.iter().filter(|row| !row.is_empty()).count(), 1);
}

/// The blob `answer` hands a conversation as `name`, read whole from the
/// app's result inbox message the answer names.
async fn handed(r: &StubRig, answer: &Value, name: &str) -> (String, Vec<u8>) {
    let message: Uuid = answer["message_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a handed result: {answer}"))
        .parse()
        .expect("a uuid");
    let rows = testutils::retained_on(&r.messenger, &app_tool_result_inbox_address(APP)).await;
    let (_, envelope) = rows
        .iter()
        .find(|(_, envelope)| envelope.message_id == message)
        .unwrap_or_else(|| panic!("{message} is on the app's result inbox"));
    let id = testutils::attachment_ids(envelope)
        .remove(name)
        .unwrap_or_else(|| panic!("the message carries `{name}`"));
    assert!(
        !answer.to_string().contains(&id),
        "the conversation is shown no blob id: {answer}"
    );
    let bytes = redeem_all(&r.messenger, &brenn_envelope::BlobId::new(id.clone()))
        .await
        .expect("the handed blob redeems");
    (id, bytes)
}

/// `GmailAttachment` called by a conversation on the hook path: the answer is
/// `{"result", "message_id", "attachments"}`, the attachment shown by size
/// and type with no blob id, and the blob on the app's result inbox holds the
/// decoded file.
// TODO(gmail-attachment-real-bridge-run): the same call through a real bridge
// and `brenn-tool save`, to the bytes on disk.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversation_is_handed_the_file_without_a_blob_id() {
    let r = conversation_rig().await;
    let bytes = file_bytes(5_000);
    serve_message(&r, "m1", 1, "1", &bytes);
    let conversation = r.conversation(APP, "alice").await;

    let call = {
        let conversation = conversation.clone();
        tokio::spawn(async move {
            conversation
                .test_post_tool_use("GmailAttachment", json!({ "id": "m1", "part_id": "1" }))
                .await
        })
    };
    let request = r.requested_id().await;
    r.run(2).await;
    let answer = call.await.unwrap();
    r.assert_finished(&request);

    let keys: Vec<&String> = answer.as_object().expect("an object").keys().collect();
    assert_eq!(keys, ["attachments", "message_id", "result"], "{answer}");
    assert_eq!(
        answer["result"],
        json!({
            "id": "m1",
            "part_id": "1",
            "filename": "scan.pdf",
            "mime_type": "application/pdf",
            "size": 5_000,
            "attachment": FILE,
        })
    );
    assert_eq!(
        answer["attachments"],
        json!({ FILE: { "size": 5_000, "content_type": "application/pdf" } })
    );
    let (_, got) = handed(&r, &answer, FILE).await;
    assert!(got == bytes, "the handed blob is the decoded file");
}

/// A conversation calling `ApiRequest` itself: `response_attachment` answers
/// the standard shape with the body as the named attachment, and
/// `body_attachment` naming an attachment of a message the conversation was
/// shown sends its bytes to the API exactly.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversation_moves_files_through_api_request() {
    let r = conversation_rig().await;
    let conversation = r.conversation(APP, "alice").await;

    let download = file_bytes(300_000);
    let path = attachment_path("m1", "ATT-raw");
    r.answer(&path, download.clone());
    let answer = conversation
        .test_post_tool_use(
            "ApiRequest",
            json!({
                "provider": "google",
                "method": "GET",
                "host": r.host,
                "path": path,
                "response_attachment": "raw",
            }),
        )
        .await;
    assert_eq!(answer["result"]["status"], 200, "{answer}");
    assert_eq!(answer["result"]["bytes"], 300_000, "{answer}");
    assert_eq!(answer["result"]["attachment"], "raw", "{answer}");
    assert_eq!(answer["attachments"]["raw"]["size"], 300_000, "{answer}");
    let (_, got) = handed(&r, &answer, "raw").await;
    assert!(got == download, "the handed blob is the API's body");

    let upload = file_bytes(200_000);
    let hold = seal_blob(&r.messenger, &upload);
    let shown = publish_attached(
        &r.messenger,
        PRODUCER,
        SRC,
        "a message to import",
        &[("mail.eml", &hold)],
        None,
    )
    .await;
    drop(hold);
    conversation
        .test_post_tool_use("MessageChannelGet", json!({ "address": SRC, "limit": 50 }))
        .await;
    r.answer(UPLOAD, json!({ "id": "m-new" }).to_string());
    let answer = conversation
        .test_post_tool_use(
            "ApiRequest",
            json!({
                "provider": "google",
                "method": "POST",
                "host": r.host,
                "path": UPLOAD,
                "query": { "uploadType": "media" },
                "headers": { "Content-Type": "message/rfc822" },
                "body_attachment": "mail",
                "attachments": {
                    "mail": { "message_id": shown.to_string(), "name": "mail.eml" },
                },
            }),
        )
        .await;
    assert_eq!(answer["status"], 200, "{answer}");
    assert!(
        r.bodies().last() == Some(&upload),
        "the API received the shown attachment's bytes exactly"
    );
}
