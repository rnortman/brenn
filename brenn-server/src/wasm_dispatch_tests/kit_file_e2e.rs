//! Files through a provider-kit component, end to end: the compiled
//! `processor-kit-file-test` fixture, served by the kit's `serve_with_blobs`,
//! forwards a caller's attachment as a nested `api-request`'s body and an
//! `api-request` answer's blob as its own completion's attachment, through
//! the real guest adapter. The fixture's instance is lowered from its
//! specification onto the rig the Google suites share: its `config` names an
//! OAuth provider and one host, which is all that rig assumes.

use brenn_envelope::AttachmentName;
use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_messaging::MountDebt;
use brenn_messaging::testutils::owed_everywhere;
use brenn_messaging_store::blob::BlobUsage;
use brenn_messaging_store::testutils::seal_bytes;
use brenn_oauth::testutil::ApiSeen;
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::google_workspace_rig::{self as workspace, Clauses, GoogleClass};
use super::stub_rig::{StubRig, file_bytes};

const KIT_FILE: GoogleClass = GoogleClass {
    wasm: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/target/components/brenn_processor_kit_file_test.wasm"
    ),
    spec: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../brenn-wasm/components/processor-kit-file-test/spec/processor-kit-file-test.brenn"
    ),
    class: "ProcessorKitFileTest",
    slug: "kit-file",
    host_key: "host",
    grants: "log, store, config, tools, provides, blobs",
};

/// Where files are put and got.
const FILES: &str = "/files/report.pdf";
/// The attachment the fixture sends and answers.
const FILE: &str = "file";
/// The size of the files moved each way.
const SIZE: usize = 3 << 20;

const CLAUSES: Clauses<'static> = &[("PUT", FILES), ("HEAD", FILES), ("GET", FILES)];

async fn rig() -> StubRig {
    workspace::rig(&KIT_FILE, CLAUSES).await
}

/// Put a 3 MiB PDF with `args`, running `steps` nested calls; every request
/// the stub saw, and the answer.
async fn put(r: &StubRig, args: Value, steps: usize) -> (Vec<ApiSeen>, Value, Vec<u8>) {
    let (attachments, bytes) = a_file(r);
    let output = r
        .call_running("KitFilePut", args, attachments, steps)
        .await
        .expect("KitFilePut answered");
    assert!(
        output.attachments.is_empty(),
        "a put answers no attachment: {:?}",
        output.attachments.refs()
    );
    let seen = r.stub.lock().unwrap().api_seen.clone();
    (seen, output.value, bytes)
}

/// The one `PUT` in `seen`: the caller's file, byte for byte, with the
/// blob's length and type.
fn assert_put_carries(seen: &ApiSeen, bytes: &[u8]) {
    assert_eq!(seen.method, "PUT");
    assert_eq!(seen.path_and_query, FILES);
    assert!(seen.body == bytes, "the stub received the file exactly");
    assert_eq!(
        seen.headers.get("content-length").unwrap(),
        &SIZE.to_string()
    );
    assert_eq!(seen.headers.get("content-type").unwrap(), "application/pdf");
}

/// 3 MiB up: the caller's attachment is the nested `PUT`'s body, forwarded
/// by name in the activation that delivered the request.
#[tokio::test(flavor = "multi_thread")]
async fn a_callers_file_is_the_put_body() {
    let r = rig().await;
    let (seen, value, bytes) = put(&r, json!({ "path": FILES }), 1).await;
    assert_eq!(seen.len(), 1, "one PUT: {seen:?}");
    assert_put_carries(&seen[0], &bytes);
    assert_eq!(value, json!({ "status": 200, "bytes": SIZE }));
}

/// The two-step put: the `PUT` is issued by the activation that receives the
/// `HEAD`'s answer, so the body is opened by the request's pending id in a
/// later activation than the one that delivered it.
#[tokio::test(flavor = "multi_thread")]
async fn a_put_after_a_head_opens_the_body_in_a_later_activation() {
    let r = rig().await;
    let (seen, value, bytes) = put(&r, json!({ "path": FILES, "head": true }), 2).await;
    assert_eq!(seen.len(), 2, "a HEAD, then a PUT: {seen:?}");
    assert_eq!(
        (seen[0].method.as_str(), seen[0].path_and_query.as_str()),
        ("HEAD", FILES)
    );
    assert!(seen[0].body.is_empty(), "the HEAD carries no body");
    assert_put_carries(&seen[1], &bytes);
    assert_eq!(value, json!({ "status": 200, "bytes": SIZE }));
}

/// 3 MiB down: `api-request` writes the `GET`'s body into a blob, and the
/// fixture hands that blob to its caller as `file` — the caller's bytes are
/// the API's, in the API's type, and the store holds one blob for both, so
/// nothing was copied.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_is_the_callers_attachment() {
    let r = rig().await;
    let bytes = file_bytes(SIZE);
    r.answer(FILES, bytes.clone());
    r.stub
        .lock()
        .unwrap()
        .api_content_types
        .insert(FILES.to_string(), Some("image/png".to_string()));
    let before = r.blob_usage();

    let output = r
        .call_running(
            "KitFileGet",
            json!({ "path": FILES }),
            HeldAttachments::none(),
            1,
        )
        .await
        .expect("KitFileGet answered");
    assert_eq!(
        output.value,
        json!({ "status": 200, "bytes": SIZE, "attachment": FILE })
    );
    let hold = output
        .attachments
        .hold(&AttachmentName::new(FILE).unwrap())
        .expect("the answer carries `file`");
    assert_eq!(output.attachments.refs().iter().count(), 1);
    assert_eq!(hold.reference().content_type, "image/png");
    assert!(
        hold.read(0, SIZE).expect("the attachment reads") == bytes,
        "the caller's attachment is the API's body exactly"
    );

    let seen = r.seen();
    assert_eq!(seen.len(), 1, "one GET: {seen:?}");
    assert_eq!((seen[0].0.as_str(), seen[0].1.as_str()), ("GET", FILES));
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: before.sealed + 1,
            unsealed: 0,
            charged_bytes: before.charged_bytes + SIZE as u64,
        },
        "the answer's blob is the caller's, not a copy"
    );
}

/// A 3 MiB PDF handed to a `KitFilePut` call as `file`.
fn a_file(r: &StubRig) -> (HeldAttachments, Vec<u8>) {
    let bytes = file_bytes(SIZE);
    let hold = seal_bytes(
        r.messenger.blob_store().expect("a blob store"),
        "application/pdf",
        &bytes,
    );
    (brenn_messaging::testutils::held(&[(FILE, &hold)]), bytes)
}

/// Call `KitFilePut` with `head: true`, serve it and run its `HEAD`, so the
/// `HEAD`'s answer is owed to the provider and the `PUT` is the next step.
/// The call's task and the request's id.
async fn put_past_its_head(
    r: &StubRig,
    attachments: HeldAttachments,
) -> (JoinHandle<Result<ToolOutput, ToolError>>, String) {
    let call = r.call_for_alice_with(
        "KitFilePut",
        json!({ "path": FILES, "head": true }),
        attachments,
    );
    let id = r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    (call, id)
}

/// Abort a call's task, as a caller that stops waiting does, and wait until
/// the call is gone.
async fn drop_caller(call: JoinHandle<Result<ToolOutput, ToolError>>) {
    call.abort();
    assert!(
        call.await.expect_err("the call was aborted").is_cancelled(),
        "the call ended before it was dropped"
    );
}

/// The ids of every request the provider's next activation is handed on its
/// `tool-requests` port, context and new.
async fn handed_requests(r: &StubRig) -> Vec<String> {
    let (_, ports) = r
        .messenger
        .load_activation_snapshot(&r.provider_sub, &r.provider.inputs, MountDebt::Settled)
        .await
        .expect("the provider is owed an activation");
    let requests = ports
        .iter()
        .find(|port| port.port == TOOL_REQUEST_INPUT_PORT)
        .expect("the provider has a tool-requests port");
    requests
        .entries
        .iter()
        .map(|(_, envelope)| {
            let body: Value =
                serde_json::from_str(envelope.body.as_text().expect("a text request body"))
                    .expect("a JSON request body");
            body["id"].as_str().expect("a request id").to_string()
        })
        .collect()
}

/// A put whose request leaves the provider's request window while it waits
/// on its `HEAD` still sends the caller's file: four newer requests, each
/// abandoned by its caller once it is on the inbox, push the put's message
/// out of the window its next activation is handed, and the body is opened
/// by the pending id.
#[tokio::test(flavor = "multi_thread")]
async fn a_put_whose_request_left_the_window_still_sends_the_file() {
    let r = rig().await;
    let (attachments, bytes) = a_file(&r);
    let (call, id) = put_past_its_head(&r, attachments).await;

    let depth = 4;
    for n in 0..depth {
        let get = r.call_for_alice_with(
            "KitFileGet",
            json!({ "path": FILES }),
            HeldAttachments::none(),
        );
        // The `HEAD`'s answer, and each request so far.
        r.owed(&r.provider_sub, n + 2).await;
        drop_caller(get).await;
    }
    let handed = handed_requests(&r).await;
    assert_eq!(handed.len(), depth, "{handed:?}");
    assert!(
        !handed.contains(&id),
        "the put's request is still in the window: {handed:?}"
    );

    r.serve().await;
    r.run_nested().await;
    r.serve().await;
    let output = call.await.unwrap().expect("KitFilePut answered");
    assert!(output.attachments.is_empty());
    assert_eq!(output.value, json!({ "status": 200, "bytes": SIZE }));
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), 2, "a HEAD, then a PUT: {seen:?}");
    assert_eq!(seen[0].method, "HEAD");
    assert_put_carries(&seen[1], &bytes);
    r.assert_finished(&id);
}

/// A put whose caller is gone before its `PUT` step sends nothing: the step
/// finds the request no longer pending, no nested call is issued and no row
/// is kept.
#[tokio::test(flavor = "multi_thread")]
async fn a_put_whose_caller_left_sends_no_file() {
    let r = rig().await;
    let (attachments, _) = a_file(&r);
    let (call, id) = put_past_its_head(&r, attachments).await;
    drop_caller(call).await;
    assert!(r.runtime.serving(&r.provider.slug, &id).is_none());

    r.serve().await;
    assert!(
        owed_everywhere(&r.messenger, &r.executor_sub())
            .await
            .is_empty(),
        "a nested call was issued for a request nobody waits on"
    );
    let seen = r.seen();
    assert_eq!(seen.len(), 1, "the HEAD alone: {seen:?}");
    assert_eq!(seen[0].0, "HEAD");
    assert!(r.inflight_keys().is_empty(), "the component kept a row");
}
