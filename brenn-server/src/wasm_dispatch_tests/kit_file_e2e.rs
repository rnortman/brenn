//! Files through a provider-kit component, end to end: the compiled
//! `processor-kit-file-test` fixture, served by the kit's `serve_with_blobs`,
//! forwards a caller's attachment as a nested `api-request`'s body and an
//! `api-request` answer's blob as its own completion's attachment, through
//! the real guest adapter. The fixture's instance is lowered from its
//! specification onto the rig the Google suites share: its `config` names an
//! OAuth provider and one host, which is all that rig assumes.

use brenn_envelope::AttachmentName;
use brenn_messaging_store::blob::BlobUsage;
use brenn_messaging_store::testutils::seal_bytes;
use brenn_oauth::testutil::ApiSeen;
use brenn_tool_registry::HeldAttachments;
use serde_json::{Value, json};

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
    let bytes = file_bytes(SIZE);
    let hold = seal_bytes(
        r.messenger.blob_store().expect("a blob store"),
        "application/pdf",
        &bytes,
    );
    let attachments = brenn_messaging::testutils::held(&[(FILE, &hold)]);
    drop(hold);
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
/// `HEAD`'s answer, so the body is opened from the request's window context
/// rather than from the activation that delivered it.
#[tokio::test(flavor = "multi_thread")]
async fn a_put_after_a_head_opens_the_body_from_window_context() {
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
