//! Files through a provider-kit component, end to end: the compiled
//! `processor-kit-file-test` fixture, served by the kit's `serve_with_blobs`,
//! forwards a caller's attachment as a nested `api-request`'s body and an
//! `api-request` answer's blob as its own completion's attachment, and builds
//! a nested call's body when the call is issued, through the real guest
//! adapter. The fixture's instance is lowered from its
//! specification onto the rig the Google suites share: its `config` names an
//! OAuth provider and one host, which is all that rig assumes.

use brenn_envelope::AttachmentName;
use brenn_envelope::addressing::TOOL_REQUEST_INPUT_PORT;
use brenn_messaging::testutils::owed_everywhere;
use brenn_messaging_store::blob::{BlobHold, BlobUsage};
use brenn_messaging_store::testutils::seal_bytes;
use brenn_oauth::testutil::ApiSeen;
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use brenn_wasm::{PROCESSOR_FUEL_MINIMUM, processor_fuel};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::google_workspace_rig::{self as workspace, Clauses, GoogleClass, RigOptions};
use super::kit_body_fuel_measure::LINE_FEEDS;
use super::provider_rig::{MARGIN, fuel};
use super::stub_rig::{StubRig, file_bytes};

pub(super) const KIT_FILE: GoogleClass = GoogleClass {
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
pub(super) const FILES: &str = "/files/report.pdf";
/// The attachment the fixture sends and answers.
const FILE: &str = "file";
/// The size of the files moved each way.
const SIZE: usize = 3 << 20;

pub(super) const CLAUSES: Clauses<'static> = &[("PUT", FILES), ("HEAD", FILES), ("GET", FILES)];
/// The fixture's Σ `max_concurrency`, its request window's depth.
const WINDOW: usize = 12;
/// The most bodies the provider kit builds in one activation.
pub(super) const BUILD_BUDGET: usize = 4;
/// How many times `KitBodyPut` repeats its text: the built body is over the
/// 64 KiB a component's call arguments are capped at.
const TIMES: usize = 80;
/// The size of the file a built body ends with.
const BUILT_FILE_SIZE: usize = 1 << 20;

pub(super) async fn rig() -> StubRig {
    workspace::rig(&KIT_FILE, CLAUSES).await
}

/// The bus cap the Google guides set, and the size of a large request's body.
pub(super) const LARGE: usize = 393_216;
/// The bus cap a deployment runs with when it sets none, and the size of a
/// default request's body.
pub(super) const DEFAULT: usize = 65_536;
/// The `KitBodyPut` requests the fixture admits at once.
const ADMITTED: usize = 8;
/// How many large requests wait in the backlog behind four small ones.
const QUEUED: usize = 2;
/// How many large requests wait in the backlog behind four small ones on the
/// envelope term alone: [`QUEUED`] of them drain on the 100M that ten
/// envelopes bring.
const QUEUED_PAST_ENVELOPES: usize = 4;
/// The file each `hex` request carries: the smallest whole number of MiB at
/// which one hex build, about 34 fuel a byte of file, costs at least twice
/// the floor.
const HEX_FILE_SIZE: usize = 3 << 20;

/// A rig whose bus carries requests of [`LARGE`] bytes.
pub(super) async fn large_rig() -> StubRig {
    workspace::rig_built(
        &KIT_FILE,
        CLAUSES,
        RigOptions {
            max_body_bytes: Some(LARGE),
            ..RigOptions::default()
        },
    )
    .await
}

/// A `KitBodyPut` `text` for a request body just under `size` bytes, of
/// [`LINE_FEEDS`], the costliest content a body is built from, a byte of
/// request.
fn line_feed_text(size: usize) -> String {
    LINE_FEEDS.string(size)
}

/// `KitBodyPut`'s arguments for [`line_feed_text`] of `size`.
fn line_feeds(size: usize) -> Value {
    body_args(&line_feed_text(size), 1)
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

/// The 1,000-character text `KitBodyPut` repeats.
fn body_text() -> String {
    (0..1000u32)
        .map(|n| char::from(b'a' + (n % 26) as u8))
        .collect()
}

/// `KitBodyPut`'s arguments: `text` repeated `times` times.
fn body_args(text: &str, times: usize) -> Value {
    json!({ "path": FILES, "text": text, "times": times })
}

/// A built `PUT` in `seen`: `body` exactly, with its length and the built
/// blob's type.
fn assert_built_put(seen: &ApiSeen, body: &[u8]) {
    assert_eq!(seen.method, "PUT");
    assert_eq!(seen.path_and_query, FILES);
    assert!(
        seen.body == body,
        "the stub received the built body exactly"
    );
    assert_eq!(
        seen.headers.get("content-length").unwrap(),
        &body.len().to_string()
    );
    assert_eq!(
        seen.headers.get("content-type").unwrap(),
        "application/octet-stream"
    );
}

/// A file of `size` bytes, sealed once and handed to each call that asks.
struct File {
    hold: BlobHold,
    bytes: Vec<u8>,
}

impl File {
    fn sealed(r: &StubRig, size: usize) -> File {
        let bytes = file_bytes(size);
        let hold = seal_bytes(
            r.messenger.blob_store().expect("a blob store"),
            "application/pdf",
            &bytes,
        );
        File { hold, bytes }
    }

    fn attachments(&self) -> HeldAttachments {
        brenn_messaging::testutils::held(&[(FILE, &self.hold)])
    }
}

/// A body built of the caller's arguments alone, larger than a component's
/// call arguments may be, is the nested `PUT`'s body.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_built_from_arguments_is_the_put_body() {
    let r = rig().await;
    let text = body_text();
    let output = r
        .call_running(
            "KitBodyPut",
            body_args(&text, TIMES),
            HeldAttachments::none(),
            1,
        )
        .await
        .expect("KitBodyPut answered");
    let body = text.repeat(TIMES).into_bytes();
    assert!(body.len() > 64 * 1024);
    assert_eq!(output.value, json!({ "status": 200, "bytes": body.len() }));
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), 1, "one PUT: {seen:?}");
    assert_built_put(&seen[0], &body);
}

/// A body built from a request's attachment: a prefix the component writes,
/// then the caller's 1 MiB file read in pieces.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_built_around_an_attachment_is_the_put_body() {
    let r = rig().await;
    let file = File::sealed(&r, BUILT_FILE_SIZE);
    let output = r
        .call_running(
            "KitBodyPut",
            body_args("prefix\n", 1),
            file.attachments(),
            1,
        )
        .await
        .expect("KitBodyPut answered");
    let mut body = b"prefix\n".to_vec();
    body.extend_from_slice(&file.bytes);
    assert_eq!(output.value, json!({ "status": 200, "bytes": body.len() }));
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), 1, "one PUT: {seen:?}");
    assert_built_put(&seen[0], &body);
}

/// With `hex`, the attachment is written as two lowercase hexadecimal digits
/// a byte after the prefix.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_built_around_an_attachment_in_hex_is_the_put_body() {
    let r = rig().await;
    let file = File::sealed(&r, BUILT_FILE_SIZE);
    let output = r
        .call_running(
            "KitBodyPut",
            json!({ "path": FILES, "text": "prefix\n", "times": 1, "hex": true }),
            file.attachments(),
            1,
        )
        .await
        .expect("KitBodyPut answered");
    let mut body = b"prefix\n".to_vec();
    for byte in &file.bytes {
        body.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    assert_eq!(body.len(), 7 + 2 * BUILT_FILE_SIZE);
    assert_eq!(output.value, json!({ "status": 200, "bytes": body.len() }));
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), 1, "one PUT: {seen:?}");
    assert_built_put(&seen[0], &body);
}

/// Five built requests delivered in one activation: four are built and
/// issued, the fifth waits in the backlog with no body, and the activation
/// that settles the first four builds and issues it. Every body arrives
/// whole.
async fn five_built_in_one_activation(
    args: Value,
    attachments: impl Fn() -> HeldAttachments,
    body: &[u8],
    r: &StubRig,
) {
    let calls: Vec<_> = (0..BUILD_BUDGET + 1)
        .map(|_| r.call_for_alice_with("KitBodyPut", args.clone(), attachments()))
        .collect();
    r.owed(&r.provider_sub, BUILD_BUDGET + 1).await;

    r.serve().await;
    assert_eq!(r.inflight_keys().len(), BUILD_BUDGET);
    assert_eq!(r.backlog_keys().len(), 1, "the fifth is queued");
    assert_eq!(r.stub.lock().unwrap().api_seen.len(), 0);

    r.run_nested_calls(BUILD_BUDGET).await;
    r.serve().await;
    assert!(r.backlog_keys().is_empty(), "the backlog was drained");
    assert_eq!(r.inflight_keys().len(), 1, "the fifth was issued");

    r.run_nested().await;
    r.serve().await;
    for call in calls {
        let output = call.await.unwrap().expect("KitBodyPut answered");
        assert_eq!(output.value, json!({ "status": 200, "bytes": body.len() }));
    }
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), BUILD_BUDGET + 1, "five PUTs");
    for put in &seen {
        assert_built_put(put, body);
    }
    assert!(
        r.inflight_keys().is_empty(),
        "the answered requests left rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn five_bodies_built_from_arguments_in_one_activation_all_arrive() {
    let r = rig().await;
    let text = body_text();
    let body = text.repeat(TIMES).into_bytes();
    five_built_in_one_activation(body_args(&text, TIMES), HeldAttachments::none, &body, &r).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn five_bodies_built_around_attachments_in_one_activation_all_arrive() {
    let r = rig().await;
    let file = File::sealed(&r, BUILT_FILE_SIZE);
    let mut body = b"prefix\n".to_vec();
    body.extend_from_slice(&file.bytes);
    five_built_in_one_activation(body_args("prefix\n", 1), || file.attachments(), &body, &r).await;
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
pub(super) async fn drop_caller(call: JoinHandle<Result<ToolOutput, ToolError>>) {
    call.abort();
    assert!(
        call.await.expect_err("the call was aborted").is_cancelled(),
        "the call ended before it was dropped"
    );
}

/// The ids of every request the provider's next activation is handed on its
/// `tool-requests` port, context and new.
async fn handed_requests(r: &StubRig) -> Vec<String> {
    let ports = r.owed_windows().await;
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
/// on its `HEAD` still sends the caller's file: a window of newer requests, each
/// abandoned by its caller once it is on the inbox, push the put's message
/// out of the window its next activation is handed, and the body is opened
/// by the pending id.
#[tokio::test(flavor = "multi_thread")]
async fn a_put_whose_request_left_the_window_still_sends_the_file() {
    let r = rig().await;
    let (attachments, bytes) = a_file(&r);
    let (call, id) = put_past_its_head(&r, attachments).await;

    let depth = WINDOW;
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

/// Two large requests queued behind four small ones, whose callers then
/// leave: the activation that settles the four answers drops both backlog
/// entries unbuilt. It runs on the floor, which building either would
/// outrun, and completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_build_whose_caller_left_is_dropped_unbuilt() {
    let r = large_rig().await;
    let (small, large) = large_queued_behind_four(&r, 2).await;
    for call in large {
        drop_caller(call).await;
    }
    r.run_nested_calls(BUILD_BUDGET).await;

    r.serve_under(fuel(PROCESSOR_FUEL_MINIMUM)).await;
    assert!(
        r.backlog_keys().is_empty(),
        "the ended entries were dropped"
    );
    assert!(
        r.inflight_keys().is_empty(),
        "the four answers were settled"
    );
    assert!(
        owed_everywhere(&r.messenger, &r.executor_sub())
            .await
            .is_empty(),
        "a nested call was issued for a request nobody waits on"
    );
    assert_answered(small, 1).await;
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), BUILD_BUDGET, "four PUTs and no fifth");
    for put in &seen {
        assert_built_put(put, b"x");
    }
}

/// The envelope term of the grant `r`'s provider's next activation runs on,
/// read from the windows it is owed: the whole grant of an activation whose
/// new messages carry no attachments, before a body or a pending request
/// brought fuel.
async fn envelope_term(r: &StubRig) -> u64 {
    let ports = r.owed_windows().await;
    let envelopes: usize = ports.iter().map(|port| port.entries.len()).sum();
    processor_fuel(envelopes, 0, 0, None)
}

/// Each call was failed by its provider's activation trapping.
async fn assert_failed(calls: Vec<JoinHandle<Result<ToolOutput, ToolError>>>) {
    for call in calls {
        match call.await.expect("the call's task") {
            Err(ToolError::Internal(detail))
                if detail.starts_with("provider activation failed") => {}
            other => panic!("expected the activation's failure, got {other:?}"),
        }
    }
}

/// Each call answered a built body of `bytes` bytes.
async fn assert_answered(calls: Vec<JoinHandle<Result<ToolOutput, ToolError>>>, bytes: usize) {
    for call in calls {
        let output = call.await.unwrap().expect("KitBodyPut answered");
        assert_eq!(output.value, json!({ "status": 200, "bytes": bytes }));
    }
}

/// `n` `KitBodyPut` calls with `args` and no attachments, once the provider
/// is owed `owed` requests in all.
async fn body_puts(
    r: &StubRig,
    args: &Value,
    n: usize,
    owed: usize,
) -> Vec<JoinHandle<Result<ToolOutput, ToolError>>> {
    let calls = (0..n)
        .map(|_| r.call_for_alice_with("KitBodyPut", args.clone(), HeldAttachments::none()))
        .collect();
    r.owed(&r.provider_sub, owed).await;
    calls
}

/// Eight default-sized requests, as many as `KitBodyPut` admits at once,
/// each built from its arguments: the activation they arrive in builds four
/// and queues four, the next settles four answers and drains the queue, the
/// last settles the rest, every one under the margin. Every body arrives
/// whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_default_window_of_built_bodies_completes_inside_the_margin() {
    let r = rig().await;
    let text = line_feed_text(DEFAULT);
    let calls = body_puts(&r, &body_args(&text, 1), ADMITTED, ADMITTED).await;

    r.serve_under(Some(MARGIN)).await;
    assert_eq!(r.inflight_keys().len(), BUILD_BUDGET, "four are issued");
    assert_eq!(
        r.backlog_keys().len(),
        ADMITTED - BUILD_BUDGET,
        "four are queued"
    );

    r.run_nested_calls(BUILD_BUDGET).await;
    r.serve_under(Some(MARGIN)).await;
    assert!(r.backlog_keys().is_empty(), "the backlog was drained");
    assert_eq!(
        r.inflight_keys().len(),
        ADMITTED - BUILD_BUDGET,
        "the queued four are issued"
    );

    r.run_nested_calls(ADMITTED - BUILD_BUDGET).await;
    r.serve_under(Some(MARGIN)).await;
    assert_answered(calls, text.len()).await;
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), ADMITTED, "eight PUTs");
    for put in &seen {
        assert_built_put(put, text.as_bytes());
    }
    assert!(
        r.inflight_keys().is_empty(),
        "the answered requests left rows"
    );
}

/// Two requests at the Google guides' bus cap are built and issued in
/// the activation they arrive in, under the margin.
#[tokio::test(flavor = "multi_thread")]
async fn two_large_bodies_are_built_inside_the_margin() {
    let r = large_rig().await;
    let text = line_feed_text(LARGE);
    let calls = body_puts(&r, &body_args(&text, 1), 2, 2).await;

    r.serve_under(Some(MARGIN)).await;
    assert_eq!(r.inflight_keys().len(), 2, "both are issued");
    r.run_nested_calls(2).await;
    r.serve().await;
    assert_answered(calls, text.len()).await;
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), 2, "two PUTs");
    for put in &seen {
        assert_built_put(put, text.as_bytes());
    }
}

/// The same two requests on the envelope term alone, the grant before a
/// body brought fuel: the activation traps and both callers are failed.
#[tokio::test(flavor = "multi_thread")]
async fn two_large_bodies_trap_on_the_envelope_term_alone() {
    let r = large_rig().await;
    let calls = body_puts(&r, &line_feeds(LARGE), 2, 2).await;
    let grant = envelope_term(&r).await;
    assert_eq!(grant, PROCESSOR_FUEL_MINIMUM);

    r.serve_under(fuel(grant)).await;
    assert_failed(calls).await;
    assert!(r.inflight_keys().is_empty(), "the trap kept a row");
}

/// `KitBodyPut`'s arguments for a body that is the hexadecimal of the
/// request's attachment after a prefix.
fn hex_args() -> Value {
    json!({ "path": FILES, "text": "prefix\n", "times": 1, "hex": true })
}

/// Five `hex` requests, each carrying `file`, new in one activation under
/// the production limits: four are built and issued, the fifth queued, and
/// the four answers are owed to the provider.
async fn five_hex_one_queued(
    r: &StubRig,
    file: &File,
) -> Vec<JoinHandle<Result<ToolOutput, ToolError>>> {
    let calls = (0..=BUILD_BUDGET)
        .map(|_| r.call_for_alice_with("KitBodyPut", hex_args(), file.attachments()))
        .collect();
    r.owed(&r.provider_sub, BUILD_BUDGET + 1).await;
    r.serve().await;
    assert_eq!(r.inflight_keys().len(), BUILD_BUDGET, "four are issued");
    assert_eq!(r.backlog_keys().len(), 1, "the fifth is queued");
    r.run_nested_calls(BUILD_BUDGET).await;
    calls
}

/// A body built from a pending request's attachment, in hex, out of the
/// backlog: the activation that settles the first four answers builds the
/// fifth on the fuel its request's attachment brings while it is pending,
/// and every `PUT` is the hex of its file.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_hex_build_from_a_pending_attachment_is_issued() {
    let r = rig().await;
    let file = File::sealed(&r, HEX_FILE_SIZE);
    let calls = five_hex_one_queued(&r, &file).await;

    r.serve().await;
    assert!(r.backlog_keys().is_empty(), "the fifth was built");
    assert_eq!(r.inflight_keys().len(), 1, "the fifth was issued");
    r.run_nested().await;
    r.serve().await;

    let mut body = b"prefix\n".to_vec();
    for byte in &file.bytes {
        body.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    assert_answered(calls, body.len()).await;
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), BUILD_BUDGET + 1, "five PUTs");
    for put in &seen {
        assert_built_put(put, &body);
    }
}

/// The same drain on the envelope term alone, the grant before a pending
/// request's attachment brought fuel: the activation traps, and the queued
/// entry and the four in-flight rows are still there.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_hex_build_traps_on_the_envelope_term_alone() {
    let r = rig().await;
    let file = File::sealed(&r, HEX_FILE_SIZE);
    let _calls = five_hex_one_queued(&r, &file).await;

    r.serve_under(fuel(envelope_term(&r).await)).await;
    assert_eq!(r.backlog_keys().len(), 1, "the queued entry was kept");
    assert_eq!(
        r.inflight_keys().len(),
        BUILD_BUDGET,
        "the four rows were kept"
    );
}

/// Four one-character requests and then `large` line-feed requests of
/// [`LARGE`] bytes, new in one activation under the production limits: the
/// small four are built and issued, the large queued, and the four answers
/// are owed to the provider. The small calls, then the large.
async fn large_queued_behind_four(
    r: &StubRig,
    large: usize,
) -> (
    Vec<JoinHandle<Result<ToolOutput, ToolError>>>,
    Vec<JoinHandle<Result<ToolOutput, ToolError>>>,
) {
    let small = body_puts(r, &body_args("x", 1), BUILD_BUDGET, BUILD_BUDGET).await;
    let queued = body_puts(r, &line_feeds(LARGE), large, BUILD_BUDGET + large).await;
    r.serve().await;
    assert_eq!(
        r.inflight_keys().len(),
        BUILD_BUDGET,
        "the small four are issued"
    );
    assert_eq!(r.backlog_keys().len(), large, "the large are queued");
    (small, queued)
}

/// Large requests queued behind four small ones are built out of the
/// backlog, under the margin, by the activation that settles the four
/// answers: their bodies bring fuel while the requests are pending. Every
/// body arrives whole.
#[tokio::test(flavor = "multi_thread")]
async fn queued_large_bodies_are_built_from_the_backlog_inside_the_margin() {
    let r = large_rig().await;
    let text = line_feed_text(LARGE);
    let (small, large) = large_queued_behind_four(&r, QUEUED).await;
    r.run_nested_calls(BUILD_BUDGET).await;

    r.serve_under(Some(MARGIN)).await;
    assert!(r.backlog_keys().is_empty(), "the backlog was drained");
    assert_eq!(r.inflight_keys().len(), QUEUED, "the large are issued");
    assert_answered(small, 1).await;

    r.run_nested_calls(QUEUED).await;
    r.serve().await;
    assert_answered(large, text.len()).await;
    let seen = r.stub.lock().unwrap().api_seen.clone();
    assert_eq!(seen.len(), BUILD_BUDGET + QUEUED);
    let (small_puts, large_puts) = seen.split_at(BUILD_BUDGET);
    for put in small_puts {
        assert_built_put(put, b"x");
    }
    for put in large_puts {
        assert_built_put(put, text.as_bytes());
    }
}

/// The same drain, of [`QUEUED_PAST_ENVELOPES`] large requests, on the
/// envelope term alone, the grant before a pending request's body brought
/// fuel: the activation traps, and the queued entries and the four in-flight
/// rows are still there.
#[tokio::test(flavor = "multi_thread")]
async fn queued_large_bodies_trap_on_the_envelope_term_alone() {
    let r = large_rig().await;
    let _calls = large_queued_behind_four(&r, QUEUED_PAST_ENVELOPES).await;
    r.run_nested_calls(BUILD_BUDGET).await;

    r.serve_under(fuel(envelope_term(&r).await)).await;
    assert_eq!(
        r.backlog_keys().len(),
        QUEUED_PAST_ENVELOPES,
        "the queued entries were kept"
    );
    assert_eq!(
        r.inflight_keys().len(),
        BUILD_BUDGET,
        "the four rows were kept"
    );
}
