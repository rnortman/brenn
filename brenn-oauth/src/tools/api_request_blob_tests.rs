//! `api-request` with a body taken from an attachment and an answer written
//! into one, against the stub server, over a blob store in a scratch
//! directory.

use std::sync::Arc;

use base64ct::{Base64, Encoding};
use brenn_envelope::AttachmentName;
use brenn_lib::config::ApiKeyTable;
use brenn_lib::messaging::ParticipantId;
use brenn_messaging_store::blob::{BlobHold, BlobLimits, BlobStore, BlobUsage, HeldAttachments};
use brenn_provider_kit::{BlobAnswer, Target, Unreadable, read_api_blob};
use brenn_tool_registry::{AsyncTool, ToolCtx, ToolError, ToolOutput};

use super::BlobStoreSlot;
use super::api_request::ApiRequestTool;
use crate::testutil::{
    ApiSeen, Stub, alice, credential, provider_clause, provider_config, service, start_stub,
    tool_ctx,
};

/// The default `max_blob_bytes` and pool, as a deployment gets them.
const MAX_BLOB: u64 = 64 << 20;
const BUDGET: u64 = 1 << 30;

/// Three MiB, above every inline cap.
const LARGE: usize = 3 << 20;

struct Fixture {
    tool: ApiRequestTool,
    stub: Stub,
    host: String,
    store: Arc<BlobStore>,
    _vault: tempfile::TempDir,
    _blobs: tempfile::TempDir,
}

/// A `google` OAuth provider on the stub with alice's credential stored, and
/// `api-request` over a store of `limits`; inline answers read up to
/// `max_response_bytes`.
async fn fixture_with(limits: BlobLimits, max_response_bytes: usize) -> Fixture {
    let (base, stub) = start_stub().await;
    let (name, mut google) = provider_config("google", &base);
    google.raw.max_response_bytes = max_response_bytes;
    let (svc, vault) = service(vec![(name, google)]);
    svc.vault()
        .store("alice", &credential("google", "alice@example.com", "RT-a"))
        .await;
    let blobs = tempfile::tempdir().unwrap();
    let store = BlobStore::for_tests(blobs.path(), limits);
    let slot = BlobStoreSlot::default();
    slot.attach(store.clone());
    Fixture {
        tool: ApiRequestTool::new(Some(svc), ApiKeyTable::empty(), slot),
        stub,
        host: base.trim_start_matches("http://").to_string(),
        store,
        _vault: vault,
        _blobs: blobs,
    }
}

async fn fixture() -> Fixture {
    fixture_with(
        BlobLimits {
            max_blob_bytes: MAX_BLOB,
            budget_bytes: BUDGET,
        },
        brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .await
}

/// `len` bytes that are not all one value, so a misplaced piece shows.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

impl Fixture {
    fn args(&self, method: &str, path: &str) -> serde_json::Value {
        serde_json::json!({
            "provider": "google", "method": method, "host": self.host, "path": path,
        })
    }

    /// A sealed blob of `bytes` and `content_type`.
    fn blob(&self, content_type: &str, bytes: &[u8]) -> BlobHold {
        let mut writer = self.store.create(content_type).unwrap();
        writer.write(0, bytes).unwrap();
        writer.seal().unwrap()
    }

    async fn call(
        &self,
        args: serde_json::Value,
        attachments: Vec<(&str, BlobHold)>,
    ) -> Result<ToolOutput, ToolError> {
        let attachments = HeldAttachments::new(
            attachments
                .into_iter()
                .map(|(name, hold)| (AttachmentName::new(name).unwrap(), hold)),
        )
        .unwrap();
        self.tool
            .execute(
                &tool_ctx(Some(alice()), vec![provider_clause("google")]),
                args,
                attachments,
            )
            .await
    }

    /// `args` called by a component whose instance is not granted `blobs`.
    async fn call_without_blobs(&self, args: serde_json::Value) -> Result<ToolOutput, ToolError> {
        self.tool
            .execute(&component_without_blobs(), args, HeldAttachments::none())
            .await
    }

    fn seen(&self) -> Vec<ApiSeen> {
        self.stub.lock().unwrap().api_seen.clone()
    }

    fn usage(&self) -> BlobUsage {
        self.store.usage()
    }

    /// Answer every request to `path` with `body`, its `Content-Type` as
    /// given (`None`: no header).
    fn answer(&self, path: &str, body: Vec<u8>, content_type: Option<&str>) {
        let mut s = self.stub.lock().unwrap();
        s.api_bodies.insert(path.to_string(), body);
        s.api_content_types
            .insert(path.to_string(), content_type.map(str::to_string));
    }
}

/// A stamped call from the component `wasm:maps`, which is not granted
/// `blobs`.
fn component_without_blobs() -> ToolCtx {
    ToolCtx {
        caller: ParticipantId::for_wasm("maps"),
        acting_conversation_id: None,
        caller_opens_blobs: false,
        ..tool_ctx(Some(alice()), vec![provider_clause("google")])
    }
}

/// The only attachment of `output`: its name, content type and bytes.
fn only_attachment(output: &ToolOutput) -> (String, String, Vec<u8>) {
    let refs: Vec<_> = output.attachments.refs().iter().collect();
    assert_eq!(refs.len(), 1, "{:?}", output.attachments);
    let (name, reference) = refs[0];
    let hold = &output.attachments.holds()[0];
    let size = usize::try_from(reference.size).unwrap();
    (
        name.as_str().to_string(),
        reference.content_type.clone(),
        hold.read(0, size).unwrap(),
    )
}

/// `output`'s value as a provider-kit component reads it, so a change to the
/// answer's shape on either side fails here.
fn kit_reads(output: &ToolOutput) -> BlobAnswer {
    let target = Target {
        method: "GET".to_string(),
        host: "stub".to_string(),
        path: "/".to_string(),
    };
    read_api_blob(Ok(output.value.clone()), &target, "Stub")
        .unwrap_or_else(|failure| panic!("the kit refused {}: {failure:?}", output.value))
}

fn invalid(out: Result<ToolOutput, ToolError>) -> String {
    match out {
        Err(ToolError::InvalidArgs(detail)) => detail,
        other => panic!("expected invalid_args, got {other:?}"),
    }
}

#[tokio::test]
async fn a_body_attachment_is_sent_as_it_is() {
    let f = fixture().await;
    let bytes = pattern(LARGE);
    let file = f.blob("application/pdf", &bytes);
    let mut args = f.args("PUT", "/upload/a");
    args["body_attachment"] = serde_json::json!("file");
    let out = f.call(args, vec![("file", file.clone())]).await.unwrap();
    assert_eq!(out.value["status"], 200);
    assert!(out.attachments.is_empty(), "an inline answer");

    // A caller's Content-Type wins over the blob's.
    let mut args = f.args("PUT", "/upload/b");
    args["body_attachment"] = serde_json::json!("file");
    args["headers"] = serde_json::json!({"Content-Type": "text/plain"});
    f.call(args, vec![("file", file)]).await.unwrap();

    let seen = f.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].body, bytes);
    assert_eq!(seen[0].headers["content-length"], LARGE.to_string());
    assert!(!seen[0].headers.contains_key("transfer-encoding"));
    assert_eq!(seen[0].headers["content-type"], "application/pdf");
    assert_eq!(seen[1].body, bytes);
    assert_eq!(seen[1].headers["content-type"], "text/plain");
}

#[tokio::test]
async fn an_empty_body_attachment_sends_an_empty_body() {
    let f = fixture().await;
    let mut args = f.args("PUT", "/upload/empty");
    args["body_attachment"] = serde_json::json!("file");
    f.call(args, vec![("file", f.blob("text/plain", b""))])
        .await
        .unwrap();
    let seen = f.seen();
    assert!(seen[0].body.is_empty());
    // Framed as an empty `body` is: no chunked encoding, no length but zero.
    assert!(!seen[0].headers.contains_key("transfer-encoding"));
    assert!(
        seen[0]
            .headers
            .get("content-length")
            .is_none_or(|length| length == "0")
    );
}

#[tokio::test]
async fn the_three_body_sources_are_mutually_exclusive() {
    let f = fixture().await;
    let sources = [
        ("body", serde_json::json!("x")),
        (
            "body_base64",
            serde_json::json!(Base64::encode_string(b"x")),
        ),
        ("body_attachment", serde_json::json!("file")),
    ];
    let combinations: [&[usize]; 4] = [&[0, 1], &[0, 2], &[1, 2], &[0, 1, 2]];
    for combination in combinations {
        let mut args = f.args("POST", "/a");
        for &i in combination {
            args[sources[i].0] = sources[i].1.clone();
        }
        let detail = invalid(
            f.call(args, vec![("file", f.blob("text/plain", b"y"))])
                .await,
        );
        assert!(detail.contains("mutually exclusive"), "{detail}");
        for &i in combination {
            assert!(detail.contains(sources[i].0), "{detail}");
        }
    }
    assert!(f.seen().is_empty());
}

#[tokio::test]
async fn attachments_must_be_the_body_and_names_must_be_valid() {
    let f = fixture().await;
    let blob = || f.blob("text/plain", b"y");

    // `body_attachment` names nothing the call carries.
    let mut args = f.args("POST", "/a");
    args["body_attachment"] = serde_json::json!("other");
    let detail = invalid(f.call(args, vec![("file", blob())]).await);
    assert!(
        detail.contains("\"other\"") && detail.contains("file"),
        "{detail}"
    );
    // A name outside the attachment-name grammar can name no carried
    // attachment, whose names are all inside it.
    let mut args = f.args("POST", "/a");
    args["body_attachment"] = serde_json::json!("bad name");
    let detail = invalid(f.call(args, vec![("file", blob())]).await);
    assert!(
        detail.contains("`body_attachment` \"bad name\" names no attachment")
            && detail.contains("[\"file\"]"),
        "{detail}"
    );

    // An attachment nothing uses, with and without a named body.
    let detail = invalid(f.call(f.args("GET", "/a"), vec![("file", blob())]).await);
    assert!(
        detail.contains("nothing uses") && detail.contains("file"),
        "{detail}"
    );
    let mut args = f.args("POST", "/a");
    args["body_attachment"] = serde_json::json!("file");
    let detail = invalid(
        f.call(args, vec![("file", blob()), ("extra", blob())])
            .await,
    );
    assert!(
        detail.contains("extra") && !detail.contains("\"file\""),
        "{detail}"
    );
    let mut args = f.args("POST", "/a");
    args["body"] = serde_json::json!("x");
    let detail = invalid(f.call(args, vec![("file", blob())]).await);
    assert!(
        detail.contains("nothing uses") && detail.contains("[\"file\"]"),
        "{detail}"
    );

    // A `response_attachment` outside the attachment-name grammar.
    for name in ["", "bad name", ".hidden", &"x".repeat(200)] {
        let mut args = f.args("GET", "/a");
        args["response_attachment"] = serde_json::json!(name);
        let detail = invalid(f.call(args, vec![]).await);
        assert!(detail.contains("response_attachment"), "{detail}");
    }

    // The headers Brenn sets stay refused beside a body blob.
    for header in ["Content-Length", "Authorization"] {
        let mut args = f.args("POST", "/a");
        args["body_attachment"] = serde_json::json!("file");
        args["headers"] = serde_json::json!({ header: "1" });
        let detail = invalid(f.call(args, vec![("file", blob())]).await);
        let reserved = format!(
            "header {:?} is Brenn's to set, not the caller's",
            header.to_ascii_lowercase()
        );
        assert!(detail.contains(&reserved), "{detail}");
    }
    assert!(f.seen().is_empty());
}

#[tokio::test]
async fn a_2xx_answer_is_written_into_the_named_attachment() {
    let f = fixture().await;
    let bytes = pattern(LARGE);
    f.answer("/media/a", bytes.clone(), Some("image/png"));
    let mut args = f.args("GET", "/media/a");
    args["response_attachment"] = serde_json::json!("body");
    let out = f.call(args, vec![]).await.unwrap();
    let value = &out.value;
    assert_eq!(value["status"], 200);
    assert_eq!(value["bytes"], LARGE);
    assert_eq!(value["attachment"], "body");
    assert_eq!(value["headers"]["content-type"], "image/png");
    assert_eq!(value["headers"]["content-length"], LARGE.to_string());
    let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["attachment", "bytes", "headers", "status"]);
    match kit_reads(&out) {
        BlobAnswer::Blob {
            status,
            headers,
            bytes,
            attachment,
        } => {
            assert_eq!(
                (status, bytes, attachment.as_str()),
                (200, LARGE as u64, "body")
            );
            assert_eq!(headers["content-type"], "image/png");
        }
        other => panic!("the kit read a sealed answer as {other:?}"),
    }
    let (name, content_type, read) = only_attachment(&out);
    assert_eq!(name, "body");
    assert_eq!(content_type, "image/png");
    assert_eq!(read, bytes);
    assert_eq!(f.usage().sealed, 1);
    drop(out);
    assert_eq!(
        f.usage(),
        BlobUsage {
            sealed: 0,
            unsealed: 0,
            charged_bytes: 0
        },
        "the answer's one hold was the output's"
    );
}

#[tokio::test]
async fn a_chunked_answer_is_written_whole() {
    let f = fixture().await;
    let bytes = pattern(LARGE);
    f.answer("/media/c", bytes.clone(), Some("application/zip"));
    f.stub.lock().unwrap().api_chunked = true;
    let mut args = f.args("GET", "/media/c");
    args["response_attachment"] = serde_json::json!("archive");
    let out = f.call(args, vec![]).await.unwrap();
    assert_eq!(out.value["bytes"], LARGE);
    assert!(out.value["headers"].get("content-length").is_none());
    match kit_reads(&out) {
        BlobAnswer::Blob {
            bytes, attachment, ..
        } => assert_eq!((bytes, attachment.as_str()), (LARGE as u64, "archive")),
        other => panic!("the kit read a sealed chunked answer as {other:?}"),
    }
    let (name, content_type, read) = only_attachment(&out);
    assert_eq!(
        (name.as_str(), content_type.as_str()),
        ("archive", "application/zip")
    );
    assert_eq!(read, bytes);
}

#[tokio::test]
async fn an_answer_ending_inside_a_piece_is_written_whole() {
    let f = fixture().await;
    let bytes = pattern(LARGE + 1000);
    f.answer("/media/r", bytes.clone(), Some("image/png"));
    for chunked in [false, true] {
        f.stub.lock().unwrap().api_chunked = chunked;
        let mut args = f.args("GET", "/media/r");
        args["response_attachment"] = serde_json::json!("body");
        let out = f.call(args, vec![]).await.unwrap();
        assert_eq!(out.value["bytes"], LARGE + 1000, "chunked {chunked}");
        let (_, _, read) = only_attachment(&out);
        assert!(read == bytes, "chunked {chunked}");
    }
}

#[tokio::test]
async fn an_unusable_content_type_makes_an_octet_stream_blob() {
    let f = fixture().await;
    for (path, content_type) in [("/absent", None), ("/malformed", Some("not a media type"))] {
        f.answer(path, b"abc".to_vec(), content_type);
        let mut args = f.args("GET", path);
        args["response_attachment"] = serde_json::json!("body");
        let out = f.call(args, vec![]).await.unwrap();
        let (_, blob_type, read) = only_attachment(&out);
        assert_eq!(blob_type, "application/octet-stream", "{path}");
        assert_eq!(read, b"abc");
        match content_type {
            None => assert!(out.value["headers"].get("content-type").is_none()),
            Some(sent) => assert_eq!(out.value["headers"]["content-type"], sent),
        }
    }
}

#[tokio::test]
async fn an_empty_2xx_answers_an_empty_attachment() {
    let f = fixture().await;
    f.answer("/empty", Vec::new(), Some("text/plain"));
    f.stub.lock().unwrap().api_statuses = [204, 200].into();
    for method in ["DELETE", "HEAD"] {
        let mut args = f.args(method, "/empty");
        args["response_attachment"] = serde_json::json!("body");
        let out = f.call(args, vec![]).await.unwrap();
        assert_eq!(out.value["bytes"], 0, "{method}");
        let (_, _, read) = only_attachment(&out);
        assert!(read.is_empty(), "{method}");
    }
}

#[tokio::test]
async fn an_answer_over_the_largest_blob_is_refused_and_leaves_nothing() {
    let limits = BlobLimits {
        max_blob_bytes: 1 << 20,
        budget_bytes: BUDGET,
    };
    let f = fixture_with(limits, brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES).await;
    let before = f.usage();
    f.answer("/big", pattern(LARGE), Some("video/mp4"));
    for chunked in [false, true] {
        f.stub.lock().unwrap().api_chunked = chunked;
        let mut args = f.args("GET", "/big");
        args["response_attachment"] = serde_json::json!("body");
        let out = f.call(args, vec![]).await.unwrap();
        assert!(out.attachments.is_empty());
        let value = &out.value;
        assert_eq!(value["error"], "response_too_large", "chunked {chunked}");
        assert_eq!(value["status"], 200);
        assert_eq!(value["limit"], 1 << 20);
        assert_eq!(value["headers"]["content-type"], "video/mp4");
        let declared = if chunked {
            serde_json::Value::Null
        } else {
            serde_json::json!(LARGE)
        };
        assert_eq!(value["bytes"], declared, "chunked {chunked}");
        assert_eq!(
            kit_reads(&out),
            BlobAnswer::TooLarge {
                limit: 1 << 20,
                bytes: declared.as_u64()
            },
            "chunked {chunked}"
        );
        assert!(value.get("truncated").is_none());
        assert_eq!(f.usage(), before, "chunked {chunked}");
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_full_pool_is_refused_and_leaves_nothing() {
    let limits = BlobLimits {
        max_blob_bytes: MAX_BLOB,
        budget_bytes: 2 << 20,
    };
    let f = fixture_with(limits, brenn_lib::config::DEFAULT_MAX_RESPONSE_BYTES).await;
    let resting = f.blob("text/plain", &pattern(1 << 20));
    let before = f.usage();
    f.answer("/big", pattern(LARGE), Some("video/mp4"));
    for chunked in [false, true] {
        f.stub.lock().unwrap().api_chunked = chunked;
        let mut args = f.args("GET", "/big");
        args["response_attachment"] = serde_json::json!("body");
        let out = f.call(args, vec![]).await.unwrap();
        assert!(out.attachments.is_empty());
        let keys: Vec<&String> = out.value.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["error", "headers", "status"], "chunked {chunked}");
        assert_eq!(out.value["error"], "blob_store_full");
        assert_eq!(kit_reads(&out), BlobAnswer::StoreFull, "chunked {chunked}");
        assert_eq!(f.usage(), before, "chunked {chunked}");
    }
    logs_assert(|lines: &[&str]| {
        let full: Vec<&&str> = lines
            .iter()
            .filter(|line| line.contains("api-request: blob store full"))
            .collect();
        if full.len() != 2 {
            return Err(format!("expected two full-pool lines, got {full:?}"));
        }
        let charged = format!("charged_bytes={}", before.charged_bytes);
        let budget = format!("budget_bytes={}", 2 << 20);
        let declared = format!("declared_bytes={LARGE}");
        for line in &full {
            for needle in [" WARN ", "caller=", &charged, &budget] {
                if !line.contains(needle) {
                    return Err(format!("{needle:?} missing from {line:?}"));
                }
            }
        }
        if !full.iter().any(|line| line.contains(&declared)) {
            return Err(format!("no line carries {declared:?}: {full:?}"));
        }
        Ok(())
    });
    drop(resting);
}

#[tokio::test]
async fn a_401_resend_sends_the_same_body_and_keeps_one_answer() {
    let f = fixture().await;
    let bytes = pattern(LARGE);
    f.answer("/upload", b"second".to_vec(), Some("text/plain"));
    f.stub.lock().unwrap().api_statuses = [401, 200].into();
    let mut args = f.args("PUT", "/upload");
    args["body_attachment"] = serde_json::json!("file");
    args["response_attachment"] = serde_json::json!("body");
    let out = f
        .call(args, vec![("file", f.blob("application/pdf", &bytes))])
        .await
        .unwrap();
    let seen = f.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].headers["authorization"], "Bearer access");
    assert_eq!(seen[1].headers["authorization"], "Bearer AT-1");
    for seen in &seen {
        assert_eq!(seen.body, bytes);
        assert_eq!(seen.headers["content-length"], LARGE.to_string());
    }
    let (_, _, read) = only_attachment(&out);
    assert_eq!(read, b"second");
    // The answer's blob; the body blob's hold went with the call.
    assert_eq!(f.usage().sealed, 1);
}

#[tokio::test]
async fn a_non_2xx_answer_is_inline_whatever_was_asked() {
    let f = fixture_with(
        BlobLimits {
            max_blob_bytes: MAX_BLOB,
            budget_bytes: BUDGET,
        },
        16,
    )
    .await;
    let before = f.usage();
    f.stub.lock().unwrap().api_failures.insert(
        "/missing".to_string(),
        (404, b"{\"error\":\"not found here\"}".to_vec()),
    );
    let mut args = f.args("GET", "/missing");
    args["response_attachment"] = serde_json::json!("body");
    let out = f.call(args, vec![]).await.unwrap();
    assert!(out.attachments.is_empty());
    assert_eq!(out.value["status"], 404);
    assert_eq!(out.value["body"], "{\"error\":\"not fo");
    assert_eq!(out.value["truncated"], true);
    assert!(out.value.get("attachment").is_none());
    assert!(
        matches!(kit_reads(&out), BlobAnswer::Unreadable(Unreadable::Cut(_))),
        "{}",
        out.value
    );
    assert_eq!(f.usage(), before);
}

#[tokio::test]
async fn an_attachment_answer_needs_an_attached_store() {
    let (base, stub) = start_stub().await;
    let (svc, _vault) = service(vec![provider_config("google", &base)]);
    let tool = ApiRequestTool::new(Some(svc), ApiKeyTable::empty(), BlobStoreSlot::default());
    let host = base.trim_start_matches("http://");
    let args = serde_json::json!({
        "provider": "google", "method": "GET", "host": host, "path": "/a",
        "response_attachment": "body",
    });
    let out = tool
        .execute(
            &tool_ctx(Some(alice()), vec![provider_clause("google")]),
            args,
            HeldAttachments::none(),
        )
        .await;
    match out {
        Err(ToolError::Internal(detail)) => {
            assert!(detail.contains("no blob store is attached"), "{detail}")
        }
        other => panic!("expected internal, got {other:?}"),
    }
    // The grant is checked before the store.
    let args = serde_json::json!({
        "provider": "google", "method": "GET", "host": host, "path": "/a",
        "response_attachment": "body",
    });
    let out = tool
        .execute(&component_without_blobs(), args, HeldAttachments::none())
        .await;
    assert!(matches!(out, Err(ToolError::Denied(_))), "{out:?}");
    assert!(stub.lock().unwrap().api_seen.is_empty());
}

/// A component not granted `blobs` could never open an answer's blob, so it
/// is refused one before anything is sent; its other calls are answered as
/// before, and a malformed name is still refused as malformed first.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_component_without_blobs_is_refused_an_attachment_answer() {
    let f = fixture().await;
    f.answer("/big", pattern(LARGE), Some("video/mp4"));
    let before = f.usage();

    let mut args = f.args("GET", "/big");
    args["response_attachment"] = serde_json::json!("body");
    match f.call_without_blobs(args).await {
        Err(ToolError::Denied(detail)) => {
            for needle in ["`response_attachment`", "wasm:maps", "`blobs`"] {
                assert!(detail.contains(needle), "{needle:?} missing from {detail}");
            }
        }
        other => panic!("expected denied, got {other:?}"),
    }
    assert!(f.seen().is_empty(), "nothing is sent");
    assert_eq!(f.usage(), before, "no blob is made");
    assert!(logs_contain(
        "api-request: `response_attachment` refused; caller is not granted `blobs`"
    ));

    let mut args = f.args("GET", "/big");
    args["response_attachment"] = serde_json::json!("bad name");
    let detail = invalid(f.call_without_blobs(args).await);
    assert!(detail.contains("`response_attachment`"), "{detail}");
    assert!(f.seen().is_empty());

    f.answer("/small", b"hello".to_vec(), Some("text/plain"));
    let out = f.call_without_blobs(f.args("GET", "/small")).await.unwrap();
    assert!(out.attachments.is_empty());
    assert_eq!(out.value["status"], 200);
    assert_eq!(out.value["body"], "hello");
    assert_eq!(f.seen().len(), 1);
    assert_eq!(f.usage(), before);
}

/// A call that uses neither form answers inline.
#[tokio::test]
async fn a_plain_call_is_answered_as_before() {
    let f = fixture().await;
    f.stub.lock().unwrap().api_body = br#"{"a":1}"#.to_vec();
    let out = f.call(f.args("GET", "/plain"), vec![]).await.unwrap();
    assert!(out.attachments.is_empty());
    let mut value = out.value;
    let date = value["headers"]
        .as_object_mut()
        .unwrap()
        .remove("date")
        .expect("the stub's server dates its answers");
    assert!(date.is_string());
    assert_eq!(
        serde_json::to_string(&value).unwrap(),
        r#"{"body":"{\"a\":1}","bytes":7,"headers":{"content-length":"7","content-type":"application/octet-stream"},"status":200,"truncated":false}"#
    );
}
