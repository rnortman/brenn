//! What a provider-kit component's passes over a request's body cost a byte,
//! measured on the compiled `processor-kit-file-test` fixture's `KitBodyPut`
//! on the real host. Run by hand; it prints its figures and asserts only that
//! each activation it measures was the one meant.
//!
//! Two activations are measured, each at two body sizes, by the least grant
//! it completes on, found by bisection to [`PRECISION`] fuel:
//!
//! - *arriving*: four requests new in one activation, each parsed, planned,
//!   its body built from its arguments and issued, and its in-flight row
//!   written;
//! - *draining*: the activation after one handed four one-character requests
//!   and then four of the measured size, which built the small four and
//!   queued the rest. It settles the four answers and builds the four queued
//!   bodies out of the backlog.
//!
//! A slope is the difference of the two least grants over the difference of
//! the body bytes the activation is counted for: the four new bodies when
//! arriving, the four queued requests' bodies when draining. Three argument
//! shapes the fixture refuses after the kit has parsed them are measured
//! arriving only; a shape four of which at the larger size outgrow guest
//! memory is measured two an activation at both sizes, or one where two
//! outgrow it too. The fixture's requests are called below the host's
//! argument check: a conversation's call whose `text` is not a string is
//! refused before it is published, so the refused shapes reach a guest in
//! production only inside a property a tool declares as an array or object
//! or leaves untyped, or from a component caller, whose arguments are capped
//! at 64 KiB.
//!
//! [`measure_hex`] measures the draining activation's build of one body
//! written as the hexadecimal of a request's attachment, a byte of file.

use std::time::Duration;

use brenn_messaging_store::testutils::seal_bytes;
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::kit_file_e2e::{DEFAULT, FILES, LARGE, large_rig, rig};
use super::provider_rig::fuel;
use super::stub_rig::{StubRig, file_bytes};

/// The bisection stops when the least completing grant is known to within
/// this much fuel.
pub(super) const PRECISION: u64 = 50_000;
/// The two request body sizes each slope is taken between, in bytes.
const SIZES: [usize; 2] = [DEFAULT, LARGE];
/// Room left in a request's body for everything but `text`: the request's
/// id, tool, `path` and `times`.
const ENVELOPE_ROOM: usize = 512;
/// How many measured requests an activation is handed.
const MEASURED: usize = 4;
/// Generous enough for any activation measured here: one that traps on it
/// traps on something other than fuel.
pub(super) const AMPLE_FUEL: u64 = 40_000_000_000;

/// What fills a request's `text`.
#[derive(Clone, Copy, Debug)]
pub(super) enum Content {
    /// A string of one repeated unit, after `lead` when the unit alone would
    /// be refused as blank. `wire` is the bytes one unit takes in the
    /// request's JSON body.
    Text {
        name: &'static str,
        lead: &'static str,
        unit: &'static str,
        wire: usize,
    },
    /// `[0,0,…]`, refused as not a string.
    Zeros,
    /// `[[],[],…]`, refused as not a string.
    EmptyArrays,
    /// `{"aaa":0,"aab":0,…}`, refused as not a string.
    ThreeCharKeys,
}

/// One `a` and then line feeds, each two bytes in the request's JSON: the
/// costliest content a body is built from, a byte of request, by the slopes
/// [`measure`] finds, and the content the fuel tests build from.
pub(super) const LINE_FEEDS: Content = Content::Text {
    name: "line feeds",
    lead: "a",
    unit: "\n",
    wire: 2,
};

const CONTENTS: [Content; 10] = [
    Content::Text {
        name: "ASCII letters",
        lead: "",
        unit: "a",
        wire: 1,
    },
    Content::Text {
        name: "quotes",
        lead: "",
        unit: "\"",
        wire: 2,
    },
    Content::Text {
        name: "backslashes",
        lead: "",
        unit: "\\",
        wire: 2,
    },
    Content::Text {
        name: "U+0001",
        lead: "",
        unit: "\u{1}",
        wire: 6,
    },
    LINE_FEEDS,
    Content::Text {
        name: "two-byte characters",
        lead: "",
        unit: "\u{e9}",
        wire: 2,
    },
    Content::Text {
        name: "four-byte characters",
        lead: "",
        unit: "\u{1f600}",
        wire: 4,
    },
    Content::Zeros,
    Content::EmptyArrays,
    Content::ThreeCharKeys,
];

impl Content {
    fn name(self) -> &'static str {
        match self {
            Content::Text { name, .. } => name,
            Content::Zeros => "array of 0s",
            Content::EmptyArrays => "array of empty arrays",
            Content::ThreeCharKeys => "object of three-character keys",
        }
    }

    fn builds(self) -> bool {
        matches!(self, Content::Text { .. })
    }

    /// The `text` of a content that builds, for a request whose body is just
    /// under `size` bytes.
    pub(super) fn string(self, size: usize) -> String {
        match self.text(size) {
            Value::String(text) => text,
            _ => panic!("{} is not a string", self.name()),
        }
    }

    /// `text` for a request whose body is just under `size` bytes.
    fn text(self, size: usize) -> Value {
        let room = size - ENVELOPE_ROOM;
        match self {
            Content::Text {
                lead, unit, wire, ..
            } => {
                let mut text = lead.to_string();
                text.push_str(&unit.repeat((room - lead.len()) / wire));
                Value::String(text)
            }
            Content::Zeros => Value::Array(vec![json!(0); room / 2]),
            Content::EmptyArrays => Value::Array(vec![json!([]); room / 3]),
            Content::ThreeCharKeys => {
                const ALPHABET: &[u8] =
                    b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                let n = ALPHABET.len();
                let keys = room / 8;
                assert!(keys <= n * n * n, "not enough distinct keys");
                Value::Object(
                    (0..keys)
                        .map(|i| {
                            let key: String = [i / (n * n), (i / n) % n, i % n]
                                .iter()
                                .map(|&d| char::from(ALPHABET[d]))
                                .collect();
                            (key, json!(0))
                        })
                        .collect(),
                )
            }
        }
    }
}

fn args(text: Value) -> Value {
    json!({ "path": FILES, "text": text, "times": 1 })
}

/// Call `KitBodyPut` with `args` `n` times, and wait until the provider is
/// owed `owed` requests in all; the calls and the body sizes of every request
/// owed.
async fn call(
    r: &StubRig,
    args: &Value,
    n: usize,
    owed: usize,
) -> (Vec<JoinHandle<Result<ToolOutput, ToolError>>>, Vec<usize>) {
    let calls = (0..n)
        .map(|_| r.call_for_alice_with("KitBodyPut", args.clone(), HeldAttachments::none()))
        .collect();
    r.owed(&r.provider_sub, owed).await;
    let sizes = brenn_messaging::testutils::owed_everywhere(&r.messenger, &r.provider_sub)
        .await
        .iter()
        .map(|(_, envelope)| envelope.body.len())
        .collect();
    (calls, sizes)
}

/// The trap's diagnosis, when a call was failed by its provider's activation
/// trapping.
async fn trap(call: JoinHandle<Result<ToolOutput, ToolError>>) -> Option<String> {
    match tokio::time::timeout(Duration::from_secs(60), call)
        .await
        .expect("the call was answered")
        .expect("the call's task")
    {
        Err(ToolError::Internal(detail)) if detail.starts_with("provider activation failed") => {
            Some(detail)
        }
        _ => None,
    }
}

/// One arriving activation handed `n` requests under `grant`: whether it
/// completed, and the body bytes of the requests it was handed.
async fn arriving(content: Content, n: usize, size: usize, grant: u64) -> (bool, u64) {
    let r = large_rig().await;
    let (calls, sizes) = call(&r, &args(content.text(size)), n, n).await;
    r.serve_under(fuel(grant)).await;
    let completed = if content.builds() {
        let inflight = r.inflight_keys().len();
        assert!(
            inflight == 0 || inflight == n,
            "{}: {inflight} in flight",
            content.name()
        );
        inflight == n
    } else {
        let mut completed = true;
        for call in calls {
            if let Some(detail) = trap(call).await {
                if grant >= AMPLE_FUEL {
                    println!("{}, {n} at {size} bytes: {detail}", content.name());
                }
                completed = false;
            }
        }
        completed
    };
    (completed, sizes.iter().map(|&n| n as u64).sum())
}

/// One draining activation under `grant`: whether it completed, and the body
/// bytes of the four queued requests.
async fn draining(content: Content, size: usize, grant: u64) -> (bool, u64) {
    let r = large_rig().await;
    let (_, small) = call(&r, &args(json!("x")), MEASURED, MEASURED).await;
    let (_, all) = call(&r, &args(content.text(size)), MEASURED, 2 * MEASURED).await;
    let mut queued: Vec<usize> = all.clone();
    for n in &small {
        let at = queued
            .iter()
            .position(|m| m == n)
            .expect("a small request is owed");
        queued.remove(at);
    }
    assert_eq!(queued.len(), MEASURED);

    r.serve_under(fuel(AMPLE_FUEL)).await;
    assert_eq!(
        r.inflight_keys().len(),
        MEASURED,
        "the small four are issued"
    );
    assert_eq!(r.backlog_keys().len(), MEASURED, "the measured four queue");
    r.run_nested_calls(MEASURED).await;

    r.serve_under(fuel(grant)).await;
    let backlog = r.backlog_keys().len();
    assert!(
        backlog == 0 || backlog == MEASURED,
        "{}: {backlog} left in the backlog",
        content.name()
    );
    (backlog == 0, queued.iter().map(|&n| n as u64).sum())
}

/// The least grant `run` completes on, to within [`PRECISION`], and the body
/// bytes it reports.
pub(super) async fn least<F, Fut>(run: F) -> (u64, u64)
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = (bool, u64)>,
{
    let mut lo = 0;
    let mut hi = 64_000_000;
    let bytes = loop {
        let (completed, bytes) = run(hi).await;
        if completed {
            break bytes;
        }
        assert!(hi < AMPLE_FUEL, "nothing completes");
        lo = hi;
        hi = (hi * 2).min(AMPLE_FUEL);
    };
    (narrow(lo, hi, PRECISION, &run).await, bytes)
}

/// The least value above `lo` that `run` completes on, to within
/// `precision`, given that it completes on `hi` and not on `lo`.
pub(super) async fn narrow<F, Fut>(mut lo: u64, mut hi: u64, precision: u64, run: &F) -> u64
where
    F: Fn(u64) -> Fut,
    Fut: std::future::Future<Output = (bool, u64)>,
{
    while hi - lo > precision {
        let mid = lo + (hi - lo) / 2;
        if run(mid).await.0 {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

/// One slope: the least grants at the two sizes, their body bytes, and the
/// fuel a byte between them.
async fn slope<F, Fut>(label: String, run: F) -> f64
where
    F: Fn(usize, u64) -> Fut,
    Fut: std::future::Future<Output = (bool, u64)>,
{
    let (small_grant, small_bytes) = least(|grant| run(SIZES[0], grant)).await;
    let (large_grant, large_bytes) = least(|grant| run(SIZES[1], grant)).await;
    let slope = (large_grant - small_grant) as f64 / (large_bytes - small_bytes) as f64;
    println!(
        "{label}: {small_grant} fuel at {small_bytes} body bytes, \
         {large_grant} at {large_bytes}: {slope:.1} fuel a byte"
    );
    slope
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by //brenn-server:kit_body_fuel_measure"]
async fn measure() {
    let mut slopes = Vec::new();
    for content in CONTENTS {
        // Requests of the larger size that outgrow guest memory four at a
        // time are measured fewer an activation at both sizes, so the fixed
        // cost cancels.
        let mut n = MEASURED;
        while !arriving(content, n, SIZES[1], AMPLE_FUEL).await.0 {
            assert!(n > 1, "{}: one request outgrows the guest", content.name());
            n /= 2;
        }
        let label = format!("arriving, {}, {n} an activation", content.name());
        let arriving = slope(label.clone(), |size, grant| {
            arriving(content, n, size, grant)
        })
        .await;
        slopes.push((label, arriving));
        if content.builds() {
            let draining = slope(format!("draining, {}", content.name()), |size, grant| {
                draining(content, size, grant)
            })
            .await;
            slopes.push((format!("draining, {}", content.name()), draining));
        }
    }
    let (label, costliest) = slopes
        .iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .expect("seventeen slopes");
    println!("costliest: {label}, {costliest:.1} fuel a byte");
}

/// The file sizes the hex build's slope is taken between, in bytes.
const HEX_SIZES: [usize; 2] = [1 << 20, 2 << 20];

/// One draining activation under `grant`, after one handed five `hex`
/// requests each carrying a file of `size` bytes, which built four and queued
/// the fifth: whether it built the fifth, and the file's size.
async fn draining_hex(size: usize, grant: u64) -> (bool, u64) {
    let r = rig().await;
    let hold = seal_bytes(
        r.messenger.blob_store().expect("a blob store"),
        "application/pdf",
        &file_bytes(size),
    );
    let args = json!({ "path": FILES, "text": "prefix\n", "times": 1, "hex": true });
    let _calls: Vec<_> = (0..=MEASURED)
        .map(|_| {
            r.call_for_alice_with(
                "KitBodyPut",
                args.clone(),
                brenn_messaging::testutils::held(&[("file", &hold)]),
            )
        })
        .collect();
    r.owed(&r.provider_sub, MEASURED + 1).await;
    r.serve_under(fuel(AMPLE_FUEL)).await;
    assert_eq!(r.inflight_keys().len(), MEASURED, "four are issued");
    assert_eq!(r.backlog_keys().len(), 1, "the fifth queues");
    r.run_nested_calls(MEASURED).await;

    r.serve_under(fuel(grant)).await;
    (r.backlog_keys().is_empty(), size as u64)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run by hand"]
async fn measure_hex() {
    let (small_grant, small) = least(|grant| draining_hex(HEX_SIZES[0], grant)).await;
    let (large_grant, large) = least(|grant| draining_hex(HEX_SIZES[1], grant)).await;
    let slope = (large_grant - small_grant) as f64 / (large - small) as f64;
    println!(
        "draining, one hex build: {small_grant} fuel at {small} file bytes, \
         {large_grant} at {large}: {slope:.1} fuel a byte"
    );
}
