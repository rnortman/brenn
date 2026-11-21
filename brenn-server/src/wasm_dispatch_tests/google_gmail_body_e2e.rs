//! `GmailBody`, end to end, on the rig in [`super::google_workspace_rig`]:
//! the compiled `google-gmail-read` component asks `api-request` for Gmail's
//! `messages.get` answer as an attachment, reads it whole through the real
//! guest adapter, and completes with each form asked for as a blob of its
//! own. The rig's blob store has production limits and the component runs
//! under the production fuel and memory unless a case says otherwise.

use base64ct::{Base64UrlUnpadded, Encoding};
use brenn_envelope::AttachmentName;
use brenn_messaging_store::blob::{BlobLimits, BlobUsage};
use brenn_tool_registry::{HeldAttachments, ToolError, ToolOutput};
use brenn_wasm_dispatch::ActivationLimits;
use serde_json::{Value, json};

use super::google_gmail_attachment_e2e::{file_of, handed, serve_message};
use super::google_workspace_rig::{self as workspace, APP, Clauses, GMAIL_READ, RigOptions};
use super::kit_body_fuel_measure::{least_under, measure_all, narrow};
use super::provider_rig::fuel;
use super::stub_rig::{StubRig, file_bytes, production_blob_limits};
use super::*;

const MESSAGES: &str = "/gmail/v1/users/me/messages";
/// The instance's result inbox, where `api-request`'s answers rest.
const PROVIDER_RESULTS: &str = "brenn:tool-results/gmail-read";
/// `GmailBody`'s bound: the largest answer from Gmail it reads, in bytes.
const BODY_ANSWER_MAX: usize = 1024 * 1024;
/// The content types of the forms.
const TEXT_PLAIN: &str = "text/plain; charset=utf-8";
const TEXT_HTML: &str = "text/html; charset=utf-8";

/// The clause `GmailRead` and `GmailBody` need.
const CLAUSES: Clauses<'static> = &[("GET", "/gmail/v1/users/me/messages/*")];

async fn rig() -> StubRig {
    workspace::rig(&GMAIL_READ, CLAUSES).await
}

/// A leaf of `mime_type` in UTF-8 holding `text`.
fn leaf(part_id: &str, mime_type: &str, text: &str) -> Value {
    json!({
        "partId": part_id,
        "mimeType": mime_type,
        "filename": "",
        "headers": [
            { "name": "Content-Type", "value": format!("{mime_type}; charset=UTF-8") },
        ],
        "body": {
            "size": text.len(),
            "data": Base64UrlUnpadded::encode_string(text.as_bytes()),
        },
    })
}

/// A `multipart/<subtype>` of `parts`.
fn multipart(subtype: &str, parts: Vec<Value>) -> Value {
    json!({
        "partId": "",
        "mimeType": format!("multipart/{subtype}"),
        "filename": "",
        "body": { "size": 0 },
        "parts": parts,
    })
}

/// Message `id` as `messages.get` answers it with `payload`, its `snippet`
/// `snippet`.
fn message(id: &str, snippet: &str, payload: &Value) -> String {
    json!({
        "id": id,
        "threadId": format!("t-{id}"),
        "labelIds": ["INBOX"],
        "snippet": snippet,
        "sizeEstimate": 4096,
        "internalDate": "1793455509000",
        "payload": payload,
    })
    .to_string()
}

/// Message `id` whose answer is exactly `len` bytes: `payload_of(n)` is the
/// payload for `n` bytes of body text, and the snippet takes what the
/// body's base64url leaves.
fn message_of_len(id: &str, len: usize, payload_of: impl Fn(usize) -> Value) -> String {
    let overhead = message(id, "", &payload_of(0)).len();
    let room = len
        .checked_sub(overhead)
        .expect("the answer holds its frame");
    // Four characters of base64url carry three bytes. The body's `size`
    // grows by a digit or so as the body does, so the last few dozen bytes
    // of room are left to the snippet.
    let body = (room * 3 / 4).saturating_sub(32);
    let framed = message(id, "", &payload_of(body)).len();
    let snippet = "s".repeat(len.checked_sub(framed).expect("the body fits its room"));
    let answer = message(id, &snippet, &payload_of(body));
    assert_eq!(answer.len(), len);
    answer
}

/// The path `messages.get` of `id` reaches.
fn message_path(id: &str) -> String {
    format!("{MESSAGES}/{id}")
}

/// A leaf of `mime_type` declared in `charset`, holding `bytes`.
fn leaf_bytes(mime_type: &str, charset: &str, bytes: &[u8]) -> Value {
    json!({
        "partId": "",
        "mimeType": mime_type,
        "filename": "",
        "headers": [
            { "name": "Content-Type", "value": format!("{mime_type}; charset={charset}") },
        ],
        "body": {
            "size": bytes.len(),
            "data": Base64UrlUnpadded::encode_string(bytes),
        },
    })
}

/// Message `id` whose answer is exactly `len` bytes: the payload
/// `payload_of(n)` for the largest `n` whose answer fits, the snippet taking
/// what is left. The answer and that `n`.
fn filled(id: &str, len: usize, payload_of: fn(usize) -> Value) -> (String, usize) {
    let size = |n: usize| message(id, "", &payload_of(n)).len();
    const SPAN: usize = 1024;
    let (one, span) = (size(1), size(1 + SPAN));
    assert!(one <= len, "one unit of the shape fits {len} bytes");
    // The answer grows by about `(span - one) / SPAN` a unit; base64url's
    // groups of four and the body's `size` growing a digit make the
    // estimate off by a unit or two either way.
    let mut lo = 1 + (len - one) * SPAN / (span - one);
    while size(lo) > len {
        lo -= 1;
    }
    while size(lo + 1) <= len {
        lo += 1;
    }
    let payload = payload_of(lo);
    let framed = message(id, "", &payload).len();
    let answer = message(id, &"s".repeat(len - framed), &payload);
    assert_eq!(answer.len(), len);
    (answer, lo)
}

/// One form of a shape's message as it is expected to be answered.
enum Form {
    /// Attached, holding these bytes.
    Bytes(Vec<u8>),
    /// Absent, for this reason.
    Absent(&'static str),
}

/// `bytes` as answered: attached, or absent when empty.
fn form(bytes: impl Into<Vec<u8>>) -> Form {
    let bytes = bytes.into();
    if bytes.is_empty() {
        Form::Absent("empty")
    } else {
        Form::Bytes(bytes)
    }
}

/// What a shape's message is expected to answer: each form, and `text`'s
/// source when it is attached.
struct Expected {
    source: &'static str,
    text: Form,
    html: Form,
    rendered: Form,
}

/// An HTML-only message's forms: its markup decoded and its rendering,
/// which is also its text.
fn html_only(html: impl Into<Vec<u8>>, rendered: impl Into<Vec<u8>>) -> Expected {
    let rendered = rendered.into();
    Expected {
        source: "text/html",
        text: form(rendered.clone()),
        html: form(html),
        rendered: form(rendered),
    }
}

/// A plain-text message's forms: its text, and no HTML.
fn plain_only(text: impl Into<Vec<u8>>) -> Expected {
    Expected {
        source: "text/plain",
        text: form(text),
        html: Form::Absent("no HTML part"),
        rendered: Form::Absent("no HTML part"),
    }
}

/// A message with no text part at all.
fn no_text() -> Expected {
    Expected {
        source: "",
        text: Form::Absent("no text part"),
        html: Form::Absent("no HTML part"),
        rendered: Form::Absent("no HTML part"),
    }
}

/// `n` HTML parts each holding `a`, side by side.
fn html_parts(n: usize) -> Expected {
    Expected {
        source: "text/html",
        text: form(units("a", n, "\n\n")),
        html: form(units("a", n, "\n")),
        rendered: form(units("a", n, "\n\n")),
    }
}

/// A `messages.get` answer built by hand of `n` repetitions of one unit, and
/// what the tool answers for it: its forms, or `None` when its decoding notes
/// alone are over the result ceiling.
struct Shape {
    name: &'static str,
    payload: fn(usize) -> Value,
    expected: fn(usize) -> Option<Expected>,
}

/// `unit` `n` times, joined by `by`.
fn units(unit: &str, n: usize, by: &str) -> String {
    vec![unit; n].join(by)
}

const LINE: &str = "Lunch at noon on Friday, at the usual place by the river.";

/// Mail's markup: a style sheet, then rows of tables with attributes, a
/// comment, a link and references.
const MAIL_HEAD: &str = "<html><head><style>td { padding: 4px; }</style></head><body>";
const MAIL_ROW: &str = "<table class=\"row\" width=\"100%\"><tr><td style=\"color:#333\">\
                        <!-- row --><a href=\"https://example.com/m?a=1&amp;b=2\">Lunch &amp; \
                        dinner</a> at noon&nbsp;sharp</td></tr></table>";
const MAIL_ROW_TEXT: &str = "Lunch & dinner at noon\u{a0}sharp";
const MAIL_TAIL: &str = "</body></html>";

/// The Windows-1252 bytes that decode to three bytes of UTF-8, and what
/// each decodes to: 17 characters and the five bytes Windows-1252 leaves
/// undefined.
const CP1252_WIDE: [(u8, char); 22] = [
    (0x80, '\u{20ac}'),
    (0x81, '\u{fffd}'),
    (0x82, '\u{201a}'),
    (0x84, '\u{201e}'),
    (0x85, '\u{2026}'),
    (0x86, '\u{2020}'),
    (0x87, '\u{2021}'),
    (0x89, '\u{2030}'),
    (0x8b, '\u{2039}'),
    (0x8d, '\u{fffd}'),
    (0x8f, '\u{fffd}'),
    (0x90, '\u{fffd}'),
    (0x91, '\u{2018}'),
    (0x92, '\u{2019}'),
    (0x93, '\u{201c}'),
    (0x94, '\u{201d}'),
    (0x95, '\u{2022}'),
    (0x96, '\u{2013}'),
    (0x97, '\u{2014}'),
    (0x99, '\u{2122}'),
    (0x9b, '\u{203a}'),
    (0x9d, '\u{fffd}'),
];

/// `n` bytes of [`CP1252_WIDE`] in turn, inside a paragraph.
fn cp1252_wide(n: usize) -> Vec<u8> {
    let mut bytes = b"<p>".to_vec();
    bytes.extend((0..n).map(|i| CP1252_WIDE[i % CP1252_WIDE.len()].0));
    bytes.extend_from_slice(b"</p>");
    bytes
}

/// A paragraph declared UTF-8 holding `unit` `n` times.
fn invalid_utf8(unit: &[u8], n: usize) -> Value {
    let mut bytes = b"<p>".to_vec();
    for _ in 0..n {
        bytes.extend_from_slice(unit);
    }
    leaf_bytes("text/html", "UTF-8", &bytes)
}

/// [`invalid_utf8`]'s forms, `unit` decoding to `text`.
fn invalid_utf8_forms(text: &str, n: usize) -> Option<Expected> {
    let text = text.repeat(n);
    Some(html_only(format!("<p>{text}"), text))
}

/// `n` HTML parts each holding `a`, the `i`th declaring `charset(i)`.
fn html_parts_in(n: usize, charset: fn(usize) -> String) -> Value {
    let parts = (0..n)
        .map(|i| {
            json!({
                "mimeType": "text/html",
                "headers": [
                    { "name": "Content-Type", "value": format!("text/html; charset={}", charset(i)) },
                ],
                "body": { "data": "YQ" },
            })
        })
        .collect();
    multipart("mixed", parts)
}

/// Distinct charsets a message can name and stay under the result ceiling
/// with a note for each.
const CHARSETS_UNDER_THE_CEILING: usize = 2400;

/// A charset name as long as a note quotes, its bytes alike.
fn long_charset() -> String {
    "y".repeat(1000)
}

/// Every shape the bound is measured on: those mail carries, those whose
/// forms are largest, those dearest to render, those densest in what the
/// parse keeps, and those densest in decoding notes.
const SHAPES: &[Shape] = &[
    Shape {
        name: "plain text",
        payload: |n| leaf("", "text/plain", &units(LINE, n, "\n")),
        expected: |n| Some(plain_only(units(LINE, n, "\n"))),
    },
    Shape {
        name: "mail's HTML",
        payload: |n| {
            let html = format!("{MAIL_HEAD}{}{MAIL_TAIL}", MAIL_ROW.repeat(n));
            leaf("", "text/html", &html)
        },
        expected: |n| {
            let html = format!("{MAIL_HEAD}{}{MAIL_TAIL}", MAIL_ROW.repeat(n));
            Some(html_only(html, units(MAIL_ROW_TEXT, n, "\n\n")))
        },
    },
    Shape {
        name: "a plain and an HTML alternative",
        payload: |n| {
            multipart(
                "alternative",
                vec![
                    leaf("0", "text/plain", &units(LINE, n, "\n")),
                    leaf("1", "text/html", &format!("<p>{LINE}</p>").repeat(n)),
                ],
            )
        },
        expected: |n| {
            Some(Expected {
                source: "text/plain",
                text: form(units(LINE, n, "\n")),
                html: form(format!("<p>{LINE}</p>").repeat(n)),
                rendered: form(units(LINE, n, "\n\n")),
            })
        },
    },
    Shape {
        name: "Windows-1252 decoding to three bytes each",
        payload: |n| leaf_bytes("text/html", "windows-1252", &cp1252_wide(n)),
        expected: |n| {
            let text: String = (0..n)
                .map(|i| CP1252_WIDE[i % CP1252_WIDE.len()].1)
                .collect();
            Some(html_only(format!("<p>{text}</p>"), text))
        },
    },
    Shape {
        name: "UTF-8 of invalid bytes",
        payload: |n| invalid_utf8(b"\xff", n),
        expected: |n| invalid_utf8_forms("\u{fffd}", n),
    },
    Shape {
        name: "UTF-8 of invalid lead bytes",
        payload: |n| invalid_utf8(b"\xc2", n),
        expected: |n| invalid_utf8_forms("\u{fffd}", n),
    },
    Shape {
        name: "UTF-8 of invalid bytes between ASCII",
        payload: |n| invalid_utf8(b"\xffa", n),
        expected: |n| invalid_utf8_forms("\u{fffd}a", n),
    },
    Shape {
        name: "UTF-8 of invalid bytes between two-byte characters",
        payload: |n| invalid_utf8(b"\xff\xc3\xa9", n),
        expected: |n| invalid_utf8_forms("\u{fffd}é", n),
    },
    Shape {
        name: "an unknown charset",
        payload: |n| leaf_bytes("text/html", "koi8-r", &"<p>".repeat(n).into_bytes()),
        expected: |n| Some(html_only("<p>".repeat(n), "")),
    },
    Shape {
        name: "ampersands opening no reference",
        payload: |n| leaf("", "text/html", &format!("<p>{}</p>", "&".repeat(n))),
        expected: |n| {
            Some(html_only(
                format!("<p>{}</p>", "&".repeat(n)),
                "&".repeat(n),
            ))
        },
    },
    Shape {
        name: "unknown named references",
        payload: |n| leaf("", "text/html", &"&zz;".repeat(n)),
        expected: |n| Some(html_only("&zz;".repeat(n), "&zz;".repeat(n))),
    },
    Shape {
        name: "named references",
        payload: |n| leaf("", "text/html", &"&lt;".repeat(n)),
        expected: |n| Some(html_only("&lt;".repeat(n), "<".repeat(n))),
    },
    Shape {
        name: "numeric references",
        payload: |n| leaf("", "text/html", &"&#128512;".repeat(n)),
        expected: |n| Some(html_only("&#128512;".repeat(n), "\u{1f600}".repeat(n))),
    },
    Shape {
        name: "minimal tags",
        payload: |n| leaf("", "text/html", &"<p>a".repeat(n)),
        expected: |n| Some(html_only("<p>a".repeat(n), units("a", n, "\n\n"))),
    },
    Shape {
        name: "empty paragraphs",
        payload: |n| leaf("", "text/html", &"<p>".repeat(n)),
        expected: |n| Some(html_only("<p>".repeat(n), "")),
    },
    Shape {
        name: "unknown tags",
        payload: |n| leaf("", "text/html", &"<b>a".repeat(n)),
        expected: |n| Some(html_only("<b>a".repeat(n), "a".repeat(n))),
    },
    Shape {
        name: "breaks",
        payload: |n| leaf("", "text/html", &"<br>a".repeat(n)),
        expected: |n| Some(html_only("<br>a".repeat(n), units("a", n, "\n"))),
    },
    Shape {
        name: "comments",
        payload: |n| leaf("", "text/html", &"<!--a-->b".repeat(n)),
        expected: |n| Some(html_only("<!--a-->b".repeat(n), "b".repeat(n))),
    },
    Shape {
        name: "words",
        payload: |n| leaf("", "text/html", &"a ".repeat(n)),
        expected: |n| Some(html_only("a ".repeat(n), units("a", n, " "))),
    },
    Shape {
        name: "headers",
        payload: |n| {
            let mut headers = vec![json!({ "name": "Content-Type", "value": "text/plain" })];
            headers.extend(std::iter::repeat_n(json!({ "name": "a", "value": "b" }), n));
            json!({
                "mimeType": "text/plain",
                "headers": headers,
                "body": { "size": LINE.len(), "data": Base64UrlUnpadded::encode_string(LINE.as_bytes()) },
            })
        },
        expected: |_| Some(plain_only(LINE)),
    },
    Shape {
        name: "parts",
        payload: |n| {
            let part = json!({ "mimeType": "text/plain", "body": { "size": 1, "data": "YQ" } });
            multipart("mixed", vec![part; n])
        },
        expected: |n| Some(plain_only(units("a", n, "\n\n"))),
    },
    Shape {
        name: "HTML parts",
        payload: |n| {
            let part = json!({ "mimeType": "text/html", "body": { "size": 1, "data": "YQ" } });
            multipart("mixed", vec![part; n])
        },
        expected: |n| Some(html_parts(n)),
    },
    Shape {
        name: "HTML parts declaring a charset",
        payload: |n| html_parts_in(n, |_| "utf-8".to_string()),
        expected: |n| Some(html_parts(n)),
    },
    Shape {
        name: "a charset after parameters without `=`",
        payload: |n| {
            json!({
                "mimeType": "text/html",
                "headers": [
                    { "name": "Content-Type", "value": format!("text/html{}; charset=utf-8", ";".repeat(n)) },
                ],
                "body": { "size": 1, "data": "YQ" },
            })
        },
        expected: |_| Some(html_only("a", "a")),
    },
    Shape {
        name: "an alternative of parts",
        payload: |n| {
            let part = json!({ "mimeType": "text/plain", "body": { "size": 1, "data": "YQ" } });
            multipart("alternative", vec![part; n])
        },
        expected: |_| Some(plain_only("a")),
    },
    Shape {
        name: "bare parts",
        payload: |n| multipart("mixed", vec![json!({ "mimeType": "x" }); n]),
        expected: |_| Some(no_text()),
    },
    Shape {
        name: "empty text parts",
        payload: |n| multipart("mixed", vec![json!({ "mimeType": "text/plain" }); n]),
        expected: |_| Some(no_text()),
    },
    Shape {
        name: "distinct charsets under the ceiling",
        payload: |n| html_parts_in(n, |i| format!("x{}", i % CHARSETS_UNDER_THE_CEILING)),
        expected: |n| Some(html_parts(n)),
    },
    Shape {
        name: "one long charset",
        payload: |n| html_parts_in(n, |_| long_charset()),
        expected: |n| Some(html_parts(n)),
    },
    Shape {
        name: "long charsets alike in what a note quotes",
        payload: |n| html_parts_in(n, |i| format!("{}{i}", long_charset())),
        expected: |n| Some(html_parts(n)),
    },
    Shape {
        name: "distinct charsets",
        payload: |n| html_parts_in(n, |i| format!("x{i}")),
        expected: |_| None,
    },
    Shape {
        name: "childless multiparts under the ceiling",
        payload: |n| {
            let parts = (0..n)
                .map(|i| json!({ "mimeType": format!("multipart/x{}", i % CHARSETS_UNDER_THE_CEILING) }))
                .collect();
            multipart("mixed", parts)
        },
        expected: |_| Some(no_text()),
    },
    Shape {
        name: "distinct childless multiparts",
        payload: |n| {
            let parts = (0..n)
                .map(|i| json!({ "mimeType": format!("multipart/x{i}") }))
                .collect();
            multipart("mixed", parts)
        },
        expected: |_| None,
    },
];

/// Assert `output` answers `expected`'s forms for an answer of `answer_len`
/// bytes, none over 2.25 times the answer.
fn assert_shape(shape: &str, output: &ToolOutput, expected: &Expected, answer_len: usize) {
    let ceiling = answer_len * 9 / 4;
    let all = [
        ("text", &expected.text),
        ("html", &expected.html),
        ("rendered", &expected.rendered),
    ];
    let names: Vec<&str> = all
        .iter()
        .filter(|(_, form)| matches!(form, Form::Bytes(_)))
        .map(|(name, _)| *name)
        .collect();
    let mut forms = forms_of(output, &names).into_iter();
    for (name, form) in all {
        match form {
            Form::Bytes(bytes) => {
                let (_, _, got) = forms.next().expect("each attached form is read");
                assert!(got == *bytes, "{shape}: the {name} form");
                assert!(
                    got.len() <= ceiling,
                    "{shape}: `{name}` is {} bytes, over 2.25 times the answer",
                    got.len()
                );
            }
            Form::Absent(reason) => {
                assert_eq!(output.value[name], json!({ "absent": reason }), "{shape}");
            }
        }
    }
    if let Form::Bytes(_) = expected.text {
        assert_eq!(output.value["text"]["source"], expected.source, "{shape}");
    }
}

/// Every shape filled to an answer of exactly `len` bytes, asked for all
/// three forms on a rig whose provider activations run under `limits`:
/// each answers what is expected of it.
async fn every_shape_answers_at(len: usize, limits: Option<ActivationLimits>) {
    for shape in SHAPES {
        let (answer, n) = filled("m1", len, shape.payload);
        let r = workspace::rig_built(
            &GMAIL_READ,
            CLAUSES,
            RigOptions {
                limits,
                ..RigOptions::default()
            },
        )
        .await;
        r.answer(&message_path("m1"), answer);
        let answered = body(&r, "m1", &["text", "html", "rendered"]).await;
        match (shape.expected)(n) {
            Some(expected) => {
                let output = answered.unwrap_or_else(|e| panic!("{}: {e:?}", shape.name));
                assert_shape(shape.name, &output, &expected, len);
            }
            None => assert_eq!(
                answered.map(|output| output.value),
                Err(ToolError::Internal(
                    "the message's decoding notes alone are over the 262144-byte result ceiling"
                        .to_string()
                )),
                "{}",
                shape.name
            ),
        }
    }
}

/// The floor and the attachment term of the production grant for the
/// activation that reads an answer of `len` bytes: the minimum, and the rate
/// for each byte of attachment. It leaves out what the body term adds for the
/// result's own body and for the pending request's, so a share of it is no
/// more than that share of the production grant.
fn fuel_floor(len: usize) -> u64 {
    brenn_wasm::PROCESSOR_FUEL_MINIMUM + brenn_wasm::PROCESSOR_FUEL_PER_ATTACHMENT_BYTE * len as u64
}

/// The shape whose forms are largest: each 2.25 times its answer.
const LARGEST_FORMS: &str = "Windows-1252 decoding to three bytes each";

/// The guest memory the tool's own work may use: three quarters of the
/// guest's, the rest left to the activation's input windows.
const MEMORY_MARGIN: usize = brenn_wasm::PROCESSOR_MAX_MEMORY_BYTES / 4 * 3;

/// Call `GmailBody` for `forms` of `id` as alice; the whole output.
async fn body(r: &StubRig, id: &str, forms: &[&str]) -> Result<ToolOutput, ToolError> {
    r.call_running(
        "GmailBody",
        json!({ "id": id, "forms": forms }),
        HeldAttachments::none(),
        1,
    )
    .await
}

/// Each attachment of `output`: its name, content type and every byte, in
/// the order of `names`; panics unless those are all it carries.
fn forms_of(output: &ToolOutput, names: &[&str]) -> Vec<(String, String, Vec<u8>)> {
    assert_eq!(
        output.attachments.holds().len(),
        names.len(),
        "{:?}",
        output.attachments.refs()
    );
    names
        .iter()
        .map(|name| {
            let hold = output
                .attachments
                .hold(&AttachmentName::new(*name).unwrap())
                .unwrap_or_else(|| panic!("the answer carries `{name}`"));
            let size = usize::try_from(hold.reference().size).unwrap();
            let bytes = hold.read(0, size).expect("the form reads");
            (
                (*name).to_string(),
                hold.reference().content_type.clone(),
                bytes,
            )
        })
        .collect()
}

/// How many rows on the instance's result inbox carry a blob.
async fn rows_with_blobs(r: &StubRig) -> usize {
    testutils::retained_on(&r.messenger, PROVIDER_RESULTS)
        .await
        .iter()
        .filter(|(_, envelope)| !testutils::attachment_ids(envelope).is_empty())
        .count()
}

/// A message of `rows` lines as a plain and an HTML alternative, each line
/// one paragraph of the HTML: the rendering expected of the HTML, the plain
/// text, the HTML, and the payload.
fn alternative_of(rows: usize) -> (String, String, String, Value) {
    let lines: Vec<String> = (0..rows)
        .map(|row| format!("Lunch at noon, row {row:05}."))
        .collect();
    let plain = lines.join("\n");
    let html: String = lines.iter().map(|line| format!("<p>{line}</p>")).collect();
    let payload = multipart(
        "alternative",
        vec![
            leaf("0", "text/plain", &plain),
            leaf("1", "text/html", &html),
        ],
    );
    (lines.join("\n\n"), plain, html, payload)
}

/// A message `GmailRead` cannot answer the body of — its text over 60,000
/// characters, its answer over `api-request`'s default read limit and the
/// bus cap — is answered `text_omitted` there, and `GmailBody` answers its
/// three forms whole. The request is `messages.get` with `format=full`; the
/// encoded answer rests on the instance's result inbox, and the forms are the
/// caller's.
#[tokio::test(flavor = "multi_thread")]
async fn the_three_forms_are_answered_above_gmail_reads_caps() {
    let r = rig().await;
    let path = message_path("m1");
    let (rendered, plain, html, payload) = alternative_of(4_000);
    let answer = message("m1", "Lunch", &payload);
    assert!(plain.chars().count() > 60_000);
    assert!(answer.len() > 256 * 1024, "over the default read limit");

    r.answer(&path, answer.clone());
    let call = r.call_for_alice("GmailRead", json!({ "id": "m1" }));
    let request = r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    r.answer(
        &path,
        message("m1", "Lunch", &multipart("alternative", vec![])),
    );
    r.serve().await;
    r.run_nested().await;
    r.serve().await;
    let read = call.await.unwrap().expect("GmailRead answered");
    r.assert_finished(&request);
    assert_eq!(read["text"], Value::Null, "{read}");
    assert!(
        read["text_omitted"]
            .as_str()
            .is_some_and(|why| why.ends_with("; GmailBody answers the body as files")),
        "{read}"
    );

    r.answer(&path, answer.clone());
    let before = r.blob_usage();
    let seen = r.seen().len();
    let output = body(&r, "m1", &["rendered", "html", "text"])
        .await
        .expect("GmailBody answered");
    assert_eq!(
        output.value,
        json!({
            "id": "m1",
            "text": { "attachment": "text", "size": plain.len(), "source": "text/plain" },
            "html": { "attachment": "html", "size": html.len(), "parts": 1 },
            "rendered": { "attachment": "rendered", "size": rendered.len(), "parts": 1 },
        })
    );
    let forms = forms_of(&output, &["text", "html", "rendered"]);
    assert_eq!(
        forms
            .iter()
            .map(|(name, ct, _)| (name.as_str(), ct.as_str()))
            .collect::<Vec<_>>(),
        [
            ("text", TEXT_PLAIN),
            ("html", TEXT_HTML),
            ("rendered", TEXT_PLAIN)
        ]
    );
    assert!(
        forms[0].2 == plain.as_bytes(),
        "text is the plain part whole"
    );
    assert!(forms[1].2 == html.as_bytes(), "html is the markup as sent");
    assert!(
        forms[2].2 == rendered.as_bytes(),
        "rendered is the HTML as text"
    );

    let seen = &r.seen()[seen..];
    assert_eq!(
        seen,
        [(
            "GET".to_string(),
            path.clone(),
            workspace::google_query(&[("format", "full")])
        )]
    );
    let forms_len = (plain.len() + html.len() + rendered.len()) as u64;
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: before.sealed + 4,
            unsealed: 0,
            charged_bytes: before.charged_bytes + answer.len() as u64 + forms_len,
        }
    );
    assert_eq!(rows_with_blobs(&r).await, 1);
    drop(output);
    assert_eq!(
        r.blob_usage().charged_bytes,
        before.charged_bytes + answer.len() as u64,
        "the encoded answer still rests on the result inbox"
    );
}

/// An answer one byte over the bound is refused naming the bound and the
/// routes, before any of it is read: the answer rests and no form is
/// written.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_one_byte_over_the_bound_is_refused() {
    let r = rig().await;
    let answer = message_of_len("over", BODY_ANSWER_MAX + 1, |n| {
        leaf("", "text/plain", &"a".repeat(n))
    });
    r.answer(&message_path("over"), answer);

    let refused = body(&r, "over", &["text"]).await;
    let Err(ToolError::InvalidArgs(detail)) = refused else {
        panic!("expected invalid_args, got {:?}", refused.map(|o| o.value));
    };
    assert_eq!(
        detail,
        format!(
            "the message's answer from Gmail is {} bytes, over the {BODY_ANSWER_MAX} bytes \
             GmailBody reads; GmailRead answers its text cut, and a caller granted \
             api-request reads the whole message with `format=raw` and \
             `response_attachment` (docs/providers/google.md)",
            BODY_ANSWER_MAX + 1
        )
    );
    assert_eq!(
        r.blob_usage(),
        BlobUsage {
            sealed: 1,
            unsealed: 0,
            charged_bytes: (BODY_ANSWER_MAX + 1) as u64,
        },
        "only the answer was written"
    );
}

/// An answer over the host's largest blob is refused by `api-request`, and
/// `GmailBody` names the setting.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_over_the_largest_blob_is_refused_naming_it() {
    let max_blob_bytes = 64 * 1024;
    let r = workspace::rig_built(
        &GMAIL_READ,
        CLAUSES,
        RigOptions {
            blob_limits: Some(BlobLimits {
                max_blob_bytes,
                budget_bytes: production_blob_limits().budget_bytes,
            }),
            ..RigOptions::default()
        },
    )
    .await;
    let answer = message_of_len("big", 2 * max_blob_bytes as usize, |n| {
        leaf("", "text/plain", &"a".repeat(n))
    });
    r.answer(&message_path("big"), answer);

    let refused = body(&r, "big", &["text"]).await;
    let Err(ToolError::InvalidArgs(detail)) = refused else {
        panic!("expected invalid_args, got {:?}", refused.map(|o| o.value));
    };
    assert_eq!(
        detail,
        format!(
            "the message's answer from Gmail exceeds the host's largest blob \
             (max_blob_bytes, {max_blob_bytes} bytes); GmailRead answers its text cut"
        )
    );
    assert_eq!(r.blob_usage().sealed, 0, "nothing was written");
}

/// `GmailBody` called by a conversation on the hook path: the answer is
/// `{"result", "message_id", "attachments"}`, the forms shown by name, size
/// and type with no blob id, and each blob on the app's result inbox holds
/// its form.
#[tokio::test(flavor = "multi_thread")]
async fn a_conversation_is_handed_the_forms_without_a_blob_id() {
    let r = workspace::rig_built(
        &GMAIL_READ,
        CLAUSES,
        RigOptions {
            conversations: true,
            ..RigOptions::default()
        },
    )
    .await;
    let (_, plain, html, payload) = alternative_of(3);
    r.answer(&message_path("m1"), message("m1", "Lunch", &payload));
    let conversation = r.conversation(APP, "alice").await;

    let call = {
        let conversation = conversation.clone();
        tokio::spawn(async move {
            conversation
                .test_post_tool_use(
                    "GmailBody",
                    json!({ "id": "m1", "forms": ["text", "html"] }),
                )
                .await
        })
    };
    let request = r.requested_id().await;
    r.run(1).await;
    let answer = call.await.unwrap();
    r.assert_finished(&request);

    let keys: Vec<&String> = answer.as_object().expect("an object").keys().collect();
    assert_eq!(keys, ["attachments", "message_id", "result"], "{answer}");
    assert_eq!(
        answer["result"],
        json!({
            "id": "m1",
            "text": { "attachment": "text", "size": plain.len(), "source": "text/plain" },
            "html": { "attachment": "html", "size": html.len(), "parts": 1 },
        })
    );
    assert_eq!(
        answer["attachments"],
        json!({
            "text": { "size": plain.len(), "content_type": TEXT_PLAIN },
            "html": { "size": html.len(), "content_type": TEXT_HTML },
        })
    );
    let (_, got) = handed(&r, &answer, "text").await;
    assert!(got == plain.as_bytes(), "the handed text is the plain part");
    let (_, got) = handed(&r, &answer, "html").await;
    assert!(got == html.as_bytes(), "the handed html is the markup");
}

/// Every shape at the bound, asked for all three forms, answers what is
/// expected of it under the production fuel and memory.
#[tokio::test(flavor = "multi_thread")]
async fn each_shape_at_the_bound_answers_its_forms() {
    every_shape_answers_at(BODY_ANSWER_MAX, None).await;
}

/// Every shape at the bound answers what is expected of it on 1/1.4 of the
/// production fuel floor for its answer: the rest of the floor is headroom
/// for a message costlier than any shape here.
///
/// `BODY_ANSWER_MAX` is set where every shape leaves at least five percent
/// of this test's grant unspent. When it was set, the costliest shape was
/// "HTML parts", which left 42.2 percent of the grant. The figure is
/// `measure_body_bound`'s.
///
/// Fuel per byte moves with the guest's compiler, `serde_json`, the provider
/// kit and the allocator, so this test can fail after a change that never
/// touched `GmailBody`. A failure is answered by measuring the shapes' cost
/// again, then lowering `BODY_ANSWER_MAX` or cutting the cost; never by
/// raising this test's fuel or changing the 5/7. Whoever changes
/// `BODY_ANSWER_MAX` measures again and restates the figure here.
#[tokio::test(flavor = "multi_thread")]
async fn each_shape_at_the_bound_answers_inside_the_fuel_margin() {
    every_shape_answers_at(BODY_ANSWER_MAX, fuel(fuel_floor(BODY_ANSWER_MAX) * 5 / 7)).await;
}

/// Every shape at the bound answers what is expected of it in three
/// quarters of the guest's memory. Measured by `measure_body_bound`, the
/// largest peak at the bound is at most 8.69 MiB, of the shapes whose parts
/// carry nothing but a `mimeType`; one 128 KiB step higher, "bare parts"
/// needs 15.88 MiB. Those shapes are inputs the reader accepts, built by
/// hand; whether Gmail sends one is not established.
#[tokio::test(flavor = "multi_thread")]
async fn each_shape_at_the_bound_answers_inside_the_memory_margin() {
    every_shape_answers_at(
        BODY_ANSWER_MAX,
        Some(ActivationLimits::Memory(MEMORY_MARGIN)),
    )
    .await;
}

/// A body answer at the bound and a multi-megabyte `GmailAttachment` answer
/// delivered to one activation, under the production limits: both complete.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_answer_and_an_attachment_answer_complete_in_one_activation() {
    let r = workspace::rig(
        &GMAIL_READ,
        &[
            ("GET", "/gmail/v1/users/me/messages/*"),
            ("GET", "/gmail/v1/users/me/messages/*/attachments/*"),
        ],
    )
    .await;
    let file = file_bytes(8 * 1024 * 1024);
    serve_message(&r, "scan", 1, "1", &file);
    let shape = SHAPES
        .iter()
        .find(|shape| shape.name == LARGEST_FORMS)
        .expect("the shape is listed");
    let (answer, n) = filled("m1", BODY_ANSWER_MAX, shape.payload);
    r.answer(&message_path("m1"), answer);

    // The attachment's part-tree read is answered and its fetch issued.
    let fetch = r.call_for_alice_with(
        "GmailAttachment",
        json!({ "id": "scan", "part_id": "1" }),
        HeldAttachments::none(),
    );
    let fetch_request = r.requested_id().await;
    r.serve().await;
    r.run_nested().await;
    r.serve().await;
    // The body's read is issued beside it, and both are answered at once.
    let read = r.call_for_alice_with(
        "GmailBody",
        json!({ "id": "m1", "forms": ["text", "html", "rendered"] }),
        HeldAttachments::none(),
    );
    let read_request = r.requested_id().await;
    r.serve().await;
    r.owed(&r.executor_sub(), 2).await;
    r.executor.drain_step().await;
    r.owed(&r.provider_sub, 2).await;
    r.serve().await;

    let fetched = fetch.await.unwrap().expect("GmailAttachment answered");
    r.assert_finished(&fetch_request);
    assert!(file_of(&fetched).1 == file, "every byte of the attachment");
    let output = read.await.unwrap().expect("GmailBody answered");
    r.assert_finished(&read_request);
    let expected = (shape.expected)(n).expect("the shape answers its forms");
    assert_shape(shape.name, &output, &expected, BODY_ANSWER_MAX);
}

/// One `GmailBody` call for all three forms of `shape` filled to an answer
/// of `len` bytes, on a rig whose provider activations all run under
/// `limits`: whether it answered as the shape expects, an output or its
/// refusal on the result ceiling.
///
/// Each activation runs to its end inside the `serve` that starts it, so
/// whether it completed is read from the rig once that returns: the first
/// completed when its read of the message is owed to the executor, the
/// last when the request is finished. A trap on the body answer fails no
/// request and leaves its in-flight row, so the caller is awaited only
/// once the request is finished and its answer already given.
async fn completes(shape: &Shape, len: usize, limits: Option<ActivationLimits>) -> bool {
    let (answer, n) = filled("m1", len, shape.payload);
    let r = workspace::rig_built(
        &GMAIL_READ,
        CLAUSES,
        RigOptions {
            limits,
            ..RigOptions::default()
        },
    )
    .await;
    r.answer(&message_path("m1"), answer);
    let call = r.call_for_alice_with(
        "GmailBody",
        json!({ "id": "m1", "forms": ["text", "html", "rendered"] }),
        HeldAttachments::none(),
    );
    let id = r.requested_id().await;
    r.serve().await;
    let issued = !brenn_messaging::testutils::owed_everywhere(&r.messenger, &r.executor_sub())
        .await
        .is_empty();
    if issued {
        r.run_nested().await;
        r.serve().await;
    }
    if !issued || !r.finished(&id) {
        call.abort();
        return false;
    }
    match (call.await, (shape.expected)(n)) {
        (Ok(Ok(_)), Some(_)) => true,
        (Ok(Err(ToolError::Internal(detail))), None) if detail.contains("result ceiling") => true,
        (answer, _) => {
            println!(
                "{}: unexpected answer under {limits:?}: {answer:?}",
                shape.name
            );
            false
        }
    }
}

/// The size of a mebibyte, for the figures printed.
const MIB: f64 = 1024.0 * 1024.0;

/// `shape` at an answer of [`BODY_ANSWER_MAX`] bytes, printed as it is
/// found: the least fuel grant it completes on, to
/// [`PRECISION`](super::kit_body_fuel_measure::PRECISION), or `None` where
/// it does not complete on [`fuel_floor`]; and the least guest memory it
/// completes in under the production fuel, to a page, or `None` where the
/// production memory does not hold it.
async fn measure_shape(shape: &'static Shape) -> (&'static Shape, Option<u64>, Option<u64>) {
    let len = BODY_ANSWER_MAX;
    let floor = fuel_floor(len);
    let grant = floor * 5 / 7;
    let least_fuel = if completes(shape, len, fuel(floor)).await {
        Some(least_under(floor, |g| completes(shape, len, fuel(g))).await)
    } else {
        None
    };
    const PAGE: u64 = 64 * 1024;
    let most = brenn_wasm::PROCESSOR_MAX_MEMORY_BYTES as u64;
    let least_memory = if completes(shape, len, None).await {
        Some(
            narrow(0, most, PAGE, &|bytes| async move {
                let limits = Some(ActivationLimits::Memory(bytes as usize));
                (completes(shape, len, limits).await, 0)
            })
            .await,
        )
    } else {
        None
    };
    let fuel = least_fuel.map_or("does not complete on the fuel floor".to_string(), |f| {
        format!(
            "fuel {f}, {:.1} a byte, {:.4} of the fuel margin test's grant",
            f as f64 / len as f64,
            f as f64 / grant as f64
        )
    });
    let memory = least_memory.map_or(
        "does not complete in the production memory".to_string(),
        |m| format!("memory {m} bytes, {:.2} MiB", m as f64 / MIB),
    );
    println!("{}: {fuel}; {memory}", shape.name);
    (shape, least_fuel, least_memory)
}

/// The measured shapes run this many at a time.
const MEASURED_AT_ONCE: usize = 11;

/// Every shape at the bound, asked for all three forms: the least fuel grant
/// the call completes on, that grant a byte of answer and as a share of
/// [`each_shape_at_the_bound_answers_inside_the_fuel_margin`]'s grant, and
/// the least guest memory it completes in; then every shape that does not
/// complete on the fuel floor or in the production memory, the costliest
/// shape among the rest by each measure, the share of that test's grant the
/// costliest leaves unspent, and [`MEMORY_MARGIN`]. Each figure is the
/// least limit under which every provider activation of the call completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "a measurement: run by hand"]
async fn measure_body_bound() {
    let len = BODY_ANSWER_MAX;
    let grant = fuel_floor(len) * 5 / 7;
    println!("bound {len} bytes; the fuel margin test's grant {grant}");
    let results = measure_all(SHAPES.iter().collect(), MEASURED_AT_ONCE, measure_shape).await;
    for (shape, f, m) in &results {
        if f.is_none() {
            println!("does not complete on the fuel floor: {}", shape.name);
        }
        if m.is_none() {
            println!("does not complete in the production memory: {}", shape.name);
        }
    }
    if let Some((shape, Some(f), _)) = results.iter().max_by_key(|(_, f, _)| *f) {
        println!(
            "costliest in fuel: {}, {f}, {:.1} a byte, leaving {:.4} of the test's grant",
            shape.name,
            *f as f64 / len as f64,
            1.0 - *f as f64 / grant as f64
        );
    }
    if let Some((shape, _, Some(m))) = results.iter().max_by_key(|(_, _, m)| *m) {
        println!(
            "costliest in memory: {}, {m} bytes, {:.2} MiB",
            shape.name,
            *m as f64 / MIB
        );
    }
    println!(
        "memory margin: {MEMORY_MARGIN} bytes, {:.2} MiB",
        MEMORY_MARGIN as f64 / MIB
    );
}
