// The page host's `blobs` fixture, built on the guest SDK.
//
// Three things wake it, and it does one thing for each:
//
// - Every new text message on `in` is a script of read commands, separated by
//   newlines or `;`, run in order. Each command appends one result word to the
//   report's `results`.
// - Every new message on `feed` has each of its attachments opened and read
//   whole. The report's `arrivals` holds, per message, one word per attachment
//   in name order: `<name>:<len>:<FNV-1a 64 hex>`, or `<name>:<error word>`.
// - A sync call on `ask` runs the request's text as a script, as `in` does,
//   and answers the result words, one per line in command order, as a
//   `text/plain` reply. Those words are not in the report.
//
// Every script is logged at `info` with its command count.
//
// Every activation that returns ok publishes one JSON report, untyped, on
// `report`: `{"results": [...], "arrivals": [{"index": n, "attachments":
// [...]}], "sync": <the sync port or null>}`.
//
// Handles live in one numbered list for the script. A number stays valid after
// `drop`, which empties its slot.
//
// Commands (`<msg>` is `self`, the script's own message, or `<port>:<index>`
// for any message of this activation's windows, context first):
//
//   limits                         -> limits:<max-read>:<max-write>
//   list <msg>                     -> name/size/type,... as `Message::attachments` reads
//   open <msg> <name>              -> blob:<n> | <open-error>
//   open-raw <port> <index> <name> -> as `open`, through the raw binding, so
//                                     a port or index no message has can be named;
//                                     only `size` reads its handle
//   size <n> / type <n>            -> the handle's size / content type
//   read <n> <offset> <len>        -> hex:<bytes> | <read-error>
//   read-all <n>                   -> all:<len>:<FNV-1a 64 hex> | <read-error>
//   drop <n>                       -> ok
//   repeat <count> <command...>    -> <result>*<times>,... in first-seen order
//
// A command that is none of these, or that names a handle or message the
// script does not have, ends the activation with an error naming it.

use brenn_guest::bindings::brenn::processor0_3_0::blobs as raw;
use brenn_guest::blobs::{self, Blob, OpenError, ReadError};
use brenn_guest::serde_json::{Value, json};
use brenn_guest::{Activation, Body, Error, Message, Processor, log, publish};

/// The script commands' port.
const IN: &str = "in";
/// The port whose new messages have their attachments read on arrival.
const FEED: &str = "feed";
/// The sync port a caller asks on.
const ASK: &str = "ask";
/// The output port the report is published on.
const REPORT: &str = "report";

struct PageBlobsTest;

/// One handle the script holds: through the SDK or through the raw binding.
enum Held {
    Sdk(Blob),
    Raw(raw::Blob),
}

/// One script's handles.
#[derive(Default)]
struct Script {
    held: Vec<Option<Held>>,
}

fn open_word(e: &OpenError) -> String {
    match e {
        OpenError::NotPermitted => "not-permitted",
        OpenError::NotFound => "not-found",
        OpenError::QuotaExceeded => "quota-exceeded",
    }
    .to_string()
}

fn read_word(e: &ReadError) -> String {
    match e {
        ReadError::OutOfRange => "out-of-range",
        ReadError::TooLarge => "too-large",
        ReadError::StorageFailed(_) => "storage-failed",
        ReadError::QuotaExceeded => "quota-exceeded",
        ReadError::Unavailable => "unavailable",
    }
    .to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn number<T: core::str::FromStr>(word: Option<&str>, what: &str) -> Result<T, Error> {
    let word = word.ok_or_else(|| Error::failed(format!("missing {what}")))?;
    word.parse()
        .map_err(|_| Error::failed(format!("{what} {word:?} is not a number")))
}

fn word<'a>(word: Option<&'a str>, what: &str) -> Result<&'a str, Error> {
    word.ok_or_else(|| Error::failed(format!("missing {what}")))
}

/// The message `spec` names: `self`, or `<port>:<index>` in this activation.
fn message<'a>(
    activation: &'a Activation,
    this: &'a Message,
    spec: &str,
) -> Result<&'a Message, Error> {
    if spec == "self" {
        return Ok(this);
    }
    let (port, index) = spec
        .split_once(':')
        .ok_or_else(|| Error::failed(format!("message {spec:?} is not self or port:index")))?;
    let index: usize = number(Some(index), "message index")?;
    let window = activation
        .port_windows()
        .find(|w| w.port() == port)
        .ok_or_else(|| Error::failed(format!("no window on port {port:?}")))?;
    window
        .context_messages()
        .iter()
        .chain(window.new_messages())
        .nth(index)
        .ok_or_else(|| Error::failed(format!("no message {index} on port {port:?}")))
}

/// `<len>:<FNV-1a 64 hex>` of `blob` read whole through the SDK, or the
/// read-error word.
fn whole(blob: &Blob) -> Result<String, String> {
    blob.read_all()
        .map(|bytes| format!("{}:{:016x}", bytes.len(), fnv1a64(&bytes)))
        .map_err(|e| read_word(&e))
}

impl Script {
    fn push(&mut self, held: Held) -> String {
        self.held.push(Some(held));
        format!("blob:{}", self.held.len() - 1)
    }

    fn get(&self, n: usize) -> Result<&Held, Error> {
        self.held
            .get(n)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::failed(format!("no blob {n}")))
    }

    /// Handle `n`, which must have come from `open`.
    fn blob(&self, n: usize) -> Result<&Blob, Error> {
        match self.get(n)? {
            Held::Sdk(blob) => Ok(blob),
            Held::Raw(_) => Err(Error::failed(format!(
                "blob {n} came from open-raw: only size reads it"
            ))),
        }
    }

    /// Run every command of `text` and return their result words.
    fn run_text(
        &mut self,
        activation: &Activation,
        this: &Message,
        text: &str,
    ) -> Result<Vec<String>, Error> {
        let lines: Vec<&str> = text
            .split(['\n', ';'])
            .filter(|line| !line.trim().is_empty())
            .collect();
        log::info(format_args!(
            "page-blobs-test: script of {} commands on {}",
            lines.len(),
            this.port()
        ));
        lines
            .into_iter()
            .map(|line| self.command(activation, this, line))
            .collect()
    }

    /// Run one command and return its result word.
    fn command(
        &mut self,
        activation: &Activation,
        this: &Message,
        line: &str,
    ) -> Result<String, Error> {
        let mut words = line.split_whitespace();
        let verb = word(words.next(), "command")?;
        let result = match verb {
            "limits" => {
                let l = blobs::limits();
                format!("limits:{}:{}", l.max_read_bytes, l.max_write_bytes)
            }
            "list" => {
                let m = message(activation, this, word(words.next(), "message")?)?;
                m.attachments()?
                    .iter()
                    .map(|a| format!("{}/{}/{}", a.name(), a.size(), a.content_type()))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            "open" => {
                let m = message(activation, this, word(words.next(), "message")?)?;
                match blobs::open(m, word(words.next(), "name")?) {
                    Ok(blob) => self.push(Held::Sdk(blob)),
                    Err(e) => open_word(&e),
                }
            }
            "open-raw" => {
                let port = word(words.next(), "port")?;
                let index: u32 = number(words.next(), "index")?;
                let name = word(words.next(), "name")?;
                match raw::open(port, index, name) {
                    Ok(blob) => self.push(Held::Raw(blob)),
                    Err(raw::OpenError::NotPermitted) => "not-permitted".to_string(),
                    Err(raw::OpenError::NotFound) => "not-found".to_string(),
                    Err(raw::OpenError::QuotaExceeded) => "quota-exceeded".to_string(),
                }
            }
            "size" => match self.get(number(words.next(), "blob")?)? {
                Held::Sdk(blob) => blob.size().to_string(),
                Held::Raw(blob) => blob.size().to_string(),
            },
            "type" => self.blob(number(words.next(), "blob")?)?.content_type(),
            "read" => {
                let blob = self.blob(number(words.next(), "blob")?)?;
                let offset: u64 = number(words.next(), "offset")?;
                let len: u32 = number(words.next(), "len")?;
                match blob.read(offset, len) {
                    Ok(bytes) => format!("hex:{}", hex(&bytes)),
                    Err(e) => read_word(&e),
                }
            }
            "read-all" => match whole(self.blob(number(words.next(), "blob")?)?) {
                Ok(sum) => format!("all:{sum}"),
                Err(word) => word,
            },
            "drop" => {
                let n: usize = number(words.next(), "blob")?;
                self.blob(n)?;
                self.held[n] = None;
                "ok".to_string()
            }
            "repeat" => {
                let count: usize = number(words.next(), "count")?;
                let rest = words.by_ref().collect::<Vec<_>>().join(" ");
                let mut tally: Vec<(String, usize)> = Vec::new();
                for _ in 0..count {
                    let result = self.command(activation, this, &rest)?;
                    // A handle number differs per call; tally its kind.
                    let key = match result.split_once(':') {
                        Some(("blob", _)) => "blob".to_string(),
                        _ => result,
                    };
                    match tally.iter_mut().find(|(k, _)| *k == key) {
                        Some((_, n)) => *n += 1,
                        None => tally.push((key, 1)),
                    }
                }
                tally
                    .iter()
                    .map(|(k, n)| format!("{k}*{n}"))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            other => return Err(Error::failed(format!("unknown command {other:?}"))),
        };
        if let Some(extra) = words.next() {
            return Err(Error::failed(format!(
                "{verb}: unexpected argument {extra:?}"
            )));
        }
        Ok(result)
    }
}

/// One word per attachment of `message`, in name order: each read whole.
fn arrival(message: &Message) -> Result<Value, Error> {
    let words: Vec<String> = message
        .attachments()?
        .iter()
        .map(|a| {
            let name = a.name().as_str();
            match blobs::open(message, name) {
                Ok(blob) => match whole(&blob) {
                    Ok(sum) => format!("{name}:{sum}"),
                    Err(word) => format!("{name}:{word}"),
                },
                Err(e) => format!("{name}:{}", open_word(&e)),
            }
        })
        .collect();
    Ok(json!({ "index": message.index(), "attachments": words }))
}

impl Processor for PageBlobsTest {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        let mut results = Vec::new();
        let mut arrivals = Vec::new();
        let mut reply = None;
        let sync = activation.sync().map(str::to_owned);
        for window in activation.port_windows() {
            let port = window.port();
            if port == IN {
                for this in window.new_messages() {
                    let text = this.body().as_text()?;
                    results.extend(Script::default().run_text(&activation, this, text)?);
                }
            } else if port == FEED {
                for this in window.new_messages() {
                    arrivals.push(arrival(this)?);
                }
            }
        }
        if let Some(window) = activation.sync_window().filter(|w| w.port() == ASK) {
            let this = window
                .new_messages()
                .first()
                .ok_or_else(|| Error::failed("the ask window carries no request"))?;
            let text = this.body().as_text()?;
            let words = Script::default().run_text(&activation, this, text)?;
            reply = Some(Body::typed("text/plain", words.join("\n").into_bytes()));
        }
        let report = json!({ "results": results, "arrivals": arrivals, "sync": sync });
        publish(REPORT, &report.to_string())?;
        Ok(reply)
    }
}

brenn_guest::export_processor!(PageBlobsTest);
