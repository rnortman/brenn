// The `blobs` interface's test fixture, built on the guest SDK.
//
// Every new text message on `in` is a script: one command per line, run in
// order. Each command appends one result word to the activation's report, a
// JSON object `{"results": [...]}` published untyped on `report` after every
// other publish of the activation. A script that runs `fail` or `trap` ends
// the activation there and publishes no report.
//
// Handles live in two numbered lists for the activation: blobs (from `open`,
// `open-raw` and `seal`) and writers (from `create`). A number stays valid
// after `drop`, which empties its slot.
//
// Commands (`<msg>` is `self`, the script's own message, or `<port>:<index>`
// for any message of this activation's windows):
//
//   limits                         -> limits:<max-read>:<max-write>
//   list <msg>                     -> name/size/type,... as `Message::attachments` reads
//   open <msg> <name>              -> blob:<n> | <open-error>
//   open-raw <port> <index> <name> -> as `open`, through the raw binding, so
//                                     a port or index no message has can be named
//   size <n> / type <n>            -> the handle's size / content type
//   read <n> <offset> <len>        -> hex:<bytes> | <read-error>
//   read-all <n>                   -> all:<len>:<fnv1a64 hex> | <read-error>
//   drop <n>                       -> ok
//   create <content-type>          -> writer:<n> | <create-error>
//   write <w> <offset> <hex>       -> ok | <write-error>
//   fill <w> <offset> <len>        -> ok | <write-error>; byte i of the
//                                     pattern is (offset + i) * 31 + 7 mod 256
//   seal <w>                       -> blob:<n> | gap:<offset>:<len> | quota-exceeded
//   drop-writer <w>                -> ok
//   spent <write|seal|drop>        -> through the raw binding, which the SDK's
//                                     consuming `seal` cannot reach: create and
//                                     seal a writer, then call `write` or `seal`
//                                     on it again (the host traps) or drop it
//                                     (-> blob:<n>, the sealed blob)
//   publish <port> <text> [<name>=<n> ...] [@<deliver-after>]
//                                  -> ok | <publish-error>
//   publish-plain <port> <text>    -> ok | the error (ports.publish)
//   publish-deferred <port> <text> <at> -> ok | the error (ports.publish-deferred)
//   defer-cancel <port> <index>    -> ok | the error (ports.defer-cancel)
//   repeat <count> <command...>    -> <result>*<times>,... in first-seen order
//   fail                           -> the activation returns Err
//   trap                           -> the guest panics

use brenn_guest::bindings::brenn::processor0_3_0::blobs as raw;
use brenn_guest::blobs::{self, Blob, CreateError, OpenError, ReadError, SealError, WriteError};
use brenn_guest::{
    Activation, Body, Error, Message, Processor, PublishAttachmentsError, PublishOptions, log,
};

struct ProcessorBlobsTest;

/// One activation's handles and results.
#[derive(Default)]
struct Script {
    blobs: Vec<Option<Blob>>,
    raw_blobs: Vec<Option<raw::Blob>>,
    writers: Vec<Option<blobs::Writer>>,
    results: Vec<String>,
    /// The last pattern `fill` built, by `(offset, len)`, so a repeated fill
    /// does not rebuild a megabyte per call.
    pattern: Option<(u64, usize, Vec<u8>)>,
}

/// How a script ends early.
enum Stop {
    Fail,
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
        ReadError::OutOfRange => "out-of-range".to_string(),
        ReadError::TooLarge => "too-large".to_string(),
        ReadError::StorageFailed(_) => "storage-failed".to_string(),
        ReadError::QuotaExceeded => "quota-exceeded".to_string(),
    }
}

fn create_word(e: &CreateError) -> String {
    match e {
        CreateError::InvalidContentType(_) => "invalid-content-type",
        CreateError::BudgetExhausted => "budget-exhausted",
        CreateError::StorageFailed(_) => "storage-failed",
        CreateError::QuotaExceeded => "quota-exceeded",
    }
    .to_string()
}

fn write_word(e: &WriteError) -> String {
    match e {
        WriteError::TooLarge => "too-large",
        WriteError::BlobTooLarge => "blob-too-large",
        WriteError::BudgetExhausted => "budget-exhausted",
        WriteError::TooFragmented => "too-fragmented",
        WriteError::StorageFailed(_) => "storage-failed",
        WriteError::QuotaExceeded => "quota-exceeded",
    }
    .to_string()
}

fn publish_word(e: &PublishAttachmentsError) -> String {
    match e {
        PublishAttachmentsError::NotPermitted => "not-permitted",
        PublishAttachmentsError::InvalidPayload(_) => "invalid-payload",
        PublishAttachmentsError::InvalidBody(_) => "invalid-body",
        PublishAttachmentsError::TextOnlyChannel => "text-only-channel",
        PublishAttachmentsError::QuotaExceeded => "quota-exceeded",
        PublishAttachmentsError::InvalidAttachmentName(_) => "invalid-attachment-name",
        PublishAttachmentsError::DuplicateAttachmentName(_) => "duplicate-attachment-name",
        PublishAttachmentsError::TooManyAttachments => "too-many-attachments",
    }
    .to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Result<Vec<u8>, Error> {
    if !text.len().is_multiple_of(2) {
        return Err(Error::failed(format!("odd-length hex {text:?}")));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16)
                .map_err(|e| Error::failed(format!("hex {text:?}: {e}")))
        })
        .collect()
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

impl Script {
    fn push_blob(&mut self, blob: Blob) -> String {
        self.blobs.push(Some(blob));
        self.raw_blobs.push(None);
        format!("blob:{}", self.blobs.len() - 1)
    }

    fn blob(&self, n: usize) -> Result<&Blob, Error> {
        self.blobs
            .get(n)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::failed(format!("no blob {n}")))
    }

    fn writer(&self, n: usize) -> Result<&blobs::Writer, Error> {
        self.writers
            .get(n)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::failed(format!("no writer {n}")))
    }

    /// Build the `fill` pattern for `(offset, len)` unless it is the last one.
    fn build_pattern(&mut self, offset: u64, len: usize) {
        let fresh = !matches!(&self.pattern, Some((o, l, _)) if *o == offset && *l == len);
        if fresh {
            let bytes = (0..len as u64)
                .map(|i| ((offset + i).wrapping_mul(31).wrapping_add(7) % 256) as u8)
                .collect();
            self.pattern = Some((offset, len, bytes));
        }
    }

    /// Run one command and return its result word.
    fn run(&mut self, activation: &Activation, this: &Message, line: &str) -> Result<String, Stop> {
        self.command(activation, this, line).map_err(|e| {
            log::error(format_args!("blobs-test: {line:?}: {e:?}"));
            Stop::Fail
        })?
    }

    fn command(
        &mut self,
        activation: &Activation,
        this: &Message,
        line: &str,
    ) -> Result<Result<String, Stop>, Error> {
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
                    Ok(blob) => self.push_blob(blob),
                    Err(e) => open_word(&e),
                }
            }
            "open-raw" => {
                let port = word(words.next(), "port")?;
                let index: u32 = number(words.next(), "index")?;
                let name = word(words.next(), "name")?;
                match raw::open(port, index, name) {
                    Ok(blob) => {
                        self.blobs.push(None);
                        self.raw_blobs.push(Some(blob));
                        format!("blob:{}", self.blobs.len() - 1)
                    }
                    Err(raw::OpenError::NotPermitted) => "not-permitted".to_string(),
                    Err(raw::OpenError::NotFound) => "not-found".to_string(),
                    Err(raw::OpenError::QuotaExceeded) => "quota-exceeded".to_string(),
                }
            }
            "size" => {
                let n: usize = number(words.next(), "blob")?;
                match self.raw_blobs.get(n).and_then(Option::as_ref) {
                    Some(r) => r.size().to_string(),
                    None => self.blob(n)?.size().to_string(),
                }
            }
            "type" => {
                let n: usize = number(words.next(), "blob")?;
                self.blob(n)?.content_type()
            }
            "read" => {
                let n: usize = number(words.next(), "blob")?;
                let offset: u64 = number(words.next(), "offset")?;
                let len: u32 = number(words.next(), "len")?;
                match self.blob(n)?.read(offset, len) {
                    Ok(bytes) => format!("hex:{}", hex(&bytes)),
                    Err(e) => read_word(&e),
                }
            }
            "read-all" => {
                let n: usize = number(words.next(), "blob")?;
                match self.blob(n)?.read_all() {
                    Ok(bytes) => format!("all:{}:{:016x}", bytes.len(), fnv1a64(&bytes)),
                    Err(e) => read_word(&e),
                }
            }
            "drop" => {
                let n: usize = number(words.next(), "blob")?;
                self.blob(n)?;
                self.blobs[n] = None;
                "ok".to_string()
            }
            "create" => match blobs::create(word(words.next(), "content type")?) {
                Ok(w) => {
                    self.writers.push(Some(w));
                    format!("writer:{}", self.writers.len() - 1)
                }
                Err(e) => create_word(&e),
            },
            "write" => {
                let n: usize = number(words.next(), "writer")?;
                let offset: u64 = number(words.next(), "offset")?;
                let bytes = unhex(word(words.next(), "hex")?)?;
                match self.writer(n)?.write(offset, &bytes) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => write_word(&e),
                }
            }
            "fill" => {
                let n: usize = number(words.next(), "writer")?;
                let offset: u64 = number(words.next(), "offset")?;
                let len: usize = number(words.next(), "len")?;
                self.writer(n)?;
                self.build_pattern(offset, len);
                let bytes = &self.pattern.as_ref().expect("built above").2;
                match self.writer(n)?.write(offset, bytes) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => write_word(&e),
                }
            }
            "seal" => {
                let n: usize = number(words.next(), "writer")?;
                let w = self
                    .writers
                    .get_mut(n)
                    .and_then(Option::take)
                    .ok_or_else(|| Error::failed(format!("no writer {n}")))?;
                match w.seal() {
                    Ok(blob) => self.push_blob(blob),
                    Err(refused) => {
                        self.writers[n] = Some(refused.writer);
                        match refused.error {
                            SealError::Gap(g) => format!("gap:{}:{}", g.offset, g.len),
                            SealError::QuotaExceeded => "quota-exceeded".to_string(),
                        }
                    }
                }
            }
            "spent" => {
                let call = word(words.next(), "call")?;
                let w = raw::create("application/octet-stream")
                    .map_err(|e| Error::failed(format!("raw create: {e:?}")))?;
                let blob = w
                    .seal()
                    .map_err(|e| Error::failed(format!("raw seal: {e:?}")))?;
                self.blobs.push(None);
                self.raw_blobs.push(Some(blob));
                match call {
                    "write" => format!("{:?}", w.write(0, &[0])),
                    "seal" => format!("{:?}", w.seal().map(|_| ())),
                    "drop" => {
                        drop(w);
                        format!("blob:{}", self.blobs.len() - 1)
                    }
                    other => return Err(Error::failed(format!("spent {other:?}"))),
                }
            }
            "drop-writer" => {
                let n: usize = number(words.next(), "writer")?;
                self.writer(n)?;
                self.writers[n] = None;
                "ok".to_string()
            }
            "publish" => {
                let port = word(words.next(), "port")?;
                let text = word(words.next(), "text")?;
                let mut named: Vec<(&str, usize)> = Vec::new();
                let mut options = PublishOptions::default();
                for arg in words.by_ref() {
                    if let Some(at) = arg.strip_prefix('@') {
                        options.deliver_after = Some(number(Some(at), "deliver-after")?);
                    } else {
                        let (name, n) = arg.split_once('=').ok_or_else(|| {
                            Error::failed(format!("attachment {arg:?} is not name=blob"))
                        })?;
                        named.push((name, number(Some(n), "blob")?));
                    }
                }
                let mut attachments: Vec<(&str, &Blob)> = Vec::with_capacity(named.len());
                for (name, n) in named {
                    attachments.push((name, self.blob(n)?));
                }
                match brenn_guest::publish_with_attachments(port, text, &attachments, options) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => publish_word(&e),
                }
            }
            "publish-plain" => {
                let port = word(words.next(), "port")?;
                let text = word(words.next(), "text")?;
                match brenn_guest::publish(port, text) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => format!("{e:?}"),
                }
            }
            "publish-deferred" => {
                let port = word(words.next(), "port")?;
                let text = word(words.next(), "text")?;
                let at: u64 = number(words.next(), "deliver-after")?;
                match brenn_guest::publish_deferred(port, text, at) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => format!("{e:?}"),
                }
            }
            "defer-cancel" => {
                let port = word(words.next(), "port")?;
                let index: u32 = number(words.next(), "index")?;
                match brenn_guest::defer_cancel(port, index) {
                    Ok(()) => "ok".to_string(),
                    Err(e) => format!("{e:?}"),
                }
            }
            "repeat" => {
                let count: usize = number(words.next(), "count")?;
                let rest: Vec<&str> = words.by_ref().collect();
                let rest = rest.join(" ");
                let mut tally: Vec<(String, usize)> = Vec::new();
                for _ in 0..count {
                    let result = match self.command(activation, this, &rest)? {
                        Ok(r) => r,
                        Err(stop) => return Ok(Err(stop)),
                    };
                    // A handle number differs per call; tally its kind.
                    let key = match result.split_once(':') {
                        Some(("blob" | "writer", _)) => {
                            result.split(':').next().expect("split").to_string()
                        }
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
            "fail" => return Ok(Err(Stop::Fail)),
            "trap" => panic!("blobs-test: trap requested"),
            other => return Err(Error::failed(format!("unknown command {other:?}"))),
        };
        if let Some(extra) = words.next() {
            return Err(Error::failed(format!(
                "{verb}: unexpected argument {extra:?}"
            )));
        }
        Ok(Ok(result))
    }
}

impl Processor for ProcessorBlobsTest {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        let mut script = Script::default();
        let scripts: Vec<&Message> = activation
            .port_windows()
            .filter(|w| w.port() == "in")
            .flat_map(|w| w.new_messages())
            .collect();
        for this in scripts {
            let Ok(text) = this.body().as_text() else {
                continue;
            };
            log::info(format_args!(
                "blobs-test: script of {} lines",
                text.lines().count()
            ));
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                match script.run(&activation, this, line) {
                    Ok(result) => script.results.push(result),
                    Err(Stop::Fail) => {
                        return Err(Error::failed(format!("blobs-test: failed at {line:?}")));
                    }
                }
            }
        }
        let report = brenn_guest::serde_json::json!({ "results": script.results });
        brenn_guest::publish("report", &report.to_string())?;
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorBlobsTest);
