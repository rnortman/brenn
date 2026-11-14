//! `brenn-tool`: an agent's Brenn tools as commands.
//!
//! A proxy with no per-tool code. It assembles one request, sends it as one
//! line to the conversation's tool socket (named by `BRENN_TOOL_SOCKET`), reads
//! one line back, and prints exactly one JSON value on stdout:
//!
//! - exit 0: the tool's result;
//! - exit 1: the tool ran and failed; its own error JSON;
//! - exit 2: `{"error":<kind>,"detail":<text>}` for everything else — usage,
//!   no socket, connect failure, protocol error, timeout, and the server's
//!   refusals (`not_available`, `unknown_tool`, `invalid_args`, `busy`,
//!   `not_ready`, `invalid_request`).
//!
//! `--help` and `--version` print text, not JSON, and exit 0.
//!
//! The whole request — stdin and every file included — is assembled before
//! the connection opens: the server gives a connection a bounded time to send
//! its line.
//!
//! `save` and `send` are commands, not tools: they move bytes no tool call
//! can. `save` writes the bytes of an attachment or a message body to a file,
//! streamed after the server's response line, and never puts them on stdout;
//! the target is replaced only once every byte has arrived. `send` is
//! `BrennSend` with files attached: it declares each file's size, waits for
//! the server's go-ahead, and streams the files' bytes; a file that changes
//! size while it is sent ends the connection, and nothing is published.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use brenn_tool_proto::{
    FileDecl, MAX_REQUEST_BYTES, Request, Response, SOCKET_ENV, SaveHeader, SavePart,
    SendFilesReply, WireError,
};
use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

/// How long a request waits for its response unless `--timeout` says
/// otherwise: the ceiling a tool call has on the hook path.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// The buffer a transfer's bytes pass through between the file and the
/// socket.
const TRANSFER_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Parser)]
// TODO(brenn-tool-version): `version` reads `CARGO_PKG_VERSION`, which the
// Bazel build leaves at `0.0.0`.
#[command(
    name = "brenn-tool",
    version,
    about = "Run this agent's Brenn tools as commands",
    long_about = "Run this agent's Brenn tools as commands.\n\n\
        Every call runs in the conversation that owns this environment, with the \
        same permissions and policy as the same tool called as a tool. stdout \
        carries exactly one JSON value. Exit 0: the result. Exit 1: the tool's own \
        error JSON. Exit 2: {\"error\":<kind>,\"detail\":<text>} for everything else."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List every tool, with whether it is available as a command and, if not, why.
    List {
        /// Indent the output.
        #[arg(long)]
        pretty: bool,
    },
    /// Show one tool, including its input schema.
    Describe {
        /// The tool's name, as `list` prints it.
        tool: String,
        /// Indent the output.
        #[arg(long)]
        pretty: bool,
    },
    /// Run a tool.
    Call(CallArgs),
    /// Save an attachment, or a message's body, to a file. The message is
    /// named by the `message_id` this conversation was shown it with; the
    /// attachment by its name in that message. Prints {"path","size",
    /// "content_type"}.
    Save(SaveArgs),
    /// Publish a message, as `call BrennSend`, with files of this environment
    /// attached. Every `--file NAME=PATH` needs a `--file-type NAME=TYPE`;
    /// nothing is guessed from the file. Prints the `BrennSend` answer.
    Send(SendArgs),
}

#[derive(clap::Args)]
struct SendArgs {
    #[command(flatten)]
    input: InputArgs,
    /// Attach the regular file at PATH under NAME.
    #[arg(long = "file", value_name = "NAME=PATH", required = true)]
    file: Vec<String>,
    /// The content type of the file attached under NAME.
    #[arg(long = "file-type", value_name = "NAME=TYPE")]
    file_type: Vec<String>,
    /// Seconds the whole send may take.
    #[arg(long, value_name = "SECS", default_value_t = DEFAULT_TIMEOUT_SECS,
          value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Indent the output.
    #[arg(long)]
    pretty: bool,
}

#[derive(clap::Args)]
#[command(group = clap::ArgGroup::new("part").required(true).args(["attachment", "body"]))]
struct SaveArgs {
    /// The message's id, as shown in the message.
    message_id: String,
    /// Save the attachment of this name.
    #[arg(long, value_name = "NAME")]
    attachment: Option<String>,
    /// Save the message's body.
    #[arg(long)]
    body: bool,
    /// The file to write. It is replaced only once every byte has arrived; on
    /// failure it is left as it was.
    #[arg(long, value_name = "PATH")]
    out: PathBuf,
    /// Seconds the whole save may take.
    #[arg(long, value_name = "SECS", default_value_t = DEFAULT_TIMEOUT_SECS,
          value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Indent the output.
    #[arg(long)]
    pretty: bool,
}

impl SaveArgs {
    fn part(&self) -> SavePart {
        match &self.attachment {
            Some(name) => SavePart::Attachment(name.clone()),
            None => SavePart::Body,
        }
    }
}

#[derive(clap::Args)]
struct CallArgs {
    /// The tool's name, as `list` prints it.
    tool: String,
    #[command(flatten)]
    input: InputArgs,
    /// Seconds to wait for the response. On expiry the call is not cancelled:
    /// it may still complete on the server.
    #[arg(long, value_name = "SECS", default_value_t = DEFAULT_TIMEOUT_SECS,
          value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Indent the output.
    #[arg(long)]
    pretty: bool,
}

/// The options that build a tool's input object.
#[derive(clap::Args)]
struct InputArgs {
    /// The input object: a JSON literal, `-` for stdin, or `@<path>` for a
    /// file. Absent: `{}`.
    #[arg(value_name = "BASE")]
    base: Option<String>,
    /// Add field K with a JSON value.
    #[arg(long = "set", value_name = "K=JSON")]
    set: Vec<String>,
    /// Add field K with a string value, taken verbatim.
    #[arg(long = "str", value_name = "K=TEXT")]
    str_value: Vec<String>,
    /// Add field K with the contents of a UTF-8 file as a string.
    #[arg(long = "str-file", value_name = "K=PATH")]
    str_file: Vec<String>,
    /// Add field K with the JSON parsed from a file.
    #[arg(long = "json-file", value_name = "K=PATH")]
    json_file: Vec<String>,
}

/// What the command prints and the status it exits with.
#[derive(Debug, PartialEq)]
struct Outcome {
    exit: u8,
    json: Value,
}

impl Outcome {
    fn result(json: Value) -> Self {
        Outcome { exit: 0, json }
    }

    fn tool_error(json: Value) -> Self {
        Outcome { exit: 1, json }
    }

    /// A failure of the CLI's own.
    fn failure(kind: ClientError, detail: impl Into<String>) -> Self {
        Self::refusal(kind.as_str(), detail.into())
    }

    /// What the server's error means for the caller: the tool's own output
    /// for `tool_error`, a refusal for every other kind.
    fn from_wire(error: WireError) -> Self {
        match error {
            WireError::ToolError { output } => Self::tool_error(output),
            refusal => {
                let detail = refusal
                    .detail()
                    .expect("every wire error but tool_error carries a detail")
                    .to_string();
                Self::refusal(refusal.kind(), detail)
            }
        }
    }

    fn refusal(kind: &str, detail: String) -> Self {
        Outcome {
            exit: 2,
            json: json!({"error": kind, "detail": detail}),
        }
    }
}

/// The CLI's own failure kinds, beside the server's wire kinds. Scripts match
/// on these spellings.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ClientError {
    /// Bad arguments.
    Usage,
    /// `BRENN_TOOL_SOCKET` is unset.
    NoSocket,
    /// The socket could not be reached.
    Connect,
    /// The socket answered something that is not a response line.
    Protocol,
    /// No answer within `--timeout`.
    Timeout,
    /// A file could not be opened, read, written or renamed, or a file being
    /// sent changed size.
    File,
}

impl ClientError {
    fn as_str(self) -> &'static str {
        match self {
            ClientError::Usage => "usage",
            ClientError::NoSocket => "no_socket",
            ClientError::Connect => "connect",
            ClientError::Protocol => "protocol",
            ClientError::Timeout => "timeout",
            ClientError::File => "file",
        }
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => {
            return emit_text(&e.render().to_string());
        }
        Err(e) => {
            return emit(
                &Outcome::failure(ClientError::Usage, e.render().to_string().trim_end()),
                false,
            );
        }
    };
    let (outcome, pretty) = run(cli);
    emit(&outcome, pretty)
}

fn run(cli: Cli) -> (Outcome, bool) {
    let (request, timeout, pretty) = match cli.command {
        Command::Save(args) => return (save(&args), args.pretty),
        Command::Send(args) => return (send_files(&args, read_stdin), args.pretty),
        Command::List { pretty } => (Ok(Request::List), DEFAULT_TIMEOUT_SECS, pretty),
        Command::Describe { tool, pretty } => {
            (Ok(Request::Describe { tool }), DEFAULT_TIMEOUT_SECS, pretty)
        }
        Command::Call(args) => {
            let request = build_input(&args.input, read_stdin).map(|input| Request::Call {
                tool: args.tool.clone(),
                input,
            });
            (request, args.timeout, args.pretty)
        }
    };
    let outcome = match request {
        Ok(request) => send(&request, Duration::from_secs(timeout)),
        Err(failure) => failure,
    };
    (outcome, pretty)
}

/// The request as its line, or the refusal of a line over the socket's cap.
fn request_line(request: &Request) -> Result<String, Outcome> {
    let line = request.to_line();
    if line.len() > MAX_REQUEST_BYTES {
        return Err(Outcome::from_wire(WireError::InvalidRequest {
            detail: format!(
                "the request is {} bytes; the socket accepts at most {MAX_REQUEST_BYTES}",
                line.len()
            ),
        }));
    }
    Ok(line)
}

/// Send a request to the socket `BRENN_TOOL_SOCKET` names and map the answer.
fn send(request: &Request, timeout: Duration) -> Outcome {
    let line = match request_line(request) {
        Ok(line) => line,
        Err(failure) => return failure,
    };
    let socket = match socket_path() {
        Ok(socket) => socket,
        Err(failure) => return failure,
    };
    let response = runtime().block_on(async {
        match tokio::time::timeout(timeout, exchange(&socket, &line)).await {
            Ok(response) => response,
            Err(_) => Err(Outcome::failure(
                ClientError::Timeout,
                format!(
                    "no response within {} s; the call was not cancelled and may still \
                     complete",
                    timeout.as_secs()
                ),
            )),
        }
    });
    match response {
        Ok(bytes) => answer(&bytes),
        Err(failure) => failure,
    }
}

/// The socket `BRENN_TOOL_SOCKET` names.
fn socket_path() -> Result<PathBuf, Outcome> {
    match std::env::var_os(SOCKET_ENV) {
        Some(socket) => Ok(PathBuf::from(socket)),
        None => Err(Outcome::failure(
            ClientError::NoSocket,
            format!(
                "{SOCKET_ENV} is not set; brenn-tool only works inside a Brenn agent's environment"
            ),
        )),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("a current-thread runtime builds")
}

/// Connect and send one request line; the write half stays open for bytes
/// that follow the line.
async fn connect_and_send(
    socket: &Path,
    line: &str,
) -> Result<
    (
        BufReader<tokio::net::unix::OwnedReadHalf>,
        tokio::net::unix::OwnedWriteHalf,
        Option<std::io::Error>,
    ),
    Outcome,
> {
    let stream = UnixStream::connect(socket).await.map_err(|e| {
        Outcome::failure(
            ClientError::Connect,
            format!("cannot connect to {}: {e}", socket.display()),
        )
    })?;
    let (read_half, mut write_half) = stream.into_split();
    let mut payload = Vec::with_capacity(line.len() + 1);
    payload.extend_from_slice(line.as_bytes());
    payload.push(b'\n');
    let write_error = write_half.write_all(&payload).await.err();
    Ok((BufReader::new(read_half), write_half, write_error))
}

/// One response line (newline stripped) from `reader`, or the protocol
/// failure naming why there is none.
async fn read_response_line(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    write_error: Option<std::io::Error>,
) -> Result<Vec<u8>, Outcome> {
    let mut response = Vec::new();
    let read = reader.read_until(b'\n', &mut response).await;
    if read.is_ok() && response.last() == Some(&b'\n') {
        response.pop();
        return Ok(response);
    }
    let mut detail = match read {
        Ok(_) => "the server closed the connection without a complete response".to_string(),
        Err(e) => format!("reading the response failed: {e}"),
    };
    if let Some(e) = write_error {
        detail.push_str(&format!(" (sending the request failed: {e})"));
    }
    Err(Outcome::failure(ClientError::Protocol, detail))
}

/// One request line out, one response line back (newline stripped).
///
/// The response is read up to its newline, not to EOF: a server that refuses a
/// request before reading it closes with the request unread, and the peer then
/// sees a reset after the queued response. A failed write is reported only if
/// no response arrives, for the same reason.
async fn exchange(socket: &Path, line: &str) -> Result<Vec<u8>, Outcome> {
    let (mut reader, _, write_error) = connect_and_send(socket, line).await?;
    read_response_line(&mut reader, write_error).await
}

/// `save`: stream one part of a message into a temporary file beside
/// `--out`, and rename it onto `--out` once exactly the announced size has
/// arrived.
fn save(args: &SaveArgs) -> Outcome {
    let request = Request::Save {
        message_id: args.message_id.clone(),
        part: args.part(),
    };
    let socket = match socket_path() {
        Ok(socket) => socket,
        Err(failure) => return failure,
    };
    let mut part_file = match PartFile::create(&args.out) {
        Ok(file) => file,
        Err(failure) => return failure,
    };
    let timeout = Duration::from_secs(args.timeout);
    let streamed = runtime().block_on(async {
        match tokio::time::timeout(timeout, stream_save(&socket, &request, &mut part_file)).await {
            Ok(streamed) => streamed,
            Err(_) => Err(Outcome::failure(
                ClientError::Timeout,
                format!(
                    "the save did not complete within {} s; {} was not changed",
                    timeout.as_secs(),
                    args.out.display()
                ),
            )),
        }
    });
    let header = match streamed {
        Ok(header) => header,
        Err(failure) => return failure,
    };
    if let Err(failure) = part_file.persist() {
        return failure;
    }
    Outcome::result(json!({
        "path": args.out.display().to_string(),
        "size": header.size,
        "content_type": header.content_type,
    }))
}

/// Send the save request, read its header, and copy exactly the announced
/// bytes into `file`.
async fn stream_save(
    socket: &Path,
    request: &Request,
    file: &mut PartFile,
) -> Result<SaveHeader, Outcome> {
    let (mut reader, _, write_error) = connect_and_send(socket, &request.to_line()).await?;
    let line = read_response_line(&mut reader, write_error).await?;
    let result = match Response::parse(&line) {
        Err(e) => return Err(Outcome::failure(ClientError::Protocol, e)),
        Ok(Response::Err(error)) => return Err(Outcome::from_wire(error)),
        Ok(Response::Ok(result)) => result,
    };
    let header =
        SaveHeader::from_result(result).map_err(|e| Outcome::failure(ClientError::Protocol, e))?;
    let mut buf = vec![0u8; TRANSFER_BUFFER_BYTES];
    let mut received = 0u64;
    while received < header.size {
        let want = usize::try_from((header.size - received).min(TRANSFER_BUFFER_BYTES as u64))
            .expect("the buffer's length fits in usize");
        let n = reader.read(&mut buf[..want]).await.map_err(|e| {
            Outcome::failure(
                ClientError::Protocol,
                format!(
                    "reading the bytes failed after {received} of {}: {e}",
                    header.size
                ),
            )
        })?;
        if n == 0 {
            return Err(Outcome::failure(
                ClientError::Protocol,
                format!(
                    "the server closed the connection after {received} of {} bytes",
                    header.size
                ),
            ));
        }
        file.write(&buf[..n])?;
        received += n as u64;
    }
    Ok(header)
}

/// One file a `send` attaches, open and measured.
struct OutgoingFile {
    path: PathBuf,
    file: std::fs::File,
    decl: FileDecl,
}

/// `send`: the `BrennSend` input and the files, declared, then the files'
/// bytes after the server's go-ahead, then the `BrennSend` answer.
fn send_files(args: &SendArgs, stdin: impl FnOnce() -> std::io::Result<String>) -> Outcome {
    let input = match build_input(&args.input, stdin) {
        Ok(input) => input,
        Err(failure) => return failure,
    };
    let mut files = match open_files(&args.file, &args.file_type) {
        Ok(files) => files,
        Err(failure) => return failure,
    };
    let request = Request::SendFiles {
        input,
        files: files.iter().map(|f| f.decl.clone()).collect(),
    };
    let line = match request_line(&request) {
        Ok(line) => line,
        Err(failure) => return failure,
    };
    let socket = match socket_path() {
        Ok(socket) => socket,
        Err(failure) => return failure,
    };
    let timeout = Duration::from_secs(args.timeout);
    let answered = runtime().block_on(async {
        match tokio::time::timeout(timeout, stream_send(&socket, &line, &mut files)).await {
            Ok(answered) => answered,
            Err(_) => Err(Outcome::failure(
                ClientError::Timeout,
                format!(
                    "no answer within {} s; if every byte was sent, the message may still \
                     be published",
                    timeout.as_secs()
                ),
            )),
        }
    });
    match answered {
        Ok(line) => answer(&line),
        Err(failure) => failure,
    }
}

/// Pair each `--file NAME=PATH` with its `--file-type NAME=TYPE`, open the
/// file and measure it from the open descriptor. The path is checked to be a
/// regular file before the open too, since opening a FIFO blocks until a
/// writer appears.
fn open_files(file_specs: &[String], type_specs: &[String]) -> Result<Vec<OutgoingFile>, Outcome> {
    let usage = |detail: String| Outcome::failure(ClientError::Usage, detail);
    let mut types: Vec<(&str, &str)> = Vec::with_capacity(type_specs.len());
    for spec in type_specs {
        let (name, content_type) = split_field("--file-type", spec).map_err(usage)?;
        if types.iter().any(|(n, _)| *n == name) {
            return Err(usage(format!("--file-type {name} is given twice")));
        }
        types.push((name, content_type));
    }
    let mut files: Vec<OutgoingFile> = Vec::with_capacity(file_specs.len());
    for spec in file_specs {
        let (name, path) = split_field("--file", spec).map_err(usage)?;
        if files.iter().any(|f| f.decl.name == name) {
            return Err(usage(format!("--file {name} is given twice")));
        }
        let Some(&(_, content_type)) = types.iter().find(|(n, _)| *n == name) else {
            return Err(usage(format!(
                "--file {name} needs --file-type {name}=<content type>; nothing is guessed"
            )));
        };
        let file_error = |detail: String| Outcome::failure(ClientError::File, detail);
        let not_regular = || file_error(format!("--file {name}: {path} is not a regular file"));
        let named = std::fs::metadata(path)
            .map_err(|e| file_error(format!("--file {name}: cannot open {path}: {e}")))?;
        if !named.is_file() {
            return Err(not_regular());
        }
        let file = std::fs::File::open(path)
            .map_err(|e| file_error(format!("--file {name}: cannot open {path}: {e}")))?;
        let meta = file
            .metadata()
            .map_err(|e| file_error(format!("--file {name}: cannot stat {path}: {e}")))?;
        if !meta.is_file() {
            return Err(not_regular());
        }
        files.push(OutgoingFile {
            path: PathBuf::from(path),
            file,
            decl: FileDecl {
                name: name.to_string(),
                content_type: content_type.to_string(),
                size: meta.len(),
            },
        });
    }
    if let Some((name, _)) = types
        .iter()
        .find(|(n, _)| !files.iter().any(|f| f.decl.name == *n))
    {
        return Err(usage(format!("--file-type {name} names no --file")));
    }
    Ok(files)
}

/// Send the request line, wait for the go-ahead, stream every file, and
/// return the server's final line.
async fn stream_send(
    socket: &Path,
    line: &str,
    files: &mut [OutgoingFile],
) -> Result<Vec<u8>, Outcome> {
    let (mut reader, mut writer, write_error) = connect_and_send(socket, line).await?;
    let first = read_response_line(&mut reader, write_error).await?;
    match SendFilesReply::parse(&first) {
        Err(e) => return Err(Outcome::failure(ClientError::Protocol, e)),
        Ok(SendFilesReply::Refused(error)) => return Err(Outcome::from_wire(error)),
        Ok(SendFilesReply::GoAhead) => {}
    }
    let mut buf = vec![0u8; TRANSFER_BUFFER_BYTES];
    for file in files.iter_mut() {
        let sent = stream_file(file, &mut writer, &mut buf).await?;
        if let Err(e) = sent {
            // The server stopped taking bytes; its answer says why.
            return read_response_line(&mut reader, Some(e)).await;
        }
    }
    read_response_line(&mut reader, None).await
}

/// Write `file`'s declared bytes to the socket. The last piece goes only
/// once the file is at end-of-file at exactly its declared size, so a file
/// that grew or shrank is an `Err` here with its piece unsent. `Ok(Err)` is a
/// failed socket write.
async fn stream_file(
    file: &mut OutgoingFile,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    buf: &mut [u8],
) -> Result<Result<(), std::io::Error>, Outcome> {
    use std::io::Read as _;
    let size = file.decl.size;
    let name = &file.decl.name;
    let path = file.path.display();
    let changed = |sent: u64| {
        Outcome::failure(
            ClientError::File,
            format!(
                "--file {name}: {path} changed size while it was sent (declared {size} bytes, \
                 {sent} read); nothing was published"
            ),
        )
    };
    let read_error = |e: std::io::Error| {
        Outcome::failure(
            ClientError::File,
            format!("--file {name}: reading {path}: {e}"),
        )
    };
    let mut sent = 0u64;
    loop {
        let want = usize::try_from((size - sent).min(buf.len() as u64))
            .expect("the buffer's length fits in usize");
        let n = if want == 0 {
            0
        } else {
            file.file.read(&mut buf[..want]).map_err(read_error)?
        };
        if n == 0 && want > 0 {
            return Err(changed(sent));
        }
        if sent + n as u64 == size {
            let mut probe = [0u8; 1];
            if file.file.read(&mut probe).map_err(read_error)? != 0 {
                return Err(changed(size + 1));
            }
        }
        if let Err(e) = writer.write_all(&buf[..n]).await {
            return Ok(Err(e));
        }
        sent += n as u64;
        if sent == size {
            return Ok(Ok(()));
        }
    }
}

/// A file being written beside its target, removed when dropped unless it was
/// renamed onto the target.
struct PartFile {
    file: tempfile::NamedTempFile,
    target: PathBuf,
}

impl PartFile {
    /// Create a new file, exclusively, in `target`'s directory, named
    /// `.<target name>.<random>.part`. Its mode is what the umask leaves of
    /// `0666`, as for any file the agent creates.
    fn create(target: &Path) -> Result<PartFile, Outcome> {
        use std::os::unix::fs::PermissionsExt as _;
        let file_error = |detail: String| Outcome::failure(ClientError::File, detail);
        let name = target
            .file_name()
            .ok_or_else(|| file_error(format!("--out {} names no file", target.display())))?;
        let dir = match target.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let mut prefix = std::ffi::OsString::from(".");
        prefix.push(name);
        prefix.push(".");
        let file = tempfile::Builder::new()
            .prefix(&prefix)
            .suffix(".part")
            .permissions(std::fs::Permissions::from_mode(0o666))
            .tempfile_in(dir)
            .map_err(|e| {
                file_error(format!(
                    "cannot create a temporary file in {}: {e}",
                    dir.display()
                ))
            })?;
        Ok(PartFile {
            file,
            target: target.to_path_buf(),
        })
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), Outcome> {
        self.file.write_all(bytes).map_err(|e| {
            Outcome::failure(
                ClientError::File,
                format!("writing {}: {e}", self.file.path().display()),
            )
        })
    }

    /// Flush the file and rename it onto the target.
    fn persist(self) -> Result<(), Outcome> {
        let file_error = |detail: String| Outcome::failure(ClientError::File, detail);
        let path = self.file.path().to_path_buf();
        self.file
            .as_file()
            .sync_all()
            .map_err(|e| file_error(format!("flushing {}: {e}", path.display())))?;
        self.file.persist(&self.target).map_err(|e| {
            file_error(format!(
                "renaming {} onto {}: {}",
                path.display(),
                self.target.display(),
                e.error
            ))
        })?;
        Ok(())
    }
}

/// Map a response line to what the command prints.
fn answer(line: &[u8]) -> Outcome {
    match Response::parse(line) {
        Err(e) => Outcome::failure(ClientError::Protocol, e),
        Ok(Response::Ok(result)) => Outcome::result(result),
        Ok(Response::Err(error)) => Outcome::from_wire(error),
    }
}

fn read_stdin() -> std::io::Result<String> {
    std::io::read_to_string(std::io::stdin())
}

/// The call's input: `BASE`, then every field option added to it.
///
/// A key given twice — by two options, or by an option and `BASE` — is a usage
/// error, never an override. Fields need `BASE` to be an object; without
/// fields, `BASE` is sent as it is and the tool's schema judges it.
fn build_input(
    args: &InputArgs,
    stdin: impl FnOnce() -> std::io::Result<String>,
) -> Result<Value, Outcome> {
    let usage = |detail: String| Outcome::failure(ClientError::Usage, detail);
    let base = match args.base.as_deref() {
        None => Value::Object(Map::new()),
        Some("-") => {
            let text = stdin().map_err(|e| usage(format!("BASE `-`: reading stdin: {e}")))?;
            parse_json("BASE from stdin", &text).map_err(usage)?
        }
        Some(spec) => match spec.strip_prefix('@') {
            Some(path) => {
                let text = read_text(path).map_err(|e| usage(format!("BASE `{spec}`: {e}")))?;
                parse_json(&format!("BASE `{spec}`"), &text).map_err(usage)?
            }
            None => parse_json("BASE", spec).map_err(usage)?,
        },
    };

    let mut fields: Vec<(String, Value)> = Vec::new();
    for spec in &args.set {
        let (key, text) = split_field("--set", spec).map_err(usage)?;
        let value = parse_json(&format!("--set {key}"), text).map_err(usage)?;
        fields.push((key.to_string(), value));
    }
    for spec in &args.str_value {
        let (key, text) = split_field("--str", spec).map_err(usage)?;
        fields.push((key.to_string(), Value::String(text.to_string())));
    }
    for spec in &args.str_file {
        let (key, path) = split_field("--str-file", spec).map_err(usage)?;
        let text = read_text(path).map_err(|e| usage(format!("--str-file {key}: {e}")))?;
        fields.push((key.to_string(), Value::String(text)));
    }
    for spec in &args.json_file {
        let (key, path) = split_field("--json-file", spec).map_err(usage)?;
        let text = read_text(path).map_err(|e| usage(format!("--json-file {key}: {e}")))?;
        let value = parse_json(&format!("--json-file {key}"), &text).map_err(usage)?;
        fields.push((key.to_string(), value));
    }
    if fields.is_empty() {
        return Ok(base);
    }

    let Value::Object(mut input) = base else {
        return Err(usage(
            "BASE must be a JSON object when fields are added to it".to_string(),
        ));
    };
    for (key, value) in fields {
        if input.contains_key(&key) {
            return Err(usage(format!("field `{key}` is given twice")));
        }
        input.insert(key, value);
    }
    Ok(Value::Object(input))
}

/// `K=V` split at the first `=`; the key must not be empty.
fn split_field<'a>(option: &str, spec: &'a str) -> Result<(&'a str, &'a str), String> {
    match spec.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key, value)),
        _ => Err(format!("{option} expects K=VALUE, got `{spec}`")),
    }
}

fn parse_json(what: &str, text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("{what} is not valid JSON: {e}"))
}

/// A file's contents as text, refusing what is not UTF-8.
fn read_text(path: &str) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {path}: {e}"))?;
    String::from_utf8(bytes).map_err(|_| format!("{path} is not UTF-8"))
}

/// Print the outcome's JSON on stdout and exit with its status. A stdout that
/// cannot be written (a closed pipe) leaves nothing to report the failure on,
/// so it only changes the status.
fn emit(outcome: &Outcome, pretty: bool) -> ExitCode {
    let text = if pretty {
        serde_json::to_string_pretty(&outcome.json)
    } else {
        serde_json::to_string(&outcome.json)
    }
    .expect("a JSON value always serializes");
    match writeln!(std::io::stdout().lock(), "{text}") {
        Ok(()) => ExitCode::from(outcome.exit),
        Err(_) => ExitCode::from(2),
    }
}

fn emit_text(text: &str) -> ExitCode {
    match write!(std::io::stdout().lock(), "{text}") {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::from(2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The input options of `call` with `argv`.
    fn call(argv: &[&str]) -> InputArgs {
        let mut full = vec!["brenn-tool", "call"];
        full.extend_from_slice(argv);
        match Cli::try_parse_from(full).expect("valid argv").command {
            Command::Call(args) => args.input,
            _ => unreachable!("argv names `call`"),
        }
    }

    fn send_args(argv: &[&str]) -> SendArgs {
        let mut full = vec!["brenn-tool", "send"];
        full.extend_from_slice(argv);
        match Cli::try_parse_from(full).expect("valid argv").command {
            Command::Send(args) => args,
            _ => unreachable!("argv names `send`"),
        }
    }

    fn strings(specs: &[&str]) -> Vec<String> {
        specs.iter().map(|s| s.to_string()).collect()
    }

    fn no_stdin() -> std::io::Result<String> {
        panic!("stdin read without BASE `-`")
    }

    fn usage_detail(result: Result<Value, Outcome>) -> String {
        let outcome = result.expect_err("a usage error");
        assert_eq!(outcome.exit, 2);
        assert_eq!(outcome.json["error"], "usage");
        outcome.json["detail"].as_str().unwrap().to_string()
    }

    #[test]
    fn absent_base_is_an_empty_object() {
        assert_eq!(build_input(&call(&["T"]), no_stdin), Ok(json!({})));
    }

    #[test]
    fn base_literal_and_fields_merge() {
        let args = call(&[
            "T",
            r#"{"a":1}"#,
            "--set",
            "b=[true,null]",
            "--str",
            "c=x=y",
        ]);
        assert_eq!(
            build_input(&args, no_stdin),
            Ok(json!({"a": 1, "b": [true, null], "c": "x=y"}))
        );
    }

    #[test]
    fn base_from_stdin() {
        let args = call(&["T", "-", "--str", "k=v"]);
        assert_eq!(
            build_input(&args, || Ok(r#"{"a":"b"}"#.to_string())),
            Ok(json!({"a": "b", "k": "v"}))
        );
    }

    #[test]
    fn a_non_object_base_is_sent_as_is_without_fields() {
        assert_eq!(build_input(&call(&["T", "[1]"]), no_stdin), Ok(json!([1])));
    }

    #[test]
    fn fields_need_an_object_base() {
        let detail = usage_detail(build_input(&call(&["T", "[1]", "--str", "k=v"]), no_stdin));
        assert!(detail.contains("must be a JSON object"), "{detail}");
    }

    #[test]
    fn a_key_given_twice_is_a_usage_error() {
        for argv in [
            &["T", "--str", "k=a", "--set", "k=1"][..],
            &["T", "--str", "k=a", "--str", "k=b"],
            &["T", r#"{"k":0}"#, "--str", "k=a"],
        ] {
            let detail = usage_detail(build_input(&call(argv), no_stdin));
            assert!(detail.contains("`k` is given twice"), "{argv:?}: {detail}");
        }
    }

    #[test]
    fn malformed_fields_are_usage_errors_naming_the_option_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let not_json = dir.path().join("data.txt");
        std::fs::write(&not_json, "plain text").unwrap();
        let not_json_spec = format!("k={}", not_json.display());
        let cases: [(&[&str], &str); 9] = [
            (&["T", "--str", "novalue"], "--str expects K=VALUE"),
            (&["T", "--str", "=v"], "--str expects K=VALUE"),
            (&["T", "--set", "k=not json"], "--set k is not valid JSON"),
            (&["T", "{not json"], "BASE is not valid JSON"),
            (
                &["T", "@/nonexistent/brenn-tool-base.json"],
                "cannot read /nonexistent/brenn-tool-base.json",
            ),
            (
                &["T", "--str-file", "k=/nonexistent/brenn-tool-body"],
                "--str-file k: cannot read /nonexistent/brenn-tool-body",
            ),
            (
                &["T", "--json-file", "novalue"],
                "--json-file expects K=VALUE",
            ),
            (
                &["T", "--json-file", "k=/nonexistent/brenn-tool-data.json"],
                "--json-file k: cannot read /nonexistent/brenn-tool-data.json",
            ),
            (
                &["T", "--json-file", &not_json_spec],
                "--json-file k is not valid JSON",
            ),
        ];
        for (argv, expected) in cases {
            let detail = usage_detail(build_input(&call(argv), no_stdin));
            assert!(detail.contains(expected), "{argv:?}: {detail}");
        }
    }

    #[test]
    fn file_fields_read_their_files() {
        let dir = tempfile::tempdir().unwrap();
        let text = dir.path().join("body.txt");
        std::fs::write(&text, "line one\n\"quoted\"\n").unwrap();
        let data = dir.path().join("data.json");
        std::fs::write(&data, r#"{"n": 2}"#).unwrap();
        let args = call(&[
            "T",
            "--str-file",
            &format!("body={}", text.display()),
            "--json-file",
            &format!("data={}", data.display()),
        ]);
        assert_eq!(
            build_input(&args, no_stdin),
            Ok(json!({"body": "line one\n\"quoted\"\n", "data": {"n": 2}}))
        );
    }

    #[test]
    fn a_non_utf8_str_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::write(&bin, [0xff, 0xfe]).unwrap();
        let detail = usage_detail(build_input(
            &call(&["T", "--str-file", &format!("k={}", bin.display())]),
            no_stdin,
        ));
        assert!(detail.contains("not UTF-8"), "{detail}");
    }

    #[test]
    fn save_needs_exactly_one_part_and_an_out_path() {
        for argv in [
            &["brenn-tool", "save", "m", "--out", "f"][..],
            &[
                "brenn-tool",
                "save",
                "m",
                "--body",
                "--attachment",
                "a",
                "--out",
                "f",
            ],
            &["brenn-tool", "save", "m", "--body"],
            &["brenn-tool", "save", "--body", "--out", "f"],
        ] {
            assert!(Cli::try_parse_from(argv).is_err(), "{argv:?}");
        }
        let Command::Save(args) = Cli::try_parse_from([
            "brenn-tool",
            "save",
            "m",
            "--attachment",
            "a.pdf",
            "--out",
            "f",
        ])
        .unwrap()
        .command
        else {
            unreachable!("argv names `save`");
        };
        assert_eq!(args.part(), SavePart::Attachment("a.pdf".into()));
        let Command::Save(args) =
            Cli::try_parse_from(["brenn-tool", "save", "m", "--body", "--out", "f"])
                .unwrap()
                .command
        else {
            unreachable!("argv names `save`");
        };
        assert_eq!(args.part(), SavePart::Body);
    }

    #[test]
    fn a_part_file_is_removed_unless_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out.bin");
        let part = PartFile::create(&target).unwrap();
        let part_path = part.file.path().to_path_buf();
        assert!(part_path.exists());
        assert_eq!(part_path.parent(), Some(dir.path()));
        let part_name = part_path.file_name().unwrap().to_str().unwrap();
        assert!(
            part_name.starts_with(".out.bin.") && part_name.ends_with(".part"),
            "{part_name}"
        );
        drop(part);
        assert!(!part_path.exists());
        assert!(!target.exists());

        std::fs::write(&target, b"old").unwrap();
        let mut part = PartFile::create(&target).unwrap();
        part.write(b"new").unwrap();
        part.persist().unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// A saved file gets the mode any file the agent creates there gets, not
    /// an owner-only one.
    #[test]
    fn a_saved_file_has_the_mode_the_umask_gives() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("plain");
        std::fs::File::create(&plain).unwrap();
        let target = dir.path().join("saved");
        PartFile::create(&target).unwrap().persist().unwrap();
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&target), mode(&plain));
    }

    #[test]
    fn a_part_file_in_a_missing_directory_is_a_file_failure() {
        let failure = PartFile::create(Path::new("/nonexistent/brenn-tool/out.bin"))
            .err()
            .unwrap();
        assert_eq!(failure.exit, 2);
        assert_eq!(failure.json["error"], "file");
    }

    #[test]
    fn send_needs_a_file_and_builds_its_input_as_call_does() {
        assert!(Cli::try_parse_from(["brenn-tool", "send", r#"{"to":"brenn:x"}"#]).is_err());
        let args = send_args(&[
            r#"{"to":"brenn:x"}"#,
            "--str",
            "body=hi",
            "--file",
            "a.pdf=/tmp/a",
            "--file-type",
            "a.pdf=application/pdf",
        ]);
        assert_eq!(
            build_input(&args.input, no_stdin),
            Ok(json!({"to": "brenn:x", "body": "hi"}))
        );
        assert_eq!(args.file, ["a.pdf=/tmp/a"]);
        assert_eq!(args.file_type, ["a.pdf=application/pdf"]);
    }

    #[test]
    fn files_are_declared_with_their_type_and_size() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        std::fs::write(&a, b"12345").unwrap();
        let empty = dir.path().join("empty");
        std::fs::write(&empty, b"").unwrap();
        let files = open_files(
            &strings(&[
                &format!("a.bin={}", a.display()),
                &format!("e={}", empty.display()),
            ]),
            &strings(&["e=text/plain", "a.bin=application/octet-stream"]),
        )
        .unwrap();
        let decls: Vec<_> = files.iter().map(|f| f.decl.clone()).collect();
        assert_eq!(
            decls,
            [
                FileDecl {
                    name: "a.bin".into(),
                    content_type: "application/octet-stream".into(),
                    size: 5,
                },
                FileDecl {
                    name: "e".into(),
                    content_type: "text/plain".into(),
                    size: 0,
                },
            ]
        );
    }

    #[test]
    fn file_options_that_do_not_pair_are_usage_errors() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        std::fs::write(&a, b"x").unwrap();
        let file = format!("a={}", a.display());
        let cases: [(&[&str], &[&str], &str); 5] = [
            (&[&file], &[], "--file a needs --file-type a="),
            (&[&file], &["b=text/plain"], "--file a needs --file-type a="),
            (
                &[&file, &file],
                &["a=text/plain"],
                "--file a is given twice",
            ),
            (
                &[&file],
                &["a=text/plain", "a=text/csv"],
                "--file-type a is given twice",
            ),
            (
                &[&file],
                &["a=text/plain", "b=text/plain"],
                "--file-type b names no --file",
            ),
        ];
        for (files, types, expected) in cases {
            let failure = open_files(&strings(files), &strings(types)).err().unwrap();
            assert_eq!(failure.json["error"], "usage", "{expected}");
            let detail = failure.json["detail"].as_str().unwrap();
            assert!(detail.contains(expected), "{detail}");
        }
        let failure = open_files(&strings(&["novalue"]), &[]).err().unwrap();
        assert_eq!(failure.json["error"], "usage");
    }

    #[test]
    fn a_file_that_cannot_be_sent_is_a_file_failure() {
        let dir = tempfile::tempdir().unwrap();
        // A FIFO with no writer: opening it would block, so it must be refused
        // before the open.
        let fifo = dir.path().join("pipe");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success(), "mkfifo: {made}");
        for (spec, expected) in [
            (
                format!("d={}", dir.path().display()),
                "is not a regular file",
            ),
            (
                format!("d={}", dir.path().join("missing").display()),
                "cannot open",
            ),
            (format!("d={}", fifo.display()), "is not a regular file"),
        ] {
            let failure = open_files(&[spec], &strings(&["d=text/plain"]))
                .err()
                .unwrap();
            assert_eq!(failure.exit, 2);
            assert_eq!(failure.json["error"], "file");
            let detail = failure.json["detail"].as_str().unwrap();
            assert!(detail.contains(expected), "{detail}");
        }
    }

    #[test]
    fn a_zero_timeout_is_refused() {
        assert!(Cli::try_parse_from(["brenn-tool", "call", "T", "--timeout", "0"]).is_err());
    }

    #[test]
    fn responses_map_to_exit_codes() {
        assert_eq!(
            answer(br#"{"v":2,"ok":true,"result":{"a":1}}"#),
            Outcome::result(json!({"a": 1}))
        );
        assert_eq!(
            answer(br#"{"v":2,"ok":false,"error":{"kind":"tool_error","output":{"ok":false}}}"#),
            Outcome::tool_error(json!({"ok": false}))
        );
        assert_eq!(
            answer(br#"{"v":2,"ok":false,"error":{"kind":"busy","detail":"d"}}"#),
            Outcome {
                exit: 2,
                json: json!({"error": "busy", "detail": "d"}),
            }
        );
        let garbled = answer(b"nope");
        assert_eq!(garbled.exit, 2);
        assert_eq!(garbled.json["error"], "protocol");
    }

    /// The CLI's own kinds are the documented set and never spell a wire kind.
    #[test]
    fn client_error_kinds_are_the_documented_set() {
        let all = [
            ClientError::Usage,
            ClientError::NoSocket,
            ClientError::Connect,
            ClientError::Protocol,
            ClientError::Timeout,
            ClientError::File,
        ];
        assert_eq!(
            all.map(ClientError::as_str),
            [
                "usage",
                "no_socket",
                "connect",
                "protocol",
                "timeout",
                "file"
            ]
        );
        let d = || "d".to_string();
        for wire in [
            WireError::InvalidRequest { detail: d() },
            WireError::UnknownTool { detail: d() },
            WireError::NotAvailable { detail: d() },
            WireError::InvalidArgs { detail: d() },
            WireError::NotReady { detail: d() },
            WireError::Busy { detail: d() },
            WireError::ToolError { output: json!({}) },
            WireError::TransferAborted { detail: d() },
        ] {
            assert!(
                !all.map(ClientError::as_str).contains(&wire.kind()),
                "{}",
                wire.kind()
            );
        }
    }
}
