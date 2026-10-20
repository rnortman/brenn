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

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use brenn_tool_proto::{MAX_REQUEST_BYTES, Request, Response, SOCKET_ENV, WireError};
use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

/// How long a request waits for its response unless `--timeout` says
/// otherwise: the ceiling a tool call has on the hook path.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

#[derive(Parser)]
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
}

#[derive(clap::Args)]
struct CallArgs {
    /// The tool's name, as `list` prints it.
    tool: String,
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
    /// Seconds to wait for the response. On expiry the call is not cancelled:
    /// it may still complete on the server.
    #[arg(long, value_name = "SECS", default_value_t = DEFAULT_TIMEOUT_SECS,
          value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    /// Indent the output.
    #[arg(long)]
    pretty: bool,
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
}

impl ClientError {
    fn as_str(self) -> &'static str {
        match self {
            ClientError::Usage => "usage",
            ClientError::NoSocket => "no_socket",
            ClientError::Connect => "connect",
            ClientError::Protocol => "protocol",
            ClientError::Timeout => "timeout",
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
        Command::List { pretty } => (Ok(Request::List), DEFAULT_TIMEOUT_SECS, pretty),
        Command::Describe { tool, pretty } => {
            (Ok(Request::Describe { tool }), DEFAULT_TIMEOUT_SECS, pretty)
        }
        Command::Call(args) => {
            let request = build_input(&args, read_stdin).map(|input| Request::Call {
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

/// Send a request to the socket `BRENN_TOOL_SOCKET` names and map the answer.
fn send(request: &Request, timeout: Duration) -> Outcome {
    let line = request.to_line();
    if line.len() > MAX_REQUEST_BYTES {
        return Outcome::from_wire(WireError::InvalidRequest {
            detail: format!(
                "the request is {} bytes; the socket accepts at most {MAX_REQUEST_BYTES}",
                line.len()
            ),
        });
    }
    let Some(socket) = std::env::var_os(SOCKET_ENV) else {
        return Outcome::failure(
            ClientError::NoSocket,
            format!(
                "{SOCKET_ENV} is not set; brenn-tool only works inside a Brenn agent's environment"
            ),
        );
    };
    let socket = PathBuf::from(socket);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("a current-thread runtime builds");
    let response = runtime.block_on(async {
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

/// One request line out, one response line back (newline stripped).
///
/// The response is read up to its newline, not to EOF: a server that refuses a
/// request before reading it closes with the request unread, and the peer then
/// sees a reset after the queued response. A failed write is reported only if
/// no response arrives, for the same reason.
async fn exchange(socket: &Path, line: &str) -> Result<Vec<u8>, Outcome> {
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
    let mut reader = BufReader::new(read_half);
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
    args: &CallArgs,
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

    fn call(argv: &[&str]) -> CallArgs {
        let mut full = vec!["brenn-tool", "call"];
        full.extend_from_slice(argv);
        match Cli::try_parse_from(full).expect("valid argv").command {
            Command::Call(args) => args,
            _ => unreachable!("argv names `call`"),
        }
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
    fn a_zero_timeout_is_refused() {
        assert!(Cli::try_parse_from(["brenn-tool", "call", "T", "--timeout", "0"]).is_err());
    }

    #[test]
    fn responses_map_to_exit_codes() {
        assert_eq!(
            answer(br#"{"v":1,"ok":true,"result":{"a":1}}"#),
            Outcome::result(json!({"a": 1}))
        );
        assert_eq!(
            answer(br#"{"v":1,"ok":false,"error":{"kind":"tool_error","output":{"ok":false}}}"#),
            Outcome::tool_error(json!({"ok": false}))
        );
        assert_eq!(
            answer(br#"{"v":1,"ok":false,"error":{"kind":"busy","detail":"d"}}"#),
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
        ];
        assert_eq!(
            all.map(ClientError::as_str),
            ["usage", "no_socket", "connect", "protocol", "timeout"]
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
        ] {
            assert!(
                !all.map(ClientError::as_str).contains(&wire.kind()),
                "{}",
                wire.kind()
            );
        }
    }
}
