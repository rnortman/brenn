//! Integration tests that drive the real `brenn-tool` binary against a fake
//! tool socket.

use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::Duration;

use brenn_tool_proto::{MAX_REQUEST_BYTES, Request, Response, SOCKET_ENV, WireError};
use serde_json::{Value, json};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-tool");

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Run {
    fn json(&self) -> Value {
        serde_json::from_str(&self.stdout)
            .unwrap_or_else(|e| panic!("stdout is not one JSON value ({e}): {:?}", self.stdout))
    }
}

/// Run the binary with `BRENN_TOOL_SOCKET` set to `socket` (or unset).
fn run(args: &[&str], socket: Option<&Path>, stdin: Option<&str>) -> Run {
    let mut cmd = Command::new(BIN);
    cmd.args(args)
        .env_remove(SOCKET_ENV)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(socket) = socket {
        cmd.env(SOCKET_ENV, socket);
    }
    let mut child = cmd.spawn().expect("spawn brenn-tool");
    let mut child_stdin = child.stdin.take().unwrap();
    if let Some(text) = stdin {
        child_stdin.write_all(text.as_bytes()).unwrap();
    }
    drop(child_stdin);
    let out = child.wait_with_output().expect("wait for brenn-tool");
    Run {
        code: out.status.code(),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

/// A one-connection server on a tempdir socket. It reads one request line,
/// answers `response`, and hands the parsed request back through the handle.
struct Fake {
    _dir: tempfile::TempDir,
    socket: PathBuf,
    request: JoinHandle<Request>,
}

fn serve_once(response: Response) -> Fake {
    serve_with(move |line, stream| {
        let request = Request::parse(&line).expect("the CLI sends a valid request");
        writeln!(stream, "{}", response.to_line()).unwrap();
        request
    })
}

fn serve_with(
    handle: impl FnOnce(Vec<u8>, &mut std::os::unix::net::UnixStream) -> Request + Send + 'static,
) -> Fake {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("tool.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let request = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = Vec::new();
        BufReader::new(&mut stream)
            .read_until(b'\n', &mut line)
            .unwrap();
        assert_eq!(line.pop(), Some(b'\n'), "the request ends in a newline");
        handle(line, &mut stream)
    });
    Fake {
        _dir: dir,
        socket,
        request,
    }
}

fn call_input(fake: Fake, tool: &str) -> Value {
    match fake.request.join().unwrap() {
        Request::Call { tool: t, input } => {
            assert_eq!(t, tool);
            input
        }
        other => panic!("expected a call, got {other:?}"),
    }
}

#[test]
fn a_result_exits_zero_with_the_result() {
    let fake = serve_once(Response::Ok(json!({"message_id": "m-1"})));
    let out = run(
        &["call", "BrennSend", "--str", "to=brenn:x"],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0), "{}", out.stdout);
    assert_eq!(out.json(), json!({"message_id": "m-1"}));
    assert_eq!(out.stderr, "");
    assert_eq!(call_input(fake, "BrennSend"), json!({"to": "brenn:x"}));
}

#[test]
fn a_tool_error_exits_one_with_the_tools_own_json() {
    let fake = serve_once(Response::Err(WireError::ToolError {
        output: json!({"ok": false, "error": "budget exhausted"}),
    }));
    let out = run(&["call", "BrennSend"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(1));
    assert_eq!(
        out.json(),
        json!({"ok": false, "error": "budget exhausted"})
    );
    assert_eq!(call_input(fake, "BrennSend"), json!({}));
}

#[test]
fn a_refusal_exits_two_with_kind_and_detail() {
    let fake = serve_once(Response::Err(WireError::NotAvailable {
        detail: "requires_approval".into(),
    }));
    let out = run(&["call", "GitRepoRun"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(2));
    assert_eq!(
        out.json(),
        json!({"error": "not_available", "detail": "requires_approval"})
    );
}

#[test]
fn base_as_a_literal_from_stdin_and_from_a_file() {
    let fake = serve_once(Response::Ok(json!(null)));
    let out = run(
        &["call", "T", r#"{"a":[1,2]}"#, "--set", "b=3"],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0));
    assert_eq!(out.json(), Value::Null);
    assert_eq!(call_input(fake, "T"), json!({"a": [1, 2], "b": 3}));

    let fake = serve_once(Response::Ok(json!(null)));
    let out = run(
        &["call", "T", "-"],
        Some(&fake.socket),
        Some(r#"{"from":"stdin"}"#),
    );
    assert_eq!(out.code, Some(0));
    assert_eq!(call_input(fake, "T"), json!({"from": "stdin"}));

    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("base.json");
    std::fs::write(&base, r#"{"from":"file"}"#).unwrap();
    let fake = serve_once(Response::Ok(json!(null)));
    let out = run(
        &["call", "T", &format!("@{}", base.display())],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0));
    assert_eq!(call_input(fake, "T"), json!({"from": "file"}));
}

#[test]
fn str_file_round_trips_a_large_file_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let body = dir.path().join("scene.json");
    let mut text = String::new();
    let mut i = 0;
    while text.len() < 100 * 1024 {
        text.push_str(&format!(
            "{{\"frame\":{i},\"text\":\"tab\\there \\\"q\\\" ünï €\"}}\n"
        ));
        i += 1;
    }
    std::fs::write(&body, &text).unwrap();
    let fake = serve_once(Response::Ok(json!({"message_id": "m"})));
    let out = run(
        &[
            "call",
            "BrennSend",
            "--str",
            "to=brenn:x",
            "--str-file",
            &format!("body={}", body.display()),
        ],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0), "{}", out.stdout);
    let input = call_input(fake, "BrennSend");
    assert_eq!(input["body"].as_str(), Some(text.as_str()));
}

#[test]
fn list_and_describe_send_their_ops_and_pretty_indents() {
    let fake = serve_once(Response::Ok(json!([{"name": "A"}, {"name": "B"}])));
    let out = run(&["list", "--pretty"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(0));
    assert!(out.stdout.lines().count() > 3, "{}", out.stdout);
    assert_eq!(out.json(), json!([{"name": "A"}, {"name": "B"}]));
    assert_eq!(fake.request.join().unwrap(), Request::List);

    let fake = serve_once(Response::Ok(json!({"name": "BrennSend"})));
    let out = run(&["describe", "BrennSend"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(0));
    assert_eq!(out.stdout.lines().count(), 1);
    assert_eq!(
        fake.request.join().unwrap(),
        Request::Describe {
            tool: "BrennSend".into()
        }
    );
}

#[test]
fn a_duplicate_key_is_a_usage_error_before_connecting() {
    let out = run(
        &["call", "T", "--str", "k=a", "--str", "k=b"],
        Some(Path::new("/nonexistent/tool.sock")),
        None,
    );
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "usage");
}

#[test]
fn an_argument_error_is_usage_json_on_stdout() {
    let out = run(&["call"], None, None);
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "usage");
    assert_eq!(out.stderr, "");
}

#[test]
fn help_is_text_and_exits_zero() {
    let out = run(&["--help"], None, None);
    assert_eq!(out.code, Some(0));
    assert!(out.stdout.contains("call"), "{}", out.stdout);
}

#[test]
fn an_unset_socket_variable_is_no_socket() {
    let out = run(&["list"], None, None);
    assert_eq!(out.code, Some(2));
    assert_eq!(
        out.json(),
        json!({
            "error": "no_socket",
            "detail": "BRENN_TOOL_SOCKET is not set; brenn-tool only works inside a Brenn agent's environment"
        })
    );
}

#[test]
fn a_socket_nobody_serves_is_a_connect_failure() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(&["list"], Some(&dir.path().join("tool.sock")), None);
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "connect");
}

#[test]
fn no_response_within_the_timeout_exits_two() {
    let fake = serve_with(|line, _stream| {
        std::thread::sleep(Duration::from_secs(4));
        Request::parse(&line).unwrap()
    });
    let out = run(&["call", "T", "--timeout", "1"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "timeout");
}

#[test]
fn a_connection_closed_without_a_response_is_a_protocol_error() {
    let fake = serve_with(|line, _stream| Request::parse(&line).unwrap());
    let out = run(&["list"], Some(&fake.socket), None);
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "protocol");
}

/// A server that refuses before reading closes with the request unread; the
/// refusal still reaches the caller.
#[test]
fn busy_before_the_request_is_read_still_reports_busy() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("tool.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let busy = Response::Err(WireError::Busy {
            detail: "too many open requests".into(),
        });
        writeln!(stream, "{}", busy.to_line()).unwrap();
    });
    let big = json!({"k": "x".repeat(MAX_REQUEST_BYTES / 2)}).to_string();
    let out = run(&["call", "T", "-"], Some(&socket), Some(&big));
    server.join().unwrap();
    assert_eq!(out.code, Some(2), "{}", out.stdout);
    assert_eq!(
        out.json(),
        json!({"error": "busy", "detail": "too many open requests"})
    );
}

#[test]
fn a_request_over_the_cap_is_refused_without_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let body = dir.path().join("big.txt");
    std::fs::write(&body, "x".repeat(MAX_REQUEST_BYTES)).unwrap();
    let out = run(
        &["call", "T", "--str-file", &format!("k={}", body.display())],
        Some(Path::new("/nonexistent/tool.sock")),
        None,
    );
    assert_eq!(out.code, Some(2));
    assert_eq!(out.json()["error"], "invalid_request");
}
