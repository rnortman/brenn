//! Integration tests that drive the real `brenn-tool` binary against a fake
//! tool socket.

use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::Duration;

use std::io::Read as _;

use brenn_tool_proto::{
    FileDecl, MAX_REQUEST_BYTES, Request, Response, SOCKET_ENV, SaveHeader, SavePart, WireError,
    go_ahead_line,
};
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

/// A fake that answers a save with `header` and then writes `bytes`, which may
/// be fewer than the header announces.
fn serve_save(header: SaveHeader, bytes: Vec<u8>) -> Fake {
    serve_with(move |line, stream| {
        let request = Request::parse(&line).expect("the CLI sends a valid request");
        writeln!(stream, "{}", Response::Ok(header.to_result()).to_line()).unwrap();
        stream.write_all(&bytes).unwrap();
        request
    })
}

/// The entries of `dir`, by name.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn save_writes_the_bytes_and_replaces_an_existing_target() {
    let bytes: Vec<u8> = (0..200_000u32).map(|i| (i % 253) as u8).collect();
    let fake = serve_save(
        SaveHeader {
            size: bytes.len() as u64,
            content_type: Some("application/pdf".into()),
        },
        bytes.clone(),
    );
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("scan.pdf");
    std::fs::write(&out_path, b"older contents").unwrap();
    let out_arg = out_path.display().to_string();
    let out = run(
        &["save", "m-1", "--attachment", "scan.pdf", "--out", &out_arg],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0), "{}", out.stdout);
    assert_eq!(
        out.json(),
        json!({"path": out_arg, "size": bytes.len(), "content_type": "application/pdf"})
    );
    assert!(std::fs::read(&out_path).unwrap() == bytes);
    assert_eq!(entries(dir.path()), ["scan.pdf"]);
    assert_eq!(
        fake.request.join().unwrap(),
        Request::Save {
            message_id: "m-1".into(),
            part: SavePart::Attachment("scan.pdf".into()),
        }
    );
}

#[test]
fn save_of_a_body_asks_for_the_body() {
    let fake = serve_save(
        SaveHeader {
            size: 2,
            content_type: None,
        },
        b"hi".to_vec(),
    );
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("body.txt");
    let out = run(
        &[
            "save",
            "m-2",
            "--body",
            "--out",
            &out_path.display().to_string(),
        ],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0), "{}", out.stdout);
    assert_eq!(out.json()["content_type"], Value::Null);
    assert_eq!(std::fs::read(&out_path).unwrap(), b"hi");
    assert_eq!(
        fake.request.join().unwrap(),
        Request::Save {
            message_id: "m-2".into(),
            part: SavePart::Body,
        }
    );
}

#[test]
fn a_short_stream_is_a_protocol_error_and_leaves_no_file() {
    let fake = serve_save(
        SaveHeader {
            size: 10,
            content_type: None,
        },
        b"12345".to_vec(),
    );
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("out.bin");
    std::fs::write(&out_path, b"keep me").unwrap();
    let out = run(
        &[
            "save",
            "m",
            "--body",
            "--out",
            &out_path.display().to_string(),
        ],
        Some(&fake.socket),
        None,
    );
    fake.request.join().unwrap();
    assert_eq!(out.code, Some(2), "{}", out.stdout);
    assert_eq!(out.json()["error"], "protocol");
    assert!(
        out.json()["detail"]
            .as_str()
            .unwrap()
            .contains("after 5 of 10"),
        "{}",
        out.stdout
    );
    assert_eq!(std::fs::read(&out_path).unwrap(), b"keep me");
    assert_eq!(entries(dir.path()), ["out.bin"]);
}

#[test]
fn a_refused_save_prints_the_tools_error_and_leaves_no_file() {
    let fake = serve_once(Response::Err(WireError::ToolError {
        output: json!({"ok": false, "reason": "not_read", "error": "read it again"}),
    }));
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("out.bin");
    let out = run(
        &[
            "save",
            "m",
            "--attachment",
            "a",
            "--out",
            &out_path.display().to_string(),
        ],
        Some(&fake.socket),
        None,
    );
    fake.request.join().unwrap();
    assert_eq!(out.code, Some(1), "{}", out.stdout);
    assert_eq!(out.json()["reason"], "not_read");
    assert!(entries(dir.path()).is_empty());
}

#[test]
fn save_into_a_missing_directory_is_a_file_failure_before_connecting() {
    let out = run(
        &[
            "save",
            "m",
            "--body",
            "--out",
            "/nonexistent/brenn-tool/out.bin",
        ],
        Some(Path::new("/nonexistent/tool.sock")),
        None,
    );
    assert_eq!(out.code, Some(2), "{}", out.stdout);
    assert_eq!(out.json()["error"], "file");
}

#[test]
fn save_needs_a_part_and_an_out_path() {
    for argv in [
        &["save", "m", "--out", "f"][..],
        &["save", "m", "--body"],
        &["save", "m", "--body", "--attachment", "a", "--out", "f"],
    ] {
        let out = run(argv, None, None);
        assert_eq!(out.code, Some(2), "{argv:?}");
        assert_eq!(out.json()["error"], "usage", "{argv:?}");
    }
}

/// A fake that answers a `send_files` with the go-ahead, after `before_go`
/// has run on the declared files, then reads what the client sends until it
/// closes or has sent every declared byte, and answers `last` if it is still
/// there. Hands back the request and the bytes received.
fn serve_send(
    before_go: impl FnOnce(&[FileDecl]) + Send + 'static,
    last: Response,
) -> (Fake, std::sync::mpsc::Receiver<Vec<u8>>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let fake = serve_with(move |line, stream| {
        let request = Request::parse(&line).expect("the CLI sends a valid request");
        let Request::SendFiles { files, .. } = &request else {
            panic!("expected send_files, got {request:?}");
        };
        before_go(files);
        writeln!(stream, "{}", go_ahead_line()).unwrap();
        let declared: u64 = files.iter().map(|f| f.size).sum();
        let mut received = Vec::new();
        (&mut *stream)
            .take(declared)
            .read_to_end(&mut received)
            .unwrap();
        // A client that ended the connection early is gone by now.
        if let Err(e) = writeln!(stream, "{}", last.to_line()) {
            assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe, "{e}");
        }
        tx.send(received).unwrap();
        request
    });
    (fake, rx)
}

fn sent_answer() -> Response {
    Response::Ok(
        json!({"address": "brenn:x", "message_id": "m-9", "ok": true, "remaining_budget": 4}),
    )
}

#[test]
fn send_streams_each_file_after_the_go_ahead() {
    let dir = tempfile::tempdir().unwrap();
    let big: Vec<u8> = (0..150_000u32).map(|i| (i % 249) as u8).collect();
    let a = dir.path().join("a.bin");
    std::fs::write(&a, &big).unwrap();
    let b = dir.path().join("b.txt");
    std::fs::write(&b, b"notes").unwrap();
    let (fake, received) = serve_send(|_| {}, sent_answer());
    let out = run(
        &[
            "send",
            r#"{"to":"brenn:x"}"#,
            "--str",
            "body=hello",
            "--file",
            &format!("scan.bin={}", a.display()),
            "--file-type",
            "scan.bin=application/octet-stream",
            "--file",
            &format!("notes.txt={}", b.display()),
            "--file-type",
            "notes.txt=text/plain",
        ],
        Some(&fake.socket),
        None,
    );
    assert_eq!(out.code, Some(0), "{}", out.stdout);
    assert_eq!(out.json()["message_id"], "m-9");
    let mut expected = big.clone();
    expected.extend_from_slice(b"notes");
    assert!(received.recv().unwrap() == expected, "the bytes differ");
    assert_eq!(
        fake.request.join().unwrap(),
        Request::SendFiles {
            input: json!({"to": "brenn:x", "body": "hello"}),
            files: vec![
                FileDecl {
                    name: "scan.bin".into(),
                    content_type: "application/octet-stream".into(),
                    size: big.len() as u64,
                },
                FileDecl {
                    name: "notes.txt".into(),
                    content_type: "text/plain".into(),
                    size: 5,
                },
            ],
        }
    );
}

/// A refusal in place of the go-ahead is reported, and no byte is sent.
#[test]
fn a_refusal_before_the_go_ahead_sends_no_byte() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.bin");
    std::fs::write(&a, b"never sent").unwrap();
    let fake = serve_with(|line, stream| {
        writeln!(
            stream,
            "{}",
            Response::Err(WireError::ToolError {
                output: json!({"ok": false, "reason": "blob_too_large", "error": "too big"}),
            })
            .to_line()
        )
        .unwrap();
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).unwrap();
        assert!(rest.is_empty(), "{} bytes sent after a refusal", rest.len());
        Request::parse(&line).unwrap()
    });
    let out = run(
        &[
            "send",
            r#"{"to":"brenn:x","body":"b"}"#,
            "--file",
            &format!("a={}", a.display()),
            "--file-type",
            "a=application/octet-stream",
        ],
        Some(&fake.socket),
        None,
    );
    fake.request.join().unwrap();
    assert_eq!(out.code, Some(1), "{}", out.stdout);
    assert_eq!(out.json()["reason"], "blob_too_large");
}

/// A file that grows or shrinks between its declaration and its bytes ends
/// the connection with its last piece unsent.
#[test]
fn a_file_that_changes_size_ends_the_connection() {
    for grow in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        std::fs::write(&a, b"0123456789").unwrap();
        let changed = a.clone();
        let (fake, received) = serve_send(
            move |_| {
                let bytes: &[u8] = if grow { b"0123456789AB" } else { b"0123" };
                std::fs::write(&changed, bytes).unwrap();
            },
            sent_answer(),
        );
        let out = run(
            &[
                "send",
                r#"{"to":"brenn:x","body":"b"}"#,
                "--file",
                &format!("a={}", a.display()),
                "--file-type",
                "a=application/octet-stream",
            ],
            Some(&fake.socket),
            None,
        );
        fake.request.join().unwrap();
        assert_eq!(out.code, Some(2), "grow={grow}: {}", out.stdout);
        assert_eq!(out.json()["error"], "file", "grow={grow}");
        assert!(
            out.json()["detail"]
                .as_str()
                .unwrap()
                .contains("changed size"),
            "{}",
            out.stdout
        );
        assert!(
            received.recv().unwrap().len() < 10,
            "grow={grow}: the declared size never arrived"
        );
    }
}

/// The server cutting a send short is reported with its own kind.
#[test]
fn a_transfer_the_server_aborts_exits_two_with_its_kind() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.bin");
    std::fs::write(&a, b"bytes").unwrap();
    let (fake, received) = serve_send(
        |_| {},
        Response::Err(WireError::TransferAborted {
            detail: "the transfer deadline passed; nothing was published".into(),
        }),
    );
    let out = run(
        &[
            "send",
            r#"{"to":"brenn:x","body":"b"}"#,
            "--file",
            &format!("a={}", a.display()),
            "--file-type",
            "a=text/plain",
        ],
        Some(&fake.socket),
        None,
    );
    fake.request.join().unwrap();
    received.recv().unwrap();
    assert_eq!(out.code, Some(2), "{}", out.stdout);
    assert_eq!(out.json()["error"], "transfer_aborted");
}

#[test]
fn send_argument_errors_are_usage_or_file_failures() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    std::fs::write(&a, b"x").unwrap();
    let file = format!("a={}", a.display());
    let socket = Some(Path::new("/nonexistent/tool.sock"));
    for argv in [
        &["send", r#"{"to":"brenn:x"}"#][..],
        &["send", "--file", &file],
        &["send", "--file", &file, "--file-type", "b=text/plain"],
    ] {
        let out = run(argv, socket, None);
        assert_eq!(out.code, Some(2), "{argv:?}");
        assert_eq!(out.json()["error"], "usage", "{argv:?}");
    }
    let out = run(
        &[
            "send",
            "--file",
            &format!("d={}", dir.path().display()),
            "--file-type",
            "d=text/plain",
        ],
        socket,
        None,
    );
    assert_eq!(out.code, Some(2), "{}", out.stdout);
    assert_eq!(out.json()["error"], "file");
}
