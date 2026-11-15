"""The `brenn_bus` module against the real server, `//brenn:brenn`, started as
a subprocess from a config written into the test's temporary directory, and
through `tls_front` for `wss://`."""

import datetime
import os
import pathlib
import re
import signal
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from collections.abc import Callable

import brenn_bus

_BRENN = os.path.abspath(os.environ["BRENN_BIN"])
_BRENN_TOOL = os.path.abspath(os.environ["BRENN_TOOL_BIN"])
_NOOP_MCP = os.path.abspath(os.environ["NOOP_MCP"])
_TLS_FRONT = os.path.abspath(os.environ["TLS_FRONT_BIN"])

_LISTENING = re.compile(r"listening on 127\.0\.0\.1:(\d+)")
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_STARTUP = 30.0

TEXT = "ephemeral:bus.text"
BYTES = "ephemeral:bus.bytes"
OTHER_TEXT = "ephemeral:bus.other"
MISSING = "ephemeral:empty.nothing"
OUTSIDE_GRANT = "ephemeral:secret.x"
MAX_BODY = 4096
# Above one read window (the server's 256 KiB chunk times 4 reads in flight),
# so a blob can span windows, and small enough to refuse one past it cheaply.
MAX_BLOB = 3 * 1024 * 1024
CHUNK = 256 * 1024
WINDOW = 4 * CHUNK

TOKENS = {
    "alice": "alice-token-7f3a9c",
    "bob": "bob-token-2d81e4",
    "carol": "carol-token-91b5d0",
}
# Remotes granted subscribe only; every other remote may publish too.
SUBSCRIBE_ONLY = {"carol"}

_CONFIG = """\
server {{
  bind_address = "127.0.0.1:0";
  static_dir = "{root}/static";
  secure_cookies = false;
  public_url = "http://127.0.0.1:1";
}}
database {{ path = "{root}/brenn.db"; }}
logging {{ log_dir = "{root}/logs"; }}
claude_defaults {{ mcp_script_path = "{noop_mcp}"; }}
messaging {{ max_body_bytes = {max_body}; max_blob_bytes = {max_blob}; }}

uuid_pins {{
  "brenn:surface.index" = "4f8c915e-c61f-497e-940d-554a9aa8a266";
}}

channel surface_index at "brenn:surface.index" {{
  push_depth = 1;
  retain_depth = 1;
  standing_retain_depth = 1;
}}

agent Floor {{ name = "Floor"; description = "test floor"; icon = "x"; working_dir = "{root}/work"; }}
new floor: Floor;

channel text at "{text}" {{ push_depth = 8; retain_depth = 8; }}
channel other_text at "{other_text}" {{ push_depth = 8; retain_depth = 8; }}
channel bytes at "{bytes}" {{ push_depth = 8; retain_depth = 8; non_text = true; }}
{remotes}"""

_REMOTE = """
remote {slug} {{
  token_file = "{root}/{slug}.token";
  grants = [subscribe, publish];
  acl subscribe [
    prefix "ephemeral:bus." {{ push_depth = 8, retain_depth = 8 }},
    prefix "ephemeral:empty." {{ push_depth = 8, retain_depth = 8 }}
  ];
  acl publish [prefix "ephemeral:bus."];
}}
"""

_SUBSCRIBE_ONLY_REMOTE = """
remote {slug} {{
  token_file = "{root}/{slug}.token";
  grants = [subscribe];
  acl subscribe [prefix "ephemeral:bus." {{ push_depth = 8, retain_depth = 8 }}];
}}
"""


class _Output:
    """A child process's output, read on a thread, waited on for a line."""

    def __init__(self, process: subprocess.Popen[str], pattern: re.Pattern[str]) -> None:
        self.lines: list[str] = []
        self.match: re.Match[str] | None = None
        self._found = threading.Event()
        self._pattern = pattern
        self._reader = threading.Thread(target=self._read, args=(process,), daemon=True)
        self._reader.start()

    def _read(self, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        for raw in process.stdout:
            line = _ANSI.sub("", raw.rstrip("\n"))
            self.lines.append(line)
            if self.match is None:
                self.match = self._pattern.search(line)
                if self.match is not None:
                    self._found.set()
        self._found.set()

    def port(self, what: str) -> int:
        """The port the awaited line names; fails, printing the output, if it
        has not appeared within the startup bound."""
        self._found.wait(_STARTUP)
        if self.match is None:
            raise AssertionError(
                f"{what} did not report its port within {_STARTUP} s:\n" + "\n".join(self.lines)
            )
        return int(self.match.group(1))


def _stop(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


class Server:
    """One brenn server with remotes `alice` and `bob`, and `carol`, who may
    only subscribe, its state under a private directory."""

    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        for name in ("run", "work", "static"):
            (root / name).mkdir()
        # The server refuses a runtime directory others can enter.
        os.chmod(root / "run", 0o700)
        for slug, token in TOKENS.items():
            path = root / f"{slug}.token"
            path.write_text(token)
            os.chmod(path, 0o600)
        config = root / "brenn.brenn"
        config.write_text(
            _CONFIG.format(
                root=root,
                noop_mcp=_NOOP_MCP,
                max_body=MAX_BODY,
                max_blob=MAX_BLOB,
                text=TEXT,
                other_text=OTHER_TEXT,
                bytes=BYTES,
                remotes="".join(
                    (_SUBSCRIBE_ONLY_REMOTE if slug in SUBSCRIBE_ONLY else _REMOTE).format(
                        slug=slug, root=root
                    )
                    for slug in TOKENS
                ),
            )
        )
        self.process = subprocess.Popen(
            [_BRENN, "--config", str(config), "serve"],
            env={
                "XDG_RUNTIME_DIR": str(root / "run"),
                "PATH": os.path.dirname(_BRENN_TOOL),
                "HOME": str(root),
            },
            cwd=root,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.output = _Output(self.process, _LISTENING)
        try:
            self.port = self.output.port("brenn")
        except BaseException:
            _stop(self.process)
            raise

    def url(self, slug: str) -> str:
        return f"ws://127.0.0.1:{self.port}/remote/{slug}/ws"

    def kill(self) -> None:
        self.process.send_signal(signal.SIGKILL)
        self.process.wait()

    def stop(self) -> None:
        _stop(self.process)


class TlsFront:
    """`tls_front` relaying to `upstream`, its authority's PEM at `ca_file`.
    Its stderr is the test's, so a relay failure lands in the test log."""

    def __init__(self, upstream: int, ca_file: pathlib.Path) -> None:
        self.ca_file = ca_file
        self.process = subprocess.Popen(
            [_TLS_FRONT, str(upstream), str(ca_file)],
            env={},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            text=True,
        )
        self.output = _Output(self.process, _LISTENING)
        try:
            self.port = self.output.port("tls_front")
        except BaseException:
            _stop(self.process)
            raise

    def stop(self) -> None:
        _stop(self.process)


class ServerTestCase(unittest.TestCase):
    """A fresh server per test class; a test's clients close at its end."""

    server: Server

    @classmethod
    def setUpClass(cls) -> None:
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.server = Server(pathlib.Path(directory.name))
        cls.addClassCleanup(cls.server.stop)

    def attach(self, slug: str, **kwargs: object) -> brenn_bus.Client:
        arguments: dict[str, object] = {
            "ident": "brenn_bus_server_test",
            "timeout": 10.0,
            "token": TOKENS[slug],
        }
        arguments.update(kwargs)
        client = brenn_bus.attach(self.server.url(slug), **arguments)  # type: ignore[arg-type]
        self.addCleanup(client.close)
        return client

    def subscribe(self, client: brenn_bus.Client, channel: str) -> None:
        answer = client.subscribe(channel, push_depth=8, retain_depth=8, timeout=5.0)
        self.assertIsInstance(answer, brenn_bus.Subscribed)

    def publish_ok(self, client: brenn_bus.Client, channel: str, body: brenn_bus.Body) -> None:
        self.assertIsInstance(client.publish(channel, body, timeout=5.0), brenn_bus.Ok)

    def next_delivery(self, client: brenn_bus.Client) -> brenn_bus.Delivery:
        """The next event, required to be a delivery."""
        event = client.recv(timeout=5.0)
        self.assertIsInstance(event, brenn_bus.Delivery, f"next event is {event!r}")
        assert isinstance(event, brenn_bus.Delivery)
        return event


def _wait_for(condition: Callable[[], bool], bound: float) -> bool:
    deadline = time.monotonic() + bound
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


class SessionTest(ServerTestCase):
    def test_two_attachers_negotiate_v6_as_distinct_participants(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        facts = [alice.facts(), bob.facts()]
        for each in facts:
            self.assertEqual(each.protocol_version, 6)
            self.assertTrue(each.participant_id)
            self.assertEqual(each.max_body_bytes, MAX_BODY)
        self.assertNotEqual(facts[0].participant_id, facts[1].participant_id)
        self.assertNotEqual(facts[0].session_id, facts[1].session_id)
        match alice.status():
            case brenn_bus.Attached(brenn_bus.AttachmentFacts(6, participant, _, _)):
                self.assertEqual(participant, facts[0].participant_id)
            case status:
                self.fail(f"status is {status!r}")

    def test_a_token_file_attaches(self) -> None:
        path = self.server.root / "alice-client.token"
        path.write_text(TOKENS["alice"] + "\n")
        os.chmod(path, 0o600)
        client = self.attach("alice", token=None, token_file=path)
        self.assertEqual(client.facts().protocol_version, 6)

    def test_a_text_round_trip(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        before = datetime.datetime.now(datetime.UTC)
        self.publish_ok(alice, TEXT, brenn_bus.Body.text("hello, bob"))
        delivery = self.next_delivery(bob)
        self.assertEqual(delivery.channel, TEXT)
        self.assertEqual(delivery.sender, alice.facts().participant_id)
        self.assertEqual(delivery.body.as_text(), "hello, bob")
        self.assertIsNone(delivery.body.content_type)
        self.assertEqual(delivery.dropped, 0)
        self.assertEqual(delivery.urgency, brenn_bus.Urgency.NORMAL)
        self.assertIsNone(delivery.reply_to)
        self.assertTrue(delivery.message_id)
        self.assertEqual(delivery.publish_ts.utcoffset(), datetime.timedelta(0))
        self.assertLess(abs(delivery.publish_ts - before), datetime.timedelta(seconds=30))

    def test_bytes_with_a_content_type_round_trip(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, BYTES)
        data = bytes(range(256))
        self.publish_ok(alice, BYTES, brenn_bus.Body(data, "application/octet-stream"))
        delivery = self.next_delivery(bob)
        self.assertEqual(delivery.channel, BYTES)
        self.assertEqual(delivery.body.data, data)
        self.assertEqual(delivery.body.content_type, "application/octet-stream")
        self.assertFalse(delivery.body.is_text)

    def test_bytes_to_a_text_channel_are_refused_by_the_server(self) -> None:
        alice = self.attach("alice")
        body = brenn_bus.Body(b"\x00\x01", "application/octet-stream")
        outcome = alice.publish(TEXT, body, timeout=5.0)
        self.assertIsInstance(outcome, brenn_bus.TextOnlyChannel)

    def test_a_body_over_the_cap_is_too_large(self) -> None:
        alice = self.attach("alice")
        outcome = alice.publish(TEXT, brenn_bus.Body.text("x" * (MAX_BODY + 1)), timeout=5.0)
        match outcome:
            case brenn_bus.BodyTooLarge(length, maximum):
                self.assertEqual(length, MAX_BODY + 1)
                self.assertEqual(maximum, MAX_BODY)
            case _:
                self.fail(f"outcome is {outcome!r}")

    def test_an_unsubscribed_channel_yields_nothing_more(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        self.subscribe(bob, OTHER_TEXT)
        bob.unsubscribe(TEXT)
        self.publish_ok(alice, TEXT, brenn_bus.Body.text("first"))
        self.publish_ok(alice, OTHER_TEXT, brenn_bus.Body.text("second"))
        delivery = self.next_delivery(bob)
        self.assertEqual(delivery.channel, OTHER_TEXT)
        self.assertEqual(delivery.body.as_text(), "second")
        with self.assertRaises(brenn_bus.NotSubscribed):
            bob.unsubscribe(TEXT)

    def test_a_missing_channel_inside_the_grant_is_unavailable(self) -> None:
        bob = self.attach("bob")
        answer = bob.subscribe(MISSING, push_depth=8, retain_depth=8, timeout=5.0)
        self.assertIsInstance(answer, brenn_bus.Unavailable)

    def test_a_second_subscribe_of_a_held_channel_is_refused(self) -> None:
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        with self.assertRaises(brenn_bus.AlreadySubscribed):
            bob.subscribe(TEXT, push_depth=8, retain_depth=8, timeout=5.0)

    def test_recv_with_a_zero_timeout_returns_none_at_once(self) -> None:
        bob = self.attach("bob")
        started = time.monotonic()
        self.assertIsNone(bob.recv(timeout=0))
        self.assertLess(time.monotonic() - started, 0.5)

    def test_a_negative_timeout_is_refused_on_every_call(self) -> None:
        bob = self.attach("bob")
        with self.assertRaisesRegex(ValueError, "timeout"):
            bob.recv(timeout=-1)
        with self.assertRaisesRegex(ValueError, "timeout"):
            bob.publish(TEXT, brenn_bus.Body.text("x"), timeout=-1)
        with self.assertRaisesRegex(ValueError, "timeout"):
            bob.subscribe(TEXT, push_depth=8, retain_depth=8, timeout=-1)

    def test_a_with_block_closes_the_client_and_lets_the_exception_through(self) -> None:
        class Boom(Exception):
            pass

        client = self.attach("bob")
        with self.assertRaises(Boom):
            with client:
                raise Boom
        with self.assertRaises(brenn_bus.ClientClosed):
            client.status()
        with self.assertRaises(brenn_bus.ClientClosed):
            client.publish(TEXT, brenn_bus.Body.text("x"), timeout=5.0)
        with self.assertRaises(brenn_bus.ClientClosed):
            client.recv(timeout=0)

    def test_close_after_the_end_returns(self) -> None:
        client = self.attach("bob")
        client.close()
        client.close()
        with client:
            pass
        with self.assertRaises(brenn_bus.ClientClosed):
            client.status()


class ThreadsTest(ServerTestCase):
    def test_two_publisher_threads_and_a_reader(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        per_thread = 8
        errors: list[BaseException] = []
        received: list[str] = []

        def publish(prefix: str, client: brenn_bus.Client) -> None:
            try:
                for n in range(per_thread):
                    outcome = client.publish(TEXT, brenn_bus.Body.text(f"{prefix}{n}"), timeout=5.0)
                    if not isinstance(outcome, brenn_bus.Ok):
                        raise AssertionError(f"{prefix}{n}: {outcome!r}")
            except BaseException as err:
                errors.append(err)

        def read() -> None:
            try:
                while len(received) < 2 * per_thread:
                    event = bob.recv(timeout=10.0)
                    if not isinstance(event, brenn_bus.Delivery):
                        raise AssertionError(f"event is {event!r}")
                    text = event.body.as_text()
                    assert text is not None
                    received.append(text)
            except BaseException as err:
                errors.append(err)

        threads = [
            threading.Thread(target=read),
            threading.Thread(target=publish, args=("a", alice)),
            threading.Thread(target=publish, args=("b", alice)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30.0)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        for prefix in ("a", "b"):
            self.assertEqual(
                [text for text in received if text.startswith(prefix)],
                [f"{prefix}{n}" for n in range(per_thread)],
            )


class OutsideGrantTest(ServerTestCase):
    def test_a_subscribe_outside_the_grant_loses_the_connection_and_reattaches(self) -> None:
        alice = self.attach("alice")
        with self.assertRaises(brenn_bus.ConnectionLost) as raised:
            alice.subscribe(OUTSIDE_GRANT, push_depth=8, retain_depth=8, timeout=10.0)
        self.assertIsNone(raised.exception.loss.code)
        self.assertTrue(
            _wait_for(lambda: isinstance(alice.status(), brenn_bus.Attached), 15.0),
            f"status is {alice.status()!r}",
        )
        self.publish_ok(alice, TEXT, brenn_bus.Body.text("after the refusal"))


    def test_an_open_outside_the_grant_loses_the_connection(self) -> None:
        alice = self.attach("alice")
        with self.assertRaises(brenn_bus.ConnectionLost) as raised:
            alice.open_attachment(OUTSIDE_GRANT, str(uuid.uuid4()), "photo", timeout=10.0)
        self.assertIsNone(raised.exception.loss.code)
        self.assertTrue(
            _wait_for(lambda: isinstance(alice.status(), brenn_bus.Attached), 15.0),
            f"status is {alice.status()!r}",
        )


def _named(delivery: brenn_bus.Delivery) -> list[tuple[str, int, str]]:
    """A delivery's attachments, in the order it lists them, matched by
    position."""
    named: list[tuple[str, int, str]] = []
    for attachment in delivery.attachments:
        match attachment:
            case brenn_bus.Attachment(name, size, content_type):
                named.append((name, size, content_type))
    return named


class BlobTest(ServerTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dir = pathlib.Path(directory.name)

    def published(
        self, sender: brenn_bus.Client, reader: brenn_bus.Client, attachments: dict[str, brenn_bus.Blob]
    ) -> brenn_bus.Delivery:
        """A message naming `attachments`, published by `sender` to TEXT and
        delivered to `reader`, which holds it."""
        text = f"with blobs {uuid.uuid4()}"
        outcome = sender.publish(
            TEXT, brenn_bus.Body.text(text), timeout=5.0, attachments=attachments
        )
        self.assertIsInstance(outcome, brenn_bus.Ok)
        return self.delivery_of(reader, text)

    def delivery_of(self, reader: brenn_bus.Client, text: str) -> brenn_bus.Delivery:
        """The delivery whose body is `text`, past the replay of the messages
        earlier tests left on the class's server."""
        while True:
            delivery = self.next_delivery(reader)
            if delivery.body.as_text() == text:
                return delivery

    def test_each_attacher_has_a_blob_door(self) -> None:
        for slug in TOKENS:
            with self.subTest(remote=slug):
                facts = self.attach(slug).facts()
                self.assertEqual(facts.protocol_version, 6)
                match facts.blobs:
                    case brenn_bus.BlobFacts(maximum, chunk, handles, held, reads, upload):
                        self.assertEqual(maximum, MAX_BLOB)
                        self.assertEqual(chunk, CHUNK)
                        self.assertGreater(handles, 0)
                        self.assertGreaterEqual(held, MAX_BLOB)
                        self.assertEqual(chunk * reads, WINDOW)
                        self.assertEqual(upload, slug not in SUBSCRIBE_ONLY)
                    case door:
                        self.fail(f"blobs is {door!r}")

    def test_uploads_are_published_opened_and_read_back(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        note = b"a short note"
        data = os.urandom(WINDOW + WINDOW // 2)
        source = self.dir / "data.bin"
        source.write_bytes(data)
        with (
            alice.upload(note, content_type="text/plain", timeout=10.0) as small,
            alice.upload_file(source, content_type="application/octet-stream", timeout=30.0) as large,
        ):
            self.assertEqual((small.size, small.content_type), (len(note), "text/plain"))
            self.assertEqual(large.size, len(data))
            delivery = self.published(alice, bob, {"note": small, "data": large})
        self.assertEqual(
            _named(delivery),
            [
                ("data", len(data), "application/octet-stream"),
                ("note", len(note), "text/plain"),
            ],
        )
        with bob.open_attachment(TEXT, delivery.message_id, "data", timeout=10.0) as opened:
            self.assertEqual(opened.size, len(data))
            self.assertEqual(opened.content_type, "application/octet-stream")
            offset = WINDOW - 100
            self.assertEqual(
                bob.read_blob(opened, offset, 300, timeout=10.0), data[offset : offset + 300]
            )
            whole = self.dir / "data.copy"
            bob.download_blob(opened, whole, timeout=30.0)
            self.assertEqual(whole.read_bytes(), data)
        with bob.open_attachment(TEXT, delivery.message_id, "note", timeout=10.0) as opened:
            copy = self.dir / "note.copy"
            bob.download_blob(opened, str(copy), timeout=10.0)
            self.assertEqual(copy.read_bytes(), note)
            with self.assertRaises(FileExistsError) as raised:
                bob.download_blob(opened, copy, timeout=10.0)
            self.assertEqual(raised.exception.filename, str(copy))
            self.assertEqual(copy.read_bytes(), note)

    def test_an_opened_attachment_is_forwarded_without_reading_it(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        self.subscribe(alice, OTHER_TEXT)
        with alice.upload(b"forward me", content_type="text/plain", timeout=10.0) as blob:
            delivery = self.published(alice, bob, {"original": blob})
        with bob.open_attachment(TEXT, delivery.message_id, "original", timeout=10.0) as opened:
            outcome = bob.publish(
                OTHER_TEXT,
                brenn_bus.Body.text("forwarded"),
                timeout=5.0,
                attachments={"copy": opened},
            )
            self.assertIsInstance(outcome, brenn_bus.Ok)
        forwarded = self.delivery_of(alice, "forwarded")
        self.assertEqual(forwarded.channel, OTHER_TEXT)
        self.assertEqual(_named(forwarded), [("copy", 10, "text/plain")])

    def test_an_upload_over_the_cap_is_refused(self) -> None:
        alice = self.attach("alice")
        with self.assertRaises(brenn_bus.UploadRefused) as raised:
            alice.upload(bytes(MAX_BLOB + 1), content_type="application/octet-stream", timeout=10.0)
        refused = raised.exception
        self.assertEqual(refused.reason, brenn_bus.UploadRefusal.BLOB_TOO_LARGE)
        self.assertEqual((refused.size, refused.max), (MAX_BLOB + 1, MAX_BLOB))
        self.assertIsNone(refused.offset)
        self.assertIsNone(refused.len)

    def test_a_subscribe_only_remote_may_not_upload(self) -> None:
        carol = self.attach("carol")
        with self.assertRaises(brenn_bus.UploadNotGranted):
            carol.upload(b"x", content_type="text/plain", timeout=10.0)

    def test_opens_of_what_is_not_there_are_refused(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        with alice.upload(b"present", content_type="text/plain", timeout=10.0) as blob:
            delivery = self.published(alice, bob, {"present": blob})
        with self.assertRaises(brenn_bus.OpenRefused) as raised:
            bob.open_attachment(TEXT, delivery.message_id, "absent", timeout=10.0)
        self.assertEqual(raised.exception.reason, brenn_bus.OpenRefusal.NO_SUCH_ATTACHMENT)
        self.assertIsNone(raised.exception.size)
        with self.assertRaises(brenn_bus.OpenRefused) as raised:
            bob.open_attachment(TEXT, str(uuid.uuid4()), "present", timeout=10.0)
        self.assertEqual(raised.exception.reason, brenn_bus.OpenRefusal.NOT_RETAINED)

    def test_a_closed_blob_is_gone(self) -> None:
        alice = self.attach("alice")
        blob = alice.upload(b"short-lived", content_type="text/plain", timeout=10.0)
        blob.close()
        blob.close()
        with self.assertRaises(brenn_bus.BlobGone):
            alice.read_blob(blob, 0, 1, timeout=10.0)
        outcome = alice.publish(
            TEXT, brenn_bus.Body.text("names a closed blob"), timeout=5.0, attachments={"gone": blob}
        )
        self.assertIsInstance(outcome, brenn_bus.AttachmentNotHeld)

    def test_a_read_past_the_end_is_refused(self) -> None:
        alice = self.attach("alice")
        with alice.upload(b"0123456789", content_type="text/plain", timeout=10.0) as blob:
            self.assertEqual(alice.read_blob(blob, 8, 2, timeout=10.0), b"89")
            with self.assertRaises(brenn_bus.ReadRefused) as raised:
                alice.read_blob(blob, 8, 3, timeout=10.0)
            self.assertEqual(raised.exception.reason, brenn_bus.ReadRefusal.OUT_OF_RANGE)
            self.assertEqual(raised.exception.size, 10)

    def test_caller_errors_are_value_and_os_errors(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        with alice.upload(b"mine", content_type="text/plain", timeout=10.0) as blob:
            with self.assertRaisesRegex(ValueError, "another client"):
                bob.read_blob(blob, 0, 1, timeout=10.0)
            with self.assertRaisesRegex(ValueError, "another client"):
                bob.publish(TEXT, brenn_bus.Body.text("x"), timeout=5.0, attachments={"a": blob})
            with self.assertRaisesRegex(ValueError, "must not be negative"):
                alice.read_blob(blob, -1, 1, timeout=10.0)
            with self.assertRaisesRegex(ValueError, "attachment name"):
                alice.publish(TEXT, brenn_bus.Body.text("x"), timeout=5.0, attachments={".x": blob})
        with self.assertRaisesRegex(ValueError, "content type"):
            alice.upload(b"x", content_type="not a media type", timeout=10.0)
        with self.assertRaisesRegex(ValueError, "not a UUID"):
            alice.open_attachment(TEXT, "not-a-uuid", "x", timeout=10.0)
        with self.assertRaisesRegex(ValueError, "local"):
            alice.open_attachment("local:x", str(uuid.uuid4()), "x", timeout=10.0)
        missing = self.dir / "missing.bin"
        with self.assertRaises(FileNotFoundError) as raised:
            alice.upload_file(missing, content_type="text/plain", timeout=10.0)
        self.assertEqual(raised.exception.filename, str(missing))


class CredentialTest(ServerTestCase):
    def test_a_wrong_token_is_rejected_without_naming_it(self) -> None:
        token = "wrong-token-5c0ffee"
        started = time.monotonic()
        with self.assertRaises(brenn_bus.CredentialRejected) as raised:
            brenn_bus.attach(
                self.server.url("alice"),
                ident="brenn_bus_server_test",
                timeout=20.0,
                token=token,
            )
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertEqual(raised.exception.status, 401)
        self.assertIsInstance(raised.exception, brenn_bus.AttachmentEnded)
        for text in (str(raised.exception), repr(raised.exception), repr(raised.exception.args)):
            self.assertNotIn(token, text)


class ServerKilledTest(ServerTestCase):
    def test_a_killed_server_detaches_the_client(self) -> None:
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        self.server.kill()
        event = bob.recv(timeout=10.0)
        self.assertIsInstance(event, brenn_bus.Detached, f"event is {event!r}")
        assert isinstance(event, brenn_bus.Detached)
        self.assertIsInstance(event.loss, brenn_bus.Loss)
        self.assertIsInstance(bob.status(), brenn_bus.Detached)
        with self.assertRaises((brenn_bus.NotAttached, brenn_bus.ConnectionLost)):
            bob.publish(TEXT, brenn_bus.Body.text("into the void"), timeout=5.0)
        started = time.monotonic()
        bob.close()
        self.assertLess(time.monotonic() - started, 5.0)


class TlsTest(ServerTestCase):
    front: TlsFront
    other: TlsFront

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.front = TlsFront(cls.server.port, cls.server.root / "front-ca.pem")
        cls.addClassCleanup(cls.front.stop)
        # A second front's authority signed nothing the first front serves.
        cls.other = TlsFront(cls.server.port, cls.server.root / "other-ca.pem")
        cls.addClassCleanup(cls.other.stop)

    def wss(self, slug: str) -> str:
        return f"wss://127.0.0.1:{self.front.port}/remote/{slug}/ws"

    def test_wss_with_the_authority_attaches_and_round_trips(self) -> None:
        alice = brenn_bus.attach(
            self.wss("alice"),
            ident="brenn_bus_server_test",
            timeout=10.0,
            token=TOKENS["alice"],
            ca_file=self.front.ca_file,
        )
        self.addCleanup(alice.close)
        bob = self.attach("bob")
        self.subscribe(bob, TEXT)
        self.publish_ok(alice, TEXT, brenn_bus.Body.text("over tls"))
        delivery = self.next_delivery(bob)
        self.assertEqual(delivery.body.as_text(), "over tls")
        self.assertEqual(delivery.sender, alice.facts().participant_id)

    def assert_untrusted(self, ca_file: pathlib.Path | None) -> None:
        with self.assertRaises(brenn_bus.AttachTimeout) as raised:
            brenn_bus.attach(
                self.wss("alice"),
                ident="brenn_bus_server_test",
                timeout=2.0,
                token=TOKENS["alice"],
                ca_file=ca_file,
            )
        failure = raised.exception.last_failure
        assert failure is not None, "no dial failure recorded"
        self.assertEqual(failure.kind, brenn_bus.DialFailureKind.TLS)
        self.assertNotIn(TOKENS["alice"], str(raised.exception))

    def test_wss_without_a_ca_file_fails_on_tls(self) -> None:
        self.assert_untrusted(None)

    def test_wss_with_another_authority_fails_on_tls(self) -> None:
        self.assert_untrusted(self.other.ca_file)


if __name__ == "__main__":
    unittest.main()
