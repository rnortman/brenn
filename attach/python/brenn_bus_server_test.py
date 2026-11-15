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

TOKENS = {"alice": "alice-token-7f3a9c", "bob": "bob-token-2d81e4"}

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
messaging {{ max_body_bytes = {max_body}; }}

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
    """One brenn server with remotes `alice` and `bob`, its state under a
    private directory."""

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
                text=TEXT,
                other_text=OTHER_TEXT,
                bytes=BYTES,
                remotes="".join(_REMOTE.format(slug=slug, root=root) for slug in TOKENS),
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
    def test_two_attachers_negotiate_v5_as_distinct_participants(self) -> None:
        alice = self.attach("alice")
        bob = self.attach("bob")
        facts = [alice.facts(), bob.facts()]
        for each in facts:
            self.assertEqual(each.protocol_version, 5)
            self.assertTrue(each.participant_id)
            self.assertEqual(each.max_body_bytes, MAX_BODY)
        self.assertNotEqual(facts[0].participant_id, facts[1].participant_id)
        self.assertNotEqual(facts[0].session_id, facts[1].session_id)
        match alice.status():
            case brenn_bus.Attached(brenn_bus.AttachmentFacts(5, participant, _, _)):
                self.assertEqual(participant, facts[0].participant_id)
            case status:
                self.fail(f"status is {status!r}")

    def test_a_token_file_attaches(self) -> None:
        path = self.server.root / "alice-client.token"
        path.write_text(TOKENS["alice"] + "\n")
        os.chmod(path, 0o600)
        client = self.attach("alice", token=None, token_file=path)
        self.assertEqual(client.facts().protocol_version, 5)

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
