"""The `brenn_bus` module with no server: loading, values, argument checks,
dial failures, interrupts, and the agreement of the stub and the
documentation page with the module."""

import ast
import importlib.util
import math
import os
import pathlib
import re
import signal
import socket
import tempfile
import threading
import time
import types
import unittest

import brenn_bus

# Members the stub parity test compares beyond the public names.
_TRACKED_DUNDERS = {"__match_args__", "__enter__", "__exit__"}


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


class ModuleLoadTest(unittest.TestCase):
    def test_imports_the_abi3_extension(self) -> None:
        spec = importlib.util.find_spec("brenn_bus")
        self.assertIsNotNone(spec)
        assert spec is not None
        self.assertIsNotNone(spec.origin)
        assert spec.origin is not None
        self.assertTrue(
            spec.origin.endswith("brenn_bus.abi3.so"),
            f"brenn_bus resolved to {spec.origin}",
        )
        self.assertEqual(brenn_bus.__name__, "brenn_bus")


class BodyTest(unittest.TestCase):
    def test_text_round_trips(self) -> None:
        body = brenn_bus.Body.text("héllo")
        self.assertTrue(body.is_text)
        self.assertIsNone(body.content_type)
        self.assertEqual(body.as_text(), "héllo")
        self.assertEqual(body.data, "héllo".encode())
        self.assertEqual(body, brenn_bus.Body("héllo".encode()))

    def test_bytes_with_a_non_text_type_round_trip(self) -> None:
        data = bytes(range(256))
        body = brenn_bus.Body(data, "application/octet-stream")
        self.assertFalse(body.is_text)
        self.assertIsNone(body.as_text())
        self.assertEqual(body.data, data)
        self.assertEqual(body.content_type, "application/octet-stream")

    def test_bytes_with_a_text_type_are_text(self) -> None:
        body = brenn_bus.Body(b"a,b", "text/csv; charset=utf-8")
        self.assertTrue(body.is_text)
        self.assertEqual(body.as_text(), "a,b")
        self.assertEqual(body.content_type, "text/csv; charset=utf-8")

    def test_untyped_bytes_that_are_not_utf8_are_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "not valid UTF-8"):
            brenn_bus.Body(b"\xff\xfe")

    def test_text_typed_bytes_that_are_not_utf8_are_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "not valid UTF-8"):
            brenn_bus.Body(b"\xff", "text/plain")

    def test_a_malformed_content_type_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid content type"):
            brenn_bus.Body(b"x", "not a media type")


class AttachArgumentTest(unittest.TestCase):
    """Every refusal is a ValueError raised before anything is dialled."""

    def setUp(self) -> None:
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.url = f"ws://127.0.0.1:{_closed_port()}/remote"

    def file(self, name: str, content: bytes, mode: int) -> pathlib.Path:
        path = pathlib.Path(self.dir.name) / name
        path.write_bytes(content)
        os.chmod(path, mode)
        return path

    def assert_refused(self, pattern: str, **kwargs: object) -> None:
        arguments: dict[str, object] = {
            "ident": "brenn_bus_test",
            "timeout": 30.0,
            "token": "secret",
        }
        arguments.update(kwargs)
        url = arguments.pop("url", self.url)
        assert isinstance(url, str)
        started = time.monotonic()
        with self.assertRaisesRegex(ValueError, pattern):
            brenn_bus.attach(url, **arguments)  # type: ignore[arg-type]
        self.assertLess(time.monotonic() - started, 1.0)

    def test_a_scheme_other_than_ws_or_wss(self) -> None:
        self.assert_refused("ws:// or wss://", url="http://127.0.0.1:1/remote")

    def test_cleartext_to_a_host_that_is_not_loopback(self) -> None:
        self.assert_refused(
            "bearer token over cleartext ws:// to non-loopback host",
            url="ws://192.0.2.1:9/remote",
        )

    def test_both_token_and_token_file(self) -> None:
        path = self.file("token", b"secret", 0o600)
        self.assert_refused("exactly one of token and token_file", token_file=path)

    def test_neither_token_nor_token_file(self) -> None:
        self.assert_refused("exactly one of token and token_file", token=None)

    def test_an_empty_token(self) -> None:
        self.assert_refused("empty", token="")

    def test_a_negative_or_infinite_timeout(self) -> None:
        self.assert_refused("timeout", timeout=-1.0)
        self.assert_refused("timeout", timeout=math.inf)
        self.assert_refused("timeout", timeout=math.nan)

    def test_a_token_file_readable_by_others(self) -> None:
        path = self.file("token", b"secret\n", 0o644)
        self.assert_refused("readable by group or others", token=None, token_file=path)

    def test_an_empty_token_file(self) -> None:
        path = self.file("token", b"\n", 0o600)
        self.assert_refused("is empty", token=None, token_file=str(path))

    def test_a_token_file_with_whitespace_beyond_one_final_newline(self) -> None:
        for content in (b" secret\n", b"secret\r\n", b"secret\n\n", b"secret \n"):
            with self.subTest(content=content):
                path = self.file("token", content, 0o600)
                self.assert_refused(
                    "whitespace around the token", token=None, token_file=path
                )

    def test_a_token_file_that_cannot_be_read(self) -> None:
        missing = pathlib.Path(self.dir.name) / "missing.token"
        for path in (missing, self.dir.name):
            with self.subTest(path=path):
                self.assert_refused(
                    re.escape(f"token_file {path}: "), token=None, token_file=path
                )

    def test_a_ca_file_that_cannot_be_read(self) -> None:
        missing = pathlib.Path(self.dir.name) / "missing.pem"
        self.assert_refused(
            re.escape(f"ca_file {missing}: "),
            url="wss://127.0.0.1:1/remote",
            ca_file=missing,
        )

    def test_a_ca_file_with_no_certificate(self) -> None:
        path = self.file("ca.pem", b"not a certificate\n", 0o644)
        self.assert_refused("no certificate", url="wss://127.0.0.1:1/remote", ca_file=path)


class AttachFailureTest(unittest.TestCase):
    def test_a_closed_port_times_out_with_the_io_failure(self) -> None:
        url = f"ws://127.0.0.1:{_closed_port()}/remote"
        started = time.monotonic()
        with self.assertRaises(brenn_bus.AttachTimeout) as raised:
            brenn_bus.attach(url, ident="brenn_bus_test", timeout=1.0, token="secret")
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 1.0)
        self.assertLess(elapsed, 3.0)
        self.assertIsInstance(raised.exception, brenn_bus.BusError)
        failure = raised.exception.last_failure
        match failure:
            case brenn_bus.DialFailure(brenn_bus.DialFailureKind.IO, None, str(detail)):
                self.assertTrue(detail)
                self.assertTrue(
                    repr(failure).startswith(
                        'DialFailure(kind=DialFailureKind.IO, status=None, detail="'
                    ),
                    repr(failure),
                )
            case _:
                self.fail(f"last_failure is {failure!r}")
        self.assertNotIn("secret", str(raised.exception))
        self.assertNotIn("secret", repr(failure))

    def test_a_listener_that_never_answers_times_out(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            url = f"ws://127.0.0.1:{listener.getsockname()[1]}/remote"
            started = time.monotonic()
            with self.assertRaises(brenn_bus.AttachTimeout) as raised:
                brenn_bus.attach(url, ident="brenn_bus_test", timeout=1.0, token="secret")
            elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 1.0)
        self.assertLess(elapsed, 3.0)
        # The dial's own 15 s deadline has not fired by the attach's 1 s, so no
        # dial has failed yet.
        self.assertIsNone(raised.exception.last_failure)

    def test_a_wait_off_the_main_thread_runs_to_its_timeout(self) -> None:
        url = f"ws://127.0.0.1:{_closed_port()}/remote"
        raised: list[BaseException] = []

        def attach() -> None:
            try:
                brenn_bus.attach(url, ident="brenn_bus_test", timeout=0.5, token="secret")
            except BaseException as err:
                raised.append(err)

        worker = threading.Thread(target=attach)
        started = time.monotonic()
        worker.start()
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive())
        self.assertGreaterEqual(time.monotonic() - started, 0.5)
        self.assertEqual(len(raised), 1)
        self.assertIsInstance(raised[0], brenn_bus.AttachTimeout)

    def test_a_signal_handler_interrupts_the_wait(self) -> None:
        class Alarm(Exception):
            pass

        def on_alarm(signum: int, frame: types.FrameType | None) -> None:
            raise Alarm

        previous = signal.signal(signal.SIGALRM, on_alarm)
        self.addCleanup(signal.signal, signal.SIGALRM, previous)
        url = f"ws://127.0.0.1:{_closed_port()}/remote"
        started = time.monotonic()
        signal.setitimer(signal.ITIMER_REAL, 0.3)
        try:
            with self.assertRaises(Alarm):
                brenn_bus.attach(url, ident="brenn_bus_test", timeout=30.0, token="secret")
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
        self.assertLess(time.monotonic() - started, 2.0)


class ExceptionHierarchyTest(unittest.TestCase):
    def test_caller_errors_are_value_errors(self) -> None:
        for error in (brenn_bus.AlreadySubscribed, brenn_bus.NotSubscribed):
            self.assertTrue(issubclass(error, brenn_bus.BusError))
            self.assertTrue(issubclass(error, ValueError))
            self.assertEqual(error.__module__, "brenn_bus")

    def test_blob_errors_are_bus_errors_and_not_ended(self) -> None:
        for error in (
            brenn_bus.BlobsUnavailable,
            brenn_bus.UploadNotGranted,
            brenn_bus.UploadRefused,
            brenn_bus.OpenRefused,
            brenn_bus.ReadRefused,
            brenn_bus.BlobGone,
        ):
            self.assertTrue(issubclass(error, brenn_bus.BusError))
            self.assertFalse(issubclass(error, brenn_bus.AttachmentEnded))
            self.assertFalse(issubclass(error, ValueError))

    def test_every_end_is_an_attachment_ended(self) -> None:
        for error in (
            brenn_bus.CredentialRejected,
            brenn_bus.IncompatibleVersions,
            brenn_bus.ProtocolError,
            brenn_bus.Futile,
            brenn_bus.ClientClosed,
            brenn_bus.SupervisorPanicked,
        ):
            self.assertTrue(issubclass(error, brenn_bus.AttachmentEnded))


def _stub_tree() -> ast.Module:
    origin = brenn_bus.__file__
    assert origin is not None
    stub = pathlib.Path(origin).with_name("brenn_bus.pyi")
    return ast.parse(stub.read_text(), filename=str(stub))


def _tracked(names: set[str]) -> set[str]:
    return {name for name in names if not name.startswith("_") or name in _TRACKED_DUNDERS}


def _stub_members(node: ast.ClassDef) -> set[str]:
    """A stub class's members. A bare annotation declares an attribute set on
    each instance (an exception's), not a class member, and is not counted."""
    names: set[str] = set()
    for item in node.body:
        if isinstance(item, ast.FunctionDef):
            names.add(item.name)
        elif isinstance(item, ast.Assign):
            names.update(target.id for target in item.targets if isinstance(target, ast.Name))
        elif isinstance(item, ast.AnnAssign) and item.value is not None:
            assert isinstance(item.target, ast.Name)
            names.add(item.target.id)
    return _tracked(names)


class StubParityTest(unittest.TestCase):
    """`brenn_bus.pyi` names exactly what the module defines."""

    def test_module_names_match(self) -> None:
        stub: set[str] = set()
        for item in _stub_tree().body:
            if isinstance(item, (ast.ClassDef, ast.FunctionDef)):
                stub.add(item.name)
            elif isinstance(item, ast.Assign):
                stub.update(target.id for target in item.targets if isinstance(target, ast.Name))
        module = {name for name in dir(brenn_bus) if not name.startswith("_")}
        self.assertEqual(stub, module)

    def test_each_class_matches(self) -> None:
        for node in _stub_tree().body:
            if not isinstance(node, ast.ClassDef):
                continue
            with self.subTest(cls=node.name):
                runtime = getattr(brenn_bus, node.name)
                self.assertIsInstance(runtime, type)
                stub_bases = [base.id for base in node.bases if isinstance(base, ast.Name)]
                self.assertEqual(
                    stub_bases or ["object"],
                    [base.__name__ for base in runtime.__bases__],
                )
                self.assertEqual(_stub_members(node), _tracked(set(vars(runtime))))


class DocumentationPageTest(unittest.TestCase):
    """`docs/python-binding.md` lists every value a caller matches on."""

    page: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.page = pathlib.Path(os.environ["BINDING_PAGE"]).read_text()

    def test_the_tables_list_exactly_the_outcomes_events_and_exceptions(self) -> None:
        # A table row's first cell opens with the class name in backticks,
        # followed by its fields or the closing backtick.
        rows = set(re.findall(r"^\| `(\w+)[`(]", self.page, re.MULTILINE))
        roots = (brenn_bus.PublishOutcome, brenn_bus.Event, brenn_bus.BusError)
        module = {
            name
            for name, value in vars(brenn_bus).items()
            if isinstance(value, type) and issubclass(value, roots) and value not in roots
        }
        # `ValueError` is the builtin the binding raises for a bad argument,
        # `OSError` the one it raises for a local file, `RuntimeError` the one
        # it raises for a bug of its own that left the attachment up.
        self.assertEqual(rows, module | {"ValueError", "OSError", "RuntimeError"})

    def test_the_refusal_tables_list_exactly_each_refusal(self) -> None:
        # A refusal table's row opens with `<Enumeration>.<MEMBER>`.
        rows = set(re.findall(r"^\| `(\w+Refusal\.\w+)`", self.page, re.MULTILINE))
        module = {
            f"{enum.__name__}.{name}"
            for enum in (brenn_bus.UploadRefusal, brenn_bus.OpenRefusal, brenn_bus.ReadRefusal)
            for name, value in vars(enum).items()
            if isinstance(value, enum)
        }
        self.assertEqual(rows, module)

    def test_every_enumeration_member_is_named(self) -> None:
        for enum in (
            brenn_bus.Urgency,
            brenn_bus.Gap,
            brenn_bus.LossKind,
            brenn_bus.DialFailureKind,
            brenn_bus.UploadRefusal,
            brenn_bus.OpenRefusal,
            brenn_bus.ReadRefusal,
        ):
            members = [name for name, value in vars(enum).items() if isinstance(value, enum)]
            self.assertTrue(members, enum.__name__)
            for member in members:
                with self.subTest(member=f"{enum.__name__}.{member}"):
                    self.assertTrue(
                        f"`{member}`" in self.page or f"`{enum.__name__}.{member}`" in self.page,
                    )


if __name__ == "__main__":
    unittest.main()
