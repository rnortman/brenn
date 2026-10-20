#!/usr/bin/env python3
"""Unit and integration tests for noop_mcp.py."""

import io
import json
import sys
import unittest
from unittest.mock import patch

import noop_mcp
from noop_mcp import _pump, handle


# ---------------------------------------------------------------------------
# Integration tests: _pump (stdin loop)
# ---------------------------------------------------------------------------

BRENN_MESSAGE_EDIT_TOOL = {
    "name": "BrennMessageEdit",
    "inputSchema": {
        "type": "object",
        "properties": {
            "message_id": {"type": "string"},
            "body": {"type": "string"},
            "deliver_after": {"type": ["string", "null"]},
            "delivery_deadline": {"type": ["string", "null"]},
            "reply_to": {"type": ["string", "null"]},
        },
        "required": ["message_id"],
    },
}

TOOLS = [BRENN_MESSAGE_EDIT_TOOL]


def _make_tools_call(id_, name, arguments):
    return json.dumps({
        "jsonrpc": "2.0",
        "id": id_,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }) + "\n"


def _read_responses(out_buf):
    out_buf.seek(0)
    responses = []
    for line in out_buf:
        line = line.strip()
        if line:
            responses.append(json.loads(line))
    return responses


class TestPumpLoop(unittest.TestCase):
    def test_main_loop_swallows_handler_exception(self):
        """Loop continues after handle() raises; -32603 sent for requests with id."""
        line1 = _make_tools_call(1, "BrennMessageEdit", {"message_id": "m1"})
        line2 = _make_tools_call(2, "BrennMessageEdit", {"message_id": "m2"})
        stdin = io.StringIO(line1 + line2)
        stdout = io.StringIO()

        with patch("noop_mcp.handle", side_effect=[RuntimeError("boom"), None]) as mock_handle:
            _pump(stdin, TOOLS, stdout)

        # Loop must have called handle() twice — proving it continued past the exception.
        self.assertEqual(mock_handle.call_count, 2)

        responses = _read_responses(stdout)
        # First call produced a -32603 error; second call returned None (no output).
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0]["id"], 1)
        self.assertIn("error", responses[0])
        self.assertEqual(responses[0]["error"]["code"], -32603)

    def test_main_loop_schema_violation_answered_noop(self):
        """A call whose arguments violate the schema is answered __NOOP__:
        the stub does not judge arguments."""
        call_a = _make_tools_call(
            10, "BrennMessageEdit",
            {"message_id": "m1", "deliver_after": 42},
        )
        call_b = _make_tools_call(11, "BrennMessageEdit", {"bogus": "x"})
        call_c = _make_tools_call(12, "BrennMessageEdit", ["not", "an", "object"])
        stdin = io.StringIO(call_a + call_b + call_c)
        stdout = io.StringIO()

        _pump(stdin, TOOLS, stdout)

        responses = _read_responses(stdout)
        self.assertEqual([r["id"] for r in responses], [10, 11, 12])
        for r in responses:
            self.assertNotIn("error", r)
            self.assertNotIn("isError", r["result"])
            self.assertEqual(r["result"]["content"][0]["text"], "__NOOP__")

    def test_unknown_tool_still_32601(self):
        """A call naming a tool the stub does not declare is an MCP-level error."""
        stdin = io.StringIO(_make_tools_call(13, "NoSuchTool", {}))
        stdout = io.StringIO()
        _pump(stdin, TOOLS, stdout)
        responses = _read_responses(stdout)
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0]["error"]["code"], -32601)

    def test_main_loop_notification_exception_no_response(self):
        """Exception from a notification (no 'id') must not produce any response;
        loop must continue and process the subsequent message."""
        # Notification: valid JSON-RPC object but no "id" field.
        notification = json.dumps({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }) + "\n"
        call = _make_tools_call(99, "BrennMessageEdit", {"message_id": "m1"})
        stdin = io.StringIO(notification + call)
        stdout = io.StringIO()

        with patch("noop_mcp.handle", side_effect=[RuntimeError("boom from notification"), None]) as mock_handle:
            _pump(stdin, TOOLS, stdout)

        # handle() must have been called for both messages.
        self.assertEqual(mock_handle.call_count, 2)

        responses = _read_responses(stdout)
        # Notification has no id → no -32603 response. Second call returned None.
        self.assertEqual(len(responses), 0)


class TestHandle(unittest.TestCase):
    """Tests for handle() directly, covering the non-dict guard."""

    @staticmethod
    def _make_writer():
        """Return (buf, writer_callable) where buf is a StringIO and writer appends to it."""
        buf = io.StringIO()
        return buf, buf.write

    def test_non_dict_sends_32600_and_returns(self):
        """handle() with a JSON array (non-dict) must send -32600 and return without raising."""
        buf, writer = self._make_writer()
        # Should not raise; early-return after sending error.
        handle([1, 2, 3], TOOLS, writer)
        responses = _read_responses(buf)
        self.assertEqual(len(responses), 1)
        self.assertIsNone(responses[0].get("id"))
        self.assertIn("error", responses[0])
        self.assertEqual(responses[0]["error"]["code"], -32600)

    def test_non_dict_null_sends_32600(self):
        """handle() with JSON null (non-dict) also triggers the guard."""
        buf, writer = self._make_writer()
        handle(None, TOOLS, writer)
        responses = _read_responses(buf)
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0]["error"]["code"], -32600)


class TestPumpLoopNonDict(unittest.TestCase):
    """_pump integration: non-dict JSON on stdin → -32600 then continue."""

    def test_pump_json_array_sends_32600_and_continues(self):
        """JSON array on stdin emits -32600 then continues; next valid call succeeds."""
        array_line = json.dumps([1, 2, 3]) + "\n"
        valid_call = _make_tools_call(5, "BrennMessageEdit", {"message_id": "m1"})
        stdin = io.StringIO(array_line + valid_call)
        stdout = io.StringIO()
        _pump(stdin, TOOLS, stdout)
        responses = _read_responses(stdout)
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0]["error"]["code"], -32600)
        self.assertEqual(responses[1]["result"]["content"][0]["text"], "__NOOP__")


class TestMain(unittest.TestCase):
    """Tests for main() startup error handling."""

    def _run_main_with_tools_arg(self, tools_path: str):
        """Call noop_mcp.main() with --tools <tools_path> injected into sys.argv."""
        with patch("sys.argv", ["noop_mcp.py", "--tools", tools_path]):
            noop_mcp.main()

    def test_main_exits_on_open_error(self):
        """OSError from opening tools file must produce a fatal exit message."""
        with patch("builtins.open", side_effect=OSError("no such file")):
            with self.assertRaises(SystemExit) as cm:
                self._run_main_with_tools_arg("/fake/tools.json")
            msg = str(cm.exception)
            self.assertTrue(
                msg.startswith("noop_mcp: fatal:"),
                f"expected 'noop_mcp: fatal:' prefix, got: {msg!r}",
            )

    def test_main_exits_on_json_decode_error(self):
        """json.JSONDecodeError from a malformed tools file must produce a fatal exit message."""
        malformed = io.StringIO("{not valid json")
        with patch("builtins.open", return_value=malformed):
            with self.assertRaises(SystemExit) as cm:
                self._run_main_with_tools_arg("/fake/tools.json")
            msg = str(cm.exception)
            self.assertTrue(
                msg.startswith("noop_mcp: fatal:"),
                f"expected 'noop_mcp: fatal:' prefix, got: {msg!r}",
            )


if __name__ == "__main__":
    unittest.main()
