#!/usr/bin/env python3
"""Unit and integration tests for noop_mcp.py."""

import io
import json
import sys
import unittest
from unittest.mock import patch

import noop_mcp
from noop_mcp import (
    _normalize_types,
    _json_type_name,
    _pump,
    handle,
    validate_tool_input,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _schema(properties=None, required=None):
    s = {"type": "object"}
    if properties is not None:
        s["properties"] = properties
    if required is not None:
        s["required"] = required
    return s


def _call_validate(props, args, required=None, tool_name="TestTool"):
    return validate_tool_input(_schema(props, required), args, tool_name=tool_name)


# ---------------------------------------------------------------------------
# Unit tests: _json_type_name
# ---------------------------------------------------------------------------

class TestJsonTypeName(unittest.TestCase):
    def test_null(self):
        self.assertEqual(_json_type_name(None), "null")

    def test_bool(self):
        self.assertEqual(_json_type_name(True), "boolean")
        self.assertEqual(_json_type_name(False), "boolean")

    def test_int(self):
        self.assertEqual(_json_type_name(42), "integer")

    def test_float(self):
        self.assertEqual(_json_type_name(3.14), "number")

    def test_str(self):
        self.assertEqual(_json_type_name("hello"), "string")

    def test_list(self):
        self.assertEqual(_json_type_name([1, 2]), "array")

    def test_dict(self):
        self.assertEqual(_json_type_name({"a": 1}), "object")


# ---------------------------------------------------------------------------
# Unit tests: _normalize_types
# ---------------------------------------------------------------------------

class TestNormalizeTypes(unittest.TestCase):
    def setUp(self):
        # Clear warn-once state between tests.
        noop_mcp._warned.clear()

    def test_none_returns_none(self):
        self.assertIsNone(_normalize_types(None, tool_name="T", field_name="f"))

    def test_known_string_returns_list(self):
        self.assertEqual(
            _normalize_types("string", tool_name="T", field_name="f"),
            ["string"],
        )

    def test_null_string_returns_list(self):
        self.assertEqual(
            _normalize_types("null", tool_name="T", field_name="f"),
            ["null"],
        )

    def test_unknown_string_returns_none_and_warns(self):
        with patch("noop_mcp._warn") as mock_warn:
            result = _normalize_types("strng", tool_name="T", field_name="f")
        self.assertIsNone(result)
        mock_warn.assert_called_once()

    def test_list_with_known_elements(self):
        self.assertEqual(
            _normalize_types(["string", "null"], tool_name="T", field_name="f"),
            ["string", "null"],
        )

    def test_list_with_unknown_element_filters_and_warns(self):
        with patch("noop_mcp._warn") as mock_warn:
            result = _normalize_types(["string", "strng"], tool_name="T", field_name="f")
        self.assertEqual(result, ["string"])
        mock_warn.assert_called_once()

    def test_list_all_unknown_returns_none(self):
        noop_mcp._warned.clear()
        result = _normalize_types(["strng", "ints"], tool_name="T", field_name="f")
        self.assertIsNone(result)

    def test_dict_type_warns_and_returns_none(self):
        with patch("noop_mcp._warn") as mock_warn:
            result = _normalize_types({"weird": True}, tool_name="T", field_name="f")
        self.assertIsNone(result)
        mock_warn.assert_called_once()

    def test_warn_once_dedup(self):
        noop_mcp._warned.clear()
        with patch("noop_mcp._warn") as mock_warn:
            _normalize_types("strng", tool_name="T", field_name="f")
            _normalize_types("strng", tool_name="T", field_name="f")
        # Only one _warn call despite two invocations.
        self.assertEqual(mock_warn.call_count, 1)


# ---------------------------------------------------------------------------
# Unit tests: validate_tool_input
# ---------------------------------------------------------------------------

class TestValidateToolInput(unittest.TestCase):
    def setUp(self):
        noop_mcp._warned.clear()

    # 1. String field + string value → None
    def test_validate_string_field_string_value(self):
        err = _call_validate({"name": {"type": "string"}}, {"name": "Alice"})
        self.assertIsNone(err)

    # 2. String field + null value → error mentioning the field
    def test_validate_string_field_null_value_rejected(self):
        err = _call_validate({"name": {"type": "string"}}, {"name": None})
        self.assertIsNotNone(err)
        self.assertIn("'name'", err)

    # 3. Integer field + bool value → rejected (bool-vs-int guard)
    def test_validate_integer_field_bool_value_rejected(self):
        err = _call_validate({"count": {"type": "integer"}}, {"count": True})
        self.assertIsNotNone(err)
        self.assertIn("'count'", err)

    # 4. Union ["string","null"] + string → None
    def test_validate_union_string_null_with_string(self):
        err = _call_validate(
            {"deliver_after": {"type": ["string", "null"]}},
            {"deliver_after": "2026-01-01T00:00:00Z"},
        )
        self.assertIsNone(err)

    # 5. Union ["string","null"] + null → None
    def test_validate_union_string_null_with_null(self):
        err = _call_validate(
            {"deliver_after": {"type": ["string", "null"]}},
            {"deliver_after": None},
        )
        self.assertIsNone(err)

    # 6. Union ["string","null"] + integer → error mentioning field; no exception
    def test_validate_union_string_null_with_integer_rejected(self):
        err = _call_validate(
            {"deliver_after": {"type": ["string", "null"]}},
            {"deliver_after": 42},
        )
        self.assertIsNotNone(err)
        self.assertIn("'deliver_after'", err)

    # 7. Unknown type shape (dict) → None (skipped); WARN logged
    def test_validate_unknown_type_shape_skips(self):
        with patch("noop_mcp._warn"):
            err = _call_validate(
                {"meta": {"type": {"weird": True}}},
                {"meta": "anything"},
            )
        self.assertIsNone(err)

    # 8. String + enum: any string passes (enum content not validated)
    def test_validate_enum_string_field(self):
        err = _call_validate(
            {"wake": {"type": "string", "enum": ["morning", "evening"]}},
            {"wake": "noon"},
        )
        self.assertIsNone(err)

    # 9. Missing required → error
    def test_validate_missing_required(self):
        err = _call_validate(
            {"body": {"type": "string"}},
            {},
            required=["body"],
        )
        self.assertIsNotNone(err)
        self.assertIn("missing", err)

    # 10. Unknown argument → error
    def test_validate_unknown_argument(self):
        err = _call_validate({"body": {"type": "string"}}, {"bogus": "x"})
        self.assertIsNotNone(err)
        self.assertIn("unknown", err)

    # 11. arguments not a dict → error
    def test_validate_arguments_not_dict(self):
        err = validate_tool_input(_schema({}), ["not", "a", "dict"], tool_name="T")
        self.assertIsNotNone(err)
        self.assertIn("object", err)

    # Bool against union ["integer","number"] → rejected (general branches cover it)
    def test_validate_integer_number_union_bool_rejected(self):
        err = _call_validate(
            {"count": {"type": ["integer", "number"]}},
            {"count": True},
        )
        self.assertIsNotNone(err)
        self.assertIn("'count'", err)

    # Parametric: all "type" shapes currently used in the VirtualToolDef catalog
    def test_parametric_catalog_type_shapes(self):
        """No exception for any JSON-loadable value against all catalog type shapes."""
        shapes = [
            {"type": "string"},
            {"type": "integer"},
            {"type": "number"},
            {"type": "object"},
            {"type": "string", "enum": ["a", "b"]},
            {"type": ["string", "null"]},
        ]
        values = [
            "hello", 0, 1, -1, 3.14, True, False, None, [], {}, [1, "x"], {"a": 1},
        ]
        for shape in shapes:
            for val in values:
                try:
                    result = _call_validate({"field": shape}, {"field": val})
                    # result is either None or a string — never an exception
                    self.assertTrue(result is None or isinstance(result, str))
                except Exception as exc:
                    self.fail(
                        f"validate_tool_input raised {type(exc).__name__} "
                        f"for shape={shape!r}, value={val!r}: {exc}"
                    )


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
    def setUp(self):
        noop_mcp._warned.clear()

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

    def test_main_loop_handles_validation_then_continues(self):
        """Three calls: valid, bad-type, valid. No exception; three responses."""
        call_a = _make_tools_call(
            10, "BrennMessageEdit",
            {"message_id": "m1", "deliver_after": "2026-01-01T00:00:00Z"},
        )
        call_b = _make_tools_call(
            11, "BrennMessageEdit",
            {"message_id": "m2", "deliver_after": 42},
        )
        call_c = _make_tools_call(
            12, "BrennMessageEdit",
            {"message_id": "m3"},
        )
        stdin = io.StringIO(call_a + call_b + call_c)
        stdout = io.StringIO()

        _pump(stdin, TOOLS, stdout)

        responses = _read_responses(stdout)
        self.assertEqual(len(responses), 3)

        # (a) valid → __NOOP__
        r_a = responses[0]
        self.assertEqual(r_a["id"], 10)
        self.assertNotIn("error", r_a)
        self.assertEqual(r_a["result"]["content"][0]["text"], "__NOOP__")

        # (b) bad type → isError: true, mentions 'deliver_after'
        r_b = responses[1]
        self.assertEqual(r_b["id"], 11)
        self.assertNotIn("error", r_b)
        self.assertTrue(r_b["result"]["isError"])
        self.assertIn("deliver_after", r_b["result"]["content"][0]["text"])

        # (c) subsequent valid → __NOOP__
        r_c = responses[2]
        self.assertEqual(r_c["id"], 12)
        self.assertNotIn("error", r_c)
        self.assertEqual(r_c["result"]["content"][0]["text"], "__NOOP__")

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

    def setUp(self):
        noop_mcp._warned.clear()

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

    def setUp(self):
        noop_mcp._warned.clear()

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
