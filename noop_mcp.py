#!/usr/bin/env python3
"""Noop MCP server for Brenn. Registers virtual tool schemas, returns __NOOP__ for all calls.

The real work happens in the Brenn server via PreToolUse/PostToolUse hook interception.
This server just exists to make the tools visible to Claude Code.

Tool schemas are loaded from a JSON file passed via --tools <path>. The file contains
core virtual tools (DisplayFile, RequestCompaction) plus integration-specific virtual
tools for the current app. Brenn writes this file before spawning CC.
"""

import argparse
import json
import sys
import traceback


def send(msg, writer=None):
    out = json.dumps(msg)
    if writer is not None:
        writer(out + "\n")
    else:
        sys.stdout.write(out + "\n")
        sys.stdout.flush()


JSON_TYPE_TO_PY = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "array": list,
}

# All JSON Schema primitive type names we recognize, including "null".
_KNOWN_TYPE_NAMES = set(JSON_TYPE_TO_PY.keys()) | {"null"}

# WARN-once dedup set: entries are (tool_name, field_name, marker) tuples.
_warned: set = set()


def _warn(msg: str) -> None:
    print(f"noop_mcp: WARN: {msg}", file=sys.stderr)


def _warn_once(key: tuple, msg: str) -> None:
    if key not in _warned:
        _warned.add(key)
        _warn(msg)


def _normalize_types(type_field, *, tool_name: str, field_name: str):
    """Normalize a JSON Schema "type" value to a list of accepted type-name strings.

    Returns a list[str] of accepted JSON-Schema primitive type names, or None
    meaning "schema does not constrain the type; skip the check".

    - None (key absent): return None (unchecked).
    - str: if known, return [that_str]; otherwise WARN once and return None.
    - list: filter to known elements, WARN once per unknown; return filtered
      list or None if empty.
    - Any other shape (dict, number, etc.): WARN once and return None.
    """
    if type_field is None:
        return None

    if isinstance(type_field, str):
        if type_field in _KNOWN_TYPE_NAMES:
            return [type_field]
        _warn_once(
            (tool_name, field_name, type_field),
            f"tool '{tool_name}' field '{field_name}': "
            f"unknown type name {type_field!r}; skipping type check",
        )
        return None

    if isinstance(type_field, list):
        accepted = []
        for item in type_field:
            if isinstance(item, str) and item in _KNOWN_TYPE_NAMES:
                accepted.append(item)
            else:
                marker = item if isinstance(item, str) else type(item).__name__
                _warn_once(
                    (tool_name, field_name, marker),
                    f"tool '{tool_name}' field '{field_name}': "
                    f"unknown/unsupported type element {item!r}; skipping element",
                )
        return accepted if accepted else None

    # Unexpected shape: dict (anyOf-style), number, etc.
    _warn_once(
        (tool_name, field_name, type(type_field).__name__),
        f"tool '{tool_name}' field '{field_name}': "
        f"cannot interpret 'type' value of shape {type(type_field).__name__!r}; "
        f"skipping type check",
    )
    return None


def _json_type_name(value) -> str:
    """Return the JSON Schema primitive type name for a Python value. Never raises."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def validate_tool_input(schema, arguments, *, tool_name: str = ""):
    """Validate an MCP tool-call's arguments against the tool's inputSchema.

    Returns an error message string if validation fails, or None on success.
    Checks required keys, rejects unknown keys, and does shallow type checks.
    A bad call is better loud than silent — CC does not validate inputs
    against the declared schema, so the MCP server is the enforcement layer.
    """
    if not isinstance(arguments, dict):
        return f"arguments must be an object, got {type(arguments).__name__}"

    properties = schema.get("properties", {}) or {}
    required = schema.get("required", []) or []

    missing = [k for k in required if k not in arguments]
    if missing:
        return (
            f"missing required argument(s): {missing} "
            f"(allowed: {sorted(properties.keys())})"
        )

    known = set(properties.keys())
    unknown = [k for k in arguments if k not in known]
    if unknown:
        return (
            f"unknown argument(s): {unknown} "
            f"(allowed: {sorted(known)}); "
            f"check the tool's inputSchema for the correct names"
        )

    for key, value in arguments.items():
        prop = properties.get(key, {})
        # Guard: schema authoring bug (property written as string, not object).
        # Bounded by _pump exception handler but better to warn than crash.
        type_field = prop.get("type") if isinstance(prop, dict) else None
        if type_field is None:
            continue
        accepted = _normalize_types(type_field, tool_name=tool_name, field_name=key)
        if accepted is None:
            continue

        if value is None:
            if "null" in accepted:
                continue
            accepted_str = "|".join(accepted)
            return f"argument '{key}' must be {accepted_str}, got null"

        json_type = _json_type_name(value)

        # Accept if the JSON type name is directly in accepted.
        if json_type in accepted:
            continue
        # "number" in schema also accepts integer values (JSON integers are valid numbers).
        if "number" in accepted and isinstance(value, (int, float)) and not isinstance(value, bool):
            continue

        accepted_str = "|".join(accepted)
        return f"argument '{key}' must be {accepted_str}, got {json_type}"

    return None


def handle(msg, tools, writer=None):
    if not isinstance(msg, dict):
        print(
            f"noop_mcp: non-dict JSON message (type={type(msg).__name__}): {msg!r}",
            file=sys.stderr,
        )
        send({
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32600, "message": "Invalid Request: message must be a JSON object"},
        }, writer)
        return
    method = msg.get("method")
    id_ = msg.get("id")

    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": id_,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "brenn", "version": "0.1.0"},
            },
        }, writer)
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": id_, "result": {"tools": tools}}, writer)
    elif method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name", "")
        arguments = params.get("arguments", {}) or {}
        tool = next((t for t in tools if t.get("name") == name), None)
        if tool is None:
            send({
                "jsonrpc": "2.0",
                "id": id_,
                "error": {"code": -32601, "message": f"unknown tool: {name!r}"},
            }, writer)
            return
        schema = tool.get("inputSchema", {}) or {}
        err = validate_tool_input(schema, arguments, tool_name=name)
        if err is not None:
            send({
                "jsonrpc": "2.0",
                "id": id_,
                "result": {
                    "isError": True,
                    "content": [{"type": "text", "text": f"invalid arguments for tool '{name}': {err}"}],
                },
            }, writer)
            return
        # All valid tool calls return __NOOP__. The real result is injected by
        # Brenn via the PostToolUse hook's updatedMCPToolOutput.
        send({
            "jsonrpc": "2.0",
            "id": id_,
            "result": {"content": [{"type": "text", "text": "__NOOP__"}]},
        }, writer)
    else:
        if id_ is not None:
            send({
                "jsonrpc": "2.0",
                "id": id_,
                "error": {"code": -32601, "message": f"Unknown method: {method}"},
            }, writer)


def _pump(stdin, tools, stdout=None):
    """Read-eval-respond loop. Separated from main() for testability.

    Reads NDJSON lines from stdin, dispatches via handle(), writes responses
    to sys.stdout (or the stdout stream if provided for testing). Continues on
    all errors except KeyboardInterrupt / SystemExit.

    Note: the catch-and-continue policy for handle() exceptions is deliberate.
    noop_mcp.py is a subprocess; if the process dies, CC loses the MCP
    transport and every in-flight tool call fails. A per-call -32603 JSON-RPC
    error is always preferable to a transport-level disconnect.
    """
    if stdout is not None:
        def writer(text):
            stdout.write(text)
            stdout.flush()
    else:
        def writer(text):
            sys.stdout.write(text)
            sys.stdout.flush()

    for line in stdin:
        line = line.strip()
        if not line:
            continue
        msg = None
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            excerpt = line[:200] + ("…" if len(line) > 200 else "")
            print(f"noop_mcp: invalid JSON: {excerpt!r}", file=sys.stderr)
            continue
        try:
            handle(msg, tools, writer)
        except Exception:
            traceback.print_exc(file=sys.stderr)
            id_ = msg.get("id") if isinstance(msg, dict) else None
            if id_ is not None:
                exc = sys.exc_info()[1]
                send({
                    "jsonrpc": "2.0",
                    "id": id_,
                    "error": {
                        "code": -32603,
                        "message": f"Internal error: {type(exc).__name__}: {exc}",
                    },
                }, writer)


def main():
    parser = argparse.ArgumentParser(description="Brenn noop MCP server")
    parser.add_argument(
        "--tools",
        required=True,
        help="Path to JSON file containing tool schemas",
    )
    args = parser.parse_args()

    try:
        with open(args.tools) as f:
            tools = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        sys.exit(f"noop_mcp: fatal: {e}")

    _pump(sys.stdin, tools)


if __name__ == "__main__":
    main()
