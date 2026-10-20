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
        tool = next((t for t in tools if t.get("name") == name), None)
        if tool is None:
            send({
                "jsonrpc": "2.0",
                "id": id_,
                "error": {"code": -32601, "message": f"unknown tool: {name!r}"},
            }, writer)
            return
        # Every call for a known tool returns __NOOP__. Brenn validates the
        # arguments and injects the real result via the PostToolUse hook's
        # updatedMCPToolOutput.
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
