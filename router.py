from __future__ import annotations

import asyncio
import json
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING, Any

from models import (
    AppendEntriesRequest,
    DataResponse,
    RequestVoteRequest,
    StatusResponse,
)

if TYPE_CHECKING:
    from node import RaftNode


def make_handler(node: "RaftNode", loop: asyncio.AbstractEventLoop):

    class Handler(BaseHTTPRequestHandler):

        def log_message(self, fmt, *args):
            pass

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            raw    = self.rfile.read(length) if length else b"{}"
            return json.loads(raw)

        def _send_json(self, code: int, body: Any) -> None:
            payload = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _run(self, coro):
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=5.0)

        def do_GET(self):
            routes = {
                "/health": self._health,
                "/status": self._status,
                "/data":   self._get_data,
            }
            handler = routes.get(self.path)
            if handler:
                handler()
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):
            routes = {
                "/data":                self._write_data,
                "/raft/request-vote":   self._request_vote,
                "/raft/append-entries": self._append_entries,
            }
            handler = routes.get(self.path)
            if handler:
                handler()
            else:
                self._send_json(404, {"error": "not found"})

        def do_PUT(self):
            if self.path == "/data":
                self._write_data()
            else:
                self._send_json(404, {"error": "not found"})

        def _health(self):
            self._send_json(200, {"status": "ok"})

        def _status(self):
            async def _():
                async with node._lock:
                    return StatusResponse(
                        node_id=node.node_id,
                        state=str(node.state),
                        term=node.current_term,
                        leader=node.current_leader,
                        log_length=len(node.log),
                        commit_index=node.commit_index,
                    ).to_dict()
            self._send_json(200, self._run(_()))

        def _get_data(self):
            async def _():
                async with node._lock:
                    return DataResponse(
                        data=dict(node.data),
                        leader=node.current_leader,
                        term=node.current_term,
                    ).to_dict()
            self._send_json(200, self._run(_()))

        def _write_data(self):
            body = self._read_json()

            async def _():
                async with node._lock:
                    state      = node.state
                    leader_url = node.current_leader_url

                if str(state) == "leader":
                    try:
                        committed = await node.append_command(body)
                    except TimeoutError:
                        return 503, {"error": "Could not achieve majority commit in time"}
                    if not committed:
                        return 503, {"error": "Lost leadership during commit"}
                    async with node._lock:
                        return 200, {"data": dict(node.data), "committed": True}

                if leader_url:
                    return 200, {"redirect": leader_url}

                return 503, {"error": "No leader known, try again shortly"}

            code, resp = self._run(_())

            if code == 200 and "redirect" in resp:
                from http_client import HttpClient
                future = asyncio.run_coroutine_threadsafe(
                    HttpClient(node.settings).proxy_write(resp["redirect"], body),
                    loop,
                )
                result = future.result(timeout=5.0)
                if isinstance(result, Exception):
                    self._send_json(502, {"error": f"Leader proxy failed: {result}"})
                else:
                    self._send_json(200, result)
                return

            self._send_json(code, resp)

        def _request_vote(self):
            req  = RequestVoteRequest.from_dict(self._read_json())
            resp = self._run(node.handle_request_vote(req))
            self._send_json(200, resp.to_dict())

        def _append_entries(self):
            req  = AppendEntriesRequest.from_dict(self._read_json())
            resp = self._run(node.handle_append_entries(req))
            self._send_json(200, resp.to_dict())

    return Handler
