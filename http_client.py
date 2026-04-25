from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Any

from config import Settings
from models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


def _post_sync(url: str, body: dict, timeout: float) -> dict:
    """Blocking HTTP POST — runs in a thread via run_in_executor."""
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


async def _post(url: str, body: dict, timeout: float) -> dict:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _post_sync, url, body, timeout)


class HttpClient:
    def __init__(self, settings: Settings) -> None:
        self._timeout = settings.http_timeout_s

    async def request_vote(
        self, peer_url: str, req: RequestVoteRequest
    ) -> RequestVoteResponse | Exception:
        try:
            data = await _post(f"{peer_url}/raft/request-vote", req.to_dict(), self._timeout)
            return RequestVoteResponse.from_dict(data)
        except Exception as exc:
            return exc

    async def append_entries(
        self, peer_url: str, req: AppendEntriesRequest
    ) -> AppendEntriesResponse | Exception:
        try:
            data = await _post(f"{peer_url}/raft/append-entries", req.to_dict(), self._timeout)
            return AppendEntriesResponse.from_dict(data)
        except Exception as exc:
            return exc

    async def proxy_write(
        self, leader_url: str, body: dict[str, Any]
    ) -> dict[str, Any] | Exception:
        try:
            return await _post(f"{leader_url}/data", body, timeout=5.0)
        except Exception as exc:
            return exc
