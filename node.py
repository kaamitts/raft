from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Any

from config import Settings
from models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    NodeState,
    RequestVoteRequest,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    from http_client import HttpClient


class RaftNode:
    def __init__(self, settings: Settings) -> None:
        self.node_id:   str       = settings.node_id
        self.peer_urls: list[str] = settings.peer_urls
        self.settings:  Settings  = settings
        
        self.current_term: int        = 0
        self.voted_for:    str | None = None
        self.log:          list[LogEntry] = []

        self.commit_index:       int            = -1
        self.last_applied:       int            = -1
        self.state:              NodeState       = NodeState.FOLLOWER
        self.current_leader:     str | None      = None
        self.current_leader_url: str | None      = None

        self.next_index:  dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self.data: dict[str, Any] = {}

        # Concurrency 
        self._lock:                 asyncio.Lock  = asyncio.Lock()
        self._election_reset_event: asyncio.Event = asyncio.Event()
        self._background_task:      asyncio.Task | None = None

        self._stopped: bool = False


    # Lifecycle
    async def start(self) -> None:
        self._stopped = False
        self._background_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stopped = True
        self._election_reset_event.set()
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass

    # Main loop
    async def _run_loop(self) -> None:
        while not self._stopped:
            async with self._lock:
                state = self.state

            if state == NodeState.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(self.settings.heartbeat_interval_ms / 1000)
            else:
                timeout = random.randint(
                    self.settings.election_timeout_min_ms,
                    self.settings.election_timeout_max_ms,
                ) / 1000
                try:
                    await asyncio.wait_for(self._wait_for_reset(), timeout=timeout)
                except TimeoutError:
                    if not self._stopped:
                        await self._start_election()

    async def _wait_for_reset(self) -> None:
        self._election_reset_event.clear()
        await self._election_reset_event.wait()

    def _reset_election_timer(self) -> None:
        self._election_reset_event.set()

    # Election
    async def _start_election(self) -> None:
        async with self._lock:
            self.state        = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for    = self.node_id
            self.current_leader     = None
            self.current_leader_url = None
            term           = self.current_term
            last_log_index = len(self.log) - 1
            last_log_term  = self.log[-1].term if self.log else 0

        print(f"[{self.node_id}] Election for term {term}")

        majority = (len(self.peer_urls) + 2) // 2
        request  = RequestVoteRequest(
            term=term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        tasks     = [self._http.request_vote(url, request) for url in self.peer_urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        async with self._lock:
            if self._stopped:
                return
            if self.state != NodeState.CANDIDATE or self.current_term != term:
                return

            votes = 1  # self-vote
            for resp in responses:
                if isinstance(resp, Exception):
                    continue
                if resp.term > self.current_term:
                    self._step_down(resp.term)
                    return
                if resp.vote_granted:
                    votes += 1

            if votes >= majority:
                self._become_leader()
            else:
                print(f"[{self.node_id}] Lost election (votes={votes})")
                self.state = NodeState.FOLLOWER
                self._reset_election_timer()

    def _become_leader(self) -> None:
        self.state              = NodeState.LEADER
        self.current_leader     = self.node_id
        self.current_leader_url = self.settings.self_url
        next_idx = len(self.log)
        for url in self.peer_urls:
            self.next_index[url]  = next_idx
            self.match_index[url] = -1
        print(f"[{self.node_id}] Became LEADER for term {self.current_term}")

    def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.state        = NodeState.FOLLOWER
        self.voted_for    = None
        self._reset_election_timer()

    async def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        if self._stopped:
            return RequestVoteResponse(term=req.term, vote_granted=False)

        async with self._lock:
            if req.term > self.current_term:
                self._step_down(req.term)

            vote_granted = False
            if req.term >= self.current_term:
                already_voted     = self.voted_for is not None and self.voted_for != req.candidate_id
                my_last_log_index = len(self.log) - 1
                my_last_log_term  = self.log[-1].term if self.log else 0
                candidate_log_ok  = (
                    req.last_log_term > my_last_log_term
                    or (req.last_log_term == my_last_log_term and req.last_log_index >= my_last_log_index)
                )
                if not already_voted and candidate_log_ok:
                    vote_granted   = True
                    self.voted_for = req.candidate_id
                    self._reset_election_timer()

            return RequestVoteResponse(term=self.current_term, vote_granted=vote_granted)

    async def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        if self._stopped:
            return AppendEntriesResponse(term=req.term, success=False, match_index=-1)

        async with self._lock:
            if req.term > self.current_term:
                self._step_down(req.term)

            if req.term < self.current_term:
                return AppendEntriesResponse(
                    term=self.current_term, success=False, match_index=-1
                )

            self.state              = NodeState.FOLLOWER
            self.current_leader     = req.leader_id
            self.current_leader_url = req.leader_addr or self.current_leader_url
            self._reset_election_timer()

            # Log consistency check 
            if req.prev_log_index >= 0:
                if len(self.log) <= req.prev_log_index:
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=len(self.log) - 1,
                    )
                if self.log[req.prev_log_index].term != req.prev_log_term:
                    self.log = self.log[:req.prev_log_index]
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=req.prev_log_index - 1,
                    )

            # Append new entries
            insert_at = req.prev_log_index + 1
            for i, entry in enumerate(req.entries):
                idx = insert_at + i
                if idx < len(self.log):
                    if self.log[idx].term != entry.term:
                        self.log = self.log[:idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Advance commit
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log) - 1)
                self._apply_committed()

            return AppendEntriesResponse(
                term=self.current_term,
                success=True,
                match_index=len(self.log) - 1,
            )

    # State machine
    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied].command
            op  = cmd.get("op")
            if op == "set":
                self.data[cmd["key"]] = cmd["value"]
            elif op == "delete":
                self.data.pop(cmd["key"], None)

    # Replication
    async def _send_heartbeats(self) -> None:
        if self._stopped:
            return
        async with self._lock:
            if self.state != NodeState.LEADER:
                return
            term        = self.current_term
            leader_id   = self.node_id
            leader_addr = self.settings.self_url
            commit      = self.commit_index

        tasks = [
            self._replicate_to_peer(url, term, leader_id, leader_addr, commit)
            for url in self.peer_urls
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _replicate_to_peer(
        self, url: str, term: int, leader_id: str, leader_addr: str, commit_index: int
    ) -> None:
        async with self._lock:
            if self.state != NodeState.LEADER:
                return
            next_idx       = self.next_index.get(url, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term  = (
                self.log[prev_log_index].term
                if 0 <= prev_log_index < len(self.log) else 0
            )
            entries = list(self.log[next_idx:])

        req  = AppendEntriesRequest(
            term=term,
            leader_id=leader_id,
            leader_addr=leader_addr,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=commit_index,
        )
        resp = await self._http.append_entries(url, req)
        if isinstance(resp, Exception):
            return

        async with self._lock:
            if resp.term > self.current_term:
                self._step_down(resp.term)
                return
            if self.state != NodeState.LEADER:
                return

            if resp.success:
                self.match_index[url] = resp.match_index
                self.next_index[url]  = resp.match_index + 1
                self._try_advance_commit_index()
            else:
                self.next_index[url] = max(0, resp.match_index + 1)

    def _try_advance_commit_index(self) -> None:
        majority = (len(self.peer_urls) + 2) // 2
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue
            count = 1 + sum(1 for mi in self.match_index.values() if mi >= n)
            if count >= majority:
                self.commit_index = n
                self._apply_committed()
                break

    # Client write entry point
    async def append_command(self, command: dict[str, Any]) -> bool:
        async with self._lock:
            if self.state != NodeState.LEADER:
                return False
            entry     = LogEntry(term=self.current_term, command=command)
            self.log.append(entry)
            new_index = len(self.log) - 1
            term      = self.current_term

        await self._send_heartbeats()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            async with self._lock:
                if self.state != NodeState.LEADER or self.current_term != term:
                    return False
                if self.commit_index >= new_index:
                    return True
            await asyncio.sleep(0.01)

        raise TimeoutError(f"Entry {new_index} not committed within 2s")

    # Lazy HTTP client property
    @property
    def _http(self) -> "HttpClient":
        from http_client import HttpClient
        return HttpClient(self.settings)
