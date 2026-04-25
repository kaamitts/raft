from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class NodeState(StrEnum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


@dataclass
class LogEntry:
    term: int
    command: dict[str, Any]

    def to_dict(self) -> dict:
        return {"term": self.term, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> LogEntry:
        return LogEntry(term=d["term"], command=d["command"])


@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> dict:
        return {
            "term":           self.term,
            "candidate_id":   self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term":  self.last_log_term,
        }

    @staticmethod
    def from_dict(d: dict) -> RequestVoteRequest:
        return RequestVoteRequest(
            term=d["term"],
            candidate_id=d["candidate_id"],
            last_log_index=d["last_log_index"],
            last_log_term=d["last_log_term"],
        )


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool

    def to_dict(self) -> dict:
        return {"term": self.term, "vote_granted": self.vote_granted}

    @staticmethod
    def from_dict(d: dict) -> RequestVoteResponse:
        return RequestVoteResponse(term=d["term"], vote_granted=d["vote_granted"])


@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: str
    leader_addr: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int

    def to_dict(self) -> dict:
        return {
            "term":           self.term,
            "leader_id":      self.leader_id,
            "leader_addr":    self.leader_addr,
            "prev_log_index": self.prev_log_index,
            "prev_log_term":  self.prev_log_term,
            "entries":        [e.to_dict() for e in self.entries],
            "leader_commit":  self.leader_commit,
        }

    @staticmethod
    def from_dict(d: dict) -> AppendEntriesRequest:
        return AppendEntriesRequest(
            term=d["term"],
            leader_id=d["leader_id"],
            leader_addr=d.get("leader_addr", ""),
            prev_log_index=d["prev_log_index"],
            prev_log_term=d["prev_log_term"],
            entries=[LogEntry.from_dict(e) for e in d.get("entries", [])],
            leader_commit=d["leader_commit"],
        )


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int

    def to_dict(self) -> dict:
        return {
            "term":        self.term,
            "success":     self.success,
            "match_index": self.match_index,
        }

    @staticmethod
    def from_dict(d: dict) -> AppendEntriesResponse:
        return AppendEntriesResponse(
            term=d["term"],
            success=d["success"],
            match_index=d.get("match_index", -1),
        )


@dataclass
class DataResponse:
    data: dict[str, Any]
    leader: str | None
    term: int

    def to_dict(self) -> dict:
        return {"data": self.data, "leader": self.leader, "term": self.term}


@dataclass
class StatusResponse:
    node_id: str
    state: str
    term: int
    leader: str | None
    log_length: int
    commit_index: int

    def to_dict(self) -> dict:
        return {
            "node_id":      self.node_id,
            "state":        self.state,
            "term":         self.term,
            "leader":       self.leader,
            "log_length":   self.log_length,
            "commit_index": self.commit_index,
        }
