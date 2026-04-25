from __future__ import annotations
import os


class Settings:
    def __init__(self) -> None:
        self.node_id:   str   = os.environ.get("RAFT_NODE_ID", "node1")
        self.self_url:  str   = os.environ.get("RAFT_SELF_URL", "http://127.0.0.1:8001")
        self.host:      str   = os.environ.get("RAFT_HOST", "0.0.0.0")
        self.port:      int   = int(os.environ.get("RAFT_PORT", "8001"))
        peers_raw = os.environ.get("RAFT_PEERS", "")
        self.peer_urls: list[str] = [p.strip() for p in peers_raw.split(",") if p.strip()]
        self.election_timeout_min_ms: int   = int(os.environ.get("RAFT_ELECTION_TIMEOUT_MIN_MS", "150"))
        self.election_timeout_max_ms: int   = int(os.environ.get("RAFT_ELECTION_TIMEOUT_MAX_MS", "300"))
        self.heartbeat_interval_ms:   int   = int(os.environ.get("RAFT_HEARTBEAT_INTERVAL_MS", "50"))
        self.http_timeout_s:          float = float(os.environ.get("RAFT_HTTP_TIMEOUT_S", "0.1"))
