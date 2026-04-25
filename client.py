from __future__ import annotations

import json
import os
import signal
import sys
import urllib.request

BASE_URLS = {f"node{i}": f"http://127.0.0.1:{8000 + i}" for i in range(1, 6)}
ALL_URLS  = list(BASE_URLS.values())


def _get(url, timeout=2.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _put(url, body, timeout=5.0):
    try:
        data = json.dumps(body).encode()
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def find_leader():
    for nid, url in BASE_URLS.items():
        s = _get(f"{url}/status")
        if s and s.get("state") == "leader":
            return nid
    return None


def cmd_status():
    print(f"\n{'NODE':>6}  {'STATE':>10}  {'TERM':>5}  {'LEADER':>6}  {'LOG':>4}  {'COMMIT':>6}  DATA")
    print("─" * 78)
    for nid, url in BASE_URLS.items():
        s = _get(f"{url}/status")
        if s is None:
            print(f"{nid:>6}  {'💀 DOWN':>10}")
            continue
        d = _get(f"{url}/data")
        data_str = str(d["data"]) if d else "?"
        icon = {"leader": "👑 leader", "follower": "follower", "candidate": "🗳  cand."}.get(s["state"], s["state"])
        print(f"{nid:>6}  {icon:>10}  {s['term']:>5}  {str(s['leader'] or '?'):>6}"
              f"  {s['log_length']:>4}  {s['commit_index']:>6}  {data_str}")
    print()


def cmd_read():
    for nid, url in BASE_URLS.items():
        d = _get(f"{url}/data")
        if d:
            print(f"✅ Data (leader={d.get('leader')}, term={d.get('term')}): {d['data']}")
            return
    print("❌ Could not read — no node available")


def cmd_set(key, raw_value):
    try:
        value = json.loads(raw_value)
    except Exception:
        value = raw_value
    leader = find_leader()
    target = BASE_URLS.get(leader) if leader else next(iter(ALL_URLS))
    resp   = _put(f"{target}/data", {"op": "set", "key": key, "value": value})
    if resp and resp.get("committed"):
        print(f"✅ Set {key!r}={value!r}. Data: {resp['data']}")
    else:
        print(f"❌ Failed: {resp}")


def cmd_delete(key):
    leader = find_leader()
    target = BASE_URLS.get(leader) if leader else next(iter(ALL_URLS))
    resp   = _put(f"{target}/data", {"op": "delete", "key": key})
    if resp and resp.get("committed"):
        print(f"✅ Deleted {key!r}. Data: {resp['data']}")
    else:
        print(f"❌ Failed: {resp}")


def cmd_kill(node_id):
    pid_file = f"/tmp/raft_v2_{node_id}.pid"
    if not os.path.exists(pid_file):
        print(f"❌ No PID file for {node_id}")
        return
    with open(pid_file) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"💀 Killed {node_id} (PID={pid})")
    except ProcessLookupError:
        print(f"❌ Process {pid} not found")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__); sys.exit(1)
    cmd = args[0]
    if cmd == "status":
        cmd_status()
    elif cmd == "read":
        cmd_read()
    elif cmd == "set" and len(args) == 3:
        cmd_set(args[1], args[2])
    elif cmd == "delete" and len(args) == 2:
        cmd_delete(args[1])
    elif cmd == "kill" and len(args) == 2:
        cmd_kill(args[1])
    else:
        print(__doc__); sys.exit(1)
