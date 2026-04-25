import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import asyncio
import threading
import time
import json
import urllib.request
from http.server import ThreadingHTTPServer

from config import Settings
from node import RaftNode
from router import make_handler


BASE  = 8100
PORTS = {f"node{i}": BASE + i for i in range(1, 6)}
URLS  = {nid: f"http://127.0.0.1:{port}" for nid, port in PORTS.items()}

NODES   = {} 
SERVERS = {}  
LOOPS   = {}   


def _make_settings(node_id: str) -> Settings:
    port  = PORTS[node_id]
    peers = ",".join(u for nid, u in URLS.items() if nid != node_id)
    os.environ.update({
        "RAFT_NODE_ID":                 node_id,
        "RAFT_SELF_URL":                f"http://127.0.0.1:{port}",
        "RAFT_HOST":                    "127.0.0.1",
        "RAFT_PORT":                    str(port),
        "RAFT_PEERS":                   peers,
        "RAFT_ELECTION_TIMEOUT_MIN_MS": "400",
        "RAFT_ELECTION_TIMEOUT_MAX_MS": "800",
        "RAFT_HEARTBEAT_INTERVAL_MS":   "75",
        "RAFT_HTTP_TIMEOUT_S":          "0.15",
    })
    return Settings()


def _launch_node(node_id: str):
    settings = _make_settings(node_id)
    node     = RaftNode(settings)
    loop     = asyncio.new_event_loop()

    def _run_loop():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(node.start())
        loop.run_forever()

    threading.Thread(target=_run_loop, daemon=True).start()
    time.sleep(0.05)  # let loop start

    handler = make_handler(node, loop)
    server  = ThreadingHTTPServer(("127.0.0.1", PORTS[node_id]), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    NODES[node_id]   = node
    SERVERS[node_id] = server
    LOOPS[node_id]   = loop


def kill_node(node_id: str):
    node = NODES[node_id]
    loop = LOOPS[node_id]

    future = asyncio.run_coroutine_threadsafe(node.stop(), loop)
    try:
        future.result(timeout=2.0)
    except Exception:
        pass

    SERVERS[node_id].shutdown()


def _get(url, timeout=1.5):
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


def wait_leader(urls_dict, timeout=6.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        leaders = [
            s["node_id"]
            for u in urls_dict.values()
            if (s := _get(f"{u}/status")) and s.get("state") == "leader"
        ]
        if len(leaders) == 1:
            return leaders[0]
        time.sleep(0.1)
    return None


def wait_data(urls, key, value, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        found = sum(
            1 for u in urls
            if (d := _get(f"{u}/data")) and d.get("data", {}).get(key) == value
        )
        if found == len(urls):
            return True
        time.sleep(0.1)
    return False


def status_table():
    print(f"\n{'NODE':>6}  {'STATE':>10}  {'TERM':>5}  {'LEADER':>6}  {'LOG':>4}  {'COMMIT':>6}  DATA")
    print("─" * 78)
    for nid, url in URLS.items():
        s = _get(f"{url}/status")
        d = _get(f"{url}/data")
        if not s:
            print(f"{nid:>6}  {'DOWN':>10}")
            continue
        icon = {"leader": "leader", "follower": "follower", "candidate": "cand."}.get(s["state"], s["state"])
        data_str = str(d["data"]) if d else "?"
        print(f"{nid:>6}  {icon:>10}  {s['term']:>5}  {str(s.get('leader') or '?'):>6}"
              f"  {s['log_length']:>4}  {s['commit_index']:>6}  {data_str}")
    print()



print("Starting 5 nodes in-process...")
for i in range(1, 6):
    _launch_node(f"node{i}")

print("Waiting for leader election...")
time.sleep(2.5)

# Test runner 

passed = 0
failed = 0


def check(cond, msg):
    global passed, failed
    if cond:
        print(f"  SUCCESS {msg}")
        passed += 1
    else:
        print(f"  FAIL: {msg}")
        failed += 1


# 
print("\n" + "=" * 55)
print("TEST 1: Leader election")
print("=" * 55)
status_table()
leader = wait_leader(URLS)
check(leader is not None, f"Leader elected: {leader}")

# 
print("\n" + "=" * 55)
print("TEST 2: Write + replication to all 5 nodes")
print("=" * 55)
lu = URLS[leader]
r = _put(f"{lu}/data", {"op": "set", "key": "name", "value": "Alice"})
check(r and r.get("committed"), "Write 'name=Alice' committed")
r = _put(f"{lu}/data", {"op": "set", "key": "age", "value": 30})
check(r and r.get("committed"), "Write 'age=30' committed")
r = _put(f"{lu}/data", {"op": "set", "key": "city", "value": "Almaty"})
check(r and r.get("committed"), "Write 'city=Almaty' committed")
check(wait_data(list(URLS.values()), "name", "Alice"), "Replicated to all 5 nodes")
status_table()

# 
print("\n" + "=" * 55)
print("TEST 3: Write via follower (proxy to leader)")
print("=" * 55)
follower_url = next(u for nid, u in URLS.items() if nid != leader)
r = _put(f"{follower_url}/data", {"op": "set", "key": "proxied", "value": True})
check(r and r.get("committed"), "Follower proxied write to leader")
check(wait_data(list(URLS.values()), "proxied", True), "Replicated to all nodes")

# 
print("\n" + "=" * 55)
print("TEST 4: Delete key (our advantage over friend's version)")
print("=" * 55)
r = _put(f"{lu}/data", {"op": "delete", "key": "age"})
check(r and r.get("committed"), "Delete 'age' committed")
deadline = time.time() + 3.0
while time.time() < deadline:
    all_deleted = all(
        (d := _get(f"{u}/data")) and "age" not in d.get("data", {})
        for u in URLS.values()
    )
    if all_deleted:
        break
    time.sleep(0.1)
check(all_deleted, "Delete propagated to all nodes")

# 
print("\n" + "=" * 55)
print("TEST 5: Kill leader → new election")
print("=" * 55)
old_leader = wait_leader(URLS)
print(f"  Killing {old_leader}...")
kill_node(old_leader)
time.sleep(0.2) 

surviving = {nid: u for nid, u in URLS.items() if nid != old_leader}
new_leader = wait_leader(surviving, timeout=6.0)
check(new_leader is not None and new_leader != old_leader, f"New leader elected: {new_leader}")
status_table()

#
print("\n" + "=" * 55)
print("TEST 6: Write after failover")
print("=" * 55)
new_lu = URLS[new_leader]
r = _put(f"{new_lu}/data", {"op": "set", "key": "score", "value": 100})
check(r and r.get("committed"), "Write after failover committed")
surviving_urls = list(surviving.values())
check(wait_data(surviving_urls, "score", 100), "Replicated to all surviving nodes")

# 
print("\n" + "=" * 55)
print("FINAL STATE")
print("=" * 55)
status_table()

print("=" * 55)
print(f"  Passed: {passed}   Failed: {failed}")
print(f"  {'All tests PASSED' if failed == 0 else 'Some tests FAILED'}")
print("=" * 55)
