from __future__ import annotations

import asyncio
import threading
from http.server import ThreadingHTTPServer

from config import Settings
from node import RaftNode
from router import make_handler


async def main() -> None:
    settings = Settings()
    node     = RaftNode(settings)
    loop     = asyncio.get_running_loop()

    HandlerClass  = make_handler(node, loop)
    server        = ThreadingHTTPServer((settings.host, settings.port), HandlerClass)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    print(f"[{settings.node_id}] 🚀 Listening on {settings.host}:{settings.port}")
    print(f"[{settings.node_id}]    peers: {settings.peer_urls}")

    await node.start()
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print(f"\n[{settings.node_id}] Shutting down...")
        await node.stop()
        server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
