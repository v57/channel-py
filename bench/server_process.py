from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Any

import orjson

from channel_py import Channel


def _emit(message: dict[str, Any]) -> None:
    payload = orjson.dumps(message) + b"\n"
    os.write(1, payload)


def _parse_port() -> int:
    raw = os.getenv("BENCH_PORT")
    if raw is None:
        raise ValueError("BENCH_PORT must be set")
    port = int(raw)
    if port <= 0:
        raise ValueError("BENCH_PORT must be a valid positive integer")
    return port


def _install_uvloop() -> None:
    if os.name == "nt":
        return
    try:
        import uvloop

        uvloop.install()
    except Exception:
        pass


async def main() -> None:
    try:
        port = _parse_port()
    except Exception as error:
        _emit({"type": "error", "message": str(error)})
        raise SystemExit(1)

    started_at = time.perf_counter()
    requests = 0
    stop_event = asyncio.Event()
    stopped = False

    def text_count(body: Any) -> int:
        nonlocal requests
        requests += 1
        payload = getattr(body, "body", None)
        text = payload.get("text") if isinstance(payload, dict) else None
        return len(text) if isinstance(text, str) else 0

    channel = Channel().post("text/count", text_count)
    server = channel.listen(port)
    await server.wait_started()

    _emit({"type": "ready", "pid": os.getpid(), "port": port})

    async def stop_server() -> None:
        nonlocal stopped
        if stopped:
            return
        stopped = True

        await server.stop_async()
        elapsed_seconds = time.perf_counter() - started_at
        _emit(
            {
                "type": "result",
                "pid": os.getpid(),
                "requests": requests,
                "elapsedSeconds": elapsed_seconds,
            }
        )
        raise SystemExit(0)

    loop = asyncio.get_running_loop()

    def on_signal() -> None:
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, on_signal)
        loop.add_signal_handler(signal.SIGINT, on_signal)
    except NotImplementedError:
        pass

    await stop_event.wait()
    await stop_server()


if __name__ == "__main__":
    _install_uvloop()
    asyncio.run(main())
