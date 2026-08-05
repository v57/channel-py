from __future__ import annotations

import asyncio
import os
import signal
from typing import Any

import orjson

from channel_py import Channel


def emit(payload: dict[str, Any]) -> None:
    print(orjson.dumps(payload).decode("utf-8"), flush=True)


def parse_duration_ms() -> int:
    raw = os.getenv("BENCH_DURATION_MS")
    if raw is None:
        raise ValueError("BENCH_DURATION_MS must be a valid positive integer")
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError("BENCH_DURATION_MS must be a valid positive integer") from error
    if value <= 0:
        raise ValueError("BENCH_DURATION_MS must be a valid positive integer")
    return value


def parse_total_requests() -> int | None:
    raw = os.getenv("BENCH_TOTAL_REQUESTS")
    if raw is None or raw.strip() == "":
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError("BENCH_TOTAL_REQUESTS must be a valid non-negative integer") from error
    if value < 0:
        raise ValueError("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
    return value


async def run_scenario(
    sender: Any,
    path: str,
    duration_ms: int,
    stop_requested: asyncio.Event,
) -> tuple[int, int, float]:
    completed = 0
    errors = 0
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + (duration_ms / 1000.0)
    while not stop_requested.is_set() and loop.time() < deadline:
        try:
            await sender.send(path)
            completed += 1
        except Exception:
            errors += 1
    return completed, errors, loop.time() - started


async def main() -> None:
    try:
        total_requests = parse_total_requests()
        duration_ms = parse_duration_ms() if total_requests is None else 0
    except ValueError as error:
        emit({"type": "error", "message": str(error)})
        raise SystemExit(1) from error

    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_requested.set)
        except NotImplementedError:
            signal.signal(sig, lambda _sig, _frame: stop_requested.set())

    sender = (
        Channel()
        .post("sync", lambda _request: 1)
        .post("async", lambda _request: asyncio.sleep(0, result=1))
        .process()
    )

    emit({"type": "ready", "pid": os.getpid()})

    if total_requests is not None:
        completed = 0
        errors = 0
        started = loop.time()
        while not stop_requested.is_set() and (completed + errors) < total_requests:
            try:
                await sender.send("sync")
                completed += 1
            except Exception:
                errors += 1

        sender.stop()
        emit(
            {
                "type": "result",
                "pid": os.getpid(),
                "completed": completed,
                "errors": errors,
                "elapsedSeconds": loop.time() - started,
            }
        )
        return

    sync_completed, sync_errors, sync_elapsed = await run_scenario(
        sender,
        "sync",
        duration_ms,
        stop_requested,
    )
    async_completed, async_errors, async_elapsed = await run_scenario(
        sender,
        "async",
        duration_ms,
        stop_requested,
    )
    sender.stop()

    emit(
        {
            "type": "result",
            "pid": os.getpid(),
            "syncCompleted": sync_completed,
            "syncErrors": sync_errors,
            "syncElapsedSeconds": sync_elapsed,
            "asyncCompleted": async_completed,
            "asyncErrors": async_errors,
            "asyncElapsedSeconds": async_elapsed,
            "asyncFallback": False,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
