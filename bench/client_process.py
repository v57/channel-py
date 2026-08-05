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


def _parse_clients() -> int:
    raw = os.getenv("BENCH_CLIENTS")
    if raw is None:
        raise ValueError("BENCH_CLIENTS must be set")
    clients = int(raw)
    if clients <= 0:
        raise ValueError("BENCH_CLIENTS must be a valid positive integer")
    return clients


def _parse_target_rps_per_client() -> float:
    raw = os.getenv("BENCH_RPS_PER_CLIENT")
    if raw is None:
        raise ValueError("BENCH_RPS_PER_CLIENT must be set")
    target = float(raw)
    if target <= 0:
        raise ValueError("BENCH_RPS_PER_CLIENT must be a positive number")
    return target


def _parse_total_requests() -> int | None:
    raw = os.getenv("BENCH_TOTAL_REQUESTS")
    if raw is None:
        return None
    value = int(raw)
    if value < 0:
        raise ValueError("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
    return value


def _parse_request_mode() -> str:
    mode = (os.getenv("BENCH_REQUEST_MODE") or "burst").strip().lower()
    if mode not in {"burst", "stream"}:
        raise ValueError("BENCH_REQUEST_MODE must be either burst or stream")
    return mode


def _parse_max_in_flight() -> int:
    raw = os.getenv("BENCH_MAX_IN_FLIGHT")
    if raw is None:
        return 4096
    value = int(raw)
    if value <= 0:
        raise ValueError("BENCH_MAX_IN_FLIGHT must be a valid positive integer")
    return value


def _install_uvloop() -> None:
    if os.name == "nt":
        return
    try:
        import uvloop

        uvloop.install()
    except Exception:
        pass


async def _wait_for_connections(connected: asyncio.Event, timeout_seconds: float) -> None:
    await asyncio.wait_for(connected.wait(), timeout=timeout_seconds)


async def main() -> None:
    try:
        port = _parse_port()
        clients_count = _parse_clients()
        total_requests = _parse_total_requests()
        request_mode = _parse_request_mode()
        max_in_flight = _parse_max_in_flight()
        target_rps_per_client = (
            _parse_target_rps_per_client() if total_requests is None else 0.0
        )
        host = os.getenv("BENCH_HOST", "127.0.0.1")
    except Exception as error:
        _emit({"type": "error", "message": str(error)})
        raise SystemExit(1)

    address = f"ws://{host}:{port}"
    channel = Channel()
    clients: list[Any] = []
    connected_flags = [False] * clients_count
    connected_count = 0
    connected_event = asyncio.Event()

    target_rps = clients_count * target_rps_per_client
    tick_ms = 10

    next_client_index = 0
    requests_sent = 0
    responses_received = 0
    errors = 0
    in_flight = 0
    running = False
    stopped = False
    in_flight_responses: set[asyncio.Future[Any]] = set()
    completion_event = asyncio.Event()
    scheduler_task: asyncio.Task[None] | None = None
    started_at = time.perf_counter()

    def make_on_connect(index: int):
        async def on_connect(_sender: Any) -> None:
            nonlocal connected_count
            if connected_flags[index]:
                return
            connected_flags[index] = True
            connected_count += 1
            if connected_count == clients_count:
                connected_event.set()

        return on_connect

    for index in range(clients_count):
        sender = channel.connect(address, {"onConnect": make_on_connect(index)})
        clients.append(sender)

    try:
        await _wait_for_connections(connected_event, timeout_seconds=10.0)
    except asyncio.TimeoutError:
        _emit(
            {
                "type": "error",
                "message": f"Timed out waiting for clients to connect ({connected_count}/{clients_count})",
            }
        )
        for client in clients:
            client.stop()
        raise SystemExit(1)

    running = True
    _emit({"type": "ready", "pid": os.getpid(), "clients": clients_count})

    def _on_response_done(future: asyncio.Future[Any]) -> None:
        nonlocal responses_received, errors, in_flight
        in_flight_responses.discard(future)
        in_flight = max(0, in_flight - 1)
        try:
            if future.cancelled():
                errors += 1
            elif future.exception() is not None:
                errors += 1
            else:
                responses_received += 1
        except Exception:
            errors += 1
        completion_event.set()

    async def stop_client() -> None:
        nonlocal running, stopped
        if stopped:
            return
        stopped = True
        running = False

        if scheduler_task is not None:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass

        if in_flight_responses:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(in_flight_responses), return_exceptions=True),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                pass

        for client in clients:
            client.stop()

        elapsed_seconds = time.perf_counter() - started_at
        _emit(
            {
                "type": "result",
                "pid": os.getpid(),
                "requestsSent": requests_sent,
                "responsesReceived": responses_received,
                "errors": errors,
                "elapsedSeconds": elapsed_seconds,
            }
        )
        raise SystemExit(0)

    def dispatch_one() -> bool:
        nonlocal next_client_index, requests_sent, in_flight
        if in_flight >= max_in_flight:
            return False
        sender = clients[next_client_index]
        next_client_index = (next_client_index + 1) % len(clients)
        request = sender.request("text/count", {"text": "hello"})
        response = asyncio.ensure_future(request.response)
        requests_sent += 1
        in_flight += 1
        in_flight_responses.add(response)
        response.add_done_callback(_on_response_done)
        return True

    def fixed_mode_done(target: int) -> bool:
        accounted = responses_received + errors
        return requests_sent >= target and accounted >= target and in_flight <= 0

    async def fixed_mode_scheduler(target: int) -> None:
        if target == 0:
            await stop_client()
            return

        while running:
            dispatched = False
            if request_mode == "stream":
                while running and requests_sent < target and in_flight < max_in_flight:
                    if not dispatch_one():
                        break
                    dispatched = True
            else:
                budget = 2000
                while running and requests_sent < target and in_flight < max_in_flight and budget > 0:
                    if not dispatch_one():
                        break
                    dispatched = True
                    budget -= 1

            if fixed_mode_done(target):
                await stop_client()
                return

            if not dispatched and in_flight >= max_in_flight:
                try:
                    await asyncio.wait_for(completion_event.wait(), timeout=0.01)
                except asyncio.TimeoutError:
                    pass
                completion_event.clear()
            else:
                await asyncio.sleep(0.001)

    async def scheduler() -> None:
        nonlocal next_client_index, requests_sent, in_flight
        carry = 0.0
        tick_seconds = tick_ms / 1000.0
        while running:
            await asyncio.sleep(tick_seconds)
            carry += (target_rps * tick_ms) / 1000.0
            planned = int(carry)
            sent = 0
            while sent < planned and running and in_flight < max_in_flight:
                if not dispatch_one():
                    break
                sent += 1
            carry -= sent
            if sent < planned and in_flight >= max_in_flight:
                try:
                    await asyncio.wait_for(completion_event.wait(), timeout=tick_seconds)
                except asyncio.TimeoutError:
                    pass
                completion_event.clear()

    if total_requests is None:
        scheduler_task = asyncio.create_task(scheduler())
    else:
        scheduler_task = asyncio.create_task(fixed_mode_scheduler(total_requests))

    stop_event = asyncio.Event()

    def on_signal() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, on_signal)
        loop.add_signal_handler(signal.SIGINT, on_signal)
    except NotImplementedError:
        pass

    await stop_event.wait()
    await stop_client()


if __name__ == "__main__":
    _install_uvloop()
    asyncio.run(main())
