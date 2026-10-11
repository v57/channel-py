"""The client used to retry every 100 ms forever while the server was down (TS parity: channel docs/client.md)."""

from __future__ import annotations

import asyncio

import pytest

from channel_py.client import WebSocketClient, reconnect_delay


def test_reconnect_delay_doubles_up_to_5_seconds() -> None:
    assert [reconnect_delay(attempt, 1) for attempt in range(8)] == pytest.approx(
        [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0]
    )
    assert reconnect_delay(0, 0) == pytest.approx(0.05)
    assert reconnect_delay(1000, 0) == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_client_backs_off_while_the_server_refuses_the_upgrade() -> None:
    # Every request is refused with HTTP 500: each connection is one failed attempt.
    attempts = 0

    async def refuse(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal attempts
        attempts += 1
        await reader.read(4096)
        writer.write(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(refuse, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = WebSocketClient(f"ws://127.0.0.1:{port}")
    await asyncio.sleep(2)
    client.stop()
    server.close()
    await server.wait_closed()

    # 100 ms retries would make ~20 attempts in 2 s; backoff allows at most 1 + 6 (waits of 50..100, 100..200, ...).
    assert 2 <= attempts <= 8
