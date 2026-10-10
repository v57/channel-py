"""Client against misbehaving servers: pending calls must settle, never hang (AUDIT2 §2)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from websockets.legacy.server import serve as ws_serve

from channel_py import Channel, ChannelError

PORT = 6601


async def _serve(handler: Any, port: int = PORT, **kwargs: Any) -> Any:
    async def wrapped(websocket: Any, _path: str = "/") -> None:
        await handler(websocket)

    return await ws_serve(wrapped, "127.0.0.1", port, max_size=None, **kwargs)


async def _first_request(websocket: Any) -> dict[str, Any]:
    message = json.loads(await websocket.recv())
    return message[0] if isinstance(message, list) else message


@pytest.mark.asyncio
async def test_unparseable_reply_fails_the_pending_call() -> None:
    async def handler(websocket: Any) -> None:
        await _first_request(websocket)
        await websocket.send("{not json")
        await asyncio.sleep(2)

    server = await _serve(handler)
    client = Channel().connect(PORT)
    try:
        with pytest.raises(ChannelError, match="invalid message"):
            await asyncio.wait_for(client.send("x"), timeout=2)
    finally:
        client.stop()
        server.close()


@pytest.mark.asyncio
async def test_close_without_reply_fails_the_pending_call() -> None:
    async def handler(websocket: Any) -> None:
        await _first_request(websocket)
        await websocket.close()

    server = await _serve(handler)
    client = Channel().connect(PORT)
    try:
        with pytest.raises(ChannelError, match="disconnected"):
            await asyncio.wait_for(client.send("x"), timeout=2)
    finally:
        client.stop()
        server.close()


@pytest.mark.asyncio
async def test_rejected_handshake_fails_the_pending_call() -> None:
    async def reject(_path: str, _headers: Any) -> Any:
        return (500, [], b"nope")

    async def handler(websocket: Any) -> None:
        await asyncio.sleep(1)

    server = await _serve(handler, process_request=reject)
    client = Channel().connect(PORT)
    try:
        with pytest.raises(ChannelError, match="HTTP 500"):
            await asyncio.wait_for(client.send("x"), timeout=2)
    finally:
        client.stop()
        server.close()


@pytest.mark.asyncio
async def test_reply_over_one_mib_is_accepted_and_over_limit_fails() -> None:
    big = "a" * (8 * 1024 * 1024)

    async def handler(websocket: Any) -> None:
        request = await _first_request(websocket)
        await websocket.send(json.dumps({"id": request["id"], "body": big}))
        await asyncio.sleep(2)

    server = await _serve(handler)
    client = Channel().connect(PORT)
    small = Channel().connect(PORT, {"max_payload": 1024})
    try:
        assert await asyncio.wait_for(client.send("x"), timeout=5) == big
        with pytest.raises(ChannelError, match="1009"):
            await asyncio.wait_for(small.send("x"), timeout=5)
    finally:
        client.stop()
        small.stop()
        server.close()


@pytest.mark.asyncio
async def test_request_before_server_is_up_is_resent_on_connect() -> None:
    client = Channel().connect(PORT)
    call = asyncio.ensure_future(client.send("hello"))
    await asyncio.sleep(0.3)  # a few failed connect attempts

    server = Channel().post("hello", lambda _body: "world").listen(PORT)
    await server.wait_started()
    try:
        assert await asyncio.wait_for(call, timeout=2) == "world"
    finally:
        client.stop()
        await server.stop_async()


@pytest.mark.asyncio
async def test_server_state_sees_lowercased_headers_that_win_over_query() -> None:
    seen: list[dict[str, Any]] = []

    def state(headers: dict[str, Any]) -> dict[str, Any]:
        seen.append(headers)
        return {}

    server = Channel().post("hello", lambda _body: "world").listen(PORT, {"state": state})
    await server.wait_started()
    client = Channel().connect(f"ws://127.0.0.1:{PORT}/?x-user=admin&q=1", {"headers": lambda: {"X-User": "alice"}})
    try:
        assert await asyncio.wait_for(client.send("hello"), timeout=2) == "world"
        assert seen[0]["x-user"] == "alice"
        assert seen[0]["q"] == "1"
        assert "X-User" not in seen[0]
    finally:
        client.stop()
        await server.stop_async()
