"""A response answers a request only on the connection the request went to.

Request ids are one counter for every connection, so on a server that forwards calls to a service it holds the
sender of (like the Hub does), another peer must not be able to answer, feed or end what the server asked the
service.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from websockets.legacy.client import connect as ws_connect

from channel_py import Channel


def relay() -> tuple[Any, Channel[Any], asyncio.Event]:
    service: list[Any] = []
    answered = asyncio.Event()

    def register(body: Any) -> bool:
        service.append(body.sender)
        return True

    async def call(body: Any) -> Any:
        return await service[0].send("svc/read", body.body)

    async def watch(_body: Any):
        async for value in service[0].values("svc/values"):
            yield value

    server = Channel().post("register", register).post("call", call).stream("watch", watch).listen(0)

    async def read(_body: Any) -> str:
        await answered.wait()
        return "real"

    async def values(_body: Any):
        await answered.wait()
        yield "real"

    service_channel = Channel().post("svc/read", read).stream("svc/values", values)
    return server, service_channel, answered


async def spray(port: int, *frames: list[dict[str, Any]]) -> Any:
    attacker = await ws_connect(f"ws://127.0.0.1:{port}")
    for frame in frames:
        await attacker.send(json.dumps(frame))
    return attacker


@pytest.mark.asyncio
async def test_a_response_from_another_connection_answers_no_forwarded_call() -> None:
    server, service_channel, answered = relay()
    await server.wait_started()
    service = service_channel.connect(server.port)
    assert await asyncio.wait_for(service.send("register"), timeout=2) is True
    victim = Channel().connect(server.port)
    call = asyncio.ensure_future(victim.send("call", "x"))
    await asyncio.sleep(0.05)

    attacker = await spray(
        server.port,
        [{"id": i, "body": "forged"} for i in range(256)],
        [{"id": i, "topic": "x", "body": "forged"} for i in range(256)],
    )
    await asyncio.sleep(0.05)
    answered.set()

    assert await asyncio.wait_for(call, timeout=2) == "real"
    await attacker.close()
    victim.stop()
    service.stop()
    await asyncio.wait_for(server.stop_async(), timeout=2)


@pytest.mark.asyncio
async def test_another_connection_can_neither_feed_nor_end_a_forwarded_stream() -> None:
    server, service_channel, answered = relay()
    await server.wait_started()
    service = service_channel.connect(server.port)
    assert await asyncio.wait_for(service.send("register"), timeout=2) is True
    victim = Channel().connect(server.port)
    received: list[Any] = []

    async def read() -> None:
        async for value in victim.values("watch"):
            received.append(value)

    reading = asyncio.ensure_future(read())
    await asyncio.sleep(0.05)

    attacker = await spray(
        server.port,
        [{"id": i, "body": "forged"} for i in range(256)],
        [{"id": i, "done": True} for i in range(256)],
    )
    await asyncio.sleep(0.05)
    answered.set()

    await asyncio.wait_for(reading, timeout=2)
    assert received == ["real"]
    await attacker.close()
    victim.stop()
    service.stop()
    await asyncio.wait_for(server.stop_async(), timeout=2)


@pytest.mark.asyncio
async def test_the_connection_a_request_went_to_still_answers_it() -> None:
    async def mirror(body: Any) -> Any:
        return await body.sender.send("hello")

    server = Channel().post("mirror", mirror).listen(0)
    await server.wait_started()
    client = Channel().post("hello", lambda _body: "client").connect(server.port)

    assert await asyncio.wait_for(client.send("mirror"), timeout=2) == "client"
    client.stop()
    await asyncio.wait_for(server.stop_async(), timeout=2)
