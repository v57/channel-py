from __future__ import annotations

import asyncio
from typing import Any

import pytest

from channel_py import Channel, Subscription


async def sleep(seconds: float = 0.005) -> None:
    await asyncio.sleep(seconds)


@pytest.mark.asyncio
async def test_python_server_and_client_end_to_end() -> None:
    updates = Subscription()

    async def stream_values(_body: Any):
        for i in range(3):
            yield i
            await sleep()

    channel = (
        Channel()
        .post("hello", lambda _body: "world")
        .post("echo", lambda body: body.body)
        .stream("stream/values", stream_values)
        .events({"updates": updates})
    )

    server = channel.listen(0)
    await server.wait_started()

    client = Channel().connect(server.port)
    await sleep(0.05)

    assert await asyncio.wait_for(client.send("hello"), timeout=2) == "world"
    assert await asyncio.wait_for(client.send("echo", {"x": 1}), timeout=2) == {"x": 1}

    stream: list[int] = []
    async for value in client.values("stream/values"):
        stream.append(value)
    assert stream == [0, 1, 2]

    events: list[str] = []
    subscription = await asyncio.wait_for(
        client.subscribe("updates", "user-1", lambda event: events.append(str(event))),
        timeout=2,
    )
    updates.send("user-1", "first")
    updates.send("user-1", "second")
    await sleep(0.05)

    assert events == ["first", "second"]

    subscription.cancel()
    updates.send("user-1", "third")
    await sleep(0.05)
    assert events == ["first", "second"]

    client.stop()
    await asyncio.wait_for(server.stop_async(), timeout=2)
