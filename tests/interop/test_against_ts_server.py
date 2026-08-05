from __future__ import annotations

import asyncio
from typing import Any

import pytest

from channel_py import Channel, Subscription


async def sleep(seconds: float = 0.001) -> None:
    await asyncio.sleep(seconds)


@pytest.fixture
async def client(ts_server: None) -> Any:
    stats = {"values_sent": 0}
    client_events = {"names": Subscription()}

    async def stream_values(_body: Any):
        for i in range(3):
            yield i
            await sleep()

    async def stream_cancel(_body: Any):
        for i in range(10):
            yield i
            stats["values_sent"] += 1
            await sleep(0.01)

    instance = (
        Channel()
        .api(
            {
                "hello": lambda _body: "client world",
                "stream": {
                    "values": stream_values,
                    "cancel": stream_cancel,
                },
            }
        )
        .events(client_events)
        .connect(2049)
    )

    # The websocket reconnect loop connects very quickly, but this avoids racey first requests.
    await sleep(0.05)
    yield instance, stats, client_events
    instance.stop()
    await sleep(0.01)


@pytest.mark.asyncio
async def test_post_hello(client: Any) -> None:
    instance, _, _ = client
    response = await instance.send("hello")
    assert response == "world"


@pytest.mark.asyncio
async def test_post_echo(client: Any) -> None:
    instance, _, _ = client
    value = 0.1234567
    response = await instance.send("echo", value)
    assert response == value


@pytest.mark.asyncio
async def test_post_empty(client: Any) -> None:
    instance, _, _ = client
    response = await instance.send("empty")
    assert response is None


@pytest.mark.asyncio
async def test_post_not_found(client: Any) -> None:
    instance, _, _ = client
    with pytest.raises(Exception, match="api not found"):
        await instance.send("notfound")


@pytest.mark.asyncio
async def test_subscription_not_found(client: Any) -> None:
    instance, _, _ = client
    with pytest.raises(Exception, match="subscription not found"):
        await instance.subscribe("notfound", "test", lambda _event: None)


@pytest.mark.asyncio
async def test_stream_values(client: Any) -> None:
    instance, _, _ = client
    values: list[int] = []
    async for value in instance.values("stream/values"):
        values.append(value)
    assert values == [0, 1, 2]


@pytest.mark.asyncio
async def test_stream_cancel(client: Any) -> None:
    instance, _, _ = client
    values: list[int] = []
    async for value in instance.values("stream/cancel"):
        values.append(value)
        if value == 2:
            break
    assert values == [0, 1, 2]


@pytest.mark.asyncio
async def test_server_post_mirror(client: Any) -> None:
    instance, _, _ = client
    response = await instance.send("mirror")
    assert response == "client world"


@pytest.mark.asyncio
async def test_server_subscribe_and_unsubscribe(client: Any) -> None:
    instance, _, client_events = client

    await instance.send("mirror/events")
    await sleep(0.01)

    client_events["names"].send("2", "po")
    client_events["names"].send("1", "h")
    await sleep(0.01)
    client_events["names"].send("1", "we")
    await sleep(0.05)

    assert "names/1" not in instance.ws.topics


@pytest.mark.asyncio
async def test_server_stream_mirror(client: Any) -> None:
    instance, _, _ = client
    values: list[int] = []
    async for value in instance.values("mirror/stream"):
        values.append(value)
    assert values == [0, 1, 2]


@pytest.mark.asyncio
async def test_server_stream_mirror_cancel(client: Any) -> None:
    instance, stats, _ = client
    values: list[int] = []

    async for value in instance.values("mirror/stream/cancel"):
        values.append(value)
        if value == 2:
            break

    await sleep(0.05)
    assert values == [0, 1, 2]
    assert 3 <= stats["values_sent"] < 10


@pytest.mark.asyncio
async def test_state_auth(client: Any) -> None:
    _, _, _ = client

    authenticated = Channel().connect(2049)
    try:
        await authenticated.send("auth", {"name": "tester"})
        name = await authenticated.send("auth/name")
        assert name == "tester"
    finally:
        authenticated.stop()


@pytest.mark.asyncio
async def test_state_unauthorized(client: Any) -> None:
    _, _, _ = client

    unauthenticated = Channel().connect(2049)
    try:
        with pytest.raises(Exception, match="unauthorized"):
            await unauthenticated.send("auth/name")
    finally:
        unauthenticated.stop()
