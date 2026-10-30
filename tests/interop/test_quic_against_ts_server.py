"""The QUIC client against the TypeScript reference's fixture (channel/tests/server.ts), which serves its routes over
QUIC on UDP next to its WebSocket on TCP, on the same port. CHANNEL_TEST_PORT picks a fixture that already runs on
another port; without it the suite starts one on 2049 like the WebSocket interop tests."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("aioquic")

from channel_py import Channel, Subscription  # noqa: E402
from tests.fixtures.ts_server import wait_for_port  # noqa: E402

CERT = Path(__file__).resolve().parents[3] / "channel" / "tests" / "quic" / "cert.pem"
PORT = int(os.environ.get("CHANNEL_TEST_PORT", "2049"))


async def wait_for(ready: Any, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not ready():
        if loop.time() > deadline:
            raise TimeoutError("timed out")
        await asyncio.sleep(0.005)


@pytest.fixture
async def fixture_port(request: pytest.FixtureRequest) -> int:
    if not CERT.exists():
        pytest.skip("the TypeScript reference's test certificate isn't next to this checkout")
    if "CHANNEL_TEST_PORT" in os.environ:
        try:
            await wait_for_port("127.0.0.1", PORT, timeout=3)
        except TimeoutError:
            pytest.skip(f"no TypeScript fixture on 127.0.0.1:{PORT}")
    else:
        request.getfixturevalue("ts_server")
    return PORT


@pytest.fixture
async def client(fixture_port: int) -> Any:
    names = Subscription()

    async def stream_values(_body: Any):
        for i in range(3):
            yield i

    instance = (
        Channel()
        .post("hello", lambda _body: "client world")
        .stream("stream/values", stream_values)
        .events({"names": names})
        .connect(f"quic://127.0.0.1:{fixture_port}", {"quic": {"ca": CERT.read_text()}, "headers": lambda: {"X-Port": "py"}})
    )
    try:
        await wait_for(lambda: instance.ws.is_connected, 3)
    except TimeoutError:
        instance.stop()
        pytest.skip(f"the fixture on {fixture_port} doesn't serve QUIC (an older channel checkout)")
    yield instance, names
    instance.stop()
    await asyncio.sleep(0.01)


async def test_requests(client: Any) -> None:
    c, _ = client
    assert await c.send("hello") == "world"
    assert await c.send("echo", {"a": [1, "b"]}) == {"a": [1, "b"]}
    assert await c.send("mirror") == "client world"
    assert await c.send("auth", {"name": "quic"}) == "quic"
    assert await c.send("auth/name") == "quic"
    # The hello headers, lowercased
    assert (await c.send("v2/headers"))["x-port"] == "py"
    with pytest.raises(Exception):
        await c.send("v2/error/code")


async def test_streams_and_events(client: Any) -> None:
    c, names = client
    assert [value async for value in c.values("mirror/stream")] == [0, 1, 2]
    # The fixture subscribes to this client's names/1 and cancels after the first event
    await c.send("mirror/events")
    await asyncio.sleep(0.01)
    names.send("1", "h")
    await asyncio.sleep(0.05)
    assert "names/1" not in c.ws.topics


async def test_drop_and_reconnect(client: Any) -> None:
    c, _ = client
    # The fixture drops the connection without answering; the request fails, the client reconnects
    assert await c.send("v2/drops/reset") is None
    drop = c.request("v2/drop")
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(drop.response, 5)
    assert await asyncio.wait_for(c.send("hello"), 5) == "world"
    await asyncio.sleep(0.1)
    # Not sent again after reconnecting
    assert await c.send("v2/drops") == 1


async def test_pinned_certificate(fixture_port: int) -> None:
    fingerprint = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"
    c = Channel().connect(f"quic://localhost:{fixture_port}", {"quic": {"fingerprint": fingerprint}})
    try:
        assert await asyncio.wait_for(c.send("hello"), 5) == "world"
    finally:
        c.stop()
