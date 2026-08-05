from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from channel_py.channel import (
    CancellableRequest,
    Channel,
    ObjectMap,
    Subscription,
    Values,
    _as_async_iterator,
)
from channel_py.types import ChannelError


@pytest.mark.asyncio
async def test_cancellable_request_alias_and_cancel_impl() -> None:
    calls: list[str] = []

    request = CancellableRequest(lambda: calls.append("impl"))
    request.onCancel(lambda: calls.append("alias"))

    def boom() -> None:
        calls.append("boom")
        raise RuntimeError("ignored")

    request.on_cancel(boom)
    request.cancel()

    assert request.is_cancelled is True
    assert calls == ["alias", "boom", "impl"]


def test_object_map_helpers() -> None:
    object_map = ObjectMap()
    object_map["a"] = 1
    object_map.uncheckedSet("b", 2)

    mapped = object_map.map(lambda value: value * 10)
    values: list[tuple[str, int]] = []
    values_alias: list[tuple[str, int]] = []

    object_map.for_each(lambda value, key: values.append((str(key), int(value))))
    object_map.forEach(lambda value, key: values_alias.append((str(key), int(value))))

    assert sorted(mapped) == [10, 20]
    assert sorted(values) == [("a", 1), ("b", 2)]
    assert sorted(values_alias) == [("a", 1), ("b", 2)]


@pytest.mark.asyncio
async def test_subscription_parse_async_send_and_publish_skip_empty() -> None:
    subscription = Subscription()

    async def body(request: str) -> str:
        return request.upper()

    subscription._body = body
    events: list[tuple[str, str]] = []

    class Publisher:
        def publish(self, event: Any) -> None:
            events.append((event.topic, event.body))

    subscription.publishers.append(Publisher())

    output: dict[str, Subscription] = {}
    Subscription.parse({"root": {"leaf": subscription}}, "p/", output)

    assert "p/root/leaf" in output
    assert subscription.prefix == "p/root//leaf"

    # Empty payloads are ignored.
    subscription.publish("x", None)

    subscription.send("ok")
    await asyncio.sleep(0)

    assert events == [("p/root//leaf/ok", "OK")]


@pytest.mark.asyncio
async def test_values_done_and_error_paths() -> None:
    cancel_ids: list[int] = []

    values = Values(
        channel=Channel(),
        path="s",
        body=None,
        context=None,
        on_send=lambda _body: None,
        on_cancel=lambda rid: cancel_ids.append(rid),
        sender=Channel().process(),
    )

    # done path in next()
    values._queued.append({"done": True})
    assert await values.next() == {"value": None, "done": True}

    with pytest.raises(StopAsyncIteration):
        await values.__anext__()

    # No rid means no cancellation callback.
    values._cancel()
    assert cancel_ids == []

    values2 = Values(
        channel=Channel(),
        path="s",
        body=None,
        context=None,
        on_send=lambda _body: None,
        on_cancel=lambda rid: cancel_ids.append(rid),
        sender=Channel().process(),
    )
    values2._queued.append({"error": "boom"})
    with pytest.raises(ChannelError, match="boom"):
        await values2.__anext__()


@pytest.mark.asyncio
async def test_as_async_iterator_paths() -> None:
    async def agen() -> AsyncIterator[int]:
        yield 1

    direct = agen()
    assert await _as_async_iterator(direct) is direct

    class AiterOnly:
        def __aiter__(self) -> AsyncIterator[int]:
            return agen()

    aiter = await _as_async_iterator(AiterOnly())
    assert [value async for value in aiter] == [1]

    iterable = await _as_async_iterator([1, 2])
    assert [value async for value in iterable] == [1, 2]

    with pytest.raises(ChannelError, match="stream must return an iterator"):
        await _as_async_iterator(123)


@pytest.mark.asyncio
async def test_channel_aliases_parse_api_merge_and_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = Channel()

    channel.postOther(lambda path: path == "x", lambda _body: "ok")
    channel.streamOther(lambda path: path == "y", lambda _body: [])
    channel.onDisconnect(lambda _state, _sender: None)

    assert len(channel.other_post_api) == 1
    assert len(channel.other_stream_api) == 1
    assert channel._on_disconnect is not None

    req = channel.makeRequest("a", 1, {"x": 1}, lambda _response: None)
    stream_req = channel.makeStream("b", 2, {"x": 2}, lambda _response: None)
    sub_req = channel.makeSubscription("c", 3, {"x": 3}, lambda _response: None)

    assert req["path"] == "a"
    assert stream_req["stream"] == "b"
    assert sub_req["sub"] == "c"

    channel.receiveOne("not-a-dict", type("C", (), {"sender": Channel().process(), "state": {}})())

    async def async_gen(_body: Any):
        yield 1

    channel.api({"root": {"_": lambda _body: 1, "stream": async_gen}})
    assert "root" in channel.post_api
    assert "root/stream" in channel.stream_api

    other = Channel().post("hello", lambda _body: "x").stream("s", lambda _body: [])
    channel.merge(other, "p")
    assert "p/hello" in channel.post_api
    assert "p/s" in channel.stream_api

    monkeypatch.setattr("channel_py.client.connect_channel", lambda _ch, _address, _options=None: "connected")
    monkeypatch.setattr("channel_py.server.listen_channel", lambda _ch, _address, _options=None: "listened")
    monkeypatch.setattr("channel_py.process.process_channel", lambda _ch: "processed")

    assert channel.connect(1) == "connected"
    assert channel.listen(1) == "listened"
    assert channel.process() == "processed"
