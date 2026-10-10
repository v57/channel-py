from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosed

import channel_py.client as client_module
from channel_py import Channel, Subscription
from channel_py.client import ClientSender, ConnectOptions, WebSocketClient, _option_value, connect_channel


class DelegatingSender:
    def __init__(self) -> None:
        self.streams = {1: "a"}
        self.requests = {2: "b"}
        self.stopped = False

    async def send(self, path: str, body: Any = None, context: Any = None) -> Any:
        return (path, body, context)

    def request(self, path: str, body: Any = None, context: Any = None) -> Any:
        return ("request", path, body, context)

    def values(self, path: str, body: Any = None, context: Any = None) -> Any:
        return ("values", path, body, context)

    async def subscribe(self, path: str, body: Any, event: Any, context: Any = None) -> Any:
        return ("subscribe", path, body, context)

    def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_client_sender_delegates() -> None:
    inner = DelegatingSender()
    sender = ClientSender(inner, ws=SimpleNamespace())

    assert sender.streams == {1: "a"}
    assert sender.requests == {2: "b"}
    assert await sender.send("x", 1, 2) == ("x", 1, 2)
    assert sender.request("x", 1, 2) == ("request", "x", 1, 2)
    assert sender.values("x", 1, 2) == ("values", "x", 1, 2)
    assert await sender.subscribe("x", 1, lambda _v: None, 2) == ("subscribe", "x", 1, 2)

    sender.stop()
    assert inner.stopped is True


@pytest.mark.asyncio
async def test_websocket_client_run_success_path(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_kwargs: list[dict[str, Any]] = []

    class FakeWebSocket:
        def __init__(self) -> None:
            self.sent: list[str] = []
            self._messages = ["{\"topic\":\"t\",\"body\":1}"]

        async def send(self, payload: str) -> None:
            self.sent.append(payload)

        def __aiter__(self) -> "FakeWebSocket":
            return self

        async def __anext__(self) -> str:
            if self._messages:
                return self._messages.pop(0)
            raise StopAsyncIteration

        async def close(self) -> None:
            return None

    fake_ws = FakeWebSocket()

    async def fake_connect(address: str, **kwargs: Any) -> Any:
        connect_kwargs.append(kwargs)
        return fake_ws

    async def fake_sleep(_delay: float) -> None:
        client.is_running = False

    monkeypatch.setattr(client_module, "ws_connect", fake_connect)
    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)

    client = WebSocketClient("ws://unit", headers=lambda: {"x": "1"})
    client.pending[999] = {"pending": True}
    client._enqueue_outbox({"stale": True})

    opened: list[int] = []
    closed: list[int] = []
    messages: list[Any] = []

    async def on_open() -> None:
        opened.append(1)

    client.onopen = on_open
    client.onclose = lambda: closed.append(1)
    client.onmessage = lambda msg: messages.append(msg)

    await asyncio.wait_for(client._run_task, timeout=1)

    assert connect_kwargs and connect_kwargs[0] == {
        "extra_headers": {"x": "1"},
        "compression": None,
        "max_size": 16 * 1024 * 1024,
        "max_queue": None,
        "read_limit": 1_048_576,
        "write_limit": 1_048_576,
        "ping_interval": None,
        "close_timeout": 1,
    }
    assert opened == [1]
    assert closed == [1]
    assert messages == [{"topic": "t", "body": 1}]
    assert client.ws is None
    assert client.is_connected is False
    assert list(client._outbox) == [[{"pending": True}]]
    client.stop()


@pytest.mark.asyncio
async def test_websocket_client_run_connection_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class ClosingWebSocket:
        def __aiter__(self) -> "ClosingWebSocket":
            return self

        async def __anext__(self) -> Any:
            raise ConnectionClosed(None, None)

        async def send(self, _payload: str) -> None:
            return None

        async def close(self) -> None:
            return None

    async def fake_connect(_address: str, **_kwargs: Any) -> Any:
        return ClosingWebSocket()

    monkeypatch.setattr(client_module, "ws_connect", fake_connect)

    client = WebSocketClient("ws://unit")
    client.onclose = lambda: setattr(client, "is_running", False)

    await asyncio.wait_for(client._run_task, timeout=1)
    assert client.is_running is False


@pytest.mark.asyncio
async def test_websocket_client_run_error_records_reason_and_keeps_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_connect(_address: str, **_kwargs: Any) -> Any:
        raise RuntimeError("handshake failed 101")

    monkeypatch.setattr(client_module, "ws_connect", fake_connect)

    client = WebSocketClient("ws://unit")
    reasons: list[Any] = []

    def on_close() -> None:
        reasons.append(client.close_error)
        client.is_running = False

    client.onclose = on_close
    await asyncio.wait_for(client._run_task, timeout=1)

    assert reasons == ["handshake failed 101"]


@pytest.mark.asyncio
async def test_websocket_client_methods_and_state_machine(monkeypatch: pytest.MonkeyPatch) -> None:
    async def idle_run(self: WebSocketClient) -> None:
        while self.is_running:
            await asyncio.sleep(0)

    monkeypatch.setattr(WebSocketClient, "_run", idle_run)

    class FakeWebSocket:
        def __init__(self) -> None:
            self.sent: list[str] = []
            self.closed = 0

        async def send(self, payload: str) -> None:
            self.sent.append(payload)

        async def close(self) -> None:
            self.closed += 1

    ws = FakeWebSocket()
    client = WebSocketClient("ws://unit")
    first = client.send({"one": 1})
    second = client.send({"two": 2})
    assert first == 0
    assert second == 1

    client.ws = ws
    client.is_connected = True
    client._outbox_event.set()
    await asyncio.sleep(0.01)

    assert ws.sent
    assert client_module.loads(ws.sent[0]) == [{"one": 1}, {"two": 2}]

    assert client.cancel(99999) is False
    pending_id = client.send({"pending": 1})
    client.is_connected = True
    assert client.cancel(pending_id) is False
    client.is_connected = False
    assert client.cancel(pending_id) is True

    sent_id = client.send({"x": 1})
    client.sent(sent_id)
    assert sent_id not in client.pending

    client.throttle()

    received: list[Any] = []
    cancel1 = client.add_topic("topic", lambda event: received.append((1, event)))
    cancel2 = client.add_topic("topic", lambda event: received.append((2, event)))

    client.received_event("topic", "hello")
    assert received == [(1, "hello"), (2, "hello")]

    assert cancel1() is False
    assert cancel2() is True

    client.ws = None
    client.notify({"no": "ws"})
    assert client._outbox

    client.stop()
    await asyncio.sleep(0)
    assert client.pending == {}
    assert not client._outbox


@pytest.mark.asyncio
async def test_websocket_client_resolve_headers_async_and_send_now_without_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    async def idle_run(self: WebSocketClient) -> None:
        while self.is_running:
            await asyncio.sleep(0)

    async def make_headers() -> dict[str, str]:
        return {"async": "yes"}

    monkeypatch.setattr(WebSocketClient, "_run", idle_run)
    client = WebSocketClient("ws://unit", headers=make_headers)
    assert await client._resolve_headers() == {"async": "yes"}

    client.notify({"queued": True})
    await asyncio.sleep(0)
    assert client._outbox

    class FakeWebSocket:
        sent: list[str]

        def __init__(self) -> None:
            self.sent = []

        async def send(self, payload: str) -> None:
            self.sent.append(payload)

        async def close(self) -> None:
            return None

    ws = FakeWebSocket()
    client.ws = ws
    client._outbox_event.set()
    await asyncio.sleep(0.01)
    assert ws.sent

    client.stop()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_option_value_and_connect_channel_with_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _option_value(None, "a", "b") is None
    assert _option_value({"a": 1}, "a", "b") == 1

    class Opt:
        a = 2

    assert _option_value(Opt(), "a", "b") == 2

    class FakeTransport:
        def __init__(self, address: str, headers: Any) -> None:
            self.address = address
            self.headers = headers
            self.id = 0
            self.onopen = None
            self.onclose = None
            self.onmessage = None
            self.is_running = True
            self.notified: list[Any] = []
            self.sent: list[Any] = []
            self._topics: dict[str, dict[int, Any]] = {}

        def send(self, body: Any) -> int:
            self.sent.append(body)
            rid = self.id
            self.id += 1
            return rid

        def sent(self, _id: int) -> None:
            return None

        def cancel(self, _id: int) -> bool:
            return False

        def notify(self, body: Any) -> None:
            self.notified.append(body)

        def add_topic(self, topic: str, event: Any) -> Any:
            handlers = self._topics.setdefault(topic, {})
            key = len(handlers) + 1
            handlers[key] = event

            def cancel() -> bool:
                handlers.pop(key, None)
                if handlers:
                    return False
                self._topics.pop(topic, None)
                return True

            return cancel

        def stop(self) -> None:
            self.is_running = False

        def received_event(self, topic: str, body: Any) -> None:
            for callback in list(self._topics.get(topic, {}).values()):
                callback(body)

    monkeypatch.setattr(client_module, "WebSocketClient", FakeTransport)

    subscription = Subscription()
    channel = Channel().events({"news": subscription})
    disconnect_calls: list[int] = []

    original_disconnect = channel.disconnect

    def disconnect(state: Any, sender: Any) -> None:
        disconnect_calls.append(1)
        original_disconnect(state, sender)

    channel.disconnect = disconnect  # type: ignore[assignment]

    opened: list[int] = []

    async def on_connect(_sender: Any) -> None:
        opened.append(1)

    client = connect_channel(channel, 1234, ConnectOptions(on_connect=on_connect))

    assert client.ws.address == "ws://127.0.0.1:1234"

    await client.ws.onopen()  # type: ignore[misc]
    assert opened == [1]

    # subscribe so publisher has an active topic
    client.ws.onmessage({"id": 1, "sub": "news", "body": "room"})  # type: ignore[misc]
    subscription.publish("room", "event")

    assert any(item.get("topic") == "news/room" for item in client.ws.notified)

    client.ws.is_running = False
    client.ws.onclose()  # type: ignore[misc]
    client.ws.onclose()  # type: ignore[misc]  # second call covers already-cleaned branch

    assert disconnect_calls
