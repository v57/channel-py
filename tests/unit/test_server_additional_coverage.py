from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

import channel_py.server as server_module
from channel_py import Channel, Subscription
from channel_py.server import (
    BodyContext,
    ListenOptions,
    RunningServer,
    Subscriptions,
    _ClientContext,
    _Connection,
    listen_channel,
)


class FakeWebSocket:
    def __init__(
        self,
        messages: list[Any] | None = None,
        fail_send: bool = False,
        fail_close: bool = False,
        iter_error: Exception | None = None,
    ) -> None:
        self.messages = list(messages or [])
        self.fail_send = fail_send
        self.fail_close = fail_close
        self.iter_error = iter_error
        self.sent: list[str] = []
        self.closed = 0
        self.request_headers = {"x-token": "abc"}

    async def send(self, payload: str) -> None:
        if self.fail_send:
            raise RuntimeError("send failed")
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("close failed")

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> Any:
        if self.iter_error is not None:
            error = self.iter_error
            self.iter_error = None
            raise error
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


@pytest.mark.asyncio
async def test_subscriptions_and_connection_methods() -> None:
    subscriptions = Subscriptions()
    received: list[Any] = []

    cancel1 = subscriptions.add_topic("topic", lambda value: received.append((1, value)))
    cancel2 = subscriptions.add_topic("topic", lambda value: received.append((2, value)))

    subscriptions.received_event("topic", "v")
    assert received == [(1, "v"), (2, "v")]
    assert cancel1() is False
    assert cancel2() is True

    ws = FakeWebSocket(fail_send=True)
    connection = _Connection(ws, subscriptions)

    assert connection.send({"x": 1}) == 0
    await asyncio.sleep(0)

    connection.notify({"y": 2})
    await asyncio.sleep(0)

    assert connection.sent(1) is None
    assert connection.cancel(1) is False
    assert connection.add_topic("t", lambda _v: None)

    connection.stop()
    await asyncio.sleep(0)
    assert ws.closed == 1


@pytest.mark.asyncio
async def test_connection_outbox_batches_and_close_cleans_up() -> None:
    ws = FakeWebSocket()
    connection = _Connection(ws, Subscriptions())

    assert connection.send({"x": 1}) == 0
    connection.notify({"y": 2})
    await asyncio.sleep(0.01)

    assert ws.sent
    assert server_module.loads(ws.sent[0]) == [{"x": 1}, {"y": 2}]

    connection.close()
    await asyncio.sleep(0)
    assert not connection._outbox


@pytest.mark.asyncio
async def test_running_server_helpers_and_resolve_state(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_start(self: RunningServer) -> None:
        self._started.set()

    monkeypatch.setattr(RunningServer, "_start", fake_start)

    channel = Channel()
    server = RunningServer(channel, "127.0.0.1", 9999, options=None)
    await server.wait_started()

    # port property branches
    assert server.port == 9999
    server._server = SimpleNamespace(sockets=None)
    assert server.port == 9999
    server._server = SimpleNamespace(sockets=[SimpleNamespace(getsockname=lambda: ("127.0.0.1", 5432))])
    assert server.port == 5432

    assert server._option_value("a", "b") is None
    server._options = {"a": 1}
    assert server._option_value("a", "b") == 1

    @dataclass
    class Opt:
        a: int = 2

    server._options = Opt()
    assert server._option_value("a", "b") == 2

    ws = FakeWebSocket()

    server._options = {"state": lambda headers: {"headers": headers}}
    resolved = await server._resolve_state(ws, "/?q=1")
    assert resolved["headers"]["q"] == "1"
    assert resolved["headers"]["x-token"] == "abc"

    async def make_state(headers: dict[str, str | None]) -> Any:
        return {"async": headers.get("x-token")}

    server._options = {"state": make_state}
    resolved_async = await server._resolve_state(ws, "/")
    assert resolved_async == {"async": "abc"}

    called: list[str] = []

    async def cb(_body: BodyContext) -> None:
        called.append("cb")

    await server._call_optional(None, BodyContext(state={}, sender=None, subscriptions=Subscriptions()))
    await server._call_optional(cb, BodyContext(state={}, sender=None, subscriptions=Subscriptions()))
    assert called == ["cb"]

    # no-events branch in _attach_publishers
    empty_server = RunningServer(Channel(), "127.0.0.1", 9998, options=None)
    await empty_server.wait_started()
    empty_server._attach_publishers()
    await empty_server.stop_async()


@pytest.mark.asyncio
async def test_running_server_handle_client_publishers_and_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_start(self: RunningServer) -> None:
        self._started.set()

    monkeypatch.setattr(RunningServer, "_start", fake_start)

    updates = Subscription()
    channel = Channel().post("hello", lambda _body: "world").events({"updates": updates})

    hooks: list[str] = []

    async def on_connect(_ctx: BodyContext) -> None:
        hooks.append("connect")

    async def on_disconnect(_ctx: BodyContext) -> None:
        hooks.append("disconnect")

    server = RunningServer(
        channel,
        "127.0.0.1",
        0,
        options=ListenOptions(on_connect=on_connect, on_disconnect=on_disconnect),
    )
    await server.wait_started()

    ws = FakeWebSocket(messages=[b"ignore", '{"id":1,"path":"hello"}', '{"topic":"updates/x","body":"e"}', "not-json"])
    await server._handle_client(ws, "/")
    await asyncio.sleep(0)

    assert hooks == ["connect", "disconnect"]
    assert ws.closed >= 1
    assert not server._clients

    # Attach publishers and publish to a subscribed client.
    notifications: list[Any] = []

    class NotifyConn:
        def notify(self, body: Any) -> None:
            notifications.append(body)

    subscribed_client = _ClientContext(
        websocket=FakeWebSocket(),
        sender=None,
        state={},
        subscriptions=Subscriptions(),
        subscribed_topics={"updates/a"},
        connection=NotifyConn(),  # type: ignore[arg-type]
    )
    server._clients.append(subscribed_client)

    server._attach_publishers()
    updates.publish("a", "payload")
    assert notifications == [{"topic": "updates/a", "body": "payload"}]

    before = len(updates.publishers)
    server._cleanup_publishers()
    after = len(updates.publishers)
    server._cleanup_publishers()
    assert after <= before

    # stop_async catches client close errors
    bad_client = _ClientContext(
        websocket=FakeWebSocket(fail_close=True),
        sender=None,
        state={},
        subscriptions=Subscriptions(),
        subscribed_topics=set(),
        connection=NotifyConn(),  # type: ignore[arg-type]
    )
    server._clients.append(bad_client)

    class FakeUnderlyingServer:
        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

        async def wait_closed(self) -> None:
            return None

    fake_underlying = FakeUnderlyingServer()
    server._server = fake_underlying

    await server.stop_async()
    assert fake_underlying.closed == 1


@pytest.mark.asyncio
async def test_running_server_handle_client_connection_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_start(self: RunningServer) -> None:
        self._started.set()

    monkeypatch.setattr(RunningServer, "_start", fake_start)

    disconnected: list[bool] = []

    async def on_disconnect(_ctx: BodyContext) -> None:
        disconnected.append(True)

    server = RunningServer(
        Channel(),
        "127.0.0.1",
        0,
        options=ListenOptions(on_disconnect=on_disconnect),
    )
    await server.wait_started()

    ws = FakeWebSocket(iter_error=server_module.ConnectionClosed(None, None, None))
    await server._handle_client(ws, "/")

    assert disconnected == [True]
    assert not server._clients
    await server.stop_async()


@pytest.mark.asyncio
async def test_running_server_stop_with_and_without_running_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_start(self: RunningServer) -> None:
        self._started.set()

    monkeypatch.setattr(RunningServer, "_start", fake_start)
    server = RunningServer(Channel(), "127.0.0.1", 0, None)
    await server.wait_started()

    # running-loop branch
    server.stop()
    await asyncio.sleep(0)

    # no-running-loop branch
    called: list[int] = []

    monkeypatch.setattr(server_module.asyncio, "get_running_loop", lambda: (_ for _ in ()).throw(RuntimeError("none")))

    def fake_run(coro: Any) -> None:
        called.append(1)
        coro.close()

    monkeypatch.setattr(server_module.asyncio, "run", fake_run)
    server.stop()

    assert called == [1]


def test_listen_channel_address_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeRunningServer:
        def __init__(self, channel: Any, host: str, port: int, options: Any) -> None:
            captured.update({"channel": channel, "host": host, "port": port, "options": options})

    monkeypatch.setattr(server_module, "RunningServer", FakeRunningServer)

    channel = Channel()
    result = listen_channel(channel, "0.0.0.0:2049", options={"x": 1})

    assert isinstance(result, FakeRunningServer)
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 2049
    assert captured["options"] == {"x": 1}
