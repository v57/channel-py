from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from channel_py.channel import Channel, ObjectMap, Subscription, make_sender


async def tick() -> None:
    await asyncio.sleep(0)


@dataclass
class DummySender:
    streams: dict[int, Any]
    requests: dict[int, Any]


class RecordingController:
    def __init__(self, sender: DummySender) -> None:
        self._sender = sender
        self._state: dict[str, Any] = {}
        self.responses: list[dict[str, Any]] = []
        self.subscriptions: list[str] = []
        self.unsubscriptions: list[str] = []
        self.events: list[tuple[str, Any]] = []

    def response(self, response: dict[str, Any]) -> None:
        self.responses.append(response)

    def subscribe(self, topic: str) -> None:
        self.subscriptions.append(topic)

    def unsubscribe(self, topic: str) -> None:
        self.unsubscriptions.append(topic)

    def event(self, topic: str, body: Any) -> None:
        self.events.append((topic, body))

    @property
    def sender(self) -> DummySender:
        return self._sender

    @property
    def state(self) -> dict[str, Any]:
        return self._state


class FakeConnection:
    def __init__(
        self,
        cancel_result: bool = False,
        send_returns_none: bool = False,
        start_id: int = 200,
    ) -> None:
        self.cancel_result = cancel_result
        self.send_returns_none = send_returns_none
        self.next_id = start_id
        self.sent_ids: list[int] = []
        self.cancel_ids: list[int] = []
        self.notify_bodies: list[Any] = []
        self.send_bodies: list[Any] = []

    def send(self, body: Any) -> int | None:
        self.send_bodies.append(body)
        if self.send_returns_none:
            return None
        rid = self.next_id
        self.next_id += 1
        return rid

    def sent(self, request_id: int) -> None:
        self.sent_ids.append(request_id)

    def cancel(self, request_id: int) -> bool:
        self.cancel_ids.append(request_id)
        return self.cancel_result

    def notify(self, body: Any) -> None:
        self.notify_bodies.append(body)

    def add_topic(self, topic: str, event: Any) -> Any:
        return lambda: True

    def stop(self) -> None:
        return None


@pytest.mark.asyncio
async def test_post_other_and_stream_other_route_dynamic_paths() -> None:
    channel = Channel()
    channel.post_other(lambda path: path.startswith("dynamic/"), lambda body: f"{body.path}:{body.body}")

    async def dynamic_stream(body: Any):
        yield body.body

    channel.stream_other(lambda path: path.endswith("/stream"), dynamic_stream)

    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    channel.receive_one({"id": 1, "path": "dynamic/echo", "body": "ok"}, controller)
    channel.receive_one({"id": 2, "stream": "dynamic/stream", "body": 7}, controller)
    await asyncio.sleep(0.01)

    assert {"id": 1, "body": "dynamic/echo:ok"} in controller.responses
    assert {"id": 2, "body": 7} in controller.responses
    assert {"id": 2, "done": True} in controller.responses


@pytest.mark.asyncio
async def test_disconnect_runs_cleanup_and_clears_pending_maps() -> None:
    disconnected = 0

    def on_disconnect(_state: dict[str, Any], _sender: Any) -> None:
        nonlocal disconnected
        disconnected += 1
        raise RuntimeError("ignored")

    channel = Channel().on_disconnect(on_disconnect)
    sender = DummySender(streams={}, requests={})

    closed_streams = 0

    class _Stream:
        async def aclose(self) -> None:
            nonlocal closed_streams
            closed_streams += 1

    sender.streams[1] = _Stream()

    cancel_actions = 0
    cancellable = type("C", (), {})()
    cancellable.is_cancelled = False
    cancellable.cancel_actions = [
        lambda: _inc_cancel(),
        lambda: (_inc_cancel(), (_ for _ in ()).throw(RuntimeError("ignored"))),
    ]

    def _inc_cancel() -> None:
        nonlocal cancel_actions
        cancel_actions += 1

    sender.requests[10] = cancellable

    channel.make_request("pending", None, None, lambda _response: None, sender)
    channel.disconnect({}, sender)
    await tick()

    assert disconnected == 1
    assert closed_streams == 1
    assert cancel_actions == 2
    assert sender.streams == {}
    assert sender.requests == {}
    assert channel.requests == {}


def test_cancel_removes_request_by_id() -> None:
    channel = Channel()
    request = channel.make_request("hello", None, None, lambda _response: None)
    assert len(channel.requests) == 1
    channel.cancel(request["id"])
    assert len(channel.requests) == 0


@pytest.mark.asyncio
async def test_receive_one_handles_async_post_rejection() -> None:
    async def fail(_body: Any) -> None:
        raise RuntimeError("boom")

    channel = Channel().post("fails", fail)
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    channel.receive_one({"id": 1, "path": "fails"}, controller)
    await tick()

    assert {"id": 1, "error": "boom"} in controller.responses


@pytest.mark.asyncio
async def test_receive_one_cancel_runs_on_cancel_actions() -> None:
    on_cancel_calls: list[int] = []

    def wait(body: Any) -> Any:
        assert body.task is not None
        body.task.on_cancel(lambda: on_cancel_calls.append(1))
        body.task.on_cancel(lambda: on_cancel_calls.append(2))
        return asyncio.Future()

    channel = Channel().post("wait", wait)
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    channel.receive_one({"id": 5, "path": "wait"}, controller)
    channel.receive_one({"cancel": 5}, controller)
    await tick()

    assert on_cancel_calls == [1, 2]
    assert {"id": 5, "error": "cancelled"} in controller.responses
    assert sender.requests == {}


@pytest.mark.asyncio
async def test_receive_one_subscribe_after_async_body_resolves() -> None:
    news = Subscription()
    news.topic(lambda _request: "ready")

    async def make_body(request: dict[str, str]) -> str:
        return request["value"].upper()

    news._body = make_body

    channel = Channel().events({"news": news})
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    channel.receive_one({"id": 9, "sub": "news", "body": {"value": "ok"}}, controller)
    await tick()

    assert controller.subscriptions == ["news/ready"]
    assert {"id": 9, "topic": "news/ready", "body": "OK"} in controller.responses


def test_subscription_topic_and_body_replace_topic_formatter() -> None:
    subscription = Subscription()
    events: list[dict[str, Any]] = []

    class Publisher:
        def publish(self, event: Any) -> None:
            events.append({"topic": event.topic, "body": event.body})

    subscription.publishers.append(Publisher())
    subscription.topic(lambda body: f"topic-{body}")
    subscription.body(lambda body: f"body-{body}")
    subscription.send("x", "payload")

    assert events == [{"topic": "body-x", "body": "payload"}]


def test_object_map_unchecked_set_behavior() -> None:
    object_map = ObjectMap()
    object_map.unchecked_set("a", 1)
    object_map.unchecked_set("a", 2)

    assert object_map["a"] == 2
    assert object_map.size == 1


@pytest.mark.asyncio
async def test_make_sender_request_resolves_and_marks_sent_id() -> None:
    channel = Channel()
    connection = FakeConnection()
    sender = make_sender(channel, connection)

    request = sender.request("hello")
    request_id = next(iter(channel.requests.keys()))

    controller = RecordingController(DummySender(streams=sender.streams, requests=sender.requests))
    channel.receive_one({"id": request_id, "body": "world"}, controller)

    assert await request.response == "world"
    assert connection.sent_ids == [200]


@pytest.mark.asyncio
async def test_make_sender_request_rejects_and_marks_sent_id() -> None:
    channel = Channel()
    connection = FakeConnection()
    sender = make_sender(channel, connection)

    request = sender.request("hello")
    request_id = next(iter(channel.requests.keys()))

    controller = RecordingController(DummySender(streams=sender.streams, requests=sender.requests))
    channel.receive_one({"id": request_id, "error": "failed"}, controller)

    with pytest.raises(Exception, match="failed"):
        await request.response

    assert connection.sent_ids == [200]


def test_make_sender_request_cancel_notifies_when_transport_cancel_fails() -> None:
    channel = Channel()
    connection = FakeConnection(cancel_result=False)
    sender = make_sender(channel, connection)

    request = sender.request("slow")
    request_id = next(iter(channel.requests.keys()))
    request.cancel()

    assert connection.cancel_ids == [200]
    assert connection.notify_bodies == [{"cancel": request_id}]
    assert channel.requests == {}


def test_make_sender_request_cancel_notifies_when_send_id_missing() -> None:
    channel = Channel()
    connection = FakeConnection(send_returns_none=True)
    sender = make_sender(channel, connection)

    request = sender.request("slow")
    request_id = next(iter(channel.requests.keys()))
    request.cancel()

    assert connection.cancel_ids == []
    assert connection.notify_bodies == [{"cancel": request_id}]
    assert channel.requests == {}


@pytest.mark.asyncio
async def test_process_stop_clears_pending_requests() -> None:
    def wait(body: Any) -> Any:
        if body.task is not None:
            body.task.on_cancel(lambda: None)
        return asyncio.Future()

    channel = Channel().post("wait", wait)
    sender = channel.process()

    for _ in range(10):
        sender.request("wait")

    await tick()
    assert len(sender.requests) > 0
    assert len(channel.requests) > 0

    sender.stop()
    await tick()

    assert sender.requests == {}
    assert channel.requests == {}


@pytest.mark.asyncio
async def test_values_return_clears_pending_stream_request() -> None:
    channel = Channel()
    connection = FakeConnection(cancel_result=False, start_id=100)
    sender = make_sender(channel, connection)

    values = sender.values("stream/stuck")
    pending_next = asyncio.create_task(values.next())
    await tick()

    assert len(channel.requests) == 1

    await values.return_()
    await tick()

    pending_next.cancel()

    assert connection.cancel_ids == [100]
    assert any(isinstance(body, dict) and body.get("cancel") is not None for body in connection.notify_bodies)
    assert channel.requests == {}


@pytest.mark.asyncio
async def test_listen_stop_cleans_up_subscription_publishers() -> None:
    updates = Subscription()
    channel = Channel().events({"updates": updates})

    assert len(updates.publishers) == 0

    first = channel.listen(0)
    await first.wait_started()
    assert len(updates.publishers) == 1

    await first.stop_async()
    assert len(updates.publishers) == 0

    second = channel.listen(0)
    await second.wait_started()
    assert len(updates.publishers) == 1

    await second.stop_async()
    assert len(updates.publishers) == 0
