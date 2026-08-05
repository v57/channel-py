from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from channel_py.channel import Channel, Subscription, _RunningStream, make_sender


@dataclass
class DummySender:
    streams: dict[int, Any]
    requests: dict[int, Any]


class RecordingController:
    def __init__(self, sender: DummySender) -> None:
        self._sender = sender
        self._state: dict[str, Any] = {}
        self.responses: list[dict[str, Any]] = []
        self.unsubscribed: list[str] = []
        self.events: list[tuple[str, Any]] = []

    def response(self, response: dict[str, Any]) -> None:
        self.responses.append(response)

    def subscribe(self, topic: str) -> None:
        return None

    def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)

    def event(self, topic: str, body: Any) -> None:
        self.events.append((topic, body))

    @property
    def sender(self) -> DummySender:
        return self._sender

    @property
    def state(self) -> dict[str, Any]:
        return self._state


class FakeConnection:
    def __init__(self, send_none: bool = False) -> None:
        self.send_none = send_none
        self.notified: list[Any] = []

    def send(self, _body: Any) -> int | None:
        return None if self.send_none else 1

    def sent(self, _request_id: int) -> None:
        return None

    def cancel(self, _request_id: int) -> bool:
        return False

    def notify(self, body: Any) -> None:
        self.notified.append(body)

    def add_topic(self, _topic: str, _event: Any) -> Any:
        return lambda: True

    def stop(self) -> None:
        return None


@pytest.mark.asyncio
async def test_channel_receive_error_and_stream_branches() -> None:
    channel = Channel()
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    # api-not-found with id
    channel.receive_one({"id": 1, "path": "missing"}, controller)

    # async success path
    async def async_ok(_body: Any) -> str:
        return "ok"

    channel.post("async/ok", async_ok)
    channel.receive_one({"id": 2, "path": "async/ok"}, controller)

    # sync exception path
    channel.post("sync/fail", lambda _body: (_ for _ in ()).throw(RuntimeError("boom")))
    channel.receive_one({"id": 3, "path": "sync/fail"}, controller)

    # stream errors: not found/id missing and with id response
    channel.receive_one({"stream": "none"}, controller)
    channel.stream("s", lambda _body: [])
    channel.receive_one({"stream": "s"}, controller)
    channel.receive_one({"id": 4, "stream": "none"}, controller)

    # cancel branch: invalid cancel id
    channel.receive_one({"cancel": None}, controller)

    # sub missing
    channel.receive_one({"id": 5, "sub": "none"}, controller)

    # async sub rejection and sync sub throw
    sub_async = Subscription()

    async def reject(_request: Any) -> Any:
        raise RuntimeError("reject")

    sub_async._body = reject
    sub_sync = Subscription()
    sub_sync._topic = lambda _request: (_ for _ in ()).throw(RuntimeError("topic"))
    channel.events({"async": sub_async, "sync": sub_sync})
    channel.receive_one({"id": 6, "sub": "async", "body": "x"}, controller)
    channel.receive_one({"id": 7, "sub": "sync", "body": "x"}, controller)

    await asyncio.sleep(0.01)

    assert {"id": 1, "error": "api not found"} in controller.responses
    assert {"id": 2, "body": "ok"} in controller.responses
    assert {"id": 3, "error": "boom"} in controller.responses
    assert {"id": 4, "error": "api not found"} in controller.responses
    assert {"id": 5, "error": "subscription not found"} in controller.responses
    assert {"id": 6, "error": "reject"} in controller.responses
    assert {"id": 7, "error": "topic"} in controller.responses


@pytest.mark.asyncio
async def test_stream_request_async_result_and_iteration_errors() -> None:
    channel = Channel()
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    async def good_stream(_body: Any):
        async def iterator():
            yield 1

        return iterator()

    async def bad_iter_stream(_body: Any):
        async def iterator():
            yield 1
            raise RuntimeError("iter")

        return iterator()

    async def bad_outer(_body: Any):
        raise RuntimeError("outer")

    channel.stream("good", good_stream)
    channel.stream("bad/iter", bad_iter_stream)
    channel.stream("bad/outer", bad_outer)

    channel.receive_one({"id": 10, "stream": "good"}, controller)
    channel.receive_one({"id": 11, "stream": "bad/iter"}, controller)
    channel.receive_one({"id": 12, "stream": "bad/outer"}, controller)

    await asyncio.sleep(0.02)

    assert {"id": 10, "body": 1} in controller.responses
    assert {"id": 10, "done": True} in controller.responses
    assert {"id": 11, "body": 1} in controller.responses
    assert {"id": 12, "error": "outer"} in controller.responses


@pytest.mark.asyncio
async def test_running_stream_aclose_exception_and_disconnect_stream_cleanup() -> None:
    class BadIterator:
        async def aclose(self) -> None:
            raise ValueError("bad")

    await _RunningStream(BadIterator(), None).aclose()

    channel = Channel()
    sender = DummySender(streams={}, requests={})

    class NoClose:
        pass

    class RaisingClose:
        def aclose(self) -> Any:
            raise RuntimeError("bad")

    sender.streams[1] = NoClose()
    sender.streams[2] = RaisingClose()
    channel.disconnect({}, sender)


@pytest.mark.asyncio
async def test_values_next_response_queue_and_cancel_notify_without_send_id() -> None:
    channel = Channel().stream("never", lambda _body: [])
    sender = make_sender(channel, FakeConnection(send_none=True))
    values = sender.values("never")

    # queued branch in _response
    values._response({"body": "queued"})
    assert await values.next() == {"value": "queued", "done": False}

    # force rid and cancel with no transport id
    values._rid = 42
    await values.return_()

    assert sender._connection.notified == [{"cancel": 42}]  # type: ignore[attr-defined]


def test_receive_list_and_merge_events_branch() -> None:
    channel = Channel().post("a", lambda _body: "x")
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    channel.receive([{"id": 1, "path": "a"}, {"unsub": "t"}], controller)
    assert {"id": 1, "body": "x"} in controller.responses
    assert controller.unsubscribed == ["t"]

    source = Channel().events({"evt": Subscription()})
    target = Channel().merge(source, "pref")
    assert target.events_api is not None
    assert "pref/evt" in target.events_api


@pytest.mark.asyncio
async def test_subscription_send_async_error_is_swallowed() -> None:
    subscription = Subscription()

    async def failing_body(_request: Any) -> Any:
        raise RuntimeError("ignore")

    subscription._body = failing_body
    subscription.send("topic")

    # delayed_publish catches and swallows errors
    await asyncio.sleep(0)


def test_cancel_branch_catches_stream_close_errors() -> None:
    channel = Channel()

    class BrokenClose:
        def aclose(self) -> Any:
            raise RuntimeError("broken")

    sender = DummySender(streams={1: BrokenClose()}, requests={})
    controller = RecordingController(sender)

    # Error in close path is swallowed by cancel branch exception handler.
    channel.receive_one({"cancel": 1}, controller)


def test_cancel_branch_cancellation_requires_id_line() -> None:
    channel = Channel()
    sender = DummySender(streams={}, requests={})
    controller = RecordingController(sender)

    # Exercise the "request_id is None" guard by returning different values
    # across successive get("cancel") calls.
    class FlakyCancel(dict):
        def __init__(self) -> None:
            super().__init__(cancel=1)
            self.calls = 0

        def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
            if key == "cancel":
                self.calls += 1
                return 1 if self.calls == 1 else None
            return super().get(key, default)

    channel.receive_one(FlakyCancel(), controller)
