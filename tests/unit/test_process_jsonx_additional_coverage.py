from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from channel_py.channel import Channel
from channel_py.jsonx import dumpb, dumps, loads
from channel_py.process import _ProcessInterface


def test_jsonx_roundtrip_including_loads() -> None:
    payload = {"a": 1, "b": [1, 2, 3]}
    as_text = dumps(payload)
    as_bytes = dumpb(payload)

    assert loads(as_text) == payload
    assert loads(as_bytes) == payload


def test_process_interface_methods() -> None:
    received: list[Any] = []
    disconnected: list[Any] = []

    class FakeChannel(Channel[Any]):
        def receive(self, some: Any, controller: Any) -> None:  # type: ignore[override]
            received.append((some, controller))

        def disconnect(self, state: Any, sender: Any) -> None:  # type: ignore[override]
            disconnected.append((state, sender))

    channel = FakeChannel()
    interface = _ProcessInterface(channel)

    assert interface.send({"x": 1}) == 0
    assert received == []

    controller = SimpleNamespace(state={"s": 1}, sender={"id": 1})
    interface.controller = controller

    assert interface.send({"x": 2}) == 0
    interface.notify({"x": 3})

    assert len(received) == 2
    assert interface.sent(1) is None
    assert interface.cancel(1) is False
    assert interface.add_topic("topic", lambda _v: None)() is False

    interface.stop()
    assert interface.controller is None
    assert disconnected == [({"s": 1}, {"id": 1})]

    # stop no-op when already stopped
    interface.stop()
