from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .channel import Channel, make_sender


@dataclass(slots=True)
class _Controller:
    response: Any
    subscribe: Any
    unsubscribe: Any
    event: Any
    sender: Any
    state: Any


class _ProcessInterface:
    def __init__(self, channel: Channel[Any]) -> None:
        self._channel = channel
        self.controller: _Controller | None = None

    def send(self, body: Any) -> int:
        if self.controller is not None:
            self._channel.receive(body, self.controller)
        return 0

    def sent(self, request_id: int) -> None:
        return None

    def cancel(self, request_id: int) -> bool:
        return False

    def notify(self, body: Any) -> None:
        if self.controller is not None:
            self._channel.receive(body, self.controller)

    def add_topic(self, topic: str, event: Any) -> Any:
        return lambda: False

    def stop(self) -> None:
        if self.controller is None:
            return
        state = self.controller.state
        sender = self.controller.sender
        self.controller = None
        self._channel.disconnect(state, sender)


def process_channel(channel: Channel[Any]) -> Any:
    connection = _ProcessInterface(channel)
    sender = make_sender(channel, connection)
    state: dict[str, Any] = {}

    controller = _Controller(
        response=lambda body: channel.receive(body, controller),
        subscribe=lambda _topic: None,
        unsubscribe=lambda _topic: None,
        event=lambda _topic, _body: None,
        sender=sender,
        state=state,
    )
    connection.controller = controller
    return sender
