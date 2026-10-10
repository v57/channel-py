from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar

StateT = TypeVar("StateT")

# Largest WebSocket message accepted by default: 16 MiB, the TS/Bun default.
DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024


class ChannelError(Exception):
    pass


@dataclass(slots=True)
class PendingRequest:
    request: dict[str, Any]
    response: Callable[[dict[str, Any]], None]
    sender: "Sender | None" = None


@dataclass(slots=True)
class SubscriptionEvent:
    topic: str
    body: Any


class EventPublisher(Protocol):
    def publish(self, event: SubscriptionEvent) -> None: ...


class Controller(Protocol, Generic[StateT]):
    def response(self, response: dict[str, Any]) -> None: ...

    def subscribe(self, topic: str) -> None: ...

    def unsubscribe(self, topic: str) -> None: ...

    def event(self, topic: str, body: Any) -> None: ...

    @property
    def sender(self) -> "Sender": ...

    @property
    def state(self) -> StateT: ...


class ConnectionInterface(Protocol):
    def send(self, body: Any) -> int | None: ...

    def sent(self, request_id: int) -> None: ...

    def cancel(self, request_id: int) -> bool: ...

    def notify(self, body: Any) -> None: ...

    def add_topic(self, topic: str, event: Callable[[Any], None]) -> Callable[[], bool]: ...

    def stop(self) -> None: ...


class Cancellable(Protocol):
    def cancel(self) -> None: ...


class SendingRequest(Protocol):
    @property
    def response(self) -> Awaitable[Any]: ...

    def cancel(self) -> None: ...


class Sender(Protocol):
    streams: dict[int, Any]
    requests: dict[int, Any]

    async def send(self, path: str, body: Any = None, context: Any = None) -> Any: ...

    def request(self, path: str, body: Any = None, context: Any = None) -> SendingRequest: ...

    def values(self, path: str, body: Any = None, context: Any = None) -> Any: ...

    async def subscribe(
        self,
        path: str,
        body: Any,
        event: Callable[[Any], None],
        context: Any = None,
    ) -> Cancellable: ...

    def stop(self) -> None: ...
