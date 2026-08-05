from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect as ws_connect

from .channel import Channel, Subscription, make_sender
from .jsonx import dumps, loads
from .types import SubscriptionEvent


@dataclass(slots=True)
class ConnectOptions:
    headers: Callable[[], dict[str, str] | Awaitable[dict[str, str]]] | None = None
    on_connect: Callable[[Any], Any | Awaitable[Any]] | None = None


class ClientSender:
    def __init__(self, sender: Any, ws: "WebSocketClient") -> None:
        self._sender = sender
        self.ws = ws

    @property
    def streams(self) -> dict[int, Any]:
        return self._sender.streams

    @property
    def requests(self) -> dict[int, Any]:
        return self._sender.requests

    async def send(self, path: str, body: Any = None, context: Any = None) -> Any:
        return await self._sender.send(path, body, context)

    def request(self, path: str, body: Any = None, context: Any = None) -> Any:
        return self._sender.request(path, body, context)

    def values(self, path: str, body: Any = None, context: Any = None) -> Any:
        return self._sender.values(path, body, context)

    async def subscribe(
        self,
        path: str,
        body: Any,
        event: Callable[[Any], None],
        context: Any = None,
    ) -> Any:
        return await self._sender.subscribe(path, body, event, context)

    def stop(self) -> None:
        self._sender.stop()


class WebSocketClient:
    def __init__(
        self,
        address: str,
        headers: Callable[[], dict[str, str] | Awaitable[dict[str, str]]] | None = None,
    ) -> None:
        self.id = 0
        self.address = address
        self.headers = headers

        self.ws: Any = None
        self.onopen: Callable[[], Any | Awaitable[Any]] | None = None
        self.onclose: Callable[[], None] | None = None
        self.onmessage: Callable[[Any], None] | None = None

        self.pending: dict[int, Any] = {}
        self.is_connected = False
        self.is_running = True

        self._outbox: deque[Any] = deque()
        self._outbox_event = asyncio.Event()
        self._drain_task = asyncio.create_task(self._drain_outbox())
        self._run_task = asyncio.create_task(self._run())

        self._subscribed: dict[str, dict[int, Callable[[Any], None]]] = defaultdict(dict)

    async def _run(self) -> None:
        while self.is_running:
            try:
                headers = await self._resolve_headers()
                kwargs: dict[str, Any] = {}
                if headers:
                    kwargs["extra_headers"] = headers
                kwargs["compression"] = None
                kwargs["max_queue"] = None
                kwargs["read_limit"] = 1_048_576
                kwargs["write_limit"] = 1_048_576
                kwargs["ping_interval"] = None

                ws = await ws_connect(self.address, **kwargs)
                self.ws = ws
                self.is_connected = True

                if self.onopen is not None:
                    value = self.onopen()
                    if asyncio.iscoroutine(value):
                        await value

                self._replay_pending()

                async for message in ws:
                    if isinstance(message, str):
                        value = loads(message)
                        if self.onmessage is not None:
                            self.onmessage(value)
            except ConnectionClosed:
                pass
            except Exception as error:
                if "101" in str(error):
                    self.is_running = False
            finally:
                self.is_connected = False
                self.ws = None
                if self.onclose is not None:
                    self.onclose()

            if self.is_running:
                await asyncio.sleep(0.1)

    async def _resolve_headers(self) -> dict[str, str] | None:
        if self.headers is None:
            return None
        value = self.headers()
        if asyncio.iscoroutine(value):
            return await value
        return value

    def _enqueue_outbox(self, body: Any) -> None:
        self._outbox.append(body)
        self._outbox_event.set()

    def _next_outbound_payload(self) -> Any | None:
        if not self._outbox:
            return None
        first = self._outbox.popleft()
        if not self._outbox:
            return first
        batch = [first]
        while self._outbox:
            batch.append(self._outbox.popleft())
        return batch

    def _restore_outbound_payload(self, payload: Any) -> None:
        if isinstance(payload, list):
            self._outbox.extendleft(reversed(payload))
        else:
            self._outbox.appendleft(payload)

    def _replay_pending(self) -> None:
        if not self.pending:
            return
        self._outbox.clear()
        self._enqueue_outbox(list(self.pending.values()))

    async def _drain_outbox(self) -> None:
        try:
            while self.is_running:
                await self._outbox_event.wait()
                self._outbox_event.clear()
                while self.is_running:
                    ws = self.ws
                    if ws is None:
                        break
                    payload = self._next_outbound_payload()
                    if payload is None:
                        break
                    try:
                        await ws.send(dumps(payload))
                    except Exception:
                        self._restore_outbound_payload(payload)
                        break
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self.is_running = False
        self.pending.clear()
        self._outbox.clear()
        self._outbox_event.set()

        ws = self.ws
        if ws is not None:
            asyncio.create_task(ws.close())

        if not self._run_task.done():
            self._run_task.cancel()
        if not self._drain_task.done():
            self._drain_task.cancel()

    def send(self, body: Any) -> int:
        request_id = self.id
        self.id += 1
        self.pending[request_id] = body
        self._enqueue_outbox(body)
        return request_id

    def cancel(self, request_id: int) -> bool:
        if self.is_connected:
            return False
        if request_id not in self.pending:
            return False
        self.pending.pop(request_id, None)
        return True

    def notify(self, body: Any) -> None:
        self._enqueue_outbox(body)

    def sent(self, request_id: int) -> None:
        self.pending.pop(request_id, None)

    def throttle(self) -> None:
        return None

    def add_topic(self, topic: str, event: Callable[[Any], None]) -> Callable[[], bool]:
        event_id = self.id + 1
        self.id = event_id

        handlers = self._subscribed[topic]
        handlers[event_id] = event

        def cancel() -> bool:
            handlers.pop(event_id, None)
            if handlers:
                return False
            self._subscribed.pop(topic, None)
            return True

        return cancel

    def received_event(self, topic: str, event: Any) -> None:
        for callback in list(self._subscribed.get(topic, {}).values()):
            callback(event)


def _option_value(options: ConnectOptions | dict[str, Any] | None, snake: str, camel: str) -> Any:
    if options is None:
        return None
    if isinstance(options, dict):
        return options.get(snake, options.get(camel))
    return getattr(options, snake, getattr(options, camel, None))


def connect_channel(channel: Channel[Any], address: str | int, options: ConnectOptions | dict[str, Any] | None = None) -> ClientSender:
    target = address if isinstance(address, str) else f"ws://127.0.0.1:{address}"
    headers = _option_value(options, "headers", "headers")
    on_connect = _option_value(options, "on_connect", "onConnect")

    ws = WebSocketClient(target, headers)
    topics: set[str] = set()
    ws.topics = topics  # type: ignore[attr-defined]
    sender = make_sender(channel, ws)
    state: dict[str, Any] = {}

    if on_connect is not None:

        async def on_open() -> None:
            result = on_connect(sender)
            if asyncio.iscoroutine(result):
                await result

        ws.onopen = on_open

    class _Controller:
        def response(self, response: dict[str, Any]) -> None:
            ws.notify(response)

        def subscribe(self, topic: str) -> None:
            topics.add(topic)

        def unsubscribe(self, topic: str) -> None:
            topics.discard(topic)

        def event(self, topic: str, body: Any) -> None:
            ws.received_event(topic, body)

        @property
        def sender(self) -> Any:
            return sender

        @property
        def state(self) -> Any:
            return state

    controller = _Controller()

    publishers: list[tuple[Subscription, Any]] = []
    removed_publishers = False

    def cleanup_publishers() -> None:
        nonlocal removed_publishers
        if removed_publishers:
            return
        removed_publishers = True
        for subscription, publisher in publishers:
            if publisher in subscription.publishers:
                subscription.publishers.remove(publisher)

    def on_close() -> None:
        if not ws.is_running:
            cleanup_publishers()
        channel.disconnect(state, sender)

    ws.onclose = on_close
    ws.onmessage = lambda message: channel.receive(message, controller)

    if channel.events_api:
        for subscription in channel.events_api.values():

            class _Publisher:
                def publish(self, event: SubscriptionEvent) -> None:
                    if event.topic not in topics:
                        return
                    ws.notify({"topic": event.topic, "body": event.body})

            publisher = _Publisher()
            subscription.publishers.append(publisher)
            publishers.append((subscription, publisher))

    return ClientSender(sender, ws)
