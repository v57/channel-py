from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServerProtocol, serve as ws_serve

from .channel import Channel, Subscription, make_sender
from .jsonx import dumps, loads
from .types import DEFAULT_MAX_PAYLOAD, SubscriptionEvent


@dataclass(slots=True)
class BodyContext:
    state: Any
    sender: Any
    subscriptions: "Subscriptions"


@dataclass(slots=True)
class ListenOptions:
    state: Callable[[dict[str, str | None]], Any | Awaitable[Any]] | None = None
    on_connect: Callable[[BodyContext], Any | Awaitable[Any]] | None = None
    on_disconnect: Callable[[BodyContext], Any | Awaitable[Any]] | None = None
    # Largest WebSocket message accepted (bytes). Matches the TS/Bun default (16 MiB).
    max_payload: int = DEFAULT_MAX_PAYLOAD


class Subscriptions:
    def __init__(self) -> None:
        self.id = 0
        self.subscribed: dict[str, dict[int, Callable[[Any], None]]] = defaultdict(dict)

    def add_topic(self, topic: str, event: Callable[[Any], None]) -> Callable[[], bool]:
        self.id += 1
        event_id = self.id
        handlers = self.subscribed[topic]
        handlers[event_id] = event

        def remove() -> bool:
            handlers.pop(event_id, None)
            if handlers:
                return False
            self.subscribed.pop(topic, None)
            return True

        return remove

    def received_event(self, topic: str, event: Any) -> None:
        for callback in list(self.subscribed.get(topic, {}).values()):
            callback(event)


class _Connection:
    def __init__(self, websocket: WebSocketServerProtocol, subscriptions: Subscriptions) -> None:
        self.websocket = websocket
        self.subscriptions = subscriptions
        self._is_running = True
        self._outbox: deque[Any] = deque()
        self._outbox_event = asyncio.Event()
        self._drain_task = asyncio.create_task(self._drain_outbox())

    def _enqueue(self, body: Any) -> None:
        if not self._is_running:
            return
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

    async def _drain_outbox(self) -> None:
        try:
            while self._is_running:
                await self._outbox_event.wait()
                self._outbox_event.clear()
                while self._is_running:
                    payload = self._next_outbound_payload()
                    if payload is None:
                        break
                    try:
                        await self.websocket.send(dumps(payload))
                    except Exception:
                        self._restore_outbound_payload(payload)
                        break
        except asyncio.CancelledError:
            pass

    def send(self, body: Any) -> int:
        self._enqueue(body)
        return 0

    def sent(self, request_id: int) -> None:
        return None

    def cancel(self, request_id: int) -> bool:
        return False

    def notify(self, body: Any) -> None:
        self._enqueue(body)

    def add_topic(self, topic: str, event: Callable[[Any], None]) -> Callable[[], bool]:
        return self.subscriptions.add_topic(topic, event)

    def close(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        self._outbox.clear()
        self._outbox_event.set()
        if not self._drain_task.done():
            self._drain_task.cancel()

    def stop(self) -> None:
        self.close()
        asyncio.create_task(self.websocket.close())


@dataclass(slots=True)
class _ClientContext:
    websocket: WebSocketServerProtocol
    sender: Any
    state: Any
    subscriptions: Subscriptions
    subscribed_topics: set[str]
    connection: _Connection

    def send_event(self, event: dict[str, Any]) -> None:
        self.connection.notify(event)


class RunningServer:
    def __init__(
        self,
        channel: Channel[Any],
        host: str,
        port: int,
        options: ListenOptions | dict[str, Any] | None,
    ) -> None:
        self._channel = channel
        self._host = host
        self._port = port
        self._options = options

        self._server: Any = None
        self._clients: list[_ClientContext] = []
        self._publishers: list[tuple[Subscription, Any]] = []
        self._publisher_cleanup_done = False
        self._started = asyncio.Event()
        self._start_task = asyncio.create_task(self._start())

    async def _start(self) -> None:
        self._attach_publishers()
        max_payload = self._option_value("max_payload", "maxPayload") or DEFAULT_MAX_PAYLOAD
        self._server = await ws_serve(
            self._handle_client,
            self._host,
            self._port,
            compression=None,
            max_size=max_payload,
            max_queue=None,
            read_limit=1_048_576,
            write_limit=1_048_576,
            ping_interval=None,
        )
        self._started.set()

    async def wait_started(self) -> None:
        await self._started.wait()

    @property
    def port(self) -> int:
        if self._server is None:
            return self._port
        sockets = getattr(self._server, "sockets", None)
        if not sockets:
            return self._port
        return int(sockets[0].getsockname()[1])

    def _option_value(self, snake: str, camel: str) -> Any:
        options = self._options
        if options is None:
            return None
        if isinstance(options, dict):
            return options.get(snake, options.get(camel))
        return getattr(options, snake, getattr(options, camel, None))

    async def _resolve_state(self, websocket: WebSocketServerProtocol, path: str) -> Any:
        state_factory = self._option_value("state", "state")
        if state_factory is None:
            return {}

        headers: dict[str, str | None] = {}
        parsed = urlparse(path)
        for key, values in parse_qs(parsed.query).items():
            headers[key] = values[-1] if values else None
        # Real headers win over query parameters, and header names are lowercased (TS/Bun parity).
        for key, value in websocket.request_headers.items():
            headers[key.lower()] = value

        value = state_factory(headers)
        if asyncio.iscoroutine(value):
            return await value
        return value

    async def _call_optional(self, callback: Callable[[BodyContext], Any] | None, body: BodyContext) -> None:
        if callback is None:
            return
        result = callback(body)
        if asyncio.iscoroutine(result):
            await result

    async def _handle_client(self, websocket: WebSocketServerProtocol, path: str) -> None:
        state = await self._resolve_state(websocket, path)
        subscriptions = Subscriptions()
        connection = _Connection(websocket, subscriptions)
        sender = make_sender(self._channel, connection)
        client = _ClientContext(
            websocket=websocket,
            sender=sender,
            state=state,
            subscriptions=subscriptions,
            subscribed_topics=set(),
            connection=connection,
        )

        body_context = BodyContext(state=state, sender=sender, subscriptions=subscriptions)

        class _Controller:
            def response(self, response: dict[str, Any]) -> None:
                connection.notify(response)

            def subscribe(self, topic: str) -> None:
                client.subscribed_topics.add(topic)

            def unsubscribe(self, topic: str) -> None:
                client.subscribed_topics.discard(topic)

            def event(self, topic: str, event: Any) -> None:
                subscriptions.received_event(topic, event)

            @property
            def sender(self) -> Any:
                return sender

            @property
            def state(self) -> Any:
                return state

        controller = _Controller()
        on_connect = self._option_value("on_connect", "onConnect")
        on_disconnect = self._option_value("on_disconnect", "onDisconnect")

        self._clients.append(client)
        await self._call_optional(on_connect, body_context)

        try:
            async for message in websocket:
                if not isinstance(message, str):
                    continue
                try:
                    req = loads(message)
                    self._channel.receive(req, controller)
                except Exception:
                    await websocket.close()
                    break
        except ConnectionClosed:
            pass
        finally:
            connection.close()
            if client in self._clients:
                self._clients.remove(client)
            self._channel.disconnect(state, sender)
            await self._call_optional(on_disconnect, body_context)

    def _attach_publishers(self) -> None:
        if not self._channel.events_api:
            return

        for subscription in self._channel.events_api.values():

            class _Publisher:
                def __init__(self, server: RunningServer) -> None:
                    self._server = server

                def publish(self, event: SubscriptionEvent) -> None:
                    payload = {"topic": event.topic, "body": event.body}
                    for client in list(self._server._clients):
                        if event.topic in client.subscribed_topics:
                            client.send_event(payload)

            publisher = _Publisher(self)
            subscription.publishers.append(publisher)
            self._publishers.append((subscription, publisher))

    def _cleanup_publishers(self) -> None:
        if self._publisher_cleanup_done:
            return
        self._publisher_cleanup_done = True
        for subscription, publisher in self._publishers:
            if publisher in subscription.publishers:
                subscription.publishers.remove(publisher)

    async def stop_async(self) -> None:
        await self._started.wait()
        self._cleanup_publishers()

        for client in list(self._clients):
            try:
                await client.websocket.close()
            except Exception:
                pass

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    def stop(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.stop_async())
        except RuntimeError:
            asyncio.run(self.stop_async())


# Keep TS-style method name.
Server = RunningServer


def listen_channel(
    channel: Channel[Any],
    address: str | int,
    options: ListenOptions | dict[str, Any] | None = None,
) -> RunningServer:
    host = "127.0.0.1"
    if isinstance(address, str):
        h, p = address.split(":", maxsplit=1)
        host = h
        port = int(p)
    else:
        port = int(address)

    return RunningServer(channel, host, port, options)
