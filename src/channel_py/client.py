from __future__ import annotations

import asyncio
import random
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.legacy.client import connect as ws_connect

from .channel import Channel, Subscription, make_sender
from .jsonx import dumps, loads
from .types import DEFAULT_MAX_PAYLOAD, SubscriptionEvent


@dataclass(slots=True)
class ConnectOptions:
    headers: Callable[[], dict[str, str] | Awaitable[dict[str, str]]] | None = None
    on_connect: Callable[[Any], Any | Awaitable[Any]] | None = None
    # Largest WebSocket message accepted (bytes). Matches the TS/Bun default (16 MiB).
    max_payload: int = DEFAULT_MAX_PAYLOAD


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


def reconnect_delay(attempt: int, jitter: float) -> float:
    """Seconds before reconnect attempt `attempt` (0 = first retry): 100 ms doubling up to 5 s, scaled to
    50-100% by `jitter` (0..1) so clients don't retry in lockstep. Same numbers as the TypeScript client."""
    return min(5.0, 0.1 * 2 ** min(attempt, 16)) * (0.5 + jitter / 2)


class WebSocketClient:
    def __init__(
        self,
        address: str,
        headers: Callable[[], dict[str, str] | Awaitable[dict[str, str]]] | None = None,
        max_payload: int = DEFAULT_MAX_PAYLOAD,
    ) -> None:
        self.id = 0
        self.address = address
        self.headers = headers
        self.max_payload = max_payload
        # Set by `_run` for the connection attempt that just ended: whether it got past the
        # handshake, and why it failed (None for a clean close or an unreachable address).
        self.was_connected = False
        self.close_error: str | None = None

        self.ws: Any = None
        self.onopen: Callable[[], Any | Awaitable[Any]] | None = None
        self.onclose: Callable[[], None] | None = None
        self.onmessage: Callable[[Any], None] | None = None

        self.pending: dict[int, Any] = {}
        self.is_connected = False
        self.is_running = True
        self.attempts = 0

        self._outbox: deque[Any] = deque()
        self._outbox_event = asyncio.Event()
        self._drain_task = asyncio.create_task(self._drain_outbox())
        self._run_task = asyncio.create_task(self._run())

        self._subscribed: dict[str, dict[int, Callable[[Any], None]]] = defaultdict(dict)

    async def _run(self) -> None:
        while self.is_running:
            self.was_connected = False
            self.close_error = None
            try:
                headers = await self._resolve_headers()
                kwargs: dict[str, Any] = {}
                if headers:
                    kwargs["extra_headers"] = headers
                kwargs["compression"] = None
                kwargs["max_size"] = self.max_payload
                kwargs["max_queue"] = None
                kwargs["read_limit"] = 1_048_576
                kwargs["write_limit"] = 1_048_576
                kwargs["ping_interval"] = None
                # A peer that breaks the protocol (oversized frame, invalid UTF-8) is dropped after
                # 1 s instead of the default 10 s close handshake, so pending calls settle quickly.
                kwargs["close_timeout"] = 1

                ws = await ws_connect(self.address, **kwargs)
                self.ws = ws
                self.is_connected = True
                self.was_connected = True
                self.attempts = 0

                if self.onopen is not None:
                    value = self.onopen()
                    if asyncio.iscoroutine(value):
                        await value

                self._replay_pending()

                async for message in ws:
                    if not isinstance(message, str):
                        continue
                    try:
                        value = loads(message)
                    except Exception as error:
                        # Same as TS: a peer that sends invalid JSON gets disconnected.
                        self.close_error = f"invalid message from server: {error}"
                        # Settle pending calls now; don't wait for the peer's close handshake.
                        asyncio.create_task(ws.close())
                        break
                    if self.onmessage is not None:
                        self.onmessage(value)
            except ConnectionClosed as error:
                # e.g. 1009 (message over max_payload) or 1007 (invalid UTF-8); a plain drop is
                # reported as "disconnected" by the channel.
                frame = error.sent or error.rcvd
                if self.close_error is None and frame is not None and frame.code not in (1000, 1001):
                    self.close_error = f"connection closed: {frame.code} {frame.reason}".strip()
            except InvalidHandshake as error:
                # The address answered but is not a WebSocket server (e.g. HTTP 500).
                self.close_error = f"websocket handshake failed: {error}"
            except asyncio.CancelledError:
                raise
            except OSError:
                pass  # not reachable yet: keep pending requests and retry
            except Exception as error:
                self.close_error = str(error) or type(error).__name__
            finally:
                self.is_connected = False
                self.ws = None
                if self.onclose is not None:
                    self.onclose()

            if self.is_running:
                await asyncio.sleep(reconnect_delay(self.attempts, random.random()))
                self.attempts += 1

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
    max_payload = _option_value(options, "max_payload", "maxPayload") or DEFAULT_MAX_PAYLOAD

    ws = WebSocketClient(target, headers)
    ws.max_payload = max_payload  # read by the first connect attempt (scheduled, not yet run)
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
        close_error = getattr(ws, "close_error", None)
        if not ws.is_running:
            cleanup_publishers()
        elif not getattr(ws, "was_connected", True) and close_error is None:
            # Address not reachable yet: keep pending requests, they are re-sent on connect.
            return
        if close_error is not None:
            channel.fail_requests(sender, close_error)
        # Settles whatever is left with "disconnected" so no caller waits forever.
        channel.disconnect(state, sender)
        pending = getattr(ws, "pending", None)
        if pending:
            # Don't re-send requests that were just failed.
            for key, body in list(pending.items()):
                if isinstance(body, dict) and "id" in body and body["id"] not in channel.requests:
                    pending.pop(key, None)

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
