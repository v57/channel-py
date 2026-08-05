from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from .types import (
    ChannelError,
    ConnectionInterface,
    Controller,
    PendingRequest,
    Sender,
    SendingRequest,
    SubscriptionEvent,
)

StateT = TypeVar("StateT")
FunctionHandler = Callable[["BodyContext[StateT]"], Any | Awaitable[Any]]
StreamHandler = Callable[["BodyContext[StateT]"], Any]


@dataclass(slots=True)
class BodyContext(Generic[StateT]):
    body: Any
    sender: Sender
    state: StateT
    path: str
    context: Any
    task: "CancellableRequest | None" = None


class CancellableRequest:
    def __init__(self, cancel_impl: Callable[[], None] | None = None) -> None:
        self.is_cancelled = False
        self.cancel_actions: list[Callable[[], None]] = []
        self._cancel_impl = cancel_impl

    def on_cancel(self, action: Callable[[], None]) -> None:
        self.cancel_actions.append(action)

    # TS alias parity
    def onCancel(self, action: Callable[[], None]) -> None:
        self.on_cancel(action)

    def cancel(self) -> None:
        self.is_cancelled = True
        for action in list(self.cancel_actions):
            try:
                action()
            except Exception:
                pass
        if self._cancel_impl is not None:
            self._cancel_impl()


class ObjectMap(dict[Any, Any]):
    @property
    def size(self) -> int:
        return len(self)

    def unchecked_set(self, key: Any, value: Any) -> None:
        self[key] = value

    def uncheckedSet(self, key: Any, value: Any) -> None:
        self.unchecked_set(key, value)

    def map(self, transform: Callable[[Any], Any]) -> list[Any]:
        return [transform(value) for value in self.values()]

    def for_each(self, callback: Callable[[Any, Any], None]) -> None:
        for key, value in self.items():
            callback(value, key)

    def forEach(self, callback: Callable[[Any, Any], None]) -> None:
        self.for_each(callback)


class Subscription:
    def __init__(self) -> None:
        self.publishers: list[Any] = []
        self.prefix = ""
        self._topic: Callable[[Any], str] = lambda request: "" if request is None else str(request)
        self._body: Callable[[Any], Any] = lambda _request: None

    @staticmethod
    def parse(object_map: dict[str, Any], prefix: str, output: dict[str, "Subscription"]) -> None:
        for key, value in object_map.items():
            if isinstance(value, Subscription):
                value.prefix = f"{prefix}/{key}" if prefix else key
                output[f"{prefix}{key}"] = value
            elif isinstance(value, dict):
                Subscription.parse(value, f"{prefix}{key}/", output)

    def topic(self, make: Callable[[Any], str]) -> None:
        self._topic = make

    # Keep TS quirk for behavior parity.
    def body(self, make: Callable[[Any], Any]) -> None:
        self._topic = cast(Callable[[Any], str], make)

    def send(self, request: Any, body: Any | None = None) -> None:
        topic = self._topic(request)
        payload = body if body is not None else self._body(request)
        if inspect.isawaitable(payload):
            async def delayed_publish() -> None:
                try:
                    value = await cast(Awaitable[Any], payload)
                    self.publish(topic, value)
                except Exception:
                    pass

            asyncio.create_task(delayed_publish())
        else:
            self.publish(topic, payload)

    def publish(self, topic: str, body: Any | None = None) -> None:
        if not body:
            return
        full_topic = f"{self.prefix}/{topic}" if self.prefix else topic
        event = SubscriptionEvent(topic=full_topic, body=body)
        for publisher in list(self.publishers):
            publisher.publish(event)


class _SendingRequest:
    def __init__(self, response: "asyncio.Future[Any]", cancel_fn: Callable[[], None]) -> None:
        self.response = response
        self._cancel = cancel_fn

    def cancel(self) -> None:
        self._cancel()


class _CancelHandle:
    def __init__(self, cancel_fn: Callable[[], None]) -> None:
        self._cancel = cancel_fn

    def cancel(self) -> None:
        self._cancel()


class Values:
    def __init__(
        self,
        channel: "Channel[Any]",
        path: str,
        body: Any,
        context: Any,
        on_send: Callable[[dict[str, Any]], None],
        on_cancel: Callable[[int], None],
        sender: "ChannelSender",
    ) -> None:
        self._channel = channel
        self._path = path
        self._body = body
        self._context = context
        self._on_send = on_send
        self._on_cancel = on_cancel
        self._sender = sender

        self._is_running = False
        self._is_closed = False
        self._queued: deque[dict[str, Any]] = deque()
        self._pending: deque[asyncio.Future[dict[str, Any]]] = deque()
        self._rid: int | None = None

    def _start(self) -> None:
        if self._is_running:
            return
        self._is_running = True

        request = self._channel.make_stream(
            self._path,
            self._body,
            self._context,
            self._response,
            self._sender,
        )
        self._rid = cast(int, request["id"])
        self._on_send(request)

    def _response(self, response: dict[str, Any]) -> None:
        if self._pending:
            waiter = self._pending.popleft()
            if not waiter.done():
                waiter.set_result(response)
        else:
            self._queued.append(response)

    def _process(self, response: dict[str, Any]) -> Any:
        if response.get("error") is not None:
            self._rid = None
            raise ChannelError(str(response["error"]))
        if response.get("done"):
            self._rid = None
            self._is_closed = True
            raise StopAsyncIteration
        return response.get("body")

    async def __anext__(self) -> Any:
        if self._is_closed:
            raise StopAsyncIteration

        if self._queued:
            response = self._queued.popleft()
            return self._process(response)

        self._start()

        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending.append(waiter)
        response = await waiter
        return self._process(response)

    async def next(self) -> dict[str, Any]:
        try:
            value = await self.__anext__()
            return {"value": value, "done": False}
        except StopAsyncIteration:
            return {"value": None, "done": True}

    def _cancel(self) -> None:
        if self._rid is None:
            return
        rid = self._rid
        self._rid = None
        self._on_cancel(rid)

    async def aclose(self) -> None:
        self._cancel()
        self._is_closed = True

    async def return_(self) -> dict[str, Any]:
        await self.aclose()
        return {"value": None, "done": True}

    def __aiter__(self) -> AsyncIterator[Any]:
        async def generator() -> AsyncIterator[Any]:
            try:
                while True:
                    yield await self.__anext__()
            except StopAsyncIteration:
                return
            finally:
                await self.aclose()

        return generator()


class _RunningStream:
    def __init__(self, iterator: Any, task: asyncio.Task[Any] | None) -> None:
        self._iterator = iterator
        self._task = task

    async def aclose(self) -> None:
        close = getattr(self._iterator, "aclose", None) or getattr(self._iterator, "return", None)
        if close is not None:
            try:
                result = close()
                if inspect.isawaitable(result):
                    await cast(Awaitable[Any], result)
            except RuntimeError:
                pass
            except Exception:
                pass
        if self._task is not None and not self._task.done():
            self._task.cancel()


class ChannelSender:
    def __init__(self, channel: "Channel[Any]", connection: ConnectionInterface) -> None:
        self._channel = channel
        self._connection = connection
        self.streams: dict[int, AsyncIterator[Any]] = {}
        self.requests: dict[int, CancellableRequest] = {}

    async def send(self, path: str, body: Any = None, context: Any = None) -> Any:
        loop = _ensure_event_loop()
        future: asyncio.Future[Any] | None = None
        send_id: int | None = None
        sent_notified = False
        resolved = False
        value: Any = None
        error: Exception | None = None

        def on_response(response: dict[str, Any]) -> None:
            nonlocal send_id, sent_notified, resolved, value, error, future
            if response.get("error") is not None:
                error = ChannelError(str(response["error"]))
                if future is not None and not future.done():
                    future.set_exception(error)
            else:
                value = response.get("body")
                if future is not None and not future.done():
                    future.set_result(value)
            resolved = True
            if send_id is not None:
                self._connection.sent(send_id)
                sent_notified = True

        request = self._channel.make_request(path, body, context, on_response, self, cancellable=False)
        send_id = self._connection.send(request)
        if resolved:
            if not sent_notified and send_id is not None:
                self._connection.sent(send_id)
            if error is not None:
                raise error
            return value

        future = loop.create_future()
        if resolved:
            if error is not None:
                future.set_exception(error)
            else:
                future.set_result(value)

        return await future

    def request(self, path: str, body: Any = None, context: Any = None) -> SendingRequest:
        loop = _ensure_event_loop()
        future: asyncio.Future[Any] = loop.create_future()
        send_id: int | None = None
        request_id: int | None = None

        def on_response(response: dict[str, Any]) -> None:
            nonlocal send_id, request_id
            if response.get("error") is not None:
                if not future.done():
                    future.set_exception(ChannelError(str(response["error"])))
            else:
                if not future.done():
                    future.set_result(response.get("body"))

            if send_id is not None:
                self._connection.sent(send_id)
                send_id = None
                request_id = None

        request = self._channel.make_request(path, body, context, on_response, self)
        request_id = cast(int, request["id"])
        send_id = self._connection.send(request)

        def cancel() -> None:
            nonlocal send_id, request_id
            if send_id is not None:
                cancelled = self._connection.cancel(send_id)
                if not cancelled and request_id is not None:
                    self._connection.notify({"cancel": request_id})
            elif request_id is not None:
                self._connection.notify({"cancel": request_id})
            if request_id is not None:
                self._channel.cancel(request_id)
            send_id = None
            request_id = None

        return _SendingRequest(future, cancel)

    def values(self, path: str, body: Any = None, context: Any = None) -> Values:
        send_id: int | None = None

        def on_send(payload: dict[str, Any]) -> None:
            nonlocal send_id
            send_id = self._connection.send(payload)

        def on_cancel(rid: int) -> None:
            nonlocal send_id
            if send_id is not None:
                cancelled = self._connection.cancel(send_id)
                if not cancelled:
                    self._connection.notify({"cancel": rid})
            else:
                self._connection.notify({"cancel": rid})
            self._channel.cancel(rid)
            send_id = None

        return Values(self._channel, path, body, context, on_send, on_cancel, self)

    async def subscribe(
        self,
        path: str,
        body: Any,
        event: Callable[[Any], None],
        context: Any = None,
    ) -> _CancelHandle:
        loop = _ensure_event_loop()
        future: asyncio.Future[_CancelHandle] = loop.create_future()

        def on_response(response: dict[str, Any]) -> None:
            if response.get("error") is not None:
                if not future.done():
                    future.set_exception(ChannelError(str(response["error"])))
                return

            topic = cast(str, response.get("topic"))
            cancel_topic = self._connection.add_topic(topic, event)

            def cancel() -> None:
                cancelled = cancel_topic()
                if cancelled:
                    self._connection.notify({"unsub": topic})

            if not future.done():
                future.set_result(_CancelHandle(cancel))

        request = self._channel.make_subscription(path, body, context, on_response, self)
        self._connection.send(request)

        return await future

    def stop(self) -> None:
        self._connection.stop()


def _is_promise(value: Any) -> bool:
    return inspect.isawaitable(value)


def _ensure_event_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        try:
            return asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop


async def _as_async_iterator(value: Any) -> AsyncIterator[Any]:
    if isinstance(value, AsyncIterator):
        return value
    if hasattr(value, "__aiter__"):
        return cast(AsyncIterator[Any], value)
    if isinstance(value, Iterable):

        async def generator() -> AsyncIterator[Any]:
            for item in value:
                yield item

        return generator()
    raise ChannelError("stream must return an iterator")


class Channel(Generic[StateT]):
    def __init__(self) -> None:
        self._id = 0
        self.publish: Callable[[str, Any], None] = lambda _topic, _body: None
        self.requests: dict[int, PendingRequest] = {}
        self.post_api: dict[str, FunctionHandler[StateT]] = {}
        self.other_post_api: list[tuple[Callable[[str], bool], FunctionHandler[StateT]]] = []
        self.stream_api: dict[str, StreamHandler[StateT]] = {}
        self.other_stream_api: list[tuple[Callable[[str], bool], StreamHandler[StateT]]] = []
        self._on_disconnect: Callable[[StateT, Sender], None] | None = None
        self.events_api: dict[str, Subscription] | None = None

    def post(self, path: str, request: FunctionHandler[StateT]) -> "Channel[StateT]":
        self.post_api[path] = request
        return self

    def post_other(self, path: Callable[[str], bool], request: FunctionHandler[StateT]) -> "Channel[StateT]":
        self.other_post_api.append((path, request))
        return self

    def postOther(self, path: Callable[[str], bool], request: FunctionHandler[StateT]) -> "Channel[StateT]":
        return self.post_other(path, request)

    def stream(self, path: str, request: StreamHandler[StateT]) -> "Channel[StateT]":
        self.stream_api[path] = request
        return self

    def stream_other(self, path: Callable[[str], bool], request: StreamHandler[StateT]) -> "Channel[StateT]":
        self.other_stream_api.append((path, request))
        return self

    def streamOther(self, path: Callable[[str], bool], request: StreamHandler[StateT]) -> "Channel[StateT]":
        return self.stream_other(path, request)

    def on_disconnect(self, action: Callable[[StateT, Sender], None]) -> "Channel[StateT]":
        self._on_disconnect = action
        return self

    def onDisconnect(self, action: Callable[[StateT, Sender], None]) -> "Channel[StateT]":
        return self.on_disconnect(action)

    def disconnect(self, state: StateT, sender: Sender) -> None:
        try:
            if self._on_disconnect is not None:
                self._on_disconnect(state, sender)
        except Exception:
            pass

        for stream in list(sender.streams.values()):
            try:
                close = getattr(stream, "aclose", None) or getattr(stream, "return", None)
                if close is None:
                    continue
                value = close()
                if inspect.isawaitable(value):
                    asyncio.create_task(cast(Awaitable[Any], value))
            except Exception:
                pass

        for request in list(sender.requests.values()):
            request.is_cancelled = True
            for action in list(request.cancel_actions):
                try:
                    action()
                except Exception:
                    pass

        to_delete = [rid for rid, request in self.requests.items() if request.sender is sender]
        for request_id in to_delete:
            self.requests.pop(request_id, None)

        sender.streams.clear()
        sender.requests.clear()

    def make_request(
        self,
        path: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
        cancellable: bool = True,
    ) -> dict[str, Any]:
        request_id = self._id
        self._id += 1
        request: dict[str, Any] = {"id": request_id, "path": path, "body": body, "context": context}
        if not cancellable:
            request["cancellable"] = False
        self.requests[request_id] = PendingRequest(request=request, response=response, sender=sender)
        return request

    def makeRequest(
        self,
        path: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
        cancellable: bool = True,
    ) -> dict[str, Any]:
        return self.make_request(path, body, context, response, sender, cancellable)

    def make_stream(
        self,
        stream: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
    ) -> dict[str, Any]:
        request_id = self._id
        self._id += 1
        request = {"id": request_id, "stream": stream, "body": body, "context": context}
        self.requests[request_id] = PendingRequest(request=request, response=response, sender=sender)
        return request

    def makeStream(
        self,
        stream: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
    ) -> dict[str, Any]:
        return self.make_stream(stream, body, context, response, sender)

    def make_subscription(
        self,
        sub: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
    ) -> dict[str, Any]:
        request_id = self._id
        self._id += 1
        request = {"id": request_id, "sub": sub, "body": body, "context": context}
        self.requests[request_id] = PendingRequest(request=request, response=response, sender=sender)
        return request

    def makeSubscription(
        self,
        sub: str,
        body: Any,
        context: Any,
        response: Callable[[dict[str, Any]], None],
        sender: Sender | None = None,
    ) -> dict[str, Any]:
        return self.make_subscription(sub, body, context, response, sender)

    def cancel(self, request_id: int) -> None:
        self.requests.pop(request_id, None)

    def receive(self, some: Any, controller: Controller[StateT]) -> None:
        if isinstance(some, list):
            for item in some:
                self.receive_one(item, controller)
        else:
            self.receive_one(some, controller)

    def receiveOne(self, some: Any, controller: Controller[StateT]) -> None:
        self.receive_one(some, controller)

    def receive_one(self, some: Any, controller: Controller[StateT]) -> None:
        if not isinstance(some, dict):
            return

        if some.get("path"):
            path = cast(str, some.get("path"))
            request_id = some.get("id")
            api = self.post_api.get(path)
            if api is None:
                for matcher, candidate in self.other_post_api:
                    if matcher(path):
                        api = candidate
                        break

            task: CancellableRequest | None = None
            try:
                if api is None:
                    raise ChannelError("api not found")

                if request_id is not None and some.get("cancellable", True) is not False:

                    def cancel_impl() -> None:
                        controller.response({"id": request_id, "error": "cancelled"})

                    task = CancellableRequest(cancel_impl)

                body_ctx = BodyContext(
                    body=some.get("body"),
                    context=some.get("context") if some.get("context") is not None else {},
                    sender=controller.sender,
                    state=controller.state,
                    path=path,
                    task=task,
                )
                result = api(body_ctx)

                if request_id is not None:
                    if _is_promise(result):
                        rid = cast(int, request_id)
                        if task is not None:
                            controller.sender.requests[rid] = task

                        async def done() -> None:
                            try:
                                value = await cast(Awaitable[Any], result)
                                if not (task and task.is_cancelled):
                                    controller.response({"id": request_id, "body": value})
                            except Exception as error:
                                if not (task and task.is_cancelled):
                                    controller.response({"id": request_id, "error": str(error)})
                            finally:
                                if task is not None:
                                    controller.sender.requests.pop(rid, None)

                        asyncio.create_task(done())
                    else:
                        controller.response({"id": request_id, "body": result})
                        if task is not None:
                            controller.sender.requests.pop(cast(int, request_id), None)
            except Exception as error:
                if request_id is not None:
                    if not (task and task.is_cancelled):
                        controller.response({"id": request_id, "error": str(error)})
                    if task is not None:
                        controller.sender.requests.pop(cast(int, request_id), None)

        elif some.get("stream"):
            request_id = some.get("id")
            stream_path = cast(str, some.get("stream"))
            try:
                api = self.stream_api.get(stream_path)
                if api is None:
                    for matcher, candidate in self.other_stream_api:
                        if matcher(stream_path):
                            api = candidate
                            break

                if api is None:
                    raise ChannelError("api not found")
                if request_id is None:
                    raise ChannelError("stream requires id")

                asyncio.create_task(
                    self._stream_request(
                        cast(int, request_id),
                        controller,
                        stream_path,
                        some.get("body"),
                        some.get("context") if some.get("context") is not None else {},
                        api,
                    )
                )
            except Exception as error:
                if request_id is not None:
                    controller.response({"id": request_id, "error": str(error)})

        elif some.get("cancel") is not None:
            try:
                request_id = cast(int | None, some.get("cancel"))
                if request_id is None:
                    raise ChannelError("cancellation requires id")

                stream = controller.sender.streams.pop(request_id, None)
                if stream is not None:
                    close = getattr(stream, "aclose", None) or getattr(stream, "return", None)
                    if close is not None:
                        value = close()
                        if inspect.isawaitable(value):
                            asyncio.create_task(cast(Awaitable[Any], value))

                request = controller.sender.requests.get(request_id)
                if request is not None:
                    request.cancel()
                    controller.sender.requests.pop(request_id, None)
            except Exception:
                pass

        elif some.get("sub"):
            request_id = some.get("id")
            subscription = None if self.events_api is None else self.events_api.get(cast(str, some.get("sub")))
            try:
                if subscription is None:
                    raise ChannelError("subscription not found")

                topic = subscription._topic(some.get("body"))
                topic = f"{subscription.prefix}/{topic}"
                body = subscription._body(some.get("body"))

                if _is_promise(body):
                    async def done() -> None:
                        try:
                            value = await cast(Awaitable[Any], body)
                            controller.subscribe(topic)
                            response = {"topic": topic, "body": value}
                            if request_id is not None:
                                response["id"] = request_id
                            controller.response(response)
                        except Exception as error:
                            if request_id is not None:
                                controller.response({"id": request_id, "error": str(error)})

                    asyncio.create_task(done())
                else:
                    controller.subscribe(topic)
                    response = {"topic": topic, "body": body}
                    if request_id is not None:
                        response["id"] = request_id
                    controller.response(response)
            except Exception as error:
                if request_id is not None:
                    controller.response({"id": request_id, "error": str(error)})

        elif some.get("unsub"):
            controller.unsubscribe(cast(str, some.get("unsub")))

        elif some.get("topic"):
            request_id = some.get("id")
            if request_id is not None:
                request = self.requests.pop(cast(int, request_id), None)
                if request is not None:
                    request.response(some)
            if some.get("body"):
                controller.event(cast(str, some.get("topic")), some.get("body"))

        elif some.get("id") is not None:
            request_id = cast(int, some.get("id"))
            request = self.requests.get(request_id)
            if request is None:
                return
            if not ("stream" in request.request and not some.get("done")):
                self.requests.pop(request_id, None)
            request.response(some)

    async def _stream_request(
        self,
        request_id: int,
        controller: Controller[StateT],
        path: str,
        body: Any,
        context: Any,
        stream: StreamHandler[StateT],
    ) -> None:
        try:
            body_ctx = BodyContext(
                body=body,
                sender=controller.sender,
                state=controller.state,
                path=path,
                context=context,
            )
            result = stream(body_ctx)
            if _is_promise(result):
                result = await cast(Awaitable[Any], result)
            values = await _as_async_iterator(result)
            controller.sender.streams[request_id] = _RunningStream(values, asyncio.current_task())
            try:
                async for value in values:
                    controller.response({"id": request_id, "body": value})
                controller.response({"id": request_id, "done": True})
            except Exception:
                pass
        except Exception as error:
            controller.response({"id": request_id, "error": str(error)})
        finally:
            controller.sender.streams.pop(request_id, None)

    def events(self, events: dict[str, Any], prefix: str = "") -> "Channel[StateT]":
        if self.events_api is None:
            self.events_api = {}

        if isinstance(events, dict):
            Subscription.parse(events, prefix, self.events_api)

        return self

    def api(self, api: dict[str, Any]) -> "Channel[StateT]":
        self._parse_api(api, "")
        return self

    def merge(self, channel: "Channel[StateT]", prefix: str = "") -> "Channel[StateT]":
        p = f"{prefix}/" if prefix else ""
        if channel.events_api:
            if self.events_api is None:
                self.events_api = {}
            for key, value in channel.events_api.items():
                self.events_api[f"{p}{key}"] = value
        for key, value in channel.post_api.items():
            self.post(f"{p}{key}", value)
        for key, value in channel.stream_api.items():
            self.stream(f"{p}{key}", value)
        return self

    def _parse_api(self, api: dict[str, Any], prefix: str) -> None:
        for key, value in api.items():
            if callable(value):
                path = prefix if key == "_" else f"{prefix}/{key}" if prefix else key
                if inspect.isgeneratorfunction(value) or inspect.isasyncgenfunction(value):
                    self.stream(path, value)
                else:
                    self.post(path, value)
            elif isinstance(value, dict):
                child_prefix = f"{prefix}/{key}" if prefix else key
                self._parse_api(value, child_prefix)

    def connect(self, address: str | int, options: Any | None = None) -> Any:
        from .client import connect_channel

        return connect_channel(self, address, options)

    def listen(self, address: str | int, options: Any | None = None) -> Any:
        from .server import listen_channel

        return listen_channel(self, address, options)

    def process(self) -> Sender:
        from .process import process_channel

        return process_channel(self)


def make_sender(channel: Channel[Any], connection: ConnectionInterface) -> ChannelSender:
    return ChannelSender(channel, connection)
