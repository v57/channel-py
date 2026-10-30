"""QUIC transport (experimental, see ../channel/docs/quic.md).

One bidirectional stream per connection carries the same JSON messages as a WebSocket, each prefixed with its length
as a QUIC variable-length integer (RFC 9000 section 16). Needs the `quic` extra (aioquic); `import channel_py` never
imports this module, `connect("quic://…")` and `Channel.listen_quic` do.
"""

from __future__ import annotations

import asyncio
import hashlib
import socket
import ssl
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

try:
    from aioquic.asyncio.protocol import QuicConnectionProtocol
    from aioquic.asyncio.server import QuicServer as _AioQuicServer
    from aioquic.asyncio.server import serve as _serve
    from aioquic.quic import events
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.connection import QuicConnection
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
except ImportError as error:  # pragma: no cover - depends on the install
    raise ImportError('the QUIC transport needs aioquic: pip install "channel-py[quic]"') from error

from .channel import Channel, Subscription, make_sender
from .client import QuicOptions, WebSocketClient
from .jsonx import dumpb, loads
from .server import BodyContext, Subscriptions
from .types import DEFAULT_MAX_PAYLOAD, SubscriptionEvent

ALPN = "channel/1"
# Largest accepted frame, same as Bun's default WebSocket payload limit
MAX_FRAME = DEFAULT_MAX_PAYLOAD
# Connections silent for 30 s are closed; clients send an empty frame every 15 s to stay open
IDLE_TIMEOUT = 30.0
KEEPALIVE_INTERVAL = 15.0
# A handshake that gets no answer (nothing listens, packets dropped) is given up after this long
HANDSHAKE_TIMEOUT = 10.0
KEEPALIVE = b"\x00"


class FrameTooLarge(ValueError):
    pass


def encode_frame(payload: bytes) -> bytes:
    """Length prefix (RFC 9000 varint, 1, 2, 4 or 8 bytes) followed by the payload."""
    n = len(payload)
    if n < 0x40:
        header = bytes((n,))
    elif n < 0x4000:
        header = (0x4000 | n).to_bytes(2, "big")
    elif n < 0x40000000:
        header = (0x80000000 | n).to_bytes(4, "big")
    else:
        header = (0xC000000000000000 | n).to_bytes(8, "big")
    return header + payload


class FrameReader:
    """Splits a byte stream back into frames. Empty frames (keepalives) are skipped."""

    def __init__(self, max_payload: int = MAX_FRAME) -> None:
        self.max_payload = max_payload
        self._buffer = bytearray()

    def push(self, data: bytes) -> list[bytes]:
        buffer = self._buffer
        buffer += data
        frames: list[bytes] = []
        offset = 0
        end = len(buffer)
        while offset < end:
            first = buffer[offset]
            size = 1 << (first >> 6)
            if end - offset < size:
                break
            length = first & 0x3F
            for i in range(1, size):
                length = (length << 8) | buffer[offset + i]
            if length > self.max_payload:
                raise FrameTooLarge("message too big")
            stop = offset + size + length
            if stop > end:
                break
            if length:
                frames.append(bytes(buffer[offset + size : stop]))
            offset = stop
        if offset:
            del buffer[:offset]
        return frames


def parse_fingerprint(fingerprint: str) -> bytes:
    """`DD:D2:…` or `ddd2…` → the 32 bytes of a SHA-256 fingerprint."""
    text = fingerprint.replace(":", "").strip().lower()
    try:
        value = bytes.fromhex(text)
    except ValueError:
        value = b""
    if len(value) != 32:
        raise ValueError(f"invalid SHA-256 fingerprint: {fingerprint!r}")
    return value


def _pem(value: str | bytes) -> bytes:
    return value.encode() if isinstance(value, str) else bytes(value)


def _option(options: Any, snake: str, camel: str | None = None) -> Any:
    if options is None:
        return None
    if isinstance(options, dict):
        return options.get(snake, options.get(camel or snake))
    return getattr(options, snake, getattr(options, camel or snake, None))


class _FramedProtocol(QuicConnectionProtocol):
    """A QUIC connection carrying frames over its one bidirectional stream."""

    def __init__(self, quic: QuicConnection, *, max_payload: int, accepts_stream: bool) -> None:
        super().__init__(quic)
        # The transport parameters let the client open exactly one bidirectional stream and nothing else; a peer
        # that opens more gets STREAM_LIMIT_ERROR. aioquic has no public setting for these limits.
        quic._local_max_streams_bidi.value = 1 if accepts_stream else 0
        quic._local_max_streams_uni.value = 0
        self._reader = FrameReader(max_payload)
        self.stream_id: int | None = None
        self.finished = False
        self.handshake: asyncio.Future[None] = self._loop.create_future()
        self.on_frame: Callable[[bytes], None] = lambda _frame: None
        self.on_finish: Callable[[Exception | None], None] = lambda _error: None
        self._silent = False

    def open_stream(self) -> None:
        self.stream_id = self._quic.get_next_available_stream_id()

    def write(self, data: bytes) -> None:
        if self.finished or self.stream_id is None:
            return
        self._quic.send_stream_data(self.stream_id, data)
        self._transmit_soon()

    def send_frame(self, payload: bytes) -> None:
        self.write(encode_frame(payload))

    def abort(self, error: Exception | None = None) -> None:
        """Closes the connection (CONNECTION_CLOSE goes out right away) and reports it."""
        if self.finished:
            return
        if not self._silent:
            self.close()
        self._finish(error)

    def silence(self) -> None:
        """Stops sending: the socket is about to be closed."""
        self._silent = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def transmit(self) -> None:
        if self._silent:
            return
        super().transmit()

    def error_received(self, exc: OSError) -> None:
        # ICMP errors on the client's connected socket, such as port unreachable while nothing listens
        self.abort(exc)

    def _finish(self, error: Exception | None) -> None:
        if self.finished:
            return
        self.finished = True
        if not self.handshake.done():
            self.handshake.set_exception(error or ConnectionError("connection closed during the handshake"))
            self.handshake.exception()  # retrieved: nobody may be waiting any more
        self.on_finish(error)

    def quic_event_received(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.StreamDataReceived):
            if self.stream_id is None and event.stream_id % 4 == 0 and not self._quic._is_client:
                self.stream_id = event.stream_id
            if event.stream_id != self.stream_id or self.finished:
                return
            try:
                frames = self._reader.push(event.data)
            except FrameTooLarge as error:
                self.abort(error)
                return
            for frame in frames:
                if self.finished:
                    return
                self.on_frame(frame)
            if event.end_stream:
                self.abort()
        elif isinstance(event, events.StreamReset):
            if event.stream_id == self.stream_id:
                self.abort()
        elif isinstance(event, events.HandshakeCompleted):
            if not self.handshake.done():
                self.handshake.set_result(None)
        elif isinstance(event, events.ConnectionTerminated):
            reason = event.reason_phrase or f"error {event.error_code}"
            self._finish(None if self.handshake.done() else ConnectionError(f"QUIC handshake failed: {reason}"))


class _ConnectedTransport:
    """The client's own UDP socket is connected to the server, so datagrams go out without an address."""

    __slots__ = ("transport",)

    def __init__(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def sendto(self, data: bytes, addr: Any = None) -> None:
        self.transport.sendto(data)


_END = object()


class _ClientSocket:
    """What WebSocketClient's loop expects of a connection: send, close and the received text messages."""

    def __init__(self, protocol: _FramedProtocol, transport: asyncio.DatagramTransport) -> None:
        self._protocol = protocol
        self._transport = transport
        self._messages: deque[Any] = deque()
        self._waiter: asyncio.Future[None] | None = None
        self._error: Exception | None = None
        protocol.on_frame = self._received
        protocol.on_finish = self._finished
        self._keepalive = asyncio.get_running_loop().create_task(self._keep_alive())
        if protocol.finished:
            self._finished(None)

    def _wake(self) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _received(self, frame: bytes) -> None:
        self._messages.append(frame)
        self._wake()

    def _finished(self, error: Exception | None) -> None:
        if isinstance(error, FrameTooLarge):
            self._error = error
        self._messages.append(_END)
        self._keepalive.cancel()
        self._protocol.silence()
        self._transport.close()
        self._wake()

    async def _keep_alive(self) -> None:
        while True:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            self._protocol.write(KEEPALIVE)

    async def send(self, text: str) -> None:
        self._protocol.send_frame(text.encode())

    async def close(self) -> None:
        self._protocol.abort()

    def __aiter__(self) -> "_ClientSocket":
        return self

    async def __anext__(self) -> str:
        while not self._messages:
            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter
        message = self._messages.popleft()
        if message is _END:
            self._messages.append(_END)
            if self._error is not None:
                raise self._error
            raise StopAsyncIteration
        try:
            return message.decode()
        except UnicodeDecodeError as error:
            self._error = ValueError(f"invalid UTF-8 from server: {error}")
            self._protocol.abort()
            raise self._error from None


class QuicClient(WebSocketClient):
    """Client side of `quic://host:port`: WebSocketClient's reconnect, replay and outbox over QUIC. Each connection
    gets its own UDP socket, as Network.framework's QUIC servers take one connection per client address and port."""

    def __init__(
        self,
        address: str,
        headers: Callable[[], dict[str, str] | Awaitable[dict[str, str]]] | None = None,
        max_payload: int = DEFAULT_MAX_PAYLOAD,
        tls: QuicOptions | dict[str, Any] | None = None,
    ) -> None:
        url = urlsplit(address)
        if url.scheme != "quic" or not url.hostname or url.port is None:
            raise ValueError(f"not a quic://host:port address: {address!r}")
        self.host = url.hostname
        self.port = url.port
        ca = _option(tls, "ca")
        fingerprint = _option(tls, "fingerprint")
        self.ca = _pem(ca) if ca else None
        self.fingerprint = parse_fingerprint(fingerprint) if fingerprint else None
        self.servername: str | None = _option(tls, "servername", "serverName")
        super().__init__(address, headers, max_payload)

    def _configuration(self) -> QuicConfiguration:
        configuration = QuicConfiguration(
            is_client=True,
            alpn_protocols=[ALPN],
            idle_timeout=IDLE_TIMEOUT,
            server_name=self.servername or self.host,
        )
        if self.fingerprint is not None:
            # The pin replaces verification; it is checked once the handshake is done
            configuration.verify_mode = ssl.CERT_NONE
        elif self.ca is not None:
            configuration.cadata = self.ca
        return configuration

    async def _open(self, headers: dict[str, str] | None) -> Any:
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo(self.host, self.port, type=socket.SOCK_DGRAM)
        family, _, _, _, address = infos[0]
        quic = QuicConnection(configuration=self._configuration())
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: _FramedProtocol(quic, max_payload=self.max_payload, accepts_stream=False),
            remote_addr=address,
            family=family,
        )
        protocol._transport = _ConnectedTransport(transport)
        try:
            protocol.connect(address)
            await asyncio.wait_for(asyncio.shield(protocol.handshake), HANDSHAKE_TIMEOUT)
            if self.fingerprint is not None:
                certificate = quic.tls._peer_certificate
                der = certificate.public_bytes(serialization.Encoding.DER) if certificate is not None else b""
                if hashlib.sha256(der).digest() != self.fingerprint:
                    raise ssl.SSLCertVerificationError("certificate fingerprint mismatch")
            protocol.open_stream()
            client = _ClientSocket(protocol, transport)
            protocol.send_frame(dumpb({"hello": headers or {}}))
            return client
        except BaseException:
            protocol.abort()
            protocol.silence()
            transport.close()
            raise


@dataclass(slots=True)
class QuicListenOptions:
    # PEM private key and certificate chain
    key: str | bytes
    cert: str | bytes
    state: Callable[[dict[str, str | None]], Any | Awaitable[Any]] | None = None
    on_connect: Callable[[BodyContext], Any | Awaitable[Any]] | None = None
    on_disconnect: Callable[[BodyContext], Any | Awaitable[Any]] | None = None
    # Largest frame accepted (bytes), 16 MiB like the WebSocket limit
    max_payload: int = DEFAULT_MAX_PAYLOAD


class _ServerConnection:
    """The sender's connection on the server side."""

    def __init__(self, session: "_ServerSession", subscriptions: Subscriptions) -> None:
        self.session = session
        self.subscriptions = subscriptions

    def send(self, body: Any) -> int:
        self.session.send_frame(dumpb(body))
        return 0

    def sent(self, request_id: int) -> None:
        return None

    def cancel(self, request_id: int) -> bool:
        return False

    def notify(self, body: Any) -> None:
        self.session.send_frame(dumpb(body))

    def add_topic(self, topic: str, event: Callable[[Any], None]) -> Callable[[], bool]:
        return self.subscriptions.add_topic(topic, event)

    def stop(self) -> None:
        self.session.abort()


async def _settle(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


class _ServerSession(_FramedProtocol):
    def __init__(self, quic: QuicConnection, server: "QuicServer") -> None:
        super().__init__(quic, max_payload=server.max_payload, accepts_stream=True)
        self.server = server
        self.subscribed: set[str] = set()
        self.context: BodyContext | None = None
        self.controller: Any = None
        self._pending: deque[bytes] = deque()
        self._setup: asyncio.Task[None] | None = None
        self.on_frame = self._frame
        self.on_finish = self._ended
        server._sessions.add(self)

    def _frame(self, frame: bytes) -> None:
        if self.controller is not None:
            self._receive(frame)
            return
        self._pending.append(frame)
        if self._setup is None:
            self._setup = self._loop.create_task(self._start())

    def _receive(self, frame: bytes) -> None:
        try:
            self.server._channel.receive(loads(frame), self.controller)
        except Exception:
            # Invalid JSON or a message the channel can't take: drop the connection, like the WebSocket server
            self.abort()

    async def _start(self) -> None:
        # First frame: {"hello": headers}, used like the WebSocket upgrade request headers
        try:
            hello = loads(self._pending.popleft())
            hello = hello.get("hello") if isinstance(hello, dict) else None
            if not isinstance(hello, dict):
                raise ValueError("the first frame isn't a hello")
            headers: dict[str, str | None] = {k.lower(): v for k, v in hello.items() if isinstance(v, str)}
            factory = self.server._option("state", "state")
            state = await _settle(factory(headers)) if factory is not None else {}
        except Exception:
            self.abort()
            return
        if self.finished:
            return
        channel = self.server._channel
        subscriptions = Subscriptions()
        sender = make_sender(channel, _ServerConnection(self, subscriptions))
        self.context = BodyContext(state=state, sender=sender, subscriptions=subscriptions)
        subscribed = self.subscribed

        class _Controller:
            def response(self_, response: dict[str, Any]) -> None:
                self.send_frame(dumpb(response))

            def subscribe(self_, topic: str) -> None:
                subscribed.add(topic)

            def unsubscribe(self_, topic: str) -> None:
                subscribed.discard(topic)

            def event(self_, topic: str, event: Any) -> None:
                subscriptions.received_event(topic, event)

            @property
            def sender(self_) -> Any:
                return sender

            @property
            def state(self_) -> Any:
                return state

        controller = _Controller()
        await _settle(_call(self.server._option("on_connect", "onConnect"), self.context))
        if self.finished:
            return
        # Frames that came in meanwhile go first, then new ones are received as they arrive (no await in between)
        self.controller = controller
        while self._pending and not self.finished:
            self._receive(self._pending.popleft())

    def _ended(self, _error: Exception | None) -> None:
        self.server._sessions.discard(self)
        self._pending.clear()
        if self._setup is not None and not self._setup.done() and self.context is None:
            self._setup.cancel()
        context = self.context
        if context is None:
            return
        self.server._channel.disconnect(context.state, context.sender)
        result = _call(self.server._option("on_disconnect", "onDisconnect"), context)
        if asyncio.iscoroutine(result):
            self._loop.create_task(result)


def _call(callback: Callable[[Any], Any] | None, argument: Any) -> Any:
    return callback(argument) if callback is not None else None


class QuicServer:
    """A running QUIC listener, returned by `Channel.listen_quic`. It starts in the background:
    `await server.wait_started()` before reading `port`."""

    def __init__(self, channel: Channel[Any], host: str, port: int, options: QuicListenOptions | dict[str, Any]) -> None:
        self._channel = channel
        self._host = host
        self._port = port
        self._options = options
        self.max_payload = self._option("max_payload", "maxPayload") or DEFAULT_MAX_PAYLOAD
        key, cert = self._option("key", "key"), self._option("cert", "cert")
        if not key or not cert:
            raise ValueError("listen_quic needs a PEM key and cert")
        certificates = x509.load_pem_x509_certificates(_pem(cert))
        self._configuration = QuicConfiguration(
            is_client=False,
            alpn_protocols=[ALPN],
            idle_timeout=IDLE_TIMEOUT,
            certificate=certificates[0],
            certificate_chain=certificates[1:],
            private_key=serialization.load_pem_private_key(_pem(key), password=None),
        )
        self._sessions: set[_ServerSession] = set()
        self._server: _AioQuicServer | None = None
        self._publishers: list[tuple[Subscription, Any]] = []
        self._stopped = False
        self._start_task = asyncio.get_running_loop().create_task(self._start())

    def _option(self, snake: str, camel: str) -> Any:
        return _option(self._options, snake, camel)

    async def _start(self) -> None:
        # No session tickets, so no 0-RTT: a replayed 0-RTT request could run twice
        self._server = await _serve(
            self._host,
            self._port,
            configuration=self._configuration,
            create_protocol=lambda quic, stream_handler=None: _ServerSession(quic, self),
        )
        self._attach_publishers()

    async def wait_started(self) -> "QuicServer":
        await self._start_task
        return self

    @property
    def port(self) -> int:
        if self._server is None or self._server._transport is None:
            return self._port
        return int(self._server._transport.get_extra_info("sockname")[1])

    def _attach_publishers(self) -> None:
        sessions = self._sessions
        for subscription in (self._channel.events_api or {}).values():

            class _Publisher:
                def publish(self, event: SubscriptionEvent) -> None:
                    frame: bytes | None = None
                    for session in list(sessions):
                        if event.topic in session.subscribed:
                            frame = frame or encode_frame(dumpb({"topic": event.topic, "body": event.body}))
                            session.write(frame)

            publisher = _Publisher()
            subscription.publishers.append(publisher)
            self._publishers.append((subscription, publisher))

    async def stop_async(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        try:
            await self._start_task
        except Exception:
            return
        for subscription, publisher in self._publishers:
            if publisher in subscription.publishers:
                subscription.publishers.remove(publisher)
        for session in list(self._sessions):
            session.abort()
            session.silence()
        if self._server is not None:
            for protocol in set(self._server._protocols.values()):
                protocol.silence()
            self._server.close()

    def stop(self) -> None:
        try:
            asyncio.get_running_loop().create_task(self.stop_async())
        except RuntimeError:
            asyncio.run(self.stop_async())


def listen_quic(
    channel: Channel[Any], address: str | int, options: QuicListenOptions | dict[str, Any]
) -> QuicServer:
    """`port` or `"host:port"` (`"[::1]:port"` for IPv6); a bare port listens on 127.0.0.1 like `listen`."""
    host = "127.0.0.1"
    if isinstance(address, str):
        host, _, port_text = address.rpartition(":")
        host = host.strip("[]") or "127.0.0.1"
        port = int(port_text)
    else:
        port = int(address)
    return QuicServer(channel, host, port, options)


__all__ = [
    "ALPN",
    "MAX_FRAME",
    "FrameReader",
    "FrameTooLarge",
    "QuicClient",
    "QuicListenOptions",
    "QuicServer",
    "encode_frame",
    "listen_quic",
    "parse_fingerprint",
]
