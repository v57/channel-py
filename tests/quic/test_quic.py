"""QUIC transport: frames, the Python client against the Python server, the wire rules with raw frames, trust,
reconnect, and the TypeScript reference's client against the Python server. Mirrors channel-go/quic/quic_test.go."""

from __future__ import annotations

import asyncio
import json
import shutil
import ssl
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("aioquic")

from aioquic.asyncio.client import connect as raw_connect  # noqa: E402
from aioquic.asyncio.protocol import QuicConnectionProtocol  # noqa: E402
from aioquic.quic import events  # noqa: E402
from aioquic.quic.configuration import QuicConfiguration  # noqa: E402

from channel_py import Channel, QuicOptions, Subscription  # noqa: E402
from channel_py.quic import ALPN, FrameReader, FrameTooLarge, encode_frame  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
# The TypeScript reference's test certificate: localhost, 127.0.0.1 and ::1
CERT_DIR = ROOT.parent / "channel" / "tests" / "quic"
FINGERPRINT = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"


def read_pem(name: str) -> str:
    path = CERT_DIR / name
    if not path.exists():
        pytest.skip("the TypeScript reference's test certificate isn't next to this checkout")
    return path.read_text()


async def wait_for(ready: Any, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not ready():
        if loop.time() > deadline:
            raise TimeoutError("timed out")
        await asyncio.sleep(0.005)


# Frames


def test_frames() -> None:
    for size in (0, 1, 63, 64, 16383, 16384, 70000):
        payload = b"x" * size
        frame = encode_frame(payload)
        header = len(frame) - size
        assert header == (1 if size < 64 else 2 if size < 16384 else 4), size
        # A keepalive in front is skipped, and an empty frame yields nothing
        stream = b"\x00" + frame + encode_frame(b"next")
        expected = ([payload] if size else []) + [b"next"]
        assert FrameReader().push(stream) == expected
        # Byte by byte gives the same frames
        reader = FrameReader()
        got: list[bytes] = []
        for byte in stream:
            got += reader.push(bytes((byte,)))
        assert got == expected


def test_frame_lengths() -> None:
    # 8-byte lengths decode too
    assert FrameReader().push(bytes([0xC0, 0, 0, 0, 0, 0, 0, 5]) + b"hello") == [b"hello"]
    assert encode_frame(b"") == b"\x00"
    assert encode_frame(b"a" * 64)[:2] == bytes([0x40, 64])
    # Anything above the limit is refused, as soon as its length is known
    with pytest.raises(FrameTooLarge):
        FrameReader(10).push(encode_frame(b"x" * 11)[:4])
    assert FrameReader(10).push(encode_frame(b"x" * 10)) == [b"x" * 10]
    with pytest.raises(FrameTooLarge):
        FrameReader().push(bytes([0x81, 0, 0, 1]))  # 16 MiB + 1


def test_import_channel_does_not_load_aioquic() -> None:
    code = (
        "import sys, channel_py; from channel_py import Channel, QuicOptions; "
        "Channel().post('a', lambda b: 1).process(); "
        "assert 'aioquic' not in sys.modules and 'channel_py.quic' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


# A Python server and client


@dataclass
class Fixture:
    server: Any
    cert: str
    key: str
    ticks: Subscription
    connected: int = 0
    disconnected: int = 0
    dropped: bool = False
    servers: list[Any] = field(default_factory=list)

    @property
    def address(self) -> str:
        return f"quic://127.0.0.1:{self.server.port}"


def server_channel(f: Fixture) -> Channel[Any]:
    async def slow(_body: Any) -> str:
        await asyncio.sleep(0.3)
        return "slow"

    def disconnect(body: Any) -> str:
        # Once: the client sends unanswered requests again after reconnecting
        if not f.dropped:
            f.dropped = True
            body.sender.stop()
        return "dropped before"

    async def count(_body: Any):
        for i in range(3):
            yield i

    def tick(_body: Any) -> None:
        f.ticks.send("a", "tick")

    return (
        Channel()
        .post("hello", lambda _body: "world")
        .post("echo", lambda body: body.body)
        .post("name", lambda body: body.state.get("x-name"))
        .post("mirror", lambda body: body.sender.send("hello"))
        .post("slow", slow)
        .post("disconnect", disconnect)
        .stream("count", count)
        .post("tick", tick)
        .events({"ticks": f.ticks})
    )


async def listen(f: Fixture, address: str | int = "127.0.0.1:0") -> Any:
    def on_connect(_context: Any) -> None:
        f.connected += 1

    def on_disconnect(_context: Any) -> None:
        f.disconnected += 1

    server = server_channel(f).listen_quic(
        address,
        {"key": f.key, "cert": f.cert, "state": lambda headers: headers, "on_connect": on_connect, "on_disconnect": on_disconnect},
    )
    await server.wait_started()
    f.server = server
    f.servers.append(server)
    return server


@pytest.fixture
async def fixture() -> Any:
    f = Fixture(server=None, cert=read_pem("cert.pem"), key=read_pem("key.pem"), ticks=Subscription())
    await listen(f)
    yield f
    for server in f.servers:
        await server.stop_async()


def client_channel() -> Channel[Any]:
    return Channel().post("hello", lambda _body: "client world")


@pytest.fixture
async def clients() -> Any:
    made: list[Any] = []
    yield made
    for client in made:
        client.stop()
    await asyncio.sleep(0.01)


def connect(clients: list[Any], address: str, options: Any) -> Any:
    client = client_channel().connect(address, options)
    clients.append(client)
    return client


async def test_client_and_server(fixture: Fixture, clients: list[Any]) -> None:
    c = connect(clients, fixture.address, {"quic": {"ca": fixture.cert}, "headers": lambda: {"X-Name": "py"}})

    # post
    assert await asyncio.wait_for(c.send("hello"), 5) == "world"
    body = {"list": [1, "two", None], "text": "é" * 20000}
    assert await c.send("echo", body) == body
    # hello headers become the state, names lowercased
    assert await c.send("name") == "py"
    # the server calls the client back
    assert await c.send("mirror") == "client world"
    # stream
    assert [value async for value in c.values("count")] == [0, 1, 2]

    # events
    got: list[Any] = []
    subscription = await c.subscribe("ticks", "a", got.append)
    fixture.ticks.send("a", "tick")
    fixture.ticks.send("b", "not for us")
    fixture.ticks.send("a", "tick")
    await wait_for(lambda: len(got) == 2)
    subscription.cancel()
    await asyncio.sleep(0.02)
    fixture.ticks.send("a", "after cancel")
    await asyncio.sleep(0.05)
    assert got == ["tick", "tick"]

    # many in flight
    requests = [c.request("echo", i) for i in range(2000)]
    assert await asyncio.wait_for(asyncio.gather(*(r.response for r in requests)), 10) == list(range(2000))


async def test_dataclass_options_and_listen_quic_alias(fixture: Fixture, clients: list[Any]) -> None:
    from channel_py import ConnectOptions

    server = await Channel().post("hello", lambda _b: "alias").listenQuic(0, {"key": fixture.key, "cert": fixture.cert}).wait_started()
    fixture.servers.append(server)
    c = connect(clients, f"quic://localhost:{server.port}", ConnectOptions(quic=QuicOptions(ca=fixture.cert)))
    assert await asyncio.wait_for(c.send("hello"), 5) == "alias"


async def test_certificate_trust(fixture: Fixture, clients: list[Any]) -> None:
    pinned = connect(clients, fixture.address, {"quic": {"fingerprint": FINGERPRINT.replace(":", "").lower()}})
    assert await asyncio.wait_for(pinned.send("hello"), 5) == "world"
    by_name = connect(clients, fixture.address, {"quic": {"ca": fixture.cert, "servername": "localhost"}})
    assert await asyncio.wait_for(by_name.send("hello"), 5) == "world"

    refused = {
        "system roots": {},
        "wrong pin": {"fingerprint": "00" * 32},
        "name not in cert": {"ca": fixture.cert, "servername": "example.com"},
    }
    attempts = {name: connect(clients, fixture.address, {"quic": tls}) for name, tls in refused.items()}
    await asyncio.sleep(0.3)
    for name, client in attempts.items():
        assert not client.ws.is_connected, name
    assert fixture.connected == 2

    with pytest.raises(ValueError):
        Channel().connect(fixture.address, {"quic": {"fingerprint": "nope"}})
    with pytest.raises(ValueError):
        Channel().connect("quic://127.0.0.1", {})


# Wire rules, with raw frames


class RawProtocol(QuicConnectionProtocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.reader = FrameReader()
        self.frames: asyncio.Queue[bytes] = asyncio.Queue()

    def quic_event_received(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.StreamDataReceived):
            for frame in self.reader.push(event.data):
                self.frames.put_nowait(frame)


class RawSession:
    """Dials the server without the Channel client, to check the wire rules."""

    def __init__(self, port: int) -> None:
        configuration = QuicConfiguration(is_client=True, alpn_protocols=[ALPN], verify_mode=ssl.CERT_NONE)
        self._context = raw_connect("127.0.0.1", port, configuration=configuration, create_protocol=RawProtocol)
        self.protocol: RawProtocol

    async def __aenter__(self) -> "RawSession":
        self.protocol = await asyncio.wait_for(self._context.__aenter__(), 5)
        self.stream = self.protocol._quic.get_next_available_stream_id()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await asyncio.wait_for(self._context.__aexit__(None, None, None), 5)

    def write(self, data: bytes, stream: int | None = None) -> None:
        self.protocol._quic.send_stream_data(self.stream if stream is None else stream, data)
        self.protocol.transmit()

    async def closed(self) -> bool:
        try:
            await asyncio.wait_for(self.protocol.wait_closed(), 2)
            return True
        except TimeoutError:
            return False


async def test_raw_frames(fixture: Fixture) -> None:
    async with RawSession(fixture.server.port) as raw:
        hello = json.dumps({"hello": {"X-Name": "raw", "ignored": 1}}).encode()
        batch = b'[{"id":1,"path":"name"},{"id":2,"path":"hello"}]'
        raw.write(encode_frame(hello) + b"\x00" + encode_frame(batch))
        got: dict[int, Any] = {}
        while len(got) < 2:
            message = json.loads(await asyncio.wait_for(raw.protocol.frames.get(), 5))
            for item in message if isinstance(message, list) else [message]:
                got[item["id"]] = item["body"]
        assert got == {1: "raw", 2: "world"}


async def test_one_stream_per_connection(fixture: Fixture) -> None:
    async with RawSession(fixture.server.port) as raw:
        raw.write(encode_frame(b'{"hello":{}}'))
        # The server lets the client open one bidirectional stream and no unidirectional ones
        assert raw.protocol._quic._remote_max_streams_bidi == 1
        assert raw.protocol._quic._remote_max_streams_uni == 0
        # A client that opens a second one anyway is closed
        raw.protocol._quic._remote_max_streams_bidi = 2
        second = raw.protocol._quic.get_next_available_stream_id()
        raw.write(encode_frame(b'{"hello":{}}'), second)
        assert await raw.closed()
    async with RawSession(fixture.server.port) as raw:
        raw.write(encode_frame(b'{"hello":{}}'))
        raw.protocol._quic._remote_max_streams_uni = 1
        uni = raw.protocol._quic.get_next_available_stream_id(is_unidirectional=True)
        raw.write(b"\x00", uni)
        assert await raw.closed()


@pytest.mark.parametrize(
    "first",
    [
        pytest.param(encode_frame(b'{"id":1,"path":"hello"}'), id="a first frame that isn't a hello closes"),
        pytest.param(encode_frame(b'{"hello":"text"}'), id="a hello that isn't an object closes"),
        pytest.param(encode_frame(b'{"hello":{}}') + encode_frame(b"{nope"), id="invalid JSON closes"),
        pytest.param(encode_frame(b'{"hello":{}}') + bytes([0x81, 0, 0, 1]), id="a frame over the limit closes"),
    ],
)
async def test_wire_rules_close(fixture: Fixture, first: bytes) -> None:
    async with RawSession(fixture.server.port) as raw:
        raw.write(first)
        assert await raw.closed()


async def test_server_frame_limit_is_configurable(fixture: Fixture, clients: list[Any]) -> None:
    server = await Channel().post("echo", lambda b: b.body).listen_quic(
        0, {"key": fixture.key, "cert": fixture.cert, "max_payload": 100}
    ).wait_started()
    fixture.servers.append(server)
    c = connect(clients, f"quic://127.0.0.1:{server.port}", {"quic": {"ca": fixture.cert}})
    assert await asyncio.wait_for(c.send("echo", "small"), 5) == "small"
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(c.send("echo", "x" * 200), 5)


# Reconnect


async def test_reconnect_replays_requests(fixture: Fixture, clients: list[Any]) -> None:
    # Same as this port's WebSocket client: a drop fails the requests in flight with "disconnected", the client
    # reconnects, and requests made while it is disconnected are sent once it is back.
    port = fixture.server.port
    c = connect(clients, fixture.address, {"quic": {"ca": fixture.cert}})
    assert await asyncio.wait_for(c.send("hello"), 5) == "world"

    # The server drops the connection
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(c.send("disconnect"), 5)
    assert await asyncio.wait_for(c.send("hello"), 5) == "world"
    assert fixture.connected == 2

    # The server goes away: the request in flight fails, one made meanwhile goes to the next server on the port
    in_flight = c.request("slow")
    await asyncio.sleep(0.05)
    await fixture.server.stop_async()
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(in_flight.response, 5)
    await wait_for(lambda: not c.ws.is_connected)
    queued = c.request("echo", "queued")
    await asyncio.sleep(0.2)
    await listen(fixture, f"127.0.0.1:{port}")
    assert await asyncio.wait_for(queued.response, 10) == "queued"
    assert c.ws.is_connected

    # Stop fails what still waits
    c.stop()
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(c.send("hello"), 2)


async def test_stop_while_disconnected_fails_requests(clients: list[Any]) -> None:
    cert = read_pem("cert.pem")
    # Nothing listens there
    c = client_channel().connect("quic://127.0.0.1:1", {"quic": {"ca": cert}})
    request = c.request("hello")
    await asyncio.sleep(0.05)
    c.stop()
    with pytest.raises(Exception, match="disconnected"):
        await asyncio.wait_for(request.response, 2)


async def test_one_udp_socket_per_connection(fixture: Fixture, clients: list[Any]) -> None:
    a = connect(clients, fixture.address, {"quic": {"ca": fixture.cert}})
    b = connect(clients, fixture.address, {"quic": {"ca": fixture.cert}})
    await asyncio.wait_for(asyncio.gather(a.send("hello"), b.send("hello")), 5)
    ports = {c.ws.ws._transport.get_extra_info("sockname")[1] for c in (a, b)}
    assert len(ports) == 2


# The TypeScript reference's client against the Python server


async def test_interop_typescript_client(fixture: Fixture) -> None:
    bun = shutil.which("bun")
    if bun is None:
        pytest.skip("bun isn't installed")
    if not (ROOT.parent / "channel" / "quic.ts").exists():
        pytest.skip("the TypeScript reference isn't next to this checkout")
    process = await asyncio.create_subprocess_exec(
        bun,
        str(ROOT / "tests" / "quic" / "client.ts"),
        str(fixture.server.port),
        str(CERT_DIR / "cert.pem"),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    except TimeoutError:
        process.kill()
        raise
    assert process.returncode == 0, stderr.decode()
    assert json.loads(stdout) == {
        "hello": "world",
        "echo": {"a": [1, "b"]},
        "name": "ts",
        "mirror": "ts world",
        "values": [0, 1, 2],
        "event": "tick",
    }
    await wait_for(lambda: fixture.connected >= 1 and fixture.disconnected == fixture.connected)
