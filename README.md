<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Python implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation
```bash
uv sync --extra dev
```

# Dependencies

Three packages, for speed (plus `aioquic` with the optional `quic` extra): [`orjson`](https://pypi.org/project/orjson/) (JSON),
[`websockets`](https://pypi.org/project/websockets/) (client and server) and
[`uvloop`](https://pypi.org/project/uvloop/) (event loop, not on Windows).

# Usage

```python
# Server
from channel_py import Channel

server = Channel().post("hello", lambda _body: "world").listen(2049)
```

```python
# Client
from channel_py import Channel

client = Channel().connect(2049)
value = await client.send("hello")
print(value)
```

# Process transport (in-memory)

```python
from channel_py import Channel

sender = Channel().post("hello", lambda _body: "world").process()
value = await sender.send("hello")
```

# QUIC transport (experimental)

The same messages over QUIC instead of WebSocket (see `../channel/docs/quic.md` for the protocol). It needs
[`aioquic`](https://pypi.org/project/aioquic/), an optional extra: the WebSocket-only install doesn't get it, and
`import channel_py` never imports it.

```bash
pip install "channel-py[quic]"      # or: uv sync --extra quic
```

```python
# Server: QUIC always needs a key and certificate (PEM text)
from channel_py import Channel

key, cert = open("key.pem").read(), open("cert.pem").read()
server = Channel().post("hello", lambda _body: "world").listen_quic(4433, {"key": key, "cert": cert})
await server.wait_started()
# same state / on_connect / on_disconnect / max_payload options as listen(); listenQuic is an alias
await server.stop_async()
```

```python
# Client: connect() takes quic:// addresses
client = Channel().connect("quic://example.com:4433")
print(await client.send("hello"))
```

Certificates (`ConnectOptions(quic=QuicOptions(...))` or `{"quic": {...}}`):
- **Trusted certificate** (for example Let's Encrypt): nothing to configure (certifi's roots).
- **Self-signed**: `{"quic": {"ca": cert}}` trusts that PEM certificate. The address must be in its subject
  alternative names, as a DNS name or an IP address.
- **Pinned**: `{"quic": {"fingerprint": "DD:D2:37:…"}}` accepts only the certificate with that SHA-256 fingerprint
  (hex, colons optional) and skips the other checks.
- `servername` overrides the name sent and checked (it defaults to the address host).

The client dials each connection from its own UDP socket, never one shared endpoint (Network.framework servers take
one connection per client address and port). Reconnects, headers, streams and events work as over WebSocket.
Languages with QUIC: TypeScript, Go, Swift, C#, Rust, Python, Elixir, Java, Kotlin, C and Zig; Dart, Ruby and PHP stay
on WebSocket (libraries and status in `channel/docs/quic.md`).

# API naming

Both naming styles are supported:
- snake_case: `post_other`, `stream_other`, `on_disconnect`
- TS-style aliases: `postOther`, `streamOther`, `onDisconnect`

# Requirements

- Python `>=3.12`
- `uv`
- `bun` (for interop tests and TS benchmark target)

# Scripts

- `scripts/test`
  - runs unit tests, then interop tests against Bun fixture
- `scripts/benchmark`
  - follows `../channel/benchmark.md` exactly
  - starts 1 client worker process + 1 server worker process per stage on a random port
  - runs 4 stages: `1`, `10`, `100`, `1000` clients
  - each stage runs for `5000 ms` and targets `1000 req/s` per client
  - supports overrides: `BENCH_STAGE_MS`, `BENCH_RPS_PER_CLIENT`
  - writes `benchmark.json` in this format:

```json
{
  "language": "Python",
  "results": [
    {
      "rps": 0,
      "clients": 1,
      "client": { "cpu": 0, "mem": 0 },
      "server": { "cpu": 0, "mem": 0 }
    }
  ]
}
```
- `scripts/coverage`
  - runs full suite and writes LCOV to `coverage/lcov.info`

# Compatibility notes

- Message envelope matches the TS `channel` protocol fields (`id`, `path`, `body`, `context`, `error`, `cancel`, `sub`, `unsub`, `topic`, `stream`, `done`).
- Supports single-message and batch-message payloads.
- Stream cancellation semantics are validated in interop tests; cancellation timing can vary by transport/event-loop scheduling.
