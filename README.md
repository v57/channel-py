<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Python implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation
```bash
uv sync --extra dev
```

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
