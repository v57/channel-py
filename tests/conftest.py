from __future__ import annotations

import asyncio

import pytest_asyncio

from tests.fixtures.ts_server import start_ts_server, stop_ts_server

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass


@pytest_asyncio.fixture(scope="session")
async def ts_server() -> None:
    process = await start_ts_server()
    try:
        yield None
    finally:
        await stop_ts_server(process)
