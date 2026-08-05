from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CHANNEL_DIR = ROOT / "channel"


async def wait_for_port(host: str, port: int, timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except Exception:
            if loop.time() > deadline:
                raise TimeoutError(f"Timed out waiting for {host}:{port}")
            await asyncio.sleep(0.05)


async def start_ts_server() -> asyncio.subprocess.Process:
    process = await asyncio.create_subprocess_exec(
        "bun",
        "tests/server.ts",
        cwd=str(CHANNEL_DIR),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await wait_for_port("127.0.0.1", 2049)
    except Exception:
        with suppress(Exception):
            process.terminate()
        with suppress(Exception):
            await asyncio.wait_for(process.wait(), timeout=5)
        raise
    return process


async def stop_ts_server(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return

    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        with suppress(Exception):
            await asyncio.wait_for(process.wait(), timeout=2)
