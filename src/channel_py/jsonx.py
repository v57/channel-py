from __future__ import annotations

from typing import Any

import orjson


def dumps(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


def dumpb(value: Any) -> bytes:
    return orjson.dumps(value)


def loads(value: str | bytes | bytearray | memoryview) -> Any:
    return orjson.loads(value)
