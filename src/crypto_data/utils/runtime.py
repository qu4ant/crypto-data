"""
Runtime helpers for sync public APIs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any


def run_async_from_sync(coro: Coroutine[Any, Any, Any], caller_name: str) -> Any:
    """
    Run a coroutine from a synchronous API with explicit event-loop safety.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # If we're already inside a running event loop, this coroutine cannot be
    # executed here. Close it explicitly to avoid "coroutine was never awaited"
    # warnings before raising the user-facing error.
    coro.close()
    raise RuntimeError(
        f"{caller_name}() cannot be called from an active event loop. "
        "Run it in a synchronous context."
    )
