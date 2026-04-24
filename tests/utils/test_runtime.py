"""
Tests for runtime async/sync helpers.
"""

import pytest

from crypto_data.utils.runtime import run_async_from_sync


def test_run_async_from_sync_executes_coroutine():
    """Runs coroutine in sync context and returns result."""

    async def _sample() -> int:
        return 42

    assert run_async_from_sync(_sample(), "demo") == 42


@pytest.mark.asyncio
async def test_run_async_from_sync_closes_coro_when_loop_active():
    """Closes coroutine and raises when called inside active event loop."""

    async def _sample() -> int:
        return 42

    coro = _sample()
    with pytest.raises(RuntimeError, match="cannot be called from an active event loop"):
        run_async_from_sync(coro, "demo")

    # Closed coroutine should have no frame attached.
    assert coro.cr_frame is None
