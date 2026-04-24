"""Unit tests for the client-initiated keep-alive ping in LighterWebSocket.

Background: Lighter drops connections at ~117s if the client never
pings proactively (passive pong response is insufficient). These tests
exercise the keep-alive task in isolation — no network I/O, the
websocket object is a minimal mock.

Coverage:
  1. _keepalive_loop sends one ping per interval (time-accelerated)
  2. _stop_keepalive_task cancels cleanly and is idempotent
  3. disconnect() cancels the keep-alive task even without a live WS
  4. pong message updates stats
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, List

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Patch the keep-alive interval BEFORE importing so the test runs fast.
# We cannot patch it after import because the module-level constant
# is captured in _keepalive_loop's closure.
import gateways.lighter_ws as lws  # noqa: E402

lws._KEEPALIVE_INTERVAL_SEC = 0.02  # 20ms per ping in tests

from gateways.lighter_ws import LighterWebSocket  # noqa: E402


class _FakeWs:
    """Minimal async WS double that records every outgoing message."""

    def __init__(self):
        self.sent: List[str] = []
        self.closed = False

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("fake ws closed")
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


def _make_ws_client() -> LighterWebSocket:
    # testnet URL keeps us from accidentally hitting mainnet if something
    # is mis-wired in the test — though nothing here makes network calls.
    return LighterWebSocket(testnet=True)


def _count_pings(fake: _FakeWs) -> int:
    n = 0
    for msg in fake.sent:
        try:
            if json.loads(msg).get("type") == "ping":
                n += 1
        except json.JSONDecodeError:
            continue
    return n


# --------------------------------------------------------------
# tests
# --------------------------------------------------------------


def test_keepalive_loop_sends_pings_on_interval():
    """Start the keep-alive loop against a fake ws, wait a few
    intervals, confirm ≥2 pings sent and stats tracked.
    """

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        # Let 3 intervals elapse (0.02s × 3 = 60ms + headroom)
        await asyncio.sleep(0.085)
        await client._stop_keepalive_task()
        return client, fake

    client, fake = asyncio.run(scenario())
    pings = _count_pings(fake)
    assert pings >= 2, f"expected ≥2 pings, got {pings} (sent={fake.sent})"
    assert client._ping_sent == pings
    assert client._last_ping_ts_ms is not None


def test_stop_keepalive_task_is_idempotent():
    """Double-stop must not raise; starting a fresh task after stop works."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(0.03)
        await client._stop_keepalive_task()
        # Second stop — no-op, no exception
        await client._stop_keepalive_task()
        # Task handle cleared
        assert client._keepalive_task is None
        # Can start a new one
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(0.03)
        await client._stop_keepalive_task()
        return client

    client = asyncio.run(scenario())
    # Two cycles of ≥1 ping each => ≥2 total
    assert client._ping_sent >= 2


def test_disconnect_cancels_keepalive_even_without_ws():
    """disconnect() must clean up the keep-alive task even if
    _direct_ws is already None (the WS loop can exit before we call
    disconnect)."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._running = True
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(0.03)
        # Simulate the run-loop exiting: clear ws handle before disconnect()
        client._direct_ws = None
        await client.disconnect()
        assert client._keepalive_task is None
        assert client._running is False
        return client

    client = asyncio.run(scenario())
    assert client._ping_sent >= 1


def test_keepalive_self_exits_when_ws_closes():
    """If the fake ws raises on send (connection dead), the loop exits
    silently so the outer reconnect path can take over."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(0.03)  # at least one ping goes out
        fake.closed = True  # next send will raise ConnectionError
        await asyncio.sleep(0.05)  # give loop time to try and fail
        # Task should have exited on its own (not cancelled)
        task = client._keepalive_task
        assert task is not None
        assert task.done(), "keep-alive task should exit when send fails"
        assert not task.cancelled()
        exc = task.exception()
        assert exc is None, f"expected clean exit, got exception: {exc!r}"

    asyncio.run(scenario())


def test_pong_message_updates_stats():
    """_handle_message must increment pong_received on {"type":"pong"}."""

    async def scenario():
        client = _make_ws_client()
        await client._handle_message({"type": "pong"})
        await client._handle_message({"type": "pong"})
        return client

    client = asyncio.run(scenario())
    stats = client.get_message_stats()
    assert stats["pong_received"] == 2
    assert stats["last_pong_ts_ms"] is not None
    assert stats["msg_count_by_type"].get("pong") == 2


def test_inbound_ping_still_gets_pong_reply():
    """Defensive double-coverage: server-initiated ping still triggers
    a pong reply even though we now drive our own ping schedule."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        await client._handle_message({"type": "ping"})
        return fake

    fake = asyncio.run(scenario())
    assert len(fake.sent) == 1
    assert json.loads(fake.sent[0]) == {"type": "pong"}
