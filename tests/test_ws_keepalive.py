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
  5. Phase 1.0.3 stale-check / lifetime-refresh / fail-counter coverage
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
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
# Phase 1.0.3 active health probes — accelerate so tests stay fast.
lws._STALE_CHECK_INTERVAL_SEC = 0.01
lws._MAX_MSG_GAP_SEC = 0.05
lws._MAX_CONNECTION_LIFETIME_SEC = 0.05
# Keep failure budget at default (3) — tests target it exactly.

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


class _FakeWsControlledFail:
    """Fake WS where the next ``fail_remaining`` sends raise."""

    def __init__(self, fail_remaining: int = 0):
        self.sent: List[str] = []
        self.fail_remaining = fail_remaining
        self.closed = False

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("fake ws closed")
        if self.fail_remaining > 0:
            self.fail_remaining -= 1
            raise ConnectionError("transient send failure")
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
    """Phase 1.0.3: the loop tolerates up to ``_MAX_KEEPALIVE_FAIL_COUNT``
    consecutive send failures before bailing. Once the budget is
    exhausted it exits silently so the outer reconnect path can take
    over."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        await asyncio.sleep(0.03)  # at least one ping goes out
        fake.closed = True  # subsequent sends will raise ConnectionError
        # Need ≥ _MAX_KEEPALIVE_FAIL_COUNT (3) more interval ticks
        # at 0.02s each plus headroom for scheduling jitter.
        await asyncio.sleep(0.12)
        task = client._keepalive_task
        assert task is not None
        assert task.done(), "keep-alive task should exit after fail budget"
        assert not task.cancelled()
        exc = task.exception()
        assert exc is None, f"expected clean exit, got exception: {exc!r}"
        # Counter should equal the budget at exit.
        assert client._keepalive_fail_count >= lws._MAX_KEEPALIVE_FAIL_COUNT

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


# --------------------------------------------------------------
# Phase 1.0.3: stale-check / lifetime / failure-budget coverage
# --------------------------------------------------------------


def test_stale_check_triggers_reconnect_after_60s_silence():
    """If no inbound msg for ``_MAX_MSG_GAP_SEC`` (test-shrunk to
    50ms), stale-check closes the WS and records reason='stale'.
    """

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        # Force the last-msg timestamp far enough into the past
        # that the very first stale-check tick (10ms) sees a gap
        # >50ms. Set it 1s ago for headroom.
        client._last_msg_ts_ms = int((time.time() - 1.0) * 1000)
        # Don't trip the lifetime check before the stale check by
        # giving the connection a fresh start timestamp.
        client._connection_started_ts_ms = int(time.time() * 1000)
        client._stale_check_task = asyncio.create_task(client._stale_check_loop())
        # Wait long enough for at least 2 stale-check ticks (20ms)
        await asyncio.sleep(0.05)
        await client._stop_stale_check_task()
        return client, fake

    client, fake = asyncio.run(scenario())
    assert fake.closed is True, "stale-check must close the WS"
    assert client._last_disconnect_reason == "stale"


def test_lifetime_refresh_triggers_at_1hr():
    """After ``_MAX_CONNECTION_LIFETIME_SEC`` (test-shrunk to 50ms),
    stale-check closes the WS even when traffic is fresh; reason='lifetime'.
    """

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        # Fresh msg so the stale trigger doesn't fire first.
        client._last_msg_ts_ms = int(time.time() * 1000)
        # Connection looks 1s old already — well past the shrunk
        # 50ms lifetime, so lifetime should win on the first tick.
        client._connection_started_ts_ms = int((time.time() - 1.0) * 1000)
        client._stale_check_task = asyncio.create_task(client._stale_check_loop())
        await asyncio.sleep(0.05)
        await client._stop_stale_check_task()
        return client, fake

    client, fake = asyncio.run(scenario())
    assert fake.closed is True, "lifetime cap must close the WS"
    assert client._last_disconnect_reason == "lifetime"


def test_keepalive_fail_counter_resets_on_success():
    """Two consecutive send failures bump the counter; the first
    success after that snaps it back to 0 (transient errors don't
    accumulate forever)."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWsControlledFail(fail_remaining=2)
        client._direct_ws = fake  # type: ignore[assignment]
        client._keepalive_task = asyncio.create_task(client._keepalive_loop())
        # 4 ticks (~80ms) ⇒ fail, fail, succeed, succeed
        await asyncio.sleep(0.09)
        await client._stop_keepalive_task()
        return client, fake

    client, fake = asyncio.run(scenario())
    # Counter must have reset on the success path.
    assert client._keepalive_fail_count == 0, (
        f"fail_count should reset on success; got {client._keepalive_fail_count}"
    )
    # At least one ping made it through after the failures.
    assert client._ping_sent >= 1
    assert len(fake.sent) >= 1


def test_last_disconnect_reason_recorded():
    """All three trigger paths set ``_last_disconnect_reason`` to
    the right tag. Run them as independent scenarios so state
    carryover isn't a confound."""

    # 1. stale
    async def stale_scenario():
        c = _make_ws_client()
        fake = _FakeWs()
        c._direct_ws = fake  # type: ignore[assignment]
        c._last_msg_ts_ms = int((time.time() - 1.0) * 1000)
        c._connection_started_ts_ms = int(time.time() * 1000)
        c._stale_check_task = asyncio.create_task(c._stale_check_loop())
        await asyncio.sleep(0.05)
        await c._stop_stale_check_task()
        return c

    c_stale = asyncio.run(stale_scenario())
    assert c_stale._last_disconnect_reason == "stale"

    # 2. lifetime
    async def lifetime_scenario():
        c = _make_ws_client()
        fake = _FakeWs()
        c._direct_ws = fake  # type: ignore[assignment]
        c._last_msg_ts_ms = int(time.time() * 1000)
        c._connection_started_ts_ms = int((time.time() - 1.0) * 1000)
        c._stale_check_task = asyncio.create_task(c._stale_check_loop())
        await asyncio.sleep(0.05)
        await c._stop_stale_check_task()
        return c

    c_life = asyncio.run(lifetime_scenario())
    assert c_life._last_disconnect_reason == "lifetime"

    # 3. keepalive_fail — manually pre-load the budget so the
    # stale-check sees an exhausted counter on first tick.
    async def keepalive_fail_scenario():
        c = _make_ws_client()
        fake = _FakeWs()
        c._direct_ws = fake  # type: ignore[assignment]
        c._last_msg_ts_ms = int(time.time() * 1000)
        c._connection_started_ts_ms = int(time.time() * 1000)
        c._keepalive_fail_count = lws._MAX_KEEPALIVE_FAIL_COUNT
        c._stale_check_task = asyncio.create_task(c._stale_check_loop())
        await asyncio.sleep(0.05)
        await c._stop_stale_check_task()
        return c, fake

    c_kf, fake_kf = asyncio.run(keepalive_fail_scenario())
    assert c_kf._last_disconnect_reason == "keepalive_fail"
    assert fake_kf.closed is True


def test_handle_message_updates_last_msg_ts():
    """Any inbound message — including bookkeeping types like ping/
    pong — bumps ``_last_msg_ts_ms`` so the stale detector measures
    pure liveness rather than data-channel activity."""

    async def scenario():
        client = _make_ws_client()
        fake = _FakeWs()
        client._direct_ws = fake  # type: ignore[assignment]
        # Initially None (no traffic yet)
        assert client._last_msg_ts_ms is None
        # Pong-only path
        before = int(time.time() * 1000)
        await client._handle_message({"type": "pong"})
        after_pong = client._last_msg_ts_ms
        assert after_pong is not None and after_pong >= before
        # Ping path (also fans a pong reply but still updates ts)
        await asyncio.sleep(0.005)
        await client._handle_message({"type": "ping"})
        after_ping = client._last_msg_ts_ms
        assert after_ping is not None and after_ping >= after_pong
        # Arbitrary update message
        await asyncio.sleep(0.005)
        await client._handle_message(
            {"type": "update/market_stats", "channel": "market_stats/1", "market_stats": {}}
        )
        after_update = client._last_msg_ts_ms
        assert after_update is not None and after_update >= after_ping
        return client

    asyncio.run(scenario())
