"""Unit tests for observe.depth_aggregation.aggregate_trades_window
and gateways.lighter_ws._on_trade trade-tape parsing.

Covers:

1. Trades within the [start, end] window are aggregated; per-side
   tallies + VWAP + first/last timestamps reflect every counted
   trade.
2. Trades outside the window are excluded entirely (count, vols, VWAP).
3. An empty/None trade list returns count=0 with no exception.
4. ``LighterWebSocket._on_trade`` ingests a payload, normalizes
   each entry, and the in-memory tape returns the most recent
   subset via ``get_recent_trades``.
5. The 5-minute retention prune drops trades older than the
   ``_TRADE_RETENTION_MS`` cutoff — verifies the memory-leak guard.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import gateways.lighter_ws as lws  # noqa: E402

from gateways.lighter_ws import LighterWebSocket  # noqa: E402
from observe.depth_aggregation import aggregate_trades_window  # noqa: E402


def _D(s):
    return Decimal(s)


def _trade(ts_ms, price, size, side="buy", trade_id=None):
    return {
        "ts_ms": int(ts_ms),
        "price": _D(price),
        "size": _D(size),
        "usd_amount": _D(price) * _D(size),
        "side": side,
        "type": "trade",
        "trade_id": trade_id,
    }


# -------------------------------------------------------------
# aggregate_trades_window
# -------------------------------------------------------------


def test_window_aggregates_buy_and_sell_with_vwap():
    """
    Two buys (1 base @ 100, 2 base @ 102) + one sell (1 base @ 101)
    inside the window → count=3, VWAP = (100*1 + 102*2 + 101*1) / 4
    = (100 + 204 + 101) / 4 = 405 / 4 = 101.25.
    """
    start, end = 1_000_000, 1_060_000
    trades = [
        _trade(1_005_000, "100", "1", side="buy"),
        _trade(1_030_000, "102", "2", side="buy"),
        _trade(1_055_000, "101", "1", side="sell"),
    ]
    out = aggregate_trades_window(trades, start, end)
    assert out["count"] == 3
    assert out["buy_count"] == 2
    assert out["sell_count"] == 1
    assert out["buy_volume_usdc"] == _D("100") * _D("1") + _D("102") * _D("2")
    assert out["sell_volume_usdc"] == _D("101") * _D("1")
    assert out["vwap"] == _D("101.25")
    assert out["first_trade_ts_ms"] == 1_005_000
    assert out["last_trade_ts_ms"] == 1_055_000


def test_trades_outside_window_excluded():
    start, end = 2_000_000, 2_060_000
    trades = [
        _trade(1_999_999, "100", "1", side="buy"),  # before window
        _trade(2_010_000, "100", "5", side="buy"),  # in
        _trade(2_060_001, "100", "1", side="sell"),  # after window
    ]
    out = aggregate_trades_window(trades, start, end)
    assert out["count"] == 1
    assert out["buy_count"] == 1
    assert out["sell_count"] == 0
    assert out["buy_volume_usdc"] == _D("500")
    # VWAP only sees the in-window trade.
    assert out["vwap"] == _D("100")
    assert out["first_trade_ts_ms"] == 2_010_000
    assert out["last_trade_ts_ms"] == 2_010_000


def test_empty_or_none_trades_returns_zero_safely():
    for trades in ([], iter([]), (t for t in [])):  # iterables
        out = aggregate_trades_window(trades, 0, 1_000_000)
        assert out["count"] == 0
        assert out["buy_count"] == 0
        assert out["sell_count"] == 0
        assert out["buy_volume_usdc"] == _D("0")
        assert out["sell_volume_usdc"] == _D("0")
        assert out["vwap"] is None
        assert out["first_trade_ts_ms"] is None
        assert out["last_trade_ts_ms"] is None


# -------------------------------------------------------------
# LighterWebSocket._on_trade integration
# -------------------------------------------------------------


def _make_ws_client() -> LighterWebSocket:
    return LighterWebSocket(testnet=True)


def test_on_trade_parses_payload_and_buffers():
    """
    Mock a Lighter trade WS message → confirm the in-memory tape has
    the expected normalized fields and side classification.
    """
    import time as _time

    client = _make_ws_client()
    base_ts = int(_time.time() * 1000)
    payload = {
        "type": "update/trade",
        "channel": "trade/162",
        "trades": [
            {
                "trade_id": 1001,
                "market_id": 162,
                "size": "5",
                "price": "100.0",
                "usd_amount": "500.0",
                "is_maker_ask": True,  # taker bought
                "timestamp": base_ts - 1_000,
                "type": "trade",
            },
            {
                "trade_id": 1002,
                "market_id": 162,
                "size": "1",
                "price": "100.1",
                "usd_amount": "100.1",
                "is_maker_ask": False,  # taker sold
                "timestamp": base_ts,
                "type": "trade",
            },
        ],
    }
    asyncio.run(client._handle_message(payload))
    tape = client.get_recent_trades(162)
    assert len(tape) == 2
    assert tape[0]["side"] == "buy"
    assert tape[0]["price"] == _D("100.0")
    assert tape[0]["size"] == _D("5")
    assert tape[0]["usd_amount"] == _D("500.0")
    assert tape[1]["side"] == "sell"
    assert tape[1]["trade_id"] == 1002


def test_recent_trades_retention_prunes_old_entries(monkeypatch):
    """
    Manually push a trade older than the retention cutoff and verify
    a fresh ingest triggers the prune.
    """
    client = _make_ws_client()
    # Pre-seed with an ancient trade
    ancient = {
        "ts_ms": 1,  # epoch-zero, definitely older than 5 min
        "price": _D("99"),
        "size": _D("1"),
        "usd_amount": _D("99"),
        "side": "buy",
        "type": "trade",
        "trade_id": 0,
    }
    client._recent_trades_by_market[162] = [ancient]

    # Now push a fresh trade — should evict the ancient one.
    import time as _time
    fresh_ts = int(_time.time() * 1000)
    fresh_payload = {
        "type": "update/trade",
        "channel": "trade/162",
        "trades": [
            {
                "trade_id": 99,
                "market_id": 162,
                "size": "1",
                "price": "100",
                "usd_amount": "100",
                "is_maker_ask": True,
                "timestamp": fresh_ts,
                "type": "trade",
            }
        ],
    }
    asyncio.run(client._handle_message(fresh_payload))
    tape = client.get_recent_trades(162)
    # Only the fresh trade survives — ancient is pruned.
    assert len(tape) == 1
    assert tape[0]["trade_id"] == 99


def test_trade_subscription_error_is_isolated():
    """A server error for trade/{mi} must not propagate; market_stats
    keeps working."""
    client = _make_ws_client()
    err_payload = {
        "type": "error",
        "channel": "trade/162",
        "message": "channel not enabled",
    }
    # Must not raise.
    asyncio.run(client._handle_message(err_payload))
    stats = client.get_message_stats()
    assert 162 in stats["trade_subscription_failed"]
    # And ingesting market_stats afterwards still works.
    asyncio.run(client._handle_message({
        "type": "update/market_stats",
        "channel": "market_stats/162",
        "market_stats": {
            "market_id": 162,
            "mark_price": "150",
        },
    }))
    assert client.get_latest_mid(162) == _D("150")
