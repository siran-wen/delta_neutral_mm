"""Unit test for the "REST-book + WS mark" merge logic in LighterObserver.

The observer pulls order-book depth via REST (``LighterGateway.get_orderbook``)
and mid-price via WS (``LighterWebSocket.get_latest_mid`` sourced from
``market_stats``). Three scenarios matter:

1. REST book present + WS mark present → mid comes from WS (src='ws'),
   depths from REST.
2. REST book present + WS mark absent → mid falls back to REST BBO midpoint
   (src='rest').
3. REST book missing → best_bid/ask/mid all None; depth counts are 0.

We don't hit the network; both the gateway and the WS client are stubbed.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from observe.lighter_observer import LighterObserver, ObservationConfig  # noqa: E402
from observe.session_clock import MarketSchedule  # noqa: E402


# -------------------------------------------------------------
# stubs
# -------------------------------------------------------------


class _StubGateway:
    def __init__(self, books: Dict[str, Optional[Dict[str, Any]]]):
        self._books = books

    async def get_orderbook(self, symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
        return self._books.get(symbol)

    def get_market_index(self, symbol: str) -> Optional[int]:  # unused but referenced
        return None

    def get_symbol(self, market_index: int) -> Optional[str]:  # unused but referenced
        return None

    @property
    def signer_client(self) -> None:
        return None


class _StubWs:
    def __init__(
        self,
        mids: Dict[int, Optional[Decimal]],
        stats: Optional[Dict[int, Dict[str, Any]]] = None,
    ):
        self._mids = mids
        self._stats = stats or {}

    def get_latest_mid(self, market_index: int) -> Optional[Decimal]:
        return self._mids.get(market_index)

    def get_market_stats(self, market_index: int) -> Optional[Dict[str, Any]]:
        return self._stats.get(market_index)

    def get_message_stats(self) -> Dict[str, Any]:
        return {
            "msg_count_by_type": {},
            "connect_count": 1,
            "subscribed_markets": [],
            "trade_subscription_failed": [],
        }

    def get_recent_trades(
        self, market_index: int, since_ts_ms: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        # No trades — exercises the empty-tape branch of the snapshot.
        return []

    def get_recent_latency_stats(self) -> Optional[Dict[str, Any]]:
        # No samples → observer omits the field.
        return None


def _make_observer(
    gateway: _StubGateway, ws: _StubWs, symbols: List[str], market_map: Dict[str, int]
) -> LighterObserver:
    cfg = ObservationConfig(
        symbols=symbols,
        duration_sec=0.0,
        snapshot_interval_sec=60.0,
        log_book_every_sec=5.0,
        output_dir=Path("."),
        include_account=False,
        schedule=MarketSchedule.CRYPTO_24X7,
    )
    obs = LighterObserver(gateway=gateway, ws=ws, config=cfg)  # type: ignore[arg-type]
    obs._symbol_markets = dict(market_map)
    # _emit_snapshot writes to the JSONL file handle; leave it None
    # so writes become no-ops (the handler already tolerates this).
    obs._message_log_fh = None
    return obs


# -------------------------------------------------------------
# tests
# -------------------------------------------------------------


def test_snapshot_uses_ws_mid_when_ws_mark_is_present():
    # REST depth-5 book
    book = {
        "symbol": "SAMSUNGUSD",
        "market_index": 162,
        "bids": [[Decimal("100.0"), Decimal("3")], [Decimal("99.9"), Decimal("2")]],
        "asks": [[Decimal("101.0"), Decimal("1")], [Decimal("101.1"), Decimal("4")]],
    }
    gw = _StubGateway({"SAMSUNGUSD": book})
    ws_mark = Decimal("100.42")
    ws = _StubWs(mids={162: ws_mark})
    obs = _make_observer(gw, ws, ["SAMSUNGUSD"], {"SAMSUNGUSD": 162})

    captured: List[Dict[str, Any]] = []
    obs._jsonl_write = captured.append  # type: ignore[assignment]

    asyncio.run(obs._emit_snapshot(now=1_000_000.0))

    snapshots = [p for p in captured if p.get("event") == "snapshot"]
    assert len(snapshots) == 1
    books = snapshots[0]["books"]["SAMSUNGUSD"]
    assert books["best_bid"] == "100.0"
    assert books["best_ask"] == "101.0"
    assert books["mid"] == str(ws_mark)
    assert books["mid_source"] == "ws"
    assert books["bid_depth"] == 2
    assert books["ask_depth"] == 2


def test_snapshot_falls_back_to_rest_mid_when_ws_mark_absent():
    book = {
        "symbol": "SAMSUNGUSD",
        "market_index": 162,
        "bids": [[Decimal("100.0"), Decimal("1")]],
        "asks": [[Decimal("102.0"), Decimal("1")]],
    }
    gw = _StubGateway({"SAMSUNGUSD": book})
    ws = _StubWs(mids={})  # no WS mid available
    obs = _make_observer(gw, ws, ["SAMSUNGUSD"], {"SAMSUNGUSD": 162})

    captured: List[Dict[str, Any]] = []
    obs._jsonl_write = captured.append  # type: ignore[assignment]

    asyncio.run(obs._emit_snapshot(now=1_000_000.0))

    books = [p for p in captured if p.get("event") == "snapshot"][0]["books"]["SAMSUNGUSD"]
    assert books["best_bid"] == "100.0"
    assert books["best_ask"] == "102.0"
    assert books["mid"] == "101.0"  # (Decimal('100.0') + Decimal('102.0')) / 2
    assert books["mid_source"] == "rest"


def test_snapshot_handles_rest_book_missing_gracefully():
    gw = _StubGateway({"SAMSUNGUSD": None})
    ws = _StubWs(mids={162: Decimal("100")})
    obs = _make_observer(gw, ws, ["SAMSUNGUSD"], {"SAMSUNGUSD": 162})

    captured: List[Dict[str, Any]] = []
    obs._jsonl_write = captured.append  # type: ignore[assignment]

    asyncio.run(obs._emit_snapshot(now=1_000_000.0))

    books = [p for p in captured if p.get("event") == "snapshot"][0]["books"]["SAMSUNGUSD"]
    assert books["best_bid"] is None
    assert books["best_ask"] is None
    # With no REST book, WS mid alone is informational but the observer
    # still records it (useful to know mark price even when depth is gone).
    # WS mark present but no REST depth: observer still records the WS mid,
    # but mid_source stays "ws" only if we actually used the ws_mid value.
    # With book=None we skip rest_mid, so mid == ws_mid and source == "ws".
    assert books["mid"] == "100"
    assert books["mid_source"] == "ws"
    assert books["bid_depth"] == 0
    assert books["ask_depth"] == 0


def test_fetch_rest_book_swallows_gateway_exception():
    class _RaisingGateway(_StubGateway):
        async def get_orderbook(self, symbol, limit=5):
            raise RuntimeError("boom")

    gw = _RaisingGateway({"SAMSUNGUSD": None})
    ws = _StubWs(mids={})
    obs = _make_observer(gw, ws, ["SAMSUNGUSD"], {"SAMSUNGUSD": 162})

    result = asyncio.run(obs._fetch_rest_book("SAMSUNGUSD"))
    assert result is None
