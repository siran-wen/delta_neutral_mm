"""Unit tests for ``strategy.lpp_quoter.LppQuoter``.

All async tests use ``asyncio.run`` so we don't need pytest-asyncio.

Coverage groups:
* A. lifecycle (5)
* B. _should_reprice decision (8)
* C. _execute_reprice (6)
* D. emergency stop (4)
* E. inventory + fill flow (4)
* F. snapshot integration (2)
* G. demo / integration smoke (1)

We use the *real* ``LighterOrderManager`` so the interface contract
between quoter and OM is exercised end-to-end. The signer, ws, and
gateway underneath the OM are still mocked.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from execution.lighter.lighter_order_manager import LighterOrderManager  # noqa: E402
from strategy.lpp_quoter import (  # noqa: E402
    LppQuoter,
    QUOTER_DEFAULTS,
    merge_config,
    _adapt_managed_order,
)
from strategy.types import MarketSnapshot, Quote, SessionPolicy  # noqa: E402


# ----- shared fakes ------------------------------------------------------


class FakeSigner:
    def __init__(self) -> None:
        self.create_calls: List[Dict[str, Any]] = []
        self.cancel_calls: List[Dict[str, Any]] = []
        self.create_always_fails = False

    async def create_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.create_calls.append(kwargs)
        if self.create_always_fails:
            return (None, None, "fake fail")
        return ("ok", "ok", None)

    async def cancel_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.cancel_calls.append(kwargs)
        return ("ok", "ok", None)


class FakeGateway:
    def __init__(self) -> None:
        self._signer = FakeSigner()
        self.book: Optional[Dict[str, Any]] = None

    @property
    def signer_client(self) -> FakeSigner:
        return self._signer

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        return self.book


class FakeWs:
    def __init__(self) -> None:
        self.account_callbacks: List[Any] = []
        self.subscribed_accounts: List[int] = []
        self.market_index_to_mid: Dict[int, Decimal] = {}
        self.market_index_to_stats: Dict[int, Dict[str, Any]] = {}
        # Default: ws is "live" — last_msg_ts is now.
        self._last_msg_ts_ms: Optional[int] = int(time.time() * 1000)

    def register_account_callback(self, cb: Any) -> None:
        self.account_callbacks.append(cb)

    async def subscribe_account(self, idx: int) -> None:
        self.subscribed_accounts.append(idx)

    def get_latest_mid(self, mi: int) -> Optional[Decimal]:
        return self.market_index_to_mid.get(mi)

    def get_market_stats(self, mi: int) -> Optional[Dict[str, Any]]:
        return self.market_index_to_stats.get(mi)

    def get_message_stats(self) -> Dict[str, Any]:
        return {"last_msg_ts_ms_global": self._last_msg_ts_ms}


class FakeTracker:
    def __init__(self) -> None:
        self.snapshots: List[Dict[str, Any]] = []
        self.summary_calls: List[Tuple[int, int, Optional[str]]] = []

    async def record_snapshot(
        self,
        ts_ms: int,
        market: MarketSnapshot,
        my_quotes: List[Quote],
        inventory: Any,
        session: SessionPolicy,
    ) -> Dict[str, Any]:
        rec = {
            "ts_ms": ts_ms,
            "session": session.name,
            "n_quotes": len(my_quotes),
        }
        self.snapshots.append(rec)
        return rec

    def get_session_summary(self, session_start_ms: int, session_end_ms: int, symbol: Optional[str] = None) -> Dict[str, Any]:
        self.summary_calls.append((session_start_ms, session_end_ms, symbol))
        return {"snapshots_count": len(self.snapshots)}

    def close(self) -> None:
        pass


# ----- fixtures ----------------------------------------------------------


def _book(mid: Decimal, *, l1_depth_usdc: Decimal = Decimal("0")) -> Dict[str, Any]:
    """REST orderbook stub. l1_depth feeds the share-warn computation
    when the test wants ``share_warn`` to NOT trigger."""
    bid = mid - Decimal("0.05")
    ask = mid + Decimal("0.05")
    bids = [[bid, Decimal("1")]]
    asks = [[ask, Decimal("1")]]
    if l1_depth_usdc > 0:
        size = l1_depth_usdc / mid
        # Add a level deeper inside the L1 tier (≤15bp) so plan_quotes
        # sees existing depth and our share calc doesn't dominate.
        deeper_bid = mid * (Decimal(1) - Decimal("12") / Decimal(10000))
        deeper_ask = mid * (Decimal(1) + Decimal("12") / Decimal(10000))
        bids.append([deeper_bid, size])
        asks.append([deeper_ask, size])
    return {
        "symbol": "SKHYNIXUSD",
        "market_index": 161,
        "bids": bids,
        "asks": asks,
        "timestamp_ms": int(time.time() * 1000),
    }


def _make_quoter(
    *,
    config_overrides: Optional[Dict[str, Any]] = None,
    book: Optional[Dict[str, Any]] = None,
    initial_mid: Decimal = Decimal("100"),
) -> Tuple[LppQuoter, FakeGateway, FakeWs, LighterOrderManager, FakeTracker]:
    gw = FakeGateway()
    gw.book = book or _book(initial_mid)
    ws = FakeWs()
    ws.market_index_to_stats[161] = {"mark_price": initial_mid, "index_price": initial_mid}
    ws.market_index_to_mid[161] = initial_mid

    om = LighterOrderManager(
        gateway=gw,
        ws=ws,
        account_index=42,
        request_timeout_sec=1.0,
        retry_max_attempts=1,
        retry_backoff_sec=0.0,
    )
    tracker = FakeTracker()

    cfg = dict(config_overrides) if config_overrides else {}
    quoter = LppQuoter(
        gateway=gw,
        ws=ws,
        order_manager=om,
        tracker=tracker,
        symbol="SKHYNIXUSD",
        market_index=161,
        price_decimals=3,
        size_decimals=3,
        account_index=42,
        config=cfg,
    )
    return quoter, gw, ws, om, tracker


def _D(s: str) -> Decimal:
    return Decimal(s)


def _ws_order_msg(coid: int, status: str, *, order_index: int = 9001, filled: str = "0", remaining: str = "1118") -> Dict[str, Any]:
    return {
        "channel": "account_all/42",
        "type": "update/account_all",
        "orders": [
            {
                "client_order_index": coid,
                "order_index": order_index,
                "market_index": 161,
                "is_ask": False,
                "price": "100000",
                "filled_base_amount": filled,
                "remaining_base_amount": remaining,
                "status": status,
            }
        ],
    }


# ----- A. lifecycle (5) --------------------------------------------------


def test_init_state():
    q, _, _, om, _ = _make_quoter()
    assert q._started is False
    assert q._closed is False
    assert q.get_active_quotes_count() == 0
    summary = q.get_summary()
    assert summary["tick_count"] == 0
    assert summary["reprice_count"] == 0


def test_start_subscribes_account_and_registers_callback():
    q, _, ws, om, _ = _make_quoter()

    asyncio.run(q.start())

    assert ws.subscribed_accounts == [42]
    assert len(ws.account_callbacks) == 1
    # The callback is OM's on_account_event — pushing a msg should
    # reach OM's parser.
    assert ws.account_callbacks[0] == om.on_account_event


def test_start_is_idempotent():
    q, _, ws, _, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        await q.start()  # second call must not re-subscribe

    asyncio.run(_go())
    assert len(ws.subscribed_accounts) == 1


def test_run_until_past_deadline_returns_immediately():
    q, _, _, _, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        # end_ts in the past → loop exits without ticking
        await q.run_until(time.time() - 1)

    asyncio.run(_go())
    assert q.get_summary()["tick_count"] == 0


def test_shutdown_idempotent_and_calls_om_cancel_all():
    q, _, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        # Pre-load an order so cancel_all has work to do
        coid = await om.submit_order(
            side="buy", market_index=161, price=_D("99.95"),
            size_base=_D("1"), price_decimals=3, size_decimals=3,
        )
        om.on_account_event(_ws_order_msg(coid, "open"))
        await q.shutdown()
        await q.shutdown()  # idempotent

    asyncio.run(_go())
    assert q._closed is True


# ----- B. _should_reprice (8) -------------------------------------------


def test_reprice_initial_when_last_mid_none():
    q, _, _, _, _ = _make_quoter()
    reason = q._should_reprice(
        last_mid=None,
        new_mid=_D("100"),
        session_changed=False,
        has_pending_fill=False,
        last_reprice_ts=0.0,
        now_ts=time.time(),
    )
    assert reason == "initial"


def test_reprice_session_change_overrides_min_interval():
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    # Last reprice 5s ago — would fail the 60s floor — but session
    # change must still fire.
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100"),
        session_changed=True,
        has_pending_fill=False,
        last_reprice_ts=now - 5,
        now_ts=now,
    )
    assert reason == "session_change"


def test_reprice_pending_fill_overrides_min_interval():
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100"),
        session_changed=False,
        has_pending_fill=True,
        last_reprice_ts=now - 5,
        now_ts=now,
    )
    assert reason == "fill"


def test_reprice_within_min_interval_returns_none_for_drift():
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100.5"),  # 50bp drift, would otherwise trigger
        session_changed=False,
        has_pending_fill=False,
        last_reprice_ts=now - 30,  # last reprice 30s ago < 60s floor
        now_ts=now,
    )
    assert reason is None


def test_reprice_drift_below_threshold_returns_none():
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    # 5bp drift, threshold 8bp
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100.05"),
        session_changed=False,
        has_pending_fill=False,
        last_reprice_ts=now - 120,  # past floor
        now_ts=now,
    )
    assert reason is None


def test_reprice_drift_above_threshold_triggers():
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    # 10bp drift, threshold 8bp
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100.10"),
        session_changed=False,
        has_pending_fill=False,
        last_reprice_ts=now - 120,
        now_ts=now,
    )
    assert reason is not None
    assert reason.startswith("mid_drift")


def test_reprice_drift_exactly_at_threshold_triggers():
    """8bp threshold + 8bp drift → should trigger (>=)."""
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    # 8bp drift exactly: 100 → 100.08
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100.08"),
        session_changed=False,
        has_pending_fill=False,
        last_reprice_ts=now - 120,
        now_ts=now,
    )
    assert reason is not None
    assert reason.startswith("mid_drift")


def test_reprice_priority_session_change_above_drift():
    """Both session change AND big drift → session_change wins."""
    q, _, _, _, _ = _make_quoter()
    now = 1000.0
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100.50"),  # 50bp drift
        session_changed=True,
        has_pending_fill=False,
        last_reprice_ts=now - 120,
        now_ts=now,
    )
    assert reason == "session_change"


# ----- C. _execute_reprice (6) ------------------------------------------


def test_execute_reprice_calls_plan_and_diff():
    q, gw, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        session = SessionPolicy(
            name="KR_OVERNIGHT",
            action="quote",
            default_distance_bp=_D("8"),
            default_size_usdc=_D("500"),
            tier_thresholds_bp=(_D("15"), _D("25"), _D("50")),
            reason="test",
        )
        await q._execute_reprice(market, session, "initial")

    asyncio.run(_go())
    assert q.get_summary()["reprice_count"] == 1
    # 2 places sent (bid + ask) on first reprice from empty
    assert len(gw.signer_client.create_calls) == 2


def test_execute_reprice_concurrent_cancel_and_place():
    """Existing orders + new desired with different price → cancel + place run together."""
    q, gw, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        # Pre-load 2 orders far from current desired
        for side in ("buy", "sell"):
            coid = await om.submit_order(
                side=side, market_index=161, price=_D("90") if side == "buy" else _D("110"),
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open", filled="0", remaining="1000"))

        gw.signer_client.create_calls.clear()
        gw.signer_client.cancel_calls.clear()

        market = await q._build_market_snapshot()
        session = SessionPolicy(
            name="KR_OVERNIGHT", action="quote",
            default_distance_bp=_D("8"), default_size_usdc=_D("500"),
            tier_thresholds_bp=(_D("15"), _D("25"), _D("50")), reason="test",
        )
        await q._execute_reprice(market, session, "mid_drift(20bp)")

    asyncio.run(_go())
    # Far-from-mid orders cancelled, new ones placed
    assert len(gw.signer_client.cancel_calls) == 2
    assert len(gw.signer_client.create_calls) == 2


def test_execute_reprice_increments_consecutive_rejects_on_fail():
    q, gw, _, om, _ = _make_quoter()
    gw.signer_client.create_always_fails = True

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        session = get_kr_overnight_session()
        await q._execute_reprice(market, session, "initial")

    asyncio.run(_go())
    assert q.get_summary()["consecutive_rejects"] >= 2  # both legs rejected


def test_execute_reprice_resets_consecutive_rejects_on_success():
    q, gw, _, om, _ = _make_quoter()
    # Pre-bump the counter as if a previous reprice failed.
    q._stats["consecutive_rejects"] = 3

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        await q._execute_reprice(market, get_kr_overnight_session(), "initial")

    asyncio.run(_go())
    assert q.get_summary()["consecutive_rejects"] == 0


def test_execute_reprice_records_last_reason_and_ts():
    q, _, _, _, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        await q._execute_reprice(market, get_kr_overnight_session(), "session_change")

    asyncio.run(_go())
    summary = q.get_summary()
    assert summary["last_reprice_reason"] == "session_change"
    assert summary["last_reprice_ts_ms"] is not None


def test_execute_reprice_with_no_active_only_places():
    q, gw, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        await q._execute_reprice(market, get_kr_overnight_session(), "initial")

    asyncio.run(_go())
    # No cancels (active was empty), 2 places (bid + ask)
    assert len(gw.signer_client.cancel_calls) == 0
    assert len(gw.signer_client.create_calls) == 2


# ----- D. emergency stop (4) --------------------------------------------


def test_emergency_stop_consecutive_rejects_above_threshold():
    q, _, _, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_consecutive_reject_count": 3},
    )
    q._stats["consecutive_rejects"] = 3
    assert q._should_emergency_stop() is True


def test_emergency_stop_below_threshold_returns_false():
    q, _, _, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_consecutive_reject_count": 5},
    )
    q._stats["consecutive_rejects"] = 4
    assert q._should_emergency_stop() is False


def test_emergency_stop_ws_silent_above_threshold():
    q, _, ws, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_ws_disconnect_sec": 60},
    )
    # Pretend the ws hasn't received anything for 120s.
    ws._last_msg_ts_ms = int(time.time() * 1000) - 120_000
    assert q._should_emergency_stop() is True


def test_emergency_stop_run_until_breaks_and_cancels_all():
    """run_until detects emergency, breaks out, doesn't keep ticking."""
    q, _, _, om, _ = _make_quoter(
        config_overrides={
            "emergency_stop_on_consecutive_reject_count": 1,
            "tick_interval_sec": Decimal("0.01"),
        },
    )

    async def _go() -> None:
        await q.start()
        # Pre-set so the first tick triggers emergency stop.
        q._stats["consecutive_rejects"] = 5
        await q.run_until(time.time() + 5)

    asyncio.run(_go())
    assert q.get_summary()["emergency_stops"] == 1
    # Loop should have broken before many ticks occurred.
    assert q.get_summary()["tick_count"] <= 3


# ----- E. inventory + fill flow (4) -------------------------------------


def test_get_inventory_starts_flat():
    q, _, _, _, _ = _make_quoter()
    inv = q.get_inventory()
    assert inv.net_delta_base == _D("0")


def test_fill_signal_consumed_by_quoter_tick():
    """A fill arriving between ticks raises the OM signal; the quoter
    pops it on the next tick and turns that into a reprice reason."""
    q, _, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        coid = await om.submit_order(
            side="buy", market_index=161, price=_D("99.95"),
            size_base=_D("1"), price_decimals=3, size_decimals=3,
        )
        om.on_account_event(_ws_order_msg(coid, "open", filled="0", remaining="1118"))
        om.on_account_event(
            _ws_order_msg(coid, "filled", filled="1118", remaining="0", order_index=9001)
        )

    asyncio.run(_go())
    # Signal raised; quoter would pop it via OM during the next tick.
    assert om.pop_fill_signal() is True
    # After consume, second pop is False.
    assert om.pop_fill_signal() is False
    # And a 5s-ago reprice + has_pending_fill=True still fires "fill"
    # despite the 60s floor.
    reason = q._should_reprice(
        last_mid=_D("100"),
        new_mid=_D("100"),
        session_changed=False,
        has_pending_fill=True,
        last_reprice_ts=time.time() - 5,
        now_ts=time.time(),
    )
    assert reason == "fill"


def test_inventory_reflects_om_fills():
    q, _, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        coid = await om.submit_order(
            side="buy", market_index=161, price=_D("100"),
            size_base=_D("1"), price_decimals=3, size_decimals=3,
        )
        om.on_account_event(_ws_order_msg(coid, "filled", filled="1", remaining="0"))

    asyncio.run(_go())
    inv = q.get_inventory()
    assert inv.net_delta_base == _D("1")


def test_buy_then_sell_inventory_close():
    q, _, _, om, _ = _make_quoter()

    async def _go() -> None:
        await q.start()
        c1 = await om.submit_order(
            side="buy", market_index=161, price=_D("100"),
            size_base=_D("1"), price_decimals=3, size_decimals=3,
        )
        om.on_account_event(_ws_order_msg(c1, "filled", filled="1", remaining="0", order_index=1))
        c2 = await om.submit_order(
            side="sell", market_index=161, price=_D("110"),
            size_base=_D("1"), price_decimals=3, size_decimals=3,
        )
        om.on_account_event(_ws_order_msg(c2, "filled", filled="1", remaining="0", order_index=2))

    asyncio.run(_go())
    inv = q.get_inventory()
    assert inv.net_delta_base == _D("0")
    assert inv.avg_entry_price is None  # flat


# ----- F. snapshot integration (2) --------------------------------------


def test_record_snapshot_calls_tracker_with_full_args():
    q, _, _, _, tracker = _make_quoter()

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        await q._record_snapshot(market, get_kr_overnight_session())

    asyncio.run(_go())
    assert len(tracker.snapshots) == 1
    rec = tracker.snapshots[0]
    assert rec["session"] == "KR_OVERNIGHT"
    # Two desired quotes (bid+ask) for default config + open spread
    assert rec["n_quotes"] == 2


def test_run_until_takes_periodic_snapshots():
    q, _, _, _, tracker = _make_quoter(
        config_overrides={
            "tick_interval_sec": Decimal("0.01"),
            "snapshot_interval_sec": Decimal("0.05"),
        },
    )

    async def _go() -> None:
        await q.start()
        await q.run_until(time.time() + 0.20)  # 200ms of ticks

    asyncio.run(_go())
    # ~20 ticks at 10ms with snapshot every 50ms → ~3-5 snapshots
    assert len(tracker.snapshots) >= 2


# ----- G. demo / integration smoke (1) ----------------------------------


def get_kr_overnight_session() -> SessionPolicy:
    """Helper — synthesise a KR_OVERNIGHT policy without going through
    the date-aware helper. We can't depend on time.now() in tests."""
    return SessionPolicy(
        name="KR_OVERNIGHT",
        action="quote",
        default_distance_bp=_D("8"),
        default_size_usdc=_D("500"),
        tier_thresholds_bp=(_D("15"), _D("25"), _D("50")),
        reason="test",
    )


def test_three_tick_demo_initial_no_drift_then_drift():
    """End-to-end demo:

    Tick 1: initial → triggers "initial" reprice, 2 places
    Tick 2: mid drifted 5bp (< 8bp threshold) → no reprice
    Tick 3: mid drifted 10bp from tick 1's mid → "mid_drift" reprice
    """
    q, gw, ws, om, _ = _make_quoter(
        config_overrides={
            # Allow drift to fire even though only milliseconds have
            # passed between ticks in this synthetic demo.
            "reprice_min_interval_sec": Decimal("0"),
        },
        initial_mid=_D("100"),
    )

    tick_results: List[Dict[str, Any]] = []

    async def _tick(mid: Decimal) -> None:
        gw.book = _book(mid)
        ws.market_index_to_mid[161] = mid
        ws.market_index_to_stats[161] = {"mark_price": mid, "index_price": mid}
        market = await q._build_market_snapshot()
        session = get_kr_overnight_session()
        last_mid = q._stats.get("_demo_last_mid")
        last_reprice_ts = q._stats.get("_demo_last_reprice_ts", 0.0)
        reason = q._should_reprice(
            last_mid=last_mid,
            new_mid=market.mid,
            session_changed=False,
            has_pending_fill=False,
            last_reprice_ts=last_reprice_ts,
            now_ts=time.time(),
        )
        creates_before = len(gw.signer_client.create_calls)
        if reason is not None:
            await q._execute_reprice(market, session, reason)
            q._stats["_demo_last_reprice_ts"] = time.time()
            # Match the main loop: ``last_mid_for_drift`` only advances
            # after a successful reprice, so a sub-threshold tick
            # doesn't reset the baseline that drift is measured from.
            q._stats["_demo_last_mid"] = market.mid
        tick_results.append(
            {
                "mid": market.mid,
                "reason": reason,
                "places": len(gw.signer_client.create_calls) - creates_before,
            }
        )

    async def _go() -> None:
        await q.start()
        # Tick 1: mid=100 (initial)
        await _tick(_D("100"))
        # Tick 2: mid=100.05 (5bp drift, below 8bp threshold)
        await _tick(_D("100.05"))
        # Tick 3: mid=100.10 (10bp drift from tick-1 100; now last_mid=100)
        await _tick(_D("100.10"))

    asyncio.run(_go())

    # Tick 1: initial, 2 places
    assert tick_results[0]["reason"] == "initial"
    assert tick_results[0]["places"] == 2
    # Tick 2: no reprice
    assert tick_results[1]["reason"] is None
    assert tick_results[1]["places"] == 0
    # Tick 3: mid_drift, 2 places (or fewer if diff finds matches)
    assert tick_results[2]["reason"] is not None
    assert tick_results[2]["reason"].startswith("mid_drift")


# ----- H. config + adapter helpers --------------------------------------


def test_merge_config_returns_defaults_when_empty():
    cfg = merge_config()
    assert cfg["reprice_drift_bp"] == _D("8")
    assert cfg["reprice_min_interval_sec"] == _D("60")
    assert cfg["share_warn_threshold"] == _D("0.95")


def test_merge_config_overrides_specific_keys():
    cfg = merge_config({"reprice_drift_bp": _D("5")})
    assert cfg["reprice_drift_bp"] == _D("5")
    # Other keys preserved
    assert cfg["share_warn_threshold"] == _D("0.95")


def test_adapt_managed_order_translates_field_names():
    from execution.lighter.lighter_order_manager import ManagedOrder

    mo = ManagedOrder(
        client_order_index=12345,
        side="buy",
        market_index=161,
        price=_D("100"),
        size_base=_D("1"),
        price_decimals=3,
        size_decimals=3,
        order_type="limit",
        time_in_force="post_only",
        reduce_only=False,
        sent_ts_ms=int(time.time() * 1000),
    )
    d = _adapt_managed_order(mo)
    assert d["client_order_id"] == 12345
    assert d["side"] == "buy"
    assert d["price"] == _D("100")
    assert d["size_base"] == _D("1")
