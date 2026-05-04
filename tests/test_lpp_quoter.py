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
        self.cancel_always_fails = False

    async def create_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.create_calls.append(kwargs)
        if self.create_always_fails:
            return (None, None, "fake fail")
        return ("ok", "ok", None)

    async def cancel_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.cancel_calls.append(kwargs)
        if self.cancel_always_fails:
            return (None, None, "fake cancel fail")
        return ("ok", "ok", None)


class FakeGateway:
    def __init__(self) -> None:
        self._signer = FakeSigner()
        self.book: Optional[Dict[str, Any]] = None
        # REST stubs for OM REST-sync and quoter backup-cancel paths.
        # Tests can preload ``open_orders_response`` and inspect the
        # call lists / cancel results.
        self.open_orders_response: List[Dict[str, Any]] = []
        self.get_open_orders_calls: List[int] = []
        self.cancel_by_index_calls: List[Tuple[int, int]] = []
        self.cancel_by_index_result: Dict[str, Any] = {"ok": True, "tx": "fake"}
        self.cancel_by_index_raises: Optional[BaseException] = None
        # Phase 2.1: collateral source for pct cap + drawdown gate.
        self.account_info: Optional[Dict[str, Any]] = {
            "collateral": Decimal("2000"),
            "available_balance": Decimal("2000"),
        }
        self.get_account_info_calls: int = 0

    @property
    def signer_client(self) -> FakeSigner:
        return self._signer

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        return self.book

    async def get_open_orders(self, market_index: int) -> List[Dict[str, Any]]:
        self.get_open_orders_calls.append(int(market_index))
        return list(self.open_orders_response)

    async def cancel_order_by_index(
        self,
        market_index: int,
        order_index: int,
    ) -> Dict[str, Any]:
        self.cancel_by_index_calls.append((int(market_index), int(order_index)))
        if self.cancel_by_index_raises is not None:
            raise self.cancel_by_index_raises
        return dict(self.cancel_by_index_result)

    async def get_account_info(self) -> Optional[Dict[str, Any]]:
        self.get_account_info_calls += 1
        if self.account_info is None:
            return None
        return dict(self.account_info)


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
    """Snapshot reflects the OM's actual active orders, not plan_quotes desired.

    The 7-min live regression: plan_quotes returns [] for a tick when
    spread momentarily under-shoots min, but the book still carries
    our resting orders. We must record what's really on the book.
    """
    q, _, _, om, tracker = _make_quoter()

    async def _go() -> None:
        await q.start()
        # Pre-load 2 active orders. These ARE the my_quotes the
        # tracker should see — even if plan_quotes might return
        # something else for this market state.
        for side, price in (("buy", _D("99.95")), ("sell", _D("100.05"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open", order_index=1))
        market = await q._build_market_snapshot()
        await q._record_snapshot(market, get_kr_overnight_session())

    asyncio.run(_go())
    assert len(tracker.snapshots) == 1
    rec = tracker.snapshots[0]
    assert rec["session"] == "KR_OVERNIGHT"
    # 2 active orders → 2 quotes, regardless of what plan_quotes thinks.
    assert rec["n_quotes"] == 2


def test_record_snapshot_uses_actual_book_when_plan_quotes_returns_empty():
    """The regression case: plan_quotes returns [] but book has live orders.

    With ``min_market_spread_bp`` set to 999 the planner refuses to
    quote any market we have, so plan_quotes is guaranteed to return
    []. The tracker still has to see the 2 orders that are really on
    the book.
    """
    q, _, _, om, tracker = _make_quoter(
        config_overrides={"min_market_spread_bp": Decimal("999")},
    )

    async def _go() -> None:
        await q.start()
        for side, price in (("buy", _D("99.95")), ("sell", _D("100.05"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open", order_index=1))
        market = await q._build_market_snapshot()
        # Sanity: with the 999bp min, plan_quotes is empty.
        from strategy.quote_planner import plan_quotes
        assert plan_quotes(
            market=market,
            session=get_kr_overnight_session(),
            inventory=om.get_inventory(mark_price=market.mid),
            config=q.config,
        ) == []
        await q._record_snapshot(market, get_kr_overnight_session())

    asyncio.run(_go())
    assert len(tracker.snapshots) == 1
    # 2 actual orders on the book, despite plan_quotes returning [].
    assert tracker.snapshots[0]["n_quotes"] == 2


def test_managed_to_quote_derives_tier_from_distance():
    """Tier comes from current-mid distance, not the planner's submit-time value.

    Sanity-checks the ``<=`` boundary semantics: L1 ceiling lands in
    L1, the next bp slips to L2.
    """
    from execution.lighter.lighter_order_manager import ManagedOrder
    q, _, _, _, _ = _make_quoter()
    session = SessionPolicy(
        name="KR_OVERNIGHT", action="quote",
        default_distance_bp=_D("8"), default_size_usdc=_D("500"),
        tier_thresholds_bp=(_D("15"), _D("25"), _D("50")), reason="test",
    )

    def _make_order(price: str) -> ManagedOrder:
        return ManagedOrder(
            client_order_index=1,
            side="buy",
            market_index=161,
            price=_D(price),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
            order_type="limit",
            time_in_force="post_only",
            reduce_only=False,
            sent_ts_ms=0,
        )

    market = _make_market_with_mid(_D("100"))
    cases = [
        ("99.99", "L1"),   # 1bp distance
        ("99.97", "L1"),   # 3bp
        ("99.85", "L1"),   # 15bp — boundary, L1
        ("99.84", "L2"),   # 16bp — slips to L2
        ("99.75", "L2"),   # 25bp — boundary, L2
        ("99.50", "L3"),   # 50bp — boundary, L3
        ("99.49", "OUT"),  # 51bp
    ]
    for price_str, expected_tier in cases:
        quote = q._managed_to_quote(_make_order(price_str), market, session)
        assert quote.tier_target == expected_tier, (
            f"price={price_str} expected={expected_tier} got={quote.tier_target}"
        )


def test_managed_to_quote_position_from_bbo():
    """Market position is re-derived from the live BBO at snapshot time."""
    from execution.lighter.lighter_order_manager import ManagedOrder
    q, _, _, _, _ = _make_quoter()
    session = SessionPolicy(
        name="KR_OVERNIGHT", action="quote",
        default_distance_bp=_D("8"), default_size_usdc=_D("500"),
        tier_thresholds_bp=(_D("15"), _D("25"), _D("50")), reason="test",
    )
    # mid=100, best_bid=99.95, best_ask=100.05
    market = _make_market_with_mid(_D("100"), best_bid=_D("99.95"), best_ask=_D("100.05"))

    def _make_order(side: str, price: str) -> ManagedOrder:
        return ManagedOrder(
            client_order_index=1, side=side, market_index=161,
            price=_D(price), size_base=_D("1"),
            price_decimals=3, size_decimals=3,
            order_type="limit", time_in_force="post_only",
            reduce_only=False, sent_ts_ms=0,
        )

    # Buy improving (above best_bid)
    assert q._managed_to_quote(
        _make_order("buy", "99.96"), market, session
    ).market_position == "improving"
    # Buy passive at best_bid
    assert q._managed_to_quote(
        _make_order("buy", "99.95"), market, session
    ).market_position == "passive"
    # Buy below best_bid
    assert q._managed_to_quote(
        _make_order("buy", "99.90"), market, session
    ).market_position == "passive_below"
    # Sell improving (below best_ask)
    assert q._managed_to_quote(
        _make_order("sell", "100.04"), market, session
    ).market_position == "improving"
    # Sell passive at best_ask
    assert q._managed_to_quote(
        _make_order("sell", "100.05"), market, session
    ).market_position == "passive"
    # Sell above best_ask
    assert q._managed_to_quote(
        _make_order("sell", "100.10"), market, session
    ).market_position == "passive_above"


def _make_market_with_mid(
    mid: Decimal,
    *,
    best_bid: Optional[Decimal] = None,
    best_ask: Optional[Decimal] = None,
) -> MarketSnapshot:
    """Helper: build a minimal MarketSnapshot for snapshot/managed-quote tests."""
    bb = best_bid if best_bid is not None else mid - Decimal("0.05")
    ba = best_ask if best_ask is not None else mid + Decimal("0.05")
    return MarketSnapshot(
        symbol="SKHYNIXUSD",
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=bb,
        best_ask=ba,
        spread_bp=(ba - bb) / mid * Decimal(10000),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            25: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            50: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
        },
        price_decimals=3,
        size_decimals=3,
        ts_ms=int(time.time() * 1000),
    )


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


# ----- G2. log diagnostics ------------------------------------------------


def test_lpp_quoter_logs_reprice_trigger(caplog):
    """``run_until`` must log a ``reprice trigger:`` line when reprice fires.

    Forensic value: when reprice cadence looks wrong post-hoc, the
    trigger log carries last_mid/new_mid/drift_bp so the cause is
    obvious without having to re-run with -vv.
    """
    import logging
    caplog.set_level(logging.INFO, logger="strategy.lpp_quoter")
    q, _, _, _, _ = _make_quoter(
        config_overrides={"tick_interval_sec": Decimal("0.01")},
    )

    async def _go() -> None:
        await q.start()
        # 0.05s budget — far past one tick at 0.01s interval; the
        # initial "reason=initial" reprice should have fired.
        await q.run_until(time.time() + 0.05)
        await q.shutdown()

    asyncio.run(_go())
    assert "reprice trigger" in caplog.text


# ----- H. consecutive cancel-failure trigger (Fix A) ---------------------


def test_emergency_stop_consecutive_cancel_failures():
    """N reprices with cancel failures → counter ≥ threshold → emergency stop.

    Configures threshold=2, runs two reprices each with 2 cancel attempts
    that fail at the SDK layer. Counter should reach ≥2 and the
    emergency-stop predicate should fire.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "emergency_stop_on_consecutive_cancel_fail_count": 2,
            # Disable other triggers so this test isolates the cancel path.
            "emergency_stop_on_consecutive_reject_count": 999,
        },
    )
    # Make the SDK fail every cancel — OM.cancel_order returns False
    # after the retry loop exhausts.
    gw.signer_client.cancel_always_fails = True

    async def _go() -> None:
        await q.start()
        # Pre-load 2 active orders far from the desired set so the
        # next reprice produces 2 cancels.
        for side, price in (("buy", _D("90")), ("sell", _D("110"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open"))

        market = await q._build_market_snapshot()
        session = get_kr_overnight_session()
        # First reprice: 2 cancels, all fail → counter should be 2.
        await q._execute_reprice(market, session, "mid_drift(20bp)")
        # Second reprice: orders are still active (cancel didn't change
        # state), so we'll attempt to cancel them again → 4 cumulative.
        await q._execute_reprice(market, session, "mid_drift(20bp)")

    asyncio.run(_go())
    assert q.get_summary()["consecutive_cancel_failures"] >= 2
    assert q._should_emergency_stop() is True


def test_consecutive_cancel_failures_resets_on_success():
    """A reprice where every cancel succeeds resets the counter to 0."""
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "emergency_stop_on_consecutive_cancel_fail_count": 5,
        },
    )

    async def _go() -> Tuple[int, int]:
        await q.start()
        # Pre-load 2 active orders.
        for side, price in (("buy", _D("90")), ("sell", _D("110"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open"))

        market = await q._build_market_snapshot()
        session = get_kr_overnight_session()

        # First reprice: cancels fail.
        gw.signer_client.cancel_always_fails = True
        await q._execute_reprice(market, session, "mid_drift(20bp)")
        after_fail = q._stats["consecutive_cancel_failures"]

        # Re-prime active orders (the OM still has them since cancel
        # didn't move them out, but we want a fresh batch with new
        # coids so order_index lookup still works on the now-passing
        # cancel path).
        # Second reprice with cancels succeeding: counter resets to 0.
        gw.signer_client.cancel_always_fails = False
        await q._execute_reprice(market, session, "mid_drift(20bp)")
        after_success = q._stats["consecutive_cancel_failures"]

        return after_fail, after_success

    after_fail, after_success = asyncio.run(_go())
    assert after_fail >= 2, f"first reprice should accumulate failures, got {after_fail}"
    assert after_success == 0, f"successful cancel batch must reset, got {after_success}"


# ----- I. backup cancel via gateway (shutdown hardening) -----------------


def test_shutdown_calls_backup_cancel_after_om_cancel_all():
    """Shutdown's polling loop is followed by a REST-direct backup cancel.

    Even if the OM thinks everything is cancelled, the gateway view of
    the book is what counts. ``_backup_cancel_via_gateway`` should fire
    and (when REST reports stale orders) issue a per-order cancel.
    """
    q, gw, _, om, _ = _make_quoter()
    # Simulate a stale order on the book that the OM was unaware of —
    # exactly the regression that produced the -1.132 short. After OM
    # cancel_all completes, REST still reports this.
    gw.open_orders_response = [
        {
            "client_order_index": 9999,
            "order_index": 7777777,
            "market_index": 161,
            "side": "buy",
            "price": _D("100"),
            "status": "open",
        }
    ]

    async def _go() -> None:
        await q.start()
        await q.shutdown()

    asyncio.run(_go())
    # Backup pulled the book.
    assert 161 in gw.get_open_orders_calls
    # And cancelled the stale order via REST.
    assert (161, 7777777) in gw.cancel_by_index_calls


def test_session_overrides_propagate_through_planner():
    """yaml session_overrides → LppQuoter → get_kr_equity_session.

    Verifies the wiring fix from 04-29 catastrophic-bug analysis:
    yaml-level per-session size_usdc overrides must reach the planner
    so a small-size live run can't be silently downgraded to the
    hard-coded $500-$1000 defaults baked into session_aware._DEFAULTS.
    """
    overrides = {
        "KR_OVERNIGHT": {"default_size_usdc": _D("50")},
        "KR_MARKET_HOURS_AM": {
            "default_size_usdc": _D("75"),
            "default_distance_bp": _D("6"),
        },
    }
    q, _, _, _, _ = _make_quoter(
        config_overrides={"session_overrides": overrides},
    )
    # Loaded onto the quoter
    assert "KR_OVERNIGHT" in q._session_overrides
    assert q._session_overrides["KR_OVERNIGHT"]["default_size_usdc"] == _D("50")
    assert q._session_overrides["KR_MARKET_HOURS_AM"]["default_size_usdc"] == _D("75")
    assert q._session_overrides["KR_MARKET_HOURS_AM"]["default_distance_bp"] == _D("6")
    # And the override actually flows into get_kr_equity_session — pick a
    # Mon 02:00 UTC instant which deterministically lands on KR_MARKET_HOURS_AM,
    # so we can assert the planner-bound size is the override (75) rather
    # than the hard-coded default (1000).
    from strategy.session_aware import get_kr_equity_session
    moment = datetime(2026, 4, 20, 2, 0, tzinfo=timezone.utc)
    s = get_kr_equity_session(moment, config=q._session_overrides)
    assert s.name == "KR_MARKET_HOURS_AM"
    assert s.default_size_usdc == _D("75")
    assert s.default_distance_bp == _D("6")


def test_emergency_stop_calls_backup_cancel():
    """Emergency-stop path also drives the REST backup cancel."""
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "emergency_stop_on_consecutive_cancel_fail_count": 1,
            "tick_interval_sec": Decimal("0.01"),
        },
    )
    # REST will report a stale order during the emergency cleanup.
    gw.open_orders_response = [
        {
            "client_order_index": 8888,
            "order_index": 5555555,
            "market_index": 161,
            "side": "sell",
            "price": _D("105"),
            "status": "open",
        }
    ]

    async def _go() -> None:
        await q.start()
        # Pre-set the cancel-failure counter past threshold so the
        # first tick trips emergency stop.
        q._stats["consecutive_cancel_failures"] = 5
        await q.run_until(time.time() + 5)

    asyncio.run(_go())
    assert q.get_summary()["emergency_stops"] == 1
    # Emergency path must have driven the backup cancel.
    assert (161, 5555555) in gw.cancel_by_index_calls


# ----- J. Phase 1.2 P0.4: send-failure surge → reconcile + pause --------


def test_send_failure_surge_triggers_reconcile_pause():
    """A burst of place failures across reprices fires a forced REST
    reconcile + backup-cancel and stamps a future ``_reconcile_pause_until_ms``
    deadline so subsequent ticks skip repricing.

    Regression: 4-30 q=0/limit-window storm — the quoter kept hammering
    the SDK once per second while 429s rolled in, generating orphan
    orders the OM couldn't see. This guard breaks the cycle.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            # Tight thresholds so a single reprice with two failed
            # places trips the surge handler without needing many
            # iterations of the test setup.
            "send_failure_threshold": 2,
            "send_failure_window_sec": 30,
            "reconcile_pause_sec": 60,
            # Avoid emergency-stop competing with the surge path on
            # the same condition.
            "emergency_stop_on_consecutive_reject_count": 999,
        },
    )
    # Make every place fail at the SDK layer.
    gw.signer_client.create_always_fails = True

    async def _go() -> None:
        await q.start()
        market = await q._build_market_snapshot()
        session = get_kr_overnight_session()
        # First reprice: 2 places, both fail → fail count = 2 → trip.
        await q._execute_reprice(market, session, "initial")

    asyncio.run(_go())
    # Reconcile fired exactly once.
    assert q.get_summary()["reconcile_count"] == 1
    # Pause deadline is in the future (well past current ms).
    now_ms = int(time.time() * 1000)
    assert q._reconcile_pause_until_ms > now_ms
    # And the surge handler triggered the backup-cancel REST sweep.
    assert 161 in gw.get_open_orders_calls


def test_reconcile_pause_skips_reprice_in_run_loop():
    """When ``_reconcile_pause_until_ms`` is set in the future, the run
    loop ticks through but does not reprice. ``reprice_count`` stays at 0
    and no SDK create_order calls go out."""
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "tick_interval_sec": Decimal("0.01"),
            # Disable the min-interval floor so any tick *would* reprice
            # if not for the pause.
            "reprice_min_interval_sec": Decimal("0"),
        },
    )

    async def _go() -> None:
        await q.start()
        # Stamp the pause well into the future.
        q._reconcile_pause_until_ms = int(time.time() * 1000) + 60_000
        await q.run_until(time.time() + 0.10)  # 100ms of ticks

    asyncio.run(_go())
    # Ticks ran (loop body executed) but reprice was skipped.
    assert q.get_summary()["tick_count"] >= 1
    assert q.get_summary()["reprice_count"] == 0
    assert len(gw.signer_client.create_calls) == 0


# ----- K. Phase 2.1 P2.1.1: collateral refresh + double cap --------------


def test_collateral_refresh_periodic():
    """``_refresh_collateral`` honours the cadence — multiple ``force=False``
    calls within the interval are no-ops; a refresh after the interval
    pulls fresh data."""
    q, gw, _, _, _ = _make_quoter(
        config_overrides={"collateral_refresh_interval_sec": 1},
    )
    # Pre-set collateral.
    gw.account_info = {"collateral": Decimal("1500")}

    async def _go() -> Tuple[int, Optional[Decimal]]:
        await q.start()  # captures baseline + first refresh
        baseline_calls = gw.get_account_info_calls
        # Within cadence: should be no-op.
        await q._refresh_collateral(force=False)
        within_calls = gw.get_account_info_calls
        # Pretend cadence elapsed by backdating the timestamp.
        q._last_collateral_refresh_ts_ms -= 5000
        gw.account_info = {"collateral": Decimal("1700")}
        await q._refresh_collateral(force=False)
        return (
            baseline_calls,
            within_calls,
            gw.get_account_info_calls,
            q._latest_collateral_usdc,
        )

    baseline, within, after, latest = asyncio.run(_go())
    assert baseline >= 1, "start() should have called get_account_info at least once"
    assert within == baseline, "within-cadence call should be a no-op"
    assert after > within, "refresh after cadence should call gateway again"
    assert latest == Decimal("1700")


# ----- L. Phase 2.1 P2.1.2: active hedge ---------------------------------


def _hedge_market(
    mid: Decimal = Decimal("100"),
) -> "MarketSnapshot":
    """A hand-rolled MarketSnapshot for the hedge unit tests.

    Avoids the gateway+ws plumbing of ``_build_market_snapshot`` so each
    test can poke at one input without needing a full quoter loop.
    """
    return MarketSnapshot(
        symbol="SKHYNIXUSD",
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=mid - Decimal("0.05"),
        best_ask=mid + Decimal("0.05"),
        spread_bp=Decimal("10"),
        depth_by_spread_bp={},
        price_decimals=3,
        size_decimals=3,
        ts_ms=int(time.time() * 1000),
    )


def _inv(usdc: str, base: str = "0", price: str = "100") -> Any:
    """Build an InventoryState directly. Avoids touching the OM."""
    from strategy.types import InventoryState as _IS
    return _IS(
        net_delta_base=_D(base),
        net_delta_usdc=_D(usdc),
        avg_entry_price=_D(price),
        open_orders_count=0,
    )


def test_active_hedge_does_not_trigger_when_disabled():
    """active_hedge_enabled=False → helper returns False even when inv
    breaches the would-be trigger threshold."""
    q, _, _, _, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": False,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
        },
    )
    market = _hedge_market()
    inv = _inv("150")  # 1.5x cap

    async def _go() -> bool:
        await q.start()
        return await q._maybe_trigger_active_hedge(market, inv)

    assert asyncio.run(_go()) is False
    assert q.get_summary()["active_hedges_total"] == 0


def test_active_hedge_triggers_when_inv_breaches_trigger():
    """P7: hedge submit uses post_only LIMIT + reduce_only=False, fills
    via natural taker (synthesised here as an inline ws push)."""
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.3"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pause_after_sec": 30,
            "active_hedge_post_submit_wait_sec": Decimal("0.5"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            # Disable the pct cap path so the absolute one binds cleanly.
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    # Long $120 against $100 cap, 100*1.0 = trigger 100, 120 >= 100 → fire.
    inv = _inv("120")

    async def _go() -> Tuple[bool, int]:
        await q.start()
        # Wrap submit so the post_only hedge synthesises a fill via ws.
        original_submit = om.submit_order

        async def _submit_with_fill(**kw: Any) -> int:
            coid = await original_submit(**kw)
            if (
                kw.get("time_in_force") == "post_only"
                and not kw.get("reduce_only")
            ):
                om.on_account_event(_ws_order_msg(
                    coid, "filled",
                    filled=str(kw["size_base"]), remaining="0",
                    order_index=coid % 100000,
                ))
            return coid

        om.submit_order = _submit_with_fill  # type: ignore[method-assign]
        ts_before = q._reconcile_pause_until_ms
        triggered = await q._maybe_trigger_active_hedge(market, inv)
        return (triggered, q._reconcile_pause_until_ms - ts_before)

    triggered, pause_advance = asyncio.run(_go())
    assert triggered is True
    assert q.get_summary()["active_hedges_total"] == 1
    last_create = gw.signer_client.create_calls[-1]
    assert last_create["is_ask"] is True  # sell to flatten long
    # P7: post_only LIMIT, NOT IOC, NOT reduce_only.
    assert last_create["reduce_only"] is False
    from execution.lighter.lighter_order_manager import (
        _ORDER_TYPE_MAP, _TIME_IN_FORCE_MAP,
    )
    assert last_create["order_type"] == _ORDER_TYPE_MAP["limit"]
    assert last_create["time_in_force"] == _TIME_IN_FORCE_MAP["post_only"]
    # No IOC ``order_expiry`` override on the post_only path.
    assert "order_expiry" not in last_create
    # Pause window roughly 30s in the future.
    assert pause_advance >= 25_000


def test_active_hedge_size_calculation_correct():
    """Hedge size = abs(inv) - target_usdc, then convert to base.

    cap=100, target_pct=0.3 → target_usdc=30. Inv=120 → flatten 90 USDC.
    At mid=100, that's 0.9 base. With size_decimals=3, expect 0.900.
    """
    q, gw, _, _, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.3"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")

    async def _go() -> None:
        await q.start()
        await q._maybe_trigger_active_hedge(market, inv)

    asyncio.run(_go())
    last_create = gw.signer_client.create_calls[-1]
    # base_amount is integer at size_decimals=3 → 0.900 base = 900.
    # The hedge times out (no fill synthesised) but the create call
    # still records the right base_amount, which is what the test
    # asserts. Whether the hedge succeeded is irrelevant here.
    assert last_create["base_amount"] == 900


def test_active_hedge_pause_blocks_reprice_in_run_loop():
    """After hedge fires, run_until's reprice path is skipped until the
    pause window elapses. ``hedge_fired`` itself causes the tick to
    sleep_to_next_tick + continue."""
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.3"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pause_after_sec": 60,
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "tick_interval_sec": Decimal("0.01"),
            "reprice_min_interval_sec": Decimal("0"),
            "hard_position_cap_pct": None,
        },
    )

    async def _go() -> None:
        await q.start()
        # Wrap submit so the post_only hedge synthesises a fill — without
        # it the hedge times out and never sets the pause we're testing.
        original_submit = om.submit_order

        async def _submit_with_fill(**kw: Any) -> int:
            coid = await original_submit(**kw)
            if (
                kw.get("time_in_force") == "post_only"
                and not kw.get("reduce_only")
            ):
                om.on_account_event(_ws_order_msg(
                    coid, "filled",
                    filled=str(kw["size_base"]), remaining="0",
                    order_index=coid % 100000,
                ))
            return coid

        om.submit_order = _submit_with_fill  # type: ignore[method-assign]
        # Inject a long position so the next tick fires hedge.
        q._om.inject_initial_inventory(
            base=Decimal("1.2"),
            avg_entry_price=Decimal("100"),
            sign=1,
        )
        await q.run_until(time.time() + 0.10)

    asyncio.run(_go())
    summary = q.get_summary()
    assert summary["active_hedges_total"] >= 1
    # At least the hedge submit; possibly no other reprice creates.
    # The hedge itself goes through submit_order so create_calls > 0.
    # Pause should have suppressed any post-hedge reprice creates.
    # Counting: if active_hedge fires and pause holds, only the hedge
    # submit is in the list (no reprice creates).
    assert len(gw.signer_client.create_calls) == summary["active_hedges_total"]


def test_active_hedge_failure_logged_not_crash():
    """submit_order raising → counter increments, return False, no crash."""
    q, gw, _, _, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.3"),
            "hard_position_cap_pct": None,
        },
    )
    # Force submit_order itself to raise (not just SDK fail).
    async def _raise_submit(*a, **kw):
        raise RuntimeError("simulated submit_order failure")

    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")

    async def _go() -> bool:
        await q.start()
        q._om.submit_order = _raise_submit  # type: ignore[method-assign]
        return await q._maybe_trigger_active_hedge(market, inv)

    assert asyncio.run(_go()) is False
    assert q.get_summary()["active_hedges_failed"] == 1


def test_active_hedge_consecutive_sdk_rejects_trip_emergency_stop():
    """N consecutive SDK-rejected hedge submits arm the emergency-stop
    gate, and a single accepted submit resets the streak.

    Regression: the 4-30 live run sent 80+ IOC hedges that the SDK
    rejected with ``OrderExpiry is invalid``. Because submit_order
    swallows SDK errors and returns the coid normally, the quoter
    counted each as a successful hedge and never tripped the kill
    switch — leaving naked exposure for the rest of the session.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.3"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pause_after_sec": 0,  # don't gate the next call
            "active_hedge_max_consecutive_fails": 5,
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")

    # Wrap submit_order so SDK-accepted post_only hedges get a synth
    # fill (recovery branch needs success); SDK-rejected ones don't
    # synth anything and follow the rejection path.
    def _install_wrap() -> None:
        original_submit = om.submit_order

        async def _wrapped_submit(**kw: Any) -> int:
            coid = await original_submit(**kw)
            state = om.get_order_state(coid)
            if (
                state is not None
                and state.status != "rejected"
                and kw.get("time_in_force") == "post_only"
                and not kw.get("reduce_only")
            ):
                om.on_account_event(_ws_order_msg(
                    coid, "filled",
                    filled=str(kw["size_base"]), remaining="0",
                    order_index=coid % 100000,
                ))
            return coid

        om.submit_order = _wrapped_submit  # type: ignore[method-assign]

    async def _fire(n: int) -> int:
        await q.start()
        _install_wrap()
        # Force the SDK to reject every submit. ``submit_order``
        # catches LighterSDKError, marks the order rejected, returns
        # the coid — the wrap above sees status=rejected and skips the
        # synth fill, so the rejection path runs cleanly.
        gw.signer_client.create_always_fails = True
        for _ in range(n):
            await q._maybe_trigger_active_hedge(market, inv)
        return q._stats["consecutive_active_hedge_fails"]

    fails = asyncio.run(_fire(5))
    # P8: at the 5th fail the threshold (max_consecutive_fails=5) is
    # hit; ``_record_active_hedge_fail`` logs DISABLED, flips
    # ``_active_hedge_enabled`` to False, and resets the streak
    # counter to 0. So ``fails`` (the post-loop counter read) is 0.
    assert fails == 0
    summary = q.get_summary()
    assert summary["consecutive_active_hedge_fails"] == 0  # reset on disable
    assert summary["active_hedges_failed"] == 5
    # ``active_hedges_total`` counts only fully-accepted submits.
    assert summary["active_hedges_total"] == 0
    # P8: sub-feature disabled, NOT whole-strategy emergency_stop.
    assert summary["active_hedge_disabled_due_to_fails"] is True
    assert q._active_hedge_enabled is False
    assert q._should_emergency_stop() is False

    # P8: recovery is via user restart (yaml re-arms enabled=true).
    # In tests we mimic that by flipping the flag back manually,
    # then verify a clean SDK accept clears the streak.
    async def _recover_via_restart() -> int:
        q._active_hedge_enabled = True
        q._active_hedge_disabled_due_to_fails = False
        gw.signer_client.create_always_fails = False
        await q._maybe_trigger_active_hedge(market, inv)
        return q._stats["consecutive_active_hedge_fails"]

    assert asyncio.run(_recover_via_restart()) == 0
    assert q._should_emergency_stop() is False
    assert q.get_summary()["active_hedges_total"] == 1


# ----- L2. 5-3 SAMSUNG self-trade-protection fix -------------------------
#
# Lighter rejects an IOC silently when our resting maker sits on the
# opposite side of the book (5-3 SAMSUNG 22:26 UTC: $199 long, 195 IOC
# sells, 0 fills). Three guard rails restored together by the fix:
#   1. cancel resting makers BEFORE the IOC (pre-cancel + ack wait),
#   2. price the IOC through the BBO (not mid-slip — mid can land
#      between our maker and the next bid, locking the self-trade),
#   3. refuse the IOC if the BBO has walked far enough that the
#      implied slip exceeds taker tolerance (mass-dump regime).
# Below tests pin each rail.


def _track_sdk_calls(gw: FakeGateway) -> List[str]:
    """Hook the FakeSigner so we can assert ordering between create
    and cancel SDK calls. Returns a shared event log; entries are the
    string ``"create"`` or ``"cancel"`` in the order they hit the SDK.
    """
    log: List[str] = []
    original_create = gw.signer_client.create_order
    original_cancel = gw.signer_client.cancel_order

    async def _track_create(**kw: Any) -> Any:
        log.append("create")
        return await original_create(**kw)

    async def _track_cancel(**kw: Any) -> Any:
        log.append("cancel")
        return await original_cancel(**kw)

    gw.signer_client.create_order = _track_create  # type: ignore[method-assign]
    gw.signer_client.cancel_order = _track_cancel  # type: ignore[method-assign]
    return log


def _patch_om_cancel_with_synth_ack(om: Any) -> None:
    """Make ``om.cancel_order`` synthesise an immediate ws-confirmed
    ``canceled`` push for the cancelled coid so the pre-cancel poll
    can see the maker move to historical without a real ws stream.
    """
    original_cancel = om.cancel_order

    async def _cancel_with_ack(coid: int) -> bool:
        result = await original_cancel(coid)
        om.on_account_event(
            _ws_order_msg(coid, "canceled", filled="0", remaining="0")
        )
        return result

    om.cancel_order = _cancel_with_ack  # type: ignore[method-assign]


def test_active_hedge_cancels_my_quotes_first():
    """Pre-cancel (P7): hedge submit comes after the resting makers are
    cancelled and confirmed terminal. P7 doesn't strictly need this
    (post_only doesn't trigger Lighter self-trade), but the pre-cancel
    is kept defensively to keep the book clean during the close.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.0"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pre_cancel_wait_sec": Decimal("1"),
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")  # long $120 → trigger fires at cap=$100 trigger=1.0

    async def _go() -> None:
        await q.start()
        # Pre-place 2 resting makers on market 161 (track their coids
        # so we can verify they're terminal before the hedge submit).
        maker_coids: List[int] = []
        for side, price in (("buy", _D("99.94")), ("sell", _D("100.06"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            maker_coids.append(coid)
            om.on_account_event(_ws_order_msg(coid, "open", order_index=coid % 100000))
        # Patch om.cancel_order so the ws ack is synthesised inline.
        _patch_om_cancel_with_synth_ack(om)
        # Wrap submit_order so the post_only hedge synthesises a fill
        # — without it the hedge times out and active_hedges_total
        # never increments.
        original_submit = om.submit_order

        async def _submit_with_fill(**kw: Any) -> int:
            coid = await original_submit(**kw)
            if (
                kw.get("time_in_force") == "post_only"
                and not kw.get("reduce_only")
                and kw.get("size_base") != _D("1")  # not the makers
            ):
                om.on_account_event(_ws_order_msg(
                    coid, "filled",
                    filled=str(kw["size_base"]), remaining="0",
                    order_index=coid % 100000,
                ))
            return coid

        om.submit_order = _submit_with_fill  # type: ignore[method-assign]
        # Hook the signer to record SDK call ordering.
        sdk_log = _track_sdk_calls(gw)
        await q._maybe_trigger_active_hedge(market, inv)
        # 2 cancels then exactly 1 create (the post_only close). The
        # 2 maker creates happened earlier (pre-place), before this
        # hook was installed.
        assert sdk_log == ["cancel", "cancel", "create"], (
            f"expected cancel,cancel,create — got {sdk_log}"
        )
        # No resting makers left active when the close went out.
        leftover_makers = [
            o for o in om.get_active_orders()
            if o.client_order_index in maker_coids
        ]
        assert leftover_makers == [], (
            f"resting makers must be terminal before close; still active: "
            f"{[(o.client_order_index, o.status) for o in leftover_makers]}"
        )

    asyncio.run(_go())
    assert q.get_summary()["active_hedges_total"] == 1


def test_active_hedge_skips_if_slip_exceeds_max():
    """Mass-dump regime: best_bid sits 5% below mid. The BBO-derived
    P7 close price would lock in a 312bp loss; refuse to send.

    Counter must NOT increment — wide spread is a market condition,
    not a hedge mechanism failure. We retry on the next eligible tick
    when the spread tightens.
    """
    q, gw, _, _, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.0"),
            "active_hedge_taker_fee_max_pct": Decimal("0.15"),  # 15bp
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "hard_position_cap_pct": None,
        },
    )
    # Crashed market: mid=160, best_bid=155 (~312bp below mid).
    market = MarketSnapshot(
        symbol="SAMSUNG",
        market_index=161,
        mid=Decimal("160"),
        mark_price=Decimal("160"),
        index_price=Decimal("160"),
        best_bid=Decimal("155"),
        best_ask=Decimal("165"),
        spread_bp=Decimal("625"),
        depth_by_spread_bp={},
        price_decimals=2,
        size_decimals=2,
        ts_ms=int(time.time() * 1000),
    )
    inv = _inv("120", price="160")

    async def _go() -> bool:
        await q.start()
        creates_before = len(gw.signer_client.create_calls)
        result = await q._maybe_trigger_active_hedge(market, inv)
        creates_after = len(gw.signer_client.create_calls)
        # No close sent.
        assert creates_after == creates_before
        return result

    triggered = asyncio.run(_go())
    assert triggered is False
    # Counter NOT incremented for slip-skip (market condition, not failure).
    assert q._stats["consecutive_active_hedge_fails"] == 0
    assert q.get_summary()["active_hedges_total"] == 0
    assert q.get_summary()["active_hedges_failed"] == 0


def test_active_hedge_aborts_if_pre_cancel_times_out():
    """Defensive: pre-cancel deadline elapses with quotes still live →
    close NOT sent, fail counter increments.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.0"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            # Tight pre-cancel deadline — 100ms — so the test runs fast.
            "active_hedge_pre_cancel_wait_sec": Decimal("0.1"),
            "active_hedge_post_submit_wait_sec": Decimal("0.05"),
            "active_hedge_poll_interval_sec": Decimal("0.01"),
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")

    async def _go() -> bool:
        await q.start()
        # Pre-place 2 resting makers — deliberately do NOT patch
        # cancel_order; the ws ack never lands.
        for side, price in (("buy", _D("99.94")), ("sell", _D("100.06"))):
            coid = await om.submit_order(
                side=side, market_index=161, price=price,
                size_base=_D("1"), price_decimals=3, size_decimals=3,
            )
            om.on_account_event(_ws_order_msg(coid, "open", order_index=coid % 100000))
        creates_before = len(gw.signer_client.create_calls)
        result = await q._maybe_trigger_active_hedge(market, inv)
        creates_after = len(gw.signer_client.create_calls)
        # 2 cancels were sent (best-effort), but no close.
        assert creates_after == creates_before
        return result

    triggered = asyncio.run(_go())
    assert triggered is False
    # Pre-cancel deadline timeout DOES count as a fail — repeated
    # timeouts mean the cancel path is broken, which is a real
    # mechanism failure; emergency_stop should arm.
    assert q._stats["consecutive_active_hedge_fails"] == 1
    assert q.get_summary()["active_hedges_failed"] == 1
    assert q.get_summary()["active_hedges_total"] == 0


def test_active_hedge_consecutive_zero_fill_trips_emergency_stop():
    """P7: external cancel/expire BEFORE any fill counts as a fail.

    Mirrors the silent-reject pattern (5-3 to 5-4 SAMSUNG, P6 IOC
    era): SDK accepted, ws then surfaces ``canceled``/``canceled-
    expired`` with ``filled_base=0``. The same code path catches it
    for post_only LIMIT closes too — if the order goes terminal
    without any fill, count as fail. Three consecutive arms
    emergency_stop.
    """
    q, gw, _, om, _ = _make_quoter(
        config_overrides={
            "active_hedge_enabled": True,
            "hard_position_cap_usdc": Decimal("100"),
            "active_hedge_trigger_pct": Decimal("1.0"),
            "active_hedge_target_pct": Decimal("0.0"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pause_after_sec": 0,  # don't gate retries
            "active_hedge_max_consecutive_fails": 3,
            # Wait briefly; the synth ws push lands inside the window.
            "active_hedge_post_submit_wait_sec": Decimal("0.5"),
            "active_hedge_poll_interval_sec": Decimal("0.02"),
            "hard_position_cap_pct": None,
        },
    )
    market = _hedge_market(mid=Decimal("100"))
    inv = _inv("120")

    # Patch submit_order so each close immediately gets a synthetic
    # ``canceled-expired`` ws push with zero fill — the silent-reject
    # signature, applicable to both IOC (P6 regression) and post_only
    # (any external cancel before a taker arrives).
    original_submit = om.submit_order

    async def _submit_then_expire(**kw: Any) -> int:
        coid = await original_submit(**kw)
        om.on_account_event(
            _ws_order_msg(coid, "canceled-expired", filled="0", remaining="900")
        )
        return coid

    async def _fire(n: int) -> int:
        await q.start()
        om.submit_order = _submit_then_expire  # type: ignore[method-assign]
        for _ in range(n):
            await q._maybe_trigger_active_hedge(market, inv)
        return q._stats["consecutive_active_hedge_fails"]

    fails = asyncio.run(_fire(3))
    # P8: 3rd fail hits threshold (max_consecutive_fails=3) → helper
    # disables active_hedge AND resets the streak. ``fails`` is the
    # post-loop counter read (0), but ``active_hedges_failed`` still
    # reflects the 3 fails that happened.
    assert fails == 0
    summary = q.get_summary()
    assert summary["active_hedges_failed"] == 3
    assert summary["active_hedges_total"] == 0
    assert summary["active_hedge_disabled_due_to_fails"] is True
    # P8: hedge fails disable the sub-feature, NOT the whole strategy.
    assert q._should_emergency_stop() is False


# ----- L3. P7 (5-4): post_only LIMIT close ------------------------------
#
# Day-3 SAMSUNG regression (5-3 → 5-4 UTC) showed the P6 cancel-then-IOC
# fix still hits Lighter's account-level self-trade protection: 3/3 IOC
# submits silently rejected with status=pending_ack filled=0 → REST sync
# flips to cancelled ~60s later. The user verified the same error throws
# on the Lighter web UI on manual close ("无法吃自己的单"), proving the
# enforcement is server-side and unavoidable from the client. P7
# replaces IOC reduce_only with post_only LIMIT @ best_bid + 1 tick
# (sell) / best_ask - 1 tick (buy), reduce_only=False — the regular
# maker path with no self-trade checks. Trade-off is fill is no longer
# instant; we wait active_hedge_post_submit_wait_sec for a natural
# external taker.


def _samsung_market(
    *,
    mid: Decimal = Decimal("160"),
    best_bid: Decimal = Decimal("159.960"),
    best_ask: Decimal = Decimal("160.200"),
    price_decimals: int = 3,
    size_decimals: int = 3,
) -> "MarketSnapshot":
    """SAMSUNG-shaped MarketSnapshot for P7 tests.

    Tick = 0.001 at price_decimals=3 keeps the math simple (best_bid +
    1 tick = 159.961). Spread of 24bp matches a realistic SAMSUNG
    BBO. Tests can override fields per scenario.
    """
    return MarketSnapshot(
        symbol="SAMSUNG",
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        spread_bp=(best_ask - best_bid) / mid * Decimal(10000),
        depth_by_spread_bp={},
        price_decimals=price_decimals,
        size_decimals=size_decimals,
        ts_ms=int(time.time() * 1000),
    )


def test_active_hedge_uses_post_only_not_ioc():
    """P7: SDK call is LIMIT + post_only + reduce_only=False, NOT IOC.

    Day-3 SAMSUNG (5-3 → 5-4) proved IOC reduce_only triggers Lighter's
    account-level self-trade protection even after pre-cancelling
    resting makers (P6). The fix is to abandon IOC entirely.
    """
    q, gw, _, _, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")

    async def _go() -> bool:
        await q.start()
        return await q._maybe_trigger_active_hedge(market, inv)

    asyncio.run(_go())
    last_create = gw.signer_client.create_calls[-1]
    from execution.lighter.lighter_order_manager import (
        _ORDER_TYPE_MAP, _TIME_IN_FORCE_MAP,
    )
    assert last_create["order_type"] == _ORDER_TYPE_MAP["limit"], (
        f"P7 must use LIMIT order_type, got {last_create['order_type']}"
    )
    assert last_create["time_in_force"] == _TIME_IN_FORCE_MAP["post_only"], (
        f"P7 must use post_only TIF, got {last_create['time_in_force']}"
    )
    assert last_create["reduce_only"] is False, (
        f"P7 must use reduce_only=False, got {last_create['reduce_only']}"
    )
    # The IOC ``order_expiry`` override is NOT applied on the post_only
    # path — confirm the kwargs reflect a vanilla limit.
    assert "order_expiry" not in last_create


def test_active_hedge_post_only_price_improves_bbo():
    """P7 sell close price = best_bid + 1 tick.

    With best_bid=159.960, best_ask=160.200, tick=0.001, the sell
    close prices at 159.961 — strictly above best_bid (improving),
    strictly below best_ask (post_only doesn't cross). The order
    becomes the new top-of-book ask; the next external taker buy
    crosses and fills our hedge.
    """
    q, gw, _, _, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")

    async def _go() -> None:
        await q.start()
        await q._maybe_trigger_active_hedge(market, inv)

    asyncio.run(_go())
    last_create = gw.signer_client.create_calls[-1]
    expected_int = int(Decimal("159.961") * Decimal(1000))
    assert last_create["price"] == expected_int, (
        f"expected price={expected_int} (= 159.961 at 3dp), "
        f"got {last_create['price']}"
    )
    assert last_create["is_ask"] is True
    bb_int = int(Decimal("159.960") * Decimal(1000))
    ba_int = int(Decimal("160.200") * Decimal(1000))
    # Improves the bid (strictly greater).
    assert last_create["price"] > bb_int, (
        f"sell close {last_create['price']} must be > best_bid {bb_int} "
        f"to improve top-of-book"
    )
    # Doesn't cross the ask (post_only would reject).
    assert last_create["price"] < ba_int, (
        f"sell close {last_create['price']} must be < best_ask {ba_int} "
        f"or post_only rejects locally"
    )


def test_active_hedge_post_only_buy_close():
    """P7 mirror: short position → buy close at best_ask - 1 tick.

    Symmetric to the sell-close test: with inv negative, the hedge
    side flips to buy and the close price prices below best_ask by
    one tick.
    """
    q, gw, _, _, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    # Short $145 → buy hedge to flatten.
    from strategy.types import InventoryState as _IS
    inv = _IS(
        net_delta_base=Decimal("-0.906"),
        net_delta_usdc=Decimal("-145"),
        avg_entry_price=Decimal("160"),
        open_orders_count=0,
    )

    async def _go() -> None:
        await q.start()
        await q._maybe_trigger_active_hedge(market, inv)

    asyncio.run(_go())
    last_create = gw.signer_client.create_calls[-1]
    expected_int = int(Decimal("160.199") * Decimal(1000))
    assert last_create["price"] == expected_int, (
        f"expected price={expected_int} (= 160.199 at 3dp), "
        f"got {last_create['price']}"
    )
    assert last_create["is_ask"] is False  # buy
    bb_int = int(Decimal("159.960") * Decimal(1000))
    ba_int = int(Decimal("160.200") * Decimal(1000))
    # Improves the ask (strictly less).
    assert last_create["price"] < ba_int
    # Doesn't cross the bid.
    assert last_create["price"] > bb_int


def test_active_hedge_post_only_fills_naturally():
    """P7 success: a synthetic ws fill arrives during the post-submit
    wait. The polling loop sees ``filled_base > 0`` and breaks; the
    fail counter resets and the post-hedge pause is set.

    Models the realistic sequence: submit, briefly idle, then a
    natural taker walks the book and our hedge fills.
    """
    q, gw, _, om, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_pause_after_sec": 60,
        "active_hedge_post_submit_wait_sec": Decimal("1"),
        "active_hedge_poll_interval_sec": Decimal("0.02"),
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")
    # Pre-load fail counter to verify reset on success.
    q._stats["consecutive_active_hedge_fails"] = 2

    async def _go() -> Tuple[bool, int]:
        await q.start()
        ts_before = q._reconcile_pause_until_ms
        # Background task: trigger hedge.
        hedge_task = asyncio.create_task(
            q._maybe_trigger_active_hedge(market, inv)
        )
        # Brief sleep, then synth fill via ws push.
        await asyncio.sleep(0.05)
        hedge_orders = [
            o for o in om.get_active_orders()
            if o.market_index == 161 and o.time_in_force == "post_only"
        ]
        assert len(hedge_orders) == 1, (
            f"expected 1 post_only hedge in flight, got {len(hedge_orders)}"
        )
        coid = hedge_orders[0].client_order_index
        size = hedge_orders[0].size_base
        om.on_account_event(_ws_order_msg(
            coid, "filled", filled=str(size), remaining="0",
            order_index=coid % 100000,
        ))
        triggered = await asyncio.wait_for(hedge_task, timeout=2.0)
        return (triggered, q._reconcile_pause_until_ms - ts_before)

    triggered, pause_advance = asyncio.run(_go())
    assert triggered is True
    assert q.get_summary()["active_hedges_total"] == 1
    # Counter reset on success.
    assert q._stats["consecutive_active_hedge_fails"] == 0
    # Pause set ~60s in the future.
    assert pause_advance >= 55_000


def test_active_hedge_post_only_times_out():
    """P7 timeout: no fill within the wait → cancel hedge + counter ↑.

    The hedge sits in pending_ack throughout (no ws fill, no terminal
    transition). The wait expires, ``_maybe_trigger_active_hedge``
    cancels the hedge and increments the consecutive-fail counter.
    """
    q, gw, _, om, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_post_submit_wait_sec": Decimal("0.10"),
        "active_hedge_poll_interval_sec": Decimal("0.02"),
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")

    async def _go() -> bool:
        await q.start()
        return await q._maybe_trigger_active_hedge(market, inv)

    triggered = asyncio.run(_go())
    assert triggered is False
    assert q._stats["consecutive_active_hedge_fails"] == 1
    assert q.get_summary()["active_hedges_failed"] == 1
    assert q.get_summary()["active_hedges_total"] == 0
    # Hedge submit went out — exactly 1 SDK create call.
    assert len(gw.signer_client.create_calls) == 1


def test_active_hedge_3_timeouts_disable_subfeature():
    """P8: 3 consecutive timeouts disable the hedge sub-feature
    (NOT whole-strategy emergency_stop).

    Counter increments on each timeout; on the 3rd it hits the
    configured threshold (max_consecutive_fails=3) and the
    ``_record_active_hedge_fail`` helper logs DISABLED, flips
    ``_active_hedge_enabled`` to False, and resets the streak. The
    strategy itself keeps running (cap + passive close + manual close
    cover residual exposure).
    """
    q, gw, _, om, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_pause_after_sec": 0,  # don't gate retries
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "active_hedge_max_consecutive_fails": 3,
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")

    async def _go() -> int:
        await q.start()
        for _ in range(3):
            await q._maybe_trigger_active_hedge(market, inv)
        return q._stats["consecutive_active_hedge_fails"]

    fails = asyncio.run(_go())
    # P8: counter reset to 0 after threshold disables the sub-feature.
    assert fails == 0
    summary = q.get_summary()
    assert summary["active_hedges_failed"] == 3
    assert summary["active_hedges_total"] == 0
    assert summary["active_hedge_disabled_due_to_fails"] is True
    assert q._active_hedge_enabled is False
    # P8: emergency_stop NOT triggered by hedge fails.
    assert q._should_emergency_stop() is False


# ----- L4. P8 (5-4): disable-only-on-fail -------------------------------
#
# Day-3 SAMSUNG (5-3 → 5-4 UTC): P6 IOC silent reject 3× → emergency_stop
# @ 5-4 01:48 → 14h fully offline (other markets too). Coupling hedge
# failure to whole-strategy shutdown is the wrong default. P8 changes
# the threshold action from emergency_stop to "disable the sub-feature
# only" — the cap + passive close + manual close cover residual
# exposure when the hedge breaks. Restart re-arms via yaml.


def test_active_hedge_3_fails_disables_not_stops():
    """P8 core invariant: at threshold, sub-feature disables and the
    strategy stays alive.

    After 3 timeout fails (max_consecutive_fails=3):
      - ``_active_hedge_disabled_due_to_fails`` is True
      - ``_active_hedge_enabled`` is False (next call early-exits)
      - ``consecutive_active_hedge_fails`` is reset to 0
      - ``_should_emergency_stop`` returns False (strategy continues)
    """
    q, _, _, _, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_pause_after_sec": 0,
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "active_hedge_max_consecutive_fails": 3,
        "hard_position_cap_pct": None,
    })
    market = _samsung_market()
    inv = _inv("145", price="160")

    async def _go() -> None:
        await q.start()
        for _ in range(3):
            await q._maybe_trigger_active_hedge(market, inv)

    asyncio.run(_go())
    assert q._active_hedge_disabled_due_to_fails is True
    assert q._active_hedge_enabled is False
    assert q._stats["consecutive_active_hedge_fails"] == 0
    # The key invariant: hedge fails do NOT trip whole-strategy stop.
    assert q._should_emergency_stop() is False


def test_strategy_continues_after_active_hedge_disabled():
    """P8: with active_hedge disabled, the reprice cycle still runs.

    Once the sub-feature is disabled, ``_maybe_trigger_active_hedge``
    early-exits (because ``_active_hedge_enabled`` is False), and the
    main run loop proceeds to the normal reprice path — plan_quotes
    runs, diff_quotes runs, submit_orders go out. Inventory above
    trigger no longer fires the hedge but is still bounded by the
    cap and passive-close logic.

    Setup uses session_overrides with $50 default_size_usdc so the
    planner stays under cap (without an override, the hard-coded
    KR_OVERNIGHT $500/$1000 default would fully suppress quotes
    against our $200 cap and obscure the test's intent).
    """
    q, gw, _, om, _ = _make_quoter(config_overrides={
        "active_hedge_enabled": True,  # starts enabled then we disable
        "hard_position_cap_usdc": Decimal("200"),
        "active_hedge_trigger_pct": Decimal("0.7"),
        "active_hedge_target_pct": Decimal("0.0"),
        "active_hedge_taker_fee_max_pct": Decimal("0.50"),
        "active_hedge_post_submit_wait_sec": Decimal("0.05"),
        "active_hedge_poll_interval_sec": Decimal("0.01"),
        "tick_interval_sec": Decimal("0.01"),
        "reprice_min_interval_sec": Decimal("0"),
        "hard_position_cap_pct": None,
        "session_overrides": {
            sess: {
                "default_size_usdc": Decimal("50"),
                "default_distance_bp": Decimal("8"),
            }
            for sess in (
                "KR_OVERNIGHT",
                "KR_BEFORE_OPEN",
                "KR_MARKET_HOURS_AM",
                "KR_MARKET_HOURS_PM",
                "KR_LUNCH_BREAK",
                "KR_AFTER_CLOSE",
                "KR_WEEKEND",
                "KR_HOLIDAY",
            )
        },
    })

    async def _go() -> None:
        await q.start()
        # Simulate the post-disable state.
        q._active_hedge_enabled = False
        q._active_hedge_disabled_due_to_fails = True
        # Inject a small long position — well under cap so plan_quotes
        # will still emit both legs.
        q._om.inject_initial_inventory(
            base=Decimal("0.20"),
            avg_entry_price=Decimal("100"),
            sign=1,
        )
        await q.run_until(time.time() + 0.10)

    asyncio.run(_go())
    summary = q.get_summary()
    # No hedge fired: disabled flag short-circuits.
    assert summary["active_hedges_total"] == 0
    # But the reprice cycle did run — non-zero reprice_count + at
    # least one place create.
    assert summary["reprice_count"] >= 1
    # Strategy creates were the reprice's bid/ask quotes (NOT a hedge).
    assert len(gw.signer_client.create_calls) >= 1
    # No emergency_stop.
    assert summary["emergency_stops"] == 0


def test_emergency_stop_still_fires_on_other_causes():
    """P8 regression-prevention: place rejects, cancel failures, and
    ws silence all still trigger ``_should_emergency_stop``.

    Removing the hedge-fails branch from emergency_stop must not
    weaken the other 3 triggers — those guard distinct production
    failures (place reject = SDK contract drift, cancel fail =
    -1.132 SKHYNIX-short class regression, ws silence = network
    partition).
    """
    # 1) place rejects above threshold
    q1, _, _, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_consecutive_reject_count": 5},
    )
    q1._stats["consecutive_rejects"] = 5
    assert q1._should_emergency_stop() is True

    # 2) cancel failures above threshold
    q2, _, _, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_consecutive_cancel_fail_count": 3},
    )
    q2._stats["consecutive_cancel_failures"] = 3
    assert q2._should_emergency_stop() is True

    # 3) ws silence above threshold
    q3, _, ws3, _, _ = _make_quoter(
        config_overrides={"emergency_stop_on_ws_disconnect_sec": 60},
    )
    ws3._last_msg_ts_ms = int(time.time() * 1000) - 120_000
    assert q3._should_emergency_stop() is True

    # 4) hedge fails: P8 — does NOT trigger emergency_stop, even
    # when the count is ostensibly above any threshold (it's been
    # reset to 0 after disable, but assert the predicate ignores
    # the field altogether).
    q4, _, _, _, _ = _make_quoter(
        config_overrides={"active_hedge_max_consecutive_fails": 3},
    )
    q4._stats["consecutive_active_hedge_fails"] = 999  # simulate stuck
    assert q4._should_emergency_stop() is False


# ----- M. Phase 2.1 P2.1.3: daily drawdown stop --------------------------


def test_daily_drawdown_breach_triggers_emergency():
    """Breach detection: drawdown >= effective_max_dd → returns True."""
    q, _, _, _, _ = _make_quoter(
        config_overrides={
            "daily_max_drawdown_usdc": Decimal("100"),
            "daily_max_drawdown_pct": Decimal("0.05"),
            "daily_drawdown_check_interval_sec": 0,  # always check
        },
    )
    q._collateral_start_usdc = Decimal("2000")
    q._latest_collateral_usdc = Decimal("1850")  # -150 drawdown
    # Effective max = min(100, 0.05*2000=100) = 100. 150 >= 100 → breach.

    async def _go() -> bool:
        return await q._maybe_check_daily_drawdown(int(time.time() * 1000))

    assert asyncio.run(_go()) is True
    assert q.get_summary()["daily_drawdown_breaches"] == 1


def test_daily_drawdown_pct_vs_usdc_takes_min():
    """Effective threshold is the tighter of usdc / pct*start."""
    q, _, _, _, _ = _make_quoter(
        config_overrides={
            "daily_max_drawdown_usdc": Decimal("500"),  # loose
            "daily_max_drawdown_pct": Decimal("0.05"),  # 5% * 2000 = 100 — tight
            "daily_drawdown_check_interval_sec": 0,
        },
    )
    q._collateral_start_usdc = Decimal("2000")
    q._latest_collateral_usdc = Decimal("1899")  # -101 drawdown

    async def _go() -> bool:
        return await q._maybe_check_daily_drawdown(int(time.time() * 1000))

    # 101 > 100 (pct cap) → breach even though 101 << 500 (usdc).
    assert asyncio.run(_go()) is True


def test_daily_drawdown_no_baseline_skips_check():
    """No collateral_start (e.g. start() couldn't reach REST) → no breach
    decision; the gate quietly returns False."""
    q, _, _, _, _ = _make_quoter(
        config_overrides={
            "daily_max_drawdown_usdc": Decimal("0.01"),  # absurdly tight
            "daily_drawdown_check_interval_sec": 0,
        },
    )
    q._collateral_start_usdc = None
    q._latest_collateral_usdc = None

    async def _go() -> bool:
        return await q._maybe_check_daily_drawdown(int(time.time() * 1000))

    assert asyncio.run(_go()) is False

