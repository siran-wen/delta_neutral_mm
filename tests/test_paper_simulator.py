"""Unit tests for ``strategy.paper_simulator.PaperSimulator``.

Coverage:

* place/cancel id roundtrip and uniqueness
* cancel returns True/False as documented
* inventory starts flat
* buy / sell fill updates inventory + avg_entry_price
* fill_probability=0 → no fills even with many ticks
* fill_probability=1 → fills only ``improving`` orders, not ``passive`` /
  ``would_cross_market``
* get_inventory_state returns the expected dataclass shape
* weighted-average entry recalculates on each add-to-position fill
* mark_price arg in get_inventory_state changes net_delta_usdc
* cancel_all empties the active set
* partial close keeps the average; full close clears it
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategy.paper_simulator import PaperSimulator  # noqa: E402
from strategy.types import InventoryState, MarketSnapshot  # noqa: E402


# ---- helpers ------------------------------------------------------------


def _market(mid: str = "100", ts_ms: int = 1_700_000_000_000) -> MarketSnapshot:
    m = Decimal(mid)
    return MarketSnapshot(
        symbol="SKHYNIXUSD",
        market_index=161,
        mid=m,
        mark_price=m,
        index_price=m,
        best_bid=m - Decimal("0.05"),
        best_ask=m + Decimal("0.05"),
        spread_bp=Decimal("10"),
        depth_by_spread_bp={},
        price_decimals=3,
        size_decimals=3,
        ts_ms=ts_ms,
    )


def _force_fill(sim: PaperSimulator, oid: str, market: MarketSnapshot, ts_ms: int) -> dict:
    """Manually trigger ``_apply_fill`` for one resting order.

    Use this when testing ``_apply_fill`` semantics directly, instead
    of relying on the probabilistic ``tick`` path.
    """
    order = sim.active_orders.pop(oid)
    fill = {
        "client_order_id": oid,
        "side": order["side"],
        "price": order["price"],
        "size_base": order["size_base"],
        "size_usdc": order["size_usdc"],
        "ts_ms": ts_ms,
        "market_index": market.market_index,
        "symbol": market.symbol,
    }
    sim.fills.append(fill)
    sim._apply_fill(fill)
    return fill


# ---- placement / cancellation -------------------------------------------


def test_place_order_returns_unique_ids():
    sim = PaperSimulator(random_seed=0)
    oid1 = sim.place_order("buy", Decimal("100"), Decimal("1"))
    oid2 = sim.place_order("sell", Decimal("101"), Decimal("1"))
    assert oid1 != oid2
    assert oid1 in sim.active_orders
    assert oid2 in sim.active_orders


def test_cancel_order_returns_true_for_existing():
    sim = PaperSimulator(random_seed=0)
    oid = sim.place_order("buy", Decimal("100"), Decimal("1"))
    assert sim.cancel_order(oid) is True
    assert oid not in sim.active_orders


def test_cancel_order_returns_false_for_nonexistent():
    sim = PaperSimulator(random_seed=0)
    assert sim.cancel_order("PAPER-999999") is False


def test_cancel_all_empties_active_set():
    sim = PaperSimulator(random_seed=0)
    sim.place_order("buy", Decimal("100"), Decimal("1"))
    sim.place_order("sell", Decimal("101"), Decimal("1"))
    n = sim.cancel_all()
    assert n == 2
    assert sim.active_orders == {}


# ---- inventory ----------------------------------------------------------


def test_inventory_starts_flat():
    sim = PaperSimulator(random_seed=0)
    inv = sim.get_inventory_state()
    assert isinstance(inv, InventoryState)
    assert inv.net_delta_base == Decimal(0)
    assert inv.net_delta_usdc == Decimal(0)
    assert inv.avg_entry_price is None
    assert inv.open_orders_count == 0


def test_buy_fill_updates_inventory_long():
    sim = PaperSimulator(random_seed=0)
    market = _market()
    oid = sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid, market, ts_ms=market.ts_ms)
    inv = sim.get_inventory_state()
    assert inv.net_delta_base == Decimal("1")
    assert inv.avg_entry_price == Decimal("100")
    assert inv.open_orders_count == 0


def test_sell_fill_updates_inventory_short():
    sim = PaperSimulator(random_seed=0)
    market = _market()
    oid = sim.place_order("sell", Decimal("100"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid, market, ts_ms=market.ts_ms)
    inv = sim.get_inventory_state()
    assert inv.net_delta_base == Decimal("-1")
    assert inv.avg_entry_price == Decimal("100")


def test_avg_price_recomputes_on_add_to_long():
    """Two consecutive longs at different prices ⇒ weighted avg."""
    sim = PaperSimulator(random_seed=0)
    market = _market()
    oid1 = sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid1, market, ts_ms=market.ts_ms)
    oid2 = sim.place_order("buy", Decimal("110"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid2, market, ts_ms=market.ts_ms)
    inv = sim.get_inventory_state()
    assert inv.net_delta_base == Decimal("2")
    assert inv.avg_entry_price == Decimal("105")


def test_full_close_clears_avg_price():
    sim = PaperSimulator(random_seed=0)
    market = _market()
    oid_buy = sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid_buy, market, ts_ms=market.ts_ms)
    oid_sell = sim.place_order("sell", Decimal("110"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid_sell, market, ts_ms=market.ts_ms)
    inv = sim.get_inventory_state()
    assert inv.net_delta_base == Decimal("0")
    assert inv.avg_entry_price is None


def test_partial_close_keeps_avg_price():
    sim = PaperSimulator(random_seed=0)
    market = _market()
    # Build long 2 @ 100
    oid1 = sim.place_order("buy", Decimal("100"), Decimal("2"), market_position="improving")
    _force_fill(sim, oid1, market, ts_ms=market.ts_ms)
    assert sim.inventory_avg_price == Decimal("100")
    # Sell 1 at unfavorable price; remaining 1 still cost-basis 100
    oid2 = sim.place_order("sell", Decimal("90"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid2, market, ts_ms=market.ts_ms)
    inv = sim.get_inventory_state()
    assert inv.net_delta_base == Decimal("1")
    assert inv.avg_entry_price == Decimal("100")  # cost basis preserved


def test_get_inventory_state_uses_mark_price_for_usdc():
    sim = PaperSimulator(random_seed=0)
    market = _market()
    oid = sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    _force_fill(sim, oid, market, ts_ms=market.ts_ms)
    inv_at_avg = sim.get_inventory_state()
    inv_at_mark = sim.get_inventory_state(mark_price=Decimal("110"))
    assert inv_at_avg.net_delta_usdc == Decimal("100")  # 1 * 100 (avg)
    assert inv_at_mark.net_delta_usdc == Decimal("110")  # 1 * 110 (mark)


# ---- fill engine --------------------------------------------------------


def test_fill_probability_zero_produces_no_fills_over_many_ticks():
    sim = PaperSimulator(
        fill_probability_per_60s=Decimal("0"),
        random_seed=42,
    )
    market = _market()
    sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    for i in range(1000):
        sim.tick(market, ts_ms=market.ts_ms + i * 1000)
    assert sim.fills == []
    assert sim.inventory_base == Decimal("0")


def test_passive_orders_are_not_eligible_for_fill():
    """Even with probability=1, passive / would-cross orders never fill."""
    sim = PaperSimulator(
        fill_probability_per_60s=Decimal("1"),
        random_seed=0,
    )
    market = _market()
    sim.place_order("buy", Decimal("99"), Decimal("1"), market_position="passive")
    sim.place_order("sell", Decimal("101"), Decimal("1"), market_position="would_cross_market")
    fills_total = []
    for i in range(50):
        fills_total.extend(sim.tick(market, ts_ms=market.ts_ms + i * 1000))
    assert fills_total == []
    assert len(sim.active_orders) == 2  # nothing was hit


def test_improving_order_eventually_fills_with_high_probability():
    sim = PaperSimulator(
        fill_probability_per_60s=Decimal("1"),
        random_seed=0,
    )
    market = _market()
    sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    fills_total = []
    for i in range(5):
        fills_total.extend(sim.tick(market, ts_ms=market.ts_ms + i * 1000))
    # With per_tick_p=1.0, the very first tick should fire.
    assert len(fills_total) == 1
    assert sim.inventory_base == Decimal("1")
    assert sim.active_orders == {}


def test_seeded_runs_are_reproducible():
    """Two simulators with the same seed produce the same fill sequence."""
    def _run(seed: int) -> list:
        sim = PaperSimulator(
            fill_probability_per_60s=Decimal("0.5"),
            random_seed=seed,
        )
        market = _market()
        sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
        sim.place_order("sell", Decimal("101"), Decimal("1"), market_position="improving")
        fills = []
        for i in range(20):
            fills.extend(sim.tick(market, ts_ms=market.ts_ms + i * 1000))
        return [(f["side"], str(f["price"])) for f in fills]

    assert _run(seed=12345) == _run(seed=12345)


def test_open_orders_count_reflects_active_set():
    sim = PaperSimulator(random_seed=0)
    sim.place_order("buy", Decimal("100"), Decimal("1"), market_position="improving")
    sim.place_order("sell", Decimal("101"), Decimal("1"), market_position="improving")
    inv = sim.get_inventory_state()
    assert inv.open_orders_count == 2
