"""Unit tests for strategy.quote_planner.plan_quotes.

Coverage areas:
* Trivial guards (withdraw / spread out of band / mid <= 0)
* Inventory skew effect on bid vs ask distance
* Hard position cap drops a side
* Share-warn widens distance when our notional dominates the L1 tier
* Quantization at SKHYNIX (price_decimals=3) and a fictional SAMSUNG
  (price_decimals=2) tick grids
* BBO-aware classification (improving / passive / would_cross_market)
* Cross-self defense (rare; fired here via a distance=0 override)
* Tier judgment after quantization-induced drift
* notes field reflects the active modifiers
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategy.quote_planner import plan_quotes  # noqa: E402
from strategy.session_aware import get_kr_equity_session  # noqa: E402
from strategy.types import (  # noqa: E402
    InventoryState,
    MarketSnapshot,
    SessionPolicy,
)


# ---- helpers -----------------------------------------------------------

DEFAULT_CONFIG = {
    "target_max_delta_usdc": Decimal("500"),
    "skew_max_offset_bp": Decimal("5"),
    "hard_position_cap_usdc": Decimal("1000"),
    "min_market_spread_bp": Decimal("3"),
    "max_market_spread_bp": Decimal("100"),
    "share_warn_threshold": Decimal("0.8"),
    "share_warn_widen_bp": Decimal("5"),
}

EMPTY_INVENTORY = InventoryState(
    net_delta_base=Decimal(0),
    net_delta_usdc=Decimal(0),
    avg_entry_price=None,
    open_orders_count=0,
)


def long_inv(usdc: str, mid: str = "100") -> InventoryState:
    net = Decimal(usdc)
    return InventoryState(
        net_delta_base=net / Decimal(mid),
        net_delta_usdc=net,
        avg_entry_price=Decimal(mid),
        open_orders_count=0,
    )


def market_kr_lunch_lite(
    mid: str = "100",
    bid: str = "99.85",
    ask: str = "100.15",
    spread_bp: str = "30",
    l1_bid: str = "0",
    l1_ask: str = "0",
    price_decimals: int = 2,
    size_decimals: int = 4,
) -> MarketSnapshot:
    return MarketSnapshot(
        symbol="SAMSUNGUSD",
        market_index=999,
        mid=Decimal(mid),
        mark_price=None,
        index_price=None,
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        spread_bp=Decimal(spread_bp),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal(l1_bid), "ask_usdc": Decimal(l1_ask), "total_usdc": Decimal(l1_bid) + Decimal(l1_ask)},
            25: {"bid_usdc": Decimal(l1_bid), "ask_usdc": Decimal(l1_ask), "total_usdc": Decimal(l1_bid) + Decimal(l1_ask)},
            50: {"bid_usdc": Decimal(l1_bid), "ask_usdc": Decimal(l1_ask), "total_usdc": Decimal(l1_bid) + Decimal(l1_ask)},
        },
        price_decimals=price_decimals,
        size_decimals=size_decimals,
        ts_ms=1_700_000_000_000,
    )


def kr_market_hours_session():
    moment = datetime(2026, 4, 20, 2, 0, tzinfo=timezone.utc)  # Mon 02:00 UTC
    return get_kr_equity_session(moment)


def kr_lunch_break_session():
    moment = datetime(2026, 4, 20, 4, 30, tzinfo=timezone.utc)  # Mon 04:30 UTC
    return get_kr_equity_session(moment)


def kr_weekend_session():
    moment = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)  # Sat 12:00 UTC
    return get_kr_equity_session(moment)


# ---- 1. withdraw ------------------------------------------------------

def test_withdraw_session_returns_empty():
    session = SessionPolicy(
        name="KR_PRE_OPEN_WITHDRAW",
        action="withdraw",
        default_distance_bp=Decimal("0"),
        default_size_usdc=Decimal("0"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="withdraw window",
    )
    market = market_kr_lunch_lite(spread_bp="20", l1_bid="5000", l1_ask="5000")
    assert plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG) == []


# ---- 2. happy path ----------------------------------------------------

def test_normal_quotes_with_zero_inventory():
    """Symmetric quotes around mid, no skew, no share warn, both improving."""
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.90", ask="100.10", spread_bp="20",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    assert len(quotes) == 2

    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    # base_distance = 5bp at this session
    assert abs(bid.distance_from_mid_bp - Decimal("5")) <= Decimal("1")
    assert abs(ask.distance_from_mid_bp - Decimal("5")) <= Decimal("1")
    assert bid.market_position == "improving"
    assert ask.market_position == "improving"
    assert bid.tier_target == "L1"
    assert ask.tier_target == "L1"


# ---- 3 / 4. inventory skew --------------------------------------------

def test_long_inventory_widens_bid_only():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, long_inv("500"), DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    # base 5 + skew 5 = 10bp on bid, base 5 on ask
    assert bid.distance_from_mid_bp > ask.distance_from_mid_bp
    assert "skew_long" in bid.notes
    assert "skew_long" not in ask.notes


def test_short_inventory_widens_ask_only():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, long_inv("-500"), DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert ask.distance_from_mid_bp > bid.distance_from_mid_bp
    assert "skew_short" in ask.notes
    assert "skew_short" not in bid.notes


# ---- 5 / 6. position cap ---------------------------------------------

def test_long_above_hard_cap_drops_bid():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(spread_bp="20", l1_bid="5000", l1_ask="5000")
    quotes = plan_quotes(market, session, long_inv("1500"), DEFAULT_CONFIG)
    assert len(quotes) == 1
    assert quotes[0].side == "sell"


def test_short_below_hard_cap_drops_ask():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(spread_bp="20", l1_bid="5000", l1_ask="5000")
    quotes = plan_quotes(market, session, long_inv("-1500"), DEFAULT_CONFIG)
    assert len(quotes) == 1
    assert quotes[0].side == "buy"


# ---- 7 / 8. spread guards --------------------------------------------

def test_market_too_tight_returns_empty():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(spread_bp="1", l1_bid="5000", l1_ask="5000")
    assert plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG) == []


def test_market_too_wide_returns_empty():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(spread_bp="150", l1_bid="5000", l1_ask="5000")
    assert plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG) == []


# ---- 9 / 10. share-warn ----------------------------------------------

def test_share_warn_triggers_with_zero_l1_depth():
    session = kr_market_hours_session()  # base $1000 at 5bp
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="0", l1_ask="0",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert "share_warn" in bid.notes
    assert "share_warn" in ask.notes
    # base 5 + 5 widen = 10bp distance
    assert bid.distance_from_mid_bp >= Decimal("9.9")
    assert ask.distance_from_mid_bp >= Decimal("9.9")


def test_share_warn_does_not_trigger_with_deep_l1():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="10000", l1_ask="10000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert "share_warn" not in bid.notes
    assert "share_warn" not in ask.notes


# ---- 11. invalid mid -------------------------------------------------

def test_invalid_mid_raises():
    session = kr_market_hours_session()
    market = MarketSnapshot(
        symbol="X", market_index=0,
        mid=Decimal(0),
        mark_price=None, index_price=None,
        best_bid=Decimal("0"), best_ask=Decimal("1"),
        spread_bp=Decimal("10"),
        depth_by_spread_bp={15: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)}},
        price_decimals=2, size_decimals=4, ts_ms=0,
    )
    with pytest.raises(ValueError):
        plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)


# ---- 12. lunch break uses session distance ---------------------------

def test_lunch_break_uses_15bp_distance():
    session = kr_lunch_break_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.65", ask="100.35", spread_bp="70",
        l1_bid="10000", l1_ask="10000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert abs(bid.distance_from_mid_bp - Decimal("15")) <= Decimal("1")
    assert abs(ask.distance_from_mid_bp - Decimal("15")) <= Decimal("1")
    assert bid.tier_target == "L1"      # 15 <= 15 → L1


# ---- 13. weekend uses 30bp + RWA tiers -------------------------------

def test_weekend_uses_30bp_distance_and_weekend_tiers():
    session = kr_weekend_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.50", ask="100.50", spread_bp="100",
        l1_bid="20000", l1_ask="20000",
        price_decimals=2, size_decimals=4,
    )
    # need to populate L1=50 depth for the weekend tier lookup
    market.depth_by_spread_bp[50] = {
        "bid_usdc": Decimal("20000"),
        "ask_usdc": Decimal("20000"),
        "total_usdc": Decimal("40000"),
    }
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert abs(bid.distance_from_mid_bp - Decimal("30")) <= Decimal("1")
    assert abs(ask.distance_from_mid_bp - Decimal("30")) <= Decimal("1")
    # 30bp in weekend tiers (50/100/200) → L1
    assert bid.tier_target == "L1"
    assert ask.tier_target == "L1"


# ---- 14. SKHYNIX price_decimals=3 quantization -----------------------

def test_skhynix_price_decimals_3_quantization():
    session = kr_market_hours_session()
    market = MarketSnapshot(
        symbol="SKHYNIXUSD", market_index=161,
        mid=Decimal("894.490"),
        mark_price=Decimal("894.490"), index_price=Decimal("894.490"),
        best_bid=Decimal("894.000"), best_ask=Decimal("894.980"),
        spread_bp=Decimal("11"),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal("3000"), "ask_usdc": Decimal("3000"), "total_usdc": Decimal("6000")},
            25: {"bid_usdc": Decimal("5000"), "ask_usdc": Decimal("5000"), "total_usdc": Decimal("10000")},
            50: {"bid_usdc": Decimal("10000"), "ask_usdc": Decimal("10000"), "total_usdc": Decimal("20000")},
        },
        price_decimals=3, size_decimals=3, ts_ms=0,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    for q in quotes:
        # all quoted prices land on the 0.001 tick grid
        assert q.price == q.price.quantize(Decimal("0.001"))
        # size_base lands on 0.001 grid
        assert q.size_base == q.size_base.quantize(Decimal("0.001"))


# ---- 15. fictional SAMSUNG price_decimals=2 quantization ------------

def test_other_symbol_price_decimals_2_quantization():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="5000", l1_ask="5000",
        price_decimals=2, size_decimals=4,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    for q in quotes:
        assert q.price == q.price.quantize(Decimal("0.01"))


# ---- 16. BBO-aware: target == best_bid is passive (strict >) --------

def test_bbo_aware_target_at_best_bid_is_passive():
    """mid=100.00, distance=5bp lands target_bid exactly at best_bid=99.95."""
    session = SessionPolicy(
        name="KR_MARKET_HOURS_AM", action="quote",
        default_distance_bp=Decimal("5"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="boundary test",
    )
    market = MarketSnapshot(
        symbol="X", market_index=0,
        mid=Decimal("100.00"),
        mark_price=None, index_price=None,
        best_bid=Decimal("99.95"), best_ask=Decimal("100.05"),
        spread_bp=Decimal("10"),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal("5000"), "ask_usdc": Decimal("5000"), "total_usdc": Decimal("10000")},
        },
        price_decimals=2, size_decimals=4, ts_ms=0,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert bid.price == Decimal("99.95")
    assert ask.price == Decimal("100.05")
    # equal to best_bid → passive (strict > for improving)
    assert bid.market_position == "passive"
    assert ask.market_position == "passive"


# ---- 17. BBO-aware: target_bid above best_ask = would_cross_market --

def test_bbo_aware_target_above_best_ask_is_would_cross():
    """Oracle/BBO disagreement: mid > best_ask → target_bid would cross."""
    session = kr_market_hours_session()  # 5bp distance
    market = MarketSnapshot(
        symbol="X", market_index=0,
        mid=Decimal("100.10"),                                  # mid above BBO
        mark_price=None, index_price=None,
        best_bid=Decimal("99.95"), best_ask=Decimal("100.00"),  # both below mid
        spread_bp=Decimal("5"),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal("5000"), "ask_usdc": Decimal("5000"), "total_usdc": Decimal("10000")},
        },
        price_decimals=2, size_decimals=4, ts_ms=0,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    # target_bid = 100.10 * 0.9995 = 100.04995 → ROUND_DOWN → 100.04 ≥ best_ask 100.00
    assert bid.market_position == "would_cross_market"
    # forced down to best_ask - tick = 99.99
    assert bid.price == Decimal("99.99")
    assert "crossed_market" in bid.notes


# ---- 18. cross-self defense: distance=0 forces overlap, gets widened

def test_cross_self_defense_keeps_at_least_one_tick():
    """A distance=0 override forces target_bid == target_ask == mid.

    Both classify as 'improving' (between BBO), then cross-self defense
    splits them across the tick grid so the final spread is >= 1 tick.
    """
    session = SessionPolicy(
        name="ZERO", action="quote",
        default_distance_bp=Decimal("0"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="cross-self stress test",
    )
    market = MarketSnapshot(
        symbol="X", market_index=0,
        mid=Decimal("100.00"),
        mark_price=None, index_price=None,
        best_bid=Decimal("99.95"), best_ask=Decimal("100.05"),
        spread_bp=Decimal("10"),                                 # comfortably above min
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal("5000"), "ask_usdc": Decimal("5000"), "total_usdc": Decimal("10000")},
        },
        price_decimals=2, size_decimals=4, ts_ms=0,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    tick = Decimal("0.01")
    assert ask.price - bid.price >= tick


# ---- 19. tier judgement after rounding drift -----------------------

def test_tier_judgment_l1_after_quantization_drift():
    """distance=6bp + small price-grid drift still resolves to L1 (≤15)."""
    session = SessionPolicy(
        name="TEST", action="quote",
        default_distance_bp=Decimal("6"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="tier judgment",
    )
    market = MarketSnapshot(
        symbol="X", market_index=0,
        mid=Decimal("100.05"),
        mark_price=None, index_price=None,
        best_bid=Decimal("99.85"), best_ask=Decimal("100.25"),
        spread_bp=Decimal("40"),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal("10000"), "ask_usdc": Decimal("10000"), "total_usdc": Decimal("20000")},
        },
        price_decimals=2, size_decimals=4, ts_ms=0,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    for q in quotes:
        assert q.tier_target == "L1"
        # actual distance is ≈ 7bp due to price_decimals=2 rounding away from mid
        assert q.distance_from_mid_bp <= Decimal("15")
        assert q.distance_from_mid_bp >= Decimal("6")


# ---- 20. notes flags ----------------------------------------------

def test_notes_includes_share_warn_and_skew_when_active():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.85", ask="100.15", spread_bp="30",
        l1_bid="0", l1_ask="0",
    )
    quotes = plan_quotes(market, session, long_inv("500"), DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert "share_warn" in bid.notes
    assert "skew_long" in bid.notes
    assert "share_warn" in ask.notes
    # skew_short should NOT be in ask side (we are long, not short)
    assert "skew_short" not in ask.notes


# ---- bonus: tier OUT when distance escapes L3 --------------------

def test_tier_target_out_when_distance_above_l3():
    session = SessionPolicy(
        name="TEST", action="quote",
        default_distance_bp=Decimal("60"),                 # past L3=50
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="tier OUT",
    )
    market = market_kr_lunch_lite(
        mid="100", bid="99.30", ask="100.70", spread_bp="80",
        l1_bid="20000", l1_ask="20000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    for q in quotes:
        assert q.tier_target == "OUT"


# ---- bonus: size_base > 0 invariant ------------------------------

def test_quote_size_base_always_positive():
    session = kr_market_hours_session()
    market = market_kr_lunch_lite(
        mid="894.490", bid="894.0", ask="894.98", spread_bp="11",
        l1_bid="3000", l1_ask="3000", price_decimals=3, size_decimals=3,
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    assert len(quotes) == 2
    for q in quotes:
        assert q.size_base > 0
        assert q.size_usdc > 0


# ---- log diagnostics — guard against future refactors stripping them ----


def test_plan_quotes_logs_empty_reason_when_spread_too_tight(caplog):
    """``plan_quotes`` must log the reason whenever it returns ``[]``.

    Forensic value: a 4-min live run that produces zero quotes is
    unrecoverable from the JSONL alone if the planner returned silently.
    The log line ties the empty result to the actual gating condition.
    """
    import logging  # local: avoid touching module-level imports
    caplog.set_level(logging.INFO, logger="strategy.quote_planner")
    session = kr_market_hours_session()
    # Spread 1bp << min (3bp by default) → empty result + log line
    market = market_kr_lunch_lite(
        spread_bp="1", l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    assert quotes == []
    assert "market_spread" in caplog.text
    assert "min" in caplog.text
