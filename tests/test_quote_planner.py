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
    # Cap is $2k so the post-fill defensive check
    # (``is_position_capped`` projects inv + size_per_side) does NOT
    # trip on the non-cap-focused tests in this file. The KR session
    # default size is $1k; with cap=$1k those tests' EMPTY_INVENTORY
    # would now project to exactly the cap and skip both sides.
    # The two cap-focused tests below explicitly use inv = ±$1500 so
    # the projected inv (±$2500) still breaches the bumped cap.
    "hard_position_cap_usdc": Decimal("2000"),
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


# ---- Phase 3: BBO-aware dynamic distance ---------------------------


def _bbo_track_config(mode: str = "passive", max_dist_bp: int = 5) -> dict:
    """DEFAULT_CONFIG + bbo_track keys."""
    cfg = dict(DEFAULT_CONFIG)
    cfg["bbo_track_mode"] = mode
    cfg["bbo_track_max_distance_bp"] = max_dist_bp
    return cfg


def _wide_distance_session() -> SessionPolicy:
    """30 bp default distance — the KR_OVERNIGHT setting that motivated
    the BBO-track feature. Far outside a tight 10 bp BBO."""
    return SessionPolicy(
        name="KR_OVERNIGHT_TEST", action="quote",
        default_distance_bp=Decimal("30"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="bbo track unit test",
    )


def test_bbo_track_passive_pulls_bid_to_bbo():
    """Passive-mode pulls a bid sitting >max_distance below BBO up to BBO.

    mid=100, best_bid=99.95, default_distance=30 bp →
    target_bid = 99.70. max_distance_bp=5 → bid_floor = 99.90.
    99.70 < 99.90, so the planner replaces actual_bid with best_bid
    (99.95) — joining the BBO queue rather than resting 25 bp behind.
    """
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, _bbo_track_config())
    bid = next(q for q in quotes if q.side == "buy")
    assert bid.price == Decimal("99.95")


def test_bbo_track_passive_keeps_close_bid():
    """A bid already inside max_distance of BBO is left untouched.

    With default_distance=3 bp, target_bid = 99.97 — only 2 bp behind
    the 99.95 BBO. max_distance=5 bp → bid_floor = 99.90; 99.97 is
    above the floor so no adjustment.

    Note: best_bid=99.95 against target=99.97 means the BBO-aware
    block tags this as "improving" (target > best_bid). passive
    bbo-track does not down-adjust improving prices.
    """
    session = SessionPolicy(
        name="TIGHT", action="quote",
        default_distance_bp=Decimal("3"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="bbo close-to-bbo test",
    )
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, _bbo_track_config())
    bid = next(q for q in quotes if q.side == "buy")
    # target_bid = 100 * (1 - 3/10000) = 99.97 → ROUND_DOWN → 99.97
    assert bid.price == Decimal("99.97")


def test_bbo_track_off_preserves_static_distance():
    """mode="off" gives the legacy fixed-distance behaviour exactly."""
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(
        market, session, EMPTY_INVENTORY, _bbo_track_config(mode="off")
    )
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    # Default 30 bp distance, no BBO-tracking → quotes at mid ± 30bp.
    # ROUND_DOWN on bid, ROUND_UP on ask, price_decimals=2.
    assert bid.price == Decimal("99.70")
    assert ask.price == Decimal("100.30")


def test_bbo_track_default_off_when_key_absent():
    """When bbo_track_mode is missing from config, the planner behaves
    exactly as it did before this feature shipped."""
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, DEFAULT_CONFIG)
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert bid.price == Decimal("99.70")
    assert ask.price == Decimal("100.30")


def test_bbo_track_passive_pulls_ask_to_bbo():
    """Symmetric to the bid test: ask sitting too far above BBO is
    pulled down onto best_ask."""
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(market, session, EMPTY_INVENTORY, _bbo_track_config())
    ask = next(q for q in quotes if q.side == "sell")
    # target_ask = 100.30, ceiling = 100.05 + 0.05 = 100.10, pull to 100.05.
    assert ask.price == Decimal("100.05")


def test_bbo_track_passive_with_skew_long_inventory():
    """Skew + BBO-track compose: a long inventory pushes the bid further
    out (skew), which only makes BBO-track's pull more relevant.

    inv=$500 long → max skew offset (5 bp on bid). default_distance=30
    bp + 5 bp skew = 35 bp → target_bid = 99.65, well below the 99.90
    floor → pull to 99.95. Ask unchanged by skew (skew_factor>0 only
    pushes the side that grows the position) — pulled to BBO 100.05.
    """
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(
        market, session, long_inv("500"), _bbo_track_config()
    )
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert bid.price == Decimal("99.95")
    assert ask.price == Decimal("100.05")


def test_bbo_track_unknown_mode_is_noop():
    """Defensive: a typo'd mode value silently falls through (off-equivalent)
    rather than crashing the planner mid-tick."""
    session = _wide_distance_session()
    market = market_kr_lunch_lite(
        mid="100", bid="99.95", ask="100.05", spread_bp="10",
        l1_bid="5000", l1_ask="5000",
    )
    quotes = plan_quotes(
        market, session, EMPTY_INVENTORY, _bbo_track_config(mode="aggressive")
    )
    bid = next(q for q in quotes if q.side == "buy")
    ask = next(q for q in quotes if q.side == "sell")
    assert bid.price == Decimal("99.70")
    assert ask.price == Decimal("100.30")


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


# ----- P9 (5-4): asymmetric BBO quoting ---------------------------------
#
# When abs(inv) breaches asymmetric_quote_trigger_pct * effective_cap,
# plan_quotes overrides both legs: close leg sits aggressively at the
# BBO (sell at best_bid+tick when long; buy at best_ask-tick when
# short), anti leg widens to ``asymmetric_anti_distance_bp`` from
# mid. Net: keep both legs post_only, no IOC, no fail counter, no
# disable state. Replaces the P6/P7/P8 active_hedge paths.


def _asym_market(
    mid: str = "160",
    bid: str = "159.96",
    ask: str = "160.04",
    *,
    price_decimals: int = 3,
    size_decimals: int = 3,
) -> MarketSnapshot:
    """SAMSUNG-shaped market for the asymmetric tests; tick = 0.001 at
    price_decimals=3 keeps the BBO+1tick math unambiguous."""
    return MarketSnapshot(
        symbol="SAMSUNGUSD",
        market_index=161,
        mid=Decimal(mid),
        mark_price=None,
        index_price=None,
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        spread_bp=(Decimal(ask) - Decimal(bid)) / Decimal(mid) * Decimal(10000),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            25: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            50: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
        },
        price_decimals=price_decimals,
        size_decimals=size_decimals,
        ts_ms=1_700_000_000_000,
    )


def _asym_session() -> SessionPolicy:
    """Generic SessionPolicy for the asymmetric tests — small size and
    a 10bp default distance so the asymmetric overrides are obvious."""
    return SessionPolicy(
        name="ASYM_TEST",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=Decimal("50"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="test",
    )


def _asym_config(**overrides) -> dict:
    """Default-tuned config for asymmetric tests. cap=$300 + trigger
    pct 0.7 → trigger threshold $210."""
    cfg = {
        "target_max_delta_usdc": Decimal("300"),
        "skew_max_offset_bp": Decimal("5"),
        "hard_position_cap_usdc": Decimal("300"),
        "min_market_spread_bp": Decimal("3"),
        "max_market_spread_bp": Decimal("100"),
        "share_warn_threshold": Decimal("0.95"),
        "share_warn_widen_bp": Decimal("5"),
        "asymmetric_quote_enabled": True,
        "asymmetric_quote_trigger_pct": Decimal("0.7"),
        "asymmetric_close_mode": "improve_bbo",
        "asymmetric_anti_distance_bp": Decimal("30"),
    }
    cfg.update(overrides)
    return cfg


def test_asymmetric_quoting_inv_below_trigger_stays_symmetric():
    """Below trigger ($210 = $300 × 0.7), plan_quotes runs the normal
    symmetric path. The asymmetric_* notes must NOT appear."""
    market = _asym_market()
    inv = long_inv("200", mid="160")  # inv=$200 < trigger $210
    quotes = plan_quotes(market, _asym_session(), inv, _asym_config())
    assert len(quotes) == 2
    for q in quotes:
        assert "asymmetric_close" not in q.notes
        assert "asymmetric_anti" not in q.notes


def test_asymmetric_quoting_inv_breaches_long():
    """Long inv above trigger → sell leg = best_bid + 1 tick (close
    leg, queue priority); buy leg = mid - 30bp (anti leg, wider than
    the symmetric 10bp). Both legs are tagged."""
    market = _asym_market()  # bid=159.96 ask=160.04 mid=160 tick=0.001
    inv = long_inv("220", mid="160")  # > trigger $210
    quotes = plan_quotes(market, _asym_session(), inv, _asym_config())
    assert len(quotes) == 2
    by_side = {q.side: q for q in quotes}
    sell, buy = by_side["sell"], by_side["buy"]
    # Close leg: sell at best_bid + 1 tick = 159.961
    assert sell.price == Decimal("159.961")
    assert "asymmetric_close" in sell.notes
    # Anti leg: buy at mid * (1 - 30/10000) = 160 * 0.997 = 159.520
    assert buy.price == Decimal("159.520")
    assert "asymmetric_anti" in buy.notes
    # Sanity: close leg is between best_bid and best_ask (post_only valid)
    assert sell.price > market.best_bid
    assert sell.price < market.best_ask


def test_asymmetric_quoting_inv_breaches_short():
    """Short inv below -trigger → buy leg = best_ask - 1 tick; sell
    leg = mid + 30bp."""
    market = _asym_market()
    # net_delta_usdc = -$220
    inv = InventoryState(
        net_delta_base=Decimal("-1.375"),
        net_delta_usdc=Decimal("-220"),
        avg_entry_price=Decimal("160"),
        open_orders_count=0,
    )
    quotes = plan_quotes(market, _asym_session(), inv, _asym_config())
    assert len(quotes) == 2
    by_side = {q.side: q for q in quotes}
    sell, buy = by_side["sell"], by_side["buy"]
    # Close leg: buy at best_ask - 1 tick = 160.039
    assert buy.price == Decimal("160.039")
    assert "asymmetric_close" in buy.notes
    # Anti leg: sell at mid * (1 + 30/10000) = 160 * 1.003 = 160.480
    assert sell.price == Decimal("160.480")
    assert "asymmetric_anti" in sell.notes


def test_asymmetric_quoting_match_bbo_mode():
    """``asymmetric_close_mode=match_bbo`` puts the close leg AT the
    existing BBO (joining the queue at the back) instead of improving."""
    market = _asym_market()
    inv = long_inv("220", mid="160")
    quotes = plan_quotes(
        market, _asym_session(), inv,
        _asym_config(asymmetric_close_mode="match_bbo"),
    )
    by_side = {q.side: q for q in quotes}
    # Sell leg matches best_ask exactly.
    assert by_side["sell"].price == market.best_ask
    # Buy anti leg unchanged from improve_bbo case.
    assert by_side["buy"].price == Decimal("159.520")


def test_asymmetric_quoting_disabled_falls_back_to_symmetric():
    """``asymmetric_quote_enabled=False`` → asymmetric notes never
    appear and prices come from the symmetric path even when inv is
    above the would-be trigger."""
    market = _asym_market()
    inv = long_inv("220", mid="160")
    quotes = plan_quotes(
        market, _asym_session(), inv,
        _asym_config(asymmetric_quote_enabled=False),
    )
    for q in quotes:
        assert "asymmetric_close" not in q.notes
        assert "asymmetric_anti" not in q.notes


def test_asymmetric_quoting_no_fail_no_disable_state():
    """P9 invariant: plan_quotes never raises and never returns []
    purely because of asymmetric mode. Repeated calls with breached
    inventory produce identical asymmetric output (no implicit
    state, no fail counter)."""
    market = _asym_market()
    inv = long_inv("220", mid="160")
    cfg = _asym_config()
    quotes_a = plan_quotes(market, _asym_session(), inv, cfg)
    quotes_b = plan_quotes(market, _asym_session(), inv, cfg)
    quotes_c = plan_quotes(market, _asym_session(), inv, cfg)
    # Three identical asymmetric outputs — no decay, no disable.
    assert len(quotes_a) == 2
    assert len(quotes_b) == 2
    assert len(quotes_c) == 2
    for a, b, c in zip(quotes_a, quotes_b, quotes_c):
        assert a.price == b.price == c.price
        assert a.side == b.side == c.side


def test_asymmetric_quoting_recovers_after_inv_drops():
    """When inv falls back below trigger, plan_quotes returns to
    symmetric mode automatically — the trigger is purely a
    threshold check on the live inv, no hysteresis state."""
    market = _asym_market()
    cfg = _asym_config()
    sess = _asym_session()
    # Above trigger: asymmetric.
    breached = plan_quotes(market, sess, long_inv("220", mid="160"), cfg)
    assert any("asymmetric_close" in q.notes for q in breached)
    # Below trigger: symmetric.
    recovered = plan_quotes(market, sess, long_inv("100", mid="160"), cfg)
    for q in recovered:
        assert "asymmetric_close" not in q.notes
        assert "asymmetric_anti" not in q.notes


def test_asymmetric_quoting_respects_pct_cap_trigger():
    """When ``hard_position_cap_pct`` × collateral is the binding
    cap (smaller than the absolute), the asymmetric trigger uses
    the pct-derived cap, not the absolute. Mirrors the
    ``is_position_capped`` effective-cap logic."""
    market = _asym_market()
    inv = long_inv("85", mid="160")  # $85
    cfg = _asym_config(
        hard_position_cap_usdc=Decimal("300"),  # absolute = 300
        hard_position_cap_pct=Decimal("0.05"),  # pct cap = 0.05*1000 = 50
    )
    # collateral=1000, pct cap=50 (binds), trigger pct=0.7 → threshold=$35.
    # inv=$85 > $35 → asymmetric should fire.
    quotes = plan_quotes(market, _asym_session(), inv, cfg, collateral_usdc=Decimal("1000"))
    assert any("asymmetric_close" in q.notes for q in quotes)


# ----- P10 (5-5): inv-aware close size scaling ------------------------
#
# When asymmetric mode is active, the close-leg notional scales
# linearly from ``default_size_usdc`` at the trigger threshold up to
# ``effective_cap`` when inv touches the cap. The anti leg stays at
# ``default_size_usdc``. ``"fixed"`` restores the P9 behaviour where
# both legs use ``default_size_usdc``.
#
# Test config: cap=$400, default_size=$100, trigger_pct=0.7 →
# trigger=$280. ``cap_room = $120``. SAMSUNG-shaped market gives
# tick=0.001 and close-leg price (long inv, improve_bbo) = best_bid
# + tick = 159.961.


def _p10_session() -> SessionPolicy:
    """SessionPolicy mirroring the P10 spec example: $100 default size."""
    return SessionPolicy(
        name="P10_TEST",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=Decimal("100"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="p10 test",
    )


def _p10_config(**overrides) -> dict:
    """Default-tuned config for P10 tests. cap=$400 + trigger pct 0.7
    → trigger threshold $280, cap_room=$120."""
    cfg = {
        "target_max_delta_usdc": Decimal("400"),
        "skew_max_offset_bp": Decimal("5"),
        "hard_position_cap_usdc": Decimal("400"),
        "min_market_spread_bp": Decimal("3"),
        "max_market_spread_bp": Decimal("100"),
        "share_warn_threshold": Decimal("0.95"),
        "share_warn_widen_bp": Decimal("5"),
        "asymmetric_quote_enabled": True,
        "asymmetric_quote_trigger_pct": Decimal("0.7"),
        "asymmetric_close_mode": "improve_bbo",
        "asymmetric_anti_distance_bp": Decimal("30"),
        "asymmetric_close_size_scaling": "linear",
    }
    cfg.update(overrides)
    return cfg


def _expected_size_base(usdc: Decimal, price: Decimal, size_decimals: int = 3) -> Decimal:
    """Replicate the planner's size_base quantization (ROUND_DOWN to
    ``size_decimals``)."""
    from decimal import ROUND_DOWN as _RD
    step = Decimal(1) / (Decimal(10) ** size_decimals)
    return (usdc / price).quantize(step, rounding=_RD)


def _p10_market_or_default() -> MarketSnapshot:
    """SAMSUNG-shaped market for the P10 tests. Identical to
    ``_asym_market`` — kept as a P10-named alias to make the test
    coupling explicit."""
    return _asym_market()


def test_asymmetric_close_size_at_trigger_uses_default():
    """At inv == trigger, scale=0 → close = default_size = $100, anti
    = default = $100. Both legs sized identically (same as P9)."""
    market = _p10_market_or_default()
    inv = long_inv("280", mid="160")  # = trigger
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    sell, buy = by_side["sell"], by_side["buy"]
    # Long inv → sell is close, buy is anti. Both at $100.
    assert sell.size_base == _expected_size_base(Decimal("100"), sell.price)
    assert buy.size_base == _expected_size_base(Decimal("100"), buy.price)


def test_asymmetric_close_size_at_midway():
    """inv halfway between trigger and cap → scale=0.5 → close =
    $100 + ($400-$100)*0.5 = $250.

    Note on the anti leg: at inv=$340 with default=$100 and cap=$400,
    ``is_position_capped`` projects post-buy-fill inv at $440 ≥ $400,
    so the anti (buy) leg is correctly skipped by the cap-projection
    safety. The close leg is what P10 changes — and it sizes to $250
    here rather than P9's flat $100, draining the position ~2.5×
    faster per fill.
    """
    market = _p10_market_or_default()
    inv = long_inv("340", mid="160")  # midway: (340-280)/(400-280) = 0.5
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    # Cap-projection drops the buy (anti) leg; sell (close) emits.
    assert "sell" in by_side
    sell = by_side["sell"]
    assert sell.size_base == _expected_size_base(Decimal("250"), sell.price)
    assert "asymmetric_close" in sell.notes


def test_asymmetric_close_size_at_cap():
    """inv just under cap → scale ≈ 1 → close ≈ cap = $400.
    (At inv=$398, scale=(398-280)/(400-280)=0.9833,
    close = $100 + $300×0.9833 = $395.)

    Single-sided here too: $398 + $100 ≥ $400 trips the cap projection
    on the anti leg. The close leg's $395 notional is the P10 payoff —
    one fill could land inv at ~$3, far below the trigger.
    """
    market = _p10_market_or_default()
    inv = long_inv("398", mid="160")
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side
    sell = by_side["sell"]
    assert sell.size_base == _expected_size_base(Decimal("395"), sell.price)
    assert "asymmetric_close" in sell.notes


def test_asymmetric_close_anti_both_legs_when_under_cap_projection():
    """At inv just past the trigger (and below cap - default), both
    legs emit: close scales tiny over default, anti stays at default.

    Verifies that P10 keeps the P9 two-leg invariant intact when the
    cap-projection isn't tripping. inv=$285 → scale=5/120=0.0417,
    close = $100 + $300×0.0417 = $112.50. Anti = $100.
    Cap projection: $285 + $100 = $385 < $400 → both legs.
    """
    market = _p10_market_or_default()
    inv = long_inv("285", mid="160")
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side and "buy" in by_side
    sell, buy = by_side["sell"], by_side["buy"]
    assert sell.size_base == _expected_size_base(Decimal("112.5"), sell.price)
    assert buy.size_base == _expected_size_base(Decimal("100"), buy.price)
    assert "asymmetric_close" in sell.notes
    assert "asymmetric_anti" in buy.notes


def test_asymmetric_close_size_at_full_cap_clamps_to_cap():
    """inv = cap exactly → scale = 1 → close = cap = $400 = inv;
    ``min(close, inv_abs)`` is a no-op here. Verifies the
    integer-cap boundary."""
    market = _p10_market_or_default()
    inv = long_inv("400", mid="160")  # at cap
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    # Long inv ≥ cap may also drop the bid via is_position_capped
    # ($400 + $100 ≥ $400 → True). So we may see only the sell quote.
    assert "sell" in by_side
    sell = by_side["sell"]
    # close size = cap = $400 (scale=1)
    assert sell.size_base == _expected_size_base(Decimal("400"), sell.price)


def test_asymmetric_close_size_does_not_exceed_inv():
    """Defensive: when ``default_size_usdc`` > the live inventory
    (rare misconfig: small cap, large default), the linear formula
    can produce close > inv. ``min(close, inv_abs)`` clamps it.

    Construct: default=$300, cap=$400, trigger=$280, inv=$280.
    scale=0 → linear=$300, but inv_abs=$280, so close clamps to $280.
    """
    market = _p10_market_or_default()
    sess = SessionPolicy(
        name="P10_BIG_DEFAULT",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=Decimal("300"),  # > trigger $280
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="p10 clamp test",
    )
    inv = long_inv("280", mid="160")
    cfg = _p10_config()
    quotes = plan_quotes(market, sess, inv, cfg)
    by_side = {q.side: q for q in quotes}
    # If the cap-projection drops the bid, only sell remains.
    assert "sell" in by_side
    sell = by_side["sell"]
    # close size clamped from $300 to inv_abs=$280
    assert sell.size_base == _expected_size_base(Decimal("280"), sell.price)


def test_asymmetric_close_size_short_side():
    """Mirror of the long midway test on the short side. inv=-$340 →
    buy is close, sized to $250 by the linear scaling. The sell-side
    anti leg gets dropped by the cap-projection ($-340 - $100 =
    $-440 ≤ -$400)."""
    market = _p10_market_or_default()
    inv = InventoryState(
        net_delta_base=Decimal("-340") / Decimal("160"),
        net_delta_usdc=Decimal("-340"),
        avg_entry_price=Decimal("160"),
        open_orders_count=0,
    )
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    assert "buy" in by_side
    buy = by_side["buy"]
    # Short → buy=close ($250).
    assert buy.size_base == _expected_size_base(Decimal("250"), buy.price)
    assert "asymmetric_close" in buy.notes


def test_asymmetric_size_scaling_fixed_falls_back_to_default():
    """``asymmetric_close_size_scaling="fixed"`` restores the P9
    behaviour: both legs use the session default size regardless of
    inv severity."""
    market = _p10_market_or_default()
    inv = long_inv("390", mid="160")  # very high inv
    cfg = _p10_config(asymmetric_close_size_scaling="fixed")
    quotes = plan_quotes(market, _p10_session(), inv, cfg)
    by_side = {q.side: q for q in quotes}
    # Long inv near cap may drop the bid via is_position_capped
    # ($390 + $100 = $490 > $400 cap). Sell still emits at default.
    assert "sell" in by_side
    sell = by_side["sell"]
    # Fixed mode → close uses default $100 even at inv=$390.
    assert sell.size_base == _expected_size_base(Decimal("100"), sell.price)


def test_asymmetric_size_scaling_default_value_is_linear():
    """When the config omits ``asymmetric_close_size_scaling``, the
    planner defaults to ``"linear"`` — sizes match the midway test."""
    market = _p10_market_or_default()
    inv = long_inv("340", mid="160")  # midway
    cfg = _p10_config()
    del cfg["asymmetric_close_size_scaling"]  # absent → default linear
    quotes = plan_quotes(market, _p10_session(), inv, cfg)
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side
    sell = by_side["sell"]
    # Same expectation as the explicit linear midway test.
    assert sell.size_base == _expected_size_base(Decimal("250"), sell.price)


def test_asymmetric_size_scaling_does_not_affect_symmetric_path():
    """When inv is below the trigger, the asymmetric block doesn't
    run — both legs use the session default size, untouched by the
    P10 scaling logic."""
    market = _p10_market_or_default()
    inv = long_inv("200", mid="160")  # below trigger $280
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    sell, buy = by_side["sell"], by_side["buy"]
    # Symmetric path: both legs at session default $100.
    assert sell.size_base == _expected_size_base(Decimal("100"), sell.price)
    assert buy.size_base == _expected_size_base(Decimal("100"), buy.price)


def test_asymmetric_size_scaling_preserves_close_price_invariant():
    """P10 must not affect price — only size. The close leg still
    sits at best_bid + 1 tick (improve_bbo) and the anti leg still
    sits at mid - asymmetric_anti_distance_bp.

    Uses inv=$285 (just past trigger) where the cap-projection
    leaves both legs alive, so we can verify both prices.
    """
    market = _p10_market_or_default()  # bid=159.96 ask=160.04 mid=160
    inv = long_inv("285", mid="160")
    quotes = plan_quotes(market, _p10_session(), inv, _p10_config())
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side and "buy" in by_side
    sell, buy = by_side["sell"], by_side["buy"]
    # Close price unchanged by P10.
    assert sell.price == Decimal("159.961")
    # Anti price unchanged by P10 (160 × 0.997 = 159.520).
    assert buy.price == Decimal("159.520")


# ----- P10.5 (Day-7): absolute cap on close-leg notional -------------
#
# P10.5 sits on top of the P10 linear scaling and caps the close-leg
# notional at ``asymmetric_close_size_max_usdc`` whenever set. Day-7
# observation: P10 produced single closes up to $598 (SKHYNIX) on a
# $600 cap with $300 default, and the inventory swings (-$134 →
# +$444 → -$159 in a single fill cycle) cost more in adverse-
# selection drag on trending tape than the faster P10 convergence
# saved. The cap bounds the single-fill loss tail without giving up
# the P10 fast-convergence bias on the close side.
#
# Test config mirrors the SAMSUNG yaml: cap=$500, trigger_pct=0.7
# → trigger=$350. Default size $100 set via session. cap_room=$150.


def _p105_config(**overrides) -> dict:
    """SAMSUNG-shaped P10.5 test config: cap=$500, trigger_pct=0.7
    (trigger=$350), default linear scaling. Tests override
    ``asymmetric_close_size_max_usdc`` (or omit it to assert P10
    fall-back behaviour)."""
    cfg = _p10_config()
    cfg["target_max_delta_usdc"] = Decimal("500")
    cfg["hard_position_cap_usdc"] = Decimal("500")
    cfg.update(overrides)
    return cfg


def test_asymmetric_close_size_capped_by_max_usdc():
    """Cap fires when the linear formula would push close above the
    max. inv=$450 with cap=$500/trigger=$350/default=$100 →
    P10 close = $100 + ($500-$100) × (450-350)/(500-350) ≈ $366.67;
    P10.5 with max=$250 clamps it to $250. Verifies the absolute
    cap actually bounds the close size."""
    market = _p10_market_or_default()
    inv = long_inv("450", mid="160")  # near cap
    cfg = _p105_config(asymmetric_close_size_max_usdc=Decimal("250"))
    quotes = plan_quotes(market, _p10_session(), inv, cfg)
    by_side = {q.side: q for q in quotes}
    # $450 + $100 = $550 ≥ cap → bid (anti) skipped by cap-projection;
    # close (sell) emits at the capped notional.
    assert "sell" in by_side
    sell = by_side["sell"]
    assert sell.size_base == _expected_size_base(Decimal("250"), sell.price)
    assert "asymmetric_close" in sell.notes


def test_asymmetric_close_size_max_usdc_does_not_affect_low_inv():
    """When linear close < max, the cap is a no-op. inv=$380 just
    past trigger=$350 → P10 close = $100 + $400 × 30/150 = $180.
    With max=$250, close stays at $180 (cap not triggered)."""
    market = _p10_market_or_default()
    inv = long_inv("380", mid="160")
    cfg = _p105_config(asymmetric_close_size_max_usdc=Decimal("250"))
    quotes = plan_quotes(market, _p10_session(), inv, cfg)
    by_side = {q.side: q for q in quotes}
    # $380 + $100 = $480 < $500 cap → both legs emit. Close=sell at $180.
    assert "sell" in by_side and "buy" in by_side
    sell, buy = by_side["sell"], by_side["buy"]
    assert sell.size_base == _expected_size_base(Decimal("180"), sell.price)
    # Anti unaffected by P10.5 — stays at session default.
    assert buy.size_base == _expected_size_base(Decimal("100"), buy.price)
    assert "asymmetric_close" in sell.notes


def test_asymmetric_close_size_max_usdc_none_falls_back_to_p10():
    """When ``asymmetric_close_size_max_usdc`` is absent (or None),
    behaviour matches P10 exactly — close can scale all the way to
    effective_cap. inv=$450 → close ≈ $366.67 (no clamp at $250 here
    because no cap is configured). Back-compat guarantee."""
    market = _p10_market_or_default()
    inv = long_inv("450", mid="160")
    cfg = _p105_config()  # no asymmetric_close_size_max_usdc
    assert "asymmetric_close_size_max_usdc" not in cfg
    quotes = plan_quotes(market, _p10_session(), inv, cfg)
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side
    sell = by_side["sell"]
    # P10 linear: close = 100 + (500-100) × (450-350)/(500-350)
    #          = 100 + 400 × 100/150 = 100 + 800/3 ≈ 366.67
    expected_close = Decimal("100") + Decimal("400") * (
        Decimal("100") / Decimal("150")
    )
    assert sell.size_base == _expected_size_base(expected_close, sell.price)
    assert "asymmetric_close" in sell.notes


def test_asymmetric_close_size_max_usdc_clamped_with_inv_abs():
    """Compose P10.5 cap with the existing P10 inv_abs clamp. With
    default=$300 > inv_abs (small position, big default), the inv
    clamp pulls close down to inv_abs first; the P10.5 max cap
    then pulls it the rest of the way to ``max_usdc``.

    Setup: cap=$400, default=$300, trigger_pct=0.7 → trigger=$280,
    inv=$280 (at trigger). Linear formula → $300 (scale=0 + default);
    inv_abs clamp → $280; max=$250 clamp → $250. Verifies the two
    clamps compose in the correct order."""
    market = _p10_market_or_default()
    sess = SessionPolicy(
        name="P105_BIG_DEFAULT",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=Decimal("300"),  # > inv_abs
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="p10.5 inv_abs + max compose",
    )
    inv = long_inv("280", mid="160")  # at trigger ($280 with cap=$400)
    cfg = _p10_config(  # cap=$400 here, default=$300 from session
        asymmetric_close_size_max_usdc=Decimal("250"),
    )
    quotes = plan_quotes(market, sess, inv, cfg)
    by_side = {q.side: q for q in quotes}
    assert "sell" in by_side
    sell = by_side["sell"]
    # Linear: $300 + ($400-$300)*0 = $300.
    # inv_abs clamp: min($300, $280) = $280.
    # P10.5 cap: min($280, $250) = $250.
    assert sell.size_base == _expected_size_base(Decimal("250"), sell.price)
    assert "asymmetric_close" in sell.notes
