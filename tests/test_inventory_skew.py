"""Unit tests for strategy.inventory_skew."""

from __future__ import annotations

import os
import sys
from decimal import Decimal

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategy.inventory_skew import compute_skew_offsets, is_position_capped  # noqa: E402
from strategy.types import InventoryState  # noqa: E402


def inv(net_usdc: str) -> InventoryState:
    """Quick InventoryState constructor with default base/avg/orders."""
    return InventoryState(
        net_delta_base=Decimal(net_usdc) / Decimal("100"),  # arbitrary, mid=100
        net_delta_usdc=Decimal(net_usdc),
        avg_entry_price=None,
        open_orders_count=0,
    )


TARGET = Decimal("500")
MAX_OFFSET = Decimal("5")


# ---- compute_skew_offsets ----------------------------------------------

def test_flat_returns_zero_offsets():
    bid_off, ask_off = compute_skew_offsets(inv("0"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal(0)
    assert ask_off == Decimal(0)


def test_long_at_target_clamps_to_full_offset():
    bid_off, ask_off = compute_skew_offsets(inv("500"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal("5")
    assert ask_off == Decimal(0)


def test_long_above_target_clamped_to_max():
    bid_off, ask_off = compute_skew_offsets(inv("1000"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal("5")
    assert ask_off == Decimal(0)


def test_short_at_target_clamps_to_full_offset():
    bid_off, ask_off = compute_skew_offsets(inv("-500"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal(0)
    assert ask_off == Decimal("5")


def test_short_at_half_target_scales_linearly():
    bid_off, ask_off = compute_skew_offsets(inv("-250"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal(0)
    assert ask_off == Decimal("2.5")


def test_zero_target_returns_zero_offsets():
    """defensive — if config sets target to 0, never blow up dividing."""
    bid_off, ask_off = compute_skew_offsets(inv("100"), Decimal(0), MAX_OFFSET)
    assert bid_off == Decimal(0)
    assert ask_off == Decimal(0)


# ---- is_position_capped ------------------------------------------------

CAP = Decimal("1000")


def test_cap_long_breach_skips_bid():
    skip_bid, skip_ask = is_position_capped(inv("1500"), CAP)
    assert skip_bid is True
    assert skip_ask is False


def test_cap_short_breach_skips_ask():
    skip_bid, skip_ask = is_position_capped(inv("-1500"), CAP)
    assert skip_bid is False
    assert skip_ask is True


def test_cap_just_under_passes():
    skip_bid, skip_ask = is_position_capped(inv("999"), CAP)
    assert skip_bid is False
    assert skip_ask is False


def test_cap_exactly_at_long_boundary_inclusive():
    skip_bid, skip_ask = is_position_capped(inv("1000"), CAP)
    assert skip_bid is True
    assert skip_ask is False


def test_cap_exactly_at_short_boundary_inclusive():
    skip_bid, skip_ask = is_position_capped(inv("-1000"), CAP)
    assert skip_bid is False
    assert skip_ask is True


# ---- skew at quarter target (0.25 factor) -----------------------------

def test_long_quarter_target_yields_quarter_offset():
    bid_off, ask_off = compute_skew_offsets(inv("125"), TARGET, MAX_OFFSET)
    assert bid_off == Decimal("1.25")
    assert ask_off == Decimal(0)


# ---- Phase 2.1 P2.1.1: pct-of-collateral double cap --------------------


def test_is_position_capped_pct_cap_tighter_than_usdc_cap():
    """pct cap binds when pct*collateral < usdc cap.

    Collateral 1000 * 30% = 300 < 600 abs. Inv 400 long → 400 >= 300 →
    skip_bid (capped on long side); 400 > -300 → no skip_ask.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("400"),
        hard_cap_usdc=Decimal("600"),
        collateral_usdc=Decimal("1000"),
        hard_cap_pct=Decimal("0.30"),
    )
    assert skip_bid is True
    assert skip_ask is False


def test_is_position_capped_usdc_cap_tighter_than_pct_cap():
    """usdc cap binds when collateral pct gives a larger threshold.

    Collateral 5000 * 30% = 1500 > 600 abs. Effective cap stays at 600.
    Inv 700 long → 700 >= 600 → skip_bid.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("700"),
        hard_cap_usdc=Decimal("600"),
        collateral_usdc=Decimal("5000"),
        hard_cap_pct=Decimal("0.30"),
    )
    assert skip_bid is True
    assert skip_ask is False


def test_is_position_capped_no_collateral_falls_back_to_usdc_only():
    """Missing collateral or missing pct → original Phase 1 behaviour.

    Inv 700, hard_cap=600. No collateral provided, so the pct cap is
    inert and the absolute cap fires on its own (700 >= 600).
    """
    skip_bid, skip_ask = is_position_capped(
        inv("700"),
        hard_cap_usdc=Decimal("600"),
        collateral_usdc=None,
        hard_cap_pct=Decimal("0.30"),
    )
    assert skip_bid is True
    assert skip_ask is False
    # And without the pct kwarg at all — also Phase 1 behaviour.
    skip_bid2, _ = is_position_capped(inv("700"), hard_cap_usdc=Decimal("600"))
    assert skip_bid2 is True


# ---- defensive (post-fill projected) cap check -----------------------
#
# 5-2 production: SKHYNIX inv reached $148 against a $100 hard cap
# because the old check only tripped at inv >= cap, but with size=$50
# a fill at inv=$98 landed inventory at $148 — too late. The defensive
# check projects post-fill inventory so the side is dropped *before*
# any fill can breach the cap.

def test_cap_defensive_buy_fill_would_exceed():
    """inv=$80 + buy_size=$50 → projected $130 ≥ cap=$100 → skip_bid.

    Sell side stays open: projected_short = 80 - 50 = 30 ≥ -100, safe.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("80"),
        hard_cap_usdc=Decimal("100"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid is True
    assert skip_ask is False


def test_cap_defensive_sell_fill_would_exceed():
    """inv=-$80 short. sell fill projects to -130, breaches -cap → skip_ask.

    Buy side reduces the short, projects to -30, still >= -100 → safe.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("-80"),
        hard_cap_usdc=Decimal("100"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid is False
    assert skip_ask is True


def test_cap_defensive_both_safe():
    """inv=$30 + size=$50 → both projected fills well within ±cap=$100."""
    skip_bid, skip_ask = is_position_capped(
        inv("30"),
        hard_cap_usdc=Decimal("100"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid is False  # 30 + 50 = 80 < 100
    assert skip_ask is False  # 30 - 50 = -20 > -100


def test_cap_backwards_compat_size_zero():
    """size_per_side_usdc default = 0 → check degrades to inv >= cap.

    Pre-defensive callers that don't pass the new kwarg keep their
    exact original behaviour; inv=$80 < cap=$100 → no skip.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("80"),
        hard_cap_usdc=Decimal("100"),
    )
    assert skip_bid is False
    assert skip_ask is False


def test_cap_with_pct_cap_smaller_uses_pct():
    """Defensive check still composes correctly with the pct double cap.

    hard_cap_usdc=$200 but pct=0.05 * $2000 collateral = $100, so
    effective_cap = $100. inv=$30 + size=$50 → projected $80 < $100,
    safe; inv=$60 + size=$50 → projected $110 >= $100, skip_bid.
    """
    skip_bid_safe, skip_ask_safe = is_position_capped(
        inv("30"),
        hard_cap_usdc=Decimal("200"),
        collateral_usdc=Decimal("2000"),
        hard_cap_pct=Decimal("0.05"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid_safe is False
    assert skip_ask_safe is False

    skip_bid_breach, _ = is_position_capped(
        inv("60"),
        hard_cap_usdc=Decimal("200"),
        collateral_usdc=Decimal("2000"),
        hard_cap_pct=Decimal("0.05"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid_breach is True


def test_cap_size_exactly_meets_cap_inclusive():
    """Projected fill landing exactly on the cap also trips skip (>=).

    inv=$50 + size=$50 → projected $100 == cap=$100 → skip_bid.
    Inclusive boundary matches original behaviour and is safer when
    pricing precision drifts the projection by sub-cent amounts.
    """
    skip_bid, _ = is_position_capped(
        inv("50"),
        hard_cap_usdc=Decimal("100"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid is True


def test_cap_day1_skhynix_replay_inv_98_size_50():
    """Regression: the exact 5-2 21:43:37 SKHYNIX state.

    inv=$98 (under the $100 cap, so old check passed and a buy was
    quoted). Next fill landed inv at $148 — 48% over cap. The
    defensive check rejects the bid before it leaves the planner.
    """
    skip_bid, skip_ask = is_position_capped(
        inv("98"),
        hard_cap_usdc=Decimal("100"),
        size_per_side_usdc=Decimal("50"),
    )
    assert skip_bid is True
    # Sell side still safe — flattens the existing long.
    assert skip_ask is False
