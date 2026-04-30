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
