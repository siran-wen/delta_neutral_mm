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
