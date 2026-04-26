"""Unit tests for observe.depth_aggregation.

Covers:

1. ``aggregate_depth_by_spread_bp`` cumulative bucketing is correct
   (mock orderbook + mid → expected per-tier USDC).
2. The boundary rule is inclusive — a level priced exactly at the
   tier threshold is counted in that tier (≤15bp covers 15bp).
3. The mid is the *(best_bid + best_ask) / 2* of the *book*, not a
   ws-mark override — the function takes mid as an explicit arg, so
   passing the BBO mid yields BBO-relative tiers.
4. Empty book / zero-mid → all tiers zero, no exception.
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

from observe.depth_aggregation import (  # noqa: E402
    DEFAULT_DEPTH_TIERS_BP,
    aggregate_depth_by_spread_bp,
    aggregate_trades_window,
    top_levels,
)


def _D(s):  # tiny helper
    return Decimal(s)


# Anchor mid = 100.000. Every 1bp = 0.01 in price.
#   100.00 → 0bp
#   99.95  → 5bp from mid (bid)
#   100.05 → 5bp from mid (ask)
#   99.85  → 15bp (bid), 100.15 → 15bp (ask)


def test_depth_tiers_cumulative_bucketing():
    bids = [
        (_D("99.99"), _D("100")),  # 1bp from mid → in 5/10/15/25/.../200
        (_D("99.95"), _D("200")),  # 5bp → in 5/10/15/25/...
        (_D("99.85"), _D("300")),  # 15bp → in 15/25/...
        (_D("99.50"), _D("400")),  # 50bp → in 50/100/200
        (_D("99.00"), _D("500")),  # 100bp → in 100/200
    ]
    asks = [
        (_D("100.01"), _D("100")),  # 1bp
        (_D("100.05"), _D("200")),  # 5bp
        (_D("100.15"), _D("300")),  # 15bp
        (_D("100.50"), _D("400")),  # 50bp
        (_D("101.00"), _D("500")),  # 100bp
    ]
    mid = (bids[0][0] + asks[0][0]) / Decimal(2)  # 100.00 exactly

    out = aggregate_depth_by_spread_bp(bids, asks, mid)

    # 5bp tier: only the 1bp + 5bp levels per side.
    bid_5 = _D("99.99") * _D("100") + _D("99.95") * _D("200")
    ask_5 = _D("100.01") * _D("100") + _D("100.05") * _D("200")
    assert out["5"]["bid_usdc"] == bid_5
    assert out["5"]["ask_usdc"] == ask_5
    assert out["5"]["total_usdc"] == bid_5 + ask_5

    # 15bp tier: 1bp + 5bp + 15bp on each side.
    bid_15 = bid_5 + _D("99.85") * _D("300")
    ask_15 = ask_5 + _D("100.15") * _D("300")
    assert out["15"]["bid_usdc"] == bid_15
    assert out["15"]["ask_usdc"] == ask_15

    # 200bp tier: every level on each side (everything is within 100bp).
    bid_200 = sum((p * s for p, s in bids), Decimal("0"))
    ask_200 = sum((p * s for p, s in asks), Decimal("0"))
    assert out["200"]["bid_usdc"] == bid_200
    assert out["200"]["ask_usdc"] == ask_200
    assert out["200"]["total_usdc"] == bid_200 + ask_200


def test_depth_tier_boundary_is_inclusive():
    """A level priced exactly at the tier upper bound is counted."""
    mid = _D("100")
    # Bid at exactly 15bp: 100 * (1 - 15/10000) = 99.85
    bids = [(_D("99.85"), _D("1"))]
    asks = [(_D("100.15"), _D("1"))]
    out = aggregate_depth_by_spread_bp(bids, asks, mid, tiers_bp=(_D("10"), _D("15")))
    # Inside 10bp? No — 15bp > 10bp.
    assert out["10"]["bid_usdc"] == _D("0")
    assert out["10"]["ask_usdc"] == _D("0")
    # At-the-boundary: must be inside 15bp.
    assert out["15"]["bid_usdc"] == _D("99.85")
    assert out["15"]["ask_usdc"] == _D("100.15")
    # And the 15bp total includes both sides.
    assert out["15"]["total_usdc"] == _D("99.85") + _D("100.15")


def test_mid_drives_tier_distance_independently_of_mark():
    """
    The function takes ``mid`` as an explicit argument. If a caller
    passes a different mid (e.g. ws mark), the tier comparison
    rebases. Verifies we don't accidentally re-derive mid from the
    book inside the function — the caller's value wins.
    """
    bids = [(_D("100.00"), _D("10"))]
    asks = [(_D("100.10"), _D("10"))]
    # Real BBO mid = 100.05 → bid is 5bp away, ask is 5bp away.
    real_mid = (bids[0][0] + asks[0][0]) / Decimal(2)
    out_real = aggregate_depth_by_spread_bp(
        bids, asks, real_mid, tiers_bp=(_D("5"),)
    )
    # 5bp tier: both sides included (boundary inclusive).
    assert out_real["5"]["bid_usdc"] == _D("1000.00")
    assert out_real["5"]["ask_usdc"] == _D("1001.00")

    # If the caller passes a *different* mid (way off), the buckets
    # change accordingly. Pass mid=110 — both bids/asks are far below
    # 110, so the 5bp tier should be empty for the bid side and asks
    # are also too far away (asks at 100.10 < mid=110).
    out_off = aggregate_depth_by_spread_bp(
        bids, asks, _D("110"), tiers_bp=(_D("5"),)
    )
    assert out_off["5"]["bid_usdc"] == _D("0")
    assert out_off["5"]["ask_usdc"] == _D("0")


def test_empty_orderbook_returns_zero_tiers_no_exception():
    out = aggregate_depth_by_spread_bp([], [], None)
    for tier in DEFAULT_DEPTH_TIERS_BP:
        # Decimal('5').to_integral_value() == Decimal('5') so key == '5'
        key = str(int(tier))
        assert out[key]["bid_usdc"] == Decimal("0")
        assert out[key]["ask_usdc"] == Decimal("0")
        assert out[key]["total_usdc"] == Decimal("0")

    # Non-empty book but mid=0 → still zeroes (defensive).
    out2 = aggregate_depth_by_spread_bp(
        [(_D("100"), _D("1"))], [(_D("101"), _D("1"))], _D("0")
    )
    assert out2["5"]["bid_usdc"] == _D("0")
    assert out2["200"]["ask_usdc"] == _D("0")


# -------------------------------------------------------------
# top_levels — pulled in here to round out the depth coverage.
# -------------------------------------------------------------


def test_top_levels_caps_at_n_and_stringifies():
    bids = [(_D("100.0"), _D("3")), (_D("99.5"), _D("2")), (_D("99.0"), _D("4"))]
    out = top_levels(bids, n=2)
    assert len(out) == 2
    assert out[0] == {"price": "100.0", "size_base": "3", "size_usdc": "300.0"}
    assert out[1]["price"] == "99.5"
    assert out[1]["size_usdc"] == "199.0"
