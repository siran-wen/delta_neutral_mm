"""Unit tests for ``strategy.quote_diff.diff_quotes``.

Coverage:

* spec example 1: no desired + two active → cancel both
* spec example 2: same price + same size → no-op
* spec example 3: same price, size differs > tol → cancel + place
* spec example 4: price drift > tol → cancel + place
* boundary: price drift exactly at tol → match
* boundary: size drift exactly at tol → match
* mixed-side: bid matches, ask differs → only ask churns
* both empty → empty diff
* multiple active orders on same side, only one matches → unclaimed cancels
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

from strategy.quote_diff import diff_quotes  # noqa: E402
from strategy.types import Quote  # noqa: E402


def _Q(side: str, price: str, size: str) -> Quote:
    p = Decimal(price)
    s = Decimal(size)
    return Quote(
        side=side,
        price=p,
        size_base=s,
        size_usdc=p * s,
        distance_from_mid_bp=Decimal("5"),
        tier_target="L1",
        market_position="passive",
        notes=(),
    )


def _A(oid: str, side: str, price: str, size: str) -> dict:
    return {
        "client_order_id": oid,
        "side": side,
        "price": Decimal(price),
        "size_base": Decimal(size),
    }


# ---- spec cases --------------------------------------------------------


def test_empty_desired_cancels_all_active():
    active = [
        _A("a1", "buy", "99.95", "10"),
        _A("a2", "sell", "100.05", "10"),
    ]
    cancels, places = diff_quotes([], active)
    assert sorted(cancels) == ["a1", "a2"]
    assert places == []


def test_match_same_price_same_size_is_noop():
    desired = [_Q("buy", "99.95", "10")]
    active = [_A("a1", "buy", "99.95", "10")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == []
    assert places == []


def test_size_drift_beyond_tolerance_replaces():
    # desired size 10, active size 5 → drift = 50% (default tol 5%)
    desired = [_Q("buy", "99.95", "10")]
    active = [_A("a1", "buy", "99.95", "5")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == ["a1"]
    assert len(places) == 1
    assert places[0].side == "buy"
    assert places[0].size_base == Decimal("10")


def test_price_drift_beyond_tolerance_replaces():
    # desired bid at 99.95, active at 99.93 ⇒ drift ≈ 2bp > 0.5bp tol
    desired = [_Q("buy", "99.95", "10")]
    active = [_A("a1", "buy", "99.93", "10")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == ["a1"]
    assert len(places) == 1


# ---- boundaries --------------------------------------------------------


def test_price_drift_exactly_at_tolerance_matches():
    # 0.5bp of 100 = 0.005. 100 vs 100.005 → drift = 0.5bp exactly.
    desired = [_Q("buy", "100", "10")]
    active = [_A("a1", "buy", "100.005", "10")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == []
    assert places == []


def test_size_drift_exactly_at_tolerance_matches():
    # default size_tolerance_pct = 5%. 10 vs 10.5 → 5% exactly.
    desired = [_Q("buy", "99.95", "10")]
    active = [_A("a1", "buy", "99.95", "10.5")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == []
    assert places == []


def test_size_drift_just_outside_tolerance_replaces():
    # 10 vs 10.51 → 5.1% > 5% tol
    desired = [_Q("buy", "99.95", "10")]
    active = [_A("a1", "buy", "99.95", "10.51")]
    cancels, places = diff_quotes(desired, active)
    assert cancels == ["a1"]
    assert len(places) == 1


# ---- mixed-side --------------------------------------------------------


def test_bid_matches_ask_drifts():
    desired = [_Q("buy", "99.95", "10"), _Q("sell", "100.05", "10")]
    active = [
        _A("a1", "buy", "99.95", "10"),  # match
        _A("a2", "sell", "100.20", "10"),  # ~15bp drift, replace
    ]
    cancels, places = diff_quotes(desired, active)
    assert cancels == ["a2"]
    assert len(places) == 1
    assert places[0].side == "sell"


def test_both_empty_returns_empty_diff():
    cancels, places = diff_quotes([], [])
    assert cancels == []
    assert places == []


def test_unmatched_active_on_same_side_is_cancelled():
    # Two active bids, only one matches the lone desired bid
    desired = [_Q("buy", "100.00", "10")]
    active = [
        _A("a1", "buy", "100.00", "10"),  # match
        _A("a2", "buy", "99.50", "10"),  # stale ladder rung
    ]
    cancels, places = diff_quotes(desired, active)
    assert cancels == ["a2"]
    assert places == []


def test_active_with_unknown_side_is_treated_as_cancel():
    # Defensive: if an active order has a malformed side, it should
    # not silently match. It also shouldn't crash; spec doesn't
    # require this explicitly but the code path is reachable.
    desired = [_Q("buy", "100", "10")]
    active = [_A("a1", "neither", "100", "10")]
    cancels, places = diff_quotes(desired, active)
    # Active with unknown side has no group → it can't be matched →
    # it ends up in cancels; desired has no match so it ends up in
    # places.
    assert cancels == ["a1"]
    assert len(places) == 1


def test_custom_tighter_tolerance_rejects_what_default_accepts():
    desired = [_Q("buy", "100", "10")]
    # 0.4bp drift — accepted by default 0.5 tol, rejected by 0.3 tol
    active = [_A("a1", "buy", "100.004", "10")]

    cancels_default, places_default = diff_quotes(desired, active)
    assert cancels_default == [] and places_default == []

    cancels_tight, places_tight = diff_quotes(
        desired, active, price_tolerance_bp=Decimal("0.3")
    )
    assert cancels_tight == ["a1"]
    assert len(places_tight) == 1
