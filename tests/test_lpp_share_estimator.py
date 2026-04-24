"""Unit tests for observe.lpp_share_estimator.

Seven boundary cases covering each tier on both the regular and RWA
weekend schedules, plus a degenerate-input case.
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

from observe.lpp_share_estimator import (  # noqa: E402
    estimate_share,
    tier_from_spread_bp,
)
from observe.session_clock import MarketSchedule  # noqa: E402


# Anchor times:
#   KR weekday open:      Mon 20 Apr 2026 03:00 UTC  (regular hours)
#   KR weekend (Saturday): Sat 25 Apr 2026 12:00 UTC  (weekend spread tiers apply)
REGULAR_AT = datetime(2026, 4, 20, 3, 0, tzinfo=timezone.utc)
WEEKEND_AT = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def test_regular_tier_1_under_5bp_full_share():
    mid = Decimal("100")
    # 4bp spread: ask - bid = 4bp of 100 = 0.04
    bid = Decimal("99.98")
    ask = Decimal("100.02")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, REGULAR_AT, mid=mid)
    assert est.tier == 1
    assert est.share == Decimal("1.0")
    assert est.is_weekend is False
    assert est.tier_threshold_bp == Decimal("5")


def test_regular_tier_2_between_5_and_10bp():
    # 8 bp of 100 → 0.08 width
    bid = Decimal("99.96")
    ask = Decimal("100.04")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, REGULAR_AT)
    assert est.tier == 2
    assert est.share == Decimal("0.6")
    assert est.tier_threshold_bp == Decimal("10")


def test_regular_tier_3_between_10_and_20bp():
    # 15 bp of 100
    bid = Decimal("99.925")
    ask = Decimal("100.075")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, REGULAR_AT)
    assert est.tier == 3
    assert est.share == Decimal("0.3")
    assert est.tier_threshold_bp == Decimal("20")


def test_regular_above_20bp_no_share():
    # 25 bp of 100
    bid = Decimal("99.875")
    ask = Decimal("100.125")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, REGULAR_AT)
    assert est.tier == 0
    assert est.share == Decimal("0")
    assert est.tier_threshold_bp is None


def test_rwa_weekend_tier_1_applies_50bp_threshold():
    # 40 bp — would be tier-0 on regular schedule but tier-1 on RWA weekend
    bid = Decimal("99.8")
    ask = Decimal("100.2")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, WEEKEND_AT)
    assert est.is_weekend is True
    assert est.tier == 1
    assert est.share == Decimal("1.0")
    assert est.tier_threshold_bp == Decimal("50")


def test_rwa_weekend_tier_3_between_100_and_200bp():
    # 150 bp
    bid = Decimal("99.25")
    ask = Decimal("100.75")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, WEEKEND_AT)
    assert est.is_weekend is True
    assert est.tier == 3
    assert est.share == Decimal("0.3")
    assert est.tier_threshold_bp == Decimal("200")


def test_degenerate_inputs_produce_infinite_spread_tier_zero():
    # crossed book — bid > ask
    bid = Decimal("101")
    ask = Decimal("99")
    est = estimate_share(bid, ask, MarketSchedule.KR_EQUITY, REGULAR_AT)
    assert est.tier == 0
    assert est.share == Decimal("0")


def test_tier_from_spread_bp_direct():
    est = tier_from_spread_bp(Decimal("5"), MarketSchedule.US_EQUITY, REGULAR_AT)
    # <=5 bp → tier 1
    assert est.tier == 1
    assert est.share == Decimal("1.0")
