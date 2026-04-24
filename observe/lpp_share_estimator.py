"""Heuristic LPP share estimator with RWA-aware spread thresholds.

Given a maker's observed spread at time ``t`` and a market schedule,
estimate the fraction of the Lighter Liquidity Provider Program (LPP)
reward the maker should expect to earn from that market.

The LPP program is tiered by spread:

* **Regular sessions**:
    - Tier 1: ≤ 5  bp  → full share
    - Tier 2: ≤ 10 bp  → 60% share
    - Tier 3: ≤ 20 bp  → 30% share
    - > 20 bp           → 0%

* **RWA weekend / off-hours** (Korean equities Fri 09:00 UTC → Sun 22:30 UTC):
    - Tier 1: ≤ 50  bp  → full share
    - Tier 2: ≤ 100 bp  → 60% share
    - Tier 3: ≤ 200 bp  → 30% share
    - > 200 bp           → 0%

These tier schedules are the TIER_6_SCHEDULE referenced by the Phase
1.0 prompt. They are *heuristics* — the real LPP formula is more
complex and involves depth, time-weighted presence, and per-market
quotas — but the tiered spread view is an accurate first-order
approximation and is enough to decide whether a candidate market is
worth quoting into.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from observe.session_clock import (
    MarketSchedule,
    is_weekend_for,
)


# Regular-hours tier schedule (bp → share)
_REGULAR_TIERS = (
    (Decimal("5"), Decimal("1.0")),
    (Decimal("10"), Decimal("0.6")),
    (Decimal("20"), Decimal("0.3")),
)

# RWA weekend tier schedule (bp → share)
_RWA_WEEKEND_TIERS = (
    (Decimal("50"), Decimal("1.0")),
    (Decimal("100"), Decimal("0.6")),
    (Decimal("200"), Decimal("0.3")),
)


@dataclass(frozen=True)
class LPPShareEstimate:
    spread_bp: Decimal
    tier: int  # 1/2/3 or 0 for "outside tiered range"
    share: Decimal
    schedule: MarketSchedule
    is_weekend: bool
    tier_threshold_bp: Optional[Decimal]


def _compute_spread_bp(
    bid: Decimal,
    ask: Decimal,
    mid: Optional[Decimal] = None,
) -> Decimal:
    """Return the bid-ask spread in basis points against the mid.

    Uses the caller-provided ``mid`` if given (e.g. a mark price);
    otherwise uses ``(bid+ask)/2``. Returns Decimal('Inf') for
    degenerate inputs (bid/ask non-positive, crossed book).
    """
    if bid <= 0 or ask <= 0:
        return Decimal("Infinity")
    if ask < bid:
        return Decimal("Infinity")
    if mid is None or mid <= 0:
        mid = (bid + ask) / Decimal(2)
    if mid <= 0:
        return Decimal("Infinity")
    return ((ask - bid) / mid) * Decimal("10000")


def tier_from_spread_bp(
    spread_bp: Decimal,
    schedule: MarketSchedule,
    at: datetime,
) -> LPPShareEstimate:
    """Map a raw spread-in-bp reading to a tier / share.

    ``at`` is used only to decide whether the RWA weekend tier table
    applies (per-schedule via session_clock.is_weekend_for).
    """
    is_weekend = is_weekend_for(schedule, at)
    tiers = _RWA_WEEKEND_TIERS if is_weekend else _REGULAR_TIERS

    tier_num = 0
    share = Decimal("0")
    threshold_bp: Optional[Decimal] = None
    for idx, (max_bp, tier_share) in enumerate(tiers, start=1):
        if spread_bp <= max_bp:
            tier_num = idx
            share = tier_share
            threshold_bp = max_bp
            break

    return LPPShareEstimate(
        spread_bp=spread_bp,
        tier=tier_num,
        share=share,
        schedule=schedule,
        is_weekend=is_weekend,
        tier_threshold_bp=threshold_bp,
    )


def estimate_share(
    bid: Decimal,
    ask: Decimal,
    schedule: MarketSchedule,
    at: datetime,
    mid: Optional[Decimal] = None,
) -> LPPShareEstimate:
    """Combine _compute_spread_bp + tier_from_spread_bp in one call."""
    spread_bp = _compute_spread_bp(bid, ask, mid)
    return tier_from_spread_bp(spread_bp, schedule, at)
