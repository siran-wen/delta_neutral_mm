"""Lighter market-observation package (Phase 1.0, read-only).

Three independent modules:

* ``session_clock``   — pure-function market-hours calendar. Distinguishes
  crypto 24/7, US equity hours, and Korean equity hours (the KR schedule
  is what drives SAMSUNGUSD / HYUNDAIUSD / SKHYNIXUSD liquidity on Lighter).

* ``lpp_share_estimator`` — heuristic LPP share calculator with RWA-aware
  spread thresholds. Weekend / off-hours KR markets widen to 50/100/200bp.

* ``lighter_observer`` — the async observation session that ties the
  gateway + ws together, emits BBO tickers per second, full snapshots
  per minute, and writes JSONL to disk.
"""

from observe.session_clock import (
    MarketSchedule,
    is_market_open,
    next_close,
    next_open,
    session_state,
    SessionState,
)
from observe.lpp_share_estimator import (
    LPPShareEstimate,
    estimate_share,
    tier_from_spread_bp,
)

__all__ = [
    "MarketSchedule",
    "is_market_open",
    "next_close",
    "next_open",
    "session_state",
    "SessionState",
    "LPPShareEstimate",
    "estimate_share",
    "tier_from_spread_bp",
]
