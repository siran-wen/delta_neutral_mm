"""KR-equity session decision for LPP-aware quote management.

The KR cash market trades 00:00-06:30 UTC on weekdays (09:00-15:30 KST,
with a 04:00-05:00 UTC lunch break). Outside that window the SKHYNIX
liquidity profile changes radically — pre-open silence, after-close
spread blow-outs in the first hour, then a slow overnight grind, then
the long Fri 09:00 → Sun 22:30 weekend (RWA continuous mode with much
wider tier thresholds).

This module returns a ``SessionPolicy`` describing what the planner
should do at any UTC instant. It is a pure function: no I/O, no global
state, no time.now() calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional

from .types import SessionPolicy


_KR_TIER_THRESHOLDS = (Decimal("15"), Decimal("25"), Decimal("50"))
_WEEKEND_TIER_THRESHOLDS = (Decimal("50"), Decimal("100"), Decimal("200"))


_DEFAULTS: Dict[str, dict] = {
    "KR_MARKET_HOURS_AM": {
        "action": "quote",
        "default_distance_bp": Decimal("5"),
        "default_size_usdc": Decimal("1000"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR market open AM (00:00-04:00 UTC, 09:00-13:00 KST)",
    },
    "KR_LUNCH_BREAK": {
        "action": "quote",
        "default_distance_bp": Decimal("15"),
        "default_size_usdc": Decimal("500"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR lunch break (04:00-05:00 UTC, liquidity evaporates)",
    },
    "KR_MARKET_HOURS_PM": {
        "action": "quote",
        "default_distance_bp": Decimal("5"),
        "default_size_usdc": Decimal("1000"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR market open PM (05:00-06:30 UTC, 14:00-15:30 KST)",
    },
    "KR_AFTER_CLOSE": {
        "action": "quote",
        "default_distance_bp": Decimal("8"),
        "default_size_usdc": Decimal("500"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR after close (06:30-09:00 UTC, first hour spread very wide)",
    },
    "KR_OVERNIGHT": {
        "action": "quote",
        "default_distance_bp": Decimal("8"),
        "default_size_usdc": Decimal("500"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR overnight (09:00-22:30 UTC weekday only)",
    },
    "KR_BEFORE_OPEN": {
        "action": "quote",
        "default_distance_bp": Decimal("8"),
        "default_size_usdc": Decimal("500"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR before open (22:30-23:30 UTC, 07:30-08:30 KST)",
    },
    "KR_PRE_OPEN_WITHDRAW": {
        # 5-5 retune: flipped from action="withdraw" to "quote" after
        # Day-5 forensics surfaced active MM during this 30-min window
        # (the original assumption was that everyone withdrew before
        # the AM session resumed). Distance/size mirror the adjacent
        # KR_BEFORE_OPEN row since the pre-resumption liquidity
        # profile is similar; per-yaml session_overrides typically
        # tighten size beyond this base. The session NAME still
        # carries the "WITHDRAW" suffix for log-pattern continuity —
        # don't rename it without sweeping the LPP forensics tooling.
        "action": "quote",
        "default_distance_bp": Decimal("8"),
        "default_size_usdc": Decimal("500"),
        "tier_thresholds_bp": _KR_TIER_THRESHOLDS,
        "reason": "KR pre-open quote window (23:30-24:00 UTC, was withdraw pre 5-5)",
    },
    "KR_WEEKEND": {
        "action": "quote",
        "default_distance_bp": Decimal("30"),
        "default_size_usdc": Decimal("1000"),
        "tier_thresholds_bp": _WEEKEND_TIER_THRESHOLDS,
        "reason": "KR weekend (Fri 09:00 UTC - Sun 22:30 UTC, RWA tiers)",
    },
}


def _to_utc(now: datetime) -> datetime:
    """Naive datetimes are treated as UTC, matching observe.session_clock."""
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _is_in_weekend_window(now_utc: datetime) -> bool:
    """[Fri 09:00 UTC, Sun 22:30 UTC) — half-open, end exclusive."""
    weekday = now_utc.weekday()  # Mon=0, Sun=6
    if weekday == 4:                                   # Friday
        return now_utc.hour >= 9
    if weekday == 5:                                   # Saturday — entire day
        return True
    if weekday == 6:                                   # Sunday before 22:30
        if now_utc.hour < 22:
            return True
        if now_utc.hour == 22 and now_utc.minute < 30:
            return True
    return False


def _weekday_session_name(now_utc: datetime) -> str:
    minutes = now_utc.hour * 60 + now_utc.minute
    if minutes < 4 * 60:                               # 00:00-04:00
        return "KR_MARKET_HOURS_AM"
    if minutes < 5 * 60:                               # 04:00-05:00
        return "KR_LUNCH_BREAK"
    if minutes < 6 * 60 + 30:                          # 05:00-06:30
        return "KR_MARKET_HOURS_PM"
    if minutes < 9 * 60:                               # 06:30-09:00
        return "KR_AFTER_CLOSE"
    if minutes < 22 * 60 + 30:                         # 09:00-22:30
        return "KR_OVERNIGHT"
    if minutes < 23 * 60 + 30:                         # 22:30-23:30
        return "KR_BEFORE_OPEN"
    return "KR_PRE_OPEN_WITHDRAW"                      # 23:30-24:00


def get_kr_equity_session(
    now: datetime,
    config: Optional[dict] = None,
) -> SessionPolicy:
    """Return the SessionPolicy for the given UTC instant.

    config schema (all keys optional):
        {
            "<SESSION_NAME>": {
                "default_distance_bp": Decimal,
                "default_size_usdc": Decimal,
                "tier_thresholds_bp": tuple[Decimal, Decimal, Decimal],
            },
            ...
        }

    Only the three numeric fields above can be overridden — name and
    action are always derived from the calendar. Action history note:
    KR_PRE_OPEN_WITHDRAW used to ship as action="withdraw"; the 5-5
    retune flipped it to "quote" after Day-5 forensics found MM
    activity during the 23:30-24:00 UTC window.
    """
    now_utc = _to_utc(now)

    if _is_in_weekend_window(now_utc):
        session_name = "KR_WEEKEND"
    else:
        session_name = _weekday_session_name(now_utc)

    spec = _DEFAULTS[session_name]
    distance = spec["default_distance_bp"]
    size = spec["default_size_usdc"]
    thresholds = spec["tier_thresholds_bp"]

    if config and session_name in config:
        override = config[session_name]
        if "default_distance_bp" in override:
            distance = override["default_distance_bp"]
        if "default_size_usdc" in override:
            size = override["default_size_usdc"]
        if "tier_thresholds_bp" in override:
            thresholds = override["tier_thresholds_bp"]

    return SessionPolicy(
        name=session_name,
        action=spec["action"],
        default_distance_bp=distance,
        default_size_usdc=size,
        tier_thresholds_bp=thresholds,
        reason=spec["reason"],
    )
