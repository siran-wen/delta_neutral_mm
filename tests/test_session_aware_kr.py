"""Unit tests for strategy.session_aware.get_kr_equity_session.

Covers:
* Each weekday session window (AM, lunch, PM, after close, overnight,
  before-open, pre-open withdraw)
* The KR weekend window [Fri 09:00 UTC, Sun 22:30 UTC) with its own
  distance / tier thresholds
* Boundary instants on both sides of each transition
* Per-session config override of distance / size / tier thresholds
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

from strategy.session_aware import get_kr_equity_session  # noqa: E402


def ts(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


# Reference week — Apr 2026 (Mon Apr 20 → Sun Apr 26)
MON = lambda hh, mm=0: ts(2026, 4, 20, hh, mm)
TUE = lambda hh, mm=0: ts(2026, 4, 21, hh, mm)
WED = lambda hh, mm=0: ts(2026, 4, 22, hh, mm)
THU = lambda hh, mm=0: ts(2026, 4, 23, hh, mm)
FRI = lambda hh, mm=0: ts(2026, 4, 24, hh, mm)
SAT = lambda hh, mm=0: ts(2026, 4, 25, hh, mm)
SUN = lambda hh, mm=0: ts(2026, 4, 26, hh, mm)


# ---- weekday windows ---------------------------------------------------

def test_monday_02_00_is_market_hours_am():
    s = get_kr_equity_session(MON(2, 0))
    assert s.name == "KR_MARKET_HOURS_AM"
    assert s.action == "quote"
    assert s.default_distance_bp == Decimal("5")
    assert s.default_size_usdc == Decimal("1000")
    assert s.tier_thresholds_bp == (Decimal("15"), Decimal("25"), Decimal("50"))


def test_monday_04_30_is_lunch_break():
    s = get_kr_equity_session(MON(4, 30))
    assert s.name == "KR_LUNCH_BREAK"
    assert s.action == "quote"
    assert s.default_distance_bp == Decimal("15")
    assert s.default_size_usdc == Decimal("500")


def test_monday_05_30_is_market_hours_pm():
    s = get_kr_equity_session(MON(5, 30))
    assert s.name == "KR_MARKET_HOURS_PM"
    assert s.default_distance_bp == Decimal("5")
    assert s.default_size_usdc == Decimal("1000")


def test_monday_23_45_is_pre_open_withdraw():
    s = get_kr_equity_session(MON(23, 45))
    assert s.name == "KR_PRE_OPEN_WITHDRAW"
    assert s.action == "withdraw"


def test_sunday_23_00_is_before_open():
    s = get_kr_equity_session(SUN(23, 0))
    assert s.name == "KR_BEFORE_OPEN"
    assert s.action == "quote"
    assert s.default_distance_bp == Decimal("8")


def test_sunday_22_25_is_weekend_inside_window():
    s = get_kr_equity_session(SUN(22, 25))
    assert s.name == "KR_WEEKEND"


def test_sunday_22_30_exits_weekend_window():
    s = get_kr_equity_session(SUN(22, 30))
    assert s.name == "KR_BEFORE_OPEN"


def test_saturday_noon_is_weekend_with_rwa_thresholds():
    s = get_kr_equity_session(SAT(12, 0))
    assert s.name == "KR_WEEKEND"
    assert s.default_distance_bp == Decimal("30")
    assert s.default_size_usdc == Decimal("1000")
    assert s.tier_thresholds_bp == (Decimal("50"), Decimal("100"), Decimal("200"))


def test_friday_08_59_still_after_close():
    # spec table: [06:30, 09:00) = KR_AFTER_CLOSE.
    # user's enumerated test #9 said OVERNIGHT but contradicts the table —
    # following the table, since the weekend window only kicks in at 09:00.
    s = get_kr_equity_session(FRI(8, 59))
    assert s.name == "KR_AFTER_CLOSE"


def test_friday_09_00_enters_weekend_window():
    s = get_kr_equity_session(FRI(9, 0))
    assert s.name == "KR_WEEKEND"


def test_monday_00_00_market_hours_am_boundary():
    s = get_kr_equity_session(MON(0, 0))
    assert s.name == "KR_MARKET_HOURS_AM"


def test_monday_04_00_lunch_break_boundary():
    s = get_kr_equity_session(MON(4, 0))
    assert s.name == "KR_LUNCH_BREAK"


def test_tuesday_06_30_after_close_boundary():
    s = get_kr_equity_session(TUE(6, 30))
    assert s.name == "KR_AFTER_CLOSE"


# ---- config override ----------------------------------------------------

def test_config_override_distance_bp():
    config = {
        "KR_MARKET_HOURS_AM": {
            "default_distance_bp": Decimal("10"),
        },
    }
    s = get_kr_equity_session(MON(2, 0), config=config)
    assert s.name == "KR_MARKET_HOURS_AM"
    assert s.default_distance_bp == Decimal("10")
    # other fields untouched
    assert s.default_size_usdc == Decimal("1000")


def test_config_override_size_and_thresholds():
    config = {
        "KR_LUNCH_BREAK": {
            "default_size_usdc": Decimal("250"),
            "tier_thresholds_bp": (Decimal("20"), Decimal("30"), Decimal("60")),
        },
    }
    s = get_kr_equity_session(MON(4, 30), config=config)
    assert s.default_size_usdc == Decimal("250")
    assert s.tier_thresholds_bp == (Decimal("20"), Decimal("30"), Decimal("60"))
    # untouched
    assert s.default_distance_bp == Decimal("15")


# ---- naive datetime is treated as UTC ---------------------------------

def test_naive_datetime_treated_as_utc():
    naive = datetime(2026, 4, 20, 2, 0)  # Mon 02:00, no tzinfo
    s = get_kr_equity_session(naive)
    assert s.name == "KR_MARKET_HOURS_AM"


# ---- timezone-aware non-UTC normalises ---------------------------------

def test_non_utc_timezone_normalises_to_utc():
    from datetime import timedelta
    kst = timezone(timedelta(hours=9))
    moment = datetime(2026, 4, 20, 13, 30, tzinfo=kst)  # = Mon 04:30 UTC
    s = get_kr_equity_session(moment)
    assert s.name == "KR_LUNCH_BREAK"


# ---- after-close boundary is not weekend even on Friday morning -----

def test_wednesday_overnight_after_09_00():
    # weekday 09:00-22:30 maps to KR_OVERNIGHT
    s = get_kr_equity_session(WED(12, 0))
    assert s.name == "KR_OVERNIGHT"
    s2 = get_kr_equity_session(WED(22, 0))
    assert s2.name == "KR_OVERNIGHT"
