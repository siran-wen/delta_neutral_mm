"""Unit tests for observe.session_clock.

Covers:
* 24 representative timestamps across KR/US/crypto schedules
* 4 boundary cases around the KR weekend (Fri 09:00 UTC, Sun 22:30 UTC)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pytest

# make the project root importable when running pytest from a fresh clone
_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from observe.session_clock import (  # noqa: E402
    MarketSchedule,
    SessionState,
    is_kr_off_hours,
    is_market_open,
    is_weekend_for,
    next_close,
    next_open,
    session_state,
)


def ts(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, 0, tzinfo=timezone.utc)


# Reference anchor: Apr 2026 (Mon Apr 20 → Sun Apr 26)
#   Mon 20, Tue 21, Wed 22, Thu 23, Fri 24, Sat 25, Sun 26
# -------------------------------------------------------------


@pytest.mark.parametrize(
    "moment,expected",
    [
        # Crypto always open — 3 points
        (ts(2026, 4, 20, 0, 0), True),    # Mon midnight UTC
        (ts(2026, 4, 25, 12, 0), True),   # Saturday noon UTC
        (ts(2026, 4, 26, 22, 30), True),  # Sunday 22:30 UTC
    ],
)
def test_crypto_is_always_open(moment, expected):
    assert is_market_open(moment, MarketSchedule.CRYPTO_24X7) is expected


@pytest.mark.parametrize(
    "moment,expected",
    [
        # KR open window (00:00-06:30 UTC on weekdays)
        (ts(2026, 4, 20, 0, 0), True),    # Mon open exact
        (ts(2026, 4, 20, 3, 0), True),    # Mon mid-session
        (ts(2026, 4, 20, 6, 29), True),   # Mon close-1min
        (ts(2026, 4, 22, 2, 15), True),   # Wed mid-session
        # KR closed within a weekday
        (ts(2026, 4, 20, 6, 30), False),  # Mon at exact close
        (ts(2026, 4, 20, 10, 0), False),  # Mon after close
        (ts(2026, 4, 20, 23, 59), False), # Mon just before next day
        (ts(2026, 4, 24, 7, 0), False),   # Fri 07:00 — after session
        # KR weekend
        (ts(2026, 4, 25, 3, 0), False),   # Saturday morning
        (ts(2026, 4, 26, 3, 0), False),   # Sunday morning
    ],
)
def test_kr_equity_window(moment, expected):
    assert is_market_open(moment, MarketSchedule.KR_EQUITY) is expected


@pytest.mark.parametrize(
    "moment,expected",
    [
        # US open window (14:30 – 21:00 UTC on weekdays)
        (ts(2026, 4, 20, 14, 30), True),  # Mon open exact
        (ts(2026, 4, 20, 16, 0), True),   # Mon 16:00
        (ts(2026, 4, 20, 20, 59), True),  # Mon 20:59
        (ts(2026, 4, 23, 18, 0), True),   # Thu 18:00
        # US closed
        (ts(2026, 4, 20, 14, 29), False), # Mon 14:29 — pre-open by 1m
        (ts(2026, 4, 20, 21, 0), False),  # Mon exact close
        (ts(2026, 4, 20, 13, 0), False),  # Mon pre-market
        (ts(2026, 4, 25, 16, 0), False),  # Saturday
        (ts(2026, 4, 26, 16, 0), False),  # Sunday
        (ts(2026, 4, 24, 22, 0), False),  # Fri after close
        (ts(2026, 4, 22, 10, 0), False),  # Wed pre-open
    ],
)
def test_us_equity_window(moment, expected):
    assert is_market_open(moment, MarketSchedule.US_EQUITY) is expected


# -------------------------------------------------------------
# KR weekend boundaries
# -------------------------------------------------------------

def test_kr_off_hours_boundary_friday_start_inclusive():
    # Fri 24 Apr 2026 09:00 UTC — exact start of weekend span
    moment = ts(2026, 4, 24, 9, 0)
    assert is_kr_off_hours(moment) is True
    assert is_weekend_for(MarketSchedule.KR_EQUITY, moment) is True


def test_kr_off_hours_boundary_friday_one_minute_before():
    # Fri 24 Apr 2026 08:59 UTC — still a weekday in the weekend sense
    moment = ts(2026, 4, 24, 8, 59)
    assert is_kr_off_hours(moment) is False
    assert is_weekend_for(MarketSchedule.KR_EQUITY, moment) is False


def test_kr_off_hours_boundary_sunday_end_exclusive():
    # Sun 26 Apr 2026 22:30 UTC — exact end of weekend span (exclusive)
    moment = ts(2026, 4, 26, 22, 30)
    assert is_kr_off_hours(moment) is False
    assert is_weekend_for(MarketSchedule.KR_EQUITY, moment) is False


def test_kr_off_hours_boundary_sunday_one_minute_before():
    # Sun 26 Apr 2026 22:29 UTC — inside the weekend span
    moment = ts(2026, 4, 26, 22, 29)
    assert is_kr_off_hours(moment) is True
    assert is_weekend_for(MarketSchedule.KR_EQUITY, moment) is True


# -------------------------------------------------------------
# helper APIs
# -------------------------------------------------------------

def test_session_state_reports_enum():
    assert session_state(ts(2026, 4, 20, 3, 0), MarketSchedule.KR_EQUITY) == SessionState.OPEN
    assert session_state(ts(2026, 4, 25, 3, 0), MarketSchedule.KR_EQUITY) == SessionState.CLOSED


def test_next_open_when_closed_returns_next_monday_open():
    # Saturday 25 Apr 2026 — next open should be Monday 27 Apr 14:30 UTC for US
    moment = ts(2026, 4, 25, 12, 0)
    no = next_open(moment, MarketSchedule.US_EQUITY)
    assert no == ts(2026, 4, 27, 14, 30)


def test_next_close_when_open_is_todays_close():
    moment = ts(2026, 4, 20, 15, 0)  # Monday 15:00 UTC — US open
    nc = next_close(moment, MarketSchedule.US_EQUITY)
    assert nc == ts(2026, 4, 20, 21, 0)


def test_crypto_schedule_has_no_next_open_or_close():
    moment = ts(2026, 4, 25, 3, 0)
    assert next_open(moment, MarketSchedule.CRYPTO_24X7) is None
    assert next_close(moment, MarketSchedule.CRYPTO_24X7) is None


def test_us_weekend_uses_calendar_weekend():
    # US is_weekend is simpler — Sat/Sun only
    assert is_weekend_for(MarketSchedule.US_EQUITY, ts(2026, 4, 25, 12, 0)) is True
    assert is_weekend_for(MarketSchedule.US_EQUITY, ts(2026, 4, 26, 12, 0)) is True
    assert is_weekend_for(MarketSchedule.US_EQUITY, ts(2026, 4, 24, 22, 0)) is False


def test_naive_datetime_is_treated_as_utc():
    naive = datetime(2026, 4, 20, 3, 0)  # no tzinfo
    assert is_market_open(naive, MarketSchedule.KR_EQUITY) is True
