"""Market-hours / session calendar for Lighter RWA observation.

Pure functions only — no I/O, no global state besides constants.

Three schedules:

* ``MarketSchedule.CRYPTO_24X7`` — always open.
* ``MarketSchedule.US_EQUITY``  — US cash equity regular session:
  Mon-Fri, 14:30–21:00 UTC (equivalent to 09:30–16:00 America/New_York
  during US standard time; we ignore DST shifts for this observer — the
  Lighter liquidity signal is coarse enough that the 1-hour offset
  during DST does not materially move the weekend-spread logic).

* ``MarketSchedule.KR_EQUITY`` — Korean cash equity regular session:
  Mon-Fri, 00:00–06:30 UTC (09:00–15:30 Asia/Seoul).

The "weekend for KR" window — Friday 09:00 UTC through Sunday 22:30 UTC —
is defined separately as ``KR_OFF_HOURS_SPAN``. That span is what
``lpp_share_estimator`` reads when deciding whether to apply the
RWA weekend spread thresholds (50 / 100 / 200 bp).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from enum import Enum
from typing import Optional


class MarketSchedule(Enum):
    CRYPTO_24X7 = "crypto_24x7"
    US_EQUITY = "us_equity"
    KR_EQUITY = "kr_equity"


class SessionState(Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True)
class _WindowDef:
    weekdays: tuple  # Monday=0 .. Sunday=6
    open_utc: time
    close_utc: time


# Monday .. Friday
_WEEKDAYS = (0, 1, 2, 3, 4)

_US_EQUITY_WINDOW = _WindowDef(
    weekdays=_WEEKDAYS,
    open_utc=time(14, 30, 0, tzinfo=timezone.utc),
    close_utc=time(21, 0, 0, tzinfo=timezone.utc),
)

_KR_EQUITY_WINDOW = _WindowDef(
    weekdays=_WEEKDAYS,
    open_utc=time(0, 0, 0, tzinfo=timezone.utc),
    close_utc=time(6, 30, 0, tzinfo=timezone.utc),
)

# KR off-hours for RWA spread-widening: Fri 09:00 UTC → Sun 22:30 UTC.
# Used by lpp_share_estimator as the "weekend" window.
KR_OFF_HOURS_START_WEEKDAY: int = 4  # Friday
KR_OFF_HOURS_START_TIME: time = time(9, 0, 0, tzinfo=timezone.utc)
KR_OFF_HOURS_END_WEEKDAY: int = 6  # Sunday
KR_OFF_HOURS_END_TIME: time = time(22, 30, 0, tzinfo=timezone.utc)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _window_for(schedule: MarketSchedule) -> Optional[_WindowDef]:
    if schedule == MarketSchedule.US_EQUITY:
        return _US_EQUITY_WINDOW
    if schedule == MarketSchedule.KR_EQUITY:
        return _KR_EQUITY_WINDOW
    return None


def is_market_open(dt: datetime, schedule: MarketSchedule) -> bool:
    """Return True iff the schedule considers ``dt`` to be a trading moment.

    Crypto schedules always return True. Equity schedules return True
    only inside the regular session on a weekday. DST shifts are ignored.
    """
    if schedule == MarketSchedule.CRYPTO_24X7:
        return True
    dt = _as_utc(dt)
    window = _window_for(schedule)
    if window is None:
        return True
    if dt.weekday() not in window.weekdays:
        return False
    t = dt.timetz()
    return window.open_utc <= t < window.close_utc


def session_state(dt: datetime, schedule: MarketSchedule) -> SessionState:
    return SessionState.OPEN if is_market_open(dt, schedule) else SessionState.CLOSED


def next_open(dt: datetime, schedule: MarketSchedule) -> Optional[datetime]:
    """Next UTC moment at or after ``dt`` when the market reopens.

    Returns ``dt`` if the market is already open. Returns None for
    24x7 schedules (the question is ill-defined). Searches at most 8
    days forward, which always covers weekly closes.
    """
    if schedule == MarketSchedule.CRYPTO_24X7:
        return None
    dt = _as_utc(dt)
    window = _window_for(schedule)
    if window is None:
        return dt
    if is_market_open(dt, schedule):
        return dt
    for day_offset in range(0, 8):
        probe_day = (dt + timedelta(days=day_offset)).date()
        wd = datetime(
            probe_day.year, probe_day.month, probe_day.day, tzinfo=timezone.utc
        ).weekday()
        if wd not in window.weekdays:
            continue
        candidate = datetime.combine(
            probe_day, window.open_utc.replace(tzinfo=None), tzinfo=timezone.utc
        )
        if candidate >= dt:
            return candidate
    return None


def next_close(dt: datetime, schedule: MarketSchedule) -> Optional[datetime]:
    """Next UTC moment at or after ``dt`` when the market closes.

    Returns None for 24x7. If the market is closed, returns the close
    moment of the next open window.
    """
    if schedule == MarketSchedule.CRYPTO_24X7:
        return None
    dt = _as_utc(dt)
    window = _window_for(schedule)
    if window is None:
        return None
    # If open today, today's close is the answer
    if is_market_open(dt, schedule):
        close_dt = datetime.combine(
            dt.date(), window.close_utc.replace(tzinfo=None), tzinfo=timezone.utc
        )
        if close_dt > dt:
            return close_dt
    # Otherwise advance to next open and return that window's close
    no = next_open(dt, schedule)
    if no is None:
        return None
    return datetime.combine(
        no.date(), window.close_utc.replace(tzinfo=None), tzinfo=timezone.utc
    )


def is_kr_off_hours(dt: datetime) -> bool:
    """Return True for Fri 09:00 UTC ≤ dt < Sun 22:30 UTC.

    This is the LPP-doc-aligned "weekend" for Korean equities. Lighter's
    RWA markets for Korean stocks (SAMSUNGUSD / HYUNDAIUSD / SKHYNIXUSD)
    typically widen spread significantly during this window.
    """
    dt = _as_utc(dt)
    wd = dt.weekday()  # Mon=0 ... Sun=6

    # Full-day weekend days: Saturday
    if wd == 5:
        return True
    # Friday after 09:00 UTC
    if wd == KR_OFF_HOURS_START_WEEKDAY and dt.timetz() >= KR_OFF_HOURS_START_TIME:
        return True
    # Sunday before 22:30 UTC
    if wd == KR_OFF_HOURS_END_WEEKDAY and dt.timetz() < KR_OFF_HOURS_END_TIME:
        return True
    return False


def is_weekend_for(schedule: MarketSchedule, dt: datetime) -> bool:
    """Generalized "off-hours weekend" predicate used by the LPP estimator.

    * KR_EQUITY: Fri 09:00 UTC – Sun 22:30 UTC (KR_OFF_HOURS span).
    * US_EQUITY: Sat-Sun (conservative approximation — US after-hours
      liquidity runs weeknights, so we only flag true weekend).
    * CRYPTO_24X7: always False.
    """
    if schedule == MarketSchedule.CRYPTO_24X7:
        return False
    if schedule == MarketSchedule.KR_EQUITY:
        return is_kr_off_hours(dt)
    if schedule == MarketSchedule.US_EQUITY:
        dt = _as_utc(dt)
        return dt.weekday() in (5, 6)
    return False
