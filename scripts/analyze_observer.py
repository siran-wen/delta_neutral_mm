"""LPP observer post-run analyzer.

Aggregates one or more ``observe_lighter`` JSONL outputs across the 22
LPP-eligible markets, identifies LPP-style market makers (long-running
two-sided makers — the same role our strategy plays), and reports the
relative position our nominal size occupies on each book. Output is a
Markdown summary on stdout, plus a machine-readable ``.analysis.json``
and ``.mm_profile.json`` next to the input file(s).

The MM identifier is intentionally simple — no ML, no sklearn. A
"cluster" is a set of order-book level sizes that fall within ±3% of
each other. A cluster is classified as ``lpp_mm`` when it appears on
both sides of the book (two-sided), shows up in at least ~50% of
snapshots (persistent), and accumulates enough events to rule out
noise. The 3% tolerance + 3-event minimum thresholds are there to
absorb sub-cent price drift on a constant ``size_base`` while still
catching the typical "$500 round size" anchor an LPP MM uses.

Stdlib only, with optional pyyaml. Runs on the same Python the rest
of this project uses (3.12).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------
# LPP pool reference
# ---------------------------------------------------------------------

POOL_BY_MARKET: Dict[str, Dict[str, Any]] = {
    # T6 — Korean RWAs (~$12k weekly each, three markets)
    "SKHYNIXUSD":  {"tier": "T6_KR",   "weekly_pool_usdc": 12269},
    "SAMSUNGUSD":  {"tier": "T6_KR",   "weekly_pool_usdc": 12269},
    "HYUNDAIUSD":  {"tier": "T6_KR",   "weekly_pool_usdc": 12269},
    # T6 — Commodities
    "XAUUSD":      {"tier": "T6_COMM", "weekly_pool_usdc": 23000},
    "XAGUSD":      {"tier": "T6_COMM", "weekly_pool_usdc": 23000},
    "BRENTOIL":    {"tier": "T6_COMM", "weekly_pool_usdc": 38000},
    "WTI":         {"tier": "T6_COMM", "weekly_pool_usdc": 38000},
    # T5 — US benchmark
    "SPYUSD":      {"tier": "T5",      "weekly_pool_usdc": 17000},
    # T4 — mid-cap mix
    "MON":         {"tier": "T4",      "weekly_pool_usdc": 4000},
    "ZEC":         {"tier": "T4",      "weekly_pool_usdc": 9000},
    "ZRO":         {"tier": "T4",      "weekly_pool_usdc": 6000},
    "CRCL":        {"tier": "T4",      "weekly_pool_usdc": 6000},
    "AAVE":        {"tier": "T4",      "weekly_pool_usdc": 9000},
    "TSLA":        {"tier": "T4",      "weekly_pool_usdc": 9000},
    "XPT":         {"tier": "T4",      "weekly_pool_usdc": 4000},
    # T3 — small-cap mix
    "NVDA":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "COIN":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "HOOD":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "META":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "MSFT":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "PLTR":        {"tier": "T3",      "weekly_pool_usdc": 3000},
    "NATGAS":      {"tier": "T3",      "weekly_pool_usdc": 3000},
}

# Verified at task creation: 22 distinct markets across the four tiers.
assert len(POOL_BY_MARKET) == 22, "LPP pool must have 22 markets"

# Calibration anchor — observed share at our size during the 5-2 live
# run. Used to scale raw-share estimates from observer data so ranks
# across markets are comparable. Update this when re-running calibration.
SKHYNIX_BASELINE_SHARE = 0.086
SKHYNIX_BASELINE_SIZE_USDC = 50

# Cluster detection tolerances. Documented in the module docstring.
CLUSTER_TOLERANCE = 0.03
CLUSTER_MIN_OCCURRENCES = 3

# LPP classification thresholds. A cluster has to look like a real MM
# — not a momentary one-off — before it shapes our recommendations.
LPP_PERSISTENCE_PCT_FLOOR = 50.0   # appears in ≥ 50% of snapshots
LPP_TOTAL_OCCURRENCES_FLOOR = 10   # ≥ 10 events combined across both sides

# Anything ≥ this value is presumed to be institutional / non-LPP and
# excluded from the "competitor" tally.
INSTITUTIONAL_SIZE_USDC = 5000.0


# ---------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------


@dataclass
class LevelEvent:
    """One book level seen in one snapshot. Distance is from the
    same-side BBO (always non-negative; a level AT the BBO has 0)."""
    snapshot_idx: int
    side: str                 # "bid" / "ask"
    size_usdc: float
    distance_from_bbo_bp: float


@dataclass
class SizeCluster:
    """A group of level events whose sizes fall within ±tolerance.

    The classification fields are derived in ``finalize`` and are
    not strictly needed for the construction algorithm itself.
    """
    centroid_usdc: float
    events: List[LevelEvent] = field(default_factory=list)

    # Derived (filled by ``finalize``).
    bid_count: int = 0
    ask_count: int = 0
    is_two_sided: bool = False
    max_levels_per_snapshot: int = 0
    median_distance_from_bbo_bp: float = 0.0
    max_distance_from_bbo_bp: float = 0.0
    time_persistence_pct: float = 0.0
    estimated_mm_count: int = 1
    classification: str = "unknown"

    def finalize(self, total_snapshots: int) -> None:
        if not self.events:
            return
        self.bid_count = sum(1 for e in self.events if e.side == "bid")
        self.ask_count = sum(1 for e in self.events if e.side == "ask")
        self.is_two_sided = (
            self.bid_count >= CLUSTER_MIN_OCCURRENCES
            and self.ask_count >= CLUSTER_MIN_OCCURRENCES
        )

        per_snapshot: Dict[int, int] = defaultdict(int)
        for e in self.events:
            per_snapshot[e.snapshot_idx] += 1
        self.max_levels_per_snapshot = max(per_snapshot.values()) if per_snapshot else 0
        snapshots_seen = len(per_snapshot)
        self.time_persistence_pct = (
            (snapshots_seen / total_snapshots * 100.0) if total_snapshots else 0.0
        )

        distances = [e.distance_from_bbo_bp for e in self.events]
        self.median_distance_from_bbo_bp = (
            statistics.median(distances) if distances else 0.0
        )
        self.max_distance_from_bbo_bp = max(distances) if distances else 0.0

        # Heuristic: if a cluster is consistently spread across many
        # levels per snapshot it's likely one MM holding multiple
        # depth tiers; if it appears as a single level repeated across
        # many snapshots, that's likely several MMs sharing a price
        # convention. Conservative: presume 1 MM unless we see
        # ≥ 3 simultaneous levels with the same size.
        self.estimated_mm_count = (
            1 if self.max_levels_per_snapshot >= 3 else 1
        )

        self.classification = self._classify()

    def _classify(self) -> str:
        if self.centroid_usdc >= INSTITUTIONAL_SIZE_USDC:
            return "institutional"
        if not self.is_two_sided:
            return "one_sided_trader"
        total_events = self.bid_count + self.ask_count
        if (
            self.time_persistence_pct >= LPP_PERSISTENCE_PCT_FLOOR
            and total_events >= LPP_TOTAL_OCCURRENCES_FLOOR
        ):
            return "lpp_mm"
        return "unknown"


@dataclass
class MarketAggregate:
    """All observer state for a single market within an analysis window."""
    symbol: str
    snapshot_count: int = 0
    spread_bp_samples: List[float] = field(default_factory=list)
    mid_samples: List[float] = field(default_factory=list)
    level_events: List[LevelEvent] = field(default_factory=list)
    sessions_seen: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    first_ts_ms: Optional[int] = None
    last_ts_ms: Optional[int] = None


# ---------------------------------------------------------------------
# KR session inference
# ---------------------------------------------------------------------


KR_SESSIONS = (
    "KR_WEEKEND",
    "KR_MARKET_HOURS_AM",
    "KR_LUNCH_BREAK",
    "KR_MARKET_HOURS_PM",
    "KR_AFTER_CLOSE",
    "KR_OVERNIGHT",
    "KR_BEFORE_OPEN",
    "KR_PRE_OPEN_WITHDRAW",
)


def infer_kr_session(ts_ms: int) -> str:
    """Map a UTC ms timestamp to one of the 8 KR-equity sessions.

    Boundaries follow the task spec:
      WEEKEND          — Sat all-day UTC, Sun before 22:30, Fri ≥ 09:00
      MARKET_HOURS_AM  — Mon-Thu 00:00-04:00 (KST 09:00-13:00)
      LUNCH_BREAK      — Mon-Thu 04:00-05:00
      MARKET_HOURS_PM  — Mon-Thu 05:00-06:30
      AFTER_CLOSE      — Mon-Thu 06:30-09:00
      OVERNIGHT        — Mon-Thu 09:00-22:30, Sun ≥ 22:30 only at AFTER_CLOSE  (here: 09:00-22:30 falls back when not weekend)
      BEFORE_OPEN      — 22:30-23:30 (Sun-Thu)
      PRE_OPEN_WITHDRAW— 23:30-24:00 (Sun-Thu)
    """
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    weekday = dt.weekday()  # Mon=0 ... Sun=6
    minutes = dt.hour * 60 + dt.minute

    # Weekend overrides — checked first because they cross the regular
    # schedule's day boundaries.
    if weekday == 5:                          # Saturday all day
        return "KR_WEEKEND"
    if weekday == 6 and minutes < 22 * 60 + 30:  # Sunday before 22:30
        return "KR_WEEKEND"
    if weekday == 4 and minutes >= 9 * 60:    # Friday ≥ 09:00
        return "KR_WEEKEND"

    # Mon-Thu, plus Fri before 09:00 and Sun from 22:30.
    if minutes < 4 * 60:
        return "KR_MARKET_HOURS_AM"
    if minutes < 5 * 60:
        return "KR_LUNCH_BREAK"
    if minutes < 6 * 60 + 30:
        return "KR_MARKET_HOURS_PM"
    if minutes < 9 * 60:
        return "KR_AFTER_CLOSE"
    if minutes < 22 * 60 + 30:
        return "KR_OVERNIGHT"
    if minutes < 23 * 60 + 30:
        return "KR_BEFORE_OPEN"
    return "KR_PRE_OPEN_WITHDRAW"


# ---------------------------------------------------------------------
# JSONL parsing
# ---------------------------------------------------------------------


def _to_float(v: Any) -> Optional[float]:
    """Permissive ``Decimal``-or-str → float coercer; returns None on
    garbage. Observer outputs prices/sizes as strings to keep precision
    so parsers can choose; we work in float for analysis since rounding
    error here doesn't compound."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _iter_snapshots(jsonl_path: Path) -> Iterator[Dict[str, Any]]:
    """Yield ``event=snapshot`` records, skipping blanks and other types.

    Defensive: a malformed line (bad JSON) is logged-and-skipped so a
    single bad write doesn't sink the whole analysis.
    """
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                # Stay silent here; the caller can re-grep with verbose
                # if it needs forensics. Observer files are typically
                # 100k+ lines so we don't want to spam stderr per blip.
                continue
            if not isinstance(ev, dict):
                continue
            if ev.get("event") != "snapshot":
                continue
            yield ev


def aggregate_jsonl(jsonl_paths: Sequence[Path]) -> Tuple[Dict[str, MarketAggregate], Dict[str, Any]]:
    """Build per-market aggregates from one or more observer JSONL files.

    Returns ``(market_aggregates, run_meta)``. ``run_meta`` carries
    earliest/latest ts and total snapshot counts so the markdown header
    can describe the window without re-walking the data.
    """
    aggregates: Dict[str, MarketAggregate] = {}
    snapshots_total = 0
    earliest_ts: Optional[int] = None
    latest_ts: Optional[int] = None

    for path in jsonl_paths:
        for ev in _iter_snapshots(path):
            ts_ms = ev.get("ts_ms")
            if not isinstance(ts_ms, (int, float)):
                continue
            ts_ms = int(ts_ms)
            session = infer_kr_session(ts_ms)
            books = ev.get("books") or {}
            if not isinstance(books, dict):
                continue
            snapshots_total += 1
            if earliest_ts is None or ts_ms < earliest_ts:
                earliest_ts = ts_ms
            if latest_ts is None or ts_ms > latest_ts:
                latest_ts = ts_ms

            for symbol, book in books.items():
                if not isinstance(book, dict):
                    continue
                best_bid = _to_float(book.get("best_bid"))
                best_ask = _to_float(book.get("best_ask"))
                mid = _to_float(book.get("mid"))
                if best_bid is None or best_ask is None or mid is None or mid <= 0:
                    continue

                agg = aggregates.setdefault(symbol, MarketAggregate(symbol=symbol))
                agg.snapshot_count += 1
                agg.sessions_seen[session] += 1
                if agg.first_ts_ms is None or ts_ms < agg.first_ts_ms:
                    agg.first_ts_ms = ts_ms
                if agg.last_ts_ms is None or ts_ms > agg.last_ts_ms:
                    agg.last_ts_ms = ts_ms

                spread_bp = book.get("spread_bp")
                if isinstance(spread_bp, (int, float)):
                    agg.spread_bp_samples.append(float(spread_bp))
                agg.mid_samples.append(mid)

                snap_idx = agg.snapshot_count - 1
                _absorb_levels(agg, snap_idx, "bid", best_bid, mid, book.get("bid_levels"))
                _absorb_levels(agg, snap_idx, "ask", best_ask, mid, book.get("ask_levels"))

    run_meta = {
        "snapshots_total": snapshots_total,
        "earliest_ts_ms": earliest_ts,
        "latest_ts_ms": latest_ts,
        "duration_hours": (
            (latest_ts - earliest_ts) / 3_600_000.0
            if earliest_ts is not None and latest_ts is not None
            else 0.0
        ),
        "input_paths": [str(p) for p in jsonl_paths],
    }
    return aggregates, run_meta


def _absorb_levels(
    agg: MarketAggregate,
    snap_idx: int,
    side: str,
    bbo_price: float,
    mid: float,
    levels: Any,
) -> None:
    if not isinstance(levels, list):
        return
    for lvl in levels:
        if not isinstance(lvl, dict):
            continue
        size_usdc = _to_float(lvl.get("size_usdc"))
        price = _to_float(lvl.get("price"))
        if size_usdc is None or price is None or size_usdc <= 0:
            continue
        if side == "bid":
            distance_bp = max(0.0, (bbo_price - price) / mid * 10000.0)
        else:
            distance_bp = max(0.0, (price - bbo_price) / mid * 10000.0)
        agg.level_events.append(LevelEvent(
            snapshot_idx=snap_idx,
            side=side,
            size_usdc=size_usdc,
            distance_from_bbo_bp=distance_bp,
        ))


# ---------------------------------------------------------------------
# Cluster detection
# ---------------------------------------------------------------------


def cluster_sizes(
    events: Sequence[LevelEvent],
    tolerance: float = CLUSTER_TOLERANCE,
    min_count: int = CLUSTER_MIN_OCCURRENCES,
) -> List[SizeCluster]:
    """Group level events by USDC size, keeping cluster width ≤ 2× tolerance.

    Anchored on the smallest member: a new event joins iff its size is
    within ``2 × tolerance`` of the cluster's smallest event. This
    guards against drift over a long sorted run (each step would only
    check against the immediately-previous event) while still being
    cheap and deterministic.
    """
    if not events:
        return []
    sorted_events = sorted(events, key=lambda e: e.size_usdc)
    out: List[SizeCluster] = []
    cur: List[LevelEvent] = []
    anchor_size: Optional[float] = None
    width = 1.0 + 2.0 * tolerance

    for ev in sorted_events:
        if anchor_size is None:
            cur = [ev]
            anchor_size = ev.size_usdc
            continue
        if anchor_size > 0 and ev.size_usdc / anchor_size <= width:
            cur.append(ev)
        else:
            if len(cur) >= min_count:
                out.append(_build_cluster(cur))
            cur = [ev]
            anchor_size = ev.size_usdc
    if len(cur) >= min_count:
        out.append(_build_cluster(cur))
    # Newest LPP-MM clusters at the top by total events; helps the
    # markdown report read correctly when a market has 4-5 visible
    # MMs.
    out.sort(key=lambda c: -(c.bid_count + c.ask_count + len(c.events)))
    return out


def _build_cluster(events: List[LevelEvent]) -> SizeCluster:
    """Construct a cluster with side-counts pre-populated.

    ``bid_count`` and ``ask_count`` only depend on the events
    themselves, so we populate them at construction. ``finalize``
    handles the cross-snapshot stats (persistence, max-levels) which
    require the total snapshot count to be known.
    """
    sizes = [e.size_usdc for e in events]
    centroid = statistics.median(sizes)
    bid_count = sum(1 for e in events if e.side == "bid")
    ask_count = sum(1 for e in events if e.side == "ask")
    return SizeCluster(
        centroid_usdc=centroid,
        events=events,
        bid_count=bid_count,
        ask_count=ask_count,
    )


# ---------------------------------------------------------------------
# Per-market analysis
# ---------------------------------------------------------------------


@dataclass
class YourPosition:
    """How a hypothetical own-quote at ``your_size_usdc`` would rank
    against the visible competitor clusters."""
    your_size_usdc: float
    rank_in_cluster_distribution: str
    main_competitor_cluster_usdc: Optional[float]
    competitor_distance_from_bbo_bp: Optional[float]
    recommended_distance_bp: Optional[float]
    saturation_level: str
    lpp_eligible_existing_usdc: float
    raw_share_estimate: Optional[float]


def derive_your_position(
    your_size_usdc: float,
    clusters: Sequence[SizeCluster],
) -> YourPosition:
    """Build the ``your_relative_position`` summary.

    Saturation: based on combined LPP-eligible USDC. ``thin`` when a
    new $50 quote can take ≥ 25% of the visible LPP depth, ``moderate``
    in the 5-25% range, ``saturated`` below 5%.
    """
    lpp_clusters = [c for c in clusters if c.classification == "lpp_mm"]
    # Eligible competitor depth — sum of cluster centroids. We compete
    # one side at a time at the BBO-front, so what matters is the
    # typical per-side notional an LPP MM rests at the front of the
    # queue. A single MM holding 4 bid + 4 ask levels at $500 each is
    # still ~$500 of competitor at the front; the deeper levels sit
    # behind us in the queue and don't divide the maker reward at our
    # tick. Multiplying by ``max_levels_per_snapshot`` here would
    # double-count the same MM's depth.
    eligible_total = sum(c.centroid_usdc for c in lpp_clusters)

    main = lpp_clusters[0] if lpp_clusters else None
    sizes_sorted = sorted([c.centroid_usdc for c in lpp_clusters], reverse=True)
    rank_label = _rank_in(sizes_sorted, your_size_usdc)

    raw_share: Optional[float]
    if eligible_total > 0:
        raw_share = your_size_usdc / (your_size_usdc + eligible_total)
    elif lpp_clusters:
        raw_share = 1.0  # only us
    else:
        raw_share = None

    if eligible_total <= 0:
        saturation = "thin"
    elif your_size_usdc / (your_size_usdc + eligible_total) >= 0.25:
        saturation = "thin"
    elif your_size_usdc / (your_size_usdc + eligible_total) >= 0.05:
        saturation = "moderate"
    else:
        saturation = "saturated"

    # Recommend matching the main MM's median distance — a typical
    # `passive at MM_distance` choice. If only one MM, undercutting by
    # 1 bp is plausible but risky; keep the recommendation conservative.
    recommended = main.median_distance_from_bbo_bp if main else None

    return YourPosition(
        your_size_usdc=your_size_usdc,
        rank_in_cluster_distribution=rank_label,
        main_competitor_cluster_usdc=main.centroid_usdc if main else None,
        competitor_distance_from_bbo_bp=(
            main.median_distance_from_bbo_bp if main else None
        ),
        recommended_distance_bp=recommended,
        saturation_level=saturation,
        lpp_eligible_existing_usdc=eligible_total,
        raw_share_estimate=raw_share,
    )


def _rank_in(sizes_descending: Sequence[float], my_size: float) -> str:
    """Return a coarse rank label: smallest / median / large / lonely."""
    if not sizes_descending:
        return "lonely"
    n = len(sizes_descending)
    smaller_or_equal = sum(1 for s in sizes_descending if s <= my_size)
    if smaller_or_equal == 0:
        return "smallest"
    if smaller_or_equal >= n:
        return "largest"
    if smaller_or_equal * 2 < n:
        return "below_median"
    if smaller_or_equal * 2 > n:
        return "above_median"
    return "median"


# ---------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------


def compute_calibration(
    profiles: Dict[str, Any],
) -> Tuple[float, Optional[float]]:
    """Return ``(multiplier, observed_skhynix_raw_share)``.

    SKHYNIX is the calibration anchor: the live 5-2 run measured an
    8.6% share at size $50. Whatever raw share the analyzer derives
    for SKHYNIX, we multiply by ``0.086 / raw`` so all markets'
    analyzer shares are comparable against the live baseline.
    """
    sk = profiles.get("SKHYNIXUSD")
    if not sk:
        return 1.0, None
    pos = sk.get("your_relative_position", {})
    raw = pos.get("raw_share_estimate")
    if raw is None or raw <= 0:
        return 1.0, None
    multiplier = SKHYNIX_BASELINE_SHARE / raw
    return multiplier, raw


def estimated_daily_reward(
    market: str,
    calibrated_share: float,
) -> float:
    """``share × daily_pool``. Daily pool = weekly / 7."""
    pool = POOL_BY_MARKET.get(market, {}).get("weekly_pool_usdc", 0.0)
    return calibrated_share * (pool / 7.0)


# ---------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------


def build_market_profiles(
    aggregates: Dict[str, MarketAggregate],
    your_size_usdc: float,
) -> Dict[str, Dict[str, Any]]:
    """For each market: cluster events → finalize → derive position."""
    profiles: Dict[str, Dict[str, Any]] = {}
    for symbol, agg in aggregates.items():
        clusters = cluster_sizes(agg.level_events)
        for c in clusters:
            c.finalize(total_snapshots=agg.snapshot_count)
        # Resort post-finalize so LPP MMs come first (classification +
        # event count are now meaningful).
        clusters.sort(
            key=lambda c: (
                0 if c.classification == "lpp_mm" else 1,
                -(c.bid_count + c.ask_count),
            )
        )
        position = derive_your_position(your_size_usdc, clusters)

        profiles[symbol] = {
            "symbol": symbol,
            "tier": POOL_BY_MARKET.get(symbol, {}).get("tier"),
            "weekly_pool_usdc": POOL_BY_MARKET.get(symbol, {}).get("weekly_pool_usdc"),
            "snapshots": agg.snapshot_count,
            "first_ts_ms": agg.first_ts_ms,
            "last_ts_ms": agg.last_ts_ms,
            "median_spread_bp": (
                statistics.median(agg.spread_bp_samples)
                if agg.spread_bp_samples else None
            ),
            "median_mid": (
                statistics.median(agg.mid_samples) if agg.mid_samples else None
            ),
            "sessions_seen": dict(agg.sessions_seen),
            "mm_clusters": [_cluster_to_dict(c) for c in clusters],
            "your_relative_position": asdict(position),
        }
    return profiles


def _cluster_to_dict(c: SizeCluster) -> Dict[str, Any]:
    return {
        "cluster_size_usdc": round(c.centroid_usdc, 2),
        "bid_levels_seen": c.bid_count,
        "ask_levels_seen": c.ask_count,
        "is_two_sided": c.is_two_sided,
        "max_levels_per_snapshot": c.max_levels_per_snapshot,
        "median_distance_from_bbo_bp": round(c.median_distance_from_bbo_bp, 2),
        "max_distance_from_bbo_bp": round(c.max_distance_from_bbo_bp, 2),
        "time_persistence_pct": round(c.time_persistence_pct, 1),
        "estimated_mm_count": c.estimated_mm_count,
        "classification": c.classification,
    }


# ---------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------


def render_markdown(
    run_meta: Dict[str, Any],
    profiles: Dict[str, Dict[str, Any]],
    multiplier: float,
    raw_skhynix_share: Optional[float],
    your_size_usdc: float,
) -> str:
    lines: List[str] = []
    lines.append("# LPP Daily Observer Summary")
    lines.append("")

    earliest = run_meta.get("earliest_ts_ms")
    latest = run_meta.get("latest_ts_ms")
    if earliest and latest:
        e_str = datetime.fromtimestamp(earliest / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        l_str = datetime.fromtimestamp(latest / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        hrs = run_meta.get("duration_hours") or 0.0
        lines.append(f"## Period: {e_str} → {l_str} UTC  ({hrs:.2f}h, {run_meta.get('snapshots_total', 0)} snapshots)")
    else:
        lines.append("## Period: (no snapshots parsed)")
    lines.append("")

    if raw_skhynix_share is not None:
        lines.append(
            f"## Calibration: SKHYNIX raw share = {raw_skhynix_share*100:.2f}%, "
            f"baseline (live 5-2) = {SKHYNIX_BASELINE_SHARE*100:.1f}%, "
            f"multiplier = {multiplier:.2f}"
        )
    else:
        lines.append("## Calibration: SKHYNIX baseline unavailable — multiplier = 1.0 (raw shares only)")
    lines.append("")
    lines.append(f"Your nominal size: ${your_size_usdc:.0f}")
    lines.append("")

    # Per-market table.
    lines.append("## Per-Market Summary")
    lines.append("")
    lines.append("| Market | Tier | Pool $ | Spread (bp) | Main MM | MM dist (bp) | Your rank | Saturation | Calib share | Daily reward |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for symbol in sorted(profiles, key=lambda s: -(profiles[s].get("weekly_pool_usdc") or 0)):
        prof = profiles[symbol]
        pos = prof["your_relative_position"]
        spread = prof.get("median_spread_bp")
        spread_str = f"{spread:.1f}" if spread is not None else "—"
        main_size = pos.get("main_competitor_cluster_usdc")
        main_str = f"${main_size:.0f}" if main_size else "(none)"
        mm_dist = pos.get("competitor_distance_from_bbo_bp")
        dist_str = f"{mm_dist:.1f}" if mm_dist is not None else "—"
        raw = pos.get("raw_share_estimate")
        cal_share = (raw or 0) * multiplier
        cal_share_str = f"{cal_share*100:.1f}%" if raw is not None else "—"
        reward = estimated_daily_reward(symbol, cal_share if raw is not None else 0)
        reward_str = f"${reward:.0f}" if raw is not None else "—"
        lines.append(
            f"| {symbol} | {prof.get('tier') or '—'} | "
            f"{prof.get('weekly_pool_usdc') or '—'} | {spread_str} | {main_str} | "
            f"{dist_str} | {pos.get('rank_in_cluster_distribution')} | "
            f"{pos.get('saturation_level')} | {cal_share_str} | {reward_str} |"
        )
    lines.append("")

    # Highlight markets where size is mismatched.
    too_small: List[Tuple[str, float, float]] = []
    competitive: List[Tuple[str, float, float]] = []
    for symbol, prof in profiles.items():
        pos = prof["your_relative_position"]
        main = pos.get("main_competitor_cluster_usdc") or 0.0
        if main <= 0:
            continue
        ratio = main / your_size_usdc
        if ratio >= 5:
            too_small.append((symbol, main, ratio))
        elif ratio <= 2:
            competitive.append((symbol, main, ratio))

    if too_small:
        lines.append(f"## Markets where size ${your_size_usdc:.0f} is too small")
        lines.append("")
        for s, m, r in sorted(too_small, key=lambda x: -x[2]):
            lines.append(f"- **{s}**: main MM ${m:.0f} = {r:.1f}× our size — share will stay low until size grows")
        lines.append("")

    if competitive:
        lines.append(f"## Markets where size ${your_size_usdc:.0f} is competitive")
        lines.append("")
        for s, m, r in sorted(competitive, key=lambda x: x[2]):
            lines.append(f"- **{s}**: main MM ${m:.0f} = {r:.1f}× our size — quoting at MM distance is enough")
        lines.append("")

    # Top-5 by daily reward.
    rewards: List[Tuple[str, float]] = []
    for symbol, prof in profiles.items():
        pos = prof["your_relative_position"]
        raw = pos.get("raw_share_estimate")
        if raw is None:
            continue
        cal_share = raw * multiplier
        rewards.append((symbol, estimated_daily_reward(symbol, cal_share)))
    if rewards:
        rewards.sort(key=lambda x: -x[1])
        lines.append(f"## Top 5 markets by projected daily reward at size ${your_size_usdc:.0f}")
        lines.append("")
        for s, r in rewards[:5]:
            lines.append(f"- {s}: ${r:.0f}/day")
        lines.append("")

    # MM-by-MM insights.
    lines.append("## MM behaviour insights (per market, top LPP cluster)")
    lines.append("")
    for symbol in sorted(profiles):
        clusters = profiles[symbol].get("mm_clusters") or []
        lpp = [c for c in clusters if c["classification"] == "lpp_mm"]
        if not lpp:
            continue
        c = lpp[0]
        lines.append(
            f"- **{symbol}** MM_{symbol}_${int(c['cluster_size_usdc'])}: "
            f"size=${c['cluster_size_usdc']:.0f}, "
            f"distance={c['median_distance_from_bbo_bp']:.1f}bp, "
            f"persistence={c['time_persistence_pct']:.0f}%, "
            f"max_levels/snap={c['max_levels_per_snapshot']}"
        )
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Aggregate observer JSONL → MM behaviour profiles + Markdown summary."
    )
    p.add_argument(
        "input",
        nargs="+",
        help="Path(s) to observer .jsonl file(s). Multiple paths are folded into one analysis.",
    )
    p.add_argument(
        "--your-size-usdc",
        type=float,
        default=50.0,
        help="Hypothetical own-quote per-side size (default: 50).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Where to drop .analysis.json + .mm_profile.json (default: dir of first input).",
    )
    p.add_argument(
        "--no-markdown",
        action="store_true",
        help="Suppress the stdout markdown report.",
    )
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    inputs = [Path(p) for p in args.input]
    for path in inputs:
        if not path.exists():
            print(f"input not found: {path}", file=sys.stderr)
            return 2

    aggregates, run_meta = aggregate_jsonl(inputs)
    profiles = build_market_profiles(aggregates, args.your_size_usdc)
    multiplier, raw_skhynix = compute_calibration(profiles)

    out_dir = Path(args.output_dir) if args.output_dir else inputs[0].parent
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = inputs[0].stem
    analysis_path = out_dir / f"{stem}.analysis.json"
    mm_profile_path = out_dir / f"{stem}.mm_profile.json"

    analysis_payload = {
        "run_meta": run_meta,
        "your_size_usdc": args.your_size_usdc,
        "calibration": {
            "multiplier": multiplier,
            "raw_skhynix_share": raw_skhynix,
            "baseline_share": SKHYNIX_BASELINE_SHARE,
            "baseline_size_usdc": SKHYNIX_BASELINE_SIZE_USDC,
        },
        "summary_per_market": {
            sym: {
                "tier": prof.get("tier"),
                "weekly_pool_usdc": prof.get("weekly_pool_usdc"),
                "median_spread_bp": prof.get("median_spread_bp"),
                "your_position": prof.get("your_relative_position"),
                "calibrated_share": (
                    (prof["your_relative_position"].get("raw_share_estimate") or 0) * multiplier
                ),
                "estimated_daily_reward_usdc": estimated_daily_reward(
                    sym,
                    (prof["your_relative_position"].get("raw_share_estimate") or 0) * multiplier,
                ),
            }
            for sym, prof in profiles.items()
        },
    }

    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(analysis_payload, f, indent=2, ensure_ascii=False, default=str)
    with open(mm_profile_path, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2, ensure_ascii=False, default=str)

    if not args.no_markdown:
        print(render_markdown(
            run_meta, profiles, multiplier, raw_skhynix, args.your_size_usdc,
        ))

    print(f"\nWrote: {analysis_path}", file=sys.stderr)
    print(f"Wrote: {mm_profile_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
