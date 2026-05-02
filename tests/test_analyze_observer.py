"""Unit tests for ``scripts.analyze_observer``.

Coverage targets:
  * KR session boundaries (8 sessions × edge cases)
  * 22-market LPP pool exhaustiveness
  * JSONL parsing tolerance to malformed lines
  * Calibration anchored on SKHYNIX
  * Per-snapshot ↔ per-market aggregation
  * Cluster detection — exact-match, ±3% tolerance, mixed sizes
  * Two-sided vs one-sided classification
  * Persistence-pct tracks cross-snapshot occurrence
  * ``derive_your_position`` ranking + recommended distance + lpp_eligible
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts.analyze_observer import (  # noqa: E402
    CLUSTER_MIN_OCCURRENCES,
    CLUSTER_TOLERANCE,
    LPP_PERSISTENCE_PCT_FLOOR,
    POOL_BY_MARKET,
    SKHYNIX_BASELINE_SHARE,
    LevelEvent,
    SizeCluster,
    aggregate_jsonl,
    build_market_profiles,
    cluster_sizes,
    compute_calibration,
    derive_your_position,
    estimated_daily_reward,
    infer_kr_session,
)


# ----- KR session inference ------------------------------------------


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def test_session_inference_covers_all_eight_sessions():
    """Every session label maps to a real timestamp inside its window."""
    # 2026-04-13 is a Monday.
    assert infer_kr_session(_ts(2026, 4, 13, 1, 30)) == "KR_MARKET_HOURS_AM"
    assert infer_kr_session(_ts(2026, 4, 13, 4, 30)) == "KR_LUNCH_BREAK"
    assert infer_kr_session(_ts(2026, 4, 13, 5, 45)) == "KR_MARKET_HOURS_PM"
    assert infer_kr_session(_ts(2026, 4, 13, 7, 0)) == "KR_AFTER_CLOSE"
    assert infer_kr_session(_ts(2026, 4, 13, 12, 0)) == "KR_OVERNIGHT"
    assert infer_kr_session(_ts(2026, 4, 13, 23, 0)) == "KR_BEFORE_OPEN"
    assert infer_kr_session(_ts(2026, 4, 13, 23, 45)) == "KR_PRE_OPEN_WITHDRAW"
    # 2026-04-18 is a Saturday → weekend all day.
    assert infer_kr_session(_ts(2026, 4, 18, 12, 0)) == "KR_WEEKEND"


def test_session_inference_friday_after_9_is_weekend():
    """Friday ≥ 09:00 UTC is weekend per the task spec; before 09:00
    Friday is still in the regular schedule (06:30 - 09:00 = AFTER_CLOSE)."""
    # 2026-04-17 is a Friday.
    assert infer_kr_session(_ts(2026, 4, 17, 8, 59)) == "KR_AFTER_CLOSE"
    assert infer_kr_session(_ts(2026, 4, 17, 9, 0)) == "KR_WEEKEND"
    assert infer_kr_session(_ts(2026, 4, 17, 18, 0)) == "KR_WEEKEND"


def test_session_inference_sunday_2230_returns_to_regular_schedule():
    """Sunday before 22:30 is weekend; from 22:30 the regular sessions
    apply (BEFORE_OPEN then PRE_OPEN_WITHDRAW)."""
    # 2026-04-19 is a Sunday.
    assert infer_kr_session(_ts(2026, 4, 19, 22, 0)) == "KR_WEEKEND"
    assert infer_kr_session(_ts(2026, 4, 19, 22, 30)) == "KR_BEFORE_OPEN"
    assert infer_kr_session(_ts(2026, 4, 19, 23, 30)) == "KR_PRE_OPEN_WITHDRAW"


# ----- LPP pool table ------------------------------------------------


def test_lpp_pool_has_exactly_22_markets():
    assert len(POOL_BY_MARKET) == 22


def test_lpp_pool_includes_all_expected_kr_rwa_markets():
    for sym in ("SKHYNIXUSD", "SAMSUNGUSD", "HYUNDAIUSD"):
        assert sym in POOL_BY_MARKET
        assert POOL_BY_MARKET[sym]["tier"] == "T6_KR"
        assert POOL_BY_MARKET[sym]["weekly_pool_usdc"] == 12269


# ----- JSONL parsing -------------------------------------------------


def test_jsonl_parsing_skips_malformed_lines(tmp_path: Path):
    """A file with one good snapshot, one blank line, one non-JSON line,
    and one non-snapshot event should still parse the one good snapshot."""
    sample = {
        "event": "snapshot",
        "ts_ms": _ts(2026, 4, 13, 1, 30),
        "books": {
            "SKHYNIXUSD": {
                "best_bid": "100.0",
                "best_ask": "100.2",
                "mid": "100.1",
                "spread_bp": 20.0,
                "bid_levels": [{"price": "100.0", "size_base": "5", "size_usdc": "500"}],
                "ask_levels": [{"price": "100.2", "size_base": "5", "size_usdc": "501"}],
            }
        },
    }
    path = tmp_path / "obs.jsonl"
    path.write_text(
        "\n"
        "this is not json\n"
        + json.dumps(sample) + "\n"
        + json.dumps({"event": "session_start"}) + "\n"
        "\n",
        encoding="utf-8",
    )
    aggs, meta = aggregate_jsonl([path])
    assert "SKHYNIXUSD" in aggs
    assert aggs["SKHYNIXUSD"].snapshot_count == 1
    assert meta["snapshots_total"] == 1


# ----- cluster detection ---------------------------------------------


def _events(*sizes_and_sides):
    """Build a list of LevelEvents.

    Each pair is ``(size_usdc, side)``; snapshot_idx auto-increments,
    distance defaults to 0 (suit-able for clustering tests that don't
    care about distance).
    """
    out = []
    for i, (size, side) in enumerate(sizes_and_sides):
        out.append(LevelEvent(snapshot_idx=i, side=side, size_usdc=size, distance_from_bbo_bp=0.0))
    return out


def test_size_cluster_detection_simple_three_equal():
    """[500, 500, 500, 34] → one cluster of $500."""
    events = _events((500, "bid"), (500, "ask"), (500, "bid"), (34, "ask"))
    clusters = cluster_sizes(events)
    # The 34 is alone (< min_count) so it's dropped.
    assert len(clusters) == 1
    assert abs(clusters[0].centroid_usdc - 500) < 1


def test_size_cluster_detection_within_3pct_tolerance():
    """500, 497, 503 should all join one cluster (drift within ±3%)."""
    events = _events((500, "bid"), (497, "ask"), (503, "bid"))
    clusters = cluster_sizes(events)
    assert len(clusters) == 1
    assert clusters[0].bid_count == 2
    assert clusters[0].ask_count == 1


def test_size_cluster_detection_separates_distinct_sizes():
    """500-cluster and 1500-cluster do NOT merge."""
    events = _events(
        (500, "bid"), (500, "ask"), (500, "bid"),
        (1500, "bid"), (1500, "ask"), (1500, "bid"),
    )
    clusters = cluster_sizes(events)
    assert len(clusters) == 2
    centroids = sorted(c.centroid_usdc for c in clusters)
    assert centroids[0] == 500
    assert centroids[1] == 1500


# ----- classification ------------------------------------------------


def test_two_sided_lpp_classified_as_lpp_mm():
    """Cluster appearing on both sides for ≥ 50% of snapshots and
    ≥ 10 events total is ``lpp_mm``."""
    events = []
    for i in range(20):
        events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=500, distance_from_bbo_bp=5))
        events.append(LevelEvent(snapshot_idx=i, side="ask", size_usdc=500, distance_from_bbo_bp=5))
    clusters = cluster_sizes(events)
    assert len(clusters) == 1
    clusters[0].finalize(total_snapshots=20)
    assert clusters[0].is_two_sided is True
    assert clusters[0].time_persistence_pct == 100.0
    assert clusters[0].classification == "lpp_mm"


def test_one_sided_cluster_classified_as_one_sided_trader():
    """No matter how persistent, a bid-only cluster is not LPP."""
    events = [
        LevelEvent(snapshot_idx=i, side="bid", size_usdc=500, distance_from_bbo_bp=5)
        for i in range(20)
    ]
    clusters = cluster_sizes(events)
    assert len(clusters) == 1
    clusters[0].finalize(total_snapshots=20)
    assert clusters[0].is_two_sided is False
    assert clusters[0].classification == "one_sided_trader"


def test_large_size_classified_as_institutional():
    """A two-sided cluster well above $5k is treated as institutional
    (excluded from the LPP-competitor tally)."""
    events = []
    for i in range(15):
        events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=10_000, distance_from_bbo_bp=2))
        events.append(LevelEvent(snapshot_idx=i, side="ask", size_usdc=10_000, distance_from_bbo_bp=2))
    clusters = cluster_sizes(events)
    clusters[0].finalize(total_snapshots=15)
    assert clusters[0].classification == "institutional"


def test_persistence_pct_tracks_cross_snapshot_count():
    """Persistence is (snapshots_seeing_cluster / total_snapshots) × 100."""
    # Cluster appears in 5 of 20 snapshots → 25% — below LPP floor.
    events = []
    for i in range(5):
        for _ in range(2):  # bid + ask in each
            events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=500, distance_from_bbo_bp=5))
            events.append(LevelEvent(snapshot_idx=i, side="ask", size_usdc=500, distance_from_bbo_bp=5))
    clusters = cluster_sizes(events)
    clusters[0].finalize(total_snapshots=20)
    assert clusters[0].time_persistence_pct == 25.0
    # Below 50% floor → not classified as lpp_mm.
    assert clusters[0].classification != "lpp_mm"


# ----- your-position derivation --------------------------------------


def test_derive_your_position_uses_main_lpp_for_recommendations():
    """The largest LPP cluster's median distance becomes the
    recommended distance; ``main_competitor_cluster_usdc`` is its size."""
    events = []
    for i in range(20):
        events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=500, distance_from_bbo_bp=7))
        events.append(LevelEvent(snapshot_idx=i, side="ask", size_usdc=500, distance_from_bbo_bp=7))
    clusters = cluster_sizes(events)
    clusters[0].finalize(total_snapshots=20)

    pos = derive_your_position(your_size_usdc=50, clusters=clusters)
    assert pos.main_competitor_cluster_usdc == 500
    assert pos.competitor_distance_from_bbo_bp == 7
    assert pos.recommended_distance_bp == 7


def test_derive_your_position_excludes_one_sided_from_eligible_total():
    """``lpp_eligible_existing_usdc`` only sums LPP-classified clusters."""
    events = []
    for i in range(20):
        events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=500, distance_from_bbo_bp=5))
        events.append(LevelEvent(snapshot_idx=i, side="ask", size_usdc=500, distance_from_bbo_bp=5))
    # Plus a one-sided $2000 trader spamming bids.
    for i in range(20):
        events.append(LevelEvent(snapshot_idx=i, side="bid", size_usdc=2000, distance_from_bbo_bp=15))
    clusters = cluster_sizes(events)
    for c in clusters:
        c.finalize(total_snapshots=20)

    pos = derive_your_position(your_size_usdc=50, clusters=clusters)
    # The 2000 one-sided cluster should NOT be in eligible_total.
    assert pos.main_competitor_cluster_usdc == 500
    # 500 × max_levels_per_snapshot. With 1 bid + 1 ask per snap →
    # max_levels = 1, so eligible = 500 × 1 = 500.
    assert pos.lpp_eligible_existing_usdc == 500.0


# ----- calibration ---------------------------------------------------


def test_calibration_multiplier_uses_skhynix_baseline():
    """If SKHYNIX raw share = 4.3% and baseline = 8.6%, multiplier = 2.0."""
    profiles = {
        "SKHYNIXUSD": {
            "your_relative_position": {"raw_share_estimate": 0.043}
        }
    }
    multiplier, raw = compute_calibration(profiles)
    assert raw == 0.043
    assert multiplier == pytest.approx(SKHYNIX_BASELINE_SHARE / 0.043)
    # Sanity: applying the multiplier reproduces the baseline.
    assert 0.043 * multiplier == pytest.approx(SKHYNIX_BASELINE_SHARE)


def test_calibration_falls_back_to_one_when_skhynix_absent():
    """Without a SKHYNIX profile, raw share is None and multiplier=1.0."""
    multiplier, raw = compute_calibration({})
    assert raw is None
    assert multiplier == 1.0


# ----- per-session aggregation + estimated reward ---------------------


def test_per_session_aggregation_via_jsonl(tmp_path: Path):
    """Snapshot at Monday 01:30 UTC is KR_MARKET_HOURS_AM; the
    aggregator records that session for SKHYNIX."""
    sample = {
        "event": "snapshot",
        "ts_ms": _ts(2026, 4, 13, 1, 30),
        "books": {
            "SKHYNIXUSD": {
                "best_bid": "900",
                "best_ask": "900.5",
                "mid": "900.25",
                "spread_bp": 5.5,
                "bid_levels": [{"price": "900", "size_base": "0.555", "size_usdc": "500"}],
                "ask_levels": [{"price": "900.5", "size_base": "0.555", "size_usdc": "500"}],
            }
        },
    }
    path = tmp_path / "obs.jsonl"
    path.write_text(json.dumps(sample) + "\n", encoding="utf-8")
    aggs, _ = aggregate_jsonl([path])
    assert aggs["SKHYNIXUSD"].sessions_seen["KR_MARKET_HOURS_AM"] == 1


def test_estimated_daily_reward_uses_pool_table():
    """daily reward = share × (weekly_pool / 7)."""
    # SAMSUNG pool $12269/wk → $1752.71/day.
    assert estimated_daily_reward("SAMSUNGUSD", 0.10) == pytest.approx(12269 / 7 * 0.10)
    # Unknown market → 0.
    assert estimated_daily_reward("DOESNOTEXIST", 0.5) == 0.0


# ----- end-to-end smoke ----------------------------------------------


def test_end_to_end_pipeline_against_synthetic_jsonl(tmp_path: Path):
    """One synthetic snapshot containing two markets exercises the
    aggregator + clustering + position derivation in one shot."""
    snap = {
        "event": "snapshot",
        "ts_ms": _ts(2026, 4, 13, 1, 30),
        "books": {
            "SKHYNIXUSD": {
                "best_bid": "900",
                "best_ask": "900.5",
                "mid": "900.25",
                "spread_bp": 5.5,
                "bid_levels": [
                    {"price": "900", "size_base": "0.55", "size_usdc": "500"},
                    {"price": "899.95", "size_base": "0.55", "size_usdc": "500"},
                    {"price": "899.9", "size_base": "0.55", "size_usdc": "500"},
                ],
                "ask_levels": [
                    {"price": "900.5", "size_base": "0.55", "size_usdc": "500"},
                    {"price": "900.55", "size_base": "0.55", "size_usdc": "500"},
                    {"price": "900.6", "size_base": "0.55", "size_usdc": "500"},
                ],
            },
        },
    }
    # Repeat the same snapshot 12 times across ts so persistence
    # passes the LPP threshold.
    path = tmp_path / "obs.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for k in range(12):
            snap = dict(snap)
            snap["ts_ms"] = _ts(2026, 4, 13, 1, 30 + k)
            f.write(json.dumps(snap) + "\n")
    aggs, meta = aggregate_jsonl([path])
    profiles = build_market_profiles(aggs, your_size_usdc=50.0)
    assert meta["snapshots_total"] == 12
    assert "SKHYNIXUSD" in profiles
    sk = profiles["SKHYNIXUSD"]
    # Should detect a $500 LPP MM.
    lpp = [c for c in sk["mm_clusters"] if c["classification"] == "lpp_mm"]
    assert lpp, f"no LPP cluster detected; clusters={sk['mm_clusters']}"
    assert abs(lpp[0]["cluster_size_usdc"] - 500) < 5


def test_no_lpp_clusters_yields_lonely_rank_and_thin_saturation():
    """When no MM is detected (or the market has nobody), rank=lonely
    and saturation=thin."""
    pos = derive_your_position(your_size_usdc=50, clusters=[])
    assert pos.rank_in_cluster_distribution == "lonely"
    assert pos.saturation_level == "thin"
    assert pos.lpp_eligible_existing_usdc == 0.0
    assert pos.raw_share_estimate is None
