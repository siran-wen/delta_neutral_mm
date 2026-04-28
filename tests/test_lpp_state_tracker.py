"""Unit tests for ``strategy.lpp_state_tracker``.

Covers:

- ``estimate_my_share`` pro-rata behaviour (existing depth, zero
  depth, cap-overflow regime, tier weight application).
- ``record_snapshot`` JSONL write + return shape.
- ``get_session_summary`` aggregation, percentiles, empty period.
- Cap lookup for the various session names exposed by
  ``strategy.session_aware``.
- ``PRE_OPEN_WITHDRAW`` (action=withdraw, no quotes) → zero reward.
- Cumulative reward accumulates monotonically.
- Unknown symbol degrades gracefully (no raise, reward=0).
- ``reconcile_actual_rewards`` stub contract.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Dict

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategy.lpp_state_tracker import (  # noqa: E402
    LppStateTracker,
    estimate_my_share,
    _CAP_TABLE,
)
from strategy.types import (  # noqa: E402
    InventoryState,
    MarketSnapshot,
    Quote,
    SessionPolicy,
)


# ---- helpers ----------------------------------------------------------


_KR_TIER_THRESHOLDS = (Decimal("15"), Decimal("25"), Decimal("50"))
_WEEKEND_TIER_THRESHOLDS = (Decimal("50"), Decimal("100"), Decimal("200"))


def _D(s) -> Decimal:
    return Decimal(str(s))


def _market(
    *,
    symbol: str = "SKHYNIXUSD",
    mid: Decimal = Decimal("100"),
    depth_by_spread_bp: Dict[int, Dict[str, Decimal]] = None,
    spread_bp: Decimal = Decimal("10"),
) -> MarketSnapshot:
    depth = depth_by_spread_bp if depth_by_spread_bp is not None else {}
    return MarketSnapshot(
        symbol=symbol,
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=mid - Decimal("0.05"),
        best_ask=mid + Decimal("0.05"),
        spread_bp=spread_bp,
        depth_by_spread_bp=depth,
        price_decimals=3,
        size_decimals=3,
        ts_ms=1_700_000_000_000,
    )


def _quote(
    *,
    side: str,
    size_usdc: Decimal,
    tier_target: str,
    distance_bp: Decimal = Decimal("10"),
) -> Quote:
    return Quote(
        side=side,
        price=Decimal("100"),
        size_base=size_usdc / Decimal("100"),
        size_usdc=size_usdc,
        distance_from_mid_bp=distance_bp,
        tier_target=tier_target,
        market_position="passive",
        notes=(),
    )


def _session(
    name: str = "KR_MARKET_HOURS_AM",
    action: str = "quote",
    tier_thresholds_bp: tuple = _KR_TIER_THRESHOLDS,
) -> SessionPolicy:
    return SessionPolicy(
        name=name,
        action=action,
        default_distance_bp=Decimal("5"),
        default_size_usdc=Decimal("1000"),
        tier_thresholds_bp=tier_thresholds_bp,
        reason=f"test session {name}",
    )


def _inventory(net_delta_usdc: Decimal = Decimal("0")) -> InventoryState:
    return InventoryState(
        net_delta_base=net_delta_usdc / Decimal("100"),
        net_delta_usdc=net_delta_usdc,
        avg_entry_price=Decimal("100"),
        open_orders_count=2,
    )


_TIER_WEIGHTS = {
    "L1": Decimal("1.0"),
    "L2": Decimal("0.6"),
    "L3": Decimal("0.3"),
    "OUT": Decimal("0.0"),
}


def _tracker(tmp_path: Path, weekly_pool=None) -> LppStateTracker:
    return LppStateTracker(
        weekly_pool_usdc=weekly_pool or {"SKHYNIXUSD": Decimal("12269")},
        tier_weights=_TIER_WEIGHTS,
        output_dir=tmp_path,
    )


# ---- 1. share with existing depth ------------------------------------


def test_estimate_share_in_l1_with_existing_depth():
    """existing $1500 + my $1000 → 40% pro-rata."""
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("1500"),
                "ask_usdc": Decimal("1500"),
                "total_usdc": Decimal("3000"),
            }
        }
    )
    q = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    out = estimate_my_share(q, market, _session(), _TIER_WEIGHTS)
    # 1000 / (1500 + 1000) = 0.4
    assert out["raw_share"] == _D("0.4")
    # weight L1 = 1.0 ⇒ weighted = raw
    assert out["weighted_share"] == _D("0.4")
    assert out["existing_in_tier_usdc"] == _D("1500")
    assert out["cap_usdc"] == _D("19000")


# ---- 2. share with zero existing depth -------------------------------


def test_estimate_share_in_l1_when_depth_zero():
    """existing $0 + my $1000 → 100% (we are the only LP at this tier)."""
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("0"),
                "ask_usdc": Decimal("0"),
                "total_usdc": Decimal("0"),
            }
        }
    )
    q = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    out = estimate_my_share(q, market, _session(), _TIER_WEIGHTS)
    assert out["raw_share"] == _D("1")
    assert out["weighted_share"] == _D("1")


# ---- 3. L2 weight -----------------------------------------------------


def test_estimate_share_in_l2_uses_l2_weight():
    """raw 0.5 at L2 → weighted 0.3 (0.5 * 0.6)."""
    market = _market(
        depth_by_spread_bp={
            25: {
                "bid_usdc": Decimal("0"),
                "ask_usdc": Decimal("1000"),
                "total_usdc": Decimal("1000"),
            }
        }
    )
    q = _quote(side="sell", size_usdc=Decimal("1000"), tier_target="L2")
    out = estimate_my_share(q, market, _session(), _TIER_WEIGHTS)
    # ask side: existing 1000, my 1000 ⇒ raw 0.5
    assert out["raw_share"] == _D("0.5")
    # weight L2 = 0.6 ⇒ weighted 0.3
    assert out["weighted_share"] == _D("0.3")
    # cap reported (AM L2)
    assert out["cap_usdc"] == _D("34000")


# ---- 4. existing > cap (still pro-rata, no explosion) ----------------


def test_share_clamped_when_existing_exceeds_cap():
    """existing $25k > L1 cap $19k; my $1k → 1k/26k pro-rata.

    Cap is reported but does NOT bound the share in this batch (per
    the module docstring — overflow handling deferred to batch 3).
    """
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("25000"),
                "ask_usdc": Decimal("25000"),
                "total_usdc": Decimal("50000"),
            }
        }
    )
    q = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    out = estimate_my_share(q, market, _session(), _TIER_WEIGHTS)
    # 1000 / (25000 + 1000) ≈ 0.03846...
    expected = Decimal("1000") / Decimal("26000")
    assert out["raw_share"] == expected
    assert out["cap_usdc"] == _D("19000")  # cap recorded, not enforced


# ---- 5. record_snapshot writes JSONL ---------------------------------


def test_record_snapshot_writes_jsonl(tmp_path):
    tracker = _tracker(tmp_path)
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("1000"),
                "ask_usdc": Decimal("1000"),
                "total_usdc": Decimal("2000"),
            }
        }
    )
    bid = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    ask = _quote(side="sell", size_usdc=Decimal("1000"), tier_target="L1")

    rec = asyncio.run(
        tracker.record_snapshot(
            ts_ms=1_700_000_000_000,
            market=market,
            my_quotes=[bid, ask],
            inventory=_inventory(),
            session=_session(),
        )
    )
    tracker.close()

    # Returned record sanity
    assert rec["symbol"] == "SKHYNIXUSD"
    assert rec["session"] == "KR_MARKET_HOURS_AM"
    assert rec["share"]["bid_tier"] == "L1"
    assert rec["share"]["ask_tier"] == "L1"

    # File exists with exactly one line, parseable JSON.
    files = list(tmp_path.glob("SKHYNIXUSD_*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    # JSONL coerces Decimals to strings via _to_jsonable
    assert parsed["symbol"] == "SKHYNIXUSD"
    assert parsed["share"]["bid_tier"] == "L1"
    # depth_at_my_tier must reference the L1 cap.
    assert parsed["depth_at_my_tier"]["L1"]["cap_usdc"] == "19000"


# ---- 6. session summary aggregation ----------------------------------


def test_get_session_summary_aggregates_correctly(tmp_path):
    """60 snapshots over 1 hour, avg share 30% ⇒ reward ≈ pool * (3600/604800) * 0.3."""
    tracker = _tracker(tmp_path)
    base_ts = 1_700_000_000_000
    market = _market(
        # tuned so bid pro-rata = 0.30: existing 7000/3 ≈ 2333.33, my 1000
        # → 1000/(2333.33+1000) ≈ 0.30
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("1000") * Decimal("7") / Decimal("3"),
                "ask_usdc": Decimal("1000") * Decimal("7") / Decimal("3"),
                "total_usdc": Decimal("0"),
            }
        }
    )
    bid = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    ask = _quote(side="sell", size_usdc=Decimal("1000"), tier_target="L1")

    for i in range(60):
        asyncio.run(
            tracker.record_snapshot(
                ts_ms=base_ts + i * 60_000,
                market=market,
                my_quotes=[bid, ask],
                inventory=_inventory(),
                session=_session(),
            )
        )
    tracker.close()

    summary = tracker.get_session_summary(
        session_start_ms=base_ts,
        session_end_ms=base_ts + 59 * 60_000,
    )
    assert summary["snapshots_count"] == 60
    # Per-snapshot reward = 12269 * (60/604800) * 0.30
    # Hour total = 60 * that = 12269 * (3600/604800) * 0.30 ≈ 21.9089
    expected = (
        Decimal("12269")
        * (Decimal("3600") / Decimal(7 * 24 * 3600))
        * Decimal("0.30")
    )
    # Allow tolerance for the 7/3 rounding in pro-rata
    delta = abs(summary["estimated_reward_usdc"] - expected)
    assert delta < Decimal("0.01"), (
        f"reward mismatch: got {summary['estimated_reward_usdc']}, "
        f"expected {expected}, delta {delta}"
    )
    # share_p50 ≈ 0.30 (within rounding)
    assert summary["share_p50"] is not None
    assert abs(summary["share_p50"] - Decimal("0.30")) < Decimal("0.001")
    # All snapshots quoted L1 on both sides ⇒ 100% L1 distribution
    assert summary["tier_distribution"] == {"L1": Decimal("1")}


# ---- 7. empty period returns zeros, no raise -------------------------


def test_get_session_summary_empty_period(tmp_path):
    tracker = _tracker(tmp_path)
    out = tracker.get_session_summary(
        session_start_ms=1_700_000_000_000,
        session_end_ms=1_700_000_001_000,
    )
    assert out["snapshots_count"] == 0
    assert out["estimated_reward_usdc"] == Decimal(0)
    assert out["share_p50"] is None
    assert out["share_p95"] is None
    assert out["tier_distribution"] == {}


# ---- 8. lunch break uses lunch caps ----------------------------------


def test_lunch_break_uses_correct_cap():
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("0"),
                "ask_usdc": Decimal("0"),
                "total_usdc": Decimal("0"),
            }
        }
    )
    q = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    out = estimate_my_share(
        q, market, _session(name="KR_LUNCH_BREAK"), _TIER_WEIGHTS
    )
    # Lunch L1 cap (verbatim from spec) is $19,000
    assert out["cap_usdc"] == _D("19000")


# ---- 9. weekend caps (or lack thereof) -------------------------------


def test_weekend_uses_weekend_caps():
    """Weekend caps come from the dedicated KR_WEEKEND row of the
    LPP spreadsheet ($19k / $43k / $94k for L1/L2/L3). The bp
    boundaries are wider (50/100/200 bp) but the L1/L2/L3 *labels*
    still apply.
    """
    market = _market(
        depth_by_spread_bp={
            50: {  # weekend L1 boundary = 50bp
                "bid_usdc": Decimal("500"),
                "ask_usdc": Decimal("500"),
                "total_usdc": Decimal("1000"),
            }
        }
    )
    weekend_session = _session(
        name="KR_WEEKEND", tier_thresholds_bp=_WEEKEND_TIER_THRESHOLDS
    )
    q = _quote(side="buy", size_usdc=Decimal("500"), tier_target="L1")
    out = estimate_my_share(q, market, weekend_session, _TIER_WEIGHTS)
    # share computed normally (pro-rata, cap doesn't bound)
    assert out["raw_share"] == _D("0.5")
    # cap is the weekend L1 entry ($19k)
    assert out["cap_usdc"] == _D("19000")

    # And L2/L3 are populated too — verify via _CAP_TABLE so future
    # mirroring mistakes can't silently zero the weekend rows.
    assert _CAP_TABLE[("KR_WEEKEND", "L2")] == _D("43000")
    assert _CAP_TABLE[("KR_WEEKEND", "L3")] == _D("94000")


def test_after_close_l1_cap_is_18k_not_19k():
    """Regression guard: KR_AFTER_CLOSE L1 cap is $18,000 in the
    LPP spreadsheet — a deliberate $1k step down from Market Hours
    L1 ($19,000). Earlier mirroring incorrectly used the OVERNIGHT
    cap ($5.3k); this test pins the spreadsheet's actual value.
    """
    assert _CAP_TABLE[("KR_AFTER_CLOSE", "L1")] == _D("18000")
    assert _CAP_TABLE[("KR_AFTER_CLOSE", "L1")] != _CAP_TABLE[
        ("KR_MARKET_HOURS_AM", "L1")
    ]
    # L2/L3 do match Market Hours per the spreadsheet
    assert _CAP_TABLE[("KR_AFTER_CLOSE", "L2")] == _D("34000")
    assert _CAP_TABLE[("KR_AFTER_CLOSE", "L3")] == _D("78000")


def test_before_open_caps_smaller_than_market_hours():
    """Regression guard: KR_BEFORE_OPEN caps ($7.5k/$20k/$51k) are
    strictly smaller than Market Hours caps ($19k/$34k/$78k). An
    incorrect mirror from any other session would break this
    invariant.
    """
    bo_l1 = _CAP_TABLE[("KR_BEFORE_OPEN", "L1")]
    bo_l2 = _CAP_TABLE[("KR_BEFORE_OPEN", "L2")]
    bo_l3 = _CAP_TABLE[("KR_BEFORE_OPEN", "L3")]
    mh_l1 = _CAP_TABLE[("KR_MARKET_HOURS_AM", "L1")]
    mh_l2 = _CAP_TABLE[("KR_MARKET_HOURS_AM", "L2")]
    mh_l3 = _CAP_TABLE[("KR_MARKET_HOURS_AM", "L3")]

    assert bo_l1 == _D("7500")
    assert bo_l2 == _D("20000")
    assert bo_l3 == _D("51000")
    assert bo_l1 < mh_l1, f"BEFORE_OPEN L1 {bo_l1} should be < MH L1 {mh_l1}"
    assert bo_l2 < mh_l2
    assert bo_l3 < mh_l3


# ---- 10. PRE_OPEN_WITHDRAW: no quotes, zero reward -------------------


def test_pre_open_withdraw_session_no_reward(tmp_path):
    tracker = _tracker(tmp_path)
    market = _market()
    rec = asyncio.run(
        tracker.record_snapshot(
            ts_ms=1_700_000_000_000,
            market=market,
            my_quotes=[],  # withdraw session ⇒ no live quotes
            inventory=_inventory(),
            session=_session(name="KR_PRE_OPEN_WITHDRAW", action="withdraw"),
        )
    )
    tracker.close()
    assert rec["estimated_reward_60s_usdc"] == Decimal(0)
    assert rec["cumulative_estimated_reward_usdc"] == Decimal(0)
    assert rec["share"]["avg_weighted_share"] == Decimal(0)
    assert rec["share"]["bid_tier"] is None
    assert rec["share"]["ask_tier"] is None


# ---- 11. cumulative reward accumulates -------------------------------


def test_cumulative_reward_accumulates(tmp_path):
    tracker = _tracker(tmp_path)
    market = _market(
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("0"),
                "ask_usdc": Decimal("0"),
                "total_usdc": Decimal("0"),
            }
        }
    )
    bid = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    ask = _quote(side="sell", size_usdc=Decimal("1000"), tier_target="L1")

    cumulatives = []
    for i in range(3):
        rec = asyncio.run(
            tracker.record_snapshot(
                ts_ms=1_700_000_000_000 + i * 60_000,
                market=market,
                my_quotes=[bid, ask],
                inventory=_inventory(),
                session=_session(),
            )
        )
        cumulatives.append(rec["cumulative_estimated_reward_usdc"])
    tracker.close()

    # Strictly monotonic: each cum > previous
    assert cumulatives[1] > cumulatives[0]
    assert cumulatives[2] > cumulatives[1]
    # And the third equals 3× the per-snapshot reward (share=1.0 both
    # sides ⇒ avg=1.0, per-snap = 12269 * 60 / 604800).
    per_snap = Decimal("12269") * Decimal("60") / Decimal(7 * 24 * 3600)
    assert abs(cumulatives[2] - per_snap * 3) < Decimal("0.0001")


# ---- 12. unknown symbol → warn, not raise ----------------------------


def test_unknown_symbol_logs_warn_not_raise(tmp_path, caplog):
    """A symbol absent from ``weekly_pool_usdc`` does NOT raise; the
    snapshot is still written with ``estimated_reward=0`` and a
    warning is emitted so the operator notices."""
    import logging as _logging

    tracker = _tracker(tmp_path)
    market = _market(
        symbol="UNKNOWNUSD",
        depth_by_spread_bp={
            15: {
                "bid_usdc": Decimal("0"),
                "ask_usdc": Decimal("0"),
                "total_usdc": Decimal("0"),
            }
        },
    )
    bid = _quote(side="buy", size_usdc=Decimal("1000"), tier_target="L1")
    ask = _quote(side="sell", size_usdc=Decimal("1000"), tier_target="L1")
    with caplog.at_level(_logging.WARNING, logger="strategy.lpp_state_tracker"):
        rec = asyncio.run(
            tracker.record_snapshot(
                ts_ms=1_700_000_000_000,
                market=market,
                my_quotes=[bid, ask],
                inventory=_inventory(),
                session=_session(),
            )
        )
    tracker.close()
    assert rec["estimated_reward_60s_usdc"] == Decimal(0)
    # share fields still computed for the audit trail
    assert rec["share"]["avg_weighted_share"] == Decimal(1)
    # Warning surfaced
    assert any("UNKNOWNUSD" in r.message for r in caplog.records)


# ---- 13. reconcile_actual_rewards stub contract ----------------------


def test_reconcile_actual_returns_stub(tmp_path):
    tracker = _tracker(tmp_path)

    class _DummyGateway:
        pass

    out = asyncio.run(
        tracker.reconcile_actual_rewards(
            gateway=_DummyGateway(),
            since_ts_ms=1_700_000_000_000,
            symbol="SKHYNIXUSD",
        )
    )
    tracker.close()
    assert out["stub"] is True
    assert out["actual_usdc"] is None
    assert out["gap_pct"] is None
    assert out["estimated_usdc"] == Decimal(0)  # nothing recorded yet
    assert out["symbol"] == "SKHYNIXUSD"


# ---- bonus: cap-table coverage sanity --------------------------------


def test_cap_table_covers_all_session_aware_names():
    """Every session name produced by ``session_aware._DEFAULTS``
    has a row (per tier) in ``_CAP_TABLE`` — even if the value is
    None (KR_WEEKEND placeholder)."""
    from strategy.session_aware import _DEFAULTS

    for session_name in _DEFAULTS:
        for tier in ("L1", "L2", "L3"):
            assert (session_name, tier) in _CAP_TABLE, (
                f"missing cap entry for ({session_name!r}, {tier!r})"
            )
