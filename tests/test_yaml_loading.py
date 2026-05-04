"""Pin the per-market production yaml values that drive risk + sizing.

These tests guard against silent drift in the three Phase 3 strategy
yamls. They load the yaml the same way ``run_lighter_strategy.py`` does
(``yaml.safe_load`` + ``_coerce_decimal_fields``) and assert the
fields a sloppy edit is most likely to break: target_max, hard_cap,
the per-session size, the market filter (HYUNDAI is the only one with
a non-default ``max_market_spread_bp``).

Why pin in tests: Day-1 production sizing is a Lagrangian optimum —
nudging size, cap, or pct without re-running the math will silently
shift exposure. Pinning the live values means any future change must
either update the test (forcing a conscious decision) or fail CI.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts.run_lighter_strategy import _coerce_decimal_fields  # noqa: E402


_CONFIG_DIR = Path(_ROOT) / "config"


def _load_strategy(yaml_filename: str) -> dict:
    with open(_CONFIG_DIR / yaml_filename, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _coerce_decimal_fields(raw["strategy"])


# ---- per-market pinned values ---------------------------------------


def test_skhynix_yaml_size_and_cap():
    """SKHYNIX retune (5-4): target $300, cap $600, pct 30%."""
    cfg = _load_strategy("lighter_strategy.yaml")
    assert cfg["market"] == "SKHYNIXUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("300")
    assert cfg["hard_position_cap_usdc"] == Decimal("600")
    assert cfg["hard_position_cap_pct"] == Decimal("0.30")
    assert cfg["skew_max_offset_bp"] == Decimal("5")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("100")


def test_samsung_yaml_size_and_cap():
    """SAMSUNG retune (5-5 sizes + cap bump): target $100, cap $500,
    pct 25%. Cap bumped 400 -> 500 alongside HYUNDAI in the 5-5
    retune (Day-5 collateral headroom). KR_WEEKEND bumped 100 -> 150."""
    cfg = _load_strategy("lighter_strategy_samsung.yaml")
    assert cfg["market"] == "SAMSUNGUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("100")
    assert cfg["hard_position_cap_usdc"] == Decimal("500")
    assert cfg["hard_position_cap_pct"] == Decimal("0.25")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("150")


def test_hyundai_yaml_size_and_cap():
    """HYUNDAI retune (5-5 cap bump): target $200, cap $500, pct 20%.
    Cap bumped 400 -> 500 alongside SAMSUNG in the 5-5 retune.
    KR_WEEKEND/KR_OVERNIGHT stay at $200 (deeper-pocket overnight)."""
    cfg = _load_strategy("lighter_strategy_hyundai.yaml")
    assert cfg["market"] == "HYUNDAIUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("200")
    assert cfg["hard_position_cap_usdc"] == Decimal("500")
    assert cfg["hard_position_cap_pct"] == Decimal("0.2")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("200")


def test_max_market_spread_bp_pinned_per_yaml():
    """Per-market spread guard pins. 5-4 retune: all three widened to
    500 bp after observing several KR_OVERNIGHT/KR_WEEKEND windows
    where the natural spread blew out past the prior 100/200/300
    pins and stalled the strategy entirely."""
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")
    assert skhynix["max_market_spread_bp"] == Decimal("500")
    assert samsung["max_market_spread_bp"] == Decimal("500")
    assert hyundai["max_market_spread_bp"] == Decimal("500")


# ---- cross-market invariants -----------------------------------------


def test_all_three_yamls_have_size_at_every_kr_session():
    """Every KR session in session_overrides must have a default_size_usdc.

    Missing a session would silently fall back to ``strategy/session_aware``
    hard-coded $500-$1000 defaults — exactly the bug Phase 2.1 fixed.
    """
    expected_sessions = {
        "KR_MARKET_HOURS_AM",
        "KR_MARKET_HOURS_PM",
        "KR_LUNCH_BREAK",
        "KR_AFTER_CLOSE",
        "KR_OVERNIGHT",
        "KR_BEFORE_OPEN",
        "KR_WEEKEND",
    }
    for fname in (
        "lighter_strategy.yaml",
        "lighter_strategy_samsung.yaml",
        "lighter_strategy_hyundai.yaml",
    ):
        cfg = _load_strategy(fname)
        sessions = set(cfg["session_overrides"].keys())
        assert expected_sessions.issubset(sessions), (
            f"{fname} missing sessions: {expected_sessions - sessions}"
        )
        for sess in expected_sessions:
            assert "default_size_usdc" in cfg["session_overrides"][sess], (
                f"{fname}/{sess} missing default_size_usdc"
            )


def test_total_absolute_cap_exposure_pinned_at_2k_collateral():
    """At ~$2,213 collateral, the sum of absolute caps across the three
    markets is the ceiling on net delta if every side fills against us
    simultaneously. Pin the math so nudging any cap without checking
    total exposure fails fast.

    5-5 retune: SAMSUNG cap $400 → $500 and HYUNDAI cap $400 → $500.
    Total now $1600 = 72% of collateral — sanity bound bumped to 75%
    to allow the new exposure ceiling while still failing if a future
    cap edit drives total > $1660."""
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")
    total_cap = (
        skhynix["hard_position_cap_usdc"]
        + samsung["hard_position_cap_usdc"]
        + hyundai["hard_position_cap_usdc"]
    )
    # $600 + $500 + $500 = $1600
    assert total_cap == Decimal("1600")
    assert total_cap / Decimal("2213") < Decimal("0.75")


def test_asymmetric_quote_state_pinned_per_yaml():
    """Pin the P9 asymmetric_quote_* knobs per-yaml. The three markets
    share the same trigger / mode / anti-distance because the inv
    profile (cap × 0.7 = trigger threshold) and the BBO improvement
    pattern aren't market-specific — only enabled state may diverge
    if a market needs to opt out for ad-hoc testing.

    P9 (5-4) replaced the P6/P7/P8 active_hedge IOC + post_only
    emergency paths with a plan_quotes-level override that prices
    the close leg aggressively at the BBO and the anti leg wide,
    keeping both legs maker-priced and post_only.
    """
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")

    for cfg in (skhynix, samsung, hyundai):
        assert cfg["asymmetric_quote_enabled"] is True
        assert cfg["asymmetric_quote_trigger_pct"] == Decimal("0.7")
        assert cfg["asymmetric_close_mode"] == "improve_bbo"
        assert cfg["asymmetric_anti_distance_bp"] == Decimal("30")
        # P10 (5-5): inv-aware close size scaling. All three markets
        # ship with "linear" — the P10 default. "fixed" is the P9
        # fall-back if a market needs to opt out for ad-hoc testing.
        assert cfg["asymmetric_close_size_scaling"] == "linear"

    # P9 strip: the old active_hedge_* keys must be gone — pin to
    # catch a partial revert from a config patch.
    for cfg in (skhynix, samsung, hyundai):
        for legacy_key in (
            "active_hedge_enabled",
            "active_hedge_trigger_pct",
            "active_hedge_target_pct",
            "active_hedge_taker_fee_max_pct",
            "active_hedge_pause_after_sec",
            "active_hedge_max_consecutive_fails",
            "active_hedge_post_submit_wait_sec",
            "active_hedge_pre_cancel_wait_sec",
        ):
            assert legacy_key not in cfg, (
                f"P9 strip incomplete: legacy {legacy_key} still in yaml"
            )
