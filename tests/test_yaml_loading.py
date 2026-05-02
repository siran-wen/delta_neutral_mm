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
    """SKHYNIX size $100, cap $200, pct 10%, KR_WEEKEND $100."""
    cfg = _load_strategy("lighter_strategy.yaml")
    assert cfg["market"] == "SKHYNIXUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("100")
    assert cfg["hard_position_cap_usdc"] == Decimal("200")
    assert cfg["hard_position_cap_pct"] == Decimal("0.10")
    assert cfg["skew_max_offset_bp"] == Decimal("5")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("100")


def test_samsung_yaml_size_and_cap():
    """SAMSUNG: $100 per side, cap $200, pct 10%, hedge enabled."""
    cfg = _load_strategy("lighter_strategy_samsung.yaml")
    assert cfg["market"] == "SAMSUNGUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("100")
    assert cfg["hard_position_cap_usdc"] == Decimal("200")
    assert cfg["hard_position_cap_pct"] == Decimal("0.1")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("100")


def test_hyundai_yaml_size_and_cap():
    """HYUNDAI thin-depth max-size: $150 per side, cap $300, pct 15%."""
    cfg = _load_strategy("lighter_strategy_hyundai.yaml")
    assert cfg["market"] == "HYUNDAIUSD"
    assert cfg["target_max_delta_usdc"] == Decimal("150")
    assert cfg["hard_position_cap_usdc"] == Decimal("300")
    assert cfg["hard_position_cap_pct"] == Decimal("0.15")
    weekend = cfg["session_overrides"]["KR_WEEKEND"]
    assert weekend["default_size_usdc"] == Decimal("150")


def test_max_market_spread_bp_pinned_per_yaml():
    """Per-market spread guard pins. HYUNDAI runs widest (weekend
    spreads regularly exceed 200bp), SAMSUNG widened post-Day-1 to
    cover its volatile windows, SKHYNIX keeps the tightest guard."""
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")
    assert skhynix["max_market_spread_bp"] == Decimal("100")
    assert samsung["max_market_spread_bp"] == Decimal("200")
    assert hyundai["max_market_spread_bp"] == Decimal("300")


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
    total exposure fails fast."""
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")
    total_cap = (
        skhynix["hard_position_cap_usdc"]
        + samsung["hard_position_cap_usdc"]
        + hyundai["hard_position_cap_usdc"]
    )
    # $200 + $200 + $300 = $700
    assert total_cap == Decimal("700")
    # 31.6% of the $2,213 baseline. Sanity bound at 35%.
    assert total_cap / Decimal("2213") < Decimal("0.35")


def test_active_hedge_state_pinned_per_yaml():
    """Pin the active_hedge_enabled flag per yaml so toggles can't slip
    in unnoticed. Hedge re-enable is an explicit decision (gated on
    the P2.2 IOC fix verifying live), so the test asserts the live
    state per market rather than a uniform default."""
    skhynix = _load_strategy("lighter_strategy.yaml")
    samsung = _load_strategy("lighter_strategy_samsung.yaml")
    hyundai = _load_strategy("lighter_strategy_hyundai.yaml")
    assert skhynix["active_hedge_enabled"] is False
    assert samsung["active_hedge_enabled"] is True
    assert hyundai["active_hedge_enabled"] is False
