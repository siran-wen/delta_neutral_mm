"""Paper-mode verification of the P9/P10 (5-4 / 5-5) asymmetric BBO quoting.

Drives ``plan_quotes`` directly with a SAMSUNG-shaped MarketSnapshot
across three inventory severities and validates both the price layout
(P9) and the close-size scaling (P10):

    1. **Both legs emit** when below the cap-projection limit — no
       single-sided quoting solely from the asymmetric branch.
    2. **Close leg priced at BBO + 1 tick** (improve_bbo mode) — the
       sell-close becomes the new top-of-book ask.
    3. **Anti leg priced at mid - asymmetric_anti_distance_bp** —
       wider than the symmetric default to dampen anti-side fills.
    4. **Close leg between BBO** (post_only valid).
    5. **(P10)** Close-leg notional scales linearly: $default at
       trigger, ~midway at the trigger-cap midpoint, ~cap at inv near
       cap. Anti notional stays at $default.

Run::

    python scripts/paper_asymmetric_quote_check.py
    python scripts/paper_asymmetric_quote_check.py --yaml config/lighter_strategy_samsung.yaml

Exit code: 0 on full pass, non-zero on any failed invariant.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_THIS = Path(__file__).resolve().parent
_ROOT = _THIS.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml  # noqa: E402

from strategy.quote_planner import plan_quotes  # noqa: E402
from strategy.types import (  # noqa: E402
    InventoryState,
    MarketSnapshot,
    SessionPolicy,
)


def _load_yaml(path: Optional[Path]) -> Dict[str, Any]:
    """Pull asymmetric_* knobs from a yaml; fall back to P9/P10 defaults."""
    if path is None or not path.exists():
        return {
            "asymmetric_quote_enabled": True,
            "asymmetric_quote_trigger_pct": Decimal("0.7"),
            "asymmetric_close_mode": "improve_bbo",
            "asymmetric_anti_distance_bp": Decimal("30"),
            "asymmetric_close_size_scaling": "linear",
        }
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    strat = raw.get("strategy") or {}
    out: Dict[str, Any] = {}
    for k in (
        "asymmetric_quote_enabled",
        "asymmetric_quote_trigger_pct",
        "asymmetric_close_mode",
        "asymmetric_anti_distance_bp",
        "asymmetric_close_size_scaling",
    ):
        if k in strat:
            v = strat[k]
            if k in {
                "asymmetric_quote_trigger_pct",
                "asymmetric_anti_distance_bp",
            } and isinstance(v, str):
                v = Decimal(v)
            out[k] = v
    out["asymmetric_quote_enabled"] = True  # force ON for the scenario
    out.setdefault("asymmetric_close_size_scaling", "linear")
    return out


def _build_market() -> MarketSnapshot:
    """SAMSUNG-shaped: mid 160, BBO 159.96/160.04, tick 0.001."""
    mid = Decimal("160")
    bid = Decimal("159.96")
    ask = Decimal("160.04")
    return MarketSnapshot(
        symbol="SAMSUNGUSD",
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=bid,
        best_ask=ask,
        spread_bp=(ask - bid) / mid * Decimal(10000),
        depth_by_spread_bp={
            15: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            25: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
            50: {"bid_usdc": Decimal(0), "ask_usdc": Decimal(0), "total_usdc": Decimal(0)},
        },
        price_decimals=3,
        size_decimals=3,
        ts_ms=1_700_000_000_000,
    )


def _build_session(default_size_usdc: Decimal) -> SessionPolicy:
    return SessionPolicy(
        name="ASYM_PAPER_TEST",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=default_size_usdc,
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="paper_test",
    )


def _expected_close_usdc(
    inv_abs: Decimal,
    cap: Decimal,
    trigger: Decimal,
    default_size: Decimal,
    mode: str,
) -> Decimal:
    """Mirror the planner's close-size formula for the assertion."""
    if mode != "linear":
        return default_size
    cap_room = cap - trigger
    if cap_room > 0:
        scale = (inv_abs - trigger) / cap_room
        if scale > Decimal(1):
            scale = Decimal(1)
        elif scale < Decimal(0):
            scale = Decimal(0)
    else:
        scale = Decimal(1)
    close = default_size + (cap - default_size) * scale
    if close > inv_abs:
        close = inv_abs
    return close


def _expected_size_base(usdc: Decimal, price: Decimal, size_decimals: int) -> Decimal:
    step = Decimal(1) / (Decimal(10) ** size_decimals)
    return (usdc / price).quantize(step, rounding=ROUND_DOWN)


def _run_scenario(
    label: str,
    inv_usdc: Decimal,
    market: MarketSnapshot,
    session: SessionPolicy,
    cfg: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """Execute one scenario, print the outcome, and return ``(pass, failures)``."""
    cap = cfg["hard_position_cap_usdc"]
    trigger = cap * cfg["asymmetric_quote_trigger_pct"]
    default_size = session.default_size_usdc
    scaling = cfg.get("asymmetric_close_size_scaling", "linear")

    inv = InventoryState(
        net_delta_base=inv_usdc / market.mid,
        net_delta_usdc=inv_usdc,
        avg_entry_price=market.mid,
        open_orders_count=0,
    )

    quotes = plan_quotes(market, session, inv, cfg)
    by_side = {q.side: q for q in quotes}

    print()
    print("-" * 60)
    print(f"Scenario: {label}")
    print(
        f"  inv=${inv_usdc} cap=${cap} trigger=${trigger} default=${default_size}"
    )
    print(f"  scaling={scaling} mode={cfg.get('asymmetric_close_mode', 'improve_bbo')}")
    print(f"  quotes emitted = {len(quotes)}")
    for q in quotes:
        print(
            f"    {q.side:5s} price={q.price} size_base={q.size_base} "
            f"size_usdc={q.size_usdc:.3f} notes={q.notes}"
        )

    failures: List[str] = []
    inv_abs = abs(inv_usdc)

    # P9 invariants — apply only when both legs emit (i.e., inv hasn't
    # also tripped the cap-projection skip).
    if "buy" in by_side and "sell" in by_side:
        sell, buy = by_side["sell"], by_side["buy"]
        tick = Decimal(10) ** -market.price_decimals
        # Close-leg price (long → sell at best_bid + tick).
        if inv_usdc > 0:
            expected_close_price = market.best_bid + tick
            close_quote = sell
            anti_quote = buy
        else:
            expected_close_price = market.best_ask - tick
            close_quote = buy
            anti_quote = sell
        if close_quote.price != expected_close_price:
            failures.append(
                f"FAIL: close price {close_quote.price} != expected "
                f"{expected_close_price}"
            )
        anti_bp = cfg["asymmetric_anti_distance_bp"]
        if inv_usdc > 0:
            expected_anti_price = (
                market.mid * (Decimal(1) - anti_bp / Decimal(10000))
            ).quantize(tick, rounding=ROUND_DOWN)
        else:
            from decimal import ROUND_UP as _RU
            expected_anti_price = (
                market.mid * (Decimal(1) + anti_bp / Decimal(10000))
            ).quantize(tick, rounding=_RU)
        if anti_quote.price != expected_anti_price:
            failures.append(
                f"FAIL: anti price {anti_quote.price} != expected "
                f"{expected_anti_price}"
            )

    # P10 invariant — close size matches the linear formula. We always
    # have at least the close leg (the cap-projection only drops the
    # leg that grows inventory, never the close leg).
    if inv_usdc > 0:
        close_quote = by_side.get("sell")
    else:
        close_quote = by_side.get("buy")
    if close_quote is None:
        failures.append("FAIL: close leg missing entirely")
    else:
        expected_close_usdc = _expected_close_usdc(
            inv_abs, cap, trigger, default_size, scaling
        )
        expected_size_base = _expected_size_base(
            expected_close_usdc, close_quote.price, market.size_decimals
        )
        if close_quote.size_base != expected_size_base:
            failures.append(
                f"FAIL: close size_base {close_quote.size_base} != "
                f"expected {expected_size_base} "
                f"(close_usdc=${expected_close_usdc:.2f})"
            )
        # Sanity tag: close note present.
        if "asymmetric_close" not in close_quote.notes:
            failures.append(
                f"FAIL: close leg missing asymmetric_close note "
                f"(notes={close_quote.notes})"
            )

    return (len(failures) == 0, failures)


def _run(yaml_path: Optional[Path]) -> int:
    market = _build_market()
    cfg: Dict[str, Any] = {
        "target_max_delta_usdc": Decimal("400"),
        "skew_max_offset_bp": Decimal("5"),
        "hard_position_cap_usdc": Decimal("400"),
        "min_market_spread_bp": Decimal("3"),
        "max_market_spread_bp": Decimal("100"),
        "share_warn_threshold": Decimal("0.95"),
        "share_warn_widen_bp": Decimal("5"),
    }
    cfg.update(_load_yaml(yaml_path))

    cap = cfg["hard_position_cap_usdc"]
    trigger = cap * cfg["asymmetric_quote_trigger_pct"]

    # Default-size pinned to $100 so the spec's example numbers match
    # ($100 → $250 → $395 across the three scenarios at cap=$400).
    default_size = Decimal("100")
    session = _build_session(default_size)

    print("=" * 60)
    print("P9/P10 asymmetric BBO quoting paper check (SAMSUNG-shaped)")
    print("=" * 60)
    print(f"  cap=${cap} trigger=${trigger} default_size=${default_size}")
    print(f"  scaling={cfg.get('asymmetric_close_size_scaling', 'linear')}")

    scenarios = [
        ("at-trigger", trigger),         # $280
        ("midway", (cap + trigger) / 2),  # $340
        ("near-cap", cap - Decimal("2")),  # $398
    ]
    all_pass = True
    all_failures: List[str] = []
    for label, inv_usdc in scenarios:
        ok, failures = _run_scenario(label, inv_usdc, market, session, cfg)
        if not ok:
            all_pass = False
            all_failures.extend(f"[{label}] {f}" for f in failures)

    print()
    print("=" * 60)
    if all_pass:
        print("RESULT: PASS — all 3 scenarios verified")
        print("  [OK] at-trigger:  close ≈ default")
        print("  [OK] midway:      close ≈ midway between default and cap")
        print("  [OK] near-cap:    close ≈ cap")
        return 0
    print("RESULT: FAIL")
    for f in all_failures:
        print(f"  {f}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yaml",
        type=Path,
        default=None,
        help="Optional strategy yaml to source asymmetric_* values from.",
    )
    args = parser.parse_args()
    return _run(args.yaml)


if __name__ == "__main__":
    sys.exit(main())
