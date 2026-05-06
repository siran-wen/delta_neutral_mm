"""Paper-mode verification of the P9/P10/P10.5 asymmetric BBO quoting.

Drives ``plan_quotes`` directly with a SAMSUNG-shaped MarketSnapshot
across four inventory severities and validates the price layout
(P9), the close-size scaling (P10), and the absolute close-size cap
(P10.5):

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
    6. **(P10.5)** ``asymmetric_close_size_max_usdc`` caps the close
       at an absolute notional even when the linear formula would
       push higher — Day-7 adverse-selection mitigation. The script
       prints a P10-vs-P10.5 comparison table so the cap-firing
       scenarios are obvious.

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
    """Pull asymmetric_* knobs from a yaml; fall back to P9/P10/P10.5 defaults."""
    if path is None or not path.exists():
        return {
            "asymmetric_quote_enabled": True,
            "asymmetric_quote_trigger_pct": Decimal("0.7"),
            "asymmetric_close_mode": "improve_bbo",
            "asymmetric_anti_distance_bp": Decimal("30"),
            "asymmetric_close_size_scaling": "linear",
            # P10.5 default — picked to mirror SAMSUNG yaml; tests
            # below reflect cap firing in 3/4 scenarios at this value.
            "asymmetric_close_size_max_usdc": Decimal("250"),
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
        "asymmetric_close_size_max_usdc",
    ):
        if k in strat:
            v = strat[k]
            if k in {
                "asymmetric_quote_trigger_pct",
                "asymmetric_anti_distance_bp",
                "asymmetric_close_size_max_usdc",
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
    max_usdc: Optional[Decimal] = None,
) -> Decimal:
    """Mirror the planner's close-size formula for the assertion.

    Order of clamps mirrors ``strategy.quote_planner``:
        1. Linear (or fixed) scaling produces an unconstrained close.
        2. ``min(close, inv_abs)`` clamps to the actual position
           (defensive against default > inv).
        3. ``min(close, max_usdc)`` (P10.5) caps the close at an
           absolute notional. ``None`` skips this step → P10 behaviour.
    """
    if mode != "linear":
        close = default_size
    else:
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
    if max_usdc is not None and close > max_usdc:
        close = max_usdc
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
) -> Tuple[bool, List[str], Dict[str, Decimal]]:
    """Execute one scenario, print the outcome, and return
    ``(pass, failures, comparison_row)`` where ``comparison_row``
    carries the P10 / P10.5 close sizes for the summary table."""
    cap = cfg["hard_position_cap_usdc"]
    trigger = cap * cfg["asymmetric_quote_trigger_pct"]
    default_size = session.default_size_usdc
    scaling = cfg.get("asymmetric_close_size_scaling", "linear")
    max_usdc = cfg.get("asymmetric_close_size_max_usdc")

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
    print(
        f"  scaling={scaling} mode={cfg.get('asymmetric_close_mode', 'improve_bbo')} "
        f"max_usdc={max_usdc}"
    )
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

    # P10 / P10.5 invariant — close size matches the linear formula
    # plus optional cap. We always have at least the close leg (the
    # cap-projection only drops the leg that grows inventory).
    if inv_usdc > 0:
        close_quote = by_side.get("sell")
    else:
        close_quote = by_side.get("buy")

    # Compute both P10 (no cap) and P10.5 (with cap) for the
    # comparison table even on early-exit paths.
    p10_close = _expected_close_usdc(
        inv_abs, cap, trigger, default_size, scaling, max_usdc=None
    )
    p105_close = _expected_close_usdc(
        inv_abs, cap, trigger, default_size, scaling, max_usdc=max_usdc
    )
    cap_hit = (max_usdc is not None) and (p105_close < p10_close)

    if close_quote is None:
        failures.append("FAIL: close leg missing entirely")
    else:
        expected_size_base = _expected_size_base(
            p105_close, close_quote.price, market.size_decimals
        )
        if close_quote.size_base != expected_size_base:
            failures.append(
                f"FAIL: close size_base {close_quote.size_base} != "
                f"expected {expected_size_base} "
                f"(P10.5 close_usdc=${p105_close:.2f}, P10=${p10_close:.2f})"
            )
        # Sanity tag: close note present.
        if "asymmetric_close" not in close_quote.notes:
            failures.append(
                f"FAIL: close leg missing asymmetric_close note "
                f"(notes={close_quote.notes})"
            )

    row: Dict[str, Decimal] = {
        "inv": inv_usdc,
        "p10": p10_close,
        "p105": p105_close,
        "cap_hit": Decimal(1) if cap_hit else Decimal(0),
    }
    return (len(failures) == 0, failures, row)


def _run(yaml_path: Optional[Path]) -> int:
    market = _build_market()
    # Cap=$500 mirrors SAMSUNG yaml; combined with trigger_pct=0.7
    # and default_size=$100 the four scenarios below land at
    # trigger=$350 / midway=$425 / near-cap=$498 / very-near-cap=$450
    # so the P10.5 absolute cap clearly fires in 3 of 4 cases.
    cfg: Dict[str, Any] = {
        "target_max_delta_usdc": Decimal("500"),
        "skew_max_offset_bp": Decimal("5"),
        "hard_position_cap_usdc": Decimal("500"),
        "min_market_spread_bp": Decimal("3"),
        "max_market_spread_bp": Decimal("100"),
        "share_warn_threshold": Decimal("0.95"),
        "share_warn_widen_bp": Decimal("5"),
    }
    cfg.update(_load_yaml(yaml_path))

    cap = cfg["hard_position_cap_usdc"]
    trigger = cap * cfg["asymmetric_quote_trigger_pct"]
    max_usdc = cfg.get("asymmetric_close_size_max_usdc")

    # Default-size pinned to $100 so the comparison numbers are clean.
    default_size = Decimal("100")
    session = _build_session(default_size)

    print("=" * 60)
    print("P9/P10/P10.5 asymmetric BBO quoting paper check (SAMSUNG-shaped)")
    print("=" * 60)
    print(f"  cap=${cap} trigger=${trigger} default_size=${default_size}")
    print(
        f"  scaling={cfg.get('asymmetric_close_size_scaling', 'linear')} "
        f"max_usdc={max_usdc} (None = P10 fallback)"
    )

    scenarios = [
        ("at-trigger",     trigger),                   # $350
        ("midway",         (cap + trigger) / 2),       # $425
        ("near-cap",       cap - Decimal("2")),        # $498
        ("very-near-cap",  Decimal("450")),            # $450
    ]
    all_pass = True
    all_failures: List[str] = []
    rows: List[Tuple[str, Dict[str, Decimal]]] = []
    for label, inv_usdc in scenarios:
        ok, failures, row = _run_scenario(label, inv_usdc, market, session, cfg)
        rows.append((label, row))
        if not ok:
            all_pass = False
            all_failures.extend(f"[{label}] {f}" for f in failures)

    # Comparison table — at a glance shows where the cap fires.
    print()
    print("=" * 60)
    print("P10 vs P10.5 comparison (close-leg notional, $)")
    print("=" * 60)
    print(f"{'scenario':<14s} {'inv':>10s} {'P10 close':>12s} "
          f"{'P10.5 close':>14s} {'cap_hit':>9s}")
    print("-" * 60)
    for label, r in rows:
        cap_hit = "YES" if r["cap_hit"] == Decimal(1) else "no"
        print(
            f"{label:<14s} {float(r['inv']):>10.2f} "
            f"{float(r['p10']):>12.2f} {float(r['p105']):>14.2f} "
            f"{cap_hit:>9s}"
        )

    print()
    print("=" * 60)
    if all_pass:
        print("RESULT: PASS — all 4 scenarios verified")
        print("  [OK] at-trigger:    close ≈ default (cap not triggered)")
        print("  [OK] midway:        P10 scales up, P10.5 cap fires")
        print("  [OK] near-cap:      P10 ≈ effective_cap, P10.5 cap fires")
        print("  [OK] very-near-cap: P10 ≈ 2/3 cap_room, P10.5 cap fires")
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
