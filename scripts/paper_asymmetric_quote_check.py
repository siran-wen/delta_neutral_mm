"""Paper-mode verification of the P9 (5-4) asymmetric BBO quoting fix.

Drives ``plan_quotes`` directly with a SAMSUNG-shaped MarketSnapshot
and a long inventory above ``asymmetric_quote_trigger_pct`` of the
configured cap. Validates the four invariants the P9 fix requires:

    1. **Both legs emit** — no single-sided quoting (the regression
       P7 introduced when it cancelled the anti leg during close).
    2. **Close leg priced at BBO + 1 tick** (improve_bbo mode) — the
       sell-close becomes the new top-of-book ask.
    3. **Anti leg priced at mid - asymmetric_anti_distance_bp** —
       wider than the symmetric default to dampen anti-side fills.
    4. **Both legs are post_only** (caller would submit them with
       ``time_in_force=post_only``); plan_quotes itself only emits
       ``Quote`` records, but the price layout proves the intent.

Run::

    python scripts/paper_asymmetric_quote_check.py
    python scripts/paper_asymmetric_quote_check.py --yaml config/lighter_strategy_samsung.yaml

Exit code: 0 on full pass, non-zero on any failed invariant.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

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
    """Pull asymmetric_* knobs from a yaml; fall back to P9 defaults."""
    if path is None or not path.exists():
        return {
            "asymmetric_quote_enabled": True,
            "asymmetric_quote_trigger_pct": Decimal("0.7"),
            "asymmetric_close_mode": "improve_bbo",
            "asymmetric_anti_distance_bp": Decimal("30"),
        }
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    strat = raw.get("strategy") or {}
    out: Dict[str, Any] = {}
    for k in (
        "asymmetric_quote_enabled",
        "asymmetric_quote_trigger_pct",
        "asymmetric_close_mode",
        "asymmetric_anti_distance_bp",
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


def _build_session() -> SessionPolicy:
    return SessionPolicy(
        name="ASYM_PAPER_TEST",
        action="quote",
        default_distance_bp=Decimal("10"),
        default_size_usdc=Decimal("50"),
        tier_thresholds_bp=(Decimal("15"), Decimal("25"), Decimal("50")),
        reason="paper_test",
    )


def _run(yaml_path: Optional[Path]) -> int:
    market = _build_market()
    session = _build_session()
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
    trigger_threshold = cap * cfg["asymmetric_quote_trigger_pct"]
    inv_usdc = cap * Decimal("0.85")  # inv at 85% of cap = above trigger

    inventory = InventoryState(
        net_delta_base=inv_usdc / market.mid,
        net_delta_usdc=inv_usdc,
        avg_entry_price=market.mid,
        open_orders_count=0,
    )

    quotes = plan_quotes(market, session, inventory, cfg)

    failures = []
    by_side = {q.side: q for q in quotes}

    print()
    print("=" * 60)
    print("Scenario (P9 asymmetric BBO quoting):")
    print(
        f"  cap=${cap} trigger_pct={cfg['asymmetric_quote_trigger_pct']} "
        f"-> trigger=${trigger_threshold}"
    )
    print(f"  inv=${inv_usdc} (over trigger)")
    print(f"  market mid={market.mid} BBO=[{market.best_bid}, {market.best_ask}]")
    print(f"  close_mode={cfg['asymmetric_close_mode']}")
    print(f"  anti_distance_bp={cfg['asymmetric_anti_distance_bp']}")
    print()
    print("Outcome:")
    print(f"  quotes emitted = {len(quotes)}")
    for q in quotes:
        print(
            f"    {q.side:5s} price={q.price} "
            f"size_base={q.size_base} size_usdc={q.size_usdc} "
            f"distance_bp={float(q.distance_from_mid_bp):.2f} "
            f"notes={q.notes}"
        )

    # Invariant 1: both legs emit (no single-sided)
    if len(quotes) != 2:
        failures.append(
            f"FAIL: expected 2 quotes (both legs), got {len(quotes)}"
        )
    if "buy" not in by_side or "sell" not in by_side:
        failures.append(
            f"FAIL: missing leg(s); sides emitted = "
            f"{sorted(by_side.keys())}"
        )

    if "sell" in by_side and "buy" in by_side:
        sell, buy = by_side["sell"], by_side["buy"]
        # Invariant 2: close leg = best_bid + 1 tick (improve_bbo)
        tick = Decimal(10) ** -market.price_decimals
        expected_close = market.best_bid + tick
        if sell.price != expected_close:
            failures.append(
                f"FAIL: sell close price {sell.price} != "
                f"best_bid+tick {expected_close}"
            )
        if "asymmetric_close" not in sell.notes:
            failures.append(
                f"FAIL: sell leg missing asymmetric_close note "
                f"(notes={sell.notes})"
            )

        # Invariant 3: anti leg at mid - anti_distance_bp
        anti_bp = cfg["asymmetric_anti_distance_bp"]
        expected_anti = (
            market.mid * (Decimal(1) - anti_bp / Decimal(10000))
        ).quantize(tick)
        if buy.price != expected_anti:
            failures.append(
                f"FAIL: buy anti price {buy.price} != mid-{anti_bp}bp "
                f"{expected_anti}"
            )
        if "asymmetric_anti" not in buy.notes:
            failures.append(
                f"FAIL: buy leg missing asymmetric_anti note "
                f"(notes={buy.notes})"
            )

        # Invariant 4: close leg between BBO (post_only valid)
        if sell.price <= market.best_bid:
            failures.append(
                f"FAIL: sell close {sell.price} not strictly above "
                f"best_bid {market.best_bid}"
            )
        if sell.price >= market.best_ask:
            failures.append(
                f"FAIL: sell close {sell.price} crosses best_ask "
                f"{market.best_ask} (post_only would reject)"
            )

    print()
    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print(f"  {f}")
        return 1
    print("RESULT: PASS — all 4 invariants verified")
    print("  [OK] both legs emitted (no single-sided)")
    print("  [OK] close leg priced at BBO + 1 tick (improve_bbo)")
    print(
        f"  [OK] anti leg priced at mid - "
        f"{cfg['asymmetric_anti_distance_bp']}bp"
    )
    print("  [OK] close leg between BBO (post_only valid)")
    return 0


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
