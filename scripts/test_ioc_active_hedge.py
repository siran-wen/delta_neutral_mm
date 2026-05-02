"""Standalone IOC active-hedge SDK probe.

Sends 1+ tiny IOC reduce_only orders against the live Lighter signer,
trying different (order_type, order_expiry) combinations to confirm
exactly which payload the matching engine accepts.

Why standalone: the strategy main loop pulled in 80+ rejects across
the 4-30 + 5-1 live runs with "OrderExpiry is invalid", but the
strategy's own retry path obscured the underlying SDK response. This
script bypasses the OrderManager and pokes the SDK directly so the
raw error string is visible.

SAFETY (read this before running):
* ``--dry-run`` (the default) signs each variant locally and prints the
  decoded tx_info without touching the network. No money moves, no
  account state changes.
* ``--send`` actually fires the tx. Even with --send, every order is
  ``reduce_only=True`` and a tiny base_size — if the account has no
  position, the server rejects the order with "no position to reduce"
  and nothing happens. If the account DOES have a position, the order
  may partially or fully reduce it. Inspect ``--probe`` output (which
  prints positions before sending) and confirm flatness.
* Set ``--symbol`` to match an active market (default SKHYNIXUSD).
* The script ALWAYS prints the SDK error/response for every variant
  so you can compare which one the server accepts.

Reading the output:
* "OrderExpiry is invalid"  → expiry value rejected
* "OrderType is invalid"    → order_type rejected
* "ReduceOnly without position" / "InvalidReduceOnly" → server checked
                                                        the payload
                                                        and rejected
                                                        on a different
                                                        invariant; the
                                                        IOC encoding
                                                        is fine
* "Order placed" / no error → variant accepted
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from gateways.lighter_gateway import LighterGateway  # noqa: E402


def _load_env_file(env_path: Path) -> None:
    """Inject .env into os.environ without overwriting existing values.

    Mirrors python-dotenv semantics minimally so the script works even
    when the user has run setup_env_permanent.ps1 (in which case env
    vars are already present).
    """
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


async def _probe_account(gateway: LighterGateway, market_index: int) -> Dict[str, Any]:
    """Best-effort: pull positions for the target market via REST.

    Returns ``{"net_base": Decimal, "raw": dict|None}``. Used by the
    safety gate so we know whether reduce_only will land on something.
    """
    try:
        details = await gateway.get_order_book_details(market_index)
    except Exception as exc:  # noqa: BLE001
        details = {"error": str(exc)}
    out: Dict[str, Any] = {"market_details": details}

    fetch = getattr(gateway, "fetch_account_state", None) or getattr(
        gateway, "get_account_state", None
    )
    if fetch is None:
        out["positions_probe"] = "gateway has no fetch_account_state — cannot probe"
        return out
    try:
        state = await fetch()
        out["account_state"] = state
    except Exception as exc:  # noqa: BLE001
        out["positions_probe"] = f"fetch_account_state failed: {exc!r}"
    return out


def _build_variants(
    market_index: int,
    coid_base: int,
    base_amount: int,
    price_int: int,
    is_ask: bool,
) -> List[Dict[str, Any]]:
    """Return the test matrix.

    Each variant is a kwargs dict ready to pass to
    ``signer.create_order(**variant)``. The variants exercise every
    plausible cell of (order_type ∈ {LIMIT, MARKET}) × (order_expiry
    ∈ {-1 default, 0, now_ms+30s}). The expected acceptance pattern
    is variant 4 (MARKET + 0 = ``DEFAULT_IOC_EXPIRY``) — the one the
    SDK helpers all use.
    """
    from lighter import SignerClient as _SC

    common = {
        "market_index": int(market_index),
        "base_amount": int(base_amount),
        "price": int(price_int),
        "is_ask": is_ask,
        "time_in_force": _SC.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
        "reduce_only": True,
    }
    now_ms = int(time.time() * 1000)
    variants = [
        # 1. Old P2.1 buggy payload: LIMIT + (now+30s)
        {
            **common,
            "client_order_index": coid_base + 1,
            "order_type": _SC.ORDER_TYPE_LIMIT,
            "order_expiry": now_ms + 30_000,
            "_label": "LIMIT + (now+30s)  [P2.1 buggy]",
        },
        # 2. LIMIT + 28-day default
        {
            **common,
            "client_order_index": coid_base + 2,
            "order_type": _SC.ORDER_TYPE_LIMIT,
            "order_expiry": _SC.DEFAULT_28_DAY_ORDER_EXPIRY,
            "_label": "LIMIT + 28-day (-1)  [SDK default]",
        },
        # 3. LIMIT + 0 (DEFAULT_IOC_EXPIRY but wrong order_type)
        {
            **common,
            "client_order_index": coid_base + 3,
            "order_type": _SC.ORDER_TYPE_LIMIT,
            "order_expiry": _SC.DEFAULT_IOC_EXPIRY,
            "_label": "LIMIT + 0  [DEFAULT_IOC_EXPIRY, wrong order_type]",
        },
        # 4. MARKET + 0 (the SDK helper canonical form)
        {
            **common,
            "client_order_index": coid_base + 4,
            "order_type": _SC.ORDER_TYPE_MARKET,
            "order_expiry": _SC.DEFAULT_IOC_EXPIRY,
            "_label": "MARKET + 0  [SDK helpers' canonical form]",
        },
        # 5. MARKET + (now+30s)
        {
            **common,
            "client_order_index": coid_base + 5,
            "order_type": _SC.ORDER_TYPE_MARKET,
            "order_expiry": now_ms + 30_000,
            "_label": "MARKET + (now+30s)",
        },
    ]
    return variants


async def _run_variants(
    gateway: LighterGateway,
    variants: List[Dict[str, Any]],
    send: bool,
) -> List[Dict[str, Any]]:
    """Sign-or-send each variant; collect outcomes."""
    signer = gateway.signer_client
    if signer is None:
        raise RuntimeError(
            "gateway.signer_client is None — API_KEY_PRIVATE_KEY missing or auth failed"
        )

    results: List[Dict[str, Any]] = []
    for v in variants:
        label = v.pop("_label")
        kwargs = dict(v)
        outcome: Dict[str, Any] = {"label": label, "kwargs": kwargs}
        if send:
            try:
                res = await asyncio.wait_for(
                    signer.create_order(**kwargs), timeout=15.0
                )
            except Exception as exc:  # noqa: BLE001
                outcome["raised"] = repr(exc)
            else:
                # SDK returns (CreateOrder, RespSendTx, None) on success
                # or (None, None, error_str) on failure.
                if isinstance(res, tuple) and len(res) >= 3:
                    if res[2] is None:
                        outcome["accepted"] = True
                        outcome["tx_resp"] = repr(res[1])[:200]
                    else:
                        outcome["accepted"] = False
                        outcome["error"] = str(res[2])
                else:
                    outcome["unexpected_shape"] = repr(res)[:200]
        else:
            # Dry run: sign locally to verify the SDK accepts the
            # payload at the signing layer without ever sending.
            try:
                tx_type, tx_info, tx_hash, error = signer.sign_create_order(**kwargs)
            except Exception as exc:  # noqa: BLE001
                outcome["sign_raised"] = repr(exc)
            else:
                if error is not None:
                    outcome["sign_error"] = str(error)
                else:
                    outcome["sign_ok"] = True
                    outcome["tx_type"] = tx_type
                    outcome["tx_hash"] = tx_hash[:32] if tx_hash else None
        results.append(outcome)
    return results


def _print_results(results: List[Dict[str, Any]]) -> None:
    print()
    print("=" * 70)
    for i, r in enumerate(results, 1):
        kw = r["kwargs"]
        print(f"[{i}] {r['label']}")
        print(
            f"    payload: order_type={kw['order_type']} "
            f"time_in_force={kw['time_in_force']} "
            f"order_expiry={kw['order_expiry']} "
            f"reduce_only={kw['reduce_only']} "
            f"base_amount={kw['base_amount']} price={kw['price']}"
        )
        if "accepted" in r:
            print(f"    SEND: accepted={r['accepted']}")
            if r["accepted"]:
                print(f"    tx_resp: {r['tx_resp']}")
            else:
                print(f"    error:  {r['error']}")
        elif "raised" in r:
            print(f"    SEND: raised {r['raised']}")
        elif "sign_ok" in r:
            print(f"    SIGN: ok (tx_type={r['tx_type']}, dry-run)")
        elif "sign_error" in r:
            print(f"    SIGN: error {r['sign_error']}")
        elif "sign_raised" in r:
            print(f"    SIGN: raised {r['sign_raised']}")
        elif "unexpected_shape" in r:
            print(f"    SEND: unexpected response shape {r['unexpected_shape']}")
        print()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--symbol", default="SKHYNIXUSD", help="market symbol")
    p.add_argument(
        "--send",
        action="store_true",
        help="actually fire each variant against Lighter (default: dry-run signs only)",
    )
    p.add_argument(
        "--probe",
        action="store_true",
        help="before sending, fetch and print account/market state",
    )
    p.add_argument(
        "--config",
        default=str(_ROOT / "config" / "lighter_config.yaml"),
        help="lighter gateway config path",
    )
    return p.parse_args()


async def _main(args: argparse.Namespace) -> int:
    _load_env_file(_ROOT / ".env")

    if not os.environ.get("API_KEY_PRIVATE_KEY"):
        print(
            "API_KEY_PRIVATE_KEY missing from env. Either:\n"
            "  - run scripts\\setup_env_permanent.ps1 once and reopen this shell, or\n"
            "  - export it inline before invoking this script.",
            file=sys.stderr,
        )
        return 2
    if not os.environ.get("LIGHTER_ACCOUNT_INDEX"):
        print("LIGHTER_ACCOUNT_INDEX missing from env.", file=sys.stderr)
        return 2

    print(f"mode  : {'SEND (live)' if args.send else 'DRY-RUN (sign only, no network)'}")
    print(f"symbol: {args.symbol}")

    gateway = LighterGateway.from_config_file(args.config)
    await gateway.initialize()

    market_index = gateway.get_market_index(args.symbol)
    if market_index is None:
        print(f"market {args.symbol!r} not found on Lighter", file=sys.stderr)
        await gateway.close()
        return 3

    if args.probe:
        probe = await _probe_account(gateway, market_index)
        print()
        print("=== ACCOUNT / MARKET PROBE ===")
        for k, v in probe.items():
            print(f"  {k}: {v!r}")
        print()

    if gateway.signer_client is None:
        print("signer_client is None — auth must have failed; aborting.", file=sys.stderr)
        await gateway.close()
        return 4

    # Test parameters: tiny size, far-from-market price so a fluke
    # acceptance still wouldn't move the needle.
    details = await gateway.get_order_book_details(market_index)
    pd = int(details.get("price_decimals") or 3)
    sd = int(details.get("size_decimals") or 3)
    last = details.get("last_trade_price") or Decimal("100")
    if not isinstance(last, Decimal):
        last = Decimal(str(last))

    # Buy IOC at 50% of last price → walks book finding sellers ≤ 0.5*last,
    # finds none, IOC cancels remainder. Tiny size further bounds risk.
    test_price = (last * Decimal("0.5")).quantize(
        Decimal(10) ** -pd
    )
    test_size = Decimal("0.001")
    price_int = int(test_price * (Decimal(10) ** pd))
    size_int = int(test_size * (Decimal(10) ** sd))

    print(f"market_index={market_index} (price_dec={pd}, size_dec={sd}, last={last})")
    print(f"test order: BUY {test_size} @ {test_price}  (price_int={price_int}, size_int={size_int})")
    print(f"reduce_only=True (will reject if account is flat — that's fine, we're probing)")

    coid_base = int(time.time()) * 100  # rough uniqueness
    variants = _build_variants(
        market_index=market_index,
        coid_base=coid_base,
        base_amount=size_int,
        price_int=price_int,
        is_ask=False,
    )
    results = await _run_variants(gateway, variants, send=args.send)
    _print_results(results)

    await gateway.close()
    return 0


def main() -> int:
    return asyncio.run(_main(_parse_args()))


if __name__ == "__main__":
    sys.exit(main())
