"""Paper-mode verification of the P7 (5-4) active-hedge fix.

Runs the new ``LppQuoter._maybe_trigger_active_hedge`` flow against a
fully-mocked gateway/OM/ws stack, with two resting maker orders on the
book and inventory above the configured trigger. Validates the four
properties the P7 fix requires:

    1. Hedge submit is **post_only LIMIT**, NOT IOC. Reason:
       Lighter's account-level self-trade protection rejects IOC
       reduce_only even after pre-cancelling resting makers (Day-3
       SAMSUNG, 5-3 → 5-4 UTC: 3/3 IOC submits silently rejected).
    2. ``reduce_only=False`` (reduce_only itself may be a self-trade
       trigger; using a regular limit avoids the flag).
    3. IOC price = ``best_bid + 1 tick`` for sells (or
       ``best_ask - 1 tick`` for buys) — improves the spread by 1
       tick, becoming the new top-of-book on the close side.
    4. Cancel SDK calls strictly precede the post_only create call
       (defensive book hygiene, kept from the P6 design).

Earlier P6 paper test verified IOC + BBO * 0.9995. P7 supersedes
that — the IOC path is left in place in the OM but no longer used by
active_hedge.

This is a smoke test, not a pytest test — run it directly to gut-check
the fix locally::

    python scripts/paper_active_hedge_check.py
    python scripts/paper_active_hedge_check.py --yaml config/lighter_strategy_samsung.yaml

Exit code: 0 on full pass, non-zero on any failed assertion.
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

_THIS = Path(__file__).resolve().parent
_ROOT = _THIS.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml  # noqa: E402

from execution.lighter.lighter_order_manager import (  # noqa: E402
    LighterOrderManager,
    _ORDER_TYPE_MAP,
    _TIME_IN_FORCE_MAP,
)
from strategy.lpp_quoter import LppQuoter  # noqa: E402
from strategy.types import InventoryState, MarketSnapshot  # noqa: E402


# ----- minimal fakes (mirror tests/test_lpp_quoter.py) -------------------


class _FakeSigner:
    def __init__(self) -> None:
        self.create_calls: List[Dict[str, Any]] = []
        self.cancel_calls: List[Dict[str, Any]] = []
        self.event_log: List[Tuple[str, int]] = []  # ("create"|"cancel", seq)
        self._seq = 0

    async def create_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self._seq += 1
        self.event_log.append(("create", self._seq))
        self.create_calls.append(kwargs)
        return ("ok", "ok", None)

    async def cancel_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self._seq += 1
        self.event_log.append(("cancel", self._seq))
        self.cancel_calls.append(kwargs)
        return ("ok", "ok", None)


class _FakeGateway:
    def __init__(self) -> None:
        self._signer = _FakeSigner()
        self.book: Optional[Dict[str, Any]] = None
        self.open_orders_response: List[Dict[str, Any]] = []
        self.account_info: Optional[Dict[str, Any]] = {
            "collateral": Decimal("2000"),
            "available_balance": Decimal("2000"),
        }

    @property
    def signer_client(self) -> _FakeSigner:
        return self._signer

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[Dict[str, Any]]:
        return self.book

    async def get_open_orders(self, market_index: int) -> List[Dict[str, Any]]:
        return list(self.open_orders_response)

    async def cancel_order_by_index(
        self, market_index: int, order_index: int
    ) -> Dict[str, Any]:
        return {"ok": True, "tx": "fake"}

    async def get_account_info(self) -> Optional[Dict[str, Any]]:
        return dict(self.account_info) if self.account_info else None


class _FakeWs:
    def __init__(self) -> None:
        self.account_callbacks: List[Any] = []
        self._last_msg_ts_ms: int = int(time.time() * 1000)

    def register_account_callback(self, cb: Any) -> None:
        self.account_callbacks.append(cb)

    async def subscribe_account(self, idx: int) -> None:
        return None

    def get_latest_mid(self, mi: int) -> Optional[Decimal]:
        return None

    def get_market_stats(self, mi: int) -> Optional[Dict[str, Any]]:
        return None

    def get_message_stats(self) -> Dict[str, Any]:
        return {"last_msg_ts_ms_global": self._last_msg_ts_ms}


class _FakeTracker:
    async def record_snapshot(self, **kwargs: Any) -> Dict[str, Any]:
        return {}

    def get_session_summary(self, **kwargs: Any) -> Dict[str, Any]:
        return {}

    def close(self) -> None:
        return None


def _ws_order_msg(
    coid: int,
    status: str,
    *,
    order_index: int = 9001,
    filled: str = "0",
    remaining: str = "0",
) -> Dict[str, Any]:
    return {
        "channel": "account_all/42",
        "type": "update/account_all",
        "orders": [
            {
                "client_order_index": coid,
                "order_index": order_index,
                "market_index": 161,
                "is_ask": False,
                "price": "100000",
                "filled_base_amount": filled,
                "remaining_base_amount": remaining,
                "status": status,
            }
        ],
    }


# ----- scenario builder --------------------------------------------------


def _load_yaml_active_hedge(path: Optional[Path]) -> Dict[str, Any]:
    """Pull the active_hedge_* knobs from a yaml; fall back to a sane
    default set matching the post-P7 SAMSUNG retune."""
    if path is None or not path.exists():
        return {
            "active_hedge_enabled": True,
            "active_hedge_trigger_pct": Decimal("0.7"),
            "active_hedge_target_pct": Decimal("0.0"),
            "active_hedge_taker_fee_max_pct": Decimal("0.50"),
            "active_hedge_pause_after_sec": 60,
            "active_hedge_max_consecutive_fails": 3,
            "active_hedge_post_submit_wait_sec": Decimal("120"),
        }
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    strat = raw.get("strategy") or {}
    out: Dict[str, Any] = {}
    for k in (
        "active_hedge_enabled",
        "active_hedge_trigger_pct",
        "active_hedge_target_pct",
        "active_hedge_taker_fee_max_pct",
        "active_hedge_pause_after_sec",
        "active_hedge_max_consecutive_fails",
        "active_hedge_post_submit_wait_sec",
    ):
        if k in strat:
            v = strat[k]
            if isinstance(v, str):
                try:
                    v = Decimal(v)
                except Exception:  # noqa: BLE001
                    pass
            out[k] = v
    # Override enabled to True for the scenario regardless of the
    # yaml's live value (P7 stays gated off in production until
    # paper-verified — which is exactly what this script does).
    out["active_hedge_enabled"] = True
    return out


def _market_samsung_like(mid: Decimal) -> MarketSnapshot:
    """SAMSUNG-shaped market: 25bp spread, 3-decimal tick (so the +1
    tick price math is unambiguous: best_bid + 0.001)."""
    best_bid = (mid - Decimal("0.020")).quantize(Decimal("0.001"))
    best_ask = (mid + Decimal("0.020")).quantize(Decimal("0.001"))
    return MarketSnapshot(
        symbol="SAMSUNG",
        market_index=161,
        mid=mid,
        mark_price=mid,
        index_price=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        spread_bp=(best_ask - best_bid) / mid * Decimal(10000),
        depth_by_spread_bp={},
        price_decimals=3,
        size_decimals=3,
        ts_ms=int(time.time() * 1000),
    )


async def _run_scenario(yaml_path: Optional[Path]) -> int:
    """Returns 0 on full pass, non-zero on any failure."""
    cfg_overrides: Dict[str, Any] = {
        "hard_position_cap_usdc": Decimal("200"),
        "hard_position_cap_pct": None,
        "active_hedge_pre_cancel_wait_sec": Decimal("1"),
        "active_hedge_post_submit_wait_sec": Decimal("0.5"),
        "active_hedge_poll_interval_sec": Decimal("0.02"),
    }
    cfg_overrides.update(_load_yaml_active_hedge(yaml_path))

    market = _market_samsung_like(Decimal("160.000"))
    gw = _FakeGateway()
    gw.book = {
        "symbol": "SAMSUNG",
        "market_index": 161,
        "bids": [[market.best_bid, Decimal("100")]],
        "asks": [[market.best_ask, Decimal("100")]],
        "timestamp_ms": int(time.time() * 1000),
    }
    ws = _FakeWs()
    om = LighterOrderManager(
        gateway=gw, ws=ws, account_index=42,
        request_timeout_sec=1.0, retry_max_attempts=1, retry_backoff_sec=0.0,
    )
    tracker = _FakeTracker()
    quoter = LppQuoter(
        gateway=gw, ws=ws, order_manager=om, tracker=tracker,
        symbol="SAMSUNG", market_index=161,
        price_decimals=3, size_decimals=3,
        account_index=42, config=cfg_overrides,
    )
    await quoter.start()

    # 1) Pre-place 2 maker quotes on market 161 (size 0.500 marker so
    # we can distinguish them from the hedge in fill-wrap and asserts).
    inv = InventoryState(
        net_delta_base=Decimal("0.906"),
        net_delta_usdc=Decimal("145"),
        avg_entry_price=Decimal("160"),
        open_orders_count=2,
    )
    maker_coids: List[int] = []
    for side, price in (("buy", Decimal("159.940")), ("sell", Decimal("160.060"))):
        coid = await om.submit_order(
            side=side, market_index=161, price=price,
            size_base=Decimal("0.500"),
            price_decimals=3, size_decimals=3,
        )
        maker_coids.append(coid)
        om.on_account_event(_ws_order_msg(coid, "open", order_index=coid % 100000))

    creates_before = len(gw.signer_client.create_calls)
    cancels_before = len(gw.signer_client.cancel_calls)
    # Patch om.cancel_order so the ws "canceled" push is synthesised.
    original_cancel = om.cancel_order

    async def _cancel_with_ack(c: int) -> bool:
        result = await original_cancel(c)
        om.on_account_event(_ws_order_msg(c, "canceled", order_index=c % 100000))
        return result

    om.cancel_order = _cancel_with_ack  # type: ignore[method-assign]

    # 2) Wrap submit_order so the post_only HEDGE (size != maker
    # marker) gets a synthetic fill via ws push. Without this, the
    # hedge times out and we can't observe the success path.
    original_submit = om.submit_order

    async def _submit_with_fill(**kw: Any) -> int:
        coid = await original_submit(**kw)
        is_hedge = (
            kw.get("time_in_force") == "post_only"
            and not kw.get("reduce_only")
            and kw.get("size_base") != Decimal("0.500")  # not a maker
        )
        if is_hedge:
            om.on_account_event(_ws_order_msg(
                coid, "filled",
                filled=str(kw["size_base"]), remaining="0",
                order_index=coid % 100000,
            ))
        return coid

    om.submit_order = _submit_with_fill  # type: ignore[method-assign]

    # 3) Capture book state at every SDK create call so we can prove
    #    the close saw an empty book.
    book_at_create: List[List[Tuple[int, str, str]]] = []
    original_signer_create = gw.signer_client.create_order

    async def _wrap_create(**kw: Any) -> Any:
        book_at_create.append([
            (o.client_order_index, o.status, o.time_in_force)
            for o in om.get_active_orders()
            if o.market_index == 161
        ])
        return await original_signer_create(**kw)

    gw.signer_client.create_order = _wrap_create  # type: ignore[assignment]

    # 4) Trigger hedge.
    triggered = await quoter._maybe_trigger_active_hedge(market, inv)

    creates_after = len(gw.signer_client.create_calls)
    cancels_after = len(gw.signer_client.cancel_calls)

    # ---- assertions ----
    failures: List[str] = []

    if not triggered:
        failures.append(
            f"FAIL: hedge did not fire (expected True given inv="
            f"{inv.net_delta_usdc} > trigger)"
        )
    if creates_after - creates_before != 1:
        failures.append(
            f"FAIL: expected 1 close create, got "
            f"{creates_after - creates_before}"
        )
    if cancels_after - cancels_before != 2:
        failures.append(
            f"FAIL: expected 2 pre-cancel calls, got "
            f"{cancels_after - cancels_before}"
        )

    # Order: cancels must precede the create. The signer event_log
    # has 2 maker creates (pre-place) at the start; everything after
    # is scenario.
    log = gw.signer_client.event_log
    scenario_log = [t[0] for t in log[2:]]
    if scenario_log != ["cancel", "cancel", "create"]:
        failures.append(
            f"FAIL: cancel-before-create ordering broken: {scenario_log}"
        )

    # P7: SDK call must be LIMIT + post_only + reduce_only=False.
    last_create = gw.signer_client.create_calls[-1]
    if last_create.get("order_type") != _ORDER_TYPE_MAP["limit"]:
        failures.append(
            f"FAIL: order_type={last_create.get('order_type')} expected "
            f"LIMIT={_ORDER_TYPE_MAP['limit']}"
        )
    if last_create.get("time_in_force") != _TIME_IN_FORCE_MAP["post_only"]:
        failures.append(
            f"FAIL: time_in_force={last_create.get('time_in_force')} "
            f"expected POST_ONLY={_TIME_IN_FORCE_MAP['post_only']}"
        )
    if last_create.get("reduce_only") is not False:
        failures.append(
            f"FAIL: reduce_only={last_create.get('reduce_only')} "
            f"(P7 must be False to avoid Lighter self-trade trigger)"
        )
    if "order_expiry" in last_create:
        failures.append(
            f"FAIL: order_expiry was set on a post_only path: "
            f"{last_create['order_expiry']} (this is the IOC path)"
        )

    # P7: close price must be best_bid + 1 tick (sell) — strictly
    # above best_bid (improving) and strictly below best_ask
    # (post_only doesn't cross).
    tick = Decimal("0.001")
    expected_close_price = (market.best_bid + tick).quantize(tick)
    expected_close_price_int = int(expected_close_price * Decimal(1000))
    if last_create["price"] != expected_close_price_int:
        failures.append(
            f"FAIL: close price {last_create['price']} != expected "
            f"{expected_close_price_int} (= best_bid {market.best_bid} "
            f"+ 1 tick = {expected_close_price})"
        )
    bb_int = int(market.best_bid * Decimal(1000))
    ba_int = int(market.best_ask * Decimal(1000))
    if last_create["price"] <= bb_int:
        failures.append(
            f"FAIL: close price {last_create['price']} not above "
            f"best_bid {bb_int} — should improve top-of-book"
        )
    if last_create["price"] >= ba_int:
        failures.append(
            f"FAIL: close price {last_create['price']} not below "
            f"best_ask {ba_int} — would cross, post_only would reject"
        )
    if last_create.get("is_ask") is not True:
        failures.append("FAIL: hedge side is not 'sell'")

    # Book at create: no resting makers should be on the book. The
    # book_at_create list has 3 entries: 2 maker pre-places (before
    # the wrap) + 1 close create. The third entry's "before this
    # create" snapshot should have no resting post_only makers.
    if not book_at_create:
        failures.append("FAIL: no SDK create captured")
    else:
        close_book = book_at_create[-1]
        leftover = [b for b in close_book if b[0] in maker_coids]
        if leftover:
            failures.append(
                f"FAIL: at close submit time, makers were still on "
                f"the book: {leftover}"
            )

    # ---- report ----
    print()
    print("=" * 60)
    print("Scenario (P7 post_only LIMIT close):")
    print(f"  cap=$200 trigger_pct=0.7 -> trigger=$140")
    print(f"  inv=${inv.net_delta_usdc} (over trigger)")
    print(f"  market mid=${market.mid} BBO=[{market.best_bid}, {market.best_ask}]")
    print(f"  pre-placed 2 resting makers @ 159.940 (buy) and 160.060 (sell)")
    print()
    print("Outcome:")
    print(f"  pre-cancel calls = {cancels_after - cancels_before}")
    print(f"  close create calls = {creates_after - creates_before}")
    print(f"  SDK call order   = {scenario_log}")
    print(f"  close price (int) = {last_create.get('price')}")
    print(f"    (best_bid={market.best_bid} + 1 tick = {expected_close_price})")
    print(f"  order_type        = {last_create.get('order_type')} "
          f"(expected LIMIT={_ORDER_TYPE_MAP['limit']})")
    print(f"  time_in_force     = {last_create.get('time_in_force')} "
          f"(expected POST_ONLY={_TIME_IN_FORCE_MAP['post_only']})")
    print(f"  reduce_only       = {last_create.get('reduce_only')} "
          f"(expected False)")
    print(f"  is_ask (sell)     = {last_create.get('is_ask')}")
    print(f"  hedge fired       = {triggered} (success path)")
    if book_at_create:
        close_book = book_at_create[-1]
        leftover = [b for b in close_book if b[0] in maker_coids]
        print(f"  makers on book at close submit = "
              f"{leftover or '[none]'}")
    print()
    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print(f"  {f}")
        await quoter.shutdown()
        return 1
    print("RESULT: PASS — all 4 properties verified")
    print("  [OK] hedge submit is post_only LIMIT (NOT IOC)")
    print("  [OK] reduce_only=False (P7 avoids self-trade trigger)")
    print("  [OK] price = best_bid + 1 tick (improves top-of-book)")
    print("  [OK] book clean of resting makers at close submit time")
    await quoter.shutdown()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yaml",
        type=Path,
        default=None,
        help="Optional strategy yaml to source active_hedge_* values from.",
    )
    args = parser.parse_args()
    return asyncio.run(_run_scenario(args.yaml))


if __name__ == "__main__":
    sys.exit(main())
