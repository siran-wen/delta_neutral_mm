"""Paper-trade entry script — drives the full Phase 1.1 strategy loop
against live Lighter market data without sending any orders.

Run with::

    python scripts/run_lighter_paper.py \\
        --market SKHYNIXUSD \\
        --duration 5 \\
        --initial-collateral 2000 \\
        --output-dir logs/paper \\
        --seed 42 \\
        -v

The script never touches ``SignerClient``. It does need outbound
network access (REST + WSS) to pull real depth / mark prices, but no
``.env`` / ``API_KEY_PRIVATE_KEY`` is read.

What runs each tick (1s default)
--------------------------------

1. Pull a fresh REST order book + WS mark for the configured market
2. Build a ``MarketSnapshot`` (depth tiers as int-keyed dict per the
   strategy contract)
3. Run ``simulator.tick`` to maybe-fill the resting paper orders
4. Decide if a reprice is needed: mark drifted ≥ ``REPRICE_DRIFT_BP``,
   the KR session just rolled over, or a fill changed our inventory
5. If yes, call ``plan_quotes``, then ``diff_quotes`` against the
   simulator's active set, and apply the cancels / places. Log every
   PAPER PLACE / PAPER CANCEL / PAPER FILL line so the JSONL audit
   has a complete decision trail
6. Every 60s, regardless of reprice, push a snapshot through
   ``LppStateTracker`` so share + reward roll-up stays continuous

Outputs
-------

* ``<output_dir>/paper_run_<symbol>_<utc>.jsonl`` — wire-level event log
* ``<output_dir>/lpp_state/<symbol>_<date>.jsonl`` — LPP per-snapshot
  share + estimated reward (managed by ``LppStateTracker``)
* End-of-session summary line on stdout + the JSONL log
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Project root on sys.path when invoked as a script
_THIS = Path(__file__).resolve()
_PROJECT_ROOT = _THIS.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml  # noqa: E402

from gateways.lighter_gateway import LighterGateway  # noqa: E402
from gateways.lighter_ws import LighterWebSocket  # noqa: E402
from observe.depth_aggregation import aggregate_depth_by_spread_bp  # noqa: E402
from strategy.lpp_state_tracker import LppStateTracker  # noqa: E402
from strategy.paper_simulator import PaperSimulator  # noqa: E402
from strategy.quote_diff import diff_quotes  # noqa: E402
from strategy.quote_planner import plan_quotes  # noqa: E402
from strategy.session_aware import get_kr_equity_session  # noqa: E402
from strategy.types import MarketSnapshot, Quote, SessionPolicy  # noqa: E402

logger = logging.getLogger("paper_run")


# ----- planner config (hardcoded; see batch 2 part 3 spec) ----------------

# share_warn_threshold raised from 0.8 → 0.95: 15h SKHYNIX observation
# showed our ask-side share routinely lived in 0.80-0.95 and the lower
# threshold would have us widening continuously, sacrificing share for
# a non-existent adverse-selection risk.
PAPER_CONFIG: Dict[str, Decimal] = {
    "target_max_delta_usdc": Decimal("500"),
    "skew_max_offset_bp": Decimal("5"),
    "hard_position_cap_usdc": Decimal("1000"),
    "min_market_spread_bp": Decimal("3"),
    "max_market_spread_bp": Decimal("100"),
    "share_warn_threshold": Decimal("0.95"),
    "share_warn_widen_bp": Decimal("5"),
}

# Spread tiers used to populate ``MarketSnapshot.depth_by_spread_bp``.
# Must include every tier that ``plan_quotes`` / ``estimate_my_share``
# might index into for KR weekday + weekend sessions
# (15/25/50 weekday, 50/100/200 weekend). Larger tiers don't hurt.
DEPTH_TIERS_BP: Tuple[Decimal, ...] = tuple(
    Decimal(s) for s in ("5", "10", "15", "25", "50", "100", "200")
)

# Reprice when the mark moves at least this far since the last
# planner run. 5bp matches the spec and is comfortable above the
# typical SKHYNIX tick noise.
REPRICE_DRIFT_BP = Decimal("5")

# How often the LPP state tracker takes a snapshot. Matches the
# tracker's default snapshot_window_sec so the per-snapshot reward
# accrual math is consistent.
SNAPSHOT_INTERVAL_SEC = 60.0

DEFAULT_TICK_INTERVAL_SEC = 1.0


# ----- helpers -------------------------------------------------------------


def _decimal_to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_decimal_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if is_dataclass(obj):
        return _decimal_to_jsonable(asdict(obj))
    return obj


def should_reprice(
    last_mid: Optional[Decimal],
    new_mid: Decimal,
    drift_threshold_bp: Decimal = REPRICE_DRIFT_BP,
) -> bool:
    """True iff the mid moved enough since the last reprice.

    First call (``last_mid is None``) always triggers — there's no
    baseline yet so we have to plan once. After that, we only re-plan
    when the cost of the cancel/place churn is justified.
    """
    if last_mid is None or last_mid <= 0:
        return True
    drift = abs(new_mid - last_mid) / last_mid * Decimal(10000)
    return drift >= drift_threshold_bp


def _coerce_int_keyed_depth(
    depth_str_keyed: Dict[str, Dict[str, Decimal]],
) -> Dict[int, Dict[str, Decimal]]:
    """``aggregate_depth_by_spread_bp`` returns string keys ("15"); the
    strategy contract expects int keys. Convert here so MarketSnapshot
    callers don't repeat the conversion."""
    out: Dict[int, Dict[str, Decimal]] = {}
    for k, v in depth_str_keyed.items():
        try:
            out[int(Decimal(str(k)))] = v
        except (ValueError, TypeError):
            continue
    return out


async def build_market_snapshot(
    gateway: LighterGateway,
    ws: LighterWebSocket,
    symbol: str,
    market_index: int,
    price_decimals: int,
    size_decimals: int,
    book_depth: int = 20,
) -> Optional[MarketSnapshot]:
    """Pull REST depth + WS mark/index, assemble a ``MarketSnapshot``.

    Returns ``None`` on degenerate input (empty book, zero mid) so the
    main loop can skip a tick rather than feed garbage to the planner.
    """
    book = await gateway.get_orderbook(symbol, limit=book_depth)
    if book is None:
        return None
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        return None

    best_bid: Decimal = bids[0][0]
    best_ask: Decimal = asks[0][0]
    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        return None

    rest_mid = (best_bid + best_ask) / Decimal(2)
    ws_mid = ws.get_latest_mid(market_index)
    stats = ws.get_market_stats(market_index)
    mark_price = (stats or {}).get("mark_price") if stats else None
    index_price = (stats or {}).get("index_price") if stats else None

    # Prefer ws mark/index for the mid (matches the planner's
    # "price oracle" intent), fall back to BBO mid.
    mid = mark_price or index_price or ws_mid or rest_mid
    if mid is None or mid <= 0:
        return None

    spread_bp = (best_ask - best_bid) / mid * Decimal(10000)

    depth_str_keyed = aggregate_depth_by_spread_bp(
        bids, asks, rest_mid, DEPTH_TIERS_BP
    )
    depth_int_keyed = _coerce_int_keyed_depth(depth_str_keyed)

    return MarketSnapshot(
        symbol=symbol,
        market_index=market_index,
        mid=mid,
        mark_price=mark_price,
        index_price=index_price,
        best_bid=best_bid,
        best_ask=best_ask,
        spread_bp=spread_bp,
        depth_by_spread_bp=depth_int_keyed,
        price_decimals=price_decimals,
        size_decimals=size_decimals,
        ts_ms=int(time.time() * 1000),
    )


# ----- main runner ---------------------------------------------------------


class PaperRun:
    """Encapsulates the paper-trade loop state so SIGINT can stop it."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self._stop_event: Optional[asyncio.Event] = None
        self._signal_registered = False

        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_symbol = args.market.replace("/", "-")
        self.jsonl_path = self.output_dir / f"paper_run_{safe_symbol}_{stamp}.jsonl"
        self._fh = open(self.jsonl_path, "a", encoding="utf-8")

    # --- lifecycle ---

    def request_stop(self) -> None:
        if self._stop_event is not None and not self._stop_event.is_set():
            self._stop_event.set()
            logger.info("Stop requested — shutting down")

    def _register_signals(self) -> None:
        if self._signal_registered:
            return
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, self.request_stop)
            try:
                loop.add_signal_handler(signal.SIGTERM, self.request_stop)
            except (AttributeError, NotImplementedError):
                pass
            self._signal_registered = True
        except NotImplementedError:
            # Windows ProactorEventLoop — KeyboardInterrupt path handles it.
            logger.info(
                "Signal handlers unavailable on this platform; "
                "Ctrl-C still aborts via KeyboardInterrupt"
            )

    # --- output ---

    def _jsonl(self, payload: Dict[str, Any]) -> None:
        try:
            self._fh.write(
                json.dumps(_decimal_to_jsonable(payload), ensure_ascii=False) + "\n"
            )
            self._fh.flush()
        except Exception as exc:  # noqa: BLE001
            logger.debug("JSONL write failed: %s", exc)

    # --- main loop ---

    async def run(self) -> int:
        args = self.args
        self._stop_event = asyncio.Event()
        self._register_signals()

        # Open gateway in public-data-only mode (no api_key_private_key).
        gw_kwargs = {
            "testnet": False,
            "api_url": "https://mainnet.zklighter.elliot.ai",
            "ws_url": "wss://mainnet.zklighter.elliot.ai/stream",
            "api_key_private_key": "",
        }
        gateway = LighterGateway(gw_kwargs)
        ws = LighterWebSocket(ws_url=gw_kwargs["ws_url"], testnet=False)

        try:
            await gateway.initialize()
        except Exception as exc:  # noqa: BLE001
            logger.error("Gateway init failed: %s", exc)
            self._jsonl({"event": "init_error", "error": str(exc)})
            self._fh.close()
            return 3

        market_index = gateway.get_market_index(args.market)
        if market_index is None:
            logger.error(
                "Symbol %s not found on Lighter. Run scripts/observe_lighter.py "
                "--probe-rwa to see available markets.",
                args.market,
            )
            await gateway.close()
            self._fh.close()
            return 2

        details = await gateway.get_order_book_details(market_index)
        price_decimals = int(details.get("price_decimals") or 4)
        size_decimals = int(details.get("size_decimals") or 4)

        try:
            await ws.connect([market_index])
        except Exception as exc:  # noqa: BLE001
            logger.error("WS connect failed: %s", exc)
            self._jsonl({"event": "ws_connect_error", "error": str(exc)})
            await gateway.close()
            self._fh.close()
            return 4

        # Load LPP pool config + instantiate tracker
        pool_path = _PROJECT_ROOT / "config" / "lpp_pool.yaml"
        with open(pool_path, "r", encoding="utf-8") as f:
            pool_cfg = yaml.safe_load(f) or {}
        weekly_pool = {
            sym: Decimal(str(v))
            for sym, v in (pool_cfg.get("lpp_pool") or {}).items()
        }
        tier_weights = {
            tag: Decimal(str(v))
            for tag, v in (pool_cfg.get("tier_weights") or {}).items()
        }
        tracker = LppStateTracker(
            weekly_pool_usdc=weekly_pool,
            tier_weights=tier_weights,
            output_dir=self.output_dir / "lpp_state",
            snapshot_window_sec=Decimal(str(SNAPSHOT_INTERVAL_SEC)),
        )

        simulator = PaperSimulator(
            initial_collateral_usdc=Decimal(str(args.initial_collateral)),
            tick_interval_sec=DEFAULT_TICK_INTERVAL_SEC,
            random_seed=args.seed,
        )

        # Session-start log
        start_ts_ms = int(time.time() * 1000)
        end_ts_sec = time.time() + args.duration * 60
        self._jsonl(
            {
                "event": "session_start",
                "ts_ms": start_ts_ms,
                "symbol": args.market,
                "market_index": market_index,
                "price_decimals": price_decimals,
                "size_decimals": size_decimals,
                "duration_min": args.duration,
                "seed": args.seed,
                "config": PAPER_CONFIG,
            }
        )
        logger.info(
            "Paper run starting: market=%s idx=%d duration=%.1fmin seed=%s log=%s",
            args.market,
            market_index,
            args.duration,
            args.seed,
            self.jsonl_path,
        )

        last_mid_for_reprice: Optional[Decimal] = None
        last_session_name: Optional[str] = None
        last_snapshot_ts: float = 0.0
        ticks = 0
        skipped_ticks = 0

        # ------------------------------------------------------------
        # main tick loop
        # ------------------------------------------------------------
        try:
            while time.time() < end_ts_sec and not self._stop_event.is_set():
                tick_start = time.time()
                ticks += 1

                market = await build_market_snapshot(
                    gateway,
                    ws,
                    args.market,
                    market_index,
                    price_decimals,
                    size_decimals,
                )
                if market is None:
                    skipped_ticks += 1
                    await self._sleep_to_next_tick(tick_start)
                    continue

                # Fill engine — runs every tick
                fills = simulator.tick(market, ts_ms=market.ts_ms)
                for fill in fills:
                    logger.info(
                        "PAPER FILL: %s side=%s price=%s size=%s usd=%s",
                        fill["client_order_id"],
                        fill["side"],
                        fill["price"],
                        fill["size_base"],
                        fill["size_usdc"],
                    )
                    self._jsonl({"event": "paper_fill", **fill})

                # Session policy
                session = get_kr_equity_session(datetime.now(timezone.utc))
                session_changed = (
                    last_session_name is not None
                    and session.name != last_session_name
                )

                # Reprice gate
                reprice_now = (
                    should_reprice(last_mid_for_reprice, market.mid)
                    or bool(fills)
                    or session_changed
                )

                inventory = simulator.get_inventory_state(mark_price=market.mid)

                desired: List[Quote] = []
                if reprice_now:
                    desired = plan_quotes(
                        market=market,
                        session=session,
                        inventory=inventory,
                        config=PAPER_CONFIG,
                    )
                    cancels, places = diff_quotes(
                        desired=desired,
                        active=list(simulator.active_orders.values()),
                    )
                    for oid in cancels:
                        simulator.cancel_order(oid)
                        logger.info("PAPER CANCEL: %s", oid)
                        self._jsonl(
                            {
                                "event": "paper_cancel",
                                "ts_ms": market.ts_ms,
                                "client_order_id": oid,
                            }
                        )
                    for q in places:
                        oid = simulator.place_order(
                            side=q.side,
                            price=q.price,
                            size_base=q.size_base,
                            market_position=q.market_position,
                            tier_target=q.tier_target,
                            ts_ms=market.ts_ms,
                        )
                        logger.info(
                            "PAPER PLACE: %s %s price=%s size=%s tier=%s pos=%s",
                            oid,
                            q.side,
                            q.price,
                            q.size_base,
                            q.tier_target,
                            q.market_position,
                        )
                        self._jsonl(
                            {
                                "event": "paper_place",
                                "ts_ms": market.ts_ms,
                                "client_order_id": oid,
                                "side": q.side,
                                "price": q.price,
                                "size_base": q.size_base,
                                "size_usdc": q.size_usdc,
                                "tier_target": q.tier_target,
                                "market_position": q.market_position,
                                "distance_from_mid_bp": q.distance_from_mid_bp,
                                "notes": list(q.notes),
                            }
                        )
                    last_mid_for_reprice = market.mid
                    last_session_name = session.name
                else:
                    # Even when not repricing, keep last_session_name in
                    # sync after the very first tick.
                    if last_session_name is None:
                        last_session_name = session.name

                # LPP snapshot at SNAPSHOT_INTERVAL_SEC cadence regardless of reprice
                now_ts = time.time()
                if now_ts - last_snapshot_ts >= SNAPSHOT_INTERVAL_SEC:
                    # Use the latest desired quotes if we just repriced;
                    # otherwise re-plan once to feed the tracker. This
                    # avoids the snapshot showing stale or empty quotes
                    # in a long passive window where no fill / drift
                    # triggered a reprice.
                    if not desired:
                        desired = plan_quotes(
                            market=market,
                            session=session,
                            inventory=inventory,
                            config=PAPER_CONFIG,
                        )
                    record = await tracker.record_snapshot(
                        ts_ms=market.ts_ms,
                        market=market,
                        my_quotes=desired,
                        inventory=inventory,
                        session=session,
                    )
                    last_snapshot_ts = now_ts
                    logger.info(
                        "[LPP] snapshot ts=%d session=%s avg_share=%s reward_60s=%s cum=%s",
                        market.ts_ms,
                        session.name,
                        record["share"]["avg_weighted_share"],
                        record["estimated_reward_60s_usdc"],
                        record["cumulative_estimated_reward_usdc"],
                    )

                await self._sleep_to_next_tick(tick_start)

        finally:
            # ------------------------------------------------------------
            # shutdown
            # ------------------------------------------------------------
            n_cancelled = simulator.cancel_all()
            if n_cancelled:
                logger.info("PAPER CANCEL ALL: %d resting orders", n_cancelled)
                self._jsonl({"event": "paper_cancel_all", "count": n_cancelled})

            try:
                summary = tracker.get_session_summary(
                    session_start_ms=start_ts_ms,
                    session_end_ms=int(time.time() * 1000),
                    symbol=args.market,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("session_summary failed: %s", exc)
                summary = {"error": str(exc)}
            try:
                tracker.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("tracker.close raised: %s", exc)

            ws_stats = ws.get_message_stats()
            sim_summary = simulator.get_summary()

            session_end = {
                "event": "session_end",
                "ts_ms": int(time.time() * 1000),
                "ticks": ticks,
                "skipped_ticks": skipped_ticks,
                "lpp_summary": summary,
                "ws_stats": {
                    "connect_count": ws_stats.get("connect_count"),
                    "msg_count_by_type": ws_stats.get("msg_count_by_type", {}),
                    "max_uptime_sec": ws_stats.get("max_uptime_sec"),
                    "last_disconnect_reason": ws_stats.get("last_disconnect_reason"),
                },
                "simulator": sim_summary,
            }
            self._jsonl(session_end)
            logger.info("PAPER SESSION SUMMARY: %s", session_end)

            try:
                await ws.disconnect()
            except Exception as exc:  # noqa: BLE001
                logger.debug("ws disconnect raised: %s", exc)
            try:
                await gateway.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("gateway close raised: %s", exc)
            try:
                self._fh.close()
            except Exception:  # noqa: BLE001
                pass
        return 0

    async def _sleep_to_next_tick(self, tick_start: float) -> None:
        elapsed = time.time() - tick_start
        remaining = DEFAULT_TICK_INTERVAL_SEC - elapsed
        if remaining > 0:
            try:
                # Sleep but wake early on stop request
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=remaining
                )
            except asyncio.TimeoutError:
                pass


# ----- CLI ----------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Paper-trade entry — runs the full Phase 1.1 loop "
        "against live Lighter market data without sending orders.",
    )
    p.add_argument(
        "--market",
        required=True,
        help="Market symbol to quote (e.g. SKHYNIXUSD)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=240.0,
        help="Run duration in minutes (default: 240 = 4 hours)",
    )
    p.add_argument(
        "--initial-collateral",
        type=str,
        default="2000",
        help="Starting USDC collateral (default: 2000)",
    )
    p.add_argument(
        "--output-dir",
        default="logs/paper",
        help="Where to write JSONL output (default: logs/paper)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for the paper fill engine (default: unseeded)",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="-v=INFO, -vv=DEBUG",
    )
    return p


def _setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def _async_main(argv: List[str]) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    runner = PaperRun(args)
    return await runner.run()


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    try:
        return asyncio.run(_async_main(argv))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
