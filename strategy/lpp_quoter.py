"""LppQuoter — Phase 1.1 batch 3 part 2 main loop.

Stitches together the five components built in earlier batches::

    LighterWsClient + LighterGateway   → market data (mark + REST book)
                                          ↓
                                   plan_quotes()           (decision)
                                          ↓
                                   diff_quotes()           (delta)
                                          ↓
                              LighterOrderManager          (execution)
                                          ↓
                              LppStateTracker              (audit / reward)

This module owns the once-per-second tick loop, decides whether to
reprice (mid drift / session change / fill / interval-floor), gates
the SDK call rate via the order manager's semaphore, and triggers an
emergency cancel-all on consecutive rejects or ws silence.

What's *not* here
-----------------
* No live entry script — that's batch 3 part 3. The unit tests below
  drive ``run_until`` against fully-mocked dependencies; a runnable
  CLI lives next to ``scripts/run_lighter_paper.py`` once we wire
  this class to a real gateway.
* No paper mode — ``scripts/run_lighter_paper.py`` already plays
  that role using ``PaperSimulator`` instead of ``LighterOrderManager``.

Reprice priority (high → low)
-----------------------------
1. Session change (e.g. KR_OVERNIGHT → KR_BEFORE_OPEN) — overrides
   the min-interval floor; size/distance changes immediately.
2. Pending fill signal — overrides the floor; inventory just shifted
   so the skew side needs adjusting now.
3. Min-interval floor (default 60s) — silently drop drift triggers
   that come too close to the previous reprice. Keeps churn down.
4. Mid drift ≥ ``reprice_drift_bp`` (default 8bp, calibrated from
   24h SKHYNIX data — 5bp triggered 36% of ticks).
5. ``last_mid is None`` (first tick) — always plan once on startup.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

from execution.lighter.lighter_order_manager import (
    LighterOrderManager,
    ManagedOrder,
)
from observe.depth_aggregation import aggregate_depth_by_spread_bp
from strategy.lpp_state_tracker import LppStateTracker
from strategy.quote_diff import diff_quotes
from strategy.quote_planner import plan_quotes
from strategy.session_aware import get_kr_equity_session
from strategy.types import InventoryState, MarketSnapshot, Quote, SessionPolicy

logger = logging.getLogger(__name__)


# ----- defaults ----------------------------------------------------------

# Spread tiers fed to ``aggregate_depth_by_spread_bp``. Must include
# every tier that ``plan_quotes`` / ``LppStateTracker`` might index
# into for KR weekday and weekend sessions (15/25/50 weekday,
# 50/100/200 weekend).
DEFAULT_DEPTH_TIERS_BP: Tuple[Decimal, ...] = tuple(
    Decimal(s) for s in ("5", "10", "15", "25", "50", "100", "200")
)

QUOTER_DEFAULTS: Dict[str, Any] = {
    # Inventory management
    "target_max_delta_usdc": Decimal("500"),
    "skew_max_offset_bp": Decimal("5"),
    "hard_position_cap_usdc": Decimal("1000"),
    # Market state guards
    "min_market_spread_bp": Decimal("3"),
    "max_market_spread_bp": Decimal("100"),
    # Share warn
    "share_warn_threshold": Decimal("0.95"),
    "share_warn_widen_bp": Decimal("5"),
    # Reprice control (calibrated from 24h SKHYNIX observation data)
    "reprice_drift_bp": Decimal("8"),
    "reprice_min_interval_sec": Decimal("60"),
    # Tick / snapshot cadence
    "tick_interval_sec": Decimal("1"),
    "snapshot_interval_sec": Decimal("60"),
    # diff_quotes tolerance — same as paper trading defaults
    "price_tolerance_bp": Decimal("0.5"),
    "size_tolerance_pct": Decimal("0.05"),
    # Emergency stop
    "emergency_stop_on_consecutive_reject_count": 5,
    "emergency_stop_on_ws_disconnect_sec": 300,
    # Cancel-failure trigger: detects the case where the OM cancel path
    # is silently broken (e.g. order_index never propagated). Without
    # this guard, a stuck cancel can let stale orders accumulate on the
    # book — exactly how the -1.132 SKHYNIXUSD short was created.
    "emergency_stop_on_consecutive_cancel_fail_count": 3,
    # Phase 1.2 P0.4: send-failure surge detection. A 429 burst (rate
    # limit) or signer-side outage that produces many SDK errors in a
    # short window is a signal to (a) re-sync state via REST so we
    # haven't accumulated orphans the OM can't see, and (b) pause
    # repricing so we stop pumping more failed sends through. Without
    # this, the 4-30 q=0/limit-window storm let orphan orders sit on
    # the book while OM thought it had no active orders.
    "send_failure_window_sec": 30,
    "send_failure_threshold": 5,
    "reconcile_pause_sec": 60,
    # Phase 2.1 P2.1.1: pct-of-collateral cap. None disables the
    # double-cap; supplied with a Decimal pct, plan_quotes uses
    # min(hard_position_cap_usdc, pct * collateral). Refresh cadence
    # below controls how often we pull latest collateral via REST.
    "hard_position_cap_pct": None,
    "collateral_refresh_interval_sec": 60,
    # P9 (5-4): asymmetric BBO quoting. Replaces the P6/P7/P8 IOC and
    # post_only-emergency-close paths entirely. When abs(inv) breaches
    # ``asymmetric_quote_trigger_pct * effective_cap``, plan_quotes
    # rebuilds both legs as post_only LIMITs but asymmetric: close
    # leg sits aggressively at the BBO (sell at best_bid + tick when
    # long, buy at best_ask - tick when short), anti leg widens to
    # ``asymmetric_anti_distance_bp`` from mid. We keep collecting
    # maker rewards on both sides while the close leg gets queue
    # priority for any natural taker.
    #
    # Why we abandoned IOC + post_only emergency: Day-3 SAMSUNG
    # (5-3 → 5-4 UTC) showed Lighter's account-level self-trade
    # protection silently rejects IOC reduce_only (3/3 silent reject,
    # web UI throws "无法吃自己的单"). The P7 post_only-with-timeout
    # workaround dropped the anti leg during close, leaving SAMSUNG
    # single-sided for 2h while inv sat at $396 — losing maker reward
    # on both sides. P9 keeps both legs live; user-driven manual
    # close handles the rare large-residual case.
    # See memory ``lighter_active_hedge_post_only`` for the full
    # IOC-rejection record.
    "asymmetric_quote_enabled": False,
    "asymmetric_quote_trigger_pct": Decimal("0.7"),
    # ``improve_bbo`` (default): close leg = BBO ± 1 tick → new
    # top-of-book on the close side, queue priority for the next
    # taker. ``match_bbo``: close leg sits AT the existing BBO,
    # joining the queue at the back — useful when the spread is
    # already 1 tick wide and improve_bbo would cross.
    "asymmetric_close_mode": "improve_bbo",
    # Anti leg (the side that adds to inventory) widens to this many
    # bp from mid. Default 30 bp absorbs typical spread (15-25bp)
    # plus a buffer that nudges fill probability down without
    # dropping the leg. Smaller values (e.g. 10bp) keep the maker
    # reward density up at the cost of more anti fills; larger
    # values (e.g. 50bp) bias the inventory drift back toward zero
    # faster.
    "asymmetric_anti_distance_bp": Decimal("30"),
    # Phase 2.1 P2.1.3: daily max drawdown stop loss. start() captures
    # collateral_start; periodic check fires emergency_stop when
    # drawdown >= min(daily_max_drawdown_usdc, daily_max_drawdown_pct
    # * collateral_start).
    "daily_max_drawdown_usdc": Decimal("100"),
    "daily_max_drawdown_pct": Decimal("0.05"),
    "daily_drawdown_check_interval_sec": 300,
}


def merge_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Shallow merge user overrides onto ``QUOTER_DEFAULTS``."""
    cfg = dict(QUOTER_DEFAULTS)
    if overrides:
        cfg.update(overrides)
    return cfg


# ----- helpers -----------------------------------------------------------


def _coerce_int_keyed_depth(
    depth_str_keyed: Dict[str, Dict[str, Decimal]],
) -> Dict[int, Dict[str, Decimal]]:
    """``aggregate_depth_by_spread_bp`` returns string keys; the
    strategy contract expects int keys. Same helper as in
    ``run_lighter_paper.py`` — kept private here to avoid coupling
    the quoter to a script module."""
    out: Dict[int, Dict[str, Decimal]] = {}
    for k, v in depth_str_keyed.items():
        try:
            out[int(Decimal(str(k)))] = v
        except (ValueError, TypeError):
            continue
    return out


def _adapt_managed_order(o: ManagedOrder) -> Dict[str, Any]:
    """Translate ``ManagedOrder`` into the dict shape ``diff_quotes`` expects.

    ``diff_quotes`` was authored for the paper simulator's
    ``active_orders.values()`` shape (``client_order_id`` / ``side`` /
    ``price`` / ``size_base``). The order manager uses
    ``client_order_index`` instead — adapt here so the upstream
    contract stays untouched.
    """
    return {
        "client_order_id": o.client_order_index,
        "side": o.side,
        "price": o.price,
        "size_base": o.size_base,
    }


# ----- the quoter --------------------------------------------------------


class LppQuoter:
    """Async main-loop class that drives plan_quotes against live Lighter.

    Lifecycle: ``__init__`` → ``await start()`` → ``await run_until(end)``
    → ``await shutdown()``. ``shutdown`` is also called automatically
    on emergency-stop conditions and is safe to invoke a second time.
    """

    def __init__(
        self,
        gateway: Any,
        ws: Any,
        order_manager: LighterOrderManager,
        tracker: LppStateTracker,
        symbol: str,
        market_index: int,
        price_decimals: int,
        size_decimals: int,
        account_index: int,
        config: Optional[Dict[str, Any]] = None,
    ):
        self._gateway = gateway
        self._ws = ws
        self._om = order_manager
        self._tracker = tracker

        self._symbol = symbol
        self._market_index = int(market_index)
        self._price_decimals = int(price_decimals)
        self._size_decimals = int(size_decimals)
        self._account_index = int(account_index)

        self.config = merge_config(config)

        self._started: bool = False
        self._stopping: bool = False
        self._closed: bool = False

        # Per-session counters surfaced via ``get_summary``.
        self._stats: Dict[str, Any] = {
            "tick_count": 0,
            "skipped_tick_count": 0,
            "reprice_count": 0,
            "last_reprice_reason": None,
            "last_reprice_ts_ms": None,
            "consecutive_rejects": 0,
            # Cancel failures across consecutive reprices. A reprice
            # whose cancel batch all succeeds resets the counter; any
            # failure increments by the count of failures observed.
            "consecutive_cancel_failures": 0,
            "emergency_stops": 0,
            "snapshots_taken": 0,
            "session_start_ts_ms": None,
            "session_end_ts_ms": None,
            # Phase 1.2 P0.4 surge handler.
            "reconcile_count": 0,
            # P9 (5-4): asymmetric BBO quoting count — number of
            # reprices that emitted asymmetric (close-aggressive +
            # anti-wide) quotes. Useful for forensics: "how often
            # did we run inv-protect mode this session?".
            "asymmetric_reprices": 0,
            # Phase 2.1 P2.1.3 daily drawdown stop.
            "daily_drawdown_breaches": 0,
        }

        # Phase 1.2 P0.4: rolling window of send-failure timestamps
        # (ms). When N failures land inside W seconds, the next
        # ``_execute_reprice`` forces a REST reconcile + backup cancel
        # and freezes repricing for ``reconcile_pause_sec`` so we
        # stop pumping more failed sends through the SDK.
        self._send_failure_window: List[int] = []
        self._reconcile_pause_until_ms: int = 0

        # Phase 2.1 P2.1.1: latest known collateral. Refreshed via REST
        # at ``collateral_refresh_interval_sec`` cadence inside the run
        # loop. None until the first refresh succeeds; plan_quotes
        # treats None as "no pct cap, fall back to absolute usdc cap".
        self._latest_collateral_usdc: Optional[Decimal] = None
        self._collateral_refresh_interval_sec: int = int(
            self.config.get("collateral_refresh_interval_sec", 60)
        )
        self._last_collateral_refresh_ts_ms: int = 0

        # P9 (5-4): asymmetric quoting flag. Plan_quotes reads this
        # via ``self.config`` directly; we keep an instance copy so
        # the startup banner and get_summary can report the live
        # state without re-reading config.
        self._asymmetric_quote_enabled: bool = bool(
            self.config.get("asymmetric_quote_enabled", False)
        )

        # Phase 2.1 P2.1.3: daily drawdown.
        self._collateral_start_usdc: Optional[Decimal] = None
        self._last_drawdown_check_ts_ms: int = 0

        # Session overrides from yaml: passed as config["session_overrides"]
        # (a dict of session_name -> {default_size_usdc, default_distance_bp,
        # tier_thresholds_bp}). If absent, get_kr_equity_session falls back
        # to hard-coded defaults in strategy/session_aware.py (which are
        # $500-$1000 nominal size). See 04-29 root-cause analysis: those
        # hard-coded defaults caused single fills 5x larger than
        # hard_position_cap_usdc on a small-size run.
        raw_overrides = self.config.get("session_overrides") or {}
        self._session_overrides: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw_overrides, dict):
            for sess_name, sess_cfg in raw_overrides.items():
                if isinstance(sess_cfg, dict):
                    self._session_overrides[str(sess_name)] = dict(sess_cfg)
        if not self._session_overrides:
            logger.warning(
                "LppQuoter: no session_overrides in config — sessions will "
                "use hard-coded defaults from strategy/session_aware.py "
                "($500-$1000 size). Verify intended."
            )
        else:
            logger.info(
                "LppQuoter: session_overrides loaded for %d sessions: %s",
                len(self._session_overrides),
                sorted(self._session_overrides.keys()),
            )

    # ------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        """Wire the ws → om callback, subscribe account_all, start om.

        ``market_stats`` and ``trade`` are assumed to have been
        subscribed already (caller's ``ws.connect([market_index])``
        does this for us). We only attach the account_all path here
        so we don't double-subscribe market channels on quoter
        restart.
        """
        if self._started:
            return
        # Route every account_all msg into the order manager's parser.
        if hasattr(self._ws, "register_account_callback"):
            self._ws.register_account_callback(self._om.on_account_event)
        else:
            logger.warning(
                "ws has no register_account_callback — order updates won't reach OM"
            )
        if hasattr(self._ws, "subscribe_account"):
            await self._ws.subscribe_account(self._account_index)
        else:
            logger.warning(
                "ws has no subscribe_account — running without account_all stream"
            )
        await self._om.start()
        self._started = True
        self._stats["session_start_ts_ms"] = int(time.time() * 1000)

        # Phase 2.1 P2.1.1 + P2.1.3: capture collateral baseline up
        # front. Used by both the pct-cap path (plan_quotes) and the
        # daily drawdown stop. Best-effort — a REST hiccup at start
        # mustn't block the quoter; the periodic refresh will fill in
        # _latest_collateral_usdc on its own when the network is back.
        await self._refresh_collateral(force=True, capture_baseline=True)

        logger.info(
            "LppQuoter started (symbol=%s, market_index=%d, account_index=%d)",
            self._symbol,
            self._market_index,
            self._account_index,
        )

    async def shutdown(self) -> None:
        """Idempotent graceful shutdown.

        1. ``_stopping = True`` so any in-flight ``run_until`` exits
           after its current tick.
        2. ``om.cancel_all`` — fire all cancel txs.
        3. Wait up to 10s for ws confirmations to drain ``_active``.
        4. Take the final tracker snapshot via ``get_session_summary``
           and log it.
        5. ``om.close`` (its inner cancel_all is a no-op since we
           already drained).
        """
        if self._closed:
            return
        self._stopping = True
        self._stats["session_end_ts_ms"] = int(time.time() * 1000)
        logger.info("shutdown: stop flag set, no new reprices accepted")

        try:
            active_before = len(self._om.get_active_orders())
            logger.info(
                "shutdown: calling om.cancel_all (active=%d)", active_before
            )
            await self._om.cancel_all()
            logger.info(
                "shutdown: om.cancel_all returned (active=%d after)",
                len(self._om.get_active_orders()),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("om.cancel_all during shutdown raised: %s", exc)

        # Wait for ws confirmations to drain _active. 10s is generous —
        # under normal conditions the cancellation push lands sub-second.
        logger.info("shutdown: waiting for ws cancellations (deadline=10s)")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if not self._om.get_active_orders():
                break
            await asyncio.sleep(0.5)
        leftover = len(self._om.get_active_orders())
        logger.info("shutdown: polling done, active=%d", leftover)
        if leftover > 0:
            logger.warning(
                "Shutdown: %d orders never confirmed cancelled within deadline",
                leftover,
            )

        # Belt-and-braces: ask the gateway directly. Catches any order
        # whose order_index never propagated to the OM (a class of bug
        # we've seen in production), and verifies the book is clean
        # before we declare shutdown done. Wrapped in try/except so a
        # REST hiccup at shutdown can't strand the close path.
        logger.info("shutdown: calling backup_cancel_via_gateway")
        try:
            await self._backup_cancel_via_gateway()
        except Exception as exc:  # noqa: BLE001
            logger.error("shutdown: backup cancel failed: %s", exc)

        # Final summary.
        try:
            summary = self._tracker.get_session_summary(
                session_start_ms=self._stats["session_start_ts_ms"] or 0,
                session_end_ms=self._stats["session_end_ts_ms"] or int(time.time() * 1000),
                symbol=self._symbol,
            )
            logger.info("Final tracker summary: %s", summary)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Final tracker summary failed: %s", exc)

        logger.info("shutdown: closing om + ws")
        try:
            await self._om.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("om.close during shutdown raised: %s", exc)

        self._closed = True
        self._started = False
        logger.info("shutdown: complete")

    # ------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------

    async def run_until(self, end_ts: float) -> None:
        """Drive the tick loop until ``end_ts`` (unix seconds).

        Single-tick exceptions are caught and logged so a transient
        error never drops the whole run. ``asyncio.CancelledError``
        is allowed to propagate so a parent task can stop the quoter.
        """
        last_reprice_ts: float = 0.0
        last_snapshot_ts: float = 0.0
        last_session_name: Optional[str] = None
        last_mid_for_drift: Optional[Decimal] = None

        tick_interval = float(self.config["tick_interval_sec"])

        while not self._stopping and time.time() < end_ts:
            tick_start = time.time()
            self._stats["tick_count"] += 1

            try:
                # Emergency stop check — runs first so a disconnect or
                # reject storm shuts us down before another reprice
                # piles on more failures.
                if self._should_emergency_stop():
                    self._stats["emergency_stops"] += 1
                    # Snapshot the full context — every threshold
                    # involved + the live state we're about to drop.
                    # Inventory uses ``None`` as the mark since we may
                    # not have a fresh market snapshot at this point;
                    # the OM falls back to avg-entry pricing for the
                    # net_delta_usdc field.
                    inv_snap = self._om.get_inventory()
                    logger.error(
                        "EMERGENCY STOP: rejects=%d (cap=%d) cancel_fails=%d (cap=%d) "
                        "ws_silent=%.1fs (cap=%ds) inv=%s active_orders=%d "
                        "asymmetric_reprices=%d",
                        self._stats["consecutive_rejects"],
                        int(self.config["emergency_stop_on_consecutive_reject_count"]),
                        self._stats["consecutive_cancel_failures"],
                        int(self.config.get(
                            "emergency_stop_on_consecutive_cancel_fail_count", 3
                        )),
                        self._ws_silence_sec() or 0,
                        int(self.config["emergency_stop_on_ws_disconnect_sec"]),
                        inv_snap.net_delta_usdc,
                        len(self._om.get_active_orders()),
                        self._stats["asymmetric_reprices"],
                    )
                    # Two-stage cleanup so a half-broken OM can't strand
                    # orders on the book:
                    # 1. Try the OM path (fast, uses cached order_index).
                    # 2. Fall back to REST direct via gateway — picks up
                    #    any order whose order_index never propagated.
                    try:
                        await self._om.cancel_all()
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "emergency: om.cancel_all failed: %s", exc
                        )
                    try:
                        await self._backup_cancel_via_gateway()
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "emergency: backup cancel failed: %s", exc
                        )
                    break

                # Phase 2.1 P2.1.1: refresh collateral on cadence so the
                # pct cap + drawdown gate see fresh values.
                await self._refresh_collateral()

                # Phase 2.1 P2.1.3: daily drawdown stop. Same effect as
                # an emergency stop — cancel everything and exit. Runs
                # before snapshot so a failing market data path can't
                # mask a real drawdown breach.
                now_ms_dd = int(time.time() * 1000)
                if await self._maybe_check_daily_drawdown(now_ms_dd):
                    logger.error(
                        "DAILY DRAWDOWN STOP: cancelling all + breaking run loop"
                    )
                    try:
                        await self._om.cancel_all()
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "drawdown stop: om.cancel_all failed: %s", exc
                        )
                    try:
                        await self._backup_cancel_via_gateway()
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "drawdown stop: backup cancel failed: %s", exc
                        )
                    break

                market = await self._build_market_snapshot()
                if market is None:
                    self._stats["skipped_tick_count"] += 1
                    await self._sleep_to_next_tick(tick_start, tick_interval)
                    continue

                # P9 (5-4): no separate hedge step. plan_quotes handles
                # asymmetric pricing internally when inv breaches the
                # configured trigger; the reprice path below carries the
                # asymmetric quotes through diff_quotes/submit_orders
                # like any other reprice.

                # Phase 1.2 P0.4: respect any active reconcile-pause.
                # We still build the snapshot above (cheap) so the
                # tick counter advances and tracker recordings can run,
                # but we skip the reprice path entirely. The pause is
                # released once the wall clock passes the deadline.
                now_ms_pause = int(time.time() * 1000)
                if now_ms_pause < self._reconcile_pause_until_ms:
                    remaining = (self._reconcile_pause_until_ms - now_ms_pause) / 1000.0
                    logger.debug(
                        "reprice paused (reconcile cooldown %.1fs remaining)",
                        remaining,
                    )
                    await self._sleep_to_next_tick(tick_start, tick_interval)
                    continue

                session = get_kr_equity_session(
                    datetime.now(timezone.utc),
                    config=self._session_overrides,
                )
                session_changed = (
                    last_session_name is not None
                    and session.name != last_session_name
                )

                pending_fill = self._om.pop_fill_signal()

                reason = self._should_reprice(
                    last_mid=last_mid_for_drift,
                    new_mid=market.mid,
                    session_changed=session_changed,
                    has_pending_fill=pending_fill,
                    last_reprice_ts=last_reprice_ts,
                    now_ts=tick_start,
                )

                if reason is not None:
                    # Drift in bp for diagnostics — only meaningful when
                    # last_mid is set (initial / first-tick reprice has
                    # no drift to report).
                    drift_bp_log = 0.0
                    if last_mid_for_drift is not None and last_mid_for_drift > 0:
                        drift_bp_log = float(
                            abs(market.mid - last_mid_for_drift)
                            / last_mid_for_drift
                            * Decimal(10000)
                        )
                    last_age_sec = (
                        (tick_start - last_reprice_ts)
                        if last_reprice_ts > 0
                        else -1.0
                    )
                    logger.info(
                        "reprice trigger: reason=%s last_mid=%s new_mid=%s "
                        "drift_bp=%.2f session_changed=%s pending_fill=%s "
                        "last_reprice_age=%.1fs",
                        reason,
                        last_mid_for_drift,
                        market.mid,
                        drift_bp_log,
                        session_changed,
                        pending_fill,
                        last_age_sec,
                    )
                    await self._execute_reprice(market, session, reason)
                    last_reprice_ts = tick_start
                    last_mid_for_drift = market.mid

                last_session_name = session.name

                # Tracker snapshot on snapshot_interval. Independent of
                # reprice cadence so the LPP audit trail is continuous.
                snap_interval = float(self.config["snapshot_interval_sec"])
                if tick_start - last_snapshot_ts >= snap_interval:
                    await self._record_snapshot(market, session)
                    last_snapshot_ts = tick_start

            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("Tick error: %s", exc, exc_info=True)

            await self._sleep_to_next_tick(tick_start, tick_interval)

    async def _sleep_to_next_tick(self, tick_start: float, tick_interval: float) -> None:
        elapsed = time.time() - tick_start
        remaining = tick_interval - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

    # ------------------------------------------------------------
    # reprice decision
    # ------------------------------------------------------------

    def _should_reprice(
        self,
        last_mid: Optional[Decimal],
        new_mid: Decimal,
        session_changed: bool,
        has_pending_fill: bool,
        last_reprice_ts: float,
        now_ts: float,
    ) -> Optional[str]:
        """Return a reason string when reprice should fire, ``None`` otherwise.

        Priority is documented in the module docstring. Implementing
        each branch as an explicit early-return makes the priority
        legible at the call site.
        """
        if session_changed:
            return "session_change"
        if has_pending_fill:
            return "fill"

        # Min-interval floor. ``last_reprice_ts == 0`` is the "never
        # repriced yet" sentinel; the floor only kicks in after the
        # first reprice has fired.
        if last_reprice_ts > 0:
            interval = now_ts - last_reprice_ts
            if interval < float(self.config["reprice_min_interval_sec"]):
                return None

        if last_mid is None:
            return "initial"

        if last_mid <= 0:
            # Defensive — last_mid invariant says it's positive once
            # set, but a degenerate market snapshot earlier in the
            # session shouldn't strand us forever.
            return "initial"

        drift_bp = abs(new_mid - last_mid) / last_mid * Decimal(10000)
        threshold = self.config["reprice_drift_bp"]
        if drift_bp >= threshold:
            return f"mid_drift({drift_bp:.1f}bp)"

        return None

    # ------------------------------------------------------------
    # Phase 2.1 helpers — collateral, active hedge, drawdown
    # ------------------------------------------------------------

    async def _refresh_collateral(
        self,
        force: bool = False,
        capture_baseline: bool = False,
    ) -> None:
        """Pull latest collateral via REST, store on ``_latest_collateral_usdc``.

        With ``force=True`` the cadence check is skipped (used by
        ``start()`` to capture the day-zero baseline) and the previous
        timestamp is still updated so the periodic loop's next check
        is offset from the baseline call.

        With ``capture_baseline=True`` (only meaningful at ``start()``),
        the fetched value is also written to ``_collateral_start_usdc``
        for the daily-drawdown gate.

        Errors are logged + swallowed — a REST hiccup must not break
        the run loop. ``_latest_collateral_usdc`` stays at its prior
        value until the next successful refresh.
        """
        now_ms = int(time.time() * 1000)
        if not force:
            interval_ms = self._collateral_refresh_interval_sec * 1000
            if (now_ms - self._last_collateral_refresh_ts_ms) < interval_ms:
                return
        try:
            info = await self._gateway.get_account_info()
        except Exception as exc:  # noqa: BLE001
            logger.warning("collateral refresh failed: %s", exc)
            self._last_collateral_refresh_ts_ms = now_ms
            return
        if not info or info.get("collateral") is None:
            self._last_collateral_refresh_ts_ms = now_ms
            return
        try:
            new_collateral = Decimal(str(info["collateral"]))
        except Exception:  # noqa: BLE001
            self._last_collateral_refresh_ts_ms = now_ms
            return
        if self._latest_collateral_usdc != new_collateral:
            logger.info(
                "collateral refresh: %s -> %s",
                self._latest_collateral_usdc,
                new_collateral,
            )
        self._latest_collateral_usdc = new_collateral
        if capture_baseline and self._collateral_start_usdc is None:
            self._collateral_start_usdc = new_collateral
            logger.info(
                "daily_drawdown: collateral_start=%s",
                self._collateral_start_usdc,
            )
        self._last_collateral_refresh_ts_ms = now_ms

    def _effective_cap_usdc(self) -> Decimal:
        """Return the live effective cap = min(absolute, pct*collateral).

        Mirrors the logic inside ``is_position_capped`` so the active
        hedge sees the same threshold the planner used. When the pct
        config is unset or collateral hasn't been fetched, returns the
        absolute cap.
        """
        cap_usdc = Decimal(str(self.config["hard_position_cap_usdc"]))
        pct_raw = self.config.get("hard_position_cap_pct")
        if (
            pct_raw is not None
            and self._latest_collateral_usdc is not None
            and self._latest_collateral_usdc > 0
        ):
            pct_dec = (
                pct_raw if isinstance(pct_raw, Decimal) else Decimal(str(pct_raw))
            )
            pct_cap = pct_dec * self._latest_collateral_usdc
            if pct_cap < cap_usdc:
                return pct_cap
        return cap_usdc

    async def _maybe_check_daily_drawdown(self, now_ms: int) -> bool:
        """Periodic daily-drawdown gate. Returns True iff breach.

        Caller (run_until) treats True the same way as
        ``_should_emergency_stop``: cancel_all + REST backup + break.
        """
        check_interval_ms = int(
            self.config.get("daily_drawdown_check_interval_sec", 300)
        ) * 1000
        if (now_ms - self._last_drawdown_check_ts_ms) < check_interval_ms:
            return False
        self._last_drawdown_check_ts_ms = now_ms

        if (
            self._collateral_start_usdc is None
            or self._latest_collateral_usdc is None
        ):
            return False

        drawdown = self._collateral_start_usdc - self._latest_collateral_usdc
        if drawdown <= 0:
            return False
        max_dd_usdc = Decimal(
            str(self.config.get("daily_max_drawdown_usdc", "100"))
        )
        max_dd_pct = Decimal(
            str(self.config.get("daily_max_drawdown_pct", "0.05"))
        )
        max_dd_pct_usdc = max_dd_pct * self._collateral_start_usdc
        effective_max_dd = (
            max_dd_pct_usdc if max_dd_pct_usdc < max_dd_usdc else max_dd_usdc
        )

        if drawdown >= effective_max_dd:
            logger.error(
                "DAILY DRAWDOWN BREACH: drawdown=%s >= max=%s "
                "(start=%s, now=%s, abs_cap=%s, pct_cap=%s)",
                drawdown,
                effective_max_dd,
                self._collateral_start_usdc,
                self._latest_collateral_usdc,
                max_dd_usdc,
                max_dd_pct_usdc,
            )
            self._stats["daily_drawdown_breaches"] += 1
            return True
        return False

    # ------------------------------------------------------------
    # reprice execution
    # ------------------------------------------------------------

    async def _execute_reprice(
        self,
        market: MarketSnapshot,
        session: SessionPolicy,
        reason: str,
    ) -> None:
        """Compute the desired quote set, diff against active, fire SDK calls."""
        inventory = self._om.get_inventory(mark_price=market.mid)

        desired = plan_quotes(
            market=market,
            session=session,
            inventory=inventory,
            config=self.config,
            collateral_usdc=self._latest_collateral_usdc,
        )

        # P9 (5-4): bump the asymmetric counter when plan_quotes
        # produced an asymmetric-tagged leg. Either leg carrying
        # ``asymmetric_close`` / ``asymmetric_anti`` in its notes
        # is enough — the trigger fires both legs together.
        if any(
            n.startswith("asymmetric_")
            for q in desired for n in q.notes
        ):
            self._stats["asymmetric_reprices"] += 1

        active = self._om.get_active_orders()
        cancels, places = diff_quotes(
            desired=desired,
            active=[_adapt_managed_order(o) for o in active],
            price_tolerance_bp=self.config["price_tolerance_bp"],
            size_tolerance_pct=self.config["size_tolerance_pct"],
        )

        # Fan the cancels first (lighter on the SDK) so the place's
        # client-order-id space stays clean if a place lands on the
        # same tier as a cancel.
        cancel_results: List[Any] = []
        if cancels:
            cancel_results = await asyncio.gather(
                *(self._om.cancel_order(coid) for coid in cancels),
                return_exceptions=True,
            )

        place_results: List[Any] = []
        if places:
            place_results = await asyncio.gather(
                *(
                    self._om.submit_order(
                        side=q.side,
                        market_index=market.market_index,
                        price=q.price,
                        size_base=q.size_base,
                        price_decimals=market.price_decimals,
                        size_decimals=market.size_decimals,
                        order_type="limit",
                        time_in_force="post_only",
                    )
                    for q in places
                ),
                return_exceptions=True,
            )

        success_cancels = sum(1 for r in cancel_results if r is True)
        fail_cancels = len(cancel_results) - success_cancels

        # Track consecutive cancel failures for emergency-stop. A
        # cancel batch where every cancel succeeded resets; any
        # failure (False return or raised exception) accumulates.
        # We only update the counter when there were cancels to send —
        # a reprice with empty cancel set must not reset a counter
        # that's tracking a real cancel-path problem.
        if len(cancel_results) > 0:
            if fail_cancels > 0:
                self._stats["consecutive_cancel_failures"] += fail_cancels
                logger.warning(
                    "cancel failures: %d this reprice (cumulative %d)",
                    fail_cancels,
                    self._stats["consecutive_cancel_failures"],
                )
            else:
                self._stats["consecutive_cancel_failures"] = 0

        # ``om.submit_order`` records SDK errors on the ManagedOrder
        # (status="rejected") rather than raising — so a returned
        # ``client_order_index`` doesn't on its own prove the place
        # was accepted. Cross-check the OM state.
        fail_places = 0
        for r in place_results:
            if isinstance(r, BaseException):
                fail_places += 1
                continue
            state = self._om.get_order_state(r)
            if state is not None and state.status == "rejected":
                fail_places += 1
        success_places = len(place_results) - fail_places

        # Track consecutive rejects for emergency-stop. A clean reprice
        # (no place failures) resets the counter.
        if fail_places > 0:
            self._stats["consecutive_rejects"] += fail_places
        else:
            self._stats["consecutive_rejects"] = 0

        # Per-failure detail. Keeps the volume small (only on failure)
        # and gives post-hoc forensics the actual error / result.
        for r in cancel_results:
            if r is not True:
                logger.warning("cancel failed: result=%r", r)
        for r in place_results:
            if isinstance(r, BaseException):
                logger.warning("place failed: %r", r)

        self._stats["reprice_count"] += 1
        self._stats["last_reprice_reason"] = reason
        self._stats["last_reprice_ts_ms"] = int(time.time() * 1000)

        logger.info(
            "reprice exec: reason=%s desired=%d active=%d → cancels=%d places=%d "
            "(success: cancels=%d/%d places=%d/%d) inv=%s session=%s",
            reason,
            len(desired),
            len(active),
            len(cancels),
            len(places),
            success_cancels,
            len(cancels),
            success_places,
            len(places),
            inventory.net_delta_usdc,
            session.name,
        )

        # Phase 1.2 P0.4: surge detection.
        # Any failure (cancel or place) feeds the rolling window. When
        # the count crosses the threshold, force a REST reconcile so any
        # orphan that built up during the surge is cancelled, then pause
        # repricing for the cooldown window so we stop pumping more
        # failed sends through.
        total_failures = fail_cancels + fail_places
        if total_failures > 0:
            now_ms = int(time.time() * 1000)
            window_sec = int(self.config.get("send_failure_window_sec", 30))
            threshold = int(self.config.get("send_failure_threshold", 5))
            cutoff = now_ms - window_sec * 1000
            self._send_failure_window = [
                ts for ts in self._send_failure_window if ts > cutoff
            ]
            for _ in range(total_failures):
                self._send_failure_window.append(now_ms)

            if len(self._send_failure_window) >= threshold:
                pause_sec = int(self.config.get("reconcile_pause_sec", 60))
                logger.error(
                    "SEND FAILURE SURGE: %d failures in %ds → forcing reconcile + %ds pause",
                    len(self._send_failure_window),
                    window_sec,
                    pause_sec,
                )
                try:
                    await self._om._sync_orders_from_rest_safe()
                except Exception as exc:  # noqa: BLE001
                    logger.error("surge reconcile sync failed: %s", exc)
                try:
                    await self._backup_cancel_via_gateway()
                except Exception as exc:  # noqa: BLE001
                    logger.error("surge backup cancel failed: %s", exc)
                self._reconcile_pause_until_ms = now_ms + pause_sec * 1000
                self._send_failure_window.clear()
                self._stats["reconcile_count"] += 1

    # ------------------------------------------------------------
    # emergency stop
    # ------------------------------------------------------------

    def _should_emergency_stop(self) -> bool:
        """True iff any kill condition fires.

        Conditions:
        1. Consecutive place rejects ≥ ``emergency_stop_on_consecutive_reject_count``
        2. Consecutive cancel failures ≥
           ``emergency_stop_on_consecutive_cancel_fail_count`` — guards the
           regression that produced the -1.132 SKHYNIXUSD short: when the
           OM cancel path silently fails, stale orders accumulate on the
           book until something fills against them.
        3. WS msg gap (any channel) > ``emergency_stop_on_ws_disconnect_sec``

        **Not** a condition: hedge / asymmetric-quote events.
        P9 (5-4) replaced the IOC + post_only-emergency hedge path
        with asymmetric quoting that lives entirely inside
        plan_quotes — there's no fail counter, no disable state,
        no separate timeout. Inventory drift past the cap is
        handled by ``is_position_capped`` skipping the breach side;
        large residual exposure is the user's manual-close concern.
        """
        threshold_rejects = int(
            self.config["emergency_stop_on_consecutive_reject_count"]
        )
        if self._stats["consecutive_rejects"] >= threshold_rejects:
            return True

        threshold_cancel_fails = int(
            self.config.get("emergency_stop_on_consecutive_cancel_fail_count", 3)
        )
        if (
            self._stats["consecutive_cancel_failures"] >= threshold_cancel_fails
        ):
            logger.error(
                "Emergency: %d consecutive cancel failures (threshold=%d)",
                self._stats["consecutive_cancel_failures"],
                threshold_cancel_fails,
            )
            return True

        gap = self._ws_silence_sec()
        if gap is not None:
            threshold_gap = float(
                self.config["emergency_stop_on_ws_disconnect_sec"]
            )
            if gap > threshold_gap:
                return True

        return False

    async def _backup_cancel_via_gateway(self) -> None:
        """REST-direct cancel of every resting order on our market.

        Belt-and-braces cancel path. Called on shutdown / emergency-stop
        after ``om.cancel_all()`` has had its chance. The OM may have
        failed to cancel orders whose ``order_index`` is still missing
        locally (the architectural gap that produced the -1.132
        SKHYNIXUSD short); pulling the live book via
        ``OrderApi.account_active_orders`` and cancelling by on-chain id
        sidesteps that.

        Idempotent — if the book is already clean, this is a single REST
        call returning ``[]`` and we just log "verified clean ✓". Each
        cancel is wrapped so one failure can't block the others.
        """
        try:
            open_orders = await self._gateway.get_open_orders(self._market_index)
        except Exception as exc:  # noqa: BLE001
            logger.error("backup cancel: get_open_orders failed: %s", exc)
            return
        if not open_orders:
            logger.info("backup cancel: account verified clean ✓ (0 orders)")
            return
        logger.warning(
            "backup cancel: %d orders still on book after om.cancel_all",
            len(open_orders),
        )
        for o in open_orders:
            oi = o.get("order_index")
            if not oi:
                continue
            try:
                res = await self._gateway.cancel_order_by_index(
                    self._market_index, int(oi)
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "backup cancel: order_index=%s raised: %s", oi, exc
                )
                continue
            if isinstance(res, dict) and not res.get("ok"):
                logger.error(
                    "backup cancel: order_index=%s failed: %s",
                    oi,
                    res.get("error"),
                )

    def _ws_silence_sec(self) -> Optional[float]:
        """Seconds since the last inbound ws msg, or None if unknown."""
        if not hasattr(self._ws, "get_message_stats"):
            return None
        try:
            stats = self._ws.get_message_stats()
        except Exception:  # noqa: BLE001
            return None
        last_ms = stats.get("last_msg_ts_ms_global")
        if not last_ms:
            return None
        return (time.time() * 1000 - last_ms) / 1000.0

    # ------------------------------------------------------------
    # market snapshot
    # ------------------------------------------------------------

    async def _build_market_snapshot(self) -> Optional[MarketSnapshot]:
        """Pull REST depth + WS mark and assemble a ``MarketSnapshot``.

        Returns ``None`` on degenerate input (empty book, zero mid,
        crossed BBO) so the main loop skips the tick rather than
        feeding garbage into the planner.
        """
        try:
            book = await self._gateway.get_orderbook(self._symbol, limit=20)
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_orderbook failed: %s", exc)
            return None
        if book is None:
            return None
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            return None
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            return None

        rest_mid = (best_bid + best_ask) / Decimal(2)

        ws_mid: Optional[Decimal] = None
        mark_price: Optional[Decimal] = None
        index_price: Optional[Decimal] = None
        if hasattr(self._ws, "get_latest_mid"):
            ws_mid = self._ws.get_latest_mid(self._market_index)
        if hasattr(self._ws, "get_market_stats"):
            ms = self._ws.get_market_stats(self._market_index) or {}
            mark_price = ms.get("mark_price")
            index_price = ms.get("index_price")

        mid = mark_price or index_price or ws_mid or rest_mid
        if mid is None or mid <= 0:
            return None

        spread_bp = (best_ask - best_bid) / mid * Decimal(10000)
        depth_str = aggregate_depth_by_spread_bp(
            bids, asks, rest_mid, DEFAULT_DEPTH_TIERS_BP
        )
        depth_int = _coerce_int_keyed_depth(depth_str)

        return MarketSnapshot(
            symbol=self._symbol,
            market_index=self._market_index,
            mid=mid,
            mark_price=mark_price,
            index_price=index_price,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bp=spread_bp,
            depth_by_spread_bp=depth_int,
            price_decimals=self._price_decimals,
            size_decimals=self._size_decimals,
            ts_ms=int(time.time() * 1000),
        )

    # ------------------------------------------------------------
    # snapshot
    # ------------------------------------------------------------

    async def _record_snapshot(
        self,
        market: MarketSnapshot,
        session: SessionPolicy,
    ) -> None:
        """Push one snapshot to the LPP state tracker.

        Source-of-truth for ``my_quotes`` is the OM's currently-active
        order set, **not** ``plan_quotes`` output. Reasoning: a 7-min
        live trace observed a session in which ``plan_quotes`` returned
        ``[]`` (market spread momentarily under ``min_market_spread_bp``)
        while the book still carried our two resting orders for ~235s
        before the next reprice cancelled them. Recording desired in
        that window logs ``my_quotes=[]`` and zeroes the LPP share
        estimate, but the orders are real and absorbing depth — the
        snapshot lied about reward exposure.

        Tier and market_position are re-derived from the live snapshot
        (the planner's at-submission-time values are stale by now).
        """
        inventory = self._om.get_inventory(mark_price=market.mid)
        active = self._om.get_active_orders()
        actual_quotes: List[Quote] = [
            self._managed_to_quote(o, market, session) for o in active
        ]
        try:
            await self._tracker.record_snapshot(
                ts_ms=market.ts_ms,
                market=market,
                my_quotes=actual_quotes,
                inventory=inventory,
                session=session,
            )
            self._stats["snapshots_taken"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("tracker.record_snapshot raised: %s", exc)

    def _managed_to_quote(
        self,
        mo: ManagedOrder,
        market: MarketSnapshot,
        session: SessionPolicy,
    ) -> Quote:
        """Translate ``ManagedOrder`` → ``Quote`` for the tracker.

        ``ManagedOrder`` carries side / price / size_base directly;
        ``tier_target`` and ``market_position`` need re-derivation
        against the live ``market`` snapshot because the planner's
        original values were computed at submission time and the BBO
        / mid have since drifted.

        The synthetic ``notes`` tuple flags this Quote as actual-book
        rather than planner-desired so post-hoc analysis can tell the
        two sources apart in a tracker JSONL.
        """
        size_usdc = mo.size_base * mo.price
        # Distance is unsigned bp away from the live mid.
        distance_bp = (
            abs(mo.price - market.mid) / market.mid * Decimal(10000)
            if market.mid > 0
            else Decimal(0)
        )
        tier = self._tier_from_distance(distance_bp, session)

        # Market_position relative to live BBO. We don't emit
        # ``would_cross_market`` here — a live resting order by
        # definition isn't crossing right now (or it would have
        # filled). ``passive_below`` / ``passive_above`` distinguish
        # bids below best_bid / asks above best_ask from the
        # standard ``passive`` (sitting AT the BBO).
        if mo.side == "buy":
            if mo.price > market.best_bid:
                position = "improving"
            elif mo.price == market.best_bid:
                position = "passive"
            else:
                position = "passive_below"
        else:  # sell
            if mo.price < market.best_ask:
                position = "improving"
            elif mo.price == market.best_ask:
                position = "passive"
            else:
                position = "passive_above"

        return Quote(
            side=mo.side,
            price=mo.price,
            size_base=mo.size_base,
            size_usdc=size_usdc,
            distance_from_mid_bp=distance_bp,
            tier_target=tier,
            market_position=position,
            notes=(f"actual_book({mo.status})",),
        )

    @staticmethod
    def _tier_from_distance(
        distance_bp: Decimal,
        session: SessionPolicy,
    ) -> str:
        """Map distance-to-mid bp → ``L1`` / ``L2`` / ``L3`` / ``OUT``.

        Boundaries are inclusive (``<=``) so the L1 ceiling is L1 (not
        L2). Matches ``strategy.quote_planner._determine_tier``.
        """
        l1, l2, l3 = session.tier_thresholds_bp
        if distance_bp <= l1:
            return "L1"
        if distance_bp <= l2:
            return "L2"
        if distance_bp <= l3:
            return "L3"
        return "OUT"

    # ------------------------------------------------------------
    # public introspection
    # ------------------------------------------------------------

    def get_inventory(self) -> InventoryState:
        """Convenience pass-through (no mark price → uses avg entry)."""
        return self._om.get_inventory()

    def get_active_quotes_count(self) -> int:
        return len(self._om.get_active_orders())

    def get_summary(self) -> Dict[str, Any]:
        """Aggregated quoter + om stats for end-of-session logging."""
        om_stats = self._om.get_stats()
        return {
            "symbol": self._symbol,
            "market_index": self._market_index,
            "tick_count": self._stats["tick_count"],
            "skipped_tick_count": self._stats["skipped_tick_count"],
            "reprice_count": self._stats["reprice_count"],
            "last_reprice_reason": self._stats["last_reprice_reason"],
            "last_reprice_ts_ms": self._stats["last_reprice_ts_ms"],
            "consecutive_rejects": self._stats["consecutive_rejects"],
            "consecutive_cancel_failures": self._stats["consecutive_cancel_failures"],
            "emergency_stops": self._stats["emergency_stops"],
            "snapshots_taken": self._stats["snapshots_taken"],
            "reconcile_count": self._stats["reconcile_count"],
            "asymmetric_quote_enabled": self._asymmetric_quote_enabled,
            "asymmetric_reprices": self._stats["asymmetric_reprices"],
            "daily_drawdown_breaches": self._stats["daily_drawdown_breaches"],
            "collateral_start_usdc": str(self._collateral_start_usdc)
            if self._collateral_start_usdc is not None
            else None,
            "collateral_latest_usdc": str(self._latest_collateral_usdc)
            if self._latest_collateral_usdc is not None
            else None,
            "session_start_ts_ms": self._stats["session_start_ts_ms"],
            "session_end_ts_ms": self._stats["session_end_ts_ms"],
            "om": om_stats,
        }
