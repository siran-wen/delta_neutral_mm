"""Async Lighter observation session.

Starts the REST gateway + WS client, subscribes to one or more markets,
emits a BBO line every second, a full snapshot every 60s, and writes
JSONL to disk. SIGINT triggers a graceful shutdown that closes the WS
task and recursively shuts every aiohttp ClientSession the SDK opened.

Phase 1.0.2 (Apr 2026): each 60s snapshot now carries deeper book
shape (top-N levels, cumulative depth in USDC across spread tiers),
the 60s trade tape per market, and rolling WS latency percentiles —
enough raw signal to design a Phase 1.1 strategy without re-running.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gateways.lighter_gateway import LighterGateway
from gateways.lighter_ws import LighterWebSocket
from observe.depth_aggregation import (
    DEFAULT_DEPTH_TIERS_BP,
    DEFAULT_TOP_LEVELS,
    aggregate_depth_by_spread_bp,
    aggregate_trades_window,
    top_levels,
)
from observe.session_clock import MarketSchedule, is_weekend_for

logger = logging.getLogger(__name__)


def _decimal_to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_decimal_to_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


@dataclass
class ObservationConfig:
    symbols: List[str]
    duration_sec: Optional[float] = None  # None = run forever until SIGINT
    snapshot_interval_sec: float = 60.0
    # REST-polling cadence for the BBO line. 5s is a safe default;
    # going lower risks 429 since every tick fetches one book per symbol.
    log_book_every_sec: float = 5.0
    output_dir: Path = field(default_factory=lambda: Path("logs/lighter_observe"))
    include_account: bool = True
    schedule: MarketSchedule = MarketSchedule.CRYPTO_24X7
    # Depth analysis tier thresholds (bp from mid). Cumulative depth is
    # reported once per snapshot so the strategy designer can reason
    # about share at each tier without re-deriving from raw levels.
    depth_tiers_bp: Tuple[Decimal, ...] = field(
        default_factory=lambda: DEFAULT_DEPTH_TIERS_BP
    )
    # How many bid/ask levels to persist verbatim per snapshot. Default
    # of 5 keeps file size bounded (~1MB per 30 min for 5 markets).
    top_levels: int = DEFAULT_TOP_LEVELS
    # Order-book depth fetched per snapshot (REST). 20 is the practical
    # sweet spot — enough to cover the 200bp tier on liquid markets,
    # but not so many that we trip 429 once per minute per market.
    snapshot_book_depth: int = 20


class LighterObserver:
    """Ties gateway + ws together into a long-running observation session."""

    def __init__(
        self,
        gateway: LighterGateway,
        ws: LighterWebSocket,
        config: ObservationConfig,
    ):
        self.gateway = gateway
        self.ws = ws
        self.config = config
        self._stop_event: asyncio.Event = asyncio.Event()
        self._jsonl_path: Optional[Path] = None
        self._start_ts_ms: Optional[int] = None
        self._message_log_fh: Optional[Any] = None
        self._signal_registered: bool = False
        # Symbol → market_index mapping resolved at start()
        self._symbol_markets: Dict[str, int] = {}
        # Optional pre-warmed REST baseline snapshot
        self._baseline_books: Dict[str, Dict[str, Any]] = {}
        # Stats
        self._bbo_print_count: int = 0
        self._snapshot_count: int = 0

    # --------------------------------------------------------------
    # signal handling
    # --------------------------------------------------------------

    def request_stop(self) -> None:
        """Idempotent — request the observation session to wind down."""
        if not self._stop_event.is_set():
            self._stop_event.set()
            logger.info("Stop requested — completing current cycle then shutting down")

    def _register_signal_handlers(self) -> None:
        if self._signal_registered:
            return
        loop = asyncio.get_running_loop()
        # On Windows, add_signal_handler is not supported on ProactorEventLoop
        # for SIGINT; fall back to default KeyboardInterrupt handling.
        try:
            loop.add_signal_handler(signal.SIGINT, self.request_stop)
            try:
                loop.add_signal_handler(signal.SIGTERM, self.request_stop)
            except (AttributeError, NotImplementedError):
                pass
            self._signal_registered = True
        except NotImplementedError:
            logger.info(
                "Signal handlers not supported on this platform; "
                "Ctrl-C will still abort via KeyboardInterrupt"
            )

    # --------------------------------------------------------------
    # lifecycle
    # --------------------------------------------------------------

    async def run(self) -> None:
        """Run the full observation pipeline. Returns cleanly on shutdown."""
        self._register_signal_handlers()
        self._start_ts_ms = int(time.time() * 1000)
        await self._prepare_output()
        try:
            await self._resolve_symbols()
            await self._warm_baseline_books()
            await self._probe_server_time()
            await self._maybe_dump_account_snapshot()
            await self._start_ws()

            loop_task = asyncio.create_task(self._periodic_loop())
            stop_task = asyncio.create_task(self._stop_event.wait())
            duration_task: Optional[asyncio.Task] = None
            if self.config.duration_sec is not None:
                duration_task = asyncio.create_task(
                    asyncio.sleep(self.config.duration_sec)
                )

            wait_set = {loop_task, stop_task}
            if duration_task is not None:
                wait_set.add(duration_task)

            done, pending = await asyncio.wait(
                wait_set, return_when=asyncio.FIRST_COMPLETED
            )
            # A finished duration timer just means we ran the whole window
            # cleanly. Signal the loop and cancel anything still pending.
            self.request_stop()
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for t in done:
                exc = t.exception() if t.cancelled() is False else None
                if exc is not None:
                    raise exc
        finally:
            await self._shutdown()

    async def _prepare_output(self) -> None:
        out_dir = Path(self.config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        symbol_tag = "_".join(s.replace("/", "-") for s in self.config.symbols) or "all"
        self._jsonl_path = out_dir / f"observe_{symbol_tag}_{stamp}.jsonl"
        # open append-mode text file — we flush after every line
        self._message_log_fh = open(self._jsonl_path, "a", encoding="utf-8")
        self._jsonl_write(
            {
                "event": "session_start",
                "ts_ms": self._start_ts_ms,
                "symbols": self.config.symbols,
                "duration_sec": self.config.duration_sec,
                "schedule": self.config.schedule.value,
            }
        )
        logger.info("Observation log → %s", self._jsonl_path)

    async def _resolve_symbols(self) -> None:
        missing: List[str] = []
        for sym in self.config.symbols:
            idx = self.gateway.get_market_index(sym)
            if idx is None:
                missing.append(sym)
            else:
                self._symbol_markets[sym] = idx
        if missing:
            available = sorted(self.gateway.symbol_to_market_index.keys())
            raise RuntimeError(
                f"Symbols not found on Lighter: {missing}. "
                f"Available (first 20): {available[:20]}"
            )
        logger.info(
            "Resolved symbols: %s",
            ", ".join(f"{s}={self._symbol_markets[s]}" for s in self.config.symbols),
        )

    async def _warm_baseline_books(self) -> None:
        for sym in self.config.symbols:
            try:
                book = await self.gateway.get_orderbook(sym, limit=5)
            except Exception as exc:  # noqa: BLE001
                logger.warning("REST baseline book failed for %s: %s", sym, exc)
                continue
            if book is not None:
                self._baseline_books[sym] = book
                self._jsonl_write(
                    {
                        "event": "rest_baseline_book",
                        "ts_ms": int(time.time() * 1000),
                        "symbol": sym,
                        "market_index": book.get("market_index"),
                        "bids": [[str(p), str(s)] for p, s in book.get("bids", [])],
                        "asks": [[str(p), str(s)] for p, s in book.get("asks", [])],
                    }
                )

    async def _probe_server_time(self) -> None:
        try:
            t = await self.gateway.get_server_time()
        except Exception as exc:  # noqa: BLE001
            logger.warning("server_time probe failed: %s", exc)
            return
        logger.info(
            "REST server_time probe: rtt=%.1fms local=%s server=%s",
            t.get("rtt_ms", float("nan")),
            t.get("local_ms"),
            t.get("server_ms"),
        )
        self._jsonl_write({"event": "server_time_probe", **t})

    async def _maybe_dump_account_snapshot(self) -> None:
        if not self.config.include_account:
            return
        if self.gateway.signer_client is None:
            logger.info(
                "No SignerClient configured; skipping account snapshot "
                "(pass --no-account to silence)"
            )
            return
        try:
            info = await self.gateway.get_account_info()
            bal = await self.gateway.get_account_balance()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Account snapshot failed: %s", exc)
            return
        self._jsonl_write(
            {
                "event": "account_snapshot",
                "ts_ms": int(time.time() * 1000),
                "account_info": _decimal_to_jsonable(info),
                "balance": _decimal_to_jsonable(bal),
            }
        )
        logger.info(
            "Account: account_index=%s, collateral=%s, available=%s",
            info.get("account_index") if info else None,
            bal.get("total") if bal else None,
            bal.get("free") if bal else None,
        )

    async def _start_ws(self) -> None:
        market_indices = list(self._symbol_markets.values())
        await self.ws.connect(market_indices)
        # Persist every orderbook / market_stats event to JSONL via callbacks
        self.ws.register_orderbook_callback(self._on_orderbook_update)
        self.ws.register_market_stats_callback(self._on_market_stats_update)
        # Persist a compact trade record for every WS trade — same
        # rationale as market_stats: keeps a wire-level audit trail
        # alongside the 60s aggregates we write at snapshot time.
        if hasattr(self.ws, "register_trade_callback"):
            self.ws.register_trade_callback(self._on_trade_update)
        logger.info("WebSocket ready on %d markets", len(market_indices))

    # --------------------------------------------------------------
    # callbacks
    # --------------------------------------------------------------

    def _on_orderbook_update(self, market_index: int, snapshot: Dict[str, Any]) -> None:
        # Fast path: persist only a compact summary; full depth is kept in-memory.
        symbol = self._market_to_symbol(market_index)
        payload = {
            "event": "ws_orderbook",
            "ts_ms": snapshot.get("recv_ts_ms"),
            "symbol": symbol,
            "market_index": market_index,
            "best_bid": _safe_str(snapshot.get("best_bid")),
            "best_ask": _safe_str(snapshot.get("best_ask")),
            "mid": _safe_str(snapshot.get("mid")),
            "bid_depth": len(snapshot.get("bids", [])),
            "ask_depth": len(snapshot.get("asks", [])),
        }
        self._jsonl_write(payload)

    def _on_market_stats_update(
        self, market_index: int, stats: Dict[str, Any]
    ) -> None:
        symbol = self._market_to_symbol(market_index)
        payload = {
            "event": "ws_market_stats",
            "ts_ms": stats.get("recv_ts_ms"),
            "symbol": symbol,
            "market_index": market_index,
            "last_trade_price": _safe_str(stats.get("last_trade_price")),
            "mark_price": _safe_str(stats.get("mark_price")),
            "index_price": _safe_str(stats.get("index_price")),
            "current_funding_rate": _safe_str(stats.get("current_funding_rate")),
        }
        self._jsonl_write(payload)

    def _on_trade_update(
        self, market_index: int, trade: Dict[str, Any]
    ) -> None:
        """Persist each trade as a JSONL line for the audit trail."""
        symbol = self._market_to_symbol(market_index)
        self._jsonl_write(
            {
                "event": "ws_trade",
                "ts_ms": trade.get("ts_ms"),
                "symbol": symbol,
                "market_index": market_index,
                "price": _safe_str(trade.get("price")),
                "size": _safe_str(trade.get("size")),
                "usd_amount": _safe_str(trade.get("usd_amount")),
                "side": trade.get("side"),
                "type": trade.get("type"),
                "trade_id": trade.get("trade_id"),
            }
        )

    # --------------------------------------------------------------
    # periodic loop
    # --------------------------------------------------------------

    async def _periodic_loop(self) -> None:
        last_snapshot_ts = 0.0
        try:
            while not self._stop_event.is_set():
                now = time.time()
                # BBO line (pulls REST order books + WS mark price)
                await self._emit_bbo_line(now)
                # Full snapshot
                if now - last_snapshot_ts >= self.config.snapshot_interval_sec:
                    await self._emit_snapshot(now)
                    last_snapshot_ts = now
                # Sleep until the next BBO tick or stop
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.config.log_book_every_sec,
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass

    async def _fetch_rest_book(
        self,
        symbol: str,
        limit: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """REST-fetch a depth-N order book, returning None on error.

        WS doesn't push order_book depth for us (see lighter_ws module
        docstring), so BBO and snapshots pull it over REST instead.
        BBO ticks pass ``limit=5``; snapshot ticks request the larger
        ``snapshot_book_depth`` so the depth-tier aggregation has
        enough levels to cover the wide tiers (≥100 bp).
        """
        try:
            return await self.gateway.get_orderbook(symbol, limit=limit)
        except Exception as exc:  # noqa: BLE001
            logger.warning("REST get_orderbook(%s, limit=%s) failed: %s", symbol, limit, exc)
            return None

    async def _emit_bbo_line(self, now: float) -> None:
        self._bbo_print_count += 1
        # Use the same widest tier the snapshot uses so the BBO line
        # answers "how thick is the book within the strategy's
        # critical band right now". For default tiers we report ≤15bp.
        tiers = self.config.depth_tiers_bp or DEFAULT_DEPTH_TIERS_BP
        # Pick a "near" tier ~15 bp by default for visibility; fall
        # back to the smallest configured tier if the user dropped it.
        target_bp = next(
            (t for t in tiers if t == Decimal("15")),
            tiers[len(tiers) // 2 if len(tiers) >= 1 else 0],
        )
        for sym, market_index in self._symbol_markets.items():
            book = await self._fetch_rest_book(sym, limit=5)
            # mid prefers WS mark_price; falls back to REST-derived BBO mid.
            ws_mid = self.ws.get_latest_mid(market_index)
            if book is None or not book.get("bids") or not book.get("asks"):
                logger.info("[BBO] %s: no book (REST fetch returned empty)", sym)
                continue
            bb = book["bids"][0][0]
            ba = book["asks"][0][0]
            rest_mid = (bb + ba) / Decimal(2)
            # For depth-tier display we use the BBO mid (rest_mid)
            # rather than ws_mid because the tier comparison is over
            # the actual book the depth is computed from.
            depth = aggregate_depth_by_spread_bp(
                book.get("bids", []),
                book.get("asks", []),
                rest_mid,
                tiers,
            )
            tier_key = str(target_bp.normalize() if isinstance(target_bp, Decimal) else target_bp)
            tier_info = depth.get(tier_key, {"bid_usdc": Decimal("0"), "ask_usdc": Decimal("0")})

            # 60s trade tape (best-effort — empty when the trade
            # channel is silent or rejected).
            now_ms = int(now * 1000)
            recent = self.ws.get_recent_trades(market_index, since_ts_ms=now_ms - 60_000)
            trades_summary = aggregate_trades_window(recent, now_ms - 60_000, now_ms)

            mid = ws_mid if ws_mid is not None else rest_mid
            spread_bp = None
            if mid and mid > 0:
                spread_bp = (ba - bb) / mid * Decimal("10000")
            logger.info(
                "[BBO] %s bid=%s ask=%s mid=%s(src=%s) spread=%sbp "
                "depth(≤%sbp): bid=$%s ask=$%s "
                "trades_60s: %d (B%d/S%d)",
                sym,
                bb,
                ba,
                mid,
                "ws" if ws_mid is not None else "rest",
                f"{spread_bp:.2f}" if spread_bp is not None else "?",
                tier_key,
                _short_money(tier_info["bid_usdc"]),
                _short_money(tier_info["ask_usdc"]),
                trades_summary["count"],
                trades_summary["buy_count"],
                trades_summary["sell_count"],
            )

    async def _emit_snapshot(self, now: float) -> None:
        self._snapshot_count += 1
        stats = self.ws.get_message_stats()
        tiers = self.config.depth_tiers_bp or DEFAULT_DEPTH_TIERS_BP
        per_symbol_books: Dict[str, Any] = {}
        now_ms = int(now * 1000)
        for sym, market_index in self._symbol_markets.items():
            book = await self._fetch_rest_book(
                sym, limit=self.config.snapshot_book_depth
            )
            ms = self.ws.get_market_stats(market_index)
            ws_mid = self.ws.get_latest_mid(market_index)
            bids = book.get("bids", []) if book else []
            asks = book.get("asks", []) if book else []
            best_bid = bids[0][0] if bids else None
            best_ask = asks[0][0] if asks else None
            rest_mid: Optional[Decimal] = None
            if best_bid is not None and best_ask is not None:
                rest_mid = (best_bid + best_ask) / Decimal(2)
            mid = ws_mid if ws_mid is not None else rest_mid
            spread_bp_val: Optional[Decimal] = None
            if mid and mid > 0 and best_bid is not None and best_ask is not None:
                spread_bp_val = (best_ask - best_bid) / mid * Decimal("10000")

            # Cumulative depth tiers compute against the *BBO mid* (not
            # ws_mid / mark) — it's the only mid that's consistent with
            # the levels we have on hand, and matches lpp_share_estimator's
            # default behavior.
            depth_ref_mid = rest_mid if rest_mid is not None else mid
            depth_by_tier_raw = aggregate_depth_by_spread_bp(
                bids, asks, depth_ref_mid, tiers
            )
            depth_by_tier = _stringify_depth(depth_by_tier_raw)

            bid_levels = top_levels(bids, self.config.top_levels)
            ask_levels = top_levels(asks, self.config.top_levels)

            recent = self.ws.get_recent_trades(market_index, since_ts_ms=now_ms - 60_000)
            trades_summary = _stringify_trade_summary(
                aggregate_trades_window(recent, now_ms - 60_000, now_ms)
            )

            per_symbol_books[sym] = {
                "market_index": market_index,
                "best_bid": _safe_str(best_bid),
                "best_ask": _safe_str(best_ask),
                "mid": _safe_str(mid),
                "mid_source": "ws" if ws_mid is not None else ("rest" if rest_mid is not None else None),
                "spread_bp": float(spread_bp_val) if spread_bp_val is not None else None,
                "bid_depth": len(bids),
                "ask_depth": len(asks),
                "mark_price": _safe_str(ms.get("mark_price") if ms else None),
                "index_price": _safe_str(ms.get("index_price") if ms else None),
                "current_funding_rate": _safe_str(
                    ms.get("current_funding_rate") if ms else None
                ),
                "bid_levels": bid_levels,
                "ask_levels": ask_levels,
                "depth_by_spread_bp": depth_by_tier,
                "trades_in_60s": trades_summary,
                "trade_subscription_failed": market_index in stats.get(
                    "trade_subscription_failed", []
                ),
            }
        # Session context
        now_utc = datetime.now(tz=timezone.utc)
        summary: Dict[str, Any] = {
            "event": "snapshot",
            "ts_ms": now_ms,
            "elapsed_sec": now - (self._start_ts_ms or now * 1000) / 1000.0,
            "schedule": self.config.schedule.value,
            "is_weekend": is_weekend_for(self.config.schedule, now_utc),
            "books": per_symbol_books,
            "ws_stats": {
                "msg_count_by_type": stats.get("msg_count_by_type", {}),
                "connect_count": stats.get("connect_count"),
                "subscribed_markets": stats.get("subscribed_markets", []),
                "trade_subscription_failed": stats.get(
                    "trade_subscription_failed", []
                ),
                # Phase 1.0.3 stability fields. Present whenever the
                # WS supports them; LighterWebSocket always populates
                # these so this branch is informational only.
                "last_msg_ts_ms_global": stats.get("last_msg_ts_ms_global"),
                "max_uptime_sec": stats.get("max_uptime_sec"),
                "connection_uptime_sec_current": stats.get(
                    "connection_uptime_sec_current"
                ),
                "last_disconnect_reason": stats.get("last_disconnect_reason"),
                "keepalive_fail_count": stats.get("keepalive_fail_count"),
                "ping_sent": stats.get("ping_sent"),
                "pong_received": stats.get("pong_received"),
            },
        }
        latency_stats = self.ws.get_recent_latency_stats()
        if latency_stats is not None:
            summary["ws_latency_60s"] = latency_stats
        self._jsonl_write(summary)
        logger.info(
            "[OBSERVE] snapshot #%d symbols=%d ws_msgs=%s connects=%d latency=%s",
            self._snapshot_count,
            len(per_symbol_books),
            stats.get("msg_count_by_type", {}),
            stats.get("connect_count") or 0,
            latency_stats,
        )

    # --------------------------------------------------------------
    # helpers
    # --------------------------------------------------------------

    def _market_to_symbol(self, market_index: int) -> Optional[str]:
        for sym, idx in self._symbol_markets.items():
            if idx == market_index:
                return sym
        return self.gateway.get_symbol(market_index)

    def _jsonl_write(self, payload: Dict[str, Any]) -> None:
        if self._message_log_fh is None:
            return
        try:
            self._message_log_fh.write(
                json.dumps(_decimal_to_jsonable(payload), ensure_ascii=False) + "\n"
            )
            self._message_log_fh.flush()
        except Exception as exc:  # noqa: BLE001
            logger.debug("JSONL write failed: %s", exc)

    async def _shutdown(self) -> None:
        logger.info("Shutting down observation session")
        try:
            await self.ws.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.warning("WS disconnect raised: %s", exc)
        try:
            await self.gateway.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Gateway close raised: %s", exc)
        # Trailing summary line + file close
        stats = self.ws.get_message_stats()
        self._jsonl_write(
            {
                "event": "session_end",
                "ts_ms": int(time.time() * 1000),
                "bbo_print_count": self._bbo_print_count,
                "snapshot_count": self._snapshot_count,
                "ws_stats": stats,
            }
        )
        if self._message_log_fh is not None:
            try:
                self._message_log_fh.close()
            except Exception:  # noqa: BLE001
                pass
            self._message_log_fh = None


async def run_rwa_probe(
    gateway: LighterGateway,
    symbols: Sequence[str] = (
        "SAMSUNGUSD",
        "HYUNDAIUSD",
        "SKHYNIXUSD",
        "XAU",
        "BRENTOIL",
    ),
) -> List[Dict[str, Any]]:
    """Print market_id / price_decimals / current book for the RWA cohort.

    Returns a list of dict rows (one per requested symbol) so the caller
    can make further assertions.
    """
    rows: List[Dict[str, Any]] = []
    for sym in symbols:
        idx = gateway.get_market_index(sym)
        if idx is None:
            rows.append({"symbol": sym, "found": False})
            print(f"[PROBE] {sym}: NOT FOUND on Lighter")
            continue
        details = await gateway.get_order_book_details(idx)
        book = await gateway.get_orderbook(sym, limit=5)
        best_bid = None
        best_ask = None
        if book and book.get("bids"):
            best_bid = book["bids"][0][0]
        if book and book.get("asks"):
            best_ask = book["asks"][0][0]
        row = {
            "symbol": sym,
            "found": True,
            "market_index": idx,
            "price_decimals": details.get("price_decimals"),
            "size_decimals": details.get("size_decimals"),
            "status": details.get("status"),
            "last_trade_price": details.get("last_trade_price"),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_depth": len(book.get("bids", [])) if book else 0,
            "ask_depth": len(book.get("asks", [])) if book else 0,
        }
        rows.append(row)
        print(
            f"[PROBE] {sym:12s} market_index={idx:3d} "
            f"price_decimals={details.get('price_decimals')} "
            f"last={details.get('last_trade_price')} "
            f"bid={best_bid} ask={best_ask} "
            f"depth={len(book.get('bids', [])) if book else 0}x"
            f"{len(book.get('asks', [])) if book else 0}"
        )
    return rows


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _stringify_depth(
    depth: Dict[str, Dict[str, Decimal]],
) -> Dict[str, Dict[str, str]]:
    """Convert ``aggregate_depth_by_spread_bp`` output to JSON-safe strings."""
    return {
        tier: {k: str(v) for k, v in info.items()}
        for tier, info in depth.items()
    }


def _stringify_trade_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Same idea for the trade summary — Decimal → str."""
    out: Dict[str, Any] = {}
    for k, v in summary.items():
        if isinstance(v, Decimal):
            out[k] = str(v)
        else:
            out[k] = v
    return out


def _short_money(value: Decimal) -> str:
    """Format a USDC amount as short human-readable money ('1.5k', '23m')."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "0"
    sign = "-" if v < 0 else ""
    v = abs(v)
    if v < 1000:
        return f"{sign}{v:.0f}"
    if v < 1_000_000:
        return f"{sign}{v/1000:.1f}k"
    return f"{sign}{v/1_000_000:.2f}m"
