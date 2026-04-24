"""Async Lighter observation session.

Starts the REST gateway + WS client, subscribes to one or more markets,
emits a BBO line every second, a full snapshot every 60s, and writes
JSONL to disk. SIGINT triggers a graceful shutdown that closes the WS
task and recursively shuts every aiohttp ClientSession the SDK opened.
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
from typing import Any, Dict, List, Optional, Sequence

from gateways.lighter_gateway import LighterGateway
from gateways.lighter_ws import LighterWebSocket
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

    async def _fetch_rest_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        """REST-fetch a depth-5 order book, returning None on error.

        WS doesn't push order_book depth for us (see lighter_ws module
        docstring), so BBO and snapshots pull it over REST instead.
        Keep depth small (5) and cadence ≥5s to stay clear of 429.
        """
        try:
            return await self.gateway.get_orderbook(symbol, limit=5)
        except Exception as exc:  # noqa: BLE001
            logger.warning("REST get_orderbook(%s) failed: %s", symbol, exc)
            return None

    async def _emit_bbo_line(self, now: float) -> None:
        self._bbo_print_count += 1
        for sym, market_index in self._symbol_markets.items():
            book = await self._fetch_rest_book(sym)
            # mid prefers WS mark_price; falls back to REST-derived BBO mid.
            ws_mid = self.ws.get_latest_mid(market_index)
            if book is None or not book.get("bids") or not book.get("asks"):
                logger.info("[BBO] %s: no book (REST fetch returned empty)", sym)
                continue
            bb = book["bids"][0][0]
            ba = book["asks"][0][0]
            rest_mid = (bb + ba) / Decimal(2)
            mid = ws_mid if ws_mid is not None else rest_mid
            spread_bp = None
            if mid and mid > 0:
                spread_bp = (ba - bb) / mid * Decimal("10000")
            logger.info(
                "[BBO] %s bid=%s ask=%s mid=%s(src=%s) spread=%sbp depth=%d/%d",
                sym,
                bb,
                ba,
                mid,
                "ws" if ws_mid is not None else "rest",
                f"{spread_bp:.2f}" if spread_bp is not None else "?",
                len(book.get("bids", [])),
                len(book.get("asks", [])),
            )

    async def _emit_snapshot(self, now: float) -> None:
        self._snapshot_count += 1
        stats = self.ws.get_message_stats()
        per_symbol_books: Dict[str, Any] = {}
        for sym, market_index in self._symbol_markets.items():
            book = await self._fetch_rest_book(sym)
            ms = self.ws.get_market_stats(market_index)
            ws_mid = self.ws.get_latest_mid(market_index)
            best_bid = book["bids"][0][0] if book and book.get("bids") else None
            best_ask = book["asks"][0][0] if book and book.get("asks") else None
            rest_mid = None
            if best_bid is not None and best_ask is not None:
                rest_mid = (best_bid + best_ask) / Decimal(2)
            mid = ws_mid if ws_mid is not None else rest_mid
            per_symbol_books[sym] = {
                "market_index": market_index,
                "best_bid": _safe_str(best_bid),
                "best_ask": _safe_str(best_ask),
                "mid": _safe_str(mid),
                "mid_source": "ws" if ws_mid is not None else ("rest" if rest_mid is not None else None),
                "bid_depth": len(book.get("bids", [])) if book else 0,
                "ask_depth": len(book.get("asks", [])) if book else 0,
                "mark_price": _safe_str(ms.get("mark_price") if ms else None),
                "index_price": _safe_str(ms.get("index_price") if ms else None),
                "current_funding_rate": _safe_str(
                    ms.get("current_funding_rate") if ms else None
                ),
            }
        # Session context
        now_utc = datetime.now(tz=timezone.utc)
        summary = {
            "event": "snapshot",
            "ts_ms": int(now * 1000),
            "elapsed_sec": now - (self._start_ts_ms or now * 1000) / 1000.0,
            "schedule": self.config.schedule.value,
            "is_weekend": is_weekend_for(self.config.schedule, now_utc),
            "books": per_symbol_books,
            "ws_stats": {
                "msg_count_by_type": stats.get("msg_count_by_type", {}),
                "connect_count": stats.get("connect_count"),
                "subscribed_markets": stats.get("subscribed_markets", []),
            },
        }
        self._jsonl_write(summary)
        logger.info(
            "[OBSERVE] snapshot #%d symbols=%d ws_msgs=%s connects=%d",
            self._snapshot_count,
            len(per_symbol_books),
            stats.get("msg_count_by_type", {}),
            stats.get("connect_count") or 0,
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
