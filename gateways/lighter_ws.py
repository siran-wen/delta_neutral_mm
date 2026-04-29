"""Direct Lighter WebSocket client (market_stats channel only).

Phase 1.0 observer. No account channels, no order channels. Order book
depth is pulled via REST (``LighterGateway.get_orderbook``); the WS is
used strictly for real-time mark/index/last price updates.

Why market_stats-only on the direct WS:

* The reference adapter (``crypto-trading-open`` / lighter_websocket.py)
  only subscribes to ``market_stats/{mi}`` over direct WS. It uses the
  SDK's sync WsClient for order-book callbacks. We avoid the SDK's
  WsClient (it's sync-only and pushes events on threads, which doesn't
  play well with the rest of our asyncio stack).
* Empirically, sending an ``order_book/{mi}`` subscribe message over
  the direct WS causes the server to silently ignore *all* subscribes
  in that connection — including the valid market_stats ones. The
  "order_book" direct-WS channel either doesn't exist or uses a
  different name; either way it's not worth discovering at this stage.

Critical Lighter WS quirks — these are load-bearing, do not remove:

1.  **Application-layer ping/pong.** Lighter sends ``{"type": "ping"}``
    as JSON over the wire — NOT a WebSocket protocol ping frame. The
    client must reply with ``{"type": "pong"}`` JSON or the server
    silently drops the connection around the 120-second mark.

2.  **Client library config.** We MUST set
    ``websockets.connect(ping_interval=None)`` to disable the
    client-side protocol ping. Lighter responds badly to client
    protocol pings. Leave ``ping_timeout=20`` so the library still
    answers server protocol pings if any.

3.  **Subscription batching.** Subscription messages are sent in
    batches of 10 with a 100ms delay between batches. Sending many at
    once causes some subscribe messages to be silently dropped.

4.  **Reconnect on any exception.** The outer loop catches every
    exception class and reconnects with a clamped exponential backoff
    (1s → 60s). This matches the reference adapter's pattern.

5.  **market_stats for realtime prices.** ~13 updates/sec per market.
    Carries mark/index/last/funding_rate. The initial subscription
    confirmation (``subscribed/market_stats``) already carries a
    full payload, so the handler dispatches both ``subscribed/...``
    and ``update/...`` to the same parser.

6.  **Active keep-alive ping every 30s.** Passive pong-in-response-to-
    server-ping alone is not enough — the server drops the connection
    after ~117s (observed 14 disconnects in a 27-minute run). We send
    a client-initiated ``{"type":"ping"}`` every 30s to keep the
    connection marked alive. The server's ``{"type":"pong"}`` reply is
    tracked for diagnostics but isn't required for the keep-alive to
    work.

7.  **Trade tape is best-effort.** ``trade/{mi}`` subscription succeeds
    on every Lighter market we have observed, but a server-side
    rejection (``{"type":"error", "channel":"trade/N", ...}``) must
    not poison the connection. We log once per market and continue
    serving market_stats. ``get_recent_trades(mi)`` will return
    ``[]`` for markets where the channel is silent or rejected.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from decimal import Decimal
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple, Union

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:  # pragma: no cover
    WEBSOCKETS_AVAILABLE = False
    websockets = None  # type: ignore[assignment]
    ConnectionClosed = Exception  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)

_MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
_TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"

OrderbookCallback = Callable[[int, Dict[str, Any]], Union[None, Awaitable[None]]]
MarketStatsCallback = Callable[[int, Dict[str, Any]], Union[None, Awaitable[None]]]
TradeCallback = Callable[[int, Dict[str, Any]], Union[None, Awaitable[None]]]
# Account callback receives the entire raw msg dict (channel + type + payload).
# Lighter's ``account_all/<idx>`` channel pushes the full account snapshot
# (positions + orders + balances) on every update; downstream consumers
# (LighterOrderManager) parse the relevant slice themselves.
AccountCallback = Callable[[Dict[str, Any]], Union[None, Awaitable[None]]]

_SUBSCRIBE_BATCH_SIZE = 10
_SUBSCRIBE_BATCH_DELAY_SEC = 0.1
_RECONNECT_BACKOFF_CAP_SEC = 60.0
# Client-initiated keep-alive cadence. Lighter drops connections at
# ~117s without active pings from the client (passive pong response
# to server pings is insufficient). Send well under that window.
_KEEPALIVE_INTERVAL_SEC = 30.0
# How long to retain in-memory trade records for ad-hoc 60s windows.
# Five minutes gives the observer headroom for delayed snapshot ticks
# without unbounded growth on quiet markets.
_TRADE_RETENTION_MS = 5 * 60 * 1000
# Max samples for the rolling latency window (server_ts → local_ts).
_LATENCY_SAMPLES_MAXLEN = 200

# Active health-probe knobs (Phase 1.0.3 stability work). 15h runs
# observed 7 reconnects with non-linear cadence even though every
# ping/pong was paired — symptoms point at zombie sockets / CDN-side
# half-closes that only surface when we try to read. Defence in depth:
#
#   - stale-msg detector: if we go silent for too long, assume the
#     socket is dead and force-reconnect rather than waiting for the
#     server to drop us.
#   - lifetime cap: even on healthy connections, refresh after an
#     hour to prevent long-lived corruption (CDN sticky-session
#     drift, accumulated buffer state).
#   - keepalive failure budget: tolerate a few transient send errors
#     before forcing reconnect (one transient hiccup ≠ dead socket).
_STALE_CHECK_INTERVAL_SEC = 10.0
_MAX_MSG_GAP_SEC = 60.0
_MAX_CONNECTION_LIFETIME_SEC = 3600.0
_MAX_KEEPALIVE_FAIL_COUNT = 3


def _safe_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError):
        return default


class LighterWebSocket:
    """Direct WebSocket client for Lighter's /stream endpoint.

    Public surface:

    * ``await ws.connect(market_indices)`` — starts the background task
      and waits for the first successful connection.
    * ``await ws.disconnect()`` — signals the task to stop and waits
      for cleanup.
    * ``ws.get_orderbook_snapshot(market_index)`` — latest book.
    * ``ws.get_latest_mid(market_index)`` — latest mid from mark/index.
    * ``ws.register_orderbook_callback(cb)`` and
      ``register_market_stats_callback(cb)`` for event fan-out.
    * ``ws.get_message_stats()`` — counts and last-message timestamps
      per channel for latency / liveness diagnostics.
    """

    def __init__(
        self,
        ws_url: Optional[str] = None,
        testnet: bool = False,
    ):
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError(
                "websockets library not installed. pip install websockets>=12.0"
            )

        self.ws_url: str = ws_url or (_TESTNET_WS_URL if testnet else _MAINNET_WS_URL)

        self._market_indices: List[int] = []
        self._ws_task: Optional[asyncio.Task] = None
        self._direct_ws: Optional[Any] = None
        self._running: bool = False
        self._connected_event: Optional[asyncio.Event] = None

        self._orderbook_cache: Dict[int, Dict[str, Any]] = {}
        self._market_stats_cache: Dict[int, Dict[str, Any]] = {}
        self._latest_mid: Dict[int, Decimal] = {}

        self._orderbook_callbacks: List[OrderbookCallback] = []
        self._market_stats_callbacks: List[MarketStatsCallback] = []
        self._trade_callbacks: List[TradeCallback] = []
        self._account_callbacks: List[AccountCallback] = []
        # Account indices we've been asked to follow. Re-sent on every
        # reconnect so the order-manager keeps seeing fills across drops.
        self._subscribed_accounts: set = set()

        # Trade tape: per-market rolling list of recent trades. Each
        # element is a dict with the fields produced by ``_on_trade``.
        # Pruned to ``_TRADE_RETENTION_MS`` on every insert so memory
        # stays bounded even on quiet markets that we never query.
        self._recent_trades_by_market: Dict[int, List[Dict[str, Any]]] = {}
        # Markets where the trade subscription was rejected by the
        # server. We log once and degrade gracefully — observers must
        # treat trade data as best-effort.
        self._trade_subscription_failed: set = set()

        # Server→local latency samples for diagnostic p50/p95/p99.
        # Sample format: (server_ts_ms, local_ts_ms). Bounded deque so
        # the buffer cannot grow unbounded if no one ever drains it.
        self._latency_samples: Deque[Tuple[int, int]] = deque(
            maxlen=_LATENCY_SAMPLES_MAXLEN
        )

        self._msg_count_by_type: Dict[str, int] = defaultdict(int)
        self._last_msg_ts_ms_by_channel: Dict[str, int] = {}
        self._connect_count: int = 0
        self._last_connect_ts_ms: Optional[int] = None
        self._subscription_confirmations: set = set()

        # Keep-alive state (see quirk #6 in module docstring)
        self._keepalive_task: Optional[asyncio.Task] = None
        self._ping_sent: int = 0
        self._pong_received: int = 0
        self._last_ping_ts_ms: Optional[int] = None
        self._last_pong_ts_ms: Optional[int] = None

        # Active health-probe state (Phase 1.0.3). The stale_check
        # task watches msg-gap, connection lifetime, and keepalive
        # failure budget — closing the socket force-triggers the
        # reconnect path in _run_loop instead of waiting for a
        # server-side drop.
        self._stale_check_task: Optional[asyncio.Task] = None
        self._last_msg_ts_ms: Optional[int] = None
        self._connection_started_ts_ms: Optional[int] = None
        self._max_uptime_sec: float = 0.0
        self._last_disconnect_reason: Optional[str] = None
        self._keepalive_fail_count: int = 0

    # --------------------------------------------------------------
    # lifecycle
    # --------------------------------------------------------------

    async def connect(
        self,
        market_indices: List[int],
        first_connect_timeout_sec: float = 10.0,
    ) -> None:
        """Start the background WS task and wait for the first connect.

        Returns once the connection is established and initial
        subscriptions have been sent. Raises on connection timeout.
        """
        if self._ws_task is not None and not self._ws_task.done():
            raise RuntimeError("LighterWebSocket already connected; call disconnect() first")

        self._market_indices = list(dict.fromkeys(int(m) for m in market_indices))
        self._running = True
        self._connected_event = asyncio.Event()
        self._ws_task = asyncio.create_task(self._run_loop(), name="lighter-ws-loop")

        try:
            await asyncio.wait_for(
                self._connected_event.wait(), timeout=first_connect_timeout_sec
            )
        except asyncio.TimeoutError:
            self._running = False
            self._ws_task.cancel()
            raise RuntimeError(
                f"Lighter WebSocket did not connect within {first_connect_timeout_sec:.1f}s"
            )

    async def disconnect(self) -> None:
        """Stop the background task and close the connection."""
        self._running = False
        await self._stop_keepalive_task()
        await self._stop_stale_check_task()
        ws = self._direct_ws
        self._direct_ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing WebSocket: %s", exc)
        if self._ws_task is not None and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                logger.debug("ws task raised on shutdown: %s", exc)
        self._ws_task = None

    async def _stop_keepalive_task(self) -> None:
        """Idempotently cancel the keep-alive task if it is running."""
        task = self._keepalive_task
        self._keepalive_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("keep-alive task raised on cancel: %s", exc)

    async def _stop_stale_check_task(self) -> None:
        """Idempotently cancel the stale-check task if it is running."""
        task = self._stale_check_task
        self._stale_check_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("stale-check task raised on cancel: %s", exc)

    async def _keepalive_loop(self) -> None:
        """Send ``{"type":"ping"}`` every ~30s to prevent server timeout.

        Runs until cancelled. A single transient ``ws.send`` failure no
        longer terminates the task — Phase 1.0.3 long runs showed
        connections going zombie without throwing on send right away,
        so we keep trying and let ``_stale_check_loop`` decide when
        the failure budget is exhausted (or when message-gap or
        lifetime triggers fire).
        """
        try:
            while True:
                await asyncio.sleep(_KEEPALIVE_INTERVAL_SEC)
                ws = self._direct_ws
                if ws is None:
                    # Connection gone; reconnect will restart this task
                    return
                try:
                    await ws.send(json.dumps({"type": "ping"}))
                    self._ping_sent += 1
                    self._last_ping_ts_ms = int(time.time() * 1000)
                    self._keepalive_fail_count = 0  # reset budget on success
                    logger.debug("keep-alive ping sent (total=%d)", self._ping_sent)
                except Exception as exc:  # noqa: BLE001
                    self._keepalive_fail_count += 1
                    logger.warning(
                        "keep-alive ping send failed (count=%d/%d): %s",
                        self._keepalive_fail_count,
                        _MAX_KEEPALIVE_FAIL_COUNT,
                        exc,
                    )
                    # Bail only when the budget is exhausted; the
                    # stale-check task will close the socket shortly
                    # after and trigger the reconnect path.
                    if self._keepalive_fail_count >= _MAX_KEEPALIVE_FAIL_COUNT:
                        return
        except asyncio.CancelledError:
            raise

    async def _stale_check_loop(self) -> None:
        """Force a reconnect when the connection looks unhealthy.

        Three triggers, all of which set ``_last_disconnect_reason``
        and call ``ws.close()``. The clean local close lets the main
        ``async for message in ws`` exit without an exception, so
        ``_run_loop`` reconnects immediately (no exponential backoff
        applied to a planned refresh).

        1. **stale**: no inbound message for ``_MAX_MSG_GAP_SEC`` —
           the socket is probably dead even if the OS hasn't told us
           yet. market_stats normally pushes ~13 msgs/sec per
           subscribed market, so a 60s gap is unambiguously bad.
        2. **lifetime**: connection has been alive for
           ``_MAX_CONNECTION_LIFETIME_SEC``. Forces periodic refresh
           so accumulated buffer/CDN-state corruption can't compound.
        3. **keepalive_fail**: the ping task burned through its
           failure budget. Caps how long we tolerate a half-open
           socket that's silently dropping our sends.
        """
        try:
            while True:
                await asyncio.sleep(_STALE_CHECK_INTERVAL_SEC)
                ws = self._direct_ws
                if ws is None:
                    return
                now_ms = int(time.time() * 1000)

                if self._last_msg_ts_ms is not None:
                    gap_sec = (now_ms - self._last_msg_ts_ms) / 1000.0
                    if gap_sec > _MAX_MSG_GAP_SEC:
                        logger.warning(
                            "WS stale: %.1fs since last msg (>%.0fs); forcing reconnect",
                            gap_sec,
                            _MAX_MSG_GAP_SEC,
                        )
                        self._last_disconnect_reason = "stale"
                        try:
                            await ws.close()
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("ws.close() raised during stale refresh: %s", exc)
                        return

                if self._connection_started_ts_ms is not None:
                    uptime_sec = (now_ms - self._connection_started_ts_ms) / 1000.0
                    if uptime_sec > _MAX_CONNECTION_LIFETIME_SEC:
                        logger.info(
                            "WS connection lifetime %.0fs reached (>%.0fs); scheduled refresh",
                            uptime_sec,
                            _MAX_CONNECTION_LIFETIME_SEC,
                        )
                        self._last_disconnect_reason = "lifetime"
                        try:
                            await ws.close()
                        except Exception as exc:  # noqa: BLE001
                            logger.debug("ws.close() raised during lifetime refresh: %s", exc)
                        return

                if self._keepalive_fail_count >= _MAX_KEEPALIVE_FAIL_COUNT:
                    logger.warning(
                        "WS keep-alive failed %d times; forcing reconnect",
                        self._keepalive_fail_count,
                    )
                    self._last_disconnect_reason = "keepalive_fail"
                    try:
                        await ws.close()
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("ws.close() raised during keepalive_fail refresh: %s", exc)
                    return
        except asyncio.CancelledError:
            raise

    async def add_markets(self, market_indices: List[int]) -> None:
        """Add markets to the subscription set while connection is live."""
        new_indices = [
            int(m) for m in market_indices if int(m) not in self._market_indices
        ]
        if not new_indices:
            return
        self._market_indices.extend(new_indices)
        if self._direct_ws is not None:
            await self._send_subscriptions(new_indices)

    # --------------------------------------------------------------
    # accessors
    # --------------------------------------------------------------

    def get_orderbook_snapshot(self, market_index: int) -> Optional[Dict[str, Any]]:
        snap = self._orderbook_cache.get(int(market_index))
        return dict(snap) if snap else None

    def get_latest_mid(self, market_index: int) -> Optional[Decimal]:
        return self._latest_mid.get(int(market_index))

    def get_market_stats(self, market_index: int) -> Optional[Dict[str, Any]]:
        data = self._market_stats_cache.get(int(market_index))
        return dict(data) if data else None

    def register_orderbook_callback(self, cb: OrderbookCallback) -> None:
        self._orderbook_callbacks.append(cb)

    def register_market_stats_callback(self, cb: MarketStatsCallback) -> None:
        self._market_stats_callbacks.append(cb)

    def register_trade_callback(self, cb: TradeCallback) -> None:
        self._trade_callbacks.append(cb)

    def register_account_callback(self, cb: AccountCallback) -> None:
        """Register a callback to receive ``account_all/<idx>`` ws msgs.

        Multiple callbacks are supported. The callback receives the
        full ws message dict (with ``type`` / ``channel`` / payload);
        consumers like ``LighterOrderManager.on_account_event`` walk
        the payload themselves.
        """
        self._account_callbacks.append(cb)

    async def subscribe_account(self, account_index: int) -> None:
        """Subscribe to ``account_all/<account_index>`` push updates.

        Idempotent: a repeated call for the same index is a no-op
        beyond keeping the index in ``_subscribed_accounts`` (which
        ``_run_loop`` re-sends on every reconnect). If the WS isn't
        live yet, the index is just remembered — the next connect
        will replay it.
        """
        idx = int(account_index)
        if idx in self._subscribed_accounts:
            return
        self._subscribed_accounts.add(idx)
        if self._direct_ws is None:
            return
        try:
            await self._direct_ws.send(
                json.dumps({"type": "subscribe", "channel": f"account_all/{idx}"})
            )
            logger.info("Subscribed to account_all/%d", idx)
        except Exception as exc:  # noqa: BLE001
            logger.warning("subscribe_account send failed (idx=%d): %s", idx, exc)

    def get_recent_trades(
        self,
        market_index: int,
        since_ts_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return trades for ``market_index`` with ``ts_ms >= since_ts_ms``.

        ``since_ts_ms`` defaults to "everything currently retained". The
        returned list is a copy — callers can sort/aggregate freely
        without affecting the rolling buffer.
        """
        trades = self._recent_trades_by_market.get(int(market_index))
        if not trades:
            return []
        if since_ts_ms is None:
            return list(trades)
        cutoff = int(since_ts_ms)
        return [t for t in trades if t.get("ts_ms", 0) >= cutoff]

    def get_recent_latency_stats(self) -> Optional[Dict[str, Any]]:
        """Compute p50/p95/p99 latency over the rolling sample window.

        Returns ``None`` when there are no samples (e.g. server never
        included a server-side timestamp), which the observer treats
        as "omit the field".
        """
        samples = list(self._latency_samples)
        if not samples:
            return None
        deltas = sorted((local - server) for server, local in samples)
        n = len(deltas)
        if n == 0:
            return None

        def _q(p: float) -> int:
            # Nearest-rank percentile. Cheap and stable for small n.
            idx = max(0, min(n - 1, int(round(p * (n - 1)))))
            return int(deltas[idx])

        return {
            "p50_ms": _q(0.50),
            "p95_ms": _q(0.95),
            "p99_ms": _q(0.99),
            "samples": n,
        }

    def get_message_stats(self) -> Dict[str, Any]:
        if self._connection_started_ts_ms is not None:
            uptime_now_sec = (
                int(time.time() * 1000) - self._connection_started_ts_ms
            ) / 1000.0
        else:
            uptime_now_sec = 0.0
        return {
            "msg_count_by_type": dict(self._msg_count_by_type),
            "last_msg_ts_ms_by_channel": dict(self._last_msg_ts_ms_by_channel),
            "connect_count": self._connect_count,
            "last_connect_ts_ms": self._last_connect_ts_ms,
            "subscribed_markets": list(self._market_indices),
            "subscription_confirmations": list(self._subscription_confirmations),
            "ping_sent": self._ping_sent,
            "pong_received": self._pong_received,
            "last_ping_ts_ms": self._last_ping_ts_ms,
            "last_pong_ts_ms": self._last_pong_ts_ms,
            "trade_subscription_failed": sorted(self._trade_subscription_failed),
            "last_msg_ts_ms_global": self._last_msg_ts_ms,
            "max_uptime_sec": self._max_uptime_sec,
            "connection_uptime_sec_current": uptime_now_sec,
            "last_disconnect_reason": self._last_disconnect_reason,
            "keepalive_fail_count": self._keepalive_fail_count,
        }

    def is_connected(self) -> bool:
        return self._direct_ws is not None and self._running

    # --------------------------------------------------------------
    # background loop
    # --------------------------------------------------------------

    async def _run_loop(self) -> None:
        retry_count = 0
        while self._running:
            # Reconnect-attempt diagnostic. Skipped on the very first
            # attempt (no prior session yet); from the second connect
            # onwards we log the *reason* the previous one ended and
            # the prior uptime so a tail can correlate stalls / drops.
            if self._connect_count > 0:
                logger.info(
                    "ws reconnecting: attempt=%d last_disconnect_reason=%s "
                    "prev_uptime=%.1fs",
                    self._connect_count + 1,
                    self._last_disconnect_reason,
                    self._max_uptime_sec,
                )
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    self._direct_ws = ws
                    self._connect_count += 1
                    now_ms = int(time.time() * 1000)
                    self._last_connect_ts_ms = now_ms
                    self._connection_started_ts_ms = now_ms
                    # Seed the stale-check baseline so the first 60s of
                    # silence after a fresh connect doesn't trip the
                    # detector (it normally takes <1s to get the first
                    # market_stats subscribed snapshot).
                    self._last_msg_ts_ms = now_ms
                    self._keepalive_fail_count = 0
                    logger.info(
                        "Lighter WS connected (attempt %d, url=%s)",
                        self._connect_count,
                        self.ws_url,
                    )
                    # Subscribe in batches
                    await self._send_subscriptions(self._market_indices)
                    # Re-send any account subscriptions the caller asked
                    # for before the (re)connect — keeps order-manager
                    # fed across drops without the caller re-driving it.
                    if self._subscribed_accounts:
                        await self._send_account_subscriptions()

                    # Start (or restart) the client-initiated keep-alive
                    # task so we stay well under the ~117s server timeout.
                    # Any previous task from an earlier connection should
                    # already be done at this point, but stop defensively.
                    await self._stop_keepalive_task()
                    await self._stop_stale_check_task()
                    self._keepalive_task = asyncio.create_task(
                        self._keepalive_loop(), name="lighter-ws-keepalive"
                    )
                    self._stale_check_task = asyncio.create_task(
                        self._stale_check_loop(), name="lighter-ws-stale-check"
                    )

                    # mark connected so connect() can return
                    if self._connected_event is not None and not self._connected_event.is_set():
                        self._connected_event.set()

                    retry_count = 0  # reset backoff after a stable connect

                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            logger.warning("Dropping non-JSON WS message")
                            continue
                        try:
                            await self._handle_message(data)
                        except Exception as exc:  # noqa: BLE001
                            # one bad message must not kill the loop
                            logger.error("WS message handler error: %s", exc, exc_info=True)

            except asyncio.CancelledError:
                raise
            except ConnectionClosed as exc:
                retry_count += 1
                delay = min(retry_count, _RECONNECT_BACKOFF_CAP_SEC)
                self._last_disconnect_reason = "exception"
                logger.warning(
                    "Lighter WS closed (%s); reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    retry_count,
                )
                self._direct_ws = None
                await self._stop_keepalive_task()
                await self._stop_stale_check_task()
                await asyncio.sleep(delay)
            except Exception as exc:  # noqa: BLE001
                retry_count += 1
                delay = min(retry_count, _RECONNECT_BACKOFF_CAP_SEC)
                self._last_disconnect_reason = "exception"
                logger.error(
                    "Lighter WS error: %s; reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    retry_count,
                    exc_info=True,
                )
                self._direct_ws = None
                await self._stop_keepalive_task()
                await self._stop_stale_check_task()
                await asyncio.sleep(delay)
            finally:
                # Capture max-uptime BEFORE clearing the start timestamp
                # so each connection contributes once to the running max.
                if self._connection_started_ts_ms is not None:
                    uptime_sec = (
                        int(time.time() * 1000) - self._connection_started_ts_ms
                    ) / 1000.0
                    if uptime_sec > self._max_uptime_sec:
                        self._max_uptime_sec = uptime_sec
                self._connection_started_ts_ms = None
                self._direct_ws = None
                # Defensive: if the async-with exited cleanly (server
                # closed the iterator), the except blocks above don't
                # fire, so stop the keep-alive here too.
                await self._stop_keepalive_task()
                await self._stop_stale_check_task()

        logger.info("Lighter WS background loop exited cleanly")

    async def _send_subscriptions(self, market_indices: List[int]) -> None:
        """Subscribe to ``market_stats`` and ``trade`` per market.

        Sends two subscribe messages per market. Server batching limit
        is per-message, so the unit of batching is messages (not
        markets) — sleep every ``_SUBSCRIBE_BATCH_SIZE`` messages.

        Order-book depth is intentionally NOT subscribed here — see
        the module docstring for why. Depth comes from REST
        (``LighterGateway.get_orderbook``).

        Trade channel is best-effort: if the server replies with an
        ``error`` for ``trade/{mi}``, the market is added to
        ``_trade_subscription_failed`` and observer code treats that
        market's trade-tape as empty. ``market_stats`` is unaffected.
        """
        if self._direct_ws is None:
            logger.warning("No WS connection — skipping subscription send")
            return
        total = len(market_indices)
        if total == 0:
            return

        # Build the flat list of subscribe messages first so the per-
        # message batching is easy to reason about.
        messages: List[Tuple[int, str, Dict[str, Any]]] = []
        for mi in market_indices:
            messages.append((
                mi,
                "market_stats",
                {"type": "subscribe", "channel": f"market_stats/{mi}"},
            ))
            messages.append((
                mi,
                "trade",
                {"type": "subscribe", "channel": f"trade/{mi}"},
            ))

        logger.info(
            "Sending subscriptions for %d markets (%d messages)",
            total,
            len(messages),
        )
        sent_ok = 0
        sent_fail = 0
        for i, (mi, kind, msg) in enumerate(messages):
            try:
                await self._direct_ws.send(json.dumps(msg))
                sent_ok += 1
            except Exception as exc:  # noqa: BLE001
                sent_fail += 1
                logger.warning(
                    "Subscribe send failed (kind=%s market_index=%s): %s",
                    kind,
                    mi,
                    exc,
                )
            # Pause every batch to avoid the silent-drop quirk. Use
            # 1-based count so the first sleep happens after exactly
            # _SUBSCRIBE_BATCH_SIZE sends.
            if (i + 1) % _SUBSCRIBE_BATCH_SIZE == 0 and (i + 1) < len(messages):
                await asyncio.sleep(_SUBSCRIBE_BATCH_DELAY_SEC)
        logger.info("Subscriptions sent: ok=%d fail=%d", sent_ok, sent_fail)

    async def _send_account_subscriptions(self) -> None:
        """Re-issue every ``account_all/<idx>`` subscribe on the live socket.

        Called after each (re)connect so the order-manager keeps
        receiving fills/cancellations even after a transient drop.
        """
        if self._direct_ws is None:
            return
        for idx in sorted(self._subscribed_accounts):
            try:
                await self._direct_ws.send(
                    json.dumps({"type": "subscribe", "channel": f"account_all/{idx}"})
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "account_all subscribe re-send failed (idx=%d): %s", idx, exc
                )

    # --------------------------------------------------------------
    # message routing
    # --------------------------------------------------------------

    async def _handle_message(self, data: Dict[str, Any]) -> None:
        msg_type = data.get("type", "")
        self._msg_count_by_type[msg_type] = self._msg_count_by_type.get(msg_type, 0) + 1
        channel = data.get("channel", "")
        local_ts_ms = int(time.time() * 1000)
        # Tracks ANY inbound msg (even ping/pong/error) so the
        # stale detector measures liveness, not just data-channel
        # activity. Update before any return paths below.
        self._last_msg_ts_ms = local_ts_ms
        if channel:
            self._last_msg_ts_ms_by_channel[channel] = local_ts_ms

        # Best-effort server timestamp capture for latency stats. Lighter
        # is inconsistent here: market_stats payloads usually carry
        # `transaction_time` or `timestamp`; trades carry `timestamp` or
        # `transaction_time`. We sample only update/* frames so the
        # subscribe-snapshot bursts don't pollute the distribution.
        if msg_type.startswith("update/"):
            self._maybe_record_latency(data, local_ts_ms)

        # Application-layer heartbeat — load-bearing. Without this the
        # server drops us at ~120s silently.
        if msg_type == "ping":
            if self._direct_ws is not None:
                try:
                    await self._direct_ws.send(json.dumps({"type": "pong"}))
                except Exception as exc:  # noqa: BLE001
                    logger.debug("pong send failed: %s", exc)
            return

        # Server's reply to our keep-alive ping (quirk #6). Track for
        # diagnostics; there's no action required — as long as the
        # keep-alive task keeps sending pings, the server keeps the
        # connection open whether or not the pong arrives back to us.
        if msg_type == "pong":
            self._pong_received += 1
            self._last_pong_ts_ms = int(time.time() * 1000)
            logger.debug("keep-alive pong received (total=%d)", self._pong_received)
            return

        # market_stats: both ``subscribed/market_stats`` (carries initial
        # snapshot) and ``update/market_stats`` (deltas) go to the same
        # parser. Check this BEFORE the generic subscribed/ branch below,
        # otherwise the initial snapshot is dropped on the floor.
        if msg_type in ("subscribed/market_stats", "update/market_stats") and "market_stats" in data:
            self._subscription_confirmations.add(channel or msg_type)
            await self._on_market_stats(data["market_stats"])
            return

        # trade channel: both subscribed/trade (initial backfill, may be
        # an empty list) and update/trade (live ticks) carry trades
        # under "trades" and/or "liquidation_trades". The subscribed/
        # form confirms the channel exists; the update/ form delivers
        # data. See _on_trade for wire-format details.
        if msg_type in ("subscribed/trade", "update/trade"):
            self._subscription_confirmations.add(channel or msg_type)
            await self._on_trade(data, channel)
            return

        # account_all: full account snapshot (positions + orders + balances).
        # We forward the raw msg dict to every registered AccountCallback
        # so downstream parsers (LighterOrderManager.on_account_event) can
        # walk it without us second-guessing the wire shape.
        if msg_type in ("subscribed/account_all", "update/account_all"):
            self._subscription_confirmations.add(channel or msg_type)
            await self._on_account_update(data)
            return

        # Server may emit an "error" for unknown channels. We treat
        # the trade channel specifically as best-effort: log once and
        # continue so market_stats keeps working.
        if msg_type == "error":
            err_msg = str(data.get("message") or data.get("error") or "")
            ch = data.get("channel") or ""
            if ch.startswith("trade/"):
                try:
                    mi = int(ch.split("/", 1)[1])
                except (ValueError, IndexError):
                    mi = -1
                if mi >= 0 and mi not in self._trade_subscription_failed:
                    self._trade_subscription_failed.add(mi)
                    logger.warning(
                        "Trade channel rejected by server for market_index=%s "
                        "(message=%r); continuing without trade tape.",
                        mi,
                        err_msg,
                    )
                return
            logger.warning("WS server error: channel=%s message=%r", ch, err_msg)
            return

        if msg_type.startswith("subscribed/"):
            self._subscription_confirmations.add(channel or msg_type)
            logger.debug("Subscribed: %s", channel or msg_type)
            return

        # order_book channel is intentionally not subscribed on this WS
        # (see module docstring). Depth comes from REST. We keep the
        # dispatch guard here as a no-op in case a future protocol bump
        # ever starts pushing ``update/order_book`` unsolicited — right
        # now it just falls through to the "unhandled" debug line.

        # Unknown message — log at debug only to keep noise low
        logger.debug("Unhandled WS msg_type=%s channel=%s", msg_type, channel)

    async def _on_account_update(self, data: Dict[str, Any]) -> None:
        """Fan out an ``account_all`` message to every registered callback.

        Callbacks see the full raw dict — payload shape is left to the
        consumer (e.g. LighterOrderManager) so wire-format drift only
        breaks the consumer's parser, not this dispatcher. Each
        callback runs in its own try/except so one buggy subscriber
        can't poison the others.
        """
        for cb in self._account_callbacks:
            try:
                result = cb(data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.warning("account_all callback raised: %s", exc)

    async def _on_market_stats(self, stats: Dict[str, Any]) -> None:
        market_id = stats.get("market_id")
        if market_id is None:
            return
        try:
            market_index = int(market_id)
        except (ValueError, TypeError):
            return

        mark = _safe_decimal(stats.get("mark_price"))
        index_px = _safe_decimal(stats.get("index_price"))
        last = _safe_decimal(stats.get("last_trade_price"))
        # mid is best-effort: prefer mark, then index, then last
        mid: Optional[Decimal] = mark or index_px or last
        if mid is not None:
            self._latest_mid[market_index] = mid

        normalized = {
            "market_index": market_index,
            "last_trade_price": last,
            "mark_price": mark,
            "index_price": index_px,
            "daily_price_high": _safe_decimal(stats.get("daily_price_high")),
            "daily_price_low": _safe_decimal(stats.get("daily_price_low")),
            "daily_base_token_volume": _safe_decimal(
                stats.get("daily_base_token_volume")
            ),
            "current_funding_rate": _safe_decimal(
                stats.get("current_funding_rate")
            ),
            "open_interest": _safe_decimal(stats.get("open_interest")),
            "recv_ts_ms": int(time.time() * 1000),
            "raw": stats,
        }
        self._market_stats_cache[market_index] = normalized

        for cb in self._market_stats_callbacks:
            try:
                result = cb(market_index, normalized)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.warning("market_stats callback raised: %s", exc)

    async def _on_trade(
        self,
        data: Dict[str, Any],
        channel: str,
    ) -> None:
        """Parse a trade-channel payload into the per-market tape.

        Wire format (observed live, Apr 2026)::

            {
              "channel": "trade:1",            # NOTE colon, not slash
              "type": "update/trade",
              "nonce": ...,
              "trades": [{...trade...}, ...],          # may be missing
              "liquidation_trades": [{...trade...}, ...]  # may be missing/null
            }

        Two important shape facts the previous parser got wrong:

        1. Lighter's *trade* channel string uses a colon
           (``trade:1``) where market_stats uses a slash
           (``market_stats/1``). Splitting on ``/`` only left
           ``mi=None`` and silently dropped every message.
        2. The top-level frame has no ``market_id`` field — only the
           channel string carries it. Each trade *item* has its own
           ``market_id``, but we cannot rely on the array being
           non-empty.
        3. The frame may carry ``trades``, ``liquidation_trades``, or
           both. We process whichever are present so liquidations
           don't get dropped from the tape.

        Per-trade keys (from the SDK ``Trade`` model — confirmed live):

        * ``size`` / ``price`` / ``usd_amount`` — string Decimal-safe
        * ``timestamp`` — int ms (13 digits)
        * ``is_maker_ask`` — bool. ``True`` ⇒ taker bought (lifted
          ask). ``False`` ⇒ taker sold (hit bid).
        * ``type`` — "trade" / "liquidation" / "deleverage"

        Defensive: if the payload shape diverges from expectations we
        skip individual entries rather than failing the message.
        """
        # Resolve market_index. The trade frame's only consistent
        # carrier is the channel string; top-level market_id is not
        # populated. Fall back to a per-trade item if needed.
        mi: Optional[int] = None
        for key in ("market_id", "market_index"):
            if key in data:
                try:
                    mi = int(data[key])
                    break
                except (TypeError, ValueError):
                    pass
        if mi is None and channel:
            # Lighter uses ":" for trade channels and "/" for
            # market_stats. Normalize both before splitting on the
            # final separator.
            tail = channel.replace(":", "/").rsplit("/", 1)[-1]
            try:
                mi = int(tail)
            except (ValueError, TypeError):
                mi = None

        trades_raw = data.get("trades")
        liq_trades_raw = data.get("liquidation_trades")
        if not isinstance(trades_raw, list):
            trades_raw = []
        if not isinstance(liq_trades_raw, list):
            liq_trades_raw = []
        # Empty subscribed-snapshot or genuine no-data frame — nothing
        # to do, but not an error.
        if not trades_raw and not liq_trades_raw:
            return
        if mi is None:
            return

        bucket = self._recent_trades_by_market.setdefault(mi, [])
        cutoff_ms = int(time.time() * 1000) - _TRADE_RETENTION_MS

        new_records: List[Dict[str, Any]] = []
        for t in trades_raw:
            rec = _normalize_trade(t, mi)
            if rec is None:
                continue
            new_records.append(rec)
        for t in liq_trades_raw:
            rec = _normalize_trade(t, mi)
            if rec is None:
                continue
            new_records.append(rec)

        if new_records:
            bucket.extend(new_records)

        # Prune in-place: anything older than retention window goes.
        # Lighter delivers in mostly-sorted order; we still scan
        # because liquidations and replays can arrive out-of-order.
        if bucket and bucket[0].get("ts_ms", 0) < cutoff_ms:
            self._recent_trades_by_market[mi] = [
                r for r in bucket if r.get("ts_ms", 0) >= cutoff_ms
            ]

        # Fan out new records (not the whole tape) to subscribers.
        for cb in self._trade_callbacks:
            for rec in new_records:
                try:
                    result = cb(mi, rec)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:  # noqa: BLE001
                    logger.warning("trade callback raised: %s", exc)

    def _maybe_record_latency(
        self,
        data: Dict[str, Any],
        local_ts_ms: int,
    ) -> None:
        """Sample server→local latency from any ``update/*`` payload.

        Lighter timestamps are inconsistent across channels — we look
        in a few well-known locations and skip the message if no
        plausible timestamp is present. Server values that look like
        seconds (10-digit) are scaled up to ms.
        """
        candidate: Any = None
        for key in ("server_ts_ms", "server_ts", "timestamp", "transaction_time"):
            if key in data:
                candidate = data[key]
                break
        if candidate is None:
            # Sometimes the timestamp is nested inside the payload
            # body (e.g. market_stats.timestamp).
            for body_key in ("market_stats", "trade", "trades"):
                body = data.get(body_key)
                if isinstance(body, dict):
                    for key in ("timestamp", "transaction_time"):
                        if key in body:
                            candidate = body[key]
                            break
                if candidate is not None:
                    break
        if candidate is None:
            return
        try:
            server_ts = int(candidate)
        except (TypeError, ValueError):
            return
        # Coerce sec → ms when value looks too small to be a 2024+ ms
        # epoch (≈ 1.7e12). Anything below 1e12 is almost certainly
        # seconds; below 1e9 we ignore as garbage.
        if server_ts < 1_000_000_000:
            return
        if server_ts < 1_000_000_000_000:
            server_ts *= 1000
        # Reject obvious clock-skew or future timestamps (>30s in the
        # future) so they don't pollute the percentile distribution.
        if server_ts - local_ts_ms > 30_000:
            return
        self._latency_samples.append((server_ts, local_ts_ms))

    async def _on_order_book(self, ob: Any, channel: str) -> None:
        """Parse an order_book payload and fan out to callbacks."""
        if not isinstance(ob, dict):
            return
        market_id = ob.get("market_id")
        if market_id is None and channel:
            # Channel format: "order_book/{market_index}"
            try:
                tail = channel.rsplit("/", 1)[-1]
                market_id = int(tail)
            except (ValueError, IndexError):
                market_id = None
        if market_id is None:
            return
        try:
            market_index = int(market_id)
        except (ValueError, TypeError):
            return

        bids_raw = ob.get("bids") or []
        asks_raw = ob.get("asks") or []
        bids = _parse_levels(bids_raw)
        asks = _parse_levels(asks_raw)

        snapshot = {
            "market_index": market_index,
            "bids": bids,
            "asks": asks,
            "recv_ts_ms": int(time.time() * 1000),
        }
        # Best-bid / best-ask mid — nicer than mark for observer output
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            if best_bid and best_ask:
                snapshot["best_bid"] = best_bid
                snapshot["best_ask"] = best_ask
                snapshot["mid"] = (best_bid + best_ask) / Decimal(2)
                # Override the mark-based mid with true BBO mid if we have it
                self._latest_mid[market_index] = snapshot["mid"]
        self._orderbook_cache[market_index] = snapshot

        for cb in self._orderbook_callbacks:
            try:
                result = cb(market_index, snapshot)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                logger.warning("orderbook callback raised: %s", exc)


def _normalize_trade(
    raw: Any,
    market_index: int,
) -> Optional[Dict[str, Any]]:
    """Best-effort normalize a single Lighter trade record.

    Returns ``None`` if the record is missing essentials (price/size).
    Side detection uses ``is_maker_ask`` per the SDK ``Trade`` model.
    Falls back to ``side`` / ``taker_side`` if present (defensive
    against payload shape drift).
    """
    if not isinstance(raw, dict):
        return None
    price = _safe_decimal(raw.get("price"))
    size = _safe_decimal(raw.get("size") or raw.get("base_amount"))
    if price is None or size is None or price <= 0 or size <= 0:
        return None

    usd = _safe_decimal(raw.get("usd_amount"))
    if usd is None:
        usd = price * size

    # ts_ms — Lighter inconsistencies observed in live frames:
    #   * ``timestamp`` is ms (13 digits, e.g. 1777347267381)
    #   * ``transaction_time`` is µs (16 digits, e.g. 1777347267401066)
    # Prefer ``timestamp``; if we end up with anything looking like
    # µs or ns, scale down to ms.
    ts_raw: Any = (
        raw.get("timestamp")
        or raw.get("transaction_time")
        or raw.get("ts_ms")
    )
    try:
        ts = int(ts_raw) if ts_raw is not None else int(time.time() * 1000)
    except (TypeError, ValueError):
        ts = int(time.time() * 1000)
    if 0 < ts < 1_000_000_000_000:
        # Looks like seconds → scale to ms.
        ts *= 1000
    elif ts >= 1_000_000_000_000_000:
        # ≥ 1e15: at least microseconds. Drop precision down to ms.
        ts //= 1000
        if ts >= 1_000_000_000_000_000:
            # Was originally nanoseconds; one more /1000.
            ts //= 1000

    # Side: True ⇒ taker bought (lifted the ask).
    side: Optional[str] = None
    if "is_maker_ask" in raw:
        side = "buy" if bool(raw["is_maker_ask"]) else "sell"
    elif raw.get("side") in ("buy", "sell"):
        side = raw["side"]
    elif raw.get("taker_side") in ("buy", "sell"):
        side = raw["taker_side"]

    return {
        "market_index": int(raw.get("market_id", market_index)),
        "ts_ms": ts,
        "price": price,
        "size": size,
        "usd_amount": usd,
        "side": side,
        "type": str(raw.get("type", "trade")),
        "trade_id": raw.get("trade_id"),
    }


def _parse_levels(levels: Any) -> List[Tuple[Decimal, Decimal]]:
    out: List[Tuple[Decimal, Decimal]] = []
    if not isinstance(levels, list):
        return out
    for lvl in levels:
        price: Optional[Decimal]
        size: Optional[Decimal]
        if isinstance(lvl, dict):
            price = _safe_decimal(lvl.get("price"))
            size = _safe_decimal(
                lvl.get("size") or lvl.get("remaining_base_amount") or lvl.get("amount")
            )
        elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            price = _safe_decimal(lvl[0])
            size = _safe_decimal(lvl[1])
        else:
            continue
        if price is None or size is None:
            continue
        if price <= 0 or size <= 0:
            continue
        out.append((price, size))
    return out
