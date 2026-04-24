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
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

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

_SUBSCRIBE_BATCH_SIZE = 10
_SUBSCRIBE_BATCH_DELAY_SEC = 0.1
_RECONNECT_BACKOFF_CAP_SEC = 60.0
# Client-initiated keep-alive cadence. Lighter drops connections at
# ~117s without active pings from the client (passive pong response
# to server pings is insufficient). Send well under that window.
_KEEPALIVE_INTERVAL_SEC = 30.0


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

    async def _keepalive_loop(self) -> None:
        """Send ``{"type":"ping"}`` every ~30s to prevent server timeout.

        Runs until cancelled. Send failures are silent — if the WS
        connection has closed, ``_run_loop`` will detect it through
        its ``async for message`` iterator and reconnect. We don't
        want this task to raise and fight the reconnect path.
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
                    logger.debug("keep-alive ping sent (total=%d)", self._ping_sent)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "keep-alive ping send failed (will be handled by reconnect): %s",
                        exc,
                    )
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

    def get_message_stats(self) -> Dict[str, Any]:
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
        }

    def is_connected(self) -> bool:
        return self._direct_ws is not None and self._running

    # --------------------------------------------------------------
    # background loop
    # --------------------------------------------------------------

    async def _run_loop(self) -> None:
        retry_count = 0
        while self._running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    self._direct_ws = ws
                    self._connect_count += 1
                    self._last_connect_ts_ms = int(time.time() * 1000)
                    logger.info(
                        "Lighter WS connected (attempt %d, url=%s)",
                        self._connect_count,
                        self.ws_url,
                    )
                    # Subscribe in batches
                    await self._send_subscriptions(self._market_indices)

                    # Start (or restart) the client-initiated keep-alive
                    # task so we stay well under the ~117s server timeout.
                    # Any previous task from an earlier connection should
                    # already be done at this point, but stop defensively.
                    await self._stop_keepalive_task()
                    self._keepalive_task = asyncio.create_task(
                        self._keepalive_loop(), name="lighter-ws-keepalive"
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
                logger.warning(
                    "Lighter WS closed (%s); reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    retry_count,
                )
                self._direct_ws = None
                await self._stop_keepalive_task()
                await asyncio.sleep(delay)
            except Exception as exc:  # noqa: BLE001
                retry_count += 1
                delay = min(retry_count, _RECONNECT_BACKOFF_CAP_SEC)
                logger.error(
                    "Lighter WS error: %s; reconnecting in %.1fs (attempt %d)",
                    exc,
                    delay,
                    retry_count,
                    exc_info=True,
                )
                self._direct_ws = None
                await self._stop_keepalive_task()
                await asyncio.sleep(delay)
            finally:
                self._direct_ws = None
                # Defensive: if the async-with exited cleanly (server
                # closed the iterator), the except blocks above don't
                # fire, so stop the keep-alive here too.
                await self._stop_keepalive_task()

        logger.info("Lighter WS background loop exited cleanly")

    async def _send_subscriptions(self, market_indices: List[int]) -> None:
        """Send ``market_stats/{mi}`` subs in batches of 10 with a 100ms gap.

        Order-book depth is intentionally NOT subscribed here — see
        the module docstring for why. Depth comes from REST
        (``LighterGateway.get_orderbook``).
        """
        if self._direct_ws is None:
            logger.warning("No WS connection — skipping subscription send")
            return
        total = len(market_indices)
        if total == 0:
            return

        logger.info("Sending market_stats subscriptions for %d markets", total)
        sent_ok = 0
        sent_fail = 0
        for i in range(0, total, _SUBSCRIBE_BATCH_SIZE):
            batch = market_indices[i : i + _SUBSCRIBE_BATCH_SIZE]
            for market_index in batch:
                msg_stats = {
                    "type": "subscribe",
                    "channel": f"market_stats/{market_index}",
                }
                try:
                    await self._direct_ws.send(json.dumps(msg_stats))
                    sent_ok += 1
                except Exception as exc:  # noqa: BLE001
                    sent_fail += 1
                    logger.warning(
                        "Subscribe send failed (market_index=%s): %s",
                        market_index,
                        exc,
                    )
            if i + _SUBSCRIBE_BATCH_SIZE < total:
                await asyncio.sleep(_SUBSCRIBE_BATCH_DELAY_SEC)
        logger.info("Subscriptions sent: ok=%d fail=%d", sent_ok, sent_fail)

    # --------------------------------------------------------------
    # message routing
    # --------------------------------------------------------------

    async def _handle_message(self, data: Dict[str, Any]) -> None:
        msg_type = data.get("type", "")
        self._msg_count_by_type[msg_type] = self._msg_count_by_type.get(msg_type, 0) + 1
        channel = data.get("channel", "")
        if channel:
            self._last_msg_ts_ms_by_channel[channel] = int(time.time() * 1000)

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
