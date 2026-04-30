"""Async order manager for Lighter perp.

Phase 1.1 batch 3 part 1. Translates ``Quote`` records into actual
``SignerClient.create_order`` calls and tracks every order through
the lifecycle below::

         submit_order()
              │
              ▼
        pending_send ──────► rejected   (SDK send_tx error after retry)
              │
              ▼
        pending_ack
              │  ws status: "in-progress" / "pending"  (still here)
              │  ws status: "open"                     (advance to live)
              ▼
            live  ◄─────┐
              │         │  (more partial fills)
              ▼         │
         partial_fill   │
              │         │
              ├─────────┘
              ▼
            filled                        (terminal — full fill)
              │
              ├─► cancelled               (ws "canceled" / "canceled-liquidation"
              │                             / "canceled-oco" / "canceled-child")
              ├─► rejected                (ws "canceled-post-only" /
              │                             "canceled-margin-not-allowed" /
              │                             "canceled-too-much-slippage" / etc.)
              └─► expired                 (ws "canceled-expired")

Scope
-----
*Does* — submit_order, cancel_order, cancel_all, retry, semaphore
rate-limit, ws-message parsing into OrderEvent, callback fan-out,
graceful close.

*Does not* — strategy decisions (delegated to lpp_quoter, batch 3
part 2), ws frequency / connection management (delegated to a future
``LighterAccountWs`` once we have a live ``account_all`` payload to
tune the parser against — see ``on_account_event`` docstring).

Why ``on_account_event`` is the public hook
-------------------------------------------
The Lighter SDK's bundled ``WsClient`` only subscribes to
``account_all/<idx>`` and pushes the entire account state on every
update. That payload shape isn't pinned down for this codebase yet
(no live mainnet sample captured), so we defer the actual ws plumbing
and instead expose ``on_account_event`` as a parser-only entry point:
the caller (paper run / lpp_quoter) is responsible for delivering the
ws message dict, and the parser is defensive about field names so a
shape drift only logs a warning rather than crashing.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

# InventoryState lives in strategy.types; importing it here keeps the
# get_inventory() return shape consistent with what plan_quotes consumes.
from strategy.types import InventoryState

logger = logging.getLogger(__name__)


# ----- SDK constants ------------------------------------------------------
# Sourced from lighter.signer_client.SignerClient (1.0.x). Hard-coded
# here so the module imports cleanly even when the SDK isn't installed
# (test runners don't need the native signer).
#
# The dynamic-load block below picks up real SDK constants when
# available; the literals are the fallback. CI tests pin both paths
# (test_sdk_constants_match_real_sdk_when_available).

_LIGHTER_ORDER_TYPE_LIMIT = 0
_LIGHTER_TIF_IOC = 0
_LIGHTER_TIF_GTT = 1
_LIGHTER_TIF_POST_ONLY = 2
_LIGHTER_NIL_TRIGGER_PRICE = 0
_LIGHTER_DEFAULT_28D_EXPIRY = -1

try:  # pragma: no cover - optional SDK presence
    from lighter import SignerClient as _SC  # type: ignore[import-not-found]

    _LIGHTER_ORDER_TYPE_LIMIT = _SC.ORDER_TYPE_LIMIT
    _LIGHTER_TIF_IOC = _SC.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    _LIGHTER_TIF_GTT = _SC.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
    _LIGHTER_TIF_POST_ONLY = _SC.ORDER_TIME_IN_FORCE_POST_ONLY
    _LIGHTER_NIL_TRIGGER_PRICE = _SC.NIL_TRIGGER_PRICE
    _LIGHTER_DEFAULT_28D_EXPIRY = _SC.DEFAULT_28_DAY_ORDER_EXPIRY
except (ImportError, AttributeError):
    pass


# Public string → SDK int. Unknown keys raise ValueError so a typo
# fails loudly rather than silently sending the wrong order type.
_ORDER_TYPE_MAP: Dict[str, int] = {
    "limit": _LIGHTER_ORDER_TYPE_LIMIT,
}

_TIME_IN_FORCE_MAP: Dict[str, int] = {
    "ioc": _LIGHTER_TIF_IOC,
    "gtt": _LIGHTER_TIF_GTT,
    "post_only": _LIGHTER_TIF_POST_ONLY,
}


# Lighter SDK's Order.status enum (lighter.models.order.Order:86) →
# our internal ManagedOrder.status. The "canceled-*" families split
# into rejected (post-only / margin / liquidity / self-trade), expired
# (canceled-expired), and cancelled (canceled / canceled-oco /
# canceled-child / canceled-liquidation).
_SDK_STATUS_MAP: Dict[str, str] = {
    "in-progress": "pending_ack",
    "pending": "pending_ack",
    "open": "live",
    "filled": "filled",
    "canceled": "cancelled",
    "canceled-post-only": "rejected",
    "canceled-margin-not-allowed": "rejected",
    "canceled-position-not-allowed": "rejected",
    "canceled-self-trade": "rejected",
    "canceled-too-much-slippage": "rejected",
    "canceled-not-enough-liquidity": "rejected",
    "canceled-invalid-balance": "rejected",
    "canceled-reduce-only": "rejected",
    "canceled-liquidation": "cancelled",
    "canceled-oco": "cancelled",
    "canceled-child": "cancelled",
    "canceled-expired": "expired",
}

_TERMINAL_STATUSES = frozenset({"filled", "cancelled", "rejected", "expired"})


# Lighter caps client_order_index at 2**48 - 1 (= 281474976710655). We
# compose: 32-bit (seconds since epoch offset) | 16-bit counter. The
# offset (Unix ts of 2026-01-01 UTC) keeps the seconds field well under
# 2**32 for the next 100+ years while leaving the 16-bit counter for
# in-second uniqueness.
_COID_EPOCH_OFFSET_SEC = 1735689600  # 2026-01-01 UTC


# ----- exceptions ---------------------------------------------------------


class LighterSDKError(Exception):
    """Raised when the Lighter SDK returns a (None, None, error_str) triple.

    The SDK uses error-as-return-value rather than exceptions; this
    wrapper lets the retry path use ordinary try/except control flow.
    """


# ----- dataclasses --------------------------------------------------------


@dataclass
class ManagedOrder:
    """Mutable state we keep for every order we send.

    Keep the public surface small: status / fills / timestamps are
    what callers care about; internals like ``raw_response`` live in
    notes for debugging.
    """

    client_order_index: int
    side: str                           # "buy" / "sell"
    market_index: int
    price: Decimal
    size_base: Decimal
    price_decimals: int
    size_decimals: int
    order_type: str                     # "limit"
    time_in_force: str                  # "post_only" / "gtt" / "ioc"
    reduce_only: bool
    sent_ts_ms: int

    order_index: Optional[int] = None
    status: str = "pending_send"
    filled_base: Decimal = field(default_factory=lambda: Decimal(0))
    avg_fill_price: Optional[Decimal] = None
    acked_ts_ms: Optional[int] = None
    closed_ts_ms: Optional[int] = None
    last_error: Optional[str] = None
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class OrderEvent:
    """Event fanned out to ``register_event_callback`` subscribers."""

    event_type: str                     # "live" / "fill" / "cancelled" / "rejected" / "expired"
    client_order_index: int
    order_index: Optional[int]
    fill_size_base: Optional[Decimal]
    fill_price: Optional[Decimal]
    remaining_base: Optional[Decimal]
    timestamp_ms: int
    raw_msg: Dict[str, Any]


# ----- helpers ------------------------------------------------------------


def _to_int_price(price: Decimal, price_decimals: int) -> int:
    """Quantize a Decimal price to the SDK's integer scale.

    ROUND_HALF_UP collapses any sub-tick float-ish remainder to the
    nearest tick — the planner already produced a tick-aligned price,
    so this is just defence against a stray Decimal trailing zero.
    """
    scale = Decimal(10) ** price_decimals
    return int((price * scale).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _to_int_size(size: Decimal, size_decimals: int) -> int:
    """Quantize a Decimal base size to the SDK's integer scale.

    ROUND_DOWN: never round size up — wouldn't want the SDK rejecting
    the order because we asked for more base than the planner sized.
    """
    scale = Decimal(10) ** size_decimals
    return int((size * scale).quantize(Decimal(1), rounding=ROUND_DOWN))


def _coerce_decimal(value: Any) -> Optional[Decimal]:
    """Best-effort Decimal conversion. None / "" / garbage → None."""
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ----- order manager ------------------------------------------------------


EventCallback = Callable[[OrderEvent], Union[None, Awaitable[None]]]


class LighterOrderManager:
    """Async manager that owns every order our process puts on Lighter.

    Lifecycle: instantiate → ``await start()`` → many submit/cancel
    calls → ``await close()``. Concurrent submit/cancel are gated by
    a ``Semaphore(max_concurrent_requests)`` so a burst of 50 orders
    never trips the SDK's per-second send_tx ceiling.
    """

    def __init__(
        self,
        gateway: Any,
        ws: Any,
        account_index: int,
        max_concurrent_requests: int = 5,
        request_timeout_sec: float = 10.0,
        retry_max_attempts: int = 3,
        retry_backoff_sec: float = 1.0,
        periodic_sync_interval_sec: float = 30.0,
        market_index_filter: Optional[int] = None,
        sync_missing_grace_sec: float = 10.0,
    ):
        self._gateway = gateway
        self._ws = ws
        self._account_index = int(account_index)

        self._max_concurrent = int(max_concurrent_requests)
        self._request_timeout_sec = float(request_timeout_sec)
        self._retry_max_attempts = int(retry_max_attempts)
        self._retry_backoff_sec = float(retry_backoff_sec)

        self._semaphore = asyncio.Semaphore(self._max_concurrent)

        self._active: Dict[int, ManagedOrder] = {}
        self._historical: Dict[int, ManagedOrder] = {}

        self._coid_lock = threading.Lock()
        # 16-bit in-second counter. Restart resets to 0, but the
        # seconds field has advanced by the time we re-bind, so a
        # collision with the previous process's resting orders is
        # impossible unless restart happens within the same wall-clock
        # second AND the previous process generated the same counter
        # value — well outside our submission rate.
        self._coid_counter: int = 0

        self._event_callbacks: List[EventCallback] = []

        self._started: bool = False
        self._closing: bool = False
        self._closed: bool = False

        # REST sync state. Lighter's ``account_all`` ws channel only
        # carries position summaries (open_order_count) — no per-order
        # detail. We pull order_index / status / fills from REST
        # ``OrderApi.account_active_orders`` whenever the ws signals a
        # change, plus a periodic safety-net sync.
        self._market_index_filter: Optional[int] = (
            int(market_index_filter) if market_index_filter is not None else None
        )
        self._last_sync_ts_ms: int = 0
        self._sync_in_progress: bool = False
        self._sync_lock: asyncio.Lock = asyncio.Lock()
        self._periodic_sync_task: Optional[asyncio.Task] = None
        self._periodic_sync_interval_sec: float = float(periodic_sync_interval_sec)
        # How long to wait after submit before treating a "missing from
        # REST" order as gone (avoid race where REST hasn't yet picked
        # up a freshly-sent order).
        self._sync_missing_grace_sec: float = float(sync_missing_grace_sec)
        # Per-market last-seen open_order_count. Position-change
        # detector trips when this differs from the new ws push.
        self._last_open_order_count: Dict[int, int] = {}

        self._stats = {
            "lifetime_submitted": 0,
            "lifetime_filled": 0,
            "lifetime_cancelled": 0,
            "lifetime_rejected": 0,
            "lifetime_expired": 0,
            "send_failures_total": 0,
            "in_flight_requests": 0,
            "ws_msgs_received": 0,
            "last_event_ts_ms": None,
            "rest_syncs_total": 0,
            "last_sync_ts_ms": None,
            "orphans_cancelled_total": 0,
        }

        # Inventory aggregation. Updated by two paths:
        # 1. ws ``positions`` push (authoritative, when wire format
        #    delivers it)
        # 2. ``orders`` array path (legacy / test fixtures) — applies
        #    the per-fill weighted-average algorithm in
        #    ``_apply_fill_to_inventory``.
        # Both write to the same fields so ``get_inventory`` stays O(1).
        self._inventory_base: Decimal = Decimal(0)
        self._inventory_avg_price: Optional[Decimal] = None

        # Fill signal — quoter polls this each tick to decide if it
        # should re-plan immediately (bypassing the reprice min-interval).
        # asyncio.Event() doesn't require a running loop in 3.10+; we
        # also keep a bool fallback so ``pop_fill_signal`` works on
        # the rare case where someone polls before any await happened.
        self._fill_signal_event: asyncio.Event = asyncio.Event()
        self._fill_signal_flag: bool = False

    # ------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        """Mark the manager started, launch periodic REST sync, prime state.

        WS subscription to ``account_all/<account_index>`` is performed
        by the caller (e.g. ``LppQuoter.start``); this method:

        * starts the periodic sync task (a safety-net REST poll every
          ``periodic_sync_interval_sec`` seconds) so order state stays
          fresh even if no ws push arrives,
        * does one immediate sync to prime ``order_index`` / status for
          any orders the recovery step missed.
        """
        if self._started:
            return
        self._started = True
        # Periodic REST poll. Background — exceptions are logged inside
        # the loop so a transient REST failure doesn't kill the task.
        if self._periodic_sync_task is None or self._periodic_sync_task.done():
            self._periodic_sync_task = asyncio.create_task(
                self._periodic_sync_loop(),
                name="lighter-om-periodic-sync",
            )
        # Prime: pull whatever the account currently has so subsequent
        # cancel_order calls don't trip on a missing order_index for an
        # order the recovery step left untouched.
        try:
            await self._sync_orders_from_rest_safe()
        except Exception as exc:  # noqa: BLE001
            logger.warning("initial REST sync failed: %s", exc)
        logger.info(
            "LighterOrderManager started (account_index=%d, max_concurrent=%d, "
            "sync_interval=%.1fs)",
            self._account_index,
            self._max_concurrent,
            self._periodic_sync_interval_sec,
        )

    async def close(self) -> None:
        """Idempotent graceful shutdown.

        1. Mark ``_closing`` so further submit attempts raise.
        2. Cancel the periodic sync task — no more REST traffic.
        3. Cancel every active order (best-effort — SDK errors are
           swallowed so close() always returns).
        4. Mark ``_closed``.
        """
        if self._closed:
            return
        self._closing = True
        # Stop the periodic REST sync first so we don't issue an
        # auth_token mid-shutdown after the gateway has begun closing.
        await self._stop_periodic_sync_task()
        try:
            await self.cancel_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning("cancel_all during close raised: %s", exc)
        self._closed = True
        self._started = False
        logger.info(
            "LighterOrderManager closed (active=%d, historical=%d)",
            len(self._active),
            len(self._historical),
        )

    async def _stop_periodic_sync_task(self) -> None:
        """Idempotently cancel the periodic sync task."""
        task = self._periodic_sync_task
        self._periodic_sync_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("periodic sync task raised on cancel: %s", exc)

    # ------------------------------------------------------------
    # client order id
    # ------------------------------------------------------------

    def _generate_client_order_index(self) -> int:
        """Return a 48-bit-safe client_order_index.

        Lighter rejects ``ClientOrderIndex >= 2**48``. We pack
        ``(seconds_since_epoch_offset << 16) | 16-bit_counter`` so the
        result stays under the limit for 100+ years while remaining
        unique across submissions. Thread-safe via a regular Lock —
        submit_order is async but the increment must be atomic against
        any concurrent caller.
        """
        with self._coid_lock:
            secs = int(time.time()) - _COID_EPOCH_OFFSET_SEC
            self._coid_counter = (self._coid_counter + 1) & 0xFFFF
            coid = (secs << 16) | self._coid_counter
            if coid >= (1 << 48):
                raise RuntimeError(
                    f"client_order_index {coid} exceeds Lighter 48-bit limit "
                    f"(secs={secs}, counter={self._coid_counter})"
                )
            return coid

    # ------------------------------------------------------------
    # submit_order
    # ------------------------------------------------------------

    async def submit_order(
        self,
        side: str,
        market_index: int,
        price: Decimal,
        size_base: Decimal,
        price_decimals: int,
        size_decimals: int,
        order_type: str = "limit",
        time_in_force: str = "post_only",
        reduce_only: bool = False,
    ) -> int:
        """Send a new order. Returns the assigned ``client_order_index``.

        Raises:
            RuntimeError: manager is closing/closed.
            ValueError: unknown ``order_type`` / ``time_in_force`` /
                ``side`` / non-positive size or price.
        """
        if self._closing or self._closed:
            raise RuntimeError("LighterOrderManager is closing/closed; not accepting new orders")
        if side not in ("buy", "sell"):
            raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")
        if order_type not in _ORDER_TYPE_MAP:
            raise ValueError(
                f"unknown order_type {order_type!r}; supported: {sorted(_ORDER_TYPE_MAP)}"
            )
        if time_in_force not in _TIME_IN_FORCE_MAP:
            raise ValueError(
                f"unknown time_in_force {time_in_force!r}; supported: {sorted(_TIME_IN_FORCE_MAP)}"
            )
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        if size_base <= 0:
            raise ValueError(f"size_base must be positive, got {size_base}")

        coid = self._generate_client_order_index()
        sent_ts = int(time.time() * 1000)

        order = ManagedOrder(
            client_order_index=coid,
            side=side,
            market_index=int(market_index),
            price=price,
            size_base=size_base,
            price_decimals=int(price_decimals),
            size_decimals=int(size_decimals),
            order_type=order_type,
            time_in_force=time_in_force,
            reduce_only=bool(reduce_only),
            sent_ts_ms=sent_ts,
        )
        self._active[coid] = order
        self._stats["lifetime_submitted"] += 1
        logger.info(
            "submit: coid=%d %s market=%d @%s size=%s tif=%s",
            coid,
            side,
            int(market_index),
            price,
            size_base,
            time_in_force,
        )

        price_int = _to_int_price(price, price_decimals)
        size_int = _to_int_size(size_base, size_decimals)
        if size_int <= 0:
            self._set_status(order, "rejected", reason="size_quantized_zero")
            order.last_error = f"size quantized to {size_int}"
            order.closed_ts_ms = sent_ts
            self._move_to_historical(coid)
            self._stats["lifetime_rejected"] += 1
            raise ValueError(f"size_base {size_base} quantized to non-positive int with size_decimals={size_decimals}")

        is_ask = side == "sell"
        sdk_order_type = _ORDER_TYPE_MAP[order_type]
        sdk_tif = _TIME_IN_FORCE_MAP[time_in_force]

        try:
            await self._send_create_order_with_retry(
                market_index=int(market_index),
                client_order_index=coid,
                base_amount=size_int,
                price=price_int,
                is_ask=is_ask,
                order_type=sdk_order_type,
                time_in_force=sdk_tif,
                reduce_only=bool(reduce_only),
            )
            self._set_status(order, "pending_ack", reason="sdk_send_ok")
        except (LighterSDKError, asyncio.TimeoutError) as exc:
            self._set_status(order, "rejected", reason="sdk_send_failed")
            order.last_error = str(exc)
            order.closed_ts_ms = int(time.time() * 1000)
            self._move_to_historical(coid)
            self._stats["lifetime_rejected"] += 1
            self._stats["send_failures_total"] += 1
            logger.warning(
                "submit_order rejected after retries (coid=%d): %s", coid, exc
            )
        except Exception as exc:  # noqa: BLE001
            # Unexpected — promote to rejected for safety, but flag.
            self._set_status(order, "rejected", reason="sdk_send_unexpected")
            order.last_error = f"unexpected: {exc!r}"
            order.closed_ts_ms = int(time.time() * 1000)
            self._move_to_historical(coid)
            self._stats["lifetime_rejected"] += 1
            self._stats["send_failures_total"] += 1
            logger.error(
                "submit_order unexpected error (coid=%d)", coid, exc_info=True
            )

        return coid

    async def _send_create_order_with_retry(self, **kwargs: Any) -> None:
        """Call ``signer.create_order`` with timeout + exponential retry.

        SDK returns ``(CreateOrder, RespSendTx, None)`` on success or
        ``(None, None, error_str)`` on failure. The latter raises
        ``LighterSDKError``, which the retry wrapper catches.
        """
        signer = self._signer()
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self._retry_max_attempts + 1):
            self._stats["in_flight_requests"] += 1
            try:
                async with self._semaphore:
                    coro = signer.create_order(**kwargs)
                    result = await asyncio.wait_for(
                        coro, timeout=self._request_timeout_sec
                    )
                _check_sdk_result(result)
                # Surface retry-success so post-hoc forensics knows the
                # final tx landed despite the earlier transient.
                if attempt > 1:
                    logger.info(
                        "submit retry success: coid=%s attempt=%d/%d (transient: %s)",
                        kwargs.get("client_order_index"),
                        attempt,
                        self._retry_max_attempts,
                        last_exc,
                    )
                return
            except (LighterSDKError, asyncio.TimeoutError) as exc:
                last_exc = exc
                logger.warning(
                    "create_order attempt %d/%d failed: %s",
                    attempt,
                    self._retry_max_attempts,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001
                # Treat unexpected SDK exceptions as transient — most
                # commonly aiohttp.ClientError on network blip.
                last_exc = exc
                logger.warning(
                    "create_order attempt %d/%d unexpected: %s",
                    attempt,
                    self._retry_max_attempts,
                    exc,
                )
            finally:
                self._stats["in_flight_requests"] -= 1

            if attempt < self._retry_max_attempts:
                await asyncio.sleep(self._retry_backoff_sec * (2 ** (attempt - 1)))

        # Out of attempts — re-raise the last error so submit_order
        # records it on ManagedOrder.
        assert last_exc is not None
        if isinstance(last_exc, (LighterSDKError, asyncio.TimeoutError)):
            raise last_exc
        raise LighterSDKError(str(last_exc))

    # ------------------------------------------------------------
    # cancel_order
    # ------------------------------------------------------------

    async def cancel_order(self, client_order_index: int) -> bool:
        """Send a cancel for ``client_order_index``.

        Returns True iff the cancel tx was successfully sent. The
        actual status transition (live → cancelled) lands when the ws
        push arrives — caller may inspect ``get_order_state`` after a
        short delay if it needs to confirm.

        Returns False without sending when:
        * order is unknown (likely from another process)
        * order is already in a terminal state
        * order has not been ack'd yet (no ``order_index`` known —
          Lighter cancel requires the on-chain id, not just our coid)
        """
        order = self._active.get(client_order_index)
        if order is None:
            logger.warning(
                "cancel_order unknown coid=%d (already historical or never seen)",
                client_order_index,
            )
            return False
        if order.status in _TERMINAL_STATUSES:
            logger.debug(
                "cancel_order coid=%d already terminal (%s); skipping",
                client_order_index,
                order.status,
            )
            return False
        if order.order_index is None:
            # Lighter's WS doesn't carry order_index — pull it from REST
            # before giving up. This is the load-bearing fallback that
            # makes shutdown's cancel_all actually able to cancel.
            logger.info(
                "cancel_order coid=%d missing order_index (status=%s); "
                "triggering REST sync",
                client_order_index,
                order.status,
            )
            await self._sync_orders_from_rest_safe()
            order = self._active.get(client_order_index)
            if order is None or order.order_index is None:
                logger.warning(
                    "cancel_order coid=%d still missing order_index after sync",
                    client_order_index,
                )
                return False

        try:
            await self._send_cancel_order_with_retry(
                market_index=order.market_index,
                order_index=order.order_index,
            )
            return True
        except (LighterSDKError, asyncio.TimeoutError) as exc:
            self._stats["send_failures_total"] += 1
            logger.warning(
                "cancel_order coid=%d failed after retries: %s",
                client_order_index,
                exc,
            )
            order.last_error = f"cancel: {exc}"
            return False
        except Exception as exc:  # noqa: BLE001
            self._stats["send_failures_total"] += 1
            order.last_error = f"cancel unexpected: {exc!r}"
            logger.error(
                "cancel_order coid=%d unexpected", client_order_index, exc_info=True
            )
            return False

    async def _send_cancel_order_with_retry(self, **kwargs: Any) -> None:
        signer = self._signer()
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self._retry_max_attempts + 1):
            self._stats["in_flight_requests"] += 1
            try:
                async with self._semaphore:
                    coro = signer.cancel_order(**kwargs)
                    result = await asyncio.wait_for(
                        coro, timeout=self._request_timeout_sec
                    )
                _check_sdk_result(result)
                return
            except (LighterSDKError, asyncio.TimeoutError) as exc:
                last_exc = exc
                logger.warning(
                    "cancel_order attempt %d/%d failed: %s",
                    attempt,
                    self._retry_max_attempts,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "cancel_order attempt %d/%d unexpected: %s",
                    attempt,
                    self._retry_max_attempts,
                    exc,
                )
            finally:
                self._stats["in_flight_requests"] -= 1

            if attempt < self._retry_max_attempts:
                await asyncio.sleep(self._retry_backoff_sec * (2 ** (attempt - 1)))

        assert last_exc is not None
        if isinstance(last_exc, (LighterSDKError, asyncio.TimeoutError)):
            raise last_exc
        raise LighterSDKError(str(last_exc))

    async def cancel_all(self) -> List[Tuple[int, bool]]:
        """Concurrently cancel every active order (semaphore-gated)."""
        coids = [
            coid
            for coid, o in list(self._active.items())
            if o.status not in _TERMINAL_STATUSES
        ]
        if not coids:
            return []
        results = await asyncio.gather(
            *(self.cancel_order(coid) for coid in coids),
            return_exceptions=False,
        )
        return list(zip(coids, results))

    # ------------------------------------------------------------
    # views
    # ------------------------------------------------------------

    def get_active_orders(self) -> List[ManagedOrder]:
        """All non-terminal orders (in-flight, pending_ack, live, partial_fill)."""
        return [
            o for o in self._active.values() if o.status not in _TERMINAL_STATUSES
        ]

    def get_order_state(self, client_order_index: int) -> Optional[ManagedOrder]:
        if client_order_index in self._active:
            return self._active[client_order_index]
        return self._historical.get(client_order_index)

    def register_event_callback(self, callback: EventCallback) -> None:
        """Append a callback. Multiple callbacks are supported."""
        self._event_callbacks.append(callback)

    def get_stats(self) -> Dict[str, Any]:
        active = self.get_active_orders()
        return {
            "active_count": len(active),
            "lifetime_submitted": self._stats["lifetime_submitted"],
            "lifetime_filled": self._stats["lifetime_filled"],
            "lifetime_cancelled": self._stats["lifetime_cancelled"],
            "lifetime_rejected": self._stats["lifetime_rejected"],
            "lifetime_expired": self._stats["lifetime_expired"],
            "in_flight_requests": self._stats["in_flight_requests"],
            "send_failures_total": self._stats["send_failures_total"],
            "ws_msgs_received": self._stats["ws_msgs_received"],
            "last_event_ts_ms": self._stats["last_event_ts_ms"],
            "rest_syncs_total": self._stats["rest_syncs_total"],
            "last_sync_ts_ms": self._stats["last_sync_ts_ms"],
            "orphans_cancelled_total": self._stats["orphans_cancelled_total"],
        }

    def get_inventory(
        self,
        mark_price: Optional[Decimal] = None,
    ) -> InventoryState:
        """Return the manager's net inventory as ``InventoryState``.

        Aggregated incrementally from every fill the ws delivered.
        Caller passes ``mark_price`` (typically the live market mid)
        so ``net_delta_usdc`` reflects current exposure rather than
        cost basis. When ``mark_price`` is omitted we fall back to
        the weighted-average entry — fine for diagnostics, but
        plan_quotes' skew/cap thresholds want a live mark.
        """
        if self._inventory_base == 0:
            net_usdc = Decimal(0)
        else:
            ref_price = (
                mark_price if mark_price is not None else self._inventory_avg_price
            )
            if ref_price is None:
                net_usdc = Decimal(0)
            else:
                net_usdc = self._inventory_base * Decimal(str(ref_price))
        return InventoryState(
            net_delta_base=self._inventory_base,
            net_delta_usdc=net_usdc,
            avg_entry_price=self._inventory_avg_price,
            open_orders_count=len(self.get_active_orders()),
        )

    def pop_fill_signal(self) -> bool:
        """Consume the "fill happened" flag set on the last ws fill.

        Returns True iff a fill arrived since the last call. Both the
        asyncio.Event and the bool fallback are cleared on read so the
        next tick won't see a stale signal.
        """
        had_signal = self._fill_signal_flag or self._fill_signal_event.is_set()
        if had_signal:
            self._fill_signal_flag = False
            self._fill_signal_event.clear()
        return had_signal

    # ------------------------------------------------------------
    # ws event entry point
    # ------------------------------------------------------------

    def on_account_event(self, msg: Dict[str, Any]) -> None:
        """Parse one ``account_all`` push, update local state, trigger sync.

        Lighter's real wire format (verified live on mainnet) carries
        only ``positions`` summaries with ``open_order_count`` — not the
        per-order array our earlier implementation expected. We therefore
        operate in two modes:

        1. **Legacy / test-fixture path** — when the message includes an
           ``orders`` list (old fixtures, or a hypothetical future
           server-side change), each entry is parsed by the original
           ``_process_order_update`` so existing tests stay valid.
        2. **Position path** — the live wire format. We diff
           ``positions[mi].open_order_count`` against the last seen
           value and, if it changed, fire a fire-and-forget
           ``_sync_orders_from_rest_safe`` task. We also pull authoritative
           position data into the inventory aggregator.

        Either path is safe; if both arrive in the same message (very
        unlikely on real wire), the legacy path runs first and the
        position path triggers a sync on top. Defensive on every field —
        a missing or renamed key skips the order with a single warning
        rather than crashing the caller.
        """
        self._stats["ws_msgs_received"] += 1
        self._stats["last_event_ts_ms"] = int(time.time() * 1000)

        if not isinstance(msg, dict):
            logger.warning("on_account_event: msg is not a dict (%r); ignoring", type(msg))
            return

        # Legacy / test-fixture path: per-order detail.
        orders_list = self._extract_orders(msg)
        if orders_list:
            for raw in orders_list:
                try:
                    self._process_order_update(raw, msg)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "on_account_event: order update parse failed (raw=%r): %s",
                        raw,
                        exc,
                        exc_info=True,
                    )

        # Position path: diff open_order_count to detect order-state
        # change without any per-order detail in the ws frame.
        positions = msg.get("positions")
        if isinstance(positions, dict) and positions:
            try:
                trigger_sync = self._update_positions_from_msg(positions)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "on_account_event: position parse failed: %s", exc, exc_info=True
                )
                trigger_sync = False
            if trigger_sync:
                try:
                    asyncio.get_running_loop().create_task(
                        self._sync_orders_from_rest_safe()
                    )
                except RuntimeError:
                    # No running loop (rare — shouldn't happen in
                    # production since on_account_event is called from
                    # the ws task). Just log and rely on periodic poll.
                    logger.debug(
                        "on_account_event: no running loop for sync trigger"
                    )

    def _extract_orders(self, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Walk a few likely locations to find the order list.

        Preference order:
        1. ``msg["orders"]`` — direct list (current Order model shape)
        2. ``msg["account"]["orders"]`` — nested under account snapshot
        3. ``msg["data"]["orders"]`` — generic "data" wrapper
        """
        candidates: List[Any] = []
        if isinstance(msg.get("orders"), list):
            candidates = msg["orders"]
        elif isinstance(msg.get("account"), dict) and isinstance(
            msg["account"].get("orders"), list
        ):
            candidates = msg["account"]["orders"]
        elif isinstance(msg.get("data"), dict) and isinstance(
            msg["data"].get("orders"), list
        ):
            candidates = msg["data"]["orders"]
        return [c for c in candidates if isinstance(c, dict)]

    def _process_order_update(
        self,
        raw: Dict[str, Any],
        full_msg: Dict[str, Any],
    ) -> None:
        coid = _coerce_int(raw.get("client_order_index"))
        if coid is None:
            logger.debug("ws order update missing client_order_index: %r", raw)
            return

        order = self._active.get(coid) or self._historical.get(coid)
        if order is None:
            # Other-process order (or stale msg) — record nothing.
            logger.debug("ws order update for unknown coid=%d; skipping", coid)
            return

        # Lighter assigns order_index on first ack; pin it once we see it.
        order_index = _coerce_int(raw.get("order_index"))
        if order_index is not None and order.order_index is None:
            order.order_index = order_index

        sdk_status_raw = raw.get("status")
        # Status may arrive as canonical str or (rare) as int code; we
        # only know the str form, so int-coded statuses get logged but
        # not mapped — better to emit a warn than silently mistype.
        new_status: Optional[str] = None
        if isinstance(sdk_status_raw, str):
            new_status = _SDK_STATUS_MAP.get(sdk_status_raw)
            if new_status is None:
                logger.warning(
                    "ws order update unknown SDK status %r for coid=%d; keeping %s",
                    sdk_status_raw,
                    coid,
                    order.status,
                )
        elif sdk_status_raw is not None:
            logger.warning(
                "ws order update non-string status %r (type=%s) for coid=%d",
                sdk_status_raw,
                type(sdk_status_raw).__name__,
                coid,
            )

        filled_base = _coerce_decimal(raw.get("filled_base_amount"))
        remaining_base = _coerce_decimal(raw.get("remaining_base_amount"))
        # Lighter prices on the wire are str; convert defensively.
        price_str = raw.get("price") or raw.get("avg_fill_price")
        price_dec = _coerce_decimal(price_str)

        # Update fill bookkeeping.
        prior_filled = order.filled_base
        if filled_base is not None and filled_base > prior_filled:
            delta_size = filled_base - prior_filled
            order.filled_base = filled_base
            # Avg fill price approximation: use whatever the ws gave us
            # if present; fall back to the order price (common for
            # post-only fills landing at the resting price).
            if price_dec is not None and price_dec > 0:
                fill_price_used = price_dec
            else:
                fill_price_used = order.price
            order.avg_fill_price = fill_price_used
            # Aggregate manager-level inventory + raise the signal so
            # the quoter knows to re-plan on its next tick.
            self._apply_fill_to_inventory(order.side, delta_size, fill_price_used)
            self._signal_fill()

        # Pick the canonical status. ws "open" with non-zero filled
        # base is a partial fill — preserve that detail.
        prior_status = order.status
        if new_status is not None:
            if new_status == "live" and order.filled_base > 0 and (
                remaining_base is None or remaining_base > 0
            ):
                self._set_status(order, "partial_fill", reason="ws_partial_fill")
            else:
                self._set_status(order, new_status, reason="ws_status")

        if order.acked_ts_ms is None and order.status in (
            "live", "partial_fill", "filled", "cancelled", "rejected", "expired"
        ):
            order.acked_ts_ms = int(time.time() * 1000)

        if order.status in _TERMINAL_STATUSES and order.closed_ts_ms is None:
            order.closed_ts_ms = int(time.time() * 1000)

        # Migrate to historical and bump terminal counters once.
        if order.status in _TERMINAL_STATUSES and coid in self._active:
            self._move_to_historical(coid)
            self._bump_terminal_stats(order.status)

        # Build & dispatch event(s).
        events: List[OrderEvent] = []
        ts_ms = int(time.time() * 1000)

        # Fill event whenever filled_base advanced this update (covers
        # partial → live, partial → filled, immediate full fill).
        if filled_base is not None and filled_base > prior_filled:
            events.append(
                OrderEvent(
                    event_type="fill",
                    client_order_index=coid,
                    order_index=order.order_index,
                    fill_size_base=filled_base - prior_filled,
                    fill_price=order.avg_fill_price,
                    remaining_base=remaining_base,
                    timestamp_ms=ts_ms,
                    raw_msg=full_msg,
                )
            )

        if new_status is not None and order.status != prior_status:
            ev_type: Optional[str] = None
            if order.status == "live" or order.status == "partial_fill":
                ev_type = "live"
            elif order.status == "filled":
                ev_type = "filled"
            elif order.status == "cancelled":
                ev_type = "cancelled"
            elif order.status == "rejected":
                ev_type = "rejected"
            elif order.status == "expired":
                ev_type = "expired"
            if ev_type is not None:
                events.append(
                    OrderEvent(
                        event_type=ev_type,
                        client_order_index=coid,
                        order_index=order.order_index,
                        fill_size_base=None,
                        fill_price=None,
                        remaining_base=remaining_base,
                        timestamp_ms=ts_ms,
                        raw_msg=full_msg,
                    )
                )

        for ev in events:
            self._dispatch_event(ev)

    # ------------------------------------------------------------
    # position path (real Lighter wire format) + REST sync
    # ------------------------------------------------------------

    def _update_positions_from_msg(self, positions: Dict[Any, Any]) -> bool:
        """Diff ``open_order_count`` per market and return True if any changed.

        Side effect: writes the position into the inventory aggregator
        so ``get_inventory`` reflects the authoritative ws state. The
        return value tells the caller whether to fire a REST sync.
        """
        trigger_sync = False
        for mi_key, pos in positions.items():
            try:
                mi = int(mi_key)
            except (TypeError, ValueError):
                continue
            if not isinstance(pos, dict):
                continue
            if (
                self._market_index_filter is not None
                and mi != self._market_index_filter
            ):
                continue

            try:
                current_count = int(pos.get("open_order_count", 0))
            except (TypeError, ValueError):
                current_count = 0
            prev_count = self._last_open_order_count.get(mi)
            if prev_count is None:
                # First sighting — count as a change so the freshly-
                # connected manager primes its order_index map.
                trigger_sync = True
                logger.info(
                    "position first-seen: market=%d open_orders=%d (priming sync)",
                    mi,
                    current_count,
                )
            elif current_count != prev_count:
                trigger_sync = True
                logger.info(
                    "position change: market=%d open_orders %d→%d",
                    mi,
                    prev_count,
                    current_count,
                )
            self._last_open_order_count[mi] = current_count

            self._update_inventory_from_position(pos)
        return trigger_sync

    def inject_initial_inventory(
        self,
        base: Decimal,
        avg_entry_price: Optional[Decimal],
        sign: int = 1,
    ) -> None:
        """Set initial inventory state from external source (e.g. recover).

        Called once before the strategy main loop starts, to seed the OM
        with the actual server-side position. After this call, ws
        position updates remain authoritative and may overwrite. Without
        this seed, OM starts at inv=0 even if the account already holds
        a position, causing ``plan_quotes`` to under-estimate risk for
        the first few seconds before a ws push arrives — or never, if
        the ``account_all`` initial snapshot for the subscription path
        doesn't carry positions.

        Args:
            base: unsigned base amount (always non-negative).
            avg_entry_price: weighted entry price (None if base == 0).
            sign: ``+1`` (long) or ``-1`` (short).
        """
        if self._inventory_base != 0:
            logger.warning(
                "inject_initial_inventory: overwriting existing _inventory_base=%s",
                self._inventory_base,
            )
        if base == 0:
            self._inventory_base = Decimal(0)
            self._inventory_avg_price = None
            logger.info("inject_initial_inventory: cleared (base=0)")
            return
        signed = -base if sign < 0 else base
        self._inventory_base = signed
        self._inventory_avg_price = (
            avg_entry_price
            if (avg_entry_price is not None and avg_entry_price > 0)
            else None
        )
        logger.info(
            "inject_initial_inventory: base=%s avg=%s sign=%d -> inventory_base=%s",
            base,
            avg_entry_price,
            sign,
            self._inventory_base,
        )

    def _update_inventory_from_position(self, pos: Dict[str, Any]) -> None:
        """Apply ws position data to the inventory aggregator.

        This is the **AUTHORITATIVE** source of inventory state. The
        :py:meth:`_apply_fill_to_inventory` path remains as a fallback
        for periods when the ws is silent (e.g. reconnect window) or
        when state arrives via the legacy orders-array path; whenever
        a fresh ws ``positions`` push lands, those values overwrite
        whatever local fill accumulation produced. The planner's
        ``hard_position_cap_usdc`` and skew thresholds depend on
        agreement with the server-side ledger — drifting from the ws
        view by even one fill is enough to over- or under-cap.

        Why ws is authoritative over OM-side fill accumulation:

        * The OM has no fills feed — it only sees ``filled_base_amount``
          via the periodic REST sync, which can lag, miss a fill, or
          double-count if a process restart happens mid-fill.
        * External processes (manual web cancel, manual close-position)
          can change the position without going through the OM at all.
        * Lighter's matching engine is the source of truth for the
          actual on-chain position; ws ``positions`` is the lowest-
          latency mirror of that ledger.

        Lighter convention: ``position`` is unsigned and ``sign`` is +1
        (long) or -1 (short). A flat position is reported as
        ``position == 0`` with sign +1 — we clear ``avg_entry_price``
        in that case.
        """
        raw_pos = _coerce_decimal(pos.get("position"))
        if raw_pos is None:
            return
        try:
            sign = int(pos.get("sign", 1))
        except (TypeError, ValueError):
            sign = 1
        avg_entry = _coerce_decimal(pos.get("avg_entry_price"))

        if raw_pos == 0:
            self._inventory_base = Decimal(0)
            self._inventory_avg_price = None
            return

        signed = -raw_pos if sign < 0 else raw_pos
        self._inventory_base = signed
        self._inventory_avg_price = (
            avg_entry if (avg_entry is not None and avg_entry > 0) else None
        )

    async def _sync_orders_from_rest_safe(self) -> None:
        """Wrapper: prevents concurrent syncs and swallows exceptions.

        The ``_sync_lock`` guards the ``_sync_in_progress`` flag rather
        than the whole sync — concurrent triggers (ws + periodic + cancel
        retry all firing within a few ms) collapse to one REST round-trip.
        """
        async with self._sync_lock:
            if self._sync_in_progress:
                return
            self._sync_in_progress = True
        try:
            await self._sync_orders_from_rest()
        except Exception as exc:  # noqa: BLE001
            logger.error("REST order sync failed: %s", exc, exc_info=True)
        finally:
            self._sync_in_progress = False

    async def _sync_orders_from_rest(self) -> None:
        """Reconcile local ManagedOrder state with ``OrderApi.account_active_orders``.

        Two directions:

        * **REST → local**: for every order REST reports as active,
          fill ``order_index`` (if missing) and advance status from
          ``pending_ack`` → ``live`` (or ``partial_fill`` if fills
          arrived). Apply any fill delta to the inventory aggregator
          and raise the fill signal.
        * **local → REST**: for every active local order *not* in REST,
          if it's been at least ``_sync_missing_grace_sec`` since
          submission, treat it as terminal — best-effort label
          ``cancelled`` (could be filled; without a fills feed we
          cannot disambiguate, and treating as cancelled is the safer
          default for the planner's reprice loop).

        REST returns only the orders for ONE market per call. We
        determine the market set from ``_market_index_filter`` (if
        set) or the union of markets across active orders. An empty
        active set + no filter is a no-op.
        """
        if self._gateway is None:
            return

        # Resolve market set.
        if self._market_index_filter is not None:
            markets = {self._market_index_filter}
        else:
            markets = {o.market_index for o in self._active.values()}
        if not markets:
            # Still mark the sync timestamp so periodic-loop tests
            # observe a tick even when there's nothing to fetch.
            self._last_sync_ts_ms = int(time.time() * 1000)
            self._stats["rest_syncs_total"] += 1
            self._stats["last_sync_ts_ms"] = self._last_sync_ts_ms
            return

        rest_orders: List[Dict[str, Any]] = []
        for mi in markets:
            try:
                page = await self._gateway.get_open_orders(int(mi))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "REST get_open_orders(market=%d) failed: %s", mi, exc
                )
                continue
            if isinstance(page, list):
                rest_orders.extend(page)

        self._last_sync_ts_ms = int(time.time() * 1000)
        self._stats["rest_syncs_total"] += 1
        self._stats["last_sync_ts_ms"] = self._last_sync_ts_ms

        rest_by_coid: Dict[int, Dict[str, Any]] = {}
        for o in rest_orders:
            coid_raw = o.get("client_order_index")
            if coid_raw is None:
                coid_raw = o.get("client_order_id")
            try:
                coid = int(coid_raw) if coid_raw is not None else None
            except (TypeError, ValueError):
                coid = None
            if coid is None:
                continue
            rest_by_coid[coid] = o

        # Single-line summary so a tailing log can correlate ws position
        # changes against REST snapshots.
        logger.info(
            "REST sync: %d active locally, %d active on book, market_set=%s",
            len(self._active),
            len(rest_orders),
            sorted(markets),
        )

        # Direction 1: REST has it → update local.
        # Phase 1.2 P0.3: any coid REST reports on our account that we
        # don't track locally (or that already moved to historical) is
        # an orphan. Likely causes: a previous failed cancel where the
        # SDK returned an error but the server actually accepted, a ws
        # miss during a 429-burst, or a race during reconnect. Cancel
        # it immediately — letting it sit on the book is exactly what
        # let the 4-30 catastrophic short build up while OM saw inv=0.
        local_coids = set(self._active.keys()) | set(self._historical.keys())
        for coid, rest_o in rest_by_coid.items():
            local = self._active.get(coid)
            if local is None:
                if coid in local_coids:
                    # We've seen this coid before — it lives in
                    # _historical because we transitioned it terminal
                    # locally. Server still has it open → orphan.
                    logger.error(
                        "ORPHAN: coid=%d in _historical but still on book "
                        "(order_index=%s) — cancelling",
                        coid,
                        rest_o.get("order_index"),
                    )
                else:
                    # Genuinely unknown to us. Could be from a previous
                    # process; either way, an open order on our account
                    # we didn't track is an attractive nuisance.
                    logger.error(
                        "ORPHAN: coid=%d unknown (not in _active/_historical, "
                        "order_index=%s) — cancelling",
                        coid,
                        rest_o.get("order_index"),
                    )
                await self._cancel_orphan(rest_o)
                continue
            self._apply_rest_order_to_local(local, rest_o)

        # Phase 1.2 P0.3: a REST-reported order with no client_order_index
        # field at all should never happen (every order we send carries
        # one). If we see it, it's both unknown to us AND server-side
        # accepted — cancel it.
        for rest_o in rest_orders:
            coid_raw = rest_o.get("client_order_index")
            if coid_raw is None:
                coid_raw = rest_o.get("client_order_id")
            try:
                coid = int(coid_raw) if coid_raw is not None else None
            except (TypeError, ValueError):
                coid = None
            if coid is None:
                logger.error(
                    "ORPHAN (no coid): order_index=%s market=%s — cancelling",
                    rest_o.get("order_index"),
                    rest_o.get("market_index"),
                )
                await self._cancel_orphan(rest_o)

        # Direction 2: local active but not in REST → terminal.
        now_ms = int(time.time() * 1000)
        for coid in list(self._active.keys()):
            if coid in rest_by_coid:
                continue
            local = self._active[coid]
            if local.status == "pending_send":
                continue
            if local.status in _TERMINAL_STATUSES:
                continue
            age_sec = (now_ms - local.sent_ts_ms) / 1000.0
            if age_sec < self._sync_missing_grace_sec:
                continue
            prev_status = local.status
            self._set_status(local, "cancelled", reason="rest_sync_missing")
            local.closed_ts_ms = now_ms
            local.notes.append("rest_sync_missing")
            logger.info(
                "REST sync: coid=%d gone from book after %.1fs (was %s); "
                "marking cancelled",
                coid,
                age_sec,
                prev_status,
            )
            self._move_to_historical(coid)
            self._stats["lifetime_cancelled"] += 1
            self._dispatch_event(
                OrderEvent(
                    event_type="cancelled",
                    client_order_index=coid,
                    order_index=local.order_index,
                    fill_size_base=None,
                    fill_price=None,
                    remaining_base=None,
                    timestamp_ms=now_ms,
                    raw_msg={"reason": "rest_sync_missing"},
                )
            )

    async def _cancel_orphan(self, rest_o: Dict[str, Any]) -> None:
        """Issue a REST cancel for an orphan order surfaced during sync.

        Errors are logged and swallowed — the periodic sync will catch
        the same orphan again on the next pass if this one fails. We
        bump ``orphans_cancelled_total`` only on a successful cancel
        send so the stat reflects real ledger writes, not attempts.
        """
        order_index = _coerce_int(rest_o.get("order_index"))
        market_index = _coerce_int(rest_o.get("market_index"))
        if market_index is None:
            market_index = self._market_index_filter
        if order_index is None or market_index is None:
            logger.error(
                "orphan cancel: missing order_index/market_index "
                "(order_index=%s, market_index=%s)",
                order_index,
                market_index,
            )
            return
        gw_cancel = getattr(self._gateway, "cancel_order_by_index", None)
        if gw_cancel is None:
            logger.error(
                "orphan cancel: gateway has no cancel_order_by_index method"
            )
            return
        try:
            res = await gw_cancel(int(market_index), int(order_index))
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "orphan cancel raised for order_index=%d: %s",
                order_index,
                exc,
            )
            return
        if isinstance(res, dict) and not res.get("ok"):
            logger.error(
                "orphan cancel failed for order_index=%d: %s",
                order_index,
                res.get("error"),
            )
            return
        self._stats["orphans_cancelled_total"] += 1
        logger.info(
            "orphan cancelled: order_index=%d market=%d total=%d",
            order_index,
            market_index,
            self._stats["orphans_cancelled_total"],
        )

    def _apply_rest_order_to_local(
        self,
        local: ManagedOrder,
        rest_o: Dict[str, Any],
    ) -> None:
        """Update one ``ManagedOrder`` in place from a REST order dict.

        Pulled out of ``_sync_orders_from_rest`` so the per-order logic
        is unit-testable in isolation. Emits live + fill events as
        warranted; status transitions follow the same state machine
        the legacy ws path uses.
        """
        coid = local.client_order_index
        now_ms = int(time.time() * 1000)
        events: List[OrderEvent] = []

        # order_index — fill once.
        rest_oi = _coerce_int(rest_o.get("order_index"))
        if rest_oi is not None and local.order_index is None:
            local.order_index = rest_oi
            logger.info(
                "REST sync: coid=%d order_index=%d primed", coid, rest_oi
            )

        # status — REST returns 'open' / 'in-progress' / 'pending'
        # for active orders. Map through the same table the ws path
        # uses; a missing key falls through with a debug log.
        prev_status = local.status
        rest_status_raw = rest_o.get("status")
        new_status: Optional[str] = None
        if isinstance(rest_status_raw, str):
            new_status = _SDK_STATUS_MAP.get(rest_status_raw)
            if new_status is None:
                logger.debug(
                    "REST sync: coid=%d unknown status %r; keeping %s",
                    coid,
                    rest_status_raw,
                    prev_status,
                )

        # Fill bookkeeping — REST gives cumulative filled amount, so
        # detect a delta vs our local cache.
        prior_filled = local.filled_base
        rest_filled = _coerce_decimal(rest_o.get("filled_base_amount"))
        rest_remaining = _coerce_decimal(rest_o.get("remaining_base_amount"))
        if rest_filled is not None and rest_filled > prior_filled:
            delta_size = rest_filled - prior_filled
            local.filled_base = rest_filled
            fill_price = local.price
            local.avg_fill_price = fill_price
            self._apply_fill_to_inventory(local.side, delta_size, fill_price)
            self._signal_fill()
            events.append(
                OrderEvent(
                    event_type="fill",
                    client_order_index=coid,
                    order_index=local.order_index,
                    fill_size_base=delta_size,
                    fill_price=fill_price,
                    remaining_base=rest_remaining,
                    timestamp_ms=now_ms,
                    raw_msg=rest_o,
                )
            )

        # Pick canonical status. Same partial-fill rule as the ws path.
        if new_status is not None:
            if (
                new_status == "live"
                and local.filled_base > 0
                and (rest_remaining is None or rest_remaining > 0)
            ):
                self._set_status(local, "partial_fill", reason="rest_partial_fill")
            else:
                self._set_status(local, new_status, reason="rest_sync")

        if local.acked_ts_ms is None and local.status in (
            "live", "partial_fill", "filled", "cancelled", "rejected", "expired"
        ):
            local.acked_ts_ms = now_ms

        if local.status in _TERMINAL_STATUSES and local.closed_ts_ms is None:
            local.closed_ts_ms = now_ms

        if local.status in _TERMINAL_STATUSES and coid in self._active:
            self._move_to_historical(coid)
            self._bump_terminal_stats(local.status)

        if local.status != prev_status and new_status is not None:
            ev_type = {
                "live": "live",
                "partial_fill": "live",
                "filled": "filled",
                "cancelled": "cancelled",
                "rejected": "rejected",
                "expired": "expired",
            }.get(local.status)
            if ev_type is not None:
                events.append(
                    OrderEvent(
                        event_type=ev_type,
                        client_order_index=coid,
                        order_index=local.order_index,
                        fill_size_base=None,
                        fill_price=None,
                        remaining_base=rest_remaining,
                        timestamp_ms=now_ms,
                        raw_msg=rest_o,
                    )
                )

        for ev in events:
            self._dispatch_event(ev)

    async def _periodic_sync_loop(self) -> None:
        """Safety-net REST poll. Runs every ``_periodic_sync_interval_sec``.

        Catches the case where the ws position-change trigger silently
        misses an event (rare but observed in long runs). Cancellation
        from ``close()`` is swallowed; any other exception is logged
        and the loop continues so a single REST hiccup doesn't kill
        the safety net.
        """
        try:
            while not self._closing and not self._closed:
                try:
                    await asyncio.sleep(self._periodic_sync_interval_sec)
                except asyncio.CancelledError:
                    raise
                if self._closing or self._closed:
                    return
                # Per-tick diagnostic. Debug level so it's silent unless
                # someone explicitly turned on -vv. The age helps spot
                # a periodic loop that's silently slipped behind.
                last_age = (
                    (time.time() * 1000 - self._last_sync_ts_ms) / 1000.0
                    if self._last_sync_ts_ms
                    else -1.0
                )
                logger.debug(
                    "periodic sync tick: rest_syncs_total=%d last_sync_age=%.1fs",
                    self._stats["rest_syncs_total"],
                    last_age,
                )
                try:
                    await self._sync_orders_from_rest_safe()
                except Exception as exc:  # noqa: BLE001
                    logger.error("periodic sync error: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            raise

    def _dispatch_event(self, event: OrderEvent) -> None:
        """Fan out to every registered callback. Exceptions are isolated."""
        for cb in self._event_callbacks:
            try:
                result = cb(event)
                if asyncio.iscoroutine(result):
                    # Schedule the coroutine — the event loop owns it.
                    # (fire-and-forget; callback exceptions show up in
                    # asyncio's default handler.)
                    asyncio.ensure_future(result)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "event callback raised on event %s coid=%d: %s",
                    event.event_type,
                    event.client_order_index,
                    exc,
                )

    # ------------------------------------------------------------
    # internals
    # ------------------------------------------------------------

    def _signer(self) -> Any:
        """Look up the SignerClient from the gateway; fail loudly if absent."""
        signer = getattr(self._gateway, "signer_client", None)
        if signer is None:
            signer = getattr(self._gateway, "_signer_client", None)
        if signer is None:
            raise RuntimeError(
                "Lighter gateway has no SignerClient — cannot send orders. "
                "Configure API_KEY_PRIVATE_KEY before instantiating "
                "the order manager."
            )
        return signer

    def _move_to_historical(self, coid: int) -> None:
        order = self._active.pop(coid, None)
        if order is not None:
            self._historical[coid] = order

    def _set_status(
        self,
        order: ManagedOrder,
        new_status: str,
        reason: str = "",
    ) -> None:
        """Mutate ``order.status`` and log every real transition.

        Centralising the assignment gives post-hoc forensics a single
        reliable trail of every state change. Log only when the value
        actually changes — no-op transitions are spammy and useless.
        """
        old = order.status
        if old != new_status:
            logger.info(
                "order status: coid=%d %s → %s (reason=%s)",
                order.client_order_index,
                old,
                new_status,
                reason or "ws/sync",
            )
        order.status = new_status

    def _bump_terminal_stats(self, status: str) -> None:
        if status == "filled":
            self._stats["lifetime_filled"] += 1
        elif status == "cancelled":
            self._stats["lifetime_cancelled"] += 1
        elif status == "rejected":
            self._stats["lifetime_rejected"] += 1
        elif status == "expired":
            self._stats["lifetime_expired"] += 1

    def _apply_fill_to_inventory(
        self, side: str, size: Decimal, price: Decimal
    ) -> None:
        """Update aggregated inventory base + weighted avg entry.

        Same logic as ``strategy.paper_simulator._apply_fill``: add to
        position re-weights the average; closing partial keeps the
        average; closing across zero resets the average to the new
        fill price; closing flat clears it.
        """
        if size <= 0 or price <= 0:
            return
        signed = size if side == "buy" else -size
        prior_base = self._inventory_base
        new_base = prior_base + signed

        if prior_base == 0:
            self._inventory_avg_price = price
        elif (prior_base > 0 and signed > 0) or (prior_base < 0 and signed < 0):
            old_notional = abs(prior_base) * (
                self._inventory_avg_price
                if self._inventory_avg_price is not None
                else Decimal(0)
            )
            new_notional = size * price
            self._inventory_avg_price = (old_notional + new_notional) / (
                abs(prior_base) + size
            )
        else:
            # Reducing the position. If we crossed zero the residual
            # came from this fill and sits at this price.
            if (prior_base > 0 and new_base < 0) or (prior_base < 0 and new_base > 0):
                self._inventory_avg_price = price
            elif new_base == 0:
                self._inventory_avg_price = None
            # else: average stays put (partial close at any price
            # doesn't change the cost basis on the unclosed remainder).

        self._inventory_base = new_base

    def _signal_fill(self) -> None:
        """Raise the fill-signal flag the quoter polls each tick.

        Both flavours (Event + bool) are set so a poller that hasn't
        ever awaited still sees the signal via ``pop_fill_signal``.
        """
        self._fill_signal_flag = True
        try:
            self._fill_signal_event.set()
        except RuntimeError:
            # Extremely rare — only happens if the Event was bound to
            # a now-closed loop. The bool fallback covers it.
            logger.debug("fill_signal_event.set() failed; bool flag still set")


def _check_sdk_result(result: Any) -> None:
    """Raise LighterSDKError if the SDK returned its (None, None, err) tuple.

    Lighter's signer_client returns 3-tuples for success/failure
    rather than raising. Centralising the check keeps the retry
    code simple.
    """
    if isinstance(result, tuple) and len(result) >= 3 and result[2] is not None:
        raise LighterSDKError(str(result[2]))
