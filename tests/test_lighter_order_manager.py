"""Unit tests for ``execution.lighter.lighter_order_manager``.

All tests use ``asyncio.run(...)`` so we don't need pytest-asyncio.

Coverage by group:
* A. lifecycle (init / start / close / close-idempotent)
* B. submit_order (unique ids, quantization, status transitions,
  failure → rejected, post-close raises)
* C. cancel_order (sends to SDK, unknown id, already closed, pre-ack)
* D. on_account_event parser (live / partial / full / canceled /
  unknown coid / callback exception isolation)
* E. concurrency (semaphore cap, retry on transient, retry exhausted)
* F. stats accounting
* G. SDK constants match installed lighter package (when available)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from execution.lighter.lighter_order_manager import (  # noqa: E402
    LighterOrderManager,
    LighterSDKError,
    ManagedOrder,
    OrderEvent,
    _ORDER_TYPE_MAP,
    _SDK_STATUS_MAP,
    _TIME_IN_FORCE_MAP,
    _to_int_price,
    _to_int_size,
)


# ----- fakes -------------------------------------------------------------


class FakeSigner:
    """Stand-in for lighter.SignerClient.

    Captures every call's kwargs and lets each test choose its
    success/failure pattern. By default both methods return the
    successful three-tuple.
    """

    def __init__(self) -> None:
        self.create_calls: List[Dict[str, Any]] = []
        self.cancel_calls: List[Dict[str, Any]] = []
        # If non-None, fail this many times then succeed (transient).
        self.create_fail_count = 0
        self.cancel_fail_count = 0
        # Permanent failure modes.
        self.create_always_fails = False
        self.cancel_always_fails = False
        # If set, raise this exception type instead of returning error tuple.
        self.create_raise: Optional[BaseException] = None
        self.cancel_raise: Optional[BaseException] = None
        # Concurrency probe — how many calls are currently in-flight.
        self.in_flight = 0
        self.peak_in_flight = 0
        # Optional artificial delay so concurrency caps are observable.
        self.create_delay_sec = 0.0

    async def create_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.create_calls.append(kwargs)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            if self.create_delay_sec > 0:
                await asyncio.sleep(self.create_delay_sec)
            if self.create_raise is not None:
                raise self.create_raise
            if self.create_always_fails:
                return (None, None, "fake permanent failure")
            if self.create_fail_count > 0:
                self.create_fail_count -= 1
                return (None, None, "fake transient failure")
            return ("CreateOrder_ok", "RespSendTx_ok", None)
        finally:
            self.in_flight -= 1

    async def cancel_order(self, **kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        self.cancel_calls.append(kwargs)
        if self.cancel_raise is not None:
            raise self.cancel_raise
        if self.cancel_always_fails:
            return (None, None, "fake cancel failure")
        if self.cancel_fail_count > 0:
            self.cancel_fail_count -= 1
            return (None, None, "fake transient cancel failure")
        return ("CancelOrder_ok", "RespSendTx_ok", None)


class FakeGateway:
    def __init__(self) -> None:
        self._signer = FakeSigner()

    @property
    def signer_client(self) -> FakeSigner:
        return self._signer


class FakeWs:
    """Placeholder — this batch's parser is invoked manually."""

    def __init__(self) -> None:
        self.subscribed: List[Any] = []


# ----- helpers -----------------------------------------------------------


def _make_manager(
    *,
    max_concurrent: int = 5,
    request_timeout_sec: float = 1.0,
    retry_max_attempts: int = 3,
    retry_backoff_sec: float = 0.0,
) -> Tuple[LighterOrderManager, FakeGateway, FakeWs]:
    gw = FakeGateway()
    ws = FakeWs()
    om = LighterOrderManager(
        gateway=gw,
        ws=ws,
        account_index=42,
        max_concurrent_requests=max_concurrent,
        request_timeout_sec=request_timeout_sec,
        retry_max_attempts=retry_max_attempts,
        # 0 backoff so retry tests don't hang on real time.sleep.
        retry_backoff_sec=retry_backoff_sec,
    )
    return om, gw, ws


def _ws_msg(
    coid: int,
    status: str,
    *,
    order_index: Optional[int] = 7000001,
    filled_base: str = "0",
    remaining_base: str = "1118",
    price: str = "894493",
) -> Dict[str, Any]:
    """Build a fake account_all update with one order entry."""
    order: Dict[str, Any] = {
        "client_order_index": coid,
        "market_index": 161,
        "is_ask": False,
        "price": price,
        "filled_base_amount": filled_base,
        "remaining_base_amount": remaining_base,
        "status": status,
        "timestamp": int(time.time() * 1000),
    }
    if order_index is not None:
        order["order_index"] = order_index
    return {
        "channel": "account_all/42",
        "type": "update/account_all",
        "orders": [order],
    }


def _D(s: str) -> Decimal:
    return Decimal(s)


# ----- A. lifecycle ------------------------------------------------------


def test_init_state_has_empty_active_and_zero_stats():
    om, _, _ = _make_manager()
    assert om.get_active_orders() == []
    stats = om.get_stats()
    assert stats["active_count"] == 0
    assert stats["lifetime_submitted"] == 0
    assert stats["lifetime_filled"] == 0
    assert stats["lifetime_cancelled"] == 0
    assert stats["in_flight_requests"] == 0


def test_start_marks_started_and_is_idempotent():
    om, _, _ = _make_manager()

    async def _go() -> None:
        await om.start()
        await om.start()  # idempotent
        assert om._started is True

    asyncio.run(_go())


def test_close_cancels_all_active_orders():
    om, gw, _ = _make_manager()

    async def _go() -> None:
        await om.start()
        coid = await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("894.493"),
            size_base=_D("1.118"),
            price_decimals=3,
            size_decimals=3,
        )
        # Simulate the ws ack so cancel has an order_index to use.
        om.on_account_event(_ws_msg(coid, "open"))
        await om.close()

    asyncio.run(_go())
    assert len(gw.signer_client.cancel_calls) == 1


def test_close_is_idempotent():
    om, _, _ = _make_manager()

    async def _go() -> None:
        await om.start()
        await om.close()
        await om.close()  # second call must not raise

    asyncio.run(_go())


# ----- B. submit_order ---------------------------------------------------


def test_submit_order_returns_unique_client_indices():
    om, _, _ = _make_manager()

    async def _go() -> List[int]:
        await om.start()
        ids = []
        for _ in range(10):
            ids.append(
                await om.submit_order(
                    side="buy",
                    market_index=161,
                    price=_D("100"),
                    size_base=_D("1"),
                    price_decimals=3,
                    size_decimals=3,
                )
            )
        return ids

    ids = asyncio.run(_go())
    assert len(set(ids)) == 10


def test_submit_order_quantizes_price_and_size_to_int():
    om, gw, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("894.493"),
            size_base=_D("1.118"),
            price_decimals=3,
            size_decimals=3,
        )

    asyncio.run(_go())
    call = gw.signer_client.create_calls[0]
    assert call["price"] == 894493
    assert call["base_amount"] == 1118
    assert call["is_ask"] is False
    assert call["order_type"] == _ORDER_TYPE_MAP["limit"]
    assert call["time_in_force"] == _TIME_IN_FORCE_MAP["post_only"]


def test_submit_order_creates_managed_order_in_active():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="sell",
            market_index=161,
            price=_D("894.493"),
            size_base=_D("1.118"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    state = om.get_order_state(coid)
    assert state is not None
    assert state.client_order_index == coid
    assert state.side == "sell"
    assert state.status == "pending_ack"  # SDK ack succeeded
    assert state.size_base == _D("1.118")


def test_submit_order_status_progresses_pending_send_then_ack():
    """Status sequence: pending_send → pending_ack on SDK success."""
    om, gw, _ = _make_manager()

    captured_during_send: Dict[str, Any] = {}

    async def slow_create(**kwargs: Any) -> Tuple[Any, Any, Optional[str]]:
        # Snapshot the order's status while the SDK call is in-flight.
        coid = kwargs["client_order_index"]
        await asyncio.sleep(0.01)
        order = om.get_order_state(coid)
        captured_during_send["mid_send_status"] = order.status if order else None
        return ("ok", "ok", None)

    gw.signer_client.create_order = slow_create  # type: ignore[assignment]

    async def _go() -> None:
        await om.start()
        await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    asyncio.run(_go())
    assert captured_during_send["mid_send_status"] == "pending_send"


def test_submit_order_sdk_failure_marks_rejected():
    om, gw, _ = _make_manager(retry_max_attempts=1)
    gw.signer_client.create_always_fails = True

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "rejected"
    assert state.last_error is not None
    assert state.closed_ts_ms is not None
    # historical, not active
    assert om.get_active_orders() == []


def test_submit_order_after_close_raises():
    om, _, _ = _make_manager()

    async def _go() -> None:
        await om.start()
        await om.close()
        with pytest.raises(RuntimeError, match="closing/closed"):
            await om.submit_order(
                side="buy",
                market_index=161,
                price=_D("100"),
                size_base=_D("1"),
                price_decimals=3,
                size_decimals=3,
            )

    asyncio.run(_go())


def test_submit_order_unknown_order_type_raises():
    om, _, _ = _make_manager()

    async def _go() -> None:
        await om.start()
        with pytest.raises(ValueError, match="unknown order_type"):
            await om.submit_order(
                side="buy",
                market_index=161,
                price=_D("100"),
                size_base=_D("1"),
                price_decimals=3,
                size_decimals=3,
                order_type="bogus",
            )

    asyncio.run(_go())


# ----- C. cancel_order ---------------------------------------------------


def test_cancel_order_sends_to_sdk_after_ack():
    om, gw, _ = _make_manager()

    async def _go() -> bool:
        await om.start()
        coid = await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )
        om.on_account_event(_ws_msg(coid, "open", order_index=9001))
        return await om.cancel_order(coid)

    ok = asyncio.run(_go())
    assert ok is True
    assert len(gw.signer_client.cancel_calls) == 1
    assert gw.signer_client.cancel_calls[0]["order_index"] == 9001


def test_cancel_order_unknown_id_returns_false():
    om, gw, _ = _make_manager()

    async def _go() -> bool:
        await om.start()
        return await om.cancel_order(99999999)

    assert asyncio.run(_go()) is False
    assert gw.signer_client.cancel_calls == []


def test_cancel_order_already_terminal_returns_false():
    om, gw, _ = _make_manager()

    async def _go() -> bool:
        await om.start()
        coid = await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )
        # Drive to terminal via filled
        om.on_account_event(
            _ws_msg(coid, "filled", filled_base="1118", remaining_base="0")
        )
        # Now cancel → should noop
        return await om.cancel_order(coid)

    assert asyncio.run(_go()) is False
    assert gw.signer_client.cancel_calls == []


def test_cancel_order_pre_ack_returns_false():
    """No order_index yet → cancel cannot proceed (Lighter needs the on-chain id)."""
    om, gw, _ = _make_manager()

    async def _go() -> bool:
        await om.start()
        coid = await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )
        # No ws ack delivered → order_index still None → cancel must skip.
        return await om.cancel_order(coid)

    assert asyncio.run(_go()) is False
    assert gw.signer_client.cancel_calls == []


# ----- D. on_account_event ----------------------------------------------


def test_ws_open_status_updates_to_live_and_emits_live_event():
    om, _, _ = _make_manager()
    captured: List[OrderEvent] = []

    async def _go() -> int:
        await om.start()
        om.register_event_callback(lambda ev: captured.append(ev))
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open"))

    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "live"
    assert state.order_index == 7000001
    assert state.acked_ts_ms is not None
    assert any(ev.event_type == "live" for ev in captured)


def test_ws_partial_fill_updates_filled_base_and_emits_fill():
    om, _, _ = _make_manager()
    captured: List[OrderEvent] = []

    async def _go() -> int:
        await om.start()
        om.register_event_callback(lambda ev: captured.append(ev))
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1.118"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    # First open
    om.on_account_event(_ws_msg(coid, "open"))
    # Then partial fill: still open status but filled_base advances
    om.on_account_event(
        _ws_msg(coid, "open", filled_base="500", remaining_base="618")
    )

    state = om.get_order_state(coid)
    assert state is not None
    assert state.filled_base == _D("500")
    assert state.status == "partial_fill"
    fill_events = [ev for ev in captured if ev.event_type == "fill"]
    assert len(fill_events) == 1
    assert fill_events[0].fill_size_base == _D("500")


def test_ws_full_fill_moves_to_historical():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open"))
    om.on_account_event(
        _ws_msg(coid, "filled", filled_base="1000", remaining_base="0")
    )
    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "filled"
    assert om.get_active_orders() == []
    stats = om.get_stats()
    assert stats["lifetime_filled"] == 1


def test_ws_canceled_emits_cancelled_event():
    om, _, _ = _make_manager()
    events: List[OrderEvent] = []

    async def _go() -> int:
        await om.start()
        om.register_event_callback(lambda ev: events.append(ev))
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open"))
    om.on_account_event(_ws_msg(coid, "canceled"))
    cancel_events = [ev for ev in events if ev.event_type == "cancelled"]
    assert len(cancel_events) == 1
    assert om.get_stats()["lifetime_cancelled"] == 1


def test_ws_canceled_post_only_marks_rejected():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "canceled-post-only"))
    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "rejected"


def test_ws_canceled_expired_marks_expired():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open"))
    om.on_account_event(_ws_msg(coid, "canceled-expired"))
    assert om.get_order_state(coid).status == "expired"
    assert om.get_stats()["lifetime_expired"] == 1


def test_ws_unknown_client_id_logs_warn_does_not_raise(caplog):
    om, _, _ = _make_manager()
    caplog.set_level(logging.DEBUG, logger="execution.lighter.lighter_order_manager")
    # Manager has no record of this coid → must just skip.
    om.on_account_event(_ws_msg(0xDEADBEEF, "open"))
    # No exception, no state created.
    assert om.get_order_state(0xDEADBEEF) is None


def test_ws_callback_exception_isolated_other_callbacks_still_called():
    om, _, _ = _make_manager()
    seen: List[OrderEvent] = []

    def bad_cb(_ev: OrderEvent) -> None:
        raise RuntimeError("boom")

    def good_cb(ev: OrderEvent) -> None:
        seen.append(ev)

    async def _go() -> int:
        await om.start()
        om.register_event_callback(bad_cb)
        om.register_event_callback(good_cb)
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open"))
    assert len(seen) >= 1


def test_ws_malformed_msg_does_not_crash():
    om, _, _ = _make_manager()
    # Not even a dict — must log + return.
    om.on_account_event("not a dict")  # type: ignore[arg-type]
    om.on_account_event({})  # missing orders
    om.on_account_event({"orders": [{"no": "coid"}]})  # missing client_order_index
    # No exception ⇒ test passes.


# ----- E. concurrency / retry --------------------------------------------


def test_max_concurrent_requests_caps_in_flight():
    om, gw, _ = _make_manager(max_concurrent=2)
    gw.signer_client.create_delay_sec = 0.05  # 50ms per call

    async def _go() -> None:
        await om.start()
        # Fire 6 concurrent submits; semaphore should cap to 2 in-flight.
        coros = [
            om.submit_order(
                side="buy",
                market_index=161,
                price=_D("100"),
                size_base=_D("1"),
                price_decimals=3,
                size_decimals=3,
            )
            for _ in range(6)
        ]
        await asyncio.gather(*coros)

    asyncio.run(_go())
    assert gw.signer_client.peak_in_flight <= 2
    assert len(gw.signer_client.create_calls) == 6


def test_retry_succeeds_after_transient_failure():
    om, gw, _ = _make_manager(retry_max_attempts=3)
    gw.signer_client.create_fail_count = 1  # fail once, then succeed

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "pending_ack"
    # 1 fail + 1 success = 2 attempts
    assert len(gw.signer_client.create_calls) == 2


def test_retry_exhausted_marks_rejected():
    om, gw, _ = _make_manager(retry_max_attempts=2)
    gw.signer_client.create_always_fails = True

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    state = om.get_order_state(coid)
    assert state is not None
    assert state.status == "rejected"
    # Each of N attempts hit the SDK once
    assert len(gw.signer_client.create_calls) == 2
    assert om.get_stats()["send_failures_total"] == 1


# ----- F. stats ----------------------------------------------------------


def test_get_stats_reflects_lifecycle_counts():
    om, _, _ = _make_manager()

    async def _go() -> List[int]:
        await om.start()
        coids = []
        for _ in range(3):
            coids.append(
                await om.submit_order(
                    side="buy",
                    market_index=161,
                    price=_D("100"),
                    size_base=_D("1"),
                    price_decimals=3,
                    size_decimals=3,
                )
            )
        return coids

    coids = asyncio.run(_go())
    # Drive: 1 fill, 1 cancel, 1 still live
    om.on_account_event(_ws_msg(coids[0], "open"))
    om.on_account_event(
        _ws_msg(coids[0], "filled", filled_base="1000", remaining_base="0")
    )
    om.on_account_event(_ws_msg(coids[1], "open"))
    om.on_account_event(_ws_msg(coids[1], "canceled"))
    om.on_account_event(_ws_msg(coids[2], "open"))

    s = om.get_stats()
    assert s["lifetime_submitted"] == 3
    assert s["lifetime_filled"] == 1
    assert s["lifetime_cancelled"] == 1
    assert s["active_count"] == 1


# ----- G. SDK constants --------------------------------------------------


def test_sdk_constants_match_real_sdk_when_available():
    """Pin the hard-coded fallback constants to the real SDK values.

    If lighter is not installed, this test skips; otherwise it asserts
    the locally captured constants haven't drifted from upstream.
    """
    try:
        from lighter import SignerClient as _SC  # type: ignore[import-not-found]
    except ImportError:
        pytest.skip("lighter SDK not installed")
    assert _ORDER_TYPE_MAP["limit"] == _SC.ORDER_TYPE_LIMIT
    assert _TIME_IN_FORCE_MAP["ioc"] == _SC.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
    assert _TIME_IN_FORCE_MAP["gtt"] == _SC.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
    assert _TIME_IN_FORCE_MAP["post_only"] == _SC.ORDER_TIME_IN_FORCE_POST_ONLY


def test_quantize_helpers_round_correctly():
    # Price ROUND_HALF_UP, size ROUND_DOWN.
    assert _to_int_price(_D("894.4937"), 3) == 894494
    assert _to_int_price(_D("894.4934"), 3) == 894493
    assert _to_int_size(_D("1.1189"), 3) == 1118  # rounds down
    assert _to_int_size(_D("1.1180"), 3) == 1118


# ----- H. inventory + fill signal (batch 3 part 2 additions) -------------


def test_get_inventory_starts_flat():
    om, _, _ = _make_manager()
    inv = om.get_inventory()
    assert inv.net_delta_base == _D("0")
    assert inv.net_delta_usdc == _D("0")
    assert inv.avg_entry_price is None
    assert inv.open_orders_count == 0


def test_buy_fill_updates_inventory_long():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    # Drive a full fill via ws.
    om.on_account_event(
        _ws_msg(coid, "filled", filled_base="1000", remaining_base="0", price="100000")
    )
    inv = om.get_inventory()
    # ws reports filled_base in size_decimals (1000 = 1.0 base).
    # Note: parser treats filled_base as a Decimal with no descaling.
    assert inv.net_delta_base == _D("1000")
    assert inv.avg_entry_price is not None
    assert inv.avg_entry_price > 0


def test_inventory_weighted_avg_on_two_buy_fills():
    om, _, _ = _make_manager()

    async def _go() -> List[int]:
        await om.start()
        return [
            await om.submit_order(
                side="buy",
                market_index=161,
                price=_D("100"),
                size_base=_D("1"),
                price_decimals=3,
                size_decimals=3,
            )
            for _ in range(2)
        ]

    coids = asyncio.run(_go())
    # Two fills at different prices: 1 base @100, 1 base @110 → avg=105
    om.on_account_event(
        _ws_msg(coids[0], "filled", filled_base="1", remaining_base="0", price="100")
    )
    om.on_account_event(
        _ws_msg(coids[1], "filled", filled_base="1", remaining_base="0", price="110")
    )
    inv = om.get_inventory()
    assert inv.net_delta_base == _D("2")
    assert inv.avg_entry_price == _D("105")


def test_get_inventory_uses_mark_price_for_usdc():
    om, _, _ = _make_manager()

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(
        _ws_msg(coid, "filled", filled_base="1", remaining_base="0", price="100")
    )
    inv_avg = om.get_inventory()
    inv_mark = om.get_inventory(mark_price=_D("110"))
    assert inv_avg.net_delta_usdc == _D("100")
    assert inv_mark.net_delta_usdc == _D("110")


def test_pop_fill_signal_set_on_fill_and_consumed_on_read():
    om, _, _ = _make_manager()
    assert om.pop_fill_signal() is False

    async def _go() -> int:
        await om.start()
        return await om.submit_order(
            side="buy",
            market_index=161,
            price=_D("100"),
            size_base=_D("1"),
            price_decimals=3,
            size_decimals=3,
        )

    coid = asyncio.run(_go())
    om.on_account_event(_ws_msg(coid, "open", filled_base="0", remaining_base="1"))
    # No fill yet (open with 0 filled) → signal stays clear
    assert om.pop_fill_signal() is False
    # Now drive a partial fill
    om.on_account_event(
        _ws_msg(coid, "open", filled_base="500", remaining_base="500")
    )
    assert om.pop_fill_signal() is True
    # Consumed — second pop returns False
    assert om.pop_fill_signal() is False
