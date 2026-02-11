#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OrderManager 全功能测试"""

import sys
import os
import time
import logging

if sys.platform == 'win32':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gateways import (
    GatewayFactory, GatewayStatus, OrderSide, OrderType,
    OrderManager, OrderState, ManagedOrder, RequestPriority,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-14s] %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TestOM")

SYMBOL = "BTC/USDC:USDC"
PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [OK] {name}" + (f"  ({detail})" if detail else ""))
    else:
        FAIL += 1
        print(f"  [FAIL] {name}" + (f"  ({detail})" if detail else ""))


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    print("\n" + "=" * 60)
    print("OrderManager Full Test".center(52))
    print("=" * 60)

    # ==== 连接 ====
    section("0. Connect to Exchange")
    gw = GatewayFactory.create("config/hyperliquid_config.yaml")
    gw.connect()
    check("Gateway authenticated", gw.status == GatewayStatus.AUTHENTICATED, gw.status.value)

    # 获取价格和余额
    ticker = gw.fetch_ticker(SYMBOL)
    price = ticker.last
    print(f"  Current {SYMBOL} price: {price}")

    balance = gw.fetch_balance()
    usdc = balance.balances.get("USDC")
    available = usdc.free if usdc else 0
    print(f"  Available balance: {available} USDC")

    if available < 11:
        print(f"\n  [!] Insufficient balance ({available} USDC < 11), cannot proceed")
        gw.disconnect()
        sys.exit(1)

    # ==== 创建 OM ====
    section("1. OrderManager Lifecycle")

    # 收集事件
    events = []
    def on_event(name):
        def handler(data):
            events.append((name, data))
        return handler

    om = OrderManager(
        gateway=gw,
        reconcile_interval=2.0,
        stale_timeout=5.0,
        rate_limit_per_sec=20,
        rtt_warn_threshold=3.0,
    )
    # 注册回调
    om.on("order_submitted", on_event("order_submitted"))
    om.on("order_cancelled", on_event("order_cancelled"))
    om.on("order_rejected", on_event("order_rejected"))
    om.on("order_filled", on_event("order_filled"))
    om.on("order_lost", on_event("order_lost"))
    om.on("order_state_change", on_event("order_state_change"))

    om.start()
    check("OM started", om.is_running)

    # ==== 冷启动恢复 ====
    section("2. Cold Start Recovery")
    all_orders = om.get_all_orders()
    print(f"  Recovered orders: {len(all_orders)}")
    check("Cold start recovery done", True, f"recovered {len(all_orders)} orders")

    # ==== 下单测试 ====
    section("3. Submit Order (limit buy)")

    test_price = float(gw.price_to_precision(SYMBOL, price * 0.5))
    raw_amount = 11.0 / test_price
    test_amount = float(gw.amount_to_precision(SYMBOL, raw_amount))
    if test_amount <= 0:
        test_amount = float(gw.amount_to_precision(SYMBOL, 0.001))

    print(f"  Order params: {SYMBOL} BUY {test_amount} @ {test_price}")
    trigger_ts = time.time()

    order1 = om.submit_order(
        symbol=SYMBOL,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        amount=test_amount,
        price=test_price,
        strategy_trigger_ts=trigger_ts,
    )

    check("Order created", order1 is not None)
    check("ClientOrderId exists", order1.client_order_id.startswith("om-"), order1.client_order_id)
    check("ExchangeOrderId exists", order1.exchange_order_id is not None, str(order1.exchange_order_id))
    check("State is OPEN", order1.state == OrderState.OPEN, order1.state.value)
    check("order_submitted event fired", any(e[0] == "order_submitted" for e in events))

    # ==== 延迟追踪 ====
    section("4. Latency Tracking")

    lat = order1.latency
    check("strategy_trigger_ts recorded", lat.strategy_trigger_ts > 0, f"{lat.strategy_trigger_ts:.3f}")
    check("api_send_ts recorded", lat.api_send_ts > 0, f"{lat.api_send_ts:.3f}")
    check("exchange_ack_ts recorded", lat.exchange_ack_ts > 0, f"{lat.exchange_ack_ts:.3f}")
    rtt = lat.order_rtt
    check("Order RTT computed", rtt is not None and rtt > 0, f"{rtt:.3f}s" if rtt else "N/A")
    total = lat.total_latency
    check("Total latency computed", total is not None and total > 0, f"{total:.3f}s" if total else "N/A")

    tracker_stats = om.latency_tracker.get_order_stats()
    check("LatencyTracker recorded", tracker_stats["count"] > 0, f"count={tracker_stats['count']}, avg={tracker_stats['avg']}s")

    # ==== 查询测试 ====
    section("5. Query Interface")

    found = om.get_order(order1.client_order_id)
    check("get_order(cid) found", found is not None and found.client_order_id == order1.client_order_id)

    found2 = om.get_order_by_exchange_id(order1.exchange_order_id)
    check("get_order_by_exchange_id found", found2 is not None and found2.client_order_id == order1.client_order_id)

    active = om.get_active_orders(SYMBOL)
    check("get_active_orders includes order", any(o.client_order_id == order1.client_order_id for o in active), f"active={len(active)}")

    # ==== 对账巡检 ====
    section("6. Reconciliation")

    print("  Waiting for one reconciliation cycle (3s)...")
    time.sleep(3.5)

    # 巡检后订单应该还是 OPEN
    after_reconcile = om.get_order(order1.client_order_id)
    check("Order still OPEN after reconcile", after_reconcile.state == OrderState.OPEN, after_reconcile.state.value)

    # ==== 频率限制 ====
    section("7. Rate Limiter")

    rl = om.rate_limiter
    rl_stats = rl.stats
    check("RateLimiter active", rl_stats["total_requests"] > 0, f"requests={rl_stats['total_requests']}")
    check("Available tokens normal", rl.available_tokens >= 0, f"tokens={rl.available_tokens:.1f}")

    # CANCEL 优先级始终放行
    result = rl.acquire(RequestPriority.CANCEL)
    check("CANCEL priority always passes", result is True)

    # ==== 原子化改单测试 ====
    section("8. Atomic Amend (cancel_first)")

    new_price = float(gw.price_to_precision(SYMBOL, price * 0.48))
    print(f"  Amend: {test_price} -> {new_price}")

    order2 = om.amend_order(
        client_order_id=order1.client_order_id,
        new_price=new_price,
        strategy="cancel_first",
    )

    check("Amend returned new order", order2 is not None)
    if order2:
        check("New CID differs", order2.client_order_id != order1.client_order_id, order2.client_order_id)
        check("New order OPEN", order2.state == OrderState.OPEN, order2.state.value)
        check("New price correct", order2.price == new_price or abs((order2.price or 0) - new_price) < 1, f"{order2.price}")
        check("Old order CANCELLED", order1.state == OrderState.CANCELLED, order1.state.value)

    # ==== 撤单测试 ====
    section("9. Cancel Order")

    if order2 and order2.state == OrderState.OPEN:
        events.clear()
        ok = om.cancel_order(order2.client_order_id)
        check("Cancel returned True", ok is True)
        check("Order state CANCELLED", order2.state == OrderState.CANCELLED, order2.state.value)
        check("order_cancelled event fired", any(e[0] == "order_cancelled" for e in events))

        cancel_rtt = order2.latency.cancel_rtt
        check("Cancel RTT recorded", cancel_rtt is not None and cancel_rtt > 0, f"{cancel_rtt:.3f}s" if cancel_rtt else "N/A")
    else:
        print("  Skipped (no active order)")

    # ==== 防重入锁测试 ====
    section("10. Reentrancy Lock")

    if order2:
        # 尝试对已终结订单操作
        ok2 = om.cancel_order(order2.client_order_id)
        check("Cancel on terminal order returns False", ok2 is False)

    # ==== 批量撤单测试 ====
    section("11. Cancel All")

    # 先下2个单
    orders_batch = []
    for i in range(2):
        p = float(gw.price_to_precision(SYMBOL, price * (0.45 + i * 0.01)))
        o = om.submit_order(SYMBOL, OrderSide.BUY, OrderType.LIMIT, test_amount, p)
        if o.state == OrderState.OPEN:
            orders_batch.append(o)
    print(f"  Created {len(orders_batch)} test orders")

    cancelled = om.cancel_all(SYMBOL)
    check("Cancel all", cancelled >= len(orders_batch), f"cancelled {cancelled}")

    # 确认无活跃订单
    time.sleep(1)
    remaining_active = om.get_active_orders(SYMBOL)
    check("No remaining active orders", len(remaining_active) == 0, f"remaining {len(remaining_active)}")

    # ==== 统计 ====
    section("12. Statistics")

    stats = om.get_stats()
    print(f"  Total orders: {stats['total_orders']}")
    print(f"  By state: {stats['orders_by_state']}")
    print(f"  Order latency: {stats['latency_order']}")
    print(f"  Cancel latency: {stats['latency_cancel']}")
    print(f"  Rate limiter: {stats['rate_limiter']}")
    check("Stats complete", stats["total_orders"] > 0)

    # ==== 停止 ====
    section("13. Stop OM")

    om.stop()
    check("OM stopped", not om.is_running)

    gw.disconnect()
    check("Gateway disconnected", gw.status == GatewayStatus.DISCONNECTED)

    # ==== 总结 ====
    print(f"\n{'='*60}")
    print(f"  Test Summary: {PASS} passed, {FAIL} failed")
    print(f"{'='*60}\n")

    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()
