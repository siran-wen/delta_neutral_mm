#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid 现货 USOL/USDC 订单生命周期验证

目的:
    验证双腿架构做市腿的前提:Hyperliquid 现货是否支持标准 limit 挂单 + 撤单。
    只下一张远离市价的 buy limit 单 (mid * 0.5),确保不会成交。

流程:
    1. 连接 Gateway
    2. 读取 USOL/USDC 的 market 元数据 (precision / limits)
    3. 取当前 mid
    4. 下 buy limit @ mid*0.5，数量 = 最小合规量
    5. fetch_open_orders 确认出现
    6. cancel_order 撤单
    7. sleep 1s 后再次 fetch_open_orders 确认消失

脚本结束时无论成功失败都执行一次 cancel_all_orders 兜底清理。

运行:
    python tests/test_spot_order_lifecycle.py
"""

import io
import math
import os
import sys
import time
import traceback
from typing import Any, Dict, Optional, Tuple

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gateways import GatewayFactory, OrderSide, OrderType


CONFIG_PATH = "config/hyperliquid_config.yaml"
SYMBOL = "USOL/USDC"

# Hyperliquid 主流要求每笔订单名义价值 >= 10 USDC。留一点 buffer。
MIN_NOTIONAL_USDC = 11.0


def _sep(title: str = "") -> None:
    line = "=" * 72
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def _mid(ticker) -> Optional[float]:
    if ticker.bid and ticker.ask:
        return (ticker.bid + ticker.ask) / 2.0
    return ticker.last


def _compute_amount(
    gateway,
    symbol: str,
    price: float,
    market: Dict[str, Any],
) -> float:
    """
    计算下单数量:
        1. 不低于 limits.amount.min
        2. 名义价值 (amount * price) >= MIN_NOTIONAL_USDC
        3. 向上对齐到 precision.amount 的整数倍
    """
    limits = market.get("limits") or {}
    amount_limits = limits.get("amount") or {}
    min_amount = float(amount_limits.get("min") or 0.0)

    precision = market.get("precision") or {}
    # CCXT 对 Hyperliquid 返回的 precision.amount 是"最小步长"（如 0.001）
    amount_step_raw = precision.get("amount")
    try:
        amount_step = float(amount_step_raw) if amount_step_raw else 0.0
    except (TypeError, ValueError):
        amount_step = 0.0

    # 按名义价值算一个基础数量
    raw_amount = MIN_NOTIONAL_USDC / price

    # 对齐到 step 的整数倍（向上取整，确保名义达标）
    if amount_step and amount_step > 0:
        steps = math.ceil(raw_amount / amount_step)
        aligned = steps * amount_step
    else:
        aligned = raw_amount

    if aligned < min_amount:
        aligned = min_amount

    # 最后再走一次 gateway.amount_to_precision 保证字符串化
    return float(gateway.amount_to_precision(symbol, aligned))


def _try(label: str, fn, *args, **kwargs) -> Tuple[bool, str, Any]:
    """统一调用 + 错误记录"""
    print(f"\n[{label}]")
    try:
        result = fn(*args, **kwargs)
        print(f"    OK")
        return True, "OK", result
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False, f"{type(e).__name__}: {e}", None


# =============================================================================
# 主流程
# =============================================================================

def main() -> None:
    _sep("Hyperliquid 现货 USOL/USDC 订单生命周期")
    print(f"CONFIG: {CONFIG_PATH}")
    print(f"SYMBOL: {SYMBOL}")

    # ── 0. 连接 Gateway ───────────────────────────────────────────────────
    _sep("0. 连接 Gateway")
    gateway = GatewayFactory.create(CONFIG_PATH)
    if not gateway.connect():
        print("Gateway.connect() 失败,退出")
        sys.exit(1)
    print(f"已连接: {gateway.exchange_name} ({gateway.status.value})")

    # 汇总结果
    results: Dict[str, Tuple[bool, str]] = {}
    order_id: Optional[str] = None

    try:
        # ── 1. 确认 market 元数据 ─────────────────────────────────────────
        _sep(f"1. 确认 {SYMBOL} 在 markets 中,打印 precision / limits")
        markets = gateway.get_markets()
        market = markets.get(SYMBOL)
        if not market:
            print(f"FAIL: {SYMBOL} 不在 markets 中")
            results["market_meta"] = (False, "symbol 不存在")
            return

        print(f"    type        : {market.get('type')}")
        print(f"    spot        : {market.get('spot')}")
        print(f"    active      : {market.get('active')}")
        print(f"    base/quote  : {market.get('base')} / {market.get('quote')}")
        print(f"    precision   : {market.get('precision')}")
        print(f"    limits      : {market.get('limits')}")
        print(f"    taker/maker : {market.get('taker')} / {market.get('maker')}")
        results["market_meta"] = (True, f"type={market.get('type')}")

        # ── 2. 取当前 mid ─────────────────────────────────────────────────
        _sep(f"2. 获取当前 {SYMBOL} 行情")
        ticker = gateway.fetch_ticker(SYMBOL)
        mid = _mid(ticker)
        print(f"    bid={ticker.bid}  ask={ticker.ask}  last={ticker.last}  mid={mid}")
        if not mid:
            print("    FAIL: 无法确定 mid（bid/ask/last 都为空）")
            results["market_meta"] = (False, "mid 不可用")
            return

        # ── 3. 计算下单参数 ───────────────────────────────────────────────
        raw_test_price = mid * 0.5
        test_price = float(gateway.price_to_precision(SYMBOL, raw_test_price))
        test_amount = _compute_amount(gateway, SYMBOL, test_price, market)
        test_notional = test_amount * test_price

        _sep("3. 下单参数")
        print(f"    raw price   : {raw_test_price:.6f}  -> aligned: {test_price}")
        print(f"    amount      : {test_amount}")
        print(f"    notional    : ~{test_notional:.4f} USDC (要求 >= {MIN_NOTIONAL_USDC})")
        print(f"    期望行为    : 远低于 mid={mid:.4f}, 保证挂单不成交")

        if test_notional < MIN_NOTIONAL_USDC - 1e-9:
            print(f"    WARN: 名义价值不足，仍尝试下单以观察交易所具体错误")

        # ── 3.5 现货 USDC 余额预检 ────────────────────────────────────────
        # Hyperliquid 的 Perp 和 Spot 是独立账户，fetch_balance 默认查 Perp，
        # 现货下单扣的是 Spot 子账户里的 USDC，必须单独查。
        _sep("3.5 现货 USDC 余额预检")
        try:
            spot_bal = gateway.exchange.fetch_balance(params={"type": "spot"})
            spot_usdc_free = float((spot_bal.get("free") or {}).get("USDC") or 0.0)
            spot_usdc_total = float((spot_bal.get("total") or {}).get("USDC") or 0.0)
            print(f"    现货 USDC free : {spot_usdc_free}")
            print(f"    现货 USDC total: {spot_usdc_total}")
        except Exception as e:
            print(f"    现货余额查询失败: {type(e).__name__}: {e}")
            spot_usdc_free = 0.0

        if spot_usdc_free < test_notional:
            print(
                f"\n    现货 USDC 余额不足: free={spot_usdc_free:.4f} "
                f"< 下单名义 {test_notional:.4f}"
            )
            print("    原因: 账户里 99 USDC 在 Perp 子账户，Spot 子账户几乎为空。")
            print("    解决: 在 Hyperliquid 界面 Transfer → Perpetual 划转 "
                  ">= 15 USDC 到 Spot,然后重跑本脚本。")
            results["spot_funding_ready"] = (
                False,
                f"Spot USDC {spot_usdc_free:.4f} < 下单需要 {test_notional:.4f}",
            )
            # 跳过实际下单 —— 避免消耗一次无用的 API 调用并污染交易所侧日志
            print("\n    [跳过 create_order / fetch_open / cancel 步骤]")
            skip_order_steps = True
        else:
            skip_order_steps = False
            results["spot_funding_ready"] = (
                True,
                f"Spot USDC free={spot_usdc_free:.4f}",
            )

        if not skip_order_steps:
            # ── 4. create_order ────────────────────────────────────────────
            _sep("4. create_order (buy limit)")
            ok_create, msg_create, order = _try(
                "create_order",
                gateway.create_order,
                SYMBOL,
                OrderSide.BUY,
                OrderType.LIMIT,
                test_amount,
                test_price,
            )
            results["create_order"] = (ok_create, msg_create)

            if ok_create and order is not None:
                print(f"    id       : {order.id}")
                print(f"    symbol   : {order.symbol}")
                print(f"    side     : {order.side}")
                print(f"    type     : {order.type}")
                print(f"    price    : {order.price}")
                print(f"    amount   : {order.amount}")
                print(f"    status   : {order.status}")
                print(f"    filled   : {order.filled}")
                order_id = order.id

            if ok_create and order_id:
                # ── 5. fetch_open_orders ───────────────────────────────────
                _sep("5. fetch_open_orders 确认订单出现")
                time.sleep(1)  # 给交易所一点入单时间
                ok_fetch, msg_fetch, open_orders = _try(
                    "fetch_open_orders",
                    gateway.fetch_open_orders,
                    SYMBOL,
                )
                if ok_fetch:
                    print(f"    当前挂单总数: {len(open_orders)}")
                    matched = next(
                        (o for o in open_orders if str(o.id) == str(order_id)),
                        None,
                    )
                    if matched:
                        print(f"    命中订单: id={matched.id}  status={matched.status}  "
                              f"price={matched.price}  amount={matched.amount}  "
                              f"filled={matched.filled}")
                        results["fetch_open"] = (
                            True, f"在挂单列表中,状态={matched.status}"
                        )
                    else:
                        print(f"    FAIL: 订单 {order_id} 不在挂单列表里")
                        for o in open_orders[:5]:
                            print(f"      · id={o.id}  status={o.status}  price={o.price}")
                        results["fetch_open"] = (False, "订单 id 不在返回列表")
                else:
                    results["fetch_open"] = (False, msg_fetch)

                # ── 6. cancel_order ────────────────────────────────────────
                _sep("6. cancel_order")
                ok_cancel, msg_cancel, _ = _try(
                    "cancel_order",
                    gateway.cancel_order,
                    order_id,
                    SYMBOL,
                )
                results["cancel_order"] = (ok_cancel, msg_cancel)

                # ── 7. 确认已撤销 ─────────────────────────────────────────
                _sep("7. 二次 fetch_open_orders 确认已撤销")
                time.sleep(1)
                ok_confirm, msg_confirm, open_orders2 = _try(
                    "fetch_open_orders",
                    gateway.fetch_open_orders,
                    SYMBOL,
                )
                if ok_confirm:
                    still_open = any(str(o.id) == str(order_id) for o in open_orders2)
                    print(f"    当前挂单总数: {len(open_orders2)}")
                    if still_open:
                        print(f"    FAIL: 订单 {order_id} 仍在列表中")
                        results["confirm_gone"] = (False, "订单仍在挂单列表")
                    else:
                        print(f"    订单 {order_id} 已不在挂单列表")
                        results["confirm_gone"] = (True, "已撤销")
                else:
                    results["confirm_gone"] = (False, msg_confirm)
            else:
                print("\n无订单 ID,跳过 fetch_open / cancel 步骤")

    finally:
        # ── 兜底清理：无论成功失败都跑一次 cancel_all_orders ──────────────
        _sep("兜底清理: cancel_all_orders(USOL/USDC)")
        try:
            n = gateway.cancel_all_orders(SYMBOL)
            print(f"    cancel_all_orders 返回 n={n}")
        except Exception as e:
            print(f"    cancel_all_orders 失败: {type(e).__name__}: {e}")

        try:
            gateway.disconnect()
        except Exception:
            pass

    # =========================================================================
    # 汇总
    # =========================================================================
    _sep("汇总")
    order_steps = ["create_order", "fetch_open", "cancel_order", "confirm_gone"]
    print(f"{'Step':20s} {'Result':8s} Note")
    print("-" * 72)

    # 先把前置(市场元数据、资金预检)打出来
    for pre in ("market_meta", "spot_funding_ready"):
        if pre in results:
            ok, note = results[pre]
            tag = "OK" if ok else "FAIL"
            print(f"{pre:20s} {tag:8s} {note}")

    # 四个核心步骤
    for step in order_steps:
        if step in results:
            ok, note = results[step]
            tag = "OK" if ok else "FAIL"
        else:
            tag = "SKIP"
            note = "前置步骤未通过而跳过"
        print(f"{step:20s} {tag:8s} {note}")

    all_order_ok = all(
        results.get(s, (False, ""))[0] for s in order_steps
    )
    funding_ok = results.get("spot_funding_ready", (False, ""))[0]

    _sep("最终结论")
    if all_order_ok:
        print("  >> 现货 USOL/USDC 完整支持 limit 挂单 / fetch / cancel。")
        print("     可以进入 Hedger 实现阶段。")
        sys.exit(0)
    elif not funding_ok:
        # 资金未就绪 —— 无法判定下单路径,但没有证据否定其可用性
        print("  >> 未能完整验证:Spot USDC 不足,create_order 无法执行。")
        print("     API 是否支持 limit 挂单仍是 **未知**(下单路径未触达).")
        print("     操作建议:在 Hyperliquid 界面 Transfer Perpetual → Spot 划转")
        print("     >= 15 USDC 到 Spot 子账户后重跑本脚本,预计能 4 步全通。")
        sys.exit(2)
    else:
        print("  >> 现货 limit 下单链路存在问题,详见上方 FAIL 项。")
        print("     依赖该能力的 Hedger 实现需先解决这些问题。")
        sys.exit(1)


if __name__ == "__main__":
    main()
