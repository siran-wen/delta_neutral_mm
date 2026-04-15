#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quoter skew 逻辑验证脚本

直接 import InventoryTracker / Quoter，通过构造不同 inventory 状态
验证 compute 方法的 skew 计算方向和幅度。

运行: python tests/test_skew_verify.py
"""

import sys
import os

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.inventory import InventoryTracker
from execution.quoter import Quoter

# ── 公共常量 ──────────────────────────────────────────────────────────────
SYMBOL = "BTC/USDC:USDC"
MID = 100_000.0
SPREAD_PCT = 0.001        # half_spread = 100
ORDER_SIZE_USD = 50.0
MAX_DELTA = 1.0
EPSILON = 1e-10

DELTAS = [-1.5, -1.0, -0.5, -0.25, 0.0, 0.25, 0.5, 1.0, 1.5]
SKEW_INTENSITIES = [0.0, 0.5, 1.0]
SPREAD_PENALTIES = [0.0, 0.5]


def make_quoter(skew_intensity: float, spread_penalty: float, delta: float):
    """创建设定好 delta 的 Quoter 实例"""
    inv = InventoryTracker()
    if delta > 0:
        inv.on_fill(SYMBOL, "buy", abs(delta), MID)
    elif delta < 0:
        inv.on_fill(SYMBOL, "sell", abs(delta), MID)
    cfg = {
        "spread_pct": SPREAD_PCT,
        "order_size_usd": ORDER_SIZE_USD,
        "skew_intensity": skew_intensity,
        "spread_penalty_factor": spread_penalty,
    }
    return Quoter(strategy_cfg=cfg, inventory=inv, max_delta_per_symbol=MAX_DELTA)


def compute_for(skew_intensity: float, spread_penalty: float, delta: float):
    """返回 (bid, ask, size)"""
    q = make_quoter(skew_intensity, spread_penalty, delta)
    return q.compute(SYMBOL, MID)


# =========================================================================
# Part 1: 参数扫描表格
# =========================================================================
def print_tables():
    print("=" * 100)
    print("Part 1: 参数扫描表格")
    print(f"  固定参数: mid={MID}, spread_pct={SPREAD_PCT}, "
          f"order_size_usd={ORDER_SIZE_USD}, max_delta={MAX_DELTA}")
    print("=" * 100)

    for si in SKEW_INTENSITIES:
        for sp in SPREAD_PENALTIES:
            print(f"\n  skew_intensity={si}, spread_penalty_factor={sp}")
            print("  " + "-" * 94)
            print(f"  {'delta':>7} | {'bid':>14} | {'ask':>14} | "
                  f"{'spread':>10} | {'bid_offset':>12} | {'ask_offset':>12}")
            print("  " + "-" * 94)

            # delta=0 时的基准值
            bid0, ask0, _ = compute_for(si, sp, 0.0)

            for d in DELTAS:
                bid, ask, _ = compute_for(si, sp, d)
                spread = ask - bid
                bid_off = bid - bid0
                ask_off = ask - ask0
                print(f"  {d:>+7.2f} | {bid:>14.4f} | {ask:>14.4f} | "
                      f"{spread:>10.4f} | {bid_off:>+12.4f} | {ask_off:>+12.4f}")

            print("  " + "-" * 94)


# =========================================================================
# Part 2: 自动化断言
# =========================================================================
class SkewVerifyError(Exception):
    pass


def assert_close(a, b, msg):
    if abs(a - b) > EPSILON:
        print(f"FAIL: {msg}")
        print(f"  期望: {b}")
        print(f"  实际: {a}")
        print(f"  差异: {abs(a - b)}")
        raise SkewVerifyError(msg)


def run_assertions():
    print("\n" + "=" * 100)
    print("Part 2: 自动化断言")
    print("=" * 100)
    passed = 0

    # ── 1. 零退化 ────────────────────────────────────────────────────────
    half_spread = MID * SPREAD_PCT
    expected_bid = MID - half_spread
    expected_ask = MID + half_spread
    expected_size = ORDER_SIZE_USD / MID

    for d in DELTAS:
        bid, ask, size = compute_for(0.0, 0.0, d)
        assert_close(bid, expected_bid,
                     f"零退化 bid: delta={d}, bid={bid}, expected={expected_bid}")
        assert_close(ask, expected_ask,
                     f"零退化 ask: delta={d}, ask={ask}, expected={expected_ask}")
        assert_close(size, expected_size,
                     f"零退化 size: delta={d}, size={size}, expected={expected_size}")
    passed += 1
    print(f"  [PASS] 1/7 零退化: skew_intensity=0, spread_penalty=0 时输出与 mid ± half_spread 完全一致")

    # ── 2. 方向正确（多头）────────────────────────────────────────────────
    bid_base, ask_base, _ = compute_for(0.0, 0.0, 0.0)
    for si in [0.5, 1.0]:
        for d in [0.25, 0.5, 1.0, 1.5]:
            bid, ask, _ = compute_for(si, 0.0, d)
            assert bid < bid_base, (
                f"方向（多头）bid 应低于基准: delta={d}, si={si}, "
                f"bid={bid}, base={bid_base}")
            assert ask < ask_base, (
                f"方向（多头）ask 应低于基准: delta={d}, si={si}, "
                f"ask={ask}, base={ask_base}")
    passed += 1
    print(f"  [PASS] 2/7 方向正确（多头）: delta>0 时 bid 和 ask 均低于无 skew 基准")

    # ── 3. 方向正确（空头）────────────────────────────────────────────────
    for si in [0.5, 1.0]:
        for d in [-0.25, -0.5, -1.0, -1.5]:
            bid, ask, _ = compute_for(si, 0.0, d)
            assert bid > bid_base, (
                f"方向（空头）bid 应高于基准: delta={d}, si={si}, "
                f"bid={bid}, base={bid_base}")
            assert ask > ask_base, (
                f"方向（空头）ask 应高于基准: delta={d}, si={si}, "
                f"ask={ask}, base={ask_base}")
    passed += 1
    print(f"  [PASS] 3/7 方向正确（空头）: delta<0 时 bid 和 ask 均高于无 skew 基准")

    # ── 4. clamp 生效 ────────────────────────────────────────────────────
    for si in SKEW_INTENSITIES:
        for sp in SPREAD_PENALTIES:
            bid15, ask15, size15 = compute_for(si, sp, 1.5)
            bid10, ask10, size10 = compute_for(si, sp, 1.0)
            assert_close(bid15, bid10,
                         f"clamp(+): si={si}, sp={sp}, bid@1.5={bid15}, bid@1.0={bid10}")
            assert_close(ask15, ask10,
                         f"clamp(+): si={si}, sp={sp}, ask@1.5={ask15}, ask@1.0={ask10}")

            bidn15, askn15, _ = compute_for(si, sp, -1.5)
            bidn10, askn10, _ = compute_for(si, sp, -1.0)
            assert_close(bidn15, bidn10,
                         f"clamp(-): si={si}, sp={sp}, bid@-1.5={bidn15}, bid@-1.0={bidn10}")
            assert_close(askn15, askn10,
                         f"clamp(-): si={si}, sp={sp}, ask@-1.5={askn15}, ask@-1.0={askn10}")
    passed += 1
    print(f"  [PASS] 4/7 clamp 生效: delta=±1.5 与 delta=±1.0 输出完全一致")

    # ── 5. 价差单调递增 ──────────────────────────────────────────────────
    for si in SKEW_INTENSITIES:
        # spread_penalty_factor > 0 时，|delta| 越大 spread 越宽
        sp = 0.5
        sorted_deltas = [0.0, 0.25, 0.5, 1.0]
        spreads = []
        for d in sorted_deltas:
            bid, ask, _ = compute_for(si, sp, d)
            spreads.append(ask - bid)
        for i in range(1, len(spreads)):
            assert spreads[i] >= spreads[i - 1] - EPSILON, (
                f"价差单调递增: si={si}, sp={sp}, "
                f"|delta|={sorted_deltas[i]} spread={spreads[i]:.6f} < "
                f"|delta|={sorted_deltas[i-1]} spread={spreads[i-1]:.6f}")
    passed += 1
    print(f"  [PASS] 5/7 价差单调递增: spread_penalty>0 时 |delta| 越大 spread 越宽")

    # ── 6. 对称性 ────────────────────────────────────────────────────────
    for si in SKEW_INTENSITIES:
        for sp in SPREAD_PENALTIES:
            for d in [0.25, 0.5, 1.0, 1.5]:
                bid_pos, ask_pos, _ = compute_for(si, sp, d)
                bid_neg, ask_neg, _ = compute_for(si, sp, -d)
                spread_pos = ask_pos - bid_pos
                spread_neg = ask_neg - bid_neg
                assert_close(spread_pos, spread_neg,
                             f"对称性: si={si}, sp={sp}, delta=±{d}, "
                             f"spread(+)={spread_pos:.6f}, spread(-)={spread_neg:.6f}")
    passed += 1
    print(f"  [PASS] 6/7 对称性: delta=+x 与 delta=-x 的 spread 相同")

    # ── 7. size 不变 ─────────────────────────────────────────────────────
    expected_size = ORDER_SIZE_USD / MID
    for si in SKEW_INTENSITIES:
        for sp in SPREAD_PENALTIES:
            for d in DELTAS:
                _, _, size = compute_for(si, sp, d)
                assert_close(size, expected_size,
                             f"size 不变: si={si}, sp={sp}, delta={d}, "
                             f"size={size}, expected={expected_size}")
    passed += 1
    print(f"  [PASS] 7/7 size 不变: 所有参数组合下 size 一致")

    return passed


# =========================================================================
# Part 3: 入口
# =========================================================================
if __name__ == "__main__":
    print_tables()
    try:
        n = run_assertions()
        print(f"\n{'=' * 100}")
        print(f"全部验证通过 ({n}/7)")
        print(f"{'=' * 100}")
    except (SkewVerifyError, AssertionError) as e:
        print(f"\n验证失败: {e}")
        sys.exit(1)
