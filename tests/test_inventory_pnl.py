# -*- coding: utf-8 -*-
"""
InventoryTracker PnL / fee / 翻仓逻辑单元测试

覆盖 4 个核心场景：
  1. 现货做市单边循环（base coin fee + USDC fee 组合）
  2. 永续开平空盈利（USDC fee）
  3. 穿越零点翻仓（realized_pnl 结算 + avg_entry 重置）
  4. 减仓不穿越零点（avg_entry 必须保持）

不依赖交易所连接、不 mock CCXT、不起网络 —— 纯 Python 计算。
所有浮点比较用 pytest.approx，禁止 ==。
"""
import pytest

from execution.inventory import InventoryTracker


@pytest.fixture
def inv():
    """每个测试独立 InventoryTracker，避免跨测试污染"""
    return InventoryTracker()


# =============================================================================
# Scenario 1: 现货 BUY/SELL 单边循环
#   base coin fee（USOL, BUY 时）+ USDC fee（SELL 时）
# =============================================================================
def test_spot_buy_sell_cycle(inv):
    # 预设 spot USDC 底仓
    inv.balances["spot"] = {"USDC": 100.0}

    # Fill 1: BUY 0.1 SOL @ 85.0, fee=0.00004 USOL（base coin 扣费）
    inv.on_fill(
        "SOL/USDC", "buy", 0.1, 85.0,
        fee_cost=0.00004, fee_currency="USOL",
    )
    # Fill 2: SELL 0.1 SOL @ 86.0, fee=0.005 USDC（quote 扣费）
    inv.on_fill(
        "SOL/USDC", "sell", 0.1, 86.0,
        fee_cost=0.005, fee_currency="USDC",
    )

    # ── 累积 fee ─────────────────────────────────────────────
    # Fill 1: USOL fee → 折算 USDC = 0.00004 × 85 = 0.0034
    # Fill 2: USDC fee 直接 = 0.005
    # 合计 = 0.0034 + 0.005 = 0.0084
    assert inv.cumulative_fees_usdc == pytest.approx(0.0084, abs=1e-6)

    # ── Realized PnL ──────────────────────────────────────────
    # Fill 1: 开仓不 realize，只扣 fee → realized = -0.0034
    # Fill 2: 平 0.1 多仓 → +0.1 × (86 - 85) = +0.1
    #         再扣 USDC fee 0.005 → realized = -0.0034 + 0.1 - 0.005 = 0.0916
    assert inv.realized_pnl == pytest.approx(0.0916, abs=1e-6)

    # ── spot[USDC] ────────────────────────────────────────────
    # 起始 100
    # Fill 1 BUY: -0.1×85 = -8.5 → 91.5（USOL fee 不动 quote）
    # Fill 2 SELL: +0.1×86 = +8.6 → 100.1
    # Fill 2 USDC fee: -0.005 → 100.095
    assert inv.balances["spot"]["USDC"] == pytest.approx(100.095, abs=1e-6)

    # ── spot[SOL] ─────────────────────────────────────────────
    # 起始 0
    # Fill 1 BUY: +0.1 → 0.1
    # Fill 1 USOL fee fallback 到 base（SOL）: -0.00004 → 0.09996
    # Fill 2 SELL: -0.1 → -0.00004
    assert inv.balances["spot"].get("SOL", 0.0) == pytest.approx(-0.00004, abs=1e-8)

    # ── 持仓归零、avg_entry 被 pop ──────────────────────────
    assert "SOL/USDC" not in inv.avg_entry
    assert inv.positions.get("SOL/USDC", 0.0) == pytest.approx(0.0, abs=1e-9)


# =============================================================================
# Scenario 2: 永续开空 → 平空盈利
#   两次 USDC fee，无 base coin fee 分支
# =============================================================================
def test_perp_open_close_short_profit(inv):
    # Fill 1: SELL 0.1 @ 86.0 开空, fee=0.01 USDC
    inv.on_fill(
        "SOL/USDC:USDC", "sell", 0.1, 86.0,
        fee_cost=0.01, fee_currency="USDC",
    )
    # Fill 2: BUY 0.1 @ 85.0 平空, fee=0.01 USDC
    inv.on_fill(
        "SOL/USDC:USDC", "buy", 0.1, 85.0,
        fee_cost=0.01, fee_currency="USDC",
    )

    # ── Realized PnL ──────────────────────────────────────────
    # Fill 1: 开仓不 realize，只扣 fee → -0.01
    # Fill 2: 平 0.1 空仓 → +0.1 × (86 − 85) = +0.1
    #         再扣 fee 0.01 → -0.01 + 0.1 - 0.01 = 0.08
    assert inv.realized_pnl == pytest.approx(0.08, abs=1e-6)

    # ── 累积 fee = 0.01 + 0.01 = 0.02 ──────────────────────
    assert inv.cumulative_fees_usdc == pytest.approx(0.02, abs=1e-6)

    # ── 持仓归零、avg_entry 被 pop ──────────────────────────
    assert inv.positions["SOL/USDC:USDC"] == pytest.approx(0.0, abs=1e-9)
    assert "SOL/USDC:USDC" not in inv.avg_entry


# =============================================================================
# Scenario 3: 穿越零点翻仓
#   +0.2 多头 → SELL 0.3 → 平 0.2 多 + 开 0.1 空
#   验证 realized_pnl 按平仓部分结算、avg_entry 重置为新方向成交价
# =============================================================================
def test_position_flip_through_zero(inv):
    # 手工 setup：已有 +0.2 多头，均价 85
    inv.positions["SOL/USDC:USDC"] = 0.2
    inv.avg_entry["SOL/USDC:USDC"] = 85.0

    inv.on_fill(
        "SOL/USDC:USDC", "sell", 0.3, 87.0,
        fee_cost=0.03, fee_currency="USDC",
    )

    # ── Realized PnL ──────────────────────────────────────────
    # 平多部分：closed = min(0.2, 0.3) = 0.2 → +0.2 × (87 − 85) = +0.4
    # Fee: -0.03 → 0.4 - 0.03 = 0.37
    # （开空 0.1 部分不 realize）
    assert inv.realized_pnl == pytest.approx(0.37, abs=1e-6)

    # ── 净持仓 = +0.2 - 0.3 = -0.1 ──────────────────────────
    assert inv.positions["SOL/USDC:USDC"] == pytest.approx(-0.1, abs=1e-9)

    # ── 翻仓后 avg_entry 重置为成交价 87（新方向以当前价为基） ──
    assert inv.avg_entry["SOL/USDC:USDC"] == pytest.approx(87.0, abs=1e-6)

    # ── 累积 fee = 0.03 ──────────────────────────────────────
    assert inv.cumulative_fees_usdc == pytest.approx(0.03, abs=1e-6)


# =============================================================================
# Scenario 4: 减仓不穿越零点
#   +0.2 多头 → SELL 0.1 → 剩 +0.1 多头
#   关键验证：avg_entry 必须保持 85 不变（没穿越零点 → 不重置）
# =============================================================================
def test_partial_close_no_flip(inv):
    # 手工 setup：已有 +0.2 多头，均价 85
    inv.positions["SOL/USDC:USDC"] = 0.2
    inv.avg_entry["SOL/USDC:USDC"] = 85.0

    inv.on_fill(
        "SOL/USDC:USDC", "sell", 0.1, 87.0,
        fee_cost=0.01, fee_currency="USDC",
    )

    # ── Realized PnL ──────────────────────────────────────────
    # 平多部分：closed = min(0.2, 0.1) = 0.1 → +0.1 × (87 − 85) = +0.2
    # Fee: -0.01 → 0.2 - 0.01 = 0.19
    assert inv.realized_pnl == pytest.approx(0.19, abs=1e-6)

    # ── 净持仓 = +0.2 - 0.1 = +0.1 ──────────────────────────
    assert inv.positions["SOL/USDC:USDC"] == pytest.approx(0.1, abs=1e-9)

    # ── 未穿越零点 → avg_entry 保持 85（不重置） ─────────────
    assert inv.avg_entry["SOL/USDC:USDC"] == pytest.approx(85.0, abs=1e-6)

    # ── 累积 fee = 0.01 ──────────────────────────────────────
    assert inv.cumulative_fees_usdc == pytest.approx(0.01, abs=1e-6)
