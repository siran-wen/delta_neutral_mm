# -*- coding: utf-8 -*-
"""
InventoryTracker 按腿分解 / basis 辅助方法单元测试。

覆盖场景:
  1. 两腿都有持仓 → unrealized_pnl_by_leg 两项、basis 正常
  2. 仅一腿持仓 → basis 返回 None
  3. 入场 basis / 当前 basis 数值校验
  4. asset_map 缺失 base → 返回 None
  5. unrealized_pnl 与 unrealized_pnl_by_leg 之和一致（兼容性）

不依赖交易所、不 mock CCXT，纯 Python 计算。浮点比较用 pytest.approx。
"""
import pytest

from execution.inventory import InventoryTracker


SPOT = "USOL/USDC"          # 现货腿 symbol
PERP = "SOL/USDC:USDC"      # 永续腿 symbol（":" 用于区分）
BASE = "SOL"
ASSET_MAP = {SPOT: BASE, PERP: BASE}


@pytest.fixture
def inv():
    """每个测试独立 InventoryTracker，避免跨测试污染"""
    return InventoryTracker()


# =============================================================================
# 两腿都有持仓
# =============================================================================

def test_both_legs_upnl_by_leg(inv):
    # 现货买 0.1 SOL @ 86.00, 永续空 0.1 SOL @ 85.80
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)

    # mid: 现货 86.10, 永续 85.95
    mids = {SPOT: 86.10, PERP: 85.95}
    by_leg = inv.unrealized_pnl_by_leg(mids)

    # 现货腿: delta=+0.1, entry=86.00, mid=86.10 → uPnL = 0.1*(86.10-86.00) = +0.01
    # 永续腿: delta=-0.1, entry=85.80, mid=85.95 → uPnL = -0.1*(85.95-85.80) = -0.015
    assert by_leg[SPOT] == pytest.approx(0.01, abs=1e-9)
    assert by_leg[PERP] == pytest.approx(-0.015, abs=1e-9)

    # 总 uPnL = 现货 + 永续
    total = inv.unrealized_pnl(mids)
    assert total == pytest.approx(0.01 - 0.015, abs=1e-9)
    # 兼容性：by_leg 的 sum 必须等于老 unrealized_pnl 的返回值
    assert total == pytest.approx(sum(by_leg.values()), abs=1e-12)


def test_one_leg_only_basis_none(inv):
    # 只开现货多头，永续没开 → basis 依赖两腿同时有持仓，必须返回 None
    inv.on_fill(SPOT, "buy", 0.1, 86.00)

    assert inv.get_entry_basis(BASE, ASSET_MAP) is None
    assert inv.get_current_basis(
        BASE, ASSET_MAP, {SPOT: 86.10, PERP: 85.95}
    ) is None


def test_basis_entry_and_current(inv):
    # 现货买 @ 86.00, 永续空 @ 85.80
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)

    # 建仓 basis = spot_entry - perp_entry = 86.00 - 85.80 = +0.20
    entry_basis = inv.get_entry_basis(BASE, ASSET_MAP)
    assert entry_basis == pytest.approx(0.20, abs=1e-9)

    # 当前 basis = spot_mid - perp_mid = 86.10 - 85.95 = +0.15
    mids = {SPOT: 86.10, PERP: 85.95}
    cur_basis = inv.get_current_basis(BASE, ASSET_MAP, mids)
    assert cur_basis == pytest.approx(0.15, abs=1e-9)

    # basis 收敛：建仓 0.20 → 当前 0.15，说明 basis 已经回归 0.05/SOL
    assert cur_basis < entry_basis


def test_asset_map_missing_base_returns_none(inv):
    # 开了 SOL 两腿
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)

    # 但调用方的 asset_map 里不包含 SOL 的映射 → 找不到 leg pair
    empty_map: dict = {}
    assert inv.get_entry_basis(BASE, empty_map) is None
    assert inv.get_current_basis(
        BASE, empty_map, {SPOT: 86.10, PERP: 85.95}
    ) is None

    # asset_map 包含别的 base（比如 "BTC"）也应返回 None
    unrelated_map = {"USBTC/USDC": "BTC", "BTC/USDC:USDC": "BTC"}
    assert inv.get_entry_basis(BASE, unrelated_map) is None


def test_basis_missing_mid_returns_none(inv):
    """当前 basis 依赖两腿 mid 都存在。缺一个腿 mid 应返回 None，
    避免拿 0 进来骗出 basis ≈ 86 这种误导值。"""
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)

    # 永续腿 mid 缺失
    assert inv.get_current_basis(BASE, ASSET_MAP, {SPOT: 86.10}) is None
    # 现货腿 mid 缺失
    assert inv.get_current_basis(BASE, ASSET_MAP, {PERP: 85.95}) is None
    # 两腿 mid 都缺
    assert inv.get_current_basis(BASE, ASSET_MAP, {}) is None


def test_zero_position_leg_basis_none(inv):
    """两腿曾经都开过但已经平掉一腿 → basis 不该再算（avg_entry 会被清除）"""
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)
    # 现货腿平掉
    inv.on_fill(SPOT, "sell", 0.1, 86.05)

    # 平仓后 SPOT 的 avg_entry 被清除
    assert SPOT not in inv.avg_entry
    # basis 应返回 None（现货腿持仓 ≈ 0）
    assert inv.get_entry_basis(BASE, ASSET_MAP) is None


def test_upnl_by_leg_missing_mid_defaults_zero(inv):
    """缺 mid 的腿应记 0.0，不歪曲分解。"""
    inv.on_fill(SPOT, "buy", 0.1, 86.00)
    inv.on_fill(PERP, "sell", 0.1, 85.80)

    # 只提供现货 mid
    by_leg = inv.unrealized_pnl_by_leg({SPOT: 86.10})
    assert by_leg[SPOT] == pytest.approx(0.01, abs=1e-9)
    # 永续腿缺 mid → 0
    assert by_leg[PERP] == pytest.approx(0.0, abs=1e-12)
