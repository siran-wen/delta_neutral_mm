# -*- coding: utf-8 -*-
"""
submit-immediate 路径上 fee 从 userFills 回填的回归测试。

背景:
  Hyperliquid 现货 BUY（taker）的 fee 扣 base coin（USOL）。CCXT 对
  create_order 的同步响应经常不带 fee 字段——fee 只在 userFills 才有完整
  数据。老实现依赖 reconcile 循环的 _infer_final_state_via_trades 回填，
  但 submit-immediate 终态订单会被 reconcile 状态过滤跳过，fee 永远
  补不上，导致：
    1. inventory.balances 漏扣 base coin fee（~0.000078 USOL/笔）
    2. BalanceGuard 放行实际会被交易所拒的订单
    3. cumulative_fees_usdc 漏计
    4. [PnL-对账] diff 稳定偏正

覆盖:
  1. happy path: 同步响应 fee=None，userFills 有 fee → 回填成功
  2. lag path: userFills 滞后返回空列表 → 不抛异常，fee_cost 保持 None
  3. CCXT 已带 fee: 不触发回填（不覆盖已有值）
  4. 多笔 fill 聚合 + builderFee 折算
  5. 非 Hyperliquid gateway（无 fetch_user_fills）→ 静默跳过
"""
import pytest

from execution.order_manager import OrderManager, OrderState
from gateways.gateway import Order, OrderSide, OrderType


# =============================================================================
# Stub Gateway
# =============================================================================

class _StubGateway:
    """
    模拟支持 fetch_user_fills 的 Hyperliquid gateway。

    create_order 返回 create_response；fetch_user_fills 返回 user_fills。
    """

    def __init__(self, create_response: dict, user_fills: list):
        self._create_response = create_response
        self._user_fills = user_fills
        self.create_calls = []
        self.fills_calls = []

    def create_order(
        self, symbol, side, order_type, amount, price=None,
        params=None, client_order_id=None,
    ):
        self.create_calls.append(
            {"symbol": symbol, "amount": amount, "price": price, "cid": client_order_id}
        )
        return Order.from_ccxt(self._create_response)

    def fetch_user_fills(self, start_time_ms=None):
        self.fills_calls.append(start_time_ms)
        return list(self._user_fills)


class _StubGatewayNoFills:
    """模拟不实现 fetch_user_fills 的非 Hyperliquid gateway。"""

    def __init__(self, create_response: dict):
        self._create_response = create_response

    def create_order(self, **kwargs):
        return Order.from_ccxt(self._create_response)

    # 注意：没有 fetch_user_fills 方法，会 raise AttributeError


def _spot_buy_no_fee_response() -> dict:
    """现货 BUY filled immediately，CCXT 响应没带 fee。"""
    return {
        "id": "eid-spot-buy-001",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.1,
        "remaining": 0.0,
        "status": "closed",
        "average": 86.00,
        # 没有 fee 字段
    }


# =============================================================================
# 1. Happy path — userFills 有 fee，回填成功
# =============================================================================

def test_submit_immediate_backfills_fee_from_userfills():
    """现货 BUY taker fee 扣 USOL，CCXT 不填 fee，从 userFills 回填。"""
    user_fills = [
        {
            "oid": "eid-spot-buy-001",
            "sz": "0.1",
            "px": "86.00",
            "fee": "0.00004",
            "feeToken": "USOL",
            "time": 1776997000000,
        },
    ]
    gw = _StubGateway(_spot_buy_no_fee_response(), user_fills)
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
        source="bootstrap",
    )

    assert managed.state == OrderState.FILLED
    assert managed.fee_cost == pytest.approx(0.00004, abs=1e-10)
    assert managed.fee_currency == "USOL"
    # fetch_user_fills 应被调用了一次
    assert len(gw.fills_calls) == 1
    assert gw.fills_calls[0] is not None  # start_time_ms 不应为 None


# =============================================================================
# 2. Lag path — userFills 返回空，fee_cost 保持 None，不抛异常
# =============================================================================

def test_submit_immediate_userfills_lag_no_exception():
    """
    Hyperliquid userFills 可能滞后 1-2s，oid 还没落盘；此时回填应
    保持 fee_cost=None，不抛异常，并让 submit_order 正常返回 FILLED。
    下次 reconcile 循环会再补一次。
    """
    gw = _StubGateway(_spot_buy_no_fee_response(), user_fills=[])
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.FILLED
    assert managed.fee_cost is None
    assert managed.fee_currency is None
    # fetch_user_fills 应被调用（尝试过），虽然返回空
    assert len(gw.fills_calls) == 1


def test_submit_immediate_userfills_only_other_oids():
    """userFills 有数据但 oid 全不匹配（滞后 + 别的单的 fill）→ 同上处理。"""
    user_fills = [
        {
            "oid": "other-eid-xx",
            "sz": "0.05",
            "px": "86.10",
            "fee": "0.00003",
            "feeToken": "USOL",
            "time": 1776997000000,
        },
    ]
    gw = _StubGateway(_spot_buy_no_fee_response(), user_fills)
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.FILLED
    assert managed.fee_cost is None
    assert managed.fee_currency is None


# =============================================================================
# 3. CCXT 已带 fee → 不触发回填
# =============================================================================

def test_submit_immediate_ccxt_fee_present_no_backfill():
    """
    如果 CCXT 同步响应里已经包含 fee（比如永续或 CCXT 新版归一化了），
    不应再调 fetch_user_fills 去重复拉。
    """
    response = {
        "id": "eid-ccxt-fee",
        "symbol": "SOL/USDC:USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.1,
        "remaining": 0.0,
        "status": "closed",
        "average": 86.00,
        "fee": {"cost": 0.012, "currency": "USDC"},
    }
    gw = _StubGateway(response, user_fills=[])
    om = OrderManager(gw)

    managed = om.submit_order(
        "SOL/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.FILLED
    # CCXT 响应里的 fee 已被 Order.from_ccxt 解析并经 sync_from_exchange 写入
    assert managed.fee_cost == pytest.approx(0.012, abs=1e-9)
    assert managed.fee_currency == "USDC"
    # 回填不应触发 → fetch_user_fills 零次调用
    assert len(gw.fills_calls) == 0


# =============================================================================
# 4. 多笔 fill 聚合 + builderFee 折算
# =============================================================================

def test_submit_immediate_multiple_fills_with_builder_fee():
    """
    一笔大 taker 单被交易所拆成两笔 fill。
    fee: 主 fee USOL，builderFee USDC，应按成交均价折算 builderFee 到 USOL。
    """
    user_fills = [
        {
            "oid": "eid-split-001",
            "sz": "0.06",
            "px": "86.00",
            "fee": "0.0000240",       # 现货 taker fee 0.04%: 0.06 * 0.0004 = 0.000024 USOL
            "feeToken": "USOL",
            "builderFee": "0.00516",  # 0.01% of notional: 0.06 * 86 * 0.0001 = 0.000516 USDC
            "time": 1776997000000,
        },
        {
            "oid": "eid-split-001",
            "sz": "0.04",
            "px": "86.05",
            "fee": "0.0000160",       # 0.04 * 0.0004 = 0.000016 USOL
            "feeToken": "USOL",
            "builderFee": "0.00344",  # 0.04 * 86.05 * 0.0001 ≈ 0.000344 USDC
            "time": 1776997000050,
        },
    ]
    response = {
        "id": "eid-split-001",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.10,
        "amount": 0.10,
        "filled": 0.10,
        "remaining": 0.0,
        "status": "closed",
        "average": 86.02,  # 加权均价
    }
    gw = _StubGateway(response, user_fills)
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.10, 86.10,
    )

    assert managed.state == OrderState.FILLED
    assert managed.fee_currency == "USOL"
    # 预期: 主 fee = 0.000024 + 0.000016 = 0.00004 USOL
    #       builderFee USDC = 0.00516 + 0.00344 = 0.0086 USDC
    #       折算到 USOL (avg_fill_price=86.02): 0.0086 / 86.02 ≈ 0.00009998 USOL
    #       总 fee ≈ 0.00004 + 0.00009998 = 0.00013998 USOL
    expected = 0.00004 + (0.0086 / 86.02)
    assert managed.fee_cost == pytest.approx(expected, rel=1e-4)


# =============================================================================
# 5. 非 Hyperliquid gateway（AttributeError 路径）
# =============================================================================

def test_submit_immediate_non_hyperliquid_gateway_silent_skip():
    """
    Stub gateway 不实现 fetch_user_fills → AttributeError → 静默跳过，
    submit_order 仍正常返回 FILLED。
    """
    gw = _StubGatewayNoFills(_spot_buy_no_fee_response())
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.FILLED
    assert managed.fee_cost is None
    assert managed.fee_currency is None


# =============================================================================
# 6. 非 FILLED 终态不应触发回填
# =============================================================================

def test_submit_immediate_cancelled_no_backfill():
    """status=canceled 不触发 fee 回填（没成交，没 fee）。"""
    response = {
        "id": "eid-cancel",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.0,
        "remaining": 0.1,
        "status": "canceled",
    }
    gw = _StubGateway(response, user_fills=[])
    om = OrderManager(gw)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.CANCELLED
    # fetch_user_fills 零次调用（只在 FILLED 分支触发回填）
    assert len(gw.fills_calls) == 0


# =============================================================================
# 7. _aggregate_fee_from_fills 独立单元验证（共享 helper 不跑偏）
# =============================================================================

def test_aggregate_fee_from_fills_usdc_main_plus_builder():
    """USDC 主 fee + USDC builderFee → 直接相加"""
    gw = _StubGateway({}, [])
    om = OrderManager(gw)

    class _FakeManaged:
        client_order_id = "fake"

    fills = [
        {"fee": "0.012", "feeToken": "USDC", "builderFee": "0.003"},
    ]
    cost, ccy = om._aggregate_fee_from_fills(fills, _FakeManaged(), avg_px_hint=86.0)
    assert cost == pytest.approx(0.015, abs=1e-9)
    assert ccy == "USDC"


def test_aggregate_fee_from_fills_empty_returns_none():
    gw = _StubGateway({}, [])
    om = OrderManager(gw)

    class _FakeManaged:
        client_order_id = "fake"

    cost, ccy = om._aggregate_fee_from_fills([], _FakeManaged(), avg_px_hint=86.0)
    assert cost is None
    assert ccy is None


def test_aggregate_fee_from_fills_builder_only_usdc():
    """只有 builderFee 没主 fee → 整笔按 USDC 记入"""
    gw = _StubGateway({}, [])
    om = OrderManager(gw)

    class _FakeManaged:
        client_order_id = "fake"

    fills = [{"builderFee": "0.005"}]
    cost, ccy = om._aggregate_fee_from_fills(fills, _FakeManaged(), avg_px_hint=86.0)
    assert cost == pytest.approx(0.005, abs=1e-9)
    assert ccy == "USDC"
