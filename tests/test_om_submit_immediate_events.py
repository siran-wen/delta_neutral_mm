# -*- coding: utf-8 -*-
"""
OrderManager.submit_order 在 submit-immediate 终态下的事件触发回归测试。

背景:
  CCXT 同步返回的 create_order 响应里 filled >= amount / status=closed /
  status=canceled / status=rejected 时，本地 state 直接进终态，reconcile
  循环只看 OPEN/PARTIAL/STALE 会跳过，老实现只 emit order_submitted，
  导致 order_filled 事件永不触发。

覆盖:
  1. status=closed → emit order_filled + submitted，filled_emit_source=
     "submit_immediate" 且 first_filled_emit_ts 打戳。
  2. filled >= amount 但 status 未给 → 同样归类为 FILLED，走同一路径。
  3. status=canceled → emit order_cancelled + submitted，不 emit filled。
  4. status=rejected → emit order_rejected + submitted，不 emit filled。
  5. status=open（老行为） → 只 emit order_submitted，不触发终态事件。
  6. 幂等性回归：多次对同一 managed 订单调用 _emit_order_filled 只首次打戳。
"""
import pytest

from execution.order_manager import OrderManager, OrderState
from gateways.gateway import Order, OrderSide, OrderType


# =============================================================================
# Stub Gateway
# =============================================================================

class _StubGateway:
    """
    仅实现 submit_order 路径必需的 create_order，避开真实 CCXT / 网络。
    其他方法（fetch_open_orders 等）只有在 om.start() 时才会被调用，
    这些测试不启动 reconcile 线程，所以用不到。
    """

    def __init__(self, response_dict: dict):
        self._response = response_dict
        self.last_call_args = None

    def create_order(
        self, symbol, side, order_type, amount, price=None,
        params=None, client_order_id=None,
    ):
        self.last_call_args = {
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "amount": amount,
            "price": price,
            "client_order_id": client_order_id,
        }
        # Order.from_ccxt 读取 id/symbol/side/type/price/amount/filled/
        # remaining/status/timestamp/average 等字段，stub 响应必须完整。
        return Order.from_ccxt(self._response)


def _make_om(response_dict: dict):
    """用 stub gateway 构造 OrderManager，不 start reconcile 线程。"""
    gw = _StubGateway(response_dict)
    om = OrderManager(gw)
    return om, gw


def _capture_events(om: OrderManager):
    """订阅所有相关事件，返回 dict 供测试断言触发顺序/次数。"""
    captured: dict = {
        "submitted": [],
        "filled": [],
        "cancelled": [],
        "rejected": [],
    }
    om.on("order_submitted", lambda m: captured["submitted"].append(m))
    om.on("order_filled", lambda m: captured["filled"].append(m))
    om.on("order_cancelled", lambda m: captured["cancelled"].append(m))
    om.on("order_rejected", lambda m: captured["rejected"].append(m))
    return captured


# =============================================================================
# 1. status=closed  → order_filled + submitted
# =============================================================================

def test_submit_immediate_status_closed_emits_filled():
    response = {
        "id": "eid-closed-001",
        "symbol": "SOL/USDC:USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.1,
        "remaining": 0.0,
        "status": "closed",
        "average": 86.01,
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "SOL/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
        source="hedger",
    )

    assert managed.state == OrderState.FILLED
    assert len(captured["filled"]) == 1, "status=closed 必须触发 order_filled"
    assert len(captured["submitted"]) == 1, "order_submitted 仍应保留"
    assert len(captured["cancelled"]) == 0
    assert len(captured["rejected"]) == 0

    # submit_immediate 标签被正确写入 ManagedOrder
    assert managed.filled_emit_source == "submit_immediate"
    assert managed.first_filled_emit_ts is not None
    assert managed.first_filled_emit_ts > 0


# =============================================================================
# 2. filled >= amount 但 status 未给  → 仍归类为 FILLED
# =============================================================================

def test_submit_immediate_full_fill_without_status_emits_filled():
    """
    Hyperliquid aggressive IOC 的极端响应：CCXT 没归一化 status，
    但 filled 字段已经等于 amount。submit_order 的触发条件里
    有 `gw_order.filled and gw_order.filled >= amount`。
    """
    response = {
        "id": "eid-implicit-fill",
        "symbol": "USOL/USDC",
        "side": "sell",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.1,
        "remaining": 0.0,
        "status": None,  # 没给
        "average": 85.99,
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.SELL, OrderType.LIMIT, 0.1, 86.00,
        source="hedger",
    )

    assert managed.state == OrderState.FILLED
    assert len(captured["filled"]) == 1
    assert managed.filled_emit_source == "submit_immediate"


# =============================================================================
# 3. status=canceled → order_cancelled + submitted
# =============================================================================

def test_submit_immediate_status_canceled_emits_cancelled():
    response = {
        "id": "eid-cancel-xx",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.0,
        "remaining": 0.1,
        "status": "canceled",
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.CANCELLED
    assert len(captured["cancelled"]) == 1
    assert len(captured["submitted"]) == 1
    assert len(captured["filled"]) == 0
    assert len(captured["rejected"]) == 0
    # 终态 CANCELLED 没经过 order_filled emit，不应设置 filled_emit_source
    assert managed.filled_emit_source is None
    assert managed.first_filled_emit_ts is None


def test_submit_immediate_status_cancelled_british_spelling():
    """CCXT 不同版本可能返回 'canceled' 或 'cancelled'，两种都要覆盖。"""
    response = {
        "id": "eid-cancel-uk",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.0,
        "remaining": 0.1,
        "status": "cancelled",
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.CANCELLED
    assert len(captured["cancelled"]) == 1


# =============================================================================
# 4. status=rejected → order_rejected + submitted
# =============================================================================

def test_submit_immediate_status_rejected_emits_rejected():
    response = {
        "id": "eid-reject-xx",
        "symbol": "SOL/USDC:USDC",
        "side": "buy",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.0,
        "remaining": 0.1,
        "status": "rejected",
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "SOL/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 86.00,
    )

    assert managed.state == OrderState.REJECTED
    assert len(captured["rejected"]) == 1
    assert len(captured["submitted"]) == 1
    assert len(captured["filled"]) == 0
    assert len(captured["cancelled"]) == 0


# =============================================================================
# 5. status=open（老行为）→ 只 emit order_submitted
# =============================================================================

def test_submit_open_only_emits_submitted():
    """
    正常情况：limit 单 resting on book，CCXT 返回 status=open，
    本地 state=OPEN。只 emit order_submitted，不应触发任何终态事件。
    """
    response = {
        "id": "eid-open-xx",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 85.00,
        "amount": 0.1,
        "filled": 0.0,
        "remaining": 0.1,
        "status": "open",
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.1, 85.00,
    )

    assert managed.state == OrderState.OPEN
    assert len(captured["submitted"]) == 1
    assert len(captured["filled"]) == 0
    assert len(captured["cancelled"]) == 0
    assert len(captured["rejected"]) == 0


# =============================================================================
# 6. CCXT IOC 归一化 bug：filled=0 / status=open 但 info 有 filled sub-dict
#    → Order.from_ccxt 用 info.totalSz 回填 filled，OM 天然进 FILLED 分支
# =============================================================================
#
# 背景：live_20260424_224447 里 6/6 hedge 遇到 CCXT 把 HL aggressive IOC 的
# 同步响应归一化成 status=open/filled=0，但 info.response.data.statuses[0].
# filled 子字典里 totalSz 和 avgPx 都在。修复落在 gateway.Order.from_ccxt，
# 以 "filled" sub-dict 存在作为 discriminator（"resting" sub-dict 是 maker
# 挂单，绝不进本分支），用 totalSz 回填 gw_order.filled，上层逻辑不用改。
#
# 下面 3 个 case 把三种边界形态锁死在测试层，防止未来有人修 from_ccxt
# 时把 partial-fill 或 maker-resting 误压进这条路径。


def _hl_response_with_info_filled(
    eid: str,
    symbol: str,
    side: str,
    amount: float,
    total_sz: float,
    avg_px: float,
    ccxt_filled: float = 0.0,
    ccxt_status: str = "open",
    ccxt_average: object = None,  # 用 sentinel：None 表示 CCXT 也没归一化 average
) -> dict:
    """
    构造 CCXT 归一化失败 + info.statuses[0].filled 里有真实成交量的响应。

    模拟 live_20260424_224447 观测到的形态：CCXT 层把 status 标成 open、
    filled 标成 0，但 HL 原生 info 里已经明确这一单全部成交。
    """
    resp: dict = {
        "id": eid,
        "symbol": symbol,
        "side": side,
        "type": "limit",
        "price": avg_px,
        "amount": amount,
        "filled": ccxt_filled,
        "remaining": max(amount - ccxt_filled, 0.0),
        "status": ccxt_status,
        "info": {
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {
                            "filled": {
                                "oid": 123456,
                                "totalSz": str(total_sz),  # HL 真实返回是字符串
                                "avgPx": str(avg_px),
                            }
                        }
                    ]
                },
            }
        },
    }
    if ccxt_average is not None:
        resp["average"] = ccxt_average
    return resp


def test_submit_immediate_ccxt_openzero_info_filled_emits_filled():
    """
    CCXT 归一化失败场景：status=open / filled=0，但 info.statuses[0].filled.
    totalSz 和 avgPx 都在 → from_ccxt 应用 totalSz 回填 gw_order.filled，OM
    原有 "filled >= amount" 分支自然进 FILLED，emit order_filled 且
    filled_emit_source = "submit_immediate"。

    同时断言 ManagedOrder.filled 被补齐到真实 totalSz（hedger 的 成交确认
    日志读这个字段，不补齐会打印 0.000000）。
    """
    amount = 0.130
    total_sz = 0.130
    avg_px = 86.0080
    response = _hl_response_with_info_filled(
        eid="eid-hl-ioc-fixed",
        symbol="SOL/USDC:USDC",
        side="sell",
        amount=amount,
        total_sz=total_sz,
        avg_px=avg_px,
        ccxt_filled=0.0,
        ccxt_status="open",
        ccxt_average=None,  # CCXT 也没给 average，由 from_ccxt 从 info 里捞
    )
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "SOL/USDC:USDC", OrderSide.SELL, OrderType.LIMIT, amount, avg_px,
        source="hedger",
    )

    assert managed.state == OrderState.FILLED, (
        "CCXT 报 open 但 info.statuses[0].filled 存在 → from_ccxt 回填 filled "
        "后 OM 应直接判 FILLED"
    )
    assert managed.filled == pytest.approx(total_sz), (
        "managed.filled 必须从 info.totalSz 补齐到真实成交量，"
        "否则 hedger 成交确认日志会误打印 filled=0"
    )
    assert managed.avg_fill_price == pytest.approx(avg_px), (
        "从 info.avgPx 回填的成交均价必须同步 sync 到 ManagedOrder"
    )
    assert len(captured["filled"]) == 1
    assert len(captured["submitted"]) == 1
    assert managed.filled_emit_source == "submit_immediate"
    assert managed.first_filled_emit_ts is not None


def test_submit_immediate_ccxt_openzero_no_info_stays_open():
    """
    反向约束：CCXT 报 filled=0 / status=open，info 里也没有 filled sub-dict
    （真正的 maker 挂单场景），不能被误判成 FILLED。

    这个 case 专门卡住未来修改：如果有人把 discriminator 换成仅看 avgPx 或
    仅看 statuses 存在性，本测试会立即炸。
    """
    response = {
        "id": "eid-true-resting",
        "symbol": "USOL/USDC",
        "side": "buy",
        "type": "limit",
        "price": 85.00,
        "amount": 0.10,
        "filled": 0.0,
        "remaining": 0.10,
        "status": "open",
        # info.statuses[0] 里只有 resting（挂单），没有 filled sub-dict
        "info": {
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {"resting": {"oid": 987654}}
                    ]
                },
            }
        },
    }
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.BUY, OrderType.LIMIT, 0.10, 85.00,
        source="hedger",
    )

    assert managed.state == OrderState.OPEN, (
        "info.statuses[0] 只有 resting 没有 filled → 不能误判成 FILLED"
    )
    assert managed.filled == 0.0
    assert len(captured["filled"]) == 0
    assert len(captured["submitted"]) == 1
    assert managed.filled_emit_source is None


def test_submit_partial_fill_not_overridden_by_info():
    """
    反向约束：CCXT 已归一化了部分成交（filled>0 且 <amount），即便 info 里
    也给了 totalSz（可能早于 CCXT 归一化前的半截数据），from_ccxt 不应覆盖
    CCXT 已有的 filled 值——避免把真正的 partial-fill 压成 full-fill。

    中间状态保持走老路径（OPEN / PARTIALLY_FILLED 由 reconcile 或后续事件
    推动），新的 filled-from-info 分支只捕获 "filled=0" 的 CCXT bug 形态。
    """
    amount = 0.10
    ccxt_partial = 0.05  # CCXT 已归一化的部分成交
    info_total_sz = 0.08  # 假想的 info 里 totalSz（不一致，但不应被采纳）
    response = _hl_response_with_info_filled(
        eid="eid-true-partial",
        symbol="USOL/USDC",
        side="sell",
        amount=amount,
        total_sz=info_total_sz,
        avg_px=85.50,
        ccxt_filled=ccxt_partial,
        ccxt_status="open",
        ccxt_average=85.50,
    )
    om, _ = _make_om(response)
    captured = _capture_events(om)

    managed = om.submit_order(
        "USOL/USDC", OrderSide.SELL, OrderType.LIMIT, amount, 85.50,
        source="hedger",
    )

    assert managed.state == OrderState.OPEN, (
        "真正的 partial-fill (filled>0 且 <amount) 必须保持 OPEN，不进"
        "submit-immediate FILLED 分支"
    )
    assert managed.filled == pytest.approx(ccxt_partial), (
        "CCXT 自带的 filled (partial) 不应被 info.totalSz 覆盖，"
        "否则会把 partial-fill 误压成 full-fill"
    )
    assert len(captured["filled"]) == 0
    assert len(captured["submitted"]) == 1


# =============================================================================
# 7. 幂等性：_emit_order_filled 多次调用不覆盖 first_filled_emit_ts
# =============================================================================

def test_emit_order_filled_idempotent():
    """
    _emit_order_filled 是所有 order_filled 路径（submit_immediate /
    cancel_recover_userfills / reconcile_*）的统一出口，幂等写入
    first_filled_emit_ts 和 filled_emit_source。
    """
    response = {
        "id": "eid-idem-xx",
        "symbol": "SOL/USDC:USDC",
        "side": "sell",
        "type": "limit",
        "price": 86.00,
        "amount": 0.1,
        "filled": 0.1,
        "remaining": 0.0,
        "status": "closed",
        "average": 85.99,
    }
    om, _ = _make_om(response)

    managed = om.submit_order(
        "SOL/USDC:USDC", OrderSide.SELL, OrderType.LIMIT, 0.1, 86.00,
    )

    ts_1 = managed.first_filled_emit_ts
    src_1 = managed.filled_emit_source
    assert ts_1 is not None
    assert src_1 == "submit_immediate"

    # 二次以不同 source 调用，stamp 不应被覆盖
    om._emit_order_filled(managed, source="reconcile_userfills")
    assert managed.first_filled_emit_ts == ts_1
    assert managed.filled_emit_source == src_1
