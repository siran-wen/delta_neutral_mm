#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
订单管理器 (Order Manager)

实盘级别的订单全生命周期管理模块，核心职责：

1. 订单生命周期管理 — 本地订单簿、状态机、ClientOrderId 映射
2. 自动对账与恢复   — 定时巡检、冲突解决、冷启动恢复
3. 延迟与性能监测   — 链路埋点、RTT 统计、滑点预警
4. 频率限制保护     — 自适应令牌桶、优先级队列
5. 原子化改单操作   — Cancel-Replace 模拟、防重入锁

用法:
    from gateways import GatewayFactory
    from execution import OrderManager
    from gateways.gateway import OrderSide, OrderType

    gw = GatewayFactory.create("config/hyperliquid_config.yaml")
    gw.connect()

    om = OrderManager(gw)
    om.start()

    order = om.submit_order("BTC/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.001, 35000.0)
    om.amend_order(order.client_order_id, new_price=36000.0)
    om.cancel_order(order.client_order_id)
    om.stop()
"""

import uuid
import time
import threading
import logging
from enum import Enum, auto
from typing import Dict, Any, Optional, List, Callable, Tuple
from dataclasses import dataclass, field
from collections import deque

from gateways.gateway import BaseGateway, Order, OrderSide, OrderType


# =============================================================================
# 1. 数据模型与状态机
# =============================================================================

class OrderState(Enum):
    """
    订单状态机

    状态转换:
        PENDING_NEW ──Ack──> OPEN ──Fill──> FILLED
            │                  │
            │(timeout)         │(cancel)
            v                  v
          STALE           PENDING_CANCEL ──Ack──> CANCELLED
            │                  │
            │(reconcile)       │(timeout)
            v                  v
         LOST/OPEN           STALE

    终态: FILLED, CANCELLED, REJECTED, LOST
    """
    PENDING_NEW = "pending_new"          # 已提交，等待交易所回执
    OPEN = "open"                        # 交易所已确认，挂单中
    PARTIALLY_FILLED = "partially_filled"  # 部分成交
    FILLED = "filled"                    # 完全成交 (终态)
    PENDING_CANCEL = "pending_cancel"    # 撤单已提交，等待确认
    CANCELLED = "cancelled"              # 已撤销 (终态)
    REJECTED = "rejected"               # 被拒绝 (终态)
    STALE = "stale"                      # 陈旧——发出后超时未收到回执
    LOST = "lost"                        # 丢失——本地有记录但交易所查不到 (终态)

    @property
    def is_terminal(self) -> bool:
        """是否为终态"""
        return self in (
            OrderState.FILLED,
            OrderState.CANCELLED,
            OrderState.REJECTED,
            OrderState.LOST,
        )

    @property
    def is_active(self) -> bool:
        """是否为活跃状态（需要持续追踪）"""
        return not self.is_terminal


class RequestPriority(Enum):
    """API 请求优先级（数值越小优先级越高）"""
    CANCEL = 0          # 最高：撤单
    HEDGE = 1           # 对冲
    NEW_ORDER = 2       # 新挂单
    QUERY = 3           # 查询
    LOW = 4             # 低优先级


@dataclass
class LatencyRecord:
    """单次下单链路延迟记录"""
    client_order_id: str
    strategy_trigger_ts: float = 0.0    # 策略触发时间
    api_send_ts: float = 0.0            # API 发送时间
    exchange_ack_ts: float = 0.0        # 交易所回执时间
    cancel_send_ts: float = 0.0         # 撤单发送时间
    cancel_ack_ts: float = 0.0          # 撤单回执时间

    @property
    def order_rtt(self) -> Optional[float]:
        """下单往返时间（秒）"""
        if self.api_send_ts > 0 and self.exchange_ack_ts > 0:
            return self.exchange_ack_ts - self.api_send_ts
        return None

    @property
    def cancel_rtt(self) -> Optional[float]:
        """撤单往返时间（秒）"""
        if self.cancel_send_ts > 0 and self.cancel_ack_ts > 0:
            return self.cancel_ack_ts - self.cancel_send_ts
        return None

    @property
    def total_latency(self) -> Optional[float]:
        """策略触发到交易所确认的总延迟（秒）"""
        if self.strategy_trigger_ts > 0 and self.exchange_ack_ts > 0:
            return self.exchange_ack_ts - self.strategy_trigger_ts
        return None


@dataclass
class ManagedOrder:
    """
    OM 管理的订单实体

    在 Gateway 层的 Order 基础上，增加了：
    - ClientOrderId（本地唯一标识）
    - 状态机
    - 时间戳追踪
    - 延迟记录
    """
    # ---- 标识 ----
    client_order_id: str                            # 本地生成的 UUID
    exchange_order_id: Optional[str] = None         # 交易所返回的 ID
    symbol: str = ""
    side: str = ""          # "buy" / "sell"
    order_type: str = ""    # "limit" / "market"
    price: Optional[float] = None
    amount: float = 0.0
    filled: float = 0.0
    remaining: Optional[float] = None

    # ---- 状态 ----
    state: OrderState = OrderState.PENDING_NEW
    previous_state: Optional[OrderState] = None
    reject_reason: str = ""

    # ---- 时间戳 (epoch seconds) ----
    created_at: float = field(default_factory=time.time)
    ack_at: Optional[float] = None            # 交易所确认时间
    last_updated_at: float = field(default_factory=time.time)
    state_changed_at: float = field(default_factory=time.time)

    # ---- 延迟追踪 ----
    latency: Optional[LatencyRecord] = field(default=None)

    # ---- 原子改单锁标记 ----
    locked: bool = False

    def __post_init__(self):
        if self.remaining is None:
            self.remaining = self.amount
        if self.latency is None:
            self.latency = LatencyRecord(client_order_id=self.client_order_id)

    def transition_to(self, new_state: OrderState) -> None:
        """状态转换"""
        self.previous_state = self.state
        self.state = new_state
        now = time.time()
        self.state_changed_at = now
        self.last_updated_at = now

    def sync_from_exchange(self, gw_order: Order) -> None:
        """从 Gateway 层的 Order 同步字段"""
        if gw_order.id:
            self.exchange_order_id = str(gw_order.id)
        if gw_order.filled is not None:
            self.filled = gw_order.filled
        if gw_order.remaining is not None:
            self.remaining = gw_order.remaining
        if gw_order.price is not None:
            self.price = gw_order.price
        self.last_updated_at = time.time()

    @property
    def is_active(self) -> bool:
        return self.state.is_active

    @property
    def age(self) -> float:
        """自创建以来经过的秒数"""
        return time.time() - self.created_at

    @property
    def time_in_state(self) -> float:
        """当前状态持续的秒数"""
        return time.time() - self.state_changed_at


# =============================================================================
# 2. 频率限制器（令牌桶 + 优先级）
# =============================================================================

class RateLimiter:
    """
    自适应令牌桶限速器

    - 按优先级排队：CANCEL > HEDGE > NEW_ORDER > QUERY
    - 令牌不足时阻塞低优先级请求，保证撤单始终能执行
    """

    def __init__(
        self,
        max_tokens: int = 20,
        refill_rate: float = 20.0,
        refill_interval: float = 1.0,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            max_tokens:     令牌桶容量
            refill_rate:    每个 refill_interval 补充的令牌数
            refill_interval: 补充间隔（秒）
        """
        self._max_tokens = max_tokens
        self._tokens = float(max_tokens)
        self._refill_rate = refill_rate
        self._refill_interval = refill_interval
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self._logger = logger or logging.getLogger("RateLimiter")

        # 统计
        self._total_requests = 0
        self._total_throttled = 0

    def acquire(self, priority: RequestPriority = RequestPriority.QUERY, cost: float = 1.0) -> bool:
        """
        尝试获取令牌

        Args:
            priority: 请求优先级
            cost:     消耗的令牌数

        Returns:
            True 表示获取成功，False 表示被限流
        """
        with self._lock:
            self._refill()
            self._total_requests += 1

            # CANCEL 请求永远放行（透支令牌）
            if priority == RequestPriority.CANCEL:
                self._tokens -= cost
                return True

            if self._tokens >= cost:
                self._tokens -= cost
                return True

            self._total_throttled += 1
            self._logger.debug(
                f"限流: priority={priority.name}, tokens={self._tokens:.1f}/{self._max_tokens}"
            )
            return False

    def wait_and_acquire(
        self,
        priority: RequestPriority = RequestPriority.QUERY,
        cost: float = 1.0,
        timeout: float = 5.0,
    ) -> bool:
        """阻塞等待直到获取令牌或超时"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.acquire(priority, cost):
                return True
            time.sleep(0.05)
        return False

    @property
    def available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "total_requests": self._total_requests,
            "total_throttled": self._total_throttled,
            "available_tokens": round(self.available_tokens, 2),
            "max_tokens": self._max_tokens,
        }

    def _refill(self) -> None:
        """补充令牌"""
        now = time.time()
        elapsed = now - self._last_refill
        if elapsed >= self._refill_interval:
            intervals = elapsed / self._refill_interval
            self._tokens = min(self._max_tokens, self._tokens + intervals * self._refill_rate)
            self._last_refill = now


# =============================================================================
# 3. 延迟追踪器
# =============================================================================

class LatencyTracker:
    """
    下单链路延迟追踪器

    - 统计最近 N 次的 RTT 均值 / P95
    - 超过阈值时触发预警回调
    """

    def __init__(
        self,
        window_size: int = 100,
        rtt_warn_threshold: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            window_size:        滑动窗口大小
            rtt_warn_threshold: RTT 预警阈值（秒）
        """
        self._window_size = window_size
        self._rtt_warn_threshold = rtt_warn_threshold
        self._order_rtts: deque = deque(maxlen=window_size)
        self._cancel_rtts: deque = deque(maxlen=window_size)
        self._logger = logger or logging.getLogger("LatencyTracker")
        self._warn_callback: Optional[Callable[[float, float], None]] = None

    def set_warn_callback(self, callback: Callable[[float, float], None]) -> None:
        """
        设置预警回调

        callback(avg_rtt, p95_rtt) — 当均值超过阈值时触发
        """
        self._warn_callback = callback

    def record_order_rtt(self, latency_record: LatencyRecord) -> None:
        """记录一次下单 RTT"""
        rtt = latency_record.order_rtt
        if rtt is not None and rtt > 0:
            self._order_rtts.append(rtt)
            self._check_warning()

    def record_cancel_rtt(self, latency_record: LatencyRecord) -> None:
        """记录一次撤单 RTT"""
        rtt = latency_record.cancel_rtt
        if rtt is not None and rtt > 0:
            self._cancel_rtts.append(rtt)

    def get_order_stats(self) -> Dict[str, Optional[float]]:
        """获取下单 RTT 统计"""
        return self._compute_stats(self._order_rtts)

    def get_cancel_stats(self) -> Dict[str, Optional[float]]:
        """获取撤单 RTT 统计"""
        return self._compute_stats(self._cancel_rtts)

    def _compute_stats(self, data: deque) -> Dict[str, Optional[float]]:
        if not data:
            return {"count": 0, "avg": None, "min": None, "max": None, "p95": None}
        sorted_data = sorted(data)
        n = len(sorted_data)
        p95_idx = min(int(n * 0.95), n - 1)
        return {
            "count": n,
            "avg": round(sum(sorted_data) / n, 4),
            "min": round(sorted_data[0], 4),
            "max": round(sorted_data[-1], 4),
            "p95": round(sorted_data[p95_idx], 4),
        }

    def _check_warning(self) -> None:
        """检查是否需要触发预警"""
        stats = self.get_order_stats()
        avg = stats.get("avg")
        p95 = stats.get("p95")
        if avg and avg > self._rtt_warn_threshold:
            self._logger.warning(
                f"延迟预警: 下单 RTT avg={avg:.3f}s, p95={p95:.3f}s, "
                f"阈值={self._rtt_warn_threshold}s"
            )
            if self._warn_callback:
                self._warn_callback(avg, p95)


# =============================================================================
# 4. 订单管理器核心
# =============================================================================

class OrderManager:
    """
    订单管理器

    将 Gateway 的原始订单操作封装为实盘级别的订单全生命周期管理。

    用法:
        gw = GatewayFactory.create("config/hyperliquid_config.yaml")
        gw.connect()

        om = OrderManager(gw)
        om.start()

        # 下单
        managed = om.submit_order("BTC/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.001, 35000.0)

        # 改单 (原子操作)
        om.amend_order(managed.client_order_id, new_price=36000.0)

        # 撤单
        om.cancel_order(managed.client_order_id)

        om.stop()
    """

    def __init__(
        self,
        gateway: BaseGateway,
        reconcile_interval: float = 3.0,
        stale_timeout: float = 5.0,
        rate_limit_per_sec: int = 20,
        rtt_warn_threshold: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            gateway:              底层交易所网关
            reconcile_interval:   对账巡检间隔（秒）
            stale_timeout:        订单变为 STALE 的超时（秒）
            rate_limit_per_sec:   每秒最大 API 请求数
            rtt_warn_threshold:   RTT 预警阈值（秒）
        """
        self._gateway = gateway
        self._reconcile_interval = reconcile_interval
        self._stale_timeout = stale_timeout
        self._logger = logger or logging.getLogger("OrderManager")

        # ---- 本地订单簿 ----
        # client_order_id -> ManagedOrder
        self._orders: Dict[str, ManagedOrder] = {}
        # exchange_order_id -> client_order_id (反向映射)
        self._eid_to_cid: Dict[str, str] = {}
        self._orders_lock = threading.Lock()

        # ---- 子模块 ----
        self._rate_limiter = RateLimiter(
            max_tokens=rate_limit_per_sec,
            refill_rate=float(rate_limit_per_sec),
            logger=self._logger,
        )
        self._latency_tracker = LatencyTracker(
            rtt_warn_threshold=rtt_warn_threshold,
            logger=self._logger,
        )

        # ---- 防重入锁 (按 client_order_id，使用 RLock 支持同线程重入) ----
        self._op_locks: Dict[str, threading.RLock] = {}
        self._op_locks_lock = threading.Lock()

        # ---- 回调 ----
        self._callbacks: Dict[str, List[Callable]] = {}

        # ---- 巡检线程 ----
        self._reconcile_thread: Optional[threading.Thread] = None
        self._running = False

    # =========================================================================
    # 生命周期
    # =========================================================================

    def start(self) -> None:
        """启动 OM（含冷启动恢复 + 巡检线程）"""
        self._logger.info("OrderManager 启动中...")
        self._running = True

        # 冷启动恢复
        self._cold_start_recovery()

        # 启动对账巡检线程
        self._reconcile_thread = threading.Thread(
            target=self._reconcile_loop,
            name="OM-Reconcile",
            daemon=True,
        )
        self._reconcile_thread.start()
        self._logger.info("OrderManager 已启动")

    def stop(self) -> None:
        """停止 OM"""
        self._logger.info("OrderManager 停止中...")
        self._running = False
        if self._reconcile_thread and self._reconcile_thread.is_alive():
            self._reconcile_thread.join(timeout=self._reconcile_interval + 2)
        self._logger.info("OrderManager 已停止")

    @property
    def is_running(self) -> bool:
        return self._running

    # =========================================================================
    # 订单操作（对外接口）
    # =========================================================================

    def submit_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        strategy_trigger_ts: Optional[float] = None,
        params: Optional[Dict] = None,
    ) -> ManagedOrder:
        """
        提交新订单

        Args:
            symbol:              交易对
            side:                买/卖
            order_type:          限价/市价
            amount:              数量
            price:               价格 (限价单必填)
            strategy_trigger_ts: 策略触发时间戳（用于延迟追踪）
            params:              交易所特定参数

        Returns:
            ManagedOrder 实例
        """
        # 生成 ClientOrderId
        cid = self._generate_client_order_id()

        # 创建本地订单记录
        managed = ManagedOrder(
            client_order_id=cid,
            symbol=symbol,
            side=side.value,
            order_type=order_type.value,
            amount=amount,
            price=price,
            state=OrderState.PENDING_NEW,
        )

        # 延迟埋点: 策略触发时间
        if strategy_trigger_ts:
            managed.latency.strategy_trigger_ts = strategy_trigger_ts
        else:
            managed.latency.strategy_trigger_ts = time.time()

        # 存入本地订单簿
        with self._orders_lock:
            self._orders[cid] = managed

        # 限流
        if not self._rate_limiter.wait_and_acquire(RequestPriority.NEW_ORDER, timeout=3.0):
            managed.transition_to(OrderState.REJECTED)
            managed.reject_reason = "rate_limited"
            self._logger.warning(f"[{cid}] 下单被限流拒绝")
            self._emit("order_rejected", managed)
            return managed

        # 发送到交易所
        try:
            managed.latency.api_send_ts = time.time()

            gw_order = self._gateway.create_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                amount=amount,
                price=price,
                params=params,
            )

            # 收到回执
            ack_ts = time.time()
            managed.latency.exchange_ack_ts = ack_ts
            managed.ack_at = ack_ts
            managed.sync_from_exchange(gw_order)

            # 建立 ExchangeOrderId -> ClientOrderId 映射
            if managed.exchange_order_id:
                with self._orders_lock:
                    self._eid_to_cid[managed.exchange_order_id] = cid

            # 根据交易所返回的状态决定本地状态
            ex_status = (gw_order.status or "").lower()
            if ex_status == "closed" or (gw_order.filled and gw_order.filled >= amount):
                managed.transition_to(OrderState.FILLED)
            elif ex_status == "canceled" or ex_status == "cancelled":
                managed.transition_to(OrderState.CANCELLED)
            elif ex_status == "rejected":
                managed.transition_to(OrderState.REJECTED)
            else:
                managed.transition_to(OrderState.OPEN)

            # 延迟追踪
            self._latency_tracker.record_order_rtt(managed.latency)

            self._logger.info(
                f"[{cid}] 订单已确认: eid={managed.exchange_order_id}, "
                f"state={managed.state.value}, "
                f"rtt={managed.latency.order_rtt:.3f}s"
                if managed.latency.order_rtt else
                f"[{cid}] 订单已确认: eid={managed.exchange_order_id}, "
                f"state={managed.state.value}"
            )

            self._emit("order_submitted", managed)
            return managed

        except Exception as e:
            managed.transition_to(OrderState.REJECTED)
            managed.reject_reason = str(e)
            self._logger.error(f"[{cid}] 下单失败: {e}")
            self._emit("order_rejected", managed)
            return managed

    def cancel_order(self, client_order_id: str) -> bool:
        """
        撤销订单

        Args:
            client_order_id: 本地订单ID

        Returns:
            是否发送成功
        """
        lock = self._get_op_lock(client_order_id)
        if not lock.acquire(blocking=False):
            self._logger.warning(f"[{client_order_id}] 操作冲突，跳过撤单")
            return False

        try:
            managed = self._get_order(client_order_id)
            if managed is None:
                self._logger.warning(f"[{client_order_id}] 订单不存在")
                return False

            if managed.state.is_terminal:
                self._logger.info(f"[{client_order_id}] 订单已在终态 {managed.state.value}，无需撤单")
                return False

            if managed.state == OrderState.PENDING_CANCEL:
                self._logger.info(f"[{client_order_id}] 撤单已在进行中")
                return False

            eid = managed.exchange_order_id
            if not eid:
                # 还没收到交易所ID，直接标记为 CANCELLED
                managed.transition_to(OrderState.CANCELLED)
                self._emit("order_cancelled", managed)
                return True

            managed.transition_to(OrderState.PENDING_CANCEL)

            # 限流：撤单最高优先级
            self._rate_limiter.acquire(RequestPriority.CANCEL)

            try:
                managed.latency.cancel_send_ts = time.time()
                success = self._gateway.cancel_order(eid, managed.symbol)
                managed.latency.cancel_ack_ts = time.time()

                if success:
                    managed.transition_to(OrderState.CANCELLED)
                    self._latency_tracker.record_cancel_rtt(managed.latency)
                    self._logger.info(
                        f"[{client_order_id}] 撤单成功, "
                        f"rtt={managed.latency.cancel_rtt:.3f}s"
                        if managed.latency.cancel_rtt else
                        f"[{client_order_id}] 撤单成功"
                    )
                else:
                    # cancel_order 返回 False 可能是订单已不存在
                    managed.transition_to(OrderState.CANCELLED)
                    self._logger.info(f"[{client_order_id}] 订单可能已成交或已撤销")

                self._emit("order_cancelled", managed)
                return True

            except Exception as e:
                # 撤单异常，回退到之前的状态
                if managed.previous_state:
                    managed.transition_to(managed.previous_state)
                self._logger.error(f"[{client_order_id}] 撤单失败: {e}")
                return False

        finally:
            lock.release()

    def cancel_all(self, symbol: Optional[str] = None) -> int:
        """撤销所有活跃订单"""
        active_orders = self.get_active_orders(symbol)
        cancelled = 0
        for managed in active_orders:
            if self.cancel_order(managed.client_order_id):
                cancelled += 1
        self._logger.info(f"批量撤单: {cancelled}/{len(active_orders)}")
        return cancelled

    def amend_order(
        self,
        client_order_id: str,
        new_price: Optional[float] = None,
        new_amount: Optional[float] = None,
        strategy: str = "cancel_first",
    ) -> Optional[ManagedOrder]:
        """
        原子化改单 (Cancel-Replace)

        Args:
            client_order_id: 要修改的订单的 ClientOrderId
            new_price:       新价格 (None 则不变)
            new_amount:      新数量 (None 则不变)
            strategy:        改单策略
                             - "cancel_first":  先撤旧单，再下新单（保守，释放保证金）
                             - "new_first":     先下新单，再撤旧单（激进，保持挂单覆盖）

        Returns:
            新的 ManagedOrder 或 None（失败时）
        """
        lock = self._get_op_lock(client_order_id)
        if not lock.acquire(blocking=False):
            self._logger.warning(f"[{client_order_id}] 操作冲突，跳过改单")
            return None

        try:
            old_order = self._get_order(client_order_id)
            if old_order is None or old_order.state.is_terminal:
                self._logger.warning(f"[{client_order_id}] 订单不存在或已终结")
                return None

            # 标记旧订单已锁定
            old_order.locked = True

            # 决定新参数
            price = new_price if new_price is not None else old_order.price
            amount = new_amount if new_amount is not None else old_order.amount

            if strategy == "cancel_first":
                return self._amend_cancel_first(old_order, price, amount)
            elif strategy == "new_first":
                return self._amend_new_first(old_order, price, amount)
            else:
                self._logger.error(f"未知改单策略: {strategy}")
                return None

        finally:
            old_order_ref = self._get_order(client_order_id)
            if old_order_ref:
                old_order_ref.locked = False
            lock.release()

    # =========================================================================
    # 查询接口
    # =========================================================================

    def get_order(self, client_order_id: str) -> Optional[ManagedOrder]:
        """根据 ClientOrderId 获取订单"""
        return self._get_order(client_order_id)

    def get_order_by_exchange_id(self, exchange_order_id: str) -> Optional[ManagedOrder]:
        """根据 ExchangeOrderId 获取订单"""
        with self._orders_lock:
            cid = self._eid_to_cid.get(exchange_order_id)
            if cid:
                return self._orders.get(cid)
        return None

    def get_active_orders(self, symbol: Optional[str] = None) -> List[ManagedOrder]:
        """获取所有活跃订单"""
        with self._orders_lock:
            result = [o for o in self._orders.values() if o.is_active]
            if symbol:
                result = [o for o in result if o.symbol == symbol]
            return result

    def get_all_orders(self) -> Dict[str, ManagedOrder]:
        """获取所有订单的快照"""
        with self._orders_lock:
            return dict(self._orders)

    # =========================================================================
    # 统计与诊断
    # =========================================================================

    @property
    def latency_tracker(self) -> LatencyTracker:
        return self._latency_tracker

    @property
    def rate_limiter(self) -> RateLimiter:
        return self._rate_limiter

    def get_stats(self) -> Dict[str, Any]:
        """获取 OM 运行统计"""
        with self._orders_lock:
            total = len(self._orders)
            by_state = {}
            for o in self._orders.values():
                state_name = o.state.value
                by_state[state_name] = by_state.get(state_name, 0) + 1

        return {
            "running": self._running,
            "total_orders": total,
            "orders_by_state": by_state,
            "rate_limiter": self._rate_limiter.stats,
            "latency_order": self._latency_tracker.get_order_stats(),
            "latency_cancel": self._latency_tracker.get_cancel_stats(),
        }

    # =========================================================================
    # 事件回调
    # =========================================================================

    def on(self, event_name: str, callback: Callable) -> None:
        """
        注册事件回调

        事件类型:
            - order_submitted:   订单已提交并确认
            - order_rejected:    订单被拒绝
            - order_cancelled:   订单已撤销
            - order_filled:      订单已成交
            - order_lost:        订单丢失
            - order_state_change: 任意状态变更
            - reconcile_conflict: 对账发现冲突
            - latency_warning:   延迟预警
        """
        if event_name not in self._callbacks:
            self._callbacks[event_name] = []
        self._callbacks[event_name].append(callback)

    def _emit(self, event_name: str, data: Any = None) -> None:
        for cb in self._callbacks.get(event_name, []):
            try:
                cb(data)
            except Exception as e:
                self._logger.error(f"回调 '{event_name}' 异常: {e}")

    # =========================================================================
    # 内部：对账巡检
    # =========================================================================

    def _reconcile_loop(self) -> None:
        """对账巡检主循环"""
        while self._running:
            try:
                self._reconcile()
            except Exception as e:
                self._logger.error(f"对账巡检异常: {e}")
            time.sleep(self._reconcile_interval)

    def _reconcile(self) -> None:
        """
        一次对账巡检

        1. 检测 STALE 订单（PENDING_NEW 超时）
        2. 拉取交易所挂单列表
        3. 交叉比对解决冲突
        """
        now = time.time()

        # ---- Step 1: STALE 检测 ----
        with self._orders_lock:
            active_orders = [o for o in self._orders.values() if o.is_active]

        for managed in active_orders:
            if managed.state == OrderState.PENDING_NEW and managed.time_in_state > self._stale_timeout:
                managed.transition_to(OrderState.STALE)
                self._logger.warning(
                    f"[{managed.client_order_id}] 订单超时变为 STALE "
                    f"(已等待 {managed.time_in_state:.1f}s)"
                )
                self._emit("order_state_change", managed)

            if managed.state == OrderState.PENDING_CANCEL and managed.time_in_state > self._stale_timeout:
                managed.transition_to(OrderState.STALE)
                self._logger.warning(
                    f"[{managed.client_order_id}] 撤单超时变为 STALE"
                )

        # ---- Step 2: 拉取交易所挂单 ----
        if not self._rate_limiter.acquire(RequestPriority.QUERY):
            return

        try:
            exchange_open_orders = self._gateway.fetch_open_orders()
        except Exception as e:
            # CCXT 4.5.x 的 fetch_open_orders 存在 422 bug，降级为 debug 避免刷屏
            self._logger.debug(f"巡检拉取挂单失败（可能是 CCXT 版本兼容问题）: {e}")
            return

        # 建立交易所侧 eid 集合
        exchange_eids = set()
        exchange_order_map: Dict[str, Order] = {}
        for gw_order in exchange_open_orders:
            eid = str(gw_order.id)
            exchange_eids.add(eid)
            exchange_order_map[eid] = gw_order

        # ---- Step 3: 交叉比对 ----
        with self._orders_lock:
            active_orders = [o for o in self._orders.values() if o.is_active]

        for managed in active_orders:
            eid = managed.exchange_order_id
            if not eid:
                continue

            if eid in exchange_eids:
                # 交易所侧有这个订单 -> 同步状态
                gw_order = exchange_order_map[eid]
                managed.sync_from_exchange(gw_order)

                # 如果部分成交
                if managed.filled and managed.filled > 0 and managed.remaining and managed.remaining > 0:
                    if managed.state != OrderState.PARTIALLY_FILLED:
                        managed.transition_to(OrderState.PARTIALLY_FILLED)
                        self._emit("order_state_change", managed)

                # STALE 订单在交易所侧依然存在 -> 恢复为 OPEN
                if managed.state == OrderState.STALE:
                    managed.transition_to(OrderState.OPEN)
                    self._logger.info(f"[{managed.client_order_id}] STALE -> OPEN (对账恢复)")
                    self._emit("order_state_change", managed)

            else:
                # 交易所侧没有这个订单
                if managed.state in (OrderState.OPEN, OrderState.PARTIALLY_FILLED, OrderState.STALE):
                    # 可能已成交或已被撤销
                    if managed.filled and managed.filled >= managed.amount:
                        managed.transition_to(OrderState.FILLED)
                        self._logger.info(
                            f"[{managed.client_order_id}] 对账发现已成交: "
                            f"filled={managed.filled}"
                        )
                        self._emit("order_filled", managed)
                    elif managed.state == OrderState.PENDING_CANCEL:
                        managed.transition_to(OrderState.CANCELLED)
                        self._emit("order_cancelled", managed)
                    else:
                        # 无法确定原因 -> 标记为 LOST
                        managed.transition_to(OrderState.LOST)
                        self._logger.warning(
                            f"[{managed.client_order_id}] 对账发现订单丢失 (eid={eid})"
                        )
                        self._emit("order_lost", managed)
                        self._emit("reconcile_conflict", {
                            "type": "order_lost",
                            "client_order_id": managed.client_order_id,
                            "exchange_order_id": eid,
                        })

    # =========================================================================
    # 内部：冷启动恢复
    # =========================================================================

    def _cold_start_recovery(self) -> None:
        """
        冷启动恢复

        程序重启时，拉取交易所当前挂单并重建本地订单簿。
        """
        self._logger.info("冷启动恢复: 拉取当前挂单...")

        try:
            exchange_open_orders = self._gateway.fetch_open_orders()
        except Exception as e:
            self._logger.warning(f"冷启动恢复失败（可能是 CCXT 版本兼容问题，不影响正常运行）: {e}")
            return

        recovered = 0
        for gw_order in exchange_open_orders:
            eid = str(gw_order.id)

            # 检查本地是否已有该订单
            existing = self.get_order_by_exchange_id(eid)
            if existing:
                existing.sync_from_exchange(gw_order)
                if existing.state == OrderState.PENDING_NEW:
                    existing.transition_to(OrderState.OPEN)
                continue

            # 本地没有 -> 创建"恢复"记录
            cid = self._generate_client_order_id()
            managed = ManagedOrder(
                client_order_id=cid,
                exchange_order_id=eid,
                symbol=gw_order.symbol,
                side=gw_order.side or "",
                order_type=gw_order.type or "",
                price=gw_order.price,
                amount=gw_order.amount or 0,
                filled=gw_order.filled or 0,
                remaining=gw_order.remaining,
                state=OrderState.OPEN,
                ack_at=time.time(),
            )
            managed.latency = LatencyRecord(client_order_id=cid)

            with self._orders_lock:
                self._orders[cid] = managed
                self._eid_to_cid[eid] = cid

            recovered += 1
            self._logger.info(
                f"冷启动恢复: eid={eid} -> cid={cid}, "
                f"{gw_order.side} {gw_order.amount} {gw_order.symbol} @ {gw_order.price}"
            )

        self._logger.info(f"冷启动恢复完成: 恢复了 {recovered} 个挂单")

    # =========================================================================
    # 内部：原子化改单
    # =========================================================================

    def _amend_cancel_first(
        self,
        old_order: ManagedOrder,
        new_price: float,
        new_amount: float,
    ) -> Optional[ManagedOrder]:
        """
        先撤旧单，再下新单（保守策略）

        优点：释放保证金，不会双重占用
        缺点：撤单和下新单之间有短暂的无挂单窗口
        """
        cid = old_order.client_order_id
        self._logger.info(f"[{cid}] 改单(cancel_first): price {old_order.price} -> {new_price}")

        # Step 1: 撤旧单
        if not self.cancel_order(cid):
            self._logger.error(f"[{cid}] 改单失败: 撤旧单失败")
            return None

        # Step 2: 下新单
        new_order = self.submit_order(
            symbol=old_order.symbol,
            side=OrderSide(old_order.side),
            order_type=OrderType(old_order.order_type),
            amount=new_amount,
            price=new_price,
        )

        if new_order.state == OrderState.REJECTED:
            self._logger.error(f"[{cid}] 改单失败: 下新单被拒绝 ({new_order.reject_reason})")
            return None

        self._logger.info(
            f"[{cid}] 改单成功: 旧订单已撤, 新订单 {new_order.client_order_id} 已创建"
        )
        return new_order

    def _amend_new_first(
        self,
        old_order: ManagedOrder,
        new_price: float,
        new_amount: float,
    ) -> Optional[ManagedOrder]:
        """
        先下新单，再撤旧单（激进策略）

        优点：始终保持挂单覆盖，不会错过行情
        缺点：短暂双重占用保证金
        """
        cid = old_order.client_order_id
        self._logger.info(f"[{cid}] 改单(new_first): price {old_order.price} -> {new_price}")

        # Step 1: 下新单
        new_order = self.submit_order(
            symbol=old_order.symbol,
            side=OrderSide(old_order.side),
            order_type=OrderType(old_order.order_type),
            amount=new_amount,
            price=new_price,
        )

        if new_order.state == OrderState.REJECTED:
            self._logger.error(f"[{cid}] 改单失败: 下新单被拒绝 ({new_order.reject_reason})")
            return None

        # Step 2: 撤旧单（新单已确认，再撤旧单）
        self.cancel_order(cid)

        self._logger.info(
            f"[{cid}] 改单成功: 新订单 {new_order.client_order_id} 已创建, 旧订单已撤"
        )
        return new_order

    # =========================================================================
    # 内部工具
    # =========================================================================

    def _get_order(self, client_order_id: str) -> Optional[ManagedOrder]:
        with self._orders_lock:
            return self._orders.get(client_order_id)

    def _get_op_lock(self, client_order_id: str) -> threading.RLock:
        """获取单个订单的操作防重入锁（RLock 允许同线程重入，支持 amend -> cancel 调用链）"""
        with self._op_locks_lock:
            if client_order_id not in self._op_locks:
                self._op_locks[client_order_id] = threading.RLock()
            return self._op_locks[client_order_id]

    @staticmethod
    def _generate_client_order_id() -> str:
        """生成唯一的 ClientOrderId"""
        return f"om-{uuid.uuid4().hex[:16]}"
