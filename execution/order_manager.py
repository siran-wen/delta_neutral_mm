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
    BOOTSTRAP = 1       # 启动期建仓（InventoryBootstrap）
    HEDGE = 2           # 对冲
    NEW_ORDER = 3       # 新挂单
    QUERY = 4           # 查询
    LOW = 5             # 低优先级


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
    price: Optional[float] = None            # 限价单挂单价（市价单 None）
    avg_fill_price: Optional[float] = None   # 真实加权成交均价（由 create_order 响应 / userFills 回填）
    fee_cost: Optional[float] = None         # 手续费金额（含 builderFee 累加；币种见 fee_currency）
    fee_currency: Optional[str] = None       # 手续费币种（"USDC" / "USOL" 等；None 表示无 fee 数据）
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

    # ---- 追溯：触发订单的模块来源（quoter / hedger / bootstrap / killswitch / amend） ----
    source: str = "unknown"

    # ---- 诊断：order_filled 首次 emit 的时间戳和代码路径 ----
    # 用于回答"hedge 订单从 submit 到最终 emit order_filled 的延迟"和
    # "最终从哪条代码路径 emit"两个问题。幂等写入：第一次 emit 时设置，
    # 后续重复 emit（理论上不应该发生）会被 _emit_order_filled 跳过。
    first_filled_emit_ts: Optional[float] = None
    filled_emit_source: Optional[str] = None

    # ---- 原子改单锁标记 ----
    locked: bool = False

    @property
    def age(self) -> float:
        """订单创建至今的秒数"""
        return time.time() - self.created_at

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
        # 真实成交均价（供 Hedger 成交确认日志 / 成本分析使用，避免与 limit price 混淆）
        gw_avg = getattr(gw_order, "average", None)
        if gw_avg is not None and gw_avg > 0:
            self.avg_fill_price = float(gw_avg)
        # 手续费透传：由 gateway.Order.from_ccxt 解析 fee + builderFee 得到
        gw_fee_cost = getattr(gw_order, "fee_cost", None)
        gw_fee_ccy = getattr(gw_order, "fee_currency", None)
        if gw_fee_cost is not None and gw_fee_cost > 0:
            self.fee_cost = float(gw_fee_cost)
            self.fee_currency = gw_fee_ccy
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

        # ---- 待复查队列：撤单判 CANCELLED 但可能延迟成交的订单 ----
        # 格式：[(recheck_time_ts, client_order_id), ...]
        # 修 Bug B (live_20260422_224158 22:50:46 @88.820) —— 撤单瞬间交易所还在撮合，
        # userFills 当时没同步，判 CANCELLED 后几秒才真成交；5 秒后 reconcile 复查。
        self._pending_cancel_recheck: List[Tuple[float, str]] = []
        self._pending_recheck_lock = threading.Lock()

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
        priority: RequestPriority = RequestPriority.NEW_ORDER,
        source: str = "unknown",
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
            priority:            限速优先级
            source:              触发模块来源（quoter / hedger / bootstrap / ...），
                                 用于排查"这一单是谁挂的"

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
            source=source,
        )

        self._logger.info(
            f"[{cid}] 提交 {symbol} {side.value} {order_type.value} "
            f"{amount}@{price}  源={source}"
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
        if not self._rate_limiter.wait_and_acquire(priority, timeout=3.0):
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
                client_order_id=cid,
            )

            # 收到回执
            ack_ts = time.time()
            managed.latency.exchange_ack_ts = ack_ts
            managed.ack_at = ack_ts

            # ── CCXT 响应归一化诊断（仅 hedger 源，避免 quoter maker 单刷屏）──
            # 目标：区分 "status=closed 立即 FILLED" / "status=open 但 info 有 avgPx
            # (CCXT 未归一化成交)" / "info 也没 avgPx (真的还没成交)" 三种情况。
            # 仅 DEBUG 级别：实盘默认 INFO 不会输出；--log-level DEBUG 时才落日志。
            if source == "hedger":
                try:
                    raw = gw_order.raw or {}
                    info_field = raw.get("info") or {}
                    resp = info_field.get("response") or {}
                    resp_data = resp.get("data") or {}
                    statuses = resp_data.get("statuses") or []
                    avg_px_in_info = None
                    if statuses and isinstance(statuses[0], dict):
                        filled_info = statuses[0].get("filled")
                        if isinstance(filled_info, dict):
                            avg_px_in_info = filled_info.get("avgPx")
                    fee_in_resp = (
                        raw.get("fee") is not None
                        or bool(raw.get("fees"))
                        or info_field.get("builderFee") is not None
                    )
                    self._logger.debug(
                        f"[{cid}] CCXT响应诊断 "
                        f"status={raw.get('status')!r} "
                        f"filled={raw.get('filled')!r} "
                        f"amount={amount} "
                        f"avgPx_in_info={avg_px_in_info!r} "
                        f"fee_in_resp={fee_in_resp}"
                    )
                except Exception as diag_err:
                    # 诊断日志永不阻塞主流程
                    self._logger.debug(
                        f"[{cid}] CCXT响应诊断 抽取失败: "
                        f"{type(diag_err).__name__}: {diag_err}"
                    )

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

            rtt_str = (
                f" rtt={managed.latency.order_rtt:.3f}s"
                if managed.latency.order_rtt else ""
            )
            self._logger.info(
                f"[{cid}] 订单已确认: eid={managed.exchange_order_id}  "
                f"state={managed.state.value}  源={source}{rtt_str}"
            )

            self._emit("order_submitted", managed)

            # ── submit-immediate 终态事件补发 ────────────────────────
            # 当 CCXT 同步响应把订单直接置为终态（激进限价被撮合吃掉 →
            # FILLED；被交易所拒 → REJECTED；边缘 canceled 状态），老实现
            # 只 emit order_submitted，reconcile 的状态过滤又只看
            # OPEN/PARTIAL/STALE，导致 order_filled / order_cancelled /
            # order_rejected 永远不触发，下游 inventory / risk 更新全丢。
            # 复盘：bootstrap 激进下单立即成交 → inventory.positions 没
            # 更新 → 下一笔 SELL 被 BalanceGuard 判余额不足。
            #
            # 发射顺序: submitted → terminal，与正常 OPEN → FILLED 的
            # 事件链语义一致。order_filled 走 _emit_order_filled 助手，
            # 和其他四条路径一样给 first_filled_emit_ts / filled_emit_source
            # 打戳，保证 Hedger 的 [HEDGE-LATENCY] 统计口径统一。
            if managed.state == OrderState.FILLED:
                # submit-immediate 路径的 fee 回填（FILLED 分支独有）：
                # CCXT 对 Hyperliquid 现货 BUY 的同步响应经常缺 fee 字段；
                # reconcile 不会处理 submit-immediate 终态订单，fee 只能
                # 在此处补拉 userFills。只在 fee_cost 缺失时触发（CCXT 已
                # 归一化 fee 的路径不打扰）。拉取失败/userFills 滞后都不
                # 阻塞事件发射，由下次 reconcile 兜底。
                if not managed.fee_cost:
                    try:
                        self._backfill_fee_from_userfills(managed)
                    except Exception as be:
                        self._logger.warning(
                            f"[{cid}] submit-immediate fee 回填失败: "
                            f"{type(be).__name__}: {be}  (reconcile 会再试一次)"
                        )
                self._emit_order_filled(managed, source="submit_immediate")
            elif managed.state == OrderState.CANCELLED:
                self._emit("order_cancelled", managed)
            elif managed.state == OrderState.REJECTED:
                self._emit("order_rejected", managed)

            return managed

        except Exception as e:
            managed.transition_to(OrderState.REJECTED)
            managed.reject_reason = str(e)
            self._logger.error(
                f"[{cid}] 下单失败: 源={source}  exc={type(e).__name__}: {e}"
            )
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
                    # cancel_order 返回 False → 交易所说订单不存在。这可能是：
                    #   a) 已经成交（maker 单被吃掉）——旧版直接判 CANCELLED 会漏记成交
                    #   b) 已经被对账/用户网页撤了
                    # 立刻查 userFills 精确区分，避免漏记成交（见 live_20260422_210538 复盘）
                    final_state = self._infer_final_state_via_trades(managed)
                    if final_state == OrderState.FILLED:
                        # Bug A 竞态保护：reconcile 可能已经在另一个线程把这单判为 FILLED
                        # 并 emit 过了（gateway.cancel_order 阻塞的那 ~1s 里）。如果 state
                        # 已经是 FILLED，说明已 emit，不要重复 emit。
                        # (live_20260422_224158 22:43:17→22:43:18 双重 emit 复盘)
                        if managed.state == OrderState.FILLED:
                            self._logger.debug(
                                f"[{client_order_id}] 撤单时发现已被 reconcile "
                                f"判 FILLED，不重复 emit"
                            )
                            return True
                        managed.transition_to(OrderState.FILLED)
                        avg = getattr(managed, "avg_price", None)
                        self._logger.info(
                            f"[{client_order_id}] 撤单前已成交: "
                            f"filled={managed.filled} avg={avg} "
                            f"(via=userFills after cancel-not-found)"
                        )
                        self._emit_order_filled(managed, source="cancel_recover_userfills")
                        return True

                    managed.transition_to(OrderState.CANCELLED)
                    # Bug B 修复：排入延迟复查队列 —— 撤单瞬间可能正在成交，
                    # userFills 当时没同步；5 秒后 reconcile 会再查一次
                    # (live_20260422_224158 22:50:46 @88.820 漏记复盘)
                    with self._pending_recheck_lock:
                        self._pending_cancel_recheck.append(
                            (time.time() + 5.0, client_order_id)
                        )
                    self._logger.info(
                        f"[{client_order_id}] 撤单确认（交易所无成交记录），"
                        f"5s 后复查防漏记"
                    )

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

    def _emit_order_filled(self, managed: ManagedOrder, source: str) -> None:
        """
        发布 order_filled 事件并记录首次 emit 的时间戳和来源（幂等）。

        source 取值（当前 4 条代码路径，与任务文档一致）:
            - "cancel_recover_userfills" : cancel_order 撤单时 gateway 返回 OrderNotFound，
                                            通过 userFills 找到成交记录
            - "reconcile_pending_recheck": 撤单判 CANCELLED 后 5s 延迟复查（Bug B 修复路径）
            - "reconcile_local_state"    : reconcile 发现交易所无挂单但本地 filled>=amount
            - "reconcile_userfills"      : reconcile 发现交易所无挂单 → userFills 匹配成交

        首次调用时写入 first_filled_emit_ts / filled_emit_source；
        重复调用不覆盖已有值（防守性幂等，正常流程不应重复 emit）。
        """
        if managed.first_filled_emit_ts is None:
            managed.first_filled_emit_ts = time.time()
            managed.filled_emit_source = source
        self._emit("order_filled", managed)

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

        0. 延迟复查队列（Bug B 修复）—— 撤单瞬间可能正在成交，5s 后再查 userFills
        1. 检测 STALE 订单（PENDING_NEW 超时）
        2. 拉取交易所挂单列表
        3. 交叉比对解决冲突
        """
        now = time.time()

        # ---- Step 0: 延迟复查队列（Bug B 修复）----
        to_recheck: List[str] = []
        with self._pending_recheck_lock:
            remaining: List[Tuple[float, str]] = []
            for recheck_ts, cid in self._pending_cancel_recheck:
                if recheck_ts <= now:
                    to_recheck.append(cid)
                else:
                    remaining.append((recheck_ts, cid))
            self._pending_cancel_recheck = remaining

        for cid in to_recheck:
            with self._orders_lock:
                managed = self._orders.get(cid)
            if managed is None:
                continue
            # 只复查仍处于 CANCELLED 状态的（其他状态已被正常处理过）
            if managed.state != OrderState.CANCELLED:
                continue
            final = self._infer_final_state_via_trades(managed)
            if final == OrderState.FILLED:
                # 5 秒后延迟发现成交 → 强制 transition 到 FILLED
                # 注意: CANCELLED 是终态，transition_to 允许无约束赋值，这里展开
                # 改字段让意图显式（previous_state 保留为 CANCELLED 便于事后溯源）
                old_state = managed.state
                managed.previous_state = old_state
                managed.state = OrderState.FILLED
                managed.state_changed_at = time.time()
                managed.last_updated_at = managed.state_changed_at
                avg = getattr(managed, "avg_price", None)
                self._logger.warning(
                    f"[{cid}] 撤单 5 秒后延迟发现成交，"
                    f"state 修正 {old_state.value} -> FILLED  "
                    f"filled={managed.filled} avg={avg}"
                )
                self._emit_order_filled(managed, source="reconcile_pending_recheck")

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
            self._logger.warning(f"对账巡检拉取挂单失败: {e}")
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
                # 交易所侧没有这个订单 —— 可能是：成交 / 撤销 / 丢失
                if managed.state == OrderState.PENDING_CANCEL:
                    # 已主动发起撤单 → CANCELLED
                    managed.transition_to(OrderState.CANCELLED)
                    self._emit("order_cancelled", managed)
                    continue

                if managed.state not in (
                    OrderState.OPEN,
                    OrderState.PARTIALLY_FILLED,
                    OrderState.STALE,
                ):
                    continue

                # 本地已经观测到 filled 达到 amount → 直接 FILLED
                if managed.filled and managed.filled >= managed.amount:
                    managed.transition_to(OrderState.FILLED)
                    self._logger.info(
                        f"[{managed.client_order_id}] 对账发现已成交: "
                        f"filled={managed.filled}  "
                        f"源={managed.source}  age={managed.age:.1f}s  via=local_state"
                    )
                    self._emit_order_filled(managed, source="reconcile_local_state")
                    continue

                # 尝试从成交历史推断真实结局
                final_state = self._infer_final_state_via_trades(managed)
                if final_state == OrderState.FILLED:
                    managed.transition_to(OrderState.FILLED)
                    self._logger.info(
                        f"[{managed.client_order_id}] 对账发现已成交: "
                        f"filled={managed.filled}  "
                        f"源={managed.source}  age={managed.age:.1f}s  via=trade_history"
                    )
                    self._emit_order_filled(managed, source="reconcile_userfills")
                elif final_state == OrderState.CANCELLED:
                    managed.transition_to(OrderState.CANCELLED)
                    self._emit("order_cancelled", managed)
                else:
                    # 订单从交易所 open_orders 消失、userFills 也没匹配 —— 可能是：
                    #   a) 真成交了但 userFills 还没落盘（给 1-2 个对账周期）
                    #   b) 被对手方撮合后原生端点暂时返回不全
                    #   c) 极少见：用户在网页撤单 / Hyperliquid 清理
                    # 旧版在此处启发式判 FILLED，但已证实会把活着的现货挂单错判成 FILLED
                    # (live_20260422_210538 复盘)。改为保持当前 state，下轮再查。
                    self._logger.warning(
                        f"[{managed.client_order_id}] 订单在交易所消失但 userFills 无匹配，"
                        f"保持 state={managed.state.value} 等下轮对账 "
                        f"(eid={eid}, amount={managed.amount}, age={managed.age:.1f}s, "
                        f"源={managed.source})"
                    )
                    if managed.age > 600:
                        self._logger.error(
                            f"[{managed.client_order_id}] ⚠️ 订单状态未知超过 10 分钟，"
                            f"可能需要人工对账 (fetch_positions + fetch_spot_balance + "
                            f"historicalOrders eid={eid})"
                        )

    def _infer_final_state_via_trades(
        self, managed: "ManagedOrder"
    ) -> Optional[OrderState]:
        """
        通过 Hyperliquid 原生 userFills 端点精确匹配订单的最终状态。

        相较旧版 CCXT fetch_my_trades 的改进：
          1. 现货和永续都能拉到（CCXT swap 模式对 Hyperliquid 现货成交返回为空）
          2. 用 oid 精确匹配，不依赖 symbol 字段（避免 CCXT base/baseName 不一致坑）
          3. 一次拉回最近 2000 笔，覆盖一个对账周期内的所有成交
          4. 聚合 userFills 的 fee 字段到 ManagedOrder（修复 B2 bug：做市订单几乎
             都走延迟对账路径，原实现漏提取 fee 导致 PnL Fees 始终为 0）

        返回:
            OrderState.FILLED : 找到匹配的成交记录，更新 managed.filled / remaining /
                                avg_price / fee_cost / fee_currency
            None              : 拉取失败、未实现或无匹配（由上层决定怎么处理）
        """
        eid = managed.exchange_order_id
        if not eid:
            return None

        # 只拉订单创建之后的成交，减少传输量并避免匹配到无关老订单
        start_ms: Optional[int] = None
        if managed.created_at:
            # 向前预留 1 秒容差，防止时钟抖动
            start_ms = int(managed.created_at * 1000) - 1000

        try:
            fills = self._gateway.fetch_user_fills(start_time_ms=start_ms)
        except AttributeError:
            # 非 Hyperliquid gateway 不实现此接口 → 返回 None，保留上层决策权
            return None
        except Exception as e:
            self._logger.warning(
                f"[{managed.client_order_id}] userFills 拉取失败: {e}"
            )
            return None

        # userFills 字段说明（来自 A 阶段 tests/diagnose_hl_spot_orders.py）：
        #   f['oid']        : 订单 id（字符串或整数）
        #   f['sz']/'px'    : 成交量 / 成交价（字符串）
        #   f['fee']        : 主 fee 数值（字符串）
        #   f['feeToken']   : 主 fee 币种（"USDC" / "USOL" / "SOL" 等）
        #   f['builderFee'] : 可选，单位 USDC 的建单方额外加收（字符串）
        #   f['time']       : 毫秒时间戳
        #
        # fee 聚合规则（修复 B2 遗漏，与 Gateway.Order.from_ccxt 语义一致）：
        #   1. 只聚合 oid 匹配的 fill 记录
        #   2. Hyperliquid 现货 BUY 的 fee 以 base coin 扣（USOL），
        #      现货 SELL / 永续 以 USDC 扣；同一订单多笔 fill feeToken 应一致，
        #      但极端场景下不一致时按"出现次数最多的币种"投票选订单级币种
        #   3. builderFee 永远是 USDC，按主 fee 币种规则累加：
        #      - 主 fee 是 USDC → 直接相加
        #      - 主 fee 是 base coin → 按成交均价折算成 base coin 再加
        #      - 只有 builderFee 没有主 fee → 整笔按 USDC 记入
        #   4. userFills 无 fee 字段（边角 case）→ 保持 None，不 raise

        # ── 过滤 + 累加 fill 级字段（供 state 推断使用）─────────────────
        # fee 聚合从这里抽到 _aggregate_fee_from_fills，submit-immediate 共用
        matching_fills: List[Dict] = []
        total_filled = 0.0
        last_price = 0.0
        last_time = 0

        for f in fills or []:
            if str(f.get("oid", "")) != str(eid):
                continue
            try:
                sz = float(f.get("sz", 0) or 0)
                px = float(f.get("px", 0) or 0)
            except (TypeError, ValueError):
                # sz/px 解析失败：该 fill 整体丢弃，不计入 total 也不计入 fee 聚合，
                # 保持与旧版完全一致的行为（旧版是 continue 跳过整条 fill）
                continue
            matching_fills.append(f)
            total_filled += sz
            if px > 0:
                last_price = px
            try:
                t = int(f.get("time", 0) or 0)
                if t > last_time:
                    last_time = t
            except (TypeError, ValueError):
                pass

        if total_filled <= 0:
            return None

        # ── fee 聚合（共享 helper，submit-immediate 路径也走这一套）────
        final_fee_cost, final_fee_ccy = self._aggregate_fee_from_fills(
            matching_fills, managed, avg_px_hint=last_price
        )

        # ── 回填 ManagedOrder ────────────────────────────────────────────
        managed.filled = total_filled
        managed.remaining = max(managed.amount - total_filled, 0.0)
        if last_price > 0:
            managed.avg_price = last_price
        # 只在取到有效 fee 时才写，避免把已有值（比如 from_ccxt 路径先写入的）覆盖为 None
        if final_fee_cost is not None and final_fee_ccy:
            managed.fee_cost = final_fee_cost
            managed.fee_currency = final_fee_ccy
        return OrderState.FILLED

    def _aggregate_fee_from_fills(
        self,
        fills: List[Dict],
        managed: "ManagedOrder",
        avg_px_hint: float,
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        从已按 oid 过滤的 userFills 列表聚合订单级 fee。

        复用路径:
          a. _infer_final_state_via_trades（reconcile 循环的成交推断）
          b. _backfill_fee_from_userfills（submit-immediate 路径，CCXT 同步
             响应不带 fee 时的补拉兜底）

        聚合规则（同 Gateway.Order.from_ccxt 语义）：
          1. Hyperliquid 现货 BUY 的 fee 以 base coin（USOL 等）扣，
             现货 SELL / 永续以 USDC 扣；同订单多笔 fill 的 feeToken 通常
             一致，不一致时按"出现次数最多的币种"投票选订单级币种。
          2. builderFee 永远是 USDC：
             - 主 fee 是 USDC → 直接相加
             - 主 fee 是 base coin → 按 avg_px_hint 折算成 base coin 再加
             - 只有 builderFee 没有主 fee → 整笔按 USDC 记入
          3. 任一条 fill 的 fee 字段解析异常跳过本条 fee，不影响其他 fill。
          4. 无可聚合 fee 时返回 (None, None)，调用方自行决定是否覆写。

        Args:
            fills:        已按 oid 过滤的 fill 列表（空列表返回 (None, None)）
            managed:      ManagedOrder，仅用于日志中的 cid 显示
            avg_px_hint:  builderFee 从 USDC 折算成 base coin 用的价格；
                          <=0 时若主 fee 是 base coin 则丢弃 builderFee（debug 日志）

        Returns:
            (fee_cost, fee_currency) — 聚合失败时 (None, None)
        """
        main_fee_sum: float = 0.0
        main_fee_ccy_votes: Dict[str, int] = {}
        builder_fee_usdc_sum: float = 0.0

        for f in fills or []:
            # 主 fee（fee + feeToken）
            try:
                fee_val = f.get("fee")
                fee_ccy = f.get("feeToken")
                if fee_val is not None and fee_ccy:
                    fee_num = float(fee_val)
                    if fee_num > 0:
                        main_fee_sum += fee_num
                        main_fee_ccy_votes[fee_ccy] = (
                            main_fee_ccy_votes.get(fee_ccy, 0) + 1
                        )
            except (TypeError, ValueError):
                pass
            # builderFee（USDC）
            try:
                bfee = f.get("builderFee")
                if bfee is not None:
                    bfee_num = float(bfee)
                    if bfee_num > 0:
                        builder_fee_usdc_sum += bfee_num
            except (TypeError, ValueError):
                pass

        # 选主 fee 币种：投票多数（单订单多笔 fill 通常只有 1 种）
        main_fee_ccy: Optional[str] = None
        if main_fee_ccy_votes:
            main_fee_ccy = max(main_fee_ccy_votes.items(), key=lambda x: x[1])[0]

        # 合并 builderFee 到主 fee
        final_fee_cost: float = main_fee_sum
        final_fee_ccy: Optional[str] = main_fee_ccy

        if builder_fee_usdc_sum > 0:
            if main_fee_ccy == "USDC":
                final_fee_cost += builder_fee_usdc_sum
            elif main_fee_ccy is None:
                final_fee_cost = builder_fee_usdc_sum
                final_fee_ccy = "USDC"
            elif avg_px_hint > 0:
                final_fee_cost += builder_fee_usdc_sum / avg_px_hint
            else:
                self._logger.debug(
                    f"[{managed.client_order_id}] builderFee 无法折算到 base coin "
                    f"(avg_px_hint=0)，丢弃: builderFee={builder_fee_usdc_sum}"
                )

        if final_fee_cost > 0 and final_fee_ccy:
            return final_fee_cost, final_fee_ccy
        return None, None

    def _backfill_fee_from_userfills(self, managed: "ManagedOrder") -> None:
        """
        submit-immediate 路径专用：CCXT 同步响应不含 fee 时，从 userFills 补拉。

        目的：Hyperliquid 现货 BUY（taker）的 fee 扣 base coin（USOL/UBTC…），
        CCXT 对 create_order 的同步响应通常不带 fee 字段，fee 只在 userFills
        能拿到。老实现依赖 reconcile 循环走 _infer_final_state_via_trades 回填，
        但 submit-immediate 终态订单会被 reconcile 跳过（非 OPEN/PARTIAL/STALE），
        fee 永远回填不上。
        复盘：live_20260424_172016 bootstrap 两笔 fill 后 Fees=-0.0000，
        下一笔 quoter SELL 因本地余额漏扣 fee 认为够而被交易所拒。

        行为:
          - 只写 fee_cost / fee_currency；不改 filled / remaining / avg_fill_price
            （这些已由 sync_from_exchange 用 CCXT 响应正确写入）
          - userFills 滞后（oid 还未落盘，1-2s）→ 留空，不抛异常，等下次
            reconcile 的 _infer_final_state_via_trades 再补
          - 非 Hyperliquid 网关不实现 fetch_user_fills → AttributeError 静默跳过
        """
        eid = managed.exchange_order_id
        if not eid:
            return

        start_ms: Optional[int] = None
        if managed.created_at:
            # 向前预留 1 秒容差，防止时钟抖动
            start_ms = int(managed.created_at * 1000) - 1000

        try:
            all_fills = self._gateway.fetch_user_fills(start_time_ms=start_ms)
        except AttributeError:
            # 非 Hyperliquid gateway（或 mock）不实现此接口 → 静默跳过
            return

        matching = [
            f for f in all_fills or []
            if str(f.get("oid", "")) == str(eid)
        ]

        if not matching:
            # userFills 滞后 → 留空，由 reconcile 下轮兜底
            self._logger.debug(
                f"[{managed.client_order_id}] submit-immediate fee 回填: "
                f"userFills 无匹配 oid={eid}（可能滞后 1-2s），"
                f"留空待 reconcile 补拉"
            )
            return

        # avg_px_hint 优先用 ManagedOrder.avg_fill_price（sync_from_exchange
        # 已从 CCXT 'average' 或 Hyperliquid 原生 avgPx 回填）；缺失时从第
        # 一条匹配 fill 的 px 取，最后兜底 0（_aggregate_fee_from_fills 里
        # 遇到 0 会对 base-coin fee 的 builderFee 折算降级丢弃）
        avg_px: float = float(managed.avg_fill_price or 0.0)
        if avg_px <= 0:
            for f in matching:
                try:
                    px = float(f.get("px", 0) or 0)
                    if px > 0:
                        avg_px = px
                        break
                except (TypeError, ValueError):
                    pass

        fee_cost, fee_ccy = self._aggregate_fee_from_fills(
            matching, managed, avg_px_hint=avg_px
        )

        if fee_cost is not None and fee_ccy:
            managed.fee_cost = fee_cost
            managed.fee_currency = fee_ccy
            self._logger.info(
                f"[{managed.client_order_id}] submit-immediate fee 回填: "
                f"fee_cost={fee_cost:.8f} {fee_ccy} "
                f"(from {len(matching)} userFill(s))"
            )

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
            # 冷启动拉不到挂单意味着本地对账基线缺失，继续运行会让本地订单簿
            # 与交易所状态发散——直接抛出阻止启动，避免双挂/漏撤。
            self._logger.error(f"冷启动恢复失败: {e}", exc_info=True)
            raise

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
            source=f"amend:{old_order.source}",
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
            source=f"amend:{old_order.source}",
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
