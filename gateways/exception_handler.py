#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异常处理与自动重连模块

核心职责:
    1. 异常分类   — 区分瞬态（可重试）与致命异常
    2. 连接监控   — 周期性心跳检测，发现断连第一时间撤单
    3. 自动重连   — 指数退避重连，恢复后自动重建状态

设计原则:
    - 安全第一: 断连 → 立即撤掉所有挂单 → 再尝试重连
    - 最小侵入: 通过组合（而非继承）与 Gateway / OrderManager 协作
    - 可观测性: 所有状态变更均通过事件回调通知上层

用法:
    from gateways import GatewayFactory, OrderManager, ConnectionMonitor

    gw = GatewayFactory.create("config/hyperliquid_config.yaml")
    gw.connect()

    om = OrderManager(gw)
    om.start()

    monitor = ConnectionMonitor(gw, om)
    monitor.start()

    # ... 运行策略 ...

    monitor.stop()
    om.stop()
    gw.disconnect()
"""

import time
import threading
import logging
from enum import Enum
from typing import Optional, Callable, Dict, List, Any
from dataclasses import dataclass, field

try:
    import ccxt
except ImportError:
    ccxt = None  # type: ignore

from gateways.gateway import BaseGateway, GatewayStatus


# =============================================================================
# 1. 异常分类器
# =============================================================================

class ExceptionSeverity(Enum):
    """异常严重等级"""
    TRANSIENT = "transient"          # 瞬态异常，可重试 (网络抖动、超时)
    RATE_LIMITED = "rate_limited"    # 被限流，需等待后重试
    FATAL = "fatal"                  # 致命异常，不可重试 (认证失败、参数错误)
    UNKNOWN = "unknown"              # 未知异常


class ExceptionClassifier:
    """
    异常分类器

    将交易所返回的各种异常归类为瞬态/限流/致命，
    供重连策略和上层逻辑决定是否重试。

    分类依据:
        - 瞬态 (TRANSIENT):  NetworkError, RequestTimeout, ExchangeNotAvailable
        - 限流 (RATE_LIMITED): RateLimitExceeded, DDoSProtection
        - 致命 (FATAL):       AuthenticationError, InvalidOrder, InsufficientFunds
        - 未知 (UNKNOWN):     其他异常
    """

    # ccxt 异常到严重等级的映射
    _CCXT_MAPPING: Dict[str, ExceptionSeverity] = {}

    @classmethod
    def _init_ccxt_mapping(cls) -> None:
        """初始化 ccxt 异常映射（延迟加载，避免 ccxt 未安装时报错）"""
        if cls._CCXT_MAPPING or ccxt is None:
            return
        cls._CCXT_MAPPING = {
            # ---- 瞬态 ----
            "NetworkError": ExceptionSeverity.TRANSIENT,
            "RequestTimeout": ExceptionSeverity.TRANSIENT,
            "ExchangeNotAvailable": ExceptionSeverity.TRANSIENT,
            # ---- 限流 ----
            "RateLimitExceeded": ExceptionSeverity.RATE_LIMITED,
            "DDoSProtection": ExceptionSeverity.RATE_LIMITED,
            # ---- 致命 ----
            "AuthenticationError": ExceptionSeverity.FATAL,
            "InvalidOrder": ExceptionSeverity.FATAL,
            "InsufficientFunds": ExceptionSeverity.FATAL,
            "BadRequest": ExceptionSeverity.FATAL,
            "BadSymbol": ExceptionSeverity.FATAL,
            "PermissionDenied": ExceptionSeverity.FATAL,
            "AccountNotEnabled": ExceptionSeverity.FATAL,
        }

    @classmethod
    def classify(cls, error: Exception) -> ExceptionSeverity:
        """
        将异常分类

        Args:
            error: 捕获的异常

        Returns:
            ExceptionSeverity 枚举值
        """
        cls._init_ccxt_mapping()

        # 原生网络异常
        if isinstance(error, (ConnectionError, TimeoutError, OSError)):
            return ExceptionSeverity.TRANSIENT

        # ccxt 异常
        error_type = type(error).__name__
        if error_type in cls._CCXT_MAPPING:
            return cls._CCXT_MAPPING[error_type]

        # ccxt 基类层级判断
        if ccxt is not None:
            if isinstance(error, ccxt.NetworkError):
                return ExceptionSeverity.TRANSIENT
            if isinstance(error, ccxt.AuthenticationError):
                return ExceptionSeverity.FATAL

        return ExceptionSeverity.UNKNOWN

    @classmethod
    def is_retryable(cls, error: Exception) -> bool:
        """该异常是否值得重试"""
        severity = cls.classify(error)
        return severity in (ExceptionSeverity.TRANSIENT, ExceptionSeverity.RATE_LIMITED)


# =============================================================================
# 2. 重连策略配置
# =============================================================================

@dataclass
class ReconnectPolicy:
    """
    重连策略参数

    Attributes:
        max_retries:         最大重试次数 (0 = 无限重试)
        initial_delay:       首次重试延迟 (秒)
        max_delay:           最大重试延迟 (秒)
        backoff_factor:      退避倍率 (每次失败后延迟翻倍)
        heartbeat_interval:  心跳检测间隔 (秒)
        heartbeat_timeout:   单次心跳超时 (秒)
        cancel_on_disconnect: 断连时是否立即撤单 (强烈建议 True)
    """
    max_retries: int = 0                # 0 = 无限重试
    initial_delay: float = 1.0          # 首次等待 1 秒
    max_delay: float = 60.0             # 最大等待 60 秒
    backoff_factor: float = 2.0         # 指数退避
    heartbeat_interval: float = 5.0     # 每 5 秒检查一次
    heartbeat_timeout: float = 10.0     # 心跳超时 10 秒
    cancel_on_disconnect: bool = True   # 断连即撤单


# =============================================================================
# 3. 连接监控器
# =============================================================================

class MonitorState(Enum):
    """监控器状态"""
    IDLE = "idle"                   # 未启动
    MONITORING = "monitoring"       # 正常监控中
    DISCONNECTED = "disconnected"   # 已检测到断连
    CANCELLING = "cancelling"       # 正在紧急撤单
    RECONNECTING = "reconnecting"   # 正在重连
    STOPPED = "stopped"             # 已停止


class ConnectionMonitor:
    """
    连接监控器

    后台线程周期性地通过轻量 API 调用（心跳）检测网关连接状态。
    当发现连接异常时:
        1. 立即通过 OrderManager 撤掉所有挂单 (安全第一)
        2. 按指数退避策略尝试重连
        3. 重连成功后触发事件，上层可决定是否恢复策略

    事件:
        - on_disconnect:       连接断开
        - on_reconnecting:     正在尝试重连 (附带重试次数)
        - on_reconnected:      重连成功
        - on_reconnect_failed: 重连失败（达到最大重试次数）
        - on_cancel_complete:  紧急撤单完成
        - on_heartbeat:        每次心跳成功
    """

    def __init__(
        self,
        gateway: BaseGateway,
        order_manager=None,
        policy: Optional[ReconnectPolicy] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            gateway:        交易所网关实例
            order_manager:  订单管理器 (可选，设置后断连时自动撤单)
            policy:         重连策略 (None 则使用默认值)
            logger:         日志记录器
        """
        self._gateway = gateway
        self._order_manager = order_manager
        self._policy = policy or ReconnectPolicy()
        self._logger = logger or logging.getLogger("ConnectionMonitor")

        # ---- 状态 ----
        self._state = MonitorState.IDLE
        self._running = False
        self._consecutive_failures = 0    # 连续心跳失败次数
        self._total_reconnects = 0        # 累计重连次数
        self._last_heartbeat_ok: float = 0.0   # 上次心跳成功时间
        self._last_error: Optional[Exception] = None

        # ---- 线程 ----
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # ---- 回调 ----
        self._callbacks: Dict[str, List[Callable]] = {}

    # =========================================================================
    # 属性
    # =========================================================================

    @property
    def state(self) -> MonitorState:
        return self._state

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def total_reconnects(self) -> int:
        return self._total_reconnects

    @property
    def last_heartbeat_ok(self) -> float:
        """上次心跳成功的 epoch 时间戳"""
        return self._last_heartbeat_ok

    @property
    def last_error(self) -> Optional[Exception]:
        return self._last_error

    @property
    def stats(self) -> Dict[str, Any]:
        """运行统计"""
        return {
            "state": self._state.value,
            "running": self._running,
            "consecutive_failures": self._consecutive_failures,
            "total_reconnects": self._total_reconnects,
            "last_heartbeat_ok": self._last_heartbeat_ok,
            "last_error": str(self._last_error) if self._last_error else None,
            "seconds_since_last_heartbeat": (
                round(time.time() - self._last_heartbeat_ok, 1)
                if self._last_heartbeat_ok > 0 else None
            ),
        }

    # =========================================================================
    # 生命周期
    # =========================================================================

    def start(self) -> None:
        """启动连接监控"""
        if self._running:
            self._logger.warning("ConnectionMonitor 已在运行")
            return

        self._running = True
        self._stop_event.clear()
        self._state = MonitorState.MONITORING
        self._last_heartbeat_ok = time.time()

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="ConnMonitor",
            daemon=True,
        )
        self._monitor_thread.start()
        self._logger.info(
            f"ConnectionMonitor 已启动 "
            f"(心跳间隔={self._policy.heartbeat_interval}s, "
            f"退避={self._policy.initial_delay}~{self._policy.max_delay}s)"
        )

    def stop(self) -> None:
        """停止连接监控"""
        if not self._running:
            return

        self._logger.info("ConnectionMonitor 停止中...")
        self._running = False
        self._stop_event.set()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=self._policy.heartbeat_interval + 5)

        self._state = MonitorState.STOPPED
        self._logger.info("ConnectionMonitor 已停止")

    # =========================================================================
    # 事件回调
    # =========================================================================

    def on(self, event_name: str, callback: Callable) -> None:
        """
        注册事件回调

        事件:
            - on_disconnect(error):        连接断开
            - on_reconnecting(attempt, delay): 正在尝试重连
            - on_reconnected():            重连成功
            - on_reconnect_failed(error):  重连彻底失败
            - on_cancel_complete(count):   紧急撤单完成
            - on_heartbeat():              心跳成功
        """
        if event_name not in self._callbacks:
            self._callbacks[event_name] = []
        self._callbacks[event_name].append(callback)

    def _emit(self, event_name: str, *args) -> None:
        for cb in self._callbacks.get(event_name, []):
            try:
                cb(*args)
            except Exception as e:
                self._logger.error(f"回调 '{event_name}' 异常: {e}")

    # =========================================================================
    # 核心: 监控主循环
    # =========================================================================

    def _monitor_loop(self) -> None:
        """监控主循环"""
        while self._running:
            # 等待到下一次心跳 (可被 stop_event 中断)
            interrupted = self._stop_event.wait(timeout=self._policy.heartbeat_interval)
            if interrupted or not self._running:
                break

            try:
                self._heartbeat()
            except Exception as e:
                self._logger.error(f"监控循环未预期异常: {e}")

    def _heartbeat(self) -> None:
        """
        执行一次心跳检测

        通过一个轻量的公共 API 调用来判断连接是否正常。
        """
        try:
            # 使用 fetch_ticker 作为心跳（轻量且无副作用）
            # 如果网关已有缓存的 markets，用第一个交易对；否则回退
            self._do_heartbeat_call()

            # 心跳成功
            if self._consecutive_failures > 0:
                self._logger.info(
                    f"心跳恢复正常 (此前连续失败 {self._consecutive_failures} 次)"
                )
            self._consecutive_failures = 0
            self._last_heartbeat_ok = time.time()
            self._last_error = None

            if self._state != MonitorState.MONITORING:
                self._state = MonitorState.MONITORING

            self._emit("on_heartbeat")

        except Exception as e:
            self._consecutive_failures += 1
            self._last_error = e
            severity = ExceptionClassifier.classify(e)

            self._logger.warning(
                f"心跳失败 [{self._consecutive_failures}]: "
                f"{type(e).__name__}: {e}  (severity={severity.value})"
            )

            # 致命异常（如认证失败）不触发自动重连
            if severity == ExceptionSeverity.FATAL:
                self._logger.error(f"致命异常，跳过自动重连: {e}")
                self._emit("on_disconnect", e)
                return

            # 连续失败 >= 2 次才触发断连处理（避免单次网络抖动误判）
            if self._consecutive_failures >= 2:
                self._handle_disconnect(e)

    def _do_heartbeat_call(self) -> None:
        """
        执行心跳 API 调用

        优先用 fetch_status()，其次用 fetch_ticker()。
        """
        gw = self._gateway

        # 如果网关已断开，直接抛出
        if gw.status == GatewayStatus.DISCONNECTED:
            raise ConnectionError("网关已处于断开状态")

        # 尝试轻量调用
        try:
            # 优先: ccxt 的 fetch_status (极轻量)
            if hasattr(gw, 'exchange') and gw.exchange is not None:
                exchange = gw.exchange
                if hasattr(exchange, 'fetch_time'):
                    exchange.fetch_time()
                    return
                # 回退: fetch_ticker 取一个活跃的交易对
                exchange.fetch_ticker("BTC/USDC:USDC")
                return
        except AttributeError:
            pass

        # 最终回退: 通过 gateway 的 fetch_ticker
        gw.fetch_ticker("BTC/USDC:USDC")

    # =========================================================================
    # 核心: 断连处理流程
    # =========================================================================

    def _handle_disconnect(self, error: Exception) -> None:
        """
        断连处理主流程

        安全顺序:
            1. 标记断连状态
            2. 紧急撤单 (安全第一)
            3. 更新网关状态
            4. 尝试自动重连
        """
        self._logger.error(
            f"检测到连接断开! 连续失败 {self._consecutive_failures} 次, "
            f"最后错误: {type(error).__name__}: {error}"
        )

        self._state = MonitorState.DISCONNECTED
        self._emit("on_disconnect", error)

        # ---- Step 1: 紧急撤单 ----
        if self._policy.cancel_on_disconnect:
            self._emergency_cancel()

        # ---- Step 2: 更新网关状态 ----
        self._gateway._set_status(GatewayStatus.RECONNECTING, "自动重连中")

        # ---- Step 3: 尝试重连 ----
        self._reconnect_loop()

    def _emergency_cancel(self) -> None:
        """
        紧急撤单

        断连时第一时间撤掉所有挂单，防止无法管理的订单暴露在市场中。
        """
        if self._order_manager is None:
            self._logger.warning("未绑定 OrderManager，跳过紧急撤单")
            return

        self._state = MonitorState.CANCELLING
        self._logger.warning(">>> 紧急撤单: 正在撤掉所有挂单 <<<")

        try:
            # 方法1: 通过 OrderManager 撤单（维护本地订单簿状态）
            cancelled = self._order_manager.cancel_all()
            self._logger.warning(f">>> 紧急撤单完成: 通过 OrderManager 撤掉 {cancelled} 个挂单 <<<")

            # 方法2: 兜底 — 直接通过 Gateway 撤单（确保交易所侧无遗漏）
            try:
                gw_cancelled = self._gateway.cancel_all_orders()
                if gw_cancelled > 0:
                    self._logger.warning(
                        f">>> 兜底撤单: Gateway 层额外撤掉 {gw_cancelled} 个挂单 <<<")
            except Exception as e:
                self._logger.error(f"Gateway 兜底撤单失败 (网络可能已断): {e}")

            self._emit("on_cancel_complete", cancelled)

        except Exception as e:
            self._logger.error(f"紧急撤单异常: {e}")

            # 最后手段: 即便 OM 失败也尝试直接通过 Gateway 撤
            try:
                self._gateway.cancel_all_orders()
                self._logger.warning("通过 Gateway 兜底撤单 (OM 异常后)")
            except Exception as e2:
                self._logger.critical(f"所有撤单手段均失败! {e2}")

    # =========================================================================
    # 核心: 自动重连
    # =========================================================================

    def _reconnect_loop(self) -> None:
        """
        重连主循环

        使用指数退避策略:
            delay = min(initial_delay * backoff_factor^attempt, max_delay)

        当 max_retries > 0 时，达到上限后放弃；
        当 max_retries == 0 时，无限重试。
        """
        self._state = MonitorState.RECONNECTING
        attempt = 0
        delay = self._policy.initial_delay

        while self._running:
            attempt += 1

            # 检查是否达到重试上限
            if 0 < self._policy.max_retries < attempt:
                self._logger.error(
                    f"重连失败: 已达最大重试次数 ({self._policy.max_retries})"
                )
                self._state = MonitorState.DISCONNECTED
                self._emit("on_reconnect_failed", self._last_error)
                return

            self._logger.info(
                f"第 {attempt} 次重连尝试 (等待 {delay:.1f}s 后开始)..."
            )
            self._emit("on_reconnecting", attempt, delay)

            # 等待（可被 stop_event 中断）
            interrupted = self._stop_event.wait(timeout=delay)
            if interrupted or not self._running:
                self._logger.info("重连被中断")
                return

            # 尝试重连
            try:
                success = self._attempt_reconnect()
                if success:
                    self._on_reconnect_success(attempt)
                    return
            except Exception as e:
                severity = ExceptionClassifier.classify(e)
                self._logger.warning(
                    f"第 {attempt} 次重连失败: {type(e).__name__}: {e} "
                    f"(severity={severity.value})"
                )
                self._last_error = e

                # 致命异常不再重试
                if severity == ExceptionSeverity.FATAL:
                    self._logger.error("致命异常，停止重连")
                    self._state = MonitorState.DISCONNECTED
                    self._emit("on_reconnect_failed", e)
                    return

            # 指数退避
            delay = min(delay * self._policy.backoff_factor, self._policy.max_delay)

    def _attempt_reconnect(self) -> bool:
        """
        执行一次重连尝试

        Returns:
            是否成功
        """
        gw = self._gateway

        # 先断开旧连接（清理残留状态）
        try:
            gw.disconnect()
        except Exception:
            pass

        # 重新连接
        return gw.connect()

    def _on_reconnect_success(self, attempt: int) -> None:
        """重连成功后的处理"""
        self._total_reconnects += 1
        self._consecutive_failures = 0
        self._last_heartbeat_ok = time.time()
        self._last_error = None
        self._state = MonitorState.MONITORING

        self._logger.info(
            f"重连成功! (第 {attempt} 次尝试, "
            f"累计重连 {self._total_reconnects} 次)"
        )

        # 通知 OrderManager 执行冷启动恢复（同步交易所侧挂单）
        if self._order_manager is not None:
            try:
                self._logger.info("重连后执行冷启动恢复...")
                self._order_manager._cold_start_recovery()
                self._logger.info("冷启动恢复完成")
            except Exception as e:
                self._logger.error(f"重连后冷启动恢复失败: {e}")

        self._emit("on_reconnected")

    # =========================================================================
    # 外部集成
    # =========================================================================

    def set_order_manager(self, order_manager) -> None:
        """
        绑定 OrderManager (支持延迟绑定)

        Args:
            order_manager: OrderManager 实例
        """
        self._order_manager = order_manager
        self._logger.info("已绑定 OrderManager")
