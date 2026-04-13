#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
风控限制模块

实盘级别的交易风控保护，核心功能：

1. 持仓限制 (PositionLimiter)
   - 实时追踪各交易对净头寸（Delta）
   - 超过阈值时自动停止挂单，并触发 Hedger 回调
   - 支持单品种上限 + 全局上限

2. 价格保护 (FatFingerGuard)
   - 挂单价偏离中价过远时拒绝执行
   - 防止 "胖手指" 错误导致巨额损失
   - 支持百分比 / 绝对值两种限制模式

3. 一键关停 (KillSwitch)
   - 捕捉 Ctrl+C / SIGTERM / 未预期异常
   - 立即撤销所有挂单
   - 以市价单平掉全部风险头寸
   - 幂等设计，只触发一次
"""

import os
import sys
import time
import signal
import threading
import logging
from enum import Enum, auto
from typing import Optional, Dict, List, Any, Callable, Tuple
from dataclasses import dataclass, field


# =============================================================================
# 1. 数据模型
# =============================================================================

class RiskBreach(Enum):
    """风控触发类型"""
    POSITION_LIMIT_SYMBOL = "position_limit_symbol"   # 单品种 Delta 超限
    POSITION_LIMIT_GLOBAL = "position_limit_global"    # 全局 Delta 超限
    FAT_FINGER_PCT = "fat_finger_pct"                  # 价格偏离（百分比）
    FAT_FINGER_ABS = "fat_finger_abs"                  # 价格偏离（绝对值）
    KILL_SWITCH = "kill_switch"                        # 一键关停


@dataclass
class RiskCheckResult:
    """
    风控检查结果

    Attributes:
        passed:  是否通过
        breach:  触发的风控类型 (None 表示通过)
        reason:  人类可读的拒绝原因
        details: 附带的诊断数据
    """
    passed: bool
    breach: Optional[RiskBreach] = None
    reason: str = ""
    details: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def ok() -> "RiskCheckResult":
        return RiskCheckResult(passed=True)

    @staticmethod
    def reject(breach: RiskBreach, reason: str, **details) -> "RiskCheckResult":
        return RiskCheckResult(passed=False, breach=breach, reason=reason, details=details)


# =============================================================================
# 2. 持仓限制 (Position Limiter)
# =============================================================================

@dataclass
class PositionLimitConfig:
    """
    持仓限制配置

    Attributes:
        max_delta_per_symbol: 单品种最大 |Delta|（合约张数 / 币数）
        max_delta_global:     全局最大 |Delta|（所有品种的 USD 等值之和）
        warn_threshold_pct:   预警阈值（占上限的百分比，0.8 = 80%）
        auto_hedge:           超限时是否自动触发 Hedger
    """
    max_delta_per_symbol: float = 1.0
    max_delta_global: float = 10.0
    warn_threshold_pct: float = 0.8
    auto_hedge: bool = True


class PositionLimiter:
    """
    持仓 Delta 限制器

    追踪各交易对的净头寸（正 = 多头，负 = 空头），
    当 |Delta| 超过设定阈值时：
        1. 拒绝会增大 Delta 暴露方向的新挂单
        2. 触发注册的 Hedger 回调

    线程安全。

    用法:
        limiter = PositionLimiter(config=PositionLimitConfig(max_delta_per_symbol=0.5))

        # 更新持仓（成交回调中调用）
        limiter.update_position("BTC/USDC:USDC", +0.01)

        # 下单前检查
        result = limiter.check("BTC/USDC:USDC", "buy", 0.01)
        if not result.passed:
            print(result.reason)

        # 注册 Hedger 回调
        limiter.set_hedger(lambda symbol, excess: hedge(symbol, excess))
    """

    def __init__(
        self,
        config: Optional[PositionLimitConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._config = config or PositionLimitConfig()
        self._logger = logger or logging.getLogger("PositionLimiter")

        # symbol -> 净头寸 (正 = long, 负 = short)
        self._positions: Dict[str, float] = {}
        self._lock = threading.Lock()

        # Hedger 回调: fn(symbol: str, delta_excess: float) -> None
        # delta_excess > 0 表示多头超限，需要卖出对冲
        # delta_excess < 0 表示空头超限，需要买入对冲
        self._hedger_callback: Optional[Callable[[str, float], None]] = None

        # 通用事件回调
        self._callbacks: Dict[str, List[Callable]] = {}

        # 已进入限仓状态的品种（避免重复告警）
        self._breached_symbols: set = set()
        self._global_breached: bool = False

    # ---- 配置 ----

    @property
    def config(self) -> PositionLimitConfig:
        return self._config

    def set_hedger(self, callback: Callable[[str, float], None]) -> None:
        """
        注册 Hedger 回调

        当 Delta 超限时调用: callback(symbol, delta_excess)
            delta_excess > 0 → 需要卖出对冲
            delta_excess < 0 → 需要买入对冲
        """
        self._hedger_callback = callback
        self._logger.info("已注册 Hedger 回调")

    # ---- 持仓追踪 ----

    def update_position(self, symbol: str, delta_change: float) -> None:
        """
        增量更新某品种的 Delta

        Args:
            symbol:       交易对
            delta_change: Delta 变化量 (买入为正, 卖出为负)
        """
        with self._lock:
            old = self._positions.get(symbol, 0.0)
            new = old + delta_change
            self._positions[symbol] = new

        self._logger.debug(f"[{symbol}] Delta: {old:.6f} -> {new:.6f} (Δ{delta_change:+.6f})")
        self._check_limits_and_hedge(symbol)

    def set_position(self, symbol: str, delta: float) -> None:
        """
        直接设置某品种的 Delta（覆盖）

        适用于对账同步场景。
        """
        with self._lock:
            self._positions[symbol] = delta

        self._logger.info(f"[{symbol}] Delta 设置为 {delta:.6f}")
        self._check_limits_and_hedge(symbol)

    def sync_positions(self, positions: Dict[str, float]) -> None:
        """
        批量同步所有持仓

        Args:
            positions: {symbol: delta} 字典
        """
        with self._lock:
            self._positions = dict(positions)
        self._logger.info(f"持仓同步完成: {len(positions)} 个品种")

    def get_position(self, symbol: str) -> float:
        """获取某品种的当前 Delta"""
        with self._lock:
            return self._positions.get(symbol, 0.0)

    def get_all_positions(self) -> Dict[str, float]:
        """获取所有品种的当前 Delta"""
        with self._lock:
            return dict(self._positions)

    @property
    def global_delta(self) -> float:
        """全局 |Delta| 之和"""
        with self._lock:
            return sum(abs(d) for d in self._positions.values())

    # ---- 风控检查 ----

    def check(self, symbol: str, side: str, amount: float) -> RiskCheckResult:
        """
        下单前的持仓限制检查

        逻辑:
            1. 计算如果该单成交后的新 Delta
            2. 如果新 Delta 的 |值| 超过单品种上限 → 拒绝
            3. 如果全局 |Delta| 之和超过全局上限 → 拒绝
            4. 如果该单会减少 Delta 暴露（对冲方向）→ 始终放行

        Args:
            symbol: 交易对
            side:   "buy" 或 "sell"
            amount: 数量

        Returns:
            RiskCheckResult
        """
        with self._lock:
            current_delta = self._positions.get(symbol, 0.0)

        # 计算成交后的 Delta
        signed_amount = amount if side.lower() == "buy" else -amount
        projected_delta = current_delta + signed_amount

        # 规则: 如果新方向在减少 |Delta|，始终放行（允许对冲 / 平仓）
        if abs(projected_delta) < abs(current_delta):
            return RiskCheckResult.ok()

        # 检查单品种限制
        max_symbol = self._config.max_delta_per_symbol
        if abs(projected_delta) > max_symbol:
            return RiskCheckResult.reject(
                RiskBreach.POSITION_LIMIT_SYMBOL,
                f"[{symbol}] Delta 将达到 {projected_delta:+.6f}，"
                f"超过单品种上限 ±{max_symbol}",
                symbol=symbol,
                current_delta=current_delta,
                projected_delta=projected_delta,
                limit=max_symbol,
            )

        # 检查全局限制
        with self._lock:
            global_delta = sum(abs(d) for s, d in self._positions.items() if s != symbol)
        global_delta += abs(projected_delta)

        max_global = self._config.max_delta_global
        if global_delta > max_global:
            return RiskCheckResult.reject(
                RiskBreach.POSITION_LIMIT_GLOBAL,
                f"全局 |Delta| 将达到 {global_delta:.6f}，"
                f"超过全局上限 {max_global}",
                symbol=symbol,
                global_delta=global_delta,
                limit=max_global,
            )

        return RiskCheckResult.ok()

    # ---- 内部: 超限检测与 Hedger 触发 ----

    def _check_limits_and_hedge(self, symbol: str) -> None:
        """检查当前持仓是否超限，如果是则触发 Hedger"""
        with self._lock:
            current_delta = self._positions.get(symbol, 0.0)

        max_symbol = self._config.max_delta_per_symbol
        warn_threshold = max_symbol * self._config.warn_threshold_pct

        # ---- 单品种预警 ----
        abs_delta = abs(current_delta)
        if abs_delta >= warn_threshold and symbol not in self._breached_symbols:
            self._logger.warning(
                f"[{symbol}] Delta 预警: {current_delta:+.6f} "
                f"(阈值 ±{warn_threshold:.6f}, 上限 ±{max_symbol})"
            )

        # ---- 单品种超限 ----
        if abs_delta > max_symbol:
            if symbol not in self._breached_symbols:
                self._breached_symbols.add(symbol)
                self._logger.error(
                    f"[{symbol}] Delta 超限! {current_delta:+.6f} > ±{max_symbol}"
                )
                self._emit("position_breach", {
                    "breach": RiskBreach.POSITION_LIMIT_SYMBOL,
                    "symbol": symbol,
                    "delta": current_delta,
                    "limit": max_symbol,
                })

            # 触发 Hedger
            if self._config.auto_hedge and self._hedger_callback:
                # delta_excess: 超出阈值的部分，带符号
                # 正 → 多头超限，需要卖出
                # 负 → 空头超限，需要买入
                if current_delta > 0:
                    excess = current_delta - max_symbol
                else:
                    excess = current_delta + max_symbol
                self._trigger_hedger(symbol, excess)
        else:
            # 回到安全区间，清除标记
            if symbol in self._breached_symbols:
                self._breached_symbols.discard(symbol)
                self._logger.info(f"[{symbol}] Delta 回归安全区间: {current_delta:+.6f}")

        # ---- 全局超限 ----
        global_delta = self.global_delta
        max_global = self._config.max_delta_global
        if global_delta > max_global:
            if not self._global_breached:
                self._global_breached = True
                self._logger.error(f"全局 |Delta| 超限! {global_delta:.6f} > {max_global}")
                self._emit("position_breach", {
                    "breach": RiskBreach.POSITION_LIMIT_GLOBAL,
                    "global_delta": global_delta,
                    "limit": max_global,
                })
        else:
            if self._global_breached:
                self._global_breached = False
                self._logger.info(f"全局 |Delta| 回归安全区间: {global_delta:.6f}")

    def _trigger_hedger(self, symbol: str, delta_excess: float) -> None:
        """调用 Hedger 回调"""
        if self._hedger_callback is None:
            self._logger.warning(f"[{symbol}] 需要对冲 {delta_excess:+.6f}，但未注册 Hedger")
            return

        self._logger.info(f"[{symbol}] 触发 Hedger: excess={delta_excess:+.6f}")
        try:
            self._hedger_callback(symbol, delta_excess)
        except Exception as e:
            self._logger.error(f"Hedger 回调异常: {e}")

    # ---- 事件 ----

    def on(self, event_name: str, callback: Callable) -> None:
        if event_name not in self._callbacks:
            self._callbacks[event_name] = []
        self._callbacks[event_name].append(callback)

    def _emit(self, event_name: str, data: Any = None) -> None:
        for cb in self._callbacks.get(event_name, []):
            try:
                cb(data)
            except Exception as e:
                self._logger.error(f"回调 '{event_name}' 异常: {e}")


# =============================================================================
# 3. 价格保护 (Fat Finger Guard)
# =============================================================================

@dataclass
class FatFingerConfig:
    """
    价格保护配置

    Attributes:
        max_deviation_pct:   最大偏离百分比 (0.05 = 5%)
        max_deviation_abs:   最大偏离绝对值 (可选, None = 不启用)
        check_market_orders: 是否检查市价单 (市价单无法检查价格，通常 False)
    """
    max_deviation_pct: float = 0.05        # 5%
    max_deviation_abs: Optional[float] = None
    check_market_orders: bool = False


class FatFingerGuard:
    """
    价格保护（防胖手指）

    在提交订单之前，检查挂单价是否偏离当前中价（Mid-price）过远。
    如果偏离超过设定阈值，拒绝该订单。

    检查逻辑:
        deviation = |order_price - mid_price| / mid_price

        拒绝条件 (满足任一):
            1. deviation > max_deviation_pct
            2. |order_price - mid_price| > max_deviation_abs (如果设置了)

    用法:
        guard = FatFingerGuard(config=FatFingerConfig(max_deviation_pct=0.03))

        result = guard.check("BTC/USDC:USDC", "buy", price=99999, mid_price=65000)
        if not result.passed:
            print(result.reason)  # "价格偏离 53.8%，超过上限 3.0%"
    """

    def __init__(
        self,
        config: Optional[FatFingerConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._config = config or FatFingerConfig()
        self._logger = logger or logging.getLogger("FatFingerGuard")

        # 可选: 内部缓存最近的 mid-price（由外部更新）
        self._mid_prices: Dict[str, float] = {}
        self._lock = threading.Lock()

    @property
    def config(self) -> FatFingerConfig:
        return self._config

    # ---- Mid-price 管理 ----

    def update_mid_price(self, symbol: str, mid_price: float) -> None:
        """更新某品种的中间价（由行情模块调用）"""
        with self._lock:
            self._mid_prices[symbol] = mid_price

    def get_mid_price(self, symbol: str) -> Optional[float]:
        """获取缓存的中间价"""
        with self._lock:
            return self._mid_prices.get(symbol)

    # ---- 风控检查 ----

    def check(
        self,
        symbol: str,
        side: str,
        price: Optional[float],
        mid_price: Optional[float] = None,
        order_type: str = "limit",
    ) -> RiskCheckResult:
        """
        检查订单价格是否合理

        Args:
            symbol:     交易对
            side:       "buy" 或 "sell"
            price:      挂单价 (市价单为 None)
            mid_price:  当前中间价 (None 则使用内部缓存)
            order_type: 订单类型 ("limit" / "market")

        Returns:
            RiskCheckResult
        """
        # 市价单通常不检查价格
        if order_type.lower() == "market":
            if self._config.check_market_orders:
                self._logger.warning(f"[{symbol}] 市价单无法做价格保护检查")
            return RiskCheckResult.ok()

        # 限价单必须有价格
        if price is None:
            return RiskCheckResult.ok()

        # 获取 mid-price
        if mid_price is None:
            mid_price = self.get_mid_price(symbol)
        if mid_price is None or mid_price <= 0:
            self._logger.warning(f"[{symbol}] 无法获取 mid-price，跳过 Fat Finger 检查")
            return RiskCheckResult.ok()

        # 计算偏离
        deviation_abs = abs(price - mid_price)
        deviation_pct = deviation_abs / mid_price

        # 检查百分比偏离
        max_pct = self._config.max_deviation_pct
        if deviation_pct > max_pct:
            reason = (
                f"[{symbol}] {side.upper()} @ {price} 偏离中价 {mid_price} "
                f"达 {deviation_pct:.1%}，超过上限 {max_pct:.1%}"
            )
            self._logger.error(reason)
            return RiskCheckResult.reject(
                RiskBreach.FAT_FINGER_PCT,
                reason,
                symbol=symbol,
                side=side,
                price=price,
                mid_price=mid_price,
                deviation_pct=round(deviation_pct, 6),
                limit_pct=max_pct,
            )

        # 检查绝对值偏离
        max_abs = self._config.max_deviation_abs
        if max_abs is not None and deviation_abs > max_abs:
            reason = (
                f"[{symbol}] {side.upper()} @ {price} 偏离中价 {mid_price} "
                f"达 {deviation_abs:.2f}，超过绝对值上限 {max_abs:.2f}"
            )
            self._logger.error(reason)
            return RiskCheckResult.reject(
                RiskBreach.FAT_FINGER_ABS,
                reason,
                symbol=symbol,
                side=side,
                price=price,
                mid_price=mid_price,
                deviation_abs=round(deviation_abs, 4),
                limit_abs=max_abs,
            )

        return RiskCheckResult.ok()


# =============================================================================
# 4. 一键关停 (Kill Switch)
# =============================================================================

@dataclass
class KillSwitchConfig:
    """
    一键关停配置

    Attributes:
        flatten_positions:     是否在关停时平掉风险头寸
        cancel_timeout:        撤单超时（秒）
        flatten_timeout:       平仓超时（秒）
        catch_signals:         要捕捉的系统信号列表
        catch_exceptions:      是否注册 sys.excepthook 捕捉未处理异常
    """
    flatten_positions: bool = True
    cancel_timeout: float = 10.0
    flatten_timeout: float = 30.0
    catch_signals: List[int] = field(default_factory=lambda: [signal.SIGINT, signal.SIGTERM])
    catch_exceptions: bool = True


class KillSwitch:
    """
    一键关停（Emergency Kill Switch）

    捕捉 Ctrl+C (SIGINT)、SIGTERM 或未处理异常，
    立即执行紧急关停流程：

        1. 撤销所有挂单（通过 OrderManager + Gateway 双重保障）
        2. 以市价单平掉全部风险头寸（可配置关闭）
        3. 停止 OrderManager、ConnectionMonitor 等组件
        4. 通知上层策略

    幂等设计: 无论触发多少次，只执行一次关停流程。
    线程安全。

    用法:
        ks = KillSwitch(gateway=gw, order_manager=om)
        ks.arm()      # 注册信号处理器

        # ... 运行策略 ...

        # 手动触发 (或者等 Ctrl+C):
        ks.trigger("策略异常退出")

        ks.disarm()   # 卸载信号处理器
    """

    def __init__(
        self,
        gateway=None,
        order_manager=None,
        connection_monitor=None,
        position_limiter: Optional[PositionLimiter] = None,
        config: Optional[KillSwitchConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            gateway:            BaseGateway 实例
            order_manager:      OrderManager 实例
            connection_monitor: ConnectionMonitor 实例 (可选)
            position_limiter:   PositionLimiter 实例 (可选, 用于获取持仓做平仓)
            config:             配置
            logger:             日志记录器
        """
        self._gateway = gateway
        self._order_manager = order_manager
        self._connection_monitor = connection_monitor
        self._position_limiter = position_limiter
        self._config = config or KillSwitchConfig()
        self._logger = logger or logging.getLogger("KillSwitch")

        self._triggered = False
        self._trigger_lock = threading.Lock()
        self._armed = False

        # 保存原始信号处理器（用于 disarm 恢复）
        self._original_handlers: Dict[int, Any] = {}
        self._original_excepthook: Optional[Any] = None

        # 回调
        self._callbacks: Dict[str, List[Callable]] = {}

    @property
    def is_triggered(self) -> bool:
        return self._triggered

    @property
    def is_armed(self) -> bool:
        return self._armed

    # ---- 信号注册 ----

    def arm(self) -> None:
        """
        注册信号处理器

        注意: 信号处理器只能在主线程中注册。
        """
        if self._armed:
            self._logger.warning("KillSwitch 已处于 armed 状态")
            return

        # 注册系统信号 (仅主线程可设置)
        try:
            for sig in self._config.catch_signals:
                try:
                    self._original_handlers[sig] = signal.getsignal(sig)
                    signal.signal(sig, self._signal_handler)
                except (OSError, ValueError) as e:
                    self._logger.warning(f"无法注册信号 {sig}: {e}")
        except Exception as e:
            self._logger.warning(f"信号注册失败 (可能不在主线程): {e}")

        # 注册未处理异常钩子
        if self._config.catch_exceptions:
            self._original_excepthook = sys.excepthook
            sys.excepthook = self._exception_handler

        self._armed = True
        sigs = ", ".join(
            signal.Signals(s).name if hasattr(signal, 'Signals') else str(s)
            for s in self._config.catch_signals
        )
        self._logger.info(f"KillSwitch 已 armed (signals=[{sigs}])")

    def disarm(self) -> None:
        """卸载信号处理器，恢复原始行为"""
        if not self._armed:
            return

        # 恢复信号处理器
        for sig, handler in self._original_handlers.items():
            try:
                signal.signal(sig, handler)
            except (OSError, ValueError):
                pass
        self._original_handlers.clear()

        # 恢复异常钩子
        if self._original_excepthook is not None:
            sys.excepthook = self._original_excepthook
            self._original_excepthook = None

        self._armed = False
        self._logger.info("KillSwitch 已 disarmed")

    # ---- 触发关停 ----

    def trigger(self, reason: str = "手动触发") -> None:
        """
        触发一键关停

        幂等: 多次调用只执行一次。

        Args:
            reason: 触发原因（写入日志）
        """
        with self._trigger_lock:
            if self._triggered:
                self._logger.warning(f"KillSwitch 已触发过，忽略重复触发: {reason}")
                return
            self._triggered = True

        self._logger.critical(f"!!! KILL SWITCH 触发: {reason} !!!")
        self._emit("on_kill", reason)

        # ---- Step 1: 撤销所有挂单 ----
        self._cancel_all_orders()

        # ---- Step 2: 平掉风险头寸 ----
        if self._config.flatten_positions:
            self._flatten_positions()

        # ---- Step 3: 停止各组件 ----
        self._stop_components()

        self._logger.critical("!!! KILL SWITCH 流程完成 !!!")
        self._emit("on_kill_complete", reason)

    # ---- 内部: 撤单 ----

    def _cancel_all_orders(self) -> None:
        """撤销所有挂单（双重保障）"""
        self._logger.warning("KillSwitch: 撤销所有挂单...")

        # 方法1: 通过 OrderManager（维护本地状态）
        if self._order_manager is not None:
            try:
                cancelled = self._order_manager.cancel_all()
                self._logger.warning(f"KillSwitch: OrderManager 撤掉 {cancelled} 个挂单")
            except Exception as e:
                self._logger.error(f"KillSwitch: OrderManager 撤单失败: {e}")

        # 方法2: 兜底 — 直接通过 Gateway
        if self._gateway is not None:
            try:
                gw_cancelled = self._gateway.cancel_all_orders()
                self._logger.warning(f"KillSwitch: Gateway 兜底撤掉 {gw_cancelled} 个挂单")
            except Exception as e:
                self._logger.error(f"KillSwitch: Gateway 兜底撤单失败: {e}")

    # ---- 内部: 平仓 ----

    def _flatten_positions(self) -> None:
        """
        以激进限价单平掉所有风险头寸

        使用激进限价单而非市价单，因为部分交易所（如 Hyperliquid）
        市价单需要额外参数。激进限价单以 ±slippage 的价格提交，
        效果等同于市价单。
        """
        if self._position_limiter is None:
            self._logger.warning("KillSwitch: 未绑定 PositionLimiter，跳过平仓")
            return

        if self._gateway is None:
            self._logger.error("KillSwitch: 未绑定 Gateway，无法平仓")
            return

        positions = self._position_limiter.get_all_positions()
        if not positions:
            self._logger.info("KillSwitch: 无持仓需要平掉")
            return

        self._logger.warning(f"KillSwitch: 平仓 {len(positions)} 个品种...")

        # 延迟导入避免循环引用
        from gateways.gateway import OrderSide, OrderType

        SLIPPAGE = 0.05  # 5% 滑点容忍度

        for symbol, delta in positions.items():
            if abs(delta) < 1e-10:
                continue

            # 多头 -> 卖出平仓，空头 -> 买入平仓
            side = OrderSide.SELL if delta > 0 else OrderSide.BUY
            amount = abs(delta)

            try:
                # 获取当前价格，以激进价格下限价单
                ticker = self._gateway.fetch_ticker(symbol)
                mid_price = ticker.last or ticker.bid or ticker.ask
                if mid_price is None or mid_price <= 0:
                    self._logger.error(f"KillSwitch: {symbol} 无法获取价格，跳过平仓")
                    continue

                if side == OrderSide.SELL:
                    # 卖出: 以低于中价 slippage 的价格挂单 (确保成交)
                    aggressive_price = mid_price * (1 - SLIPPAGE)
                else:
                    # 买入: 以高于中价 slippage 的价格挂单
                    aggressive_price = mid_price * (1 + SLIPPAGE)

                # 精度处理
                aggressive_price = float(self._gateway.price_to_precision(symbol, aggressive_price))
                amount = float(self._gateway.amount_to_precision(symbol, amount))

                self._logger.warning(
                    f"KillSwitch: 平仓 {symbol} "
                    f"{side.value.upper()} {amount} @ {aggressive_price} "
                    f"(当前 Delta={delta:+.6f}, mid={mid_price})"
                )
                self._gateway.create_order(
                    symbol=symbol,
                    side=side,
                    order_type=OrderType.LIMIT,
                    amount=amount,
                    price=aggressive_price,
                )
                self._logger.info(f"KillSwitch: {symbol} 平仓单已发送")
            except Exception as e:
                self._logger.error(f"KillSwitch: {symbol} 平仓失败: {e}")

    # ---- 内部: 停止组件 ----

    def _stop_components(self) -> None:
        """停止各关联组件"""
        if self._connection_monitor is not None:
            try:
                self._connection_monitor.stop()
                self._logger.info("KillSwitch: ConnectionMonitor 已停止")
            except Exception as e:
                self._logger.error(f"KillSwitch: 停止 ConnectionMonitor 失败: {e}")

        if self._order_manager is not None:
            try:
                self._order_manager.stop()
                self._logger.info("KillSwitch: OrderManager 已停止")
            except Exception as e:
                self._logger.error(f"KillSwitch: 停止 OrderManager 失败: {e}")

    # ---- 信号与异常处理器 ----

    def _signal_handler(self, signum, frame) -> None:
        """系统信号处理器"""
        sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        self.trigger(f"收到系统信号 {sig_name}")

    def _exception_handler(self, exc_type, exc_value, exc_tb) -> None:
        """未处理异常钩子"""
        self._logger.critical(
            f"未处理异常: {exc_type.__name__}: {exc_value}",
            exc_info=(exc_type, exc_value, exc_tb),
        )
        self.trigger(f"未处理异常: {exc_type.__name__}: {exc_value}")

        # 调用原始钩子（保留默认 traceback 输出）
        if self._original_excepthook:
            self._original_excepthook(exc_type, exc_value, exc_tb)

    # ---- 事件回调 ----

    def on(self, event_name: str, callback: Callable) -> None:
        """
        注册事件回调

        事件:
            - on_kill(reason):          关停触发
            - on_kill_complete(reason):  关停流程完成
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


# =============================================================================
# 5. 统一风控管理器 (Risk Manager)
# =============================================================================

@dataclass
class RiskConfig:
    """
    统一风控配置

    组合持仓限制、价格保护、一键关停的配置。
    """
    position_limit: PositionLimitConfig = field(default_factory=PositionLimitConfig)
    fat_finger: FatFingerConfig = field(default_factory=FatFingerConfig)
    kill_switch: KillSwitchConfig = field(default_factory=KillSwitchConfig)


class RiskManager:
    """
    统一风控管理器

    组合 PositionLimiter + FatFingerGuard + KillSwitch，
    提供单一入口进行下单前风控检查和紧急关停。

    用法:
        rm = RiskManager(
            gateway=gw,
            order_manager=om,
            config=RiskConfig(
                position_limit=PositionLimitConfig(max_delta_per_symbol=0.5),
                fat_finger=FatFingerConfig(max_deviation_pct=0.03),
            ),
        )
        rm.arm()

        # 下单前
        result = rm.pre_trade_check("BTC/USDC:USDC", "buy", 0.01, price=65000.0)
        if result.passed:
            om.submit_order(...)

        # 更新持仓
        rm.update_position("BTC/USDC:USDC", +0.01)

        # 更新中价
        rm.update_mid_price("BTC/USDC:USDC", 65000.0)

        rm.disarm()
    """

    def __init__(
        self,
        gateway=None,
        order_manager=None,
        connection_monitor=None,
        config: Optional[RiskConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._config = config or RiskConfig()
        self._logger = logger or logging.getLogger("RiskManager")

        # 子模块
        self._position_limiter = PositionLimiter(
            config=self._config.position_limit,
            logger=self._logger.getChild("Position"),
        )
        self._fat_finger_guard = FatFingerGuard(
            config=self._config.fat_finger,
            logger=self._logger.getChild("FatFinger"),
        )
        self._kill_switch = KillSwitch(
            gateway=gateway,
            order_manager=order_manager,
            connection_monitor=connection_monitor,
            position_limiter=self._position_limiter,
            config=self._config.kill_switch,
            logger=self._logger.getChild("KillSwitch"),
        )

        # 风控开关（全局启停）
        self._enabled = True

        # 回调
        self._callbacks: Dict[str, List[Callable]] = {}

    # ---- 属性 ----

    @property
    def position_limiter(self) -> PositionLimiter:
        return self._position_limiter

    @property
    def fat_finger_guard(self) -> FatFingerGuard:
        return self._fat_finger_guard

    @property
    def kill_switch(self) -> KillSwitch:
        return self._kill_switch

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ---- 生命周期 ----

    def arm(self) -> None:
        """注册信号处理、启动风控"""
        self._kill_switch.arm()
        self._enabled = True
        self._logger.info("RiskManager armed")

    def disarm(self) -> None:
        """卸载信号处理"""
        self._kill_switch.disarm()
        self._logger.info("RiskManager disarmed")

    def enable(self) -> None:
        """启用风控检查"""
        self._enabled = True
        self._logger.info("风控检查已启用")

    def disable(self) -> None:
        """临时禁用风控检查（紧急情况下使用）"""
        self._enabled = False
        self._logger.warning("!!! 风控检查已禁用 !!!")

    # ---- 核心: 下单前风控检查 ----

    def pre_trade_check(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        mid_price: Optional[float] = None,
        order_type: str = "limit",
    ) -> RiskCheckResult:
        """
        下单前的统一风控检查

        检查顺序:
            1. KillSwitch 是否已触发
            2. 持仓 Delta 限制
            3. 价格保护（Fat Finger）

        全部通过才返回 passed=True。

        Args:
            symbol:     交易对
            side:       "buy" / "sell"
            amount:     数量
            price:      挂单价 (限价单)
            mid_price:  当前中间价 (None 则使用 FatFingerGuard 内部缓存)
            order_type: "limit" / "market"

        Returns:
            RiskCheckResult
        """
        # 风控关闭时直接放行
        if not self._enabled:
            return RiskCheckResult.ok()

        # 检查1: KillSwitch 已触发 → 一切拒绝
        if self._kill_switch.is_triggered:
            return RiskCheckResult.reject(
                RiskBreach.KILL_SWITCH,
                "KillSwitch 已触发，拒绝所有新订单",
            )

        # 检查2: 持仓限制
        pos_result = self._position_limiter.check(symbol, side, amount)
        if not pos_result.passed:
            self._emit("risk_breach", pos_result)
            return pos_result

        # 检查3: 价格保护
        ff_result = self._fat_finger_guard.check(
            symbol=symbol,
            side=side,
            price=price,
            mid_price=mid_price,
            order_type=order_type,
        )
        if not ff_result.passed:
            self._emit("risk_breach", ff_result)
            return ff_result

        return RiskCheckResult.ok()

    # ---- 代理方法（便利封装） ----

    def update_position(self, symbol: str, delta_change: float) -> None:
        """增量更新持仓 Delta"""
        self._position_limiter.update_position(symbol, delta_change)

    def set_position(self, symbol: str, delta: float) -> None:
        """直接设置持仓 Delta"""
        self._position_limiter.set_position(symbol, delta)

    def sync_positions(self, positions: Dict[str, float]) -> None:
        """批量同步持仓"""
        self._position_limiter.sync_positions(positions)

    def update_mid_price(self, symbol: str, mid_price: float) -> None:
        """更新中间价"""
        self._fat_finger_guard.update_mid_price(symbol, mid_price)

    def set_hedger(self, callback: Callable[[str, float], None]) -> None:
        """注册 Hedger 回调"""
        self._position_limiter.set_hedger(callback)

    def emergency_stop(self, reason: str = "手动紧急关停") -> None:
        """手动触发一键关停"""
        self._kill_switch.trigger(reason)

    # ---- 统计 ----

    def get_stats(self) -> Dict[str, Any]:
        """获取风控运行统计"""
        return {
            "enabled": self._enabled,
            "kill_switch_triggered": self._kill_switch.is_triggered,
            "kill_switch_armed": self._kill_switch.is_armed,
            "positions": self._position_limiter.get_all_positions(),
            "global_delta": self._position_limiter.global_delta,
            "mid_prices": dict(self._fat_finger_guard._mid_prices),
            "position_limit_config": {
                "max_delta_per_symbol": self._config.position_limit.max_delta_per_symbol,
                "max_delta_global": self._config.position_limit.max_delta_global,
            },
            "fat_finger_config": {
                "max_deviation_pct": self._config.fat_finger.max_deviation_pct,
                "max_deviation_abs": self._config.fat_finger.max_deviation_abs,
            },
        }

    # ---- 事件 ----

    def on(self, event_name: str, callback: Callable) -> None:
        """
        注册事件回调

        事件:
            - risk_breach(RiskCheckResult): 风控拦截
            - on_kill(reason):             关停触发 (代理 KillSwitch)
            - on_kill_complete(reason):    关停完成 (代理 KillSwitch)
        """
        if event_name in ("on_kill", "on_kill_complete"):
            self._kill_switch.on(event_name, callback)
        else:
            if event_name not in self._callbacks:
                self._callbacks[event_name] = []
            self._callbacks[event_name].append(callback)

    def _emit(self, event_name: str, data: Any = None) -> None:
        for cb in self._callbacks.get(event_name, []):
            try:
                cb(data)
            except Exception as e:
                self._logger.error(f"回调 '{event_name}' 异常: {e}")
