"""
风控模块

提供实盘交易的风控保护:
    - PositionLimiter  : 持仓 Delta 限制，超阈值停止挂单并触发 Hedger
    - FatFingerGuard   : 价格保护，拒绝偏离中价过远的订单
    - KillSwitch       : 一键关停，撤单 + 平仓
    - RiskManager      : 统一风控管理器，组合以上模块

用法:
    from risk import RiskManager, RiskConfig

    rm = RiskManager(gateway=gw, order_manager=om, config=RiskConfig())
    rm.arm()                   # 注册信号、启动监控

    # 下单前检查
    ok, reason = rm.pre_trade_check("BTC/USDC:USDC", "buy", 0.01, price=65000.0)
    if not ok:
        print(f"风控拒绝: {reason}")

    rm.disarm()
"""

from risk.pre_trade import (
    RiskManager,
    RiskConfig,
    RiskCheckResult,
    PositionLimitConfig,
    FatFingerConfig,
    BalanceGuardConfig,
    KillSwitchConfig,
)

__all__ = [
    "RiskManager",
    "RiskConfig",
    "RiskCheckResult",
    "PositionLimitConfig",
    "FatFingerConfig",
    "BalanceGuardConfig",
    "KillSwitchConfig",
]
