"""
交易所网关模块

提供与不同交易所连接的统一抽象层（行情、下单、撤单）。
订单生命周期管理已移至 execution.order_manager。

用法:
    from gateways import GatewayFactory, OrderSide, OrderType

    gateway = GatewayFactory.create("config/hyperliquid_config.yaml")
    gateway.connect()
"""

from gateways.gateway import (
    BaseGateway,
    HyperliquidGateway,
    GatewayFactory,
    GatewayStatus,
    OrderSide,
    OrderType,
    Ticker,
    OrderBook,
    Order,
    Balance,
    AccountBalance,
    Position,
    GatewayEvent,
    StatusEvent,
    ErrorEvent,
)

from gateways.exception_handler import (
    ConnectionMonitor,
    ReconnectPolicy,
    ExceptionClassifier,
    ExceptionSeverity,
    MonitorState,
)

__all__ = [
    # gateway
    "BaseGateway",
    "HyperliquidGateway",
    "GatewayFactory",
    "GatewayStatus",
    "OrderSide",
    "OrderType",
    "Ticker",
    "OrderBook",
    "Order",
    "Balance",
    "AccountBalance",
    "Position",
    "GatewayEvent",
    "StatusEvent",
    "ErrorEvent",
    # exception_handler
    "ConnectionMonitor",
    "ReconnectPolicy",
    "ExceptionClassifier",
    "ExceptionSeverity",
    "MonitorState",
]
