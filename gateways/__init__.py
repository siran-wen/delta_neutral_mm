"""
交易所网关模块

提供与不同交易所连接的统一抽象层，以及实盘级订单全生命周期管理。

用法:
    from gateways import GatewayFactory, OrderManager, OrderSide, OrderType

    # 创建网关并连接
    gateway = GatewayFactory.create("config/hyperliquid_config.yaml")
    gateway.connect()

    # 创建订单管理器
    om = OrderManager(gateway)
    om.start()

    # 下单
    order = om.submit_order("BTC/USDC:USDC", OrderSide.BUY, OrderType.LIMIT, 0.001, 35000.0)

    # 改单
    om.amend_order(order.client_order_id, new_price=36000.0)

    # 撤单
    om.cancel_order(order.client_order_id)

    om.stop()
"""

from gateways.gateway import (
    # 抽象基类
    BaseGateway,
    # 具体实现
    HyperliquidGateway,
    # 工厂
    GatewayFactory,
    # 枚举
    GatewayStatus,
    OrderSide,
    OrderType,
    # 数据模型
    Ticker,
    OrderBook,
    Order,
    Balance,
    AccountBalance,
    # 事件
    GatewayEvent,
    StatusEvent,
    ErrorEvent,
)

from gateways.orderManager import (
    OrderManager,
    ManagedOrder,
    OrderState,
    RequestPriority,
    LatencyRecord,
    LatencyTracker,
    RateLimiter,
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
    "GatewayEvent",
    "StatusEvent",
    "ErrorEvent",
    # orderManager
    "OrderManager",
    "ManagedOrder",
    "OrderState",
    "RequestPriority",
    "LatencyRecord",
    "LatencyTracker",
    "RateLimiter",
    # exception_handler
    "ConnectionMonitor",
    "ReconnectPolicy",
    "ExceptionClassifier",
    "ExceptionSeverity",
    "MonitorState",
]
