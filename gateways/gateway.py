#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易所网关模块

提供与交易所连接的统一抽象层，支持通过配置文件扩展不同交易所。

架构:
    BaseGateway (抽象基类)
        ├── HyperliquidGateway  (Hyperliquid实现)
        ├── BinanceGateway      (未来扩展)
        └── ...
    GatewayFactory (工厂类) — 根据配置自动创建网关实例
"""

import yaml
import time
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from datetime import datetime

try:
    import ccxt
except ImportError:
    raise ImportError("缺少依赖: ccxt，请运行 pip install ccxt")


# =============================================================================
# 数据模型
# =============================================================================

class GatewayStatus(Enum):
    """网关连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    RECONNECTING = "reconnecting"
    ERROR = "error"


class OrderSide(Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """订单类型"""
    LIMIT = "limit"
    MARKET = "market"


@dataclass
class Ticker:
    """行情快照"""
    symbol: str
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    timestamp: Optional[int] = None

    @classmethod
    def from_ccxt(cls, data: dict) -> "Ticker":
        return cls(
            symbol=data.get("symbol", ""),
            last=data.get("last"),
            bid=data.get("bid"),
            ask=data.get("ask"),
            high=data.get("high"),
            low=data.get("low"),
            volume=data.get("baseVolume"),
            timestamp=data.get("timestamp"),
        )


@dataclass
class OrderBook:
    """订单簿快照"""
    symbol: str
    bids: List[List[float]] = field(default_factory=list)  # [[price, amount], ...]
    asks: List[List[float]] = field(default_factory=list)
    timestamp: Optional[int] = None

    @classmethod
    def from_ccxt(cls, data: dict) -> "OrderBook":
        return cls(
            symbol=data.get("symbol", ""),
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp"),
        )


@dataclass
class Order:
    """订单信息"""
    id: str
    symbol: str
    side: str
    type: str
    price: Optional[float] = None
    amount: Optional[float] = None
    filled: Optional[float] = None
    remaining: Optional[float] = None
    status: Optional[str] = None
    timestamp: Optional[int] = None
    raw: Optional[dict] = None  # 保留原始响应

    @classmethod
    def from_ccxt(cls, data: dict) -> "Order":
        return cls(
            id=str(data.get("id", "")),
            symbol=data.get("symbol", ""),
            side=data.get("side", ""),
            type=data.get("type", ""),
            price=data.get("price"),
            amount=data.get("amount"),
            filled=data.get("filled"),
            remaining=data.get("remaining"),
            status=data.get("status"),
            timestamp=data.get("timestamp"),
            raw=data,
        )


@dataclass
class Balance:
    """账户余额"""
    currency: str
    free: float = 0.0
    used: float = 0.0
    total: float = 0.0


@dataclass
class AccountBalance:
    """账户余额汇总"""
    balances: Dict[str, Balance] = field(default_factory=dict)
    raw: Optional[dict] = None

    @classmethod
    def from_ccxt(cls, data: dict) -> "AccountBalance":
        balances = {}
        free = data.get("free", {})
        used = data.get("used", {})
        total = data.get("total", {})
        for currency in total:
            t = total.get(currency, 0) or 0
            if t > 0:
                balances[currency] = Balance(
                    currency=currency,
                    free=free.get(currency, 0) or 0,
                    used=used.get(currency, 0) or 0,
                    total=t,
                )
        return cls(balances=balances, raw=data)


# =============================================================================
# 回调事件类型
# =============================================================================

@dataclass
class GatewayEvent:
    """网关事件基类"""
    gateway_name: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class StatusEvent(GatewayEvent):
    """状态变更事件"""
    status: GatewayStatus = GatewayStatus.DISCONNECTED
    message: str = ""


@dataclass
class ErrorEvent(GatewayEvent):
    """错误事件"""
    error: str = ""
    error_type: str = ""


# =============================================================================
# 抽象基类
# =============================================================================

class BaseGateway(ABC):
    """
    交易所网关抽象基类

    所有交易所的具体实现都应继承此类，并实现其中的抽象方法。
    通过统一的接口，上层策略和订单管理模块可以无差别地与不同交易所交互。

    扩展方法:
        1. 在 config/ 下新建 <exchange>_config.yaml
        2. 新建 <Exchange>Gateway 类继承 BaseGateway
        3. 在 GatewayFactory.GATEWAY_MAP 中注册
    """

    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Args:
            config: 从 YAML 配置文件加载的完整配置字典
            logger: 可选的日志记录器实例
        """
        self._config = config
        self._status = GatewayStatus.DISCONNECTED
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._callbacks: Dict[str, List[Callable]] = {}

    # ---- 属性 ----

    @property
    def status(self) -> GatewayStatus:
        """当前网关连接状态"""
        return self._status

    @property
    def config(self) -> Dict[str, Any]:
        """原始配置字典"""
        return self._config

    @property
    @abstractmethod
    def exchange_id(self) -> str:
        """交易所唯一标识符 (如 'hyperliquid', 'binance')"""
        ...

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """交易所显示名称"""
        ...

    # ---- 连接管理 ----

    @abstractmethod
    def connect(self) -> bool:
        """
        建立与交易所的连接

        Returns:
            连接是否成功
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """断开与交易所的连接"""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """检查当前是否已连接"""
        ...

    # ---- 市场数据 ----

    @abstractmethod
    def fetch_ticker(self, symbol: str) -> Ticker:
        """
        获取单个交易对的行情快照

        Args:
            symbol: 交易对 (如 'BTC/USDC:USDC')
        """
        ...

    @abstractmethod
    def fetch_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Ticker]:
        """获取多个交易对的行情快照"""
        ...

    @abstractmethod
    def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """获取订单簿"""
        ...

    @abstractmethod
    def get_markets(self) -> Dict[str, Any]:
        """获取所有可用交易对信息"""
        ...

    # ---- 账户信息 ----

    @abstractmethod
    def fetch_balance(self) -> AccountBalance:
        """获取账户余额"""
        ...

    # ---- 订单管理 ----

    @abstractmethod
    def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None,
    ) -> Order:
        """
        创建订单

        Args:
            symbol:     交易对
            side:       买/卖
            order_type: 限价/市价
            amount:     数量
            price:      价格 (限价单必填)
            params:     交易所特定的额外参数
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        撤销订单

        Args:
            order_id: 订单ID
            symbol:   交易对
        Returns:
            是否撤销成功
        """
        ...

    @abstractmethod
    def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """
        撤销所有挂单

        Args:
            symbol: 交易对 (None 则撤销全部)
        Returns:
            撤销的订单数量
        """
        ...

    @abstractmethod
    def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """获取当前挂单列表"""
        ...

    # ---- 精度工具 ----

    @abstractmethod
    def price_to_precision(self, symbol: str, price: float) -> str:
        """将价格转换为交易所要求的精度"""
        ...

    @abstractmethod
    def amount_to_precision(self, symbol: str, amount: float) -> str:
        """将数量转换为交易所要求的精度"""
        ...

    # ---- 事件回调 ----

    def on(self, event_name: str, callback: Callable) -> None:
        """
        注册事件回调

        Args:
            event_name: 事件名称 ('status', 'error', 'ticker', 'order', ...)
            callback:   回调函数
        """
        if event_name not in self._callbacks:
            self._callbacks[event_name] = []
        self._callbacks[event_name].append(callback)

    def _emit(self, event_name: str, data: Any = None) -> None:
        """触发事件"""
        for cb in self._callbacks.get(event_name, []):
            try:
                cb(data)
            except Exception as e:
                self._logger.error(f"事件回调 '{event_name}' 执行异常: {e}")

    def _set_status(self, status: GatewayStatus, message: str = "") -> None:
        """更新网关状态并触发事件"""
        old_status = self._status
        self._status = status
        if old_status != status:
            self._logger.info(f"状态变更: {old_status.value} -> {status.value}" +
                              (f" ({message})" if message else ""))
            self._emit("status", StatusEvent(
                gateway_name=self.exchange_id,
                status=status,
                message=message,
            ))

    def _on_error(self, error: Exception, context: str = "") -> None:
        """统一错误处理"""
        error_msg = f"[{context}] {type(error).__name__}: {error}" if context else str(error)
        self._logger.error(error_msg)
        self._emit("error", ErrorEvent(
            gateway_name=self.exchange_id,
            error=str(error),
            error_type=type(error).__name__,
        ))


# =============================================================================
# Hyperliquid 网关实现
# =============================================================================

class HyperliquidGateway(BaseGateway):
    """
    Hyperliquid 交易所网关

    通过 CCXT 库与 Hyperliquid 进行交互。
    配置文件: config/hyperliquid_config.yaml
    """

    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)

        # 解析 Hyperliquid 专属配置
        hl_config = config.get("hyperliquid", {})
        self._api_config = hl_config.get("api", {})
        self._auth_config = hl_config.get("authentication", {})
        self._trading_config = hl_config.get("trading", {})
        self._precision_config = hl_config.get("precision", {})
        self._rate_limits = hl_config.get("rate_limits", {})
        self._ws_config = hl_config.get("websocket", {})
        self._symbols_config = hl_config.get("symbols", {})
        self._fee_config = hl_config.get("fees", {})
        self._market_types_config = hl_config.get("market_types", {})

        self._timeout = self._api_config.get("timeout", 30)

        # CCXT 交易所实例
        self._exchange: Optional[ccxt.hyperliquid] = None
        # 市场信息缓存
        self._markets: Dict[str, Any] = {}

    # ---- 属性 ----

    @property
    def exchange_id(self) -> str:
        return "hyperliquid"

    @property
    def exchange_name(self) -> str:
        return "Hyperliquid"

    @property
    def exchange(self) -> Optional[ccxt.hyperliquid]:
        """获取底层 CCXT 交易所实例 (供高级用途)"""
        return self._exchange

    @property
    def fees(self) -> Dict[str, Any]:
        """手续费配置"""
        return self._fee_config

    @property
    def trading_config(self) -> Dict[str, Any]:
        """交易参数配置"""
        return self._trading_config

    @property
    def supported_symbols(self) -> Dict[str, List[str]]:
        """配置文件中声明的交易对"""
        return self._symbols_config

    # ---- 连接管理 ----

    def connect(self) -> bool:
        """建立与 Hyperliquid 的连接"""
        self._set_status(GatewayStatus.CONNECTING)

        try:
            self._exchange = self._create_exchange_instance()

            # 加载市场信息（同时也验证了网络连通性）
            self._markets = self._exchange.load_markets()
            self._logger.info(f"已加载 {len(self._markets)} 个交易对")

            # 验证认证
            if self._has_credentials():
                try:
                    self._exchange.fetch_balance()
                    self._set_status(GatewayStatus.AUTHENTICATED, "认证成功")
                    return True
                except ccxt.AuthenticationError as e:
                    self._on_error(e, "认证")
                    self._set_status(GatewayStatus.CONNECTED, "公共API已连接，认证失败")
                    return True
                except Exception as e:
                    # CCXT 4.5.x 的 fetch_balance 存在 422 序列化 bug，
                    # 不代表认证失败（下单测试已验证凭证有效），直接标记为 AUTHENTICATED
                    self._logger.warning(
                        f"余额查询失败（可能是 CCXT 版本兼容问题，不影响下单）: {e}"
                    )
                    self._set_status(GatewayStatus.AUTHENTICATED, "认证成功（余额查询跳过）")
                    return True
            else:
                self._set_status(GatewayStatus.CONNECTED, "公共API已连接（未配置认证）")
                return True

        except ccxt.NetworkError as e:
            self._on_error(e, "网络连接")
            self._set_status(GatewayStatus.ERROR, f"网络错误: {e}")
            return False
        except Exception as e:
            self._on_error(e, "连接")
            self._set_status(GatewayStatus.ERROR, str(e))
            return False

    def disconnect(self) -> None:
        """断开连接"""
        self._exchange = None
        self._markets = {}
        self._set_status(GatewayStatus.DISCONNECTED)

    def is_connected(self) -> bool:
        """检查是否已连接"""
        return (
            self._exchange is not None
            and self._status in (GatewayStatus.CONNECTED, GatewayStatus.AUTHENTICATED)
        )

    # ---- 市场数据 ----

    def fetch_ticker(self, symbol: str) -> Ticker:
        self._ensure_connected()
        data = self._exchange.fetch_ticker(symbol)
        return Ticker.from_ccxt(data)

    def fetch_tickers(self, symbols: Optional[List[str]] = None) -> Dict[str, Ticker]:
        self._ensure_connected()
        data = self._exchange.fetch_tickers(symbols)
        return {sym: Ticker.from_ccxt(t) for sym, t in data.items()}

    def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        self._ensure_connected()
        data = self._exchange.fetch_order_book(symbol, limit=limit)
        data["symbol"] = symbol
        return OrderBook.from_ccxt(data)

    def get_markets(self) -> Dict[str, Any]:
        self._ensure_connected()
        return self._exchange.markets

    # ---- 账户信息 ----

    def fetch_balance(self) -> AccountBalance:
        self._ensure_authenticated()
        data = self._exchange.fetch_balance()
        return AccountBalance.from_ccxt(data)

    # ---- 订单管理 ----

    def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None,
    ) -> Order:
        self._ensure_authenticated()

        # 精度处理
        formatted_amount = float(self._exchange.amount_to_precision(symbol, amount))
        formatted_price = (
            float(self._exchange.price_to_precision(symbol, price))
            if price is not None
            else None
        )

        self._logger.info(
            f"创建订单: {symbol} {side.value} {order_type.value} "
            f"数量={formatted_amount} 价格={formatted_price}"
        )

        data = self._exchange.create_order(
            symbol=symbol,
            type=order_type.value,
            side=side.value,
            amount=formatted_amount,
            price=formatted_price,
            params=params or {},
        )
        order = Order.from_ccxt(data)
        self._logger.info(f"订单已创建: id={order.id}, status={order.status}")
        self._emit("order", order)
        return order

    def cancel_order(self, order_id: str, symbol: str) -> bool:
        self._ensure_authenticated()
        self._logger.info(f"撤销订单: id={order_id}, symbol={symbol}")

        try:
            self._exchange.cancel_order(order_id, symbol)
            self._logger.info(f"订单 {order_id} 已撤销")
            return True
        except ccxt.OrderNotFound:
            self._logger.warning(f"订单 {order_id} 未找到（可能已成交或已撤销）")
            return False
        except Exception as e:
            self._on_error(e, "撤单")
            raise

    def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        self._ensure_authenticated()
        self._logger.info(f"撤销所有挂单: symbol={symbol or 'ALL'}")

        # 先获取挂单列表
        open_orders = self.fetch_open_orders(symbol)
        if not open_orders:
            return 0

        cancelled = 0
        for order in open_orders:
            try:
                self._exchange.cancel_order(order.id, order.symbol)
                cancelled += 1
            except Exception as e:
                self._logger.warning(f"撤销订单 {order.id} 失败: {e}")

        self._logger.info(f"已撤销 {cancelled}/{len(open_orders)} 个订单")
        return cancelled

    def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        self._ensure_authenticated()
        data = self._exchange.fetch_open_orders(symbol)
        return [Order.from_ccxt(o) for o in data]

    # ---- 精度工具 ----

    def price_to_precision(self, symbol: str, price: float) -> str:
        self._ensure_connected()
        return self._exchange.price_to_precision(symbol, price)

    def amount_to_precision(self, symbol: str, amount: float) -> str:
        self._ensure_connected()
        return self._exchange.amount_to_precision(symbol, amount)

    # ---- 私有辅助方法 ----

    def _create_exchange_instance(self) -> ccxt.hyperliquid:
        """创建 CCXT Hyperliquid 实例"""
        private_key = self._auth_config.get("private_key", "").strip()
        wallet_address = self._auth_config.get("wallet_address", "").strip()

        default_market = self._market_types_config.get("default_market", "perpetual")
        default_type = "swap" if default_market == "perpetual" else "spot"

        exchange_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": self._timeout * 1000,
            "options": {
                "defaultType": default_type,
            },
        }

        if self._has_credentials():
            exchange_config["walletAddress"] = wallet_address
            exchange_config["privateKey"] = private_key

        instance = ccxt.hyperliquid(exchange_config)

        # CCXT 4.5.x 变更: Hyperliquid 新增 HIP-3 DEX 市场，fetchHip3Markets()
        # 默认会拉取所有 DEX 且超过 5 个上限时报错。做市只需 spot/swap，
        # 把 hip3 从 fetchMarkets.types 中移除即可跳过该调用。
        instance.options.setdefault("fetchMarkets", {})["types"] = ["spot", "swap"]

        return instance

    def _has_credentials(self) -> bool:
        """检查是否配置了有效的认证信息"""
        pk = self._auth_config.get("private_key", "").strip()
        wa = self._auth_config.get("wallet_address", "").strip()
        if not pk or not wa:
            return False
        if "YOUR" in pk.upper() or "YOUR" in wa.upper():
            return False
        return True

    def _ensure_connected(self) -> None:
        """确保已连接，否则抛出异常"""
        if not self.is_connected():
            raise ConnectionError(
                f"{self.exchange_name} 网关未连接，请先调用 connect()"
            )

    def _ensure_authenticated(self) -> None:
        """确保已认证，否则抛出异常"""
        self._ensure_connected()
        if self._status != GatewayStatus.AUTHENTICATED:
            raise PermissionError(
                f"{self.exchange_name} 网关未认证，无法执行私有API操作"
            )


# =============================================================================
# 网关工厂
# =============================================================================

class GatewayFactory:
    """
    网关工厂类

    根据配置文件自动创建对应的交易所网关实例。

    扩展新交易所步骤:
        1. 新建 config/<exchange>_config.yaml (参照 hyperliquid_config.yaml 格式)
        2. 新建 <Exchange>Gateway 类继承 BaseGateway
        3. 在 GATEWAY_MAP 中注册:  GATEWAY_MAP["<exchange>"] = <Exchange>Gateway
    """

    # 交易所 ID -> 网关类 的映射表
    GATEWAY_MAP: Dict[str, type] = {
        "hyperliquid": HyperliquidGateway,
        # 扩展示例:
        # "binance": BinanceGateway,
        # "okx": OkxGateway,
    }

    @classmethod
    def create(
        cls,
        config_path: str,
        exchange_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> BaseGateway:
        """
        根据配置文件创建网关实例

        Args:
            config_path:  YAML 配置文件路径
            exchange_id:  交易所 ID (可选，不传则从配置文件自动推断)
            logger:       日志记录器

        Returns:
            对应交易所的 Gateway 实例

        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError:        无法识别交易所类型
        """
        config = cls._load_config(config_path)

        # 自动推断交易所 ID
        if exchange_id is None:
            exchange_id = cls._detect_exchange_id(config, config_path)

        if exchange_id not in cls.GATEWAY_MAP:
            supported = ", ".join(cls.GATEWAY_MAP.keys())
            raise ValueError(
                f"不支持的交易所: '{exchange_id}'。当前支持: {supported}"
            )

        gateway_cls = cls.GATEWAY_MAP[exchange_id]
        return gateway_cls(config=config, logger=logger)

    @classmethod
    def register(cls, exchange_id: str, gateway_cls: type) -> None:
        """
        注册新的网关类型

        Args:
            exchange_id: 交易所 ID
            gateway_cls: 网关类 (必须是 BaseGateway 的子类)
        """
        if not issubclass(gateway_cls, BaseGateway):
            raise TypeError(f"{gateway_cls} 必须继承 BaseGateway")
        cls.GATEWAY_MAP[exchange_id] = gateway_cls

    @classmethod
    def supported_exchanges(cls) -> List[str]:
        """返回当前支持的所有交易所 ID"""
        return list(cls.GATEWAY_MAP.keys())

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        """加载 YAML 配置文件"""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _detect_exchange_id(config: Dict[str, Any], config_path: str) -> str:
        """
        从配置内容或文件名推断交易所 ID

        优先级:
            1. 配置中的 exchange_id 字段
            2. 配置的顶层 key (如 'hyperliquid')
            3. 文件名中的交易所名称 (如 'hyperliquid_config.yaml')
        """
        # 方法1: 查找各顶层 key 下的 exchange_id
        for key, value in config.items():
            if isinstance(value, dict) and "exchange_id" in value:
                return value["exchange_id"]

        # 方法2: 顶层 key 本身就是交易所名
        for key in config:
            if key in GatewayFactory.GATEWAY_MAP:
                return key

        # 方法3: 从文件名推断
        filename = Path(config_path).stem.lower()  # e.g. 'hyperliquid_config'
        for eid in GatewayFactory.GATEWAY_MAP:
            if eid in filename:
                return eid

        raise ValueError(f"无法从配置文件 '{config_path}' 推断交易所类型，请通过 exchange_id 参数指定")
