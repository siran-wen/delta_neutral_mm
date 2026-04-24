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
    price: Optional[float] = None           # 限价单的挂单价（市价单可能为 None）
    average: Optional[float] = None         # 加权成交均价（IOC/部分成交时由交易所回填）
    amount: Optional[float] = None
    filled: Optional[float] = None
    remaining: Optional[float] = None
    status: Optional[str] = None
    timestamp: Optional[int] = None
    fee_cost: Optional[float] = None        # 手续费金额（可能是 base coin 数量或 USDC 金额）
    fee_currency: Optional[str] = None      # 手续费币种（"USDC" / "USOL" / "SOL" 等）
    raw: Optional[dict] = None  # 保留原始响应

    @classmethod
    def from_ccxt(cls, data: dict) -> "Order":
        # ---- Hyperliquid 原生 info.response.data.statuses[0] 提取 ----
        # HL 的订单状态通过 statuses[0] 的子字典标明：
        #   - "filled"  sub-dict 存在 → 本单已真实成交（aggressive IOC 语义上
        #                               all-or-none），内含 avgPx / totalSz
        #   - "resting" sub-dict 存在 → 本单挂在订单簿（maker）
        # CCXT 对 HL 同步响应的归一化偶尔失灵（复盘 live_20260424_224447：
        # 6/6 hedge 收到 status=open / filled=0，但 info.statuses[0].filled
        # 里有真实 totalSz 和 avgPx）。用 "filled" sub-dict 的存在作为唯一
        # discriminator 扫一遍 info，同时产出 hl_avg_px_in_info / hl_total_sz_in_info，
        # 后面 avg / filled 字段的归一化都复用这两个值。
        hl_avg_px_in_info: Optional[float] = None
        hl_total_sz_in_info: Optional[float] = None
        try:
            info = data.get("info") or {}
            resp = info.get("response") or {}
            rd = resp.get("data") or {}
            for st in rd.get("statuses") or []:
                filled_info = (st or {}).get("filled") if isinstance(st, dict) else None
                if not filled_info:
                    continue
                try:
                    px = float(filled_info.get("avgPx") or 0)
                    if px > 0:
                        hl_avg_px_in_info = px
                except (TypeError, ValueError):
                    pass
                try:
                    sz = float(filled_info.get("totalSz") or 0)
                    if sz > 0:
                        hl_total_sz_in_info = sz
                except (TypeError, ValueError):
                    pass
                break  # HL 单订单响应只有一个 status，取到即止
        except (TypeError, ValueError, AttributeError):
            pass

        # ---- 加权成交均价三级提取 ----
        #   1. CCXT 标准 'average' 字段（最稳）
        #   2. CCXT 'trades' 数组按 (price, amount) 聚合（部分成交场景）
        #   3. 上面 HL 原生 info.response.data.statuses[0].filled.avgPx
        avg = data.get("average")
        if avg is None and data.get("trades"):
            total_sz = 0.0
            total_notional = 0.0
            for t in data.get("trades") or []:
                try:
                    sz = float(t.get("amount") or 0)
                    px = float(t.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if sz > 0 and px > 0:
                    total_sz += sz
                    total_notional += sz * px
            if total_sz > 0:
                avg = total_notional / total_sz
        if avg is None and hl_avg_px_in_info is not None:
            avg = hl_avg_px_in_info

        # ---- filled / remaining 归一化修复 ----
        # CCXT 对 HL aggressive IOC 的同步响应 bug：status=open、filled=0，
        # 但 info.statuses[0].filled.totalSz 有真实成交量。discriminator 已由
        # 上面的 "filled" sub-dict 检查把 maker 挂单（resting）路径过滤掉，
        # 这里只需保证：
        #   a. 只在 CCXT 自报 filled=0 / None 时覆盖。若 CCXT 已归一化了部分
        #      成交（filled > 0 且 < amount），保持原值不动——避免把真正的
        #      partial-fill 场景压成 full-fill。
        #   b. filled 被覆盖时同步重算 remaining，保持 filled + remaining = amount。
        filled_val = data.get("filled")
        remaining_val = data.get("remaining")
        try:
            raw_filled_num = float(filled_val) if filled_val is not None else 0.0
        except (TypeError, ValueError):
            raw_filled_num = 0.0
        if raw_filled_num == 0.0 and hl_total_sz_in_info is not None:
            filled_val = hl_total_sz_in_info
            try:
                amt = float(data.get("amount") or 0)
                if amt > 0:
                    remaining_val = max(amt - hl_total_sz_in_info, 0.0)
            except (TypeError, ValueError):
                pass

        # ---- 手续费提取 ----
        # Hyperliquid 实际 fee 结构（来自 tests/diagnose_hl_spot_orders.py 诊断）：
        #   现货 BUY  → fee 扣 base coin  (如 {cost: 0.00004761, currency: "USOL"})
        #   现货 SELL → fee 扣 USDC       (如 {cost: 0.005, currency: "USDC"})
        #   永续      → fee 扣 USDC       (如 {cost: 0.01, currency: "USDC"})
        # 另有 info.builderFee 字符串字段，单位 USDC，属于建单方额外加收
        # 的 0.01% 费率，也得算进总 fee。
        #
        # 提取顺序：
        #   1. 主 fee: data["fee"] = {"cost", "currency"}  (CCXT 单 fee 路径)
        #   2. 若 1 不可用，data["fees"] 取 [0]            (CCXT list fallback)
        #   3. 合并 info.builderFee（USDC）。若主 fee 币种非 USDC，按 avg 折算成
        #      base coin 后再加，保持 fee_currency 不变（方案 A）。
        fee_cost: Optional[float] = None
        fee_currency: Optional[str] = None

        def _parse_fee_dict(f: Any) -> None:
            """把一个 fee dict 解析进外层 fee_cost / fee_currency（防御性）"""
            nonlocal fee_cost, fee_currency
            if not isinstance(f, dict):
                return
            try:
                cost = float(f.get("cost") or 0)
            except (TypeError, ValueError):
                cost = 0.0
            if cost <= 0:
                return
            currency = f.get("currency")
            if not isinstance(currency, str) or not currency:
                return
            fee_cost = cost
            fee_currency = currency

        _parse_fee_dict(data.get("fee"))
        if fee_cost is None:
            fees_list = data.get("fees")
            if isinstance(fees_list, list) and fees_list:
                _parse_fee_dict(fees_list[0])

        # builderFee（USDC）—— 无论主 fee 是否存在都要加
        builder_fee_usdc = 0.0
        try:
            info_for_bf = data.get("info") or {}
            bf = info_for_bf.get("builderFee")
            if bf is not None and bf != "":
                builder_fee_usdc = max(float(bf), 0.0)
        except (TypeError, ValueError):
            builder_fee_usdc = 0.0

        if builder_fee_usdc > 0:
            if fee_cost is None:
                # 没有主 fee，但有 builderFee → 单独作为 USDC 记账
                fee_cost = builder_fee_usdc
                fee_currency = "USDC"
            elif fee_currency == "USDC":
                # 主 fee 也是 USDC，直接相加
                fee_cost = fee_cost + builder_fee_usdc
            else:
                # 主 fee 是 base coin（如 USOL），按 avg 折算 builderFee→base
                # fill price avg 缺失时退化到 data["price"]（限价单未成交场景 avg 不稳）
                conv_px: Optional[float] = avg if (avg and avg > 0) else None
                if conv_px is None:
                    try:
                        raw_px = float(data.get("price") or 0)
                        conv_px = raw_px if raw_px > 0 else None
                    except (TypeError, ValueError):
                        conv_px = None
                if conv_px:
                    fee_cost = fee_cost + (builder_fee_usdc / conv_px)
                # 无可用价格 → 放弃 builderFee 折算（极罕见），避免污染 fee_cost

        return cls(
            id=str(data.get("id", "")),
            symbol=data.get("symbol", ""),
            side=data.get("side", ""),
            type=data.get("type", ""),
            price=data.get("price"),
            average=avg,
            amount=data.get("amount"),
            filled=filled_val,
            remaining=remaining_val,
            status=data.get("status"),
            timestamp=data.get("timestamp"),
            fee_cost=fee_cost,
            fee_currency=fee_currency,
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


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    side: str                                    # "long" / "short" / "flat"
    size: float                                  # 持仓数量（绝对值）
    entry_price: float                           # 开仓均价
    unrealized_pnl: float                        # 未实现盈亏
    notional_value: float                        # 名义价值
    leverage: Optional[float] = None             # 杠杆倍数（交易所不一定返回）
    liquidation_price: Optional[float] = None    # 强平价格（交易所不一定返回）
    margin_type: Optional[str] = None            # 保证金模式（交易所不一定返回）
    raw: Optional[dict] = None                   # 保留原始响应

    @classmethod
    def from_ccxt(cls, data: dict) -> "Position":
        """
        从 CCXT fetch_positions 返回的标准结构构造 Position

        CCXT 返回格式参考:
            https://docs.ccxt.com/#/?id=position-structure
        """
        symbol = data.get("symbol", "")

        # ---- size: 取绝对值，防御 None ----
        contracts = data.get("contracts")
        contract_size = data.get("contractSize")
        if contracts is not None and contract_size is not None:
            size = abs(float(contracts) * float(contract_size))
        elif contracts is not None:
            size = abs(float(contracts))
        else:
            size = 0.0

        # ---- side: 优先取 CCXT 标准字段，缺失时根据 contracts 正负推断 ----
        raw_side = data.get("side")
        if size == 0.0:
            side = "flat"
        elif raw_side in ("long", "short"):
            side = raw_side
        elif contracts is not None:
            side = "long" if float(contracts) >= 0 else "short"
        else:
            side = "flat"

        entry_price = float(data.get("entryPrice") or 0.0)
        unrealized_pnl = float(data.get("unrealizedPnl") or 0.0)

        # ---- notional_value: 优先取 CCXT 的 notional，缺失时计算 ----
        notional = data.get("notional")
        if notional is not None:
            notional_value = abs(float(notional))
        else:
            notional_value = size * entry_price

        leverage = None
        raw_leverage = data.get("leverage")
        if raw_leverage is not None:
            leverage = float(raw_leverage)

        liquidation_price = None
        raw_liq = data.get("liquidationPrice")
        if raw_liq is not None:
            liquidation_price = float(raw_liq)

        margin_type = data.get("marginMode") or data.get("marginType")

        return cls(
            symbol=symbol,
            side=side,
            size=size,
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl,
            notional_value=notional_value,
            leverage=leverage,
            liquidation_price=liquidation_price,
            margin_type=margin_type,
            raw=data,
        )


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

    def fetch_spot_balance(self) -> AccountBalance:
        """获取现货账户余额。部分交易所的 fetch_balance 只返回保证金账户，
        此方法用于显式拉取现货子账户余额。默认抛 NotImplementedError。"""
        raise NotImplementedError

    @abstractmethod
    def fetch_positions(self, symbols: Optional[List[str]] = None) -> List[Position]:
        """
        获取当前持仓列表

        Args:
            symbols: 交易对列表 (None 则获取全部持仓)

        Returns:
            持仓列表，无持仓时返回空列表
        """
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
        client_order_id: Optional[str] = None,
    ) -> Order:
        """
        创建订单

        Args:
            symbol:           交易对
            side:             买/卖
            order_type:       限价/市价
            amount:           数量
            price:            价格 (限价单必填)
            params:           交易所特定的额外参数
            client_order_id:  本地订单 ID（仅用于日志追溯，不传给交易所）
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
                    self._on_error(e, "认证-余额查询")
                    self._set_status(GatewayStatus.ERROR, f"余额查询异常: {e}")
                    return False
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

    def fetch_spot_balance(self) -> AccountBalance:
        """
        通过 Hyperliquid 原生 /info POST 的 spotClearinghouseState 拉取现货账户余额。

        CCXT 在 defaultType="swap" 下 fetch_balance 只返回永续账户的 USDC，不包含
        现货 token（USOL/HYPE/USDH 等）。这里绕开 CCXT 直接走 REST /info。

        原生响应格式：
            {"balances": [
                {"coin": "USDC", "token": 0,   "total": "71.30", "hold": "0.0", ...},
                {"coin": "USOL", "token": 254, "total": "0.30",  "hold": "0.0", ...},
            ]}
        语义:
            total = 总持有量（包括被挂单占用）
            hold  = 被挂单占用
            free  = total - hold

        Returns:
            AccountBalance，balances 字典以 coin 名（USDC / USOL / HYPE …）为 key。
        """
        self._ensure_authenticated()

        wallet = self._auth_config.get("wallet_address", "").strip()
        if not wallet:
            raise ValueError("wallet_address 未配置，无法拉取 spot 余额")

        base_url = self._api_config.get("base_url", "https://api.hyperliquid.xyz")
        url = base_url.rstrip("/") + "/info"

        self._logger.debug(f"fetch_spot_balance: wallet={wallet}  url={url}")

        try:
            import requests
            resp = requests.post(
                url,
                json={"type": "spotClearinghouseState", "user": wallet},
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self._on_error(e, "spotClearinghouseState")
            raise

        # 建立 "原生 coin 名 (baseName)" → "CCXT 剥离 U 前缀后的 base" 映射
        # Hyperliquid 把 canonical 资产包装成 USOL / UBTC / UETH 等，CCXT 在加载
        # markets 时保留原名到 baseName、剥离版放 base。下单和 BalanceGuard 都用
        # CCXT 的 base 口径，这里同时把两个 key（USOL 和 SOL）指向同一个 Balance。
        name_to_base: Dict[str, str] = {}
        try:
            for _m in (self._exchange.markets or {}).values():
                if (
                    _m.get("spot")
                    and _m.get("baseName")
                    and _m.get("base")
                    and _m["baseName"] != _m["base"]
                ):
                    name_to_base[_m["baseName"]] = _m["base"]
        except Exception:
            pass

        balances: Dict[str, Balance] = {}
        for item in data.get("balances", []):
            coin = item.get("coin", "")
            if not coin:
                continue
            try:
                total = float(item.get("total", 0) or 0)
                hold = float(item.get("hold", 0) or 0)
            except (TypeError, ValueError):
                continue
            if total <= 0 and hold <= 0:
                continue
            bal_obj = Balance(
                currency=coin,
                free=max(total - hold, 0.0),
                used=hold,
                total=total,
            )
            balances[coin] = bal_obj
            # 别名：同一个 Balance 对象既可用 native name (USOL) 也可用 CCXT base (SOL)
            ccxt_base = name_to_base.get(coin)
            if ccxt_base and ccxt_base not in balances:
                balances[ccxt_base] = bal_obj

        self._logger.info(
            f"获取到现货余额: {len(balances)} 个 key"
            + (
                f" ({', '.join(f'{k}={v.total}' for k, v in balances.items())})"
                if balances else ""
            )
        )
        return AccountBalance(balances=balances, raw=data)

    def fetch_positions(
        self,
        symbols: Optional[List[str]] = None,
        include_empty: bool = False,
    ) -> List[Position]:
        """
        获取当前持仓列表

        Args:
            symbols:       交易对列表 (None 则获取全部持仓)
            include_empty: 是否包含空仓位 (对账场景需要)

        Returns:
            持仓列表，无持仓时返回空列表
        """
        self._ensure_authenticated()
        self._logger.info(f"查询持仓: symbols={symbols or 'ALL'}")

        try:
            data = self._exchange.fetch_positions(symbols)

            positions = []
            for item in data:
                pos = Position.from_ccxt(item)
                if include_empty or pos.size > 0:
                    positions.append(pos)

            self._logger.info(f"获取到 {len(positions)} 个持仓")
            return positions
        except ccxt.AuthenticationError as e:
            self._on_error(e, "查询持仓")
            raise
        except ccxt.NetworkError as e:
            self._on_error(e, "查询持仓")
            raise
        except Exception as e:
            self._on_error(e, "查询持仓")
            raise

    def fetch_my_trades(
        self, symbol: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """拉取最近成交记录，CCXT 标准格式 list[dict]，含 'order' (eid) / 'amount' / 'price' 等"""
        self._ensure_authenticated()
        try:
            trades = self._exchange.fetch_my_trades(symbol, limit=limit)
            return trades or []
        except Exception as e:
            self._on_error(e, f"fetch_my_trades({symbol})")
            raise

    def fetch_user_fills(self, start_time_ms: Optional[int] = None) -> List[Dict]:
        """
        通过 Hyperliquid 原生 /info userFills 拉取用户成交历史。

        CCXT 的 fetch_my_trades 在 defaultType="swap" 下对现货成交返回为空，而
        reconcile/_infer_final_state_via_trades 又强依赖成交历史精确判最终状态。
        这里绕开 CCXT 直接走 REST /info，现货与永续一次性全拉回来，上层按 oid
        精确匹配 managed.exchange_order_id 即可。

        返回结构样例（混合现货和永续）:
            {
              'oid': 392723517075,        # 订单 ID（== 程序里的 exchange_order_id）
              'coin': 'SOL',              # 永续: 字母；现货: "@N"（token_index）
              'px': '88.684',             # 成交价
              'sz': '0.13',               # 成交量
              'side': 'B',                # 'B' = Buy, 'A' = Sell (Ask)
              'dir': 'Close Short',       # 方向语义
              'fee': '0.006132',
              'feeToken': 'USDC',
              'time': 1776863556087,      # Unix ms
              'hash': '0x...',
              'tid': 683109186144482,     # trade id
              ...
            }

        Args:
            start_time_ms: 可选，只返回这个时间之后的成交（Unix ms）。
                           None 则请求全量 userFills（最近 2000 笔上下）

        Returns:
            List[dict]: 成交列表（按时间倒序，最新的在前）。失败时返回空列表，不抛异常。
        """
        self._ensure_authenticated()

        wallet = self._auth_config.get("wallet_address", "").strip()
        if not wallet:
            raise ValueError("wallet_address 未配置，无法拉取 userFills")

        base_url = self._api_config.get("base_url", "https://api.hyperliquid.xyz")
        url = base_url.rstrip("/") + "/info"

        if start_time_ms is not None:
            payload = {
                "type": "userFillsByTime",
                "user": wallet,
                "startTime": int(start_time_ms),
            }
        else:
            payload = {"type": "userFills", "user": wallet}

        try:
            import requests
            resp = requests.post(url, json=payload, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self._logger.warning(
                f"fetch_user_fills 异常: {type(e).__name__}: {e}"
            )
            return []

        if not isinstance(data, list):
            self._logger.warning(
                f"fetch_user_fills 返回非列表: type={type(data).__name__} data={data!r:.200}"
            )
            return []

        self._logger.debug(
            f"fetch_user_fills: 返回 {len(data)} 条成交"
            + (f" (startTime={start_time_ms})" if start_time_ms else "")
        )
        return data

    # ---- 订单管理 ----

    def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None,
        client_order_id: Optional[str] = None,
    ) -> Order:
        self._ensure_authenticated()

        # 精度处理
        formatted_amount = float(self._exchange.amount_to_precision(symbol, amount))
        formatted_price = (
            float(self._exchange.price_to_precision(symbol, price))
            if price is not None
            else None
        )

        cid_tag = f" cid={client_order_id}" if client_order_id else ""
        self._logger.info(
            f"创建订单{cid_tag}: {symbol} {side.value} {order_type.value} "
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
        self._logger.info(f"订单已创建{cid_tag}: eid={order.id}, status={order.status}")
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

        # ── 优先：批量撤单接口（单次 API 调用，延迟最低） ──────────────────
        try:
            if symbol:
                # 指定 symbol：单次调用即可
                result = self._exchange.cancel_all_orders(symbol)
                cancelled = len(result) if isinstance(result, list) else 0
            else:
                # 未指定 symbol：按 symbol 分组批量撤（Hyperliquid 要求指定 symbol）
                open_orders = self.fetch_open_orders()
                if not open_orders:
                    return 0
                # 收集有挂单的 symbol 去重
                active_symbols = list({o.symbol for o in open_orders})
                cancelled = 0
                for sym in active_symbols:
                    result = self._exchange.cancel_all_orders(sym)
                    cancelled += len(result) if isinstance(result, list) else 0

            self._logger.info(f"已撤销 {cancelled} 个订单（批量接口）")
            return cancelled

        except Exception as e:
            # ── Fallback：串行逐个撤单 ─────────────────────────────────────
            self._logger.warning(f"批量撤单接口不可用，回退串行模式: {e}")

            open_orders = self.fetch_open_orders(symbol)
            if not open_orders:
                return 0

            cancelled = 0
            for order in open_orders:
                try:
                    self._exchange.cancel_order(order.id, order.symbol)
                    cancelled += 1
                except Exception as cancel_err:
                    self._logger.warning(f"撤销订单 {order.id} 失败: {cancel_err}")

            self._logger.info(f"已撤销 {cancelled}/{len(open_orders)} 个订单（串行模式）")
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
