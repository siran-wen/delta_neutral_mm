#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双边报价计算器 (Quoter)

根据中间价和策略参数计算 bid/ask 报价与下单数量，核心职责：

1. 报价计算   — mid ± half_spread 计算双边价格，名义金额换算数量
2. 重报判断   — mid 偏移超过阈值时触发撤旧挂新

线程安全:
    Quoter 无可变状态，不需要锁保护。
    内部读取 InventoryTracker 时依赖其自身的锁。

用法:
    from execution.inventory import InventoryTracker
    from execution.quoter import Quoter

    inventory = InventoryTracker()
    quoter = Quoter(strategy_cfg={"spread_pct": 0.001, ...}, inventory=inventory)

    bid, ask, size = quoter.compute("BTC/USDC:USDC", mid=35000.0)
    need = quoter.should_requote("BTC/USDC:USDC", old_mid=35000.0, new_mid=35050.0)

TODO: 后续迭代实现：
    - 基于 inventory delta 的 skew 调整
    - 基于 realized_vol / ATR 的动态价差
    - 多档报价（L1~L3）
    - ML 预测信号偏斜（接入 ml_service 后）
"""

from typing import Tuple

from execution.inventory import InventoryTracker


class Quoter:
    """
    双边报价计算器。

    根据策略参数和持仓状态计算 bid/ask/size，并判断是否需要重新报价。
    """

    def __init__(self, strategy_cfg: dict, inventory: InventoryTracker) -> None:
        self._strategy_cfg = strategy_cfg
        self._inventory = inventory

    def compute(self, symbol: str, mid: float) -> Tuple[float, float, float]:
        """
        根据 mid 价格和策略参数计算 bid/ask/size。

        Args:
            symbol: 交易对（当前未用，供 skew 扩展预留）
            mid:    当前中间价

        Returns:
            (bid_price, ask_price, size) — 精度由 gateway 规范化

        TODO: 后续迭代实现：
            - 基于 inventory.get_position(symbol) 的 skew 调整
              （持多→抬高卖价/降低买价，引导库存回归中性）
            - 基于 realized_vol / ATR 的动态价差
            - 多档报价（L1~L3，当前仅挂最优档）
            - ML 预测信号偏斜（接入 ml_service 后）
        """
        half_spread = mid * self._strategy_cfg["spread_pct"]
        bid_price = mid - half_spread
        ask_price = mid + half_spread
        # 数量 = 名义金额 / 中价；gateway 会做最小单位裁剪
        size = self._strategy_cfg["order_size_usd"] / mid if mid > 0 else 0.0
        return bid_price, ask_price, size

    def should_requote(self, symbol: str, old_mid: float, new_mid: float) -> bool:
        """
        判断 mid 是否偏离足够大，需要撤旧单重新报价。

        TODO: 后续迭代加入更细粒度触发条件：
            - 当前挂单距 mid 的实际偏离（而非 mid 本身变化）
            - 库存 Delta 超过软阈值时强制刷新
            - 成交概率模型触发
        """
        if old_mid <= 0:
            return True
        return abs(new_mid - old_mid) / old_mid >= self._strategy_cfg["requote_threshold"]
