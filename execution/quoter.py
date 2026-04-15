#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双边报价计算器 (Quoter)

根据中间价和策略参数计算 bid/ask 报价与下单数量，核心职责：

1. 报价计算   — mid ± half_spread 计算双边价格，名义金额换算数量
2. 库存 skew  — 根据持仓偏移报价锚点，驱动库存向零回归
3. 重报判断   — mid 偏移超过阈值时触发撤旧挂新

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
    - 基于 realized_vol / ATR 的动态价差
    - 多档报价（L1~L3）
    - ML 预测信号偏斜（接入 ml_service 后）
"""

import logging
from typing import Dict, Optional, Tuple

from execution.inventory import InventoryTracker

logger = logging.getLogger(__name__)


class Quoter:
    """
    双边报价计算器。

    根据策略参数和持仓状态计算 bid/ask/size，并判断是否需要重新报价。
    支持基于库存的 skew 调整：偏移报价锚点 + 加宽价差，驱动库存向零回归。
    """

    def __init__(
        self,
        strategy_cfg: dict,
        inventory: InventoryTracker,
        max_delta_per_symbol: float = 1.0,
    ) -> None:
        self._strategy_cfg = strategy_cfg
        self._inventory = inventory
        # max_position_for_skew 优先于 max_delta_per_symbol
        self._max_delta = (
            strategy_cfg.get("max_position_for_skew")
            if strategy_cfg.get("max_position_for_skew") is not None
            else max_delta_per_symbol
        )

    def compute(self, symbol: str, mid: float) -> Tuple[float, float, float]:
        """
        根据 mid 价格和策略参数计算 bid/ask/size。

        计算流程：
        1. 基础半价差: half_spread = mid × spread_pct
        2. 库存 skew (delta_ratio = clamp(delta / max_delta, -1, 1)):
           - 价差加宽: effective_half_spread = half_spread × (1 + spread_penalty_factor × delta_ratio²)
           - 锚点偏移: adjusted_mid = mid - skew_intensity × delta_ratio × half_spread
        3. 最终报价: bid = adjusted_mid - effective_half_spread
                     ask = adjusted_mid + effective_half_spread

        Args:
            symbol: 交易对
            mid:    当前中间价

        Returns:
            (bid_price, ask_price, size) — 精度由 gateway 规范化
        """
        half_spread = mid * self._strategy_cfg["spread_pct"]

        # ── 库存 skew ────────────────────────────────────────────────────
        skew_intensity = self._strategy_cfg.get("skew_intensity", 0.0)
        spread_penalty_factor = self._strategy_cfg.get("spread_penalty_factor", 0.0)

        if skew_intensity != 0.0 or spread_penalty_factor != 0.0:
            delta = self._inventory.get_position(symbol)
            max_delta = self._max_delta
            # delta_ratio 夹到 [-1, 1]，防止 delta 暂时超限时出现过激报价
            delta_ratio = max(-1.0, min(1.0, delta / max_delta)) if max_delta > 0 else 0.0

            # 第二层：价差加宽 — delta_ratio² 使小仓位时几乎不变，大仓位时显著加宽
            spread_multiplier = 1.0 + spread_penalty_factor * delta_ratio * delta_ratio
            effective_half_spread = half_spread * spread_multiplier

            # 第一层：价格偏移 — 持多头压低报价鼓励卖出，持空头抬高报价鼓励买入
            skew_offset = skew_intensity * delta_ratio * half_spread
            adjusted_mid = mid - skew_offset

            logger.debug(
                "[%s] skew: delta=%.4f, ratio=%.4f, offset=%.6f, spread_mult=%.4f",
                symbol, delta, delta_ratio, skew_offset, spread_multiplier,
            )
        else:
            # skew 关闭，行为与改动前完全一致
            adjusted_mid = mid
            effective_half_spread = half_spread

        bid_price = adjusted_mid - effective_half_spread
        ask_price = adjusted_mid + effective_half_spread

        # 数量 = 名义金额 / 中价；gateway 会做最小单位裁剪
        size = self._strategy_cfg["order_size_usd"] / mid if mid > 0 else 0.0
        return bid_price, ask_price, size

    def skew_info(self, symbol: str, mid: float) -> Dict[str, float]:
        """返回 skew 计算的中间变量，供调试日志使用。"""
        delta = self._inventory.get_position(symbol)
        max_delta = self._max_delta
        delta_ratio = max(-1.0, min(1.0, delta / max_delta)) if max_delta > 0 else 0.0
        half_spread = mid * self._strategy_cfg["spread_pct"]
        skew_intensity = self._strategy_cfg.get("skew_intensity", 0.0)
        spread_penalty_factor = self._strategy_cfg.get("spread_penalty_factor", 0.0)
        return {
            "delta": delta,
            "delta_ratio": delta_ratio,
            "skew_offset": skew_intensity * delta_ratio * half_spread,
            "spread_multiplier": 1.0 + spread_penalty_factor * delta_ratio * delta_ratio,
        }

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
