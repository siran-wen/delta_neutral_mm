#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持仓跟踪器 (Inventory Tracker)

本地持仓与损益跟踪模块，核心职责：

1. 持仓状态管理 — 按品种维护净 Delta 和加权平均入场价
2. 未实现 PnL   — 按 mid 价格估算浮动盈亏
3. 线程安全     — 所有读写操作持锁保护，支持多线程并发访问

线程模型:
    on_fill() 可能在 OM-Reconcile 线程中被调用，而主循环线程同时
    通过 get_position / get_all_positions / unrealized_pnl 读取。
    所有公开方法通过 self._lock 互斥保护。

用法:
    from execution.inventory import InventoryTracker

    inventory = InventoryTracker()
    inventory.on_fill("BTC/USDC:USDC", "buy", 0.001, 35000.0)
    pos = inventory.get_position("BTC/USDC:USDC")
    upnl = inventory.unrealized_pnl({"BTC/USDC:USDC": 36000.0})

TODO: 后续迭代实现：
    - FIFO 成本基础（realized PnL 精确核算）
    - 资金费率累计
    - 多账户/多品种 USD 等值汇总
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class InventoryTracker:
    """
    本地持仓与损益跟踪。

    TODO: 后续迭代实现：
        - FIFO 成本基础（realized PnL 精确核算）
        - 资金费率累计
        - 多账户/多品种 USD 等值汇总
    """
    # symbol → 净 Delta（正=多头, 负=空头）
    positions: Dict[str, float] = field(default_factory=dict)
    # symbol → 加权平均入场价
    avg_entry: Dict[str, float] = field(default_factory=dict)
    # 已实现 PnL（USDC），暂不计算，留 placeholder
    realized_pnl: float = 0.0
    # 线程安全锁：on_fill 可能在 OM-Reconcile 线程中被调用，
    # 而主循环线程同时读取 positions/avg_entry，需要互斥保护
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def on_fill(self, symbol: str, side: str, amount: float, price: float) -> None:
        """
        根据成交更新持仓和加权均价。

        TODO: 替换为精确计算，包含：
              - 平仓时的 realized PnL 结算
              - FIFO / 加权均价两种模式切换
        """
        with self._lock:
            prev = self.positions.get(symbol, 0.0)
            change = amount if side == "buy" else -amount
            new_pos = prev + change

            # 更新加权均价（仅在加仓时更新；减仓不改变成本价）
            if change > 0:
                prev_cost = abs(prev) * self.avg_entry.get(symbol, price)
                self.avg_entry[symbol] = (prev_cost + change * price) / (abs(prev) + change)
            elif new_pos == 0:
                self.avg_entry.pop(symbol, None)

            self.positions[symbol] = new_pos

    def get_position(self, symbol: str) -> float:
        """返回单品种净 Delta，不存在时返回 0.0"""
        with self._lock:
            return self.positions.get(symbol, 0.0)

    def get_all_positions(self) -> Dict[str, float]:
        """返回所有品种持仓的快照拷贝"""
        with self._lock:
            return dict(self.positions)

    def unrealized_pnl(self, mid_prices: Dict[str, float]) -> float:
        """
        按当前 mid 估算未实现 PnL。

        TODO: 替换为标记价格（mark price），考虑资金费率。
        """
        # 持锁拷贝快照，锁外计算
        with self._lock:
            pos_snapshot = dict(self.positions)
            entry_snapshot = dict(self.avg_entry)
        total = 0.0
        for sym, delta in pos_snapshot.items():
            mid = mid_prices.get(sym)
            entry = entry_snapshot.get(sym)
            if mid and entry:
                total += delta * (mid - entry)
        return total

    def summary_lines(self, mid_prices: Dict[str, float]) -> List[str]:
        """生成持仓摘要行，供 shutdown 时打印"""
        # 持锁拷贝快照，锁外生成字符串
        with self._lock:
            pos_snapshot = dict(self.positions)
            entry_snapshot = dict(self.avg_entry)
        lines = []
        for sym, delta in pos_snapshot.items():
            mid = mid_prices.get(sym, 0.0)
            entry = entry_snapshot.get(sym, 0.0)
            upnl = delta * (mid - entry) if mid and entry else 0.0
            lines.append(
                f"  {sym:<22} delta={delta:+.6f}  "
                f"entry={entry:.4f}  mid={mid:.4f}  uPnL={upnl:+.2f} USDC"
            )
        return lines
