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
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from gateways.gateway import Position


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
    # 已实现 PnL（USDC，已扣除 fee —— 见 on_fill 里的 Step 3）
    realized_pnl: float = 0.0
    # 累积总手续费（折算成 USDC，始终非负）。用于 PnL 日志 gross vs net 对比
    cumulative_fees_usdc: float = 0.0
    # (account, currency) → 余额，account 为 "spot" 或 "perp"
    balances: Dict[str, Dict[str, float]] = field(default_factory=dict)
    # 线程安全锁：on_fill 可能在 OM-Reconcile 线程中被调用，
    # 而主循环线程同时读取 positions/avg_entry，需要互斥保护
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def on_fill(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: float,
        fee_cost: float = 0.0,
        fee_currency: Optional[str] = None,
    ) -> None:
        """
        根据成交更新持仓、加权均价、已实现 PnL、累积手续费、现货余额。

        已实现 PnL 按加权均价法结算：平仓部分按 (成交价 - 均价) 计入，再扣除本笔 fee。

        fee_cost / fee_currency 语义（由 gateway.Order.from_ccxt 解析得到）：
            - 现货 BUY:  fee 扣 base coin  (fee_currency = "USOL" / 别名 "SOL" 等)
            - 现货 SELL: fee 扣 USDC       (fee_currency = "USDC")
            - 永续:      fee 扣 USDC       (fee_currency = "USDC")
            - 含 builderFee 累加（Gateway 层已按 fill price 折算到对应币种）

        缺省时 fee_cost=0.0 / fee_currency=None，不影响 PnL 和余额字典。
        """
        # 防御性：None / 负数 / 非法值一律按 0 处理
        if fee_cost is None or fee_cost < 0:
            fee_cost = 0.0
        else:
            try:
                fee_cost = float(fee_cost)
            except (TypeError, ValueError):
                fee_cost = 0.0

        with self._lock:
            prev = self.positions.get(symbol, 0.0)
            change = amount if side == "buy" else -amount
            new_pos = prev + change

            # ── 已实现 PnL（仓位缩减时按均价结算）────────────────────
            entry = self.avg_entry.get(symbol, 0.0)
            if entry > 0:
                if prev > 0 and change < 0:
                    # 平多仓
                    closed = min(prev, amount)
                    self.realized_pnl += closed * (price - entry)
                elif prev < 0 and change > 0:
                    # 平空仓
                    closed = min(abs(prev), amount)
                    self.realized_pnl += closed * (entry - price)

            # ── 更新加权均价 ─────────────────────────────────────────
            if new_pos == 0:
                self.avg_entry.pop(symbol, None)
            elif prev >= 0 and change > 0:
                # 加多仓（含从零新建）
                prev_cost = prev * self.avg_entry.get(symbol, price)
                self.avg_entry[symbol] = (prev_cost + change * price) / (prev + change)
            elif prev <= 0 and change < 0:
                # 加空仓（含从零新建）
                prev_cost = abs(prev) * self.avg_entry.get(symbol, price)
                self.avg_entry[symbol] = (
                    (prev_cost + abs(change) * price) / (abs(prev) + abs(change))
                )
            elif (prev > 0 and new_pos < 0) or (prev < 0 and new_pos > 0):
                # 翻仓（穿越零点）— 新方向以当前价为均价
                self.avg_entry[symbol] = price
            # else: 减仓但未穿越零点 → 保持原均价

            # ── 更新余额字典（供 BalanceGuard 实时感知账户余额变化）──
            # 仅对现货 symbol 生效（永续没有现货余额概念，保证金变动由 fetch_balance 刷）。
            # 现货的 fee 都会从 spot balance 扣除（USDC 从 quote 扣、base coin 从 base 扣），
            # 保持 Inventory 本地余额与 fetch_balance 真实快照的长期对齐。永续的 fee
            # 不扣 balance 字典（永续没有 spot 余额概念，fee 仅进 realized_pnl）。
            if ":" not in symbol:
                base_asset, quote_asset = symbol.split("/")
                notional = amount * price
                spot = self.balances.setdefault("spot", {})
                if side == "buy":
                    spot[quote_asset] = spot.get(quote_asset, 0.0) - notional
                    spot[base_asset] = spot.get(base_asset, 0.0) + amount
                else:
                    spot[quote_asset] = spot.get(quote_asset, 0.0) + notional
                    spot[base_asset] = spot.get(base_asset, 0.0) - amount

            self.positions[symbol] = new_pos

            # ── Step 1: 折算 fee 到 USDC ─────────────────────────────
            if fee_cost > 0 and fee_currency:
                if fee_currency == "USDC":
                    fee_usdc = fee_cost
                else:
                    # base coin 扣费（现货 BUY），用当前 price 折算
                    # price 单位是"每单位 base 多少 USDC"，fee_cost * price 即折算值
                    fee_usdc = fee_cost * price
            else:
                fee_usdc = 0.0

            # ── Step 2: 累积总 fee ──────────────────────────────────
            self.cumulative_fees_usdc += fee_usdc

            # ── Step 3: 从 realized_pnl 扣除 fee ────────────────────
            self.realized_pnl -= fee_usdc

            # ── Step 4: 扣 spot 余额（现货 fee，USDC 或 base coin 都扣）──
            # 修复 A1 遗留：USDC fee（现货 SELL / spot SELL 路径）过去不扣 spot 字典，
            # 长期会让 spot[USDC] 快照偏高 ≈ notional × fee_rate × N 笔累积。
            # USDC fee（现货 SELL）：从 quote 币扣，保持 spot[USDC] 快照与真实余额对齐
            # base coin fee（现货 BUY，Hyperliquid 特例）：从 base 币扣
            # 永续的 fee 不在此处理（没有 spot balance 概念，fee 已进 realized_pnl）
            if ":" not in symbol and fee_cost > 0 and fee_currency:
                base_asset, quote_asset = symbol.split("/")
                spot = self.balances.setdefault("spot", {})
                if fee_currency == "USDC":
                    # quote 币扣费（现货 SELL）
                    spot[quote_asset] = spot.get(quote_asset, 0.0) - fee_cost
                else:
                    # base 币扣费（现货 BUY）
                    # fee_currency 可能是原生名（"USOL"）或 CCXT base 别名（"SOL"）
                    # 优先按 fee_currency 查 spot dict key；找不到 fallback 到 symbol 拆出的 base_asset
                    target_key = fee_currency if fee_currency in spot else base_asset
                    spot[target_key] = spot.get(target_key, 0.0) - fee_cost

    def sync_from_positions(self, positions: List["Position"]) -> int:
        """
        从交易所持仓快照批量初始化/同步本地状态。

        用于启动时拉取 gateway.fetch_positions() 的结果，一次性写入
        positions 和 avg_entry，避免逐笔 on_fill 的开销和语义不匹配。

        Args:
            positions: gateway.fetch_positions() 返回的 Position 列表

        Returns:
            同步的品种数量
        """
        with self._lock:
            self.positions.clear()
            self.avg_entry.clear()
            for pos in positions:
                if pos.size == 0 or pos.side == "flat":
                    continue
                delta = pos.size if pos.side == "long" else -pos.size
                self.positions[pos.symbol] = delta
                if pos.entry_price > 0:
                    self.avg_entry[pos.symbol] = pos.entry_price
        return len(self.positions)

    def get_position(self, symbol: str) -> float:
        """返回单品种净 Delta，不存在时返回 0.0"""
        with self._lock:
            return self.positions.get(symbol, 0.0)

    def get_all_positions(self) -> Dict[str, float]:
        """返回所有品种持仓的快照拷贝"""
        with self._lock:
            return dict(self.positions)

    def get_net_delta(self, base_asset: str, asset_map: Dict[str, str]) -> float:
        """
        聚合某个 base asset 在所有市场（现货 + 永续）的净 Delta。

        Args:
            base_asset: 标的资产名称（如 "SOL"）
            asset_map:  symbol → base_asset 的映射表

        Returns:
            净 Delta（现货多头 + 永续空头 应接近 0）
        """
        # 持锁拷贝快照，锁外计算
        with self._lock:
            pos_snapshot = dict(self.positions)
        total = 0.0
        for sym, delta in pos_snapshot.items():
            if asset_map.get(sym) == base_asset:
                total += delta
        return total

    # =========================================================================
    # 余额管理
    # =========================================================================

    def set_balance(self, account: str, currency: str, amount: float) -> None:
        """设置指定账户和币种的余额"""
        with self._lock:
            if account not in self.balances:
                self.balances[account] = {}
            self.balances[account][currency] = amount

    def get_balance(self, account: str, currency: str) -> float:
        """获取指定账户和币种的余额，不存在返回 0.0"""
        with self._lock:
            return self.balances.get(account, {}).get(currency, 0.0)

    def get_all_balances(self) -> Dict[str, Dict[str, float]]:
        """返回所有余额的快照拷贝"""
        with self._lock:
            return {acct: dict(curs) for acct, curs in self.balances.items()}

    def deduct_balance(self, account: str, currency: str, amount: float) -> None:
        """扣减余额（成交后调用）"""
        with self._lock:
            cur = self.balances.get(account, {}).get(currency, 0.0)
            if account not in self.balances:
                self.balances[account] = {}
            self.balances[account][currency] = cur - amount

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
