#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对冲执行器 (Hedger)

监听做市腿的成交事件，立即在对冲腿下反向单抵消敞口。
通过 OrderManager 下单，走完整的限速 + 风控链路。

线程模型:
    on_market_fill() 在 OM-Reconcile 线程中被调用，
    submit_order 是同步阻塞的，会阻塞对账线程直到下单完成。
    MVP 阶段接受这个延迟，后续可改为异步队列。

用法:
    from execution.hedger import Hedger, TradingPairConfig

    pairs = [TradingPairConfig(base="SOL", market_symbol="USOL/USDC",
                               hedge_symbol="SOL/USDC:USDC")]
    hedger = Hedger(gateway, om, rm, inventory, pairs)

    # 在 on_fill 回调中:
    hedger.on_market_fill(managed_order)
"""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

from execution.order_manager import ManagedOrder, OrderState, RequestPriority
from gateways import OrderSide, OrderType

if TYPE_CHECKING:
    from execution.inventory import InventoryTracker
    from execution.order_manager import OrderManager
    from gateways import BaseGateway
    from risk.pre_trade import RiskManager


@dataclass
class TradingPairConfig:
    """做市/对冲配对"""
    base: str                    # 标的资产（如 "SOL"）
    market_symbol: str           # 做市腿 symbol（如 "USOL/USDC"）
    hedge_symbol: str            # 对冲腿 symbol（如 "SOL/USDC:USDC"）


class Hedger:
    """
    对冲执行器

    监听做市腿的成交事件，立即在对冲腿下反向单抵消敞口。
    通过 OrderManager 下单，走完整的限速 + 风控链路。
    """

    def __init__(
        self,
        gateway: "BaseGateway",
        order_manager: "OrderManager",
        risk_manager: "RiskManager",
        inventory: "InventoryTracker",
        trading_pairs: List[TradingPairConfig],
        hedge_slippage: float = 0.002,
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self._gateway = gateway
        self._om = order_manager
        self._rm = risk_manager
        self._inventory = inventory
        self._trading_pairs = trading_pairs
        self._hedge_slippage = hedge_slippage
        self._dry_run = dry_run
        self._logger = logger or logging.getLogger("hedger")

        # market_symbol → TradingPairConfig 查找表
        self._market_to_pair: Dict[str, TradingPairConfig] = {
            pair.market_symbol: pair for pair in trading_pairs
        }

        # base_asset → 带符号的未对冲数量（正=需要空头对冲，负=需要多头对冲）
        self._pending_hedge_debt: Dict[str, float] = {}

        # 统计
        self._total_hedged: int = 0
        self._total_failed: int = 0

        # 暂停开关：InventoryBootstrap 期间置 True，防止建仓单触发重复对冲
        self._paused: bool = False

    # =========================================================================
    # 核心方法
    # =========================================================================

    def pause(self) -> None:
        """
        暂停对冲。

        用于启动阶段的 InventoryBootstrap：建仓自己会同时下"现货买 + 永续空"两腿，
        Hedger 若在此期间收到做市腿成交事件会重复下对冲单。建仓完成后调用 resume()。
        """
        self._paused = True
        self._logger.info("[HEDGE] 已暂停（bootstrap 期间）")

    def resume(self) -> None:
        """恢复对冲。与 pause() 成对使用。"""
        self._paused = False
        self._logger.info("[HEDGE] 已恢复")

    def on_market_fill(self, managed: ManagedOrder) -> None:
        """
        做市腿成交回调。

        由 main.py 的 on_fill 回调调用（在 OM-Reconcile 线程中执行）。
        忽略对冲腿自己的成交，避免无限循环。
        """
        # ── 暂停保护：bootstrap 期间不对冲（避免重复对冲建仓单） ──────────
        if self._paused:
            return

        # ── 第一步：判断是否是做市腿的成交 ────────────────────────────────
        pair = self._market_to_pair.get(managed.symbol)
        if pair is None:
            return  # 忽略非做市腿（包括对冲腿自己的成交）

        # ── 第二步：查找对应的 hedge_symbol ──────────────────────────────
        hedge_symbol = pair.hedge_symbol
        base = pair.base

        # ── 第三步：计算对冲方向和数量 ───────────────────────────────────
        fill_qty = managed.filled or 0.0
        if fill_qty <= 0:
            return

        # 现货买入 → 永续开空（sell），现货卖出 → 永续开多（buy）
        if managed.side == "buy":
            debt_change = fill_qty    # 正=需要卖出对冲
        else:
            debt_change = -fill_qty   # 负=需要买入对冲

        # ── 第四步：加上未对冲残量 ───────────────────────────────────────
        total_debt = debt_change + self._pending_hedge_debt.get(base, 0.0)

        if abs(total_debt) < 1e-12:
            self._logger.info(f"[HEDGE] {base} 净对冲需求为零，跳过")
            return

        hedge_side = "sell" if total_debt > 0 else "buy"
        hedge_amount = abs(total_debt)

        self._logger.info(
            f"[HEDGE] 做市腿成交: {managed.symbol} {managed.side} "
            f"{fill_qty:.6f} → 需对冲 {hedge_symbol} {hedge_side} {hedge_amount:.6f} "
            f"(含残量 {self._pending_hedge_debt.get(base, 0.0):+.6f})"
        )

        # ── Dry-run 模式：只打印不下单 ──────────────────────────────────
        if self._dry_run:
            self._logger.info(
                f"[HEDGE] [DRY-RUN] 跳过对冲下单: "
                f"{hedge_symbol} {hedge_side} {hedge_amount:.6f}"
            )
            return

        # ── 第五步：拉取 ticker 获取 mid，计算激进限价 ───────────────────
        try:
            ticker = self._gateway.fetch_ticker(hedge_symbol)
            mid = None
            if ticker.bid and ticker.ask:
                mid = (ticker.bid + ticker.ask) / 2.0
            elif ticker.last:
                mid = ticker.last

            if not mid:
                raise ValueError(f"无法获取 {hedge_symbol} 中间价")
        except Exception as e:
            self._logger.error(f"[HEDGE] 获取 {hedge_symbol} 行情失败: {e}")
            self._pending_hedge_debt[base] = total_debt
            self._total_failed += 1
            return

        if hedge_side == "sell":
            hedge_price = mid * (1 - self._hedge_slippage)
        else:
            hedge_price = mid * (1 + self._hedge_slippage)

        # ── 第六步：精度处理 ─────────────────────────────────────────────
        try:
            hedge_price = float(
                self._gateway.price_to_precision(hedge_symbol, hedge_price)
            )
            hedge_amount = float(
                self._gateway.amount_to_precision(hedge_symbol, hedge_amount)
            )
        except Exception as e:
            self._logger.error(f"[HEDGE] 精度处理失败 [{hedge_symbol}]: {e}")
            self._pending_hedge_debt[base] = total_debt
            self._total_failed += 1
            return

        if hedge_amount <= 0:
            self._logger.warning(f"[HEDGE] 精度处理后数量为零，跳过")
            self._pending_hedge_debt[base] = total_debt
            return

        # ── 第七步：风控检查 ─────────────────────────────────────────────
        risk_result = self._rm.pre_trade_check(
            hedge_symbol, hedge_side, hedge_amount,
            price=hedge_price, mid_price=mid,
        )
        if not risk_result.passed:
            self._logger.warning(
                f"[HEDGE] 风控拒绝: {hedge_symbol} {hedge_side} "
                f"{hedge_amount:.6f} @ {hedge_price} — {risk_result.reason}"
            )
            self._pending_hedge_debt[base] = total_debt
            self._total_failed += 1
            return

        # ── 第八步：通过 OrderManager 下单 ───────────────────────────────
        strategy_trigger_ts = (
            managed.state_changed_at or managed.ack_at or time.time()
        )
        order_side = OrderSide.SELL if hedge_side == "sell" else OrderSide.BUY

        try:
            hedge_order = self._om.submit_order(
                symbol=hedge_symbol,
                side=order_side,
                order_type=OrderType.LIMIT,
                amount=hedge_amount,
                price=hedge_price,
                strategy_trigger_ts=strategy_trigger_ts,
                priority=RequestPriority.HEDGE,
            )
        except Exception as e:
            self._logger.error(
                f"[HEDGE] 下单异常: {hedge_symbol} {hedge_side} — {e}"
            )
            self._pending_hedge_debt[base] = total_debt
            self._total_failed += 1
            return

        # ── 判断对冲结果 ─────────────────────────────────────────────────
        filled = hedge_order.filled or 0.0

        if hedge_order.state == OrderState.FILLED and filled > 0:
            # 成功：更新对冲腿持仓和风控
            self._inventory.on_fill(hedge_symbol, hedge_side, filled, hedge_price)
            signed_delta = filled if hedge_side == "buy" else -filled
            self._rm.update_position(hedge_symbol, signed_delta)

            # 清除债务（激进限价单假设全量成交）
            self._pending_hedge_debt.pop(base, None)

            self._total_hedged += 1
            hedge_latency = time.time() - strategy_trigger_ts
            self._logger.info(
                f"[HEDGE] 对冲成功: {hedge_symbol} {hedge_side} "
                f"{filled:.6f} @ {hedge_price}  "
                f"延迟={hedge_latency:.3f}s"
            )
        else:
            # 失败或未立即成交：记入债务，等待下次做市腿成交时重试
            self._pending_hedge_debt[base] = total_debt
            self._total_failed += 1
            self._logger.warning(
                f"[HEDGE] 对冲未成交: {hedge_symbol} {hedge_side} "
                f"state={hedge_order.state.value} filled={filled:.6f}  "
                f"未对冲={total_debt:+.6f}  "
                f"reason={hedge_order.reject_reason or 'N/A'}"
            )

    # =========================================================================
    # 辅助方法
    # =========================================================================

    @property
    def stats(self) -> Dict:
        """对冲统计摘要"""
        return {
            "total_hedged": self._total_hedged,
            "total_failed": self._total_failed,
            "pending_hedge_debt": dict(self._pending_hedge_debt),
        }

    def build_asset_map(self) -> Dict[str, str]:
        """
        构建 symbol → base_asset 映射表，
        供 InventoryTracker.get_net_delta 使用。
        """
        asset_map: Dict[str, str] = {}
        for pair in self._trading_pairs:
            asset_map[pair.market_symbol] = pair.base
            asset_map[pair.hedge_symbol] = pair.base
        return asset_map
