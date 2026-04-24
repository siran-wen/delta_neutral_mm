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
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

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
        hedge_threshold: float = 0.05,
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self._gateway = gateway
        self._om = order_manager
        self._rm = risk_manager
        self._inventory = inventory
        self._trading_pairs = trading_pairs
        self._hedge_slippage = hedge_slippage
        # 净 delta 绝对值小于此阈值时跳过对冲（默认 0.05 ≈ 单笔做市 size 的一半）
        self._hedge_threshold = hedge_threshold
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

        # 失败原因分类计数（供关停摘要 [HEDGE] 失败原因分布 用）
        self._failure_reasons: Dict[str, int] = {
            "timeout": 0,       # submit 返回时非 FILLED
            "risk_reject": 0,   # pre_trade_check 拒绝
            "api_error": 0,     # 下单异常 / 获取行情失败
            "other": 0,         # 精度、size<=0 等其他
        }

        # 单调递增的对冲序号，用于 [HEDGE] #N 日志标签贯穿触发→决策→成交链路
        self._hedge_sequence: int = 0

        # 暂停开关：InventoryBootstrap 期间置 True，防止建仓单触发重复对冲
        self._paused: bool = False

        # ── Late-fill 假失败修正机制 ──────────────────────────────────────
        # 背景：hedge 订单 submit 返回时若未 FILLED（OPEN / PARTIALLY_FILLED），
        # 当前逻辑立刻 total_failed += 1；但 OM 的 Bug B 修复路径会在撤单 5s 后
        # 通过 userFills 延迟发现真成交、emit "order_filled"。统计不修正就会虚高。
        #
        # 方案：登记 timeout 分类的失败为 late-fill 候选，监听 order_filled，
        #       事件触发时回滚统计（failed-1 / timeout-1 / hedged+1 / late_recovered+1）。
        #
        # late-fill 候选表：client_order_id → (base, net_delta_at_trigger, registered_ts)
        self._late_fill_candidates: Dict[str, Tuple[str, float, float]] = {}

        # 候选 TTL（秒）：超过此时间无回调的候选清理掉，避免内存泄漏。
        # 5 分钟足够覆盖 OM Bug B 的 5s 复查窗口 + 各种网络抖动。
        self._late_fill_ttl: float = 300.0

        # 累积的"被捞回"次数（从 failed 纠正为 hedged，只增不减）
        self._late_recovered: int = 0

        # ── Latency 诊断记录 ─────────────────────────────────────────────
        # 按 cid 索引，保留最近 _latency_records_cap 笔 hedge 的延迟分布数据，
        # 关停时 latency_report() 汇总输出。
        # 字段：ccxt_rtt / emit_delay / source / final_state / registered_ts / ack_ts
        self._latency_records: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._latency_records_cap: int = 200

        # 线程安全锁：OM 回调在 reconcile/main 线程触发、hedger 主路径在 on_market_fill
        # 调用线程运行，跨线程读写 _late_fill_candidates / _total_failed /
        # _failure_reasons / _total_hedged / _late_recovered / _pending_hedge_debt /
        # _latency_records 需互斥保护。
        self._stats_lock: threading.Lock = threading.Lock()

        # 订阅 OM order_filled 事件。OM 的接口是 om.on(event_name, callback)。
        # _handle_late_fill 会过滤非 hedger 来源订单，对 market leg fills 静默跳过。
        self._om.on("order_filled", self._handle_late_fill)

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

        决策基于 inventory.get_net_delta —— 读取做市腿 + 对冲腿当前持仓的聚合
        净 delta，方向和数量都由净 delta 的符号/幅度决定。避免早期实现"只按
        本次 fill_qty 增量 ± pending_debt"导致过对冲后回不来的 bug。
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

        fill_qty = managed.filled or 0.0
        if fill_qty <= 0:
            return

        # ── 第三步：分配序号并打印触发日志 ───────────────────────────────
        self._hedge_sequence += 1
        seq = self._hedge_sequence

        asset_map = {pair.market_symbol: base, pair.hedge_symbol: base}
        net_delta = self._inventory.get_net_delta(base, asset_map)
        pending_residual = self._pending_hedge_debt.get(base, 0.0)

        self._logger.info(
            f"[HEDGE] #{seq} 触发: 做市腿成交 {managed.symbol} {managed.side} "
            f"{fill_qty:.6f}  当前净delta={net_delta:+.6f}  "
            f"已pending残量={pending_residual:+.6f}"
        )

        # 阈值内视为平衡，不对冲
        if abs(net_delta) < self._hedge_threshold:
            self._logger.info(
                f"[HEDGE] #{seq} 跳过: 需对冲量 {net_delta:+.6f} "
                f"在阈值 ±{self._hedge_threshold:.6f} 内"
            )
            with self._stats_lock:
                self._pending_hedge_debt.pop(base, None)
            return

        # net_delta > 0：现货偏多 / 永续不够空 → 卖永续
        # net_delta < 0：现货偏少 / 永续过空 → 买永续（反向修正）
        if net_delta > 0:
            hedge_side = "sell"
            hedge_amount = net_delta
        else:
            hedge_side = "buy"
            hedge_amount = -net_delta

        # ── Dry-run 模式：只打印不下单 ──────────────────────────────────
        if self._dry_run:
            self._logger.info(
                f"[HEDGE] #{seq} [DRY-RUN] 跳过对冲下单: "
                f"{hedge_symbol} {hedge_side} {hedge_amount:.6f}"
            )
            return

        # ── 第四步：拉取 ticker 获取 mid，计算激进限价 ───────────────────
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
            self._logger.error(
                f"[HEDGE] #{seq} 获取行情失败: {hedge_symbol} "
                f"exc={type(e).__name__}: {e}  → 保留 pending={net_delta:+.6f}"
            )
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._total_failed += 1
                self._failure_reasons["api_error"] += 1
            return

        if hedge_side == "sell":
            hedge_price = mid * (1 - self._hedge_slippage)
        else:
            hedge_price = mid * (1 + self._hedge_slippage)

        # ── 第五步：精度处理 ─────────────────────────────────────────────
        try:
            hedge_price = float(
                self._gateway.price_to_precision(hedge_symbol, hedge_price)
            )
            hedge_amount = float(
                self._gateway.amount_to_precision(hedge_symbol, hedge_amount)
            )
        except Exception as e:
            self._logger.error(
                f"[HEDGE] #{seq} 精度处理失败: {hedge_symbol} "
                f"exc={type(e).__name__}: {e}  → 保留 pending={net_delta:+.6f}"
            )
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._total_failed += 1
                self._failure_reasons["other"] += 1
            return

        if hedge_amount <= 0:
            self._logger.warning(
                f"[HEDGE] #{seq} 精度处理后数量为零，跳过  "
                f"→ 保留 pending={net_delta:+.6f}"
            )
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._failure_reasons["other"] += 1
            return

        # 决策日志（拿到最终 price/amount 之后）
        self._logger.info(
            f"[HEDGE] #{seq} 决策: {hedge_symbol} {hedge_side} "
            f"{hedge_amount:.6f} @ {hedge_price:.4f}  "
            f"(slip={self._hedge_slippage:.3%}  mid={mid:.4f})"
        )

        # ── 第六步：风控检查 ─────────────────────────────────────────────
        risk_result = self._rm.pre_trade_check(
            hedge_symbol, hedge_side, hedge_amount,
            price=hedge_price, mid_price=mid,
        )
        if not risk_result.passed:
            self._logger.warning(
                f"[HEDGE] #{seq} 风控拒绝: {hedge_symbol} {hedge_side} "
                f"{hedge_amount:.6f} @ {hedge_price:.4f}  "
                f"reason={risk_result.reason}  "
                f"→ 动作: 保留 pending={net_delta:+.6f}，等待下次成交触发重试"
            )
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._total_failed += 1
                self._failure_reasons["risk_reject"] += 1
            return

        # ── 第七步：通过 OrderManager 下单 ───────────────────────────────
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
                source="hedger",
            )
        except Exception as e:
            self._logger.error(
                f"[HEDGE] #{seq} 下单异常: {hedge_symbol} {hedge_side}  "
                f"exc={type(e).__name__}: {e}  → 保留 pending={net_delta:+.6f}"
            )
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._total_failed += 1
                self._failure_reasons["api_error"] += 1
            return

        # ── 判断对冲结果 ─────────────────────────────────────────────────
        filled = hedge_order.filled or 0.0

        # submit 返回后的链路诊断数据（两分支共享）。
        # ccxt_rtt = gateway.create_order 的 HTTP RTT（不含风控/精度处理）。
        ccxt_rtt: Optional[float] = (
            hedge_order.latency.order_rtt if hedge_order.latency else None
        )

        if hedge_order.state == OrderState.FILLED and filled > 0:
            # 成功：inventory / RiskManager 的更新不再在此处做。
            # OM 的 submit-immediate 修复（2026-04-24）之后，submit 返回即终态
            # 的 FILLED 会同步 emit "order_filled"，main.py 注册的 on_fill
            # 回调统一走 inventory.on_fill + rm.update_position，这里再重复调
            # 就是双计。_handle_late_fill 走 late-fill 捞回路径时也只改
            # stats，不碰 inventory，逻辑一致。
            # 这里只保留 hedger 自己的账本：对冲债务 + 成功计数。

            # 清除债务（激进限价单假设全量成交）+ 累计成功次数
            with self._stats_lock:
                self._pending_hedge_debt.pop(base, None)
                self._total_hedged += 1

            hedge_latency = time.time() - strategy_trigger_ts
            # 真实成交均价：优先 ManagedOrder.avg_fill_price（OM 从 CCXT 'average'
            # 或 Hyperliquid 原生 avgPx 回填），缺失时 fallback 查 userFills 按 oid
            # 聚合加权均价，最后兜底用 limit price 保证日志不中断。
            real_avg = self._resolve_fill_avg_price(hedge_order, fallback=hedge_price)
            self._logger.info(
                f"[HEDGE] #{seq} 成交确认 cid={hedge_order.client_order_id}: "
                f"filled={filled:.6f} @ {real_avg:.4f}  "
                f"延迟={hedge_latency:.3f}s"
            )
            # submit 返回诊断（成功分支：state=FILLED，submit 时即归一化为 FILLED）
            self._log_submit_return_diag(seq, hedge_order, hedge_amount, filled, ccxt_rtt)
            # Latency 记录：FILLED at submit ack → emit_delay=0 / source=submit_immediate
            self._record_latency(
                cid=hedge_order.client_order_id,
                ccxt_rtt=ccxt_rtt,
                emit_delay=0.0,
                source="submit_immediate",
                final_state="FILLED",
                ack_ts=(hedge_order.latency.exchange_ack_ts if hedge_order.latency else 0.0),
            )
        else:
            # 未立即成交：submit 返回但还没成交（OPEN / PARTIALLY_FILLED / REJECTED）
            # REJECTED 时 reject_reason 有效；其他视为"等 reconcile 确认"即超时
            reject_reason = hedge_order.reject_reason
            is_rejected = hedge_order.state.name == "REJECTED"
            category = "api_error" if is_rejected else "timeout"

            reason_tag = (
                f"reason={reject_reason}" if reject_reason
                else f"state={hedge_order.state.value}"
            )
            # 写共享状态（持锁）+ 登记 late-fill 候选。注意：日志 I/O 留到锁外。
            with self._stats_lock:
                self._pending_hedge_debt[base] = net_delta
                self._total_failed += 1
                self._failure_reasons[category] += 1
                # 只有 timeout 才可能"事后真成交"。
                # api_error / rejected 订单不会在交易所有 fill 记录，不登记候选。
                if category == "timeout":
                    self._late_fill_candidates[hedge_order.client_order_id] = (
                        base, net_delta, time.time()
                    )
                    # 顺手清理过期候选（摊销成本，不额外起定时器）
                    self._cleanup_expired_candidates_locked()

            self._logger.warning(
                f"[HEDGE] #{seq} 未成交 cid={hedge_order.client_order_id}: "
                f"{reason_tag}  filled={filled:.6f}  "
                f"→ 保留 pending={net_delta:+.6f}，等待 reconcile / 下次触发"
            )
            # submit 返回诊断（失败分支：叠加在现有 warning 之后）
            self._log_submit_return_diag(seq, hedge_order, hedge_amount, filled, ccxt_rtt)
            # Latency 记录：待 _handle_late_fill 捞回时填 emit_delay/source；
            # final_state 先记 submit 返回时的 state.value，捞回后会被修正为 FILLED。
            self._record_latency(
                cid=hedge_order.client_order_id,
                ccxt_rtt=ccxt_rtt,
                emit_delay=None,
                source=None,
                final_state=hedge_order.state.value,
                ack_ts=(hedge_order.latency.exchange_ack_ts if hedge_order.latency else 0.0),
            )

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def _resolve_fill_avg_price(
        self, hedge_order: ManagedOrder, fallback: float
    ) -> float:
        """
        对冲单真实成交均价解析。

        优先级：
            a. ManagedOrder.avg_fill_price — OM 在 sync_from_exchange 时从 gateway
               Order.average（CCXT 标准字段 / trades 聚合 / Hyperliquid 原生
               info.response.data.statuses[0].filled.avgPx）回填。
            b. 按 exchange_order_id 查 userFills 聚合加权均价 —— a 缺失时兜底，
               避免在 CCXT 未归一化 avgPx 的边角场景下继续打印 limit price。
            c. fallback（调用方传入的 limit price）—— 前两项都拿不到时，至少保留
               原来的日志字段，不影响 "成交确认" 行继续出现。

        Args:
            hedge_order: 已 FILLED 的对冲单 ManagedOrder
            fallback:    limit price 兜底值

        Returns:
            最佳可用的成交均价（优先级从高到低）。
        """
        avg = getattr(hedge_order, "avg_fill_price", None)
        if avg is not None and avg > 0:
            return float(avg)

        eid = hedge_order.exchange_order_id
        if not eid:
            return fallback

        try:
            start_ms = int(hedge_order.created_at * 1000) - 1000
            fills = self._gateway.fetch_user_fills(start_time_ms=start_ms)
        except Exception as e:
            self._logger.debug(
                f"[HEDGE] userFills 兜底拉取失败 eid={eid}: "
                f"{type(e).__name__}: {e}"
            )
            return fallback

        total_sz = 0.0
        total_notional = 0.0
        for f in fills or []:
            if str(f.get("oid", "")) != str(eid):
                continue
            try:
                sz = float(f.get("sz", 0) or 0)
                px = float(f.get("px", 0) or 0)
            except (TypeError, ValueError):
                continue
            if sz > 0 and px > 0:
                total_sz += sz
                total_notional += sz * px

        if total_sz > 0:
            return total_notional / total_sz
        return fallback

    @property
    def stats(self) -> Dict:
        """
        对冲统计快照（线程安全）。

        late_recovered: 启动以来被 _handle_late_fill 从 failed 捞回 hedged 的次数。
        """
        with self._stats_lock:
            return {
                "total_hedged": self._total_hedged,
                "total_failed": self._total_failed,
                "late_recovered": self._late_recovered,
                "pending_hedge_debt": dict(self._pending_hedge_debt),
            }

    # =========================================================================
    # Late-fill 假失败修正
    # =========================================================================

    def _handle_late_fill(self, order: "ManagedOrder") -> None:
        """
        OM emit 'order_filled' 事件时触发（通常在 reconcile 线程，cancel 路径也
        可能从 main 线程触发）。

        若该订单之前被 on_market_fill 登记为 late-fill 候选（timeout 判失败但
        事后真的成交），此处回滚 hedger 统计并清除对应的 pending 债务。

        非 hedger 来源订单、不在候选表的订单、重复回调，均静默跳过。
        """
        # ── 防御性过滤：只处理 hedger 来源订单 ──────────────────────
        # OM amend 路径会把 source 改成 "amend:hedger"，也需要纳入。
        src = getattr(order, "source", "") or ""
        if not (src == "hedger" or src.startswith("amend:hedger")):
            return

        cid = order.client_order_id
        if not cid:
            return

        # ── emit_delay 计算（锁外，只读 ManagedOrder 的原子字段）────────
        # emit_delay = first_filled_emit_ts - exchange_ack_ts
        # 语义：CCXT 接受 submit 的回执时刻到 order_filled 事件首次 fire 的延迟。
        # 绝大多数 late-fill 应体现为 3~6s（reconcile 周期 + 5s pending_recheck）。
        emit_ts = getattr(order, "first_filled_emit_ts", None)
        ack_ts = (
            order.latency.exchange_ack_ts if getattr(order, "latency", None) else 0.0
        ) or 0.0
        emit_delay: Optional[float] = None
        if emit_ts and ack_ts and emit_ts >= ack_ts:
            emit_delay = emit_ts - ack_ts
        emit_source = getattr(order, "filled_emit_source", None)

        with self._stats_lock:
            if cid not in self._late_fill_candidates:
                # 不在候选表：
                #   - submit 时即 FILLED（走了 success 分支，不会登记）
                #   - 非 timeout 类失败（api_error / rejected 不登记）
                #   - 已过期清理
                #   - 重复回调
                return

            base, registered_debt, registered_ts = self._late_fill_candidates.pop(cid)

            # ── 保守更新 pending_hedge_debt ──────────────────────────
            # 登记时 debt=net_delta。若当前 debt 还等于登记值，说明期间没有新
            # hedge 消费它，安全 pop；否则说明已被后续 hedge 覆盖/消费，只回滚
            # 统计，避免把新的 debt 也误清掉。
            current_debt = self._pending_hedge_debt.get(base)
            if current_debt is not None and abs(current_debt - registered_debt) < 1e-9:
                self._pending_hedge_debt.pop(base, None)

            # ── 回滚统计 ──────────────────────────────────────────────
            # max(0, ...) 防御：理论上锁已防并发，但保底避免负数统计污染日志。
            self._total_failed = max(0, self._total_failed - 1)
            self._failure_reasons["timeout"] = max(
                0, self._failure_reasons.get("timeout", 0) - 1
            )
            self._total_hedged += 1
            self._late_recovered += 1

            # ── 更新 latency 记录（填充 late-fill 对应的 emit_delay / source）──
            rec = self._latency_records.get(cid)
            if rec is not None:
                rec["final_state"] = "FILLED"
                if emit_source:
                    rec["source"] = emit_source
                if emit_delay is not None:
                    rec["emit_delay"] = emit_delay

        # ── 日志打印在锁外，避免持锁阻塞 logging I/O ──────────────────
        real_avg = (
            getattr(order, "avg_fill_price", None)
            or getattr(order, "price", None)
            or 0.0
        )
        filled = getattr(order, "filled", 0.0) or 0.0
        delay = time.time() - registered_ts
        emit_delay_str = f"{emit_delay:.3f}s" if emit_delay is not None else "N/A"
        emit_source_str = emit_source or "unknown"
        self._logger.info(
            f"[HEDGE] late-fill 捞回 cid={cid}: "
            f"base={base} filled={filled:.6f} @ {real_avg:.4f}  "
            f"注册延迟={delay:.1f}s  emit_delay={emit_delay_str}  "
            f"emit_source={emit_source_str}  "
            f"(stats: recovered+1 failed-1 hedged+1)"
        )

    def _cleanup_expired_candidates_locked(self) -> None:
        """
        清理超过 TTL 的 late-fill 候选（不可能再成交的单）。

        【必须在 self._stats_lock 持锁状态下调用】方法名后缀 _locked 提示调用者。
        """
        now = time.time()
        expired = [
            cid
            for cid, (_, _, ts) in self._late_fill_candidates.items()
            if now - ts > self._late_fill_ttl
        ]
        for cid in expired:
            self._late_fill_candidates.pop(cid, None)
        if expired:
            self._logger.debug(
                f"[HEDGE] late-fill 清理 {len(expired)} 个过期候选 "
                f"(TTL={self._late_fill_ttl}s)"
            )

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

    # =========================================================================
    # Latency 诊断
    # =========================================================================

    def _log_submit_return_diag(
        self,
        seq: int,
        hedge_order: ManagedOrder,
        amount: float,
        filled: float,
        ccxt_rtt: Optional[float],
    ) -> None:
        """
        submit_order 返回后的链路诊断日志（两分支共享）。

        字段语义:
            state           - OrderManager 根据 CCXT status 归一化后的本地状态
            ccxt_rtt        - gateway.create_order 的 HTTP RTT
                              (= latency.exchange_ack_ts - latency.api_send_ts)
            filled / amount - ManagedOrder.filled / 本次 hedge 目标数量
            avg_fill_price  - ManagedOrder.avg_fill_price（None 表示 CCXT 响应
                              里没有 average 字段且 userFills 兜底未匹配）
        """
        rtt_str = f"{ccxt_rtt:.3f}s" if ccxt_rtt is not None else "N/A"
        avg = getattr(hedge_order, "avg_fill_price", None)
        avg_str = f"{avg:.4f}" if avg else "None"
        self._logger.info(
            f"[HEDGE] #{seq} submit 返回诊断: "
            f"state={hedge_order.state.value} "
            f"ccxt_rtt={rtt_str} "
            f"filled={filled:.6f} amount={amount:.6f} "
            f"avg_fill_price={avg_str}"
        )

    def _record_latency(
        self,
        cid: str,
        ccxt_rtt: Optional[float],
        emit_delay: Optional[float],
        source: Optional[str],
        final_state: str,
        ack_ts: float,
    ) -> None:
        """
        追加一笔 hedge 的延迟诊断记录（OrderedDict + cap 淘汰最旧）。

        线程安全：内部持 stats_lock，调用方不应已持锁。
        """
        with self._stats_lock:
            if (
                cid not in self._latency_records
                and len(self._latency_records) >= self._latency_records_cap
            ):
                # OrderedDict FIFO 淘汰：popitem(last=False) 弹出最早插入的
                self._latency_records.popitem(last=False)
            self._latency_records[cid] = {
                "cid": cid,
                "ccxt_rtt": ccxt_rtt,
                "emit_delay": emit_delay,
                "source": source,
                "final_state": final_state,
                "registered_ts": time.time(),
                "ack_ts": ack_ts,
            }

    @staticmethod
    def _percentiles(xs: List[float]) -> Tuple[float, float, float]:
        """
        返回 (p50, p95, max)。样本为空时返回 (0, 0, 0)。
        最近秩法（nearest-rank），对诊断场景精度足够。
        """
        if not xs:
            return (0.0, 0.0, 0.0)
        s = sorted(xs)
        n = len(s)
        p50 = s[max(0, int((n - 1) * 0.5))]
        p95 = s[max(0, int((n - 1) * 0.95))]
        return (p50, p95, s[-1])

    def latency_report(self, logger: Optional[logging.Logger] = None) -> None:
        """
        关停前打印 [HEDGE-LATENCY] 分布报告。

        输出四块：
            1. CCXT_RTT    — gateway.create_order 的 HTTP 来回延迟
            2. EMIT_DELAY  — submit ack 到 order_filled 事件首次 fire 的延迟
                             （FILLED 分支 submit_immediate=0；late-fill 为 3~6s 典型）
            3. EMIT_SOURCE — order_filled 的代码路径分布（按 filled_emit_source 计数）
            4. FINAL_STATE — hedge 订单最终状态分布
        """
        log = logger or self._logger

        with self._stats_lock:
            records = list(self._latency_records.values())

        n = len(records)
        if n == 0:
            log.info("[HEDGE-LATENCY] N=0 (无 hedge 订单记录)")
            return

        ccxt_rtts = [r["ccxt_rtt"] for r in records if r["ccxt_rtt"] is not None]
        emit_delays = [r["emit_delay"] for r in records if r["emit_delay"] is not None]

        rtt_p50, rtt_p95, rtt_max = self._percentiles(ccxt_rtts)
        emd_p50, emd_p95, emd_max = self._percentiles(emit_delays)

        # EMIT_SOURCE: None 归为 "未捞回/未emit"（超时后从未 fire order_filled）
        source_counts: Dict[str, int] = {}
        for r in records:
            src = r.get("source") or "未捞回(emit未触发)"
            source_counts[src] = source_counts.get(src, 0) + 1

        # FINAL_STATE: late-fill 捞回会把 final_state 改为 FILLED；其他保留 submit 时状态
        state_counts: Dict[str, int] = {}
        for r in records:
            st = r.get("final_state") or "unknown"
            state_counts[st] = state_counts.get(st, 0) + 1

        log.info(f"[HEDGE-LATENCY] N={n}")
        log.info(
            f"  CCXT_RTT        p50={rtt_p50:.3f}s p95={rtt_p95:.3f}s "
            f"max={rtt_max:.3f}s (samples={len(ccxt_rtts)})"
        )
        log.info(
            f"  EMIT_DELAY      p50={emd_p50:.3f}s p95={emd_p95:.3f}s "
            f"max={emd_max:.3f}s (samples={len(emit_delays)})"
        )
        src_str = "  ".join(
            f"{src}={cnt}" for src, cnt in sorted(
                source_counts.items(), key=lambda kv: -kv[1]
            )
        )
        log.info(f"  EMIT_SOURCE     {src_str}")
        state_str = "  ".join(
            f"{st}={cnt}" for st, cnt in sorted(
                state_counts.items(), key=lambda kv: -kv[1]
            )
        )
        log.info(f"  FINAL_STATE     {state_str}")
