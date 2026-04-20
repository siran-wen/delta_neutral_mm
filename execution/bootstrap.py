#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
底仓自动建仓 (Inventory Bootstrap)

现货做市的卖单必须持有 base asset 才能挂上。账户从零启动时，
sell 单会被交易所拒绝（Insufficient spot balance），只能挂 buy 单，
无法形成完整的双边做市。

本模块在启动阶段一次性建立"USDC + base asset 混合底仓 + 等量永续空头对冲"
的初始结构，建仓完成后净 Delta ≈ 0，可以开始正常做市。

两腿顺序（必须原子化）：
    1. 现货激进限价买入（模拟市价）→ 立刻得到 base asset
    2. 永续激进限价卖出（模拟市价）→ 对冲价格风险
    3. 任何一步失败 → 回滚或明确报错，不留裸敞口

用法:
    from execution.bootstrap import InventoryBootstrap

    bootstrap = InventoryBootstrap(
        gateway=gw, order_manager=om, inventory=inv,
        trading_pairs=pairs, strategy_cfg=strat, bootstrap_cfg=boot,
    )
    ok = bootstrap.run()
"""

import logging
import sys
import time
from typing import TYPE_CHECKING, List, Optional

from execution.order_manager import OrderState, RequestPriority
from gateways import OrderSide, OrderType

if TYPE_CHECKING:
    from execution.hedger import TradingPairConfig
    from execution.inventory import InventoryTracker
    from execution.order_manager import ManagedOrder, OrderManager
    from gateways import BaseGateway


# 永续杠杆保守估算（用于保证金需求计算）
# 按任务要求：暂用 1.0，避免依赖 yaml 配置错误
_CONSERVATIVE_LEVERAGE = 1.0

# 等待成交的轮询配置
_FILL_WAIT_TIMEOUT_S = 5.0
_FILL_POLL_INTERVAL_S = 1.0

# 成交比例阈值：实际成交量必须 ≥ target × 此比例才视为成功
_FILL_RATIO_THRESHOLD = 0.95


class InventoryBootstrap:
    """
    启动时自动建立 USDC + base asset 混合底仓 + 永续空头对冲。

    用于现货做市的前置条件 —— sell 侧必须持有 base asset 才能挂上。
    建仓完成后净 delta 应该接近零。

    构造参数：
        gateway:       交易所网关（用于行情、余额、回滚下单）
        order_manager: OM 实例（建仓订单通过 OM 提交，复用限速 + 风控链路）
        inventory:     持仓跟踪器（建仓后手动写入，避免依赖 on_fill 回调时序）
        trading_pairs: 要建仓的 (market_symbol, hedge_symbol, base) 列表
        strategy_cfg:  策略配置，读取 order_size_usd
        bootstrap_cfg: bootstrap 节配置（见 main._get_bootstrap_config）
        dry_run:       True 时只打印预演不下单
        logger:        可选 logger
    """

    def __init__(
        self,
        gateway: "BaseGateway",
        order_manager: "OrderManager",
        inventory: "InventoryTracker",
        trading_pairs: List["TradingPairConfig"],
        strategy_cfg: dict,
        bootstrap_cfg: dict,
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self._gateway = gateway
        self._om = order_manager
        self._inventory = inventory
        self._trading_pairs = trading_pairs
        self._strategy_cfg = strategy_cfg
        self._cfg = bootstrap_cfg
        self._dry_run = dry_run
        self._logger = logger or logging.getLogger("bootstrap")

    # =========================================================================
    # 公开接口
    # =========================================================================

    def run(self) -> bool:
        """
        对每个 trading_pair 顺序执行建仓。

        Returns:
            True  : 全部 pair 建仓成功（或已跳过）
            False : 任一 pair 失败（已回滚或明确报错后退出）
        """
        order_size_usd = float(self._strategy_cfg.get("order_size_usd", 0) or 0)
        if order_size_usd <= 0:
            self._logger.error("[BOOTSTRAP] strategy.order_size_usd 必须为正，终止建仓")
            return False

        if not self._trading_pairs:
            self._logger.warning("[BOOTSTRAP] trading_pairs 为空，无需建仓")
            return True

        self._logger.info(
            f"[BOOTSTRAP] 开始建仓：{len(self._trading_pairs)} 个 pair  "
            f"order_size_usd={order_size_usd}  "
            f"multiplier={self._cfg['base_inventory_multiplier']}  "
            f"dry_run={self._dry_run}"
        )
        # 已知限制（不拦截启动，但务必告知用户）：
        # 1. Hyperliquid 的 Spot 和 Perp 是独立子账户，USDC 不共享。
        #    fetch_balance() 经 CCXT 后可能聚合为一个 AccountBalance，无法区分
        #    spot.USDC / perp.USDC。因此本模块的余额充足性检查是对"总量"的粗检，
        #    不保证两个子账户分别够用 —— 请在启动前手动确认：
        #      Spot 子账户 ≥ Σ(target_qty × market_mid × (1+slippage)) + min_spot_usdc_buffer
        #      Perp 子账户 ≥ Σ(target_qty × hedge_mid × (1+slippage)) + min_perp_usdc_buffer
        # 2. "已跳过" 仅当"现货 ≥ threshold AND 永续空头 ≥ threshold" 时成立。
        #    如果上次建仓失败、回滚不完整，或用户手工调整过账户导致半成品状态
        #    （例：只有现货没有空头），本模块会再次下两腿单，叠加到已有仓位之上。
        #    请在这种场景下手动清理后再重启。
        self._logger.warning(
            "[BOOTSTRAP] 已知限制: "
            "(1) Spot/Perp 子账户 USDC 不共享，余额检查基于聚合总量，"
            "请自行确保两个子账户分别充足; "
            "(2) 若账户处于'建仓半成品'状态（只有现货或只有空头），"
            "请先手动清理，否则会叠加下单导致敞口放大"
        )

        for pair in self._trading_pairs:
            try:
                ok = self._bootstrap_one_pair(pair, order_size_usd)
            except SystemExit:
                # 内部已决定以 sys.exit 退出（余额不足、验证失败等）
                raise
            except Exception as e:
                self._logger.error(
                    f"[BOOTSTRAP] {pair.base} 建仓异常: {e}", exc_info=True
                )
                return False
            if not ok:
                return False

        self._logger.info(f"[BOOTSTRAP] 全部 {len(self._trading_pairs)} 个 pair 建仓完成")
        return True

    # =========================================================================
    # 单个 pair 建仓
    # =========================================================================

    def _bootstrap_one_pair(self, pair: "TradingPairConfig", order_size_usd: float) -> bool:
        """单个 pair 的建仓流程。失败时返回 False（dry-run）或 sys.exit（实盘）。"""
        market_sym = pair.market_symbol
        hedge_sym = pair.hedge_symbol
        base_asset_spot = market_sym.split("/")[0]   # 现货底仓的 base（如 "USOL"）
        base_asset_perp = hedge_sym.split("/")[0]    # 永续对冲的 base（如 "SOL"）

        # ── 1. 拉取两条腿的 mid ──────────────────────────────────────────────
        market_mid = self._fetch_mid(market_sym)
        hedge_mid = self._fetch_mid(hedge_sym)
        if market_mid is None or hedge_mid is None:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 无法获取行情: "
                f"{market_sym} mid={market_mid}  {hedge_sym} mid={hedge_mid}"
            )
            return False

        # ── 2. 计算目标底仓数量 ──────────────────────────────────────────────
        raw_qty = order_size_usd * float(self._cfg["base_inventory_multiplier"]) / market_mid
        try:
            target_qty = float(self._gateway.amount_to_precision(market_sym, raw_qty))
        except Exception as e:
            self._logger.error(f"[BOOTSTRAP] 数量精度化失败 [{market_sym}]: {e}")
            return False

        if target_qty <= 0:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 目标数量为 0（order_size_usd 太小？）: "
                f"raw={raw_qty}"
            )
            return False

        self._logger.info(
            f"[BOOTSTRAP] {pair.base} 目标底仓: {target_qty} {base_asset_spot}  "
            f"market_mid={market_mid}  hedge_mid={hedge_mid}"
        )

        # ── 3. 检查现有底仓 ──────────────────────────────────────────────────
        existing_spot, existing_short = self._read_existing_inventory(
            base_asset_spot, hedge_sym
        )
        skip_pct = float(self._cfg["skip_if_existing_pct"])
        skip_threshold = target_qty * skip_pct

        if existing_spot >= skip_threshold and existing_short >= skip_threshold:
            self._logger.info(
                f"[BOOTSTRAP] {pair.base} 已有底仓充足，跳过建仓: "
                f"spot={existing_spot} {base_asset_spot}  "
                f"perp_short={existing_short} {base_asset_perp}  "
                f"(阈值 {skip_threshold:.6f})"
            )
            return True

        # ── 4. 余额充足性检查 ────────────────────────────────────────────────
        slippage = float(self._cfg["ioc_slippage"])
        need_spot = target_qty * market_mid * (1 + slippage) + float(self._cfg["min_spot_usdc_buffer"])
        need_perp = (
            target_qty * hedge_mid * (1 + slippage) / _CONSERVATIVE_LEVERAGE
            + float(self._cfg["min_perp_usdc_buffer"])
        )
        avail_usdc = self._read_available_usdc()

        # ── 5. Dry-run 路径：只预演不下单 ────────────────────────────────────
        if self._dry_run:
            self._print_dry_run_preview(
                pair, target_qty, market_mid, hedge_mid, slippage,
                need_spot, need_perp, avail_usdc,
            )
            return True

        # 实盘：余额不足直接 sys.exit（建仓是启动阶段，不允许带病运行）
        if avail_usdc is not None and avail_usdc < need_spot + need_perp:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} USDC 余额不足: "
                f"可用 {avail_usdc:.2f}，"
                f"需要 {need_spot + need_perp:.2f} "
                f"(现货 {need_spot:.2f} + 永续保证金 {need_perp:.2f})，"
                f"缺 {need_spot + need_perp - avail_usdc:.2f}"
            )
            sys.exit(1)

        # ── 6. 现货市价买入（激进 IOC 限价） ─────────────────────────────────
        spot_buy_price_raw = market_mid * (1 + slippage)
        try:
            spot_buy_price = float(self._gateway.price_to_precision(market_sym, spot_buy_price_raw))
        except Exception as e:
            self._logger.error(f"[BOOTSTRAP] {market_sym} 价格精度化失败: {e}")
            return False

        spot_order = self._submit_with_retry(
            symbol=market_sym, side=OrderSide.BUY,
            amount=target_qty, price=spot_buy_price,
            label=f"{pair.base} 现货买入",
        )
        if spot_order is None:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 现货买入失败（已重试 {self._cfg['max_attempts']} 次），"
                f"未下永续单，账户状态未变更。退出。"
            )
            sys.exit(1)

        # 等待现货成交确认：或从 managed 状态立即判定，或从余额差值轮询
        actual_spot_filled = self._confirm_fill_via_balance(
            base_asset_spot, existing_spot, target_qty, spot_order,
        )
        if actual_spot_filled is None:
            # 现货未成交，尝试撤单然后退出
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 现货买单超时未成交，尝试撤单后退出"
            )
            self._safe_cancel(spot_order)
            sys.exit(1)

        self._logger.info(
            f"[BOOTSTRAP] {pair.base} 现货买入成功: "
            f"{actual_spot_filled} {base_asset_spot} @ {spot_buy_price}"
        )

        # ── 7. 永续空头开仓（立刻，时间窗口越短越好） ────────────────────────
        perp_sell_price_raw = hedge_mid * (1 - slippage)
        try:
            perp_sell_price = float(self._gateway.price_to_precision(hedge_sym, perp_sell_price_raw))
            perp_qty = float(self._gateway.amount_to_precision(hedge_sym, actual_spot_filled))
        except Exception as e:
            self._logger.error(
                f"[BOOTSTRAP] {hedge_sym} 精度化失败: {e} —— 立刻回滚现货"
            )
            self._rollback_spot(market_sym, actual_spot_filled, market_mid, slippage, pair.base)
            sys.exit(1)

        perp_order = self._submit_with_retry(
            symbol=hedge_sym, side=OrderSide.SELL,
            amount=perp_qty, price=perp_sell_price,
            label=f"{pair.base} 永续卖出",
        )
        if perp_order is None:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 永续开空失败 —— 立刻回滚现货以消除裸多头敞口"
            )
            self._rollback_spot(market_sym, actual_spot_filled, market_mid, slippage, pair.base)
            sys.exit(1)

        # 永续腿的成交确认：直接看 managed（订单状态）或短轮询 fetch_positions
        perp_filled = self._confirm_perp_fill(hedge_sym, existing_short, perp_qty, perp_order)
        if perp_filled is None:
            self._logger.error(
                f"[BOOTSTRAP] {pair.base} 永续空头超时未成交，尝试撤单 + 回滚现货"
            )
            self._safe_cancel(perp_order)
            self._rollback_spot(market_sym, actual_spot_filled, market_mid, slippage, pair.base)
            sys.exit(1)

        # ── 8. 更新本地状态（inventory.on_fill） ────────────────────────────
        # 注：OM 订单在 submit_order 返回时若已 FILLED，不会触发 on_fill 回调
        #    （order_filled 事件只在 reconcile 检测到交易所侧消失时发出），
        #    因此这里手动写入不会与 main.py 的 on_fill 重复。
        self._inventory.on_fill(market_sym, "buy", actual_spot_filled, spot_buy_price)
        self._inventory.on_fill(hedge_sym, "sell", perp_filled, perp_sell_price)

        # ── 9. 验证净 delta ──────────────────────────────────────────────────
        net_delta = self._compute_net_delta(pair, market_sym, hedge_sym)
        tol = float(self._cfg["verify_net_delta_tolerance"])
        if abs(net_delta) > tol:
            self._logger.critical(
                f"[BOOTSTRAP] {pair.base} 建仓后净 Delta 超过容差: "
                f"net_delta={net_delta:+.6f}  tol={tol}  "
                f"spot_filled={actual_spot_filled}  perp_filled={perp_filled}  "
                f"请人工核对持仓。"
            )
            sys.exit(1)

        self._logger.info(
            f"[BOOTSTRAP] {pair.base} 建仓完成: "
            f"现货+{actual_spot_filled} 永续-{perp_filled} 净delta={net_delta:+.6f}"
        )
        return True

    # =========================================================================
    # 行情 / 余额 / 持仓读取
    # =========================================================================

    def _fetch_mid(self, symbol: str) -> Optional[float]:
        """拉取 symbol 中间价，失败返回 None"""
        try:
            ticker = self._gateway.fetch_ticker(symbol)
        except Exception as e:
            self._logger.error(f"[BOOTSTRAP] fetch_ticker({symbol}) 失败: {e}")
            return None
        if ticker.bid and ticker.ask:
            return (ticker.bid + ticker.ask) / 2.0
        if ticker.last:
            return float(ticker.last)
        return None

    def _read_existing_inventory(self, base_asset_spot: str, hedge_sym: str) -> tuple:
        """
        读取现有现货底仓（base_asset_spot 的 free）和永续空头数量。

        Returns:
            (existing_spot_free, existing_perp_short_size)  — 都是 >= 0 的数量
        """
        existing_spot = 0.0
        try:
            bal = self._gateway.fetch_balance()
            b = bal.balances.get(base_asset_spot)
            if b is not None:
                existing_spot = float(b.free or 0.0)
        except Exception as e:
            self._logger.warning(f"[BOOTSTRAP] fetch_balance 失败（视为余额 0）: {e}")

        existing_short = 0.0
        try:
            positions = self._gateway.fetch_positions([hedge_sym])
            for p in positions:
                if p.symbol == hedge_sym and p.side == "short":
                    existing_short = float(p.size or 0.0)
                    break
        except Exception as e:
            self._logger.warning(
                f"[BOOTSTRAP] fetch_positions({hedge_sym}) 失败（视为空头 0）: {e}"
            )

        return existing_spot, existing_short

    def _read_available_usdc(self) -> Optional[float]:
        """
        读取可用 USDC 余额。
        Hyperliquid 在 CCXT 层 spot / perp 账户余额可能聚合在同一 AccountBalance 里，
        这里取 USDC.free 作为综合可用估算。
        """
        try:
            bal = self._gateway.fetch_balance()
        except Exception as e:
            self._logger.warning(
                f"[BOOTSTRAP] fetch_balance 失败（跳过余额检查）: {e}"
            )
            return None
        b = bal.balances.get("USDC")
        return float(b.free or 0.0) if b is not None else 0.0

    # =========================================================================
    # 下单 / 撤单 / 成交确认
    # =========================================================================

    def _submit_with_retry(
        self,
        symbol: str,
        side: OrderSide,
        amount: float,
        price: float,
        label: str,
    ) -> Optional["ManagedOrder"]:
        """
        通过 OM 下单，失败时重试最多 max_attempts 次。
        返回值为 None 表示所有重试都失败，或订单处于非可接受的终态。
        """
        max_attempts = int(self._cfg["max_attempts"])
        last_reason = "unknown"

        for attempt in range(1, max_attempts + 1):
            try:
                order = self._om.submit_order(
                    symbol=symbol, side=side, order_type=OrderType.LIMIT,
                    amount=amount, price=price,
                    strategy_trigger_ts=time.time(),
                    priority=RequestPriority.BOOTSTRAP,
                )
            except Exception as e:
                last_reason = f"exception: {e}"
                self._logger.warning(
                    f"[BOOTSTRAP] {label} 第 {attempt}/{max_attempts} 次下单异常: {e}"
                )
                if attempt < max_attempts:
                    time.sleep(1.0)
                continue

            # 可接受的终态：FILLED（立刻成交），或 OPEN/PARTIALLY_FILLED（继续等成交）
            if order.state in (OrderState.FILLED, OrderState.OPEN, OrderState.PARTIALLY_FILLED):
                return order

            # REJECTED / STALE / 其他异常终态 → 重试
            last_reason = f"state={order.state.value} reason={order.reject_reason}"
            self._logger.warning(
                f"[BOOTSTRAP] {label} 第 {attempt}/{max_attempts} 次: "
                f"state={order.state.value} reject={order.reject_reason}"
            )
            if attempt < max_attempts:
                time.sleep(1.0)

        self._logger.error(f"[BOOTSTRAP] {label} 全部重试失败，最后原因: {last_reason}")
        return None

    def _confirm_fill_via_balance(
        self,
        base_asset: str,
        baseline: float,
        target_qty: float,
        order: "ManagedOrder",
    ) -> Optional[float]:
        """
        确认现货买入已成交。

        优先路径：若 submit_order 返回时 order.state 已是 FILLED，
                 直接使用 order.filled 作为实际成交量。
        降级路径：轮询 fetch_balance，直到 base_asset.free 比 baseline 增加 ≥ target × 0.95
                 或超时。

        Returns:
            实际成交数量；超时未成交返回 None
        """
        threshold = target_qty * _FILL_RATIO_THRESHOLD

        # 优先：订单已立刻 FILLED
        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            return float(order.filled)

        # 降级：轮询余额
        deadline = time.time() + _FILL_WAIT_TIMEOUT_S
        while time.time() < deadline:
            time.sleep(_FILL_POLL_INTERVAL_S)
            try:
                bal = self._gateway.fetch_balance()
                b = bal.balances.get(base_asset)
                free_now = float(b.free or 0.0) if b is not None else 0.0
            except Exception as e:
                self._logger.warning(f"[BOOTSTRAP] 等待成交时 fetch_balance 失败: {e}")
                continue
            delta = free_now - baseline
            if delta >= threshold:
                self._logger.info(
                    f"[BOOTSTRAP] 通过余额差值确认现货成交: "
                    f"{base_asset} baseline={baseline} now={free_now} delta={delta}"
                )
                return delta
        return None

    def _confirm_perp_fill(
        self,
        hedge_sym: str,
        baseline_short: float,
        target_qty: float,
        order: "ManagedOrder",
    ) -> Optional[float]:
        """
        确认永续空头已成交。与 _confirm_fill_via_balance 对称，但用 fetch_positions。
        """
        threshold = target_qty * _FILL_RATIO_THRESHOLD

        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            return float(order.filled)

        deadline = time.time() + _FILL_WAIT_TIMEOUT_S
        while time.time() < deadline:
            time.sleep(_FILL_POLL_INTERVAL_S)
            try:
                positions = self._gateway.fetch_positions([hedge_sym])
                short_now = 0.0
                for p in positions:
                    if p.symbol == hedge_sym and p.side == "short":
                        short_now = float(p.size or 0.0)
                        break
            except Exception as e:
                self._logger.warning(f"[BOOTSTRAP] 等待永续成交时 fetch_positions 失败: {e}")
                continue
            delta = short_now - baseline_short
            if delta >= threshold:
                self._logger.info(
                    f"[BOOTSTRAP] 通过持仓差值确认永续空头成交: "
                    f"{hedge_sym} baseline_short={baseline_short} now={short_now} delta={delta}"
                )
                return delta
        return None

    def _safe_cancel(self, order: "ManagedOrder") -> None:
        """
        撤单，异常静默。

        直接走 gateway.cancel_order(eid, symbol)，与 _rollback_spot 的"绕过 OM"保持一致。
        若订单已无 exchange_order_id（极少数），回退到 om.cancel_order。
        """
        try:
            eid = order.exchange_order_id
            if eid:
                self._gateway.cancel_order(eid, order.symbol)
            else:
                self._om.cancel_order(order.client_order_id)
        except Exception as e:
            self._logger.warning(
                f"[BOOTSTRAP] 撤单异常（可能已终态）cid={order.client_order_id}: {e}"
            )

    # =========================================================================
    # 回滚
    # =========================================================================

    def _rollback_spot(
        self,
        market_sym: str,
        amount: float,
        market_mid: float,
        slippage: float,
        base: str,
    ) -> None:
        """
        立刻把已买入的现货卖回去（激进 IOC 限价）。
        回滚绕过 OM，直接走 gateway（避免 OM 的限速队列和重新提交时的竞争）。
        """
        if amount <= 0:
            return
        sell_price_raw = market_mid * (1 - slippage)
        self._logger.warning(
            f"[BOOTSTRAP] {base} 执行现货回滚: SELL {amount} @ ~{sell_price_raw}"
        )
        try:
            sell_price = float(self._gateway.price_to_precision(market_sym, sell_price_raw))
            sell_amount = float(self._gateway.amount_to_precision(market_sym, amount))
            self._gateway.create_order(
                symbol=market_sym,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                amount=sell_amount,
                price=sell_price,
            )
            self._logger.warning(
                f"[BOOTSTRAP] {base} 现货回滚成功: 已 SELL {sell_amount} @ {sell_price}，"
                f"建仓失败但账户恢复初始状态"
            )
        except Exception as e:
            self._logger.critical(
                f"[BOOTSTRAP] {base} 现货回滚失败: {e} —— "
                f"账户遗留 {amount} {market_sym.split('/')[0]} 未对冲的多头，"
                f"请立刻手动清理（市价卖出 {amount} {market_sym}）"
            )

    # =========================================================================
    # Dry-run 预演
    # =========================================================================

    def _print_dry_run_preview(
        self,
        pair: "TradingPairConfig",
        target_qty: float,
        market_mid: float,
        hedge_mid: float,
        slippage: float,
        need_spot: float,
        need_perp: float,
        avail_usdc: Optional[float],
    ) -> None:
        """打印"如果实盘会执行的操作"；不修改任何状态"""
        spot_buy_price = market_mid * (1 + slippage)
        perp_sell_price = hedge_mid * (1 - slippage)
        base_spot = pair.market_symbol.split("/")[0]
        base_perp = pair.hedge_symbol.split("/")[0]

        self._logger.info(f"[DRY-RUN][BOOTSTRAP] {pair.base} 建仓预演:")
        self._logger.info(
            f"  现货: 买入 {target_qty} {base_spot} @ {spot_buy_price:.4f} "
            f"(mid {market_mid:.4f} × {1 + slippage}), 需 USDC ≈ {target_qty * spot_buy_price:.2f}"
        )
        self._logger.info(
            f"  永续: 卖出 {target_qty} {base_perp} @ {perp_sell_price:.4f} "
            f"(mid {hedge_mid:.4f} × {1 - slippage}), "
            f"需保证金 ≈ {target_qty * perp_sell_price / _CONSERVATIVE_LEVERAGE:.2f}"
        )
        avail_str = f"{avail_usdc:.2f}" if avail_usdc is not None else "未知"
        self._logger.info(
            f"  合计需 Spot USDC: {need_spot:.2f}  "
            f"Perp USDC: {need_perp:.2f}  "
            f"当前可用 USDC: {avail_str}"
        )

    # =========================================================================
    # 验证
    # =========================================================================

    def _compute_net_delta(
        self,
        pair: "TradingPairConfig",
        market_sym: str,
        hedge_sym: str,
    ) -> float:
        """
        通过 InventoryTracker.get_net_delta 计算 pair 的净 Delta。

        现货腿（如 USOL/USDC）和永续腿（如 SOL/USDC:USDC）在 inventory 里是
        两个独立 symbol，但经济上同属 pair.base。用 asset_map 统一映射到 base
        后聚合，与 main.py 关停摘要里的 hedger.build_asset_map 路径保持一致。
        """
        asset_map = {market_sym: pair.base, hedge_sym: pair.base}
        return self._inventory.get_net_delta(pair.base, asset_map)
