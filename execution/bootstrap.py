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
# 从 5s 上调到 15s：OM reconcile 周期 = 3s + trade_history 路径延迟 3-5s，
# 原 5s 在慢成交时撞墙（B2 bug）。15s 足够 OM 跑 5 轮对账。
_FILL_WAIT_TIMEOUT_S = 15.0
# 从 1s 下调到 0.5s：主要靠 order.state（OM 已在后台更新），本地轮询便宜，
# 缩短确认延迟；fetch_balance/positions 兜底路径也跟随这个节奏。
_FILL_POLL_INTERVAL_S = 0.5

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

        # inventory 基准快照（在任何下单动作之前取），供后续 _wait_inventory_sync
        # 判定 delta 用。修复 B2 假警报：on_fill 回调可能在 _confirm_fill_via_balance
        # 返回前就同步累加完毕，方法内自取 baseline 会拿到累加后值、导致必定超时。
        # 必须在下单前取;skip 分支里取了也无害(当场 return,值不会被使用)。
        inventory_baseline_spot = self._inventory.get_position(market_sym)
        inventory_baseline_perp = self._inventory.get_position(hedge_sym)

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

        # ── 5. Dry-run 路径：打印预演 + 基于 inventory 的余额拦截 ────────────
        if self._dry_run:
            self._print_dry_run_preview(
                pair, target_qty, market_mid, hedge_mid, slippage,
                need_spot, need_perp, avail_usdc,
            )
            # 修复 B1：dry-run 原逻辑直接 return True，preset 不足时也不拦截，
            # 用户会误判"余额够建仓"。现改为：基于 inventory 可用余额判断，
            # 不足则打印错误并返回 False（上抛给 main.py 的 run()），后者会
            # sys.exit(1)。
            if avail_usdc is not None and avail_usdc < need_spot + need_perp:
                self._logger.error(
                    f"[DRY-RUN][BOOTSTRAP] {pair.base} 余额不足，跳过建仓: "
                    f"可用 {avail_usdc:.2f} USDC，"
                    f"需要 {need_spot + need_perp:.2f} "
                    f"(现货 {need_spot:.2f} + 永续保证金 {need_perp:.2f})，"
                    f"缺 {need_spot + need_perp - avail_usdc:.2f}"
                )
                return False
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

        # ── 8. 等待 OM 事件链驱动 inventory 同步 ──────────────────────────────
        # 历史修复：之前这里手动 inventory.on_fill(...)，在 B2 timeout 修复后
        # 出现双重计入 bug —— OM reconcile 通过 trade_history 发现成交并 emit
        # order_filled 事件 → main.py on_fill 回调 → inventory 已更新；此时
        # bootstrap 再手动 on_fill 会造成第二次累加，净 delta 翻倍误报。
        # 改为：让 OM reconcile 的 order_filled 事件统一驱动 main.py 的 on_fill
        # 回调，bootstrap 在此等 inventory 追上即可，避免双重数据源。
        #
        # 为什么需要显式等待：_confirm_fill_via_balance 返回时 order.state 已是
        # FILLED，但 main.py on_fill 回调可能还在 OM reconcile 线程的事件分发
        # 队列里排队执行，inventory 未必立刻反映变化。
        self._wait_inventory_sync(
            market_sym, side="buy",
            expected_delta=actual_spot_filled,
            baseline=inventory_baseline_spot,
        )
        self._wait_inventory_sync(
            hedge_sym, side="sell",
            expected_delta=perp_filled,
            baseline=inventory_baseline_perp,
        )

        # ── 9. 验证净 delta ──────────────────────────────────────────────────
        net_delta = self._compute_net_delta(pair, market_sym, hedge_sym)
        tol = float(self._cfg["verify_net_delta_tolerance"])

        if abs(net_delta) > tol:
            # inventory 报出异常 delta。在抛 critical 前走一次交易所真实查询
            # 兜底，避免被 inventory 同步延迟（或 OM 回调丢失）误判。
            self._logger.warning(
                f"[BOOTSTRAP] {pair.base} inventory 净 delta 异常 "
                f"({net_delta:+.6f} > tol {tol})，查交易所真实持仓二次验证..."
            )
            exchange_delta = self._compute_net_delta_from_exchange(
                pair, market_sym, hedge_sym,
            )
            if exchange_delta is not None:
                # 交易所 spot 读的是总余额（含启动前残留 existing_spot），
                # perp 读的是总空头（含启动前 existing_short）。
                # 本次 bootstrap 引入的净 delta = 交易所 - 启动前基准
                #   spot 增量 = exchange_spot - existing_spot（多头 +）
                #   perp 增量 = exchange_short - existing_short（空头 -）
                #   adjusted = spot 增量 - perp 增量
                #            = (exchange_spot - exchange_short) - (existing_spot - existing_short)
                #            = exchange_delta - existing_spot + existing_short
                adjusted_exchange_delta = exchange_delta - existing_spot + existing_short
                self._logger.warning(
                    f"[BOOTSTRAP] 交易所兜底: exchange_delta={exchange_delta:+.6f}  "
                    f"existing_spot={existing_spot}  existing_short={existing_short}  "
                    f"adjusted={adjusted_exchange_delta:+.6f}"
                )
                if abs(adjusted_exchange_delta) <= tol:
                    self._logger.info(
                        f"[BOOTSTRAP] {pair.base} 交易所真实 delta 在容差内 "
                        f"({adjusted_exchange_delta:+.6f} <= {tol})，放行。"
                        f"inventory 可能有同步延迟或重复计入，不影响账户安全。"
                    )
                    net_delta = adjusted_exchange_delta  # 用真实值走后续 info 日志
                else:
                    inv_delta_final = self._inventory.get_net_delta(
                        pair.base,
                        {market_sym: pair.base, hedge_sym: pair.base},
                    )
                    self._logger.critical(
                        f"[BOOTSTRAP] {pair.base} 交易所 delta 也超容差: "
                        f"exchange={adjusted_exchange_delta:+.6f}  tol={tol}  "
                        f"spot_filled={actual_spot_filled}  perp_filled={perp_filled}  "
                        f"inventory_delta={inv_delta_final:+.6f}  "
                        f"请人工核对持仓。"
                    )
                    sys.exit(1)
            else:
                # 兜底查询也失败，保守 exit
                self._logger.critical(
                    f"[BOOTSTRAP] {pair.base} inventory delta 异常且兜底查询失败: "
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

    def _get_available_balance(self, account: str, currency: str) -> float:
        """
        获取指定账户 / 币种的可用余额。

        数据源选择（修复 B1 dry-run preset 被 bypass 的 bug）：
          - dry-run 模式 + inventory.balances[account] 已注入（preset-balance 经
            main.py 的 inventory.set_balance(...) 写入）：返回 inventory 里的值。
          - 其他情况（实盘 / dry-run 但未 preset 该账户）：走 gateway.fetch_balance
            的粗检路径，保持原行为（Hyperliquid 的 CCXT fetch_balance 不区分
            spot/perp 子账户，统一返回 USDC.free）。

        失败时返回 0.0（gateway 异常视为"余额为零"），而非 None。调用方如需
        区分"拿不到数据 vs 真的为零"，需上层额外判断。
        """
        if self._dry_run and account in self._inventory.balances:
            val = float(self._inventory.balances[account].get(currency, 0.0))
            self._logger.debug(
                f"[BOOTSTRAP] dry-run 使用 inventory 余额: "
                f"{account}:{currency}={val:.6f}"
            )
            return val
        # 实盘 / dry-run 未 preset：走原 gateway 路径
        try:
            bal = self._gateway.fetch_balance()
        except Exception as e:
            self._logger.warning(
                f"[BOOTSTRAP] fetch_balance 失败（视为 {account}:{currency}=0）: {e}"
            )
            return 0.0
        b = bal.balances.get(currency)
        return float(b.free or 0.0) if b is not None else 0.0

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

        数据源选择（与 _read_available_usdc 对称，修复 B1 dry-run bug）：
          - dry-run 模式 + inventory 已有 preset 数据（spot 余额或 hedge 持仓）：
            全部从 inventory 读取。spot 用 balances["spot"][base]；perp 空头用
            positions[hedge_sym] 的绝对值（负数=空头），避免 dry-run 走 gateway
            拿到真实账户状态污染预演。
          - 实盘 / dry-run 但未 preset：走原 gateway 路径，spot 用 fetch_balance，
            perp 用 fetch_positions。

        Returns:
            (existing_spot_free, existing_perp_short_size)  — 都是 >= 0 的数量
        """
        # dry-run + preset 注入过：用 inventory 数据源
        if self._dry_run and (
            "spot" in self._inventory.balances
            or self._inventory.positions.get(hedge_sym, 0.0) != 0.0
        ):
            spot_bal = self._inventory.balances.get("spot", {}).get(base_asset_spot, 0.0)
            existing_spot = max(float(spot_bal), 0.0)
            perp_pos = float(self._inventory.positions.get(hedge_sym, 0.0))
            existing_short = abs(perp_pos) if perp_pos < 0 else 0.0
            self._logger.debug(
                f"[BOOTSTRAP] dry-run 使用 inventory 底仓: "
                f"spot[{base_asset_spot}]={existing_spot}  "
                f"perp_short({hedge_sym})={existing_short}"
            )
            return existing_spot, existing_short

        # 实盘 / dry-run 未 preset：原 gateway 路径
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
        读取可用 USDC 余额（用于建仓前的余额充足性检查）。

        数据源：
          - dry-run 模式 + 任一子账户有 preset-balance 注入：
            返回 spot:USDC + perp:USDC 的合计（经 _get_available_balance）。
            修复 B1 bug —— 原实现在 dry-run + preset 下直接查 gateway 真实账户，
            导致预设值被忽略、虚拟建仓按真实账户余额通过。
          - 实盘 / dry-run 未 preset：走原 gateway.fetch_balance 路径，取 USDC.free
            作为综合可用估算（Hyperliquid 在 CCXT 层 spot/perp 可能聚合，
            实盘路径保持既有"粗检"行为）。失败返回 None → 上层跳过余额检查。
        """
        if self._dry_run and (
            "spot" in self._inventory.balances
            or "perp" in self._inventory.balances
        ):
            spot_usdc = self._get_available_balance("spot", "USDC")
            perp_usdc = self._get_available_balance("perp", "USDC")
            total = spot_usdc + perp_usdc
            self._logger.debug(
                f"[BOOTSTRAP] dry-run 使用 inventory USDC 合计: "
                f"spot={spot_usdc:.4f} + perp={perp_usdc:.4f} = {total:.4f}"
            )
            return total

        # 实盘 / dry-run 未 preset：原粗检路径
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
                    source="bootstrap",
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

        数据源优先级（每轮循环都查）：
          1. order.state == FILLED  ← OM 后台对账已更新（主路径）
             返回 order.filled（OM 从 userFills/trade_history 填入的真实值）
          2. fetch_balance 差值兜底（应对 OM 对账延迟的极端情况）

        修复 B2 bug：原实现只在 submit 瞬间查一次 order.state，之后完全靠
        fetch_balance 轮询；gateway balance 刷新延迟可导致 OM 已确认成交但
        bootstrap 超时失败（裸 delta 敞口）。

        Returns:
            实际成交数量；超时未成交返回 None
        """
        threshold = target_qty * _FILL_RATIO_THRESHOLD

        # 立刻检查（submit 返回就 FILLED 的场景）
        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            self._logger.debug(
                f"[BOOTSTRAP] {base_asset} 订单 submit 即 FILLED: "
                f"filled={order.filled}"
            )
            return float(order.filled)

        deadline = time.time() + _FILL_WAIT_TIMEOUT_S
        while time.time() < deadline:
            time.sleep(_FILL_POLL_INTERVAL_S)

            # ── 主路径：OM 的订单状态 ─────────────────────────────
            # OM reconcile 线程会从 userFills/trade_history 把 state 推到 FILLED
            # 并把 order.filled 更新为真实成交量
            if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
                self._logger.info(
                    f"[BOOTSTRAP] 通过 OM state 确认现货成交: "
                    f"{base_asset} filled={order.filled} (via order.state)"
                )
                return float(order.filled)

            # ── 兜底：fetch_balance 差值 ──────────────────────────
            # 如果 OM 因为某种原因漏掉这笔 fill（理论上不该），balance 变化能救一次
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
                    f"[BOOTSTRAP] 通过余额差值确认现货成交(兜底路径): "
                    f"{base_asset} baseline={baseline} now={free_now} delta={delta}"
                )
                return delta

        # 最后一次检查 order.state（避免 sleep 和 deadline 之间的竞态）
        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            self._logger.info(
                f"[BOOTSTRAP] 最终兜底 OM state 检查命中: "
                f"{base_asset} filled={order.filled}"
            )
            return float(order.filled)

        # 诊断日志：超时时打印最终 state + balance，方便排障
        try:
            bal = self._gateway.fetch_balance()
            b = bal.balances.get(base_asset)
            free_final = float(b.free or 0.0) if b is not None else 0.0
        except Exception:
            free_final = None
        self._logger.error(
            f"[BOOTSTRAP] {base_asset} 成交确认超时 ({_FILL_WAIT_TIMEOUT_S}s): "
            f"order.state={order.state}  order.filled={order.filled}  "
            f"balance_final={free_final}  baseline={baseline}  threshold={threshold}"
        )
        return None

    def _confirm_perp_fill(
        self,
        hedge_sym: str,
        baseline_short: float,
        target_qty: float,
        order: "ManagedOrder",
    ) -> Optional[float]:
        """
        确认永续空头已成交。与 _confirm_fill_via_balance 对称：
          1. 主路径：order.state == FILLED（OM 后台对账结果）
          2. 兜底：fetch_positions 查空头持仓差值

        修复 B2 bug（对称于现货腿）：原实现超出 submit 的 snapshot 后就不再看
        order.state，只靠 fetch_positions 轮询，在交易所持仓传播慢时可能误判超时。
        """
        threshold = target_qty * _FILL_RATIO_THRESHOLD

        # 立刻检查（submit 返回就 FILLED 的场景）
        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            self._logger.debug(
                f"[BOOTSTRAP] {hedge_sym} 订单 submit 即 FILLED: "
                f"filled={order.filled}"
            )
            return float(order.filled)

        deadline = time.time() + _FILL_WAIT_TIMEOUT_S
        while time.time() < deadline:
            time.sleep(_FILL_POLL_INTERVAL_S)

            # ── 主路径：OM state ──────────────────────────────────
            if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
                self._logger.info(
                    f"[BOOTSTRAP] 通过 OM state 确认永续空头成交: "
                    f"{hedge_sym} filled={order.filled} (via order.state)"
                )
                return float(order.filled)

            # ── 兜底：fetch_positions ─────────────────────────────
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
                    f"[BOOTSTRAP] 通过持仓差值确认永续空头成交(兜底路径): "
                    f"{hedge_sym} baseline_short={baseline_short} now={short_now} delta={delta}"
                )
                return delta

        # 最后一次 order.state 检查（避免竞态）
        if order.state == OrderState.FILLED and order.filled and order.filled >= threshold:
            self._logger.info(
                f"[BOOTSTRAP] 最终兜底 OM state 检查命中: "
                f"{hedge_sym} filled={order.filled}"
            )
            return float(order.filled)

        # 诊断日志
        try:
            positions = self._gateway.fetch_positions([hedge_sym])
            short_final = 0.0
            for p in positions:
                if p.symbol == hedge_sym and p.side == "short":
                    short_final = float(p.size or 0.0)
                    break
        except Exception:
            short_final = None
        self._logger.error(
            f"[BOOTSTRAP] {hedge_sym} 永续成交确认超时 ({_FILL_WAIT_TIMEOUT_S}s): "
            f"order.state={order.state}  order.filled={order.filled}  "
            f"position_final={short_final}  baseline={baseline_short}  threshold={threshold}"
        )
        return None

    def _wait_inventory_sync(
        self,
        symbol: str,
        side: str,
        expected_delta: float,
        baseline: Optional[float] = None,
        timeout_s: float = 5.0,
        poll_interval_s: float = 0.2,
    ) -> bool:
        """
        等 inventory 通过 OM 事件链被 main.py on_fill 更新到位。

        数据路径：OM reconcile 检测 fill → emit "order_filled" 事件 →
        main.py on_fill 回调 → inventory.on_fill()。这里只负责等 inventory
        追上 caller 给定的基准（或方法开始时自取的基准），不自己写入，避免
        重复计入（B2 timeout 修复后暴露的 bug）。

        参数：
          baseline:  可选，"调用建仓前的 inventory 快照"。
                     如果 on_fill 可能在调用本方法前就已累加（B2 第三次实盘
                     暴露的假警报：_confirm_fill_via_balance 等到 order.state
                     == FILLED 时，OM reconcile 线程已同步派发 order_filled
                     事件并让 main.py on_fill 把累加跑完），caller 必须在下单
                     前就取 snapshot 并传入,否则方法内自取的 baseline 就等于
                     累加后值，delta 永远 0，必定超时。
                     None 时退化到方法开始时自取（向后兼容；不推荐）。

        判定逻辑：
          - buy  侧：current - baseline >= expected × 0.95
          - sell 侧：baseline - current >= expected × 0.95
            （perp 开空：position 从 baseline（如 0）减到更小（如 -0.26），
              baseline - current = 0 - (-0.26) = 0.26）

        超时返回 False 但不 raise —— 让 caller 继续走净 delta 校验（含交易所
        兜底路径），inventory 漂移时仍能安全放行或 critical 退出。

        Returns:
            True  = inventory 已同步到 baseline + expected_delta
            False = 超时未追上
        """
        threshold = expected_delta * 0.95
        if baseline is None:
            # 向后兼容：方法开始时自取（可能撞到时序 bug，不推荐）
            baseline = self._inventory.get_position(symbol)
            self._logger.debug(
                f"[BOOTSTRAP] _wait_inventory_sync 未传 baseline，"
                f"方法内自取 {symbol}={baseline}（注意时序风险）"
            )

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            current = self._inventory.get_position(symbol)
            if side == "buy":
                delta = current - baseline
            else:  # sell
                delta = baseline - current

            if delta >= threshold:
                self._logger.debug(
                    f"[BOOTSTRAP] inventory 已同步 {symbol} {side}: "
                    f"baseline={baseline} current={current} delta={delta}"
                )
                return True
            time.sleep(poll_interval_s)

        # 超时：打诊断日志但不 raise
        final = self._inventory.get_position(symbol)
        self._logger.warning(
            f"[BOOTSTRAP] inventory 同步超时 {symbol} {side}: "
            f"baseline={baseline} final={final} expected_delta={expected_delta} "
            f"threshold={threshold} timeout={timeout_s}s "
            f"(OM 事件链可能延迟或丢失回调，后续净 delta 校验会检出)"
        )
        return False

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
        通过 InventoryTracker.get_net_delta 计算 pair 的净 Delta（主路径）。

        现货腿（如 USOL/USDC）和永续腿（如 SOL/USDC:USDC）在 inventory 里是
        两个独立 symbol，但经济上同属 pair.base。用 asset_map 统一映射到 base
        后聚合，与 main.py 关停摘要里的 hedger.build_asset_map 路径保持一致。

        容差判断留给 caller（_bootstrap_one_pair 内已有 verify_net_delta_tolerance
        检查）。如果 caller 检出异常值，可改调 _compute_net_delta_from_exchange
        做交易所真实查询二次验证。
        """
        asset_map = {market_sym: pair.base, hedge_sym: pair.base}
        return self._inventory.get_net_delta(pair.base, asset_map)

    def _compute_net_delta_from_exchange(
        self,
        pair: "TradingPairConfig",
        market_sym: str,
        hedge_sym: str,
    ) -> Optional[float]:
        """
        直接从交易所真实持仓计算净 delta（用于 inventory 异常时的二次验证）。

        现货：fetch_balance 读 base coin 余额
        永续：fetch_positions 读 short 持仓
        返回 = spot_qty - perp_short

        ⚠️ 读的是当前**总余额 / 总持仓**，包含启动前已有的残留（existing_spot /
        existing_short）。调用方用它做 sanity check 时要自行扣除启动前基准，
        得到"本次 bootstrap 引入的净 delta"。

        Returns:
            spot_qty - perp_short（未扣除启动前基准）；任一腿查询失败返回 None
        """
        try:
            bal = self._gateway.fetch_balance()
            base_asset = market_sym.split("/")[0]
            b = bal.balances.get(base_asset)
            spot_qty = float(b.free or 0.0) if b is not None else 0.0
        except Exception as e:
            self._logger.warning(f"[BOOTSTRAP] 兜底查 spot 余额失败: {e}")
            return None

        try:
            positions = self._gateway.fetch_positions([hedge_sym])
            perp_short = 0.0
            for p in positions:
                if p.symbol == hedge_sym and p.side == "short":
                    perp_short = float(p.size or 0.0)
                    break
        except Exception as e:
            self._logger.warning(f"[BOOTSTRAP] 兜底查 perp 持仓失败: {e}")
            return None

        return spot_qty - perp_short
