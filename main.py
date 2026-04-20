#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delta 中性做市系统入口

MVP 职责:
  1. 初始化各模块（Gateway → OrderManager → ConnectionMonitor → RiskManager）
  2. 行情轮询：驱动 bid/ask 双边报价，mid 变动超阈值时撤旧挂新
  3. 成交回调：更新本地持仓，触发同侧补单
  4. 风控联动：下单前经 RiskManager 检查，仓位超限方向停止报价
  5. 断连保护：连接断开时撤所有单并暂停做市，重连后自动恢复
  6. Graceful Shutdown：Ctrl+C / SIGTERM → 撤所有挂单 → 打印持仓摘要 → 断连

用法:
    python main.py
    python main.py --config config/hyperliquid_config.yaml
    python main.py --symbols BTC/USDC:USDC ETH/USDC:USDC --poll-interval 1.0
    python main.py --log-level DEBUG
"""

import argparse
import io
import logging
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

import yaml

# Windows 终端 UTF-8 兼容
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

from gateways import GatewayFactory, OrderSide, OrderType, ConnectionMonitor, ReconnectPolicy
from execution import (
    OrderManager, ManagedOrder, OrderState, InventoryTracker, Quoter,
    Hedger, TradingPairConfig, InventoryBootstrap,
)
from risk import (
    RiskManager, RiskConfig, PositionLimitConfig, FatFingerConfig,
    BalanceGuardConfig, KillSwitchConfig,
)


# =============================================================================
# 配置加载
# =============================================================================

def _load_yaml(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_symbols(cfg: dict, cli_symbols: Optional[List[str]]) -> List[str]:
    if cli_symbols:
        return cli_symbols
    return cfg.get("hyperliquid", {}).get("symbols", {}).get("perpetual", [])


def _get_poll_interval(cfg: dict, cli_val: Optional[float]) -> float:
    if cli_val is not None:
        return cli_val
    ms = cfg.get("hyperliquid", {}).get("market_data", {}).get("ticker_interval", 1000)
    return ms / 1000.0


def _get_strategy_cfg(cfg: dict) -> dict:
    """
    从 yaml 的 strategy: 节读取参数，缺失时使用保守默认值。

    示例 yaml 配置（添加到 hyperliquid_config.yaml 末尾）:
        strategy:
          spread_pct: 0.001          # 单侧价差比例（0.1%）
          order_size_usd: 50.0       # 每侧名义价值（USDC）
          requote_threshold: 0.0005  # mid 变动超过此比例时重新报价（0.05%）
    """
    defaults = {
        "spread_pct": 0.001,              # 单侧价差 0.1%
        "order_size_usd": 50.0,           # 每侧 50 USDC
        "requote_threshold": 5e-4,        # 0.05% 触发重报
        "skew_intensity": 0.0,            # 库存 skew 强度（0 = 关闭）
        "spread_penalty_factor": 0.0,     # 库存价差加宽因子（0 = 不加宽）
    }
    return {**defaults, **cfg.get("strategy", {})}


def _get_risk_config(cfg: dict) -> RiskConfig:
    """
    从 yaml 的 risk: 节构造 RiskConfig，缺失时使用 dataclass 默认值。

    嵌套结构: risk.position_limit / risk.fat_finger / risk.kill_switch
    每一层都容忍完全缺失。
    """
    risk_raw = cfg.get("risk", {})

    pl_defaults = {
        "max_delta_per_symbol": 1.0,
        "max_delta_global": 10.0,
        "warn_threshold_pct": 0.8,
        "auto_hedge": True,
    }
    ff_defaults = {
        "max_deviation_pct": 0.05,
        "max_deviation_abs": None,
        "check_market_orders": False,
    }
    ks_defaults = {
        "flatten_positions": True,
        "cancel_timeout": 10.0,
        "flatten_timeout": 30.0,
    }
    bg_defaults = {
        "enabled": False,
        "check_spot": True,
        "check_perp": False,
        "fee_buffer_pct": 0.005,
    }

    return RiskConfig(
        position_limit=PositionLimitConfig(**{**pl_defaults, **risk_raw.get("position_limit", {})}),
        fat_finger=FatFingerConfig(**{**ff_defaults, **risk_raw.get("fat_finger", {})}),
        balance_guard=BalanceGuardConfig(**{**bg_defaults, **risk_raw.get("balance_guard", {})}),
        kill_switch=KillSwitchConfig(**{**ks_defaults, **risk_raw.get("kill_switch", {})}),
    )


def _get_reconnect_policy(cfg: dict) -> "ReconnectPolicy":
    """
    从 yaml 的 reconnect: 节构造 ReconnectPolicy，缺失时使用 dataclass 默认值。
    """
    defaults = {
        "max_retries": 0,
        "initial_delay": 1.0,
        "max_delay": 60.0,
        "backoff_factor": 2.0,
        "heartbeat_interval": 5.0,
        "heartbeat_timeout": 10.0,
        "cancel_on_disconnect": True,
    }
    return ReconnectPolicy(**{**defaults, **cfg.get("reconnect", {})})


def _get_om_config(cfg: dict) -> dict:
    """
    从 yaml 的 order_manager: 节读取参数，缺失时使用默认值。
    """
    defaults = {
        "reconcile_interval": 3.0,
        "stale_timeout": 5.0,
        "rate_limit_per_sec": 20,
        "rtt_warn_threshold": 2.0,
    }
    return {**defaults, **cfg.get("order_manager", {})}


def _get_bootstrap_config(cfg: dict) -> dict:
    """
    从 yaml 的 bootstrap: 节读取底仓建仓参数，缺失时整体禁用。

    与 --bootstrap-inventory CLI 标志的交互：
        - 若 yaml 里 enabled: true 则无条件启用
        - 否则靠 CLI 标志触发（在 main() 里合并）
    """
    defaults = {
        "enabled": False,
        "base_inventory_multiplier": 3.0,
        "min_spot_usdc_buffer": 50.0,
        "min_perp_usdc_buffer": 30.0,
        "skip_if_existing_pct": 0.5,
        "ioc_slippage": 0.005,
        "max_attempts": 3,
        "verify_net_delta_tolerance": 0.05,
    }
    return {**defaults, **cfg.get("bootstrap", {})}


def _get_hedger_config(cfg: dict) -> dict:
    """
    从 yaml 的 hedger: 和 trading_pairs: 节读取对冲配置，缺失时默认关闭。

    嵌套结构: hedger.enabled / hedger.hedge_slippage
    trading_pairs 为顶层列表。
    """
    hedger_raw = cfg.get("hedger", {})
    defaults = {
        "enabled": False,
        "hedge_slippage": 0.002,
    }
    hedger_cfg = {**defaults, **hedger_raw}

    pairs_raw = cfg.get("trading_pairs", [])
    hedger_cfg["trading_pairs"] = [
        TradingPairConfig(
            base=p["base"],
            market_symbol=p["market_symbol"],
            hedge_symbol=p["hedge_symbol"],
        )
        for p in pairs_raw
    ]
    return hedger_cfg


# =============================================================================
# 工具
# =============================================================================

def _compute_mid(ticker) -> Optional[float]:
    """从 Ticker 对象计算中间价"""
    if ticker.bid and ticker.ask:
        return (ticker.bid + ticker.ask) / 2.0
    return ticker.last  # 无盘口时回退到最新成交价


def setup_logging(level: str = "INFO", dry_run: bool = False) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    os.makedirs("logs", exist_ok=True)
    prefix = "dry_run" if dry_run else "live"
    filename = f"logs/{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    handlers: list = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(filename, encoding="utf-8"),
    ]

    logging.basicConfig(
        level=log_level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delta Neutral Market Maker")
    parser.add_argument("--config", default="config/hyperliquid_config.yaml",
                        help="配置文件路径")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="交易品种（覆盖 config 中的 symbols.perpetual）")
    parser.add_argument("--poll-interval", type=float, default=None,
                        help="行情轮询间隔（秒）")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="观察模式：连接交易所并打印报价，但不实际下单")
    parser.add_argument("--capital", type=float, default=10000.0,
                        help="初始资金（USDC），用于计算年化收益率")
    parser.add_argument("--preset-position", nargs="+", default=None,
                        help="预设持仓（仅 dry-run 有效），格式 SYMBOL:USD_AMOUNT，"
                             "正数=多头，负数=空头。例: USOL/USDC:5000 SOL/USDC:USDC:-5000")
    parser.add_argument("--preset-balance", nargs="+", default=None,
                        help="预设余额（仅 dry-run 有效），格式 ACCOUNT:CURRENCY:AMOUNT。"
                             "例: spot:USDC:10000 perp:USDC:500")
    parser.add_argument("--bootstrap-inventory", action="store_true", default=False,
                        help="启动时自动建立底仓 + 对冲空头（仅在 hedger.enabled=true 时有效）")
    return parser.parse_args()


# =============================================================================
# 最终摘要（shutdown 时调用）
# =============================================================================

def print_final_summary(
    inventory: InventoryTracker,
    last_mids: Dict[str, float],
    logger: logging.Logger,
    session_start: float = 0.0,
    initial_capital: float = 0.0,
) -> None:
    rpnl = inventory.realized_pnl
    upnl = inventory.unrealized_pnl(last_mids)
    total = rpnl + upnl
    lines = inventory.summary_lines(last_mids)
    sep = "=" * 55
    logger.info(sep)
    logger.info("  最终持仓 & PnL 摘要")
    logger.info(sep)
    if lines:
        for line in lines:
            logger.info(line)
    else:
        logger.info("  无持仓记录")
    logger.info(f"  已实现 PnL : {rpnl:+.4f} USDC")
    logger.info(f"  未实现 PnL : {upnl:+.4f} USDC")
    logger.info(f"  合计 PnL   : {total:+.4f} USDC")
    if session_start > 0 and initial_capital > 0:
        elapsed_h = (time.time() - session_start) / 3600
        annual_pct = (
            (total / initial_capital) * (8760 / elapsed_h) * 100
            if elapsed_h > 0 else 0.0
        )
        logger.info(f"  初始资金   : {initial_capital:.0f} USDC")
        logger.info(f"  运行时间   : {elapsed_h:.2f} 小时")
        logger.info(f"  预期年化   : {annual_pct:+.1f}%")
    logger.info(sep)


# =============================================================================
# 主入口
# =============================================================================

def main() -> None:
    args = parse_args()
    setup_logging(args.log_level, dry_run=args.dry_run)
    logger = logging.getLogger("main")
    dry_run: bool = args.dry_run

    sep = "=" * 55
    logger.info(sep)
    if dry_run:
        logger.info("  [DRY-RUN] Delta Neutral Market Maker 启动（仅观察，不下单）")
    else:
        logger.info("  Delta Neutral Market Maker  启动")
    logger.info(sep)

    # ── 安全检查：preset 参数仅限 dry-run ─────────────────────────────────────
    if not dry_run and args.preset_position:
        logger.error("--preset-position 仅在 --dry-run 模式下可用，拒绝启动")
        sys.exit(1)
    if not dry_run and args.preset_balance:
        logger.error("--preset-balance 仅在 --dry-run 模式下可用，拒绝启动")
        sys.exit(1)

    # ── 配置 ──────────────────────────────────────────────────────────────────
    cfg = _load_yaml(args.config)
    hedger_cfg: dict          = _get_hedger_config(cfg)

    # 根据 hedger 模式选择做市品种
    if hedger_cfg["enabled"]:
        trading_pairs = hedger_cfg["trading_pairs"]
        if not trading_pairs:
            logger.error("[HEDGE] hedger.enabled=true 但未配置 trading_pairs")
            sys.exit(1)
        symbols: List[str] = [pair.market_symbol for pair in trading_pairs]
    else:
        symbols: List[str] = _get_symbols(cfg, args.symbols)

    poll_interval: float      = _get_poll_interval(cfg, args.poll_interval)
    strategy_cfg: dict        = _get_strategy_cfg(cfg)
    risk_cfg: RiskConfig      = _get_risk_config(cfg)
    bootstrap_cfg: dict       = _get_bootstrap_config(cfg)
    # CLI 标志可以覆盖 yaml 的 enabled=false（但不覆盖 yaml 已开启的场景）
    if args.bootstrap_inventory:
        bootstrap_cfg = {**bootstrap_cfg, "enabled": True}
    reconnect_policy          = _get_reconnect_policy(cfg)
    om_cfg: dict              = _get_om_config(cfg)

    if not symbols:
        logger.error("未配置交易品种，请在 yaml 中添加 symbols.perpetual 或使用 --symbols")
        sys.exit(1)

    logger.info(f"交易品种 : {symbols}")
    logger.info(f"初始资金 : {args.capital:.0f} USDC")
    logger.info(f"轮询间隔 : {poll_interval}s")
    logger.info(
        f"策略参数 : 价差={strategy_cfg['spread_pct']:.3%}  "
        f"每侧={strategy_cfg['order_size_usd']} USDC  "
        f"重报阈值={strategy_cfg['requote_threshold']:.3%}"
    )
    logger.info(
        f"风控参数 : 单品种Delta上限={risk_cfg.position_limit.max_delta_per_symbol}  "
        f"全局Delta上限={risk_cfg.position_limit.max_delta_global}  "
        f"价格保护={risk_cfg.fat_finger.max_deviation_pct:.1%}"
    )
    logger.info(
        f"连接监控 : 心跳={reconnect_policy.heartbeat_interval}s  "
        f"退避={reconnect_policy.initial_delay}s→{reconnect_policy.max_delay}s  "
        f"断连撤单={reconnect_policy.cancel_on_disconnect}"
    )
    logger.info(
        f"订单管理 : 对账={om_cfg['reconcile_interval']}s  "
        f"限速={om_cfg['rate_limit_per_sec']}/s  "
        f"STALE超时={om_cfg['stale_timeout']}s"
    )
    if hedger_cfg["enabled"]:
        logger.info(
            f"对冲模式 : 已启用  滑点={hedger_cfg['hedge_slippage']:.3%}"
        )

    # =========================================================================
    # 1. 模块初始化
    # =========================================================================
    logger.info("── 初始化 Gateway ──")
    gateway = GatewayFactory.create(args.config)
    if not gateway.connect():
        logger.error("网关连接失败，退出")
        sys.exit(1)
    logger.info(f"Gateway 已连接: {gateway.exchange_name} ({gateway.status.value})")

    # ── Hedger: 验证 trading_pairs 中的 symbol 存在于交易所 ──
    if hedger_cfg["enabled"]:
        markets = gateway.get_markets()
        for pair in hedger_cfg["trading_pairs"]:
            for sym in (pair.market_symbol, pair.hedge_symbol):
                if sym not in markets:
                    logger.error(
                        f"[HEDGE] symbol {sym} 不存在于交易所市场列表中，退出"
                    )
                    gateway.disconnect()
                    sys.exit(1)

    logger.info("── 初始化 OrderManager ──")
    om = OrderManager(
        gateway,
        reconcile_interval=om_cfg["reconcile_interval"],
        stale_timeout=om_cfg["stale_timeout"],
        rate_limit_per_sec=om_cfg["rate_limit_per_sec"],
        rtt_warn_threshold=om_cfg["rtt_warn_threshold"],
    )
    if not dry_run:
        om.start()
    else:
        logger.info("  [DRY-RUN] 跳过 OM reconcile 线程（无订单需对账）")

    logger.info("── 初始化 ConnectionMonitor ──")
    monitor = ConnectionMonitor(gateway, om, policy=reconnect_policy)

    # =========================================================================
    # 2. 共享状态（闭包变量，由回调和主循环共同读写）
    # =========================================================================
    inventory = InventoryTracker()
    quoter = Quoter(
        strategy_cfg=strategy_cfg,
        inventory=inventory,
        max_delta_per_symbol=risk_cfg.position_limit.max_delta_per_symbol,
    )

    logger.info("── 初始化 RiskManager ──")
    rm = RiskManager(
        gateway=gateway,
        order_manager=om,
        connection_monitor=monitor,
        config=risk_cfg,
        inventory=inventory,
    )

    # ── Hedger 初始化 ────────────────────────────────────────────────────
    hedger: Optional[Hedger] = None
    if hedger_cfg["enabled"]:
        hedger = Hedger(
            gateway=gateway,
            order_manager=om,
            risk_manager=rm,
            inventory=inventory,
            trading_pairs=hedger_cfg["trading_pairs"],
            hedge_slippage=hedger_cfg["hedge_slippage"],
            dry_run=dry_run,
        )
        for pair in hedger_cfg["trading_pairs"]:
            logger.info(
                f"[HEDGE] 双腿模式: {pair.market_symbol} (做市) "
                f"↔ {pair.hedge_symbol} (对冲)"
            )

    # ── 启动时同步交易所持仓，消除重启后 delta 盲区 ─────────────────────
    try:
        if hedger is not None:
            sync_symbols = symbols + [
                p.hedge_symbol for p in hedger_cfg["trading_pairs"]
            ]
            positions = gateway.fetch_positions(sync_symbols)
        else:
            positions = gateway.fetch_positions(symbols)
        n_synced = inventory.sync_from_positions(positions)
        logger.info(f"初始持仓同步完成: {n_synced} 个品种")
    except Exception as e:
        logger.warning(f"初始持仓同步失败（将从零开始跟踪）: {e}")

    # ── [DRY-RUN] 预设持仓注入 ───────────────────────────────────────────
    if dry_run and args.preset_position:
        # 构建合法 symbol 白名单
        valid_symbols = set(symbols)
        if hedger_cfg["enabled"]:
            for pair in hedger_cfg["trading_pairs"]:
                valid_symbols.add(pair.hedge_symbol)

        for spec in args.preset_position:
            # 从最后一个冒号分割：左边 symbol，右边 USD 金额
            idx = spec.rfind(":")
            if idx <= 0:
                logger.error(f"[DRY-RUN] 预设持仓格式错误（需要 SYMBOL:USD_AMOUNT）: {spec}")
                sys.exit(1)
            sym, usd_str = spec[:idx], spec[idx + 1:]
            try:
                usd_amount = float(usd_str)
            except ValueError:
                logger.error(f"[DRY-RUN] 预设持仓金额无效: {spec}")
                sys.exit(1)
            if sym not in valid_symbols:
                logger.error(
                    f"[DRY-RUN] 预设持仓 symbol {sym} 不在做市品种列表中 "
                    f"(合法: {sorted(valid_symbols)})"
                )
                sys.exit(1)
            if abs(usd_amount) < 0.01:
                continue

            # 拉取 mid 换算 size
            ticker = gateway.fetch_ticker(sym)
            mid = (ticker.bid + ticker.ask) / 2.0 if ticker.bid and ticker.ask else ticker.last
            if not mid or mid <= 0:
                logger.error(f"[DRY-RUN] 无法获取 {sym} mid-price，跳过预设")
                continue
            size = abs(usd_amount) / mid
            side = "buy" if usd_amount > 0 else "sell"
            inventory.on_fill(sym, side, size, mid)
            logger.info(
                f"[DRY-RUN] 预设持仓: {sym} {'+' if usd_amount > 0 else ''}"
                f"{usd_amount:.0f} USDC @ mid={mid:.4f} -> delta={inventory.get_position(sym):+.6f}"
            )

    # ── [DRY-RUN] 预设余额注入 ───────────────────────────────────────────
    if dry_run and args.preset_balance:
        for spec in args.preset_balance:
            parts = spec.split(":", 2)
            if len(parts) != 3:
                logger.error(f"[DRY-RUN] 预设余额格式错误（需要 ACCOUNT:CURRENCY:AMOUNT）: {spec}")
                sys.exit(1)
            account, currency, amt_str = parts
            if account not in ("spot", "perp"):
                logger.error(f"[DRY-RUN] 预设余额 account 必须是 spot 或 perp，收到: {account}")
                sys.exit(1)
            try:
                amount = float(amt_str)
            except ValueError:
                logger.error(f"[DRY-RUN] 预设余额金额无效: {spec}")
                sys.exit(1)
            inventory.set_balance(account, currency, amount)
            logger.info(f"[DRY-RUN] 预设余额: {account}:{currency} = {amount:.2f}")

    # ── 同步持仓到风控 ───────────────────────────────────────────────────
    rm.sync_positions(inventory.get_all_positions())
    if dry_run and (args.preset_position or args.preset_balance):
        logger.info(f"[DRY-RUN] 最终持仓: {inventory.get_all_positions()}")
        logger.info(f"[DRY-RUN] 最终余额: {inventory.get_all_balances()}")

    # symbol → {"bid": cid | None, "ask": cid | None}
    active_quotes: Dict[str, Dict[str, Optional[str]]] = {
        s: {"bid": None, "ask": None} for s in symbols
    }

    # 上次触发报价时的 mid（用于重报阈值判断）
    last_quoted_mid: Dict[str, float] = {}

    # 最新 mid（供最终摘要使用）
    last_mids: Dict[str, float] = {}

    # 成交后需补单的品种集合（由回调线程写，主循环读）
    needs_requote: Dict[str, bool] = {s: False for s in symbols}

    # 主循环退出信号
    shutdown_event = threading.Event()

    # 网关连接状态（断连清空，重连后设置）
    is_connected = threading.Event()
    is_connected.set()

    # 成交触发补单的即时唤醒事件
    requote_event = threading.Event()

    # ── P&L 追踪 ─────────────────────────────────────────────────────
    session_start = time.time()
    initial_capital = args.capital
    pnl_log_interval = 60.0               # P&L 日志间隔（秒）
    last_pnl_log = session_start
    counters = {"fills": 0, "volume_usdc": 0.0}  # 成交计数器

    # =========================================================================
    # 3. 内部操作函数
    # =========================================================================

    def _cancel_side(symbol: str, side: str) -> None:
        """撤销单侧报价"""
        cid = active_quotes[symbol].get(side)
        if cid:
            try:
                om.cancel_order(cid)
            except Exception as e:
                logger.warning(f"撤单失败 [{symbol} {side} {cid}]: {e}")
            active_quotes[symbol][side] = None

    def _cancel_symbol_quotes(symbol: str) -> None:
        """撤销某品种的所有活跃报价"""
        _cancel_side(symbol, "bid")
        _cancel_side(symbol, "ask")

    def _cancel_all_quotes() -> None:
        """撤销所有品种的活跃报价"""
        for s in symbols:
            _cancel_symbol_quotes(s)

    def _place_symbol_quotes(symbol, mid):
        """
        撤旧挂新：在 bid/ask 两侧各挂一张限价单。

        流程:
            撤旧单 → quoter.compute() → 风控检查 → 下单
        dry_run 时:
            quoter.compute() → 精度规范化 → 风控检查 → 打印日志（不下单）

        Returns:
            (bid_price, ask_price, size) 精度规范化后的值，
            或 None（规范化失败 / size<=0 时）。
        """
        if not dry_run:
            # 撤旧单，避免双边重叠持仓
            _cancel_symbol_quotes(symbol)

        # 计算报价
        bid_price, ask_price, size = quoter.compute(symbol, mid)

        # 交易所精度规范化
        try:
            bid_price = float(gateway.price_to_precision(symbol, bid_price))
            ask_price = float(gateway.price_to_precision(symbol, ask_price))
            size      = float(gateway.amount_to_precision(symbol, size))
        except Exception as e:
            logger.warning(f"精度规范化失败 [{symbol}]: {e}")
            return None

        if size <= 0:
            return None

        # ── 买单：仓位超多头限制时被 RiskManager 拒绝，只报卖单 ──────────────
        bid_ok = rm.pre_trade_check(symbol, "buy", size, price=bid_price, mid_price=mid)
        if dry_run:
            status = "passed" if bid_ok.passed else f"rejected: {bid_ok.reason}"
            logger.info(f"[DRY-RUN] [{symbol}] 买单 {size} @ {bid_price} (风控: {status})")
        elif bid_ok.passed:
            try:
                order = om.submit_order(
                    symbol, OrderSide.BUY, OrderType.LIMIT, size, bid_price,
                    strategy_trigger_ts=time.time(),
                )
                if not order.state.is_terminal:
                    active_quotes[symbol]["bid"] = order.client_order_id
                    logger.info(f"[{symbol}] 挂买单  {size} @ {bid_price}")
            except Exception as e:
                logger.error(f"买单失败 [{symbol}]: {e}")
        else:
            logger.info(f"[{symbol}] 买单被风控拒绝: {bid_ok.reason}")

        # ── 卖单：仓位超空头限制时被 RiskManager 拒绝，只报买单 ──────────────
        ask_ok = rm.pre_trade_check(symbol, "sell", size, price=ask_price, mid_price=mid)
        if dry_run:
            status = "passed" if ask_ok.passed else f"rejected: {ask_ok.reason}"
            logger.info(f"[DRY-RUN] [{symbol}] 卖单 {size} @ {ask_price} (风控: {status})")
        elif ask_ok.passed:
            try:
                order = om.submit_order(
                    symbol, OrderSide.SELL, OrderType.LIMIT, size, ask_price,
                    strategy_trigger_ts=time.time(),
                )
                if not order.state.is_terminal:
                    active_quotes[symbol]["ask"] = order.client_order_id
                    logger.info(f"[{symbol}] 挂卖单  {size} @ {ask_price}")
            except Exception as e:
                logger.error(f"卖单失败 [{symbol}]: {e}")
        else:
            logger.info(f"[{symbol}] 卖单被风控拒绝: {ask_ok.reason}")

        last_quoted_mid[symbol] = mid
        return bid_price, ask_price, size

    # =========================================================================
    # 4. 事件回调
    # =========================================================================

    def on_fill(managed: ManagedOrder) -> None:
        """
        成交回调：更新持仓 → 通知 RiskManager → 标记补单 → 触发对冲。

        注意: 本函数可能在对账线程（OM-Reconcile）中被调用。
              做市腿成交时：轻量状态更新 + 触发 Hedger 对冲（阻塞）。
              对冲腿成交时：仅更新持仓/风控，不触发补单和对冲。
        """
        amount = managed.filled or managed.amount
        if amount <= 0:
            return

        price  = managed.price or 0.0
        signed = amount if managed.side == "buy" else -amount

        # 更新本地持仓
        inventory.on_fill(managed.symbol, managed.side, amount, price)

        # 同步通知 RiskManager 更新 Delta（用于仓位限制检查）
        rm.update_position(managed.symbol, signed)

        # 成交计数
        counters["fills"] += 1
        counters["volume_usdc"] += amount * price

        logger.info(
            f"成交: {managed.symbol} {managed.side.upper()}  "
            f"{amount:.6f} @ {price}  "
            f"持仓={inventory.get_position(managed.symbol):+.6f}"
        )

        # 清除已成交侧的报价 CID + 标记补单（仅做市腿品种）
        if managed.symbol in active_quotes:
            side_key = "bid" if managed.side == "buy" else "ask"
            active_quotes[managed.symbol][side_key] = None

            # 标记需补单，唤醒主循环（而非直接在此处下单，避免线程争用）
            needs_requote[managed.symbol] = True
            requote_event.set()

        # 对冲触发（做市腿成交 → 对冲腿下反向单）
        if hedger is not None:
            hedger.on_market_fill(managed)

    def on_state_change(managed: ManagedOrder) -> None:
        """
        订单状态变更通知。

        TODO: 部分成交时接入 execution.quoter 的单侧库存调整逻辑：
              当前仅记录日志，等待完全成交后由 on_fill 统一处理。
        """
        if managed.state == OrderState.PARTIALLY_FILLED:
            logger.info(
                f"部分成交: {managed.symbol} {managed.side}  "
                f"{managed.filled:.6f}/{managed.amount:.6f} @ {managed.price}"
            )

    def on_disconnect(error: Exception) -> None:
        """
        断连回调：撤所有报价，暂停做市直到重连。
        """
        logger.warning(f"网关断连: {error} — 暂停做市，等待自动重连")
        is_connected.clear()
        # 尽力撤单（网络断连时可能失败，ConnectionMonitor 内部还有兜底）
        try:
            _cancel_all_quotes()
        except Exception:
            pass
        # 重置报价状态（断连后本地与交易所状态不一致，全部清空）
        for s in symbols:
            active_quotes[s] = {"bid": None, "ask": None}

    def on_reconnected() -> None:
        """
        重连回调：重置报价状态，触发全量重新报价。
        """
        logger.info("网关重连成功，恢复做市")
        is_connected.set()
        for s in symbols:
            active_quotes[s] = {"bid": None, "ask": None}
            last_quoted_mid.pop(s, None)
            needs_requote[s] = True
        requote_event.set()

    def on_kill_complete(_) -> None:
        """KillSwitch 完成后通知主循环退出"""
        shutdown_event.set()

    # 注册所有回调
    om.on("order_filled",      on_fill)
    om.on("order_state_change", on_state_change)
    monitor.on("on_disconnect",  on_disconnect)
    monitor.on("on_reconnected", on_reconnected)
    rm.on("on_kill_complete",    on_kill_complete)

    # =========================================================================
    # 5. 启动 Monitor 和 RiskManager
    # =========================================================================
    monitor.start()

    # ── 底仓自动建仓（在 rm.arm 之前：KillSwitch 尚未接管 SIGINT，便于失败时清理） ──
    if bootstrap_cfg["enabled"]:
        if not hedger_cfg["enabled"]:
            logger.error("[BOOTSTRAP] 需要 hedger.enabled=true 才能建仓（缺少 hedge_symbol）")
            sys.exit(1)

        # 暂停 Hedger，防止建仓单触发重复对冲
        if hedger is not None:
            hedger.pause()
        try:
            bootstrap = InventoryBootstrap(
                gateway=gateway,
                order_manager=om,
                inventory=inventory,
                trading_pairs=hedger_cfg["trading_pairs"],
                strategy_cfg=strategy_cfg,
                bootstrap_cfg=bootstrap_cfg,
                dry_run=dry_run,
            )
            ok = bootstrap.run()
        finally:
            if hedger is not None:
                hedger.resume()

        if not ok:
            logger.error("[BOOTSTRAP] 建仓失败，退出")
            sys.exit(1)
        logger.info("[BOOTSTRAP] 全部 trading_pair 建仓完成")

        # 同步建仓产生的持仓到 RiskManager 的 PositionLimiter
        if not dry_run:
            rm.sync_positions(inventory.get_all_positions())

    rm.arm()

    # =========================================================================
    # 6. 行情轮询 + 做市主循环（后台线程）
    # =========================================================================

    def market_loop() -> None:
        """
        行情轮询与做市驱动循环。

        每轮:
          1. 等待网关连接可用（断连时阻塞）
          2. 逐品种拉取 ticker，更新 mid-price
          3. 对 mid 变动超阈值或成交触发的品种执行报价刷新
          4. 打印行情摘要
          5. 精确等待剩余间隔（或被成交事件提前唤醒）
        """
        logger.info(f"行情轮询启动: {symbols}  interval={poll_interval}s")

        while not shutdown_event.is_set():
            # 断连时等待重连（每 1s 检查一次 shutdown_event）
            if not is_connected.wait(timeout=1.0):
                continue

            loop_start = time.time()

            for symbol in symbols:
                if shutdown_event.is_set() or not is_connected.is_set():
                    break

                # ── 拉取行情 ────────────────────────────────────────────────
                try:
                    ticker = gateway.fetch_ticker(symbol)
                    mid = _compute_mid(ticker)
                    if not mid:
                        continue
                    rm.update_mid_price(symbol, mid)  # 供 FatFingerGuard 使用
                    last_mids[symbol] = mid
                except Exception as e:
                    logger.warning(f"行情获取失败 [{symbol}]: {e}")
                    continue

                # ── 判断是否需要重新报价 ─────────────────────────────────────
                triggered    = needs_requote.pop(symbol, False)  # 成交补单触发
                mid_drifted  = quoter.should_requote(
                    symbol, last_quoted_mid.get(symbol, 0.0), mid
                )

                quote_result = None
                if triggered or mid_drifted:
                    quote_result = _place_symbol_quotes(symbol, mid)

                    # TODO: strategy.on_tick(symbol, mid, inventory) 调用点
                    #       未来在此接入 strategy.delta_neutral_mm 的决策逻辑

                # ── dry-run 每轮每品种汇总 ──────────────────────────────────
                if dry_run and mid:
                    if quote_result is not None:
                        bid, ask, sz = quote_result
                    else:
                        bid, ask, sz = quoter.compute(symbol, mid)
                    info = quoter.skew_info(symbol, mid)
                    short_sym = symbol.split("/")[0]
                    logger.info(
                        f"[DRY-RUN] {short_sym} mid={mid:.2f} "
                        f"bid={bid:.2f} ask={ask:.2f} size={sz:.5f} "
                        f"delta={info['delta']:+.4f} skew_offset={info['skew_offset']:.2f}"
                    )

            # ── 打印行情摘要 ─────────────────────────────────────────────────
            if last_mids and not dry_run:
                summary = "  ".join(
                    f"{s.split('/')[0]}={v:.2f}" for s, v in last_mids.items()
                )
                logger.info(f"[行情] {summary}")

            # ── 定期 P&L 日志 ────────────────────────────────────────────
            nonlocal last_pnl_log
            now = time.time()
            if last_mids and now - last_pnl_log >= pnl_log_interval:
                rpnl = inventory.realized_pnl
                upnl = inventory.unrealized_pnl(last_mids)
                total_pnl = rpnl + upnl
                elapsed_h = (now - session_start) / 3600
                annual_pct = (
                    (total_pnl / initial_capital) * (8760 / elapsed_h) * 100
                    if elapsed_h > 0 and initial_capital > 0 else 0.0
                )
                pos_str = "  ".join(
                    f"{s.split('/')[0]}={inventory.get_position(s):+.4f}"
                    for s in symbols
                )
                logger.info(
                    f"[PnL] 已实现={rpnl:+.4f}  未实现={upnl:+.4f}  "
                    f"合计={total_pnl:+.4f} USDC  "
                    f"年化={annual_pct:+.1f}%  "
                    f"成交={counters['fills']}笔  "
                    f"量={counters['volume_usdc']:.0f}USDC  "
                    f"运行={elapsed_h:.2f}h"
                )
                if pos_str:
                    logger.info(f"[PnL] 持仓: {pos_str}")
                logger.info(
                    f"[PnL] 策略: spread={strategy_cfg['spread_pct']:.3%}  "
                    f"size={strategy_cfg['order_size_usd']:.0f}USDC  "
                    f"requote={strategy_cfg['requote_threshold']:.3%}  "
                    f"skew={strategy_cfg['skew_intensity']}  "
                    f"delta_max={risk_cfg.position_limit.max_delta_per_symbol}"
                )
                last_pnl_log = now

            # ── 精确睡眠（扣除本轮耗时），或被成交事件提前唤醒 ──────────────
            elapsed = time.time() - loop_start
            requote_event.wait(timeout=max(0.0, poll_interval - elapsed))
            requote_event.clear()

        logger.info("行情轮询已停止")

    poll_thread = threading.Thread(
        target=market_loop, name="MarketLoop", daemon=True
    )
    poll_thread.start()

    logger.info(sep)
    logger.info("  系统就绪 (Ctrl+C 退出)")
    logger.info(sep)

    # =========================================================================
    # 7. 主线程：等待退出信号
    # =========================================================================
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1.0)

    except KeyboardInterrupt:
        # SIGINT 通常已被 KillSwitch 捕获；偶发情况下主线程先感知到，补充触发
        if not rm.kill_switch.is_triggered:
            logger.info("收到 Ctrl+C，开始 graceful shutdown")
            shutdown_event.set()

    # =========================================================================
    # 8. Graceful Shutdown
    # =========================================================================
    finally:
        logger.info(sep)
        logger.info("  Graceful Shutdown")
        logger.info(sep)

        if not rm.kill_switch.is_triggered:
            # ── 正常退出路径：手动执行完整关停序列 ─────────────────────────

            # Step 1: 停止主循环（已通过 shutdown_event 完成）

            if dry_run:
                # dry-run 无实际挂单，跳过撤单步骤
                logger.info("步骤1/3: [DRY-RUN] 跳过撤单（无实际挂单）")
            else:
                # Step 2: 撤销所有挂单（等待交易所 ACK，om.cancel_order 为同步调用）
                logger.info("步骤1/3: 撤销所有挂单...")
                try:
                    n_om = om.cancel_all()
                    logger.info(f"  OrderManager 撤掉 {n_om} 个挂单")
                except Exception as e:
                    logger.error(f"  OM 撤单失败: {e}")
                # Gateway 层兜底（防止 OM 漏掉非本进程创建的挂单）
                try:
                    n_gw = gateway.cancel_all_orders()
                    if n_gw:
                        logger.info(f"  Gateway 兜底撤掉 {n_gw} 个挂单")
                except Exception as e:
                    logger.error(f"  Gateway 兜底撤单失败（网络可能已中断）: {e}")

            # Step 3: 停止各组件
            logger.info("步骤2/3: 停止各组件")
            monitor.stop()
            if not dry_run:
                om.stop()

        else:
            # KillSwitch 已执行撤单和组件停止，跳过重复操作
            logger.info("KillSwitch 已完成撤单和组件停止")

        rm.disarm()

        # 等待轮询线程退出
        poll_thread.join(timeout=poll_interval + 2)

        # Step 最后：打印持仓摘要
        logger.info("步骤3/3: 最终持仓摘要")
        logger.info(
            f"  成交统计: {counters['fills']} 笔, "
            f"成交量={counters['volume_usdc']:.2f} USDC"
        )
        print_final_summary(
            inventory, last_mids, logger,
            session_start=session_start,
            initial_capital=initial_capital,
        )

        # ── Hedger 统计 ─────────────────────────────────────────────────
        if hedger is not None:
            stats = hedger.stats
            logger.info(
                f"[HEDGE] 对冲统计: 成功={stats['total_hedged']}  "
                f"失败={stats['total_failed']}  "
                f"未对冲残量={stats['pending_hedge_debt']}"
            )
            asset_map = hedger.build_asset_map()
            for pair in hedger_cfg["trading_pairs"]:
                net_delta = inventory.get_net_delta(pair.base, asset_map)
                logger.info(f"[HEDGE] {pair.base} 净Delta={net_delta:+.6f}")

        gateway.disconnect()
        logger.info("系统已关停")


if __name__ == "__main__":
    main()
