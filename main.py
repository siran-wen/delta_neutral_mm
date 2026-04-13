#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delta 中性做市系统入口

启动顺序:
    Gateway → OrderManager → ConnectionMonitor → RiskManager → 行情轮询主循环

关停顺序 (graceful shutdown):
    撤所有挂单 → rm.disarm() → monitor.stop() → om.stop() → gateway.disconnect()

用法:
    python main.py
    python main.py --config config/hyperliquid_config.yaml --symbols BTC/USDC:USDC ETH/USDC:USDC
    python main.py --poll-interval 2.0 --log-level DEBUG
"""

import argparse
import io
import logging
import sys
import threading
import time
from typing import List, Optional

import yaml

# Windows 终端 UTF-8 兼容
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

from gateways.gateway import GatewayFactory
from gateways.exception_handler import ConnectionMonitor, ReconnectPolicy
from execution.order_manager import OrderManager
from risk.pre_trade import RiskManager, RiskConfig


# =============================================================================
# 初始化工具
# =============================================================================

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delta Neutral Market Maker")
    parser.add_argument(
        "--config",
        default="config/hyperliquid_config.yaml",
        help="配置文件路径 (默认: config/hyperliquid_config.yaml)",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="交易品种列表，不填则从 config 读取 symbols.perpetual",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=None,
        help="行情轮询间隔（秒），不填则从 config 读取 market_data.ticker_interval(ms)/1000",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def load_symbols_from_config(config_path: str) -> List[str]:
    """从 YAML 读取 symbols.perpetual 列表"""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("hyperliquid", {}).get("symbols", {}).get("perpetual", [])


def load_poll_interval_from_config(config_path: str) -> float:
    """从 YAML 读取 market_data.ticker_interval (ms)，转换为秒"""
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    ms = cfg.get("hyperliquid", {}).get("market_data", {}).get("ticker_interval", 1000)
    return ms / 1000.0


# =============================================================================
# 回调：成交 → 持仓更新
# =============================================================================

def make_fill_handler(rm: RiskManager, logger: logging.Logger):
    """
    订单成交时更新 RiskManager 的持仓 Delta。

    ManagedOrder.side = "buy" / "sell"
    ManagedOrder.filled = 本次已成交数量（累计，非增量）
    delta_change: 买入为正，卖出为负
    """
    last_filled: dict = {}  # client_order_id -> 上次记录的 filled 量

    def on_fill(managed):
        cid = managed.client_order_id
        prev = last_filled.get(cid, 0.0)
        increment = managed.filled - prev          # 本次新增成交量
        if increment <= 0:
            return
        last_filled[cid] = managed.filled

        signed = increment if managed.side == "buy" else -increment
        rm.update_position(managed.symbol, signed)
        logger.info(
            f"成交回调: {managed.symbol} {managed.side} +{increment:.6f} "
            f"(累计 {managed.filled:.6f}) → Delta {signed:+.6f}"
        )

    return on_fill


# =============================================================================
# 主循环
# =============================================================================

def market_data_loop(
    gateway,
    rm: RiskManager,
    symbols: List[str],
    poll_interval: float,
    shutdown_event: threading.Event,
    logger: logging.Logger,
) -> None:
    """
    行情轮询主循环。

    每隔 poll_interval 秒拉取一次所有品种的 ticker，
    更新 RiskManager 的 mid-price，并预留策略调用点。
    """
    logger.info(
        f"行情轮询启动: symbols={symbols}, interval={poll_interval}s"
    )

    while not shutdown_event.is_set():
        loop_start = time.time()
        mids = {}

        for symbol in symbols:
            if shutdown_event.is_set():
                break
            try:
                ticker = gateway.fetch_ticker(symbol)

                # 计算 mid-price
                if ticker.bid and ticker.ask:
                    mid = (ticker.bid + ticker.ask) / 2.0
                elif ticker.last:
                    mid = ticker.last
                else:
                    continue

                rm.update_mid_price(symbol, mid)
                mids[symbol] = mid

                # ── 策略调用点 (TODO: strategy.on_ticker(ticker)) ──────────

            except Exception as e:
                logger.warning(f"行情获取失败 [{symbol}]: {e}")

        if mids:
            summary = "  ".join(f"{s.split('/')[0]}={v:.2f}" for s, v in mids.items())
            logger.info(f"[行情] {summary}")

        # 精确等待剩余时间（去掉已用的轮询耗时）
        elapsed = time.time() - loop_start
        wait = max(0.0, poll_interval - elapsed)
        shutdown_event.wait(timeout=wait)

    logger.info("行情轮询已停止")


# =============================================================================
# 入口
# =============================================================================

def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("main")

    logger.info("=" * 55)
    logger.info("  Delta Neutral Market Maker  启动")
    logger.info("=" * 55)

    # ---- 参数解析 ----
    symbols: List[str] = args.symbols or load_symbols_from_config(args.config)
    poll_interval: float = args.poll_interval or load_poll_interval_from_config(args.config)

    if not symbols:
        logger.error("未配置交易品种，请通过 --symbols 或 config.symbols.perpetual 指定")
        sys.exit(1)

    logger.info(f"配置文件   : {args.config}")
    logger.info(f"交易品种   : {symbols}")
    logger.info(f"轮询间隔   : {poll_interval}s")

    # =========================================================================
    # 1. Gateway
    # =========================================================================
    logger.info("── 初始化 Gateway ──")
    gateway = GatewayFactory.create(args.config)
    if not gateway.connect():
        logger.error("网关连接失败，退出")
        sys.exit(1)
    logger.info(f"Gateway 已连接: {gateway.exchange_name} ({gateway.status.value})")

    # =========================================================================
    # 2. OrderManager
    # =========================================================================
    logger.info("── 初始化 OrderManager ──")
    om = OrderManager(gateway, reconcile_interval=3.0)
    om.start()

    # =========================================================================
    # 3. ConnectionMonitor
    # =========================================================================
    logger.info("── 初始化 ConnectionMonitor ──")
    monitor = ConnectionMonitor(gateway, om, policy=ReconnectPolicy())
    monitor.on("on_disconnect", lambda e: logger.warning(f"连接断开: {e}，自动重连中..."))
    monitor.on("on_reconnected", lambda: logger.info("重连成功，恢复行情订阅"))
    monitor.start()

    # =========================================================================
    # 4. RiskManager
    # =========================================================================
    logger.info("── 初始化 RiskManager ──")
    rm = RiskManager(
        gateway=gateway,
        order_manager=om,
        connection_monitor=monitor,
        config=RiskConfig(),
    )

    # KillSwitch 触发后通知主循环退出
    shutdown_event = threading.Event()
    rm.on("on_kill_complete", lambda _: shutdown_event.set())

    rm.arm()

    # =========================================================================
    # 5. 事件回调
    # =========================================================================
    om.on("order_filled", make_fill_handler(rm, logger))
    om.on("order_submitted", lambda o: logger.info(
        f"挂单确认: {o.symbol} {o.side} {o.amount} @ {o.price} [{o.client_order_id}]"
    ))
    om.on("order_cancelled", lambda o: logger.info(
        f"撤单确认: {o.symbol} {o.side} [{o.client_order_id}]"
    ))

    # =========================================================================
    # 6. 行情轮询（后台线程）
    # =========================================================================
    poll_thread = threading.Thread(
        target=market_data_loop,
        args=(gateway, rm, symbols, poll_interval, shutdown_event, logger),
        name="MarketDataPoller",
        daemon=True,
    )
    poll_thread.start()

    logger.info("=" * 55)
    logger.info("  系统就绪，等待策略信号 (Ctrl+C 退出)")
    logger.info("=" * 55)

    # =========================================================================
    # 7. 主线程：等待退出信号
    # =========================================================================
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1.0)

    except KeyboardInterrupt:
        # KillSwitch 会捕获 SIGINT，但偶尔主线程先感知到，补一次触发
        if not rm.kill_switch.is_triggered:
            logger.info("收到 Ctrl+C")
            shutdown_event.set()

    # =========================================================================
    # 8. Graceful shutdown
    # =========================================================================
    finally:
        logger.info("── Graceful Shutdown ──")

        if not rm.kill_switch.is_triggered:
            # KillSwitch 未触发（正常退出路径）：手动撤单
            logger.info("撤销所有挂单...")
            try:
                cancelled_om = om.cancel_all()
                logger.info(f"OrderManager 撤掉 {cancelled_om} 个挂单")
            except Exception as e:
                logger.error(f"OM 撤单失败: {e}")
            try:
                cancelled_gw = gateway.cancel_all_orders()
                if cancelled_gw:
                    logger.info(f"Gateway 兜底撤掉 {cancelled_gw} 个挂单")
            except Exception as e:
                logger.error(f"Gateway 兜底撤单失败: {e}")

            monitor.stop()
            om.stop()
        else:
            # KillSwitch 已处理撤单和组件停止，等它结束
            logger.info("KillSwitch 已处理撤单，跳过重复操作")

        rm.disarm()
        gateway.disconnect()

        # 等待轮询线程自然退出
        poll_thread.join(timeout=poll_interval + 1)

        logger.info("=" * 55)
        logger.info("  系统已关停")
        logger.info("=" * 55)


if __name__ == "__main__":
    main()
