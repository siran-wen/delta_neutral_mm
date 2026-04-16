#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid 单 CCXT 实例双市场（现货 + 永续）能力验证

目的:
    验证一个 HyperliquidGateway 实例能否同时操作现货和永续。
    这决定了"现货做市 + 永续对冲"双腿架构是单 Gateway 还是双 Gateway。

验证步骤:
    1. 行情查询双市场 (fetch_ticker)
    2. 订单簿查询双市场 (fetch_order_book)
    3. 市场元数据 (get_markets) - 过滤 spot / swap
    4. 持仓 (fetch_positions) + 现货余额 (fetch_balance)
    5. 现货 symbol 格式确认 (CCXT 对 Hyperliquid 现货的 symbol 命名)

运行:
    python tests/test_dual_market.py
"""

import io
import os
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gateways import GatewayFactory


CONFIG_PATH = "config/hyperliquid_config.yaml"

# 永续 symbol 按 CCXT 惯例是 BASE/QUOTE:SETTLE
PERP_SYMBOL = "SOL/USDC:USDC"
# 现货 symbol 先按常见 CCXT 格式猜测，实际 key 会在 Step 5 里确认
SPOT_SYMBOL_GUESS = "SOL/USDC"


def _sep(title: str = "") -> None:
    line = "=" * 72
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def _subsep(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 68 - len(title)))


def _mid(ticker) -> Optional[float]:
    if ticker.bid and ticker.ask:
        return (ticker.bid + ticker.ask) / 2.0
    return ticker.last


# =============================================================================
# 步骤函数（每个返回 (成功与否, 备注)）
# =============================================================================

def step1_tickers(gateway, spot_symbol: str) -> Dict[str, Tuple[bool, str]]:
    """双市场行情查询"""
    _sep("步骤 1: 行情查询双市场 (fetch_ticker)")
    results: Dict[str, Tuple[bool, str]] = {}

    _subsep(f"1a. 永续 {PERP_SYMBOL}")
    try:
        t = gateway.fetch_ticker(PERP_SYMBOL)
        mid = _mid(t)
        print(f"    bid={t.bid}  ask={t.ask}  last={t.last}  mid={mid}")
        results["perp_ticker"] = (True, f"mid={mid}")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["perp_ticker"] = (False, f"{type(e).__name__}: {e}")

    _subsep(f"1b. 现货 {spot_symbol}")
    try:
        t = gateway.fetch_ticker(spot_symbol)
        mid = _mid(t)
        print(f"    bid={t.bid}  ask={t.ask}  last={t.last}  mid={mid}")
        results["spot_ticker"] = (True, f"mid={mid}")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["spot_ticker"] = (False, f"{type(e).__name__}: {e}")

    return results


def step2_orderbooks(gateway, spot_symbol: str) -> Dict[str, Tuple[bool, str]]:
    """双市场订单簿查询"""
    _sep("步骤 2: OrderBook 查询双市场 (fetch_order_book limit=5)")
    results: Dict[str, Tuple[bool, str]] = {}

    def _print_ob(ob) -> None:
        print(f"    Top 5 bids: {ob.bids[:5]}")
        print(f"    Top 5 asks: {ob.asks[:5]}")

    _subsep(f"2a. 永续 {PERP_SYMBOL}")
    try:
        ob = gateway.fetch_order_book(PERP_SYMBOL, limit=5)
        _print_ob(ob)
        results["perp_orderbook"] = (True, f"{len(ob.bids)} bids / {len(ob.asks)} asks")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["perp_orderbook"] = (False, f"{type(e).__name__}: {e}")

    _subsep(f"2b. 现货 {spot_symbol}")
    try:
        ob = gateway.fetch_order_book(spot_symbol, limit=5)
        _print_ob(ob)
        results["spot_orderbook"] = (True, f"{len(ob.bids)} bids / {len(ob.asks)} asks")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["spot_orderbook"] = (False, f"{type(e).__name__}: {e}")

    return results


def step3_markets(
    gateway, spot_symbol: str
) -> Tuple[Dict[str, Tuple[bool, str]], Dict[str, Any]]:
    """市场元数据 (get_markets)"""
    _sep("步骤 3: 市场元数据验证 (get_markets)")
    results: Dict[str, Tuple[bool, str]] = {}
    markets: Dict[str, Any] = {}

    try:
        markets = gateway.get_markets()
        print(f"    markets 总数: {len(markets)}")
    except Exception as e:
        print(f"    FAIL get_markets: {type(e).__name__}: {e}")
        results["get_markets"] = (False, f"{type(e).__name__}: {e}")
        return results, markets

    # 按 type 统计
    type_count: Dict[str, int] = {}
    for m in markets.values():
        t = m.get("type") or "unknown"
        type_count[t] = type_count.get(t, 0) + 1
    print(f"    按 type 分布: {type_count}")

    _subsep(f"3a. 永续 {PERP_SYMBOL} 是否在 markets 字典")
    if PERP_SYMBOL in markets:
        m = markets[PERP_SYMBOL]
        print(f"    存在  type={m.get('type')}  swap={m.get('swap')}  spot={m.get('spot')}")
        print(f"    precision={m.get('precision')}")
        results["perp_market_meta"] = (True, f"type={m.get('type')}")
    else:
        print(f"    缺失")
        results["perp_market_meta"] = (False, "key 不存在")

    _subsep(f"3b. 现货 {spot_symbol} 是否在 markets 字典")
    if spot_symbol in markets:
        m = markets[spot_symbol]
        print(f"    存在  type={m.get('type')}  swap={m.get('swap')}  spot={m.get('spot')}")
        print(f"    precision={m.get('precision')}")
        results["spot_market_meta"] = (True, f"type={m.get('type')}")
    else:
        print(f"    缺失 — 真实现货 symbol 可能不是 {spot_symbol}（见步骤 5）")
        results["spot_market_meta"] = (False, "key 不存在")

    return results, markets


def step4_positions_and_balance(gateway) -> Dict[str, Tuple[bool, str]]:
    """永续持仓 + 现货余额"""
    _sep("步骤 4: 持仓 / 余额查询")
    results: Dict[str, Tuple[bool, str]] = {}

    _subsep(f"4a. 永续持仓 fetch_positions([{PERP_SYMBOL}])")
    try:
        positions = gateway.fetch_positions([PERP_SYMBOL])
        print(f"    返回 {len(positions)} 个持仓记录")
        for p in positions:
            print(
                f"      {p.symbol}  side={p.side}  size={p.size}  "
                f"entry={p.entry_price}  uPnL={p.unrealized_pnl}"
            )
        results["perp_positions"] = (True, f"{len(positions)} 条")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["perp_positions"] = (False, f"{type(e).__name__}: {e}")

    _subsep("4b. 现货余额 fetch_balance()")
    try:
        bal = gateway.fetch_balance()
        print(f"    返回 {len(bal.balances)} 个有余额的币种")
        for cur, b in bal.balances.items():
            print(f"      {cur}  free={b.free}  used={b.used}  total={b.total}")
        results["spot_balance"] = (True, f"{len(bal.balances)} 个币种")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        results["spot_balance"] = (False, f"{type(e).__name__}: {e}")

    return results


def step5_spot_symbol_format(
    markets: Dict[str, Any],
) -> Tuple[Dict[str, Tuple[bool, str]], Optional[str]]:
    """列出所有 spot symbol，确认 CCXT 对 Hyperliquid 现货的命名方式"""
    _sep("步骤 5: 现货 symbol 格式确认 (过滤 type == 'spot')")
    results: Dict[str, Tuple[bool, str]] = {}

    spot_symbols: List[str] = []
    for sym, m in markets.items():
        if m.get("type") == "spot" or m.get("spot") is True:
            spot_symbols.append(sym)

    print(f"    总计 spot 市场数: {len(spot_symbols)}")
    if not spot_symbols:
        print("    无 spot 市场 — 检查 CCXT 版本是否支持 Hyperliquid 现货；")
        print("    或 defaultType='swap' 时是否过滤掉了现货 markets。")
        results["spot_enum"] = (False, "0 个 spot symbol")
        return results, None

    # 打印前 30 个作为样本
    sample = spot_symbols[:30]
    print(f"    前 {len(sample)} 个样本:")
    for s in sample:
        base = markets[s].get("base")
        quote = markets[s].get("quote")
        print(f"      {s:25s}  base={base}  quote={quote}")

    # 尝试定位"SOL 现货"
    # Hyperliquid 惯例：U 前缀（USOL/UBTC/UETH）才是"统一现货"对应的 L1 原生资产；
    # 其他 SOL 子串的 symbol 是 meme 或衍生币（如 SOLV）。
    sol_candidates = [s for s in spot_symbols if "SOL" in s.upper()]
    if sol_candidates:
        print(f"\n    SOL 相关 spot 候选: {sol_candidates}")
        preferred = [s for s in sol_candidates if s.startswith("USOL/")]
        sol_symbol = preferred[0] if preferred else sol_candidates[0]
        note = (
            f"{len(spot_symbols)} 个，SOL 候选: {sol_candidates}，"
            f"选定: {sol_symbol}"
        )
        results["spot_enum"] = (True, note)
        return results, sol_symbol
    else:
        print("    未找到 SOL 相关 spot — Hyperliquid 可能没有 SOL 现货")
        results["spot_enum"] = (True, f"{len(spot_symbols)} 个，但无 SOL 现货")
        return results, None


# =============================================================================
# 主流程
# =============================================================================

def main() -> None:
    _sep("Hyperliquid 单实例双市场验证")
    print(f"CONFIG: {CONFIG_PATH}")
    print(f"PERP_SYMBOL: {PERP_SYMBOL}")
    print(f"SPOT_SYMBOL (初始猜测): {SPOT_SYMBOL_GUESS}")

    all_results: Dict[str, Tuple[bool, str]] = {}

    # ── 创建并连接 Gateway ───────────────────────────────────────────────
    _sep("0. 初始化 Gateway")
    try:
        gateway = GatewayFactory.create(CONFIG_PATH)
        if not gateway.connect():
            print("    Gateway.connect() 返回 False，退出")
            sys.exit(1)
        print(f"    已连接: {gateway.exchange_name} ({gateway.status.value})")

        # 打印 CCXT 底层配置，帮助诊断 defaultType
        try:
            opts = gateway.exchange.options if gateway.exchange else {}
            print(f"    ccxt.options.defaultType = {opts.get('defaultType')}")
            fetch_markets_opts = opts.get("fetchMarkets") or {}
            print(f"    ccxt.options.fetchMarkets = {fetch_markets_opts}")
        except Exception as e:
            print(f"    读取 ccxt.options 失败: {e}")
    except Exception as e:
        print(f"    Gateway 创建失败: {type(e).__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)

    # ── 先做一次市场元数据查询，拿到真实可用的现货 symbol ───────────────
    # 这样即便初始猜测的 SPOT_SYMBOL_GUESS 不对，后续步骤也能用正确 symbol
    try:
        preload_markets = gateway.get_markets()
    except Exception as e:
        print(f"    预加载 markets 失败: {type(e).__name__}: {e}")
        preload_markets = {}

    # 从 preload 里找 SOL 现货作为后续步骤真实 symbol
    # Hyperliquid 现货命名惯例: USOL = Solana, UBTC = BTC, UETH = ETH（U 前缀表示 "unified"）
    # 注意: "SOL/USDC" 和 "SOLV/USDC" 是其他代币（meme/衍生品），不是 Solana 现货
    candidate_spot = SPOT_SYMBOL_GUESS
    sol_spots: List[str] = []
    if preload_markets:
        sol_spots = [
            s for s, m in preload_markets.items()
            if (m.get("type") == "spot" or m.get("spot") is True) and "SOL" in s.upper()
        ]
        if sol_spots:
            # 优先选 USOL/USDC（Hyperliquid 现货 SOL 的真实 symbol）
            preferred = [s for s in sol_spots if s.startswith("USOL/")]
            candidate_spot = preferred[0] if preferred else sol_spots[0]
            print(f"    SOL 相关 spot 候选: {sol_spots}")
            print(f"    选定用于后续测试的现货 symbol: {candidate_spot}")
        else:
            print(f"    markets 中未识别到 SOL 现货，沿用猜测值: {SPOT_SYMBOL_GUESS}")

    # 对所有 SOL 现货候选都打印一次 ticker，帮助确认哪个是 Solana 现货
    if sol_spots:
        _subsep("SOL 现货候选 ticker 对照（通过 last/mid 量级可辨别）")
        for s in sol_spots:
            try:
                t = gateway.fetch_ticker(s)
                print(f"      {s:15s}  bid={t.bid}  ask={t.ask}  last={t.last}")
            except Exception as e:
                print(f"      {s:15s}  FAIL: {type(e).__name__}: {e}")

    # ── 步骤 1-4 ─────────────────────────────────────────────────────────
    all_results.update(step1_tickers(gateway, candidate_spot))
    all_results.update(step2_orderbooks(gateway, candidate_spot))
    step3_res, markets = step3_markets(gateway, candidate_spot)
    all_results.update(step3_res)
    all_results.update(step4_positions_and_balance(gateway))

    # ── 步骤 5 ───────────────────────────────────────────────────────────
    step5_res, real_sol_spot = step5_spot_symbol_format(markets or preload_markets)
    all_results.update(step5_res)

    # 如果 Step 5 发现了真实现货 symbol 且与之前使用的不同，用它重跑一次 ticker/orderbook
    if real_sol_spot and real_sol_spot != candidate_spot:
        _sep(f"追加: 用真实 SOL 现货 symbol '{real_sol_spot}' 重跑 1+2")
        all_results.update(
            {f"retry_{k}": v for k, v in step1_tickers(gateway, real_sol_spot).items()}
        )
        all_results.update(
            {f"retry_{k}": v for k, v in step2_orderbooks(gateway, real_sol_spot).items()}
        )

    # ── 断开 ─────────────────────────────────────────────────────────────
    try:
        gateway.disconnect()
    except Exception:
        pass

    # =========================================================================
    # 总结
    # =========================================================================
    _sep("总结")
    n_pass = sum(1 for ok, _ in all_results.values() if ok)
    n_fail = len(all_results) - n_pass

    print(f"通过: {n_pass}   失败: {n_fail}\n")
    print(f"{'Step':30s} {'Result':8s} Note")
    print("-" * 72)
    for step, (ok, note) in all_results.items():
        tag = "PASS" if ok else "FAIL"
        print(f"{step:30s} {tag:8s} {note}")

    # 最终结论
    _sep("最终结论")
    # 公开数据（无签名，能判断"单实例能否路由到双市场"的关键）
    perp_public_ok = all(
        all_results.get(k, (False, ""))[0]
        for k in ("perp_ticker", "perp_orderbook", "perp_market_meta")
    )
    spot_ticker_ok = any(
        all_results.get(k, (False, ""))[0]
        for k in ("spot_ticker", "retry_spot_ticker")
    )
    spot_ob_ok = any(
        all_results.get(k, (False, ""))[0]
        for k in ("spot_orderbook", "retry_spot_orderbook")
    )
    spot_meta_ok = all_results.get("spot_market_meta", (False, ""))[0]
    spot_enum_ok = all_results.get("spot_enum", (False, ""))[0]
    # 私有接口 — 失败多半是配置 / 认证问题（见 tests/ccxt_422_investigation.md）
    positions_ok = all_results.get("perp_positions", (False, ""))[0]
    balance_ok = all_results.get("spot_balance", (False, ""))[0]

    print("  [公开数据]")
    print(f"    永续 ticker/ob/meta                : {'OK' if perp_public_ok else 'FAIL'}")
    print(f"    现货 ticker                         : {'OK' if spot_ticker_ok else 'FAIL'}")
    print(f"    现货 orderbook                      : {'OK' if spot_ob_ok else 'FAIL'}")
    print(f"    现货 markets 元数据                 : {'OK' if spot_meta_ok else 'FAIL'}")
    print(f"    现货 symbol 枚举                    : {'OK' if spot_enum_ok else 'FAIL'}")
    print("  [私有接口]")
    print(f"    永续 fetch_positions               : {'OK' if positions_ok else 'FAIL'}")
    print(f"    现货 fetch_balance                 : {'OK' if balance_ok else 'FAIL'}")

    public_dual_ok = perp_public_ok and spot_ticker_ok and spot_ob_ok and spot_meta_ok
    private_ok = positions_ok and balance_ok

    print()
    if public_dual_ok and private_ok:
        verdict = (
            "完全可行 — 单 CCXT 实例能同时处理现货和永续的公开+私有接口。"
            "建议用单 Gateway 架构，双腿通过 symbol 区分。"
        )
    elif public_dual_ok and not private_ok:
        verdict = (
            "只读层面可行 — 单实例已证实可路由到 spot 和 swap 两个 type 的 symbol。\n"
            "  私有接口（positions / balance）失败与单/双实例无关，\n"
            "  典型根因是 config/hyperliquid_config.yaml 的 wallet_address 格式错误\n"
            "  （完整排查见 tests/ccxt_422_investigation.md）。先修配置再复测。"
        )
    elif perp_public_ok and not (spot_ticker_ok or spot_ob_ok):
        verdict = (
            "不可行 — 永续只读正常但现货所有查询失败，"
            "需要双 Gateway 架构（或独立 defaultType=spot 实例）。"
        )
    else:
        verdict = "部分可行 — 请看上面 FAIL 项的具体原因再决定架构。"
    print(f"  >> {verdict}")
    _sep()


if __name__ == "__main__":
    main()
