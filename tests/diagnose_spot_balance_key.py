#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid 现货 USOL/USDC 在 fetch_balance 中的真实 key 诊断

问题背景:
    实盘账户里手动建好了 0.3 USOL 现货底仓，但 main.py 的"现货底仓同步"逻辑
    没触发（日志里没有 "现货底仓同步:" 这一行，只有 USDC 余额同步）。
    怀疑 _boot_balance.balances.get("USOL") 返回了 None，即 Hyperliquid
    在 CCXT 里 base asset 的 key 不是字面的 "USOL"。

验收回答的问题:
    1) 现货 SOL 在 balance 字典里的 key 是 "USOL" / "SOL" / "@107" / 其他？
    2) markets["USOL/USDC"] 的 base / baseId / info 分别是什么？
    3) 不同 key 下的 free / total 值是多少？

运行:
    python tests/diagnose_spot_balance_key.py

只读操作，不下任何单。
"""

import io
import os
import sys
import traceback
from typing import Any, Dict, List, Optional

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gateways import GatewayFactory


CONFIG_PATH = "config/hyperliquid_config.yaml"
SPOT_SYMBOL = "USOL/USDC"
PERP_SYMBOL = "SOL/USDC:USDC"


def _sep(title: str = "") -> None:
    line = "=" * 72
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def _subsep(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 68 - len(title)))


def _pretty(v: Any, max_len: int = 400) -> str:
    s = repr(v)
    if len(s) > max_len:
        return s[:max_len] + f"... <截断，原长度 {len(s)}>"
    return s


# =============================================================================
# 步骤 1: Gateway 初始化
# =============================================================================
def step1_connect():
    _sep("步骤 1: 创建 Gateway 并连接")
    try:
        gateway = GatewayFactory.create(CONFIG_PATH)
        ok = gateway.connect()
        print(f"    connect() 返回: {ok}")
        print(f"    exchange_name:   {gateway.exchange_name}")
        print(f"    status:          {gateway.status.value}")
        if not ok:
            print("    FAIL — Gateway 未连接成功")
            sys.exit(1)
        return gateway
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)


# =============================================================================
# 步骤 2: 调用 fetch_balance → 打印 gateway 封装层的所有 balance 条目
# =============================================================================
def step2_gateway_balance(gateway) -> None:
    _sep("步骤 2: gateway.fetch_balance() 的 AccountBalance.balances 字典")
    print("    注: AccountBalance.from_ccxt 只保留 total > 0 的条目")
    print("         → 这里如果 USOL 不在输出中，要么是 total=0，要么 key 不是 'USOL'")
    try:
        acct = gateway.fetch_balance()
        print(f"    balances 字典大小: {len(acct.balances)}")
        for cur, b in acct.balances.items():
            print(f"      key={cur!r:15s} free={b.free}  used={b.used}  total={b.total}")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()


# =============================================================================
# 步骤 3: 直接读 CCXT 原始 fetch_balance dict，绕过 AccountBalance.from_ccxt 的过滤
# =============================================================================
def step3_raw_balance(gateway) -> None:
    _sep("步骤 3: 直接读 ccxt._exchange.fetch_balance() — 不过滤 total > 0")
    try:
        exchange = gateway.exchange
        if exchange is None:
            print("    FAIL: gateway.exchange 为 None")
            return

        raw = exchange.fetch_balance()
        print(f"    raw dict 顶层 key 列表: {list(raw.keys())}")

        for sub_name in ("total", "free", "used"):
            sub = raw.get(sub_name, {}) or {}
            _subsep(f"3.{sub_name}: raw[{sub_name!r}]  ({len(sub)} 个 key)")
            if not sub:
                print("      (空)")
                continue
            # 遍历所有 key（包括值为 0 或 None 的）
            for k, v in sub.items():
                print(f"      key={k!r:20s} value={v!r}")

        # info 段 — Hyperliquid 的 L1 原始响应在这里，
        # 可看到 spotBalances / assetBalances 等原生字段
        info = raw.get("info") or {}
        _subsep("3.info: raw['info'] 顶层 key")
        if isinstance(info, dict):
            for k, v in info.items():
                print(f"      {k}: {_pretty(v, max_len=300)}")
        else:
            print(f"      (非 dict) {_pretty(info, max_len=400)}")

    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()


# =============================================================================
# 步骤 4: 打印现货 market USOL/USDC 的关键字段
# =============================================================================
def step4_spot_market_meta(gateway) -> None:
    _sep(f"步骤 4: markets[{SPOT_SYMBOL!r}] 元数据")
    try:
        markets = gateway.get_markets()
        if SPOT_SYMBOL not in markets:
            print(f"    FAIL: {SPOT_SYMBOL!r} 不在 markets — 现有 spot key 前 30 个:")
            spots = [
                s for s, m in markets.items()
                if m.get("type") == "spot" or m.get("spot") is True
            ]
            for s in spots[:30]:
                print(f"      {s}")
            return

        m = markets[SPOT_SYMBOL]
        # 关键字段
        for field_name in (
            "id", "symbol", "base", "quote", "baseId", "quoteId",
            "settle", "settleId", "type", "spot", "swap",
            "active", "precision", "limits",
        ):
            print(f"    {field_name:12s}= {_pretty(m.get(field_name), max_len=300)}")

        # info 段 — Hyperliquid 原始响应，常在此能看到 "@107" 这类 token id
        info = m.get("info")
        _subsep("markets[USOL/USDC]['info']")
        if isinstance(info, dict):
            for k, v in info.items():
                print(f"      {k}: {_pretty(v, max_len=400)}")
        else:
            print(f"      (非 dict) {_pretty(info, max_len=500)}")

    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()


# =============================================================================
# 步骤 5: 打印永续 market SOL/USDC:USDC 的关键字段（对照用）
# =============================================================================
def step5_perp_market_meta(gateway) -> None:
    _sep(f"步骤 5: markets[{PERP_SYMBOL!r}] 元数据（对照）")
    try:
        markets = gateway.get_markets()
        if PERP_SYMBOL not in markets:
            print(f"    FAIL: {PERP_SYMBOL!r} 不在 markets")
            return

        m = markets[PERP_SYMBOL]
        for field_name in (
            "id", "symbol", "base", "quote", "baseId", "quoteId",
            "settle", "settleId", "type", "spot", "swap",
            "active", "precision", "limits",
        ):
            print(f"    {field_name:12s}= {_pretty(m.get(field_name), max_len=300)}")

        info = m.get("info")
        _subsep("markets[SOL/USDC:USDC]['info']")
        if isinstance(info, dict):
            for k, v in info.items():
                print(f"      {k}: {_pretty(v, max_len=400)}")
        else:
            print(f"      (非 dict) {_pretty(info, max_len=500)}")

    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()


# =============================================================================
# 步骤 6: 列出所有 balance keys 中疑似 SOL 相关的条目，对照 markets 的 baseId
# =============================================================================
def step6_cross_reference(gateway) -> None:
    _sep("步骤 6: 交叉比对 — balance key vs. markets baseId")
    try:
        raw = gateway.exchange.fetch_balance()
        markets = gateway.get_markets()

        # 所有 balance 里出现过的 key
        all_bal_keys = set()
        for sub_name in ("total", "free", "used"):
            sub = raw.get(sub_name, {}) or {}
            all_bal_keys.update(sub.keys())

        print(f"    balance 中出现的所有 key 数量: {len(all_bal_keys)}")
        print(f"    balance 中所有 key: {sorted(all_bal_keys)}")

        # 疑似 SOL 相关的 balance key（大小写不敏感 + 包含 SOL 子串）
        sol_like_bal_keys = sorted(k for k in all_bal_keys if "SOL" in str(k).upper())
        _subsep("含 'SOL' 的 balance key")
        if sol_like_bal_keys:
            for k in sol_like_bal_keys:
                total = (raw.get("total") or {}).get(k)
                free = (raw.get("free") or {}).get(k)
                used = (raw.get("used") or {}).get(k)
                print(f"      key={k!r:15s} total={total!r}  free={free!r}  used={used!r}")
        else:
            print("      (无)")

        # USOL/USDC 对应的 baseId
        usol_market = markets.get(SPOT_SYMBOL) or {}
        base_id = usol_market.get("baseId")
        base = usol_market.get("base")
        _subsep(f"markets[{SPOT_SYMBOL}] 的 base / baseId")
        print(f"      base   = {base!r}")
        print(f"      baseId = {base_id!r}")
        if base_id is not None:
            in_bal = base_id in all_bal_keys
            print(f"      baseId in balance keys? {in_bal}")
        if base is not None:
            in_bal = base in all_bal_keys
            print(f"      base   in balance keys? {in_bal}")

        # 最终结论
        _subsep("结论")
        hit_keys = []
        for candidate in (base, base_id, "USOL", "SOL"):
            if candidate and candidate in all_bal_keys:
                hit_keys.append(candidate)
        if hit_keys:
            print(f"      >>> 可用作 balance dict 的 key: {hit_keys}")
            print(f"      >>> main.py 应该用其中一个作 lookup（推荐用 markets[sym]['base']）")
        else:
            print("      >>> 没有候选 key 能在 balance 中命中")
            print("      >>> 可能原因: (a) 账户里确实无 SOL 现货；")
            print("                   (b) Hyperliquid 把现货余额放在 info.spotBalances")
            print("                       而 CCXT 没平铺到 total/free/used；")
            print("                       检查步骤 3 的 raw['info'] 输出。")
    except Exception as e:
        print(f"    FAIL: {type(e).__name__}: {e}")
        traceback.print_exc()


# =============================================================================
# 主流程
# =============================================================================
def main() -> None:
    _sep("Hyperliquid 现货 base asset key 诊断")
    print(f"CONFIG_PATH = {CONFIG_PATH}")
    print(f"SPOT_SYMBOL = {SPOT_SYMBOL}")
    print(f"PERP_SYMBOL = {PERP_SYMBOL}")

    gateway = step1_connect()

    try:
        step2_gateway_balance(gateway)
    except Exception as e:
        print(f"步骤 2 异常: {type(e).__name__}: {e}")

    try:
        step3_raw_balance(gateway)
    except Exception as e:
        print(f"步骤 3 异常: {type(e).__name__}: {e}")

    try:
        step4_spot_market_meta(gateway)
    except Exception as e:
        print(f"步骤 4 异常: {type(e).__name__}: {e}")

    try:
        step5_perp_market_meta(gateway)
    except Exception as e:
        print(f"步骤 5 异常: {type(e).__name__}: {e}")

    try:
        step6_cross_reference(gateway)
    except Exception as e:
        print(f"步骤 6 异常: {type(e).__name__}: {e}")

    # ── 断开 ─────────────────────────────────────────────────────────────
    try:
        gateway.disconnect()
    except Exception:
        pass

    _sep("诊断结束")


if __name__ == "__main__":
    main()
