#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid 现货 SOL 的真实 symbol 和 fetch_balance key 诊断

背景:
    网页显示 SOL/USDC，但 gateway.get_markets() 既没有 USOL/USDC 也没有
    SOL/USDC。本脚本绕开 gateway 封装，直接用 CCXT + Hyperliquid 原生
    /info 端点定位真实 symbol 和 balance key。

回答的问题:
    1) CCXT 里 Hyperliquid 现货 SOL 实际的 symbol 是什么？
    2) fetch_balance 里如果有 SOL 现货余额，key 是什么？
    3) Hyperliquid 原生 spotMeta 里 SOL 对应的 token 记录？
    4) 当前账户 spotClearinghouseState 的真实现货持仓？

只读操作，不下任何单。
"""

import io
import os
import sys

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ccxt
import yaml

CONFIG_PATH = "config/hyperliquid_config.yaml"


# =============================================================================
# 1. 加载凭证
# =============================================================================
with open(CONFIG_PATH, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)["hyperliquid"]


# =============================================================================
# 2. 分别创建 spot 模式和 swap 模式的实例，看 markets 差异
# =============================================================================
for default_type in ("spot", "swap"):
    print(f"\n{'='*70}")
    print(f"  defaultType = {default_type}")
    print(f"{'='*70}")

    ex = ccxt.hyperliquid({
        "walletAddress": cfg["authentication"]["wallet_address"],
        "privateKey": cfg["authentication"]["private_key"],
        "options": {"defaultType": default_type},
        "enableRateLimit": True,
    })
    ex.options.setdefault("fetchMarkets", {})["types"] = ["spot", "swap"]

    try:
        markets = ex.load_markets()
    except Exception as e:
        print(f"  load_markets 失败: {type(e).__name__}: {e}")
        continue

    # 只看含 "SOL" 的
    sol_markets = {s: m for s, m in markets.items() if "SOL" in s.upper()}
    print(f"  包含 SOL 的 market 共 {len(sol_markets)} 个:")
    for s, m in sol_markets.items():
        print(
            f"    {s:<30} type={m.get('type')} "
            f"base={m.get('base')} baseId={m.get('baseId')} "
            f"quote={m.get('quote')} quoteId={m.get('quoteId')}"
        )

    # fetch_balance
    print(f"\n  fetch_balance 结果:")
    try:
        bal = ex.fetch_balance()
        for key in ("total", "free", "used"):
            sub = bal.get(key, {}) or {}
            sol_keys = {
                k: v for k, v in sub.items()
                if "SOL" in str(k).upper() or k in ("UBTC", "UETH", "USOL")
            }
            if sol_keys:
                print(f"    bal['{key}'] SOL 相关: {sol_keys}")
            else:
                print(f"    bal['{key}'] SOL 相关: (无)")
        # 完整 key 列表也打出来，便于发现 "@107" 这类 id
        all_keys = set()
        for key in ("total", "free", "used"):
            all_keys.update((bal.get(key) or {}).keys())
        print(f"    bal 所有 key: {sorted(all_keys)}")
    except Exception as e:
        print(f"    fetch_balance 失败: {type(e).__name__}: {e}")


# =============================================================================
# 3. Hyperliquid 原生 /info spotMeta（所有现货币种 + 交易对）
# =============================================================================
print(f"\n{'='*70}")
print(f"  Hyperliquid 原生 API: spotMeta（所有现货币种）")
print(f"{'='*70}")
try:
    import requests
    r = requests.post(
        "https://api.hyperliquid.xyz/info",
        json={"type": "spotMeta"},
        timeout=10,
    )
    meta = r.json()

    tokens = meta.get("tokens", [])
    print(f"  tokens 共 {len(tokens)} 个，名字包含 SOL 的:")
    for tok in tokens:
        name = tok.get("name", "")
        if "SOL" in name.upper():
            print(f"    {tok}")

    universe = meta.get("universe", [])
    print(f"\n  universe 共 {len(universe)} 个交易对，名字包含 SOL 的:")
    for u in universe:
        name = u.get("name", "")
        if "SOL" in name.upper():
            print(f"    {u}")

    # 作为交叉参考，把和 SOL token 相关的 universe 条目（通过 tokens 索引）也列出来
    sol_token_indices = [
        i for i, tok in enumerate(tokens)
        if "SOL" in tok.get("name", "").upper()
    ]
    if sol_token_indices:
        print(f"\n  通过 token index 查找引用 SOL 的 universe 条目:")
        for u in universe:
            tokens_ref = u.get("tokens", [])
            if any(idx in tokens_ref for idx in sol_token_indices):
                print(f"    universe entry: {u}")
except Exception as e:
    print(f"  spotMeta 请求失败: {type(e).__name__}: {e}")


# =============================================================================
# 4. 账户现货持仓: spotClearinghouseState
# =============================================================================
print(f"\n{'='*70}")
print(f"  账户现货持仓: spotClearinghouseState")
print(f"{'='*70}")
try:
    addr = cfg["authentication"]["wallet_address"]
    print(f"  wallet_address: {addr}")
    r = requests.post(
        "https://api.hyperliquid.xyz/info",
        json={"type": "spotClearinghouseState", "user": addr},
        timeout=10,
    )
    resp = r.json()
    print(f"  响应: {resp}")
    # 解读 balances 字段
    if isinstance(resp, dict) and "balances" in resp:
        print(f"\n  balances 明细:")
        for b in resp.get("balances", []):
            print(f"    {b}")
except Exception as e:
    print(f"  spotClearinghouseState 请求失败: {type(e).__name__}: {e}")


print(f"\n{'='*70}")
print(f"  诊断结束")
print(f"{'='*70}")
