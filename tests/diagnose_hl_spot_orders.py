#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断 CCXT 对 Hyperliquid 现货订单/成交历史的支持

背景:
    live_20260422_210538.log 暴露三个现象：
    1) 现货挂单会被对账巡检"启发式判为 FILLED"，尽管交易所侧订单还活着
    2) 现货单被撤销时交易所回"订单未找到"，但实际已成交，程序漏记
    3) 现货对账用 fetch_my_trades 从未匹配到 trade_history（age 很短就走启发式）

    怀疑 CCXT 在 defaultType=swap 下对 Hyperliquid 现货订单/成交的查询有 bug，
    fetch_open_orders / fetch_my_trades 要么返回空，要么 symbol 约定不对。

回答的问题:
    A) fetch_open_orders() 返回的订单列表里是否包含现货挂单？
    B) fetch_open_orders(symbol="SOL/USDC") 和 fetch_open_orders(symbol="USOL/USDC")
       能不能拿到挂单？
    C) fetch_my_trades("SOL/USDC") 和 fetch_my_trades("USOL/USDC") 能不能拿到
       最近的现货成交历史？
    D) 同样的查询对永续 SOL/USDC:USDC 行不行（作为对照组）？
    E) Hyperliquid 原生 /info 的 "openOrders" 和 "userFills" 端点能否补齐 CCXT
       返回不了的数据？

前置条件:
    账户上必须有一笔活跃的现货挂单（本次诊断的关键）。脚本会先提示用户：
    "请在 Hyperliquid 网页手动挂一笔现货 SOL/USDC 的远离 mid 的限价单（比如
     买单挂到 mid 的 80% 价位）再运行。"

    最近 1 小时内还要有若干现货成交（可以从本次实盘的关停状态继承，不用额外操作）。

只读操作，不下任何单，不撤任何单。
"""

import io
import json
import os
import sys
from pprint import pformat

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ccxt
import requests
import yaml

CONFIG_PATH = "config/hyperliquid_config.yaml"


# =============================================================================
# 工具函数
# =============================================================================
def section(title: str) -> None:
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)


def subsection(title: str) -> None:
    print()
    print("-" * 80)
    print(f"  {title}")
    print("-" * 80)


def safe_call(label: str, fn, *args, **kwargs):
    """调用并捕获异常，打印摘要结果"""
    try:
        result = fn(*args, **kwargs)
        if isinstance(result, list):
            print(f"  [{label}] 返回列表长度={len(result)}")
            if result:
                # 只打印前 3 条
                for i, r in enumerate(result[:3]):
                    print(f"    [{i}] {pformat(r, width=140, compact=True)[:400]}")
                if len(result) > 3:
                    print(f"    ... (剩余 {len(result) - 3} 条省略)")
        else:
            print(f"  [{label}] 返回: {pformat(result, width=140, compact=True)[:400]}")
        return result
    except Exception as e:
        print(f"  [{label}] 异常: {type(e).__name__}: {e}")
        return None


# =============================================================================
# 1. 加载凭证
# =============================================================================
if not os.path.exists(CONFIG_PATH):
    print(f"未找到配置文件 {CONFIG_PATH}，请先配置凭证")
    sys.exit(1)

with open(CONFIG_PATH, encoding="utf-8") as f:
    cfg_root = yaml.safe_load(f)
    cfg = cfg_root["hyperliquid"]

# 兼容 YAML 的两种字段名：authentication (现行) 或 auth (旧格式)
auth_section = cfg.get("authentication") or cfg.get("auth") or {}
if not auth_section:
    print("YAML 里找不到 authentication 或 auth 节，请检查配置")
    sys.exit(1)
PRIVATE_KEY = auth_section["private_key"]
WALLET_ADDRESS = auth_section["wallet_address"].strip()
BASE_URL = cfg["api"]["base_url"]

print(f"钱包: {WALLET_ADDRESS}")
print(f"API : {BASE_URL}")


# =============================================================================
# 2. 提示用户准备诊断环境
# =============================================================================
section("前置提示")
print("""
建议先做以下准备（否则有些查询结果会为空，看不出 bug）:

  1. 本次实盘（live_20260422_210538）的账户状态应该还有现货挂单和近期成交。
     直接运行脚本也能看到部分结果。

  2. 如果想复现 "撤单时订单未找到" 的 case，可以：
     a) 在 Hyperliquid 网页手动挂一笔 SOL/USDC 远离 mid 的限价单
     b) 立即运行这个脚本
     c) 看 CCXT 能不能通过 fetch_open_orders() 拉到这笔现货单

  3. 如果能回答"fetch_my_trades 对现货是否返回空"，只需要最近 1 小时内有现货
     成交记录即可（你上次的 live 就行）。

按 Enter 继续，或 Ctrl+C 退出...
""")
try:
    input()
except (EOFError, KeyboardInterrupt):
    print("\n已中止")
    sys.exit(0)


# =============================================================================
# 3. 创建两套 CCXT 实例：swap 模式和 spot 模式，各自查询
# =============================================================================
section("CCXT 查询能力诊断")

for default_type in ("swap", "spot"):
    subsection(f"CCXT defaultType='{default_type}' 模式")

    try:
        exchange = ccxt.hyperliquid({
            "privateKey": PRIVATE_KEY,
            "walletAddress": WALLET_ADDRESS,
            "options": {
                "defaultType": default_type,
            },
        })
        exchange.load_markets()
        print(f"  已加载 {len(exchange.markets)} 个 markets")

        # 列几个关键 symbol 看 CCXT 怎么命名的
        found_symbols = []
        for sym in exchange.markets:
            if "SOL" in sym and ("USDC" in sym or "USDT" in sym):
                found_symbols.append(sym)
        print(f"  包含 SOL 的 markets: {found_symbols[:10]}")

    except Exception as e:
        print(f"  初始化异常: {type(e).__name__}: {e}")
        continue

    # ---- Q: fetch_open_orders 返回什么 ----
    print(f"\n  ► Q{default_type[:2].upper()}1: fetch_open_orders() 不带 symbol")
    safe_call("open_orders", exchange.fetch_open_orders)

    print(f"\n  ► Q{default_type[:2].upper()}2: fetch_open_orders(symbol='SOL/USDC')")
    safe_call("open_orders SOL/USDC", exchange.fetch_open_orders, "SOL/USDC")

    print(f"\n  ► Q{default_type[:2].upper()}3: fetch_open_orders(symbol='USOL/USDC')")
    safe_call("open_orders USOL/USDC", exchange.fetch_open_orders, "USOL/USDC")

    print(f"\n  ► Q{default_type[:2].upper()}4: fetch_open_orders(symbol='SOL/USDC:USDC')  (永续)")
    safe_call("open_orders SOL/USDC:USDC", exchange.fetch_open_orders, "SOL/USDC:USDC")

    # ---- Q: fetch_my_trades ----
    print(f"\n  ► Q{default_type[:2].upper()}5: fetch_my_trades(symbol='SOL/USDC', limit=20)")
    safe_call("trades SOL/USDC", exchange.fetch_my_trades, "SOL/USDC", limit=20)

    print(f"\n  ► Q{default_type[:2].upper()}6: fetch_my_trades(symbol='USOL/USDC', limit=20)")
    safe_call("trades USOL/USDC", exchange.fetch_my_trades, "USOL/USDC", limit=20)

    print(f"\n  ► Q{default_type[:2].upper()}7: fetch_my_trades(symbol='SOL/USDC:USDC', limit=20)  (永续)")
    safe_call("trades SOL/USDC:USDC", exchange.fetch_my_trades, "SOL/USDC:USDC", limit=20)


# =============================================================================
# 4. Hyperliquid 原生 API 对比
# =============================================================================
section("Hyperliquid 原生 /info 端点查询")

INFO_URL = BASE_URL.rstrip("/") + "/info"

# ---- 原生 openOrders ----
subsection("openOrders 端点")
try:
    resp = requests.post(
        INFO_URL,
        json={"type": "openOrders", "user": WALLET_ADDRESS},
        timeout=10,
    )
    data = resp.json()
    print(f"  HTTP {resp.status_code}, 返回 {len(data)} 条挂单")
    if data:
        print(f"  第一条样例:\n    {pformat(data[0], width=140, compact=True)}")
        # 统计 spot/perp 各有多少
        # Hyperliquid 的现货 coin 通常以 "@NNN" 或者 "USOL"/"HYPE" 等形式出现，
        # 永续通常就是 "SOL" 这种简短名
        spot_count = 0
        perp_count = 0
        for o in data:
            coin = str(o.get("coin", ""))
            # 粗略判断：@数字 前缀是现货 token，纯大写短名是永续
            if coin.startswith("@") or coin in ("USOL", "HYPE", "USDH", "PURR"):
                spot_count += 1
            else:
                perp_count += 1
        print(f"  推断: 现货挂单 ≈ {spot_count} 笔, 永续挂单 ≈ {perp_count} 笔")
        print(f"  所有 coin 字段: {sorted(set(str(o.get('coin', '')) for o in data))}")
except Exception as e:
    print(f"  异常: {type(e).__name__}: {e}")

# ---- 原生 userFills（成交历史）----
subsection("userFills 端点（最近 50 笔成交）")
try:
    resp = requests.post(
        INFO_URL,
        json={"type": "userFills", "user": WALLET_ADDRESS},
        timeout=10,
    )
    data = resp.json()
    print(f"  HTTP {resp.status_code}, 返回 {len(data)} 条成交")
    if data:
        print(f"  最近一条样例:\n    {pformat(data[0], width=140, compact=True)}")
        # 按 coin 分组
        from collections import Counter
        coins = Counter(str(f.get("coin", "")) for f in data)
        print(f"  按 coin 分布: {dict(coins)}")
        # 筛出最近 1 小时的 SOL 现货成交
        import time as _time
        now_ms = int(_time.time() * 1000)
        recent_sol_spot = [
            f for f in data
            if (now_ms - int(f.get("time", 0))) < 3600_000  # 1 小时
            and str(f.get("coin", "")).upper() in ("SOL", "USOL", "@107", "@126", "@254")
        ]
        print(f"  最近 1 小时 SOL/USOL 相关成交: {len(recent_sol_spot)} 笔")
        for f in recent_sol_spot[:5]:
            print(f"    {pformat(f, width=140, compact=True)}")
except Exception as e:
    print(f"  异常: {type(e).__name__}: {e}")

# ---- 原生 historicalOrders（最近下的单，包含已撤销/已成交）----
subsection("historicalOrders 端点（如果可用）")
try:
    resp = requests.post(
        INFO_URL,
        json={"type": "historicalOrders", "user": WALLET_ADDRESS},
        timeout=10,
    )
    data = resp.json()
    print(f"  HTTP {resp.status_code}, 返回 {len(data) if isinstance(data, list) else 'N/A'} 条")
    if isinstance(data, list) and data:
        print(f"  最近一条样例:\n    {pformat(data[0], width=140, compact=True)}")
except Exception as e:
    print(f"  异常: {type(e).__name__}: {e}")


# =============================================================================
# 5. 总结判断
# =============================================================================
section("诊断结论模板（根据上面输出填写）")
print("""
请看上面的实际输出，回答以下几个关键问题：

  1. CCXT swap 模式 fetch_open_orders() 里有没有现货挂单？
     - 如果没有 → 确认 Bug: 现货挂单在 swap 模式下不被 CCXT 感知
     - 如果有 → 继续看下一个

  2. 现货 fetch_my_trades 用 SOL/USDC 还是 USOL/USDC 有返回结果？
     - 都返回空 → 确认 Bug: CCXT 对 Hyperliquid 现货成交历史拉取失败
     - 其中一个有效 → 记下正确的 symbol，修改程序里 fetch_my_trades 的调用

  3. Hyperliquid 原生 userFills 返回是否完整？
     - 包含现货和永续的所有成交 → 可作为 fallback：自己按 coin + oid 匹配
     - 用时间戳排序的最近 N 笔是否覆盖本次实盘的 9 笔？

  4. 原生 openOrders 是否能拉到 CCXT 拿不到的现货挂单？
     - 能 → 可以写 fetch_spot_open_orders 走原生 API
""")