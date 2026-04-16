#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CCXT Hyperliquid 私有接口 422 bug 最小复现脚本

目的:
    不依赖项目其它代码，直接用 ccxt.hyperliquid 复现 fetch_balance / fetch_positions /
    fetch_open_orders 的 422 "Failed to deserialize the JSON body" 错误。

用途:
    - 确认当前 CCXT 版本是否仍有该问题
    - 升级 CCXT 后用同一脚本验证是否修复
    - 为 tests/ccxt_422_investigation.md 提供错误特征数据

运行:
    python tests/test_ccxt_422_repro.py
"""

import io
import os
import sys
import traceback
from pathlib import Path

import yaml

# ── Windows UTF-8 终端兼容 ───────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        pass

import ccxt


CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "hyperliquid_config.yaml"


def _sep(title: str = "") -> None:
    line = "=" * 78
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def _load_auth(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    auth = cfg.get("hyperliquid", {}).get("authentication", {})
    return {
        "wallet_address": (auth.get("wallet_address") or "").strip(),
        "private_key": (auth.get("private_key") or "").strip(),
    }


def _describe_exception(e: BaseException) -> None:
    """打印异常的所有可观察特征"""
    print(f"    type:    {type(e).__module__}.{type(e).__name__}")
    print(f"    str:     {e}")

    # ccxt BaseError 子类通常有以下属性
    for attr in ("httpCode", "httpStatusCode", "http_status", "response"):
        if hasattr(e, attr):
            val = getattr(e, attr)
            if val is not None:
                print(f"    {attr}: {repr(val)[:300]}")

    # args 里常常藏着原始响应
    if e.args:
        for i, arg in enumerate(e.args):
            s = repr(arg)
            if len(s) > 500:
                s = s[:500] + "...(truncated)"
            print(f"    args[{i}]: {s}")

    # 完整 traceback 写到 stderr 以便保留定位
    print("    ── traceback ──")
    tb = traceback.format_exception(type(e), e, e.__traceback__)
    for line in tb:
        for sub in line.rstrip().splitlines():
            print(f"      {sub}")


def _try_call(label: str, fn, *args, **kwargs) -> tuple[bool, str]:
    print(f"\n[{label}]")
    try:
        result = fn(*args, **kwargs)
        preview = repr(result)
        if len(preview) > 400:
            preview = preview[:400] + "...(truncated)"
        print(f"    OK  返回预览: {preview}")
        return True, "OK"
    except Exception as e:
        _describe_exception(e)
        return False, f"{type(e).__name__}: {e}"


def main() -> None:
    _sep("CCXT Hyperliquid 422 bug 复现")
    print(f"CCXT 版本: {ccxt.__version__}")
    print(f"CCXT 路径: {os.path.dirname(ccxt.__file__)}")
    print(f"配置文件:  {CONFIG_PATH}")

    auth = _load_auth(CONFIG_PATH)
    if not auth["wallet_address"] or not auth["private_key"]:
        print("配置文件缺少 wallet_address 或 private_key")
        sys.exit(1)
    print(f"wallet:    {auth['wallet_address'][:10]}...{auth['wallet_address'][-4:]}")
    print(f"privkey:   {'*' * 10}...{auth['private_key'][-6:]}")

    # ── 构造 ccxt 实例（尽量贴近项目生产配置） ──────────────────────────
    _sep("初始化 ccxt.hyperliquid 实例")
    exchange = ccxt.hyperliquid({
        "walletAddress": auth["wallet_address"],
        "privateKey": auth["private_key"],
        "enableRateLimit": True,
        "timeout": 30_000,
        "options": {
            "defaultType": "swap",
        },
    })
    # 项目生产侧设置：只拉 spot/swap，避开 hip3
    exchange.options.setdefault("fetchMarkets", {})["types"] = ["spot", "swap"]
    print(f"    defaultType: {exchange.options.get('defaultType')}")
    print(f"    fetchMarkets.types: {exchange.options['fetchMarkets']['types']}")

    _sep("load_markets()")
    ok, _ = _try_call("load_markets", exchange.load_markets)
    if not ok:
        print("\nload_markets 失败，无法继续。")
        sys.exit(2)
    print(f"    已加载 {len(exchange.markets)} 个市场")

    # =========================================================================
    # 三个私有调用：每个独立 try/except
    # =========================================================================
    _sep("私有接口 1: fetch_balance()")
    ok_balance, msg_balance = _try_call("fetch_balance", exchange.fetch_balance)

    _sep("私有接口 2: fetch_positions()")
    ok_positions, msg_positions = _try_call("fetch_positions", exchange.fetch_positions)

    _sep("私有接口 3: fetch_open_orders()")
    ok_open, msg_open = _try_call("fetch_open_orders", exchange.fetch_open_orders)

    # 补充变体: 指定 symbol 的 fetch_open_orders（有时 Hyperliquid endpoint 行为不同）
    _sep("私有接口 3b: fetch_open_orders(symbol='SOL/USDC:USDC')")
    ok_open_sym, msg_open_sym = _try_call(
        "fetch_open_orders(symbol)",
        exchange.fetch_open_orders,
        "SOL/USDC:USDC",
    )

    # 补充: fetch_positions 指定 symbols
    _sep("私有接口 2b: fetch_positions(['SOL/USDC:USDC'])")
    ok_pos_sym, msg_pos_sym = _try_call(
        "fetch_positions(symbols)",
        exchange.fetch_positions,
        ["SOL/USDC:USDC"],
    )

    # =========================================================================
    # 总结
    # =========================================================================
    _sep("总结")
    rows = [
        ("fetch_balance",                        ok_balance,  msg_balance),
        ("fetch_positions",                      ok_positions, msg_positions),
        ("fetch_positions(['SOL/USDC:USDC'])",   ok_pos_sym,  msg_pos_sym),
        ("fetch_open_orders",                    ok_open,     msg_open),
        ("fetch_open_orders('SOL/USDC:USDC')",   ok_open_sym, msg_open_sym),
    ]
    print(f"CCXT 版本: {ccxt.__version__}\n")
    print(f"{'API':45s} {'Result':6s} Note")
    print("-" * 78)
    for name, ok, note in rows:
        tag = "PASS" if ok else "FAIL"
        print(f"{name:45s} {tag:6s} {note[:120]}")

    n_fail = sum(1 for _, ok, _ in rows if not ok)
    if n_fail == 0:
        print("\n所有私有调用通过 — 422 bug 不存在于当前版本")
        sys.exit(0)
    else:
        print(f"\n{n_fail}/{len(rows)} 私有调用失败 — 422 bug 仍存在")
        sys.exit(3)


if __name__ == "__main__":
    main()
