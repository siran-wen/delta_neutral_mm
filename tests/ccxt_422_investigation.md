# Hyperliquid `/info` 422 错误根因调研

**结论速读：这从一开始就不是 CCXT 的 bug，是 `config/hyperliquid_config.yaml` 里的
`wallet_address` 字段多打了一个字符（41 hex chars，合法以太坊地址必须是 40 hex
chars）。修正配置后，CCXT 4.5.22 的所有私有接口在**未升级**的情况下直接可用。**

---

## 背景

项目代码和文档中有三处标注"CCXT 4.5.x 对 Hyperliquid `/info` POST 的序列化 bug"：

| 位置 | 行号 | 行为 |
|------|------|------|
| `gateways/gateway.py` | `connect()` 中 `fetch_balance` 异常分支 | warning 日志后继续标记 AUTHENTICATED |
| `execution/order_manager.py` | reconcile 循环中 `fetch_open_orders` 异常分支 | 降级为 debug 日志 |
| `execution/order_manager.py` | `recover_from_exchange()` 中 `fetch_open_orders` 异常分支 | warning 后直接 return |

异常表现统一为：
```
ExchangeError: hyperliquid POST https://api.hyperliquid.xyz/info
422 Unprocessable Entity Failed to deserialize the JSON body into the target type
```

---

## Step 1: 环境信息

```
ccxt version: 4.5.22
Location:     C:\Users\10358\AppData\Local\Programs\Python\Python312\Lib\site-packages
Python:       3.12
OS:           Windows 11
```

## Step 2: 最小复现 + 根因定位

脚本：`tests/test_ccxt_422_repro.py`

### 2.1 初次复现 — 5/5 私有调用全部 422

```
fetch_balance                                 FAIL  422 Failed to deserialize ...
fetch_positions                               FAIL  422 Failed to deserialize ...
fetch_positions(['SOL/USDC:USDC'])            FAIL  422 Failed to deserialize ...
fetch_open_orders                             FAIL  422 Failed to deserialize ...
fetch_open_orders('SOL/USDC:USDC')            FAIL  422 Failed to deserialize ...
```

### 2.2 抓取 CCXT 实际发出的请求 body

Monkey-patch `exchange.fetch` 打印所有出站请求：

```
POST https://api.hyperliquid.xyz/info
body: {"type":"clearinghouseState","user":"0x<ADDR>"}   # <ADDR> 来自配置
```

请求结构完全符合 Hyperliquid 官方 `/info` 规范，签名路径（`/exchange`）所有调用
都 **200 OK**（如 `approveBuilderFee`、`setReferrer`），说明签名是正确的。

### 2.3 关键观察 — 直接 curl 也 422

用 `urllib.request` 绕过 CCXT，直接 POST 相同 body：

| 请求 | 结果 |
|------|------|
| `{"type":"allMids"}` (无鉴权) | **200 OK** |
| `{"type":"clearinghouseState","user":"<BAD_ADDR_41_hex>"}` | **422** |
| `{"type":"userState","user":"<BAD_ADDR_41_hex>"}` | **422** |
| `{"type":"spotClearinghouseState","user":"<BAD_ADDR_41_hex>"}` | **422** |
| `{"type":"frontendOpenOrders","user":"<BAD_ADDR_41_hex>"}` | **422** |

所有带"41 hex chars 的 user 字段"请求都 422。且 lowercase 和 checksummed 两种写法都 422。

### 2.4 根因定位 — 钱包地址长度异常

`config/hyperliquid_config.yaml` 中 `wallet_address` 字段：

```
0x<...41 hex chars...>   # 配置里的错误值，末尾多了一个字符
  |-------- 41 hex chars (非法) --------|
```

以太坊地址规范要求 20 字节 = 40 hex chars，配置中多了末尾一个 `E`。

用私钥派生出正确地址进行对照：

```python
from eth_account import Account
acct = Account.from_key(cfg["hyperliquid"]["authentication"]["private_key"])
# acct.address 为 0x<40 hex chars>（正确），与配置中 41 hex 的版本不相等
```

### 2.5 用正确地址直接 POST `/info` → 200 OK

把配置里的 `wallet_address` 改为私钥派生的 40 hex 正确版本后：

```bash
body: {"type":"clearinghouseState","user":"0x<CORRECT_ADDR_40_hex>"}
响应: 200 OK
  {"marginSummary":{"accountValue":"<N>.xxx", ...},
   "assetPositions":[], ...}
```

---

## 为什么误诊为 "CCXT bug"?

1. **错误消息误导**："Failed to deserialize the JSON body" 听起来像是客户端 SDK
   构造的 JSON 格式有问题，但实际上 Hyperliquid 用这个消息涵盖了它的 serde 层
   任何反序列化失败（包括 `user` 字段校验失败在内）。
2. **CCXT 4.5.22 发布时间较近**，恰好上线后就看到 422，容易归咎于版本回归。
3. **对应代码是"曾经真的有过 CCXT bug"**：历史上 CCXT 确实有过几次 Hyperliquid
   fetchBalance 的修复（见下方 changelog 摘要）。但那批 issue 与当前 422
   的表现不同——当前 422 是服务端拒绝，历史 bug 是客户端参数缺失/格式错误。
4. **公开接口正常**：`load_markets` / `fetch_ticker` / `fetch_order_book` 都
   200，让人感觉"就是私有接口在 CCXT 里有问题"。

---

## Step 3 原计划的 CCXT issue 调研

由于 Step 2 已经定位到根因不在 CCXT，不再需要升级 CCXT。以下仍保留一次轻度
调研，确认"当前 4.5.22 不存在与此症状相关的未修复 bug"：

**CCXT 当前最新版本**（截至 2026-04-16）：约 4.5.x 系列，无重大 Hyperliquid
fetchBalance/fetchPositions 回归 issue。

历史相关 PR/issue（示意）：
- Hyperliquid fetchBalance 参数修复（2024 年若干 PR），已合并进 4.2+
- HIP-3 DEX 支持（4.5.x 新增），需要 `options.fetchMarkets.types=['spot','swap']`
  手动过滤（项目已通过 `gateway.py` 第 836 行生效）

项目生产侧已经规避 HIP-3 问题，没有观察到其他 Hyperliquid 相关未修复 bug。

---

## Step 4 结论 — 不适用情况 A / B，归为"情况 C：并非 CCXT 问题"

- **不需要升级 CCXT**。4.5.22 当前完全可用。
- **不需要切换到 Hyperliquid 官方 SDK**（Step 5 不执行）。
- **修复措施**：`config/hyperliquid_config.yaml` 的 `wallet_address` 去掉末尾
  多出来的那一位字符，使总长度为 `0x` + 40 hex chars = 42。修改后用
  `python tests/test_ccxt_422_repro.py` 复测,5/5 私有调用全 PASS。

---

## Step 6 — 三处 workaround 应清理的理由

| 位置 | 原因变更 |
|------|---------|
| `gateways/gateway.py` `connect()` | `fetch_balance` 现在会真正返回余额，任何异常都是真异常，应设置 ERROR 状态而非静默 AUTHENTICATED |
| `execution/order_manager.py` reconcile | `fetch_open_orders` 现在稳定返回列表，拉取失败应抬高为 warning 而非 debug |
| `execution/order_manager.py` `recover_from_exchange` | 冷启动拉不到挂单意味着对账基线丢失，应直接抛出阻止启动 |

见主任务 Step 6 的具体改法。

---

## 历史教训（Feedback）

**不要用宽容的 try/except 把不熟悉的错误"绕过去"。**

原 workaround 是出于"CCXT 版本可能有 bug"的善意猜测，但它同时也把真实的
"配置打错了"这种易纠正问题藏了起来，直到双腿对冲架构引入了对
`fetch_positions` 的硬依赖才暴露。

正确的做法：出现陌生错误先抓请求 body、绕过封装直接复现，再下结论是 SDK 还是配置。
