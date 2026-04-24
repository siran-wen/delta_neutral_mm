# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta-neutral market making framework for the Hyperliquid exchange. The infrastructure layer (gateway, order management, risk controls, connection resilience) is complete; the trading strategy and ML service are not yet implemented.

## Setup & Commands

```bash
# Install dependencies
pip install -r utils/requirements.txt

# Run connectivity tests against Hyperliquid
python utils/test_hyperliquid_connectivity.py --config config/hyperliquid_config.yaml
```

No build step, Makefile, or test runner — pure Python 3.7+. Dependencies: `ccxt>=4.0.0`, `eth-account>=0.9.0`, `PyYAML>=6.0`.

## Architecture

The system is a layered stack where each layer depends only on the one below it:

```
Strategy (not yet implemented)
        ↓
RiskManager  →  pre_trade_check() gate before every order
        ↓
OrderManager  →  order lifecycle state machine, rate limiting, reconciliation
        ↓
ConnectionMonitor  →  heartbeat, auto-reconnect, emergency cancel on disconnect
        ↓
Gateway (HyperliquidGateway via CCXT)  →  exchange abstraction
```

### Key modules

- **`gateways/gateway.py`** — `BaseGateway` abstract interface + `HyperliquidGateway` (CCXT-based). `GatewayFactory` creates instances from YAML config. Event-driven via `.on()` callbacks.
- **`gateways/exception_handler.py`** — `ConnectionMonitor` runs a heartbeat thread. On disconnect: cancels all open orders, then reconnects with exponential backoff (1s→60s). Classifies errors as TRANSIENT (retry), RATE_LIMITED (wait), or FATAL (give up). Cold-start recovery re-syncs orders after reconnect.
- **`execution/order_manager.py`** — Tracks every order through a state machine (`PENDING_NEW → OPEN → PARTIALLY_FILLED → FILLED | CANCELLED | REJECTED | LOST | STALE`). Token-bucket rate limiter with priority queue (cancel > hedge > new order > query). Background reconciliation thread syncs local state with exchange. Latency tracker (sliding window, P95).
- **`execution/inventory.py`** — `InventoryTracker`: thread-safe local position tracking (net delta, weighted-average entry price, unrealized PnL). Supports `sync_from_positions()` for startup recovery.
- **`execution/quoter.py`** — `Quoter`: bid/ask price and size calculation from mid-price and strategy parameters. Requote threshold check.
- **`risk/pre_trade.py`** — Three components unified by `RiskManager`:
  - `PositionLimiter`: per-symbol and global delta limits; triggers hedger callback on breach.
  - `FatFingerGuard`: rejects orders deviating too far from mid-price (% and absolute thresholds).
  - `KillSwitch`: intercepts SIGINT/SIGTERM/uncaught exceptions → cancels all orders → flattens positions → shuts down.

### Threading model

All core components are thread-safe. `OrderManager` uses per-order RLocks plus a global lock for the order book. `ConnectionMonitor` and reconciliation run in background daemon threads. `KillSwitch` is idempotent (executes shutdown sequence exactly once).

### Configuration

`config/hyperliquid_config.yaml` defines API endpoints, authentication (private key / wallet address), per-symbol precision, leverage, rate limits, WebSocket settings, and fee structure. The gateway factory reads this directly.

### Initialization flow

```python
from gateways import GatewayFactory, ConnectionMonitor, ReconnectPolicy
from execution import OrderManager, InventoryTracker, Quoter
from risk import RiskManager, RiskConfig

gateway = GatewayFactory.create("config/hyperliquid_config.yaml")
gateway.connect()
om = OrderManager(gateway, reconcile_interval=3.0)
om.start()
monitor = ConnectionMonitor(gateway, om, policy=ReconnectPolicy())
monitor.start()
rm = RiskManager(gateway=gateway, order_manager=om, connection_monitor=monitor, config=RiskConfig())
rm.arm()

inventory = InventoryTracker()
inventory.sync_from_positions(gateway.fetch_positions())
rm.sync_positions(inventory.get_all_positions())
```

Shutdown: `rm.disarm() → monitor.stop() → om.stop() → gateway.disconnect()`.
