"""Shared frozen dataclasses for the strategy layer.

These types form the contract between the read-side observers
(market data, inventory, session) and the write-side planner
(quote_planner). Keeping them frozen + Decimal-typed makes the
planner trivially deterministic and testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class Quote:
    """A single quote the planner wants the order layer to place."""

    side: str                          # "buy" / "sell"
    price: Decimal                     # quantized to price_decimals
    size_base: Decimal                 # base-asset quantity, quantized to size_decimals
    size_usdc: Decimal                 # notional after quantization (size_base * price)
    distance_from_mid_bp: Decimal      # actual distance after quantization
    tier_target: str                   # "L1" / "L2" / "L3" / "OUT"
    market_position: str               # "improving" / "passive" / "would_cross_market"
    notes: tuple                       # decision trail: ("share_warn", "skew_long", ...)


@dataclass(frozen=True)
class MarketSnapshot:
    """A point-in-time view of one market's BBO and depth."""

    symbol: str
    market_index: int
    mid: Decimal
    mark_price: Optional[Decimal]
    index_price: Optional[Decimal]
    best_bid: Decimal
    best_ask: Decimal
    spread_bp: Decimal
    # depth_by_spread_bp[tier_bp] = {"bid_usdc": Decimal, "ask_usdc": Decimal, "total_usdc": Decimal}
    depth_by_spread_bp: Dict[int, Dict[str, Decimal]]
    price_decimals: int
    size_decimals: int
    ts_ms: int


@dataclass(frozen=True)
class InventoryState:
    """Net delta + open-order count seen by the planner."""

    net_delta_base: Decimal
    net_delta_usdc: Decimal
    avg_entry_price: Optional[Decimal]
    open_orders_count: int


@dataclass(frozen=True)
class SessionState:
    """A session-aware quoting policy slice."""

    name: str                          # e.g. "KR_MARKET_HOURS_AM"
    action: str                        # "quote" / "withdraw"
    default_distance_bp: Decimal       # one-sided distance from mid
    default_size_usdc: Decimal         # one-sided notional
    tier_thresholds_bp: Tuple[Decimal, Decimal, Decimal]  # (L1, L2, L3)
    reason: str                        # diagnostic / log message
