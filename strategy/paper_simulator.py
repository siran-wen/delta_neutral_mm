"""Paper-trading simulator — minimal stand-in for the live order layer.

Phase 1.1 batch 2 part 3 entry. Drives the real strategy loop against
real Lighter market data without touching ``SignerClient`` or sending
any orders to the exchange.

Scope (deliberately tiny):

* ``place_order`` always succeeds and inserts into ``active_orders``.
* ``cancel_order`` always succeeds when the id exists.
* ``tick(market, ts_ms)`` decides whether one of our resting orders
  gets hit. Probability is configurable, and ``random_seed`` makes
  test runs reproducible.
* Fills update an in-memory inventory (size + weighted-average entry).
* No partials, no rejects, no expirations, no queue priority — those
  belong in the real ``lighter_order_manager`` (batch 3).

Why ``market_position``-gated fills
-----------------------------------
``plan_quotes`` already classifies each quote as ``improving`` /
``passive`` / ``would_cross_market``. A ``passive`` quote sits behind
others at the same price and almost never sees a fill in our window;
an ``improving`` quote sets the new BBO and is the natural target for
the next taker. Only ``improving`` quotes are eligible to fill in the
simulator — that matches the dominant fill mechanism we expect on
mainnet and keeps the noise down.
"""

from __future__ import annotations

import logging
import random
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .types import InventoryState, MarketSnapshot

logger = logging.getLogger(__name__)


_ZERO = Decimal(0)


def _D(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class PaperSimulator:
    """In-memory paper-trading book + fill engine."""

    def __init__(
        self,
        initial_collateral_usdc: Decimal = Decimal("2000"),
        fill_probability_per_60s: Decimal = Decimal("0.01"),
        tick_interval_sec: float = 1.0,
        random_seed: Optional[int] = None,
    ):
        # Constants captured at construction so the tick math is stable
        # even if the caller mutates them later.
        self.collateral = _D(initial_collateral_usdc)
        self.fill_probability_per_60s = _D(fill_probability_per_60s)
        self.tick_interval_sec = float(tick_interval_sec)

        self.active_orders: Dict[str, Dict[str, Any]] = {}
        self.fills: List[Dict[str, Any]] = []

        # Inventory: net base + weighted-average entry price. None when
        # flat — flushed once a fill takes us back across zero.
        self.inventory_base: Decimal = _ZERO
        self.inventory_avg_price: Optional[Decimal] = None

        self._rng = random.Random(random_seed)
        self._next_id = 0

        # Per-tick fill probability. Compounds ``fill_probability_per_60s``
        # over the fraction of a minute one tick covers — matches the
        # spec's "60s 1% ⇒ ~0.6 fills per hour at 1s ticks" feel.
        # Using floats here is fine: random.random() returns a float and
        # the per-tick value is only used for one comparison.
        p_60 = float(self.fill_probability_per_60s)
        if p_60 <= 0:
            self._per_tick_p = 0.0
        elif p_60 >= 1:
            self._per_tick_p = 1.0
        else:
            self._per_tick_p = 1.0 - (1.0 - p_60) ** (self.tick_interval_sec / 60.0)

    # ------------------------------------------------------------
    # order management
    # ------------------------------------------------------------

    def place_order(
        self,
        side: str,
        price: Decimal,
        size_base: Decimal,
        market_position: str = "passive",
        tier_target: Optional[str] = None,
        ts_ms: Optional[int] = None,
    ) -> str:
        """Insert a paper order and return its client_order_id.

        ``market_position`` controls fill eligibility — only
        ``"improving"`` orders can be hit by ``tick``. Caller passes
        the value that came back from ``plan_quotes`` so the
        simulator's view stays aligned with the planner.
        """
        if side not in ("buy", "sell"):
            raise ValueError(f"invalid side: {side!r}")

        oid = f"PAPER-{self._next_id:06d}"
        self._next_id += 1
        price_d = _D(price)
        size_d = _D(size_base)
        self.active_orders[oid] = {
            "client_order_id": oid,
            "side": side,
            "price": price_d,
            "size_base": size_d,
            "size_usdc": price_d * size_d,
            "market_position": market_position,
            "tier_target": tier_target,
            "place_ts_ms": ts_ms,
        }
        return oid

    def cancel_order(self, client_order_id: str) -> bool:
        """Drop the order from ``active_orders``. True if it was there."""
        return self.active_orders.pop(client_order_id, None) is not None

    def cancel_all(self) -> int:
        """Drop every resting order. Returns count cancelled."""
        n = len(self.active_orders)
        self.active_orders.clear()
        return n

    # ------------------------------------------------------------
    # fill engine
    # ------------------------------------------------------------

    def tick(self, market: MarketSnapshot, ts_ms: int) -> List[Dict[str, Any]]:
        """One simulation step. Returns any fills produced this tick.

        Single-fill cap per tick keeps the model conservative — even
        in a fast market the paper run won't spuriously empty the
        book in one second.
        """
        improving_oids = [
            oid
            for oid, o in self.active_orders.items()
            if o.get("market_position") == "improving"
        ]
        if not improving_oids or self._per_tick_p <= 0.0:
            return []

        roll = self._rng.random()
        if roll >= self._per_tick_p:
            return []

        oid = self._rng.choice(improving_oids)
        order = self.active_orders.pop(oid)
        fill = {
            "client_order_id": oid,
            "side": order["side"],
            "price": order["price"],
            "size_base": order["size_base"],
            "size_usdc": order["size_usdc"],
            "ts_ms": int(ts_ms),
            "market_index": market.market_index,
            "symbol": market.symbol,
        }
        self.fills.append(fill)
        self._apply_fill(fill)
        return [fill]

    def _apply_fill(self, fill: Dict[str, Any]) -> None:
        """Update ``inventory_base`` and the weighted-average entry price."""
        side = fill["side"]
        size = _D(fill["size_base"])
        price = _D(fill["price"])

        signed_size = size if side == "buy" else -size
        prior_base = self.inventory_base
        new_base = prior_base + signed_size

        # Adding to the existing direction → re-weight average.
        # Reducing past zero → flip direction → average becomes the new fill price.
        # Reducing within direction → keep average.
        if prior_base == 0:
            self.inventory_avg_price = price
        elif (prior_base > 0 and signed_size > 0) or (prior_base < 0 and signed_size < 0):
            old_notional = abs(prior_base) * (
                self.inventory_avg_price if self.inventory_avg_price is not None else _ZERO
            )
            new_notional = size * price
            self.inventory_avg_price = (old_notional + new_notional) / (
                abs(prior_base) + size
            )
        else:
            # Reducing the position. If we crossed zero, the residual
            # came from this fill and sits at this price.
            if (prior_base > 0 and new_base < 0) or (prior_base < 0 and new_base > 0):
                self.inventory_avg_price = price
            elif new_base == 0:
                self.inventory_avg_price = None
            # else: average stays put — partial close at favourable
            # or unfavourable price doesn't change the cost basis.

        self.inventory_base = new_base

    # ------------------------------------------------------------
    # views
    # ------------------------------------------------------------

    def get_inventory_state(
        self,
        mark_price: Optional[Decimal] = None,
    ) -> InventoryState:
        """Return an ``InventoryState`` snapshot for the planner.

        If ``mark_price`` is provided, ``net_delta_usdc`` uses it for a
        live-marked notional; otherwise we fall back to the weighted-
        average entry price so a flat-then-loaded position still has a
        coherent USDC delta. ``mark_price`` is the right input from
        ``plan_quotes`` callers because the skew/cap thresholds
        ultimately measure exposure at the current mid.
        """
        if self.inventory_base == 0:
            net_usdc = _ZERO
        else:
            ref_price = (
                mark_price if mark_price is not None else self.inventory_avg_price
            )
            if ref_price is None:
                net_usdc = _ZERO
            else:
                net_usdc = self.inventory_base * _D(ref_price)
        return InventoryState(
            net_delta_base=self.inventory_base,
            net_delta_usdc=net_usdc,
            avg_entry_price=self.inventory_avg_price,
            open_orders_count=len(self.active_orders),
        )

    def get_summary(self) -> Dict[str, Any]:
        """Diagnostic snapshot for end-of-session logging."""
        return {
            "collateral_start_usdc": str(self.collateral),
            "active_orders": len(self.active_orders),
            "fills_count": len(self.fills),
            "inventory_base": str(self.inventory_base),
            "inventory_avg_price": (
                str(self.inventory_avg_price)
                if self.inventory_avg_price is not None
                else None
            ),
            "fill_probability_per_60s": str(self.fill_probability_per_60s),
            "tick_interval_sec": self.tick_interval_sec,
            "per_tick_p": self._per_tick_p,
        }
