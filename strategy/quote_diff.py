"""Diff a desired quote set against the currently-active orders.

The planner produces a fresh ``List[Quote]`` every reprice tick. The
order layer (or PaperSimulator) holds the live order set. Naively
"cancel everything, place everything" works but burns through the
exchange rate-limit and fights LPP queue priority — so this module
matches each desired quote against any near-identical active order
and only emits cancels/places for the deltas.

Pure function. No I/O, no side effects. A 0.5bp price tolerance and
5% size tolerance keep the rounding noise from the quantize step in
``plan_quotes`` from causing a churn loop where every tick re-places
the same quote at a price one tick away from where it already is.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Tuple

from .types import Quote


_BP_DENOM = Decimal(10000)


def _price_within_tolerance(
    desired_price: Decimal,
    active_price: Decimal,
    tolerance_bp: Decimal,
) -> bool:
    """True iff ``active_price`` is within ±tolerance_bp of ``desired_price``.

    Uses the desired side as the reference so a 0.5bp tolerance on a
    $100 quote means roughly $0.005 either way regardless of which
    price we use as denominator.
    """
    if desired_price <= 0:
        return False
    drift = abs(active_price - desired_price) / desired_price * _BP_DENOM
    return drift <= tolerance_bp


def _size_within_tolerance(
    desired_size: Decimal,
    active_size: Decimal,
    tolerance_pct: Decimal,
) -> bool:
    """True iff ``active_size`` is within ±tolerance_pct of ``desired_size``.

    ``tolerance_pct`` is a fraction (0.05 = 5%). A desired size of zero
    only matches an active size of zero — preserves "no quote" semantics
    rather than swallowing tiny outliers as zero.
    """
    if desired_size == 0:
        return active_size == 0
    drift = abs(active_size - desired_size) / desired_size
    return drift <= tolerance_pct


def diff_quotes(
    desired: List[Quote],
    active: List[Dict[str, Any]],
    price_tolerance_bp: Decimal = Decimal("0.5"),
    size_tolerance_pct: Decimal = Decimal("0.05"),
) -> Tuple[List[str], List[Quote]]:
    """Compute (orders_to_cancel, quotes_to_place) given current state.

    ``active`` entries are dicts (PaperSimulator's
    ``active_orders.values()`` shape). Each must carry:

        * ``client_order_id`` (str) — the cancel handle
        * ``side`` (str) — "buy" / "sell"
        * ``price`` (Decimal)
        * ``size_base`` (Decimal)

    Matching is greedy per-side: walk desired quotes in order, claim
    the first un-claimed active order on the same side that is within
    both tolerances. Greedy is fine here because ``plan_quotes`` only
    ever emits one bid + one ask, so per-side there is at most one
    candidate to match.
    """
    cancels: List[str] = []
    places: List[Quote] = []

    # Group active by side; preserve insertion order so the match is
    # deterministic when there are multiple candidates (rare —
    # plan_quotes emits 0/1 per side, but a stale order from a
    # previous reprice could still be hanging around).
    active_by_side: Dict[str, List[Dict[str, Any]]] = {"buy": [], "sell": []}
    for o in active:
        side = o.get("side")
        if side in active_by_side:
            active_by_side[side].append(o)

    matched_ids: set = set()

    for q in desired:
        candidates = active_by_side.get(q.side, [])
        match: Dict[str, Any] = None  # type: ignore[assignment]
        for cand in candidates:
            if cand["client_order_id"] in matched_ids:
                continue
            cand_price = Decimal(str(cand["price"]))
            cand_size = Decimal(str(cand["size_base"]))
            if not _price_within_tolerance(q.price, cand_price, price_tolerance_bp):
                continue
            if not _size_within_tolerance(q.size_base, cand_size, size_tolerance_pct):
                continue
            match = cand
            break
        if match is not None:
            matched_ids.add(match["client_order_id"])
        else:
            places.append(q)

    # Anything not claimed needs to go.
    for o in active:
        if o["client_order_id"] not in matched_ids:
            cancels.append(o["client_order_id"])

    return cancels, places
