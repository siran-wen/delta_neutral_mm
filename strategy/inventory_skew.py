"""Inventory-aware skew + hard position cap.

Goal: when net delta drifts toward the user's $1k single-side budget,
push the side that would *grow* the position farther from mid (less
likely to fill) and keep the *reducing* side at base distance. Past a
hard cap, drop the growing side entirely.

Pure functions. No exchange calls, no thread state.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional, Tuple

from .types import InventoryState


def compute_skew_offsets(
    inventory: InventoryState,
    target_max_delta_usdc: Decimal,
    skew_max_offset_bp: Decimal,
) -> Tuple[Decimal, Decimal]:
    """Return ``(bid_offset_bp, ask_offset_bp)`` (positive widens away from mid).

    ``skew_factor = clamp(net_delta_usdc / target_max_delta_usdc, -1, 1)``

    * net long  → push bid farther (don't add to long), keep ask at base
    * net short → keep bid at base, push ask farther (don't add to short)
    * flat      → both zero
    """
    if target_max_delta_usdc <= 0:
        return (Decimal(0), Decimal(0))

    raw = inventory.net_delta_usdc / target_max_delta_usdc
    if raw > 1:
        skew_factor = Decimal(1)
    elif raw < -1:
        skew_factor = Decimal(-1)
    else:
        skew_factor = raw

    if skew_factor > 0:
        return (skew_max_offset_bp * skew_factor, Decimal(0))
    if skew_factor < 0:
        return (Decimal(0), skew_max_offset_bp * (-skew_factor))
    return (Decimal(0), Decimal(0))


def is_position_capped(
    inventory: InventoryState,
    hard_cap_usdc: Decimal,
    collateral_usdc: Optional[Decimal] = None,
    hard_cap_pct: Optional[Decimal] = None,
) -> Tuple[bool, bool]:
    """Return ``(skip_bid, skip_ask)``.

    skip_bid is True once net long >= effective_cap (must stop adding longs).
    skip_ask is True once net short <= -effective_cap (must stop adding shorts).
    Boundary is inclusive (>=) so the cap is never breached by a fill.

    Phase 2.1: when both ``collateral_usdc`` and ``hard_cap_pct`` are
    supplied, the effective cap is ``min(hard_cap_usdc, hard_cap_pct *
    collateral_usdc)``. The pct cap protects against the absolute cap
    becoming a too-large fraction of net worth as collateral drifts
    (e.g., losses shrink it). Both caps are active simultaneously; the
    tighter one trips first. Missing collateral or pct → degrade to
    the absolute cap.
    """
    effective_cap = hard_cap_usdc
    if (
        hard_cap_pct is not None
        and collateral_usdc is not None
        and collateral_usdc > 0
    ):
        pct_cap = hard_cap_pct * collateral_usdc
        if pct_cap < effective_cap:
            effective_cap = pct_cap
    skip_bid = inventory.net_delta_usdc >= effective_cap
    skip_ask = inventory.net_delta_usdc <= -effective_cap
    return (skip_bid, skip_ask)
