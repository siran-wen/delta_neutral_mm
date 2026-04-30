"""LPP-aware quote planner.

Given a market snapshot, a session policy and the current inventory,
produce zero, one or two ``Quote`` records describing what the order
layer should have resting on the book.

Design notes
------------
* This is **not** spread capture. The dominant edge is sitting in the
  weighted depth tiers when the LPP score samples; ``tier_target`` is
  the field the upstream layer optimises against.
* BBO-aware: we never let our target price cross the live BBO. If the
  computed target would be marketable, we step inside the BBO by 1
  tick instead, and tag the quote with ``would_cross_market`` so the
  diagnostic layer can see the original intent.
* share-warn: when our notional alone would be >80% of the L1-tier
  depth, the next taker on that side is almost certainly going to hit
  us. We widen by ``share_warn_widen_bp`` to lower fill probability,
  trading some LPP weight for adverse-selection protection.
* Cross-self: a defensive check after BBO-aware. Only trips on
  pathological inputs (mid disagreeing with BBO, distance ≈ 0, etc.)
  but cheap to keep.
* All math is Decimal; floats only ever appear in user-facing logs.
"""

from __future__ import annotations

import logging
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import List, Optional, Tuple

from .inventory_skew import compute_skew_offsets, is_position_capped
from .types import InventoryState, MarketSnapshot, Quote, SessionPolicy

logger = logging.getLogger(__name__)

_ZERO = Decimal(0)
_ONE = Decimal(1)
_BP_DENOM = Decimal(10000)


def _quantize_price(value: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return value.quantize(tick, rounding=rounding)


def _determine_tier(distance_bp: Decimal, thresholds: Tuple[Decimal, Decimal, Decimal]) -> str:
    l1, l2, l3 = thresholds
    if distance_bp <= l1:
        return "L1"
    if distance_bp <= l2:
        return "L2"
    if distance_bp <= l3:
        return "L3"
    return "OUT"


def _coerce_depth(depth_dict: dict, key: str) -> Decimal:
    raw = depth_dict.get(key, 0)
    if isinstance(raw, Decimal):
        return raw
    return Decimal(str(raw))


def plan_quotes(
    market: MarketSnapshot,
    session: SessionPolicy,
    inventory: InventoryState,
    config: dict,
    collateral_usdc: Optional[Decimal] = None,
) -> List[Quote]:
    """Produce 0/1/2 ``Quote`` objects for the current snapshot.

    ``config`` keys (all required):
        target_max_delta_usdc:   Decimal — skew clamp denominator
        skew_max_offset_bp:      Decimal — max bp added by skew
        hard_position_cap_usdc:  Decimal — hard skip threshold (absolute)
        min_market_spread_bp:    Decimal — below = market too tight, return []
        max_market_spread_bp:    Decimal — above = abnormal, return []
        share_warn_threshold:    Decimal — fraction triggering widen
        share_warn_widen_bp:     Decimal — added bp on share warn

    ``config`` optional (Phase 2.1 double cap):
        hard_position_cap_pct:   Decimal — pct of collateral; effective
                                 cap = min(hard_cap_usdc, pct*collateral)
                                 when ``collateral_usdc`` is supplied.

    ``collateral_usdc``: caller may pass the latest account collateral so
    the pct cap activates. ``None`` (or ``hard_position_cap_pct`` absent)
    falls back to the absolute cap behaviour from Phase 1.
    """
    if market.mid <= 0:
        raise ValueError(f"invalid mid price: {market.mid}")

    if session.action == "withdraw":
        logger.info(
            "plan_quotes: empty (session=%s action=withdraw)",
            session.name,
        )
        return []

    if market.spread_bp < config["min_market_spread_bp"]:
        logger.info(
            "plan_quotes: empty (market_spread=%.2fbp < min=%.2fbp) "
            "best_bid=%s best_ask=%s mid=%s mark_price=%s index_price=%s",
            float(market.spread_bp),
            float(config["min_market_spread_bp"]),
            market.best_bid,
            market.best_ask,
            market.mid,
            market.mark_price,
            market.index_price,
        )
        return []
    if market.spread_bp > config["max_market_spread_bp"]:
        logger.info(
            "plan_quotes: empty (market_spread=%.2fbp > max=%.2fbp; market unstable) "
            "best_bid=%s best_ask=%s mid=%s mark_price=%s index_price=%s",
            float(market.spread_bp),
            float(config["max_market_spread_bp"]),
            market.best_bid,
            market.best_ask,
            market.mid,
            market.mark_price,
            market.index_price,
        )
        return []

    base_distance = session.default_distance_bp
    base_size = session.default_size_usdc
    tier_l1, _, _ = session.tier_thresholds_bp

    bid_offset, ask_offset = compute_skew_offsets(
        inventory,
        config["target_max_delta_usdc"],
        config["skew_max_offset_bp"],
    )
    hard_cap_pct_raw = config.get("hard_position_cap_pct")
    hard_cap_pct: Optional[Decimal] = None
    if hard_cap_pct_raw is not None:
        hard_cap_pct = (
            hard_cap_pct_raw
            if isinstance(hard_cap_pct_raw, Decimal)
            else Decimal(str(hard_cap_pct_raw))
        )
    skip_bid, skip_ask = is_position_capped(
        inventory,
        config["hard_position_cap_usdc"],
        collateral_usdc=collateral_usdc,
        hard_cap_pct=hard_cap_pct,
    )

    # share-warn — estimate our share of the top-tier depth bucket
    l1_depth = market.depth_by_spread_bp.get(int(tier_l1), {})
    bid_existing = _coerce_depth(l1_depth, "bid_usdc")
    ask_existing = _coerce_depth(l1_depth, "ask_usdc")

    bid_total = bid_existing + base_size
    ask_total = ask_existing + base_size
    bid_share_est = (base_size / bid_total) if bid_total > 0 else _ZERO
    ask_share_est = (base_size / ask_total) if ask_total > 0 else _ZERO

    share_warn_bid = bid_share_est > config["share_warn_threshold"]
    share_warn_ask = ask_share_est > config["share_warn_threshold"]

    bid_distance = base_distance + bid_offset + (
        config["share_warn_widen_bp"] if share_warn_bid else _ZERO
    )
    ask_distance = base_distance + ask_offset + (
        config["share_warn_widen_bp"] if share_warn_ask else _ZERO
    )

    tick = _ONE / (Decimal(10) ** market.price_decimals)

    target_bid_raw = market.mid * (_ONE - bid_distance / _BP_DENOM)
    target_ask_raw = market.mid * (_ONE + ask_distance / _BP_DENOM)

    target_bid = _quantize_price(target_bid_raw, tick, ROUND_DOWN)
    target_ask = _quantize_price(target_ask_raw, tick, ROUND_UP)

    # BBO-aware adjustment — bid
    if target_bid >= market.best_ask:
        actual_bid = market.best_ask - tick
        bid_position = "would_cross_market"
    elif target_bid > market.best_bid:
        actual_bid = target_bid
        bid_position = "improving"
    else:
        actual_bid = target_bid
        bid_position = "passive"

    # BBO-aware adjustment — ask
    if target_ask <= market.best_bid:
        actual_ask = market.best_bid + tick
        ask_position = "would_cross_market"
    elif target_ask < market.best_ask:
        actual_ask = target_ask
        ask_position = "improving"
    else:
        actual_ask = target_ask
        ask_position = "passive"

    # cross-self defense (rare; trips on distance≈0 or oracle/BBO disagreement)
    if actual_bid >= actual_ask:
        mid_actual = (actual_bid + actual_ask) / Decimal(2)
        actual_bid = _quantize_price(mid_actual - tick / Decimal(2), tick, ROUND_DOWN)
        actual_ask = _quantize_price(mid_actual + tick / Decimal(2), tick, ROUND_UP)
        if actual_ask - actual_bid < tick:
            actual_ask = actual_bid + tick

    actual_bid_distance_bp = (market.mid - actual_bid) / market.mid * _BP_DENOM
    actual_ask_distance_bp = (actual_ask - market.mid) / market.mid * _BP_DENOM

    bid_tier = _determine_tier(actual_bid_distance_bp, session.tier_thresholds_bp)
    ask_tier = _determine_tier(actual_ask_distance_bp, session.tier_thresholds_bp)

    size_step = _ONE / (Decimal(10) ** market.size_decimals)
    bid_size_base = (base_size / actual_bid).quantize(size_step, rounding=ROUND_DOWN)
    ask_size_base = (base_size / actual_ask).quantize(size_step, rounding=ROUND_DOWN)
    bid_size_usdc = bid_size_base * actual_bid
    ask_size_usdc = ask_size_base * actual_ask

    bid_notes: List[str] = []
    if share_warn_bid:
        bid_notes.append("share_warn")
    if bid_offset > 0:
        bid_notes.append("skew_long")
    if bid_position == "would_cross_market":
        bid_notes.append("crossed_market")

    ask_notes: List[str] = []
    if share_warn_ask:
        ask_notes.append("share_warn")
    if ask_offset > 0:
        ask_notes.append("skew_short")
    if ask_position == "would_cross_market":
        ask_notes.append("crossed_market")

    quotes: List[Quote] = []

    if skip_bid:
        logger.info(
            "plan_quotes: bid skipped (long inventory %s > hard_cap %s)",
            inventory.net_delta_usdc,
            config["hard_position_cap_usdc"],
        )
    else:
        if bid_size_base > 0:
            quotes.append(Quote(
                side="buy",
                price=actual_bid,
                size_base=bid_size_base,
                size_usdc=bid_size_usdc,
                distance_from_mid_bp=actual_bid_distance_bp,
                tier_target=bid_tier,
                market_position=bid_position,
                notes=tuple(bid_notes),
            ))
        else:
            logger.warning(
                "bid size quantized to 0 for %s: base_size=%s, price=%s — skipping",
                market.symbol, base_size, actual_bid,
            )

    if skip_ask:
        logger.info(
            "plan_quotes: ask skipped (short inventory %s < -%s)",
            inventory.net_delta_usdc,
            config["hard_position_cap_usdc"],
        )
    else:
        if ask_size_base > 0:
            quotes.append(Quote(
                side="sell",
                price=actual_ask,
                size_base=ask_size_base,
                size_usdc=ask_size_usdc,
                distance_from_mid_bp=actual_ask_distance_bp,
                tier_target=ask_tier,
                market_position=ask_position,
                notes=tuple(ask_notes),
            ))
        else:
            logger.warning(
                "ask size quantized to 0 for %s: base_size=%s, price=%s — skipping",
                market.symbol, base_size, actual_ask,
            )

    # Decision summary — debug level so production -v prints it but
    # default (WARNING+) stays quiet. Lazy formatting via %s; the
    # float() casts on bp values are cheap and only evaluated when
    # the handler emits.
    logger.debug(
        "plan_quotes: %d quotes session=%s mid=%s spread=%.2fbp inv=%s "
        "skew_offset=(%s,%s) share_warn=(%s,%s)",
        len(quotes),
        session.name,
        market.mid,
        float(market.spread_bp),
        inventory.net_delta_usdc,
        bid_offset,
        ask_offset,
        share_warn_bid,
        share_warn_ask,
    )
    for q in quotes:
        logger.debug(
            "plan_quotes: %s @%s size=%s tier=%s pos=%s notes=%s",
            q.side,
            q.price,
            q.size_base,
            q.tier_target,
            q.market_position,
            q.notes,
        )

    return quotes
