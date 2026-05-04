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


def _apply_bbo_track(
    actual_bid: Decimal,
    actual_ask: Decimal,
    market: MarketSnapshot,
    mode: str,
    max_distance_bp: Decimal,
) -> Tuple[Decimal, Decimal]:
    """Pull resting prices toward BBO when the static distance overshoots.

    Day 1 production data showed the fixed ``default_distance_bp=30`` left
    quotes ~17 bp behind a 13-15 bp market spread on SAMSUNG, dragging
    share to 2.3%. ``passive`` mode pulls the quote up to ``best_bid``
    (or down to ``best_ask`` for the sell side) whenever the planner's
    target sits more than ``max_distance_bp`` outside the current BBO,
    keeping us at the front of the queue while still resting as a maker.

    Modes:
        ``"off"``     — no adjustment (legacy behaviour).
        ``"passive"`` — if the planner placed a side farther than
                        ``max_distance_bp`` from the corresponding BBO,
                        pull it onto the BBO. ``actual_bid > best_bid``
                        (improving) is left alone — the BBO-aware block
                        already validated that, and re-pulling would
                        widen us back out unnecessarily.
    """
    if mode == "off":
        return actual_bid, actual_ask
    if mode != "passive":
        # Unknown mode — silently no-op rather than crashing the
        # planner. The startup config echo logs the active mode, so a
        # typo is visible there.
        return actual_bid, actual_ask

    max_dist = market.mid * max_distance_bp / _BP_DENOM

    bid_floor = market.best_bid - max_dist
    if actual_bid < bid_floor:
        actual_bid = market.best_bid

    ask_ceiling = market.best_ask + max_dist
    if actual_ask > ask_ceiling:
        actual_ask = market.best_ask

    return actual_bid, actual_ask


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
        # Defensive: pass the per-side size so the cap check projects
        # post-fill inventory rather than firing only after a breach.
        # 5-2 SKHYNIX hit inv=$148 against $100 cap because the old
        # check only tripped at inv >= cap, but inv=$98 + $50 fill
        # landed at $148. See is_position_capped docstring for the
        # post-fill projection logic.
        size_per_side_usdc=base_size,
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

    # Phase 3: pull static-distance quotes onto BBO when the configured
    # ``default_distance_bp`` would otherwise leave us further than
    # ``bbo_track_max_distance_bp`` behind the live BBO. ``mode="off"``
    # restores the pre-Phase-3 fixed-distance behaviour exactly.
    bbo_track_mode = str(config.get("bbo_track_mode", "off"))
    if bbo_track_mode != "off":
        bbo_track_max_dist_bp = Decimal(str(
            config.get("bbo_track_max_distance_bp", 5)
        ))
        actual_bid, actual_ask = _apply_bbo_track(
            actual_bid,
            actual_ask,
            market,
            bbo_track_mode,
            bbo_track_max_dist_bp,
        )

    # P9 (5-4): asymmetric BBO quoting. When abs(inv) breaches
    # ``asymmetric_quote_trigger_pct * effective_cap``, both legs stay
    # post_only (no IOC, no taker, no fail/disable counters), but the
    # close-side leg sits aggressively near BBO and the anti-side leg
    # widens to ``asymmetric_anti_distance_bp`` from mid. Net effect:
    # we keep collecting maker rewards on both sides while the close
    # leg gets queue priority for any natural taker that walks in.
    #
    # P6/P7/P8 IOC and post_only-emergency paths are abandoned —
    # Day-3 SAMSUNG (5-3 → 5-4 UTC) showed Lighter's account-level
    # self-trade protection silently rejects IOC reduce_only, and
    # P7's post_only-with-timeout-disable left $396 SAMSUNG long
    # unhedged for 2h with single-sided quoting (lost reward
    # window). Asymmetric quoting trusts user-driven manual close
    # for cleanup; the cap + skew + this asymmetric layer cover
    # the bounded-tail-risk case.
    asymmetric_active = False
    asymmetric_close_side: Optional[str] = None
    if bool(config.get("asymmetric_quote_enabled", False)):
        trigger_pct_raw = config.get("asymmetric_quote_trigger_pct", "0.7")
        trigger_pct = (
            trigger_pct_raw
            if isinstance(trigger_pct_raw, Decimal)
            else Decimal(str(trigger_pct_raw))
        )
        effective_cap = Decimal(str(config["hard_position_cap_usdc"]))
        if (
            hard_cap_pct is not None
            and collateral_usdc is not None
            and collateral_usdc > 0
        ):
            pct_cap = hard_cap_pct * collateral_usdc
            if pct_cap < effective_cap:
                effective_cap = pct_cap
        trigger_threshold = effective_cap * trigger_pct
        if abs(inventory.net_delta_usdc) >= trigger_threshold:
            asymmetric_active = True
            close_mode = str(
                config.get("asymmetric_close_mode", "improve_bbo")
            )
            anti_distance_bp_raw = config.get(
                "asymmetric_anti_distance_bp", "30"
            )
            anti_distance_bp = (
                anti_distance_bp_raw
                if isinstance(anti_distance_bp_raw, Decimal)
                else Decimal(str(anti_distance_bp_raw))
            )
            if inventory.net_delta_usdc > 0:
                # Long: sell-side is the close leg, buy-side is anti.
                asymmetric_close_side = "sell"
                if close_mode == "match_bbo":
                    # Join the existing top-of-book ask queue.
                    actual_ask = market.best_ask
                    ask_position = "passive"
                else:
                    # ``improve_bbo`` (default): become the new
                    # top-of-book ask. Cross-self defense below
                    # catches the rare case where best_bid + tick
                    # lands at-or-above best_ask (1-tick spread).
                    actual_ask = market.best_bid + tick
                    ask_position = "improving"
                # Anti (buy) sits ``anti_distance_bp`` from mid —
                # wider than the symmetric default to dampen fill
                # probability without dropping the leg entirely.
                anti_bid_raw = market.mid * (
                    _ONE - anti_distance_bp / _BP_DENOM
                )
                actual_bid = _quantize_price(
                    anti_bid_raw, tick, ROUND_DOWN
                )
                bid_position = (
                    "improving"
                    if actual_bid > market.best_bid
                    else "passive"
                )
            else:
                # Short: buy-side is the close leg, sell-side is anti.
                asymmetric_close_side = "buy"
                if close_mode == "match_bbo":
                    actual_bid = market.best_bid
                    bid_position = "passive"
                else:
                    actual_bid = market.best_ask - tick
                    bid_position = "improving"
                anti_ask_raw = market.mid * (
                    _ONE + anti_distance_bp / _BP_DENOM
                )
                actual_ask = _quantize_price(
                    anti_ask_raw, tick, ROUND_UP
                )
                ask_position = (
                    "improving"
                    if actual_ask < market.best_ask
                    else "passive"
                )
            logger.info(
                "plan_quotes: asymmetric_quote active inv=%s "
                "trigger=%s close_side=%s mode=%s anti_dist_bp=%s",
                inventory.net_delta_usdc,
                trigger_threshold,
                asymmetric_close_side,
                close_mode,
                anti_distance_bp,
            )

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
    if asymmetric_active:
        bid_notes.append(
            "asymmetric_close" if asymmetric_close_side == "buy"
            else "asymmetric_anti"
        )

    ask_notes: List[str] = []
    if share_warn_ask:
        ask_notes.append("share_warn")
    if ask_offset > 0:
        ask_notes.append("skew_short")
    if ask_position == "would_cross_market":
        ask_notes.append("crossed_market")
    if asymmetric_active:
        ask_notes.append(
            "asymmetric_close" if asymmetric_close_side == "sell"
            else "asymmetric_anti"
        )

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
