"""Pure functions for converting raw order-book and trade-tape data
into the per-snapshot summaries the observer writes to JSONL.

Two operations live here:

* ``aggregate_depth_by_spread_bp`` — given price/size lists and a mid,
  compute cumulative USDC notional present within ``≤ tier`` basis
  points of mid, separately per side. Inclusive on the tier upper
  bound (``≤ 15bp`` includes a level priced exactly at 15bp from mid).

* ``aggregate_trades_window`` — given a list of normalized trade
  records (from ``LighterWebSocket._normalize_trade``) plus a
  [start, end] millisecond window, compute count/buy/sell/VWAP.

Both are pure (no I/O, no globals) so they're trivial to unit-test.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# Default spread tiers (bp from mid). Chosen to cover the LPP regular
# tier table (5/10/20) and the RWA weekend tier table (50/100/200).
DEFAULT_DEPTH_TIERS_BP: Tuple[Decimal, ...] = (
    Decimal("5"),
    Decimal("10"),
    Decimal("15"),
    Decimal("25"),
    Decimal("50"),
    Decimal("100"),
    Decimal("200"),
)

# Top-N raw levels persisted per snapshot, per side. Keep small to
# avoid unbounded growth of the JSONL.
DEFAULT_TOP_LEVELS = 5


def _coerce_level(lvl: Any) -> Optional[Tuple[Decimal, Decimal]]:
    """Best-effort normalize a (price, size) pair from list/tuple/dict."""
    if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
        try:
            price = Decimal(str(lvl[0]))
            size = Decimal(str(lvl[1]))
        except (TypeError, ValueError, ArithmeticError):
            return None
    elif isinstance(lvl, dict):
        try:
            price = Decimal(str(lvl.get("price")))
            raw_size = lvl.get("size") or lvl.get("remaining_base_amount") or lvl.get("amount")
            size = Decimal(str(raw_size))
        except (TypeError, ValueError, ArithmeticError):
            return None
    else:
        return None
    if price <= 0 or size <= 0:
        return None
    return price, size


def aggregate_depth_by_spread_bp(
    bids: Sequence[Any],
    asks: Sequence[Any],
    mid: Optional[Decimal],
    tiers_bp: Sequence[Decimal] = DEFAULT_DEPTH_TIERS_BP,
) -> Dict[str, Dict[str, Decimal]]:
    """Compute cumulative USDC notional within each spread tier.

    ``bids`` / ``asks`` are lists of (price, size_base) tuples (or
    list-of-list, or list-of-dict with 'price'/'size' keys — see
    ``_coerce_level``). ``mid`` is the reference mid used for the
    tier comparison (typically ``(best_bid + best_ask) / 2``).

    Returns a dict keyed by tier (string form, e.g. ``"15"``) with
    the structure::

        {
          "15": {
            "bid_usdc": Decimal,
            "ask_usdc": Decimal,
            "total_usdc": Decimal,
          },
          ...
        }

    Empty book / missing mid → all tiers report 0 (no exception).
    """
    out: Dict[str, Dict[str, Decimal]] = {}
    for tier in tiers_bp:
        key = _tier_key(tier)
        out[key] = {
            "bid_usdc": Decimal("0"),
            "ask_usdc": Decimal("0"),
            "total_usdc": Decimal("0"),
        }
    if mid is None or mid <= 0:
        return out

    # Sort tiers ascending so we can short-circuit the per-side scan
    # once a level falls outside the largest tier.
    sorted_tiers = sorted(((Decimal(str(t)), _tier_key(t)) for t in tiers_bp))
    if not sorted_tiers:
        return out
    largest_tier = sorted_tiers[-1][0]

    # Bid side: walk from best bid down. Bid distance = (mid - price)/mid * 1e4.
    # A bid priced exactly at mid is at 0bp; a bid priced *above* mid means
    # the book is crossed relative to the caller's reference mid (stale mid
    # or genuinely crossed book) and is excluded from the tier roll-up — it
    # would otherwise pollute "depth within X bp" with off-side liquidity.
    for lvl in bids:
        coerced = _coerce_level(lvl)
        if coerced is None:
            continue
        price, size = coerced
        if price > mid:
            continue
        if price == mid:
            distance_bp = Decimal("0")
        else:
            distance_bp = ((mid - price) / mid) * Decimal("10000")
        if distance_bp > largest_tier:
            # Sorted books → no further bid will be inside the band.
            break
        notional = price * size
        for tier_val, key in sorted_tiers:
            if distance_bp <= tier_val:
                bucket = out[key]
                bucket["bid_usdc"] += notional
                bucket["total_usdc"] += notional

    # Ask side: walk from best ask up. Symmetric to bids — asks priced
    # below the reference mid are excluded.
    for lvl in asks:
        coerced = _coerce_level(lvl)
        if coerced is None:
            continue
        price, size = coerced
        if price < mid:
            continue
        if price == mid:
            distance_bp = Decimal("0")
        else:
            distance_bp = ((price - mid) / mid) * Decimal("10000")
        if distance_bp > largest_tier:
            break
        notional = price * size
        for tier_val, key in sorted_tiers:
            if distance_bp <= tier_val:
                bucket = out[key]
                bucket["ask_usdc"] += notional
                bucket["total_usdc"] += notional

    return out


def top_levels(
    levels: Sequence[Any],
    n: int = DEFAULT_TOP_LEVELS,
) -> List[Dict[str, str]]:
    """Return the top-N levels as list of ``{price, size_base, size_usdc}`` strings.

    Order is preserved (caller is responsible for passing best-first).
    Skips malformed entries silently.
    """
    out: List[Dict[str, str]] = []
    for lvl in levels:
        if len(out) >= n:
            break
        coerced = _coerce_level(lvl)
        if coerced is None:
            continue
        price, size = coerced
        out.append(
            {
                "price": str(price),
                "size_base": str(size),
                "size_usdc": str(price * size),
            }
        )
    return out


def aggregate_trades_window(
    trades: Iterable[Dict[str, Any]],
    start_ts_ms: int,
    end_ts_ms: int,
) -> Dict[str, Any]:
    """Aggregate trades whose ``ts_ms`` is in [start_ts_ms, end_ts_ms].

    Returns count/buy/sell counts, buy_volume_usdc, sell_volume_usdc,
    VWAP (None if total volume is zero), and first/last trade ts.
    Side classification: ``side == "buy"`` ⇒ buy_count and
    buy_volume_usdc. Trades with no side (e.g. liquidations on some
    feeds) are counted in ``count`` but excluded from buy/sell tallies.
    """
    count = 0
    buy_count = 0
    sell_count = 0
    buy_vol = Decimal("0")
    sell_vol = Decimal("0")
    vwap_num = Decimal("0")
    vwap_den = Decimal("0")
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    for t in trades:
        ts = t.get("ts_ms")
        if ts is None:
            continue
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < start_ts_ms or ts > end_ts_ms:
            continue

        price = t.get("price")
        size = t.get("size")
        if not isinstance(price, Decimal):
            try:
                price = Decimal(str(price)) if price is not None else None
            except (TypeError, ValueError, ArithmeticError):
                price = None
        if not isinstance(size, Decimal):
            try:
                size = Decimal(str(size)) if size is not None else None
            except (TypeError, ValueError, ArithmeticError):
                size = None
        if price is None or size is None or price <= 0 or size <= 0:
            continue

        usd_amount = t.get("usd_amount")
        if isinstance(usd_amount, Decimal):
            notional = usd_amount
        else:
            try:
                notional = Decimal(str(usd_amount)) if usd_amount is not None else price * size
            except (TypeError, ValueError, ArithmeticError):
                notional = price * size
        if notional <= 0:
            notional = price * size

        count += 1
        if first_ts is None or ts < first_ts:
            first_ts = ts
        if last_ts is None or ts > last_ts:
            last_ts = ts

        side = t.get("side")
        if side == "buy":
            buy_count += 1
            buy_vol += notional
        elif side == "sell":
            sell_count += 1
            sell_vol += notional

        vwap_num += price * size
        vwap_den += size

    vwap = vwap_num / vwap_den if vwap_den > 0 else None

    return {
        "count": count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buy_volume_usdc": buy_vol,
        "sell_volume_usdc": sell_vol,
        "vwap": vwap,
        "first_trade_ts_ms": first_ts,
        "last_trade_ts_ms": last_ts,
    }


def _tier_key(tier: Any) -> str:
    """Stable string key for a tier value (e.g. Decimal('15') → '15').

    Integer-valued Decimals render without trailing ``.0``; fractional
    values keep their natural decimal form. Avoid ``Decimal.normalize``
    here — it can yield exponent-form output (``1E+2``) which is not
    what callers expect to dict-key on.
    """
    if isinstance(tier, Decimal):
        if tier == tier.to_integral_value():
            return str(int(tier))
        return format(tier, "f")
    if isinstance(tier, float):
        if tier == int(tier):
            return str(int(tier))
        return repr(tier)
    return str(tier)
