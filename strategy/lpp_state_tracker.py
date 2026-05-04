"""LPP share + estimated-reward tracker for Phase 1.1.

Phase 1.1 batch 2/3 of the SKHYNIX LPP strategy. Pure observation
layer: given a stream of ``(MarketSnapshot, my_quotes, inventory,
session)`` tuples, produce a per-snapshot share estimate, accumulate
estimated reward, and persist a JSONL audit trail.

Why this exists
---------------
After 4 weeks of running the strategy we must be able to answer:

1. **Calibration**: did our estimated reward match what Lighter
   actually paid out? Without a ground truth comparison we cannot
   tell whether the share model is over-/under-confident.
2. **Conditional fit**: is the gap consistent across sessions / spread
   tiers, or does the model break down in (say) the lunch break?

Without this tracker, account-balance-only analysis cannot separate
LPP rewards from market PnL from fee accruals — and a strategy could
quietly run unprofitable for weeks before we noticed.

What's in scope here
--------------------
* ``estimate_my_share(quote, market, session, ...)`` — pure
  function, deterministic, side-effect free.
* ``LppStateTracker.record_snapshot(...)`` — async (mainly so the
  caller is async-compatible; the implementation is fully sync).
  Computes share, accrues reward, writes a JSONL line, returns the
  same dict for further inspection.
* ``LppStateTracker.get_session_summary(start_ms, end_ms)`` —
  in-memory aggregate over the snapshot buffer.
* ``LppStateTracker.reconcile_actual_rewards(gateway, since_ms)`` —
  STUB until Lighter exposes the actual-rewards REST shape; returns
  ``{"actual_usdc": None, "stub": True, ...}``.

What's NOT here
---------------
* No order placement, no main loop, no paper-trading mode.
* No gateway interaction beyond the stub.
* The cap table and tier weights are inputs — config lives in
  ``config/lpp_pool.yaml``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .types import InventoryState, MarketSnapshot, Quote, SessionPolicy

logger = logging.getLogger(__name__)


_SECONDS_PER_WEEK = Decimal(7 * 24 * 3600)  # 604800
_DEFAULT_SNAPSHOT_WINDOW_SEC = Decimal(60)


# Cap table — USD-denominated rewardable depth ceiling per (session,
# tier). The KR session names match those produced by
# ``strategy.session_aware.get_kr_equity_session``.
#
# Source: Lighter LPP spreadsheet, Tier 6 (KR equity / RWA) row.
# Each row in the spreadsheet keys on a *liquidity regime* label
# (Market Hours / Before Open / After Close / Overnight / Weekend);
# we map our finer-grained session names onto those rows below.
#
# IMPORTANT: ``L1 / L2 / L3`` here are TIER LABELS — they don't
# correspond to a fixed bp band. The actual bp boundary for "L1"
# comes from ``SessionPolicy.tier_thresholds_bp[0]`` and varies by
# session: KR weekday tiers are 15/25/50 bp, KR_WEEKEND tiers are
# 50/100/200 bp. The same L1/L2/L3 cap entries apply to whichever
# bp band the active session declares.
#
# Cap is "max rewardable USDC of one-sided depth in that tier".
_CAP_TABLE: Dict[Tuple[str, str], Optional[Decimal]] = {
    # Market Hours — AM (00:00-04:00 UTC) and PM (05:00-06:30 UTC)
    # share the spreadsheet's "Market Hours" row.
    ("KR_MARKET_HOURS_AM", "L1"): Decimal("19000"),
    ("KR_MARKET_HOURS_AM", "L2"): Decimal("34000"),
    ("KR_MARKET_HOURS_AM", "L3"): Decimal("78000"),
    ("KR_MARKET_HOURS_PM", "L1"): Decimal("19000"),
    ("KR_MARKET_HOURS_PM", "L2"): Decimal("34000"),
    ("KR_MARKET_HOURS_PM", "L3"): Decimal("78000"),
    # Lunch break (04:00-05:00 UTC). The LPP spreadsheet does not
    # carve out a lunch row — Market Hours caps apply throughout.
    ("KR_LUNCH_BREAK", "L1"): Decimal("19000"),
    ("KR_LUNCH_BREAK", "L2"): Decimal("34000"),
    ("KR_LUNCH_BREAK", "L3"): Decimal("78000"),
    # Before Open (22:30-23:30 UTC) — the spreadsheet's smallest
    # caps, reflecting the thin liquidity right before resumption.
    ("KR_BEFORE_OPEN", "L1"): Decimal("7500"),
    ("KR_BEFORE_OPEN", "L2"): Decimal("20000"),
    ("KR_BEFORE_OPEN", "L3"): Decimal("51000"),
    # After Close (06:30-09:00 UTC) — almost matches Market Hours
    # but L1 is $18k (not $19k); a real spreadsheet quirk worth
    # preserving.
    ("KR_AFTER_CLOSE", "L1"): Decimal("18000"),
    ("KR_AFTER_CLOSE", "L2"): Decimal("34000"),
    ("KR_AFTER_CLOSE", "L3"): Decimal("78000"),
    # Overnight (09:00-22:30 UTC weekday) — narrowest reward band.
    ("KR_OVERNIGHT", "L1"): Decimal("5300"),
    ("KR_OVERNIGHT", "L2"): Decimal("12000"),
    ("KR_OVERNIGHT", "L3"): Decimal("31000"),
    # Pre-open window (23:30-24:00 UTC). 5-5 retune flipped this from
    # action="withdraw" → "quote" after Day-5 forensics found active
    # MM here. Caps cloned from KR_BEFORE_OPEN since the windows are
    # contiguous and the pre-resumption liquidity profile matches —
    # tune in place once we have a dedicated LPP spreadsheet row.
    ("KR_PRE_OPEN_WITHDRAW", "L1"): Decimal("7500"),
    ("KR_PRE_OPEN_WITHDRAW", "L2"): Decimal("20000"),
    ("KR_PRE_OPEN_WITHDRAW", "L3"): Decimal("51000"),
    # Weekend (Fri 09:00 UTC - Sun 22:30 UTC) — RWA-wide tiers
    # (50/100/200 bp boundaries declared by SessionPolicy). The
    # cap *labels* are still L1/L2/L3 and apply to that wider
    # spread band.
    ("KR_WEEKEND", "L1"): Decimal("19000"),
    ("KR_WEEKEND", "L2"): Decimal("43000"),
    ("KR_WEEKEND", "L3"): Decimal("94000"),
}


_TIER_INDEX = {"L1": 0, "L2": 1, "L3": 2}


def estimate_my_share(
    quote: Quote,
    market: MarketSnapshot,
    session: SessionPolicy,
    tier_weights: Dict[str, Decimal],
    cap_table: Optional[Dict[Tuple[str, str], Optional[Decimal]]] = None,
) -> Dict[str, Any]:
    """Pro-rata share estimate for one quote against the visible book.

    Model::

        raw_share = my_size / (existing_in_tier + my_size)
        weighted_share = raw_share * tier_weights[tier_target]

    where ``existing_in_tier`` is the cumulative one-sided notional
    already resting in the depth bucket whose upper edge is the
    quote's claimed tier (``L1`` → ``tier_thresholds_bp[0]``, etc.).

    The reported ``cap_usdc`` is informational for this batch — it
    drives reconciliation analysis later but does not bound the
    share. Overflow handling (existing > cap, attribution to next
    tier) is deferred to batch 3.

    ``tier_target == "OUT"`` short-circuits to share=0 with weight=0
    (an OUT quote is outside any rewardable tier).
    """
    table = cap_table if cap_table is not None else _CAP_TABLE

    tier = quote.tier_target
    my_size = quote.size_usdc

    if tier not in _TIER_INDEX:
        # OUT or unrecognised tag → no rewardable share.
        return {
            "tier": tier,
            "raw_share": Decimal(0),
            "weighted_share": Decimal(0),
            "existing_in_tier_usdc": Decimal(0),
            "cap_usdc": None,
            "my_size_usdc": my_size,
        }

    threshold_bp = session.tier_thresholds_bp[_TIER_INDEX[tier]]
    threshold_int = int(threshold_bp)
    bucket = market.depth_by_spread_bp.get(threshold_int, {})
    if quote.side == "buy":
        existing = bucket.get("bid_usdc", Decimal(0))
    else:
        existing = bucket.get("ask_usdc", Decimal(0))

    # Pure pro-rata. See module docstring for the cap-overflow
    # rationale — the cap is reported for reconciliation but does
    # not enter the share denominator in this batch.
    denominator = existing + my_size
    if denominator > 0:
        raw_share = my_size / denominator
    else:
        raw_share = Decimal(0)

    cap = table.get((session.name, tier))

    weight = tier_weights.get(tier, Decimal(0))
    weighted_share = raw_share * weight

    return {
        "tier": tier,
        "raw_share": raw_share,
        "weighted_share": weighted_share,
        "existing_in_tier_usdc": existing,
        "cap_usdc": cap,
        "my_size_usdc": my_size,
    }


def _to_jsonable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if is_dataclass(obj):
        return _to_jsonable(asdict(obj))
    return obj


def _percentile(values: List[Decimal], p: float) -> Optional[Decimal]:
    """Nearest-rank percentile. ``None`` for empty input."""
    if not values:
        return None
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[idx]


class LppStateTracker:
    """Eyes-only tracker: estimates LPP share and accrued reward.

    One tracker instance handles every symbol the strategy quotes;
    ``record_snapshot`` resolves the symbol from the supplied
    ``MarketSnapshot.symbol`` and routes JSONL writes to a per-
    symbol file under ``output_dir``.

    Thread-safety: not thread-safe by design. The expected caller is
    a single asyncio event loop. If you need concurrent updates,
    wrap with an external lock.
    """

    def __init__(
        self,
        weekly_pool_usdc: Dict[str, Decimal],
        tier_weights: Dict[str, Decimal],
        output_dir: Path,
        snapshot_window_sec: Decimal = _DEFAULT_SNAPSHOT_WINDOW_SEC,
        cap_table: Optional[Dict[Tuple[str, str], Optional[Decimal]]] = None,
    ):
        self._weekly_pool_usdc = {
            sym: Decimal(str(v)) for sym, v in weekly_pool_usdc.items()
        }
        self._tier_weights = {
            tag: Decimal(str(v)) for tag, v in tier_weights.items()
        }
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._snapshot_window_sec = Decimal(str(snapshot_window_sec))
        self._cap_table = cap_table  # None ⇒ use module default

        # Per-symbol cumulative reward (Decimal), buffer of records
        # for in-memory aggregation, and lazy file handles.
        self._cumulative_reward: Dict[str, Decimal] = {}
        self._records: Dict[str, List[Dict[str, Any]]] = {}
        self._fh: Dict[str, Any] = {}

    # ------------------------------------------------------------
    # public surface
    # ------------------------------------------------------------

    async def record_snapshot(
        self,
        ts_ms: int,
        market: MarketSnapshot,
        my_quotes: List[Quote],
        inventory: InventoryState,
        session: SessionPolicy,
    ) -> Dict[str, Any]:
        """Compute share + reward for the snapshot and persist a JSONL line.

        Returns the same record dict that was written to disk so
        callers (incl. the upcoming main loop) can introspect or
        re-emit it without re-reading the file.

        Unknown symbols (not in ``weekly_pool_usdc``) emit a single
        warning then continue with ``estimated_reward_60s_usdc=0``;
        the share/depth fields are still recorded so dry-run setups
        on test markets still produce useful audit trails.
        """
        symbol = market.symbol

        # Per-side share rollup. With the typical (1 bid, 1 ask)
        # plan_quotes output we expect two entries; defensive
        # against other counts.
        per_quote_share: List[Dict[str, Any]] = []
        bid_share: Optional[Dict[str, Any]] = None
        ask_share: Optional[Dict[str, Any]] = None
        for q in my_quotes:
            est = estimate_my_share(
                q, market, session, self._tier_weights, self._cap_table
            )
            per_quote_share.append(est)
            if q.side == "buy" and bid_share is None:
                bid_share = est
            elif q.side == "sell" and ask_share is None:
                ask_share = est

        # avg_weighted_share averages the SIDES we actually quoted.
        # If we only quoted one side, that side's weight is the
        # average — not zeroed-out, since rewards are per-side.
        weighted_components = [
            est["weighted_share"]
            for est in (bid_share, ask_share)
            if est is not None
        ]
        avg_weighted_share = (
            sum(weighted_components, Decimal(0)) / Decimal(len(weighted_components))
            if weighted_components
            else Decimal(0)
        )

        # Per-snapshot reward = weekly_pool * (window / week) * share.
        # ``avg_weighted_share`` already collapses bid+ask down to
        # one number, so we don't double-count the two sides.
        weekly_pool = self._weekly_pool_usdc.get(symbol)
        if weekly_pool is None:
            logger.warning(
                "lpp_state_tracker: symbol %r not in weekly_pool_usdc; "
                "recording snapshot with estimated_reward=0",
                symbol,
            )
            estimated_reward = Decimal(0)
        else:
            estimated_reward = (
                weekly_pool
                * (self._snapshot_window_sec / _SECONDS_PER_WEEK)
                * avg_weighted_share
            )

        cum = self._cumulative_reward.get(symbol, Decimal(0)) + estimated_reward
        self._cumulative_reward[symbol] = cum

        # Build depth_at_my_tier: one entry per unique tier we
        # actually have a quote in. Carries existing depth on the
        # right side (bid or ask) and the cap for cross-reference.
        # NOTE: the spec example showed both bid and ask side data
        # in one entry; we pick the side whose quote claimed the
        # tier. If both sides claim the same tier the bid wins
        # the entry but we expose both via ``per_quote_share``.
        depth_at_my_tier: Dict[str, Dict[str, Any]] = {}
        for est in per_quote_share:
            tier = est["tier"]
            if tier not in _TIER_INDEX:
                continue
            if tier in depth_at_my_tier:
                # Already recorded for the other side; skip to keep
                # the section compact.
                continue
            depth_at_my_tier[tier] = {
                "existing_usdc": est["existing_in_tier_usdc"],
                "cap_usdc": est["cap_usdc"],
            }

        # Record dict — dataclass-ish but kept as plain dict so the
        # JSONL representation is the canonical truth.
        record = {
            "ts_ms": int(ts_ms),
            "symbol": symbol,
            "session": session.name,
            "session_action": session.action,
            "mid": market.mid,
            "spread_bp": market.spread_bp,
            "my_quotes": [self._quote_to_dict(q) for q in my_quotes],
            "share": {
                "bid_tier": bid_share["tier"] if bid_share else None,
                "bid_raw_share": bid_share["raw_share"] if bid_share else None,
                "bid_weighted_share": bid_share["weighted_share"] if bid_share else None,
                "ask_tier": ask_share["tier"] if ask_share else None,
                "ask_raw_share": ask_share["raw_share"] if ask_share else None,
                "ask_weighted_share": ask_share["weighted_share"] if ask_share else None,
                "avg_weighted_share": avg_weighted_share,
            },
            "depth_at_my_tier": depth_at_my_tier,
            "estimated_reward_60s_usdc": estimated_reward,
            "cumulative_estimated_reward_usdc": cum,
            "inventory_net_delta_base": inventory.net_delta_base,
            "inventory_net_delta_usdc": inventory.net_delta_usdc,
        }

        # Buffer for in-memory aggregation, then persist.
        self._records.setdefault(symbol, []).append(record)
        self._write_record(symbol, record)
        return record

    def get_session_summary(
        self,
        session_start_ms: int,
        session_end_ms: int,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Aggregate the in-memory record buffer over [start, end].

        ``symbol=None`` aggregates across all symbols. Returns a
        dict shaped for direct JSONL serialization (Decimals are
        kept as Decimals for the caller to coerce).

        Empty period → zero-valued result with no exception.
        """
        records: List[Dict[str, Any]] = []
        symbols_iter = (
            [symbol] if symbol is not None else list(self._records.keys())
        )
        for sym in symbols_iter:
            for r in self._records.get(sym, ()):
                if session_start_ms <= r["ts_ms"] <= session_end_ms:
                    records.append(r)

        if not records:
            return {
                "duration_sec": 0,
                "estimated_reward_usdc": Decimal(0),
                "share_p50": None,
                "share_p95": None,
                "tier_distribution": {},
                "snapshots_count": 0,
            }

        estimated_total = sum(
            (r["estimated_reward_60s_usdc"] for r in records), Decimal(0)
        )
        avg_shares = [r["share"]["avg_weighted_share"] for r in records]
        share_p50 = _percentile(avg_shares, 0.50)
        share_p95 = _percentile(avg_shares, 0.95)

        # Tier distribution: count every quoted side's tier across
        # the window. A snapshot with bid=L1 and ask=L1 contributes
        # 2 to L1; a snapshot with bid=L1 and no ask contributes 1.
        tier_counts: Dict[str, int] = {}
        for r in records:
            for key in ("bid_tier", "ask_tier"):
                t = r["share"].get(key)
                if t is None:
                    continue
                tier_counts[t] = tier_counts.get(t, 0) + 1
        total = sum(tier_counts.values())
        tier_distribution = (
            {t: Decimal(c) / Decimal(total) for t, c in tier_counts.items()}
            if total > 0
            else {}
        )

        # Duration: span between first and last recorded snapshot,
        # not the requested window — gives the actual coverage.
        first_ts = min(r["ts_ms"] for r in records)
        last_ts = max(r["ts_ms"] for r in records)
        duration_sec = (last_ts - first_ts) / 1000.0

        return {
            "duration_sec": duration_sec,
            "estimated_reward_usdc": estimated_total,
            "share_p50": share_p50,
            "share_p95": share_p95,
            "tier_distribution": tier_distribution,
            "snapshots_count": len(records),
        }

    async def reconcile_actual_rewards(
        self,
        gateway: Any,
        since_ts_ms: int,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare ``estimated`` vs. on-chain ``actual`` LPP rewards.

        STUB: Lighter's actual-rewards REST shape isn't pinned down
        for this codebase yet. Returns ``actual_usdc=None`` and
        ``stub=True`` so callers can detect the placeholder. Wire
        the real REST path in batch 3 once the API is mapped.
        """
        logger.info(
            "reconcile_actual_rewards: stubbed (Lighter REST schema "
            "not yet integrated). Returning estimated only."
        )
        if symbol is not None:
            estimated = self._cumulative_reward.get(symbol, Decimal(0))
        else:
            estimated = sum(self._cumulative_reward.values(), Decimal(0))
        return {
            "estimated_usdc": estimated,
            "actual_usdc": None,
            "gap_pct": None,
            "details": [],
            "stub": True,
            "since_ts_ms": int(since_ts_ms),
            "symbol": symbol,
        }

    def close(self) -> None:
        """Flush and close every per-symbol JSONL file."""
        for sym, fh in list(self._fh.items()):
            try:
                fh.flush()
                fh.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("close(%s) raised: %s", sym, exc)
        self._fh.clear()

    # ------------------------------------------------------------
    # internals
    # ------------------------------------------------------------

    def _quote_to_dict(self, q: Quote) -> Dict[str, Any]:
        """Compact JSONL-friendly view of a Quote."""
        return {
            "side": q.side,
            "price": q.price,
            "size_base": q.size_base,
            "size_usdc": q.size_usdc,
            "distance_from_mid_bp": q.distance_from_mid_bp,
            "tier_target": q.tier_target,
            "market_position": q.market_position,
        }

    def _path_for(self, symbol: str) -> Path:
        date = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
        safe_symbol = symbol.replace("/", "-")
        return self._output_dir / f"{safe_symbol}_{date}.jsonl"

    def _ensure_handle(self, symbol: str) -> Any:
        fh = self._fh.get(symbol)
        if fh is not None:
            return fh
        path = self._path_for(symbol)
        # Append-mode so resuming a session from a crashed run
        # extends the same file rather than overwriting.
        fh = open(path, "a", encoding="utf-8")
        self._fh[symbol] = fh
        return fh

    def _write_record(self, symbol: str, record: Dict[str, Any]) -> None:
        try:
            fh = self._ensure_handle(symbol)
            fh.write(json.dumps(_to_jsonable(record), ensure_ascii=False) + "\n")
            fh.flush()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "lpp_state_tracker: failed to write JSONL for %s: %s",
                symbol,
                exc,
            )
