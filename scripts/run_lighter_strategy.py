"""Phase 1.1 batch 3 part 3 — strategy runner entry point.

Two modes:

* ``--paper`` (default): delegates to ``scripts/run_lighter_paper.py``'s
  ``PaperRun`` so the same simulator + tracker pipeline that's been
  shaken out for hours of read-only mainnet runs is what we use here.
  No ``.env`` required.

* ``--live --i-understand-this-is-real``: connects the real signer,
  optionally cancels every stale order on the account
  (``recover_initial_state``), starts ``LppQuoter`` against the live
  ``LighterOrderManager``, and lets it quote until the duration
  elapses or SIGINT arrives.

Two safety locks on the live path:

1. ``--live`` without ``--i-understand-this-is-real`` exits 2 before
   any network connection.
2. On a TTY, the runner blocks on ``YES I AGREE`` after summarising
   the run (market, account index, duration, output). On a
   non-interactive shell (systemd / pipe / CI) the prompt is
   skipped — but ``--i-understand-this-is-real`` is still required.

Recovery (live only)
--------------------
Before the quoter starts we pull the account's open orders on the
target market and cancel each one through the SDK. We also pull the
live position and:

* Any non-zero residual base → call
  ``LighterOrderManager.inject_initial_inventory`` so the OM starts
  with the correct net delta. Without this seed (the original
  behaviour through 4-30), the planner under-caps risk for the first
  few seconds before the ws ``account_all`` push lands — long enough
  for repeated fills to push the account well past the hard cap.
* At or above ``recovery.abort_if_existing_position_above_usdc`` →
  log ERROR and abort the startup. The default in
  ``config/lighter_strategy.yaml`` is ``"0"`` (abort on ANY non-zero
  residual) so a clean shutdown is required between runs; raise the
  threshold to permit the inject path during dev/testing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Project root + scripts dir on sys.path so we can import siblings
# (run_lighter_paper.PaperRun) and project packages alike.
_THIS = Path(__file__).resolve()
_PROJECT_ROOT = _THIS.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml  # noqa: E402

from execution.lighter.lighter_order_manager import LighterOrderManager  # noqa: E402
from gateways.lighter_gateway import LighterGateway, LighterGatewayError  # noqa: E402
from gateways.lighter_ws import LighterWebSocket  # noqa: E402
from strategy.lpp_quoter import LppQuoter  # noqa: E402
from strategy.lpp_state_tracker import LppStateTracker  # noqa: E402

logger = logging.getLogger("strategy_run")


# ----- argparse + confirmation -------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Lighter LPP strategy runner. Defaults to --paper mode "
            "(no orders sent). --live requires --i-understand-this-is-real "
            "and a TTY confirmation."
        ),
    )
    p.add_argument(
        "--market",
        default=None,
        help="Market symbol (e.g. SKHYNIXUSD). Falls back to config 'strategy.market'.",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=240.0,
        help="Run duration in minutes. 0 = run forever (until SIGINT).",
    )
    p.add_argument(
        "--config",
        default="config/lighter_strategy.yaml",
        help="Path to strategy yaml (default: config/lighter_strategy.yaml).",
    )
    p.add_argument(
        "--output-dir",
        default="logs/strategy",
        help="Where to write JSONL output (default: logs/strategy).",
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--paper",
        action="store_true",
        help="Paper mode (default). Reuses the paper trading runner — no orders sent.",
    )
    mode.add_argument(
        "--live",
        action="store_true",
        help="Live mode. Real orders against real collateral. Requires confirmation.",
    )
    p.add_argument(
        "--i-understand-this-is-real",
        dest="i_understand_this_is_real",
        action="store_true",
        help="Required with --live. Acknowledges real-money risk.",
    )
    p.add_argument(
        "--no-recover",
        action="store_true",
        help="Skip startup inventory sync (cancel stale orders / position warn).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for paper fill simulator (paper mode only).",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="-v=INFO, -vv=DEBUG.",
    )
    return p


def _confirm_live_mode(args: argparse.Namespace, account_index: Optional[str]) -> None:
    """Block until the user types ``YES I AGREE`` on a TTY.

    On non-TTY (systemd, piped stdin) the prompt is skipped — the
    ``--i-understand-this-is-real`` flag is the only confirmation
    available there. We've already validated that flag in the caller.
    """
    sys.stderr.write("\n=== LIVE TRADING MODE ===\n")
    sys.stderr.write(f"market:        {args.market}\n")
    sys.stderr.write(f"account_index: {account_index}\n")
    duration_label = "forever (Ctrl-C to stop)" if args.duration == 0 else f"{args.duration} min"
    sys.stderr.write(f"duration:      {duration_label}\n")
    sys.stderr.write(f"output:        {args.output_dir}\n")
    sys.stderr.write(f"recover:       {'NO' if args.no_recover else 'yes'}\n")
    sys.stderr.write("\nThis will place REAL orders against REAL collateral.\n")
    sys.stderr.flush()

    if sys.stdin.isatty():
        try:
            response = input("Type 'YES I AGREE' to proceed: ").strip()
        except EOFError:
            response = ""
        if response != "YES I AGREE":
            sys.stderr.write("Aborted.\n")
            sys.exit(2)
    else:
        sys.stderr.write(
            "Non-interactive shell — proceeding without prompt.\n"
        )


def parse_and_confirm_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse argv, fail fast on invalid combos, run the live confirmation.

    Pure-ish: everything that touches stdin/stderr happens through
    ``_confirm_live_mode`` so unit tests can patch it.
    """
    args = _build_arg_parser().parse_args(argv)

    # Default to paper if neither --paper nor --live is set.
    if not args.live and not args.paper:
        args.paper = True

    if args.live and not args.i_understand_this_is_real:
        sys.stderr.write(
            "--live requires --i-understand-this-is-real to acknowledge real-money risk\n"
        )
        sys.exit(2)

    if args.live:
        # Surface the account_index from env so the prompt is concrete.
        account_index = os.environ.get("LIGHTER_ACCOUNT_INDEX") or "<unset>"
        _confirm_live_mode(args, account_index)

    return args


# ----- yaml + env loading ------------------------------------------------


def _setup_logging(verbosity: int, jsonl_path: Path) -> logging.Logger:
    # Logging levels:
    #   default (no -v): WARNING+ for libraries, INFO for strategy
    #   -v:              INFO for everything
    #   -vv (future):    DEBUG for everything (heavy disk IO, only for replay)
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("strategy_run")


def _load_strategy_yaml(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"strategy config not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw


def _coerce_decimal_fields(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Convert known Decimal-typed strategy fields from yaml str → Decimal.

    Yaml stores numbers as float by default and strings as str; we
    quote the numeric values in the yaml so they parse as str, then
    coerce here. This avoids accidental Decimal/float type mismatches
    in plan_quotes (which is strict about Decimal arithmetic).
    """
    decimal_keys = {
        "target_max_delta_usdc", "skew_max_offset_bp", "hard_position_cap_usdc",
        "min_market_spread_bp", "max_market_spread_bp",
        "share_warn_threshold", "share_warn_widen_bp",
        "reprice_drift_bp", "reprice_min_interval_sec",
        "tick_interval_sec", "snapshot_interval_sec",
        "price_tolerance_bp", "size_tolerance_pct",
        # Phase 2.1: double cap + active hedge + daily drawdown.
        "hard_position_cap_pct",
        "active_hedge_trigger_pct", "active_hedge_target_pct",
        "active_hedge_taker_fee_max_pct",
        "daily_max_drawdown_usdc", "daily_max_drawdown_pct",
    }
    out: Dict[str, Any] = dict(cfg)
    for k in decimal_keys:
        if k in out and not isinstance(out[k], Decimal):
            out[k] = Decimal(str(out[k]))

    # Coerce nested session_overrides: each session's numeric fields → Decimal.
    # The override dict shape is `{SESSION_NAME: {default_size_usdc: ..., ...}}`
    # — yaml's auto-typing doesn't reach into the inner dict, so plan_quotes
    # would otherwise see strings and break Decimal arithmetic.
    if "session_overrides" in out and isinstance(out["session_overrides"], dict):
        session_decimal_keys = {"default_size_usdc", "default_distance_bp"}
        coerced_overrides: Dict[str, Any] = {}
        for sess_name, sess_cfg in out["session_overrides"].items():
            if not isinstance(sess_cfg, dict):
                continue
            sess_out = dict(sess_cfg)
            for k in session_decimal_keys:
                if k in sess_out and not isinstance(sess_out[k], Decimal):
                    sess_out[k] = Decimal(str(sess_out[k]))
            coerced_overrides[str(sess_name)] = sess_out
        out["session_overrides"] = coerced_overrides
    return out


def _load_lpp_pool_yaml() -> Dict[str, Any]:
    pool_path = _PROJECT_ROOT / "config" / "lpp_pool.yaml"
    if not pool_path.exists():
        return {"lpp_pool": {}, "tier_weights": {}}
    with open(pool_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ----- recover_initial_state --------------------------------------------


async def recover_initial_state(
    gateway: LighterGateway,
    market_index: int,
    symbol: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Cancel stale open orders on the target market + diagnose position.

    Returns ``{
        "cancelled_orders": list[int],
        "cancel_failures": list[dict],
        "existing_position": dict | None,
        "abort": bool,
        "abort_reason": str | None,
    }``.

    ``abort`` is set when the existing position notional ≥
    ``recovery.abort_if_existing_position_above_usdc`` so the caller
    can refuse to start the quoter. The OM has no public
    set_initial_inventory hook, so a non-trivial residual position
    has to be flattened manually before the strategy can start.
    """
    result: Dict[str, Any] = {
        "cancelled_orders": [],
        "cancel_failures": [],
        "existing_position": None,
        "abort": False,
        "abort_reason": None,
    }

    # 1. Existing position diagnosis ----------------------------------
    try:
        positions = await gateway.get_account_positions()
    except Exception as exc:  # noqa: BLE001
        logger.error("recover: get_account_positions failed: %s", exc)
        positions = []

    pos_for_market: Optional[Dict[str, Any]] = None
    for p in positions:
        if p.get("symbol") == symbol or p.get("market_index") == market_index:
            pos_for_market = p
            break
    if pos_for_market is not None:
        base = pos_for_market.get("base") or Decimal(0)
        notional = pos_for_market.get("position_value_usdc") or Decimal(0)
        if notional is None:
            notional = Decimal(0)
        notional_abs = abs(Decimal(str(notional)))
        result["existing_position"] = pos_for_market

        warn_threshold = Decimal(
            str(config.get("warn_if_existing_position_above_usdc", "100"))
        )
        abort_threshold = Decimal(
            str(config.get("abort_if_existing_position_above_usdc", "1000"))
        )
        if base != 0:
            # Phase 1.2 P0.2 semantics:
            #   abort_threshold == 0  → abort on ANY non-zero residual
            #     (production default — forces a clean account every run)
            #   abort_threshold  > 0  → abort if notional_abs >= threshold
            #     (dev/testing — exercises the OM inject path below)
            #   abort_threshold  < 0  → never abort (explicit opt-out)
            should_abort = abort_threshold == 0 or (
                abort_threshold > 0 and notional_abs >= abort_threshold
            )
            if should_abort:
                result["abort"] = True
                result["abort_reason"] = (
                    f"existing position notional {notional_abs} USDC "
                    f"(base={base}) ≥ abort threshold {abort_threshold} USDC; "
                    "flatten manually before starting"
                )
                logger.error("RECOVER ABORT: %s", result["abort_reason"])
            else:
                # Any non-zero residual is now injected into the OM by
                # the caller (run_live_mode). The runner logs this WARN
                # and the inject log line itself together form the audit
                # trail. Removed the prior "ignoring small residual"
                # branch — silently dropping a position is what caused
                # the 4-30 catastrophic OM/server desync.
                logger.warning(
                    "RECOVER: existing position base=%s notional=%s USDC; "
                    "will inject into OM (warn=%s, abort=%s)",
                    base,
                    notional_abs,
                    warn_threshold,
                    abort_threshold,
                )

    # If we already decided to abort, skip the cancel pass.
    if result["abort"]:
        return result

    if not config.get("cancel_stale_orders", True):
        logger.info("RECOVER: cancel_stale_orders=false; leaving any open orders alone")
        return result

    # 2. Cancel stale orders -----------------------------------------
    try:
        open_orders = await gateway.get_open_orders(market_index)
    except Exception as exc:  # noqa: BLE001
        logger.error("recover: get_open_orders failed: %s", exc)
        open_orders = []

    if open_orders:
        logger.warning(
            "RECOVER: found %d stale orders on %s, cancelling all",
            len(open_orders),
            symbol,
        )
    for o in open_orders:
        order_index = o.get("order_index")
        market_idx = o.get("market_index", market_index)
        if not order_index:
            continue
        try:
            cancel_result = await gateway.cancel_order_by_index(market_idx, order_index)
        except Exception as exc:  # noqa: BLE001
            cancel_result = {"ok": False, "error": str(exc)}
        if cancel_result.get("ok"):
            result["cancelled_orders"].append(order_index)
        else:
            result["cancel_failures"].append({
                "order_index": order_index,
                "error": cancel_result.get("error", "unknown"),
            })
            logger.error(
                "RECOVER: cancel_order_by_index(%s) failed: %s",
                order_index,
                cancel_result.get("error"),
            )

    # 3. Wait for ws-side cancellations to confirm -------------------
    if result["cancelled_orders"]:
        wait_sec = float(config.get("wait_after_cancel_sec", 5))
        logger.info("RECOVER: waiting %.1fs for cancellations to confirm", wait_sec)
        await asyncio.sleep(wait_sec)

    return result


# ----- JSONL writer ------------------------------------------------------


class _JsonlWriter:
    """Append-only JSONL log shared with all callers in this run."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")
        self.path = path

    def write(self, payload: Dict[str, Any]) -> None:
        try:
            self._fh.write(
                json.dumps(_jsonable(payload), ensure_ascii=False) + "\n"
            )
            self._fh.flush()
        except Exception as exc:  # noqa: BLE001
            logger.debug("jsonl write failed: %s", exc)

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:  # noqa: BLE001
            pass


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if is_dataclass(obj):
        return _jsonable(asdict(obj))
    return obj


# ----- live mode runner --------------------------------------------------


async def run_live_mode(
    args: argparse.Namespace,
    yaml_cfg: Dict[str, Any],
    jsonl: _JsonlWriter,
) -> int:
    """Connect signer + ws, recover state, drive LppQuoter."""
    strategy_cfg_raw = yaml_cfg.get("strategy", {}) or {}
    strategy_cfg = _coerce_decimal_fields(strategy_cfg_raw)
    market = args.market or strategy_cfg.get("market", "SKHYNIXUSD")
    market_index = int(strategy_cfg.get("market_index", 0))
    price_decimals = int(strategy_cfg.get("price_decimals", 4))
    size_decimals = int(strategy_cfg.get("size_decimals", 4))

    # Build gateway from env-backed default config (API_KEY_PRIVATE_KEY etc).
    gw_cfg_path = _PROJECT_ROOT / "config" / "lighter_config.yaml"
    if not gw_cfg_path.exists():
        logger.error("missing %s — cannot connect signer", gw_cfg_path)
        return 3
    gateway = LighterGateway.from_config_file(str(gw_cfg_path))
    try:
        await gateway.initialize()
    except LighterGatewayError as exc:
        logger.error("gateway init failed: %s", exc)
        jsonl.write({"event": "init_error", "error": str(exc)})
        return 3

    if gateway.account_index is None or gateway.signer_client is None:
        logger.error("--live requires API_KEY_PRIVATE_KEY + LIGHTER_ACCOUNT_INDEX in env")
        await gateway.close()
        return 3

    # Re-derive market_index from gateway if config didn't set it.
    if market_index == 0:
        idx = gateway.get_market_index(market)
        if idx is None:
            logger.error("market %r not found on Lighter", market)
            await gateway.close()
            return 2
        market_index = idx

    ws = LighterWebSocket(testnet=False)

    # 1. Recovery (before quoter starts)
    recovery_summary: Dict[str, Any] = {}
    if not args.no_recover:
        recovery_cfg = yaml_cfg.get("recovery", {}) or {}
        recovery_summary = await recover_initial_state(
            gateway, market_index, market, recovery_cfg
        )
        jsonl.write({"event": "recover_done", **_jsonable(recovery_summary)})
        if recovery_summary.get("abort"):
            logger.error("aborting startup due to recovery: %s",
                         recovery_summary.get("abort_reason"))
            await gateway.close()
            return 4

    # 2. WS connect (subscribes market_stats + trade automatically)
    try:
        await ws.connect([market_index])
    except Exception as exc:  # noqa: BLE001
        logger.error("ws connect failed: %s", exc)
        jsonl.write({"event": "ws_connect_error", "error": str(exc)})
        await gateway.close()
        return 5

    # 3. OM + tracker + quoter
    om = LighterOrderManager(
        gateway=gateway,
        ws=ws,
        account_index=int(gateway.account_index),
        market_index_filter=market_index,
        # Phase 3 multi-market REST budget: stretch the safety-net
        # poll so 3 concurrent processes don't burn the WAF allowance.
        # Default 30s in the OM; yaml overrides via this key.
        periodic_sync_interval_sec=float(
            strategy_cfg.get("om_periodic_sync_interval_sec", 30)
        ),
    )

    # Phase 1.2 P0.1: seed the OM with the existing position from
    # recover. Without this, OM starts at inv=0 and ``plan_quotes``
    # under-caps risk until the ws ``account_all`` push lands (which
    # may take seconds, may not include positions for some subscription
    # paths, or may race with the first reprice). The 4-30 Run 2
    # catastrophic short ($131 against a $100 hard cap) was caused
    # exactly by this gap.
    recover_pos = recovery_summary.get("existing_position")
    if recover_pos is not None and recover_pos.get("base") is not None:
        try:
            base_raw = recover_pos.get("base") or "0"
            base = abs(Decimal(str(base_raw)))
            sign = int(recover_pos.get("sign", 1))
            avg_entry = recover_pos.get("avg_entry_price")
            avg_entry_dec: Optional[Decimal] = None
            if avg_entry is not None:
                try:
                    avg_entry_dec = Decimal(str(avg_entry))
                    if avg_entry_dec <= 0:
                        avg_entry_dec = None
                except Exception:  # noqa: BLE001
                    avg_entry_dec = None
            if base > 0:
                om.inject_initial_inventory(
                    base=base,
                    avg_entry_price=avg_entry_dec,
                    sign=sign,
                )
                logger.info(
                    "RECOVER: injected existing position into OM "
                    "(base=%s, avg=%s, sign=%s)",
                    base, avg_entry_dec, sign,
                )
                jsonl.write({
                    "event": "recover_inject",
                    "base": str(base),
                    "avg_entry_price": str(avg_entry_dec) if avg_entry_dec else None,
                    "sign": sign,
                })
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "RECOVER: failed to inject existing position into OM: %s",
                exc,
            )
    pool_yaml = _load_lpp_pool_yaml()
    weekly_pool = {
        sym: Decimal(str(v))
        for sym, v in (pool_yaml.get("lpp_pool") or {}).items()
    }
    tier_weights = {
        tag: Decimal(str(v))
        for tag, v in (pool_yaml.get("tier_weights") or {}).items()
    }
    tracker = LppStateTracker(
        weekly_pool_usdc=weekly_pool,
        tier_weights=tier_weights,
        output_dir=Path(args.output_dir) / "lpp_state",
        snapshot_window_sec=Decimal(str(strategy_cfg.get("snapshot_interval_sec", 60))),
    )
    quoter = LppQuoter(
        gateway=gateway,
        ws=ws,
        order_manager=om,
        tracker=tracker,
        symbol=market,
        market_index=market_index,
        price_decimals=price_decimals,
        size_decimals=size_decimals,
        account_index=int(gateway.account_index),
        config=strategy_cfg,
    )

    jsonl.write({
        "event": "strategy_start",
        "ts_ms": int(time.time() * 1000),
        "mode": "live",
        "market": market,
        "market_index": market_index,
        "duration_min": args.duration,
        "config": _jsonable(strategy_cfg),
        "recovery": _jsonable(recovery_summary),
    })
    logger.info(
        "STRATEGY START mode=live market=%s market_index=%d duration_min=%.1f",
        market, market_index, args.duration,
    )

    # Echo the *effective* config so a tailing log makes it obvious
    # what knobs the run is using (yaml + defaults merged). Mirrors
    # the JSONL strategy_start event we just wrote, but in a
    # tail-friendly format. Keys come from ``strategy_cfg`` (already
    # decimal-coerced via ``_coerce_decimal_fields``).
    cfg = strategy_cfg
    logger.info("=== STRATEGY CONFIG (effective) ===")
    logger.info(
        "  market: %s (idx=%d, price_dec=%d, size_dec=%d)",
        market, market_index, price_decimals, size_decimals,
    )
    logger.info(
        "  size: target_max=%s hard_cap=%s skew_max=%sbp",
        cfg.get("target_max_delta_usdc"),
        cfg.get("hard_position_cap_usdc"),
        cfg.get("skew_max_offset_bp"),
    )
    logger.info(
        "  hard_cap_pct: %s (double cap; effective = min(usdc, pct*collateral))",
        cfg.get("hard_position_cap_pct") or "(disabled)",
    )
    logger.info(
        "  active_hedge: enabled=%s trigger=%s target=%s slip_pct=%s pause=%ss",
        cfg.get("active_hedge_enabled", False),
        cfg.get("active_hedge_trigger_pct"),
        cfg.get("active_hedge_target_pct"),
        cfg.get("active_hedge_taker_fee_max_pct"),
        cfg.get("active_hedge_pause_after_sec"),
    )
    logger.info(
        "  daily_drawdown: max_usdc=%s max_pct=%s interval=%ss",
        cfg.get("daily_max_drawdown_usdc"),
        cfg.get("daily_max_drawdown_pct"),
        cfg.get("daily_drawdown_check_interval_sec"),
    )
    logger.info(
        "  market filters: spread_min=%sbp max=%sbp",
        cfg.get("min_market_spread_bp"),
        cfg.get("max_market_spread_bp"),
    )
    logger.info(
        "  share: warn_threshold=%s widen_bp=%s",
        cfg.get("share_warn_threshold"),
        cfg.get("share_warn_widen_bp"),
    )
    logger.info(
        "  reprice: drift=%sbp min_interval=%ss",
        cfg.get("reprice_drift_bp"),
        cfg.get("reprice_min_interval_sec"),
    )
    logger.info(
        "  cadence: tick=%ss snapshot=%ss collateral_refresh=%ss om_sync=%ss",
        cfg.get("tick_interval_sec"),
        cfg.get("snapshot_interval_sec"),
        cfg.get("collateral_refresh_interval_sec"),
        cfg.get("om_periodic_sync_interval_sec", 30),
    )
    logger.info(
        "  emergency: rejects=%s cancel_fails=%s ws_silent=%ss",
        cfg.get("emergency_stop_on_consecutive_reject_count"),
        cfg.get("emergency_stop_on_consecutive_cancel_fail_count"),
        cfg.get("emergency_stop_on_ws_disconnect_sec"),
    )
    overrides = cfg.get("session_overrides") or {}
    if overrides:
        logger.info("  session_overrides:")
        for sess_name, sess_cfg in sorted(overrides.items()):
            size = sess_cfg.get("default_size_usdc", "(default)")
            dist = sess_cfg.get("default_distance_bp", "(default)")
            logger.info("    %s: size=%s dist=%s", sess_name, size, dist)
    else:
        logger.warning(
            "  session_overrides: NONE — using hard-coded $500-$1000 defaults"
        )
    logger.info("=== END CONFIG ===")

    # 4. SIGINT / SIGTERM → request stop
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    await quoter.start()

    try:
        # Permanent run if duration==0; clamp to 365d as the
        # actual deadline so run_until's bookkeeping is finite.
        deadline = (
            time.time() + args.duration * 60
            if args.duration > 0
            else time.time() + 365 * 86400
        )
        run_task = asyncio.create_task(quoter.run_until(deadline))
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            [run_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        logger.info("STRATEGY SHUTDOWN starting")
        try:
            await quoter.shutdown()
        except Exception as exc:  # noqa: BLE001
            logger.warning("quoter.shutdown raised: %s", exc)
        summary = quoter.get_summary()
        jsonl.write({
            "event": "strategy_end",
            "ts_ms": int(time.time() * 1000),
            "summary": _jsonable(summary),
        })
        # Tail-friendly final summary. JSONL still has the canonical
        # record above; this is for the operator reading the console.
        try:
            om_stats = summary.get("om", {}) or {}
            session_start_ms = summary.get("session_start_ts_ms") or 0
            session_end_ms = summary.get("session_end_ts_ms") or int(time.time() * 1000)
            actual_duration_min = (
                (session_end_ms - session_start_ms) / 60_000.0
                if session_start_ms and session_end_ms
                else 0.0
            )
            try:
                ws_stats = ws.get_message_stats() or {}
            except Exception:  # noqa: BLE001
                ws_stats = {}
            try:
                inv = quoter.get_inventory()
            except Exception:  # noqa: BLE001
                inv = None
            try:
                tracker_summary = quoter._tracker.get_session_summary(
                    session_start_ms=session_start_ms,
                    session_end_ms=session_end_ms,
                    symbol=market,
                ) or {}
            except Exception:  # noqa: BLE001
                tracker_summary = {}

            logger.info("=== STRATEGY FINAL SUMMARY ===")
            logger.info(
                "  duration: %.1fmin (%d ticks, %d skipped)",
                actual_duration_min,
                summary.get("tick_count", 0),
                summary.get("skipped_tick_count", 0),
            )
            logger.info(
                "  reprice: count=%d (last_reason=%s)",
                summary.get("reprice_count", 0),
                summary.get("last_reprice_reason") or "-",
            )
            logger.info(
                "  emergency: stops=%d rejects=%d cancel_fails=%d",
                summary.get("emergency_stops", 0),
                summary.get("consecutive_rejects", 0),
                summary.get("consecutive_cancel_failures", 0),
            )
            logger.info(
                "  om: submitted=%d filled=%d cancelled=%d rejected=%d active=%d",
                om_stats.get("lifetime_submitted", 0),
                om_stats.get("lifetime_filled", 0),
                om_stats.get("lifetime_cancelled", 0),
                om_stats.get("lifetime_rejected", 0),
                om_stats.get("active_count", 0),
            )
            logger.info(
                "  rest_syncs: total=%d", om_stats.get("rest_syncs_total", 0)
            )
            logger.info(
                "  ws: connect_count=%d max_uptime=%.0fs last_reason=%s",
                ws_stats.get("connect_count", 0),
                ws_stats.get("max_uptime_sec", 0) or 0,
                ws_stats.get("last_disconnect_reason"),
            )
            if inv is not None:
                logger.info(
                    "  inventory: %s base @ %s avg, %s USDC mark-to-market",
                    inv.net_delta_base,
                    inv.avg_entry_price if inv.avg_entry_price is not None else "-",
                    inv.net_delta_usdc,
                )
            logger.info(
                "  lpp: snapshots=%d est_reward=%s share_p50=%s",
                tracker_summary.get("snapshots_count", 0),
                tracker_summary.get("estimated_reward_usdc"),
                tracker_summary.get("share_p50"),
            )
            logger.info("=== END SUMMARY ===")
        except Exception as exc:  # noqa: BLE001
            # Keep the original concise log as a fallback so a buggy
            # summary block can't swallow the run-end notice.
            logger.warning("final summary formatting failed: %s", exc)
            logger.info("STRATEGY SHUTDOWN done summary=%s", summary)

        # Phase 1.2 P0.2: verify the account is clean post-shutdown.
        # We don't auto-close residual positions (taker risk + cross-market
        # complexity); instead we emit a loud ERROR so the operator knows
        # the next run will need either a manual flatten or a recovery
        # threshold bump. Mirrors the recover-side abort gate so the dirty
        # state can't link from one run to the next unnoticed.
        try:
            final_positions = await gateway.get_account_positions()
        except Exception as exc:  # noqa: BLE001
            logger.error("post-shutdown position check failed: %s", exc)
            final_positions = []
        try:
            final_orders = await gateway.get_open_orders(market_index)
        except Exception as exc:  # noqa: BLE001
            logger.error("post-shutdown order check failed: %s", exc)
            final_orders = []
        nz_positions = []
        for p in final_positions:
            if p.get("symbol") == market or p.get("market_index") == market_index:
                try:
                    if Decimal(str(p.get("base") or 0)) != 0:
                        nz_positions.append(p)
                except Exception:  # noqa: BLE001
                    continue
        if nz_positions or final_orders:
            logger.error("=" * 70)
            logger.error("POST-SHUTDOWN ACCOUNT NOT CLEAN")
            for p in nz_positions:
                logger.error(
                    "  RESIDUAL POSITION: %s base=%s notional=%s",
                    p.get("symbol"),
                    p.get("base"),
                    p.get("position_value_usdc"),
                )
            if final_orders:
                logger.error("  RESIDUAL ORDERS: %d on book", len(final_orders))
            logger.error(
                "  -> Manually close on https://app.lighter.xyz before next run"
            )
            logger.error(
                "  -> Next --live run will refuse to start until account is clean"
            )
            logger.error("=" * 70)
            jsonl.write({
                "event": "post_shutdown_dirty",
                "residual_positions": _jsonable(nz_positions),
                "residual_orders_count": len(final_orders),
            })
        else:
            logger.info("POST-SHUTDOWN: account verified clean")
            jsonl.write({"event": "post_shutdown_clean"})

        try:
            await ws.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.debug("ws disconnect raised: %s", exc)
        try:
            await gateway.close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("gateway close raised: %s", exc)

    return 0


# ----- paper mode runner -------------------------------------------------


async def run_paper_mode(
    args: argparse.Namespace,
    jsonl: _JsonlWriter,
) -> int:
    """Delegate to scripts/run_lighter_paper.py's PaperRun.

    Reuses the simulator + tracker pipeline that's been shaken out
    on hours of read-only mainnet runs. Args translate 1:1 (the
    paper runner's CLI is a strict subset of ours).
    """
    from scripts.run_lighter_paper import PaperRun

    paper_args = argparse.Namespace(
        market=args.market or "SKHYNIXUSD",
        duration=args.duration if args.duration > 0 else 24 * 60.0,
        seed=args.seed,
        output_dir=str(Path(args.output_dir) / "paper"),
        initial_collateral="2000",
        verbose=args.verbose,
    )
    jsonl.write({
        "event": "strategy_start",
        "ts_ms": int(time.time() * 1000),
        "mode": "paper",
        "market": paper_args.market,
        "duration_min": paper_args.duration,
        "seed": paper_args.seed,
    })
    logger.info(
        "STRATEGY START mode=paper market=%s duration_min=%.1f",
        paper_args.market,
        paper_args.duration,
    )
    runner = PaperRun(paper_args)
    code = await runner.run()
    jsonl.write({
        "event": "strategy_end",
        "ts_ms": int(time.time() * 1000),
        "mode": "paper",
        "exit_code": code,
    })
    logger.info("STRATEGY SHUTDOWN done (paper exit=%d)", code)
    return code


# ----- signal handling ---------------------------------------------------


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Best-effort SIGINT/SIGTERM → ``stop_event.set``.

    Windows ProactorEventLoop doesn't support ``add_signal_handler``;
    on that platform the process still exits via KeyboardInterrupt
    and the outer ``main`` catches it.
    """
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        try:
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        except (AttributeError, NotImplementedError):
            pass
    except (NotImplementedError, RuntimeError):
        logger.info(
            "Signal handlers unavailable on this platform; "
            "Ctrl-C still aborts via KeyboardInterrupt"
        )


# ----- main --------------------------------------------------------------


async def _async_main(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_tag = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
    yaml_cfg = _load_strategy_yaml(args.config)
    log_section = yaml_cfg.get("logging", {}) or {}
    jsonl_filename = log_section.get("jsonl_filename", "strategy_{date}.jsonl").format(
        date=date_tag
    )
    jsonl_path = output_dir / jsonl_filename
    _setup_logging(args.verbose, jsonl_path)
    jsonl = _JsonlWriter(jsonl_path)
    logger.info("strategy log → %s", jsonl_path)

    try:
        if args.live:
            return await run_live_mode(args, yaml_cfg, jsonl)
        return await run_paper_mode(args, jsonl)
    finally:
        jsonl.close()


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_and_confirm_args(argv)
    try:
        return asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
