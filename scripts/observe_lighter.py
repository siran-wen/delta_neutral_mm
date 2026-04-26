"""CLI entry point for the Lighter observation session.

Typical invocations::

    # Quick probe of the RWA cohort (no long session)
    python scripts/observe_lighter.py --probe-rwa

    # 30-minute single-symbol session without account auth
    python scripts/observe_lighter.py --market SAMSUNGUSD --duration 30 --no-account

    # Multi-symbol, default duration (runs until SIGINT)
    python scripts/observe_lighter.py --market ETH --market BTC --schedule crypto

The script is pure-async; it uses ``asyncio.run`` as its entry point.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure the project root is on sys.path when running as a script
_THIS = Path(__file__).resolve()
_PROJECT_ROOT = _THIS.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml  # noqa: E402  (after sys.path insert)

from gateways.lighter_gateway import LighterGateway, LighterGatewayError  # noqa: E402
from gateways.lighter_ws import LighterWebSocket  # noqa: E402
from observe.lighter_observer import (  # noqa: E402
    LighterObserver,
    ObservationConfig,
    run_rwa_probe,
)
from observe.session_clock import MarketSchedule  # noqa: E402


RWA_PROBE_SYMBOLS = ("SAMSUNGUSD", "HYUNDAIUSD", "SKHYNIXUSD", "XAU", "BRENTOIL")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Lighter market observation session (Phase 1.0 read-only)"
    )
    p.add_argument(
        "--config",
        default="config/lighter_config.yaml",
        help="Path to lighter config YAML (default: config/lighter_config.yaml)",
    )
    p.add_argument(
        "--market",
        action="append",
        default=None,
        help="Market symbol to observe (repeatable, e.g. --market SAMSUNGUSD)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Observation duration in minutes. Omit = run until SIGINT.",
    )
    p.add_argument(
        "--no-account",
        action="store_true",
        help="Skip the account-balance snapshot (useful for read-only keys)",
    )
    p.add_argument(
        "--probe-rwa",
        action="store_true",
        help="Probe SAMSUNGUSD / HYUNDAIUSD / SKHYNIXUSD / XAU / BRENTOIL and exit",
    )
    p.add_argument(
        "--schedule",
        choices=("crypto", "us", "kr"),
        default="crypto",
        help="Market-hours schedule used by snapshot metadata (default: crypto)",
    )
    p.add_argument(
        "--snapshot-interval",
        type=float,
        default=60.0,
        help="Full snapshot interval in seconds (default: 60)",
    )
    p.add_argument(
        "--bbo-interval",
        type=float,
        default=5.0,
        help="BBO log cadence in seconds (default: 5). Each tick pulls "
        "one REST order book per symbol, so don't go much below 5 or "
        "you'll trip the 429 rate limit.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Where to write JSONL output (default: from config or logs/lighter_observe)",
    )
    p.add_argument(
        "--depth-tiers",
        default="5,10,15,25,50,100,200",
        help="Comma-separated list of spread tiers (bp) for cumulative-depth aggregation. "
        "Default covers regular and RWA-weekend LPP tiers.",
    )
    p.add_argument(
        "--top-levels",
        type=int,
        default=5,
        help="Number of bid/ask levels to persist verbatim per snapshot (default: 5)",
    )
    p.add_argument(
        "--snapshot-book-depth",
        type=int,
        default=20,
        help="Order-book depth fetched per snapshot via REST (default: 20)",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (-v=INFO, -vv=DEBUG)",
    )
    return p


def _parse_tiers(spec: str) -> tuple:
    """Parse '5,10,15' into a tuple of Decimals. Reject empty / NaN."""
    from decimal import Decimal, InvalidOperation
    out = []
    for piece in (spec or "").split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            value = Decimal(piece)
        except InvalidOperation as exc:
            raise ValueError(f"invalid depth tier value: {piece!r}") from exc
        if value <= 0:
            raise ValueError(f"depth tier must be positive: {piece}")
        out.append(value)
    if not out:
        raise ValueError("--depth-tiers must contain at least one value")
    return tuple(out)


def _setup_logging(verbosity: int) -> None:
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


def _schedule_from_arg(name: str) -> MarketSchedule:
    return {
        "crypto": MarketSchedule.CRYPTO_24X7,
        "us": MarketSchedule.US_EQUITY,
        "kr": MarketSchedule.KR_EQUITY,
    }[name]


def _load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw


def _expand_env(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    s = value.strip()
    if s.startswith("${") and s.endswith("}"):
        return os.environ.get(s[2:-1], "")
    return s


def _gateway_kwargs_from(cfg: Dict[str, Any]) -> Dict[str, Any]:
    section = cfg.get("lighter") if isinstance(cfg.get("lighter"), dict) else cfg
    auth = (section or {}).get("auth", {}) or {}
    return {
        "testnet": section.get("testnet", False),
        "api_url": section.get("api_url"),
        "ws_url": section.get("ws_url"),
        "api_key_private_key": _expand_env(auth.get("api_key_private_key", "")),
        "account_index": _expand_env(auth.get("account_index", "")),
        "api_key_index": _expand_env(auth.get("api_key_index", "")),
    }


def _observe_section(cfg: Dict[str, Any]) -> Dict[str, Any]:
    section = cfg.get("observe", {}) or {}
    return section if isinstance(section, dict) else {}


async def _run_probe_only(gw_kwargs: Dict[str, Any]) -> int:
    gateway = LighterGateway(gw_kwargs)
    try:
        await gateway.initialize()
        rows = await run_rwa_probe(gateway, RWA_PROBE_SYMBOLS)
    finally:
        await gateway.close()
    missing = [r["symbol"] for r in rows if not r.get("found")]
    if missing:
        print(f"[PROBE] WARNING: Not found on Lighter: {missing}")
    return 0


async def _run_observe(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    markets: List[str] = args.market or []
    if not markets:
        print(
            "error: --market is required unless using --probe-rwa",
            file=sys.stderr,
        )
        return 2

    gw_kwargs = _gateway_kwargs_from(cfg)
    obs_cfg_raw = _observe_section(cfg)

    duration_sec: Optional[float] = None
    if args.duration is not None:
        duration_sec = float(args.duration) * 60.0

    output_dir = args.output_dir or obs_cfg_raw.get(
        "output_dir", "logs/lighter_observe"
    )
    snapshot_interval = float(
        args.snapshot_interval or obs_cfg_raw.get("snapshot_interval_s", 60)
    )
    log_book_every = float(
        args.bbo_interval or obs_cfg_raw.get("log_book_state_every_s", 1)
    )

    try:
        depth_tiers = _parse_tiers(args.depth_tiers)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    obs_cfg = ObservationConfig(
        symbols=markets,
        duration_sec=duration_sec,
        snapshot_interval_sec=snapshot_interval,
        log_book_every_sec=log_book_every,
        output_dir=Path(output_dir),
        include_account=not args.no_account,
        schedule=_schedule_from_arg(args.schedule),
        depth_tiers_bp=depth_tiers,
        top_levels=int(args.top_levels),
        snapshot_book_depth=int(args.snapshot_book_depth),
    )

    gateway = LighterGateway(gw_kwargs)
    ws = LighterWebSocket(ws_url=gw_kwargs.get("ws_url"), testnet=gw_kwargs.get("testnet", False))

    try:
        await gateway.initialize()
    except LighterGatewayError as exc:
        print(f"error: gateway init failed: {exc}", file=sys.stderr)
        await gateway.close()
        return 3
    observer = LighterObserver(gateway, ws, obs_cfg)
    try:
        await observer.run()
    except KeyboardInterrupt:
        # When signal handlers aren't available (Windows Proactor loop),
        # rely on Ctrl-C's KeyboardInterrupt and let shutdown happen via finally.
        observer.request_stop()
    return 0


async def _async_main(argv: List[str]) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    try:
        cfg = _load_config(args.config)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    gw_kwargs = _gateway_kwargs_from(cfg)

    if args.probe_rwa:
        return await _run_probe_only(gw_kwargs)
    return await _run_observe(args, cfg)


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    try:
        return asyncio.run(_async_main(argv))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
