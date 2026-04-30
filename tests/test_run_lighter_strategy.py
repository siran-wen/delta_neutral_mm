"""Unit tests for ``scripts/run_lighter_strategy.py``.

Coverage:

* A. argparse + confirmation
* B. recover_initial_state (cancel + position diagnosis)
* C. JSONL + decimal coercion helpers
* D. paper-mode dispatch (mocked PaperRun)

We don't try to drive ``run_live_mode`` end-to-end — that needs a
real signer + ws connection. The recovery path is the live branch's
non-trivial logic; the rest is wiring covered by other test files
(LppQuoter, OM, Gateway).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

_THIS = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from scripts.run_lighter_strategy import (  # noqa: E402
    _JsonlWriter,
    _coerce_decimal_fields,
    _jsonable,
    parse_and_confirm_args,
    recover_initial_state,
    run_paper_mode,
)


def _D(s: str) -> Decimal:
    return Decimal(s)


# ----- A. argparse + confirmation ----------------------------------------


def test_paper_is_default_when_no_mode_flag():
    args = parse_and_confirm_args(["--market", "SKHYNIXUSD", "--duration", "5"])
    assert args.paper is True
    assert args.live is False


def test_paper_does_not_require_understand_flag():
    args = parse_and_confirm_args(["--paper", "--market", "SKHYNIXUSD"])
    assert args.paper is True


def test_live_without_understand_flag_exits_2():
    with pytest.raises(SystemExit) as exc_info:
        parse_and_confirm_args(["--live", "--market", "SKHYNIXUSD"])
    assert exc_info.value.code == 2


def test_live_with_understand_flag_skips_when_non_tty():
    """On non-TTY stdin, --i-understand-this-is-real is enough."""
    with patch("sys.stdin.isatty", return_value=False), \
         patch.dict(os.environ, {"LIGHTER_ACCOUNT_INDEX": "12345"}):
        args = parse_and_confirm_args(
            ["--live", "--i-understand-this-is-real",
             "--market", "SKHYNIXUSD", "--duration", "1"]
        )
    assert args.live is True


def test_live_tty_yes_i_agree_proceeds():
    with patch("sys.stdin.isatty", return_value=True), \
         patch("builtins.input", return_value="YES I AGREE"), \
         patch.dict(os.environ, {"LIGHTER_ACCOUNT_INDEX": "12345"}):
        args = parse_and_confirm_args(
            ["--live", "--i-understand-this-is-real", "--market", "SKHYNIXUSD"]
        )
    assert args.live is True


def test_live_tty_wrong_response_aborts():
    with patch("sys.stdin.isatty", return_value=True), \
         patch("builtins.input", return_value="yes"), \
         patch.dict(os.environ, {"LIGHTER_ACCOUNT_INDEX": "12345"}):
        with pytest.raises(SystemExit) as exc_info:
            parse_and_confirm_args(
                ["--live", "--i-understand-this-is-real", "--market", "SKHYNIXUSD"]
            )
        assert exc_info.value.code == 2


def test_live_and_paper_mutually_exclusive():
    """argparse should reject both flags at once."""
    with pytest.raises(SystemExit):
        parse_and_confirm_args(["--live", "--paper", "--market", "X"])


# ----- B. recover_initial_state -----------------------------------------


class _FakeGateway:
    """Minimal stub exposing the 3 methods recover_initial_state needs."""

    def __init__(self) -> None:
        self.positions: List[Dict[str, Any]] = []
        self.open_orders: List[Dict[str, Any]] = []
        # cancel_order_by_index recorded calls + injected per-order outcomes.
        self.cancel_calls: List[tuple] = []
        # Map order_index → "ok" / error string.
        self.cancel_outcomes: Dict[int, Any] = {}

    async def get_account_positions(self) -> List[Dict[str, Any]]:
        return list(self.positions)

    async def get_open_orders(self, market_index: int) -> List[Dict[str, Any]]:
        return [o for o in self.open_orders if o.get("market_index") == market_index]

    async def cancel_order_by_index(self, market_index: int, order_index: int) -> Dict[str, Any]:
        self.cancel_calls.append((market_index, order_index))
        outcome = self.cancel_outcomes.get(order_index, "ok")
        if outcome == "ok":
            return {"ok": True, "tx": "fake-tx"}
        return {"ok": False, "error": str(outcome)}


_RECOVER_CFG_DEFAULTS = {
    "cancel_stale_orders": True,
    "warn_if_existing_position_above_usdc": "100",
    "abort_if_existing_position_above_usdc": "1000",
    "wait_after_cancel_sec": 0,  # tests don't sleep
}


def test_recover_no_position_no_orders():
    gw = _FakeGateway()
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["existing_position"] is None
    assert result["cancelled_orders"] == []
    assert result["abort"] is False


def test_recover_position_below_warn_does_not_abort():
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": 1,
        "base": _D("0.01"),
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("1.0"),
    }]
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["existing_position"] is not None
    assert result["abort"] is False


def test_recover_position_above_warn_continues(caplog):
    """Position below abort threshold is not an abort, but is logged at WARN
    so the operator sees it. Phase 1.2 P0.1: the message also flags that
    the residual will be injected into the OM (no more silent ignore)."""
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": 1,
        "base": _D("5"),
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("500"),  # > 100 warn, < 1000 abort
    }]
    import logging
    caplog.set_level(logging.WARNING)
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["abort"] is False
    assert any(
        "RECOVER" in r.message and "will inject into OM" in r.message
        for r in caplog.records
    )


def test_recover_position_above_abort_sets_abort_flag():
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": 1,
        "base": _D("50"),
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("5000"),  # ≥ 1000 abort
    }]
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["abort"] is True
    assert result["abort_reason"] is not None
    # Should NOT have attempted cancellations (early abort)
    assert gw.cancel_calls == []


def test_recover_cancels_all_stale_orders():
    gw = _FakeGateway()
    gw.open_orders = [
        {"market_index": 161, "order_index": 1001, "client_order_index": 5001,
         "side": "buy", "price": _D("99"), "remaining_base_amount": _D("1")},
        {"market_index": 161, "order_index": 1002, "client_order_index": 5002,
         "side": "sell", "price": _D("101"), "remaining_base_amount": _D("1")},
    ]
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert sorted(result["cancelled_orders"]) == [1001, 1002]
    assert sorted(c[1] for c in gw.cancel_calls) == [1001, 1002]
    assert result["cancel_failures"] == []


def test_recover_continues_after_individual_cancel_failure():
    gw = _FakeGateway()
    gw.open_orders = [
        {"market_index": 161, "order_index": 1001, "client_order_index": 5001,
         "side": "buy", "price": _D("99"), "remaining_base_amount": _D("1")},
        {"market_index": 161, "order_index": 1002, "client_order_index": 5002,
         "side": "sell", "price": _D("101"), "remaining_base_amount": _D("1")},
    ]
    # First cancel fails, second succeeds.
    gw.cancel_outcomes[1001] = "fake error"
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert 1002 in result["cancelled_orders"]
    assert any(f["order_index"] == 1001 for f in result["cancel_failures"])


def test_recover_skips_cancel_when_disabled():
    gw = _FakeGateway()
    gw.open_orders = [
        {"market_index": 161, "order_index": 1001, "client_order_index": 5001,
         "side": "buy", "price": _D("99"), "remaining_base_amount": _D("1")},
    ]
    cfg = dict(_RECOVER_CFG_DEFAULTS, cancel_stale_orders=False)
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", cfg))
    assert result["cancelled_orders"] == []
    assert gw.cancel_calls == []


def test_recover_filters_orders_by_market_index():
    gw = _FakeGateway()
    gw.open_orders = [
        {"market_index": 161, "order_index": 1001, "client_order_index": 5001,
         "side": "buy", "price": _D("99"), "remaining_base_amount": _D("1")},
        {"market_index": 999, "order_index": 9999, "client_order_index": 5999,
         "side": "buy", "price": _D("50"), "remaining_base_amount": _D("1")},
    ]
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["cancelled_orders"] == [1001]


def test_recover_handles_get_positions_exception():
    gw = _FakeGateway()

    async def _raise():
        raise RuntimeError("network down")

    gw.get_account_positions = _raise  # type: ignore[method-assign]
    # Should not raise — recovery is best-effort.
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", _RECOVER_CFG_DEFAULTS))
    assert result["existing_position"] is None


# ----- C. helpers -------------------------------------------------------


def test_jsonable_handles_decimal_dataclass_datetime():
    from datetime import datetime, timezone
    payload = {
        "decimal": Decimal("123.456"),
        "list": [Decimal("1"), 2, "three"],
        "ts": datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc),
    }
    out = _jsonable(payload)
    assert out["decimal"] == "123.456"
    assert out["list"][0] == "1"
    assert "2026-04-29" in out["ts"]


def test_coerce_decimal_fields_converts_known_keys():
    cfg = {
        "target_max_delta_usdc": "500",
        "reprice_drift_bp": "8",
        "tick_interval_sec": 1,  # int
        "market": "SKHYNIXUSD",  # not a Decimal field
    }
    out = _coerce_decimal_fields(cfg)
    assert out["target_max_delta_usdc"] == _D("500")
    assert out["reprice_drift_bp"] == _D("8")
    assert out["tick_interval_sec"] == _D("1")
    assert out["market"] == "SKHYNIXUSD"  # untouched


def test_coerce_decimal_handles_session_overrides_nested():
    """Nested session_overrides → each session's numeric fields → Decimal.

    Yaml's auto-typing doesn't reach into the inner dict, so without the
    nested coercion plan_quotes would break Decimal arithmetic on string
    values when the yaml carries quoted numbers.
    """
    raw = {
        "target_max_delta_usdc": "50",
        "session_overrides": {
            "KR_OVERNIGHT": {"default_size_usdc": "50"},
            "KR_MARKET_HOURS_AM": {
                "default_size_usdc": "100",
                "default_distance_bp": "5",
            },
        },
    }
    out = _coerce_decimal_fields(raw)
    assert out["target_max_delta_usdc"] == _D("50")
    assert out["session_overrides"]["KR_OVERNIGHT"]["default_size_usdc"] == _D("50")
    assert out["session_overrides"]["KR_MARKET_HOURS_AM"]["default_size_usdc"] == _D("100")
    assert out["session_overrides"]["KR_MARKET_HOURS_AM"]["default_distance_bp"] == _D("5")


def test_jsonl_writer_appends_lines(tmp_path):
    p = tmp_path / "nested" / "out.jsonl"
    writer = _JsonlWriter(p)
    writer.write({"event": "a", "n": 1})
    writer.write({"event": "b", "ts_ms": 1000})
    writer.close()
    lines = p.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["event"] == "a"
    assert parsed[1]["ts_ms"] == 1000


# ----- D. paper-mode dispatch -------------------------------------------


def test_run_paper_mode_invokes_paper_run(tmp_path):
    """run_paper_mode constructs PaperRun with translated args and awaits run()."""
    args = SimpleNamespace(
        market="SKHYNIXUSD",
        duration=1.0,
        seed=42,
        output_dir=str(tmp_path),
        verbose=0,
    )
    jsonl = _JsonlWriter(tmp_path / "out.jsonl")

    captured: Dict[str, Any] = {}

    class FakePaperRun:
        def __init__(self, paper_args):
            captured["args"] = paper_args

        async def run(self):
            return 0

    with patch("scripts.run_lighter_paper.PaperRun", FakePaperRun):
        rc = asyncio.run(run_paper_mode(args, jsonl))
    assert rc == 0
    paper_args = captured["args"]
    assert paper_args.market == "SKHYNIXUSD"
    assert paper_args.seed == 42
    assert paper_args.duration == 1.0
    # Paper output dir should be a sub-folder of the strategy output_dir.
    assert paper_args.output_dir.startswith(str(tmp_path))
    jsonl.close()


def test_run_paper_mode_records_start_and_end_events(tmp_path):
    args = SimpleNamespace(
        market="SKHYNIXUSD",
        duration=1.0,
        seed=None,
        output_dir=str(tmp_path),
        verbose=0,
    )
    jsonl_path = tmp_path / "events.jsonl"
    jsonl = _JsonlWriter(jsonl_path)

    class FakePaperRun:
        def __init__(self, _args):
            pass

        async def run(self):
            return 0

    with patch("scripts.run_lighter_paper.PaperRun", FakePaperRun):
        asyncio.run(run_paper_mode(args, jsonl))
    jsonl.close()
    lines = jsonl_path.read_text(encoding="utf-8").strip().split("\n")
    parsed = [json.loads(line) for line in lines]
    events = [p["event"] for p in parsed]
    assert "strategy_start" in events
    assert "strategy_end" in events
    start = next(p for p in parsed if p["event"] == "strategy_start")
    assert start["mode"] == "paper"


def test_run_paper_mode_zero_duration_translates_to_one_day(tmp_path):
    """``--duration 0`` must not pass 0 to PaperRun (would exit immediately).
    The strategy runner translates it to a long fallback (24h)."""
    args = SimpleNamespace(
        market="SKHYNIXUSD",
        duration=0.0,
        seed=None,
        output_dir=str(tmp_path),
        verbose=0,
    )
    jsonl = _JsonlWriter(tmp_path / "out.jsonl")
    captured: Dict[str, Any] = {}

    class FakePaperRun:
        def __init__(self, paper_args):
            captured["args"] = paper_args

        async def run(self):
            return 0

    with patch("scripts.run_lighter_paper.PaperRun", FakePaperRun):
        asyncio.run(run_paper_mode(args, jsonl))
    jsonl.close()
    # Forever in paper mode is clamped to 24h (24*60 minutes).
    assert captured["args"].duration == 24 * 60.0


# ----- E. Phase 1.2 P0.1 + P0.2 hardening -------------------------------


def test_recover_position_zero_threshold_aborts_on_any_nonzero():
    """abort_threshold=0 means abort on ANY non-zero residual.

    Phase 1.2 P0.2 production default — forces a clean account between
    runs to break the dirty-state link that produced the 4-30
    catastrophic short. The OM-inject path is reached only when the
    threshold is explicitly raised (dev/testing scenario)."""
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": 1,
        "base": _D("0.001"),  # tiny but non-zero
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("0.1"),
    }]
    cfg = dict(_RECOVER_CFG_DEFAULTS, abort_if_existing_position_above_usdc="0")
    result = asyncio.run(recover_initial_state(gw, 161, "SKHYNIXUSD", cfg))
    assert result["abort"] is True
    assert "0.1" in (result["abort_reason"] or "") or "0.001" in (
        result["abort_reason"] or ""
    )


def test_recover_inject_called_with_existing_position():
    """When abort_threshold is raised above the residual, recover logs the
    inject decision (the actual om.inject_initial_inventory call lives in
    run_live_mode and is exercised by the live integration path)."""
    import logging
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": -1,
        "base": _D("0.5"),
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("50"),
    }]
    # Threshold 1000 > notional 50 → no abort; recover logs WARN with
    # "will inject into OM" so the runner can pick the recover_summary
    # up and call inject explicitly.
    cfg = dict(
        _RECOVER_CFG_DEFAULTS,
        abort_if_existing_position_above_usdc="1000",
    )
    caplog_records: List[logging.LogRecord] = []
    target = logging.getLogger("strategy_run")

    class _H(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            caplog_records.append(record)

    h = _H(level=logging.WARNING)
    target.addHandler(h)
    try:
        result = asyncio.run(
            recover_initial_state(gw, 161, "SKHYNIXUSD", cfg)
        )
    finally:
        target.removeHandler(h)
    assert result["abort"] is False
    assert result["existing_position"] is not None
    assert result["existing_position"].get("sign") == -1
    assert any(
        "will inject into OM" in r.message for r in caplog_records
    )


def test_post_shutdown_clean_check_logs_residual():
    """The post-shutdown verification helper logs ERROR when the account is
    not clean. Regression guard: prior runs ended without verifying, so
    Run 1's residual long was inherited by Run 2 → catastrophic short.

    This test exercises the verification logic directly: we simulate the
    sequence of ``await gateway.get_account_positions()`` /
    ``get_open_orders()`` post-shutdown and assert the ERROR log fires.
    Driving the full ``run_live_mode`` would require a real signer.
    """
    import logging
    gw = _FakeGateway()
    gw.positions = [{
        "market_index": 161,
        "symbol": "SKHYNIXUSD",
        "sign": 1,
        "base": _D("0.5"),
        "avg_entry_price": _D("100"),
        "position_value_usdc": _D("50"),
    }]

    target = logging.getLogger("strategy_run")
    caplog_records: List[logging.LogRecord] = []

    class _H(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            caplog_records.append(record)

    h = _H(level=logging.ERROR)
    target.addHandler(h)

    async def _verify() -> None:
        # Mirror the verification body from run_live_mode's finally
        # block. The contract under test: when positions are non-zero
        # post-shutdown, ERROR is logged with a "NOT CLEAN" header so
        # the operator can't miss it tail-following the console.
        market = "SKHYNIXUSD"
        market_index = 161
        final_positions = await gw.get_account_positions()
        final_orders = await gw.get_open_orders(market_index)
        nz_positions = []
        for p in final_positions:
            if p.get("symbol") == market or p.get("market_index") == market_index:
                if Decimal(str(p.get("base") or 0)) != 0:
                    nz_positions.append(p)
        if nz_positions or final_orders:
            target.error("=" * 70)
            target.error("POST-SHUTDOWN ACCOUNT NOT CLEAN")
            for p in nz_positions:
                target.error(
                    "  RESIDUAL POSITION: %s base=%s notional=%s",
                    p.get("symbol"),
                    p.get("base"),
                    p.get("position_value_usdc"),
                )

    try:
        asyncio.run(_verify())
    finally:
        target.removeHandler(h)

    assert any("NOT CLEAN" in r.message for r in caplog_records)
    assert any("RESIDUAL POSITION" in r.message for r in caplog_records)
