"""Lighter exchange gateway (async, read-only).

Phase 1.0 scope: connect, observe, authenticate. No order submission.

Notable Lighter SDK quirks encoded here (cross-referenced against the
`crypto-trading-open` reference adapter under docs/reference/ or the
LighterRest class):

1.  `market_id` starts at 0 (ETH = 0). Never iterate from 1.
2.  Use `order_books()` for the master market list — `order_book_details()`
    only returns active markets and can silently miss inactive ones.
3.  Dynamic price precision: each market has its own `price_decimals`.
    `price_int = price × 10**price_decimals` when placing orders, and
    `quantity_multiplier = 10**(6 - price_decimals)`. We expose the raw
    `price_decimals` so callers can compute both.
4.  The SDK's ApiClient/SignerClient each hold aiohttp ClientSessions
    that do not always close cleanly. `close()` recursively walks the
    object graph and shuts every ClientSession it finds, avoiding
    "Unclosed client session" warnings on shutdown.
5.  Market info cache holds for 5 minutes. Batched access to market
    metadata without this cache will trigger 429 rate limits.
6.  SignerClient.check_client() is called at initialize() time to fail
    fast on a bad API private key. Even in read-only mode this is cheap
    sanity. No order transactions are ever signed from this module.

The gateway is intentionally async-only: the existing Hyperliquid
stack in this project is sync/threaded, and we do not try to bridge
the two. Callers drive this gateway from an asyncio event loop.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

try:
    import lighter
    from lighter import ApiClient, Configuration, SignerClient
    from lighter.api import AccountApi, OrderApi
    LIGHTER_SDK_AVAILABLE = True
except ImportError:  # pragma: no cover - import-guarded
    LIGHTER_SDK_AVAILABLE = False
    lighter = None  # type: ignore[assignment]
    ApiClient = None  # type: ignore[assignment]
    Configuration = None  # type: ignore[assignment]
    SignerClient = None  # type: ignore[assignment]
    AccountApi = None  # type: ignore[assignment]
    OrderApi = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

MAINNET_URL = "https://mainnet.zklighter.elliot.ai"
TESTNET_URL = "https://testnet.zklighter.elliot.ai"
MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"

MARKET_INFO_CACHE_TTL_SEC = 300.0


def _safe_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError):
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


class LighterGatewayError(RuntimeError):
    """Raised on unrecoverable Lighter connectivity / auth failures."""


class LighterGateway:
    """Async, read-only Lighter exchange gateway.

    Usage::

        gw = LighterGateway(config)
        await gw.initialize()
        book = await gw.get_orderbook("SAMSUNGUSD")
        await gw.close()
    """

    def __init__(self, config: Dict[str, Any]):
        if not LIGHTER_SDK_AVAILABLE:
            raise ImportError(
                "lighter SDK not installed. "
                "Install with: pip install git+https://github.com/elliottech/lighter-python.git"
            )

        self.config = dict(config or {})
        self.testnet: bool = bool(self.config.get("testnet", False))

        self.base_url: str = (
            self.config.get("api_url")
            or (TESTNET_URL if self.testnet else MAINNET_URL)
        )
        self.ws_url: str = (
            self.config.get("ws_url")
            or (TESTNET_WS_URL if self.testnet else MAINNET_WS_URL)
        )

        self.api_key_private_key: str = self.config.get("api_key_private_key", "") or ""
        self.account_index: Optional[int] = _safe_int(
            self.config.get("account_index"), default=None
        )
        self.api_key_index: int = _safe_int(self.config.get("api_key_index"), default=0) or 0

        self.api_client: Optional[ApiClient] = None
        self.account_api: Optional[AccountApi] = None
        self.order_api: Optional[OrderApi] = None
        self._signer_client: Optional[SignerClient] = None

        self._markets_cache: Dict[int, Dict[str, Any]] = {}
        self._symbol_to_market_index: Dict[str, int] = {}
        self._market_info_cache: Dict[str, Dict[str, Any]] = {}

        self._connected: bool = False
        self._signer_checked: bool = False

    # --------------------------------------------------------------
    # lifecycle
    # --------------------------------------------------------------

    @property
    def signer_client(self) -> Optional[SignerClient]:
        """Expose the SignerClient for components (e.g. ws) that need auth tokens."""
        return self._signer_client

    def is_connected(self) -> bool:
        return self._connected

    async def initialize(self) -> None:
        """Open the REST connection, load markets, optionally validate signer."""
        try:
            configuration = Configuration(host=self.base_url)
            self.api_client = ApiClient(configuration=configuration)
            self.account_api = AccountApi(self.api_client)
            self.order_api = OrderApi(self.api_client)

            if self.api_key_private_key:
                self._signer_client = SignerClient(
                    url=self.base_url,
                    account_index=self.account_index or 0,
                    api_private_keys={self.api_key_index: self.api_key_private_key},
                )
                err = self._signer_client.check_client()
                if err is not None:
                    raise LighterGatewayError(
                        f"SignerClient.check_client() failed: {err}. "
                        "Verify API_KEY_PRIVATE_KEY / LIGHTER_ACCOUNT_INDEX / "
                        "LIGHTER_API_KEY_INDEX match the keys registered on Lighter."
                    )
                self._signer_checked = True
                logger.info(
                    "SignerClient ready (account_index=%s, api_key_index=%s)",
                    self.account_index,
                    self.api_key_index,
                )
            else:
                logger.info(
                    "No api_key_private_key configured — running in public-data-only mode"
                )

            await self._load_markets()
            self._connected = True
            logger.info(
                "LighterGateway connected: %s (testnet=%s, markets=%d)",
                self.base_url,
                self.testnet,
                len(self._markets_cache),
            )
        except Exception:
            # Best-effort cleanup if initialize partially succeeded
            await self._close_quietly()
            raise

    async def close(self) -> None:
        await self._close_quietly()
        self._connected = False
        logger.info("LighterGateway closed")

    async def _close_quietly(self) -> None:
        if self._signer_client is not None:
            try:
                await self._signer_client.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("SignerClient.close() raised: %s", exc)
            try:
                await self._find_and_close_sessions(self._signer_client)
            except Exception as exc:  # noqa: BLE001
                logger.debug("SignerClient session sweep raised: %s", exc)

        if self.api_client is not None:
            try:
                await self.api_client.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("ApiClient.close() raised: %s", exc)
            try:
                await self._find_and_close_sessions(self.api_client)
            except Exception as exc:  # noqa: BLE001
                logger.debug("ApiClient session sweep raised: %s", exc)

        # Let loop drain any pending session-close callbacks
        try:
            await asyncio.sleep(0.1)
        except Exception:  # noqa: BLE001
            pass

    async def _find_and_close_sessions(self, obj: Any, visited: Optional[set] = None) -> None:
        """Recursively close any aiohttp ClientSession reachable from obj.

        The Lighter SDK's ApiClient / SignerClient each own one or more
        ClientSessions that the public close() methods sometimes leave
        open. Walking the object graph closes them and silences the
        ``Unclosed client session`` warnings on shutdown.
        """
        if visited is None:
            visited = set()
        obj_id = id(obj)
        if obj_id in visited:
            return
        visited.add(obj_id)

        try:
            import aiohttp  # local import — aiohttp ships as SDK dep
        except ImportError:
            return

        if isinstance(obj, aiohttp.ClientSession):
            if not obj.closed:
                try:
                    await obj.close()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Failed to close ClientSession: %s", exc)
            return

        if isinstance(obj, (list, tuple, set, frozenset)):
            for item in obj:
                await self._find_and_close_sessions(item, visited)
            return
        if isinstance(obj, dict):
            for item in obj.values():
                await self._find_and_close_sessions(item, visited)
            return

        attrs = getattr(obj, "__dict__", None)
        if attrs:
            for val in list(attrs.values()):
                try:
                    await self._find_and_close_sessions(val, visited)
                except Exception:  # noqa: BLE001
                    # Some descriptors raise on access — ignore
                    continue

    # --------------------------------------------------------------
    # markets
    # --------------------------------------------------------------

    async def _load_markets(self) -> None:
        """Populate the symbol ↔ market_index caches via ``order_books()``.

        MUST use ``order_books()``: ``order_book_details()`` only
        reports active markets and can silently miss RWA / low-volume
        ones. ETH is market_id=0, so never iterate from 1.
        """
        assert self.order_api is not None
        response = await self.order_api.order_books()
        markets = getattr(response, "order_books", None) or []
        loaded = 0
        for ob in markets:
            market_index = getattr(ob, "market_id", None)
            if market_index is None:
                market_index = getattr(ob, "market_index", None)
            if market_index is None:
                continue
            symbol = getattr(ob, "symbol", "") or ""
            info: Dict[str, Any] = {
                "market_index": int(market_index),
                "symbol": symbol,
                "status": getattr(ob, "status", None),
            }
            # price_decimals may be on the order_books response; if not,
            # it's filled in lazily by get_order_book_details()
            pd = getattr(ob, "price_decimals", None)
            if pd is not None:
                info["price_decimals"] = int(pd)
            self._markets_cache[int(market_index)] = info
            if symbol:
                self._symbol_to_market_index[symbol] = int(market_index)
            loaded += 1
        logger.info("Loaded %d markets from order_books()", loaded)

    @property
    def markets(self) -> Dict[int, Dict[str, Any]]:
        """Copy of market_index → market-info cache."""
        return dict(self._markets_cache)

    @property
    def symbol_to_market_index(self) -> Dict[str, int]:
        return dict(self._symbol_to_market_index)

    def get_market_index(self, symbol: str) -> Optional[int]:
        if not symbol:
            return None
        # Try exact first, then a canonicalized form
        idx = self._symbol_to_market_index.get(symbol)
        if idx is not None:
            return idx
        upper = symbol.upper()
        if upper in self._symbol_to_market_index:
            return self._symbol_to_market_index[upper]
        return None

    def get_symbol(self, market_index: int) -> Optional[str]:
        info = self._markets_cache.get(int(market_index))
        if info is None:
            return None
        return info.get("symbol") or None

    async def get_order_book_details(self, market_index: int) -> Dict[str, Any]:
        """Fetch detailed market info (price_decimals, size_decimals, ...).

        Result is cached for ``MARKET_INFO_CACHE_TTL_SEC`` (5 min) to
        avoid 429s when probing many markets. Cache key is market_index.
        """
        assert self.order_api is not None
        cache_key = str(market_index)
        now = time.time()
        cached = self._market_info_cache.get(cache_key)
        if cached and (now - cached.get("_ts", 0.0)) < MARKET_INFO_CACHE_TTL_SEC:
            return cached["info"]

        response = await self.order_api.order_book_details(market_id=market_index)
        detail = None
        details_list = getattr(response, "order_book_details", None) or []
        if details_list:
            detail = details_list[0]
        info: Dict[str, Any] = {"market_index": int(market_index)}
        if detail is not None:
            info["symbol"] = getattr(detail, "symbol", None)
            pd = getattr(detail, "price_decimals", None)
            if pd is not None:
                info["price_decimals"] = int(pd)
            sd = getattr(detail, "size_decimals", None)
            if sd is not None:
                info["size_decimals"] = int(sd)
            info["status"] = getattr(detail, "status", None)
            info["last_trade_price"] = _safe_decimal(
                getattr(detail, "last_trade_price", None)
            )
            info["daily_price_high"] = _safe_decimal(
                getattr(detail, "daily_price_high", None)
            )
            info["daily_price_low"] = _safe_decimal(
                getattr(detail, "daily_price_low", None)
            )
            info["daily_base_token_volume"] = _safe_decimal(
                getattr(detail, "daily_base_token_volume", None)
            )
        # Fold price_decimals back into the master cache so symbol→info is consistent
        if "price_decimals" in info and int(market_index) in self._markets_cache:
            self._markets_cache[int(market_index)].setdefault(
                "price_decimals", info["price_decimals"]
            )
        self._market_info_cache[cache_key] = {"info": info, "_ts": now}
        return info

    # --------------------------------------------------------------
    # public market data
    # --------------------------------------------------------------

    async def get_orderbook(
        self, symbol: str, limit: int = 20
    ) -> Optional[Dict[str, Any]]:
        """Return a normalized order book snapshot for ``symbol``.

        Returns ``{"symbol", "bids", "asks", "timestamp_ms"}``
        with ``bids``/``asks`` as lists of ``(price: Decimal, size: Decimal)``.
        Returns None if the symbol is unknown or the response is empty.
        """
        assert self.order_api is not None
        market_index = self.get_market_index(symbol)
        if market_index is None:
            logger.warning("Unknown symbol %s — market index not found", symbol)
            return None
        response = await self.order_api.order_book_orders(
            market_id=market_index, limit=limit
        )
        if response is None:
            return None
        bids_raw = getattr(response, "bids", None) or []
        asks_raw = getattr(response, "asks", None) or []
        bids: List[List[Decimal]] = []
        asks: List[List[Decimal]] = []
        for lvl in bids_raw[:limit]:
            price = _safe_decimal(getattr(lvl, "price", None))
            size = _safe_decimal(getattr(lvl, "remaining_base_amount", None))
            if price is not None and size is not None and price > 0 and size > 0:
                bids.append([price, size])
        for lvl in asks_raw[:limit]:
            price = _safe_decimal(getattr(lvl, "price", None))
            size = _safe_decimal(getattr(lvl, "remaining_base_amount", None))
            if price is not None and size is not None and price > 0 and size > 0:
                asks.append([price, size])
        return {
            "symbol": symbol,
            "market_index": market_index,
            "bids": bids,
            "asks": asks,
            "timestamp_ms": int(time.time() * 1000),
        }

    async def get_server_time(self) -> Dict[str, Any]:
        """Estimate server time via REST round-trip.

        Lighter's SDK does not expose a dedicated /time endpoint, so we
        measure the round-trip of a cheap call (order_books) and return
        the RTT plus a best-effort server timestamp if the SDK response
        carries one.

        Returns ``{"local_ms", "rtt_ms", "server_ms" (optional)}``.
        """
        assert self.order_api is not None
        t0 = time.perf_counter()
        resp = await self.order_api.order_books()
        t1 = time.perf_counter()
        rtt_ms = (t1 - t0) * 1000.0
        server_ms: Optional[int] = None
        ts_attr = getattr(resp, "timestamp", None) or getattr(resp, "server_time", None)
        if ts_attr is not None:
            try:
                server_ms = int(ts_attr)
            except (TypeError, ValueError):
                server_ms = None
        out: Dict[str, Any] = {
            "local_ms": int(time.time() * 1000),
            "rtt_ms": rtt_ms,
        }
        if server_ms is not None:
            out["server_ms"] = server_ms
        return out

    # --------------------------------------------------------------
    # account data (requires signer)
    # --------------------------------------------------------------

    async def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Fetch raw account metadata by account_index.

        Useful as a sanity check that the configured account exists and
        is reachable. Returns None if account_index is unset.
        """
        if self.account_index is None:
            logger.warning("account_index not configured — skipping account info")
            return None
        assert self.account_api is not None
        response = await self.account_api.account(
            by="index", value=str(self.account_index)
        )
        accounts = getattr(response, "accounts", None) or []
        if not accounts:
            return None
        acct = accounts[0]
        return {
            "account_index": self.account_index,
            "l1_address": getattr(acct, "l1_address", None),
            "account_type": getattr(acct, "account_type", None),
            "status": getattr(acct, "status", None),
            "collateral": _safe_decimal(getattr(acct, "collateral", None)),
            "available_balance": _safe_decimal(
                getattr(acct, "available_balance", None)
            ),
            "total_order_count": getattr(acct, "total_order_count", None),
        }

    async def get_account_balance(self) -> Optional[Dict[str, Any]]:
        """Return normalized USDC balance, or None if no account configured."""
        info = await self.get_account_info()
        if info is None:
            return None
        collateral = info.get("collateral") or Decimal("0")
        available = info.get("available_balance") or Decimal("0")
        locked = collateral - available
        if locked < 0:
            locked = Decimal("0")
        return {
            "currency": "USDC",
            "free": available,
            "used": locked,
            "total": collateral,
        }

    async def get_account_positions(self) -> List[Dict[str, Any]]:
        """List active perp positions across all markets for the configured account.

        Wraps ``AccountApi.account(by="index", value=str(account_index))``
        and pulls the ``positions`` field from the returned
        ``DetailedAccount``. Each entry is normalised to a dict with
        ``market_index`` / ``symbol`` / ``sign`` (1=long, -1=short,
        0=flat) / ``base`` (Decimal, signed) / ``avg_entry_price``
        (Decimal) / ``position_value_usdc`` / ``unrealized_pnl``.
        Empty list when the account is not configured or no positions
        exist.
        """
        if self.account_index is None or self.account_api is None:
            return []
        response = await self.account_api.account(
            by="index", value=str(self.account_index)
        )
        accounts = getattr(response, "accounts", None) or []
        if not accounts:
            return []
        acct = accounts[0]
        positions_raw = getattr(acct, "positions", None) or []
        out: List[Dict[str, Any]] = []
        for p in positions_raw:
            sign = int(getattr(p, "sign", 0) or 0)
            size_str = getattr(p, "position", "0") or "0"
            try:
                size = Decimal(str(size_str))
            except (ValueError, ArithmeticError):
                size = Decimal(0)
            # Lighter reports magnitude in ``position`` and direction
            # in ``sign``. Combine into a signed base amount so callers
            # can sum across markets without re-parsing.
            signed_base = size if sign >= 0 else -size
            out.append({
                "market_index": int(getattr(p, "market_id", 0) or 0),
                "symbol": getattr(p, "symbol", "") or "",
                "sign": sign,
                "base": signed_base,
                "avg_entry_price": _safe_decimal(getattr(p, "avg_entry_price", None)),
                "position_value_usdc": _safe_decimal(getattr(p, "position_value", None)),
                "unrealized_pnl": _safe_decimal(getattr(p, "unrealized_pnl", None)),
                "open_order_count": int(getattr(p, "open_order_count", 0) or 0),
            })
        return out

    async def get_open_orders(
        self,
        market_index: int,
        auth_deadline_sec: int = 600,
    ) -> List[Dict[str, Any]]:
        """List open orders for the configured account on one market.

        Wraps ``OrderApi.account_active_orders``, which is an
        authenticated endpoint — we mint a short-lived auth token via
        ``SignerClient.create_auth_token_with_expiry`` and pass it as
        the ``auth`` query parameter. Empty list when the signer or
        account is unconfigured.

        Returned dict shape mirrors a subset of ``lighter.models.Order``:
        ``market_index`` / ``order_index`` / ``client_order_index`` /
        ``side`` ("buy"/"sell") / ``price`` / ``initial_base_amount`` /
        ``filled_base_amount`` / ``remaining_base_amount`` / ``status``.
        """
        if (
            self._signer_client is None
            or self.account_index is None
            or self.order_api is None
        ):
            return []
        # create_auth_token_with_expiry returns (token, error) — sync helper.
        auth_token, err = self._signer_client.create_auth_token_with_expiry(
            deadline=int(auth_deadline_sec)
        )
        if err is not None or not auth_token:
            raise LighterGatewayError(f"create_auth_token failed: {err}")
        response = await self.order_api.account_active_orders(
            account_index=int(self.account_index),
            market_id=int(market_index),
            auth=auth_token,
        )
        orders_raw = getattr(response, "orders", None) or []
        out: List[Dict[str, Any]] = []
        for o in orders_raw:
            out.append({
                "market_index": int(getattr(o, "market_index", market_index) or market_index),
                "order_index": int(getattr(o, "order_index", 0) or 0),
                "client_order_index": int(getattr(o, "client_order_index", 0) or 0),
                "side": getattr(o, "side", None) or (
                    "sell" if bool(getattr(o, "is_ask", False)) else "buy"
                ),
                "price": _safe_decimal(getattr(o, "price", None)),
                "initial_base_amount": _safe_decimal(getattr(o, "initial_base_amount", None)),
                "filled_base_amount": _safe_decimal(getattr(o, "filled_base_amount", None)),
                "remaining_base_amount": _safe_decimal(getattr(o, "remaining_base_amount", None)),
                "status": getattr(o, "status", None),
            })
        return out

    async def cancel_order_by_index(
        self,
        market_index: int,
        order_index: int,
    ) -> Dict[str, Any]:
        """Cancel a single resting order by its on-chain order_index.

        Thin wrapper around ``SignerClient.cancel_order``. Returns
        ``{"ok": True, "tx": ...}`` on success or
        ``{"ok": False, "error": ...}`` on failure. We *don't* raise
        on the SDK's (None, None, error) tuple here — recovery code
        wants to attempt every order in a list and surface partial
        failure rather than abort on the first one.
        """
        if self._signer_client is None:
            return {"ok": False, "error": "signer not configured"}
        result = await self._signer_client.cancel_order(
            market_index=int(market_index),
            order_index=int(order_index),
        )
        if isinstance(result, tuple) and len(result) >= 3 and result[2] is not None:
            return {"ok": False, "error": str(result[2])}
        return {"ok": True, "tx": result}

    # --------------------------------------------------------------
    # config loading helpers
    # --------------------------------------------------------------

    @classmethod
    def from_config_file(cls, path: str) -> "LighterGateway":
        """Convenience factory: load yaml, expand env vars, instantiate."""
        import yaml  # lazy import — only needed on this path

        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        lighter_cfg = raw.get("lighter", raw)
        auth = lighter_cfg.get("auth", {}) or {}
        flat: Dict[str, Any] = {
            "testnet": lighter_cfg.get("testnet", False),
            "api_url": lighter_cfg.get("api_url"),
            "ws_url": lighter_cfg.get("ws_url"),
            "api_key_private_key": _expand_env(auth.get("api_key_private_key", "")),
            "account_index": _expand_env(auth.get("account_index", "")),
            "api_key_index": _expand_env(auth.get("api_key_index", "")),
        }
        return cls(flat)


def _expand_env(value: Any) -> Any:
    """Expand ``${ENV_VAR}`` placeholders using os.environ.

    Bare strings are returned as-is. Placeholder with no env value
    resolves to empty string (so downstream can detect "unconfigured").
    """
    if not isinstance(value, str):
        return value
    s = value.strip()
    if s.startswith("${") and s.endswith("}"):
        var = s[2:-1]
        return os.environ.get(var, "")
    return s
