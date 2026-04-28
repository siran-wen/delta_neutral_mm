"""Lighter execution layer (Phase 1.1 batch 3).

Houses the async order manager that drives ``lighter.SignerClient``
and tracks every active order through its lifecycle. Sits one level
below the strategy / lpp_quoter loop and one level above the SDK
wrapper in ``gateways/lighter_gateway.py``.

Sibling-empty for now; future ``execution/hyperliquid/`` reorganisation
would parallel this layout.
"""
