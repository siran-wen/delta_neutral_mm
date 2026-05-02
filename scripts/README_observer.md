# LPP observer toolkit

22-market read-only observer + post-run MM-behaviour analyzer for the
Lighter LPP pool. Designed to run on a separate PC from the strategy
box so neither competes for the same outbound IP's WAF budget.

## Architecture

```
+------------------------+      +--------------------------+
| Observation PC         |      | Strategy PC              |
|                        |      |                          |
|  observe_all_markets   |      |  run_lighter_strategy    |
|     ↓                  |      |     × 3 (per market)     |
|  observe_lighter.py    |      |                          |
|     ↓ JSONL/day        |      |                          |
|  analyze_observer.py   |      |                          |
|     ↓ analysis.json +  |      |                          |
|     mm_profile.json    |      |                          |
+------------------------+      +--------------------------+
       ^                                ^
       |                                |
   shared outbound IP would trip WAF — keep PCs separate.
```

* **Observation PC** runs ``observe_all_markets.ps1`` once a day for a
  24h window and analyzes the JSONL after.
* **Strategy PC** runs the production strategy (3 yamls × 3
  processes), unaffected by observer traffic.

## One-time configuration

1. Clone the repo on the observation PC.
2. Run ``scripts\setup_env_permanent.ps1`` if you also want this PC
   to be able to launch the strategy. **Not required for observation
   only** — observer uses ``--no-account`` and never touches the signer.
3. Confirm Python is available:

   ```powershell
   $env:LOCALAPPDATA + '\Programs\Python\Python312\python.exe' | Test-Path
   ```

   The launcher autodetects 3.11 / 3.12 / 3.13 user-scope installs and
   refuses the WindowsApps Store stub.

## Daily run flow

```powershell
# 1. Start the 24h observation window (run from any PowerShell;
#    launcher uses absolute paths internally).
scripts\observe_all_markets.ps1                  # default 1440 min, 90 s BBO

# 2. Wait for it to finish (or Ctrl-C early — JSONL is flushed on
#    every snapshot so partial runs are usable).

# 3. Analyze the JSONL the observer dropped in
#    logs\lpp_observer_daily\.
scripts\python.exe scripts\analyze_observer.py `
    logs\lpp_observer_daily\<observer-output>.jsonl `
    --your-size-usdc 50

#    The analyzer writes <stem>.analysis.json + <stem>.mm_profile.json
#    next to the input and prints the Markdown summary on stdout.
```

If the observer drops more than one JSONL (one per session_start —
e.g. after a network hiccup), pass them all to the analyzer:

```powershell
scripts\python.exe scripts\analyze_observer.py `
    logs\lpp_observer_daily\*.jsonl --your-size-usdc 50
```

The analyzer folds them into a single window.

## REST cadence vs WAF

Lighter's CDN WAF rate-limits at roughly **100 REST calls / 5 min /
outbound IP**. The observer issues one REST per market per
``BboInterval``; cumulative load:

| BboInterval | calls / market / min | calls / 5 min for 22 markets | Verdict |
|---|---|---|---|
| 30 s | 2.0 | 220 | trip-rate |
| 60 s | 1.0 | 110 | over the cap |
| 75 s | 0.8 | 88 | borderline |
| **90 s** | **0.67** | **73** | **default — comfortable margin** |
| 120 s | 0.5 | 55 | very safe |

Snapshot interval is independent (one full book pull per market per
``SnapshotInterval``, default 60 s — these calls are folded into the
same REST budget). Don't raise either cadence below 90/60 unless the
observation PC has its own outbound IP.

## Calibration

The analyzer has one calibration anchor — SKHYNIX, our flagship
production market — set via the ``SKHYNIX_BASELINE_SHARE`` constant:

```python
SKHYNIX_BASELINE_SHARE = 0.086    # = 8.6%, measured 5-2 live at $50
SKHYNIX_BASELINE_SIZE_USDC = 50
```

When SKHYNIX appears in the analyzer's input it computes a *raw* share
from the observer-derived competitor depth, then scales every market's
raw share by ``baseline / raw_skhynix`` so the resulting numbers are
directly comparable to the live 8.6% reference. Without SKHYNIX in the
input the multiplier falls back to 1.0 and the report says so.

**Caveat**: the calibration assumes our 5-2 share is the truth. If a
later production run shows a different share at the same $50 size,
re-update the constant and re-analyze.

## 22-market tier table

| Tier | Weekly pool | Markets |
|---|---|---|
| T6 (KR RWA) | $12,269 each | SKHYNIXUSD, SAMSUNGUSD, HYUNDAIUSD |
| T6 (commodities) | $23-38k | XAUUSD, XAGUSD, BRENTOIL, WTI |
| T5 (US benchmark) | $17,000 | SPYUSD |
| T4 (mid-cap) | $4-9k | MON, ZEC, ZRO, CRCL, AAVE, TSLA, XPT |
| T3 (small-cap) | $3,000 each | NVDA, COIN, HOOD, META, MSFT, PLTR, NATGAS |

Total 22 markets. Pool numbers reflect the 5-2 LPP schedule and are
mirrored in ``scripts/analyze_observer.py:POOL_BY_MARKET`` —
update both together when LPP weights change.

## How the MM identifier works

For each market, the analyzer:

1. Walks every snapshot's ``bid_levels`` and ``ask_levels``.
2. Records each level as a ``(snapshot_idx, side, size_usdc, distance_from_bbo_bp)`` event.
3. Sorts events by size and groups them into clusters: an event joins
   the current cluster iff ``size / cluster_min ≤ 1 + 2 × tolerance``
   (default ``tolerance = 0.03``, i.e. cluster width capped at ~6%).
4. Drops clusters with fewer than 3 events.
5. For each cluster, derives:
   * ``bid_count`` / ``ask_count``
   * ``is_two_sided`` — both ≥ 3 events
   * ``time_persistence_pct`` — fraction of snapshots seeing the cluster
   * ``max_levels_per_snapshot`` — peak simultaneous levels at this size
   * ``median_distance_from_bbo_bp`` — typical distance from the BBO
   * ``classification`` — one of:
       * ``lpp_mm`` — two-sided, ≥ 50% persistence, ≥ 10 events
       * ``one_sided_trader`` — only on one side
       * ``institutional`` — centroid ≥ $5k (excluded from competitor
         tally)
       * ``unknown``

The "your relative position" block sums every ``lpp_mm`` cluster's
centroid as ``lpp_eligible_existing_usdc``, ranks our nominal size in
the cluster distribution, and recommends the main MM's median
distance as our quote distance.

### Tuning the cluster thresholds

Defaults (constants at the top of ``analyze_observer.py``):

* ``CLUSTER_TOLERANCE = 0.03`` — ±3%, absorbs sub-cent price drift
  on a constant ``size_base``.
* ``CLUSTER_MIN_OCCURRENCES = 3`` — three is the smallest count where
  "MM" is more likely than "noise".
* ``LPP_PERSISTENCE_PCT_FLOOR = 50`` — visible in at least half the
  snapshots.
* ``LPP_TOTAL_OCCURRENCES_FLOOR = 10`` — combined bid+ask events.
* ``INSTITUTIONAL_SIZE_USDC = 5000`` — anyone bigger doesn't compete
  with our LPP-style quote.

Increase ``CLUSTER_MIN_OCCURRENCES`` if you observe noise being
mis-clustered. Tighten ``CLUSTER_TOLERANCE`` if MMs in your sample
seem to pin sizes more precisely than ±3%.

## Known limitations

* **Single IP, observation only**. Co-locating the observer on the
  strategy PC will exceed the WAF budget. Use a separate machine or a
  VPN exit on a different IP.
* **WS depth not subscribed** — the observer pulls depth via REST per
  ``BboInterval``. WS only carries top-of-book + trades. This is a
  deliberate REST-budget choice; deeper-than-top-5 data needs REST.
* **Schedule = ``kr``** by default — the LPP pool clearly weights KR
  RWAs, so weekend tiers and overnight cadence target that schedule.
  For US-only or crypto-only watch sessions, override
  ``-Schedule us`` / ``-Schedule crypto``.
* **Calibration is a single anchor**. If SKHYNIX is missing from the
  observer's run (e.g. you only watch a subset of markets), every
  market's "calibrated_share" is just the raw share — clearly labeled
  in the report.

## Troubleshooting

* **Observer launcher fails with "No real Python interpreter found"**.
  The script refuses the WindowsApps stub. Install Python 3.11/3.12/
  3.13 from the official installer (user-scope is fine), then either
  rerun ``scripts\setup_env_permanent.ps1`` or open a fresh shell.

* **Observer prints "Cannot connect to host mainnet.zklighter.elliot.ai"**.
  Transient — your VPN/proxy probably dropped. Confirm:
  ```powershell
  curl https://mainnet.zklighter.elliot.ai/api/v1/orderBooks
  ```
  If that succeeds, restart the observer.

* **Analyzer reports "no LPP cluster detected"** for a market.
  Either the market truly has no persistent two-sided MM at any one
  size (common in T3 small-cap), or the observation window was too
  short (< 30 minutes typically not enough for LPP_PERSISTENCE_PCT to
  hit 50%). Check ``snapshots`` in the analysis JSON.

* **Analyzer reports negative or absurd ``calibrated_share``**.
  Bug in either the multiplier (check ``analysis.json` ``calibration``
  block) or your ``--your-size-usdc`` is wildly different from the
  baseline 50. Override the calibration anchor in the source and
  re-run.
