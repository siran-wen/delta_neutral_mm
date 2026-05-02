#requires -Version 5.1
<#
.SYNOPSIS
    Launch ``observe_lighter.py`` against the 22 LPP-pool markets.

.DESCRIPTION
    Starts a single observer process that watches every Phase-3 LPP
    pool market in parallel, on the KR_EQUITY schedule. Sized for a
    24h run on an observation-only PC (no API key, no orders sent).

    REST budget math (the reason BboInterval defaults to 90s):
      22 markets × 60s/min ÷ <BboInterval>s × 5min  =  REST calls / 5min
      e.g. 22 × 60 ÷ 90 × 5  =  73 calls / 5min  (under the ~100 WAF cap)
      e.g. 22 × 60 ÷ 60 × 5  =  110 calls / 5min (above; will trip)

.PARAMETER DurationMin
    How long to observe, in minutes. Default 1440 (24h).

.PARAMETER BboInterval
    Per-market REST poll cadence in seconds. 90 keeps 22 markets under
    the WAF; reduce only if you have a reason to and have measured.

.PARAMETER OutputDir
    Where to drop the JSONL output. Created if missing.

.PARAMETER Schedule
    ``kr`` / ``crypto`` / ``us``. Default kr — the LPP pool weights
    pivot on KR market hours.

.NOTES
    Observation only — no signer, no orders, no .env required.
    Run on a separate PC from the strategy box so neither competes
    for the WAF budget on the same outbound IP.
#>

[CmdletBinding()]
param(
    [int]$DurationMin = 1440,
    [int]$BboInterval = 90,
    [string]$OutputDir = "logs\lpp_observer_daily",
    [ValidateSet("kr","crypto","us")][string]$Schedule = "kr",
    [int]$TopLevels = 5,
    [int]$SnapshotInterval = 60,
    [int]$SnapshotBookDepth = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 22 LPP pool markets — keep aligned with scripts/analyze_observer.py
# POOL_BY_MARKET. Tier comment is for the operator; the observer
# itself doesn't read this list.
$markets = @(
    # T6 KR RWAs
    'SKHYNIXUSD','SAMSUNGUSD','HYUNDAIUSD',
    # T6 commodities
    'XAUUSD','XAGUSD','BRENTOIL','WTI',
    # T5 US benchmark
    'SPYUSD',
    # T4 mid-cap mix
    'MON','ZEC','ZRO','CRCL','AAVE','TSLA','XPT',
    # T3 small-cap mix
    'NVDA','COIN','HOOD','META','MSFT','PLTR','NATGAS'
)

if ($markets.Count -ne 22) {
    Write-Error "Expected 22 markets in the LPP pool list, got $($markets.Count)"
    exit 1
}

# Real Python autodetect — WindowsApps stub will silently 'fail' and
# the observer never starts, so refuse it explicitly.
function Find-Python {
    $candidates = @(
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe"
    )
    foreach ($p in $candidates) {
        if (Test-Path -LiteralPath $p) { return $p }
    }
    # Fallback: first ``python`` on PATH that is NOT the WindowsApps stub.
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and ($cmd.Source -notlike '*WindowsApps*')) {
        return $cmd.Source
    }
    Write-Error @"
No real Python interpreter found.
Looked in:
  $env:LOCALAPPDATA\Programs\Python\Python312\python.exe (and 3.11 / 3.13)
  PATH (excluding the WindowsApps Store stub)
Run scripts\setup_env_permanent.ps1 → reopen this shell, then re-run this script.
"@
    exit 1
}

$python = Find-Python

# Resolve repo root from script location so the user can launch from
# anywhere.
$repoRoot = Split-Path -Parent $PSScriptRoot
$observePy = Join-Path $repoRoot 'scripts\observe_lighter.py'
if (-not (Test-Path -LiteralPath $observePy)) {
    Write-Error "scripts\observe_lighter.py not found under $repoRoot"
    exit 1
}

$outputAbs = if ([System.IO.Path]::IsPathRooted($OutputDir)) {
    $OutputDir
} else {
    Join-Path $repoRoot $OutputDir
}
if (-not (Test-Path -LiteralPath $outputAbs)) {
    New-Item -ItemType Directory -Path $outputAbs -Force | Out-Null
}

# REST budget projection — print before launch so the operator can
# bail out if BboInterval is set too low for 22 markets.
$callsPer5Min = [math]::Round($markets.Count * 60.0 / $BboInterval * 5.0, 1)
$wafCap = 100
Write-Host ''
Write-Host '== LPP observer launcher ==' -ForegroundColor Cyan
Write-Host "  python      : $python"
Write-Host "  observer    : $observePy"
Write-Host "  output dir  : $outputAbs"
Write-Host "  duration    : $DurationMin min ($([math]::Round($DurationMin/60.0, 1)) h)"
Write-Host "  schedule    : $Schedule"
Write-Host "  bbo interval: $BboInterval s"
Write-Host "  snapshot    : $SnapshotInterval s, top_levels=$TopLevels, depth=$SnapshotBookDepth"
Write-Host "  markets     : $($markets.Count)  ($($markets -join ', '))"
Write-Host ''
Write-Host ("REST budget : ~{0} calls / 5 min  (WAF cap ~{1})" -f $callsPer5Min, $wafCap) -ForegroundColor $(
    if ($callsPer5Min -ge $wafCap) { 'Red' } elseif ($callsPer5Min -ge ($wafCap * 0.8)) { 'Yellow' } else { 'Green' }
)
if ($callsPer5Min -ge $wafCap) {
    Write-Warning "Projected REST > WAF cap. Increase BboInterval before running."
}
Write-Host ''

# Build the command line. ``--market`` is repeatable in observe_lighter.py.
$marketArgs = @()
foreach ($m in $markets) {
    $marketArgs += @('--market', $m)
}
$cmdArgs = @(
    $observePy,
    '--no-account',
    '--schedule', $Schedule,
    '--top-levels', $TopLevels,
    '--snapshot-book-depth', $SnapshotBookDepth,
    '--snapshot-interval', $SnapshotInterval,
    '--bbo-interval', $BboInterval,
    '--duration', $DurationMin,
    '--output-dir', $outputAbs,
    '-v'
) + $marketArgs

Write-Host "launching: $python $($cmdArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ''
& $python @cmdArgs
exit $LASTEXITCODE
