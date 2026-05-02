#requires -Version 5.1
<#
.SYNOPSIS
    Verify every key in <repo>\.env is set in Windows User-scope env.

.DESCRIPTION
    Mirror of setup_env_permanent.ps1: re-reads the .env to enumerate
    expected keys, then asks the User-scope env store for each. Reports
    presence + length only; never prints the value.

    Run from a FRESH PowerShell window after setup (the running session
    that invoked setup still holds its old process-level env block).

.OUTPUTS
    Exits 0 if every key is present and non-empty in User scope.
    Exits 1 otherwise.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$envPath = Join-Path $PSScriptRoot '..\.env'
if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Error ".env not found at $envPath"
    exit 1
}

$total   = 0
$missing = 0

foreach ($raw in (Get-Content -LiteralPath $envPath -Encoding UTF8)) {
    $line = $raw.Trim()
    if (-not $line) { continue }
    if ($line.StartsWith('#')) { continue }

    $idx = $line.IndexOf('=')
    if ($idx -lt 1) { continue }

    $key = $line.Substring(0, $idx).Trim()
    if (-not $key) { continue }

    $total++
    $val = [Environment]::GetEnvironmentVariable($key, 'User')
    if ($val -and $val.Length -gt 0) {
        Write-Host ("OK  {0} (User scope, length={1})" -f $key, $val.Length) -ForegroundColor Green
    } else {
        Write-Host ("--  {0} NOT SET in User scope" -f $key) -ForegroundColor Red
        $missing++
    }
}

Write-Host ''
if ($missing -eq 0 -and $total -gt 0) {
    Write-Host ("All {0} env vars present. New PowerShell windows will auto-load them." -f $total) -ForegroundColor Cyan
    exit 0
} elseif ($total -eq 0) {
    Write-Host 'No KEY=VAL lines found in .env.' -ForegroundColor Yellow
    exit 1
} else {
    Write-Host ("{0}/{1} env vars missing. Re-run setup_env_permanent.ps1." -f $missing, $total) -ForegroundColor Yellow
    exit 1
}
