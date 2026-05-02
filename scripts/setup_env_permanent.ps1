#requires -Version 5.1
<#
.SYNOPSIS
    Persist .env into Windows User-scope environment variables.

.DESCRIPTION
    Reads <repo>\.env, parses each KEY=VALUE line (skipping blanks and
    comments), and writes every entry to the Windows User-scope
    environment store (HKCU\Environment). After running once, every new
    PowerShell / cmd window inherits the values automatically — no more
    per-session Get-Content .env shim.

    Existing console sessions are NOT updated (Windows only refreshes the
    process env block at process start). Close every open shell and open
    a fresh one, then run scripts\verify_env_permanent.ps1 to confirm.

.NOTES
    Values are written to HKCU\Environment in plaintext. Acceptable on a
    single-user machine where you also accept the risk of any local
    process being able to read them. To revert, run
    scripts\unset_env_permanent.ps1.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$envPath = Join-Path $PSScriptRoot '..\.env'
if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Error ".env not found at $envPath"
    exit 1
}

$count    = 0
$skipped  = 0
$malformed = 0

foreach ($raw in (Get-Content -LiteralPath $envPath -Encoding UTF8)) {
    $line = $raw.Trim()
    if (-not $line) { continue }
    if ($line.StartsWith('#')) { continue }

    $idx = $line.IndexOf('=')
    if ($idx -lt 1) {
        # No '=' or starts with '=' — neither is a valid KEY=VAL pair.
        $malformed++
        continue
    }

    $key = $line.Substring(0, $idx).Trim()
    $val = $line.Substring($idx + 1).Trim()
    # Strip a single layer of surrounding quotes if present (the same
    # convention python-dotenv uses). A value that is itself just '""'
    # collapses to empty, which is intentional.
    if ($val.Length -ge 2) {
        $first = $val[0]
        $last  = $val[$val.Length - 1]
        if (($first -eq '"' -and $last -eq '"') -or ($first -eq "'" -and $last -eq "'")) {
            $val = $val.Substring(1, $val.Length - 2)
        }
    }

    if (-not $key) {
        $malformed++
        continue
    }

    $existing = [Environment]::GetEnvironmentVariable($key, 'User')
    [Environment]::SetEnvironmentVariable($key, $val, 'User')
    $count++

    $tag = if ($existing) { '*' } else { '+' }
    Write-Host ("{0} {1} (length={2})" -f $tag, $key, $val.Length) -ForegroundColor Green
    if ($skipped) { } # silence StrictMode unused-var nag in older PS
}

Write-Host ''
Write-Host ("Done. {0} variables written to User scope ({1} malformed lines skipped)." -f $count, $malformed) -ForegroundColor Cyan
Write-Host ''
Write-Host 'NEXT STEPS:'
Write-Host '  1. Close ALL PowerShell / cmd / VS Code terminals.'
Write-Host '  2. Open a fresh PowerShell window.'
Write-Host '  3. Run: scripts\verify_env_permanent.ps1'
Write-Host ''
Write-Host 'To undo: scripts\unset_env_permanent.ps1'
