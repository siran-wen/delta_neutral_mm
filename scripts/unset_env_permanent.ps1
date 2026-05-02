#requires -Version 5.1
<#
.SYNOPSIS
    Remove every .env key from Windows User-scope environment variables.

.DESCRIPTION
    Reverses setup_env_permanent.ps1. Reads <repo>\.env to enumerate the
    keys to clear, then for each key calls
    [Environment]::SetEnvironmentVariable($key, $null, 'User') which
    deletes the entry from HKCU\Environment.

    Use this when retiring the machine, switching accounts, or rotating
    credentials.

.NOTES
    Like setup, the change only takes effect for processes started AFTER
    this script runs. Existing shells keep their stale env block until
    closed.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$envPath = Join-Path $PSScriptRoot '..\.env'
if (-not (Test-Path -LiteralPath $envPath)) {
    Write-Error ".env not found at $envPath"
    exit 1
}

$cleared      = 0
$alreadyEmpty = 0

foreach ($raw in (Get-Content -LiteralPath $envPath -Encoding UTF8)) {
    $line = $raw.Trim()
    if (-not $line) { continue }
    if ($line.StartsWith('#')) { continue }

    $idx = $line.IndexOf('=')
    if ($idx -lt 1) { continue }

    $key = $line.Substring(0, $idx).Trim()
    if (-not $key) { continue }

    $existing = [Environment]::GetEnvironmentVariable($key, 'User')
    if ($existing) {
        [Environment]::SetEnvironmentVariable($key, $null, 'User')
        Write-Host ("x   {0} cleared" -f $key) -ForegroundColor Green
        $cleared++
    } else {
        Write-Host ("--  {0} was not set" -f $key) -ForegroundColor DarkGray
        $alreadyEmpty++
    }
}

Write-Host ''
Write-Host ("Done. Cleared {0} keys ({1} were already empty)." -f $cleared, $alreadyEmpty) -ForegroundColor Cyan
Write-Host 'Open a new PowerShell window to confirm — current shells still hold stale env.'
