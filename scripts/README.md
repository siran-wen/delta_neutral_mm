# scripts/

Operational scripts. Strategy runners (`run_lighter_*.py`), observation
tools (`observe_lighter.py`), one-shot diagnostics, and platform glue.

## .env → Windows User-scope env vars

The Lighter strategy runner reads `API_KEY_PRIVATE_KEY`,
`LIGHTER_ACCOUNT_INDEX`, etc. from process env. The repo ships a `.env`
in the project root, but Python doesn't auto-load it (no `dotenv` import
on the hot path). Three options:

1. Inline shim (one-shot, every new shell): copy a Get-Content `.env` |
   ForEach-Object loop into the prompt before launching.
2. Per-launch wrapper (per command): prefix env vars in the launch line.
3. **Persist once, into Windows User scope** — preferred for
   single-user machines. Three scripts in this directory automate it:

| Script | Purpose |
| --- | --- |
| `setup_env_permanent.ps1` | Read `.env`, write every key to HKCU\Environment (User scope). One-shot. |
| `verify_env_permanent.ps1` | Re-read `.env` keys, confirm each is present + non-empty in User scope. Exit 1 on any missing. |
| `unset_env_permanent.ps1` | Reverse of setup — clears every `.env` key from User scope. |

### Recommended sequence

```powershell
# 1. Run from any PowerShell window in the repo root.
scripts\setup_env_permanent.ps1

# 2. Close ALL PowerShell / cmd / VS Code terminals.
#    Windows only refreshes the process env block at process start;
#    sessions that were already open keep their old block.

# 3. Open a fresh PowerShell window. Verify:
scripts\verify_env_permanent.ps1
```

After step 3 reports every key present, the runner can be invoked
directly without any env shim:

```powershell
python scripts\run_lighter_strategy.py --market SKHYNIXUSD --duration 1 --paper -v
```

### Security caveat

Values are written to `HKCU\Environment` in plaintext. Anything else
running under the same Windows user can read them via
`[Environment]::GetEnvironmentVariable(name, 'User')` or by reading the
registry key directly. This is acceptable for a single-user machine
where the alternative is the same plaintext sitting in a `.env` file
that any local process can also read.

If the machine is ever shared, retired, or the credentials are rotated,
run:

```powershell
scripts\unset_env_permanent.ps1
```

This iterates the `.env` keys (so it stays in sync with whatever the
file currently declares) and deletes each from User scope. The `.env`
file itself is not touched.

### Output format — what gets logged

All three scripts log only the **key name** and the **length** of the
value. The value itself is never printed. Example:

```
+ API_KEY_PRIVATE_KEY (length=82)
+ LIGHTER_ACCOUNT_INDEX (length=4)
+ LIGHTER_API_KEY_INDEX (length=2)
```

A `*` prefix in setup output indicates an existing User-scope value
was overwritten (vs. `+` for a fresh key).
