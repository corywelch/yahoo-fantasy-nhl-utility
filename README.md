# Yahoo Fantasy NHL Utility — OAuth Setup (Localhost HTTPS, mkcert)

This repository is configured to use **localhost HTTPS** for the OAuth redirect with “Sign in with Yahoo” + Fantasy Sports read access.

## 1) Create a Yahoo Developer App
1. https://developer.yahoo.com/apps/
2. Redirect URIs — add **both** of these (exact match required):
   - `https://127.0.0.1:8910/callback`
   - `https://localhost:8910/callback`
3. Enable scopes you need. For sign-in + Fantasy read, use: `openid email profile fspt-r`.
4. Note your **Client ID** and **Client Secret**.

## 2) Generate Local HTTPS Certs (mkcert)
```bash
# One-time: install mkcert & local root CA
# Windows (choco as admin): choco install mkcert
# macOS: brew install mkcert nss
# Linux: see mkcert README

mkcert -install
mkdir -p certs
mkcert -key-file certs/localhost-key.pem -cert-file certs/localhost.pem 127.0.0.1 localhost
```

## 3) Create `.env`
Copy this to `.env` in the project root and fill your Client ID/Secret:
```env
# Yahoo OAuth/OIDC
YAHOO_CLIENT_ID=your_client_id_here
YAHOO_CLIENT_SECRET=your_client_secret_here
YAHOO_REDIRECT_URI=https://127.0.0.1:8910/callback

# Scopes: Sign-in + Fantasy Sports read
YAHOO_SCOPE=openid fspt-r

# Project paths
CACHE_DIR=./data
TOKEN_FILE=./data/yahoo_token.json

# Logging / runtime
LOG_LEVEL=INFO
TZ=America/Toronto
HTTP_TIMEOUT=30

# Automatic local HTTPS callback (mkcert outputs)
OAUTH_MANUAL=0
TLS_CERT_FILE=./certs/localhost.pem
TLS_KEY_FILE=./certs/localhost-key.pem
```

## 4) Install & Validate
```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt

python scripts/env_check.py
```

## 5) Run OAuth Flow (Automatic)
```bash
python -m src.oauth
```
- The script prints an **Authorize URL** (also tries to open your browser).
- Sign in and approve. Yahoo redirects back to `https://127.0.0.1:8910/callback`.
- The script captures the code, exchanges tokens, and writes `./data/yahoo_token.json`.

Re-run to verify/refresh:
```bash
python -m src.oauth
# -> Token OK (expires: ...)
```

## Troubleshooting
- **Browser shows certificate warning**: ensure you ran `mkcert -install` and used the generated `certs/*.pem` paths.
- **“Something went wrong” on Yahoo**: Redirect URI mismatch. Make sure **Yahoo app** and **.env** match character-for-character.
- **Wrong Yahoo account**: open the authorize URL in a private/incognito window.
- **App restricted**: add your Yahoo account as a **tester** in the Yahoo app console.


# Yahoo Fantasy NHL Utility — League Dump Module

## Overview
This module provides a reliable way to export **Yahoo Fantasy Hockey league data** for any season using the full league key. It focuses on what you’ll reference across other tools: **league metadata**, **teams**, and **scoring settings**. The output is saved as clean JSON (for programmatic use) and an optional **polished Excel workbook** (for quick analysis and lookups).

The script reads your existing OAuth token (via `src/auth/oauth.get_session()`), calls Yahoo’s Fantasy Sports API, normalizes Yahoo’s nested response shape, and writes outputs under an export directory (`EXPORT_DIR`, default `./exports`).

## Key Features
- Pulls **league metadata** (name, season, dates, scoring type, etc.).
- Extracts **teams** (ids, names, URLs, logos, waiver priority, manager).  
- Extracts **scoring settings**:
  - Stat categories & modifiers
  - Roster positions (with counts and types)
  - Tie-breakers (best-effort)
  - Head-to-head configuration (draft type, waivers, playoffs, etc.)
  - Goalie minimums (if provided)
- Writes tidy JSON files:
  - `league_info.json`, `league_teams.json`, `league_scoring.json`
- Optional **Excel export** (`league_info.xlsx`) with:
  - League sheet (key/value with a clickable URL)
  - Teams sheet (name hyperlinks, **embedded logos**, frozen header, filters, widths)
  - Scoring tabs (ScoringCategories, StatModifiers, RosterPositions, HeadToHeadSettings, GoalieMinimums)
- Supports **historical leagues** via full league key (e.g., `453.l.33099`).

## Project Structure (relevant to this module)
```
src/
  auth/oauth.py         # returns an authenticated requests.Session (Bearer token)
  config/env.py         # resolves EXPORT_DIR (default ./exports)

scripts/
  league_dump.py        # CLI: dumps metadata, teams, scoring to JSON (and Excel)
```
> Other scripts (e.g., `standings_dump.py`, `transactions_dump.py`, `draft_dump.py`) will be separate modules with their own scopes.

## Setup & Installation
1. **Python deps**  
   - `requests`
   - `openpyxl`
2. **Environment**  
   Ensure you have a working OAuth token flow. The module uses `src/auth/oauth.get_session()` which should read your token (e.g., `./data/yahoo_token.json`) and refresh when needed.
3. **Repo path**  
   Run from the repo root with `PYTHONPATH=.` so `src/` imports resolve.
4. **Exports directory**  
   Set `EXPORT_DIR` in your environment or `.env`. If not set, `./exports` is used.

## Usage Examples
Fetch for a specific league using the **full league key** (recommended for historical seasons):
```bash
python -m scripts.league_dump --league-key 453.l.33099 --pretty --to-excel
```
Or derive the league key from game + id (defaults `--game nhl`):
```bash
python -m scripts.league_dump --league-id 33099 --pretty
```

### Outputs
- `exports/league_info.json`  
- `exports/league_teams.json`  
- `exports/league_scoring.json`  
- `exports/league_info.xlsx` (if `--to-excel` is used)

## Configuration
- **OAuth**: `src/auth/oauth.get_session()` should be wired to your existing token file and refresh logic. No import of your old `fantasy.py` is required.  
- **Env vars**:
  - `EXPORT_DIR` — output directory (default `./exports`).
  - Any OAuth-related env vars your `get_session()` requires (e.g., `YAHOO_CLIENT_ID`, `YAHOO_CLIENT_SECRET`, `YAHOO_REDIRECT_URI`).

## Known Limitations / TODOs
- **Standings, Transactions, Draft** are intentionally **out of scope** for this script. Create dedicated modules:
  - `standings_dump.py`
  - `transactions_dump.py`
  - `draft_dump.py`
- Yahoo response formats vary slightly across seasons/leagues; the extractors are permissive but may need minor tweaks if new shapes appear (send a raw JSON dump and adjust the mapper accordingly).
- Team manager details vary across leagues (hidden emails, multiple managers). We capture the **first** manager entry when present.

## Changelog Summary
- **League dump finalized**: consolidated metadata, teams, and scoring settings into one script.
- **Removed fantasy.py dependency**: now uses `src/` only (`get_session`, `get_export_dir`).
- **JSON + polished Excel**: hyperlinks, embedded logos, frozen headers, auto-filters, sensible widths.
- **Historical support**: `--league-key` accepts full keys so you can dump past seasons reliably.
