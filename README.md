# Yahoo Fantasy NHL Toolkit


This project is a **personal Python utility** for interacting with the Yahoo Fantasy Hockey API.  
It provides modular CLI scripts to export league, team, and scoring data into clean JSON and polished Excel workbooks.

The toolkit is built for long-term maintainability with small, well-commented modules and clear scoping.  
It emphasizes **presentation-ready Excel output**, **automatic OAuth token refresh**, and **minimal API traffic** through caching and batching.


## ✨ Features
- **League Dump**: Export metadata, teams, and scoring settings.
- **Polished Excel**: Hyperlinks, embedded team logos, frozen headers, auto-filters, widths.
- **JSON Output**: Tidy, structured snapshots for programmatic use.
- **Historical Support**: Works with full league keys (e.g., `453.l.33099`).
- **OAuth Refresh**: Unified token handling in `src/auth/oauth.py` (manual or HTTPS callback via mkcert).
- **Future Modules**: Planned `standings_dump.py`, `transactions_dump.py`, `draft_dump.py`, etc.
- **Timestamp Triplets**: All exports include `_unix`, `_excel_serial`, `_iso_utc`.


## 📂 Project Structure
```
src/
  auth/oauth.py         # OAuth2 (auto-refresh, manual or HTTPS callback)
  config/env.py         # Environment + export dir helpers
  io/                   # Excel + JSON writers
  yahoo_*               # Yahoo API helpers
  nhl_*, df_*           # (future) NHL stats + dataframe helpers

scripts/
  league_dump.py        # Export league metadata, teams, scoring
  standings_dump.py     # (planned) Export league standings
  transactions_dump.py  # (planned) Export recent transactions
  draft_dump.py         # (planned) Export draft results
```


## ⚙️ Installation
```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt
```
### requirements.txt (minimums)
```
python-dotenv>=1.0.1
requests>=2.32.3
requests-oauthlib>1.3.0
pandas>=2.0
openpyxl>=3.1
Pillow>=10.0.0
```


## 🔑 OAuth Setup (Localhost HTTPS with mkcert)
1. **Create Yahoo App** at https://developer.yahoo.com/apps/
   - Redirect URIs (must match exactly):
     - `https://127.0.0.1:8910/callback`
     - `https://localhost:8910/callback`
   - Enable scopes: `openid fspt-r`
   - Copy your **Client ID** and **Client Secret**.

2. **Generate local HTTPS certs** with mkcert:
```bash
mkcert -install
mkdir -p certs
mkcert -key-file certs/localhost-key.pem -cert-file certs/localhost.pem 127.0.0.1 localhost
```

3. **.env file** (example):
```env
YAHOO_CLIENT_ID=your_client_id_here
YAHOO_CLIENT_SECRET=your_client_secret_here
YAHOO_REDIRECT_URI=https://127.0.0.1:8910/callback
YAHOO_SCOPE=openid fspt-r

CACHE_DIR=./data
TOKEN_FILE=./data/yahoo_token.json

OAUTH_MANUAL=0
TLS_CERT_FILE=./certs/localhost.pem
TLS_KEY_FILE=./certs/localhost-key.pem
OAUTH_DEBUG=0
```

4. **Validate**:
```bash
python scripts/env_check.py
python -m src.auth.oauth
```


## 🚀 Usage

### Raw Fetch
```bash
python -m scripts.raw_fetch --league-key 453.l.33099 --path standings
python -m scripts.raw_fetch --league-key 453.l.33099 --path "scoreboard;week=5"
python -m scripts.raw_fetch --league-key 453.l.33099 --path "transactions;type=trade"
```
Low-level helper for grabbing unparsed Yahoo Fantasy API JSON for a given league + endpoint.
Outputs are written to exports/_debug/ as one file per call, e.g.:

- `exports/_debug/standings.json`
- `exports/_debug/scoreboard;week=5.json`
- `exports/_debug/transactions;type=trade.json`

Useful for inspecting raw payloads while developing new extractors (standings, transactions, players, etc.).
The _debug files are not part of the stable export layout and can be safely deleted at any time.

### League Dump
```bash
python -m scripts.league_dump --league-key 453.l.33099 --pretty --to-excel
```
Produces a league-scoped export tree under exports/<league_key>/, for example:

- `exports/nhl.453.l.33099/_meta/league_profile.json` – canonical team directory (team_key → {name, logo_url, team_url}).
- `exports/nhl.453.l.33099/_meta/latest.json` – pointers to the latest league_dump artifacts.
- `exports/nhl.453.l.33099/league_dump/raw/settings.<ISO>.json` – raw Yahoo API responses (metadata, teams, settings).
- `exports/nhl.453.l.33099/league_dump/processed/league.<ISO>.json` – normalized league snapshot with _generated_* timestamps.
- `exports/nhl.453.l.33099/league_dump/excel/league.<ISO>.xlsx` – polished workbook (League, Teams, Scoring, Run Info).
- `exports/nhl.453.l.33099/league_dump/manifest/manifest.<ISO>.json` – manifest listing all files, sizes, hashes, and CLI args.

<ISO> is a UTC timestamp like 20250912T143012Z; all files from a single run share the same <ISO> suffix.

### Standings Dump
```bash
python -m scripts.standings_dump --league-key 453.l.33099 --pretty --to-excel
```
Requires a prior league_dump run for the same league (reads _meta/latest.json and the latest league.*.json as context).
Produces league-scoped outputs under exports/<league_key>/standings_dump/ including:

- `raw/scoreboard.wkNNN.<ISO>.json` – raw weekly scoreboard snapshots.
- `processed/matchups.<ISO>.json` – week-by-week matchup ledger.
- `processed/weekly.<ISO>.json` – per-team, per-week category totals + W/L/T, with playoff flags.
- `processed/summary.<ISO>.json` – regular-season + playoff summaries (totals, averages, ranks).
- `excel/standings.wkSS-EE.<ISO>.xlsx` – Matchups, WeeklyTotals, RegularSummary, PlayoffSummary, RunInfo sheets.

### Transactions Dump
```bash
python -m scripts.transactions_dump --league-key 453.l.33099 --pretty --to-excel
```
Also requires a prior league_dump run (uses _meta/latest.json + latest processed league JSON).
Produces league-scoped outputs under exports/<league_key>/transactions_dump/ including:

- `raw/transactions.<ISO>.json` – full-season Yahoo transactions payload.
- `processed/master.<ISO>.json` – normalized move ledger with timestamps, weeks, types, and per-player moves.
- `excel/transactions.<ISO>.xlsx` – AllMoves, Adds, Drops, Trades sheets with team + player context.
- `manifest/manifest.<ISO>.json` – file list, sizes, hashes, and CLI arguments for this run.

### Draft Dump
```bash
python -m scripts.transactions_dump --league-key 453.l.33099 --pretty --to-excel
```
Also requires a prior league_dump run (uses _meta/latest.json + latest processed league JSON).
Fetches the draft results for the specified league under exports/<league_key>/draft_dump/ including:

- `raw/draftresults.<ISO>.json`
- `processed/draft.<ISO>.json`
- `excel/draft.<ISO>.xlsx`
- `manifest/manifest.<ISO>.json`

### Rostered Players List Script
```bash
python -m scripts.rostered_players_list --league-key 453.l.33099 --pretty --to-excel
```

### Player Stat Data Dump
```bash
python -m scripts.players_dump --league-key 453.l.33099 --season 2024 --pretty
```
This script depends on prior runs of `league_dump` and `rostered_players_list`
for the same league. Then, for the league’s rostered player universe, it fetches season-level player
stats from Yahoo’s `league/<league_key>/players;out=stats` endpoint, with local
per-player caching to avoid repeated API calls.

Outputs under:

- `raw/players.stats.season<YYYY>.<ISO>.json`
- `processed/player_stats.season<YYYY>.<ISO>.json`
- `manifest/manifest.season<YYYY>.<ISO>.json`
- `manifest/cache/season-<YYYY>/<player_key>.json`    (per-player cache, internal use)

<league_key> is the full Yahoo league key (e.g. `465.l.22607`)
<YYYY> is the fantasy season (e.g. `2025`)
<ISO> is a run identifier like `20251129T014755Z` (UTC timestamp)

The processed JSON contains one record per rostered player with identity fields
(player_key, editorial_player_key, name, NHL team, positions), the target
`season`, and flat maps of `stat_id → value` for both standard and advanced
stats.


### Token Refresh
- Automatic via `get_session()` in `src/auth/oauth.py`
- Manual repair:
```bash
python -m src.auth.oauth
```


## 🧩 Constraints & Rules
- Language: Python 3.10+
- Code style: small OOP modules, verbose comments, ALL_CAPS only for constants
- Naming: `yahoo_*`, `nhl_*`, `df_*`
- No secrets in git: `.env` and token files are ignored
- Minimize API traffic: caching, batching, no brute force
- Exports: always include `_unix`, `_excel_serial`, `_iso_utc`


## 📝 Changelog
- League dump finalized (metadata, teams, scoring).
- Unified OAuth refresh in `src/auth/oauth.py`.
- Deprecated legacy scripts (`scripts/standalone_oauth.py`, `scripts/standalone_fantasy.py`).
- README reorganized with OAuth + League Dump instructions.
- Scaffolding prepared for standings, transactions, and draft modules.
