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
Used for executing code and lookup queries directly on the API. Files save to exports/_debug
```bash
python -m scripts.raw_fetch --league-key 453.l.33099 --path settings
python -m scripts.raw_fetch --league-key 453.l.33099 --path "scoreboard;week=8"
```

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
python -m scripts.standings_dump --league-key 453.l.33099 --to-excel
```
Produces a league-scoped export tree under exports/<league_key>/

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
