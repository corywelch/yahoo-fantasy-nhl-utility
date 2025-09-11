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
