# AI Development Context & Rules (Yahoo Fantasy API Toolkit)

This repository is a **script-driven Python toolkit** for interacting with the **Yahoo Fantasy Hockey API** and exporting results to:
- **clean JSON snapshots**
- **presentation-ready Excel workbooks** (hyperlinks, images/logos, formatting)

This file is the **single source of truth** for AI coding assistants (GitHub Copilot, Cline, Devstral, GPT‑5.2).  
If you are an AI agent working in this repo: **follow the rules below exactly**.

---

## 1) What this project is (and isn’t)

### Primary goals
- Provide small, composable CLI scripts under `scripts/` that fetch and export league data.
- Keep outputs stable and reproducible: consistent filenames, manifests, and timestamps.
- Minimize API traffic using caching and batching.

### Non-goals
- No secrets in git (no tokens, no client secrets, no copied raw OAuth payloads).
- No big architectural rewrites unless explicitly requested.
- No “clever” one-off session/auth flows; use the existing OAuth module.

---

## 2) Golden rules (do not violate)

### 2.1 OAuth + session handling
- **Always** use `get_session()` from `src/auth/oauth.py` for authenticated Yahoo requests.
- Do **not** hardcode tokens, add alternate auth flows, or bypass refresh logic.
- Do **not** change redirect URIs / mkcert instructions unless explicitly instructed.

### 2.2 Yahoo response parsing behavior
- Preserve the existing behavior in `src/yahoo/client.py`:
  - Prefer JSON
  - Fall back to XML (`xmltodict`) when needed
- If you touch parsing, keep the fallback logic and error messages intact.

### 2.3 Export contract (stable outputs)
- Preserve the **timestamp triplet fields** used across exports:
  - `_unix`
  - `_excel_serial`
  - `_iso_utc`
- Preserve the export layout and “latest pointers” pattern:
  - `_meta/latest.json` is the source of truth for “latest” artifacts per module.
- Preserve manifest behavior (hashes, sizes, CLI args, run metadata).

### 2.4 API traffic discipline
- Avoid brute force loops over players/teams without caching.
- Prefer batch endpoints when available.
- If you must loop:
  - cache per-entity payloads
  - keep requests resumable
  - make it observable in the manifest and logs

---

## 3) Repository map (how to navigate)

### 3.1 CLI entrypoints
- `scripts/*.py` are user-facing CLIs (e.g., `league_dump.py`, `raw_fetch.py`, `players_dump.py`).
- New workflows should generally be implemented as new scripts here.

### 3.2 Reusable modules (core)
- `src/auth/oauth.py` — OAuth2 + token refresh; provides `get_session()`.
- `src/yahoo/client.py` — HTTP client + payload parsing (JSON + XML fallback).
- `src/yahoo/api.py` — thin wrapper API surface (e.g., `YahooLeagueAPI`).
- Writers/helpers live under `src/io/` and/or `src/export/` (follow what exists in the tree).

---

## 4) Export layout (expected filesystem contract)

### 4.1 Debug raw captures (development only)
- `exports/_debug/` is for raw inspection payloads and is **not** part of the stable export contract.
- Use `scripts/raw_fetch` first when building new extractors.

### 4.2 League-scoped exports (stable contract)
All stable exports are league-scoped:

```
exports/<league_key>/
  _meta/
    league_profile.json
    latest.json
  <module_name>/
    raw/
    processed/
    excel/
    manifest/
```

#### Notes
- `<ISO>` is a UTC run identifier like `20251129T014755Z`.
- A single run should share the same `<ISO>` suffix across the raw/processed/excel/manifest files.
- Each module updates `_meta/latest.json` to point at the newest processed artifacts.

---

## 5) How to add a new “dump” module (the approved playbook)

When implementing a new export flow (e.g., `standings_dump`, `transactions_dump`, `draft_dump`, `players_dump` improvements):

1. **Capture raw payload(s)** using `scripts/raw_fetch` and save under `exports/_debug/`.
2. Inspect payload structure; write extraction helpers:
   - `extract_*` for extracting fields
   - `normalize_*` for producing stable processed objects
3. Implement the CLI under `scripts/<module>_dump.py`:
   - read `--league-key`
   - load context from `_meta/latest.json` when needed
   - write outputs to league-scoped layout (`raw/`, `processed/`, `excel/`, `manifest/`)
4. Ensure processed JSON includes timestamp triplets.
5. Generate a manifest listing:
   - file list + sizes + hashes
   - CLI args
   - timestamps / run id
6. Update `_meta/latest.json` for this module.
7. Verify with a real league key:
   - run the script twice
   - confirm caching prevents duplicate traffic
   - confirm outputs are stable and readable

---

## 6) Code style & standards (must match existing project)

### 6.1 Naming
- `snake_case` for variables/functions; `PascalCase` for classes; `UPPER_CASE` for constants only.
- Use suffix conventions:
  - keys: `*_key` (e.g., `league_key`, `team_key`, `player_key`)
  - ids: `*_id`
- Prefer explicit, descriptive names; avoid nonstandard abbreviations.

### 6.2 Type annotations
- Always annotate parameters and return values.
- Use module-level type aliases for complex nested JSON structures.

### 6.3 Imports
- Group imports in this order:
  1) future imports  
  2) stdlib  
  3) third-party  
  4) local  
- One import per line, with blank lines between groups.

### 6.4 Docstrings & comments
- Public functions must have docstrings (Google style).
- Comment **why** (not what) for any non-obvious logic.
- Use section headers inside larger functions to keep them readable.

### 6.5 Error handling
- Use specific exceptions; include context in messages.
- Validate inputs early (“fail fast”) with clear errors.
- Prefer graceful degradation in CLIs when feasible (empty outputs + warnings) unless `--fail-on-error` exists.

---

## 7) Local dev workflow (reference)

### Setup
```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### OAuth validation
```bash
python scripts/env_check.py
python -m src.auth.oauth
```

### Typical runs
```bash
python -m scripts.raw_fetch --league-key 453.l.33099 --path standings
python -m scripts.league_dump --league-key 453.l.33099 --pretty --to-excel
```

---

## 8) AI agent operating rules (how to behave during changes)

When you (the AI assistant) are asked to make changes:

- Prefer **small, reviewable diffs** over refactors.
- Do not rename public CLI flags or change output paths unless explicitly instructed.
- If you need to change behavior:
  - update or add minimal docs
  - ensure backward compatibility where reasonable
  - call out any breaking change loudly
- Never add secrets to files, logs, or examples.
- If uncertain about repository structure, inspect the tree before inventing new folders/modules.

---

## 9) “Definition of Done” checklist (use for every PR/change)

- [ ] Uses `get_session()` (no new auth flows)
- [ ] Maintains JSON→XML fallback behavior if touching parsing
- [ ] Preserves export contract (timestamps, layout, manifests, latest pointers)
- [ ] Minimizes API traffic (caching/batching; no brute force loops)
- [ ] Adds/updates docstrings for public functions
- [ ] Clear error messages and input validation
- [ ] Manual run confirms outputs look correct for a real league key

---

## 10) If this file conflicts with other docs
- If this conflicts with the **existing code behavior**, the code is authoritative (and update this file after).
- If this conflicts with the **README**, keep README behavior and update this file to match.

End of file.
