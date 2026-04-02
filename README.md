# LesDal AI Workspace

Internal workspace for LesDal content automation and AI-assisted sales inbox workflows.

## What Is Included

- Content pipeline for RSS -> AI post generation -> Telegram / MAX / VK publishing
- AI sales bot runtime for Telegram / VK / MAX
- Local operator inbox dashboard for manual takeover and reply workflows
- SQLite and JSON fallback storage for conversation state

## Quick Start

1. Create `secrets/.env.local` from `.env.example`.
2. Put real credentials only in `secrets/.env.local`.
3. If Google Sheets sync is needed, place the service account file at `secrets/google-service-account.json`.
4. Install dependencies:

```powershell
.\.venv\Scripts\pip.exe install -r requirements.txt
```

5. Run the dashboard:

```powershell
python -B AI_BOT\dashboard_main.py
```

## Secrets Policy

- Do not store real secrets in tracked files.
- Do not commit `secrets/`, `.env.local`, `.env.*`, service-account JSON files, or private keys.
- Use `.env.example` and `credentials.example.json` as safe templates.

See [SECURITY.md](SECURITY.md) for the publication and rotation policy.

## Main Paths

- `src/ai_sales_bot/` - sales bot runtime, dashboard, channels, storage
- `src/processors/` - content generation pipeline
- `src/publishers/` - Telegram / VK / MAX content publishing
- `AI_BOT/` - launch scripts, catalog, local runtime assets
- `Docs/` - supporting notes and setup docs
- `tests/` - unit tests

## Notes

- This repository is safe to publish only in its sanitized state.
- If a real secret was ever committed or shared publicly, rotate it before reuse.
