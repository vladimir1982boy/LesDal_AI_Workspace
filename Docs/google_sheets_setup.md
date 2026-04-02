# Google Sheets Setup

## Security Note

- Keep real secrets only in `secrets/.env.local`.
- Keep the Google service account JSON only in `secrets/google-service-account.json`.
- Commit only `.env.example` and `credentials.example.json`.
- If a secret has ever been committed or shared, rotate it before making the repository public.

## What You Need

1. Create a Google Cloud project.
2. Enable the Google Sheets API.
3. Create a Service Account.
4. Download the JSON credentials file.
5. Create a Google Sheet for leads.
6. Share the sheet with the service account email as an editor.

## Environment Variables

Put the real values in `secrets/.env.local`:

- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SHEETS_CREDENTIALS_PATH=secrets/google-service-account.json`
- `GOOGLE_SHEETS_LEADS_SHEET`

Alternative to the file:

- `GOOGLE_SHEETS_CREDENTIALS_JSON`

If `GOOGLE_SHEETS_CREDENTIALS_JSON` is set, it has priority over the file path.

## How To Get Spreadsheet ID

Take it from the sheet URL:

`https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit`

## How Sync Works

- The bot stores leads in the local database.
- After each important conversation update, it upserts a row in the `Leads` sheet.
- The row key is `conversation_id`.
- If the sheet does not exist yet, the bot tries to create it automatically.

## Columns Written By The Bot

- `conversation_id`
- `lead_id`
- `contact_id`
- `created_at`
- `updated_at`
- `channel`
- `external_user_id`
- `external_chat_id`
- `stage`
- `mode`
- `display_name`
- `username`
- `city`
- `interested_products`
- `tags`
- `summary`
- `last_sender`
- `last_message`
- `manager_summary`
