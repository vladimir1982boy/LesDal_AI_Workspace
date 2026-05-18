# Security Policy

## Secret Storage

- Store real runtime secrets only in `secrets/.env.local`.
- Store real Google service account files only in `secrets/google-service-account.json`.
- Keep tracked files limited to placeholders, examples, or sanitized values.

## Loading Order

The project loads secrets in this order:

1. `secrets/.env.local`
2. `.env.local`
3. `.env`

The root `.env` may exist as a placeholder, but it should not contain real production credentials.

## Encoding Rule

- Save `secrets/.env.local` as `UTF-8` without `BOM`.
- If the first key in `secrets/.env.local` is ignored, check for `UTF-8 with BOM` and resave the file.

## Before Running

Use this check before the first run or after editing secrets:

```powershell
.\scripts\check_secrets.ps1
```

## Before A Public Push

Verify all of the following:

1. `secrets/` is ignored and not staged.
2. `.env.example` contains placeholders only.
3. `credentials.example.json` contains placeholders only.
4. Root `.env` and `credentials.json` do not contain real credentials.
5. No API keys, bot tokens, private keys, or service-account JSON files remain in tracked files.

## Rotation Rule

If a credential was ever committed, uploaded, pasted into a public issue, or exposed in logs or screenshots, treat it as compromised and rotate it before reuse.

## Not Allowed

- Committing real `.env` values
- Committing real service-account JSON files
- Embedding secrets in docs, notebooks, scripts, or test fixtures

## If A Secret Is Found

If you find a secret in the tracked tree, remove it immediately, replace it with a placeholder, and rotate the credential if there is any doubt about prior exposure.
