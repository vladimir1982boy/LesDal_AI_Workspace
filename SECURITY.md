# Security Policy

## Secrets Handling

- Real credentials must live only in `secrets/.env.local`.
- Real Google service account files must live only in `secrets/google-service-account.json`.
- Tracked files must contain only placeholders or examples.

## Before A Public Push

Verify all of the following:

1. `secrets/` is ignored and not staged.
2. `.env.example` contains placeholders only.
3. `credentials.example.json` contains placeholders only.
4. Root `.env` and `credentials.json` do not contain real credentials.
5. No API keys, bot tokens, private keys, or service-account JSON files remain in tracked files.

## Rotation Rule

If a credential was ever committed, uploaded, pasted into a public issue, or exposed in logs or screenshots, treat it as compromised and rotate it.

## Supported Secret Locations

- `secrets/.env.local`
- `secrets/google-service-account.json`

## Not Supported

- Committing real `.env` values
- Committing real service-account JSON files
- Embedding secrets in docs, notebooks, scripts, or test fixtures

## Reporting

If you find a secret in the tracked tree, remove it immediately, replace it with a placeholder, and rotate the credential if there is any doubt about prior exposure.
