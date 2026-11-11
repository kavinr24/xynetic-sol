# AGENT BLACKLIST

Files and paths listed here are **off-limits to AI agents** (and any automated tooling).
Agents must not read, print, copy, modify, rename, move, or delete them.

If a task appears to require touching anything on this list, stop and ask the human owner.

## Never touch

| Path / pattern | Why |
| --- | --- |
| `.env` | Real environment file with live secrets |
| `.env.local`, `.env.*.local` | Local developer secrets |
| `.env.production`, `.env.staging`, `.env.development` | Environment-specific secrets |
| `.env.*` (except `.env.example`) | Any environment file may hold real values |
| `*.pem`, `*.key`, `*.p12`, `*.pfx`, `*.jks`, `*.keystore` | Private keys and certificates |
| `id_rsa`, `id_rsa.*`, `id_ed25519`, `id_ed25519.*` | SSH private keys |
| `*.crt`, `*.cer` | Certificate material |
| `.ssh/` | SSH configuration and keys |
| `.aws/`, `.gcp/`, `.azure/`, `.kube/` | Cloud provider credentials |
| `.npmrc`, `.pypirc`, `.netrc`, `.git-credentials` | Registry and credential stores |
| `secrets.*`, `credentials.*`, `auth.json`, `token*.json` | Files whose names signal secrets |
| `**/service-account*.json` | GCP service account keys |
| `*.tfstate`, `*.tfstate.backup` | Terraform state (often contains secrets) |
| `.git/` | Repository internals; never rewrite history or hooks |
| `.freebuff/` | Agent runtime metadata |

## Do not commit or echo

- Never paste secret values into code, commit messages, logs, issues, or chat output.
- Never commit any file matching the patterns above, even if it was already staged.
- Never run `git push --force`, `git reset --hard`, or delete branches unless explicitly instructed.
- Never add these paths to `.gitignore` exceptions (e.g. negating `.env`).

## Allowed

- `.env.example`, `.env.sample`, `.env.template` — placeholders with **no real values**.
- Editing this file, `.gitignore`, and documentation is allowed.

## Notes for agents

1. Treat any unknown file matching a secret-like pattern (`*secret*`, `*token*`, `*credential*`, `*password*`, `*.key`, `*.pem`) as blacklisted by default.
2. If a secret is discovered in a tracked file, report it — do not scrub it yourself.
3. When in doubt, ask before acting.
