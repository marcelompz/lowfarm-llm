# LowFarm secrets

This directory holds the **real** secret values. It is gitignored — only
`lowfarm.env.example` and this README are committed.

```
secrets/
├── README.md            ← committed
├── lowfarm.env.example  ← committed, template
└── lowfarm.env          ← IGNORED, the real values (mode 0600)
```

## Never

- Commit `lowfarm.env`. The pre-commit guard (`scripts/check_secrets.py`)
  fails the build if any secret value appears in the index or in the git
  history.
- Paste a key into a log, a commit message, an issue or a chat. The scripts
  in this repo print **hashes**, never values.
- Reuse a key across environments. Dev, staging and production get their
  own file.

## Quick start

```sh
make secrets     # generates secrets/lowfarm.env with 256-bit random values
make check       # validates strength, permissions and leak history
make up          # brings the stack up using that file
```

See [`docs/SECRETS.md`](../docs/SECRETS.md) for the full policy:
classification, generation, rotation, revocation and incident response.
