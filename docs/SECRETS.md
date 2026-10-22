# Secret Policy

How LowFarm handles the credentials that guard the farm, the gateway and the
control plane. This is the operational companion to
[`MasterProjectPlan.md`](../docs/MasterProjectPlan.md) §6.

The short version: **one file, one generator, one scanner, and nothing that
looks like a key ever reaches git or a log.**

---

## 1\. Classification

| Secret | Where it lives | What it grants | Exposure if leaked |
|---|---|---|---|
| `GATEWAY_API_KEY` | `secrets/lowfarm.env` | The public OpenAI-compatible plane | Anyone can spend the farm's capacity and read the chat traffic |
| `ADMIN_API_KEY` | `secrets/lowfarm.env` | The worker control plane, client management | Anyone can register rogue workers, read the fleet, create API keys |
| `WORKER_ADMIN_KEY` | `secrets/lowfarm.env` | Worker → gateway registration | Anyone can register a worker and receive inference traffic |
| `GRAFANA_PASSWORD` | `secrets/lowfarm.env` | The metrics dashboard | Anyone can read the fleet's behaviour and topology |
| Per-client keys | Redis (SHA-256 hash only) | A single client's quota | One client's quota, not the farm |
| `GATEWAY_API_KEY` in the worker | Termux, per device | That worker's upstream access | One worker's capacity |

The **control plane is the crown jewel**: `ADMIN_API_KEY` can mint client
keys. Treat it as the most sensitive value in the system, and keep it off
any machine that runs the workers.

---

## 2\. Generation

```sh
make secrets
```

which runs `scripts/generate_secrets.py`:

* every value is `secrets.token_urlsafe(32)` — 256 bits from the OS CSPRNG;
* the file is written with mode `0600`;
* the script prints **SHA-256 prefixes, never values**, so its output is
  safe in a terminal, a CI log or a screen share.

Rotating one key without touching the others:

```sh
python scripts/generate_secrets.py --rotate gateway_api_key
python scripts/generate_secrets.py --rotate all
```

Never use a password manager's " memorable" mode, a dictionary word, a
hostname, or the same value in two environments.

---

## 3\. Storage

| Environment | Mechanism |
|---|---|
| Development | `secrets/lowfarm.env`, mode `0600`, gitignored |
| Compose | `--env-file secrets/lowfarm.env` (see `Makefile`) |
| Production | Docker `secrets:` (see §7) or a mounted `0400` file from a vault |

The file is gitignored, and the scan in §5 proves it stays that way.

```sh
# every environment gets its own file
cp secrets/lowfarm.env.example secrets/lowfarm.staging.env
```

---

## 4\. Rotation

Rotation must not drop traffic, so the gateway accepts **more than one key
at a time**:

```sh
# during the rotation window both keys work
GATEWAY_API_KEY=newkey...,oldkey...
```

The procedure:

1. Generate the new key: `python scripts/generate_secrets.py --rotate gateway_api_key`.
2. Append the **old** key to the list, separated by a comma, and restart the
   gateway. Both are now valid.
3. Distribute the new key to the clients. Watch `lowfarm_requests_total`
   to confirm traffic is using it.
4. Remove the old key from the list and restart.

The same mechanism applies independently to `ADMIN_API_KEY`, so the control
plane and the public plane never need to rotate in lockstep.

`WORKER_ADMIN_KEY` rotates the same way, but note that every worker must
pick up the new value — otherwise it will fail to register.

> **Rotation revokes the old bootstrap key automatically.** The default
> client record lives in Redis and outlives the process, so the gateway
> keeps a pointer to it and deletes the previous record when the bootstrap
> key changes. Without that, a rotated key would keep working forever.
> Exactly one client may be named `default`, and it is always the current
> bootstrap key. Clients created through `POST /v1/admin/clients` are
> unaffected by a rotation of the bootstrap key.

**Rotation interval:** every 90 days, and immediately after any suspected
exposure.

---

## 5\. Leak detection

```sh
make check
```

`scripts/check_secrets.py` verifies:

1. the secrets file exists and is not the template;
2. every managed key is present and non-empty;
3. the values are at least 24 characters and contain no placeholder marker
   (`change-me`, `example`, `password`, …);
4. the file mode is `0600`;
5. **no secret value appears in any tracked file**;
6. **no secret value appears anywhere in the git history** — a key that was
   committed and later removed is still a leak.

Points 5 and 6 are what make the policy enforceable rather than aspirational.

The gateway also audits its own keys at boot: with `REQUIRE_STRONG_KEYS=true`
(compose default) it **refuses to start** with a placeholder key. The
variable name is logged, never the value.

---

## 6\. Revocation

| Scope | How |
|---|---|
| One client | `DELETE /v1/admin/clients/{key_hash}` — instant, the key stops working |
| The public plane | Remove the key from `GATEWAY_API_KEY` and restart. If it was the bootstrap key, its client record is deleted automatically. |
| The control plane | Remove the key from `ADMIN_API_KEY` and restart |
| Everything | `docker compose down` and rotate all |

A revoked per-client key is deleted from Redis, so it cannot be reused
even if the same secret is presented again.

---

## 7\. Production: Docker secrets

For a deployed farm, prefer Docker's secret mechanism over environment
variables — the value never appears in `docker inspect` output:

```yaml
services:
  gateway:
    secrets:
      - gateway_api_key
      - admin_api_key
    environment:
      - GATEWAY_API_KEY_FILE=/run/secrets/gateway_api_key

secrets:
  gateway_api_key:
    file: /etc/lowfarm/secrets/gateway_api_key
  admin_api_key:
    file: /etc/lowfarm/secrets/admin_api_key
```

The gateway reads `*_FILE` variables in preference to the plain ones. This
is the shape to use with a vault (Vault, SOPS, sealed-secrets) that
materialises the file on the host.

---

## 8\. Incident response

If a key may have leaked:

1. **Revoke it first.** `DELETE /v1/admin/clients/{hash}` for a client key,
   or remove it from the list and restart for a plane key.
2. **Rotate.** `make secrets` → `python scripts/generate_secrets.py --rotate all`.
3. **Check the history.** `make check` will tell you whether the value is in
   git. If it is, the key is burned — do not "fix" it by removing the file;
   the commit is still in the history and the key must be considered public.
4. **If it was in git history**, rewrite the history (`git filter-repo`) or,
   more practically, treat the key as compromised and rotate it. History
   rewriting is not a substitute for rotation.
5. **Audit usage.** `lowfarm_requests_total` and the admin audit log tell
   you whether the key was used by someone else.

---

## 9\. Rules of thumb

- **Never** print a secret. The scripts in this repo print fingerprints.
- **Never** commit a secret. The scanner in §5 exists so that this is
  enforced, not merely intended.
- **Never** reuse a key across environments.
- **Never** let a key travel over an unencrypted channel. The farm's LAN
  is the blast radius; the gateway's public plane should be behind TLS
  (§6.1 of the plan) and a VPN for remote access.
- **Never** log the `Authorization` header. The gateway never does.
