#!/usr/bin/env python3
"""Validate the LowFarm secrets and check for leaks.

Checks, in order:

1. the secrets file exists and is not the template;
2. every managed key is present and non-empty;
3. the values are strong enough (length, and not a known placeholder);
4. the file permissions are 0600;
5. no secret value appears in the git index;
6. no secret value appears anywhere in the git history.

Exit code 0 means clean, 1 means problems were found.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SECRETS = REPO_ROOT / "secrets" / "lowfarm.env"
TEMPLATE = REPO_ROOT / "secrets" / "lowfarm.env.example"

REQUIRED_KEYS = [
    "GATEWAY_API_KEY",
    "ADMIN_API_KEY",
    "WORKER_ADMIN_KEY",
    "GRAFANA_PASSWORD",
]

# Only these are scanned for leaks.  A passthrough value such as
# GRAFANA_USER=admin is a common string and would flag every file.
SECRET_KEYS = REQUIRED_KEYS

MIN_LENGTH = 24

FORBIDDEN_MARKERS = [
    "change-me",
    "changeme",
    "example",
    "password",
    "secret123",
    "lowfarm-secret",
]

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def read_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def git(*args: str, cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def check_git_index(values: dict[str, str]) -> list[str]:
    """Any secret value present in a tracked file is a leak."""
    result = git("ls-files", "-z")
    if result.returncode != 0:
        return []
    problems: list[str] = []
    for relative in result.stdout.split("\0"):
        if not relative:
            continue
        path = REPO_ROOT / relative
        if not path.is_file() or path.stat().st_size > 2_000_000:
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for name in SECRET_KEYS:
            value = values.get(name, "")
            if value and value in content:
                problems.append(f"{name} appears in tracked file {relative}")
    return problems


def check_git_history(values: dict[str, str]) -> list[str]:
    """Any secret value in the history is a leak, even if later removed."""
    result = git("log", "--all", "-p", "--no-color")
    if result.returncode != 0:
        return []
    history = result.stdout
    problems: list[str] = []
    for name in SECRET_KEYS:
        value = values.get(name, "")
        if value and value in history:
            digest = hashlib.sha256(value.encode()).hexdigest()[:12]
            problems.append(
                f"{name} (sha256:{digest}) appears in the git history — rotate it"
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secrets", type=Path, default=DEFAULT_SECRETS)
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="skip the (slower) git history scan",
    )
    args = parser.parse_args()

    failures: list[str] = []
    warnings: list[str] = []

    if not args.secrets.exists():
        print(f"{RED}FAIL{RESET} {args.secrets} does not exist")
        print("     run: make secrets")
        return 1

    if args.secrets.resolve() == TEMPLATE.resolve():
        print(f"{RED}FAIL{RESET} refusing to validate the template")
        return 1

    values = read_values(args.secrets)

    print(f"checking {args.secrets.relative_to(REPO_ROOT)}")
    for name in REQUIRED_KEYS:
        value = values.get(name, "")
        if not value:
            failures.append(f"{name} is missing or empty")
            continue
        if len(value) < MIN_LENGTH:
            failures.append(
                f"{name} is only {len(value)} chars (minimum {MIN_LENGTH})"
            )
        lowered = value.lower()
        for marker in FORBIDDEN_MARKERS:
            if marker in lowered:
                failures.append(f"{name} contains the placeholder marker {marker!r}")
                break

    mode = stat.S_IMODE(args.secrets.stat().st_mode)
    if mode & 0o077:
        failures.append(
            f"file mode is {oct(mode)}; it must be 0600 (owner only)"
        )

    if not args.skip_history:
        failures.extend(check_git_index(values))
        failures.extend(check_git_history(values))

    if warnings:
        for warning in warnings:
            print(f"  {YELLOW}WARN{RESET} {warning}")

    if failures:
        print(f"\n{RED}{len(failures)} problem(s) found:{RESET}")
        for failure in failures:
            print(f"  {RED}x{RESET} {failure}")
        return 1

    print(f"  {GREEN}ok{RESET} {len(REQUIRED_KEYS)} keys, mode 0600, no leaks detected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
