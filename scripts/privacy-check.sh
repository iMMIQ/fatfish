#!/usr/bin/env bash
set -euo pipefail

# Scans repository files for accidental private IPs, secrets, or personal data.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAIL=0

scan_pattern() {
  local label="$1"
  local pattern="$2"
  local hits
  hits="$(grep -rnE --exclude-dir=.git --exclude='privacy-check.sh' "$pattern" . || true)"
  if [[ -n "$hits" ]]; then
    echo "[PRIVACY FAIL] $label:"
    echo "$hits"
    FAIL=1
  fi
}

scan_pattern "Private RFC1918 192.168.x.x host IP" '192\.168\.([1-9][0-9]{0,2}\.[0-9]{1,3}|0\.[1-9][0-9]{0,2})'
scan_pattern "Private RFC1918 10.x.x.x IP (non-doc)" 'https?://10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
scan_pattern "API key / token literal" '(sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AIza[0-9A-Za-z_-]{20,})'
scan_pattern "Hardcoded Bearer token" 'Bearer [A-Za-z0-9._-]{20,}'
scan_pattern "Private heiyu/lazycat hostnames" 'heiyu\.space|lzcbox-[a-z0-9]+'

if [[ "$FAIL" -ne 0 ]]; then
  echo "Privacy check FAILED. Remove private values or replace with placeholders/env vars."
  exit 1
fi

echo "Privacy check PASSED."
