#!/usr/bin/env bash
# Full local verification: unit tests + a real two-process delivery round-trip.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> unit tests"
node --test test/*.test.js

echo "==> tool schemas vs the installed dsh-tools"
node scripts/verify-schema.mjs

echo "==> cross-process round-trip"
node scripts/cross-process-smoke.mjs

echo "smoke: all good"
