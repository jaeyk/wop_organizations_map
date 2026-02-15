#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

COMMIT_MSG="${1:-Deploy updated map data and UI}"

echo "[deploy] Geocoding datasets..."
python3 scripts/geocode_organizations.py

echo "[deploy] Staging changes..."
git add -A

if git diff --cached --quiet; then
  echo "[deploy] No changes to commit."
  exit 0
fi

echo "[deploy] Committing..."
git commit -m "$COMMIT_MSG"

echo "[deploy] Pushing to origin/main..."
git push origin main

echo "[deploy] Done."
