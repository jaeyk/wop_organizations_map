#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

FORCE_GEOCODE="${FORCE_GEOCODE:-0}"
COMMIT_MSG="${1:-Deploy updated map data and UI}"

if [ "$FORCE_GEOCODE" = "1" ]; then
  echo "[deploy] FORCE_GEOCODE=1, regenerating geocoded datasets..."
  python3 scripts/geocode_organizations.py
elif [ -s "processed_data/asian_org_geocoded.csv" ] && [ -s "processed_data/latino_org_geocoded.csv" ]; then
  echo "[deploy] Existing geocoded CSV files found; skipping API geocoding."
else
  echo "[deploy] Geocoded CSV files missing; generating datasets..."
  python3 scripts/geocode_organizations.py
fi

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
