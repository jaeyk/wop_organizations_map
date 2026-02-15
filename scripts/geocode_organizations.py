#!/usr/bin/env python3
"""Geocode organization CSV files into point-level datasets.

Supports Geocodio (preferred when API key is available) and U.S. Census geocoder.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, Tuple

CENSUS_ENDPOINT = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
CENSUS_BENCHMARK = "Public_AR_Current"
GEOCODIO_ENDPOINT = "https://api.geocod.io/v1.7/geocode"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def save_csv(path: Path, rows: Iterable[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_query(row: dict[str, str]) -> str:
    address = (row.get("Address") or "").strip().strip(",")
    city = (row.get("City") or "").strip()
    state = (row.get("States") or "").strip()

    if address:
        return address
    if city and state:
        return f"{city}, {state}"
    if state:
        return state
    return ""


def load_dotenv_key(dotenv_path: Path, key_name: str) -> str:
    if not dotenv_path.exists():
        return ""

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, v = stripped.split("=", 1)
        if k.strip() == key_name:
            return v.strip().strip("'").strip('"')
    return ""


def load_key_from_file(path: Path) -> str:
    if not path.exists():
        return ""
    value = path.read_text(encoding="utf-8").strip()
    return value


def geocode_census(query: str, timeout: float) -> dict[str, str]:
    params = urllib.parse.urlencode(
        {
            "address": query,
            "benchmark": CENSUS_BENCHMARK,
            "format": "json",
        }
    )
    url = f"{CENSUS_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "wop-org-map-geocoder/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {
            "latitude": "",
            "longitude": "",
            "geocode_source": "census_lookup_error",
            "geocode_input": query,
            "matched_address": "",
            "match_type": "",
        }

    matches = payload.get("result", {}).get("addressMatches", [])
    if not matches:
        return {
            "latitude": "",
            "longitude": "",
            "geocode_source": "census_no_match",
            "geocode_input": query,
            "matched_address": "",
            "match_type": "",
        }

    first = matches[0]
    coords = first.get("coordinates", {})
    return {
        "latitude": str(coords.get("y", "")),
        "longitude": str(coords.get("x", "")),
        "geocode_source": "census",
        "geocode_input": query,
        "matched_address": first.get("matchedAddress", ""),
        "match_type": first.get("tigerLine", {}).get("side", ""),
    }


def geocode_geocodio(query: str, api_key: str, timeout: float) -> dict[str, str]:
    params = urllib.parse.urlencode({"q": query, "api_key": api_key})
    url = f"{GEOCODIO_ENDPOINT}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "wop-org-map-geocoder/1.0"})

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {
            "latitude": "",
            "longitude": "",
            "geocode_source": "geocodio_lookup_error",
            "geocode_input": query,
            "matched_address": "",
            "match_type": "",
        }

    results = payload.get("results", [])
    if not results:
        return {
            "latitude": "",
            "longitude": "",
            "geocode_source": "geocodio_no_match",
            "geocode_input": query,
            "matched_address": "",
            "match_type": "",
        }

    first = results[0]
    location = first.get("location", {})
    return {
        "latitude": str(location.get("lat", "")),
        "longitude": str(location.get("lng", "")),
        "geocode_source": "geocodio",
        "geocode_input": query,
        "matched_address": first.get("formatted_address", ""),
        "match_type": first.get("accuracy", ""),
    }


def geocode(query: str, provider: str, geocodio_key: str, timeout: float) -> dict[str, str]:
    if not query:
        return {
            "latitude": "",
            "longitude": "",
            "geocode_source": "missing_query",
            "geocode_input": "",
            "matched_address": "",
            "match_type": "",
        }

    if provider == "geocodio":
        return geocode_geocodio(query, api_key=geocodio_key, timeout=timeout)

    return geocode_census(query, timeout=timeout)


def render_progress(prefix: str, current: int, total: int, width: int = 28) -> None:
    if total <= 0:
        return
    ratio = current / total
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    pct = ratio * 100
    sys.stdout.write(f"\r{prefix}: [{bar}] {current}/{total} ({pct:5.1f}%)")
    if current >= total:
        sys.stdout.write("\n")
    sys.stdout.flush()


def process_dataset(
    in_path: Path,
    out_path: Path,
    cache: Dict[str, dict[str, str]],
    sleep_sec: float,
    timeout: float,
    provider: str,
    geocodio_key: str,
) -> Tuple[int, int]:
    rows = load_csv(in_path)
    if not rows:
        save_csv(out_path, [], [])
        return 0, 0

    out_rows: list[dict[str, str]] = []
    success = 0
    total = len(rows)
    prefix = in_path.stem

    for i, row in enumerate(rows, start=1):
        query = build_query(row)
        cache_key = f"{provider}::{query}"

        if cache_key not in cache:
            cache[cache_key] = geocode(query, provider=provider, geocodio_key=geocodio_key, timeout=timeout)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

        result = cache.get(cache_key, {})
        combined = dict(row)
        combined.update(
            {
                "latitude": result.get("latitude", ""),
                "longitude": result.get("longitude", ""),
                "geocode_source": result.get("geocode_source", ""),
                "geocode_input": result.get("geocode_input", query),
                "matched_address": result.get("matched_address", ""),
                "match_type": result.get("match_type", ""),
            }
        )

        if combined["latitude"] and combined["longitude"]:
            success += 1
        out_rows.append(combined)
        render_progress(prefix, i, total)

    fieldnames = list(rows[0].keys()) + [
        "latitude",
        "longitude",
        "geocode_source",
        "geocode_input",
        "matched_address",
        "match_type",
    ]
    save_csv(out_path, out_rows, fieldnames)
    return total, success


def resolve_provider(provider_arg: str, geocodio_key: str) -> str:
    if provider_arg in {"census", "geocodio"}:
        return provider_arg
    return "geocodio" if geocodio_key else "census"


def main() -> int:
    parser = argparse.ArgumentParser(description="Geocode organization CSV files into processed_data/.")
    parser.add_argument("--input-dir", default="raw_data", help="Directory containing asian_org.csv and latino_org.csv")
    parser.add_argument("--output-dir", default="processed_data", help="Directory for geocoded CSV outputs")
    parser.add_argument("--cache", default="processed_data/geocode_cache.json", help="Path to geocode cache JSON")
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between new geocode queries (seconds)")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout (seconds)")
    parser.add_argument(
        "--provider",
        choices=["auto", "census", "geocodio"],
        default="geocodio",
        help="Geocoding backend to use (default: geocodio)",
    )
    parser.add_argument(
        "--geocodio-key",
        default="",
        help="Geocodio API key (or set GEOCODIO_API_KEY environment variable)",
    )
    args = parser.parse_args()

    geocodio_key = (
        args.geocodio_key
        or os.environ.get("GEOCODIO_API_KEY", "")
        or load_dotenv_key(Path(".env.local"), "GEOCODIO_API_KEY")
        or load_key_from_file(Path("misc/geocodio_api_key.txt"))
        or load_key_from_file(Path("misc/geocodeo_api_key.txt"))
    )
    provider = resolve_provider(args.provider, geocodio_key)

    if provider == "geocodio" and not geocodio_key:
        raise SystemExit("Geocodio provider selected but no API key found. Use --geocodio-key or GEOCODIO_API_KEY.")

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    cache_path = Path(args.cache)

    cache: Dict[str, dict[str, str]] = {}
    if cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding="utf-8"))

    datasets = [
        (input_dir / "asian_org.csv", output_dir / "asian_org_geocoded.csv"),
        (input_dir / "latino_org.csv", output_dir / "latino_org_geocoded.csv"),
    ]

    print(f"Using provider: {provider}")

    stats = []
    for in_path, out_path in datasets:
        total, success = process_dataset(
            in_path,
            out_path,
            cache,
            sleep_sec=args.sleep,
            timeout=args.timeout,
            provider=provider,
            geocodio_key=geocodio_key,
        )
        stats.append((in_path.name, total, success))

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=True, indent=2), encoding="utf-8")

    for name, total, success in stats:
        print(f"{name}: geocoded {success}/{total}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
