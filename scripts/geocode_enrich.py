#!/usr/bin/env python3
"""
Geocoding enrichment using Nominatim (OpenStreetMap).

Two modes:
  1. reverse_geocode   — fill missing city/state for stations that have lat/lon
  2. forward_geocode   — convert an address string to lat/lon

Nominatim usage policy: max 1 req/s, must send a real User-Agent.
We cache results in raw_sources/.geocode_cache.json to avoid re-querying.
"""

from __future__ import annotations

import json
import pathlib
import time
import urllib.parse
import urllib.request
from utils import REPO_ROOT, state_from_coords

NOMINATIM_BASE = "https://nominatim.openstreetmap.org"
CACHE_PATH = REPO_ROOT / "raw_sources" / ".geocode_cache.json"
_DELAY = 1.1  # seconds between requests

_UA = (
    "CNG-Route-Planner-India/2.0 "
    "(github.com/bash1401/cng-route-planner-india-data; "
    "contact: opensource@cngroute.in)"
)

# ─── Cache ────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if CACHE_PATH.exists():
        with CACHE_PATH.open(encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                pass
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


# ─── Nominatim calls ─────────────────────────────────────────────────────────

def reverse_geocode(lat: float, lon: float, cache: dict) -> tuple[str, str]:
    """
    Return (city, state) for given coordinates.
    Uses bounding-box lookup first (instant), then Nominatim for unknowns.
    """
    # Fast path: state from bounding box
    state = state_from_coords(lat, lon)

    key = f"rev:{round(lat,3)},{round(lon,3)}"
    if key in cache:
        cached = cache[key]
        return cached.get("city", ""), cached.get("state", state)

    params = urllib.parse.urlencode({
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": 14,
    })
    url = f"{NOMINATIM_BASE}/reverse?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        addr = data.get("address") or {}
        city = (
            addr.get("city") or addr.get("town") or
            addr.get("village") or addr.get("suburb") or
            addr.get("county") or ""
        )
        state_nom = addr.get("state") or state
        cache[key] = {"city": city, "state": state_nom}
        return city, state_nom
    except Exception:
        cache[key] = {"city": "", "state": state}
        return "", state


def forward_geocode(query: str, cache: dict) -> tuple[float, float] | None:
    """Convert address string to (lat, lon). Returns None on failure."""
    key = f"fwd:{query[:100]}"
    if key in cache:
        c = cache[key]
        if c is None:
            return None
        return c.get("lat"), c.get("lon")

    params = urllib.parse.urlencode({
        "q": query + ", India",
        "format": "json",
        "limit": 1,
        "countrycodes": "in",
        "addressdetails": 0,
    })
    url = f"{NOMINATIM_BASE}/search?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=10) as resp:
            results = json.loads(resp.read().decode("utf-8"))
        if results:
            lat = float(results[0]["lat"])
            lon = float(results[0]["lon"])
            cache[key] = {"lat": lat, "lon": lon}
            return lat, lon
        cache[key] = None
        return None
    except Exception:
        return None


# ─── Batch enrichment ────────────────────────────────────────────────────────

def enrich_stations(
    stations: list[dict],
    max_requests: int = 2000,
) -> list[dict]:
    """
    Fill in missing city/state for stations that have lat/lon.
    Respects Nominatim rate limits.
    Returns the enriched list.
    """
    cache = _load_cache()
    requests_made = 0
    enriched = 0

    for s in stations:
        if s.get("city") and s.get("state"):
            # Already have both — use state_from_coords as fallback if state is bad
            if not s["state"]:
                s["state"] = state_from_coords(s["latitude"], s["longitude"])
            continue

        if requests_made >= max_requests:
            # Ran out of API budget — use bounding box for remaining
            if not s.get("state"):
                s["state"] = state_from_coords(s["latitude"], s["longitude"])
            continue

        lat, lon = s.get("latitude"), s.get("longitude")
        if lat is None or lon is None:
            continue

        key = f"rev:{round(lat,3)},{round(lon,3)}"
        if key not in cache:
            time.sleep(_DELAY)
            requests_made += 1

        city, state = reverse_geocode(lat, lon, cache)

        if not s.get("city") and city:
            s["city"] = city
            enriched += 1
        if not s.get("state") and state:
            s["state"] = state
            enriched += 1

        if requests_made % 100 == 0 and requests_made > 0:
            _save_cache(cache)
            print(f"  [{requests_made} geocode calls, {enriched} fields filled]")

    _save_cache(cache)
    print(f"  Geocode enrichment: {requests_made} API calls, {enriched} fields filled")
    return stations


if __name__ == "__main__":
    # CLI: python geocode_enrich.py <raw_sources/file.json>
    import sys
    import pathlib
    if len(sys.argv) < 2:
        print("Usage: geocode_enrich.py <raw_json_file>")
        sys.exit(1)
    path = pathlib.Path(sys.argv[1])
    with path.open() as f:
        data = json.load(f)
    enriched = enrich_stations(data)
    with path.open("w") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(enriched)} enriched records to {path}")
