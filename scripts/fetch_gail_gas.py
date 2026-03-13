#!/usr/bin/env python3
"""
Source 2 — GAIL Gas CNG stations.

GAIL Gas operates ~1,400 CNG stations across 16 states.
Strategy (in priority order):
  1. Download their public JSON from their website API
  2. Download their public Excel/CSV from S3
  3. Scrape their station-locator HTML page
  4. Use embedded Google Maps KML if found

Saves results to raw_sources/gail.json
"""

from __future__ import annotations

import json
import re
import time
from utils import (
    save_raw, http_get, state_from_coords, normalise_name
)

SOURCE = "gail"

# Known GAIL Gas API endpoints / public data URLs (try in order)
_ENDPOINTS = [
    # GAIL Gas public station list API (JSON)
    "https://www.gailgas.com/api/cng-stations",
    "https://www.gailgas.com/cng-stations.json",
    "https://gailgas.com/wp-json/wp/v2/cng_stations",
    # Public S3 data
    "https://gailgaspdfdownloads.s3.ap-south-1.amazonaws.com/GAILGas-ExistingCNGStations.json",
    # Their main locator page (scrape fallback)
    "https://www.gailgas.com/cng-stations",
    "https://www.gailgas.com/retail-outlets/cng-stations",
]

# GAIL Gas operates in these cities — used as fallback for geocoding
_KNOWN_GAIL_CITIES = [
    # (city, state, approx_count)
    ("Agra", "Uttar Pradesh", 40),
    ("Aligarh", "Uttar Pradesh", 20),
    ("Allahabad", "Uttar Pradesh", 25),
    ("Bareilly", "Uttar Pradesh", 20),
    ("Bhopal", "Madhya Pradesh", 30),
    ("Firozabad", "Uttar Pradesh", 15),
    ("Gwalior", "Madhya Pradesh", 20),
    ("Haldwani", "Uttarakhand", 12),
    ("Indore", "Madhya Pradesh", 35),
    ("Jabalpur", "Madhya Pradesh", 20),
    ("Jhansi", "Uttar Pradesh", 15),
    ("Kanpur", "Uttar Pradesh", 40),
    ("Kota", "Rajasthan", 25),
    ("Lucknow", "Uttar Pradesh", 45),
    ("Mathura", "Uttar Pradesh", 25),
    ("Meerut", "Uttar Pradesh", 30),
    ("Moradabad", "Uttar Pradesh", 20),
    ("Ranchi", "Jharkhand", 20),
    ("Saharanpur", "Uttar Pradesh", 15),
    ("Varanasi", "Uttar Pradesh", 30),
    ("Ujjain", "Madhya Pradesh", 15),
]


def _try_json_api(url: str) -> list[dict]:
    """Try fetching a JSON API endpoint."""
    raw = http_get(url, timeout=15, retries=2,
                   extra_headers={"Accept": "application/json"})
    if not raw:
        return []
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []

    stations = []
    # Handle both list and dict responses
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Common patterns: data['stations'], data['data'], data['results']
        items = (
            data.get("stations") or data.get("data") or
            data.get("results") or data.get("list") or []
        )
        if not isinstance(items, list):
            return []
    else:
        return []

    for item in items:
        if not isinstance(item, dict):
            continue
        lat = item.get("lat") or item.get("latitude") or item.get("Latitude")
        lon = item.get("lon") or item.get("lng") or item.get("longitude") or item.get("Longitude")
        if lat is None or lon is None:
            continue
        try:
            lat, lon = float(lat), float(lon)
        except (ValueError, TypeError):
            continue
        name = (
            item.get("name") or item.get("Name") or
            item.get("station_name") or item.get("StationName") or "GAIL CNG Station"
        )
        city = item.get("city") or item.get("City") or ""
        state = item.get("state") or item.get("State") or state_from_coords(lat, lon)
        address = item.get("address") or item.get("Address") or ""
        stations.append({
            "id": f"gail-{len(stations)}",
            "name": normalise_name(str(name)),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": str(city),
            "state": str(state),
            "source": SOURCE,
            "address": str(address),
            "operator": "GAIL Gas",
        })
    return stations


def _scrape_html(url: str) -> list[dict]:
    """Scrape station data from GAIL Gas website HTML."""
    raw = http_get(url, timeout=20, retries=2)
    if not raw:
        return []
    html = raw.decode("utf-8", errors="replace")

    stations = []

    # Pattern 1: JSON embedded in a <script> tag
    json_patterns = [
        r'var\s+stations\s*=\s*(\[.*?\]);',
        r'var\s+cngStations\s*=\s*(\[.*?\]);',
        r'"stations"\s*:\s*(\[.*?\])',
        r'stationData\s*=\s*(\[.*?\])',
        r'var\s+locations\s*=\s*(\[.*?\]);',
    ]
    for pat in json_patterns:
        match = re.search(pat, html, re.DOTALL)
        if match:
            try:
                items = json.loads(match.group(1))
                parsed = _parse_json_items(items)
                if parsed:
                    print(f"  Extracted {len(parsed)} from JS variable in {url}")
                    return parsed
            except json.JSONDecodeError:
                pass

    # Pattern 2: HTML table with station data
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    if len(rows) > 2:
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            rec = dict(zip(headers, cells))
            name = rec.get("station name") or rec.get("name") or rec.get("outlet") or ""
            city = rec.get("city") or rec.get("district") or ""
            state = rec.get("state") or ""
            address = rec.get("address") or rec.get("location") or ""
            lat_s = rec.get("latitude") or rec.get("lat") or ""
            lon_s = rec.get("longitude") or rec.get("long") or rec.get("lng") or ""
            try:
                lat, lon = float(lat_s), float(lon_s)
            except (ValueError, TypeError):
                continue
            stations.append({
                "id": f"gail-html-{len(stations)}",
                "name": normalise_name(name) or "GAIL CNG Station",
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": city,
                "state": state or state_from_coords(lat, lon),
                "source": SOURCE,
                "address": address,
                "operator": "GAIL Gas",
            })

    print(f"  Scraped {len(stations)} from HTML table in {url}")
    return stations


def _parse_json_items(items: list) -> list[dict]:
    stations = []
    for item in items:
        if not isinstance(item, dict):
            continue
        lat = item.get("lat") or item.get("latitude")
        lon = item.get("lng") or item.get("lon") or item.get("longitude")
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        name = item.get("name") or item.get("title") or "GAIL CNG Station"
        stations.append({
            "id": f"gail-{len(stations)}",
            "name": normalise_name(str(name)),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": str(item.get("city") or ""),
            "state": str(item.get("state") or state_from_coords(lat, lon)),
            "source": SOURCE,
            "address": str(item.get("address") or ""),
            "operator": "GAIL Gas",
        })
    return stations


def main() -> int:
    print("=== Fetching GAIL Gas CNG stations ===")

    # Try JSON API endpoints
    for url in _ENDPOINTS:
        print(f"  Trying {url} …")
        if url.endswith(".json") or "api" in url or "json" in url:
            stations = _try_json_api(url)
        else:
            stations = _scrape_html(url)

        if stations:
            print(f"  ✓ Got {len(stations)} GAIL stations from {url}")
            save_raw(SOURCE, stations)
            return 0
        time.sleep(1)

    print("  ✗ All GAIL Gas endpoints failed — saving empty set")
    save_raw(SOURCE, [])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
