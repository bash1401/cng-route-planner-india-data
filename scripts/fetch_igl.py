#!/usr/bin/env python3
"""
Source 3 — Indraprastha Gas Limited (IGL).

IGL operates ~800 CNG stations in Delhi, Noida, Greater Noida, Gurgaon,
Faridabad, Ghaziabad, Muzaffarnagar, Meerut, Ajmer, Kaithal, Karnal,
Fatehabad, Rewari, Hapur, Pilibhit.

Strategy:
  1. Try IGL's internal JSON/XML API endpoints
  2. Scrape their station-locator HTML pages per city
  3. Parse any JavaScript variables in the page
"""

from __future__ import annotations

import json
import re
import time
from utils import save_raw, http_get, http_post, state_from_coords, normalise_name

SOURCE = "igl"

# IGL cities they serve
IGL_CITIES = [
    "Delhi", "Noida", "Greater Noida", "Gurgaon", "Faridabad",
    "Ghaziabad", "Muzaffarnagar", "Meerut", "Hapur",
    "Kaithal", "Karnal", "Fatehabad", "Rewari", "Ajmer", "Pilibhit",
]

# Try these API/endpoint patterns
_API_PATTERNS = [
    "https://www.iglonline.net/api/cng-stations",
    "https://www.iglonline.net/api/stations",
    "https://www.iglonline.net/find-cng-station/",
    "https://www.iglonline.net/cng-station-list",
]

# IGL city-based locator URL (POST form)
_CITY_URL = "https://www.iglonline.net/find-cng-station/"


def _try_json_api() -> list[dict]:
    """Try known IGL JSON API patterns."""
    for url in _API_PATTERNS:
        raw = http_get(url, timeout=15, retries=2,
                       extra_headers={"Accept": "application/json, */*"})
        if not raw:
            continue
        try:
            data = json.loads(raw.decode("utf-8"))
            items = (
                data if isinstance(data, list)
                else data.get("data") or data.get("stations") or []
            )
            parsed = _parse_items(items)
            if parsed:
                print(f"  ✓ IGL JSON API: {len(parsed)} stations from {url}")
                return parsed
        except (json.JSONDecodeError, Exception):
            pass
        time.sleep(0.5)
    return []


def _scrape_city(city: str) -> list[dict]:
    """POST city to IGL locator and parse response."""
    raw = http_post(
        _CITY_URL,
        data=f"city={city}".encode("utf-8"),
        content_type="application/x-www-form-urlencoded",
        timeout=15, retries=2,
    )
    if not raw:
        return []
    return _parse_html(raw.decode("utf-8", errors="replace"), city)


def _scrape_main_page() -> list[dict]:
    """Scrape the main IGL CNG locator page for embedded data."""
    raw = http_get(_CITY_URL, timeout=20, retries=2)
    if not raw:
        return []
    return _parse_html(raw.decode("utf-8", errors="replace"), "")


def _parse_html(html: str, city_hint: str) -> list[dict]:
    stations = []

    # Look for JSON data embedded in JS vars
    patterns = [
        r'var\s+(?:stations|cngStations|markerData|locationData|stationList)\s*=\s*(\[[\s\S]*?\]);',
        r'"stations"\s*:\s*(\[[\s\S]*?\])',
        r'(?:var|const|let)\s+\w+\s*=\s*(\[\s*\{[^;]+\}\s*\]);',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.DOTALL)
        if m:
            try:
                items = json.loads(m.group(1))
                parsed = _parse_items(items, city_hint)
                if parsed:
                    return parsed
            except json.JSONDecodeError:
                pass

    # Google Maps marker pattern: new google.maps.Marker({position:{lat:X,lng:Y},…})
    gm_pat = re.compile(
        r'lat\s*:\s*([\d.]+).*?l(?:ng|on)\s*:\s*([\d.]+).*?(?:title|name)\s*:\s*["\']([^"\']+)',
        re.DOTALL
    )
    for m in gm_pat.finditer(html):
        try:
            lat, lon, name = float(m.group(1)), float(m.group(2)), m.group(3)
            if 6 < lat < 38 and 68 < lon < 98:
                stations.append(_make_record(name, lat, lon, city_hint, "Delhi"))
        except (ValueError, AttributeError):
            pass

    # HTML table fallback
    if not stations:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        for table in soup.select("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 2:
                    continue
                # Try to find lat/lon in any cell
                for i, cell in enumerate(cells):
                    lat_m = re.search(r'(2[5-9]\.\d+)', cell)
                    lon_m = re.search(r'(7[5-9]\.\d+)', cell)
                    if lat_m and lon_m:
                        try:
                            lat, lon = float(lat_m.group(1)), float(lon_m.group(1))
                            name = cells[0] if cells else "IGL CNG Station"
                            stations.append(_make_record(name, lat, lon, city_hint, "Delhi"))
                        except ValueError:
                            pass

    return stations


def _parse_items(items: list, city_hint: str = "") -> list[dict]:
    stations = []
    for item in items:
        if not isinstance(item, dict):
            continue
        lat = item.get("lat") or item.get("latitude") or item.get("Lat")
        lon = (item.get("lng") or item.get("lon") or
               item.get("longitude") or item.get("Long"))
        try:
            lat, lon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not (6 < lat < 38 and 68 < lon < 98):
            continue
        name = (
            item.get("name") or item.get("title") or
            item.get("station_name") or item.get("StationName") or
            "IGL CNG Station"
        )
        city = item.get("city") or item.get("City") or city_hint
        state = item.get("state") or state_from_coords(lat, lon)
        stations.append(_make_record(name, lat, lon, city, state,
                                     item.get("address") or ""))
    return stations


def _make_record(name: str, lat: float, lon: float,
                 city: str, state: str, address: str = "") -> dict:
    return {
        "id": f"igl-{round(lat,4)}-{round(lon,4)}",
        "name": normalise_name(name),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "city": city,
        "state": state or state_from_coords(lat, lon),
        "source": SOURCE,
        "address": address,
        "operator": "IGL",
    }


def main() -> int:
    print("=== Fetching IGL CNG stations ===")

    # Try JSON API first
    stations = _try_json_api()

    # Try scraping main page
    if not stations:
        print("  Scraping main IGL locator page …")
        stations = _scrape_main_page()

    # Try per-city scraping
    if not stations:
        print("  Trying per-city scraping …")
        for city in IGL_CITIES:
            city_stations = _scrape_city(city)
            if city_stations:
                print(f"    {city}: {len(city_stations)} stations")
            stations.extend(city_stations)
            time.sleep(1)

    print(f"  Total IGL: {len(stations)} stations")
    save_raw(SOURCE, stations)
    return 0 if stations else 1


if __name__ == "__main__":
    raise SystemExit(main())
