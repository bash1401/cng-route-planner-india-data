#!/usr/bin/env python3
"""
Source 4 — Mahanagar Gas Limited (MGL).

MGL operates ~290 CNG stations in Mumbai, Thane, and Navi Mumbai.
Website: https://www.mahanagargas.com/cng/cng-station-locator
"""

from __future__ import annotations

import json
import re
import time
from utils import save_raw, http_get, state_from_coords, normalise_name

SOURCE = "mgl"

_ENDPOINTS = [
    # Try MGL API patterns
    "https://www.mahanagargas.com/api/cng-stations",
    "https://www.mahanagargas.com/api/stations",
    "https://api.mahanagargas.com/cng-stations",
    "https://www.mahanagargas.com/cng/cng-station-locator",
    "https://www.mahanagargas.com/residential/cng-station-locator",
    "https://www.mahanagargas.com/downloads/cng-stations.json",
]

# MGL areas
_MGL_AREAS = ["Mumbai", "Thane", "Navi Mumbai", "Raigad"]


def _parse_response(raw: bytes, hint_city: str = "Mumbai") -> list[dict]:
    text = raw.decode("utf-8", errors="replace")

    # Try JSON
    try:
        data = json.loads(text)
        items = (
            data if isinstance(data, list)
            else data.get("data") or data.get("stations") or
            data.get("CNG_STATION") or data.get("cng_stations") or []
        )
        stations = []
        for item in items:
            if not isinstance(item, dict):
                continue
            lat = item.get("lat") or item.get("latitude") or item.get("Latitude")
            lon = (item.get("lng") or item.get("lon") or
                   item.get("longitude") or item.get("Longitude"))
            try:
                lat, lon = float(lat), float(lon)
            except (TypeError, ValueError):
                continue
            if not (18 < lat < 21 and 72 < lon < 74):
                continue
            name = (item.get("name") or item.get("Name") or
                    item.get("station_name") or "MGL CNG Station")
            city = item.get("city") or hint_city
            address = item.get("address") or item.get("Address") or ""
            stations.append({
                "id": f"mgl-{round(lat,4)}-{round(lon,4)}",
                "name": normalise_name(str(name)),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": str(city),
                "state": "Maharashtra",
                "source": SOURCE,
                "address": str(address),
                "operator": "MGL",
            })
        if stations:
            return stations
    except json.JSONDecodeError:
        pass

    # Try HTML parsing
    stations = []

    # Google Maps embedded lat/lng
    gm_pat = re.compile(
        r'(?:lat|latitude)\s*[=:]\s*(1[89]\.\d+).*?'
        r'(?:lng|lon|longitude)\s*[=:]\s*(7[23]\.\d+)',
        re.DOTALL | re.IGNORECASE,
    )
    name_pat = re.compile(
        r'(?:title|name|stationName|outlet)\s*[=:]\s*["\']([^"\']{5,60})',
        re.IGNORECASE,
    )

    latlons = list(gm_pat.finditer(text))
    names = [m.group(1) for m in name_pat.finditer(text)]

    for i, m in enumerate(latlons):
        try:
            lat, lon = float(m.group(1)), float(m.group(2))
            name = names[i] if i < len(names) else "MGL CNG Station"
            stations.append({
                "id": f"mgl-{round(lat,4)}-{round(lon,4)}",
                "name": normalise_name(name),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": hint_city,
                "state": "Maharashtra",
                "source": SOURCE,
                "address": "",
                "operator": "MGL",
            })
        except ValueError:
            pass

    # HTML table
    if not stations:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(text, "lxml")
        for row in soup.select("table tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            for cell in cells:
                lat_m = re.search(r'(1[89]\.\d{4,})', cell)
                lon_m = re.search(r'(7[23]\.\d{4,})', cell)
                if lat_m and lon_m:
                    try:
                        lat, lon = float(lat_m.group(1)), float(lon_m.group(1))
                        stations.append({
                            "id": f"mgl-{round(lat,4)}-{round(lon,4)}",
                            "name": normalise_name(cells[0]),
                            "latitude": round(lat, 6),
                            "longitude": round(lon, 6),
                            "city": hint_city,
                            "state": "Maharashtra",
                            "source": SOURCE,
                            "address": " ".join(cells[1:]),
                            "operator": "MGL",
                        })
                    except ValueError:
                        pass

    return stations


def main() -> int:
    print("=== Fetching MGL CNG stations ===")
    all_stations: list[dict] = []
    seen: set[str] = set()

    for url in _ENDPOINTS:
        print(f"  Trying {url} …")
        raw = http_get(url, timeout=20, retries=2)
        if raw:
            parsed = _parse_response(raw)
            new = [s for s in parsed if s["id"] not in seen]
            if new:
                print(f"  ✓ {len(new)} new stations from {url}")
                all_stations.extend(new)
                seen.update(s["id"] for s in new)
        time.sleep(0.5)

    print(f"  Total MGL: {len(all_stations)} stations")
    save_raw(SOURCE, all_stations)
    return 0 if all_stations else 1


if __name__ == "__main__":
    raise SystemExit(main())
