#!/usr/bin/env python3
"""
Source 5 — Adani Total Gas Limited (ATGL).

ATGL operates 500+ CNG stations across Ahmedabad, Faridabad, Khurja, Udham
Singh Nagar, Dhamra, Chandrapur, and other cities.
Website: https://www.adanitotalgas.in/
"""

from __future__ import annotations

import json
import re
import time
from utils import save_raw, http_get, state_from_coords, normalise_name

SOURCE = "atgl"

_ENDPOINTS = [
    "https://www.adanitotalgas.in/api/cng-stations",
    "https://www.adanitotalgas.in/index.php/api/cng-stations",
    "https://www.adanitotalgas.in/cng/cng-stations",
    "https://www.adanitotalgas.in/index.php/cng/cng-station",
    "https://www.adanitotalgas.in/api/outlets",
    # Adani Gas (legacy brand)
    "https://www.adanigas.com/api/cng-stations",
    "https://www.adanigas.com/cng-station-locator",
]

# ATGL cities with approximate coverage
_ATGL_CITIES = [
    ("Ahmedabad", "Gujarat"),
    ("Vadodara", "Gujarat"),
    ("Faridabad", "Haryana"),
    ("Khurja", "Uttar Pradesh"),
    ("Udham Singh Nagar", "Uttarakhand"),
    ("Dhamra", "Odisha"),
    ("Chandrapur", "Maharashtra"),
    ("Ernakulam", "Kerala"),
    ("Mysuru", "Karnataka"),
    ("Bellary", "Karnataka"),
    ("Nagpur", "Maharashtra"),
    ("Jalandhar", "Punjab"),
]


def _parse_response(raw: bytes) -> list[dict]:
    text = raw.decode("utf-8", errors="replace")
    stations = []

    # JSON attempt
    try:
        data = json.loads(text)
        items = (
            data if isinstance(data, list)
            else data.get("data") or data.get("stations") or
            data.get("StationList") or data.get("cngStations") or []
        )
        for item in items:
            if not isinstance(item, dict):
                continue
            lat = (item.get("lat") or item.get("latitude") or
                   item.get("Latitude") or item.get("Lat"))
            lon = (item.get("lng") or item.get("lon") or
                   item.get("longitude") or item.get("Longitude") or item.get("Long"))
            try:
                lat, lon = float(lat), float(lon)
            except (TypeError, ValueError):
                continue
            if not (6 < lat < 38 and 68 < lon < 98):
                continue
            name = (
                item.get("name") or item.get("Name") or item.get("OutletName") or
                item.get("station_name") or "ATGL CNG Station"
            )
            city = item.get("city") or item.get("City") or ""
            state = item.get("state") or item.get("State") or state_from_coords(lat, lon)
            address = item.get("address") or item.get("Address") or ""
            stations.append({
                "id": f"atgl-{round(lat,4)}-{round(lon,4)}",
                "name": normalise_name(str(name)),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": str(city),
                "state": str(state),
                "source": SOURCE,
                "address": str(address),
                "operator": "ATGL",
            })
        if stations:
            return stations
    except json.JSONDecodeError:
        pass

    # JS embedded data
    for pat in [
        r'(?:var|const|let)\s+\w*[Ss]tation\w*\s*=\s*(\[[\s\S]+?\]);',
        r'stationData\s*=\s*(\[[\s\S]+?\]);',
        r'"stations"\s*:\s*(\[[\s\S]+?\])',
    ]:
        m = re.search(pat, text, re.DOTALL)
        if m:
            try:
                items = json.loads(m.group(1))
                parsed = _parse_json_items(items)
                if parsed:
                    return parsed
            except json.JSONDecodeError:
                pass

    # Google Maps markers
    pairs = re.findall(
        r'lat\s*:\s*([\d.]+).*?l(?:ng|on)\s*:\s*([\d.]+)',
        text, re.DOTALL
    )
    names = re.findall(r'(?:title|name)\s*:\s*["\']([^"\']{5,80})', text)
    for i, (lat_s, lon_s) in enumerate(pairs):
        try:
            lat, lon = float(lat_s), float(lon_s)
            if 6 < lat < 38 and 68 < lon < 98:
                name = names[i] if i < len(names) else "ATGL CNG Station"
                stations.append({
                    "id": f"atgl-{round(lat,4)}-{round(lon,4)}",
                    "name": normalise_name(name),
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "city": "",
                    "state": state_from_coords(lat, lon),
                    "source": SOURCE,
                    "address": "",
                    "operator": "ATGL",
                })
        except ValueError:
            pass

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
        name = item.get("name") or item.get("title") or "ATGL CNG Station"
        stations.append({
            "id": f"atgl-{round(lat,4)}-{round(lon,4)}",
            "name": normalise_name(str(name)),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": str(item.get("city") or ""),
            "state": str(item.get("state") or state_from_coords(lat, lon)),
            "source": SOURCE,
            "address": str(item.get("address") or ""),
            "operator": "ATGL",
        })
    return stations


def main() -> int:
    print("=== Fetching ATGL CNG stations ===")
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
        time.sleep(1)

    print(f"  Total ATGL: {len(all_stations)} stations")
    save_raw(SOURCE, all_stations)
    return 0 if all_stations else 1


if __name__ == "__main__":
    raise SystemExit(main())
