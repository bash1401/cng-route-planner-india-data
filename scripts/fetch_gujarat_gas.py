#!/usr/bin/env python3
"""
Source 6 — Gujarat Gas Limited (GGL).

Gujarat Gas operates 1,000+ CNG stations across Gujarat.
Website: https://www.gujaratgas.com/
"""

from __future__ import annotations

import json
import re
import time
from utils import save_raw, http_get, state_from_coords, normalise_name

SOURCE = "gujarat_gas"

_ENDPOINTS = [
    "https://www.gujaratgas.com/api/cng-stations",
    "https://www.gujaratgas.com/cng-station-locator",
    "https://www.gujaratgas.com/cng-stations",
    "https://www.gujaratgas.com/retail/cng-station",
    "https://www.gujaratgas.com/json/cng-stations.json",
    "https://api.gujaratgas.com/cng-stations",
]


def _parse_response(raw: bytes) -> list[dict]:
    text = raw.decode("utf-8", errors="replace")
    stations = []

    # JSON first
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
            lat = item.get("lat") or item.get("latitude") or item.get("Latitude")
            lon = (item.get("lng") or item.get("lon") or
                   item.get("longitude") or item.get("Longitude"))
            try:
                lat, lon = float(lat), float(lon)
            except (TypeError, ValueError):
                continue
            if not (20 < lat < 25 and 68 < lon < 75):
                lat = None  # out of Gujarat
            if lat is None:
                continue
            name = (
                item.get("name") or item.get("Name") or
                item.get("station_name") or "Gujarat Gas CNG"
            )
            city = item.get("city") or item.get("City") or ""
            address = item.get("address") or item.get("Address") or ""
            stations.append({
                "id": f"ggl-{round(lat,4)}-{round(lon,4)}",
                "name": normalise_name(str(name)),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": str(city),
                "state": "Gujarat",
                "source": SOURCE,
                "address": str(address),
                "operator": "Gujarat Gas",
            })
        if stations:
            return stations
    except json.JSONDecodeError:
        pass

    # JS embedded patterns
    for pat in [
        r'(?:var|const|let)\s+\w*[Ss]tation\w*\s*=\s*(\[[\s\S]+?\]);',
        r'"stations"\s*:\s*(\[[\s\S]+?\])',
        r'(?:markerArray|markers)\s*=\s*(\[[\s\S]+?\]);',
    ]:
        m = re.search(pat, text, re.DOTALL)
        if m:
            try:
                items = json.loads(m.group(1))
                parsed = [
                    {
                        "id": f"ggl-{round(float(x.get('lat',0)),4)}-{round(float(x.get('lng',x.get('lon',0))),4)}",
                        "name": normalise_name(str(x.get("name") or x.get("title") or "Gujarat Gas CNG")),
                        "latitude": round(float(x["lat"]), 6),
                        "longitude": round(float(x.get("lng") or x.get("lon")), 6),
                        "city": str(x.get("city") or ""),
                        "state": "Gujarat",
                        "source": SOURCE,
                        "address": str(x.get("address") or ""),
                        "operator": "Gujarat Gas",
                    }
                    for x in items
                    if isinstance(x, dict)
                    and x.get("lat") and (x.get("lng") or x.get("lon"))
                    and 20 < float(x.get("lat", 0)) < 25
                ]
                if parsed:
                    return parsed
            except (json.JSONDecodeError, ValueError, KeyError):
                pass

    # HTML table
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(text, "lxml")
    for table in soup.select("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 3:
                continue
            lat_s = lon_s = ""
            for cell in cells:
                if not lat_s:
                    m = re.search(r'(2[01234]\.\d{4,})', cell)
                    if m:
                        lat_s = m.group(1)
                if not lon_s:
                    m = re.search(r'(7[01234]\.\d{4,})', cell)
                    if m:
                        lon_s = m.group(1)
            if lat_s and lon_s:
                try:
                    lat, lon = float(lat_s), float(lon_s)
                    stations.append({
                        "id": f"ggl-{round(lat,4)}-{round(lon,4)}",
                        "name": normalise_name(cells[0]),
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "city": cells[1] if len(cells) > 1 else "",
                        "state": "Gujarat",
                        "source": SOURCE,
                        "address": " ".join(cells[2:]),
                        "operator": "Gujarat Gas",
                    })
                except ValueError:
                    pass

    return stations


def main() -> int:
    print("=== Fetching Gujarat Gas CNG stations ===")
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

    print(f"  Total Gujarat Gas: {len(all_stations)} stations")
    save_raw(SOURCE, all_stations)
    return 0 if all_stations else 1


if __name__ == "__main__":
    raise SystemExit(main())
