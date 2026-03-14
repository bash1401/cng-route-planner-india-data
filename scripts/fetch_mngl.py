"""
Fetch CNG stations from Maharashtra Natural Gas Limited (MNGL) website.

MNGL serves Pune and surrounding areas including Nashik, Nanded, Ramanagara,
Sindhudurg, Valsad, and Nizamabad.

The website lists ~120 CNG stations with Google Maps links containing exact coordinates.
Source: https://mngl.in/cng/cylinders-filling-stations
Saves to raw_sources/mngl.json
"""
from __future__ import annotations

import os
import re
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, state_from_coords, INDIA_STATES, normalise_name

SOURCE = "mngl"

BASE_URL = "https://mngl.in/cng/cylinders-filling-stations"


def _dms_to_dd(deg: str, min_: str, sec: str, direction: str) -> float:
    dd = float(deg) + float(min_) / 60 + float(sec) / 3600
    if direction in ("S", "W"):
        dd = -dd
    return dd


def _parse_maps_url(url: str) -> tuple[float | None, float | None]:
    """Extract lat/lon from Google Maps URL (decimal or DMS format)."""
    url = (
        url.replace("&#039;", "'")
        .replace("&quot;", '"')
        .replace("&amp;", "&")
    )
    # Decimal: place/18.507218, 73.802620
    m = re.search(r"place/([-\d.]+),\s*([-\d.]+)", url)
    if m:
        return float(m.group(1)), float(m.group(2))
    # DMS: 18°28'58.7"N 73°48'08.1"E
    m = re.search(
        r"place/(\d+)°(\d+)'([\d.]+)\"([NS])\s+(\d+)°(\d+)'([\d.]+)\"([EW])", url
    )
    if m:
        lat = _dms_to_dd(m.group(1), m.group(2), m.group(3), m.group(4))
        lon = _dms_to_dd(m.group(5), m.group(6), m.group(7), m.group(8))
        return lat, lon
    return None, None


def _fetch_page(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read(500_000).decode("utf-8", errors="ignore")


def _parse_stations(body: str) -> list[dict]:
    records: list[dict] = []
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", body, re.S)
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
        if len(cells) < 6:
            continue
        name = re.sub(r"<[^>]+>", "", cells[1]).strip()
        if not name or name.isdigit():
            continue

        area = re.sub(r"<[^>]+>", "", cells[2]).strip()

        maps_match = re.search(
            r'href=["\']([^"\']*google[^"\']*maps[^"\']*)["\']',
            cells[5] if len(cells) > 5 else "",
        )
        if not maps_match:
            continue

        lat, lon = _parse_maps_url(maps_match.group(1))
        if lat is None or lon is None:
            continue
        if not (6.0 <= lat <= 38.0 and 68.0 <= lon <= 98.0):
            continue

        state = state_from_coords(lat, lon)
        if not state or state not in INDIA_STATES:
            state = "Maharashtra"

        records.append(
            {
                "name": normalise_name(name) or "MNGL CNG Station",
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": area or "Pune",
                "state": state,
                "source": SOURCE,
            }
        )
    return records


def main() -> int:
    print("[MNGL] Fetching CNG station list…")
    try:
        body = _fetch_page(BASE_URL)
    except Exception as exc:
        print(f"[MNGL] Fetch failed: {exc}")
        save_raw(SOURCE, [])
        return 0

    records = _parse_stations(body)
    print(f"[MNGL] Extracted {len(records)} CNG stations")
    save_raw(SOURCE, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
