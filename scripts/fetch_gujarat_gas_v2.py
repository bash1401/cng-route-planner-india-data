"""
Scrape Gujarat Gas Limited CNG stations.

Sources:
  1. https://www.gujaratgas.com/cng/cng-stations/ (primary, 343+ stations)
  2. https://test.gujaratgas.com/cng/cng-stations/ (fallback, 564+ stations)

Stations are in a static HTML page organised in regional tabs.
Coordinates are obtained using a tiered geocoding approach:
  1. Village/locality + Taluka + District + State via Nominatim
  2. PIN code + State via Nominatim
  3. District + State via Nominatim

Saves to raw_sources/gujarat_gas.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, normalise_name, state_from_coords, INDIA_STATES

SOURCE = "gujarat_gas"

_SOURCES = [
    "https://www.gujaratgas.com/cng/cng-stations/",
    "https://test.gujaratgas.com/cng/cng-stations/",
]
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}

_GEOCODE_DELAY = 1.2
# Cache: query_string -> (lat, lon) or None
_GEO_CACHE: dict[str, tuple[float, float] | None] = {}


def _nominatim(query: str) -> tuple[float, float] | None:
    """Call Nominatim structured or free-form search."""
    if query in _GEO_CACHE:
        return _GEO_CACHE[query]
    time.sleep(_GEOCODE_DELAY)
    try:
        params = urllib.parse.urlencode({
            "q": query, "format": "json", "limit": "1", "countrycodes": "in",
        })
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "CNG-Planner-India/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        if data:
            result: tuple[float, float] | None = (float(data[0]["lat"]), float(data[0]["lon"]))
            _GEO_CACHE[query] = result
            return result
    except Exception:
        pass
    _GEO_CACHE[query] = None
    return None


def _geocode_station(name: str, addr: str, state: str) -> tuple[float, float] | None:
    """Try progressively coarser geocoding until we get a result."""
    # Extract components
    pin_m = re.search(r"\b(\d{6})\b", addr)
    pin = pin_m.group(1) if pin_m else ""

    # Extract district
    dist_m = re.search(r"Dist[.:\s]+([A-Za-z][A-Za-z\s]+?)(?:\s*$|\s*Tal|\s*\d|,)", addr, re.I)
    district = dist_m.group(1).strip() if dist_m else ""

    # Extract taluka
    tal_m = re.search(r"Tal[.:\s]+([A-Za-z][A-Za-z\s]+?)(?:\s*$|\s*Dist|\s*\d|,)", addr, re.I)
    taluka = tal_m.group(1).strip() if tal_m else ""

    # Extract village/locality (text before first comma or dash that looks like a name)
    village_m = re.search(r"[Vv]ill(?:age)?[.:\s]+([A-Za-z][A-Za-z\s]+?)(?:\s*,|\s*\d|$)", addr)
    village = village_m.group(1).strip() if village_m else ""

    # Tier 1: village + taluka + district + state
    if village and district and state:
        result = _nominatim(f"{village}, {taluka or district}, {state}, India")
        if result:
            return result

    # Tier 2: taluka + district + state  
    if taluka and district and state:
        result = _nominatim(f"{taluka}, {district}, {state}, India")
        if result:
            return result

    # Tier 3: PIN code + state
    if pin:
        result = _nominatim(f"{pin}, {state}, India")
        if result:
            return result

    # Tier 4: district + state
    if district and state:
        result = _nominatim(f"{district}, {state}, India")
        if result:
            return result

    return None


_REGION_STATE_MAP: dict[str, str] = {
    "central-gujarat": "Gujarat",
    "north-gujarat": "Gujarat",
    "south-gujarat": "Gujarat",
    "saurashtra": "Gujarat",
    "maharashtra": "Maharashtra",
    "rajasthan": "Rajasthan",
    "madhyapradesh": "Madhya Pradesh",
    "haryana-br": "Haryana",
    "haryana": "Haryana",
    "punjab": "Punjab",
    "dadra_nagar_haveli": "Dadra and Nagar Haveli",
    "dadra-and-nagar-haveli": "Dadra and Nagar Haveli",
}


def _fetch_body(url: str) -> str | None:
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read(500_000).decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"[GGL] Fetch failed {url}: {exc}")
        return None


def _parse_body(body: str) -> list[tuple[str, str, str]]:
    """Return list of (region_id, name, address) tuples."""
    entries: list[tuple[str, str, str]] = []

    # Find all tab sections by id
    tab_sections = re.findall(
        r'<div[^>]+id="([^"]+)"[^>]*class="[^"]*tab-content[^"]*"[^>]*>(.*?)(?=<div[^>]+id="[^"]+"\s+class="[^"]*tab-content|</div>\s*</div>\s*</div>\s*</div>)',
        body, re.S | re.I,
    )

    if not tab_sections:
        # Fallback: parse the whole page
        tab_sections = [("all", body)]

    for region_id, section_html in tab_sections:
        # Pattern 1: explicit strong + span.small structure
        found = re.findall(
            r'<strong>([^<]{3,100})</strong><br/?>\s*<span[^>]*class="small"[^>]*>(.*?)</span>',
            section_html, re.S,
        )
        if not found:
            # Pattern 2: strong followed by br and text
            found = re.findall(
                r'<strong>([^<]{3,100})</strong><br/?>\s*([^<]{10,300})',
                section_html,
            )
        for name, addr in found:
            clean_addr = re.sub(r"<[^>]+>", " ", addr).strip()
            clean_addr = re.sub(r"\s+", " ", clean_addr)
            entries.append((region_id, name.strip(), clean_addr))

    return entries


def main() -> int:
    print("[GGL] Fetching Gujarat Gas CNG station list…")

    body: str | None = None
    for url in _SOURCES:
        body = _fetch_body(url)
        if body and len(body) > 10000:
            print(f"[GGL] Using source: {url}")
            break

    if not body:
        print("[GGL] All sources failed.")
        save_raw(SOURCE, [])
        return 1

    print(f"[GGL] Page size: {len(body):,} bytes")
    entries = _parse_body(body)
    print(f"[GGL] Found {len(entries)} raw station entries")

    records: list[dict] = []
    for i, (region_id, raw_name, addr) in enumerate(entries):
        state = _REGION_STATE_MAP.get(region_id.lower(), "Gujarat")
        name = normalise_name(raw_name) or "Gujarat Gas CNG Station"

        coords = _geocode_station(name, addr, state)
        if not coords:
            continue

        lat, lon = coords
        if not (6.0 <= lat <= 38.0 and 68.0 <= lon <= 98.0):
            continue

        actual_state = state_from_coords(lat, lon) or state
        if actual_state not in INDIA_STATES:
            continue

        pin_m = re.search(r"\b(\d{6})\b", addr)
        dist_m = re.search(r"Dist[.:\s]+([A-Za-z][A-Za-z\s]+?)(?:\s*$|\s*\d|,)", addr, re.I)
        city = (dist_m.group(1).strip() if dist_m else "") or state

        records.append({
            "name": name,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": actual_state,
            "source": SOURCE,
        })

        if (i + 1) % 25 == 0:
            print(f"  [GGL] Geocoded {i+1}/{len(entries)} entries → {len(records)} with coords")

    print(f"[GGL] Total: {len(records)} stations geocoded")
    save_raw(SOURCE, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
