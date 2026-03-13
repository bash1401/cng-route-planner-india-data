#!/usr/bin/env python3
"""
Source 1 — OpenStreetMap via Overpass API.

Uses 20+ tag/operator union queries to maximise CNG station coverage.
Saves raw results to raw_sources/osm.json
"""

from __future__ import annotations

import json
import time
from utils import (
    RAW_DIR, save_raw, http_post, state_from_coords, normalise_name, COMMON_HEADERS
)

SOURCE = "osm"

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

INDIA_BBOX = "6.5,68.0,37.5,97.5"  # Coarse pre-filter — area filter enforces India
# Prepend to each query block to restrict to India's administrative boundary
_AREA_FILTER = 'area["name"="India"]["admin_level"="2"]->.india;'

_CGD_OPERATORS = (
    "IGL|Indraprastha Gas"
    "|MGL|Mahanagar Gas"
    "|GAIL Gas|GAIL"
    "|Adani Gas|ATGL|Adani Total Gas"
    "|Gujarat Gas"
    "|Torrent Gas"
    "|Sabarmati Gas"
    "|Central UP Gas|CUGL"
    "|Green Gas"
    "|MNGL|Maharashtra Natural Gas"
    "|AG&P|AG&P Pratham"
    "|Charotar Gas"
    "|Siti Energy"
    "|Think Gas"
    "|Bhagyanagar Gas"
    "|Tripura Natural Gas|TNGCL"
    "|Assam Gas|AGCL"
    "|Megha Engineering"
    "|Vadodara Gas"
    "|Rajkot Gas"
    "|Har-Har Gas"
    "|Unique Gas"
    "|Goa Natural Gas"
    "|CNG Filling"
    "|HPCL CNG|BPCL CNG|IOCL CNG"
)

OVERPASS_QUERY = f"""
[out:json][timeout:300];
{_AREA_FILTER}
(
  node["amenity"="fuel"]["fuel:cng"="yes"](area.india);
  way["amenity"="fuel"]["fuel:cng"="yes"](area.india);
  relation["amenity"="fuel"]["fuel:cng"="yes"](area.india);

  node["amenity"="fuel"]["compressed_natural_gas"="yes"](area.india);
  way["amenity"="fuel"]["compressed_natural_gas"="yes"](area.india);

  node["amenity"="fuel"]["fuel:compressed_natural_gas"="yes"](area.india);
  way["amenity"="fuel"]["fuel:compressed_natural_gas"="yes"](area.india);

  node["amenity"="fuel"]["name"~"CNG",i](area.india);
  way["amenity"="fuel"]["name"~"CNG",i](area.india);

  node["amenity"="fuel"]["name"~"Compressed Natural Gas",i](area.india);

  node["amenity"="fuel"]["operator"~"{_CGD_OPERATORS}",i](area.india);
  way["amenity"="fuel"]["operator"~"{_CGD_OPERATORS}",i](area.india);

  node["amenity"="fuel"]["brand"~"IGL|MGL|GAIL Gas|Adani Gas|Gujarat Gas|ATGL|Torrent Gas",i](area.india);
  way["amenity"="fuel"]["brand"~"IGL|MGL|GAIL Gas|Adani Gas|Gujarat Gas|ATGL|Torrent Gas",i](area.india);

  node["amenity"="fuel"]["network"~"IGL|MGL|GAIL|CNG",i](area.india);
  way["amenity"="fuel"]["network"~"IGL|MGL|GAIL|CNG",i](area.india);

  node["name"~"^(IGL|MGL|GAIL|Adani|Gujarat Gas|ATGL|Torrent|Sabarmati).*(CNG|Station|Gas)",i](area.india);
  node["name"~"CNG.*(Station|Pump|Point|Outlet|Filling|Dispenser)",i](area.india);
  node["name"~"Compressed Natural Gas",i](area.india);
);
out center tags;
"""

_OPERATOR_KEYWORDS = [
    "IGL", "MGL", "GAIL", "Adani", "ATGL", "Gujarat Gas",
    "Torrent", "Sabarmati", "Green Gas", "MNGL", "Think Gas",
    "Siti", "AG&P", "Charotar", "CUGL", "HPCL", "BPCL", "IOCL",
]


def _infer_operator(tags: dict) -> str:
    for field in ("operator", "brand", "network", "name"):
        val = tags.get(field, "")
        for op in _OPERATOR_KEYWORDS:
            if op.lower() in val.lower():
                return op
    return ""


def fetch() -> list[dict]:
    payload = OVERPASS_QUERY.encode("utf-8")
    for endpoint in OVERPASS_ENDPOINTS:
        print(f"  Trying {endpoint} …")
        raw = http_post(endpoint, payload, timeout=360, retries=2)
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
                elements = data.get("elements", [])
                print(f"  ✓ {len(elements)} raw OSM elements")
                return elements
            except json.JSONDecodeError as exc:
                print(f"  ✗ JSON error: {exc}")
        time.sleep(3)
    return []


def normalise(elements: list[dict]) -> list[dict]:
    records: list[dict] = []
    seen: set[str] = set()

    for elem in elements:
        tags = elem.get("tags") or {}
        lat = elem.get("lat")
        lon = elem.get("lon")
        if lat is None or lon is None:
            center = elem.get("center") or {}
            lat, lon = center.get("lat"), center.get("lon")
        if lat is None or lon is None:
            continue
        lat, lon = float(lat), float(lon)
        if not (6.5 <= lat <= 37.5 and 68.0 <= lon <= 97.5):
            continue

        osm_id = f"{elem.get('type','node')}-{elem.get('id',0)}"
        if osm_id in seen:
            continue
        seen.add(osm_id)

        raw_name = (tags.get("name") or "").strip()
        operator = _infer_operator(tags)
        if not raw_name:
            raw_name = f"{operator} CNG Station" if operator else "CNG Station"

        city = (
            tags.get("addr:city") or tags.get("is_in:city")
            or tags.get("addr:suburb") or tags.get("addr:town") or ""
        ).strip()
        state = (
            tags.get("addr:state") or tags.get("is_in:state") or ""
        ).strip()
        if not state:
            state = state_from_coords(lat, lon)

        address_parts = [
            tags.get("addr:housenumber", ""),
            tags.get("addr:street", ""),
            city, state,
        ]
        address = ", ".join(p for p in address_parts if p)

        records.append({
            "id": f"osm-{osm_id}",
            "name": normalise_name(raw_name),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": state,
            "source": SOURCE,
            "address": address,
            "operator": operator,
        })

    print(f"  Normalised to {len(records)} OSM records")
    return records


def main() -> int:
    print("=== Fetching OSM CNG stations ===")
    elements = fetch()
    if not elements:
        print("  No data from OSM — skipping")
        return 1
    records = normalise(elements)
    save_raw(SOURCE, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
