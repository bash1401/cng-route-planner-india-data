#!/usr/bin/env python3
"""
Extra OSM data using targeted city/operator-specific Overpass queries.

The main OSM query misses stations that lack standard tags but ARE present in
specific city areas for known operators. This script:
  1. Queries by area: fetches ALL fuel stations in high-CNG cities and filters
     for those with any CNG indicator in name, operator, or service tags.
  2. Queries for petrol stations that have CNG as a secondary service
     (tagged under shop, services, or other tag variants).

Saves to raw_sources/osm_extra.json
"""

from __future__ import annotations

import json
import time
from utils import (
    save_raw, http_post, state_from_coords, normalise_name,
    COMMON_HEADERS,
)

SOURCE = "osm_extra"

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

INDIA_BBOX = "6.5,68.0,37.5,97.5"
_AREA = 'area["name"="India"]["admin_level"="2"]->.india;'

# All CNG-related tag key combinations used by mappers in India
_EXTRA_QUERIES = [
    # CNG as a dispensed fuel at any fuel station
    f"""[out:json][timeout:120];
    {_AREA}
    (
      node["amenity"="fuel"]["fuel:CNG"="yes"](area.india);
      way["amenity"="fuel"]["fuel:CNG"="yes"](area.india);
      node["amenity"="fuel"]["cng"="yes"](area.india);
      node["amenity"="fuel"]["natural_gas"="yes"](area.india);
      node["amenity"="fuel"]["fuel:natural_gas"="yes"](area.india);
      node["amenity"="fuel"]["gas:cng"="yes"](area.india);
    );out center tags;""",

    # Stations tagged as gas_station (alternate tag)
    f"""[out:json][timeout:120];
    {_AREA}
    (
      node["amenity"="gas_station"]["fuel:cng"="yes"](area.india);
      node["amenity"="gas_station"]["name"~"CNG",i](area.india);
      node["shop"="gas"]["fuel:cng"="yes"](area.india);
    );out center tags;""",

    # Stations tagged by minor CGD operators often missed
    f"""[out:json][timeout:120];
    {_AREA}
    (
      node["amenity"="fuel"]["operator"~"Think Gas|Siti Energy|Siti Networks|AG&P|Sagarmala|Har-Har Gas|Unique Gas|Goa Natural Gas|Vadodara Gas|Rajkot Gas|MNGL|CUGL|Green Gas|Charotar|Bhagyanagar|Tripura|Assam Gas|Megha|Indian Gas|IndianOil|HPCL|BPCL",i](area.india);
      way["amenity"="fuel"]["operator"~"Think Gas|Siti Energy|Siti Networks|AG&P|Sagarmala|Har-Har Gas|Unique Gas|Goa Natural Gas|Vadodara Gas|Rajkot Gas|MNGL|CUGL|Green Gas|Charotar|Bhagyanagar|Tripura|Assam Gas|Megha|Indian Gas",i](area.india);
    );out center tags;""",

    # Stations tagged with ref= containing CNG operator references
    f"""[out:json][timeout:120];
    {_AREA}
    (
      node["amenity"="fuel"]["ref"~"CNG|IGL|MGL|GAIL|ATGL",i](area.india);
      node["fuel"="cng"](area.india);
      node["fuel_type"="CNG"](area.india);
      node["fuel_type"~"CNG",i](area.india);
    );out center tags;""",
]

_CNG_INDICATORS = [
    "cng", "compressed natural gas", "igl", "mgl", "gail gas", "atgl",
    "gujarat gas", "torrent gas", "sabarmati", "mahanagar", "indraprastha",
    "mngl", "think gas", "ag&p", "green gas", "cugl", "charotar",
]


def _has_cng_indicator(tags: dict) -> bool:
    """Return True if any tag indicates a CNG filling station."""
    check_keys = (
        "name", "operator", "brand", "network", "ref", "description",
        "fuel:cng", "fuel:CNG", "cng", "natural_gas",
        "fuel:natural_gas", "fuel:compressed_natural_gas",
    )
    for key in check_keys:
        val = str(tags.get(key, "")).lower()
        if not val:
            continue
        if val in ("yes", "true", "1"):
            return True
        for indicator in _CNG_INDICATORS:
            if indicator in val:
                return True
    return False


def _fetch_query(query: str) -> list[dict]:
    payload = query.encode("utf-8")
    for endpoint in OVERPASS_ENDPOINTS:
        raw = http_post(endpoint, payload, timeout=180, retries=2)
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
                return data.get("elements", [])
            except json.JSONDecodeError:
                pass
        time.sleep(2)
    return []


def _normalise(elements: list[dict], seen: set[str]) -> list[dict]:
    records = []
    for elem in elements:
        tags = elem.get("tags") or {}
        if not _has_cng_indicator(tags):
            continue

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
        operator = tags.get("operator") or tags.get("brand") or ""
        if not raw_name:
            raw_name = f"{operator} CNG Station" if operator else "CNG Station"

        city = (
            tags.get("addr:city") or tags.get("is_in:city")
            or tags.get("addr:suburb") or tags.get("addr:town") or ""
        ).strip()
        state = (tags.get("addr:state") or "").strip() or state_from_coords(lat, lon)

        records.append({
            "id": f"osm-{osm_id}",
            "name": normalise_name(raw_name),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": state,
            "source": SOURCE,
            "address": "",
            "operator": operator,
        })
    return records


def main() -> int:
    print("=== Fetching Extra OSM (targeted queries) ===")
    all_records: list[dict] = []
    seen_ids: set[str] = set()

    for i, query in enumerate(_EXTRA_QUERIES, 1):
        print(f"  Running extra query {i}/{len(_EXTRA_QUERIES)} …")
        elements = _fetch_query(query)
        print(f"  Raw elements: {len(elements)}")
        records = _normalise(elements, seen_ids)
        all_records.extend(records)
        print(f"  New stations: {len(records)}")
        time.sleep(3)

    save_raw(SOURCE, all_records)
    print(f"  Total extra OSM: {len(all_records)} stations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
