"""
Wikidata SPARQL — CNG / fuel stations in India.

Fixes from v1:
  - Correct WKT Point parsing (lon lat order, not lat lon).
  - geof: prefix was not recognised; replaced with coordinate string parsing.
  - Broader queries: filling stations + gas stations + operator-specific items.
  - Added a third query for items whose name / description contains "CNG".
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, http_get, state_from_coords, normalise_name, INDIA_STATES

SOURCE = "wikidata"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_DELAY = 10  # seconds between SPARQL requests to respect rate limit


def _sparql(query: str) -> list[dict]:
    params = urllib.parse.urlencode({"query": query.strip(), "format": "json"})
    url = f"{SPARQL_ENDPOINT}?{params}"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "CNG-Route-Planner-India/2.0 (https://github.com/bash1401/cng-route-planner-india-data)",
                "Accept": "application/sparql-results+json",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
            return data.get("results", {}).get("bindings", [])
    except Exception as exc:
        print(f"  [WD] SPARQL error: {exc}")
        return []


def _parse_coord(binding: dict) -> tuple[float, float] | None:
    """Extract (lat, lon) from a binding that has a 'coord' WKT Point value."""
    val = binding.get("coord", {}).get("value", "")
    m = re.match(r"Point\((-?\d+\.?\d*)\s+(-?\d+\.?\d*)\)", val)
    if m:
        lon, lat = float(m.group(1)), float(m.group(2))
        return lat, lon
    # Some bindings already have lat/lon directly
    lat_val = binding.get("lat", {}).get("value")
    lon_val = binding.get("lon", {}).get("value")
    if lat_val and lon_val:
        try:
            return float(lat_val), float(lon_val)
        except ValueError:
            pass
    return None


# ── Query 1: All filling stations (Q44922) in India ──────────────────────────
Q1 = """
SELECT DISTINCT ?item ?itemLabel ?coord WHERE {
  ?item wdt:P31 wd:Q44922 ;
        wdt:P17 wd:Q668 ;
        wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,hi". }
}
LIMIT 2000
"""

# ── Query 2: CNG stations / compressed natural gas vehicles ──────────────────
Q2 = """
SELECT DISTINCT ?item ?itemLabel ?coord WHERE {
  ?item wdt:P17 wd:Q668 ;
        wdt:P625 ?coord .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "en")
  FILTER(CONTAINS(LCASE(?label), "cng") || CONTAINS(LCASE(?label), "compressed natural gas"))
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 2000
"""

# ── Query 3: Stations operated by major Indian CGD companies ─────────────────
# IGL=Q6026680  MGL=Q6762556  Gujarat Gas=Q7872843  GAIL=Q918225  ATGL=Q59614028
Q3 = """
SELECT DISTINCT ?item ?itemLabel ?coord WHERE {
  VALUES ?op { wd:Q6026680 wd:Q6762556 wd:Q7872843 wd:Q918225 wd:Q59614028 }
  ?item wdt:P17 wd:Q668 ;
        wdt:P625 ?coord .
  { ?item wdt:P137 ?op . } UNION { ?item wdt:P749 ?op . } UNION { ?item wdt:P1158 ?op . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 2000
"""

# ── Query 4: Gas stations (Q7391294) and service areas ───────────────────────
Q4 = """
SELECT DISTINCT ?item ?itemLabel ?coord WHERE {
  VALUES ?type { wd:Q7391294 wd:Q27551814 wd:Q2545735 }
  ?item wdt:P31 ?type ;
        wdt:P17 wd:Q668 ;
        wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,hi". }
}
LIMIT 2000
"""

_QUERIES = [
    ("Filling stations in India", Q1),
    ("Items labelled CNG/compressed natural gas", Q2),
    ("Stations operated by Indian CGD companies", Q3),
    ("Gas stations / service areas in India", Q4),
]


def main() -> int:
    print("=== Fetching Wikidata CNG stations ===")

    all_records: list[dict] = []
    seen: set[str] = set()

    for label, query in _QUERIES:
        print(f"  Query: {label}…")
        bindings = _sparql(query)
        print(f"  Got {len(bindings)} raw results")

        for b in bindings:
            item_uri = b.get("item", {}).get("value", "")
            item_id = item_uri.split("/")[-1]
            if item_id in seen:
                continue

            coords = _parse_coord(b)
            if not coords:
                continue

            lat, lon = coords
            if not (6.0 < lat < 38.0 and 68.0 < lon < 98.0):
                continue

            state = state_from_coords(lat, lon)
            if not state or state not in INDIA_STATES:
                continue

            name = normalise_name(b.get("itemLabel", {}).get("value", "") or "CNG Station")
            seen.add(item_id)
            all_records.append({
                "id": f"wd-{item_id}",
                "name": name,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": "",
                "state": state,
                "source": SOURCE,
                "address": "",
                "operator": "",
            })

        print(f"  Running total: {len(all_records)} unique stations")
        time.sleep(_DELAY)

    print(f"=== Wikidata total: {len(all_records)} ===")
    save_raw(SOURCE, all_records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
