#!/usr/bin/env python3
"""
Source 7 — Wikidata SPARQL.

Queries Wikidata for fuel stations in India that are tagged as CNG or
operated by known CGD companies. Provides an additional 200–500 stations
with coordinates.
"""

from __future__ import annotations

import json
import time
from utils import save_raw, http_get, state_from_coords, normalise_name

SOURCE = "wikidata"

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Query 1: Direct instance of filling station in India
SPARQL_CNG = """
SELECT DISTINCT ?item ?itemLabel ?lat ?lon WHERE {
  ?item wdt:P31 wd:Q44922 .
  ?item wdt:P17 wd:Q668 .
  ?item wdt:P625 ?coord .
  BIND(geof:latitude(?coord) AS ?lat)
  BIND(geof:longitude(?coord) AS ?lon)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 2000
"""

# Query 2: Stations operated by known Indian CGD companies
SPARQL_OPERATORS = """
SELECT DISTINCT ?item ?itemLabel ?lat ?lon WHERE {
  ?item wdt:P17 wd:Q668 .
  ?item wdt:P625 ?coord .
  BIND(geof:latitude(?coord) AS ?lat)
  BIND(geof:longitude(?coord) AS ?lon)
  { ?item wdt:P137 wd:Q6026680 . }  UNION  # IGL
  { ?item wdt:P137 wd:Q6762556 . }  UNION  # MGL
  { ?item wdt:P137 wd:Q7872843 . }         # Gujarat Gas
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 2000
"""

_OPERATOR_QIDS = {
    "Q6026680": "IGL",
    "Q6762556": "MGL",
    "Q918225": "GAIL",
    "Q7872843": "Gujarat Gas",
    "Q59614028": "ATGL",
}


def _sparql_query(query: str) -> list[dict]:
    import urllib.parse
    params = urllib.parse.urlencode({
        "query": query,
        "format": "json",
    })
    url = f"{SPARQL_ENDPOINT}?{params}"
    raw = http_get(
        url, timeout=30, retries=3,
        extra_headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": "CNG-Route-Planner-India/2.0 (github.com/bash1401/cng-route-planner-india-data)",
        },
    )
    if not raw:
        return []
    try:
        data = json.loads(raw.decode("utf-8"))
        return data.get("results", {}).get("bindings", [])
    except (json.JSONDecodeError, KeyError):
        return []


def _bindings_to_records(bindings: list[dict], source_tag: str) -> list[dict]:
    records = []
    seen: set[str] = set()

    for b in bindings:
        try:
            lat = float(b["lat"]["value"])
            lon = float(b["lon"]["value"])
        except (KeyError, ValueError, TypeError):
            continue
        if not (6 < lat < 38 and 68 < lon < 98):
            continue
        key = f"{round(lat,3)}-{round(lon,3)}"
        if key in seen:
            continue
        seen.add(key)

        name = b.get("itemLabel", {}).get("value") or "CNG Station"
        city = b.get("cityLabel", {}).get("value") or ""
        state = b.get("stateLabel", {}).get("value") or state_from_coords(lat, lon)
        item_id = b.get("item", {}).get("value", "").split("/")[-1]

        records.append({
            "id": f"wd-{item_id}",
            "name": normalise_name(name),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": state,
            "source": source_tag,
            "address": "",
            "operator": "",
        })

    return records


def main() -> int:
    print("=== Fetching Wikidata CNG stations ===")
    all_records: list[dict] = []
    seen_ids: set[str] = set()

    # Query 1: CNG-tagged filling stations
    print("  Query 1: Filling stations tagged CNG …")
    bindings = _sparql_query(SPARQL_CNG)
    print(f"  Got {len(bindings)} results")
    records = _bindings_to_records(bindings, SOURCE)
    for r in records:
        if r["id"] not in seen_ids:
            all_records.append(r)
            seen_ids.add(r["id"])

    time.sleep(5)  # Be respectful of Wikidata's rate limits

    # Query 2: Operator-based search
    print("  Query 2: Stations operated by Indian CGD companies …")
    bindings2 = _sparql_query(SPARQL_OPERATORS)
    print(f"  Got {len(bindings2)} results")
    records2 = _bindings_to_records(bindings2, SOURCE)
    for r in records2:
        if r["id"] not in seen_ids:
            all_records.append(r)
            seen_ids.add(r["id"])

    print(f"  Total Wikidata: {len(all_records)} stations")
    save_raw(SOURCE, all_records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
