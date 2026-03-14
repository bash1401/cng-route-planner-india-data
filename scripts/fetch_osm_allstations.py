"""
Comprehensive OSM fuel station sweep.

Strategy:
  1. Query ALL amenity=fuel nodes in India's bounding box (fast, no filter).
  2. Apply a broad CNG indicator filter client-side to catch stations that have
     CNG in their name/operator/brand but are NOT tagged fuel:cng=yes.
  3. Query ways state-by-state to avoid timeout on the large bbox.
  4. Also do targeted operator-name queries for major CGD companies.

This supplements fetch_osm_cng.py which only queries explicitly tagged stations.
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
from utils import save_raw, haversine_m, normalise_name, state_from_coords, INDIA_STATES

SOURCE = "osm_allstations"

# ── CNG indicators ─────────────────────────────────────────────────────────────
_CNG_KEYWORDS = [
    "cng", "compressed natural gas", "city gas",
    "igl", "indraprastha gas", "indraprastha gas limited",
    "mgl", "mahanagar gas", "mahanagar gas limited",
    "gail gas", "gail gas limited",
    "gujarat gas", "ggl",
    "adani total gas", "atgl",
    "haryana city gas", "hcg",
    "green gas", "ggsl",
    "assam gas", "tripura gas",
    "matrix gas", "mngl", "megha gas",
    "central up gas", "gail india cng",
    "think gas", "siti energy", "avantika gas",
    "unison enviro", "bhagyanagar gas",
    "rajkot gas", "agni gas",
    "natural gas station", "cng pump", "cng filling",
    "cng fuel", "cng refueling", "cng dispensing",
    "torrent gas", "sabarmati gas", "charotar gas",
    "mahesh gas", "ag&p", "vadodara gas",
]

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]

_INDIA_BBOX = "6.5,68.0,37.5,97.5"

_STATE_BBOXES: list[tuple[str, str]] = [
    ("Delhi",           "28.40,76.85,28.92,77.37"),
    ("Haryana",         "27.65,74.45,30.92,77.60"),
    ("Punjab",          "29.55,73.90,32.55,76.95"),
    ("Chandigarh",      "30.62,76.68,30.79,76.88"),
    ("Uttar Pradesh",   "23.87,77.05,30.50,84.65"),
    ("Uttarakhand",     "28.90,77.55,31.50,81.05"),
    ("Rajasthan",       "23.06,69.48,30.20,78.25"),
    ("Gujarat",         "20.10,68.18,24.72,74.48"),
    ("Maharashtra",     "15.60,72.62,22.10,80.90"),
    ("Goa",             "14.90,73.68,15.82,74.32"),
    ("Madhya Pradesh",  "21.10,74.05,26.88,82.82"),
    ("Chhattisgarh",    "17.80,80.25,24.10,84.40"),
    ("Karnataka",       "11.60,74.06,18.46,78.57"),
    ("Tamil Nadu",      "8.07,76.24,13.57,80.33"),
    ("Telangana",       "15.85,77.17,19.93,81.34"),
    ("Andhra Pradesh",  "12.62,76.73,19.92,84.76"),
    ("Kerala",          "8.18,74.86,12.78,77.42"),
    ("West Bengal",     "21.44,85.83,27.22,89.87"),
    ("Bihar",           "24.30,83.33,27.53,88.30"),
    ("Jharkhand",       "21.97,83.32,25.35,87.48"),
    ("Odisha",          "17.80,81.38,22.57,87.50"),
    ("Assam",           "24.12,89.69,27.87,96.02"),
    ("Himachal Pradesh","30.40,75.60,33.26,79.00"),
    ("Jammu and Kashmir","32.00,73.40,37.10,80.40"),
]


def _is_cng(tags: dict) -> bool:
    if tags.get("fuel:cng") == "yes" or tags.get("fuel:CNG") == "yes":
        return True
    if tags.get("compressed_natural_gas") == "yes":
        return True
    combined = " ".join(str(v) for v in tags.values()).lower()
    return any(kw in combined for kw in _CNG_KEYWORDS)


def _overpass(query: str, ep_idx: int = 0) -> list[dict]:
    ep = _OVERPASS_ENDPOINTS[ep_idx % len(_OVERPASS_ENDPOINTS)]
    data = urllib.parse.urlencode({"data": query})
    try:
        req = urllib.request.Request(
            ep, data=data.encode(),
            headers={"User-Agent": "CNG-Planner-India/1.0",
                     "Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read()).get("elements", [])
    except Exception as exc:
        print(f"  [OSM-ALL] Overpass error ({ep}): {exc}")
        if ep_idx < len(_OVERPASS_ENDPOINTS) - 1:
            time.sleep(5)
            return _overpass(query, ep_idx + 1)
        return []


def _element_to_record(e: dict, require_cng_filter: bool = False) -> dict | None:
    tags = e.get("tags", {})
    # For targeted queries (fuel:cng=yes + name~CNG + operator filter), skip extra filter
    # For broad "all fuel" queries, apply the CNG keyword filter
    if require_cng_filter and not _is_cng(tags):
        return None

    lat = e.get("lat") or (e.get("center") or {}).get("lat")
    lon = e.get("lon") or (e.get("center") or {}).get("lon")
    if not lat or not lon:
        return None
    lat, lon = float(lat), float(lon)

    if not (6.5 <= lat <= 37.5 and 68.0 <= lon <= 97.5):
        return None

    state = state_from_coords(lat, lon)
    if not state or state not in INDIA_STATES:
        return None

    name = normalise_name(
        tags.get("name") or tags.get("operator") or tags.get("brand") or ""
    ) or "CNG Station"

    city = (
        tags.get("addr:city")
        or tags.get("addr:district")
        or tags.get("addr:town")
        or tags.get("addr:suburb")
        or ""
    )

    return {
        "name": name,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "city": city,
        "state": state,
        "source": SOURCE,
    }


_NODE_QUADRANTS = [
    # Split India into 8 parts to avoid response-size timeout
    "6.5,68.0,22.0,82.75",   # SW (Gujarat, Maharashtra, Goa, Karnataka, Kerala, Tamil Nadu west)
    "6.5,82.75,22.0,97.5",   # SE (Tamil Nadu east, AP, Telangana, Odisha, Bengal south)
    "22.0,68.0,30.0,77.5",   # NW lower (Rajasthan, UP west, Delhi, Haryana)
    "22.0,77.5,30.0,85.0",   # NC lower (UP east, MP, Chhattisgarh, Jharkhand)
    "22.0,85.0,30.0,97.5",   # NE lower (Bengal, Bihar, Assam south)
    "30.0,68.0,37.5,79.0",   # NW upper (Punjab, HP, J&K, Ladakh west)
    "30.0,79.0,37.5,97.5",   # NE upper (Uttarakhand, UP north, Assam north, NE states)
]


_CGD_OPERATORS_PATTERN = (
    "IGL|Indraprastha Gas|MGL|Mahanagar Gas|GAIL Gas|GAIL"
    "|Gujarat Gas|GGL|Adani Total Gas|ATGL|Adani Gas"
    "|Haryana City Gas|HCG|Green Gas|Central UP Gas|CUGL"
    "|Think Gas|Avantika Gas|Maharashtra Natural Gas|MNGL"
    "|Bhagyanagar Gas|Torrent Gas|Sabarmati Gas|Charotar Gas"
    "|AG.P|AG&P|Pratham|Tripura Natural Gas|TNGCL|Assam Gas"
    "|Vadodara Gas|Rajkot Gas|Siti Energy|Unison Enviro"
    "|Matrix Gas|MEGHA GAS|Unique Gas|Goa Natural Gas"
)


_INDIA_BBOX = "6.5,68.0,37.5,97.5"

# Shorter operator list for faster regex matching
_KEY_OPERATORS = "IGL|MGL|GAIL Gas|Gujarat Gas|ATGL|Adani Gas|MNGL|Green Gas|Torrent Gas|Sabarmati Gas"


def fetch_nodes() -> list[dict]:
    """
    Fetch CNG-likely nodes using targeted queries.
    Uses whole-India bbox to avoid missing stations at quadrant boundaries.
    """
    print("[OSM-ALL] Fetching CNG fuel nodes in India…")
    all_records: list[dict] = []

    # Query 1: All fuel:cng=yes (fastest, most reliable)
    print("  [OSM-ALL] Query 1: fuel:cng=yes all India…")
    q1 = f"""[out:json][timeout:120];
(
  node["fuel:cng"="yes"]({_INDIA_BBOX});
  way["fuel:cng"="yes"]({_INDIA_BBOX});
  node["compressed_natural_gas"="yes"]({_INDIA_BBOX});
);
out center tags;"""
    elems = _overpass(q1)
    r1 = [r for e in elems if (r := _element_to_record(e))]
    print(f"    → {len(elems)} elements, {len(r1)} valid")
    all_records.extend(r1)
    time.sleep(3)

    # Query 2: CNG in name
    print("  [OSM-ALL] Query 2: name~CNG all India…")
    q2 = f"""[out:json][timeout:120];
(
  node["amenity"="fuel"]["name"~"CNG",i]({_INDIA_BBOX});
  way["amenity"="fuel"]["name"~"CNG",i]({_INDIA_BBOX});
  node["amenity"="fuel"]["name"~"compressed natural gas",i]({_INDIA_BBOX});
);
out center tags;"""
    elems = _overpass(q2)
    r2 = [r for e in elems if (r := _element_to_record(e))]
    print(f"    → {len(elems)} elements, {len(r2)} valid")
    all_records.extend(r2)
    time.sleep(3)

    # Query 3: Known CGD operators
    print("  [OSM-ALL] Query 3: key CGD operators all India…")
    q3 = f"""[out:json][timeout:120];
(
  node["amenity"="fuel"]["operator"~"{_KEY_OPERATORS}",i]({_INDIA_BBOX});
  way["amenity"="fuel"]["operator"~"{_KEY_OPERATORS}",i]({_INDIA_BBOX});
  node["amenity"="fuel"]["brand"~"{_KEY_OPERATORS}",i]({_INDIA_BBOX});
  way["amenity"="fuel"]["brand"~"{_KEY_OPERATORS}",i]({_INDIA_BBOX});
);
out center tags;"""
    elems = _overpass(q3)
    r3 = [r for e in elems if (r := _element_to_record(e))]
    print(f"    → {len(elems)} elements, {len(r3)} valid")
    all_records.extend(r3)
    time.sleep(3)

    print(f"[OSM-ALL] Total targeted nodes: {len(all_records)}")
    return all_records


def fetch_ways_by_state() -> list[dict]:
    """Query fuel ways state-by-state to avoid timeout."""
    all_records: list[dict] = []
    for i, (state_name, bbox) in enumerate(_STATE_BBOXES):
        print(f"[OSM-ALL] Ways: {state_name} ({i+1}/{len(_STATE_BBOXES)})…")
        query = (
            f"[out:json][timeout:60];"
            f"way[\"amenity\"=\"fuel\"]({bbox});"
            f"out center tags;"
        )
        elements = _overpass(query)
        state_records = [r for e in elements if (r := _element_to_record(e, require_cng_filter=True))]
        print(f"  → {len(state_records)} CNG ways in {state_name}")
        all_records.extend(state_records)
        time.sleep(2)
    return all_records


def fetch_operator_targeted() -> list[dict]:
    """Targeted queries by operator name for major CGD companies."""
    print("[OSM-ALL] Running operator-targeted queries…")
    op_patterns = (
        "IGL|Indraprastha Gas|MGL|Mahanagar Gas|GAIL Gas"
        "|Gujarat Gas|Adani Total Gas|ATGL|Haryana City Gas|HCG"
        "|Green Gas|Think Gas|Avantika Gas|Bhagyanagar Gas|Assam Gas"
        "|Central U.P. Gas|CUGL|Torrent Gas|Sabarmati Gas"
        "|Charotar Gas|Siti Energy|Rajkot Gas|Agni Gas|Vadodara Gas"
        "|AG&P|Maharashtra Natural Gas|MNGL|Tripura Natural Gas|TNGCL"
    )
    query = f"""[out:json][timeout:120];
(
  node["amenity"="fuel"]["operator"~"{op_patterns}",i]({_INDIA_BBOX});
  way["amenity"="fuel"]["operator"~"{op_patterns}",i]({_INDIA_BBOX});
  node["amenity"="fuel"]["brand"~"{op_patterns}",i]({_INDIA_BBOX});
  way["amenity"="fuel"]["brand"~"{op_patterns}",i]({_INDIA_BBOX});
);
out center tags;"""
    elements = _overpass(query)
    records: list[dict] = []
    for e in elements:
        tags = e.get("tags", {})
        lat = e.get("lat") or (e.get("center") or {}).get("lat")
        lon = e.get("lon") or (e.get("center") or {}).get("lon")
        if not lat or not lon:
            continue
        lat, lon = float(lat), float(lon)
        if not (6.5 <= lat <= 37.5 and 68.0 <= lon <= 97.5):
            continue
        state = state_from_coords(lat, lon)
        if not state or state not in INDIA_STATES:
            continue
        name = normalise_name(tags.get("name") or tags.get("operator") or "") or "CNG Station"
        city = tags.get("addr:city") or tags.get("addr:town") or ""
        records.append({"name": name, "latitude": round(lat, 6), "longitude": round(lon, 6),
                        "city": city, "state": state, "source": SOURCE})
    print(f"[OSM-ALL] Operator-targeted: {len(records)} stations")
    return records


def main() -> int:
    # Primary: Use targeted fetch_nodes (3 clean queries)
    nodes = fetch_nodes()
    time.sleep(3)

    # Secondary: fetch_operator_targeted for additional CGD operators
    targeted = fetch_operator_targeted()

    combined = nodes + targeted

    # Dedup within 100m
    seen: list[tuple[float, float]] = []
    unique: list[dict] = []
    for r in combined:
        lat, lon = r["latitude"], r["longitude"]
        if not any(haversine_m(lat, lon, la, lo) < 100 for la, lo in seen):
            unique.append(r)
            seen.append((lat, lon))

    print(f"[OSM-ALL] Final unique: {len(unique)}")
    save_raw(SOURCE, unique)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
