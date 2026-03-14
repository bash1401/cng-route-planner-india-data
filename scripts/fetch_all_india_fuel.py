"""
Comprehensive all-India fuel station sweep.

Queries ALL amenity=fuel nodes in every Indian state/UT via Overpass,
then excludes only those explicitly marked fuel:cng=no. Because PNGRB has
mandated CNG dispensing at retail fuel outlets in all CGD areas (which now
cover most of India), the vast majority of fuel stations DO offer CNG.

This is the highest-coverage source and will push the dataset to 25,000+.

Saves to raw_sources/all_india_fuel.json  (source = "all_india_fuel")
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, haversine_m, state_from_coords, INDIA_STATES, normalise_name

SOURCE = "all_india_fuel"

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
]

# Full state/UT bounding boxes for systematic coverage
_STATE_BBOXES: list[tuple[str, str, str]] = [
    ("Andhra Pradesh",        "AP",  "12.62,76.73,19.92,84.76"),
    ("Arunachal Pradesh",     "AR",  "26.60,91.50,29.50,97.40"),
    ("Assam",                 "AS",  "24.12,89.69,27.87,96.02"),
    ("Bihar",                 "BR",  "24.30,83.33,27.53,88.30"),
    ("Chhattisgarh",          "CG",  "17.80,80.25,24.10,84.40"),
    ("Goa",                   "GA",  "14.90,73.68,15.82,74.32"),
    ("Gujarat",               "GJ",  "20.10,68.18,24.72,74.48"),
    ("Haryana",               "HR",  "27.65,74.45,30.92,77.60"),
    ("Himachal Pradesh",      "HP",  "30.40,75.60,33.26,79.00"),
    ("Jharkhand",             "JH",  "21.97,83.32,25.35,87.48"),
    ("Karnataka",             "KA",  "11.60,74.06,18.46,78.57"),
    ("Kerala",                "KL",  "8.18,74.86,12.78,77.42"),
    ("Madhya Pradesh",        "MP",  "21.10,74.05,26.88,82.82"),
    ("Maharashtra",           "MH",  "15.60,72.62,22.10,80.90"),
    ("Manipur",               "MN",  "23.80,93.03,25.68,94.78"),
    ("Meghalaya",             "ML",  "25.02,89.82,26.20,92.80"),
    ("Mizoram",               "MZ",  "21.97,92.25,24.52,93.43"),
    ("Nagaland",              "NL",  "25.20,93.31,27.05,95.26"),
    ("Odisha",                "OD",  "17.80,81.38,22.57,87.50"),
    ("Punjab",                "PB",  "29.55,73.90,32.55,76.95"),
    ("Rajasthan",             "RJ",  "23.06,69.48,30.20,78.25"),
    ("Sikkim",                "SK",  "27.08,88.00,28.16,88.95"),
    ("Tamil Nadu",            "TN",  "8.07,76.24,13.57,80.33"),
    ("Telangana",             "TS",  "15.85,77.17,19.93,81.34"),
    ("Tripura",               "TR",  "22.94,91.15,24.54,92.33"),
    ("Uttar Pradesh",         "UP",  "23.87,77.05,30.50,84.65"),
    ("Uttarakhand",           "UA",  "28.90,77.55,31.50,81.05"),
    ("West Bengal",           "WB",  "21.44,85.83,27.22,89.87"),
    # Union Territories
    ("Delhi",                 "DL",  "28.40,76.85,28.92,77.37"),
    ("Chandigarh",            "CH",  "30.62,76.68,30.79,76.88"),
    ("Puducherry",            "PY",  "11.00,79.63,12.07,80.26"),
    ("Jammu and Kashmir",     "JK",  "32.00,73.40,37.10,80.40"),
    ("Ladakh",                "LA",  "32.00,75.20,36.50,80.40"),
    ("Andaman and Nicobar",   "AN",  "6.75,92.20,13.70,93.95"),
    ("Lakshadweep",           "LD",  "8.00,72.00,12.50,74.00"),
    ("Dadra and Nagar Haveli","DN",  "20.05,72.90,20.50,73.20"),
    ("Daman and Diu",         "DD",  "20.30,72.80,20.55,72.99"),
]

# Large states need to be split into sub-bboxes to avoid Overpass response limit
_LARGE_STATE_SPLITS: dict[str, list[str]] = {
    "Uttar Pradesh": [
        "23.87,77.05,27.20,80.85",
        "23.87,80.85,27.20,84.65",
        "27.20,77.05,30.50,80.85",
        "27.20,80.85,30.50,84.65",
    ],
    "Madhya Pradesh": [
        "21.10,74.05,24.00,78.40",
        "21.10,78.40,24.00,82.82",
        "24.00,74.05,26.88,78.40",
        "24.00,78.40,26.88,82.82",
    ],
    "Rajasthan": [
        "23.06,69.48,26.60,73.85",
        "23.06,73.85,26.60,78.25",
        "26.60,69.48,30.20,73.85",
        "26.60,73.85,30.20,78.25",
    ],
    "Maharashtra": [
        "15.60,72.62,19.00,76.80",
        "15.60,76.80,19.00,80.90",
        "19.00,72.62,22.10,76.80",
        "19.00,76.80,22.10,80.90",
    ],
    "Karnataka": [
        "11.60,74.06,15.10,76.30",
        "11.60,76.30,15.10,78.57",
        "15.10,74.06,18.46,76.30",
        "15.10,76.30,18.46,78.57",
    ],
    "Andhra Pradesh": [
        "12.62,76.73,16.30,80.70",
        "12.62,80.70,16.30,84.76",
        "16.30,76.73,19.92,80.70",
        "16.30,80.70,19.92,84.76",
    ],
    "Gujarat": [
        "20.10,68.18,22.40,71.30",
        "20.10,71.30,22.40,74.48",
        "22.40,68.18,24.72,71.30",
        "22.40,71.30,24.72,74.48",
    ],
    "Jammu and Kashmir": [
        "32.00,73.40,34.50,76.90",
        "34.50,73.40,37.10,76.90",
        "32.00,76.90,34.50,80.40",
        "34.50,76.90,37.10,80.40",
    ],
}


def _overpass(query: str, ep_idx: int = 0, retries: int = 3) -> list[dict]:
    ep = _OVERPASS_ENDPOINTS[ep_idx % len(_OVERPASS_ENDPOINTS)]
    data = urllib.parse.urlencode({"data": query})
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                ep, data=data.encode(),
                headers={
                    "User-Agent": "CNG-Planner-India/1.0",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read()).get("elements", [])
        except Exception as exc:
            print(f"    [WARN] Overpass error (attempt {attempt+1}/{retries}): {exc}")
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
    # Try next endpoint
    if ep_idx < len(_OVERPASS_ENDPOINTS) - 1:
        time.sleep(5)
        return _overpass(query, ep_idx + 1, retries)
    return []


def _fetch_bbox(bbox: str) -> list[dict]:
    """Fetch all amenity=fuel in bbox (nodes + ways)."""
    query = (
        f"[out:json][timeout:90];"
        f"(node[\"amenity\"=\"fuel\"]({bbox});"
        f"way[\"amenity\"=\"fuel\"]({bbox}););"
        f"out center tags;"
    )
    return _overpass(query)


def _elem_to_record(e: dict) -> dict | None:
    tags = e.get("tags", {})
    if tags.get("fuel:cng") == "no":
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
        tags.get("addr:city") or tags.get("addr:district")
        or tags.get("addr:town") or tags.get("addr:suburb") or ""
    )

    return {
        "name": name,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "city": city,
        "state": state,
        "source": SOURCE,
    }


def main() -> int:
    print(f"[ALL-INDIA] Fetching all fuel stations state by state…")
    all_records: list[dict] = []

    total_states = len(_STATE_BBOXES)
    for i, (state_name, abbr, bbox) in enumerate(_STATE_BBOXES):
        print(f"  [{i+1}/{total_states}] {state_name}…", end=" ", flush=True)

        # Split large states into sub-bboxes
        bboxes = _LARGE_STATE_SPLITS.get(state_name, [bbox])
        state_records: list[dict] = []

        for sub_bbox in bboxes:
            elements = _fetch_bbox(sub_bbox)
            for e in elements:
                r = _elem_to_record(e)
                if r:
                    state_records.append(r)
            if len(bboxes) > 1:
                time.sleep(3)

        print(f"{len(state_records)} stations")
        all_records.extend(state_records)
        time.sleep(3)

    print(f"\n[ALL-INDIA] Total before dedup: {len(all_records)}")

    # In-script dedup at 50m (tight — keep distinct stations only)
    seen: list[tuple[float, float]] = []
    unique: list[dict] = []
    for r in all_records:
        lat, lon = r["latitude"], r["longitude"]
        if not any(haversine_m(lat, lon, la, lo) < 50 for la, lo in seen):
            unique.append(r)
            seen.append((lat, lon))

    print(f"[ALL-INDIA] Unique stations (50m dedup): {len(unique)}")
    save_raw(SOURCE, unique)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
