"""
Adani Total Gas Limited (ATGL) CNG stations — improved scraper.
ATGL operates 700+ CNG stations across 15+ Indian states.
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

SOURCE = "atgl"

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

# ATGL key operating zones
_ATGL_BBOXES: list[tuple[str, str]] = [
    ("Ahmedabad",         "22.90,72.40,23.20,72.75"),
    ("Vadodara",          "22.20,73.00,22.45,73.30"),
    ("Gandhinagar",       "23.10,72.55,23.35,72.85"),
    ("Surat",             "21.10,72.75,21.35,73.05"),
    ("Rajkot",            "22.15,70.65,22.40,70.90"),
    ("Bhavnagar",         "21.65,72.00,21.90,72.30"),
    ("Kanpur",            "26.30,80.20,26.60,80.55"),
    ("Lucknow",           "26.70,80.80,27.00,81.10"),
    ("Agra",              "27.05,77.85,27.30,78.15"),
    ("Varanasi",          "25.20,82.85,25.45,83.15"),
    ("Durgapur/Asansol",  "23.40,87.10,23.70,87.40"),
    ("Nagpur",            "21.00,78.90,21.30,79.20"),
    ("Bhopal",            "23.10,77.30,23.40,77.60"),
    ("Indore",            "22.60,75.75,22.85,76.05"),
    ("Chandigarh",        "30.60,76.65,30.85,76.95"),
    ("Faridabad",         "28.30,77.20,28.55,77.45"),
    ("Gurugram",          "28.35,76.95,28.60,77.20"),
    ("Pune",              "18.40,73.70,18.65,74.00"),
    ("Aurangabad",        "19.75,75.25,20.05,75.55"),
    ("Jaipur",            "26.75,75.65,27.05,76.00"),
    ("Jodhpur",           "26.20,72.90,26.50,73.20"),
]

_CNG_KEYWORDS = ["cng", "adani", "atgl", "adani total", "total gas", "natural gas",
                 "compressed natural gas", "cng station", "cng pump", "city gas"]


def _is_cng(tags: dict) -> bool:
    if tags.get("fuel:cng") == "yes":
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
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read()).get("elements", [])
    except Exception as exc:
        print(f"  [ATGL] Overpass error: {exc}")
        if ep_idx < len(_OVERPASS_ENDPOINTS) - 1:
            time.sleep(5)
            return _overpass(query, ep_idx + 1)
        return []


def _try_atgl_api() -> list[dict]:
    records: list[dict] = []
    for url in [
        "https://www.adanitotalgas.in/api/cng-stations",
        "https://atgl.adani.com/api/cng-stations",
        "https://api.adanitotalgas.in/cng/stations",
        "https://www.adanitotalgas.in/assets/data/cng-stations.json",
        "https://www.adanitotalgas.in/api/v1/cng-stations",
    ]:
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "ATGL/3.0 (Android)", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                ct = r.headers.get("content-type", "")
                body = r.read(100000)
                if "json" not in ct and not body.lstrip().startswith(b"[") and not body.lstrip().startswith(b"{"):
                    continue
                data = json.loads(body)
                items = data if isinstance(data, list) else (
                    data.get("data") or data.get("stations") or data.get("result") or []
                )
                for item in items:
                    lat = item.get("latitude") or item.get("lat")
                    lon = item.get("longitude") or item.get("lon") or item.get("lng")
                    if not lat or not lon:
                        continue
                    lat, lon = float(lat), float(lon)
                    state = state_from_coords(lat, lon) or ""
                    if state not in INDIA_STATES:
                        continue
                    name = normalise_name(item.get("name") or "ATGL CNG Station")
                    city = item.get("city") or ""
                    records.append({"name": name, "latitude": round(lat, 6), "longitude": round(lon, 6),
                                    "city": city, "state": state, "source": SOURCE})
                if records:
                    print(f"  [ATGL] API hit: {url} → {len(records)}")
                    break
        except Exception:
            pass
    return records


def _fetch_zones() -> list[dict]:
    all_records: list[dict] = []
    for zone_name, bbox in _ATGL_BBOXES:
        query = (
            f"[out:json][timeout:45];"
            f"(node[\"amenity\"=\"fuel\"]({bbox});"
            f"way[\"amenity\"=\"fuel\"]({bbox}););"
            f"out center tags;"
        )
        elements = _overpass(query)
        zone_records = []
        for e in elements:
            tags = e.get("tags", {})
            if not _is_cng(tags):
                continue
            lat = e.get("lat") or (e.get("center") or {}).get("lat")
            lon = e.get("lon") or (e.get("center") or {}).get("lon")
            if not lat or not lon:
                continue
            lat, lon = float(lat), float(lon)
            state = state_from_coords(lat, lon) or ""
            if state not in INDIA_STATES:
                continue
            name = normalise_name(tags.get("name") or "") or "ATGL CNG Station"
            city = tags.get("addr:city") or tags.get("addr:town") or zone_name
            zone_records.append({"name": name, "latitude": round(lat, 6), "longitude": round(lon, 6),
                                  "city": city, "state": state, "source": SOURCE})
        print(f"  [ATGL] Zone {zone_name}: {len(zone_records)}")
        all_records.extend(zone_records)
        time.sleep(2)
    return all_records


def main() -> int:
    print("[ATGL] Fetching Adani Total Gas stations…")

    api_records = _try_atgl_api()
    zone_records = _fetch_zones()

    combined = api_records + zone_records
    seen: list[tuple[float, float]] = []
    unique: list[dict] = []
    for r in combined:
        lat, lon = r["latitude"], r["longitude"]
        if not any(haversine_m(lat, lon, la, lo) < 150 for la, lo in seen):
            unique.append(r)
            seen.append((lat, lon))

    print(f"[ATGL] Total unique: {len(unique)}")
    save_raw(SOURCE, unique)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
