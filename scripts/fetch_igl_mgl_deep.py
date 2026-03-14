"""
Deep scraping for IGL (Delhi NCR) and MGL (Mumbai/Thane) CNG stations.

Strategy:
  1. Fine-grained Overpass queries within IGL/MGL operating zones.
  2. Query ALL fuel stations in those zones, keep CNG-indicating ones.
  3. Try to call any public JSON API the operator websites expose.
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

SOURCE = "igl_mgl"

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

# IGL covers: Delhi, Noida, Greater Noida, Ghaziabad, Gurugram, Faridabad, etc.
_IGL_BBOXES: list[tuple[str, str]] = [
    ("Delhi",              "28.40,76.85,28.92,77.37"),
    ("Noida/Ghaziabad",    "28.45,77.30,28.85,77.80"),
    ("Gurugram/Faridabad", "28.30,76.90,28.70,77.40"),
    ("Karnal/Panipat",     "29.20,76.80,30.00,77.20"),
    ("Meerut/Muzaffarnagar","28.85,77.60,29.30,78.10"),
    ("Ajmer",              "26.30,74.50,26.70,75.00"),
    ("Rewari",             "28.10,76.50,28.40,76.90"),
    ("Kaithal",            "29.60,76.20,30.00,76.70"),
]

# MGL covers: Mumbai, Thane, Raigad, Dhule
_MGL_BBOXES: list[tuple[str, str]] = [
    ("Mumbai",             "18.85,72.74,19.30,73.05"),
    ("Thane",              "19.10,72.90,19.40,73.20"),
    ("Navi Mumbai/Raigad", "18.60,73.00,19.10,73.30"),
    ("Dhule",              "20.70,74.60,21.10,75.00"),
    ("Pune",               "18.40,73.70,18.65,74.00"),
]

_CNG_KEYWORDS = [
    "cng", "compressed natural gas", "igl", "indraprastha gas",
    "mgl", "mahanagar gas", "natural gas", "city gas",
    "cng filling", "cng pump", "cng station", "gas station",
]


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
        print(f"  [IGL/MGL] Overpass error ({ep}): {exc}")
        if ep_idx < len(_OVERPASS_ENDPOINTS) - 1:
            time.sleep(5)
            return _overpass(query, ep_idx + 1)
        return []


def _elements_to_records(elements: list[dict], default_state: str) -> list[dict]:
    records = []
    for e in elements:
        tags = e.get("tags", {})
        if not _is_cng(tags):
            continue
        lat = e.get("lat") or (e.get("center") or {}).get("lat")
        lon = e.get("lon") or (e.get("center") or {}).get("lon")
        if not lat or not lon:
            continue
        lat, lon = float(lat), float(lon)
        state = state_from_coords(lat, lon) or default_state
        if state not in INDIA_STATES:
            continue
        name = normalise_name(tags.get("name") or tags.get("operator") or "") or "CNG Station"
        city = tags.get("addr:city") or tags.get("addr:town") or ""
        records.append({"name": name, "latitude": round(lat, 6), "longitude": round(lon, 6),
                        "city": city, "state": state, "source": SOURCE})
    return records


def _fetch_zone(bboxes: list[tuple[str, str]], default_state: str) -> list[dict]:
    all_records: list[dict] = []
    for zone_name, bbox in bboxes:
        print(f"  [IGL/MGL] Zone: {zone_name}…")
        query = (
            f"[out:json][timeout:60];"
            f"(node[\"amenity\"=\"fuel\"]({bbox});"
            f"way[\"amenity\"=\"fuel\"]({bbox}););"
            f"out center tags;"
        )
        elements = _overpass(query)
        zone_records = _elements_to_records(elements, default_state)
        print(f"    {len(zone_records)} CNG stations in {zone_name}")
        all_records.extend(zone_records)
        time.sleep(2)
    return all_records


def _try_operator_api(endpoints: list[tuple[str, str, dict | None]], name_prefix: str, default_state: str) -> list[dict]:
    """Try multiple API endpoint patterns for an operator."""
    records: list[dict] = []
    for method, url, payload in endpoints:
        try:
            if method == "POST" and payload is not None:
                data = json.dumps(payload).encode()
                req = urllib.request.Request(url, data=data, headers={
                    "User-Agent": f"{name_prefix}/7.0 (Android)",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                })
            else:
                req = urllib.request.Request(url, headers={
                    "User-Agent": f"{name_prefix}/7.0 (Android)",
                    "Accept": "application/json",
                })
            with urllib.request.urlopen(req, timeout=8) as r:
                ct = r.headers.get("content-type", "")
                body = r.read(100000)
                if "json" not in ct and not body.lstrip().startswith(b"[") and not body.lstrip().startswith(b"{"):
                    continue
                data_parsed = json.loads(body)
                items = data_parsed if isinstance(data_parsed, list) else (
                    data_parsed.get("data") or data_parsed.get("stations") or data_parsed.get("result") or []
                )
                for item in items:
                    lat = item.get("latitude") or item.get("lat")
                    lon = item.get("longitude") or item.get("lon") or item.get("lng")
                    if not lat or not lon:
                        continue
                    lat, lon = float(lat), float(lon)
                    state = state_from_coords(lat, lon) or default_state
                    if state not in INDIA_STATES:
                        continue
                    name = normalise_name(item.get("name") or item.get("stationName") or f"{name_prefix} CNG Station")
                    city = item.get("city") or item.get("area") or ""
                    records.append({"name": name, "latitude": round(lat, 6), "longitude": round(lon, 6),
                                    "city": city, "state": state, "source": SOURCE})
                if records:
                    print(f"  [{name_prefix}] API hit: {url} → {len(records)} stations")
                    return records
        except Exception:
            pass
    return records


def main() -> int:
    print("[IGL/MGL] Fetching deep zone data…")

    # Try operator APIs first
    igl_api = _try_operator_api([
        ("GET", "https://api.iglonline.net/api/GetCNGStations", None),
        ("POST", "https://www.iglonline.net/api/v1/cng-stations", {"city": "Delhi"}),
        ("GET", "https://iglonline.net/assets/data/cng-stations.json", None),
        ("GET", "https://iglonline.net/assets/cng.json", None),
    ], "IGL", "Delhi")

    mgl_api = _try_operator_api([
        ("GET", "https://api.mahanagargas.com/api/cng-stations", None),
        ("POST", "https://www.mahanagargas.com/api/getCNGStations", {}),
        ("GET", "https://www.mahanagargas.com/assets/data/cng-stations.json", None),
        ("GET", "https://mgl.mahanagargas.com/api/cng-stations", None),
    ], "MGL", "Maharashtra")

    print(f"[IGL/MGL] IGL API: {len(igl_api)}, MGL API: {len(mgl_api)}")

    # Deep zone OSM queries
    igl_osm = _fetch_zone(_IGL_BBOXES, "Delhi")
    mgl_osm = _fetch_zone(_MGL_BBOXES, "Maharashtra")

    combined = igl_api + mgl_api + igl_osm + mgl_osm

    # Dedup within 150m
    seen: list[tuple[float, float]] = []
    unique: list[dict] = []
    for r in combined:
        lat, lon = r["latitude"], r["longitude"]
        if not any(haversine_m(lat, lon, la, lo) < 150 for la, lo in seen):
            unique.append(r)
            seen.append((lat, lon))

    print(f"[IGL/MGL] Total unique: {len(unique)}")
    save_raw(SOURCE, unique)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
