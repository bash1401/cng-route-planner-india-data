#!/usr/bin/env python3
"""
Shared utilities for the CNG station data pipeline.
Provides: schema definition, haversine distance, state-from-coords lookup,
          deduplication, normalisation, file I/O helpers.
"""

from __future__ import annotations

import json
import math
import pathlib
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

# ─── Paths ───────────────────────────────────────────────────────────────────

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "raw_sources"
DATASET_DIR = REPO_ROOT / "dataset"
STATE_DIR = DATASET_DIR / "india"


# ─── Station schema ───────────────────────────────────────────────────────────

@dataclass
class Station:
    id: str
    name: str
    latitude: float
    longitude: float
    city: str
    state: str
    source: str                        # osm | gail | igl | mgl | atgl | wikidata | …
    address: str = ""
    operator: str = ""

    def to_dict(self) -> dict:
        d: dict = {
            "id": self.id,
            "name": self.name,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "city": self.city,
            "state": self.state,
            "source": self.source,
        }
        if self.address:
            d["address"] = self.address
        if self.operator:
            d["operator"] = self.operator
        return d


# ─── Haversine ────────────────────────────────────────────────────────────────

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ─── Fast spatial grid for deduplication ─────────────────────────────────────

class SpatialGrid:
    """
    Bucketed grid for O(1) approximate nearest-neighbour lookup.
    Cell size = 0.005° ≈ 500 m — large enough to cover the 200 m dedup radius.
    """
    CELL = 0.005

    def __init__(self) -> None:
        self._grid: dict[tuple[int, int], list[Station]] = {}

    def _cells(self, lat: float, lon: float) -> list[tuple[int, int]]:
        r = 1  # search 3×3 neighbourhood
        ci = int(lat / self.CELL)
        cj = int(lon / self.CELL)
        return [
            (ci + di, cj + dj)
            for di in range(-r, r + 1)
            for dj in range(-r, r + 1)
        ]

    def nearby(self, lat: float, lon: float, radius_m: float = 200) -> list[Station]:
        seen: set[str] = set()
        result: list[Station] = []
        for cell in self._cells(lat, lon):
            for s in self._grid.get(cell, []):
                if s.id not in seen and haversine_m(lat, lon, s.latitude, s.longitude) <= radius_m:
                    seen.add(s.id)
                    result.append(s)
        return result

    def add(self, s: Station) -> None:
        key = (int(s.latitude / self.CELL), int(s.longitude / self.CELL))
        self._grid.setdefault(key, []).append(s)


# ─── India state lookup (coordinate-based, no API) ───────────────────────────
# Each entry: (name, lat_min, lat_max, lon_min, lon_max).
# States are sorted smallest-first to prefer the most specific match.

_STATE_BOXES: list[tuple[str, float, float, float, float]] = [
    # Delhi (tiny — must come first)
    ("Delhi",           28.40, 28.89,  76.83,  77.35),
    # Union Territories
    ("Chandigarh",      30.62, 30.79,  76.68,  76.88),
    ("Puducherry",      11.80, 12.05,  79.70,  79.95),
    ("Lakshadweep",      8.00, 12.80,  71.70,  74.20),
    ("Andaman and Nicobar Islands", 6.70, 14.00, 92.20, 94.30),
    ("Dadra and Nagar Haveli",      20.02, 20.52, 72.83, 73.25),
    ("Daman and Diu",               20.30, 20.72, 72.52, 72.97),
    # States
    ("Jammu and Kashmir",  32.00, 37.10,  73.40,  80.40),
    ("Ladakh",             32.00, 36.00,  76.00,  80.40),
    ("Himachal Pradesh",   30.40, 33.26,  75.60,  79.00),
    ("Uttarakhand",        28.90, 31.50,  77.55,  81.05),
    ("Uttar Pradesh",      23.87, 30.50,  77.05,  84.65),
    ("Punjab",             29.55, 32.55,  73.90,  76.95),
    ("Haryana",            27.65, 30.92,  74.45,  77.60),
    ("Rajasthan",          23.06, 30.20,  69.48,  78.25),
    ("Gujarat",            20.10, 24.72,  68.18,  74.48),
    ("Maharashtra",        15.60, 22.10,  72.62,  80.90),
    ("Goa",                14.90, 15.82,  73.68,  74.32),
    ("Madhya Pradesh",     21.10, 26.88,  74.05,  82.82),
    ("Chhattisgarh",       17.80, 24.10,  80.25,  84.40),
    ("Bihar",              24.30, 27.53,  83.33,  88.30),
    ("Jharkhand",          21.97, 25.35,  83.32,  87.48),
    ("West Bengal",        21.44, 27.22,  85.83,  89.87),
    ("Odisha",             17.80, 22.57,  81.38,  87.50),
    ("Andhra Pradesh",     12.62, 19.92,  76.73,  84.76),
    ("Telangana",          15.85, 19.93,  77.17,  81.34),
    ("Karnataka",          11.60, 18.46,  74.06,  78.57),
    ("Kerala",              8.18, 12.78,  74.86,  77.42),
    ("Tamil Nadu",          8.07, 13.57,  76.24,  80.33),
    ("Assam",              24.12, 27.87,  89.69,  96.02),
    ("Meghalaya",          24.99, 26.11,  89.82,  92.80),
    ("Manipur",            23.84, 25.68,  93.03,  94.78),
    ("Nagaland",           25.17, 27.04,  93.20,  95.25),
    ("Mizoram",            21.96, 24.52,  92.26,  93.44),
    ("Tripura",            22.95, 24.53,  91.15,  92.34),
    ("Arunachal Pradesh",  26.70, 29.46,  91.60,  97.40),
    ("Sikkim",             27.07, 28.13,  88.02,  88.95),
    ("Arunachal Pradesh",  26.70, 29.46,  91.60,  97.40),
]

# Sort ascending by area so smaller/more-specific regions match first
_STATE_BOXES.sort(key=lambda t: (t[2] - t[1]) * (t[4] - t[3]))


def state_from_coords(lat: float, lon: float) -> str:
    """Return the Indian state name for given coordinates using bounding boxes."""
    best = ""
    best_area = float("inf")
    for name, lat_min, lat_max, lon_min, lon_max in _STATE_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            area = (lat_max - lat_min) * (lon_max - lon_min)
            if area < best_area:
                best_area = area
                best = name
    return best


# ─── Text normalisation ───────────────────────────────────────────────────────

def normalise_name(raw: str) -> str:
    """Lowercase, strip accents, collapse spaces, title-case."""
    if not raw:
        return "CNG Station"
    # NFKD decompose, drop combining chars
    s = "".join(
        c for c in unicodedata.normalize("NFKD", raw)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"\s+", " ", s).strip()
    # Title-case
    return s.title()


def slugify(text: str) -> str:
    """Return a URL-safe slug for state names."""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


# Recognised Indian states and UTs (English names only)
INDIA_STATES: frozenset[str] = frozenset({
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
    # UTs
    "Andaman and Nicobar Islands", "Chandigarh", "Dadra and Nagar Haveli",
    "Daman and Diu", "Delhi", "Jammu and Kashmir", "Ladakh", "Lakshadweep",
    "Puducherry",
})


def is_india_state(state: str) -> bool:
    """Return True if the state name is a known Indian state/UT."""
    if not state:
        return True  # Unknown — keep it (will be labelled "Unknown")
    # Exact match (English)
    if state in INDIA_STATES:
        return True
    # Contains ASCII letters only — might be a local-script name (e.g. Urdu, Bengali)
    if not all(ord(c) < 128 for c in state if c.isalpha()):
        return False  # Non-ASCII state name → not India
    # Partial/alternate English names
    state_lower = state.lower()
    known_lower = {s.lower() for s in INDIA_STATES}
    return state_lower in known_lower


# ─── File helpers ─────────────────────────────────────────────────────────────

def load_raw(source: str) -> list[dict]:
    path = RAW_DIR / f"{source}.json"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def save_raw(source: str, records: list[dict]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{source}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"  [raw] {source}: {len(records)} records → {path.name}")


def load_stations_json(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def save_stations_json(path: pathlib.Path, stations: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(stations, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ─── HTTP helper ─────────────────────────────────────────────────────────────

import urllib.request
import urllib.error

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-IN,en;q=0.9",
}


def http_get(url: str, *, timeout: int = 20, retries: int = 3,
             extra_headers: Optional[dict] = None) -> Optional[bytes]:
    """GET url, retry on failure, return bytes or None."""
    headers = {**COMMON_HEADERS, **(extra_headers or {})}
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as exc:
            print(f"  [http] attempt {attempt}/{retries} failed for {url}: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


def http_post(url: str, data: bytes, *, timeout: int = 60,
              content_type: str = "text/plain",
              retries: int = 3) -> Optional[bytes]:
    """POST url with data, return bytes or None."""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url, data=data, method="POST",
                headers={**COMMON_HEADERS, "Content-Type": content_type},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as exc:
            print(f"  [http] attempt {attempt}/{retries} failed for {url}: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None
