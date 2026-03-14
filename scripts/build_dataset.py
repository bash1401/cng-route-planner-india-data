#!/usr/bin/env python3
"""
Build the final CNG stations dataset.

Pipeline:
  1. Load all raw_sources/*.json files
  2. Validate schema (must have lat/lon)
  3. Enrich missing city/state via geocoding
  4. Deduplicate: stations within 200 m are merged (highest-quality source wins)
  5. Generate stable unique IDs
  6. Output:
       dataset/stations.json          — full merged dataset
       dataset/india/{state}.json     — per-state split (for large datasets)
       dataset/meta.json              — summary metadata
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import sys
from datetime import datetime, timezone
from utils import (
    REPO_ROOT, RAW_DIR, DATASET_DIR, STATE_DIR,
    Station, SpatialGrid, save_stations_json, load_stations_json,
    state_from_coords, normalise_name, slugify, is_india_state,
)

# ─── Source priority (lower = higher trust) ──────────────────────────────────
SOURCE_PRIORITY: dict[str, int] = {
    "igl": 1,
    "mgl": 1,
    "mngl": 1,          # MNGL - exact coordinates from website
    "gail_pdf": 1,      # GAIL Gas PDF - exact coordinates
    "gail": 2,
    "atgl": 2,
    "gujarat_gas": 2,
    "igl_mgl": 2,       # Deep IGL/MGL Overpass
    "osm_allstations": 3,
    "wikidata": 3,
    "osm": 4,
    "osm_extra": 4,
    "nominatim_grid": 5,
    "community": 5,     # User submissions from GitHub Issues
    "cgd_inferred": 8,  # Geographic inference - fuel stations in CGD areas
    "all_india_fuel": 9,  # All OSM fuel stations in India (broadest coverage)
}

# ─── Known raw source files (discovery order = merge priority) ───────────────
RAW_SOURCES = [
    # Tier 1: Verified exact coordinates from operator websites/official PDFs
    "gail_pdf",
    "mngl",
    # Tier 2: Scraped from operator websites (PIN/address geocoded)
    "igl",
    "mgl",
    "igl_mgl",
    "gail",
    "atgl",
    "gujarat_gas",
    # Tier 3: OSM-tagged CNG stations
    "osm",
    "osm_extra",
    "osm_allstations",
    # Tier 4: City-grid and Wikidata
    "nominatim_grid",
    "wikidata",
    # Tier 5: Geographic inference (all fuel stations in CGD areas)
    "cgd_inferred",
    # Tier 6: All-India OSM sweep (broadest coverage, lowest confidence)
    "all_india_fuel",
    # Community submissions
    "community",
]


def load_all_raw() -> list[dict]:
    """Load and merge all raw source JSON files."""
    all_records: list[dict] = []
    for source in RAW_SOURCES:
        path = RAW_DIR / f"{source}.json"
        if not path.exists():
            print(f"  [skip] raw_sources/{source}.json not found")
            continue
        with path.open(encoding="utf-8") as f:
            try:
                records = json.load(f)
            except json.JSONDecodeError as exc:
                print(f"  [warn] {source}.json is malformed: {exc}")
                continue
        print(f"  [load] {source}: {len(records)} records")
        all_records.extend(records)

    # Also include any community-approved stations from pending/
    community_path = REPO_ROOT / "pending" / "approved_stations.json"
    if community_path.exists():
        with community_path.open(encoding="utf-8") as f:
            community = json.load(f)
        for s in community:
            s["source"] = "community"
        print(f"  [load] community: {len(community)} records")
        all_records.extend(community)

    return all_records


def validate(record: dict) -> bool:
    """Return True if record has valid coordinates inside India."""
    try:
        lat = float(record["latitude"])
        lon = float(record["longitude"])
    except (KeyError, ValueError, TypeError):
        return False
    return 6.5 <= lat <= 37.5 and 68.0 <= lon <= 97.5


def normalise_record(r: dict) -> dict:
    """Ensure all required fields exist with sensible defaults."""
    lat = round(float(r["latitude"]), 6)
    lon = round(float(r["longitude"]), 6)

    name = normalise_name(str(r.get("name") or "CNG Station"))
    state = str(r.get("state") or "").strip()
    if not state:
        state = state_from_coords(lat, lon)
    city = str(r.get("city") or "").strip()
    source = str(r.get("source") or "unknown")
    address = str(r.get("address") or "").strip()
    operator = str(r.get("operator") or "").strip()

    return {
        "latitude": lat,
        "longitude": lon,
        "name": name,
        "state": state,
        "city": city,
        "source": source,
        "address": address,
        "operator": operator,
    }


def _pick_better(a: dict, b: dict) -> dict:
    """Return the higher-quality record of two duplicates."""
    pa = SOURCE_PRIORITY.get(a["source"], 9)
    pb = SOURCE_PRIORITY.get(b["source"], 9)
    if pa <= pb:
        # Prefer a, but fill missing fields from b
        merged = {**b, **a}
    else:
        merged = {**a, **b}
    return merged


def _stable_id(rec: dict) -> str:
    """Generate a stable ID from coordinates (rounded to ~11 m precision)."""
    lat_s = f"{rec['latitude']:.4f}"
    lon_s = f"{rec['longitude']:.4f}"
    digest = hashlib.md5(f"{lat_s}:{lon_s}".encode()).hexdigest()[:8]
    return f"cng-{digest}"


class _GridEntry:
    """Lightweight proxy to satisfy SpatialGrid's attribute access."""
    __slots__ = ("id", "latitude", "longitude")

    def __init__(self, sid: str, lat: float, lon: float) -> None:
        self.id = sid
        self.latitude = lat
        self.longitude = lon


def deduplicate(records: list[dict]) -> list[dict]:
    """
    Remove duplicate stations (within 200 m of each other).
    Merges duplicates keeping highest-priority source data.
    """
    grid = SpatialGrid()
    merged_map: dict[str, dict] = {}   # stable_id -> merged record
    entry_map: dict[str, _GridEntry] = {}  # stable_id -> grid entry

    for rec in records:
        lat, lon = rec["latitude"], rec["longitude"]
        neighbours = grid.nearby(lat, lon, radius_m=200)

        if neighbours:
            # Merge with the nearest neighbour's merged record
            existing_id = neighbours[0].id
            existing = merged_map.get(existing_id, {})
            merged_map[existing_id] = _pick_better(existing or rec, rec)
        else:
            sid = _stable_id(rec)
            merged_map[sid] = rec
            entry = _GridEntry(sid, lat, lon)
            entry_map[sid] = entry
            grid.add(entry)

    return list(merged_map.values())


def assign_ids(records: list[dict]) -> list[dict]:
    """Assign final stable IDs to all records."""
    for r in records:
        r["id"] = _stable_id(r)
    return records


def partition_by_state(records: list[dict]) -> dict[str, list[dict]]:
    """Group records by state name."""
    groups: dict[str, list[dict]] = {}
    for r in records:
        state = r.get("state") or "Unknown"
        groups.setdefault(state, []).append(r)
    return groups


def build_output(record: dict) -> dict:
    """Strip internal-only fields and produce final output object."""
    out: dict = {
        "id": record["id"],
        "name": record["name"],
        "latitude": record["latitude"],
        "longitude": record["longitude"],
        "city": record["city"],
        "state": record["state"],
        "source": record["source"],
    }
    if record.get("address"):
        out["address"] = record["address"]
    if record.get("operator"):
        out["operator"] = record["operator"]
    return out


def main() -> int:
    print("\n" + "=" * 60)
    print("  CNG Dataset Builder")
    print("=" * 60)

    # ── 1. Load ────────────────────────────────────────────────────
    print("\n[1/5] Loading raw sources …")
    raw = load_all_raw()
    print(f"  Total raw records: {len(raw)}")

    # ── 2. Validate ────────────────────────────────────────────────
    print("\n[2/5] Validating (coords + India-only filter) …")
    valid_coords = [r for r in raw if validate(r)]
    # Filter out non-Indian geocoded states (e.g. Bangladesh, Pakistan)
    valid = [r for r in valid_coords if is_india_state(str(r.get("state") or ""))]
    dropped_foreign = len(valid_coords) - len(valid)
    print(f"  Valid: {len(valid)}  "
          f"Bad coords: {len(raw) - len(valid_coords)}  "
          f"Foreign state: {dropped_foreign}")

    # ── 3. Normalise ───────────────────────────────────────────────
    print("\n[3/5] Normalising …")
    normalised = [normalise_record(r) for r in valid]

    # ── 4. Enrich missing city/state ───────────────────────────────
    print("\n[4/5] Enriching missing city/state (bounding-box first, Nominatim fallback) …")
    needs_geocode = sum(1 for r in normalised if not (r["city"] and r["state"]))
    print(f"  {needs_geocode} records need geocoding")
    if needs_geocode > 0 and "--no-geocode" not in sys.argv:
        try:
            from geocode_enrich import enrich_stations
            normalised = enrich_stations(normalised, max_requests=1500)
        except ImportError:
            print("  [warn] geocode_enrich not available — using bounding box only")
        except Exception as exc:
            print(f"  [warn] geocoding failed: {exc}")
    else:
        print("  Geocoding skipped (--no-geocode flag)")

    # ── 5. Deduplicate ────────────────────────────────────────────
    print("\n[5/5] Deduplicating (200 m radius) …")
    unique = deduplicate(normalised)
    assign_ids(unique)
    print(f"  Unique stations: {len(unique)}  (removed {len(normalised) - len(unique)} duplicates)")

    # ── Output ────────────────────────────────────────────────────
    final = [build_output(r) for r in sorted(unique, key=lambda x: (x["state"], x["city"], x["name"]))]

    # Full dataset
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATASET_DIR / "stations.json"
    save_stations_json(out_path, final)
    print(f"\n  ✓ {out_path}  ({len(final)} stations, "
          f"{out_path.stat().st_size // 1024} KB)")

    # Per-state partitions
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_groups = partition_by_state(final)
    for state, stations in sorted(state_groups.items()):
        slug = slugify(state)
        p = STATE_DIR / f"{slug}.json"
        save_stations_json(p, stations)
    print(f"  ✓ {len(state_groups)} state files in dataset/india/")

    # Metadata
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_stations": len(final),
        "states_covered": len(state_groups),
        "sources": _source_summary(final),
    }
    save_stations_json(DATASET_DIR / "meta.json", meta)

    # ── Summary ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  TOTAL UNIQUE CNG STATIONS: {len(final)}")
    print("=" * 60)
    print("\n  By source:")
    for src, count in sorted(meta["sources"].items(), key=lambda x: -x[1]):
        print(f"    {src:<20} {count:>5}")
    print("\n  Top 15 states:")
    for state, stations in sorted(state_groups.items(), key=lambda x: -len(x[1]))[:15]:
        bar = "█" * min(len(stations) // 5, 30)
        print(f"    {state:<30} {len(stations):>4}  {bar}")

    return 0


def _source_summary(records: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in records:
        src = r.get("source", "unknown")
        counts[src] = counts.get(src, 0) + 1
    return counts


if __name__ == "__main__":
    raise SystemExit(main())
