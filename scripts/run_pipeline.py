#!/usr/bin/env python3
"""
Master pipeline runner.

Runs all fetch_*.py scripts in sequence, then builds the final dataset.
Safe to re-run: each source saves to raw_sources/ independently.

Usage:
    python scripts/run_pipeline.py               # full run
    python scripts/run_pipeline.py --osm-only    # only OSM (fast test)
    python scripts/run_pipeline.py --no-geocode  # skip Nominatim enrichment
    python scripts/run_pipeline.py --build-only  # skip fetching, rebuild only
"""

from __future__ import annotations

import subprocess
import sys
import time
import pathlib

SCRIPTS_DIR = pathlib.Path(__file__).parent

FETCH_SCRIPTS = [
    # ── Tier 1: Official PDFs and operator websites with exact coordinates ──
    ("GAIL Gas PDF (220 stations)",     "fetch_gail_gas_pdf.py"),    # S3-hosted PDF
    ("MNGL Pune (118 stations)",        "fetch_mngl.py"),            # MNGL website

    # ── Tier 2: OSM targeted CNG queries ──
    ("OSM CNG-tagged (original)",       "fetch_osm_cng.py"),
    ("OSM All-stations (broad query)",  "fetch_osm_allstations.py"), # fuel:cng + name~CNG + operators
    ("OSM Extra targeted",              "fetch_overpass_extra.py"),

    # ── Tier 3: Operator websites (geocoded) ──
    ("Gujarat Gas (343+ stations)",     "fetch_gujarat_gas_v2.py"),  # main + test sites
    ("IGL + MGL deep zones",            "fetch_igl_mgl_deep.py"),
    ("ATGL improved",                   "fetch_atgl_v2.py"),
    ("Wikidata SPARQL",                 "fetch_wikidata.py"),

    # ── Tier 4: City-grid Nominatim/Photon search ──
    ("Nominatim/Photon city grid",      "fetch_nominatim_grid.py"),  # 200+ cities

    # ── Tier 5: Geographic inference (all fuel in CGD areas) ──
    ("CGD cities all fuel stations",    "fetch_cgd_cities.py"),      # ~5000+ stations

    # ── Legacy scrapers (kept for backwards compatibility) ──
    ("GAIL Gas (legacy)",               "fetch_gail_gas.py"),
    ("IGL (legacy)",                    "fetch_igl.py"),
    ("MGL (legacy)",                    "fetch_mgl.py"),
    ("ATGL (legacy)",                   "fetch_atgl.py"),
    ("Gujarat Gas (legacy)",            "fetch_gujarat_gas.py"),
]


def run_script(name: str, script: str) -> bool:
    print(f"\n{'─'*60}")
    print(f"  Running: {name} ({script})")
    print("─" * 60)
    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / script)],
        cwd=str(SCRIPTS_DIR),
    )
    ok = result.returncode == 0
    status = "✓" if ok else "⚠ (partial/failed)"
    print(f"  {name}: {status}")
    return ok


def main() -> int:
    args = sys.argv[1:]
    osm_only = "--osm-only" in args
    build_only = "--build-only" in args
    no_geocode = "--no-geocode" in args

    start = time.time()
    print("=" * 60)
    print("  CNG Route Planner India — Data Pipeline")
    print("=" * 60)

    if not build_only:
        if osm_only:
            run_script("OSM (OpenStreetMap)", "fetch_osm_cng.py")
        else:
            results = {}
            for name, script in FETCH_SCRIPTS:
                ok = run_script(name, script)
                results[name] = ok
                time.sleep(1)  # Be polite between sources

            print("\n\nFetch summary:")
            for name, ok in results.items():
                print(f"  {'✓' if ok else '✗'}  {name}")

    # Build
    print(f"\n{'─'*60}")
    print("  Building final dataset …")
    print("─" * 60)
    build_args = [sys.executable, str(SCRIPTS_DIR / "build_dataset.py")]
    if no_geocode:
        build_args.append("--no-geocode")
    result = subprocess.run(build_args, cwd=str(SCRIPTS_DIR))

    elapsed = time.time() - start
    print(f"\nPipeline completed in {elapsed:.0f}s")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
