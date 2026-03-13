# CNG Route Planner India — Dataset Repository

> **Goal:** Build the largest open, free, and accurate dataset of CNG (Compressed Natural Gas) filling stations in India for use by the [CNG Route Planner India](https://github.com/bash1401/cng-route-planner-india-app) mobile app and any other project.

[![Update Dataset](https://github.com/bash1401/cng-route-planner-india-data/actions/workflows/update_dataset.yml/badge.svg)](https://github.com/bash1401/cng-route-planner-india-data/actions/workflows/update_dataset.yml)

---

## Dataset

| File | Description |
|---|---|
| `dataset/stations.json` | **Full India dataset** — all verified CNG stations |
| `dataset/india/{state}.json` | Per-state split (e.g. `gujarat.json`, `delhi.json`) |
| `dataset/meta.json` | Build metadata (count, sources, timestamp) |

### Station Schema

```json
{
  "id":        "cng-a1b2c3d4",
  "name":      "IGL CNG Station, Sector 15",
  "latitude":  28.570123,
  "longitude": 77.321456,
  "city":      "Noida",
  "state":     "Uttar Pradesh",
  "source":    "igl",
  "address":   "Plot 12, Sector 15, Noida",
  "operator":  "IGL"
}
```

| Field | Required | Notes |
|---|---|---|
| `id` | ✓ | Stable hash based on coordinates |
| `name` | ✓ | Station display name |
| `latitude` | ✓ | WGS84 decimal degrees (6 d.p.) |
| `longitude` | ✓ | WGS84 decimal degrees (6 d.p.) |
| `city` | ✓ | City or district |
| `state` | ✓ | Indian state/UT name |
| `source` | ✓ | Data source (`osm`, `igl`, `mgl`, `gail`, `atgl`, `gujarat_gas`, `wikidata`, `community`) |
| `address` | optional | Full street address if available |
| `operator` | optional | CGD company name |

### Flutter App Integration

```dart
// Fetch full dataset (cached 24h)
const datasetUrl =
  'https://raw.githubusercontent.com/bash1401/cng-route-planner-india-data/main/dataset/stations.json';

// Per-state (faster for state-specific queries)
const gujaratUrl =
  'https://raw.githubusercontent.com/bash1401/cng-route-planner-india-data/main/dataset/india/gujarat.json';
```

---

## Data Sources

| Source | Operator | Coverage | Script |
|---|---|---|---|
| **OpenStreetMap** | Community-tagged | All India | `fetch_osm_cng.py` |
| **GAIL Gas** | GAIL Gas | 16 states, ~1,384 stations | `fetch_gail_gas.py` |
| **IGL** | Indraprastha Gas | Delhi/NCR | `fetch_igl.py` |
| **MGL** | Mahanagar Gas | Mumbai, Thane | `fetch_mgl.py` |
| **ATGL** | Adani Total Gas | Multiple cities | `fetch_atgl.py` |
| **Gujarat Gas** | Gujarat Gas | Gujarat | `fetch_gujarat_gas.py` |
| **Wikidata** | Community | All India | `fetch_wikidata.py` |
| **Community** | User submissions | Growing | GitHub Issues |

---

## Automated Updates

A GitHub Actions workflow runs **daily at 02:00 UTC**:
1. Queries all data sources
2. Normalises and deduplicates (stations within 200 m are merged)
3. Enriches missing city/state via Nominatim reverse geocoding
4. Commits the updated `dataset/` if any changes are found

**Manual trigger:**
Go to [Actions → Rebuild CNG Dataset → Run workflow](../../actions/workflows/update_dataset.yml)

---

## Repository Structure

```
.
├── dataset/
│   ├── stations.json           # Full merged dataset (6,000+ stations)
│   ├── meta.json               # Build metadata
│   └── india/
│       ├── delhi.json
│       ├── gujarat.json
│       ├── maharashtra.json
│       └── ...                 # One file per state
├── raw_sources/
│   ├── osm.json                # Raw OSM data
│   ├── gail.json               # Raw GAIL Gas data
│   ├── igl.json                # Raw IGL data
│   └── ...
├── pending/
│   ├── pending_stations.json   # Community-submitted, awaiting 3 confirmations
│   └── station_reports.json    # Reports of incorrect/closed stations
├── scripts/
│   ├── utils.py                # Shared utilities
│   ├── fetch_osm_cng.py        # OpenStreetMap Overpass fetcher
│   ├── fetch_gail_gas.py       # GAIL Gas scraper
│   ├── fetch_igl.py            # IGL scraper
│   ├── fetch_mgl.py            # MGL scraper
│   ├── fetch_atgl.py           # ATGL scraper
│   ├── fetch_gujarat_gas.py    # Gujarat Gas scraper
│   ├── fetch_wikidata.py       # Wikidata SPARQL
│   ├── geocode_enrich.py       # Nominatim geocoding
│   ├── build_dataset.py        # Merge + dedup + partition
│   └── run_pipeline.py         # Master runner
├── .github/
│   ├── workflows/
│   │   ├── update_dataset.yml  # Daily auto-refresh
│   │   ├── validate-station.yml
│   │   ├── consensus-check.yml
│   │   └── process-report.yml
│   └── ISSUE_TEMPLATE/
│       ├── new_station.yml
│       └── report_station.yml
└── requirements.txt
```

---

## Running Locally

```bash
# Clone
git clone https://github.com/bash1401/cng-route-planner-india-data.git
cd cng-route-planner-india-data

# Install dependencies
pip install -r requirements.txt

# Full pipeline (all sources + geocoding — takes ~30 min)
cd scripts
python run_pipeline.py

# Fast test (OSM only, no geocoding — takes ~3 min)
python run_pipeline.py --osm-only --no-geocode

# Rebuild dataset from existing raw_sources/ (no fetching)
python run_pipeline.py --build-only

# Individual source
python fetch_gail_gas.py
python fetch_igl.py
```

---

## Contributing Stations

### Option 1 — In-App Submission (Recommended)

Open the **CNG Route Planner India** app, tap **"Add Missing Station"** on the home screen, fill in the details and tap Submit. No GitHub account required.

### Option 2 — GitHub Issue

Use the structured issue template:
👉 [Submit a new station](../../issues/new?template=new_station.yml)

**Validation rules:**
- Coordinates must be inside India (6.5°–37.5°N, 68°–97.5°E)
- Must not already exist within 200 m of an existing station
- Requires confirmation from 3 unique users before being added

### Option 3 — Pull Request

1. Fork this repository
2. Add your station(s) to `pending/pending_stations.json`:
   ```json
   {
     "name": "Station Name",
     "latitude": 28.1234,
     "longitude": 77.5678,
     "city": "City Name",
     "state": "State Name",
     "address": "Optional full address",
     "operator": "IGL",
     "verified_by": "your-github-username",
     "verification_note": "Refuelled here on 2026-03-10"
   }
   ```
3. Open a Pull Request — a maintainer will review and merge

### Reporting Wrong/Closed Stations

👉 [Report a station issue](../../issues/new?template=report_station.yml)

Stations flagged by 5 unique users are automatically removed.

---

## Data Quality

- **Deduplication**: Any two stations within 200 m are merged into one (highest-quality source wins)
- **Coordinate validation**: All points are verified to fall within India's bounding box
- **State enrichment**: Missing state/city data is filled via OSM Nominatim reverse geocoding
- **Source trust order**: IGL/MGL (official) → GAIL/ATGL (official) → Wikidata → OSM → Community

---

## License

Dataset: [Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/)  
Scripts: MIT License  

OSM data © OpenStreetMap contributors.  
Wikidata content is available under [CC0](https://creativecommons.org/publicdomain/zero/1.0/).
