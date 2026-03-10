# CNG Route Planner India Dataset

Public, auto-updated CNG station dataset for India.

## Contents
- `dataset/stations.json`: normalized station list
- `scripts/extract_cng_stations.py`: Overpass fetch + cleanup
- `.github/workflows/update-stations.yml`: daily refresh automation
- `.github/ISSUE_TEMPLATE/station-suggestion.md`: crowd-sourced additions

## Dataset schema
Each station entry uses:
- `id`
- `name`
- `latitude`
- `longitude`
- `city`
- `state`

## Regenerate locally
```bash
python scripts/extract_cng_stations.py
```

## Data source
- OpenStreetMap via Overpass API (`amenity=fuel`, `fuel:cng=yes`)
