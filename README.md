# CNG Route Planner India — Station Dataset

Public dataset of CNG stations across India. Used by the [CNG Route Planner India](https://github.com/bash1401/cng-route-planner-india-app) app.

---

## How the data pipeline works

```
OpenStreetMap (Overpass API)
        ↓ daily cron
  dataset/stations.json   ←──────────────────────────────────┐
        ↑                                                     │
Community contributions (GitHub Issues)                       │
        ↓                                                     │
  validate-station.yml                                        │
  (instant on issue open)                                     │
   • India bounds check                                       │
   • Duplicate check (<300m from existing)                    │
   • Overpass API cross-check                                 │
   → Labels: ✅ validated / ❌ duplicate / 🚫 out-of-bounds   │
        ↓                                                     │
  pending/pending_stations.json                               │
        ↓                                                     │
  consensus-check.yml (daily 02:00 UTC)                       │
   • 3 unique reporters confirm same spot ──────── APPROVE ───┘
   • < 3 reporters → stays pending

Station removal reports (separate issue template)
        ↓
  process-report.yml (instant on issue open)
   • 5 unique reporters required before any action
   • Action: remove (permanently closed) or flag (wrong location)
```

---

## Station counts

| Source     | Count  |
|------------|--------|
| OpenStreetMap (Overpass) | See dataset/stations.json |
| Community additions | See pending/pending_stations.json |

---

## Add a missing station

**[→ Submit a new station](https://github.com/bash1401/cng-route-planner-india-data/issues/new?template=new_station.yml)**

Requirements:
- Coordinates must be within India bounds
- Must not be within 300m of an existing station
- Needs **3 independent confirmations** before it goes live
- OSM-verified stations get priority trust

---

## Report a wrong or closed station

**[→ Report a station issue](https://github.com/bash1401/cng-route-planner-india-data/issues/new?template=report_station.yml)**

Requirements:
- Needs **5 independent reports** before removal/flagging
- High threshold intentional — removing a good station can strand CNG vehicle users

---

## Dataset format

```json
[
  {
    "id": "osm-node-123456789",
    "name": "GAIL CNG Station",
    "latitude": 28.6139,
    "longitude": 77.2090,
    "city": "New Delhi",
    "state": "Delhi",
    "source": "osm"
  }
]
```

Community-added stations include extra fields:
```json
{
  "source": "community",
  "verified_by": ["user1", "user2", "user3"],
  "added_at": "2026-03-12T00:00:00Z"
}
```

---

## Automation schedule

| Workflow | Trigger |
|----------|---------|
| `update-stations.yml` | Daily 01:30 UTC — refresh from Overpass |
| `validate-station.yml` | On every new `new-station` issue opened |
| `consensus-check.yml` | Daily 02:00 UTC + when issue labeled `validated` |
| `process-report.yml` | On every new `station-report` issue opened |
