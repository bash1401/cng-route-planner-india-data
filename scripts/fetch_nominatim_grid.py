"""
Nominatim + Photon city-grid search — CNG stations across 150+ Indian cities.

Photon (photon.komoot.io) is OSM-based but has its own indexing, often
returning different results from direct Overpass. Nominatim structured
searches also catch things the bbox queries miss.

Rate limits: Nominatim 1 req/sec, Photon is more lenient.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, haversine_m, normalise_name, state_from_coords, INDIA_STATES

SOURCE = "nominatim_grid"
_NOMINATIM = "https://nominatim.openstreetmap.org/search"
_PHOTON = "https://photon.komoot.io/api/"
_DELAY = 1.1

_CITIES: list[tuple[str, str]] = [
    ("Delhi", "Delhi"), ("Noida", "Uttar Pradesh"), ("Greater Noida", "Uttar Pradesh"),
    ("Ghaziabad", "Uttar Pradesh"), ("Gurugram", "Haryana"), ("Faridabad", "Haryana"),
    ("Karnal", "Haryana"), ("Panipat", "Haryana"), ("Rewari", "Haryana"),
    ("Sonipat", "Haryana"), ("Kaithal", "Haryana"), ("Rohtak", "Haryana"),
    ("Panchkula", "Haryana"), ("Ambala", "Haryana"), ("Chandigarh", "Chandigarh"),
    ("Ludhiana", "Punjab"), ("Amritsar", "Punjab"), ("Jalandhar", "Punjab"),
    ("Patiala", "Punjab"), ("Bathinda", "Punjab"), ("Mohali", "Punjab"),
    ("Mumbai", "Maharashtra"), ("Thane", "Maharashtra"), ("Navi Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"), ("Nagpur", "Maharashtra"), ("Nashik", "Maharashtra"),
    ("Aurangabad", "Maharashtra"), ("Solapur", "Maharashtra"), ("Kolhapur", "Maharashtra"),
    ("Raigad", "Maharashtra"), ("Dhule", "Maharashtra"),
    ("Ahmedabad", "Gujarat"), ("Surat", "Gujarat"), ("Vadodara", "Gujarat"),
    ("Rajkot", "Gujarat"), ("Bhavnagar", "Gujarat"), ("Gandhinagar", "Gujarat"),
    ("Junagadh", "Gujarat"), ("Anand", "Gujarat"), ("Morbi", "Gujarat"),
    ("Jamnagar", "Gujarat"), ("Mehsana", "Gujarat"), ("Kutch", "Gujarat"),
    ("Bharuch", "Gujarat"), ("Amreli", "Gujarat"), ("Patan", "Gujarat"),
    ("Lucknow", "Uttar Pradesh"), ("Kanpur", "Uttar Pradesh"), ("Agra", "Uttar Pradesh"),
    ("Varanasi", "Uttar Pradesh"), ("Prayagraj", "Uttar Pradesh"),
    ("Meerut", "Uttar Pradesh"), ("Mathura", "Uttar Pradesh"), ("Moradabad", "Uttar Pradesh"),
    ("Aligarh", "Uttar Pradesh"), ("Bareilly", "Uttar Pradesh"),
    ("Muzaffarnagar", "Uttar Pradesh"), ("Hapur", "Uttar Pradesh"),
    ("Jaipur", "Rajasthan"), ("Jodhpur", "Rajasthan"), ("Udaipur", "Rajasthan"),
    ("Ajmer", "Rajasthan"), ("Kota", "Rajasthan"), ("Bikaner", "Rajasthan"),
    ("Sikar", "Rajasthan"), ("Alwar", "Rajasthan"), ("Bharatpur", "Rajasthan"),
    ("Bhopal", "Madhya Pradesh"), ("Indore", "Madhya Pradesh"), ("Jabalpur", "Madhya Pradesh"),
    ("Gwalior", "Madhya Pradesh"), ("Ujjain", "Madhya Pradesh"), ("Dewas", "Madhya Pradesh"),
    ("Bengaluru", "Karnataka"), ("Mysuru", "Karnataka"), ("Hubli", "Karnataka"),
    ("Mangaluru", "Karnataka"), ("Belagavi", "Karnataka"), ("Tumkur", "Karnataka"),
    ("Chennai", "Tamil Nadu"), ("Coimbatore", "Tamil Nadu"), ("Madurai", "Tamil Nadu"),
    ("Tiruchirappalli", "Tamil Nadu"), ("Salem", "Tamil Nadu"),
    ("Hyderabad", "Telangana"), ("Warangal", "Telangana"), ("Nizamabad", "Telangana"),
    ("Vijayawada", "Andhra Pradesh"), ("Visakhapatnam", "Andhra Pradesh"),
    ("Tirupati", "Andhra Pradesh"), ("Guntur", "Andhra Pradesh"),
    ("Kolkata", "West Bengal"), ("Howrah", "West Bengal"), ("Durgapur", "West Bengal"),
    ("Asansol", "West Bengal"), ("Siliguri", "West Bengal"),
    ("Patna", "Bihar"), ("Gaya", "Bihar"), ("Muzaffarpur", "Bihar"),
    ("Ranchi", "Jharkhand"), ("Jamshedpur", "Jharkhand"), ("Dhanbad", "Jharkhand"),
    ("Bhubaneswar", "Odisha"), ("Cuttack", "Odisha"), ("Rourkela", "Odisha"),
    ("Dehradun", "Uttarakhand"), ("Haridwar", "Uttarakhand"), ("Roorkee", "Uttarakhand"),
    ("Raipur", "Chhattisgarh"), ("Bilaspur", "Chhattisgarh"), ("Bhilai", "Chhattisgarh"),
    ("Guwahati", "Assam"), ("Kochi", "Kerala"), ("Thiruvananthapuram", "Kerala"),
    ("Panaji", "Goa"), ("Jammu", "Jammu and Kashmir"),
    ("Mira Bhayandar", "Maharashtra"), ("Vasai Virar", "Maharashtra"),
    ("Pimpri Chinchwad", "Maharashtra"), ("Nanded", "Maharashtra"),
    ("Ludhiana", "Punjab"), ("Jalandhar", "Punjab"),
    ("Hisar", "Haryana"), ("Yamunanagar", "Haryana"), ("Gurgaon", "Haryana"),
    ("Modinagar", "Uttar Pradesh"), ("Bulandshahr", "Uttar Pradesh"),
    ("Anand", "Gujarat"), ("Nadiad", "Gujarat"), ("Palanpur", "Gujarat"),
    ("Daman", "Daman and Diu"), ("Silvassa", "Dadra and Nagar Haveli"),
]

_FUEL_TYPES = {"fuel", "amenity"}


def _geocode_city(city: str, state: str) -> tuple[float, float] | None:
    params = urllib.parse.urlencode({
        "q": f"{city}, {state}, India",
        "format": "json", "limit": "1", "countrycodes": "in",
    })
    time.sleep(_DELAY)
    try:
        req = urllib.request.Request(
            f"{_NOMINATIM}?{params}",
            headers={"User-Agent": "CNG-Planner-India/1.0"},
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None


def _photon_search(lat: float, lon: float, radius: float = 0.25) -> list[dict]:
    params = urllib.parse.urlencode({
        "q": "CNG",
        "limit": "50",
        "lang": "en",
        "bbox": f"{lon-radius},{lat-radius},{lon+radius},{lat+radius}",
    })
    try:
        req = urllib.request.Request(
            f"{_PHOTON}?{params}",
            headers={"User-Agent": "CNG-Planner-India/1.0"},
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read()).get("features", [])
    except Exception:
        return []


def _nominatim_search(city: str, state: str) -> list[dict]:
    params = urllib.parse.urlencode({
        "q": f"CNG pump {city} India",
        "format": "json", "limit": "50", "countrycodes": "in",
    })
    time.sleep(_DELAY)
    try:
        req = urllib.request.Request(
            f"{_NOMINATIM}?{params}",
            headers={"User-Agent": "CNG-Planner-India/1.0"},
        )
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read())
    except Exception:
        return []


def main() -> int:
    print(f"[NOM-GRID] Searching {len(_CITIES)} cities…")
    records: list[dict] = []
    seen: list[tuple[float, float]] = []

    for i, (city, state) in enumerate(_CITIES):
        if i % 25 == 0:
            print(f"  [{i}/{len(_CITIES)}] …{city} — {len(records)} stations so far")

        city_coords = _geocode_city(city, state)
        if not city_coords:
            continue
        clat, clon = city_coords

        # Photon search
        features = _photon_search(clat, clon)
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry", {}).get("coordinates", [])
            if len(geom) < 2:
                continue
            flon, flat = float(geom[0]), float(geom[1])
            if not (6.5 <= flat <= 37.5 and 68.0 <= flon <= 97.5):
                continue
            # Filter: must be a fuel/amenity type
            osm_value = props.get("osm_value") or ""
            feat_name = props.get("name") or ""
            if "fuel" not in osm_value.lower() and not any(
                kw in feat_name.lower() for kw in ["cng", "gas", "fuel", "petroleum", "petrol"]
            ):
                continue
            fstate = state_from_coords(flat, flon) or state
            if fstate not in INDIA_STATES:
                continue
            if not any(haversine_m(flat, flon, la, lo) < 50 for la, lo in seen):
                name = normalise_name(feat_name) or "CNG Station"
                records.append({"name": name, "latitude": round(flat, 6), "longitude": round(flon, 6),
                                 "city": city, "state": fstate, "source": SOURCE})
                seen.append((flat, flon))
        time.sleep(0.5)

        # Nominatim search
        results = _nominatim_search(city, state)
        for r in results:
            flat, flon = float(r.get("lat", 0)), float(r.get("lon", 0))
            if not (6.5 <= flat <= 37.5 and 68.0 <= flon <= 97.5):
                continue
            fstate = state_from_coords(flat, flon) or state
            if fstate not in INDIA_STATES:
                continue
            if not any(haversine_m(flat, flon, la, lo) < 50 for la, lo in seen):
                name = normalise_name(r.get("display_name", "").split(",")[0]) or "CNG Station"
                records.append({"name": name, "latitude": round(flat, 6), "longitude": round(flon, 6),
                                 "city": city, "state": fstate, "source": SOURCE})
                seen.append((flat, flon))

    print(f"[NOM-GRID] Total: {len(records)}")
    save_raw(SOURCE, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
