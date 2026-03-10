#!/usr/bin/env python3
"""Fetch and normalize CNG stations in India from Overpass API."""

from __future__ import annotations

import json
import math
import pathlib
import urllib.error
import urllib.request
from dataclasses import dataclass

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUTPUT_PATH = pathlib.Path(__file__).resolve().parents[1] / "dataset" / "stations.json"

OVERPASS_QUERY = """
[out:json][timeout:300];
(
  node["amenity"="fuel"]["fuel:cng"="yes"](6.5,68.0,37.5,97.5);
  way["amenity"="fuel"]["fuel:cng"="yes"](6.5,68.0,37.5,97.5);
  relation["amenity"="fuel"]["fuel:cng"="yes"](6.5,68.0,37.5,97.5);
);
out center tags;
"""


@dataclass(frozen=True)
class Station:
    id: str
    name: str
    latitude: float
    longitude: float
    city: str
    state: str

    def to_json(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "city": self.city,
            "state": self.state,
        }


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def fetch_overpass() -> list[dict[str, object]]:
    payload = OVERPASS_QUERY.encode("utf-8")
    req = urllib.request.Request(
        OVERPASS_URL,
        data=payload,
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    with urllib.request.urlopen(req, timeout=360) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data.get("elements", [])


def normalize(elements: list[dict[str, object]]) -> list[Station]:
    normalized: list[Station] = []

    for element in elements:
        tags = element.get("tags", {}) or {}
        lat = element.get("lat")
        lon = element.get("lon")

        if lat is None or lon is None:
            center = element.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        if lat is None or lon is None:
            continue

        lat = float(lat)
        lon = float(lon)
        name = (tags.get("name") or "CNG Station").strip()
        city = (
            tags.get("addr:city")
            or tags.get("is_in:city")
            or tags.get("addr:suburb")
            or ""
        ).strip()
        state = (tags.get("addr:state") or tags.get("is_in:state") or "").strip()

        station = Station(
            id=f"osm-{element.get('type', 'node')}-{element.get('id', '0')}",
            name=name,
            latitude=lat,
            longitude=lon,
            city=city,
            state=state,
        )

        if _is_duplicate(station, normalized):
            continue
        normalized.append(station)

    normalized.sort(key=lambda s: (s.state, s.city, s.name))
    return normalized


def _is_duplicate(candidate: Station, existing: list[Station]) -> bool:
    for station in existing:
        if haversine_meters(
            candidate.latitude,
            candidate.longitude,
            station.latitude,
            station.longitude,
        ) <= 200:
            return True
    return False


def save(stations: list[Station]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump([station.to_json() for station in stations], f, indent=2)
        f.write("\n")


def main() -> int:
    try:
        elements = fetch_overpass()
        stations = normalize(elements)
        save(stations)
        print(f"Saved {len(stations)} stations to {OUTPUT_PATH}")
        return 0
    except urllib.error.URLError as exc:
        print(f"Network error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
