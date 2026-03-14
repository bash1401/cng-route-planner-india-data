"""
Fetch CNG stations from GAIL Gas published PDF.

Source: GAILGas-ExistingCNGStations.pdf (hosted on S3)
Extracts table data using pdfplumber, which correctly handles the 8-column table.
Saves to raw_sources/gail_pdf.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from utils import save_raw, state_from_coords, INDIA_STATES, normalise_name

SOURCE = "gail_pdf"

PDF_URL = "https://gailgaspdfdownloads.s3.ap-south-1.amazonaws.com/GAILGas-ExistingCNGStations.pdf"
PDF_FALLBACK_URL = "https://www.gailgas.com/wp-content/uploads/GAILGas-ExistingCNGStations.pdf"
_CACHE = "/tmp/gail_cng_stations.pdf"


def _download_pdf() -> str | None:
    for url in [PDF_URL, PDF_FALLBACK_URL]:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                content = r.read()
            if content[:4] == b"%PDF":
                with open(_CACHE, "wb") as f:
                    f.write(content)
                print(f"[GAIL-PDF] Downloaded {len(content):,} bytes from {url}")
                return _CACHE
        except Exception as exc:
            print(f"[GAIL-PDF] Download failed ({url}): {exc}")

    # Try the already-downloaded file at /tmp/gail_cng.pdf
    if os.path.exists("/tmp/gail_cng.pdf"):
        print("[GAIL-PDF] Using cached /tmp/gail_cng.pdf")
        return "/tmp/gail_cng.pdf"

    return None


def _parse_pdf(path: str) -> list[dict]:
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        print("[GAIL-PDF] pdfplumber not installed. Run: pip3 install pdfplumber")
        return []

    records: list[dict] = []

    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 8:
                        continue
                    sr = (row[0] or "").strip()
                    if not sr or not re.match(r"^\d+$", sr):
                        continue

                    name = (row[4] or "").strip().replace("\n", " ")
                    address = (row[5] or "").strip().replace("\n", " ")
                    area = (row[1] or "").strip().replace("\n", " ")
                    lat_str = (row[6] or "").strip()
                    lon_str = (row[7] or "").strip()

                    try:
                        lat = float(lat_str)
                        lon = float(lon_str)
                    except ValueError:
                        continue

                    if not (6.0 <= lat <= 38.0 and 68.0 <= lon <= 98.0):
                        continue

                    state = state_from_coords(lat, lon)
                    if not state or state not in INDIA_STATES:
                        continue

                    name = normalise_name(name) or "GAIL Gas CNG Station"

                    records.append({
                        "name": name,
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "city": area or "",
                        "state": state,
                        "source": SOURCE,
                    })

    print(f"[GAIL-PDF] Extracted {len(records)} stations from PDF")
    return records


def main() -> int:
    path = _download_pdf()
    if not path:
        print("[GAIL-PDF] Could not obtain PDF, skipping.")
        save_raw(SOURCE, [])
        return 0

    records = _parse_pdf(path)
    save_raw(SOURCE, records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
