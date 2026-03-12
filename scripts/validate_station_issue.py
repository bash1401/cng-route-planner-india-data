#!/usr/bin/env python3
"""
validate_station_issue.py
─────────────────────────
Called by GitHub Actions when a new `new-station` issue is opened.

ENV variables injected by the workflow:
  ISSUE_NUMBER    – GitHub issue number
  ISSUE_BODY      – Full issue body text
  ISSUE_AUTHOR    – GitHub login of the submitter
  GITHUB_TOKEN    – For posting comments and adding labels
  GITHUB_REPO     – owner/repo  (e.g. bash1401/cng-route-planner-india-data)
"""

import json, math, os, re, sys, time, urllib.error, urllib.request

# ── Constants ──────────────────────────────────────────────────────────────────

INDIA_BOUNDS = dict(lat_min=6.5, lat_max=37.5, lon_min=68.0, lon_max=97.5)
DUPLICATE_RADIUS_M   = 300   # closer than this → duplicate
OVERPASS_RADIUS_M    = 500   # OSM cross-check radius
OVERPASS_API         = "https://overpass-api.de/api/interpreter"
DATASET_PATH         = "dataset/stations.json"
PENDING_PATH         = "pending/pending_stations.json"

# ── Helpers ────────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def gh_request(method, path, body=None, token=None):
    url = f"https://api.github.com{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"GitHub API error {e.code}: {e.read().decode()[:300]}")
        return None


def overpass_has_cng_nearby(lat, lon, radius_m):
    """Return True if OSM already has a CNG fuel station within radius_m."""
    query = f"""
[out:json][timeout:30];
(
  node["amenity"="fuel"]["fuel:cng"="yes"](around:{radius_m},{lat},{lon});
  way["amenity"="fuel"]["fuel:cng"="yes"](around:{radius_m},{lat},{lon});
);
out count;
"""
    try:
        req = urllib.request.Request(
            OVERPASS_API,
            data=query.encode(),
            method="POST",
            headers={"Content-Type": "text/plain"},
        )
        with urllib.request.urlopen(req, timeout=40) as r:
            data = json.loads(r.read())
            count = data.get("elements", [{}])[0].get("tags", {}).get("total", "0")
            return int(count) > 0
    except Exception as e:
        print(f"Overpass check failed: {e}")
        return None  # Unknown — don't penalise


def parse_field(body, field_id):
    """Extract answer from GitHub issue form body by field heading."""
    # GitHub renders YAML forms as "### Heading\n\nValue" blocks
    patterns = [
        rf"###\s+{re.escape(field_id)}\s*\n+(.+?)(?=\n###|\Z)",
        rf"\*\*{re.escape(field_id)}\*\*\s*\n+(.+?)(?=\n\*\*|\Z)",
    ]
    for pattern in patterns:
        m = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(1).strip()
    return ""


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    issue_number = int(os.environ["ISSUE_NUMBER"])
    issue_body   = os.environ["ISSUE_BODY"]
    issue_author = os.environ.get("ISSUE_AUTHOR", "unknown")
    token        = os.environ["GITHUB_TOKEN"]
    repo         = os.environ["GITHUB_REPO"]

    print(f"Validating issue #{issue_number} by @{issue_author}")

    # ── Parse fields ──────────────────────────────────────────────────────────
    name    = parse_field(issue_body, "Station Name")
    lat_str = parse_field(issue_body, "Latitude")
    lon_str = parse_field(issue_body, "Longitude")
    city    = parse_field(issue_body, "City / Town")
    state   = parse_field(issue_body, "State")

    errors   = []
    warnings = []

    # ── Validate lat/lon ──────────────────────────────────────────────────────
    try:
        lat = float(lat_str.replace(",", "."))
        lon = float(lon_str.replace(",", "."))
    except (ValueError, AttributeError):
        errors.append(f"❌ **Invalid coordinates**: `{lat_str}`, `{lon_str}` — must be decimal numbers.")
        _post_result(token, repo, issue_number, errors, warnings, label="invalid-data")
        return

    if not (INDIA_BOUNDS["lat_min"] <= lat <= INDIA_BOUNDS["lat_max"]
            and INDIA_BOUNDS["lon_min"] <= lon <= INDIA_BOUNDS["lon_max"]):
        errors.append(
            f"❌ **Out of India bounds**: ({lat:.4f}, {lon:.4f}). "
            f"Expected lat {INDIA_BOUNDS['lat_min']}–{INDIA_BOUNDS['lat_max']}, "
            f"lon {INDIA_BOUNDS['lon_min']}–{INDIA_BOUNDS['lon_max']}."
        )
        _post_result(token, repo, issue_number, errors, warnings, label="out-of-bounds")
        return

    # ── Duplicate check against existing dataset ──────────────────────────────
    existing = []
    if os.path.exists(DATASET_PATH):
        with open(DATASET_PATH) as f:
            existing = json.load(f)

    for s in existing:
        dist = haversine(lat, lon, s["latitude"], s["longitude"])
        if dist < DUPLICATE_RADIUS_M:
            errors.append(
                f"❌ **Duplicate**: Station **{s['name']}** is only "
                f"**{int(dist)} m** away (ID: `{s['id']}`). "
                f"This station may already be on the map at ({s['latitude']}, {s['longitude']})."
            )
            _post_result(token, repo, issue_number, errors, warnings, label="duplicate")
            return

    # ── Check pending queue (another user already submitted this) ─────────────
    pending_list = []
    if os.path.exists(PENDING_PATH):
        with open(PENDING_PATH) as f:
            pending_list = json.load(f)

    already_pending = None
    for p in pending_list:
        dist = haversine(lat, lon, p["latitude"], p["longitude"])
        if dist < DUPLICATE_RADIUS_M:
            already_pending = p
            break

    # ── Overpass cross-check ──────────────────────────────────────────────────
    osm_verified = overpass_has_cng_nearby(lat, lon, OVERPASS_RADIUS_M)
    if osm_verified is True:
        warnings.append(
            f"✅ **OSM Cross-check**: OpenStreetMap already has a CNG station within "
            f"{OVERPASS_RADIUS_M} m of this location — this submission is **highly likely correct**."
        )
    elif osm_verified is False:
        warnings.append(
            f"⚠️ **OSM Cross-check**: No CNG station found in OpenStreetMap within "
            f"{OVERPASS_RADIUS_M} m. This does **not** mean the station is wrong — "
            f"OSM data is incomplete. Requires standard 3-report consensus."
        )
    else:
        warnings.append("⚠️ **OSM Cross-check**: Overpass API was unreachable; check skipped.")

    # ── Update pending list ───────────────────────────────────────────────────
    if already_pending:
        if issue_author not in already_pending.get("reporters", []):
            already_pending["reporters"].append(issue_author)
        already_pending["issue_numbers"].append(issue_number)
        reporters_count = len(set(already_pending["reporters"]))
        warnings.append(
            f"📊 **Confirmation progress**: **{reporters_count}/3** unique reporters "
            f"have confirmed this station. "
            + ("🎉 **Threshold reached! This station will be added in the next consensus run.**"
               if reporters_count >= 3 else
               f"Need {3 - reporters_count} more.")
        )
    else:
        pending_list.append({
            "temp_id": f"pending-{issue_number}",
            "name": name or "CNG Station",
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "city": city,
            "state": state,
            "reporters": [issue_author],
            "issue_numbers": [issue_number],
            "osm_verified": osm_verified,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        warnings.append(
            "📊 **Confirmation progress**: **1/3** unique reporters. "
            "Need 2 more independent confirmations before this is added to the map."
        )

    # ── Save updated pending list ─────────────────────────────────────────────
    os.makedirs("pending", exist_ok=True)
    with open(PENDING_PATH, "w") as f:
        json.dump(pending_list, f, indent=2)
        f.write("\n")

    _post_result(token, repo, issue_number, errors, warnings, label="validated")


def _post_result(token, repo, issue_number, errors, warnings, label):
    all_ok = len(errors) == 0
    emoji  = "✅" if all_ok else "❌"
    status = "Validation passed" if all_ok else "Validation failed"

    lines = [
        f"## {emoji} {status}",
        "",
        "**Automated validation result** — CNG Route Planner India Bot",
        "",
    ]
    if errors:
        lines += ["### Issues found", ""] + errors + [""]
    if warnings:
        lines += ["### Notes", ""] + warnings + [""]

    if all_ok:
        lines += [
            "---",
            "> This station will be added to the map automatically once **3 unique users** confirm the same location.",
            "> Thank you for contributing! 🙏",
        ]
    else:
        lines += [
            "---",
            "> Please correct the issues above and submit a new issue. This issue has been closed.",
        ]

    body = "\n".join(lines)

    gh_request("POST", f"/repos/{repo}/issues/{issue_number}/comments",
               {"body": body}, token)

    # Apply label
    existing_labels = gh_request("GET", f"/repos/{repo}/issues/{issue_number}/labels",
                                 token=token) or []
    current = {l["name"] for l in existing_labels}
    current.discard("pending-validation")
    current.add(label)
    gh_request("PUT", f"/repos/{repo}/issues/{issue_number}/labels",
               {"labels": list(current)}, token)

    # Close invalid issues
    if not all_ok:
        gh_request("PATCH", f"/repos/{repo}/issues/{issue_number}",
                   {"state": "closed", "state_reason": "not_planned"}, token)

    print(f"Result posted: {status} (label={label})")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
