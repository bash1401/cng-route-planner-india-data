#!/usr/bin/env python3
"""
process_consensus.py
────────────────────
Runs daily (and on-demand) to check if any pending stations have reached
the 3-reporter threshold and should be added to the live dataset.

Also handles station removal reports (5-reporter threshold for flagging).

ENV variables:
  GITHUB_TOKEN  – For posting comments and closing issues
  GITHUB_REPO   – owner/repo
"""

import json, math, os, re, time, urllib.error, urllib.request

DATASET_PATH        = "dataset/stations.json"
PENDING_PATH        = "pending/pending_stations.json"
REPORTS_PATH        = "pending/station_reports.json"
NEW_THRESHOLD       = 3   # unique reporters to ADD a station
REMOVE_THRESHOLD    = 5   # unique reporters to FLAG a station for removal


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
        print(f"GitHub API error {e.code}: {e.read().decode()[:200]}")
        return None


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


# ── Process new station additions ─────────────────────────────────────────────

def process_new_stations(token, repo):
    pending  = load_json(PENDING_PATH, [])
    dataset  = load_json(DATASET_PATH, [])
    approved = []
    remaining = []

    for entry in pending:
        reporters = list(set(entry.get("reporters", [])))
        count = len(reporters)

        if count >= NEW_THRESHOLD:
            # Generate a proper ID
            station_id = (
                f"community-{entry['name'].lower().replace(' ', '-')[:20]}"
                f"-{int(entry['latitude']*100)}-{int(entry['longitude']*100)}"
            )
            new_station = {
                "id": station_id,
                "name": entry["name"],
                "latitude": entry["latitude"],
                "longitude": entry["longitude"],
                "city": entry.get("city", ""),
                "state": entry.get("state", ""),
                "source": "community",
                "verified_by": reporters,
                "added_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            # Final duplicate check before adding
            is_dup = any(
                haversine(new_station["latitude"], new_station["longitude"],
                          s["latitude"], s["longitude"]) < 300
                for s in dataset
            )
            if is_dup:
                print(f"Skipping duplicate on final check: {new_station['name']}")
                for inum in entry.get("issue_numbers", []):
                    gh_request(
                        "POST", f"/repos/{repo}/issues/{inum}/comments",
                        {"body": (
                            "⚠️ **Auto-closed**: A station at this location "
                            "was added to the dataset by another submission first. "
                            "Thank you for contributing!"
                        )},
                        token,
                    )
                    gh_request("PATCH", f"/repos/{repo}/issues/{inum}",
                               {"state": "closed", "state_reason": "completed"}, token)
                continue

            dataset.append(new_station)
            approved.append(new_station)

            # Close all related issues with a thank-you comment
            for inum in entry.get("issue_numbers", []):
                reporters_str = ", ".join(f"@{r}" for r in reporters)
                gh_request(
                    "POST", f"/repos/{repo}/issues/{inum}/comments",
                    {"body": (
                        f"## ✅ Station Added!\n\n"
                        f"**{new_station['name']}** has been added to the CNG Route Planner India map.\n\n"
                        f"**Confirmed by**: {reporters_str}\n\n"
                        f"**Location**: {new_station['latitude']}, {new_station['longitude']}\n\n"
                        f"The station will appear in the app within 24 hours (next dataset refresh).\n\n"
                        f"Thank you for contributing to a more complete CNG network map! 🙏"
                    )},
                    token,
                )
                gh_request(
                    "PUT", f"/repos/{repo}/issues/{inum}/labels",
                    {"labels": ["new-station", "approved", "added-to-dataset"]},
                    token,
                )
                gh_request("PATCH", f"/repos/{repo}/issues/{inum}",
                           {"state": "closed", "state_reason": "completed"}, token)

            print(f"✅ Added station: {new_station['name']} ({new_station['latitude']}, {new_station['longitude']})")
        else:
            remaining.append(entry)

    if approved:
        # Sort dataset
        dataset.sort(key=lambda s: (s.get("state",""), s.get("city",""), s["name"]))
        save_json(DATASET_PATH, dataset)
        print(f"Dataset updated: {len(approved)} station(s) added. Total: {len(dataset)}")

    save_json(PENDING_PATH, remaining)
    return len(approved)


# ── Process station removal reports ───────────────────────────────────────────

def process_reports(token, repo):
    reports  = load_json(REPORTS_PATH, [])
    dataset  = load_json(DATASET_PATH, [])
    updated  = False
    remaining = []

    for report in reports:
        reporters = list(set(report.get("reporters", [])))
        count = len(reporters)

        if count >= REMOVE_THRESHOLD:
            station_id = report.get("station_id")
            issue_type = report.get("issue_type", "")

            # Find the station
            idx = next(
                (i for i, s in enumerate(dataset) if s["id"] == station_id),
                None,
            )
            if idx is None:
                # Already removed or ID changed
                for inum in report.get("issue_numbers", []):
                    gh_request("PATCH", f"/repos/{repo}/issues/{inum}",
                               {"state": "closed", "state_reason": "not_planned"}, token)
                continue

            station = dataset[idx]
            reporters_str = ", ".join(f"@{r}" for r in reporters)

            if "permanently closed" in issue_type or "does not dispense CNG" in issue_type:
                # Remove it
                dataset.pop(idx)
                updated = True
                action = "removed from"
                label  = "station-removed"
            elif "wrong" in issue_type.lower() or "coordinates" in issue_type.lower():
                # Flag it but don't remove — needs manual review
                dataset[idx]["flagged"] = True
                dataset[idx]["flag_reason"] = issue_type
                dataset[idx]["flag_reporters"] = reporters
                updated = True
                action = "flagged for review in"
                label  = "flagged-wrong-location"
            else:
                dataset[idx]["flagged"] = True
                dataset[idx]["flag_reason"] = issue_type
                dataset[idx]["flag_reporters"] = reporters
                updated = True
                action = "flagged in"
                label  = "flagged-review"

            for inum in report.get("issue_numbers", []):
                gh_request(
                    "POST", f"/repos/{repo}/issues/{inum}/comments",
                    {"body": (
                        f"## 🔄 Report Processed\n\n"
                        f"Station **{station['name']}** has been **{action}** the dataset.\n\n"
                        f"**Reported by**: {reporters_str}\n\n"
                        f"Changes will appear in the app within 24 hours.\n\n"
                        f"Thank you for helping maintain accuracy! 🙏"
                    )},
                    token,
                )
                gh_request(
                    "PUT", f"/repos/{repo}/issues/{inum}/labels",
                    {"labels": ["station-report", "processed", label]},
                    token,
                )
                gh_request("PATCH", f"/repos/{repo}/issues/{inum}",
                           {"state": "closed", "state_reason": "completed"}, token)

            print(f"Processed report for station: {station['name']} → {action}")
        else:
            remaining.append(report)

    if updated:
        dataset.sort(key=lambda s: (s.get("state",""), s.get("city",""), s["name"]))
        save_json(DATASET_PATH, dataset)

    save_json(REPORTS_PATH, remaining)


# ── Summary comment on the repo ───────────────────────────────────────────────

def post_summary(token, repo, added_count, pending):
    if added_count == 0 and not pending:
        return
    osm_count = sum(1 for p in pending if p.get("osm_verified"))
    msg = (
        f"## 🗺️ Consensus Run Summary — {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}\n\n"
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Stations added this run | **{added_count}** |\n"
        f"| Pending (need more confirmations) | **{len(pending)}** |\n"
        f"| Pending with OSM cross-verification | **{osm_count}** |\n\n"
        + (f"**Pending stations still need {3} - (current reports) more confirmations to be added.**\n"
           if pending else "")
    )
    # Create a discussion or commit comment — here we just print
    print(msg)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    token = os.environ.get("GITHUB_TOKEN", "")
    repo  = os.environ.get("GITHUB_REPO", "")

    added = process_new_stations(token, repo)
    process_reports(token, repo)

    pending = load_json(PENDING_PATH, [])
    post_summary(token, repo, added, pending)
    print("Consensus run complete.")


if __name__ == "__main__":
    main()
