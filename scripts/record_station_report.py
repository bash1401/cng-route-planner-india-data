#!/usr/bin/env python3
"""
record_station_report.py
────────────────────────
Records a "wrong/closed station" report into the pending reports queue.
The consensus script checks it daily and removes/flags when 5 reporters agree.
"""

import json, os, re, time, urllib.error, urllib.request

REPORTS_PATH = "pending/station_reports.json"
DATASET_PATH = "dataset/stations.json"
REMOVE_THRESHOLD = 5


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


def parse_field(body, field_id):
    patterns = [
        rf"###\s+{re.escape(field_id)}\s*\n+(.+?)(?=\n###|\Z)",
        rf"\*\*{re.escape(field_id)}\*\*\s*\n+(.+?)(?=\n\*\*|\Z)",
    ]
    for pattern in patterns:
        m = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(1).strip()
    return ""


def load_json(path, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def main():
    issue_number = int(os.environ["ISSUE_NUMBER"])
    issue_body   = os.environ["ISSUE_BODY"]
    issue_author = os.environ.get("ISSUE_AUTHOR", "unknown")
    token        = os.environ["GITHUB_TOKEN"]
    repo         = os.environ["GITHUB_REPO"]

    station_id_or_name = parse_field(issue_body, "Station ID or Name")
    issue_type         = parse_field(issue_body, "What is wrong?")

    # Try to find station by ID or name in dataset
    dataset = load_json(DATASET_PATH, [])
    matched_station = None
    for s in dataset:
        if station_id_or_name and (
            station_id_or_name.lower() in s["id"].lower()
            or station_id_or_name.lower() in s["name"].lower()
        ):
            matched_station = s
            break

    if not matched_station:
        gh_request(
            "POST", f"/repos/{repo}/issues/{issue_number}/comments",
            {"body": (
                "⚠️ **Could not find station** in the current dataset matching "
                f"`{station_id_or_name}`.\n\n"
                "This may be because:\n"
                "- The station was already removed\n"
                "- The ID or name doesn't exactly match\n\n"
                "Please check the exact station ID from the app and resubmit. Thank you!"
            )},
            token,
        )
        return

    reports = load_json(REPORTS_PATH, [])

    # Find or create report entry for this station
    existing = next(
        (r for r in reports if r.get("station_id") == matched_station["id"]),
        None,
    )

    if existing:
        if issue_author not in existing.get("reporters", []):
            existing["reporters"].append(issue_author)
        existing["issue_numbers"].append(issue_number)
        existing["last_reported"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        count = len(set(existing["reporters"]))
    else:
        entry = {
            "station_id": matched_station["id"],
            "station_name": matched_station["name"],
            "issue_type": issue_type,
            "reporters": [issue_author],
            "issue_numbers": [issue_number],
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "last_reported": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        reports.append(entry)
        count = 1

    # Acknowledge the report
    remaining = REMOVE_THRESHOLD - count
    gh_request(
        "POST", f"/repos/{repo}/issues/{issue_number}/comments",
        {"body": (
            f"## 📝 Report Recorded\n\n"
            f"Thank you for reporting **{matched_station['name']}**.\n\n"
            f"**Issue**: {issue_type}\n\n"
            f"**Confirmation progress**: {count}/{REMOVE_THRESHOLD} reporters\n\n"
            + (f"🔄 Threshold not yet reached. Need **{remaining} more** independent reports to take action.\n\n"
               if remaining > 0 else
               "✅ Threshold reached! This station will be reviewed/removed in the next consensus run.\n\n")
            + "> We require multiple independent reports before removing any station to avoid accidentally stranding CNG vehicle users."
        )},
        token,
    )

    gh_request(
        "PUT", f"/repos/{repo}/issues/{issue_number}/labels",
        {"labels": ["station-report", "recorded"]},
        token,
    )

    os.makedirs("pending", exist_ok=True)
    with open(REPORTS_PATH, "w") as f:
        json.dump(reports, f, indent=2)
        f.write("\n")

    print(f"Report recorded: {matched_station['name']} — {count}/{REMOVE_THRESHOLD} reporters")


if __name__ == "__main__":
    main()
