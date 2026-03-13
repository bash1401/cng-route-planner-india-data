#!/usr/bin/env python3
"""
Selenium-based scraper for JavaScript-heavy operator station locators.

Requires: selenium, webdriver-manager
  pip install selenium webdriver-manager

In GitHub Actions this runs with headless Chrome.
Targets: IGL, MGL, ATGL, Gujarat Gas (all use JS-rendered station finders).

Run:  python fetch_operators_selenium.py [igl|mgl|atgl|gujgas|all]
"""

from __future__ import annotations

import json
import re
import sys
import time
from typing import Optional
from utils import save_raw, state_from_coords, normalise_name

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException,
    )
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        _USE_WDM = True
    except ImportError:
        _USE_WDM = False
    _SELENIUM_OK = True
except ImportError:
    _SELENIUM_OK = False


def _make_driver() -> "webdriver.Chrome":
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,800")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36"
    )
    if _USE_WDM:
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    return webdriver.Chrome(options=opts)


# ─── IGL scraper ─────────────────────────────────────────────────────────────

IGL_URL = "https://www.iglonline.net/find-cng-station/"

IGL_CITIES = [
    "Delhi", "Noida", "Greater Noida", "Gurgaon", "Faridabad",
    "Ghaziabad", "Muzaffarnagar", "Meerut", "Hapur",
    "Kaithal", "Karnal", "Fatehabad", "Rewari",
    "Ajmer", "Pilibhit", "Muradnagar",
]


def scrape_igl(driver: "webdriver.Chrome") -> list[dict]:
    stations = []
    seen: set[str] = set()

    driver.get(IGL_URL)
    time.sleep(3)

    # Try to find city dropdown
    for city in IGL_CITIES:
        try:
            wait = WebDriverWait(driver, 5)
            # Try selecting from dropdown
            try:
                sel = Select(driver.find_element(By.CSS_SELECTOR, "select[name*=city], select#city, select.city-select"))
                sel.select_by_visible_text(city)
            except (NoSuchElementException, Exception):
                # Try clicking a city link or button
                try:
                    btn = driver.find_element(By.XPATH, f"//option[contains(text(),'{city}')]/..")
                    Select(btn).select_by_visible_text(city)
                except Exception:
                    continue

            # Submit
            try:
                driver.find_element(
                    By.CSS_SELECTOR, "input[type=submit], button[type=submit]"
                ).click()
            except Exception:
                pass

            time.sleep(2)

            # Extract stations from the page
            page_src = driver.page_source
            city_stations = _parse_igl_html(page_src, city, seen)
            stations.extend(city_stations)
            print(f"  IGL {city}: {len(city_stations)} stations")
            time.sleep(1)

        except Exception as exc:
            print(f"  IGL {city}: error — {exc}")

    return stations


def _parse_igl_html(html: str, city: str, seen: set) -> list[dict]:
    stations = []

    # Pattern: Google Maps API markers with lat/lng
    pairs = re.findall(
        r'(?:lat|latitude)\s*[=:,]\s*(2[5-9]\.\d+).*?'
        r'(?:lng|lon|longitude)\s*[=:,]\s*(7[5-9]\.\d+)',
        html, re.DOTALL | re.IGNORECASE,
    )
    names = re.findall(
        r'(?:title|name|infowindow|content)\s*[=:]\s*["\']([^"\']{5,80})',
        html, re.IGNORECASE,
    )

    for i, (lat_s, lon_s) in enumerate(pairs):
        try:
            lat, lon = float(lat_s), float(lon_s)
            key = f"{round(lat,4)},{round(lon,4)}"
            if key in seen:
                continue
            seen.add(key)
            name = names[i] if i < len(names) else f"IGL CNG Station {city}"
            stations.append({
                "id": f"igl-{key}",
                "name": normalise_name(name),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "city": city,
                "state": state_from_coords(lat, lon) or "Delhi",
                "source": "igl",
                "address": "",
                "operator": "IGL",
            })
        except ValueError:
            pass

    # HTML table fallback
    if not stations:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        for row in soup.select("table tr")[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            lat_s = lon_s = ""
            for cell in cells:
                if not lat_s:
                    m = re.search(r'(2[5-9]\.\d{4,})', cell)
                    if m:
                        lat_s = m.group(1)
                if not lon_s:
                    m = re.search(r'(7[5-8]\.\d{4,})', cell)
                    if m:
                        lon_s = m.group(1)
            if lat_s and lon_s:
                try:
                    lat, lon = float(lat_s), float(lon_s)
                    key = f"{round(lat,4)},{round(lon,4)}"
                    if key not in seen:
                        seen.add(key)
                        stations.append({
                            "id": f"igl-{key}",
                            "name": normalise_name(cells[0]),
                            "latitude": round(lat, 6),
                            "longitude": round(lon, 6),
                            "city": city,
                            "state": state_from_coords(lat, lon) or "Delhi",
                            "source": "igl",
                            "address": " | ".join(cells[1:3]),
                            "operator": "IGL",
                        })
                except ValueError:
                    pass

    return stations


# ─── MGL scraper ─────────────────────────────────────────────────────────────

MGL_URL = "https://www.mahanagargas.com/"
MGL_LOCATOR_PATTERNS = [
    "https://www.mahanagargas.com/cng-station-locator",
    "https://www.mahanagargas.com/cng/cng-station-locator",
    "https://www.mahanagargas.com/our-business/cng/cng-station-locator",
]


def scrape_mgl(driver: "webdriver.Chrome") -> list[dict]:
    stations = []
    seen: set[str] = set()

    for url in MGL_LOCATOR_PATTERNS:
        try:
            driver.get(url)
            time.sleep(4)
            html = driver.page_source

            pairs = re.findall(
                r'(1[89]\.\d{4,}).*?(7[23]\.\d{4,})',
                html, re.DOTALL,
            )
            names = re.findall(
                r'(?:title|name|outlet|station)\s*[=:]\s*["\']([^"\']{5,60})',
                html, re.IGNORECASE,
            )

            for i, (lat_s, lon_s) in enumerate(pairs):
                try:
                    lat, lon = float(lat_s), float(lon_s)
                    key = f"{round(lat,4)},{round(lon,4)}"
                    if key in seen:
                        continue
                    seen.add(key)
                    name = names[i] if i < len(names) else "MGL CNG Station"
                    stations.append({
                        "id": f"mgl-{key}",
                        "name": normalise_name(name),
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "city": "Mumbai",
                        "state": "Maharashtra",
                        "source": "mgl",
                        "address": "",
                        "operator": "MGL",
                    })
                except ValueError:
                    pass

            if stations:
                print(f"  MGL: {len(stations)} stations from {url}")
                break

        except Exception as exc:
            print(f"  MGL {url}: {exc}")

    return stations


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(targets: list[str]) -> int:
    if not _SELENIUM_OK:
        print("  Selenium not installed. Run: pip install selenium webdriver-manager")
        print("  Saving empty files for all targets.")
        for t in (["igl", "mgl"] if "all" in targets else targets):
            save_raw(t, [])
        return 1

    driver = None
    try:
        print("  Starting headless Chrome …")
        driver = _make_driver()

        all_data: dict[str, list[dict]] = {}

        if "igl" in targets or "all" in targets:
            print("  Scraping IGL …")
            all_data["igl"] = scrape_igl(driver)
            save_raw("igl", all_data["igl"])
            print(f"  IGL total: {len(all_data['igl'])} stations")

        if "mgl" in targets or "all" in targets:
            print("  Scraping MGL …")
            all_data["mgl"] = scrape_mgl(driver)
            save_raw("mgl", all_data["mgl"])
            print(f"  MGL total: {len(all_data['mgl'])} stations")

    except WebDriverException as exc:
        print(f"  Chrome driver error: {exc}")
        return 1
    finally:
        if driver:
            driver.quit()

    return 0


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ["all"]
    raise SystemExit(main(targets))
