import csv
import time
import re
import sys
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.olx.in/items/q-car-cover"
OUTPUT_CSV = "olx_car_covers.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9",
}

def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
        else:
            print(f"[warn] HTTP {r.status_code} for {url}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return None

def extract_listings(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Primary: anchors that look like item pages
    cards = soup.select('a[href*="/item/"], a[data-aut-id="itemTitle"]')
    results = []
    seen: Set[str] = set()

    for a in cards:
        href = a.get("href")
        if not href:
            continue
        # Normalize URL
        url = urljoin("https://www.olx.in", href)

        if url in seen:
            continue

        # Title
        title = a.get_text(" ", strip=True) or None

        # Try to get nearby text for price/location
        price = None
        location = None
        snippet = None

        parent = a
        # Bubble up a few levels to collect context
        for _ in range(3):
            if parent and parent.parent:
                parent = parent.parent
            else:
                break

        if parent:
            text = parent.get_text(" ", strip=True)
            snippet = text[:300] if text else None

            # crude price extraction (₹ 1,234 etc.)
            m = re.search(r"₹\s?[\d,]+", text)
            if m:
                price = m.group(0)

            # heuristic for location: look for patterns like "in <place>" or "• <place>"
            m2 = re.search(r"(?:in|at|•)\s+([A-Z][A-Za-z .,-]{2,})", text)
            if m2:
                location = m2.group(1).strip()

        results.append({
            "title": title,
            "url": url,
            "price_guess": price,
            "location_guess": location,
            "snippet": snippet
        })
        seen.add(url)

    # Deduplicate by URL, keep first occurrence
    return results

def next_page_url(current_page: int) -> str:
    if current_page <= 1:
        return BASE_URL
    return f"{BASE_URL}?page={current_page}"

def scrape(max_pages: int = 5, delay_sec: float = 1.5) -> List[Dict]:
    session = requests.Session()
    all_rows: List[Dict] = []
    seen_urls: Set[str] = set()

    for page_num in range(1, max_pages + 1):
        url = next_page_url(page_num)
        print(f"[info] Fetching page {page_num}: {url}")
        html = fetch_page(url, session=session)
        if not html:
            print("[warn] No HTML fetched, stopping.")
            break

        rows = extract_listings(html)
        # filter new
        new_rows = [r for r in rows if r["url"] not in seen_urls]
        if not new_rows:
            print("[info] No new rows found; stopping.")
            break

        all_rows.extend(new_rows)
        seen_urls.update(r["url"] for r in new_rows)
        time.sleep(delay_sec)

    return all_rows

def save_csv(rows: List[Dict], path: str = OUTPUT_CSV) -> None:
    if not rows:
        print("[info] No rows to save.")
        return
    fieldnames = ["title", "url", "price_guess", "location_guess", "snippet"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[info] Saved {len(rows)} rows to {path}")

if __name__ == "__main__":
    # Optional: accept max_pages from CLI
    max_pages = 5
    if len(sys.argv) >= 2:
        try:
            max_pages = int(sys.argv[1])
        except:
            pass

    rows = scrape(max_pages=max_pages, delay_sec=1.8)
    save_csv(rows, OUTPUT_CSV)
    print("[done]")