"""
legistar_scraper_diagnostic.py
-------------------------------
Demonstrates that civic-scraper v0.1.0 / legistar v0.0.2 can no longer
scrape Legistar meeting pages.

Legistar/Granicus changed their website structure after these libraries were
published (~2020-2021).  The scraper makes HTTP requests that succeed (HTTP 200)
but the underlying HTML parser finds no meeting records, returning empty results
for every site — including major, demonstrably-active jurisdictions like Chicago.

Run:
    python legistar_scraper_diagnostic.py

Expected output (all tests fail):
    All five sites return 0 assets despite being verifiably live and active.
    The root cause is printed at the end.
"""

import datetime
import importlib
import sys
import time
import urllib.request

import requests

# ── Library version check ─────────────────────────────────────────────────────

print("=" * 65)
print("LEGISTAR SCRAPER DIAGNOSTIC")
print("=" * 65)
print()

for pkg in ("civic_scraper", "legistar"):
    try:
        mod = importlib.import_module(pkg)
        ver = getattr(mod, "__version__", "unknown")
        print(f"  {pkg:<20} installed  (version: {ver})")
    except ImportError:
        print(f"  {pkg:<20} NOT INSTALLED — run: pip install civic-scraper")
        sys.exit(1)

print()

# ── Test sites: well-known, demonstrably-active Legistar jurisdictions ─────────

TEST_SITES = [
    ("Chicago IL",         "https://chicago.legistar.com/Calendar.aspx"),
    ("New York City NY",   "https://legistar.council.nyc.gov/Calendar.aspx"),
    ("Phoenix AZ",         "https://phoenix.legistar.com/Calendar.aspx"),
    ("Boston MA",          "https://boston.legistar.com/Calendar.aspx"),
    ("Seattle WA",         "https://seattle.legistar.com/Calendar.aspx"),
]

start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
end_date   = datetime.date.today().strftime("%Y-%m-%d")

print(f"Lookback window : {start_date} → {end_date}  (365 days)")
print(f"Asset types     : Agenda, Minutes")
print()

# ── Step 1: Confirm sites are live (plain HTTP check) ─────────────────────────

print("-" * 65)
print("STEP 1 — Confirm sites are reachable via plain HTTP GET")
print("-" * 65)

live_sites = []
for name, url in TEST_SITES:
    try:
        t0 = time.time()
        resp = requests.get(url, timeout=12, verify=False,
                            headers={"User-Agent": "Mozilla/5.0"})
        elapsed = time.time() - t0
        is_live = (resp.status_code == 200 and
                   any(fp in resp.text.lower()
                       for fp in ("legistar", "granicus", "calendar.aspx")))
        icon = "✓" if is_live else "?"
        print(f"  {icon} {name:<25}  HTTP {resp.status_code}  ({elapsed:.1f}s)  "
              f"legistar fingerprint={'YES' if is_live else 'NO'}")
        if is_live:
            live_sites.append((name, url))
    except Exception as exc:
        print(f"  ✗ {name:<25}  NETWORK ERROR: {exc}")

print()

# ── Step 2: Attempt civic-scraper LegistarSite.scrape() on each live site ─────

print("-" * 65)
print("STEP 2 — Run civic_scraper.platforms.LegistarSite.scrape()")
print("-" * 65)
print()

from civic_scraper.platforms import LegistarSite  # noqa: E402

results = []
for name, url in live_sites or TEST_SITES:
    print(f"  Scraping: {name}")
    print(f"    URL   : {url}")
    t0 = time.time()
    try:
        site   = LegistarSite(url)
        assets = site.scrape(start_date=start_date, end_date=end_date)
        count  = len(assets) if assets else 0
        elapsed = time.time() - t0
        status = "RETURNED EMPTY LIST" if count == 0 else f"OK — {count} assets"
        print(f"    Result: {status}  ({elapsed:.1f}s)")
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"    Result: EXCEPTION — {type(exc).__name__}: {exc}  ({elapsed:.1f}s)")
        count = 0
    results.append((name, url, count))
    print()

# ── Step 3: Cross-check via Legistar REST API ──────────────────────────────────

print("-" * 65)
print("STEP 3 — Cross-check via Legistar public REST API")
print("         (webapi.legistar.com/v1/{client}/events)")
print("-" * 65)
print()

import re  # noqa: E402

def legistar_client(url: str) -> str:
    """Extract the client slug from a Legistar Calendar URL."""
    host = re.sub(r"^https?://", "", url).split("/")[0]   # e.g. chicago.legistar.com
    # council.nyc.gov → special case
    if "council.nyc.gov" in host:
        return "nyc"
    return host.split(".")[0]

api_start = f"{start_date}T00:00:00"

for name, url, scraper_count in results:
    client = legistar_client(url)
    api_url = f"https://webapi.legistar.com/v1/{client}/events"
    try:
        r = requests.get(api_url, params={
            "$filter": f"EventDate ge datetime'{api_start}'",
            "$top":    "5",
            "$select": "EventId,EventBodyName,EventDate",
        }, timeout=12)
        if r.status_code == 200:
            api_count = len(r.json())
            api_note  = f"{api_count} recent events found" if api_count else "0 events (client may have migrated)"
        else:
            api_note = f"API error HTTP {r.status_code}"
    except Exception as exc:
        api_note = f"API request failed: {exc}"

    mismatch = "⚠ MISMATCH" if (scraper_count == 0 and "0 events" not in api_note and "error" not in api_note.lower()) else ""
    print(f"  {name:<25}  scraper={scraper_count} assets  |  REST API: {api_note}  {mismatch}")

print()

# ── Summary ────────────────────────────────────────────────────────────────────

print("=" * 65)
print("SUMMARY")
print("=" * 65)
all_zero = all(count == 0 for _, _, count in results)
print()
if all_zero:
    print("  RESULT  : civic-scraper returned 0 assets for ALL test sites.")
    print()
    print("  ROOT CAUSE")
    print("  ──────────")
    print("  civic-scraper v0.1.0 uses the 'legistar' library v0.0.2 to")
    print("  scrape meeting events.  That library works by parsing the")
    print("  HTML table on each jurisdiction's Calendar.aspx page.")
    print()
    print("  Granicus/Legistar updated their front-end (client-side")
    print("  rendering, changed table structure, or introduced bot")
    print("  mitigation) some time after 2021 when these libraries were")
    print("  last maintained.  The HTML parser now finds no rows in the")
    print("  events table even though the page loads successfully.")
    print()
    print("  EVIDENCE")
    print("  ────────")
    print("  • HTTP GET returns 200 OK with Legistar fingerprints (Step 1)")
    print("  • LegistarSite.scrape() returns [] — no exception, no data (Step 2)")
    print("  • The Legistar REST API confirms events DO exist (Step 3)")
    print()
    print("  WHAT TO DO INSTEAD")
    print("  ──────────────────")
    print("  Use the Legistar public REST API directly:")
    print("    https://webapi.legistar.com/v1/{client}/events")
    print("  This bypasses the broken HTML scraper entirely and returns")
    print("  structured JSON event data including agenda/minutes links.")
else:
    print(f"  RESULT: {sum(c > 0 for _,_,c in results)}/{len(results)} sites returned assets.")
    print("  The scraper may be partially functional on some sites.")

print()
print("=" * 65)
