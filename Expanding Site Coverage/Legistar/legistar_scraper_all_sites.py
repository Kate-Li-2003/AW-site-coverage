#!/usr/bin/env python3
"""
legistar_scraper_all_sites.py
------------------------------
Attempts to scrape every site in Copy_of_Legistrar_sites.xlsx using
civic-scraper's LegistarSite, then writes results to:

    Original Sites to Verify/legistar_scrape_results_<date>.xlsx

Key notes (from Legistar_Debugging.ipynb):
  - Endpoints in the spreadsheet lack the https:// scheme — it is prepended here.
  - Some sites work, some fail with errors like KeyError: 'Meeting Detail'.
  - For each failure, the error type and message are captured and recorded.

Usage:
    python3.9 legistar_scraper_all_sites.py
    python3.9 legistar_scraper_all_sites.py --all       # include non-US (default: US+CA only)
    python3.9 legistar_scraper_all_sites.py --workers 5 # parallel workers (default: 5)
"""

import argparse
import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# Suppress the chardet warning from requests
import warnings
warnings.filterwarnings("ignore")

from civic_scraper.platforms import LegistarSite

# ── Config ────────────────────────────────────────────────────────────────────
BASE        = "Original Sites to Verify"
INPUT_FILE  = f"{BASE}/Copy_of_Legistrar_sites.xlsx"
TODAY       = datetime.date.today().strftime("%Y-%m-%d")
OUTPUT_FILE = f"{BASE}/legistar_scrape_results_{TODAY}.xlsx"

# Date window: 2 months back → 2 months forward (matches notebook defaults)
start_date = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
end_date   = (datetime.date.today() + datetime.timedelta(days=60)).strftime("%Y-%m-%d")

DEFAULT_WORKERS = 5

# ── Helper ────────────────────────────────────────────────────────────────────
def ensure_https(endpoint: str) -> str:
    """Prepend https:// if the endpoint lacks a scheme."""
    ep = str(endpoint).strip()
    if ep.startswith("http://") or ep.startswith("https://"):
        return ep
    return "https://" + ep


def scrape_site(row: dict) -> dict:
    """
    Attempt to scrape one Legistar site.
    Returns a result dict with scrape_status, asset_count, error_type, error_message.
    """
    endpoint = row.get("endpoint", "")
    url = ensure_https(endpoint)
    name = row.get("name", "")
    state = row.get("state", "")

    result = {
        "name":          name,
        "state":         state,
        "site_type":     row.get("site_type", ""),
        "aw_active":     row.get("aw_active", ""),
        "endpoint":      endpoint,
        "url_used":      url,
        "scrape_status": None,   # "success" | "empty" | "error"
        "asset_count":   0,
        "error_type":    None,
        "error_message": None,
    }

    try:
        site = LegistarSite(url, timezone="US/Eastern")
        assets = site.scrape(start_date=start_date, end_date=end_date)
        count = len(assets) if assets else 0
        result["asset_count"]   = count
        result["scrape_status"] = "success" if count > 0 else "empty"
    except Exception as exc:
        result["scrape_status"] = "error"
        result["error_type"]    = type(exc).__name__
        result["error_message"] = str(exc)[:500]   # cap length
        # Print progress immediately so long runs show activity
        print(f"  ERROR [{state}] {name}: {type(exc).__name__}: {str(exc)[:120]}")
        return result

    status_icon = "✓" if result["asset_count"] > 0 else "○"
    print(f"  {status_icon} [{state}] {name}: {result['asset_count']} assets")
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scrape all Legistar sites and record results.")
    parser.add_argument("--all", action="store_true",
                        help="Include non-US/CA sites (default: skip AB/BC/ON/QC etc.)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    args = parser.parse_args()

    print("=" * 65)
    print("LEGISTAR — FULL SITE SCRAPE")
    print("=" * 65)
    print(f"  Date window : {start_date} → {end_date}")
    print(f"  Workers     : {args.workers}")
    print()

    # Load sites
    df = pd.read_excel(INPUT_FILE, dtype=str)
    df = df.where(df.notna(), other=None)   # convert NaN → None for cleaner dicts
    total_rows = len(df)
    print(f"Loaded {total_rows} rows from {INPUT_FILE}")

    # Optionally filter to US + DC only (skip Canadian provinces unless --all)
    CANADIAN_PROVINCES = {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU",
                          "ON", "PE", "QC", "SK", "YT"}
    if not args.all:
        before = len(df)
        df = df[~df["state"].str.strip().isin(CANADIAN_PROVINCES)]
        skipped = before - len(df)
        print(f"Skipping {skipped} non-US rows (use --all to include them)")

    rows = df.to_dict(orient="records")
    print(f"Scraping {len(rows)} sites …\n")

    # Run scrapes in parallel
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(scrape_site, row): row for row in rows}
        completed = 0
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            results.append(result)
            if completed % 25 == 0:
                print(f"  … {completed}/{len(rows)} done …")

    # Build results dataframe
    out_df = pd.DataFrame(results, columns=[
        "name", "state", "site_type", "aw_active",
        "endpoint", "url_used",
        "scrape_status", "asset_count",
        "error_type", "error_message",
    ])

    # Sort: successes first, then empties, then errors; within each group by state/name
    status_order = {"success": 0, "empty": 1, "error": 2}
    out_df["_sort"] = out_df["scrape_status"].map(status_order)
    out_df = out_df.sort_values(["_sort", "state", "name"]).drop(columns="_sort").reset_index(drop=True)

    # Summary counts
    counts = out_df["scrape_status"].value_counts()
    n_success = counts.get("success", 0)
    n_empty   = counts.get("empty",   0)
    n_error   = counts.get("error",   0)

    print()
    print("=" * 65)
    print("RESULTS SUMMARY")
    print("=" * 65)
    print(f"  Total scraped : {len(out_df)}")
    print(f"  ✓ success     : {n_success}  (assets returned)")
    print(f"  ○ empty       : {n_empty}   (0 assets, no error)")
    print(f"  ✗ error       : {n_error}   (exception raised)")
    print()

    if n_error > 0:
        print("Error type breakdown:")
        for etype, cnt in out_df[out_df["scrape_status"] == "error"]["error_type"].value_counts().items():
            print(f"    {etype:<35} {cnt}")
        print()

    # Write Excel output
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Results", index=False)

        # Summary sheet
        summary_data = {
            "Metric": [
                "Run date", "Date window start", "Date window end",
                "Total sites", "Success (assets found)", "Empty (0 assets, no error)", "Error",
            ],
            "Value": [
                TODAY, start_date, end_date,
                len(out_df), n_success, n_empty, n_error,
            ],
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

    print(f"Saved results to: {OUTPUT_FILE}")
    print()


if __name__ == "__main__":
    main()
