#!/usr/bin/env python3
"""
Quick inspection of the original and _updated AW xlsx files.
Confirms row counts, column presence, and null counts for geographic columns.
"""
import pandas as pd

ORIG = "Original Sites to Verify/Copy of AW_civic_scraper_sites.xlsx"
UPDT = "Original Sites to Verify/Copy of AW_civic_scraper_sites_updated.xlsx"

orig = pd.read_excel(ORIG)
updt = pd.read_excel(UPDT)

print("=== ROW / COLUMN COUNTS ===")
print(f"Original : {len(orig)} rows, {len(orig.columns)} cols")
print(f"Updated  : {len(updt)} rows, {len(updt.columns)} cols")

print("\n=== COLUMNS IN ORIGINAL ===")
print(list(orig.columns))

print("\n=== COLUMNS IN UPDATED ===")
print(list(updt.columns))

GEO_COLS = ['gov_level', 'state_fips', 'county_fips']

# Check which geo cols exist in each file
print("\n=== GEO COLUMNS PRESENT ===")
for col in GEO_COLS:
    print(f"  {col}: orig={'YES' if col in orig.columns else 'NO'}  "
          f"updt={'YES' if col in updt.columns else 'NO'}")

# Active US rows in original
US_ABBRS = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
}

orig_active = orig[orig['aw_active'] == True]
orig_us = orig_active[orig_active['state'].isin(US_ABBRS)]
print(f"\n=== ORIGINAL: active US rows = {len(orig_us)} ===")

for col in GEO_COLS:
    if col in orig.columns:
        null_count = orig_us[col].isna().sum()
        print(f"  {col} nulls: {null_count} / {len(orig_us)}")
    else:
        print(f"  {col}: COLUMN MISSING")

updt_active = updt[updt['aw_active'] == True]
updt_us = updt_active[updt_active['state'].isin(US_ABBRS)]
print(f"\n=== UPDATED: active US rows = {len(updt_us)} ===")

for col in GEO_COLS:
    if col in updt.columns:
        null_count = updt_us[col].isna().sum()
        print(f"  {col} nulls: {null_count} / {len(updt_us)}")
    else:
        print(f"  {col}: COLUMN MISSING")

# Sample a few rows from updated to see what values look like
print("\n=== SAMPLE FROM UPDATED (first 5 active US rows) ===")
print(updt_us[['name', 'state'] + [c for c in GEO_COLS if c in updt.columns]].head(5).to_string())
