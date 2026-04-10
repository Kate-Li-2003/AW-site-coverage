---
name: FIPS enrichment scripts
description: populate_fips.py and merge_fips_back.py — locations, purposes, and run commands
type: project
---

## populate_fips.py

**Path:** `/Users/kateli/Desktop/BigLocal/populate_fips.py`
**Purpose:** Full enrichment pipeline. Downloads Census county and place-county crosswalk files from census.gov, then populates gov_level, state_fips, and county_fips for all active US rows in both the AW and Legistrar Excel files.
**Outputs:**
- `Original Sites to Verify/Copy of AW_civic_scraper_sites_updated.xlsx`
- `Original Sites to Verify/Copy_of_Legistrar_sites_updated.xlsx`
- `Original Sites to Verify/AW_civic_scraper_sites_FIPS_notes.txt`
- `Original Sites to Verify/Legistrar_sites_FIPS_notes.txt`

**Run command (from /Users/kateli/Desktop/BigLocal/):**
```
/Users/kateli/Desktop/BigLocal/venv/bin/python3 /Users/kateli/Desktop/BigLocal/populate_fips.py
```

**Notes:**
- Requires internet access to download Census files
- Must be run from /Users/kateli/Desktop/BigLocal/ (uses relative paths internally)
- Writes to _updated files, not the originals

## merge_fips_back.py

**Path:** `/Users/kateli/Desktop/BigLocal/merge_fips_back.py`
**Purpose:** Copies already-enriched gov_level, state_fips, county_fips from the _updated file back into the original file IN PLACE. Uses openpyxl to preserve formatting. Idempotent — skips cells that already have values. Prints a before/after validation report.

**Run command (absolute path, works from any directory):**
```
/Users/kateli/Desktop/BigLocal/venv/bin/python3 /Users/kateli/Desktop/BigLocal/merge_fips_back.py
```

**When to use this vs. populate_fips.py:**
- Use merge_fips_back.py when the _updated file is already current and you just need to sync the values back to the original.
- Use populate_fips.py when you want to re-derive FIPS from scratch (e.g., after adding new rows or changing the Census data sources).

## venv

**Path:** `/Users/kateli/Desktop/BigLocal/venv/`
**Packages:** pandas, openpyxl, requests (all required by both scripts)
