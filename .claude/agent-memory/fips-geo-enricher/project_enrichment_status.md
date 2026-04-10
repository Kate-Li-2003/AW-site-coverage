---
name: Enrichment run history
description: History of populate_fips.py runs and outcome counts
type: project
---

## Run: 2026-03-18

**File:** AW_civic_scraper_sites
**Active US rows:** 996
**Result:** 996/996 rows fully populated (state_fips + county_fips) in _updated file
**Remaining gaps in _updated:** 0 (all active US rows resolved)

**File:** Legistrar_sites
**Result:** Processed; see Legistrar_sites_FIPS_notes.txt for not-found list

## Merge back: 2026-03-18

**Script:** merge_fips_back.py (written 2026-03-18, not yet executed — awaiting user to run)
**Goal:** Copy enriched values from _updated back into the original file in place

**Why:** Original file still had many rows missing county_fips even though _updated was complete. The merge script syncs them without requiring a new Census download.
