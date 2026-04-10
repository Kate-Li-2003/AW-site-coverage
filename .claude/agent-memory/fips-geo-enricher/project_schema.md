---
name: AW and Legistrar file schemas
description: Column names, data types, and active-row filter logic for the two civic scraper Excel files
type: project
---

## AW civic scraper file

**Path:** `Original Sites to Verify/Copy of AW_civic_scraper_sites.xlsx`
**Enriched output:** `Original Sites to Verify/Copy of AW_civic_scraper_sites_updated.xlsx`

Key columns:
- `name` — entity name (string)
- `state` — 2-letter US state abbreviation (e.g. 'CA', 'TX')
- `aw_active` — boolean True/False; only True rows are processed
- `gov_level` — classification: 'county', 'municipality', 'other', etc.
- `state_fips` — stored as integer (2-digit FIPS, leading zeros dropped by Excel); e.g. California = 6
- `county_fips` — stored as integer (5-digit FIPS, leading zeros dropped); e.g. Los Angeles = 6037

**Active US row filter:** `aw_active == True AND state in US_STATE_ABBRS`
**Active US row count:** 996 rows (as of 2026-03-18)

## Legistrar file

**Path:** `Original Sites to Verify/Copy_of_Legistrar_sites.xlsx`
**Enriched output:** `Original Sites to Verify/Copy_of_Legistrar_sites_updated.xlsx`

Key columns (same geo columns as AW file, plus):
- `site_type` — used to derive `gov_level` via map_site_type_to_gov_level()
- `aw_active` — string 'yes'/'no' (NOT boolean like AW file)

**Active US row filter:** `aw_active == 'yes' (case-insensitive) AND state in US_STATE_ABBRS`

## FIPS storage convention

FIPS codes are stored as plain integers in Excel (leading zeros are dropped).
When querying or exporting, zero-pad to enforce Census standards:
- state_fips: zfill(2)  → '06', '48', '11', etc.
- county_fips: zfill(5) → '06037', '48201', etc.

**Why:** openpyxl/pandas write int(fips) to avoid Excel treating numeric strings as text.
