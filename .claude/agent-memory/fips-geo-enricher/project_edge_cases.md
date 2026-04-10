---
name: FIPS edge cases in populate_fips.py
description: Special entity types and their county_fips assignments baked into populate_fips.py
type: project
---

## Virginia independent cities
Virginia has 38 independent cities that are county-equivalents (no parent county).
They each have their own 5-digit FIPS (51xxx range).
Stored in MANUAL_COUNTY dict in populate_fips.py.
Examples:
- Alexandria VA → 51510
- Richmond VA → 51760
- Virginia Beach VA → 51810
- Falls Church VA → 51610

## DC
District of Columbia is treated as a single county-equivalent.
- Washington DC → county_fips 11001

## Alaska boroughs
Alaska uses boroughs instead of counties. Several are in MANUAL_COUNTY.
- Kenai Peninsula Borough → 02122
- Matanuska-Susitna Borough → 02170
- Petersburg Borough → 02195
- Sitka → 02220

## Montana consolidated city-county
- Butte-Silver Bow → 30023

## Colorado consolidated city-county
- Broomfield CO → 08014

## Multi-jurisdictional special districts
Stored in MANUAL_OTHER_COUNTY. Assigned to the county of administrative headquarters.
Examples:
- San Francisco Bay Area Rapid Transit District (CA) → 06001 (Alameda, HQ Oakland)
- Metropolitan Transportation Commission (CA) → 06001 (Alameda)
- Solid Waste Authority of Central Ohio → 39049 (Franklin)
- Milwaukee Metropolitan Sewerage District (WI) → 55079 (Milwaukee)

## State-level entities with no county
Some Legistrar entries are state-level or federal with no valid county:
- North Pacific Fishery Management Council (AK) → county_fips = NULL (federal entity)
- Judicial Council of California → county_fips = NULL (statewide)

## Census naming quirks that required manual overrides

### Connecticut towns
Census calls CT towns "town" (not "city"), so place crosswalk fails for bare town names.
Manual entries in MANUAL_OTHER_COUNTY for: Coventry, Hamden, Madison, Preston, Westbrook, Woodbridge, etc.

### Massachusetts towns
Same issue as CT — Census uses "town" suffix.
Manual entries for: Agawam, Ashland, Braintree, Canton, Carlisle, Chelmsford, Concord, Natick, Weston, Yarmouth, etc.

### NJ townships
Census uses "township" suffix; bare names fail lookup.
Manual entries for: Cherry Hill, East Brunswick, Holmdel, Howell, Middletown, Plainsboro, etc.

### Compound/run-together place names in source data
Some records have no spaces (e.g., 'gulfshores' instead of 'Gulf Shores').
Handled via MANUAL_OTHER_COUNTY with normalized keys.
Examples: gulfshores (AL) → 01003, gilabend (AZ) → 04013, longbeach (CA) → 06037
