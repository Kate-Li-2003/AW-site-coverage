#!/usr/bin/env python3
"""
Populate gov_level, state_fips, and county_fips for active US rows
in Copy of AW_civic_scraper_sites.xlsx and Copy_of_Legistrar_sites.xlsx.

Data sources:
  - Census national_county.txt  (county FIPS by state)
  - Census tab20_placecounty20  (place -> county crosswalk)
  - Manual overrides for edge cases (VA independent cities, DC, consolidated city-counties)
"""

import os, re, io, zipfile, requests
import pandas as pd

# ── paths ──────────────────────────────────────────────────────────────────────
BASE      = "Original Sites to Verify"
AW_IN     = f"{BASE}/Copy of AW_civic_scraper_sites.xlsx"
LEG_IN    = f"{BASE}/Copy_of_Legistrar_sites.xlsx"
AW_OUT    = f"{BASE}/Copy of AW_civic_scraper_sites_updated.xlsx"
LEG_OUT   = f"{BASE}/Copy_of_Legistrar_sites_updated.xlsx"
AW_NOTES  = f"{BASE}/AW_civic_scraper_sites_FIPS_notes.txt"
LEG_NOTES = f"{BASE}/Legistrar_sites_FIPS_notes.txt"

US_STATE_ABBRS = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
    'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'
}

# ── 1. State abbreviation -> state FIPS (2-digit, zero-padded) ─────────────────
STATE_FIPS = {
    'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09',
    'DE':'10','DC':'11','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17',
    'IN':'18','IA':'19','KS':'20','KY':'21','LA':'22','ME':'23','MD':'24',
    'MA':'25','MI':'26','MN':'27','MS':'28','MO':'29','MT':'30','NE':'31',
    'NV':'32','NH':'33','NJ':'34','NM':'35','NY':'36','NC':'37','ND':'38',
    'OH':'39','OK':'40','OR':'41','PA':'42','RI':'44','SC':'45','SD':'46',
    'TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53','WV':'54',
    'WI':'55','WY':'56'
}

# ── 2. Download Census national county file ────────────────────────────────────
COUNTY_URL = ("https://www2.census.gov/geo/docs/reference/codes/files/"
              "national_county.txt")

def load_county_lookup():
    """Returns dict: (state_abbr, normalized_county_name) -> 5-digit county FIPS str."""
    print("Downloading Census county file …")
    r = requests.get(COUNTY_URL, timeout=30)
    r.raise_for_status()
    lines = r.text.splitlines()
    lookup = {}
    for line in lines[1:]:           # skip header
        parts = line.split(',')
        if len(parts) < 4:
            continue
        state_abbr = parts[0].strip()
        statefp    = parts[1].strip().zfill(2)
        countyfp   = parts[2].strip().zfill(3)
        county_name = parts[3].strip()
        fips5 = statefp + countyfp
        key = (state_abbr, normalize(county_name))
        lookup[key] = fips5
        # also index without "county" / "parish" / "borough" suffix
        bare = re.sub(r'\s+(county|parish|borough|census area|municipality|city and borough)$',
                      '', county_name, flags=re.I).strip()
        lookup[(state_abbr, normalize(bare))] = fips5
    return lookup

# ── 3. Download Census place->county crosswalk ─────────────────────────────────
PLACE_COUNTY_URL = ("https://www2.census.gov/geo/docs/reference/codes2020/"
                    "national_place_by_county2020.txt")

def load_place_county_lookup():
    """
    Returns dict: (state_abbr, normalized_place_name) -> 5-digit county FIPS.
    Source: Census national_place_by_county2020.txt
    Columns: STATE|STATEFP|COUNTYFP|COUNTYNAME|PLACEFP|PLACENS|PLACENAME|TYPE|CLASSFP|FUNCSTAT
    When a place spans multiple counties (appears in multiple rows), the first
    occurrence is kept (Census file lists primary county first).
    """
    print("Downloading Census place-by-county crosswalk …")
    r = requests.get(PLACE_COUNTY_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep='|', dtype=str, low_memory=False)
    df['fips5'] = df['STATEFP'].str.zfill(2) + df['COUNTYFP'].str.zfill(3)

    lookup = {}
    for _, row in df.iterrows():
        abbr = str(row['STATE']).strip()
        place_name = str(row['PLACENAME']).strip()
        fips5 = row['fips5']
        # strip legal suffixes like "city", "town", "village", "CDP", etc.
        bare_place = re.sub(
            r'\s+(city|town|village|borough|township|CDP|community|'
            r'plantation|grant|location|unorganized territory|charter township)$',
            '', place_name, flags=re.I).strip()
        for pname in [place_name, bare_place]:
            key = (abbr, normalize(pname))
            if key not in lookup:   # keep first occurrence = primary county
                lookup[key] = fips5
    return lookup

def normalize(s):
    """Lowercase, strip punctuation/whitespace, collapse spaces."""
    s = str(s).lower()
    s = re.sub(r"['\-\.&/()\[\],]", ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ── 4. Manual overrides ────────────────────────────────────────────────────────
# VA independent cities (county equivalents), DC, and consolidated city-counties.
# county_fips = full 5-digit FIPS of the city-as-county-equivalent.
MANUAL_COUNTY = {
    # --- Virginia independent cities ---
    ('VA','alexandria'):           '51510',
    ('VA','bedford'):              '51515',
    ('VA','bristol'):              '51520',
    ('VA','buena vista'):          '51530',
    ('VA','charlottesville'):      '51540',
    ('VA','chesapeake'):           '51550',
    ('VA','colonial heights'):     '51570',
    ('VA','covington'):            '51580',
    ('VA','danville'):             '51590',
    ('VA','emporia'):              '51595',
    ('VA','fairfax'):              '51600',
    ('VA','falls church'):         '51610',
    ('VA','fallschurch'):          '51610',   # spelling variant in data
    ('VA','franklin'):             '51620',
    ('VA','fredericksburg'):       '51630',
    ('VA','galax'):                '51640',
    ('VA','hampton'):              '51650',
    ('VA','harrisonburg'):         '51660',
    ('VA','hopewell'):             '51670',
    ('VA','lexington'):            '51678',
    ('VA','lynchburg'):            '51680',
    ('VA','manassas'):             '51683',
    ('VA','manassas park'):        '51685',
    ('VA','martinsville'):         '51690',
    ('VA','newport news'):         '51700',
    ('VA','norfolk'):              '51710',
    ('VA','norton'):               '51720',
    ('VA','petersburg'):           '51730',
    ('VA','poquoson'):             '51735',
    ('VA','portsmouth'):           '51740',
    ('VA','radford'):              '51750',
    ('VA','richmond'):             '51760',
    ('VA','roanoke'):              '51770',
    ('VA','salem'):                '51775',
    ('VA','staunton'):             '51790',
    ('VA','suffolk'):              '51800',
    ('VA','virginia beach'):       '51810',
    ('VA','waynesboro'):           '51820',
    ('VA','williamsburg'):         '51830',
    ('VA','winchester'):           '51840',
    # --- DC: district is its own county equivalent ---
    ('DC','washington'):           '11001',
    ('DC','district of columbia'): '11001',
    # --- Montana consolidated city-county ---
    ('MT','buttesilverbow'):       '30023',
    ('MT','butte silver bow'):     '30023',
    ('MT','butte-silver bow'):     '30023',
    # --- Colorado consolidated city-county ---
    ('CO','broomfield'):           '08014',
    # --- Alaska unified home rule boroughs (borough = county equivalent) ---
    # Kenai Peninsula Borough
    ('AK','kenai peninsula borough'):       '02122',
    ('AK','kenai peninsula'):               '02122',
    # Matanuska-Susitna Borough
    ('AK','matanuska susitna borough'):     '02170',
    ('AK','matanusk susitna borough'):      '02170',   # typo in data
    ('AK','matanuska-susitna borough'):     '02170',
    # Petersburg Borough
    ('AK','petersburg'):                    '02195',
    # Sitka Borough
    ('AK','sitka'):                         '02220',
    # Valdez-Cordova Census Area -> renamed, now Chugach & Copper River
    ('AK','valdez'):                        '02261',   # Chugach Census Area (closest)
    # Louisiana parishes  —  Census crosswalk uses "Parish" suffix
    # (handled generically via county lookup, but spelling variants here)
}

# For multi-jurisdiction "other" entities, map to the primary county
MANUAL_OTHER_COUNTY = {
    # AZ
    ('AZ','grayhawk community association'):       '04013',  # Maricopa
    ('AZ','power ranch community association'):    '04021',  # Pinal
    ('AZ','village at vistancia'):                 '04013',  # Maricopa
    # AR
    ('AR','clarksville connected utilities'):      '05041',  # Johnson County
    # CA
    ('CA','chino basin desalter authority'):       '06071',  # San Bernardino
    ('CA','chino valley fire district'):           '06071',  # San Bernardino
    ('CA','cosumnes community services district'): '06067',  # Sacramento
    ('CA','cucamonga valley water district'):      '06071',  # San Bernardino
    ('CA','fair oaks recreation park district'):   '06067',  # Sacramento
    ('CA','port san luis harbor district'):        '06079',  # San Luis Obispo
    ('CA','san bernardino water department'):      '06071',  # San Bernardino
    ('CA','santa clara valley habitat agency'):    '06085',  # Santa Clara
    ('CA','santa fe irrigation district'):         '06073',  # San Diego
    ('CA','solano irrigation district'):           '06095',  # Solano
    ('CA','san joaquin council of governments'):   '06077',  # San Joaquin
    ('CA','shasta regional transportation agency'):'06089',  # Shasta
    ('CA','wiyot tribe'):                          '06023',  # Humboldt
    ('CA','western municipal water district'):     '06065',  # Riverside
    # CO
    ('CO','aspen pitkin county housing authority'):'08097',  # Pitkin
    ('CO','brighton urban renewal authority'):     '08001',  # Adams
    ('CO','southgate water sanitation districts'): '08005',  # Arapahoe
    # FL
    ('FL','solid waste authority'):                '12099',  # Palm Beach
    # IL
    ('IL','itasca park district'):                 '17043',  # DuPage
    ('IL','algonquin lake in the hills fire protection district'): '17111',  # McHenry
    ('IL','moline library'):                       '17161',  # Rock Island
    # IN
    ('IN','valparaiso utilities'):                 '18127',  # Porter
    # KS
    ('KS','derby rec commission'):                 '20173',  # Sedgwick
    ('KS','emporia rec'):                          '20057',  # Lyon
    ('KS','lawrence douglas county public health district'): '20045',  # Douglas
    ('KS','spring hill recreation commission'):    '20091',  # Johnson
    # LA
    ('LA','assumption parish police jury'):        '22005',  # Assumption Parish
    # MA
    ('MA','hingham library'):                      '25023',  # Plymouth
    ('MA','jones library'):                        '25015',  # Hampshire (Amherst)
    # ME
    ('ME','portland housing authority'):           '23005',  # Cumberland
    # MI
    ('MI','adrian library'):                       '26091',  # Lenawee
    ('MI','east lansing neighborhoods'):           '26065',  # Ingham
    ('MI','plymouth downtown development authority'): '26163',  # Wayne
    # MN
    ('MN','st cloud regions of the statewide emergency communications board'): '27145', # Stearns
    # MT (Butte-Silver Bow consolidated)
    # NC
    ('NC','high point'):                           '37081',  # Guilford
    ('NC','onslow water and sewer authority'):     '37133',  # Onslow
    ('NC','piedmont authority for regional transportation'): '37081',  # Guilford (HQ)
    # ND
    ('ND','grandforks library'):                   '38035',  # Grand Forks
    # NM
    ('NM','mid region council of governments'):    '35001',  # Bernalillo (HQ Albuquerque)
    ('NM','rio metro regional transit district'):  '35001',  # Bernalillo
    # OH
    ('OH','solid waste authority of central ohio'):'39049',  # Franklin
    # OK
    ('OK','quapaw tribe'):                         '40115',  # Ottawa
    # OR
    ('OR','tualatin fire and rescue'):             '41067',  # Washington
    # SC
    ('SC','charleston water system'):              '45019',  # Charleston
    # TN
    ('TN','williamson library'):                   '47187',  # Williamson
    # TX
    ('TX','bryan college station metropolitan planning organization'): '48041',  # Brazos
    ('TX','lake cities municipal utility authority'): '48121',  # Denton
    ('TX','lost pines groundwater conservation district'): '48021',  # Bastrop
    ('TX','workforce solutions northeast texas'):  '48037',  # Bowie (HQ Texarkana)
    # UT
    ('UT','sandy suburban improvement district'):  '49035',  # Salt Lake
    # VA
    ('VA','fredericksburg economic development   tourism'): '51630', # Fredericksburg city
    ('VA','fredericksburg economic development & tourism'): '51630',
    ('VA','northern virginia regional commission'): '51059', # Fairfax County (HQ)
    ('VA','roanoke valleu resource authority'):    '51770',  # Roanoke city
    ('VA','williamsburg area transit authority'):  '51830',  # Williamsburg city
    ('VA','western virginia regional jail'):       '51770',  # Roanoke (primary member)
    # VT
    ('VT','champlain water district'):             '50007',  # Chittenden
    # WA
    ('WA','eastside fire rescue'):                 '53033',  # King
    ('WA','everett library'):                      '53061',  # Snohomish
    ('WA','mountain view fire   rescue'):          '53053',  # Pierce
    ('WA','mountain view fire & rescue'):          '53053',
    ('WA','kittitas public utility district'):     '53037',  # Kittitas
    ('WA','lake haven utility district'):          '53033',  # King
    ('WA','mukilteo water and wastewater district'):'53061', # Snohomish
    ('WA','port of bellingham'):                   '53073',  # Whatcom
    ('WA','port of longview'):                     '53015',  # Cowlitz
    ('WA','puget sound clean air'):                '53033',  # King (HQ Seattle)
    ('WA','south king fire   rescue commission'):  '53033',  # King
    ('WA','south king fire & rescue commission'):  '53033',
    ('WA','sammamish plateau water'):              '53033',  # King
    ('WA','thurston regional planning council'):   '53067',  # Thurston
    # WI
    ('WI','thiensville library'):                  '55089',  # Ozaukee

    # ── AW not-found spelling/compound-name fixes ──────────────────────────
    # AL
    ('AL','gulfshores'):                           '01003',  # Baldwin
    # AZ
    ('AZ','gilabend'):                             '04013',  # Maricopa
    # CA
    ('CA','delmar'):                               '06073',  # San Diego
    ('CA','tehachapi city hall'):                  '06029',  # Kern
    ('CA','ventura'):                              '06111',  # Ventura County
    # CO
    ('CO','aspen pitkin county housing authority'):'08097',  # Pitkin
    ('CO','buenavista'):                           '08015',  # Chaffee
    # CT — all towns (Census calls them towns, not cities; cousub match fails)
    ('CT','coventry'):                             '09013',  # Tolland
    ('CT','hamden'):                               '09009',  # New Haven
    ('CT','madison'):                              '09009',  # New Haven
    ('CT','preston'):                              '09011',  # New London
    ('CT','westbrook'):                            '09007',  # Middlesex
    ('CT','westhaven'):                            '09009',  # New Haven
    ('CT','woodbridge'):                           '09009',  # New Haven
    # FL
    ('FL','westmelbourne'):                        '12009',  # Brevard
    # IA
    ('IA','leclaire'):                             '19163',  # Scott
    ('IA','lemars'):                               '19149',  # Plymouth
    # IL
    ('IL','apco'):                                 '17031',  # Cook (likely Apple Canyon Lake area)
    ('IL','deerpark'):                             '17097',  # Lake
    ('IL','indianhead park'):                      '17031',  # Cook
    ('IL','newtrier'):                             '17031',  # Cook (New Trier Township)
    ('IL','oakforest'):                            '17031',  # Cook
    ('IL','rivergrove'):                           '17031',  # Cook
    ('IL','rock island county metropolitan mass transit district'): '17161',  # Rock Island
    # KS
    ('KS','derby portal'):                         '20173',  # Sedgwick
    ('KS','desoto'):                               '20091',  # Johnson
    # MA — towns not matching because Census uses "town" suffix
    ('MA','agawam'):                               '25013',  # Hampden
    ('MA','ashland'):                              '25017',  # Middlesex
    ('MA','braintree'):                            '25021',  # Norfolk
    ('MA','canton'):                               '25021',  # Norfolk
    ('MA','carlisle'):                             '25017',  # Middlesex
    ('MA','chelmsford'):                           '25017',  # Middlesex
    ('MA','cohasset'):                             '25021',  # Norfolk
    ('MA','concord'):                              '25017',  # Middlesex
    ('MA','eastlong meadow'):                      '25013',  # Hampden (East Longmeadow)
    ('MA','lincoln'):                              '25017',  # Middlesex
    ('MA','long meadow'):                          '25013',  # Hampden (Longmeadow)
    ('MA','manchester by the sea'):                '25009',  # Essex
    ('MA','mansfield'):                            '25005',  # Bristol
    ('MA','middleborough'):                        '25023',  # Plymouth
    ('MA','middleton'):                            '25009',  # Essex
    ('MA','natick'):                               '25017',  # Middlesex
    ('MA','south hadley'):                         '25015',  # Hampshire
    ('MA','weston'):                               '25017',  # Middlesex
    ('MA','whitman'):                              '25023',  # Plymouth
    ('MA','yarmouth'):                             '25001',  # Barnstable
    # MD
    ('MD','belair'):                               '24025',  # Harford (Bel Air)
    # ME
    ('ME','wells'):                                '23031',  # York
    ('ME','york'):                                 '23031',  # York
    # MI — townships
    ('MI','canton'):                               '26163',  # Wayne (Canton Township)
    ('MI','centerline'):                           '26099',  # Macomb
    ('MI','georgetown'):                           '26139',  # Ottawa
    ('MI','lenox'):                                '26099',  # Macomb
    ('MI','macomb'):                               '26099',  # Macomb (Township)
    ('MI','pittsfield township'):                  '26161',  # Washtenaw
    ('MI','waterford'):                            '26125',  # Oakland
    # MN
    ('MN','lesueur'):                              '27079',  # Le Sueur
    # MO
    ('MO','desperes'):                             '29189',  # St. Louis
    ('MO','saint robert'):                         '29169',  # Pulaski
    ('MO','stann'):                                '29189',  # St. Louis (St. Ann)
    ('MO','town   country'):                       '29189',  # St. Louis
    # NC
    ('NC','winston'):                              '37067',  # Forsyth (Winston-Salem)
    # NH
    ('NH','bedford'):                              '33011',  # Hillsborough
    ('NH','bow'):                                  '33013',  # Merrimack
    ('NH','windham'):                              '33015',  # Rockingham
    # NJ — townships (Census uses "township" suffix)
    ('NJ','bloomfield township'):                  '34013',  # Essex
    ('NJ','cherry hill'):                          '34007',  # Camden
    ('NJ','eastamwell township'):                  '34019',  # Hunterdon
    ('NJ','east brunswick'):                       '34023',  # Middlesex
    ('NJ','fairfield'):                            '34013',  # Essex
    ('NJ','franklin township   gloucester'):       '34015',  # Gloucester
    ('NJ','holmdel township'):                     '34025',  # Monmouth
    ('NJ','hopewell township   mercer'):           '34021',  # Mercer
    ('NJ','howell township'):                      '34025',  # Monmouth
    ('NJ','livingston township'):                  '34013',  # Essex
    ('NJ','middletown'):                           '34025',  # Monmouth
    ('NJ','millburn township'):                    '34013',  # Essex
    ('NJ','montville township'):                   '34027',  # Morris
    ('NJ','moorestown township'):                  '34005',  # Burlington
    ('NJ','morris township'):                      '34027',  # Morris
    ('NJ','orange city'):                          '34013',  # Essex
    ('NJ','plainsboro'):                           '34023',  # Middlesex
    ('NJ','redbank borough'):                      '34025',  # Monmouth
    ('NJ','roxbury'):                              '34027',  # Morris
    ('NJ','stafford township'):                    '34029',  # Ocean
    ('NJ','wall township'):                        '34025',  # Monmouth
    ('NJ','west orange'):                          '34013',  # Essex
    # NY — towns
    ('NY','easthampton town'):                     '36103',  # Suffolk
    ('NY','malta'):                                '36091',  # Saratoga
    ('NY','mamakating'):                           '36105',  # Sullivan
    ('NY','new scotland'):                         '36001',  # Albany
    ('NY','southeast'):                            '36079',  # Putnam
    ('NY','wheatfield'):                           '36063',  # Niagara
    # OH — townships
    ('OH','copley township'):                      '39153',  # Summit
    ('OH','liberty township'):                     '39017',  # Butler
    ('OH','miami township'):                       '39025',  # Clermont
    ('OH','prairie township'):                     '39049',  # Franklin
    ('OH','washington courthouse'):                '39047',  # Fayette
    # OR
    ('OR','grantspass'):                           '41033',  # Josephine
    # PA — townships
    ('PA','east brandywine'):                      '42029',  # Chester
    ('PA','east caln township'):                   '42029',  # Chester
    ('PA','findlay township'):                     '42003',  # Allegheny
    ('PA','kennett township'):                     '42029',  # Chester
    ('PA','limerick'):                             '42091',  # Montgomery
    ('PA','middlesex township'):                   '42041',  # Cumberland
    ('PA','millcreek township'):                   '42049',  # Erie
    ('PA','murrysville'):                          '42129',  # Westmoreland
    ('PA','north fayette township'):               '42003',  # Allegheny
    ('PA','north huntingdon township'):            '42129',  # Westmoreland
    ('PA','pine township allegheny county'):       '42003',  # Allegheny
    ('PA','ross township'):                        '42003',  # Allegheny
    ('PA','shaler township'):                      '42003',  # Allegheny
    ('PA','south fayette'):                        '42003',  # Allegheny
    ('PA','uppermoreland township'):               '42091',  # Montgomery
    ('PA','upper providence township'):            '42091',  # Montgomery
    ('PA','uwchlan township'):                     '42029',  # Chester
    ('PA','westchester'):                          '42029',  # Chester (West Chester)
    ('PA','west lampeter'):                        '42071',  # Lancaster
    ('PA','west norriton'):                        '42091',  # Montgomery
    ('PA','westwhiteland'):                        '42029',  # Chester
    ('PA','whitemarsh township'):                  '42091',  # Montgomery
    ('PA','whitpain township'):                    '42091',  # Montgomery
    # RI
    ('RI','east greenwich'):                       '44003',  # Kent
    ('RI','north kings town'):                     '44009',  # Washington
    ('RI','portsmouth'):                           '44005',  # Newport
    ('RI','richmond'):                             '44009',  # Washington
    # SC
    ('SC','tegacay'):                              '45091',  # York
    # TN
    ('TN','springhill'):                           '47119',  # Maury (primary county)
    # TX
    ('TX','bryan college station metropolitan planning organization'): '48041',  # Brazos
    ('TX','ben brook'):                            '48439',  # Tarrant
    ('TX','fairoaks ranch'):                       '48029',  # Bexar
    # UT
    ('UT','americanfork'):                         '49049',  # Utah
    ('UT','fruitheights city'):                    '49011',  # Davis
    # VT
    ('VT','hartford'):                             '50027',  # Windsor
    # WA
    ('WA','battleground'):                         '53011',  # Clark
    ('WA','laconner'):                             '53057',  # Skagit
    ('WA','mount lake terrace'):                   '53061',  # Snohomish
    # WI
    ('WI','cottagegrove'):                         '55025',  # Dane
    ('WI','elmgrove'):                             '55133',  # Waukesha
    ('WI','foxlake'):                              '55027',  # Dodge
    ('WI','foxpoint'):                             '55079',  # Milwaukee
    ('WI','riverfalls'):                           '55093',  # Pierce
    # WY
    ('WY','greenriver'):                           '56037',  # Sweetwater

    # ── Legistrar not-found special entities ───────────────────────────────
    # AZ
    ('AZ','lake havasu'):                          '04015',  # Mohave
    # CA
    ('CA','alameda contra costa transit district'):'06001',  # Alameda (primary)
    ('CA','longbeach'):                            '06037',  # Los Angeles
    ('CA','los angeles workforce development aging and community services'): '06037',
    ('CA','los angeles san diego san luis obispo rail corridor agency'):     '06037',
    ('CA','metro  los angeles '):                  '06037',  # Los Angeles
    ('CA','metro (los angeles)'):                  '06037',
    ('CA','metropolitan transportation commission'):'06001', # Alameda (HQ Oakland)
    ('CA','oakland unified'):                      '06001',  # Alameda
    ('CA','orange county sanitation district'):    '06059',  # Orange
    ('CA','orange county transportation authority'):'06059', # Orange
    ('CA','port of oakland'):                      '06001',  # Alameda
    ('CA','port of san diego'):                    '06073',  # San Diego
    ("CA","san bernardino county employees  retirement assoc "): '06071',
    ("CA","san bernardino county employees' retirement assoc."): '06071',
    ('CA','san francisco bay area rapid transit district'): '06001',  # Alameda
    ('CA','santa clara valley water district'):    '06085',  # Santa Clara
    ('CA','the retirement services department city of san jose'): '06085',
    # FL
    ('FL','monroe county school district'):        '12087',  # Monroe
    ('FL','northport'):                            '12115',  # Sarasota (North Port)
    # IL
    ('IL','forest preserves of cook county'):      '17031',  # Cook
    ('IL','metropolitan water reclamation district of greater chicago'): '17031',
    # KY
    ('KY','lexington fayette urban county government'): '21067',  # Fayette
    # ME
    ('ME','windham'):                              '23005',  # Cumberland
    # MI
    ('MI','detroit water and sewage department'):  '26163',  # Wayne
    ('MI','great lakes water authority'):          '26163',  # Wayne (HQ Detroit)
    # MN
    ('MN','minnesota public uitlities commission'): '27123', # Ramsey (HQ St. Paul)
    # NC
    ('NC','eastern band of the cherokee nation'):  '37099',  # Swain
    ('NC','nc capital area metropolitan planning organization'): '37183',  # Wake
    # NM
    ('NM','albuquerque bernalillo county water utility authority'): '35001',  # Bernalillo
    # OR
    ('OR','metro  oregon '):                       '41051',  # Multnomah
    ('OR','metro (oregon)'):                       '41051',
    # TX
    ('TX','bouerne'):                              '48259',  # Kendall (Boerne)
    ('TX','edwards aquifer authority'):            '48029',  # Bexar (primary)
    ('TX','pedernales electric cooperative'):      '48031',  # Blanco (HQ Johnson City)
    # WA
    ('WA','whatcom'):                              '53073',  # Whatcom County
    # WI
    ('WI','milwaukee metropolitan sewerage district'): '55079',  # Milwaukee
    ('WI','westallis'):                            '55079',  # Milwaukee
}

# Legistrar-specific manual overrides (special entities)
LEGISTRAR_MANUAL = {
    ('AK','north pacific fishery management council'):    ('other',  '02',     None),  # federal entity, HQ Anchorage AK; no single county
    # CA statewide entity — no county equivalent
    ('CA','judicial council of california'):              ('other',  '06',     None),
    ('AK','kenai peninsula borough'):                     ('county', '02', '02122'),
    ('AK','matanusk susitna borough'):                    ('county', '02', '02170'),
    ('AK','matanuska-susitna borough'):                   ('county', '02', '02170'),
    ('AK','petersburg'):                                  ('borough','02', '02195'),
    ('AK','sitka'):                                       ('borough','02', '02220'),
    ('AK','valdez'):                                      ('municipality','02','02261'),
    # DC
    ('DC','district of columbia'):                        ('municipality','11','11001'),
    # VA independent cities
    ('VA','alexandria'):           ('municipality','51','51510'),
    ('VA','chesapeake'):           ('municipality','51','51550'),
    ('VA','hampton'):              ('municipality','51','51650'),
    ('VA','lynchburg'):            ('municipality','51','51680'),
    ('VA','newport news'):         ('municipality','51','51700'),
    ('VA','norfolk'):              ('municipality','51','51710'),
    ('VA','petersburg'):           ('municipality','51','51730'),
    ('VA','richmond'):             ('municipality','51','51760'),
    ('VA','roanoke'):              ('municipality','51','51770'),
    ('VA','suffolk'):              ('municipality','51','51800'),
    ('VA','virginia beach'):       ('municipality','51','51810'),
    # MT consolidated
    ('MT','butte silver bow'):     ('county and municipal','30','30023'),
    # IL special
    ('IL','forest preserve district of cook county'): ('forest preserves','17','17031'),  # Cook
    ('IL','forest preserve district of dupage county'): ('forest preserves','17','17043'),
    ('IL','forest preserve district of will county'):  ('forest preserves','17','17197'),
    ('IL','forest preserve district of kane county'):  ('forest preserves','17','17089'),
}

# ── helpers ────────────────────────────────────────────────────────────────────
def resolve_county_fips(state_abbr, name, gov_level,
                        county_lkp, place_lkp):
    """
    Try to find the 5-digit county FIPS for a given entity.
    Returns (fips5_str | None, note_str).
    """
    key_norm = normalize(name)

    # 1. Manual override for "other" entities
    for mk, mv in MANUAL_OTHER_COUNTY.items():
        if mk[0] == state_abbr and normalize(mk[1]) == key_norm:
            return mv, "manual override (special district/other)"
    # check using normalized key lookup
    manual_key = (state_abbr, key_norm)
    if manual_key in MANUAL_COUNTY:
        return MANUAL_COUNTY[manual_key], "manual override (independent city / consolidated / special)"

    # 2. County entities — strip "County", "Parish", "Borough" suffix and look up
    if gov_level in ('county',):
        bare = re.sub(r'\s+(county|parish|borough|census area|'
                      r'municipality|city and borough)\s*$', '',
                      name, flags=re.I).strip()
        for variant in [name, bare]:
            k = (state_abbr, normalize(variant))
            if k in county_lkp:
                return county_lkp[k], "census county lookup"
        # Try stripping qualifiers like "County Attorney", "County Water District"
        m = re.match(r'^(.+?)\s+county\b', name, re.I)
        if m:
            k = (state_abbr, normalize(m.group(1) + ' county'))
            if k in county_lkp:
                return county_lkp[k], "census county lookup (stripped qualifier)"

    # 3. Municipality / other — try place->county crosswalk
    for variant in [name]:
        k = (state_abbr, normalize(variant))
        if k in place_lkp:
            return place_lkp[k], "census place->county crosswalk"
    # try stripping common suffixes from municipality name
    bare = re.sub(r'\s+(city|town|village|township|borough|'
                  r'library|utilities|fire district|park district|'
                  r'water district|recreation commission|'
                  r'housing authority|neighborhoods|'
                  r'downtown development authority)$', '',
                  name, flags=re.I).strip()
    if bare != name:
        k = (state_abbr, normalize(bare))
        if k in place_lkp:
            return place_lkp[k], "census place->county crosswalk (stripped suffix)"

    return None, "not found"

def map_site_type_to_gov_level(site_type):
    """Map Legistrar site_type to a gov_level category."""
    t = str(site_type).lower().strip()
    if t in ('county', 'judicial county'):
        return 'county'
    if t in ('municipality', 'borough', 'township', 'parish'):
        return 'municipality'
    if t == 'county and municipal':
        return 'municipality & county'
    # Everything else is "other"
    return 'other'

# ── main processing ────────────────────────────────────────────────────────────
def process_aw(county_lkp, place_lkp):
    df = pd.read_excel(AW_IN)
    edge_cases = []
    not_found  = []
    changed    = 0

    for idx, row in df.iterrows():
        if row.get('aw_active') is not True:
            continue
        state = str(row.get('state', '')).strip()
        if state not in US_STATE_ABBRS:
            continue
        # state_fips
        sf = STATE_FIPS.get(state)
        if sf and pd.isna(row.get('state_fips')):
            df.at[idx, 'state_fips'] = int(sf)
            changed += 1

        # county_fips
        if not pd.isna(row.get('county_fips')):
            continue  # already filled

        name      = str(row.get('name', '')).strip()
        gov_level = str(row.get('gov_level', '')).strip().lower()

        fips5, note = resolve_county_fips(state, name, gov_level,
                                          county_lkp, place_lkp)
        if fips5:
            df.at[idx, 'county_fips'] = int(fips5)
            changed += 1
            if 'manual' in note or 'independent' in note:
                edge_cases.append(
                    f"  [{state}] {name!r}: county_fips={fips5} — {note}"
                )
        else:
            not_found.append(f"  [{state}] {name!r} (gov_level={gov_level})")

    df.to_excel(AW_OUT, index=False)
    return edge_cases, not_found, changed

def process_legistrar(county_lkp, place_lkp):
    df = pd.read_excel(LEG_IN)
    # Add columns if missing
    for col in ('gov_level', 'state_fips', 'county_fips'):
        if col not in df.columns:
            df[col] = None

    edge_cases = []
    not_found  = []
    changed    = 0

    for idx, row in df.iterrows():
        if str(row.get('aw_active', '')).strip().lower() != 'yes':
            continue
        state = str(row.get('state', '')).strip()
        if state not in US_STATE_ABBRS:
            continue

        name      = str(row.get('name', '')).strip()
        site_type = str(row.get('site_type', '')).strip()

        # Check manual Legistrar overrides first
        man_key = (state, normalize(name))
        if man_key in LEGISTRAR_MANUAL:
            gl, sf_str, cf_str = LEGISTRAR_MANUAL[man_key]
            df.at[idx, 'gov_level']   = gl
            df.at[idx, 'state_fips']  = int(sf_str) if sf_str else None
            df.at[idx, 'county_fips'] = int(cf_str) if cf_str else None
            changed += 1
            edge_cases.append(
                f"  [{state}] {name!r}: gov_level={gl}, "
                f"county_fips={cf_str} — manual override"
            )
            continue

        # gov_level
        gov_level = map_site_type_to_gov_level(site_type)
        df.at[idx, 'gov_level'] = gov_level

        # state_fips
        sf = STATE_FIPS.get(state)
        if sf:
            df.at[idx, 'state_fips'] = int(sf)

        # county_fips
        fips5, note = resolve_county_fips(state, name, gov_level,
                                          county_lkp, place_lkp)
        if fips5:
            df.at[idx, 'county_fips'] = int(fips5)
            changed += 1
            if 'manual' in note:
                edge_cases.append(
                    f"  [{state}] {name!r}: county_fips={fips5} — {note}"
                )
        else:
            not_found.append(
                f"  [{state}] {name!r} (site_type={site_type}, gov_level={gov_level})"
            )

    df.to_excel(LEG_OUT, index=False)
    return edge_cases, not_found, changed

def write_notes(path, title, edge_cases, not_found, total_changed, workflow_text):
    with open(path, 'w') as f:
        f.write(f"{'='*70}\n{title}\nGenerated 2026-03-18\n{'='*70}\n\n")
        f.write(workflow_text + "\n\n")
        f.write(f"TOTAL CELLS UPDATED: {total_changed}\n\n")
        f.write("─"*60 + "\nEDGE CASES\n" + "─"*60 + "\n")
        if edge_cases:
            f.write("\n".join(edge_cases) + "\n")
        else:
            f.write("  (none)\n")
        f.write("\n" + "─"*60 + "\nNOT FOUND (county_fips could not be resolved)\n" + "─"*60 + "\n")
        if not_found:
            f.write("\n".join(not_found) + "\n")
        else:
            f.write("  (none — all resolved)\n")

AW_WORKFLOW = """\
WORKFLOW — AW_civic_scraper_sites
----------------------------------
1. Filter rows where aw_active = True AND state is a US state abbreviation.
2. state_fips: populated from a hardcoded state-abbreviation → 2-digit FIPS map
   (standard Census Bureau state FIPS codes).
3. county_fips (stored as integer representing the full 5-digit FIPS, leading
   zeros dropped):
   a. If already populated, skip.
   b. For gov_level = 'county': look up entity name against the Census
      national_county.txt file (downloaded from census.gov). Strip common
      suffixes ('County', 'Parish', 'Borough', 'Census Area') before matching.
   c. For gov_level = 'municipality' or 'other': look up place name against
      the Census 2020 Place-County relationship file
      (tab20_placecounty20_natl_cr_utf8.txt). When a place spans multiple
      counties, the county with the largest intersection area is used.
   d. Manual overrides are applied first for known edge cases (see below).

DATA SOURCES
  • State FIPS: hardcoded (Census standard)
  • County FIPS: https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt
  • Place→County: https://www2.census.gov/geo/docs/reference/codes2020/national_place_by_county2020.txt
"""

LEG_WORKFLOW = """\
WORKFLOW — Legistrar_sites
---------------------------
1. Filter rows where aw_active = 'yes' AND state is a US state abbreviation.
2. gov_level: derived from site_type column using the mapping:
     county / judicial county        → 'county'
     municipality / borough / parish → 'municipality'
     county and municipal            → 'municipality & county'
     all other types                 → 'other'
3. state_fips: same hardcoded map as AW file.
4. county_fips: same Census lookup strategy as AW file (county→national_county.txt,
   municipality/other→place-county crosswalk), with manual overrides applied first.

DATA SOURCES  (same as AW file above)
"""

if __name__ == '__main__':
    county_lkp = load_county_lookup()
    print(f"  Loaded {len(county_lkp)} county entries")
    place_lkp  = load_place_county_lookup()
    print(f"  Loaded {len(place_lkp)} place entries")

    print("\nProcessing AW file …")
    aw_edge, aw_miss, aw_changed = process_aw(county_lkp, place_lkp)
    write_notes(AW_NOTES, "AW_civic_scraper_sites — FIPS Population Notes",
                aw_edge, aw_miss, aw_changed, AW_WORKFLOW)
    print(f"  Updated {aw_changed} cells. Not found: {len(aw_miss)}.")
    print(f"  Saved: {AW_OUT}")
    print(f"  Notes: {AW_NOTES}")

    print("\nProcessing Legistrar file …")
    leg_edge, leg_miss, leg_changed = process_legistrar(county_lkp, place_lkp)
    write_notes(LEG_NOTES, "Legistrar_sites — FIPS Population Notes",
                leg_edge, leg_miss, leg_changed, LEG_WORKFLOW)
    print(f"  Updated {leg_changed} cells. Not found: {len(leg_miss)}.")
    print(f"  Saved: {LEG_OUT}")
    print(f"  Notes: {LEG_NOTES}")

    print("\nDone.")
