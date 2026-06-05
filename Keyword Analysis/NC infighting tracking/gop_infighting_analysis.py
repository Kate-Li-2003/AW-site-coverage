"""
Empirical analysis of intra-Republican-party conflict around the 2020 election.

Operationalizes "in-fighting" as two measurable variables:
  (a) Censures/rebukes of named Republican federal officials by their own
      state or county GOP parties.
  (b) U.S. House Republican incumbents facing contested primaries.

Sources (all linkable):
  - Ballotpedia, "Annual Congressional Competitiveness Report" 2018-2024
  - Ballotpedia, "State party censures in response to Trump impeachment, 2021"
  - FiveThirtyEight (Geoffrey Skelley, Mar 2022), "How An Uptick In Censures
    Among Local Republicans Signals A Growing Radicalism"
  - Brookings Primaries Project (Kamarck) 2014/2016/2018/2022
"""

import csv
import json
from pathlib import Path

OUT = Path(__file__).parent

# ---------------------------------------------------------------------------
# (a) Censures of named GOP officials
# ---------------------------------------------------------------------------
#
# Two sub-series:
#   * STATE-PARTY censures/rebukes of federal GOP officials (Ballotpedia)
#   * COUNTY-PARTY censures of any GOP officeholder (FiveThirtyEight)
#
# Pre-2021 numbers come from FiveThirtyEight's count of 34 county-level
# censures across 3,000+ counties for 2015-2021 (28 in 2021 alone, leaving
# 6 across the prior six years). The five Democratic 2021 cases are excluded
# from the Republican series.

county_censures_R = {
    # Skelley/538 reports ~6 censures across 2015-2020 combined, partisan
    # split not broken out by year. We distribute evenly as a conservative
    # baseline: ~1 R censure per year.
    2015: 1, 2016: 1, 2017: 1, 2018: 1, 2019: 1, 2020: 1,
    2021: 23,  # explicitly reported
}

# State-party-level formal responses to Republican officials who voted to
# impeach/convict Trump for Jan 6 (Ballotpedia, April 2021 snapshot).
# Coding: 1 = censure or rebuke issued; 0 = no formal action.
state_party_responses_2021 = {
    "Burr (NC)": "censure",
    "Cassidy (LA)": "censure",
    "Murkowski (AK)": "censure",
    "Cheney (WY)": "censure",
    "Gonzalez (OH)": "censure",
    "Rice (SC)": "censure",
    "Sasse (NE)": "rebuke",
    "Toomey (PA)": "rebuke",
    "Herrera Beutler (WA)": "rebuke",
    "Newhouse (WA)": "rebuke",
    "Collins (ME)": "none",
    "Romney (UT)": "none",
    "Katko (NY)": "none",
    "Kinzinger (IL)": "none",   # censured at county level + RNC instead
    "Meijer (MI)": "none",      # censured at county level instead
    "Upton (MI)": "none",
    "Valadao (CA)": "none",
}

state_party_censures = sum(1 for v in state_party_responses_2021.values()
                           if v == "censure")
state_party_rebukes = sum(1 for v in state_party_responses_2021.values()
                          if v == "rebuke")

# Plus the RNC's Feb 2022 censure of Cheney + Kinzinger (national-party level)
rnc_2022_censures = 2

# ---------------------------------------------------------------------------
# (b) Contested primaries for House R incumbents
# ---------------------------------------------------------------------------
# Source: Ballotpedia Annual Congressional Competitiveness Reports
# "Contested R House primaries" = primaries with >1 R candidate (incumbent
#   primaries are a subset).
# "House incumbents in contested primaries (all parties)" is reported
#   directly by Ballotpedia.

contested_R_house_primaries = {
    2018: 170,
    2020: 224,
    2022: 230,   # decade high
    2024: 189,
}

house_incumbents_contested_pct = {
    2014: 21.7,
    2016: 22.8,
    2018: 24.9,
    2020: 53.8,  # methodology note: Ballotpedia tightened definition ~2020
    2022: 60.2,  # decade high since at least 2014
    2024: 52.3,
}

house_R_incumbents_defeated_in_primary = {
    2014: 4,    # est. from Brookings/Vital Stats
    2016: 5,
    2018: 2,
    2020: 5,
    2022: 10,   # confirmed from Ballotpedia table; 4 of these voted to impeach
    2024: 2,
}

# ---------------------------------------------------------------------------
# Pre/post-2020 difference
# ---------------------------------------------------------------------------

def avg(d, years):
    return sum(d[y] for y in years if y in d) / len([y for y in years if y in d])

pre_county_R = avg(county_censures_R, [2015, 2016, 2017, 2018, 2019, 2020])
post_county_R = county_censures_R[2021]

pre_R_primaries = avg(contested_R_house_primaries, [2018])  # only pre-2020 cycle in data
post_R_primaries = avg(contested_R_house_primaries, [2020, 2022, 2024])

pre_R_def = avg(house_R_incumbents_defeated_in_primary, [2014, 2016, 2018])
post_R_def = avg(house_R_incumbents_defeated_in_primary, [2020, 2022, 2024])

summary = {
    "county_GOP_censures_of_R_officials": {
        "pre_2020_baseline_per_year": pre_county_R,
        "2021": post_county_R,
        "ratio": post_county_R / pre_county_R,
    },
    "state_party_actions_2021_only": {
        "formal_censures": state_party_censures,
        "rebukes": state_party_rebukes,
        "no_action": sum(1 for v in state_party_responses_2021.values()
                         if v == "none"),
        "denominator_jan6_impeach_voters": len(state_party_responses_2021),
        "share_with_formal_response": (state_party_censures + state_party_rebukes)
                                       / len(state_party_responses_2021),
        "rnc_national_censures_2022": rnc_2022_censures,
    },
    "contested_R_House_primaries": {
        "2018_baseline": pre_R_primaries,
        "post_2020_avg": post_R_primaries,
        "pct_change": (post_R_primaries - pre_R_primaries) / pre_R_primaries * 100,
    },
    "House_R_incumbents_defeated_in_primary": {
        "pre_2020_avg_per_cycle": pre_R_def,
        "post_2020_avg_per_cycle": post_R_def,
        "ratio": post_R_def / pre_R_def,
    },
    "pct_all_House_incumbents_in_contested_primaries": house_incumbents_contested_pct,
}

# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

(OUT / "gop_infighting_summary.json").write_text(json.dumps(summary, indent=2))

with (OUT / "gop_infighting_timeseries.csv").open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["year",
                "county_R_censures",
                "contested_R_house_primaries",
                "pct_all_house_incumbents_contested",
                "R_house_incumbents_defeated_primary"])
    for y in sorted(set(list(county_censures_R) +
                        list(contested_R_house_primaries) +
                        list(house_incumbents_contested_pct) +
                        list(house_R_incumbents_defeated_in_primary))):
        w.writerow([y,
                    county_censures_R.get(y, ""),
                    contested_R_house_primaries.get(y, ""),
                    house_incumbents_contested_pct.get(y, ""),
                    house_R_incumbents_defeated_in_primary.get(y, "")])

print(json.dumps(summary, indent=2))
