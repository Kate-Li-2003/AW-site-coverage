import pandas as pd

CANADIAN = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}

df = pd.read_excel('batch_processing/scraper_results_2026-02-23.xlsx', header=3)
df.columns = [str(c).lower().replace(' ','_').replace('(','').replace(')','') for c in df.columns]

no_assets = df[df['assets_found'] == False].copy()
us = no_assets[~no_assets['state'].str.strip().isin(CANADIAN)].copy()
us = us[['name','state','gov_level','site_type','url']].reset_index(drop=True)
print(f"US no-asset sites: {len(us)}")
print(f"By gov_level:\n{us['gov_level'].value_counts().to_string()}")
print(f"\nBy site_type:\n{us['site_type'].value_counts().to_string()}")
print(f"\nBy state (top 20):\n{us['state'].value_counts().head(20).to_string()}")

# Sample: pick a mix of municipalities & counties across states
# Focus on recognizable/mid-size governments rather than tiny districts
municipalities = us[us['gov_level']=='municipality'].sort_values('state')
counties = us[us['gov_level']=='county'].sort_values('state')
others = us[us['gov_level']=='other'].sort_values('state')

# 15 municipalities, 8 counties, 7 others
sample = pd.concat([
    municipalities.groupby('state').first().reset_index().head(15),
    counties.groupby('state').first().reset_index().head(8),
    others.head(7),
]).reset_index(drop=True)

print(f"\nSample ({len(sample)} sites):")
for _, r in sample.iterrows():
    print(f"  [{r['state']}] {r['name']} ({r['gov_level']}) — {r['url']}")

sample.to_csv('/tmp/no_assets_sample.csv', index=False)
us.to_csv('/tmp/no_assets_us_full.csv', index=False)
