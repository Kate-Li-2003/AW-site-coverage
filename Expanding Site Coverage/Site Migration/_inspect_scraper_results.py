import pandas as pd, sys
# The file has 3 metadata rows before the real header (row index 3)
df = pd.read_excel('batch_processing/scraper_results_2026-02-23.xlsx', header=3)
df.columns = [str(c).lower().replace(' ','_').replace('(','').replace(')','') for c in df.columns]
print('Columns:', list(df.columns))
print('Total rows:', len(df))
print()
# Find the "assets found" column
af_col = [c for c in df.columns if 'asset' in c and 'found' in c]
print('Assets-found col:', af_col)
if af_col:
    col = af_col[0]
    print('Values:', df[col].value_counts().to_dict())
    no_assets = df[df[col] == False].copy()
    print(f'\nNo-asset rows: {len(no_assets)}')
    show = ['name','state','gov_level','site_type','url'] + af_col
    show = [c for c in show if c in df.columns]
    print(no_assets[show].to_string())
sys.stdout.flush()
