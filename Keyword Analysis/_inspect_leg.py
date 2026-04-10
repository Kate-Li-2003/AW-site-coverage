import pandas as pd, sys
df = pd.read_excel('Original Sites to Verify/Copy_of_Legistrar_sites.xlsx', dtype=str)
print('Columns:', list(df.columns))
print('Total rows:', len(df))
if 'aw_active' in df.columns:
    print('aw_active values:', df['aw_active'].value_counts().to_dict())
url_cols = [c for c in df.columns if 'url' in c.lower() or 'site' in c.lower()]
print('URL-related cols:', url_cols)
show = ['name','state'] + url_cols[:3]
show = [c for c in show if c in df.columns]
print('\nFirst 5 rows:')
print(df[show].head(5).to_string())
print('\nSample url values:')
for c in url_cols:
    print(f'  {c}:', df[c].dropna().head(3).tolist())
sys.stdout.flush()
