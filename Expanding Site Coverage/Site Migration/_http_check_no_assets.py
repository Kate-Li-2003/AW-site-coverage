"""
Quick HTTP check on all 517 US no-asset CivicPlus URLs.
Categorises each as: 200_with_content, 200_empty, redirect, 404, error
Also checks if the base domain redirects to a different URL (migration signal).
"""
import csv, re, sys, time, warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
warnings.filterwarnings("ignore")

df_rows = []
with open('/tmp/no_assets_us_full.csv') as f:
    reader = csv.DictReader(f)
    df_rows = list(reader)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}

PLATFORM_SIGS = {
    "granicus":    ["granicus.com", "civicplus.com/AgendaCenter", "legistar", "granicus"],
    "primegov":    ["primegov.com", "primegov"],
    "boarddocs":   ["boarddocs.com", "boarddocs"],
    "municode":    ["municode.com", "municode"],
    "laserfiche":  ["laserfiche.com", "laserfiche", "weblink"],
    "neogov":      ["neogov.com", "neogov"],
    "iqm2":        ["iqm2.com", "iqm2"],
    "nupoint":     ["nupointgroup.com", "nupoint"],
    "civicweb":    ["civicweb.net", "civicweb"],
    "onboardsolutions": ["onboardsolutions.com"],
    "intellicheck":["intellicheck"],
    "wordpress":   ["wp-content", "wordpress"],
    "agendaonline":["agendaonline.net"],
}

def detect_platform(text, final_url):
    combined = (text + " " + final_url).lower()
    for platform, sigs in PLATFORM_SIGS.items():
        if any(s.lower() in combined for s in sigs):
            return platform
    return None

def check_url(row):
    url = row['url'].strip()
    name = row['name']
    state = row['state']

    # Try the AgendaCenter URL, then the base domain
    base_url = re.sub(r'/AgendaCenter.*$', '', url)

    result = {
        'name': name, 'state': state, 'gov_level': row['gov_level'],
        'original_url': url,
        'http_status': None, 'final_url': None,
        'redirected': False, 'redirect_domain': None,
        'platform_detected': None,
        'agenda_center_live': False,
        'base_domain_live': False,
        'notes': '',
    }

    # Check AgendaCenter URL
    try:
        r = requests.get(url, timeout=10, headers=HEADERS, allow_redirects=True)
        result['http_status'] = r.status_code
        result['final_url'] = r.url
        result['redirected'] = (r.url.rstrip('/') != url.rstrip('/'))
        if result['redirected']:
            result['redirect_domain'] = re.sub(r'^https?://([^/]+).*', r'\1', r.url)

        if r.status_code == 200:
            result['agenda_center_live'] = True
            p = detect_platform(r.text, r.url)
            if p:
                result['platform_detected'] = p
            # Check if AgendaCenter has any items listed
            has_items = any(x in r.text for x in [
                'AgendaItemAttachmentList', 'AgendaList', 'agenda-list',
                'AgendaListData', '.pdf', 'meetingId'
            ])
            result['notes'] = 'has_items' if has_items else 'page_loads_but_empty'
    except Exception as e:
        result['http_status'] = f'ERROR:{type(e).__name__}'
        result['notes'] = str(e)[:100]

    # If AgendaCenter is dead/redirected, check base domain
    if not result['agenda_center_live'] or result['redirected']:
        try:
            r2 = requests.get(base_url, timeout=8, headers=HEADERS, allow_redirects=True)
            if r2.status_code == 200:
                result['base_domain_live'] = True
                p = detect_platform(r2.text, r2.url)
                if p and not result['platform_detected']:
                    result['platform_detected'] = p
        except:
            pass

    return result

print(f"Checking {len(df_rows)} URLs …")
results = []
with ThreadPoolExecutor(max_workers=20) as pool:
    futures = {pool.submit(check_url, row): row for row in df_rows}
    done = 0
    for fut in as_completed(futures):
        results.append(fut.result())
        done += 1
        if done % 50 == 0:
            print(f"  {done}/{len(df_rows)} checked …")

# Summarise
agenda_live = [r for r in results if r['agenda_center_live'] and not r['redirected']]
redirected  = [r for r in results if r['redirected']]
dead        = [r for r in results if not r['agenda_center_live']]
platforms   = {}
for r in results:
    p = r['platform_detected']
    if p:
        platforms[p] = platforms.get(p, 0) + 1

print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"AgendaCenter page loads (200, not redirected): {len(agenda_live)}")
print(f"Redirected to different URL:                   {len(redirected)}")
print(f"Dead / error:                                  {len(dead)}")
print(f"\nPlatforms detected in redirects/base domains:")
for p,c in sorted(platforms.items(), key=lambda x: -x[1]):
    print(f"  {p:<25} {c}")

print(f"\nSample redirected sites (up to 20):")
for r in sorted(redirected, key=lambda x: x['state'])[:20]:
    print(f"  [{r['state']}] {r['name']}: {r['original_url']} → {r['final_url'][:80]}")

# Save full results
import json
with open('/tmp/no_assets_http_results.json', 'w') as f:
    json.dump(results, f, indent=2, default=str)
print(f"\nFull results saved to /tmp/no_assets_http_results.json")
sys.stdout.flush()
