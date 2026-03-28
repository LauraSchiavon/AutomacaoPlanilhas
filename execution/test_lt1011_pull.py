"""
Test pull for LT1011 from 02/03/2026 to now (GMT-3).
- Facebook: Spend from all accounts
- RedTrack: Sales (convtype2), Revenue, ROAS
"""
import requests

FB_TOKEN = "EAAWDHozjODgBQ6y16hDYtG5psMsSYZBv3S6O8w3lxeihOtCryNgLFwQXGVxwbJuSks9BJQZBsWY2Iw5ZAuV9sCuTmBusecWWrkxb5glHaKLE9DSvhe6edmfT0b96sVWGZAsBVwZCMAF9cKsPJQ4qZBEGJWFoOZA9iDAPZBFT5GNUMRIIJEjj0hik5d1nTZAvWcjNMwMurcmcWgOfXOWtaZBaM3nYZB2EbdqllBH3mAjCxSGkAgocfYvJZBZCkZCdfmqUcY5LekdzG3w6doGIB0IytWhbY9JZBTEepCY5DgS3ZB8OXQZDZD"
RT_TOKEN = "wB7qY69R0KVU9tl4TBaQ"
BASE_URL = "https://graph.facebook.com/v19.0"

# Date range: 02/03/2026 to 06/03/2026 (padded +1 day for timezone)
DATE_START = "2026-03-02"
DATE_END = "2026-03-06"

accounts = [
    "987248712293933", "1583963688753767", "6481076258591934",
    "1230367020974448", "542987171356461", "1300700817490669",
    "854290605799299", "982787082695569", "875839750407156",
    "550719079377709", "1277255524217242"
]

# =====================================================
# 1. FACEBOOK: Pull spend for all campaigns matching LT1011
# =====================================================
print("=" * 60)
print("FACEBOOK ADS - Spend for LT1011 (02/03 to 05/03)")
print("=" * 60)

fb_matches = []
for acc in accounts:
    r = requests.get(
        f"{BASE_URL}/act_{acc}/insights",
        params={
            "access_token": FB_TOKEN,
            "fields": "campaign_name,spend,cpm",
            "level": "campaign",
            "time_range": f'{{"since":"{DATE_START}","until":"{DATE_END}"}}',
            "limit": 500
        },
        timeout=30
    )
    data = r.json()
    if 'data' in data:
        for c in data['data']:
            c_name = c['campaign_name']
            spend = float(c.get('spend', 0))
            if 'lt1011' in c_name.lower() and spend > 0:
                fb_matches.append({
                    'account': acc,
                    'campaign': c_name,
                    'spend_usd': spend,
                    'cpm': float(c.get('cpm', 0))
                })

total_spend = 0
for m in fb_matches:
    print(f"  ${m['spend_usd']:>8.2f} | CPM ${m['cpm']:.2f} | {m['campaign']}")
    total_spend += m['spend_usd']
print(f"\n  TOTAL SPEND (USD): ${total_spend:.2f}")

# =====================================================
# 2. REDTRACK: Pull sales, revenue, ROAS for LT1011
# =====================================================
print("\n" + "=" * 60)
print("REDTRACK - Sales/Revenue/ROAS for LT1011 (02/03 to 05/03)")
print("=" * 60)

rt_matches = []
page = 1
while True:
    r = requests.get('https://api.redtrack.io/report', params={
        'api_key': RT_TOKEN,
        'date_from': DATE_START,
        'date_to': DATE_END,
        'group': 'rt_ad',
        'limit': 1000,
        'page': page
    }, timeout=30)
    if r.status_code != 200:
        print(f"  RedTrack API error: {r.status_code}")
        break
    rt_data = r.json()
    for row in rt_data:
        rt_ad = str(row.get('rt_ad', '')).strip().lower()
        if 'lt1011' in rt_ad:
            rt_matches.append(row)
    if len(rt_data) < 1000:
        break
    page += 1

if rt_matches:
    print(f"\n  Found {len(rt_matches)} RedTrack entries matching 'lt1011':\n")
    # Print ALL available fields from the first match to discover what's available
    print("  --- All available RedTrack fields ---")
    first = rt_matches[0]
    for key, val in sorted(first.items()):
        print(f"    {key}: {val}")
    
    print("\n  --- Summary per rt_ad ---")
    for row in rt_matches:
        rt_ad = row.get('rt_ad', '')
        clicks = row.get('clicks', 0)
        conversions = row.get('conversions', 0)
        convtype2 = row.get('convtype2', 0)
        revenue = row.get('revenue', 0)
        cost = row.get('cost', 0)
        roi = row.get('roi', 0)
        print(f"  rt_ad: {rt_ad}")
        print(f"    clicks={clicks}, conversions={conversions}, convtype2(vendas)={convtype2}")
        print(f"    revenue=${revenue}, cost=${cost}, roi={roi}")
        print()
else:
    print("  No RedTrack data found for LT1011 in this date range.")
