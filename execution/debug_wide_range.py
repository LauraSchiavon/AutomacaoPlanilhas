import requests

api_key = "wB7qY69R0KVU9tl4TBaQ"
date_start = "2024-01-01" 
date_end = "2026-03-05" 

print(f"Fetching RedTrack Data for {date_start} to {date_end}...")
r_rt = requests.get('https://api.redtrack.io/report', params={
    'api_key': api_key,
    'date_from': date_start,
    'date_to': date_end,
    'group': 'rt_ad',
    'limit': 5000
}, timeout=30)

data = r_rt.json()
print(f"Returned {len(data)} rows from RedTrack")

redtrack_map = {}
for r_row in data:
    rt_ad = str(r_row.get('rt_ad', '')).strip().lower()
    if rt_ad:
        redtrack_map[rt_ad] = float(r_row.get('convtype2', 0))

test_ads = ['lt1017.7', 'lt1010.2', 'lt899.22', 'lt1033.2', 'lt1011.4', 'lt899.32', 'lt899.33', 'lt1034.4', 'lt1069', 'lt1070']

print("\n--- Map Contents for Test Ads ---")
for t in test_ads:
    v = redtrack_map.get(t, "NOT FOUND (0)")
    print(f"{t}: {v}")
