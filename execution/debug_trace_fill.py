"""
Trace fill_creative_tests logic step by step for account 1277255524217242
to find exactly where it drops the data.
"""
import re
import requests

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"
BASE_URL = "https://graph.facebook.com/v19.0"
ACC = "1277255524217242"

def extract_ad_name_from_campaign(campaign_name: str) -> str:
    if not campaign_name: return ""
    match = re.search(r'(?:ABO|CBO)\s+\S+\s*-\s*(.+)$', campaign_name, re.IGNORECASE)
    if match: return match.group(1).strip()
    parts = campaign_name.rsplit(' - ', 1)
    if len(parts) == 2: return parts[1].strip()
    return ""

# STEP 1: Fetch campaigns (same as fill_creative_tests.py lines 83-99)
print("=== STEP 1: Fetching campaigns ===")
all_campaigns = []
url = f"{BASE_URL}/act_{ACC}/campaigns"
params = {"access_token": TOKEN, "fields": "id,name", "limit": 500}
while url:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    page_data = data.get("data", [])
    if not page_data: break
    all_campaigns.extend(page_data)
    url = data.get("paging", {}).get("next")
    params = {}
print(f"Total campaigns fetched: {len(all_campaigns)}")

# STEP 2: Build ad_to_campaign dict (same as lines 101-110)
print("\n=== STEP 2: Building ad_to_campaign dict ===")
ad_to_campaign = {}
for camp in all_campaigns:
    c_name = camp.get("name", "")
    c_id = camp.get("id")
    if c_name:
        extracted = extract_ad_name_from_campaign(c_name)
        if extracted:
            key = extracted.strip().lower()
            if key not in ad_to_campaign:
                ad_to_campaign[key] = {"id": c_id, "name": c_name}

test_ads = ['lt1017.7', 'lt1010.2', 'lt899.22', 'lt1033.2', 'lt1011.4', 'lt899.32', 'lt899.33', 'lt1034.4', 'lt1069', 'lt1070']
print(f"Total keys in ad_to_campaign: {len(ad_to_campaign)}")
for t in test_ads:
    if t in ad_to_campaign:
        info = ad_to_campaign[t]
        print(f"  [FOUND] {t} -> Campaign ID: {info['id']}, Name: {info['name']}")
    else:
        print(f"  [MISSING] {t} not in ad_to_campaign!")
        # Try substring match (fallback logic from lines 236-239)
        for key, info in ad_to_campaign.items():
            if t in key or key in t:
                print(f"    -> Substring match: key='{key}' -> {info['name']}")
                break

# STEP 3: Fetch insights (same as lines 121-173)
print("\n=== STEP 3: Fetching insights ===")
finance_map = {}
insights_url = f"{BASE_URL}/act_{ACC}/insights"
params = {
    'access_token': TOKEN,
    'fields': 'campaign_id,impressions,cpc,cpm,ctr,spend,actions,video_p75_watched_actions',
    'level': 'campaign',
    'date_start': '2026-02-01',
    'date_end': '2026-03-06',
    'limit': 100
}
pages = 0
while insights_url:
    r_ins = requests.get(insights_url, params=params, timeout=30)
    r_ins.raise_for_status()
    data_ins = r_ins.json()
    page_ins = data_ins.get('data', [])
    if not page_ins: break
    for row in page_ins:
        c_id = row.get("campaign_id")
        if c_id:
            finance_map[c_id] = {
                "spend": float(row.get("spend", 0.0)),
                "cpm": float(row.get("cpm", 0.0)),
            }
    insights_url = data_ins.get("paging", {}).get("next")
    params = {}
    pages += 1

print(f"Total campaign IDs with insights: {len(finance_map)} (pages: {pages})")

# STEP 4: Cross-reference test ads
print("\n=== STEP 4: Cross-referencing test ads with finance data ===")
for t in test_ads:
    if t in ad_to_campaign:
        info = ad_to_campaign[t]
        c_id = info['id']
        if c_id in finance_map:
            fin = finance_map[c_id]
            print(f"  [OK] {t} -> Spend USD: ${fin['spend']:.2f}, CPM: ${fin['cpm']:.2f}")
        else:
            print(f"  [NO INSIGHTS] {t} -> Campaign {c_id} has NO insights data!")
    else:
        print(f"  [NOT IN CATALOG] {t}")
