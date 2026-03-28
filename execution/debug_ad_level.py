import requests

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"
BASE_URL = "https://graph.facebook.com/v19.0"

accounts = [
    "987248712293933", "1583963688753767", "6481076258591934",
    "1230367020974448", "542987171356461", "1300700817490669",
    "854290605799299", "982787082695569", "875839750407156", "550719079377709"
]

print("Scanning for test ads at the AD level (Feb - Mar) ...")
test_ads = ['lt1017.7', 'lt1010.2', 'lt899.22', 'lt1033.2', 'lt1011.4', 'lt899.32', 'lt899.33', 'lt1034.4', 'lt1069', 'lt1070']
found = set()

for acc in accounts:
    r = requests.get(
        f"{BASE_URL}/act_{acc}/insights",
        params={
            "access_token": TOKEN,
            "fields": "ad_name,spend",
            "level": "ad",
            "time_range": '{"since":"2026-02-01","until":"2026-03-06"}',
            "limit": 5000
        },
        timeout=30
    )
    data = r.json()
    if 'data' in data:
        for a in data['data']:
            a_name = a.get('ad_name', '')
            spend = float(a.get('spend', 0))
            if spend > 0:
                for t in test_ads:
                    if t in a_name.lower():
                        print(f"[{acc}] Found AD {t} in >> {a_name} (Spend: ${spend:.2f})")
                        found.add(t)

print("\nMissing from AD Level:")
for t in test_ads:
    if t not in found:
        print(f" -> {t}")
