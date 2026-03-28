import requests
import json

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"
BASE_URL = "https://graph.facebook.com/v19.0"

# Using one of the accounts from earlier
acc_raw = "982787082695569" # Diana CA03

def get_spend(until_date):
    r = requests.get(
        f"{BASE_URL}/act_{acc_raw}/insights",
        params={
            "access_token": TOKEN,
            "fields": "campaign_name,spend",
            "level": "campaign",
            "time_range": f'{{"since":"2026-03-01","until":"{until_date}"}}',
            "limit": 500
        },
        timeout=30
    )
    return r.json()

data_5 = get_spend("2026-03-05")
data_6 = get_spend("2026-03-06")

def extract(data):
    if "data" not in data:
        return {}
    return {c['campaign_name']: float(c['spend']) for c in data['data']}

map_5 = extract(data_5)
map_6 = extract(data_6)

print("Comparison for Campaigns actively spending today:")
for c_name, spend_5 in map_5.items():
    spend_6 = map_6.get(c_name, 0)
    if spend_6 > spend_5:
        print(f"[{c_name}] Until 05: ${spend_5:.2f} | Until 06: ${spend_6:.2f} -> Missing: ${spend_6 - spend_5:.2f}")

print("Done.")
