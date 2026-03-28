import requests

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"
BASE_URL = "https://graph.facebook.com/v19.0"

accounts = [
    "987248712293933", "1583963688753767", "6481076258591934",
    "1230367020974448", "542987171356461", "1300700817490669",
    "854290605799299", "982787082695569"
]

tc_campaigns = {}

for acc in accounts:
    r = requests.get(
        f"{BASE_URL}/act_{acc}/insights",
        params={
            "access_token": TOKEN,
            "fields": "campaign_name,spend",
            "level": "campaign",
            "time_range": '{"since":"2024-01-01","until":"2026-03-06"}',
            "limit": 5000
        },
        timeout=30
    )
    data = r.json()
    if 'data' in data:
        for c in data['data']:
            c_name = c['campaign_name']
            spend = float(c.get('spend', 0))
            if spend > 0:
                if " - " in c_name:
                    c_tc = c_name.split(" - ")[-1].strip().lower()
                else:
                    c_tc = c_name.strip().lower().split(" ")[-1]
                
                if c_tc not in tc_campaigns:
                    tc_campaigns[c_tc] = []
                tc_campaigns[c_tc].append((c_name, spend))

test_ads = ['lt1017.7', 'lt1010.2', 'lt899.22', 'lt1033.2', 'lt1011.4', 'lt899.32', 'lt899.33', 'lt1034.4', 'lt1069', 'lt1070']

print("\n--- Verificando campanhas que possuem um teste único ---")
for tc in test_ads:
    if tc in tc_campaigns:
        camps = tc_campaigns[tc]
        if len(camps) >= 1: # Print everything!
            print(f"\nTC: {tc}")
            total = 0
            for name, spend in camps:
                print(f"   -> Campanha: {name} | Gasto API US$: ${spend:.2f}")
                total += spend
            print(f"   => TOTAL SOMADO US$: ${total:.2f}")

