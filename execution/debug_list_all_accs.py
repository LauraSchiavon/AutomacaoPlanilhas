import sys
import json
import requests
sys.path.append(r"C:\Preencher planilha\execution")
from facebook_redtrack_importer_v2 import FacebookAdsAPI

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"
BASE_URL = "https://graph.facebook.com/v19.0"

fb = FacebookAdsAPI(TOKEN)
bms = fb.get_bms()
print(f"BMS found: {len(bms)}")
if bms:
    accounts = fb.get_ad_accounts(bms[0]['id'])
    print(f"Accounts in first BM: {len(accounts)}")
    for a in accounts:
        print(f" -> {a['name']} ({a['id']})")
