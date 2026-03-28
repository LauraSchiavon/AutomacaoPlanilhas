"""Check which BM owns account 1277255524217242 and whether it appears in get_business_managers()"""
import sys
sys.path.append(r"C:\Preencher planilha\execution")
from facebook_redtrack_importer_v2 import FacebookAdsAPI
import requests

TOKEN = "EAAWDHozjODgBQwIYZAMRFBq5IyKIqff4ngn2S7Ca7j6apNVGvf7kdNnt1W6wJA8XhO1G3p5hA0SJOkPZABtcdKqPHSdhxvnZBruHuRZBb6gX3ZB1gVrsUp19cDA5cVjGhy3FZBZCjJjNVAiZBFZA55ZBZBk2BpSbBiVPngJgJoomdSRe8o33CstSyK8yZCX6c53NZBhlzZCcJWlGdj2H72TkvFYgl4NkZAS2fnM3PFgkPAHVIUu3GIfDx99wq9EfZCKEKqOlHI7PcLXhsfdCZClmOZAjHUWcIgHTocwvUZCKIuWK3vfCO4ZD"

fb = FacebookAdsAPI(TOKEN)

# Step 1: List all BMs
print("=== All Business Managers ===")
bms = fb.get_business_managers()
if bms:
    for bm in bms:
        print(f"  BM: {bm['name']} (ID: {bm['id']})")
else:
    print("  No BMs found!")

# Step 2: For each BM, list accounts and check for 1277255524217242
target = "1277255524217242"
print(f"\n=== Searching for account {target} ===")
found_in_bm = None
if bms:
    for bm in bms:
        accounts = fb.get_ad_accounts(bm['id'])
        if accounts:
            for acc in accounts:
                acc_id_raw = acc['id'].replace('act_', '')
                if acc_id_raw == target:
                    found_in_bm = bm
                    print(f"  FOUND in BM: {bm['name']} (ID: {bm['id']})")
                    print(f"    Account: {acc['name']} ({acc['id']})")
                    break

if not found_in_bm:
    print(f"  Account {target} NOT FOUND in any BM!")
    print("  Trying direct API check...")
    r = requests.get(
        f"https://graph.facebook.com/v19.0/act_{target}",
        params={"access_token": TOKEN, "fields": "name,business,account_status"},
        timeout=30
    )
    print(f"  Direct API response: {r.json()}")
