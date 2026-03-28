#!/usr/bin/env python3
"""Find which BM has a ca6.diana or lotto-type account"""
import requests, json

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
BASE = "https://graph.facebook.com/v19.0"

# Get all BMs
r = requests.get(f"{BASE}/me/businesses", params={"access_token": TOKEN, "fields": "id,name"}, timeout=15)
bms = r.json().get("data", [])

print(f"Found {len(bms)} BMs. Checking each for accounts...\n")

for bm in bms:
    bm_id = bm["id"]
    bm_name = bm["name"]

    # Try both owned and client accounts
    for endpoint in ["owned_ad_accounts", "client_ad_accounts"]:
        r2 = requests.get(
            f"{BASE}/{bm_id}/{endpoint}",
            params={"access_token": TOKEN, "fields": "id,name", "limit": 50},
            timeout=15
        )
        accs = r2.json().get("data", [])
        if accs:
            print(f"BM: {bm_name} ({bm_id}) - {endpoint}:")
            for acc in accs:
                print(f"   {acc['name']} -> {acc['id']}")
            print()
