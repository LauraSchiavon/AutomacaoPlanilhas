#!/usr/bin/env python3
"""Quick test: fetch campaign-level data from Diana Vargas CA06 and verify campaign names"""
import requests, json, sys, os
sys.stdout.reconfigure(encoding='utf-8')

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
BASE = "https://graph.facebook.com/v19.0"

# Diana Vargas CA06 confirmed from debug_find_account.py
ACCOUNT_ID = "act_542987171356461"

r = requests.get(
    f"{BASE}/{ACCOUNT_ID}/insights",
    params={
        "access_token": TOKEN,
        "fields": "campaign_name,impressions,spend",
        "level": "campaign",
        "date_start": "2026-02-01",
        "date_end": "2026-03-03",
        "limit": 5,
    },
    timeout=30
)
data = r.json()
campaigns = data.get("data", [])
print(f"Found {len(campaigns)} campaigns at campaign level")
for c in campaigns:
    print(f"  {c.get('campaign_name')}")
