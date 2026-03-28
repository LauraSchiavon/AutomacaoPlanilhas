#!/usr/bin/env python3
"""Print all ad names and campaigns returned by the API for comparison"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import FacebookAdsAPI

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
ACCOUNT_ID = "act_542987171356461"

fb_api = FacebookAdsAPI(TOKEN)
data = fb_api.get_ad_insights(ACCOUNT_ID, "2026-02-01", "2026-03-03", level="ad")

print(f"Total records fetched: {len(data)}")

# Let's see if ANY of the user's missing ads are in here
missing = ["LT1077", "LT1078", "LT1079", "LT1081", "LT1082", "LT1083", "LT129.315", "LT899.37", "LT899.39"]

print("\n--- Any match for missing ads? ---")
found_any = False
for record in data:
    ad_name = record.get("ad_name", "")
    camp_name = record.get("campaign_name", "")
    for m in missing:
        if m.lower() in ad_name.lower() or m.lower() in camp_name.lower():
            print(f"Match found for {m}!")
            print(f"  Campaign: {camp_name}")
            print(f"  Ad: {ad_name}")
            found_any = True

if not found_any:
    print("None of the missing ads were found in the API data.")

print("\n--- Sample of ad names actually returned ---")
for r in data[:20]:
    print(f"{r.get('ad_name')} (Campaign: {r.get('campaign_name')})")

