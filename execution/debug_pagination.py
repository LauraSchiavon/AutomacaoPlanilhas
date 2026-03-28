#!/usr/bin/env python3
"""Test API pagination to see if it breaks after 200 records"""
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import FacebookAdsAPI

TOKEN = "EAAWDHozjODgBQ4gAIJDmZCVTNc6ngXmc8zBH78CqowDy8f9dEYYbeqNeXTwqUYlMsuDRzhc1GxnGZAjPspiWfo8am5D8p7MipDbNW1ZBUxaZCPnN00azppueER018h3kpQsCdM3RyevhDIqX1NcekjH11HzeRjSrXgI7eDpOibMWS0G9ZAqVouLTiEWEhC9NAAOZCRwFzTBvVZAmh7wdzRn08taFtob6ZAUnErQ67b9oZChQXW9aLdzF5nAvAniP2QyBR17QhJL9NDQCUigpGq6V6JBcoLvzi2ECP9WAoEQZDZD"
ACCOUNT_ID = "act_1471816737648462"  # CA05 - THE DIGITAL DAY

print(f"Testing pagination for {ACCOUNT_ID}...")
fb_api = FacebookAdsAPI(TOKEN)

def pg(msg):
    print(f"Progress: {msg}")

try:
    data = fb_api.get_ad_insights(
        ACCOUNT_ID,
        "2026-02-01",
        "2026-03-03",
        level="ad",
        progress_callback=pg
    )
    print(f"\nFinal count: {len(data)} records")
except Exception as e:
    print(f"\nEXCEPTION RAISED: {e}")
