#!/usr/bin/env python3
"""
Test script to fetch RedTrack Conversions and inspect the data structure
to figure out how to map them to Facebook Ads.
"""
import sys
import os
import json
sys.stdout.reconfigure(encoding='utf-8')
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from facebook_redtrack_importer_v2 import RedTrackAPI

# The key found in the project
RT_KEY = "wB7qY69R0KVU9tl4TBaQ"

rt_api = RedTrackAPI(RT_KEY)

print("Fetching RedTrack Conversions for the last few days...")
date_start = "2026-03-01"
date_end = "2026-03-03"

conversions = rt_api.get_conversions(date_start, date_end)

print(f"\nTotal Conversions Found: {len(conversions)}")

if conversions:
    print("\n--- SAMPLE CONVERSION DATA ---")
    print(json.dumps(conversions[0], indent=2, ensure_ascii=False))
    
    # Let's see if there's varied data we can use for matching:
    print("\n--- SUBS PARAMETER ANALYSIS (First 10 convs) ---")
    for idx, c in enumerate(conversions[:10]):
        print(f"[{idx}] subs: {c.get('subs')}")
        print(f"[{idx}] cmp_name: {c.get('cmp_name')} | source: {c.get('source')} | ref_id: {c.get('ref_id')}")
else:
    print("No conversions found in this period. Let's try 2025-01-01 to 2026-03-03...")
    conversions = rt_api.get_conversions("2025-01-01", "2026-03-03")
    print(f"\nTotal Conversions Found (All Time): {len(conversions)}")
    if conversions:
        print("\n--- SAMPLE CONVERSION DATA ---")
        print(json.dumps(conversions[0], indent=2, ensure_ascii=False))
        print("\n--- SUBS PARAMETER ANALYSIS (First 5 convs) ---")
        for idx, c in enumerate(conversions[:5]):
            print(f"[{idx}] subs: {c.get('subs')}")
